"""Test-only PostgreSQL wire proxy for deterministic COMMIT response loss."""

from __future__ import annotations

import socket
import struct
import threading
from pathlib import Path


def receive_exact(connection: socket.socket, size: int) -> bytes:
    result = bytearray()
    while len(result) < size:
        data = connection.recv(size - len(result))
        if not data:
            raise EOFError("PostgreSQL connection closed")
        result.extend(data)
    return bytes(result)


def receive_frame(connection: socket.socket) -> tuple[bytes, bytes, bytes]:
    kind = receive_exact(connection, 1)
    length_bytes = receive_exact(connection, 4)
    length = struct.unpack("!I", length_bytes)[0]
    if not 4 <= length <= 16 * 1024 * 1024:
        raise ValueError("invalid PostgreSQL message length")
    payload = receive_exact(connection, length - 4)
    return kind, payload, kind + length_bytes + payload


class CommitProxy:
    def __init__(self, upstream_host: str, upstream_port: int, mode: str):
        if mode not in {"before_forward", "ack_lost"}:
            raise ValueError("unknown COMMIT fault mode")
        self.upstream_host = upstream_host
        self.upstream_port = upstream_port
        self.mode = mode
        self.lock = threading.Lock()
        self.armed = threading.Event()
        self.stopped = threading.Event()
        self.dropped = threading.Event()
        self.connections: set[socket.socket] = set()
        self.observation = {"mode": mode, "injected": 0, "commit_forwarded": False, "server_committed": False}
        self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen()
        self.listener.settimeout(0.2)
        self.port = self.listener.getsockname()[1]
        self.thread = threading.Thread(target=self.accept, daemon=True)

    def __enter__(self) -> CommitProxy:
        self.thread.start()
        return self

    def __exit__(self, *args) -> None:
        self.disconnect()
        self.thread.join(timeout=2)

    def upstream(self) -> socket.socket:
        if self.upstream_host.startswith("/"):
            result = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            result.connect(str(Path(self.upstream_host) / f".s.PGSQL.{self.upstream_port}"))
        else:
            result = socket.create_connection((self.upstream_host, self.upstream_port), timeout=5)
            result.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            result.settimeout(None)
        return result

    def accept(self) -> None:
        while not self.stopped.is_set():
            try:
                client, _ = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            threading.Thread(target=self.bridge, args=(client,), daemon=True).start()

    def disconnect(self) -> None:
        self.stopped.set()
        self.listener.close()
        with self.lock:
            connections = list(self.connections)
        for connection in connections:
            try:
                connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            connection.close()

    def bridge(self, client: socket.socket) -> None:
        server = None
        state = {"withhold": False, "commit_tag": False}
        try:
            server = self.upstream()
            with self.lock:
                if self.stopped.is_set():
                    return
                self.connections.update([client, server])
            length_bytes = receive_exact(client, 4)
            length = struct.unpack("!I", length_bytes)[0]
            if not 8 <= length <= 1024 * 1024:
                raise ValueError("invalid PostgreSQL startup length")
            startup = receive_exact(client, length - 4)
            if struct.unpack("!I", startup[:4])[0] != 196608:
                raise ValueError("fault probe requires PostgreSQL protocol 3 without TLS/GSS negotiation")
            server.sendall(length_bytes + startup)
            threading.Thread(target=self.backend, args=(client, server, state), daemon=True).start()
            while not self.stopped.is_set():
                kind, payload, frame = receive_frame(client)
                query = payload.rstrip(b"\0").strip().rstrip(b";").strip().upper() if kind == b"Q" else b""
                if query == b"COMMIT" and self.armed.is_set():
                    with self.lock:
                        chosen = self.observation["injected"] == 0
                        if chosen:
                            self.observation["injected"] = 1
                            self.observation["frontend_query"] = "COMMIT"
                            state["withhold"] = True
                    if chosen:
                        if self.mode == "before_forward":
                            self.disconnect()
                            self.dropped.set()
                            return
                        self.observation["commit_forwarded"] = True
                server.sendall(frame)
        except (OSError, EOFError):
            pass
        except Exception as error:
            self.observation["proxy_error"] = str(error)
        finally:
            for connection in [client, server]:
                if connection is not None:
                    with self.lock:
                        self.connections.discard(connection)
                    connection.close()

    def backend(self, client: socket.socket, server: socket.socket, state: dict) -> None:
        try:
            while not self.stopped.is_set():
                kind, payload, frame = receive_frame(server)
                if state["withhold"]:
                    if kind == b"C":
                        state["commit_tag"] = payload == b"COMMIT\0"
                        self.observation["command_complete"] = payload.rstrip(b"\0").decode("ascii", errors="replace")
                    if kind == b"Z":
                        self.observation["ready_status"] = payload.decode("ascii", errors="replace")
                        self.observation["server_committed"] = state["commit_tag"] and payload == b"I"
                        self.disconnect()
                        self.dropped.set()
                        return
                    continue
                client.sendall(frame)
        except (OSError, EOFError):
            pass
        except Exception as error:
            self.observation["proxy_error"] = str(error)
