"""A loopback HTTP fixture: pause before forwarding or lose a completed reply."""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import http.client
import threading
from urllib.parse import urlsplit, urlunsplit


class Fault:
    def __init__(self, endpoint, mode):
        assert mode in ("before_forward", "drop_reply")
        self.endpoint, self.mode = urlsplit(endpoint), mode
        self.entered, self.release, self.completed = threading.Event(), threading.Event(), threading.Event()
        self.observation = {"requests": 0, "forwarded": False, "status": None}
        owner = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *_):
                pass

            def handle_request(self):
                length = int(self.headers.get("Content-Length", 0))
                assert length <= 64 * 1024 * 1024
                body = self.rfile.read(length)
                owner.observation["requests"] += 1
                owner.entered.set()
                if not owner.release.wait(timeout=30):
                    self.close_connection = True
                    return
                upstream = http.client.HTTPConnection(owner.endpoint.hostname, owner.endpoint.port, timeout=10)
                try:
                    upstream.request(self.command, self.path, body=body, headers=dict(self.headers))
                    response = upstream.getresponse()
                    data = response.read()
                    owner.observation.update(forwarded=True, status=response.status)
                    if owner.mode == "drop_reply":
                        self.close_connection = True
                        return
                    self.send_response(response.status)
                    for key, value in response.getheaders():
                        if key.lower() not in ("connection", "transfer-encoding", "content-length"):
                            self.send_header(key, value)
                    self.send_header("Content-Length", str(len(data)))
                    self.end_headers()
                    self.wfile.write(data)
                finally:
                    upstream.close()
                    owner.completed.set()

            do_PUT = do_DELETE = handle_request

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def route(self, ticket):
        original = urlsplit(ticket.url)
        url = urlunsplit(("http", f"127.0.0.1:{self.server.server_port}", original.path, original.query, ""))
        # The fixture changes the TCP route while preserving the signed HTTP Host.
        return type(ticket)(ticket.method, ticket.key, url, ticket.headers + (("Host", original.netloc),))

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *args):
        self.release.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
