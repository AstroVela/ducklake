#!/usr/bin/env python3
"""Deterministic reader ownership, disconnect fencing and bounded pin recovery."""

from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import argparse
import hashlib
import importlib.metadata
import importlib.util
import json
import os
from pathlib import Path
import selectors
import signal
import socket
import subprocess
import sys
import threading
import time
import uuid

import psycopg

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("reader_bound_fixture", HERE / "test_vane_vortex_bound_catalog.py")
H = importlib.util.module_from_spec(spec)
spec.loader.exec_module(H)
B, M, R, X, T, BASE, S, P = H.B, H.M, H.R, H.X, H.T, H.BASE, H.S, H.P
O = H.RS
RULE = {
    "schema": 1,
    "layouts": {"storage": 1, "typed": 1},
    "reader_session": "One lifetime connection, explicit terminal fence, no TTL/reconnect/adoption.",
    "materialization": "One exact version per grant, durable copy acceptance before delivery.",
    "budget": "Caller's existing <=16 work items / <=64 MiB, zero payload bytes for pin cleanup.",
    "scope": "Real PG/S3 component contracts; opaque source/index fixtures, no native remote ANN.",
    "acceptance": "All independent state/value/lock and handshake checks pass; no timing gate.",
}


def require(value, message):
    if not value:
        raise AssertionError(message)


def wait_until(predicate, label):
    deadline = time.monotonic() + 15
    while not predicate():
        require(time.monotonic() < deadline, "Handshake deadline: " + label)
        time.sleep(0.01)


class Checks(H.Checks):
    def __init__(self, *args):
        self.locks = []
        super().__init__(*args)

    def check(self):
        raw, typed, expected, locks = O.observe(self.b, self.s, H.O, BASE, T)
        T.inspect(typed, self.model)
        H.O.inspect(raw, typed, expected, self.copies(), self.raw)
        O.inspect(raw, self.copies(), self.raw)
        self.raw, self.typed, self.locks = raw, typed, locks

    def cleanup(self, label, fn, *args, **kwargs):
        raw, typed = self.raw, self.typed
        result = self.call(label, fn, *args, **kwargs)
        for key in (
            "control",
            "units",
            "objects",
            "requests",
            "gc",
            "bound_units",
            "bound_requests",
            "bound_heads",
            "versions",
        ):
            require(raw[key] == self.raw[key], "Pin maintenance touched canonical state: " + key)
        require(typed == self.typed, "Pin maintenance changed typed business history")
        return result

    def new_session(self, label):
        result = self.call("session-" + label, self.x.open_session, str(uuid.uuid4()), str(uuid.uuid4()))
        self.sessions.append(result)
        return result

    def view(self, session, reader, kind="vector", source=None):
        return self.call("view-" + reader, self.x.capture, session, *self.context("neighbor"), reader, kind, source)

    def reclaim(self, session_uuid, readers, budget=None):
        cleanup = R.ReaderReclaimer(self.b, self.m)
        budget = B.Budget(objects=16) if budget is None else budget
        wait_until(lambda: lock_missing(self, session_uuid), "explicitly disconnected holder lock release")
        self.cleanup("begin-" + session_uuid, cleanup.begin, session_uuid, 1, self.token(), budget)
        for reader in readers:
            self.cleanup("reclaim-" + reader, cleanup.view, session_uuid, 1, reader, self.token(), budget)
        result = self.cleanup("finish-" + session_uuid, cleanup.finish, session_uuid, 1, self.token(), budget)
        require(
            self.cleanup("replay-finish", cleanup.finish, session_uuid, 1, self.token(), budget) == result,
            "Finish replay changed",
        )
        return result


def worker():
    config = json.loads(sys.stdin.readline())
    backend = B.Backend(
        config["dsn"],
        config["schema"],
        B.S3Objects(S.client(config["endpoint"], "r6-admin", config["secret"]), config["bucket"], config["prefix"]),
    )
    bound = X.BoundCatalog(backend, M.TypedMetadata(backend))
    session = bound.open_session(config["session_uuid"], config["owner_uuid"])
    try:
        for reader, kind in (("worker-vector", "vector"), ("worker-sql", "sql")):
            bound.capture(session, *config["context"], reader, kind, config["source"] if kind == "sql" else None)
        print(
            json.dumps(
                {
                    "event": "ready",
                    "session_uuid": session.id,
                    "backend_pid": session.pid,
                    "affinity": sorted(os.sched_getaffinity(0)),
                }
            ),
            flush=True,
        )
        for line in sys.stdin:
            require(line.strip() == "ping", "Unknown worker command")
            print(json.dumps({"event": "alive"}), flush=True)
    finally:
        session.disconnect()


def receive(process):
    with selectors.DefaultSelector() as selector:
        selector.register(process.stdout, selectors.EVENT_READ)
        require(selector.select(30), "Reader worker handshake timeout")
    line = process.stdout.readline()
    require(line, "Reader worker exited before handshake")
    return json.loads(line)


def lock_missing(h, session_uuid):
    # An unforwarded open has no registry row; observe the known key directly.
    key = int.from_bytes(
        hashlib.sha256((h.raw["control"][0]["catalog"] + ":" + session_uuid).encode()).digest()[:8], "big", signed=True
    )
    bits = key & (2**64 - 1)
    with h.b.connect() as c:
        return not c.execute(
            "SELECT 1 FROM pg_locks WHERE locktype='advisory' AND granted AND objsubid=1 AND database=(SELECT oid FROM pg_database WHERE datname=current_database()) AND classid=%s::oid AND objid=%s::oid",
            (bits >> 32, bits & (2**32 - 1)),
        ).fetchone()


def worker_crash(h, source, report, root):
    sid, owner = str(uuid.uuid4()), str(uuid.uuid4())
    config = {
        "dsn": h.s.dsn,
        "schema": h.b.schema,
        "endpoint": h.s.endpoint,
        "secret": h.s.secret,
        "bucket": h.s.bucket,
        "prefix": h.b.objects.prefix,
        "session_uuid": sid,
        "owner_uuid": owner,
        "context": h.context("items"),
        "source": source,
    }
    with (root / "reader-worker.log").open("wb") as log:
        process = subprocess.Popen(
            [sys.executable, "-I", str(Path(__file__).resolve()), "--reader-worker"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=log,
            text=True,
        )
        try:
            process.stdin.write(json.dumps(config) + "\n")
            process.stdin.flush()
            ready = receive(process)
            require(
                ready["event"] == "ready" and ready["affinity"] == sorted(os.sched_getaffinity(0)),
                "Reader worker did not inherit CPU profile",
            )
            h.check()
            cleanup = R.ReaderReclaimer(h.b, h.m)
            os.kill(process.pid, signal.SIGSTOP)
            _, status = os.waitpid(process.pid, os.WUNTRACED)
            require(os.WIFSTOPPED(status), "Reader process was not stopped")
            h.cleanup(
                "stopped-reader-still-active", cleanup.begin, sid, 1, h.token(), B.Budget(), rejected="READER_ACTIVE"
            )
            os.kill(process.pid, signal.SIGCONT)
            process.stdin.write("ping\n")
            process.stdin.flush()
            require(receive(process) == {"event": "alive"}, "Reader did not resume")
            table_uuid = h.tables["items"]["table_uuid"]
            h.call(
                "retire-with-live-session",
                h.m.retire,
                "items",
                table_uuid,
                h.token(),
                model=lambda: h.model.retire(table_uuid),
            )
            generation = "generation:" + h.x.request_key(table_uuid, 1, "same")
            h.call(
                "live-reader-pins-protect-generation",
                h.x.plan_gc,
                table_uuid,
                generation,
                h.token(),
                B.Budget(),
                rejected="REFERENCED",
            )
            process.kill()
            require(process.wait(timeout=15) == -9, "Reader SIGKILL not observed")
            wait_until(lambda: lock_missing(h, sid), "reader lifetime lock release")
            h.check()
            h.call(
                "crashed-reader-pins-still-durable",
                h.x.plan_gc,
                table_uuid,
                generation,
                h.token(),
                B.Budget(),
                rejected="REFERENCED",
            )
            h.call(
                "crashed-SQL-session-still-protects-native",
                h.x.expire_native,
                table_uuid,
                source,
                h.token(),
                rejected="ORDINARY_TRANSACTION",
            )
            budget = B.Budget(objects=16)
            h.cleanup("begin-crashed-reader", cleanup.begin, sid, 1, h.token(), budget)
            h.cleanup(
                "fence-alone-keeps-SQL-protection",
                cleanup.finish,
                sid,
                1,
                h.token(),
                budget,
                rejected="READER_VIEWS_REMAIN",
            )
            receipt = h.cleanup("reclaim-crashed-vector", cleanup.view, sid, 1, "worker-vector", h.token(), budget)
            require(
                h.cleanup("replay-vector-reclaim", cleanup.view, sid, 1, "worker-vector", h.token(), budget) == receipt,
                "View receipt changed",
            )
            h.call(
                "SQL-protection-between-batches",
                h.x.expire_native,
                table_uuid,
                source,
                h.token(),
                rejected="ORDINARY_TRANSACTION",
            )
            h.collect(table_uuid, generation, budget)
            h.cleanup("reclaim-crashed-SQL", cleanup.view, sid, 1, "worker-sql", h.token(), budget)
            h.cleanup("finish-crashed-reader", cleanup.finish, sid, 1, h.token(), budget)
            h.call("expire-after-explicit-SQL-reclaim", h.x.expire_native, table_uuid, source, h.token())
            h.collect(table_uuid, source, budget)
            report["faults"].append(
                {
                    "operation": "stopped-live-and-SIGKILL-reader",
                    "stopped_reader_reclaim": "READER_ACTIVE",
                    "exit_code": process.returncode,
                    "worker_affinity": ready["affinity"],
                }
            )
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=15)
            process.stdin.close()
            process.stdout.close()


def ownership(h):
    neighbor = h.tables["neighbor"]["table_uuid"]
    session = h.new_session("ownership")
    h.view(session, "owned-view")
    with h.b.connect() as c:
        h.call(
            "cross-session-reclaim-FK",
            c.execute,
            "INSERT INTO reader_reclaims VALUES('owned-view',%s,1,'{}'::jsonb)",
            (uuid.UUID(h.session.id),),
            rejected=psycopg.errors.ForeignKeyViolation,
        )
        h.call(
            "session-fence-CHECK",
            c.execute,
            "UPDATE reader_sessions SET fence=17 WHERE session_uuid=%s",
            (uuid.UUID(session.id),),
            rejected=psycopg.errors.CheckViolation,
        )
    h.call(
        "other-session-cannot-materialize",
        h.x.materialize,
        h.session,
        "owned-view",
        rejected="READER_VIEW_OWNER_MISMATCH",
    )
    h.call("other-session-cannot-close", h.x.close_view, h.session, "owned-view", rejected="READER_VIEW_OWNER_MISMATCH")
    h.call(
        "other-session-cannot-unpin",
        h.x.release_copy_pin,
        h.session,
        "owned-view",
        rejected="READER_VIEW_OWNER_MISMATCH",
    )
    h.call("open-view-blocks-session-close", h.x.close_session, session, rejected="READER_VIEWS_REMAIN")
    h.call(
        "session-UUID-cannot-be-adopted",
        h.x.open_session,
        session.id,
        str(uuid.uuid4()),
        rejected="READER_SESSION_EXISTS",
    )
    for bad in (0, True, 2, 2**63):
        h.cleanup(
            "invalid-reader-fence",
            R.ReaderReclaimer(h.b, h.m).begin,
            session.id,
            bad,
            h.token(),
            B.Budget(),
            rejected="STALE_READER_FENCE" if type(bad) is int and bad == 2 else "BAD_READER_FENCE",
        )
    old = h.token()
    h.call("writer-owner-transfer", h.b.takeover, old)
    h.cleanup(
        "stale-writer-cannot-reclaim",
        R.ReaderReclaimer(h.b, h.m).begin,
        session.id,
        1,
        old,
        B.Budget(),
        rejected="FENCED",
    )
    copy = h.call("writer-transfer-keeps-reader", h.x.materialize, session, "owned-view")
    require(copy["identity"]["query"]["table_uuid"] == neighbor, "Wrong UUID in reader copy")
    h.call("explicit-copy-unpin", h.x.release_copy_pin, session, "owned-view")
    h.call("explicit-view-close", h.x.close_view, session, "owned-view")
    h.call("graceful-session-close", h.x.close_session, session)
    h.call(
        "closed-session-cannot-capture",
        h.x.capture,
        session,
        *h.context("neighbor"),
        "after-close",
        rejected="READER_FENCED",
    )
    h.call(
        "closed-session-UUID-not-reused",
        h.x.open_session,
        session.id,
        str(uuid.uuid4()),
        rejected="READER_SESSION_EXISTS",
    )


def proxy_eof(h, report):
    with P.CommitProxy("127.0.0.1", h.s.pg_port, "ack_lost") as proxy:
        backend = B.Backend(h.s.dsn + f" port={proxy.port}", h.b.schema, h.b.objects)
        bound = X.BoundCatalog(backend, M.TypedMetadata(backend))
        session = h.call("proxy-long-lived-session", bound.open_session, str(uuid.uuid4()), str(uuid.uuid4()))
        h.sessions.append(session)
        channel = socket.fromfd(session.connection.pgconn.socket, socket.AF_INET, socket.SOCK_STREAM)
        channel.shutdown(socket.SHUT_RDWR)
        channel.close()
        session.disconnect()
        wait_until(lambda: lock_missing(h, session.id), "proxy client EOF releases upstream lifetime lock")
        with h.b.connect() as c:
            require(
                not c.execute("SELECT 1 FROM pg_stat_activity WHERE pid=%s", (session.pid,)).fetchone(),
                "Proxy retained a PG backend after client EOF",
            )
    h.reclaim(session.id, [])
    report["faults"].append(
        {"operation": "reader-client-EOF-through-proxy", "server_backend_closed": True, "lifetime_lock_released": True}
    )


def session_waiter(h, session):
    key = session.key & (2**64 - 1)
    with h.b.connect() as c:
        waiter = c.execute(
            """SELECT l.pid,pg_blocking_pids(l.pid) AS blockers FROM pg_locks l
            WHERE l.locktype='advisory' AND NOT l.granted AND l.mode='ExclusiveLock'
            AND l.objsubid=2 AND l.classid::bigint=%s AND l.objid::bigint=%s
            AND l.database=(SELECT oid FROM pg_database WHERE datname=current_database())""",
            (key >> 32, key & (2**32 - 1)),
        ).fetchone()
    return waiter if waiter and waiter["blockers"] else None


def inflight(h, report, reader, *, kill_operation=False, pause_before_dispatch=False):
    # Pause one real GET on either side of dispatch, using a client separate from the oracle.
    backend = B.Backend(
        h.s.dsn, h.b.schema, B.S3Objects(S.client(h.s.endpoint, "r6-admin", h.s.secret), h.s.bucket, h.b.objects.prefix)
    )
    bound = X.BoundCatalog(backend, M.TypedMetadata(backend))
    h.reader_processes.append(bound)
    session = h.call("open-inflight-session", bound.open_session, str(uuid.uuid4()), str(uuid.uuid4()))
    h.sessions.append(session)
    h.call("capture-" + reader, bound.capture, session, *h.context("neighbor"), reader)
    units = next(v["units"] for v in h.raw["readers"] if v["id"] == reader)
    versions = {obj["key"]: obj["version"] for obj in h.raw["objects"] if obj["unit"] in units}
    require(len(versions) > 1, "Expected a ReadView that needs more than one object grant")
    original = backend.objects.read
    entered, release = threading.Event(), threading.Event()
    observations = []

    def pause_get():
        entered.set()
        require(release.wait(30), "Held GET was not released")

    def hold(key, version=None):
        first = not observations
        if first and pause_before_dispatch:
            pause_get()
        with h.b.connect() as c:
            phase = c.execute(
                "SELECT state,fence FROM reader_sessions WHERE session_uuid=%s", (uuid.UUID(session.id),)
            ).fetchone()
        require(versions.get(key) == version and version is not None, "GET did not use its exact planned version")
        result = original(key, version)
        observations.append(
            {
                "key": key,
                "version": result["version"],
                "sha": result["sha"],
                "size": result["size"],
                "session_at_dispatch": phase,
            }
        )
        if first and not pause_before_dispatch:
            pause_get()
        return result

    backend.objects.read = hold
    cleanup, budget = R.ReaderReclaimer(h.b, h.m), B.Budget(objects=16)
    with ThreadPoolExecutor(max_workers=2) as pool:
        pending = pool.submit(bound.materialize, session, reader)
        require(entered.wait(15), "Real GET did not reach the acceptance boundary")
        try:
            with h.b.connect() as c:
                require(
                    c.execute("SELECT pg_terminate_backend(%s,5000) AS killed", (session.pid,)).fetchone()["killed"],
                    "Lifetime backend was not terminated",
                )
            wait_until(lambda: lock_missing(h, session.id), "inflight lifetime lock release")
            reclaim = pool.submit(cleanup.begin, session.id, 1, h.token(), budget)
            waiting = {}

            def blocked():
                waiter = session_waiter(h, session)
                if waiter is None:
                    return False
                waiting.update(waiter)
                return True

            wait_until(blocked, "reclaimer waits on this session's operation lock")
            with h.b.connect() as c:
                c.execute("SELECT 1 FROM control WHERE singleton FOR UPDATE NOWAIT").fetchone()
            h.mutate("concurrent-writer", "write-while-" + reader, [H.row(2, reader)])
            require(not reclaim.done(), "Reclaimer advanced the fence while the operation lock was held")
            require(
                next(s for s in h.raw["reader_sessions"] if s["session_uuid"] == session.id)["state"] == "active",
                "Reclaimer bypassed the in-flight operation",
            )
            if kill_operation:
                with h.b.connect() as c:
                    require(
                        c.execute(
                            "SELECT pg_terminate_backend(%s,5000) AS killed", (waiting["blockers"][0],)
                        ).fetchone()["killed"],
                        "Operation backend was not terminated",
                    )
                reclaim.result(timeout=15)
            if pause_before_dispatch:
                require(not observations, "GET was sent before the fence")
                h.reclaim(session.id, [reader], budget)
                require(
                    accepted_materializations(h, reader) == 0
                    and not any(r["kind"] == "reader" and r["holder"] == reader for r in h.raw["refs"]),
                    "Reclaimed view acquired a copy or retained a pin",
                )
            release.set()
            try:
                pending.result(timeout=15)
            except Exception as error:
                require(
                    (isinstance(error, B.ContractError) and str(error) == "READER_DISCONNECTED")
                    or (kill_operation and isinstance(error, psycopg.OperationalError)),
                    "Stale GET failed for an unrelated reason: " + str(error),
                )
            else:
                raise AssertionError("Stale GET published a copy")
            reclaim.result(timeout=15)
        finally:
            release.set()
    require(
        reader not in bound.copies and len(observations) == 1, "Stale operation installed a copy or issued another GET"
    )
    require(
        observations[0]["session_at_dispatch"]
        == ({"state": "reclaimed", "fence": 2} if pause_before_dispatch else {"state": "active", "fence": 1}),
        "GET did not dispatch at the expected fence",
    )
    h.check()
    for label, fn, args in (
        ("capture", bound.capture, (session, *h.context("neighbor"), "fenced-new")),
        ("materialize", bound.materialize, (session, reader)),
        ("unpin", bound.release_copy_pin, (session, reader)),
        ("close", bound.close_view, (session, reader)),
    ):
        h.call("fenced-" + reader + "-" + label, fn, *args, rejected="READER_FENCED")
    require(len(observations) == 1, "Fenced RPC reached S3")
    backend.objects.read = original
    if not pause_before_dispatch:
        h.cleanup("reclaim-" + reader, cleanup.view, session.id, 1, reader, h.token(), budget)
        h.cleanup("finish-" + reader, cleanup.finish, session.id, 1, h.token(), budget)
    session.disconnect()
    report["faults"].append(
        {
            "operation": reader,
            "completed_S3_GETs": observations,
            "reclaimer_waited_for_session_operation_lock": True,
            "another_table_write_committed_while_waiting": True,
            "operation_backend_terminated": kill_operation,
            "paused_before_dispatch": pause_before_dispatch,
            "copy_published": False,
        }
    )


def paired_readers(h, proxy, reader, objects):
    backend = B.Backend(h.s.dsn + f" port={proxy.port}", h.b.schema, objects)
    metadata = M.TypedMetadata(backend)
    owner, peer = X.BoundCatalog(backend, metadata), X.BoundCatalog(backend, metadata)
    h.reader_processes.extend((owner, peer))
    session = h.call("open-" + reader, owner.open_session, str(uuid.uuid4()), str(uuid.uuid4()))
    h.sessions.append(session)
    h.call("capture-" + reader, owner.capture, session, *h.context("neighbor"), reader)
    return owner, peer, session


def accepted_materializations(h, reader):
    return next(v["accepted_materializations"] for v in h.raw["reader_session_views"] if v["reader"] == reader)


def rejected_materialization(pending, code):
    try:
        pending.result(timeout=15)
    except B.ContractError as error:
        require(str(error) == code, f"Expected {code}, got {error}")
    else:
        raise AssertionError("An invalidated materialization returned a copy")


def graceful_close_during_get(h, report):
    reader = "graceful-close-during-get"
    objects = B.S3Objects(S.client(h.s.endpoint, "r6-admin", h.s.secret), h.s.bucket, h.b.objects.prefix)
    with P.CommitProxy("127.0.0.1", h.s.pg_port, "ack_paused") as wire:
        owner, peer, session = paired_readers(h, wire, reader, objects)
        read_object = objects.read
        entered, release_get = threading.Event(), threading.Event()
        requests = []

        def read(key, version=None):
            with h.b.connect() as c:
                phase = c.execute(
                    "SELECT state,fence FROM reader_sessions WHERE session_uuid=%s", (uuid.UUID(session.id),)
                ).fetchone()
            requests.append(phase)
            result = read_object(key, version)
            if len(requests) == 1:
                entered.set()
                require(release_get.wait(30), "First GET result was not released")
            return result

        objects.read = read
        with ThreadPoolExecutor(max_workers=2) as pool:
            pending = pool.submit(peer.materialize, session, reader)
            try:
                require(entered.wait(15), "The real GET did not reach its handshake")
                h.call("graceful-close-view-during-get", owner.close_view, session, reader)
                wire.armed.set()
                closing = pool.submit(owner.close_session, session)
                wait_until(lambda: session_waiter(h, session), "graceful close waits for the in-flight read")
                require(not wire.ack_paused.is_set() and not closing.done(), "Close jumped ahead of the held GET")
                h.mutate("concurrent-writer", "write-during-graceful-close", [H.row(2, reader)])
                require(
                    next(s for s in h.raw["reader_sessions"] if s["session_uuid"] == session.id)["state"] == "active",
                    "Session was fenced while the old read was still running",
                )

                release_get.set()
                rejected_materialization(pending, "VIEW_CLOSED")
                require(wire.ack_paused.wait(15) and not closing.done(), "Close acknowledgement did not pause")
                h.check()
                closed = next(s for s in h.raw["reader_sessions"] if s["session_uuid"] == session.id)
                require(
                    closed["state"] == "closed" and closed["fence"] == 2 and not lock_missing(h, session.id),
                    "The server did not commit a closed session while the lifetime lock remained",
                )
                dispatched = len(requests)
                stale = pool.submit(peer.materialize, session, reader)
                wait_until(lambda: session_waiter(h, session), "reader waits for the pending close acknowledgement")
                require(len(requests) == dispatched, "A fenced session dispatched a GET")
                wire.release_ack.set()
                require(closing.result(timeout=15) == closed["receipt"], "Close did not return the durable receipt")
                rejected_materialization(stale, "READER_FENCED")
            finally:
                release_get.set()
                wire.release_ack.set()
        require(
            requests == [{"state": "active", "fence": 1}],
            "Closed view authorized another GET or first GET dispatched after the fence",
        )
        require(
            reader not in peer.copies and session.connection.closed, "A closed session left a new copy or connection"
        )
        h.check()
    report["faults"].append(
        {
            "operation": reader,
            "close_waited_for_session_operation_lock": True,
            "another_table_write_committed_while_waiting": True,
            "read_blocked_during_close_ack": True,
            "get_dispatch_states": requests,
            "copy_published": False,
        }
    )


def invalidate_during_materialize_ack(h, report):
    for action, error in (("close", "VIEW_CLOSED"), ("unpin", "VIEW_NOT_PINNED")):
        reader = action + "-during-copy-ack"
        with P.CommitProxy("127.0.0.1", h.s.pg_port, "ack_paused") as wire:
            owner, peer, session = paired_readers(h, wire, reader, h.b.objects)
            if action == "unpin":
                existing = deepcopy(h.call("copy-before-unpin-ack", owner.materialize, session, reader))
            initial_acceptance = accepted_materializations(h, reader)
            wire.armed.set()
            with ThreadPoolExecutor(max_workers=2) as pool:
                pending = pool.submit(peer.materialize, session, reader)
                try:
                    require(
                        wire.ack_paused.wait(15) and not pending.done() and reader not in peer.copies,
                        "The copy appeared before its COMMIT acknowledgement",
                    )
                    if action == "close":
                        h.call("close-peer-view-during-copy-ack", owner.close_view, session, reader)
                        closing = pool.submit(owner.close_session, session)
                        wait_until(lambda: session_waiter(h, session), "normal close waits through the copy COMMIT")
                        require(not closing.done(), "Normal close outran the copy operation")
                    else:
                        h.call("release-peer-pin-during-copy-ack", owner.release_copy_pin, session, reader)
                        require(owner.copies[reader] == existing, "The previously returned copy changed")
                    require(
                        not any(r["kind"] == "reader" and r["holder"] == reader for r in h.raw["refs"]),
                        "The other instance did not remove the durable pins",
                    )
                    wire.release_ack.set()
                    rejected_materialization(pending, error)
                    if action == "close":
                        require(
                            closing.result(timeout=15)["fence"] == 2, "Normal close did not reach its terminal fence"
                        )
                finally:
                    wire.release_ack.set()
            require(reader not in peer.copies, "A second instance published a copy after removal of the pins")
            if action == "unpin":
                h.call("close-peer-view-after-unpin-ack", owner.close_view, session, reader)
                h.call("close-peer-session-after-unpin-ack", owner.close_session, session)
            h.check()
            require(
                accepted_materializations(h, reader) == initial_acceptance,
                "Invalidated materialization left a durable acceptance",
            )
        report["faults"].append(
            {
                "operation": reader,
                **wire.observation,
                "peer_removed_pins_before_ack": True,
                "materialize_rejected": error,
                "copy_published": False,
            }
        )


def return_accepted_copy_after_fence(h, report):
    reader = "fence-after-copy-acceptance"
    session = h.new_session(reader)
    h.view(session, reader)
    first = deepcopy(h.call("first-copy-acceptance", h.x.materialize, session, reader))
    peer = X.BoundCatalog(h.b, h.m)
    h.reader_processes.append(peer)
    accept = peer._accept_materialization
    acknowledged, release = threading.Event(), threading.Event()
    operation = {}

    def pause_after_ack(c, *args):
        acceptance = accept(c, *args)
        operation.update(pid=c.info.backend_pid, acceptance=acceptance)
        acknowledged.set()
        require(release.wait(30), "Acknowledged acceptance was not released")
        return acceptance

    peer._accept_materialization = pause_after_ack
    with ThreadPoolExecutor(max_workers=1) as pool:
        pending = pool.submit(peer.materialize, session, reader)
        try:
            require(acknowledged.wait(15), "The acceptance COMMIT was not acknowledged")
            h.check()
            require(
                first["acceptance"] == 1 and operation["acceptance"] == accepted_materializations(h, reader) == 2,
                "The second copy lacks a distinct durable acceptance",
            )
            with h.b.connect() as c:
                require(
                    c.execute("SELECT pg_terminate_backend(%s,5000) AS killed", (operation["pid"],)).fetchone()["killed"],
                    "Accepted operation backend was not terminated",
                )
            h.call("close-view-after-copy-acceptance", h.x.close_view, session, reader)
            closed = h.call("close-session-after-copy-acceptance", h.x.close_session, session)
            require(
                closed["fence"] == 2 and accepted_materializations(h, reader) == 2 and reader not in peer.copies,
                "Expected a durable closed fence before delivery of the accepted copy",
            )
            release.set()
            result = pending.result(timeout=15)
        finally:
            release.set()
    require(result == dict(first, acceptance=2) == peer.copies[reader], "Late delivery changed the accepted copy")
    h.check()
    h.call("fenced-session-cannot-accept-again", peer.materialize, session, reader, rejected="READER_FENCED")
    report["faults"].append(
        {
            "operation": reader,
            "acceptance_acknowledged": True,
            "acceptance_before_fence": result["acceptance"],
            "durable_fence_before_delivery": closed["fence"],
            "operation_backend_terminated": True,
            "same_view_and_payload_after_fence": True,
        }
    )


def budget_case(h, source, report):
    session = h.new_session("budget")
    readers = ["budget-" + str(i) for i in range(17)]
    for reader in readers:
        h.view(session, reader)
    session.disconnect()
    wait_until(lambda: lock_missing(h, session.id), "budget holder lock release")
    cleanup = R.ReaderReclaimer(h.b, h.m)
    for token in (None, True, float(h.token())):
        invalid_owner_budget = B.Budget(objects=1)
        h.cleanup(
            "invalid-owner-cannot-begin",
            cleanup.begin,
            session.id,
            1,
            token,
            invalid_owner_budget,
            rejected="BAD_OWNER",
        )
        require(not invalid_owner_budget.admitted, "An invalid writer consumed GC budget")
    for bad in (B.Budget(objects=0), B.Budget(objects=17), B.Budget(bytes=0), B.Budget(bytes=64 * B.MIB + 1)):
        h.cleanup("invalid-reclaim-budget", cleanup.begin, session.id, 1, h.token(), bad, rejected="GC_BUDGET")
    context = h.context("neighbor")
    h.call("prepare-budget-GC", h.x.prepare, *context, 1, "budget-abort", source, b"index", 1, h.token())
    h.call("cancel-budget-GC", h.x.cancel, context[1], 1, "budget-abort", h.token())
    generation = "generation:" + h.x.request_key(context[1], 1, "budget-abort")
    budget = B.Budget(objects=16)
    h.call("share-existing-GC-budget", h.x.plan_gc, context[1], generation, h.token(), budget)
    h.cleanup("budget-begin", cleanup.begin, session.id, 1, h.token(), budget)
    admitted = budget.admitted.copy()
    for token in (None, True, float(h.token())):
        h.cleanup(
            "invalid-owner-cannot-reclaim-view",
            cleanup.view,
            session.id,
            1,
            readers[0],
            token,
            budget,
            rejected="BAD_OWNER",
        )
        h.cleanup(
            "invalid-owner-cannot-finish-reclaim", cleanup.finish, session.id, 1, token, budget, rejected="BAD_OWNER"
        )
    require(budget.admitted == admitted, "An invalid writer changed the existing budget")
    h.cleanup(
        "cannot-reclaim-another-session-view",
        cleanup.view,
        session.id,
        1,
        "control-copy",
        h.token(),
        budget,
        rejected="READER_VIEW_OWNER_MISMATCH",
    )
    for reader in readers[:14]:
        h.cleanup("budget-reclaim-" + reader, cleanup.view, session.id, 1, reader, h.token(), budget)
    h.cleanup(
        "seventeenth-work-item-rejected",
        cleanup.view,
        session.id,
        1,
        readers[14],
        h.token(),
        budget,
        rejected="GC_BUDGET",
    )
    require(len(budget.admitted) == 16 and sum(budget.admitted.values()) == 5, "Shared work/byte budget changed")
    h.collect(context[1], generation, budget, start=False)
    more = B.Budget(objects=4)
    for reader in readers[14:]:
        h.cleanup("next-explicit-budget-" + reader, cleanup.view, session.id, 1, reader, h.token(), more)
    h.cleanup("finish-budget-session", cleanup.finish, session.id, 1, h.token(), more)
    report["budget"] = {
        "views": 17,
        "first_budget_items": len(budget.admitted),
        "first_payload_bytes": sum(budget.admitted.values()),
        "next_budget_items": len(more.admitted),
        "next_payload_bytes": sum(more.admitted.values()),
    }


def rollback_cases(h):
    for phase in ("capture", "begin", "view", "finish"):
        session = h.new_session("rollback-" + phase)
        reader = "rollback-" + phase
        cleanup, budget = R.ReaderReclaimer(h.b, h.m), B.Budget(objects=16)
        if phase != "capture":
            h.view(session, reader)
            session.disconnect()
            wait_until(lambda: lock_missing(h, session.id), "rollback fixture holder lock release")
        if phase in ("view", "finish"):
            h.cleanup("prepare-rollback-begin", cleanup.begin, session.id, 1, h.token(), budget)
        if phase == "finish":
            h.cleanup("prepare-rollback-view", cleanup.view, session.id, 1, reader, h.token(), budget)
        target, event, condition = {
            "capture": ("reader_session_views", "INSERT", "NEW.reader='" + reader + "'"),
            "begin": ("reader_sessions", "UPDATE", "NEW.state='reclaiming'"),
            "view": ("reader_reclaims", "INSERT", "NEW.reader='" + reader + "'"),
            "finish": ("reader_sessions", "UPDATE", "NEW.state='reclaimed'"),
        }[phase]
        with h.b.connect() as c:
            c.execute(
                f"CREATE FUNCTION reader_abort() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF {condition} THEN RAISE EXCEPTION 'reader-abort'; END IF; RETURN NEW; END $$"
            )
            c.execute(
                f"CREATE TRIGGER reader_abort AFTER {event} ON {target} FOR EACH ROW EXECUTE FUNCTION reader_abort()"
            )
            fn, args = {
                "capture": (h.x.capture, (session, *h.context("neighbor"), reader)),
                "begin": (cleanup.begin, (session.id, 1, h.token(), budget)),
                "view": (cleanup.view, (session.id, 1, reader, h.token(), budget)),
                "finish": (cleanup.finish, (session.id, 1, h.token(), budget)),
            }[phase]
            h.call("rollback-" + phase, fn, *args, rejected=psycopg.errors.RaiseException)
            c.execute("DROP TRIGGER reader_abort ON " + target)
            c.execute("DROP FUNCTION reader_abort()")
        if phase == "capture":
            h.view(session, reader)
            session.disconnect()
            wait_until(lambda: lock_missing(h, session.id), "uncertain fixture holder lock release")
        h.reclaim(session.id, [] if phase == "finish" else [reader], budget)


def uncertain(h, report):
    for phase in ("begin", "view", "finish"):
        for mode in ("before_forward", "ack_lost"):
            session = h.new_session("unknown-" + phase)
            reader = "unknown-" + phase + "-" + mode
            h.view(session, reader)
            session.disconnect()
            cleanup, budget = R.ReaderReclaimer(h.b, h.m), B.Budget(objects=16)
            if phase in ("view", "finish"):
                h.cleanup("prepare-unknown-begin", cleanup.begin, session.id, 1, h.token(), budget)
            if phase == "finish":
                h.cleanup("prepare-unknown-view", cleanup.view, session.id, 1, reader, h.token(), budget)
            before = (h.raw, h.typed)
            with P.CommitProxy("127.0.0.1", h.s.pg_port, mode) as proxy:
                backend = B.Backend(h.s.dsn + f" port={proxy.port}", h.b.schema, h.b.objects)
                routed = R.ReaderReclaimer(backend, M.TypedMetadata(backend))
                fn, args = {
                    "begin": (routed.begin, (session.id, 1, h.token(), budget)),
                    "view": (routed.view, (session.id, 1, reader, h.token(), budget)),
                    "finish": (routed.finish, (session.id, 1, h.token(), budget)),
                }[phase]
                proxy.armed.set()
                try:
                    fn(*args)
                except B.CommitUnknown:
                    pass
                else:
                    raise AssertionError("Reclaim COMMIT uncertainty hidden")
                require(
                    proxy.dropped.wait(5)
                    and proxy.observation["injected"] == 1
                    and proxy.observation["server_committed"] == (mode == "ack_lost"),
                    "Wrong COMMIT boundary",
                )
                report["faults"].append({"operation": "reclaim-" + phase, **proxy.observation})
            h.check()
            if mode == "before_forward":
                require((h.raw, h.typed) == before, "Unforwarded reclaim changed durable state")
            h.reclaim(session.id, [] if phase == "finish" else [reader], budget)


def reader_uncertain(h, report):
    for phase in ("capture", "materialize", "acceptance"):
        for mode in ("before_forward", "ack_lost"):
            reader = "unknown-" + phase + "-" + mode
            with P.CommitProxy("127.0.0.1", h.s.pg_port, mode) as proxy:
                backend = B.Backend(h.s.dsn + f" port={proxy.port}", h.b.schema, h.b.objects)
                bound = X.BoundCatalog(backend, M.TypedMetadata(backend))
                h.reader_processes.append(bound)
                session = h.call(
                    "open-" + phase + "-unknown-session", bound.open_session, str(uuid.uuid4()), str(uuid.uuid4())
                )
                h.sessions.append(session)
                if phase != "capture":
                    h.call("capture-before-materialize-unknown", bound.capture, session, *h.context("neighbor"), reader)
                before = (h.raw, h.typed)
                if phase == "acceptance":
                    accept = bound._accept_materialization

                    def arm_acceptance(*args):
                        proxy.armed.set()
                        return accept(*args)

                    bound._accept_materialization = arm_acceptance
                else:
                    proxy.armed.set()
                try:
                    if phase == "capture":
                        bound.capture(session, *h.context("neighbor"), reader)
                    else:
                        bound.materialize(session, reader)
                except B.CommitUnknown:
                    pass
                else:
                    raise AssertionError("Reader COMMIT uncertainty hidden")
                require(
                    proxy.dropped.wait(5)
                    and proxy.observation["injected"] == 1
                    and proxy.observation["server_committed"] == (mode == "ack_lost"),
                    "Wrong reader COMMIT boundary",
                )
                require(reader not in bound.copies, "Unknown materialize published an unconfirmed copy")
                session.disconnect()
                report["faults"].append({"operation": phase, **proxy.observation})
            h.check()
            if mode == "before_forward" or phase != "capture":
                expected = deepcopy(before[0])
                if phase == "acceptance" and mode == "ack_lost":
                    link = next(v for v in expected["reader_session_views"] if v["reader"] == reader)
                    link["accepted_materializations"] = 1
                    expected["reader_session_views"].sort(key=O.encode)
                require((h.raw, h.typed) == (expected, before[1]), "Unknown COMMIT changed unexpected durable state")
            pins_exist = mode == "ack_lost" or phase != "capture"
            if pins_exist:
                view = next(r for r in h.raw["readers"] if r["id"] == reader)
                require(
                    not view["closed"]
                    and {r["unit"] for r in h.raw["refs"] if r["kind"] == "reader" and r["holder"] == reader}
                    == set(view["units"]),
                    "Unknown COMMIT removed a durable pin",
                )
            h.reclaim(session.id, [reader] if pins_exist else [])


def lifetime_uncertain(h, report):
    for phase in ("open", "close"):
        for mode in ("before_forward", "ack_lost"):
            sid, owner = str(uuid.uuid4()), str(uuid.uuid4())
            with P.CommitProxy("127.0.0.1", h.s.pg_port, mode) as proxy:
                backend = B.Backend(h.s.dsn + f" port={proxy.port}", h.b.schema, h.b.objects)
                bound = X.BoundCatalog(backend, M.TypedMetadata(backend))
                if phase == "close":
                    session = h.call("open-before-close-unknown", bound.open_session, sid, owner)
                    h.sessions.append(session)
                before = (h.raw, h.typed)
                proxy.armed.set()
                try:
                    if phase == "open":
                        bound.open_session(sid, owner)
                    else:
                        bound.close_session(session)
                except B.CommitUnknown:
                    pass
                else:
                    raise AssertionError("Session lifetime COMMIT uncertainty hidden")
                require(
                    proxy.dropped.wait(5)
                    and proxy.observation["injected"] == 1
                    and proxy.observation["server_committed"] == (mode == "ack_lost"),
                    "Wrong lifetime COMMIT boundary",
                )
                if phase == "close":
                    require(session.connection.closed, "Unknown close retained a local lifetime connection")
                report["faults"].append({"operation": "session-" + phase, **proxy.observation})
            h.check()
            if mode == "before_forward":
                require((h.raw, h.typed) == before, "Unforwarded lifetime COMMIT changed metadata")
            if phase == "open" and mode == "before_forward":
                wait_until(lambda: lock_missing(h, sid), "uncommitted open lifetime lock release")
                session = h.call("explicit-open-after-confirmed-absence", h.x.open_session, sid, owner)
                h.sessions.append(session)
                session.disconnect()
            if phase == "close" and mode == "ack_lost":
                row = h.call("reconcile-original-close", R.ReaderReclaimer(h.b, h.m).status, sid)
                require(row["state"] == "closed" and row["receipt"]["owner_uuid"] == owner, "Close receipt was rebased")
            else:
                h.reclaim(sid, [])


def mutants(h, report):
    base, _, _, _ = O.observe(h.b, h.s, H.O, BASE, T)
    copies = h.copies()
    O.inspect(base, copies)
    variants = []
    for name in ("wrong-fence", "missing-session-link", "reassign-owner", "wrong-copy-owner", "unaccepted-copy"):
        raw, private = deepcopy(base), deepcopy(copies)
        if name == "wrong-fence":
            raw["reader_sessions"][0]["fence"] = 17
        elif name == "missing-session-link":
            raw["reader_session_views"].pop()
        elif name == "reassign-owner":
            raw["reader_sessions"][0]["owner_uuid"] = str(uuid.uuid4())
        elif name == "wrong-copy-owner":
            private["control-copy"]["session_uuid"] = str(uuid.uuid4())
        else:
            private["control-copy"]["acceptance"] += 1
        try:
            O.inspect(raw, private, base)
        except AssertionError:
            variants.append(name)
        else:
            raise AssertionError("Unsafe reader mutant was accepted: " + name)
    report["unsafe_variants"] = variants


def exercise(h, report, root):
    a, b = h.create("items"), h.create("neighbor")
    source_a, _, _ = h.source("items")
    source_b, _, _ = h.source("neighbor")
    for name, source in (("items", source_a), ("neighbor", source_b)):
        h.ready(name, "same", source)
        h.publish(name, "same")
    h.capture("neighbor", "control-copy")
    saved = deepcopy(h.call("materialize-control", h.x.materialize, h.session, "control-copy"))
    h.call("control-copy-unpin", h.x.release_copy_pin, h.session, "control-copy")
    ownership(h)
    proxy_eof(h, report)
    worker_crash(h, source_a, report, root)
    h.create("concurrent-writer")
    inflight(h, report, "inflight-lifetime")
    inflight(h, report, "inflight-operation", kill_operation=True)
    inflight(h, report, "fence-after-get-authorization", kill_operation=True, pause_before_dispatch=True)
    graceful_close_during_get(h, report)
    invalidate_during_materialize_ack(h, report)
    return_accepted_copy_after_fence(h, report)
    budget_case(h, source_b, report)
    rollback_cases(h)
    uncertain(h, report)
    reader_uncertain(h, report)
    lifetime_uncertain(h, report)
    mutants(h, report)
    partial = h.new_session("partial-reclaimer-crash")
    h.view(partial, "partial-vector")
    h.view(partial, "partial-sql", "sql", source_b)
    partial.disconnect()
    wait_until(lambda: lock_missing(h, partial.id), "partial cleanup holder lock release")
    cleanup, budget = R.ReaderReclaimer(h.b, h.m), B.Budget(objects=16)
    h.cleanup("partial-reclaim-begin", cleanup.begin, partial.id, 1, h.token(), budget)
    partial_receipt = h.cleanup(
        "partial-reclaim-vector", cleanup.view, partial.id, 1, "partial-vector", h.token(), budget
    )
    old_session = h.session
    before = (h.raw, h.typed)
    h.s.restart()
    require(h.s.crash_exit_codes == [-9, -9], "Services did not exit by SIGKILL")
    h.check()
    require(
        (h.raw, h.typed) == before and h.copies()["control-copy"] == saved,
        "Service crash changed durable pins or returned private bytes",
    )
    h.call(
        "old-session-does-not-reconnect", h.x.materialize, old_session, "control-copy", rejected="READER_DISCONNECTED"
    )
    previous_owner = h.token()
    h.call("new-owner-resumes-partial-reclaim", h.b.takeover, previous_owner)
    h.cleanup(
        "old-owner-cannot-resume-partial-reclaim",
        cleanup.view,
        partial.id,
        1,
        "partial-sql",
        previous_owner,
        budget,
        rejected="FENCED",
    )
    h.cleanup(
        "partial-intent-survives-restart",
        cleanup.finish,
        partial.id,
        1,
        h.token(),
        budget,
        rejected="READER_VIEWS_REMAIN",
    )
    require(
        h.cleanup(
            "partial-original-receipt-survives-restart",
            cleanup.view,
            partial.id,
            1,
            "partial-vector",
            h.token(),
            budget,
        )
        == partial_receipt,
        "Partially completed reclaim receipt changed",
    )
    h.cleanup("resume-partial-SQL-reclaim", cleanup.view, partial.id, 1, "partial-sql", h.token(), budget)
    h.cleanup("finish-partial-after-restart", cleanup.finish, partial.id, 1, h.token(), budget)
    h.reclaim(old_session.id, ["control-copy"])
    old_session.disconnect()
    h.session = h.new_session("after-server-restart")
    h.capture("neighbor", "fresh-after-server-restart")
    h.call("fresh-session-can-read", h.x.materialize, h.session, "fresh-after-server-restart")
    h.call("close-fresh-view", h.x.close_view, h.session, "fresh-after-server-restart")
    h.call("close-fresh-session", h.x.close_session, h.session)
    report["faults"].append(
        {
            "operation": "PG-MinIO-SIGKILL",
            "exit_codes": h.s.crash_exit_codes,
            "private_bytes_preserved": True,
            "old_session_reconnected": False,
        }
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cpus", required=True)
    parser.add_argument("--pg-bin", type=Path, required=True)
    parser.add_argument("--minio", type=Path, required=True)
    parser.add_argument("--object-data-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    original = os.sched_getaffinity(0)
    cpus = {int(v) for v in args.cpus.split(",")}
    require(cpus and cpus <= original, "Requested CPU set unavailable")
    root = args.output_dir.resolve()
    root.mkdir(mode=0o700)
    modules = (H, B, M, R, X, O, H.O, T, BASE, S, P, H.HTTP)
    sources = sorted({Path(__file__).resolve(), *[Path(m.__file__).resolve() for m in modules]})
    hashes = {str(p): B.sha(p.read_bytes()) for p in sources}
    threads = {key: os.environ.get(key) for key in H.THREADS}
    report = {
        "status": "running",
        "rule": RULE,
        "source_sha256": hashes,
        "threads": threads,
        "original_affinity": sorted(original),
        "checks": [],
        "faults": [],
    }
    (root / "rule.json").write_text(json.dumps(RULE, indent=2) + "\n")
    started = time.monotonic()
    h = None
    try:
        os.sched_setaffinity(0, cpus)
        report["applied_affinity"] = sorted(os.sched_getaffinity(0))
        with S.Services(
            root / "services", args.pg_bin.resolve(), args.minio.resolve(), args.object_data_dir.resolve()
        ) as services:
            report["services"] = {
                "postgres": subprocess.check_output([str(args.pg_bin / "postgres"), "--version"], text=True).strip(),
                "minio_sha256": B.sha(args.minio.read_bytes()),
                "affinity": {
                    "postgres": sorted(os.sched_getaffinity(services.pg.pid)),
                    "minio": sorted(os.sched_getaffinity(services.s3.pid)),
                },
            }
            require(all(v == sorted(cpus) for v in report["services"]["affinity"].values()), "Service CPU set changed")
            backend = B.Backend(
                services.dsn,
                "r6_" + uuid.uuid4().hex,
                B.S3Objects(services.root_client, services.bucket, "catalog/" + uuid.uuid4().hex + "/"),
            )
            backend.initialize(str(uuid.uuid4()), str(uuid.uuid4()))
            metadata = M.TypedMetadata(backend)
            metadata.initialize(1)
            services.root_client.put_object(Bucket=services.bucket, Key="foreign/neighbor", Body=b"must-remain")
            h = Checks(services, backend, metadata, X.BoundCatalog(backend, metadata), report)
            exercise(h, report, root)
            report["final_state"] = {"storage": h.raw, "typed": h.typed}
            report["final_lock_observations"] = h.locks
            for session in h.sessions:
                session.disconnect()
        require(threads == {key: os.environ.get(key) for key in H.THREADS}, "Thread settings changed")
        require(hashes == {str(p): B.sha(p.read_bytes()) for p in sources}, "Executed source changed")
        report["clients"] = {
            key: importlib.metadata.version(key) for key in ("boto3", "botocore", "psycopg", "psycopg-binary")
        }
        report["status"] = "passed"
    except BaseException as error:
        report.update(status="failed", error=f"{type(error).__name__}: {error}")
        raise
    finally:
        if h is not None:
            for session in h.sessions:
                session.disconnect()
        os.sched_setaffinity(0, original)
        report["restored_affinity"] = sorted(os.sched_getaffinity(0))
        report["seconds_observation_only"] = time.monotonic() - started
        (root / "report.json").write_text(json.dumps(report, indent=2, default=str) + "\n")
    print(json.dumps({"status": report["status"], "checks": len(report["checks"]), "faults": report["faults"]}))


if __name__ == "__main__":
    if sys.argv[1:] == ["--reader-worker"]:
        worker()
    else:
        main()
