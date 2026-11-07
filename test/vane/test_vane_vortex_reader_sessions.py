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
O = H.load("reader_sessions_oracle", HERE / "vortex_reader_sessions_oracle.py")
RULE = {
    "schema": 1,
    "layouts": {"storage": 2, "typed": 3},
    "reader_session": "One lifetime connection, explicit terminal fence, no TTL/reconnect/adoption.",
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
        self.copy_history, self.locks = {}, []
        super().__init__(*args)

    def check(self):
        raw, typed, expected, locks = O.observe(self.b, self.s, H.O, BASE, T)
        T.inspect(typed, self.model)
        H.O.inspect(raw, typed, expected, self.copies(), self.raw)
        O.inspect(raw, self.copies(), locks, self.raw, self.copy_history)
        self.raw, self.typed, self.locks = raw, typed, locks
        self.copy_history = deepcopy(self.copies())

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


def inflight(h, report, kill_operation):
    # Use a separate broker object/client; its real GET result is held before
    # returning to materialize. The physical oracle keeps its own independent client.
    backend = B.Backend(
        h.s.dsn, h.b.schema, B.S3Objects(S.client(h.s.endpoint, "r6-admin", h.s.secret), h.s.bucket, h.b.objects.prefix)
    )
    bound = X.BoundCatalog(backend, M.TypedMetadata(backend))
    h.reader_processes.append(bound)
    session = h.call("open-inflight-session", bound.open_session, str(uuid.uuid4()), str(uuid.uuid4()))
    h.sessions.append(session)
    reader = "inflight-operation" if kill_operation else "inflight-lifetime"
    h.call("capture-" + reader, bound.capture, session, *h.context("neighbor"), reader)
    original = backend.objects.read
    entered, release = threading.Event(), threading.Event()
    observations = []

    def hold(key, version=None):
        result = original(key, version)
        observations.append({"key": key, "version": result["version"], "sha": result["sha"], "size": result["size"]})
        if len(observations) == 1:
            entered.set()
            require(release.wait(30), "Held GET was not released")
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
                with h.b.connect() as c:
                    row = c.execute(
                        "SELECT pid,pg_blocking_pids(pid) AS blockers FROM pg_stat_activity WHERE wait_event_type='Lock' AND query='SELECT * FROM control WHERE singleton FOR UPDATE'"
                    ).fetchone()
                if row and row["blockers"]:
                    waiting.update(row)
                    return True
                return False

            wait_until(blocked, "reclaimer waits for reader operation transaction")
            h.check()
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
    h.cleanup("reclaim-" + reader, cleanup.view, session.id, 1, reader, h.token(), budget)
    h.cleanup("finish-" + reader, cleanup.finish, session.id, 1, h.token(), budget)
    session.disconnect()
    report["faults"].append(
        {
            "operation": reader,
            "completed_S3_GETs": observations,
            "reclaimer_waited_for_transaction": True,
            "operation_backend_terminated": kill_operation,
            "copy_published": False,
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
    for bad in (B.Budget(objects=0), B.Budget(objects=17), B.Budget(bytes=0), B.Budget(bytes=64 * B.MIB + 1)):
        h.cleanup("invalid-reclaim-budget", cleanup.begin, session.id, 1, h.token(), bad, rejected="GC_BUDGET")
    context = h.context("neighbor")
    h.call("prepare-budget-GC", h.x.prepare, *context, 1, "budget-abort", source, b"index", 1, h.token())
    h.call("cancel-budget-GC", h.x.cancel, context[1], 1, "budget-abort", h.token())
    generation = "generation:" + h.x.request_key(context[1], 1, "budget-abort")
    budget = B.Budget(objects=16)
    h.call("share-existing-GC-budget", h.x.plan_gc, context[1], generation, h.token(), budget)
    h.cleanup("budget-begin", cleanup.begin, session.id, 1, h.token(), budget)
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
    for phase in ("capture", "materialize"):
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
                if phase == "materialize":
                    h.call("capture-before-materialize-unknown", bound.capture, session, *h.context("neighbor"), reader)
                before = (h.raw, h.typed)
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
            if mode == "before_forward" or phase == "materialize":
                require((h.raw, h.typed) == before, "Unforwarded/read-only operation changed durable pins")
            pins_exist = mode == "ack_lost" or phase == "materialize"
            if pins_exist:
                require(
                    any(r["id"] == reader and not r["closed"] for r in h.raw["readers"]),
                    "Captured view lost its durable pin",
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
    base, typed, expected, locks = O.observe(h.b, h.s, H.O, BASE, T)
    copies = h.copies()
    O.inspect(base, copies, locks)
    variants = []
    for name in ("wrong-fence", "missing-session-link", "reassign-owner", "wrong-copy-owner"):
        raw, private = deepcopy(base), deepcopy(copies)
        if name == "wrong-fence":
            raw["reader_sessions"][0]["fence"] = 17
        elif name == "missing-session-link":
            raw["reader_session_views"].pop()
        elif name == "reassign-owner":
            raw["reader_sessions"][0]["owner_uuid"] = str(uuid.uuid4())
        else:
            private["control-copy"]["session_uuid"] = str(uuid.uuid4())
        try:
            O.inspect(raw, private, locks, base, copies)
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
    inflight(h, report, False)
    inflight(h, report, True)
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
