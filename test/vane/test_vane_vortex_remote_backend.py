#!/usr/bin/env python3
"""Real PostgreSQL/S3 ticket, publication and GC conformance; no load qualification."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import hashlib
import importlib.metadata
import importlib.util
import json
import os
from pathlib import Path
import sys
import subprocess
import time
import uuid
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from botocore.exceptions import ClientError
import psycopg
from psycopg import sql


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


HERE = Path(__file__).resolve().parent
B = load("remote_backend", HERE.parents[1] / "scripts/vortex_remote_backend.py")
S = load("remote_services", HERE / "vortex_remote_services.py")
O = load("backend_oracle", HERE / "vortex_remote_backend_oracle.py")
H = load("http_fault", HERE / "vortex_remote_http_fault.py")
PG = load("commit_proxy", HERE / "postgres_commit_proxy.py")
THREADS = (
    "OMP_NUM_THREADS",
    "OMP_DYNAMIC",
    "OMP_PROC_BIND",
    "OMP_PLACES",
    "GOMP_CPU_AFFINITY",
    "KMP_AFFINITY",
    "OPENBLAS_NUM_THREADS",
    "MKL_NUM_THREADS",
    "NUMEXPR_NUM_THREADS",
    "VECLIB_MAXIMUM_THREADS",
    "RAYON_NUM_THREADS",
    "TORCH_NUM_THREADS",
    "TOKIO_WORKER_THREADS",
)
RULE = {
    "schema": 1,
    "mode": "real-postgresql-and-minio",
    "worker_authorization": "signed-ticket-only",
    "acceptance": "All fixed assertions and independent physical-state checks pass; no timing thresholds.",
    "qualification": "No native remote SQL, real AWS, HA/disk power-loss, distributed ANN or stable-load qualification.",
}


def require(value, message):
    if not value:
        raise AssertionError(message)


def wait_until(check):
    deadline = time.monotonic() + 30
    while not check():
        require(time.monotonic() < deadline, "Deterministic handshake was not observed")
        time.sleep(0.02)


class Checks:
    def __init__(self, services, backend, report):
        self.s, self.b, self.report = services, backend, report
        self.image = self.observe()
        O.inspect(self.image)

    def observe(self):
        return O.observe(self.s.dsn, self.b.schema, self.s.root_client, self.s.bucket)

    def check(self):
        after = self.observe()
        O.inspect(after, self.image)
        self.image = after

    def call(self, name, function, *args, rejected=None, **kwargs):
        before = self.image
        try:
            result = function(*args, **kwargs)
        except Exception as error:
            if rejected is None:
                raise
            require(
                (isinstance(rejected, type) and isinstance(error, rejected)) or str(error) == rejected,
                f"Unexpected rejection {type(error).__name__}: {error}",
            )
            self.check()
            require(self.image == before, "Rejected operation changed metadata or physical objects")
            self.report["checks"].append(
                {"name": name, "rejected": rejected.__name__ if isinstance(rejected, type) else rejected}
            )
            return None
        require(rejected is None, "Expected a rejection: " + name)
        self.check()
        self.report["checks"].append({"name": name, "state_sha256": B.sha(O.encode(self.image).encode())})
        if len(self.report["checks"]) % 25 == 0:
            print(f"PASS: {len(self.report['checks'])} checks; {name}", flush=True)
        return result

    def token(self):
        return self.b.state()["owner"]

    def source(self, name):
        data = ("source-bytes:" + name).encode() * 16
        token = self.token()
        unit = self.call("register-source-" + name, self.b.register_source, name, data, "snapshot:" + name, token)
        ticket = self.call("source-ticket-" + name, self.b.source_ticket, unit, data, token)
        self.call("put-source-" + name, B.dispatch, ticket, data)
        self.call("seal-source-" + name, self.b.seal_source, unit, token)
        return unit

    def prepare(self, name, source="source:b", expected=None):
        data = ("index-bytes:" + name).encode() * 16
        state = self.b.state()
        self.call(
            "prepare-" + name,
            self.b.prepare,
            name,
            source,
            data,
            state["owner"],
            state["revision"] if expected is None else expected,
            str(state["table_uuid"]),
        )
        return data

    def ready(self, name, source="source:b", expected=None):
        data = self.prepare(name, source, expected)
        token = self.token()
        for role in ("index", "manifest"):
            if role == "manifest":
                data = self.call("make-manifest-" + name, self.b.manifest, name, token)
            ticket = self.call("ticket-" + name + "-" + role, self.b.payload_ticket, name, role, data, token)
            self.call("put-" + name + "-" + role, B.dispatch, ticket, data)
            self.call("seal-" + name + "-" + role, self.b.seal, name, role, token)

    def collect(self, unit, budget=None, start=True):
        budget = B.Budget() if budget is None else budget
        token = self.token()
        if start:
            self.call("plan-GC-" + unit, self.b.plan_gc, unit, token, budget)
        keys = [o["key"] for o in self.image["objects"] if o["unit"] == unit]
        for key in keys:
            ticket, body = self.call("stop-probe-" + unit, self.b.stop, unit, key, token, budget)
            if ticket is not None:
                self.call("put-STOP-" + unit, B.dispatch, ticket, body)
                require(
                    self.call("confirm-STOP-" + unit, self.b.stop, unit, key, token, budget)[0] is None,
                    "STOP did not confirm",
                )
            ticket = self.call("delete-ticket-" + unit, self.b.delete_payload, unit, key, token, budget)
            if ticket is not None:
                self.call("delete-exact-payload-" + unit, B.dispatch, ticket)
        result = self.call("finish-GC-" + unit, self.b.finish_gc, unit, token, budget)
        require(
            len(budget.admitted) <= budget.objects and sum(budget.admitted.values()) <= budget.bytes,
            "GC budget exceeded",
        )
        self.call("replay-GC-receipt-" + unit, self.b.finish_gc, unit, token, budget)
        return result


def capabilities(s, report):
    objects = B.S3Objects(s.root_client, s.bucket, "capability/")
    objects.profile()
    results = []
    unversioned = s.bucket + "-unversioned"
    s.root_client.create_bucket(Bucket=unversioned)
    try:
        B.S3Objects(s.root_client, unversioned, "capability/").profile()
    except B.ContractError as error:
        require(str(error) == "VERSIONING_REQUIRED", "Unversioned backend did not fail closed")
        results.append({"name": "unversioned-bucket", "rejected": str(error)})
    else:
        raise AssertionError("Unversioned bucket was admitted")
    s.root_client.put_bucket_lifecycle_configuration(
        Bucket=s.bucket,
        LifecycleConfiguration={
            "Rules": [
                {
                    "ID": "fixture-only",
                    "Status": "Enabled",
                    "Filter": {"Prefix": "capability/"},
                    "Expiration": {"Days": 1},
                }
            ]
        },
    )
    try:
        objects.profile()
    except B.ContractError as error:
        require(str(error) == "LIFECYCLE_MUST_NOT_REMOVE_BARRIERS", "Lifecycle did not fail closed")
        results.append({"name": "active-lifecycle", "rejected": str(error)})
    else:
        raise AssertionError("Lifecycle could remove protocol barriers")
    finally:
        s.root_client.delete_bucket_lifecycle(Bucket=s.bucket)
    body = b"checksum-fixture"
    for name, change in (
        ("omit-condition", "omit"),
        ("change-condition", "change"),
        ("change-key", "key"),
        ("checksum-body", "body"),
        ("unsigned-delete", "method"),
    ):
        key = "capability/" + name
        ticket = objects.ticket(key, data=body, condition="*")
        headers = dict(ticket.headers)
        data = body
        if change == "omit":
            headers.pop("If-None-Match")
        if change == "change":
            headers["If-None-Match"] = "changed"
        if change == "body":
            data = b"checksum-fixturE"
        altered = replace(ticket, headers=tuple(headers.items()))
        if change == "key":
            parts = urlsplit(ticket.url)
            altered = replace(
                altered, url=urlunsplit((parts.scheme, parts.netloc, parts.path + "-other", parts.query, ""))
            )
        if change == "method":
            altered = replace(altered, method="DELETE")
        try:
            B.dispatch(altered, data)
        except B.ContractError as error:
            require(str(error) in ("HTTP_400", "HTTP_403"), "Signature/checksum rejection was not observed")
            results.append({"name": name, "rejected": str(error)})
        else:
            raise AssertionError("Worker changed its signed capability: " + name)
        require(objects.read(key) is None, "Rejected ticket created an object")
    key = "capability/version-test"
    payload = objects.ticket(key, data=body, condition="*")
    B.dispatch(payload, body)
    original = objects.read(key)
    stop = objects.ticket(key, data=b"ordinary-stop", condition=original["etag"])
    B.dispatch(stop, b"ordinary-stop")
    marker = objects.read(key)
    require(marker["version"] != original["version"], "Version identities were reused")
    require(objects.read(key, original["version"])["data"] == body, "Exact GET returned latest bytes")
    deletion = objects.ticket(key, version=original["version"])
    parts = urlsplit(deletion.url)
    query = [(k, marker["version"] if k == "versionId" else v) for k, v in parse_qsl(parts.query)]
    altered = replace(deletion, url=urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), "")))
    try:
        B.dispatch(altered)
    except B.ContractError as error:
        require(str(error) == "HTTP_403", "Changed DELETE version was accepted")
    else:
        raise AssertionError("A worker redirected an exact DELETE to the STOP version")
    B.dispatch(deletion)
    require(
        objects.read(key, original["version"]) is None and objects.read(key)["data"] == b"ordinary-stop",
        "Exact DELETE removed the STOP",
    )
    try:
        B.dispatch(payload, body)
    except B.ContractError as error:
        require(str(error) == "HTTP_412", "Late ticket was not rejected by the STOP")
    else:
        raise AssertionError("Collected key accepted late payload")
    # A real S3 delete marker remains an insufficient barrier; this key is outside adapter ownership.
    s.root_client.delete_object(Bucket=s.bucket, Key=key)
    B.dispatch(payload, body)
    require(objects.read(key)["data"] == body, "Delete-marker counterexample did not occur")
    for page in s.root_client.get_paginator("list_object_versions").paginate(Bucket=s.bucket, Prefix="capability/"):
        for item in page.get("Versions", []) + page.get("DeleteMarkers", []):
            s.root_client.delete_object(Bucket=s.bucket, Key=item["Key"], VersionId=item["VersionId"])
    report["capabilities"] = results + [
        {"name": "exact-version-and-STOP", "passed": True},
        {"name": "real-delete-marker-resurrection-counterexample", "observed": True},
    ]


def exercise(h, report, root):
    b, s = h.b, h.s
    a, source_b = h.source("a"), h.source("b")
    h.ready("seed", a)
    h.call("publish-seed", b.publish, "seed", h.token())
    h.call("capture-old", b.capture, "old")
    h.call("capture-ordinary", b.capture, "ordinary", "sql", source_b)
    h.ready("next", source_b)
    receipt = h.call("publish-next", b.publish, "next", h.token())
    h.call("GC-protects-old-view", b.plan_gc, "generation:seed", h.token(), B.Budget(), rejected="REFERENCED")
    h.call("ordinary-SQL-gate-is-global", b.expire_native, a, h.token(), rejected="ORDINARY_TRANSACTION")
    old_owner = h.token()
    h.call("owner-transfer", b.takeover, old_owner)
    h.call("stale-owner-cannot-publish", b.publish, "next", old_owner, rejected="FENCED")
    copied = h.call("materialize-old", b.materialize, "old")
    h.call("release-copied-pin", b.release_copy_pin, "old")
    h.collect("generation:seed")
    h.call("native-history-remains-independent", b.plan_gc, a, h.token(), B.Budget(), rejected="REFERENCED")
    h.call("end-ordinary-transaction", b.close_view, "ordinary")
    h.call("expire-native-source", b.expire_native, a, h.token())
    h.collect(a)
    require(b.copies["old"] == copied, "GC revoked the private byte copy")
    h.call("close-old-copy", b.close_view, "old")
    h.call("reconcile-original-next", b.reconcile, "next", receipt["payload_hash"])

    # Opaque tickets can arrive after both GC completion and owner transfer.
    data = h.prepare("late", source_b)
    ticket = h.call("register-late-PUT", b.payload_ticket, "late", "index", data, h.token())
    with H.Fault(s.endpoint, "before_forward") as proxy, ThreadPoolExecutor(max_workers=1) as pool:
        pending = pool.submit(B.dispatch, proxy.route(ticket), data)
        require(proxy.entered.wait(10), "Late PUT never reached the fixed handshake")
        h.call("cancel-late", b.cancel, "late", h.token())
        h.call("transfer-with-PUT-in-flight", b.takeover, h.token())
        h.collect("generation:late")
        proxy.release.set()
        try:
            pending.result(timeout=15)
        except B.ContractError as error:
            require(str(error) == "HTTP_412", "Late PUT was not conditionally rejected")
        else:
            raise AssertionError("Late payload resurrected a collected object")
        require(
            proxy.observation == {"requests": 1, "forwarded": True, "status": 412},
            "Late upload retried or missed its boundary",
        )
        report["faults"].append({"name": "late-old-upload-after-GC", **proxy.observation})
    h.check()

    # The payload wins between the GC probe and conditional STOP; resume explicitly.
    data = h.prepare("stop-race", source_b)
    upload = h.call("race-payload-ticket", b.payload_ticket, "stop-race", "index", data, h.token())
    h.call("cancel-stop-race", b.cancel, "stop-race", h.token())
    budget = B.Budget()
    unit = "generation:stop-race"
    h.call("plan-stop-race", b.plan_gc, unit, h.token(), budget)
    stop, stop_body = h.call("probe-before-payload", b.stop, unit, upload.key, h.token(), budget)
    h.call("payload-wins-before-STOP", B.dispatch, upload, data)
    h.call("stale-STOP-precondition-fails", B.dispatch, stop, stop_body, rejected="HTTP_412")
    h.collect(unit, budget, False)

    # PUT and DELETE really succeed upstream but their HTTP replies disappear.
    data = h.prepare("unknown-put", source_b)
    ticket = h.call("unknown-PUT-ticket", b.payload_ticket, "unknown-put", "index", data, h.token())
    with H.Fault(s.endpoint, "drop_reply") as proxy, ThreadPoolExecutor(max_workers=1) as pool:
        pending = pool.submit(B.dispatch, proxy.route(ticket), data)
        require(proxy.entered.wait(10), "PUT handshake missing")
        proxy.release.set()
        try:
            pending.result(timeout=15)
        except OSError:
            pass
        else:
            raise AssertionError("PUT reply was not lost")
        require(proxy.observation == {"requests": 1, "forwarded": True, "status": 200}, "Unknown PUT was retried")
        report["faults"].append({"name": "PUT-reply-lost", **proxy.observation})
    h.check()
    h.call("seal-unknown-PUT-by-exact-identity", b.seal, "unknown-put", "index", h.token())
    h.call("cancel-unknown-put", b.cancel, "unknown-put", h.token())
    budget, unit = B.Budget(), "generation:unknown-put"
    h.call("plan-unknown-delete", b.plan_gc, unit, h.token(), budget)
    stop, stop_body = h.call("stop-before-unknown-delete", b.stop, unit, ticket.key, h.token(), budget)
    h.call("apply-unknown-delete-STOP", B.dispatch, stop, stop_body)
    h.call("confirm-unknown-delete-STOP", b.stop, unit, ticket.key, h.token(), budget)
    deletion = h.call("unknown-delete-ticket", b.delete_payload, unit, ticket.key, h.token(), budget)
    with H.Fault(s.endpoint, "drop_reply") as proxy, ThreadPoolExecutor(max_workers=1) as pool:
        pending = pool.submit(B.dispatch, proxy.route(deletion))
        require(proxy.entered.wait(10), "DELETE handshake missing")
        proxy.release.set()
        try:
            pending.result(timeout=15)
        except OSError:
            pass
        else:
            raise AssertionError("DELETE reply was not lost")
        require(proxy.observation == {"requests": 1, "forwarded": True, "status": 204}, "Unknown DELETE was retried")
        report["faults"].append({"name": "DELETE-reply-lost", **proxy.observation})
    h.check()
    h.collect(unit, budget, False)

    # Real PostgreSQL COMMIT loss, using the existing protocol-frame proxy.
    for mode in ("before_forward", "ack_lost"):
        name = "commit-" + mode.replace("_", "-")
        h.ready(name, source_b)
        before = h.image
        with PG.CommitProxy("127.0.0.1", s.pg_port, mode) as proxy:
            routed = B.Backend(s.dsn + f" port={proxy.port}", b.schema, b.objects)
            proxy.armed.set()
            try:
                routed.publish(name, h.token())
            except B.CommitUnknown:
                pass
            else:
                raise AssertionError("COMMIT uncertainty did not reach the caller")
            require(proxy.dropped.wait(5) and proxy.observation["injected"] == 1, "COMMIT boundary was not intercepted")
            require(proxy.observation["server_committed"] == (mode == "ack_lost"), "Unexpected backend COMMIT outcome")
            report["faults"].append({"name": "postgres-" + mode, **proxy.observation})
        h.check()
        if mode == "before_forward":
            require(h.image == before, "Unforwarded COMMIT changed durable state")
        else:
            require(b.status(name)["receipt"] is not None, "Server-committed receipt missing")
        h.call("explicit-COMMIT-reconcile-" + mode, b.reconcile, name, b.status(name)["payload_hash"])
        committed = h.call("explicit-request-retry-" + mode, b.publish, name, h.token())
        again = h.call("repeat-request-receipt-" + mode, b.publish, name, h.token())
        require(committed == again, "Retry repeated metadata publication")

    # A server-side error after head/refs staging must roll the whole transaction back.
    h.ready("abort", source_b)
    with b.connect() as c:
        c.execute(
            "CREATE FUNCTION reject_receipt() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.id='abort' AND NEW.state='committed' THEN RAISE EXCEPTION 'fixture-abort'; END IF; RETURN NEW; END $$"
        )
        c.execute(
            "CREATE TRIGGER reject_receipt BEFORE UPDATE ON requests FOR EACH ROW EXECUTE FUNCTION reject_receipt()"
        )
    h.call("rollback-head-refs-receipt-together", b.publish, "abort", h.token(), rejected=psycopg.errors.RaiseException)
    with b.connect() as c:
        c.execute("DROP TRIGGER reject_receipt ON requests")
        c.execute("DROP FUNCTION reject_receipt()")
    h.call("cancel-aborted-publication", b.cancel, "abort", h.token())
    h.collect("generation:abort")

    # Two real broker connections contend for the same row; force a deterministic winner.
    expected = b.state()["revision"]
    h.ready("cas-first", source_b, expected)
    h.ready("cas-second", source_b, expected)
    lock_id = 912107
    with b.connect() as gate:
        gate.execute(
            "CREATE FUNCTION hold_first() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.head='cas-first' THEN PERFORM pg_advisory_xact_lock(912107); END IF; RETURN NEW; END $$"
        )
        gate.execute("CREATE TRIGGER hold_first BEFORE UPDATE ON control FOR EACH ROW EXECUTE FUNCTION hold_first()")
        gate.execute("SELECT pg_advisory_lock(%s)", (lock_id,))
        first = B.Backend(s.dsn + " application_name=r6-first", b.schema, b.objects)
        second = B.Backend(s.dsn + " application_name=r6-second", b.schema, b.objects)

        def blocked(name):
            return gate.execute(
                "SELECT 1 FROM pg_stat_activity WHERE application_name=%s AND wait_event_type='Lock'", (name,)
            ).fetchone()

        with ThreadPoolExecutor(max_workers=2) as pool:
            one = pool.submit(first.publish, "cas-first", h.token())
            try:
                wait_until(lambda: blocked("r6-first"))
                two = pool.submit(second.publish, "cas-second", h.token())
                wait_until(lambda: blocked("r6-second"))
                require(h.observe() == h.image, "An observer saw partial publication")
            finally:
                gate.execute("SELECT pg_advisory_unlock(%s)", (lock_id,))
            one.result(timeout=15)
            try:
                two.result(timeout=15)
            except B.ContractError as error:
                require(str(error) == "REVISION_CONFLICT", "Second publisher did not lose CAS")
            else:
                raise AssertionError("Both competing revisions committed")
        gate.execute("DROP TRIGGER hold_first ON control")
        gate.execute("DROP FUNCTION hold_first()")
    h.check()
    report["faults"].append(
        {"name": "two-brokers-CAS", "winner": "cas-first", "loser": "REVISION_CONFLICT", "partial_visible": False}
    )
    h.call("adopt-competing-original-request", b.adopt, "cas-second", h.token())
    h.call("adoption-does-not-rebase", b.publish, "cas-second", h.token(), rejected="REVISION_CONFLICT")
    h.call("cancel-CAS-loser", b.cancel, "cas-second", h.token())
    h.collect("generation:cas-second")

    # Crash the two owned services with a durable partial deletion intent.
    h.ready("restart", source_b)
    h.call("cancel-before-restart", b.cancel, "restart", h.token())
    unit, budget = "generation:restart", B.Budget()
    h.call("durable-GC-intent", b.plan_gc, unit, h.token(), budget)
    key = next(o["key"] for o in h.image["objects"] if o["unit"] == unit and o["role"] == "index")
    stop, body = h.call("register-STOP-before-crash", b.stop, unit, key, h.token(), budget)
    h.call("STOP-applied-without-metadata-ACK", B.dispatch, stop, body)
    h.call("pin-reader-before-service-crash", b.capture, "restart-reader")
    before = h.image
    s.restart()
    require(h.observe() == before, "Server restart changed committed metadata or object versions")
    b = B.Backend(s.dsn, b.schema, B.S3Objects(s.root_client, s.bucket, b.objects.prefix))
    h.b = b
    h.call("new-owner-after-service-restart", b.takeover, h.token())
    h.collect(unit)
    h.call("old-exact-DELETE-after-owner-transfer", B.dispatch, deletion)
    h.call("old-STOP-after-owner-transfer", B.dispatch, stop, body, rejected="HTTP_412")
    h.call("read-pin-survives-service-crash", b.materialize, "restart-reader")
    h.call("close-restarted-reader", b.close_view, "restart-reader")
    report["faults"].append(
        {
            "name": "owned-service-crash-and-explicit-GC-recovery",
            "metadata_and_versions_preserved": True,
            "owned_exit_codes": s.crash_exit_codes,
            "restarted_affinity": {
                "postgres": sorted(os.sched_getaffinity(s.pg.pid)),
                "minio": sorted(os.sched_getaffinity(s.s3.pid)),
            },
        }
    )
    require(s.crash_exit_codes == [-9, -9], "Owned services were not actually SIGKILLed")

    # Admission refuses boundaries without network writes or metadata changes.
    state = b.state()
    for objects in (
        B.S3Objects(s.root_client, s.bucket + "-wrong", b.objects.prefix),
        B.S3Objects(s.root_client, s.bucket, "different-prefix/"),
        B.S3Objects(S.client("http://127.0.0.1:1", "unused", "unused-secret"), s.bucket, b.objects.prefix),
    ):
        wrong = B.Backend(s.dsn, b.schema, objects)
        h.call("reopen-rejects-store-binding-mismatch", wrong.takeover, h.token(), rejected="STORE_BINDING_MISMATCH")
    for name, kwargs, code in (
        ("wrong-table", {"table_uuid": str(uuid.uuid4())}, "STALE_REQUEST_IDENTITY"),
        ("old-epoch", {"epoch": 2}, "STALE_REQUEST_IDENTITY"),
        ("bad-dimension", {"dimension": 4097}, "ROW_VECTOR_BUDGET"),
        ("rows-over", {"rows": 100001}, "ROW_VECTOR_BUDGET"),
        ("vector-over", {"rows": 4097, "dimension": 4096}, "ROW_VECTOR_BUDGET"),
        ("four-segments", {"segments": 4}, "SEGMENT_BUDGET"),
    ):
        params = dict(token=h.token(), expected=state["revision"], table_uuid=str(state["table_uuid"]))
        params.update(kwargs)
        h.call(name, b.prepare, name, source_b, b"payload", rejected=code, **params)
    for size in (0, 64 * B.MIB + 1):
        h.call(
            "artifact-boundary-" + str(size),
            b.prepare,
            "bad-size",
            source_b,
            b"x" * size,
            h.token(),
            state["revision"],
            str(state["table_uuid"]),
            rejected="ARTIFACT_BUDGET",
        )
    for objects, bytes_ in ((0, 1), (17, 1), (1, 0), (1, 64 * B.MIB + 1)):
        h.call(
            "GC-budget-boundary",
            b.plan_gc,
            "generation:commit-before-forward",
            h.token(),
            B.Budget(objects, bytes_),
            rejected="GC_BUDGET",
        )

    # Exercise exactly sixteen admissions in one real metadata round.
    sixteen = B.Budget(16, 64 * B.MIB)
    for n in range(17):
        name = f"object-boundary-{n}"
        h.prepare(name, source_b)
        h.call("cancel-" + name, b.cancel, name, h.token())
        h.call(
            "admit-" + name,
            b.plan_gc,
            "generation:" + name,
            h.token(),
            sixteen,
            rejected="GC_BUDGET" if n == 16 else None,
        )
    for n in range(16):
        h.collect(f"generation:object-boundary-{n}", sixteen, False)

    # An unuploaded 64 MiB registration consumes its full original admission charge.
    data = b"z" * (64 * B.MIB)
    h.call(
        "register-exact-64MiB",
        b.prepare,
        "byte-boundary",
        source_b,
        data,
        h.token(),
        b.state()["revision"],
        str(state["table_uuid"]),
    )
    h.call("cancel-exact-64MiB", b.cancel, "byte-boundary", h.token())
    del data
    h.call(
        "GC-one-byte-short",
        b.plan_gc,
        "generation:byte-boundary",
        h.token(),
        B.Budget(16, 64 * B.MIB - 1),
        rejected="GC_BUDGET",
    )
    exact = B.Budget(16, 64 * B.MIB)
    h.call("GC-exact-64MiB", b.plan_gc, "generation:byte-boundary", h.token(), exact)
    h.call(
        "GC-shared-budget-exhausted", b.plan_gc, "generation:object-boundary-16", h.token(), exact, rejected="GC_BUDGET"
    )
    require(
        h.collect("generation:byte-boundary", exact, False) == {"objects": 1, "bytes": 64 * B.MIB},
        "Original admission charge changed during recovery",
    )
    # The original receipt survives later publications and an ownership change.
    require(
        h.call("original-receipt-after-newer-head", b.reconcile, "next", receipt["payload_hash"]) == receipt,
        "Reconciliation followed the latest head",
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--pg-bin", type=Path, required=True)
    parser.add_argument("--minio", type=Path, required=True)
    parser.add_argument("--object-data-dir", type=Path, required=True)
    parser.add_argument("--cpus", required=True)
    args = parser.parse_args()
    root = args.output_dir.resolve()
    root.mkdir(mode=0o700)
    cpus = {int(x) for x in args.cpus.split(",")}
    original = os.sched_getaffinity(0)
    require(cpus and cpus <= original, "Requested CPU set unavailable")
    threads = {name: os.environ.get(name) for name in THREADS}
    sources = [
        Path(__file__),
        Path(B.__file__),
        Path(S.__file__),
        Path(O.__file__),
        Path(H.__file__),
        Path(PG.__file__),
    ]
    hashes = {str(p): B.sha(p.read_bytes()) for p in sources}
    report = {
        "status": "running",
        "rule": RULE,
        "limits": B.LIMITS,
        "source_sha256": hashes,
        "threads": threads,
        "original_affinity": sorted(original),
        "checks": [],
        "faults": [],
        "clients": {n: importlib.metadata.version(n) for n in ("boto3", "botocore", "psycopg", "psycopg-binary")},
    }
    (root / "rule.json").write_text(json.dumps(RULE, indent=2) + "\n")
    started = time.monotonic()
    try:
        os.sched_setaffinity(0, cpus)
        report["applied_affinity"] = sorted(os.sched_getaffinity(0))
        with S.Services(
            root / "services", args.pg_bin.resolve(), args.minio.resolve(), args.object_data_dir.resolve()
        ) as services:
            report["services"] = {
                "postgres": services.pg_port,
                "minio": services.s3_port,
                "pg_version": subprocess.check_output([str(args.pg_bin / "postgres"), "--version"], text=True).strip(),
                "minio_sha256": B.sha(args.minio.read_bytes()),
                "affinity": {
                    "postgres": sorted(os.sched_getaffinity(services.pg.pid)),
                    "minio": sorted(os.sched_getaffinity(services.s3.pid)),
                },
            }
            require(all(v == sorted(cpus) for v in report["services"]["affinity"].values()), "Service affinity changed")
            capabilities(services, report)
            schema = "r6_" + uuid.uuid4().hex
            objects = B.S3Objects(services.root_client, services.bucket, "catalog/" + uuid.uuid4().hex + "/")
            backend = B.Backend(services.dsn, schema, objects)
            backend.initialize(str(uuid.uuid4()), str(uuid.uuid4()))
            # Unknown neighboring objects must remain byte/version-identical throughout adapter GC.
            services.root_client.put_object(Bucket=services.bucket, Key="foreign/neighbor", Body=b"neighbor-kept")
            h = Checks(services, backend, report)
            exercise(h, report, root)
            report["final_state"] = h.image
            report["status"] = "passed"
        require(threads == {name: os.environ.get(name) for name in THREADS}, "Inherited thread settings changed")
        require(hashes == {str(p): B.sha(p.read_bytes()) for p in sources}, "Executed source changed")
    except BaseException as error:
        report.update(status="failed", error=f"{type(error).__name__}: {error}")
        raise
    finally:
        os.sched_setaffinity(0, original)
        report["restored_affinity"] = sorted(os.sched_getaffinity(0))
        report["seconds_observation_only"] = time.monotonic() - started
        (root / "report.json").write_text(json.dumps(report, indent=2, default=str) + "\n")
    print(json.dumps({"status": report["status"], "checks": len(report["checks"]), "faults": report["faults"]}))


if __name__ == "__main__":
    main()
