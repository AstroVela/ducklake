#!/usr/bin/env python3
"""Deterministic real-service binding, retirement, ReadView and GC contracts."""

from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import argparse
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid

import psycopg

HERE = Path(__file__).resolve().parent


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


B = load("vortex_remote_backend", HERE.parents[1] / "scripts/vortex_remote_backend.py")
M = load("vortex_postgres_metadata", HERE.parents[1] / "scripts/vortex_postgres_metadata.py")
X = load("vortex_bound_catalog", HERE.parents[1] / "scripts/vortex_bound_catalog.py")
O = load("bound_oracle", HERE / "vortex_bound_catalog_oracle.py")
T = load("bound_typed_oracle", HERE / "vortex_postgres_metadata_oracle.py")
BASE = load("bound_storage_oracle", HERE / "vortex_remote_backend_oracle.py")
S = load("bound_services", HERE / "vortex_remote_services.py")
P = load("bound_commit_proxy", HERE / "postgres_commit_proxy.py")
HTTP = load("bound_http_proxy", HERE / "vortex_remote_http_fault.py")
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
    "layouts": {"storage": 2, "typed": 2},
    "mode": "mandatory typed bindings",
    "acceptance": "All real-service assertions and independent physical-state checks pass; no timing gate.",
    "scope": "Component identity/visibility only; opaque object fixtures, no native remote ANN qualification.",
}


def require(value, message):
    if not value:
        raise AssertionError(message)


def row(key, payload="value"):
    return {
        "id": key,
        "embedding": [float(key), -0.0],
        "category": "A\0",
        "payload": payload,
        "f32": 0.1,
        "f64": -0.0,
        "data": b"\0\xff",
        "numbers": [None, 0.1],
    }


class Checks:
    def __init__(self, services, backend, metadata, bound, report):
        self.s, self.b, self.m, self.x, self.report = services, backend, metadata, bound, report
        self.model, self.tables, self.reader_processes = T.Model(), {}, [bound]
        self.raw = None
        self.check()

    def token(self):
        return self.b.state()["owner"]

    def copies(self):
        result = {}
        for process in self.reader_processes:
            result.update(process.copies)
        return result

    def check(self):
        raw, typed, expected = O.observe(self.b, self.s, BASE, T)
        T.inspect(typed, self.model)
        O.inspect(raw, typed, expected, self.copies(), self.raw)
        self.raw, self.typed = raw, typed

    def call(self, label, fn, *args, rejected=None, model=None, **kwargs):
        before = (self.raw, self.typed)
        try:
            result = fn(*args, **kwargs)
        except Exception as error:
            if rejected is None:
                raise
            require(
                (isinstance(rejected, type) and isinstance(error, rejected)) or str(error) == rejected,
                f"Unexpected rejection {type(error).__name__}: {error}",
            )
            self.check()
            require((self.raw, self.typed) == before, "Rejected binding operation changed state")
            self.report["checks"].append(
                {"name": label, "rejected": rejected.__name__ if isinstance(rejected, type) else rejected}
            )
            return None
        require(rejected is None, "Expected rejection: " + label)
        if model is not None:
            expected = model()
            if expected is not None:
                require(result == expected, "Typed lifecycle result differs from reference")
        self.check()
        self.report["checks"].append({"name": label, "state_sha256": B.sha(O.encode([self.raw, self.typed]).encode())})
        if len(self.report["checks"]) % 25 == 0:
            print(f"PASS: {len(self.report['checks'])} bound checks; {label}", flush=True)
        return result

    def create(self, name, ids=tuple(range(1, 9))):
        table_uuid = str(uuid.uuid4())
        result = self.call(
            "create-" + name,
            self.m.create,
            name,
            table_uuid,
            2,
            self.token(),
            ids,
            model=lambda: self.model.create(name, table_uuid, 2, ids),
        )
        self.tables[name] = result
        self.mutate(name, "seed", [row(i) for i in range(65)])
        return result

    def mutate(self, name, request, rows, deleted=()):
        table = self.tables[name]
        epoch = self.model.tables[table["table_uuid"]]["request_epoch"]
        return self.call(
            "typed-" + request,
            self.m.mutate,
            name,
            table["table_uuid"],
            epoch,
            request,
            rows,
            list(deleted),
            table["schema_hash"],
            self.token(),
            model=lambda: self.model.mutate(table["table_uuid"], epoch, request, rows, list(deleted)),
        )

    def context(self, name):
        table = self.tables[name]
        return name, table["table_uuid"], table["schema_hash"]

    def source(self, name, source_id="shared"):
        context = self.context(name)
        data = b"same-opaque-source-bytes"
        unit = self.call(
            "register-source-" + name,
            self.x.register_source,
            *context,
            self.model.snapshot,
            source_id,
            data,
            self.token(),
        )
        ticket = self.call("source-ticket-" + name, self.x.source_ticket, *context, unit, data, self.token())
        self.call("upload-source-" + name, B.dispatch, ticket, data)
        self.call("seal-source-" + name, self.x.seal_source, *context, unit, self.token())
        return unit, ticket, data

    def ready(self, name, request, source, expected=None, epoch=None):
        context = self.context(name)
        table_uuid = context[1]
        epoch = self.model.tables[table_uuid]["request_epoch"] if epoch is None else epoch
        head = next(v for v in self.raw["bound_heads"] if v["table_uuid"] == table_uuid)
        expected = head["revision"] if expected is None else expected
        data = b"same-opaque-index-bytes"
        self.call(
            "prepare-" + name + "-" + request,
            self.x.prepare,
            *context,
            epoch,
            request,
            source,
            data,
            expected,
            self.token(),
        )
        for role in ("index", "manifest"):
            if role == "manifest":
                data = self.call("manifest-" + name, self.x.manifest, *context, epoch, request, self.token())
            ticket = self.call(
                "ticket-" + name + "-" + role, self.x.payload_ticket, *context, epoch, request, role, data, self.token()
            )
            self.call("PUT-" + name + "-" + role, B.dispatch, ticket, data)
            self.call("seal-" + name + "-" + role, self.x.seal, *context, epoch, request, role, self.token())
        return epoch

    def publish(self, name, request, epoch=None):
        context = self.context(name)
        epoch = self.model.tables[context[1]]["request_epoch"] if epoch is None else epoch
        return self.call("publish-" + name + "-" + request, self.x.publish, *context, epoch, request, self.token())

    def capture(self, name, reader, kind="vector", source=None):
        return self.call("capture-" + reader, self.x.capture, *self.context(name), reader, kind, source)

    def collect(self, table_uuid, unit, budget=None, start=True):
        budget = B.Budget() if budget is None else budget
        token = self.token()
        if start:
            self.call("GC-admit", self.x.plan_gc, table_uuid, unit, token, budget)
        for key in [o["key"] for o in self.raw["objects"] if o["unit"] == unit]:
            ticket, data = self.call("GC-probe-STOP", self.x.stop, table_uuid, unit, key, token, budget)
            if ticket is not None:
                self.call("GC-write-STOP", B.dispatch, ticket, data)
                require(
                    self.call("GC-confirm-STOP", self.x.stop, table_uuid, unit, key, token, budget)[0] is None,
                    "Barrier did not confirm",
                )
            ticket = self.call("GC-delete-ticket", self.x.delete_payload, table_uuid, unit, key, token, budget)
            if ticket is not None:
                self.call("GC-delete-version", B.dispatch, ticket)
        result = self.call("GC-finish", self.x.finish_gc, table_uuid, unit, token, budget)
        require(
            len(budget.admitted) <= budget.objects and sum(budget.admitted.values()) <= budget.bytes,
            "GC budget changed",
        )
        self.call("GC-receipt-replay", self.x.finish_gc, table_uuid, unit, token, budget)
        return result


def exercise(h, report):
    a, b = h.create("items", (3, 11, 24, 37, 48, 59, 70, 81)), h.create("neighbor")
    au, bu = a["table_uuid"], b["table_uuid"]
    source_a, old_source_ticket, source_data = h.source("items")
    source_b, _, _ = h.source("neighbor")
    h.call(
        "foreign-source-rejected",
        h.x.prepare,
        *h.context("neighbor"),
        1,
        "wrong-source",
        source_a,
        b"index",
        0,
        h.token(),
        rejected="UNIT_BINDING_MISMATCH",
    )
    h.call("foreign-GC-rejected", h.x.plan_gc, bu, source_a, h.token(), B.Budget(), rejected="UNIT_BINDING_MISMATCH")
    h.call("stale-schema-rejected", h.x.capture, "items", au, "00" * 32, "bad-schema", rejected="STALE_SCHEMA")
    with h.b.connect() as c:
        h.call(
            "foreign-table-id-FK-rejected",
            c.execute,
            "UPDATE bound_units SET table_id=%s WHERE unit=%s",
            (b["table_id"], source_a),
            rejected=psycopg.errors.ForeignKeyViolation,
        )
        h.call(
            "storage-UUID-FK-rejected",
            c.execute,
            "UPDATE units SET table_uuid=%s WHERE id=%s",
            (bu, source_a),
            rejected=psycopg.errors.ForeignKeyViolation,
        )
    # Identical request/source labels and byte payloads remain independent across UUIDs.
    h.ready("items", "same", source_a)
    h.call(
        "unbound-adoption-rejected",
        h.x.adopt,
        *h.context("neighbor"),
        1,
        "same",
        h.token(),
        rejected="UNKNOWN_BOUND_REQUEST",
    )
    h.ready("items", "competitor", source_a, expected=0)
    h.ready("neighbor", "same", source_b)
    h.publish("neighbor", "same")
    original_receipt = h.publish("items", "same")
    h.call(
        "local-CAS-conflict", h.x.publish, *h.context("items"), 1, "competitor", h.token(), rejected="REVISION_CONFLICT"
    )
    h.call("cancel-CAS-loser", h.x.cancel, au, 1, "competitor", h.token())
    h.collect(au, "generation:" + h.x.request_key(au, 1, "competitor"))
    old_identity = h.capture("items", "old-view")
    h.capture("neighbor", "neighbor-sql", "sql", source_b)
    h.capture("neighbor", "neighbor-vector")
    # Advancing a request window cannot rebase an already prepared publication.
    h.ready("items", "expired", source_a)
    h.call("advance-with-prepared-build", h.m.advance, "items", au, 1, h.token(), model=lambda: h.model.advance(au, 1))
    h.call(
        "expired-publication-rejected",
        h.x.publish,
        *h.context("items"),
        1,
        "expired",
        h.token(),
        rejected="EXPIRED_REQUEST",
    )
    h.call("pending-build-blocks-retirement", h.m.retire, "items", au, h.token(), rejected="BUILD_IN_PROGRESS")
    h.call("cancel-expired-build", h.x.cancel, au, 1, "expired", h.token())
    h.call("uncollected-build-blocks-retirement", h.m.retire, "items", au, h.token(), rejected="BUILD_IN_PROGRESS")
    h.collect(au, "generation:" + h.x.request_key(au, 1, "expired"))
    h.ready("items", "same", source_a)
    h.publish("items", "same")
    h.mutate("items", "visible-update", [row(0, "changed"), row(99, "new")], [1])
    new_identity = h.capture("items", "new-view")
    require(
        len(old_identity["visible_source_rows"]) == 65
        and len(new_identity["visible_source_rows"]) == 63
        and len(new_identity["query"]["row_ids"]) == 65,
        "ReadView did not bind same-snapshot physical visibility",
    )
    # An error after named refs/head are staged must restore every relation.
    with h.b.connect() as c:
        c.execute(
            f"CREATE FUNCTION reject_bound_retire() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.table_uuid='{au}'::uuid AND NEW.end_snapshot IS NOT NULL THEN RAISE EXCEPTION 'bound-retire-abort'; END IF; RETURN NEW; END $$"
        )
        c.execute(
            "CREATE TRIGGER reject_bound_retire BEFORE UPDATE ON typed_tables FOR EACH ROW EXECUTE FUNCTION reject_bound_retire()"
        )
        h.call("atomic-retirement-rollback", h.m.retire, "items", au, h.token(), rejected=psycopg.errors.RaiseException)
        c.execute("DROP TRIGGER reject_bound_retire ON typed_tables")
        c.execute("DROP FUNCTION reject_bound_retire()")
    for mode in ("before_forward", "ack_lost"):
        before = (h.raw, h.typed)
        with P.CommitProxy("127.0.0.1", h.s.pg_port, mode) as proxy:
            routed = B.Backend(h.s.dsn + f" port={proxy.port}", h.b.schema, h.b.objects)
            proxy.armed.set()
            try:
                M.TypedMetadata(routed).retire("items", au, h.token())
            except B.CommitUnknown:
                pass
            else:
                raise AssertionError("Retirement COMMIT uncertainty was hidden")
            require(
                proxy.dropped.wait(5)
                and proxy.observation["injected"] == 1
                and proxy.observation["server_committed"] == (mode == "ack_lost"),
                "Retirement boundary was not observed",
            )
            report["faults"].append({"operation": "bound-retire", **proxy.observation})
        if mode == "ack_lost":
            h.model.retire(au)
        h.check()
        if mode == "before_forward":
            require((h.raw, h.typed) == before, "Unforwarded retirement changed state")
    h.call("retire-receipt-replay", h.m.retire, "items", au, h.token(), model=lambda: h.model.retire(au))
    h.call("retired-capture-rejected", h.x.capture, *h.context("items"), "retired-view", rejected="STALE_TABLE")
    h.call(
        "old-view-still-pinned",
        h.x.plan_gc,
        au,
        "generation:" + h.x.request_key(au, 1, "same"),
        h.token(),
        B.Budget(),
        rejected="REFERENCED",
    )
    old_copy = deepcopy(h.call("materialize-old-view-after-retire", h.x.materialize, "old-view"))
    new_copy = deepcopy(h.call("materialize-new-view-after-retire", h.x.materialize, "new-view"))
    require(
        len(old_copy["rows"]) == len(new_copy["rows"]) == 65
        and old_copy["rows"][0]["payload"] == "value"
        and new_copy["rows"][0]["payload"] == "changed",
        "Old/new payload snapshot was mixed",
    )
    h.call("unpin-old-copy", h.x.release_copy_pin, "old-view")
    h.call("unpin-new-copy", h.x.release_copy_pin, "new-view")
    h.collect(au, "generation:" + h.x.request_key(au, 1, "same"))
    h.collect(au, "generation:" + h.x.request_key(au, 2, "same"))
    h.call(
        "unrelated-SQL-reader-protects-source",
        h.x.expire_native,
        au,
        source_a,
        h.token(),
        rejected="ORDINARY_TRANSACTION",
    )
    h.call("end-unrelated-SQL-reader", h.x.close_view, "neighbor-sql")
    h.call("expire-retired-source", h.x.expire_native, au, source_a, h.token())
    # Delay an exact old-source DELETE across same-name recreation and publication.
    budget = B.Budget()
    h.call("plan-old-source-GC", h.x.plan_gc, au, source_a, h.token(), budget)
    key = next(o["key"] for o in h.raw["objects"] if o["unit"] == source_a)
    stop, data = h.call("old-source-STOP-ticket", h.x.stop, au, source_a, key, h.token(), budget)
    h.call("old-source-STOP", B.dispatch, stop, data)
    h.call("confirm-old-source-STOP", h.x.stop, au, source_a, key, h.token(), budget)
    deletion = h.call("old-source-DELETE-ticket", h.x.delete_payload, au, source_a, key, h.token(), budget)
    with HTTP.Fault(h.s.endpoint, "before_forward") as proxy, ThreadPoolExecutor(max_workers=1) as pool:
        pending = pool.submit(B.dispatch, proxy.route(deletion))
        require(proxy.entered.wait(10), "Old DELETE did not reach its handshake")
        try:
            recreated = h.create("items", (5, 17, 29, 41, 53, 67, 79, 97))
            nu = recreated["table_uuid"]
            new_source, _, _ = h.source("items")
            h.ready("items", "same", new_source)
            h.publish("items", "same")
        finally:
            proxy.release.set()
        pending.result(timeout=15)
        require(
            proxy.observation == {"requests": 1, "forwarded": True, "status": 204},
            "Old exact DELETE retried or hit wrong boundary",
        )
        report["faults"].append({"operation": "old-DELETE-across-recreated-UUID", **proxy.observation})
    h.check()
    h.call("finish-old-source-GC", h.x.finish_gc, au, source_a, h.token(), budget)
    h.call("late-old-source-PUT-rejected", B.dispatch, old_source_ticket, source_data, rejected="HTTP_412")
    require(
        h.x.copies["old-view"] == old_copy and h.x.copies["new-view"] == new_copy,
        "Canonical GC changed private view copies",
    )
    for label, fn, args in (
        ("stale-UUID-capture", h.x.capture, ("items", au, a["schema_hash"], "bad-old")),
        ("stale-UUID-publication", h.x.publish, ("items", au, a["schema_hash"], 1, "same", h.token())),
        (
            "new-UUID-cannot-adopt-old-source",
            h.x.prepare,
            (*h.context("items"), 1, "bad-source", source_a, b"index", 1, h.token()),
        ),
        ("new-UUID-cannot-GC-old-source", h.x.plan_gc, (nu, source_a, h.token(), B.Budget())),
    ):
        h.call(label, fn, *args, rejected="UNIT_BINDING_MISMATCH" if "cannot" in label else "STALE_TABLE")
    require(
        h.call("reconcile-original-retired-publication", h.x.reconcile, au, 1, "same", original_receipt["payload_hash"])
        == original_receipt,
        "Reconciliation rebased onto a new UUID",
    )
    # Cancellation and GC beat an index upload already received by the proxy.
    context = h.context("neighbor")
    h.call("prepare-late-cancel", h.x.prepare, *context, 1, "late", source_b, b"index", 1, h.token())
    ticket = h.call("late-index-ticket", h.x.payload_ticket, *context, 1, "late", "index", b"index", h.token())
    with HTTP.Fault(h.s.endpoint, "before_forward") as proxy, ThreadPoolExecutor(max_workers=1) as pool:
        pending = pool.submit(B.dispatch, proxy.route(ticket), b"index")
        require(proxy.entered.wait(10), "Late upload handshake missing")
        try:
            h.call("cancel-bound-upload", h.x.cancel, bu, 1, "late", h.token())
            h.collect(bu, "generation:" + h.x.request_key(bu, 1, "late"))
        finally:
            proxy.release.set()
        try:
            pending.result(timeout=15)
        except B.ContractError as error:
            require(str(error) == "HTTP_412", "Late bound upload was not rejected")
        else:
            raise AssertionError("Canceled bound upload resurrected")
        report["faults"].append({"operation": "canceled-bound-upload-after-GC", **proxy.observation})
    h.check()
    # Roll back a real staged head/reference publication.
    h.ready("neighbor", "rollback", source_b)
    reqkey = h.x.request_key(bu, 1, "rollback")
    with h.b.connect() as c:
        c.execute(
            f"CREATE FUNCTION reject_bound_publish() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.id='{reqkey}' AND NEW.state='committed' THEN RAISE EXCEPTION 'bound-publish-abort'; END IF; RETURN NEW; END $$"
        )
        c.execute(
            "CREATE TRIGGER reject_bound_publish BEFORE UPDATE ON requests FOR EACH ROW EXECUTE FUNCTION reject_bound_publish()"
        )
        h.call(
            "atomic-bound-publication-rollback",
            h.x.publish,
            *context,
            1,
            "rollback",
            h.token(),
            rejected=psycopg.errors.RaiseException,
        )
        c.execute("DROP TRIGGER reject_bound_publish ON requests")
        c.execute("DROP FUNCTION reject_bound_publish()")
    h.publish("neighbor", "rollback")
    h.ready("items", "restart", new_source)
    before = deepcopy((h.raw, h.typed))
    h.s.restart()
    h.check()
    require((h.raw, h.typed) == before and h.s.crash_exit_codes == [-9, -9], "Service crash lost bound state")
    writer = B.Backend(h.s.dsn, h.b.schema, h.b.objects)
    metadata = M.TypedMetadata(writer)
    bound = X.BoundCatalog(writer, metadata)
    old_token = h.token()
    h.b, h.m, h.x = writer, metadata, bound
    h.reader_processes.append(bound)
    h.call("owner-transfer-after-restart", writer.takeover, old_token)
    h.call("old-owner-fenced", bound.adopt, *h.context("items"), 1, "restart", old_token, rejected="FENCED")
    h.call("explicit-adopt-original-binding", bound.adopt, *h.context("items"), 1, "restart", h.token())
    h.publish("items", "restart")
    require(h.copies()["old-view"] == old_copy, "Owner transfer revoked independent reader copy")
    report["faults"].append({"operation": "service-crash-owner-transfer", "exit_codes": h.s.crash_exit_codes})
    for mode in ("before_forward", "ack_lost"):
        request = "commit-" + mode.replace("_", "-")
        h.ready("items", request, new_source)
        before = deepcopy((h.raw, h.typed))
        with P.CommitProxy("127.0.0.1", h.s.pg_port, mode) as proxy:
            routed = B.Backend(h.s.dsn + f" port={proxy.port}", h.b.schema, h.b.objects)
            temporary = X.BoundCatalog(routed, M.TypedMetadata(routed))
            proxy.armed.set()
            try:
                temporary.publish(*h.context("items"), 1, request, h.token())
            except B.CommitUnknown:
                pass
            else:
                raise AssertionError("Bound publication uncertainty was hidden")
            require(
                proxy.dropped.wait(5)
                and proxy.observation["injected"] == 1
                and proxy.observation["server_committed"] == (mode == "ack_lost"),
                "Bound COMMIT boundary missing",
            )
            report["faults"].append({"operation": "bound-publish", **proxy.observation})
        h.check()
        if mode == "before_forward":
            require((h.raw, h.typed) == before, "Unforwarded publication changed state")
        receipt = h.publish("items", request)
        require(h.publish("items", request) == receipt, "Publication retry changed its original receipt")
    h.capture("items", "current-view")
    h.call("current-view-still-readable", h.x.materialize, "current-view")
    h.call("neighbor-old-view-still-readable", h.x.materialize, "neighbor-vector")
    h.call("close-old-independent-copy", h.reader_processes[0].close_view, "old-view")
    h.call("close-new-independent-copy", h.reader_processes[0].close_view, "new-view")


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
    original = os.sched_getaffinity(0)
    cpus = {int(v) for v in args.cpus.split(",")}
    require(cpus and cpus <= original, "Requested CPU set unavailable")
    threads = {n: os.environ.get(n) for n in THREADS}
    sources = [Path(__file__), *[Path(m.__file__) for m in (B, M, X, O, T, BASE, S, P, HTTP)]]
    hashes = {str(p): B.sha(p.read_bytes()) for p in sources}
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
            # A deliberately unrelated storage seed proves identities come from typed tables.
            backend.initialize(str(uuid.uuid4()), str(uuid.uuid4()))
            metadata = M.TypedMetadata(backend)
            metadata.initialize(1)
            bound = X.BoundCatalog(backend, metadata)
            services.root_client.put_object(Bucket=services.bucket, Key="foreign/neighbor", Body=b"must-remain")
            h = Checks(services, backend, metadata, bound, report)
            exercise(h, report)
            report["final_state"] = {"storage": h.raw, "typed": h.typed}
        require(threads == {n: os.environ.get(n) for n in THREADS}, "Thread settings changed")
        require(hashes == {str(p): B.sha(p.read_bytes()) for p in sources}, "Executed source changed")
        report["status"] = "passed"
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
