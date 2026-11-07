#!/usr/bin/env python3
"""Deterministic typed PostgreSQL metadata and UUID/epoch lifecycle contracts."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
import hashlib
import importlib.metadata
import importlib.util
import json
import math
import os
from pathlib import Path
import struct
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


B = load("typed_broker", HERE.parents[1] / "scripts/vortex_remote_backend.py")
M = load("typed_metadata", HERE.parents[1] / "scripts/vortex_postgres_metadata.py")
O = load("typed_oracle", HERE / "vortex_postgres_metadata_oracle.py")
S = load("typed_services", HERE / "vortex_remote_services.py")
P = load("typed_commit_proxy", HERE / "postgres_commit_proxy.py")
BASE = load("publication_oracle", HERE / "vortex_remote_backend_oracle.py")
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
    "scope": "R6 typed component; native codecs and SQL guards unchanged",
    "acceptance": "Exact typed/reference state and original publication/object state; no timing thresholds",
    "epoch_storage": "NUMERIC(20,0), positive uint64",
    "receipt_limit": 100000,
    "faults": "COMMIT before-forward/ACK loss for mutation, epoch advance and retirement; staged SQL errors; lock handshakes; owned SIGKILL",
}


def require(value, message):
    if not value:
        raise AssertionError(message)


def wait_until(check):
    end = time.monotonic() + 30
    while not check():
        require(time.monotonic() < end, "Fixed database handshake not observed")
        time.sleep(0.02)


def row(key, vector=None, **overrides):
    result = {
        "id": key,
        "embedding": [0.1, -0.0] if vector is None else vector,
        "category": "A\0中文'é",
        "payload": "",
        "f32": 2**-149,
        "f64": 0.1,
        "data": b"\0\xff\x80'\\",
        "numbers": [None, -0.0, 0.1],
    }
    result.update(overrides)
    return result


class Checks:
    def __init__(self, services, backend, metadata, report):
        self.s, self.b, self.m, self.report = services, backend, metadata, report
        self.model = O.Model()
        self.original = BASE.observe(services.dsn, backend.schema, services.root_client, services.bucket)
        BASE.inspect(self.original)
        self.image = O.observe(backend)
        self.ids = {}
        O.inspect(self.image, self.model)

    def token(self):
        return self.b.state()["owner"]

    def check(self):
        self.image = O.observe(self.b)
        O.inspect(self.image, self.model)
        current = BASE.observe(self.s.dsn, self.b.schema, self.s.root_client, self.s.bucket)
        require(current == self.original, "Typed operation changed the #92 publication/reference/object inventory")

    def call(self, label, fn, *args, model=None, rejected=None, **kwargs):
        before = self.image
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
            require(self.image == before, "Rejected typed operation changed committed state")
            self.report["checks"].append(
                {"name": label, "rejected": rejected.__name__ if isinstance(rejected, type) else rejected}
            )
            return None
        require(rejected is None, "Expected rejection: " + label)
        if model is not None:
            expected = model()
            if expected is not None:
                require(result == expected, "API result differs from independent model: " + label)
        self.check()
        self.report["checks"].append(
            {"name": label, "state_sha256": B.sha(json.dumps(self.image, sort_keys=True).encode())}
        )
        if len(self.report["checks"]) % 20 == 0:
            print(f"PASS: {len(self.report['checks'])} typed checks; {label}", flush=True)
        return result

    def create(self, name, dimension=2, ids=tuple(range(1, 9))):
        identity = str(uuid.uuid4())
        result = self.call(
            "create-" + name,
            self.m.create,
            name,
            identity,
            dimension,
            self.token(),
            ids,
            model=lambda: self.model.create(name, identity, dimension, ids),
        )
        self.ids[name] = identity
        require(result["schema_hash"] == self.model.tables[identity]["schema_hash"], "Schema hash mismatch")
        return identity

    def mutate(self, name, request, rows, deleted=(), epoch=None):
        identity = self.ids[name]
        table = self.model.tables[identity]
        epoch = table["request_epoch"] if epoch is None else epoch
        return self.call(
            "mutate-" + request,
            self.m.mutate,
            name,
            identity,
            epoch,
            request,
            rows,
            list(deleted),
            table["schema_hash"],
            self.token(),
            model=lambda: self.model.mutate(identity, epoch, request, rows, list(deleted)),
        )

    def advance(self, name, previous=None):
        identity = self.ids[name]
        previous = self.model.tables[identity]["request_epoch"] if previous is None else previous
        return self.call(
            "advance-" + name,
            self.m.advance,
            name,
            identity,
            previous,
            self.token(),
            model=lambda: self.model.advance(identity, previous),
        )

    def retire(self, name):
        identity = self.ids[name]
        return self.call(
            "retire-" + name, self.m.retire, name, identity, self.token(), model=lambda: self.model.retire(identity)
        )

    def read(self, name, identity=None, snapshot=None):
        identity = self.ids[name] if identity is None else identity
        actual = self.call(
            "read-" + name,
            self.m.read if snapshot is None else self.m.read_at,
            *((name, identity) if snapshot is None else (identity, snapshot)),
        )
        expected = [
            r
            for r in self.model.rows
            if r["table_uuid"] == identity
            and (
                (r["end_snapshot"] is None)
                if snapshot is None
                else (r["begin_snapshot"] <= snapshot and (r["end_snapshot"] is None or r["end_snapshot"] > snapshot))
            )
        ]
        expected = sorted(expected, key=lambda r: r["id"])
        projection = [{**O.raw(r), "physical_row_id": r["physical_row_id"]} for r in actual]
        require(
            projection == [{k: r[k] for k in (*O.FIELDS, "physical_row_id")} for r in expected],
            "Binary API read changed typed bits or NULLs",
        )
        return actual


def typed_cases(h, report):
    a, neighbor = h.create("items", ids=(3, 11, 24, 37, 48, 59, 70, 81)), h.create("neighbor")
    initial = [
        row(-(2**63)),
        row(
            2**63 - 1, [2**-149, -(2**-149)], category=None, payload=None, f32=-0.0, f64=2**-1074, data=b"", numbers=[]
        ),
        row(
            7,
            [3.4028234663852886e38, -3.4028234663852886e38],
            category="",
            payload="\0",
            f32=None,
            f64=None,
            data=None,
            numbers=None,
        ),
    ]
    receipt = h.mutate("items", "ieee-null-blob", initial)
    require(
        h.model.tables[a]["schema_hash"] == "275033cd1b8b6867acf4d8c0e3825a8fa1cb245806626ee2341a8d693e4fb6cc"
        and receipt["payload_hash"] == "7cc8a1ee2223fa3b810cf8a0842976765d03582accf02d4ecea5919479ecc0ef",
        "Fixed binary schema/request golden changed",
    )
    require(
        receipt["payload_hash"] == O.request_digest(initial, [], h.model.tables[a]["schema_hash"])[0],
        "Canonical payload hash drifted",
    )
    h.mutate("neighbor", "same-id", [row(1)])
    h.read("items")
    first = next(r for r in h.image["rows"] if r["id"] == -(2**63))
    require(
        first["embedding"] == ["3dcccccd", "80000000"]
        and first["f32"] == "00000001"
        and first["f64"] == "3fb999999999999a",
        "Fixed IEEE golden words changed",
    )
    with h.b.connect() as c:
        types = c.execute(
            """SELECT attname,atttypid FROM pg_attribute WHERE attrelid='typed_rows'::regclass AND attnum>0 ORDER BY attnum"""
        ).fetchall()
        actual = {r["attname"]: r["atttypid"] for r in types}
        require(
            {k: actual[k] for k in ("id", "embedding", "category", "payload", "f32", "f64", "data", "numbers")}
            == {
                "id": 20,
                "embedding": 1021,
                "category": 17,
                "payload": 17,
                "f32": 700,
                "f64": 701,
                "data": 17,
                "numbers": 1021,
            },
            "Unexpected physical PG types",
        )
        require(
            c.execute(
                "SELECT atttypid FROM pg_attribute WHERE attrelid='typed_tables'::regclass AND attname='request_epoch'"
            ).fetchone()["atttypid"]
            == 1700,
            "Epoch is not exact NUMERIC",
        )
    report["physical_types"] = actual
    h.mutate("items", "ieee-null-blob", list(reversed(initial)))
    changed = [row(5, [1.0, 2.0], category="next", payload="after", f32=0.1, f64=-0.0, numbers=[None])]
    old_snapshot = h.model.snapshot
    h.mutate("items", "replace-delete", changed, [7])
    h.read("items", snapshot=old_snapshot)
    h.mutate("items", "ieee-null-blob", initial)
    h.read("items")
    h.mutate("items", "missing-delete", [], [123456])
    for dimension in (1, 3, 4096):
        name = "dimension-" + str(dimension)
        h.create(name, dimension)
        h.mutate(name, "roundtrip", [row(1, [float(i % 7) for i in range(dimension)])])
        h.read(name)

    # Rejections use exactly the same committed inventory before and after.
    table = h.model.tables[a]

    def reject(label, data, deleted=(), epoch=1, identity=a, schema=None, code="NONFINITE_FLOAT"):
        return h.call(
            label,
            h.m.mutate,
            "items",
            identity,
            epoch,
            label,
            data,
            list(deleted),
            table["schema_hash"] if schema is None else schema,
            h.token(),
            rejected=code,
        )

    for value in (math.nan, math.inf, -math.inf):
        reject("nonfinite-vector-" + str(value), [row(2, [value, 0.0])])
        reject("nonfinite-scalar-" + str(value), [row(2, f64=value)])
    reject("float32-overflow", [row(2, [3.5e38, 0.0])], code="FLOAT_RANGE")
    for value in (None, [], [None, 1], [[0.0], [1.0]], [0.0]):
        reject(
            "malformed-vector",
            [row(2, value)] if value is not None else [dict(row(2), embedding=None)],
            code="BAD_LIST" if value is None else "DIMENSION_MISMATCH" if len(value) != 2 else "BAD_FLOAT",
        )
    reject("field-id-binding", [row(2)], schema="00" * 32, code="STALE_SCHEMA")
    reject("wrong-uuid", [row(2)], identity=neighbor, code="STALE_TABLE")
    reject("future-epoch", [row(2)], epoch=2, code="FUTURE_REQUEST")
    reject("duplicate-id", [row(2), row(2)], code="DUPLICATE_ID")
    reject("upsert-delete-overlap", [row(2)], [2], code="DUPLICATE_ID")
    reject("invalid-utf8", [row(2, payload="\ud800")], code="BAD_UTF8")
    reject("NUL-request-id\0", [row(2)], code="BAD_REQUEST_ID")
    reject("empty-batch", [], code="OPERATION_BUDGET")
    reject("1001-operations", [], list(range(1001)), code="OPERATION_BUDGET")
    reject("over-4MiB", [row(2, data=b"x" * (4 * 1024**2))], code="BATCH_BYTE_BUDGET")
    # Null vs empty and signed-zero changes must not masquerade as request retries.
    h.call(
        "same-id-changed-payload",
        h.m.mutate,
        "items",
        a,
        1,
        "ieee-null-blob",
        [row(1)],
        [],
        table["schema_hash"],
        h.token(),
        rejected="PAYLOAD_MISMATCH",
    )
    for name, altered in (
        ("NULL-vs-empty-retry", [dict(r, numbers=[]) if r["id"] == 7 else r for r in initial]),
        ("signed-zero-retry", [dict(r, embedding=[0.1, 0.0]) if r["id"] == -(2**63) else r for r in initial]),
    ):
        h.call(
            name,
            h.m.mutate,
            "items",
            a,
            1,
            "ieee-null-blob",
            altered,
            [],
            table["schema_hash"],
            h.token(),
            rejected="PAYLOAD_MISMATCH",
        )
    exact = row(2048, data=b"")
    _, overhead = O.request_digest([exact], [], table["schema_hash"])
    exact["data"] = b"z" * (4 * 1024**2 - overhead)
    require(
        O.request_digest([exact], [], table["schema_hash"])[1] == 4 * 1024**2, "Independent exact-byte fixture is wrong"
    )
    h.mutate("items", "exact-4MiB", [exact])
    with h.b.connect() as c:
        for statement in (
            "UPDATE typed_rows SET embedding=ARRAY[0.0::real,NULL] WHERE id=5 AND end_snapshot IS NULL",
            "UPDATE typed_rows SET embedding='[0:1]={0,1}'::real[] WHERE id=5 AND end_snapshot IS NULL",
            "UPDATE typed_rows SET f32='NaN'::real WHERE id=5 AND end_snapshot IS NULL",
            "UPDATE typed_tables SET request_epoch=0 WHERE end_snapshot IS NULL",
            "UPDATE typed_tables SET request_epoch=18446744073709551616 WHERE end_snapshot IS NULL",
            "UPDATE typed_tables SET request_epoch=NULL WHERE end_snapshot IS NULL",
        ):
            h.call("direct-SQL-type-constraint", c.execute, statement, rejected=psycopg.errors.CheckViolation)
    return a, neighbor


def epoch_cases(h):
    a = h.ids["items"]
    old_snapshot = h.model.snapshot
    before = h.m.status("neighbor", h.ids["neighbor"])
    advanced = h.advance("items")
    h.mutate("items", "in-new-window", [row(22)])
    require(h.advance("items", 1) == advanced, "Advance retry erased new-window receipts")
    h.read("items", snapshot=old_snapshot)
    for request in ("ieee-null-blob", "never-committed"):
        h.call(
            "expired-" + request,
            h.m.mutate,
            "items",
            a,
            1,
            request,
            [row(1)],
            [],
            h.model.tables[a]["schema_hash"],
            h.token(),
            rejected="EXPIRED_REQUEST",
        )
    h.advance("items")
    h.call("old-advance-is-not-rebased", h.m.advance, "items", a, 1, h.token(), rejected="EPOCH_CONFLICT")
    require(
        h.m.status("neighbor", h.ids["neighbor"])["retained_requests"] == before["retained_requests"],
        "Neighbor window changed",
    )
    h.create("unsigned-limit")
    identity = h.ids["unsigned-limit"]
    # Fixture seed reaches an otherwise impractical epoch; the advance itself is real.
    with h.b.transaction(h.token()) as (c, _):
        c.execute(
            "UPDATE typed_tables SET request_epoch=%s WHERE table_uuid=%s", (Decimal(2**64 - 2), uuid.UUID(identity))
        )
    h.model.tables[identity]["request_epoch"] = 2**64 - 2
    h.check()
    h.advance("unsigned-limit")
    h.mutate("unsigned-limit", "at-uint64-max", [row(1)])
    h.advance("unsigned-limit", 2**64 - 2)
    h.call(
        "uint64-exhausted", h.m.advance, "unsigned-limit", identity, 2**64 - 1, h.token(), rejected="RESOURCE_EXHAUSTED"
    )
    h.call("epoch-overflow", h.m.advance, "unsigned-limit", identity, 2**64, h.token(), rejected="BAD_EPOCH")
    for bad in (0, -1, 1.5, True):
        h.call(
            "epoch-must-be-positive-integer",
            h.m.advance,
            "unsigned-limit",
            identity,
            bad,
            h.token(),
            rejected="BAD_EPOCH",
        )


def commit_faults(h, report):
    for operation in ("mutate", "advance", "retire"):
        for mode in ("before_forward", "ack_lost"):
            name = operation + "-" + mode.replace("_", "-")
            identity = h.create(name)
            h.mutate(name, "before", [row(1)])
            before = h.image

            def action(metadata):
                if operation == "mutate":
                    return metadata.mutate(
                        name,
                        identity,
                        1,
                        "uncertain",
                        [row(2)],
                        [1],
                        h.model.tables[identity]["schema_hash"],
                        h.token(),
                    )
                if operation == "advance":
                    return metadata.advance(name, identity, 1, h.token())
                return metadata.retire(name, identity, h.token())

            def expected():
                if operation == "mutate":
                    return h.model.mutate(identity, 1, "uncertain", [row(2)], [1])
                if operation == "advance":
                    return h.model.advance(identity, 1)
                return h.model.retire(identity)

            with P.CommitProxy("127.0.0.1", h.s.pg_port, mode) as proxy:
                routed = B.Backend(h.s.dsn + f" port={proxy.port}", h.b.schema, h.b.objects)
                proxy.armed.set()
                try:
                    action(M.TypedMetadata(routed))
                except B.CommitUnknown:
                    pass
                else:
                    raise AssertionError("Typed COMMIT uncertainty was hidden")
                require(
                    proxy.dropped.wait(5)
                    and proxy.observation["injected"] == 1
                    and proxy.observation["server_committed"] == (mode == "ack_lost"),
                    "Typed COMMIT boundary was not observed",
                )
                report["faults"].append({"operation": operation, **proxy.observation})
            if mode == "ack_lost":
                expected()
            h.check()
            if mode == "before_forward":
                require(h.image == before, "Unforwarded COMMIT changed typed metadata")
            # Explicit retry must match the original operation, never a new UUID/window.
            result = action(h.m)
            require(result == expected(), "Uncertain operation retried with a changed effect")
            h.check()
            require(action(h.m) == result, "Repeated typed retry was not idempotent")
            h.check()


def rollback_cases(h):
    for operation in ("mutate", "advance", "retire"):
        name = "rollback-" + operation
        identity = h.create(name)
        h.mutate(name, "before", [row(1)])
        with h.b.connect() as c:
            if operation == "mutate":
                target, event, condition = "typed_receipts", "INSERT", "NEW.request_id=decode('6661696c','hex')"
            elif operation == "advance":
                target, event, condition = "typed_tables", "UPDATE", "NEW.request_epoch=2"
            else:
                target, event, condition = "typed_tables", "UPDATE", "NEW.end_snapshot IS NOT NULL"
            c.execute(
                f"CREATE FUNCTION typed_abort() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF {condition} THEN RAISE EXCEPTION 'typed-fixture-abort'; END IF; RETURN NEW; END $$"
            )
            c.execute(
                f"CREATE TRIGGER typed_abort BEFORE {event} ON {target} FOR EACH ROW EXECUTE FUNCTION typed_abort()"
            )
            if operation == "mutate":
                h.call(
                    "rollback-staged-rows-and-receipt",
                    h.m.mutate,
                    name,
                    identity,
                    1,
                    "fail",
                    [row(2)],
                    [1],
                    h.model.tables[identity]["schema_hash"],
                    h.token(),
                    rejected=psycopg.errors.RaiseException,
                )
            elif operation == "advance":
                h.call(
                    "rollback-pruned-receipts-and-epoch",
                    h.m.advance,
                    name,
                    identity,
                    1,
                    h.token(),
                    rejected=psycopg.errors.RaiseException,
                )
            else:
                h.call(
                    "rollback-retirement-and-UUID-state",
                    h.m.retire,
                    name,
                    identity,
                    h.token(),
                    rejected=psycopg.errors.RaiseException,
                )
            c.execute(f"DROP TRIGGER typed_abort ON {target}")
            c.execute("DROP FUNCTION typed_abort()")


def retirement_cases(h):
    identity = h.create("same-name")
    h.mutate("same-name", "same-request", [row(1)])
    snapshot = h.model.snapshot
    # A real old PostgreSQL transaction keeps its tuple snapshot across retirement/recreation.
    reader = h.b.connect()
    try:
        reader.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        query = "SELECT encode(payload,'hex') AS payload FROM typed_rows WHERE table_uuid=%s AND end_snapshot IS NULL"
        before = reader.execute(query, (uuid.UUID(identity),)).fetchall()
        result = h.retire("same-name")
        require(h.retire("same-name") == result, "Retirement replay changed the snapshot")
        h.read("same-name", identity, snapshot)
        new = h.create("same-name", ids=(5, 17, 29, 41, 53, 67, 79, 97))
        h.mutate("same-name", "same-request", [row(3, payload="new-UUID")])
        h.call(
            "new-UUID-rejects-old-field-binding",
            h.m.mutate,
            "same-name",
            new,
            1,
            "old-fields",
            [row(1)],
            [],
            h.model.tables[identity]["schema_hash"],
            h.token(),
            rejected="STALE_SCHEMA",
        )
        for label, fn, args in (
            (
                "stale-write",
                h.m.mutate,
                (
                    "same-name",
                    identity,
                    1,
                    "same-request",
                    [row(1)],
                    [],
                    h.model.tables[identity]["schema_hash"],
                    h.token(),
                ),
            ),
            ("stale-advance", h.m.advance, ("same-name", identity, 1, h.token())),
            ("stale-retire", h.m.retire, ("same-name", identity, h.token())),
        ):
            h.call(label, fn, *args, rejected="STALE_TABLE")
        require(
            reader.execute(query, (uuid.UUID(identity),)).fetchall() == before,
            "Old PostgreSQL reader lost its snapshot",
        )
        h.read("same-name", identity, snapshot)
        require(new != identity, "Recreated table reused UUID")
    finally:
        reader.close()


def concurrency(h, report):
    name, identity = "ordered-epoch", h.create("ordered-epoch")
    h.mutate(name, "initial", [row(1)])
    with h.b.connect() as gate:
        gate.execute(
            "CREATE FUNCTION typed_hold() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.request_id=decode('6f726465726564','hex') THEN PERFORM pg_advisory_xact_lock(932107); END IF; RETURN NEW; END $$"
        )
        gate.execute(
            "CREATE TRIGGER typed_hold BEFORE INSERT ON typed_receipts FOR EACH ROW EXECUTE FUNCTION typed_hold()"
        )
        gate.execute("SELECT pg_advisory_lock(932107)")
        first = M.TypedMetadata(B.Backend(h.s.dsn + " application_name=typed-first", h.b.schema, h.b.objects))
        second = M.TypedMetadata(B.Backend(h.s.dsn + " application_name=typed-second", h.b.schema, h.b.objects))

        def blocked(label):
            return gate.execute(
                "SELECT 1 FROM pg_stat_activity WHERE application_name=%s AND wait_event_type='Lock'", (label,)
            ).fetchone()

        with ThreadPoolExecutor(max_workers=2) as pool:
            write = pool.submit(
                first.mutate,
                name,
                identity,
                1,
                "ordered",
                [row(2)],
                [],
                h.model.tables[identity]["schema_hash"],
                h.token(),
            )
            try:
                wait_until(lambda: blocked("typed-first"))
                advance = pool.submit(second.advance, name, identity, 1, h.token())
                wait_until(lambda: blocked("typed-second"))
                require(O.observe(h.b) == h.image, "Uncommitted typed state leaked")
            finally:
                gate.execute("SELECT pg_advisory_unlock(932107)")
            require(
                write.result(timeout=15) == h.model.mutate(identity, 1, "ordered", [row(2)], []),
                "First mutation lost its order",
            )
            require(
                advance.result(timeout=15) == h.model.advance(identity, 1),
                "Epoch did not include the serialized request",
            )
        gate.execute("DROP TRIGGER typed_hold ON typed_receipts")
        gate.execute("DROP FUNCTION typed_hold()")
    h.check()
    h.call(
        "ordered-old-retry-expired",
        h.m.mutate,
        name,
        identity,
        1,
        "ordered",
        [row(2)],
        [],
        h.model.tables[identity]["schema_hash"],
        h.token(),
        rejected="EXPIRED_REQUEST",
    )
    report["faults"].append({"operation": "concurrent-mutate-advance", "partial_visible": False, "expired_requests": 2})


def quota(h, report):
    name, identity = "quota", h.create("quota")
    receipt = h.mutate(name, "real-retry", [], [123])
    current = len(h.model.receipts)
    count = 100000 - current
    generated = []
    for n in range(count):
        request = "quota-" + str(n)
        entry = {
            "table_uuid": identity,
            "request_epoch": 1,
            "request_id": request,
            "payload_hash": O.request_digest([], [n], h.model.tables[identity]["schema_hash"])[0],
            "row_count": 1,
            "snapshot_id": h.model.snapshot,
        }
        generated.append(entry)
    with h.b.transaction(h.token()) as (c, _):
        with c.cursor().copy("COPY typed_receipts FROM STDIN") as stream:
            for r in generated:
                stream.write_row(
                    (
                        uuid.UUID(identity),
                        Decimal(1),
                        r["request_id"].encode(),
                        bytes.fromhex(r["payload_hash"]),
                        1,
                        r["snapshot_id"],
                    )
                )
    for r in generated:
        h.model.receipts[(identity, 1, r["request_id"])] = r
    h.check()
    require(h.m.status(name, identity)["catalog_requests"] == 100000, "Catalog quota fixture is not full")
    require(h.mutate(name, "real-retry", [], [123]) == receipt, "Retry at full capacity was refused")
    h.mutate(name, "quota-0", [], [0])
    h.call(
        "new-request-at-capacity",
        h.m.mutate,
        name,
        identity,
        1,
        "over",
        [],
        [321],
        h.model.tables[identity]["schema_hash"],
        h.token(),
        rejected="REQUEST_BUDGET",
    )
    other = h.ids["items"]
    h.call(
        "quota-is-catalog-wide",
        h.m.mutate,
        "items",
        other,
        h.model.tables[other]["request_epoch"],
        "other-over",
        [],
        [321],
        h.model.tables[other]["schema_hash"],
        h.token(),
        rejected="REQUEST_BUDGET",
    )
    h.advance(name)
    h.mutate(name, "after-release", [row(1)])
    h.retire(name)
    report["capacity"] = {
        "seeded_receipts": count,
        "catalog_limit": 100000,
        "retry_at_full": True,
        "advance_and_retire_release": True,
    }


def kill_worker(h, report, root):
    name, identity = "killed-writer", h.create("killed-writer")
    h.mutate(name, "initial", [row(1)])
    config = {
        "dsn": h.s.dsn + " application_name=typed-killed",
        "schema": h.b.schema,
        "endpoint": h.s.endpoint,
        "bucket": h.s.bucket,
        "prefix": h.b.objects.prefix,
        "name": name,
        "table_uuid": identity,
        "schema_hash": h.model.tables[identity]["schema_hash"],
        "token": h.token(),
        "affinity_file": str(root / "worker-affinity.json"),
    }
    config_path = root / "worker.json"
    config_path.write_text(json.dumps(config))
    with h.b.connect() as gate:
        gate.execute(
            "CREATE FUNCTION kill_hold() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.request_id=decode('6b696c6c6564','hex') THEN PERFORM pg_advisory_xact_lock(932108); END IF; RETURN NEW; END $$"
        )
        gate.execute(
            "CREATE TRIGGER kill_hold BEFORE INSERT ON typed_receipts FOR EACH ROW EXECUTE FUNCTION kill_hold()"
        )
        gate.execute("SELECT pg_advisory_lock(932108)")
        with (root / "worker.log").open("wb") as log:
            process = subprocess.Popen(
                [sys.executable, "-I", str(Path(__file__).resolve()), "--worker", str(config_path)],
                stdout=log,
                stderr=log,
                env=dict(os.environ, R6_TYPED_S3_SECRET=h.s.secret),
            )
            try:
                wait_until(
                    lambda: gate.execute(
                        "SELECT 1 FROM pg_stat_activity WHERE application_name='typed-killed' AND wait_event='advisory'"
                    ).fetchone()
                )
                require(O.observe(h.b) == h.image, "Staged rows leaked before worker kill")
                process.kill()
                require(process.wait(timeout=10) == -9, "Worker was not actually SIGKILLed")
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=10)
                gate.execute("SELECT pg_advisory_unlock(932108)")
            wait_until(
                lambda: not gate.execute(
                    "SELECT 1 FROM pg_stat_activity WHERE application_name='typed-killed'"
                ).fetchone()
            )
        gate.execute("DROP TRIGGER kill_hold ON typed_receipts")
        gate.execute("DROP FUNCTION kill_hold()")
    h.check()
    cpus = json.loads((root / "worker-affinity.json").read_text())
    require(cpus == sorted(os.sched_getaffinity(0)), "Worker CPU affinity changed")
    h.mutate(name, "killed", [row(42)], [1])
    report["faults"].append({"operation": "staged-mutation-worker-SIGKILL", "exit_code": -9, "affinity": cpus})


def worker(path):
    cfg = json.loads(path.read_text())
    Path(cfg["affinity_file"]).write_text(json.dumps(sorted(os.sched_getaffinity(0))))
    objects = B.S3Objects(
        S.client(cfg["endpoint"], "r6-admin", os.environ["R6_TYPED_S3_SECRET"]), cfg["bucket"], cfg["prefix"]
    )
    metadata = M.TypedMetadata(B.Backend(cfg["dsn"], cfg["schema"], objects))
    metadata.mutate(cfg["name"], cfg["table_uuid"], 1, "killed", [row(42)], [1], cfg["schema_hash"], cfg["token"])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker", type=Path)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--pg-bin", type=Path)
    parser.add_argument("--minio", type=Path)
    parser.add_argument("--object-data-dir", type=Path)
    parser.add_argument("--cpus")
    args = parser.parse_args()
    if args.worker is not None:
        worker(args.worker)
        return
    require(
        all((args.output_dir, args.pg_bin, args.minio, args.object_data_dir, args.cpus)),
        "Missing explicit service / output / CPU arguments",
    )
    root = args.output_dir.resolve()
    root.mkdir(mode=0o700)
    original = os.sched_getaffinity(0)
    cpus = {int(x) for x in args.cpus.split(",")}
    require(cpus and cpus <= original, "Requested CPU set unavailable")
    threads = {n: os.environ.get(n) for n in THREADS}
    sources = [
        Path(__file__),
        Path(B.__file__),
        Path(M.__file__),
        Path(O.__file__),
        Path(S.__file__),
        Path(P.__file__),
        Path(BASE.__file__),
    ]
    hashes = {str(p): B.sha(p.read_bytes()) for p in sources}
    report = {
        "status": "running",
        "rule": RULE,
        "limits": M.LIMITS,
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
            require(
                all(v == sorted(cpus) for v in report["services"]["affinity"].values()), "Service CPU affinity changed"
            )
            backend = B.Backend(
                services.dsn,
                "r6_" + uuid.uuid4().hex,
                B.S3Objects(services.root_client, services.bucket, "catalog/" + uuid.uuid4().hex + "/"),
            )
            backend.initialize(str(uuid.uuid4()), str(uuid.uuid4()))
            unit = backend.register_source("anchor", b"kept-source-bytes", "anchor-snapshot", 1)
            B.dispatch(backend.source_ticket(unit, b"kept-source-bytes", 1), b"kept-source-bytes")
            backend.seal_source(unit, 1)
            backend.prepare("anchor", unit, b"kept-index-bytes", 1, 0, str(backend.state()["table_uuid"]))
            for role, data in (("index", b"kept-index-bytes"), ("manifest", None)):
                data = backend.manifest("anchor", 1) if data is None else data
                B.dispatch(backend.payload_ticket("anchor", role, data, 1), data)
                backend.seal("anchor", role, 1)
            backend.publish("anchor", 1)
            backend.capture("anchor-reader")
            metadata = M.TypedMetadata(backend)
            metadata.initialize(1)
            h = Checks(services, backend, metadata, report)
            typed_cases(h, report)
            epoch_cases(h)
            commit_faults(h, report)
            rollback_cases(h)
            retirement_cases(h)
            concurrency(h, report)
            kill_worker(h, report, root)
            quota(h, report)
            before = h.image
            services.restart()
            require(services.crash_exit_codes == [-9, -9], "Services were not actually SIGKILLed")
            h.b = B.Backend(services.dsn, backend.schema, backend.objects)
            h.m = M.TypedMetadata(h.b)
            h.check()
            require(h.image == before, "Restart changed typed UUID / epoch / row history")
            report["faults"].append({"operation": "service-SIGKILL-restart", "exit_codes": services.crash_exit_codes})
            # Explicitly transfer owner; only this known field may change in the #92 baseline.
            old = h.token()
            h.b.takeover(old)
            h.original["control"][0]["owner"] = old + 1
            h.check()
            identity = h.ids["items"]
            h.call(
                "stale-owner-fenced",
                h.m.advance,
                "items",
                identity,
                h.model.tables[identity]["request_epoch"],
                old,
                rejected="FENCED",
            )
            h.mutate("items", "after-restart-owner", [row(99)])
            h.read("items")
            require(h.b.materialize("anchor-reader"), "Original pinned object bytes disappeared")
            h.check()
            report["final_state"] = h.image
            report["original_publication_sha256"] = B.sha(BASE.encode(h.original).encode())
        require(threads == {n: os.environ.get(n) for n in THREADS}, "Thread environment changed")
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
