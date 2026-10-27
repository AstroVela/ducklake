#!/usr/bin/env python3
"""Qualify native vector visibility across processes with a PostgreSQL catalog."""

from __future__ import annotations

import argparse
import csv
import importlib.util
import os
import subprocess
import tempfile
import time
import uuid
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    "native_recovery", Path(__file__).with_name("test_native_realtime_vector_recovery.py")
)
native = importlib.util.module_from_spec(spec)
spec.loader.exec_module(native)
sql_string = native.sql_string
require_equal = native.require_equal

RANK = (
    "SELECT id, payload, list_distance(embedding, [0, 0, 0]::FLOAT[]) AS distance "
    "FROM lake.items WHERE category = 'A' ORDER BY distance, id LIMIT 2"
)
INITIAL = [{"id": 1, "payload": "initial", "distance": 0}, {"id": 2, "payload": "delete", "distance": 1}]
GUARD = (
    "CREATE MACRO should_apply(incoming_id, incoming_hash) AS "
    "(SELECT CASE WHEN matches = 0 THEN true WHEN matches = 1 AND stored_hash = incoming_hash THEN false "
    "ELSE error('request_id payload mismatch or duplicate record') END "
    "FROM (SELECT count(*) AS matches, min(payload_hash) AS stored_hash "
    "FROM lake.requests WHERE request_id = incoming_id));"
)


class PostgresProbe(native.RecoveryProbe):
    def __init__(
        self,
        duckdb: Path,
        extension: Path | None,
        postgres_extension: Path | None,
        root: Path,
        dsn: str,
        row_limit: int = 100,
    ):
        super().__init__(duckdb, extension, root)
        self.schema = "vector_proof_" + uuid.uuid4().hex
        load_lake = "LOAD ducklake;" if extension is None else f"LOAD {sql_string(extension)};"
        load_pg = "LOAD postgres_scanner;" if postgres_extension is None else f"LOAD {sql_string(postgres_extension)};"
        self.driver_prefix = (
            "SET autoinstall_known_extensions = false;SET autoload_known_extensions = false;"
            "SET threads = 1;SET memory_limit = '512MiB';LOAD parquet;" + load_lake + load_pg
        )
        self.dsn = dsn
        self.prefix = (
            self.driver_prefix
            + f"ATTACH {sql_string('ducklake:postgres:' + dsn)} AS lake "
            + f"(DATA_PATH {sql_string(root / 'data')}, METADATA_SCHEMA {sql_string(self.schema)}, "
            + f"DATA_INLINING_ROW_LIMIT {row_limit});"
        )

    def cleanup(self) -> None:
        # Only this invocation's generated schema is removed.
        sql = (
            self.driver_prefix
            + f"ATTACH {sql_string(self.dsn)} AS pg (TYPE postgres);"
            + f"CALL postgres_execute('pg', {sql_string('DROP SCHEMA IF EXISTS ' + self.schema + ' CASCADE')});"
        )
        result = subprocess.run(self.command + [sql], capture_output=True, text=True, timeout=30)
        if result.returncode:
            raise AssertionError(f"failed to remove proof schema: {result.stderr}")


class SnapshotReader:
    def __init__(self, probe: PostgresProbe):
        self.probe = probe
        self.log = (probe.root / "reader.log").open("wb")
        self.process = subprocess.Popen(probe.command[:-1], stdin=subprocess.PIPE, stdout=self.log, stderr=self.log)

    def send(self, sql: str) -> None:
        self.process.stdin.write((sql + "\n").encode())
        self.process.stdin.flush()

    def copy_rank(self, name: str) -> str:
        return f"COPY ({RANK}) TO {sql_string(self.probe.root / (name + '.csv'))} (HEADER true);"

    def start(self) -> None:
        ready = self.probe.root / "reader.ready"
        self.send(self.probe.prefix + "BEGIN;" + self.copy_rank("before") + f"COPY (SELECT 1) TO {sql_string(ready)};")
        deadline = time.monotonic() + 30
        while not ready.exists():
            if self.process.poll() is not None or time.monotonic() >= deadline:
                raise AssertionError(
                    "reader did not establish its snapshot: " + (self.probe.root / "reader.log").read_text()
                )
            time.sleep(0.02)

    def finish(self) -> None:
        self.send(self.copy_rank("after") + "COMMIT;")
        self.process.stdin.close()
        require_equal(self.process.wait(timeout=30), 0, "snapshot reader exit")
        expected = [{k: str(v) for k, v in row.items()} for row in INITIAL]
        for name in ["before", "after"]:
            with (self.probe.root / (name + ".csv")).open() as handle:
                rows = list(csv.DictReader(handle))
            for row in rows:
                row["distance"] = str(int(float(row["distance"])))
            require_equal(rows, expected, "held reader " + name)

    def close(self) -> None:
        if self.process.poll() is None:
            self.process.kill()
            self.process.wait(timeout=10)
        self.log.close()


def mutation() -> str:
    return (
        GUARD
        + "BEGIN;"
        + "MERGE INTO lake.items AS target USING (SELECT * FROM (VALUES "
        + "(1, [9,0,0]::FLOAT[], 'B', 'moved'), (4, [0,1,0]::FLOAT[], 'A', 'new')) "
        + "AS incoming(id, embedding, category, payload) WHERE should_apply('r2', sha256('update-batch'))) AS source "
        + "ON target.id = source.id WHEN MATCHED THEN UPDATE SET embedding = source.embedding, "
        + "category = source.category, payload = source.payload "
        + "WHEN NOT MATCHED THEN INSERT VALUES (source.id, source.embedding, source.category, source.payload);"
        + "DELETE FROM lake.items WHERE id = 2 AND should_apply('r2', sha256('update-batch'));"
        + "INSERT INTO lake.requests SELECT 'r2', sha256('update-batch') WHERE should_apply('r2', sha256('update-batch'));"
        + "COMMIT;"
    )


def exercise(probe: PostgresProbe) -> None:
    probe.query(
        "CREATE TABLE lake.items(id BIGINT NOT NULL, embedding FLOAT[] NOT NULL, category VARCHAR NOT NULL, payload VARCHAR);"
        "CREATE TABLE lake.requests(request_id VARCHAR NOT NULL, payload_hash VARCHAR NOT NULL);"
        "BEGIN;"
        "INSERT INTO lake.items VALUES (1,[0,0,0],'A','initial'), (2,[1,0,0],'A','delete'), "
        "(3,[2,0,0],'A','stable'), (8,[0.125,-0.5,1.25],'C','business''quoted-中文');"
        "INSERT INTO lake.requests VALUES ('r1',sha256('initial-batch'));COMMIT;"
    )
    require_equal(probe.query(RANK), INITIAL, "initial exact retrieval")
    require_equal(
        probe.query("SELECT embedding, payload FROM lake.items WHERE id = 8;"),
        [{"embedding": [0.125, -0.5, 1.25], "payload": "business'quoted-中文"}],
        "FLOAT[] and business text round trip",
    )
    require_equal(
        probe.query("SELECT count(*) AS rows FROM lake.items WHERE filename LIKE 'ducklake_inlined_data_%';"),
        [{"rows": 4}],
        "PostgreSQL really stores inlined vectors",
    )
    require_equal(list((probe.root / "data").rglob("*.parquet")), [], "no data files before flush")
    original_snapshot = probe.query("SELECT max(snapshot_id) AS id FROM lake.snapshots();")[0]["id"]
    reader = SnapshotReader(probe)
    try:
        reader.start()
        probe.query(mutation())
        updated = [{"id": 4, "payload": "new", "distance": 1}, {"id": 3, "payload": "stable", "distance": 2}]
        require_equal(probe.query(RANK), updated, "fresh process sees the complete committed mutation")
        probe.query("CALL ducklake_flush_inlined_data('lake');")
        require_equal(probe.query(RANK), updated, "flush preserves current retrieval")
        require_equal(
            probe.query("SELECT count(*) AS rows FROM lake.items WHERE filename LIKE 'ducklake_inlined_data_%';"),
            [{"rows": 0}],
            "flush really materialized vectors",
        )
        reader.finish()
    finally:
        reader.close()
    require_equal(
        probe.query(
            RANK.replace("FROM lake.items WHERE", f"FROM lake.items AT (VERSION => {original_snapshot}) WHERE")
        ),
        INITIAL,
        "old snapshot after materialization",
    )

    probe.query(
        "BEGIN;INSERT INTO lake.items VALUES (5,[0,0,0],'A','hot');DELETE FROM lake.items WHERE id=4;"
        "INSERT INTO lake.requests VALUES ('r3',sha256('mixed-batch'));COMMIT;"
    )
    latest = [{"id": 5, "payload": "hot", "distance": 0}, {"id": 3, "payload": "stable", "distance": 2}]
    require_equal(probe.query(RANK), latest, "inlined vectors plus deletion of a materialized vector")
    probe.query(mutation())
    require_equal(probe.query(RANK), latest, "old request retry cannot resurrect the deleted vector")
    require_equal(
        probe.query("SELECT count(*) AS rows, count(DISTINCT request_id) AS ids FROM lake.requests;"),
        [{"rows": 3, "ids": 3}],
        "serialized request retry",
    )

    try:
        probe.query(
            "BEGIN;UPDATE lake.items SET payload='uncommitted' WHERE id=3;"
            "INSERT INTO lake.requests VALUES ('failed',sha256('invalid'));"
            "INSERT INTO lake.items VALUES (9,[0,0,0],NULL,'invalid');COMMIT;"
        )
    except AssertionError as error:
        if "NOT NULL constraint failed" not in str(error):
            raise
    else:
        raise AssertionError("invalid batch unexpectedly succeeded")
    require_equal(probe.query(RANK), latest, "failed batch rollback")
    require_equal(
        probe.query("SELECT request_id FROM lake.requests WHERE request_id='failed';"), [], "request rollback"
    )

    probe.kill_after(
        "pg_committed",
        "BEGIN;INSERT INTO lake.items VALUES (6,[0,0,0],'D','ack-lost');"
        "INSERT INTO lake.requests VALUES ('ack-lost',sha256('committed'));COMMIT;",
    )
    require_equal(
        probe.query("SELECT payload FROM lake.items WHERE id=6;"),
        [{"payload": "ack-lost"}],
        "client death after commit",
    )
    require_equal(
        probe.query("SELECT request_id FROM lake.requests WHERE request_id='ack-lost';"),
        [{"request_id": "ack-lost"}],
        "committed request after client death",
    )
    probe.kill_after(
        "pg_uncommitted",
        "BEGIN;UPDATE lake.items SET embedding=[0,0,0] WHERE id=3;DELETE FROM lake.items WHERE id=5;"
        "INSERT INTO lake.requests VALUES ('inflight',sha256('uncommitted'));",
    )
    require_equal(probe.query(RANK), latest, "client death before commit rolls back all rows")
    require_equal(
        probe.query("SELECT request_id FROM lake.requests WHERE request_id='inflight';"),
        [],
        "inflight request rollback",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duckdb", required=True, type=Path)
    parser.add_argument(
        "--dsn", required=True, help="dedicated test database; supply credentials through libpq environment"
    )
    parser.add_argument("--extension", type=Path)
    parser.add_argument("--postgres-extension", type=Path)
    args = parser.parse_args()
    if os.name != "posix":
        parser.error("this probe requires POSIX SIGKILL")
    with tempfile.TemporaryDirectory(prefix="ducklake-postgres-vectors-") as directory:
        probe = PostgresProbe(
            args.duckdb.resolve(strict=True),
            args.extension.resolve(strict=True) if args.extension else None,
            args.postgres_extension.resolve(strict=True) if args.postgres_extension else None,
            Path(directory),
            args.dsn,
        )
        try:
            exercise(probe)
        finally:
            probe.cleanup()
        print(
            "PASS: PostgreSQL inlined vectors, cross-process snapshots, flush, serialized retry, rollback, client death"
        )


if __name__ == "__main__":
    main()
