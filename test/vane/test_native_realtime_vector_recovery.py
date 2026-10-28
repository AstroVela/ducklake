#!/usr/bin/env python3
"""Kill native DuckDB processes and verify DuckLake vector transaction recovery."""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import tempfile
import time
from pathlib import Path


def sql_string(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def require_equal(actual: object, expected: object, description: str) -> None:
    if actual != expected:
        raise AssertionError(f"{description}: expected {expected!r}, got {actual!r}")


class RecoveryProbe:
    def __init__(self, duckdb: Path, extension: Path | None, root: Path, file_format: str = "parquet") -> None:
        self.duckdb = duckdb
        self.root = root
        self.metadata = root / "metadata.ducklake"
        self.file_format = file_format
        self.command = [str(duckdb), "-unsigned", "-batch", "-bail", "-json", "-c"]
        load = "LOAD ducklake;" if extension is None else f"LOAD {sql_string(extension)};"
        self.prefix = (
            "SET autoinstall_known_extensions = false;"
            "SET autoload_known_extensions = false;"
            "SET threads = 1;"
            "SET memory_limit = '512MiB';"
            "SET checkpoint_threshold = '1TB';"
            "LOAD parquet;"
            f"{load}"
            f"ATTACH {sql_string('ducklake:' + str(self.metadata))} AS lake "
            f"(DATA_PATH {sql_string(root / 'data')}, DATA_INLINING_ROW_LIMIT 100);"
        )
        if file_format == "vortex":
            self.prefix += "LOAD vortex;"

    def configure_format(self, table: str) -> str:
        if self.file_format == "parquet":
            return ""
        return (
            "CALL ducklake_set_option('lake', 'experimental_data_file_format', 'vortex', "
            f"table_name => {sql_string(table)});"
        )

    def query(self, sql: str) -> list[dict]:
        result = subprocess.run(self.command + [self.prefix + sql], capture_output=True, text=True, timeout=30)
        if result.returncode:
            raise AssertionError(f"DuckDB exited {result.returncode}:\n{result.stdout}\n{result.stderr}")
        # CALL can emit a result before the final SELECT in the same CLI invocation.
        decoder = json.JSONDecoder()
        remaining = result.stdout.strip()
        rows = []
        while remaining:
            rows, offset = decoder.raw_decode(remaining)
            remaining = remaining[offset:].lstrip()
        return rows

    def kill_after(self, name: str, sql: str, require_wal: bool = False) -> None:
        ready = self.root / f"{name}.ready"
        log_path = self.root / f"{name}.log"
        # COPY is an external handshake after the requested transaction boundary.
        # The final query keeps the process alive without a clean database shutdown.
        command = self.command + [
            self.prefix
            + sql
            + f"COPY (SELECT 'ready') TO {sql_string(ready)} (HEADER false);"
            + "SELECT sum(hash(i)) FROM range(100000000000000) AS source(i);"
        ]
        with log_path.open("wb") as log:
            process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
            try:
                deadline = time.monotonic() + 30
                while not ready.exists():
                    if process.poll() is not None or time.monotonic() >= deadline:
                        raise AssertionError(f"{name} did not reach its boundary:\n{log_path.read_text()}")
                    time.sleep(0.02)
                if require_wal:
                    wal = Path(str(self.metadata) + ".wal")
                    if not wal.exists() or wal.stat().st_size == 0:
                        raise AssertionError(f"{name}: expected an uncheckpointed metadata WAL")
                process.send_signal(signal.SIGKILL)
                require_equal(process.wait(timeout=10), -signal.SIGKILL, f"{name} termination")
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=10)


def exercise(probe: RecoveryProbe) -> None:
    rank = (
        "SELECT id, payload, list_distance(embedding, [0, 0, 0]::FLOAT[]) AS distance "
        "FROM lake.items WHERE category = 'A' ORDER BY distance, id LIMIT 2;"
    )
    initial = [
        {"id": 1, "payload": "initial", "distance": 0},
        {"id": 2, "payload": "delete", "distance": 1},
    ]
    probe.kill_after(
        "committed",
        "CREATE TABLE lake.items(id BIGINT, embedding FLOAT[], category VARCHAR, payload VARCHAR);"
        "CREATE TABLE lake.requests(request_id VARCHAR, payload_hash VARCHAR);"
        + probe.configure_format("items")
        + probe.configure_format("requests")
        + "BEGIN;"
        "INSERT INTO lake.items VALUES (1, [0, 0, 0], 'A', 'initial'), (2, [1, 0, 0], 'A', 'delete');"
        "INSERT INTO lake.requests VALUES ('r1', sha256('initial-batch'));"
        "COMMIT;",
        require_wal=True,
    )
    require_equal(probe.query(rank), initial, "committed vector recovery")
    require_equal(probe.query("SELECT request_id FROM lake.requests;"), [{"request_id": "r1"}], "committed request")
    require_equal(
        probe.query("SELECT count(*) AS rows FROM lake.items WHERE filename LIKE 'ducklake_inlined_data_%';"),
        [{"rows": 2}],
        "committed data was actually inlined",
    )
    original_snapshot = probe.query("SELECT max(snapshot_id) AS id FROM lake.snapshots();")[0]["id"]

    mutation = (
        "UPDATE lake.items SET embedding = [9, 0, 0], category = 'B', payload = 'moved' WHERE id = 1;"
        "DELETE FROM lake.items WHERE id = 2;"
        "INSERT INTO lake.items VALUES (3, [0, 1, 0], 'A', 'new');"
        "INSERT INTO lake.requests VALUES ('r2', sha256('mutation-batch'));"
    )
    probe.kill_after("uncommitted", "BEGIN;" + mutation)
    require_equal(probe.query(rank), initial, "uncommitted vectors rolled back")
    require_equal(probe.query("SELECT request_id FROM lake.requests;"), [{"request_id": "r1"}], "request rolled back")

    probe.query("BEGIN;" + mutation + "COMMIT;")
    probe.kill_after("materialized", "CALL ducklake_flush_inlined_data('lake');", require_wal=True)
    require_equal(
        probe.query(
            "SELECT bool_and(ends_with(data_file, "
            + sql_string("." + probe.file_format)
            + ")) AS matches FROM ducklake_list_files('lake', 'items');"
        ),
        [{"matches": True}],
        "materialization used the selected physical format",
    )
    require_equal(probe.query(rank), [{"id": 3, "payload": "new", "distance": 1}], "materialized vector recovery")
    require_equal(
        probe.query("SELECT id, embedding[1] AS x, category, payload FROM lake.items ORDER BY id;"),
        [
            {"id": 1, "x": 9, "category": "B", "payload": "moved"},
            {"id": 3, "x": 0, "category": "A", "payload": "new"},
        ],
        "updated vectors and business columns recovered together",
    )
    require_equal(
        probe.query("SELECT request_id FROM lake.requests ORDER BY request_id;"),
        [{"request_id": "r1"}, {"request_id": "r2"}],
        "request records survived materialization",
    )
    require_equal(
        probe.query("SELECT count(*) AS rows FROM lake.items WHERE filename LIKE 'ducklake_inlined_data_%';"),
        [{"rows": 0}],
        "materialization produced file-backed rows",
    )
    require_equal(
        probe.query(
            rank.replace("FROM lake.items WHERE", f"FROM lake.items AT (VERSION => {original_snapshot}) WHERE")
        ),
        initial,
        "historical vectors survived materialization and process death",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duckdb", required=True, type=Path, help="native DuckDB CLI matching the extension")
    parser.add_argument("--extension", type=Path, help="loadable artifact; omit for a statically linked DuckLake")
    parser.add_argument("--file-format", choices=("parquet", "vortex"), default="parquet")
    args = parser.parse_args()
    if os.name != "posix":
        parser.error("this probe requires POSIX SIGKILL")
    duckdb = args.duckdb.resolve(strict=True)
    extension = args.extension.resolve(strict=True) if args.extension else None
    with tempfile.TemporaryDirectory(prefix="ducklake-native-vector-recovery-") as directory:
        probe = RecoveryProbe(duckdb, extension, Path(directory), args.file_format)
        exercise(probe)
        print("PASS: committed WAL recovery, uncommitted batch rollback, materialization recovery, historical vectors")


if __name__ == "__main__":
    main()
