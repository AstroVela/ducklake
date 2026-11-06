#!/usr/bin/env python3
"""Validate vector transactions when PostgreSQL COMMIT is aborted or its reply is lost."""

from __future__ import annotations

import argparse
import importlib.util
import json
import subprocess
import tempfile
import time
from pathlib import Path


def load_helper(name: str, filename: str):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(filename))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


pg = load_helper("postgres_vectors", "test_postgres_realtime_vector.py")
wire = load_helper("commit_proxy", "postgres_commit_proxy.py")
require_equal = pg.require_equal
sql_string = pg.sql_string

INITIAL = [{"id": 1, "payload": "initial", "distance": 0}, {"id": 2, "payload": "delete", "distance": 1}]
UPDATED = [{"id": 3, "payload": "new", "distance": 1}]


def batch() -> str:
    return (
        "UPDATE lake.items SET embedding=[9,0,0],category='B',payload='moved' "
        "WHERE id=1 AND should_apply('fault-request',sha256('vector-mutation'));"
        "DELETE FROM lake.items WHERE id=2 AND should_apply('fault-request',sha256('vector-mutation'));"
        "INSERT INTO lake.items SELECT 3,[0,1,0],'A','new' "
        "WHERE should_apply('fault-request',sha256('vector-mutation'));"
        "INSERT INTO lake.requests SELECT 'fault-request',sha256('vector-mutation') "
        "WHERE should_apply('fault-request',sha256('vector-mutation'));"
    )


def fault_commit(probe, args, mode: str) -> dict:
    ready = probe.root / "transaction.ready"
    log_path = probe.root / "transaction.log"
    with wire.CommitProxy(args.upstream_host, args.upstream_port, mode) as proxy:
        routed_dsn = probe.dsn + f" host=127.0.0.1 port={proxy.port} sslmode=disable gssencmode=disable"
        original = sql_string("ducklake:postgres:" + probe.dsn)
        require_equal(probe.prefix.count(original), 1, "catalog route is unambiguous")
        prefix = probe.prefix.replace(original, sql_string("ducklake:postgres:" + routed_dsn), 1)
        with log_path.open("wb") as log:
            worker = subprocess.Popen(probe.command[:-1], stdin=subprocess.PIPE, stdout=log, stderr=log)
            try:
                # COPY only signals that the batch is prepared; it does not commit it.
                sql = prefix + pg.GUARD + "BEGIN;" + batch() + f"COPY (SELECT 1) TO {sql_string(ready)};\n"
                worker.stdin.write(sql.encode())
                worker.stdin.flush()
                deadline = time.monotonic() + 30
                while not ready.exists():
                    if worker.poll() is not None or time.monotonic() >= deadline:
                        raise AssertionError("writer did not prepare its batch: " + log_path.read_text())
                    time.sleep(0.02)
                files_before = sorted(str(path.relative_to(probe.root)) for path in probe.root.rglob("*.parquet"))
                proxy.armed.set()
                worker.stdin.write(b"COMMIT;\n")
                worker.stdin.flush()
                worker.stdin.close()
                exit_code = worker.wait(timeout=30)
                if not proxy.dropped.wait(timeout=2):
                    raise AssertionError(
                        "COMMIT fault was not injected: " + str(proxy.observation) + log_path.read_text()
                    )
                observation = dict(proxy.observation)
                observation["client_exit"] = exit_code
                observation["files_before"] = files_before
                observation["files_after"] = sorted(
                    str(path.relative_to(probe.root)) for path in probe.root.rglob("*.parquet")
                )
                observation["removed_files"] = sorted(set(files_before) - set(observation["files_after"]))
                observation["client_reported_unknown"] = "commit outcome is unknown" in log_path.read_text().lower()
                probe.commit_observation = observation
                require_equal(observation["injected"], 1, "exactly one COMMIT fault")
                require_equal(observation["commit_forwarded"], mode == "ack_lost", "COMMIT forwarding boundary")
                require_equal(observation["server_committed"], mode == "ack_lost", "server COMMIT boundary")
                require_equal(observation["client_reported_unknown"], True, "explicit uncertain-commit result")
                require_equal(observation["removed_files"], [], "uncertain commit preserves prepared files")
                if "proxy_error" in observation:
                    raise AssertionError("unexpected protocol proxy error: " + observation["proxy_error"])
                if exit_code == 0:
                    raise AssertionError("client acknowledged a transaction whose COMMIT outcome it could not verify")
                return observation
            finally:
                if worker.poll() is None:
                    worker.kill()
                    worker.wait(timeout=10)


def exercise(probe, args, mode: str) -> dict:
    probe.query(
        "CREATE TABLE lake.items(id BIGINT NOT NULL, embedding FLOAT[] NOT NULL, category VARCHAR NOT NULL, payload VARCHAR);"
        "CREATE TABLE lake.requests(request_id VARCHAR NOT NULL,payload_hash VARCHAR NOT NULL);"
        "BEGIN;INSERT INTO lake.items VALUES (1,[0,0,0],'A','initial'),(2,[1,0,0],'A','delete');"
        "INSERT INTO lake.requests VALUES ('baseline',sha256('baseline'));COMMIT;"
    )
    original_snapshot = probe.query("SELECT max(snapshot_id) AS id FROM lake.snapshots();")[0]["id"]
    observation = fault_commit(probe, args, mode)
    # Inspect with a fresh process connected directly, outside the failed route.
    expected = UPDATED if mode == "ack_lost" else INITIAL
    require_equal(probe.query(pg.RANK), expected, "vector state matches the observed server COMMIT outcome")
    require_equal(
        probe.query("SELECT count(*) AS rows FROM lake.requests WHERE request_id='fault-request';"),
        [{"rows": int(mode == "ack_lost")}],
        "request record shares the business transaction outcome",
    )
    historical = pg.RANK.replace("FROM lake.items WHERE", f"FROM lake.items AT (VERSION => {original_snapshot}) WHERE")
    require_equal(probe.query(historical), INITIAL, "historical vectors survive failed or uncertain commit")

    probe.query(pg.GUARD + "BEGIN;" + batch() + "COMMIT;")
    probe.query(pg.GUARD + "BEGIN;" + batch() + "COMMIT;")
    require_equal(probe.query(pg.RANK), UPDATED, "same request retried without duplicate vectors or resurrection")
    require_equal(
        probe.query("SELECT count(*) AS rows,count(DISTINCT request_id) AS ids FROM lake.requests;"),
        [{"rows": 2, "ids": 2}],
        "one persisted request effect",
    )
    require_equal(
        probe.query("SELECT id,embedding,category,payload FROM lake.items ORDER BY id;"),
        [
            {"id": 1, "embedding": [9, 0, 0], "category": "B", "payload": "moved"},
            {"id": 3, "embedding": [0, 1, 0], "category": "A", "payload": "new"},
        ],
        "all vector and business columns agree after retries",
    )
    require_equal(probe.query(historical), INITIAL, "historical vectors after retries")
    return observation


def catalog_files(probe) -> list:
    query = (
        f'SELECT path,table_id,begin_snapshot,end_snapshot FROM "{probe.schema}".ducklake_data_file '
        "WHERE end_snapshot IS NULL ORDER BY data_file_id"
    )
    sql = (
        probe.driver_prefix
        + f"ATTACH {sql_string(probe.dsn)} AS pg (TYPE postgres);"
        + f"SELECT * FROM postgres_query('pg', {sql_string(query)});"
    )
    result = subprocess.run(probe.command + [sql], capture_output=True, text=True, timeout=30)
    if result.returncode:
        raise AssertionError(result.stderr)
    return json.loads(result.stdout)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duckdb", required=True, type=Path)
    parser.add_argument("--dsn", required=True, help="dedicated test database; use libpq environment for credentials")
    parser.add_argument("--upstream-host", required=True, help="test PostgreSQL host or Unix socket directory")
    parser.add_argument("--upstream-port", type=int, default=5432)
    parser.add_argument("--extension", type=Path)
    parser.add_argument("--postgres-extension", type=Path)
    parser.add_argument("--report", type=Path)
    parser.add_argument("--row-limit", type=int, choices=[0, 100], help="reproduce one storage path")
    parser.add_argument("--mode", choices=["before_forward", "ack_lost"], help="reproduce one fault boundary")
    args = parser.parse_args()
    results = []
    for row_limit in [args.row_limit] if args.row_limit is not None else [100, 0]:
        for mode in [args.mode] if args.mode else ["before_forward", "ack_lost"]:
            result = {"row_limit": row_limit, "mode": mode, "passed": False}
            with tempfile.TemporaryDirectory(prefix="ducklake-commit-outcome-") as directory:
                probe = pg.PostgresProbe(
                    args.duckdb.resolve(strict=True),
                    args.extension.resolve(strict=True) if args.extension else None,
                    args.postgres_extension.resolve(strict=True) if args.postgres_extension else None,
                    Path(directory),
                    args.dsn,
                    row_limit=row_limit,
                )
                try:
                    result["wire"] = exercise(probe, args, mode)
                    result["passed"] = True
                except Exception as error:
                    result["error"] = str(error)
                    try:
                        result["catalog_files"] = catalog_files(probe)
                        removed = {
                            Path(path).name
                            for path in getattr(probe, "commit_observation", {}).get("removed_files", [])
                        }
                        result["catalog_references_to_removed_files"] = [
                            row for row in result["catalog_files"] if Path(row["path"]).name in removed
                        ]
                    except Exception as inspect_error:
                        result["inspection_error"] = str(inspect_error)
                finally:
                    if hasattr(probe, "commit_observation"):
                        result["wire"] = probe.commit_observation
                    try:
                        probe.cleanup()
                    except Exception as error:
                        result["cleanup_error"] = str(error)
                        result["passed"] = False
            results.append(result)
            print(json.dumps(result, ensure_ascii=False), flush=True)
    if args.report:
        args.report.write_text(json.dumps(results, ensure_ascii=False, indent=2) + "\n")
    if not all(result["passed"] for result in results):
        raise SystemExit(1)
    print(f"PASS: {len(results)} COMMIT fault cases, request retry, and historical vector checks")


if __name__ == "__main__":
    main()
