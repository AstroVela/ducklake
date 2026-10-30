#!/usr/bin/env python3
"""Qualify retryable native Vortex batches through installed Vane providers."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import threading
import time

SPEC = importlib.util.spec_from_file_location(
    "vane_native_proof", Path(__file__).with_name("test_vane_native_vortex.py")
)
assert SPEC is not None and SPEC.loader is not None
BASE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BASE)
query, require, quote = BASE.query, BASE.require, BASE.quote
REQUESTS = "__ducklake_metadata_lake.ducklake_vortex_write_requests_v1"
DRIVER_ROWS = [
    {"id": 1, "embedding": [1.0, 0.0], "category": "A", "payload": "一"},
    {"id": 2, "embedding": [2.0, 0.0], "category": None, "payload": None},
]


def batch(connection, request_id, rows, table="items"):
    return query_parameters(
        connection,
        "SELECT * FROM ducklake_write_vortex_batch('lake','main',?,'knn',?,"
        "?::STRUCT(id BIGINT,embedding FLOAT[],category VARCHAR,payload VARCHAR)[])",
        [table, request_id, rows],
    )[0]


def query_parameters(connection, sql, parameters):
    result = connection.execute(sql, parameters)
    names = [item[0] for item in result.description]
    return [dict(zip(names, row)) for row in result.fetchall()]


def rejected(call, message):
    try:
        call()
    except Exception as error:
        require(message.lower() in str(error).lower(), f"Expected {message!r}, got {error}")
    else:
        raise AssertionError(f"Expected rejection: {message}")


def rows(start, count):
    return [
        {
            "id": start + i,
            "embedding": [128 + i / 16, (i * 4) % 12, (i * 5) % 13, 0, 1, 8, 7, 16],
            "category": "B" if i % 4 == 0 else "A",
            "payload": None if i == 0 else "实时-'\x00-向量",
        }
        for i in range(count)
    ]


def count(connection):
    return query(connection, "SELECT count(*) AS n FROM lake.items")[0]["n"]


def check_no_request(connection, request_id):
    require(
        query(connection, "SELECT count(*) AS n FROM " + REQUESTS + " WHERE request_id=" + quote(request_id))
        == [{"n": 0}],
        "Rejected batch retained a request record",
    )


def exercise(root, report):
    import vane

    (root / "indexes").mkdir()
    connection = BASE.connect(root, create=True)
    recalls, phases, checks = [], [], []
    try:
        report.update(vane_version=vane.__version__, engine=query(connection, "PRAGMA version")[0])
        connection.execute("SELECT * FROM ducklake_initialize_vortex_writes('lake')")
        retry_setting = query(connection, "SELECT current_setting('ducklake_max_retry_count') AS retries")
        original = rows(1000, 16)
        receipt = batch(connection, "first-'请求", original)
        require(receipt["row_count"] == 16 and count(connection) == 16, "Batch did not append all rows")
        require(
            query(connection, "SELECT max(snapshot_id) AS snapshot_id FROM __ducklake_metadata_lake.ducklake_snapshot")
            == [{"snapshot_id": receipt["snapshot_id"]}],
            "Receipt did not identify the native commit snapshot",
        )
        connection.execute("PREPARE nearest AS " + BASE.SEARCH.search_sql(BASE.VECTOR))
        phases.append(BASE.qualify(connection, "committed-T", recalls))
        for _ in range(3):
            require(batch(connection, "first-'请求", original) == receipt, "Retry changed the original result")
        require(count(connection) == 16, "Retry duplicated rows")
        connection.execute("UPDATE lake.items SET payload='newer' WHERE id=1001")
        require(batch(connection, "first-'请求", original) == receipt, "Later mutation changed the request result")
        require(
            query(connection, "SELECT payload FROM lake.items WHERE id=1001") == [{"payload": "newer"}],
            "Retry overwrote a newer row version",
        )
        rejected(lambda: batch(connection, "first-'请求", list(reversed(original))), "payload mismatch")
        for field, replacement in [("id", -1000), ("embedding", [0.0] * 8), ("category", "A"), ("payload", "")]:
            changed = [dict(row) for row in original]
            changed[0][field] = replacement
            rejected(lambda: batch(connection, "first-'请求", changed), "payload mismatch")
        rounded = [dict(row) for row in original]
        rounded[0]["embedding"] = [128.0 + 1e-8] + original[0]["embedding"][1:]
        require(
            batch(connection, "first-'请求", rounded) == receipt, "Equivalent float32 input changed request identity"
        )
        require(count(connection) == 16, "Payload mismatch changed data")
        checks.append(
            "stable receipt, native commit snapshot, order-sensitive payload and retry after a newer mutation"
        )

        connections = [connection.cursor() for _ in range(8)]
        barrier = threading.Barrier(len(connections))

        def same_request(reader):
            barrier.wait(timeout=15)
            return batch(reader, "parallel", rows(5000, 2))

        try:
            with ThreadPoolExecutor(max_workers=8) as pool:
                results = list(pool.map(same_request, connections))
            require(
                all(result == results[0] for result in results), "Concurrent duplicates returned different receipts"
            )
            require(count(connection) == 18, "Concurrent duplicate request appended more than once")
            barrier = threading.Barrier(2)

            def conflicting_request(pair):
                reader, first_id = pair
                barrier.wait(timeout=15)
                try:
                    return batch(reader, "different-payloads", rows(first_id, 1))
                except Exception as error:
                    require("payload mismatch" in str(error), str(error))
                    return None

            with ThreadPoolExecutor(max_workers=2) as pool:
                outcomes = list(pool.map(conflicting_request, zip(connections[:2], [6000, 7000])))
            require(
                sum(item is not None for item in outcomes) == 1 and count(connection) == 19,
                "Conflicting concurrent payloads did not choose exactly one committed batch",
            )
        finally:
            for reader in connections:
                reader.close()
        checks.append("eight simultaneous identical requests commit once; two different payloads have one winner")

        zero = [{"id": None, "embedding": [0.0] * 8, "category": None, "payload": None}]
        zero_receipt = batch(connection, "zero", zero)
        zero[0]["embedding"] = [-0.0] * 8
        require(batch(connection, "zero", zero) == zero_receipt, "Signed zero changed canonical request identity")
        for request_id, invalid, message in [
            ("empty", [], "1 to 1000"),
            ("null-row", [None], "row cannot be NULL"),
            ("dimension", rows(9000, 1) + [{**rows(9001, 1)[0], "embedding": [1.0]}], "dimension"),
            ("null-vector", [{**rows(9000, 1)[0], "embedding": None}], "NULL list"),
            ("null-component", [{**rows(9000, 1)[0], "embedding": [None] * 8}], "non-NULL"),
            ("nan", [{**rows(9000, 1)[0], "embedding": [float("nan")] * 8}], "finite"),
            ("infinity", [{**rows(9000, 1)[0], "embedding": [float("inf")] * 8}], "finite"),
            ("too-many", rows(9000, 1001), "1 to 1000"),
            ("too-large", [{**rows(9000, 1)[0], "payload": "x" * (4 * 1024 * 1024)}], "byte budget"),
        ]:
            rejected(lambda: batch(connection, request_id, invalid), message)
            check_no_request(connection, request_id)
        rejected(lambda: batch(connection, "", rows(9000, 1)), "request ID")
        rejected(lambda: batch(connection, "x" * 129, rows(9000, 1)), "request ID")
        connection.execute("BEGIN")
        try:
            rejected(lambda: batch(connection, "outer-transaction", rows(9000, 1)), "autocommit")
        finally:
            connection.execute("ROLLBACK")
        require(count(connection) == 20, "Rejected input committed partial rows")
        checks.append(
            "canonical signed zero, NULL business values, Unicode/NUL text and atomic validation/size rejection"
        )

        (root / "other-index").mkdir()
        connection.execute("CREATE TABLE lake.other(id BIGINT,embedding FLOAT[],category VARCHAR,payload VARCHAR)")
        connection.execute(
            "CALL ducklake_set_option('lake','experimental_data_file_format','vortex',table_name=>'other')"
        )
        connection.execute(
            "SELECT * FROM ducklake_create_vortex_index('lake','main','other','knn',"
            + quote(root / "other-index")
            + ",8)"
        )
        other = batch(connection, "first-'请求", original, table="other")
        require(
            other["table_uuid"] != receipt["table_uuid"] and other["payload_hash"] == receipt["payload_hash"],
            "Request identity was not scoped by stable table identity",
        )
        checks.append("identical request IDs are independent across table UUIDs")

        connection.execute(
            "SELECT * FROM ducklake_enable_vortex_optimizer('lake','main','items','knn',"
            + quote(root / "indexes")
            + ",8,100000,4194304,100000,67108864)"
        )
        connection.execute(
            "SELECT * FROM ducklake_set_vortex_write_budget('lake','main','items','knn',21,4194304,512,67108864)"
        )
        rejected(lambda: batch(connection, "budget", rows(8000, 2)), "RESOURCE_EXHAUSTED")
        check_no_request(connection, "budget")
        require(count(connection) == 20, "Write budget rejection committed part of a batch")
        connection.execute(
            "SELECT * FROM ducklake_set_vortex_write_budget('lake','main','items','knn',1000,4194304,512,67108864)"
        )
        batch(connection, "budget", rows(8000, 2))
        connection.execute("SELECT * FROM ducklake_cancel_vortex_optimizer('lake','main','items','knn')")
        require(
            query(connection, "SELECT current_setting('ducklake_max_retry_count') AS retries") == retry_setting,
            "WriteBatch changed another connection's native retry policy",
        )
        checks.append(
            "native write-budget failure rolls back data and receipt; explicit retry succeeds after admission changes"
        )

        rejected(
            lambda: connection.execute("INSERT INTO " + REQUESTS + " SELECT * FROM " + REQUESTS + " LIMIT 1"),
            "duplicate key",
        )
        used = query(connection, "SELECT count(*) AS n FROM " + REQUESTS)[0]["n"]
        connection.execute(
            "INSERT INTO "
            + REQUESTS
            + " SELECT 'capacity-test',i::VARCHAR,'hash',1,0 FROM range("
            + str(100000 - used)
            + ")t(i)"
        )
        require(
            batch(connection, "first-'请求", original) == receipt, "Full request catalog rejected a committed retry"
        )
        rejected(lambda: batch(connection, "catalog-full", rows(9000, 1)), "request catalog budget")
        check_no_request(connection, "catalog-full")
        connection.execute("DELETE FROM " + REQUESTS + " WHERE table_uuid='capacity-test'")
        checks.append(
            "native PRIMARY KEY rejects duplicate records; bounded request catalog still resolves existing requests"
        )

        batch(connection, "for-index", rows(10000, 128))
        connection.execute("CALL ducklake_flush_inlined_data('lake',table_name=>'items')")
        phases.append(BASE.qualify(connection, "flushed-F", recalls))
        connection.execute(
            "SELECT * FROM ducklake_append_vortex_index_segment('lake','main','items','knn',"
            + quote(root / "indexes" / "first-B.json")
            + ",8,1)"
        )
        phases.append(BASE.qualify(connection, "indexed-B", recalls))
        live = [{"id": 100000, "embedding": BASE.VECTOR, "category": "A", "payload": "B旁实时"}]
        batch(connection, "after-B", live)
        require(batch(connection, "first-'请求", original) == receipt, "Flush or indexing changed an old receipt")
        phases.append(BASE.qualify(connection, "B-plus-new-T", recalls))
    finally:
        connection.close()
    report.update(checks=checks, phases=phases, fixture_recalls=recalls, minimum_recall=min(recalls))


def crash_cases(driver, root, report):
    checks = []
    for stage, committed in [("before_commit", False), ("after_commit", True)]:
        case = root / stage
        case.mkdir(mode=0o700)
        with (case / "driver.log").open("w") as log:
            child = subprocess.Popen([str(driver), str(case), "pause-" + stage], stdout=log, stderr=subprocess.STDOUT)
            try:
                deadline = time.monotonic() + 60
                while not (case / "checkpoint").exists():
                    require(
                        child.poll() is None,
                        "Native writer exited before checkpoint: " + (case / "driver.log").read_text(),
                    )
                    require(time.monotonic() < deadline, "Native writer did not reach checkpoint")
                    time.sleep(0.02)
                if committed:
                    wal = case / "metadata.ducklake.wal"
                    require(wal.is_file() and wal.stat().st_size > 0, "Committed recovery case has no metadata WAL")
                child.kill()
                child.wait(timeout=10)
                require(child.returncode == -9, "Recovery did not exercise SIGKILL")
            finally:
                if child.poll() is None:
                    child.kill()
                    child.wait(timeout=10)
        connection = BASE.connect(case)
        try:
            expected = 2 if committed else 0
            require(count(connection) == expected, "SIGKILL recovered a partial or missing batch")
            require(
                query(connection, "SELECT count(*) AS n FROM " + REQUESTS) == [{"n": 1 if committed else 0}],
                "SIGKILL separated request identity from data",
            )
            old = query(connection, "SELECT * FROM " + REQUESTS)
            receipt = batch(connection, "request", DRIVER_ROWS)
            require(
                receipt["payload_hash"] == "sha256:37aeff8eb03044a003d08bebcb70ab9f3b32b57eca287c37fe5a150c6e967eb0",
                "Canonical typed bytes disagreed with the independent fixed test vector",
            )
            require(
                batch(connection, "request", DRIVER_ROWS) == receipt and count(connection) == 2,
                "Retry after recovery duplicated the batch",
            )
            if committed:
                require(receipt == old[0], "Lost response changed the recovered result")
            BASE.SEARCH.qualify(
                {"rows": query(connection, BASE.SEARCH.search_sql([0, 0]))}, BASE.raw(connection), [0, 0]
            )
        finally:
            connection.close()
        checks.append(stage + ": SIGKILL, installed-provider recovery, atomic receipt/data and safe request retry")
    for mode in ["fail-after_lookup", "fail-before_commit", "fail-after_commit", "cancel", "conflict"]:
        case = root / mode
        case.mkdir(mode=0o700)
        with (case / "driver.log").open("w") as log:
            subprocess.run([str(driver), str(case), mode], stdout=log, stderr=subprocess.STDOUT, check=True, timeout=60)
        checks.append(mode + ": native rollback/commit outcome and fresh-connection retry passed")
    report["recovery_checks"] = checks


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--driver", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    require(os.environ.get("VANE_RUNNER") == "local-fast", "Set VANE_RUNNER=local-fast explicitly")
    root = args.output_dir.resolve()
    root.mkdir(mode=0o700, parents=True, exist_ok=False)
    report = {"status": "running"}
    started = time.monotonic()
    try:
        exercise(root, report)
        crash_cases(args.driver.resolve(), root, report)
        report["status"] = "passed"
    except Exception as error:
        report.update(status="failed", error=str(error))
        raise
    finally:
        report["seconds"] = time.monotonic() - started
        (root / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
