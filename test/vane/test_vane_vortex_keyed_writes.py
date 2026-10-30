#!/usr/bin/env python3
"""Qualify managed, retryable upsert/delete through installed Vane."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import time

SPEC = importlib.util.spec_from_file_location(
    "write_proof", Path(__file__).with_name("test_vane_vortex_write_batch.py")
)
assert SPEC is not None and SPEC.loader is not None
WRITE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(WRITE)
BASE = WRITE.BASE
query, require, quote = BASE.query, BASE.require, BASE.quote


def mutate(connection, request, rows, deletes=(), table="items"):
    return WRITE.query_parameters(
        connection,
        "SELECT * FROM ducklake_mutate_vortex_batch('lake','main',?,'knn',?,"
        "?::STRUCT(id BIGINT,embedding FLOAT[],category VARCHAR,payload VARCHAR)[],?::BIGINT[])",
        [table, request, rows, list(deletes)],
    )[0]


def manage(connection, table="items"):
    connection.execute("SELECT * FROM ducklake_manage_vortex_writes('lake','main'," + quote(table) + ",'knn')")


def source_idle(connection):
    deadline = time.monotonic() + 60
    while True:
        source = query(connection, "SELECT * FROM ducklake_vortex_source_retention_status('lake')")[0]
        worker = query(connection, "SELECT * FROM ducklake_vortex_maintenance_status('lake')")[0]
        require(worker["state"] != "failed", str(worker))
        if not worker["running"] and not any(
            source[key] for key in ["eligible_snapshots", "scheduled_files", "pending_deletions"]
        ):
            return
        require(time.monotonic() < deadline, f"Source cleanup stalled: {source}, {worker}")
        time.sleep(0.02)


def exercise(root, report):
    (root / "indexes").mkdir()
    connection = BASE.connect(root, create=True)
    recalls, phases, checks = [], [], []
    try:
        connection.execute("SELECT * FROM ducklake_initialize_vortex_writes('lake')")
        WRITE.rejected(lambda: mutate(connection, "unmanaged", WRITE.rows(0, 1)), "managed write mode")
        manage(connection)
        connection.execute("PREPARE nearest AS " + BASE.SEARCH.search_sql(BASE.VECTOR))
        seed = WRITE.rows(0, 256)
        original = mutate(connection, "seed-'请求", seed)
        require(mutate(connection, "seed-'请求", seed) == original, "A seed retry changed its receipt")
        phases.append(BASE.qualify(connection, "managed-T", recalls))

        for sql in [
            "INSERT INTO lake.items VALUES(900,[1,0,0,0,0,0,0,0],'A','outside')",
            "UPDATE lake.items SET payload='outside' WHERE id=0",
            "DELETE FROM lake.items WHERE id=0",
            "ALTER TABLE lake.items RENAME TO renamed",
            "TRUNCATE lake.items",
            "DROP TABLE lake.items",
        ]:
            WRITE.rejected(lambda sql=sql: connection.execute(sql), "Managed Vortex tables require")
        WRITE.rejected(lambda: connection.execute("ALTER TABLE lake.items ADD COLUMN extra BIGINT"), "fixed schema")
        WRITE.rejected(lambda: WRITE.batch(connection, "append", WRITE.rows(900, 1)), "managed write mode")
        require(BASE.raw(connection) == seed, "An uncoordinated write changed managed rows")
        connection.execute("CREATE TABLE lake.unmanaged(id BIGINT); INSERT INTO lake.unmanaged VALUES(1)")
        checks.append(
            "explicit empty-table ownership rejects ordinary DML/schema changes and append; unrelated SQL remains usable"
        )

        before = BASE.raw(connection)
        invalid = [
            ([], [], "1 to 1000 operations"),
            ([dict(WRITE.rows(0, 1)[0], id=None)], [], "non-NULL unique ids"),
            (WRITE.rows(0, 1) * 2, [], "non-NULL unique ids"),
            (WRITE.rows(0, 1), [0], "non-NULL unique ids"),
            ([], [1, 1], "non-NULL unique ids"),
            ([], [None], "non-NULL unique ids"),
            ([], list(range(1001)), "1 to 1000 operations"),
            ([dict(WRITE.rows(0, 1)[0], embedding=[1, 2])], [], "dimension"),
            ([dict(WRITE.rows(0, 1)[0], payload="x" * 4194304)], [], "byte budget"),
        ]
        for index, (rows, deletes, message) in enumerate(invalid):
            request = f"invalid-{index}"
            WRITE.rejected(lambda: mutate(connection, request, rows, deletes), message)
            WRITE.check_no_request(connection, request)
        require(BASE.raw(connection) == before, "A rejected mutation partially deleted current rows")
        checks.append(
            "non-NULL unique ids across both operation lists, dimensions, operation/byte limits reject atomically"
        )

        connection.execute("CALL ducklake_flush_inlined_data('lake',table_name=>'items')")
        connection.execute(
            "SELECT * FROM ducklake_append_vortex_index_segment('lake','main','items','knn',"
            + quote(root / "indexes/seed.json")
            + ",8,1)"
        )
        first = BASE.head(connection)["reference"]["segments"][0]
        signature = BASE.signature(first)
        old_source = Path(first["reference"]["snapshot"]["files"][0]["uri"])
        phases.append(BASE.qualify(connection, "first-B", recalls))
        connection.execute(
            "SELECT * FROM ducklake_enable_vortex_optimizer('lake','main','items','knn',"
            + quote(root / "indexes")
            + ",8,100000,4194304,100000,67108864)"
        )
        connection.execute("SELECT * FROM ducklake_cancel_vortex_optimizer('lake','main','items','knn')")
        deadline = time.monotonic() + 30
        while BASE.status(connection)["running"]:
            require(time.monotonic() < deadline, "Seed optimizer did not stop")
            time.sleep(0.01)
        connection.execute(
            "SELECT * FROM ducklake_set_vortex_write_budget('lake','main','items','knn',1,4194304,4096,67108864)"
        )
        WRITE.rejected(lambda: mutate(connection, "quota", WRITE.rows(10, 2), [12]), "RESOURCE_EXHAUSTED")
        WRITE.check_no_request(connection, "quota")
        require(BASE.raw(connection) == seed, "Admission rollback lost the rows deleted by upsert")
        connection.execute(
            "SELECT * FROM ducklake_set_vortex_write_budget('lake','main','items','knn',1000,4194304,4096,67108864)"
        )

        replacement = [
            {"id": 0, "embedding": BASE.VECTOR, "category": "A", "payload": "新版本-'\x00-向量"},
            {"id": 10000, "embedding": [133.125, *BASE.VECTOR[1:]], "category": "B", "payload": None},
        ]
        reader = connection.cursor()
        try:
            reader.execute("BEGIN")
            old_rows = BASE.raw(reader)
            receipt = mutate(connection, "replace", replacement, [1, 99999])
            require(
                receipt["row_count"] == 4 and BASE.raw(reader) == old_rows,
                "Atomic mutation changed an old native transaction",
            )
            reader.execute("COMMIT")
        finally:
            reader.close()
        require(
            BASE.head(connection)["reference"]["segments"][0] == first and BASE.signature(first) == signature,
            "A logical upsert rebuilt its existing B generation",
        )
        require(
            mutate(connection, "replace", replacement, [1, 99999]) == receipt, "A mutation retry changed its receipt"
        )
        WRITE.rejected(lambda: mutate(connection, "replace", replacement, [2, 99999]), "request_id payload mismatch")
        require(
            query(
                connection,
                "SELECT id,payload FROM ducklake_vortex_search('lake','main','items','knn',"
                + str(BASE.VECTOR)
                + "::FLOAT[],1)",
            )
            == [{"id": 0, "payload": replacement[0]["payload"]}],
            "Committed T did not supersede old B before top-k",
        )
        phases.append(BASE.qualify(connection, "B-plus-new-T", recalls))
        deletion = mutate(connection, "erase", [], [0])
        require(
            mutate(connection, "replace", replacement, [1, 99999]) == receipt
            and query(connection, "SELECT count(*) AS n FROM lake.items WHERE id=0") == [{"n": 0}],
            "An older upsert retry resurrected a deleted key",
        )
        phases.append(BASE.qualify(connection, "delete-visible", recalls))
        revived = [dict(replacement[0], payload="显式重建")]
        mutate(connection, "revive", revived)
        require(
            mutate(connection, "erase", [], [0]) == deletion
            and query(connection, "SELECT payload FROM lake.items WHERE id=0") == [{"payload": "显式重建"}],
            "An old delete retry removed a newer version",
        )
        no_op = mutate(connection, "absent", [], [-9999])
        require(
            query(
                connection,
                "SELECT count(*) AS n FROM __ducklake_metadata_lake.ducklake_snapshot WHERE snapshot_id="
                + str(no_op["snapshot_id"]),
            )
            == [{"n": 1}],
            "A missing-key delete returned a nonexistent native snapshot",
        )
        require(mutate(connection, "absent", [], [-9999]) == no_op, "Missing-key delete was not durable and idempotent")
        phases.append(BASE.qualify(connection, "reinsert-and-old-retries", recalls))
        checks.append(
            "quota rejection rolls back deletes; B generation stays fixed; T supersedes B; stale request retries never reapply upserts/deletes"
        )

        def simultaneous(item):
            cursor = connection.cursor()
            try:
                row = dict(replacement[0], payload=item)
                return mutate(cursor, item, [row]), row
            finally:
                cursor.close()

        with ThreadPoolExecutor(max_workers=2) as pool:
            written = list(pool.map(simultaneous, ["writer-A", "writer-B"]))
        winner = max(written, key=lambda item: item[0]["snapshot_id"])[1]
        require(
            query(connection, "SELECT count(*) AS n FROM lake.items WHERE id=0") == [{"n": 1}]
            and query(connection, "SELECT payload FROM lake.items WHERE id=0") == [{"payload": winner["payload"]}],
            "Concurrent upserts violated uniqueness or committed order",
        )
        for recorded, row in written:
            require(
                mutate(connection, recorded["request_id"], [row]) == recorded, "A concurrent retry changed its receipt"
            )
        require(
            query(connection, "SELECT payload FROM lake.items WHERE id=0") == [{"payload": winner["payload"]}],
            "Replaying an older concurrent request changed the winner",
        )
        checks.append(
            "concurrent same-key writes have one current row and follow native commit order; each old request keeps its exact result"
        )

        for table in ["second", "occupied"]:
            connection.execute(
                f"CREATE TABLE lake.{table}(id BIGINT,embedding FLOAT[],category VARCHAR,payload VARCHAR)"
            )
            connection.execute(
                f"CALL ducklake_set_option('lake','experimental_data_file_format','vortex',table_name=>'{table}')"
            )
            connection.execute(
                "SELECT * FROM ducklake_create_vortex_index('lake','main',"
                + quote(table)
                + ",'knn',"
                + quote(root / "indexes")
                + ",8)"
            )
        manage(connection, "second")
        other_receipt = mutate(connection, "seed-'请求", WRITE.rows(0, 1), table="second")
        require(other_receipt["table_uuid"] != original["table_uuid"], "Request identity leaked across managed tables")
        connection.execute("INSERT INTO lake.occupied VALUES(1,[1,0,0,0,0,0,0,0],'A','existing')")
        WRITE.rejected(lambda: manage(connection, "occupied"), "empty table")

        mutate(connection, "for-second-B", WRITE.rows(100000, 128))
        connection.execute("CALL ducklake_flush_inlined_data('lake',table_name=>'items')")
        connection.execute(
            "SELECT * FROM ducklake_append_vortex_index_segment('lake','main','items','knn',"
            + quote(root / "indexes/second.json")
            + ",8,2)"
        )
        require(
            BASE.head(connection)["reference"]["segments"][0] == first and BASE.signature(first) == signature,
            "Appending independent B2 changed B1",
        )
        phases.append(BASE.qualify(connection, "B1-plus-B2", recalls))
        connection.execute("SELECT * FROM ducklake_set_vortex_source_retention('lake',true,1,0)")
        connection.execute("SELECT * FROM ducklake_enable_vortex_maintenance('lake',2,67108864)")
        generation = first["reference"]["generation"]["generation"]
        connection.execute(
            "SELECT * FROM ducklake_rewrite_vortex_index_segment('lake','main','items','knn',"
            + quote(generation)
            + ","
            + quote(root / "indexes/rewrite.json")
            + ",3)"
        )
        source_idle(connection)
        require(not old_source.exists(), "Rewritten managed source was not collected")
        require(
            mutate(connection, "seed-'请求", seed) == original
            and mutate(connection, "replace", replacement, [1, 99999]) == receipt,
            "Source expiry lost managed receipts",
        )
        require(
            query(
                connection,
                "SELECT count(*) AS n FROM __ducklake_metadata_lake.ducklake_snapshot WHERE snapshot_id="
                + str(original["snapshot_id"]),
            )
            == [{"n": 0}],
            "Receipt retry did not exercise an expired original snapshot",
        )
        phases.append(BASE.qualify(connection, "rewrite-and-source-GC", recalls))
        connection.execute("SELECT * FROM ducklake_cancel_vortex_maintenance('lake')")
        checks.append(
            "table-scoped identity, explicit empty-table admission, independent B2, rewrite and source expiry preserve keyed state and receipts"
        )
    finally:
        connection.close()
    report.update(checks=checks, phases=phases, fixture_recalls=recalls, minimum_recall=min(recalls))


DRIVER_ROWS = [
    {"id": 0, "embedding": [0.125, 0], "category": "A", "payload": "更新"},
    {"id": 10000, "embedding": [0.25, 0], "category": None, "payload": None},
]


def recover(case, committed):
    connection = BASE.connect(case)
    try:
        expected = "更新" if committed else "seed-0"
        require(
            query(connection, "SELECT payload FROM lake.items WHERE id=0") == [{"payload": expected}],
            "Recovery exposed a partial upsert/delete",
        )
        old = query(connection, "SELECT * FROM " + WRITE.REQUESTS + " WHERE request_id='change'")
        require(
            len(old) == int(committed) and WRITE.count(connection) == 256,
            "Recovery separated mutation rows and receipt",
        )
        receipt = mutate(connection, "change", DRIVER_ROWS, [1, 99999])
        if committed:
            require(receipt == old[0], "Lost response changed the committed mutation receipt")
        require(
            mutate(connection, "change", DRIVER_ROWS, [1, 99999]) == receipt and WRITE.count(connection) == 256,
            "Recovered mutation retry duplicated current rows",
        )
        require(
            query(connection, "SELECT count(*) AS n FROM lake.items WHERE id=1") == [{"n": 0}],
            "Deleted B row reappeared after restart",
        )
        rows = query(connection, BASE.SEARCH.search_sql([0, 0]))
        BASE.SEARCH.qualify({"rows": rows}, BASE.raw(connection), [0, 0])
        WRITE.rejected(
            lambda: connection.execute("INSERT INTO lake.items VALUES(9,[9,0],'A','outside')"),
            "Managed Vortex tables require",
        )
    finally:
        connection.close()


def native_cases(driver, root, report):
    cases = []
    for mode in [
        "keyed-ok",
        "keyed-preexisting",
        "keyed-held",
        "keyed-fail-after_delete",
        "keyed-fail-after_write",
        "keyed-fail-before_commit",
        "keyed-fail-after_commit",
        "keyed-cancel-after_delete",
        "keyed-cancel-before_commit",
        "keyed-conflict",
    ]:
        case = root / mode
        case.mkdir(mode=0o700)
        with (case / "driver.log").open("w") as log:
            result = subprocess.run([str(driver), str(case), mode], stdout=log, stderr=subprocess.STDOUT, timeout=120)
        require(result.returncode == 0, f"Native {mode} failed: {(case / 'driver.log').read_text()}")
        cases.append(mode)
        print(f"Keyed native case passed: {mode}", flush=True)
    for stage in ["after_delete", "before_commit", "after_commit"]:
        case = root / ("kill-" + stage)
        case.mkdir(mode=0o700)
        with (case / "driver.log").open("w") as log:
            child = subprocess.Popen(
                [str(driver), str(case), "keyed-pause-" + stage], stdout=log, stderr=subprocess.STDOUT
            )
            try:
                deadline = time.monotonic() + 60
                while not (case / "checkpoint").exists():
                    require(
                        child.poll() is None, "Writer exited before checkpoint: " + (case / "driver.log").read_text()
                    )
                    require(time.monotonic() < deadline, "Writer checkpoint timed out")
                    time.sleep(0.02)
                wal = case / "metadata.ducklake.wal"
                require(wal.is_file() and wal.stat().st_size > 0, "Crash fixture has no native metadata WAL")
                child.kill()
                child.wait(timeout=10)
                require(child.returncode == -9, "Recovery did not exercise SIGKILL")
            finally:
                if child.poll() is None:
                    child.kill()
                    child.wait(timeout=10)
        with (case / "recovery.log").open("w") as log:
            result = subprocess.run(
                [
                    sys.executable,
                    "-I",
                    str(Path(__file__).resolve()),
                    "--recover-case",
                    str(case),
                    "--committed",
                    str(int(stage == "after_commit")),
                ],
                stdout=log,
                stderr=subprocess.STDOUT,
                timeout=120,
            )
        require(result.returncode == 0, f"Recovery {stage} failed: {(case / 'recovery.log').read_text()}")
        cases.append(stage + ": SIGKILL + installed recovery + exact retry")
    report["native_cases"] = cases


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--driver", type=Path)
    destination = parser.add_mutually_exclusive_group(required=True)
    destination.add_argument("--output-dir", type=Path)
    destination.add_argument("--recover-case", type=Path)
    parser.add_argument("--committed", type=int, choices=[0, 1])
    args = parser.parse_args()
    require(os.environ.get("VANE_RUNNER") == "local-fast", "Set VANE_RUNNER=local-fast explicitly")
    if args.recover_case:
        require(args.committed is not None, "Recovery requires an explicit expected commit outcome")
        recover(args.recover_case.resolve(), bool(args.committed))
        return
    require(args.driver is not None, "Set --driver for native checkpoints")
    root = args.output_dir.resolve()
    root.mkdir(mode=0o700, parents=True)
    report = {"status": "running"}
    started = time.monotonic()
    try:
        exercise(root, report)
        native_cases(args.driver.resolve(), root, report)
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
