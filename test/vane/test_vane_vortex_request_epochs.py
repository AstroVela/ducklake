#!/usr/bin/env python3
"""Qualify explicit receipt windows, atomic retirement and durable stale-request rejection."""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import time


def load(name, filename):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(filename))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


KEYED = load("epoch_keyed", "test_vane_vortex_keyed_writes.py")
PIN = load("epoch_affinity", "test_vane_vortex_source_gc_affinity.py")
SCRATCH = load("epoch_scratch", "vortex_query_scratch_model.py")
WRITE, BASE = KEYED.WRITE, KEYED.BASE
query, require, quote = BASE.query, BASE.require, BASE.quote
EPOCHS = "__ducklake_metadata_lake.ducklake_vortex_request_epochs_v1"


def status(connection, table="items"):
    return WRITE.query_parameters(
        connection, "SELECT * FROM ducklake_vortex_request_status('lake','main',?,'knn')", [table]
    )[0]


def advance(connection, epoch, table="items"):
    return WRITE.query_parameters(
        connection,
        "SELECT * FROM ducklake_advance_vortex_request_epoch('lake','main',?,'knn',?::UBIGINT,?::VARCHAR)",
        [table, epoch, WRITE.table_uuid(connection, table)],
    )[0]


def create_table(connection, root, table, managed=True):
    connection.execute(f"CREATE TABLE lake.{table}(id BIGINT,embedding FLOAT[],category VARCHAR,payload VARCHAR)")
    connection.execute(
        "CALL ducklake_set_option('lake','experimental_data_file_format','vortex',table_name=>" + quote(table) + ")"
    )
    connection.execute(
        "SELECT * FROM ducklake_create_vortex_index('lake','main',"
        + quote(table)
        + ",'knn',"
        + quote(root / "indexes")
        + ",8)"
    )
    if managed:
        KEYED.manage(connection, table)


def bootstrap(root):
    root.mkdir(mode=0o700)
    (root / "indexes").mkdir(mode=0o700)
    c = BASE.connect(root, create=True)
    c.execute("SELECT * FROM ducklake_initialize_vortex_writes('lake')")
    KEYED.manage(c)
    return c


def exercise(root, checks):
    connection = bootstrap(root)
    try:
        initial = status(connection)
        require(
            initial["request_epoch"] == 1 and initial["catalog_requests"] == 0,
            "New request window is not empty epoch 1",
        )
        original = WRITE.rows(0, 4)
        seed = KEYED.mutate(connection, "seed", original, epoch=1)
        require(
            seed["request_epoch"] == 1 and KEYED.mutate(connection, "seed", original, epoch=1) == seed,
            "Current receipt retry changed",
        )
        deletion = KEYED.mutate(connection, "delete", [], [0], epoch=1)
        replacement = [dict(original[1], payload="newer")]
        KEYED.mutate(connection, "replace", replacement, epoch=1)
        revived = [dict(original[0], payload="revived")]
        KEYED.mutate(connection, "revive", revived, epoch=1)
        require(
            KEYED.mutate(connection, "seed", original, epoch=1) == seed
            and KEYED.mutate(connection, "delete", [], [0], epoch=1) == deletion,
            "Old current-window retries changed receipts",
        )
        model = sorted([revived[0], replacement[0], *original[2:]], key=lambda r: r["id"])
        require(BASE.raw(connection) == model, "Current receipt retry reapplied old upserts/deletes")
        WRITE.rejected(lambda: KEYED.mutate(connection, "seed", replacement, epoch=1), "payload mismatch")
        checks.append("current-epoch retries preserve receipts and newer typed versions; mismatched payload fails")

        create_table(connection, root, "other")
        other_rows = WRITE.rows(1000, 1)
        other = KEYED.mutate(connection, "seed", other_rows, table="other", epoch=1)
        before_indexes = query(
            connection,
            "SELECT table_uuid,index_name,revision,reference FROM __ducklake_metadata_lake.ducklake_vortex_indexes_v1 ORDER BY table_uuid,index_name",
        )
        reader = connection.cursor()
        try:
            reader.execute("BEGIN")
            old_count = query(reader, "SELECT count(*) AS n FROM " + WRITE.REQUESTS)[0]["n"]
            retired = advance(connection, 1)
            require(
                retired
                == {"table_uuid": seed["table_uuid"], "previous_epoch": 1, "request_epoch": 2, "expired_requests": 4},
                "Retirement result does not match the closed window",
            )
            require(
                query(reader, "SELECT count(*) AS n FROM " + WRITE.REQUESTS)[0]["n"] == old_count,
                "Receipt retirement changed an ordinary reader's old snapshot",
            )
            reader.execute("COMMIT")
        finally:
            reader.close()
        require(
            status(connection)["retained_requests"] == 0
            and status(connection)["catalog_requests"] == 1
            and status(connection, "other")["request_epoch"] == 1,
            "Retirement crossed a table UUID boundary",
        )
        require(
            BASE.raw(connection) == model
            and before_indexes
            == query(
                connection,
                "SELECT table_uuid,index_name,revision,reference FROM __ducklake_metadata_lake.ducklake_vortex_indexes_v1 ORDER BY table_uuid,index_name",
            ),
            "Receipt retirement changed business data or index generations",
        )
        fresh_rows = WRITE.rows(100, 1)
        fresh = KEYED.mutate(connection, "seed", fresh_rows, epoch=2)
        model += fresh_rows
        require(
            advance(connection, 1) == retired and KEYED.mutate(connection, "seed", fresh_rows, epoch=2) == fresh,
            "Last advance retry cleared a newer receipt",
        )
        for request, values in (("seed", original), ("never-submitted", WRITE.rows(500, 1))):
            WRITE.rejected(lambda: KEYED.mutate(connection, request, values, epoch=1), "EXPIRED_REQUEST")
        WRITE.rejected(
            lambda: KEYED.mutate(connection, "future", WRITE.rows(501, 1), epoch=3), "does not match current epoch"
        )
        require(
            KEYED.mutate(connection, "seed", other_rows, table="other", epoch=1) == other,
            "Other table lost its current retry window",
        )
        checks.append(
            "table-scoped atomic retirement, ordinary reader isolation, same ID in new epoch and durable old/future rejection"
        )

        template = KEYED.mutate(connection, "capacity-template", [], [-999999], epoch=2)
        used = status(connection)["catalog_requests"]
        connection.execute(
            "INSERT INTO "
            + WRITE.REQUESTS
            + " SELECT "
            + quote(seed["table_uuid"])
            + ",'capacity-'||i::VARCHAR,"
            + quote(template["payload_hash"])
            + ",1,"
            + str(template["snapshot_id"])
            + ",2 FROM range("
            + str(100000 - used)
            + ") t(i)"
        )
        require(
            status(connection)["catalog_requests"] == 100000, "Capacity fixture did not fill the unchanged global limit"
        )
        require(
            KEYED.mutate(connection, "seed", fresh_rows, epoch=2) == fresh, "Full catalog rejected a committed retry"
        )
        WRITE.rejected(
            lambda: KEYED.mutate(connection, "blocked", WRITE.rows(600, 1), epoch=2), "request catalog budget"
        )
        WRITE.check_no_request(connection, "blocked")
        next_window = advance(connection, 2)
        require(
            next_window["expired_requests"] == 99999 and status(connection)["catalog_requests"] == 1,
            "Retirement did not release only the target table's bounded receipt slots",
        )
        extra = WRITE.rows(200, 1)
        newest = KEYED.mutate(connection, "available", extra, epoch=3)
        model += extra
        require(advance(connection, 2) == next_window, "Last advance retry changed after a new epoch-3 request")
        WRITE.rejected(lambda: advance(connection, 1), "does not match current epoch")
        WRITE.rejected(lambda: KEYED.mutate(connection, "blocked", WRITE.rows(600, 1), epoch=2), "EXPIRED_REQUEST")
        require(BASE.raw(connection) == model, "Full-capacity rejection or retirement changed current rows")
        checks.append(
            "100,000 catalog receipts retain existing retries; retirement frees slots without changing the limit or other tables"
        )

        create_table(connection, root, "empty")
        require(status(connection, "empty")["request_epoch"] == 1, "Empty table did not start at epoch 1")
        empty = advance(connection, 1, "empty")
        require(
            empty["expired_requests"] == 0 and advance(connection, 1, "empty") == empty,
            "Empty-window advance is not idempotent",
        )
        WRITE.rejected(
            lambda: KEYED.mutate(connection, "old-empty", WRITE.rows(0, 1), table="empty", epoch=1), "EXPIRED_REQUEST"
        )
        maximum = (1 << 64) - 1
        connection.execute(
            "UPDATE "
            + EPOCHS
            + " SET request_epoch="
            + str(maximum)
            + " WHERE table_uuid="
            + quote(empty["table_uuid"])
        )
        max_rows = WRITE.rows(700, 1)
        max_receipt = KEYED.mutate(connection, "maximum", max_rows, table="empty", epoch=maximum)
        WRITE.rejected(lambda: advance(connection, maximum, "empty"), "epoch is exhausted")
        require(
            KEYED.mutate(connection, "maximum", max_rows, table="empty", epoch=maximum) == max_receipt
            and status(connection, "empty")["request_epoch"] == maximum,
            "Epoch exhaustion reset the fence or changed a current receipt",
        )
        checks.append(
            "empty windows fence unseen old requests; uint64 exhaustion preserves the current window without wraparound"
        )

        create_table(connection, root, "append_only", managed=False)
        appended = WRITE.batch(connection, "append", WRITE.rows(800, 1), table="append_only", epoch=1)
        require(
            advance(connection, 1, "append_only")["expired_requests"] == 1,
            "Append receipt did not participate in retirement",
        )
        WRITE.rejected(
            lambda: WRITE.batch(connection, "append", WRITE.rows(800, 1), table="append_only", epoch=1),
            "EXPIRED_REQUEST",
        )
        require(
            WRITE.batch(connection, "append", WRITE.rows(801, 1), table="append_only", epoch=2)["request_epoch"] == 2,
            "Append did not admit a new-window operation",
        )
        checks.append("append and mutation share explicit epoch semantics; unmanaged ownership semantics stay explicit")

        WRITE.rejected(lambda: KEYED.mutate(connection, "zero", WRITE.rows(0, 1), epoch=0), "request epoch")
        WRITE.rejected(lambda: advance(connection, 0), "positive epoch")
        WRITE.rejected(lambda: advance(connection, -1), "range")
        WRITE.rejected(lambda: advance(connection, 1 << 64), "range")
        WRITE.rejected(lambda: KEYED.mutate(connection, "null", WRITE.rows(0, 1), epoch=None), "cannot be NULL")
        WRITE.rejected(
            lambda: WRITE.query_parameters(
                connection,
                "SELECT * FROM ducklake_mutate_vortex_batch('lake','main','items','knn','old-arity',?::STRUCT(id BIGINT,embedding FLOAT[],category VARCHAR,payload VARCHAR)[],[]::BIGINT[])",
                [WRITE.rows(0, 1)],
            ),
            "No function matches",
        )
        connection.execute("BEGIN")
        try:
            WRITE.rejected(lambda: advance(connection, 3), "autocommit")
        finally:
            connection.execute("ROLLBACK")
        require(
            BASE.raw(connection) == model and status(connection)["request_epoch"] == 3,
            "Invalid lifecycle input changed current state",
        )
        checks.append(
            "zero/negative/overflow/NULL epochs, old SQL arity and outer transactions reject without state changes"
        )
        connection.close()
        connection = BASE.connect(root)
        require(
            BASE.raw(connection) == model and status(connection)["request_epoch"] == 3,
            "Reopen lost epoch or typed versions",
        )
        require(
            KEYED.mutate(connection, "available", extra, epoch=3) == newest and advance(connection, 2) == next_window,
            "Reopen changed current receipts or last-advance replay",
        )
        WRITE.rejected(lambda: KEYED.mutate(connection, "seed", original, epoch=1), "EXPIRED_REQUEST")
        require(status(connection, "empty")["request_epoch"] == maximum, "Reopen changed uint64 epoch state")
        checks.append(
            "reopen preserves current receipts, last advance result, table-scoped epochs and stale-request rejection"
        )
    finally:
        connection.close()


def legacy(root, checks):
    c = bootstrap(root)
    try:
        rows = WRITE.rows(0, 1)
        KEYED.mutate(c, "old", rows, epoch=1)
        c.execute(
            "CREATE TABLE __ducklake_metadata_lake.ducklake_vortex_write_requests_v1 AS SELECT table_uuid,request_id,payload_hash,row_count,snapshot_id FROM "
            + WRITE.REQUESTS
        )
        c.execute("DROP TABLE " + WRITE.REQUESTS)
        c.execute("DROP TABLE " + EPOCHS)
        WRITE.rejected(lambda: KEYED.mutate(c, "new", rows, epoch=1), "not initialized")
        WRITE.rejected(lambda: advance(c, 1), "not initialized")
        WRITE.rejected(
            lambda: c.execute("INSERT INTO lake.items VALUES(99,[0,0,0,0,0,0,0,0],'A','outside')"),
            "Managed Vortex tables require",
        )
        WRITE.rejected(
            lambda: c.execute("SELECT * FROM ducklake_initialize_vortex_writes('lake')"), "fresh write catalog"
        )
        require(BASE.raw(c) == rows, "Legacy rejection changed managed rows")
        c.execute("DROP TABLE __ducklake_metadata_lake.ducklake_vortex_keyed_tables_v1")
        WRITE.rejected(
            lambda: c.execute("SELECT * FROM ducklake_initialize_vortex_writes('lake')"), "fresh write catalog"
        )
        checks.append(
            "legacy managed layout keeps ordinary-write protection; legacy receipts, including pre-managed layout, cannot be silently reinitialized"
        )
    finally:
        c.close()


def recover(case, committed):
    c = BASE.connect(case)
    try:
        state = status(c)
        require(
            state["request_epoch"] == (2 if committed else 1) and state["retained_requests"] == (0 if committed else 2),
            "WAL recovery separated the epoch fence from receipt deletion",
        )
        require(
            query(c, "SELECT id,payload FROM lake.items ORDER BY id") == [{"id": 1, "payload": "current"}],
            "Epoch crash recovery changed business versions",
        )
        retired = advance(c, 1)
        require(
            retired["request_epoch"] == 2 and retired["expired_requests"] == 2,
            "Recovered advance did not produce its original result",
        )
        changed = [{"id": 3, "embedding": [3.0, 0.0], "category": "B", "payload": "new-window"}]
        receipt = KEYED.mutate(c, "new-window", changed, epoch=2)
        require(
            advance(c, 1) == retired and KEYED.mutate(c, "new-window", changed, epoch=2) == receipt,
            "Recovered advance replay deleted a newer receipt",
        )
        WRITE.rejected(lambda: KEYED.mutate(c, "unseen-old", changed, epoch=1), "EXPIRED_REQUEST")
        require(status(c)["retained_requests"] == 1, "Recovered stale request left a receipt")
    finally:
        c.close()


def native(driver, root, report):
    root.mkdir(mode=0o700)
    modes = (
        "ok",
        "write-first",
        "advance-first",
        "parallel-advance",
        "fail-after_prune",
        "fail-after_epoch",
        "fail-before_commit",
        "fail-after_commit",
        "cancel",
        "conflict",
    )
    for mode in modes:
        case = root / mode
        case.mkdir(mode=0o700)
        with (case / "driver.log").open("w") as log:
            result = subprocess.run([str(driver), str(case), "epoch-" + mode], stdout=log, stderr=subprocess.STDOUT)
        require(result.returncode == 0, f"Native epoch case failed: {case / 'driver.log'}")
        report["native_cases"].append(mode)
        print("native", mode, "passed", flush=True)
    for stage in ("after_prune", "after_epoch", "before_commit", "after_commit"):
        case = root / ("crash-" + stage)
        case.mkdir(mode=0o700)
        with (case / "driver.log").open("w") as log:
            process = subprocess.Popen(
                [str(driver), str(case), "epoch-pause-" + stage], stdout=log, stderr=subprocess.STDOUT
            )
            try:
                deadline = time.monotonic() + 120
                while not (case / "checkpoint").is_file():
                    require(
                        process.poll() is None and time.monotonic() < deadline,
                        "Epoch crash fixture did not reach its checkpoint",
                    )
                    time.sleep(0.01)
                require((case / "checkpoint").read_text() == stage, "Wrong epoch crash boundary")
                wal = case / "metadata.ducklake.wal"
                require(wal.is_file() and wal.stat().st_size > 0, "Epoch recovery did not exercise a metadata WAL")
                process.kill()
                require(process.wait(timeout=20) == -9, "Epoch crash did not use SIGKILL")
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=20)
        with (case / "recovery.log").open("w") as log:
            result = subprocess.run(
                [
                    sys.executable,
                    "-I",
                    str(Path(__file__).resolve()),
                    "--recover",
                    str(case),
                    "--committed",
                    str(int(stage == "after_commit")),
                ],
                stdout=log,
                stderr=subprocess.STDOUT,
            )
        require(result.returncode == 0, f"Epoch recovery failed: {case / 'recovery.log'}")
        report["native_cases"].append("SIGKILL-" + stage)
        print("recovery", stage, "passed", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--driver", type=Path)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--cpus", type=PIN.parse_cpus)
    parser.add_argument("--recover", type=Path)
    parser.add_argument("--committed", type=int, choices=(0, 1))
    args = parser.parse_args()
    if args.recover:
        recover(args.recover.resolve(), bool(args.committed))
        return
    require(
        args.driver is not None and args.output_dir is not None and args.cpus is not None,
        "Require driver, output directory and CPU set",
    )
    root = args.output_dir.resolve()
    root.mkdir(mode=0o700)
    private = root / "private"
    private.mkdir(mode=0o700)
    os.environ["TMPDIR"] = str(private)
    original = sorted(os.sched_getaffinity(0))
    threads = PIN.thread_environment()
    repo = Path(__file__).resolve().parents[2]
    tracked = [
        Path(__file__),
        Path(KEYED.__file__),
        Path(WRITE.__file__),
        Path(BASE.__file__),
        repo / "src/storage/ducklake_vortex_write.cpp",
        repo / "src/functions/ducklake_vortex_write_function.cpp",
        repo / "src/include/storage/ducklake_vortex_write.hpp",
        repo / "test/vane/native_vortex_epoch_fixture.hpp",
    ]
    hashes = {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in tracked}
    report = {
        "status": "running",
        "mode": "functional_only",
        "checks": [],
        "native_cases": [],
        "original_affinity": original,
        "thread_environment": threads,
        "driver_sha256": hashlib.sha256(args.driver.read_bytes()).hexdigest(),
        "source_sha256": hashes,
        "acceptance": "Atomic epochs/receipts/data, explicit stale rejection, exact retries and deterministic recovery; no performance gate.",
    }
    started = time.monotonic()
    try:
        with PIN.pinned_affinity(args.cpus):
            report["applied_affinity"] = sorted(os.sched_getaffinity(0))
            exercise(root / "public", report["checks"])
            legacy(root / "legacy", report["checks"])
            native(args.driver.resolve(), root / "native", report)
            SCRATCH.assert_released(private)
            require(PIN.thread_environment() == threads, "Thread settings changed")
            require(
                all(hashlib.sha256(p.read_bytes()).hexdigest() == hashes[str(p)] for p in tracked),
                "Epoch implementation/test source changed during proof",
            )
        report["status"] = "passed"
    except BaseException as error:
        report.update(status="failed", error=f"{type(error).__name__}: {error}")
        raise
    finally:
        report["restored_affinity"] = sorted(os.sched_getaffinity(0))
        report["seconds_observation_only"] = time.monotonic() - started
        (root / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
