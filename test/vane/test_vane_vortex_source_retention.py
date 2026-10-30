#!/usr/bin/env python3
"""Qualify bounded source retention through installed Vane and native checkpoints."""
from __future__ import annotations

import argparse
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


def status(connection):
    return query(connection, "SELECT * FROM ducklake_vortex_source_retention_status('lake')")[0]


def collect(connection, objects=16, size=67108864):
    return query(connection, f"SELECT * FROM ducklake_collect_vortex_sources('lake',{objects},{size})")[0]


def policy(connection, enabled=True, keep=1, age=0):
    connection.execute(
        f"SELECT * FROM ducklake_set_vortex_source_retention('lake',{str(enabled).lower()},{keep},{age})"
    )


def idle(connection):
    deadline = time.monotonic() + 60
    while True:
        source = status(connection)
        maintenance = query(connection, "SELECT * FROM ducklake_vortex_maintenance_status('lake')")[0]
        require(maintenance["state"] != "failed", f"Maintenance failed: {maintenance}")
        if not maintenance["running"] and not any(
            source[name] for name in ["eligible_snapshots", "scheduled_files", "pending_deletions"]
        ):
            return source
        require(time.monotonic() < deadline, f"Source retention did not converge: {source}, {maintenance}")
        time.sleep(0.02)


def exercise(root, report):
    (root / "indexes").mkdir()
    connection = BASE.connect(root, create=True)
    recalls, phases, checks = [], [], []
    reader = None
    try:
        require(not status(connection)["enabled"], "Source retention was implicitly enabled")
        require(collect(connection)["expired_snapshots"] == 0, "Disabled source policy expired snapshots")
        WRITE.rejected(lambda: policy(connection, keep=0), "Invalid Vortex source retention")
        WRITE.rejected(lambda: policy(connection, age=-1), "Invalid Vortex source retention")
        WRITE.rejected(lambda: collect(connection, objects=17), "budgets")
        connection.execute("SELECT * FROM ducklake_initialize_vortex_writes('lake')")
        original = WRITE.rows(1000, 128)
        receipt = WRITE.batch(connection, "seed", original)
        connection.execute("CALL ducklake_flush_inlined_data('lake',table_name=>'items')")
        connection.execute(
            "SELECT * FROM ducklake_append_vortex_index_segment('lake','main','items','knn',"
            + quote(root / "indexes/seed.json")
            + ",8,1)"
        )
        old_source = next((root / "data").rglob("*.vortex"))
        sentinel = root / "data/unregistered.keep"
        sentinel.write_bytes(b"not-owned")
        connection.execute("PREPARE nearest AS " + BASE.SEARCH.search_sql(BASE.VECTOR))
        phases.append(BASE.qualify(connection, "seed-B", recalls))
        connection.execute(
            "SELECT * FROM ducklake_enable_vortex_optimizer('lake','main','items','knn',"
            + quote(root / "indexes")
            + ",8,100000,4194304,100000,67108864)"
        )
        connection.execute("SELECT * FROM ducklake_cancel_vortex_optimizer('lake','main','items','knn')")
        connection.execute(
            "SELECT * FROM ducklake_set_vortex_write_budget('lake','main','items','knn',1000,4194304,4096,67108864)"
        )
        deadline = time.monotonic() + 30
        while BASE.status(connection)["running"]:
            require(time.monotonic() < deadline, "Seed optimizer did not stop")
            time.sleep(0.01)

        reader = connection.cursor()
        reader.execute("BEGIN")
        old_rows = BASE.raw(reader)
        WRITE.batch(
            connection, "new-T", [{"id": 90000, "embedding": BASE.VECTOR, "category": "A", "payload": "新实时"}]
        )
        connection.execute("DELETE FROM lake.items WHERE id<1032")
        head = BASE.head(connection)
        generation = head["reference"]["segments"][0]["reference"]["generation"]["generation"]
        connection.execute(
            "SELECT * FROM ducklake_rewrite_vortex_index_segment('lake','main','items','knn',"
            + quote(generation)
            + ","
            + quote(root / "indexes/rewrite.json")
            + ",2)"
        )
        policy(connection)
        require(collect(connection)["blocked_by_transactions"], "An active native reader did not defer collection")
        require(BASE.raw(reader) == old_rows and old_source.is_file(), "An old native transaction lost its source")
        connection.execute("SELECT * FROM ducklake_enable_vortex_maintenance('lake',2,67108864)")
        deadline = time.monotonic() + 30
        while True:
            maintenance = query(connection, "SELECT * FROM ducklake_vortex_maintenance_status('lake')")[0]
            require(maintenance["state"] != "failed", str(maintenance))
            if (
                not maintenance["running"]
                and not maintenance["remaining_builds"]
                and not maintenance["remaining_generations"]
            ):
                break
            require(time.monotonic() < deadline, "Artifact maintenance did not finish around the held reader")
            time.sleep(0.02)
        runs = maintenance["run_count"]
        time.sleep(0.2)
        require(
            query(connection, "SELECT run_count FROM ducklake_vortex_maintenance_status('lake')")[0]["run_count"]
            == runs,
            "Blocked source retention generated empty maintenance runs",
        )
        reader.execute("COMMIT")
        reader.close()
        reader = None
        completed = idle(connection)
        require(
            not old_source.exists() and completed["collected_files"] > 0, "Background source file was not reclaimed"
        )
        require(
            WRITE.batch(connection, "seed", original) == receipt and WRITE.count(connection) == 97,
            "Source expiry changed a receipt or replayed old business rows",
        )
        require(
            query(
                connection,
                "SELECT count(*) AS n FROM __ducklake_metadata_lake.ducklake_snapshot WHERE snapshot_id="
                + str(receipt["snapshot_id"]),
            )
            == [{"n": 0}],
            "Original snapshot was not expired",
        )
        require(sentinel.read_bytes() == b"not-owned", "Collector removed an unregistered source file")
        phases.append(BASE.qualify(connection, "source-GC-B-plus-T", recalls))
        checks.append(
            "default off, bounded policy validation, active transaction deferral without idle write churn, and background source GC"
        )
        checks.append(
            "expired original snapshot retains exact WriteBatch receipt; current B/T and unregistered data survive"
        )

        policy(connection, enabled=False)
        WRITE.batch(connection, "extra", WRITE.rows(100000, 32))
        before = query(connection, "SELECT count(*) AS n FROM __ducklake_metadata_lake.ducklake_snapshot")[0]["n"]
        time.sleep(0.15)
        require(
            query(connection, "SELECT count(*) AS n FROM __ducklake_metadata_lake.ducklake_snapshot")
            == [{"n": before}],
            "Disabled retention changed snapshots",
        )
        policy(connection, keep=2, age=315360000)
        require(status(connection)["eligible_snapshots"] == 0, "Age retention did not protect snapshots")
        policy(connection, keep=2)
        idle(connection)
        require(
            query(connection, "SELECT count(*) AS n FROM __ducklake_metadata_lake.ducklake_snapshot") == [{"n": 2}],
            "Snapshot count retention did not converge",
        )
        phases.append(BASE.qualify(connection, "retention-reconfigured", recalls))
        checks.append("disable and age/count retention preserve their configured history boundaries")

        connection.execute("SELECT * FROM ducklake_cancel_vortex_maintenance('lake')")
        policy(connection, enabled=False)
        connection.execute("DELETE FROM lake.items WHERE id>=1032 AND id<1064")
        head = BASE.head(connection)
        generation = head["reference"]["segments"][0]["reference"]["generation"]["generation"]
        connection.execute(
            "SELECT * FROM ducklake_rewrite_vortex_index_segment_to_f('lake','main','items','knn',"
            + quote(generation)
            + ","
            + str(head["revision"])
            + ")"
        )
        policy(connection)
        collect(connection)
        require(
            collect(connection, objects=1, size=1)["collected_files"] == 0,
            "Source collector exceeded a configured byte bound",
        )
        connection.execute("SELECT * FROM ducklake_resume_vortex_maintenance('lake')")
        idle(connection)
        phases.append(BASE.qualify(connection, "source-GC-F-plus-T", recalls))
        require(WRITE.batch(connection, "seed", original) == receipt, "F rewrite or source GC changed the old receipt")
        connection.execute("SELECT * FROM ducklake_cancel_vortex_maintenance('lake')")
        checks.append(
            "bounded explicit pass, source rewrite to F, and explicit maintenance resume preserve query and request semantics"
        )
    finally:
        if reader is not None:
            reader.close()
        connection.close()
    report.update(checks=checks, phases=phases, fixture_recalls=recalls, minimum_recall=min(recalls))


def recover(case):
    connection = BASE.connect(case)
    try:
        maintenance = query(connection, "SELECT * FROM ducklake_vortex_maintenance_status('lake')")[0]
        require(not maintenance["running"], "Restart automatically resumed source cleanup")
        connection.execute("SELECT * FROM ducklake_resume_vortex_maintenance('lake')")
        idle(connection)
        result = query(
            connection, "SELECT id FROM ducklake_vortex_search('lake','main','items','knn',[0,0]::FLOAT[],1)"
        )
        require(result == [{"id": 32}], "Recovered source worker changed current ANN visibility")
        require(WRITE.count(connection) == 96, "Recovered source worker changed typed current rows")
        connection.execute("SELECT * FROM ducklake_cancel_vortex_maintenance('lake')")
    finally:
        connection.close()


def native_cases(driver, root, report):
    cases = []
    for mode in [
        "age",
        "defer",
        "held",
        "admission",
        "budget",
        "live",
        "gate",
        "checksum",
        "symlink",
        "manual",
        "cancel",
        "concurrent",
        "fail-source_expiry_committed",
        "fail-source_delete_intent_committed",
        "fail-source_after_unlink",
        "fail-source_delete_completed",
    ]:
        case = root / mode
        case.mkdir(mode=0o700)
        with (case / "driver.log").open("w") as log:
            result = subprocess.run([str(driver), str(case), mode], stdout=log, stderr=subprocess.STDOUT, timeout=120)
        require(result.returncode == 0, f"Native {mode} failed: {(case / 'driver.log').read_text()}")
        cases.append(mode)
        print(f"Native source case passed: {mode}", flush=True)
    for mode in [
        "background-pause-source_expiry_committed",
        "background-pause-source_delete_intent_committed",
        "background-pause-source_after_unlink",
        "background-pause-source_delete_completed",
    ]:
        case = root / mode
        case.mkdir(mode=0o700)
        with (case / "driver.log").open("w") as log:
            child = subprocess.Popen([str(driver), str(case), mode], stdout=log, stderr=subprocess.STDOUT)
            try:
                deadline = time.monotonic() + 60
                while not (case / "checkpoint").exists():
                    require(
                        child.poll() is None,
                        "Source worker exited before checkpoint: " + (case / "driver.log").read_text(),
                    )
                    require(time.monotonic() < deadline, "Source worker did not reach checkpoint")
                    time.sleep(0.02)
                wal = case / "metadata.ducklake.wal"
                require(
                    wal.is_file() and wal.stat().st_size > 0,
                    "Source worker kill did not exercise metadata WAL recovery",
                )
                child.kill()
                child.wait(timeout=10)
                require(child.returncode == -9, "Source worker recovery did not exercise SIGKILL")
            finally:
                if child.poll() is None:
                    child.kill()
                    child.wait(timeout=10)
        with (case / "recovery.log").open("w") as log:
            result = subprocess.run(
                [sys.executable, "-I", str(Path(__file__).resolve()), "--recover-case", str(case)],
                stdout=log,
                stderr=subprocess.STDOUT,
                timeout=120,
            )
        require(result.returncode == 0, f"Recovery {mode} failed: {(case / 'recovery.log').read_text()}")
        cases.append(mode + ": SIGKILL + installed Vane recovery + explicit resume")
        print(f"Installed source recovery passed: {mode}", flush=True)
    report["native_cases"] = cases


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--driver", type=Path)
    destination = parser.add_mutually_exclusive_group(required=True)
    destination.add_argument("--output-dir", type=Path)
    destination.add_argument("--recover-case", type=Path)
    args = parser.parse_args()
    require(os.environ.get("VANE_RUNNER") == "local-fast", "Set VANE_RUNNER=local-fast explicitly")
    if args.recover_case:
        recover(args.recover_case.resolve())
        return
    require(args.driver is not None, "Set --driver for native checkpoints")
    root = args.output_dir.resolve()
    root.mkdir(mode=0o700, parents=True, exist_ok=False)
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
