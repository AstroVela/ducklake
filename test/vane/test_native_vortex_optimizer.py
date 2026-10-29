#!/usr/bin/env python3
"""Qualify the real background optimizer, concurrent search and durable task recovery."""
from __future__ import annotations

import argparse
import importlib.util
import json
import os
from pathlib import Path
import time

SPEC = importlib.util.spec_from_file_location("native_build", Path(__file__).with_name("test_native_vortex_build.py"))
assert SPEC is not None and SPEC.loader is not None
BUILD = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BUILD)
CAT = BUILD.CAT
BASE = BUILD.BASE
SEARCH = BUILD.SEARCH
require = BASE.require


def status(native):
    rows = native.call("optimizer-status")
    require(len(rows) == 1, f"Expected one optimizer policy: {rows}")
    return rows[0]


def wait(native, predicate, label):
    deadline = time.monotonic() + 60
    while True:
        row = status(native)
        if predicate(row):
            return row
        require(row["state"] != "failed", f"Optimizer failed while waiting for {label}: {row}")
        require(time.monotonic() < deadline, f"Timed out waiting for {label}: {row}")
        time.sleep(0.03)


def steady(native, row):
    time.sleep(0.4)
    current = status(native)
    require(current["run_count"] == row["run_count"], f"Optimizer repeated a completed/paused task: {current}")
    return current


def insert(native, start, count):
    native.sql(
        f"INSERT INTO lake.items SELECT {start}+i,[({start}+i)::FLOAT,((i*4)%12)::FLOAT,"
        "((i*5)%13)::FLOAT,0::FLOAT,1::FLOAT,8::FLOAT,7::FLOAT,16::FLOAT],"
        f"CASE WHEN i%4=0 THEN 'B' ELSE 'A' END,'optimizer-row' FROM range({count})t(i);"
    )


def initialize(native):
    native.call("init")
    native.call("optimizer-init")


def crash(native, stage, before_state="publishing"):
    marker = native.root / f"optimizer-{stage}.ready"
    deadline = time.monotonic() + 60
    while not marker.exists():
        require(native.process.poll() is None, "Optimizer process exited before crash boundary")
        require(time.monotonic() < deadline, f"Crash boundary timed out: {stage}")
        time.sleep(0.01)
    durable = status(native)
    require(
        durable["state"] == before_state
        and (before_state != "publishing" or durable["candidate_generation"] or durable["task_kind"] == "retire"),
        "Task checkpoint was not durable before crash",
    )
    native.process.kill()
    native.process.wait(timeout=10)
    native.log.close()
    require(native.process.returncode == -9, "Optimizer was not killed with SIGKILL")
    native.start("resume")
    resumed = status(native)
    require(not resumed["running"], "Restart automatically replayed an unfinished task")
    return durable


def exercise(native, root, checks):
    artifacts = root / "artifacts"
    artifacts.mkdir()
    native.sql(
        "DROP TABLE lake.items;CREATE TABLE lake.items(id BIGINT,embedding FLOAT[],category VARCHAR,payload VARCHAR);"
        "CALL ducklake_set_option('lake','experimental_data_file_format','vortex',table_name=>'items');"
        "CALL ducklake_set_option('lake','data_inlining_row_limit',1000,table_name=>'items');"
    )
    initialize(native)
    native.call("optimizer-enable", artifacts, 128, 4194304, 128, 67108864)
    native.call("optimizer-enable", artifacts, 128, 4194304, 128, 67108864, expected="Duplicate key")
    native.call("optimizer-resume", "build_started", "pause")
    insert(native, 0, 64)
    low = wait(native, lambda s: s["inline_rows"] == 64, "sub-threshold inline visibility")
    require(low["run_count"] == 0 and low["state"] == "idle", "Optimizer flushed below its row/byte thresholds")
    steady(native, low)
    insert(native, 64, 192)
    native.call("optimizer-wait")
    first = status(native)
    require(first["run_count"] == 1 and first["running"], "T-to-F-to-B task was not running in the background")
    native.call("optimizer-resume", expected="still running")
    native.sql("CALL ducklake_set_option('lake','data_inlining_row_limit',32,table_name=>'items');")
    insert(native, 5000, 64)
    native.sql(
        "BEGIN;UPDATE lake.items SET embedding=[900000,0,0,0,0,0,0,0],payload='updated' WHERE id=133;"
        "DELETE FROM lake.items WHERE id=134;"
        "INSERT INTO lake.items VALUES(200000,[133.0625,4,2,0,1,8,7,16],'A','T-live');COMMIT;"
        "SELECT CASE WHEN (SELECT id FROM lake.items ORDER BY list_distance(embedding,"
        "[133,4,2,0,1,8,7,16]::FLOAT[]) LIMIT 1)=200000 THEN true ELSE error('Exact live query failed') END;"
    )
    steady(native, first)
    native.call("optimizer-release")
    published = wait(
        native, lambda s: s["state"] == "published" and s["published_revision"] == 1, "first automatic publication"
    )
    require(published["run_count"] == 1, "Concurrent writes launched duplicate optimizer jobs")
    recalls = []

    def query(label):
        native.call("raw", label)
        rows = native.rows(label)
        core = native.call("core-search")
        recalls.append(SEARCH.qualify(core, rows, BUILD.VECTOR))
        sql = SEARCH.query(native, SEARCH.search_sql(BUILD.VECTOR))
        recalls.append(SEARCH.qualify(sql, rows, BUILD.VECTOR))
        require(sql["rows"] == core["rows"], "Background publication changed SQL/core results")
        return core["diagnostics"]

    mixed = query("first-published")
    require(mixed["index_revision"] == 1, "Unexpected ready revision after first promotion")
    require(
        mixed["routes"]["indexed_files"] and mixed["routes"]["scan_files"] and mixed["routes"]["inline_rows"],
        "Post-capture F/T did not remain queryable beside B",
    )
    checks.append(
        "threshold idle, automatic T flush and F indexing; concurrent exact/ANN queries, update/delete and post-capture F/T"
    )

    insert(native, 6000, 64)
    wait(native, lambda s: s["published_revision"] == 2 and s["state"] == "published", "second automatic F promotion")
    promoted = query("second-published")
    require(
        not promoted["routes"]["scan_files"] and promoted["routes"]["inline_rows"],
        "Automatic F promotion lost T or retained old F",
    )

    explicit_rebuilds = []

    def make_room():
        head = native.call("head", "knn")
        if len(head["reference"]["segments"]) == 2:
            native.call("optimizer-cancel")
            wait(native, lambda state: not state["running"], "idle before explicit consolidation")
            rebuilt = native.call(
                "build", "knn", artifacts / f"explicit-consolidation-{head['revision']}.json", head["revision"]
            )
            head = native.call("head", "knn")
            require(
                head["revision"] == rebuilt["revision"] and len(head["reference"]["segments"]) == 1,
                "Explicit consolidation did not restore one segment",
            )
            explicit_rebuilds.append(head["revision"])
        return head["revision"]

    revision = make_room()
    native.call("optimizer-resume", "build_started", "pause")
    insert(native, 7000, 128)
    native.call("optimizer-wait")
    cancelled = status(native)
    native.call("optimizer-cancel")
    native.call("optimizer-release")
    cancelled = wait(native, lambda s: s["state"] == "cancelled" and not s["running"], "cooperative cancellation")
    require(
        not cancelled["enabled"] and native.call("head", "knn")["revision"] == revision,
        "Cancellation changed the ready head",
    )
    steady(native, cancelled)
    query("cancelled-query")
    native.call("optimizer-resume")
    wait(
        native,
        lambda s: s["published_revision"] == revision + 1 and s["state"] == "published",
        "explicit resume after cancellation",
    )
    checks.append(
        "one active job; cancellation retains old ready head, stops policy and explicit resume plans a new task"
    )

    revision = make_room()
    native.call("optimizer-resume", "before_build_publish", "fail")
    insert(native, 8000, 128)
    failed = wait(native, lambda s: s["state"] == "failed" and not s["running"], "terminal task failure")
    require(
        not failed["enabled"] and "Injected optimizer failure" in failed["error"],
        "Failure was not durable and inspectable",
    )
    require(native.call("head", "knn")["revision"] == revision, "Failure exposed an unpublished candidate")
    steady(native, failed)
    native.call("optimizer-resume")
    wait(
        native,
        lambda s: s["published_revision"] == revision + 1 and s["state"] == "published",
        "explicit resume after failure",
    )
    checks.append(
        "build failure stops the policy without retry/fallback; status records the failed task and explicit resume succeeds"
    )

    revision = make_room()
    native.call("optimizer-resume", "after_commit", "fail")
    insert(native, 9000, 128)
    acknowledged = wait(
        native,
        lambda s: s["published_revision"] == revision + 1 and s["state"] == "published",
        "lost publication ACK reconciliation",
    )
    require(
        acknowledged["enabled"] and acknowledged["error"], "Committed publication was treated as an uncommitted failure"
    )
    steady(native, acknowledged)
    query("lost-ack-query")

    crash_runs = {}
    for stage, first_id in [("before_commit", 10000), ("after_commit", 11000)]:
        revision = make_room()
        native.call("optimizer-resume", stage, "pause")
        insert(native, first_id, 128)
        durable = crash(native, stage)
        require(
            native.call("head", "knn")["revision"] == revision + (stage == "after_commit"),
            "Crash recovered an incorrect ready head",
        )
        steady(native, status(native))
        native.call("optimizer-resume")
        revision += 1
        recovered = wait(
            native,
            lambda s: s["state"] == "published" and s["published_revision"] == revision,
            "restart reconciliation at " + stage,
        )
        delta = 1 if stage == "before_commit" else 0
        require(recovered["run_count"] == durable["run_count"] + delta, "Restart blindly replayed a published task")
        require(
            (recovered["candidate_generation"] == durable["candidate_generation"]) == (delta == 0),
            "Restart reused an uncommitted candidate or replaced a committed generation",
        )
        steady(native, recovered)
        query(stage + "-recovered")
        crash_runs[stage] = recovered["run_count"]
    checks.append(
        "lost ACK and SIGKILL before/after commit reconcile ready generation; committed tasks are not rebuilt, unfinished tasks replan"
    )

    native.call("optimizer-resume", "optimizer_before_build", "pause")
    insert(native, 12000, 128)
    native.call("optimizer-wait")
    native.call("build-wrong-field", artifacts / "wrong-field.json", expected="identity changed")
    native.sql(
        "DROP TABLE lake.items;CREATE TABLE lake.items(id BIGINT,embedding FLOAT[],category VARCHAR,payload VARCHAR);"
        "CALL ducklake_set_option('lake','experimental_data_file_format','vortex',table_name=>'items');"
    )
    native.call("optimizer-release")
    invalid = wait(native, lambda s: s["state"] == "failed", "table identity rejection")
    require(not invalid["enabled"] and "identity changed" in invalid["error"], "Changed vector identity was accepted")
    native.call("optimizer-resume", expected="No Vortex optimizer policy")
    checks.append(
        "capture rejects a mismatched vector field ID; drop/recreate rejects a scheduled task bound to the old table UUID"
    )
    return {
        "fixture_recalls": recalls,
        "recall_threshold": 0.8,
        "final_revision": revision,
        "restart_run_counts": crash_runs,
        "explicit_consolidation_revisions": explicit_rebuilds,
    }


def bytes_and_close(driver, root, checks):
    native = CAT.Native(driver, root / "bytes")
    try:
        artifacts = root / "bytes-artifacts"
        artifacts.mkdir()
        initialize(native)
        native.call("optimizer-enable", artifacts, 100000, 1, 100000, 1)
        wait(native, lambda s: s["published_revision"] == 1, "F byte threshold")
        native.sql("PREPARE optimizer_search AS " + SEARCH.search_sql(BUILD.VECTOR) + ";EXECUTE optimizer_search;")
        native.sql("CALL ducklake_set_option('lake','data_inlining_row_limit',1000,table_name=>'items');")
        insert(native, 20000, 128)
        wait(native, lambda s: s["published_revision"] == 2, "T byte threshold and F byte promotion")
        native.sql("CALL ducklake_set_option('lake','auto_compact',false,table_name=>'items');")
        stopped = wait(native, lambda s: s["state"] == "failed", "invalid flush configuration rejection")
        require(
            not stopped["enabled"] and "auto_compact" in stopped["error"], "Old success masked a new planning failure"
        )
        steady(native, stopped)
        native.sql("CALL ducklake_set_option('lake','auto_compact',true,table_name=>'items');")
        native.call("build", "knn", artifacts / "explicit-consolidation.json", 2)
        native.call("optimizer-resume", "optimizer_before_build", "pause")
        native.sql("CALL ducklake_set_option('lake','data_inlining_row_limit',0,table_name=>'items');")
        insert(native, 21000, 128)
        native.call("optimizer-wait")
        native.sql("DEALLOCATE optimizer_search;")
        native.finish()
        require(native.process.returncode == 0, "Active optimizer retained its database after close")
        native.start("resume")
        durable = status(native)
        require(not durable["running"], "Reopened database started jobs implicitly")
        native.call("optimizer-resume")
        wait(native, lambda s: s["published_revision"] == 4, "reopened optimizer ownership")
        native.finish()
        require(native.process.returncode == 0, "Idle optimizer retained its database after close")
        checks.append(
            "row and byte thresholds trigger independently; active and idle owner close release database and worker ownership"
        )
        return {"byte_thresholds": "passed", "active_and_idle_close": "passed"}
    finally:
        if native.process.poll() is None:
            native.process.kill()
            native.process.wait(timeout=5)


def table_identity(driver, root, checks):
    native = CAT.Native(driver, root / "identity")
    try:
        artifacts = root / "identity-artifacts"
        artifacts.mkdir()
        initialize(native)
        native.call("optimizer-enable", artifacts, 128, 4194304, 128, 67108864, "optimizer_before_build", "pause")
        native.call("optimizer-wait")
        native.sql(
            "DROP TABLE lake.items;CREATE TABLE lake.items(id BIGINT,embedding FLOAT[],category VARCHAR,payload VARCHAR);"
            "CALL ducklake_set_option('lake','experimental_data_file_format','vortex',table_name=>'items');"
        )
        native.call("optimizer-release")
        invalid = wait(native, lambda s: s["state"] == "failed", "table UUID replacement rejection")
        require(
            not invalid["enabled"] and "identity changed" in invalid["error"], "Recreated table accepted an old task"
        )
        native.call("optimizer-resume", expected="No Vortex optimizer policy")
        native.call("head", "knn", expected="No published Vortex index")
        native.finish()
        checks.append(
            "drop/recreate before capture fails the old UUID-bound task and cannot resume it against the new table"
        )
        return {"table_identity": "passed"}
    finally:
        if native.process.poll() is None:
            native.process.kill()
            native.process.wait(timeout=5)


def flush_recovery(driver, root, checks):
    native = CAT.Native(driver, root / "flush")
    try:
        artifacts = root / "flush-artifacts"
        artifacts.mkdir()
        native.sql(
            "DROP TABLE lake.items;CREATE TABLE lake.items(id BIGINT,embedding FLOAT[],category VARCHAR,payload VARCHAR);"
            "CALL ducklake_set_option('lake','experimental_data_file_format','vortex',table_name=>'items');"
            "CALL ducklake_set_option('lake','data_inlining_row_limit',1000,table_name=>'items');"
        )
        initialize(native)
        native.call("optimizer-enable", artifacts, 128, 4194304, 512, 67108864)
        insert(native, 0, 128)
        flushed = wait(
            native, lambda s: s["state"] == "flushed" and s["inline_rows"] == 0, "flush without an index build"
        )
        require(flushed["run_count"] == 1 and flushed["unindexed_rows"] == 128, "Small F triggered a premature index")
        native.call("head", "knn", expected="No published Vortex index")
        native.call("optimizer-resume", "optimizer_flushed", "pause")
        insert(native, 1000, 128)
        durable = crash(native, "optimizer_flushed", "flushing")
        native.call("optimizer-resume")
        replanned = wait(
            native, lambda s: s["inline_rows"] == 0 and s["unindexed_rows"] == 256, "committed flush reconciliation"
        )
        require(replanned["run_count"] == durable["run_count"], "Recovery repeated a committed flush")
        steady(native, replanned)
        native.sql("CALL ducklake_set_option('lake','data_inlining_row_limit',0,table_name=>'items');")
        insert(native, 2000, 256)
        indexed = wait(native, lambda s: s["published_revision"] == 1, "accumulated F threshold")
        require(indexed["run_count"] == durable["run_count"] + 1, "F accumulation scheduled duplicate tasks")
        native.finish()
        checks.append(
            "flush can complete below the ANN threshold; SIGKILL after T-to-F commit resumes from F without repeating the flush"
        )
        return {"flush_recovery": "passed"}
    finally:
        if native.process.poll() is None:
            native.process.kill()
            native.process.wait(timeout=5)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--driver", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()
    root = args.output_dir.resolve()
    root.mkdir(parents=True, exist_ok=False)
    scratch = root / "private"
    scratch.mkdir()
    os.environ["TMPDIR"] = str(scratch)
    driver = args.driver.resolve(strict=True)
    native = None
    checks = []
    report = {"status": "running", "driver_sha256": BASE.digest(driver)}
    started = time.monotonic()
    try:
        native = CAT.Native(driver, root / "native")
        report.update(exercise(native, root, checks))
        native.finish()
        require(native.process.returncode == 0, "Optimizer database close failed")
        report.update(bytes_and_close(driver, root, checks))
        report.update(table_identity(driver, root, checks))
        report.update(flush_recovery(driver, root, checks))
        require(list(scratch.iterdir()) == [], "Optimizer leaked private staging")
        report["status"] = "passed"
    except Exception as error:
        report.update(status="failed", error=str(error))
        raise
    finally:
        if native is not None and native.process.poll() is None:
            try:
                native.call("optimizer-release")
                native.finish()
            except Exception:
                native.process.kill()
                native.process.wait(timeout=5)
        report.update(
            seconds=time.monotonic() - started,
            checks=checks,
            native_requests=native.requests if native else 0,
            native_rejections=native.errors if native else 0,
            external_build_workers=0,
            manual_scheduler_ticks=0,
            probe_processes=0,
        )
        (root / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
