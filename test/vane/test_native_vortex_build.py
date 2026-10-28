#!/usr/bin/env python3
"""Qualify native construction/publication and F-to-B promotion without a probe."""
from __future__ import annotations

import argparse
import importlib.util
import json
import os
from pathlib import Path
import time

SPEC = importlib.util.spec_from_file_location("native_search", Path(__file__).with_name("test_native_vortex_search.py"))
assert SPEC is not None and SPEC.loader is not None
SEARCH = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SEARCH)
CAT = SEARCH.CAT
BASE = CAT.BASE
require = BASE.require
VECTOR = [133, 4, 2, 0, 1, 8, 7, 16]


def crash_build(native, stage, reference, revision):
    marker = native.root / f"build-{stage}.ready"
    native.send("crash-build", stage, reference, revision)
    deadline = time.monotonic() + 60
    while not marker.exists():
        require(native.process.poll() is None, "builder exited before the crash checkpoint")
        require(time.monotonic() < deadline, "build crash checkpoint timed out")
        time.sleep(0.01)
    native.process.kill()
    native.process.wait(timeout=10)
    native.log.close()
    require(native.process.returncode == -9, "native builder was not killed with SIGKILL")
    native.start("resume")


def exercise(native, root, checks):
    artifacts = root / "artifacts"
    artifacts.mkdir()
    serial = 0

    def reference(label):
        nonlocal serial
        serial += 1
        return artifacts / f"{serial}-{label}.json"

    def oracle(label):
        native.call("raw", label)
        return native.rows(label)

    recalls = []

    def qualify(result, rows):
        recalls.append(SEARCH.qualify(result, rows, VECTOR))
        return result

    def search(rows):
        result = qualify(SEARCH.query(native, SEARCH.search_sql(VECTOR)), rows)
        core = qualify(native.call("core-search"), rows)
        require(result["rows"] == core["rows"], "SQL and core construction/search results diverged")
        return core["diagnostics"]

    native.call("build", "knn", reference("uninitialized"), 0, expected="catalog is not initialized")
    native.call("init")
    first = reference("initial")
    built = native.call("build", "knn", first, 0)
    require(built["revision"] == 1, "initial native build did not publish revision one")
    require(CAT.generation(first) == built["generation"], "native builder changed its sealed generation")
    first.unlink()
    rows = oracle("initial")
    initial = search(rows)
    require(
        len(initial["routes"]["indexed_files"]) == 2 and not initial["routes"]["scan_files"],
        "initial B coverage changed",
    )
    checks.append(
        "native SQL builds, seals, verifies and publishes SPFresh in-process; query discovery survives reference removal"
    )

    native.sql(
        "CALL ducklake_set_option('lake','data_inlining_row_limit',100,table_name=>'items');"
        "INSERT INTO lake.items SELECT 5000+i,CASE WHEN i=0 THEN [133.25,4,2,0,1,8,7,16] "
        "ELSE [7000+i,0,0,0,0,0,0,0] END,'B','F-vector' FROM range(128)t(i);"
        "BEGIN;UPDATE lake.items SET embedding=[700000,0,0,0,0,0,0,0],payload='updated' WHERE id=133;"
        "DELETE FROM lake.items WHERE id=134;"
        "INSERT INTO lake.items VALUES(200000,[133.125,4,2,0,1,8,7,16],'A','T-live');COMMIT;"
    )
    before_rows = oracle("before-build")
    mutation = root / "during-build.sql"
    mutation.write_text(
        "BEGIN;UPDATE lake.items SET payload='已提交的新版' WHERE id=200000;DELETE FROM lake.items WHERE id=5001;COMMIT;"
        "CALL ducklake_flush_inlined_data('lake');"
        "INSERT INTO lake.items VALUES(300000,[133.0625,4,2,0,1,8,7,16],'A','T-after-capture');"
    )
    promoted = reference("promotion")
    overlap = native.call("split-build", promoted, 1, "reference_ready", mutation)
    qualify(overlap["before"], before_rows)
    rows = oracle("after-build")
    qualify(overlap["during"], rows)
    require(overlap["before"]["diagnostics"]["index_revision"] == 1, "build changed the ready head before publication")
    require(overlap["during"]["diagnostics"]["index_revision"] == 1, "concurrent query saw an unpublished generation")
    latest = search(rows)
    require(
        latest["index_revision"] == 2 and len(latest["routes"]["indexed_files"]) == 3, "captured F did not become B"
    )
    require(
        len(latest["routes"]["scan_files"]) == 1 and latest["routes"]["inline_rows"] == 1,
        "post-capture F/T were omitted",
    )
    checks.append(
        "queries and update/delete/flush commit during construction; captured F becomes B while later F/T remain queryable"
    )

    raced = native.call("race-build", "knn", reference("race-a"), reference("race-b"), 2)
    revision = raced["revision"]
    require(revision == 3, "concurrent native builders did not produce exactly one successor")
    search(rows)
    checks.append(
        "concurrent native builders retain separate sealed candidates and exactly one revision-CAS publication wins"
    )

    checkpoints = [
        "after_pin",
        "source_pinned",
        "build_started",
        "build_complete",
        "generation_sealed",
        "before_reference",
        "reference_ready",
        "before_build_publish",
        "before_commit",
    ]
    for stage in checkpoints:
        native.call("cancel-build", stage, reference("cancel-" + stage), revision, expected="Interrupted")
        require(native.call("head", "knn")["revision"] == revision, "cancelled build changed the ready head")
    for stage in ["source_pinned", "build_complete", "reference_ready", "before_commit"]:
        native.call(
            "fail-build", stage, reference("failure-" + stage), revision, expected="Injected native build failure"
        )
        require(native.call("head", "knn")["revision"] == revision, "failed build changed the ready head")
    native.call(
        "build",
        "knn",
        native.root / "data" / "managed-reference.json",
        revision,
        expected="outside the managed data path",
    )
    native.call("build", "knn", reference("stale-revision"), 0, expected="revision conflict")
    search(rows)
    checks.append(
        "cancellation/failure before publication releases ownership/guards; managed roots and stale revisions reject without fallback"
    )

    crashes = ["build_complete", "before_commit", "after_commit"]
    for stage in crashes:
        crash_build(native, stage, reference("crash-" + stage), revision)
        if stage == "after_commit":
            revision += 1
        require(native.call("head", "knn")["revision"] == revision, "restart exposed an incorrect ready generation")
        search(rows)
    native.call(
        "fail-build", "after_commit", reference("lost-ack"), revision, expected="committed but acknowledgement failed"
    )
    revision += 1
    require(native.call("head", "knn")["revision"] == revision, "lost ACK undid a committed publication")
    search(rows)
    checks.append(
        "SIGKILL after native build and around catalog commit recovers the old/new head; lost acknowledgement retains the committed generation"
    )

    native.call("raw", "before-compaction")
    old = [Path(f["uri"]) for f in json.loads(native.mixed("before-compaction").read_text())["file_capture"]["files"]]
    maintenance = root / "compact.sql"
    maintenance.write_text(
        "CALL ducklake_flush_inlined_data('lake');CALL ducklake_rewrite_data_files('lake',delete_threshold=>0);"
        "CALL ducklake_merge_adjacent_files('lake');"
        "SELECT count(*) FROM ducklake_expire_snapshots('lake',older_than=>now());"
        "SELECT count(*) FROM ducklake_cleanup_old_files('lake',cleanup_all=>true);"
    )
    rewritten = reference("rewritten")
    native.call(
        "mutate-build",
        rewritten,
        revision,
        "build_started",
        maintenance,
        expected="absent or its identity changed",
    )
    require(rewritten.exists(), "owned source bytes could not finish building after canonical source GC")
    require(native.call("head", "knn")["revision"] == revision, "source rewrite published a stale native generation")
    reclaimed = sum(not path.exists() for path in old)
    require(reclaimed == len(old) and reclaimed > 0, "maintenance did not reclaim the captured source files")
    SEARCH.query(native, SEARCH.search_sql(VECTOR), expected="Referenced Vortex index file is absent")
    rebuilt = native.call("build", "knn", reference("rebuild"), revision)
    revision += 1
    require(rebuilt["revision"] == revision, "explicit native rebuild did not repair the physical layout")
    rows = oracle("rewritten")
    search(rows)
    checks.append(
        "canonical source compaction/GC during construction rejects stale publication; an explicit native rebuild restores search"
    )

    native.sql(
        "DROP TABLE lake.items;CREATE TABLE lake.items(id BIGINT,embedding FLOAT[],category VARCHAR,payload VARCHAR);"
        "CALL ducklake_set_option('lake','experimental_data_file_format','vortex',table_name=>'items');"
        "CALL ducklake_set_option('lake','data_inlining_row_limit',100,table_name=>'items');"
        "INSERT INTO lake.items VALUES(1,[133,4,2,0,1,8,7,16],'A','inline-only');"
        "SELECT CASE WHEN (SELECT id FROM lake.items ORDER BY list_distance(embedding,"
        "[133,4,2,0,1,8,7,16]::FLOAT[]) LIMIT 1)=1 THEN true ELSE error('Inline exact query failed') END;"
    )
    native.call("build", "knn", reference("inline-only"), 0, expected="requires materialized Vortex files")
    checks.append("pure inline data does not invent a file index or retry another build mode")
    return {
        "fixture_recalls": recalls,
        "recall_threshold": 0.8,
        "final_revision": revision,
        "crash_boundaries": crashes,
        "cancel_checkpoints": checkpoints,
        "reclaimed_source_files": reclaimed,
        "external_build_workers": 0,
        "probe_processes": 0,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ["driver", "output-dir"]:
        parser.add_argument("--" + name, required=True, type=Path)
    args = parser.parse_args()
    root = args.output_dir.resolve()
    root.mkdir(parents=True, exist_ok=False)
    scratch = root / "private"
    scratch.mkdir()
    os.environ["TMPDIR"] = str(scratch)
    native = None
    checks = []
    report = {
        "status": "running",
        "driver_sha256": BASE.digest(args.driver.resolve(strict=True)),
        "vortex_revision": "531fed6588d1b1b0492f1a50cad42179ce68faa5",
    }
    started = time.monotonic()
    try:
        native = CAT.Native(args.driver.resolve(strict=True), root / "native")
        report.update(exercise(native, root, checks))
        require(list(scratch.iterdir()) == [], "build/search leaked private staging")
        report["status"] = "passed"
    except Exception as error:
        report.update(status="failed", error=str(error))
        raise
    finally:
        if native is not None and native.process.poll() is None:
            try:
                native.finish()
            except Exception:
                native.process.kill()
                native.process.wait(timeout=5)
        report.update(
            seconds=time.monotonic() - started,
            checks=checks,
            native_requests=native.requests if native else 0,
            native_rejections=native.errors if native else 0,
            native_sessions=native.sessions if native else 0,
        )
        (root / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
