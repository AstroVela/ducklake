#!/usr/bin/env python3
"""Qualify in-process native SQL SPFresh search against captured DuckLake rows."""
from __future__ import annotations

import argparse
import importlib.util
import json
import math
import os
from pathlib import Path
import time

SPEC = importlib.util.spec_from_file_location(
    "native_catalog_proof", Path(__file__).with_name("test_native_vortex_catalog.py")
)
assert SPEC is not None and SPEC.loader is not None
CAT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CAT)
BASE = CAT.BASE
require = BASE.require


def search_sql(query, k=10, category=None):
    vector = "[" + ",".join(map(str, query)) + "]::FLOAT[]"
    predicate = "" if category is None else ",category=>'" + category.replace("'", "''") + "'"
    return f"SELECT * FROM ducklake_vortex_search('lake','main','items','knn',{vector},{k}{predicate})"


def query(native, text, expected=None):
    path = native.root.parent / f"search-{native.requests+1}.sql"
    path.write_text(text)
    return native.call("search", path, expected=expected)


def qualify(result, oracle, vector, category=None, k=10):
    rows = result["rows"]
    eligible = [row for row in oracle if category is None or row["category"] == category]
    distance = lambda row: sum((a - b) ** 2 for a, b in zip(row["embedding"], vector))
    expected = sorted(eligible, key=distance)[:k]
    require(len(rows) <= k, "SQL search returned excess rows")
    require(len({row["id"] for row in rows}) == len(rows), "SQL search duplicated a business row")
    by_id = {row["id"]: row for row in eligible}
    for row in rows:
        source = by_id.get(row["id"])
        require(source is not None, "SQL search returned invisible/disallowed business data")
        require(
            {key: row[key] for key in ("id", "embedding", "category", "payload")}
            == {key: source[key] for key in ("id", "embedding", "category", "payload")},
            "SQL typed take changed row version, Unicode or nullability",
        )
        require(math.isclose(row["distance"], distance(row), rel_tol=1e-5, abs_tol=1e-4), "SQL distance mismatch")
    require([row["distance"] for row in rows] == sorted(row["distance"] for row in rows), "SQL result lost rank order")
    recall = len({row["id"] for row in rows} & {row["id"] for row in expected}) / len(expected) if expected else 1.0
    require(recall >= 0.8, f"SQL fixture recall below threshold: {recall}")
    return recall


def no_leases(scratch):
    require(list(scratch.iterdir()) == [], "native SQL query leaked private source/index staging")


def exercise(proof, native, scratch, checks):
    vector = [133, 4, 2, 0, 1, 8, 7, 16]
    native.call("raw", "bootstrap")
    native.sql("PREPARE reusable_search AS " + search_sql(vector))
    artifacts = proof.root / "artifacts"
    artifacts.mkdir()
    refs = [artifacts / f"{name}.reference.json" for name in ("first", "replacement")]
    for ref in refs:
        proof.run([str(proof.probe), "build-admitted-file-index", str(native.capture("bootstrap")), str(ref)])
    native.call("init")
    native.call("publish", "knn", refs[0], 0)
    first_generation = CAT.generation(refs[0])
    refs[0].unlink()
    binary = native.call("binary-rejections")
    require(
        binary["rejections"] == 7 and binary["reordered_open"], "native binary handoff rejection gate did not complete"
    )
    no_leases(scratch)
    initial = query(native, "EXECUTE reusable_search")
    recalls = [qualify(initial, native.rows("bootstrap"), vector)]
    core = native.call("core-search")
    require(core["rows"] == initial["rows"], "native SQL and shared query core diverged")
    require(core["diagnostics"]["routes"]["ann_calls"] == 1, "native SQL core did not use SPFresh ANN")
    filtered = query(native, search_sql(vector, category="B"))
    recalls.append(qualify(filtered, native.rows("bootstrap"), vector, category="B"))
    require(query(native, search_sql(vector, category="absent"))["rows"] == [], "empty prefilter returned rows")
    no_leases(scratch)
    checks.append(
        "native SQL invokes the shared in-process SPFresh core; typed ranked results and category prefilter match the same-view oracle without a reference file"
    )

    native.sql(
        "CALL ducklake_set_option('lake','data_inlining_row_limit',100,table_name=>'items');"
        "INSERT INTO lake.items SELECT 5000+i,CASE WHEN i=0 THEN [133.25,4,2,0,1,8,7,16] "
        "WHEN i=1 THEN [133,4,2,0,1,8,7,16] ELSE [16000+i,0,0,0,0,0,0,0] END,'B','未索引-'||i FROM range(128)t(i);"
        "BEGIN;UPDATE lake.items SET embedding=[7000,0,0,0,0,0,0,0],category=NULL,payload=NULL WHERE id=133;"
        "DELETE FROM lake.items WHERE id IN(134,5001);"
        "INSERT INTO lake.items VALUES(6000,[133.125,4,2,0,1,8,7,16],'A','内联-live'),"
        "(NULL,[8000,0,0,0,0,0,0,0],NULL,'空-ID-业务');COMMIT;"
    )
    native.call("raw", "delta")
    oracle = native.rows("delta")
    delta = query(native, "EXECUTE reusable_search")
    recalls.append(qualify(delta, oracle, vector))
    require([row["id"] for row in delta["rows"][:2]] == [6000, 5000], "SQL B/F/T search lost current delta rows")
    for category, winner in (("B", 5000), ("A", 6000)):
        result = query(native, search_sql(vector, k=1, category=category))
        recalls.append(qualify(result, oracle, vector, category=category, k=1))
        require(result["rows"][0]["id"] == winner, "SQL category was applied after branch/global top-k")
    nullable_vector = [7000, 0, 0, 0, 0, 0, 0, 0]
    nullable = query(native, search_sql(nullable_vector, k=1))
    recalls.append(qualify(nullable, oracle, nullable_vector, k=1))
    require(
        nullable["rows"][0]["id"] == 133
        and nullable["rows"][0]["category"] is None
        and nullable["rows"][0]["payload"] is None,
        "SQL nullable business fields changed",
    )
    null_id_vector = [8000, 0, 0, 0, 0, 0, 0, 0]
    null_id = query(native, search_sql(null_id_vector, k=1))
    recalls.append(qualify(null_id, oracle, null_id_vector, k=1))
    require(
        null_id["rows"][0]["id"] is None and null_id["rows"][0]["payload"] == "空-ID-业务",
        "SQL NULL business ID or Unicode payload changed",
    )
    core = native.call("core-search")
    routes = core["diagnostics"]["routes"]
    require(
        len(routes["indexed_files"]) == 2 and len(routes["scan_files"]) == 1 and routes["inline_rows"] == 3,
        "SQL shared core failed to compose B/F/T",
    )
    checks.append(
        "SQL visibility excludes old updates/deletes; F and T win nearest rank; category prefilters run before every top-k; nullable and Unicode business fields survive typed take"
    )

    native.sql("BEGIN; INSERT INTO lake.items VALUES(7001,[133.0625,4,2,0,1,8,7,16],'A','uncommitted');")
    require(query(native, search_sql(vector))["rows"] == delta["rows"], "SQL search admitted uncommitted writer data")
    native.sql("ROLLBACK;")
    failures = [
        ("cancel-search", stage, "Interrupted")
        for stage in (
            "after_visibility",
            "source_pinned",
            "after_open",
            "category_filter",
            "after_ann",
            "scan_batch",
            "before_fetch",
        )
    ]
    failures += [("fail-search", stage, "Injected query lease failure") for stage in ("source_pinned", "after_open")]
    for op, stage, error in failures:
        native.call(op, stage, expected=error)
        no_leases(scratch)
        require(query(native, search_sql(vector))["rows"] == delta["rows"], "failed query changed following SQL reads")
    limited = query(native, search_sql(vector) + " LIMIT 1")
    require(limited["rows"] == delta["rows"][:1], "SQL LIMIT changed ranked result")
    no_leases(scratch)
    query(native, search_sql([1, 2, 3]), expected="Query dimension")
    no_leases(scratch)
    checks.append(
        "interruption after index admission, injected failure, dimension rejection and early SQL LIMIT release private staging and leave fresh SQL queries usable"
    )

    split_write = proof.root / "split-write.sql"
    split_write.write_text(
        "UPDATE lake.items SET payload='committed-after-visibility' WHERE id=6000;"
        "CALL ducklake_flush_inlined_data('lake');"
    )
    overlapping = native.call("split-search", split_write)
    require(overlapping["rows"] == delta["rows"], "split visibility/T capture mixed metadata transactions")
    native.call("raw", "delta")
    oracle = native.rows("delta")
    delta = query(native, "EXECUTE reusable_search")
    recalls.append(qualify(delta, oracle, vector))
    require(delta["rows"][0]["payload"] == "committed-after-visibility", "fresh SQL view missed the concurrent flush")
    no_leases(scratch)
    checks.append(
        "a writer commits and flushes after visibility capture; the held query keeps old T business rows and a fresh query sees the new F version"
    )

    maintenance = proof.root / "maintenance.sql"
    maintenance.write_text(
        "UPDATE lake.items SET embedding=[9000,0,0,0,0,0,0,0] WHERE id=6000;"
        "CALL ducklake_flush_inlined_data('lake');CALL ducklake_rewrite_data_files('lake',delete_threshold=>0);"
        "CALL ducklake_merge_adjacent_files('lake');"
        "SELECT count(*) FROM ducklake_expire_snapshots('lake',older_than=>now());"
        "SELECT count(*) FROM ducklake_cleanup_old_files('lake',cleanup_all=>true);"
    )
    held = native.call("held-search", refs[1], 1, maintenance, artifacts / first_generation)
    require(
        held["rows"] == delta["rows"] and held["diagnostics"]["index_revision"] == 1,
        "in-flight native SQL core changed source snapshot or ready generation during maintenance",
    )
    captured = json.loads(native.mixed("delta").read_text())
    old_files = [Path(file["uri"]) for file in captured["file_capture"]["files"]]
    require(all(not path.exists() for path in old_files), "concurrent maintenance did not reclaim B/F source files")
    require(not (artifacts / first_generation).exists(), "old canonical index generation was not removed")
    no_leases(scratch)
    query(native, "EXECUTE reusable_search", expected="Referenced Vortex index file is absent")
    checks.append(
        "a held native query core keeps its admitted B/F/T view and leased SPFresh generation across publication, actual source GC and canonical index removal; fresh stale SQL search fails explicitly"
    )

    native.call("raw", "new-layout")
    fresh = artifacts / "new-layout.reference.json"
    proof.run([str(proof.probe), "build-admitted-file-index", str(native.capture("new-layout")), str(fresh)])
    native.call("publish", "knn", fresh, 2)
    latest = query(native, "EXECUTE reusable_search")
    recalls.append(qualify(latest, native.rows("new-layout"), vector))
    require(latest["rows"][0]["id"] == 5000, "fresh SQL search missed the newly committed version")
    damaged = CAT.artifact(fresh)
    saved = damaged.read_bytes()
    try:
        damaged.write_bytes(saved + b"corrupt")
        query(native, search_sql(vector), expected="mismatch")
        no_leases(scratch)
    finally:
        damaged.write_bytes(saved)
    require(
        query(native, search_sql(vector))["rows"] == latest["rows"], "corrupt query leaked or poisoned a later open"
    )
    native.finish()
    os.environ["TMPDIR"] = str(native.root / "data")
    native.start("resume")
    query(native, search_sql(vector), expected="scratch directory must be outside the managed data path")
    native.finish()
    os.environ["TMPDIR"] = str(scratch)
    native.start("resume")
    require(query(native, search_sql(vector))["rows"] == latest["rows"], "scratch rejection changed the ready index")
    native.sql(
        "DROP TABLE lake.items;CREATE TABLE lake.items(id BIGINT,embedding FLOAT[],category VARCHAR,payload VARCHAR);"
    )
    query(native, search_sql(vector), expected="No published Vortex index")
    no_leases(scratch)
    native.finish()
    checks.append(
        "explicit rebuild/publish restores SQL search after compaction; corrupt artifacts and dropped/recreated table UUIDs reject without fallback and failed opens clean up"
    )
    return {
        "fixture_recalls": recalls,
        "recall_threshold": 0.8,
        "index_builds": 3,
        "reclaimed_source_files": len(old_files),
        "external_query_workers": 0,
        "binary_rejections": 7,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("duckdb", "driver", "probe", "output-dir"):
        parser.add_argument("--" + name, required=True, type=Path)
    args = parser.parse_args()
    root = args.output_dir.resolve()
    root.mkdir(parents=True, exist_ok=False)
    scratch = root / "private"
    scratch.mkdir()
    os.environ["TMPDIR"] = str(scratch)
    proof = BASE.Proof(args.duckdb.resolve(strict=True), args.probe.resolve(strict=True), root)
    native = None
    checks = []
    started = time.monotonic()
    report = {
        "duckdb_sha256": BASE.digest(proof.duckdb),
        "probe_sha256": BASE.digest(proof.probe),
        "driver_sha256": BASE.digest(args.driver.resolve(strict=True)),
        "vortex_revision": BASE.VORTEX_REVISION,
    }
    try:
        native = CAT.Native(args.driver.resolve(strict=True), root / "native")
        report.update(exercise(proof, native, scratch, checks))
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
            probe_processes=proof.processes,
            native_requests=native.requests if native else 0,
            native_rejections=native.errors if native else 0,
            checks=checks,
        )
        (root / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
