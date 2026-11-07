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
    distance = lambda row: CAT.MIXED.float32_squared_l2(vector, row["embedding"])
    address = lambda row: CAT.MIXED.key(CAT.MIXED.ref(row))
    expected = sorted(eligible, key=lambda row: (distance(row), address(row)))[:k]
    require(len(rows) <= k, "SQL search returned excess rows")
    require(len({row["id"] for row in rows}) == len(rows), "SQL search duplicated a business row")
    by_id = {row["id"]: row for row in eligible}
    for row in rows:
        source = by_id.get(row["id"])
        require(source is not None, "SQL search returned invisible/disallowed business data")
        require(
            CAT.MIXED.READ.business(row) == CAT.MIXED.READ.business(source),
            "SQL typed take changed row version, Unicode or nullability",
        )
        require(math.isclose(row["distance"], distance(row), rel_tol=1e-5, abs_tol=1e-4), "SQL distance mismatch")
    ordering = [(row["distance"], address(by_id[row["id"]])) for row in rows]
    require(ordering == sorted(ordering), "SQL result lost distance/physical-address rank order")
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
        for stage in ("after_visibility", "after_open", "category_filter", "after_ann", "scan_batch", "before_fetch")
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
    }


def exercise_float32(proof, driver, scratch, checks):
    root = proof.root / "float32"
    root.mkdir()
    native = CAT.Native(driver, root / "native")
    recalls = []
    try:
        native.call("raw", "bootstrap")
        artifacts = root / "artifacts"
        artifacts.mkdir()
        reference = artifacts / "index.json"
        proof.run([str(proof.probe), "build-admitted-file-index", str(native.capture("bootstrap")), str(reference)])
        native.call("init")
        native.call("publish", "knn", reference, 0)
        native.sql(
            "CALL ducklake_set_option('lake','data_inlining_row_limit',0,table_name=>'items');"
            "INSERT INTO lake.items VALUES"
            "(10000,[10000,1,0,0,0,0,0,0],'F','rounded-tie-first'),"
            "(10001,[10000,0,0,0,0,0,0,0],'F','rounded-tie-second'),"
            "(10002,[100000112,1,0,0,0,0,0,0],'raw-query','rounded-query'),"
            "(10003,[0.1,0,0,0,0,0,0,0],'fraction-F','fractional-file'),"
            "(10004,[10000,1,0,0,0,0,0,0],'tie','file-tie'),"
            "(10005,[133,4,2,0,1,8,7,16],'A','file-exact');"
        )
        native.sql(
            "CALL ducklake_set_option('lake','data_inlining_row_limit',100,table_name=>'items');"
            "INSERT INTO lake.items VALUES"
            "(11000,[10000,1,0,0,0,0,0,0],'T','rounded-tie-first'),"
            "(11001,[10000,0,0,0,0,0,0,0],'T','rounded-tie-second'),"
            "(11002,[0.1,0,0,0,0,0,0,0],'fraction-T','fractional-inline'),"
            "(11003,[8000,0,0,0,0,0,0,0],'decimal','小数-query'),"
            "(11004,[10000,0,0,0,0,0,0,0],'tie','inline-tie'),"
            "(11005,[133,4,2,0,1,8,7,16],'A','inline-exact');"
        )
        native.call("raw", "delta")
        oracle = native.rows("delta")
        cases = [
            ([0] * 8, "F", 1, [10000], 100000000.0),
            ([0] * 8, "T", 1, [11000], 100000000.0),
            ([0] * 8, "tie", 1, [10004], 100000000.0),
            ([100000110.0, 1, 0, 0, 0, 0, 0, 0], "raw-query", 1, [10002], 0.0),
            ([0.1, 0, 0, 0, 0, 0, 0, 0], "fraction-F", 1, [10003], 0.0),
            ([0.1, 0, 0, 0, 0, 0, 0, 0], "fraction-T", 1, [11002], 0.0),
            ([8001.1, 0, 0, 0, 0, 0, 0, 0], "decimal", 1, [11003], 1.2102148532867432),
            ([133, 4, 2, 0, 1, 8, 7, 16], "A", 3, [133, 10005, 11005], 0.0),
        ]
        for vector, category, k, identifiers, score in cases:
            result = query(native, search_sql(vector, k=k, category=category))
            recalls.append(qualify(result, oracle, vector, category=category, k=k))
            require(
                [row["id"] for row in result["rows"]] == identifiers
                and all(row["distance"] == score for row in result["rows"])
                and recalls[-1] == 1.0,
                "SQL Float32 rounding, physical tie order or typed business values changed",
            )
            no_leases(scratch)
        native.sql(
            "PREPARE fractional_search AS SELECT * FROM ducklake_vortex_search("
            "'lake','main','items','knn',$1,1,category=>'decimal');"
        )
        for value in (8001.1, 8000.0):
            vector = [value, 0, 0, 0, 0, 0, 0, 0]
            result = query(native, f"EXECUTE fractional_search([{','.join(map(str, vector))}]::FLOAT[])")
            recalls.append(qualify(result, oracle, vector, category="decimal", k=1))
            require(result["rows"][0]["id"] == 11003 and recalls[-1] == 1.0, "prepared FLOAT query changed")
            no_leases(scratch)
    finally:
        if native.process.poll() is None:
            native.finish()
    checks.append(
        "SQL Float32 input/score normalization, fractional F/T fetch, parameterized queries and physical F/T/B ties preserve the unchanged tolerance and recall gates"
    )
    return {"recalls": recalls, "native_requests": native.requests, "index_builds": 1}


def exercise_column_renames(proof, driver, scratch, checks):
    root = proof.root / "column-renames"
    root.mkdir()
    native = CAT.Native(driver, root / "native")
    vector = [133, 4, 2, 0, 1, 8, 7, 16]
    recalls = []
    try:
        native.call("raw", "bootstrap")
        reference = root / "index.json"
        proof.run([str(proof.probe), "build-admitted-file-index", str(native.capture("bootstrap")), str(reference)])
        native.call("init")
        native.call("publish", "knn", reference, 0)
        native.sql(
            "PREPARE renamed_search AS " + search_sql(vector) + ";"
            "INSERT INTO lake.items VALUES(9000,[133.5,4,2,0,1,8,7,16],'old-file-category','new-file-category');"
            "CALL ducklake_set_option('lake','data_inlining_row_limit',100,table_name=>'items');"
            "INSERT INTO lake.items VALUES(9001,[133,4,2,0,1,8,7,16],'old-category','new-category');"
        )
        swap = (
            "ALTER TABLE lake.items RENAME category TO temporary_name;"
            "ALTER TABLE lake.items RENAME payload TO category;"
            "ALTER TABLE lake.items RENAME temporary_name TO payload;"
        )
        native.sql(swap)
        native.call("raw", "swapped")
        oracle = native.rows("swapped")
        selected = {row["id"]: row for row in oracle if row["id"] in (9000, 9001)}
        require(
            selected[9001]["filename"].startswith("ducklake_inlined_data_")
            and selected[9001]["category"] == "new-category"
            and selected[9001]["payload"] == "old-category",
            "rename fixture did not retain the old inline source and swap logical field meanings",
        )
        for category, identifier in (("new-category", 9001), ("new-file-category", 9000)):
            result = query(native, search_sql(vector, k=1, category=category))
            recalls.append(qualify(result, oracle, vector, category=category, k=1))
            require([row["id"] for row in result["rows"]] == [identifier], "renamed category lost its row")
            no_leases(scratch)
        require(query(native, search_sql(vector, category="old-category"))["rows"] == [], "old category admitted T")
        prepared = query(native, "EXECUTE renamed_search")
        recalls.append(qualify(prepared, oracle, vector))
        require({9000, 9001} <= {row["id"] for row in prepared["rows"]}, "prepared search lost renamed F/T")
        no_leases(scratch)

        native.sql(
            "INSERT INTO lake.items(id,embedding,category,payload) "
            "VALUES(9002,[133.0625,4,2,0,1,8,7,16],'second-category','second-payload');"
            'ALTER TABLE lake.items RENAME payload TO "body ""value""";'
            'INSERT INTO lake.items(id,embedding,category,"body ""value""") '
            "VALUES(9003,[133.125,4,2,0,1,8,7,16],'quoted-category','renamed-value');"
            'ALTER TABLE lake.items RENAME "body ""value""" TO payload;'
        )
        native.call("raw", "multiple-schemas")
        oracle = native.rows("multiple-schemas")
        selected = {row["id"]: row for row in oracle if row["id"] in (9001, 9002, 9003)}
        require(
            len({row["filename"] for row in selected.values()}) == 3
            and all(row["filename"].startswith("ducklake_inlined_data_") for row in selected.values())
            and selected[9003]["payload"] == "renamed-value",
            "rename fixture did not create three inline sources with distinct historical schemas",
        )
        for category, identifier in (("new-category", 9001), ("second-category", 9002), ("quoted-category", 9003)):
            result = query(native, search_sql(vector, k=1, category=category))
            recalls.append(qualify(result, oracle, vector, category=category, k=1))
            require([row["id"] for row in result["rows"]] == [identifier], "historical inline schema lost its row")
            no_leases(scratch)
        prepared = query(native, "EXECUTE renamed_search")
        recalls.append(qualify(prepared, oracle, vector))
        require({9000, 9001, 9002, 9003} <= {row["id"] for row in prepared["rows"]}, "mixed schema search lost F/T")
        no_leases(scratch)

        # Resolve historical columns after a concurrent rename, using the held query schema.
        rename = root / "rename-after-visibility.sql"
        rename.write_text(swap)
        held = native.call("split-search", rename)
        recalls.append(qualify(held, oracle, vector))
        require(held["rows"] == prepared["rows"], "concurrent rename changed held inline field mappings")
        native.call("raw", "restored")
        oracle = native.rows("restored")
        fresh = query(native, "EXECUTE renamed_search")
        recalls.append(qualify(fresh, oracle, vector))
        require(fresh["rows"] != held["rows"], "fresh execution did not observe renamed field meanings")
        result = query(native, search_sql(vector, k=1, category="old-category"))
        recalls.append(qualify(result, oracle, vector, category="old-category", k=1))
        require([row["id"] for row in result["rows"]] == [9001], "fresh renamed category lost T")
        core = native.call("core-search")
        require(
            core["diagnostics"]["index_revision"] == 1 and core["diagnostics"]["routes"]["inline_rows"] == 3,
            "column renames changed the published B generation or lost inline sources",
        )
        no_leases(scratch)
    finally:
        if native.process.poll() is None:
            native.finish()
    checks.append(
        "field IDs preserve T/F business values and category predicates across swapped names, quoted historical columns, three inline schemas, prepared execution and concurrent rename after visibility"
    )
    return {"recalls": recalls, "native_requests": native.requests, "inline_sources": 3, "index_builds": 1}


def exercise_data_paths(proof, scratch, checks):
    root = proof.root / "data-paths"
    root.mkdir()
    cases = []
    for mode in ("canonical", "dot", "relative", "symlink", "file-colon"):
        case = root / mode
        case.mkdir()
        data = case / "data"
        data.mkdir()
        represented = str(data)
        if mode == "dot":
            represented = str(case) + "/./data"
        elif mode == "relative":
            represented = os.path.relpath(data, Path.cwd())
        elif mode == "symlink":
            (case / "data-link").symlink_to(data, target_is_directory=True)
            represented = str(case / "data-link")
        elif mode == "file-colon":
            represented = "file:" + str(data)
        prefix = (
            "SET autoinstall_known_extensions=false;SET autoload_known_extensions=false;"
            "LOAD parquet;LOAD ducklake;LOAD vortex;SET threads=2;ATTACH "
            + BASE.quote("ducklake:" + str(case / "metadata.ducklake"))
            + " AS lake(DATA_PATH "
            + BASE.quote(represented)
            + ",DATA_INLINING_ROW_LIMIT 0);"
        )

        def sql(statement, expected=None):
            return proof.run([str(proof.duckdb), "-batch", "-bail", "-json", "-c", prefix + statement], expected)

        sql(
            "CREATE TABLE lake.items(id BIGINT,embedding FLOAT[],category VARCHAR,payload VARCHAR);"
            "CALL ducklake_set_option('lake','experimental_data_file_format','vortex',table_name=>'items');"
            "INSERT INTO lake.items SELECT i,[i::FLOAT,4,2,0,1,8,7,16],'A','row-'||i FROM range(4096)t(i);"
            "SELECT count(*) FROM lake.items;"
        )
        capture = case / "admitted.json"
        capture.write_text(
            json.loads(sql("SELECT admission FROM ducklake_vortex_read_view('lake','main','items',8,67108864,100000)"))[
                0
            ]["admission"]
        )
        original_files = json.loads(capture.read_text())["capture"]["file_capture"]["files"]
        artifacts = case / "artifacts"
        artifacts.mkdir()
        reference = artifacts / "index.json"
        proof.run([str(proof.probe), "build-admitted-file-index", str(capture), str(reference)])
        sql(
            "SELECT * FROM ducklake_initialize_vortex_indexes('lake');"
            "SELECT * FROM ducklake_publish_vortex_index('lake','main','items','knn'," + BASE.quote(reference) + ",0);"
        )
        statement = search_sql([133, 4, 2, 0, 1, 8, 7, 16], k=1, category="A")
        result = json.loads(sql(statement))
        require(
            len(result) == 1 and int(result[0]["id"]) == 133 and float(result[0]["distance"]) == 0.0,
            f"valid {mode} managed path could not execute SQL search",
        )
        named = json.loads(
            json.loads(
                sql("SELECT admission FROM ducklake_vortex_index_view('lake','main','items','knn',67108864,100000)")
            )[0]["admission"]
        )
        require(
            named["capture"]["file_capture"]["files"] == original_files
            and named["index"]["revision"] == 1
            and named["index"]["reference"] == json.loads(reference.read_text()),
            f"managed path expansion changed the sealed source identity for {mode}",
        )
        no_leases(scratch)
        if mode == "file-colon":
            try:
                os.environ["TMPDIR"] = str(data)
                sql(statement, "scratch directory must be outside the managed data path")
                require(not list(data.glob(".ducklake-vortex-query-*")), "rejected scratch created managed staging")
            finally:
                os.environ["TMPDIR"] = str(scratch)
            require(json.loads(sql(statement)) == result, "managed scratch rejection changed subsequent SQL results")
            no_leases(scratch)
        cases.append({"data_path": mode, "visible_rows": 4096, "winner": 133, "source_identity_unchanged": True})
    checks.append(
        "canonical/dot/relative/symlink/file: managed paths execute SQL search with unchanged source URI binding; physical managed scratch containment remains enforced"
    )
    return cases


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
        "vortex_revision": "145f816102d6024a12164e37737ff8864faf5033",
    }
    try:
        native = CAT.Native(args.driver.resolve(strict=True), root / "native")
        report.update(exercise(proof, native, scratch, checks))
        float32 = exercise_float32(proof, args.driver.resolve(strict=True), scratch, checks)
        report["float32_fixture_recalls"] = float32["recalls"]
        report["float32_native_requests"] = float32["native_requests"]
        renames = exercise_column_renames(proof, args.driver.resolve(strict=True), scratch, checks)
        report["column_rename_regressions"] = renames
        report["data_path_regressions"] = exercise_data_paths(proof, scratch, checks)
        report["lifecycle_index_builds"] = report["index_builds"]
        report["index_builds"] += (
            float32["index_builds"] + renames["index_builds"] + len(report["data_path_regressions"])
        )
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
