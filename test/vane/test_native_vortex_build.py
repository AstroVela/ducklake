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
    overlap = native.call("split-build", promoted, 1, "build_started", mutation)
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
        "before_build",
        "after_pin",
        "build_input",
        "source_pinned",
        "build_started",
        "build_complete",
        "generation_sealed",
        "before_reference",
        "reference_ready",
        "before_build_publish",
        "after_validation",
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


def exercise_data_paths(driver, root, scratch):
    cases = []
    for mode in ("canonical", "dot", "relative", "symlink", "file-colon"):
        case = root / ("data-path-" + mode)
        case.mkdir()
        data = case / "managed"
        data.mkdir()
        represented = str(data)
        if mode == "dot":
            represented = str(case) + "/./managed"
        elif mode == "relative":
            represented = os.path.relpath(data, Path.cwd())
        elif mode == "symlink":
            (case / "data-link").symlink_to(data, target_is_directory=True)
            represented = str(case / "data-link")
        elif mode == "file-colon":
            represented = "file:" + str(data)
        artifacts = case / "artifacts"
        artifacts.mkdir()
        native = CAT.Native(driver, case / "native")
        try:
            native.sql(
                "DETACH lake;ATTACH "
                + BASE.quote("ducklake:" + str(case / "metadata.ducklake"))
                + " AS lake(DATA_PATH "
                + BASE.quote(represented)
                + ",DATA_INLINING_ROW_LIMIT 0);"
                "CREATE TABLE lake.items(id BIGINT,embedding FLOAT[],category VARCHAR,payload VARCHAR);"
                "CALL ducklake_set_option('lake','experimental_data_file_format','vortex',table_name=>'items');"
                "INSERT INTO lake.items SELECT i,[i::FLOAT,4,2,0,1,8,7,16],'A','row-'||i FROM range(4096)t(i);"
            )
            native.call("raw", "before-build")
            original = json.loads(native.mixed("before-build").read_text())["file_capture"]["files"]
            require(original, "native URI fixture captured no files")
            if mode == "relative":
                require(all(not Path(file["uri"]).is_absolute() for file in original), "relative URI was rewritten")
            elif mode == "file-colon":
                require(all(file["uri"].startswith(represented + "/") for file in original), "file: URI was rewritten")
            native.call("init")
            reference = artifacts / "native.json"
            built = native.call("build", "knn", reference, 0)
            descriptor = CAT.descriptor(reference)
            require(
                descriptor["snapshot"]["files"] == original and built["revision"] == 1,
                "native construction changed its captured source identities",
            )
            result = SEARCH.query(native, SEARCH.search_sql(VECTOR, k=1, category="A"))
            require(
                len(result["rows"]) == 1 and result["rows"][0]["id"] == 133 and result["rows"][0]["distance"] == 0,
                "native URI construction/search returned the wrong winner",
            )
            native.call("build", "knn", data / "forbidden.json", 1, expected="outside the managed data path")
            require(not (data / "forbidden.json").exists(), "managed reference rejection wrote an output")
            require(
                CAT.single_reference(native.call("head", "knn")) == descriptor,
                "rejected reference changed the ready head",
            )
            require(
                SEARCH.query(native, SEARCH.search_sql(VECTOR, k=1, category="A")) == result,
                "managed reference rejection changed subsequent search",
            )
            if mode == "canonical":
                before = {path.relative_to(artifacts) for path in artifacts.rglob("*")}
                native.call(
                    "build",
                    "knn",
                    artifacts / "reserved.duckdb_extension",
                    1,
                    expected="reserved for DuckDB extensions",
                )
                require(
                    {path.relative_to(artifacts) for path in artifacts.rglob("*")} == before,
                    "reserved reference rejection wrote construction artifacts",
                )
                require(
                    CAT.single_reference(native.call("head", "knn")) == descriptor,
                    "reserved output changed the ready head",
                )
            SEARCH.no_leases(scratch)
            cases.append(
                {
                    "data_path": mode,
                    "source_uris": [file["uri"] for file in original],
                    "winner": 133,
                    "native_requests": native.requests,
                    "native_rejections": native.errors,
                }
            )
        finally:
            if native.process.poll() is None:
                native.finish()
    return cases


def exercise_historical_fields(driver, root, scratch):
    case = root / "historical-fields"
    case.mkdir()
    native = CAT.Native(driver, case / "native")
    recalls = []
    swap = (
        "ALTER TABLE lake.items RENAME category TO temporary_name;"
        "ALTER TABLE lake.items RENAME payload TO category;"
        "ALTER TABLE lake.items RENAME temporary_name TO payload;"
    )
    try:
        native.sql(
            "INSERT INTO lake.items VALUES(9000,[133.5,4,2,0,1,8,7,16],'old-file-category','new-file-category');"
            "CALL ducklake_set_option('lake','data_inlining_row_limit',100,table_name=>'items');"
            "INSERT INTO lake.items VALUES(9001,[133,4,2,0,1,8,7,16],'old-category','new-category');"
            + swap
            + "INSERT INTO lake.items(id,embedding,category,payload) "
            "VALUES(9002,[133.0625,4,2,0,1,8,7,16],'second-category','second-payload');"
            'ALTER TABLE lake.items RENAME payload TO "body ""value""";'
            'INSERT INTO lake.items(id,embedding,category,"body ""value""") '
            "VALUES(9003,[133.125,4,2,0,1,8,7,16],'quoted-category','quoted-value');"
            'ALTER TABLE lake.items RENAME "body ""value""" TO payload;'
        )
        native.call("raw", "historical")
        rows = native.rows("historical")
        inline = [row for row in rows if row["id"] in (9001, 9002, 9003)]
        require(
            len({row["filename"] for row in inline}) == 3
            and all(row["filename"].startswith("ducklake_inlined_data_") for row in inline),
            "native build fixture did not retain three historical inline schemas",
        )
        selected = {row["id"]: row for row in inline}
        require(
            selected[9001]["category"] == "new-category"
            and selected[9002]["category"] == "second-category"
            and selected[9002]["payload"] == "second-payload"
            and selected[9003]["category"] == "quoted-category"
            and selected[9003]["payload"] == "quoted-value",
            "historical field fixture did not preserve the intended named-column values",
        )
        native.call("init")
        built = native.call("build", "knn", case / "first.json", 0)
        for category, identifier in (
            ("new-file-category", 9000),
            ("new-category", 9001),
            ("second-category", 9002),
            ("quoted-category", 9003),
        ):
            result = SEARCH.query(native, SEARCH.search_sql(VECTOR, k=1, category=category))
            recalls.append(SEARCH.qualify(result, rows, VECTOR, category=category, k=1))
            require(
                [row["id"] for row in result["rows"]] == [identifier], "native build lost a historical field mapping"
            )
        baseline = SEARCH.query(native, SEARCH.search_sql(VECTOR))
        require(native.call("cache-search")["rows"] == baseline["rows"], "native build cache eviction changed T values")
        rename = case / "during-build.sql"
        rename.write_text(swap)
        rebuilt = native.call("mutate-build", case / "second.json", 1, "build_complete", rename)
        require(
            rebuilt["revision"] == 2
            and CAT.single_reference(rebuilt)["generation"]["generation"] != built["generation"],
            "native rebuild did not publish",
        )
        native.call("raw", "renamed")
        rows = native.rows("renamed")
        result = SEARCH.query(native, SEARCH.search_sql(VECTOR, k=1, category="old-category"))
        recalls.append(SEARCH.qualify(result, rows, VECTOR, category="old-category", k=1))
        require([row["id"] for row in result["rows"]] == [9001], "concurrent build rename lost historical T")
        diagnostics = native.call("core-search")["diagnostics"]
        require(
            diagnostics["routes"]["inline_rows"] == 3 and not diagnostics["routes"]["scan_files"],
            "native construction copied T or omitted a materialized source",
        )
        SEARCH.no_leases(scratch)
        return {
            "fixture_recalls": recalls,
            "inline_sources": 3,
            "native_builds": 2,
            "native_requests": native.requests,
            "native_rejections": native.errors,
        }
    finally:
        if native.process.poll() is None:
            native.finish()


def check_denied_output(native, output, stage=None):
    claims = [row for row in native.call("build-status") if Path(row["index_root"]) == output]
    if stage is None:
        require(not claims and not list(output.iterdir()), "Denied admission created build ownership or files")
        return
    require(len(claims) == 1 and claims[0]["state"] == "abandoned", "Revoked access lost the registered task")
    workspace = Path(claims[0]["workspace"])
    expected = {workspace, workspace / "scratch"} if stage == "build_started" else {workspace}
    require(
        set(output.rglob("*")) == expected and all(path.is_dir() for path in expected),
        "Construction wrote additional paths after access was revoked",
    )
    native.call("reopen")
    native.call("build-gc", 16, 67108864)
    require(not list(output.iterdir()), "Authorized recovery left the abandoned workspace")


def exercise_output_permissions(driver, root, scratch):
    cases = []
    for mode in ("denied", "exact-paths", "allowed-directory", "allowed-source-files", "revoke-during-build"):
        case = root / ("permissions-" + mode)
        case.mkdir()
        output = case / "output"
        output.mkdir()
        initial = case / "initial"
        initial.mkdir()
        reference = output / "build.json"
        native = CAT.Native(driver, case / "native")
        try:
            native.call("init")
            native.call("build", "knn", initial / "ready.json", 0)
            previous = native.call("head", "knn")
            directories = [] if mode == "allowed-source-files" else [native.root / "data"]
            allowed = mode in ("allowed-directory", "allowed-source-files")
            if allowed:
                directories.extend([output, initial, scratch])
            policy = "SET allowed_directories=[" + ",".join(BASE.quote(path) for path in directories) + "];"
            if mode == "exact-paths":
                policy += "SET allowed_paths=[" + BASE.quote(output) + "," + BASE.quote(reference) + "];"
            if mode == "allowed-source-files":
                native.call("raw", "source-files")
                files = CAT.descriptor(native.capture("source-files"))["capture"]["file_capture"]["files"]
                require(files, "File-scoped authorization needs materialized sources")
                policy += "SET allowed_paths=[" + ",".join(BASE.quote(file["uri"]) for file in files) + "];"
            policy += "SET enable_external_access=false;"
            if mode == "revoke-during-build":
                mutation = case / "restrict.sql"
                mutation.write_text(policy)
                native.call("mutate-build", reference, 1, "build_started", mutation, expected="directory")
            else:
                native.sql(policy)
            if allowed:
                built = native.call("build", "knn", reference, 1)
                require(built["revision"] == 2 and reference.exists(), "allowed output directory could not publish")
                result = SEARCH.query(native, SEARCH.search_sql(VECTOR, k=1))
                require(
                    len(result["rows"]) == 1 and result["rows"][0]["id"] == 133,
                    "allowlisted native construction changed subsequent search",
                )
            else:
                control = case / "copy.sql"
                control.write_text("COPY (SELECT 1) TO " + BASE.quote(output / "ordinary-copy.csv") + ";")
                native.call("sql", control, expected="file system operations are disabled")
                if mode != "revoke-during-build":
                    native.call("build", "knn", reference, 1, expected="directory")
                check_denied_output(native, output, "build_started" if mode == "revoke-during-build" else None)
                require(native.call("head", "knn") == previous, "denied output authorization changed the ready head")
            SEARCH.no_leases(scratch)
            cases.append(
                {
                    "policy": mode,
                    "output_files": sum(path.is_file() for path in output.rglob("*")),
                    "native_requests": native.requests,
                    "native_rejections": native.errors,
                }
            )
        finally:
            if native.process.poll() is None:
                native.finish()
    return cases


def exercise_disabled_filesystems(driver, root, scratch):
    cases = []
    for stage in ("before-call", "source_pinned", "build_started"):
        case = root / ("disabled-filesystem-" + stage)
        case.mkdir()
        output = case / "output"
        output.mkdir()
        initial = case / "initial"
        initial.mkdir()
        reference = output / "build.json"
        native = CAT.Native(driver, case / "native")
        try:
            native.call("init")
            native.call("build", "knn", initial / "ready.json", 0)
            previous = native.call("head", "knn")
            policy = "SET disabled_filesystems='LocalFileSystem';"
            if stage == "before-call":
                native.sql(policy)
                native.call("build", "knn", reference, 1, expected="disabled by configuration")
            else:
                mutation = case / "disable.sql"
                mutation.write_text(policy)
                native.call("mutate-build", reference, 1, stage, mutation, expected="disabled by configuration")
            native.sql(
                "SELECT CASE WHEN current_setting('enable_external_access') THEN true "
                "ELSE error('Disabled filesystem fixture changed external access') END;"
            )
            control = case / "copy.sql"
            control.write_text("COPY (SELECT 1) TO " + BASE.quote(output / "ordinary-copy.csv") + ";")
            native.call("sql", control, expected="disabled by configuration")
            check_denied_output(native, output, None if stage == "before-call" else stage)
            require(native.call("head", "knn") == previous, "disabled filesystem changed the ready head")
            SEARCH.no_leases(scratch)
            cases.append(
                {
                    "stage": stage,
                    "output_files": 0,
                    "native_requests": native.requests,
                    "native_rejections": native.errors,
                }
            )
        finally:
            if native.process.poll() is None:
                native.finish()
    return cases


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
        "vortex_revision": BASE.VORTEX_REVISION,
    }
    started = time.monotonic()
    try:
        native = CAT.Native(args.driver.resolve(strict=True), root / "native")
        report.update(exercise(native, root, checks))
        report["data_path_regressions"] = exercise_data_paths(args.driver.resolve(strict=True), root, scratch)
        report["historical_field_regressions"] = exercise_historical_fields(
            args.driver.resolve(strict=True), root, scratch
        )
        report["permission_regressions"] = exercise_output_permissions(args.driver.resolve(strict=True), root, scratch)
        report["disabled_filesystem_regressions"] = exercise_disabled_filesystems(
            args.driver.resolve(strict=True), root, scratch
        )
        additional = [
            *report["data_path_regressions"],
            report["historical_field_regressions"],
            *report["permission_regressions"],
            *report["disabled_filesystem_regressions"],
        ]
        report["total_native_requests"] = native.requests + sum(case["native_requests"] for case in additional)
        report["total_native_rejections"] = native.errors + sum(case["native_rejections"] for case in additional)
        checks.append(
            "native binary construction preserves opaque source URIs and rejects references inside managed data"
        )
        checks.append("native construction and concurrent column renames retain three historical inline field mappings")
        checks.append(
            "directory authorization precedes native writes; exact path grants and revoked access retain empty output trees and the ready head"
        )
        checks.append(
            "context filesystem dispatch rejects disabled LocalFileSystem before native writes, including source_pinned and build_started revocation"
        )
        BASE.assert_query_scratch_released(scratch)
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
