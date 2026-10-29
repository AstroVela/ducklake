#!/usr/bin/env python3
"""Qualify immutable B1/B2 publication, mixed search, crashes and retained GC views."""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import time

SPEC = importlib.util.spec_from_file_location("native_build", Path(__file__).with_name("test_native_vortex_build.py"))
assert SPEC is not None and SPEC.loader is not None
BUILD = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BUILD)
CAT, BASE, SEARCH = BUILD.CAT, BUILD.BASE, BUILD.SEARCH
MIXED = CAT.MIXED
require = BASE.require
VECTOR = BUILD.VECTOR


def segments(record):
    manifest = record["reference"]
    require(manifest["format_version"] == 2, "Wrong segment manifest version")
    return manifest["segments"]


def generation(segment):
    return segment["reference"]["generation"]["generation"]


def signature(directory):
    return {
        str(p.relative_to(directory)): [BASE.digest(p), p.stat().st_size, p.stat().st_mtime_ns]
        for p in directory.rglob("*")
        if p.is_file()
    }


def sql(native, root, text):
    path = root / f"sql-{native.requests+1}.sql"
    path.write_text(text)
    return path


def add_f(native, start, near=133.25):
    native.sql(
        "CALL ducklake_set_option('lake','data_inlining_row_limit',100,table_name=>'items');"
        f"INSERT INTO lake.items SELECT {start}+i,CASE WHEN i=0 THEN [{near},4,2,0,1,8,7,16] "
        "ELSE [7000+i,0,0,0,0,0,0,0] END,'B','新段-F' FROM range(128)t(i)"
    )


def setup(native, root):
    artifacts = root / "artifacts"
    artifacts.mkdir()
    native.call("init")
    reference = artifacts / "B1.json"
    native.call("build", "knn", reference, 0)
    first = segments(native.call("head", "knn"))[0]
    frozen = signature(artifacts / generation(first))
    require(bool(frozen), "B1 has no sealed artifacts")
    return artifacts, reference, first, frozen


def unchanged(native, first, frozen):
    current = segments(native.call("head", "knn"))
    require(current[0] == first, "Append replaced B1's descriptor or generation")
    require(signature(Path(first["root"]) / generation(first)) == frozen, "Append changed B1's sealed artifacts")


def qualify(native, label, recalls, two=False):
    native.call("raw", label)
    rows = native.rows(label)
    core = native.call("core-search")
    result = SEARCH.query(native, SEARCH.search_sql(VECTOR))
    recalls.extend([SEARCH.qualify(core, rows, VECTOR), SEARCH.qualify(result, rows, VECTOR)])
    require(core["rows"] == result["rows"], "SQL and multi-segment core disagree")
    if two:
        require(core["diagnostics"]["routes"]["ann_calls"] == 2, "Query did not search both B generations")
    for category in ["A", "B"]:
        filtered = SEARCH.query(native, SEARCH.search_sql(VECTOR, category=category))
        recalls.append(SEARCH.qualify(filtered, rows, VECTOR, category))
    return core, rows


def exercise(proof, native, root, checks, workers):
    artifacts, first_ref, first, frozen = setup(native, root)
    recalls = []
    add_f(native, 5000)
    native.sql(
        "BEGIN;UPDATE lake.items SET embedding=[700000,0,0,0,0,0,0,0],payload=NULL WHERE id=133;"
        "DELETE FROM lake.items WHERE id=134;"
        "INSERT INTO lake.items VALUES(200000,[133.0625,4,2,0,1,8,7,16],'A','实时-T');COMMIT"
    )
    qualify(native, "before", recalls)
    old_ids = {file["id"] for file in first["reference"]["snapshot"]["files"]}
    all_ids = {file["id"] for file in CAT.descriptor(native.capture("before"))["capture"]["file_capture"]["files"]}
    serial = 0

    def reference(label):
        nonlocal serial
        serial += 1
        return artifacts / f"{serial}-{label}.json"

    for stage in ["source_pinned", "build_started", "before_reference", "reference_ready", "before_commit"]:
        native.call("cancel-append", stage, reference(stage), 1, expected="Interrupted")
        require(native.call("head", "knn")["revision"] == 1, "Cancelled append exposed its candidate")
        unchanged(native, first, frozen)
    for stage in ["source_pinned", "reference_ready", "before_commit"]:
        native.call("fail-append", stage, reference(stage), 1, expected="Injected native build failure")
        require(native.call("head", "knn")["revision"] == 1, "Failed append changed ready metadata")
    candidate = reference("B2")
    native.call("fail-append", "before_build_publish", candidate, 1, expected="Injected native build failure")
    second = CAT.descriptor(candidate)
    require(
        {f["id"] for f in second["snapshot"]["files"]} == all_ids - old_ids,
        "B2 builder included old B files or omitted new F",
    )
    require(sum(f["row_count"] for f in second["snapshot"]["files"]) == 128, "B2 rebuilt rows outside the new F batch")
    native.call("publish-segment", "knn", first_ref, 1, expected="Duplicate Vortex segment generation")
    oversized = json.loads(candidate.read_text())
    oversized["snapshot"]["files"][0]["row_count"] = 100001
    inventory = json.dumps(oversized["snapshot"]["files"], separators=(",", ":"))
    oversized["snapshot"]["version"] = "sha256:" + hashlib.sha256(inventory.encode()).hexdigest()
    oversized_path = reference("oversized")
    oversized_path.write_text(json.dumps(oversized))
    native.call("publish-segment", "knn", oversized_path, 1, expected="segment row/vector budget exceeded")
    damaged = CAT.artifact(candidate)
    original = damaged.read_bytes()
    try:
        damaged.write_bytes(original[:-1])
        native.call("publish-segment", "knn", candidate, 1, expected="size/checksum mismatch")
    finally:
        damaged.write_bytes(original)
    add_f(native, 6000, 133.125)
    published = native.call("race-append", "knn", candidate, candidate, 1)
    require(
        published["revision"] == 2 and len(segments(published)) == 2, "CAS append did not publish exactly two segments"
    )
    require(generation(segments(published)[1]) == second["generation"]["generation"], "Publication rebuilt B2")
    unchanged(native, first, frozen)
    core, rows = qualify(native, "two-B", recalls, two=True)
    routes = core["diagnostics"]["routes"]
    require(routes["scan_files"] and routes["inline_rows"], "Two-B query omitted remaining F or T")
    native.call("append", "knn", reference("third"), 2, expected="segment count budget exceeded")
    require(native.call("head", "knn")["revision"] == 2, "Segment budget changed the ready head")
    checks.append(
        "B2 covers only the 128 new F rows; B1 descriptor, generation, artifact bytes and write times remain unchanged"
    )
    checks.append(
        "cancellation, failed sealing/publication, corrupt artifacts, duplicate generations and stale-CAS competition preserve the ready head"
    )
    checks.append(
        "B1/B2 use independent visibility/predicate filters before top-k and merge with post-capture F and committed T"
    )

    for op, stage, expected in [
        ("cancel-search", "after_open", "Interrupted"),
        ("cancel-search", "after_ann", "Interrupted"),
        ("fail-search", "after_open", "Injected query lease failure"),
    ]:
        native.call(op, stage, expected=expected)
        require(not list((root.parent / "private").iterdir()), "Two-segment failure retained private leases")
    native.call("capture", "held", "knn")
    worker = CAT.Worker(proof, "held-worker", max_views=2)
    workers.append(worker)
    opened = worker.open("held", native.capture("held"))
    require(len(opened["generations"]) == 2, "Retained view opened only one B segment")
    limited = CAT.Worker(proof, "limited-worker", max_views=1, max_artifact_bytes=opened["artifact_bytes"] - 1)
    workers.append(limited)
    limited.open("too-large", native.capture("held"), expected="artifact byte budget exceeded")
    require(
        limited.call({"op": "stats"}) == MIXED.READ.empty_stats(),
        "Partial multi-segment admission retained leases or reservations",
    )
    limited.finish(eof=True)
    baseline = worker.search("held", VECTOR)
    recalls.append(MIXED.qualify(baseline, rows, VECTOR))
    allowed = [MIXED.ref(row) for row in rows if row["category"] == "B"]
    recalls.append(MIXED.qualify(worker.search("held", VECTOR, allowed), rows, VECTOR, allowed=allowed))
    replacement = reference("explicit-rebuild")
    proof.run([str(proof.probe), "build-admitted-file-index", str(native.capture("two-B")), str(replacement)])
    maintenance = sql(
        native,
        root,
        "UPDATE lake.items SET payload='changed-after-capture',embedding=[9000,0,0,0,0,0,0,0] WHERE id=200000;"
        "CALL ducklake_flush_inlined_data('lake');CALL ducklake_rewrite_data_files('lake',delete_threshold=>0);"
        "CALL ducklake_merge_adjacent_files('lake');"
        "SELECT count(*) FROM ducklake_expire_snapshots('lake',older_than=>now());"
        "SELECT count(*) FROM ducklake_cleanup_old_files('lake',cleanup_all=>true)",
    )
    held = native.call("held-search", replacement, 2, maintenance, artifacts)
    require(
        held["rows"] == core["rows"] and held["diagnostics"]["routes"]["ann_calls"] == 2,
        "Held native query changed its two-generation view during publication/GC",
    )
    require(
        all(
            not Path(f["uri"]).exists()
            for f in CAT.descriptor(native.capture("held"))["capture"]["file_capture"]["files"]
        ),
        "Maintenance did not reclaim actual B/F source files",
    )
    require(not artifacts.exists(), "Canonical index generations were not reclaimed")
    require(worker.search("held", VECTOR) == baseline, "GC invalidated a retained two-segment view")
    wanted = [next(row for row in rows if row["id"] == i) for i in [5000, 6000, 200000, 0, 5000]]
    require(
        worker.fetch("held", [MIXED.ref(row) for row in wanted]) == [MIXED.READ.business(row) for row in wanted],
        "Delayed B1/B2/F/T fetch changed row versions, Unicode/nulls or duplicate ordering",
    )
    native.call("core-search", expected="Referenced Vortex index file is absent")
    require(
        worker.close_view("held") == MIXED.READ.empty_stats(),
        "Closing a two-segment view retained aggregate reservations",
    )
    worker.finish(eof=True)
    checks.append(
        "native and retained two-B views survive publication, actual B/F source GC and canonical index removal; delayed duplicate fetch stays on the held snapshot"
    )
    checks.append(
        "the artifact budget applies to B1 plus B2; rejecting the second segment releases the first segment and all source reservations"
    )
    return {
        "fixture_recalls": recalls,
        "first_generation": generation(first),
        "second_generation": second["generation"]["generation"],
        "first_artifact_signature": frozen,
        "indexed_delta_rows": 128,
        "max_segments": 2,
    }


def crash_case(driver, root, stage, checks):
    native = CAT.Native(driver, root / "native")
    try:
        artifacts, _, first, frozen = setup(native, root)
        add_f(native, 5000)
        target = artifacts / "crash-B2.json"
        marker = native.root / f"build-{stage}.ready"
        native.send("crash-append", stage, target, 1)
        deadline = time.monotonic() + 60
        while not marker.exists():
            require(
                native.process.poll() is None and time.monotonic() < deadline, "Append did not reach its crash boundary"
            )
            time.sleep(0.01)
        candidate = CAT.generation(target)
        native.process.kill()
        native.process.wait(timeout=5)
        native.log.close()
        require(native.process.returncode == -9, "Append fixture was not killed with SIGKILL")
        native.start("resume")
        head = native.call("head", "knn")
        expected = 2 if stage == "after_commit" else 1
        require(
            head["revision"] == expected and len(segments(head)) == expected, "Crash exposed a partial segment manifest"
        )
        unchanged(native, first, frozen)
        if stage == "before_commit":
            retried = artifacts / "fresh-B2.json"
            native.call("append", "knn", retried, 1)
            require(CAT.generation(retried) != candidate, "Restart silently replayed an uncommitted candidate")
        else:
            require(generation(segments(head)[1]) == candidate, "Committed B2 was rebuilt after restart")
        recalls = []
        qualify(native, "recovered", recalls, two=True)
        native.finish()
        checks.append(f"SIGKILL {stage} recovers the complete old/new segment set and leaves B1 unchanged")
        return {"ready_revision_at_reopen": expected, "fixture_recalls": recalls}
    finally:
        if native.process.poll() is None:
            native.process.kill()
            native.process.wait(timeout=5)


def aggregate_rows(driver, root, checks):
    native = CAT.Native(driver, root / "native")
    try:
        artifacts, _, first, frozen = setup(native, root)
        native.sql(
            "CALL ducklake_set_option('lake','data_inlining_row_limit',0,table_name=>'items');"
            "INSERT INTO lake.items VALUES(9999,[0,0,0,0,0,0,0,0],'A','small-F')"
        )
        native.call("append", "knn", artifacts / "small.json", 1, expected="requires at least 64 rows")
        unchanged(native, first, frozen)
        native.sql(
            "INSERT INTO lake.items SELECT 10000+i,[i::FLOAT,0,0,0,0,0,0,0],'A','global-cap' FROM range(95904)t(i)"
        )
        target = artifacts / "rejected.json"
        native.call("append", "knn", target, 1, expected="physical row budget exceeded")
        require(not target.exists(), "Incremental builder ignored the total table capacity")
        unchanged(native, first, frozen)
        native.finish()
        checks.append(
            "incremental capture keeps the global 100,000-row ceiling: a delta that fits alone cannot bypass B1 plus F admission"
        )
        checks.append("a delta below the static SPFresh minimum fails explicitly without rebuilding B1")
    finally:
        if native.process.poll() is None:
            native.process.kill()
            native.process.wait(timeout=5)


def background(driver, root, checks):
    spec = importlib.util.spec_from_file_location(
        "segment_optimizer", Path(__file__).with_name("test_native_vortex_optimizer.py")
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    native = CAT.Native(driver, root / "native")
    try:
        artifacts, _, first, frozen = setup(native, root)
        native.call("optimizer-init")
        native.call("optimizer-enable", artifacts, 100000, 4194304, 128, 67108864)
        native.call("optimizer-resume", "build_started", "pause")
        native.sql(
            "SELECT * FROM ducklake_set_vortex_write_budget('lake','main','items','knn',32,1048576,128,67108864)"
        )
        add_f(native, 5000)
        native.call("optimizer-wait")
        require(native.call("head", "knn")["revision"] == 1, "Background construction published an unfinished B2")
        native.sql("INSERT INTO lake.items VALUES(200000,[133.0625,4,2,0,1,8,7,16],'A','后台-T')")
        native.call("optimizer-release")
        module.wait(
            native, lambda state: state["published_revision"] == 2 and state["state"] == "published", "background B2"
        )
        unchanged(native, first, frozen)
        add_f(native, 6000)
        stopped = module.wait(
            native, lambda state: state["state"] == "failed" and not state["running"], "two-segment limit"
        )
        require(
            not stopped["enabled"] and "segment count budget exceeded" in stopped["error"],
            "Background exceeded its segment budget",
        )
        require(native.call("head", "knn")["revision"] == 2, "Background silently rebuilt B1/B2 after exhausting slots")
        rejected = sql(
            native,
            root,
            "INSERT INTO lake.items SELECT 10000+i,[i::FLOAT,0,0,0,0,0,0,0],'B','over-limit' FROM range(128)t(i)",
        )
        native.call("sql", rejected, expected="unindexed rows")
        recalls = []
        core, _ = qualify(native, "bounded-background", recalls, two=True)
        require(
            core["diagnostics"]["routes"]["scan_files"] and core["diagnostics"]["routes"]["inline_rows"],
            "Exhaustion hid F/T",
        )
        unchanged(native, first, frozen)
        rebuilt = native.call("build", "knn", artifacts / "explicit-consolidation.json", 2)
        require(
            rebuilt["revision"] == 3 and len(segments(native.call("head", "knn"))) == 1,
            "Explicit consolidation did not restore capacity",
        )
        add_f(native, 7000)
        native.finish()
        checks.append(
            "background F-to-B appends B2 without rebuilding B1; two-slot exhaustion stops explicitly, preserves F/T and write quotas, and explicit consolidation restores capacity"
        )
        return {
            "fixture_recalls": recalls,
            "stopped_run_count": stopped["run_count"],
            "explicit_consolidation_revision": 3,
        }
    finally:
        if native.process.poll() is None:
            native.process.kill()
            native.process.wait(timeout=5)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ["duckdb", "driver", "probe", "output-dir"]:
        parser.add_argument("--" + name, required=True, type=Path)
    args = parser.parse_args()
    root = args.output_dir.resolve()
    root.mkdir(parents=True, exist_ok=False)
    private = root / "private"
    private.mkdir()
    os.environ["TMPDIR"] = str(private)
    directory = root / "main"
    directory.mkdir()
    proof = BASE.Proof(args.duckdb.resolve(strict=True), args.probe.resolve(strict=True), directory)
    native = CAT.Native(args.driver.resolve(strict=True), directory / "native")
    workers = []
    report = {
        "status": "running",
        "vortex_revision": BASE.VORTEX_REVISION,
        "driver_sha256": BASE.digest(args.driver),
        "checks": [],
    }
    started = time.monotonic()
    try:
        report["main"] = exercise(proof, native, directory, report["checks"], workers)
        native.finish()
        for stage in ["before_commit", "after_commit"]:
            directory = root / stage
            directory.mkdir()
            report[stage] = crash_case(args.driver.resolve(strict=True), directory, stage, report["checks"])
        directory = root / "aggregate-rows"
        directory.mkdir()
        aggregate_rows(args.driver.resolve(strict=True), directory, report["checks"])
        directory = root / "background"
        directory.mkdir()
        report["background"] = background(args.driver.resolve(strict=True), directory, report["checks"])
        require(not list(private.iterdir()), "Segment qualification retained private query leases")
        report["status"] = "passed"
    except Exception as error:
        report.update(status="failed", error=str(error))
        raise
    finally:
        for process in [native.process] + [worker.process for worker in workers]:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)
        report["seconds"] = time.monotonic() - started
        (root / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
