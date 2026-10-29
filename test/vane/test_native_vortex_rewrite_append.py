#!/usr/bin/env python3
"""Qualify one native build with late file-ID binding under committed appends."""
from __future__ import annotations

import argparse
import importlib.util
import json
import os
from pathlib import Path
import time

SPEC = importlib.util.spec_from_file_location(
    "rewrite", Path(__file__).with_name("test_native_vortex_index_rewrite.py")
)
assert SPEC is not None and SPEC.loader is not None
RW = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RW)
MA, M, SEG, CAT, BASE = RW.MA, RW.M, RW.SEG, RW.CAT, RW.BASE
require = BASE.require


def append_sql(native, root, cycle=0):
    return SEG.sql(
        native,
        root,
        f"INSERT INTO lake.items SELECT {10000 + cycle * 100}+i,[9000+i,0,0,0,0,0,0,0],'A','append-F' FROM range(32)t(i);"
        "CALL ducklake_flush_inlined_data('lake',table_name=>'items');"
        f"INSERT INTO lake.items VALUES({200001 + cycle},[133.0625,4,2,0,1,8,7,16],'A',NULL)",
    )


def output(native, record):
    segment = SEG.segments(record)[-1]
    intent = next(
        row
        for row in RW.intents(native)
        if row["new_path"] in {f["uri"] for f in segment["reference"]["snapshot"]["files"]}
    )
    file = next(f for f in segment["reference"]["snapshot"]["files"] if f["uri"] == intent["new_path"])
    metadata = next(row for row in native.call("source-status") if row["file_id"] == file["id"])
    require(
        intent["keep_file"] and intent["new_file_id"] == file["id"],
        "Committed ownership lost the final file ID",
    )
    require(
        metadata["end_snapshot"] is None and metadata["rows"] == file["row_count"],
        "Final row map does not match the active native file",
    )
    require(
        metadata["begin_snapshot"] > intent["capture_snapshot"] + 1,
        "Proof did not commit appends during construction",
    )
    return segment, file, intent


def identical_native(segment, intent):
    workspace = Path(segment["root"]) / (".build-" + intent["build_id"])
    prepared = workspace / "prepared/artifacts/spfresh/native"
    published = Path(segment["root"]) / SEG.generation(segment) / "artifacts/spfresh/native"
    names = ["vectors.bin", "tree.bin", "graph.bin", "deletes.bin", "head_ids.bin", "postings.bin"]
    for name in names:
        require(
            BASE.digest(prepared / name) == BASE.digest(published / name),
            "Final binding changed trained native bytes",
        )


def exercise(proof, native, root, checks, workers):
    rewritten, first, parts, frozen = RW.setup(native, root, maintenance=False)
    native.sql("INSERT INTO lake.items VALUES(200000,[133.125,4,2,0,1,8,7,16],'A','old-T')")
    recalls = []
    M.qualify(native, "old", recalls, 3)
    old_rows = native.rows("old")
    native.call("capture", "held", "knn")
    worker = CAT.Worker(proof, "held", max_views=1)
    workers.append(worker)
    worker.open("held", native.capture("held"))
    baseline = worker.search("held", M.VECTOR)
    evidence = []
    stages = [
        "rewrite_allocated",
        "rewrite_written",
        "generation_sealed",
        "rewrite_candidate_ready",
        "rewrite_source_linked",
    ]
    for cycle, (stage, cutoff) in enumerate(zip(stages, [2148, 1124, 612, 356, 228])):
        if cycle:
            native.sql(f"DELETE FROM lake.items WHERE id>={cutoff} AND id<4096")
        current = first if not cycle else RW.selected(native)
        result = native.call(
            "rewrite-write-race",
            stage,
            SEG.generation(current),
            rewritten / f"append-{cycle}.json",
            3 + cycle,
            append_sql(native, root, cycle),
        )
        require("published" in result, f"Independent appends prevented rewrite publication: {result}")
        require(
            result["native_builds"] == result["bindings"] == 1,
            "Append caused a native rebuild or repeated binding",
        )
        require(result["published"]["revision"] == 4 + cycle, "Rewrite did not publish exactly one successor")
        segment, file, intent = output(native, result["published"])
        require(file["id"] > result["provisional_file_id"], "Append did not consume the provisional file ID")
        identical_native(segment, intent)
        expected = RW.active_rows(native) + 1
        native.sql(
            f"SELECT CASE WHEN (SELECT count(*) FROM lake.items)={expected} "
            f"AND (SELECT count(DISTINCT rowid) FROM lake.items)={expected} THEN true "
            "ELSE error('rewrite lost append counts or stable row IDs') END"
        )
        RW.unchanged(native, parts, frozen)
        M.qualify(native, f"append-{cycle}", recalls, 3)
        require(worker.search("held", M.VECTOR) == baseline, "Concurrent appends changed an old private view")
        evidence.append(
            {
                "stage": stage,
                "provisional_file_id": result["provisional_file_id"],
                "final_file_id": file["id"],
                "rows": RW.active_rows(native),
            }
        )
    require(RW.active_rows(native) == 549, "Five rewrites lost appends or retained ended B vectors")
    native.call("maintenance-enable", 16, RW.LIMIT)
    MA.drain(native)
    require(not RW.intents(native), "Late-bound build ownership did not drain")
    native.sql(
        "SELECT * FROM ducklake_expire_snapshots('lake',older_than=>now()); SELECT * FROM ducklake_cleanup_old_files('lake',cleanup_all=>true)"
    )
    require(
        all(not Path(f["uri"]).exists() for f in first["reference"]["snapshot"]["files"]),
        "Old canonical source survived explicit expiry and GC",
    )
    require(
        worker.search("held", M.VECTOR) == baseline,
        "Old view depended on retired canonical source/index files",
    )
    wanted = [next(row for row in old_rows if row["id"] == i) for i in [128, 200, 5000, 6000, 200000, 200]]
    require(
        worker.fetch("held", [CAT.MIXED.ref(row) for row in wanted])
        == [CAT.MIXED.READ.business(row) for row in wanted],
        "Delayed old-view typed fetch changed after late binding and GC",
    )
    RW.finish(native)
    checks.extend(
        [
            "five rewrites accept committed T/F appends across construction stages and bind fresh file IDs without another native build",
            "all six published native artifacts equal the prepared bytes and both unselected B generations remain unchanged",
            "current B/F/T queries preserve visibility and typed rows while old private search/fetch survive ANN and source GC",
        ]
    )
    return {"fixture_recalls": recalls, "revision": 8, "physical_rows": 549, "bindings": evidence}


def live_append(driver, root, checks):
    native = CAT.Native(driver, root / "native")
    try:
        rewritten, first, parts, frozen = RW.setup(native, root, maintenance=False)
        text = ";".join(append_sql(native, root, cycle).read_text() for cycle in range(3))
        result = native.call(
            "rewrite-live-append",
            SEG.generation(first),
            rewritten / "live.json",
            3,
            SEG.sql(native, root, text),
        )
        require(result["native_builds"] == result["bindings"] == 1, "Live appends retrained the index")
        segment, file, intent = output(native, result["published"])
        require(
            file["id"] > result["provisional_file_id"] and RW.active_rows(native) == 2402,
            "Live appends were lost or final binding kept the provisional file ID",
        )
        identical_native(segment, intent)
        native.call("maintenance-enable", 16, RW.LIMIT)
        MA.drain(native)
        RW.unchanged(native, parts, frozen)
        recalls = []
        M.qualify(native, "live-appends", recalls, 3)
        RW.finish(native)
        checks.append(
            "a separate writer runs nine T/flush/T commits concurrently with native construction; one final binding retains all appends"
        )
        return {
            "fixture_recalls": recalls,
            "provisional_file_id": result["provisional_file_id"],
            "final_file_id": file["id"],
        }
    finally:
        M.stop(native)


def conflict(driver, root, kind, checks):
    native = CAT.Native(driver, root / "native")
    try:
        rewritten, first, parts, frozen = RW.setup(native, root, maintenance=False)
        before = native.call("source-status")
        if kind == "delete":
            text = "DELETE FROM lake.items WHERE id=200"
        elif kind == "update":
            text = "UPDATE lake.items SET embedding=[133.03125,4,2,0,1,8,7,16],payload=NULL WHERE id=200"
        else:
            reference = Path(first["root"]) / "B1.json"
            text = f"SELECT * FROM ducklake_publish_vortex_index('lake','main','items','alias','{reference}',0)"
        result = native.call(
            "rewrite-write-race",
            "rewrite_candidate_ready",
            SEG.generation(first),
            rewritten / "conflict.json",
            3,
            SEG.sql(native, root, text),
        )
        require(
            "changed before commit" in result.get("error", ""),
            f"Changed rewrite input was accepted or retried: {result}",
        )
        require(native.call("head", "knn")["revision"] == 3, "Conflicting rewrite changed the ready index")
        require(native.call("source-status") == before, "Conflicting rewrite published a replacement source file")
        native.call("maintenance-enable", 16, RW.LIMIT)
        MA.drain(native)
        RW.unchanged(native, parts, frozen)
        recalls = []
        M.qualify(native, "after-conflict", recalls, 3)
        RW.finish(native)
        checks.append(
            f"{kind} changes cause an explicit input/schema/index conflict without replacement, fallback or retry"
        )
        return {"fixture_recalls": recalls}
    finally:
        M.stop(native)


def schema_boundary(driver, root, target, checks):
    native = CAT.Native(driver, root / "native")
    try:
        rewritten, first, parts, frozen = RW.setup(native, root, maintenance=False)
        text = (
            "ALTER TABLE lake.items ALTER COLUMN payload SET DEFAULT 'new-default'"
            if target
            else "CREATE TABLE lake.unrelated(value INTEGER)"
        )
        result = native.call(
            "rewrite-write-race",
            "rewrite_candidate_ready",
            SEG.generation(first),
            rewritten / "schema.json",
            3,
            SEG.sql(native, root, text),
            expected="fixed schema and partitioning" if target else None,
        )
        if not target:
            require(
                "published" in result,
                f"An unrelated schema commit invalidated unchanged target inputs: {result}",
            )
        require(
            native.call("head", "knn")["revision"] == 4 and RW.active_rows(native) == 2304,
            "Schema boundary did not preserve the target schema and complete the rewrite",
        )
        native.call("maintenance-enable", 16, RW.LIMIT)
        MA.drain(native)
        RW.unchanged(native, parts, frozen)
        recalls = []
        M.qualify(native, "schema-boundary", recalls, 3)
        RW.finish(native)
        checks.append(
            "target schema mutation is rejected by the native fixed-schema writer; unchanged rewrite inputs still publish"
            if target
            else "an unrelated schema commit advances global schema_version without invalidating the target table's unchanged schema"
        )
        return {"fixture_recalls": recalls}
    finally:
        M.stop(native)


def final_commit(driver, root, stage, checks):
    native = CAT.Native(driver, root / "native")
    try:
        rewritten, first, parts, frozen = RW.setup(native, root, maintenance=False)
        sql = SEG.sql(
            native,
            root,
            "INSERT INTO lake.items VALUES(200001,[133.03125,4,2,0,1,8,7,16],'A','after-publication')",
        )
        result = native.call("rewrite-commit-race", stage, SEG.generation(first), rewritten / "commit.json", 3, sql)
        require(
            "published" in result and result["native_builds"] == result["bindings"] == 1,
            f"Final publication failed or rebuilt native bytes: {result}",
        )
        native.call("maintenance-enable", 16, RW.LIMIT)
        MA.drain(native)
        RW.unchanged(native, parts, frozen)
        recalls = []
        M.qualify(native, "after-commit", recalls, 3)
        RW.finish(native)
        checks.append(
            f"commit at {stage} waits for atomic publication and succeeds against the successor without losing its row"
        )
        return {"fixture_recalls": recalls}
    finally:
        M.stop(native)


def interrupted_binding(driver, root, op, stage, checks):
    native = CAT.Native(driver, root / "native")
    try:
        rewritten, first, parts, frozen = RW.setup(native, root, maintenance=False)
        before = native.call("source-status")
        native.call(
            op,
            stage,
            SEG.generation(first),
            rewritten / "failed.json",
            3,
            expected="Injected indexed rewrite failure" if op == "rewrite-fail" else "Interrupted",
        )
        require(
            native.call("head", "knn")["revision"] == 3 and native.call("source-status") == before,
            "Binding failure partially published metadata",
        )
        native.call("maintenance-enable", 16, RW.LIMIT)
        MA.drain(native)
        RW.unchanged(native, parts, frozen)
        recalls = []
        M.qualify(native, "after-interruption", recalls, 3)
        RW.finish(native)
        checks.append(
            f"{op} at {stage} leaves the old source/index intact and explicit maintenance reclaims both private generations"
        )
        return {"fixture_recalls": recalls}
    finally:
        M.stop(native)


def corrupt(driver, root, checks):
    native = CAT.Native(driver, root / "native")
    try:
        rewritten, first, parts, frozen = RW.setup(native, root, maintenance=False)
        before = native.call("source-status")
        native.call(
            "rewrite-corrupt-prepared",
            "rewrite_candidate_ready",
            SEG.generation(first),
            rewritten / "corrupt.json",
            3,
            expected="checksum",
        )
        require(
            native.call("head", "knn")["revision"] == 3 and native.call("source-status") == before,
            "Corrupt prepared bytes were published",
        )
        native.call("maintenance-enable", 16, RW.LIMIT)
        MA.drain(native)
        RW.unchanged(native, parts, frozen)
        recalls = []
        M.qualify(native, "after-corruption", recalls, 3)
        RW.finish(native)
        checks.append(
            "corrupt prepared native bytes fail verification before final binding and are never rebuilt or adopted"
        )
        return {"fixture_recalls": recalls}
    finally:
        M.stop(native)


def crash(driver, root, stage, checks):
    native = CAT.Native(driver, root / "native")
    try:
        rewritten, first, parts, frozen = RW.setup(native, root, maintenance=False)
        native.call("maintenance-enable", 16, RW.LIMIT)
        MA.drain(native)
        native.call("maintenance-cancel")
        before = RW.active_rows(native)
        marker = native.root / f"rewrite-{stage}.ready"
        native.send(
            "rewrite-crash-append",
            stage,
            SEG.generation(first),
            rewritten / "crash.json",
            3,
            append_sql(native, root),
        )
        deadline = time.monotonic() + 60
        while not marker.exists():
            require(native.process.poll() is None, "Rewrite owner exited before the crash checkpoint")
            require(time.monotonic() < deadline, f"Rewrite crash checkpoint timed out at {stage}")
            time.sleep(0.01)
        MA.kill(native)
        committed = stage == "rewrite_after_commit"
        require(
            native.call("head", "knn")["revision"] == 3 + committed,
            "Crash exposed mixed source/index metadata",
        )
        require(
            RW.active_rows(native) == before + 32 - (2048 if committed else 0),
            "Crash lost committed appends or published only part of the rewrite",
        )
        records = RW.intents(native)
        require(len(records) == 1 and records[0]["keep_file"] == committed, "Crash lost atomic source ownership")
        target = Path(records[0]["new_path"])
        status = RW.BG.status(native)
        MA.steady(native)
        require(RW.BG.status(native) == status, "Restart implicitly retried or collected a stopped rewrite")
        native.call("maintenance-resume")
        MA.drain(native)
        require(
            not RW.intents(native) and target.exists() == committed,
            "Recovery deleted a winner or retained an abandoned managed output",
        )
        RW.unchanged(native, parts, frozen)
        recalls = []
        M.qualify(native, "recovered", recalls, 3)
        RW.finish(native)
        checks.append(
            f"SIGKILL at {stage} retains acknowledged T/F appends, exposes one complete source/index version and resumes GC explicitly"
        )
        return {"fixture_recalls": recalls}
    finally:
        M.stop(native)


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
    driver = args.driver.resolve(strict=True)
    directory = root / "main"
    directory.mkdir()
    native = CAT.Native(driver, directory / "native")
    proof = BASE.Proof(args.duckdb.resolve(strict=True), args.probe.resolve(strict=True), directory)
    workers = []
    report = {
        "status": "running",
        "driver_sha256": BASE.digest(driver),
        "checks": [],
        "vortex_revision": BASE.VORTEX_REVISION,
    }
    started = time.monotonic()
    try:
        report["main"] = exercise(proof, native, directory, report["checks"], workers)
        cases = [
            ("conflict-" + kind, lambda d, r, c, k=kind: conflict(d, r, k, c)) for kind in ["delete", "update", "alias"]
        ]
        cases.insert(0, ("live-appends", live_append))
        cases.extend(
            [
                ("schema-target", lambda d, r, c: schema_boundary(d, r, True, c)),
                ("schema-unrelated", lambda d, r, c: schema_boundary(d, r, False, c)),
            ]
        )
        for stage in ["rewrite_bind_created", "rewrite_bound", "rewrite_after_stage"]:
            cases.append(("commit-" + stage, lambda d, r, c, s=stage: final_commit(d, r, s, c)))
        for op in ["rewrite-fail", "rewrite-cancel"]:
            for stage in [
                "rewrite_bind_started",
                "rewrite_bind_created",
                "rewrite_bind_sealed",
                "rewrite_bound",
            ]:
                cases.append((op + "-" + stage, lambda d, r, c, s=stage, o=op: interrupted_binding(d, r, o, s, c)))
        cases.append(("corrupt", corrupt))
        for stage in [
            "rewrite_candidate_ready",
            "rewrite_source_linked",
            "rewrite_bind_created",
            "rewrite_bind_sealed",
            "reference_written",
            "rewrite_after_stage",
            "rewrite_after_commit",
        ]:
            cases.append(("crash-" + stage, lambda d, r, c, s=stage: crash(d, r, s, c)))
        report["cases"] = []
        for name, run in cases:
            directory = root / name
            directory.mkdir()
            report["cases"].append({"name": name, **run(driver, directory, report["checks"])})
        require(not list(private.iterdir()), "Late-binding proof leaked private leases")
        report["status"] = "passed"
    except Exception as error:
        report.update(status="failed", error=str(error))
        raise
    finally:
        M.stop(native)
        for worker in workers:
            if worker.process.poll() is None:
                worker.process.kill()
                worker.process.wait(timeout=5)
        report["seconds"] = time.monotonic() - started
        (root / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
