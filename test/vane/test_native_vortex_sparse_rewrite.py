#!/usr/bin/env python3
"""Qualify explicit sparse-B to F publication, admission, recovery and retained views."""
from __future__ import annotations

import argparse
import importlib.util
import json
import os
from pathlib import Path
import time

SPEC = importlib.util.spec_from_file_location(
    "empty", Path(__file__).with_name("test_native_vortex_empty_retirement.py")
)
assert SPEC is not None and SPEC.loader is not None
ER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ER)
BG, RW, MA, M, SEG, CAT, BASE, OPT = ER.BG, ER.RW, ER.MA, ER.M, ER.SEG, ER.CAT, ER.BASE, ER.OPT
require = BASE.require


def initialize(native, root, first, index_rows=100000):
    native.call("optimizer-init")
    native.call("optimizer-enable", root / "artifacts", 100000, 4194304, index_rows, RW.LIMIT)
    BG.stop(native)
    native.sql(
        "SELECT * FROM ducklake_set_vortex_write_budget('lake','main','items','knn',100000,4194304,256,67108864)"
    )
    native.call("maintenance-enable", 16, RW.LIMIT)
    MA.drain(native)
    BG.configure(native, 1, 1)
    return first


def single(native, root, rows=32, index_rows=100000):
    _, _, first, _ = SEG.setup(native, root)
    initialize(native, root, first, index_rows)
    native.sql(f"DELETE FROM lake.items WHERE id<128 OR id>={128 + rows}")
    return first


def one_intent(native):
    rows = RW.intents(native)
    require(len(rows) == 1, f"Expected one owned source rewrite: {rows}")
    return rows[0]


def no_ann_build(native):
    for row in native.call("build-status"):
        require(not row["keep_generation"], "F rewrite claimed an ANN generation")
        require(not Path(row["index_root"], row["generation"]).exists(), "F rewrite created ANN artifacts")
        require(not Path(row["workspace"], "prepared").exists(), "F rewrite invoked the native ANN preparer")


def qualify(native, label, recalls, count):
    return ER.qualify(native, label, recalls, count)


def exercise(proof, native, root, checks, workers):
    first, parts, frozen = ER.three(native, root)
    native.call("capture", "old", "knn")
    old_rows = native.rows("old")
    worker = CAT.Worker(proof, "held", max_views=2)
    workers.append(worker)
    worker.open("old", native.capture("old"))
    old = worker.search("old", M.VECTOR)
    native.call("rewrite-f", SEG.generation(first), 3, expected="requires 1..64 visible rows")
    native.sql("DELETE FROM lake.items WHERE id<128 OR (id>=160 AND id<4096)")
    native.sql("CREATE TABLE expected AS SELECT rowid,id,embedding,category,payload FROM lake.items")
    BG.configure(native, 1, 1)
    state = BG.pause(native, "rewrite_candidate_ready")
    require(
        state["task_kind"] == "rewrite_f"
        and state["state"] == "rewriting"
        and state["selected_generations"] == [SEG.generation(first)]
        and state["remaining_rows"] == 32
        and state["candidate_generation"] == "",
        "Worker did not durably select explicit F publication",
    )
    intent = one_intent(native)
    provisional = intent["new_file_id"]
    no_ann_build(native)
    before = native.call("head", "knn")
    before_sources = native.call("source-status")
    native.call("sql", BG.APP.append_sql(native, root))
    native.sql("INSERT INTO expected SELECT rowid,id,embedding,category,payload FROM lake.items WHERE id>=10000")
    require(native.call("head", "knn") == before, "Private F preparation changed the ready plan")
    during = BG.visible_only(native, "during")
    native.call("optimizer-release")
    row = BG.published(native, 4)
    BG.stop(native)
    head = native.call("head", "knn")
    require(
        head["reference"]["publication"] == row["run_id"] and SEG.segments(head) == parts,
        "F publication lost ownership or rebuilt another B",
    )
    output = intent
    actual = next(r for r in native.call("source-status") if r["path"].endswith(Path(output["new_path"]).name))
    require(
        actual["file_id"] > provisional and actual["end_snapshot"] is None,
        "F output did not bind a fresh final file ID",
    )
    require(RW.active_rows(native) == 320, "Sparse rewrite did not replace 4,096 physical B rows with 32 F rows")
    require(
        all(
            next(now for now in native.call("source-status") if now["file_id"] == old_file["file_id"])["end_snapshot"]
            is not None
            for old_file in before_sources
            if old_file["file_id"] in {f["id"] for f in M.files([first])}
        ),
        "Sparse rewrite left selected old source IDs active",
    )
    native.sql(
        "SELECT CASE WHEN count(*)=0 THEN true ELSE error('F rewrite changed rows or stable row IDs') END FROM ((SELECT rowid,id,embedding,category,payload FROM lake.items EXCEPT ALL SELECT * FROM expected) UNION ALL (SELECT * FROM expected EXCEPT ALL SELECT rowid,id,embedding,category,payload FROM lake.items))"
    )
    RW.unchanged(native, parts, frozen)
    no_ann_build(native)
    recalls = []
    core, _ = qualify(native, "after-F", recalls, 2)
    require(
        len(core["diagnostics"]["routes"]["scan_files"]) == 2 and core["diagnostics"]["routes"]["inline_rows"] == 1,
        "F/T appends were not composed in the new plan",
    )
    require(worker.search("old", M.VECTOR) == old, "Sparse publication changed a held old view")
    native.call("capture", "new", "knn")
    worker.open("new", native.capture("new"))
    current = worker.search("new", M.VECTOR)
    MA.drain(native)
    native.sql(
        "SELECT * FROM ducklake_expire_snapshots('lake',older_than=>now());SELECT * FROM ducklake_cleanup_old_files('lake',cleanup_all=>true)"
    )
    require(
        all(not Path(f["uri"]).exists() for f in M.files([first])),
        "Expired selected source files survived native cleanup",
    )
    require(Path(output["new_path"]).is_file(), "Build GC collected the published F output")
    require(
        worker.search("old", M.VECTOR) == old and worker.search("new", M.VECTOR) == current,
        "GC invalidated old/new views",
    )
    wanted = [next(row for row in old_rows if row["id"] == i) for i in [128, 200, 5000, 6000, 200]]
    require(
        worker.fetch("old", [CAT.MIXED.ref(r) for r in wanted]) == [CAT.MIXED.READ.business(r) for r in wanted],
        "Delayed old-view fetch lost row versions after GC",
    )
    worker.close_view("old")
    worker.close_view("new")
    worker.finish(eof=True)
    BG.finish(native)
    checks.extend(
        [
            "one selected B rewrites to F without an ANN build; both unselected descriptors and artifact bytes remain unchanged",
            "concurrent T/flush/T commits consume the provisional file ID; final F binding and source/index publication remain atomic",
            "current rows, stable row IDs, category filtering, F/T top-k and typed take match the native oracle",
            "old/new retained views and delayed typed fetch survive index, workspace and explicit source GC; published F remains owned",
        ]
    )
    return {"fixture_recalls": recalls, "informational_prepublication_recall": during, "released_rows": 4064}


def boundary(driver, root, rows, checks):
    native = CAT.Native(driver, root / "native")
    try:
        first = single(native, root, rows)
        if not rows:
            native.call("rewrite-f", SEG.generation(first), 1, expected="requires 1..64 visible rows")
        native.call("optimizer-resume")
        row = BG.published(native, 2)
        require(
            row["task_kind"] == ("rewrite_f" if rows else "retire") and RW.active_rows(native) == rows,
            "Sparse boundary retained indexed physical rows",
        )
        OPT.steady(native, row)
        recalls = []
        qualify(native, "boundary", recalls, 0)
        require(not SEG.segments(native.call("head", "knn")), "Last B was not removed from the explicit F plan")
        native.call("rewrite-f", SEG.generation(first), 2, expected="selected generation is not ready")
        BG.finish(native)
        checks.append(f"{rows} live rows become exact F with no empty/tiny ANN build")
        return {"fixture_recalls": recalls}
    finally:
        M.stop(native)


def healthy_file(driver, root, checks):
    native = CAT.Native(driver, root / "native")
    try:
        artifacts = root / "artifacts"
        artifacts.mkdir()
        native.call("init")
        native.sql(
            "INSERT INTO lake.items SELECT 5000+i,[133+i/16.0,4,2,0,1,8,7,16],'A','健康源文件' FROM range(32)t(i)"
        )
        native.call("build", "knn", artifacts / "original.json", 0)
        first = SEG.segments(native.call("head", "knn"))[0]
        initialize(native, root, first)
        native.sql("DELETE FROM lake.items WHERE id<4096")
        native.call("optimizer-resume")
        BG.published(native, 2)
        require(
            RW.active_rows(native) == 32,
            "Wholly masked inputs plus an undeleted small file were not compacted together",
        )
        recalls = []
        qualify(native, "healthy-retained", recalls, 0)
        BG.finish(native)
        checks.append(
            "F compaction includes undeleted members of the selected B instead of requiring a nonempty delete-only output"
        )
        return {"fixture_recalls": recalls}
    finally:
        M.stop(native)


def budget(driver, root, kind, checks):
    native = CAT.Native(driver, root / "native")
    try:
        first = single(native, root)
        if kind == "row":
            native.sql(
                "CALL ducklake_set_option('lake','data_inlining_row_limit',100,table_name=>'items');INSERT INTO lake.items SELECT 10000+i,[10000+i,0,0,0,0,0,0,0]::FLOAT[],'B','budget-F' FROM range(225)t(i)"
            )
        else:
            native.sql(
                "SELECT * FROM ducklake_set_vortex_write_budget('lake','main','items','knn',100000,4194304,256,1)"
            )
        before = native.call("head", "knn")
        sources = native.call("source-status")
        reason = "unindexed_" + kind + "_budget"
        native.call("rewrite-f", SEG.generation(first), 1, expected=reason)
        native.call("optimizer-resume")
        row = OPT.wait(native, lambda s: s["compaction_reason"] == reason, "F admission deferral")
        require(
            row["run_count"] == 0 and row["enabled"] and not row["running"],
            "Budget deferral created or retried a task",
        )
        OPT.steady(native, row)
        require(
            native.call("head", "knn") == before
            and native.call("source-status") == sources
            and not RW.intents(native)
            and not native.call("build-status"),
            "Known budget exhaustion allocated files or changed metadata",
        )
        if kind == "row":
            native.sql(
                "CALL ducklake_set_option('lake','data_inlining_row_limit',0,table_name=>'items');DELETE FROM lake.items WHERE id>=10000"
            )
        else:
            native.sql(
                "SELECT * FROM ducklake_set_vortex_write_budget('lake','main','items','knn',100000,4194304,256,67108864)"
            )
        BG.published(native, 2)
        recalls = []
        qualify(native, "budget-resolved", recalls, 0)
        BG.finish(native)
        checks.append(
            f"known F {kind} exhaustion defers without intent/allocation/retry; the enabled policy progresses when the budget condition is resolved"
        )
        return {"fixture_recalls": recalls}
    finally:
        M.stop(native)


def fixed_shape(driver, root, kind, checks):
    native = CAT.Native(driver, root / "native")
    try:
        first = single(native, root)
        head = native.call("head", "knn")
        sources = native.call("source-status")
        sql = (
            "ALTER TABLE lake.items SET PARTITIONED BY(category)"
            if kind == "partition"
            else "ALTER TABLE lake.items ADD COLUMN extra BIGINT"
        )
        native.call("sql", SEG.sql(native, root, sql), expected="fixed schema and partitioning")
        require(
            native.call("head", "knn") == head and native.call("source-status") == sources and not RW.intents(native),
            "Rejected DDL changed data or index state",
        )
        native.call("rewrite-f", SEG.generation(first), 1)
        require(RW.active_rows(native) == 32, "F rewrite failed after rejected schema/partition mutation")
        recalls = []
        qualify(native, "fixed-target", recalls, 0)
        BG.finish(native)
        checks.append(
            f"existing native {kind} DDL guard rejects mutation and preserves a valid explicit F rewrite target"
        )
        return {"fixture_recalls": recalls}
    finally:
        M.stop(native)


def interruption(driver, root, kind, checks):
    native = CAT.Native(driver, root / "native")
    foreign = []
    try:
        first = single(native, root)
        if kind in ["failure", "ack"]:
            native.call("optimizer-resume", "rewrite_after_commit" if kind == "ack" else "rewrite_after_stage", "fail")
        else:
            BG.pause(native, "rewrite_source_linked" if kind == "corrupt" else "rewrite_candidate_ready")
            if kind == "cancel":
                native.call("optimizer-cancel")
            elif kind == "input":
                native.sql("DELETE FROM lake.items WHERE id=140")
            elif kind == "budget":
                native.sql(
                    "CALL ducklake_set_option('lake','data_inlining_row_limit',100,table_name=>'items');INSERT INTO lake.items SELECT 10000+i,[10000+i,0,0,0,0,0,0,0]::FLOAT[],'B','concurrent-F' FROM range(240)t(i)"
                )
            elif kind == "race":
                native.call("rewrite-f", SEG.generation(first), 1)
            elif kind == "corrupt":
                path = Path(one_intent(native)["new_path"])
                data = path.read_bytes()
                path.write_bytes(bytes([data[0] ^ 1]) + data[1:])
            elif kind == "collision":
                foreign = [Path(one_intent(native)["new_path"])]
                foreign[0].write_bytes(b"foreign-owned-output")
            native.call("optimizer-release")
        row = ER.stopped(native)
        committed = kind in ["ack", "race"]
        require(
            row["run_count"] == 1 and row["candidate_generation"] == "",
            "F task trained ANN or retried automatically",
        )
        require(
            row["state"] == ("published" if kind == "ack" else "cancelled" if kind == "cancel" else "failed"),
            "F failure was misattributed as publication",
        )
        head = native.call("head", "knn")
        require(
            head["revision"] == 1 + committed
            and RW.active_rows(native) == (32 if committed else 4336 if kind == "budget" else 4096),
            "Failed publication changed only half of the data/index pair",
        )
        if kind == "race":
            require(
                head["reference"]["publication"] != row["run_id"],
                "Another publisher's F was attributed to the interrupted task",
            )
        if kind == "corrupt":
            require(
                "output file identity mismatch" in row["error"],
                "Corrupt F output escaped checksum verification",
            )
        OPT.steady(native, row)
        MA.drain(native)
        require(
            all(p.read_bytes() == b"foreign-owned-output" for p in foreign),
            "Cleanup deleted a foreign collision target",
        )
        if kind == "budget":
            native.sql(
                "CALL ducklake_set_option('lake','data_inlining_row_limit',0,table_name=>'items');DELETE FROM lake.items WHERE id>=10000"
            )
        native.call("optimizer-resume")
        row = OPT.wait(
            native,
            lambda s: s["compaction_reason"] == "no_segments" and not s["running"],
            "explicit F-task resumption",
        )
        require(row["run_count"] == (1 if committed else 2), "Explicit resume replayed a committed F publication")
        recalls = []
        qualify(native, "resumed-F", recalls, 0)
        BG.finish(native)
        checks.append(
            f"{kind} preserves atomic publication and exact task ownership, stops scheduling and requires explicit resume"
        )
        return {"fixture_recalls": recalls}
    finally:
        M.stop(native)


def crash(driver, root, stage, checks):
    native = CAT.Native(driver, root / "native")
    try:
        single(native, root)
        native.call("maintenance-cancel")
        native.call("optimizer-resume", stage, "pause")
        state = (
            "publishing"
            if stage in ["rewrite_after_stage", "rewrite_after_commit"]
            else "published" if stage == "optimizer_rewritten" else "rewriting"
        )
        durable = OPT.crash(native, stage, state)
        committed = stage in ["rewrite_after_commit", "optimizer_rewritten"]
        head = native.call("head", "knn")
        require(
            head["revision"] == 1 + committed and RW.active_rows(native) == (32 if committed else 4096),
            "Crash split native F data and index publication",
        )
        if committed:
            require(
                head["reference"]["publication"] == durable["run_id"] and not SEG.segments(head),
                "Restart lost the explicit F publication identity",
            )
        OPT.steady(native, OPT.status(native))
        native.call("optimizer-resume")
        row = OPT.wait(
            native,
            lambda s: s["compaction_reason"] == "no_segments" and not s["running"],
            "post-crash F publication",
        )
        require(
            row["run_count"] == (1 if committed else 2),
            "Restart replayed committed F or automatically retried an interrupted task",
        )
        native.call("maintenance-resume")
        recalls = []
        qualify(native, "recovered-F", recalls, 0)
        BG.finish(native)
        checks.append(
            f"SIGKILL at {stage} recovers a complete old/new plan and only explicit resume permits fresh work"
        )
        return {"fixture_recalls": recalls}
    finally:
        M.stop(native)


def capacity(driver, root, checks):
    native = CAT.Native(driver, root / "native")
    try:
        single(native, root)
        native.sql(
            "SELECT * FROM ducklake_set_vortex_write_budget('lake','main','items','knn',100000,4194304,100000,67108864);CALL ducklake_set_option('lake','data_inlining_row_limit',100,table_name=>'items');INSERT INTO lake.items SELECT 10000+i,[10000+i,0,0,0,0,0,0,0]::FLOAT[],'B','capacity-F' FROM range(95904)t(i)"
        )
        insert = SEG.sql(
            native,
            root,
            "INSERT INTO lake.items VALUES(200000,[133.0625,4,2,0,1,8,7,16],'A','capacity-restored')",
        )
        native.call("sql", insert, expected="physical and inline rows")
        native.call("optimizer-resume")
        BG.published(native, 2)
        BG.stop(native)
        require(RW.active_rows(native) == 95936, "Sparse F failed to release 4,064 physical row/vector slots")
        native.call("sql", insert)
        recalls = []
        qualify(native, "capacity-restored", recalls, 0)
        BG.finish(native)
        checks.append(
            "at the unchanged 100,000-row/vector ceiling, 4,096-row B becomes 32-row F, retains 95,904 existing F rows and admits a new T"
        )
        return {"fixture_recalls": recalls, "released_rows": 4064}
    finally:
        M.stop(native)


def reindex(driver, root, checks):
    native = CAT.Native(driver, root / "native")
    try:
        single(native, root, index_rows=1)
        native.call("optimizer-resume")
        BG.published(native, 2)
        row = OPT.wait(
            native,
            lambda s: s["compaction_reason"] == "no_segments" and s["unindexed_rows"] == 32,
            "tiny F stabilization",
        )
        OPT.steady(native, row)
        native.sql(
            "CALL ducklake_set_option('lake','data_inlining_row_limit',0,table_name=>'items');INSERT INTO lake.items SELECT 10000+i,[10000+i,0,0,0,0,0,0,0]::FLOAT[],'B','next-F' FROM range(32)t(i)"
        )
        row = OPT.wait(native, lambda s: s["unindexed_rows"] == 64, "subminimum F staging")
        require(row["run_count"] == 1, "A 64-row F started a native ANN build")
        OPT.steady(native, row)
        native.sql(
            "INSERT INTO lake.items SELECT 11000+i,[11000+i,0,0,0,0,0,0,0]::FLOAT[],'B','buildable-F' FROM range(64)t(i)"
        )
        row = BG.published(native, 3)
        BG.stop(native)
        require(
            row["run_count"] == 2
            and row["task_kind"] == "build"
            and len(SEG.segments(native.call("head", "knn"))) == 1,
            "Accumulated F could not build a new first B",
        )
        recalls = []
        qualify(native, "new-B", recalls, 1)
        BG.finish(native)
        checks.append(
            "32/64-row F stays queryable below the native build minimum even with threshold 1; a later batch automatically builds a new first B"
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
        cases = [("capacity", capacity), ("healthy-file", healthy_file), ("reindex", reindex)]
        cases += [(f"rows-{n}", lambda d, r, c, n=n: boundary(d, r, n, c)) for n in [0, 1, 64]]
        cases += [(f"budget-{kind}", lambda d, r, c, k=kind: budget(d, r, k, c)) for kind in ["row", "byte"]]
        cases += [("fixed-" + k, lambda d, r, c, k=k: fixed_shape(d, r, k, c)) for k in ["partition", "schema"]]
        cases += [
            (kind, lambda d, r, c, k=kind: interruption(d, r, k, c))
            for kind in ["failure", "cancel", "ack", "input", "budget", "race", "corrupt", "collision"]
        ]
        cases += [
            ("crash-" + stage, lambda d, r, c, s=stage: crash(d, r, s, c))
            for stage in [
                "optimizer_before_rewrite",
                "rewrite_allocated",
                "rewrite_written",
                "rewrite_source_linked",
                "rewrite_after_stage",
                "rewrite_after_commit",
                "optimizer_rewritten",
            ]
        ]
        report["cases"] = []
        for name, run in cases:
            directory = root / name
            directory.mkdir()
            report["cases"].append({"name": name, **run(driver, directory, report["checks"])})
        require(not list(private.iterdir()), "Sparse F publication leaked private query leases")
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
