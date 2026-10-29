#!/usr/bin/env python3
"""Qualify atomic empty-file pruning within one B, retained sources and recovery."""
from __future__ import annotations

import argparse
import importlib.util
import json
import os
from pathlib import Path
import time

SPEC = importlib.util.spec_from_file_location(
    "sparse", Path(__file__).with_name("test_native_vortex_sparse_rewrite.py")
)
assert SPEC is not None and SPEC.loader is not None
S = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(S)
BG, RW, MA, M, SEG, CAT, BASE, OPT = S.BG, S.RW, S.MA, S.M, S.SEG, S.CAT, S.BASE, S.OPT
require = BASE.require


def setup(native, root, three=False, delete=True):
    artifacts, _, first, _ = SEG.setup(native, root)
    if three:
        native.sql("CALL ducklake_set_option('lake','data_inlining_row_limit',100,table_name=>'items')")
        for ordinal, start in enumerate([5000, 6000], 2):
            native.sql(
                f"INSERT INTO lake.items SELECT {start}+i,[128+i/16.0,((i*4)%12)::FLOAT,((i*5)%13)::FLOAT,0,1,8,7,16],"
                "'B','独立-B' FROM range(128)t(i)"
            )
            native.call("append", "knn", artifacts / f"B{ordinal}.json", ordinal - 1)
    S.initialize(native, root, first)
    if delete:
        native.sql("DELETE FROM lake.items WHERE id<4096 AND id%2=0")
    return first


def source_signature(file):
    path = Path(file["uri"])
    return [BASE.digest(path), path.stat().st_size, path.stat().st_mtime_ns]


def no_output(native):
    require(not RW.intents(native), "Pruning allocated a source-output intent")
    for row in native.call("build-status"):
        require(not Path(row["workspace"], "rewrite.vortex").exists(), "Pruning copied Vortex source data")


def exercise(proof, native, root, checks, workers):
    first = setup(native, root, three=True, delete=False)
    before = native.call("head", "knn")
    parts = SEG.segments(before)[1:]
    frozen = {SEG.generation(p): SEG.signature(Path(p["root"]) / SEG.generation(p)) for p in parts}
    sources = M.files([first])
    empty, healthy = sources
    retained_signature = source_signature(healthy)
    native.call("capture", "old", "knn")
    old_rows = native.rows("old")
    native.sql("CREATE TABLE original AS SELECT rowid,id,embedding,category,payload FROM lake.items")
    native.sql("SET VARIABLE original_snapshot=(SELECT max(snapshot_id) FROM lake.snapshots())")
    worker = CAT.Worker(proof, "held", max_views=2)
    workers.append(worker)
    worker.open("old", native.capture("old"))
    baseline = worker.search("old", M.VECTOR)
    native.sql("DELETE FROM lake.items WHERE id<4096 AND id%2=0")
    native.sql("CREATE TABLE expected AS SELECT rowid,id,embedding,category,payload FROM lake.items")
    row = BG.pause(native, "rewrite_candidate_ready")
    require(
        row["task_kind"] == "prune"
        and row["state"] == "rewriting"
        and row["selected_generations"] == [SEG.generation(first)]
        and row["remaining_rows"] == 2048
        and row["reclaimable_rows"] == 2048,
        "Planner did not durably select whole-file pruning",
    )
    no_output(native)
    native.call("sql", BG.APP.append_sql(native, root))
    native.sql("INSERT INTO expected SELECT rowid,id,embedding,category,payload FROM lake.items WHERE id>=10000")
    during_sources = native.call("source-status")
    require(native.call("head", "knn") == before, "Private pruning changed the ready plan")
    recalls = []
    M.qualify(native, "during-prune", recalls, 3)
    native.call("optimizer-release")
    row = BG.published(native, 4)
    BG.stop(native)
    head = native.call("head", "knn")
    ready = SEG.segments(head)
    require(
        head["reference"]["publication"] == row["run_id"]
        and SEG.generation(ready[-1]) == row["candidate_generation"]
        and ready[:2] == parts
        and M.files([ready[-1]]) == [healthy],
        "Pruning lost exact task ownership, retained coverage or an unselected B",
    )
    after_sources = native.call("source-status")
    require(
        {r["file_id"] for r in after_sources if r["end_snapshot"] is None}
        == {r["file_id"] for r in during_sources if r["end_snapshot"] is None} - {empty["id"]}
        and len(after_sources) == len(during_sources)
        and RW.active_rows(native) == 2336,
        "Pruning created a source file, retired a healthy file or lost concurrent F",
    )
    require(source_signature(healthy) == retained_signature, "Pruning changed healthy source bytes or mtime")
    RW.unchanged(native, parts, frozen)
    native.sql(
        "SELECT CASE WHEN count(*)=0 THEN true ELSE error('prune changed rows or row IDs') END FROM "
        "((SELECT rowid,id,embedding,category,payload FROM lake.items EXCEPT ALL SELECT * FROM expected) "
        "UNION ALL (SELECT * FROM expected EXCEPT ALL SELECT rowid,id,embedding,category,payload FROM lake.items))"
    )
    native.sql(
        "SELECT CASE WHEN count(*)=0 THEN true ELSE error('prune changed historical rows') END FROM "
        "((SELECT rowid,id,embedding,category,payload FROM lake.items AT(VERSION=>getvariable('original_snapshot')) "
        "EXCEPT ALL SELECT * FROM original) UNION ALL (SELECT * FROM original EXCEPT ALL "
        "SELECT rowid,id,embedding,category,payload FROM lake.items AT(VERSION=>getvariable('original_snapshot'))))"
    )
    core, _ = M.qualify(native, "after-prune", recalls, 3)
    require(
        len(core["diagnostics"]["routes"]["scan_files"]) == 1 and core["diagnostics"]["routes"]["inline_rows"] == 1,
        "New ReadView did not compose B/F/T",
    )
    native.call("capture", "new", "knn")
    worker.open("new", native.capture("new"))
    current = worker.search("new", M.VECTOR)
    MA.drain(native)
    require(not Path(first["root"], SEG.generation(first)).exists(), "Old B artifacts survived authorized GC")
    require(Path(empty["uri"]).is_file(), "Index GC removed a source still protected by native history")
    native.sql(
        "SELECT * FROM ducklake_expire_snapshots('lake',older_than=>now());"
        "SELECT * FROM ducklake_cleanup_old_files('lake',cleanup_all=>true)"
    )
    require(not Path(empty["uri"]).exists(), "Expired empty source survived native cleanup")
    require(source_signature(healthy) == retained_signature, "Cleanup collected a retained source")
    require(
        worker.search("old", M.VECTOR) == baseline and worker.search("new", M.VECTOR) == current,
        "GC changed an old/new private ReadView",
    )
    wanted = [next(r for r in old_rows if r["id"] == i) for i in [133, 2500, 5000, 6000, 2500]]
    require(
        worker.fetch("old", [CAT.MIXED.ref(r) for r in wanted]) == [CAT.MIXED.READ.business(r) for r in wanted],
        "Delayed fetch lost retired source rows after GC",
    )
    worker.close_view("old")
    worker.close_view("new")
    worker.finish(eof=True)
    no_output(native)
    BG.finish(native)
    checks.extend(
        [
            "one B drops only its fully masked member and rebuilds retained coverage; both unselected generations remain byte-identical",
            "concurrent T/flush/T commits survive atomic source/index publication without allocating a replacement data file",
            "current and historical row IDs/typed values, category filtering and B/F/T top-k match native visibility",
            "index GC preserves native historical sources; explicit source GC preserves healthy bytes and old/new views with delayed fetch",
        ]
    )
    return {"fixture_recalls": recalls, "released_rows": 2048}


def guards(driver, root, kind, checks):
    native = CAT.Native(driver, root / "native")
    try:
        first = setup(native, root, delete=False)
        if kind == "partial":
            native.sql("DELETE FROM lake.items WHERE id=2500")
            expected = "fully visible retained files"
        elif kind == "small":
            native.sql(
                "INSERT INTO lake.items SELECT 5000+i,[133+i/16.0,4,2,0,1,8,7,16],'A','健康小文件' FROM range(64)t(i)"
            )
            native.call("build", "knn", root / "artifacts/small.json", 1)
            first = SEG.segments(native.call("head", "knn"))[0]
            MA.drain(native)
            native.sql("DELETE FROM lake.items WHERE id<4096")
            expected = "at least 65 retained rows"
        elif kind == "empty":
            native.sql("DELETE FROM lake.items")
            expected = "at least 65 retained rows"
        else:
            expected = "empty member files"
        before, sources = native.call("head", "knn"), native.call("source-status")
        native.call(
            "prune", SEG.generation(first), root / "artifacts/rejected.json", before["revision"], expected=expected
        )
        require(
            native.call("head", "knn") == before
            and native.call("source-status") == sources
            and not native.call("build-status"),
            "Rejected prune allocated a build or changed native source/index state",
        )
        no_output(native)
        BG.finish(native)
        checks.append(f"explicit pruning rejects {kind} inputs before allocation")
        return {}
    finally:
        M.stop(native)


def interruption(driver, root, kind, checks):
    native = CAT.Native(driver, root / "native")
    try:
        first = setup(native, root)
        if kind in ["failure", "ack"]:
            native.call("optimizer-resume", "rewrite_after_commit" if kind == "ack" else "rewrite_after_stage", "fail")
        else:
            BG.pause(native, "rewrite_candidate_ready")
            if kind == "cancel":
                native.call("optimizer-cancel")
            elif kind == "input":
                native.sql("DELETE FROM lake.items WHERE id=141")
            elif kind == "race":
                native.call("prune", SEG.generation(first), root / "artifacts/other.json", 1)
            elif kind == "same-candidate":
                owner = native.call("build-status")
                require(len(owner) == 1, "Expected one sealed prune candidate")
                native.call("publish", "knn", owner[0]["reference_path"], 1)
            native.call("optimizer-release")
        row = S.ER.stopped(native)
        committed = kind in ["ack", "race"]
        published_index = committed or kind == "same-candidate"
        head = native.call("head", "knn")
        require(
            row["run_count"] == 1
            and row["state"] == ("published" if kind == "ack" else "cancelled" if kind == "cancel" else "failed")
            and head["revision"] == 1 + published_index
            and RW.active_rows(native) == (2048 if committed else 4096),
            "Interrupted prune split publication or attributed another publisher's work to itself",
        )
        if kind in ["race", "same-candidate"]:
            require(head["reference"]["publication"] != row["run_id"], "Task claimed another publication UUID")
        OPT.steady(native, row)
        no_output(native)
        MA.drain(native)
        recalls = []
        M.qualify(native, "interrupted", recalls, 1)
        if kind != "same-candidate":
            native.call("optimizer-resume")
            row = (
                OPT.wait(
                    native, lambda s: s["compaction_reason"] == "no_deletions" and not s["running"], "other publication"
                )
                if kind == "race"
                else BG.published(native, 2)
            )
            BG.stop(native)
            require(row["run_count"] == (1 if committed else 2), "Resume replayed a committed prune")
            require(RW.active_rows(native) == (2047 if kind == "input" else 2048), "Resume did not reclaim empty rows")
            M.qualify(native, "resumed", recalls, 1)
        BG.finish(native)
        checks.append(f"{kind} preserves source/index atomicity and exact task ownership; failure stops without retry")
        return {"fixture_recalls": recalls}
    finally:
        M.stop(native)


def crash(driver, root, stage, checks):
    native = CAT.Native(driver, root / "native")
    try:
        setup(native, root)
        native.call("maintenance-cancel")
        native.call("optimizer-resume", stage, "pause")
        state = "publishing" if stage in ["rewrite_after_stage", "rewrite_after_commit"] else "rewriting"
        if stage == "optimizer_rewritten":
            state = "published"
        durable = OPT.crash(native, stage, state)
        committed = stage in ["rewrite_after_commit", "optimizer_rewritten"]
        head = native.call("head", "knn")
        require(
            head["revision"] == 1 + committed and RW.active_rows(native) == (2048 if committed else 4096),
            "SIGKILL recovered a partial prune transaction",
        )
        if committed:
            require(
                head["reference"]["publication"] == durable["run_id"]
                and SEG.generation(SEG.segments(head)[0]) == durable["candidate_generation"],
                "Recovery lost the committed task's generation/publication pair",
            )
        OPT.steady(native, OPT.status(native))
        native.call("optimizer-resume")
        row = BG.published(native, 2)
        BG.stop(native)
        require(
            row["run_count"] == (1 if committed else 2), "Recovery replayed committed work or retried automatically"
        )
        native.call("maintenance-resume")
        recalls = []
        M.qualify(native, "recovered", recalls, 1)
        no_output(native)
        BG.finish(native)
        checks.append(f"SIGKILL at {stage} restores a complete old/new plan; only explicit resume permits fresh work")
        return {"fixture_recalls": recalls}
    finally:
        M.stop(native)


def recovered_candidate_race(driver, root, checks):
    native = CAT.Native(driver, root / "native")
    try:
        setup(native, root)
        native.call("maintenance-cancel")
        native.call("optimizer-resume", "rewrite_after_stage", "pause")
        durable = OPT.crash(native, "rewrite_after_stage")
        owner = native.call("build-status")
        require(len(owner) == 1, "Lost sealed candidate ownership after interrupted publication")
        native.call("publish", "knn", owner[0]["reference_path"], 1)
        head = native.call("head", "knn")
        require(
            head["revision"] == 2
            and SEG.generation(SEG.segments(head)[0]) == durable["candidate_generation"]
            and head["reference"]["publication"] != durable["run_id"]
            and RW.active_rows(native) == 4096,
            "Competing publisher did not reuse exactly the sealed generation without retiring source data",
        )
        native.call("optimizer-resume")
        row = OPT.wait(
            native,
            lambda s: s["compaction_reason"] == "no_deletions" and not s["running"],
            "recovered publication race",
        )
        require(
            row["run_id"] == durable["run_id"] and row["run_count"] == 1 and row["published_revision"] == 0,
            "Recovery attributed an index-only publication to the interrupted atomic prune task",
        )
        native.call("maintenance-resume")
        no_output(native)
        recalls = []
        M.qualify(native, "recovered-other-publisher", recalls, 1)
        BG.finish(native)
        checks.append(
            "durable publishing recovery requires the exact run UUID and generation; reusing the same sealed generation cannot claim source retirement"
        )
        return {"fixture_recalls": recalls}
    finally:
        M.stop(native)


def capacity(driver, root, checks):
    native = CAT.Native(driver, root / "native")
    try:
        setup(native, root)
        native.sql(
            "SELECT * FROM ducklake_set_vortex_write_budget('lake','main','items','knn',100000,4194304,100000,67108864);"
            "CALL ducklake_set_option('lake','data_inlining_row_limit',100,table_name=>'items');"
            "INSERT INTO lake.items SELECT 10000+i,[10000+i,0,0,0,0,0,0,0]::FLOAT[],'B','capacity-F' FROM range(95904)t(i)"
        )
        insert = SEG.sql(
            native, root, "INSERT INTO lake.items VALUES(200000,[133.0625,4,2,0,1,8,7,16],'A','capacity-restored')"
        )
        native.call("sql", insert, expected="physical and inline rows")
        native.call("optimizer-resume")
        BG.published(native, 2)
        BG.stop(native)
        require(RW.active_rows(native) == 97952, "Pruning did not release exactly 2,048 current physical/vector slots")
        native.call("sql", insert)
        recalls = []
        M.qualify(native, "capacity-restored", recalls, 1)
        no_output(native)
        BG.finish(native)
        checks.append("at the unchanged 100,000-row/vector ceiling, pruning releases 2,048 slots and admits a new T")
        return {"fixture_recalls": recalls, "released_rows": 2048}
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
        cases = [("capacity", capacity), ("recovered-candidate-race", recovered_candidate_race)]
        cases += [
            ("guard-" + k, lambda d, r, c, k=k: guards(d, r, k, c)) for k in ["healthy", "partial", "small", "empty"]
        ]
        cases += [
            (k, lambda d, r, c, k=k: interruption(d, r, k, c))
            for k in ["cancel", "failure", "ack", "input", "race", "same-candidate"]
        ]
        cases += [
            ("crash-" + s, lambda d, r, c, s=s: crash(d, r, s, c))
            for s in [
                "optimizer_before_rewrite",
                "rewrite_allocated",
                "rewrite_candidate_ready",
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
        require(not list(private.iterdir()), "Pruning leaked private query leases")
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
