#!/usr/bin/env python3
"""Run one named vector query from an empty table through realtime writes and background ANN."""
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
BG, RW, M, SEG, CAT, BASE, OPT = S.BG, S.RW, S.M, S.SEG, S.CAT, S.BASE, S.OPT
require = BASE.require


def insert(native, start, rows):
    native.sql(
        f"INSERT INTO lake.items SELECT {start}+i,[128+i/16.0,((i*4)%12)::FLOAT,((i*5)%13)::FLOAT,0,1,8,7,16],"
        "CASE WHEN i%4=0 THEN 'B' ELSE 'A' END,CASE WHEN i=0 THEN NULL ELSE '实时-向量' END "
        f"FROM range({rows})t(i)"
    )


def exercise(proof, native, root, checks, workers):
    artifacts = root / "artifacts"
    artifacts.mkdir()
    native.sql(
        "DROP TABLE lake.items;CREATE TABLE lake.items(id BIGINT,embedding FLOAT[],category VARCHAR,payload VARCHAR);"
        "CALL ducklake_set_option('lake','experimental_data_file_format','vortex',table_name=>'items');"
        "CALL ducklake_set_option('lake','data_inlining_row_limit',1000,table_name=>'items')"
    )
    native.call("init")
    native.call("optimizer-init")
    native.sql(f"SELECT * FROM ducklake_create_vortex_index('lake','main','items','knn',{BASE.quote(artifacts)},8)")
    head = native.call("head", "knn")
    require(head["revision"] == 1 and not SEG.segments(head), "Empty definition fabricated an ANN generation")
    require(not native.call("build-status") and not native.call("generation-status"), "Definition invoked ANN build")
    recalls, phases = [], []

    def query(label, count):
        core, rows = M.qualify(native, label, recalls, count)
        phases.append({"phase": label, "rows": len(rows), "routes": core["diagnostics"]["routes"]})
        return core, rows

    query("empty", 0)
    native.call("optimizer-enable", artifacts, 64, 4194304, 128, RW.LIMIT, "build_started", "pause")
    native.sql("SELECT * FROM ducklake_set_vortex_write_budget('lake','main','items','knn',1000,4194304,512,67108864)")
    insert(native, 1000, 16)
    core, old_rows = query("committed-T", 0)
    require(
        core["diagnostics"]["routes"]["inline_rows"] == 16 and not core["diagnostics"]["routes"]["scan_files"],
        "First committed rows were not immediately queryable as T",
    )
    native.call("capture", "old", "knn")
    worker = CAT.Worker(proof, "held-T", max_views=1)
    workers.append(worker)
    worker.open("old", native.capture("old"))
    baseline = worker.search("old", M.VECTOR)
    native.sql(
        "BEGIN;UPDATE lake.items SET embedding=[133.001,4,2,0,1,8,7,16],payload='原子更新' WHERE id=1003;"
        "DELETE FROM lake.items WHERE id=1002;"
        "INSERT INTO lake.items VALUES(1020,[133.0005,4,2,0,1,8,7,16],'A','原子插入')"
    )
    _, hidden = query("uncommitted-hidden", 0)
    require(hidden == old_rows, "Uncommitted batch leaked into another reader")
    native.sql("COMMIT")
    _, committed = query("atomic-batch", 0)
    require(
        len(committed) == 16
        and all(r["id"] != 1002 for r in committed)
        and next(r for r in committed if r["id"] == 1003)["payload"] == "原子更新",
        "Committed batch did not atomically replace visible row versions",
    )
    native.sql("CREATE TABLE basic_expected AS SELECT rowid,id,embedding,category,payload FROM lake.items")
    native.sql("CALL ducklake_flush_inlined_data('lake',table_name=>'items')")
    core, flushed = query("flushed-small-F", 0)
    require(
        not core["diagnostics"]["routes"]["inline_rows"]
        and core["diagnostics"]["routes"]["scan_files"]
        and {r["id"]: CAT.MIXED.READ.business(r) for r in flushed}
        == {r["id"]: CAT.MIXED.READ.business(r) for r in committed}
        and not native.call("build-status"),
        "Small F required an ANN generation or changed visible rows/row IDs",
    )
    native.sql(
        "SELECT CASE WHEN count(*)=0 THEN true ELSE error('flush changed native rows or row IDs') END FROM "
        "((SELECT rowid,id,embedding,category,payload FROM lake.items EXCEPT ALL SELECT * FROM basic_expected) "
        "UNION ALL (SELECT * FROM basic_expected EXCEPT ALL SELECT rowid,id,embedding,category,payload FROM lake.items))"
    )
    insert(native, 2000, 112)
    native.call("optimizer-wait")
    state = OPT.status(native)
    require(state["running"] and state["expected_revision"] == 1, "Threshold did not start a background build")
    core, _ = query("F-during-first-build", 0)
    require(core["diagnostics"]["routes"]["scan_files"], "Building private B hid committed F")
    insert(native, 10000, 8)
    native.sql("CALL ducklake_flush_inlined_data('lake',table_name=>'items')")
    native.sql("INSERT INTO lake.items VALUES(200000,[133.0001,4,2,0,1,8,7,16],'A','并发-T')")
    query("concurrent-F-T", 0)
    native.call("optimizer-release")
    BG.published(native, 2)
    first = SEG.segments(native.call("head", "knn"))[0]
    frozen = SEG.signature(Path(first["root"]) / SEG.generation(first))
    core, _ = query("first-B-plus-F-T", 1)
    require(
        core["diagnostics"]["routes"]["indexed_files"]
        and core["diagnostics"]["routes"]["scan_files"]
        and core["diagnostics"]["routes"]["inline_rows"] == 1,
        "First ANN publication did not compose committed B/F/T",
    )
    native.sql(
        "BEGIN;UPDATE lake.items SET embedding=[800,4,2,0,1,8,7,16],category='B',payload='B后更新' WHERE id=1003;"
        "DELETE FROM lake.items WHERE id=1004;"
        "INSERT INTO lake.items VALUES(200001,[133.00002,4,2,0,1,8,7,16],'A','B后插入');COMMIT"
    )
    query("updates-beside-B", 1)
    insert(native, 3000, 128)
    BG.published(native, 3)
    SEG.unchanged(native, first, frozen)
    native.sql("INSERT INTO lake.items VALUES(400000,[133.00001,4,2,0,1,8,7,16],NULL,'最终-T')")
    _, before_restart = query("second-independent-B", 2)
    BG.stop(native)
    require(worker.search("old", M.VECTOR) == baseline, "Flush/index publication changed an old T view")
    wanted = [next(r for r in old_rows if r["id"] == i) for i in [1002, 1003, 1000, 1003]]
    require(
        worker.fetch("old", [CAT.MIXED.ref(r) for r in wanted]) == [CAT.MIXED.READ.business(r) for r in wanted],
        "Old T view lost original versions after flush and indexing",
    )
    wal = native.root / "metadata.ducklake.wal"
    require(wal.is_file() and wal.stat().st_size > 0, "Recovery did not exercise an uncheckpointed metadata WAL")
    native.process.kill()
    native.process.wait(timeout=10)
    native.log.close()
    require(native.process.returncode == -9, "Basic flow recovery did not use SIGKILL")
    native.start("resume")
    require(not OPT.status(native)["running"], "Restart implicitly started optimizer work")
    _, recovered = query("reopened-B-plus-T", 2)
    require(recovered == before_restart, "Recovery changed current typed rows or stable row IDs")
    require(worker.search("old", M.VECTOR) == baseline, "Native restart changed a retained T view")
    worker.close_view("old")
    worker.finish(eof=True)
    native.finish()
    checks.extend(
        [
            "empty table creates an explicit zero-B definition without native ANN artifacts",
            "one named query sees committed T immediately and hides uncommitted vector/business mutations",
            "small F is queryable before ANN; background threshold builds first B while F/T remain visible",
            "B/F/T queries filter stale/deleted row versions and match the native oracle",
            "a later threshold appends a second B without changing the first generation or artifact bytes",
            "old T views preserve original typed rows through updates, flush, indexing and native restart",
            "SIGKILL with a nonempty metadata WAL recovers identical B/T rows without implicit optimizer replay",
        ]
    )
    return {"phases": phases, "fixture_recalls": recalls, "minimum_recall": min(recalls)}


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
    native = CAT.Native(driver, root / "native")
    proof = BASE.Proof(args.duckdb.resolve(strict=True), args.probe.resolve(strict=True), root)
    workers = []
    report = {"status": "running", "driver_sha256": BASE.digest(driver), "checks": []}
    started = time.monotonic()
    try:
        report.update(exercise(proof, native, root, report["checks"], workers))
        require(not list(private.iterdir()), "Basic flow leaked private view leases")
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
