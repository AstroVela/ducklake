#!/usr/bin/env python3
"""Qualify one-segment data rewrite, atomic ANN handoff, capacity and recovery."""
from __future__ import annotations

import argparse
import importlib.util
import json
import os
from pathlib import Path
import time

SPEC = importlib.util.spec_from_file_location(
    "maintenance", Path(__file__).with_name("test_native_vortex_maintenance.py")
)
assert SPEC is not None and SPEC.loader is not None
MA = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MA)
BG, GC, M, SEG, CAT, BASE, OPT = MA.BG, MA.GC, MA.M, MA.SEG, MA.CAT, MA.BASE, MA.OPT
require = BASE.require
LIMIT = 64 * 1024 * 1024


def active_rows(native):
    return sum(row["rows"] for row in native.call("source-status") if row["end_snapshot"] is None)


def intents(native):
    return native.call("rewrite-status")


def selected(native):
    return next(
        part
        for part in SEG.segments(native.call("head", "knn"))
        if part["reference"]["snapshot"]["files"][0]["row_count"] > 128
    )


def unchanged(native, parts, signatures):
    current = {SEG.generation(part): part for part in SEG.segments(native.call("head", "knn"))}
    for part in parts:
        generation = SEG.generation(part)
        require(current.get(generation) == part, "Rewrite changed an unselected B descriptor")
        require(
            SEG.signature(Path(part["root"]) / generation) == signatures[generation],
            "Rewrite rebuilt an unselected generation",
        )


def setup(native, root, maintenance=True):
    artifacts, _, rewritten, first, _, parts = M.setup(native, root)
    frozen = {SEG.generation(p): SEG.signature(Path(p["root"]) / SEG.generation(p)) for p in parts}
    native.call("optimizer-init")
    native.call("optimizer-enable", artifacts, 100000, 4194304, 100000, LIMIT)
    native.call("optimizer-cancel")
    native.sql(
        "SELECT * FROM ducklake_set_vortex_write_budget('lake','main','items','knn',100000,4194304,256,67108864)"
    )
    if maintenance:
        native.call("maintenance-enable", 16, LIMIT)
        MA.drain(native)
    native.sql("DELETE FROM lake.items WHERE id<100 OR (id>=2148 AND id<4096)")
    return rewritten, first, parts, frozen


def finish(native):
    native.call("maintenance-cancel")
    native.finish()


def exercise(proof, native, root, checks, workers):
    rewritten, first, parts, frozen = setup(native, root)
    native.sql(
        "UPDATE lake.items SET embedding=[133.01,4,2,0,1,8,7,16],payload=NULL WHERE id=200;"
        "INSERT INTO lake.items VALUES(200000,[133.0625,4,2,0,1,8,7,16],'A','rewrite-T')"
    )
    source_hashes = {f["uri"]: BASE.digest(Path(f["uri"])) for f in M.files([first, *parts])}
    recalls = []
    M.qualify(native, "old", recalls, 3)
    old_rows = native.rows("old")
    native.call("capture", "held", "knn")
    worker = CAT.Worker(proof, "held", max_views=1)
    workers.append(worker)
    worker.open("held", native.capture("held"))
    baseline = worker.search("held", M.VECTOR)
    unknown = rewritten / ".build-unregistered"
    unknown.mkdir()
    (unknown / "keep").write_bytes(b"outside rewrite ownership")
    revision = 3
    prior_rows = active_rows(native)
    old_generation = SEG.generation(first)
    for cycle, cutoff in enumerate([2148, 1124, 612, 356, 228]):
        if cycle:
            native.sql(f"DELETE FROM lake.items WHERE id>={cutoff} AND id<4096")
        current = first if cycle == 0 else selected(native)
        generation = SEG.generation(current)
        path = rewritten / f"rewrite-{cycle}.json"
        result = native.call("rewrite", generation, path, revision)
        revision += 1
        require(result["revision"] == revision, "Rewrite did not publish one successor revision")
        MA.drain(native)
        rows = active_rows(native)
        require(rows < prior_rows, "Delete rewrite did not reclaim current physical row capacity")
        prior_rows = rows
        require(not intents(native), "Published rewrite ownership accumulated after cleanup")
        unchanged(native, parts, frozen)
        M.qualify(native, f"cycle-{cycle}", recalls, 3)
        require(worker.search("held", M.VECTOR) == baseline, "Rewrite changed an already opened old view")
    require(
        prior_rows == 383 and len(GC.status(native)) == 3,
        "Repeated rewrite retained ended physical rows or generations",
    )
    require(not (Path(first["root"]) / old_generation).exists(), "Old ANN generation was not collected")
    require(
        all(BASE.digest(Path(path)) == digest for path, digest in source_hashes.items()),
        "Index rewrite changed retained source file bytes",
    )
    native.sql(
        "SELECT * FROM ducklake_expire_snapshots('lake',older_than=>now());"
        "SELECT * FROM ducklake_cleanup_old_files('lake',cleanup_all=>true)"
    )
    require(
        all(not Path(f["uri"]).exists() for f in first["reference"]["snapshot"]["files"]),
        "Expired source files were not collected",
    )
    require(worker.search("held", M.VECTOR) == baseline, "Old private source lease depended on deleted canonical files")
    wanted = [next(row for row in old_rows if row["id"] == i) for i in [128, 1000, 200, 5000, 6000, 200000, 1000]]
    require(
        worker.fetch("held", [CAT.MIXED.ref(row) for row in wanted])
        == [CAT.MIXED.READ.business(row) for row in wanted],
        "Delayed old-view typed fetch changed after source GC",
    )
    worker.close_view("held")
    worker.finish(eof=True)
    require((unknown / "keep").read_bytes() == b"outside rewrite ownership", "Rewrite GC scanned unknown directories")
    M.qualify(native, "after-source-gc", recalls, 3)
    limits = OPT.status(native)
    require(
        limits["max_inline_rows"] == 100000 and limits["max_unindexed_rows"] == 256, "Rewrite expanded write quotas"
    )
    finish(native)
    checks.extend(
        [
            "five selected-B rewrites reduce current physical rows from 4352 to 383 without rebuilding either unselected B",
            "current B/F/T queries retain visibility, distances and typed fetch while old private views survive both ANN and expired source-file GC",
            "published output ownership transfers atomically to native file/index registries; registered leftovers drain under unchanged quotas",
        ]
    )
    return {"fixture_recalls": recalls, "revision": revision, "physical_rows": prior_rows}


def failures(driver, root, checks):
    native = CAT.Native(driver, root / "native")
    try:
        rewritten, first, parts, frozen = setup(native, root)
        recalls = []
        stages = [
            "rewrite_allocated",
            "rewrite_before_copy",
            "rewrite_written",
            "generation_created",
            "generation_sealed",
            "reference_written",
            "rewrite_candidate_ready",
            "rewrite_source_linked",
            "rewrite_before_commit",
            "rewrite_after_stage",
        ]
        rows = active_rows(native)
        for op in ["rewrite-fail", "rewrite-cancel"]:
            for stage in stages:
                native.call(
                    op,
                    stage,
                    SEG.generation(first),
                    rewritten / f"{op}-{stage}.json",
                    3,
                    expected="Injected indexed rewrite failure" if op.endswith("fail") else "Interrupted",
                )
                require(
                    native.call("head", "knn")["revision"] == 3 and active_rows(native) == rows,
                    "Failed rewrite committed files or index independently",
                )
                MA.drain(native)
                require(not intents(native), "Failed rewrite left a source ownership record")
                unchanged(native, parts, frozen)
        for ordinal, op in enumerate(["rewrite-fail", "rewrite-cancel"]):
            current = first if ordinal == 0 else selected(native)
            if ordinal:
                native.sql("DELETE FROM lake.items WHERE id=2048")
            native.call(
                op,
                "rewrite_after_commit",
                SEG.generation(current),
                rewritten / f"{op}-ack.json",
                3 + ordinal,
                expected="committed but acknowledgement failed",
            )
            MA.drain(native)
            require(native.call("head", "knn")["revision"] == 4 + ordinal, "Lost ACK rolled back a committed rewrite")
            unchanged(native, parts, frozen)
        M.qualify(native, "after-errors", recalls, 3)
        finish(native)
        checks.append(
            "failure/cancellation at ten boundaries roll back source/index together; lost ACK after commit retains both published outputs"
        )
        return {"fixture_recalls": recalls}
    finally:
        M.stop(native)


def kill_at(native, stage, generation, reference, revision):
    marker = native.root / f"rewrite-{stage}.ready"
    native.send("rewrite-crash", stage, generation, reference, revision)
    deadline = time.monotonic() + 60
    while not marker.exists():
        require(native.process.poll() is None, "Rewrite owner exited before SIGKILL boundary")
        require(time.monotonic() < deadline, f"Rewrite crash checkpoint timed out: {stage}")
        time.sleep(0.01)
    MA.kill(native)


def crash(driver, root, stage, checks):
    native = CAT.Native(driver, root / "native")
    try:
        rewritten, first, parts, frozen = setup(native, root)
        before = active_rows(native)
        kill_at(native, stage, SEG.generation(first), rewritten / "crash.json", 3)
        committed = stage == "rewrite_after_commit"
        require(native.call("head", "knn")["revision"] == 3 + committed, "Crash exposed mixed source/index state")
        require(active_rows(native) == (2304 if committed else before), "Crash committed an incorrect source inventory")
        records = intents(native)
        require(
            len(records) == 1 and records[0]["keep_file"] == committed,
            "Publication did not commit source ownership atomically",
        )
        output = Path(records[0]["new_path"])
        before_records = BG.status(native)
        MA.steady(native)
        require(BG.status(native) == before_records, "Restart implicitly recovered or cleaned a rewrite")
        native.call("maintenance-resume")
        MA.drain(native)
        require(
            output.exists() == committed and not intents(native),
            "Explicit recovery deleted a winner or retained an abandoned managed output",
        )
        unchanged(native, parts, frozen)
        recalls = []
        M.qualify(native, "recovered", recalls, 3)
        finish(native)
        checks.append(
            f"SIGKILL at {stage} preserves one complete source/index state and explicit recovery keeps only committed output"
        )
        return {"fixture_recalls": recalls}
    finally:
        M.stop(native)


def race(driver, root, stage, kind, checks):
    native = CAT.Native(driver, root / "native")
    try:
        rewritten, first, parts, frozen = setup(native, root, maintenance=False)
        before = native.call("core-search")
        path = rewritten / "race.json"
        if kind == "reader":
            result = native.call("rewrite-reader-race", stage, SEG.generation(first), path, 3)
            require(
                result["held"]["rows"] == before["rows"] and result["held"]["diagnostics"]["index_revision"] == 3,
                "Old query admission mixed rewritten files and index versions",
            )
            require(result["gc"]["collected_generations"] == 1, "Reader/GC race did not collect the retired generation")
        elif kind == "orphan":
            result = native.call("rewrite-orphan-race", stage, SEG.generation(first), path, 3)
            require("published" in result, f"Orphan cleanup prevented rewrite publication: {result}")
            record = next(row for row in intents(native) if row["keep_file"])
            require(Path(record["new_path"]).exists(), "Parquet orphan cleanup touched a registered Vortex output")
        else:
            sql = SEG.sql(
                native, root, "INSERT INTO lake.items VALUES(200000,[133.01,4,2,0,1,8,7,16],'A','concurrent-write')"
            )
            result = native.call("rewrite-write-race", stage, SEG.generation(first), path, 3, sql)
            require(
                "changed before commit" in result.get("error", ""), f"Snapshot conflict was hidden or retried: {result}"
            )
            require(native.call("head", "knn")["revision"] == 3, "Conflicting rewrite changed the ready index")
        native.call("maintenance-enable", 16, LIMIT)
        MA.drain(native)
        unchanged(native, parts, frozen)
        recalls = []
        M.qualify(native, "after-race", recalls, 3)
        finish(native)
        checks.append(
            f"{kind} race at {stage} preserves atomic views, explicit conflict behavior and protected source ownership"
        )
        return {"fixture_recalls": recalls}
    finally:
        M.stop(native)


def collision(driver, root, checks):
    native = CAT.Native(driver, root / "native")
    try:
        rewritten, first, parts, frozen = setup(native, root, maintenance=False)
        native.call(
            "rewrite-collision",
            "rewrite_candidate_ready",
            SEG.generation(first),
            rewritten / "collision.json",
            3,
            expected="destination already exists",
        )
        records = intents(native)
        require(len(records) == 1 and records[0]["keep_file"], "Existing destination was claimed for collection")
        output = Path(records[0]["new_path"])
        require(native.call("head", "knn")["revision"] == 3, "Failed exclusive link published an index")
        native.call("maintenance-enable", 16, LIMIT)
        MA.drain(native)
        require(output.read_bytes() == b"foreign-rewrite-output", "Recovery deleted or changed an unowned file")
        unchanged(native, parts, frozen)
        recalls = []
        M.qualify(native, "collision", recalls, 3)
        finish(native)
        checks.append(
            "an existing destination fails the exclusive link and survives rollback and explicit GC unchanged"
        )
        return {"fixture_recalls": recalls}
    finally:
        M.stop(native)


def deletion_quality(driver, root, checks):
    native = CAT.Native(driver, root / "native")
    try:
        rewritten, first, parts, frozen = setup(native, root)
        native.sql("DELETE FROM lake.items WHERE id<2048")
        native.call("raw", "sparse")
        oracle = native.rows("sparse")
        before = native.call("core-search")
        distance = lambda row: sum((a - b) ** 2 for a, b in zip(row["embedding"], M.VECTOR))
        exact = sorted(oracle, key=distance)[:10]
        before_recall = len({r["id"] for r in before["rows"]} & {r["id"] for r in exact}) / len(exact)
        by_id = {row["id"]: row for row in oracle}
        for row in before["rows"]:
            require(
                row["id"] in by_id and all(row[k] == by_id[row["id"]][k] for k in ["embedding", "category", "payload"]),
                "Pre-rewrite ANN returned an invisible or incorrect row version",
            )
        native.call("rewrite", SEG.generation(first), rewritten / "quality.json", 3)
        MA.drain(native)
        recalls = []
        M.qualify(native, "compacted", recalls, 3)
        unchanged(native, parts, frozen)
        finish(native)
        checks.append(
            "a separate deletion-dense sample reports pre-rewrite static ANN recall and gates post-rewrite same-view recall without changing provider budgets"
        )
        return {"pre_rewrite_recall": before_recall, "fixture_recalls": recalls}
    finally:
        M.stop(native)


def capacity(driver, root, checks):
    native = CAT.Native(driver, root / "native")
    try:
        artifacts = root / "artifacts"
        artifacts.mkdir()
        native.call("init")
        native.call("build", "knn", artifacts / "original.json", 0)
        native.call("optimizer-init")
        native.call("optimizer-enable", artifacts, 100000, 4194304, 100000, LIMIT)
        native.call("optimizer-cancel")
        native.sql(
            "SELECT * FROM ducklake_set_vortex_write_budget('lake','main','items','knn',100000,4194304,100000,67108864);"
            "CALL ducklake_set_option('lake','data_inlining_row_limit',100,table_name=>'items');"
            "INSERT INTO lake.items SELECT 10000+i,[10000+i,0,0,0,0,0,0,0]::FLOAT[],'A','capacity-F' FROM range(95904)t(i);"
            "DELETE FROM lake.items WHERE id<3968"
        )
        insert = SEG.sql(
            native, root, "INSERT INTO lake.items VALUES(200000,[133,4,2,0,1,8,7,16],'B','after-compaction')"
        )
        native.call("sql", insert, expected="physical and inline rows")
        part = SEG.segments(native.call("head", "knn"))[0]
        native.call("rewrite", SEG.generation(part), artifacts / "compacted.json", 1)
        require(active_rows(native) == 96032, "Rewrite did not release exhausted physical capacity")
        native.call("sql", insert)
        native.call("maintenance-enable", 16, LIMIT)
        MA.drain(native)
        recalls = []
        M.qualify(native, "capacity-recovered", recalls, 1)
        finish(native)
        checks.append(
            "at the unchanged 100,000-row ceiling, deletion alone rejects growth; rewriting B releases 3,968 rows while F stays unchanged and new writes succeed"
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
        report["cases"] = []
        cases = [
            ("failures", failures),
            ("collision", collision),
            ("deletion-quality", deletion_quality),
            ("capacity", capacity),
        ]
        for stage in [
            "rewrite_allocated",
            "rewrite_written",
            "generation_sealed",
            "rewrite_source_linked",
            "rewrite_after_stage",
            "rewrite_after_commit",
        ]:
            cases.append((stage, lambda d, r, c, s=stage: crash(d, r, s, c)))
        for kind, stages in [
            ("reader", ["after_snapshot", "after_open"]),
            ("writer", ["rewrite_written", "rewrite_candidate_ready"]),
            ("orphan", ["rewrite_source_linked"]),
        ]:
            for stage in stages:
                cases.append((kind + "-" + stage, lambda d, r, c, s=stage, k=kind: race(d, r, s, k, c)))
        for name, run in cases:
            directory = root / name
            directory.mkdir()
            report["cases"].append({"name": name, **run(driver, directory, report["checks"])})
        require(not list(private.iterdir()), "Rewrite proof leaked private leases")
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
