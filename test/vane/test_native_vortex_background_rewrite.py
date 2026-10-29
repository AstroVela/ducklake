#!/usr/bin/env python3
"""Qualify explicit background single-B rewrite, capacity, pipeline and recovery."""
from __future__ import annotations

import argparse
import importlib.util
import json
import os
from pathlib import Path
import time

SPEC = importlib.util.spec_from_file_location(
    "append", Path(__file__).with_name("test_native_vortex_rewrite_append.py")
)
assert SPEC is not None and SPEC.loader is not None
APP = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(APP)
RW, MA, M, SEG, CAT, BASE = APP.RW, APP.MA, APP.M, APP.SEG, APP.CAT, APP.BASE
OPT = RW.OPT
require = BASE.require


def configure(native, rows=32, percent=40, enabled=True):
    native.call("compaction-policy", str(enabled).lower(), rows, percent)


def stop(native):
    native.call("optimizer-cancel")
    return OPT.wait(native, lambda s: not s["enabled"] and not s["running"], "optimizer cancellation")


def pause(native, stage):
    native.call("optimizer-resume", stage, "pause")
    native.call("optimizer-wait")
    return OPT.status(native)


def published(native, revision):
    return OPT.wait(
        native, lambda s: s["published_revision"] == revision and not s["running"], "background publication"
    )


def finish(native):
    stop(native)
    MA.drain(native)
    native.call("maintenance-cancel")
    native.finish()


def visible_only(native, label):
    native.call("raw", label)
    oracle = native.rows(label)
    found = native.call("core-search")
    by_id = {row["id"]: row for row in oracle}
    for row in found["rows"]:
        require(
            row["id"] in by_id
            and all(row[key] == by_id[row["id"]][key] for key in ["embedding", "category", "payload"]),
            "A blocked compaction returned an invisible or incorrect row version",
        )
    exact = sorted(oracle, key=lambda row: sum((a - b) ** 2 for a, b in zip(row["embedding"], M.VECTOR)))[:10]
    return len({row["id"] for row in found["rows"]} & {row["id"] for row in exact}) / len(exact)


def exercise(proof, native, root, checks, workers):
    _, first, parts, frozen = RW.setup(native, root)
    native.sql("INSERT INTO lake.items VALUES(200000,[133.125,4,2,0,1,8,7,16],'A','old-T')")
    recalls = []
    M.qualify(native, "old", recalls, 3)
    old_rows = native.rows("old")
    native.call("capture", "held", "knn")
    worker = CAT.Worker(proof, "held", max_views=1)
    workers.append(worker)
    worker.open("held", native.capture("held"))
    baseline = worker.search("held", M.VECTOR)
    configure(native)
    runs = []
    for cycle, cutoff in enumerate([2148, 1124, 612, 356, 228]):
        if cycle:
            native.sql(f"DELETE FROM lake.items WHERE id>={cutoff} AND id<4096")
        selected = first if not cycle else RW.selected(native)
        current = pause(native, "rewrite_candidate_ready")
        require(
            current["state"] == "rewriting"
            and current["task_kind"] == "rewrite"
            and current["selected_generations"] == [SEG.generation(selected)],
            "Background planner did not durably select one B",
        )
        require(
            current["reclaimable_rows"] >= 32 and current["remaining_rows"] >= 65,
            "Planner selected an ineligible segment",
        )
        if not cycle:
            native.call("compaction-policy", "false", 32, 40, expected="Cancel the running optimizer task")
        native.call("sql", APP.append_sql(native, root, cycle))
        M.qualify(native, f"during-{cycle}", recalls, 3)
        native.call("optimizer-release")
        row = published(native, 4 + cycle)
        require(row["run_count"] == cycle + 1, "Worker retried or ran more than one rewrite")
        OPT.steady(native, row)
        stop(native)
        MA.drain(native)
        require(not RW.intents(native), "Registered rewrite workspaces did not converge")
        RW.unchanged(native, parts, frozen)
        M.qualify(native, f"after-{cycle}", recalls, 3)
        expected = RW.active_rows(native) + 1
        native.sql(
            f"SELECT CASE WHEN (SELECT count(*) FROM lake.items)={expected} AND "
            f"(SELECT count(DISTINCT rowid) FROM lake.items)={expected} THEN true ELSE error('background rewrite lost rows') END"
        )
        require(worker.search("held", M.VECTOR) == baseline, "Background rewrite changed a held private view")
        runs.append(
            {
                "run": row["run_id"],
                "selected": row["selected_generations"][0],
                "physical_rows": RW.active_rows(native),
            }
        )
    require(
        RW.active_rows(native) == 549,
        "Repeated background compaction lost appends or retained ended B vectors",
    )
    native.sql(
        "SELECT * FROM ducklake_expire_snapshots('lake',older_than=>now()); SELECT * FROM ducklake_cleanup_old_files('lake',cleanup_all=>true)"
    )
    require(
        all(not Path(file["uri"]).exists() for file in first["reference"]["snapshot"]["files"]),
        "Expired canonical source files survived explicit cleanup",
    )
    require(worker.search("held", M.VECTOR) == baseline, "Old private search depended on collected files")
    wanted = [next(row for row in old_rows if row["id"] == i) for i in [128, 200, 5000, 6000, 200000, 200]]
    require(
        worker.fetch("held", [CAT.MIXED.ref(row) for row in wanted])
        == [CAT.MIXED.READ.business(row) for row in wanted],
        "Delayed old-view fetch changed after background rewrite and GC",
    )
    worker.close_view("held")
    worker.finish(eof=True)
    finish(native)
    checks.extend(
        [
            "five explicitly enabled background rewrites select one B, accept concurrent T/F commits and preserve both unselected generations",
            "one worker converges registered workspaces/index garbage under unchanged quotas without manual rewrite calls",
            "current B/F/T counts, stable row IDs, search and typed rows match; old private search/fetch survive index and explicit source GC",
        ]
    )
    return {"fixture_recalls": recalls, "physical_rows": 549, "revision": 8, "runs": runs}


def continuous(driver, root, checks):
    native = CAT.Native(driver, root / "native")
    try:
        _, _, parts, frozen = RW.setup(native, root)
        configure(native)
        native.call("optimizer-resume")
        recalls = []
        for cycle, cutoff in enumerate([2148, 1124, 612, 356]):
            if cycle:
                native.sql(f"DELETE FROM lake.items WHERE id>={cutoff} AND id<4096")
            native.call("sql", APP.append_sql(native, root, cycle))
            row = published(native, 4 + cycle)
            require(
                row["run_count"] == cycle + 1 and row["enabled"],
                "Continuous policy stopped or repeated a task",
            )
            RW.unchanged(native, parts, frozen)
            M.qualify(native, f"continuous-{cycle}", recalls, 3)
            MA.drain(native)
        require(RW.active_rows(native) == 643, "Continuous operation lost appends or failed to reclaim rows")
        finish(native)
        checks.append(
            "one enable/resume drives four successive delete/append rounds without per-round cancel/resume or manual rewrite"
        )
        return {"fixture_recalls": recalls, "physical_rows": 643}
    finally:
        M.stop(native)


def thresholds(driver, root, checks):
    native = CAT.Native(driver, root / "native")
    try:
        RW.setup(native, root)
        native.call("optimizer-resume")
        row = OPT.wait(native, lambda s: s["compaction_reason"] == "disabled", "default opt-out")
        require(row["run_count"] == 0 and RW.active_rows(native) == 4352, "Source rewrite was implicitly enabled")
        OPT.steady(native, row)
        stop(native)
        for rows, percent in [(2049, 50), (1, 51)]:
            configure(native, rows, percent)
            native.call("optimizer-resume")
            row = OPT.wait(
                native,
                lambda s: s["compaction_reason"] == "below_threshold",
                "independent row/ratio thresholds",
            )
            require(
                row["reclaimable_rows"] == 2048 and row["remaining_rows"] == 2048 and row["run_count"] == 0,
                "Metadata-only selector miscounted deletes or ignored a threshold",
            )
            OPT.steady(native, row)
            stop(native)
        configure(native, 2048, 50)
        native.call("optimizer-resume")
        row = published(native, 4)
        require(
            row["run_count"] == 1 and RW.active_rows(native) == 2304,
            "Inclusive thresholds did not trigger one rewrite",
        )
        recalls = []
        M.qualify(native, "threshold-published", recalls, 3)
        finish(native)
        checks.append(
            "compaction defaults off, row and percentage thresholds both apply, and their exact boundary triggers one task"
        )
        return {"fixture_recalls": recalls}
    finally:
        M.stop(native)


def mixed_deletions(driver, root, checks):
    native = CAT.Native(driver, root / "native")
    try:
        _, _, parts, frozen = RW.setup(native, root)
        native.sql("DELETE FROM lake.items WHERE id=200; DELETE FROM lake.items WHERE id>=201 AND id<401")
        configure(native, 2250, 1)
        native.call("optimizer-resume")
        row = OPT.wait(native, lambda s: s["compaction_reason"] == "below_threshold", "mixed deletion accounting")
        require(
            row["reclaimable_rows"] == 2249 and row["remaining_rows"] == 1847 and row["run_count"] == 0,
            "File and inlined deletion metadata were double-counted",
        )
        stop(native)
        configure(native, 2249, 1)
        native.call("optimizer-resume")
        published(native, 4)
        require(RW.active_rows(native) == 2103, "Mixed deletion accounting did not match actual rewrite output")
        RW.unchanged(native, parts, frozen)
        recalls = []
        M.qualify(native, "mixed-deletions", recalls, 3)
        finish(native)
        checks.append(
            "metadata counts combine delete files and inlined deletions without duplicate rows; the exact threshold matches physical reclamation"
        )
        return {"fixture_recalls": recalls}
    finally:
        M.stop(native)


def selection(driver, root, checks):
    native = CAT.Native(driver, root / "native")
    try:
        _, first, parts, frozen = RW.setup(native, root)
        native.sql("DELETE FROM lake.items WHERE id>5000 AND id<=5032")
        configure(native, 32, 20)
        current = pause(native, "optimizer_rewritten")
        require(
            current["task_kind"] == "rewrite"
            and current["selected_generations"] == [SEG.generation(first)]
            and current["run_count"] == 1
            and current["published_revision"] == 4,
            "Planner did not choose the most reclaimable B first",
        )
        RW.unchanged(native, parts, frozen)
        retained = RW.selected(native)
        signature = SEG.signature(Path(retained["root"]) / SEG.generation(retained))
        native.call("optimizer-cancel")
        native.call("optimizer-release")
        OPT.wait(native, lambda s: not s["running"] and not s["enabled"], "cancel after first committed rewrite")
        native.call("optimizer-resume")
        row = published(native, 5)
        require(
            row["run_count"] == 2 and row["selected_generations"] == [SEG.generation(parts[0])],
            "Second B was not a separate task",
        )
        require(
            SEG.signature(Path(retained["root"]) / SEG.generation(retained)) == signature,
            "Second rewrite rebuilt the first B",
        )
        RW.unchanged(native, [parts[1]], {SEG.generation(parts[1]): frozen[SEG.generation(parts[1])]})
        recalls = []
        M.qualify(native, "selected-tasks", recalls, 3)
        finish(native)
        checks.append(
            "greatest reclaimable-row count selects the first B; another eligible B runs separately after explicit resume"
        )
        return {"fixture_recalls": recalls}
    finally:
        M.stop(native)


def blocked(driver, root, kind, checks):
    native = CAT.Native(driver, root / "native")
    try:
        artifacts, _, _, _ = RW.setup(native, root)
        if kind == "small":
            native.sql("DELETE FROM lake.items WHERE id<2084")
            reason = "below_native_minimum"
        elif kind == "empty":
            native.sql("CALL ducklake_set_option('lake','data_inlining_row_limit',100000,table_name=>'items')")
            native.sql("DELETE FROM lake.items WHERE id<4096")
            reason = "empty_rewrite_output"
        else:
            native.call("sql", APP.append_sql(native, root))
            native.call("build", "knn", artifacts / "mixed-schema.json", 3)
            native.sql("DELETE FROM lake.items WHERE id=10000")
            reason = "multiple_schema_groups"
        before = native.call("head", "knn")
        sources = native.call("source-status")
        configure(native, 1, 1)
        native.call("optimizer-resume")
        if kind == "groups":
            row = published(native, 5)
            require(
                row["run_count"] == 1 and RW.active_rows(native) == 2335,
                "A global schema-version change incorrectly split the unchanged source schema",
            )
            recalls = []
            M.qualify(native, "same-logical-schema", recalls, 1)
            finish(native)
            checks.append(
                "internal inlining/global schema changes do not create false compaction groups for unchanged table columns"
            )
            return {"fixture_recalls": recalls}
        row = OPT.wait(native, lambda s: s["compaction_reason"] == reason, "unsupported candidate classification")
        require(
            row["run_count"] == 0 and row["enabled"] and not row["running"],
            "Unsupported candidate became a failed/repeated native task",
        )
        OPT.steady(native, row)
        require(
            native.call("head", "knn") == before and native.call("source-status") == sources,
            "Skipped candidate changed coverage or source files",
        )
        observed = visible_only(native, "blocked")
        finish(native)
        checks.append(f"{reason} is inspectable and starts no task, retry, source downgrade or fallback")
        return {"informational_recall": observed}
    finally:
        M.stop(native)


def retirement_boundary(driver, root, checks):
    native = CAT.Native(driver, root / "native")
    try:
        RW.setup(native, root)
        before = native.call("head", "knn")
        native.sql("DELETE FROM lake.items WHERE id<4096")
        configure(native)
        native.call("optimizer-resume")
        deadline = time.monotonic() + 60
        while True:
            row = OPT.status(native)
            if row["state"] == "failed" and not row["running"]:
                break
            require(time.monotonic() < deadline, f"Retired-source boundary did not fail explicitly: {row}")
            time.sleep(0.02)
        require(
            not row["enabled"] and row["run_count"] == 0 and "explicit rebuild required" in row["error"],
            "Missing indexed coverage was silently accepted or scheduled",
        )
        require(native.call("head", "knn") == before, "Coverage failure rewrote the index plan")
        native.call("core-search", expected="absent")
        OPT.steady(native, row)
        finish(native)
        checks.append(
            "native full-file retirement remains an explicit missing-coverage error; neither planner nor query downgrades or falls back"
        )
        return {}
    finally:
        M.stop(native)


def interruption(driver, root, kind, checks):
    native = CAT.Native(driver, root / "native")
    try:
        _, _, parts, frozen = RW.setup(native, root)
        configure(native)
        if kind == "failure":
            native.call("optimizer-resume", "rewrite_bind_sealed", "fail")
        elif kind == "ack":
            native.call("optimizer-resume", "rewrite_after_commit", "fail")
        else:
            pause(native, "rewrite_candidate_ready")
            if kind == "conflict":
                native.sql(
                    "DELETE FROM lake.items WHERE id=200; INSERT INTO lake.items VALUES(200001,[133.03125,4,2,0,1,8,7,16],'A','acknowledged-T')"
                )
            else:
                native.call("optimizer-cancel")
            native.call("optimizer-release")
        deadline = time.monotonic() + 60
        while True:
            row = OPT.status(native)
            if not row["enabled"] and not row["running"]:
                break
            require(time.monotonic() < deadline, f"Interrupted rewrite did not stop: {row}")
            time.sleep(0.02)
        committed = kind == "ack"
        require(
            row["run_count"] == 1 and native.call("head", "knn")["revision"] == 3 + committed,
            "Interruption lost atomic publication or retried the task",
        )
        require(
            row["state"] == ("published" if committed else "cancelled" if kind == "cancel" else "failed"),
            "Interruption state does not distinguish committed work",
        )
        OPT.steady(native, row)
        MA.drain(native)
        RW.unchanged(native, parts, frozen)
        native.call("optimizer-resume")
        row = (
            published(native, 4)
            if not committed
            else OPT.wait(
                native,
                lambda s: s["compaction_reason"] == "no_deletions",
                "committed acknowledgement inspection",
            )
        )
        require(row["run_count"] == (1 if committed else 2), "Explicit resume replayed a committed task")
        recalls = []
        M.qualify(native, "resumed", recalls, 3)
        finish(native)
        checks.append(
            f"{kind} records the correct committed/failed state and stops the policy; only explicit resume permits fresh work"
        )
        return {"fixture_recalls": recalls}
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
        native.call("optimizer-enable", artifacts, 100000, 4194304, 100000, RW.LIMIT)
        stop(native)
        native.sql(
            "SELECT * FROM ducklake_set_vortex_write_budget('lake','main','items','knn',100000,4194304,100000,67108864);"
            "CALL ducklake_set_option('lake','data_inlining_row_limit',100,table_name=>'items');"
            "INSERT INTO lake.items SELECT 10000+i,[10000+i,0,0,0,0,0,0,0]::FLOAT[],'A','capacity-F' FROM range(95904)t(i);"
            "DELETE FROM lake.items WHERE id<3968"
        )
        insert = SEG.sql(
            native, root, "INSERT INTO lake.items VALUES(200000,[133,4,2,0,1,8,7,16],'B','capacity-restored')"
        )
        native.call("sql", insert, expected="physical and inline rows")
        configure(native)
        native.call("optimizer-resume")
        published(native, 2)
        require(RW.active_rows(native) == 96032, "Background rewrite did not release 3,968 physical row slots")
        native.call("sql", insert)
        native.call("maintenance-enable", 16, RW.LIMIT)
        MA.drain(native)
        recalls = []
        M.qualify(native, "capacity-restored", recalls, 1)
        finish(native)
        checks.append(
            "at the unchanged 100,000-row ceiling, background B rewrite retains the 95,904-row F, frees 3,968 slots and admits the next write"
        )
        return {"fixture_recalls": recalls}
    finally:
        M.stop(native)


def pipeline(driver, root, checks):
    native = CAT.Native(driver, root / "native")
    try:
        artifacts, _, _, _, _, parts = M.setup(native, root)
        native.call("optimizer-init")
        native.call("optimizer-enable", artifacts, 128, 4194304, 128, RW.LIMIT)
        stop(native)
        native.sql(
            "SELECT * FROM ducklake_set_vortex_write_budget('lake','main','items','knn',256,4194304,256,67108864);"
            "CALL ducklake_set_option('lake','data_inlining_row_limit',1000,table_name=>'items');"
            "DELETE FROM lake.items WHERE id<100 OR (id>=2148 AND id<4096);"
            "INSERT INTO lake.items SELECT 200000+i,[133+i/16.0,4,2,0,1,8,7,16],'A','pipeline-T' FROM range(128)t(i)"
        )
        frozen = {SEG.generation(part): SEG.signature(Path(part["root"]) / SEG.generation(part)) for part in parts}
        configure(native, 512, 40)
        first = pause(native, "optimizer_rewritten")
        require(
            first["task_kind"] == "rewrite" and first["run_count"] == 1 and first["published_revision"] == 4,
            "Eligible source compaction did not precede flush/index work",
        )
        RW.unchanged(native, parts, frozen)
        replacement = RW.selected(native)
        signature = SEG.signature(Path(replacement["root"]) / SEG.generation(replacement))
        native.call("optimizer-release")
        final = published(native, 6)
        require(final["run_count"] == 3, "Flush/merge/append did not converge as separate serialized tasks")
        require(
            SEG.signature(Path(replacement["root"]) / SEG.generation(replacement)) == signature,
            "Subsequent index work rebuilt the rewritten large B",
        )
        require(len(SEG.segments(native.call("head", "knn"))) == 3, "Pipeline expanded the B segment bound")
        OPT.steady(native, final)
        native.call("maintenance-enable", 16, RW.LIMIT)
        MA.drain(native)
        recalls = []
        M.qualify(native, "pipeline", recalls, 3)
        finish(native)
        checks.append(
            "one worker prioritizes source rewrite then completes T flush, selective index merge, F indexing and explicit maintenance without manual rewrite/flush/build"
        )
        return {"fixture_recalls": recalls}
    finally:
        M.stop(native)


def crash(driver, root, stage, checks):
    native = CAT.Native(driver, root / "native")
    try:
        _, _, parts, frozen = RW.setup(native, root)
        native.call("sql", APP.append_sql(native, root))
        configure(native)
        native.call("optimizer-resume", stage, "pause")
        before = (
            "rewriting"
            if stage in ["optimizer_before_rewrite", "rewrite_candidate_ready"]
            else "published" if stage == "optimizer_rewritten" else "publishing"
        )
        durable = OPT.crash(native, stage, before_state=before)
        committed = stage in ["rewrite_after_commit", "optimizer_rewritten"]
        require(
            native.call("head", "knn")["revision"] == 3 + committed,
            "Optimizer restart exposed partial source/index publication",
        )
        require(RW.active_rows(native) == (2336 if committed else 4384), "Crash lost acknowledged F appends")
        OPT.steady(native, durable)
        native.call("optimizer-resume")
        if committed:
            row = OPT.wait(
                native,
                lambda s: s["compaction_reason"] == "no_deletions" and not s["running"],
                "published-task reconciliation",
            )
        else:
            row = published(native, 4)
        require(
            row["run_count"] == (1 if committed else 2),
            "Resume replayed committed work or automatically reused an unfinished task",
        )
        native.call("maintenance-resume")
        MA.drain(native)
        RW.unchanged(native, parts, frozen)
        recalls = []
        M.qualify(native, "recovered", recalls, 3)
        finish(native)
        checks.append(
            f"SIGKILL at {stage} keeps acknowledged T/F data, starts no work on restart and reconciles only after explicit optimizer/maintenance resume"
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
            ("continuous", continuous),
            ("thresholds", thresholds),
            ("mixed-deletions", mixed_deletions),
            ("selection", selection),
            ("capacity", capacity),
            ("pipeline", pipeline),
            ("retirement-boundary", retirement_boundary),
        ]
        for kind in ["small", "empty", "groups"]:
            cases.append(
                (
                    "schema-grouping" if kind == "groups" else "blocked-" + kind,
                    lambda d, r, c, k=kind: blocked(d, r, k, c),
                )
            )
        for kind in ["conflict", "failure", "cancel", "ack"]:
            cases.append((kind, lambda d, r, c, k=kind: interruption(d, r, k, c)))
        for stage in [
            "optimizer_before_rewrite",
            "rewrite_candidate_ready",
            "rewrite_after_stage",
            "rewrite_after_commit",
            "optimizer_rewritten",
        ]:
            cases.append(("crash-" + stage, lambda d, r, c, s=stage: crash(d, r, s, c)))
        report["cases"] = []
        for name, run in cases:
            directory = root / name
            directory.mkdir()
            report["cases"].append({"name": name, **run(driver, directory, report["checks"])})
        require(not list(private.iterdir()), "Background rewrite proof leaked private leases")
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
