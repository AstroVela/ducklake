#!/usr/bin/env python3
"""Qualify serialized background recovery and GC through continuous index maintenance."""
from __future__ import annotations

import argparse
import importlib.util
import json
import os
from pathlib import Path
import time

SPEC = importlib.util.spec_from_file_location("build_gc", Path(__file__).with_name("test_native_vortex_build_gc.py"))
assert SPEC is not None and SPEC.loader is not None
BG = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BG)
GC, M, SEG, CAT, BASE = BG.GC, BG.M, BG.SEG, BG.CAT, BG.BASE
OPT = M.OPT
require = BASE.require
LIMIT = 64 * 1024 * 1024


def status(native, catalog="lake"):
    rows = native.call("maintenance-status", catalog)
    require(len(rows) == 1, f"Expected a single catalog maintenance policy: {rows}")
    return rows[0]


def wait(native, predicate, label):
    deadline = time.monotonic() + 60
    while True:
        row = status(native)
        if predicate(row):
            return row
        require(row["state"] != "failed", f"Maintenance failed while waiting for {label}: {row}")
        require(time.monotonic() < deadline, f"Maintenance timed out waiting for {label}: {row}")
        time.sleep(0.02)


def drain(native):
    deadline = time.monotonic() + 60
    while True:
        row = status(native)
        require(row["state"] != "failed", f"Background cleanup failed: {row}")
        if not row["running"] and row["state"] == "idle" and not BG.status(native) and not GC.retired(native):
            row = status(native)
            if (
                not row["running"]
                and row["state"] == "idle"
                and not row["remaining_builds"]
                and not row["remaining_generations"]
            ):
                return row
        require(time.monotonic() < deadline, f"Background cleanup did not drain: {row}")
        time.sleep(0.02)


def steady(native):
    before = status(native)
    time.sleep(0.35)
    after = status(native)
    require(after["run_count"] == before["run_count"], "Maintenance retried a failed or completed task")
    return after


def stop(native):
    native.call("optimizer-cancel")
    native.call("maintenance-cancel")
    native.finish()


def exercise(proof, native, root, checks, workers):
    artifacts, _, _, first, frozen, parts = M.setup(native, root)
    native.call("optimizer-init")
    native.call("optimizer-enable", artifacts, 128, 4194304, 128, LIMIT)
    native.call("optimizer-cancel")
    native.sql("SELECT * FROM ducklake_set_vortex_write_budget('lake','main','items','knn',256,4194304,256,67108864)")
    sources = {str(Path(f["uri"])): BASE.digest(Path(f["uri"])) for f in M.files([first, *parts])}
    workspaces = [Path(row["workspace"]) for row in BG.status(native)]
    require(len(workspaces) == 3, "Initial builds did not register scratch ownership")
    unknown = artifacts / ".build-unregistered"
    unknown.mkdir()
    (unknown / "keep").write_bytes(b"outside registered maintenance")
    native.call("maintenance-enable", 1, LIMIT)
    initial = drain(native)
    require(
        initial["collected_builds"] == 3 and all(not p.exists() for p in workspaces),
        "Background scratch cleanup missed initial builds",
    )
    require(initial["max_objects"] == 1 and initial["max_bytes"] == LIMIT, "Maintenance changed its configured budget")
    steady(native)
    checks.append(
        "explicit catalog opt-in drains registered scratch and idle polling does not create new maintenance runs"
    )

    SEG.add_f(native, 7000)
    native.sql(
        "UPDATE lake.items SET embedding=[700000,0,0,0,0,0,0,0],payload=NULL WHERE id=133;"
        "DELETE FROM lake.items WHERE id=134;"
        "INSERT INTO lake.items VALUES(200000,[133.0625,4,2,0,1,8,7,16],'A','maintenance-T')"
    )
    recalls = []
    _, rows = M.qualify(native, "old", recalls, 3)
    native.call("capture", "held", "knn")
    worker = CAT.Worker(proof, "held", max_views=1)
    workers.append(worker)
    worker.open("held", native.capture("held"))
    baseline = worker.search("held", M.VECTOR)
    recalls.append(CAT.MIXED.qualify(baseline, rows, M.VECTOR))
    old_directories = [GC.directory(row) for row in GC.status(native) if row["generation"] != SEG.generation(first)]
    native.call("optimizer-resume", "optimizer_merged", "pause")
    native.call("optimizer-wait")
    require(
        status(native)["run_count"] == initial["run_count"] and not status(native)["running"],
        "GC overlapped an active optimizer callback",
    )
    require(len(GC.retired(native)) == 2 and BG.status(native), "Merge did not leave registered maintenance work")
    M.qualify(native, "during", recalls, 2)
    rejected = SEG.sql(
        native,
        root,
        "INSERT INTO lake.items SELECT 300000+i,[0,0,0,0,0,0,0,0]::FLOAT[],'B','quota' FROM range(129)t(i)",
    )
    native.call("sql", rejected, expected="unindexed rows")
    require(worker.search("held", M.VECTOR) == baseline, "Concurrent merge changed an old view")
    native.call("optimizer-release")
    OPT.wait(native, lambda s: s["published_revision"] == 5 and not s["running"], "first merge and append")
    drain(native)
    require(all(not p.exists() for p in old_directories), "Background GC retained retired canonical generations")
    checks.append(
        "one worker serializes merge/append and cleanup while B/F/T queries and an already opened old view stay usable"
    )

    revision = 5
    for cycle in range(5):
        SEG.add_f(native, 8000 + 1000 * cycle)
        native.sql(f"UPDATE lake.items SET payload='cycle-{cycle}' WHERE id=200000")
        revision += 2
        OPT.wait(native, lambda s: s["published_revision"] == revision and not s["running"], "continued merge/append")
        final = drain(native)
        require(
            len(GC.status(native)) == 3 and all(row["state"] == "active" for row in GC.status(native)),
            "Repeated maintenance accumulated retired registry entries",
        )
        SEG.unchanged(native, first, frozen)
        M.qualify(native, f"cycle-{cycle}", recalls, 3)
    native.sql("CALL ducklake_set_option('lake','data_inlining_row_limit',1000,table_name=>'items')")
    OPT.insert(native, 20000, 128)
    revision += 2
    OPT.wait(
        native, lambda s: s["published_revision"] == revision and not s["running"], "background T flush and promotion"
    )
    final = drain(native)
    M.qualify(native, "flushed", recalls, 3)
    require(
        final["collected_generations"] == 14 and final["collected_builds"] == 17,
        "Steady-state work was skipped or replayed",
    )
    limits = OPT.status(native)
    require(
        limits["max_inline_rows"] == 256
        and limits["max_unindexed_rows"] == 256
        and limits["max_inline_bytes"] == 4194304
        and limits["max_unindexed_bytes"] == LIMIT,
        "Cleanup changed write admission limits",
    )
    require(worker.search("held", M.VECTOR) == baseline, "Canonical GC broke an old generation lease")
    wanted = [next(row for row in rows if row["id"] == i) for i in [0, 133, 5001, 6000, 7000, 200000, 5001]]
    require(
        worker.fetch("held", [CAT.MIXED.ref(row) for row in wanted])
        == [CAT.MIXED.READ.business(row) for row in wanted],
        "Delayed old-view fetch changed",
    )
    worker.close_view("held")
    worker.finish(eof=True)
    require(
        all(BASE.digest(Path(path)) == digest for path, digest in sources.items()), "Maintenance rewrote source files"
    )
    require(
        (unknown / "keep").read_bytes() == b"outside registered maintenance",
        "Maintenance scanned an unregistered workspace",
    )
    checks.append(
        "seven merge/append/GC rounds including background T flush converge to three active generations, preserve B1/source bytes and retain write quotas"
    )
    stop(native)
    return {"fixture_recalls": recalls, "revision": revision, "maintenance": final}


def build_error(driver, root, cancel, checks):
    native = CAT.Native(driver, root / "native")
    try:
        artifacts, _, first, frozen = SEG.setup(native, root)
        native.call("optimizer-init")
        native.call("maintenance-enable", 16, LIMIT)
        drain(native)
        native.call(
            "optimizer-enable",
            artifacts,
            100000,
            4194304,
            128,
            LIMIT,
            "generation_created" if cancel else "before_build_publish",
            "pause" if cancel else "fail",
        )
        SEG.add_f(native, 5000)
        if cancel:
            native.call("optimizer-wait")
            require(not status(native)["running"], "Maintenance overlapped a native builder")
            native.call("optimizer-cancel")
            native.call("optimizer-release")
        failed = OPT.wait(
            native, lambda s: s["state"] == ("cancelled" if cancel else "failed") and not s["running"], "terminal build"
        )
        require(not failed["enabled"], "Failed build remained enabled")
        drain(native)
        OPT.steady(native, failed)
        SEG.unchanged(native, first, frozen)
        recalls = []
        M.qualify(native, "failed", recalls, 1)
        native.call("optimizer-resume")
        OPT.wait(native, lambda s: s["published_revision"] == 2 and not s["running"], "explicit build resume")
        drain(native)
        M.qualify(native, "resumed", recalls, 2)
        stop(native)
        checks.append(
            f"{'cancelled' if cancel else 'failed'} indexing stays stopped while independent registered cleanup completes; explicit resume builds fresh work"
        )
        return {"fixture_recalls": recalls}
    finally:
        M.stop(native)


def maintenance_error(driver, root, checks):
    native = CAT.Native(driver, root / "native")
    try:
        artifacts, _, merged, first, frozen, parts = M.setup(native, root)
        native.call("merge", merged / "replacement.json", 3, *M.selection(parts))
        native.call("optimizer-init")
        native.call("maintenance-enable", 16, LIMIT, "gc_file_deleted", "fail")
        failed = wait(native, lambda s: s["state"] == "failed" and not s["running"], "terminal maintenance failure")
        require(
            not failed["enabled"] and "Injected optimizer failure" in failed["error"], "Maintenance failure was hidden"
        )
        steady(native)
        require(any(row["state"] == "deleting" for row in GC.retired(native)), "Partial GC lost deletion ownership")
        recalls = []
        M.qualify(native, "failed", recalls, 2)
        native.call("maintenance-resume")
        drain(native)
        SEG.unchanged(native, first, frozen)
        M.qualify(native, "resumed", recalls, 2)
        native.call("maintenance-cancel")
        native.finish()
        checks.append(
            "GC failure is durable, stops maintenance without retries, and explicit resume finishes partial deletion while queries keep their active plan"
        )
        return {"fixture_recalls": recalls}
    finally:
        M.stop(native)


def budgets(driver, root, checks):
    native = CAT.Native(driver, root / "native")
    try:
        artifacts, _, _, _ = SEG.setup(native, root)
        native.call("optimizer-init")
        native.call("maintenance-enable", 1, LIMIT)
        before = drain(native)
        native.call("maintenance-cancel")
        native.call("build", "knn", artifacts / "replacement.json", 1)
        native.call("maintenance-resume", "maintenance_complete", "pause")
        native.call("optimizer-wait")
        during = status(native)
        require(
            during["run_count"] == before["run_count"] + 1
            and during["collected_builds"] == before["collected_builds"] + 1
            and during["collected_generations"] == before["collected_generations"]
            and len(GC.retired(native)) == 1,
            "Build and generation cleanup exceeded the count budget in one completed pass",
        )
        native.call("optimizer-release")
        drain(native)
        native.call("maintenance-cancel")
        row = BG.failed(native, artifacts / "oversized.json")
        large = Path(row["workspace"]) / "large.partial"
        with large.open("wb") as output:
            output.truncate(LIMIT + 1024 * 1024)
        native.call("maintenance-resume", "build_gc_truncated", "pause")
        native.call("optimizer-wait")
        require(
            large.stat().st_size == 1024 * 1024 and BG.candidate(native)["state"] == "deleting",
            "Background cleanup exceeded its byte budget",
        )
        native.call("maintenance-cancel")
        native.call("optimizer-release")
        wait(native, lambda s: s["state"] == "cancelled" and not s["running"], "maintenance cancellation")
        steady(native)
        native.call("maintenance-resume")
        drain(native)
        BG.gone(row)
        recalls = []
        M.qualify(native, "resumed", recalls, 1)
        native.call("maintenance-cancel")
        native.finish()
        checks.append(
            "one completed pass shares the object budget; cancellation of a 65 MiB partial file persists deletion and requires explicit resume"
        )
        return {"fixture_recalls": recalls}
    finally:
        M.stop(native)


def shared_bytes(driver, root, checks):
    native = CAT.Native(driver, root / "native")
    try:
        artifacts, _, _, _ = SEG.setup(native, root)
        native.call("build-gc", 16, LIMIT)
        native.call("build", "knn", artifacts / "replacement.json", 1)
        candidate = BG.candidate(native)
        workspace = Path(candidate["workspace"])
        (workspace / "budget.partial").write_bytes(b"p")
        scratch = sum(path.stat().st_size for path in workspace.rglob("*") if path.is_file())
        pending = GC.retired(native)
        require(len(pending) == 1 and 0 < scratch < pending[0]["artifact_bytes"], "Invalid shared byte fixture")
        budget = pending[0]["artifact_bytes"]
        native.call("optimizer-init")
        native.call("maintenance-enable", 16, budget, "maintenance_complete", "pause")
        native.call("optimizer-wait")
        first = status(native)
        require(
            first["run_count"] == 1
            and first["collected_builds"] == 1
            and first["collected_generations"] == 0
            and first["collected_bytes"] == scratch
            and GC.retired(native) == pending,
            "Both collectors did not share the byte budget in one completed pass",
        )
        native.call("optimizer-release")
        final = drain(native)
        require(
            final["run_count"] == 2
            and final["collected_generations"] == 1
            and final["collected_bytes"] == scratch + budget,
            "The generation did not fit in a fresh pass",
        )
        BG.gone(candidate)
        native.call("maintenance-cancel")
        native.finish()
        checks.append("one completed pass shares bytes across both collectors and the next pass can use the full limit")
        return {"budget": budget, "first_pass_bytes": first["collected_bytes"], "total_bytes": final["collected_bytes"]}
    finally:
        M.stop(native)


def blocked_budget(driver, root, checks):
    native = CAT.Native(driver, root / "native")
    try:
        artifacts, _, _, first, _, _ = M.setup(native, root)
        native.call("build", "knn", artifacts / "replacement.json", 3)
        native.call("build-gc", 16, LIMIT)
        pending = GC.retired(native)
        require(len(pending) == 3 and pending[0]["generation"] == M.generation(first), "Invalid GC ordering fixture")
        budget = max(row["artifact_bytes"] for row in pending[1:])
        require(pending[0]["artifact_bytes"] > budget > 0, "Expected the first generation to exceed the budget")
        native.call("optimizer-init")
        native.call("maintenance-enable", 16, budget)
        blocked = wait(native, lambda row: row["state"] == "blocked" and not row["running"], "blocked generation")
        require(
            blocked["remaining_generations"] == 1
            and blocked["collected_generations"] == 2
            and GC.retired(native) == pending[:1]
            and "byte budget" in blocked["error"],
            "An oversized first generation hid eligible later entries",
        )
        steady(native)
        manual = native.call("gc", 1, LIMIT)
        require(manual["collected_generations"] == 1, "Explicit collection could not clear the blocked generation")
        final = drain(native)
        require(final["run_count"] == blocked["run_count"] and not final["error"], "Blocked backlog caused a dry run")
        native.call("maintenance-cancel")
        native.finish()
        checks.append(
            "eligible generations advance past oversized entries and a wholly blocked backlog creates no runs"
        )
        return {"budget": budget, "blocked_artifact_bytes": pending[0]["artifact_bytes"], "runs": final["run_count"]}
    finally:
        M.stop(native)


def attachment_binding(driver, root, checks):
    native = CAT.Native(driver, root / "native")
    try:
        SEG.setup(native, root)
        build = BG.candidate(native)
        native.call("optimizer-init")
        native.call("maintenance-enable", 16, LIMIT, "build_gc_waiting", "pause")
        native.call("optimizer-wait")
        original = status(native)
        require(original["running"] and original["state"] == "collecting_builds", "GC did not reach its lock boundary")
        native.call("maintenance-reattach")
        require(not status(native)["running"], "The new attachment inherited the old running registration")
        native.call("maintenance-resume", "maintenance_before_recovery", "pause")
        native.call("optimizer-wait")
        resumed = status(native)
        require(
            resumed["run_count"] == original["run_count"] + 1
            and BG.candidate(native) == build
            and Path(build["workspace"]).is_dir(),
            "The previous attachment's collector reached the newly attached workspace",
        )
        native.call("optimizer-release")
        final = drain(native)
        require(final["collected_builds"] == original["collected_builds"] + 1, "Explicit resume did not collect")
        BG.gone(build)
        native.call("maintenance-cancel")
        native.finish()
        checks.append("reattaching a lake while GC waits requires a new explicit registration before cleanup")
        return {"old_run": original["run_id"], "new_run": resumed["run_id"]}
    finally:
        M.stop(native)


def control_ordering(driver, root, checks):
    native = CAT.Native(driver, root / "native")
    try:
        artifacts, _, _, _ = SEG.setup(native, root)
        native.call("optimizer-init")
        native.call("maintenance-enable", 16, LIMIT, "maintenance_after_generation_gc", "pause")
        native.call("optimizer-wait")
        active = status(native)
        require(active["running"] and status(native, "LAKE") == active, "Catalog case changed task identity")
        native.call("maintenance-resume", "LAKE", expected="maintenance is still running")
        native.call("maintenance-cancel", "LAKE")
        require(status(native)["state"] == "cancelling", "Cancellation lost the active completion boundary")
        native.call("optimizer-release")
        cancelled = wait(
            native, lambda row: row["state"] == "cancelled" and not row["running"], "terminal cancellation"
        )
        require(
            cancelled["run_count"] == active["run_count"] and not cancelled["enabled"], "Cancellation was not durable"
        )

        BG.failed(native, artifacts / "before-completion.json")
        native.call("maintenance-resume", "maintenance_complete", "fail-after-release")
        native.call("optimizer-wait")
        complete = status(native)
        require(complete["state"] == "idle" and not complete["running"], "Completed run still appeared active")
        native.call("maintenance-cancel", "LAKE")
        require(status(native)["state"] == "cancelled", "Completed run left a pending cancellation")
        native.call("maintenance-resume", "LAKE")
        build = BG.failed(native, artifacts / "after-resume.json")
        native.call("optimizer-release")
        final = drain(native)
        require(
            final["enabled"] and not final["error"] and final["run_count"] == complete["run_count"] + 1,
            "An old completion error overwrote the resumed policy",
        )
        BG.gone(build)
        native.call("maintenance-cancel")
        native.finish()
        checks.append(
            "maintenance controls serialize cancellation and completion across spellings and fence old callbacks"
        )
        return {"cancelled_run": cancelled["run_id"], "resumed_run": final["run_id"]}
    finally:
        M.stop(native)


def kill(native):
    native.process.kill()
    native.process.wait(timeout=10)
    native.log.close()
    require(native.process.returncode == -9, "Owner was not stopped with SIGKILL")
    native.start("resume")


def publication_crash(driver, root, stage, checks):
    native = CAT.Native(driver, root / "native")
    try:
        artifacts, _, _, first, frozen, _ = M.setup(native, root)
        native.call("optimizer-init")
        native.call("maintenance-enable", 16, LIMIT)
        drain(native)
        native.call("optimizer-enable", artifacts, 100000, 4194304, 128, LIMIT, stage, "pause")
        SEG.add_f(native, 7000)
        native.call("optimizer-wait")
        candidate = OPT.status(native)["candidate_generation"]
        require(candidate, "Publication did not persist its candidate identity")
        kill(native)
        committed = stage == "after_commit"
        require(
            native.call("head", "knn")["revision"] == 3 + committed and not status(native)["running"],
            "Restart replayed maintenance or changed publication",
        )
        native.call("maintenance-resume")
        drain(native)
        require(
            (artifacts / candidate).exists() == committed,
            "Recovery collected a committed winner or kept an abandoned candidate",
        )
        native.call("optimizer-resume")
        OPT.wait(native, lambda s: s["published_revision"] == 5 and not s["running"], "recovered indexing")
        drain(native)
        SEG.unchanged(native, first, frozen)
        recalls = []
        M.qualify(native, "recovered", recalls, 3)
        stop(native)
        checks.append(
            f"SIGKILL {stage} requires explicit maintenance/index resume, reconciles committed ownership first and never rebuilds a winning merge"
        )
        return {"fixture_recalls": recalls}
    finally:
        M.stop(native)


def gc_crash(driver, root, stage, checks):
    native = CAT.Native(driver, root / "native")
    try:
        artifacts, _, merged, first, frozen, parts = M.setup(native, root)
        native.call("merge", merged / "replacement.json", 3, *M.selection(parts))
        native.call(
            "fail-build", "generation_created", merged / "abandoned.json", 4, expected="Injected native build failure"
        )
        abandoned = next(row for row in BG.status(native) if row["state"] == "abandoned")
        pending = Path(abandoned["workspace"]) / "partial"
        with pending.open("wb") as output:
            output.truncate(LIMIT + 1024 * 1024 if stage == "build_gc_truncated" else 32)
        native.call("optimizer-init")
        native.call("maintenance-enable", 16, LIMIT, stage, "pause")
        native.call("optimizer-wait")
        durable = status(native)
        kill(native)
        reopened = status(native)
        require(
            not reopened["running"] and reopened["run_count"] == durable["run_count"], "Restart implicitly resumed GC"
        )
        steady(native)
        if stage == "maintenance_after_generation_gc":
            require(
                (reopened["remaining_builds"] or reopened["remaining_generations"])
                and not BG.status(native)
                and not GC.retired(native),
                "The stopped run did not leave stale backlog counts",
            )
            native.call("optimizer-enable", merged, 100000, 4194304, 128, LIMIT, "optimizer_before_build", "pause")
            SEG.add_f(native, 8000)
            native.call("optimizer-wait")
        native.call("maintenance-resume")
        if stage == "maintenance_after_generation_gc":
            resumed = status(native)
            require(
                resumed["state"] == "idle"
                and not resumed["running"]
                and resumed["run_count"] == reopened["run_count"]
                and not resumed["remaining_builds"]
                and not resumed["remaining_generations"],
                "Explicit resume did not refresh stale counts while the worker was occupied",
            )
            native.call("optimizer-cancel")
            native.call("optimizer-release")
            checks.append("explicit resume refreshes stale backlog with the worker occupied and creates no extra run")
        recovered = drain(native)
        require(
            not recovered["remaining_builds"] and not recovered["remaining_generations"],
            "Explicit resume retained stale backlog after completed collection",
        )
        BG.gone(abandoned)
        SEG.unchanged(native, first, frozen)
        recalls = []
        M.qualify(native, "recovered", recalls, 2)
        steady(native)
        native.call("maintenance-cancel")
        native.finish()
        checks.append(
            f"SIGKILL at {stage} preserves durable ownership and resumes cleanup explicitly, including partial and already missing files"
        )
        return {"fixture_recalls": recalls, "durable_state": durable["state"]}
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
            ("failed-build", lambda d, r, c: build_error(d, r, False, c)),
            ("cancelled-build", lambda d, r, c: build_error(d, r, True, c)),
            ("maintenance-failure", maintenance_error),
            ("budgets", budgets),
            ("shared-bytes", shared_bytes),
            ("blocked-budget", blocked_budget),
            ("attachment-binding", attachment_binding),
            ("control-ordering", control_ordering),
        ]
        for stage in ["before_commit", "after_commit"]:
            cases.append((stage, lambda d, r, c, s=stage: publication_crash(d, r, s, c)))
        for stage in [
            "maintenance_before_recovery",
            "build_gc_deleting",
            "build_gc_file_deleted",
            "build_gc_truncated",
            "gc_file_deleted",
            "maintenance_after_generation_gc",
            "maintenance_complete",
        ]:
            cases.append((stage, lambda d, r, c, s=stage: gc_crash(d, r, s, c)))
        for name, run in cases:
            directory = root / name
            directory.mkdir()
            report["cases"].append({"name": name, **run(driver, directory, report["checks"])})
        require(not list(private.iterdir()), "Maintenance proof leaked private leases")
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
