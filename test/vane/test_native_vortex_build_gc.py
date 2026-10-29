#!/usr/bin/env python3
"""Qualify registered native build recovery and bounded workspace/candidate cleanup."""
from __future__ import annotations

import argparse
import importlib.util
import json
import os
from pathlib import Path
import time

SPEC = importlib.util.spec_from_file_location(
    "generation_gc", Path(__file__).with_name("test_native_vortex_generation_gc.py")
)
assert SPEC is not None and SPEC.loader is not None
GC = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(GC)
M, SEG, CAT, BASE = GC.M, GC.SEG, GC.CAT, GC.BASE
require = BASE.require
LIMIT = 64 * 1024 * 1024


def status(native):
    return native.call("build-status")


def candidate(native):
    rows = status(native)
    require(len(rows) == 1, f"Expected one registered build: {rows}")
    row = rows[0]
    require(
        row["generation"] == "generation-" + row["build_id"]
        and Path(row["workspace"]) == Path(row["index_root"]) / (".build-" + row["build_id"]),
        "Registration did not allocate the workspace and generation together",
    )
    return row


def gone(row):
    require(not Path(row["workspace"]).exists(), "Completed collection retained a registered workspace")
    if not row["keep_generation"]:
        require(not GC.directory(row).exists(), "Completed collection retained an abandoned generation")


def collect(native, count=16, budget=LIMIT):
    return native.call("build-gc", count, budget)


def kill_at(native, operation, stage, *args):
    prefix = "build-gc-" if operation == "build-gc-crash" else "build-"
    marker = native.root / f"{prefix}{stage}.ready"
    marker.unlink(missing_ok=True)
    native.send(operation, stage, *args)
    deadline = time.monotonic() + 60
    while not marker.exists():
        require(native.process.poll() is None, "Owner exited before SIGKILL checkpoint")
        require(time.monotonic() < deadline, f"SIGKILL checkpoint timed out: {stage}")
        time.sleep(0.01)
    native.process.kill()
    native.process.wait(timeout=10)
    native.log.close()
    require(native.process.returncode == -9, "Owner was not stopped with SIGKILL")
    native.start("resume")


def failed(native, reference, stage="scratch_created"):
    revision = native.call("head", "knn")["revision"]
    native.call("fail-build", stage, reference, revision, expected="Injected native build failure")
    row = candidate(native)
    require(row["state"] == "abandoned" and not row["keep_generation"], "Failed build was not abandoned")
    return row


def qualify(native, label, recalls):
    native.call("raw", label)
    rows = native.rows(label)
    core = native.call("core-search")
    recalls.append(SEG.SEARCH.qualify(core, rows, SEG.VECTOR))
    result = SEG.SEARCH.query(native, SEG.SEARCH.search_sql(SEG.VECTOR))
    recalls.append(SEG.SEARCH.qualify(result, rows, SEG.VECTOR))
    require(result["rows"] == core["rows"], "SQL and native search diverged after build collection")


def exercise(proof, native, root, checks, workers):
    artifacts, _, first, frozen = SEG.setup(native, root)
    SEG.add_f(native, 5000)
    native.call("raw", "initial")
    sources = {
        str(Path(file["uri"])): BASE.digest(Path(file["uri"]))
        for file in CAT.descriptor(native.capture("initial"))["capture"]["file_capture"]["files"]
    }
    native.call("capture", "held", "knn")
    worker = CAT.Worker(proof, "held", max_views=2)
    workers.append(worker)
    worker.open("held", native.capture("held"))
    held_rows = native.rows("held")
    baseline = worker.search("held", SEG.VECTOR)
    recalls = [CAT.MIXED.qualify(baseline, held_rows, SEG.VECTOR)]
    initial = candidate(native)
    require(initial["state"] == "published" and initial["keep_generation"], "Publication did not commit ownership")
    require(collect(native)["collected_builds"] == 1, "Published scratch was not collected")
    gone(initial)
    require(GC.directory(initial).exists(), "Scratch collection deleted an active generation")
    unknown = artifacts / ".build-unregistered"
    unknown.mkdir()
    (unknown / "keep").write_bytes(b"unregistered")
    unknown_generation = artifacts / "generation-unregistered"
    unknown_generation.mkdir()
    (unknown_generation / "keep").write_bytes(b"unregistered generation")
    checks.append("publication atomically keeps its generation; registered scratch alone is collectible")

    stages = [
        "build_allocated",
        "build_workspace_created",
        "source_pinned",
        "scratch_created",
        "build_started",
        "generation_created",
        "build_complete",
        "generation_sealed",
        "reference_written",
        "reference_ready",
        "before_commit",
    ]
    for operation in ["fail-append", "cancel-append"]:
        for stage in stages:
            reference = artifacts / f"{operation}-{stage}.json"
            native.call(
                operation,
                stage,
                reference,
                1,
                expected="Interrupted" if operation.startswith("cancel") else "Injected native build failure",
            )
            row = candidate(native)
            require(row["state"] == "abandoned" and not row["keep_generation"], "Error retained live build ownership")
            present = reference.read_bytes() if reference.exists() else None
            require(collect(native)["collected_builds"] == 1, "Failed/cancelled build did not clean up")
            gone(row)
            require(not status(native), "Repeated failures accumulated ownership records")
            require(
                (reference.read_bytes() if reference.exists() else None) == present, "GC changed caller reference JSON"
            )
            SEG.unchanged(native, first, frozen)
    checks.append(
        "failure and cancellation at eleven allocation/build/seal/publication boundaries reclaim registered leftovers"
    )

    lost_ack = artifacts / "lost-ack.json"
    native.call("fail-append", "after_commit", lost_ack, 1, expected="committed but acknowledgement failed")
    row = candidate(native)
    require(row["state"] == "published" and row["keep_generation"], "Lost ACK abandoned a committed winner")
    require(native.call("head", "knn")["revision"] == 2, "Lost ACK rolled back a committed index")
    collect(native)
    gone(row)
    require(GC.directory(row).exists(), "Lost ACK winner was deleted")
    SEG.add_f(native, 6000)
    checks.append("lost publication ACK reconciles against committed ownership and keeps the winning generation")

    for stage in [s for s in stages if s != "build_started"] + ["after_commit"]:
        reference = artifacts / f"crash-{stage}.json"
        kill_at(native, "crash-append", stage, reference, 2)
        row = candidate(native)
        committed = stage == "after_commit"
        expected_state = (
            "published"
            if committed
            else (
                "sealed"
                if stage in ["generation_sealed", "reference_written", "reference_ready", "before_commit"]
                else ("allocated" if stage in ["build_allocated", "build_workspace_created"] else "building")
            )
        )
        require(row["state"] == expected_state, "Crash checkpoint did not persist build ownership")
        if stage == "reference_written":
            require(
                not reference.exists()
                and any(
                    p.is_file() and row["generation"].encode() in p.read_bytes()
                    for p in Path(row["workspace"]).iterdir()
                ),
                "Reference temporary file escaped the registered workspace",
            )
        require(native.call("head", "knn")["revision"] == (3 if committed else 2), "Crash recovered wrong revision")
        require(
            collect(native)["collected_builds"] == int(committed), "Collector inferred abandonment without recovery"
        )
        if not committed:
            require(native.call("recover-builds", 1) == 1, "Explicit recovery did not reconcile a stopped build")
            row = candidate(native)
            require(
                row["state"] == "abandoned" and not row["keep_generation"], "Recovery retained an unpublished candidate"
            )
            require(collect(native)["collected_builds"] == 1, "Recovered build did not become collectible")
        else:
            require(native.call("recover-builds", 16) == 0, "Recovery replayed an acknowledged publication")
        gone(row)
        if committed:
            require(GC.directory(row).exists(), "Restart cleanup deleted committed output")
        require(not status(native), "Restart/cleanup retained a completed task")
        SEG.unchanged(native, first, frozen)
    checks.append(
        "SIGKILL at eleven boundaries, including reference staging, pre-seal and publication commit, requires explicit bounded recovery"
    )

    row = failed(native, artifacts / "bounded.json")
    scratch = Path(row["workspace"]) / "scratch"
    shared_source = Path(next(iter(sources)))
    os.link(shared_source, scratch / "shared-source")
    large = scratch / "large.partial"
    with large.open("wb") as output:
        output.truncate(LIMIT + 1024 * 1024)
    result = collect(native)
    require(
        result == {"collected_builds": 0, "collected_bytes": LIMIT, "remaining_builds": 1},
        "Oversized scratch escaped the byte budget",
    )
    require(
        large.stat().st_size == 1024 * 1024 and candidate(native)["state"] == "deleting",
        "Partial cleanup was not durable",
    )
    result = collect(native)
    require(
        result["collected_builds"] == 1 and result["collected_bytes"] == 1024 * 1024,
        "Explicit bounded cleanup did not finish",
    )
    gone(row)
    require(BASE.digest(shared_source) == sources[str(shared_source)], "Bounded GC truncated a multiply-linked source")
    checks.append(
        "65 MiB partial scratch cleans in bounded calls without raising the 64 MiB limit or truncating hardlinked sources"
    )

    row = failed(native, artifacts / "symlink.json")
    link = Path(row["workspace"]) / "outside"
    link.symlink_to(unknown, target_is_directory=True)
    native.call("build-gc", 16, LIMIT, expected="Unexpected link/type")
    require(
        candidate(native)["state"] == "abandoned" and (unknown / "keep").read_bytes() == b"unregistered",
        "GC followed an unowned link",
    )
    link.unlink()
    collect(native)
    gone(row)
    for ordinal in range(2):
        revision = native.call("head", "knn")["revision"]
        native.call(
            "fail-build",
            "scratch_created",
            artifacts / f"count-{ordinal}.json",
            revision,
            expected="Injected native build failure",
        )
    result = collect(native, 1)
    require(result["collected_builds"] == 1 and result["remaining_builds"] == 1, "GC exceeded its build count budget")
    collect(native, 1)
    checks.append("unknown namespaces stay untouched; symlinks fail before deletion and task count is bounded")

    reference = artifacts / "explicit-publish.json"
    failed(native, reference, "before_build_publish")
    native.call("publish", "alias", reference, 0)
    row = candidate(native)
    require(
        row["state"] == "published" and row["keep_generation"], "Explicit publication did not claim candidate ownership"
    )
    collect(native)
    require(GC.directory(row).exists(), "GC deleted a candidate adopted by a named index")
    reference = artifacts / "publication-race.json"
    row = failed(native, reference, "before_build_publish")
    native.call("build-gc-publish-race", reference)
    gone(row)
    native.call("head", "gc-alias", expected="No published Vortex index")
    checks.append(
        "explicit candidate publication claims ownership atomically; collection and competing publication serialize"
    )

    fault_stages = [
        "build_gc_validated",
        "build_gc_deleting",
        "build_gc_file_deleted",
        "build_gc_truncated",
        "build_gc_before_finish",
        "build_gc_after_finish",
    ]
    for operation in ["build-gc-fail", "build-gc-cancel", "build-gc-crash"]:
        for stage in fault_stages:
            row = failed(native, artifacts / f"{operation}-{stage}.json", "generation_created")
            (Path(row["workspace"]) / "partial").write_bytes(b"partial scratch bytes")
            budget = 1 if stage == "build_gc_truncated" else LIMIT
            if operation == "build-gc-crash":
                kill_at(native, operation, stage, 16, budget)
            else:
                native.call(
                    operation,
                    stage,
                    16,
                    budget,
                    expected=(
                        "Interrupted" if operation.endswith("cancel") else "Injected Vortex build collection failure"
                    ),
                )
            if stage == "build_gc_after_finish":
                require(not status(native), "Completed GC metadata was replayed after a lost ACK")
            else:
                current = candidate(native)
                require(
                    current["state"] == ("abandoned" if stage == "build_gc_validated" else "deleting"),
                    "GC lost its durable deletion checkpoint",
                )
            collect(native)
            gone(row)
            require(collect(native)["collected_builds"] == 0, "Completed GC was not idempotent")
    checks.append(
        "failure, cancellation and SIGKILL at six GC boundaries resume safely, including partial truncation and lost completion ACK"
    )

    require(worker.search("held", SEG.VECTOR) == baseline, "Build GC changed an already opened old view")
    wanted = held_rows[:8]
    require(
        worker.fetch("held", [CAT.MIXED.ref(row) for row in wanted])
        == [CAT.MIXED.READ.business(row) for row in wanted],
        "Old-view fetch changed after build GC",
    )
    recalls.append(CAT.MIXED.qualify(baseline, held_rows, SEG.VECTOR))
    worker.close_view("held")
    worker.finish()
    qualify(native, "current", recalls)
    SEG.unchanged(native, first, frozen)
    require(all(BASE.digest(Path(path)) == digest for path, digest in sources.items()), "Build GC changed source bytes")
    require(
        (unknown / "keep").read_bytes() == b"unregistered"
        and (unknown_generation / "keep").read_bytes() == b"unregistered generation",
        "GC scanned unknown directories",
    )
    require(not status(native), "Repeated failures/crashes left registered tasks")
    checks.append(
        "current queries and old-view search/fetch survive repeated cleanup; B1 identity, artifacts and source bytes stay unchanged"
    )
    return {"fixture_recalls": recalls, "build_boundaries": len(stages), "gc_boundaries": len(fault_stages)}


def live_race(driver, root, mode, stage, checks):
    native = CAT.Native(driver, root / "native")
    try:
        artifacts, _, first, frozen = SEG.setup(native, root)
        collect(native)
        SEG.add_f(native, 5000)
        result = native.call("build-maintenance-race", artifacts / "B2.json", 1, stage, mode)
        require(result["published"]["revision"] == 2, "Build maintenance interrupted a live builder")
        require(
            result["maintenance"] == 0 if mode == "recover" else result["maintenance"]["collected_builds"] == 1,
            "Maintenance misclassified a live builder",
        )
        collect(native)
        SEG.unchanged(native, first, frozen)
        recalls = []
        qualify(native, "current", recalls)
        native.finish()
        checks.append(f"{mode} waits for a live builder paused at {stage}, then preserves its published winner")
        return {"mode": mode, "stage": stage, "fixture_recalls": recalls}
    finally:
        M.stop(native)


def recovery_bounds(driver, root, checks):
    native = CAT.Native(driver, root / "native")
    try:
        artifacts, _, first, frozen = SEG.setup(native, root)
        collect(native)
        for stage in ["build_allocated", "generation_created"]:
            kill_at(native, "crash-build", stage, artifacts / f"{stage}.json", 1)
        require(
            len(status(native)) == 2 and collect(native)["collected_builds"] == 0,
            "Restart implicitly abandoned a pending build",
        )
        require(
            native.call("recover-builds", 1) == 1 and sum(row["state"] == "abandoned" for row in status(native)) == 1,
            "Recovery exceeded its count budget",
        )
        require(native.call("recover-builds", 1) == 1, "Second explicit recovery missed a pending build")
        require(
            collect(native, 1)["remaining_builds"] == 1 and collect(native, 1)["remaining_builds"] == 0,
            "Bounded recovery/collection left a stopped task",
        )
        SEG.unchanged(native, first, frozen)
        recalls = []
        qualify(native, "current", recalls)
        native.finish()
        checks.append(
            "multiple stopped builds recover and collect in explicit bounded calls without replaying construction"
        )
        return {"fixture_recalls": recalls}
    finally:
        M.stop(native)


def retired_scratch(driver, root, checks):
    native = CAT.Native(driver, root / "native")
    try:
        artifacts, _, first, _ = SEG.setup(native, root)
        native.call("build", "knn", artifacts / "replacement.json", 1)
        require(
            native.call("gc", 16, LIMIT)["collected_generations"] == 1,
            "Published replacement did not retire the original generation",
        )
        require(not (artifacts / SEG.generation(first)).exists(), "Retirement collector kept the old generation")
        require(
            collect(native)["collected_builds"] == 2 and not status(native),
            "Workspace cleanup lost ownership after generation retirement",
        )
        recalls = []
        qualify(native, "current", recalls)
        native.finish()
        checks.append(
            "published workspace ownership remains safe after its retired canonical generation was already collected"
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
        "vortex_revision": BASE.VORTEX_REVISION,
        "checks": [],
    }
    started = time.monotonic()
    try:
        report["main"] = exercise(proof, native, directory, report["checks"], workers)
        native.finish()
        report["races"] = []
        for mode, stage in [("gc", "build_allocated"), ("gc", "generation_created"), ("recover", "source_pinned")]:
            directory = root / (mode + "-" + stage)
            directory.mkdir()
            report["races"].append(live_race(driver, directory, mode, stage, report["checks"]))
        for name, exercise_case in [("recovery-bounds", recovery_bounds), ("retired-scratch", retired_scratch)]:
            directory = root / name
            directory.mkdir()
            report[name] = exercise_case(driver, directory, report["checks"])
        require(not list(private.iterdir()), "Build cleanup proof leaked private query leases")
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
