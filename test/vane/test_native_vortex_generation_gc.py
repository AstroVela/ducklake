#!/usr/bin/env python3
"""Qualify registered generation retirement, bounded GC and protected lease admission."""
from __future__ import annotations

import argparse
import importlib.util
import json
import os
from pathlib import Path
import time

SPEC = importlib.util.spec_from_file_location("merge", Path(__file__).with_name("test_native_vortex_merge.py"))
assert SPEC is not None and SPEC.loader is not None
M = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(M)
CAT, BASE, SEG, MIXED = M.CAT, M.BASE, M.SEG, M.MIXED
require, segments, generation = M.require, M.segments, M.generation
LIMIT = 64 * 1024 * 1024


def status(native):
    return native.call("generation-status")


def retired(native):
    return [row for row in status(native) if row["state"] != "active"]


def directory(row):
    return Path(row["index_root"]) / row["generation"]


def seal(native, merged, chosen, label="candidate"):
    path = merged / f"{label}.json"
    native.call("fail-merge", "before_build_publish", path, 3, *chosen, expected="Injected native build failure")
    return path


def setup(native, root):
    return M.setup(native, root)


def publish(native, candidate, chosen):
    result = native.call("publish-merge", candidate, 3, *chosen)
    require(result["revision"] == 4 and len(segments(result)) == 2, "Merge publication was incomplete")
    rows = status(native)
    require(
        sum(row["state"] == "active" for row in rows) == 2 and sum(row["state"] == "retired" for row in rows) == 2,
        "Publication did not atomically retire exactly the replaced pair",
    )
    require(
        all(row["reference_count"] == (1 if row["state"] == "active" else 0) for row in rows),
        "Reference map disagrees with the catalog",
    )
    return result


def exercise(proof, native, root, checks, workers):
    _, _, merged, first, frozen, parts = setup(native, root)
    chosen = M.selection(parts)
    SEG.add_f(native, 7000, 133.03125)
    native.sql("INSERT INTO lake.items VALUES(200000,[133.0625,4,2,0,1,8,7,16],'A','GC-实时-T')")
    recalls = []
    _, old_rows = M.qualify(native, "old", recalls, 3)
    native.call("capture", "old", "knn")
    worker = CAT.Worker(proof, "old-worker", max_views=1)
    workers.append(worker)
    worker.open("old", native.capture("old"))
    baseline = worker.search("old", M.VECTOR)
    native.call("cancel-merge", "before_commit", merged / "cancelled.json", 3, *chosen, expected="Interrupted")
    require(
        len(status(native)) == 3 and not retired(native), "Rolled-back publication registered a candidate or retirement"
    )
    candidate = seal(native, merged, chosen)
    require(
        CAT.generation(candidate) not in {r["generation"] for r in status(native)},
        "Unpublished candidate entered GC ownership",
    )
    unknown = merged / "generation-unregistered"
    unknown.mkdir()
    (unknown / "keep").write_text("not registered")
    cancelled = merged / CAT.generation(merged / "cancelled.json")
    cancelled_signature = SEG.signature(cancelled)
    publish(native, candidate, chosen)
    pending = retired(native)
    paths = [directory(row) for row in pending]
    protected = [row for row in status(native) if row["state"] == "active"]
    source_identity = {f["uri"]: BASE.digest(Path(f["uri"])) for f in M.files([first, *parts])}
    limited = native.call("gc", 16, pending[0]["artifact_bytes"] - 1)
    require(limited["collected_generations"] == 0 and limited["remaining_generations"] == 2, "Byte budget was exceeded")
    require(all(path.exists() for path in paths), "Budget rejection changed registered files")
    first_batch = native.call("gc", 1, LIMIT)
    require(
        first_batch["collected_generations"] == 1 and first_batch["remaining_generations"] == 1,
        "Count budget was exceeded",
    )
    require(sum(path.exists() for path in paths) == 1, "Collector removed the wrong generation set")
    remaining = retired(native)[0]
    target = directory(remaining)
    original = SEG.signature(target)
    extra = target / "unregistered-file"
    extra.write_text("must not be deleted")
    native.call("gc", 16, LIMIT, expected="Unexpected path")
    require(extra.read_text() == "must not be deleted", "Collector deleted an unregistered child")
    extra.unlink()
    require(
        SEG.signature(target) == original and retired(native)[0]["state"] == "retired",
        "Failed validation mutated the registered generation",
    )
    manifest = CAT.descriptor(target / "manifest.json")
    artifact = target / "artifacts" / manifest["artifacts"][0]["path"]
    artifact_stat = artifact.stat()
    content = artifact.read_bytes()
    try:
        artifact.write_bytes(bytes([content[0] ^ 1]) + content[1:])
        native.call("gc", 16, LIMIT, expected="checksum changed")
    finally:
        artifact.write_bytes(content)
        os.utime(artifact, ns=(artifact_stat.st_atime_ns, artifact_stat.st_mtime_ns))
    outside = root / "outside"
    outside.mkdir()
    keep = outside / "keep"
    keep.write_bytes(content)
    artifact.unlink()
    try:
        artifact.symlink_to(keep)
        native.call("gc", 16, LIMIT, expected="Unexpected path")
        require(keep.read_bytes() == content, "Collector followed an artifact symlink")
    finally:
        artifact.unlink()
        artifact.write_bytes(content)
        os.utime(artifact, ns=(artifact_stat.st_atime_ns, artifact_stat.st_mtime_ns))
    parent = target.parent
    backup = parent.with_name(parent.name + "-saved")
    parent.rename(backup)
    try:
        parent.symlink_to(outside, target_is_directory=True)
        native.call("gc", 16, LIMIT, expected="without following links")
        require(keep.read_bytes() == content, "Collector followed a root symlink")
    finally:
        parent.unlink()
        backup.rename(parent)
    require(SEG.signature(target) == original, "Failed validation changed sealed artifacts")
    native.call("gc-cancel", "gc_file_deleted", 1, LIMIT, expected="Interrupted")
    require(retired(native)[0]["state"] == "deleting" and target.exists(), "Partial deletion was not recoverable")
    require(SEG.signature(target) != original, "Cancellation did not occur after actual unlink")
    M.qualify(native, "partial-gc", recalls, 2)
    require(worker.search("old", M.VECTOR) == baseline, "Partial GC invalidated old private leases")
    native.call("gc-fail", "gc_before_finish", 1, LIMIT, expected="Injected Vortex collection failure")
    require(
        not target.exists() and retired(native)[0]["state"] == "deleting", "Failed completion lost its deleting record"
    )
    finished = native.call("gc", 16, LIMIT)
    require(
        finished["collected_generations"] == 1 and not finished["remaining_generations"],
        "Explicit resumption did not finish deletion",
    )
    require(native.call("gc", 16, LIMIT)["collected_generations"] == 0, "Repeated collection deleted more data")
    require(all(directory(row).exists() for row in protected), "GC deleted an active generation")
    require(
        SEG.signature(cancelled) == cancelled_signature and (unknown / "keep").read_text() == "not registered",
        "GC guessed ownership of an unpublished directory",
    )
    require(
        all(BASE.digest(Path(path)) == digest for path, digest in source_identity.items()),
        "Index GC changed source data",
    )
    revision = 4
    for ordinal in range(3):
        native.call("append", "knn", merged / f"append-{ordinal}.json", revision)
        revision += 1
        head = native.call("head", "knn")
        selected = segments(head)[1:]
        SEG.add_f(native, 8000 + ordinal * 1000)
        native.call("merge", merged / f"merge-{ordinal}.json", revision, *M.selection(selected))
        revision += 1
        old = [directory(row) for row in retired(native)]
        result = native.call("gc", 16, LIMIT)
        require(
            result["collected_generations"] == 2 and not result["remaining_generations"],
            "Repeated promotion accumulated retired artifacts",
        )
        require(all(not path.exists() for path in old), "Completed collection retained published retired directories")
        require(len(status(native)) == 2, "Completed retirement metadata accumulated")
        SEG.unchanged(native, first, frozen)
        M.qualify(native, f"cycle-{ordinal}", recalls, 2)
    require(worker.search("old", M.VECTOR) == baseline, "Later merge/GC changed an old retained result")
    wanted = [next(row for row in old_rows if row["id"] == i) for i in [0, 5001, 6000, 7000, 200000, 5001]]
    require(
        worker.fetch("old", [MIXED.ref(row) for row in wanted]) == [MIXED.READ.business(row) for row in wanted],
        "Delayed old B1/B2/B3/F/T fetch changed after GC",
    )
    require(worker.close_view("old") == MIXED.READ.empty_stats(), "Closing old view retained reservations")
    worker.finish(eof=True)
    checks.extend(
        [
            "catalog publication, active references and retirement are atomic; cancelled and sealed-unpublished generations stay outside GC",
            "count and artifact-byte budgets bound collection; only registered generation contents are unlinked",
            "unknown children, corrupt bytes and symlink roots/artifacts fail explicitly without deleting unrelated data",
            "partial deletion persists deleting state; cancellation and failed completion resume explicitly and repeated execution is safe",
            "four append/merge/GC rounds reclaim retired published artifacts and their registry rows while preserving B1, winners, source data and old-view typed fetch",
        ]
    )
    return {"fixture_recalls": recalls, "final_revision": revision, "rounds": 4}


def race(driver, root, stage, cancel, checks):
    native = CAT.Native(driver, root / "native")
    try:
        _, _, merged, first, frozen, parts = setup(native, root)
        SEG.add_f(native, 7000)
        native.sql("INSERT INTO lake.items VALUES(200000,[133.0625,4,2,0,1,8,7,16],'A','race-T')")
        recalls = []
        baseline, _ = M.qualify(native, "before", recalls, 3)
        candidate = seal(native, merged, M.selection(parts))
        result = native.call("gc-lease-race", stage, candidate, 3, *M.selection(parts), "cancel" if cancel else "keep")
        require(
            result["gc"]["collected_generations"] == 2 and not retired(native),
            "Lease admission race did not finish actual GC",
        )
        if not cancel:
            require(
                result["held"]["rows"] == baseline["rows"]
                and result["held"]["diagnostics"]["routes"]["ann_calls"] == 3,
                "Admission adopted a new generation or lost the old one",
            )
        require(
            all(not (Path(part["root"]) / generation(part)).exists() for part in parts),
            "Race did not reclaim retired canonical generations",
        )
        M.qualify(native, "after", recalls, 2)
        SEG.unchanged(native, first, frozen)
        native.finish()
        checks.append(
            f"lease admission at {stage}{' with cancellation' if cancel else ''} coordinates GC; fully opened private views permit collection before query completion"
        )
        return {"fixture_recalls": recalls, "stage": stage, "cancelled": cancel}
    finally:
        M.stop(native)


def build_race(driver, root, checks):
    native = CAT.Native(driver, root / "native")
    try:
        _, _, merged, first, frozen, parts = setup(native, root)
        candidate = seal(native, merged, M.selection(parts))
        publish(native, candidate, M.selection(parts))
        SEG.add_f(native, 7000)
        result = native.call("gc-build-race", merged / "next.json", 4)
        require(
            result["published"]["revision"] == 5 and result["gc"]["collected_generations"] == 2,
            "GC/build coordination lost publication or retirement",
        )
        require(
            not retired(native) and len(status(native)) == 3,
            "GC removed an in-flight candidate or retained completed retirements",
        )
        recalls = []
        M.qualify(native, "current", recalls, 3)
        SEG.unchanged(native, first, frozen)
        native.finish()
        checks.append(
            "GC waits for a native build's generation guard and then reclaims only retired inputs, preserving its newly published winner"
        )
        return {"fixture_recalls": recalls}
    finally:
        M.stop(native)


def kill_at(native, operation, stage, *args):
    prefix = "gc-" if operation == "gc-crash" else "build-"
    marker = native.root / f"{prefix}{stage}.ready"
    native.send(operation, stage, *args)
    deadline = time.monotonic() + 60
    while not marker.exists():
        require(native.process.poll() is None, "Owner exited before crash boundary")
        require(time.monotonic() < deadline, f"Crash boundary timed out: {stage}")
        time.sleep(0.01)
    native.process.kill()
    native.process.wait(timeout=10)
    native.log.close()
    require(native.process.returncode == -9, "Owner did not stop with SIGKILL")
    native.start("resume")


def publication_race(driver, root, checks):
    native = CAT.Native(driver, root / "native")
    try:
        _, parent, merged, first, frozen, parts = setup(native, root)
        candidate = seal(native, merged, M.selection(parts))
        publish(native, candidate, M.selection(parts))
        target = retired(native)[0]
        reference = next(
            parent / f"B{i}.json" for i in [2, 3] if CAT.generation(parent / f"B{i}.json") == target["generation"]
        )
        result = native.call("gc-publish-race", reference)
        require(
            result["collected_generations"] == 1 and not directory(target).exists(),
            "Publication/GC race did not remove its selected retirement",
        )
        native.call("head", "gc-alias", expected="No published Vortex index")
        native.call("gc", 16, LIMIT)
        SEG.unchanged(native, first, frozen)
        recalls = []
        M.qualify(native, "current", recalls, 2)
        native.finish()
        checks.append(
            "publication and deletion serialize: a publisher cannot reactivate a generation while GC removes it"
        )
        return {"fixture_recalls": recalls}
    finally:
        M.stop(native)


def crash(driver, root, stage, checks):
    native = CAT.Native(driver, root / "native")
    try:
        _, _, merged, first, frozen, parts = setup(native, root)
        candidate = seal(native, merged, M.selection(parts))
        publish(native, candidate, M.selection(parts))
        target = retired(native)[0]
        path = directory(target)
        kill_at(native, "gc-crash", stage, 1, LIMIT)
        rows = retired(native)
        if stage == "gc_after_finish":
            require(len(rows) == 1 and not path.exists(), "Completed GC was replayed at restart")
        else:
            current = next(row for row in rows if row["generation"] == target["generation"])
            require(len(rows) == 2, "Restart changed retirement metadata implicitly")
            require(
                current["state"] == ("retired" if stage == "gc_validated" else "deleting"),
                "Deletion checkpoint was not durable",
            )
            require(
                path.exists() == (stage not in ["gc_files_deleted"]),
                "File deletion checkpoint disagrees with durable state",
            )
        result = native.call("gc", 16, LIMIT)
        require(
            result["collected_generations"] == (1 if stage == "gc_after_finish" else 2),
            "Explicit GC resumption lost a registered generation",
        )
        require(not retired(native) and not path.exists(), "Resume did not finish registered cleanup")
        require(native.call("gc", 16, LIMIT)["collected_generations"] == 0, "Completed GC was not idempotent")
        SEG.unchanged(native, first, frozen)
        recalls = []
        M.qualify(native, "recovered", recalls, 2)
        native.call("reopen")
        require(not retired(native), "Same-process reopen restored completed retirement state")
        native.finish()
        checks.append(
            f"SIGKILL at {stage} preserves active data and resumes registered deletion explicitly, including partial/missing files"
        )
        return {"fixture_recalls": recalls}
    finally:
        M.stop(native)


def publication_crash(driver, root, stage, checks):
    native = CAT.Native(driver, root / "native")
    try:
        _, _, merged, first, frozen, parts = setup(native, root)
        reference = merged / "crash.json"
        kill_at(native, "crash-merge", stage, reference, 3, *M.selection(parts))
        committed = stage == "after_commit"
        head = native.call("head", "knn")
        require(head["revision"] == (4 if committed else 3), "Publication recovered an incorrect revision")
        require(len(retired(native)) == (2 if committed else 0), "Catalog and retirement records committed separately")
        require(
            (CAT.generation(reference) in {row["generation"] for row in status(native)}) == committed,
            "Uncommitted candidate entered generation ownership",
        )
        result = native.call("gc", 16, LIMIT)
        require(
            result["collected_generations"] == (2 if committed else 0),
            "GC reclaimed an uncommitted or active generation",
        )
        SEG.unchanged(native, first, frozen)
        recalls = []
        M.qualify(native, "recovered", recalls, 2 if committed else 3)
        native.finish()
        checks.append(
            f"SIGKILL {stage} commits or rolls back catalog, generation ownership, references and retirement together"
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
        report["admissions"] = []
        for stage, cancel in [
            ("after_snapshot", False),
            ("before_open", False),
            ("source_pinned", False),
            ("segment_opened", False),
            ("after_open", False),
            ("source_pinned", True),
        ]:
            directory = root / (stage + ("-cancel" if cancel else ""))
            directory.mkdir()
            report["admissions"].append(race(driver, directory, stage, cancel, report["checks"]))
        directory = root / "build"
        directory.mkdir()
        report["build"] = build_race(driver, directory, report["checks"])
        directory = root / "publication-race"
        directory.mkdir()
        report["publication_race"] = publication_race(driver, directory, report["checks"])
        report["gc_crashes"] = []
        for stage in ["gc_validated", "gc_deleting", "gc_file_deleted", "gc_files_deleted", "gc_after_finish"]:
            directory = root / stage
            directory.mkdir()
            report["gc_crashes"].append(crash(driver, directory, stage, report["checks"]))
        report["publication_crashes"] = []
        for stage in ["before_commit", "after_commit"]:
            directory = root / ("publication-" + stage)
            directory.mkdir()
            report["publication_crashes"].append(publication_crash(driver, directory, stage, report["checks"]))
        require(not list(private.iterdir()), "Generation GC left private query leases")
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
