#!/usr/bin/env python3
"""Qualify selective B compaction, continuous promotion and durable merge recovery."""
from __future__ import annotations

import argparse
import importlib.util
import json
import os
from pathlib import Path
import shutil
import time


def load(name, filename):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(filename))
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


SEG = load("segments", "test_native_vortex_segments.py")
OPT = load("optimizer", "test_native_vortex_optimizer.py")
CAT, BASE, SEARCH, MIXED = SEG.CAT, SEG.BASE, SEG.SEARCH, SEG.MIXED
require, segments, generation = SEG.require, SEG.segments, SEG.generation
VECTOR = SEG.VECTOR


def setup(native, root):
    artifacts, _, first, frozen = SEG.setup(native, root)
    retired = root / "retired"
    retired.mkdir()
    for ordinal, start in enumerate([5000, 6000], 2):
        SEG.add_f(native, start, 133 + ordinal / 16)
        native.call("append", "knn", retired / f"B{ordinal}.json", ordinal - 1)
    head = native.call("head", "knn")
    require(head["revision"] == 3 and len(segments(head)) == 3, "Could not publish three independent B segments")
    SEG.unchanged(native, first, frozen)
    merged = root / "merged"
    merged.mkdir()
    return artifacts, retired, merged, first, frozen, segments(head)[1:]


def selection(parts):
    return [generation(part) for part in parts]


def files(parts):
    return sorted([file for part in parts for file in part["reference"]["snapshot"]["files"]], key=lambda f: f["id"])


def covered(reference, parts):
    result = CAT.descriptor(reference)
    require(
        result["snapshot"]["files"] == files(parts), "Merge captured unselected B, F, T or changed source identities"
    )
    return result


def qualify(native, label, recalls, count):
    core, rows = SEG.qualify(native, label, recalls)
    require(core["diagnostics"]["routes"]["ann_calls"] == count, "Query skipped or reopened an unexpected B segment")
    return core, rows


def stop(native):
    if native.process.poll() is None:
        native.process.kill()
        native.process.wait(timeout=5)


def exercise(proof, native, root, checks, workers):
    _, retired, merged, first, frozen, parts = setup(native, root)
    chosen = selection(parts)
    recalls = []
    SEG.add_f(native, 7000, 133.03125)
    native.sql(
        "BEGIN;UPDATE lake.items SET embedding=[700000,0,0,0,0,0,0,0],payload=NULL WHERE id=133;"
        "DELETE FROM lake.items WHERE id IN (134,6001);"
        "INSERT INTO lake.items VALUES(200000,[133.0625,4,2,0,1,8,7,16],'A','合并前-T');COMMIT"
    )
    core, rows = qualify(native, "three-B", recalls, 3)
    require(core["diagnostics"]["routes"]["scan_files"] and core["diagnostics"]["routes"]["inline_rows"], "Missing F/T")
    native.call("append", "knn", merged / "fourth.json", 3, expected="segment count budget exceeded")
    native.call("merge", merged / "missing.json", 3, "missing", chosen[0], expected="no longer ready")
    native.call("merge", merged / "duplicate.json", 3, chosen[0], chosen[0], expected="two distinct ready generations")
    for stage in ["source_pinned", "build_started", "generation_sealed", "reference_ready", "before_commit"]:
        native.call("cancel-merge", stage, merged / f"cancel-{stage}.json", 3, *chosen, expected="Interrupted")
        require(native.call("head", "knn")["revision"] == 3, "Cancelled merge changed the ready manifest")
        SEG.unchanged(native, first, frozen)
    for stage in ["source_pinned", "reference_ready", "before_commit"]:
        native.call(
            "fail-merge", stage, merged / f"fail-{stage}.json", 3, *chosen, expected="Injected native build failure"
        )
        require(native.call("head", "knn")["revision"] == 3, "Failed merge exposed a partial replacement")
        SEG.unchanged(native, first, frozen)
    candidate = merged / "candidate.json"
    native.call("fail-merge", "before_build_publish", candidate, 3, *chosen, expected="Injected native build failure")
    descriptor = covered(candidate, parts)
    require(sum(f["row_count"] for f in descriptor["snapshot"]["files"]) == 256, "Merge rebuilt B1 or pending F")
    native.call("publish-merge", retired / "B2.json", 3, *chosen, expected="exactly the selected segments")
    native.call("publish-merge", candidate, 3, generation(first), chosen[0], expected="exactly the selected segments")
    damaged = CAT.artifact(candidate)
    original = damaged.read_bytes()
    try:
        damaged.write_bytes(original[:-1])
        native.call("publish-merge", candidate, 3, *chosen, expected="size/checksum mismatch")
    finally:
        damaged.write_bytes(original)
    for stage in ["segment_opened", "after_open", "after_ann"]:
        native.call("cancel-search", stage, expected="Interrupted")
        require(not list((root.parent / "private").iterdir()), "Interrupted three-B search leaked partial leases")
    native.call("capture", "held", "knn")
    worker = CAT.Worker(proof, "held", max_views=2)
    workers.append(worker)
    opened = worker.open("old", native.capture("held"))
    require(len(opened["generations"]) == 3, "Old view did not own all three generations")
    limited = CAT.Worker(proof, "limited", max_views=1, max_artifact_bytes=opened["artifact_bytes"] - 1)
    workers.append(limited)
    limited.open("rejected", native.capture("held"), expected="artifact byte budget exceeded")
    require(limited.call({"op": "stats"}) == MIXED.READ.empty_stats(), "Third-lease rejection retained earlier leases")
    limited.finish(eof=True)
    baseline = worker.search("old", VECTOR)
    recalls.append(MIXED.qualify(baseline, rows, VECTOR))
    published = native.call("race-merge", "knn", candidate, candidate, 3, *chosen)
    require(published["revision"] == 4 and len(segments(published)) == 2, "Merge CAS did not have one complete winner")
    require(generation(segments(published)[1]) == CAT.generation(candidate), "Publisher rebuilt the sealed candidate")
    SEG.unchanged(native, first, frozen)
    shutil.rmtree(retired)
    qualify(native, "after-retirement", recalls, 2)
    require(worker.search("old", VECTOR) == baseline, "Retired artifact GC invalidated the old three-B view")
    native.call("merge", merged / "stale-selection.json", 4, *chosen, expected="no longer ready")
    SEG.add_f(native, 8000, 133.015625)
    native.sql("UPDATE lake.items SET payload='合并后的新版本',embedding=[9000,0,0,0,0,0,0,0] WHERE id=200000")
    native.call("append", "knn", merged / "B4.json", 4)
    head = native.call("head", "knn")
    next_parts = segments(head)[1:]
    concurrent = SEG.sql(
        native,
        root,
        "INSERT INTO lake.items SELECT 9000+i,[133.0078125+i,4,2,0,1,8,7,16],'B','并发-F' FROM range(128)t(i);"
        "INSERT INTO lake.items VALUES(200001,[133.00390625,4,2,0,1,8,7,16],'A','并发-T')",
    )
    second = merged / "second.json"
    result = native.call("split-merge", second, 5, "build_started", concurrent, *selection(next_parts))
    require(result["published"]["revision"] == 6, "Concurrent merge did not replace exactly one catalog revision")
    require(
        result["before"]["diagnostics"]["routes"]["ann_calls"] == 3
        and result["during"]["diagnostics"]["routes"]["ann_calls"] == 3,
        "Construction hid a ready segment",
    )
    require(
        len(result["during"]["diagnostics"]["routes"]["scan_files"]) == 1, "Concurrent F entered the merge snapshot"
    )
    covered(second, next_parts)
    qualify(native, "after-concurrent-merge", recalls, 2)
    native.call("append", "knn", merged / "B5.json", 6)
    qualify(native, "reused-slot", recalls, 3)
    SEG.unchanged(native, first, frozen)
    require(worker.search("old", VECTOR) == baseline, "Further promotion changed the pinned old result")
    wanted = [next(row for row in rows if row["id"] == i) for i in [0, 5001, 6000, 7000, 200000, 5001]]
    require(
        worker.fetch("old", [MIXED.ref(row) for row in wanted]) == [MIXED.READ.business(row) for row in wanted],
        "Delayed old B1/B2/B3/F/T fetch lost versions, nulls, Unicode or duplicate order",
    )
    require(worker.close_view("old") == MIXED.READ.empty_stats(), "Old view retained aggregate reservations")
    worker.finish(eof=True)
    checks.extend(
        [
            "three B segments share one view; partial third-lease failure and interruption release all earlier resources",
            "only B2/B3's 256 physical rows are rebuilt; B1 generation, descriptor, checksums and nanosecond write times are unchanged",
            "selection identity, exact coverage, artifact validation, cancellation and publication CAS preserve a complete ready set",
            "merge construction permits concurrent writes/queries and excludes unselected B and F/T from the candidate",
            "repeated append/merge reuses the three slots; a held view and delayed typed fetch survive retired artifact GC and later promotions",
        ]
    )
    return {
        "fixture_recalls": recalls,
        "final_revision": 7,
        "selected_rows": 256,
        "unchanged_generation": generation(first),
    }


def held_native(driver, root, checks):
    native = CAT.Native(driver, root / "native")
    try:
        _, retired, merged, first, frozen, parts = setup(native, root)
        chosen = selection(parts)
        SEG.add_f(native, 7000)
        native.sql("INSERT INTO lake.items VALUES(200000,[133.0625,4,2,0,1,8,7,16],'A','held-T')")
        recalls = []
        before, _ = qualify(native, "before", recalls, 3)
        candidate = merged / "held.json"
        native.call(
            "fail-merge", "before_build_publish", candidate, 3, *chosen, expected="Injected native build failure"
        )
        mutation = SEG.sql(
            native,
            root,
            "UPDATE lake.items SET embedding=[9000,0,0,0,0,0,0,0],payload='新版本' WHERE id=5000;"
            "DELETE FROM lake.items WHERE id=6000;"
            "SELECT count(*) FROM ducklake_expire_snapshots('lake',older_than=>now());"
            "SELECT count(*) FROM ducklake_cleanup_old_files('lake',cleanup_all=>true)",
        )
        held = native.call("held-merge", candidate, 3, mutation, retired, *chosen)
        require(
            held["rows"] == before["rows"] and held["diagnostics"]["routes"]["ann_calls"] == 3,
            "Native view changed during merge/GC",
        )
        require(not retired.exists(), "Retired canonical generations were not actually reclaimed")
        qualify(native, "current", recalls, 2)
        SEG.unchanged(native, first, frozen)
        native.finish()
        checks.append(
            "a paused native three-B query survives atomic selective replacement, current-row mutation and actual retired generation GC"
        )
        return {"fixture_recalls": recalls}
    finally:
        stop(native)


def background(driver, root, checks):
    native = CAT.Native(driver, root / "native")
    try:
        artifacts, _, _, first, frozen, parts = setup(native, root)
        chosen = selection(parts)
        native.call("optimizer-init")
        native.call("optimizer-enable", artifacts, 100000, 4194304, 128, 67108864, "build_started", "pause")
        native.sql(
            "SELECT * FROM ducklake_set_vortex_write_budget('lake','main','items','knn',32,1048576,256,67108864)"
        )
        SEG.add_f(native, 7000)
        native.call("optimizer-wait")
        durable = OPT.status(native)
        require(
            durable["task_kind"] == "merge" and set(durable["selected_generations"]) == set(chosen),
            "Smallest-pair plan was not durable",
        )
        require(durable["expected_revision"] == 3, "Merge was not bound to its scheduling revision")
        SEG.add_f(native, 8000)
        native.sql("INSERT INTO lake.items VALUES(200000,[133.0625,4,2,0,1,8,7,16],'A','后台-T')")
        rejected = SEG.sql(
            native, root, "INSERT INTO lake.items SELECT 9000+i,[i::FLOAT,0,0,0,0,0,0,0],'B','over' FROM range(128)t(i)"
        )
        native.call("sql", rejected, expected="unindexed rows")
        native.call("optimizer-cancel")
        native.call("optimizer-release")
        cancelled = OPT.wait(native, lambda s: s["state"] == "cancelled" and not s["running"], "cancelled merge")
        require(
            not cancelled["enabled"] and native.call("head", "knn")["revision"] == 3, "Cancellation exposed a merge"
        )
        native.call("sql", rejected, expected="unindexed rows")
        native.call("optimizer-resume", "optimizer_merged", "pause")
        native.call("optimizer-wait")
        published = OPT.status(native)
        require(
            published["task_kind"] == "merge" and published["published_revision"] == 4, "Merge was not a separate task"
        )
        head = native.call("head", "knn")
        require(
            len(segments(head)) == 2 and segments(head)[1]["reference"]["snapshot"]["files"] == files(parts),
            "Merge consumed F",
        )
        SEG.unchanged(native, first, frozen)
        recalls = []
        qualify(native, "paused-after-merge", recalls, 2)
        native.call("sql", rejected, expected="unindexed rows")
        native.call("optimizer-release")
        OPT.wait(native, lambda s: s["published_revision"] == 5 and not s["running"], "append after merge")
        for ordinal, start in enumerate([9000, 10000, 11000], 1):
            SEG.add_f(native, start)
            revision = 5 + 2 * ordinal
            OPT.wait(
                native, lambda s: s["published_revision"] == revision and not s["running"], "continued merge and append"
            )
            require(len(segments(native.call("head", "knn"))) == 3, "Steady state exceeded the segment bound")
            SEG.unchanged(native, first, frozen)
            qualify(native, f"cycle-{ordinal}", recalls, 3)
        final = OPT.status(native)
        require(
            final["run_count"] == 9 and final["enabled"],
            "Merge scheduling retried work or stopped at the segment limit",
        )
        native.call("optimizer-cancel")
        native.finish()
        checks.append(
            "durable smallest-pair tasks precede append at the three-slot limit; four cycles preserve B1, F/T visibility and write quotas through cancellation"
        )
        return {"fixture_recalls": recalls, "final_revision": 11, "run_count": final["run_count"]}
    finally:
        stop(native)


def crash(driver, root, stage, checks):
    native = CAT.Native(driver, root / "native")
    try:
        artifacts, _, _, first, frozen, parts = setup(native, root)
        native.call("optimizer-init")
        native.call("optimizer-enable", artifacts, 100000, 4194304, 128, 67108864, stage, "pause")
        SEG.add_f(native, 7000)
        native.call("optimizer-wait")
        durable = OPT.status(native)
        require(
            durable["task_kind"] == "merge" and set(durable["selected_generations"]) == set(selection(parts)),
            "Crash task lost selection",
        )
        require(
            durable["candidate_generation"] and durable["state"] == "publishing", "Candidate identity was not durable"
        )
        candidate = durable["candidate_generation"]
        native.process.kill()
        native.process.wait(timeout=10)
        native.log.close()
        require(native.process.returncode == -9, "Merge was not killed with SIGKILL")
        native.start("resume")
        initial = native.call("head", "knn")
        committed = stage == "after_commit"
        require(
            initial["revision"] == 3 + committed and len(segments(initial)) == (2 if committed else 3),
            "Recovery exposed a partial manifest",
        )
        require(not OPT.status(native)["running"], "Restart automatically replayed a merge")
        SEG.unchanged(native, first, frozen)
        if committed:
            native.call("optimizer-resume", "optimizer_before_build", "pause")
        else:
            native.call("optimizer-resume", "optimizer_before_merge", "pause")
        native.call("optimizer-wait")
        replanned = OPT.status(native)
        require(replanned["run_count"] == durable["run_count"] + 1, "Recovery duplicated or skipped a task")
        if committed:
            require(generation(segments(native.call("head", "knn"))[1]) == candidate, "Committed merge was rebuilt")
            require(replanned["task_kind"] == "build", "Recovered merge was scheduled again")
        else:
            require(
                replanned["run_id"] != durable["run_id"] and not replanned["candidate_generation"],
                "Uncommitted candidate was silently replayed",
            )
        native.call("optimizer-release")
        OPT.wait(native, lambda s: s["published_revision"] == 5 and not s["running"], "recovered continuous promotion")
        head = native.call("head", "knn")
        recovered = generation(segments(head)[1])
        require((recovered == candidate) == committed, "Recovery reused the wrong merge generation")
        require(
            segments(head)[1]["reference"]["snapshot"]["files"] == files(parts), "Recovery merged the wrong coverage"
        )
        SEG.unchanged(native, first, frozen)
        recalls = []
        qualify(native, "recovered", recalls, 3)
        native.call("optimizer-cancel")
        native.finish()
        checks.append(
            f"SIGKILL {stage} recovers a complete manifest, preserves B1 and reconciles the exact merge generation before continuing F promotion"
        )
        return {
            "fixture_recalls": recalls,
            "revision_at_reopen": initial["revision"],
            "committed_candidate_reused": committed,
        }
    finally:
        stop(native)


def failure(driver, root, checks):
    native = CAT.Native(driver, root / "native")
    try:
        artifacts, _, _, first, frozen, parts = setup(native, root)
        native.call("optimizer-init")
        native.call("optimizer-enable", artifacts, 100000, 4194304, 128, 67108864, "before_build_publish", "fail")
        SEG.add_f(native, 7000)
        failed = OPT.wait(native, lambda s: s["state"] == "failed" and not s["running"], "terminal merge failure")
        require(not failed["enabled"] and failed["task_kind"] == "merge", "Failed merge stayed active")
        require(native.call("head", "knn")["revision"] == 3, "Failed candidate changed the ready set")
        candidate = artifacts / f"optimizer-{failed['run_id']}.json"
        covered(candidate, parts)
        OPT.steady(native, failed)
        recalls = []
        qualify(native, "failed", recalls, 3)
        native.call("optimizer-resume", "after_commit", "fail")
        published = OPT.wait(
            native, lambda s: s["published_revision"] == 5 and not s["running"], "lost merge/append ACK"
        )
        require(
            published["enabled"]
            and published["run_count"] == 3
            and "committed but acknowledgement failed" in published["error"],
            "Lost ACK duplicated a merge or stopped already committed work",
        )
        head = native.call("head", "knn")
        require(
            segments(head)[1]["reference"]["snapshot"]["files"] == files(parts), "ACK recovery changed merge coverage"
        )
        require(
            generation(segments(head)[1]) != CAT.generation(candidate), "Resume silently replayed the failed candidate"
        )
        SEG.unchanged(native, first, frozen)
        OPT.steady(native, published)
        qualify(native, "acknowledged", recalls, 3)
        native.call("optimizer-cancel")
        native.call("reopen")
        require(native.call("head", "knn") == head, "Same-process reopen rebuilt a reconciled generation")
        native.finish()
        checks.append(
            "merge failure stops without retry; explicit resume builds afresh and lost merge/append ACKs reconcile without duplicate publication or rebuild"
        )
        return {"fixture_recalls": recalls, "run_count": 3}
    finally:
        stop(native)


def global_rows(driver, root, checks):
    native = CAT.Native(driver, root / "native")
    try:
        _, _, merged, first, frozen, parts = setup(native, root)
        native.sql("INSERT INTO lake.items SELECT 10000+i,[i::FLOAT,0,0,0,0,0,0,0],'B','global' FROM range(95649)t(i)")
        candidate = merged / "too-large.json"
        native.call("merge", candidate, 3, *selection(parts), expected="physical row budget exceeded")
        require(not candidate.exists(), "Selected compaction bypassed the aggregate table row ceiling")
        SEG.unchanged(native, first, frozen)
        native.finish()
        checks.append("a 256-row selected merge cannot bypass the unchanged global 100,000-row table admission budget")
    finally:
        stop(native)


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
    driver = args.driver.resolve(strict=True)
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
        for name, action in [
            ("held-native", held_native),
            ("background", background),
            ("failure", failure),
            ("global-rows", global_rows),
        ]:
            directory = root / name
            directory.mkdir()
            result = action(driver, directory, report["checks"])
            if result is not None:
                report[name] = result
        for stage in ["before_commit", "after_commit"]:
            directory = root / stage
            directory.mkdir()
            report[stage] = crash(driver, directory, stage, report["checks"])
        require(not list(private.iterdir()), "Merge qualification retained private leases")
        report["status"] = "passed"
    except Exception as error:
        report.update(status="failed", error=str(error))
        raise
    finally:
        stop(native)
        for worker in workers:
            if worker.process.poll() is None:
                worker.process.kill()
                worker.process.wait(timeout=5)
        report["seconds"] = time.monotonic() - started
        (root / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
