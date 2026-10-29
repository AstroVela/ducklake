#!/usr/bin/env python3
"""Qualify empty-B retirement, explicit zero-B views and metadata-only recovery."""
from __future__ import annotations

import argparse
import importlib.util
import json
import os
from pathlib import Path
import time

SPEC = importlib.util.spec_from_file_location(
    "background", Path(__file__).with_name("test_native_vortex_background_rewrite.py")
)
assert SPEC is not None and SPEC.loader is not None
BG = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BG)
RW, MA, M, SEG, CAT, BASE, OPT = BG.RW, BG.MA, BG.M, BG.SEG, BG.CAT, BG.BASE, BG.OPT
SEARCH = SEG.SEARCH
require = BASE.require


def qualify(native, label, recalls, count):
    core, rows = M.qualify(native, label, recalls, count)
    if not count:
        require(
            core["diagnostics"]["routes"]["indexed_files"] == []
            and core["diagnostics"]["generations"] == []
            and core["diagnostics"]["generation"] is None,
            "Zero-B search invented an ANN generation or indexed source",
        )
    return core, rows


def single(native, root):
    artifacts, _, first, _ = SEG.setup(native, root)
    native.call("optimizer-init")
    native.call("optimizer-enable", artifacts, 100000, 4194304, 100000, RW.LIMIT)
    BG.stop(native)
    native.sql(
        "SELECT * FROM ducklake_set_vortex_write_budget('lake','main','items','knn',100000,4194304,100000,67108864)"
    )
    native.call("maintenance-enable", 16, RW.LIMIT)
    MA.drain(native)
    native.sql("DELETE FROM lake.items")
    BG.configure(native, 1, 1)
    return first


def three(native, root):
    artifacts, _, first, _ = SEG.setup(native, root)
    native.sql("CALL ducklake_set_option('lake','data_inlining_row_limit',100,table_name=>'items')")
    for ordinal, start in enumerate([5000, 6000], 2):
        native.sql(
            f"INSERT INTO lake.items SELECT {start}+i,[128+i/16.0,((i*4)%12)::FLOAT,((i*5)%13)::FLOAT,0,1,8,7,16],"
            "'B','独立-B' FROM range(128)t(i)"
        )
        native.call("append", "knn", artifacts / f"B{ordinal}.json", ordinal - 1)
    parts = SEG.segments(native.call("head", "knn"))[1:]
    frozen = {SEG.generation(p): SEG.signature(Path(p["root"]) / SEG.generation(p)) for p in parts}
    native.call("optimizer-init")
    native.call("optimizer-enable", artifacts, 100000, 4194304, 100000, RW.LIMIT)
    BG.stop(native)
    native.sql(
        "SELECT * FROM ducklake_set_vortex_write_budget('lake','main','items','knn',100000,4194304,256,67108864)"
    )
    native.call("maintenance-enable", 16, RW.LIMIT)
    MA.drain(native)
    native.sql("DELETE FROM lake.items WHERE id<100 OR (id>=2148 AND id<4096)")
    return first, parts, frozen


def exercise(proof, native, root, checks, workers):
    first, parts, frozen = three(native, root)
    recalls = []
    native.call("capture", "held", "knn")
    old_rows = native.rows("held")
    worker = CAT.Worker(proof, "held", max_views=2)
    workers.append(worker)
    worker.open("held", native.capture("held"))
    baseline = worker.search("held", M.VECTOR)
    native.call("retire", SEG.generation(first), 3, expected="still has visible rows")
    before = native.call("head", "knn")
    native.sql("DELETE FROM lake.items WHERE id<4096")
    require(
        native.call("head", "knn") == before and RW.active_rows(native) == 4352,
        "DELETE broke ready B coverage",
    )
    qualify(native, "masked", recalls, 3)
    BG.configure(native)
    row = BG.pause(native, "retire_candidate_ready")
    require(
        row["state"] == "retiring"
        and row["task_kind"] == "retire"
        and row["selected_generations"] == [SEG.generation(first)]
        and row["remaining_rows"] == 0,
        "Worker did not durably select an empty B for retirement",
    )
    native.sql(
        "SELECT CASE WHEN reference_path='' THEN true ELSE error('metadata-only retirement allocated an index reference') END "
        "FROM __ducklake_metadata_lake.ducklake_vortex_optimizer_v2"
    )
    native.call("sql", BG.APP.append_sql(native, root))
    qualify(native, "append-during-retirement", recalls, 3)
    native.call("optimizer-release")
    row = BG.published(native, 4)
    BG.stop(native)
    head = native.call("head", "knn")
    require(head["reference"]["publication"] == row["run_id"], "Retirement lost its publication identity")
    require(
        SEG.segments(head) == parts and RW.active_rows(native) == 288,
        "Retirement rebuilt B or lost concurrent F",
    )
    RW.unchanged(native, parts, frozen)
    qualify(native, "retired-one", recalls, 2)
    MA.drain(native)
    require(
        not Path(first["root"], SEG.generation(first)).exists(),
        "Retired B did not converge under authorized GC",
    )
    require(worker.search("held", M.VECTOR) == baseline, "Index GC changed a held old view")
    for ordinal, part in enumerate(parts):
        start = 5000 + ordinal * 1000
        native.sql(f"DELETE FROM lake.items WHERE id>={start} AND id<{start + 128}")
        BG.pause(native, "retire_candidate_ready")
        native.call("optimizer-release")
        row = BG.published(native, 5 + ordinal)
        BG.stop(native)
        require(row["run_count"] == 2 + ordinal, "Retirement retried or started duplicate work")
        qualify(native, f"remaining-{1 - ordinal}", recalls, 1 - ordinal)
    head = native.call("head", "knn")
    require(
        not SEG.segments(head)
        and head["reference"]["dimension"] == 8
        and RW.active_rows(native) == 32
        and not RW.intents(native)
        and not native.call("build-status"),
        "Last B retirement lost the typed definition, charged retired vectors or allocated ANN build artifacts",
    )
    native.call("retire", SEG.generation(first), 6, expected="selected generation is not ready")
    native.sql(
        "CALL ducklake_set_option('lake','data_inlining_row_limit',0,table_name=>'items');DELETE FROM lake.items"
    )
    core, rows = qualify(native, "empty", recalls, 0)
    require(
        not rows and core["diagnostics"]["native_capture"]["binary_source_count"] == 0,
        "Empty table retained fake sources",
    )
    native.sql(
        "CALL ducklake_set_option('lake','data_inlining_row_limit',100,table_name=>'items');"
        "INSERT INTO lake.items VALUES(200500,[133.03125,4,2,0,1,8,7,16],NULL,'实时-零B')"
    )
    core, _ = qualify(native, "T-only", recalls, 0)
    require(core["rows"][0]["id"] == 200500, "T-only zero-B search lost its only row")
    native.call("capture", "zero", "knn")
    opened = worker.open("zero", native.capture("zero"))
    require(
        opened["generations"] == [] and opened["generation"] is None and opened["artifact_bytes"] == 0,
        "Zero-B lease invented artifacts",
    )
    zero_baseline = worker.search("zero", M.VECTOR)
    capture = CAT.descriptor(native.capture("zero"))
    rejected = CAT.Worker(proof, "rejected", max_views=1)
    workers.append(rejected)
    for name, value in [
        ("format_version", 2),
        ("dimension", 9),
        ("table_uuid", "other-table"),
        ("field_id", 999),
        ("root", "/different-root"),
        ("publication", "x" * 36),
    ]:
        malformed = json.loads(json.dumps(capture))
        malformed["index"]["reference"][name] = value
        path = root / f"invalid-{name}.json"
        path.write_text(json.dumps(malformed))
        rejected.open("invalid", path, expected="manifest")
    malformed = json.loads(json.dumps(capture))
    fields = malformed["capture"]["file_capture"]["business_fields"]
    fields["id"] = fields["category"]
    path = root / "invalid-empty-schema.json"
    path.write_text(json.dumps(malformed))
    rejected.open("invalid", path, expected="distinct field IDs")
    rejected.finish(eof=True)
    SEG.add_f(native, 50000, 133.125)
    qualify(native, "F-T-zero-B", recalls, 0)
    new_reference = root / "artifacts/new-first.json"
    native.call("append", "knn", new_reference, 6)
    rebuilt = native.call("head", "knn")
    require(
        rebuilt["revision"] == 7 and len(SEG.segments(rebuilt)) == 1,
        "Empty plan could not append a new first B",
    )
    require(
        SEG.generation(SEG.segments(rebuilt)[0]) not in M.selection([first, *parts]),
        "Rebuilt plan reused a retired B",
    )
    qualify(native, "new-first-B", recalls, 1)
    MA.drain(native)
    native.sql(
        "SELECT * FROM ducklake_expire_snapshots('lake',older_than=>now());"
        "SELECT * FROM ducklake_cleanup_old_files('lake',cleanup_all=>true)"
    )
    require(
        all(not Path(f["uri"]).exists() for f in M.files([first, *parts])),
        "Expired retired sources survived native GC",
    )
    require(
        worker.search("held", M.VECTOR) == baseline and worker.search("zero", M.VECTOR) == zero_baseline,
        "GC/new publication changed old views",
    )
    wanted = [next(row for row in old_rows if row["id"] == i) for i in [128, 200, 5000, 6000, 200]]
    require(
        worker.fetch("held", [CAT.MIXED.ref(row) for row in wanted])
        == [CAT.MIXED.READ.business(row) for row in wanted],
        "Retired source GC broke delayed typed fetch",
    )
    worker.close_view("held")
    worker.close_view("zero")
    worker.finish(eof=True)
    BG.finish(native)
    checks.extend(
        [
            "native full-file DELETE preserves indexed inputs and visibility until atomic metadata-only retirement",
            "three independent B retire separately with concurrent T/F; unselected generations remain byte-identical",
            "explicit 0-B plans cover empty, T-only and F/T search, filtering, typed rows and a new first B",
            "zero-B plans reject old versions/wrong definitions; no empty ANN, scratch build, compatibility path or fallback is allocated",
            "old indexed and zero-B views preserve search and delayed typed fetch after index and native source GC",
        ]
    )
    return {"fixture_recalls": recalls, "revision": 7}


def stopped(native):
    deadline = time.monotonic() + 60
    while True:
        row = OPT.status(native)
        if not row["enabled"] and not row["running"]:
            return row
        require(time.monotonic() < deadline, f"Retirement did not stop: {row}")
        time.sleep(0.02)


def interruption(driver, root, kind, checks):
    native = CAT.Native(driver, root / "native")
    try:
        first = single(native, root)
        if kind in ["failure", "ack"]:
            native.call("optimizer-resume", "retire_after_commit" if kind == "ack" else "retire_after_stage", "fail")
        else:
            BG.pause(native, "retire_candidate_ready")
            if kind == "cancel":
                native.call("optimizer-cancel")
            else:
                native.call("retire", SEG.generation(first), 1)
            native.call("optimizer-release")
        row = stopped(native)
        committed = kind in ["ack", "race"]
        require(
            native.call("head", "knn")["revision"] == 1 + committed,
            "Retirement failure exposed a partial metadata commit",
        )
        require(
            row["run_count"] == 1 and row["candidate_generation"] == "",
            "Metadata-only retirement built ANN or retried",
        )
        require(
            row["state"] == ("published" if kind == "ack" else "cancelled" if kind == "cancel" else "failed"),
            "Failed retirement was misclassified",
        )
        if kind == "race":
            require(
                native.call("head", "knn")["reference"]["publication"] != row["run_id"],
                "Publication race did not use independent IDs",
            )
        OPT.steady(native, row)
        recalls = []
        qualify(native, "stopped", recalls, 0 if committed else 1)
        native.call("optimizer-resume")
        row = OPT.wait(
            native,
            lambda s: s["compaction_reason"] == "no_segments" and not s["running"],
            "retirement resume",
        )
        require(row["run_count"] == (1 if committed else 2), "Resume replayed committed retirement")
        require(not RW.active_rows(native), "Retirement did not release physical source rows")
        BG.finish(native)
        checks.append(
            f"{kind} keeps data/index publication atomic, proves exact task ownership and requires explicit resume after failure"
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
        before = (
            "retiring"
            if stage in ["optimizer_before_retire", "retire_candidate_ready"]
            else "published" if stage == "optimizer_retired" else "publishing"
        )
        durable = OPT.crash(native, stage, before)
        committed = stage in ["retire_after_commit", "optimizer_retired"]
        head = native.call("head", "knn")
        require(
            head["revision"] == 1 + committed and len(SEG.segments(head)) == 1 - committed,
            "Crash split source/index retirement",
        )
        require(
            RW.active_rows(native) == (0 if committed else 4096),
            "Crash committed only half of source retirement",
        )
        if committed:
            require(
                head["reference"]["publication"] == durable["run_id"],
                "Restart lost retirement publication identity",
            )
        OPT.steady(native, OPT.status(native))
        native.call("optimizer-resume")
        row = OPT.wait(
            native,
            lambda s: s["compaction_reason"] == "no_segments" and not s["running"],
            "post-crash retirement",
        )
        require(
            row["run_count"] == (1 if committed else 2),
            "Crash recovery replayed a committed task or auto retried",
        )
        native.call("maintenance-resume")
        recalls = []
        qualify(native, "restarted-empty", recalls, 0)
        BG.finish(native)
        checks.append(f"SIGKILL at {stage} recovers a complete old/new plan; only explicit resume schedules fresh work")
        return {"fixture_recalls": recalls}
    finally:
        M.stop(native)


def capacity(driver, root, checks):
    native = CAT.Native(driver, root / "native")
    try:
        single(native, root)
        native.sql(
            "CALL ducklake_set_option('lake','data_inlining_row_limit',100,table_name=>'items');"
            "INSERT INTO lake.items SELECT 10000+i,[10000+i,0,0,0,0,0,0,0]::FLOAT[],'B','capacity-F' FROM range(95904)t(i)"
        )
        insert = SEG.sql(
            native, root, "INSERT INTO lake.items VALUES(200000,[133,4,2,0,1,8,7,16],'A','capacity-restored')"
        )
        native.call("sql", insert, expected="physical and inline rows")
        native.call("optimizer-resume")
        BG.published(native, 2)
        BG.stop(native)
        require(RW.active_rows(native) == 95904, "Retirement failed to release all 4,096 B slots")
        native.call("sql", insert)
        recalls = []
        qualify(native, "capacity-restored", recalls, 0)
        BG.finish(native)
        checks.append(
            "under the unchanged 100,000-row/vector ceiling, retire 4,096 masked B rows, retain 95,904 F and admit the next T"
        )
        return {"fixture_recalls": recalls, "released_rows": 4096}
    finally:
        M.stop(native)


def delete_publication_race(driver, root, checks):
    native = CAT.Native(driver, root / "native")
    try:
        artifacts, _, first, frozen = SEG.setup(native, root)
        SEG.add_f(native, 5000)
        native.sql("BEGIN;DELETE FROM lake.items WHERE id>=5000")
        native.call("append", "knn", artifacts / "raced.json", 1)
        native.call(
            "sql",
            SEG.sql(native, root, "COMMIT"),
            expected="cannot be retired without atomic index publication",
        )
        SEG.unchanged(native, first, frozen)
        recalls = []
        qualify(native, "rejected-drop", recalls, 2)
        native.sql(
            "SELECT CASE WHEN count(*)=4224 THEN true ELSE error('concurrent index publication lost F') END FROM lake.items"
        )
        native.finish()
        checks.append(
            "DELETE staged against unindexed F cannot drop sources indexed by a concurrent publisher; metadata commit rejects the stale drop"
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
        cases = [("capacity", capacity), ("delete-publication-race", delete_publication_race)]
        cases += [
            (kind, lambda d, r, c, k=kind: interruption(d, r, k, c)) for kind in ["failure", "cancel", "ack", "race"]
        ]
        cases += [
            ("crash-" + stage, lambda d, r, c, s=stage: crash(d, r, s, c))
            for stage in [
                "optimizer_before_retire",
                "retire_candidate_ready",
                "retire_before_commit",
                "retire_after_stage",
                "retire_after_commit",
                "optimizer_retired",
            ]
        ]
        for name, run in cases:
            directory = root / name
            directory.mkdir()
            report["cases"].append({"name": name, **run(driver, directory, report["checks"])})
        require(not list(private.iterdir()), "Empty retirement proof leaked private leases")
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
