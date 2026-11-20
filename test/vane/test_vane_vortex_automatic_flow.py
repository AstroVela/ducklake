#!/usr/bin/env python3
"""Qualify managed writes with automatic indexing, compaction and source GC."""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import time

SPEC = importlib.util.spec_from_file_location(
    "keyed_proof", Path(__file__).with_name("test_vane_vortex_keyed_writes.py")
)
assert SPEC is not None and SPEC.loader is not None
KEYED = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(KEYED)
BASE, WRITE = KEYED.BASE, KEYED.WRITE
query, require, quote = BASE.query, BASE.require, BASE.quote
META = "__ducklake_metadata_lake."


def row(key, epoch):
    return {
        "id": key,
        "embedding": [128 + key / 16 + (epoch % 3) / 64, (key * 4) % 12, (key * 5) % 13, 0, 1, 8, 7, 16],
        "category": "B" if (key + epoch) % 4 == 0 else "A",
        "payload": None if (key + epoch) % 17 == 0 else f"自动-{epoch}-{key}-'\x00",
    }


def configure(connection, root):
    connection.execute("SELECT * FROM ducklake_initialize_vortex_writes('lake')")
    KEYED.manage(connection)
    connection.execute(
        "SELECT * FROM ducklake_enable_vortex_optimizer('lake','main','items','knn',"
        + quote(root / "indexes")
        + ",8,128,4194304,128,67108864)"
    )
    connection.execute(
        "SELECT * FROM ducklake_set_vortex_write_budget('lake','main','items','knn',256,4194304,1024,67108864)"
    )
    connection.execute(
        "SELECT * FROM ducklake_set_vortex_data_compaction_policy('lake','main','items','knn',true,32,25)"
    )
    connection.execute("SELECT * FROM ducklake_set_vortex_source_retention('lake',true,2,0)")
    connection.execute("SELECT * FROM ducklake_enable_vortex_maintenance('lake',2,67108864)")


def healthy(connection):
    optimizer = BASE.status(connection)
    maintenance = query(connection, "SELECT * FROM ducklake_vortex_maintenance_status('lake')")[0]
    require(optimizer["state"] != "failed" and optimizer["enabled"], f"Optimizer stopped: {optimizer}")
    require(maintenance["state"] != "failed" and maintenance["enabled"], f"Maintenance stopped: {maintenance}")
    return optimizer, maintenance


def settle(connection, root, revision, max_retained_rows=2048):
    deadline = time.monotonic() + 90
    while True:
        optimizer, maintenance = healthy(connection)
        source = query(connection, "SELECT * FROM ducklake_vortex_source_retention_status('lake')")[0]
        head = BASE.head(connection)
        covered = {
            file["id"]
            for segment in head["reference"]["segments"]
            for file in segment["reference"]["snapshot"]["files"]
        }
        active = {
            entry["data_file_id"]
            for entry in query(
                connection, "SELECT data_file_id FROM " + META + "ducklake_data_file WHERE end_snapshot IS NULL"
            )
        }
        inline = query(
            connection, "SELECT count(*) AS n FROM lake.items WHERE starts_with(filename,'ducklake_inlined_data_')"
        )[0]["n"]
        builds = query(connection, "SELECT count(*) AS n FROM " + META + "ducklake_vortex_builds_v1")[0]["n"]
        retired = query(
            connection, "SELECT count(*) AS n FROM " + META + "ducklake_vortex_generations_v1 WHERE state='retired'"
        )[0]["n"]
        if (
            head["revision"] >= revision
            and not optimizer["running"]
            and not maintenance["running"]
            and optimizer["inline_rows"] == optimizer["unindexed_rows"] == 0
            and optimizer["compaction_reason"] != "eligible"
            and not inline
            and active == covered
            and not builds
            and not retired
            and maintenance["remaining_builds"] == maintenance["remaining_generations"] == 0
            and source["eligible_snapshots"] == source["scheduled_files"] == source["pending_deletions"] == 0
        ):
            return head, source, footprint(connection, root, head, max_retained_rows)
        require(
            time.monotonic() < deadline,
            f"Automatic pipeline did not settle: {optimizer}, {maintenance}, {source}, inline={inline}, active={active}, covered={covered}",
        )
        time.sleep(0.03)


def footprint(connection, root, head, max_retained_rows=2048):
    inventory = query(
        connection,
        "SELECT coalesce((SELECT sum(file_size_bytes) FROM "
        + META
        + "ducklake_data_file),0)+coalesce((SELECT sum(file_size_bytes) FROM "
        + META
        + "ducklake_delete_file),0) AS bytes",
    )[0]["bytes"]
    actual = sum(path.stat().st_size for path in (root / "data").rglob("*") if path.is_file())
    require(actual == inventory, f"Managed source bytes did not converge: {actual} != {inventory}")
    generations = query(
        connection,
        "SELECT index_root,generation,artifact_bytes FROM "
        + META
        + "ducklake_vortex_generations_v1 WHERE state='active'",
    )
    directories = {Path(entry["index_root"]) / entry["generation"] for entry in generations}
    wanted = {
        Path(segment["root"]) / segment["reference"]["generation"]["generation"]
        for segment in head["reference"]["segments"]
    }
    require(
        directories == wanted and len(directories) <= 3, "Active generation ownership differs from the published index"
    )
    require(
        set((root / "indexes").glob("generation-*")) == wanted, "Retired generation directories survived automatic GC"
    )
    require(not list((root / "indexes").glob(".build-*")), "Registered build scratch survived automatic GC")
    index_bytes = 0
    for entry in generations:
        directory = Path(entry["index_root"]) / entry["generation"]
        length = sum(path.stat().st_size for path in directory.rglob("*") if path.is_file())
        require(
            length == entry["artifact_bytes"] + (directory / "manifest.json").stat().st_size,
            "Generation disk bytes differ from its authenticated artifact inventory",
        )
        index_bytes += length
    require(sum(entry["artifact_bytes"] for entry in generations) <= 67108864, "Index artifact budget expanded")
    retained_rows = query(connection, "SELECT coalesce(sum(record_count),0) AS n FROM " + META + "ducklake_data_file")[
        0
    ]["n"]
    require(
        retained_rows <= max_retained_rows,
        f"Retained physical source rows grew beyond the bounded fixture window: {retained_rows}",
    )
    references = list((root / "indexes").glob("*.json"))
    return {
        "source_bytes": actual,
        "index_bytes_including_manifests": index_bytes,
        "retained_physical_rows": retained_rows,
        "request_receipts": query(connection, "SELECT count(*) AS n FROM " + WRITE.REQUESTS)[0]["n"],
        "metadata_bytes": (root / "metadata.ducklake").stat().st_size,
        "wal_bytes": (
            (root / "metadata.ducklake.wal").stat().st_size if (root / "metadata.ducklake.wal").exists() else 0
        ),
        "caller_reference_files": len(references),
        "caller_reference_bytes": sum(path.stat().st_size for path in references),
    }


def submit(connection, request, rows, deletes, retries):
    deadline = time.monotonic() + 60
    while True:
        try:
            return KEYED.mutate(connection, request, rows, deletes)
        except Exception as error:
            message = str(error)
            # The client resubmits the same request only for explicit conflict/backpressure.
            require(
                "conflict" in message.lower() or "RESOURCE_EXHAUSTED" in message, f"Unexpected write error: {error}"
            )
            retries.append({"request_id": request, "error": message})
            healthy(connection)
            require(time.monotonic() < deadline, f"Write never regained admission: {request}: {error}")
            time.sleep(0.02)


def current_stamp(connection):
    [stamp] = query(
        connection,
        "SELECT (SELECT max(snapshot_id) FROM "
        + META
        + "ducklake_snapshot) AS snapshot_id,revision FROM "
        + META
        + "ducklake_vortex_indexes_v1 WHERE index_name COLLATE C='knn'",
    )
    return stamp


def qualify(connection, model, label, recalls):
    expected = [model[key] for key in sorted(model)]
    require(KEYED.business_rows(connection) == expected, f"Current logical state diverged: {label}")
    deadline = time.monotonic() + 90
    while True:
        before = current_stamp(connection)
        oracle = BASE.raw(connection)
        results = [
            (category, query(connection, BASE.SEARCH.search_sql(BASE.VECTOR, category=category)))
            for category in [None, "A", "B"]
        ]
        results.append((None, query(connection, "EXECUTE nearest")))
        if current_stamp(connection) == before:
            break
        require(time.monotonic() < deadline, f"Automatic query capture did not stabilize: {label}")
    for category, rows in results:
        recalls.append(BASE.SEARCH.qualify({"rows": rows}, oracle, BASE.VECTOR, category))
    return dict(phase=label, rows=len(oracle), **before)


def stop_workers(connection):
    connection.execute("SELECT * FROM ducklake_cancel_vortex_optimizer('lake','main','items','knn')")
    connection.execute("SELECT * FROM ducklake_cancel_vortex_maintenance('lake')")
    deadline = time.monotonic() + 60
    while (
        BASE.status(connection)["running"]
        or query(connection, "SELECT * FROM ducklake_vortex_maintenance_status('lake')")[0]["running"]
    ):
        require(time.monotonic() < deadline, "Automatic workers did not stop after drain")
        time.sleep(0.02)


def replay(connection, receipts, model):
    before = current_stamp(connection)
    expected = [model[key] for key in sorted(model)]
    require(KEYED.business_rows(connection) == expected, "Rows diverged before exact receipt replay")
    for request, rows, deletes, receipt in receipts:
        require(KEYED.mutate(connection, request, rows, deletes) == receipt, f"Retry changed receipt: {request}")
        require(
            current_stamp(connection) == before and KEYED.business_rows(connection) == expected,
            f"Retry changed the snapshot, index or current rows: {request}",
        )


def segment_files(segment):
    return {item["id"]: item for item in segment["reference"]["snapshot"]["files"]}


def covered_files(head):
    return {
        file_id: Path(item["uri"])
        for segment in head["reference"]["segments"]
        for file_id, item in segment_files(segment).items()
    }


def pending_intents(connection):
    return query(
        connection,
        "SELECT data_file_id,path,path_is_relative,file_bytes,device,inode,digest FROM "
        + META
        + "ducklake_vortex_source_deletions_v1",
    )


def frozen_ties(root):
    case = root / "ties"
    (case / "indexes").mkdir(mode=0o700, parents=True)
    connection = BASE.connect(case, root / "extensions", create=True)
    try:
        connection.execute("SELECT * FROM ducklake_initialize_vortex_writes('lake')")
        KEYED.manage(connection)
        b_rows = [dict(row(key, 0), embedding=[32 + key - 1000, 0, 0, 0, 0, 0, 0, 0]) for key in range(1000, 1127)]
        b_rows.append(dict(row(900, 0), embedding=[1, 0.0001, 0, 0, 0, 0, 0, 0]))
        KEYED.mutate(connection, "tie-B", b_rows)
        connection.execute("CALL ducklake_flush_inlined_data('lake',table_name=>'items')")
        connection.execute(
            "SELECT * FROM ducklake_append_vortex_index_segment('lake','main','items','knn',"
            + quote(case / "indexes" / "tie-B.json")
            + ",8,1)"
        )
        KEYED.mutate(connection, "tie-F", [dict(row(60, 0), embedding=[1, 0, 0, 0, 0, 0, 0, 0])])
        connection.execute("CALL ducklake_flush_inlined_data('lake',table_name=>'items')")
        KEYED.mutate(connection, "tie-T", [dict(row(-1, 0), embedding=[1, 0.0002, 0, 0, 0, 0, 0, 0])])

        oracle = BASE.raw(connection)
        by_id = {entry["id"]: entry for entry in oracle}
        indexed = covered_files(BASE.head(connection))
        require(
            by_id[900]["file_id"] in indexed and by_id[60]["file_id"] not in indexed and "logical_row_id" in by_id[-1],
            "Frozen ties did not put one equal-score key in each of B, F and T",
        )
        vector = [0] * 8
        for k in (2, 3):
            results = query(connection, BASE.SEARCH.search_sql(vector, k=k))
            require(
                [hit["id"] for hit in results] == [900, 60, -1][:k] and all(hit["distance"] == 1.0 for hit in results),
                "Float32 B/F/T ties did not follow physical addresses",
            )
            require(BASE.SEARCH.qualify({"rows": results}, oracle, vector, k=k) == 1.0, "Frozen tie recall changed")
        return {"route_order": ["B", "F", "T"], "top_k_checked": [2, 3], "minimum_recall": 1.0}
    finally:
        connection.close()


def exercise(root, rounds, report):
    (root / "indexes").mkdir()
    connection = BASE.connect(root, root / "extensions", create=True)
    model, receipts, recalls, phases, samples, retries = {}, [], [], [], [], []
    try:
        configure(connection, root)
        connection.execute("PREPARE nearest AS " + BASE.SEARCH.search_sql(BASE.VECTOR))
        for wave in range(4):
            rows = [row(key, 0) for key in range(wave * 128, (wave + 1) * 128)]
            previous = BASE.head(connection)
            if wave == 3:
                first, second, retained = sorted(
                    previous["reference"]["segments"],
                    key=lambda segment: (
                        sum(item["row_count"] for item in segment_files(segment).values()),
                        segment["reference"]["generation"]["generation"],
                    ),
                )
                unchanged = BASE.signature(retained)
            request = f"seed-{wave}"
            receipt = submit(connection, request, rows, [], retries)
            receipts.append((request, rows, [], receipt))
            model.update((entry["id"], entry) for entry in rows)
            head, _, sample = settle(connection, root, previous["revision"] + 1)
            require(
                len(head["reference"]["segments"]) == min(wave + 1, 3),
                "Automatic append/merge did not maintain its segment bound",
            )
            if wave == 3:
                segments = head["reference"]["segments"]
                packed = segment_files(first) | segment_files(second)
                fresh = {item["file_id"] for item in BASE.raw(connection) if item["id"] >= 384}
                require(
                    retained in segments and BASE.signature(retained) == unchanged,
                    "Automatic selective merge changed the descriptor or bytes of the unselected B",
                )
                require(
                    any(segment_files(segment) == packed for segment in segments),
                    "Automatic merge did not use the exact two smallest sealed inputs",
                )
                require(
                    len(fresh) == 1
                    and fresh.isdisjoint(covered_files(previous))
                    and any(segment_files(segment).keys() == fresh for segment in segments)
                    and head["revision"] == previous["revision"] + 2,
                    "Fourth growth wave did not publish its F file as a separate B after the merge",
                )
            phases.append(qualify(connection, model, request, recalls))
            samples.append(dict(phase=request, **sample))

        next_key = 512
        last_source = query(connection, "SELECT * FROM ducklake_vortex_source_retention_status('lake')")[0]
        pending_retirements = set()
        for wave in range(rounds):
            head = BASE.head(connection)
            before_files = covered_files(head)
            locations = query(connection, "SELECT id,filename FROM lake.items ORDER BY id")
            largest = max(
                head["reference"]["segments"],
                key=lambda segment: sum(entry["row_count"] for entry in segment["reference"]["snapshot"]["files"]),
            )
            paths = {os.path.normpath(entry["uri"]) for entry in largest["reference"]["snapshot"]["files"]}
            selected = [entry["id"] for entry in locations if os.path.normpath(entry["filename"]) in paths]
            require(len(selected) >= 96, "Automatic fixture lost its bounded live B input")
            updated = selected[:96]
            removed = [key for key in sorted(model) if key not in set(updated)][:32]
            added = list(range(next_key, next_key + 32))
            next_key += 32
            rows = [row(key, wave + 1) for key in updated + added]
            request = f"wave-{wave}"
            receipt = submit(connection, request, rows, removed, retries)
            receipts.append((request, rows, removed, receipt))
            for key in removed:
                del model[key]
            model.update((entry["id"], entry) for entry in rows)
            require(len(model) == 512, "Rolling fixture changed its live-data volume")
            phases.append(qualify(connection, model, request + "-committed", recalls))
            after, source, sample = settle(connection, root, head["revision"] + 1)
            retired = before_files.keys() - covered_files(after).keys()
            require(retired, f"Automatic rewrite retired no previous source: {request}")
            pending_retirements.update(before_files[file_id] for file_id in retired)
            reclaimed = {path for path in pending_retirements if not path.exists()}
            require(
                reclaimed and source["collected_files"] > last_source["collected_files"],
                f"Automatic GC reclaimed no retired source after {request}",
            )
            pending_retirements.difference_update(reclaimed)
            last_source = source
            phases.append(qualify(connection, model, request + "-drained", recalls))
            samples.append(
                dict(phase=request, retired_sources=len(retired), reclaimed_sources=len(reclaimed), **sample)
            )
            print(f"Automatic wave settled: {wave + 1}/{rounds}", flush=True)

        stop_workers(connection)
        replay(connection, receipts, model)
        require(
            query(
                connection,
                "SELECT count(*) AS n FROM "
                + META
                + "ducklake_snapshot WHERE snapshot_id="
                + str(receipts[0][3]["snapshot_id"]),
            )
            == [{"n": 0}],
            "Old receipt replay did not cover an expired snapshot",
        )
        require(
            query(connection, "SELECT count(*) AS n FROM " + WRITE.REQUESTS) == [{"n": len(receipts)}],
            "Retries created or lost request receipts",
        )
        report.update(
            phases=phases, fixture_recalls=recalls, minimum_recall=min(recalls), samples=samples, client_retries=retries
        )
    finally:
        connection.close()


def recover(case, stage):
    connection = BASE.connect(case, case.parent / "extensions")
    try:
        plans = {entry["request_id"]: entry for entry in json.loads((case / "plans.json").read_text())}
        acknowledged = json.loads((case / "acknowledged.json").read_text())
        checkpoint = json.loads((case / "checkpoint-state.json").read_text())
        committed = checkpoint["receipts"]
        checkpoint_by_id = {receipt["request_id"]: receipt for receipt in committed}
        recorded = query(connection, "SELECT * FROM " + WRITE.REQUESTS + " ORDER BY snapshot_id")
        require(recorded == committed, "Recovery changed the exact pre-crash receipt ledger")
        require(
            all(checkpoint_by_id.get(receipt["request_id"]) == receipt for receipt in acknowledged),
            "A receipt acknowledged before the crash is missing or different",
        )
        model, receipts = {}, []
        for receipt in committed:
            request = receipt["request_id"]
            require(request in plans, "Recovered an unknown mutation")
            plan = plans[request]
            for key in plan["deletes"]:
                model.pop(key, None)
            model.update((entry["id"], entry) for entry in plan["rows"])
            receipts.append((request, plan["rows"], plan["deletes"], receipt))
        require(
            KEYED.business_rows(connection) == [model[key] for key in sorted(model)],
            "SIGKILL separated committed receipts and logical versions",
        )

        before = BASE.head(connection)
        require(
            before == {"revision": checkpoint["revision"], "reference": json.loads(checkpoint["reference"])},
            "Recovery changed the exact checkpoint publication",
        )
        intents = pending_intents(connection)
        require(intents == checkpoint["source_intents"], "Recovery changed the exact checkpoint deletion intents")
        segments = before["reference"]["segments"]
        prior_signature, pending_file = None, None
        if stage in {"ack", "before_commit", "after_commit"}:
            require(
                len(recorded) == 1
                and recorded[0]["request_id"] == "seed-0"
                and recorded[0]["row_count"] == len(model) == 128,
                f"Initial mutation receipt or rows were lost: {stage}",
            )
            if stage == "after_commit":
                require(before["revision"] == 2 and len(segments) == 1, "Committed first B was not recovered")
                prior_signature = BASE.signature(segments[0])
                require(prior_signature, "Recovered B has no generation artifacts")
            else:
                require(before["revision"] == 1 and not segments, f"Recovered an uncommitted first B: {stage}")
            if stage == "ack":
                require(not acknowledged, "Lost-acknowledgement fixture recorded a response")
        else:
            require(len(intents) == 1, "Source deletion checkpoint did not retain one durable intent")
            intent = intents[0]
            pending_file = Path(intent["path"])
            if intent["path_is_relative"]:
                pending_file = case / "data" / pending_file
            require(pending_file.is_file(), "The file recorded at the source deletion checkpoint is missing")
            stat = pending_file.stat()
            checksum = hashlib.sha256()
            with pending_file.open("rb") as source:
                for block in iter(lambda: source.read(128 * 1024), b""):
                    checksum.update(block)
            digest = "sha256:" + checksum.hexdigest()
            require(
                (stat.st_dev, stat.st_ino, stat.st_size, digest)
                == (intent["device"], intent["inode"], intent["file_bytes"], intent["digest"])
                and intent["data_file_id"] not in covered_files(before),
                "The durable source intent does not identify an existing retired file",
            )

        optimizer = BASE.status(connection)
        maintenance = query(connection, "SELECT * FROM ducklake_vortex_maintenance_status('lake')")[0]
        require(not optimizer["running"] and not maintenance["running"], "Restart implicitly started background work")
        time.sleep(0.15)
        require(BASE.head(connection) == before, "Restart published work before explicit resume")
        connection.execute("SELECT * FROM ducklake_resume_vortex_maintenance('lake')")
        connection.execute("SELECT * FROM ducklake_resume_vortex_optimizer('lake','main','items','knn')")
        after, _, _ = settle(connection, case, max(2, before["revision"]))
        if stage == "after_commit":
            require(
                after == before and BASE.signature(after["reference"]["segments"][0]) == prior_signature,
                "Resume rebuilt or changed the committed first B",
            )
        if stage in {"ack", "before_commit"}:
            require(
                after["revision"] == 2 and len(after["reference"]["segments"]) == 1,
                f"Resume did not publish exactly one first B: {stage}",
            )
        if pending_file is not None:
            require(
                not pending_file.exists() and not pending_intents(connection),
                "Resume did not finish the exact pending source deletion",
            )

        stop_workers(connection)
        replay(connection, receipts, model)
        query_vector = [-1, 0]
        BASE.SEARCH.qualify(
            {"rows": query(connection, BASE.SEARCH.search_sql(query_vector))}, BASE.raw(connection), query_vector
        )
    finally:
        connection.close()


def native_cases(driver, root, report):
    cases = []
    for mode in [
        "automatic-live",
        "automatic-backpressure",
        "automatic-flush-serialization",
        "automatic-rewrite-serialization",
        "automatic-replan-after-writer",
        "automatic-cancel-writer-wait",
        "automatic-reject-rebound-catalog",
        "automatic-reject-rebound-build",
        "automatic-reject-rebound-publication",
        "automatic-reject-nonlake-capture",
    ]:
        case = root / mode
        case.mkdir(mode=0o700)
        with (case / "driver.log").open("w") as log:
            result = subprocess.run([str(driver), str(case), mode], stdout=log, stderr=subprocess.STDOUT, timeout=180)
        require(result.returncode == 0, f"Native {mode} failed: {(case / 'driver.log').read_text()}")
        cases.append(mode)
        print(f"Automatic native case passed: {mode}", flush=True)
    for stage in ["ack", "before_commit", "after_commit", "source_delete_intent_committed"]:
        case = root / ("kill-" + stage)
        case.mkdir(mode=0o700)
        with (case / "driver.log").open("w") as log:
            child = subprocess.Popen(
                [str(driver), str(case), "automatic-pause-" + stage], stdout=log, stderr=subprocess.STDOUT
            )
            try:
                deadline = time.monotonic() + 90
                while not (case / "checkpoint").exists():
                    require(
                        child.poll() is None,
                        "Automatic worker exited before checkpoint: " + (case / "driver.log").read_text(),
                    )
                    require(time.monotonic() < deadline, "Automatic worker checkpoint timed out")
                    time.sleep(0.02)
                wal = case / "metadata.ducklake.wal"
                require(wal.is_file() and wal.stat().st_size > 0, "Automatic crash case has no native WAL")
                child.kill()
                child.wait(timeout=10)
                require(child.returncode == -9, "Automatic recovery did not exercise SIGKILL")
            finally:
                if child.poll() is None:
                    child.kill()
                    child.wait(timeout=10)
        with (case / "recovery.log").open("w") as log:
            result = subprocess.run(
                [sys.executable, "-I", str(Path(__file__).resolve()), "--recover-case", str(case), "--stage", stage],
                stdout=log,
                stderr=subprocess.STDOUT,
                timeout=180,
            )
        require(result.returncode == 0, f"Automatic recovery {stage} failed: {(case / 'recovery.log').read_text()}")
        cases.append(stage + ": SIGKILL + native WAL + explicit resume + exact retry")
        print(f"Automatic recovery passed: {stage}", flush=True)
    report["native_cases"] = cases


def run_public(root, rounds):
    log_path = root / "public.log"
    with log_path.open("w") as log:
        try:
            result = subprocess.run(
                [
                    sys.executable,
                    "-I",
                    str(Path(__file__).resolve()),
                    "--public-case",
                    str(root),
                    "--rounds",
                    str(rounds),
                ],
                stdout=log,
                stderr=subprocess.STDOUT,
                timeout=180 * ((rounds + 7) // 8),
            )
        except subprocess.TimeoutExpired as error:
            raise RuntimeError(f"Public automatic-flow proof timed out. Log is {log_path}") from error
    require(result.returncode == 0, f"Public automatic-flow proof failed: {log_path.read_text()}")
    return json.loads((root / "public-report.json").read_text())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--driver", type=Path)
    destination = parser.add_mutually_exclusive_group(required=True)
    destination.add_argument("--output-dir", type=Path)
    destination.add_argument("--recover-case", type=Path)
    destination.add_argument("--public-case", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--stage")
    parser.add_argument("--rounds", type=int, default=8)
    args = parser.parse_args()
    require(os.environ.get("VANE_RUNNER") == "local-fast", "Set VANE_RUNNER=local-fast explicitly")
    if args.recover_case:
        require(args.stage is not None, "Recovery requires an expected checkpoint")
        recover(args.recover_case.resolve(), args.stage)
        return
    if args.public_case:
        root = args.public_case.resolve()
        report = {}
        exercise(root, args.rounds, report)
        report["frozen_ties"] = frozen_ties(root)
        (root / "public-report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
        return
    require(args.driver is not None and 4 <= args.rounds <= 256, "Set --driver and 4..256 rounds")
    root = args.output_dir.resolve()
    root.mkdir(mode=0o700, parents=True)
    report = {"status": "running", "rounds": args.rounds}
    started = time.monotonic()
    try:
        report.update(run_public(root, args.rounds))
        native_cases(args.driver.resolve(), root, report)
        report["status"] = "passed"
    except Exception as error:
        report.update(status="failed", error=str(error))
        raise
    finally:
        report["seconds"] = time.monotonic() - started
        (root / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
