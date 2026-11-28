#!/usr/bin/env python3
"""R7 local native Add/Delete, sealed checkpoint recovery and frozen ReadViews."""

from copy import deepcopy
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import selectors
import signal
import struct
import subprocess
import sys
import time
from unittest.mock import Mock, call, patch

HERE = Path(__file__).resolve().parent


def load(name, filename):
    spec = importlib.util.spec_from_file_location(name, HERE / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


K = load("r7_keyed", "test_vane_vortex_keyed_writes.py")
F = load("r7_fixture", "vortex_spfresh_update_fixture.py")
O = load("r7_oracle", "vortex_spfresh_update_oracle.py")
PIN = load("r7_affinity", "test_vane_vortex_source_gc_affinity.py")
BASE, WRITE = K.BASE, K.WRITE
require, query, quote = BASE.require, BASE.query, BASE.quote
RULE = {
    "schema": 1,
    "scope": "Local component experiment, actual SPFresh AddIndexSPFresh/DeleteIndex; production SQL still uses spfresh.static.",
    "profile": (
        "8D Float32 squared L2 with 64 heads, 64 search heads, 12 pages per posting and 4 replicas. "
        "Head selection, head build and SSD build/search each configure one thread. The seed uses 20 posting writers."
    ),
    "updates": "Synchronous posting updates; LIRE split/merge/reassign and sustained performance are not qualified.",
    "source": "Committed DuckLake managed mutations, flush, actual native ReadView snapshot and physical addresses.",
    "versions": "One private mutable candidate, explicit close/checkpoint/seal, expected-revision publication; no adoption/retry.",
    "limits": {
        "physical_rows": 100000,
        "source_bytes": F.LIMIT,
        "vector_bytes": F.LIMIT,
        "artifact_bytes": F.LIMIT,
        "manifest_bytes": F.MIB,
        "mutation_ops": 1000,
        "mutation_bytes": 4 * F.MIB,
        "gc_objects": 16,
        "gc_bytes": F.LIMIT,
    },
    "acceptance": "Independent historical SQL/physical Vortex/IEEE/rank checks; recall >=0.8; deterministic handshakes, no latency gate.",
    "deferred": [
        "R6",
        "R4 stable-load tests",
        "native SQL dynamic-provider registration",
        "automatic incremental maintenance",
        "LIRE rebalance",
        "power-loss qualification",
    ],
}


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def captured(connection, report):
    result = query(connection, "SELECT * FROM ducklake_vortex_read_view('lake','main','items',8,67108864,100000)")[0]
    admission = json.loads(result["admission"])
    file = admission["capture"]["file_capture"]
    require(file["snapshot_id"] == result["snapshot_id"], "Actual query snapshot differs")
    value = {"file_capture": file, "rows": json.loads(result["rows"])}
    expected = O.capture(connection, value)
    require(len(expected) == result["visible_rows"], "Native visible count differs")
    report["captures"].append(
        {
            "snapshot_id": result["snapshot_id"],
            "visible_rows": result["visible_rows"],
            "inline_rows": result["inline_rows"],
            "source_bytes": result["source_bytes"],
            "sha256": F.sha(F.encoded(value)),
        }
    )
    return value


def rejected(report, name, fn, code):
    try:
        fn()
    except ValueError as error:
        require(str(error) == code, f"Wrong rejection: {error}")
    else:
        raise AssertionError("Expected rejection: " + name)
    report["rejections"].append({"name": name, "code": code})


def qualify(report, name, view, capture):
    before = F.inventory(view.root / "native")
    for offset in (0.0, 1.0625, 127.125, 255.5):
        vector = [offset, *([0.0] * 7)]
        for k in (1, 3, 10):
            hits = view.query(vector, k)
            recall = O.qualify(hits, capture["rows"], vector, k)
            report["queries"].append(
                {
                    "phase": name,
                    "query": offset,
                    "k": k,
                    "recall": recall,
                    "snapshot_id": capture["file_capture"]["snapshot_id"],
                    "ids": [h["row"]["id"] for h in hits],
                }
            )
    require(F.inventory(view.root / "native") == before, "Native read-only query wrote checkpoint files")


def checkpoint_child(config):
    def stop(stage, **evidence):
        if stage == config["stage"]:
            print(json.dumps({"event": stage, "affinity": sorted(os.sched_getaffinity(0)), **evidence}), flush=True)
            require(sys.stdin.readline().strip() == "continue", "Fault handshake lost")

    root = Path(config["root"])
    catalog = F.Catalog(root)
    if config["operation"] == "ducklake":
        lake = Path(config["lake"])
        c = BASE.connect(lake, lake / "extensions")
        receipt = K.mutate(c, "after-crash", config["rows"], [3])
        stop("after_ducklake_commit", receipt=receipt)
        c.close()
    elif config["operation"] == "advance":
        F.advance(
            Path(config["probe"]),
            root / config["source"],
            root / config["target"],
            config["capture"],
            Path(config["log"]),
            stop,
        )
    else:
        catalog.publish(config["target"], config["expected"], stop)


def crash(report, root, operation, stage, **values):
    config = {"root": str(root), "operation": operation, "stage": stage, **values}
    with (root.parent / (stage + ".log")).open("wb") as log:
        process = subprocess.Popen(
            [sys.executable, "-I", str(Path(__file__).resolve()), "--fault-child"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=log,
            text=True,
            start_new_session=True,
        )
        try:
            process.stdin.write(json.dumps(config) + "\n")
            process.stdin.flush()
            with selectors.DefaultSelector() as selector:
                selector.register(process.stdout, selectors.EVENT_READ)
                require(selector.select(90), "Fault handshake deadline")
            event = json.loads(process.stdout.readline())
            receipt = event.pop("receipt") if operation == "ducklake" else None
            require(event == {"event": stage, "affinity": sorted(os.sched_getaffinity(0))}, "Wrong crash boundary")
            require(operation != "ducklake" or isinstance(receipt, dict), "Missing committed receipt")
            os.killpg(process.pid, signal.SIGKILL)
            require(process.wait(timeout=30) == -signal.SIGKILL, "No actual SIGKILL")
            report["faults"].append({"operation": operation, "stage": stage, "exit_code": -9})
        finally:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=30)
    return receipt


def exercise_native_limits(probe, root, source, report):
    reopened = {}
    for deleted in (127, 128, 129, 256):
        target = root / f"native-delete-{deleted}"
        F.clone(source, target)
        log = root / f"native-delete-{deleted}.log"
        worker = F.Worker(probe, "update", target / "native", log)
        try:
            for vid in range(deleted):
                require(worker.call("delete", vid) == {"deleted": True}, "Expected a valid native deletion")
            if deleted == 256:
                rejected(report, "empty-checkpoint", lambda: worker.call("checkpoint"), "NATIVE_OPERATION_FAILED")
                require(worker.process.wait(timeout=30) == 1, "Empty index did not reject cleanly")
            else:
                require(worker.call("checkpoint") == {"saved": True}, "Nonempty checkpoint failed")
                worker.close()
        finally:
            worker.stop()
        if deleted == 256:
            require("SaveIndex:21" in log.read_text(), "Missing EmptyIndex diagnostic")
            continue
        _, view = F.validate_checkpoint(probe, target, "0" * deleted + "1" * (256 - deleted), log, retain_reader=True)
        try:
            reopened[str(deleted)] = view.ready["live"]
            view.close()
        finally:
            view.stop()
    report["native_live_rows_after_deletes"] = reopened

    target = root / "native-posting-overflow"
    F.clone(source, target)
    worker = F.Worker(probe, "update", target / "native", root / "native-posting-overflow.log")
    try:
        require(
            worker.call("add", 1000, *([0.0] * 8000)) == {"ids": list(range(256, 1256))},
            "Insertion below posting capacity failed",
        )
        # Repeated vectors take the same route. 1,329 records of 37 bytes exceed 12 pages.
        rejected(
            report, "posting-capacity", lambda: worker.call("add", 329, *([0.0] * (329 * 8))), "POSTING_CAPACITY"
        )
        require(worker.process.wait(timeout=30) == 1, "Posting overflow did not reject cleanly")
    finally:
        worker.stop()

    target = root / "native-missing-posting"
    F.clone(source, target)
    log = root / "native-missing-posting.log"
    with log.open("wb") as stream:
        subprocess.run(
            [str(probe), "remove-postings", str(target / "native")],
            stdout=stream,
            stderr=subprocess.STDOUT,
            check=True,
            timeout=30,
        )
    rejected(
        report, "missing-posting-on-reopen", lambda: F.Worker(probe, "open", target / "native", log),
        "NATIVE_OPERATION_FAILED",
    )
    # A native query also propagates MultiGet errors when run on the disposable update handle.
    worker = F.Worker(probe, "update", target / "native", log)
    try:
        rejected(
            report, "missing-posting", lambda: worker.call("search", 3, *([0.0] * 8)), "NATIVE_OPERATION_FAILED"
        )
        require(worker.process.wait(timeout=30) == 1, "Missing posting did not reject cleanly")
    finally:
        worker.stop()
    diagnostic = log.read_text()
    require(
        "NotFound" in diagnostic and "R7 rejected: RocksDB posting read failed" in diagnostic,
        "Missing posting did not preserve the read error",
    )


def exercise_manifests(root, source, head, report):
    catalog = F.Catalog(root / "manifest-catalog")
    F.clone(source, catalog.root / "g1")
    head_path, seal_path = catalog.root / "head.json", catalog.root / "g1/seal.json"
    watched = {head_path: [], seal_path: []}
    original_open = Path.open

    def track_reads(path, *args, **kwargs):
        stream = original_open(path, *args, **kwargs)
        if path in watched:
            stream.read = Mock(wraps=stream.read)
            watched[path].append(stream.read)
        return stream

    with patch.object(Path, "open", track_reads):
        require(catalog.publish("g1", 0) == head, "Private manifest publication differs")
        require(len(watched[seal_path]) == 1, "Publication reopened its seal")
        require(catalog.head() == head, "Private head verification differs")
        require(len(watched[head_path]) == 1 and len(watched[seal_path]) == 2, "Head reopened its manifest")
        require(
            all(read.call_args_list == [call(F.MIB + 1)] for reads in watched.values() for read in reads),
            "Manifest content was read without its byte limit",
        )
    for name, path in (("seal", seal_path), ("head", head_path)):
        with path.open("r+b") as stream:
            stream.truncate(F.MIB + 1)
        with patch.object(Path, "open", track_reads):
            rejected(report, f"oversized-{name}-before-read", catalog.head, "MANIFEST_BUDGET")
        require(watched[path][-1].call_count == 0, "Oversized manifest content was read")


def exercise_failed_checkpoints(probe, root, catalog, capture, report):
    head = catalog.head()
    source = catalog.root / head["generation"]
    old_labels = (source / "native/versions.bin").read_bytes()
    require(
        len(F.verify(source)["capture"]["rows"]) != len(capture["rows"]),
        "Stale label control needs a different live-row count",
    )
    target = catalog.root / "label-save-failure"
    log = root / "label-save-failure.log"

    def refuse_label_file(stage):
        if stage == "after_mutation":
            path = target / "native/versions.bin"
            path.unlink()
            path.mkdir()

    rejected(
        report,
        "version-label-save-error",
        lambda: F.advance(probe, source, target, capture, log, refuse_label_file),
        "NATIVE_OPERATION_FAILED",
    )
    require("SaveIndex:3" in log.read_text(), "Missing FailedCreateFile label diagnostic")
    require(not (target / "seal.json").exists(), "Failed label write acquired a seal")

    target = catalog.root / "stale-labels"

    def restore_old_labels(stage):
        if stage == "after_checkpoint":
            (target / "native/versions.bin").write_bytes(old_labels)

    rejected(
        report,
        "stale-labels-on-closed-reopen",
        lambda: F.advance(probe, source, target, capture, root / "stale-labels.log", restore_old_labels),
        "NATIVE_ROW_COUNT",
    )
    require(not (target / "seal.json").exists(), "Native row mismatch acquired a seal")
    require(catalog.head() == head, "Checkpoint failures changed the published head")


def exercise_checkpoint_integrity(probe, root, catalog, capture, report):
    head = catalog.head()
    source = catalog.root / head["generation"]
    previous = F.verify(source)
    require(len(previous["capture"]["rows"]) == len(capture["rows"]), "Stale labels need a balanced update")
    old_labels = (source / "native/versions.bin").read_bytes()
    version_header = struct.calcsize("=iii")
    posting_header = struct.calcsize("=ii")

    def check(name, damage, code, diagnostic=None):
        target = catalog.root / name
        log = root / (name + ".log")

        def after_checkpoint(stage):
            if stage == "after_checkpoint":
                damage(target / "native")

        rejected(report, name, lambda: F.advance(probe, source, target, capture, log, after_checkpoint), code)
        if diagnostic:
            require(diagnostic in log.read_text(), "Checkpoint failure lost its loader diagnostic")
        require(not (target / "seal.json").exists(), "Invalid checkpoint acquired a seal")
        require(catalog.head() == head, "Invalid checkpoint changed the published head")

    def truncate(path, bytes_):
        with path.open("r+b") as stream:
            stream.truncate(bytes_)

    def swap_version_flags(native):
        path = native / "versions.bin"
        data = bytearray(path.read_bytes())
        tombstone = 0xFE
        deleted = data.index(tombstone, version_header)
        live = next(i for i in range(version_header, len(data)) if data[i] != tombstone)
        data[deleted], data[live] = data[live], data[deleted]
        path.write_bytes(data)

    def extra_deleted_vid(native):
        path = native / "versions.bin"
        data = bytearray(path.read_bytes())
        deleted, total = struct.unpack_from("=ii", data)
        struct.pack_into("=ii", data, 0, deleted + 1, total + 1)
        data.append(0xFE)
        path.write_bytes(data)

    def bad_version_flag(native):
        path = native / "versions.bin"
        data = bytearray(path.read_bytes())
        data[data.index(0xFF, version_header)] = 0xFD
        path.write_bytes(data)

    def remove_posting_size(native):
        path = native / "posting-sizes.bin"
        data = bytearray(path.read_bytes())
        rows = struct.unpack_from("=i", data)[0]
        struct.pack_into("=i", data, 0, rows - 1)
        path.write_bytes(data[:-struct.calcsize("=i")])

    def change_posting_size(native):
        path = native / "posting-sizes.bin"
        data = bytearray(path.read_bytes())
        size = struct.unpack_from("=i", data, posting_header)[0]
        struct.pack_into("=i", data, posting_header, size + 1)
        path.write_bytes(data)

    def append_byte(path):
        with path.open("ab") as stream:
            stream.write(b"X")

    check(
        "same-count-stale-labels", lambda native: (native / "versions.bin").write_bytes(old_labels),
        "NATIVE_VID_VISIBILITY",
    )
    check("swapped-vid-labels", swap_version_flags, "NATIVE_VID_VISIBILITY")
    check("extra-deleted-vid", extra_deleted_vid, "NATIVE_VID_VISIBILITY")
    check("invalid-version-flag", bad_version_flag, "NATIVE_OPERATION_FAILED", "Failed to load SPFresh version labels")
    check(
        "truncated-version-labels", lambda native: truncate(native / "versions.bin", version_header),
        "NATIVE_OPERATION_FAILED", "Failed to load SPFresh version labels",
    )
    check(
        "truncated-posting-sizes", lambda native: truncate(native / "posting-sizes.bin", posting_header),
        "NATIVE_OPERATION_FAILED", "Failed to load SPFresh posting sizes",
    )
    check(
        "wrong-posting-head-count", remove_posting_size, "NATIVE_OPERATION_FAILED",
        "SPFresh posting size count does not match the head count",
    )
    check("wrong-posting-length", change_posting_size, "NATIVE_OPERATION_FAILED", "SPFresh posting length differs")
    for name, diagnostic in (("versions.bin", "version labels"), ("posting-sizes.bin", "posting sizes")):
        check(
            "trailing-" + name.split(".")[0], lambda native, name=name: append_byte(native / name),
            "NATIVE_OPERATION_FAILED", "Failed to load SPFresh " + diagnostic,
        )

    start_worker = F.Worker

    def reject_native_reopen(executable, mode, *args, **kwargs):
        require(mode != "open", "Native reopen ran before checking the artifact budget")
        return start_worker(executable, mode, *args, **kwargs)

    with patch.object(F, "Worker", reject_native_reopen):
        check(
            "oversized-native-config-before-reopen", lambda native: truncate(native / "indexloader.ini", F.LIMIT + 1),
            "ARTIFACT_BUDGET",
        )


def exercise_reopen_inventory(probe, root, source, report):
    target = root / "reopen-budget"
    body = F.clone(source, target)
    seal_path = target / "seal.json"
    seal_path.unlink()
    native = target / "native"
    padding = native / "padding.bin"
    remaining = F.LIMIT - sum(entry["size"] for entry in body["files"].values())
    require(remaining > 0, "Checkpoint seed exhausts artifact budget")
    with padding.open("xb") as stream:
        stream.truncate(remaining)

    def seal():
        return F.seal(probe, target, body["capture"], body["mapping"], root / "reopen-budget.log")

    with patch.object(F, "Worker", wraps=F.Worker) as launch:
        sealed = seal()
        at_limit = launch.call_count
        require(at_limit == 1 and launch.call_args.args[1] == "open", "Exact budget did not reopen once")
    require(sum(entry["size"] for entry in sealed["files"].values()) == F.LIMIT, "Wrong exact-budget fixture")
    require(F.verify(target) == sealed, "Exact-budget seal did not verify")
    seal_path.unlink()
    with padding.open("ab") as stream:
        stream.write(b"x")
    with patch.object(F, "Worker", side_effect=AssertionError("Over-budget native reopen")) as launch:
        rejected(report, "total-budget-plus-one", seal, "ARTIFACT_BUDGET")
        launch.assert_not_called()
        over_limit = launch.call_count
    require(not seal_path.exists(), "Over-budget checkpoint acquired a seal")
    report["checkpoint_budget"] = {"limit_bytes": F.LIMIT, "at_limit_reopens": at_limit, "over_limit_reopens": over_limit}

    padding.write_bytes(b"A")
    added = native / "after-reopen.bin"
    close = F.Worker.close
    for name, changed_file in (("added", added), ("same-size-change", padding)):
        def change_after_close(worker):
            close(worker)
            changed_file.write_bytes(b"B")

        with patch.object(F.Worker, "close", change_after_close):
            rejected(report, "reopen-inventory-" + name, seal, "NATIVE_REOPEN_CHANGED")
        require(not seal_path.exists(), "Changed inventory acquired a seal")

    private = root / "reopen-read-view"
    F.clone(source, private)
    start = F.Worker
    workers = []

    def change_after_open(*args, **kwargs):
        worker = start(*args, **kwargs)
        workers.append(worker)
        (private / "native/after-open.bin").write_bytes(b"B")
        return worker

    with patch.object(F, "Worker", change_after_open):
        rejected(
            report, "read-view-inventory-after-open",
            lambda: F.ReadView(probe, private, body, root / "reopen-read-view.log"), "NATIVE_REOPEN_CHANGED",
        )
    require(len(workers) == 1 and workers[0].process.poll() is not None, "Rejected reader still running")
    require(F.verify(source) == body, "Inventory controls changed the seed")


def exercise_oracle(report):
    def row(offset, value):
        return {
            "row_ref": {"kind": "file", "file_id": 1, "row_offset": offset},
            "row": {"id": offset, "embedding": [value, *([0.0] * 7)], "category": None, "payload": None},
        }

    rows = [row(i, x) for i, x in enumerate((0.0, 1.0, 1.0, 1.0))]
    hits = [{**r, "distance": float(i != 0)} for i, r in enumerate(rows)]
    try:
        O.qualify(hits[1:], rows, [0.0] * 8, 3)
    except AssertionError as error:
        require(str(error) == "Native recall is below the existing 0.8 gate", "Wrong cutoff-tie rejection")
    else:
        raise AssertionError("Cutoff ties replaced a strictly closer row")
    require(O.qualify([hits[0], *hits[2:]], rows, [0.0] * 8, 3) == 1.0, "Equivalent cutoff ties failed")
    rounded = row(0, 16777218.0)
    require(
        O.qualify([{**rounded, "distance": 0.0}], [rounded], [16777217.1, *([0.0] * 7)], 1) == 1.0,
        "Oracle scored the query before Float32 rounding",
    )
    report["oracle_controls"] = ["missing-closer-rejected", "equivalent-cutoff-ties", "float32-query-rounding"]


def exercise(args, root, report):
    lake = root / "lake"
    lake.mkdir(mode=0o700)
    (lake / "indexes").mkdir(mode=0o700)
    extensions = lake / "extensions"
    c = BASE.connect(lake, extensions, create=True)
    views = []
    try:
        c.execute("SELECT * FROM ducklake_initialize_vortex_writes('lake')")
        K.manage(c)
        rows = [
            {
                "id": i,
                "embedding": [float(i), *([0.0] * 6), -0.0],
                "category": "A\0" if i % 2 else None,
                "payload": None if i % 3 else "原始-\0-" + str(i),
            }
            for i in range(256)
        ]
        receipt = K.mutate(c, "seed", rows)
        require(K.mutate(c, "seed", rows) == receipt, "DuckLake retry changed")
        c.execute("CALL ducklake_flush_inlined_data('lake',table_name=>'items')")
        revision = BASE.head(c)["revision"]
        c.execute(
            "SELECT * FROM ducklake_append_vortex_index_segment('lake','main','items','knn',"
            + quote(lake / "indexes/seed.json")
            + f",8,{revision})"
        )
        static = deepcopy(BASE.head(c))
        initial = captured(c, report)
        F.validate_capture(initial)
        oversized = deepcopy(initial)
        oversized["file_capture"]["files"][0]["row_count"] = 100001
        rejected(report, "physical-row-budget", lambda: F.validate_capture(oversized), "PHYSICAL_ROW_BUDGET")
        rejected(
            report,
            "manifest-budget",
            lambda: F.write(root / "oversized.json", {"data": "x" * F.MIB}),
            "MANIFEST_BUDGET",
        )
        artifact = root / "oversized-artifact"
        artifact.mkdir(mode=0o700)
        with (artifact / "sparse.bin").open("wb") as stream:
            stream.truncate(F.LIMIT + 1)
        rejected(report, "artifact-budget-before-read", lambda: F.inventory(artifact), "ARTIFACT_BUDGET")
        (artifact / "sparse.bin").unlink()
        artifact.rmdir()
        catalog = F.Catalog(root / "catalog")
        first = F.seed(args.probe, catalog.root / "g1", initial, root / "seed.log")
        head = catalog.publish("g1", 0)
        view = catalog.read(args.probe, root / "view-old", initial, root / "view-old.log")
        views.append(view)
        qualify(report, "seed", view, initial)
        rejected(report, "current-generation-GC", lambda: catalog.collect("g1", 16, F.LIMIT), "REFERENCED")
        exercise_native_limits(args.probe, root, catalog.root / "g1", report)
        exercise_manifests(root, catalog.root / "g1", head, report)
        exercise_reopen_inventory(args.probe, root, catalog.root / "g1", report)
        require(catalog.head() == head, "Private regression changed the published head")

        # Real ordinary DuckLake transaction observes its old logical version.
        ordinary = c.cursor()
        ordinary.execute("BEGIN")
        before = K.business_rows(ordinary)
        replacement = [
            {"id": 0, "embedding": [0.125, *([0.0] * 7)], "category": "B", "payload": "新版本-\0"},
            {"id": 10000, "embedding": [0.25, *([0.0] * 7)], "category": None, "payload": None},
        ]
        changed = K.mutate(c, "replace", replacement, [1])
        require(K.business_rows(ordinary) == before, "Native update changed an old DuckLake transaction")
        require(K.mutate(c, "replace", replacement, [1]) == changed, "Committed mutation replay changed")
        WRITE.rejected(lambda: K.mutate(c, "replace", replacement, [2]), "payload mismatch")
        pending = captured(c, report)
        rejected(report, "T-is-not-index-ready", lambda: F.validate_capture(pending), "UNFLUSHED_ROWS")
        rejected(
            report,
            "index-watermark-mismatch",
            lambda: catalog.read(args.probe, root / "bad-watermark", pending, root / "bad-watermark.log"),
            "INDEX_WATERMARK_MISMATCH",
        )
        require(
            query(c, "SELECT id FROM ducklake_vortex_search('lake','main','items','knn',[0,0,0,0,0,0,0,0]::FLOAT[],1)")
            == [{"id": 0}],
            "Existing B+T path lost committed update",
        )
        require(BASE.head(c) == static, "Logical mutation rebuilt the original static B")
        qualify(report, "old-view-after-DuckLake-commit", view, initial)
        ordinary.execute("COMMIT")
        ordinary.close()
        report["checks"].append("DuckLake transaction/MVCC/receipt chain and normal B+T query remain intact")
        c.execute("CALL ducklake_flush_inlined_data('lake',table_name=>'items')")
        current = captured(c, report)
        F.validate_capture(current)
        exercise_checkpoint_integrity(args.probe, root, catalog, current, report)

        for stage in ("after_mutation", "after_checkpoint", "after_seal"):
            name = "crash-" + stage.replace("_", "-")
            crash(
                report,
                catalog.root,
                "advance",
                stage,
                probe=str(args.probe),
                source="g1",
                target=name,
                capture=current,
                log=str(root / (name + "-native.log")),
            )
            require(F.Catalog(catalog.root).head() == head, "Unpublished candidate changed durable head")
            if stage != "after_seal":
                require(not (catalog.root / name / "seal.json").exists(), "Incomplete candidate acquired a seal")
                try:
                    F.verify(catalog.root / name)
                except ValueError as error:
                    require(str(error) == "GENERATION_INVENTORY", "Wrong incomplete-generation rejection")
                else:
                    raise AssertionError("Incomplete native checkpoint was accepted")
            else:
                candidate = F.verify(catalog.root / name)
                require(candidate["capture"] == current, "Sealed candidate lost target snapshot")
            qualify(report, "old-view-after-" + stage, view, initial)

        second = F.advance(args.probe, catalog.root / "g1", catalog.root / "g2", current, root / "advance.log")
        # In this first native-API experiment no head rebalance is requested.
        for name in first["files"]:
            if name.startswith("HeadIndex/"):
                require(second["files"][name] == first["files"][name], "Add/Delete rebuilt the head index")
        require(len(second["mapping"]) == len(first["mapping"]) + 2, "Native add did not append two physical versions")
        rejected(report, "stale-publication-CAS", lambda: catalog.publish("g2", 99), "PUBLICATION_CONFLICT")
        crash(report, catalog.root, "publish", "before_publish", target="g2", expected=1)
        require(catalog.head() == head, "Unforwarded publication changed head")
        crash(report, catalog.root, "publish", "after_publish", target="g2", expected=1)
        recovered = F.Catalog(catalog.root).head()
        require(recovered["revision"] == 2 and recovered["generation"] == "g2", "Durable publish was not recovered")
        rejected(report, "no-automatic-republish", lambda: catalog.publish("g2", 1), "PUBLICATION_CONFLICT")
        newer = catalog.read(args.probe, root / "view-new", current, root / "view-new.log")
        views.append(newer)
        qualify(report, "updated-and-reopened", newer, current)
        require(newer.query([0.0] * 8, 1)[0]["row"]["id"] == 0, "Updated native index lost new nearest version")
        require(view.query([0.0] * 8, 1)[0]["row"]["embedding"][0] == 0.0, "Old native view lost old vector")
        report["checks"].append("Real native Add/Delete persists across close/reopen; head files are unchanged")

        for count, amount in ((0, F.LIMIT), (17, F.LIMIT), (16, 0), (16, F.LIMIT + 1)):
            rejected(
                report,
                "invalid-GC-budget",
                lambda count=count, amount=amount: catalog.collect("g1", count, amount),
                "GC_BUDGET",
            )
        rejected(report, "small-GC-budget", lambda: catalog.collect("g1", 16, 1), "GC_BUDGET")
        report["gc"] = catalog.collect("g1", 16, F.LIMIT)
        require(not (catalog.root / "g1").exists(), "Canonical generation was not collected")
        qualify(report, "old-view-after-canonical-GC", view, initial)
        qualify(report, "new-view-after-canonical-GC", newer, current)
        report["checks"].append("Private old ReadView remains queryable after original canonical checkpoint GC")

        # A saved caller reference is not allowed to silently query a different UUID.
        wrong = deepcopy(current)
        wrong["file_capture"]["table_uuid"] = "00000000-0000-0000-0000-000000000001"
        rejected(
            report,
            "wrong-table-UUID",
            lambda: catalog.read(args.probe, root / "wrong-uuid", wrong, root / "wrong-uuid.log"),
            "INDEX_WATERMARK_MISMATCH",
        )
        tampered = root / "tampered"
        F.clone(catalog.root / "g2", tampered)
        file = tampered / "native" / "versions.bin"
        blob = bytearray(file.read_bytes())
        blob[-1] ^= 1
        file.write_bytes(blob)
        rejected(report, "native-file-corruption", lambda: F.verify(tampered), "GENERATION_CHANGED")
        for k in (0, 65, True):
            rejected(report, "invalid-query-k", lambda k=k: newer.query([0.0] * 8, k), "QUERY_K")
        for vector in ([0.0], [float("nan")] * 8, [float("inf")] * 8, [1e100] * 8):
            rejected(report, "invalid-query-vector", lambda vector=vector: newer.query(vector, 1), "QUERY_VECTOR")

        # Mutation tests prove the acceptance reference rejects unsafe results.
        vector = [0.0] * 8
        good = newer.query(vector, 3)
        variants = []
        for name in ("duplicate", "wrong-payload", "wrong-distance", "invisible-address"):
            bad = deepcopy(good)
            if name == "duplicate":
                bad[1] = deepcopy(bad[0])
            elif name == "wrong-payload":
                bad[0]["row"]["payload"] = "stale"
            elif name == "wrong-distance":
                bad[0]["distance"] += 100
            else:
                bad[0]["row_ref"]["row_offset"] = 999999
            try:
                O.qualify(bad, current["rows"], vector, 3)
            except AssertionError:
                variants.append(name)
            else:
                raise AssertionError("Independent reference accepted unsafe result: " + name)
        report["unsafe_variants"] = variants
        exercise_oracle(report)

        for item in views:
            item.close()
        views.clear()
        require(BASE.head(c) == static, "R7 experiment mutated production static publication")
        expected_rows = deepcopy(K.business_rows(c))
        c.close()
        c = BASE.connect(lake, extensions)
        require(
            K.business_rows(c) == expected_rows and K.mutate(c, "replace", replacement, [1]) == changed,
            "DuckLake rows or receipts changed across reopen",
        )
        report["checks"].append("Native DuckLake reopen retains business rows, UUID, receipts and static publication")
        reopened = catalog.read(args.probe, root / "view-recovered", current, root / "view-recovered.log")
        views.append(reopened)
        qualify(report, "fresh-process-after-DuckLake-reopen", reopened, current)
        c.close()
        final_rows = [{"id": 2, "embedding": [0.0625, *([0.0] * 7)], "category": "A", "payload": "已提交后崩溃"}]
        receipt = crash(report, catalog.root, "ducklake", "after_ducklake_commit", lake=str(lake), rows=final_rows)
        c = BASE.connect(lake, extensions)
        expected_rows = [r for r in expected_rows if r["id"] not in (2, 3)] + final_rows
        require(
            K.business_rows(c) == sorted(expected_rows, key=lambda r: r["id"]),
            "DuckLake WAL recovery lost committed versions",
        )
        snapshot_sql = "SELECT id FROM ducklake_current_snapshot('lake')"
        before_replay = query(c, snapshot_sql)[0]["id"]
        replay = K.mutate(c, "after-crash", final_rows, [3])
        after_replay = query(c, snapshot_sql)[0]["id"]
        require(replay == receipt, "Crash replay changed the complete committed receipt")
        require(
            before_replay == receipt["snapshot_id"] == after_replay,
            "Crash replay advanced the committed DuckLake snapshot",
        )
        report["crash_replay"] = {
            "original_receipt": receipt,
            "replayed_receipt": replay,
            "snapshot_before": before_replay,
            "snapshot_after": after_replay,
        }
        lagging = captured(c, report)
        rejected(
            report,
            "crash-leaves-explicit-index-lag",
            lambda: catalog.read(args.probe, root / "lagging", lagging, root / "lagging.log"),
            "INDEX_WATERMARK_MISMATCH",
        )
        require(
            query(c, "SELECT id FROM ducklake_vortex_search('lake','main','items','knn',[0,0,0,0,0,0,0,0]::FLOAT[],1)")
            == [{"id": 2}],
            "Existing B/F/T query lost recovered committed T",
        )
        qualify(report, "old-view-after-DuckLake-SIGKILL", reopened, current)
        c.execute("CALL ducklake_flush_inlined_data('lake',table_name=>'items')")
        latest = captured(c, report)
        exercise_failed_checkpoints(args.probe, root, catalog, latest, report)
        F.advance(args.probe, catalog.root / "g2", catalog.root / "g3", latest, root / "advance-recovered.log")
        recovered = catalog.publish("g3", 2)
        final = catalog.read(args.probe, root / "view-final", latest, root / "view-final.log")
        views.append(final)
        qualify(report, "explicit-index-advance-after-recovery", final, latest)
        require(final.query([0.0] * 8, 1)[0]["row"]["id"] == 2, "Explicit native catch-up lost recovered version")
        report["second_gc"] = catalog.collect("g2", 16, F.LIMIT)
        qualify(report, "second-old-view-after-GC", reopened, current)
        qualify(report, "final-view-after-GC", final, latest)
        for view in views:
            view.close()
        views.clear()
        report["checks"].append(
            "Real DuckLake SIGKILL recovers committed T and receipts; index catches up only after explicit flush/advance"
        )
        report["final_head"] = recovered
    finally:
        for view in views:
            view.stop()
        c.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--probe", type=Path, required=True)
    parser.add_argument("--build-report", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--cpus", required=True)
    args = parser.parse_args()
    args.probe = args.probe.resolve()
    build = json.loads(args.build_report.read_bytes())
    require(build["status"] == "built" and Path(build["binary"]).resolve() == args.probe, "Wrong native build report")
    require(build["binary_sha256"] == digest(args.probe), "Native binary does not match clean build")
    require(
        all(digest(Path(name)) == value for name, value in build["input_sha256"].items()), "Native build inputs changed"
    )
    root = args.output_dir.resolve()
    root.mkdir(mode=0o700)
    cpus = PIN.parse_cpus(args.cpus)
    original = os.sched_getaffinity(0)
    require(set(cpus) <= original, "CPU set is unavailable")
    threads = {name: os.environ.get(name) for name in (*PIN.THREAD_ENV, "TORCH_NUM_THREADS")}
    source_paths = [
        Path(__file__),
        *[Path(module.__file__) for module in (F, O, K, WRITE, BASE, BASE.SEARCH, PIN, PIN.AA)],
        HERE / "spfresh_update/probe.cpp",
        HERE / "spfresh_update/CMakeLists.txt",
        HERE / "spfresh_update/native-update.patch",
        HERE / "build_vortex_spfresh_updates.py",
    ]
    hashes = {str(p): digest(p) for p in source_paths}
    report = {
        "status": "running",
        "rule": RULE,
        "source_sha256": hashes,
        "probe_sha256": digest(args.probe),
        "build_report_sha256": digest(args.build_report),
        "threads": threads,
        "original_affinity": sorted(original),
        "checks": [],
        "captures": [],
        "queries": [],
        "rejections": [],
        "faults": [],
    }
    (root / "rule.json").write_text(json.dumps(RULE, indent=2) + "\n")
    started = time.monotonic()
    try:
        os.sched_setaffinity(0, cpus)
        report["applied_affinity"] = sorted(os.sched_getaffinity(0))
        exercise(args, root, report)
        require(threads == {name: os.environ.get(name) for name in threads}, "Thread settings changed")
        require(hashes == {str(p): digest(p) for p in source_paths}, "Executed source changed")
        require(report["probe_sha256"] == digest(args.probe), "Executed native binary changed")
        report["status"] = "passed"
    except BaseException as error:
        report.update(status="failed", error=f"{type(error).__name__}: {error}")
        raise
    finally:
        os.sched_setaffinity(0, original)
        report["restored_affinity"] = sorted(os.sched_getaffinity(0))
        report["seconds_observation_only"] = time.monotonic() - started
        (root / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(
        json.dumps(
            {
                "status": report["status"],
                "queries": len(report["queries"]),
                "rejections": len(report["rejections"]),
                "faults": report["faults"],
            }
        )
    )


if __name__ == "__main__":
    if sys.argv[1:] == ["--fault-child"]:
        checkpoint_child(json.loads(sys.stdin.readline()))
    else:
        main()
