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
import subprocess
import sys
import time

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
    "profile": "8D Float32 squared L2; 64 heads, 64 search heads, 12 pages, 4 replicas; one native build/search thread.",
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
    def stop(stage):
        if stage == config["stage"]:
            print(json.dumps({"event": stage, "affinity": sorted(os.sched_getaffinity(0))}), flush=True)
            require(sys.stdin.readline().strip() == "continue", "Fault handshake lost")

    root = Path(config["root"])
    catalog = F.Catalog(root)
    if config["operation"] == "ducklake":
        c = BASE.connect(Path(config["lake"]))
        K.mutate(c, "after-crash", config["rows"], [3])
        stop("after_ducklake_commit")
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
            require(event == {"event": stage, "affinity": sorted(os.sched_getaffinity(0))}, "Wrong crash boundary")
            os.killpg(process.pid, signal.SIGKILL)
            require(process.wait(timeout=30) == -signal.SIGKILL, "No actual SIGKILL")
            report["faults"].append({"operation": operation, "stage": stage, "exit_code": -9})
        finally:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=30)


def exercise(args, root, report):
    lake = root / "lake"
    lake.mkdir(mode=0o700)
    (lake / "indexes").mkdir(mode=0o700)
    c = BASE.connect(lake, create=True)
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

        # Real ordinary DuckLake transaction observes its old logical version.
        ordinary = c.cursor()
        ordinary.execute("BEGIN")
        before = BASE.raw(ordinary)
        replacement = [
            {"id": 0, "embedding": [0.125, *([0.0] * 7)], "category": "B", "payload": "新版本-\0"},
            {"id": 10000, "embedding": [0.25, *([0.0] * 7)], "category": None, "payload": None},
        ]
        changed = K.mutate(c, "replace", replacement, [1])
        require(BASE.raw(ordinary) == before, "Native update changed an old DuckLake transaction")
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

        for item in views:
            item.close()
        views.clear()
        require(BASE.head(c) == static, "R7 experiment mutated production static publication")
        expected_rows = deepcopy(BASE.raw(c))
        c.close()
        c = BASE.connect(lake)
        require(
            BASE.raw(c) == expected_rows and K.mutate(c, "replace", replacement, [1]) == changed,
            "DuckLake rows or receipts changed across reopen",
        )
        report["checks"].append("Native DuckLake reopen retains business rows, UUID, receipts and static publication")
        reopened = catalog.read(args.probe, root / "view-recovered", current, root / "view-recovered.log")
        views.append(reopened)
        qualify(report, "fresh-process-after-DuckLake-reopen", reopened, current)
        c.close()
        final_rows = [{"id": 2, "embedding": [0.0625, *([0.0] * 7)], "category": "A", "payload": "已提交后崩溃"}]
        crash(report, catalog.root, "ducklake", "after_ducklake_commit", lake=str(lake), rows=final_rows)
        c = BASE.connect(lake)
        expected_rows = [r for r in expected_rows if r["id"] not in (2, 3)] + final_rows
        require(
            BASE.raw(c) == sorted(expected_rows, key=lambda r: r["id"]), "DuckLake WAL recovery lost committed versions"
        )
        require(K.mutate(c, "after-crash", final_rows, [3])["row_count"] == 2, "Crash replay lost receipt")
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
