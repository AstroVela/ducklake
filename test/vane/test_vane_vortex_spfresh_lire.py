#!/usr/bin/env python3
"""Deterministic native LIRE structure, version and recovery qualification."""

from copy import deepcopy
import argparse
import importlib.util
import json
import os
from pathlib import Path
import selectors
import signal
import shutil
import struct
import subprocess
import sys
import time

HERE = Path(__file__).resolve().parent


def load(name, filename):
    spec = importlib.util.spec_from_file_location(name, HERE / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


H = load("lire_update_cases", "test_vane_vortex_spfresh_updates.py")
L = load("lire_candidates", "vortex_spfresh_lire_fixture.py")
O = load("lire_physical_oracle", "vortex_spfresh_lire_oracle.py")
MERGE = load("lire_merge_case", "vortex_spfresh_lire_merge_case.py")
F, K, BASE = L.F, H.K, H.BASE
require, query, quote = BASE.require, BASE.query, BASE.quote
RULE = {
    "schema": 1,
    "scope": "Local controlled native Split/MergePostings/Reassign; production SQL/provider unchanged, R6 deferred.",
    "scheduler": "Explicit single-thread queue runs real upstream jobs; no background workers or timing criteria.",
    "budget": "Each explicit drain: <=16 nested native operations, <=64 MiB serialized posting writes; reads/checkpoint files have separate original admission limits.",
    "admission": "Reassign reserves 1+replicas operations before dequeue; insufficient work budget defers the intact job.",
    "native_profile": "8D Float32/L2, 64 initial heads, 64 search heads, 12 posting pages, 4 replicas, max_check=4096, native thread=1, DuckDB threads=2.",
    "acceptance": "Every visible VID has current-version coverage and exact nearest-partition assignment; historical SQL/IEEE/ANN recall >=0.8; old views survive canonical GC.",
    "deferred": [
        "R6",
        "background concurrency",
        "production dynamic registration/publication",
        "stable-load performance",
        "power-loss qualification",
    ],
}


def trace(report, name, state):
    require(0 <= state["steps"] <= 16 and 0 <= state["posting_bytes"] <= F.LIMIT, "LIRE exceeded original budgets")
    report["drains"].append(dict(phase=name, **{k: v for k, v in state.items() if k != "heads"}))


def finish(report, name, candidate):
    state = candidate.state()
    for _ in range(2000):
        if state["queued"] == 0:
            return candidate.seal()
        state = candidate.drain(16, F.LIMIT)
        trace(report, name, state)
        require(state["steps"] > 0, "Full explicit budget did not make progress")
    raise AssertionError("LIRE did not converge within the declared finite operation count")


def physical(report, name, args, root, body):
    image = O.observe(args.probe, root / "native", body)
    (args.output_dir / (name + "-physical.json")).write_text(json.dumps(image, indent=2) + "\n")
    report["physical"].append(
        dict(
            phase=name,
            **{k: v for k, v in image.items() if k not in ("current_postings", "head_vectors", "native_versions")},
        )
    )
    return image


def qualify(report, name, view, capture):
    H.qualify(report, name, view, capture)
    before = F.inventory(view.root / "native")
    center = report["input"]["center"]
    for offset in (center - 0.25, center, center + 0.25):
        vector = [offset, *([0.0] * 7)]
        for k in (1, 3, 10):
            recall = H.O.qualify(view.query(vector, k), capture["rows"], vector, k)
            report["queries"].append(
                {
                    "phase": name,
                    "query": offset,
                    "k": k,
                    "recall": recall,
                    "snapshot_id": capture["file_capture"]["snapshot_id"],
                    "boundary": True,
                }
            )
    require(before == F.inventory(view.root / "native"), "Boundary query changed read-only files")


def oracle_guards(args, report, native, body, image):
    live_vid = next(v for v in image["current_postings"].values() if v)[0]
    head = next(int(h) for h, vids in image["current_postings"].items() if vids)
    for name, reason in (
        ("version-label", "Native delete labels differ from ReadView"),
        ("vector-mapping", "Posting vector/VID mapping changed"),
        ("posting-size", "Persisted posting-size record differs from raw RocksDB"),
        ("head-retirement", "Retired head retains a live size"),
    ):
        target = args.output_dir / ("corrupt-" + name)
        shutil.copytree(native, target)
        F.inventory(target)
        changed = deepcopy(body)
        if name == "version-label":
            path = target / "versions.bin"
            data = bytearray(path.read_bytes())
            struct.pack_into("<i", data, 0, struct.unpack_from("<i", data)[0] + 1)
            data[12 + live_vid] = 0xFE
            path.write_bytes(data)
        elif name == "vector-mapping":
            changed["mapping"][live_vid]["row"]["embedding"][0] += 1.0
        elif name == "posting-size":
            path = target / "posting-sizes.bin"
            data = bytearray(path.read_bytes())
            offset = 8 + head * 4
            struct.pack_into("<i", data, offset, struct.unpack_from("<i", data, offset)[0] + 1)
            path.write_bytes(data)
        else:
            path = target / "HeadIndex/deletes.bin"
            data = bytearray(path.read_bytes())
            struct.pack_into("<i", data, 0, struct.unpack_from("<i", data)[0] + 1)
            data[12 + head] = 1
            path.write_bytes(data)
        before = F.inventory(target)
        try:
            O.observe(args.probe, target, changed)
        except AssertionError as error:
            require(reason in str(error), "Wrong oracle guard: " + str(error))
            report["rejections"].append({"name": name, "code": str(error)})
        else:
            raise AssertionError("Corrupt native state passed: " + name)
        require(F.inventory(target) == before, "Oracle changed inspected files")


def fault_child(config):
    root = Path(config["root"])
    candidate = L.Candidate(
        Path(config["probe"]), root / config["source"], root / config["target"], config["capture"], Path(config["log"])
    )
    try:
        if config.get("merge") is not None:
            candidate.merge(config["merge"])
        stage = config["stage"]
        if stage == "checkpoint_busy":
            candidate.worker.call("checkpoint")
            raise AssertionError("Pending LIRE checkpoint was accepted")
        if stage == "byte_budget":
            candidate.drain(16, 1)
            raise AssertionError("Tiny posting budget was accepted")
        if stage.startswith("budget-"):
            work, bytes_ = config["budget"]
            candidate.drain(work, bytes_)
            raise AssertionError("Invalid LIRE budget was accepted")
        require(candidate.worker.call("pause", stage) == {"armed": True}, "Native pause not armed")
        for _ in range(2000):
            value = candidate.drain(16, F.LIMIT)
            if value == {"pause": stage}:
                print(json.dumps({"event": stage, "affinity": sorted(os.sched_getaffinity(0))}), flush=True)
                require(sys.stdin.readline().strip() == "continue", "Fault controller closed")
                return
            require(value["queued"] > 0, "Expected native stage never occurred")
        raise AssertionError("Native pause was not reached")
    finally:
        candidate.stop()


def fault(report, args, catalog, source, capture, stage, merge=None, budget=None):
    target = "fault-" + stage.replace("_", "-")
    config = {
        "root": str(catalog.root),
        "probe": str(args.probe),
        "source": source,
        "target": target,
        "capture": capture,
        "stage": stage,
        "merge": merge,
        "budget": budget,
        "log": str(args.output_dir / (target + "-native.log")),
    }
    before = catalog.head()
    with (args.output_dir / (target + ".log")).open("wb") as log:
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
            if stage in ("checkpoint_busy", "byte_budget") or stage.startswith("budget-"):
                require(process.wait(timeout=90) != 0, "Native guard did not fail")
                native = Path(config["log"]).read_text()
                code = (
                    "LIRE_CHECKPOINT_BUSY"
                    if stage == "checkpoint_busy"
                    else "LIRE_BYTE_BUDGET" if stage == "byte_budget" else "LIRE_BUDGET"
                )
                require(code in native, "Wrong native guard: " + native[-500:])
                report["rejections"].append({"name": stage, "code": code})
            else:
                with selectors.DefaultSelector() as selector:
                    selector.register(process.stdout, selectors.EVENT_READ)
                    require(selector.select(90), "Native fault handshake deadline")
                event = json.loads(process.stdout.readline())
                require(
                    event == {"event": stage, "affinity": sorted(os.sched_getaffinity(0))}, "Wrong native fault event"
                )
                os.killpg(process.pid, signal.SIGKILL)
                require(process.wait(timeout=30) == -9, "No actual SIGKILL")
                report["faults"].append({"stage": stage, "exit_code": -9})
        finally:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=30)
    require(not (catalog.root / target / "seal.json").exists(), "Interrupted LIRE acquired a seal")
    require(F.Catalog(catalog.root).head() == before, "Interrupted candidate changed published state")


def exercise(args, report):
    root = args.output_dir
    lake = root / "lake"
    lake.mkdir(mode=0o700)
    (lake / "indexes").mkdir(mode=0o700)
    c = BASE.connect(lake, create=True)
    views = []
    try:
        c.execute("SELECT * FROM ducklake_initialize_vortex_writes('lake')")
        K.manage(c)
        seed_rows = [
            {"id": i, "embedding": [float(i), *([0.0] * 6), -0.0], "category": None, "payload": None}
            for i in range(256)
        ]
        K.mutate(c, "seed-lire", seed_rows)
        c.execute("CALL ducklake_flush_inlined_data('lake',table_name=>'items')")
        revision = BASE.head(c)["revision"]
        c.execute(
            "SELECT * FROM ducklake_append_vortex_index_segment('lake','main','items','knn',"
            + quote(lake / "indexes/seed.json")
            + f",8,{revision})"
        )
        static = deepcopy(BASE.head(c))
        initial = H.captured(c, report)
        catalog = F.Catalog(root / "catalog")
        first, state = L.seed(args.probe, catalog.root / "g1", initial, root / "seed.log")
        physical(report, "seed", args, catalog.root / "g1", first)
        catalog.publish("g1", 0)
        old = L.read(catalog, args.probe, root / "view-initial", initial, root / "view-initial.log")
        views.append(old)
        heads = sorted(state["heads"], key=lambda h: h["vector"][0])
        head = min(heads[1:-1], key=lambda h: (h["size"], h["id"]))
        center = head["vector"][0]
        report["input"] = {"seed": 20261107, "center": center, "head": head["id"], "burst_rows": 1600}
        qualify(report, "initial", old, initial)
        captures = [initial]
        for batch in range(2):
            rows = [
                {
                    "id": 10000 + i,
                    "embedding": [center + (-0.25 if i % 2 == 0 else 0.25) + (i % 64) / 8192, *([0.0] * 7)],
                    "category": "A\0" if i % 2 else None,
                    "payload": None,
                }
                for i in range(batch * 800, (batch + 1) * 800)
            ]
            receipt = K.mutate(c, "burst-" + str(batch), rows)
            require(K.mutate(c, "burst-" + str(batch), rows) == receipt, "Business replay changed during LIRE")
            c.execute("CALL ducklake_flush_inlined_data('lake',table_name=>'items')")
            captures.append(H.captured(c, report))
            if batch == 0:
                candidate = L.Candidate(
                    args.probe, catalog.root / "g1", catalog.root / "g2", captures[-1], root / "first-burst.log"
                )
                try:
                    second, _ = finish(report, "first-burst", candidate)
                finally:
                    candidate.stop()
                physical(report, "first-burst", args, catalog.root / "g2", second)
                catalog.publish("g2", 1)
        before_split, target = captures[1], captures[2]
        checkpoint_view = L.read(
            catalog, args.probe, root / "view-before-split", before_split, root / "view-before-split.log"
        )
        views.append(checkpoint_view)
        for stage in ("after_split", "after_reassign_version", "checkpoint_busy", "byte_budget"):
            fault(report, args, catalog, "g2", target, stage)
            qualify(report, "old-after-" + stage, checkpoint_view, before_split)
        for i, budget in enumerate(((0, F.LIMIT), (17, F.LIMIT), (16, 0), (16, F.LIMIT + 1))):
            fault(report, args, catalog, "g2", target, "budget-" + str(i), budget=budget)
        candidate = L.Candidate(args.probe, catalog.root / "g2", catalog.root / "g3", target, root / "split.log")
        try:
            first_step = candidate.drain(1, F.LIMIT)
            trace(report, "one-work-item", first_step)
            require(first_step["steps"] == 1 and first_step["queued"] > 0, "Split did not leave real reassign work")
            deferred = candidate.drain(1, F.LIMIT)
            trace(report, "nested-reservation-deferred", deferred)
            require(
                deferred["steps"] == 0
                and deferred["queued"] == first_step["queued"]
                and deferred["posting_bytes"] == 0,
                "Small work budget executed a nested job",
            )
            third, split_state = finish(report, "split-reassign", candidate)
        finally:
            candidate.stop()
        require(
            split_state["splits"] > 0 and split_state["reassigns"] > 0 and split_state["version_changes"] > 0,
            "LIRE operations were not actually exercised",
        )
        split_image = physical(report, "split", args, catalog.root / "g3", third)
        require(
            split_image["head_slots"] > 64 and split_image["retired_heads"] > 0 and split_image["version_changes"] > 0,
            "Persisted split/version state did not change",
        )
        catalog.publish("g3", 2)
        new = L.read(catalog, args.probe, root / "view-split", target, root / "view-split.log")
        views.append(new)
        qualify(report, "split-reopened", new, target)
        report["gc"] = [catalog.collect("g1", 16, F.LIMIT), catalog.collect("g2", 16, F.LIMIT)]
        qualify(report, "initial-after-GC", old, initial)
        qualify(report, "pre-split-after-GC", checkpoint_view, before_split)

        merge_head = min(
            (int(h) for h in split_image["current_postings"] if int(h) >= 64),
            key=lambda h: len(split_image["current_postings"][str(h)]),
        )
        delete_vids = split_image["current_postings"][str(merge_head)][5:]
        delete_ids = [third["mapping"][vid]["row"]["id"] for vid in delete_vids]
        require(len(delete_ids) > 0, "Controlled merge requires a sparse posting")
        merge_source, next_generation = "g3", 4
        intermediate = []
        chunks = [delete_ids[offset : offset + 1000] for offset in range(0, len(delete_ids), 1000)]
        for index, chunk in enumerate(chunks[:-1]):
            K.mutate(c, "prepare-merge-" + str(index), [], chunk)
            partial_capture = H.captured(c, report)
            name = "g" + str(next_generation)
            partial = L.Candidate(
                args.probe,
                catalog.root / merge_source,
                catalog.root / name,
                partial_capture,
                root / (name + "-delete.log"),
            )
            try:
                partial_body, _ = finish(report, "bounded-delete", partial)
            finally:
                partial.stop()
            physical(report, "bounded-delete-" + str(index), args, catalog.root / name, partial_body)
            catalog.publish(name, next_generation - 1)
            intermediate.append(name)
            merge_source = name
            next_generation += 1
        K.mutate(c, "prepare-merge-final", [], chunks[-1])
        merged_capture = H.captured(c, report)
        report["merge_input"] = {"head": merge_head, "deleted_vids": delete_vids}
        fault(report, args, catalog, merge_source, merged_capture, "after_merge", merge=merge_head)
        qualify(report, "old-after-merge-SIGKILL", new, target)
        merged_name = "g" + str(next_generation)
        candidate = L.Candidate(
            args.probe, catalog.root / merge_source, catalog.root / merged_name, merged_capture, root / "merge.log"
        )
        try:
            candidate.merge(merge_head)
            fourth, merge_state = finish(report, "merge-reassign", candidate)
        finally:
            candidate.stop()
        require(merge_state["merges"] > 0, "Controlled merge did not merge any head")
        merged_image = physical(report, "merge", args, catalog.root / merged_name, fourth)
        require(merged_image["active_heads"] < split_image["active_heads"], "Merge did not retire a head")
        oracle_guards(args, report, catalog.root / merged_name / "native", fourth, merged_image)
        catalog.publish(merged_name, next_generation - 1)
        final = L.read(catalog, args.probe, root / "view-merged", merged_capture, root / "view-merged.log")
        views.append(final)
        qualify(report, "merge-reopened", final, merged_capture)
        report["gc"].append(catalog.collect("g3", 16, F.LIMIT))
        for name in intermediate:
            report["gc"].append(catalog.collect(name, 16, F.LIMIT))
        qualify(report, "split-view-after-GC", new, target)
        qualify(report, "merge-view-after-GC", final, merged_capture)
        require(BASE.head(c) == static, "LIRE experiment changed production static publication")
        report["final_head"] = catalog.head()
        for view in views:
            view.close()
        views.clear()
    finally:
        for view in views:
            view.stop()
        c.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--probe", type=Path, required=True)
    parser.add_argument("--build-report", type=Path, required=True)
    parser.add_argument("--merge-control-probe", type=Path, required=True)
    parser.add_argument("--merge-control-build-report", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--cpus", required=True)
    args = parser.parse_args()
    args.probe = args.probe.resolve()
    args.merge_control_probe = args.merge_control_probe.resolve()
    args.output_dir = args.output_dir.resolve()
    args.output_dir.mkdir(mode=0o700)
    builds = []
    for path, probe, profile in (
        (args.build_report, args.probe, "lire-fixed"),
        (args.merge_control_build_report, args.merge_control_probe, "lire-merge-control"),
    ):
        build = json.loads(path.read_bytes())
        require(
            build["status"] == "built" and build["profile"] == profile and build["binary_sha256"] == H.digest(probe),
            "Native build mismatch",
        )
        require(all(H.digest(Path(n)) == v for n, v in build["input_sha256"].items()), "Native build inputs changed")
        builds.append(build)
    require(builds[0]["source_sha256"].keys() == builds[1]["source_sha256"].keys(), "Control source inventory changed")
    require(
        [
            name
            for name in builds[0]["source_sha256"]
            if builds[0]["source_sha256"][name] != builds[1]["source_sha256"][name]
        ]
        == ["AnnService/inc/Core/SPANN/ExtraDynamicSearcher.h"],
        "Control source difference changed",
    )
    cpus = H.PIN.parse_cpus(args.cpus)
    original = os.sched_getaffinity(0)
    require(set(cpus) <= original, "CPU set unavailable")
    sources = {
        Path(__file__),
        *[Path(m.__file__) for m in (H, L, L.F, O, MERGE, H.K, H.WRITE, H.BASE, H.BASE.SEARCH, H.O, H.PIN, H.PIN.AA)],
    }
    hashes = {str(p): H.digest(p) for p in sources}
    threads = {k: os.environ.get(k) for k in (*H.PIN.THREAD_ENV, "TORCH_NUM_THREADS")}
    report = {
        "status": "running",
        "rule": RULE,
        "source_sha256": hashes,
        "probe_sha256": H.digest(args.probe),
        "build_report_sha256": H.digest(args.build_report),
        "merge_control_probe_sha256": H.digest(args.merge_control_probe),
        "merge_control_build_report_sha256": H.digest(args.merge_control_build_report),
        "threads": threads,
        "original_affinity": sorted(original),
        "captures": [],
        "queries": [],
        "physical": [],
        "drains": [],
        "faults": [],
        "rejections": [],
    }
    (args.output_dir / "rule.json").write_text(json.dumps(RULE, indent=2) + '\n')
    started = time.monotonic()
    try:
        os.sched_setaffinity(0, cpus)
        report["applied_affinity"] = sorted(os.sched_getaffinity(0))
        exercise(args, report)
        MERGE.exercise(args, report, F, O, trace)
        require(hashes == {str(p): H.digest(p) for p in sources}, "Executed source changed")
        require(threads == {k: os.environ.get(k) for k in threads}, "Thread settings changed")
        report['status'] = 'passed'
    except BaseException as error:
        report.update(status='failed', error=f'{type(error).__name__}: {error}')
        raise
    finally:
        os.sched_setaffinity(0, original)
        report['restored_affinity'] = sorted(os.sched_getaffinity(0))
        report['seconds_observation_only'] = time.monotonic() - started
        (args.output_dir / 'report.json').write_text(json.dumps(report, indent=2) + '\n')
    print(
        json.dumps(
            {
                'status': report['status'],
                'queries': len(report['queries']),
                'drains': len(report['drains']),
                'faults': report['faults'],
                'rejections': report['rejections'],
            }
        )
    )


if __name__ == '__main__':
    if sys.argv[1:] == ['--fault-child']:
        fault_child(json.loads(sys.stdin.readline()))
    else:
        main()
