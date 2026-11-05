#!/usr/bin/env python3
"""Measure guarded native source GC from identical, restored local input images."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import statistics
import subprocess
import time


CONTRACT = {
    "scope": "one complete native source collector call; excludes readiness and other maintenance classes",
    "max_objects": 2,
    "max_bytes": 64 * 1024 * 1024,
    "keep_snapshots": 2,
    "min_age_seconds": 0,
    "tasks": ["expire", "files"],
    "history_storage": ["inline", "file"],
    "restoration": "same canonical paths, byte-identical metadata/files, fresh copies, no existing deletion intents",
    "primary": "existing production phase timers; no added SQL callbacks",
    "diagnostic": "separate restored processes; SQL/post-plan callbacks and checkpoints; excluded from primary statistics",
    "order": "rotate cases each round, reverse odd rounds; warmups excluded",
    "interpretation": "baseline attribution only; no runtime candidate, A/B improvement or sustained throughput claim",
    "reference": "2a9377062cddc0c222d8dd99e4d5efee3a298c76",
}


def save(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")


def manifest(root):
    result = {}
    for path in sorted(root.rglob("*")):
        mode = path.lstat().st_mode
        if stat.S_ISDIR(mode):
            continue
        if not stat.S_ISREG(mode) or path.stat().st_nlink != 1:
            raise RuntimeError(f"Input image contains a nonregular or linked entry: {path}")
        result[path.relative_to(root).as_posix()] = {
            "bytes": path.stat().st_size,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        }
    return result


def fingerprint(entries):
    return hashlib.sha256(json.dumps(entries, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def validate_sample(sample, expected, diagnostic):
    before, after = sample["before"], sample["after"]
    assert before == {
        k: v for k, v in expected.items() if k not in ("task", "history", "history_storage")
    }, "restored task input changed"
    assert sample["history"] == expected["history"] and sample["task"] == expected["task"]
    assert sample["history_storage"] == expected["history_storage"]
    assert sample["diagnostic"] is diagnostic
    assert before["intents"] == after["intents"] == 0
    assert sample["complete_source_gc_ns"] > 0
    assert 0 <= sample["bytes"] <= CONTRACT["max_bytes"]
    assert after["last_snapshot"] == before["last_snapshot"]
    assert after["expired_total"] == before["expired_total"] + sample["expired"]
    assert after["files_total"] == before["files_total"] + sample["files"]
    assert after["bytes_total"] == before["bytes_total"] + sample["bytes"]
    if sample["task"] == "expire":
        assert (sample["expired"], sample["files"], sample["bytes"]) == (2, 0, 0)
        assert after["snapshots"] == before["snapshots"] - 2 and after["scheduled"] >= 2
        assert before["scheduled"] == 0 and sample["deleted_paths"] == []
    else:
        assert (sample["expired"], sample["files"]) == (0, 2) and sample["bytes"] > 0
        assert after["snapshots"] == before["snapshots"] and after["scheduled"] == before["scheduled"] - 2
        assert len(sample["deleted_paths"]) == len(set(sample["deleted_paths"])) == 2
    assert [p["stage"] for p in sample["phases"]] == ["wait", "metadata", "expire", "artifact", "unlink"]
    assert all(p["total_ns"] >= 0 and p["calls"] >= 0 for p in sample["phases"])
    assert sum(p["total_ns"] for p in sample["phases"]) <= sample["complete_source_gc_ns"]
    assert bool(sample["queries"]) == diagnostic
    assert before["data_files"] == (3 if sample["task"] == "expire" else 1) + (
        sample["history"] + 1 if sample["history_storage"] == "file" else 0
    )
    assert after["data_files"] == before["data_files"] - (2 if sample["task"] == "expire" else 0)


def summary(samples):
    assert samples and all(s["kind"] == "primary" and not s["diagnostic"] for s in samples)
    totals = [s["complete_source_gc_ns"] for s in samples]
    return {
        "samples": len(samples),
        "complete_source_gc_ms": {
            "mean": statistics.mean(totals) / 1e6,
            "p50": statistics.median(totals) / 1e6,
            "p95": sorted(totals)[round((len(totals) - 1) * 0.95)] / 1e6,
            "min": min(totals) / 1e6,
            "max": max(totals) / 1e6,
        },
        "phase_mean_ms": {
            p["stage"]: statistics.mean(s["phases"][i]["total_ns"] for s in samples) / 1e6
            for i, p in enumerate(samples[0]["phases"])
        },
    }


def diagnostic_summary(samples):
    groups = {}
    for sample in samples:
        assert sample["diagnostic"] and sample["kind"] == "diagnostic"
        top = sorted((q for q in sample["queries"] if q["depth"] == 0), key=lambda q: q["begin_ns"])
        assert top
        assert all(a["end_ns"] <= b["begin_ns"] for a, b in zip(top, top[1:])), "top-level query scopes overlap"
        for query in sample["queries"]:
            assert query["query_ns"] == query["end_ns"] - query["begin_ns"]
            assert 0 <= query["begin_ns"] <= query["end_ns"] <= sample["complete_source_gc_ns"]
            if query["depth"]:
                assert any(
                    parent["depth"] == query["depth"] - 1
                    and parent["begin_ns"] <= query["begin_ns"]
                    and parent["end_ns"] >= query["end_ns"]
                    for parent in sample["queries"]
                ), "nested query has no containing parent"
        # An outer DuckLake COMMIT contains a native metadata COMMIT. Count only the outer scope.
        for query in top:
            # Retain exact SQL rather than hide differing predicates behind a category.
            groups.setdefault(query["sql"], []).append(query)
    return sorted(
        [
            {
                "sql": sql,
                "calls": len(queries),
                "total_ms": sum(q["query_ns"] for q in queries) / 1e6,
                "per_collector_ms": sum(q["query_ns"] for q in queries) / len(samples) / 1e6,
                "bind_plan_total_ms": sum(q.get("bind_plan_ns", 0) for q in queries) / 1e6,
                "execute_fetch_total_ms": sum(q.get("execute_fetch_ns", 0) for q in queries) / 1e6,
            }
            for sql, queries in groups.items()
        ],
        key=lambda q: q["total_ms"],
        reverse=True,
    )


def run(args):
    root = args.output_dir.resolve()
    root.mkdir(mode=0o700)
    private = root / "private"
    private.mkdir(mode=0o700)
    env = dict(os.environ, TMPDIR=str(private), VANE_RUNNER="local-fast")
    env.pop("VORTEX_SPFRESH_NATIVE", None)
    driver = args.driver.resolve()
    contract = dict(
        CONTRACT,
        histories=args.histories,
        repetitions=args.repetitions,
        warmups=args.warmups,
        diagnostic_repetitions=args.diagnostics,
    )
    save(root / "measurement-contract.json", contract)
    report = {
        "status": "running",
        "contract": contract,
        "driver_sha256": hashlib.sha256(driver.read_bytes()).hexdigest(),
        "cases": [],
    }
    started = time.monotonic()

    def command(case, mode, diagnostic, log_name):
        log_path = Path(case["root"]) / log_name
        with log_path.open("w") as log:
            result = subprocess.run(
                [
                    str(driver),
                    case["work"],
                    mode,
                    str(case["history"]),
                    case["task"],
                    "1" if diagnostic else "0",
                    case["history_storage"],
                ],
                env=env,
                stdout=log,
                stderr=subprocess.STDOUT,
                timeout=900 if mode == "prepare" else 90,
            )
        if result.returncode:
            raise RuntimeError(f"Native source GC benchmark failed: {log_path}\n{log_path.read_text()[-6000:]}")

    def measure(case, kind, repetition):
        work, image = Path(case["work"]), Path(case["image"])
        assert fingerprint(manifest(image)) == case["input_image_sha256"], "sealed input image changed"
        # Only this newly created benchmark's working directory is replaced, after its child exited.
        shutil.rmtree(work)
        shutil.copytree(image, work, copy_function=shutil.copy2)
        restored = manifest(work)
        assert fingerprint(restored) == case["input_image_sha256"], "restored image differs before native open"
        diagnostic = kind == "diagnostic"
        name = f"{kind}-{repetition:03d}"
        command(case, "run", diagnostic, name + ".log")
        sample = json.loads((work / "sample.json").read_text())
        validate_sample(sample, case["input"], diagnostic)
        after = manifest(work)
        data_before = {p for p in restored if p.startswith("data/")}
        data_after = {p for p in after if p.startswith("data/")}
        expected_deleted = {Path(p).relative_to(work).as_posix() for p in sample["deleted_paths"]}
        assert data_before - data_after == expected_deleted and not data_after - data_before
        assert all(restored[p] == after[p] for p in data_after), "GC changed a retained physical source"
        assert all(
            after.get(p) == info for p, info in restored.items() if p.startswith("indexes/")
        ), "GC changed index artifacts"
        sample.update(kind=kind, repetition=repetition, restored_image_sha256=fingerprint(restored))
        save(Path(case["root"]) / (name + ".json"), sample)
        case["samples"].append(sample)

    try:
        for history, storage, task in (
            (n, storage, task)
            for n in args.histories
            for storage in CONTRACT["history_storage"]
            for task in CONTRACT["tasks"]
        ):
            case_root = root / f"history-{history}-{storage}-{task}"
            case_root.mkdir(mode=0o700)
            work, image = case_root / "work", case_root / "image"
            work.mkdir(mode=0o700)
            case = {
                "history": history,
                "history_storage": storage,
                "task": task,
                "root": str(case_root),
                "work": str(work),
                "image": str(image),
                "samples": [],
            }
            command(case, "prepare", False, "prepare.log")
            case["input"] = json.loads((work / "input.json").read_text())
            shutil.copytree(work, image, copy_function=shutil.copy2)
            entries = manifest(image)
            case["input_image_sha256"] = fingerprint(entries)
            save(case_root / "input-manifest.json", entries)
            report["cases"].append(case)
            print(
                f"Prepared history={history}, storage={storage}, task={task}: {case['input']['snapshots']} snapshots, {case['input']['data_files']} data files",
                flush=True,
            )
            save(root / "report.json", report)
        cases = report["cases"]
        for repetition in range(args.warmups + args.repetitions):
            offset = repetition % len(cases)
            order = cases[offset:] + cases[:offset]
            if repetition % 2:
                order = list(reversed(order))
            kind = "warmup" if repetition < args.warmups else "primary"
            for case in order:
                measure(case, kind, repetition)
            print(f"Completed {kind} round {repetition + 1}", flush=True)
            save(root / "report.json", report)
        # Additional callbacks copy the prepared AST; keep all diagnostics outside primary measurements.
        for repetition in range(args.diagnostics):
            for case in cases:
                measure(case, "diagnostic", repetition)
        for case in cases:
            case["timing"] = summary([s for s in case["samples"] if s["kind"] == "primary"])
            case["diagnostic"] = diagnostic_summary([s for s in case["samples"] if s["kind"] == "diagnostic"])
            assert fingerprint(manifest(Path(case["image"]))) == case["input_image_sha256"]
        report.update(status="completed", decision="baseline_only", semantic_checks="passed", input_images="unchanged")
        print("Completed native source-GC attribution; no runtime change measured", flush=True)
    finally:
        report["seconds"] = time.monotonic() - started
        save(root / "report.json", report)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--driver", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--histories", type=int, nargs="+", default=[8, 512, 4096])
    parser.add_argument("--repetitions", type=int, default=16)
    parser.add_argument("--warmups", type=int, default=2)
    parser.add_argument("--diagnostics", type=int, default=3)
    args = parser.parse_args()
    if (
        args.histories != sorted(set(args.histories))
        or not args.histories
        or any(n < 8 or n > 10000 for n in args.histories)
    ):
        parser.error("histories must be distinct, increasing and within 8..10000")
    if not 1 <= args.repetitions <= 64 or not 0 <= args.warmups <= 8 or not 1 <= args.diagnostics <= 8:
        parser.error("invalid sample counts")
    run(args)
