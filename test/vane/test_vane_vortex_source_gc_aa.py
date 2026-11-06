#!/usr/bin/env python3
"""Check restored-collector measurement precision with one executable and two labels."""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import resource
import shutil
import statistics
import subprocess
import time

SPEC = importlib.util.spec_from_file_location("gc_ab", Path(__file__).with_name("test_vane_vortex_source_gc_ab.py"))
AB = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(AB)
BASE = AB.BASE

# These existing analysis labels are experimental groups; both invoke the same path and bytes.
LABELS = ("baseline", "candidate")
FOCUS = {(8, "inline", "files"), (4096, "file", "files")}
FULL = {(n, storage, task) for n in (8, 512, 4096) for storage in ("inline", "file") for task in ("expire", "files")}
SMOKE = {c for c in FULL if c[0] == 8}
RULE = {
    "mode": "identical_binary_aa",
    "scope": "complete_source_gc_ns",
    "profiles": {"focus": 16, "full": 64, "smoke": 2},
    "focus_cases": [list(c) for c in sorted(FOCUS)],
    "equivalence_band": [0.98, 1.02],
    "interval": "unchanged AB paired-round median percentile bootstrap, descriptive 95%",
    "bootstrap_resamples": AB.RULE["bootstrap_resamples"],
    "bootstrap_seed": AB.RULE["bootstrap_seed"],
    "block_rounds": 16,
    "precision": "Every case has its complete 95% interval and each consecutive 16-round block median inside [0.98,1.02].",
    "prefix": "Report the first 16 full-matrix rounds separately; compute all analyses after fixed sampling ends.",
    "order": AB.RULE["order"],
    "sampling": "Each requested profile runs its fixed count; stop only for execution, semantic or integrity failure. No adaptive sampling or rerun-until-pass. A parent protocol declares phase advancement.",
    "interpretation": "Per-case descriptive equivalence of this aggregate protocol; no runtime adoption, old-result reclassification or sustained-stability claim.",
}
COUNTERS = ("ru_utime", "ru_stime", "ru_minflt", "ru_majflt", "ru_nvcsw", "ru_nivcsw")


def select_cases(profile, originals):
    keys = [(c["history"], c["history_storage"], c["task"]) for c in originals]
    assert len(keys) == len(set(keys)), "duplicate input case"
    expected = SMOKE if profile == "smoke" else FULL
    assert set(keys) == expected, "input corpus does not match the declared profile"
    wanted = FOCUS if profile == "focus" else expected
    return [c for c, key in zip(originals, keys) if key in wanted]


def analyze(samples, rounds):
    assert all(math.isfinite(s["complete_source_gc_ns"]) and s["complete_source_gc_ns"] > 0 for s in samples)
    assert len(samples) == rounds * 4
    result = AB.paired(samples, rounds)
    block = RULE["block_rounds"]
    result["block_medians"] = [statistics.median(result["ratios"][i : i + block]) for i in range(0, rounds, block)]
    low, high = RULE["equivalence_band"]
    result["interval_within_band"] = low <= result["bootstrap_95pct"][0] <= result["bootstrap_95pct"][1] <= high
    result["blocks_within_band"] = all(low <= x <= high for x in result["block_medians"])
    result["precision_demonstrated"] = result["interval_within_band"] and result["blocks_within_band"]
    return result


def decision(profile, cases):
    if profile == "smoke":
        return "smoke_only"
    expected = FOCUS if profile == "focus" else FULL
    keys = {(c["history"], c["history_storage"], c["task"]) for c in cases}
    assert len(cases) == len(expected) and keys == expected
    assert all(c["paired"]["rounds"] == RULE["profiles"][profile] for c in cases)
    return (
        "precision_demonstrated"
        if all(c["paired"]["precision_demonstrated"] for c in cases)
        else "precision_not_demonstrated"
    )


def run(args):
    root = args.output_dir.resolve()
    root.mkdir(mode=0o700)
    inputs = args.inputs.resolve()
    source = json.loads(inputs.read_text())
    assert source["status"] == "completed" and source["decision"] == "baseline_only"
    selected = select_cases(args.profile, source["cases"])
    driver = args.driver.resolve()
    driver_hash = hashlib.sha256(driver.read_bytes()).hexdigest()
    assert driver_hash == source["driver_sha256"], "driver differs from the input-producing baseline"
    tracked = [Path(__file__), Path(AB.__file__), Path(BASE.__file__)]
    code_hashes = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in tracked}
    private = root / "private"
    private.mkdir(mode=0o700)
    env = dict(os.environ, TMPDIR=str(private), VANE_RUNNER="local-fast")
    env.pop("VORTEX_SPFRESH_NATIVE", None)
    rounds = RULE["profiles"][args.profile]
    report = {
        "status": "running",
        "profile": args.profile,
        "rule": RULE,
        "driver": str(driver),
        "driver_sha256": {label: driver_hash for label in LABELS},
        "inputs_report_sha256": hashlib.sha256(inputs.read_bytes()).hexdigest(),
        "code_sha256": code_hashes,
        "host": {
            "kernel": os.uname().release,
            "machine": os.uname().machine,
            "affinity": sorted(os.sched_getaffinity(0)),
            "load_at_start": os.getloadavg(),
        },
        "resource_scope": "Whole native subprocess including startup and functional checks; excludes parent restore/hash; not collector-only CPU or causal attribution. No affinity/governor changes.",
        "cases": [],
    }
    BASE.save(root / "decision-rule.json", dict(RULE, profile=args.profile, rounds=rounds))
    BASE.save(root / "report.json", report)
    started = time.monotonic()
    for original in selected:
        case = {
            k: original[k]
            for k in ("history", "history_storage", "task", "input", "image", "work", "root", "input_image_sha256")
        }
        name = f"history-{case['history']}-{case['history_storage']}-{case['task']}"
        expected_root = inputs.parent / name
        assert Path(case["root"]) == expected_root and expected_root.resolve() == expected_root
        assert Path(case["work"]) == expected_root / "work" and Path(case["image"]) == expected_root / "image"
        assert not Path(case["work"]).is_symlink() and not Path(case["image"]).is_symlink()
        case["output"] = str(root / name)
        Path(case["output"]).mkdir(mode=0o700)
        case["samples"] = []
        report["cases"].append(case)

    def measure(case, label, kind, number, block):
        work, image = Path(case["work"]), Path(case["image"])
        assert BASE.fingerprint(BASE.manifest(image)) == case["input_image_sha256"]
        shutil.rmtree(work)
        shutil.copytree(image, work, copy_function=shutil.copy2)
        before = BASE.manifest(work)
        assert BASE.fingerprint(before) == case["input_image_sha256"]
        name = f"{kind}-{number:02d}-{block}-{label}"
        diagnostic = kind == "diagnostic"
        usage_before = resource.getrusage(resource.RUSAGE_CHILDREN)
        process_begin = time.monotonic_ns()
        with (Path(case["output"]) / (name + ".log")).open("w") as log:
            result = subprocess.run(
                [
                    str(driver),
                    str(work),
                    "run",
                    str(case["history"]),
                    case["task"],
                    "1" if diagnostic else "0",
                    case["history_storage"],
                ],
                env=env,
                stdout=log,
                stderr=subprocess.STDOUT,
                timeout=90,
            )
        process_end = time.monotonic_ns()
        usage_after = resource.getrusage(resource.RUSAGE_CHILDREN)
        assert result.returncode == 0, str(Path(case["output"]) / (name + ".log"))
        sample = json.loads((work / "sample.json").read_text())
        BASE.validate_sample(sample, case["input"], diagnostic)
        after = BASE.manifest(work)
        old_data, new_data = ({p for p in m if p.startswith("data/")} for m in (before, after))
        deleted = {Path(p).relative_to(work).as_posix() for p in sample["deleted_paths"]}
        assert old_data - new_data == deleted and not new_data - old_data
        assert all(before[p] == after[p] for p in new_data)
        assert all(after.get(p) == value for p, value in before.items() if p.startswith("indexes/"))
        sample.update(
            kind=kind,
            variant=label,
            round=number,
            block=block,
            restored_image_sha256=BASE.fingerprint(before),
            process_begin_ns=process_begin,
            process_wall_ns=process_end - process_begin,
            process_usage={key: getattr(usage_after, key) - getattr(usage_before, key) for key in COUNTERS},
        )
        BASE.save(Path(case["output"]) / (name + ".json"), sample)
        case["samples"].append(sample)

    try:
        for case in report["cases"]:
            for block, label in enumerate(("baseline", "candidate", "candidate", "baseline")):
                measure(case, label, "warmup", 0, block)
        for number in range(rounds):
            cases = report["cases"]
            offset = number % len(cases)
            order = cases[offset:] + cases[:offset]
            if number % 2:
                order = list(reversed(order))
            labels = (
                ("baseline", "candidate", "candidate", "baseline")
                if number % 2 == 0
                else ("candidate", "baseline", "baseline", "candidate")
            )
            for case in order:
                for block, label in enumerate(labels):
                    measure(case, label, "primary", number, block)
            report["completed_rounds"] = number + 1
            report["elapsed_seconds"] = time.monotonic() - started
            print(f"Completed {args.profile} A/A round {number + 1}/{rounds}", flush=True)
            BASE.save(root / "report.json", report)
        for case in report["cases"]:
            for block, label in enumerate(LABELS):
                measure(case, label, "diagnostic", 0, block)
            primary = [s for s in case["samples"] if s["kind"] == "primary"]
            case["paired"] = analyze(primary, rounds)
            if rounds >= 16:
                case["prefix_16"] = analyze([s for s in primary if s["round"] < 16], 16)
            case["timing"] = {label: BASE.summary([s for s in primary if s["variant"] == label]) for label in LABELS}
            case["diagnostic"] = {
                label: BASE.diagnostic_summary(
                    [s for s in case["samples"] if s["kind"] == "diagnostic" and s["variant"] == label]
                )
                for label in LABELS
            }
            assert BASE.fingerprint(BASE.manifest(Path(case["image"]))) == case["input_image_sha256"]
        assert hashlib.sha256(driver.read_bytes()).hexdigest() == driver_hash
        assert all(hashlib.sha256(p.read_bytes()).hexdigest() == code_hashes[p.name] for p in tracked)
        assert hashlib.sha256(inputs.read_bytes()).hexdigest() == report["inputs_report_sha256"]
        assert sorted(os.sched_getaffinity(0)) == report["host"]["affinity"]
        report.update(
            status="completed",
            decision=decision(args.profile, report["cases"]),
            semantic_checks="passed",
            input_images="unchanged",
            load_at_end=os.getloadavg(),
        )
        if rounds >= 16:
            report["prefix_16_passing_cases"] = sum(c["prefix_16"]["precision_demonstrated"] for c in report["cases"])
    except BaseException as error:
        report.update(status="failed", error=f"{type(error).__name__}: {error}")
        raise
    finally:
        report["seconds"] = time.monotonic() - started
        BASE.save(root / "report.json", report)
    print("Decision: " + report["decision"], flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--inputs", type=Path, required=True)
    parser.add_argument("--driver", type=Path, required=True)
    parser.add_argument("--profile", choices=tuple(RULE["profiles"]), required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    run(parser.parse_args())
