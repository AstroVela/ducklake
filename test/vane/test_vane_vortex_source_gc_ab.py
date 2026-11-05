#!/usr/bin/env python3
"""Compare complete native collectors from the same sealed input before every call."""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import random
import shutil
import statistics
import subprocess
import time


SPEC = importlib.util.spec_from_file_location(
    "source_gc_cost", Path(__file__).with_name("test_vane_vortex_source_gc_benchmark.py")
)
BASE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BASE)

RULE = {
    "scope": "complete_source_gc_ns",
    "minimum_rounds": 16,
    "target": "materialized-history expiry at 512 and 4096 updates",
    "minimum_median_gain": 0.02,
    "bootstrap_upper_ratio_below": 1.0,
    "minimum_winning_fraction": 0.8,
    "maximum_control_median_regression": 0.02,
    "bootstrap_resamples": 10000,
    "bootstrap_seed": 76077,
    "order": "ABBA on even rounds, BAAB on odd rounds; rotate/reverse cases; four excluded warmups per case",
    "retention": "Both target cases meet every gain criterion; every other case has median regression <=2%; otherwise withdraw.",
}


def paired(samples, rounds):
    ratios = []
    for number in range(rounds):
        selected = [s for s in samples if s["round"] == number]
        assert all(sum(s["variant"] == v for s in selected) == 2 for v in ("baseline", "candidate"))
        totals = {
            v: sum(s["complete_source_gc_ns"] for s in selected if s["variant"] == v) for v in ("baseline", "candidate")
        }
        ratios.append(totals["candidate"] / totals["baseline"])
    rng = random.Random(RULE["bootstrap_seed"])
    boot = sorted(statistics.median(rng.choices(ratios, k=len(ratios))) for _ in range(RULE["bootstrap_resamples"]))
    return {
        "ratios": ratios,
        "median_ratio": statistics.median(ratios),
        "bootstrap_95pct": [boot[round((len(boot) - 1) * 0.025)], boot[round((len(boot) - 1) * 0.975)]],
        "winning_rounds": sum(r < 1 for r in ratios),
        "rounds": rounds,
    }


def meets_gain(result):
    return (
        result["median_ratio"] <= 1 - RULE["minimum_median_gain"]
        and result["bootstrap_95pct"][1] < RULE["bootstrap_upper_ratio_below"]
        and result["winning_rounds"] >= RULE["minimum_winning_fraction"] * result["rounds"]
    )


def run(args):
    root = args.output_dir.resolve()
    root.mkdir(mode=0o700)
    inputs = args.inputs.resolve()
    source = json.loads(inputs.read_text())
    assert source["status"] == "completed" and source["decision"] == "baseline_only"
    drivers = {"baseline": args.baseline_driver.resolve(), "candidate": args.candidate_driver.resolve()}
    hashes = {v: hashlib.sha256(p.read_bytes()).hexdigest() for v, p in drivers.items()}
    assert hashes["baseline"] == source["driver_sha256"] and hashes["candidate"] != hashes["baseline"]
    private = root / "private"
    private.mkdir(mode=0o700)
    env = dict(os.environ, TMPDIR=str(private), VANE_RUNNER="local-fast")
    env.pop("VORTEX_SPFRESH_NATIVE", None)
    report = {
        "status": "running",
        "rule": RULE,
        "driver_sha256": hashes,
        "inputs_report_sha256": hashlib.sha256(inputs.read_bytes()).hexdigest(),
        "cases": [],
    }
    BASE.save(root / "decision-rule.json", RULE)
    started = time.monotonic()

    for original in source["cases"]:
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

    def measure(case, variant, kind, number, block):
        work, image = Path(case["work"]), Path(case["image"])
        assert BASE.fingerprint(BASE.manifest(image)) == case["input_image_sha256"]
        shutil.rmtree(work)
        shutil.copytree(image, work, copy_function=shutil.copy2)
        before = BASE.manifest(work)
        assert BASE.fingerprint(before) == case["input_image_sha256"]
        name = f"{kind}-{number:02d}-{block}-{variant}"
        diagnostic = kind == "diagnostic"
        with (Path(case["output"]) / (name + ".log")).open("w") as log:
            result = subprocess.run(
                [
                    str(drivers[variant]),
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
            kind=kind, variant=variant, round=number, block=block, restored_image_sha256=BASE.fingerprint(before)
        )
        BASE.save(Path(case["output"]) / (name + ".json"), sample)
        case["samples"].append(sample)

    try:
        for case in report["cases"]:
            for block, variant in enumerate(("baseline", "candidate", "candidate", "baseline")):
                measure(case, variant, "warmup", 0, block)
        for number in range(args.rounds):
            cases = report["cases"]
            offset = number % len(cases)
            order = cases[offset:] + cases[:offset]
            if number % 2:
                order = list(reversed(order))
            variants = ("baseline", "candidate", "candidate", "baseline")
            if number % 2:
                variants = ("candidate", "baseline", "baseline", "candidate")
            for case in order:
                for block, variant in enumerate(variants):
                    measure(case, variant, "primary", number, block)
            print(f"Completed paired collector round {number + 1}/{args.rounds}", flush=True)
            BASE.save(root / "report.json", report)
        for case in report["cases"]:
            for block, variant in enumerate(("baseline", "candidate")):
                measure(case, variant, "diagnostic", 0, block)
            primary = [s for s in case["samples"] if s["kind"] == "primary"]
            case["paired"] = paired(primary, args.rounds)
            case["timing"] = {v: BASE.summary([s for s in primary if s["variant"] == v]) for v in drivers}
            case["diagnostic"] = {
                v: BASE.diagnostic_summary(
                    [s for s in case["samples"] if s["kind"] == "diagnostic" and s["variant"] == v]
                )
                for v in drivers
            }
            assert BASE.fingerprint(BASE.manifest(Path(case["image"]))) == case["input_image_sha256"]
        expected = {
            (n, storage, task) for n in (8, 512, 4096) for storage in ("inline", "file") for task in ("expire", "files")
        }
        actual = {(c["history"], c["history_storage"], c["task"]) for c in report["cases"]}
        full = len(report["cases"]) == 12 and actual == expected and args.rounds >= RULE["minimum_rounds"]
        targets = [
            c
            for c in report["cases"]
            if c["task"] == "expire" and c["history_storage"] == "file" and c["history"] >= 512
        ]
        controls = [c for c in report["cases"] if c not in targets]
        retain = (
            full
            and all(meets_gain(c["paired"]) for c in targets)
            and all(c["paired"]["median_ratio"] <= 1 + RULE["maximum_control_median_regression"] for c in controls)
        )
        report.update(
            status="completed",
            decision="retain" if retain else "withdraw" if full else "smoke_only",
            semantic_checks="passed",
            input_images="unchanged",
        )
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
    parser.add_argument("--baseline-driver", type=Path, required=True)
    parser.add_argument("--candidate-driver", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--rounds", type=int, default=16)
    args = parser.parse_args()
    if not 1 <= args.rounds <= 64:
        parser.error("rounds must be within 1..64")
    run(args)
