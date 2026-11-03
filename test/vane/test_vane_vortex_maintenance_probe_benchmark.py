#!/usr/bin/env python3
"""Repeat matched native maintenance read probes and decide whether to retain query fusion."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import random
import statistics
import subprocess
import time


RULE = {
    "primary_scope": "complete_read_probe_ns",
    "minimum_rounds": 16,
    "minimum_median_gain": 0.02,
    "bootstrap_upper_ratio_below": 1.0,
    "minimum_winning_fraction": 0.8,
    "maximum_small_case_regression": 0.02,
    "bootstrap_resamples": 10000,
    "bootstrap_seed": 74174,
    "retention": "All medium/large live/stale cases must meet every gain criterion; small cases must not regress >2%. Otherwise withdraw.",
    "reference_split": "9819016e841ea79c8eea054415f4724fa2270e85",
    "reference_combined": "80f4110ac45c555fbcd29e5dab52d0059983a339",
}
STAGES = [
    "metadata_open_ns",
    "policy_backlog_ns",
    "source_readiness_ns",
    "commit_ns",
    "connection_close_ns",
    "complete_read_probe_ns",
]


def percentile(values, fraction):
    ordered = sorted(values)
    return ordered[round((len(ordered) - 1) * fraction)]


def paired_summary(report):
    samples = report["samples"]
    rounds = sorted({s["round"] for s in samples})
    ratios = []
    for number in rounds:
        selected = [s for s in samples if s["round"] == number]
        totals = {
            variant: sum(s["complete_read_probe_ns"] for s in selected if s["variant"] == variant)
            for variant in ["split", "combined"]
        }
        assert all(sum(s["variant"] == variant for s in selected) == 2 * report["batch"] for variant in totals)
        ratios.append(totals["combined"] / totals["split"])
    rng = random.Random(RULE["bootstrap_seed"])
    bootstrap = [statistics.median(rng.choices(ratios, k=len(ratios))) for _ in range(RULE["bootstrap_resamples"])]
    return {
        "paired_round_ratios": ratios,
        "median_ratio": statistics.median(ratios),
        "bootstrap_median_ratio_95pct": [percentile(bootstrap, 0.025), percentile(bootstrap, 0.975)],
        "winning_rounds": sum(ratio < 1 for ratio in ratios),
        "rounds": len(rounds),
        "variants": {
            variant: {
                stage: {
                    "mean_ms": statistics.mean(s[stage] for s in samples if s["variant"] == variant) / 1e6,
                    "p50_ms": statistics.median(s[stage] for s in samples if s["variant"] == variant) / 1e6,
                    "p95_ms": percentile([s[stage] for s in samples if s["variant"] == variant], 0.95) / 1e6,
                }
                for stage in STAGES
            }
            for variant in ["split", "combined"]
        },
    }


def diagnostic_summary(report):
    result = {}
    for variant in ["split", "combined"]:
        selected = [s for s in report["samples"] if s["variant"] == variant]
        expected = 2 if variant == "split" else 1
        assert all(len(s["queries"]) == expected for s in selected)
        result[variant] = {
            "statements_per_probe": expected,
            "bind_plan_mean_ms": statistics.mean(sum(q["bind_plan_ns"] for q in s["queries"]) for s in selected) / 1e6,
            "execute_fetch_mean_ms": statistics.mean(sum(q["execute_fetch_ns"] for q in s["queries"]) for s in selected)
            / 1e6,
            "representative_sql": [q["sql"] for q in selected[0]["queries"]],
        }
    return result


def meets_gain(summary):
    return (
        summary["median_ratio"] <= 1 - RULE["minimum_median_gain"]
        and summary["bootstrap_median_ratio_95pct"][1] < RULE["bootstrap_upper_ratio_below"]
        and summary["winning_rounds"] >= RULE["minimum_winning_fraction"] * summary["rounds"]
    )


def run(args):
    root = args.output_dir.resolve()
    root.mkdir(mode=0o700)
    driver = args.driver.resolve()
    env = dict(os.environ, VANE_RUNNER="local-fast")
    env.pop("VORTEX_SPFRESH_NATIVE", None)
    private = root / "private"
    private.mkdir(mode=0o700)
    env["TMPDIR"] = str(private)
    report = {
        "status": "running",
        "decision_rule": RULE,
        "driver_sha256": hashlib.sha256(driver.read_bytes()).hexdigest(),
        "histories": args.histories,
        "rounds": args.rounds,
        "batch": args.batch,
        "order": "ABBA on even rounds; BAAB on odd rounds; 12 alternating warmups discarded",
        "cases": [],
    }
    # Freeze the decision rule before either implementation is measured.
    (root / "decision-rule.json").write_text(json.dumps(RULE, indent=2) + "\n")
    started = time.monotonic()

    def command(case, mode, history, task, diagnostic=False):
        values = [
            str(driver),
            str(case),
            mode,
            str(history),
            task,
            str(3 if diagnostic else args.rounds),
            str(3 if diagnostic else args.batch),
            "1" if diagnostic else "0",
        ]
        log_name = "diagnostic.log" if diagnostic else mode + ".log"
        with (case / log_name).open("w") as log:
            result = subprocess.run(values, env=env, stdout=log, stderr=subprocess.STDOUT, timeout=900)
        if result.returncode:
            raise RuntimeError(f"Native benchmark failed: {case / log_name}\n{(case / log_name).read_text()[-6000:]}")

    try:
        for history in args.histories:
            for task in ["live", "stale"]:
                case = root / f"history-{history}-{task}"
                case.mkdir(mode=0o700)
                command(case, "prepare", history, task)
                print(f"Prepared immutable input: history={history}, task={task}", flush=True)
                command(case, "run", history, task)
                primary = json.loads((case / "timing.json").read_text())
                assert not primary["diagnostic"] and primary["input_before"] == primary["input_after"]
                summary = paired_summary(primary)
                report["cases"].append(
                    {
                        "history": history,
                        "task": task,
                        "input": primary["input_before"],
                        "timing": summary,
                        "root": str(case),
                    }
                )
                (root / "report.json").write_text(json.dumps(report, indent=2) + "\n")
                print(
                    f"Measured history={history}, task={task}: paired median ratio={summary['median_ratio']:.6f}",
                    flush=True,
                )
        # Diagnostics run separately after primary measurements; their callback/AST-copy cost is not a decision input.
        for case_result in report["cases"]:
            case = Path(case_result["root"])
            command(case, "run", case_result["history"], case_result["task"], diagnostic=True)
            diagnostic = json.loads((case / "diagnostic.json").read_text())
            assert diagnostic["input_before"] == diagnostic["input_after"] == case_result["input"]
            case_result["diagnostic"] = diagnostic_summary(diagnostic)
        full = args.histories == [8, 512, 4096] and args.rounds >= RULE["minimum_rounds"]
        medium_large = [c for c in report["cases"] if c["history"] >= 512]
        small = [c for c in report["cases"] if c["history"] < 512]
        retain = (
            full
            and all(meets_gain(c["timing"]) for c in medium_large)
            and all(c["timing"]["median_ratio"] <= 1 + RULE["maximum_small_case_regression"] for c in small)
        )
        report.update(
            status="completed",
            decision="retain" if retain else "withdraw" if full else "smoke_only",
            full_decision_matrix=full,
            semantic_checks="passed",
            readonly_inputs="unchanged",
        )
        print(f"Decision: {report['decision']}", flush=True)
    finally:
        report["seconds"] = time.monotonic() - started
        (root / "report.json").write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--driver", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--histories", type=int, nargs="+", default=[8, 512, 4096])
    parser.add_argument("--rounds", type=int, default=16)
    parser.add_argument("--batch", type=int, default=10)
    args = parser.parse_args()
    if (
        sorted(set(args.histories)) != args.histories
        or not args.histories
        or any(n < 0 or n > 10000 for n in args.histories)
    ):
        parser.error("histories must be distinct, increasing and within 0..10000")
    if not 1 <= args.rounds <= 128 or not 1 <= args.batch <= 100:
        parser.error("invalid round or batch count")
    run(args)
