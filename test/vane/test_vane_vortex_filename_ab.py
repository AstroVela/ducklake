#!/usr/bin/env python3
"""Run the predeclared matched-object filename-prefilter A/B after its oracle passes."""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import time

SPEC = importlib.util.spec_from_file_location(
    "gc_pin", Path(__file__).with_name("test_vane_vortex_source_gc_affinity.py")
)
PIN = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PIN)
AA, AB, BASE = PIN.AA, PIN.AA.AB, PIN.AA.BASE
TARGETS = {(512, "file", "files"), (4096, "file", "files")}
RULE = {
    **AB.RULE,
    "scope": "complete_source_gc_ns; no SQL callback in primary calls",
    "target": "materialized-history file unlink at 512 and 4096 updates",
    "profiles": {"formal": 16, "smoke": 2},
    "affinity": "Explicit fixed CPU set; formal uses 6–11, no thread changes",
    "linking": "Same baseline headers/compile flags/source coordinates/explicit metadata-object position on both sides",
    "sampling": "One complete fixed matrix; stop for execution, semantic or integrity failure; no adaptive counts or rerun-until-pass",
    "retention": "Both file-unlink targets meet the unchanged >=2% median gain, upper95<1 and >=80% winning-round gates; all ten controls have <=2% median regression. Otherwise withdraw.",
    "interpretation": "Candidate qualification within this finite controlled trial; no old A/A reclassification or sustained-stability claim",
}


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def decision(cases, profile):
    keys = [(c["history"], c["history_storage"], c["task"]) for c in cases]
    expected = AA.SMOKE if profile == "smoke" else AA.FULL
    assert len(keys) == len(set(keys)) and set(keys) == expected
    assert all(c["paired"]["rounds"] == RULE["profiles"][profile] for c in cases)
    if profile == "smoke":
        return "smoke_only"
    targets = [c for c, key in zip(cases, keys) if key in TARGETS]
    controls = [c for c, key in zip(cases, keys) if key not in TARGETS]
    assert len(targets) == 2 and len(controls) == 10
    return (
        "candidate_qualified"
        if all(AB.meets_gain(c["paired"]) for c in targets)
        and all(c["paired"]["median_ratio"] <= 1 + RULE["maximum_control_median_regression"] for c in controls)
        else "withdraw"
    )


def run(args):
    root, inputs = args.output_dir.resolve(), args.inputs.resolve()
    root.mkdir(mode=0o700)
    build_path, oracle_path = args.build_report.resolve(), args.oracle_report.resolve()
    build, oracle = json.loads(build_path.read_text()), json.loads(oracle_path.read_text())
    assert build["mode"] == "matched_filename_trial" and oracle["status"] == "passed"
    assert oracle["profile"] == args.profile
    assert all(digest(Path(p)) == h for p, h in oracle["identities"].items())
    assert oracle["identities"][str(build_path)] == digest(build_path)
    source = json.loads(inputs.read_text())
    assert source["status"] == "completed" and source["decision"] == "baseline_only"
    assert digest(args.producer_driver) == source["driver_sha256"]
    assert oracle["identities"][str(inputs)] == digest(inputs)
    selected = AA.select_cases("smoke" if args.profile == "smoke" else "full", source["cases"])
    drivers = {v: Path(build["executables"][v]["benchmark"]["path"]) for v in AA.LABELS}
    tracked_files = [Path(__file__), Path(PIN.__file__), Path(AA.__file__), Path(AB.__file__), Path(BASE.__file__)]
    tracked = {
        **build["source_sha256"],
        **build["link_inputs_sha256"],
        **{str(p): digest(p) for p in tracked_files},
        str(inputs): digest(inputs),
        str(args.producer_driver.resolve()): source["driver_sha256"],
        str(build_path): digest(build_path),
        str(oracle_path): digest(oracle_path),
    }
    tracked.update({str(p): build["executables"][v]["benchmark"]["sha256"] for v, p in drivers.items()})
    assert all(digest(Path(p)) == h for p, h in tracked.items())
    assert digest(drivers["baseline"]) != digest(drivers["candidate"])
    cpus, original = args.cpus, sorted(os.sched_getaffinity(0))
    rows, threads = PIN.topology(cpus), PIN.thread_environment()
    PIN.validate_cpus(cpus, original, rows, args.profile == "formal")
    private = root / "private"
    private.mkdir(mode=0o700)
    env = dict(os.environ, TMPDIR=str(private), VANE_RUNNER="local-fast")
    env.pop("VORTEX_SPFRESH_NATIVE", None)
    rounds = RULE["profiles"][args.profile]
    report = {
        "status": "running",
        "profile": args.profile,
        "rule": RULE,
        "identities": tracked,
        "build_report": str(build_path),
        "oracle_report": str(oracle_path),
        "original_affinity": original,
        "requested_affinity": cpus,
        "topology": rows,
        "thread_environment": threads,
        "cases": [],
    }
    for item in selected:
        case = {
            k: item[k]
            for k in ("history", "history_storage", "task", "input", "root", "image", "work", "input_image_sha256")
        }
        name = f"history-{case['history']}-{case['history_storage']}-{case['task']}"
        expected = inputs.parent / name
        assert Path(case["root"]) == expected and expected.resolve() == expected
        assert Path(case["work"]) == expected / "work" and Path(case["image"]) == expected / "image"
        assert not Path(case["work"]).is_symlink() and not Path(case["image"]).is_symlink()
        case.update(output=str(root / name), samples=[])
        Path(case["output"]).mkdir(mode=0o700)
        report["cases"].append(case)
    BASE.save(root / "decision-rule.json", dict(RULE, cpus=cpus, profile=args.profile))
    BASE.save(root / "report.json", report)

    def measure(case, variant, kind, number, block):
        work, image = Path(case["work"]), Path(case["image"])
        assert BASE.fingerprint(BASE.manifest(image)) == case["input_image_sha256"]
        shutil.rmtree(work)
        shutil.copytree(image, work, copy_function=shutil.copy2)
        before = BASE.manifest(work)
        assert BASE.fingerprint(before) == case["input_image_sha256"]
        assert os.sched_getaffinity(0) == set(cpus) and PIN.thread_environment() == threads
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
        assert all(after.get(p) == h for p, h in before.items() if p.startswith("indexes/"))
        sample.update(
            kind=kind, variant=variant, round=number, block=block, restored_image_sha256=BASE.fingerprint(before)
        )
        BASE.save(Path(case["output"]) / (name + ".json"), sample)
        case["samples"].append(sample)

    started = time.monotonic()
    try:
        with PIN.pinned_affinity(cpus):
            report["applied_affinity"] = sorted(os.sched_getaffinity(0))
            report["observations_before"] = PIN.observations(cpus)
            for case in report["cases"]:
                for block, variant in enumerate(("baseline", "candidate", "candidate", "baseline")):
                    measure(case, variant, "warmup", 0, block)
            for number in range(rounds):
                cases = report["cases"]
                offset = number % len(cases)
                order = cases[offset:] + cases[:offset]
                if number % 2:
                    order = list(reversed(order))
                variants = (
                    ("baseline", "candidate", "candidate", "baseline")
                    if number % 2 == 0
                    else ("candidate", "baseline", "baseline", "candidate")
                )
                for case in order:
                    for block, variant in enumerate(variants):
                        measure(case, variant, "primary", number, block)
                report["completed_rounds"] = number + 1
                BASE.save(root / "report.json", report)
                print(f"Completed filename A/B round {number + 1}/{rounds}", flush=True)
            for case in report["cases"]:
                for block, variant in enumerate(AA.LABELS):
                    measure(case, variant, "diagnostic", 0, block)
                primary = [s for s in case["samples"] if s["kind"] == "primary"]
                assert len(primary) == rounds * 4 and all(not s["diagnostic"] for s in primary)
                case["paired"] = AB.paired(primary, rounds)
                case["timing"] = {v: BASE.summary([s for s in primary if s["variant"] == v]) for v in AA.LABELS}
                case["diagnostic"] = {
                    v: BASE.diagnostic_summary(
                        [s for s in case["samples"] if s["kind"] == "diagnostic" and s["variant"] == v]
                    )
                    for v in AA.LABELS
                }
                assert BASE.fingerprint(BASE.manifest(Path(case["image"]))) == case["input_image_sha256"]
            report["observations_after"] = PIN.observations(cpus)
            assert all(digest(Path(p)) == h for p, h in tracked.items())
            assert PIN.topology(cpus) == rows and PIN.thread_environment() == threads
            assert os.sched_getaffinity(0) == set(cpus)
        report.update(
            status="completed",
            decision=decision(report["cases"], args.profile),
            restored_affinity=sorted(os.sched_getaffinity(0)),
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
    parser.add_argument("--build-report", type=Path, required=True)
    parser.add_argument("--oracle-report", type=Path, required=True)
    parser.add_argument("--producer-driver", type=Path, required=True)
    parser.add_argument("--cpus", type=PIN.parse_cpus, required=True)
    parser.add_argument("--profile", choices=("formal", "smoke"), required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    run(parser.parse_args())
