#!/usr/bin/env python3
"""Pin a source-GC A/A experiment without changing its native work or thread settings."""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import time

SPEC = importlib.util.spec_from_file_location("gc_aa", Path(__file__).with_name("test_vane_vortex_source_gc_aa.py"))
AA = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(AA)
THREAD_ENV = (
    "OMP_NUM_THREADS",
    "OMP_DYNAMIC",
    "OMP_PROC_BIND",
    "OMP_PLACES",
    "GOMP_CPU_AFFINITY",
    "KMP_AFFINITY",
    "OPENBLAS_NUM_THREADS",
    "MKL_NUM_THREADS",
    "NUMEXPR_NUM_THREADS",
    "VECLIB_MAXIMUM_THREADS",
    "RAYON_NUM_THREADS",
    "TOKIO_WORKER_THREADS",
)
RULE = {
    "mode": "fixed_process_affinity_aa",
    "scope": "test orchestrator, A/A sampler and inherited native children",
    "phases": {"focus": 16, "full": 64, "smoke": 2},
    "advancement": "Run focus once; run full once only if both focus cases demonstrate the existing precision criterion. Otherwise finish with no full-matrix claim.",
    "precision": AA.RULE["precision"],
    "equivalence_band": AA.RULE["equivalence_band"],
    "thread_settings": "Unchanged binary, DuckDB SET threads=2, existing SPFresh settings and inherited thread environment. No thread-count overrides.",
    "isolation_scope": "Affinity restricts scheduling; it reserves no cores, pins no memory and changes no governor or frequency.",
}


def parse_cpus(value):
    try:
        values = [int(part) for part in value.split(",")]
    except ValueError as error:
        raise argparse.ArgumentTypeError("cpus must be comma-separated nonnegative integers") from error
    if not values or min(values) < 0 or len(set(values)) != len(values):
        raise argparse.ArgumentTypeError("cpus must be nonempty, nonnegative and unique")
    return sorted(values)


def topology(cpus):
    rows = []
    for cpu in cpus:
        root = Path(f"/sys/devices/system/cpu/cpu{cpu}")
        rows.append(
            {
                "cpu": cpu,
                "package": int((root / "topology/physical_package_id").read_text()),
                "core": int((root / "topology/core_id").read_text()),
                "siblings": (root / "topology/thread_siblings_list").read_text().strip(),
                "numa": sorted(p.name for p in root.glob("node[0-9]*")),
                "l3_shared": sorted(
                    (p / "shared_cpu_list").read_text().strip()
                    for p in (root / "cache").glob("index*")
                    if (p / "level").read_text().strip() == "3"
                ),
            }
        )
    return rows


def validate_cpus(cpus, available, rows, formal):
    assert cpus and len(cpus) == len(set(cpus)) and set(cpus) <= set(available), "CPU set is unavailable or duplicated"
    assert len(rows) == len(cpus) and {r["cpu"] for r in rows} == set(cpus)
    assert len({(r["package"], r["core"]) for r in rows}) == len(cpus), "CPU set shares an SMT core"
    assert (
        all(len(r["numa"]) == 1 for r in rows) and len({tuple(r["numa"]) for r in rows}) == 1
    ), "CPU set must have one known NUMA node"
    assert not formal or len(cpus) >= 2, "Formal CPU set must accommodate the unchanged DuckDB thread setting"


def thread_environment():
    return {key: os.environ.get(key) for key in THREAD_ENV}


def observations(cpus):
    rows = []
    for cpu in cpus:
        root = Path(f"/sys/devices/system/cpu/cpu{cpu}/cpufreq")
        row = {"cpu": cpu}
        for name in ("scaling_governor", "scaling_cur_freq"):
            path = root / name
            row[name] = path.read_text().strip() if path.is_file() else None
        rows.append(row)
    return {"monotonic_ns": time.monotonic_ns(), "load": os.getloadavg(), "cpus": rows}


@contextmanager
def pinned_affinity(cpus):
    original = os.sched_getaffinity(0)
    try:
        os.sched_setaffinity(0, cpus)
        assert os.sched_getaffinity(0) == set(cpus), "Affinity request was not applied exactly"
        yield
    finally:
        os.sched_setaffinity(0, original)
        assert os.sched_getaffinity(0) == original, "Orchestrator affinity was not restored"


def advance_after_focus(report):
    assert report["status"] == "completed" and report["profile"] == "focus"
    decision = AA.decision("focus", report["cases"])
    assert report["decision"] == decision
    return decision == "precision_demonstrated"


def run(args):
    root = args.output_dir.resolve()
    root.mkdir(mode=0o700)
    cpus, original = args.cpus, sorted(os.sched_getaffinity(0))
    assert set(cpus) <= set(original), "Requested CPU set is unavailable"
    rows = topology(cpus)
    validate_cpus(cpus, original, rows, args.profile == "formal")
    threads = thread_environment()
    tracked = [Path(__file__), Path(AA.__file__), Path(AA.AB.__file__), Path(AA.BASE.__file__)]
    hashes = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in tracked}
    report = {
        "status": "running",
        "profile": args.profile,
        "rule": RULE,
        "original_affinity": original,
        "requested_affinity": cpus,
        "topology": rows,
        "thread_environment": threads,
        "code_sha256": hashes,
        "observation_scope": "Frequency/governor are read-only boundary observations; null means the kernel exposes no such diagnostic. Affinity failure aborts sampling.",
        "phases": [],
    }
    AA.BASE.save(root / "decision-rule.json", dict(RULE, profile=args.profile, cpus=cpus))
    AA.BASE.save(root / "report.json", report)
    started = time.monotonic()
    try:
        with pinned_affinity(cpus):
            report["observations_before"] = observations(cpus)
            report["applied_affinity"] = sorted(os.sched_getaffinity(0))
            profiles = ("smoke",) if args.profile == "smoke" else ("focus", "full")
            for profile in profiles:
                if profile == "full" and not advance_after_focus(child):
                    report["full_phase"] = "not_started_focus_gate_failed"
                    break
                report["current_phase"] = profile
                AA.BASE.save(root / "report.json", report)
                with (root / (profile + ".log")).open("w") as log:
                    result = subprocess.run(
                        [
                            sys.executable,
                            "-I",
                            str(Path(AA.__file__).resolve()),
                            "--driver",
                            str(args.driver.resolve()),
                            "--inputs",
                            str(args.inputs.resolve()),
                            "--profile",
                            profile,
                            "--output-dir",
                            str(root / profile),
                        ],
                        stdout=log,
                        stderr=subprocess.STDOUT,
                    )
                assert result.returncode == 0, str(root / (profile + ".log"))
                path = root / profile / "report.json"
                child = json.loads(path.read_text())
                assert child["status"] == "completed" and child["profile"] == profile
                assert child["host"]["affinity"] == cpus and os.sched_getaffinity(0) == set(cpus)
                assert thread_environment() == threads
                if profile == "full":
                    report["full_phase"] = "completed"
                report["phases"].append(
                    {
                        "profile": profile,
                        "decision": child["decision"],
                        "report": str(path),
                        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                        "observations_after": observations(cpus),
                    }
                )
                AA.BASE.save(root / "report.json", report)
                print(profile, child["decision"], flush=True)
            report["observations_after"] = observations(cpus)
            assert topology(cpus) == rows, "CPU topology changed during sampling"
            assert all(hashlib.sha256(p.read_bytes()).hexdigest() == hashes[p.name] for p in tracked)
            assert thread_environment() == threads
            report["decision"] = "smoke_only" if args.profile == "smoke" else child["decision"]
        report.update(status="completed", restored_affinity=sorted(os.sched_getaffinity(0)))
    except BaseException as error:
        report.update(status="failed", error=f"{type(error).__name__}: {error}")
        raise
    finally:
        report["seconds"] = time.monotonic() - started
        AA.BASE.save(root / "report.json", report)
    print("Decision: " + report["decision"], flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--inputs", type=Path, required=True)
    parser.add_argument("--driver", type=Path, required=True)
    parser.add_argument("--cpus", type=parse_cpus, required=True)
    parser.add_argument("--profile", choices=("formal", "smoke"), required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    run(parser.parse_args())
