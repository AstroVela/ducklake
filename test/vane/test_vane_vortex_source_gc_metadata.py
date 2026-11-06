#!/usr/bin/env python3
"""Repeat callback-enabled source-GC diagnostics without changing the runtime."""
from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import shutil
import statistics
import subprocess
import sys
import time

SPEC = importlib.util.spec_from_file_location(
    "gc_pin", Path(__file__).with_name("test_vane_vortex_source_gc_affinity.py")
)
PIN = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PIN)
AA, BASE = PIN.AA, PIN.AA.BASE
CATALOG = '"__ducklake_metadata_lake"."main".'
EXPECTED = {
    "known_source_paths": 4,
    "scheduled_ownership": 4,
    "index_references": 4,
    "begin": 10,
    "commit": 5,
    "other_sql": 15,
}
RULE = {
    "mode": "repeated_metadata_diagnostics",
    "profiles": {"formal": 16, "smoke": 2},
    "warmups_per_case": 4,
    "order": "Four ABBA calls on even rounds, BAAB on odd rounds; rotate cases, reverse odd rounds. Both labels use one identical executable.",
    "sampling": "Fixed counts, one run; stop for execution, semantic, trace or integrity failure. No adaptive sampling or timing gate.",
    "interpretation": "Callback-enabled diagnostic distributions only. No primary performance samples, equivalence verdict, causal attribution or sustained-load claim.",
    "partition": "Sum disjoint depth-zero SQL scopes; nested COMMIT is validated but excluded. Outside-SQL time completes the collector partition. Native phase timers are a separate, overlapping view.",
    "split": "Report bind/plan and execute/fetch only when both are observed; retain unsplit time explicitly. Execution/fetch includes unseparated waiting.",
    "variation": "Cov(part,total)/Var(total) is a signed descriptive covariance share, not causal variance explained; shares sum to one. Constant total gives null.",
    "thread_settings": PIN.RULE["thread_settings"],
    "expected_outer_calls": EXPECTED,
}


def block_labels(number):
    return (
        ("baseline", "candidate", "candidate", "baseline")
        if number % 2 == 0
        else ("candidate", "baseline", "baseline", "candidate")
    )


def category(sql):
    if sql.startswith("SELECT REPLACE("):
        assert all(
            token in sql
            for token in (
                " AS full_path",
                CATALOG + "ducklake_data_file f",
                CATALOG + "ducklake_delete_file f",
                CATALOG + "ducklake_table t",
                CATALOG + "ducklake_schema s",
            )
        )
        assert "ducklake_files_scheduled_for_deletion" not in sql
        return "known_source_paths"
    if sql.startswith("SELECT count(*) FROM " + CATALOG + "ducklake_files_scheduled_for_deletion WHERE data_file_id="):
        return "scheduled_ownership"
    if sql == "SELECT table_uuid,reference FROM " + CATALOG + "ducklake_vortex_indexes_v1":
        return "index_references"
    if sql == "BEGIN TRANSACTION":
        return "begin"
    if sql == "COMMIT":
        return "commit"
    return "other_sql"


def trace_partition(sample):
    assert sample["diagnostic"] is True and sample["kind"] in ("diagnostic", "warmup")
    total, queries = sample["complete_source_gc_ns"], sample["queries"]
    assert isinstance(total, int) and total > 0
    for q in queries:
        assert isinstance(q["depth"], int) and q["depth"] >= 0
        assert q["sql"] and 0 <= q["begin_ns"] <= q["end_ns"] <= total
        assert q["query_ns"] == q["end_ns"] - q["begin_ns"]
        split = ("bind_plan_ns" in q, "execute_fetch_ns" in q)
        assert split[0] == split[1], "partial planning split"
        if split[0]:
            assert min(q["bind_plan_ns"], q["execute_fetch_ns"]) >= 0
            assert q["bind_plan_ns"] + q["execute_fetch_ns"] == q["query_ns"]
        if q["depth"]:
            assert (
                sum(
                    p["depth"] == q["depth"] - 1 and p["begin_ns"] <= q["begin_ns"] and q["end_ns"] <= p["end_ns"]
                    for p in queries
                )
                == 1
            ), "nested query needs one containing parent"
    top = sorted((q for q in queries if q["depth"] == 0), key=lambda q: q["begin_ns"])
    assert all(a["end_ns"] <= b["begin_ns"] for a, b in zip(top, top[1:])), "outer SQL scopes overlap"
    nested = [q for q in queries if q["depth"]]
    assert len(nested) == 5 and all(q["depth"] == 1 and q["sql"] == "COMMIT" for q in nested)
    counts = Counter(category(q["sql"]) for q in top)
    assert dict(counts) == EXPECTED, "unexpected outer SQL inventory"
    checkpoints = sample["checkpoints"]
    expected_stages = ["source_gc_waiting"] + [
        "source_before_plan",
        "source_delete_intent_committed",
        "source_before_unlink",
        "source_after_unlink",
        "source_delete_completed",
    ] * 2
    assert [c["stage"] for c in checkpoints] == expected_stages
    times = [c["elapsed_ns"] for c in checkpoints]
    assert times == sorted(times) and 0 <= times[0] <= times[-1] <= total
    parts = {key: sum(q["query_ns"] for q in top if category(q["sql"]) == key) for key in EXPECTED}
    parts["outside_sql"] = total - sum(parts.values())
    assert min(parts.values()) >= 0 and sum(parts.values()) == total
    return parts, top


def distribution(values):
    assert values and all(math.isfinite(v) and v >= 0 for v in values)
    ordered = sorted(values)
    return {
        "mean": statistics.mean(values) / 1e6,
        "p50": statistics.median(values) / 1e6,
        "p95": ordered[math.ceil(0.95 * len(values)) - 1] / 1e6,
        "min": ordered[0] / 1e6,
        "max": ordered[-1] / 1e6,
    }


def covariance_shares(parts, totals):
    assert len(totals) >= 2 and all(len(values) == len(totals) for values in parts.values())
    assert all(sum(values[i] for values in parts.values()) == total for i, total in enumerate(totals))
    center = statistics.mean(totals)
    denominator = sum((v - center) ** 2 for v in totals)
    if denominator == 0:
        return {key: None for key in parts}
    result = {
        key: sum((x - statistics.mean(values)) * (y - center) for x, y in zip(values, totals)) / denominator
        for key, values in parts.items()
    }
    assert math.isclose(sum(result.values()), 1.0, rel_tol=1e-9, abs_tol=1e-9)
    return result


def validate_inventory(samples, rounds):
    assert len(samples) == rounds * 4
    actual = [(s["round"], s["block"], s["variant"]) for s in samples]
    expected = [(r, b, label) for r in range(rounds) for b, label in enumerate(block_labels(r))]
    assert sorted(actual) == sorted(expected), "missing, duplicate or reordered label inventory"
    assert all(s["kind"] == "diagnostic" and s["diagnostic"] is True for s in samples)


def summarize(samples):
    assert samples and all(s["kind"] == "diagnostic" and s["diagnostic"] is True for s in samples)
    traces = [trace_partition(s) for s in samples]
    totals = [s["complete_source_gc_ns"] for s in samples]
    parts = {key: [p[key] for p, _ in traces] for key in traces[0][0]}
    shares = covariance_shares(parts, totals)
    groups = {}
    for _, top in traces:
        for q in top:
            groups.setdefault(q["sql"], []).append(q)
    exact = []
    for sql, queries in groups.items():
        observed = [q for q in queries if "bind_plan_ns" in q]
        unsplit = [q for q in queries if "bind_plan_ns" not in q]
        exact.append(
            {
                "sql": sql,
                "sha256": hashlib.sha256(sql.encode()).hexdigest(),
                "category": category(sql),
                "calls": len(queries),
                "per_collector_ms": sum(q["query_ns"] for q in queries) / len(samples) / 1e6,
                "observed_split_calls": len(observed),
                "bind_plan_total_ms": sum(q["bind_plan_ns"] for q in observed) / 1e6,
                "execute_fetch_total_ms": sum(q["execute_fetch_ns"] for q in observed) / 1e6,
                "unsplit_calls": len(unsplit),
                "unsplit_total_ms": sum(q["query_ns"] for q in unsplit) / 1e6,
            }
        )
    splits = {}
    for key in EXPECTED:
        per_call = [[q for q in top if category(q["sql"]) == key] for _, top in traces]
        splits[key] = {
            "observed_split_calls": sum("bind_plan_ns" in q for queries in per_call for q in queries),
            "unsplit_calls": sum("bind_plan_ns" not in q for queries in per_call for q in queries),
            "bind_plan_ms": distribution(
                [sum(q["bind_plan_ns"] for q in queries if "bind_plan_ns" in q) for queries in per_call]
            ),
            "execute_fetch_ms": distribution(
                [sum(q["execute_fetch_ns"] for q in queries if "bind_plan_ns" in q) for queries in per_call]
            ),
            "unsplit_ms": distribution(
                [sum(q["query_ns"] for q in queries if "bind_plan_ns" not in q) for queries in per_call]
            ),
        }
    return {
        "samples": len(samples),
        "complete_collector_ms": distribution(totals),
        "native_phases_ms": {
            p["stage"]: distribution([s["phases"][i]["total_ns"] for s in samples])
            for i, p in enumerate(samples[0]["phases"])
        },
        "outer_partition": {
            key: {
                "time_ms": distribution(values),
                "fraction_of_mean_total": statistics.mean(values) / statistics.mean(totals),
                "covariance_share": shares[key],
            }
            for key, values in parts.items()
        },
        "outer_planning_splits": splits,
        "round_mean_ms": {
            str(r): statistics.mean(s["complete_source_gc_ns"] for s in samples if s["round"] == r) / 1e6
            for r in sorted({s["round"] for s in samples})
        },
        "exact_outer_sql": sorted(exact, key=lambda q: q["per_collector_ms"], reverse=True),
        "excluded_nested_queries": sum(len(s["queries"]) - len(top) for s, (_, top) in zip(samples, traces)),
    }


def run(args):
    root, inputs, driver = args.output_dir.resolve(), args.inputs.resolve(), args.driver.resolve()
    root.mkdir(mode=0o700)
    source = json.loads(inputs.read_text())
    assert source["status"] == "completed" and source["decision"] == "baseline_only"
    selected = AA.select_cases("smoke" if args.profile == "smoke" else "focus", source["cases"])
    selected = [c for c in selected if c["task"] == "files"]
    assert len(selected) == 2
    driver_hash = hashlib.sha256(driver.read_bytes()).hexdigest()
    assert driver_hash == source["driver_sha256"]
    cpus, original = args.cpus, sorted(os.sched_getaffinity(0))
    rows, threads = PIN.topology(cpus), PIN.thread_environment()
    PIN.validate_cpus(cpus, original, rows, args.profile == "formal")
    tracked = [Path(__file__), Path(PIN.__file__), Path(AA.__file__), Path(AA.AB.__file__), Path(BASE.__file__)]
    hashes = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in tracked}
    private = root / "private"
    private.mkdir(mode=0o700)
    env = dict(os.environ, TMPDIR=str(private), VANE_RUNNER="local-fast")
    env.pop("VORTEX_SPFRESH_NATIVE", None)
    rounds = RULE["profiles"][args.profile]
    report = {
        "status": "running",
        "profile": args.profile,
        "rule": RULE,
        "rounds": rounds,
        "driver": str(driver),
        "driver_sha256": driver_hash,
        "inputs_report_sha256": hashlib.sha256(inputs.read_bytes()).hexdigest(),
        "code_sha256": hashes,
        "original_affinity": original,
        "requested_affinity": cpus,
        "topology": rows,
        "thread_environment": threads,
        "cases": [],
    }
    for original_case in selected:
        case = {
            k: original_case[k]
            for k in ("history", "history_storage", "task", "input", "image", "work", "root", "input_image_sha256")
        }
        name = f"history-{case['history']}-{case['history_storage']}-{case['task']}"
        expected = inputs.parent / name
        assert Path(case["root"]) == expected and expected.resolve() == expected
        assert Path(case["work"]) == expected / "work" and Path(case["image"]) == expected / "image"
        assert not Path(case["work"]).is_symlink() and not Path(case["image"]).is_symlink()
        case.update(output=str(root / name), samples=[])
        Path(case["output"]).mkdir(mode=0o700)
        report["cases"].append(case)
    BASE.save(root / "measurement-contract.json", dict(RULE, profile=args.profile, rounds=rounds, cpus=cpus))
    BASE.save(root / "report.json", report)

    def measure(case, label, kind, number, block):
        work, image = Path(case["work"]), Path(case["image"])
        assert BASE.fingerprint(BASE.manifest(image)) == case["input_image_sha256"]
        shutil.rmtree(work)
        shutil.copytree(image, work, copy_function=shutil.copy2)
        before = BASE.manifest(work)
        assert BASE.fingerprint(before) == case["input_image_sha256"]
        assert os.sched_getaffinity(0) == set(cpus) and PIN.thread_environment() == threads
        name = f"{kind}-{number:02d}-{block}-{label}"
        with (Path(case["output"]) / (name + ".log")).open("w") as log:
            result = subprocess.run(
                [str(driver), str(work), "run", str(case["history"]), "files", "1", case["history_storage"]],
                env=env,
                stdout=log,
                stderr=subprocess.STDOUT,
                timeout=90,
            )
        assert result.returncode == 0, str(Path(case["output"]) / (name + ".log"))
        sample = json.loads((work / "sample.json").read_text())
        BASE.validate_sample(sample, case["input"], True)
        after = BASE.manifest(work)
        old_data, new_data = ({p for p in m if p.startswith("data/")} for m in (before, after))
        deleted = {Path(p).relative_to(work).as_posix() for p in sample["deleted_paths"]}
        assert old_data - new_data == deleted and not new_data - old_data
        assert all(before[p] == after[p] for p in new_data)
        assert all(after.get(p) == value for p, value in before.items() if p.startswith("indexes/"))
        sample.update(
            kind=kind, variant=label, round=number, block=block, restored_image_sha256=BASE.fingerprint(before)
        )
        sample["outer_partition_ns"], _ = trace_partition(sample)
        BASE.save(Path(case["output"]) / (name + ".json"), sample)
        case["samples"].append(sample)

    started = time.monotonic()
    try:
        with PIN.pinned_affinity(cpus):
            probe = subprocess.check_output(
                [
                    sys.executable,
                    "-I",
                    "-c",
                    "import os,json; keys="
                    + repr(PIN.THREAD_ENV)
                    + "; print(json.dumps({'cpus':sorted(os.sched_getaffinity(0)),'threads':{k:os.environ.get(k) for k in keys}}))",
                ],
                env=env,
                text=True,
            )
            report["child_probe"] = json.loads(probe)
            assert report["child_probe"] == {"cpus": cpus, "threads": threads}
            report["applied_affinity"] = sorted(os.sched_getaffinity(0))
            report["observations_before"] = PIN.observations(cpus)
            for case in report["cases"]:
                for block, label in enumerate(block_labels(0)):
                    measure(case, label, "warmup", 0, block)
            for number in range(rounds):
                cases = report["cases"]
                offset = number % len(cases)
                order = cases[offset:] + cases[:offset]
                if number % 2:
                    order = list(reversed(order))
                for case in order:
                    for block, label in enumerate(block_labels(number)):
                        measure(case, label, "diagnostic", number, block)
                report["completed_rounds"] = number + 1
                BASE.save(root / "report.json", report)
                print(f"Completed diagnostic round {number + 1}/{rounds}", flush=True)
            for case in report["cases"]:
                samples = [s for s in case["samples"] if s["kind"] == "diagnostic"]
                validate_inventory(samples, rounds)
                case["summary"] = summarize(samples)
                case["label_summaries"] = {
                    label: summarize([s for s in samples if s["variant"] == label]) for label in AA.LABELS
                }
                assert BASE.fingerprint(BASE.manifest(Path(case["image"]))) == case["input_image_sha256"]
            report["observations_after"] = PIN.observations(cpus)
            assert PIN.topology(cpus) == rows and PIN.thread_environment() == threads
            assert os.sched_getaffinity(0) == set(cpus)
            assert hashlib.sha256(driver.read_bytes()).hexdigest() == driver_hash
            assert hashlib.sha256(inputs.read_bytes()).hexdigest() == report["inputs_report_sha256"]
            assert all(hashlib.sha256(p.read_bytes()).hexdigest() == hashes[p.name] for p in tracked)
        report.update(
            status="completed",
            restored_affinity=sorted(os.sched_getaffinity(0)),
            decision="smoke_only" if args.profile == "smoke" else "diagnostics_only",
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
    parser.add_argument("--driver", type=Path, required=True)
    parser.add_argument("--cpus", type=PIN.parse_cpus, required=True)
    parser.add_argument("--profile", choices=tuple(RULE["profiles"]), required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    run(parser.parse_args())
