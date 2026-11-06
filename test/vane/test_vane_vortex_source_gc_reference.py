#!/usr/bin/env python3
"""Locate post-query reference work in a separate, instrumented diagnostic."""
from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import time

SPEC = importlib.util.spec_from_file_location(
    "gc_metadata", Path(__file__).with_name("test_vane_vortex_source_gc_metadata.py")
)
META = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(META)
BASE, PIN = META.BASE, META.PIN
RULE = {
    "mode": "private_reference_diagnostics",
    "profiles": META.RULE["profiles"],
    "sampling": META.RULE["sampling"],
    "warmups_per_case": META.RULE["warmups_per_case"],
    "order": META.RULE["order"],
    "oracle": "Once per selected case before sampling: live reference, lexical alias, absent reference, query-error propagation; roll back metadata edits and verify original logical state.",
    "producer": "Original sealed images and producing executable stay unchanged. A separate declaration identifies the private instrumented consumer and retains producer provenance.",
    "path_loop": "One timer per nonempty chunk: Value/string extraction, CanonicalizePath and comparison together, including diagnostic overhead. No per-row clock and no separate pure canonicalization estimate.",
    "partition": "Match each of four reference call scopes to exactly one contained known-source SQL query on the same metadata connection. Replace outside-SQL with reference Fetch, path loop, reference remainder and remaining outside-reference time.",
    "interpretation": "Instrumented diagnostics only; no optimization or primary/A/A verdict. Do not subtract times across builds or attribute the old outside-SQL total from this trace alone.",
}


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def partition(sample):
    original, top = META.trace_partition(sample)
    calls = sample["reference_calls"]
    assert len(calls) == 4
    expected_paths = Counter({os.path.normpath(p): 2 for p in sample["deleted_paths"]})
    assert len(expected_paths) == 2 and Counter(c["path"] for c in calls) == expected_paths
    rows = sample["before"]["data_files"] + sample["before"]["delete_files"]
    parts = {k: v for k, v in original.items() if k != "outside_sql"}
    parts.update(reference_fetch=0, reference_path_loop=0, reference_remainder=0)
    previous, reference_total, matched = 0, 0, []
    for call in calls:
        assert call["completed"] is True and call["found"] is False
        assert previous <= call["begin_ns"] <= call["end_ns"] <= sample["complete_source_gc_ns"]
        previous = call["end_ns"]
        assert call["call_ns"] == call["end_ns"] - call["begin_ns"]
        assert call["rows"] == rows and call["chunks"] > 0 and call["fetch_calls"] == call["chunks"] + 1
        assert 0 <= call["fetch_ns"] and 0 <= call["path_loop_ns"]
        overlapping = [q for q in top if q["begin_ns"] < call["end_ns"] and q["end_ns"] > call["begin_ns"]]
        assert len(overlapping) == 1, "reference call must overlap exactly its SQL query"
        query = overlapping[0]
        assert META.category(query["sql"]) == "known_source_paths"
        assert call["begin_ns"] <= query["begin_ns"] <= query["end_ns"] <= call["end_ns"]
        assert query["connection_id"] == call["connection_id"], "reference metadata connection differs"
        matched.append(query)
        remainder = call["call_ns"] - query["query_ns"] - call["fetch_ns"] - call["path_loop_ns"]
        assert remainder >= 0, "reference subscopes exceed the call"
        parts["reference_fetch"] += call["fetch_ns"]
        parts["reference_path_loop"] += call["path_loop_ns"]
        parts["reference_remainder"] += remainder
        reference_total += call["call_ns"]
    assert len({id(q) for q in matched}) == 4
    assert sum(q["query_ns"] for q in matched) == original["known_source_paths"]
    parts["outside_reference"] = original["outside_sql"] - (reference_total - original["known_source_paths"])
    assert min(parts.values()) >= 0 and sum(parts.values()) == sample["complete_source_gc_ns"]
    return parts


def summarize(samples):
    assert samples and all(s["kind"] == "diagnostic" and s["diagnostic"] is True for s in samples)
    measured = [partition(s) for s in samples]
    totals = [s["complete_source_gc_ns"] for s in samples]
    parts = {k: [p[k] for p in measured] for k in measured[0]}
    shares = META.covariance_shares(parts, totals)
    return {
        "samples": len(samples),
        "complete_collector_ms": META.distribution(totals),
        "reference_total_ms": META.distribution([sum(c["call_ns"] for c in s["reference_calls"]) for s in samples]),
        "partition": {
            k: {
                "time_ms": META.distribution(v),
                "fraction_of_mean_total": sum(v) / sum(totals),
                "covariance_share": shares[k],
            }
            for k, v in parts.items()
        },
        "rows_per_collector": sorted({sum(c["rows"] for c in s["reference_calls"]) for s in samples}),
        "chunks_per_collector": sorted({sum(c["chunks"] for c in s["reference_calls"]) for s in samples}),
        "fetch_calls_per_collector": sorted({sum(c["fetch_calls"] for c in s["reference_calls"]) for s in samples}),
        "metadata": META.summarize(samples),
    }


def run(args):
    root, inputs = args.output_dir.resolve(), args.inputs.resolve()
    root.mkdir(mode=0o700)
    report = {"status": "running", "profile": args.profile, "rule": RULE, "oracles": [], "cases": []}
    BASE.save(root / "report.json", report)
    started = time.monotonic()
    try:
        build_path = args.build_report.resolve()
        build = json.loads(build_path.read_text())
        assert build["mode"] == "private_reference_diagnostic" and build["production_sources_unchanged"] is True
        assert build["production_source_sha256"] == "97a4b1cf7d1a00c7846a974fee77647649c8b18c7ae6ae254097b9d474ffe96e"
        source = json.loads(inputs.read_text())
        assert source["status"] == "completed" and source["decision"] == "baseline_only"
        assert digest(args.producer_driver.resolve()) == source["driver_sha256"]
        driver = Path(build["driver"])
        assert driver.is_absolute() and digest(driver) == build["driver_sha256"]
        tracked = {
            **build["source_sha256"],
            **build["link_inputs_sha256"],
            str(inputs): digest(inputs),
            str(build_path): digest(build_path),
            str(driver): digest(driver),
            str(args.producer_driver.resolve()): source["driver_sha256"],
            str(Path(__file__).resolve()): digest(Path(__file__)),
        }
        assert all(digest(Path(p)) == h for p, h in tracked.items())
        selected = META.AA.select_cases("smoke" if args.profile == "smoke" else "focus", source["cases"])
        selected = [c for c in selected if c["task"] == "files"]
        cpus, original = args.cpus, sorted(os.sched_getaffinity(0))
        topology, threads = PIN.topology(cpus), PIN.thread_environment()
        PIN.validate_cpus(cpus, original, topology, args.profile == "formal")
        report.update(
            build_report=str(build_path),
            identities=tracked,
            original_input_report=str(inputs),
            original_input_report_sha256=digest(inputs),
            producer_driver_sha256=source["driver_sha256"],
            consumer_driver_sha256=build["driver_sha256"],
            original_affinity=original,
            requested_affinity=cpus,
            thread_environment=threads,
        )
        private = root / "private"
        private.mkdir(mode=0o700)
        env = dict(os.environ, TMPDIR=str(private), VANE_RUNNER="local-fast")
        env.pop("VORTEX_SPFRESH_NATIVE", None)
        with PIN.pinned_affinity(cpus):
            for case in selected:
                expected = inputs.parent / f"history-{case['history']}-{case['history_storage']}-files"
                assert Path(case["root"]) == expected and expected.resolve() == expected
                work, image = Path(case["work"]), Path(case["image"])
                assert work == expected / "work" and image == expected / "image"
                assert not work.is_symlink() and not image.is_symlink()
                assert BASE.fingerprint(BASE.manifest(image)) == case["input_image_sha256"]
                shutil.rmtree(work)
                shutil.copytree(image, work, copy_function=shutil.copy2)
                before = BASE.manifest(work)
                name = f"oracle-{case['history']}-{case['history_storage']}"
                with (root / (name + ".log")).open("w") as log:
                    result = subprocess.run(
                        [
                            str(driver),
                            str(work),
                            "reference-oracle",
                            str(case["history"]),
                            "files",
                            "1",
                            case["history_storage"],
                        ],
                        env=env,
                        stdout=log,
                        stderr=subprocess.STDOUT,
                        timeout=90,
                    )
                assert result.returncode == 0, str(root / (name + ".log"))
                oracle = json.loads((work / "reference-oracle.json").read_text())
                assert oracle["status"] == "passed" and oracle["cases"] == 4
                calls = oracle["reference_calls"]
                assert len(calls) == 4 and [c["found"] for c in calls] == [True, True, False, False]
                assert [c["completed"] for c in calls] == [True, True, True, False]
                after = BASE.manifest(work)
                assert all(after.get(p) == v for p, v in before.items() if p.startswith(("data/", "indexes/")))
                assert BASE.fingerprint(BASE.manifest(image)) == case["input_image_sha256"]
                BASE.save(root / (name + ".json"), oracle)
                report["oracles"].append(
                    {
                        "history": case["history"],
                        "history_storage": case["history_storage"],
                        "cases": 4,
                        "status": "passed",
                    }
                )
        # The shared sampler requires its declared consumer hash, while all images keep
        # their original producer and canonical paths. Never overwrite the sealed report.
        declaration = dict(
            source,
            driver_sha256=build["driver_sha256"],
            driver_role="instrumented_consumer_of_sealed_images",
            producer_driver_sha256=source["driver_sha256"],
            producer_report_sha256=digest(inputs),
            producer_report=str(inputs),
            diagnostic_build_report_sha256=digest(build_path),
        )
        declaration_path = inputs.parent / (root.name + "-consumer-inputs.json")
        with declaration_path.open("x") as stream:
            json.dump(declaration, stream, indent=2)
            stream.write("\n")
        report["consumer_declaration"] = str(declaration_path)
        report["consumer_declaration_sha256"] = digest(declaration_path)
        (root / "consumer-inputs.json").write_bytes(declaration_path.read_bytes())
        BASE.save(
            root / "measurement-contract.json",
            dict(
                RULE,
                cpus=cpus,
                profile=args.profile,
                producer_report_sha256=digest(inputs),
                consumer_declaration_sha256=digest(declaration_path),
            ),
        )
        BASE.save(root / "report.json", report)
        META.run(
            argparse.Namespace(
                output_dir=root / "sampling", inputs=declaration_path, driver=driver, cpus=cpus, profile=args.profile
            )
        )
        raw = root / "sampling/report.json"
        sampled = json.loads(raw.read_text())
        assert sampled["status"] == "completed" and sampled["decision"] == (
            "smoke_only" if args.profile == "smoke" else "diagnostics_only"
        )
        assert sampled["applied_affinity"] == sampled["child_probe"]["cpus"] == cpus
        assert sampled["restored_affinity"] == original and sampled["thread_environment"] == threads
        for case in sampled["cases"]:
            for sample in case["samples"]:
                partition(sample)
            samples = [s for s in case["samples"] if s["kind"] == "diagnostic"]
            META.validate_inventory(samples, RULE["profiles"][args.profile])
            report["cases"].append(
                {
                    "history": case["history"],
                    "history_storage": case["history_storage"],
                    "task": case["task"],
                    "summary": summarize(samples),
                    "label_summaries": {
                        label: summarize([s for s in samples if s["variant"] == label]) for label in META.AA.LABELS
                    },
                }
            )
        assert all(digest(Path(p)) == h for p, h in tracked.items())
        assert PIN.thread_environment() == threads and sorted(os.sched_getaffinity(0)) == original
        assert PIN.topology(cpus) == topology and digest(declaration_path) == report["consumer_declaration_sha256"]
        report.update(
            status="completed",
            decision="smoke_only" if args.profile == "smoke" else "reference_diagnostics_only",
            sampling_report=str(raw),
            sampling_report_sha256=digest(raw),
            restored_affinity=original,
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
    parser.add_argument("--producer-driver", type=Path, required=True)
    parser.add_argument("--build-report", type=Path, required=True)
    parser.add_argument("--cpus", type=PIN.parse_cpus, required=True)
    parser.add_argument("--profile", choices=tuple(RULE["profiles"]), required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    run(parser.parse_args())
