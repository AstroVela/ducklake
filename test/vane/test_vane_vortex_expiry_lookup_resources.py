#!/usr/bin/env python3
"""Account for both queries and the retained-ID vector on copied #76 metadata."""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import subprocess

SPEC = importlib.util.spec_from_file_location(
    "source_gc_cost", Path(__file__).with_name("test_vane_vortex_source_gc_benchmark.py")
)
BASE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BASE)


def run(args):
    root = args.output_dir.resolve()
    root.mkdir(mode=0o700)
    source = json.loads(args.inputs.read_text())
    assert source["status"] == "completed" and source["decision"] == "baseline_only"
    report = {
        "status": "running",
        "scope": "native metadata queries after removing the same two oldest snapshots; no worker or ANN timing",
        "driver_sha256": hashlib.sha256(args.driver.read_bytes()).hexdigest(),
        "rss_scope": "Current RSS from /proc/self/statm at query boundaries; ru_maxrss may inherit startup high-water. Neither is an admission cap or a measured within-query maximum.",
        "inputs_sha256": hashlib.sha256(args.inputs.read_bytes()).hexdigest(),
        "cases": [],
    }
    try:
        for case in source["cases"]:
            if case["task"] != "expire":
                continue
            image = Path(case["image"])
            assert BASE.fingerprint(BASE.manifest(image)) == case["input_image_sha256"]
            output = root / f"history-{case['history']}-{case['history_storage']}"
            output.mkdir(mode=0o700)
            samples = []
            for repeat in range(3):
                for variant in ("baseline", "lookup"):
                    catalog = output / f"{repeat}-{variant}.ducklake"
                    shutil.copy2(image / "metadata.ducklake", catalog)
                    result = output / f"{repeat}-{variant}.json"
                    subprocess.run(
                        [str(args.driver.resolve()), str(catalog), variant, str(result)], check=True, timeout=90
                    )
                    sample = json.loads(result.read_text())
                    sample["repeat"] = repeat
                    samples.append(sample)
                    catalog.unlink()
            assert len({s["eligible_files"] for s in samples}) == 1
            assert all(s["snapshot_vector_capacity_bytes"] >= s["snapshot_vector_bytes"] for s in samples)
            assert BASE.fingerprint(BASE.manifest(image)) == case["input_image_sha256"]
            report["cases"].append(
                {
                    "history": case["history"],
                    "history_storage": case["history_storage"],
                    "image_sha256": case["input_image_sha256"],
                    "samples": samples,
                }
            )
        expected = {(c["history"], c["history_storage"]) for c in source["cases"] if c["task"] == "expire"}
        assert expected and {(c["history"], c["history_storage"]) for c in report["cases"]} == expected
        report["full_corpus"] = expected == {(n, storage) for n in (8, 512, 4096) for storage in ("inline", "file")}
        assert hashlib.sha256(args.driver.read_bytes()).hexdigest() == report["driver_sha256"]
        report["status"] = "passed"
    finally:
        (root / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(
        json.dumps(
            {
                "status": report["status"],
                "cases": len(report["cases"]),
                "samples": sum(len(c["samples"]) for c in report["cases"]),
            }
        ),
        flush=True,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--driver", type=Path, required=True)
    parser.add_argument("--inputs", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    run(parser.parse_args())
