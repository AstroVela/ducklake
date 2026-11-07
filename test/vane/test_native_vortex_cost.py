#!/usr/bin/env python3
"""Measure native query cost with explicit logical-projection regression gates."""
from __future__ import annotations

import argparse
import importlib.util
import json
import math
import os
import platform
from pathlib import Path
import statistics
import subprocess
import time

SPEC = importlib.util.spec_from_file_location(
    "sql_search_proof", Path(__file__).with_name("test_native_vortex_search.py")
)
assert SPEC is not None and SPEC.loader is not None
SEARCH = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SEARCH)
CAT = SEARCH.CAT
BASE = CAT.BASE
require = BASE.require
VECTOR = [133, 4, 2, 0, 1, 8, 7, 16]


def percentiles(values):
    ordered = sorted(values)
    return {"median": statistics.median(values), "p95": ordered[math.ceil(0.95 * len(ordered)) - 1]}


def check_hits(sample, oracle, category):
    eligible = [row for row in oracle if category == "ALL" or row["category"] == category]
    distance = lambda row: sum((a - b) ** 2 for a, b in zip(row["embedding"], VECTOR))
    expected = {row["id"] for row in sorted(eligible, key=distance)[:10]}
    by_id = {row["id"]: row for row in eligible}
    hits = sample["hits"]
    require(len({row["id"] for row in hits}) == len(hits), "cost query returned duplicate rows")
    for hit in hits:
        row = by_id.get(hit["id"])
        require(
            row is not None
            and hit["category"] == row["category"]
            and hit["payload_bytes"] == len(row["payload"].encode()),
            "cost query returned wrong business projection",
        )
        require(
            math.isclose(hit["distance"], distance(row), rel_tol=1e-5, abs_tol=1e-4), "cost query distance mismatch"
        )
    require([hit["distance"] for hit in hits] == sorted(hit["distance"] for hit in hits), "cost query lost rank order")
    recall = len({row["id"] for row in hits} & expected) / len(expected)
    require(recall >= 0.8, f"cost fixture recall below threshold: {recall}")
    return recall


def summarize(samples):
    events = (
        "after_inventory",
        "before_rows",
        "before_publish",
        "before_open",
        "source_pinned",
        "after_open",
        "before_ann",
        "after_ann",
        "before_fetch",
        "after_fetch",
    )
    phases = {}
    for before, after, label in (
        ("after_inventory", "before_rows", "source_copy"),
        ("before_rows", "before_publish", "visibility_and_business"),
        ("before_publish", "before_open", "finish_capture"),
        ("before_open", "source_pinned", "decode_stage_and_source_open"),
        ("source_pinned", "after_open", "index_open"),
        ("after_open", "before_ann", "filter"),
        ("before_ann", "after_ann", "ann"),
        ("after_ann", "before_fetch", "rerank_scan_merge"),
        ("before_fetch", "after_fetch", "ranked_take"),
    ):
        phases[label] = percentiles([s["event_us"][after] - s["event_us"][before] for s in samples[1:]])
    for sample in samples:
        require(all(event in sample["event_us"] for event in events), "benchmark is missing a phase checkpoint")
    return {
        "first_query_us": samples[0]["elapsed_us"],
        "warm_us": percentiles([s["elapsed_us"] for s in samples[1:]]),
        "max_peak_rss_kib": max(s["peak_rss_kib"] for s in samples),
        "warm_current_rss_kib": percentiles([s["rss_after_kib"] for s in samples[1:]]),
        "warm_rchar": percentiles([s["rchar"] for s in samples[1:]]),
        "warm_read_bytes": percentiles([s["read_bytes"] for s in samples[1:]]),
        "warm_write_bytes": percentiles([s["write_bytes"] for s in samples[1:]]),
        "warm_wchar": percentiles([s["wchar"] for s in samples[1:]]),
        "warm_phases_us": phases,
    }


def fixture(proof, driver, count):
    native = CAT.Native(driver, proof.root / "native")
    try:
        native.sql(
            "DROP TABLE lake.items;CREATE TABLE lake.items(id BIGINT,embedding FLOAT[],category VARCHAR,payload VARCHAR);"
            "CALL ducklake_set_option('lake','experimental_data_file_format','vortex',table_name=>'items');"
        )
        for parity in range(2):
            native.sql(
                "INSERT INTO lake.items SELECT id,[id::FLOAT,((id*4)%12)::FLOAT,((id*5)%13)::FLOAT,"
                "((id*6)%14)::FLOAT,((id*7)%15)::FLOAT,((id*8)%16)::FLOAT,((id*9)%17)::FLOAT,((id*10)%18)::FLOAT],"
                "CASE WHEN id%4=0 THEN 'B' ELSE 'A' END,sha256(id::VARCHAR)||repeat('宽',320) "
                f"FROM(SELECT i*2+{parity} AS id FROM range({count//2})t(i));"
            )
        native.call("raw", "base")
        reference = proof.root / "artifacts" / "reference.json"
        reference.parent.mkdir()
        proof.run([str(proof.probe), "build-admitted-file-index", str(native.capture("base")), str(reference)])
        native.call("init")
        native.call("publish", "knn", reference, 0)
        native.sql(
            "CALL ducklake_set_option('lake','data_inlining_row_limit',100,table_name=>'items');"
            "INSERT INTO lake.items SELECT 100000+i,CASE WHEN i=0 THEN [133.25,4,2,0,1,8,7,16] "
            "ELSE [160000+i,0,0,0,0,0,0,0] END,'B','F-delta' FROM range(128)t(i);"
            "BEGIN;UPDATE lake.items SET embedding=[700000,0,0,0,0,0,0,0],payload=NULL WHERE id=133;"
            "DELETE FROM lake.items WHERE id=134;"
            "INSERT INTO lake.items VALUES(200000,[133.125,4,2,0,1,8,7,16],'A','T-live');COMMIT;"
        )
        native.call("raw", "delta")
        oracle = native.rows("delta")
        capture = json.loads(native.mixed("delta").read_text())
        return native.root, oracle, capture
    finally:
        native.finish()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("duckdb", "driver", "cost-driver", "probe", "output-dir"):
        parser.add_argument("--" + name, required=True, type=Path)
    parser.add_argument("--baseline-driver", type=Path)
    parser.add_argument("--baseline-revision")
    parser.add_argument("--rows", type=int, nargs="+", default=[4096, 32768])
    parser.add_argument("--repeats", type=int, default=9)
    args = parser.parse_args()
    require(
        bool(args.baseline_driver) == bool(args.baseline_revision),
        "baseline driver and revision must be explicit together",
    )
    require(
        all(4096 <= n <= 32768 and n % 2 == 0 for n in args.rows),
        "fixture rows must be even and between 4096 and 32768",
    )
    root = args.output_dir.resolve()
    root.mkdir(parents=True, exist_ok=False)
    private = root / "private"
    private.mkdir()
    os.environ["TMPDIR"] = str(private)
    report = {
        "status": "running",
        "cases": [],
        "vortex_revision": "145f816102d6024a12164e37737ff8864faf5033",
        "cost_driver_sha256": BASE.digest(args.cost_driver.resolve(strict=True)),
        "baseline_driver_sha256": (
            BASE.digest(args.baseline_driver.resolve(strict=True)) if args.baseline_driver else None
        ),
        "baseline_revision": args.baseline_revision,
        "machine": {"architecture": platform.machine(), "kernel": platform.release()},
        "conditions": "isolated process per case; first query does not imply a cold OS cache; later queries reopen sources and index; no cache dropping; times are informational",
    }
    started = time.monotonic()
    try:
        for count in args.rows:
            directory = root / f"rows-{count}"
            directory.mkdir()
            proof = BASE.Proof(args.duckdb.resolve(strict=True), args.probe.resolve(strict=True), directory)
            native_root, oracle, capture = fixture(proof, args.driver.resolve(strict=True), count)
            file_rows = len(capture["file_capture"]["visible"])
            inline_rows = len(capture["inline_rows"])
            for category in ("ALL", "B"):
                variants = [("optimized", args.cost_driver)]
                if args.baseline_driver:
                    variants.insert(0, ("baseline", args.baseline_driver))
                    if category == "B":
                        variants.reverse()
                case = {
                    "base_rows": count,
                    "category": category,
                    "visible_file_rows": file_rows,
                    "inline_rows": inline_rows,
                    "variants": {},
                    "variant_order": [label for label, _ in variants],
                }
                original = None
                for label, driver in variants:
                    path = directory / f"{category}-{label}.json"
                    with path.with_suffix('.log').open('w') as log:
                        result = subprocess.run(
                            [
                                str(driver.resolve(strict=True)),
                                str(native_root),
                                str(path),
                                str(args.repeats),
                                category,
                            ],
                            stdout=log,
                            stderr=subprocess.STDOUT,
                            text=True,
                            timeout=600,
                        )
                    require(result.returncode == 0, f"{label} cost driver failed; inspect {path.with_suffix('.log')}")
                    samples = json.loads(path.read_text())["samples"]
                    require(len(samples) == args.repeats, "benchmark returned incomplete repetitions")
                    recalls = [check_hits(s, oracle, category) for s in samples]
                    for sample in samples:
                        require(
                            sample["hits"] == samples[0]["hits"], "repeated native query changed its immutable result"
                        )
                        if label == "optimized":
                            d = sample["diagnostics"]
                            require(
                                d["native_capture"]["oracle_bytes"] == 0
                                and d["native_capture"]["business_rows"] == inline_rows,
                                "SQL capture materialized physical business rows or an oracle",
                            )
                            projection = d["cost"]["projection_rows"]
                            require(
                                projection["category_rows"] == (file_rows if category != "ALL" else 0),
                                "category projection read wrong row set",
                            )
                            require(
                                projection["business_rows"] <= len(sample["hits"]) <= 10
                                and projection["vector_rows"] <= 10,
                                "ANN/category path fetched full business rows before the final bounded merge",
                            )
                    if original is not None:
                        require(samples[0]["hits"] == original, "optimized query changed baseline ranked results")
                    original = samples[0]["hits"]
                    require(list(private.iterdir()) == [], "benchmark query leaked source/index staging")
                    case["variants"][label] = {
                        **summarize(samples),
                        "recall_min": min(recalls),
                        "samples_file": str(path.relative_to(root)),
                    }
                if args.baseline_driver:
                    old = case["variants"]["baseline"]
                    new = case["variants"]["optimized"]
                    case["warm_median_speedup"] = old["warm_us"]["median"] / new["warm_us"]["median"]
                report["cases"].append(case)
        report["status"] = "passed"
    except Exception as error:
        report.update(status="failed", error=str(error))
        raise
    finally:
        report["seconds"] = time.monotonic() - started
        (root / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
