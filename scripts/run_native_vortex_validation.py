#!/usr/bin/env python3
"""Qualify the experimental native Vortex path and its disabled build."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import time


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duckdb", required=True, type=Path)
    parser.add_argument("--unittest", required=True, type=Path)
    parser.add_argument("--mode", required=True, choices=("ON", "OFF"))
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    duckdb = args.duckdb.resolve(strict=True)
    unittest = args.unittest.resolve(strict=True)
    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256()
    with duckdb.open("rb") as binary:
        for block in iter(lambda: binary.read(1024 * 1024), b""):
            digest.update(block)
    report = {"mode": args.mode, "duckdb_sha256": digest.hexdigest(), "checks": []}

    def run(
        name: str,
        command: list[str],
        sql_tests: bool = False,
        expected_error: str | None = None,
    ) -> None:
        started = time.monotonic()
        result = subprocess.run(
            command, cwd=root, capture_output=True, text=True, timeout=1200
        )
        text = result.stdout + result.stderr
        (output / f"{name}.log").write_text(text)
        check = {
            "name": name,
            "returncode": result.returncode,
            "seconds": time.monotonic() - started,
        }
        report["checks"].append(check)
        if expected_error is not None:
            if result.returncode == 0 or expected_error not in text:
                raise AssertionError(
                    f"{name}: expected explicit rejection, see {output / (name + '.log')}"
                )
        elif result.returncode:
            raise AssertionError(
                f"{name}: exit {result.returncode}, see {output / (name + '.log')}"
            )
        if sql_tests:
            counts = re.search(r"(\d+) assertions in (\d+) test cases?", text)
            if "All tests passed" not in text or counts is None or int(counts[2]) == 0:
                raise AssertionError(
                    f"{name}: native SQL tests did not complete, see {output / (name + '.log')}"
                )
            check.update(assertions=int(counts[1]), test_cases=int(counts[2]))
            skipped = re.search(r"(\d+) skipped tests?", text)
            check["skipped_tests"] = int(skipped[1]) if skipped else 0
        print(f"PASS: {name}", flush=True)

    try:
        if args.mode == "OFF":
            with tempfile.TemporaryDirectory(
                prefix="ducklake-vortex-disabled-"
            ) as directory:
                sql = (
                    "SET autoinstall_known_extensions=false; SET autoload_known_extensions=false; LOAD ducklake;"
                    f"ATTACH 'ducklake:{directory}/metadata.ducklake' AS lake (DATA_PATH '{directory}/data');"
                    "CREATE TABLE lake.items(id BIGINT, embedding FLOAT[]);"
                    "CALL ducklake_set_option('lake', 'experimental_data_file_format', 'vortex', table_name => 'items');"
                )
                run(
                    "disabled-rejection",
                    [str(duckdb), "-batch", "-bail", "-c", sql],
                    expected_error="native DUCKLAKE_ENABLE_VORTEX build",
                )
        else:
            run(
                "vortex-sql",
                [
                    str(unittest),
                    "*test/sql/vortex/*",
                    "--require",
                    "ducklake",
                    "--require",
                    "parquet",
                    "--require",
                    "vortex",
                ],
                sql_tests=True,
            )
            run(
                "vortex-recovery",
                [
                    sys.executable,
                    "-I",
                    str(root / "test/vane/test_native_realtime_vector_recovery.py"),
                    "--duckdb",
                    str(duckdb),
                    "--file-format",
                    "vortex",
                ],
            )
        selection = ",".join(
            "*test/sql/" + path
            for path in (
                "vane/*",
                "transaction/*",
                "types/*",
                "default/*",
                "update/*",
                "merge/*",
                "constraints/*",
                "metadata/appender_data_files.test",
                "deletion_inlining/inlined_delete_metadata_snapshot.test",
                "virtualcolumns/*",
                "rowid/*",
                "time_travel/*",
                "compaction/*",
            )
        )
        run(
            "native-regressions",
            [str(unittest), selection, "--require", "ducklake", "--require", "parquet"],
            sql_tests=True,
        )
        run(
            "parquet-recovery",
            [
                sys.executable,
                "-I",
                str(root / "test/vane/test_native_realtime_vector_recovery.py"),
                "--duckdb",
                str(duckdb),
            ],
        )
        report["status"] = "passed"
    except Exception as error:
        report.update(status="failed", error=str(error))
        raise
    finally:
        (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
