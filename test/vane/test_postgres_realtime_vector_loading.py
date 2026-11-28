#!/usr/bin/env python3
"""Check PostgreSQL proof artifact selection with real CLI processes and no catalog."""

from __future__ import annotations

import argparse
import importlib.util
from pathlib import Path
import subprocess
import tempfile

SPEC = importlib.util.spec_from_file_location(
    "postgres_proof", Path(__file__).with_name("test_postgres_realtime_vector.py")
)
PROOF = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PROOF)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duckdb", required=True, type=Path, help="CLI with statically linked DuckLake")
    parser.add_argument("--postgres-extension", required=True, type=Path, help="matching PostgreSQL scanner artifact")
    args = parser.parse_args()
    duckdb = args.duckdb.resolve(strict=True)
    scanner = args.postgres_extension.resolve(strict=True)
    with tempfile.TemporaryDirectory(prefix="ducklake-postgres-loader-selection-") as directory:
        root = Path(directory)
        probe = PROOF.PostgresProbe(duckdb, None, scanner, root, "dbname=unused_loader_check")
        probe.prefix = probe.driver_prefix
        PROOF.require_equal(probe.query("SELECT 42 AS n"), [{"n": 42}], "selected scanner loads")

        invalid_lake = root / "ducklake.duckdb_extension"
        invalid_scanner = root / "postgres_scanner.duckdb_extension"
        invalid_lake.write_text("THIS IS NOT A VALID DUCKDB EXTENSION\n")
        invalid_scanner.write_text("THIS IS NOT A VALID DUCKDB EXTENSION\n")
        cases = [
            ("preloaded DuckLake", invalid_lake, scanner, ""),
            ("preloaded PostgreSQL scanner", None, invalid_scanner, f"LOAD {PROOF.sql_string(scanner)};"),
        ]
        for description, lake, postgres, preload in cases:
            probe = PROOF.PostgresProbe(duckdb, lake, postgres, root, "dbname=unused_loader_check")
            result = subprocess.run(
                probe.command + [preload + probe.driver_prefix + "SELECT 42 AS n"],
                capture_output=True,
                text=True,
                timeout=30,
            )
            if result.returncode <= 0:
                raise AssertionError(
                    f"{description} ignored an invalid selected artifact: "
                    f"exit={result.returncode}\n{result.stdout}\n{result.stderr}"
                )
    print("PASS: real scanner loads; preloaded extensions cannot hide invalid selected artifacts")


if __name__ == "__main__":
    main()
