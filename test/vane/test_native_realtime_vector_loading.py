#!/usr/bin/env python3
"""Reject ignored artifact selections using a real statically linked CLI."""
from __future__ import annotations

import argparse
import importlib.util
from pathlib import Path
import subprocess
import tempfile

SPEC = importlib.util.spec_from_file_location(
    "recovery", Path(__file__).with_name("test_native_realtime_vector_recovery.py")
)
RECOVERY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RECOVERY)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duckdb", required=True, type=Path, help="CLI with statically linked DuckLake")
    args = parser.parse_args()
    duckdb = args.duckdb.resolve(strict=True)
    with tempfile.TemporaryDirectory(prefix="ducklake-native-loader-selection-") as directory:
        root = Path(directory)
        static = root / "static"
        static.mkdir()
        probe = RECOVERY.RecoveryProbe(duckdb, None, static)
        RECOVERY.require_equal(probe.query("SELECT 42 AS n"), [{"n": 42}], "static selection")
        artifact = root / "ducklake.duckdb_extension"
        artifact.write_text("THIS IS NOT A VALID DUCKDB EXTENSION\n")
        rejected = root / "rejected"
        rejected.mkdir()
        probe = RECOVERY.RecoveryProbe(duckdb, artifact, rejected)
        result = subprocess.run(
            probe.command + [probe.prefix + "SELECT 42 AS n"], capture_output=True, text=True, timeout=30
        )
        if result.returncode <= 0:
            raise AssertionError(
                f"Expected an explicit selection error, got {result.returncode}:\n{result.stdout}\n{result.stderr}"
            )
        RECOVERY.require_equal(probe.metadata.exists(), False, "artifact rejection before catalog attach")
    print("PASS: static selection succeeds; ignored invalid artifact is rejected before catalog attach")


if __name__ == "__main__":
    main()
