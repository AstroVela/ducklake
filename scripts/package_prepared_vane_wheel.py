#!/usr/bin/env python3
"""Package already-signed DuckLake native data without keys or native builds."""

from __future__ import annotations

import argparse
import importlib.util
import os
import shutil
import stat
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def load_builder():
    spec = importlib.util.spec_from_file_location(
        "ducklake_native_packaging", ROOT / "scripts/build_vane_dynamic_wheel.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def regular_file(path: Path, maximum: int) -> Path:
    metadata = path.lstat()
    if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1 or not 0 < metadata.st_size <= maximum:
        raise ValueError(f"packaging requires bounded regular data files: {path.name}")
    return path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--vane-source", required=True, type=Path)
    parser.add_argument("--vane-revision", required=True)
    parser.add_argument("--profile", required=True, choices=("testpypi", "production"))
    parser.add_argument("--prepared", required=True, type=Path)
    parser.add_argument("--signed", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--runtime-python", action="append", required=True, type=Path)
    parser.add_argument("--runtime-wheel", action="append", required=True, type=Path)
    args = parser.parse_args()
    builder = load_builder()
    if len(args.runtime_python) != len(args.runtime_wheel):
        raise ValueError("each indexed runtime wheel requires exactly one interpreter")
    runtimes = tuple(zip(args.runtime_python, args.runtime_wheel, strict=True))
    if args.profile == "production":
        builder._require_production_runtimes(runtimes)
    builder._require_git_revision(args.vane_source, args.vane_revision, "Vane")
    artifact = regular_file(args.signed / "ducklake.duckdb_extension", 384 * 1024 * 1024)
    unsigned = regular_file(args.prepared / "artifacts/ducklake.duckdb_extension", 384 * 1024 * 1024)
    if artifact.read_bytes()[:-256] != unsigned.read_bytes()[:-256]:
        raise ValueError("the signed native payload differs from the original preparation artifact")
    license_directory = args.prepared / "licenses/ducklake"
    if {path.name for path in license_directory.iterdir()} != set(builder.LICENSE_NAMES):
        raise ValueError("prepared license data differs from the reviewed DuckLake contract")
    licenses = tuple(regular_file(license_directory / name, 16 * 1024 * 1024) for name in builder.LICENSE_NAMES)
    args.output.mkdir(parents=True, exist_ok=True)
    if any(args.output.iterdir()):
        raise ValueError("provider wheel output must be empty")
    identity = builder.SIGNING_PROFILES[args.profile][0]
    with tempfile.TemporaryDirectory(prefix="ducklake-provider-package-") as value:
        staging = Path(value)
        for index, (interpreter, runtime_wheel) in enumerate(runtimes):
            if not os.access(interpreter, os.X_OK):
                raise ValueError("runtime interpreter is not executable")
            environment, python = builder._builder_python(interpreter.resolve(), runtime_wheel.resolve(), staging)
            try:
                provider = staging / f"provider-{index}"
                provider.mkdir()
                wheel = builder._build_provider_wheel(
                    python=python,
                    vane_source=args.vane_source.resolve(),
                    artifact=artifact.resolve(),
                    extension_name="ducklake",
                    output_directory=provider,
                    platform_tag=builder.PROVIDER_PLATFORM_TAG,
                    trust_identity=identity,
                    license_files=[path.resolve() for path in licenses],
                )
                builder._run(
                    (
                        str(python),
                        "-I",
                        str(args.vane_source.resolve() / "scripts/verify_extension_wheel.py"),
                        "--base-wheel",
                        str(runtime_wheel.resolve()),
                        "--extension-wheel",
                        str(wheel),
                        "--extension-name",
                        "ducklake",
                        "--trust-identity",
                        identity,
                    )
                )
                destination = args.output / wheel.name
                if destination.exists():
                    raise ValueError("runtime targets produced a duplicate provider wheel")
                shutil.copyfile(wheel, destination)
            finally:
                environment.cleanup()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
