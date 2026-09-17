#!/usr/bin/env python3
"""Package already-signed SQLite and DuckLake native data without keys or native builds."""

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
    expected_artifacts = {f"{name}.duckdb_extension" for name in builder.EXTENSION_NAMES}
    for directory in (args.signed, args.prepared / "artifacts"):
        if directory.is_symlink() or {path.name for path in directory.iterdir()} != expected_artifacts:
            raise ValueError("packaging requires the complete fixed SQLite and DuckLake artifact set")
    artifacts, licenses = {}, {}
    for name in builder.EXTENSION_NAMES:
        artifact = regular_file(args.signed / f"{name}.duckdb_extension", 384 * 1024 * 1024)
        unsigned = regular_file(args.prepared / f"artifacts/{name}.duckdb_extension", 384 * 1024 * 1024)
        if artifact.read_bytes()[:-256] != unsigned.read_bytes()[:-256]:
            raise ValueError(f"the signed {name} payload differs from the original preparation artifact")
        artifacts[name] = artifact
        license_directory = args.prepared / "licenses" / name
        if license_directory.is_symlink() or {path.name for path in license_directory.iterdir()} != set(
            builder.PROVIDER_LICENSE_NAMES[name]
        ):
            raise ValueError(f"prepared license data differs from the reviewed {name} contract")
        licenses[name] = tuple(
            regular_file(license_directory / filename, 16 * 1024 * 1024)
            for filename in builder.PROVIDER_LICENSE_NAMES[name]
        )
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
                sqlite_wheel = builder._build_provider_wheel(
                    python=python,
                    vane_source=args.vane_source.resolve(),
                    artifact=artifacts["sqlite_scanner"].resolve(),
                    extension_name="sqlite_scanner",
                    output_directory=provider,
                    platform_tag=builder.PROVIDER_PLATFORM_TAG,
                    trust_identity=identity,
                    license_files=[path.resolve() for path in licenses["sqlite_scanner"]],
                )
                wheel = builder._build_provider_wheel(
                    python=python,
                    vane_source=args.vane_source.resolve(),
                    artifact=artifacts["ducklake"].resolve(),
                    extension_name="ducklake",
                    output_directory=provider,
                    platform_tag=builder.PROVIDER_PLATFORM_TAG,
                    trust_identity=identity,
                    license_files=[path.resolve() for path in licenses["ducklake"]],
                    dependency_wheel=sqlite_wheel,
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
                        "--dependency-wheel",
                        str(sqlite_wheel),
                        "--dependency-trust-identity",
                        identity,
                        "--extension-name",
                        "ducklake",
                        "--trust-identity",
                        identity,
                    )
                )
                for artifact_wheel in (sqlite_wheel, wheel):
                    destination = args.output / artifact_wheel.name
                    if destination.exists():
                        raise ValueError("runtime targets produced a duplicate provider wheel")
                    shutil.copyfile(artifact_wheel, destination)
            finally:
                environment.cleanup()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
