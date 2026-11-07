#!/usr/bin/env python3
"""Build the isolated R7 probe from exact upstream sources; no provider changes."""

import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import tarfile

SPFRESH = "5893eb61ee3b18610b6b00f1939be7dae1af8904"
VORTEX = "122b63e0408a187a9d0ecdd129f1cbe12b6b46b2"
HERE = Path(__file__).resolve().parent


def check(value, message):
    if not value:
        raise ValueError(message)


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def git(root, *args):
    return subprocess.check_output(["git", "-C", str(root), *args])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spfresh-source", type=Path, required=True)
    parser.add_argument("--vortex-source", type=Path, required=True)
    parser.add_argument("--dependency-prefix", type=Path, required=True)
    parser.add_argument("--build-dir", type=Path, required=True)
    parser.add_argument("--cpus", required=True)
    args = parser.parse_args()
    for path, revision in ((args.spfresh_source, SPFRESH), (args.vortex_source, VORTEX)):
        check(git(path, "rev-parse", "HEAD").decode().strip() == revision, "Unexpected upstream revision")
        check(not git(path, "status", "--porcelain"), "Upstream source must be clean")
    cpus = {int(v) for v in args.cpus.split(",")}
    check(cpus and cpus <= os.sched_getaffinity(0), "CPU set unavailable")
    os.sched_setaffinity(0, cpus)
    root = args.build_dir.resolve()
    root.mkdir(mode=0o700)
    source = root / "source"
    source.mkdir(mode=0o700)
    archive = git(args.spfresh_source, "archive", "--format=tar", SPFRESH)
    check(len(archive) <= 64 * 1024 * 1024, "Upstream source archive budget")
    with tarfile.open(fileobj=io.BytesIO(archive)) as stream:
        stream.extractall(source, filter="data")
    patches = (
        args.vortex_source / "vortex-index-spfresh/native/static-only.patch",
        HERE / "spfresh_update/native-update.patch",
    )
    for patch in patches:
        subprocess.run(["git", "apply", str(patch.resolve())], cwd=source, check=True)
    inputs = {
        str(p): sha(p)
        for p in (
            Path(__file__).resolve(),
            HERE / "spfresh_update/probe.cpp",
            HERE / "spfresh_update/CMakeLists.txt",
            *patches,
        )
    }
    subprocess.run(
        [
            "cmake",
            "-S",
            str(HERE / "spfresh_update"),
            "-B",
            str(root / "build"),
            "-G",
            "Ninja",
            "-DCMAKE_BUILD_TYPE=Release",
            "-DSPFRESH_SOURCE=" + str(source),
            "-DCMAKE_PREFIX_PATH=" + str(args.dependency_prefix.resolve()),
        ],
        check=True,
    )
    subprocess.run(["cmake", "--build", str(root / "build"), "--parallel", "2"], check=True)
    binary = root / "build/ducklake_spfresh_update_probe"
    cache = (root / "build/CMakeCache.txt").read_text().splitlines()
    libraries = {
        line.split(":", 1)[0]: line.split("=", 1)[1]
        for line in cache
        if line.startswith(("ROCKSDB_LIBRARY:FILEPATH=", "ZSTD_LIBRARY:FILEPATH="))
    }
    report = {
        "schema": 1,
        "status": "built",
        "spfresh_revision": SPFRESH,
        "vortex_revision": VORTEX,
        "input_sha256": inputs,
        "applied_affinity": sorted(os.sched_getaffinity(0)),
        "source_sha256": {str(p.relative_to(source)): sha(p) for p in sorted(source.rglob("*")) if p.is_file()},
        "binary": str(binary),
        "binary_sha256": sha(binary),
        "libraries": {key: {"path": value, "sha256": sha(Path(value).resolve())} for key, value in libraries.items()},
        "runtime_closure": subprocess.check_output(["ldd", str(binary)], text=True),
    }
    check("not found" not in report["runtime_closure"], "Incomplete runtime closure")
    check(inputs == {name: sha(Path(name)) for name in inputs}, "Build input changed")
    (root / "build.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"binary": str(binary), "sha256": report["binary_sha256"]}))


if __name__ == "__main__":
    main()
