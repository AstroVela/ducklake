#!/usr/bin/env python3
"""Build the withdrawn lookup in private translation units; provider sources stay unchanged."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import shlex
import subprocess

BASE_SOURCE_SHA256 = "9e0f21ed12467bfa4c194dab7518f7f5994198379fe35f7483fdc5382b286c2b"


def run(args):
    root = args.output_dir.resolve()
    root.mkdir(mode=0o700)
    build = args.build_dir.resolve()
    repo = Path(__file__).resolve().parents[2]
    commands = json.loads((build / "compile_commands.json").read_text())

    def compile_source(suffix, source, output):
        entries = [e for e in commands if e["file"].endswith(suffix)]
        assert len(entries) == 1
        entry = entries[0]
        command = shlex.split(entry["command"])
        command[command.index(entry["file"])] = str(source)
        command[command.index("-o") + 1] = str(output)
        command += ["-I" + str(repo / "test/vane")]
        subprocess.run(command, cwd=entry["directory"], check=True)

    benchmark = root / "benchmark.o"
    compile_source(
        "/native_vortex_write_driver.cpp", repo / "test/vane/native_vortex_source_gc_benchmark.cpp", benchmark
    )
    link_text = subprocess.check_output(
        ["ninja", "-C", str(build), "-t", "commands", "ducklake_vortex_write_driver"], text=True
    )
    links = [line for line in link_text.splitlines() if " -o ducklake_vortex_write_driver " in line]
    assert len(links) == 1
    command = shlex.split(links[0])
    assert command[:2] == [":", "&&"] and command[-2:] == ["&&", ":"]
    command = command[2:-2]
    positions = [i for i, value in enumerate(command) if value.endswith("/native_vortex_write_driver.cpp.o")]
    assert len(positions) == 1
    position = positions[0]
    command[position] = str(benchmark)
    source = repo / "src/storage/ducklake_metadata_manager.cpp"
    assert hashlib.sha256(source.read_bytes()).hexdigest() == BASE_SOURCE_SHA256
    patch = repo / "test/vane/expiry_snapshot_lookup.patch"
    generated = root / "src/storage/ducklake_metadata_manager.cpp"
    generated.parent.mkdir(parents=True)
    generated.write_bytes(source.read_bytes())
    subprocess.run(["patch", "--batch", "--forward", "--fuzz=0", "-p1", "--input", str(patch)], cwd=root, check=True)
    metadata = root / "lookup.o"
    compile_source("/ducklake_metadata_manager.cpp", generated, metadata)
    baseline_metadata = root / "baseline.o"
    compile_source("/ducklake_metadata_manager.cpp", source, baseline_metadata)
    report = {
        "baseline_source_sha256": BASE_SOURCE_SHA256,
        "candidate_source_sha256": hashlib.sha256(generated.read_bytes()).hexdigest(),
        "patch_sha256": hashlib.sha256(patch.read_bytes()).hexdigest(),
        "helper_sha256": hashlib.sha256(
            (repo / "test/vane/common/ducklake_snapshot_lookup.hpp").read_bytes()
        ).hexdigest(),
        "executables": {},
    }
    for variant, obj in (("baseline", baseline_metadata), ("lookup", metadata)):
        link = [
            f"-Wl,--dependency-file={root / (variant + '.d')}" if arg.startswith("-Wl,--dependency-file=") else arg
            for arg in command
        ]
        link.insert(position + 1, str(obj))
        output = root / variant
        link[link.index("-o") + 1] = str(output)
        subprocess.run(link, cwd=build, check=True)
        report["executables"][variant] = {
            "path": str(output),
            "sha256": hashlib.sha256(output.read_bytes()).hexdigest(),
        }
    assert hashlib.sha256(source.read_bytes()).hexdigest() == BASE_SOURCE_SHA256
    (root / "build.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--build-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    run(parser.parse_args())
