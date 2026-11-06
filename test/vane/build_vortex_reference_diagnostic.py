#!/usr/bin/env python3
"""Instrument one private metadata object; leave provider sources and archives intact."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import shlex
import subprocess

BASE_SOURCE_SHA256 = "97a4b1cf7d1a00c7846a974fee77647649c8b18c7ae6ae254097b9d474ffe96e"
EXTENSIONS = {"core_functions", "file", "httpfs", "icu", "json", "parquet", "ducklake", "vortex"}


def validate_source(entry, original):
    assert Path(entry["file"]).resolve() == original.resolve(), "configure explicit local provider source directories"


def validate_link(command):
    extensions = {
        Path(p).name.removeprefix("lib").removesuffix("_extension.a") for p in command if p.endswith("_extension.a")
    }
    assert extensions == EXTENSIONS, "unexpected static extension closure; use DISABLE_EXTENSIONS_FOR_TEST=1"
    return sorted(extensions)


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run(args):
    root, build = args.output_dir.resolve(), args.build_dir.resolve()
    root.mkdir(mode=0o700)
    repo = Path(__file__).resolve().parents[2]
    source = repo / "src/storage/ducklake_metadata_manager.cpp"
    assert digest(source) == BASE_SOURCE_SHA256, "unreviewed metadata source"
    tracked = [
        source,
        Path(__file__).resolve(),
        repo / "test/vane/native_vortex_source_gc_benchmark.cpp",
        repo / "test/vane/native_vortex_reference_trace.hpp",
        repo / "test/vane/reference_diagnostic.patch",
    ]
    source_hashes = {str(p): digest(p) for p in tracked}
    generated = root / "src/storage/ducklake_metadata_manager.cpp"
    generated.parent.mkdir(parents=True)
    generated.write_bytes(source.read_bytes())
    subprocess.run(
        ["patch", "--batch", "--forward", "--fuzz=0", "-p1", "--input", str(tracked[-1])], cwd=root, check=True
    )
    commands = json.loads((build / "compile_commands.json").read_text())
    compiled = []

    def compile_source(original, path, output, extra):
        entries = [e for e in commands if Path(e["file"]).name == original.name]
        assert len(entries) == 1, original
        entry = entries[0]
        validate_source(entry, original)
        command = shlex.split(entry["command"])
        assert command.count(entry["file"]) == 1 and command.count("-o") == 1
        command[command.index(entry["file"])] = str(path)
        command[command.index("-o") + 1] = str(output)
        command += ["-I" + str(repo / "test/vane"), *extra]
        subprocess.run(command, cwd=entry["directory"], check=True)
        compiled.append({"command": command, "directory": entry["directory"], "output_sha256": digest(output)})

    benchmark, metadata = root / "benchmark.o", root / "metadata.o"
    compile_source(tracked[2], tracked[2], benchmark, ["-DDUCKLAKE_REFERENCE_DIAGNOSTIC"])
    compile_source(source, generated, metadata, [])
    text = subprocess.check_output(
        ["ninja", "-C", str(build), "-t", "commands", "ducklake_vortex_source_gc_benchmark"], text=True
    )
    links = [line for line in text.splitlines() if " -o ducklake_vortex_source_gc_benchmark " in line]
    assert len(links) == 1
    command = shlex.split(links[0])
    assert command[:2] == [":", "&&"] and command[-2:] == ["&&", ":"]
    command = command[2:-2]
    extensions = validate_link(command)
    positions = [i for i, value in enumerate(command) if value.endswith("/native_vortex_source_gc_benchmark.cpp.o")]
    assert len(positions) == 1
    position = positions[0]
    command[position : position + 1] = [str(benchmark), str(metadata)]
    output = root / "reference-diagnostic"
    command[command.index("-o") + 1] = str(output)
    inputs = {
        str((build / value).resolve()): digest((build / value).resolve())
        for value in command
        if value.endswith((".a", ".so"))
    }
    assert inputs
    subprocess.run(command, cwd=build, check=True)
    assert all(digest(Path(p)) == value for p, value in inputs.items())
    assert all(digest(Path(p)) == value for p, value in source_hashes.items())
    report = {
        "mode": "private_reference_diagnostic",
        "static_extensions": extensions,
        "production_source_sha256": BASE_SOURCE_SHA256,
        "source_sha256": source_hashes,
        "patched_source_sha256": digest(generated),
        "compile": compiled,
        "link": command,
        "link_inputs_sha256": inputs,
        "driver": str(output),
        "driver_sha256": digest(output),
        "production_sources_unchanged": True,
    }
    (root / "build.json").write_text(json.dumps(report, indent=2) + "\n")
    print(
        json.dumps({k: report[k] for k in ("mode", "driver", "driver_sha256", "production_sources_unchanged")}),
        flush=True,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--build-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    run(parser.parse_args())
