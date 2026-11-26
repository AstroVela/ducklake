#!/usr/bin/env python3
"""Build isolated test executables for rejected expiry queries from a qualified Ninja build."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import shlex
import subprocess


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
    text = (repo / "src/storage/ducklake_metadata_manager.cpp").read_text()
    start = text.index(
        "// get a list of files that are no longer required after these deletions",
        text.index("void DuckLakeMetadataManager::DeleteSnapshots("),
    )
    match = re.search(r'StringUtil::Format\(R"\((.*?)\)"', text[start:], re.S)
    assert match is not None
    old = match.group(1)
    assert "NOT EXISTS" in old and "path_is_relative%s" in old and old.count("%s") == 2
    query = (repo / "test/vane/expiry_interval_window.sql").read_text().format(marker="%s", forced="%s")
    query = query.replace("ducklake_data_file", "{METADATA_CATALOG}.ducklake_data_file")
    query = query.replace("ducklake_snapshot", "{METADATA_CATALOG}.ducklake_snapshot")
    report = {"baseline_source_sha256": hashlib.sha256(text.encode()).hexdigest(), "executables": {}}
    for variant in ("baseline", "unbounded", "bounded"):
        trial_source = text
        if variant != "baseline":
            selected = query
            if variant == "unbounded":
                bound = (
                    "    WHERE snapshot_id < (SELECT MAX(end_snapshot) FROM {METADATA_CATALOG}.ducklake_data_file)\n"
                )
                assert selected.count(bound) == 1
                selected = selected.replace(bound, "")
            offset = start + match.start(1)
            trial_source = text[:offset] + selected + text[offset + len(old) :]
        generated = root / (variant + ".cpp")
        generated.write_text(trial_source)
        obj = root / (variant + ".o")
        compile_source("/ducklake_metadata_manager.cpp", generated, obj)
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
    (root / "build.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--build-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    run(parser.parse_args())
