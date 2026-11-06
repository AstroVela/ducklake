#!/usr/bin/env python3
"""Build matched explicit metadata objects for the filename-prefilter trial."""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import shlex
import subprocess

SPEC = importlib.util.spec_from_file_location(
    "reference_build", Path(__file__).with_name("build_vortex_reference_diagnostic.py")
)
BUILD = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BUILD)


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run(args):
    root, build = args.output_dir.resolve(), args.build_dir.resolve()
    root.mkdir(mode=0o700)
    repo = Path(__file__).resolve().parents[2]
    original = repo / "src/storage/ducklake_metadata_manager.cpp"
    assert digest(original) == BUILD.BASE_SOURCE_SHA256
    tracked = [
        Path(__file__).resolve(),
        Path(BUILD.__file__),
        original,
        repo / "test/vane/common/ducklake_filename_prefilter.hpp",
        repo / "test/vane/filename_prefilter.patch",
        repo / "test/vane/native_vortex_filename_oracle.cpp",
        repo / "test/vane/native_vortex_source_gc_benchmark.cpp",
        repo / "test/vane/native_vortex_source_driver.cpp",
    ]
    hashes = {str(p): digest(p) for p in tracked}
    entries = json.loads((build / "compile_commands.json").read_text())
    commands = []

    def compile_source(template, source, output):
        selected = [e for e in entries if Path(e["file"]).name == template.name]
        assert len(selected) == 1
        entry = selected[0]
        BUILD.validate_source(entry, template)
        command = shlex.split(entry["command"])
        command[command.index(entry["file"])] = str(source)
        command[command.index("-o") + 1] = str(output)
        command += ["-I" + str(repo / "test/vane")]
        subprocess.run(command, cwd=entry["directory"], check=True)
        commands.append({"command": command, "directory": entry["directory"], "sha256": digest(output)})

    text = subprocess.check_output(
        ["ninja", "-C", str(build), "-t", "commands", "ducklake_vortex_source_gc_benchmark"], text=True
    )
    lines = [line for line in text.splitlines() if " -o ducklake_vortex_source_gc_benchmark " in line]
    assert len(lines) == 1
    link = shlex.split(lines[0])
    assert link[:2] == [":", "&&"] and link[-2:] == ["&&", ":"]
    link = link[2:-2]
    extensions = BUILD.validate_link(link)
    positions = [i for i, arg in enumerate(link) if arg.endswith("/native_vortex_source_gc_benchmark.cpp.o")]
    assert len(positions) == 1
    position = positions[0]
    inputs = {
        str((build / arg).resolve()): digest((build / arg).resolve()) for arg in link if arg.endswith((".a", ".so"))
    }
    objects = {}
    for name, source in (("benchmark", tracked[6]), ("oracle", tracked[5]), ("source", tracked[7])):
        output = root / (name + ".o")
        compile_source(tracked[6], source, output)
        objects[name] = output
    patched = root / "src/storage/ducklake_metadata_manager.cpp"
    patched.parent.mkdir(parents=True)
    patched.write_bytes(original.read_bytes())
    subprocess.run(
        ["patch", "--batch", "--forward", "--fuzz=0", "-p1", "--input", str(tracked[4])], cwd=root, check=True
    )
    candidate = patched.read_text()
    prefix = '#include "common/ducklake_filename_prefilter.hpp"\n'
    assert candidate.startswith(prefix)
    candidate = candidate[len(prefix) :]
    baseline = original.read_text()
    # Restore source coordinates after the only edited function so unrelated
    # exception locations and source-file labels agree between the two objects.
    marker = "vector<DuckLakeFileForCleanup> DuckLakeMetadataManager::GetOrphanFilesForCleanup"
    line = baseline[: baseline.index(marker)].count("\n") + 1
    sources = {}
    for name, body in (("baseline", baseline), ("candidate", candidate)):
        assert body.count(marker) == 1
        body = body.replace(marker, f'#line {line} "ducklake_metadata_manager.cpp"\n' + marker)
        sources[name] = prefix + '#line 1 "ducklake_metadata_manager.cpp"\n' + body
    report = {
        "mode": "matched_filename_trial",
        "original_source_sha256": BUILD.BASE_SOURCE_SHA256,
        "source_sha256": hashes,
        "static_extensions": extensions,
        "link_inputs_sha256": inputs,
        "compile": commands,
        "metadata_sources": {},
        "executables": {},
        "links": {},
        "matched_object_position": position,
        "sampling_callbacks": "unchanged benchmark; callback flag enabled only for separate diagnostics",
    }
    for name in ("baseline", "candidate"):
        # Both compiles use this same source path and exactly the same flags.
        patched.write_text(sources[name])
        report["metadata_sources"][name] = {"sha256": digest(patched), "bytes": patched.stat().st_size}
        (root / (name + "-metadata.cpp")).write_bytes(patched.read_bytes())
        metadata = root / (name + "-metadata.o")
        compile_source(original, patched, metadata)
        report["executables"][name] = {}
        for tool, obj in objects.items():
            command = list(link)
            command[position : position + 1] = [str(obj), str(metadata)]
            output = root / (name + "-" + tool)
            command[command.index("-o") + 1] = str(output)
            subprocess.run(command, cwd=build, check=True)
            report["executables"][name][tool] = {"path": str(output), "sha256": digest(output)}
            report["links"][name + "-" + tool] = command
    assert all(digest(Path(p)) == h for p, h in {**hashes, **inputs}.items())
    (root / "build.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"mode": report["mode"], "executables": report["executables"]}, indent=2), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--build-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    run(parser.parse_args())
