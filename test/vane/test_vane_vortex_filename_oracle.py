#!/usr/bin/env python3
"""Compare native baseline/candidate paths and real metadata references to independent models."""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import itertools
import json
import os
from pathlib import Path
import shutil
import subprocess
import time

SPEC = importlib.util.spec_from_file_location(
    "gc_pin", Path(__file__).with_name("test_vane_vortex_source_gc_affinity.py")
)
PIN = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PIN)
BASE = PIN.AA.BASE
REMOTE = (
    "http://",
    "https://",
    "s3://",
    "s3a://",
    "s3n://",
    "gcs://",
    "gs://",
    "r2://",
    "azure://",
    "az://",
    "abfss://",
    "hf://",
)


def canonical(path, separator):
    if path.startswith(REMOTE):
        return path
    components = []
    # Native StringUtil::Split retains the original input when every field is empty.
    fields = [component for component in path.split(separator) if component] or [path]
    for component in fields:
        if component in ("", "."):
            continue
        if component == "..":
            if components:
                components.pop()
        else:
            components.append(component)
    return ("/" if path.startswith("/") else "") + separator.join(components)


def encode(value):
    return value.encode().hex() or "-"


def decode(value):
    return bytes.fromhex(value).decode() if value != "-" else ""


def path_cases():
    tokens = ("", ".", "..", "a", "b", "shared.vortex", "数据", "résumé")
    for separator in ("/", "\\", "::", "aa", "é"):
        paths = {"", "/", ".", "..", "file", "folder\\..\\file", "a/b/..", "a/.", "same/file", "other/file"}
        for count in range(1, 4):
            for components in itertools.product(tokens, repeat=count):
                value = separator.join(components)
                for prefix, suffix in itertools.product(("", "/"), ("", separator)):
                    paths.add(prefix + value + suffix)
        paths |= {prefix + "bucket/a/../file" for prefix in REMOTE}
        paths |= {"/a/\x00file", "/a/\nfile", "/a/white space", "/a/单个向量.vortex"}
        for known in sorted(paths):
            normalized = canonical(known, separator)
            for target in sorted(
                {normalized, normalized + "-different", "wrong/" + normalized, known, "/other/shared.vortex"}
            ):
                yield separator, known, target


def metadata_cases(root):
    variants = (
        "shared.vortex",
        "./shared.vortex",
        "unused/../shared.vortex",
        "shared.vortex/.",
        "shared.vortex/child/..",
        "shared.vortex/",
        "other.vortex",
        "数据/shared.vortex",
    )
    for kind, historical, file_relative, table_relative, schema_relative in itertools.product((0, 1), repeat=5):
        schema = "schema/./" if schema_relative else "/absolute-schema/"
        table = "table//" if table_relative else "/absolute-table/"
        for variant in variants:
            file = variant if file_relative else "/absolute-file/" + variant
            if not file_relative:
                full = file
            elif not table_relative:
                full = table + file
            elif not schema_relative:
                full = schema + table + file
            else:
                full = str(root / "data") + "/" + schema + table + file
            for target in (canonical(full, "/"), "/different-directory/shared.vortex"):
                yield (
                    kind,
                    historical,
                    file_relative,
                    table_relative,
                    schema_relative,
                    schema,
                    table,
                    file,
                    target,
                ), canonical(full, "/") == target
    for prefix in REMOTE:
        file = prefix + "bucket/dir/../shared.vortex"
        for target in (file, prefix + "bucket/shared.vortex"):
            yield (0, 1, 0, 1, 1, "schema/", "table/", file, target), canonical(file, "/") == target


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run(args):
    root = args.output_dir.resolve()
    root.mkdir(mode=0o700)
    manifest = json.loads(args.build_report.read_text())
    assert manifest["mode"] == "matched_filename_trial"
    inputs = args.inputs.resolve()
    source = json.loads(inputs.read_text())
    assert source["status"] == "completed" and source["decision"] == "baseline_only"
    tracked = {
        **manifest["source_sha256"],
        **manifest["link_inputs_sha256"],
        str(args.build_report.resolve()): digest(args.build_report),
        str(inputs): digest(inputs),
        str(Path(__file__).resolve()): digest(Path(__file__)),
    }
    for variant in ("baseline", "candidate"):
        for value in manifest["executables"][variant].values():
            tracked[value["path"]] = value["sha256"]
    assert all(digest(Path(p)) == h for p, h in tracked.items())
    cpus, original = args.cpus, sorted(os.sched_getaffinity(0))
    PIN.validate_cpus(cpus, original, PIN.topology(cpus), args.profile == "formal")
    threads = PIN.thread_environment()
    cases = list(path_cases())
    path_input = root / "paths.txt"
    with path_input.open("w") as stream:
        for number, (separator, known, target) in enumerate(cases):
            stream.write(f"{number} {encode(separator)} {encode(known)} {encode(target)}\n")
    report = {
        "status": "running",
        "profile": args.profile,
        "path_cases": len(cases),
        "metadata": [],
        "identities": tracked,
        "cpus": cpus,
        "thread_environment": threads,
    }
    BASE.save(root / "report.json", report)
    started = time.monotonic()
    try:
        with PIN.pinned_affinity(cpus):
            observed = []
            for variant in ("baseline", "candidate"):
                driver = manifest["executables"][variant]["oracle"]["path"]
                output = root / (variant + "-paths.txt")
                subprocess.run([driver, "paths", str(path_input), str(output)], check=True, timeout=120)
                lines = output.read_text().splitlines()
                assert len(lines) == len(cases)
                rejects = positives = retained_negatives = 0
                for number, (line, (separator, known, target)) in enumerate(zip(lines, cases)):
                    fields = line.split()
                    assert len(fields) == 5 and int(fields[0]) == number
                    may, actual, candidate = map(int, fields[1:4])
                    expected = canonical(known, separator)
                    assert decode(fields[4]) == expected and actual == candidate == int(expected == target), (
                        number,
                        separator,
                        known,
                        target,
                        fields,
                    )
                    assert not actual or may
                    rejects += not may
                    positives += actual
                    retained_negatives += may and not actual
                assert rejects > 0 and positives > 0 and retained_negatives > 0
                observed.append(
                    {
                        "variant": variant,
                        "cases": len(cases),
                        "excluded": rejects,
                        "matches": positives,
                        "retained_nonmatches": retained_negatives,
                        "output_sha256": digest(output),
                    }
                )
            selected = PIN.AA.select_cases("smoke" if args.profile == "smoke" else "focus", source["cases"])
            selected = [c for c in selected if c["task"] == "files"]
            assert len(selected) == 2
            for case in selected:
                work, image = Path(case["work"]), Path(case["image"])
                expected = inputs.parent / f"history-{case['history']}-{case['history_storage']}-files"
                assert work == expected / "work" and image == expected / "image" and expected.resolve() == expected
                assert not work.is_symlink() and not image.is_symlink()
                rows = list(metadata_cases(work))
                name = f"metadata-{case['history']}-{case['history_storage']}"
                path = root / (name + ".txt")
                with path.open("w") as stream:
                    for number, (row, _) in enumerate(rows):
                        stream.write(
                            " ".join(map(str, (number, *row[:5]))) + " " + " ".join(encode(v) for v in row[5:]) + "\n"
                        )
                outputs = []
                for variant in ("baseline", "candidate"):
                    assert BASE.fingerprint(BASE.manifest(image)) == case["input_image_sha256"]
                    shutil.rmtree(work)
                    shutil.copytree(image, work, copy_function=shutil.copy2)
                    before = BASE.manifest(work)
                    output = root / (variant + "-" + name + ".txt")
                    with (root / (variant + "-" + name + ".log")).open("w") as log:
                        result = subprocess.run(
                            [
                                manifest["executables"][variant]["oracle"]["path"],
                                "metadata",
                                str(work),
                                str(path),
                                str(output),
                            ],
                            stdout=log,
                            stderr=subprocess.STDOUT,
                            timeout=180,
                        )
                    assert result.returncode == 0, str(root / (variant + "-" + name + ".log"))
                    lines = output.read_text().splitlines()
                    assert lines[-2:] == ["error_propagation 1", "rollback_state 1"] and len(lines) == len(rows) + 2
                    for number, (line, (_, wanted)) in enumerate(zip(lines, rows)):
                        assert line == f"{number} {int(wanted)}", (variant, case["history"], number, line, wanted)
                    after = BASE.manifest(work)
                    assert all(
                        after.get(p) == value for p, value in before.items() if p.startswith(("data/", "indexes/"))
                    )
                    assert BASE.fingerprint(BASE.manifest(image)) == case["input_image_sha256"]
                    outputs.append(output.read_text())
                assert outputs[0] == outputs[1]
                report["metadata"].append(
                    {
                        "history": case["history"],
                        "history_storage": case["history_storage"],
                        "cases_per_variant": len(rows),
                        "variants": 2,
                        "native_and_delete_historical_references": "passed",
                        "error_propagation": "passed",
                        "rollback_state": "passed",
                    }
                )
            assert PIN.thread_environment() == threads
        assert all(digest(Path(p)) == h for p, h in tracked.items())
        assert sorted(os.sched_getaffinity(0)) == original
        report.update(status="passed", path_variants=observed, restored_affinity=original, input_images="unchanged")
    except BaseException as error:
        report.update(status="failed", error=f"{type(error).__name__}: {error}")
        raise
    finally:
        report["seconds"] = time.monotonic() - started
        BASE.save(root / "report.json", report)
    print(json.dumps({k: report[k] for k in ("status", "path_cases", "metadata", "seconds")}, indent=2), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--build-report", type=Path, required=True)
    parser.add_argument("--inputs", type=Path, required=True)
    parser.add_argument("--cpus", type=PIN.parse_cpus, required=True)
    parser.add_argument("--profile", choices=("formal", "smoke"), required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    run(parser.parse_args())
