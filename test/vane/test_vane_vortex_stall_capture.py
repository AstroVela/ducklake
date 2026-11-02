#!/usr/bin/env python3
"""Prove native/Python stall capture observes a waiting worker without terminating it."""
from __future__ import annotations

import argparse
import importlib.util
import json
from pathlib import Path
import select
import shutil
import subprocess
import sys
import time

PROBE = Path(__file__).with_name("test_vane_vortex_sustained_load.py")
SPEC = importlib.util.spec_from_file_location("load_proof", PROBE)
assert SPEC is not None and SPEC.loader is not None
LOAD = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(LOAD)
require = LOAD.require


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    require(shutil.which("gdb"), "Stall capture qualification requires gdb")
    root = args.output_dir.resolve()
    root.mkdir(mode=0o700, parents=True)
    script = (
        "import importlib.util,sys,threading;"
        "s=importlib.util.spec_from_file_location('load_proof',sys.argv[1]);"
        "m=importlib.util.module_from_spec(s);s.loader.exec_module(m);"
        "m.enable_stall_capture();print('waiting',flush=True);threading.Event().wait()"
    )
    with (root / "python-stacks.log").open("w") as log:
        child = subprocess.Popen(
            [sys.executable, "-I", "-c", script, str(PROBE)],
            stdout=subprocess.PIPE,
            stderr=log,
            text=True,
        )
        try:
            require(select.select([child.stdout], [], [], 10)[0], "Controlled worker did not become ready")
            require(child.stdout.readline().strip() == "waiting", "Controlled worker failed before its wait")
            LOAD.capture_stall(child, root, {"ready": True, "kind": "controlled_wait"}, "controlled_wait")
            deadline = time.monotonic() + 5
            while 'File "<string>"' not in (root / "python-stacks.log").read_text():
                require(time.monotonic() < deadline, "Python stack dump did not arrive")
                time.sleep(0.01)
            diagnostic = json.loads((root / "stall.json").read_text())
            require(diagnostic["gdb_exit_status"] == 0, "Native stack capture failed; inspect native-stacks.log")
            require("Thread 1" in (root / "native-stacks.log").read_text(), "Native worker stack is absent")
            require(child.poll() is None, "Diagnostic capture terminated the worker")
        finally:
            if child.poll() is None:
                child.terminate()
            child.wait(timeout=5)
            child.stdout.close()
    report = dict(status="passed", native_stack=True, python_stack=True, worker_survived_capture=True)
    (root / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
