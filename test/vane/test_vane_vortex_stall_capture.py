#!/usr/bin/env python3
"""Qualify stack capture and cleanup against controlled, owned test workers."""
from __future__ import annotations

import argparse
from contextlib import contextmanager, nullcontext
import importlib.util
import json
from pathlib import Path
import select
import shutil
import signal
import subprocess
import sys
import threading
import time
from unittest.mock import patch

PROBE = Path(__file__).with_name("test_vane_vortex_sustained_load.py")
SPEC = importlib.util.spec_from_file_location("load_proof", PROBE)
assert SPEC is not None and SPEC.loader is not None
LOAD = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(LOAD)
require = LOAD.require

WAITING_WORKER = """
import faulthandler, importlib.util, signal, sys
spec = importlib.util.spec_from_file_location('load_proof', sys.argv[1])
load = importlib.util.module_from_spec(spec)
spec.loader.exec_module(load)
load.enable_stall_capture()
if sys.argv[2] == 'handler_unregistered':
    faulthandler.unregister(signal.SIGUSR2)
print('waiting', flush=True)
sys.stdin.buffer.read(1)
"""


@contextmanager
def owned_worker(case, script, *arguments):
    case.mkdir(mode=0o700)
    with (case / "worker.log").open("wb") as log, subprocess.Popen(
        [sys.executable, "-I", "-c", script, *arguments],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=log,
    ) as child:
        try:
            yield child
        finally:
            # Synchronous capture or the event monitor has already returned.
            if child.poll() is None:
                child.kill()
                child.wait(timeout=5)


def release_worker(child):
    child.stdin.write(b"x")
    child.stdin.flush()


def check_stacks(case, mode):
    with owned_worker(case, WAITING_WORKER, str(PROBE), mode) as child:
        require(select.select([child.stdout], [], [], 10)[0], "Controlled worker did not become ready")
        require(child.stdout.readline().strip() == b"waiting", "Controlled worker failed before its wait")
        capture = (
            patch.object(LOAD.subprocess, "run", side_effect=subprocess.TimeoutExpired("gdb", 20))
            if mode == "native_timeout"
            else nullcontext()
        )
        with capture:
            LOAD.capture_stall(child, case, {"ready": True, "diagnostics_ready": True}, "controlled_wait")
        diagnostic = json.loads((case / "stall.json").read_text())
        require(diagnostic["python_dump_requested"], "Python stack signal was not requested")
        if mode == "native_timeout":
            require("gdb_error" in diagnostic, "GDB timeout was not recorded")
        else:
            require(diagnostic["gdb_exit_status"] == 0, "Native stack capture failed")
            require("Thread 1" in (case / "native-stacks.log").read_text(), "Native stack is absent")
        if mode != "handler_unregistered":
            deadline = time.monotonic() + 5
            while 'File "<string>"' not in (case / "worker.log").read_text():
                require(time.monotonic() < deadline, "Python stack dump did not arrive")
                time.sleep(0.01)
        release_worker(child)
        require(child.wait(timeout=5) == 0, "Capture changed the worker's natural exit")


def check_unready_watchdog(case):
    partial = b'{"kind":"query","payload":"\xe4\xb8'
    script = "import os, sys, threading; os.write(1, bytes.fromhex(sys.argv[1])); threading.Event().wait()"
    with owned_worker(case, script, partial.hex()) as child:
        try:
            LOAD.collect_worker_events(child, case, child.stdout, 2.0, True)
        except AssertionError as error:
            message = str(error)
            require(
                "watchdog_expired=True" in message and "Malformed event" in message,
                "A clipped event concealed the watchdog exit",
            )
        else:
            raise AssertionError("The startup stall did not reach the watchdog")
        require(child.returncode == -signal.SIGKILL, "The diagnostic terminated an unready child")
    diagnostic = json.loads((case / "stall.json").read_text())
    require(
        diagnostic["skipped"] == "worker_diagnostics_not_ready"
        and diagnostic["reason"] == "watchdog_deadline"
        and not diagnostic["python_dump_requested"]
        and not (case / "native-stacks.log").exists(),
        "Capture ran before the worker handshake",
    )
    require((case / "events.jsonl").read_bytes() == partial, "The truncated event was lost")


def check_exit_during_capture(case):
    script = """import signal, sys
signal.signal(signal.SIGUSR2, signal.SIG_IGN)
print('{"kind":"diagnostics_ready"}', flush=True)
sys.stdin.buffer.read(1)
"""
    owner = threading.current_thread()
    exit_observed, still_owned = threading.Event(), threading.Event()
    exited = LOAD.worker_exited
    original_reason = LOAD.stall_reason

    def observe_exit(child):
        result = exited(child)
        if result and threading.current_thread() is owner:
            exit_observed.set()
        return result

    with owned_worker(case, script) as child:

        def debugger(command, **kwargs):
            release_worker(child)
            if exit_observed.wait(5) and exited(child):
                still_owned.set()
            return subprocess.CompletedProcess(command, 0)

        def ready_to_capture(progress, now, deadline):
            if progress["diagnostics_ready"]:
                return original_reason(progress, now, deadline)
            return None

        with (
            patch.object(LOAD, "worker_exited", observe_exit),
            patch.object(LOAD.subprocess, "run", debugger),
            patch.object(LOAD, "stall_reason", ready_to_capture),
        ):
            LOAD.collect_worker_events(child, case, child.stdout, 10, True)
        require(child.returncode == 0 and still_owned.is_set(), "The worker was reaped during diagnostic capture")
        require(
            not json.loads((case / "stall.json").read_text())["python_dump_requested"],
            "Capture signaled an exited worker",
        )


def check_thread_start_failure(case):
    start = threading.Thread.start

    def fail_diagnostics(thread):
        if isinstance(thread, threading.Timer):
            return start(thread)
        raise RuntimeError("controlled diagnostic startup failure")

    with owned_worker(case, "import threading; threading.Event().wait()") as child:
        with patch.object(threading.Thread, "start", fail_diagnostics):
            try:
                LOAD.collect_worker_events(child, case, child.stdout, 10, True)
            except RuntimeError as error:
                require(str(error) == "controlled diagnostic startup failure", str(error))
            else:
                raise AssertionError("A failed diagnostic start was ignored")
        require(child.returncode == -signal.SIGKILL, "A failed diagnostic start left a worker alive")


def check_disabled(case):
    with owned_worker(case, """print('{"kind":"ready"}', flush=True)""") as child:
        events = LOAD.collect_worker_events(child, case, child.stdout, 5, False)
    require(events == [{"kind": "ready"}] and not (case / "stall.json").exists(), "Disabled mode attempted capture")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    require(shutil.which("gdb"), "Stall capture qualification requires gdb")
    root = args.output_dir.resolve()
    root.mkdir(mode=0o700, parents=True)
    for mode in ("ready", "native_timeout", "handler_unregistered"):
        check_stacks(root / mode, mode)
    check_unready_watchdog(root / "unready_watchdog")
    check_exit_during_capture(root / "exit_during_capture")
    check_thread_start_failure(root / "thread_start_failure")
    check_disabled(root / "disabled")
    require(
        LOAD.stall_reason({"ready": True, "time": 0, "query_time": 0}, 31, 100) == "no_events_30_seconds"
        and LOAD.stall_reason({"ready": True, "time": 30, "query_time": 0}, 31, 100) == "queries_stalled_30_seconds",
        "The fixture did not distinguish silence from a query-only stall",
    )
    report = dict(
        status="passed",
        native_stack=True,
        python_stack=True,
        worker_survived_capture=True,
        checks=[
            "native_timeout",
            "handler_unregistered",
            "unready_watchdog_and_truncated_event",
            "exit_during_capture",
            "thread_start_failure",
            "disabled",
            "stall_reason",
        ],
    )
    (root / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
