#!/usr/bin/env python3
"""Check affinity rejection, inheritance/restoration and the focused precision gate."""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import unittest

SPEC = importlib.util.spec_from_file_location(
    "gc_affinity", Path(__file__).with_name("test_vane_vortex_source_gc_affinity.py")
)
PIN = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PIN)


def rows(cpus):
    return [{"cpu": cpu, "package": 0, "core": cpu, "numa": ["node0"]} for cpu in cpus]


def focus(passes):
    return {
        "status": "completed",
        "profile": "focus",
        "decision": ("precision_demonstrated" if all(passes) else "precision_not_demonstrated"),
        "cases": [
            {
                "history": n,
                "history_storage": storage,
                "task": task,
                "paired": {"rounds": 16, "precision_demonstrated": passed},
            }
            for (n, storage, task), passed in zip(sorted(PIN.AA.FOCUS), passes)
        ],
    }


class AffinityTests(unittest.TestCase):
    def test_invalid_cpu_syntax_fails(self):
        for value in ("", "1,", "-1", "1,1", "1-3", "x"):
            with self.assertRaises(argparse.ArgumentTypeError):
                PIN.parse_cpus(value)
        self.assertEqual(PIN.parse_cpus("3,1"), [1, 3])

    def test_unavailable_cpu_fails(self):
        with self.assertRaises(AssertionError):
            PIN.validate_cpus([1, 2], {1}, rows([1, 2]), True)

    def test_smt_siblings_fail(self):
        value = rows([1, 2])
        value[1]["core"] = 1
        with self.assertRaises(AssertionError):
            PIN.validate_cpus([1, 2], {1, 2}, value, True)

    def test_mixed_or_unknown_numa_fails(self):
        for node in (["node1"], []):
            value = rows([1, 2])
            value[1]["numa"] = node
            with self.assertRaises(AssertionError):
                PIN.validate_cpus([1, 2], {1, 2}, value, True)

    def test_formal_requires_thread_headroom(self):
        with self.assertRaises(AssertionError):
            PIN.validate_cpus([1], {1}, rows([1]), True)
        PIN.validate_cpus([1, 2], {1, 2}, rows([1, 2]), True)

    def test_child_inherits_affinity_and_thread_environment(self):
        original = os.sched_getaffinity(0)
        threads = PIN.thread_environment()
        selected = {min(original)}
        with PIN.pinned_affinity(selected):
            child = subprocess.check_output(
                [
                    sys.executable,
                    "-I",
                    "-c",
                    "import os,json; keys="
                    + repr(PIN.THREAD_ENV)
                    + "; print(json.dumps({'cpus':sorted(os.sched_getaffinity(0)),'env':{k:os.environ.get(k) for k in keys}}))",
                ],
                text=True,
            )
            child = json.loads(child)
            self.assertEqual(child["cpus"], sorted(selected))
            self.assertEqual({k: child["env"].get(k) for k in PIN.THREAD_ENV}, threads)
        self.assertEqual(os.sched_getaffinity(0), original)
        self.assertEqual(PIN.thread_environment(), threads)

    def test_exception_restores_affinity(self):
        original = os.sched_getaffinity(0)
        with self.assertRaisesRegex(RuntimeError, "probe"):
            with PIN.pinned_affinity({min(original)}):
                raise RuntimeError("probe")
        self.assertEqual(os.sched_getaffinity(0), original)

    def test_failed_focus_does_not_advance(self):
        self.assertFalse(PIN.advance_after_focus(focus([True, False])))
        self.assertTrue(PIN.advance_after_focus(focus([True, True])))

    def test_smoke_or_incomplete_report_cannot_advance(self):
        bad = focus([True, True])
        bad["profile"] = "smoke"
        with self.assertRaises(AssertionError):
            PIN.advance_after_focus(bad)
        bad = focus([True, True])
        bad["cases"].pop()
        with self.assertRaises(AssertionError):
            PIN.advance_after_focus(bad)
        bad = focus([True, False])
        bad["decision"] = "precision_demonstrated"
        with self.assertRaises(AssertionError):
            PIN.advance_after_focus(bad)


if __name__ == "__main__":
    unittest.main()
