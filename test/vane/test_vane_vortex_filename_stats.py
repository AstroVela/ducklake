#!/usr/bin/env python3
"""Require the filename target/control gates and independent path expectations."""
import copy
import importlib.util
from pathlib import Path
import unittest


def load(name, file):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(file))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


TRIAL = load("filename_ab", "test_vane_vortex_filename_ab.py")
ORACLE = load("filename_oracle", "test_vane_vortex_filename_oracle.py")


def cases():
    return [
        {
            "history": n,
            "history_storage": storage,
            "task": task,
            "paired": {
                "rounds": 16,
                "median_ratio": 0.8 if (n, storage, task) in TRIAL.TARGETS else 1.0,
                "bootstrap_95pct": [0.75, 0.85],
                "winning_rounds": 16,
            },
        }
        for n, storage, task in sorted(TRIAL.AA.FULL)
    ]


class FilenameTests(unittest.TestCase):
    def test_exact_two_targets_qualify(self):
        self.assertEqual(TRIAL.decision(cases(), "formal"), "candidate_qualified")

    def test_any_control_regression_withdraws(self):
        values = cases()
        values[0]["paired"]["median_ratio"] = 1.021
        self.assertEqual(TRIAL.decision(values, "formal"), "withdraw")

    def test_each_gain_criterion_is_required(self):
        for change in (dict(median_ratio=0.99), dict(bootstrap_95pct=[0.75, 1.001]), dict(winning_rounds=12)):
            values = cases()
            target = next(c for c in values if (c["history"], c["history_storage"], c["task"]) in TRIAL.TARGETS)
            target["paired"].update(change)
            self.assertEqual(TRIAL.decision(values, "formal"), "withdraw")

    def test_missing_duplicate_or_short_matrix_fails(self):
        values = cases()
        for bad in (
            values[:-1],
            values[:-1] + values[:1],
            [dict(c, paired=dict(c["paired"], rounds=15)) for c in values],
        ):
            with self.assertRaises(AssertionError):
                TRIAL.decision(bad, "formal")

    def test_smoke_cannot_qualify(self):
        values = [dict(c, paired=dict(c["paired"], rounds=2)) for c in cases() if c["history"] == 8]
        self.assertEqual(TRIAL.decision(values, "smoke"), "smoke_only")

    def test_model_handles_aliases_and_remote_identity(self):
        for raw, expected in (
            ("/a/b/../file", "/a/file"),
            ("../file", "file"),
            ("/file/child/..", "/file"),
            ("/file/.", "/file"),
            ("/file/", "/file"),
            ("", ""),
            ("/a/../..", "/"),
        ):
            self.assertEqual(ORACLE.canonical(raw, "/"), expected)
        self.assertEqual(ORACLE.canonical("s3://bucket/a/../file", "/"), "s3://bucket/a/../file")
        self.assertEqual(ORACLE.canonical("/base\\..\\file", "\\"), "/file")

    def test_binary_encoding_roundtrips(self):
        for raw in ("", "a\x00b", "数据", "space name\n"):
            self.assertEqual(ORACLE.decode(ORACLE.encode(raw)), raw)

    def test_native_separator_only_split_behavior_is_preserved(self):
        self.assertEqual(ORACLE.canonical("/", "/"), "//")
        self.assertEqual(ORACLE.canonical("///", "/"), "////")
        self.assertEqual(ORACLE.canonical("::::", "::"), "::::")


if __name__ == "__main__":
    unittest.main()
