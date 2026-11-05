#!/usr/bin/env python3
"""Reject faster samples that changed restored input, task, or observation scope."""
import copy
import importlib.util
import os
from pathlib import Path
import tempfile
import unittest


SPEC = importlib.util.spec_from_file_location(
    "source_gc_benchmark", Path(__file__).with_name("test_vane_vortex_source_gc_benchmark.py")
)
BENCH = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BENCH)


class SourceGCEvidenceTests(unittest.TestCase):
    def sample(self):
        before = {
            "snapshots": 13,
            "first_snapshot": 5,
            "last_snapshot": 17,
            "scheduled": 0,
            "intents": 0,
            "expired_total": 5,
            "files_total": 0,
            "bytes_total": 0,
            "data_files": 3,
            "delete_files": 0,
        }
        after = dict(before, snapshots=11, first_snapshot=7, scheduled=2, expired_total=7, data_files=1)
        expected = dict(before, task="expire", history=8, history_storage="inline")
        sample = {
            "before": before,
            "after": after,
            "task": "expire",
            "history": 8,
            "history_storage": "inline",
            "diagnostic": False,
            "complete_source_gc_ns": 100,
            "expired": 2,
            "files": 0,
            "bytes": 0,
            "deleted_paths": [],
            "phases": [
                {"stage": stage, "calls": 1, "total_ns": 10}
                for stage in ("wait", "metadata", "expire", "artifact", "unlink")
            ],
            "queries": [],
            "kind": "primary",
        }
        BENCH.validate_sample(sample, expected, False)
        return sample, expected

    def test_smaller_restored_history_is_rejected(self):
        sample, expected = self.sample()
        sample["before"]["snapshots"] -= 1
        sample["before"]["first_snapshot"] += 1
        sample["after"]["snapshots"] -= 1
        sample["after"]["first_snapshot"] += 1
        with self.assertRaises(AssertionError):
            BENCH.validate_sample(sample, expected, False)

    def test_zero_progress_is_rejected(self):
        sample, expected = self.sample()
        sample.update(expired=0, after=copy.deepcopy(sample["before"]), complete_source_gc_ns=1)
        sample["phases"] = [dict(p, calls=0, total_ns=0) for p in sample["phases"]]
        with self.assertRaises(AssertionError):
            BENCH.validate_sample(sample, expected, False)

    def test_warmups_and_callback_timings_cannot_enter_primary_summary(self):
        sample, _ = self.sample()
        self.assertEqual(BENCH.summary([sample])["samples"], 1)
        for changes in ({"kind": "warmup"}, {"kind": "diagnostic", "diagnostic": True}):
            with self.subTest(changes=changes), self.assertRaises(AssertionError):
                BENCH.summary([sample, dict(sample, **changes)])

    def test_images_reject_hard_links_and_symlinks(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first = root / "input"
            first.write_bytes(b"same bytes, shared inode")
            before = BENCH.manifest(root)
            os.link(first, root / "linked")
            with self.assertRaises(RuntimeError):
                BENCH.manifest(root)
            (root / "linked").unlink()
            (root / "symlink").symlink_to(first)
            with self.assertRaises(RuntimeError):
                BENCH.manifest(root)
            (root / "symlink").unlink()
            self.assertEqual(BENCH.manifest(root), before)

    def test_nested_commit_is_not_counted_twice(self):
        sample = {
            "diagnostic": True,
            "kind": "diagnostic",
            "complete_source_gc_ns": 110,
            "queries": [
                {"sql": "COMMIT", "depth": 1, "begin_ns": 5, "end_ns": 95, "query_ns": 90},
                {"sql": "COMMIT", "depth": 0, "begin_ns": 0, "end_ns": 100, "query_ns": 100},
            ],
        }
        result = BENCH.diagnostic_summary([sample])
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["calls"], 1)
        self.assertEqual(result[0]["total_ms"], 100 / 1e6)
        sample["queries"][0]["end_ns"] = 101
        sample["queries"][0]["query_ns"] = 96
        with self.assertRaises(AssertionError):
            BENCH.diagnostic_summary([sample])


if __name__ == "__main__":
    unittest.main()
