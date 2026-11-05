#!/usr/bin/env python3
"""Check that A/A equivalence requires precision, complete input and no block drift."""
import importlib.util
from pathlib import Path
import unittest

SPEC = importlib.util.spec_from_file_location("gc_aa", Path(__file__).with_name("test_vane_vortex_source_gc_aa.py"))
AA = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(AA)


def samples(ratios):
    return [
        {"round": number, "variant": label, "complete_source_gc_ns": 1000000 * scale}
        for number, ratio in enumerate(ratios)
        for label, scale in (("baseline", 1), ("candidate", ratio), ("candidate", ratio), ("baseline", 1))
    ]


class CollectorAATests(unittest.TestCase):
    def test_equal_work_passes(self):
        self.assertTrue(AA.analyze(samples([1.0] * 64), 64)["precision_demonstrated"])

    def test_small_balanced_noise_passes(self):
        self.assertTrue(AA.analyze(samples([0.995, 1.005] * 32), 64)["precision_demonstrated"])

    def test_systematic_shift_fails(self):
        self.assertFalse(AA.analyze(samples([1.03] * 64), 64)["precision_demonstrated"])

    def test_contains_one_is_insufficient(self):
        result = AA.analyze(samples([0.97, 1.03] * 32), 64)
        self.assertLess(result["bootstrap_95pct"][0], 1)
        self.assertGreater(result["bootstrap_95pct"][1], 1)
        self.assertFalse(result["precision_demonstrated"])

    def test_aggregate_can_hide_block_drift(self):
        result = AA.analyze(samples([1.035] * 16 + [0.995] * 48), 64)
        self.assertTrue(result["interval_within_band"])
        self.assertFalse(result["blocks_within_band"])
        self.assertFalse(result["precision_demonstrated"])

    def test_missing_sample_fails(self):
        with self.assertRaises(AssertionError):
            AA.analyze(samples([1.0] * 64)[:-1], 64)

    def test_invalid_timing_fails(self):
        for bad in (0, -1, float("nan"), float("inf")):
            value = samples([1.0] * 64)
            value[0]["complete_source_gc_ns"] = bad
            with self.assertRaises(AssertionError):
                AA.analyze(value, 64)

    def test_incomplete_full_corpus_fails(self):
        values = [{"history": n, "history_storage": s, "task": t} for n, s, t in sorted(AA.FULL)]
        with self.assertRaises(AssertionError):
            AA.select_cases("full", values[:-1])
        with self.assertRaises(AssertionError):
            AA.select_cases("full", values + values[:1])
        selected = AA.select_cases("focus", values)
        self.assertEqual({(c["history"], c["history_storage"], c["task"]) for c in selected}, AA.FOCUS)

    def test_smoke_cannot_claim_precision(self):
        self.assertEqual(AA.decision("smoke", []), "smoke_only")

    def test_short_full_result_cannot_claim_precision(self):
        paired = AA.analyze(samples([1.0] * 16), 16)
        cases = [{"history": n, "history_storage": s, "task": t, "paired": paired} for n, s, t in sorted(AA.FULL)]
        with self.assertRaises(AssertionError):
            AA.decision("full", cases)


if __name__ == "__main__":
    unittest.main()
