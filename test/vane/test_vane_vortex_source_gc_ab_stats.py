#!/usr/bin/env python3
"""Check that collector retention requires material gains and consistent paired rounds."""
import importlib.util
from pathlib import Path
import unittest

SPEC = importlib.util.spec_from_file_location("gc_ab", Path(__file__).with_name("test_vane_vortex_source_gc_ab.py"))
AB = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(AB)


def samples(ratios):
    result = []
    for number, ratio in enumerate(ratios):
        for variant, scale in (("baseline", 1), ("candidate", ratio), ("candidate", ratio), ("baseline", 1)):
            result.append({"round": number, "variant": variant, "complete_source_gc_ns": 1000000 * scale})
    return result


class CollectorABTests(unittest.TestCase):
    def test_consistent_material_gain_passes(self):
        self.assertTrue(AB.meets_gain(AB.paired(samples([0.75] * 16), 16)))

    def test_tiny_gain_fails(self):
        self.assertFalse(AB.meets_gain(AB.paired(samples([0.99] * 16), 16)))

    def test_opposing_rounds_fail(self):
        self.assertFalse(AB.meets_gain(AB.paired(samples([0.8] * 12 + [1.4] * 4), 16)))

    def test_missing_variant_sample_fails(self):
        with self.assertRaises(AssertionError):
            AB.paired(samples([0.75] * 16)[:-1], 16)


if __name__ == "__main__":
    unittest.main()
