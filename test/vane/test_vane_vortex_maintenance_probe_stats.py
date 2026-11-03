#!/usr/bin/env python3
"""Check that the matched-probe analysis rejects noise and negligible gains."""
import importlib.util
from pathlib import Path
import unittest

SPEC = importlib.util.spec_from_file_location(
    "probe_benchmark", Path(__file__).with_name("test_vane_vortex_maintenance_probe_benchmark.py")
)
BENCH = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BENCH)


def observations(ratios):
    samples = []
    for number, ratio in enumerate(ratios):
        for block in range(4):
            combined = (block in [1, 2]) != bool(number % 2)
            for repeat in range(2):
                cost = (1000000 + number * 1000 + block * 10000 + repeat * 100) * (ratio if combined else 1)
                samples.append(
                    dict(
                        round=number,
                        block=block,
                        variant="combined" if combined else "split",
                        **{stage: cost for stage in BENCH.STAGES}
                    )
                )
    return BENCH.paired_summary(dict(samples=samples, batch=2))


class MatchedProbeAnalysis(unittest.TestCase):
    def test_linear_drift_does_not_create_a_gain(self):
        result = observations([1] * 16)
        self.assertEqual(result["median_ratio"], 1)
        self.assertEqual(result["bootstrap_median_ratio_95pct"], [1, 1])
        self.assertFalse(BENCH.meets_gain(result))

    def test_repeated_gain_survives_counterbalanced_drift(self):
        result = observations([0.95] * 16)
        self.assertAlmostEqual(result["median_ratio"], 0.95)
        self.assertEqual(result["winning_rounds"], 16)
        self.assertTrue(BENCH.meets_gain(result))

    def test_tiny_gain_is_insufficient(self):
        self.assertFalse(BENCH.meets_gain(observations([0.999] * 16)))

    def test_opposing_rounds_are_not_a_repeatable_gain(self):
        result = observations([0.9, 1.1] * 8)
        self.assertLess(result["bootstrap_median_ratio_95pct"][0], 1)
        self.assertGreater(result["bootstrap_median_ratio_95pct"][1], 1)
        self.assertFalse(BENCH.meets_gain(result))


if __name__ == "__main__":
    unittest.main()
