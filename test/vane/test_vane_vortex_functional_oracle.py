#!/usr/bin/env python3
"""Ensure the functional oracle rejects incorrect visibility, filtering and top-k."""
import copy
import importlib.util
from pathlib import Path
import struct
import unittest

SPEC = importlib.util.spec_from_file_location(
    "matrix", Path(__file__).with_name("test_vane_vortex_functional_matrix.py")
)
M = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(M)


def model():
    return {i: {"id": i, "embedding": [float(i)], "category": "A", "payload": f"原始\x00-{i}"} for i in range(5)}


def addresses(values):
    return {row_id: (1, "fixture", row_id) for row_id in values}


def hits(values):
    return [dict(row, distance=row["embedding"][0] ** 2) for row in values.values()]


class OracleTests(unittest.TestCase):
    def test_fixture_components_are_exact_float32(self):
        for d in M.DIMENSIONS:
            for ordinal in (0, 64, 64.125, 136, 300):
                v = M.vector(ordinal, d)
                self.assertEqual(v, list(struct.unpack("<" + "f" * d, struct.pack("<" + "f" * d, *v))))

    def test_exact_topk_requires_every_expected_key(self):
        values = model()
        self.assertEqual([r["id"] for r in M.ranked(values, [1.25], None, 2, addresses(values))], [1, 2])
        M.check_hits(hits(values)[:2], values, [0.0], None, 2, True, addresses(values))
        with self.assertRaises(AssertionError):
            M.check_hits(hits(values)[:1], values, [0.0], None, 2, True, addresses(values))

    def test_equal_float32_scores_use_physical_address_order(self):
        values = {
            0: dict(model()[0], embedding=[10000.0, 0.0]),
            1: dict(model()[1], embedding=[10000.0, 1.0]),
        }
        address = {0: (1, "inline", 0), 1: (0, 1, 0)}
        target = [0.0, 0.0]
        self.assertEqual([r["id"] for r in M.ranked(values, target, None, 2, address)], [1, 0])
        first, second = (dict(values[key], distance=100000000.0) for key in (1, 0))
        M.check_hits([first], values, target, None, 1, True, address)
        with self.assertRaises(AssertionError):
            M.check_hits([second], values, target, None, 1, True, address)
        with self.assertRaises(AssertionError):
            M.check_hits([second, first], values, target, None, 2, True, address)
        # One ULP of distance error stays within tolerance but cannot change an exact tie.
        with self.assertRaisesRegex(AssertionError, "Incorrect exact top-k order"):
            M.check_hits([dict(second, distance=99999992.0), first], values, target, None, 2, True, address)

    def test_shared_dense_score_keeps_small_terms(self):
        vector = [4096.0] + [1.0] * 4095
        zero = [0.0] * len(vector)
        for order in (vector, vector[::-1]):
            self.assertEqual(M.MIXED.float32_squared_l2(zero, order), float((1 << 24) + 4096))

    def test_ann_gate_preserves_existing_point_eight_threshold(self):
        values = model()
        self.assertEqual(M.check_hits(hits(values)[:4], values, [0.0], None, 5, False, addresses(values)), 0.8)
        with self.assertRaises(AssertionError):
            M.check_hits(hits(values)[:3], values, [0.0], None, 5, False, addresses(values))

    def test_wrong_visibility_version_and_distance_fail(self):
        values = model()
        for changed in (
            {"id": 999},
            {"payload": "old"},
            {"embedding": [0.0, 1.0]},
            {"category": "B"},
            {"distance": 1.0},
            {"distance": float("nan")},
            {"distance": -0.00005},
        ):
            result = hits(values)
            result[0].update(changed)
            with self.assertRaises(AssertionError):
                M.check_hits(result, values, [0.0], None, 5, False, addresses(values))

    def test_category_empty_and_null_remain_distinct(self):
        values = model()
        values[0]["category"] = ""
        values[1]["category"] = None
        values[2]["category"] = "a"
        self.assertEqual([r["id"] for r in M.ranked(values, [0.0], "", 5, addresses(values))], [0])
        self.assertEqual([r["id"] for r in M.ranked(values, [0.0], "a", 5, addresses(values))], [2])
        M.check_hits([], values, [0.0], "absent", 64, True, addresses(values))
        with self.assertRaises(AssertionError):
            M.check_hits(hits(values)[:1], values, [0.0], "A", 1, False, addresses(values))

    def test_order_duplicate_and_excess_hits_fail(self):
        values = model()
        for result, k in ((list(reversed(hits(values))), 5), (hits(values)[:1] * 2, 5), (hits(values), 4)):
            with self.assertRaises(AssertionError):
                M.check_hits(copy.deepcopy(result), values, [0.0], None, k, False, addresses(values))


if __name__ == "__main__":
    unittest.main()
