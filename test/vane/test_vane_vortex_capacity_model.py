#!/usr/bin/env python3
"""Check the independent capacity oracle at fixed crossover and encoding examples."""
import importlib.util
from pathlib import Path
import unittest

SPEC = importlib.util.spec_from_file_location("capacity_model", Path(__file__).with_name("vortex_capacity_model.py"))
M = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(M)


class CapacityModelTests(unittest.TestCase):
    def test_precomputed_row_vector_crossover(self):
        self.assertEqual(
            {d: M.row_limit(d) for d in (1, 8, 128, 167, 168, 4096)},
            {1: 100000, 8: 100000, 128: 100000, 167: 100000, 168: 99864, 4096: 4096},
        )

    def test_invalid_dimensions_fail(self):
        for dimension in (0, -1, 4097):
            with self.assertRaises(ValueError):
                M.row_limit(dimension)

    def test_utf8_charge_and_nullable_empty_fields(self):
        row = {"id": 7, "embedding": [0.0, 1.0], "category": "类", "payload": "向量\x00"}
        self.assertEqual(M.row_bytes(row), 82)
        self.assertEqual(M.row_bytes(dict(row, category=None, payload="")), 72)

    def test_request_null_and_empty_have_distinct_identity(self):
        row = {"id": 7, "embedding": [0.0], "category": None, "payload": None}
        self.assertNotEqual(M.canonical([row], 1), M.canonical([dict(row, payload="")], 1))
        self.assertNotEqual(M.canonical([row], 1), M.canonical([row], 1, []))

    def test_signed_zero_and_float32_rounding_are_canonical(self):
        row = {"id": 7, "embedding": [128.0, 0.0], "category": None, "payload": ""}
        self.assertEqual(M.canonical([row], 2), M.canonical([dict(row, embedding=[128.0 + 1e-8, -0.0])], 2))
        self.assertEqual(M.canonical([row], 2), M.canonical([dict(row, embedding=[128.0, -1e-100])], 2))

    def test_delete_keys_and_row_order_change_identity(self):
        a = {"id": 1, "embedding": [1.0], "category": None, "payload": None}
        b = dict(a, id=2)
        self.assertNotEqual(M.canonical([a, b], 1), M.canonical([b, a], 1))
        self.assertNotEqual(M.canonical([a], 1, [7]), M.canonical([a], 1, [8]))

    def test_manifest_boundaries_use_realistic_sized_paths(self):
        for size in (1048575, 1048576, 1048577):
            manifest = M.sized_manifest(size)
            self.assertEqual(len(M.compact(manifest).encode()), size)
            files = manifest["segments"][0]["reference"]["snapshot"]["files"]
            self.assertEqual(len(files), 4096)
            self.assertTrue(all(len(file["uri"]) < 512 for file in files))


if __name__ == "__main__":
    unittest.main()
