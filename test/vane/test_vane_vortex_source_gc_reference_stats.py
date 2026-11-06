#!/usr/bin/env python3
"""Reject unmatched reference scopes, weakened row coverage and double-counted time."""
import copy
import importlib.util
from pathlib import Path
import unittest


def load(name, filename):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(filename))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


REF = load("gc_reference", "test_vane_vortex_source_gc_reference.py")
FIXTURE = load("gc_metadata_stats", "test_vane_vortex_source_gc_metadata_stats.py")


def sample():
    value = FIXTURE.sample()
    value.update(before={"data_files": 3, "delete_files": 0}, deleted_paths=["/data/a", "/data/b"])
    for query in value["queries"]:
        query["connection_id"] = 7
    known = [q for q in value["queries"] if REF.META.category(q["sql"]) == "known_source_paths"]
    value["reference_calls"] = [
        dict(
            path="/data/" + ("a" if i < 2 else "b"),
            connection_id=7,
            begin_ns=q["begin_ns"] - 2,
            end_ns=q["end_ns"] + 4,
            call_ns=18,
            fetch_ns=1,
            path_loop_ns=3,
            rows=3,
            chunks=1,
            fetch_calls=2,
            completed=True,
            found=False,
        )
        for i, q in enumerate(known)
    ]
    return value


class ReferenceTests(unittest.TestCase):
    def test_partition_conserves_without_sql_overlap(self):
        value = sample()
        parts = REF.partition(value)
        self.assertEqual(sum(parts.values()), value["complete_source_gc_ns"])
        self.assertEqual(parts["known_source_paths"], 48)
        self.assertEqual(parts["reference_fetch"], 4)
        self.assertEqual(parts["reference_path_loop"], 12)
        self.assertEqual(parts["reference_remainder"], 8)
        self.assertEqual(parts["outside_reference"], 472)
        self.assertNotIn("outside_sql", parts)
        result = REF.summarize([value, copy.deepcopy(value)])
        self.assertEqual(result["rows_per_collector"], [12])

    def test_missing_duplicate_or_overlapping_call_fails(self):
        values = []
        value = sample()
        value["reference_calls"].pop()
        values.append(value)
        value = sample()
        value["reference_calls"][1] = copy.deepcopy(value["reference_calls"][0])
        values.append(value)
        for value in values:
            with self.assertRaises(AssertionError):
                REF.partition(value)

    def test_wrong_connection_or_path_fails(self):
        for change in (dict(connection_id=8), dict(path="/data/other")):
            value = sample()
            value["reference_calls"][0].update(change)
            with self.assertRaises(AssertionError):
                REF.partition(value)

    def test_reference_true_incomplete_or_missing_rows_fails(self):
        for change in (dict(found=True), dict(completed=False), dict(rows=2), dict(fetch_calls=1), dict(chunks=0)):
            value = sample()
            value["reference_calls"][0].update(change)
            with self.assertRaises(AssertionError):
                REF.partition(value)

    def test_boundary_duration_and_negative_remainder_fail(self):
        for change in (dict(begin_ns=0), dict(end_ns=1001), dict(call_ns=17), dict(fetch_ns=7), dict(path_loop_ns=-1)):
            value = sample()
            value["reference_calls"][0].update(change)
            with self.assertRaises(AssertionError):
                REF.partition(value)

    def test_sql_straddling_reference_boundary_fails(self):
        value = sample()
        call = value["reference_calls"][0]
        call.update(begin_ns=call["begin_ns"] + 3, call_ns=call["call_ns"] - 3)
        with self.assertRaises(AssertionError):
            REF.partition(value)

    def test_other_sql_inside_reference_scope_fails(self):
        value = sample()
        call = value["reference_calls"][0]
        call.update(begin_ns=call["begin_ns"] - 20, call_ns=call["call_ns"] + 20)
        with self.assertRaises(AssertionError):
            REF.partition(value)

    def test_primary_and_warmup_cannot_enter_summary(self):
        for kind in ("primary", "warmup"):
            value = sample()
            value["kind"] = kind
            with self.assertRaises(AssertionError):
                REF.summarize([value, copy.deepcopy(value)])


if __name__ == "__main__":
    unittest.main()
