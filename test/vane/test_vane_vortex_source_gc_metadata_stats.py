#!/usr/bin/env python3
"""Check trace conservation, nested scopes and diagnostic-only accounting."""
import copy
import importlib.util
from pathlib import Path
import unittest

SPEC = importlib.util.spec_from_file_location(
    "gc_metadata", Path(__file__).with_name("test_vane_vortex_source_gc_metadata.py")
)
GC = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(GC)


def sample():
    catalog = GC.CATALOG
    known = "SELECT REPLACE( AS full_path " + " ".join(
        catalog + table
        for table in ("ducklake_data_file f", "ducklake_delete_file f", "ducklake_table t", "ducklake_schema s")
    )
    statements = (
        ["BEGIN TRANSACTION"] * 10
        + ["COMMIT"] * 5
        + [known] * 4
        + ["SELECT count(*) FROM " + catalog + "ducklake_files_scheduled_for_deletion WHERE data_file_id=1"] * 4
        + ["SELECT table_uuid,reference FROM " + catalog + "ducklake_vortex_indexes_v1"] * 4
        + ["SELECT other_" + str(i) for i in range(15)]
    )
    queries = []
    for i, sql in enumerate(statements):
        begin = i * 20 + 1
        queries.append(
            dict(sql=sql, depth=0, begin_ns=begin, end_ns=begin + 12, query_ns=12, bind_plan_ns=4, execute_fetch_ns=8)
        )
        if sql == "COMMIT":
            queries.append(dict(sql=sql, depth=1, begin_ns=begin + 5, end_ns=begin + 6, query_ns=1))
    stages = ["source_gc_waiting"] + [
        "source_before_plan",
        "source_delete_intent_committed",
        "source_before_unlink",
        "source_after_unlink",
        "source_delete_completed",
    ] * 2
    return dict(
        kind="diagnostic",
        diagnostic=True,
        round=0,
        block=0,
        variant="baseline",
        complete_source_gc_ns=1000,
        queries=queries,
        phases=[
            dict(stage=stage, total_ns=value)
            for stage, value in (("wait", 1), ("metadata", 900), ("expire", 0), ("artifact", 20), ("unlink", 30))
        ],
        checkpoints=[dict(stage=stage, elapsed_ns=i) for i, stage in enumerate(stages)],
    )


class MetadataTests(unittest.TestCase):
    def test_nested_commits_are_excluded_and_partition_conserves(self):
        value = sample()
        parts, top = GC.trace_partition(value)
        self.assertEqual(len(top), 42)
        self.assertEqual(parts["commit"], 60)
        self.assertEqual(parts["outside_sql"], 496)
        self.assertEqual(sum(parts.values()), 1000)
        report = GC.summarize([value, copy.deepcopy(value)])
        self.assertEqual(report["excluded_nested_queries"], 10)
        self.assertTrue(all(p["covariance_share"] is None for p in report["outer_partition"].values()))

    def test_overlap_out_of_bounds_or_duration_mismatch_fails(self):
        for change in (
            dict(begin_ns=0, end_ns=30, query_ns=30, bind_plan_ns=4, execute_fetch_ns=26),
            dict(end_ns=1001),
            dict(query_ns=13),
        ):
            value = sample()
            value["queries"][0].update(change)
            with self.assertRaises(AssertionError):
                GC.trace_partition(value)

    def test_orphan_negative_depth_and_missing_nested_commit_fail(self):
        value = sample()
        index = next(i for i, q in enumerate(value["queries"]) if q["depth"] == 1)
        for change in (dict(depth=2), dict(depth=-1), dict(begin_ns=999, end_ns=1000, query_ns=1)):
            bad = copy.deepcopy(value)
            bad["queries"][index].update(change)
            with self.assertRaises(AssertionError):
                GC.trace_partition(bad)
        del value["queries"][index]
        with self.assertRaises(AssertionError):
            GC.trace_partition(value)

    def test_partial_or_inconsistent_split_fails(self):
        for field in ("bind_plan_ns", "execute_fetch_ns"):
            value = sample()
            del value["queries"][0][field]
            with self.assertRaises(AssertionError):
                GC.trace_partition(value)
        for change in (dict(bind_plan_ns=5), dict(bind_plan_ns=-1, execute_fetch_ns=13)):
            value = sample()
            value["queries"][0].update(change)
            with self.assertRaises(AssertionError):
                GC.trace_partition(value)

    def test_missing_split_is_reported_as_unsplit(self):
        value = sample()
        for field in ("bind_plan_ns", "execute_fetch_ns"):
            del value["queries"][0][field]
        result = GC.summarize([value, copy.deepcopy(value)])
        begin = next(q for q in result["exact_outer_sql"] if q["sql"] == "BEGIN TRANSACTION")
        self.assertEqual((begin["observed_split_calls"], begin["unsplit_calls"]), (18, 2))
        self.assertEqual(begin["unsplit_total_ms"], 24 / 1e6)

    def test_changed_reference_query_or_count_fails(self):
        for missing in (True, False):
            value = sample()
            index = next(i for i, q in enumerate(value["queries"]) if q["sql"].startswith("SELECT REPLACE("))
            if missing:
                del value["queries"][index]
            else:
                value["queries"][index]["sql"] += " ducklake_files_scheduled_for_deletion"
            with self.assertRaises(AssertionError):
                GC.trace_partition(value)

    def test_checkpoint_order_or_boundary_fails(self):
        for change in (dict(stage="source_gc_deferred"), dict(elapsed_ns=1001)):
            value = sample()
            value["checkpoints"][0].update(change)
            with self.assertRaises(AssertionError):
                GC.trace_partition(value)

    def test_signed_covariance_shares_conserve(self):
        shares = GC.covariance_shares({"rising": [1, 3, 5], "falling": [3, 2, 1]}, [4, 5, 6])
        self.assertEqual(shares, {"rising": 2, "falling": -1})
        with self.assertRaises(AssertionError):
            GC.covariance_shares({"incomplete": [1, 2]}, [3, 4])

    def test_primary_or_warmup_cannot_enter_summary(self):
        for kind, diagnostic in (("primary", False), ("warmup", True)):
            value = sample()
            value.update(kind=kind, diagnostic=diagnostic)
            with self.assertRaises(AssertionError):
                GC.summarize([value])

    def test_inventory_requires_exact_round_block_and_labels(self):
        values = [
            dict(sample(), round=r, block=b, variant=label)
            for r in range(2)
            for b, label in enumerate(GC.block_labels(r))
        ]
        GC.validate_inventory(values, 2)
        for bad in (values[:-1], values[:-1] + values[:1], [dict(v, variant="baseline") for v in values]):
            with self.assertRaises(AssertionError):
                GC.validate_inventory(bad, 2)


if __name__ == "__main__":
    unittest.main()
