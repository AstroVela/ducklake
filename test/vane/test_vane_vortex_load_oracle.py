#!/usr/bin/env python3
"""Reject ambiguous or stale load evidence and resolve the actual captured snapshot."""
from copy import deepcopy
import importlib.util
from pathlib import Path
import unittest

SPEC = importlib.util.spec_from_file_location(
    "load_proof", Path(__file__).with_name("test_vane_vortex_sustained_load.py")
)
assert SPEC is not None and SPEC.loader is not None
LOAD = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(LOAD)


class SnapshotOracleTests(unittest.TestCase):
    def setUp(self):
        self.initial = {key: LOAD.row(key, 0, 8, "automatic") for key in range(512)}
        self.current = deepcopy(self.initial)
        upserts, deletes, _ = LOAD.plan(list(range(512)), 1, 8, "automatic")
        for key in deletes:
            del self.current[key]
        self.current.update((entry["id"], entry) for entry in upserts)

    def events(self, snapshot=25, state=None, category=None):
        state = self.current if state is None else state
        vector = self.initial[0]["embedding"]
        rows = [
            dict(
                entry,
                distance=sum((a - b) ** 2 for a, b in zip(entry["embedding"], vector)),
            )
            for entry in state.values()
            if category is None or entry["category"] == category
        ]
        rows.sort(key=lambda entry: (entry["distance"], entry["id"]))
        return [
            dict(kind="ready", seed_snapshot=10),
            dict(kind="ack", epoch=1, receipt=dict(snapshot_id=20), finished_ns=100),
            *[
                dict(
                    kind="query",
                    snapshot_id=snapshot,
                    before=0,
                    after=20,
                    before_time_ns=50,
                    canary=i % 2 == 0,
                    vector=vector,
                    category=category,
                    rows=deepcopy(rows[:10]),
                )
                for i in range(12)
            ],
        ]

    def validate(self, events):
        return LOAD.validate_queries(events, 512, 8, "automatic")

    def test_physical_snapshot_between_mutations_uses_last_logical_commit(self):
        result, expected = self.validate(self.events())
        self.assertEqual(result["oracle"], "actual_read_view_snapshot")
        self.assertEqual(result["queries_overlapping_commits"], 12)
        self.assertEqual(result["minimum_recall"], 1)
        self.assertEqual(expected, [self.current[key] for key in sorted(self.current)])

    def test_capture_before_overlapping_commit_uses_old_state(self):
        result, _ = self.validate(self.events(snapshot=15, state=self.initial))
        self.assertEqual(result["minimum_recall"], 1)

    def test_compatible_other_cut_cannot_hide_wrong_row_versions(self):
        with self.assertRaisesRegex(AssertionError, "actual ReadView snapshot's committed model"):
            self.validate(self.events(snapshot=25, state=self.initial))

    def test_actual_cut_resolves_different_recalls_for_identical_typed_hits(self):
        events = self.events()
        vector = self.initial[139]["embedding"]
        rows = [
            dict(
                entry,
                distance=sum((a - b) ** 2 for a, b in zip(entry["embedding"], vector)),
            )
            for entry in self.current.values()
        ]
        rows.sort(key=lambda entry: (entry["distance"], entry["id"]))
        for entry in events[2:]:
            entry.update(vector=vector, rows=rows[:10])
        self.assertEqual(LOAD.valid_hits(events[2], self.initial), 0.6)
        self.assertEqual(LOAD.valid_hits(events[2], self.current), 1)
        result, _ = self.validate(events)
        self.assertEqual(result["minimum_recall"], 1)
        for entry in events[2:]:
            entry["snapshot_id"] = 15
        result, _ = self.validate(events)
        self.assertEqual(result["minimum_recall"], 0.6)
        self.assertFalse(result["exploration_meets_0_8_target"])

    def test_missing_or_invalid_snapshot_never_uses_watermark_fallback(self):
        for value in [None, True, "25", 9]:
            events = self.events()
            for entry in events[2:]:
                entry["snapshot_id"] = value
            with self.subTest(value=value), self.assertRaisesRegex(AssertionError, "no actual ReadView snapshot"):
                self.validate(events)
        events = self.events()
        del events[2]["snapshot_id"]
        with self.assertRaisesRegex(AssertionError, "no actual ReadView snapshot"):
            self.validate(events)

    def test_snapshot_cannot_cross_observed_commit_window(self):
        for before, after, snapshot in [(0, 0, 25), (20, 20, 15)]:
            events = self.events(snapshot=snapshot)
            for entry in events[2:]:
                entry.update(before=before, after=after)
            with self.subTest(before=before), self.assertRaisesRegex(
                AssertionError, "outside its committed read window"
            ):
                self.validate(events)

    def test_empty_prefilter_still_requires_actual_snapshot(self):
        result, _ = self.validate(self.events(category="absent"))
        self.assertEqual(result["minimum_recall"], 1)

    def test_real_low_recall_keeps_unchanged_gate_failed(self):
        events = self.events()
        vector = events[2]["vector"]
        rows = [
            dict(
                entry,
                distance=sum((a - b) ** 2 for a, b in zip(entry["embedding"], vector)),
            )
            for entry in self.current.values()
        ]
        rows.sort(key=lambda entry: (entry["distance"], entry["id"]))
        for entry in events[2:]:
            entry["rows"] = rows[-10:]
        result, _ = self.validate(events)
        self.assertEqual(result["minimum_recall"], 0)
        self.assertFalse(result["canary_gate_passed"])
        self.assertFalse(result["exploration_meets_0_8_target"])


class ProfileTests(unittest.TestCase):
    def setUp(self):
        self.begin = [dict(profile_id="one", stage="source_expire", calls=1, total_ns=20, max_ns=20)]
        self.end = [dict(profile_id="one", stage="source_expire", calls=3, total_ns=50, max_ns=20)]

    def test_measured_delta_keeps_lifetime_max_separate(self):
        result = LOAD.profile_delta(self.begin, self.end)["source_expire"]
        self.assertEqual(result["calls"], 2)
        self.assertEqual(result["total_ms"], 30 / 1e6)
        self.assertEqual(result["average_ms"], 15 / 1e6)
        self.assertEqual(result["lifetime_max_ms"], 20 / 1e6)

    def test_reset_is_rejected_instead_of_subtracting_new_profile(self):
        self.end[0]["profile_id"] = "two"
        with self.assertRaisesRegex(AssertionError, "reset inside"):
            LOAD.profile_delta(self.begin, self.end)

    def test_decreased_or_duplicated_counters_are_rejected(self):
        self.end[0]["total_ns"] = 19
        with self.assertRaisesRegex(AssertionError, "invalid or decreased"):
            LOAD.profile_delta(self.begin, self.end)
        with self.assertRaisesRegex(AssertionError, "Duplicate"):
            LOAD.profile_delta(self.begin * 2, self.end * 2)


if __name__ == "__main__":
    unittest.main()
