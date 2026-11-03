#!/usr/bin/env python3
"""Check file-cost accounting using real links, growth, deletion and WAL checkpoints."""
from pathlib import Path
import importlib.util
import os
import tempfile
import unittest

SPEC = importlib.util.spec_from_file_location("load_metrics", Path(__file__).with_name("vortex_load_metrics.py"))
assert SPEC is not None and SPEC.loader is not None
METRICS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(METRICS)


class InventoryTests(unittest.TestCase):
    def setUp(self):
        self.scratch = tempfile.TemporaryDirectory()
        self.addCleanup(self.scratch.cleanup)
        self.root = Path(self.scratch.name)
        for name in ["data", "indexes", "private/.ducklake-vortex-query-fixture"]:
            (self.root / name).mkdir(parents=True)
        self.source = self.root / "data/source.vortex"
        self.alias = self.root / "private/.ducklake-vortex-query-fixture/alias.vortex"
        self.source.write_bytes(b"data")
        os.link(self.source, self.alias)
        (self.root / "indexes/manifest.json").write_bytes(b"{}")
        (self.root / "indexes/generation-a").mkdir()
        (self.root / "indexes/generation-a/artifact").write_bytes(b"abc")
        (self.root / "metadata.ducklake").write_bytes(b"metadata")
        self.inventory = METRICS.FileInventory(self.root)

    def test_hardlinks_count_once_and_bootstrap_is_not_new_write_cost(self):
        result = self.inventory.sample()
        areas = result["areas"]
        self.assertTrue(result["baseline"])
        self.assertEqual(result["disappeared_during_sample"], 0)
        self.assertEqual(areas["source"]["attributed_inode_bytes"], 4)
        self.assertEqual(areas["query_scratch"]["logical_bytes"], 4)
        self.assertEqual(areas["query_scratch"]["attributed_inode_bytes"], 0)
        self.assertEqual(areas["index"]["attributed_inode_bytes"], 3)
        self.assertEqual(areas["caller_references"]["attributed_inode_bytes"], 2)
        self.assertEqual(sum(area["observed_new_inode_bytes"] for area in areas.values()), 0)

    def test_shared_inode_growth_and_owner_removal_do_not_invent_copies(self):
        self.inventory.sample()
        with self.source.open("ab") as stream:
            stream.write(b"more")
        result = self.inventory.sample()["areas"]
        self.assertEqual(result["source"]["observed_extent_growth_bytes"], 4)
        self.assertEqual(result["query_scratch"]["observed_extent_growth_bytes"], 0)
        self.source.unlink()
        result = self.inventory.sample()["areas"]
        self.assertEqual(result["query_scratch"]["attributed_inode_bytes"], 8)
        self.assertEqual(result["query_scratch"]["observed_new_inode_bytes"], 0)
        self.assertEqual(result["source"]["observed_removed_inode_bytes"], 0)
        self.alias.unlink()
        self.assertEqual(self.inventory.sample()["areas"]["query_scratch"]["observed_removed_inode_bytes"], 8)

    def test_wal_shrink_and_checkpoint_keep_cumulative_growth_separate(self):
        self.inventory.sample()
        wal = self.root / "metadata.ducklake.wal"
        wal.write_bytes(b"12345678")
        self.assertEqual(self.inventory.sample()["areas"]["wal"]["observed_new_inode_bytes"], 8)
        with wal.open("ab") as stream:
            stream.write(b"abcd")
        self.assertEqual(self.inventory.sample()["areas"]["wal"]["observed_extent_growth_bytes"], 4)
        with wal.open("r+b") as stream:
            stream.truncate(2)
        self.assertEqual(self.inventory.sample()["areas"]["wal"]["observed_extent_growth_bytes"], 4)
        wal.unlink()
        result = self.inventory.sample()
        self.assertEqual(result["disappeared_during_sample"], 0)
        self.assertEqual(result["areas"]["wal"]["observed_removed_inode_bytes"], 2)

    def test_symlinks_are_counted_without_following_external_targets(self):
        (self.root / "data/alias").symlink_to(self.root / "metadata.ducklake")
        result = self.inventory.sample()
        self.assertEqual(result["ignored_symlinks"], 1)
        self.assertEqual(result["areas"]["source"]["logical_bytes"], 4)

    def test_trend_keeps_ingestion_windows_and_rate(self):
        result = METRICS.trend([(100, 2), (130, 3), (160, 4), (220, 6)])
        self.assertEqual(result["slope_per_minute"], 2)
        self.assertEqual([w["minute"] for w in result["minute_windows"]], [0, 1, 2])
        self.assertEqual(result["last"], 6)


if __name__ == "__main__":
    unittest.main()
