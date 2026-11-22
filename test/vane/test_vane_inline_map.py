#!/usr/bin/env python3
"""Exercise frozen MAP schemas through the native distributed worker interface."""

from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path


def load_helpers():
    path = Path(__file__).with_name("test_vane_wheel_ray_ducklake.py")
    specification = importlib.util.spec_from_file_location("inline_map_helpers", path)
    if specification is None or specification.loader is None:
        raise AssertionError(f"cannot load worker transport helpers: {path}")
    helpers = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(helpers)
    return helpers


class InlineMapTests(unittest.TestCase):
    def setUp(self):
        import vane

        self.vane = vane
        self.helpers = load_helpers()
        self.directory = tempfile.TemporaryDirectory(prefix="vane-inline-map-")
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.coordinator = self.connect()
        self.worker = self.connect()
        self.runner = vane.ray_cxx.DistributedPhysicalPlanRunner()
        self.addCleanup(self.runner.shutdown)
        self.coordinator.execute(
            f"ATTACH {self.helpers.sql_string('ducklake:' + str(self.root / 'metadata.duckdb'))} "
            f"AS lake (DATA_PATH {self.helpers.sql_string(self.root / 'data')}, DATA_INLINING_ROW_LIMIT 1000)"
        )

    def connect(self):
        connection = self.vane._native._connect_with_runner("local-fast")
        self.addCleanup(connection.close)
        connection.execute("SET autoinstall_known_extensions=false")
        connection.execute("SET autoload_known_extensions=false")
        self.helpers.load_test_extensions(connection)
        return connection

    def capture(self, query):
        plan = self.helpers.make_physical_plan(self.vane, self.coordinator, query)
        node_id, batches = self.helpers.single_scan_source(plan)
        fragment = self.helpers.capture_worker_fragment(
            self.vane, self.coordinator, plan, node_id
        )
        return node_id, self.vane.ray_cxx.merge_scan_split_batches(batches), fragment

    def read_worker(self, captured):
        node_id, batch, fragment = captured
        result = self.runner.execute_native(
            self.worker.cursor(),
            fragment.clone(self.worker),
            scan_split_batch={node_id: batch},
        )
        self.assertEqual(result.completion_status, "ok")
        rows = [
            row
            for table in result.partition_payloads
            for row in zip(*(c.to_pylist() for c in table.columns))
        ]
        self.assertEqual(
            self.worker.execute(
                "SELECT count(*) FROM duckdb_databases() WHERE database_name='lake'"
            ).fetchone(),
            (0,),
        )
        return sorted(rows, key=lambda row: row[0])

    def native_rows(self, query):
        table = self.coordinator.execute(query).to_arrow_table()
        return sorted(
            zip(*(column.to_pylist() for column in table.columns)),
            key=lambda row: row[0],
        )

    def test_map_schemas_and_projections_before_and_after_flush(self):
        self.coordinator.execute(
            "CREATE TABLE lake.items("
            "id BIGINT, key_value VARCHAR, direct MAP(VARCHAR, INTEGER), "
            "nested STRUCT(inner_map MAP(VARCHAR, INTEGER)), "
            "listed MAP(VARCHAR, INTEGER)[], deep MAP(VARCHAR, MAP(VARCHAR, INTEGER)))"
        )
        self.coordinator.execute(
            "INSERT INTO lake.items VALUES "
            "(1,'business value',MAP(['key','null'],[2,NULL]), "
            "{'inner_map':MAP(['nested'],[3])}, [MAP(['listed'],[4]),NULL], "
            "MAP(['outer'],[MAP(['inner'],[5])])), "
            "(2,'empty',MAP(),{'inner_map':MAP()},[],MAP()), "
            "(3,NULL,NULL,{'inner_map':NULL},NULL,NULL), "
            "(4,'null struct',NULL,NULL,[MAP()],NULL)"
        )
        self.assertEqual(list((self.root / "data").rglob("*.parquet")), [])
        queries = [
            "SELECT * FROM lake.items",
            "SELECT id FROM lake.items",
            "SELECT id,nested.inner_map AS nested_map FROM lake.items WHERE id<>2",
            "SELECT id,direct['key'] AS entry FROM lake.items WHERE id<3",
            "SELECT id,listed,deep FROM lake.items WHERE id>=2",
        ]
        expected = {query: self.native_rows(query) for query in queries}
        for phase in ("inline", "flushed"):
            if phase == "flushed":
                self.coordinator.execute("CALL ducklake_flush_inlined_data('lake')")
                self.assertTrue(list((self.root / "data").rglob("*.parquet")))
            for query in queries:
                with self.subTest(phase=phase, query=query):
                    self.assertEqual(self.native_rows(query), expected[query])
                    self.assertEqual(
                        self.read_worker(self.capture(query)), expected[query]
                    )

    def test_frozen_map_replay_after_update_delete_and_flush(self):
        self.coordinator.execute(
            "CREATE TABLE lake.items(id BIGINT, data MAP(VARCHAR, INTEGER))"
        )
        self.coordinator.execute(
            "INSERT INTO lake.items VALUES (1,MAP(['key'],[2])),(2,NULL),(3,MAP())"
        )
        query = "SELECT id,data FROM lake.items"
        expected = self.native_rows(query)
        captured = self.capture(query)
        self.assertEqual(self.read_worker(captured), expected)
        self.coordinator.execute(
            "UPDATE lake.items SET data=MAP(['changed'],[7]) WHERE id=1"
        )
        self.coordinator.execute("DELETE FROM lake.items WHERE id=2")
        self.coordinator.execute("CALL ducklake_flush_inlined_data('lake')")
        for replay in range(2):
            with self.subTest(replay=replay):
                self.assertEqual(self.read_worker(captured), expected)
        current = self.native_rows(query)
        self.assertNotEqual(current, expected)
        self.assertEqual(self.read_worker(self.capture(query)), current)


if __name__ == "__main__":
    unittest.main(verbosity=2)
