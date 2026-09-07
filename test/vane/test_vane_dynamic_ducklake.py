#!/usr/bin/env python3
"""Qualify the installed DuckLake provider locally or on two Ray workers."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import tempfile
import time
from importlib import import_module
from importlib.metadata import entry_points
from pathlib import Path

WORKER_COUNT = 2
FILE_COUNT = 8
ROWS_PER_FILE = 256
ROW_COUNT = FILE_COUNT * ROWS_PER_FILE


def load_test_helpers(name: str) -> object:
    path = Path(__file__).resolve().with_name(f"{name}.py")
    specification = importlib.util.spec_from_file_location(name, path)
    if specification is None or specification.loader is None:
        raise AssertionError(f"cannot load packaged integration helpers: {path}")
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    return module


local_helpers = load_test_helpers("test_vane_wheel_ducklake")
require_equal = local_helpers.require_equal
sql_string = local_helpers.sql_string


def load_dynamic_ducklake(vane: object, connection: object) -> dict[str, object]:
    from vane.extensions import LocalExtensionProvider

    trust_identity = os.environ.get("VANE_EXPECTED_EXTENSION_TRUST_IDENTITY")
    if trust_identity not in {"vane-ci-test-key", "astrovela/vane-testpypi"}:
        raise AssertionError("VANE_EXPECTED_EXTENSION_TRUST_IDENTITY must select the explicit qualification trust root")
    matches = [
        candidate
        for candidate in entry_points(group="vane.dynamic_extension_providers")
        if candidate.name == "ducklake"
    ]
    require_equal(len(matches), 1, "installed DuckLake provider count")
    entry_point = matches[0]
    provider = entry_point.load()()
    if not isinstance(provider, LocalExtensionProvider):
        raise AssertionError("the DuckLake entry point must return LocalExtensionProvider")
    descriptor = import_module(entry_point.module).descriptor()
    require_equal(descriptor.name, "ducklake", "provider extension name")
    require_equal(descriptor.trust_identity, trust_identity, "provider trust root")
    require_equal(descriptor.dependencies, (), "dynamic provider dependencies")
    require_equal(descriptor.vane_version, vane.__version__, "provider runtime version")
    artifact = provider.find(descriptor.identity)
    if artifact is None or artifact.descriptor != descriptor:
        raise AssertionError("the installed provider does not own its exact DuckLake descriptor")
    digest = hashlib.sha256(descriptor.to_json().encode()).hexdigest()
    require_equal(entry_point.module, f"vane_extensions.ducklake_{digest}", "packaged provider module")

    security = connection.execute(
        "SELECT CAST(current_setting('allow_unsigned_extensions') AS BOOLEAN), "
        "CAST(current_setting('autoinstall_known_extensions') AS BOOLEAN), "
        "CAST(current_setting('autoload_known_extensions') AS BOOLEAN)"
    ).fetchone()
    require_equal(security, (False, False, False), "dynamic extension security settings")
    query = "SELECT loaded, installed, install_mode FROM duckdb_extensions() WHERE extension_name = 'ducklake'"
    state = connection.execute(query).fetchone()
    if state not in (None, (False, False, "NOT_INSTALLED")):
        raise AssertionError(f"DuckLake must not be installed or statically linked before provider loading: {state!r}")
    resolved = vane.load_installed_extension("ducklake", connection=connection)
    require_equal(resolved.descriptor, descriptor, "loaded provider descriptor")
    require_equal(connection.execute(query).fetchone(), (True, False, "NOT_INSTALLED"), "dynamic DuckLake load state")
    manifest = [json.loads(entry) for entry in connection._export_dynamic_extension_snapshot_entries()]
    require_equal(manifest, [descriptor.to_dict()], "trusted dynamic extension snapshot")
    return descriptor.to_dict()


def provider_connection(vane: object) -> tuple[object, dict[str, object]]:
    connection = vane.connect(
        ":memory:",
        config={
            "autoinstall_known_extensions": "false",
            "autoload_known_extensions": "false",
        },
    )
    try:
        connection.execute("LOAD parquet")
        descriptor = load_dynamic_ducklake(vane, connection)
        return connection, descriptor
    except BaseException:
        connection.close()
        raise


def seed_local_lake(vane: object, root: Path, *, scan_fixture: bool) -> dict[str, object]:
    connection, descriptor = provider_connection(vane)
    try:
        connection.execute(
            f"ATTACH {sql_string('ducklake:' + str(root / 'metadata.ducklake'))} AS lake "
            f"(DATA_PATH {sql_string(root / 'data')}, DATA_INLINING_ROW_LIMIT 0)"
        )
        connection.execute("CREATE TABLE lake.items(id INTEGER, payload VARCHAR)")
        connection.execute("INSERT INTO lake.items VALUES (1, 'one'), (2, 'two'), (3, 'three')")
        connection.execute("UPDATE lake.items SET payload = 'updated' WHERE id = 2")
        connection.execute("DELETE FROM lake.items WHERE id = 1")
        require_equal(
            connection.sql("SELECT id, payload FROM lake.items ORDER BY id").fetchall(),
            [(2, "updated"), (3, "three")],
            "provider-backed local CRUD",
        )
        if scan_fixture:
            connection.execute("CREATE TABLE lake.source(id INTEGER, payload VARCHAR)")
            for file_index in range(FILE_COUNT):
                start = file_index * ROWS_PER_FILE
                connection.execute(
                    "INSERT INTO lake.source SELECT i::INTEGER, ('value-' || i::VARCHAR)::VARCHAR "
                    f"FROM range({start}, {start + ROWS_PER_FILE}) AS rows(i)"
                )
            require_equal(
                connection.execute("SELECT count(*) FROM ducklake_list_files('lake', 'source')").fetchone(),
                (FILE_COUNT,),
                "independent DuckLake data files",
            )
        connection.execute("DETACH lake")
    finally:
        connection.close()
    return descriptor


class AnnotateWorkerNode:
    """Record which execution node consumes each DuckLake-backed batch."""

    def __call__(self, table: object) -> object:
        import pyarrow as pa
        import ray

        time.sleep(0.25)
        node_id = str(ray.get_runtime_context().get_node_id())
        return pa.table({"id": table.column("id"), "worker_node_id": [node_id] * table.num_rows})


def exercise_ray_scan(vane: object, root: Path, descriptor: dict[str, object]) -> None:
    import ray
    from vane import runners

    if ray.is_initialized():
        raise RuntimeError("dynamic DuckLake qualification must own its Ray cluster")
    helpers = load_test_helpers("test_vane_wheel_ray_ducklake")
    cluster = helpers.create_two_worker_cluster(ray)
    connection = None
    runner = None
    original_run_iter_tables = None
    try:
        expected_nodes = helpers.execution_node_ids(ray)
        vane.set_runner_ray(noop_if_initialized=True)
        runner = runners.get_or_create_runner()
        require_equal(getattr(runner, "name", None), "ray", "configured runner")
        os.environ["VANE_RUNNER"] = "local-fast"
        try:
            connection, loaded_descriptor = provider_connection(vane)
            require_equal(loaded_descriptor, descriptor, "reopened provider identity")
            connection.execute(
                f"ATTACH {sql_string('ducklake:' + str(root / 'metadata.ducklake'))} AS lake "
                f"(DATA_PATH {sql_string(root / 'data')}, READ_ONLY)"
            )
        finally:
            os.environ["VANE_RUNNER"] = "ray"
        plan = helpers.make_physical_plan(vane, connection, "SELECT id, payload FROM lake.source")
        require_equal(
            sum(len(batches) for batches in plan.scan_split_batch_map().values()),
            FILE_COUNT,
            "independently schedulable DuckLake file splits",
        )
        require_equal(plan.__getstate__()[6]["dynamic_extensions"], [descriptor], "worker preparation manifest")

        dispatch_count = 0
        original_run_iter_tables = runner.run_iter_tables

        def record_distributed_read(*args: object, **kwargs: object) -> object:
            nonlocal dispatch_count
            dispatch_count += 1
            return original_run_iter_tables(*args, **kwargs)

        runner.run_iter_tables = record_distributed_read
        require_equal(
            connection.sql("SELECT id, payload FROM lake.source WHERE id BETWEEN 510 AND 514 ORDER BY id").fetchall(),
            [(value, f"value-{value}") for value in range(510, 515)],
            "distributed provider-backed projection and filter",
        )
        annotated_rows = (
            connection.sql("SELECT id, payload FROM lake.source")
            .map_batches(
                AnnotateWorkerNode,
                schema={"id": vane.sqltype("INTEGER"), "worker_node_id": vane.sqltype("VARCHAR")},
                batch_size=64,
                cpus=1.0,
                execution_backend="ray_actor",
                actor_number=WORKER_COUNT,
                target_max_batch_bytes=4096,
            )
            .fetchall()
        )
        require_equal(
            sorted(row[0] for row in annotated_rows), list(range(ROW_COUNT)), "complete distributed row coverage"
        )
        require_equal({str(row[1]) for row in annotated_rows}, expected_nodes, "two-worker provider scan topology")
        if dispatch_count < 2:
            raise AssertionError("DuckLake queries did not execute through the Ray runner")
    finally:
        if runner is not None and original_run_iter_tables is not None:
            runner.run_iter_tables = original_run_iter_tables
        try:
            if connection is not None:
                connection.close()
        finally:
            try:
                vane.teardown_runner()
            finally:
                try:
                    if ray.is_initialized():
                        ray.shutdown()
                finally:
                    cluster.shutdown()


def main() -> None:
    runner_kind = os.environ.get("VANE_RUNNER")
    if runner_kind not in {"local-fast", "ray"}:
        raise RuntimeError("dynamic DuckLake qualification requires VANE_RUNNER=local-fast or ray")
    os.environ["VANE_FTE_DYNAMIC_SCAN_MAX_SPLITS_PER_PARTITION"] = "1"
    os.environ["VANE_RUNNER"] = "local-fast"
    import vane

    try:
        with tempfile.TemporaryDirectory(prefix="vane-dynamic-ducklake-") as value:
            root = Path(value)
            try:
                descriptor = seed_local_lake(vane, root, scan_fixture=runner_kind == "ray")
            finally:
                vane.teardown_runner()
            os.environ["VANE_RUNNER"] = runner_kind
            if runner_kind == "ray":
                exercise_ray_scan(vane, root, descriptor)
    finally:
        os.environ["VANE_RUNNER"] = runner_kind


if __name__ == "__main__":
    main()
