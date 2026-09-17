#!/usr/bin/env python3
"""Exercise DuckLake CRUD from a packaged wheel with the default Ray runner."""

from __future__ import annotations

import importlib.util
import os
import tempfile
from pathlib import Path


def require_equal(actual: object, expected: object, description: str) -> None:
    if actual != expected:
        raise AssertionError(f"{description}: expected {expected!r}, got {actual!r}")


def sql_string(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def verify_extension_is_wheel_linked(connection: object) -> None:
    extension = connection.execute(
        "SELECT loaded, install_mode FROM duckdb_extensions() WHERE extension_name = 'ducklake'"
    ).fetchone()
    if extension is None:
        raise AssertionError("the packaged Vane wheel does not contain ducklake")
    require_equal(extension[1], "STATICALLY_LINKED", "ducklake install mode before LOAD")
    connection.execute("LOAD parquet")
    connection.execute("LOAD sqlite_scanner")
    connection.execute("LOAD ducklake")
    loaded = connection.execute(
        "SELECT loaded, install_mode FROM duckdb_extensions() WHERE extension_name = 'ducklake'"
    ).fetchone()
    require_equal(loaded, (True, "STATICALLY_LINKED"), "ducklake after LOAD")


def main() -> None:
    if "VANE_RUNNER" in os.environ:
        raise RuntimeError("leave VANE_RUNNER unset to qualify the default Ray runner")

    import ray
    import vane
    from vane import runners

    specification = importlib.util.spec_from_file_location(
        "ray_helpers", Path(__file__).with_name("test_vane_wheel_ray_ducklake.py")
    )
    helpers = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(helpers)
    if ray.is_initialized():
        raise RuntimeError("the wheel smoke must own its Ray cluster")
    cluster = helpers.create_two_worker_cluster(ray)
    try:
        require_equal(getattr(runners.get_or_create_runner(), "name", None), "ray", "default runner")
        exercise_crud(vane)
    finally:
        try:
            vane.teardown_runner()
        finally:
            ray.shutdown()
            cluster.shutdown()


def exercise_crud(vane: object) -> None:
    with tempfile.TemporaryDirectory(prefix="vane-ducklake-smoke-") as temporary_directory:
        root = Path(temporary_directory)
        connection = vane.connect(
            ":memory:",
            config={
                "autoinstall_known_extensions": "false",
                "autoload_known_extensions": "false",
            },
        )
        try:
            verify_extension_is_wheel_linked(connection)
            connection.execute(
                f"ATTACH 'ducklake:sqlite:{root / 'metadata.sqlite'}' AS lake "
                f"(DATA_PATH {sql_string(root / 'data')}, DATA_INLINING_ROW_LIMIT 0)"
            )
            connection.execute("CREATE TABLE lake.items(id INTEGER, payload VARCHAR)")
            connection.execute("INSERT INTO lake.items VALUES (1, 'one'), (2, 'two'), (3, 'three')")
            connection.execute("DELETE FROM lake.items WHERE id = 2")
            require_equal(
                connection.sql("SELECT id, payload FROM lake.items ORDER BY id").fetchall(),
                [(1, "one"), (3, "three")],
                "default Ray DuckLake readback",
            )
        finally:
            connection.close()


if __name__ == "__main__":
    main()
