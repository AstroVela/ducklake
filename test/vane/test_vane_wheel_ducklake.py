#!/usr/bin/env python3
"""Exercise native DuckLake scans from a packaged Vane wheel."""

from __future__ import annotations

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
    connection.execute("LOAD ducklake")
    loaded = connection.execute(
        "SELECT loaded, install_mode FROM duckdb_extensions() WHERE extension_name = 'ducklake'"
    ).fetchone()
    require_equal(loaded, (True, "STATICALLY_LINKED"), "ducklake after LOAD")


def main() -> None:
    if os.environ.get("VANE_RUNNER") != "local-fast":
        raise RuntimeError("the wheel integration test requires VANE_RUNNER=local-fast")

    import vane

    with tempfile.TemporaryDirectory(prefix="vane-ducklake-local-") as temporary_directory:
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
                f"ATTACH 'ducklake:{root / 'metadata.ducklake'}' AS lake "
                f"(DATA_PATH {sql_string(root / 'data')}, DATA_INLINING_ROW_LIMIT 0)"
            )
            connection.execute("CREATE TABLE lake.items(id INTEGER, payload VARCHAR)")
            connection.execute("INSERT INTO lake.items VALUES (1, 'one'), (2, 'two'), (3, 'three')")
            connection.execute("DELETE FROM lake.items WHERE id = 2")
            require_equal(
                connection.sql("SELECT id, payload FROM lake.items ORDER BY id").fetchall(),
                [(1, "one"), (3, "three")],
                "native DuckLake readback",
            )
        finally:
            connection.close()


if __name__ == "__main__":
    main()
