"""Keep committed Vortex files through orphan cleanup for each local path form."""

from __future__ import annotations

import argparse
import json
import subprocess
import tempfile
from functools import partial
from pathlib import Path


def sql_string(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def execute(
    duckdb: Path, root: Path, prefix: str, sql: str, result: bool = True
) -> object:
    completed = subprocess.run(
        [str(duckdb), "-batch", "-bail", "-json", "-c", prefix + sql],
        cwd=root,
        capture_output=True,
        text=True,
        timeout=60,
        check=True,
    )
    return json.loads(completed.stdout) if result else None


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duckdb", required=True, type=Path)
    args = parser.parse_args()
    duckdb = args.duckdb.resolve(strict=True)
    with tempfile.TemporaryDirectory(prefix="ducklake-vortex-cleanup-") as directory:
        for form in ("absolute", "relative", "dot-relative"):
            root = Path(directory) / form
            root.mkdir()
            data_path = str(root / "data") if form == "absolute" else "data"
            if form == "dot-relative":
                data_path = "./data"
            prefix = (
                "SET autoinstall_known_extensions=false;"
                "SET autoload_known_extensions=false;"
                "LOAD parquet; LOAD ducklake; LOAD vortex;"
                f"ATTACH {sql_string('ducklake:' + str(root / 'metadata.ducklake'))} AS lake "
                f"(DATA_PATH {sql_string(data_path)}, DATA_INLINING_ROW_LIMIT 0);"
            )

            run = partial(execute, duckdb, root, prefix)
            run(
                "CREATE TABLE lake.items(id BIGINT, embedding FLOAT[]);"
                "CALL ducklake_set_option('lake', 'experimental_data_file_format', 'vortex', table_name => 'items');"
                "INSERT INTO lake.items VALUES (1, [1, 2, 3]), (2, [4, 5, 6]);",
                result=False,
            )
            paths = run(
                "SELECT path, path_is_relative FROM __ducklake_metadata_lake.ducklake_data_file;"
            )
            assert len(paths) == 1, (form, paths)
            assert paths[0]["path_is_relative"] and not paths[0]["path"].startswith(
                ("/", "\\")
            ), (form, paths)
            committed = root / "data" / "main" / "items" / paths[0]["path"]
            assert committed.is_file(), (form, committed)
            assert run(
                "SELECT count(*) AS n FROM ducklake_delete_orphaned_files('lake', dry_run => true, cleanup_all => true);"
            ) == [{"n": 0}], form

            orphan = root / "data" / "main" / "items" / "orphan.vortex"
            run(
                f"COPY (SELECT 99::BIGINT AS id) TO {sql_string(orphan)} (FORMAT vortex);",
                result=False,
            )
            assert run(
                "SELECT count(*) AS n FROM ducklake_delete_orphaned_files('lake', dry_run => true, cleanup_all => true);"
            ) == [{"n": 1}], form
            assert committed.is_file() and orphan.is_file(), form
            assert run(
                "SELECT count(*) AS n FROM ducklake_delete_orphaned_files('lake', cleanup_all => true);"
            ) == [{"n": 1}], form
            assert committed.is_file() and not orphan.exists(), form
            assert run(
                "SELECT id, embedding[1] AS x, embedding[2] AS y, embedding[3] AS z FROM lake.items ORDER BY id;"
            ) == [
                {"id": 1, "x": 1, "y": 2, "z": 3},
                {"id": 2, "x": 4, "y": 5, "z": 6},
            ], form
            print(f"PASS: {form} cleanup preserves committed Vortex data", flush=True)


if __name__ == "__main__":
    main()
