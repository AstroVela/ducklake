#!/usr/bin/env python3
"""Qualify realtime Vortex retrieval through an installed Vane package."""
from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import sysconfig
import time

SPEC = importlib.util.spec_from_file_location("search_proof", Path(__file__).with_name("test_native_vortex_search.py"))
assert SPEC is not None and SPEC.loader is not None
SEARCH = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SEARCH)
require = SEARCH.require
quote = SEARCH.BASE.quote
VECTOR = [133, 4, 2, 0, 1, 8, 7, 16]


def query(connection, sql):
    result = connection.execute(sql)
    names = [item[0] for item in result.description]
    return [dict(zip(names, row)) for row in result.fetchall()]


def connect(root, create=False):
    import vane

    connection = vane.connect(
        ":memory:",
        config={
            "threads": "2",
            "autoinstall_known_extensions": "false",
            "autoload_known_extensions": "false",
            "extension_directory": str(root / "extensions"),
        },
    )
    connection.execute("LOAD parquet")
    vane.load_installed_extension("ducklake", connection=connection)
    for name in ["vortex", "ducklake"]:
        installed = query(connection, "SELECT loaded FROM duckdb_extensions() WHERE extension_name=" + quote(name))
        require(installed == [{"loaded": True}], f"Installed provider did not load {name}")
    connection.execute(
        "ATTACH "
        + quote("ducklake:" + str(root / "metadata.ducklake"))
        + " AS lake (DATA_PATH "
        + quote(root / "data")
        + ", DATA_INLINING_ROW_LIMIT 1000)"
    )
    if create:
        connection.execute("CREATE TABLE lake.items(id BIGINT,embedding FLOAT[],category VARCHAR,payload VARCHAR)")
        connection.execute(
            "CALL ducklake_set_option('lake','experimental_data_file_format','vortex',table_name=>'items')"
        )
        connection.execute("SELECT * FROM ducklake_initialize_vortex_indexes('lake')")
        connection.execute("SELECT * FROM ducklake_initialize_vortex_optimizer('lake')")
        connection.execute(
            "SELECT * FROM ducklake_create_vortex_index('lake','main','items','knn'," + quote(root / "indexes") + ",8)"
        )
    return connection


def raw(connection):
    return query(connection, "SELECT id,embedding,category,payload FROM lake.items ORDER BY id")


def head(connection):
    row = query(connection, "SELECT revision,reference FROM __ducklake_metadata_lake.ducklake_vortex_indexes_v1")[0]
    return {"revision": row["revision"], "reference": json.loads(row["reference"])}


def status(connection):
    return query(connection, "SELECT * FROM ducklake_vortex_optimizer_status('lake')")[0]


def wait_revision(connection, revision):
    deadline = time.monotonic() + 120
    while True:
        row = status(connection)
        require(row["state"] != "failed", f"Optimizer failed: {row}")
        if row["published_revision"] == revision and not row["running"]:
            return row
        require(time.monotonic() < deadline, f"Timed out waiting for revision {revision}: {row}")
        time.sleep(0.02)


def insert(connection, start, rows):
    connection.execute(
        f"INSERT INTO lake.items SELECT {start}+i,[128+i/16.0,((i*4)%12)::FLOAT,((i*5)%13)::FLOAT,0,1,8,7,16],"
        "CASE WHEN i%4=0 THEN 'B' ELSE 'A' END,CASE WHEN i=0 THEN NULL ELSE '实时向量' END "
        f"FROM range({rows})t(i)"
    )


def signature(segment):
    generation = segment["reference"]["generation"]["generation"]
    directory = Path(segment["root"]) / generation
    return {
        str(path.relative_to(directory)): [hashlib.sha256(path.read_bytes()).hexdigest(), path.stat().st_size]
        for path in directory.rglob("*")
        if path.is_file()
    }


def qualify(connection, label, recalls):
    oracle = raw(connection)
    for category in [None, "A", "B"]:
        result = {"rows": query(connection, SEARCH.search_sql(VECTOR, category=category))}
        recalls.append(SEARCH.qualify(result, oracle, VECTOR, category))
    prepared = query(connection, "EXECUTE nearest")
    recalls.append(SEARCH.qualify({"rows": prepared}, oracle, VECTOR))
    return {"phase": label, "rows": len(oracle), "revision": head(connection)["revision"]}


def crash_writer(root):
    connection = connect(root)
    connection.execute("INSERT INTO lake.items VALUES(900000,[133.000001,4,2,0,1,8,7,16],NULL,'崩溃后可见')")
    expected = query(connection, "SELECT rowid,id,embedding,category,payload FROM lake.items ORDER BY id")
    (root / "committed.json").write_text(json.dumps(expected, ensure_ascii=False))
    wal = root / "metadata.ducklake.wal"
    require(wal.is_file() and wal.stat().st_size > 0, "Crash boundary did not retain a metadata WAL")
    (root / "committed.ready").write_text("ready")
    while True:
        time.sleep(0.1)


def exercise(root, report):
    import vane
    from vane import runners

    site = Path(sysconfig.get_path("purelib")).resolve()
    require(
        Path(vane.__file__).resolve().is_relative_to(site), "Vane must come from this environment's installed wheel"
    )
    require(runners.get_or_infer_runner_type() == "local-fast", "Native Vane runner was not selected")
    report.update(vane_version=importlib.metadata.version("vane-ai"), vane_path=str(Path(vane.__file__).resolve()))
    (root / "indexes").mkdir()
    connection = connect(root, create=True)
    reader = None
    recalls, phases = [], []
    try:
        report["engine"] = query(connection, "PRAGMA version")[0]
        connection.execute("PREPARE nearest AS " + SEARCH.search_sql(VECTOR))
        phases.append(qualify(connection, "empty", recalls))
        connection.execute(
            "SELECT * FROM ducklake_enable_vortex_optimizer('lake','main','items','knn',"
            + quote(root / "indexes")
            + ",8,64,4194304,128,67108864)"
        )
        connection.execute(
            "SELECT * FROM ducklake_set_vortex_write_budget('lake','main','items','knn',1000,4194304,512,67108864)"
        )
        insert(connection, 1000, 16)
        phases.append(qualify(connection, "committed-T", recalls))
        require(not list((root / "data").rglob("*.vortex")), "First T query required data-file generation")
        reader = connection.cursor()
        reader.execute("BEGIN")
        original = query(reader, "SELECT rowid,id,embedding,category,payload FROM lake.items ORDER BY id")
        connection.execute("BEGIN")
        connection.execute(
            "MERGE INTO lake.items AS target USING (VALUES "
            "(1003,[133.001,4,2,0,1,8,7,16]::FLOAT[],'A','事务更新'),"
            "(1020,[133.0005,4,2,0,1,8,7,16]::FLOAT[],'A','事务插入')"
            ") AS source(id,embedding,category,payload) ON target.id=source.id "
            "WHEN MATCHED THEN UPDATE SET embedding=source.embedding,payload=source.payload "
            "WHEN NOT MATCHED THEN INSERT VALUES(source.id,source.embedding,source.category,source.payload)"
        )
        connection.execute(
            "MERGE INTO lake.items AS target USING (VALUES(1002)) AS source(id) ON target.id=source.id "
            "WHEN MATCHED THEN DELETE"
        )
        require(
            query(reader, "SELECT rowid,id,embedding,category,payload FROM lake.items ORDER BY id") == original,
            "Uncommitted mutations changed another reader's snapshot",
        )
        connection.execute("COMMIT")
        require(
            query(connection, "SELECT id,payload FROM lake.items WHERE id IN(1002,1003,1020) ORDER BY id")
            == [{"id": 1003, "payload": "事务更新"}, {"id": 1020, "payload": "事务插入"}],
            "Committed MERGE lost an insert, update or delete",
        )
        phases.append(qualify(connection, "committed-batch", recalls))
        connection.execute("CREATE TABLE expected_flush AS SELECT rowid,id,embedding,category,payload FROM lake.items")
        connection.execute("CALL ducklake_flush_inlined_data('lake',table_name=>'items')")
        phases.append(qualify(connection, "small-F-before-ANN", recalls))
        require(not head(connection)["reference"]["segments"], "Small F fabricated an ANN generation")
        require(
            query(connection, "SELECT rowid,id,embedding,category,payload FROM lake.items ORDER BY id")
            == query(connection, "SELECT * FROM expected_flush ORDER BY id"),
            "Flush changed typed rows or native row IDs",
        )
        insert(connection, 2000, 112)
        wait_revision(connection, 2)
        first = head(connection)["reference"]["segments"][0]
        frozen = signature(first)
        require(bool(frozen), "Background B has no sealed artifacts")
        insert(connection, 10000, 8)
        connection.execute("CALL ducklake_flush_inlined_data('lake',table_name=>'items')")
        connection.execute("INSERT INTO lake.items VALUES(200000,[133.0001,4,2,0,1,8,7,16],'A','B旁实时')")
        phases.append(qualify(connection, "B-plus-F-plus-T", recalls))
        connection.execute("BEGIN")
        connection.execute(
            "UPDATE lake.items SET embedding=[800,4,2,0,1,8,7,16],category='B',payload='索引后更新' WHERE id=1003"
        )
        connection.execute("DELETE FROM lake.items WHERE id=1004")
        connection.execute("INSERT INTO lake.items VALUES(200001,[133.00002,4,2,0,1,8,7,16],'A','索引后插入')")
        connection.execute("COMMIT")
        phases.append(qualify(connection, "mutations-beside-B", recalls))
        insert(connection, 3000, 128)
        wait_revision(connection, 3)
        segments = head(connection)["reference"]["segments"]
        require(
            len(segments) == 2 and segments[0] == first and signature(first) == frozen, "Second B rebuilt the first B"
        )
        connection.execute("INSERT INTO lake.items VALUES(400000,[133.00001,4,2,0,1,8,7,16],NULL,'最终实时')")
        phases.append(qualify(connection, "second-B-plus-T", recalls))
        require(
            query(reader, "SELECT rowid,id,embedding,category,payload FROM lake.items ORDER BY id") == original,
            "Flush/index publication invalidated an old native transaction snapshot",
        )
        reader.execute("COMMIT")
        reader.close()
        reader = None
        connection.execute("SELECT * FROM ducklake_cancel_vortex_optimizer('lake','main','items','knn')")
    finally:
        if reader is not None:
            reader.close()
        connection.close()

    with (root / "crash-writer.log").open("w") as log:
        child = subprocess.Popen(
            [sys.executable, "-I", str(Path(__file__).resolve()), "--crash-writer", str(root)],
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        try:
            deadline = time.monotonic() + 60
            while not (root / "committed.ready").exists():
                require(child.poll() is None, "Crash writer exited before commit; inspect crash-writer.log")
                require(time.monotonic() < deadline, "Crash writer did not reach committed boundary")
                time.sleep(0.02)
            child.kill()
            child.wait(timeout=10)
            require(child.returncode == -9, "Recovery did not exercise SIGKILL")
        finally:
            if child.poll() is None:
                child.kill()
                child.wait(timeout=10)
    connection = connect(root)
    try:
        connection.execute("PREPARE nearest AS " + SEARCH.search_sql(VECTOR))
        require(
            query(connection, "SELECT rowid,id,embedding,category,payload FROM lake.items ORDER BY id")
            == json.loads((root / "committed.json").read_text()),
            "Recovery changed committed row values or native row IDs",
        )
        require(not status(connection)["running"], "Restart implicitly replayed optimizer work")
        phases.append(qualify(connection, "SIGKILL-reopen", recalls))
        require(
            head(connection)["reference"]["segments"][0] == first and signature(first) == frozen, "Recovery changed B1"
        )
    finally:
        connection.close()
    report.update(phases=phases, fixture_recalls=recalls, minimum_recall=min(recalls))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--crash-writer", type=Path)
    args = parser.parse_args()
    require(os.environ.get("VANE_RUNNER") == "local-fast", "Set VANE_RUNNER=local-fast explicitly")
    if args.crash_writer:
        crash_writer(args.crash_writer.resolve())
        return
    require(args.output_dir is not None, "--output-dir is required")
    root = args.output_dir.resolve()
    root.mkdir(mode=0o700, parents=True, exist_ok=False)
    report = {"status": "running"}
    started = time.monotonic()
    try:
        exercise(root, report)
        report["status"] = "passed"
    except Exception as error:
        report.update(status="failed", error=str(error))
        raise
    finally:
        report["seconds"] = time.monotonic() - started
        (root / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
