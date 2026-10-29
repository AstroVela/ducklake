#!/usr/bin/env python3
"""Qualify transactional write quotas under concurrent commits and stalled indexing."""
from __future__ import annotations

import argparse
import importlib.util
import json
import os
from pathlib import Path
import time

SPEC = importlib.util.spec_from_file_location(
    "native_optimizer", Path(__file__).with_name("test_native_vortex_optimizer.py")
)
assert SPEC is not None and SPEC.loader is not None
OPT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(OPT)
CAT, BASE, SEARCH = OPT.CAT, OPT.BASE, OPT.SEARCH
require = BASE.require
VECTOR = OPT.BUILD.VECTOR


def sql(native, text, expected=None):
    path = native.root.parent / f"budget-{native.root.name}-{native.requests+1}.sql"
    path.write_text(text)
    return native.call("sql", path, expected=expected)


def count(native, expected):
    sql(
        native,
        f"SELECT CASE WHEN count(*)={expected} THEN true ELSE error('Wrong committed count') END FROM lake.items",
    )


def budget(native, t_rows, t_bytes, f_rows, f_bytes=67108864, expected=None):
    return sql(
        native,
        "SELECT * FROM ducklake_set_vortex_write_budget('lake','main','items','knn',"
        f"{t_rows},{t_bytes},{f_rows},{f_bytes})",
        expected,
    )


def insert(start, rows):
    return (
        f"INSERT INTO lake.items SELECT {start}+i,[({start}+i)::FLOAT,4::FLOAT,2::FLOAT,"
        "0::FLOAT,1::FLOAT,8::FLOAT,7::FLOAT,16::FLOAT],'A','预算-row' "
        f"FROM range({rows})t(i)"
    )


def owners(native):
    deadline = time.monotonic() + 5
    while True:
        row = native.call("owners")
        if row["database_references"] == 2 and len(row["connections"]) == 1 and not row["connections"][0]["active"]:
            return
        require(time.monotonic() < deadline, f"Write admission retained owners: {row}")
        time.sleep(0.01)


def setup(native, root, empty=False, dimension=8):
    if empty:
        sql(
            native,
            "DROP TABLE lake.items;CREATE TABLE lake.items(id BIGINT,embedding FLOAT[],category VARCHAR,payload VARCHAR);"
            "CALL ducklake_set_option('lake','experimental_data_file_format','vortex',table_name=>'items')",
        )
    native.call("init")
    native.call("optimizer-init")
    artifacts = root / "artifacts"
    artifacts.mkdir()
    if not empty:
        native.call("build", "knn", artifacts / "base.json", 0)
    sql(
        native,
        "CALL ducklake_set_option('lake','data_inlining_row_limit',1000,table_name=>'items');"
        "SELECT * FROM ducklake_enable_vortex_optimizer('lake','main','items','knn',"
        f"{BASE.quote(artifacts)},{dimension},128,4194304,128,67108864)",
    )
    return artifacts


def transactional(native, root, checks):
    setup(native, root, empty=True)
    native.call("optimizer-cancel")
    budget(native, 4, 1048576, 512)
    sql(native, insert(0, 3))
    path = root / "race.sql"
    path.write_text(insert(1000, 1).replace("1000+i", "1000+{WRITER}+i"))
    raced = native.call("budget-race", 4, path)
    require(sum(not row["error"] for row in raced) == 1, f"Concurrent commits overshot or lost capacity: {raced}")
    require(
        all(not row["error"] or "RESOURCE_EXHAUSTED" in row["error"] for row in raced),
        f"Concurrent commit failed outside resource admission: {raced}",
    )
    count(native, 4)
    budget(native, 3, 1048576, 512, expected="inline rows")
    require(OPT.status(native)["max_inline_rows"] == 4, "Rejected configuration changed the active budget")
    sql(native, "BEGIN;" + insert(2000, 1) + ";ROLLBACK")
    sql(
        native,
        "CREATE TABLE lake.audit(id BIGINT);BEGIN;INSERT INTO lake.audit VALUES(1);" + insert(3000, 1) + ";COMMIT",
        "inline rows",
    )
    sql(
        native,
        "SELECT CASE WHEN count(*)=0 THEN true ELSE error('Partially committed transaction') END FROM lake.audit",
    )
    sql(native, "UPDATE lake.items SET payload=repeat('x',1048576) WHERE id=0", "inline bytes")
    sql(native, "UPDATE lake.items SET payload='更新-成功' WHERE id=0")
    interrupted = root / "interrupted.sql"
    interrupted.write_text(insert(4000, 1))
    native.call("budget-interrupt", interrupted, expected="Interrupted")
    sql(native, "DELETE FROM lake.items WHERE id=0;" + insert(5000, 1))
    count(native, 4)
    owners(native)
    checks.append(
        "four stale-snapshot transactions race for one remaining slot: one commits, three fail resource admission"
    )
    checks.append(
        "inline update accounting, byte rejection, multi-table atomicity, rollback and interruption preserve committed capacity"
    )

    native.call("reopen")
    sql(native, insert(6000, 1), "inline rows")
    require(OPT.status(native)["max_inline_rows"] == 4, "Same-process reopen lost durable limits")
    sql(native, "BEGIN;" + insert(7000, 1))
    native.process.kill()
    native.process.wait(timeout=5)
    native.log.close()
    require(native.process.returncode == -9, "Budget fixture was not stopped with SIGKILL")
    native.start("resume")
    count(native, 4)
    sql(native, insert(8000, 1), "inline rows")
    sql(native, "DELETE FROM lake.items WHERE id=5000;" + insert(9000, 1))
    count(native, 4)
    owners(native)
    checks.append(
        "budget and committed occupancy survive same-process reopen and SIGKILL; uncommitted rows consume no recovered capacity"
    )
    budget(native, 4, 4194304, 512)
    for version in range(21):
        sql(native, f"UPDATE lake.items SET payload=repeat('x',3145728)||'{version}' WHERE id=9000")
    sql(native, "UPDATE lake.items SET payload=repeat('x',3145728)||'overflow' WHERE id=9000", "pending inline bytes")
    sql(native, "UPDATE lake.items SET payload='small' WHERE id=9000")
    sql(native, "UPDATE lake.items SET payload=repeat('x',3145728) WHERE id=9000", "pending inline bytes")
    count(native, 4)
    owners(native)
    checks.append(
        "ended inline versions remain charged to the materialization byte ceiling even after the visible row shrinks"
    )
    return {"concurrent_outcomes": raced, "committed_rows": 4}


def stalled(native, root, checks):
    setup(native, root)
    budget(native, 256, 1048576, 384)
    native.call("optimizer-resume", "build_started", "pause")
    sql(native, insert(10000, 256))
    native.call("optimizer-wait")
    sql(
        native,
        "CALL ducklake_set_option('lake','data_inlining_row_limit',100,table_name=>'items');" + insert(20000, 128),
    )
    before = {str(p) for p in (native.root / "data").rglob("*.vortex")}
    sql(native, insert(30000, 128), "unindexed rows")
    require(
        {str(p) for p in (native.root / "data").rglob("*.vortex")} == before,
        "Rejected file commit left a physical artifact",
    )
    sql(
        native,
        "BEGIN;UPDATE lake.items SET embedding=[900000,0,0,0,0,0,0,0],payload=NULL WHERE id=133;"
        "DELETE FROM lake.items WHERE id=134;"
        "INSERT INTO lake.items VALUES(200000,[133.0625,4,2,0,1,8,7,16],'A','实时-T');COMMIT",
    )
    count(native, 4480)
    native.call("raw", "stalled")
    result = SEARCH.query(native, SEARCH.search_sql(VECTOR))
    recalls = [SEARCH.qualify(result, native.rows("stalled"), VECTOR)]
    core = native.call("core-search")
    recalls.append(SEARCH.qualify(core, native.rows("stalled"), VECTOR))
    require(core["rows"] == result["rows"], "Budgeted SQL and core search disagree")
    require(
        core["diagnostics"]["routes"]["indexed_files"]
        and core["diagnostics"]["routes"]["scan_files"]
        and core["diagnostics"]["routes"]["inline_rows"],
        "Stalled query omitted a B/F/T route",
    )
    native.call("optimizer-cancel")
    native.call("optimizer-release")
    OPT.wait(native, lambda row: row["state"] == "cancelled" and not row["running"], "cancelled build")
    sql(native, insert(30000, 128), "unindexed rows")
    native.call("reopen")
    sql(native, insert(30000, 128), "unindexed rows")
    native.call("optimizer-resume")
    OPT.wait(
        native,
        lambda row: row["state"] == "published" and row["published_revision"] == 2,
        "promotion releasing F capacity",
    )
    sql(native, insert(40000, 128))
    count(native, 4608)
    native.call("raw", "recovered")
    recalls.append(SEARCH.qualify(SEARCH.query(native, SEARCH.search_sql(VECTOR)), native.rows("recovered"), VECTOR))
    native.call("optimizer-cancel")
    OPT.wait(native, lambda row: not row["running"], "final cancellation")
    owners(native)
    checks.append(
        "a paused native build bounds F growth; acknowledged B/F/T remains queryable with current update/delete visibility"
    )
    checks.append("cancel/reopen cannot disable write quotas; successful F-to-B publication restores write capacity")
    return {"fixture_recalls": recalls, "rows_after_recovery": 4608, "vortex_files_at_rejection": len(before)}


def bytes_and_vectors(native, root, checks):
    setup(native, root)
    native.call("optimizer-cancel")
    budget(native, 8, 1048576, 512, 1)
    sql(native, "CALL ducklake_set_option('lake','data_inlining_row_limit',100,table_name=>'items')")
    before = {str(p) for p in (native.root / "data").rglob("*.vortex")}
    sql(native, insert(10000, 128), "unindexed bytes")
    require({str(p) for p in (native.root / "data").rglob("*.vortex")} == before, "Byte rejection retained a file")
    count(native, 4096)
    owners(native)
    checks.append("encoded F byte limits reject file commits without deleting B or leaving rejected artifacts")


def wide(native, root, checks):
    setup(native, root, empty=True, dimension=4096)
    native.call("optimizer-cancel")
    budget(native, 100000, 4194304, 100000)
    sql(native, "CALL ducklake_set_option('lake','data_inlining_row_limit',0,table_name=>'items')")
    for batch in range(32):
        sql(
            native,
            "INSERT INTO lake.items SELECT " + str(batch * 128) + "+i,"
            "list_resize([]::FLOAT[],4096,0::FLOAT),'A','wide' FROM range(128)t(i)",
        )
    count(native, 4096)
    sql(
        native,
        "INSERT INTO lake.items VALUES(4096,list_resize([]::FLOAT[],4096,0::FLOAT),'A','wide')",
        "physical and inline rows",
    )
    count(native, 4096)
    owners(native)
    checks.append("the 64 MiB vector ceiling rejects 4,097 dimension-4,096 rows even below the configured row quota")


def history(native, root, checks):
    setup(native, root, empty=True)
    native.call("optimizer-cancel")
    budget(native, 4, 1048576, 100000)
    sql(native, insert(0, 99999))
    sql(native, insert(200000, 1))
    count(native, 100000)
    sql(native, "UPDATE lake.items SET payload='new-version' WHERE id=200000", "physical and inline rows")
    count(native, 100000)
    owners(native)
    checks.append(
        "retained inline history reserves physical build capacity: replacing one visible T row at the ceiling is rejected"
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--driver", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()
    root = args.output_dir.resolve()
    root.mkdir(parents=True, exist_ok=False)
    private = root / "private"
    private.mkdir()
    os.environ["TMPDIR"] = str(private)
    driver = args.driver.resolve(strict=True)
    report = {
        "status": "running",
        "driver_sha256": BASE.digest(driver),
        "vortex_revision": BASE.VORTEX_REVISION,
        "checks": [],
        "cases": {},
    }
    started = time.monotonic()
    native = None
    try:
        for name, case in [
            ("transactional", transactional),
            ("stalled", stalled),
            ("bytes", bytes_and_vectors),
            ("wide", wide),
            ("history", history),
        ]:
            case_root = root / name
            case_root.mkdir()
            native = CAT.Native(driver, case_root / "native")
            report["cases"][name] = case(native, case_root, report["checks"])
            native.finish()
            require(not list(private.iterdir()), f"{name} retained private leases")
        report["status"] = "passed"
    except Exception as error:
        report.update(status="failed", error=str(error))
        raise
    finally:
        if native is not None and native.process.poll() is None:
            native.process.kill()
            native.process.wait(timeout=5)
        report["seconds"] = time.monotonic() - started
        (root / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
