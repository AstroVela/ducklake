#!/usr/bin/env python3
"""Exercise the compiled snapshot lookup against SQL and a separate interval oracle."""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import itertools
import json
from pathlib import Path
import sqlite3
import subprocess

SPEC = importlib.util.spec_from_file_location(
    "expiry_intervals", Path(__file__).with_name("test_vane_vortex_expiry_interval.py")
)
INTERVALS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(INTERVALS)


def cases():
    yield from INTERVALS.cases()
    limits = [-(2**63), -1, 0, 2**63 - 2, 2**63 - 1]
    files = [(i, 1 + i % 2, begin, end) for i, (begin, end) in enumerate(itertools.product(limits + [None], repeat=2))]
    for mask in range(1 << len(limits)):
        snapshots = [s for i, s in enumerate(limits) if mask & (1 << i)]
        for forced in ([], [1], [1, 2]):
            yield snapshots, files, forced


def sql_oracle(connection, inputs):
    connection.execute("CREATE TABLE ducklake_snapshot(snapshot_id BIGINT)")
    connection.execute(
        "CREATE TABLE ducklake_data_file(data_file_id BIGINT, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, path VARCHAR, path_is_relative BOOLEAN)"
    )
    results = []
    for snapshots, files, forced in inputs:
        connection.execute("DELETE FROM ducklake_snapshot")
        connection.execute("DELETE FROM ducklake_data_file")
        if snapshots:
            connection.executemany("INSERT INTO ducklake_snapshot VALUES(?)", [(s,) for s in snapshots])
        connection.executemany(
            "INSERT INTO ducklake_data_file VALUES(?,?,?,?,?,?)",
            [(i, table, begin, end, "source-" + str(i), True) for i, table, begin, end in files],
        )
        predicate = "table_id IN (" + ",".join(map(str, forced)) + ") OR" if forced else ""
        actual = sorted(connection.execute(INTERVALS.REFERENCE.format(forced=predicate)).fetchall())
        expected = INTERVALS.oracle(snapshots, files, forced)
        assert actual == expected, (snapshots, files, forced, actual, expected)
        results.append([row[0] for row in expected])
    return results


def run(args):
    import vane

    root = args.output_dir.resolve()
    root.mkdir(mode=0o700)
    driver_sha256 = hashlib.sha256(args.driver.read_bytes()).hexdigest()
    inputs = list(cases())
    expected = [[row[0] for row in INTERVALS.oracle(*case)] for case in inputs]
    path = root / "intervals.txt"
    with path.open("w") as output:
        for number, (snapshots, files, forced) in enumerate(inputs):
            output.write(f"{number} {len(snapshots)} {len(files)}\n")
            output.write(" ".join(map(str, snapshots)) + "\n")
            for file_id, table, begin, end in files:
                output.write(
                    f"{file_id} {int(begin is None)} {begin if begin is not None else 0} {int(end is None)} {end if end is not None else 0} {int(table in forced)}\n"
                )
    result = root / "actual.txt"
    subprocess.run([str(args.driver.resolve()), str(path), str(result)], check=True, timeout=90)
    lines = [list(map(int, line.split())) for line in result.read_text().splitlines()]
    assert len(lines) == len(inputs)
    for number, (line, wanted) in enumerate(zip(lines, expected)):
        assert line == [number] + wanted, (number, line, wanted)
    engines = []
    for name, connection in [
        ("native-vane", vane.connect(":memory:", config={"threads": "2"})),
        ("sqlite", sqlite3.connect(":memory:")),
    ]:
        try:
            assert sql_oracle(connection, inputs) == expected
            engines.append({"engine": name, "cases": len(inputs), "status": "passed"})
        finally:
            connection.close()
    assert hashlib.sha256(args.driver.read_bytes()).hexdigest() == driver_sha256
    report = {
        "status": "passed",
        "compiled_helper_cases": len(inputs),
        "engines": engines,
        "driver_sha256": driver_sha256,
        "input_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
    }
    (root / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--driver", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    run(parser.parse_args())
