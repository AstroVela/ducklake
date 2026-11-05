#!/usr/bin/env python3
"""Check the rejected window query against an independent interval oracle."""
from __future__ import annotations

import argparse
import hashlib
import itertools
import json
from pathlib import Path
import random
import sqlite3
import time


REFERENCE = """
SELECT data_file_id, table_id, path, path_is_relative
FROM ducklake_data_file
WHERE {forced} (end_snapshot IS NOT NULL AND NOT EXISTS (
    SELECT snapshot_id FROM ducklake_snapshot
    WHERE snapshot_id >= begin_snapshot AND snapshot_id < end_snapshot
))
"""


def candidate_query():
    # A frozen test reference, excluded from provider artifacts after performance withdrawal.
    return Path(__file__).with_name("expiry_interval_window.sql").read_text()


def cases():
    boundaries = [0, 1, 2, 3, 4, 7, 8]
    intervals = list(itertools.product(boundaries + [None], repeat=2))
    files = [(i, 1 + i % 3, begin, end) for i, (begin, end) in enumerate(intervals)]
    # All retained subsets cover empty catalogs, gaps, adjacent/equal boundaries and reversed/NULL intervals.
    for mask in range(32):
        snapshots = [s for i, s in enumerate([0, 1, 2, 4, 7]) if mask & (1 << i)]
        for forced in [[], [1], [1, 3], [99]]:
            yield snapshots, files, forced
    rng = random.Random(76077)
    for _ in range(128):
        snapshots = sorted(rng.sample(range(100), rng.randrange(30)))
        files = [
            (i, rng.randrange(1, 6), rng.randrange(100), None if i % 9 == 0 else rng.randrange(100)) for i in range(128)
        ]
        yield snapshots, files, rng.sample(range(1, 6), rng.randrange(4))
    # A long retained tail beyond every ended file is irrelevant to every predecessor.
    files = [(0, 1, 3, 7), (1, 2, 5, 7), (2, 3, 8, None)]
    yield list(range(4096)), files, []
    yield list(range(7, 4096)), files, []
    yield [s for s in range(4096) if s not in (5, 6)], files, [3]
    yield list(range(4096)), [(0, 1, 3, None), (1, 2, 5, None)], [2]


def oracle(snapshots, files, forced):
    return sorted(
        (file_id, table, "source-" + str(file_id), True)
        for file_id, table, begin, end in files
        if table in forced or (end is not None and not any(begin is not None and begin <= s < end for s in snapshots))
    )


def exercise(connection, candidate):
    connection.execute("CREATE TABLE ducklake_snapshot(snapshot_id BIGINT)")
    connection.execute(
        "CREATE TABLE ducklake_data_file(data_file_id BIGINT,table_id BIGINT,begin_snapshot BIGINT,"
        "end_snapshot BIGINT,path VARCHAR,path_is_relative BOOLEAN)"
    )
    checked = 0
    for snapshots, files, forced in cases():
        connection.execute("DELETE FROM ducklake_snapshot")
        connection.execute("DELETE FROM ducklake_data_file")
        if snapshots:
            connection.executemany("INSERT INTO ducklake_snapshot VALUES(?)", [(s,) for s in snapshots])
        connection.executemany(
            "INSERT INTO ducklake_data_file VALUES(?,?,?,?,?,?)",
            [(i, table, begin, end, "source-" + str(i), True) for i, table, begin, end in files],
        )
        predicate = "table_id IN (" + ",".join(map(str, forced)) + ") OR" if forced else ""
        expected = oracle(snapshots, files, forced)
        original = sorted(connection.execute(REFERENCE.format(forced=predicate)).fetchall())
        actual = sorted(connection.execute(candidate.format(forced=predicate)).fetchall())
        assert original == actual == expected, (checked, snapshots, forced, expected, original, actual)
        checked += 1
    return checked


def run(output):
    import vane

    output.mkdir(mode=0o700)
    query = candidate_query()
    (output / "candidate.sql").write_text(query)
    report = {"status": "running", "candidate_sha256": hashlib.sha256(query.encode()).hexdigest(), "engines": []}
    started = time.monotonic()
    try:
        for name, connection in [
            ("native-vane", vane.connect(":memory:", config={"threads": "2"})),
            ("sqlite", sqlite3.connect(":memory:")),
        ]:
            try:
                count = exercise(connection, query)
                report["engines"].append({"name": name, "cases": count, "status": "passed"})
            finally:
                connection.close()
        report["status"] = "passed"
    finally:
        report["seconds"] = time.monotonic() - started
        (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, required=True)
    run(parser.parse_args().output_dir.resolve())
