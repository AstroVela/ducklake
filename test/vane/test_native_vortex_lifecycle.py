#!/usr/bin/env python3
"""Check scan ownership without polling Vortex again and close databases in one process."""
from __future__ import annotations

import argparse
import importlib.util
import json
import os
from pathlib import Path
import time

SPEC = importlib.util.spec_from_file_location("native_search", Path(__file__).with_name("test_native_vortex_search.py"))
assert SPEC is not None and SPEC.loader is not None
SEARCH = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SEARCH)
CAT = SEARCH.CAT
BASE = SEARCH.BASE
require = BASE.require
VECTOR = [133, 4, 2, 0, 1, 8, 7, 16]


def delta(native):
    native.sql(
        "CALL ducklake_set_option('lake','data_inlining_row_limit',100,table_name=>'items');"
        "INSERT INTO lake.items SELECT 5000+i,[i::FLOAT,0,0,0,0,0,0,0],'B','F-delta' FROM range(128)t(i);"
        "BEGIN;UPDATE lake.items SET embedding=[700000,0,0,0,0,0,0,0],payload=NULL WHERE id=133;"
        "DELETE FROM lake.items WHERE id=134;"
        "INSERT INTO lake.items VALUES(200000,[133.125,4,2,0,1,8,7,16],'A','T-live');COMMIT;"
    )


def ownership_baseline(native):
    counters = native.call("owners")
    connections = counters["connections"]
    require(
        counters["database_references"] == 2 and len(connections) == 1 and connections[0]["active"] is False,
        f"Unexpected ownership baseline: {counters}",
    )
    return connections[0]["references"]


def owner_probe_publication(native):
    before = native.call("owners")
    context_references = ownership_baseline(native)
    published = native.call("owners-published-context")
    connections = published["connections"]
    require(published["database_references"] == before["database_references"] + 1, "Published control lost its database owner")
    require(len(connections) == 2, "Published control was missing from the connection snapshot")
    foreign = [row for row in connections if row["active"] is None]
    writer = [row for row in connections if row["active"] is False]
    require(len(foreign) == 1 and len(writer) == 1, "Probe inspected a foreign transaction or lost its idle writer")
    require(all("connection_id" not in row for row in connections), "Probe inspected connection IDs before construction completed")
    require(foreign[0]["references"] == 2, "Published control did not retain exactly its local and snapshot owners")
    require(writer[0]["references"] == context_references, "Published control changed writer ownership")
    after = native.call("owners")
    require(after == before, "Published control did not unregister and release its context/database owners")
    return {"before": before, "published": published, "after": after}


def settled(native, context_references):
    deadline = time.monotonic() + 5
    while True:
        counters = native.call("owners")
        connections = counters["connections"]
        if (
            counters["database_references"] == 2
            and len(connections) == 1
            and connections[0]["active"] is False
            and connections[0]["references"] == context_references
        ):
            return counters
        require(time.monotonic() < deadline, f"Query retained database/context/transaction owners: {counters}")
        time.sleep(0.01)


def rss(native):
    for line in Path(f"/proc/{native.process.pid}/status").read_text().splitlines():
        if line.startswith("VmRSS:"):
            return int(line.split()[1])
    raise AssertionError("Native process has no RSS observation")


def baseline(driver, root):
    native = CAT.Native(driver, root / "baseline")
    try:
        delta(native)
        native.sql("SET threads=1;")
        initial = native.call("owners")
        require(initial["database_references"] == 2, "Baseline fixture already held unexpected references")
        observations = []
        for i in range(8):
            native.call("raw", f"baseline-{i}")
            observations.append(native.call("owners"))
        require(
            any(row["database_references"] > 2 for row in observations), "Old archive did not reproduce retained owners"
        )
        native.process.stdin.close()
        native.process.wait(timeout=30)
        native.log.close()
        log = native.root.with_suffix(".session-1.log").read_text()
        require(
            native.process.returncode == 1 and "retained database resources after close" in log,
            "The baseline did not fail the same strict shutdown check",
        )
        return {
            "driver_sha256": BASE.digest(driver),
            "observations": observations,
            "strict_close_exit": native.process.returncode,
        }
    finally:
        if native.process.poll() is None:
            native.process.kill()
            native.process.wait(timeout=5)


def exercise(native, root, cycles, reopens, checks):
    artifacts = root / "artifacts"
    artifacts.mkdir()
    native.call("init")
    native.call("build", "knn", artifacts / "first.json", 0)
    delta(native)
    native.call("raw", "delta")
    rows = native.rows("delta")
    observations = []
    recalls = []
    rss_samples = []
    context_before_prepare = ownership_baseline(native)
    native.sql("PREPARE lifecycle_search AS " + SEARCH.search_sql(VECTOR))
    context_references = ownership_baseline(native)

    def released(label):
        observations.append({"operation": label, **settled(native, context_references)})
        BASE.assert_query_scratch_released(root / "private")

    for i in range(cycles):
        native.sql("SET threads=" + str(1 + i % 2) + ";")
        result = SEARCH.query(native, "EXECUTE lifecycle_search")
        recalls.append(SEARCH.qualify(result, rows, VECTOR))
        released("prepared-search")
        limited = SEARCH.query(native, SEARCH.search_sql(VECTOR) + " LIMIT 1")
        require(limited["rows"] == result["rows"][:1], "Early LIMIT changed ranked output")
        released("search-limit")
        native.sql("SELECT id,payload FROM lake.items LIMIT 1;")
        released("scan-limit")
        rss_samples.append(rss(native))
    checks.append(
        "successful mixed queries, prepared execution and early LIMIT release owners with one/two DuckDB threads"
    )

    reopen_pid = native.process.pid
    direct_reopens = []

    def reopen_after(label):
        native.call("reopen")
        require(native.process.pid == reopen_pid, "Reopen changed the native process")
        direct_reopens.append(label)
        return ownership_baseline(native)

    SEARCH.query(native, "EXECUTE lifecycle_search")
    released("prepared-before-reopen")
    context_references = reopen_after("prepared-search")
    SEARCH.query(native, SEARCH.search_sql(VECTOR) + " LIMIT 1")
    released("search-limit-before-reopen")
    context_references = reopen_after("search-limit")
    native.sql("SELECT id,payload FROM lake.items LIMIT 1;")
    released("scan-limit-before-reopen")
    context_references = reopen_after("scan-limit")

    for operation, stage, expected in [
        ("cancel-search", "after_visibility", "Interrupted"),
        ("cancel-search", "source_pinned", "Interrupted"),
        ("cancel-search", "after_open", "Interrupted"),
        ("cancel-search", "after_ann", "Interrupted"),
        ("cancel-search", "before_fetch", "Interrupted"),
        ("fail-search", "source_pinned", "Injected query lease failure"),
        ("fail-search", "after_open", "Injected query lease failure"),
    ]:
        native.call(operation, stage, expected=expected)
        released(operation + "-" + stage)
        context_references = reopen_after(operation + "-" + stage)
    SEARCH.query(native, SEARCH.search_sql([1, 2]), expected="Query dimension")
    released("dimension-error")
    context_references = reopen_after("dimension-error")
    checks.append(
        "interruption, callback failure and dimension rejection release owners before any subsequent Vortex operation"
    )

    for _ in range(reopens):
        native.call("reopen")
        require(native.process.pid == reopen_pid, "Reopen changed the native process")
        context_references = ownership_baseline(native)
        result = native.call("core-search")
        recalls.append(SEARCH.qualify(result, rows, VECTOR))
        released("core-search-after-reopen")
    checks.append("strict database weak-reference checks pass before reopening another database in the same process")

    native.call("optimizer-init")
    native.call("optimizer-enable", artifacts, 100000, 4194304, 100000, 67108864)
    for _ in range(4):
        native.call("core-search")
        released("optimizer-core-search")
    native.call("optimizer-cancel")
    deadline = time.monotonic() + 10
    while True:
        state = native.call("optimizer-status")[0]
        if not state["enabled"] and not state["running"]:
            break
        require(time.monotonic() < deadline, "Optimizer cancellation did not settle")
        time.sleep(0.02)
    released("optimizer-cancel")
    native.call("reopen")
    context_references = ownership_baseline(native)
    result = native.call("core-search")
    recalls.append(SEARCH.qualify(result, rows, VECTOR))
    released("core-search-after-optimizer-reopen")
    checks.append("the same ownership and close checks pass with an optimizer installed and after its cancellation")
    return {
        "fixture_recalls": recalls,
        "owner_observations": observations,
        "same_process_reopens": reopens + 1 + len(direct_reopens),
        "direct_reopen_endings": direct_reopens,
        "context_references_before_prepare": context_before_prepare,
        "query_cycles": cycles,
        "rss_samples_kib": rss_samples,
        "rss_conditions": "allocator/cache observations; ownership is the hard gate",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--driver", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--baseline-driver", type=Path)
    parser.add_argument("--cycles", type=int, default=20)
    parser.add_argument("--reopens", type=int, default=6)
    args = parser.parse_args()
    require(args.cycles >= 2 and args.reopens >= 2, "Lifecycle qualification requires repeated queries and reopens")
    root = args.output_dir.resolve()
    root.mkdir(parents=True, exist_ok=False)
    scratch = root / "private"
    scratch.mkdir()
    os.environ["TMPDIR"] = str(scratch)
    driver = args.driver.resolve(strict=True)
    native = None
    checks = []
    report = {"status": "running", "driver_sha256": BASE.digest(driver), "vortex_revision": BASE.VORTEX_REVISION}
    started = time.monotonic()
    try:
        if args.baseline_driver:
            report["baseline"] = baseline(args.baseline_driver.resolve(strict=True), root)
            checks.append(
                "same strict driver with the old native archive reproduces retained database owners and fails shutdown"
            )
        native = CAT.Native(driver, root / "native")
        report["owner_probe_publication"] = owner_probe_publication(native)
        checks.append("published-but-unassigned foreign contexts expose only safe shared-owner counts")
        report.update(exercise(native, root, args.cycles, args.reopens, checks))
        native.finish()
        BASE.assert_query_scratch_released(scratch)
        report["status"] = "passed"
    except Exception as error:
        report.update(status="failed", error=str(error))
        raise
    finally:
        if native is not None and native.process.poll() is None:
            native.process.kill()
            native.process.wait(timeout=5)
        report.update(
            seconds=time.monotonic() - started,
            checks=checks,
            native_requests=native.requests if native else 0,
            native_rejections=native.errors if native else 0,
        )
        (root / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
