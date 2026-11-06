#!/usr/bin/env python3
"""Qualify reference ownership and native metadata checkpoint/recovery without timing gates."""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import time


def load(filename):
    spec = importlib.util.spec_from_file_location("lifecycle_epochs", Path(__file__).with_name(filename))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


EPOCH = load("test_vane_vortex_request_epochs.py")
SCRATCH = load("vortex_query_scratch_model.py")
KEYED, WRITE, BASE, PIN = EPOCH.KEYED, EPOCH.WRITE, EPOCH.BASE, EPOCH.PIN
require, query, quote = BASE.require, BASE.query, BASE.quote
META = "__ducklake_metadata_lake"
SCRIPT = Path(__file__).resolve()
METADATA_TABLES = (
    "ducklake_vortex_builds_v1",
    "ducklake_vortex_generation_refs_v1",
    "ducklake_vortex_generations_v1",
    "ducklake_vortex_indexes_v1",
    "ducklake_vortex_keyed_tables_v1",
    "ducklake_vortex_maintenance_v1",
    "ducklake_vortex_optimizer_v2",
    "ducklake_vortex_request_epochs_v1",
    "ducklake_vortex_rewrites_v1",
    "ducklake_vortex_source_deletions_v1",
    "ducklake_vortex_source_retention_v1",
    "ducklake_vortex_write_budgets_v1",
    "ducklake_vortex_write_requests_v2",
)


def rows(connection, sql):
    return [list(row) for row in connection.execute(sql).fetchall()]


def snapshot(connection):
    connection.execute("BEGIN")
    try:
        result = {
            "metadata": {
                name: rows(connection, f"SELECT * FROM {META}.{name} ORDER BY ALL") for name in METADATA_TABLES
            },
            "snapshots": rows(
                connection, f"SELECT snapshot_id,schema_version FROM {META}.ducklake_snapshot ORDER BY ALL"
            ),
            "rows": rows(connection, "SELECT id,embedding,category,payload FROM lake.items ORDER BY id"),
        }
        connection.execute("COMMIT")
        return result
    except BaseException:
        connection.execute("ROLLBACK")
        raise


def checkpoint(connection, root):
    databases = query(connection, "SELECT database_name,path,type FROM duckdb_databases() WHERE database_name='lake'")
    require(
        len(databases) == 1
        and databases[0]["type"] == "ducklake"
        and Path(databases[0]["path"]).resolve() == root / "metadata.ducklake",
        "Checkpoint target is not the attached native metadata file",
    )
    connection.execute("CHECKPOINT " + META)


def files(root):
    selected = [*root.joinpath("data").rglob("*.vortex")]
    selected += [p for p in root.joinpath("indexes").rglob("*") if p.is_file()]
    selected += [p for p in root.joinpath("private").rglob("*") if p.is_file()]
    return {
        str(p.relative_to(root)): {"bytes": p.stat().st_size, "sha256": hashlib.sha256(p.read_bytes()).hexdigest()}
        for p in sorted(selected)
    }


def wal_bytes(root):
    path = root / "metadata.ducklake.wal"
    return path.stat().st_size if path.exists() else 0


def model(extra=False):
    result = [[i, [float(i), 0.0], "A", "seed-" + str(i)] for i in range(256) if i not in (0, 1)]
    result += [[0, [0.125, 0.0], "A", "更新"], [10000, [0.25, 0.0], None, None]]
    if extra:
        result.append([12345, [12345.0, 0.0], "A", "after-checkpoint"])
    return sorted(result, key=lambda row: row[0])


def check_query(connection, expected):
    for name in ("knn", "shadow"):
        actual = rows(
            connection,
            "SELECT id,embedding,category,payload FROM ducklake_vortex_search('lake','main','items',"
            + quote(name)
            + ",[0,0]::FLOAT[],3)",
        )
        nearest = sorted(expected, key=lambda row: (sum(x * x for x in row[1]), row[0]))[:3]
        require(actual == nearest, "Named query lost its current typed top-k after reference/WAL cleanup")


def recover(root, extra):
    expected = json.loads((root / "expected.json").read_text())
    sealed = json.loads((root / "files.json").read_text())
    require(expected["rows"] == model(extra), "Native crash boundary did not represent the declared committed state")
    c = BASE.connect(root)
    try:
        require(
            snapshot(c) == expected, "Recovery changed typed rows, snapshots, receipts, policies or index ownership"
        )
        require(files(root) == sealed, "Recovery changed caller files, canonical generations or source identities")
        require(not BASE.status(c)["running"], "Reopen implicitly resumed background optimization")
        require(not (root / "indexes/seed.json").exists(), "Recovery recreated a caller-deleted reference")
        require((root / "indexes/retained.json").is_file(), "A stale caller reference was collected implicitly")
        require(
            not query(c, "SELECT * FROM ducklake_vortex_build_status('lake')"),
            "Completed build ownership was resurrected",
        )
        generations = query(c, "SELECT * FROM ducklake_vortex_generation_status('lake')")
        require(
            len(generations) == 1 and generations[0]["state"] == "active" and generations[0]["reference_count"] == 2,
            "Shared current generation ownership was not preserved",
        )
        check_query(c, expected["rows"])
        current = EPOCH.status(c)
        require(
            current["request_epoch"] == 2 and current["retained_requests"] == (2 if extra else 1),
            "Epoch fence was lost",
        )
        last = EPOCH.advance(c, 1)
        require(
            last["request_epoch"] == 2 and last["expired_requests"] == 2, "Last advance result changed after checkpoint"
        )
        prior = KEYED.mutate(c, "window", [], [-999999], epoch=2)
        require(KEYED.mutate(c, "window", [], [-999999], epoch=2) == prior, "Current receipt replay changed")
        WRITE.rejected(lambda: KEYED.mutate(c, "unseen-expired", [], [-999999], epoch=1), "EXPIRED_REQUEST")
        require(snapshot(c) == expected, "Read/retry/expired-request checks changed committed state")

        WRITE.rejected(
            lambda: c.execute(
                "SELECT * FROM ducklake_build_vortex_index('lake','main','items','knn',"
                + quote(root / "indexes/current.json")
                + ",2,3)"
            ),
            "Build reference must be new",
        )
        require(snapshot(c) == expected and files(root) == sealed, "Rejected output-path reuse changed ownership")
        before = wal_bytes(root)
        checkpoint(c, root)
        require(wal_bytes(root) == 0, "Quiescent native checkpoint left an old WAL")
        require(snapshot(c) == expected and files(root) == sealed, "Checkpoint changed logical state or managed files")

        # The same post-checkpoint request either resolves its committed receipt
        # or commits once after an interrupted/unsubmitted attempt.
        appended = [{"id": 12345, "embedding": [12345.0, 0.0], "category": "A", "payload": "after-checkpoint"}]
        receipt = KEYED.mutate(c, "after-checkpoint", appended, epoch=2)
        require(KEYED.mutate(c, "after-checkpoint", appended, epoch=2) == receipt, "Post-checkpoint request duplicated")
        require(
            rows(c, "SELECT id,embedding,category,payload FROM lake.items ORDER BY id") == model(True),
            "Retry reapplied wrong versions",
        )
        require(wal_bytes(root) > 0 or extra, "A new committed write did not create a new metadata WAL")
        check_query(c, model(True))
        final = snapshot(c)
        checkpoint(c, root)
        require(wal_bytes(root) == 0 and snapshot(c) == final, "Second checkpoint lost the newer receipt")
        require(files(root) == sealed, "Native metadata checkpoint flushed or reclaimed Vortex inputs")
        require(PIN.thread_environment() == json.loads((root / "threads.json").read_text()), "Thread settings changed")
        return {"status": "passed", "wal_before_recovery_checkpoint": before, "post_checkpoint_receipt": receipt}
    finally:
        c.close()


def run_recovery(case, extra):
    with (case / "recovery.log").open("w") as log:
        result = subprocess.run(
            [sys.executable, "-I", str(SCRIPT), "--recover", str(case), "--extra", str(int(extra))],
            stdout=log,
            stderr=subprocess.STDOUT,
            timeout=120,
        )
    require(result.returncode == 0, "Installed lifecycle recovery failed: " + str(case / "recovery.log"))


def prepare_recovery(case, threads):
    (case / "files.json").write_text(json.dumps(files(case), indent=2) + "\n")
    (case / "threads.json").write_text(json.dumps(threads, indent=2) + "\n")


def native_environment(case):
    private = case / "private"
    private.mkdir(mode=0o700)
    return dict(os.environ, TMPDIR=str(private))


def exercise(driver, root, report):
    for mode in ("ok", "gc-cancel", "gc-fail", "wal-guards"):
        case = root / mode
        case.mkdir(mode=0o700)
        env = native_environment(case)
        with (case / "driver.log").open("w") as log:
            result = subprocess.run(
                [str(driver), str(case), "lifecycle-" + mode],
                env=env,
                stdout=log,
                stderr=subprocess.STDOUT,
                timeout=120,
            )
        require(result.returncode == 0, "Native lifecycle case failed: " + str(case / "driver.log"))
        require((case / "checkpoint").read_text() == "complete", "Lifecycle fixture did not finish its old-view checks")
        SCRATCH.assert_released(case / "private")
        prepare_recovery(case, report["thread_environment"])
        run_recovery(case, False)
        report["native_cases"].append(mode)
        print("native and reopen", mode, "passed", flush=True)

    crashes = (
        ("pause-before-checkpoint", "before-checkpoint"),
        ("pause-during-checkpoint", "during-checkpoint"),
        ("pause-after-checkpoint", "after-checkpoint"),
        ("fault-BEFORE_HEADER", "BEFORE_HEADER"),
        ("fault-BEFORE_TRUNCATE", "BEFORE_TRUNCATE"),
        ("fault-BEFORE_WAL_FINISH", "BEFORE_WAL_FINISH"),
        ("pause-write-before_commit", "write-before_commit"),
        ("pause-write-after_commit", "write-after_commit"),
    )
    for mode, stage in crashes:
        case = root / mode
        case.mkdir(mode=0o700)
        env = native_environment(case)
        with (case / "driver.log").open("w") as log:
            process = subprocess.Popen(
                [str(driver), str(case), "lifecycle-" + mode], env=env, stdout=log, stderr=subprocess.STDOUT
            )
            try:
                deadline = time.monotonic() + 120
                while not (case / "checkpoint").is_file():
                    require(
                        process.poll() is None and time.monotonic() < deadline,
                        "Lifecycle crash boundary was not reached",
                    )
                    time.sleep(0.01)
                require((case / "checkpoint").read_text() == stage, "Wrong lifecycle crash boundary")
                if stage == "during-checkpoint":
                    before = int((case / "wal-before").read_text())
                    while wal_bytes(case) <= before:
                        require(
                            process.poll() is None and time.monotonic() < deadline,
                            "Native checkpoint did not flush its WAL marker",
                        )
                        time.sleep(0.01)
                    report["inflight_checkpoint"] = {"wal_before": before, "wal_after_marker": wal_bytes(case)}
                if stage in (
                    "before-checkpoint",
                    "BEFORE_HEADER",
                    "BEFORE_TRUNCATE",
                    "BEFORE_WAL_FINISH",
                    "write-after_commit",
                ):
                    require(wal_bytes(case) > 0, "Crash recovery did not exercise a native metadata WAL")
                if stage in ("after-checkpoint", "write-before_commit"):
                    require(wal_bytes(case) == 0, "Checkpoint did not finish before the declared boundary")
                prepare_recovery(case, report["thread_environment"])
                require(
                    any(key.startswith("private/") and "/payload/" in key for key in files(case)),
                    "Crash fixture did not retain its private ReadView",
                )
                process.kill()
                require(process.wait(timeout=20) == -9, "Lifecycle crash did not use SIGKILL")
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=20)
        run_recovery(case, stage == "write-after_commit")
        report["crash_cases"].append(stage)
        report["retained_private_files"][stage] = sum(key.startswith("private/") for key in files(case))
        print("SIGKILL and installed recovery", stage, "passed", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--driver", type=Path)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--cpus", type=PIN.parse_cpus)
    parser.add_argument("--recover", type=Path)
    parser.add_argument("--extra", type=int, choices=(0, 1))
    args = parser.parse_args()
    if args.recover:
        print(json.dumps(recover(args.recover.resolve(), bool(args.extra)), indent=2))
        return
    require(
        args.driver is not None and args.output_dir is not None and args.cpus is not None,
        "Require driver, output and CPU set",
    )
    root = args.output_dir.resolve()
    root.mkdir(mode=0o700)
    private = root / "private"
    private.mkdir(mode=0o700)
    os.environ["TMPDIR"] = str(private)
    repo = SCRIPT.parents[2]
    tracked = [
        SCRIPT,
        Path(EPOCH.__file__),
        Path(KEYED.__file__),
        Path(WRITE.__file__),
        Path(BASE.__file__),
        repo / "test/vane/native_vortex_lifecycle_fixture.hpp",
        repo / "test/vane/native_vortex_write_driver.cpp",
        repo / "test/vane/native_vortex_keyed_fixture.hpp",
        repo / "test/vane/native_vortex_fixture.hpp",
        repo / "test/vane/test_vane_vortex_source_gc_affinity.py",
        repo / "src/storage/ducklake_vortex_index_catalog.cpp",
        repo / "src/storage/ducklake_vortex_write.cpp",
        repo / "src/storage/ducklake_vortex_source_retention.cpp",
        repo / "src/storage/ducklake_checkpoint.cpp",
    ]
    hashes = {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in tracked}
    threads = PIN.thread_environment()
    native = Path(importlib.util.find_spec("vane._native").origin)
    report = {
        "status": "running",
        "mode": "functional_only",
        "native_cases": [],
        "crash_cases": [],
        "retained_private_files": {},
        "original_affinity": sorted(os.sched_getaffinity(0)),
        "thread_environment": threads,
        "source_sha256": hashes,
        "driver_sha256": hashlib.sha256(args.driver.read_bytes()).hexdigest(),
        "native_path": str(native),
        "native_sha256": hashlib.sha256(native.read_bytes()).hexdigest(),
        "acceptance": "Exact ownership, typed state and recovery; no timing/throughput gate or physical capacity claim.",
    }
    started = time.monotonic()
    try:
        with PIN.pinned_affinity(args.cpus):
            report["applied_affinity"] = sorted(os.sched_getaffinity(0))
            exercise(args.driver.resolve(), root, report)
            SCRATCH.assert_released(private)
            require(PIN.thread_environment() == threads, "Thread environment changed")
            require(
                all(hashlib.sha256(p.read_bytes()).hexdigest() == hashes[str(p)] for p in tracked),
                "Executed source changed",
            )
            require(
                hashlib.sha256(args.driver.read_bytes()).hexdigest() == report["driver_sha256"], "Native driver changed"
            )
        report["status"] = "passed"
    except BaseException as error:
        report.update(status="failed", error=f"{type(error).__name__}: {error}")
        raise
    finally:
        report["restored_affinity"] = sorted(os.sched_getaffinity(0))
        report["seconds_observation_only"] = time.monotonic() - started
        (root / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(
        json.dumps(
            {
                key: report[key]
                for key in ("status", "native_cases", "crash_cases", "applied_affinity", "inflight_checkpoint")
            }
        )
    )


if __name__ == "__main__":
    main()
