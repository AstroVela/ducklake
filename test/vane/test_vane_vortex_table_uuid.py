#!/usr/bin/env python3
"""Deterministic UUID fences, atomic retirement and real native crash recovery."""
from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time


def load(name, filename):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(filename))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


REF = load("uuid_reference", "test_vane_vortex_reference_wal.py")
SCRATCH = load("uuid_scratch", "test_vane_vortex_query_scratch.py")
SOURCE = load("uuid_sources", "test_vane_vortex_source_retention.py")
EPOCH, WRITE, KEYED, BASE, PIN = REF.EPOCH, REF.WRITE, REF.KEYED, REF.BASE, REF.PIN
require, query, quote = BASE.require, BASE.query, BASE.quote
SCRIPT = Path(__file__).resolve()
STAGES = (
    "retire_acquired",
    "retire_after_lookup",
    "retire_before_commit",
    "retire_before_stage",
    "retire_after_indexes",
    "retire_after_requests",
    "retire_after_stage",
)
MIB = 1024 * 1024


def mutate(c, uuid, request, rows, deletes=(), epoch=1):
    return WRITE.query_parameters(
        c,
        "SELECT * FROM ducklake_mutate_vortex_batch('lake','main','items','knn',?,"
        "?::STRUCT(id BIGINT,embedding FLOAT[],category VARCHAR,payload VARCHAR)[],?::BIGINT[],?::UBIGINT,?::VARCHAR)",
        [request, rows, list(deletes), epoch, uuid],
    )[0]


def advance(c, uuid, epoch):
    return WRITE.query_parameters(
        c,
        "SELECT * FROM ducklake_advance_vortex_request_epoch('lake','main','items','knn',?::UBIGINT,?::VARCHAR)",
        [epoch, uuid],
    )[0]


def retire(c, uuid):
    return query(c, "SELECT * FROM ducklake_retire_vortex_table('lake','main','items'," + quote(uuid) + ")")[0]


def state(c):
    c.execute("BEGIN")
    try:
        tables = REF.rows(
            c,
            "SELECT table_uuid::VARCHAR,table_id,table_name,begin_snapshot,end_snapshot "
            "FROM __ducklake_metadata_lake.ducklake_table ORDER BY ALL",
        )
        current = [r for r in tables if r[2] == "items" and r[4] is None]
        result = {
            "metadata": {
                name: REF.rows(c, f"SELECT * FROM {REF.META}.{name} ORDER BY ALL") for name in REF.METADATA_TABLES
            },
            "snapshots": REF.rows(
                c, f"SELECT snapshot_id,schema_version FROM {REF.META}.ducklake_snapshot ORDER BY ALL"
            ),
            "tables": tables,
            "rows": REF.rows(c, "SELECT id,embedding,category,payload FROM lake.items ORDER BY id") if current else [],
        }
        c.execute("COMMIT")
        return result
    except BaseException:
        c.execute("ROLLBACK")
        raise


def owned(c, uuid):
    names = [
        "ducklake_vortex_indexes_v1",
        "ducklake_vortex_generation_refs_v1",
        "ducklake_vortex_builds_v1",
        "ducklake_vortex_rewrites_v1",
        "ducklake_vortex_keyed_tables_v1",
        "ducklake_vortex_optimizer_v2",
        "ducklake_vortex_write_budgets_v1",
        "ducklake_vortex_request_epochs_v1",
        "ducklake_vortex_write_requests_v2",
    ]
    return {
        name: REF.rows(c, f"SELECT count(*) FROM {REF.META}.{name} WHERE table_uuid=" + quote(uuid))[0][0]
        for name in names
    }


def rejected(c, root, call, marker):
    before, files = state(c), SCRATCH.protected_files(root)
    WRITE.rejected(call, marker)
    require(
        state(c) == before and SCRATCH.protected_files(root) == files,
        "Rejected UUID operation changed business, metadata, snapshots or owned files",
    )


def configure(c, root, dimension):
    c.execute(
        "SELECT * FROM ducklake_enable_vortex_optimizer('lake','main','items','knn',"
        + quote(root / "indexes")
        + f",{dimension},100000,4194304,100000,67108864)"
    )
    c.execute("SELECT * FROM ducklake_cancel_vortex_optimizer('lake','main','items','knn')")
    deadline = time.monotonic() + 60
    while BASE.status(c)["running"]:
        require(time.monotonic() < deadline, "Optimizer did not stop")
        time.sleep(0.01)
    c.execute("SELECT * FROM ducklake_set_vortex_write_budget('lake','main','items','knn',1000,4194304,4096,67108864)")


def recreate(c, root, dimension=8):
    c.execute("CREATE TABLE lake.items(id BIGINT,embedding FLOAT[],category VARCHAR,payload VARCHAR)")
    c.execute("CALL ducklake_set_option('lake','experimental_data_file_format','vortex',table_name=>'items')")
    c.execute(
        "SELECT * FROM ducklake_create_vortex_index('lake','main','items','knn',"
        + quote(root / "indexes")
        + f",{dimension})"
    )
    KEYED.manage(c)
    return WRITE.table_uuid(c)


def public(root, report):
    def bootstrap(name):
        case = root / name
        case.mkdir(mode=0o700)
        case.joinpath("private").mkdir(mode=0o700)
        os.environ["TMPDIR"] = str(case / "private")
        data = case / "catalog"
        c = EPOCH.bootstrap(data)
        report["public_cases"].append(name)
        return c, data

    c, path = bootstrap("name-reuse-fences")
    try:
        old = WRITE.table_uuid(c)
        original = WRITE.rows(0, 3)
        receipt = mutate(c, old, "same-id", original)
        require(mutate(c, old, "same-id", original) == receipt, "Pinned receipt retry changed")
        configure(c, path, 8)
        rejected(c, path, lambda: c.execute("DROP TABLE lake.items"), "Managed Vortex tables require")
        require(retire(c, old) == {"table_uuid": old, "retired": True}, "Retirement returned wrong UUID")
        require(not any(owned(c, old).values()), "Retired UUID retained metadata")
        stable = state(c)
        require(
            retire(c, old) == {"table_uuid": old, "retired": True} and state(c) == stable,
            "Retirement replay changed native snapshots",
        )
        new = recreate(c, path, 128)
        require(new != old and EPOCH.status(c)["request_epoch"] == 1, "Same-name table inherited UUID or epoch")
        for call in (
            lambda: mutate(c, old, "same-id", original),
            lambda: mutate(c, old, "unseen", original),
            lambda: advance(c, old, 1),
            lambda: retire(c, old),
        ):
            rejected(c, path, call, "STALE_TABLE")
        require(not any(owned(c, old).values()), "Old request resurrected UUID state")
        row = {"id": 900, "embedding": [0.0] * 128, "category": None, "payload": "new-table"}
        fresh = mutate(c, new, "same-id", [row])
        require(fresh["table_uuid"] == new and mutate(c, new, "same-id", [row]) == fresh, "New UUID receipt failed")
        require(
            REF.rows(c, "SELECT id,embedding,category,payload FROM lake.items")
            == [[900, [0.0] * 128, None, "new-table"]],
            "Old payload crossed UUID boundary",
        )
        require(advance(c, new, 1)["request_epoch"] == 2, "New UUID epoch could not advance")
    finally:
        c.close()

    c, path = bootstrap("admission-and-native-history")
    try:
        old = WRITE.table_uuid(c)
        receipt = mutate(c, old, "seed", WRITE.rows(0, 1))
        c.execute(
            f"INSERT INTO {WRITE.REQUESTS} SELECT "
            + quote(old)
            + ",'cap-'||i,"
            + quote(receipt["payload_hash"])
            + f",1,{receipt['snapshot_id']},1 FROM range(99999)t(i)"
        )
        require(EPOCH.status(c)["catalog_requests"] == 100000, "Did not fill the existing receipt budget")
        rejected(c, path, lambda: mutate(c, old, "full", WRITE.rows(1, 1)), "request catalog budget")
        retire(c, old)
        require(not any(owned(c, old).values()), "Retirement did not release full receipt ownership")
        new = recreate(c, path)
        require(
            mutate(c, new, "full", WRITE.rows(1, 1))["table_uuid"] == new,
            "Released receipt slot could not be reused by new UUID",
        )
        history = REF.rows(
            c, "SELECT end_snapshot FROM __ducklake_metadata_lake.ducklake_table WHERE table_uuid=" + quote(old)
        )
        require(history and all(r[0] is not None for r in history), "Retirement deleted native table history")
    finally:
        c.close()

    c, path = bootstrap("policy-capacity-reuse")
    try:
        identities = []
        for i in range(17):
            uuid = WRITE.table_uuid(c)
            identities.append(uuid)
            mutate(c, uuid, "cycle", WRITE.rows(i, 1))
            configure(c, path, 8)
            retire(c, uuid)
            require(not any(owned(c, uuid).values()), "UUID retirement retained quota or policy state")
            if i != 16:
                recreate(c, path)
        require(len(set(identities)) == 17, "Recreation reused a table UUID")
        require(
            REF.rows(c, f"SELECT count(*) FROM {REF.META}.ducklake_vortex_optimizer_v2") == [[0]],
            "Retired policies consumed the existing 16-policy budget",
        )
    finally:
        c.close()

    c, path = bootstrap("ordinary-reader-protection")
    reader = None
    try:
        old = WRITE.table_uuid(c)
        records = WRITE.rows(0, 2)
        mutate(c, old, "seed", records)
        reader = c.cursor()
        reader.execute("BEGIN")
        expected = REF.rows(reader, "SELECT id,embedding,category,payload FROM lake.items ORDER BY id")
        retire(c, old)
        require(
            REF.rows(reader, "SELECT id,embedding,category,payload FROM lake.items ORDER BY id") == expected,
            "UUID retirement revoked an existing SQL snapshot",
        )
        SOURCE.policy(c, keep=1, age=0)
        require(SOURCE.collect(c)["blocked_by_transactions"], "Retirement bypassed the existing transaction GC gate")
        reader.execute("ROLLBACK")
        reader.close()
        reader = None
        require(not SOURCE.collect(c)["blocked_by_transactions"], "Ended reader still blocked source GC")
    finally:
        if reader:
            reader.close()
        c.close()

    c, path = bootstrap("foreign-uuid-isolation")
    try:
        target = WRITE.table_uuid(c)
        mutate(c, target, "target", WRITE.rows(0, 1))
        EPOCH.create_table(c, path, "neighbor")
        foreign = WRITE.table_uuid(c, "neighbor")
        records = WRITE.rows(100, 128)
        KEYED.mutate(c, "foreign-seed", records, table="neighbor")
        c.execute("CALL ducklake_flush_inlined_data('lake',table_name=>'neighbor')")
        c.execute(
            "SELECT * FROM ducklake_build_vortex_index('lake','main','neighbor','knn',"
            + quote(path / "indexes/foreign.json")
            + ",8,1)"
        )
        names = [
            name
            for name in REF.METADATA_TABLES
            if name
            not in (
                "ducklake_vortex_generations_v1",
                "ducklake_vortex_maintenance_v1",
                "ducklake_vortex_source_deletions_v1",
                "ducklake_vortex_source_retention_v1",
            )
        ]
        before = {
            name: REF.rows(c, f"SELECT * FROM {REF.META}.{name} WHERE table_uuid=" + quote(foreign) + " ORDER BY ALL")
            for name in names
        }
        generations = REF.rows(c, f"SELECT * FROM {REF.META}.ducklake_vortex_generations_v1 ORDER BY ALL")
        business = REF.rows(c, "SELECT id,embedding,category,payload FROM lake.neighbor ORDER BY id")
        files = SCRATCH.protected_files(path)
        retire(c, target)
        require(not any(owned(c, target).values()), "Target UUID state was not released")
        require(
            {
                name: REF.rows(
                    c, f"SELECT * FROM {REF.META}.{name} WHERE table_uuid=" + quote(foreign) + " ORDER BY ALL"
                )
                for name in names
            }
            == before,
            "Retirement changed another UUID's receipt, build, index or policy",
        )
        require(
            REF.rows(c, f"SELECT * FROM {REF.META}.ducklake_vortex_generations_v1 ORDER BY ALL") == generations,
            "Retirement marked another UUID's referenced generation retired",
        )
        require(
            REF.rows(c, "SELECT id,embedding,category,payload FROM lake.neighbor ORDER BY id") == business
            and SCRATCH.protected_files(path) == files,
            "Retirement changed foreign data or artifacts",
        )
        hits = query(
            c,
            "SELECT id FROM ducklake_vortex_search('lake','main','neighbor','knn',"
            + str(records[0]["embedding"])
            + "::FLOAT[],1)",
        )
        require(hits == [{"id": records[0]["id"]}], "Another UUID's named B query stopped working")
    finally:
        c.close()

    c, path = bootstrap("invalid-and-autocommit")
    try:
        uuid = WRITE.table_uuid(c)
        mutate(c, uuid, "seed", WRITE.rows(0, 1))
        for bad in ("", "invalid", uuid.upper()):
            if bad != uuid:
                rejected(c, path, lambda bad=bad: retire(c, bad), "UUID")
        rejected(c, path, lambda: retire(c, "00000000-0000-0000-0000-000000000000"), "STALE_TABLE")
        rejected(
            c, path, lambda: c.execute("SELECT * FROM ducklake_retire_vortex_table('lake','main','items',NULL)"), "NULL"
        )
        rejected(
            c,
            path,
            lambda: c.execute("SELECT * FROM ducklake_advance_vortex_request_epoch('lake','main','items','knn',1)"),
            "No function matches",
        )
        c.execute("BEGIN")
        try:
            WRITE.rejected(lambda: retire(c, uuid), "autocommit")
        finally:
            c.execute("ROLLBACK")
        require(EPOCH.status(c)["retained_requests"] == 1, "Invalid UUID changed receipt ownership")
    finally:
        c.close()


def recover(path, committed):
    os.environ["TMPDIR"] = str(path / "private")
    uuid = path.joinpath("table-uuid").read_text()
    expected = json.loads(path.joinpath("expected.json").read_text())
    sealed = json.loads(path.joinpath("files.json").read_text())
    c = BASE.connect(path)
    try:
        require(
            state(c) == expected, "Reopen changed one of 13 metadata tables, table history, snapshots or typed rows"
        )
        require(SCRATCH.protected_files(path) == sealed, "Reopen changed source/generation/caller files")
        if committed:
            require(expected["rows"] == [] and not any(owned(c, uuid).values()), "Committed retirement retained state")
        else:
            require(
                expected["rows"] == [[i, [float(i), 0.0], "A", f"seed-{i}"] for i in range(256)],
                "Uncommitted crash differs from the independent typed seed model",
            )
            require(owned(c, uuid)["ducklake_vortex_write_requests_v2"] == 1, "Uncommitted retirement lost its receipt")
        require(retire(c, uuid) == {"table_uuid": uuid, "retired": True}, "Recovery could not explicitly retire UUID")
        after = state(c)
        require(not any(owned(c, uuid).values()), "Explicit recovery left UUID metadata")
        require(
            retire(c, uuid) == {"table_uuid": uuid, "retired": True} and state(c) == after,
            "Recovery replay changed state",
        )
        require(SCRATCH.protected_files(path) == sealed, "Metadata retirement performed implicit physical GC")
        require(
            not query(c, "SELECT * FROM ducklake_vortex_optimizer_status('lake')"), "Reopen restored retired optimizer"
        )
        return {
            "status": "passed",
            "committed": committed,
            "cpus": sorted(os.sched_getaffinity(0)),
            "threads": PIN.thread_environment(),
        }
    finally:
        c.close()


def native(driver, root, report):
    ordinary = ["ok", "build-blocked", "held", "optimizer-blocked", "race", "conflict"]
    ordinary += [kind + "-" + stage for kind in ("cancel", "fail") for stage in STAGES]
    cases = [(mode, None) for mode in ordinary]
    cases += [
        ("pause-" + stage, stage)
        for stage in (
            "retire_before_commit",
            "retire_after_indexes",
            "retire_after_requests",
            "retire_after_stage",
            "retire_after_commit",
        )
    ]
    for mode, stage in cases:
        path = root / ("native-" + mode)
        path.mkdir(mode=0o700)
        path.joinpath("private").mkdir(mode=0o700)
        with path.joinpath("driver.log").open("w") as log:
            process = subprocess.Popen(
                [str(driver), str(path), "uuid-" + mode],
                env=dict(os.environ, TMPDIR=str(path / "private")),
                stdout=log,
                stderr=subprocess.STDOUT,
            )
            try:
                if stage:
                    SCRATCH.wait_at(process, path / "checkpoint", stage, report["applied_affinity"])
                    path.joinpath("files.json").write_text(json.dumps(SCRATCH.protected_files(path), indent=2) + "\n")
                    SCRATCH.kill(process, report, "native-uuid:" + stage)
                else:
                    require(process.wait(timeout=180) == 0, "Native UUID case failed: " + str(path / "driver.log"))
                    require(path.joinpath("checkpoint").read_text() == "uuid-complete", "UUID fixture incomplete")
                    path.joinpath("files.json").write_text(json.dumps(SCRATCH.protected_files(path), indent=2) + "\n")
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=20)
        committed = stage is None or stage == "retire_after_commit"
        with path.joinpath("recovery.log").open("w") as log:
            result = subprocess.run(
                [sys.executable, "-I", str(SCRIPT), "--recover", str(path), "--committed", str(int(committed))],
                stdout=log,
                stderr=subprocess.STDOUT,
                timeout=180,
            )
        require(result.returncode == 0, "Installed UUID recovery failed: " + str(path / "recovery.log"))
        evidence = json.loads(path.joinpath("recovery.json").read_text())
        require(
            evidence["cpus"] == report["applied_affinity"] and evidence["threads"] == report["threads"],
            "Recovery CPU affinity or thread settings changed",
        )
        if path.joinpath("extensions").is_dir():
            shutil.rmtree(path / "extensions")
        report["native_cases"].append(mode)
        print("native and reopen", mode, "passed", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--driver", type=Path)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--cpus", type=PIN.parse_cpus)
    parser.add_argument("--recover", type=Path)
    parser.add_argument("--committed", type=int, choices=(0, 1))
    args = parser.parse_args()
    if args.recover:
        path = args.recover.resolve()
        evidence = recover(path, bool(args.committed))
        path.joinpath("recovery.json").write_text(json.dumps(evidence, indent=2) + "\n")
        return
    require(args.driver and args.output_dir and args.cpus, "Require driver, fresh output and CPU set")
    root = args.output_dir.resolve()
    root.mkdir(mode=0o700)
    repo = SCRIPT.parents[2]
    tracked = [
        SCRIPT,
        Path(REF.__file__),
        Path(WRITE.__file__),
        Path(KEYED.__file__),
        Path(EPOCH.__file__),
        Path(BASE.__file__),
        Path(SCRATCH.__file__),
        Path(PIN.__file__),
    ]
    tracked += [
        repo / name
        for name in (
            "src/storage/ducklake_vortex_write.cpp",
            "src/storage/ducklake_vortex_optimizer.cpp",
            "src/storage/ducklake_vortex_index_catalog.cpp",
            "src/storage/ducklake_transaction.cpp",
            "src/functions/ducklake_vortex_write_function.cpp",
            "src/include/storage/ducklake_vortex_write.hpp",
            "test/vane/native_vortex_uuid_fixture.hpp",
            "test/vane/native_vortex_write_driver.cpp",
        )
    ]
    hashes = {str(p): SCRATCH.digest(p) for p in tracked}
    driver_sha = SCRATCH.digest(args.driver)
    native_path = Path(importlib.util.find_spec("vane._native").origin)
    binaries = {str(args.driver.resolve()): driver_sha, str(native_path): SCRATCH.digest(native_path)}
    providers = []
    for distribution in importlib.metadata.distributions():
        if distribution.metadata["Name"].replace("_", "-").startswith("vane-extension-"):
            artifacts = [
                distribution.locate_file(p) for p in distribution.files if str(p).endswith(".duckdb_extension")
            ]
            hashes_ = {str(p): SCRATCH.digest(p) for p in artifacts}
            providers.append(
                {"name": distribution.metadata["Name"], "version": distribution.version, "artifacts": hashes_}
            )
            binaries.update(hashes_)
    report = {
        "status": "running",
        "mode": "functional_only",
        "source_sha256": hashes,
        "driver_sha256": driver_sha,
        "binary_sha256": binaries,
        "providers": providers,
        "threads": PIN.thread_environment(),
        "public_cases": [],
        "native_cases": [],
        "sigkill": [],
        "original_affinity": sorted(os.sched_getaffinity(0)),
        "acceptance": "UUID isolation, atomic retirement and explicit recovery; stable-load qualifications deferred",
    }
    started = time.monotonic()
    try:
        with PIN.pinned_affinity(args.cpus):
            report["applied_affinity"] = sorted(os.sched_getaffinity(0))
            public(root, report)
            native(args.driver.resolve(), root, report)
            require(PIN.thread_environment() == report["threads"], "Thread environment changed")
            require({str(p): SCRATCH.digest(p) for p in tracked} == hashes, "Executed source changed")
            require(SCRATCH.digest(args.driver) == driver_sha, "Native driver changed")
            require(
                all(SCRATCH.digest(Path(p)) == sha for p, sha in binaries.items()), "Executed provider closure changed"
            )
        report["status"] = "passed"
    except BaseException as error:
        report.update(status="failed", error=f"{type(error).__name__}: {error}")
        raise
    finally:
        report["restored_affinity"] = sorted(os.sched_getaffinity(0))
        report["seconds_observation_only"] = time.monotonic() - started
        root.joinpath("report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({key: report[key] for key in ("status", "public_cases", "native_cases", "sigkill")}))


if __name__ == "__main__":
    main()
