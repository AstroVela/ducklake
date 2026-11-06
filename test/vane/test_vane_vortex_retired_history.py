#!/usr/bin/env python3
"""Qualify retired native history, bounded physical GC and fresh-process recovery."""
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


def load(filename):
    spec = importlib.util.spec_from_file_location("retired_uuid", Path(__file__).with_name(filename))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


UUID = load("test_vane_vortex_table_uuid.py")
REF, SOURCE, SCRATCH = UUID.REF, UUID.SOURCE, UUID.SCRATCH
EPOCH, WRITE, KEYED, BASE, PIN = UUID.EPOCH, UUID.WRITE, UUID.KEYED, UUID.BASE, UUID.PIN
query, require, quote = BASE.query, BASE.require, BASE.quote
META, SCRIPT, MIB = REF.META, Path(__file__).resolve(), 1024 * 1024
NATIVE_TABLES = (
    "ducklake_metadata",
    "ducklake_snapshot",
    "ducklake_snapshot_changes",
    "ducklake_schema",
    "ducklake_table",
    "ducklake_view",
    "ducklake_tag",
    "ducklake_column_tag",
    "ducklake_data_file",
    "ducklake_file_column_stats",
    "ducklake_file_variant_stats",
    "ducklake_delete_file",
    "ducklake_column",
    "ducklake_table_stats",
    "ducklake_table_column_stats",
    "ducklake_partition_info",
    "ducklake_partition_column",
    "ducklake_file_partition_value",
    "ducklake_files_scheduled_for_deletion",
    "ducklake_inlined_data_tables",
    "ducklake_column_mapping",
    "ducklake_name_mapping",
    "ducklake_schema_versions",
    "ducklake_macro",
    "ducklake_macro_impl",
    "ducklake_macro_parameters",
    "ducklake_sort_info",
    "ducklake_sort_expression",
)
STAGES = (
    "source_before_expire",
    "history_inline_drop_staged",
    "source_expiry_committed",
    "source_delete_intent_committed",
    "source_after_unlink",
    "source_delete_completed",
    "gc_deleting",
    "gc_file_deleted",
    "gc_files_deleted",
    "gc_after_finish",
    "history_before_checkpoint",
    "history_after_checkpoint",
)


def identifier(name):
    return '"' + name.replace('"', '""') + '"'


def exists(c, name):
    """The fixture explicitly probes both presence and absence of its known native tables."""
    try:
        c.execute(f"SELECT * FROM {META}.{identifier(name)} LIMIT 0")
        return True
    except Exception as error:
        require("Catalog Error" in str(error) and "does not exist" in str(error), "Unrelated table probe error")
        return False


def snapshot(c):
    ids = [r[0] for r in REF.rows(c, f"SELECT DISTINCT table_id FROM {META}.ducklake_table ORDER BY ALL")]
    deletes = [f"ducklake_inlined_delete_{i}" for i in ids if exists(c, f"ducklake_inlined_delete_{i}")]
    c.execute("BEGIN")
    try:
        result = {
            "native": {
                name: REF.rows(c, f"SELECT to_json(t) FROM {META}.{name} t ORDER BY ALL") for name in NATIVE_TABLES
            },
            "vortex": {
                name: REF.rows(c, f"SELECT to_json(t) FROM {META}.{name} t ORDER BY ALL")
                for name in REF.METADATA_TABLES
            },
        }
        inlined = [
            r[0] for r in REF.rows(c, f"SELECT table_name FROM {META}.ducklake_inlined_data_tables ORDER BY ALL")
        ]
        result["inline"] = {
            name: REF.rows(c, f"SELECT to_json(t) FROM {META}.{identifier(name)} t ORDER BY ALL")
            for name in sorted(inlined + deletes)
        }
        c.execute("COMMIT")
        return result
    except BaseException:
        c.execute("ROLLBACK")
        raise


def decoded(state, section, table):
    return [json.loads(row[0]) for row in state[section][table]]


def source_once(c, size=64 * MIB):
    result = SOURCE.collect(c, 1, size)
    require(not result["blocked_by_transactions"], "Unexpected active transaction during explicit recovery")
    require(result["expired_snapshots"] + result["collected_files"] <= 1, "Source object budget widened")
    require(result["collected_bytes"] <= size, "Source byte budget widened")
    return result


def drain(c):
    passes = []
    for _ in range(128):
        status = SOURCE.status(c)
        if not any(status[name] for name in ("eligible_snapshots", "scheduled_files", "pending_deletions")):
            return passes
        passes.append(source_once(c))
    raise AssertionError("Source cleanup exceeded the fixed work bound")


def generations(c, size=64 * MIB):
    result = query(c, f"SELECT * FROM ducklake_collect_vortex_generations('lake',1,{size})")[0]
    require(
        result["collected_generations"] <= 1 and result["collected_artifact_bytes"] <= size, "Generation budget widened"
    )
    return result


def no_change(c, root, call):
    before, files = snapshot(c), SCRATCH.protected_files(root)
    result = call()
    require(snapshot(c) == before and SCRATCH.protected_files(root) == files, "Deferred/rejected work changed state")
    return result


def live_rows(c, table="items"):
    return REF.rows(c, f"SELECT id,embedding,category,payload FROM lake.{table} ORDER BY id")


def typed(records):
    return sorted([[r["id"], r["embedding"], r["category"], r["payload"]] for r in records], key=lambda r: r[0])


def nearest(c, table="items"):
    return REF.rows(
        c,
        "SELECT id,embedding,category,payload,distance FROM ducklake_vortex_search('lake','main',"
        + quote(table)
        + ",'knn',[0,0,0,0,0,0,0,0]::FLOAT[],3)",
    )


def append_segment(c, root, table, filename):
    c.execute("CALL ducklake_flush_inlined_data('lake',table_name=>" + quote(table) + ")")
    c.execute(
        "SELECT * FROM ducklake_append_vortex_index_segment('lake','main',"
        + quote(table)
        + ",'knn',"
        + quote(root / "indexes" / filename)
        + ",8,1)"
    )
    c.execute("SELECT * FROM ducklake_collect_vortex_builds('lake',16,67108864)")


def clean_cache(root):
    for path in root.rglob("verified-v1"):
        if path.is_dir() and path.parent.name == ".vane" and path.resolve().is_relative_to(root.resolve()):
            shutil.rmtree(path)


def public_case(root, report, only):
    for kind in ("t", "bft"):
        if kind != only:
            continue
        case = root / ("public-" + kind)
        case.mkdir(mode=0o700)
        (case / "private").mkdir(mode=0o700)
        os.environ["TMPDIR"] = str(case / "private")
        data = case / "catalog"
        c = EPOCH.bootstrap(data)
        reader = None
        try:
            c.execute("CALL ducklake_set_option('lake','data_inlining_row_limit',1000)")
            c.execute("CALL ducklake_set_option('lake','data_inlining_row_limit',1000,schema=>'main')")
            old = WRITE.table_uuid(c)
            UUID.mutate(c, old, "seed", WRITE.rows(0, 128))
            if kind == "bft":
                append_segment(c, data, "items", "old.json")
                UUID.mutate(c, old, "unindexed", WRITE.rows(128, 32))
                c.execute("CALL ducklake_flush_inlined_data('lake',table_name=>'items')")
                UUID.mutate(c, old, "tail", WRITE.rows(160, 3), [0])
            else:
                UUID.mutate(c, old, "replace", WRITE.rows(0, 1), [1])
            old_id = REF.rows(c, f"SELECT table_id FROM {META}.ducklake_table WHERE table_uuid=" + quote(old))[0][0]
            old_inline = [
                r[0]
                for r in REF.rows(
                    c, f"SELECT table_name FROM {META}.ducklake_inlined_data_tables WHERE table_id={old_id}"
                )
            ]
            if exists(c, f"ducklake_inlined_delete_{old_id}"):
                old_inline.append(f"ducklake_inlined_delete_{old_id}")
            old_sources = {k: v for k, v in SCRATCH.protected_files(data).items() if k.startswith("data/")}
            old_gens = query(c, f"SELECT generation,artifact_bytes FROM {META}.ducklake_vortex_generations_v1")

            EPOCH.create_table(c, data, "neighbor")
            neighbor = WRITE.rows(50000, 128)
            KEYED.mutate(c, "neighbor", neighbor, table="neighbor")
            append_segment(c, data, "neighbor", "neighbor.json")
            foreign_query, foreign_rows = nearest(c, "neighbor"), live_rows(c, "neighbor")
            require(foreign_query and foreign_rows == typed(neighbor), "Foreign B fixture has no typed query evidence")
            EPOCH.create_table(c, data, "also_retired")
            KEYED.mutate(c, "also", WRITE.rows(70000, 2), table="also_retired")
            other_uuid = WRITE.table_uuid(c, "also_retired")
            other_id = REF.rows(c, f"SELECT table_id FROM {META}.ducklake_table WHERE table_uuid=" + quote(other_uuid))[
                0
            ][0]
            SOURCE.policy(c)
            drain(c)
            REF.checkpoint(c, data)
            reader = c.cursor()
            reader.execute("BEGIN")
            old_rows = live_rows(reader)
            old_snapshot = REF.rows(c, f"SELECT max(snapshot_id) FROM {META}.ducklake_snapshot")[0][0]
            UUID.retire(c, old)
            c.execute(
                "SELECT * FROM ducklake_retire_vortex_table('lake','main','also_retired'," + quote(other_uuid) + ")"
            )
            new = UUID.recreate(c, data)
            fresh = WRITE.rows(90000, 128 if kind == "bft" else 2)
            UUID.mutate(c, new, "new-table", fresh)
            if kind == "bft":
                append_segment(c, data, "items", "new.json")
            new_query = nearest(c)
            (data / "data/unknown.keep").write_bytes(b"unregistered data")
            (data / "indexes/unknown.keep").write_bytes(b"unregistered index")
            before = snapshot(c)
            require(
                any(
                    r["scope"] == "table" and r["scope_id"] == old_id
                    for r in decoded(before, "native", "ducklake_metadata")
                ),
                "Retirement removed options needed by retained snapshots",
            )
            before_files = SCRATCH.protected_files(data)
            blocked = no_change(c, data, lambda: SOURCE.collect(c, 1))
            require(
                blocked["blocked_by_transactions"]
                and not blocked["expired_snapshots"]
                and not blocked["collected_files"],
                "Old reader did not block history/physical GC",
            )
            require(live_rows(reader) == old_rows, "Retirement/recreation revoked the ordinary reader's old table")
            reader.execute("COMMIT")
            reader.close()
            reader = None

            for enabled, keep, age in (
                (False, 1, 0),
                (True, 1, 315360000),
                (True, len(before["native"]["ducklake_snapshot"]), 0),
            ):
                SOURCE.policy(c, enabled, keep, age)
                result = no_change(c, data, lambda: SOURCE.collect(c, 1))
                require(
                    not result["expired_snapshots"] and not result["collected_files"],
                    "Retention policy lost its existing scope",
                )
            SOURCE.policy(c)
            require(
                REF.rows(
                    c, f"SELECT id,embedding,category,payload FROM lake.items AT (VERSION=>{old_snapshot}) ORDER BY id"
                )
                == old_rows,
                "A retained old snapshot no longer resolves the retired UUID after name reuse",
            )
            # A single explicit raw expiry stays inside the caller transaction, including native inline DROP.
            anchor = min(r["snapshot_id"] for r in decoded(before, "native", "ducklake_snapshot"))
            stable = snapshot(c)
            c.execute("BEGIN")
            c.execute(f"SELECT * FROM ducklake_expire_snapshots('lake',versions=>[{anchor}])")
            c.execute("ROLLBACK")
            require(
                snapshot(c) == stable and SCRATCH.protected_files(data) == before_files,
                "Rolled-back expiry committed native history/DDL",
            )
            small_gen = no_change(c, data, lambda: generations(c, 1))
            require(not small_gen["collected_generations"], "Generation collector exceeded its byte budget")

            passes = []
            for _ in range(128):
                target = REF.rows(c, f"SELECT count(*) FROM {META}.ducklake_table WHERE table_uuid=" + quote(old))[0][0]
                if not target:
                    break
                passes.append(source_once(c, 1))
            require(
                not target and all(not p["collected_files"] for p in passes),
                "History expiry performed an out-of-budget physical unlink",
            )
            for name in old_inline:
                require(not exists(c, name), "Unreferenced retired inline table was not physically dropped")
            if old_sources:
                protected = SCRATCH.protected_files(data)
                too_small = source_once(c, 1)
                require(
                    not too_small["collected_files"]
                    and not too_small["collected_bytes"]
                    and SCRATCH.protected_files(data) == protected,
                    "Head source bypassed the byte budget; independent bounded expiry may continue",
                )
            drain_passes = drain(c)
            gc = generations(c)
            after_files = SCRATCH.protected_files(data)
            removed = set(before_files) - set(after_files)
            allowed = set(old_sources) | {
                name
                for name in before_files
                if any(name.startswith("indexes/" + g["generation"] + "/") for g in old_gens)
            }
            require(
                removed == allowed and all(before_files[k] == v for k, v in after_files.items()),
                "Physical cleanup touched live/foreign/caller files or retained old payload",
            )
            require(
                sum(p["collected_files"] for p in drain_passes) == len(old_sources)
                and sum(p["collected_bytes"] for p in drain_passes) == sum(v["bytes"] for v in old_sources.values()),
                "Source object/byte accounting differs from actual removed files",
            )
            require(
                gc["collected_generations"] == len(old_gens)
                and gc["collected_artifact_bytes"] == sum(g["artifact_bytes"] for g in old_gens),
                "Generation accounting differs from registered retired ownership",
            )
            require(
                live_rows(c) == typed(fresh)
                and nearest(c) == new_query
                and live_rows(c, "neighbor") == foreign_rows
                and nearest(c, "neighbor") == foreign_query,
                "Cleanup changed new UUID or foreign B queries",
            )
            require(
                not REF.rows(c, f"SELECT table_id FROM {META}.ducklake_table WHERE table_uuid=" + quote(other_uuid)),
                "Catalog retention was incorrectly narrowed to the target UUID",
            )
            final = snapshot(c)
            retained_options = [
                r
                for r in decoded(before, "native", "ducklake_metadata")
                if not (r["scope"] == "table" and r["scope_id"] in (old_id, other_id))
            ]
            require(
                decoded(final, "native", "ducklake_metadata") == retained_options,
                "Expiry retained dead table options or changed live/catalog/schema configuration",
            )
            db_before = (data / "metadata.ducklake").stat().st_size
            wal_before = REF.wal_bytes(data)
            REF.checkpoint(c, data)
            require(
                REF.wal_bytes(data) == 0 and snapshot(c) == final and SCRATCH.protected_files(data) == after_files,
                "Native checkpoint changed logical state or physical ownership",
            )
            report["public_cases"].append(
                {
                    "kind": kind,
                    "objects_removed": len(removed),
                    "actual_bytes_removed": sum(before_files[k]["bytes"] for k in removed),
                    "source_files": len(old_sources),
                    "source_bytes": sum(v["bytes"] for v in old_sources.values()),
                    "generation_accounted_bytes": gc["collected_artifact_bytes"],
                    "database_bytes_before_checkpoint": db_before,
                    "database_bytes_after_checkpoint": (data / "metadata.ducklake").stat().st_size,
                    "wal_bytes_before_checkpoint": wal_before,
                    "wal_bytes_after_checkpoint": REF.wal_bytes(data),
                    "catalog_scope": "also_retired native history reclaimed; neighbor and recreated table preserved",
                }
            )
        finally:
            if reader is not None:
                reader.close()
            c.close()
            clean_cache(case)

    if only != "replay":
        return
    case = root / "public-retirement-replay-history-limit"
    case.mkdir(mode=0o700)
    (case / "private").mkdir(mode=0o700)
    os.environ["TMPDIR"] = str(case / "private")
    c = EPOCH.bootstrap(case / "catalog")
    try:
        old = WRITE.table_uuid(c)
        UUID.mutate(c, old, "seed", WRITE.rows(0, 2))
        UUID.retire(c, old)
        require(UUID.retire(c, old)["retired"], "Retirement was not replayable while native history remained")
        SOURCE.policy(c)
        drain(c)
        no_change(
            c,
            case / "catalog",
            lambda: WRITE.rejected(lambda: UUID.retire(c, old), "Unknown Vortex table retirement UUID"),
        )
        report["public_cases"].append(
            {
                "kind": "retirement-replay-history-limit",
                "result": "expired native history cannot authenticate retirement replay",
            }
        )
    finally:
        c.close()
        clean_cache(case)


def public(root, report):
    # Each provider is loaded once per process; do not reload separately cached static OpenMP runtimes.
    for kind in ("t", "bft", "replay", "reuse"):
        with (root / ("public-" + kind + ".log")).open("w") as log:
            result = subprocess.run(
                [sys.executable, "-I", str(SCRIPT), "--public-case", kind, "--output-dir", str(root)],
                stdout=log,
                stderr=subprocess.STDOUT,
                timeout=180,
            )
        require(result.returncode == 0, "Public history contract failed: " + str(root / ("public-" + kind + ".log")))
        evidence = json.loads((root / ("public-" + kind + "-report.json")).read_text())
        require(
            evidence["cpus"] == report["applied_affinity"] and evidence["threads"] == report["threads"],
            "Public child affinity or threads changed",
        )
        report["public_cases"].extend(evidence["public_cases"])
        print("public", kind, "passed", flush=True)


def reuse_case(root, report):
    case = root / "public-native-block-reuse"
    case.mkdir(mode=0o700)
    (case / "private").mkdir(mode=0o700)
    os.environ["TMPDIR"] = str(case / "private")
    data = case / "catalog"
    c = EPOCH.bootstrap(data)
    released, uuids, observations = set(), [], []
    try:
        for cycle in range(3):
            uuid = WRITE.table_uuid(c)
            uuids.append(uuid)
            records = WRITE.rows(cycle * 1000, 512)
            for i, row in enumerate(records):
                row["payload"] = "".join(hashlib.sha256(f"{cycle}:{i}:{j}".encode()).hexdigest() for j in range(32))
            UUID.mutate(c, uuid, "seed", records)
            require(live_rows(c) == typed(records), "Block-reuse fixture changed typed rows")
            REF.checkpoint(c, data)
            names = [r[0] for r in REF.rows(c, f"SELECT table_name FROM {META}.ducklake_inlined_data_tables")]
            blocks = set()
            for name in names:
                storage = query(c, "SELECT * FROM pragma_storage_info(" + quote(META + "." + name) + ")")
                blocks.update(r["block_id"] for r in storage if r["block_id"] is not None and r["block_id"] >= 0)
            require(blocks and (not cycle or blocks & released), "No actual released native block was reused")
            observation = {
                "cycle": cycle,
                "persisted_blocks": sorted(blocks),
                "reused_blocks": sorted(blocks & released),
                "database_bytes_written": (data / "metadata.ducklake").stat().st_size,
            }
            released.update(blocks)
            UUID.retire(c, uuid)
            SOURCE.policy(c)
            drain(c)
            require(
                not any(UUID.owned(c, uuid).values()) and all(not exists(c, name) for name in names),
                "Retired cycle retained UUID state or native inline tables",
            )
            require(
                not REF.rows(
                    c,
                    f"SELECT scope_id FROM {META}.ducklake_metadata WHERE scope='table' AND scope_id NOT IN (SELECT table_id FROM {META}.ducklake_table)",
                ),
                "Retired cycles accumulated orphaned native table configuration",
            )
            REF.checkpoint(c, data)
            require(REF.wal_bytes(data) == 0, "Explicit checkpoint retained the old WAL without active writers")
            observation.update(
                database_bytes_retired=(data / "metadata.ducklake").stat().st_size,
                wal_bytes_retired=REF.wal_bytes(data),
            )
            observations.append(observation)
            if cycle < 2:
                UUID.recreate(c, data)
        require(
            len(set(uuids)) == 3 and not list((data / "data").rglob("*.vortex")),
            "T-only reuse changed UUID identity or flushed business rows",
        )
        report["public_cases"].append(
            {
                "kind": "native-block-reuse",
                "cycles": observations,
                "scope": "fixed three-cycle native allocator reuse; no file-size, RSS or sustained capacity bound",
            }
        )
    finally:
        c.close()
        clean_cache(case)


def recover(root):
    os.environ["TMPDIR"] = str(root / "private")
    identity = json.loads((root / "identity.json").read_text())
    expected = json.loads((root / "expected.json").read_text())
    sealed = json.loads((root / "files.json").read_text())
    baseline_files = json.loads((root / "baseline-files.json").read_text())
    baseline = json.loads((root / "before.json").read_text())
    old_sources = {k: v for k, v in baseline_files.items() if k.endswith(".vortex")}
    generation_ids = {r["generation"] for r in decoded(baseline, "vortex", "ducklake_vortex_generations_v1")}
    c = BASE.connect(root)
    try:
        actual = snapshot(c)
        require(
            set(actual["native"]) == set(NATIVE_TABLES) and len(actual["vortex"]) == 13,
            "Incomplete native/Vortex inventory",
        )
        require(actual == expected, "Native recovery changed committed metadata or inline history")
        require(SCRATCH.protected_files(root) == sealed, "Reopen implicitly collected data/generation/caller files")
        require(
            not query(c, "SELECT * FROM ducklake_vortex_optimizer_status('lake')"),
            "Reopen restarted retired optimizer work",
        )
        fresh = [[i, [float(i), 0.0], "A", "seed-" + str(i)] for i in (20000, 20001)]
        require(
            live_rows(c) == fresh and REF.rows(c, "SELECT id FROM lake.other") == [[2]],
            "Fresh process lost independently modeled live rows",
        )
        result = REF.rows(
            c,
            "SELECT id,embedding,category,payload FROM ducklake_vortex_search('lake','main','items','knn',[0,0]::FLOAT[],3)",
        )
        require(result == fresh, "Fresh process cannot query the recreated UUID's exact T vectors")
        source_counts = SOURCE.status(c)
        baseline_counts = decoded(baseline, "vortex", "ducklake_vortex_source_retention_v1")[0]
        passes = drain(c)
        gc = generations(c)
        final = snapshot(c)
        require(not any(UUID.owned(c, identity["old_uuid"]).values()), "Recovered retired UUID state reappeared")
        require(
            all(t["table_id"] != identity["old_id"] for t in decoded(final, "native", "ducklake_table")),
            "Retired native table history survived explicit recovery",
        )
        require(
            not any(
                r["scope"] == "table" and r["scope_id"] == identity["old_id"]
                for r in decoded(final, "native", "ducklake_metadata")
            ),
            "Explicit recovery retained dead table options",
        )
        retained_options = [
            r
            for r in decoded(baseline, "native", "ducklake_metadata")
            if not (r["scope"] == "table" and r["scope_id"] == identity["old_id"])
        ]
        require(
            decoded(final, "native", "ducklake_metadata") == retained_options,
            "Recovery changed catalog/schema or recreated-table configuration",
        )
        for name in identity["old_inline"]:
            require(not exists(c, name), "Retired native inline/deletion table survived explicit recovery")
        require(
            live_rows(c) == fresh and REF.rows(c, "SELECT id FROM lake.other") == [[2]],
            "Explicit recovery changed live typed rows",
        )
        final_files = SCRATCH.protected_files(root)
        allowed = set(old_sources) | {
            name for name in baseline_files if any(name.startswith("indexes/" + g + "/") for g in generation_ids)
        }
        require(
            set(baseline_files) - set(final_files) == allowed
            and all(baseline_files[k] == v for k, v in final_files.items()),
            "Recovery collected an unknown/live object or retained old payload",
        )
        status = SOURCE.status(c)
        require(
            status["collected_files"] - baseline_counts["collected_files"] == len(old_sources)
            and status["collected_bytes"] - baseline_counts["collected_bytes"]
            == sum(v["bytes"] for v in old_sources.values()),
            "Partial unlink recovery double-counted or lost source bytes",
        )
        UUID.WRITE.rejected(lambda: UUID.retire(c, identity["old_uuid"]), "STALE_TABLE")
        REF.checkpoint(c, root)
        require(
            REF.wal_bytes(root) == 0 and snapshot(c) == final and SCRATCH.protected_files(root) == final_files,
            "Post-recovery checkpoint changed history/ownership",
        )
        again = drain(c)
        repeated = generations(c)
        require(
            not again and not repeated["collected_generations"] and snapshot(c) == final,
            "Explicit reclamation replay changed committed state",
        )
        return {
            "status": "passed",
            "cpus": sorted(os.sched_getaffinity(0)),
            "threads": PIN.thread_environment(),
            "source_at_reopen": source_counts,
            "source_recovery_passes": passes,
            "generation_recovery": gc,
            "physical_files_removed": len(allowed),
            "physical_bytes_removed": sum(baseline_files[k]["bytes"] for k in allowed),
        }
    finally:
        c.close()


def native(driver, root, report):
    cases = [("t", "ok"), ("bft", "ok"), ("bft", "held"), ("bft", "conflict")]
    cases += [
        ("bft", kind + "-" + stage)
        for kind in ("fail", "cancel")
        for stage in (
            "source_before_expire",
            "history_inline_drop_staged",
            "source_expiry_committed",
            "source_delete_intent_committed",
            "source_after_unlink",
            "gc_deleting",
            "gc_file_deleted",
        )
    ]
    cases += [("bft", "pause-" + stage) for stage in STAGES]
    cases += [
        ("t", "pause-" + stage)
        for stage in ("history_inline_drop_staged", "source_expiry_committed", "history_after_checkpoint")
    ]
    for kind, mode in cases:
        case = root / ("native-" + kind + "-" + mode)
        case.mkdir(mode=0o700)
        (case / "private").mkdir(mode=0o700)
        action = "history-" + ("t-" if kind == "t" else "") + mode
        with (case / "driver.log").open("w") as log:
            process = subprocess.Popen(
                [str(driver), str(case), action],
                env=dict(os.environ, TMPDIR=str(case / "private")),
                stdout=log,
                stderr=subprocess.STDOUT,
            )
            try:
                SCRATCH.wait_at(process, case / "checkpoint", "history_prepared", report["applied_affinity"])
                (case / "baseline-files.json").write_text(json.dumps(SCRATCH.protected_files(case), indent=2) + "\n")
                (case / "checkpoint").unlink()
                (case / "release-history_prepared").write_text("release\n")
                if mode.startswith("pause-"):
                    stage = mode.removeprefix("pause-")
                    SCRATCH.wait_at(process, case / "checkpoint", stage, report["applied_affinity"])
                    (case / "files.json").write_text(json.dumps(SCRATCH.protected_files(case), indent=2) + "\n")
                    SCRATCH.kill(process, report, kind + ":" + stage)
                else:
                    require(
                        process.wait(timeout=180) == 0,
                        "Native retired-history fixture failed: " + str(case / "driver.log"),
                    )
                    require(
                        (case / "checkpoint").read_text() == "history_complete", "Native history fixture did not finish"
                    )
                    (case / "files.json").write_text(json.dumps(SCRATCH.protected_files(case), indent=2) + "\n")
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=20)
        with (case / "recovery.log").open("w") as log:
            result = subprocess.run(
                [sys.executable, "-I", str(SCRIPT), "--recover", str(case)],
                stdout=log,
                stderr=subprocess.STDOUT,
                timeout=180,
            )
        require(result.returncode == 0, "Installed retired-history recovery failed: " + str(case / "recovery.log"))
        evidence = json.loads((case / "recovery.json").read_text())
        require(
            evidence["cpus"] == report["applied_affinity"] and evidence["threads"] == report["threads"],
            "Recovery affinity or threads changed",
        )
        report["native_cases"].append(
            {
                "kind": kind,
                "mode": mode,
                "status": "passed",
                "storage_observations": {p.stem: json.loads(p.read_text()) for p in case.glob("storage-*.json")},
            }
        )
        clean_cache(case)
        print("native and fresh recovery", kind, mode, "passed", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--driver", type=Path)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--cpus", type=PIN.parse_cpus)
    parser.add_argument("--recover", type=Path)
    parser.add_argument("--public-case", choices=("t", "bft", "replay", "reuse"))
    args = parser.parse_args()
    if args.public_case:
        evidence = {"public_cases": [], "cpus": sorted(os.sched_getaffinity(0)), "threads": PIN.thread_environment()}
        if args.public_case == "reuse":
            reuse_case(args.output_dir.resolve(), evidence)
        else:
            public_case(args.output_dir.resolve(), evidence, args.public_case)
        (args.output_dir / ("public-" + args.public_case + "-report.json")).write_text(
            json.dumps(evidence, indent=2) + "\n"
        )
        return
    if args.recover:
        evidence = recover(args.recover.resolve())
        (args.recover / "recovery.json").write_text(json.dumps(evidence, indent=2) + "\n")
        return
    require(args.driver and args.output_dir and args.cpus, "Require native driver, fresh output and CPU set")
    root = args.output_dir.resolve()
    root.mkdir(mode=0o700)
    repo = SCRIPT.parents[2]
    tracked = [
        SCRIPT,
        Path(UUID.__file__),
        Path(REF.__file__),
        Path(SOURCE.__file__),
        Path(SCRATCH.__file__),
        Path(EPOCH.__file__),
        Path(WRITE.__file__),
        Path(KEYED.__file__),
        Path(BASE.__file__),
        Path(PIN.__file__),
    ]
    tracked += [
        repo / name
        for name in (
            "test/vane/native_vortex_history_fixture.hpp",
            "test/vane/native_vortex_write_driver.cpp",
            "src/storage/ducklake_metadata_manager.cpp",
            "src/storage/ducklake_vortex_source_retention.cpp",
            "src/storage/ducklake_vortex_index_catalog.cpp",
            "src/storage/ducklake_vortex_write.cpp",
            "src/storage/ducklake_transaction.cpp",
        )
    ]
    hashes = {str(p): SCRATCH.digest(p) for p in tracked}
    native_path = Path(importlib.util.find_spec("vane._native").origin)
    binaries = {str(args.driver.resolve()): SCRATCH.digest(args.driver), str(native_path): SCRATCH.digest(native_path)}
    providers = []
    for distribution in importlib.metadata.distributions():
        if distribution.metadata["Name"].replace("_", "-").startswith("vane-extension-"):
            artifacts = {
                str(distribution.locate_file(p)): SCRATCH.digest(distribution.locate_file(p))
                for p in distribution.files
                if str(p).endswith(".duckdb_extension")
            }
            providers.append(
                {"name": distribution.metadata["Name"], "version": distribution.version, "artifacts": artifacts}
            )
            binaries.update(artifacts)
    report = {
        "status": "running",
        "mode": "deterministic_functional_only",
        "source_sha256": hashes,
        "binary_sha256": binaries,
        "providers": providers,
        "threads": PIN.thread_environment(),
        "original_affinity": sorted(os.sched_getaffinity(0)),
        "public_cases": [],
        "native_cases": [],
        "sigkill": [],
        "acceptance": "Native history and physical ownership/recovery; stable-load qualifications deferred",
    }
    started = time.monotonic()
    try:
        with PIN.pinned_affinity(args.cpus):
            report["applied_affinity"] = sorted(os.sched_getaffinity(0))
            public(root, report)
            native(args.driver.resolve(), root, report)
            require(PIN.thread_environment() == report["threads"], "Thread environment changed")
            require({str(p): SCRATCH.digest(p) for p in tracked} == hashes, "Executed source changed")
            require(
                all(SCRATCH.digest(Path(p)) == sha for p, sha in binaries.items()),
                "Executed binary/provider closure changed",
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
                "status": report["status"],
                "public_cases": len(report["public_cases"]),
                "native_cases": len(report["native_cases"]),
                "actual_sigkill": len(report["sigkill"]),
            }
        )
    )


if __name__ == "__main__":
    main()
