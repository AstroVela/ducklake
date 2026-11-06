#!/usr/bin/env python3
"""Qualify capacity admission with fixed inputs, exact boundaries and native recovery."""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
from types import SimpleNamespace


def load(name, filename):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(filename))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


REF = load("capacity_lifecycle", "test_vane_vortex_reference_wal.py")
MODEL = load("capacity_model", "vortex_capacity_model.py")
VIEW = load("capacity_view", "test_native_vortex_read_view.py")
SOURCE = load("capacity_sources", "test_vane_vortex_source_retention.py")
EPOCH, KEYED, WRITE, BASE, PIN = REF.EPOCH, REF.KEYED, REF.WRITE, REF.BASE, REF.PIN
require, query, quote = BASE.require, BASE.query, BASE.quote
SCRIPT = Path(__file__).resolve()
MIB = 1024 * 1024


def budget(c, t_rows=100000, t_bytes=4 * MIB, f_rows=100000, f_bytes=64 * MIB):
    c.execute(
        "SELECT * FROM ducklake_set_vortex_write_budget('lake','main','items','knn',"
        f"{t_rows},{t_bytes},{f_rows},{f_bytes})"
    )


def bootstrap(root, dimension, managed=False, inline=False):
    (root / "indexes").mkdir(mode=0o700)
    c = BASE.connect(root)
    c.execute("CREATE TABLE lake.items(id BIGINT,embedding FLOAT[],category VARCHAR,payload VARCHAR)")
    c.execute("CALL ducklake_set_option('lake','experimental_data_file_format','vortex',table_name=>'items')")
    if not inline:
        c.execute("CALL ducklake_set_option('lake','data_inlining_row_limit',0,table_name=>'items')")
    for kind in ("indexes", "optimizer", "writes"):
        c.execute(f"SELECT * FROM ducklake_initialize_vortex_{kind}('lake')")
    c.execute(
        "SELECT * FROM ducklake_create_vortex_index('lake','main','items','knn',"
        + quote(root / "indexes")
        + f",{dimension})"
    )
    if managed:
        KEYED.manage(c)
    # Start/cancel while empty: fixed-input admission must not depend on a background build race.
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
    budget(c)
    return c


def row(key, dimension=2, category=None, payload=None):
    return {"id": key, "embedding": [float(key)] + [0.0] * (dimension - 1), "category": category, "payload": payload}


def state(c, root):
    # Fixed metadata universe; include all source/delete files, not only registered data files.
    metadata = {name: REF.rows(c, f"SELECT * FROM {REF.META}.{name} ORDER BY ALL") for name in REF.METADATA_TABLES}
    metadata["snapshots"] = REF.rows(
        c, f"SELECT snapshot_id,schema_version FROM {REF.META}.ducklake_snapshot ORDER BY ALL"
    )
    metadata["business"] = REF.rows(c, "SELECT count(*),sum(hash(id,embedding,category,payload)) FROM lake.items")
    for name in inline_names(c):
        metadata[name] = REF.rows(
            c,
            f'SELECT count(*),sum(hash(id,embedding,category,payload,begin_snapshot,end_snapshot)) FROM {REF.META}."{name}"',
        )
    files = {}
    for directory in ("data", "indexes", "private"):
        for path in sorted((root / directory).rglob("*")):
            if path.is_file():
                with path.open("rb") as stream:
                    files[str(path.relative_to(root))] = [
                        path.stat().st_size,
                        hashlib.file_digest(stream, "sha256").hexdigest(),
                    ]
    return {"metadata": metadata, "files": files}


def reject(c, root, call, marker):
    before = state(c, root)
    WRITE.rejected(call, marker)
    require(state(c, root) == before, "Rejected admission changed business, receipts, snapshots, budgets or files")


def ack(c, request, rows, dimension, deletes=None):
    result = WRITE.batch(c, request, rows) if deletes is None else KEYED.mutate(c, request, rows, deletes)
    require(
        result["payload_hash"] == MODEL.digest(MODEL.canonical(rows, dimension, deletes)),
        "Receipt digest differs from independent encoding",
    )
    require(
        result["row_count"] == len(rows) + (0 if deletes is None else len(deletes)), "Receipt operation count differs"
    )
    return result


def inline_names(c):
    identity = REF.rows(c, f"SELECT table_id FROM {REF.META}.ducklake_vortex_write_budgets_v1 WHERE index_name='knn'")
    require(len(identity) == 1 and type(identity[0][0]) is int, "Capacity fixture requires one budgeted target table")
    entries = query(
        c,
        f"SELECT table_name FROM {REF.META}.ducklake_inlined_data_tables WHERE table_id={identity[0][0]} ORDER BY table_name",
    )
    names = [entry["table_name"] for entry in entries]
    require(
        all(name.startswith("ducklake_inlined_data_") and name.replace("_", "").isalnum() for name in names),
        "Unexpected inline table identity",
    )
    return names


def pending_versions(c):
    return [
        version
        for name in inline_names(c)
        for version in query(c, f'SELECT id,embedding,category,payload FROM {REF.META}."{name}"')
    ]


def append(c, root, name, dimension):
    head = BASE.head(c)
    c.execute(
        "SELECT * FROM ducklake_append_vortex_index_segment('lake','main','items','knn',"
        + quote(root / "indexes" / (name + ".json"))
        + f",{dimension},{head['revision']})"
    )


def search_first(c, dimension, key=0):
    hits = query(
        c,
        "SELECT id FROM ducklake_vortex_search('lake','main','items','knn'," + str([0.0] * dimension) + "::FLOAT[],1)",
    )
    require(hits == [{"id": key}], "Capacity boundary changed current query visibility/top-k")


def seed_sql(c, start, stop, dimension):
    c.execute(
        f"INSERT INTO lake.items SELECT i,list_resize([i::FLOAT],{dimension},0::FLOAT),'C','seed' FROM range({start},{stop})t(i)"
    )


def check_seed(c, count, dimension, minimum=0):
    actual = REF.rows(
        c,
        "SELECT count(*),count(DISTINCT id),min(id),max(id),"
        f"count(*) FILTER(WHERE array_length(embedding)!={dimension} OR list_count(embedding)!={dimension} OR embedding[1]!=id::FLOAT "
        "OR coalesce(list_sum(list_transform(list_slice(embedding,2,-1),x->abs(x))),0)!=0 OR category IS DISTINCT FROM 'C' OR payload IS DISTINCT FROM 'seed') FROM lake.items",
    )
    require(
        actual == [[count, count, minimum, minimum + count - 1, 0]],
        "Real boundary rows differ from the independently generated range",
    )


def physical_case(root, dimension):
    limit = MODEL.row_limit(dimension)
    c = bootstrap(root, dimension)
    reader = None
    try:
        # A genuine B keeps F below its own row limit, isolating the global row/vector guard.
        seed_sql(c, 0, 65, dimension)
        append(c, root, "first", dimension)
        seed_sql(c, 65, limit - 1, dimension)
        last = [row(limit - 1, dimension, "C", "seed")]
        receipt = ack(c, "exact", last, dimension)
        check_seed(c, limit, dimension)
        search_first(c, dimension)
        extra = [row(limit, dimension, "C", "seed")]
        reject(c, root, lambda: WRITE.batch(c, "over", extra), "physical and inline rows")
        require(WRITE.batch(c, "exact", last) == receipt, "Retry at physical capacity failed")
        if dimension == 4096:
            reader = c.cursor()
            reader.execute("BEGIN")
            check_seed(reader, limit, dimension)
        c.execute("DELETE FROM lake.items WHERE id=0")
        reject(c, root, lambda: WRITE.batch(c, "over", extra), "physical and inline rows")
        require(WRITE.batch(c, "exact", last) == receipt, "Delete changed a prior receipt")
        if dimension == 4096:
            head = BASE.head(c)
            first = head["reference"]["segments"][0]
            generation = first["reference"]["generation"]["generation"]
            old_sources = [Path(f["uri"]) for f in first["reference"]["snapshot"]["files"]]
            c.execute(
                "SELECT * FROM ducklake_rewrite_vortex_index_segment_to_f('lake','main','items','knn',"
                + quote(generation)
                + f",{head['revision']})"
            )
            require(len(BASE.head(c)["reference"]["segments"]) == 0, "Sparse rewrite did not remove selected B input")
            ack(c, "over", extra, dimension)
            check_seed(c, limit, dimension, 1)
            check_seed(reader, limit, dimension)
            SOURCE.policy(c)
            held = SOURCE.collect(c, objects=2)
            require(
                held["blocked_by_transactions"] and all(p.is_file() for p in old_sources),
                "Admission rewrite bypassed ordinary-reader source protection",
            )
            reader.execute("ROLLBACK")
            reader.close()
            reader = None
            # Explicit bounded collectors only; admission does not require old-view GC.
            for _ in range(32):
                SOURCE.collect(c, objects=2)
                c.execute("SELECT * FROM ducklake_collect_vortex_generations('lake',2,67108864)")
                c.execute("SELECT * FROM ducklake_collect_vortex_builds('lake',2,67108864)")
                if all(not p.exists() for p in old_sources):
                    break
            require(all(not p.exists() for p in old_sources), "Unprotected retired source was not collected")
            search_first(c, dimension, 1)
        c.close()
        c = BASE.connect(root)
        require(WRITE.batch(c, "exact", last) == receipt, "Reopen lost full-capacity receipt")
        reject(c, root, lambda: WRITE.batch(c, "still-over", [row(limit + 1, dimension)]), "physical and inline rows")
        return {
            "dimension": dimension,
            "physical_row_limit": limit,
            "vector_bytes": limit * dimension * 4,
            "rewrite_reuses_slot": dimension == 4096,
        }
    finally:
        if reader is not None:
            reader.close()
        c.close()


def batch_case(root, managed):
    c = bootstrap(root, 2, managed=managed)
    expected = []
    try:
        for operations in (999, 1000, 1001):
            rows = [row(operations * 10000 + i) for i in range(500 if managed else operations)]
            deletes = list(range(-operations, -500)) if managed else None
            if operations <= 1000:
                ack(c, f"ops-{operations}", rows, 2, deletes)
                expected += rows
            else:
                reject(
                    c,
                    root,
                    lambda: KEYED.mutate(c, "ops-over", rows, deletes) if managed else WRITE.batch(c, "ops-over", rows),
                    "1 to 1000",
                )
        reject(
            c, root, lambda: KEYED.mutate(c, "empty", [], []) if managed else WRITE.batch(c, "empty", []), "1 to 1000"
        )
        lengths = []
        for size in (4 * MIB - 1, 4 * MIB, 4 * MIB + 1):
            value = row(size, category="类", payload="向量\x00")
            deletes = [] if managed else None
            value["payload"] += "x" * (size - len(MODEL.canonical([value], 2, deletes)))
            require(len(MODEL.canonical([value], 2, deletes)) == size, "Request padding changed canonical size")
            if size <= 4 * MIB:
                receipt = ack(c, f"bytes-{size}", [value], 2, deletes)
                expected.append(value)
                retry = (
                    KEYED.mutate(c, f"bytes-{size}", [value], [])
                    if managed
                    else WRITE.batch(c, f"bytes-{size}", [value])
                )
                require(retry == receipt, "Exact-size request retry changed")
                changed = dict(value, payload=value["payload"][:-1] + "y")
                reject(
                    c,
                    root,
                    lambda: (
                        KEYED.mutate(c, f"bytes-{size}", [changed], [])
                        if managed
                        else WRITE.batch(c, f"bytes-{size}", [changed])
                    ),
                    "payload mismatch",
                )
            else:
                reject(
                    c,
                    root,
                    lambda: (
                        KEYED.mutate(c, "bytes-over", [value], []) if managed else WRITE.batch(c, "bytes-over", [value])
                    ),
                    "byte budget",
                )
            lengths.append(size)
        require(
            BASE.raw(c) == sorted(expected, key=lambda r: r["id"]),
            "Accepted boundary requests changed typed data or duplicated retries",
        )
        return {"managed": managed, "operation_boundaries": [999, 1000, 1001], "canonical_bytes": lengths}
    finally:
        c.close()


def inline_case(root):
    c = bootstrap(root, 2, managed=True, inline=True)
    try:
        first, second = row(1, category="类", payload="向量\x00"), row(2)
        require([MODEL.row_bytes(first), MODEL.row_bytes(second)] == [82, 72], "Unexpected UTF8 fixture charge")
        budget(c, t_rows=2, t_bytes=154)
        original = ack(c, "first", [first], 2, [])
        ack(c, "second", [second], 2, [])
        reject(c, root, lambda: KEYED.mutate(c, "rows-over", [row(3)]), "inline rows")
        reject(
            c,
            root,
            lambda: KEYED.mutate(c, "bytes-over", [dict(first, payload=first["payload"] + "x")]),
            "inline bytes",
        )
        newer = dict(first, payload="新量\x00")
        require(MODEL.row_bytes(newer) == 82, "Replacement charge changed")
        ack(c, "replace", [newer], 2, [])
        require(
            KEYED.mutate(c, "first", [first]) == original and BASE.raw(c)[0] == newer,
            "Old receipt replay resurrected a version",
        )
        ack(c, "delete", [], 2, [2])
        ack(c, "reused", [row(3)], 2, [])
        require(BASE.raw(c) == [newer, row(3)], "Visible T slots did not follow current keyed rows")
        retained = pending_versions(c)
        require(
            len(retained) == 4 and sum(MODEL.row_bytes(v) for v in retained) == 308,
            "Visible quota reuse erased retained historical charges",
        )
        reject(c, root, lambda: budget(c, t_rows=1, t_bytes=154), "inline rows")
        reject(c, root, lambda: budget(c, t_rows=2, t_bytes=153), "inline bytes")
        return {"visible_rows": 2, "visible_bytes": 154, "retained_version_charges": [82, 72, 82, 72]}
    finally:
        c.close()


def pending_case(root):
    c = bootstrap(root, 1, managed=True, inline=True)
    reader = None
    try:
        budget(c, t_rows=1, t_bytes=MIB)
        receipts = []
        versions = []
        for i in range(64):
            value = row(0, 1, payload=f"{i:02d}" + "x" * (MIB - 68 - 2))
            require(MODEL.row_bytes(value) == MIB, "Pending history fixture charge changed")
            versions.append(value)
            receipts.append(ack(c, f"version-{i}", [value], 1, []))
        extra = row(0, 1, payload="64" + "x" * (MIB - 70))
        retained = pending_versions(c)
        require(
            len(retained) == 64 and sum(MODEL.row_bytes(v) for v in retained) == 64 * MIB,
            "Stored history differs from the independently charged versions",
        )
        require(sorted(retained, key=lambda v: v["payload"]) == versions, "Retained history changed typed versions")
        del retained
        reject(c, root, lambda: KEYED.mutate(c, "pending-over", [extra]), "pending inline bytes")
        for i in (0, 63):
            require(KEYED.mutate(c, f"version-{i}", [versions[i]]) == receipts[i], "Pending-full retry failed")
        require(BASE.raw(c) == [versions[-1]], "Receipt retry changed current pending version")
        reader = c.cursor()
        reader.execute("BEGIN")
        require(BASE.raw(reader) == [versions[-1]], "Old reader did not establish current version")
        ack(c, "delete", [], 1, [0])
        reject(c, root, lambda: KEYED.mutate(c, "empty-but-full", [extra]), "pending inline bytes")
        c.execute("CALL ducklake_flush_inlined_data('lake',table_name=>'items')")
        require(
            REF.rows(c, f"SELECT sum(record_count) FROM {REF.META}.ducklake_data_file WHERE end_snapshot IS NULL")
            == [[64]],
            "Flush did not materialize all ended physical versions",
        )
        ack(c, "after-flush", [extra], 1, [])
        require(
            BASE.raw(c) == [extra] and BASE.raw(reader) == [versions[-1]], "Flush lost current or old-reader versions"
        )
        reader.execute("ROLLBACK")
        reader.close()
        reader = None
        c.close()
        c = BASE.connect(root)
        require(BASE.raw(c) == [extra], "Reopen changed post-flush pending admission")
        return {
            "retained_versions": 64,
            "bytes_per_version": MIB,
            "pending_bytes": 64 * MIB,
            "visible_bytes_before_delete": MIB,
            "flush_allows_next": True,
        }
    finally:
        if reader is not None:
            reader.close()
        c.close()


def index_view(c, rows, source_bytes):
    return query(
        c, "SELECT * FROM ducklake_vortex_index_view('lake','main','items','knn'," + f"{source_bytes},{rows})"
    )[0]


def source_case(root):
    c = bootstrap(root, 2)
    try:
        seed_sql(c, 0, 8, 2)
        actual = sum(p.stat().st_size for p in (root / "data").rglob("*.vortex"))
        require(actual > 0, "No real source fixture")
        reject(c, root, lambda: budget(c, f_rows=7), "unindexed rows")
        budget(c, f_rows=8)
        reject(c, root, lambda: WRITE.batch(c, "f-row-over", [row(8)]), "unindexed rows")
        budget(c)
        reject(c, root, lambda: budget(c, f_bytes=actual - 1), "unindexed bytes")
        for size in (actual, actual + 1):
            budget(c, f_bytes=size)
        budget(c, f_bytes=actual)
        reject(c, root, lambda: WRITE.batch(c, "f-byte-over", [row(8)]), "unindexed bytes")
        for n, size, error in ((7, actual, "row budget"), (8, actual - 1, "byte budget")):
            reject(c, root, lambda: index_view(c, n, size), error)
        for n, size in ((8, actual), (9, actual + 1)):
            require(
                index_view(c, n, size)["source_bytes"] == actual,
                "Read-view source byte accounting differs from real file sizes",
            )
        for limits in ((100001, 64 * MIB), (8, 64 * MIB + 1)):
            reject(c, root, lambda: index_view(c, *limits), "Invalid")
        reject(c, root, lambda: budget(c, t_rows=0), "write budgets must be positive")
        for kwargs in ({"t_bytes": 4 * MIB + 1}, {"f_rows": 100001}, {"f_bytes": 64 * MIB + 1}):
            reject(c, root, lambda: budget(c, **kwargs), "Invalid Vortex write budget")
        return {"rows": 8, "source_bytes": actual, "source_boundaries": [actual - 1, actual, actual + 1]}
    finally:
        c.close()


def lease_case(root, probe):
    c = bootstrap(root, 2, managed=True, inline=True)
    workers = []
    try:
        for part in range(3):
            ack(c, f"part-{part}", [row(part * 65 + i) for i in range(65)], 2, [])
            c.execute("CALL ducklake_flush_inlined_data('lake',table_name=>'items')")
            append(c, root, f"part-{part}", 2)
        head = BASE.head(c)
        require(len(head["reference"]["segments"]) == 3, "Three independent B segments were not published")
        source_bytes = artifact_bytes = 0
        for entry in head["reference"]["segments"]:
            reference = entry["reference"]
            source_bytes += sum(Path(f["uri"]).stat().st_size for f in reference["snapshot"]["files"])
            directory = Path(entry["root"]) / reference["generation"]["generation"]
            manifest = json.loads((directory / "manifest.json").read_text())
            artifact_bytes += sum((directory / "artifacts" / a["path"]).stat().st_size for a in manifest["artifacts"])
        capture = root / "capture.json"
        view = index_view(c, 100000, 64 * MIB)
        capture.write_text(view["admission"])
        require(view["source_bytes"] == source_bytes, "Catalog source sum differs from filesystem")
        for kind, total in (("source", source_bytes), ("artifact", artifact_bytes)):
            for delta in (-1, 0, 1):
                limits = {
                    "max_views": 1,
                    "max_source_bytes": 64 * MIB,
                    "max_artifact_bytes": 64 * MIB,
                    "max_inline_rows": 100000,
                    "max_inline_bytes": 4 * MIB,
                }
                limits["max_" + kind + "_bytes"] = total + delta
                w = VIEW.Worker(SimpleNamespace(root=root, probe=probe), f"{kind}-{delta}", **limits)
                workers.append(w)
                result = w.call(
                    {"op": "open_catalogued", "view": "current", "capture": str(capture), "index_name": "knn"},
                    "byte budget" if delta < 0 else None,
                )
                if delta < 0:
                    require(w.stats() == VIEW.empty_stats(), "Rejected genuine lease retained resources")
                else:
                    require(
                        result["source_bytes"] == source_bytes and result["artifact_bytes"] == artifact_bytes,
                        "Lease aggregate differs from actual authenticated files",
                    )
                    hits = w.call(
                        {
                            "op": "search_mixed",
                            "view": "current",
                            "capture_digest": result["capture_digest"],
                            "request": {"query": [0.0, 0.0], "k": 1, "allow": None},
                        }
                    )
                    require(
                        len(hits["hits"]) == 1 and hits["hits"][0]["row"] == row(0),
                        "Genuine lease at its byte boundary lost typed ANN search",
                    )
                    w.call(
                        {"op": "open_catalogued", "view": "second", "capture": str(capture), "index_name": "knn"},
                        "count budget",
                    )
                    require(
                        w.close_view("current") == VIEW.empty_stats(), "Closing lease did not return all reservations"
                    )
                    w.call(
                        {"op": "open_catalogued", "view": "replacement", "capture": str(capture), "index_name": "knn"}
                    )
                    w.close_view("replacement")
                w.finish()
        search_first(c, 2)
        # A full B count still permits F. A fourth build must not publish a new revision.
        ack(c, "fourth-input", [row(195 + i) for i in range(65)], 2, [])
        c.execute("CALL ducklake_flush_inlined_data('lake',table_name=>'items')")
        for _ in range(8):
            c.execute("SELECT * FROM ducklake_collect_vortex_builds('lake',2,67108864)")
        before_head, before_rows = BASE.head(c), BASE.raw(c)
        before_state = state(c, root)
        WRITE.rejected(lambda: append(c, root, "fourth", 2), "segment")
        require(
            BASE.head(c) == before_head and BASE.raw(c) == before_rows and not (root / "indexes/fourth.json").exists(),
            "Rejected fourth B changed publication/business state",
        )
        for _ in range(8):
            c.execute("SELECT * FROM ducklake_collect_vortex_builds('lake',2,67108864)")
        require(
            not query(c, "SELECT * FROM ducklake_vortex_build_status('lake')"),
            "Rejected build ownership was not explicitly reclaimed",
        )
        require(
            state(c, root) == before_state, "Explicit failed-build cleanup did not restore admission metadata and files"
        )
        require(BASE.raw(c) == [row(i) for i in range(260)], "Three B plus admitted F changed typed rows")
        return {
            "segments": 3,
            "source_bytes": source_bytes,
            "artifact_bytes": artifact_bytes,
            "lease_boundary_cases": 6,
            "fourth_segment_rejected": True,
        }
    finally:
        for w in workers:
            if w.process.poll() is None:
                w.finish()
        c.close()


def recover_native(root, mode):
    expected = json.loads((root / "expected.json").read_text())
    sealed = json.loads((root / "files.json").read_text())
    seed = [[i, [float(i), 0.0], "A", "seed-" + str(i)] for i in range(256)]
    if mode == "race":
        extra = [r for r in expected["rows"] if r[0] >= 20000]
        require(
            len(extra) == 1 and extra[0][0] in (20000, 20001) and extra[0][1:] == [[0.5, 0.0], None, None],
            "Concurrent final-slot model differs",
        )
        model = seed + extra
    else:
        model = seed if mode == "before_commit" else REF.model()
    require(expected["rows"] == model, "Native expected state differs from independent business model")
    c = BASE.connect(root)
    try:
        require(
            REF.snapshot(c) == expected and REF.files(root) == sealed,
            "Reopen/recovery changed committed admission state or identities",
        )
        REF.check_query(c, model)
        if mode == "before_commit":
            KEYED.mutate(
                c,
                "change",
                [
                    dict(row(0), embedding=[0.125, 0.0], category="A", payload="更新"),
                    dict(row(10000), embedding=[0.25, 0.0]),
                ],
                [1, 99999],
            )
        if mode != "race":
            require(REF.snapshot(c)["rows"] == REF.model(), "Retry after recovery did not fill exactly the final slots")
            reject(c, root, lambda: KEYED.mutate(c, "blocked", [dict(row(30000), embedding=[0.5, 0.0])]), "inline rows")
        # Receipt retention and current data capacity are separate domains.
        seed_input = [dict(zip(("id", "embedding", "category", "payload"), r)) for r in seed]
        current = state(c, root)
        original = KEYED.mutate(c, "seed", seed_input)
        require(
            original["row_count"] == 256 and state(c, root) == current,
            "Full-capacity seed replay changed current versions",
        )
        retired = EPOCH.advance(c, 1)
        require(
            retired["request_epoch"] == 2 and EPOCH.status(c)["retained_requests"] == 0,
            "Receipt epoch did not retire independently",
        )
        reject(
            c,
            root,
            lambda: KEYED.mutate(c, "new-epoch-full", [dict(row(30000), embedding=[0.5, 0.0])], epoch=2),
            "inline rows",
        )
        reject(c, root, lambda: KEYED.mutate(c, "seed", seed_input, epoch=1), "EXPIRED_REQUEST")
        return {
            "mode": mode,
            "wal_bytes_before_recovery": json.loads((root / "sealed.json").read_text())["wal_bytes"],
            "metadata_tables": len(REF.METADATA_TABLES),
        }
    finally:
        c.close()


def native_cases(driver, root, cpus):
    results = []
    for mode in ("exact", "cancel", "conflict", "race", "pause-before_commit", "pause-after_commit", "pause-rejected"):
        case = root / ("native-" + mode)
        case.mkdir(mode=0o700)
        env = REF.native_environment(case)
        with (case / "driver.log").open("w") as log:
            p = subprocess.Popen(
                [str(driver), str(case), "capacity-" + mode], env=env, stdout=log, stderr=subprocess.STDOUT
            )
            try:
                deadline = time.monotonic() + 180
                while not (case / "checkpoint").is_file():
                    require(
                        p.poll() is None and time.monotonic() < deadline,
                        "Native capacity boundary not reached: " + str(case / "driver.log"),
                    )
                    time.sleep(0.01)
                stage = (case / "checkpoint").read_text()
                if mode.startswith("pause-"):
                    require(stage == "capacity-" + mode.removeprefix("pause-"), "Wrong native crash boundary")
                    wal = REF.wal_bytes(case)
                    require(wal > 0, "Crash fixture did not exercise native WAL")
                    (case / "sealed.json").write_text(json.dumps({"wal_bytes": wal}))
                    (case / "files.json").write_text(json.dumps(REF.files(case)))
                    p.kill()
                    require(p.wait(timeout=20) == -9, "Capacity crash did not use SIGKILL")
                else:
                    require(p.wait(timeout=30) == 0 and stage == "capacity-complete", "Native capacity fixture failed")
                    (case / "sealed.json").write_text(json.dumps({"wal_bytes": REF.wal_bytes(case)}))
                    (case / "files.json").write_text(json.dumps(REF.files(case)))
            finally:
                if p.poll() is None:
                    p.kill()
                    p.wait(timeout=20)
        recovery = mode.removeprefix("pause-") if mode.startswith("pause-") else mode
        command = [sys.executable, "-I", str(SCRIPT), "--recover", str(case), "--case", recovery]
        with (case / "recovery.log").open("w") as log:
            done = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, env=env, timeout=120)
        require(done.returncode == 0, "Capacity recovery failed: " + str(case / "recovery.log"))
        result = json.loads((case / "recovery.json").read_text())
        require(
            result["affinity"] == cpus and result["thread_environment"] == PIN.thread_environment(),
            "Recovery changed CPU/thread contract",
        )
        results.append(result)
        print("native/recovery", mode, "passed", flush=True)
        shutil.rmtree(case / "extensions")
    return results


def parser_cases(driver, root):
    fixtures = []
    for dimension in (1, 167, 168, 4096):
        limit = MODEL.row_limit(dimension)
        for delta in (-1, 0, 1):
            fixtures.append(
                (f"rows-{dimension}-{delta}", MODEL.manifest(dimension, [limit + delta]), delta <= 0, "row")
            )
            base, remainder = divmod(limit + delta, 3)
            counts = [base + (i < remainder) for i in range(3)]
            fixtures.append(
                (f"global-three-B-{dimension}-{delta}", MODEL.manifest(dimension, counts), delta <= 0, "row")
            )
    for count in (0, 2, 3, 4):
        fixtures.append((f"segments-{count}", MODEL.manifest(2, [65] * count), count <= 3, "segment"))
    for size in (MIB - 1, MIB, MIB + 1):
        fixtures.append((f"envelope-{size}", MODEL.sized_manifest(size), size <= MIB, "budget"))
    results = []
    for name, value, accepted, error in fixtures:
        case = root / ("parser-" + name)
        case.mkdir(mode=0o700)
        encoded = MODEL.compact(value).encode()
        (case / "input.json").write_bytes(encoded)
        result = subprocess.run(
            [str(driver), str(case), "capacity-parse"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=60,
        )
        (case / "driver.log").write_text(result.stdout)
        require((result.returncode == 0) == accepted, "Native envelope parser disagrees with model: " + str(case))
        if accepted:
            actual = json.loads((case / "parsed.json").read_text())
            n = sum(f["row_count"] for s in value["segments"] for f in s["reference"]["snapshot"]["files"])
            require(
                actual
                == {
                    "rows": n,
                    "dimension": value["dimension"],
                    "segments": len(value["segments"]),
                    "manifest_bytes": len(encoded),
                },
                "Canonical envelope accounting differs",
            )
        else:
            require(error in result.stdout.lower(), "Parser failed for an unrelated reason")
        results.append(
            {
                "name": name,
                "accepted": accepted,
                "bytes": len(encoded),
                "scope": "native envelope parser; no filesystem publication",
            }
        )
    return results


def store_cases(probe, root):
    output = root / "store.json"
    result = subprocess.run(
        [str(probe), "capacity-store", str(root / "opaque-store"), str(output)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=180,
    )
    (root / "store.log").write_text(result.stdout)
    require(result.returncode == 0, "Local-store capacity probe failed: " + str(root / "store.log"))
    actual = json.loads(output.read_text())
    require(actual["limits"] == {"artifact": 64 * MIB, "manifest": MIB}, "Store limits changed")
    expected = [
        (kind, size, size <= limit)
        for kind, limit in (("artifact", 64 * MIB), ("manifest", MIB))
        for size in (limit - 1, limit, limit + 1)
    ]
    expected.append(("aggregate", 64 * MIB + 2, False))
    require(
        [(c["kind"], c["bytes"], c["accepted"]) for c in actual["cases"]] == expected,
        "Store boundaries disagree with independent model",
    )
    for case in actual["cases"]:
        if not case["accepted"]:
            require("byte" in case["error"].lower(), "Store rejected for an unrelated reason")
    return actual


def receipt_case(root):
    checks = []
    EPOCH.exercise(root / "epochs", checks)
    return {
        "checks": checks,
        "catalog_receipt_limit": 100000,
        "fixture": "real empty-delete receipt copied into fixed 100000-row catalog; not 100000 API writes",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--driver", type=Path)
    parser.add_argument("--probe", type=Path)
    parser.add_argument("--cpus", type=PIN.parse_cpus)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--child", type=Path)
    parser.add_argument("--case")
    parser.add_argument("--recover", type=Path)
    args = parser.parse_args()
    if args.recover:
        result = recover_native(args.recover, args.case)
        result.update(affinity=sorted(os.sched_getaffinity(0)), thread_environment=PIN.thread_environment())
        (args.recover / "recovery.json").write_text(json.dumps(result, indent=2) + "\n")
        return
    if args.child:
        functions = {
            "append": lambda: batch_case(args.child, False),
            "mutate": lambda: batch_case(args.child, True),
            "inline": lambda: inline_case(args.child),
            "pending": lambda: pending_case(args.child),
            "source": lambda: source_case(args.child),
            "lease": lambda: lease_case(args.child, args.probe),
            "receipts": lambda: receipt_case(args.child),
        }
        started = time.monotonic()
        try:
            result = (
                physical_case(args.child, int(args.case.split("-")[1]))
                if args.case.startswith("physical-")
                else functions[args.case]()
            )
            report = {"status": "passed", "result": result}
        except BaseException as error:
            report = {"status": "failed", "error": f"{type(error).__name__}: {error}"}
            raise
        finally:
            report.update(
                case=args.case,
                affinity=sorted(os.sched_getaffinity(0)),
                thread_environment=PIN.thread_environment(),
                seconds_observation_only=time.monotonic() - started,
            )
            (args.child / "report.json").write_text(json.dumps(report, indent=2) + "\n")
        return
    require(
        args.driver and args.probe and args.cpus and args.output_dir,
        "driver, probe, CPUs and output directory are required",
    )
    root = args.output_dir.resolve()
    root.mkdir(mode=0o700, parents=True, exist_ok=False)
    args.driver, args.probe = args.driver.resolve(strict=True), args.probe.resolve(strict=True)
    tracked = [
        SCRIPT,
        *SCRIPT.parent.glob("*.py"),
        *SCRIPT.parent.glob("native_vortex_*.hpp"),
        *SCRIPT.parent.glob("native_vortex_*.cpp"),
        *SCRIPT.parent.joinpath("vortex_index_probe/src").glob("*.rs"),
    ]
    hashes = {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in set(tracked)}
    threads = PIN.thread_environment()
    import vane._native as native

    report = {
        "status": "running",
        "original_affinity": sorted(os.sched_getaffinity(0)),
        "thread_environment": threads,
        "source_sha256": hashes,
        "driver_sha256": hashlib.sha256(args.driver.read_bytes()).hexdigest(),
        "probe_sha256": hashlib.sha256(args.probe.read_bytes()).hexdigest(),
        "native_sha256": hashlib.sha256(Path(native.__file__).read_bytes()).hexdigest(),
        "cases": [],
        "acceptance": "Fixed-input functional capacity boundaries; elapsed times are observations, no throughput/latency gate; store/parser cases are explicitly separate from ANN publication.",
    }
    started = time.monotonic()
    try:
        with PIN.pinned_affinity(args.cpus):
            report["applied_affinity"] = sorted(os.sched_getaffinity(0))
            report["store"] = store_cases(args.probe, root)
            report["parser"] = parser_cases(args.driver, root)
            for name in (
                "physical-1",
                "physical-167",
                "physical-168",
                "physical-4096",
                "append",
                "mutate",
                "inline",
                "pending",
                "source",
                "lease",
                "receipts",
            ):
                case = root / name
                case.mkdir(mode=0o700)
                env = REF.native_environment(case)
                with (case / "child.log").open("w") as log:
                    done = subprocess.run(
                        [
                            sys.executable,
                            "-I",
                            str(SCRIPT),
                            "--child",
                            str(case),
                            "--case",
                            name,
                            "--probe",
                            str(args.probe),
                        ],
                        env=env,
                        stdout=log,
                        stderr=subprocess.STDOUT,
                        timeout=900,
                    )
                require(done.returncode == 0, "Capacity case failed: " + str(case / "child.log"))
                result = json.loads((case / "report.json").read_text())
                require(
                    result["affinity"] == args.cpus and result["thread_environment"] == threads,
                    "Child CPU/thread contract changed",
                )
                report["cases"].append(result)
                # Reproducible test provider caches are separate from source/index/WAL evidence.
                for cache in (case / "extensions", case / "epochs/extensions"):
                    if cache.is_dir():
                        shutil.rmtree(cache)
                print("capacity", name, "passed", flush=True)
                (root / "report.json").write_text(json.dumps(report, indent=2) + "\n")
            report["native"] = native_cases(args.driver, root, args.cpus)
            require(
                PIN.thread_environment() == threads
                and all(hashlib.sha256(p.read_bytes()).hexdigest() == hashes[str(p)] for p in set(tracked)),
                "Executed source/thread contract changed",
            )
            require(
                hashlib.sha256(args.driver.read_bytes()).hexdigest() == report["driver_sha256"]
                and hashlib.sha256(args.probe.read_bytes()).hexdigest() == report["probe_sha256"]
                and hashlib.sha256(Path(native.__file__).read_bytes()).hexdigest() == report["native_sha256"],
                "Executed native/probe identity changed",
            )
        report["status"] = "passed"
    except BaseException as error:
        report.update(status="failed", error=f"{type(error).__name__}: {error}")
        raise
    finally:
        report.update(
            restored_affinity=sorted(os.sched_getaffinity(0)), seconds_observation_only=time.monotonic() - started
        )
        require(report["restored_affinity"] == report["original_affinity"], "CPU affinity was not restored")
        (root / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({k: report[k] for k in ("status", "applied_affinity")}))


if __name__ == "__main__":
    main()
