#!/usr/bin/env python3
"""Qualify dimensional/query contracts without a latency or throughput gate."""
from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import importlib.util
import json
import math
import os
from pathlib import Path
import subprocess
import sys
import sysconfig
import time


def load(name, filename):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(filename))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


KEYED = load("matrix_keyed", "test_vane_vortex_keyed_writes.py")
PIN = load("matrix_affinity", "test_vane_vortex_source_gc_affinity.py")
BASE, WRITE = KEYED.BASE, KEYED.WRITE
query, require, quote = BASE.query, BASE.require, BASE.quote
DIMENSIONS = (1, 8, 128, 4096)
FILTERS = (None, "A", "a", "", "稀有-'类别", "absent")
BUSINESS = ("id", "embedding", "category", "payload")
INLINE_SQL = Path(__file__).resolve().parents[1] / "sql/data_inlining/float_exact_roundtrip.test"


def vector(ordinal, dimension):
    return [
        (ordinal - 64) / 128 + (column % 7 - 3) / 32 + ((int(ordinal) * (column + 1) * 17) % 31) / 65536
        for column in range(dimension)
    ]


def rows(start, count, dimension):
    categories = (None, "A", "a", "", "稀有-'类别")
    return [
        {
            "id": i,
            "embedding": vector(i, dimension),
            "category": categories[i % len(categories)],
            "payload": None if i % 7 == 0 else f"向量-'\x00-{i}",
        }
        for i in range(start, start + count)
    ]


def ranked(model, target, category, k):
    eligible = [r for r in model.values() if category is None or r["category"] == category]
    return sorted(eligible, key=lambda r: (math.fsum((a - b) ** 2 for a, b in zip(r["embedding"], target)), r["id"]))[
        :k
    ]


def check_hits(hits, model, target, category, k, exact):
    expected = ranked(model, target, category, k)
    require(len(hits) <= k and len({r["id"] for r in hits}) == len(hits), "Excess or duplicate hits")
    for hit in hits:
        source = model.get(hit["id"])
        require(source is not None and all(hit[key] == source[key] for key in BUSINESS), "Wrong visible typed version")
        require(category is None or source["category"] == category, "Filter applied after top-k")
        require(len(hit["embedding"]) == len(target), "Returned vector dimension changed")
        distance = math.fsum((a - b) ** 2 for a, b in zip(source["embedding"], target))
        require(math.isclose(hit["distance"], distance, rel_tol=1e-5, abs_tol=1e-4), "Squared-L2 mismatch")
    require([r["distance"] for r in hits] == sorted(r["distance"] for r in hits), "Unranked results")
    recall = len({r["id"] for r in hits} & {r["id"] for r in expected}) / len(expected) if expected else 1.0
    require(recall >= (1.0 if exact else 0.8), f"Recall contract failed: {recall}")
    require(bool(expected) or not hits, "Empty eligible set returned hits")
    return recall


def inline_roundtrip(root):
    import vane

    root.mkdir(mode=0o700)
    (root / "fixtures").mkdir(mode=0o700)
    previous = Path.cwd()
    connection = vane.connect(
        ":memory:",
        config={
            "threads": "2",
            "autoinstall_known_extensions": "false",
            "autoload_known_extensions": "false",
            "extension_directory": str(root / "extensions"),
        },
    )
    checked = 0
    try:
        os.chdir(root)
        connection.execute("LOAD parquet")
        vane.load_installed_extension("ducklake", connection=connection)
        sql = INLINE_SQL.read_text().replace("__WORKING_DIRECTORY__", str(root)).replace("__TEST_DIR__", "fixtures")
        for block in sql.split("\n\n"):
            lines = [line for line in block.splitlines() if line and not line.startswith("#")]
            if not lines or lines[0].startswith("require "):
                continue
            header = lines[0]
            require(header in ("statement ok", "query I"), "Unexpected inline regression directive")
            parts = "\n".join(lines[1:]).split("\n----\n", 1)
            result = connection.execute(parts[0])
            if header == "query I":
                require(
                    len(parts) == 2 and [str(r[0]) for r in result.fetchall()] == parts[1].strip().splitlines(),
                    "Inline floating round-trip regression",
                )
            checked += 1
        require(checked == 11, "Incomplete inline regression fixture")
        return {
            "status": "passed",
            "assertion_blocks": checked,
            "source_sha256": hashlib.sha256(INLINE_SQL.read_bytes()).hexdigest(),
        }
    finally:
        connection.close()
        os.chdir(previous)


def dense_l2(root):
    root.mkdir(mode=0o700)
    connection = bootstrap(root, 4096)
    try:
        first = [4096.0] + [1.0] * 4095
        values = [
            {"id": i, "embedding": v, "category": "A", "payload": "dense"}
            for i, v in enumerate((first, list(reversed(first))))
        ]
        KEYED.mutate(connection, "dense-l2", values)
        model = {r["id"]: r for r in values}
        target = [0.0] * 4096
        result = search(connection, target, 2)
        # Exact real sum is 2**24 + 4095, whose FLOAT rounding is 2**24 + 4096.
        expected = float((1 << 24) + 4096)
        require(
            len(result) == 2 and all(r["distance"] == expected for r in result),
            "Dense L2 lost small terms or depended on component order",
        )
        check_hits(result, model, target, None, 2, exact=True)
        diagnostic = search(connection, target, 2, diagnostics=True)
        require(len(diagnostic) == 1 and diagnostic[0]["hits"] == result, "Dense L2 diagnostics changed hits")
        WRITE.rejected(lambda: search(connection, [3.4028234663852886e38] * 4096, 2), "Squared L2 distance overflow")
        require(search(connection, target, 2) == result, "Overflow rejection damaged the next query")
        return {
            "status": "passed",
            "dimension": 4096,
            "rows": 2,
            "expected_distance": expected,
            "component_order_invariant": True,
            "overflow_rejected": True,
        }
    finally:
        connection.close()


def search(connection, target, k, category=None, diagnostics=False):
    function = "ducklake_vortex_search_diagnostics" if diagnostics else "ducklake_vortex_search"
    sql = f"SELECT * FROM {function}('lake','main','items','knn',?::FLOAT[],?"
    parameters = [target, k]
    if category is not None:
        sql += ",category=>?"
        parameters.append(category)
    return WRITE.query_parameters(connection, sql + ")", parameters)


def qualify(connection, model, dimension, phase, segments, report, scratch):
    require(
        BASE.raw(connection) == sorted(model.values(), key=lambda r: r["id"]), "Native rows diverged from client model"
    )
    require(len(BASE.head(connection)["reference"]["segments"]) == segments, "Unexpected B count")
    snapshot = query(connection, "SELECT max(snapshot_id) AS n FROM __ducklake_metadata_lake.ducklake_snapshot")[0]["n"]
    samples = []
    for category in FILTERS:
        for k in (1, 64):
            target = vector(64.125, dimension)
            hits = search(connection, target, k, category)
            diagnostic = search(connection, target, k, category, diagnostics=True)
            require(len(diagnostic) == 1 and diagnostic[0]["hits"] == hits, "Diagnostics changed ranked typed hits")
            details = json.loads(diagnostic[0]["diagnostics"])
            require(details["native_capture"]["snapshot_id"] == snapshot, "Query used a different committed snapshot")
            routes = details["routes"]
            require(routes["ann_calls"] == segments, "Wrong ANN route count")
            recall = check_hits(hits, model, target, category, k, exact=segments == 0)
            samples.append({"category": category, "k": k, "ids": [r["id"] for r in hits], "recall": recall})
            require(not list(scratch.iterdir()), "SQL search retained private query staging")
    require(
        query(connection, "SELECT max(snapshot_id) AS n FROM __ducklake_metadata_lake.ducklake_snapshot")[0]["n"]
        == snapshot,
        "Read-only qualification advanced the snapshot",
    )
    report["phases"].append(
        {
            "phase": phase,
            "rows": len(model),
            "snapshot_id": snapshot,
            "segments": segments,
            "routes": routes,
            "capture": details["native_capture"],
            "queries": samples,
        }
    )
    print(dimension, phase, "passed", flush=True)


def bootstrap(root, dimension):
    (root / "indexes").mkdir(mode=0o700)
    connection = BASE.connect(root)
    for sql in (
        "CREATE TABLE lake.items(id BIGINT,embedding FLOAT[],category VARCHAR,payload VARCHAR)",
        "CALL ducklake_set_option('lake','experimental_data_file_format','vortex',table_name=>'items')",
        "SELECT * FROM ducklake_initialize_vortex_indexes('lake')",
        "SELECT * FROM ducklake_initialize_vortex_optimizer('lake')",
        "SELECT * FROM ducklake_initialize_vortex_writes('lake')",
        "SELECT * FROM ducklake_create_vortex_index('lake','main','items','knn',"
        + quote(root / "indexes")
        + f",{dimension})",
    ):
        connection.execute(sql)
    KEYED.manage(connection)
    return connection


def append_segment(connection, root, name, dimension):
    revision = BASE.head(connection)["revision"]
    connection.execute(
        "SELECT * FROM ducklake_append_vortex_index_segment('lake','main','items','knn',"
        + quote(root / "indexes" / (name + ".json"))
        + f",{dimension},{revision})"
    )


def rejections(connection, dimension, model, report):
    target = vector(64.125, dimension)
    failures = [
        (lambda: search(connection, target, 0), "k must be"),
        (lambda: search(connection, target, 65), "k must be"),
        (lambda: search(connection, [], 1), "dimension"),
        (lambda: search(connection, [0.0] * 4097, 1), "dimension"),
        (lambda: search(connection, [0.0] * (2 if dimension == 1 else dimension - 1), 1), "dimension"),
        (lambda: search(connection, [float("nan")] * dimension, 1), "finite"),
        (lambda: search(connection, [float("inf")] * dimension, 1), "finite"),
        (lambda: search(connection, [None] * dimension, 1), "non-NULL"),
        (lambda: search(connection, target, 1, "x" * 65537), "byte budget"),
        (
            lambda: WRITE.query_parameters(
                connection,
                "SELECT * FROM ducklake_vortex_search('lake','main','items','knn',?::FLOAT[],1,category=>NULL)",
                [target],
            ),
            "filter cannot be NULL",
        ),
    ]
    for call, message in failures:
        WRITE.rejected(call, message)
    for name, changed, deleted, message in (
        ("bad-dimension", [dict(rows(200, 1, dimension)[0], embedding=[])], [0], "dimension"),
        ("bad-finite", [dict(rows(200, 1, dimension)[0], embedding=[float("inf")] * dimension)], [0], "finite"),
        ("bad-count", [], list(range(1001)), "1 to 1000"),
        ("bad-bytes", [dict(rows(200, 1, dimension)[0], payload="x" * (4 * 1024 * 1024))], [0], "byte budget"),
    ):
        WRITE.rejected(lambda: KEYED.mutate(connection, name, changed, deleted), message)
        WRITE.check_no_request(connection, name)
    require(
        BASE.raw(connection) == sorted(model.values(), key=lambda r: r["id"]), "Rejected operation changed current rows"
    )
    report["rejections"] = len(failures) + 4


def exercise(root, dimension, report, scratch):
    root.mkdir(mode=0o700)
    connection = bootstrap(root, dimension)
    model = {}
    receipts = []

    def mutate(name, changed, removed=()):
        receipt = KEYED.mutate(connection, name, changed, removed)
        for key in removed:
            model.pop(key, None)
        model.update((r["id"], r) for r in changed)
        receipts.append((name, changed, list(removed), receipt))

    def qualify_phase(name, segments):
        qualify(connection, model, dimension, name, segments, report, scratch)
        (root / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")

    def flush():
        connection.execute("CALL ducklake_flush_inlined_data('lake',table_name=>'items')")

    try:
        qualify_phase("empty", 0)
        mutate("seed-64", rows(0, 64, dimension))
        qualify_phase("T-64", 0)
        flush()
        qualify_phase("F-64-before-ANN", 0)
        mutate("seed-65", rows(64, 1, dimension))
        flush()
        append_segment(connection, root, "first", dimension)
        first = BASE.head(connection)["reference"]["segments"][0]
        frozen = BASE.signature(first)
        require(bool(frozen), "B1 has no sealed generation")
        qualify_phase("B1-65", 1)
        mutate("second-65", rows(65, 65, dimension))
        flush()
        append_segment(connection, root, "second", dimension)
        segments = BASE.head(connection)["reference"]["segments"]
        require(segments[0] == first and BASE.signature(first) == frozen, "B2 rebuilt B1")
        signatures = [BASE.signature(s) for s in segments]
        qualify_phase("B1-plus-B2", 2)
        mutate("F-four", rows(130, 4, dimension))
        flush()
        mutate("T-three", rows(134, 3, dimension))
        qualify_phase("B1-B2-F-T", 2)
        routes = report["phases"][-1]["routes"]
        require(routes["indexed_files"] and routes["scan_files"] and routes["inline_rows"] == 3, "Missing B/F/T route")
        reader = connection.cursor()
        try:
            reader.execute("BEGIN")
            old = BASE.raw(reader)
            changed = [
                dict(rows(0, 1, dimension)[0], embedding=vector(300, dimension), payload="B1新版本"),
                dict(rows(130, 1, dimension)[0], embedding=vector(64.125, dimension), payload="F新版本"),
                dict(rows(90000, 1, dimension)[0], embedding=vector(64.0625, dimension), category=None),
            ]
            mutate("replace-delete", changed, [1, 65, 131, 135])
            require(BASE.raw(reader) == old, "Mutation changed an existing native transaction")
            reader.execute("COMMIT")
        finally:
            reader.close()
        qualify_phase("replaced-and-deleted", 2)
        require(
            BASE.head(connection)["reference"]["segments"] == segments
            and [BASE.signature(s) for s in segments] == signatures,
            "Mutation rebuilt a B generation",
        )
        rejections(connection, dimension, model, report)
        for request, changed, removed, receipt in receipts:
            require(KEYED.mutate(connection, request, changed, removed) == receipt, "Replay changed a receipt")
        qualify_phase("old-request-replays", 2)
        connection.close()
        connection = BASE.connect(root)
        qualify_phase("reopened", 2)
        report.update(
            status="passed",
            receipts=len(receipts),
            minimum_recall=min(q["recall"] for phase in report["phases"] for q in phase["queries"]),
        )
    finally:
        connection.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--cpus", required=True, type=PIN.parse_cpus)
    parser.add_argument("--case", choices=("inline", "l2", *map(str, DIMENSIONS)), help=argparse.SUPPRESS)
    args = parser.parse_args()
    root = args.output_dir.resolve()
    if args.case:
        result = {
            "status": "running",
            "phases": [],
            "affinity": args.cpus,
            "thread_environment": PIN.thread_environment(),
        }
        if args.case not in ("inline", "l2"):
            result["dimension"] = int(args.case)
        try:
            with PIN.pinned_affinity(args.cpus):
                if args.case == "inline":
                    result.update(inline_roundtrip(root))
                elif args.case == "l2":
                    result.update(dense_l2(root))
                else:
                    exercise(root, int(args.case), result, Path(os.environ["TMPDIR"]))
        except BaseException as error:
            result.update(status="failed", error=f"{type(error).__name__}: {error}")
            raise
        finally:
            if root.is_dir():
                (root / "report.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
        return
    root.mkdir(mode=0o700)
    scratch = root / "private"
    scratch.mkdir(mode=0o700)
    os.environ["TMPDIR"] = str(scratch)
    import vane
    import vane._native
    from vane import runners

    require(
        Path(vane.__file__).resolve().is_relative_to(Path(sysconfig.get_path("purelib")).resolve()),
        "Expected non-editable installed Vane",
    )
    require(runners.get_or_infer_runner_type() == "local-fast", "Expected local-fast runner")
    native = Path(vane._native.__file__).resolve()
    tracked = [
        Path(__file__),
        Path(KEYED.__file__),
        Path(WRITE.__file__),
        Path(BASE.__file__),
        Path(BASE.SEARCH.__file__),
        Path(PIN.__file__),
        INLINE_SQL,
        Path(__file__).resolve().parents[2] / "src/common/ducklake_util.cpp",
        Path(__file__).resolve().parents[2] / "src/vortex_query/src/mixed.rs",
    ]
    source_hashes = {str(p.resolve()): hashlib.sha256(p.read_bytes()).hexdigest() for p in tracked}
    threads = PIN.thread_environment()
    original = sorted(os.sched_getaffinity(0))
    require(set(args.cpus) <= set(original), "CPU set unavailable")
    report = {
        "status": "running",
        "mode": "functional_only",
        "dimensions": list(DIMENSIONS),
        "acceptance": "Exact typed visibility/distances and zero-B top-k; fixture ANN recall >=0.8. No latency, throughput or stable-load gate.",
        "deferred": "R4 A/A, performance A/B, sustained-load qualification; all earlier failures/withdrawals remain.",
        "native_sha256": hashlib.sha256(native.read_bytes()).hexdigest(),
        "native_path": str(native),
        "source_sha256": source_hashes,
        "vane_version": importlib.metadata.version("vane-ai"),
        "original_affinity": original,
        "thread_environment": threads,
        "cases": [],
    }
    started = time.monotonic()

    def run_case(kind):
        name = "inline-roundtrip" if kind == "inline" else "dense-l2" if kind == "l2" else "dimension-" + kind
        directory = root / name
        with (root / (name + ".log")).open("w") as log:
            child = subprocess.run(
                [
                    sys.executable,
                    "-I",
                    str(Path(__file__).resolve()),
                    "--case",
                    kind,
                    "--cpus",
                    ",".join(map(str, args.cpus)),
                    "--output-dir",
                    str(directory),
                ],
                stdout=log,
                stderr=subprocess.STDOUT,
            )
        path = directory / "report.json"
        result = json.loads(path.read_text()) if path.is_file() else {"status": "failed"}
        result.update(exit_code=child.returncode, report=str(path))
        if kind == "inline":
            report["inline_roundtrip"] = result
        elif kind == "l2":
            report["dense_l2"] = result
        else:
            report["cases"].append(result)
        require(
            child.returncode == 0 and result["status"] == "passed", f"Functional child failed: {root / (name + '.log')}"
        )
        require(
            result["affinity"] == args.cpus and result["thread_environment"] == threads,
            "Child changed CPU/thread settings",
        )
        print(name, "passed", flush=True)

    try:
        with PIN.pinned_affinity(args.cpus):
            report["applied_affinity"] = sorted(os.sched_getaffinity(0))
            run_case("inline")
            run_case("l2")
            for dimension in DIMENSIONS:
                run_case(str(dimension))
                require(PIN.thread_environment() == threads, "Thread settings changed")
            require(not list(scratch.iterdir()), "Private staging leaked")
            require(
                all(hashlib.sha256(p.read_bytes()).hexdigest() == source_hashes[str(p.resolve())] for p in tracked),
                "Test source changed during qualification",
            )
        report.update(status="passed", restored_affinity=sorted(os.sched_getaffinity(0)))
    except BaseException as error:
        report.update(status="failed", error=f"{type(error).__name__}: {error}")
        raise
    finally:
        report["restored_affinity"] = sorted(os.sched_getaffinity(0))
        report["seconds_observation_only"] = time.monotonic() - started
        (root / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({"status": report["status"], "dimensions": list(DIMENSIONS)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
