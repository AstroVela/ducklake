#!/usr/bin/env python3
"""Qualify B/F/T top-k with a reused Vortex index and one retained DuckLake view."""

from __future__ import annotations

import argparse
import importlib.util
import json
import math
from pathlib import Path
import shutil
import time


SPEC = importlib.util.spec_from_file_location(
    "native_read_view_proof", Path(__file__).with_name("test_native_vortex_read_view.py")
)
assert SPEC is not None and SPEC.loader is not None
READ = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(READ)
BASE = READ.BASE
require = BASE.require


def write_json(path: Path, value) -> Path:
    path.write_text(json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n")
    return path


def ref(row: dict) -> dict:
    if row["filename"].startswith("ducklake_inlined_data_"):
        return {"kind": "inline", "source": row["filename"], "row_id": row["logical_row_id"]}
    return {"kind": "file", "file_id": row["file_id"], "row_offset": row["row_offset"]}


def key(row_ref: dict) -> tuple:
    if row_ref["kind"] == "file":
        return 0, row_ref["file_id"], row_ref["row_offset"]
    return 1, row_ref["source"], row_ref["row_id"]


def capture(proof, directory: Path, name: str) -> tuple[Path, list[dict]]:
    files, rows = proof.capture_rows(directory)
    mixed = {
        "format_version": 1,
        "file_capture": files,
        "inline_rows": [
            {"row_ref": ref(row), "row": READ.business(row)}
            for row in rows
            if row["filename"].startswith("ducklake_inlined_data_")
        ],
    }
    return write_json(proof.root / f"{name}.mixed.json", mixed), rows


class Worker(READ.Worker):
    def open(self, name: str, capture: Path, reference: Path, expected: str | None = None):
        result = self.call(
            {"op": "open_mixed", "view": name, "capture": str(capture), "reference": str(reference)}, expected
        )
        if result is not None:
            self.digests[name] = result["capture_digest"]
        return result

    def search(self, name: str, query: list[float], allowed=None, k: int = 10, expected: str | None = None):
        return self.call(
            {
                "op": "search_mixed",
                "view": name,
                "capture_digest": self.digests[name],
                "request": {"query": query, "k": k, "allow": allowed},
            },
            expected,
        )

    def fetch(self, name: str, rows: list[dict], expected: str | None = None):
        return self.call(
            {"op": "fetch_mixed", "view": name, "capture_digest": self.digests[name], "rows": rows}, expected
        )


def qualify(
    result: dict, rows: list[dict], query: list[float], k: int = 10, allowed=None, recall_threshold: float = 0.8
) -> float:
    allowed_keys = None if allowed is None else {key(row_ref) for row_ref in allowed}
    eligible = [row for row in rows if allowed_keys is None or key(ref(row)) in allowed_keys]
    lookup = {key(ref(row)): row for row in eligible}
    expected = sorted(
        eligible,
        key=lambda row: (sum((a - b) ** 2 for a, b in zip(query, row["embedding"])), key(ref(row))),
    )[:k]
    hits = result["hits"]
    require(len(hits) <= min(k, len(eligible)), "mixed branch exceeded eligible candidates")
    require(len({key(hit["row_ref"]) for hit in hits}) == len(hits), "mixed merge duplicated a row")
    order = []
    for hit in hits:
        require(key(hit["row_ref"]) in lookup, "mixed result contains an invisible or disallowed version")
        row = lookup[key(hit["row_ref"])]
        require(hit["row"] == READ.business(row), "ranked business fetch changed the selected version")
        distance = sum((a - b) ** 2 for a, b in zip(query, row["embedding"]))
        require(
            math.isfinite(hit["distance"]) and math.isclose(hit["distance"], distance, rel_tol=1e-5, abs_tol=1e-4),
            "mixed squared L2 disagrees with its returned vector",
        )
        order.append((hit["distance"], key(hit["row_ref"])))
    require(order == sorted(order), "mixed top-k is not ordered by squared L2 and typed address")
    oracle = {key(ref(row)) for row in expected}
    recall = len(oracle.intersection(key(hit["row_ref"]) for hit in hits)) / len(oracle) if oracle else 1.0
    require(
        recall >= recall_threshold, f"mixed fixture ANN recall below its separate {recall_threshold} gate: {recall}"
    )
    require(result["routes"]["merge_candidates"] <= 3 * k, "merge exceeded three bounded branch top-k lists")
    return recall


def check_routes(result: dict, path: Path, indexed_ids: set[int], k: int = 10) -> None:
    captured = json.loads(path.read_text())
    files = captured["file_capture"]["files"]
    current = {file["id"] for file in files}
    routes = result["routes"]
    require(
        set(routes["indexed_files"]) == indexed_ids and set(routes["scan_files"]) == current - indexed_ids,
        "B/F partition changed or silently rerouted indexed files",
    )
    require(not indexed_ids.intersection(routes["scan_files"]) and indexed_ids.issubset(current), "B/F overlap or gap")
    require(
        routes["scanned_rows"] == sum(file["row_count"] for file in files if file["id"] not in indexed_ids),
        "F did not scan precisely its declared physical files",
    )
    require(routes["inline_rows"] == len(captured["inline_rows"]), "T did not use the captured inlined inventory")
    require(routes["ann_calls"] == 1 and routes["ann_candidates"] <= k, "B did not execute bounded native ANN")


def exercise(proof, workers: list[Worker], checks: list[str]) -> dict:
    directory = proof.root / "lake"
    proof.create(directory)
    file_capture, original_rows = proof.capture(directory, "indexed-files")
    reference = proof.root / "files.reference.json"
    proof.run([str(proof.probe), "build-file-index", str(file_capture), str(reference)])
    proof.run([str(proof.probe), "build-file-index", str(file_capture), str(reference)], expected="already exists")
    reference_digest = BASE.digest(reference)
    descriptor = json.loads(reference.read_text())
    generation = proof.root / descriptor["generation"]["generation"]
    artifact_digests = {
        str(path.relative_to(generation)): BASE.digest(path) for path in generation.rglob("*") if path.is_file()
    }
    indexed_ids = {file["id"] for file in descriptor["snapshot"]["files"]}
    require(len(indexed_ids) == 2, "fixture must build one generation over two immutable B files")
    query = next(row["embedding"] for row in original_rows if row["id"] == 133)
    old, old_rows = capture(proof, directory, "old")
    worker = Worker(proof, "mixed")
    workers.append(worker)
    worker.open("old", old, reference)
    baseline = worker.search("old", query)
    recalls = [qualify(baseline, old_rows, query)]
    check_routes(baseline, old, indexed_ids)
    require(baseline["hits"][0]["row"]["id"] == 133, "original indexed self-neighbor missing")

    proof.sql(
        directory,
        "CALL ducklake_set_option('lake', 'data_inlining_row_limit', 100, table_name => 'items');"
        "INSERT INTO lake.items SELECT 5000+i, "
        f"CASE WHEN i=0 THEN {str([133.25] + query[1:])} WHEN i=1 THEN {str(query)} "
        "ELSE [16000+i,0,0,0,0,0,0,0] END, 'B', '未索引-' || i FROM range(128) t(i);",
    )
    proof.sql(
        directory,
        "BEGIN; UPDATE lake.items SET embedding=[7000,0,0,0,0,0,0,0], category='B', payload=NULL WHERE id=133;"
        "DELETE FROM lake.items WHERE id IN (134,5001);"
        f"INSERT INTO lake.items VALUES (6000,{str([133.125] + query[1:])},'A','inline-old'),"
        f"(6001,{str(query)},'B','inline-deleted'); COMMIT;",
    )
    proof.sql(
        directory,
        "BEGIN; UPDATE lake.items SET payload='内联-live' WHERE id=6000;"
        "DELETE FROM lake.items WHERE id=6001; COMMIT;",
    )
    current, rows = capture(proof, directory, "current")
    opened = worker.open("current", current, reference)
    require(opened["inline_rows"] == 2, "fixture did not retain the updated B row and new T row inline")
    result = worker.search("current", query)
    recalls.append(qualify(result, rows, query))
    check_routes(result, current, indexed_ids)
    require(
        result["routes"]["scanned_rows"] == 128 and len(result["routes"]["scan_files"]) == 1,
        "fixture did not produce one declared F file",
    )
    require([hit["row"]["id"] for hit in result["hits"][:2]] == [6000, 5000], "T/F nearest rows did not beat B")
    require(
        all(hit["row"]["id"] not in (133, 134, 5001, 6001) for hit in result["hits"]),
        "a stale B, deleted F or deleted T row survived pre-top-k visibility",
    )
    require(worker.search("old", query) == baseline, "new DuckLake commits changed the retained old view")
    require(
        BASE.digest(reference) == reference_digest
        and {str(path.relative_to(generation)): BASE.digest(path) for path in generation.rglob("*") if path.is_file()}
        == artifact_digests,
        "querying a new snapshot rebuilt or mutated the sealed B index",
    )
    checks.append(
        "one unchanged SPFresh generation reused across snapshots; B/F/T contribute with current update/delete visibility"
    )
    b_allow = [ref(row) for row in rows if row.get("file_id") in indexed_ids]
    b_result = worker.search("current", query, b_allow)
    b_recalls = [recalls[0], qualify(b_result, rows, query, allowed=b_allow)]
    check_routes(b_result, current, indexed_ids)

    for row_id in (129, 5127, 133):
        selected = [ref(row) for row in rows if row["id"] == row_id]
        filtered = worker.search("current", query, selected, k=1)
        recalls.append(qualify(filtered, rows, query, k=1, allowed=selected))
        check_routes(filtered, current, indexed_ids, k=1)
        require([hit["row"]["id"] for hit in filtered["hits"]] == [row_id], "branch post-filtered its unfiltered top-k")
    category = [ref(row) for row in rows if row["category"] == "B"]
    filtered = worker.search("current", query, category)
    recalls.append(qualify(filtered, rows, query, allowed=category))
    require(filtered["hits"][0]["row"]["id"] == 5000, "business prefilter did not precede branch retrieval")
    require(worker.search("current", query, [], k=1)["hits"] == [], "empty prefilter returned candidates")
    sparse_allow = [ref(row) for row in rows if row["id"] == 3001]
    sparse = worker.search("current", query, sparse_allow, k=1)
    sparse_recall = qualify(sparse, rows, query, k=1, allowed=sparse_allow, recall_threshold=0.0)
    check_routes(sparse, current, indexed_ids, k=1)
    held = worker.stats()
    worker.search("current", [0.0], expected="dimension")
    worker.search("current", query, k=0, expected="between 1 and 64")
    worker.search("current", query, k=65, expected="between 1 and 64")
    for row_id in (5127, 133):
        selected = [ref(row) for row in rows if row["id"] == row_id]
        worker.search("current", [3e38] * 8, selected, k=1, expected="distance overflow")
    hidden = ref(next(row for row in old_rows if row["id"] == 133))
    worker.search("current", query, [hidden], expected="visible addresses")
    worker.fetch("current", [hidden], expected="visible addresses")
    worker.call(
        {"op": "fetch_mixed", "view": "current", "capture_digest": "other-view", "rows": []}, expected="mismatch"
    )
    READ.Worker.search(worker, "current", query, expected="mixed commands")
    require(
        worker.stats() == held and worker.search("current", query) == result, "failed query changed a retained plan"
    )
    ranked = [result["hits"][1]["row_ref"], result["hits"][0]["row_ref"], result["hits"][1]["row_ref"]]
    require(
        worker.fetch("current", ranked)
        == [result["hits"][1]["row"], result["hits"][0]["row"], result["hits"][1]["row"]],
        "mixed ordered duplicate fetch lost its typed row identity",
    )
    checks.append(
        "visibility and business predicates precede each branch top-k; squared L2, typed duplicate fetch and bounded merge"
    )

    for name, field, value, error in (
        ("table", "table_uuid", "other-table", "specification mismatch"),
        ("field", "field_id", 999, "specification mismatch"),
        ("dimension", "dimension", 7, "specification mismatch"),
        ("metric", "metric", "cosine", "specification mismatch"),
    ):
        changed = json.loads(reference.read_text())
        changed[field] = value
        worker.open("invalid", current, write_json(proof.root / f"bad-{name}.reference.json", changed), expected=error)
        require(worker.stats() == held, "identity rejection retained resources")
    changed = json.loads(current.read_text())
    changed["file_capture"]["files"][0]["version"] = "sha256:" + "0" * 64
    worker.open(
        "invalid",
        write_json(proof.root / "bad-source.mixed.json", changed),
        reference,
        expected="file version mismatch",
    )
    changed = json.loads(reference.read_text())
    changed["snapshot"]["files"][0]["row_count"] += 1
    worker.open(
        "invalid",
        current,
        write_json(proof.root / "bad-file.reference.json", changed),
        expected="file identity mismatch",
    )
    for name, change, error in (
        ("duplicate-inline", lambda data: data["inline_rows"].append(data["inline_rows"][0]), "Duplicate inline"),
        ("inline-file-id", lambda data: data["inline_rows"][0].update(row_ref=hidden), "never a file ID"),
        ("inline-dimension", lambda data: data["inline_rows"][0]["row"].update(embedding=[0.0]), "dimension"),
        ("inline-null", lambda data: data["inline_rows"][0]["row"].update(embedding=[None] * 8), "expected f32"),
    ):
        changed = json.loads(current.read_text())
        change(changed)
        worker.open("invalid", write_json(proof.root / f"{name}.mixed.json", changed), reference, expected=error)
    manifest = json.loads((generation / "manifest.json").read_text())
    artifact = generation / "artifacts" / manifest["artifacts"][-1]["path"]
    saved = artifact.read_bytes()
    try:
        artifact.write_bytes(saved + b"corrupt")
        worker.open("invalid", current, reference, expected="mismatch")
        require(
            worker.stats() == held and worker.search("current", query) == result,
            "index corruption changed an owned view",
        )
    finally:
        artifact.write_bytes(saved)
    checks.append(
        "table/field/dimension/metric/file/inline/artifact mismatches reject open without scan downgrade or leaked leases"
    )

    for name, limits, error in (
        ("inline-rows", {"max_inline_rows": opened["inline_rows"]}, "inline row budget"),
        ("inline-bytes", {"max_inline_bytes": opened["inline_bytes"]}, "inline byte budget"),
    ):
        limited = Worker(proof, name, **limits)
        workers.append(limited)
        limited.open("first", current, reference)
        before = limited.stats()
        limited.open("second", current, reference, expected=error)
        require(
            limited.stats() == before and limited.search("first", query) == result,
            "inline admission changed the first view",
        )
        require(limited.close_view("first") == READ.empty_stats(), "closing mixed view leaked inline reservations")
        limited.open("replacement", current, reference)
        limited.finish()
    checks.append("aggregate inline row/byte budgets reject before index admission and release on close/shutdown")

    empty = Worker(proof, "no-inline-budget", max_inline_rows=0, max_inline_bytes=0)
    workers.append(empty)
    empty.open("old", old, reference)
    before = empty.stats()
    empty.open("inline", current, reference, expected="inline row budget")
    require(
        empty.stats() == before and empty.search("old", query) == baseline,
        "zero T budget damaged a file-only mixed view",
    )
    empty.finish()

    proof.sql(directory, "CALL ducklake_flush_inlined_data('lake');")
    flushed, flushed_rows = capture(proof, directory, "flushed")
    worker.open("flushed", flushed, reference)
    flushed_result = worker.search("flushed", query)
    recalls.append(qualify(flushed_result, flushed_rows, query))
    check_routes(flushed_result, flushed, indexed_ids)
    require(
        flushed_result["routes"]["inline_rows"] == 0 and len(flushed_result["routes"]["scan_files"]) > 1,
        "flush did not move T to newly declared F files",
    )
    require(
        [hit["row"] for hit in flushed_result["hits"]] == [hit["row"] for hit in result["hits"]],
        "T-to-F flush lost, duplicated or changed a logical result",
    )
    require(BASE.digest(reference) == reference_digest, "flush rebuilt the B index")
    require(worker.search("current", query) == result, "flush changed the view with frozen inline rows")
    checks.append(
        "T-to-F flush preserves logical top-k, retains original B generation and leaves the old inline view usable"
    )

    worker.close_view("old")
    proof.sql(
        directory,
        "CALL ducklake_rewrite_data_files('lake', delete_threshold => 0);"
        "CALL ducklake_merge_adjacent_files('lake');",
    )
    compacted, _ = capture(proof, directory, "compacted")
    require(
        indexed_ids.isdisjoint(file["id"] for file in json.loads(compacted.read_text())["file_capture"]["files"]),
        "compaction did not replace B files",
    )
    before = worker.stats()
    worker.open("rewritten", compacted, reference, expected="Referenced index file is absent")
    require(worker.stats() == before, "failed rewritten-file admission leaked a plan")
    proof.sql(
        directory,
        "SELECT count(*) FROM ducklake_expire_snapshots('lake', older_than => now());"
        "SELECT count(*) FROM ducklake_cleanup_old_files('lake', cleanup_all => true);",
    )
    original_files = [Path(file["uri"]) for file in json.loads(current.read_text())["file_capture"]["files"]]
    require(all(not path.exists() for path in original_files), "DuckLake cleanup did not actually reclaim B/F inputs")
    shutil.rmtree(generation)
    require(
        worker.search("current", query) == result and worker.search("flushed", query) == flushed_result,
        "retained mixed search reopened reclaimed files or generation",
    )
    untouched = [next(row for row in rows if row["id"] == row_id) for row_id in (4001, 5127, 133, 4001)]
    require(
        worker.fetch("current", [ref(row) for row in untouched]) == [READ.business(row) for row in untouched],
        "delayed mixed fetch lost a B/F/T row after canonical GC",
    )
    unseen_query = untouched[0]["embedding"]
    unseen_result = worker.search("current", unseen_query, b_allow)
    b_recalls.append(qualify(unseen_result, rows, unseen_query, allowed=b_allow))
    require(
        unseen_result["hits"][0]["row"]["id"] == 4001, "reclaimed B index lost a previously unqueried self-neighbor"
    )
    worker.close_view("flushed")
    require(worker.close_view("current") == READ.empty_stats(), "final mixed close retained leases or reservations")
    worker.finish(eof=True)
    checks.append(
        "rewritten B rejects stale plan; retained B/F/T search and previously unqueried fetch survive actual source/artifact GC"
    )
    return {
        "index_builds": 1,
        "indexed_files": sorted(indexed_ids),
        "inline_rows_before_flush": opened["inline_rows"],
        "scanned_rows_before_flush": result["routes"]["scanned_rows"],
        "reclaimed_source_files": len(original_files),
        "fixture_recalls": recalls,
        "b_fixture_recalls": b_recalls,
        "recall_threshold": 0.8,
        "sparse_ann_probe": {"eligible_rows": 1, "hits": len(sparse["hits"]), "recall": sparse_recall},
        "old_source_bytes": opened["source_bytes"],
        "old_artifact_bytes": opened["artifact_bytes"],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duckdb", required=True, type=Path)
    parser.add_argument("--probe", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()
    root = args.output_dir.resolve()
    root.mkdir(parents=True, exist_ok=False)
    proof = BASE.Proof(args.duckdb.resolve(strict=True), args.probe.resolve(strict=True), root)
    workers: list[Worker] = []
    checks: list[str] = []
    report = {
        "duckdb_sha256": BASE.digest(proof.duckdb),
        "probe_sha256": BASE.digest(proof.probe),
        "vortex_revision": "145f816102d6024a12164e37737ff8864faf5033",
    }
    started = time.monotonic()
    try:
        report.update(exercise(proof, workers, checks))
        report["status"] = "passed"
    except Exception as error:
        report.update(status="failed", error=str(error))
        raise
    finally:
        for worker in workers:
            if worker.process.poll() is None:
                try:
                    worker.finish()
                except Exception:
                    worker.process.kill()
                    worker.process.wait(timeout=5)
        report.update(
            seconds=time.monotonic() - started,
            sql_probe_processes=proof.processes,
            workers=len(workers),
            worker_requests=sum(worker.requests for worker in workers),
            worker_rejections=sum(worker.errors for worker in workers),
            checks=checks,
        )
        (root / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
