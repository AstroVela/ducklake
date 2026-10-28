#!/usr/bin/env python3
"""Qualify retained Vortex read views across native DuckLake maintenance."""

from __future__ import annotations

import argparse
import importlib.util
import json
import math
from pathlib import Path
import shutil
import subprocess
import time


SPEC = importlib.util.spec_from_file_location(
    "native_index_proof", Path(__file__).with_name("test_native_vortex_index.py")
)
assert SPEC is not None and SPEC.loader is not None
BASE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BASE)
require = BASE.require
address = BASE.address


class Worker:
    def __init__(self, proof, name: str, **limits) -> None:
        self.root = proof.root / name
        self.requests = 0
        self.errors = 0
        self.digests: dict[str, str] = {}
        self.log = (proof.root / f"{name}.log").open("w")
        configured = {
            "max_views": 3,
            "max_source_bytes": 64 * 1024 * 1024,
            "max_artifact_bytes": 128 * 1024 * 1024,
            "max_inline_rows": 4096,
            "max_inline_bytes": 4 * 1024 * 1024,
            **limits,
        }
        self.process = subprocess.Popen(
            [str(proof.probe), "serve", str(self.root), json.dumps(configured)],
            stdin=subprocess.PIPE,
            stdout=self.log,
            stderr=subprocess.STDOUT,
            text=True,
        )

    def call(self, command: dict, expected: str | None = None):
        self.requests += 1
        response = self.root / "responses" / f"{self.requests}.json"
        require(self.process.poll() is None, f"read-view worker exited: {self.root}")
        self.process.stdin.write(json.dumps({"id": self.requests, "command": command}) + "\n")
        self.process.stdin.flush()
        deadline = time.monotonic() + 30
        while not response.exists():
            require(self.process.poll() is None, f"read-view worker failed: {self.root}")
            require(time.monotonic() < deadline, f"read-view response timed out: {self.root}")
            time.sleep(0.01)
        result = json.loads(response.read_text())
        if expected is None:
            require(result["ok"], f"read-view request failed: {result}")
            return result["result"]
        self.errors += 1
        require(
            not result["ok"] and expected.lower() in result["error"].lower(),
            f"expected read-view rejection {expected!r}: {result}",
        )
        return None

    def open(self, name: str, capture: Path, reference: Path, expected: str | None = None):
        result = self.call({"op": "open", "view": name, "capture": str(capture), "reference": str(reference)}, expected)
        if result is not None:
            self.digests[name] = result["capture_digest"]
        return result

    def search(
        self,
        name: str,
        query: list[float],
        allowed=None,
        k: int = 10,
        expected: str | None = None,
        capture_digest: str | None = None,
    ):
        return self.call(
            {
                "op": "search",
                "view": name,
                "capture_digest": capture_digest or self.digests[name],
                "request": {"query": query, "k": k, "allow": allowed, "exact": False},
            },
            expected,
        )

    def fetch(self, name: str, rows: list[dict], expected: str | None = None):
        return self.call({"op": "fetch", "view": name, "capture_digest": self.digests[name], "rows": rows}, expected)

    def stats(self) -> dict:
        return self.call({"op": "stats"})

    def close_view(self, name: str, expected: str | None = None):
        return self.call({"op": "close", "view": name}, expected)

    def finish(self, eof: bool = False) -> None:
        if self.process.poll() is None:
            if not eof:
                stats = self.call({"op": "shutdown"})
                require(stats == empty_stats(), "shutdown retained view resources")
            self.process.stdin.close()
            try:
                self.process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
                raise AssertionError("read-view worker failed to close")
        self.log.close()
        require(self.process.returncode == 0, f"read-view worker did not exit cleanly: {self.root}")
        leases = self.root / "leases"
        require(leases.is_dir() and not any(leases.iterdir()), "worker exit left protected index artifacts")


def empty_stats() -> dict:
    return {
        "active_views": 0,
        "source_bytes": 0,
        "artifact_bytes": 0,
        "inline_rows": 0,
        "inline_bytes": 0,
        "lease_directories": 0,
    }


def refs(rows: list[dict]) -> list[dict]:
    return [{"file_id": row["file_id"], "row_offset": row["row_offset"]} for row in rows]


def business(row: dict) -> dict:
    return {key: row[key] for key in ("id", "embedding", "category", "payload")}


def qualify(hits: list[dict], rows: list[dict], query: list[float], allowed=None) -> None:
    lookup = {address(row): row for row in rows}
    allow = None if allowed is None else {address(row) for row in allowed}
    require(
        len(hits) == 10 and len({address(hit) for hit in hits}) == 10, "retained fixture requires ten distinct hits"
    )
    ordering = []
    for hit in hits:
        require(
            address(hit) in lookup and (allow is None or address(hit) in allow),
            "retained search returned an invisible or disallowed address",
        )
        row = lookup[address(hit)]
        require(hit["row"] == business(row), "retained business take changed its logical version")
        require(hit["embedding"] == row["embedding"], "retained vector take changed")
        distance = sum((a - b) ** 2 for a, b in zip(query, row["embedding"]))
        require(
            math.isfinite(hit["distance"]) and math.isclose(hit["distance"], distance, rel_tol=1e-5, abs_tol=1e-4),
            "retained distance disagrees with the returned vector",
        )
        ordering.append((hit["distance"], address(hit)))
    require(ordering == sorted(ordering), "retained rank order changed")


def exercise(proof, workers: list[Worker], checks: list[str]) -> dict:
    directory = proof.root / "lake"
    proof.create(directory)
    proof.sql(
        directory,
        "UPDATE lake.items SET payload=NULL WHERE id=131;" "UPDATE lake.items SET category=NULL WHERE id=129;",
    )
    capture, rows = proof.capture(directory, "original")
    reference = proof.build(capture, "original")
    query = next(row["embedding"] for row in rows if row["id"] == 133)
    by_address = {address(row): row for row in rows}

    worker = Worker(proof, "retained")
    workers.append(worker)
    require(worker.stats() == empty_stats(), "new worker retained resources")
    opened = worker.open("old", capture, reference)
    held = worker.stats()
    require(held["active_views"] == held["lease_directories"] == 1, "view failed to own its private artifacts")
    require(
        held["source_bytes"] == opened["source_bytes"] > 0 and held["artifact_bytes"] == opened["artifact_bytes"] > 0,
        "retained bytes were not accounted",
    )
    baseline = worker.search("old", query)
    qualify(baseline, rows, query)
    require(baseline[0]["row"]["id"] == 133, "retained original self-neighbor missing")
    require(
        any(hit["row"]["payload"] is None for hit in baseline)
        and any(hit["row"]["category"] is None for hit in baseline),
        "nullable business values were not exercised",
    )
    selected = refs([row for row in rows if row["category"] == "A"])
    filtered = worker.search("old", query, selected)
    qualify(filtered, rows, query, selected)
    require(worker.search("old", query, [], k=10) == [], "empty prefilter returned rows")
    ranked_refs = refs([baseline[2], baseline[0], baseline[2]])
    expected_fetch = [business(by_address[address(row)]) for row in ranked_refs]
    require(worker.fetch("old", ranked_refs) == expected_fetch, "duplicate/reordered take changed row order")
    worker.search("old", [0.0], expected="dimension")
    worker.search("old", query, capture_digest="wrong-capture", expected="mismatch")
    worker.open("old", capture, reference, expected="already open")
    require(
        worker.stats() == held and worker.search("old", query) == baseline, "failed requests changed the active view"
    )
    checks.append(
        "retained ANN, nullable Unicode business fields, ordered duplicate fetch and immutable capture binding"
    )

    legacy = proof.root / "legacy.capture.json"
    legacy_data = json.loads(capture.read_text())
    legacy_data.pop("business_fields")
    legacy.write_text(json.dumps(legacy_data, sort_keys=True, separators=(",", ":")) + "\n")
    legacy_reference = proof.build(legacy, "legacy")
    legacy_hits = proof.search(legacy, legacy_reference, query, k=1)
    require(
        [address(hit) for hit in legacy_hits] == [address(baseline[0])],
        "legacy one-shot captures lost their vector-search contract",
    )
    worker.open("legacy", legacy, legacy_reference, expected="business field IDs")
    require(worker.stats() == held, "rejected legacy retained view left reservations")
    checks.append("legacy one-shot captures remain supported and retained mode requires business field IDs")

    for name, limits, error in (
        ("count-limit", {"max_views": 1}, "count budget"),
        ("source-limit", {"max_source_bytes": opened["source_bytes"]}, "byte budget"),
        ("artifact-limit", {"max_artifact_bytes": opened["artifact_bytes"]}, "artifact byte budget"),
    ):
        limited = Worker(proof, name, **limits)
        workers.append(limited)
        limited.open("first", capture, reference)
        before = limited.stats()
        limited.open("second", capture, reference, expected=error)
        require(limited.stats() == before, "rejected admission changed reservations or private leases")
        require(limited.search("first", query) == baseline, "rejected admission damaged the retained view")
        require(limited.close_view("first") == empty_stats(), "close retained byte reservations")
        limited.open("replacement", capture, reference)
        limited.finish()
    checks.append("view-count, aggregate source and artifact budgets reject admission and release on close/shutdown")

    descriptor = json.loads(reference.read_text())
    generation = proof.root / descriptor["generation"]["generation"]
    manifest = generation / "manifest.json"
    artifacts = json.loads(manifest.read_text())["artifacts"]
    for path, expected in (
        (reference, "trailing characters"),
        (manifest, "mismatch"),
        (generation / "artifacts" / artifacts[-1]["path"], "mismatch"),
    ):
        original = path.read_bytes()
        try:
            path.write_bytes(original + b"changed")
            worker.open("invalid", capture, reference, expected=expected)
            require(worker.stats() == held, "failed reopen left a private copy or reservation")
            require(
                worker.search("old", query) == baseline and worker.fetch("old", ranked_refs) == expected_fetch,
                "canonical reference/artifact changes affected a retained view",
            )
        finally:
            path.write_bytes(original)
    checks.append(
        "canonical reference, manifest and late-artifact corruption cannot change retained handles; failed opens clean up"
    )

    proof.sql(
        directory,
        "CALL ducklake_set_option('lake', 'data_inlining_row_limit', 100, table_name => 'items');"
        "BEGIN; DELETE FROM lake.items WHERE id=134;"
        "UPDATE lake.items SET embedding=[100000,0,0,0,0,0,0,0], category='B', payload='updated' WHERE id=133;"
        f"INSERT INTO lake.items VALUES (4096, {str([133.125] + query[1:])}, 'A', 'new-nearest'); COMMIT;",
    )
    inlined = proof.sql(
        directory, "SELECT count(*) AS n FROM lake.items WHERE filename LIKE 'ducklake_inlined_data_%';"
    )[0]["n"]
    require(inlined > 0, "writer did not exercise committed inline changes")
    require(
        worker.search("old", query) == baseline and worker.fetch("old", ranked_refs) == expected_fetch,
        "committed inline updates/deletes changed the old read view",
    )
    try:
        proof.capture(directory, "unsupported-inline")
    except AssertionError as error:
        require("mixed-query gate" in str(error), "inline capture failed for an unrelated reason")
    else:
        raise AssertionError("fresh file-only capture silently omitted inlined rows")
    proof.sql(directory, "CALL ducklake_flush_inlined_data('lake');")
    flushed, flushed_rows = proof.capture(directory, "flushed")
    worker.open("stale", flushed, reference, expected="snapshot")
    flushed_index = proof.build(flushed, "flushed")
    worker.open("flushed", flushed, flushed_index)
    current_allow = refs([row for row in flushed_rows if row["category"] == "A"])
    fresh_hits = worker.search("flushed", query, current_allow)
    qualify(fresh_hits, flushed_rows, query, current_allow)
    require(
        fresh_hits[0]["row"]["id"] == 4096 and all(hit["row"]["id"] not in (133, 134) for hit in fresh_hits),
        "new view returned an updated/deleted old neighbor",
    )
    hidden = next(row for row in rows if row["id"] == 133)
    worker.fetch("flushed", refs([hidden]), expected="visible addresses")
    require(
        worker.search("old", query) == baseline and worker.fetch("old", ranked_refs) == expected_fetch,
        "flush changed the old view or its delayed fetch",
    )
    checks.append("separate-connection inline updates/deletes and flush preserve old view; new view sees new versions")

    proof.sql(
        directory,
        "CALL ducklake_rewrite_data_files('lake', delete_threshold => 0);"
        "CALL ducklake_merge_adjacent_files('lake');",
    )
    compacted, compacted_rows = proof.capture(directory, "compacted")
    require(
        set(file["id"] for file in json.loads(compacted.read_text())["files"]).isdisjoint(
            file["id"] for file in json.loads(capture.read_text())["files"]
        ),
        "compaction did not retire original files",
    )
    worker.open("stale-layout", compacted, flushed_index, expected="snapshot")
    require(
        worker.search("old", query) == baseline and worker.search("flushed", query, current_allow) == fresh_hits,
        "compaction changed either retained generation",
    )
    require(worker.fetch("old", ranked_refs) == expected_fetch, "delayed take used compacted physical positions")
    proof.sql(
        directory,
        "SELECT count(*) FROM ducklake_expire_snapshots('lake', older_than => now());"
        "SELECT count(*) FROM ducklake_cleanup_old_files('lake', cleanup_all => true);",
    )
    original_files = [Path(file["uri"]) for file in json.loads(capture.read_text())["files"]]
    require(all(not path.exists() for path in original_files), "cleanup did not remove original physical files")
    shutil.rmtree(generation)
    worker.open("reclaimed", capture, reference, expected="No such file")
    require(
        worker.search("old", query) == baseline and worker.fetch("old", ranked_refs) == expected_fetch,
        "retained source/index depended on reclaimed canonical files",
    )
    unseen = next(row for row in rows if row["id"] == 2049)
    unseen_hits = worker.search("old", unseen["embedding"])
    qualify(unseen_hits, rows, unseen["embedding"])
    require(unseen_hits[0]["row"]["id"] == 2049, "reclaimed view could not answer a previously unqueried vector")
    untouched = [unseen, next(row for row in rows if row["id"] == 42), unseen]
    require(
        worker.fetch("old", refs(untouched)) == [business(row) for row in untouched],
        "reclaimed view could not fetch previously unqueried business rows",
    )
    require(worker.search("flushed", query, current_allow) == fresh_hits, "cleanup changed the other retained view")
    checks.append(
        "actual snapshot expiration/file cleanup and canonical index removal preserve pinned search and delayed fetch"
    )

    latest, latest_rows = proof.capture(directory, "latest")
    latest_index = proof.build(latest, "latest")
    worker.open("latest", latest, latest_index)
    latest_allow = refs([row for row in latest_rows if row["category"] == "A"])
    latest_hits = worker.search("latest", query, latest_allow)
    qualify(latest_hits, latest_rows, query, latest_allow)
    require(latest_hits[0]["row"]["id"] == 4096, "latest view did not use its rebuilt generation")
    updated = next(row for row in latest_rows if row["id"] == 133)
    require(
        worker.fetch("latest", refs([updated])) == [business(updated)] and updated["payload"] == "updated",
        "latest business fetch returned the old logical version",
    )
    worker.close_view("flushed")
    worker.close_view("old")
    worker.search("old", query, expected="Unknown read view")
    worker.close_view("old", expected="Unknown read view")
    require(
        worker.stats()["active_views"] == worker.stats()["lease_directories"] == 1,
        "closing old views removed or retained the wrong leases",
    )
    require(worker.search("latest", query, latest_allow) == latest_hits, "closing old views damaged latest view")
    require(worker.close_view("latest") == empty_stats(), "final close retained reservations or private artifacts")
    worker.open("eof", latest, latest_index)
    worker.finish(eof=True)
    checks.append(
        "close isolates remaining views, rejects closed handles, reuses budgets and releases leases on stdin EOF"
    )
    return {
        "old_source_bytes": opened["source_bytes"],
        "old_artifact_bytes": opened["artifact_bytes"],
        "reclaimed_source_files": len(original_files),
        "inline_rows_before_flush": inlined,
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
        "vortex_revision": "ee085f55a705cb9b2edab18d392dd822540774d7",
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
