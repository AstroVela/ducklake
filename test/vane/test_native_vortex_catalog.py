#!/usr/bin/env python3
"""Qualify native named-index publication, snapshot discovery and crash recovery."""
from __future__ import annotations

import argparse
import importlib.util
import json
from pathlib import Path
import shutil
import subprocess
import time

SPEC = importlib.util.spec_from_file_location(
    "native_admission_proof", Path(__file__).with_name("test_native_vortex_admission.py")
)
assert SPEC is not None and SPEC.loader is not None
ADMIT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ADMIT)
MIXED = ADMIT.MIXED
BASE = ADMIT.BASE
require = BASE.require


class Native(ADMIT.Native):
    def __init__(self, driver: Path, root: Path):
        self.driver = driver
        self.root = root
        self.requests = 0
        self.errors = 0
        self.sessions = 0
        self.start("create")

    def start(self, mode: str):
        self.sessions += 1
        self.log = self.root.with_suffix(f".session-{self.sessions}.log").open("w")
        self.process = subprocess.Popen(
            [str(self.driver), str(self.root), mode],
            stdin=subprocess.PIPE,
            stdout=self.log,
            stderr=subprocess.STDOUT,
            text=True,
        )

    def send(self, op: str, *args) -> Path:
        self.requests += 1
        require(self.process.poll() is None, "catalog owner exited; inspect its session log")
        self.process.stdin.write("\t".join([str(self.requests), op, *map(str, args)]) + "\n")
        self.process.stdin.flush()
        return self.root / "responses" / f"{self.requests}.json"

    def call(self, op: str, *args, expected: str | None = None):
        path = self.send(op, *args)
        deadline = time.monotonic() + 60
        while not path.exists():
            require(self.process.poll() is None, "catalog owner exited; inspect its session log")
            require(time.monotonic() < deadline, f"catalog operation timed out: {op}")
            time.sleep(0.01)
        value = json.loads(path.read_text())
        if expected is None:
            require(value["ok"], f"catalog operation {op} failed: {value}")
            return value["result"]
        self.errors += 1
        require(not value["ok"] and expected.lower() in value["error"].lower(), f"expected {expected!r}: {value}")

    def sql(self, text: str):
        path = self.root.parent / f"sql-{self.requests+1}.sql"
        path.write_text(text)
        return self.call("sql", path)

    def crash(self, stage: str, reference: Path, revision: int):
        marker = self.root / f"{stage}.ready"
        require(not marker.exists(), "crash marker already exists")
        self.send(stage, "knn", reference, revision)
        deadline = time.monotonic() + 30
        while not marker.exists():
            require(self.process.poll() is None, "publisher exited before crash checkpoint")
            require(time.monotonic() < deadline, "publication crash checkpoint timed out")
            time.sleep(0.01)
        self.process.kill()
        self.process.wait(timeout=10)
        self.log.close()
        require(self.process.returncode == -9, "publication fixture was not stopped with SIGKILL")
        self.start("resume")


class Worker(ADMIT.Worker):
    def open(self, name: str, capture: Path, index_name: str = "knn", expected: str | None = None):
        result = self.call(
            {"op": "open_catalogued", "view": name, "capture": str(capture), "index_name": index_name}, expected
        )
        if result is not None:
            self.digests[name] = result["capture_digest"]
        return result


def descriptor(path: Path) -> dict:
    return json.loads(path.read_text())


def generation(path: Path) -> str:
    return descriptor(path)["generation"]["generation"]


def artifact(path: Path) -> Path:
    root = path.parent / generation(path)
    manifest = descriptor(root / "manifest.json")
    return root / "artifacts" / manifest["artifacts"][-1]["path"]


def exercise(proof, native: Native, workers: list[Worker], checks: list[str]) -> dict:
    native.call("raw", "bootstrap")
    native.call("capture", "no-init", "knn", expected="catalog is not initialized")
    native.call("init")
    native.call("init", expected="already exists")
    native.call("capture", "no-index", "knn", expected="No published Vortex index")
    artifacts = proof.root / "artifacts"
    artifacts.mkdir()
    refs = {}
    for name in "abcdef":
        refs[name] = artifacts / f"{name}.reference.json"
        proof.run([str(proof.probe), "build-admitted-file-index", str(native.capture("bootstrap")), str(refs[name])])
    descriptors = {name: descriptor(path) for name, path in refs.items()}
    generations = {name: generation(path) for name, path in refs.items()}
    require(len(set(generations.values())) == 6, "builders reused an immutable generation")
    query = next(row["embedding"] for row in native.rows("bootstrap") if row["id"] == 133)
    published = native.call("publish", "knn", refs["a"], 0)
    require(
        published["revision"] == 1 and published["generation"] == generations["a"],
        "initial publication identity changed",
    )
    refs["a"].unlink()
    native.call("capture", "initial", "knn")
    worker = Worker(proof, "consumer", max_views=4)
    workers.append(worker)
    opened = worker.open("initial", native.capture("initial"))
    require(
        opened["index_revision"] == 1 and opened["generation"] == generations["a"],
        "consumer did not use named catalog generation",
    )
    baseline = worker.search("initial", query)
    recalls = [MIXED.qualify(baseline, native.rows("initial"), query)]
    checks.append(
        "explicit initialization and atomic sealed-generation publication; named acquisition works after reference-file removal"
    )

    for name, field, value, error in (
        ("bad-table", "table_uuid", "other-table", "table, field or metric mismatch"),
        ("bad-field", "field_id", 999, "table, field or metric mismatch"),
        ("bad-metric", "metric", "cosine", "table, field or metric mismatch"),
        ("bad-dimension", "dimension", 7, "physical snapshot mismatch"),
    ):
        changed = json.loads(json.dumps(descriptors["b"]))
        changed[field] = value
        path = MIXED.write_json(artifacts / f"{name}.json", changed)
        native.call("publish", "knn", path, 1, expected=error)
    damaged = artifact(refs["b"])
    original = damaged.read_bytes()
    try:
        damaged.write_bytes(original + b"corrupt")
        native.call("publish", "knn", refs["b"], 1, expected="checksum mismatch")
        require(worker.search("initial", query) == baseline, "failed publication damaged an existing view")
    finally:
        damaged.write_bytes(original)
    native.call("publish", "knn", refs["b"], 0, expected="revision conflict")
    managed_reference = native.root / "data" / "managed.reference.json"
    managed_reference.write_text(refs["b"].read_text())
    try:
        native.call("publish", "knn", managed_reference, 1, expected="outside the managed data path")
    finally:
        managed_reference.unlink()
    require(native.call("head", "knn")["revision"] == 1, "failed publication changed committed index head")
    checks.append(
        "table/field/dimension/metric/artifact validation and stale-revision rejection leave the committed head unchanged"
    )

    raced = native.call("race", "knn", refs["b"], refs["c"], 1)
    require(
        raced["revision"] == 2
        and raced["reference"]["generation"]["generation"] in (generations["b"], generations["c"]),
        "two concurrent publishers did not resolve to one atomic revision",
    )
    native.call("capture", "raced", "knn")
    worker.open("raced", native.capture("raced"))
    recalls.append(MIXED.qualify(worker.search("raced", query), native.rows("raced"), query))
    published = native.call("old-read", "knn", refs["d"], 2, "overlap")
    require(published["revision"] == 3, "replacement publication did not advance revision")
    overlap = descriptor(native.capture("overlap"))["index"]
    require(
        overlap["revision"] == 2 and overlap["reference"] == raced["reference"],
        "held metadata transaction read a newer index head",
    )
    worker.open("overlap", native.capture("overlap"))
    recalls.append(MIXED.qualify(worker.search("overlap", query), native.rows("overlap"), query))
    worker.close_view("raced")
    worker.close_view("overlap")
    native.call("capture", "third", "knn")
    third = worker.open("third", native.capture("third"))
    require(
        third["index_revision"] == 3 and third["generation"] == generations["d"],
        "fresh read view failed to discover replacement",
    )
    checks.append(
        "one CAS winner under concurrent publication; an old metadata transaction keeps its prior generation while a fresh reader discovers the replacement"
    )

    native.sql(
        "CALL ducklake_set_option('lake','data_inlining_row_limit',100,table_name=>'items');"
        "INSERT INTO lake.items SELECT 5000+i,CASE WHEN i=0 THEN [133.25,4,2,0,1,8,7,16] WHEN i=1 THEN [133,4,2,0,1,8,7,16] "
        "ELSE [16000+i,0,0,0,0,0,0,0] END,'B','未索引-'||i FROM range(128)t(i);"
        "BEGIN; UPDATE lake.items SET embedding=[7000,0,0,0,0,0,0,0],category='B',payload=NULL WHERE id=133;"
        "DELETE FROM lake.items WHERE id IN (134,5001);INSERT INTO lake.items VALUES(6000,[133.125,4,2,0,1,8,7,16],'A','内联-live');COMMIT;"
    )
    native.call("capture", "delta", "knn")
    worker.open("delta", native.capture("delta"))
    rows = native.rows("delta")
    delta = worker.search("delta", query)
    recalls.append(MIXED.qualify(delta, rows, query))
    indexed = {file["id"] for file in descriptors["a"]["snapshot"]["files"]}
    MIXED.check_routes(delta, native.mixed("delta"), indexed)
    require(
        [hit["row"]["id"] for hit in delta["hits"][:2]] == [6000, 5000],
        "named B/F/T plan lost current visibility or delta nearest rows",
    )
    require(worker.search("initial", query) == baseline, "new source commits changed an old catalogued view")
    worker.close_view("third")
    native.call("commit-race", "knn", refs["e"], 3)
    native.call("capture", "fourth", "knn")
    fourth = worker.open("fourth", native.capture("fourth"))
    fourth_rows = native.rows("fourth")
    fourth_result = worker.search("fourth", query)
    recalls.append(MIXED.qualify(fourth_result, fourth_rows, query))
    require(
        fourth["index_revision"] == 4 and fourth_result["hits"][0]["row"]["id"] == 7001,
        "publication/source-commit fence lost the subsequent committed T row",
    )
    worker.close_view("delta")
    native.call("ack", "knn", refs["f"], 4, expected="committed but acknowledgement failed")
    require(native.call("head", "knn")["revision"] == 5, "lost acknowledgement reverted a committed publication")
    checks.append(
        "named discovery composes B/F/T; publication fences source commits; lost acknowledgement preserves the committed head and all generations"
    )

    loser = "c" if raced["reference"]["generation"]["generation"] == generations["b"] else "b"
    native.crash("crash-before", refs[loser], 5)
    require(native.call("head", "knn")["revision"] == 5, "SIGKILL before metadata commit published an uncommitted head")
    require(
        (artifacts / generations[loser] / "manifest.json").exists(),
        "rollback/crash reclaimed a sealed candidate generation",
    )
    native.crash("crash-after", refs[loser], 5)
    recovered = native.call("head", "knn")
    require(
        recovered["revision"] == 6 and recovered["reference"] == descriptors[loser],
        "SIGKILL after commit lost the durable publication",
    )
    refs[loser].unlink()
    native.call("capture", "recovered", "knn")
    reopened = worker.open("recovered", native.capture("recovered"))
    require(
        reopened["index_revision"] == 6 and reopened["generation"] == generations[loser],
        "restart failed to rediscover the durable generation",
    )
    recalls.append(MIXED.qualify(worker.search("recovered", query), native.rows("recovered"), query))
    checks.append(
        "SIGKILL before/after publication commit recovers the old/new head respectively; sealed candidates survive and ready lookup does not need reference files"
    )

    native.sql(
        "CALL ducklake_flush_inlined_data('lake');CALL ducklake_rewrite_data_files('lake',delete_threshold=>0);"
        "CALL ducklake_merge_adjacent_files('lake');"
    )
    native.call("capture", "stale", "knn", expected="Referenced Vortex index file is absent")
    native.call("publish", "knn", refs["f"], 6, expected="Referenced Vortex index file is absent")
    require(native.call("head", "knn")["revision"] == 6, "coverage rejection changed the committed head")
    native.call("raw", "new-layout")
    fresh = artifacts / "new-layout.reference.json"
    proof.run([str(proof.probe), "build-admitted-file-index", str(native.capture("new-layout")), str(fresh)])
    native.call("publish", "knn", fresh, 6)
    native.call("capture", "latest", "knn")
    latest_open = worker.open("latest", native.capture("latest"))
    latest_rows = native.rows("latest")
    latest = worker.search("latest", query)
    recalls.append(MIXED.qualify(latest, latest_rows, query))
    require(
        latest_open["index_revision"] == 7 and latest["hits"][0]["row"]["id"] == 7001,
        "explicit rebuild did not restore the new file layout",
    )
    worker.close_view("recovered")
    held = worker.stats()
    worker.open("wrong-name", native.capture("latest"), index_name="other", expected="name, revision or root mismatch")
    ADMIT.Worker.open(worker, "wrong-mode", native.capture("latest"), fresh, expected="unknown field")
    broken = artifact(fresh)
    saved = broken.read_bytes()
    try:
        broken.write_bytes(saved + b"corrupt")
        native.call("capture", "bad-artifact", "knn")
        worker.open("bad-artifact", native.capture("bad-artifact"), expected="mismatch")
        require(
            worker.stats() == held and worker.search("latest", query) == latest,
            "failed ready-index open leaked resources or fell back",
        )
    finally:
        broken.write_bytes(saved)
    checks.append(
        "compacted B rejects stale acquisition/publication without scan downgrade; explicit rebuild publishes a new layout; corrupt active artifacts fail consumer admission"
    )

    native.sql(
        "SELECT count(*) FROM ducklake_expire_snapshots('lake',older_than=>now());"
        "SELECT count(*) FROM ducklake_cleanup_old_files('lake',cleanup_all=>true);"
    )
    old_files = [Path(file["uri"]) for file in descriptor(native.capture("fourth"))["capture"]["file_capture"]["files"]]
    require(all(not path.exists() for path in old_files), "native GC did not reclaim original B/F files")
    shutil.rmtree(artifacts / generations["a"])
    shutil.rmtree(artifacts / generations["e"])
    native.sql(
        "DROP TABLE lake.items;CREATE TABLE lake.items(id BIGINT,embedding FLOAT[],category VARCHAR,payload VARCHAR);"
    )
    native.call("capture", "new-table", "knn", expected="No published Vortex index")
    native.finish()
    require(
        worker.search("initial", query) == baseline and worker.search("fourth", query) == fourth_result,
        "retained catalogued views reopened latest metadata or reclaimed generations",
    )
    untouched = [next(row for row in fourth_rows if row["id"] == i) for i in (4001, 5127, 133, 6000, 4001)]
    require(
        worker.fetch("fourth", [MIXED.ref(row) for row in untouched])
        == [MIXED.READ.business(row) for row in untouched],
        "delayed B/F/T take changed native source version or duplicate ordering",
    )
    recalls.append(
        MIXED.qualify(
            worker.search("initial", next(row["embedding"] for row in native.rows("bootstrap") if row["id"] == 4001)),
            native.rows("bootstrap"),
            next(row["embedding"] for row in native.rows("bootstrap") if row["id"] == 4001),
        )
    )
    for name in ("initial", "fourth", "latest"):
        stats = worker.close_view(name)
    require(stats == MIXED.READ.empty_stats(), "catalogued close retained source staging or index leases")
    worker.finish(eof=True)
    checks.append(
        "table UUID prevents reuse after drop/recreate; retained views and unseen fetch survive source/index GC and native-owner exit"
    )
    return {
        "index_builds": 7,
        "final_revision": 7,
        "crash_boundaries": 2,
        "reclaimed_source_files": len(old_files),
        "fixture_recalls": recalls,
        "recall_threshold": 0.8,
        "winner_generation": recovered["reference"]["generation"]["generation"],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("duckdb", "driver", "probe", "output-dir"):
        parser.add_argument("--" + name, required=True, type=Path)
    args = parser.parse_args()
    root = args.output_dir.resolve()
    root.mkdir(parents=True, exist_ok=False)
    proof = BASE.Proof(args.duckdb.resolve(strict=True), args.probe.resolve(strict=True), root)
    workers = []
    checks = []
    native = None
    started = time.monotonic()
    report = {
        "duckdb_sha256": BASE.digest(proof.duckdb),
        "probe_sha256": BASE.digest(proof.probe),
        "driver_sha256": BASE.digest(args.driver.resolve(strict=True)),
        "vortex_revision": "664e8cb733c5fbaa2945239e806df5b22e6d190d",
    }
    try:
        native = Native(args.driver.resolve(strict=True), root / "native")
        report.update(exercise(proof, native, workers, checks))
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
        if native is not None and native.process.poll() is None:
            try:
                native.finish()
            except Exception:
                native.process.kill()
                native.process.wait(timeout=5)
        report.update(
            seconds=time.monotonic() - started,
            probe_processes=proof.processes,
            workers=len(workers),
            worker_requests=sum(worker.requests for worker in workers),
            worker_rejections=sum(worker.errors for worker in workers),
            native_requests=native.requests if native else 0,
            native_rejections=native.errors if native else 0,
            native_sessions=native.sessions if native else 0,
            checks=checks,
        )
        (root / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
