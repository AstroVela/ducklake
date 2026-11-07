#!/usr/bin/env python3
"""Qualify native named-index publication, snapshot discovery and crash recovery."""
from __future__ import annotations

import argparse
import importlib.util
import json
import os
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
    native.call("cancel-init")
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
    native.call("cancel-publish", "knn", refs["b"], 1)
    cancelled = native.call("head", "knn")
    require(
        cancelled["revision"] == 1 and cancelled["reference"] == descriptors["a"],
        "publication cancelled before its metadata transaction changed the ready index",
    )
    for stage in ("after_validation", "before_commit"):
        native.call("cancel-stage", "knn", refs["b"], 1, stage)
        cancelled = native.call("head", "knn")
        require(
            cancelled["revision"] == 1 and cancelled["reference"] == descriptors["a"],
            f"publication cancelled at {stage} changed the ready index",
        )
    checks.append(
        "caller cancellation before metadata transaction, after validation and before commit preserves ready identity; initialization cancellation releases its owner"
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
        "cancelled_catalog_requests": 4,
        "reclaimed_source_files": len(old_files),
        "fixture_recalls": recalls,
        "recall_threshold": 0.8,
        "winner_generation": recovered["reference"]["generation"]["generation"],
    }


def exercise_data_paths(proof, root: Path, checks: list[str]) -> list[dict]:
    root.mkdir()
    cases = []
    for mode in ("canonical", "dot", "relative", "symlink"):
        case = root / mode
        case.mkdir()
        # Exercise a managed artifact directory whose generation parent is outside it.
        data = case / "generation" / "artifacts" if mode == "canonical" else case / "data"
        data.mkdir(parents=True)
        data_path = str(data)
        if mode == "dot":
            data_path = str(case) + "/./data"
        elif mode == "relative":
            data_path = os.path.relpath(data, Path.cwd())
        elif mode == "symlink":
            (case / "data-link").symlink_to(data, target_is_directory=True)
            data_path = str(case / "data-link")
        prefix = (
            "SET autoinstall_known_extensions=false;SET autoload_known_extensions=false;"
            "LOAD ducklake;LOAD parquet;LOAD vortex;SET threads=1;ATTACH "
            + BASE.quote("ducklake:" + str(case / "metadata.ducklake"))
            + " AS lake(DATA_PATH "
            + BASE.quote(data_path)
            + ",DATA_INLINING_ROW_LIMIT 0);"
        )

        def sql(statement: str, expected: str | None = None):
            return proof.run([str(proof.duckdb), "-batch", "-bail", "-json", "-c", prefix + statement], expected)

        sql(
            "CREATE TABLE lake.items(id BIGINT NOT NULL,embedding FLOAT[],category VARCHAR,payload VARCHAR);"
            "CALL ducklake_set_option('lake','experimental_data_file_format','vortex',table_name=>'items');"
            "INSERT INTO lake.items SELECT i,[i::FLOAT,4,2,0,1,8,7,16],'A','row-'||i FROM range(4096)t(i);"
            "SELECT * FROM ducklake_initialize_vortex_indexes('lake');"
        )
        admission = json.loads(
            sql("SELECT admission FROM ducklake_vortex_read_view('lake','main','items',8,67108864,100000)")
        )[0]["admission"]
        capture = case / "capture.json"
        capture.write_text(admission)
        # A sibling with the same prefix is outside the managed directory.
        index = data.parent / (data.name + "-index")
        index.mkdir()
        reference = index / "reference.json"
        proof.run([str(proof.probe), "build-admitted-file-index", str(capture), str(reference)])

        def publish(path: Path, revision: int, expected: str | None = None):
            return sql(
                "SELECT * FROM ducklake_publish_vortex_index('lake','main','items','knn',"
                + BASE.quote(path)
                + f",{revision})",
                expected,
            )

        published = json.loads(publish(reference, 0))[0]
        require(int(published["revision"]) == 1, f"valid {mode} DATA_PATH could not publish")
        inside = data / "index"
        shutil.copytree(index, inside)
        publish(inside / "reference.json", 1, "outside the managed data path")
        rejected = ["inside managed data path"]

        def relocated_reference(directory: Path, path: Path) -> Path:
            source = index / generation(reference)
            shutil.copytree(source / "artifacts", directory / "artifacts", dirs_exist_ok=True)
            manifest = json.loads((source / "manifest.json").read_text())
            manifest["generation"] = directory.name
            manifest_path = directory / "manifest.json"
            manifest_path.write_text(json.dumps(manifest, separators=(",", ":")))
            descriptor = json.loads(reference.read_text())
            descriptor["generation"]["generation"] = directory.name
            descriptor["generation"]["manifest"].update(
                size=manifest_path.stat().st_size, checksum=BASE.digest(manifest_path)
            )
            path.write_text(json.dumps(descriptor, separators=(",", ":")))
            return path

        # An external reference must not admit a generation inside the managed root.
        nested_generation = relocated_reference(data, data.parent / "managed-generation.reference.json")
        publish(nested_generation, 1, "outside the managed data path")
        rejected.append("managed generation with external reference")
        if mode == "canonical":
            # Here the generation directory is outside DATA_PATH, but its artifacts are inside.
            nested_artifacts = relocated_reference(data.parent, case / "managed-artifacts.reference.json")
            publish(nested_artifacts, 1, "outside the managed data path")
            rejected.append("managed artifacts with external generation")
            link = case / "reference-link.json"
            link.symlink_to(reference)
            publish(link, 1, "must be canonical")
            rejected.append("reference symlink")
            protected = artifact(reference)
            original = protected.read_bytes()
            target = case / "artifact-copy"
            target.write_bytes(original)
            protected.unlink()
            protected.symlink_to(target)
            try:
                publish(reference, 1, "must be canonical")
            finally:
                protected.unlink()
                protected.write_bytes(original)
            rejected.append("artifact symlink")

        unchanged = json.loads(
            sql("SELECT admission FROM ducklake_vortex_index_view('lake','main','items','knn',67108864,100000)")
        )[0]["admission"]
        named_before = json.loads(unchanged)["index"]
        require(
            named_before["revision"] == 1
            and named_before["root"] == str(index)
            and named_before["reference"] == json.loads(reference.read_text()),
            f"path rejections changed the ready identity for {mode}",
        )

        # An ancestor reference root is valid when its actual generation is a sibling of DATA_PATH.
        shutil.copytree(index / generation(reference), data.parent / generation(reference))
        ancestor_reference = data.parent / "ancestor.reference.json"
        ancestor_reference.write_text(reference.read_text())
        ancestor = json.loads(publish(ancestor_reference, 1))[0]
        require(int(ancestor["revision"]) == 2, f"valid ancestor index root was rejected for {mode}")
        acquired = json.loads(
            sql(
                "SELECT visible_rows,admission FROM ducklake_vortex_index_view('lake','main','items','knn',67108864,100000)"
            )
        )[0]
        named = json.loads(acquired["admission"])["index"]
        require(
            int(acquired["visible_rows"]) == 4096
            and named["revision"] == 2
            and named["root"] == str(data.parent)
            and named["reference"]["generation"]["generation"] == generation(reference),
            f"path validation changed the ready index for {mode}",
        )
        cases.append(
            {"data_path": mode, "visible_rows": 4096, "revision": 2, "ancestor_root": True, "rejections": rejected}
        )
    checks.append(
        "canonical/dot/relative/symlink managed paths and valid ancestor roots publish and acquire; actual generation/artifact containment and strict reference/artifact paths remain enforced"
    )
    return cases


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
        "vortex_revision": "703ed464a5e03bee45c8713c5a4286571349bce1",
    }
    try:
        native = Native(args.driver.resolve(strict=True), root / "native")
        report.update(exercise(proof, native, workers, checks))
        report["data_path_regressions"] = exercise_data_paths(proof, root / "data-paths", checks)
        report["catalog_index_builds"] = report["index_builds"]
        report["index_builds"] += len(report["data_path_regressions"])
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
