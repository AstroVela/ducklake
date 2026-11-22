#!/usr/bin/env python3
"""Qualify native snapshot admission against concurrent writes and physical GC."""

from __future__ import annotations

import argparse
import importlib.util
import json
from pathlib import Path
import shutil
import subprocess
import time


SPEC = importlib.util.spec_from_file_location(
    "native_mixed_proof", Path(__file__).with_name("test_native_vortex_mixed.py")
)
assert SPEC is not None and SPEC.loader is not None
MIXED = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MIXED)
BASE = MIXED.BASE
require = BASE.require


class Native:
    def __init__(self, driver: Path, root: Path) -> None:
        self.root = root
        self.log = root.with_suffix(".log").open("w")
        self.process = subprocess.Popen(
            [str(driver), str(root)], stdin=subprocess.PIPE, stdout=self.log, stderr=subprocess.STDOUT, text=True
        )
        try:
            self.phases = {"bootstrap": self.wait("bootstrap")}
        except Exception:
            if self.process.poll() is None:
                self.process.kill()
                self.process.wait(timeout=5)
            self.log.close()
            raise

    def wait(self, phase: str) -> dict:
        path = self.root / f"{phase}.ready.json"
        deadline = time.monotonic() + 60
        while not path.exists():
            require(self.process.poll() is None, f"native driver exited: {self.root.with_suffix('.log')}")
            require(time.monotonic() < deadline, f"native phase timed out: {phase}")
            time.sleep(0.01)
        return json.loads(path.read_text())

    def phase(self, phase: str) -> dict:
        self.process.stdin.write(phase + "\n")
        self.process.stdin.flush()
        self.phases[phase] = self.wait(phase)
        return self.phases[phase]

    def capture(self, name: str) -> Path:
        return self.root / f"{name}.admitted.json"

    def rows(self, name: str) -> list[dict]:
        captured = json.loads(self.capture(name).read_text())["capture"]["file_capture"]
        files = {file["id"]: file["uri"] for file in captured["files"]}
        result = []
        for record in json.loads((self.root / f"{name}.rows.json").read_text()):
            address = record["row_ref"]
            row = dict(record["row"])
            if address["kind"] == "file":
                row.update(
                    filename=files[address["file_id"]], file_id=address["file_id"], row_offset=address["row_offset"]
                )
            else:
                row.update(filename=address["source"], logical_row_id=address["row_id"])
            result.append(row)
        return result

    def mixed(self, name: str) -> Path:
        return MIXED.write_json(self.root / f"{name}.mixed.json", json.loads(self.capture(name).read_text())["capture"])

    def finish(self) -> None:
        if self.process.poll() is None:
            self.process.stdin.close()
            try:
                self.process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
                raise AssertionError("native driver did not release its owners")
        self.log.close()
        require(self.process.returncode == 0, "native driver failed; inspect its log")


class Worker(MIXED.Worker):
    def open(self, name: str, capture: Path, reference: Path, expected: str | None = None):
        result = self.call(
            {"op": "open_admitted", "view": name, "capture": str(capture), "reference": str(reference)}, expected
        )
        if result is not None:
            self.digests[name] = result["capture_digest"]
        return result


def build(proof, capture: Path, name: str) -> Path:
    reference = proof.root / f"{name}.reference.json"
    proof.run([str(proof.probe), "build-admitted-file-index", str(capture), str(reference)])
    return reference


def exercise(proof, native: Native, workers: list[Worker], checks: list[str]) -> dict:
    initial_rows = native.rows("initial")
    query = next(row["embedding"] for row in initial_rows if row["id"] == 133)
    reference = build(proof, native.capture("initial"), "original")
    ref_digest = BASE.digest(reference)
    indexed_ids = {file["id"] for file in json.loads(reference.read_text())["snapshot"]["files"]}
    worker = Worker(proof, "consumer")
    workers.append(worker)
    worker.open("initial", native.capture("initial"), reference)
    baseline = worker.search("initial", query)
    recalls = [MIXED.qualify(baseline, initial_rows, query)]

    native.phase("advance")
    during_rows = native.rows("during")
    current_rows = native.rows("current")
    worker.open("during", native.capture("during"), reference)
    during = worker.search("during", query)
    recalls.append(MIXED.qualify(during, during_rows, query))
    require(
        any(row["id"] == 5001 for row in during_rows) and any(row["id"] == 134 for row in during_rows),
        "pre-commit native view lost rows deleted by the concurrent writer",
    )
    require(
        next(row for row in during_rows if row["id"] == 133)["payload"] == "中文-row-133",
        "native admission mixed business columns from a later commit",
    )
    worker.close_view("during")
    opened = worker.open("current", native.capture("current"), reference)
    result = worker.search("current", query)
    recalls.append(MIXED.qualify(result, current_rows, query))
    MIXED.check_routes(result, native.mixed("current"), indexed_ids)
    require([hit["row"]["id"] for hit in result["hits"][:2]] == [6000, 5000], "native B/F/T plan lost T/F nearest rows")
    require(
        not {134, 5001, 6001}.intersection(row["id"] for row in current_rows), "native admission exposed deleted rows"
    )
    require(
        next(row for row in current_rows if row["id"] == 133)["payload"] is None,
        "native admission lost the new nullable business version",
    )
    require(
        worker.search("initial", query) == baseline and BASE.digest(reference) == ref_digest,
        "concurrent commits changed the old consumer or rebuilt B",
    )
    checks.append(
        "native snapshot/file/visibility/field/T capture remains atomic while another connection commits updates/deletes"
    )

    held = worker.stats()
    admitted = json.loads(native.capture("current").read_text())
    for name, change, error in (
        ("missing-source", lambda value: value["sources"].pop(), "source inventory mismatch"),
        ("duplicate-source", lambda value: value["sources"].__setitem__(1, value["sources"][0]), "duplicate source"),
        (
            "corrupt-source",
            lambda value: value["sources"][0].update(
                hex=("00" if value["sources"][0]["hex"][:2] != "00" else "ff") + value["sources"][0]["hex"][2:]
            ),
            "content version mismatch",
        ),
    ):
        value = json.loads(json.dumps(admitted))
        change(value)
        worker.open("bad", MIXED.write_json(proof.root / f"{name}.json", value), reference, expected=error)
        require(worker.stats() == held, "rejected native payload leaked source staging or artifact leases")
    MIXED.Worker.open(worker, "wrong-mode", native.capture("current"), reference, expected="unknown field")
    require(
        worker.stats() == held and worker.search("current", query) == result, "wrong admission mode changed the view"
    )
    limited = Worker(proof, "source-limit", max_source_bytes=1)
    workers.append(limited)
    limited.open("limited", native.capture("current"), reference, expected="source byte budget")
    require(limited.stats() == MIXED.READ.empty_stats(), "failed native source admission leaked temporary files")
    limited.finish()
    checks.append(
        "explicit native format rejects missing/duplicate/corrupt source bytes and budgets without canonical-path retry"
    )

    native.phase("maintain")
    original_files = [Path(file["uri"]) for file in admitted["capture"]["file_capture"]["files"]]
    require(
        all(not path.exists() for path in original_files),
        "concurrent GC did not physically remove all original B/F files",
    )
    worker.open("window", native.capture("window"), reference)
    window_rows = native.rows("window")
    window_result = worker.search("window", query)
    recalls.append(MIXED.qualify(window_result, window_rows, query))
    MIXED.check_routes(window_result, native.mixed("window"), indexed_ids)
    require(window_result == result, "flush/rewrite/expiration during native admission changed its frozen B/F/T view")
    require(
        worker.search("initial", query) == baseline and worker.search("current", query) == result,
        "concurrent maintenance invalidated an already admitted consumer",
    )
    worker.close_view("initial")
    worker.open("stale-layout", native.capture("latest"), reference, expected="Referenced index file is absent")
    latest_reference = build(proof, native.capture("latest"), "latest")
    worker.open("latest", native.capture("latest"), latest_reference)
    latest_rows = native.rows("latest")
    latest_result = worker.search("latest", query)
    recalls.append(MIXED.qualify(latest_result, latest_rows, query))
    require(
        [hit["row"] for hit in latest_result["hits"]] == [hit["row"] for hit in result["hits"]],
        "fresh post-maintenance admission lost logical row parity",
    )
    checks.append(
        "cleanup waits during native source acquisition; imported old B/F/T bytes remain usable after actual GC"
    )

    native.phase("failures")
    require(
        native.phases["failures"]["native_rejections"] == 4,
        "native partial-pin failure/cancellation/budgets were not exercised",
    )
    uncommitted = native.rows("uncommitted")
    require(all(row["id"] != 9999 for row in uncommitted), "native committed-only view exposed an uncommitted insert")
    worker.close_view("window")
    worker.open("uncommitted", native.capture("uncommitted"), latest_reference)
    unseen_result = worker.search("uncommitted", query)
    recalls.append(MIXED.qualify(unseen_result, uncommitted, query))
    require(unseen_result == latest_result, "rolled-back writer changed an admitted consumer")
    checks.append(
        "native cancellation and partial-pin failures release transaction/guard state; fresh acquisition works and uncommitted writes stay hidden"
    )

    native.phase("drop")
    require(
        native.phases["drop"]["active_views"] == native.phases["drop"]["source_bytes"] == 0,
        "native producer retained source buffers after close",
    )
    for path in native.root.glob("*.admitted.json"):
        path.unlink()
    for path in (reference, latest_reference):
        descriptor = json.loads(path.read_text())
        shutil.rmtree(path.parent / descriptor["generation"]["generation"])
    require(
        worker.search("current", query) == result and worker.search("latest", query) == latest_result,
        "consumer reopened producer payload or canonical index after the owner released it",
    )
    untouched = [next(row for row in current_rows if row["id"] == row_id) for row_id in (4001, 5127, 133, 6000, 4001)]
    require(
        worker.fetch("current", [MIXED.ref(row) for row in untouched])
        == [MIXED.READ.business(row) for row in untouched],
        "delayed B/F/T fetch lost the native snapshot or duplicate order",
    )
    unseen_query = untouched[0]["embedding"]
    recalls.append(MIXED.qualify(worker.search("current", unseen_query), current_rows, unseen_query))
    for name in ("latest", "uncommitted", "current"):
        stats = worker.close_view(name)
    require(stats == MIXED.READ.empty_stats(), "consumer close retained native staging or index resources")
    worker.search("current", query, expected="Unknown read view")
    worker.finish(eof=True)
    native.finish()
    checks.append(
        "producer/consumer lifetimes are independent; unseen search and ranked business fetch survive both owners' input cleanup"
    )
    return {
        "native_phases": native.phases,
        "fixture_recalls": recalls,
        "recall_threshold": 0.8,
        "reclaimed_source_files": len(original_files),
        "native_inline_rows": opened["inline_rows"],
        "index_builds": 2,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duckdb", required=True, type=Path)
    parser.add_argument("--driver", required=True, type=Path)
    parser.add_argument("--probe", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()
    root = args.output_dir.resolve()
    root.mkdir(parents=True, exist_ok=False)
    proof = BASE.Proof(args.duckdb.resolve(strict=True), args.probe.resolve(strict=True), root)
    driver = args.driver.resolve(strict=True)
    workers: list[Worker] = []
    checks: list[str] = []
    native = None
    report = {
        "duckdb_sha256": BASE.digest(proof.duckdb),
        "driver_sha256": BASE.digest(driver),
        "probe_sha256": BASE.digest(proof.probe),
        "vortex_revision": BASE.VORTEX_REVISION,
    }
    started = time.monotonic()
    try:
        native = Native(driver, root / "native")
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
            checks=checks,
        )
        (root / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
