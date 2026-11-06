#!/usr/bin/env python3
"""Deterministic registered-scratch ownership, budgets and real SIGKILL recovery."""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import importlib.metadata
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


REF = load("scratch_reference", "test_vane_vortex_reference_wal.py")
MODEL = load("scratch_model", "vortex_query_scratch_model.py")
BASE, PIN, WRITE = REF.BASE, REF.PIN, REF.WRITE
require, query, quote = BASE.require, BASE.query, BASE.quote
SCRIPT = Path(__file__).resolve()
MIB = 1024 * 1024
CUTS = (
    "scratch_plan_published",
    "scratch_before_unlink",
    "scratch_after_unlink",
    "scratch_before_remove_view",
    "scratch_after_remove_view",
)


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def wait_at(process, checkpoint, expected, cpus):
    deadline = time.monotonic() + 120
    while not checkpoint.is_file():
        require(
            process.poll() is None and time.monotonic() < deadline,
            "Crash cutpoint not reached: " + str(checkpoint),
        )
        time.sleep(0.01)
    require(process.poll() is None, "Cutpoint owner exited")
    require(
        set(os.sched_getaffinity(process.pid)) == set(cpus),
        "Child CPU affinity changed",
    )
    value = checkpoint.read_text()
    try:
        value = json.loads(value)["stage"]
    except json.JSONDecodeError:
        pass
    require(value == expected, "Wrong crash cutpoint: " + str(value))


def kill(process, report, tag):
    process.kill()
    require(process.wait(timeout=20) == -9, "Crash must use actual SIGKILL")
    report["sigkill"].append(tag)


class Probe:
    def __init__(self, binary, path, report):
        self.binary, self.path, self.report = binary, path, report
        path.mkdir(mode=0o700)
        self.data, self.parent = path / "data", path / "private"
        self.data.mkdir(mode=0o700)
        self.parent.mkdir(mode=0o700)
        self.root = MODEL.root_for(self.parent, self.data)
        self.sequence = 0
        self.owners = {}

    def start(self, operation, **kwargs):
        self.sequence += 1
        input_ = self.path / f"{self.sequence:03}.input.json"
        output = self.path / f"{self.sequence:03}.output.json"
        spec = dict(
            operation=operation,
            data_path=str(self.data),
            parent=str(self.parent),
            root=str(self.root),
            sizes=[],
            max_objects=16,
            max_bytes=64 * MIB,
            stop=None,
            fail=None,
        )
        spec.update(kwargs)
        input_.write_text(json.dumps(spec) + "\n")
        with output.with_suffix(".log").open("w") as log:
            process = subprocess.Popen(
                [str(self.binary), "scratch-probe", str(input_), str(output)],
                stdout=log,
                stderr=subprocess.STDOUT,
            )
        return process, output

    def call(self, operation="collect", error=None, **kwargs):
        process, output = self.start(operation, **kwargs)
        require(
            process.wait(timeout=120) == 0,
            "Probe process failed: " + str(output.with_suffix(".log")),
        )
        value = json.loads(output.read_text())
        if error is not None:
            require(
                not value["ok"] and error.lower() in value["error"].lower(),
                "Unexpected rejection: " + str(value),
            )
            return value
        require(value["ok"], "Probe rejected: " + str(value))
        if operation == "collect":
            MODEL.assert_budget(
                value["result"],
                kwargs.get("max_objects", 16),
                kwargs.get("max_bytes", 64 * MIB),
            )
        return value["result"]

    def owner(self, sizes=(17, 31), stop="payload_ready", crash=True):
        prior = set(MODEL.registrations(self.root)) if self.root.exists() else set()
        process, output = self.start("owner", sizes=list(sizes), stop=stop)
        try:
            wait_at(process, output.with_suffix(".checkpoint.json"), stop, self.report["applied_affinity"])
        except BaseException:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=20)
            raise
        require(
            Path(json.loads(output.with_suffix(".checkpoint.json").read_text())["root"]) == self.root,
            "Root differs from independent canonical data-path hash",
        )
        MODEL.assert_root(self.root, self.data)
        added = set(MODEL.registrations(self.root)) - prior
        require(len(added) == 1, "Owner did not publish one unique registration")
        self.owners[process.pid] = added.pop()
        if crash:
            kill(process, self.report, "probe-owner:" + self.path.name + ":" + stop)
        return process, output

    def drain(self, **kwargs):
        passes = []
        for _ in range(64):
            result = self.call(**kwargs)
            passes.append(result)
            if result["remaining_views"] == 0:
                MODEL.assert_released(self.parent)
                return passes
        raise AssertionError("Registered dead owners did not drain")

    def work(self):
        records = MODEL.registrations(self.root)
        require(len(records) == 1, "Fixture must own one registered view")
        return self.root / "work" / next(iter(records))

    def seal_rejection(self, marker):
        before = MODEL.fingerprint(self.root)
        self.call(error=marker)
        require(
            MODEL.fingerprint(self.root) == before,
            "Rejected cleanup changed the registered tree",
        )


def generic(probe, root, report):
    def case(name):
        report["protocol_cases"].append(name)
        print("protocol", name, flush=True)
        return Probe(probe, root / name, report)

    p = case("normal-release")
    foreign = p.parent / ".ducklake-vortex-query-legacy"
    foreign.mkdir(mode=0o700)
    (foreign / "keep").write_text("unknown namespace")
    p.call("owner", sizes=[17, 31])
    require(
        (foreign / "keep").read_text() == "unknown namespace",
        "Adopted an unregistered namespace",
    )
    foreign.rename(p.path / "legacy-preserved")
    MODEL.assert_released(p.parent)

    p = case("active-owner")
    process, output = p.owner(crash=False)
    try:
        before = MODEL.fingerprint(p.work())
        result = p.call()
        require(
            result["blocked_views"] == 1 and result["collected_objects"] == 0,
            "Active owner was not protected",
        )
        require(MODEL.fingerprint(p.work()) == before, "Traversed or changed active payload")
        output.with_suffix(".release").touch()
        require(
            process.wait(timeout=30) == 0 and json.loads(output.read_text())["ok"],
            "Live owner failed release",
        )
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=20)
    MODEL.assert_released(p.parent)

    p = case("allocation-intent")
    process, _ = p.owner(stop="scratch_allocated", crash=False)
    try:
        p.seal_rejection("busy")
        kill(process, report, "allocation-intent")
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=20)
    result = p.call(max_objects=1, max_bytes=1)
    require(
        result["removed_views"] == 1 and result["collected_bytes"] == 0,
        "Allocation intent not reclaimed",
    )
    MODEL.assert_released(p.parent)

    p = case("object-byte-boundary")
    p.owner()
    result = p.call(max_objects=1, max_bytes=16)
    require(result["collected_objects"] == 0, "Deleted an over-budget file")
    result = p.call(max_objects=1, max_bytes=17)
    require(
        result["collected_files"] == 1 and result["collected_bytes"] == 17,
        "Exact byte boundary failed",
    )
    require(
        sum(x["collected_bytes"] for x in p.drain(max_objects=1)) == 31,
        "Remaining payload bytes changed",
    )

    for name, sizes in (
        ("exact-64mib", [64 * MIB]),
        ("global-64mib", [32 * MIB + 1] * 2),
    ):
        p = case(name)
        p.owner(sizes=sizes)
        result = p.call(max_bytes=(64 * MIB - 1 if len(sizes) == 1 else 64 * MIB))
        require(
            result["collected_files"] == (0 if len(sizes) == 1 else 1),
            "Byte quota multiplied across files",
        )
        passes = [result, *p.drain()]
        require(
            sum(x["collected_bytes"] for x in passes) == sum(sizes),
            "Payload byte accounting changed",
        )

    p = case("invalid-budget-and-root")
    p.owner()
    before = MODEL.fingerprint(p.root)
    for objects, bytes_ in ((0, 1), (17, 1), (1, 0), (1, 64 * MIB + 1)):
        p.call(max_objects=objects, max_bytes=bytes_, error="budgets")
    other = p.path / "other-data"
    other.mkdir()
    p.call(data_path=str(other), error="does not belong")
    p.call(root=str(p.parent), error="does not belong")
    require(MODEL.fingerprint(p.root) == before, "Invalid arguments changed ownership")
    p.drain()

    for kind in (
        "unknown-control",
        "payload-symlink",
        "payload-hardlink",
        "work-identity",
        "lock-identity",
        "payload-identity",
    ):
        p = case(kind)
        p.owner()
        work = p.work()
        witness = p.path / "witness"
        witness.write_text("foreign bytes")
        original = None
        if kind == "unknown-control":
            injected = work / "unknown"
            injected.write_text("preserve")
            marker = "Unknown file outside"
        elif kind.startswith("payload-") and kind.endswith("link"):
            injected = work / "payload" / "injected"
            if kind.endswith("symlink"):
                injected.symlink_to(witness)
            else:
                os.link(witness, injected)
            marker = "ownership"
        else:
            injected = {
                "work-identity": work,
                "lock-identity": work / "owner.lock",
                "payload-identity": work / "payload",
            }[kind]
            original = p.path / "saved-original"
            injected.rename(original)
            if kind == "lock-identity":
                injected.touch(mode=0o600)
            else:
                injected.mkdir(mode=0o700)
            marker = "identity"
        p.seal_rejection(marker)
        require(witness.read_text() == "foreign bytes", "Changed a foreign target")
        if injected.is_dir():
            injected.rmdir()
        else:
            injected.unlink()
        if original:
            original.rename(injected)
        p.drain()

    for kind in ("post-plan-addition", "unintended-missing", "planned-file-identity"):
        p = case(kind)
        p.owner()
        p.call(fail="scratch_plan_published", error="Injected scratch")
        work = p.work()
        if kind == "post-plan-addition":
            target = work / "payload" / "unknown-after-plan"
            target.write_text("preserve")
            p.seal_rejection("Unknown payload")
            target.unlink()
        else:
            target = next(work.joinpath("payload").rglob("*.bin"))
            saved = p.path / "saved-original"
            target.rename(saved)
            if kind == "planned-file-identity":
                target.write_bytes(saved.read_bytes())
            p.seal_rejection("filesystem mismatch")
            if target.exists():
                target.unlink()
            saved.rename(target)
        p.drain()

    p = case("namespace-bindings")
    p.owner()
    for part in ("registry", "work"):
        target, saved = p.root / part, p.path / (part + "-saved")
        target.rename(saved)
        target.mkdir(mode=0o700)
        p.seal_rejection("identity")
        target.rmdir()
        saved.rename(target)
    copied = p.path / "copied-parent"
    copied.mkdir(mode=0o700)
    clone = copied / p.root.name
    shutil.copytree(p.root, clone)
    shutil.copy2(p.root.with_name(p.root.name + ".lock"), clone.with_name(clone.name + ".lock"))
    before = MODEL.fingerprint(copied)
    p.call(root=str(clone), error="registration identity")
    require(MODEL.fingerprint(copied) == before, "Adopted copied registration")
    p.drain()

    for stage in ("scratch_before_unlink", "scratch_after_unlink"):
        p = case("retry-" + stage)
        p.owner()
        p.call(fail=stage, error="Injected scratch")
        record = next(iter(MODEL.registrations(p.root).values()))
        pending = [e for e in record["plan"] if e["progress"] == "pending"]
        require(len(pending) == 1, "Must have one durable deletion intent")
        exists = p.work().joinpath("payload", pending[0]["path"]).exists()
        require(exists == (stage == "scratch_before_unlink"), "Wrong unlink cutpoint")
        result = p.call(max_objects=1, max_bytes=1)
        require(
            result["collected_files"] == 0 and result["collected_bytes"] == 0,
            "Recharged a completed unlink",
        )
        record = next(iter(MODEL.registrations(p.root).values()))
        require(
            sum(e["progress"] == "pending" for e in record["plan"]) == int(exists),
            "Created multiple intents",
        )
        p.drain(max_objects=1)

    for stage in CUTS:
        p = case("kill-" + stage)
        p.owner()
        process, output = p.start("collect", stop=stage)
        try:
            wait_at(
                process,
                output.with_suffix(".checkpoint.json"),
                stage,
                report["applied_affinity"],
            )
            kill(process, report, "probe-collect:" + stage)
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=20)
        p.drain(max_objects=1)

    p = case("cursor-fairness")
    live = []
    try:
        for _ in range(18):
            live.append(p.owner(sizes=(), crash=False))
        victim = max(MODEL.registrations(p.root))
        index = next(i for i, (process, _) in enumerate(live) if p.owners[process.pid] == victim)
        kill(live[index][0], report, "cursor-fairness-dead-owner")
        first, second = p.call(), p.call()
        require(
            first["blocked_views"] == 16 and first["removed_views"] == 0,
            "Cursor fixture order changed",
        )
        require(second["removed_views"] == 1, "Active owners starved a later dead owner")
        for process, output in live:
            if process.poll() is None:
                output.with_suffix(".release").touch()
                require(
                    process.wait(timeout=30) == 0 and json.loads(output.read_text())["ok"],
                    "Live close failed",
                )
    finally:
        for process, _ in live:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=20)
    MODEL.assert_released(p.parent)


def protected_files(root):
    return {
        str(path.relative_to(root)): {"bytes": path.stat().st_size, "sha256": digest(path)}
        for directory in ("data", "indexes")
        for path in sorted((root / directory).rglob("*"))
        if path.is_file()
    }


def sql_collect(c, root, objects=2, bytes_=64 * MIB):
    result = query(
        c,
        "SELECT * FROM ducklake_collect_vortex_query_scratch('lake'," + quote(root) + f",{objects},{bytes_})",
    )
    require(len(result) == 1, "Collector must return one typed accounting row")
    MODEL.assert_budget(result[0], objects, bytes_)
    return result[0]


def recover(root, changed=False):
    os.environ["TMPDIR"] = str(root / "private")
    expected = json.loads((root / "expected.json").read_text())
    model = REF.model() if changed else [[i, [float(i), 0.0], "A", f"seed-{i}"] for i in range(256)]
    require(
        expected["rows"] == model,
        "Native fixture differs from independent typed row model",
    )
    sealed = json.loads((root / "sealed.json").read_text())
    require(
        protected_files(root) == sealed,
        "Native source/generation/reference files changed",
    )
    scratch = Path((root / "scratch-root").read_text())
    MODEL.assert_root(scratch, root / "data")
    before = MODEL.fingerprint(scratch / "work")
    registered = len(MODEL.registrations(scratch))
    c = BASE.connect(root)
    try:
        require(
            REF.snapshot(c) == expected,
            "Reopen changed one of 13 metadata tables, snapshots or typed rows",
        )
        require(
            MODEL.fingerprint(scratch / "work") == before,
            "Reopen implicitly collected private payload",
        )
        require(not BASE.status(c)["running"], "Reopen resumed optimization")
        REF.check_query(c, model)
        require(
            MODEL.fingerprint(scratch / "work") == before,
            "New query changed another private owner",
        )
        c.execute("BEGIN")
        try:
            WRITE.rejected(lambda: sql_collect(c, scratch), "autocommit")
        finally:
            c.execute("ROLLBACK")
        for objects, bytes_ in ((0, 1), (17, 1), (1, 0), (1, 64 * MIB + 1)):
            WRITE.rejected(lambda: sql_collect(c, scratch, objects, bytes_), "budget")
        WRITE.rejected(lambda: sql_collect(c, root / "private"), "does not belong")
        WRITE.rejected(
            lambda: c.execute("SELECT * FROM ducklake_collect_vortex_query_scratch('lake',NULL,2,67108864)"),
            "cannot be NULL",
        )
        require(
            MODEL.fingerprint(scratch / "work") == before,
            "Rejected SQL changed private payload",
        )
        passes = []
        for _ in range(64):
            result = sql_collect(c, scratch)
            passes.append(result)
            if result["remaining_views"] == 0:
                break
        else:
            raise AssertionError("Native private payload did not drain")
        require(
            sum(x["removed_views"] for x in passes) == registered,
            "Lost a registered native owner",
        )
        require(
            sql_collect(c, scratch)["collected_objects"] == 0,
            "Repeat collection changed empty root",
        )
        MODEL.assert_released(root / "private")
        REF.check_query(c, model)
        require(
            REF.snapshot(c) == expected and protected_files(root) == sealed,
            "Scratch GC changed canonical state",
        )
        result = {
            "status": "passed",
            "registered_before": registered,
            "collection": passes,
            "cpus": sorted(os.sched_getaffinity(0)),
            "threads": PIN.thread_environment(),
        }
        (root / "recovery.json").write_text(json.dumps(result, indent=2) + "\n")
        return result
    finally:
        c.close()


def recovery_process(case, changed, report):
    with (case / "recovery.log").open("w") as log:
        result = subprocess.run(
            [
                sys.executable,
                "-I",
                str(SCRIPT),
                "--recover",
                str(case),
                "--changed",
                str(int(changed)),
            ],
            stdout=log,
            stderr=subprocess.STDOUT,
            timeout=120,
        )
    require(
        result.returncode == 0,
        "Installed scratch recovery failed: " + str(case / "recovery.log"),
    )
    evidence = json.loads((case / "recovery.json").read_text())
    require(
        evidence["cpus"] == report["applied_affinity"] and evidence["threads"] == report["thread_environment"],
        "Recovery affinity or threads changed",
    )
    # Provider extraction caches are reproducible; preserve all databases, payloads and proof records.
    if (case / "extensions").is_dir():
        shutil.rmtree(case / "extensions")


def native(driver, root, report):
    def start(case, mode):
        with (case / (mode + ".log")).open("w") as log:
            return subprocess.Popen(
                [str(driver), str(case), mode],
                env=dict(os.environ, TMPDIR=str(case / "private")),
                stdout=log,
                stderr=subprocess.STDOUT,
            )

    def new_case(name):
        case = root / name
        case.mkdir(mode=0o700)
        case.joinpath("private").mkdir(mode=0o700)
        return case

    def seal(case):
        case.joinpath("sealed.json").write_text(json.dumps(protected_files(case), indent=2) + "\n")

    def crash_query(case, stage):
        process = start(case, "scratch-query-pause-" + stage)
        try:
            wait_at(
                process,
                case / "checkpoint",
                "query-" + stage,
                report["applied_affinity"],
            )
            seal(case)
            scratch = Path((case / "scratch-root").read_text())
            require(
                len(MODEL.registrations(scratch)) == 1,
                "Query did not retain one registered owner",
            )
            if stage in ("after_open", "before_fetch"):
                require(
                    any(p.is_file() for p in scratch.joinpath("work").rglob("*.bin")),
                    "No actual private index copies",
                )
            kill(process, report, "native-query:" + case.name + ":" + stage)
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=20)

    for mode in ("normal", "query-cancel", "allocation-cancel", "query-fail", "held"):
        case = new_case("native-" + mode)
        process = start(case, "scratch-" + mode)
        require(process.wait(timeout=120) == 0, "Native scratch case failed: " + str(case))
        require(
            (case / "checkpoint").read_text() == "scratch-complete",
            "Native case incomplete",
        )
        MODEL.assert_released(case / "private")
        seal(case)
        recovery_process(case, mode == "held", report)
        report["native_cases"].append(mode)
        print("native and reopen", mode, "passed", flush=True)

    for stage in (
        "scratch_allocated",
        "scratch_registered",
        "source_pinned",
        "after_open",
        "before_fetch",
    ):
        case = new_case("query-kill-" + stage)
        crash_query(case, stage)
        recovery_process(case, False, report)
        report["native_query_crashes"].append(stage)
        print("query SIGKILL and reopen", stage, "passed", flush=True)

    for mode in [*("pause-" + stage for stage in CUTS), "cancel", "fail"]:
        case = new_case("collect-" + mode)
        crash_query(case, "before_fetch")
        case.joinpath("checkpoint").unlink()
        process = start(case, "scratch-collect-" + mode)
        try:
            if mode.startswith("pause-"):
                stage = mode.removeprefix("pause-")
                wait_at(
                    process,
                    case / "checkpoint",
                    "collect-" + stage,
                    report["applied_affinity"],
                )
                kill(process, report, "native-collect:" + stage)
            else:
                require(
                    process.wait(timeout=120) == 0 and case.joinpath("checkpoint").read_text() == "collect-failed",
                    "Native collector cancellation/failure was not exercised",
                )
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=20)
        recovery_process(case, False, report)
        report["native_collection_faults"].append(mode)
        print("collector fault and reopen", mode, "passed", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--probe", type=Path)
    parser.add_argument("--driver", type=Path)
    parser.add_argument("--cpus", type=PIN.parse_cpus)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--protocol-only", action="store_true")
    parser.add_argument("--recover", type=Path)
    parser.add_argument("--changed", type=int, choices=(0, 1), default=0)
    args = parser.parse_args()
    if args.recover:
        print(json.dumps(recover(args.recover.resolve(), bool(args.changed))))
        return
    require(
        args.probe and args.cpus and args.output_dir and (args.protocol_only or args.driver),
        "Require probe, driver, CPU set and fresh output",
    )
    root = args.output_dir.resolve()
    root.mkdir(mode=0o700)
    repo = SCRIPT.parents[2]
    tracked = [
        SCRIPT,
        Path(MODEL.__file__),
        Path(REF.__file__),
        Path(REF.EPOCH.__file__),
        Path(REF.KEYED.__file__),
        Path(REF.WRITE.__file__),
        Path(BASE.__file__),
        Path(BASE.SEARCH.__file__),
        Path(BASE.SEARCH.BASE.__file__),
        Path(PIN.__file__),
    ]
    tracked += [
        repo / name
        for name in (
            "src/vortex_query/src/scratch.rs",
            "src/vortex_query/src/ffi.rs",
            "src/vortex_query/Cargo.toml",
            "Cargo.lock",
            "src/functions/ducklake_vortex_search_function.cpp",
            "src/storage/ducklake_vortex_search.cpp",
            "src/include/storage/ducklake_vortex_query_ffi.hpp",
            "src/include/storage/ducklake_vortex_search.hpp",
            "test/vane/native_vortex_scratch_fixture.hpp",
            "test/vane/native_vortex_write_driver.cpp",
            "test/vane/native_vortex_lifecycle_fixture.hpp",
            "test/vane/vortex_index_probe/src/scratch_probe.rs",
        )
    ]
    hashes = {str(p): digest(p) for p in tracked}
    binaries = {str(p.resolve()): digest(p) for p in (args.probe, args.driver) if p is not None}
    providers = []
    if not args.protocol_only:
        native_path = Path(importlib.util.find_spec("vane._native").origin)
        binaries[str(native_path)] = digest(native_path)
        for distribution in importlib.metadata.distributions():
            if distribution.metadata["Name"].replace("_", "-").startswith("vane-extension-"):
                artifacts = [
                    distribution.locate_file(p) for p in distribution.files if str(p).endswith(".duckdb_extension")
                ]
                providers.append(
                    {
                        "name": distribution.metadata["Name"],
                        "version": distribution.version,
                        "artifacts": {str(p): digest(p) for p in artifacts},
                    }
                )
                binaries.update({str(p): digest(p) for p in artifacts})
    report = dict(
        status="running",
        providers=providers,
        mode="functional_only",
        source_sha256=hashes,
        binary_sha256=binaries,
        protocol_cases=[],
        native_cases=[],
        native_query_crashes=[],
        native_collection_faults=[],
        sigkill=[],
        thread_environment=PIN.thread_environment(),
        original_affinity=sorted(os.sched_getaffinity(0)),
        acceptance="Ownership, exact budgets and recovery only; stable-load A/A and performance A/B deferred.",
    )
    started = time.monotonic()
    try:
        with PIN.pinned_affinity(args.cpus):
            report["applied_affinity"] = sorted(os.sched_getaffinity(0))
            generic(args.probe.resolve(), root, report)
            if not args.protocol_only:
                native(args.driver.resolve(), root, report)
            require(
                PIN.thread_environment() == report["thread_environment"],
                "Thread settings changed",
            )
            require(
                {str(p): digest(p) for p in tracked} == hashes,
                "Executed source changed",
            )
            require(
                all(digest(Path(p)) == sha for p, sha in binaries.items()),
                "Executed binary changed",
            )
        report["status"] = "passed"
    except BaseException as error:
        report.update(status="failed", error=f"{type(error).__name__}: {error}")
        raise
    finally:
        report["restored_affinity"] = sorted(os.sched_getaffinity(0))
        report["seconds_observation_only"] = time.monotonic() - started
        (root / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({key: report[key] for key in ("status", "protocol_cases", "native_cases", "sigkill")}))


if __name__ == "__main__":
    main()
