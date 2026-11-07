#!/usr/bin/env python3
"""Deterministic durable R7 candidate ownership, cancellation and reclamation."""

from copy import deepcopy
import argparse
import ctypes
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import selectors
import signal
import shutil
import subprocess
import sys
import time
import types
import uuid

HERE = Path(__file__).resolve().parent


def load(name, filename):
    spec = importlib.util.spec_from_file_location(name, HERE / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


C = load("r7_candidate_registry", "vortex_spfresh_candidates.py")
O = load("r7_candidate_model", "vortex_spfresh_candidate_oracle.py")
H = load("r7_lire_cases", "test_vane_vortex_spfresh_lire.py")
F, U, K, BASE = C.F, H.H, H.K, H.BASE
require = BASE.require
RULE = {
    "schema": 1,
    "scope": "Local component registry around real SPFresh checkpoints; production SQL/provider unchanged, R6 paused.",
    "publication": "One fresh state.json atomically commits head plus candidate/retirement state; no legacy decoder or migration.",
    "owner": "Unique candidate/owner UUIDs and an inherited open-file-description flock; no PID, timeout, TTL or adoption.",
    "recovery": "Explicitly discard pending metadata, cancel unowned building/sealed candidates, and reconcile committed publication.",
    "gc": "<=16 entries (files, directories, lease and final record), <=64 MiB declared file bytes per explicit call; durable intent before unlink, conservative charges on missing planned entries.",
    "limits": "Registry <=1 MiB and <=16 records; native/source/vector/artifact/mutation/LIRE budgets and thread settings unchanged.",
    "deferred": [
        "R6",
        "production metadata registration",
        "dynamic provider",
        "background concurrency",
        "stable-load performance",
        "power loss",
    ],
}


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def rejected(report, name, function, code):
    try:
        function()
    except ValueError as error:
        require(str(error) == code, f"Wrong rejection for {name}: {error}")
    else:
        raise AssertionError("Expected rejection: " + name)
    report["rejections"].append({"name": name, "code": code})


def inspect(report, label, registry, model):
    value = model.inspect(registry.root)
    report["states"].append({"phase": label, **value})


def finish(report, session):
    for _ in range(2000):
        state = session.call("state")
        if state["queued"] == 0:
            return session.seal()
        state = session.call("drain", 16, F.LIMIT)
        H.trace(report, "registered-lire", state)
        require(state["steps"] > 0, "Full LIRE budget made no progress")
    raise AssertionError("Registered LIRE did not converge")


def collect(report, registry, model, name, objects=16, bytes_=F.LIMIT):
    before = O.files(registry.root)
    result = registry.collect(name, objects, bytes_)
    after = O.files(registry.root)
    value = O.gc_observation(before, after, result, objects, bytes_)
    value.update(id=name, object_budget=objects, byte_budget=bytes_)
    report["gc"].append(value)
    if result["objects"]:
        model.collected(name, name not in O.committed(registry.root)["records"])
    inspect(report, "GC", registry, model)
    return result


def clean(report, registry, model, name, objects=16):
    for _ in range(5000):
        if name not in O.committed(registry.root)["records"]:
            return
        value = collect(report, registry, model, name, objects)
        require(value["objects"] > 0, "Explicit GC did not make progress")
    raise AssertionError("GC finite operation count exhausted")


def invalid_registry(report, args, registry, handle):
    root = args.output_dir / "invalid-registry"
    shutil.copytree(registry.root, root)
    saved = O.committed(root)
    value = deepcopy(saved)
    value["format_version"] = 0
    (root / "state.json").write_bytes(F.encoded(value))
    rejected(report, "old-registry-format", lambda: C.Registry(root).snapshot(), "REGISTRY_FORMAT")
    value = deepcopy(saved)
    record = value["records"][handle["id"]]
    record.update(
        phase="deleting",
        gc={
            "entries": [
                {
                    "kind": "file",
                    "path": "../../../outside-gc-victim",
                    "size": 9,
                    "sha256": "0" * 64,
                    "dev": 0,
                    "ino": 0,
                }
            ],
            "pending": [0],
        },
    )
    (root / "state.json").write_bytes(F.encoded(value))
    victim = args.output_dir / "outside-gc-victim"
    victim.write_bytes(b"protected")
    before = O.files(root)
    rejected(report, "GC-path-escape", lambda: C.Registry(root).collect(handle["id"], 16, F.LIMIT), "GC_FORMAT")
    require(O.files(root) == before and victim.read_bytes() == b"protected", "Invalid GC modified protected bytes")


def point(stage, config, extra=None):
    if stage != config["stage"]:
        return
    print(json.dumps({"stage": stage, "affinity": sorted(os.sched_getaffinity(0)), **(extra or {})}), flush=True)
    require(sys.stdin.readline().strip() == "continue", "Fault controller closed")


def fault_child(config):
    registry = C.Registry(Path(config["registry"]))
    observe = lambda stage: point(stage, config)
    if config.get("gc") is not None:
        registry.collect(config["gc"], 1, F.LIMIT, observe)
        raise AssertionError("GC fault point not reached")
    session = C.Session(
        registry, Path(config["probe"]), config["capture"], config["revision"], Path(config["native_log"]), observe
    )
    try:
        if config["stage"] == "cancel-committed":
            session.cancel()
            raise AssertionError("Cancellation fault point not reached")
        if config["stage"] in ("native-after-split", "native-owner-handoff"):
            require(session.call("pause", "after_split") == {"armed": True}, "Native pause not armed")
            require(session.call("drain", 16, F.LIMIT) == {"pause": "after_split"}, "Native split pause not reached")
            native_pid = session.candidate.worker.process.pid
            if config["stage"] == "native-owner-handoff":
                os.kill(native_pid, signal.SIGSTOP)
            point(config["stage"], config, {"native_pid": native_pid})
            raise AssertionError("Native controller resumed unexpectedly")
        finish({"drains": []}, session)
        session.publish()
        raise AssertionError("Candidate fault point not reached")
    finally:
        session.stop()


def fault(report, args, registry, model, capture, stage, gc=None, handoff=False):
    before = O.committed(registry.root)
    config = {
        "registry": str(registry.root),
        "probe": str(args.probe),
        "capture": capture,
        "revision": before["head"]["revision"],
        "stage": stage,
        "gc": gc,
        "native_log": str(args.output_dir / (stage + "-native.log")),
    }
    with (args.output_dir / (stage + ".log")).open("wb") as log:
        process = subprocess.Popen(
            [sys.executable, "-I", str(Path(__file__).resolve()), "--fault-child"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=log,
            text=True,
            start_new_session=True,
        )
        native_pid = None
        try:
            process.stdin.write(json.dumps(config) + "\n")
            process.stdin.flush()
            with selectors.DefaultSelector() as selector:
                selector.register(process.stdout, selectors.EVENT_READ)
                require(selector.select(60), "Candidate fault handshake deadline")
            line = process.stdout.readline()
            require(bool(line), "Fault child ended: " + (args.output_dir / (stage + ".log")).read_text()[-2000:])
            event = json.loads(line)
            require(
                event["stage"] == stage and event["affinity"] == sorted(os.sched_getaffinity(0)),
                "Wrong fault handshake",
            )
            native_pid = event.get("native_pid")
            if handoff:
                os.kill(process.pid, signal.SIGKILL)
            else:
                os.killpg(process.pid, signal.SIGKILL)
            require(process.wait(timeout=30) == -9, "No actual SIGKILL")
            if not handoff:
                while True:
                    try:
                        os.waitpid(-process.pid, 0)
                    except ChildProcessError:
                        break
            after = O.committed(registry.root)
            if gc is None:
                added = set(after["records"]) - set(before["records"])
                require(len(added) == 1, "Fault candidate was not durably registered")
                name = added.pop()
                model.registered(name, capture)
                if stage in ("sealed-committed", "publish-pre-rename", "publish-committed"):
                    model.sealed(name)
                if stage == "publish-committed":
                    model.publish(name)
                elif stage == "cancel-committed":
                    model.cancel(name)
                else:
                    require(after["head"] == before["head"], "Unpublished candidate changed committed head")
            else:
                name = gc
                model.collected(name, False)
            inspect(report, stage, registry, model)
            handle = {"id": name, "owner": after["records"][name]["owner"]}
            if stage == "publish-pre-rename":
                rejected(report, "pending-metadata-blocks-write", lambda: registry.cancel(handle), "PENDING_METADATA")
                require(O.committed(registry.root) == after, "Rejected write adopted pending metadata")
            if handoff:
                require(
                    registry.recover(name) == {"phase": "building", "busy": True},
                    "Native child lost its inherited ownership lock",
                )
                registry.cancel(handle)
                model.cancel(name)
                require(
                    registry.recover(name) == {"phase": "cancelled", "busy": True},
                    "Cancelled live native child lost ownership",
                )
                result = collect(report, registry, model, name)
                require(result["blocked"] == "live-owner", "GC reclaimed a live orphan native child")
                os.kill(native_pid, signal.SIGKILL)
                require(os.waitpid(native_pid, 0)[1] == signal.SIGKILL, "Orphan child was not reaped")
                native_pid = None
            recovered = registry.recover_metadata()
            report["recovery"].append({"stage": stage, **recovered})
            if stage in ("publish-pre-rename", "gc-progress-pre-rename"):
                require(recovered["discarded_pending"], "Pending metadata was not explicitly discarded")
            if stage == "publish-committed":
                require(
                    registry.recover(name) == {"phase": "published", "busy": False}, "Committed publication was lost"
                )
            elif gc is None:
                if not handoff:
                    require(
                        registry.recover(name) == {"phase": "cancelled", "busy": False},
                        "Dead candidate was not cancelled",
                    )
                    model.cancel(name)
                inspect(report, "explicit-recovery-" + stage, registry, model)
            report["faults"].append(
                {"stage": stage, "exit_code": -9, "candidate": name, "native_lease_handoff": handoff}
            )
            return name
        finally:
            if native_pid is not None and handoff:
                os.kill(native_pid, signal.SIGKILL)
                os.waitpid(native_pid, 0)
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=30)


def exercise(args, report):
    lake = args.output_dir / "lake"
    lake.mkdir(mode=0o700)
    (lake / "indexes").mkdir(mode=0o700)
    connection = BASE.connect(lake, create=True)
    sessions, views = [], []
    try:
        connection.execute("SELECT * FROM ducklake_initialize_vortex_writes('lake')")
        K.manage(connection)
        rows = [
            {"id": i, "embedding": [float(i), *([0.0] * 6), -0.0], "category": None, "payload": None}
            for i in range(256)
        ]
        K.mutate(connection, "candidate-seed", rows)
        connection.execute("CALL ducklake_flush_inlined_data('lake',table_name=>'items')")
        revision = BASE.head(connection)["revision"]
        connection.execute(
            "SELECT * FROM ducklake_append_vortex_index_segment('lake','main','items','knn',"
            + BASE.quote(lake / "indexes/seed.json")
            + f",8,{revision})"
        )
        static = deepcopy(BASE.head(connection))
        initial = U.captured(connection, report)
        seed, state = C.L.seed(args.probe, args.output_dir / "seed", initial, args.output_dir / "seed.log")
        heads = sorted(state["heads"], key=lambda h: h["vector"][0])
        center = min(heads[1:-1], key=lambda h: (h["size"], h["id"]))["vector"][0]
        report["input"] = {"center": center}
        registry = C.Registry.create(args.output_dir / "registry", args.output_dir / "seed")
        first = registry.head()["generation"]
        model = O.Model(first, initial)
        old = registry.read(args.probe, initial, args.output_dir / "view-initial", args.output_dir / "view-initial.log")
        views.append(old)
        inspect(report, "initial", registry, model)
        H.qualify(report, "initial", old, initial)

        captures = [initial]
        for batch in range(2):
            rows = [
                {
                    "id": 10000 + i,
                    "embedding": [center + (-0.25 if i % 2 == 0 else 0.25) + (i % 64) / 8192, *([0.0] * 7)],
                    "category": "A\0" if i % 2 else None,
                    "payload": None,
                }
                for i in range(batch * 800, (batch + 1) * 800)
            ]
            receipt = K.mutate(connection, "candidate-burst-" + str(batch), rows)
            require(K.mutate(connection, "candidate-burst-" + str(batch), rows) == receipt, "Business replay changed")
            connection.execute("CALL ducklake_flush_inlined_data('lake',table_name=>'items')")
            captures.append(U.captured(connection, report))
            if batch == 0:
                for name, capture in (
                    ("table-uuid", deepcopy(captures[1])),
                    ("field-id", deepcopy(captures[1])),
                    ("old-snapshot", initial),
                ):
                    if name == "table-uuid":
                        capture["file_capture"]["table_uuid"] = str(uuid.uuid4())
                    if name == "field-id":
                        capture["file_capture"]["field_id"] += 1
                    rejected(
                        report,
                        name,
                        lambda capture=capture: registry.register(capture, 1),
                        "STALE_SNAPSHOT" if name == "old-snapshot" else "CAPTURE_BINDING_CHANGED",
                    )
                rejected(report, "stale-revision", lambda: registry.register(captures[1], 0), "PUBLICATION_CONFLICT")
                rejected(
                    report, "boolean-revision", lambda: registry.register(captures[1], True), "PUBLICATION_CONFLICT"
                )
                oversized = deepcopy(captures[1])
                oversized["rows"][0]["row"]["payload"] = "x" * F.MIB
                rejected(report, "oversized-capture", lambda: registry.register(oversized, 1), "MANIFEST_BUDGET")
                winner = C.Session(registry, args.probe, captures[1], 1, args.output_dir / "winner.log")
                sessions.append(winner)
                model.registered(winner.handle["id"], captures[1])
                loser = C.Session(registry, args.probe, captures[1], 1, args.output_dir / "loser.log")
                sessions.append(loser)
                model.registered(loser.handle["id"], captures[1])
                rejected(report, "unsealed-publication", lambda: registry.publish(loser.handle), "CANDIDATE_STATE")
                require(registry.recover(loser.handle["id"])["busy"], "A live candidate was recovered")
                finish(report, winner)
                model.sealed(winner.handle["id"])
                inspect(report, "sealed", registry, model)
                winner.publish()
                model.publish(winner.handle["id"])
                inspect(report, "published", registry, model)
                require(
                    collect(report, registry, model, first)["blocked"] == "source-pin",
                    "A losing candidate lost its source pin",
                )
                rejected(report, "stale-worker", lambda: loser.call("state"), "PUBLICATION_CONFLICT")
                wrong = dict(loser.handle, owner=uuid.uuid4().hex)
                rejected(report, "wrong-owner", lambda: registry.cancel(wrong), "CANDIDATE_OWNER")
                registry.cancel(loser.handle)
                model.cancel(loser.handle["id"])
                require(
                    collect(report, registry, model, loser.handle["id"])["blocked"] == "live-owner",
                    "Cancellation discarded a live native lease",
                )
                require(
                    collect(report, registry, model, first)["blocked"] == "source-pin",
                    "Cancelled candidate released its source too soon",
                )
                rejected(report, "cancelled-worker", lambda: loser.call("drain", 16, F.LIMIT), "CANDIDATE_STATE")
                loser.stop()
                invalid_registry(report, args, registry, loser.handle)
                rejected(
                    report, "referenced-head", lambda: registry.collect(winner.handle["id"], 16, F.LIMIT), "REFERENCED"
                )
                for i, (objects, bytes_) in enumerate(((0, F.LIMIT), (17, F.LIMIT), (16, 0), (16, F.LIMIT + 1))):
                    rejected(
                        report,
                        "GC-budget-" + str(i),
                        lambda objects=objects, bytes_=bytes_: registry.collect(loser.handle["id"], objects, bytes_),
                        "GC_BUDGET",
                    )
                require(
                    collect(report, registry, model, loser.handle["id"], 1, 1)["blocked"] == "byte-budget",
                    "Tiny byte budget deleted a file",
                )
                clean(report, registry, model, loser.handle["id"], objects=1)
                rejected(report, "collected-handle", lambda: registry.cancel(loser.handle), "NO_CANDIDATE")
                clean(report, registry, model, first)
                H.qualify(report, "initial-after-GC", old, initial)

        target = captures[2]
        current = registry.read(
            args.probe, captures[1], args.output_dir / "view-before-LIRE", args.output_dir / "view-before-LIRE.log"
        )
        views.append(current)
        require(len(registry.snapshot()["records"]) == 1, "Old candidate slots were not reclaimed")
        handles = []
        for _ in range(15):
            handle, lease = registry.register(target, 2)
            lease.close()
            handles.append(handle)
            model.registered(handle["id"], target)
        rejected(report, "candidate-capacity", lambda: registry.register(target, 2), "REGISTRY_CAPACITY")
        inspect(report, "full-registry", registry, model)
        for handle in handles:
            require(registry.recover(handle["id"])["phase"] == "cancelled", "Unowned intent not cancelled")
            model.cancel(handle["id"])
            clean(report, registry, model, handle["id"], objects=1)

        stages = (
            "register-committed",
            "lease-committed",
            "copied",
            "mutated",
            "cancel-committed",
            "native-after-split",
            "sealed-files",
            "sealed-committed",
            "publish-pre-rename",
        )
        for stage in stages:
            name = fault(report, args, registry, model, target, stage)
            if stage == "publish-pre-rename":
                require(not (registry.root / "state.pending").exists(), "Pending metadata survived explicit recovery")
            if stage == "mutated":
                for gc_stage in ("gc-unlinked", "gc-progress-pre-rename"):
                    fault(report, args, registry, model, target, gc_stage, gc=name)
                    pending = O.committed(registry.root)["records"][name]["gc"]
                    require(len(pending["pending"]) == 1, "GC crash did not leave one durable pending item")
                    planned = pending["entries"][pending["pending"][0]]
                    require(planned["kind"] == "file" and planned["size"] > 0, "GC test did not unlink a real file")
                    result = collect(report, registry, model, name, 1)
                    require(
                        result["objects"] == 1
                        and result["bytes"] == planned["size"]
                        and report["gc"][-1]["physical_bytes"] == 0,
                        "Resumed missing entry was not conservatively charged",
                    )
            clean(report, registry, model, name)
            H.qualify(report, "old-after-" + stage, current, captures[1])

        name = fault(report, args, registry, model, target, "native-owner-handoff", handoff=True)
        clean(report, registry, model, name)
        H.qualify(report, "old-after-native-handoff", current, captures[1])

        final_name = fault(report, args, registry, model, target, "publish-committed")
        actual = F.verify(registry.generation(final_name))
        report["physical"] = H.O.observe(args.probe, registry.generation(final_name) / "native", actual)
        require(
            report["physical"]["head_slots"] > 64 and report["physical"]["version_changes"] > 0,
            "Registered publication did not execute real LIRE",
        )
        final = registry.read(args.probe, target, args.output_dir / "view-final", args.output_dir / "view-final.log")
        views.append(final)
        H.qualify(report, "final-reopened", final, target)
        clean(report, registry, model, winner.handle["id"])
        H.qualify(report, "old-after-final-GC", current, captures[1])
        H.qualify(report, "final-after-GC", final, target)
        for label, change in (
            ("old-head", lambda v: v["head"].update(generation=winner.handle["id"])),
            ("wrong-watermark", lambda v: v["records"][final_name]["target"].update(snapshot_id=1)),
            ("forged-phase", lambda v: v["records"][final_name].update(phase="building")),
            ("wrong-source-ownership", lambda v: v["records"][final_name].update(pin_source=True)),
        ):
            value = O.committed(registry.root)
            change(value)
            try:
                model.inspect(registry.root, value)
            except AssertionError as error:
                report["oracle_rejections"].append({"name": label, "reason": str(error)})
            else:
                raise AssertionError("Unsafe registry accepted by independent model: " + label)
        require(BASE.head(connection) == static, "Candidate protocol changed production static publication")
        report["final_head"] = registry.head()
        report["final_registry"] = registry.snapshot()
        for view in views:
            view.close()
        views.clear()
    finally:
        for session in sessions:
            session.stop()
        for view in views:
            view.stop()
        connection.close()


def sources():
    paths, seen = set(), set()

    def walk(module):
        if id(module) in seen:
            return
        seen.add(id(module))
        path = Path(module.__file__).resolve() if getattr(module, "__file__", None) else None
        if path is None or path.parent != HERE:
            return
        paths.add(path)
        for value in vars(module).values():
            if isinstance(value, types.ModuleType):
                walk(value)

    for module in (C, O, H):
        walk(module)
    paths.add(Path(__file__).resolve())
    return paths


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--probe", type=Path, required=True)
    parser.add_argument("--build-report", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--cpus", required=True)
    args = parser.parse_args()
    args.probe = args.probe.resolve()
    args.output_dir = args.output_dir.resolve()
    args.output_dir.mkdir(mode=0o700)
    build = json.loads(args.build_report.read_bytes())
    require(
        build["status"] == "built"
        and build["profile"] == "lire-fixed"
        and build["binary_sha256"] == digest(args.probe),
        "Native build mismatch",
    )
    for name, sha in build["input_sha256"].items():
        require(digest(Path(name)) == sha, "Recorded native input changed")
        if "/test/vane/" in name:
            require(
                digest(HERE / name.split("/test/vane/", 1)[1]) == sha,
                "Current native source differs from verified build",
            )
    cpus = U.PIN.parse_cpus(args.cpus)
    original = os.sched_getaffinity(0)
    require(set(cpus) <= original, "CPU set unavailable")
    hashes = {str(p): digest(p) for p in sources()}
    threads = {k: os.environ.get(k) for k in (*U.PIN.THREAD_ENV, "TORCH_NUM_THREADS")}
    report = {
        "status": "running",
        "rule": RULE,
        "source_sha256": hashes,
        "probe_sha256": digest(args.probe),
        "build_report_sha256": digest(args.build_report),
        "threads": threads,
        "original_affinity": sorted(original),
        "captures": [],
        "queries": [],
        "faults": [],
        "rejections": [],
        "oracle_rejections": [],
        "gc": [],
        "states": [],
        "drains": [],
        "recovery": [],
    }
    started = time.monotonic()
    libc = ctypes.CDLL(None, use_errno=True)
    previous = ctypes.c_int()
    require(libc.prctl(37, ctypes.byref(previous), 0, 0, 0) == 0, "Cannot inspect test subreaper")
    require(libc.prctl(36, 1, 0, 0, 0) == 0, "Cannot set test subreaper")
    try:
        os.sched_setaffinity(0, cpus)
        report["applied_affinity"] = sorted(os.sched_getaffinity(0))
        exercise(args, report)
        require(hashes == {str(p): digest(Path(p)) for p in hashes}, "Executed source changed")
        require(threads == {k: os.environ.get(k) for k in threads}, "Thread settings changed")
        report["status"] = "passed"
    except BaseException as error:
        report.update(status="failed", error=f"{type(error).__name__}: {error}")
        raise
    finally:
        require(libc.prctl(36, previous.value, 0, 0, 0) == 0, "Cannot restore test subreaper")
        os.sched_setaffinity(0, original)
        report["restored_affinity"] = sorted(os.sched_getaffinity(0))
        report["seconds_observation_only"] = time.monotonic() - started
        (args.output_dir / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(
        json.dumps(
            {
                "status": report["status"],
                "queries": len(report["queries"]),
                "faults": len(report["faults"]),
                "gc_calls": len(report["gc"]),
                "rejections": len(report["rejections"]),
                "oracle_rejections": len(report["oracle_rejections"]),
            }
        )
    )


if __name__ == "__main__":
    if sys.argv[1:] == ["--fault-child"]:
        fault_child(json.loads(sys.stdin.readline()))
    else:
        main()
