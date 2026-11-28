"""Explicit LIRE profile over the existing sealed-checkpoint experiment."""

from copy import deepcopy
import fcntl
import importlib.util
from pathlib import Path
import struct

spec = importlib.util.spec_from_file_location(
    "lire_local_files", Path(__file__).with_name("vortex_spfresh_update_fixture.py")
)
F = importlib.util.module_from_spec(spec)
spec.loader.exec_module(F)


def append(worker, rows, first_vid):
    reply = worker.call("add", len(rows), *[v for row in rows for v in row["row"]["embedding"]])
    remaining = reply.get("pending_add", 0)
    F.require(type(remaining) is int and 0 <= remaining <= len(rows), "ADD_IDENTITY")
    expected = {"ids": list(range(first_vid, first_vid + len(rows) - remaining))}
    if remaining:
        expected["pending_add"] = remaining
    F.require(reply == expected, "ADD_IDENTITY")


def seed(executable, root, capture, log):
    F.validate_capture(capture)
    F.require(len(capture["rows"]) >= 65, "BUILD_MINIMUM")
    root.mkdir(mode=0o700)
    rows = sorted(deepcopy(capture["rows"]), key=F.address)
    path = root / "vectors.bin"
    path.write_bytes(b"".join(struct.pack("<8f", *r["row"]["embedding"]) for r in rows))
    worker = F.Worker(executable, "seed-lire", root / "native", log, path)
    try:
        state = worker.call("state")
        worker.close()
    finally:
        worker.stop()
    path.unlink()
    return F.seal(executable, root, capture, rows, log, lire=True), state


class Candidate:
    def __init__(self, executable, source, target, capture, log):
        self.worker = None
        self.target, self.capture = target, capture
        self.executable, self.log = executable, log
        F.validate_capture(capture)
        with (source.parent / "lock").open("rb") as lock:
            fcntl.flock(lock, fcntl.LOCK_SH)
            previous = F.clone(source, target)
        old, new = previous["capture"]["file_capture"], capture["file_capture"]
        for key in ("table_uuid", "schema_version", "field_id", "business_fields", "dimension"):
            F.require(old[key] == new[key], "CAPTURE_BINDING_CHANGED")
        F.require(new["snapshot_id"] > old["snapshot_id"], "STALE_SNAPSHOT")
        (target / "seal.json").unlink()
        F.sync(target)
        self.mapping = previous["mapping"]
        current = {F.address(r): r for r in capture["rows"]}
        before = {F.address(r): r for r in previous["capture"]["rows"]}
        F.require(all(current[k] == before[k] for k in set(current) & set(before)), "PHYSICAL_ROW_CHANGED")
        removed = [i for i, r in enumerate(self.mapping) if F.address(r) in before and F.address(r) not in current]
        added = sorted([r for key, r in current.items() if key not in before], key=F.address)
        F.require(len(self.mapping) + len(added) <= F.ROWS and len(removed) + len(added) <= 1000, "MUTATION_BUDGET")
        F.require(len(F.encoded({"added": added, "deleted": removed})) <= 4 * F.MIB, "MUTATION_BUDGET")
        self.worker = F.Worker(executable, "update-lire", target / "native", log)
        try:
            for vid in removed:
                F.require(self.worker.call("delete", vid) == {"deleted": True}, "DELETE_IDENTITY")
            if added:
                append(self.worker, added, len(self.mapping))
            self.mapping = self.mapping + added
        except BaseException:
            self.stop()
            raise

    def state(self):
        return self.worker.call("state")

    def drain(self, work, bytes_):
        return self.worker.call("drain", work, bytes_)

    def merge(self, head):
        return self.worker.call("merge", head)

    def seal(self):
        state = self.state()
        F.require(not state["queued"] and not state["pending_add"], "LIRE_CHECKPOINT_BUSY")
        F.require(state["vids"] == len(self.mapping), "ADD_IDENTITY")
        F.require(self.worker.call("checkpoint") == {"saved": True}, "CHECKPOINT_FAILED")
        self.worker.close()
        self.worker = None
        return F.seal(self.executable, self.target, self.capture, self.mapping, self.log, lire=True), state

    def stop(self):
        if self.worker is not None:
            self.worker.stop()
            self.worker = None
