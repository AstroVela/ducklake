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


class View(F.ReadView):
    def __init__(self, executable, private, body, log):
        self.root, self.body = private, body
        self.worker = F.Worker(executable, "open-lire", private / "native", log)
        try:
            F.require(self.worker.ready["live"] == len(body["capture"]["rows"]), "NATIVE_ROW_COUNT")
        except BaseException:
            self.worker.stop()
            raise


def read(catalog, executable, private, capture, log):
    with (catalog.root / "lock").open("rb") as lock:
        fcntl.flock(lock, fcntl.LOCK_SH)
        head = catalog.head()
        F.require(
            all(head[k] == capture["file_capture"][k] for k in ("table_uuid", "snapshot_id")),
            "INDEX_WATERMARK_MISMATCH",
        )
        body = F.clone(catalog.root / head["generation"], private)
        F.require(body["capture"] == capture, "READVIEW_CHANGED")
        return View(executable, private, body, log)


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
    return F.seal(root, capture, rows), state


class Candidate:
    def __init__(self, executable, source, target, capture, log):
        self.worker = None
        self.target, self.capture = target, capture
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
                actual = self.worker.call("add", len(added), *[v for r in added for v in r["row"]["embedding"]])
                F.require(
                    actual == {"ids": list(range(len(self.mapping), len(self.mapping) + len(added)))}, "ADD_IDENTITY"
                )
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
        F.require(state["queued"] == 0, "LIRE_CHECKPOINT_BUSY")
        F.require(self.worker.call("checkpoint") == {"saved": True}, "CHECKPOINT_FAILED")
        self.worker.close()
        self.worker = None
        return F.seal(self.target, self.capture, self.mapping), state

    def stop(self):
        if self.worker is not None:
            self.worker.stop()
            self.worker = None
