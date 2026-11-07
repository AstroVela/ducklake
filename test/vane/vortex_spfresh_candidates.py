"""Local R7 candidate lifecycle experiment; not a production metadata provider.

One strict registry commits both publication and candidate state. Inherited POSIX
locks protect live native children; explicit recovery cancels orphan candidates.
"""

from contextlib import contextmanager
from copy import deepcopy
import fcntl
import importlib.util
import json
import os
from pathlib import Path
import re
import stat
import uuid

spec = importlib.util.spec_from_file_location(
    "registered_lire", Path(__file__).with_name("vortex_spfresh_lire_fixture.py")
)
L = importlib.util.module_from_spec(spec)
spec.loader.exec_module(L)
F = L.F
SLOTS = 16
MAX_REVISION = (1 << 64) - 1
IDENTITY = ("table_uuid", "schema_version", "field_id", "business_fields", "dimension")
PHASES = {"building", "sealed", "published", "retired", "cancelled", "deleting"}


def identity(capture):
    return {k: deepcopy(capture["file_capture"][k]) for k in IDENTITY}


def target(capture):
    snapshot = capture["file_capture"]["snapshot_id"]
    F.require(type(snapshot) is int and 1 <= snapshot <= MAX_REVISION, "SNAPSHOT_RANGE")
    return {"snapshot_id": snapshot, "capture_sha256": F.sha(F.encoded(capture))}


def uid(value):
    F.require(type(value) is str and re.fullmatch(r"[0-9a-f]{32}", value) is not None, "CANDIDATE_ID")
    return value


def regular(path):
    info = path.lstat()
    F.require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and info.st_uid == os.getuid(), "REGULAR_OWNED_FILE")
    return info


def inode(info):
    return {"dev": info.st_dev, "ino": info.st_ino}


class Registry:
    """Fresh layout only; state.json is the single atomic committed root."""

    def __init__(self, root):
        self.root = Path(root)
        info = self.root.lstat()
        F.require(
            stat.S_ISDIR(info.st_mode) and info.st_uid == os.getuid() and stat.S_IMODE(info.st_mode) == 0o700,
            "PRIVATE_REGISTRY",
        )

    @classmethod
    def create(cls, root, seed):
        root.mkdir(mode=0o700)
        for name in ("generations", "leases"):
            (root / name).mkdir(mode=0o700)
        for name in ("lock", "generations/lock"):
            (root / name).touch(mode=0o600)
        registry = cls(root)
        name = uuid.uuid4().hex
        body = F.clone(seed, registry.generation(name))
        capture = body["capture"]
        F.seal(registry.generation(name), capture, body["mapping"])
        F.sync(root / "generations")
        F.sync(root / "leases")
        seal = F.sha((registry.generation(name) / "seal.json").read_bytes())
        record = {
            "owner": None,
            "phase": "published",
            "base": None,
            "target": target(capture),
            "pin_source": False,
            "lease": None,
            "seal_sha256": seal,
            "reason": None,
            "gc": None,
        }
        state = {
            "format_version": 1,
            "identity": identity(capture),
            "head": {
                "generation": name,
                "revision": 1,
                "snapshot_id": record["target"]["snapshot_id"],
                "seal_sha256": seal,
            },
            "records": {name: record},
        }
        registry._write(state, "initialize", lambda _: None)
        return registry

    def generation(self, name):
        return self.root / "generations" / uid(name)

    def lease_path(self, name):
        return self.root / "leases" / uid(name)

    @contextmanager
    def lock(self, exclusive):
        regular(self.root / "lock")
        with (self.root / "lock").open("rb") as stream:
            fcntl.flock(stream, fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
            yield

    def _load(self):
        path = self.root / "state.json"
        F.require(regular(path).st_size <= F.MIB, "REGISTRY_BUDGET")
        packet = path.read_bytes()
        state = json.loads(packet)
        F.require(F.encoded(state) == packet, "REGISTRY_FORMAT")
        F.require(set(state) == {"format_version", "identity", "head", "records"}, "REGISTRY_FORMAT")
        F.require(state["format_version"] == 1 and set(state["identity"]) == set(IDENTITY), "REGISTRY_FORMAT")
        records, head = state["records"], state["head"]
        F.require(type(records) is dict and 1 <= len(records) <= SLOTS, "REGISTRY_CAPACITY")
        F.require(set(head) == {"generation", "revision", "snapshot_id", "seal_sha256"}, "REGISTRY_FORMAT")
        F.require(type(head["revision"]) is int and 1 <= head["revision"] <= MAX_REVISION, "REGISTRY_FORMAT")
        F.require(type(head["snapshot_id"]) is int and 1 <= head["snapshot_id"] <= MAX_REVISION, "REGISTRY_FORMAT")
        F.require(
            type(head["seal_sha256"]) is str and re.fullmatch(r"[0-9a-f]{64}", head["seal_sha256"]) is not None,
            "REGISTRY_FORMAT",
        )
        for name, record in records.items():
            uid(name)
            F.require(
                set(record)
                == {"owner", "phase", "base", "target", "pin_source", "lease", "seal_sha256", "reason", "gc"}
                and record["phase"] in PHASES
                and type(record["pin_source"]) is bool,
                "REGISTRY_FORMAT",
            )
            if record["owner"] is not None:
                uid(record["owner"])
            F.require(set(record["target"]) == {"snapshot_id", "capture_sha256"}, "REGISTRY_FORMAT")
            F.require(
                type(record["target"]["snapshot_id"]) is int and 1 <= record["target"]["snapshot_id"] <= MAX_REVISION,
                "REGISTRY_FORMAT",
            )
            F.require(
                type(record["target"]["capture_sha256"]) is str
                and re.fullmatch(r"[0-9a-f]{64}", record["target"]["capture_sha256"]) is not None,
                "REGISTRY_FORMAT",
            )
            if record["base"] is not None:
                base = record["base"]
                uid(base["generation"])
                F.require(
                    set(base) == {"generation", "revision", "snapshot_id", "seal_sha256", "capture_sha256"},
                    "REGISTRY_FORMAT",
                )
            if record["pin_source"]:
                F.require(record["base"] is not None and record["base"]["generation"] in records, "SOURCE_PIN")
            if record["phase"] in ("building", "sealed", "cancelled"):
                F.require(record["owner"] is not None and record["pin_source"], "SOURCE_PIN")
            if record["lease"] is not None:
                F.require(
                    set(record["lease"]) == {"dev", "ino"}
                    and all(type(v) is int and v >= 0 for v in record["lease"].values()),
                    "REGISTRY_FORMAT",
                )
            F.require((record["gc"] is not None) == (record["phase"] == "deleting"), "REGISTRY_FORMAT")
            if record["gc"] is not None:
                gc = record["gc"]
                F.require(
                    set(gc) == {"entries", "pending"}
                    and type(gc["entries"]) is list
                    and 1 <= len(gc["entries"]) <= 4099,
                    "GC_FORMAT",
                )
                F.require(
                    type(gc["pending"]) is list
                    and all(type(i) is int and 0 <= i < len(gc["entries"]) for i in gc["pending"])
                    and len(set(gc["pending"])) == len(gc["pending"]),
                    "GC_FORMAT",
                )
                seen = set()
                for entry in gc["entries"]:
                    F.require(type(entry) is dict and {"path", "kind", "size"} <= set(entry), "GC_FORMAT")
                    kind, path = entry["kind"], entry["path"]
                    F.require(kind in ("file", "dir", "lease", "record") and type(path) is str, "GC_FORMAT")
                    extra = {"dev", "ino"} if kind != "record" else set()
                    if kind == "file":
                        extra.add("sha256")
                    F.require(set(entry) == {"path", "kind", "size"} | extra, "GC_FORMAT")
                    F.require(
                        type(entry["size"]) is int
                        and 0 <= entry["size"] <= F.LIMIT
                        and (kind == "file" or entry["size"] == 0),
                        "GC_FORMAT",
                    )
                    F.require(not Path(path).is_absolute() and ".." not in Path(path).parts, "GC_FORMAT")
                    F.require(
                        (kind in ("lease", "record") and path == "") or (kind in ("file", "dir") and path != ""),
                        "GC_FORMAT",
                    )
                    F.require(
                        kind != "file" or (path != "." and re.fullmatch(r"[0-9a-f]{64}", entry["sha256"]) is not None),
                        "GC_FORMAT",
                    )
                    F.require((kind, path) not in seen, "GC_FORMAT")
                    seen.add((kind, path))
        F.require(uid(head["generation"]) in records, "REGISTRY_FORMAT")
        current = records[head["generation"]]
        F.require(
            current["phase"] == "published"
            and not current["pin_source"]
            and current["target"]["snapshot_id"] == head["snapshot_id"]
            and current["seal_sha256"] == head["seal_sha256"],
            "REGISTRY_HEAD",
        )
        F.require(sum(r["phase"] == "published" for r in records.values()) == 1, "REGISTRY_HEAD")
        return state

    def _write(self, state, operation, observe):
        packet = F.encoded(state)
        F.require(len(packet) <= F.MIB, "REGISTRY_BUDGET")
        pending = self.root / "state.pending"
        F.require(not pending.exists() and not pending.is_symlink(), "PENDING_METADATA")
        with pending.open("xb") as stream:
            stream.write(packet)
            stream.flush()
            os.fsync(stream.fileno())
        observe(operation + "-pre-rename")
        os.replace(pending, self.root / "state.json")
        F.sync(self.root)
        observe(operation + "-committed")

    def snapshot(self):
        with self.lock(False):
            return self._load()

    def head(self):
        return self.snapshot()["head"]

    def recover_metadata(self):
        with self.lock(True):
            self._load()
            pending = self.root / "state.pending"
            if not pending.exists() and not pending.is_symlink():
                return {"discarded_pending": False}
            F.require(regular(pending).st_size <= F.MIB, "REGISTRY_BUDGET")
            pending.unlink()
            F.sync(self.root)
            return {"discarded_pending": True}

    @staticmethod
    def record(state, handle):
        F.require(set(handle) == {"id", "owner"}, "CANDIDATE_HANDLE")
        uid(handle["id"])
        uid(handle["owner"])
        F.require(handle["id"] in state["records"], "NO_CANDIDATE")
        record = state["records"][handle["id"]]
        F.require(record["owner"] == handle["owner"], "CANDIDATE_OWNER")
        return record

    def _current_source(self, state):
        head = state["head"]
        path = self.generation(head["generation"])
        body = F.verify(path)
        F.require(F.sha((path / "seal.json").read_bytes()) == head["seal_sha256"], "SOURCE_CHANGED")
        F.require(identity(body["capture"]) == state["identity"], "CAPTURE_BINDING_CHANGED")
        F.require(target(body["capture"]) == state["records"][head["generation"]]["target"], "SOURCE_CHANGED")
        return body

    def register(self, capture, expected_revision, observe=lambda _: None):
        F.validate_capture(capture)
        F.require(type(expected_revision) is int and 1 <= expected_revision <= MAX_REVISION, "PUBLICATION_CONFLICT")
        F.require(len(F.encoded(capture)) <= F.MIB, "MANIFEST_BUDGET")
        captured_target = target(capture)
        with self.lock(True):
            state = self._load()
            F.require(len(state["records"]) < SLOTS, "REGISTRY_CAPACITY")
            F.require(state["head"]["revision"] == expected_revision, "PUBLICATION_CONFLICT")
            F.require(identity(capture) == state["identity"], "CAPTURE_BINDING_CHANGED")
            F.require(capture["file_capture"]["snapshot_id"] > state["head"]["snapshot_id"], "STALE_SNAPSHOT")
            source = self._current_source(state)
            name, owner = uuid.uuid4().hex, uuid.uuid4().hex
            record = {
                "owner": owner,
                "phase": "building",
                "base": dict(state["head"], capture_sha256=target(source["capture"])["capture_sha256"]),
                "target": captured_target,
                "pin_source": True,
                "lease": None,
                "seal_sha256": None,
                "reason": None,
                "gc": None,
            }
            state["records"][name] = record
            self._write(state, "register", observe)
            lease = self.lease_path(name).open("xb+")
            try:
                fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
                os.fsync(lease.fileno())
                F.sync(self.root / "leases")
                record["lease"] = inode(os.fstat(lease.fileno()))
                self._write(state, "lease", observe)
                return {"id": name, "owner": owner}, lease
            except BaseException:
                lease.close()
                raise

    @contextmanager
    def guard(self, handle, lease, phase="building"):
        with self.lock(False):
            state = self._load()
            record = self.record(state, handle)
            F.require(record["phase"] == phase, "CANDIDATE_STATE")
            self.check_base(state, record)
            F.require(
                record["lease"] == inode(os.fstat(lease.fileno())) == inode(regular(self.lease_path(handle["id"]))),
                "CANDIDATE_LEASE",
            )
            yield record

    @staticmethod
    def check_base(state, record):
        F.require(state["head"] == {k: record["base"][k] for k in state["head"]}, "PUBLICATION_CONFLICT")
        source = state["records"][record["base"]["generation"]]
        F.require(source["target"]["capture_sha256"] == record["base"]["capture_sha256"], "SOURCE_CHANGED")

    def sealed(self, handle, capture, observe):
        with self.lock(True):
            state = self._load()
            record = self.record(state, handle)
            F.require(record["phase"] == "building", "CANDIDATE_STATE")
            self.check_base(state, record)
            body = F.verify(self.generation(handle["id"]))
            F.require(
                body["capture"] == capture
                and target(capture) == record["target"]
                and identity(capture) == state["identity"],
                "CANDIDATE_CAPTURE",
            )
            record.update(phase="sealed", seal_sha256=F.sha((self.generation(handle["id"]) / "seal.json").read_bytes()))
            self._write(state, "sealed", observe)

    def publish(self, handle, observe=lambda _: None):
        with self.lock(True):
            state = self._load()
            record = self.record(state, handle)
            F.require(record["phase"] == "sealed", "CANDIDATE_STATE")
            self.check_base(state, record)
            F.require(state["head"]["revision"] < MAX_REVISION, "REVISION_EXHAUSTED")
            body = F.verify(self.generation(handle["id"]))
            F.require(
                identity(body["capture"]) == state["identity"] and target(body["capture"]) == record["target"],
                "CANDIDATE_CAPTURE",
            )
            F.require(
                F.sha((self.generation(handle["id"]) / "seal.json").read_bytes()) == record["seal_sha256"],
                "CANDIDATE_SEAL",
            )
            old = state["head"]
            state["records"][old["generation"]]["phase"] = "retired"
            record.update(phase="published", pin_source=False)
            state["head"] = {
                "generation": handle["id"],
                "revision": old["revision"] + 1,
                "snapshot_id": record["target"]["snapshot_id"],
                "seal_sha256": record["seal_sha256"],
            }
            self._write(state, "publish", observe)
            return deepcopy(state["head"])

    def cancel(self, handle, observe=lambda _: None):
        with self.lock(True):
            state = self._load()
            record = self.record(state, handle)
            if record["phase"] == "cancelled":
                return {"phase": "cancelled"}
            F.require(record["phase"] in ("building", "sealed"), "CANDIDATE_STATE")
            record.update(phase="cancelled", reason="explicit")
            self._write(state, "cancel", observe)
            return {"phase": "cancelled"}

    @contextmanager
    def try_lease(self, name, record):
        path = self.lease_path(name)
        if not path.exists() and not path.is_symlink():
            missing_planned = record["phase"] == "deleting" and (
                not any(e["kind"] == "lease" for e in record["gc"]["entries"])
                or any(
                    e["kind"] == "lease" and i in record["gc"]["pending"] for i, e in enumerate(record["gc"]["entries"])
                )
            )
            F.require(record["lease"] is None or missing_planned, "CANDIDATE_LEASE_MISSING")
            yield True
            return
        info = regular(path)
        F.require(info.st_size == 0 and (record["lease"] is None or inode(info) == record["lease"]), "CANDIDATE_LEASE")
        with path.open("rb+") as stream:
            try:
                fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                yield False
                return
            yield True

    def recover(self, name, observe=lambda _: None):
        with self.lock(True):
            state = self._load()
            F.require(uid(name) in state["records"], "NO_CANDIDATE")
            record = state["records"][name]
            if record["phase"] not in ("building", "sealed", "cancelled"):
                return {"phase": record["phase"], "busy": False}
            with self.try_lease(name, record) as acquired:
                if not acquired:
                    return {"phase": record["phase"], "busy": True}
                if record["phase"] == "cancelled":
                    return {"phase": "cancelled", "busy": False}
                record.update(phase="cancelled", reason="orphan")
                self._write(state, "recover", observe)
                return {"phase": "cancelled", "busy": False}

    def read(self, probe, capture, private, log):
        with self.lock(False):
            state = self._load()
            F.require(
                identity(capture) == state["identity"]
                and target(capture) == state["records"][state["head"]["generation"]]["target"],
                "INDEX_WATERMARK_MISMATCH",
            )
            self._current_source(state)
            body = F.clone(self.generation(state["head"]["generation"]), private)
            F.require(body["capture"] == capture, "READVIEW_CHANGED")
            return L.View(probe, private, body, log)

    def _scan(self, name):
        root = self.generation(name)
        files, directories = [], []
        if root.exists() or root.is_symlink():
            F.require(stat.S_ISDIR(root.lstat().st_mode), "REGULAR_OWNED_FILE")
            for path in [root, *sorted(root.rglob("*"))]:
                info = path.lstat()
                F.require(info.st_uid == os.getuid(), "REGULAR_OWNED_FILE")
                key = str(path.relative_to(root))
                if stat.S_ISDIR(info.st_mode):
                    directories.append({"path": key, "kind": "dir", "size": 0, **inode(info)})
                else:
                    regular(path)
                    F.require(info.st_size <= F.LIMIT, "GC_FILE_BUDGET")
                    files.append(
                        {
                            "path": key,
                            "kind": "file",
                            "size": info.st_size,
                            "sha256": F.sha(path.read_bytes()),
                            **inode(info),
                        }
                    )
                F.require(len(files) + len(directories) <= 4096, "GC_INVENTORY_BUDGET")
        result = files + sorted(directories, key=lambda e: (-len(Path(e["path"]).parts), e["path"]))
        lease = self.lease_path(name)
        if lease.exists() or lease.is_symlink():
            info = regular(lease)
            F.require(info.st_size == 0, "CANDIDATE_LEASE")
            result.append({"path": "", "kind": "lease", "size": 0, **inode(info)})
        result.append({"path": "", "kind": "record", "size": 0})
        F.require(len(F.encoded(result)) <= F.MIB, "GC_INVENTORY_BUDGET")
        return result

    def collect(self, name, objects, bytes_, observe=lambda _: None):
        F.require(
            type(objects) is int and 1 <= objects <= 16 and type(bytes_) is int and 1 <= bytes_ <= F.LIMIT, "GC_BUDGET"
        )
        with self.lock(True):
            state = self._load()
            F.require(uid(name) in state["records"], "NO_CANDIDATE")
            record = state["records"][name]
            F.require(state["head"]["generation"] != name, "REFERENCED")
            F.require(record["phase"] in ("cancelled", "retired", "deleting"), "CANDIDATE_STATE")
            if any(r["pin_source"] and r["base"]["generation"] == name for r in state["records"].values()):
                return {"objects": 0, "bytes": 0, "blocked": "source-pin"}
            with self.try_lease(name, record) as acquired:
                if not acquired:
                    return {"objects": 0, "bytes": 0, "blocked": "live-owner"}
                if record["phase"] != "deleting":
                    record.update(phase="deleting", gc={"entries": self._scan(name), "pending": []})
                gc = record["gc"]
                expected = {(e["kind"], e["path"]): e for e in gc["entries"]}
                actual = {(e["kind"], e["path"]): e for e in self._scan(name)}
                pending = {(gc["entries"][i]["kind"], gc["entries"][i]["path"]) for i in gc["pending"]}
                F.require(
                    set(actual) <= set(expected) and set(expected) - set(actual) <= pending, "GC_INVENTORY_CHANGED"
                )
                F.require(all(value == expected[key] for key, value in actual.items()), "GC_INVENTORY_CHANGED")
                indexes = gc["pending"] or list(range(len(gc["entries"])))
                planned, charge = [], 0
                for i in indexes:
                    size = gc["entries"][i]["size"]
                    if len(planned) == objects or charge + size > bytes_:
                        break
                    planned.append(i)
                    charge += size
                if not planned:
                    return {"objects": 0, "bytes": 0, "blocked": "byte-budget"}
                if not gc["pending"]:
                    gc["pending"] = planned
                    self._write(state, "gc-plan", observe)
                for i in planned:
                    entry = gc["entries"][i]
                    if entry["kind"] == "record":
                        continue
                    path = self.lease_path(name) if entry["kind"] == "lease" else self.generation(name) / entry["path"]
                    if path.exists():
                        if entry["kind"] == "dir":
                            path.rmdir()
                        else:
                            path.unlink()
                        F.sync(path.parent)
                    observe("gc-unlinked")
                remaining = [e for i, e in enumerate(gc["entries"]) if i not in planned]
                old_pending = [gc["entries"][i] for i in gc["pending"] if i not in planned]
                if remaining:
                    record["gc"] = {"entries": remaining, "pending": [remaining.index(e) for e in old_pending]}
                else:
                    del state["records"][name]
                self._write(state, "gc-progress", observe)
                return {"objects": len(planned), "bytes": charge, "blocked": None}


class Session:
    def __init__(self, registry, probe, capture, expected_revision, log, observe=lambda _: None):
        self.registry, self.observe = registry, observe
        self.capture = deepcopy(capture)
        self.candidate = self.lease = None
        self.handle, self.lease = registry.register(capture, expected_revision, observe)
        try:
            observe("registered")
            with registry.guard(self.handle, self.lease) as record:
                self.candidate = L.Candidate(
                    probe,
                    registry.generation(record["base"]["generation"]),
                    registry.generation(self.handle["id"]),
                    capture,
                    log,
                    pass_fds=(self.lease.fileno(),),
                    observe=observe,
                )
        except BaseException:
            self.stop()
            raise

    def call(self, operation, *values):
        F.require(operation in ("state", "drain", "merge", "pause"), "CANDIDATE_OPERATION")
        with self.registry.guard(self.handle, self.lease):
            F.require(self.candidate is not None and self.candidate.worker is not None, "CANDIDATE_CLOSED")
            return self.candidate.worker.call(operation, *values)

    def seal(self):
        with self.registry.guard(self.handle, self.lease):
            body, state = self.candidate.seal()
            self.observe("sealed-files")
        self.registry.sealed(self.handle, self.capture, self.observe)
        return body, state

    def publish(self):
        result = self.registry.publish(self.handle, self.observe)
        self.stop()
        return result

    def cancel(self):
        result = self.registry.cancel(self.handle, self.observe)
        self.stop()
        return result

    def stop(self):
        if self.candidate is not None:
            self.candidate.stop()
            self.candidate = None
        if self.lease is not None:
            self.lease.close()
            self.lease = None
