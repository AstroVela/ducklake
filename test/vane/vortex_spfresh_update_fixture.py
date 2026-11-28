"""Explicit local R7 experiment; never imported by the SQL/provider runtime.

One mutable candidate, closed checkpoints, immutable published versions and private
read copies. No implicit retry, adoption, format conversion or query fallback.
"""

from copy import deepcopy
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import queue
import re
import shutil
import struct
import subprocess
import threading

MIB = 1024 * 1024
LIMIT = 64 * MIB
ROWS = 100000


def require(value, message):
    if not value:
        raise ValueError(message)


def encoded(value):
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def sha(value):
    return hashlib.sha256(value).hexdigest()


def sync(directory):
    fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def write(path, value):
    data = encoded(value)
    require(len(data) <= MIB, "MANIFEST_BUDGET")
    with path.with_suffix(".pending").open("xb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(path.with_suffix(".pending"), path)
    sync(path.parent)


def address(row):
    return encoded(row["row_ref"])


def validate_capture(capture):
    require(set(capture) == {"file_capture", "rows"}, "CAPTURE_FORMAT")
    file = capture["file_capture"]
    require(file["format_version"] == 1 and file["dimension"] == 8, "VECTOR_SPEC")
    require(len(capture["rows"]) <= ROWS and len(capture["rows"]) * 8 * 4 <= LIMIT, "ROW_VECTOR_BUDGET")
    files = {item["id"]: item for item in file["files"]}
    require(len(files) == len(file["files"]), "SOURCE_INVENTORY")
    require(sum(item["row_count"] for item in files.values()) <= ROWS, "PHYSICAL_ROW_BUDGET")
    visible = {(r["file_id"], r["row_offset"]) for r in file["visible"]}
    require(len(visible) == len(file["visible"]), "VISIBLE_ROWS")
    found = set()
    for row in capture["rows"]:
        ref = row["row_ref"]
        require(ref["kind"] == "file", "UNFLUSHED_ROWS")
        require(set(ref) == {"kind", "file_id", "row_offset"}, "ROW_ADDRESS")
        require(ref["file_id"] in files and 0 <= ref["row_offset"] < files[ref["file_id"]]["row_count"], "ROW_ADDRESS")
        require(address(row) not in found, "ROW_ADDRESS")
        found.add(address(row))
        require(
            len(row["row"]["embedding"]) == 8 and all(math.isfinite(v) for v in row["row"]["embedding"]), "VECTOR_SPEC"
        )
    require(visible == {(r["row_ref"]["file_id"], r["row_ref"]["row_offset"]) for r in capture["rows"]}, "VISIBLE_ROWS")


def inventory(root):
    result, total = {}, 0
    for path in sorted(root.rglob("*")):
        require(not path.is_symlink(), "SYMLINK_INPUT")
        if path.is_dir():
            continue
        require(path.is_file(), "NONREGULAR_INPUT")
        size = path.stat().st_size
        require(size <= LIMIT - total, "ARTIFACT_BUDGET")
        total += size
        result[str(path.relative_to(root))] = {"size": size, "sha256": sha(path.read_bytes())}
    return result


def seal(root, capture, mapping):
    validate_capture(capture)
    require(len(mapping) <= ROWS and len({address(r) for r in mapping}) == len(mapping), "VID_MAP")
    live = {address(r): r for r in capture["rows"]}
    require(set(live) <= {address(r) for r in mapping}, "VID_MAP")
    require(all(row == live[address(row)] for row in mapping if address(row) in live), "VID_MAP")
    body = {"format_version": 1, "capture": capture, "mapping": mapping, "files": inventory(root / "native")}
    # Persist every declared native file before the single publication pointer.
    for name in body["files"]:
        with (root / "native" / name).open("rb") as stream:
            os.fsync(stream.fileno())
    for path in sorted((root / "native").rglob("*"), reverse=True):
        if path.is_dir():
            sync(path)
    sync(root / "native")
    write(root / "seal.json", body)
    return body


def verify(root):
    require(not root.is_symlink() and root.is_dir(), "GENERATION_REQUIRED")
    require({p.name for p in root.iterdir()} == {"native", "seal.json"}, "GENERATION_INVENTORY")
    require(not (root / "native").is_symlink() and (root / "native").is_dir(), "SYMLINK_INPUT")
    require(not (root / "seal.json").is_symlink(), "SYMLINK_INPUT")
    packet = (root / "seal.json").read_bytes()
    require(len(packet) <= MIB, "MANIFEST_BUDGET")
    body = json.loads(packet)
    require(
        set(body) == {"format_version", "capture", "mapping", "files"} and body["format_version"] == 1,
        "GENERATION_FORMAT",
    )
    require(encoded(body) == packet, "GENERATION_FORMAT")
    validate_capture(body["capture"])
    require(body["files"] == inventory(root / "native"), "GENERATION_CHANGED")
    live = {address(r): r for r in body["capture"]["rows"]}
    require(
        len(body["mapping"]) <= ROWS and len({address(r) for r in body["mapping"]}) == len(body["mapping"]), "VID_MAP"
    )
    require(set(live) <= {address(r) for r in body["mapping"]}, "VID_MAP")
    require(all(row == live[address(row)] for row in body["mapping"] if address(row) in live), "VID_MAP")
    return body


class Worker:
    def __init__(self, executable, mode, root, log, vectors=None):
        args = [str(executable), mode, str(root)] + ([] if vectors is None else [str(vectors)])
        self.events, self.log = queue.Queue(), log.open("ab")
        self.process = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=self.log, text=True)

        def read():
            for line in self.process.stdout:
                if line.startswith("R7 "):
                    self.events.put(json.loads(line[3:]))
                else:
                    self.log.write(line.encode())
            self.events.put(None)

        self.reader = threading.Thread(target=read, daemon=True)
        self.reader.start()
        try:
            self.ready = self.receive()
            require(self.ready.get("ready") is True, "WORKER_NOT_READY")
            require(os.sched_getaffinity(self.process.pid) == os.sched_getaffinity(0), "NATIVE_CPU_PROFILE")
        except BaseException:
            self.stop()
            raise

    def receive(self):
        event = self.events.get(timeout=90)
        require(event is not None, "NATIVE_OPERATION_FAILED")
        return event

    def call(self, operation, *values):
        self.process.stdin.write(" ".join(map(str, (operation, *values))) + "\n")
        self.process.stdin.flush()
        return self.receive()

    def close(self):
        require(self.call("close") == {"closed": True}, "NATIVE_CLOSE_FAILED")
        require(self.process.wait(timeout=30) == 0, "NATIVE_CLOSE_FAILED")
        self.reader.join(timeout=5)
        require(not self.reader.is_alive(), "NATIVE_PIPE_STILL_OPEN")
        self.log.close()

    def stop(self):
        if self.process.poll() is None:
            self.process.kill()
        self.process.wait(timeout=30)
        self.reader.join(timeout=5)
        self.log.close()


def seed(executable, root, capture, log):
    validate_capture(capture)
    require(len(capture["rows"]) >= 65, "BUILD_MINIMUM")
    root.mkdir(mode=0o700)
    rows = sorted(deepcopy(capture["rows"]), key=address)
    path = root / "vectors.bin"
    path.write_bytes(b"".join(struct.pack("<8f", *r["row"]["embedding"]) for r in rows))
    worker = Worker(executable, "seed", root / "native", log, path)
    try:
        worker.close()
    finally:
        worker.stop()
    path.unlink()
    return seal(root, capture, rows)


def clone(source, target):
    body = verify(source)
    require(not target.exists(), "CANDIDATE_EXISTS")
    shutil.copytree(source, target)
    require(verify(target) == body, "COPY_CHANGED")
    return body


def advance(executable, source, target, capture, log, observe=lambda _: None):
    validate_capture(capture)
    with (source.parent / "lock").open("rb") as lock:
        fcntl.flock(lock, fcntl.LOCK_SH)
        previous = clone(source, target)
    old, new = previous["capture"]["file_capture"], capture["file_capture"]
    for key in ("table_uuid", "schema_version", "field_id", "business_fields", "dimension"):
        require(old[key] == new[key], "CAPTURE_BINDING_CHANGED")
    require(new["snapshot_id"] > old["snapshot_id"], "STALE_SNAPSHOT")
    # Index is allowed to lag; this native-only experiment never mutates a
    # published version and never promotes an unsealed candidate.
    (target / "seal.json").unlink()
    sync(target)
    current = {address(r): r for r in capture["rows"]}
    before = {address(r): r for r in previous["capture"]["rows"]}
    mapping = previous["mapping"]
    removed = [i for i, row in enumerate(mapping) if address(row) in before and address(row) not in current]
    added = sorted([row for key, row in current.items() if key not in before], key=address)
    require(all(current[k] == before[k] for k in set(current) & set(before)), "PHYSICAL_ROW_CHANGED")
    require(len(mapping) + len(added) <= ROWS and len(added) + len(removed) <= 1000, "MUTATION_BUDGET")
    require(len(encoded({"added": added, "deleted": removed})) <= 4 * MIB, "MUTATION_BUDGET")
    worker = Worker(executable, "update", target / "native", log)
    try:
        for vid in removed:
            require(worker.call("delete", vid) == {"deleted": True}, "DELETE_IDENTITY")
        if added:
            actual = worker.call("add", len(added), *[v for row in added for v in row["row"]["embedding"]])
            require(actual == {"ids": list(range(len(mapping), len(mapping) + len(added)))}, "ADD_IDENTITY")
        observe("after_mutation")
        require(worker.call("checkpoint") == {"saved": True}, "CHECKPOINT_FAILED")
        observe("after_checkpoint")
        worker.close()
    finally:
        worker.stop()
    body = seal(target, capture, mapping + added)
    observe("after_seal")
    return body


class Catalog:
    """Test protocol for sealed native checkpoints; no SQL index registration."""

    def __init__(self, root):
        self.root = root
        if not root.exists():
            root.mkdir(mode=0o700)
            (root / "lock").touch(mode=0o600)

    def head(self):
        require((self.root / "head.json").is_file(), "NO_PUBLISHED_INDEX")
        head = json.loads((self.root / "head.json").read_bytes())
        require(
            set(head) == {"format_version", "revision", "generation", "seal_sha256", "table_uuid", "snapshot_id"}
            and head["format_version"] == 1,
            "HEAD_FORMAT",
        )
        require(re.fullmatch(r"[a-z0-9-]+", head["generation"]) is not None, "HEAD_FORMAT")
        root = self.root / head["generation"]
        require(sha((root / "seal.json").read_bytes()) == head["seal_sha256"], "HEAD_CHANGED")
        body = verify(root)
        require(
            all(body["capture"]["file_capture"][key] == head[key] for key in ("table_uuid", "snapshot_id")),
            "HEAD_CHANGED",
        )
        return head

    def publish(self, name, expected_revision, observe=lambda _: None):
        require(re.fullmatch(r"[a-z0-9-]+", name) is not None, "GENERATION_NAME")
        body = verify(self.root / name)
        with (self.root / "lock").open("rb") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            old = self.head() if (self.root / "head.json").exists() else None
            require((0 if old is None else old["revision"]) == expected_revision, "PUBLICATION_CONFLICT")
            capture = body["capture"]["file_capture"]
            require(old is None or old["table_uuid"] == capture["table_uuid"], "TABLE_UUID_CHANGED")
            require(old is None or old["snapshot_id"] < capture["snapshot_id"], "STALE_SNAPSHOT")
            head = {
                "format_version": 1,
                "revision": expected_revision + 1,
                "generation": name,
                "seal_sha256": sha((self.root / name / "seal.json").read_bytes()),
                "table_uuid": capture["table_uuid"],
                "snapshot_id": capture["snapshot_id"],
            }
            observe("before_publish")
            write(self.root / "head.json", head)
            observe("after_publish")
            return head

    def read(self, executable, private, capture, log):
        with (self.root / "lock").open("rb") as lock:
            fcntl.flock(lock, fcntl.LOCK_SH)
            head = self.head()
            require(
                all(head[key] == capture["file_capture"][key] for key in ("table_uuid", "snapshot_id")),
                "INDEX_WATERMARK_MISMATCH",
            )
            body = clone(self.root / head["generation"], private)
            require(body["capture"] == capture, "READVIEW_CHANGED")
            return ReadView(executable, private, body, log)

    def collect(self, name, objects, bytes_):
        require(1 <= objects <= 16 and 1 <= bytes_ <= LIMIT, "GC_BUDGET")
        require(re.fullmatch(r"[a-z0-9-]+", name) is not None, "GENERATION_NAME")
        with (self.root / "lock").open("rb") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            require(self.head()["generation"] != name, "REFERENCED")
            body = verify(self.root / name)
            charge = (
                sum(item["size"] for item in body["files"].values()) + (self.root / name / "seal.json").stat().st_size
            )
            require(charge <= bytes_, "GC_BUDGET")
            shutil.rmtree(self.root / name)
            sync(self.root)
            return {"objects": 1, "bytes": charge}


class ReadView:
    def __init__(self, executable, private, body, log):
        self.root, self.body = private, body
        self.worker = Worker(executable, "open", private / "native", log)
        try:
            require(self.worker.ready["live"] == len(body["capture"]["rows"]), "NATIVE_ROW_COUNT")
        except BaseException:
            self.worker.stop()
            raise

    def query(self, vector, k):
        require(type(k) is int and 1 <= k <= 64, "QUERY_K")
        require(len(vector) == 8 and all(math.isfinite(v) for v in vector), "QUERY_VECTOR")
        try:
            vector = [struct.unpack("!f", struct.pack("!f", v))[0] for v in vector]
        except (OverflowError, struct.error):
            require(False, "QUERY_VECTOR")
        require(all(math.isfinite(v) for v in vector), "QUERY_VECTOR")
        values = self.worker.call("search", k, *vector)["hits"]
        current = {address(r) for r in self.body["capture"]["rows"]}
        hits = []
        for vid, distance in values:
            require(type(vid) is int and 0 <= vid < len(self.body["mapping"]), "NATIVE_VID")
            row = self.body["mapping"][vid]
            require(address(row) in current, "INVISIBLE_NATIVE_HIT")
            hits.append(
                {"vid": vid, "row_ref": deepcopy(row["row_ref"]), "row": deepcopy(row["row"]), "distance": distance}
            )
        return hits

    def close(self):
        self.worker.close()
        require(verify(self.root) == self.body, "READ_ONLY_VIEW_CHANGED")

    def stop(self):
        self.worker.stop()
