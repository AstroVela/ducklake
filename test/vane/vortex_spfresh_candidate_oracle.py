"""Independent event model and filesystem observations for R7 candidate tests.

This module imports neither the registry nor its file helpers. It compares the
committed root with externally declared events, not with returned state labels.
"""

from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import stat


def require(value, message):
    if not value:
        raise AssertionError(message)


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode()


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def committed(root):
    return json.loads((root / "state.json").read_bytes())


def files(root):
    result = {}
    for path in sorted(root.rglob("*")):
        info = path.lstat()
        require(not stat.S_ISLNK(info.st_mode), "Oracle saw a symlink")
        if stat.S_ISDIR(info.st_mode):
            result[str(path.relative_to(root))] = {"kind": "dir", "size": 0}
        else:
            require(stat.S_ISREG(info.st_mode), "Oracle saw a non-file")
            result[str(path.relative_to(root))] = {"kind": "file", "size": info.st_size, "sha256": sha(path)}
    return result


class Model:
    def __init__(self, initial, capture):
        self.head, self.revision = initial, 1
        self.seen = {initial}
        self.records = {initial: self.expected("published", None, capture)}

    @staticmethod
    def expected(phase, source, capture):
        return {
            "phase": phase,
            "source": source,
            "snapshot": capture["file_capture"]["snapshot_id"],
            "capture_sha256": hashlib.sha256(encoded(capture)).hexdigest(),
        }

    def registered(self, name, capture):
        require(name not in self.seen, "Model candidate UUID was reused")
        self.seen.add(name)
        self.records[name] = self.expected("building", self.head, capture)

    def sealed(self, name):
        require(self.records[name]["phase"] == "building", "Invalid model seal")
        self.records[name]["phase"] = "sealed"

    def cancel(self, name):
        require(self.records[name]["phase"] in ("building", "sealed", "cancelled"), "Invalid model cancellation")
        self.records[name]["phase"] = "cancelled"

    def publish(self, name):
        require(self.records[name]["phase"] == "sealed", "Model published an unsealed candidate")
        self.records[self.head]["phase"] = "retired"
        self.records[name].update(phase="published", source=None)
        self.head, self.revision = name, self.revision + 1

    def collected(self, name, finished):
        if finished:
            require(name != self.head, "Model GC removed the head")
            del self.records[name]
        else:
            self.records[name]["phase"] = "deleting"

    def inspect(self, root, value=None):
        actual = committed(root) if value is None else deepcopy(value)
        require(
            actual["head"]["generation"] == self.head and actual["head"]["revision"] == self.revision,
            "Committed head differs from event model",
        )
        require(set(actual["records"]) == set(self.records), "Registered UUID inventory differs from event model")
        for name, expected in self.records.items():
            record = actual["records"][name]
            require(record["phase"] == expected["phase"], "Candidate phase differs from event model")
            require(
                record["target"] == {"snapshot_id": expected["snapshot"], "capture_sha256": expected["capture_sha256"]},
                "Candidate ReadView binding differs from event model",
            )
            require(
                record["pin_source"] == (expected["source"] is not None),
                "Candidate source ownership differs from event model",
            )
            if expected["source"] is not None:
                require(record["base"]["generation"] == expected["source"], "Wrong pinned source generation")
                require(expected["source"] in actual["records"], "A referenced source was reclaimed")
            if record["phase"] in ("published", "retired", "sealed"):
                native = root / "generations" / name
                seal = json.loads((native / "seal.json").read_bytes())
                require(record["seal_sha256"] == sha(native / "seal.json"), "Committed seal hash differs")
                require(
                    hashlib.sha256(encoded(seal["capture"])).hexdigest() == expected["capture_sha256"],
                    "Sealed capture differs from event model",
                )
                physical = files(native / "native")
                require(
                    {k: {"size": v["size"], "sha256": v["sha256"]} for k, v in physical.items() if v["kind"] == "file"}
                    == seal["files"],
                    "Sealed native files changed",
                )
        return {
            "head": self.head,
            "revision": self.revision,
            "phases": {k: v["phase"] for k, v in self.records.items()},
        }


def gc_observation(before, after, result, objects, bytes_):
    require(
        0 <= result["objects"] <= objects <= 16 and 0 <= result["bytes"] <= bytes_ <= 64 * 1024 * 1024,
        "GC returned an expanded budget",
    )
    protected_before = {k: v for k, v in before.items() if k not in ("state.json", "state.pending")}
    protected_after = {k: v for k, v in after.items() if k not in ("state.json", "state.pending")}
    require(set(protected_after) <= set(protected_before), "GC created a data file")
    require(all(v == protected_before[k] for k, v in protected_after.items()), "GC modified retained bytes")
    removed = set(protected_before) - set(protected_after)
    removed_bytes = sum(protected_before[k]["size"] for k in removed)
    require(len(removed) <= result["objects"] and removed_bytes <= result["bytes"], "GC undercharged actual deletion")
    return {"removed": sorted(removed), "physical_bytes": removed_bytes, **result}
