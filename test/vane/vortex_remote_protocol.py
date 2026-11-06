"""Executable R6 protocol model; no runtime adapter, credentials or remote I/O."""

from __future__ import annotations

from copy import deepcopy
import hashlib
import json

MIB = 1024 * 1024
CATALOG = "00000000-0000-4000-8000-000000000001"
TABLE = "00000000-0000-4000-8000-000000000002"
LIMITS = {
    "source_bytes": 64 * MIB,
    "artifact_bytes": 64 * MIB,
    "manifest_bytes": MIB,
    "rows": 100000,
    "vector_bytes": 64 * MIB,
    "segments": 3,
    "gc_objects": 16,
    "gc_bytes": 64 * MIB,
}


def compact(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def digest(value):
    return hashlib.sha256(value.encode()).hexdigest()


class Rejected(Exception):
    pass


def need(condition, code):
    if not condition:
        raise Rejected(code)


class Protocol:
    """Metadata transactions, a versioned object service, and independently delayed RPCs.

    Payload sizes are symbolic envelope sizes. Digests identify fixture content;
    this model does not implement Vortex encoding, PostgreSQL or an S3 SDK.
    """

    def __init__(self, mutant="", source_b_bytes=1024):
        self.mutant = ""
        self.m = {
            "owner": 1,
            "revision": 0,
            "head": None,
            "refs": [],
            "requests": {},
            "units": {},
            "objects": {},
            "native": ["source-a", "source-b"],
            "views": {},
            "commits": [],
            "gc": {},
            "completed": {},
        }
        self.store, self.wire, self.sequence = {}, {}, {}
        self.local = {"frames": {}, "rounds": {}, "copies": {}}
        for name in ("source-a", "source-b"):
            size = source_b_bytes if name == "source-b" else 1024
            self._unit(name, "source", size, [name])
            self._object(name, name, "source", size, digest(name))
            version = self._put(name, "payload", digest(name), size)
            self.m["objects"][name].update(version=version, sealed=True)
            self.m["units"][name]["state"] = "ready"
        self.store["foreign/unregistered"] = [
            {
                "id": "foreign-version",
                "etag": "foreign-etag",
                "kind": "payload",
                "sha": digest("foreign"),
                "size": 19,
                "marker": None,
            }
        ]
        self.prepare("seed", 1, 0, "source-a")
        self.upload("seed", "index", 1)
        self.deliver("put:seed:index:1")
        self.seal("seed", "index", 1)
        self.manifest("seed", 1)
        self.upload("seed", "manifest", 1)
        self.deliver("put:seed:manifest:1")
        self.seal("seed", "manifest", 1)
        self.begin("seed", 1)
        for part in ("head", "refs", "receipt"):
            self.stage("seed", part, 1)
        self.commit("seed", 1)
        self.wire.clear()
        self.mutant = mutant

    def clone(self):
        return deepcopy(self)

    def image(self):
        return {
            "metadata": self.m,
            "objects": self.store,
            "wire": self.wire,
            "sequence": self.sequence,
            "local": self.local,
        }

    @classmethod
    def restore(cls, image, mutant=""):
        result = cls.__new__(cls)
        result.mutant = mutant
        result.m, result.store, result.wire, result.sequence, result.local = deepcopy(
            (image["metadata"], image["objects"], image["wire"], image["sequence"], image["local"])
        )
        return result

    def fingerprint(self):
        return compact(self.image())

    def fence(self, token):
        need(token == self.m["owner"], "FENCED")

    def _unit(self, name, kind, charge, files):
        self.m["units"][name] = {"kind": kind, "charge": charge, "files": files, "state": "allocated"}

    def _object(self, name, unit, role, size, sha):
        self.m["objects"][name] = {
            "unit": unit,
            "role": role,
            "size": size,
            "sha": sha,
            "version": None,
            "sealed": False,
        }

    def _put(self, key, kind, sha, size, marker=None):
        versions = self.store.setdefault(key, [])
        number = self.sequence.get(key, 0) + 1
        self.sequence[key] = number
        version = f"version:{key}:{number}"
        # ETag is an opaque conditional token, deliberately different from SHA-256.
        versions.append(
            {"id": version, "etag": "opaque:" + sha[:20], "kind": kind, "sha": sha, "size": size, "marker": marker}
        )
        return version

    def head_object(self, key):
        versions = self.store.get(key, [])
        return versions[-1] if versions else None

    def version(self, key, version):
        return next((v for v in self.store.get(key, []) if v["id"] == version), None)

    def request(self, name):
        need(name in self.m["requests"], "UNKNOWN_REQUEST")
        return self.m["requests"][name]

    def prepare(
        self,
        name,
        token,
        expected,
        source,
        payload=None,
        table=TABLE,
        epoch=1,
        artifact_bytes=4096,
        rows=128,
        dimension=8,
        segments=1,
    ):
        need(table == TABLE and epoch == 1, "STALE_REQUEST_IDENTITY")
        payload = digest(name + ":" + source) if payload is None else payload
        if name in self.m["requests"]:
            req = self.request(name)
            need(
                req["digest"] == payload and req["expected"] == expected and req["source"] == source, "PAYLOAD_MISMATCH"
            )
            return deepcopy(req)
        self.fence(token)
        need(source in self.m["native"], "INPUT_SNAPSHOT_CHANGED")
        need(0 < artifact_bytes <= LIMITS["artifact_bytes"], "ARTIFACT_BUDGET")
        need(
            1 <= dimension <= 4096 and 65 <= rows <= min(LIMITS["rows"], LIMITS["vector_bytes"] // (4 * dimension)),
            "ROW_VECTOR_BUDGET",
        )
        need(1 <= segments <= LIMITS["segments"], "SEGMENT_BUDGET")
        need(self.m["units"][source]["charge"] <= LIMITS["source_bytes"], "SOURCE_BUDGET")
        need(expected == self.m["revision"], "REVISION_CONFLICT")
        unit = "generation:" + name
        prefix = f"{CATALOG}/{TABLE}/owner-{token}/{name}"
        index, manifest = prefix + "/index", prefix + "/manifest"
        self._unit(unit, "generation", artifact_bytes, [index, manifest])
        self._object(index, unit, "index", artifact_bytes, digest("index:" + payload))
        self._object(manifest, unit, "manifest", 0, None)
        self.m["requests"][name] = {
            "table": table,
            "epoch": epoch,
            "digest": payload,
            "expected": expected,
            "source": source,
            "unit": unit,
            "index": index,
            "manifest": manifest,
            "claim": token,
            "state": "prepared",
            "receipt": None,
            "body": None,
        }
        return deepcopy(self.request(name))

    def writable(self, name, token):
        self.fence(token)
        req = self.request(name)
        need(req["claim"] == token and req["state"] == "prepared", "REQUEST_CLOSED")
        return req

    def takeover(self, expected):
        need(expected == self.m["owner"], "OWNER_CAS_CONFLICT")
        self.m["owner"] += 1
        if self.mutant == "takeover_drops_pins":
            for view in self.m["views"].values():
                view["pins"] = []

    def adopt(self, name, token):
        self.fence(token)
        req = self.request(name)
        need(req["state"] == "prepared", "REQUEST_CLOSED")
        req["claim"] = token
        if self.mutant == "adopt_rebases_request":
            req["expected"] = self.m["revision"]

    def upload(self, name, role, token):
        req = self.writable(name, token)
        need(role in ("index", "manifest"), "BAD_ROLE")
        key = req[role]
        obj = self.m["objects"][key]
        need(obj["sha"] is not None and self.m["units"][obj["unit"]]["state"] == "allocated", "NOT_UPLOADABLE")
        message = f"put:{name}:{role}:{token}"
        need(message not in self.wire, "RPC_ALREADY_REGISTERED")
        self.wire[message] = {
            "op": "put",
            "key": key,
            "sha": obj["sha"],
            "size": obj["size"],
            "condition": "absent",
            "delivered": False,
            "result": None,
            "ack": False,
        }
        return message

    def deliver(self, message, ack=True):
        need(message in self.wire and not self.wire[message]["delivered"], "RPC_NOT_PENDING")
        rpc = self.wire[message]
        key, current = rpc["key"], self.head_object(rpc["key"])
        if rpc["op"] in ("put", "stop"):
            condition = rpc["condition"]
            allowed = (
                (current is None or current["kind"] == "delete-marker")
                if condition == "absent"
                else (current is not None and current["etag"] == condition)
            )
            if self.mutant == "unconditional_put" and rpc["op"] == "put":
                allowed = True
            if allowed:
                kind = "payload" if rpc["op"] == "put" else "stop"
                if self.mutant == "stop_is_delete_marker" and rpc["op"] == "stop":
                    kind = "delete-marker"
                version = self._put(key, kind, rpc["sha"], rpc["size"], rpc.get("marker"))
                rpc["result"] = version
            else:
                rpc["result"] = "PRECONDITION_FAILED"
        else:
            version = rpc["version"]
            if self.mutant == "delete_current_version":
                version = current["id"] if current else version
            self.store[key] = [v for v in self.store.get(key, []) if v["id"] != version]
            rpc["result"] = "ABSENT"
        rpc["delivered"], rpc["ack"] = True, ack

    def duplicate(self, message, new_id):
        need(message in self.wire and new_id not in self.wire, "UNKNOWN_OR_DUPLICATE_RPC")
        self.wire[new_id] = deepcopy(self.wire[message])
        self.wire[new_id].update(delivered=False, result=None, ack=False)

    def seal(self, name, role, token, version=None):
        req = self.writable(name, token)
        need(role in ("index", "manifest"), "BAD_ROLE")
        obj = self.m["objects"][req[role]]
        current = self.head_object(req[role])
        found = self.version(req[role], version) if version is not None else current
        need(found is not None and found["kind"] == "payload", "UPLOAD_INCOMPLETE")
        need(self.m["units"][req["unit"]]["state"] == "allocated", "OBJECT_RETIRING")
        if self.mutant != "skip_digest_validation":
            need(found["sha"] == obj["sha"] and found["size"] == obj["size"], "IDENTITY_MISMATCH")
        obj.update(version=found["id"], sealed=True)

    def descriptor(self, req):
        keys = [req["source"], req["index"]]
        return {
            "table_uuid": TABLE,
            "request_epoch": req["epoch"],
            "payload_hash": req["digest"],
            "input_snapshot": "native-snapshot:" + req["source"],
            "objects": [
                [key, self.m["objects"][key]["version"], self.m["objects"][key]["sha"], self.m["objects"][key]["size"]]
                for key in keys
            ],
        }

    def manifest(self, name, token, padding=0):
        req = self.writable(name, token)
        need(self.m["objects"][req["index"]]["sealed"], "INDEX_NOT_SEALED")
        need(req["source"] in self.m["native"], "INPUT_SNAPSHOT_CHANGED")
        body = compact(self.descriptor(req))
        need(len(body.encode()) + padding <= LIMITS["manifest_bytes"] and padding >= 0, "MANIFEST_BUDGET")
        obj = self.m["objects"][req["manifest"]]
        need(
            req["body"] is None or (req["body"] == body and obj["size"] == len(body.encode()) + padding),
            "MANIFEST_CHANGED",
        )
        req["body"] = body
        obj.update(size=len(body.encode()) + padding, sha=digest(body))

    def validate_commit(self, name, token):
        req = self.writable(name, token) if self.mutant != "skip_owner_fence" else self.request(name)
        if self.mutant != "skip_revision_cas":
            need(req["expected"] == self.m["revision"], "REVISION_CONFLICT")
        need(req["state"] == "prepared" and req["source"] in self.m["native"], "REQUEST_CLOSED")
        need(self.m["units"][req["unit"]]["state"] == "allocated", "OBJECT_RETIRING")
        need(all(self.m["objects"][req[r]]["sealed"] for r in ("index", "manifest")), "UNSEALED_PUBLICATION")
        need(req["body"] == compact(self.descriptor(req)), "MANIFEST_CHANGED")
        return req

    def begin(self, name, token):
        req = self.validate_commit(name, token)
        key = f"{name}:{token}"
        need(key not in self.local["frames"], "TRANSACTION_EXISTS")
        self.local["frames"][key] = {"token": token, "parts": []}
        return req["digest"]

    def stage(self, name, part, token):
        key = f"{name}:{token}"
        need(key in self.local["frames"] and part in ("head", "refs", "receipt"), "NO_TRANSACTION")
        frame = self.local["frames"][key]
        need(part not in frame["parts"], "PART_ALREADY_STAGED")
        frame["parts"].append(part)
        frame["parts"].sort()
        if self.mutant == "split_publication" and part == "head":
            self.m["head"] = name
            self.m["revision"] += 1

    def commit(self, name, token, ack=True):
        req = self.request(name)
        if req["receipt"] is not None:
            return self.reconcile(name, req["digest"])
        key = f"{name}:{token}"
        need(
            key in self.local["frames"]
            and self.local["frames"][key]["token"] == token
            and self.local["frames"][key]["parts"] == ["head", "receipt", "refs"],
            "INCOMPLETE_TRANSACTION",
        )
        if self.mutant == "skip_commit_revalidation":
            self.fence(token)
            req = self.request(name)
        else:
            req = self.validate_commit(name, token)
        revision = self.m["revision"] + 1
        receipt = {
            "request": name,
            "table_uuid": TABLE,
            "request_epoch": req["epoch"],
            "payload_hash": req["digest"],
            "revision": revision,
        }
        record = {
            "request": name,
            "revision": revision,
            "owner": token,
            "body": req["body"],
            "source": req["source"],
            "unit": req["unit"],
            "manifest": req["manifest"],
            "manifest_version": self.m["objects"][req["manifest"]]["version"],
        }
        # These assignments stand for ONE durable metadata transaction, not a client-side multi-write sequence.
        self.m.update(head=name, revision=revision, refs=sorted([req["source"], req["unit"]]))
        req.update(state="committed", receipt=receipt)
        self.m["units"][req["unit"]]["state"] = "ready"
        self.m["commits"].append(record)
        del self.local["frames"][key]
        return deepcopy(receipt) if ack else None

    def reconcile(self, name, payload):
        req = self.request(name)
        need(payload == req["digest"], "PAYLOAD_MISMATCH")
        if self.mutant == "reconcile_returns_head":
            return deepcopy(self.request(self.m["head"])["receipt"])
        return deepcopy(req["receipt"])

    def cancel(self, name, token):
        self.fence(token)
        req = self.request(name)
        need(req["state"] == "prepared", "REQUEST_CLOSED")
        req["state"] = "cancelled"

    def capture(self, name, kind="vector", source="source-a"):
        need(name not in self.m["views"] and kind in ("vector", "sql"), "VIEW_EXISTS_OR_KIND")
        req = self.request(self.m["head"])
        need(kind != "sql" or source in self.m["native"], "EXPIRED_NATIVE_HISTORY")
        units = sorted([req["source"], req["unit"]]) if kind == "vector" else [source]
        if self.mutant == "mixed_readview" and kind == "vector":
            units[units.index(req["source"])] = "source-b" if req["source"] == "source-a" else "source-a"
            units.sort()
        need(all(self.m["units"][u]["state"] not in ("deleting", "deleted") for u in units), "OBJECT_RETIRING")
        self.m["views"][name] = {
            "kind": kind,
            "revision": self.m["revision"],
            "root": self.m["head"],
            "units": units,
            "pins": list(units),
            "closed": False,
        }

    def materialize(self, name):
        need(name in self.m["views"] and not self.m["views"][name]["closed"], "VIEW_CLOSED")
        view = self.m["views"][name]
        need(view["pins"] == view["units"], "VIEW_NOT_PINNED")
        data = []
        for unit in view["units"]:
            for key in self.m["units"][unit]["files"]:
                obj = self.m["objects"][key]
                raw = self.version(key, obj["version"])
                need(
                    raw is not None and raw["sha"] == obj["sha"] and raw["size"] == obj["size"],
                    "READ_IDENTITY_MISMATCH",
                )
                data.append([key, raw["id"], raw["sha"], raw["size"]])
        self.local["copies"][name] = data

    def release_copy_pin(self, name):
        need(name in self.local["copies"] and not self.m["views"][name]["closed"], "COPY_INCOMPLETE")
        self.m["views"][name]["pins"] = []

    def close(self, name):
        need(name in self.m["views"] and not self.m["views"][name]["closed"], "VIEW_CLOSED")
        self.m["views"][name].update(pins=[], closed=True)
        self.local["copies"].pop(name, None)

    def expire_native(self, source, token):
        self.fence(token)
        need(source in self.m["native"] and source not in self.m["refs"], "NATIVE_STILL_CURRENT")
        if self.mutant != "ignore_sql_gate":
            need(
                not any(v["kind"] == "sql" and not v["closed"] for v in self.m["views"].values()),
                "ORDINARY_TRANSACTION",
            )
        self.m["native"].remove(source)

    def protected(self):
        result = set(self.m["refs"]) | set(self.m["native"])
        for req in self.m["requests"].values():
            if req["state"] == "prepared":
                result.update([req["source"], req["unit"]])
        for view in self.m["views"].values():
            result.update(view["pins"])
        return result

    def gc_round(self, name, token, objects=1, bytes_=64 * MIB):
        self.fence(token)
        need(
            name not in self.local["rounds"]
            and 0 < objects <= LIMITS["gc_objects"]
            and 0 < bytes_ <= LIMITS["gc_bytes"],
            "GC_BUDGET",
        )
        self.local["rounds"][name] = {"owner": token, "max_objects": objects, "max_bytes": bytes_, "units": []}

    def plan_gc(self, unit, token, round_):
        self.fence(token)
        need(unit in self.m["units"] and round_ in self.local["rounds"], "UNKNOWN_GC_UNIT_OR_ROUND")
        item, budget = self.m["units"][unit], self.local["rounds"][round_]
        need(budget["owner"] == token, "GC_ROUND_OWNER")
        need(item["state"] != "deleted", "ALREADY_COLLECTED")
        if self.mutant != "ignore_gc_references":
            need(unit not in self.protected(), "REFERENCED")
        need(
            item["kind"] != "source"
            or not any(v["kind"] == "sql" and not v["closed"] for v in self.m["views"].values()),
            "ORDINARY_TRANSACTION",
        )
        if self.mutant != "ignore_gc_budget":
            need(
                len(budget["units"]) < budget["max_objects"]
                and sum(self.m["units"][u]["charge"] for u in budget["units"]) + item["charge"] <= budget["max_bytes"],
                "GC_BUDGET",
            )
        need(unit not in budget["units"], "DUPLICATE_GC_ADMISSION")
        budget["units"].append(unit)
        item["state"] = "deleting"
        if unit not in self.m["gc"]:
            self.m["gc"][unit] = {"nonce": "gc:" + unit, "claim": token, "targets": {}, "barriers": []}
        self.m["gc"][unit]["claim"] = token

    def gc_access(self, unit, token, round_):
        self.fence(token)
        need(
            unit in self.m["gc"]
            and self.m["units"][unit]["state"] == "deleting"
            and self.m["gc"][unit]["claim"] == token
            and round_ in self.local["rounds"]
            and self.local["rounds"][round_]["owner"] == token
            and unit in self.local["rounds"][round_]["units"],
            "NO_GC_ADMISSION",
        )
        return self.m["gc"][unit]

    def stop(self, unit, key, token, round_):
        intent = self.gc_access(unit, token, round_)
        need(key in self.m["units"][unit]["files"], "KEY_NOT_OWNED")
        obj, current = self.m["objects"][key], self.head_object(key)
        barrier_kind = "delete-marker" if self.mutant == "stop_is_delete_marker" else "stop"
        if current is not None and current["kind"] == barrier_kind:
            need(current["marker"] == intent["nonce"], "FOREIGN_STOP")
            if key not in intent["barriers"]:
                intent["barriers"].append(key)
                intent["barriers"].sort()
            return None
        if current is not None:
            need(
                current["kind"] == "payload" and current["sha"] == obj["sha"] and current["size"] == obj["size"],
                "IDENTITY_MISMATCH",
            )
        target = current["id"] if current is not None else None
        message = f"stop:{unit}:{key}:{target}:{token}"
        need(message not in self.wire, "RPC_ALREADY_REGISTERED")
        intent["targets"][key] = target
        body = compact(
            {"kind": "vortex-stop-v1", "catalog": CATALOG, "table": TABLE, "key": key, "intent": intent["nonce"]}
        )
        self.wire[message] = {
            "op": "stop",
            "key": key,
            "sha": digest(body),
            "size": len(body.encode()),
            "marker": intent["nonce"],
            "condition": current["etag"] if current else "absent",
            "delivered": False,
            "result": None,
            "ack": False,
        }
        return message

    def delete_payload(self, unit, key, token, round_):
        intent = self.gc_access(unit, token, round_)
        need(key in self.m["units"][unit]["files"] and key in intent["targets"], "KEY_NOT_OWNED")
        if self.mutant != "omit_stop_barrier":
            need(
                key in intent["barriers"]
                and self.head_object(key) is not None
                and self.head_object(key)["kind"]
                == ("delete-marker" if self.mutant == "stop_is_delete_marker" else "stop"),
                "STOP_NOT_CONFIRMED",
            )
        version = intent["targets"][key]
        if version is None:
            return None
        message = f"delete:{unit}:{key}:{version}:{token}"
        need(message not in self.wire, "RPC_ALREADY_REGISTERED")
        self.wire[message] = {
            "op": "delete",
            "key": key,
            "version": version,
            "delivered": False,
            "result": None,
            "ack": False,
        }
        return message

    def finish_gc(self, unit, token, round_):
        if unit in self.m["completed"]:
            return deepcopy(self.m["completed"][unit])
        intent = self.gc_access(unit, token, round_)
        files = self.m["units"][unit]["files"]
        if self.mutant != "omit_stop_barrier":
            need(set(intent["barriers"]) == set(files), "STOP_NOT_CONFIRMED")
        need(not any(v["kind"] == "payload" for k in files for v in self.store.get(k, [])), "PAYLOAD_REMAINS")
        self.m["units"][unit]["state"] = "deleted"
        self.m["completed"][unit] = {"objects": 1, "bytes": self.m["units"][unit]["charge"]}
        if self.mutant == "delete_stop_after_finish":
            for key in files:
                self.store[key] = []
        if self.mutant == "sweep_foreign_keys":
            self.store["foreign/unregistered"] = []
        return deepcopy(self.m["completed"][unit])

    def crash(self, token):
        # Only this writer/collector process is lost; independent reader processes retain their private copies.
        self.local["frames"] = {k: v for k, v in self.local["frames"].items() if v["token"] != token}
        self.local["rounds"] = {k: v for k, v in self.local["rounds"].items() if v["owner"] != token}

    def step(self, command):
        operation, *args = command
        need(
            not operation.startswith("_")
            and operation
            in {
                "prepare",
                "takeover",
                "adopt",
                "upload",
                "deliver",
                "duplicate",
                "seal",
                "manifest",
                "begin",
                "stage",
                "commit",
                "reconcile",
                "cancel",
                "capture",
                "materialize",
                "release_copy_pin",
                "close",
                "expire_native",
                "gc_round",
                "plan_gc",
                "stop",
                "delete_payload",
                "finish_gc",
                "crash",
            },
            "UNKNOWN_OPERATION",
        )
        return getattr(self, operation)(*args)
