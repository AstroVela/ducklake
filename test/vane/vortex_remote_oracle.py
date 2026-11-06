"""History/ownership assertions independent of the executable protocol's guards."""

import hashlib
import json


class Violation(AssertionError):
    pass


def require(condition, code):
    if not condition:
        raise Violation(code)


def inspect(model, before=None, command=None, result=None):
    meta, raw, local = model.m, model.store, model.local
    units, requests = meta["units"], meta["requests"]
    commits = meta["commits"]
    require(meta["revision"] == len(commits), "ATOMIC_HEAD_RECEIPT")
    require([c["revision"] for c in commits] == list(range(1, len(commits) + 1)), "REVISION_CHAIN")
    require(commits[-1]["request"] == meta["head"], "HEAD_COMMIT_DISAGREEMENT")
    current = requests[meta["head"]]
    require(set(meta["refs"]) == {current["source"], current["unit"]}, "ATOMIC_REFERENCE_SET")

    protected, complete = set(meta["native"]) | set(meta["refs"]), set(meta["native"]) | set(meta["refs"])
    for req in requests.values():
        if req["state"] == "prepared":
            protected.update((req["unit"], req["source"]))
        if req["receipt"] is not None:
            record = commits[req["receipt"]["revision"] - 1]
            receipt = req["receipt"]
            require(
                record["request"] == receipt["request"]
                and receipt["payload_hash"] == req["digest"]
                and receipt["table_uuid"] == req["table"]
                and receipt["request_epoch"] == req["epoch"],
                "RECEIPT_IDENTITY",
            )
    for name, view in meta["views"].items():
        if view["closed"]:
            require(not view["pins"], "CLOSED_VIEW_PINNED")
            continue
        if view["kind"] == "vector":
            captured = commits[view["revision"] - 1]
            require(
                captured["request"] == view["root"] and set(view["units"]) == {captured["source"], captured["unit"]},
                "MIXED_READVIEW",
            )
        require(set(view["pins"]) in (set(view["units"]), set()), "PARTIAL_VIEW_PIN")
        if not view["pins"]:
            require(name in local["copies"], "UNPROTECTED_READVIEW")
        protected.update(view["pins"])
        complete.update(view["pins"])
        if name in local["copies"]:
            expected = []
            for unit in view["units"]:
                for key in units[unit]["files"]:
                    obj = meta["objects"][key]
                    expected.append([key, obj["version"], obj["sha"], obj["size"]])
            require(local["copies"][name] == expected, "PRIVATE_COPY_CHANGED")

    for name, unit in units.items():
        if name in protected:
            require(unit["state"] not in ("deleting", "deleted"), "GC_REFERENCED_UNIT")
        for key in unit["files"]:
            obj = meta["objects"][key]
            versions = raw.get(key, [])
            found = next((v for v in versions if v["id"] == obj["version"]), None)
            if name in complete or (obj["sealed"] and unit["state"] not in ("deleting", "deleted")):
                require(
                    obj["sealed"]
                    and found is not None
                    and found["kind"] == "payload"
                    and found["sha"] == obj["sha"]
                    and found["size"] == obj["size"],
                    "VISIBLE_OBJECT_INCOMPLETE",
                )
            if unit["state"] == "deleted":
                require(not any(v["kind"] == "payload" for v in versions), "LATE_UPLOAD_RESURRECTION")
        if unit["state"] == "deleted":
            require(
                name in meta["completed"] and meta["completed"][name] == {"objects": 1, "bytes": unit["charge"]},
                "GC_ACCOUNTING",
            )
    require(
        raw.get("foreign/unregistered")
        == [
            {
                "id": "foreign-version",
                "etag": "foreign-etag",
                "kind": "payload",
                "sha": hashlib.sha256(b"foreign").hexdigest(),
                "size": 19,
                "marker": None,
            }
        ],
        "FOREIGN_OBJECT_CHANGED",
    )
    for budget in local["rounds"].values():
        require(
            len(budget["units"]) <= budget["max_objects"]
            and sum(units[u]["charge"] for u in budget["units"]) <= budget["max_bytes"],
            "GC_BUDGET",
        )

    for record in commits:
        req = requests[record["request"]]
        require(record["body"] is not None, "MISSING_MANIFEST")
        body = json.loads(record["body"])
        require(
            body["table_uuid"] == req["table"]
            and body["request_epoch"] == req["epoch"]
            and body["payload_hash"] == req["digest"]
            and body["input_snapshot"] == "native-snapshot:" + req["source"],
            "MANIFEST_LINEAGE",
        )
        expected = [
            [k, meta["objects"][k]["version"], meta["objects"][k]["sha"], meta["objects"][k]["size"]]
            for k in (req["source"], req["index"])
        ]
        require(body["objects"] == expected, "MANIFEST_VERSION_BINDING")
        manifest = meta["objects"][record["manifest"]]
        require(
            manifest["sha"] == hashlib.sha256(record["body"].encode()).hexdigest()
            and manifest["version"] == record["manifest_version"],
            "SEALED_MANIFEST_CHANGED",
        )

    if before is None:
        return
    old = before.m
    require(commits[: len(old["commits"])] == old["commits"], "COMMITTED_HISTORY_CHANGED")
    for name, req in old["requests"].items():
        fields = ("table", "epoch", "digest", "expected", "source", "unit", "index", "manifest")
        require(all(req[f] == requests[name][f] for f in fields), "REQUEST_IDENTITY_REWRITTEN")
        if req["receipt"] is not None:
            require(req["receipt"] == requests[name]["receipt"], "COMMITTED_RECEIPT_CHANGED")
    if len(commits) > len(old["commits"]):
        require(command[0] == "commit" and command[2] == old["owner"], "STALE_OWNER_COMMIT")
        require(old["requests"][command[1]]["expected"] == old["revision"], "PUBLICATION_CAS_MISSING")
        prior = old["requests"][command[1]]
        require(
            prior["state"] == "prepared" and old["units"][prior["unit"]]["state"] not in ("deleting", "deleted"),
            "CANCELLED_OR_RETIRED_REQUEST_PUBLISHED",
        )
    if command[0] == "expire_native":
        require(not any(v["kind"] == "sql" and not v["closed"] for v in old["views"].values()), "SQL_GATE_WEAKENED")
    if command[0] == "reconcile":
        require(result == old["requests"][command[1]]["receipt"], "RECONCILE_CHANGED_IDENTITY")
    for key, versions in before.store.items():
        after_ids = {v["id"] for v in raw.get(key, [])}
        removed = [v for v in versions if v["id"] not in after_ids]
        if not removed:
            continue
        require(command[0] == "deliver", "UNAUTHORIZED_PHYSICAL_DELETE")
        rpc = before.wire[command[1]]
        require(
            rpc["op"] == "delete"
            and rpc["key"] == key
            and {v["id"] for v in removed} == {rpc["version"]}
            and all(v["kind"] == "payload" for v in removed),
            "DELETE_NOT_EXACT_VERSION",
        )
