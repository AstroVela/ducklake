"""Independent PostgreSQL rows / S3 versions oracle; no adapter guards or reads."""

import hashlib
import json

import psycopg
from psycopg import sql
from psycopg.rows import dict_row


TABLES = ("control", "units", "objects", "requests", "refs", "readers", "gc")


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def require(value, code):
    if not value:
        raise AssertionError(code)


def observe(dsn, schema, client, bucket):
    image = {}
    with psycopg.connect(dsn, row_factory=dict_row) as c:
        c.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        for name in TABLES:
            rows = c.execute(
                sql.SQL("SELECT * FROM {}.{}").format(sql.Identifier(schema), sql.Identifier(name))
            ).fetchall()
            image[name] = sorted(rows, key=encode)
    versions = []
    for page in client.get_paginator("list_object_versions").paginate(Bucket=bucket):
        require(not page.get("DeleteMarkers"), "UNEXPECTED_DELETE_MARKER")
        for item in page.get("Versions", []):
            response = client.get_object(Bucket=bucket, Key=item["Key"], VersionId=item["VersionId"])
            try:
                data = response["Body"].read()
            finally:
                response["Body"].close()
            versions.append(
                {
                    "key": item["Key"],
                    "version": item["VersionId"],
                    "latest": item["IsLatest"],
                    "size": len(data),
                    "sha": hashlib.sha256(data).hexdigest(),
                }
            )
    image["versions"] = sorted(versions, key=encode)
    return json.loads(encode(image))


def inspect(image, before=None):
    state = image["control"][0]
    units = {r["id"]: r for r in image["units"]}
    objects = {r["key"]: r for r in image["objects"]}
    requests = {r["id"]: r for r in image["requests"]}
    versions = {(r["key"], r["version"]): r for r in image["versions"]}
    intents = {r["unit"]: r for r in image["gc"]}
    committed = [r for r in requests.values() if r["receipt"] is not None]
    require(state["revision"] == len(committed), "HEAD_RECEIPT_ATOMICITY")
    require(sorted(r["receipt"]["revision"] for r in committed) == list(range(1, len(committed) + 1)), "REVISION_CHAIN")
    named = {r["unit"] for r in image["refs"] if r["kind"] == "named"}
    if state["head"] is not None:
        req = requests[state["head"]]
        require(
            req["receipt"]["revision"] == state["revision"] and named == {"generation:" + req["id"], req["source"]},
            "HEAD_REFERENCES_ATOMICITY",
        )
    else:
        require(not named, "EMPTY_INDEX_REFERENCES")
    for ref in image["refs"]:
        require(units[ref["unit"]]["state"] not in ("deleting", "deleted"), "REFERENCED_OBJECT_RETIRED")
    complete = {r["unit"] for r in image["refs"] if r["kind"] != "build"}
    for key, obj in objects.items():
        unit = units[obj["unit"]]
        if obj["unit"] in complete or (obj["sealed"] and unit["state"] not in ("deleting", "deleted")):
            raw = versions.get((key, obj["version"]))
            require(
                obj["sealed"] and raw and raw["sha"] == obj["sha"] and raw["size"] == obj["size"],
                "VISIBLE_OBJECT_MISSING_OR_CORRUPT",
            )
        if unit["state"] == "deleted":
            intent = intents[unit["id"]]
            expected = {
                "kind": "vortex-stop-v1",
                "catalog": state["catalog"],
                "table": state["table_uuid"],
                "key": key,
                "intent": intent["nonce"],
            }
            body = json.dumps(expected, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
            remaining = [r for r in image["versions"] if r["key"] == key]
            require(
                len(remaining) == 1
                and remaining[0]["latest"]
                and remaining[0]["sha"] == hashlib.sha256(body).hexdigest(),
                "COLLECTED_PAYLOAD_RESURRECTED",
            )
            require(intent["receipt"] == {"objects": 1, "bytes": unit["charge"]}, "GC_ACCOUNTING")
    for req in committed:
        receipt = req["receipt"]
        require(
            receipt["table_uuid"] == state["table_uuid"]
            and receipt["request_epoch"] == state["epoch"]
            and receipt["request_id"] == req["id"]
            and receipt["payload_hash"] == req["payload_hash"]
            and receipt["revision"] == req["expected"] + 1,
            "RECEIPT_IDENTITY",
        )
        body = json.loads(req["manifest"])
        require(body["store"] == {k: state[k] for k in ("endpoint", "bucket", "prefix")}, "MANIFEST_STORE_BINDING")
        require(
            body["table_uuid"] == state["table_uuid"]
            and body["request_epoch"] == state["epoch"]
            and body["request_id"] == req["id"]
            and body["payload_hash"] == req["payload_hash"]
            and body["expected_revision"] == req["expected"]
            and body["input_snapshot"] == units[req["source"]]["snapshot"],
            "MANIFEST_LINEAGE",
        )
        index = next(o for o in objects.values() if o["unit"] == "generation:" + req["id"] and o["role"] == "index")
        source = next(o for o in objects.values() if o["unit"] == req["source"])
        require(
            body["objects"] == [{k: o[k] for k in ("key", "version", "size", "sha")} for o in (source, index)],
            "MANIFEST_VERSION_BINDING",
        )
        manifest = next(
            o for o in objects.values() if o["unit"] == "generation:" + req["id"] and o["role"] == "manifest"
        )
        require(manifest["sha"] == hashlib.sha256(req["manifest"].encode()).hexdigest(), "MANIFEST_CHANGED")
    for view in image["readers"]:
        if view["kind"] == "vector":
            root = requests[view["root"]]
            require(
                view["revision"] == root["receipt"]["revision"]
                and set(view["units"]) == {"generation:" + root["id"], root["source"]},
                "MIXED_READVIEW",
            )
    if before is not None:
        previous = before["control"][0]
        require(
            all(state[k] == previous[k] for k in ("catalog", "table_uuid", "epoch", "endpoint", "bucket", "prefix")),
            "CATALOG_IDENTITY_CHANGED",
        )
        old = {r["id"]: r for r in before["requests"]}
        for name, req in old.items():
            require(
                all(req[k] == requests[name][k] for k in ("id", "payload_hash", "source", "expected")),
                "REQUEST_REBASED",
            )
            if req["receipt"] is not None:
                require(req["receipt"] == requests[name]["receipt"], "COMMITTED_RECEIPT_CHANGED")
        foreign = lambda rows: [r for r in rows if r["key"] not in objects]
        require(foreign(image["versions"]) == foreign(before["versions"]), "UNREGISTERED_OBJECT_CHANGED")
