"""Raw PostgreSQL/S3 assertions independent of the bound catalog implementation."""

import hashlib
import json
import struct


TABLES = ("bound_units", "bound_requests", "bound_heads", "bound_readers")


def require(value, message):
    if not value:
        raise AssertionError(message)


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def identity(c, table_uuid, snapshot):
    table = c.execute(
        "SELECT table_uuid::text,table_id,dimension,encode(schema_hash,'hex') AS schema_hash,begin_snapshot,end_snapshot FROM typed_tables WHERE table_uuid=%s",
        (table_uuid,),
    ).fetchone()
    require(
        table
        and table["begin_snapshot"] <= snapshot
        and (table["end_snapshot"] is None or snapshot < table["end_snapshot"]),
        "Binding outside typed lifetime",
    )
    fields = c.execute(
        "SELECT field_id FROM typed_fields WHERE table_uuid=%s ORDER BY ordinal", (table_uuid,)
    ).fetchall()
    rows = c.execute(
        """SELECT id,physical_row_id,
      ARRAY(SELECT encode(float4send(x),'hex') FROM unnest(embedding) x) AS embedding,
      encode(category,'hex') AS category,encode(payload,'hex') AS payload,encode(float4send(f32),'hex') AS f32,
      encode(float8send(f64),'hex') AS f64,encode(data,'hex') AS data,
      CASE WHEN numbers IS NULL THEN NULL ELSE ARRAY(SELECT encode(float4send(x),'hex') FROM unnest(numbers) x) END AS numbers
      FROM typed_rows WHERE table_uuid=%s AND begin_snapshot<=%s AND (end_snapshot IS NULL OR end_snapshot>%s) ORDER BY id""",
        (table_uuid, snapshot, snapshot),
    ).fetchall()
    digest = hashlib.sha256()
    for row in rows:
        packed = bytearray(
            row["physical_row_id"].to_bytes(8, "big", signed=True) + row["id"].to_bytes(8, "big", signed=True)
        )
        for name in ("embedding", "category", "payload", "f32", "f64", "data", "numbers"):
            value = row[name]
            packed += bytes([value is not None])
            if value is None:
                continue
            if name in ("embedding", "numbers"):
                packed += len(value).to_bytes(4, "big")
                for word in value:
                    packed += bytes([word is not None])
                    if word is not None:
                        packed += bytes.fromhex(word)
            elif name in ("f32", "f64"):
                packed += bytes.fromhex(value)
            else:
                blob = bytes.fromhex(value)
                packed += len(blob).to_bytes(4, "big") + blob
        digest.update(packed)
    result = {k: table[k] for k in ("table_uuid", "table_id", "schema_hash", "dimension")}
    result.update(
        vector_field_id=fields[1]["field_id"],
        field_ids=[f["field_id"] for f in fields],
        snapshot_id=snapshot,
        row_ids=[r["physical_row_id"] for r in rows],
        row_digest=digest.hexdigest(),
    )
    return result, rows


def observe(backend, services, storage_oracle, typed_oracle):
    raw = storage_oracle.observe(services.dsn, backend.schema, services.root_client, services.bucket)
    typed = typed_oracle.observe(backend)
    expected = {}
    with backend.connect() as c:
        c.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        for table in TABLES:
            raw[table] = sorted(c.execute("SELECT * FROM " + table).fetchall(), key=encode)
        for unit in raw["bound_units"]:
            key = (str(unit["table_uuid"]), unit["identity"]["snapshot_id"])
            expected[key] = identity(c, *key)
        for view in raw["bound_readers"]:
            key = (str(view["table_uuid"]), view["snapshot_id"])
            expected[key] = identity(c, *key)
    raw = json.loads(encode(raw))
    return raw, typed, expected


def inspect(raw, typed, expected, copies, before=None):
    control = raw["control"][0]
    tables = {t["table_uuid"]: t for t in typed["tables"]}
    units = {u["id"]: u for u in raw["units"]}
    objects = {o["key"]: o for o in raw["objects"]}
    bindings = {b["unit"]: b for b in raw["bound_units"]}
    requests = {r["id"]: r for r in raw["requests"]}
    scopes = {r["request"]: r for r in raw["bound_requests"]}
    versions = {(v["key"], v["version"]): v for v in raw["versions"]}
    for name, binding in bindings.items():
        table_uuid = binding["table_uuid"]
        require(units[name]["table_uuid"] == table_uuid, "Storage and typed UUID differ")
        require(
            binding["identity"] == expected[(table_uuid, binding["identity"]["snapshot_id"])][0],
            "Source/schema/snapshot binding differs",
        )
        require(
            binding["table_id"] == tables[table_uuid]["table_id"]
            and binding["dimension"] == tables[table_uuid]["dimension"],
            "Table ID or vector dimension crossed UUID",
        )
    for ref in raw["refs"]:
        require(units[ref["unit"]]["state"] not in ("deleting", "deleted"), "Protected unit was collected")
    for obj in objects.values():
        unit = units[obj["unit"]]
        version = versions.get((obj["key"], obj["version"]))
        if obj["sealed"] and unit["state"] not in ("deleting", "deleted"):
            require(
                version and version["sha"] == obj["sha"] and version["size"] == obj["size"],
                "Sealed object missing or corrupt",
            )
        if unit["state"] == "deleted":
            intent = next(g for g in raw["gc"] if g["unit"] == unit["id"])
            body = encode(
                {
                    "kind": "vortex-stop-v1",
                    "catalog": control["catalog"],
                    "table": unit["table_uuid"],
                    "key": obj["key"],
                    "intent": intent["nonce"],
                }
            ).encode()
            remaining = [v for v in raw["versions"] if v["key"] == obj["key"]]
            require(
                remaining and all(v["sha"] == hashlib.sha256(body).hexdigest() for v in remaining),
                "Wrong-UUID STOP or resurrected payload",
            )
            require(intent["receipt"] == {"objects": 1, "bytes": unit["charge"]}, "GC accounting changed")
    committed = [r for r in requests.values() if r["receipt"] is not None]
    require(
        control["revision"] == len(committed)
        and sorted(r["receipt"]["catalog_revision"] for r in committed) == list(range(1, len(committed) + 1)),
        "Catalog publication atomicity failed",
    )
    if committed:
        require(
            requests[control["head"]]["receipt"]["catalog_revision"] == control["revision"],
            "Last publication pointer differs",
        )
    for req in requests.values():
        scope = scopes[req["id"]]
        table_uuid = scope["table_uuid"]
        key = f"{table_uuid}:{int(scope['request_epoch'])}:{scope['request_id']}"
        require(
            req["id"] == key and bindings[req["source"]]["table_uuid"] == table_uuid,
            "Request scope/physical input differs",
        )
        require(
            bindings["generation:" + key]["identity"] == bindings[req["source"]]["identity"],
            "Generation/source identities mixed",
        )
        if req["receipt"] is None:
            continue
        receipt = req["receipt"]
        require(
            receipt["table_uuid"] == table_uuid
            and receipt["table_id"] == tables[table_uuid]["table_id"]
            and receipt["request_epoch"] == int(scope["request_epoch"])
            and receipt["request_id"] == scope["request_id"]
            and receipt["payload_hash"] == req["payload_hash"]
            and receipt["revision"] == req["expected"] + 1,
            "Receipt identity changed",
        )
        index = next(o for o in objects.values() if o["unit"] == "generation:" + key and o["role"] == "index")
        source = next(o for o in objects.values() if o["unit"] == req["source"])
        manifest = next(o for o in objects.values() if o["unit"] == "generation:" + key and o["role"] == "manifest")
        body = encode(
            {
                "layout": "bound-manifest-v1",
                "catalog_uuid": control["catalog"],
                "binding": bindings[req["source"]]["identity"],
                "request_epoch": int(scope["request_epoch"]),
                "request_id": scope["request_id"],
                "request_key": key,
                "payload_hash": req["payload_hash"],
                "expected_revision": req["expected"],
                "store": {k: control[k] for k in ("endpoint", "bucket", "prefix")},
                "objects": [{k: o[k] for k in ("key", "version", "size", "sha")} for o in (source, index)],
            }
        )
        require(
            req["manifest"] == body
            and manifest["sha"] == hashlib.sha256(body.encode()).hexdigest()
            and manifest["size"] == len(body.encode()),
            "Manifest identity/version/byte closure differs",
        )
    for head in raw["bound_heads"]:
        table_uuid = head["table_uuid"]
        local_revisions = sorted(
            r["receipt"]["revision"] for r in committed if scopes[r["id"]]["table_uuid"] == table_uuid
        )
        require(
            head["revision"] == len(local_revisions) and local_revisions == list(range(1, len(local_revisions) + 1)),
            "Per-UUID publication history forked",
        )
        refs = {r["unit"] for r in raw["refs"] if r["kind"] == "named" and r["holder"] == "bound:" + table_uuid}
        if tables[table_uuid]["end_snapshot"] is not None:
            require(head["request"] is None and not refs, "Retirement left live named references")
        elif head["request"] is None:
            require(not refs, "Empty head owns references")
        else:
            req = requests[head["request"]]
            require(
                scopes[req["id"]]["table_uuid"] == table_uuid
                and req["receipt"]["revision"] == head["revision"]
                and refs == {req["source"], "generation:" + req["id"]},
                "Head/reference transaction differs",
            )
    readers = {r["id"]: r for r in raw["readers"]}
    for binding in raw["bound_readers"]:
        name, table_uuid = binding["reader"], binding["table_uuid"]
        view, bound = readers[name], binding["identity"]
        query, query_rows = expected[(table_uuid, binding["snapshot_id"])]
        require(bound["query"] == query, "ReadView queried another snapshot/schema")
        source_units = [u for u in view["units"] if units[u]["kind"] == "source"]
        require(
            bound["root"] == view["root"]
            and bound["revision"] == view["revision"]
            and len(source_units) == 1
            and bindings[source_units[0]]["identity"] == bound["source"]
            and all(bindings[u]["table_uuid"] == table_uuid for u in view["units"]),
            "ReadView source/root binding differs",
        )
        if view["kind"] == "vector":
            req = requests[view["root"]]
            require(
                scopes[req["id"]]["table_uuid"] == table_uuid
                and bound["source"] == bindings[req["source"]]["identity"]
                and view["revision"] == req["receipt"]["revision"]
                and set(view["units"]) == {req["source"], "generation:" + req["id"]},
                "ReadView root/units mixed across tables",
            )
        require(
            bound["visible_source_rows"] == sorted(set(query["row_ids"]) & set(bound["source"]["row_ids"])),
            "Physical-row visibility mixed",
        )
        pins = {r["unit"] for r in raw["refs"] if r["kind"] == "reader" and r["holder"] == name}
        if view["closed"]:
            require(not pins, "Closed view still pinned")
        elif not pins:
            require(name in copies and copies[name]["identity"] == bound, "Unprotected view has no authenticated copy")
        else:
            require(pins == set(view["units"]), "ReadView pin set incomplete")
        if name in copies:
            require(
                copies[name]["identity"] == bound
                and [r["physical_row_id"] for r in copies[name]["rows"]] == query["row_ids"],
                "Private typed copy identity changed",
            )
            require(
                set(copies[name]["objects"]) == {o["key"] for o in objects.values() if o["unit"] in view["units"]},
                "Private object copy is incomplete",
            )
            for key, data in copies[name]["objects"].items():
                require(
                    hashlib.sha256(data).hexdigest() == objects[key]["sha"] and len(data) == objects[key]["size"],
                    "Private object copy corrupted",
                )
            decoded = []
            for row in copies[name]["rows"]:
                projected = {
                    "id": row["id"],
                    "physical_row_id": row["physical_row_id"],
                    "embedding": [struct.pack("!f", v).hex() for v in row["embedding"]],
                    "numbers": (
                        None
                        if row["numbers"] is None
                        else [None if v is None else struct.pack("!f", v).hex() for v in row["numbers"]]
                    ),
                }
                for field in ("category", "payload"):
                    projected[field] = None if row[field] is None else row[field].encode("utf-8").hex()
                for field in ("data",):
                    projected[field] = None if row[field] is None else bytes(row[field]).hex()
                for field, code in (("f32", "!f"), ("f64", "!d")):
                    projected[field] = None if row[field] is None else struct.pack(code, row[field]).hex()
                decoded.append(projected)
            require(decoded == query_rows, "Private payload/vector values came from another view")
    if before is not None:
        require(
            all(bindings[b["unit"]] == b for b in before["bound_units"]),
            "Persisted source/generation identity changed",
        )
        previous = {r["id"]: r for r in before["requests"]}
        for key, req in previous.items():
            require(
                all(req[k] == requests[key][k] for k in ("id", "payload_hash", "source", "expected")),
                "Original request was rebased",
            )
            if req["receipt"] is not None:
                require(req["receipt"] == requests[key]["receipt"], "Original receipt changed")
        foreign = lambda image: [v for v in image["versions"] if v["key"] not in objects]
        require(foreign(raw) == foreign(before), "Foreign objects were changed")
