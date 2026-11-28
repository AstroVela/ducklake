"""Strict typed identity boundary for the experimental PostgreSQL/S3 components.

Every entry uses a UUID/schema or a persisted scoped identity. No identity is
inferred from the storage fixture's catalog-wide table seed, and no old layout
or unbound request is adopted on failure.
"""

from decimal import Decimal
import hashlib
import re
import struct

from psycopg.types.json import Jsonb

from vortex_remote_backend import LIMITS, canonical, require, sha
from vortex_postgres_metadata import MAX_EPOCH, canonical_uuid, encoded_row, integer, normalized


class BoundCatalog:
    def __init__(self, backend, metadata):
        require(metadata.backend is backend, "METADATA_CATALOG_MISMATCH")
        self.backend, self.metadata, self.copies = backend, metadata, {}

    @staticmethod
    def label(value):
        require(type(value) is str and re.fullmatch(r"[a-z0-9-]{1,80}", value), "BAD_IDENTIFIER")
        return value

    @classmethod
    def request_key(cls, table_uuid, epoch, request_id):
        canonical_uuid(table_uuid)
        integer(epoch, 1, MAX_EPOCH, "BAD_EPOCH")
        return f"{table_uuid}:{epoch}:{cls.label(request_id)}"

    def target(self, c, name, table_uuid, schema_hash, epoch=None):
        self.metadata.catalog(c)
        table = self.metadata.target(c, name, table_uuid)
        require(bytes(table["schema_hash"]).hex() == schema_hash, "STALE_SCHEMA")
        if epoch is not None:
            integer(epoch, 1, MAX_EPOCH, "BAD_EPOCH")
            require(epoch >= int(table["request_epoch"]), "EXPIRED_REQUEST")
            require(epoch == int(table["request_epoch"]), "FUTURE_REQUEST")
        return table

    @staticmethod
    def schema(c, table):
        fields = c.execute(
            "SELECT * FROM typed_fields WHERE table_uuid=%s ORDER BY ordinal", (table["table_uuid"],)
        ).fetchall()
        data = b"".join(
            struct.pack("!qI", f["field_id"], f["ordinal"])
            + bytes(f["name"])
            + b"\0"
            + f["logical_type"].encode()
            + bytes([f["nullable"]])
            for f in fields
        ) + struct.pack("!I", table["dimension"])
        require(sha(data) == bytes(table["schema_hash"]).hex(), "SCHEMA_DESCRIPTOR_CHANGED")
        vector = next((f for f in fields if f["ordinal"] == 1), None)
        require(vector and vector["logical_type"] == "FLOAT[]" and not vector["nullable"], "VECTOR_FIELD_MISMATCH")
        return {
            "table_uuid": str(table["table_uuid"]),
            "table_id": table["table_id"],
            "schema_hash": bytes(table["schema_hash"]).hex(),
            "dimension": table["dimension"],
            "vector_field_id": vector["field_id"],
            "field_ids": [f["field_id"] for f in fields],
        }

    def snapshot(self, c, table, snapshot):
        state = self.metadata.catalog(c)
        integer(snapshot, 0, 2**63 - 1, "BAD_SNAPSHOT")
        require(
            table["begin_snapshot"] <= snapshot <= state["snapshot"]
            and (table["end_snapshot"] is None or snapshot < table["end_snapshot"]),
            "TABLE_NOT_IN_SNAPSHOT",
        )
        # Bound typed capture bytes before fetching variable-width row contents.
        size = c.execute(
            """SELECT count(*) AS n,coalesce(sum(128+cardinality(embedding)*5+
            coalesce(octet_length(category),0)+coalesce(octet_length(payload),0)+coalesce(octet_length(data),0)+
            coalesce(cardinality(numbers)*5,0)),0) AS bytes FROM typed_rows WHERE table_uuid=%s AND
            begin_snapshot<=%s AND (end_snapshot IS NULL OR end_snapshot>%s)""",
            (table["table_uuid"], snapshot, snapshot),
        ).fetchone()
        require(size["n"] <= LIMITS["rows"] and size["bytes"] <= LIMITS["source"], "TYPED_CAPTURE_BUDGET")
        rows = self.metadata._rows(c, table["table_uuid"], snapshot)
        digest = hashlib.sha256()
        for row in rows:
            digest.update(struct.pack("!q", row["physical_row_id"]))
            digest.update(
                encoded_row(normalized({k: v for k, v in row.items() if k != "physical_row_id"}, table["dimension"]))
            )
        identity = dict(
            self.schema(c, table),
            snapshot_id=snapshot,
            row_ids=[r["physical_row_id"] for r in rows],
            row_digest=digest.hexdigest(),
        )
        return identity, rows

    def unit(self, c, unit, table_uuid):
        canonical_uuid(table_uuid)
        binding = c.execute(
            "SELECT b.*,u.kind,u.state,u.table_uuid AS storage_uuid FROM bound_units b JOIN units u ON u.id=b.unit WHERE b.unit=%s",
            (unit,),
        ).fetchone()
        require(
            binding and str(binding["table_uuid"]) == table_uuid and str(binding["storage_uuid"]) == table_uuid,
            "UNIT_BINDING_MISMATCH",
        )
        table = c.execute("SELECT * FROM typed_tables WHERE table_uuid=%s", (binding["table_uuid"],)).fetchone()
        require(table is not None, "UNKNOWN_TABLE_UUID")
        identity, _ = self.snapshot(c, table, binding["identity"]["snapshot_id"])
        require(identity == binding["identity"], "SOURCE_BINDING_CHANGED")
        return binding

    @staticmethod
    def insert_binding(c, unit, identity):
        c.execute(
            "INSERT INTO bound_units VALUES(%s,%s,%s,%s,%s,%s)",
            (
                unit,
                canonical_uuid(identity["table_uuid"]),
                identity["table_id"],
                bytes.fromhex(identity["schema_hash"]),
                identity["dimension"],
                Jsonb(identity),
            ),
        )

    def register_source(self, name, table_uuid, schema_hash, snapshot, source_id, data, token):
        self.label(source_id)
        require(0 < len(data) <= LIMITS["source"], "SOURCE_BUDGET")
        unit = f"source:{table_uuid}:{source_id}"
        with self.backend.transaction(token) as (c, _):
            table = self.target(c, name, table_uuid, schema_hash)
            identity, _ = self.snapshot(c, table, snapshot)
            require(len(identity["row_ids"]) * identity["dimension"] * 4 <= LIMITS["vector"], "ROW_VECTOR_BUDGET")
            key = self.backend.objects.prefix + f"{table_uuid}/owner-{token}/source-{source_id}"
            c.execute(
                "INSERT INTO units VALUES(%s,'source',%s,'allocated',%s,%s)",
                (unit, len(data), str(snapshot), table["table_uuid"]),
            )
            c.execute(
                "INSERT INTO objects(key,unit,role,size,sha) VALUES(%s,%s,'source',%s,%s)",
                (key, unit, len(data), sha(data)),
            )
            self.insert_binding(c, unit, identity)
            c.execute("INSERT INTO refs VALUES('build',%s,%s)", (unit, unit))
        return unit

    def source_ticket(self, name, table_uuid, schema_hash, unit, data, token):
        with self.backend.transaction(token) as (c, _):
            self.target(c, name, table_uuid, schema_hash)
            binding = self.unit(c, unit, table_uuid)
            require(binding["kind"] == "source" and binding["state"] == "allocated", "SOURCE_CLOSED")
            obj = self.backend.files(c, unit)[0]
            self.backend.identity(obj, {"sha": sha(data), "size": len(data)})
            return self.backend.objects.ticket(obj["key"], data=data, condition="*")

    def seal_source(self, name, table_uuid, schema_hash, unit, token):
        with self.backend.transaction(token) as (c, _):
            self.target(c, name, table_uuid, schema_hash)
            binding = self.unit(c, unit, table_uuid)
            require(binding["kind"] == "source" and binding["state"] == "allocated", "SOURCE_CLOSED")
            obj = self.backend.files(c, unit)[0]
            raw = self.backend.objects.read(obj["key"])
            self.backend.identity(obj, raw)
            c.execute(
                "UPDATE objects SET sealed=true,version=%s,etag=%s WHERE key=%s",
                (raw["version"], raw["etag"], obj["key"]),
            )
            c.execute("UPDATE units SET state='ready' WHERE id=%s", (unit,))
            c.execute("DELETE FROM refs WHERE kind='build' AND holder=%s", (unit,))
            c.execute("INSERT INTO refs VALUES('native',%s,%s)", (unit, unit))

    def prepare(self, name, table_uuid, schema_hash, epoch, request_id, source, data, expected, token):
        key = self.request_key(table_uuid, epoch, request_id)
        require(0 < len(data) <= LIMITS["artifact"], "ARTIFACT_BUDGET")
        with self.backend.transaction(token) as (c, _):
            table = self.target(c, name, table_uuid, schema_hash, epoch)
            binding = self.unit(c, source, table_uuid)
            require(
                binding["kind"] == "source"
                and binding["state"] == "ready"
                and c.execute("SELECT 1 FROM refs WHERE kind='native' AND unit=%s", (source,)).fetchone(),
                "INPUT_SNAPSHOT_CHANGED",
            )
            require(65 <= len(binding["identity"]["row_ids"]) <= LIMITS["rows"], "ROW_VECTOR_BUDGET")
            payload_hash = sha(canonical([key, binding["identity"], sha(data), len(data), expected]).encode())
            old = c.execute("SELECT * FROM requests WHERE id=%s", (key,)).fetchone()
            if old is not None:
                require(
                    old["payload_hash"] == payload_hash and old["source"] == source and old["expected"] == expected,
                    "PAYLOAD_MISMATCH",
                )
                return old
            head = c.execute("SELECT * FROM bound_heads WHERE table_uuid=%s", (table["table_uuid"],)).fetchone()
            require(head["revision"] == expected, "REVISION_CONFLICT")
            unit = "generation:" + key
            prefix = self.backend.objects.prefix + f"{table_uuid}/owner-{token}/{epoch}/{request_id}/"
            c.execute(
                "INSERT INTO units VALUES(%s,'generation',%s,'allocated',%s,%s)",
                (unit, len(data), str(binding["identity"]["snapshot_id"]), table["table_uuid"]),
            )
            c.execute(
                "INSERT INTO objects(key,unit,role,size,sha) VALUES(%s,%s,'index',%s,%s),(%s,%s,'manifest',0,NULL)",
                (prefix + "index", unit, len(data), sha(data), prefix + "manifest", unit),
            )
            self.insert_binding(c, unit, binding["identity"])
            c.execute(
                "INSERT INTO requests VALUES(%s,%s,%s,%s,%s,'prepared',NULL,NULL)",
                (key, payload_hash, source, expected, token),
            )
            c.execute(
                "INSERT INTO bound_requests VALUES(%s,%s,%s,%s)", (key, table["table_uuid"], Decimal(epoch), request_id)
            )
            for ref in (unit, source):
                c.execute("INSERT INTO refs VALUES('build',%s,%s)", (key, ref))
        return self.status(table_uuid, epoch, request_id)

    def status(self, table_uuid, epoch, request_id):
        key = self.request_key(table_uuid, epoch, request_id)
        with self.backend.connect() as c:
            require(
                c.execute("SELECT 1 FROM bound_requests WHERE request=%s", (key,)).fetchone(), "UNKNOWN_BOUND_REQUEST"
            )
            return self.backend.request(c, key)

    def request(self, c, name, table_uuid, schema_hash, epoch, request_id, token):
        self.target(c, name, table_uuid, schema_hash, epoch)
        key = self.request_key(table_uuid, epoch, request_id)
        require(
            c.execute(
                "SELECT 1 FROM bound_requests WHERE request=%s AND table_uuid=%s AND request_epoch=%s AND request_id=%s",
                (key, canonical_uuid(table_uuid), Decimal(epoch), request_id),
            ).fetchone(),
            "UNKNOWN_BOUND_REQUEST",
        )
        req = self.backend.request(c, key, token)
        self.unit(c, req["source"], table_uuid)
        self.unit(c, "generation:" + key, table_uuid)
        return req

    def adopt(self, name, table_uuid, schema_hash, epoch, request_id, token):
        with self.backend.transaction(token) as (c, _):
            self.target(c, name, table_uuid, schema_hash, epoch)
            key = self.request_key(table_uuid, epoch, request_id)
            require(
                c.execute(
                    "SELECT 1 FROM bound_requests WHERE request=%s AND table_uuid=%s AND request_epoch=%s AND request_id=%s",
                    (key, canonical_uuid(table_uuid), Decimal(epoch), request_id),
                ).fetchone(),
                "UNKNOWN_BOUND_REQUEST",
            )
            req = self.backend.request(c, key)
            require(req["state"] == "prepared", "REQUEST_CLOSED")
            self.unit(c, req["source"], table_uuid)
            self.unit(c, "generation:" + key, table_uuid)
            c.execute("UPDATE requests SET claim=%s WHERE id=%s", (token, key))

    def payload_ticket(self, name, table_uuid, schema_hash, epoch, request_id, role, data, token):
        require(role in ("index", "manifest"), "BAD_ROLE")
        with self.backend.transaction(token) as (c, _):
            req = self.request(c, name, table_uuid, schema_hash, epoch, request_id, token)
            obj = next(o for o in self.backend.files(c, "generation:" + req["id"]) if o["role"] == role)
            require(obj["sha"] is not None and not obj["sealed"], "NOT_UPLOADABLE")
            self.backend.identity(obj, {"sha": sha(data), "size": len(data)})
            return self.backend.objects.ticket(obj["key"], data=data, condition="*")

    def seal(self, name, table_uuid, schema_hash, epoch, request_id, role, token):
        require(role in ("index", "manifest"), "BAD_ROLE")
        with self.backend.transaction(token) as (c, _):
            req = self.request(c, name, table_uuid, schema_hash, epoch, request_id, token)
            obj = next(o for o in self.backend.files(c, "generation:" + req["id"]) if o["role"] == role)
            require(obj["sha"] is not None, "NOT_UPLOADABLE")
            raw = self.backend.objects.read(obj["key"])
            self.backend.identity(obj, raw)
            require(not obj["sealed"] or obj["version"] == raw["version"], "SEALED_VERSION_CHANGED")
            c.execute(
                "UPDATE objects SET sealed=true,version=%s,etag=%s WHERE key=%s",
                (raw["version"], raw["etag"], obj["key"]),
            )

    def manifest(self, name, table_uuid, schema_hash, epoch, request_id, token):
        with self.backend.transaction(token) as (c, state):
            req = self.request(c, name, table_uuid, schema_hash, epoch, request_id, token)
            binding = self.unit(c, req["source"], table_uuid)["identity"]
            source = self.backend.files(c, req["source"])[0]
            index = next(o for o in self.backend.files(c, "generation:" + req["id"]) if o["role"] == "index")
            require(source["sealed"] and index["sealed"], "INDEX_NOT_SEALED")
            body = canonical(
                {
                    "layout": "bound-manifest-v1",
                    "catalog_uuid": str(state["catalog"]),
                    "binding": binding,
                    "request_epoch": epoch,
                    "request_id": request_id,
                    "request_key": req["id"],
                    "payload_hash": req["payload_hash"],
                    "expected_revision": req["expected"],
                    "store": {k: state[k] for k in ("endpoint", "bucket", "prefix")},
                    "objects": [{k: o[k] for k in ("key", "version", "size", "sha")} for o in (source, index)],
                }
            )
            require(len(body.encode()) <= LIMITS["manifest"], "MANIFEST_BUDGET")
            require(req["manifest"] is None or req["manifest"] == body, "MANIFEST_CHANGED")
            c.execute("UPDATE requests SET manifest=%s WHERE id=%s", (body, req["id"]))
            c.execute(
                "UPDATE objects SET size=%s,sha=%s WHERE unit=%s AND role='manifest'",
                (len(body.encode()), sha(body.encode()), "generation:" + req["id"]),
            )
        return body.encode()

    def publish(self, name, table_uuid, schema_hash, epoch, request_id, token):
        with self.backend.transaction(token) as (c, state):
            table = self.target(c, name, table_uuid, schema_hash, epoch)
            key = self.request_key(table_uuid, epoch, request_id)
            old = self.backend.request(c, key)
            require(
                c.execute("SELECT 1 FROM bound_requests WHERE request=%s", (key,)).fetchone(), "UNKNOWN_BOUND_REQUEST"
            )
            if old["receipt"] is not None:
                return old["receipt"]
            req = self.request(c, name, table_uuid, schema_hash, epoch, request_id, token)
            head = c.execute("SELECT * FROM bound_heads WHERE table_uuid=%s", (table["table_uuid"],)).fetchone()
            require(req["expected"] == head["revision"], "REVISION_CONFLICT")
            unit = "generation:" + key
            require(
                req["manifest"] is not None and all(o["sealed"] for o in self.backend.files(c, unit)),
                "UNSEALED_PUBLICATION",
            )
            require(
                c.execute("SELECT state FROM units WHERE id=%s", (unit,)).fetchone()["state"] == "allocated"
                and c.execute("SELECT 1 FROM refs WHERE kind='native' AND unit=%s", (req["source"],)).fetchone(),
                "INPUT_SNAPSHOT_CHANGED",
            )
            receipt = {
                "table_uuid": table_uuid,
                "table_id": table["table_id"],
                "request_epoch": epoch,
                "request_id": request_id,
                "payload_hash": req["payload_hash"],
                "revision": head["revision"] + 1,
                "catalog_revision": state["revision"] + 1,
            }
            c.execute(
                "UPDATE bound_heads SET revision=%s,request=%s WHERE table_uuid=%s",
                (receipt["revision"], key, table["table_uuid"]),
            )
            c.execute(
                "DELETE FROM refs WHERE (kind='named' AND holder=%s) OR (kind='build' AND holder=%s)",
                ("bound:" + table_uuid, key),
            )
            for ref in (unit, req["source"]):
                c.execute("INSERT INTO refs VALUES('named',%s,%s)", ("bound:" + table_uuid, ref))
            c.execute("UPDATE units SET state='ready' WHERE id=%s", (unit,))
            c.execute(
                "UPDATE units SET state='retired' WHERE kind='generation' AND state='ready' AND id<>%s AND id IN (SELECT unit FROM bound_units WHERE table_uuid=%s) AND NOT EXISTS(SELECT 1 FROM refs WHERE refs.unit=units.id AND kind='named')",
                (unit, table["table_uuid"]),
            )
            c.execute("UPDATE control SET revision=%s,head=%s WHERE singleton", (receipt["catalog_revision"], key))
            c.execute("UPDATE requests SET state='committed',receipt=%s WHERE id=%s", (Jsonb(receipt), key))
        return receipt

    def reconcile(self, table_uuid, epoch, request_id, payload_hash):
        req = self.status(table_uuid, epoch, request_id)
        require(req["payload_hash"] == payload_hash, "PAYLOAD_MISMATCH")
        return req["receipt"]

    def cancel(self, table_uuid, epoch, request_id, token):
        key = self.request_key(table_uuid, epoch, request_id)
        with self.backend.transaction(token) as (c, _):
            require(
                c.execute(
                    "SELECT 1 FROM bound_requests WHERE request=%s AND table_uuid=%s", (key, canonical_uuid(table_uuid))
                ).fetchone(),
                "UNKNOWN_BOUND_REQUEST",
            )
            req = self.backend.request(c, key)
            require(req["state"] == "prepared", "REQUEST_CLOSED")
            c.execute("UPDATE requests SET state='cancelled' WHERE id=%s", (key,))
            c.execute("DELETE FROM refs WHERE kind='build' AND holder=%s", (key,))

    def capture(self, name, table_uuid, schema_hash, reader_id, kind="vector", source=None):
        require(kind in ("vector", "sql"), "BAD_READER_KIND")
        with self.backend.transaction() as (c, _):
            table = self.target(c, name, table_uuid, schema_hash)
            query, _ = self.snapshot(c, table, self.metadata.catalog(c)["snapshot"])
            head = c.execute("SELECT * FROM bound_heads WHERE table_uuid=%s", (table["table_uuid"],)).fetchone()
            if kind == "vector":
                require(head["request"] is not None, "EMPTY_INDEX")
                req = self.backend.request(c, head["request"])
                units = sorted([req["source"], "generation:" + req["id"]])
                source_identity = self.unit(c, req["source"], table_uuid)["identity"]
            else:
                require(
                    source is not None
                    and c.execute("SELECT 1 FROM refs WHERE kind='native' AND unit=%s", (source,)).fetchone(),
                    "EXPIRED_NATIVE_HISTORY",
                )
                source_identity = self.unit(c, source, table_uuid)["identity"]
                units = [source]
            for unit in units:
                bound = self.unit(c, unit, table_uuid)
                require(bound["state"] == "ready", "OBJECT_RETIRING")
                c.execute("INSERT INTO refs VALUES('reader',%s,%s)", (reader_id, unit))
            identity = {
                "query": query,
                "source": source_identity,
                "root": head["request"],
                "revision": head["revision"],
                "visible_source_rows": sorted(set(query["row_ids"]) & set(source_identity["row_ids"])),
            }
            c.execute(
                "INSERT INTO readers VALUES(%s,%s,%s,%s,%s,false)",
                (reader_id, kind, head["request"], head["revision"], Jsonb(units)),
            )
            c.execute(
                "INSERT INTO bound_readers VALUES(%s,%s,%s,%s)",
                (reader_id, table["table_uuid"], query["snapshot_id"], Jsonb(identity)),
            )
        return identity

    def materialize(self, reader_id):
        with self.backend.transaction() as (c, _):
            view = c.execute(
                "SELECT r.*,b.identity,b.table_uuid,b.snapshot_id FROM readers r JOIN bound_readers b ON b.reader=r.id WHERE r.id=%s AND NOT r.closed",
                (reader_id,),
            ).fetchone()
            require(view is not None, "VIEW_CLOSED")
            refs = c.execute("SELECT unit FROM refs WHERE kind='reader' AND holder=%s", (reader_id,)).fetchall()
            require({r["unit"] for r in refs} == set(view["units"]), "VIEW_NOT_PINNED")
            table = c.execute("SELECT * FROM typed_tables WHERE table_uuid=%s", (view["table_uuid"],)).fetchone()
            query, rows = self.snapshot(c, table, view["snapshot_id"])
            require(query == view["identity"]["query"], "READVIEW_BINDING_CHANGED")
            result = {"identity": view["identity"], "rows": rows, "objects": {}}
            for unit in view["units"]:
                self.unit(c, unit, str(view["table_uuid"]))
                for obj in self.backend.files(c, unit):
                    raw = self.backend.objects.read(obj["key"], obj["version"])
                    self.backend.identity(obj, raw)
                    result["objects"][obj["key"]] = raw["data"]
            self.copies[reader_id] = result
        return result

    def release_copy_pin(self, reader_id):
        require(reader_id in self.copies, "COPY_INCOMPLETE")
        with self.backend.transaction() as (c, _):
            require(
                c.execute(
                    "SELECT 1 FROM bound_readers b JOIN readers r ON r.id=b.reader WHERE reader=%s AND NOT closed",
                    (reader_id,),
                ).fetchone(),
                "VIEW_CLOSED",
            )
            c.execute("DELETE FROM refs WHERE kind='reader' AND holder=%s", (reader_id,))

    def close_view(self, reader_id):
        with self.backend.transaction() as (c, _):
            require(
                c.execute("SELECT 1 FROM bound_readers WHERE reader=%s", (reader_id,)).fetchone(),
                "UNKNOWN_BOUND_READER",
            )
            c.execute("UPDATE readers SET closed=true WHERE id=%s", (reader_id,))
            c.execute("DELETE FROM refs WHERE kind='reader' AND holder=%s", (reader_id,))
        self.copies.pop(reader_id, None)

    def validate_gc(self, table_uuid, unit, token):
        with self.backend.transaction(token) as (c, _):
            self.unit(c, unit, table_uuid)

    def expire_native(self, table_uuid, source, token):
        self.validate_gc(table_uuid, source, token)
        return self.backend.expire_native(source, token)

    def plan_gc(self, table_uuid, unit, token, budget):
        self.validate_gc(table_uuid, unit, token)
        return self.backend.plan_gc(unit, token, budget)

    def stop(self, table_uuid, unit, key, token, budget):
        self.validate_gc(table_uuid, unit, token)
        return self.backend.stop(unit, key, token, budget)

    def delete_payload(self, table_uuid, unit, key, token, budget):
        self.validate_gc(table_uuid, unit, token)
        return self.backend.delete_payload(unit, key, token, budget)

    def finish_gc(self, table_uuid, unit, token, budget):
        self.validate_gc(table_uuid, unit, token)
        return self.backend.finish_gc(unit, token, budget)
