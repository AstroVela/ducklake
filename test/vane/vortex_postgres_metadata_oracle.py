"""Independent typed-row / UUID / epoch history model and raw PostgreSQL view."""

from copy import deepcopy
import hashlib
import json
import struct
import uuid


FIELDS = ("id", "embedding", "category", "payload", "f32", "f64", "data", "numbers")


def require(value, message):
    if not value:
        raise AssertionError(message)


def bits(value, width):
    return None if value is None else struct.pack(">f" if width == 4 else ">d", value).hex()


def raw(row):
    result = {"id": row["id"]}
    result["embedding"] = [bits(v, 4) for v in row["embedding"]]
    for name in ("category", "payload"):
        result[name] = None if row[name] is None else row[name].encode("utf-8").hex()
    result["f32"], result["f64"] = bits(row["f32"], 4), bits(row["f64"], 8)
    result["data"] = (
        None if row["data"] is None else {"n": len(row["data"]), "sha256": hashlib.sha256(row["data"]).hexdigest()}
    )
    result["numbers"] = None if row["numbers"] is None else [bits(v, 4) for v in row["numbers"]]
    return result


def schema_digest(dimension, ids):
    types = ("BIGINT", "FLOAT[]", "VARCHAR", "VARCHAR", "FLOAT", "DOUBLE", "BLOB", "FLOAT[]")
    encoded = bytearray()
    for order, (fid, name, logical) in enumerate(zip(ids, FIELDS, types)):
        encoded.extend(fid.to_bytes(8, "big", signed=True))
        encoded.extend(order.to_bytes(4, "big"))
        encoded.extend(name.encode() + b"\x00" + logical.encode() + bytes([order >= 2]))
    encoded.extend(dimension.to_bytes(4, "big"))
    return hashlib.sha256(encoded).hexdigest()


def request_digest(rows, deleted, schema):
    """Independent byte grammar using captured IEEE words, never the adapter codec."""
    data = bytearray(b"typed-mutation-v1\0" + bytes.fromhex(schema))
    data += len(rows).to_bytes(4, "big") + len(deleted).to_bytes(4, "big")
    for original in sorted(rows, key=lambda r: r["id"]):
        row = raw(original)
        data += row["id"].to_bytes(8, "big", signed=True)
        for name in FIELDS[1:]:
            value = row[name]
            data += bytes([value is not None])
            if value is None:
                continue
            if name in ("embedding", "numbers"):
                data += len(value).to_bytes(4, "big")
                for word in value:
                    data += bytes([word is not None])
                    if word is not None:
                        data += bytes.fromhex(word)
            elif name in ("f32", "f64"):
                data += bytes.fromhex(value)
            else:
                blob = original["data"] if name == "data" else bytes.fromhex(value)
                data += len(blob).to_bytes(4, "big") + blob
    for key in sorted(deleted):
        data += key.to_bytes(8, "big", signed=True)
    return hashlib.sha256(data).hexdigest(), len(data)


def receipt_summary(receipts):
    ordered = sorted(
        receipts.values(), key=lambda r: (uuid.UUID(r["table_uuid"]).int, r["request_epoch"], r["request_id"].encode())
    )
    lines = [
        "\x1f".join(
            [
                r["table_uuid"],
                str(r["request_epoch"]),
                r["request_id"].encode().hex(),
                r["payload_hash"],
                str(r["row_count"]),
                str(r["snapshot_id"]),
            ]
        )
        for r in ordered
    ]
    return {"n": len(ordered), "sha": hashlib.sha256("\n".join(lines).encode()).hexdigest()}


class Model:
    def __init__(self):
        self.snapshot, self.next_table, self.next_row = 0, 1, 1
        self.tables, self.rows, self.receipts = {}, [], {}

    def create(self, name, table_uuid, dimension, ids=tuple(range(1, 9))):
        self.snapshot += 1
        self.tables[table_uuid] = {
            "table_uuid": table_uuid,
            "table_id": self.next_table,
            "name": name.encode().hex(),
            "dimension": dimension,
            "schema_hash": schema_digest(dimension, ids),
            "begin_snapshot": self.snapshot,
            "end_snapshot": None,
            "request_epoch": 1,
            "last_previous": None,
            "last_expired": None,
            "managed": True,
            "field_ids": list(ids),
        }
        self.next_table += 1

    def mutate(self, table_uuid, epoch, name, rows, deletes):
        identity = (table_uuid, epoch, name)
        schema = self.tables[table_uuid]["schema_hash"]
        digest, _ = request_digest(rows, deletes, schema)
        if identity in self.receipts:
            require(self.receipts[identity]["payload_hash"] == digest, "Reference request changed")
            return self.receipts[identity]
        keys = {r["id"] for r in rows} | set(deletes)
        changed = bool(rows) or any(
            r["table_uuid"] == table_uuid and r["end_snapshot"] is None and r["id"] in keys for r in self.rows
        )
        if changed:
            self.snapshot += 1
            for row in self.rows:
                if row["table_uuid"] == table_uuid and row["end_snapshot"] is None and row["id"] in keys:
                    row["end_snapshot"] = self.snapshot
            for original in sorted(rows, key=lambda r: r["id"]):
                row = raw(original)
                row.update(
                    table_uuid=table_uuid,
                    physical_row_id=self.next_row,
                    dimension=self.tables[table_uuid]["dimension"],
                    begin_snapshot=self.snapshot,
                    end_snapshot=None,
                )
                self.rows.append(row)
                self.next_row += 1
        receipt = {
            "table_uuid": table_uuid,
            "request_epoch": epoch,
            "request_id": name,
            "payload_hash": digest,
            "row_count": len(rows) + len(deletes),
            "snapshot_id": self.snapshot,
        }
        self.receipts[identity] = receipt
        return receipt

    def advance(self, table_uuid, previous):
        table = self.tables[table_uuid]
        if table["last_previous"] == previous and table["request_epoch"] == previous + 1:
            return {
                "table_uuid": table_uuid,
                "previous_epoch": previous,
                "request_epoch": previous + 1,
                "expired_requests": table["last_expired"],
            }
        removed = [k for k in self.receipts if k[:2] == (table_uuid, previous)]
        for key in removed:
            del self.receipts[key]
        table.update(request_epoch=previous + 1, last_previous=previous, last_expired=len(removed))
        return {
            "table_uuid": table_uuid,
            "previous_epoch": previous,
            "request_epoch": previous + 1,
            "expired_requests": len(removed),
        }

    def retire(self, table_uuid):
        table = self.tables[table_uuid]
        if table["end_snapshot"] is None:
            self.snapshot += 1
            table.update(
                end_snapshot=self.snapshot, request_epoch=None, last_previous=None, last_expired=None, managed=False
            )
            self.receipts = {k: v for k, v in self.receipts.items() if k[0] != table_uuid}
            for row in self.rows:
                if row["table_uuid"] == table_uuid and row["end_snapshot"] is None:
                    row["end_snapshot"] = self.snapshot
        return {"table_uuid": table_uuid, "retired": True, "snapshot_id": table["end_snapshot"]}


def observe(backend):
    with backend.connect() as c:
        c.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        state = c.execute("SELECT snapshot,next_table_id,next_row_id,layout FROM typed_catalog").fetchone()
        tables = c.execute(
            """SELECT table_uuid::text,table_id,encode(name,'hex') AS name,dimension,encode(schema_hash,'hex') AS schema_hash,
                            begin_snapshot,end_snapshot,request_epoch::text,last_previous::text,last_expired,managed FROM typed_tables ORDER BY table_id"""
        ).fetchall()
        fields = c.execute(
            "SELECT table_uuid::text,field_id,ordinal,encode(name,'hex') AS name,logical_type,nullable FROM typed_fields ORDER BY table_uuid,ordinal"
        ).fetchall()
        rows = c.execute(
            """SELECT table_uuid::text,id,physical_row_id,dimension,begin_snapshot,end_snapshot,
            ARRAY(SELECT encode(float4send(v),'hex') FROM unnest(embedding) v) AS embedding,
            encode(category,'hex') AS category,encode(payload,'hex') AS payload,encode(float4send(f32),'hex') AS f32,
            encode(float8send(f64),'hex') AS f64,CASE WHEN data IS NULL THEN NULL ELSE
            json_build_object('n',octet_length(data),'sha256',encode(sha256(data),'hex')) END AS data,
            CASE WHEN numbers IS NULL THEN NULL ELSE ARRAY(SELECT encode(float4send(v),'hex') FROM unnest(numbers) v) END AS numbers
            FROM typed_rows ORDER BY physical_row_id"""
        ).fetchall()
        receipts = c.execute(
            """SELECT count(*) AS n,encode(sha256(convert_to(coalesce(string_agg(
            table_uuid::text||chr(31)||request_epoch::text||chr(31)||encode(request_id,'hex')||chr(31)||
            encode(payload_hash,'hex')||chr(31)||row_count::text||chr(31)||snapshot_id::text,chr(10)
            ORDER BY table_uuid,request_epoch,request_id),''),'UTF8')),'hex') AS sha FROM typed_receipts"""
        ).fetchone()
    for row in tables:
        for key in ("request_epoch", "last_previous"):
            row[key] = None if row[key] is None else int(row[key])
    return {"state": state, "tables": tables, "fields": fields, "rows": rows, "receipts": receipts}


def inspect(image, model):
    require(
        image["state"]
        == {"snapshot": model.snapshot, "next_table_id": model.next_table, "next_row_id": model.next_row, "layout": 3},
        "Typed snapshot / allocation mismatch",
    )
    expected = []
    for table in sorted(model.tables.values(), key=lambda t: t["table_id"]):
        expected.append({k: v for k, v in table.items() if k != "field_ids"})
    require(image["tables"] == expected, "UUID / epoch / table identity mismatch")
    require(image["rows"] == model.rows, "Typed IEEE / NULL / BYTEA / physical-version mismatch")
    require(image["receipts"] == receipt_summary(model.receipts), "Typed receipt set differs")
    for table in model.tables.values():
        actual = [f for f in image["fields"] if f["table_uuid"] == table["table_uuid"]]
        require([f["field_id"] for f in actual] == table["field_ids"], "Field IDs changed")
        require([bytes.fromhex(f["name"]).decode() for f in actual] == list(FIELDS), "Field order/name mismatch")
        require(
            [f["logical_type"] for f in actual]
            == ["BIGINT", "FLOAT[]", "VARCHAR", "VARCHAR", "FLOAT", "DOUBLE", "BLOB", "FLOAT[]"],
            "Field type mismatch",
        )
        require(
            [f["nullable"] for f in actual] == [False, False, True, True, True, True, True, True],
            "Field nullability mismatch",
        )
