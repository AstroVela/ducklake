"""Experimental typed PostgreSQL metadata using the R6 broker transaction boundary.

This component has one fresh, explicit layout. It does not change DuckLake's
existing inline codec or connect native SQL to the remote publication adapter.
"""

from decimal import Decimal
import hashlib
import math
import struct
import uuid


MAX_EPOCH = 2**64 - 1
LIMITS = {
    "requests": 100000,
    "operations": 1000,
    "batch_bytes": 4 * 1024**2,
    "rows": 100000,
    "vector_bytes": 64 * 1024**2,
    "dimension": 4096,
}
FIELDS = (
    ("id", "BIGINT", False),
    ("embedding", "FLOAT[]", False),
    ("category", "VARCHAR", True),
    ("payload", "VARCHAR", True),
    ("f32", "FLOAT", True),
    ("f64", "DOUBLE", True),
    ("data", "BLOB", True),
    ("numbers", "FLOAT[]", True),
)


class MetadataError(Exception):
    pass


def require(value, code):
    if not value:
        raise MetadataError(code)


def canonical_uuid(value):
    try:
        parsed = uuid.UUID(value)
    except (ValueError, TypeError, AttributeError):
        raise MetadataError("BAD_UUID") from None
    require(str(parsed) == value, "BAD_UUID")
    return parsed


def integer(value, low, high, code):
    require(type(value) is int and low <= value <= high, code)
    return value


def text(value):
    require(type(value) is str, "BAD_TEXT")
    try:
        return value.encode("utf-8", "strict")
    except UnicodeEncodeError:
        raise MetadataError("BAD_UTF8") from None


def floating(value, width):
    require(type(value) in (int, float), "BAD_FLOAT")
    try:
        value = float(value)
        require(math.isfinite(value), "NONFINITE_FLOAT")
        result = struct.pack("!f" if width == 4 else "!d", value)
    except (OverflowError, struct.error):
        raise MetadataError("FLOAT_RANGE") from None
    return struct.unpack("!f" if width == 4 else "!d", result)[0]


def array(value, nullable):
    require(type(value) in (list, tuple), "BAD_LIST")
    return [None if x is None and nullable else floating(x, 4) for x in value]


def normalized(row, dimension):
    require(type(row) is dict and set(row) == {f[0] for f in FIELDS}, "FIELD_SET_MISMATCH")
    r = dict(row)
    r["id"] = integer(r["id"], -(2**63), 2**63 - 1, "BAD_ID")
    r["embedding"] = array(r["embedding"], False)
    require(len(r["embedding"]) == dimension, "DIMENSION_MISMATCH")
    for name in ("category", "payload"):
        r[name] = None if r[name] is None else text(r[name])
    for name, width in (("f32", 4), ("f64", 8)):
        r[name] = None if r[name] is None else floating(r[name], width)
    require(r["data"] is None or type(r["data"]) is bytes, "BAD_BLOB")
    r["numbers"] = None if r["numbers"] is None else array(r["numbers"], True)
    return r


def encoded_row(row):
    result = bytearray(struct.pack("!q", row["id"]))
    for name in ("embedding", "category", "payload", "f32", "f64", "data", "numbers"):
        value = row[name]
        if value is None:
            result += b"\0"
            continue
        result += b"\1"
        if name in ("embedding", "numbers"):
            result += struct.pack("!I", len(value))
            for item in value:
                result += b"\0" if item is None else b"\1" + struct.pack("!f", item)
        elif name in ("f32", "f64"):
            result += struct.pack("!f" if name == "f32" else "!d", value)
        else:
            result += struct.pack("!I", len(value)) + value
    return bytes(result)


DDL = f"""
CREATE TABLE typed_catalog(singleton boolean PRIMARY KEY CHECK(singleton),layout integer NOT NULL CHECK(layout=2),
 snapshot bigint NOT NULL CHECK(snapshot>=0),next_table_id bigint NOT NULL,next_row_id bigint NOT NULL);
INSERT INTO typed_catalog VALUES(true,2,0,1,1);
CREATE TABLE typed_tables(table_uuid uuid PRIMARY KEY,table_id bigint NOT NULL UNIQUE,name bytea NOT NULL,
 dimension integer NOT NULL CHECK(dimension BETWEEN 1 AND 4096),schema_hash bytea NOT NULL CHECK(octet_length(schema_hash)=32),
 begin_snapshot bigint NOT NULL,end_snapshot bigint,request_epoch numeric(20,0),last_previous numeric(20,0),
 last_expired bigint,managed boolean NOT NULL,
 CHECK((end_snapshot IS NULL AND managed AND request_epoch IS NOT NULL AND request_epoch BETWEEN 1 AND {MAX_EPOCH}) OR
       (end_snapshot IS NOT NULL AND NOT managed AND request_epoch IS NULL AND last_previous IS NULL AND last_expired IS NULL)),
 CHECK(end_snapshot IS NULL OR end_snapshot>begin_snapshot),
 CHECK((last_previous IS NULL AND last_expired IS NULL) OR
       (last_previous IS NOT NULL AND last_expired IS NOT NULL AND last_previous BETWEEN 1 AND {MAX_EPOCH-1}
        AND request_epoch=last_previous+1 AND last_expired>=0)),UNIQUE(table_uuid,dimension),
 UNIQUE(table_uuid,table_id,schema_hash,dimension));
CREATE UNIQUE INDEX typed_current_name ON typed_tables(name) WHERE end_snapshot IS NULL;
CREATE TABLE typed_fields(table_uuid uuid REFERENCES typed_tables(table_uuid),field_id bigint NOT NULL CHECK(field_id>0),
 ordinal integer NOT NULL,name bytea NOT NULL,logical_type text NOT NULL,nullable boolean NOT NULL,
 PRIMARY KEY(table_uuid,field_id),UNIQUE(table_uuid,ordinal),UNIQUE(table_uuid,name));
CREATE FUNCTION typed_finite(values_ real[]) RETURNS boolean LANGUAGE sql IMMUTABLE AS $$
 SELECT NOT EXISTS(SELECT 1 FROM unnest(values_) v WHERE v IN ('NaN'::real,'Infinity'::real,'-Infinity'::real)) $$;
CREATE TABLE typed_rows(table_uuid uuid NOT NULL,id bigint NOT NULL,physical_row_id bigint NOT NULL UNIQUE,
 dimension integer NOT NULL,begin_snapshot bigint NOT NULL,end_snapshot bigint,
 embedding real[] NOT NULL,category bytea,payload bytea,f32 real,f64 double precision,data bytea,numbers real[],
 PRIMARY KEY(table_uuid,id,begin_snapshot),FOREIGN KEY(table_uuid,dimension) REFERENCES typed_tables(table_uuid,dimension),
 CHECK(end_snapshot IS NULL OR end_snapshot>begin_snapshot),
 CHECK(cardinality(embedding)=dimension AND array_ndims(embedding)=1 AND array_lower(embedding,1)=1 AND
       array_position(embedding,NULL) IS NULL AND typed_finite(embedding)),
 CHECK(f32 IS NULL OR f32 NOT IN ('NaN'::real,'Infinity'::real,'-Infinity'::real)),
 CHECK(f64 IS NULL OR f64 NOT IN ('NaN'::float8,'Infinity'::float8,'-Infinity'::float8)),
 CHECK(numbers IS NULL OR (typed_finite(numbers) AND
       (cardinality(numbers)=0 OR (array_ndims(numbers)=1 AND array_lower(numbers,1)=1)))));
CREATE UNIQUE INDEX typed_current_row ON typed_rows(table_uuid,id) WHERE end_snapshot IS NULL;
CREATE TABLE typed_receipts(table_uuid uuid REFERENCES typed_tables(table_uuid),request_epoch numeric(20,0) NOT NULL
 CHECK(request_epoch BETWEEN 1 AND {MAX_EPOCH}),request_id bytea NOT NULL CHECK(octet_length(request_id) BETWEEN 1 AND 128),
 payload_hash bytea NOT NULL CHECK(octet_length(payload_hash)=32),row_count bigint NOT NULL CHECK(row_count BETWEEN 1 AND 1000),
 snapshot_id bigint NOT NULL,PRIMARY KEY(table_uuid,request_epoch,request_id));
CREATE TABLE bound_units(unit text PRIMARY KEY,table_uuid uuid NOT NULL,
 table_id bigint NOT NULL,schema_hash bytea NOT NULL,dimension integer NOT NULL,identity jsonb NOT NULL,
 FOREIGN KEY(table_uuid,table_id,schema_hash,dimension) REFERENCES typed_tables(table_uuid,table_id,schema_hash,dimension),
 FOREIGN KEY(unit,table_uuid) REFERENCES units(id,table_uuid));
CREATE TABLE bound_requests(request text PRIMARY KEY REFERENCES requests(id),table_uuid uuid NOT NULL REFERENCES typed_tables(table_uuid),
 request_epoch numeric(20,0) NOT NULL CHECK(request_epoch BETWEEN 1 AND {MAX_EPOCH}),request_id text NOT NULL,
 UNIQUE(table_uuid,request_epoch,request_id));
CREATE TABLE bound_heads(table_uuid uuid PRIMARY KEY REFERENCES typed_tables(table_uuid),revision bigint NOT NULL,
 request text REFERENCES requests(id));
CREATE TABLE bound_readers(reader text PRIMARY KEY REFERENCES readers(id),table_uuid uuid NOT NULL REFERENCES typed_tables(table_uuid),
 snapshot_id bigint NOT NULL,identity jsonb NOT NULL);
"""


class TypedMetadata:
    def __init__(self, backend):
        self.backend = backend

    def initialize(self, token):
        with self.backend.transaction(token) as (c, _):
            c.execute(DDL)

    @staticmethod
    def catalog(c):
        state = c.execute("SELECT * FROM typed_catalog WHERE singleton").fetchone()
        require(state and state["layout"] == 2, "UNSUPPORTED_TYPED_LAYOUT")
        return state

    @staticmethod
    def target(c, name, expected_uuid):
        parsed = canonical_uuid(expected_uuid)
        table = c.execute("SELECT * FROM typed_tables WHERE name=%s AND end_snapshot IS NULL", (text(name),)).fetchone()
        require(table and table["table_uuid"] == parsed, "STALE_TABLE")
        return table

    @staticmethod
    def receipt(row):
        return {
            "table_uuid": str(row["table_uuid"]),
            "request_epoch": int(row["request_epoch"]),
            "request_id": bytes(row["request_id"]).decode("utf-8"),
            "payload_hash": bytes(row["payload_hash"]).hex(),
            "row_count": row["row_count"],
            "snapshot_id": row["snapshot_id"],
        }

    def create(self, name, table_uuid, dimension, token, field_ids=tuple(range(1, 9))):
        parsed = canonical_uuid(table_uuid)
        raw_name = text(name)
        require(raw_name and len(raw_name) <= 63 and b"\0" not in raw_name, "BAD_TABLE_NAME")
        integer(dimension, 1, LIMITS["dimension"], "DIMENSION_MISMATCH")
        require(len(field_ids) == len(FIELDS) and len(set(field_ids)) == len(field_ids), "BAD_FIELD_IDS")
        for field_id in field_ids:
            integer(field_id, 1, 2**63 - 1, "BAD_FIELD_IDS")
        descriptor = b"".join(
            struct.pack("!qI", n, i) + f[0].encode() + b"\0" + f[1].encode() + bytes([f[2]])
            for i, (n, f) in enumerate(zip(field_ids, FIELDS))
        ) + struct.pack("!I", dimension)
        schema_hash = hashlib.sha256(descriptor).digest()
        with self.backend.transaction(token) as (c, _):
            state = self.catalog(c)
            require(
                not c.execute("SELECT 1 FROM typed_tables WHERE table_uuid=%s", (parsed,)).fetchone(),
                "UUID_ALREADY_REGISTERED",
            )
            require(
                not c.execute(
                    "SELECT 1 FROM typed_tables WHERE name=%s AND end_snapshot IS NULL", (raw_name,)
                ).fetchone(),
                "TABLE_EXISTS",
            )
            snapshot = state["snapshot"] + 1
            c.execute(
                "INSERT INTO typed_tables VALUES(%s,%s,%s,%s,%s,%s,NULL,1,NULL,NULL,true)",
                (parsed, state["next_table_id"], raw_name, dimension, schema_hash, snapshot),
            )
            for ordinal, (field_id, f) in enumerate(zip(field_ids, FIELDS)):
                c.execute(
                    "INSERT INTO typed_fields VALUES(%s,%s,%s,%s,%s,%s)",
                    (parsed, field_id, ordinal, f[0].encode(), f[1], f[2]),
                )
            c.execute("INSERT INTO bound_heads VALUES(%s,0,NULL)", (parsed,))
            c.execute("UPDATE typed_catalog SET snapshot=%s,next_table_id=next_table_id+1 WHERE singleton", (snapshot,))
        return {
            "table_uuid": table_uuid,
            "table_id": state["next_table_id"],
            "request_epoch": 1,
            "schema_hash": schema_hash.hex(),
            "snapshot_id": snapshot,
        }

    def status(self, name, table_uuid):
        with self.backend.transaction() as (c, _):
            table = self.target(c, name, table_uuid)
            count = c.execute(
                "SELECT count(*) AS n FROM typed_receipts WHERE table_uuid=%s", (table["table_uuid"],)
            ).fetchone()["n"]
            total = c.execute("SELECT count(*) AS n FROM typed_receipts").fetchone()["n"]
            return {
                "table_uuid": table_uuid,
                "request_epoch": int(table["request_epoch"]),
                "retained_requests": count,
                "catalog_requests": total,
            }

    def mutate(self, name, table_uuid, epoch, request_id, rows, deletes, schema_hash, token):
        integer(epoch, 1, MAX_EPOCH, "BAD_EPOCH")
        request_key = text(request_id)
        require(1 <= len(request_key) <= 128 and b"\0" not in request_key, "BAD_REQUEST_ID")
        require(type(rows) in (list, tuple) and type(deletes) in (list, tuple), "BAD_BATCH")
        require(1 <= len(rows) + len(deletes) <= LIMITS["operations"], "OPERATION_BUDGET")
        with self.backend.transaction(token) as (c, _):
            state = self.catalog(c)
            table = self.target(c, name, table_uuid)
            current = int(table["request_epoch"])
            require(epoch >= current, "EXPIRED_REQUEST")
            require(epoch == current, "FUTURE_REQUEST")
            require(schema_hash == bytes(table["schema_hash"]).hex(), "STALE_SCHEMA")
            values = sorted([normalized(row, table["dimension"]) for row in rows], key=lambda r: r["id"])
            removed = sorted(integer(v, -(2**63), 2**63 - 1, "BAD_ID") for v in deletes)
            ids = [r["id"] for r in values]
            require(len(set(ids + removed)) == len(ids) + len(removed), "DUPLICATE_ID")
            data = (
                b"typed-mutation-v1\0"
                + bytes(table["schema_hash"])
                + struct.pack("!II", len(values), len(removed))
                + b"".join(encoded_row(r) for r in values)
                + b"".join(struct.pack("!q", v) for v in removed)
            )
            require(len(data) <= LIMITS["batch_bytes"], "BATCH_BYTE_BUDGET")
            payload_hash = hashlib.sha256(data).digest()
            previous = c.execute(
                "SELECT * FROM typed_receipts WHERE table_uuid=%s AND request_epoch=%s AND request_id=%s",
                (table["table_uuid"], Decimal(epoch), request_key),
            ).fetchone()
            if previous is not None:
                require(bytes(previous["payload_hash"]) == payload_hash, "PAYLOAD_MISMATCH")
                return self.receipt(previous)
            require(
                c.execute("SELECT count(*) AS n FROM typed_receipts").fetchone()["n"] < LIMITS["requests"],
                "REQUEST_BUDGET",
            )
            keys = ids + removed
            existing = c.execute(
                "SELECT id FROM typed_rows WHERE table_uuid=%s AND end_snapshot IS NULL AND id=ANY(%s::bigint[])",
                (table["table_uuid"], keys),
            ).fetchall()
            snapshot = state["snapshot"] + bool(values or existing)
            if values or existing:
                c.execute(
                    "UPDATE typed_rows SET end_snapshot=%s WHERE table_uuid=%s AND end_snapshot IS NULL AND id=ANY(%s::bigint[])",
                    (snapshot, table["table_uuid"], keys),
                )
                for i, row in enumerate(values):
                    c.execute(
                        "INSERT INTO typed_rows VALUES(%s,%s,%s,%s,%s,NULL,%s::real[],%s,%s,%s::real,%s::double precision,%s,%s::real[])",
                        (
                            table["table_uuid"],
                            row["id"],
                            state["next_row_id"] + i,
                            table["dimension"],
                            snapshot,
                            row["embedding"],
                            row["category"],
                            row["payload"],
                            row["f32"],
                            row["f64"],
                            row["data"],
                            row["numbers"],
                        ),
                    )
                capacity = c.execute(
                    "SELECT count(*) AS n,coalesce(sum(cardinality(embedding)*4),0) AS bytes FROM typed_rows WHERE table_uuid=%s AND end_snapshot IS NULL",
                    (table["table_uuid"],),
                ).fetchone()
                require(
                    capacity["n"] <= LIMITS["rows"] and capacity["bytes"] <= LIMITS["vector_bytes"], "ROW_VECTOR_BUDGET"
                )
                c.execute(
                    "UPDATE typed_catalog SET snapshot=%s,next_row_id=next_row_id+%s WHERE singleton",
                    (snapshot, len(values)),
                )
            c.execute(
                "INSERT INTO typed_receipts VALUES(%s,%s,%s,%s,%s,%s)",
                (table["table_uuid"], Decimal(epoch), request_key, payload_hash, len(rows) + len(deletes), snapshot),
            )
            return {
                "table_uuid": table_uuid,
                "request_epoch": epoch,
                "request_id": request_id,
                "payload_hash": payload_hash.hex(),
                "row_count": len(rows) + len(deletes),
                "snapshot_id": snapshot,
            }

    def advance(self, name, table_uuid, expected, token):
        integer(expected, 1, MAX_EPOCH, "BAD_EPOCH")
        with self.backend.transaction(token) as (c, _):
            self.catalog(c)
            table = self.target(c, name, table_uuid)
            current = int(table["request_epoch"])
            if (
                table["last_previous"] is not None
                and int(table["last_previous"]) == expected
                and current == expected + 1
            ):
                return {
                    "table_uuid": table_uuid,
                    "previous_epoch": expected,
                    "request_epoch": current,
                    "expired_requests": table["last_expired"],
                }
            require(current == expected, "EPOCH_CONFLICT")
            require(current < MAX_EPOCH, "RESOURCE_EXHAUSTED")
            count = c.execute(
                "DELETE FROM typed_receipts WHERE table_uuid=%s AND request_epoch=%s RETURNING request_id",
                (table["table_uuid"], Decimal(expected)),
            ).rowcount
            c.execute(
                "UPDATE typed_tables SET request_epoch=%s,last_previous=%s,last_expired=%s WHERE table_uuid=%s",
                (Decimal(expected + 1), Decimal(expected), count, table["table_uuid"]),
            )
            return {
                "table_uuid": table_uuid,
                "previous_epoch": expected,
                "request_epoch": expected + 1,
                "expired_requests": count,
            }

    def retire(self, name, table_uuid, token):
        parsed = canonical_uuid(table_uuid)
        with self.backend.transaction(token) as (c, _):
            state = self.catalog(c)
            live = c.execute(
                "SELECT * FROM typed_tables WHERE name=%s AND end_snapshot IS NULL", (text(name),)
            ).fetchone()
            if live is None:
                old = c.execute(
                    "SELECT * FROM typed_tables WHERE table_uuid=%s AND name=%s AND end_snapshot IS NOT NULL",
                    (parsed, text(name)),
                ).fetchone()
                require(old is not None, "UNKNOWN_RETIREMENT_UUID")
                return {"table_uuid": table_uuid, "retired": True, "snapshot_id": old["end_snapshot"]}
            require(live["table_uuid"] == parsed, "STALE_TABLE")
            require(
                not c.execute(
                    "SELECT 1 FROM bound_units b JOIN units u ON u.id=b.unit WHERE b.table_uuid=%s AND u.state IN ('allocated','deleting')",
                    (parsed,),
                ).fetchone(),
                "BUILD_IN_PROGRESS",
            )
            c.execute("DELETE FROM refs WHERE kind='named' AND holder=%s", ("bound:" + table_uuid,))
            c.execute("UPDATE bound_heads SET request=NULL WHERE table_uuid=%s", (parsed,))
            c.execute(
                "UPDATE units SET state='retired' WHERE kind='generation' AND state='ready' AND id IN (SELECT unit FROM bound_units WHERE table_uuid=%s) AND NOT EXISTS(SELECT 1 FROM refs WHERE refs.unit=units.id AND kind='named')",
                (parsed,),
            )
            snapshot = state["snapshot"] + 1
            c.execute("DELETE FROM typed_receipts WHERE table_uuid=%s", (parsed,))
            c.execute(
                "UPDATE typed_tables SET end_snapshot=%s,managed=false,request_epoch=NULL,last_previous=NULL,last_expired=NULL WHERE table_uuid=%s",
                (snapshot, parsed),
            )
            c.execute(
                "UPDATE typed_rows SET end_snapshot=%s WHERE table_uuid=%s AND end_snapshot IS NULL", (snapshot, parsed)
            )
            c.execute("UPDATE typed_catalog SET snapshot=%s WHERE singleton", (snapshot,))
            return {"table_uuid": table_uuid, "retired": True, "snapshot_id": snapshot}

    def read(self, name, table_uuid):
        with self.backend.transaction() as (c, _):
            table = self.target(c, name, table_uuid)
            return self._rows(c, table["table_uuid"], None)

    def read_at(self, table_uuid, snapshot):
        parsed = canonical_uuid(table_uuid)
        integer(snapshot, 0, 2**63 - 1, "BAD_SNAPSHOT")
        with self.backend.transaction() as (c, _):
            state = self.catalog(c)
            require(snapshot <= state["snapshot"], "FUTURE_SNAPSHOT")
            table = c.execute(
                "SELECT 1 FROM typed_tables WHERE table_uuid=%s AND begin_snapshot<=%s AND (end_snapshot IS NULL OR end_snapshot>%s)",
                (parsed, snapshot, snapshot),
            ).fetchone()
            require(table is not None, "TABLE_NOT_IN_SNAPSHOT")
            return self._rows(c, parsed, snapshot)

    @staticmethod
    def _rows(c, table_uuid, snapshot):
        condition = (
            "end_snapshot IS NULL"
            if snapshot is None
            else "begin_snapshot<=%s AND (end_snapshot IS NULL OR end_snapshot>%s)"
        )
        params = (table_uuid,) if snapshot is None else (table_uuid, snapshot, snapshot)
        with c.cursor(binary=True) as cursor:
            rows = cursor.execute(
                "SELECT id,embedding,category,payload,f32,f64,data,numbers,physical_row_id FROM typed_rows WHERE table_uuid=%s AND "
                + condition
                + " ORDER BY id",
                params,
            ).fetchall()
        for row in rows:
            for name in ("category", "payload"):
                if row[name] is not None:
                    row[name] = bytes(row[name]).decode("utf-8", "strict")
        return rows
