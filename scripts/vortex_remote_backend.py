"""Experimental R6 PostgreSQL/S3 adapter, independent of the native SQL entry points.

The broker holds the S3 signing credentials. Workers receive only a SigV4 ticket
for one registered key, checksum and condition (or one exact DELETE version).
No operation retries, schema migration, local storage or alternate backend.
"""

from contextlib import contextmanager
from dataclasses import dataclass, field
import base64
import hashlib
import http.client
import json
import re
import ssl
from urllib.parse import parse_qs, urlsplit
import uuid

from botocore.exceptions import ClientError
import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb


MIB = 1024 * 1024
LIMITS = {
    "source": 64 * MIB,
    "artifact": 64 * MIB,
    "manifest": MIB,
    "vector": 64 * MIB,
    "rows": 100000,
    "segments": 3,
    "gc_objects": 16,
    "gc_bytes": 64 * MIB,
}


class ContractError(Exception):
    pass


class CommitUnknown(Exception):
    """COMMIT was sent but its result could not be received; reconcile explicitly."""


def require(value, code):
    if not value:
        raise ContractError(code)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def sha(data):
    return hashlib.sha256(data).hexdigest()


@dataclass(frozen=True)
class Ticket:
    method: str
    key: str
    url: str = field(repr=False)
    headers: tuple = field(repr=False)


def dispatch(ticket, body=None):
    """One HTTP attempt. A disconnected response never triggers an implicit retry."""
    parsed = urlsplit(ticket.url)
    require(parsed.scheme == "https" or (parsed.scheme == "http" and parsed.hostname == "127.0.0.1"), "HTTPS_REQUIRED")
    if parsed.scheme == "https":
        connection = http.client.HTTPSConnection(
            parsed.hostname, parsed.port, timeout=30, context=ssl.create_default_context()
        )
    else:
        connection = http.client.HTTPConnection(parsed.hostname, parsed.port, timeout=30)
    try:
        connection.request(ticket.method, parsed.path + "?" + parsed.query, body=body, headers=dict(ticket.headers))
        response = connection.getresponse()
        data, headers = response.read(MIB + 1), dict(response.getheaders())
        require(len(data) <= MIB, "HTTP_METADATA_BUDGET")
        require(response.status in (200, 204), f"HTTP_{response.status}")
        return {k.lower(): v for k, v in headers.items()}
    finally:
        connection.close()


class S3Objects:
    def __init__(self, client, bucket, prefix):
        endpoint = urlsplit(client.meta.endpoint_url)
        require(
            endpoint.scheme == "https" or (endpoint.scheme == "http" and endpoint.hostname == "127.0.0.1"),
            "HTTPS_REQUIRED",
        )
        require(re.fullmatch(r"[a-z0-9/-]+/", prefix), "BAD_PREFIX")
        require(client.meta.config.retries.get("total_max_attempts") == 1, "SDK_RETRIES_MUST_BE_DISABLED")
        require(client.meta.config.signature_version == "s3v4", "SIGV4_REQUIRED")
        self.client, self.bucket, self.prefix = client, bucket, prefix

    def profile(self):
        require(self.client.get_bucket_versioning(Bucket=self.bucket).get("Status") == "Enabled", "VERSIONING_REQUIRED")
        try:
            rules = self.client.get_bucket_lifecycle_configuration(Bucket=self.bucket).get("Rules", [])
        except ClientError as error:
            if error.response["Error"]["Code"] != "NoSuchLifecycleConfiguration":
                raise
            rules = []
        require(not any(r["Status"] == "Enabled" for r in rules), "LIFECYCLE_MUST_NOT_REMOVE_BARRIERS")

    def key(self, key):
        require(key.startswith(self.prefix) and ".." not in key and "//" not in key, "KEY_NOT_OWNED")

    def ticket(self, key, *, data=None, condition=None, version=None):
        self.key(key)
        params, headers = {"Bucket": self.bucket, "Key": key}, {}
        if data is not None:
            require(condition is not None and version is None, "CONDITIONAL_PUT_REQUIRED")
            checksum = base64.b64encode(hashlib.sha256(data).digest()).decode()
            params.update(ContentLength=len(data), ChecksumSHA256=checksum)
            headers.update({"Content-Length": str(len(data)), "x-amz-checksum-sha256": checksum})
            name = "IfNoneMatch" if condition == "*" else "IfMatch"
            params[name] = condition
            headers["If-None-Match" if condition == "*" else "If-Match"] = condition
            operation, method = "put_object", "PUT"
        else:
            require(version and version != "null", "EXACT_VERSION_REQUIRED")
            params["VersionId"] = version
            operation, method = "delete_object", "DELETE"
        url = self.client.generate_presigned_url(operation, Params=params, ExpiresIn=300)
        query = parse_qs(urlsplit(url).query)
        signed = set(query.get("X-Amz-SignedHeaders", [""])[0].split(";"))
        require({"host", *[k.lower() for k in headers]} <= signed, "REQUIRED_HEADERS_NOT_SIGNED")
        require(version is None or query.get("versionId") == [version], "DELETE_VERSION_NOT_BOUND")
        return Ticket(method, key, url, tuple(sorted(headers.items())))

    def read(self, key, version=None):
        self.key(key)
        params = {"Bucket": self.bucket, "Key": key}
        if version is not None:
            require(version != "null", "EXACT_VERSION_REQUIRED")
            params["VersionId"] = version
        try:
            result = self.client.get_object(**params)
        except ClientError as error:
            if error.response["Error"]["Code"] in ("NoSuchKey", "NoSuchVersion"):
                return None
            raise
        stream = result["Body"]
        try:
            require(result.get("VersionId") not in (None, "null"), "VERSIONING_REQUIRED")
            require(result["ContentLength"] <= LIMITS["source"], "OBJECT_BUDGET")
            data = stream.read(LIMITS["source"] + 1)
            require(len(data) == result["ContentLength"] <= LIMITS["source"], "INCOMPLETE_OBJECT")
            return {
                "version": result["VersionId"],
                "etag": result["ETag"],
                "data": data,
                "sha": sha(data),
                "size": len(data),
            }
        finally:
            stream.close()


@dataclass
class Budget:
    objects: int = 1
    bytes: int = 64 * MIB
    admitted: dict = field(default_factory=dict)

    def check(self, unit, charge):
        require(0 < self.objects <= LIMITS["gc_objects"] and 0 < self.bytes <= LIMITS["gc_bytes"], "GC_BUDGET")
        require(unit not in self.admitted, "DUPLICATE_GC_ADMISSION")
        require(len(self.admitted) < self.objects and sum(self.admitted.values()) + charge <= self.bytes, "GC_BUDGET")


DDL = """
CREATE TABLE control (
 singleton boolean PRIMARY KEY CHECK(singleton), layout integer NOT NULL CHECK(layout=1),
 catalog uuid NOT NULL, table_uuid uuid NOT NULL, epoch bigint NOT NULL CHECK(epoch>0),
 owner bigint NOT NULL CHECK(owner>0), revision bigint NOT NULL, head text,
 bucket text NOT NULL, prefix text NOT NULL, endpoint text NOT NULL);
CREATE TABLE units (id text PRIMARY KEY, kind text NOT NULL, charge bigint NOT NULL CHECK(charge>0),
 state text NOT NULL, snapshot text);
CREATE TABLE objects (key text PRIMARY KEY, unit text NOT NULL REFERENCES units(id), role text NOT NULL,
 size bigint NOT NULL, sha text, version text, etag text, sealed boolean NOT NULL DEFAULT false);
CREATE TABLE requests (id text PRIMARY KEY, payload_hash text NOT NULL, source text NOT NULL REFERENCES units(id),
 expected bigint NOT NULL, claim bigint NOT NULL, state text NOT NULL, manifest text, receipt jsonb);
CREATE TABLE refs (kind text NOT NULL, holder text NOT NULL, unit text NOT NULL REFERENCES units(id),
 PRIMARY KEY(kind,holder,unit));
CREATE TABLE readers (id text PRIMARY KEY, kind text NOT NULL, root text, revision bigint NOT NULL,
 units jsonb NOT NULL, closed boolean NOT NULL DEFAULT false);
CREATE TABLE gc (unit text PRIMARY KEY REFERENCES units(id), nonce uuid NOT NULL, claim bigint NOT NULL,
 targets jsonb NOT NULL DEFAULT '{}', barriers jsonb NOT NULL DEFAULT '[]', receipt jsonb);
"""


class Backend:
    """One catalog, one table UUID/epoch and one named index in a private v1 schema.

    control-row locking serializes metadata mutations across broker processes.
    Native business SQL and distributed serving are deliberately not connected.
    """

    def __init__(self, dsn, schema, objects):
        require(re.fullmatch(r"r6_[a-z0-9_]{1,48}", schema), "BAD_SCHEMA")
        self.dsn, self.schema, self.objects = dsn, schema, objects
        self.copies = {}

    def connect(self):
        c = psycopg.connect(self.dsn, autocommit=True, row_factory=dict_row)
        c.execute(sql.SQL("SET search_path TO {},pg_catalog").format(sql.Identifier(self.schema)))
        return c

    def initialize(self, catalog, table_uuid, epoch=1):
        self.objects.profile()
        with self.connect() as c:
            require(c.execute("SHOW fsync").fetchone()["fsync"] == "on", "FSYNC_REQUIRED")
            require(
                c.execute("SHOW full_page_writes").fetchone()["full_page_writes"] == "on", "FULL_PAGE_WRITES_REQUIRED"
            )
            with c.transaction():
                c.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(self.schema)))
                c.execute(DDL)
                c.execute(
                    "INSERT INTO control VALUES(true,1,%s,%s,%s,1,0,NULL,%s,%s,%s)",
                    (
                        catalog,
                        table_uuid,
                        epoch,
                        self.objects.bucket,
                        self.objects.prefix,
                        self.objects.client.meta.endpoint_url,
                    ),
                )

    @contextmanager
    def transaction(self, token=None):
        c, committing = self.connect(), False
        try:
            c.execute("BEGIN ISOLATION LEVEL READ COMMITTED")
            c.execute("SET LOCAL synchronous_commit=on")
            state = c.execute("SELECT * FROM control WHERE singleton FOR UPDATE").fetchone()
            require(state and state["layout"] == 1, "UNSUPPORTED_LAYOUT")
            require(
                state["bucket"] == self.objects.bucket
                and state["prefix"] == self.objects.prefix
                and state["endpoint"] == self.objects.client.meta.endpoint_url,
                "STORE_BINDING_MISMATCH",
            )
            if token is not None:
                require(state["owner"] == token, "FENCED")
            yield c, state
            committing = True
            c.execute("COMMIT")
        except psycopg.OperationalError as error:
            if committing and error.sqlstate is None:
                raise CommitUnknown("Reconcile the original request; do not delete prepared objects") from None
            raise
        finally:
            c.close()

    def state(self):
        with self.connect() as c:
            state = c.execute("SELECT * FROM control WHERE singleton").fetchone()
            require(
                state["bucket"] == self.objects.bucket
                and state["prefix"] == self.objects.prefix
                and state["endpoint"] == self.objects.client.meta.endpoint_url,
                "STORE_BINDING_MISMATCH",
            )
            return state

    @staticmethod
    def request(c, name, token=None):
        row = c.execute("SELECT * FROM requests WHERE id=%s", (name,)).fetchone()
        require(row is not None, "UNKNOWN_REQUEST")
        if token is not None:
            require(row["claim"] == token and row["state"] == "prepared", "REQUEST_CLOSED")
        return row

    @staticmethod
    def files(c, unit):
        return c.execute("SELECT * FROM objects WHERE unit=%s ORDER BY role", (unit,)).fetchall()

    @staticmethod
    def object(c, key):
        row = c.execute("SELECT * FROM objects WHERE key=%s", (key,)).fetchone()
        require(row is not None, "KEY_NOT_OWNED")
        return row

    @staticmethod
    def identity(obj, raw):
        require(raw is not None and raw["sha"] == obj["sha"] and raw["size"] == obj["size"], "IDENTITY_MISMATCH")

    def takeover(self, expected):
        with self.transaction(expected) as (c, state):
            require(state["owner"] < 2**63 - 1, "OWNER_EXHAUSTED")
            c.execute("UPDATE control SET owner=owner+1 WHERE singleton")
        return expected + 1

    def register_source(self, name, data, snapshot, token):
        require(re.fullmatch(r"[a-z0-9-]{1,80}", name), "BAD_SOURCE_ID")
        require(0 < len(data) <= LIMITS["source"], "SOURCE_BUDGET")
        unit = "source:" + name
        with self.transaction(token) as (c, state):
            key = self.objects.prefix + f"{state['table_uuid']}/owner-{token}/source-{name}"
            c.execute("INSERT INTO units VALUES(%s,'source',%s,'allocated',%s)", (unit, len(data), snapshot))
            c.execute(
                "INSERT INTO objects(key,unit,role,size,sha) VALUES(%s,%s,'source',%s,%s)",
                (key, unit, len(data), sha(data)),
            )
            c.execute("INSERT INTO refs VALUES('build',%s,%s)", (unit, unit))
        return unit

    def source_ticket(self, unit, data, token):
        with self.transaction(token) as (c, _):
            row = c.execute("SELECT * FROM units WHERE id=%s", (unit,)).fetchone()
            require(row and row["kind"] == "source" and row["state"] == "allocated", "SOURCE_CLOSED")
            obj = self.files(c, unit)[0]
            self.identity(obj, {"sha": sha(data), "size": len(data)})
            return self.objects.ticket(obj["key"], data=data, condition="*")

    def seal_source(self, unit, token):
        with self.transaction(token) as (c, _):
            obj = self.files(c, unit)[0]
            raw = self.objects.read(obj["key"])
            self.identity(obj, raw)
            c.execute(
                "UPDATE objects SET sealed=true,version=%s,etag=%s WHERE key=%s",
                (raw["version"], raw["etag"], obj["key"]),
            )
            c.execute("UPDATE units SET state='ready' WHERE id=%s AND state='allocated'", (unit,))
            c.execute("DELETE FROM refs WHERE kind='build' AND holder=%s", (unit,))
            c.execute("INSERT INTO refs VALUES('native',%s,%s)", (unit, unit))

    def prepare(self, name, source, data, token, expected, table_uuid, epoch=1, rows=128, dimension=8, segments=1):
        require(re.fullmatch(r"[a-z0-9-]{1,80}", name), "BAD_REQUEST_ID")
        require(0 < len(data) <= LIMITS["artifact"], "ARTIFACT_BUDGET")
        require(
            1 <= dimension <= 4096 and 65 <= rows <= min(LIMITS["rows"], LIMITS["vector"] // (4 * dimension)),
            "ROW_VECTOR_BUDGET",
        )
        require(1 <= segments <= LIMITS["segments"], "SEGMENT_BUDGET")
        payload_hash = sha(canonical([source, sha(data), len(data), rows, dimension, segments]).encode())
        unit = "generation:" + name
        with self.transaction(token) as (c, state):
            require(str(state["table_uuid"]) == table_uuid and state["epoch"] == epoch, "STALE_REQUEST_IDENTITY")
            old = c.execute("SELECT * FROM requests WHERE id=%s", (name,)).fetchone()
            if old is not None:
                require(
                    old["payload_hash"] == payload_hash and old["expected"] == expected and old["source"] == source,
                    "PAYLOAD_MISMATCH",
                )
                return old
            require(state["revision"] == expected, "REVISION_CONFLICT")
            src = c.execute("SELECT * FROM units WHERE id=%s", (source,)).fetchone()
            require(
                src
                and src["kind"] == "source"
                and src["state"] == "ready"
                and c.execute("SELECT 1 FROM refs WHERE kind='native' AND unit=%s", (source,)).fetchone(),
                "INPUT_SNAPSHOT_CHANGED",
            )
            prefix = self.objects.prefix + f"{table_uuid}/owner-{token}/{name}/"
            c.execute("INSERT INTO units VALUES(%s,'generation',%s,'allocated',NULL)", (unit, len(data)))
            c.execute(
                "INSERT INTO objects(key,unit,role,size,sha) VALUES(%s,%s,'index',%s,%s),(%s,%s,'manifest',0,NULL)",
                (prefix + "index", unit, len(data), sha(data), prefix + "manifest", unit),
            )
            c.execute(
                "INSERT INTO requests VALUES(%s,%s,%s,%s,%s,'prepared',NULL,NULL)",
                (name, payload_hash, source, expected, token),
            )
            for ref in (unit, source):
                c.execute("INSERT INTO refs VALUES('build',%s,%s)", (name, ref))
        return self.status(name)

    def status(self, name):
        with self.connect() as c:
            return self.request(c, name)

    def adopt(self, name, token):
        with self.transaction(token) as (c, _):
            req = self.request(c, name)
            require(req["state"] == "prepared", "REQUEST_CLOSED")
            c.execute("UPDATE requests SET claim=%s WHERE id=%s", (token, name))

    def payload_ticket(self, name, role, data, token):
        with self.transaction(token) as (c, _):
            self.request(c, name, token)
            obj = c.execute("SELECT * FROM objects WHERE unit=%s AND role=%s", ("generation:" + name, role)).fetchone()
            require(obj and obj["sha"] is not None and not obj["sealed"], "NOT_UPLOADABLE")
            self.identity(obj, {"sha": sha(data), "size": len(data)})
            return self.objects.ticket(obj["key"], data=data, condition="*")

    def seal(self, name, role, token):
        with self.transaction(token) as (c, _):
            self.request(c, name, token)
            obj = c.execute("SELECT * FROM objects WHERE unit=%s AND role=%s", ("generation:" + name, role)).fetchone()
            require(obj is not None and obj["sha"] is not None, "NOT_UPLOADABLE")
            raw = self.objects.read(obj["key"])
            self.identity(obj, raw)
            require(not obj["sealed"] or obj["version"] == raw["version"], "SEALED_VERSION_CHANGED")
            c.execute(
                "UPDATE objects SET sealed=true,version=%s,etag=%s WHERE key=%s",
                (raw["version"], raw["etag"], obj["key"]),
            )

    def manifest(self, name, token):
        with self.transaction(token) as (c, state):
            req = self.request(c, name, token)
            source = self.files(c, req["source"])[0]
            index = next(o for o in self.files(c, "generation:" + name) if o["role"] == "index")
            require(source["sealed"] and index["sealed"], "INDEX_NOT_SEALED")
            snapshot = c.execute("SELECT snapshot FROM units WHERE id=%s", (req["source"],)).fetchone()["snapshot"]
            body = canonical(
                {
                    "catalog_uuid": str(state["catalog"]),
                    "table_uuid": str(state["table_uuid"]),
                    "request_epoch": state["epoch"],
                    "request_id": name,
                    "payload_hash": req["payload_hash"],
                    "expected_revision": req["expected"],
                    "input_snapshot": snapshot,
                    "store": {k: state[k] for k in ("endpoint", "bucket", "prefix")},
                    "objects": [{k: o[k] for k in ("key", "version", "size", "sha")} for o in (source, index)],
                }
            )
            require(len(body.encode()) <= LIMITS["manifest"], "MANIFEST_BUDGET")
            require(req["manifest"] is None or req["manifest"] == body, "MANIFEST_CHANGED")
            c.execute("UPDATE requests SET manifest=%s WHERE id=%s", (body, name))
            c.execute(
                "UPDATE objects SET size=%s,sha=%s WHERE unit=%s AND role='manifest'",
                (len(body.encode()), sha(body.encode()), "generation:" + name),
            )
        return body.encode()

    def publish(self, name, token):
        with self.transaction(token) as (c, state):
            req = self.request(c, name)
            if req["receipt"] is not None:
                return req["receipt"]
            self.request(c, name, token)
            require(req["expected"] == state["revision"], "REVISION_CONFLICT")
            require(req["manifest"] is not None, "UNSEALED_PUBLICATION")
            unit = "generation:" + name
            require(
                c.execute("SELECT state FROM units WHERE id=%s", (unit,)).fetchone()["state"] == "allocated",
                "OBJECT_RETIRING",
            )
            require(all(o["sealed"] for o in self.files(c, unit)), "UNSEALED_PUBLICATION")
            require(
                c.execute("SELECT 1 FROM refs WHERE kind='native' AND unit=%s", (req["source"],)).fetchone(),
                "INPUT_SNAPSHOT_CHANGED",
            )
            receipt = {
                "table_uuid": str(state["table_uuid"]),
                "request_epoch": state["epoch"],
                "request_id": name,
                "payload_hash": req["payload_hash"],
                "revision": state["revision"] + 1,
            }
            c.execute("UPDATE control SET revision=%s,head=%s WHERE singleton", (receipt["revision"], name))
            c.execute("DELETE FROM refs WHERE kind='named' OR (kind='build' AND holder=%s)", (name,))
            for ref in (unit, req["source"]):
                c.execute("INSERT INTO refs VALUES('named','knn',%s)", (ref,))
            c.execute("UPDATE units SET state='ready' WHERE id=%s", (unit,))
            c.execute("UPDATE requests SET state='committed',receipt=%s WHERE id=%s", (Jsonb(receipt), name))
        return receipt

    def reconcile(self, name, payload_hash):
        req = self.status(name)
        require(req["payload_hash"] == payload_hash, "PAYLOAD_MISMATCH")
        return req["receipt"]

    def cancel(self, name, token):
        with self.transaction(token) as (c, _):
            req = self.request(c, name)
            require(req["state"] == "prepared", "REQUEST_CLOSED")
            c.execute("UPDATE requests SET state='cancelled' WHERE id=%s", (name,))
            c.execute("DELETE FROM refs WHERE kind='build' AND holder=%s", (name,))

    def capture(self, name, kind="vector", source=None):
        require(kind in ("vector", "sql"), "BAD_READER_KIND")
        with self.transaction() as (c, state):
            if kind == "vector":
                require(state["head"] is not None, "EMPTY_INDEX")
                req = self.request(c, state["head"])
                units = sorted(["generation:" + req["id"], req["source"]])
            else:
                require(
                    c.execute("SELECT 1 FROM refs WHERE kind='native' AND unit=%s", (source,)).fetchone(),
                    "EXPIRED_NATIVE_HISTORY",
                )
                units = [source]
            for unit in units:
                require(
                    c.execute("SELECT state FROM units WHERE id=%s", (unit,)).fetchone()["state"] == "ready",
                    "OBJECT_RETIRING",
                )
                c.execute("INSERT INTO refs VALUES('reader',%s,%s)", (name, unit))
            c.execute(
                "INSERT INTO readers VALUES(%s,%s,%s,%s,%s,false)",
                (name, kind, state["head"], state["revision"], Jsonb(units)),
            )

    def materialize(self, name):
        self.state()
        with self.connect() as c:
            view = c.execute("SELECT * FROM readers WHERE id=%s AND NOT closed", (name,)).fetchone()
            require(view is not None, "VIEW_CLOSED")
            require(
                c.execute("SELECT count(*) AS n FROM refs WHERE kind='reader' AND holder=%s", (name,)).fetchone()["n"]
                == len(view["units"]),
                "VIEW_NOT_PINNED",
            )
            result = {}
            for unit in view["units"]:
                for obj in self.files(c, unit):
                    raw = self.objects.read(obj["key"], obj["version"])
                    self.identity(obj, raw)
                    result[obj["key"]] = raw["data"]
            self.copies[name] = result
        return result

    def release_copy_pin(self, name):
        require(name in self.copies, "COPY_INCOMPLETE")
        with self.transaction() as (c, _):
            require(c.execute("SELECT 1 FROM readers WHERE id=%s AND NOT closed", (name,)).fetchone(), "VIEW_CLOSED")
            c.execute("DELETE FROM refs WHERE kind='reader' AND holder=%s", (name,))

    def close_view(self, name):
        with self.transaction() as (c, _):
            c.execute("UPDATE readers SET closed=true WHERE id=%s", (name,))
            c.execute("DELETE FROM refs WHERE kind='reader' AND holder=%s", (name,))
        self.copies.pop(name, None)

    def expire_native(self, source, token):
        with self.transaction(token) as (c, _):
            require(
                not c.execute("SELECT 1 FROM readers WHERE kind='sql' AND NOT closed").fetchone(),
                "ORDINARY_TRANSACTION",
            )
            require(
                not c.execute("SELECT 1 FROM refs WHERE kind='named' AND unit=%s", (source,)).fetchone(),
                "NATIVE_STILL_CURRENT",
            )
            c.execute("DELETE FROM refs WHERE kind='native' AND unit=%s", (source,))

    def plan_gc(self, unit, token, budget):
        with self.transaction(token) as (c, _):
            item = c.execute("SELECT * FROM units WHERE id=%s", (unit,)).fetchone()
            require(item and item["state"] != "deleted", "ALREADY_COLLECTED")
            require(not c.execute("SELECT 1 FROM refs WHERE unit=%s", (unit,)).fetchone(), "REFERENCED")
            require(
                item["kind"] != "source"
                or not c.execute("SELECT 1 FROM readers WHERE kind='sql' AND NOT closed").fetchone(),
                "ORDINARY_TRANSACTION",
            )
            budget.check(unit, item["charge"])
            c.execute("UPDATE units SET state='deleting' WHERE id=%s", (unit,))
            c.execute(
                "INSERT INTO gc(unit,nonce,claim) VALUES(%s,%s,%s) ON CONFLICT(unit) DO UPDATE SET claim=excluded.claim",
                (unit, uuid.uuid4(), token),
            )
        budget.admitted[unit] = item["charge"]

    @staticmethod
    def intent(c, unit, token, budget):
        require(unit in budget.admitted, "NO_GC_ADMISSION")
        intent = c.execute("SELECT * FROM gc WHERE unit=%s", (unit,)).fetchone()
        require(intent and intent["claim"] == token and intent["receipt"] is None, "NO_GC_ADMISSION")
        return intent

    def stop(self, unit, key, token, budget):
        with self.transaction(token) as (c, state):
            intent = self.intent(c, unit, token, budget)
            obj = self.object(c, key)
            require(obj["unit"] == unit, "KEY_NOT_OWNED")
            body = canonical(
                {
                    "kind": "vortex-stop-v1",
                    "catalog": str(state["catalog"]),
                    "table": str(state["table_uuid"]),
                    "key": key,
                    "intent": str(intent["nonce"]),
                }
            ).encode()
            raw = self.objects.read(key)
            if raw is not None and raw["data"] == body:
                require(key in intent["targets"], "UNREGISTERED_BARRIER")
                barriers = sorted(set(intent["barriers"]) | {key})
                c.execute("UPDATE gc SET barriers=%s WHERE unit=%s", (Jsonb(barriers), unit))
                return None, body
            if raw is not None:
                self.identity(obj, raw)
            targets = dict(intent["targets"], **{key: raw["version"] if raw else None})
            c.execute("UPDATE gc SET targets=%s WHERE unit=%s", (Jsonb(targets), unit))
            ticket = self.objects.ticket(key, data=body, condition=raw["etag"] if raw else "*")
        return ticket, body

    def delete_payload(self, unit, key, token, budget):
        with self.transaction(token) as (c, _):
            intent = self.intent(c, unit, token, budget)
            require(key in intent["barriers"] and self.object(c, key)["unit"] == unit, "STOP_NOT_CONFIRMED")
            version = intent["targets"][key]
            if version is None or self.objects.read(key, version) is None:
                return None
            return self.objects.ticket(key, version=version)

    def finish_gc(self, unit, token, budget):
        with self.transaction(token) as (c, state):
            old = c.execute("SELECT * FROM gc WHERE unit=%s", (unit,)).fetchone()
            require(old is not None, "UNKNOWN_GC_INTENT")
            if old["receipt"] is not None:
                return old["receipt"]
            intent = self.intent(c, unit, token, budget)
            files = self.files(c, unit)
            require(set(intent["barriers"]) == {o["key"] for o in files}, "STOP_NOT_CONFIRMED")
            for obj in files:
                body = canonical(
                    {
                        "kind": "vortex-stop-v1",
                        "catalog": str(state["catalog"]),
                        "table": str(state["table_uuid"]),
                        "key": obj["key"],
                        "intent": str(intent["nonce"]),
                    }
                ).encode()
                raw = self.objects.read(obj["key"])
                require(raw is not None and raw["data"] == body, "STOP_NOT_CONFIRMED")
                target = intent["targets"][obj["key"]]
                require(target is None or self.objects.read(obj["key"], target) is None, "PAYLOAD_REMAINS")
            charge = c.execute("SELECT charge FROM units WHERE id=%s", (unit,)).fetchone()["charge"]
            receipt = {"objects": 1, "bytes": charge}
            c.execute("UPDATE units SET state='deleted' WHERE id=%s", (unit,))
            c.execute("UPDATE gc SET receipt=%s WHERE unit=%s", (Jsonb(receipt), unit))
        return receipt
