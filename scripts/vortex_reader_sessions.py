"""Explicit PostgreSQL reader ownership; no TTL, reconnect or orphan adoption."""

from contextlib import contextmanager

from psycopg.types.json import Jsonb

from vortex_remote_backend import require, sha
from vortex_postgres_metadata import canonical_uuid, integer


class ReaderSession:
    def __init__(self, backend, metadata, session_uuid, owner_uuid, connection, row):
        self.backend, self.metadata = backend, metadata
        self.id, self.owner, self.connection = session_uuid, owner_uuid, connection
        self.fence, self.key = row["fence"], row["lock_key"]
        self.pid, self.started = row["backend_pid"], row["backend_start"]

    @classmethod
    def open(cls, backend, metadata, session_uuid, owner_uuid):
        parsed, owner = canonical_uuid(session_uuid), canonical_uuid(owner_uuid)
        connection = None
        try:
            with backend.transaction() as (c, state):
                metadata.catalog(c)
                require(
                    not c.execute("SELECT 1 FROM reader_sessions WHERE session_uuid=%s", (parsed,)).fetchone(),
                    "READER_SESSION_EXISTS",
                )
                key = int.from_bytes(
                    bytes.fromhex(sha((str(state["catalog"]) + ":" + session_uuid).encode()))[:8], "big", signed=True
                )
                connection = backend.connect()
                require(
                    connection.execute("SELECT pg_try_advisory_lock(%s) AS held", (key,)).fetchone()["held"],
                    "READER_LOCK_BUSY",
                )
                process = connection.execute(
                    "SELECT pid,backend_start FROM pg_stat_activity WHERE pid=pg_backend_pid()"
                ).fetchone()
                row = c.execute(
                    "INSERT INTO reader_sessions VALUES(%s,%s,%s,%s,%s,1,'active',NULL,NULL) RETURNING *",
                    (parsed, owner, key, process["pid"], process["backend_start"]),
                ).fetchone()
            return cls(backend, metadata, session_uuid, owner_uuid, connection, row)
        except BaseException:
            if connection is not None:
                connection.close()
            raise

    def disconnect(self):
        # Disconnecting this holder never removes durable pins or changes metadata.
        self.connection.close()

    def require_lock(self, c):
        require(not self.connection.closed and self.connection.info.backend_pid == self.pid, "READER_DISCONNECTED")
        bits = self.key & (2**64 - 1)
        require(
            c.execute(
                """SELECT 1 FROM pg_locks l JOIN pg_stat_activity a ON a.pid=l.pid WHERE
                l.locktype='advisory' AND l.granted AND l.mode='ExclusiveLock' AND l.objsubid=1
                AND l.database=(SELECT oid FROM pg_database WHERE datname=current_database())
                AND l.pid=%s AND a.backend_start=%s AND l.classid=%s AND l.objid=%s""",
                (self.pid, self.started, bits >> 32, bits & (2**32 - 1)),
            ).fetchone(),
            "READER_DISCONNECTED",
        )

    @contextmanager
    def operation(self, backend, metadata):
        require(self.backend is backend and self.metadata is metadata, "READER_CATALOG_MISMATCH")
        with backend.transaction() as (c, state):
            metadata.catalog(c)
            row = c.execute(
                "SELECT * FROM reader_sessions WHERE session_uuid=%s", (canonical_uuid(self.id),)
            ).fetchone()
            require(
                row
                and row["state"] == "active"
                and row["fence"] == self.fence
                and str(row["owner_uuid"]) == self.owner
                and row["lock_key"] == self.key
                and row["backend_pid"] == self.pid
                and row["backend_start"] == self.started,
                "READER_FENCED",
            )
            self.require_lock(c)
            yield c, state
            # A lost lifetime connection during a remote GET must not publish a copy.
            # No reconnect is attempted if either PostgreSQL connection has died.
            self.require_lock(c)

    def view(self, c, reader):
        require(
            c.execute(
                "SELECT 1 FROM reader_session_views WHERE reader=%s AND session_uuid=%s AND session_fence=%s",
                (reader, canonical_uuid(self.id), self.fence),
            ).fetchone(),
            "READER_VIEW_OWNER_MISMATCH",
        )


class ReaderReclaimer:
    def __init__(self, backend, metadata):
        require(metadata.backend is backend, "METADATA_CATALOG_MISMATCH")
        self.backend, self.metadata = backend, metadata

    def status(self, session_uuid):
        with self.backend.transaction() as (c, _):
            self.metadata.catalog(c)
            row = c.execute(
                "SELECT * FROM reader_sessions WHERE session_uuid=%s", (canonical_uuid(session_uuid),)
            ).fetchone()
            require(row is not None, "UNKNOWN_READER_SESSION")
            return row

    @staticmethod
    def charge(budget, key):
        # Releasing a bounded view's at-most-two pins reads/deletes no payload.
        # Use the caller's existing global work budget; never allocate one here.
        if key in budget.admitted:
            require(budget.admitted[key] == 0, "GC_BUDGET")
        else:
            budget.check(key, 0)
            budget.admitted[key] = 0

    @staticmethod
    def target(c, session_uuid, original):
        parsed = canonical_uuid(session_uuid)
        integer(original, 1, 2**63 - 1, "BAD_READER_FENCE")
        row = c.execute("SELECT * FROM reader_sessions WHERE session_uuid=%s", (parsed,)).fetchone()
        require(row is not None, "UNKNOWN_READER_SESSION")
        require(original == 1, "STALE_READER_FENCE")
        return row

    def begin(self, session_uuid, original, token, budget):
        with self.backend.transaction(token) as (c, _):
            self.metadata.catalog(c)
            row = self.target(c, session_uuid, original)
            if row["intent"] is not None:
                return row["intent"]
            require(row["state"] == "active" and row["fence"] == original, "READER_SESSION_CLOSED")
            require(
                c.execute("SELECT pg_try_advisory_xact_lock(%s) AS held", (row["lock_key"],)).fetchone()["held"],
                "READER_ACTIVE",
            )
            self.charge(budget, "reader-session:" + session_uuid)
            intent = {
                "session_uuid": session_uuid,
                "owner_uuid": str(row["owner_uuid"]),
                "original_fence": original,
                "fence": original + 1,
            }
            c.execute(
                "UPDATE reader_sessions SET fence=%s,state='reclaiming',intent=%s WHERE session_uuid=%s",
                (original + 1, Jsonb(intent), canonical_uuid(session_uuid)),
            )
        return intent

    def view(self, session_uuid, original, reader, token, budget):
        with self.backend.transaction(token) as (c, _):
            self.metadata.catalog(c)
            row = self.target(c, session_uuid, original)
            require(row["intent"] is not None and row["fence"] == original + 1, "NO_READER_RECLAIM_INTENT")
            require(
                c.execute(
                    "SELECT 1 FROM reader_session_views WHERE reader=%s AND session_uuid=%s AND session_fence=%s",
                    (reader, canonical_uuid(session_uuid), original),
                ).fetchone(),
                "READER_VIEW_OWNER_MISMATCH",
            )
            old = c.execute("SELECT receipt FROM reader_reclaims WHERE reader=%s", (reader,)).fetchone()
            if old is not None:
                return old["receipt"]
            require(row["state"] == "reclaiming", "READER_SESSION_CLOSED")
            view = c.execute("SELECT * FROM readers WHERE id=%s", (reader,)).fetchone()
            require(not view["closed"], "VIEW_CLOSED")
            pins = [
                r["unit"]
                for r in c.execute(
                    "SELECT unit FROM refs WHERE kind='reader' AND holder=%s ORDER BY unit", (reader,)
                ).fetchall()
            ]
            require(len(view["units"]) <= 2 and (not pins or pins == sorted(view["units"])), "READER_PIN_SET")
            self.charge(budget, "reader-view:" + reader)
            receipt = {
                "session_uuid": session_uuid,
                "reader": reader,
                "original_fence": original,
                "fence": original + 1,
                "released_units": pins,
                "bytes": 0,
            }
            c.execute("DELETE FROM refs WHERE kind='reader' AND holder=%s", (reader,))
            c.execute("UPDATE readers SET closed=true WHERE id=%s", (reader,))
            c.execute(
                "INSERT INTO reader_reclaims VALUES(%s,%s,%s,%s)",
                (reader, canonical_uuid(session_uuid), original, Jsonb(receipt)),
            )
        return receipt

    def finish(self, session_uuid, original, token, budget):
        with self.backend.transaction(token) as (c, _):
            self.metadata.catalog(c)
            row = self.target(c, session_uuid, original)
            require(row["intent"] is not None and row["fence"] == original + 1, "NO_READER_RECLAIM_INTENT")
            if row["receipt"] is not None:
                return row["receipt"]
            require(row["state"] == "reclaiming", "READER_SESSION_CLOSED")
            require(
                not c.execute(
                    "SELECT 1 FROM reader_session_views b JOIN readers r ON r.id=b.reader WHERE b.session_uuid=%s AND NOT r.closed",
                    (canonical_uuid(session_uuid),),
                ).fetchone(),
                "READER_VIEWS_REMAIN",
            )
            require(
                not c.execute(
                    "SELECT 1 FROM refs r JOIN reader_session_views b ON r.kind='reader' AND r.holder=b.reader WHERE b.session_uuid=%s",
                    (canonical_uuid(session_uuid),),
                ).fetchone(),
                "READER_PINS_REMAIN",
            )
            self.charge(budget, "reader-session:" + session_uuid)
            receipt = dict(row["intent"], finished=True)
            c.execute(
                "UPDATE reader_sessions SET state='reclaimed',receipt=%s WHERE session_uuid=%s",
                (Jsonb(receipt), canonical_uuid(session_uuid)),
            )
        return receipt
