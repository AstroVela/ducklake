"""Independent durable-session checks over raw PostgreSQL rows and lock facts."""

import hashlib
import json


TABLES = ("reader_sessions", "reader_session_views", "reader_reclaims")


def require(value, message):
    if not value:
        raise AssertionError(message)


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def observe(backend, services, bound_oracle, storage_oracle, typed_oracle):
    raw, typed, expected = bound_oracle.observe(backend, services, storage_oracle, typed_oracle)
    with backend.connect() as c:
        c.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        for table in TABLES:
            raw[table] = sorted(c.execute("SELECT * FROM " + table).fetchall(), key=encode)
        locks = c.execute(
            """SELECT l.pid,l.classid::bigint AS high,l.objid::bigint AS low,a.backend_start
            FROM pg_locks l JOIN pg_stat_activity a ON a.pid=l.pid WHERE l.locktype='advisory'
            AND l.granted AND l.mode='ExclusiveLock' AND l.objsubid=1 AND
            l.database=(SELECT oid FROM pg_database WHERE datname=current_database())"""
        ).fetchall()
    return json.loads(encode(raw)), typed, expected, json.loads(encode(locks))


def inspect(raw, copies, locks, before=None, previous_copies=None):
    sessions = {s["session_uuid"]: s for s in raw["reader_sessions"]}
    links = {v["reader"]: v for v in raw["reader_session_views"]}
    reclaims = {r["reader"]: r for r in raw["reader_reclaims"]}
    readers = {r["id"]: r for r in raw["readers"]}
    bound = {r["reader"]: r for r in raw["bound_readers"]}
    require(set(links) == set(bound), "A typed view has no mandatory session")
    for sid, session in sessions.items():
        key = int.from_bytes(
            hashlib.sha256((raw["control"][0]["catalog"] + ":" + sid).encode()).digest()[:8], "big", signed=True
        )
        require(session["lock_key"] == key, "Session lock identity changed")
        require(session["fence"] == (1 if session["state"] == "active" else 2), "Wrong reader fence")
        if session["state"] in ("reclaiming", "reclaimed"):
            intent = {"session_uuid": sid, "owner_uuid": session["owner_uuid"], "original_fence": 1, "fence": 2}
            require(session["intent"] == intent, "Reclaim intent identity differs")
            require(
                session["receipt"] == (dict(intent, finished=True) if session["state"] == "reclaimed" else None),
                "Session completion receipt differs",
            )
        elif session["state"] == "closed":
            require(
                session["intent"] is None
                and session["receipt"]
                == {
                    "session_uuid": sid,
                    "owner_uuid": session["owner_uuid"],
                    "original_fence": 1,
                    "fence": 2,
                    "closed": True,
                },
                "Graceful close receipt differs",
            )
        else:
            require(session["intent"] is None and session["receipt"] is None, "Live session contains cleanup state")
        if session["state"] in ("closed", "reclaimed"):
            require(
                all(readers[r]["closed"] for r, v in links.items() if v["session_uuid"] == sid),
                "Terminal session still has open readers",
            )
    for reader, link in links.items():
        require(link["session_uuid"] in sessions and link["session_fence"] == 1, "Wrong session-view lineage")
        pins = sorted(r["unit"] for r in raw["refs"] if r["kind"] == "reader" and r["holder"] == reader)
        session = sessions[link["session_uuid"]]
        if reader in reclaims:
            reclaim = reclaims[reader]
            receipt = reclaim["receipt"]
            require(
                session["state"] in ("reclaiming", "reclaimed")
                and reclaim["session_uuid"] == link["session_uuid"]
                and reclaim["original_fence"] == 1,
                "Reclaim crossed session ownership",
            )
            require(readers[reader]["closed"] and not pins, "Reclaimed view remains usable or pinned")
            require(
                receipt
                == {
                    "session_uuid": link["session_uuid"],
                    "reader": reader,
                    "original_fence": 1,
                    "fence": 2,
                    "released_units": receipt["released_units"],
                    "bytes": 0,
                }
                and receipt["released_units"] in ([], sorted(readers[reader]["units"])),
                "Wrong pin release receipt",
            )
        if reader in copies:
            copy = copies[reader]
            require(
                copy["session_uuid"] == link["session_uuid"] and copy["session_fence"] == 1,
                "Private copy crossed session",
            )
            if previous_copies is not None and reader not in previous_copies:
                bits = session["lock_key"] & (2**64 - 1)
                require(
                    session["state"] == "active"
                    and any(
                        l["pid"] == session["backend_pid"]
                        and l["backend_start"] == session["backend_start"]
                        and l["high"] == bits >> 32
                        and l["low"] == bits & (2**32 - 1)
                        for l in locks
                    ),
                    "A disconnected/fenced session published a new copy",
                )
    if before is not None:
        old_sessions = {s["session_uuid"]: s for s in before["reader_sessions"]}
        old_links = {v["reader"]: v for v in before["reader_session_views"]}
        for sid, old in old_sessions.items():
            current = sessions[sid]
            require(
                all(
                    old[k] == current[k]
                    for k in ("session_uuid", "owner_uuid", "lock_key", "backend_pid", "backend_start")
                ),
                "Session ownership was adopted",
            )
            require(
                old["state"] == "active"
                or current["state"]
                in ({"reclaiming", "reclaimed"} if old["state"] == "reclaiming" else {old["state"]}),
                "Session was resurrected",
            )
            if old["state"] != "active":
                require(old["intent"] == current["intent"], "Original reclaim intent changed")
            if old["receipt"] is not None:
                require(old["receipt"] == current["receipt"], "Original session receipt changed")
        require(all(links[k] == v for k, v in old_links.items()), "View was transferred to another session")
        for reader in set(links) - set(old_links):
            sid = links[reader]["session_uuid"]
            require(
                sid not in old_sessions or old_sessions[sid]["state"] == "active", "Fenced session captured a new view"
            )
        old_receipts = {r["reader"]: r for r in before["reader_reclaims"]}
        require(all(reclaims[k] == v for k, v in old_receipts.items()), "Original view reclaim receipt changed")
        for reader in set(reclaims) - set(old_receipts):
            pins = sorted(r["unit"] for r in before["refs"] if r["kind"] == "reader" and r["holder"] == reader)
            require(
                reclaims[reader]["receipt"]["released_units"] == pins, "Released pin set differs from the original view"
            )
