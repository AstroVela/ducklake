"""Independent SQL/IEEE/ranking acceptance, without importing the R7 fixture."""

import hashlib
import json
import math
from pathlib import Path
import struct


def require(value, message):
    if not value:
        raise AssertionError(message)


def key(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def bits(row):
    return (row["id"], tuple(struct.pack("!f", v) for v in row["embedding"]), row["category"], row["payload"])


def capture(connection, captured):
    file = captured["file_capture"]
    snapshot = file["snapshot_id"]
    result = connection.execute(
        "SELECT id,embedding,category,payload,filename,file_row_number,rowid "
        f"FROM lake.items AT (VERSION => {snapshot}) ORDER BY id"
    ).fetchall()
    files = {str(Path(f["uri"])): f for f in file["files"]}
    expected = []
    for row in result:
        if str(Path(row[4])) in files:
            address = {"kind": "file", "file_id": files[str(Path(row[4]))]["id"], "row_offset": row[5]}
        else:
            require(row[4].startswith("ducklake_inlined_data_"), "Unknown SQL physical source")
            address = {"kind": "inline", "source": row[4], "row_id": row[6]}
        expected.append({"row_ref": address, "row": dict(zip(("id", "embedding", "category", "payload"), row[:4]))})
    actual = sorted(captured["rows"], key=lambda r: r["row"]["id"])
    require(len(actual) == len(expected), "Captured row count differs from historical SQL")
    require(
        all(a["row_ref"] == b["row_ref"] and bits(a["row"]) == bits(b["row"]) for a, b in zip(actual, expected)),
        "ReadView changed physical addresses or typed business values",
    )
    total = 0
    fields = file["business_fields"]
    for item in file["files"]:
        data = Path(item["uri"]).read_bytes()
        total += len(data)
        require(item["version"] == "sha256:" + hashlib.sha256(data).hexdigest(), "Source digest differs")
        names = [
            f"__ducklake_vortex_v1_field_{v}"
            for v in (fields["id"], file["field_id"], fields["category"], fields["payload"])
        ]
        physical = connection.execute("SELECT " + ",".join(names) + " FROM read_vortex(?)", [item["uri"]]).fetchall()
        require(len(physical) == item["row_count"], "Vortex row count differs")
        for row in expected:
            ref = row["row_ref"]
            if ref["kind"] == "file" and ref["file_id"] == item["id"]:
                decoded = dict(zip(("id", "embedding", "category", "payload"), physical[ref["row_offset"]]))
                require(bits(decoded) == bits(row["row"]), "Vortex physical offset differs from snapshot")
    require(total <= 64 * 1024 * 1024 and len(expected) <= 100000, "Source budget exceeded")
    return expected


def qualify(hits, expected, vector, k, minimum=0.8):
    rows = {key(r["row_ref"]): r for r in expected}
    require(len(rows) == len(expected), "Reference row addresses are not unique")
    ranked = sorted(
        (
            (sum((float(v) - float(q)) ** 2 for v, q in zip(r["row"]["embedding"], vector)), key(r["row_ref"]))
            for r in expected
        ),
        key=lambda item: (item[0], item[1]),
    )
    target = min(k, len(ranked))
    require(len(hits) == target, "Native result is incomplete")
    seen, distances = set(), []
    for hit in hits:
        address = key(hit["row_ref"])
        require(address in rows and address not in seen, "Invisible or duplicate native hit")
        seen.add(address)
        require(bits(hit["row"]) == bits(rows[address]["row"]), "Native hit payload came from another version")
        exact = sum((float(v) - float(q)) ** 2 for v, q in zip(rows[address]["row"]["embedding"], vector))
        require(
            math.isfinite(hit["distance"]) and math.isclose(hit["distance"], exact, rel_tol=1e-5, abs_tol=1e-5),
            "Native distance differs",
        )
        distances.append(hit["distance"])
    require(distances == sorted(distances), "Native rank is not ascending")
    cutoff = ranked[target - 1][0] if target else -1
    # Equal-distance neighbors at the cutoff are interchangeable in this native
    # component test. The production global RowRef tie order is unchanged.
    eligible = {address for distance, address in ranked if distance <= cutoff}
    recall = len(seen & eligible) / target if target else 1.0
    require(recall >= minimum, "Native recall is below the existing 0.8 gate")
    return recall
