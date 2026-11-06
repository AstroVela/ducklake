"""Independent byte/row accounting from the documented Vortex request and capacity contracts."""

from __future__ import annotations

import hashlib
import json
import struct

MIB = 1024 * 1024
ROW_LIMIT = 100000
VECTOR_BYTES = SOURCE_BYTES = ARTIFACT_BYTES = PENDING_BYTES = 64 * MIB
INLINE_BYTES = BATCH_BYTES = 4 * MIB
MANIFEST_BYTES = MIB
OPERATIONS = 1000
SEGMENTS = 3
UUID = "00000000-0000-0000-0000-000000000001"


def row_limit(dimension):
    if not 1 <= dimension <= 4096:
        raise ValueError("Invalid dimension")
    return min(ROW_LIMIT, VECTOR_BYTES // (4 * dimension))


def row_bytes(row):
    return (
        64
        + 4 * len(row["embedding"])
        + sum(len(row[field].encode("utf-8")) for field in ("category", "payload") if row[field] is not None)
    )


def canonical(rows, dimension, deletes=None):
    mutation = deletes is not None
    result = bytearray(b"ducklake-vortex-mutate-v1" if mutation else b"ducklake-vortex-write-v1")
    result += struct.pack("<IQ", dimension, len(rows))
    for row in rows:
        value = row["id"]
        result += b"\x00" if value is None else b"\x01" + struct.pack("<q", value)
        for value in row["embedding"]:
            rounded = struct.unpack("<f", struct.pack("<f", value))[0]
            result += struct.pack("<f", 0.0 if rounded == 0 else rounded)
        for field in ("category", "payload"):
            value = row[field]
            if value is None:
                result += b"\x00"
            else:
                encoded = value.encode("utf-8")
                result += b"\x01" + struct.pack("<Q", len(encoded)) + encoded
    if mutation:
        result += struct.pack("<Q", len(deletes))
        for value in deletes:
            result += struct.pack("<q", value)
    return bytes(result)


def compact(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def digest(data):
    return "sha256:" + hashlib.sha256(data).hexdigest()


def manifest(dimension, counts, files_per_segment=1, path_padding=0):
    entries = []
    for part, count in enumerate(counts):
        files = [
            {
                "id": part * 100000 + i,
                "uri": "/source/" + "x" * path_padding + f"file-{part}-{i:05d}.vortex",
                "version": "sha256:" + "0" * 64,
                "row_count": count if files_per_segment == 1 else 1,
            }
            for i in range(files_per_segment)
        ]
        reference = {
            "format_version": 1,
            "table_uuid": UUID,
            "field_id": 1,
            "dimension": dimension,
            "metric": "squared_l2",
            "snapshot": {
                "dataset_id": "ducklake-physical-files:" + UUID,
                "version": digest(compact(files).encode()),
                "schema_fingerprint": f"ducklake-file-vector-v1:1:float32:{dimension}:squared_l2",
                "files": files,
            },
            "generation": {
                "generation": "generation-" + str(part),
                "manifest": {"path": "manifest.json", "size": 1, "checksum": "sha256:" + "0" * 64},
            },
        }
        entries.append({"root": "/capacity", "reference": reference})
    return {
        "format_version": 3,
        "table_uuid": UUID,
        "field_id": 1,
        "dimension": dimension,
        "root": "/capacity",
        "publication": UUID,
        "segments": entries,
    }


def sized_manifest(size):
    result = manifest(1, [4096], files_per_segment=4096)
    files = result["segments"][0]["reference"]["snapshot"]["files"]
    difference = size - len(compact(result).encode())
    if difference < 0:
        raise ValueError("Requested manifest is too small")
    whole, remainder = divmod(difference, len(files))
    for i, file in enumerate(files):
        file["uri"] = "/source/" + "x" * (whole + (i < remainder)) + f"file-0-{i:05d}.vortex"
    result["segments"][0]["reference"]["snapshot"]["version"] = digest(compact(files).encode())
    assert len(compact(result).encode()) == size
    return result
