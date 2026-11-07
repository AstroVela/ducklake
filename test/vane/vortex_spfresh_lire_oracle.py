"""Independently decode persistent heads, labels and RocksDB posting records."""

import json
from pathlib import Path
import struct
import subprocess


def require(value, message):
    if not value:
        raise AssertionError(message)


def labels(path):
    data = path.read_bytes()
    require(len(data) >= 12, "Truncated native label data")
    deleted, rows, columns = struct.unpack_from("<iii", data)
    require(0 <= rows <= 100000 and columns == 1 and len(data) == 12 + rows, "Native label dimensions")
    return deleted, data[12:]


def observe(probe, native, body):
    process = subprocess.run(
        [str(probe), "dump", str(native)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )
    require(process.returncode == 0, "Raw RocksDB inspection failed: " + process.stderr[-2000:])
    packets = [json.loads(line[3:]) for line in process.stdout.splitlines() if line.startswith("R7 ")]
    require(len(packets) == 1 and set(packets[0]) == {"postings"}, "Raw posting inspection protocol")
    deleted, versions = labels(native / "versions.bin")
    mapping = body["mapping"]
    require(len(versions) == len(mapping) and deleted == versions.count(0xFE), "Version labels lost native VIDs")
    visible = {json.dumps(r["row_ref"], sort_keys=True) for r in body["capture"]["rows"]}
    live = {i for i, row in enumerate(mapping) if json.dumps(row["row_ref"], sort_keys=True) in visible}
    require({i for i, tag in enumerate(versions) if tag != 0xFE} == live, "Native delete labels differ from ReadView")

    data = (native / "HeadIndex/vectors.bin").read_bytes()
    count, dimension = struct.unpack_from("<ii", data)
    require(0 < count <= 100000 and dimension == 8 and len(data) == 8 + count * 32, "Head vector dimensions")
    heads = [struct.unpack_from("<8f", data, 8 + i * 32) for i in range(count)]
    retired, head_labels = labels(native / "HeadIndex/deletes.bin")
    require(len(head_labels) == count and retired == head_labels.count(1), "Head retirement labels differ")
    sizes = (native / "posting-sizes.bin").read_bytes()
    require(struct.unpack_from("<ii", sizes) == (count, 1) and len(sizes) == 8 + count * 4, "Posting size dimensions")
    recorded = struct.unpack_from("<" + "i" * count, sizes, 8)
    active = {i for i, value in enumerate(head_labels) if value != 1}
    physical, current, stale = {}, {}, 0
    postings = packets[0]["postings"]
    require(len({head for head, _ in postings}) == len(postings), "Repeated RocksDB key")
    for head, value in postings:
        raw = bytes.fromhex(value)
        require(0 <= head < count and len(raw) % 37 == 0, "Malformed native posting record")
        physical[head], current[head] = [], set()
        for offset in range(0, len(raw), 37):
            vid, tag = struct.unpack_from("<iB", raw, offset)
            require(0 <= vid < len(mapping), "Posting refers to unknown native VID")
            vector = raw[offset + 5 : offset + 37]
            require(
                vector == struct.pack("<8f", *mapping[vid]["row"]["embedding"]), "Posting vector/VID mapping changed"
            )
            physical[head].append((vid, tag))
            if tag == versions[vid] and vid in live:
                require(vid not in current[head], "Duplicate current-version VID in one posting")
                current[head].add(vid)
            else:
                stale += 1
        if head in active:
            require(recorded[head] == len(physical[head]), "Persisted posting-size record differs from raw RocksDB")
        else:
            require(recorded[head] == 0, "Retired head retains a live size")
    require(active <= set(physical), "An active head has no RocksDB posting")
    covered = set().union(*(current[h] for h in active))
    require(covered == live, "LIRE lost current-version posting coverage")
    for vid in sorted(live):
        vector = mapping[vid]["row"]["embedding"]
        distances = {h: sum((float(v) - float(c)) ** 2 for v, c in zip(vector, heads[h])) for h in active}
        nearest = min(distances.values())
        require(
            any(vid in current[h] for h, d in distances.items() if d == nearest),
            f"Nearest-partition assignment violated: vid={vid}, nearest={[h for h,d in distances.items() if d==nearest]}, assigned={[h for h in active if vid in current[h]]}",
        )
    return {
        "head_slots": count,
        "active_heads": len(active),
        "retired_heads": retired,
        "live_vids": len(live),
        "version_changes": sum(v not in (0xFF, 0xFE) for v in versions),
        "stale_records": stale,
        "current_postings": {str(h): sorted(current[h]) for h in sorted(active)},
        "head_vectors": {str(h): list(heads[h]) for h in sorted(active)},
        "native_versions": list(versions),
    }
