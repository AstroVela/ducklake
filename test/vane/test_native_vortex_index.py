#!/usr/bin/env python3
"""Qualify DuckLake snapshots with Vortex's existing static SPFresh index."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path
import subprocess
import time


def quote(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def address(row: dict) -> tuple[int, int]:
    return row["file_id"], row["row_offset"]


def digest(path: Path) -> str:
    checksum = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            checksum.update(block)
    return "sha256:" + checksum.hexdigest()


class Proof:
    def __init__(self, duckdb: Path, probe: Path, root: Path) -> None:
        self.duckdb = duckdb
        self.probe = probe
        self.root = root
        self.processes = 0
        self.negatives = 0
        self.recalls: list[float] = []
        self.checks: list[str] = []

    def run(self, command: list[str], expected: str | None = None) -> str:
        self.processes += 1
        result = subprocess.run(command, text=True, capture_output=True, timeout=180)
        text = result.stdout + result.stderr
        log = self.root / f"process-{self.processes:03d}.log"
        log.write_text(text)
        if expected is None:
            require(
                result.returncode == 0,
                f"exit {result.returncode}: {log}\n{text[-4000:]}",
            )
        else:
            self.negatives += 1
            require(
                result.returncode > 0 and expected.lower() in text.lower(),
                f"expected {expected!r}: {log}\n{text[-4000:]}",
            )
        return result.stdout

    def sql(self, directory: Path, sql: str) -> list[dict]:
        directory.mkdir(parents=True, exist_ok=True)
        prefix = (
            "SET autoinstall_known_extensions=false; SET autoload_known_extensions=false;"
            "LOAD parquet; LOAD ducklake; LOAD vortex; SET threads=1;"
            f"ATTACH {quote('ducklake:' + str(directory / 'metadata.ducklake'))} AS lake "
            f"(DATA_PATH {quote(directory / 'data')}, DATA_INLINING_ROW_LIMIT 0);"
        )
        output = self.run([str(self.duckdb), "-batch", "-bail", "-json", "-c", prefix + sql])
        decoder = json.JSONDecoder()
        result = []
        remaining = output.strip()
        while remaining:
            result, end = decoder.raw_decode(remaining)
            remaining = remaining[end:].lstrip()
        return result

    def create(self, directory: Path, rows: int = 4096, embedding: str | None = None) -> None:
        self.sql(
            directory,
            "CREATE TABLE lake.items(id BIGINT NOT NULL, embedding FLOAT[], category VARCHAR, payload VARCHAR);"
            "CALL ducklake_set_option('lake', 'experimental_data_file_format', 'vortex', table_name => 'items');",
        )
        expression = (
            embedding
            or "[id::FLOAT, " + ",".join(f"((id * {axis + 3}) % {11 + axis})::FLOAT" for axis in range(1, 8)) + "]"
        )
        for parity in (0, 1):
            self.sql(
                directory,
                f"INSERT INTO lake.items SELECT id, {expression}, "
                "CASE WHEN id % 4 = 0 THEN 'B' ELSE 'A' END, '中文-row-' || id "
                f"FROM (SELECT i * 2 + {parity} AS id FROM range({rows // 2}) t(i));",
            )

    def capture(
        self,
        directory: Path,
        name: str,
        snapshot: int | None = None,
        read_vectors: bool = True,
    ) -> tuple[Path, list[dict]]:
        capture, rows = self.capture_rows(directory, snapshot, read_vectors)
        require(
            all(not row["filename"].startswith("ducklake_inlined_data_") for row in rows),
            "inlined rows require the separate mixed-query gate",
        )
        target = self.root / f"{name}.capture.json"
        target.write_text(json.dumps(capture, sort_keys=True, separators=(",", ":")) + "\n")
        return target, rows

    def capture_rows(
        self, directory: Path, snapshot: int | None = None, read_vectors: bool = True
    ) -> tuple[dict, list[dict]]:
        if snapshot is None:
            snapshot = self.sql(directory, "SELECT max(snapshot_id) AS id FROM lake.snapshots();")[0]["id"]
        valid = f"begin_snapshot <= {snapshot} AND (end_snapshot IS NULL OR end_snapshot > {snapshot})"
        table = self.sql(
            directory,
            f"SELECT * FROM __ducklake_metadata_lake.ducklake_table WHERE table_name='items' AND {valid};",
        )[0]
        fields = self.sql(
            directory,
            f"SELECT column_id, column_name FROM __ducklake_metadata_lake.ducklake_column WHERE table_id={table['table_id']} "
            f"AND column_name IN ('id','embedding','category','payload') AND parent_column IS NULL AND {valid};",
        )
        field_ids = {field["column_name"]: field["column_id"] for field in fields}
        field = field_ids["embedding"]
        schema = self.sql(
            directory,
            f"SELECT schema_version FROM lake.snapshots() WHERE snapshot_id={snapshot};",
        )[
            0
        ]["schema_version"]
        files = self.sql(
            directory,
            "SELECT data_file_id, path, path_is_relative, record_count, file_format "
            f"FROM __ducklake_metadata_lake.ducklake_data_file WHERE table_id={table['table_id']} "
            f"AND {valid} ORDER BY data_file_id;",
        )
        schema_entry = self.sql(
            directory,
            "SELECT path, path_is_relative FROM __ducklake_metadata_lake.ducklake_schema "
            f"WHERE schema_id={table['schema_id']} AND {valid};",
        )[0]
        schema_path = Path(schema_entry["path"])
        if schema_entry["path_is_relative"]:
            schema_path = directory / "data" / schema_entry["path"].lstrip("/")
        table_path = Path(table["path"])
        if table["path_is_relative"]:
            table_path = schema_path / table["path"].lstrip("/")
        inventory = []
        by_path = {}
        for file in files:
            require(
                file["file_format"] == "vortex",
                "ANN proof requires actual Vortex files",
            )
            path = Path(file["path"])
            if file["path_is_relative"]:
                path = table_path / file["path"].lstrip("/")
            path = path.resolve(strict=True)
            require(str(path) not in by_path, "duplicate physical file path")
            by_path[str(path)] = file["data_file_id"]
            inventory.append(
                {
                    "id": file["data_file_id"],
                    "uri": str(path),
                    "version": digest(path),
                    "row_count": file["record_count"],
                }
            )
        listed = self.sql(
            directory,
            f"SELECT data_file FROM ducklake_list_files('lake', 'items', snapshot_version => {snapshot});",
        )
        require(
            {str(Path(row["data_file"]).resolve(strict=True)) for row in listed} == set(by_path),
            "catalog identities differ from DuckLake's selected file inventory",
        )
        vector_projection = "embedding" if read_vectors else "NULL::FLOAT[] AS embedding"
        rows = self.sql(
            directory,
            f"SELECT id, {vector_projection}, category, payload, filename, file_row_number AS row_offset, "
            "rowid AS logical_row_id "
            f"FROM lake.items AT (VERSION => {snapshot}) ORDER BY filename, file_row_number;",
        )
        for row in rows:
            if row["filename"].startswith("ducklake_inlined_data_"):
                continue
            filename = str(Path(row["filename"]).resolve(strict=True))
            require(
                filename in by_path,
                "inlined or uncovered rows require the separate mixed-query gate",
            )
            row["file_id"] = by_path[filename]
        capture = {
            "format_version": 1,
            "table_uuid": table["table_uuid"],
            "snapshot_id": snapshot,
            "schema_version": schema,
            "field_id": field,
            "dimension": 8,
            "business_fields": {name: field_ids[name] for name in ("id", "category", "payload")},
            "files": inventory,
            "visible": [
                {"file_id": row["file_id"], "row_offset": row["row_offset"]} for row in rows if "file_id" in row
            ],
        }
        return capture, rows

    def build(self, capture: Path, name: str, expected: str | None = None) -> Path:
        reference = self.root / f"{name}.reference.json"
        self.run([str(self.probe), "build", str(capture), str(reference)], expected)
        require(
            reference.exists() == (expected is None),
            "failed build published an index reference",
        )
        return reference

    def search(
        self,
        capture: Path,
        reference: Path,
        query: list[float],
        k: int = 10,
        allowed: list[dict] | None = None,
        expected: str | None = None,
        exact: bool = False,
    ) -> list[dict]:
        name = f"request-{self.processes + 1:03d}"
        request = self.root / f"{name}.json"
        output = self.root / f"{name}.result.json"
        request.write_text(json.dumps({"query": query, "k": k, "allow": allowed, "exact": exact}))
        self.run(
            [
                str(self.probe),
                "search",
                str(capture),
                str(reference),
                str(request),
                str(output),
            ],
            expected,
        )
        require(output.exists() == (expected is None), "failed search produced a result")
        return json.loads(output.read_text()) if expected is None else []

    def qualify(
        self,
        directory: Path,
        capture: Path,
        reference: Path,
        rows: list[dict],
        query: list[float],
        predicate=lambda row: True,
        k: int = 10,
    ) -> list[int]:
        snapshot = json.loads(capture.read_text())["snapshot_id"]
        current_capture, _ = self.capture(directory, "query-inventory", snapshot=snapshot)
        require(
            current_capture.read_bytes() == capture.read_bytes(),
            "DuckLake snapshot physical inventory changed; the capture expired",
        )
        selected = [row for row in rows if predicate(row)]
        allowed = [{"file_id": row["file_id"], "row_offset": row["row_offset"]} for row in selected]
        hits = self.search(capture, reference, query, k, allowed)
        require(
            len(hits) == min(k, len(selected)),
            "fixture search did not return the expected number of eligible rows",
        )
        lookup = {address(row): row for row in selected}
        require(len({address(hit) for hit in hits}) == len(hits), "duplicate ANN addresses")
        exact = sorted(
            selected,
            key=lambda row: (
                sum((a - b) ** 2 for a, b in zip(row["embedding"], query)),
                address(row),
            ),
        )[:k]
        expected_addresses = {address(row) for row in exact}
        if exact:
            self.recalls.append(len(expected_addresses.intersection(map(address, hits))) / len(exact))
        ids = []
        ordering = []
        for hit in hits:
            require(
                address(hit) in lookup,
                "ANN returned a row outside snapshot visibility or the predicate",
            )
            original = lookup[address(hit)]
            fetched = self.sql(
                directory,
                f"SELECT id, embedding, category, payload FROM lake.items AT (VERSION => {snapshot}) "
                f"WHERE filename={quote(original['filename'])} AND file_row_number={hit['row_offset']};",
            )
            require(
                fetched == [{key: original[key] for key in ("id", "embedding", "category", "payload")}],
                "ranked DuckLake fetch changed the version or business fields",
            )
            require(
                hit["embedding"] == fetched[0]["embedding"],
                "Vortex physical take and DuckLake fetch refer to different vectors",
            )
            distance = sum((a - b) ** 2 for a, b in zip(fetched[0]["embedding"], query))
            require(
                math.isfinite(hit["distance"]) and math.isclose(hit["distance"], distance, rel_tol=1e-5, abs_tol=1e-4),
                "ANN squared-L2 score differs from the fetched vector",
            )
            ordering.append((hit["distance"], address(hit)))
            ids.append(original["id"])
        require(ordering == sorted(ordering), "ANN rank order changed during fetch")
        return ids

    def exercise(self) -> None:
        directory = self.root / "lake"
        self.create(directory)
        capture, rows = self.capture(directory, "original")
        require(
            len(json.loads(capture.read_text())["files"]) == 2,
            "proof requires multiple physical files",
        )
        reference = self.build(capture, "original")
        query = next(row["embedding"] for row in rows if row["id"] == 133)
        baseline = self.qualify(directory, capture, reference, rows, query)
        require(baseline[0] == 133, "self-neighbor missing from the original index")
        self.qualify(
            directory,
            capture,
            reference,
            rows,
            query,
            lambda row: row["category"] == "A",
        )
        restricted = self.qualify(
            directory,
            capture,
            reference,
            rows,
            query,
            lambda row: row["id"] in (1000, 1001),
            k=1,
        )
        require(restricted == [1000], "selective predicate was applied after top-k")
        self.qualify(directory, capture, reference, rows, query, lambda row: False)
        for identifier in (7, 1024, 2049, 4000):
            vector = next(row["embedding"] for row in rows if row["id"] == identifier)
            self.qualify(directory, capture, reference, rows, vector)
        self.checks.append("cross-process build/reopen, multi-file identity, ranked fetch and predicate prefilter")

        self.sql(
            directory,
            "BEGIN; DELETE FROM lake.items WHERE id=134; "
            "UPDATE lake.items SET embedding=[100000,0,0,0,0,0,0,0], category='B', payload='updated' WHERE id=133; "
            f"INSERT INTO lake.items VALUES (4096, {str([133.125] + query[1:])}, 'A', 'new-nearest'); COMMIT;",
        )
        changed, changed_rows = self.capture(directory, "changed")
        self.search(changed, reference, query, expected="snapshot")
        current = self.build(changed, "changed")
        ids = self.qualify(
            directory,
            changed,
            current,
            changed_rows,
            query,
            lambda row: row["category"] == "A",
        )
        require(
            ids[0] == 4096 and 133 not in ids and 134 not in ids,
            "updated/deleted old nearest neighbor survived",
        )
        old = next(row for row in rows if row["id"] == 133)
        fresh = next(row for row in changed_rows if row["id"] == 4096)
        hits = self.search(
            changed,
            current,
            query,
            1,
            [{"file_id": row["file_id"], "row_offset": row["row_offset"]} for row in (old, fresh)],
        )
        require(
            [address(hit) for hit in hits] == [address(fresh)],
            "mandatory deletion exclusion did not override the allow list",
        )
        require(
            self.qualify(directory, capture, reference, rows, query) == baseline,
            "new commits changed the old snapshot index",
        )
        self.checks.append(
            "new snapshot rejects stale generation; rebuilt generation excludes old update/delete versions before top-k"
        )

        self.sql(
            directory,
            "CALL ducklake_rewrite_data_files('lake', delete_threshold => 0); CALL ducklake_merge_adjacent_files('lake');",
        )
        compacted, compacted_rows = self.capture(directory, "compacted")
        require(
            {file["id"] for file in json.loads(compacted.read_text())["files"]}
            != {file["id"] for file in json.loads(changed.read_text())["files"]},
            "compaction did not replace file identities",
        )
        self.search(compacted, current, query, expected="snapshot")
        compacted_index = self.build(compacted, "compacted")
        compacted_ids = self.qualify(
            directory,
            compacted,
            compacted_index,
            compacted_rows,
            query,
            lambda row: row["category"] == "A",
        )
        logical = lambda records: sorted(
            (row["id"], row["embedding"], row["category"], row["payload"]) for row in records
        )
        require(
            logical(compacted_rows) == logical(changed_rows),
            "compaction changed visible logical rows",
        )
        require(
            compacted_ids[0] == ids[0] == 4096,
            "compaction lost the qualified nearest neighbor",
        )
        historical, historical_rows = self.capture(
            directory,
            "historical-refreshed",
            snapshot=json.loads(changed.read_text())["snapshot_id"],
        )
        require(
            logical(historical_rows) == logical(changed_rows),
            "compaction changed historical logical rows",
        )
        require(
            historical.read_bytes() != changed.read_bytes(),
            "history layout change was not exercised",
        )
        self.search(historical, current, query, expected="snapshot")
        historical_index = self.build(historical, "historical-refreshed")
        historical_ids = self.qualify(
            directory,
            historical,
            historical_index,
            historical_rows,
            query,
            lambda row: row["category"] == "A",
        )
        require(
            historical_ids[0] == 4096,
            "refreshed historical generation lost the nearest neighbor",
        )
        self.checks.append(
            "compaction invalidates current and same-version historical layouts; refreshed history keeps logical results"
        )

        capture, reference = compacted, compacted_index
        self.search(capture, reference, [0.0], expected="dimension")
        self.search(capture, reference, query, k=0, expected="nonzero")
        self.search(capture, reference, query, exact=True, expected="approximate")
        self.search(
            capture,
            reference,
            query,
            allowed=[{"file_id": 999999, "row_offset": 0}],
            expected="outside",
        )
        self.run(
            [str(self.probe), "build", str(capture), str(reference)],
            expected="already exists",
        )
        source = Path(json.loads(capture.read_text())["files"][0]["uri"])
        original = source.read_bytes()
        try:
            source.write_bytes(original + b"changed")
            self.search(capture, reference, query, expected="version")
        finally:
            source.write_bytes(original)
        descriptor = json.loads(reference.read_text())
        generation = self.root / descriptor["generation"]["generation"]
        manifest = generation / "manifest.json"
        inventory = json.loads(manifest.read_text())["artifacts"]
        for path in (manifest, generation / "artifacts" / inventory[0]["path"]):
            original = path.read_bytes()
            try:
                path.write_bytes(original + b"changed")
                self.search(capture, reference, query, expected="mismatch")
            finally:
                path.write_bytes(original)
        moved = source.with_suffix(".moved")
        source.rename(moved)
        try:
            self.search(capture, reference, query, expected="No such file")
        finally:
            moved.rename(source)
        self.checks.append(
            "invalid queries, stale coverage, missing/modified files and corrupted artifacts fail explicitly"
        )

        for name, embedding, error in (
            ("wrong-dimension", "[1::FLOAT,2::FLOAT]", "dimension"),
            ("empty-vector", "[]::FLOAT[]", "dimension"),
            ("null-vector", "NULL::FLOAT[]", "NULL lists"),
            ("null-component", "[NULL::FLOAT,0,0,0,0,0,0,0]", "NULL components"),
            ("nan-component", "['NaN'::FLOAT,0,0,0,0,0,0,0]", "finite"),
            ("infinite-component", "['Infinity'::FLOAT,0,0,0,0,0,0,0]", "finite"),
        ):
            bad = self.root / name
            self.create(bad, rows=128, embedding=embedding)
            invalid, _ = self.capture(bad, name, read_vectors=False)
            self.build(invalid, name, expected=error)
        require(
            min(self.recalls) >= 0.8,
            "fixture recall@k fell below its explicit qualification threshold",
        )
        self.checks.append("FLOAT[] admission rejects wrong/empty dimensions, NULLs and non-finite components")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duckdb", type=Path, required=True)
    parser.add_argument("--probe", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    root = args.output_dir.resolve()
    root.mkdir(parents=True, exist_ok=False)
    proof = Proof(args.duckdb.resolve(strict=True), args.probe.resolve(strict=True), root)
    report = {
        "backend": "spfresh.static",
        "vortex_revision": "145f816102d6024a12164e37737ff8864faf5033",
        "duckdb_sha256": digest(proof.duckdb),
        "probe_sha256": digest(proof.probe),
    }
    started = time.monotonic()
    try:
        proof.exercise()
        report["status"] = "passed"
    except Exception as error:
        report.update(status="failed", error=str(error))
        raise
    finally:
        report.update(
            seconds=time.monotonic() - started,
            processes=proof.processes,
            negative_cases=proof.negatives,
            checks=proof.checks,
            recall_at_k=proof.recalls,
        )
        (root / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
