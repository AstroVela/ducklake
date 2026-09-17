# DuckLake Extension for Vane

[Project overview](README.md) · [DuckDB guide](DUCKDB_README.md)

Create, modify, and query DuckLake tables through Vane's SQL and Python Relation
APIs. Supported data operations use the default Ray runner. Leave `VANE_RUNNER`
unset; no runner-selection call is needed.

## Install a provider package

Install `vane-extension-ducklake` with its exact matching `vane-ai` and
`vane-extension-sqlite-scanner` dependencies. Use the same wheels on the
application, Ray coordinator, and workers. Provider versions include an
artifact identity and differ from the base runtime version.

The following development-channel recipe downloads only the three matching
artifacts from TestPyPI, then installs ordinary dependencies from PyPI. Replace
the placeholders with published matching versions for your interpreter and
platform, and use a fresh wheel directory:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
DUCKLAKE_VERSION='<provider-version>'
SQLITE_VERSION='<matching-sqlite-provider-version>'
VANE_VERSION='<matching-vane-version>'
python -m pip download --no-deps --only-binary=:all: \
  --index-url https://test.pypi.org/simple/ --dest ducklake-wheels \
  "vane-extension-ducklake==$DUCKLAKE_VERSION" \
  "vane-extension-sqlite-scanner==$SQLITE_VERSION" "vane-ai==$VANE_VERSION"
python -m pip install --index-url https://pypi.org/simple/ \
  ./ducklake-wheels/*.whl grpcio
python -m pip check
```

See the [provider release guide](docs/vane-provider-release.md) for package
identities and release channels. Load the installed provider:

```python
import vane
from vane import col

connection = vane.connect()
vane.load_installed_extension("ducklake", connection=connection)
```

The provider also loads its SQLite dependency. No `set_runner_ray()` or
`ray.init()` is needed in the application. Vane initializes Ray when required.
To connect to an existing cluster, supply `RAY_ADDRESS` before starting Python.

## Usage

Run these blocks in order in one Python process from a fresh working directory.
The example needs no cloud service or downloaded data. It uses multiple Ray
processes on one physical host, sharing local absolute paths.

### Prepare the catalog

```python
from pathlib import Path

metadata_path = str(Path("ducklake_demo.sqlite").resolve())
data_path = str(Path("ducklake_demo_data").resolve())

def sql_string(value):
    return "'" + value.replace("'", "''") + "'"

connection.execute(
    f"ATTACH {sql_string('ducklake:sqlite:' + metadata_path)} AS lake "
    f"(DATA_PATH {sql_string(data_path)}, DATA_INLINING_ROW_LIMIT 0)"
)
```

`ATTACH` registers the catalog on the client connection. SQLite permits the
client and Ray coordinating actor to open the metadata file across processes
on this host. A writable DuckDB metadata file cannot provide that access.
`DATA_INLINING_ROW_LIMIT 0` makes writes file-backed, as required by the
distributed scan contract. DuckLake creates its storage; no Python `mkdir` is
needed. Absolute paths avoid different actor working directories.

### 1. Create a table with CTAS

CTAS means **CREATE TABLE AS SELECT**: the query provides both the schema and
initial rows. `.create()` is its Relation API form:

```python
connection.sql("""
    SELECT i::BIGINT AS id, ('value-' || i::VARCHAR)::VARCHAR AS payload
    FROM range(1000) AS source(i)
""").create("lake.events")
```

This creates a table with 1,000 rows. A schema-only `CREATE TABLE` just defines
an empty table; CTAS also dispatches the input query and data writers to Ray.
The coordinator prepares catalog state and commits the selected worker files.

### 2. Insert, update, delete, and merge

Append ten rows:

```python
connection.sql("""
    SELECT i::BIGINT AS id, ('value-' || i::VARCHAR)::VARCHAR AS payload
    FROM range(1000, 1010) AS source(i)
""").insert_into("lake.events")
```

Update the first five rows, then delete IDs 1005 through 1009:

```python
connection.table("lake.events").update(
    {"payload": vane.SQLExpression("'updated'")},
    condition=col("id") < 5,
)
connection.table("lake.events").delete(condition=col("id") >= 1005)
```

Merge a small source relation: update ID 0 and insert ID 1010.

```python
connection.sql("""
    SELECT * FROM (VALUES
        (0::BIGINT, 'merged'::VARCHAR),
        (1010::BIGINT, 'new'::VARCHAR)
    ) AS source(id, payload)
""").merge_into(
    "lake.events",
    "target.id = source.id",
    [
        "WHEN MATCHED THEN UPDATE SET payload = source.payload",
        "WHEN NOT MATCHED THEN INSERT (id, payload) VALUES (source.id, source.payload)",
    ],
)
```

These writes use Ray with coordinator-side commits. Keep the connection in
auto-commit mode; do not wrap distributed operations in an explicit transaction.

### 3. Query with SQL

```python
connection.sql("""
    SELECT count(*) AS rows, sum(id)::BIGINT AS id_sum FROM lake.events
""").show()
# rows = 1006, id_sum = 505520

print(connection.sql("""
    SELECT id, payload FROM lake.events WHERE id < 5
""").fetchall())
# ID 0 is 'merged'; IDs 1 through 4 are 'updated'.
```

Use `.show()` for display and `.fetchall()` when application code needs Python
rows. Both execute supported data relations on Ray. Row order is unspecified
without ORDER BY. The bounded detail previews use `.fetchall()` because
`.show()` adds a limit that can trigger a batch-index error in the tested
runtime; aggregate displays above are supported.

### Use the Relation API

```python
events = connection.table("lake.events")
filtered = events.filter(col("id") >= 100).select(col("id"), col("payload"))
print(filtered.filter(col("id") < 105).fetchall())
filtered.aggregate("count(*) AS rows, sum(id)::BIGINT AS id_sum").show()
# rows = 906, id_sum = 500570
```

Relations are lazy. The preview filter does not modify `filtered`; its aggregate
still sees all 906 matching rows. Cast sums to `BIGINT` for a standard Arrow
integer result. A derived relation can also be written with `.create()` or
`.insert_into()`.

### 4. Read a historical snapshot

The first CTAS creates snapshot 1 in this fresh catalog. Later mutations do
not change its rows:

```python
connection.sql("""
    SELECT count(*) AS rows, sum(id)::BIGINT AS id_sum
    FROM lake.events AT (VERSION => 1)
""").show()
# rows = 1000, id_sum = 499500
```

This historical data scan also uses Ray. Retain the snapshot and its data
files while queries are running. For other native DuckLake features, see the
[DuckDB guide](DUCKDB_README.md); they are not all part of the distributed contract.

## Execution and storage boundaries

| Operation | Execution |
| --- | --- |
| Provider loading and ATTACH | Client connection initialization |
| File-backed current and historical scans | Ray workers |
| CTAS, INSERT, UPDATE, DELETE, MERGE | Ray execution with coordinator-side catalog commit |
| Schema-only DDL | Catalog coordination, not a parallel data-writing job |

The default runner does not make catalog registration or metadata commits
parallel worker operations. The pinned runtime also dispatches supported SQL
data statements through `execute()`; the Relation API above makes composition
explicit without selecting a runner.

Distributed scans support committed, unencrypted Parquet data and committed
delete state. Inlined data, transaction-local changes, encrypted data, change
scans, and legacy Parquet VARIANT decoding are rejected before workers run.
There is no local fallback. Small scans may have fewer useful splits than
available workers.

The SQLite example is qualified across processes on **one physical host**.
For multiple hosts, use a metadata backend suitable for that deployment and
make data paths accessible to every worker. Do not assume that putting SQLite
on a network filesystem provides a supported distributed metadata service.
See the [integration notes](docs/vane.md) for catalog access constraints.

## Build from source

Ordinary Make targets build for DuckDB. Vane targets use the exact source
identities in [vane-extension.toml](vane-extension.toml):

```bash
git clone --branch v1.5-variegata_vane --recurse-submodules \
  https://github.com/AstroVela/ducklake.git
cd ducklake
make vane_validate
VCPKG_TOOLCHAIN_PATH='<vcpkg>/scripts/buildsystems/vcpkg.cmake' make vane_ci
VCPKG_TOOLCHAIN_PATH='<vcpkg>/scripts/buildsystems/vcpkg.cmake' make vane_wheel
```

These commands require the toolchain described by the
[shared build tools](https://github.com/AstroVela/vane-extension-ci-tools).
The static wheel is a separate installation path from the provider recipe.
Use non-editable installs. See [provider releases](docs/vane-provider-release.md)
and the [Ray integration suite](test/vane/test_vane_wheel_ray_ducklake.py)
for artifact qualification and broader mutation/failure coverage.

## Tested examples

All nine Python blocks above were executed sequentially on 2026-09-17 with
Python 3.12, non-editable provider wheels, and `vane-ai==0.2.0.dev660` from
Vane revision `4e12994a2fed5b872a7bdb44df72c1b9c5653cdc`. The DuckLake native
sources matched this branch at `b372e423`; the matching SQLite provider was
installed as well.

The test left `VANE_RUNNER` unset, asserted the default Ray runner, and used
an owned cluster with two CPU execution nodes on one physical host. All blocks
passed in 71.60 seconds, with five Ray writes and nine Ray reads including
additional assertions. Checks compared all final rows after the mutations,
both aggregate totals, and every row of the original historical snapshot.

This validates the local provider walkthrough, not cloud storage, multi-host
metadata access, or the source-build recipe. Installation used matching local
wheels; replace the TestPyPI placeholders with published versions.
