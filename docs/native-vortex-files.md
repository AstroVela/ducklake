# Experimental native Vortex files

This DL3a validation draft adds Vortex data files to the DuckLake fork's native
transaction path. The DuckLake catalog continues to own commits, snapshots,
inlined rows, deletes, and recovery. It adds no second source WAL and does not
select DuckLake as the production table format.

The draft is based on the uncertain-COMMIT repair in
[DuckLake #25](https://github.com/AstroVela/ducklake/pull/25). The existing
[native vector proof](native-realtime-vector-proof.md) and
[commit-outcome proof](postgres-commit-outcome-proof.md) describe that baseline.
Ray validation in [#26](https://github.com/AstroVela/ducklake/pull/26) remains a
separate branch and qualification effort.

## Build and use

The default build leaves Vortex support disabled. The optional native build
pins duckdb-vortex `b2253585db8fef4fa490e312841c6e8fa6479fd9`, which pins Vortex
core `ee085f55a705cb9b2edab18d392dd822540774d7`. Build it against the repository's
pinned DuckDB engine (`d8cdaa33fda8df955cc76ef58a280f68f4cd43fa`, v1.5.5), using
the normal vcpkg setup:

```sh
git submodule update --init --recursive
DISABLE_EXTENSIONS_FOR_TEST=1 cmake -S duckdb -B build/vortex-validation -G Ninja \
  -DCMAKE_BUILD_TYPE=Release -DOVERRIDE_GIT_DESCRIBE=v1.5.5 \
  -DCMAKE_TOOLCHAIN_FILE="$VCPKG_TOOLCHAIN_PATH" \
  -DVCPKG_BUILD=ON -DVCPKG_MANIFEST_DIR="$PWD" \
  -DDUCKDB_EXTENSION_CONFIGS="$PWD/extension_config.cmake" \
  -DDUCKLAKE_ENABLE_VORTEX=ON -DRust_TOOLCHAIN=1.91.0 \
  -DBUILD_EXTENSIONS=parquet -DUNITTEST_ROOT_DIRECTORY="$PWD"
cmake --build build/vortex-validation --target duckdb unittest
python3 -I scripts/run_native_vortex_validation.py \
  --duckdb build/vortex-validation/duckdb \
  --unittest build/vortex-validation/test/unittest \
  --mode ON --output-dir build/validation-on
```

The CI workflow runs separate ON and default-OFF builds. OFF qualification
requires an explicit unsupported-capability error when selecting Vortex, then
runs the native Parquet regressions and recovery proof. The validation runner
rejects successful exit codes that ran no SQL tests and retains a JSON report
and each command's output.

Create a table before selecting its experimental output format:

```sql
LOAD parquet;
LOAD ducklake;
LOAD vortex;
ATTACH 'ducklake:metadata.ducklake' AS lake
  (DATA_PATH 'vectors', DATA_INLINING_ROW_LIMIT 100);
CREATE TABLE lake.items(id BIGINT NOT NULL, embedding FLOAT[] NOT NULL, label VARCHAR);
CALL ducklake_set_option('lake', 'experimental_data_file_format', 'vortex', table_name => 'items');
INSERT INTO lake.items VALUES (1, [0, 1, 2], 'one');
SELECT * FROM lake.items;
CALL ducklake_flush_inlined_data('lake');
SELECT id, embedding, filename, file_row_number FROM lake.items;
```

The option requires table scope. Existing Parquet files remain readable beside
new Vortex files. Vortex output cannot be switched back through this option.
Column and table renames retain field identities; other schema changes and
partitioning changes are rejected for these experimental tables.

## File and row contract

Each committed data file records its actual format in
`ducklake_data_file.file_format`. Dispatch uses this catalog value, including
for historical reads and transaction-local files. Unknown formats fail.
Vortex writes use physical column names containing DuckLake field IDs.
Projection and renames therefore do not reinterpret fields by visible name.

The persistent file identity is the catalog's `data_file_id`, scoped to the
table. `file_index` remains local to a scan. `file_row_number` comes from
Vortex's original physical row positions and is not renumbered by filters or
deletes. A rewrite creates new file identities and positions. The normal
DuckLake row-ID and snapshot columns preserve logical identity and historical
visibility through flush and compaction.

The reader uses the native DuckDB filesystem adapter, explicitly projects
physical row positions, and applies DuckLake's existing deletion and row-filter
logic. Flush writes the internal row-ID and snapshot columns with their bounds.
It retains the source catalog's snapshot and delete semantics.

The initial writer emits row counts, byte sizes, and top-level NULL counts.
Complete business-column and nested-column bounds are not implemented.
Column statistics callbacks therefore return unknown for Vortex tables.
Otherwise, merging a later inline delta can produce incomplete bounds and
incorrect integer compression or MIN/MAX results. The optimizer stays enabled;
the mixed-file/delta tests exercise this case explicitly.

## Validation scope

The SQL proof covers real Vortex writes and reopen, a 65,537-row file, original
positions after filtering and deletion, multiple files, prepared projections,
renames, mixed Parquet/Vortex files, NULL/NaN/infinity/signed-zero values, empty
writes, rollback, exact filtered top-k, active readers across flush, historical
snapshots, and compaction. Unsupported options, types and partitioning fail
explicitly. File IDs remain stable until a rewrite replaces their files.

The process-recovery proof also runs with `--file-format vortex`. It kills a
process after a committed inline batch, during an uncommitted batch, and after
flush. Fresh processes verify vectors, business fields, request records,
selected file format, and historical results. The committed boundaries require
a nonempty metadata WAL before SIGKILL.

This is a local native proof with a DuckDB metadata catalog and unencrypted,
unpartitioned tables. Supported business types are BOOLEAN, INTEGER, BIGINT,
FLOAT, DOUBLE and VARCHAR, plus FLOAT[] values. Writes currently use unescaped
local filesystem paths and one serialized output file per statement. File-size
rotation and per-thread-output tuning are not qualified.

FLOAT[] remains a SQL list: nullable lists, empty lists and nullable elements
keep their ordinary SQL meaning where the schema permits them. This draft does
not provide fixed vector dimensions, finite-component admission, or conversion
to the static ANN provider's FixedSizeList contract. Those belong to DL3b.

Static ANN construction and reopen, filtered ANN top-k, index coverage and
publication, production ingestion/idempotency APIs, cloud storage, PostgreSQL
and SQLite catalogs, Ray execution, node loss, server failover, and power loss
remain separate gates. Vane builds reject Vortex data files before transporting
ordinary scan or row-mutation tasks. Default provider manifests and release
configurations do not enable the experimental format.

Local builds reuse native engine and Vortex objects from the exact pins above
and rebuild the DuckLake sources. Clean ON/OFF builds remain a hosted CI gate;
the draft does not claim that an uncompleted hosted run passed.
