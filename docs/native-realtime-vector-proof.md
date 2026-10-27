# Native realtime vector proof

This draft evaluates the existing DuckLake fork as the transaction and snapshot
owner for realtime vector tables. It uses the native DuckDB path, a local
DuckDB metadata catalog, `FLOAT[]` vectors, business columns, and Parquet files.
Retrieval uses exact `list_distance`, with filtering and deterministic top-k.

The proof exercises committed inlined data before file generation, the same
logical rows after materialization, and recovery after process termination.
The source data remains under DuckLake's transaction protocol throughout.

The [PostgreSQL proof](postgres-realtime-vector-proof.md) separately qualifies
the remote catalog path and independent native client processes.

## Run the proof

Initialize the pinned submodules and build the ordinary extension with the
repository's existing vcpkg setup:

```sh
git submodule update --init --recursive
VCPKG_TOOLCHAIN_PATH='<vcpkg>/scripts/buildsystems/vcpkg.cmake' make release
build/release/test/unittest 'test/sql/vane/native_realtime_vector*' --require ducklake
python3 -I test/vane/test_native_realtime_vector_recovery.py --duckdb build/release/duckdb
```

The SQL tests also run under the existing `test/sql/vane/*` selection in Vane
native CI. The process recovery probe runs after the native build in the DBMS
Catalog Tests workflow. It uses temporary local files, POSIX `SIGKILL`, and the
built CLI without downloads or external services.

To test a loadable artifact against a matching native CLI, pass
`--extension /absolute/path/to/ducklake.duckdb_extension`. The probe disables
automatic extension installation and loading, and explicitly loads Parquet
and the selected DuckLake artifact or statically linked extension.

## Assertions

| Area | Evidence |
| --- | --- |
| Inlined vectors | Rows report an inlined filename and no Parquet data files exist before flush. |
| Committed visibility | A transaction updates a vector and its business columns, deletes a second row, and inserts a third. The next exact query observes the whole batch. |
| Filtering and top-k | An updated row whose category no longer matches cannot reappear through its old matching vector; deleted rows cannot consume result slots. |
| Snapshot isolation | A reader establishes its snapshot before the mutation and remains readable across the writer's commit and inline flush. Explicit historical queries retain the original vectors. |
| Mixed data | Queries combine a newly inlined vector and materialized rows, including deletion of a materialized row. |
| Rollback | Explicit rollback and a failing statement discard the batch and its request record together. |
| Serialized request retry | A guarded write and its request hash commit in the same DuckLake transaction. Retrying an older request cannot overwrite a newer vector. A different payload for the same request is rejected. |
| Reopen | Current vectors, request records, and historical results remain readable after database reopen. |
| Process termination | The probe kills processes after a committed batch, during an uncommitted batch, and after inline flush. Separate processes verify committed data, request records, rollback, and historical vectors. |

For committed and materialized termination points, the process probe asserts
that the metadata WAL exists and is nonempty before sending `SIGKILL`.
It therefore exercises recovery without a clean database shutdown. This
qualifies process failure on the local filesystem; power loss, disk failure,
remote durability, and failure during the catalog commit remain separate
validation work.

## Request ownership

`native_realtime_vector_requests.test` is a single-writer SQL recipe. It keeps
the request record in the same DuckLake catalog and transaction as the vector
changes, and checks the request hash before applying the write. The test uses
explicit fixture payload hashes. A production write API still needs a defined
canonical request encoding and table-scoped identity.

DuckLake does not enforce `PRIMARY KEY` or `UNIQUE` on these tables. Writer
serialization is a precondition of this recipe; the test does not establish a
concurrent-writer uniqueness guarantee. It also does not add a public write
API, dimension validation, or an independent source WAL.

## Adoption gate

Use the results to decide whether DuckLake should become the primary table
format for realtime tables. This draft qualifies only the selected native
DuckDB catalog path. SQLite and PostgreSQL metadata backends need their own
vector, transaction, and recovery qualification.

Follow-up work covers Vortex file writing and reading, stable row identity,
static ANN indexes, index coverage and snapshot filtering, and Vane/Ray reads
of committed inlined rows. The current distributed path continues to reject
inlined data. The [Vane scan contract](vane.md) describes that boundary.

Selecting DuckLake as the source transaction owner would replace the proposed
custom source WAL and Iceberg handoff for these tables. Any Iceberg output
would need an explicit export or materialization protocol; this proof does
not introduce an atomic transaction across the two catalogs.
