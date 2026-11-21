# PostgreSQL realtime vector proof

This draft extends the native proof to a PostgreSQL metadata catalog. Each
writer/query invocation starts a separate native DuckDB process. Another
process holds a read transaction while writes and inline materialization
commit against the same catalog.

Run it against a dedicated test database after building the repository with
`ENABLE_POSTGRES_SCANNER=ON` and making that matching scanner artifact available
to the CLI:

```sh
python3 -I test/vane/test_postgres_realtime_vector.py \
  --duckdb build/release/duckdb --dsn 'dbname=ducklake_vector_proof'
```

Use libpq environment variables or a service definition for credentials. An
explicit `--postgres-extension /absolute/path/postgres_scanner.duckdb_extension`
selects a local scanner artifact and requires a CLI that has not already loaded
that scanner. `--extension` similarly selects DuckLake and requires a matching
CLI without preloaded DuckLake. Every process checks the selected mode before
attaching a catalog; an invalid same-name artifact cannot be hidden by an
already loaded extension. Omitting `--extension` requires statically linked
DuckLake; omitting `--postgres-extension` loads the scanner already built or
installed for that CLI. Automatic installation and loading remain disabled.

The real-CLI selection regression runs without a PostgreSQL connection:

```sh
python3 -I test/vane/test_postgres_realtime_vector_loading.py \
  --duckdb build/release/duckdb \
  --postgres-extension /absolute/path/postgres_scanner.duckdb_extension
```

The probe creates a unique `vector_proof_<UUID>` metadata schema and temporary
data directory. It removes only that schema on exit and never resets the
database's public schema. The DBMS Catalog Tests workflow runs it before the
existing PostgreSQL SQL suite, using the already provisioned service and built
scanner. The native DuckDB-catalog proof remains independently runnable.

## Native nested NOT NULL

The unpatched fork accepts a top-level NULL for `embedding FLOAT[] NOT NULL`
through both inline and file-backed writes. Native writes now check each
constrained root vector's validity mask before the inline buffer or Parquet
writer accepts it. UPDATE validates the replacement row before staging its
deletion, and MERGE INSERT validates its target row before copying.

The original two SQL regressions retain the required rejection. A broader
regression covers reordered/defaulted inputs, selected/batched inserts,
UPDATE, MERGE UPDATE/INSERT, reopen, and both storage paths:

```sh
build/release/test/unittest \
  'test/sql/vane/native_nested_not_null_writes.test,test/sql/vane/native_vector_not_null_*' \
  --require ducklake --require parquet
```

The PostgreSQL probe also rejects NULL vector inserts, updates, and both MERGE
actions in fresh clients with inlining limits 100 and 0. Empty lists and lists
with NULL children remain valid non-null root values; non-null structs/maps
with NULL children are also valid. Failed writes preserve the existing rows.

## Assertions

- `FLOAT[]`, fractional and negative components, quoted Unicode business text,
  and request records round-trip through the PostgreSQL inline representation.
- Rows report an inlined filename and no Parquet file exists before flush.
- A fresh client sees an atomic vector update, category change, deletion, and
  insertion. Complete vectors, category, payload, row membership, and request
  hashes are checked after each mutation, flush, retry, rollback, and client
  termination. Top-k is checked independently so filtering cannot hide a
  partial update.
- A separate reader pins its snapshot before the writer commits and remains
  readable after inline materialization. It checks complete rows and request
  hashes as well as top-k, preserving fractional distances in comparisons.
  Explicit historical reads agree.
- New inlined vectors combine with materialized data and a deletion from an
  existing file. Retrying an old request cannot resurrect the deleted vector.
- A failed batch rolls back its business changes and request record together.
- Client termination after commit preserves the vector and request record;
  termination before commit rolls the entire batch back.

## Interpretation

This validates several native processes sharing one PostgreSQL catalog and
local data files on one machine. It does not establish multi-host data-file
access, Ray inlined reads, or catalog-server failover. The process termination
cases kill the DuckDB client, not the PostgreSQL server.

Writes remain serialized. Request records use the same guarded SQL recipe as
the native baseline and do not enforce PRIMARY KEY/UNIQUE or provide a public
ingestion API. Commit-response loss and failure during catalog commit require
the [COMMIT outcome fault proof](postgres-commit-outcome-proof.md). The [baseline guide](native-realtime-vector-proof.md)
records the remaining Vortex, ANN, and table-format adoption gates.
