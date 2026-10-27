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
selects a local scanner artifact. `--extension` similarly selects DuckLake;
otherwise it must be statically linked or already installed for that CLI.
Automatic extension installation and loading remain disabled.

The probe creates a unique `vector_proof_<UUID>` metadata schema and temporary
data directory. It removes only that schema on exit and never resets the
database's public schema. The DBMS Catalog Tests workflow runs it before the
existing PostgreSQL SQL suite, using the already provisioned service and built
scanner. The native DuckDB-catalog proof remains independently runnable.

## Observed blocker: vector NOT NULL

The fork currently accepts `INSERT INTO items VALUES (1, NULL)` for an
`embedding FLOAT[] NOT NULL` column. `DESCRIBE` still reports the column as
non-nullable, and a fresh process reads the persisted null vector. This
reproduces with both DuckDB and PostgreSQL catalogs and with inlining limits
100 and 0 (inlined and file-backed writes).

Two SQL regressions retain the required behavior and intentionally fail on
the current implementation:

```sh
build/release/test/unittest 'test/sql/vane/native_vector_not_null_*' --require ducklake
```

The PostgreSQL transaction probe uses a scalar category NOT NULL violation
to independently qualify rollback. That passing scenario does not resolve
the nested-vector constraint defect. This draft stays unmerged while the
failing regressions and adoption decision are reviewed.

## Assertions

- `FLOAT[]`, fractional and negative components, quoted Unicode business text,
  and request records round-trip through the PostgreSQL inline representation.
- Rows report an inlined filename and no Parquet file exists before flush.
- A fresh client sees an atomic vector update, category change, deletion, and
  insertion. Old matching vectors do not leak through filtering/top-k.
- A separate reader pins its snapshot before the writer commits and remains
  readable after inline materialization. Explicit historical reads agree.
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
