# PostgreSQL COMMIT outcome proof

This validation draft checks the gap between a catalog commit and the
client receiving its successful response. It builds on the native and
PostgreSQL proofs through feature branches; neither prerequisite needs to
be merged to run it.

## Fault boundaries

The test-only proxy decodes PostgreSQL protocol 3 messages. A writer first
prepares vector updates, a deletion, an insertion, and a request record in
one DuckLake transaction. An external file handshake confirms that the
writer is waiting before COMMIT. Only then is the fault armed.

| Mode | Transport action | Required independently observed state |
| --- | --- | --- |
| `before_forward` | Close the writer's proxy connections before forwarding its COMMIT message. | Original vectors remain, the request record is absent, and the client cannot report success. |
| `ack_lost` | Forward COMMIT, withhold replies, require CommandComplete `COMMIT` and ReadyForQuery `I`, then close the writer's connections. | Updated vectors and their business columns are readable, deleted rows stay hidden, and exactly one request record exists. |

The [PostgreSQL message specification](https://www.postgresql.org/docs/15/protocol-message-formats.html)
defines these Query, CommandComplete, and ReadyForQuery frames. Sleep duration
does not select the failure window. The probe fails if it does not observe
exactly one intended COMMIT interception, or if the backend's completed
transaction status does not match the injected mode.

Each boundary runs with inlining limit 100 and with inlining disabled (0).
The file-backed cases query the actual vector files after the lost response,
so catalog-only success is insufficient. A fresh process connects directly
to inspect the outcome, then retries the same guarded request twice. The
probe verifies the full vector/business rows, request cardinality, exact
retrieval, and the original historical snapshot.

## Run

Use a dedicated PostgreSQL test database and the matching native DuckDB,
DuckLake, and PostgreSQL scanner builds:

```sh
python3 -I test/vane/test_postgres_commit_outcome.py \
  --duckdb build/release/duckdb --dsn 'dbname=ducklake_vector_proof' \
  --upstream-host 127.0.0.1 --upstream-port 5432 \
  --report build/postgres-commit-outcomes.json
```

`--upstream-host` also accepts a Unix socket directory. Credentials should
come from the libpq environment/service configuration. Explicit
`--postgres-extension` and `--extension` select local matching artifacts.
The Catalogs workflow runs this proof after its PostgreSQL vector probe.

The proxy listens on an ephemeral localhost port. Its client route explicitly
disables TLS/GSS negotiation to inspect protocol frames on the dedicated test
connection. It is a fault-injection fixture, not a production proxy or a
change to production connection security. It closes only connections that
it created and removes only the invocation's generated metadata schema.

## Interpretation

### Observed result on the current fork

The four-case local matrix produced these results with PostgreSQL 15.13 and
the fork's pinned native engine and scanner:

| Storage | Before forwarding COMMIT | Successful response lost |
| --- | --- | --- |
| Inlined (100) | Pass | Pass |
| File-backed (0) | Pass | **Fail: committed files disappear** |

The failing case was independently reproduced. PostgreSQL sent CommandComplete
`COMMIT` and ReadyForQuery `I`; the proxy withheld those responses and the
client exited with an error. Before the fault, six Parquet files existed;
after client exit, only two remained. Four newly written files had been
removed, including three files still referenced by live data-file metadata
at snapshot 4. A fresh direct client failed with `Cannot open file` when
reading the committed vector table.

The client's rollback path calls file cleanup after a commit error; the
current implementation does not first establish whether the catalog commit
actually succeeded. The source paths are
[transaction rollback](../src/storage/ducklake_transaction.cpp) and
[transaction file cleanup](../src/storage/ducklake_transaction_state.cpp).
This draft preserves the failing assertion and records the vanished files
and their live catalog references in the JSON report.

Reproduce only this blocking case with the same arguments above plus:

```sh
--row-limit 0 --mode ack_lost
```

This result blocks accepting the current fork's file-backed uncertain-commit
recovery contract. Fixing the cleanup protocol and rerunning the proof are
separate work; passing the inline case does not qualify file-backed writes.

The JSON report preserves each injection boundary, the client's exit status,
and whether all state/retry/history checks passed. All four cases run even
if one fails, so inline and file-backed behavior can be compared. A failure
keeps its required assertion and is reported as a qualification blocker.

These tests use serialized writers and local data files. They do not qualify
server failover, disk/power loss, long network partitions, or multi-host file
access. The PostgreSQL service's own durability settings must be qualified
separately; the local run uses fsync and synchronous_commit enabled.

The [PostgreSQL vector proof](postgres-realtime-vector-proof.md) records the
independently reproduced nested-vector NOT NULL defect. This draft retains
those failing regressions and does not change primary-table ownership,
introduce a source WAL, or enable Vortex/ANN/Ray integration.
