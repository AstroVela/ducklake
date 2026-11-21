# PostgreSQL COMMIT outcome proof

This proof checks a catalog COMMIT whose successful response may never reach
the client. The branch includes the [file-retention repair](uncertain-commit-file-retention.md)
and requires both the inline and file-backed cases to pass. It builds on the
reviewed PostgreSQL vector proof without requiring its prerequisite branch
to be merged first.

## Fault boundaries

The test-only proxy decodes PostgreSQL protocol 3 messages. A writer prepares
vector updates, a deletion, an insertion, and a request record in one DuckLake
transaction. An external file handshake confirms preparation before the proxy
is armed and the client sends COMMIT.

| Mode | Transport action | Required directly observed state |
| --- | --- | --- |
| `before_forward` | Close the route before forwarding COMMIT. | Original complete rows and request hashes remain; no new catalog snapshot exists. |
| `ack_lost` | Forward COMMIT, withhold replies, require CommandComplete `COMMIT` and ReadyForQuery `I`, then close the route. | Complete updated rows and request hashes are readable at one new snapshot. |

The [PostgreSQL message specification](https://www.postgresql.org/docs/15/protocol-message-formats.html)
defines these Query, CommandComplete, and ReadyForQuery frames. Sleep duration
does not select the failure window. Exactly one intended COMMIT interception
must be observed, with the forwarding and server outcome matching the mode.

Both modes run with inlining limits 100 and 0. In every case the client must
report an unknown outcome, return an error, and retain all Parquet files that
existed before the attempted COMMIT. Losing a response cannot authorize file
cleanup or automatic transaction replay.

## State, history, and retries

Fresh clients connect directly, outside the failed proxy route. The probe
compares the full ordered `id, embedding, category, payload` rows and complete
`request_id, payload_hash` set after initialization, immediately after the
fault and before any retry, and after each of two explicit retries. Expected
hashes are independently computed from the fixture request payloads.

Historical reads compare complete items and request records from the initial
snapshot at the same boundaries. Filtered top-k remains a separate assertion:
a vector with the same distance but different components cannot satisfy the
complete-row comparison, and a request count cannot hide a corrupted hash.

When COMMIT was not forwarded, the first explicit retry creates one snapshot.
When it committed, the first retry is already a no-op. The second retry must
leave the snapshot list unchanged in both modes. Existing serialized-writer
guards and retry budgets are retained.

## Run

Use a dedicated PostgreSQL database and matching native DuckDB, DuckLake, and
PostgreSQL scanner builds:

```sh
python3 -I test/vane/test_postgres_commit_outcome.py \
  --duckdb build/release/duckdb --dsn 'dbname=ducklake_vector_proof' \
  --upstream-host 127.0.0.1 --upstream-port 5432 \
  --report build/postgres-commit-outcomes.json
```

Use `--row-limit 0 --mode ack_lost` to isolate file-backed response loss.
`--upstream-host` also accepts a Unix socket directory. Credentials should
come from the libpq environment or service configuration. Explicit
`--postgres-extension` and `--extension` select matching local artifacts;
their loading rules are documented in the [vector proof](postgres-realtime-vector-proof.md).
Neither explicit selection substitutes an already loaded same-name extension.

The proxy listens on an ephemeral localhost port. Its test route disables
TLS/GSS negotiation so the fixture can inspect protocol frames. It closes
only its own connections. Each case owns a generated metadata schema and
temporary data directory, and cleanup removes only that schema. The Catalogs
workflow runs this proof after the PostgreSQL vector probe.

## Failure evidence

The unpatched baseline deleted four new Parquet files after a successful
server COMMIT whose reply was lost. Three deleted data files remained live
in the catalog at snapshot 4; a fresh process failed with `Cannot open file`.
The original probe's four-case matrix was 3 pass / 1 fail. The repaired
contract requires all four cases to pass, with prepared files retained.

The JSON report records the wire boundary, client exit status, explicit
unknown-outcome result, before/after file lists, and snapshots around retries.
All selected cases run even if one fails. A failure records live data-file
metadata and its references to removed files when those diagnostics succeed.
Loading checks may emit JSON before the final metadata SELECT; diagnostics
use the same strict multi-result decoder as normal queries and validate the
returned row structure.

If COMMIT never reached PostgreSQL, retaining files can leave unreferenced
objects. Reclamation belongs to ordinary catalog-aware orphan maintenance
after resolving the outcome and protecting active work. This proof does not
add immediate GC or automatic reconciliation.

## Interpretation

This qualifies serialized native writers sharing a PostgreSQL catalog and
local files. The native nested NOT NULL repair is inherited from the vector
proof; ordinary known-abort cleanup remains covered by
`native_known_rollback_file_cleanup.test`.

Local qualification uses PostgreSQL 15.13 with fsync and synchronous_commit
enabled. Server failover, power loss, long network partitions, concurrent
uniqueness, multi-host files, distributed artifact cleanup, Quack transport,
Vortex, ANN, and Ray reads remain separate gates. No source WAL or public
ingestion API is added.
