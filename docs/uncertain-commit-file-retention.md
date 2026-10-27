# Files after an uncertain metadata commit

A metadata connection error does not establish that PostgreSQL rolled back.
The COMMIT outcome proof showed the catalog committing successfully while the
client's error handling removed the files that commit referenced.

The repair tracks the metadata commit boundary in the transaction state:

| Outcome | File disposition | Client behavior |
| --- | --- | --- |
| Not committed | A known abort may clean unpublished files. | Existing pre-commit conflict handling and confirmed local rollback apply. |
| Unknown | Retain every prepared file. | Return an explicit unknown-outcome error; do not replay the commit automatically. |
| Committed | Retain committed files, including after a finalization error. | A later error must not be reported as an ordinary aborted write. |

The state becomes unknown immediately before committing the metadata
connection, and committed only after that operation returns successfully.
The same conservative boundary is marked before the Quack staged-commit
RPC; a verified successful response records the committed state.

Native DuckDB can reject a metadata commit because of a concurrent constraint
conflict. Its rollback callback exposes the original typed error after the
engine has rolled back. For a native DuckDB metadata catalog, a confirmed
constraint or transaction abort restores the not-committed state and allows
the existing conflict retry in both client-side and server-side commit loops.
I/O failures and remote commit errors remain unknown; error-message matching
cannot establish whether they committed.

Both the commit-loop cleanup and the outer transaction rollback use the
same guard. A caller therefore cannot erase committed files by rolling
back the local transaction after its successful response was lost.

## Recovery responsibility

An unknown result remains an error. A caller must inspect the catalog from
a new connection and resolve its operation identity before retrying. The
validation recipe uses request records committed atomically with business
changes and serializes writers; it does not enforce concurrent primary-key
uniqueness or add a public ingestion API.

If COMMIT never reached the server, retaining the prepared files can leave
unreferenced objects. That is deliberate until the outcome is resolved.
Reclamation must use the catalog and the normal orphan-file maintenance
rules, with appropriate protection for active work. This patch does not
add durable commit intents, automatic outcome reconciliation, or immediate
garbage collection after an uncertain result.

## Verification

Run the [COMMIT outcome proof](postgres-commit-outcome-proof.md). The repaired
contract additionally requires the client to report an unknown outcome and
requires all files present before COMMIT to remain after that error. Fresh
direct clients still check both actual outcomes and retry behavior.

The native `native_known_rollback_file_cleanup.test` verifies that ordinary
ROLLBACK and a constraint failure before COMMIT still remove unpublished
Parquet files and preserve previously committed rows. It also exercises
concurrent file-backed inserts and verifies the rows and files after reopen.
The original proof branches retain the baseline failures for comparison.

The repair passed all four PostgreSQL fault cases: inline/file-backed writes
crossed with COMMIT not forwarded/COMMIT acknowledged by the server but its
response lost. Neither uncertain case acknowledged success to the client or
removed prepared files. Fresh clients verified the actual rows, historical
snapshots, and repeated request handling. In the file-backed cases all six
prepared/existing Parquet files remained, including the deletion file.

The affected native selection passed 51 test cases and 1,429 assertions,
with seven tests skipped for unavailable optional httpfs/icu/json/tpch
extensions. It includes transactions, concurrent writes, defaults, nested
types, constraints, UPDATE, MERGE, and the Vane native tests. The native
process-recovery probe also passed all three SIGKILL boundaries.

Local protocol qualification uses PostgreSQL 15.13 with fsync and
synchronous_commit enabled, native DuckDB and local files. It does not
qualify server failover, power loss, distributed artifact cleanup, or the
Quack transport at runtime. Those remain separate validation gates.
