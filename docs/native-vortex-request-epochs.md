# Explicit request epochs and receipt retirement

R5 receipt lifecycle work uses deterministic transaction and recovery checks.
R4 A/A, performance A/B and sustained-load qualification stay deferred.

## Contract

Every append/mutation supplies a positive uint64 `request_epoch`, fixed when the
request is created and retained on retries. Identity is
`(table_uuid, request_epoch, request_id)`. New tables start at epoch 1. An exact
retry in the current epoch returns its original receipt; changed content fails.
Earlier epochs fail with `EXPIRED_REQUEST`, including IDs never seen before.
Future epochs fail too. A client must never fetch the latest epoch and relabel
an old request: that explicitly creates a different operation identity.

```sql
SELECT * FROM ducklake_vortex_request_status('lake','main','items','knn');
SELECT * FROM ducklake_mutate_vortex_batch(
    'lake','main','items','knn','request-1',
    [{'id':1,'embedding':[1,0]::FLOAT[],'category':'A','payload':'value'}],
    []::BIGINT[],1::UBIGINT);
SELECT * FROM ducklake_advance_vortex_request_epoch('lake','main','items','knn',1::UBIGINT);
```

Status returns `table_uuid, request_epoch, retained_requests, catalog_requests`
from one native transaction. Advancing compares the expected epoch, deletes only
that table's current-epoch receipts and persists the next epoch in **one native
metadata transaction**. It never changes business rows, index generations or
other tables' receipt windows. Operators advance only after accepting that
current-epoch requests lose their retry window, including pending requests.

Advance returns `table_uuid, previous_epoch, request_epoch, expired_requests`.
One compact row per table retains the current epoch and last transition's expired
count. Retrying the last transition returns the same result even after new
requests commit, without clearing their receipts or advancing again. Once a
later transition commits, older transition retries fail; the status still
identifies the durable current window. uint64 exhaustion fails without reset.

The catalog-wide 100,000-receipt ceiling, per-batch 1–1,000 operations/4 MiB,
existing T/F and source/index budgets, writer serialization, managed ownership,
transaction protection and zero internal commit retries remain. A current
receipt can still be retried at full capacity; explicit retirement releases
logical receipt slots. This does not claim immediate file/WAL shrinkage or an
overall metadata/RSS bound. Epoch rows follow table UUIDs and are not erased by
receipt cleanup; table/reference/WAL lifecycles remain separate R5 work.

## Native layout and API

Fresh initialization creates `ducklake_vortex_write_requests_v2`, keyed by the
three-part request identity, and `ducklake_vortex_request_epochs_v1`. The original
`ducklake_vortex_keyed_tables_v1` ownership registry is retained, so existing
managed tables do not lose ordinary-write protection. Old request layouts are
not migrated or inferred. Missing new tables fail initialization checks; the
old SQL arities are not registered. There is no compatibility mode or fallback.

If a fresh catalog has no epoch row for a table yet, its defined initial state is
epoch 1; the first successful request or advance persists it. This is distinct
from missing lifecycle metadata tables, which always fail initialization checks.
`ducklake_initialize_vortex_writes` explicitly rejects any legacy requests-v1
table before creating the new layout, including pre-managed legacy catalogs.

Use explicit `::UBIGINT` for SQL epoch literals; the function requires the unsigned
64-bit type. Bound Python examples also cast their epoch parameter explicitly.

The new required epoch is the **last positional argument** of both append and
mutation SQL functions, and the last field of their receipt. Payload digest
encoding is unchanged: epoch scopes identity rather than changing content.
The existing fresh-fixture helpers use initial epoch 1; lifecycle probes pass
the recorded epoch explicitly and never auto-upgrade it.

## Validation

The proof must check table-scoped cleanup and bounded catalog capacity; stale,
future, zero and overflowing epochs; old upsert/delete replay; exact last-advance
retry after lost ACK and newer writes; native ordinary-reader isolation; both
writer/advance orders and simultaneous identical advances; cancellation, native
metadata conflict, failure before/after fence staging and actual SIGKILL before
and after commit. Reopen through the installed matching providers and assert
that epoch, receipts and business data recover together. An old managed layout
must remain protected even while new write APIs reject it.

Use CPU 6–11, existing DuckDB/SPFresh thread settings and current budgets. There
is no elapsed-time/throughput acceptance condition. Standard watchdogs detect
test hangs only. Current numerical and B/F/T matrix, append/managed/source
regressions and Vane base checks remain required.

After rebuilding/reinstalling the runtime non-editably and installing both
matching test providers as in the [native build workflow](vane-native-vortex-proof.md),
run the proof with the matching write driver:

```bash
VANE_RUNNER=local-fast python -I test/vane/test_vane_vortex_request_epochs.py \
    --driver /path/to/build/ducklake_vortex_write_driver \
    --cpus 6,7,8,9,10,11 --output-dir /tmp/vortex-request-epochs
```

The [native-provider CI workflow](../.github/workflows/VaneNativeVortex.yml) runs
the same probe using a CPU available on its runner and retains the report,
native/recovery logs and crash checkpoints. This is functional qualification.

## Result

The installed-provider [lifecycle proof](../test/vane/test_vane_vortex_request_epochs.py)
passed **8 public contract groups** and **14 native/fault/recovery cases**:

- Current-epoch exact retry, content mismatch, newer business versions, table
  isolation and unchanged index references; same request ID in a new window is
  a new operation, while seen and unseen earlier IDs fail.
- Exactly 100,000 global receipts still admit existing retries and reject new
  ones. Retiring the target table frees its 99,999 slots while preserving the
  other table's receipt and the existing limit. The bounded fixture synthesizes
  receipts from an actual missing-key delete; it does not claim 100,000 API calls.
- Empty windows, maximum uint64 without wraparound, malformed/NULL epochs, old
  SQL arity, outer transactions, reopen and legacy-layout rejection.
- Both writer/advance orders, eight simultaneous identical advances, ordinary
  reader snapshot isolation, cancellation, real native metadata update conflict,
  failures after receipt pruning/epoch staging and before/after commit.
- Actual SIGKILL with a nonempty metadata WAL after pruning, after epoch staging,
  before commit and after commit. Fresh installed-provider processes recovered
  the epoch and receipts together, preserved business rows, returned the same
  last-advance result and refused unseen old requests without deleting newer
  receipts.

The real conflict fixture uses a changing metadata UPDATE; an earlier no-op
UPDATE was optimized away and was corrected before the accepted complete run.
Elapsed time is observational; none of these checks requires stable host load.
CPU affinity was applied to 6–11 and restored, and thread settings stayed intact.

The four-dimension B/F/T and numerical matrix passed again against the new
required-epoch API: 36 phases, 864 ordinary/diagnostic searches and 56 rejection
checks, minimum fixture recall 1.0. The six existing installed flow groups passed
67 native/recovery cases and 172 oracle comparisons (minimum recall 0.9),
including source retention, automatic maintenance and controlled stall capture.
Six oracle self-checks and 189 SQL assertion blocks passed. The Vane base launcher
passed 1,222 non-Ray and 32 real-Ray tests in separate processes, with 15 optional
skips. These Ray tests qualify the base runtime, not distributed ANN.

Formatting, copyleft, workflow YAML, current-guide Python snippets and relative
document links passed. All eight epoch implementation/test source hashes and
the exact native driver remained unchanged through qualification. The runtime
was reinstalled non-editably using the incremental Release build, then matching
Vortex and DuckLake providers were test-signed, packaged and installed into
`/tmp/ducklake-vane-request-epochs-env-20261106`. Frozen earlier trial environments
and their wheels were preserved. Local installation explicitly replaced both
matching provider wheels while preserving the existing Python dependency set;
this is not a release or clean offline dependency-resolution qualification.

| Evidence | Local path | SHA-256 |
| --- | --- | --- |
| Epoch contract/concurrency/crash proof | `/tmp/ducklake-vortex-request-epochs-v2-20261106/report.json` | `b7491c4ebcef7a0dca98bbb372d17ce7d6c7a0d1f08d16540c6966bb8aaeb277` |
| Required-epoch numerical/B/F/T matrix | `/tmp/ducklake-vortex-epochs-functional-matrix-20261106/report.json` | `14b974857d0299472f3e66f1003da1e766d5f489ffa84ab8aa78f4e5eda147c9` |
| Existing regressions and Vane base | `/tmp/ducklake-vortex-epochs-qualification-20261106/report.json` | `ae91afd1a7b7ff1748b93ad02303af59db60bbfc52e852c6b84ed19a5e4abb0e` |
| Matched provider bundle | `/tmp/ducklake-vortex-epochs-wheels-v2-20261106/report.json` | `159655a3f5a2f6007041f01d751dba8d0c987698066c25f3ac60c0a41c4bf218` |

These deterministic results do not close R4 or establish total disk, metadata,
RSS, throughput or latency bounds. Reference/WAL ownership and lifecycle, and
further capacity admission contracts, remain R5 work.
