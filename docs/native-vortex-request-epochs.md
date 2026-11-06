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
SELECT * FROM ducklake_advance_vortex_request_epoch('lake','main','items','knn',1);
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

## Result

Implementation and deterministic validation are in progress.
