# Native write admission and backlog budgets

This draft follows [DuckLake #39](https://github.com/AstroVela/ducklake/pull/39)
and the [original design's resource budgets](https://chatgpt.com/space/page_5ee0f8c698d08191a17198d45d152362).
It adds explicit write quotas to the existing local T/F/B prototype. Writes
that would exceed a quota fail before metadata commit; acknowledged B/F/T stays
in the query plan while indexing is paused, cancelled or recovering.

## Configuration

Initialize the index and optimizer catalogs and install an optimizer policy as
described in the [optimizer proof](native-vortex-optimizer-proof.md), then set:

```sql
SELECT * FROM ducklake_set_vortex_write_budget(
    'lake', 'main', 'items', 'knn', 256, 1048576, 384, 67108864);
SELECT max_inline_rows, max_inline_bytes, max_unindexed_rows, max_unindexed_bytes
FROM ducklake_vortex_optimizer_status('lake');
```

The four limits bound current visible T rows/bytes and physical unindexed F
rows/encoded bytes. T bytes use the existing optimizer accounting: vector float
bytes, 64 bytes per row and UTF-8 business strings. All limits are positive;
T/F row limits cannot exceed 100,000, T bytes cannot exceed 4 MiB, and F bytes
cannot exceed 64 MiB. Unconfigured policies report NULL quota columns.

One durable quota record per table UUID selects the named optimizer/index
policy. Installing or replacing it validates current committed occupancy and
the existing native capacity ceilings. A rejected change preserves the previous
limits. Quotas remain active when the policy is disabled or after reopening the
database; explicit optimizer resume retains its existing recovery semantics.
The feature requires native DuckDB metadata and the four business columns.
Catalog initialization creates the quota table; no catalog migration is added.

## Commit boundary and capacity

Native growth commits acquire a database-owned admission reservation before
reading current committed metadata. It is held through the actual commit, so
transactions with older snapshots cannot all consume the same remaining slot.
The reservation is a short serialized check/commit section; it does not hold a
metadata transaction while waiting for an optimizer build. There is no durable
reservation counter to replay after a crash. New connections reconstruct usage
from committed files and inline versions.

Admission projects every changed protected table before committing any of the
transaction. It includes new files, inline rows and business bytes, subtracts
inline versions replaced by the transaction, and accounts for flush cutoffs
while preserving later concurrent inline writes. Unindexed compaction replaces
its source charge; replacing a covered B source is rejected rather than leaving
the selected index stale. Other tables retain their existing write semantics.

The total native materialization checks also include retained, ended inline
versions: flush writes those historical versions into physical files. Existing
physical rows plus all pending inline versions must fit both 100,000 rows and
the dimension-dependent 64 MiB vector ceiling. Pending inline data, including
historical business strings, is bounded at 64 MiB; encoded physical sources
remain bounded at 64 MiB. Repeated updates cannot evade capacity limits merely
by keeping the latest visible T small. A completed flush removes its pending
inline charge and introduces the actual physical file charge.

Exhaustion returns an explicit `RESOURCE_EXHAUSTED` error. The transaction is
aborted; the client can submit a new transaction after capacity becomes
available. The implementation adds no automatic retry, scan downgrade or
compatibility path. These counters bound logical/encoded data, not resident
memory, the native metadata WAL or SPFresh's internal threads. Promotion frees
F quota but does not expand the prototype's total physical capacity ceiling.
The [multi-segment follow-up](native-vortex-segments-proof.md) counts B coverage
across generations. [Selective merging](native-vortex-segment-merge-proof.md)
reuses three bounded slots while preserving write quotas through merge,
cancellation and recovery. It does not expand total physical capacity.

## Failure cleanup

The new rejection path exposed an existing transaction-manager leak: DuckDB
marks a failed commit as rolled back, but DuckLake retained the failed
transaction and its metadata connection. The manager now rolls back and erases
that transaction before returning its error. Cleanup errors are merged with the
original error. Existing `NOT_COMMITTED`/`UNKNOWN`/`COMMITTED` file-retention
rules continue determining which files can be removed; unknown or committed
files are preserved. Admission/configuration metadata transactions also roll
back explicitly on failure.

## Qualification

```sh
python3 -I test/vane/test_native_vortex_backpressure.py \
    --driver build/index-shell/ducklake_vortex_catalog_driver \
    --output-dir build/index-logs/backpressure
```

The proof races four prepared transactions for one slot, checks rollback,
interruption, failed updates, multi-table atomicity, durable limits after
same-process reopen and SIGKILL, and bounds F while a real background builder
is paused. It verifies current B/F/T search against the visible-row oracle,
keeps quotas through cancellation/reopen, and verifies renewed admission after
publication. Byte rejection must remove rejected physical artifacts. High
dimensional batches fill the 64 MiB vector ceiling exactly and reject the next
row. Retained inline payloads and a table at the total row ceiling exercise
history accounting separately from current-visible quotas.

C++ ownership probes must return to the owner database/connection pair and
private query leases must be empty. All fixtures use strict database shutdown.
A minimal SQL test covers configuration, updates, errors, flush, promotion and
the total physical row ceiling. CI runs the process proof and the ON/OFF
SQL/recovery suites. Distributed admission, producer in-flight memory and
production-scale capacity remain separate work.

The local DuckDB v1.5.5 (`d8cdaa33`) qualification passed all five process cases,
with fixture recall 1.0 before and after backlog recovery. The catalog driver
SHA-256 was `1ebf741a79e7a497be28f11d1f401cf49df22f90cf862a3352bd1a1f04912cb9`.
ON passed 11 Vortex SQL cases (436 assertions) and Vortex crash recovery; ON
and OFF each passed 86 base cases (3,582 assertions; 13 optional-dependency
skips) and Parquet crash recovery. The audit case passed 122 assertions.
Existing native build, catalog/GC, SQL search, optimizer and lifecycle proofs
passed, including 20 query cycles and seven same-process database reopens.
The isolated cost proof passed four cases with nine queries each and recall
1.0; timings remain informational. `make format-fix` completed successfully.
