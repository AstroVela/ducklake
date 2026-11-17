# Native write admission and backlog budgets

This proof follows [DuckLake #39](https://github.com/AstroVela/ducklake/pull/39)
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
limits. The bound UUID resolves the current table identity after a legal rename;
reconfiguration and reopen preserve the same quota. Quotas remain active when
the policy is disabled; explicit optimizer resume retains its existing recovery
semantics.
The feature requires native DuckDB metadata and the four business columns.
Catalog initialization creates the quota table; no catalog migration is added.

## Commit boundary and capacity

Native growth commits acquire a database-owned admission reservation before
taking the publication shared lock and reading current committed metadata. Both
locks are held through the actual commit, so transactions with older snapshots
cannot all consume the same remaining slot. Coverage reads and write projection
cannot cross an index publication; configuration uses the same
admission-before-publication order.
The reservation is a short serialized check/commit section; it does not hold a
metadata transaction while waiting for an optimizer build. There is no durable
reservation counter to replay after a crash. New connections reconstruct usage
from committed files and inline versions.

Admission projects every changed protected table before committing any of the
transaction. It includes new files, inline rows and business bytes, subtracts
inline versions replaced by the transaction, and accounts for flush cutoffs
while preserving later concurrent inline writes. Whole-file deletions and final
table drops use the transaction's net occupancy, so replacing a full unindexed
file at its row limit does not charge both the old and new files. Visible and
retained inline
sources resolve business and vector columns by their stable field IDs, including
historical schemas with renamed columns. Unindexed compaction replaces
its source charge; replacing a covered B source is rejected rather than leaving
the selected index stale. A legitimate zero-output compaction subtracts its
replaced sources without charging a nonexistent output file. Other tables retain
their existing write semantics.

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

## Failure cleanup

The new rejection path exposed an existing transaction-manager leak: DuckDB
marks a failed commit as rolled back, but DuckLake retained the failed
transaction and its metadata connection. The manager now rolls back and erases
that transaction before returning its error. Cleanup errors are merged with the
original error. Existing `NOT_COMMITTED`/`UNKNOWN`/`COMMITTED` file-retention
rules continue determining which files can be removed; unknown or committed
files are preserved. Rollback removes newly created compaction outputs and
their delete files, as well as ordinary insert/delete artifacts; it keeps the
original input files. Admission/configuration metadata transactions also roll
back explicitly on failure. Configuration checks the original caller's
interruption after acquiring its locks and before committing the independent
metadata connection, so cancellation while waiting preserves the old quota.

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

Additional regressions pause a real publication after validation while an
already-prepared compaction commits, require the stale compaction to abort and
clean its output, and preserve the published index and later writes. Three
rejected indexed-source compactions plus explicit rollback leave exactly the
original source files across reopen. A fully deleted historical flush rewrites
to zero output and releases F capacity. Table rename/reconfiguration/reopen and
historical inline column renames exercise stable identity accounting. Whole-file
delete/append and update at a full F limit, insert-then-drop of a protected table,
and cancelled SQL configuration complete the net-occupancy and cancellation
regressions.

C++ ownership probes must return to the owner database/connection pair and
private query leases must be empty. All fixtures use strict database shutdown.
A minimal SQL test covers configuration, updates, errors, flush, promotion and
the total physical row ceiling. The validation workflow includes the process
proof and the ON/OFF SQL/recovery suites. Distributed admission, producer in-flight memory and
production-scale capacity remain separate work.

Local Linux qualification on 2026-11-17 used DuckDB v1.5.5 (`d8cdaa33`),
functional source `af03f418`, Vortex `b20e34c2`, codec `84cef204`, and SPFresh
`5893eb61` with the pinned Vortex static-only patch. The final catalog driver
SHA-256 is `ba8ad055deffb7fbc36976ff7871dda0875718c6cdc8db864d8dac1987b8ee41`.

All 13 backpressure process cases passed in 18.7 seconds with the unchanged
180-second proof budget. ON passed 20 Vortex SQL cases / 809 assertions. ON and
OFF each passed 86 base cases / 3,582 assertions with 13 optional-dependency
skips. ON deletion inlining passed 18 cases / 1,099 assertions with two
optional-dependency skips; the audit case passed 122 assertions. Vortex/Parquet
recovery, cleanup, index/read-view/mixed admission and catalog/GC proofs passed.
The 32-case FFI ABI matrix passed. Build and optimizer proofs passed with 20
and 12 recall checks; lifecycle passed 20 query cycles and 18 same-process
reopens with 27 recall checks. All measured fixture recalls were 1.0, retaining
the existing 0.8 recall threshold and Float32 tolerances.

Seventeen functional gates kept their existing 180-second budgets. The separate
counter/recall gate kept its existing 600-second outer budget and completed in
11.5 seconds; its four cases / 36 queries are functional observations, without
a performance claim. C++ ownership and strict-shutdown checks passed, and
private query leases were empty. Scoped C++/Python formatting, Rust query/probe
formatting, clippy and builds passed; the Rust workspace has no unit tests, so
the native/ABI proofs provide the behavioral coverage. Unchanged Vortex inputs
retain their hash-qualified #39 results: 310 I/O/file tests, 164 index/SPFresh
tests and three index doctests, with one pre-existing ignored I/O doctest.

Initial review failures and the intermediate snapshot-query parser failure were
retained as controls. The final combination passed without increasing threads,
tolerances, capacity limits or operation deadlines. CI, Windows and stable-load
performance measurements were outside this local qualification.
