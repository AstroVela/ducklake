# Native query and database ownership

This validation follows [DuckLake #38](https://github.com/AstroVela/ducklake/pull/38).
It closes the query teardown qualification gap recorded there and pins
the Vortex fixes through the native DuckDB-Vortex reader and shared query/probe
dependencies. It adds no compatibility path or backend substitution.

The dependency changes are [Vortex #28](https://github.com/AstroVela/vortex/pull/28)
at `65346aaf1632f37ab889b821135cf669b56110fa` and
[DuckDB-Vortex #37](https://github.com/AstroVela/duckdb-vortex/pull/37) at
`d2ee7fae92655b87c8934b74d5931e294f7cba8b`.

## Retained ownership

The minimal reproduction appends an unindexed Vortex file, commits an inline
update/delete/insert, then captures that mixed table. Database references rise
above the owner connection's baseline even though no additional connection or
transaction remains active. Closing the last connection does not destroy the
database. The same observation was reproduced on #37 without an optimizer;
deallocating a named prepared statement did not remove it.

Two asynchronous ownership boundaries needed correction:

- The last `ThreadSafeIterator` clone previously dropped its driver task without
  driving cancellation. A current-thread executor could retain the task's stream
  and readers until another operation polled it. A shared driver owner now cancels
  and joins that task when its last iterator closes, including concurrent clone
  drops. Cancellation completion includes the tasks released by the driver and
  their descendants, even across executor scheduling batches or worker threads.
  A cleanup queue flattens nested cancellation. Dropping an active task's own
  handle requests cancellation without joining its current poll. Cleanup consumes
  a stored background panic; normal `next()` still propagates the original panic.
  Unrelated self-waking tasks do not prevent owned cleanup from finishing.
  Once a queued join finishes, its completed receipt is removed from the live
  parent. Pending descendant receipts remain available to acknowledge teardown.
- `FileSegmentSource`'s idle request driver also owned its reader. It could keep
  the DuckDB filesystem handle and database alive after a scan completed. The
  source and live request futures now own reader leases; the idle driver holds a
  weak reference and acquires a strong lease during actual I/O. Requests can still
  finish after their source is dropped. Running blocking reads retain their own
  handle until they finish.

The DuckDB file bridge continues protecting its filesystem with database
ownership while its handle is live. The fix releases owners at their actual
lifetime boundary; it does not force-close borrowed handles or pump unrelated
queries to clear them.

## Validation contract

`test_native_vortex_lifecycle.py` checks successful mixed queries, named prepared
execution, early LIMIT, callback failures, interruption and dimension rejection.
Its ownership probe is C++ only and does not poll Vortex. It runs immediately
after each prepared search, search LIMIT, ordinary scan LIMIT and rejected query,
before another Vortex operation can conceal delayed cleanup. After each operation,
database references must return to the owner database/connection pair, with one
registered connection and no active transaction. The writer context must return
to its valid reference baseline, established again after PREPARE and database
reopen. Private query leases must also be gone. It alternates one/two DuckDB
threads and records RSS as an allocator/cache observation; live ownership is the
hard gate.

The fixture now applies the database weak-reference shutdown check to every
session. Its `reopen` command destroys the old database and verifies release
before constructing another database in the same process. This prevents a new
Vortex operation from concealing unfinished teardown in the global runtime.
Direct reopens follow prepared search, both LIMIT endings, every interruption,
callback failure and dimension rejection. The proof also repeats this with an
installed optimizer and after cancellation.
An active native operation may finish while owning its connection; teardown
checks wait for those owners without issuing another Vortex operation. The
probe reads transaction state only for its command-exclusive writer connection.
Other registered contexts prevent the settled condition without reading their
concurrently changing transaction state.

The optional `--baseline-driver` runs the identical minimal scenario with the
old native reader archive. Qualification requires observing the extra owners
and the old binary failing the same strict close check. It then runs the fixed
binary and requires successful ownership recovery and same-process reopens.

Existing SQL search, native construction and optimizer proofs use the same
unconditional close assertion. Their held-view/GC, artifact failure, cancellation,
lost ACK and SIGKILL cases remain in place. A SQL case also checks that early
LIMIT and a dimension error leave prepared mixed search usable before and after
inline flush. CI runs the lifecycle proof explicitly and uploads its report.

```sh
python3 -I test/vane/test_native_vortex_lifecycle.py \
    --driver build/index-shell/ducklake_vortex_catalog_driver \
    --output-dir build/index-logs/query-lifecycle
```

## Local qualification

The DuckDB v1.5.5 (`d8cdaa33fda8df955cc76ef58a280f68f4cd43fa`) run used
the same current C++ fixture, shared query archive and all other link inputs for
both binaries. Only the native DuckDB-Vortex reader archive changed. The old
archive retained an extra database owner after capture and exited with the
strict close failure. The fixed archive returned to two database references in
all 83 ownership observations and passed 20 mixed-query cycles, eight expected
rejections and 18 same-process reopens. All 27 measured fixture recalls were 1.0.
Neither the owner probe nor the close wait drives another Vortex operation.

| Binary | SHA-256 |
| --- | --- |
| Old native reader with final fixture and query archive | `9c851da3326ac847c290574c52636edf0ed4d3f87d799c0ed4cb233cb96823a5` |
| Fixed reader with the same fixture and query archive | `69d0ac392b5fabd93a35bf6cdd2407ca9cb1ee3330c65448f0f9564add123545` |

Both ON and OFF builds passed 86 base SQL cases (3,582 assertions and 13 skips
each) and Parquet crash recovery. ON also passed 19 Vortex SQL cases (773
assertions), 18 deletion-inlining cases (1,099 assertions and two skips), Vortex
crash recovery, cleanup, index, retained read-view, mixed search, admission,
catalog/GC, SQL search, native build and background optimizer proofs. Native
build, lifecycle and optimizer proofs retained their 180-second outer budgets.
The ABI probe passed all 32 inventory, identity, URI, cancellation and guarded
release cases. OFF rejected Vortex format and exposed none of the 11 native
Vortex functions checked by the proof.

The shared query/probe workspace passed formatting, build and all-target,
all-feature Clippy with warnings denied. It currently has no Rust unit cases;
its query behavior is exercised by the native and probe-driven proofs above.
The pinned Vortex revision passed 308 I/O/file tests, including 14 runtime and
four real-file lifecycle regressions, plus 164 index/SPFresh native tests and
three index doctests. One existing file doctest is ignored. Both modified Vortex
crates passed nightly formatting and all-target/all-feature Clippy with warnings
denied. The whole-workspace formatting check reported pre-existing macro
formatting in four unchanged DuckDB/FFI files whose blobs match the parent.

The isolated counters run passed four cases with nine queries each at
4,096/32,768 base rows, with and without a category prefilter. Projection, binary
handoff and private-lease gates passed. Timings remain informational.

An isolated allocation-counter regression keeps one parent alive across
100,000 completed child cancellations and bounds live allocation growth after
warm-up. The unfixed receipt bookkeeping retains about 13.4 MB; the fixed public
counter probe stays around 73 KB after warm-up. The two-worker successful-await
probe stays around 92 KB. These requested-allocation counters use no RSS or
elapsed-time threshold. Native production-scale memory remains unmeasured.

The existing local Vortex/native DuckDB-metadata scope, transaction protection,
source/vector/artifact budgets, thread settings, distance tolerances and recall
requirement still apply. Started blocking I/O keeps its real handle until it
finishes. This qualifies ownership for these cases; writer backpressure,
distributed optimizer ownership and production capacity remain separate work.
