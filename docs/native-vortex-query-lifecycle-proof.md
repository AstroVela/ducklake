# Native query and database ownership

This validation follows [DuckLake #38](https://github.com/AstroVela/ducklake/pull/38).
It closes the query teardown qualification gap recorded by that draft and pins
the Vortex fixes through the native DuckDB-Vortex reader and shared query/probe
dependencies. It adds no compatibility path or backend substitution.

The dependency changes are [Vortex #28](https://github.com/AstroVela/vortex/pull/28)
at `65dab44f86495d35bb46f69c8ec5cfc7a3544c1b` and
[DuckDB-Vortex #37](https://github.com/AstroVela/duckdb-vortex/pull/37) at
`b819cc031be61efabeedd6ded0a814984dc56d86`.

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
  drops. Normal completion and panic propagation keep their existing behavior.
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
Its ownership probe is C++ only and does not poll Vortex. After each operation,
database references must return to the owner database/connection pair, with one
registered connection and no active transaction. Private query leases must also
be gone. It alternates one/two DuckDB threads and records RSS as an allocator/cache
observation; live ownership is the hard gate.

The fixture now applies the database weak-reference shutdown check to every
session. Its `reopen` command destroys the old database and verifies release
before constructing another database in the same process. This prevents a new
Vortex operation from concealing unfinished teardown in the global runtime.
The proof repeats this with an installed optimizer and after cancellation.
An active native operation may finish while owning its connection; teardown
checks wait for those owners without issuing another Vortex operation.

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
the same C++ fixture and shared query archive for both binaries; only the native
DuckDB-Vortex reader archive changed. The old archive retained three database
references after capture, against a baseline of two, and exited with the strict
close failure. The fixed archive returned to two after every check and passed
20 mixed-query cycles, eight expected rejections and seven same-process reopens.
All 27 measured fixture recalls were 1.0. Neither the owner probe nor the close
wait drives Vortex to clear pending work.

| Binary | SHA-256 |
| --- | --- |
| Old native reader with final query archive | `f40aa72fe934a7e565d48bc74e6317fd54ea313702d4959c7bab8947a96424a1` |
| Fixed reader and query archive | `8635a84139a7f754ed6f73e82f2544592ca0beca8f9a6e6a78941ea7d7b5073e` |

Both ON and OFF builds passed 86 base SQL cases (3,582 assertions; 13 optional
dependency skips) and Parquet crash recovery. ON also passed ten Vortex SQL
cases (400 assertions), Vortex crash recovery, the index, retained read-view,
mixed search, admission, catalog/GC, SQL search, native build and background
optimizer proofs. The shared query/probe workspace passed all-target/all-feature
Clippy with warnings denied. The pinned Vortex revision passed 290 I/O/file
tests, 160 index/SPFresh tests and three index doctests; one existing file
doctest is ignored.

The final isolated cost run also passed four cases with nine queries each at
4,096/32,768 base rows, with and without a category prefilter. Its projection,
binary handoff and private-lease gates passed; timings remain informational.

The existing local Vortex/native DuckDB-metadata scope and source/vector/artifact
budgets still apply. This qualifies ownership for these cases; writer backpressure,
distributed optimizer ownership and production capacity remain separate work.
