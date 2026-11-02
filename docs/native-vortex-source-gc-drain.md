# Source-only maintenance without empty index collection

The [posting-coverage matrix](native-vortex-posting-coverage.md) reached T = F = 0
for both 32,768-row / 128D inputs, but did not finish source reclamation within
the unchanged 90-second drain deadline. The retained trace showed repeated
transaction deferral while maintenance continued through index-recovery and
collection phases with no index backlog.

## Scheduling change

Each maintenance pass already records its durable build and retired-generation
backlogs. Use those counts to invoke only the applicable index phases. When
only source work remains, skip empty build recovery, build collection and
generation collection. Those APIs each acquire the index collection and
publication locks even when their metadata query returns no candidates.

This avoids waiting for an index capture solely to discover there is no index
work. It also reduces the delay between observing an idle source-transaction
window and entering source collection. Actual index work keeps its existing
ordering and shares the same object and byte budgets with source collection.
The final backlog refresh discovers work created concurrently; a later pass
handles it using its then-current metadata.

Source collection still acquires its publication, transaction-start and read
admission guards and checks for existing native transactions. An old ordinary
transaction still defers snapshot expiration and deletion. Captured ReadViews
retain their private sources and index artifacts. No lock, deletion-ownership
check, coverage check, checksum, age/count policy or crash checkpoint is removed.
The worker does not add retries or an alternate cleanup path.

The native `source-only` regression drains build and generation work first,
then holds the index collection lock exclusively while enabling source
maintenance. Source expiration and deletion must complete before that lock is
released, without entering an empty index collector. The existing held-view,
old-transaction, deletion-intent, cancellation and SIGKILL cases remain part of
the source-retention suite.
The worker reports completion after observing an empty source backlog; the
waiting test thread opens no native transactions that could defer its own GC.

The same regression fixture failed with the preceding maintenance implementation
from `9521627f`: source-only work remained blocked on the held index collection
lock. The test releases that lock and stops the worker before reporting failure.

## Qualification gates

The preceding CI also exposed two validation defects. A one-row build rejection
still expected the old SPFresh error message; it now matches the explicit
65-row minimum introduced by head planning. The mixed-load probe required every
short burst to produce a budget rejection, which depends on the relative speed
of the producer, queries and maintenance on that machine.

Before measurement, each load worker now submits a deterministic 257-row
mutation against the unchanged 256-row T budget. It requires the precise budget
error, no receipt and an unchanged row count. The measured burst still reports
every rejection and adds `burst_reached_write_budget`; zero observed rejections
does not mean that saturation/backpressure performance was exercised. The SQL
budget regression and the paused native backpressure case continue checking
atomic rejection and eventual admission after an explicit state change.

The workload, phase lengths, 90-second drain deadline, readers, query intervals,
recall gates and all production budgets stay unchanged. Full typed query-window
consistency, final rows, receipt replay and retained-file inventory remain hard
checks. Broader ANN quality and long-run liveness are separate acceptance work.

## Local qualification on 2026-11-01

The fixed native regression passed while the index collection lock remained
held; the preceding implementation failed the same fixture. The rebuilt
extension was packaged as a signed optional provider and installed against the
unchanged Vane and Vortex wheels from the posting-coverage qualification.

Both previously failing 32,768-row / 128D cases completed with the original
workload, thresholds, budgets, readers and 90-second drain deadline:

| Input | Previous drain | Fixed drain, seconds | Checked queries | Canary / exploration minimum |
| --- | --- | ---: | ---: | --- |
| automatic | Exceeded 90 seconds | 7.30 | 70 | 1.0 / 1.0 |
| cost | Exceeded 90 seconds | 9.85 | 77 | 0.9 / 1.0 |

Both committed ten measured batches, observed budget rejections during the
burst, stayed within T/F row and byte budgets, checked complete typed results
against committed read windows, and preserved final logical rows under exact
old-receipt replay. Each retained 32,896 physical rows after reclamation: the
remaining dead rows were below the existing compaction threshold. Sources and
index artifacts matched their retained inventories; historical snapshots and
scheduled deletions drained without a manual collector or worker resume.

These are bounded convergence checks, not a production latency or throughput
qualification. The actual number of committed batches depends on foreground
and rewrite costs; short-run timings do not establish long-run liveness.
The preceding small-case recall misses remain separate work.

The expanded CI command also passed all four automatic-input combinations of
4,096 / 32,768 rows and 8D / 128D, checking another 1,125 query results. Every
fixed canary reached 1.0; the 4,096-row exploration minimum was 0.4. The repeated
32,768-row / 128D case drained in 8.24 seconds. CI now retains all four cases:

```sh
VANE_RUNNER=local-fast python -I test/vane/test_vane_vortex_sustained_load.py \
    --fixtures automatic --rows 4096 32768 --dimensions 8 128 --seconds 12 \
    --output-dir /tmp/vane-vortex-source-drain
```

The automatic-flow regression passed 80 query comparisons, four native cases
and four real SIGKILL recovery points. Source retention passed 16 comparisons
and 21 native/recovery cases; the final source-only regression also passed
three isolated repetitions and failed against the preceding implementation.
Keyed writes passed 28 comparisons and 13 cases, append passed 16 comparisons
and seven recovery/fault cases, and the basic installed flow passed 32
comparisons. The nine affected SQL fixtures passed all 223 assertion blocks
through the installed-provider probe; standard SQLLogicTest remains CI.

The Vane base release launcher passed 1,222 non-Ray and 32 Ray tests in separate
processes, with 15 optional-dependency skips. Formatting, Python/YAML syntax,
documentation links and Vane source/native copyleft checks passed. The final
test fixture uses shared observer state and worker completion notification so
failure handling cannot leave a dangling observer or create self-deferring
status transactions.

An additional 512-row diagnostic on the preceding signed provider passed the
new atomic budget precheck, then stopped its optimizer with `Vortex rewrite
requires one nonempty source compaction`. That run is retained as a failed
baseline diagnostic, not counted as a passing load or resolved by this scheduler
change.

The [physical-planning follow-up](native-vortex-rewrite-planning.md) investigates
that retained 512-row failure and serializes plan selection with managed
mutations.
