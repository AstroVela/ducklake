# Actual query snapshots and maintenance stage timing

The [maintenance-fairness validation](native-vortex-maintenance-fairness.md)
left two questions unresolved: one query matched two logical cuts with different
recalls, and source cleanup failed the ninety-second drain. This change records
the query's actual ReadView snapshot and measures the existing worker without
changing its scheduling, budgets or retention policy.

## One query, one captured snapshot

The shared native search core already records `native_capture.snapshot_id` in
its owned diagnostics. The new `ducklake_vortex_search_diagnostics` SQL table
function uses the same binder, search execution and category filter as
`ducklake_vortex_search`. It returns exactly one row with `diagnostics` and a
typed `hits` list, including an empty list when the search has no hits. It does
not issue a second search or recapture a later view. Ordinary search still
returns the same ranked business columns.

```sql
SELECT diagnostics, hits
FROM ducklake_vortex_search_diagnostics(
    'lake', 'main', 'items', 'knn', [133,4,2,0,1,8,7,16]::FLOAT[], 10,
    category => 'B');
```

The sustained-load trace now records `snapshot_id` from that exact invocation.
The independent oracle resolves it to the latest committed mutation receipt at
or before the capture. Physical flush, rewrite and maintenance snapshots between
mutations retain the preceding logical state. The before/after receipt watermarks
remain causality bounds, rather than alternative oracles. Missing/invalid capture
IDs, captures before the committed seed, captures outside the read window and
rows from another cut are rejected. There is no watermark fallback or selection
of a more favorable compatible cut. Canary and exploration still require 0.8.

Regression cases distinguish identical typed hits with recall 0.6 against the
older cut and 1.0 against the newer cut: only the recorded capture decides the
result. They also reject incorrect row versions and missing evidence and retain
a failing gate for genuinely low recall. Installed SQL checks cover typed/ranked
parity, category filters and empty-result captures.

## In-memory worker profile

```sql
SELECT * FROM ducklake_vortex_maintenance_profile('lake');
```

The profile returns fifteen fixed stages with `profile_id`, `stage`, `calls`,
`total_ns` and `max_ns`. It exists for a catalog's current in-process maintenance
registration. Explicit enable/resume starts a new identity and zeroed counters;
before registration or after process restart there are no rows until enable or
resume. It is not a durable maintenance checkpoint or recovery journal.

Reading a profile copies bounded counters under an in-memory mutex. It does not
open a DuckLake metadata transaction or acquire collector admission. There is no
per-operation log, background profiler or persisted metrics table. Timers use
`steady_clock` wall time and retain completed scopes on success, exception and
cancellation. They include lock waits and any observation-hook pauses.

| Stage | Measured work |
| --- | --- |
| `optimizer_total` | Entire automatic optimizer invocation, including idle probes |
| `optimizer_probe` | Plan/compaction probes, including initial writer-gate admission |
| `optimizer_metadata` | Policy loads, task/progress/publication state and finalization |
| `maintenance_total` | Entire maintenance invocation, including no-work probes |
| `maintenance_probe` | Registry/source readiness and final registry backlog refresh |
| `maintenance_metadata` | Remaining maintenance policy/progress and control work |
| `build_recovery` | Build recovery before deletion |
| `build_gc` | Registered build collection |
| `generation_gc` | Retired generation collection |
| `source_gc` | Source collection as called by the maintenance worker |
| `source_wait` | Publication, capture-admission and transaction gates, plus transaction deferral |
| `source_metadata` | Source policy/candidate queries and deletion-intent/completion transactions |
| `source_expire` | Snapshot selection, expiry, counters and commit |
| `source_artifact` | Directory opens, file identity and digest verification |
| `source_unlink` | Unlink and parent-directory sync |

Parent totals overlap their children and must not be added to them. Source
sub-stages partition collector work; `calls` counts timed segments, rather than
files or expired snapshots. Work before and after a collector is charged to its
outer maintenance stage. Optimizer work not covered by probe/policy timers is
visible in `optimizer_total` but is not further split by this validation.

The load monitor records cumulative profiles. Reports subtract matching profile
identities at phase and measurement boundaries, and preserve a final profile
after worker cancellation. A timed scope is counted when it finishes and can
straddle a sample boundary. Reported `lifetime_max_ms` is the maximum since
enable/resume, not the maximum solely within a subtracted interval. A profile
reset inside a measured interval is rejected instead of combining generations.

Controlled native regressions cover executed cleanup stages under one/two-object
budgets, profile reads while source GC holds its gates, a cancelled/held scope's
elapsed time, explicit-resume identity changes and resumed artifact/unlink time.
Existing transaction, old-view, cancellation and crash-recovery gates remain.

## Validation contract

Use the same thirty-minute closed-loop workload and existing budgets:

```sh
VANE_RUNNER=local-fast python -I test/vane/test_vane_vortex_sustained_load.py \
    --profile steady --seconds 1800 --write-interval 1 \
    --fixtures automatic --rows 512 --dimensions 8 --readers 4 \
    --query-pause 0.02 --recovery-query-pause 0.5 --capture-stalls \
    --output-dir /tmp/vane-vortex-readview-maintenance-30m
```

Only the DuckLake provider is rebuilt; the Vane and Vortex dependency wheels
remain byte-identical to the preceding closure. Profile collection and nested
SQL diagnostic results add observation work, so a single trace is not a
controlled per-change throughput comparison. The original source-drain and 0.8
quality targets remain in force. No compatibility mode, fallback, weighted
cleanup rounds, receipt expiry or native SPFresh online-update API is added.

## Local regression qualification on 2026-11-03

The final installed candidate passed all basic, write, source, keyed, automatic
and stall-diagnostic groups. This includes 28 native source cases, thirteen
maintenance cases and 261 SQL assertion blocks. Eleven independent oracle/timing
regressions and five file-accounting tests passed. The Vane base launcher passed
1,222 non-Ray and 32 Ray tests, with fifteen optional skips in separate processes.
Formatting and copyleft checks passed.

The one-minute steady smoke checked 2,804 queries using their actual snapshots.
Canary/exploration minima were 1.0/0.9; the ninety-second drain completed in
0.936 seconds. Final logical-row and old idempotent-retry checks completed.
All fifteen profile stages were retained, including a final read after worker
cancellation. This short smoke does not establish sustained cleanup throughput.

CI runs the oracle regressions and retains the short steady trace/reports as
well as the existing phased-load evidence. The complete thirty-minute local
measurement is evaluated separately against the unchanged quality and drain
gates.

## Complete thirty-minute observation

The 1,800.062-second ingestion period acknowledged 1,633 batches, or 261,280
mutation operations, with no rejected attempts. It checked 77,582 steady queries
and 78,197 including drain, using 4,898 distinct actual ReadView snapshots.
Of these queries, 4,857 overlapped a commit window. Typed versions, visibility,
category filters, distance, uniqueness and rank all matched the selected
snapshot's committed model. Canary/exploration recall minima were 1.0/0.9,
above the unchanged 0.8 targets. There was no stall capture or watchdog expiry.

**The full load still exited 1 and is not fully qualified.** Source cleanup
failed the unchanged ninety-second gate with 512 eligible snapshots, zero
scheduled files and zero pending deletion intents in both the timeout exception
and the last monitor sample. T/F and build/generation backlogs were zero at
timeout. Final logical-row and old idempotent-retry checks were not reached
after this failed drain; those checks did complete in the short smoke and
regression qualification.

The source queue ended ingestion at its peak of 933 eligible snapshots, with a
full-period linear-fit slope of +28.60/minute. Sampled T/F peaks were 128 rows
each; active B remained at most three. Remaining build/generation peaks were
ten/twelve, and sampled RSS peaked at 792.3 MiB. The optimizer completed 4,401
tasks, maintenance 7,804 runs, and source cleanup expired 4,388 snapshots and
collected 2,924 files through the last drain sample.

| Steady latency, milliseconds | p50 | p95 | p99 |
| --- | ---: | ---: | ---: |
| ACK attempt / client wait | 97.267 | 139.984 | 154.206 |
| All queries | 62.656 | 118.119 | 146.448 |
| Canary | 60.886 | 116.170 | 144.136 |
| Exploration | 63.235 | 118.662 | 147.187 |

### Stage costs

Times below are steady-clock seconds for scopes completed within the sampled
boundaries. The post-ingestion column spans 90.026 seconds from the phase end
to the measured endpoint, before worker cancellation. Calls and averages are
for the ingestion interval. Parent rows overlap their children.

| Stage | Ingestion seconds | Calls | Average ms | Post-ingestion seconds |
| --- | ---: | ---: | ---: | ---: |
| Optimizer total | 1092.769 | 7533 | 145.064 | 23.959 |
| Optimizer probe | 233.862 | 9165 | 25.517 | 16.706 |
| Optimizer metadata | 190.657 | 26629 | 7.160 | 6.752 |
| Maintenance total | 681.874 | 7533 | 90.518 | 33.576 |
| Maintenance probe | 43.233 | 14868 | 2.908 | 3.218 |
| Maintenance metadata | 138.419 | 31102 | 4.450 | 10.020 |
| Build recovery | 46.972 | 2354 | 19.954 | 0.041 |
| Build GC | 110.889 | 2354 | 47.107 | 0.052 |
| Generation GC | 166.991 | 2472 | 67.553 | 0.193 |
| Source GC | 175.369 | 3875 | 45.256 | 20.054 |

| Source sub-stage | Ingestion seconds | Calls | Average ms | Post-ingestion seconds |
| --- | ---: | ---: | ---: | ---: |
| Wait | 37.283 | 3875 | 9.621 | 3.234 |
| Metadata | 68.064 | 15402 | 4.419 | 9.172 |
| Snapshot expiry | 65.908 | 2403 | 27.428 | 7.202 |
| Artifact verification | 0.649 | 11664 | 0.056 | 0.071 |
| Unlink / directory sync | 3.449 | 2642 | 1.305 | 0.374 |

Source metadata plus expiry account for 76.4% of source-GC wall time in
ingestion, gates for 21.3%, and artifact/unlink work for 2.3%. Optimizer probe
and policy work continue after ingestion and take 23.458 seconds combined in
that window. The two invocation totals account for 57.535 of the 90.026
post-ingestion seconds. The remainder includes existing loop waits and work
outside these scopes; the profile does not separately attribute that remainder
or measure CPU utilization.

### Change over the run

The following compare first-to-last monitor samples in the indicated minute,
covering approximately 59.7 seconds each. They are observed averages over
different work mixes, not isolated per-operation benchmarks.

| Observation | Minute 0 | Minute 29 |
| --- | ---: | ---: |
| Eligible snapshot queue, first to last | 0 to 1 | 837 to 933 |
| Snapshots expired / second | 2.744 | 1.106 |
| Source files collected / second | 1.807 | 0.738 |
| Optimizer invocation average ms | 108.995 | 197.909 |
| Maintenance invocation average ms | 80.287 | 107.767 |
| Source GC average ms | 37.612 | 54.949 |
| Source expiry average ms | 24.657 | 33.643 |
| Source metadata segment average ms | 3.694 | 5.742 |

This evidence supports examining repeated metadata/probe work before optimizing
unlink or changing cleanup weights. A next validation should reduce that work
while retaining the count/byte budget, ownership checks, transaction admission,
deletion intents and ninety-second drain. Attribute loop wait time explicitly
before claiming that the unmeasured remainder has a particular cause. These
timers diagnose costs; they do not fix the sustained throughput limit.

The actual-snapshot oracle is fail-closed and does not choose a favorable cut.
A separate replay of this new trace found no compatible-cut lower bound below
0.8, so the successful quality observation alone is not evidence that the
previous run's ambiguous query used its newer cut. That older trace did not
record an actual capture and remains indeterminate. The controlled 0.6/1.0
regression establishes that the recorded cut, including the worse one, governs
the new oracle.

The existing extent accounting remains observational: 360 cost samples took
46.619 seconds and encountered 348 concurrent disappearances. The last failed
drain sample attributed 2.458 MiB to sources, 0.088 MiB to index artifacts,
36.262 MiB to metadata, 9.674 MiB to WAL and 3.511 MiB to caller references;
query scratch was zero. Caller references occupied 15.297 MiB of allocated
blocks. Receipt/reference lifetimes are unchanged. Process counters reported
11.139 GiB write bytes, 10.313 GiB cancelled write bytes and 6.080 GiB logical
`wchar` excluding the event pipe. The process write/input ratio was 729.70.
These are not per-layer physical writes or compaction-only amplification, and
the failed drain does not establish a settled footprint.

Local events, worker log and reports are retained under
`/tmp/ducklake-vortex-readview-timing-steady-30m-v1-20261103/`.
The aggregate report SHA-256 is
`af2f6edefa1617a93c99dffb282fb7551cfd5036deeb3b510e69067b21f3287c`.
