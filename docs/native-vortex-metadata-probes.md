# Reduce repeated metadata probes and attribute worker waits

The [actual-snapshot profile](native-vortex-readview-maintenance-profile.md)
validated query quality but left 512 eligible snapshots after its ninety-second
drain. This follow-up reduces duplicate reads within existing transactions and
records the worker's condition-variable waits. It does not cache state across
passes or change scheduling, budgets, ownership or source-deletion protection.

## Probe capture reuse

An optimizer probe already validates the current table and parses the ready
index for coverage, dimensions, field identity and merge planning. Compaction
selection previously looked up that same table/index again in the same metadata
transaction. The probe now returns its table and owned parsed index to the
compaction selector. All alias, budget, source identity and deletion checks
remain; the captured revision must match the probe's expected revision.

The capture is local to that probe and is discarded before the metadata
transaction ends. A following pass obtains a fresh capture. Write-admission
probes retain their existing behavior and do not perform compaction selection.

The native regression uses a generated identity column on a temporary copy of
the actual index table to count revision evaluations. It pauses exactly one
compaction-enabled probe before policy publication. The candidate performs two
evaluations: the revision check and the initial index lookup. An isolated
control with only the second lookup restored performs three and fails the same
assertion. Both use the same fixture and provider closure; this is a controlled
duplicate-read proof, not a complete parent performance baseline. The current
ANN winner remains unchanged.

## Maintenance and source metadata

The final build/generation backlog refresh and idle progress update now share
one metadata transaction instead of committing a read and opening a second
transaction for the update. The conditional update still requires the run ID
and exactly one returned row, and preserves concurrent cancellation through
`enabled=enabled`. Collector commits and deletion intents remain independent.

Source status now obtains policy fields, cumulative counters and exact pending
counts in one SQL statement, replacing five statements in the same transaction.
It retains policy validation, the ten existing output columns, finite read
admission, capture classification and the ordinary-transaction blocker check.
The status-pause regression continues to exercise a real query result.

Source readiness needs only to know whether an eligible snapshot exists. Its
probe now uses `EXISTS`; public status still reports an exact count. Policy,
transaction and collector guards are unchanged, including the collector's fresh
checks before expiry or deletion. No index lookup, ownership check, reference
check, intent commit, identity/digest verification or directory sync is removed
from destructive collection.

## Loop wait timing

`ducklake_vortex_maintenance_profile` now has seventeen fixed stages. The two
new stages are `loop_wait_timeout` and `loop_wait_wake`. They time the existing
`changed.wait_for(..., 100ms)` call, including reacquiring the runtime mutex.
Wake includes notifications and possible spurious wakeups; timeout describes
the condition-variable outcome rather than a hard elapsed-time ceiling.

This is one shared worker wait. Its elapsed time is recorded in each catalog
profile scheduled for cleanup in that iteration. It must not be summed across
catalogs. With one catalog, the two wait stages complement the optimizer and
maintenance invocation totals; they are outside those invocation scopes. As
with the existing counters, only completed waits are published and a wait can
straddle a sample boundary. Enable/resume creates a new profile identity.

The idle native regression observes a completed timeout and at least fifty
milliseconds of wall time under a no-source-work policy. Profile reads remain
in memory and do not add metadata transactions or collector admission.

## Validation contract

Retain the previous installed closure's Vane/Vortex wheels and rebuild only
DuckLake. Run the affected native and SQL regressions, followed by the base
release launcher, then measure the same closed-loop workload separately:

```sh
VANE_RUNNER=local-fast python -I test/vane/test_vane_vortex_sustained_load.py \
    --profile steady --seconds 1800 --write-interval 1 \
    --fixtures automatic --rows 512 --dimensions 8 --readers 4 \
    --query-pause 0.02 --recovery-query-pause 0.5 --capture-stalls \
    --output-dir /tmp/vane-vortex-metadata-probes-30m
```

The actual ReadView snapshot oracle, both 0.8 recall targets, ninety-second
drain and per-pass two-object/64-MiB budget remain unchanged. Count/byte
accounting, held-view safety, cancellation and crash-recovery checks remain
required. No compatibility layer, fallback, cross-transaction cache, weighted
cleanup rounds, receipt expiry or native SPFresh online-update API is added.

## Local regression qualification on 2026-11-03

The final installed closure passed basic, write, source, keyed, automatic and
stall-diagnostic groups, including thirty native source cases, thirteen
maintenance cases and 261 SQL assertion blocks. Eleven actual-snapshot/timing
oracle regressions passed. The base launcher passed 1,222 non-Ray and 32 Ray
tests, with fifteen optional skips in separate processes. Formatting and
copyleft checks passed. Vane and Vortex wheel bytes are unchanged from the
preceding closure; only DuckLake was rebuilt and non-editably installed.

The one-minute smoke checked 2,677 queries against their actual snapshots at
canary/exploration minima 1.0/0.9. It completed the drain in 0.464 seconds and
reached final logical-row and old idempotent-retry checks. Seventeen stages,
including completed loop timeouts, were retained. This short smoke does not
establish sustained cleanup throughput.

## Complete thirty-minute observation

The final 1,800.529-second ingestion period acknowledged 1,632 batches, or
261,120 mutation operations, without rejected attempts. It validated 75,033
steady queries and 75,642 including drain against 4,897 actual ReadView
snapshots. Of these, 4,993 queries overlapped a commit window. Typed versions,
visibility, filters, distance, uniqueness and rank matched the selected model.
Canary/exploration recall minima were 1.0/0.9, above both existing 0.8 targets.
There was no stall capture or watchdog expiry.

**Full sustained acceptance still fails (exit 1).** The ninety-second timeout
reports 267 eligible snapshots and two scheduled files; the independent last
monitor sample reports 269 and zero scheduled files. Neither has pending
deletion intents. T/F and build/generation backlogs are zero at timeout.
Final logical-row and old idempotent-retry checks are not reached after the
failed drain; they complete in the short smoke and regression qualification.

Ingestion ends at 820 eligible snapshots, peaks at 821 and has a full-period
linear-fit slope of +24.83/minute. Sampled T/F peak at 128 rows each, active B
remains at most three, and RSS peaks at 790.5 MiB. Through the final monitor
sample the optimizer completed 4,412 tasks and maintenance 8,036 runs; source
GC expired 4,628 snapshots and collected 3,084 files.

| Steady latency, milliseconds | p50 | p95 | p99 |
| --- | ---: | ---: | ---: |
| ACK attempt / client wait | 98.348 | 139.512 | 155.001 |
| All queries | 66.057 | 123.022 | 152.005 |
| Canary | 64.249 | 120.441 | 147.593 |
| Exploration | 66.715 | 123.873 | 153.138 |

### Stage costs and the previously unattributed interval

The post-ingestion interval below spans 90.051 seconds from phase end to the
measured endpoint, before worker cancellation. Times are steady-clock seconds
for completed scopes. Ingestion call counts/averages are given in the middle
columns; parent stages overlap their children. `source_metadata` counts timed
segments rather than files, as in the previous profile.

| Stage | Ingestion seconds | Calls | Average ms | Post-ingestion seconds |
| --- | ---: | ---: | ---: | ---: |
| Optimizer total | 1077.515 | 7601 | 141.760 | 22.029 |
| Optimizer probe | 214.472 | 9233 | 23.229 | 14.706 |
| Optimizer metadata | 190.083 | 26806 | 7.091 | 7.318 |
| Maintenance total | 699.953 | 7600 | 92.099 | 39.083 |
| Maintenance probe | 41.935 | 15049 | 2.787 | 3.668 |
| Maintenance metadata | 129.328 | 31451 | 4.112 | 10.677 |
| Build recovery | 56.967 | 2352 | 24.221 | 0 |
| Build GC | 115.458 | 2352 | 49.089 | 0 |
| Generation GC | 172.360 | 2464 | 69.951 | 0 |
| Source GC | 183.917 | 3984 | 46.164 | 24.725 |
| Source wait | 43.146 | 3985 | 10.827 | 3.825 |
| Source metadata | 69.529 | 15783 | 4.405 | 11.393 |
| Source expiry | 66.957 | 2442 | 27.419 | 8.955 |
| Source artifact verification | 0.659 | 11969 | 0.055 | 0.091 |
| Source unlink / sync | 3.613 | 2718 | 1.329 | 0.456 |
| Loop timeout wait | 23.017 | 230 | 100.074 | 28.925 |
| Loop wake wait | 0 | 0 | n/a | 0 |

The two invocation totals and the two loop waits sum to 1,800.485 seconds
during ingestion and 90.037 seconds after it, compared with elapsed periods of
1,800.529 and 90.051 seconds. Completed-scope deltas can straddle boundaries,
so these are close sampled accounting totals rather than an exact partition
at every instant. No CPU utilization is inferred from these wall times.

Timeout waiting accounts for 32.1% of the post-ingestion interval. This directly
measures the existing wait; the previous profile could only report a remainder
outside invocation scopes. No notification/spurious-wake outcome was observed
inside this measured interval. It does not follow that every timeout had the
same cause or that removing ordinary-transaction blockers would be safe.

### Change over the run and comparison limits

First-to-last monitor samples in minute 0 cover 59.908 seconds; minute 29 covers
59.834 seconds. Different task mixes contribute to these averages.

| Observation | Minute 0 | Minute 29 |
| --- | ---: | ---: |
| Eligible snapshots, first to last | 0 to 6 | 748 to 821 |
| Snapshots expired / second | 2.654 | 1.504 |
| Source files collected / second | 1.769 | 1.036 |
| Optimizer probe average ms | 18.570 | 26.124 |
| Maintenance invocation average ms | 85.128 | 107.845 |
| Source GC average ms | 41.390 | 53.991 |
| Source expiry average ms | 25.445 | 31.235 |

The previous profile left 512 snapshots in its final sample; this run leaves
269. Its ingestion peak/slope were 933/+28.60 per minute, versus 821/+24.83
here. Optimizer-probe average time is 25.517 versus 23.229 ms, and maintenance
metadata totals are 138.419 versus 129.328 seconds. However, steady queries
are 77,582 versus 75,033 and p50 query latency 62.656 versus 66.057 ms; optimizer
task counts are 4,401 versus 4,412. These are separate single-host traces with
different publication/cleanup schedules and observational overhead. They do
not isolate a per-change throughput gain. The controlled 3-to-2 evaluation
case establishes the duplicate-lookup reduction; the long trace still fails
the original cleanup target.

With a nonempty source queue and substantial measured timeout waiting, the
next validation should examine transaction-completion wakeup for deferred
source work. It must preserve ordinary-transaction exclusion, closed collector
admission, ownership/reference checks, deletion intents and per-pass budgets.
The profile alone does not identify the transaction behind every deferral, so
record that cause when validating a wakeup change rather than bypassing the
blocker or merely increasing cleanup limits.

The retained extent accounting is observational: 360 samples took 46.739
seconds and saw 302 concurrent disappearances. Last failed-drain samples
attribute 1.400 MiB to sources, 0.088 MiB to indexes, 36.262 MiB to metadata,
10.646 MiB to WAL and 3.528 MiB to caller references; query scratch is zero.
Receipt and caller-reference retention are unchanged. These are not physical
per-layer writes or proof of a settled storage footprint.

Complete events, logs and reports are retained under
`/tmp/ducklake-vortex-metadata-probes-steady-30m-v1-20261103/`.
The aggregate report SHA-256 is
`ca42a356939d8e4f79b791e25afcf91cab21a11101e068a274599489aebb353e`.

The [transaction-completion wakeup follow-up](native-vortex-transaction-gc-wakeup.md)
validates a guarded retry signal and an epoch latch for deferred source work.
