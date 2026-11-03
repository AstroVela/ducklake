# Thirty-minute Vane native steady-cost baseline

This validation extends the existing installed-provider load driver with a
single fixed-interval period. It measures sustained ingestion with concurrent
ANN queries, queue trends, ACK/query tails and retained file costs. The installed
Vane base wheel and signed Vortex/DuckLake provider pair are unchanged from
[the unrestricted-query correction](native-vortex-unfiltered-posting-radius.md).
No native rebuild, query backend change, compatibility layer or fallback is
introduced.

## Reproduce

```sh
VANE_RUNNER=local-fast python -I test/vane/test_vane_vortex_sustained_load.py \
    --profile steady --seconds 1800 --write-interval 1 \
    --fixtures automatic --rows 512 --dimensions 8 --readers 4 \
    --query-pause 0.02 --recovery-query-pause 0.5 --capture-stalls \
    --output-dir /tmp/vane-vortex-steady-30m
```

The live row count stays at 512. Each acknowledged request performs the existing
128 upserts and 32 deletes. The producer waits one second after each attempt;
ACK latency and explicit retry waits therefore reduce the achieved rate. This
is a closed-loop fixed-interval workload, not an open-loop arrival-rate or
saturation benchmark. The thirty minutes apply to the single steady period.
The default `phased` profile still runs paced, burst and recovery periods.

The source/index budget, at most three active B segments, T/F limits, query
pacing, visibility model, first/last idempotent retries and exact final-row
checks remain unchanged. Both canary and exploration enforce their existing
recall lower bound of 0.8. The producer stops before drain; all four readers
continue with the existing 0.5-second recovery pause. Cleanup still has ninety
seconds. The worker watchdog uses the existing nominal duration plus 300
seconds formula: 2,100 seconds for this single thirty-minute period.

## Measurements

The existing quarter-second monitor records T/F occupancy, worker state,
registered build/generation backlog, source GC candidates, CPU, RSS and Linux
process I/O counters. ACK and query p50/p95/p99 are retained for the whole period
and per minute, including per-query-class tails. Successful ACK attempt time
and full client wait with explicit retries remain separate.

Every five seconds, the same monitor also samples the owned source, index,
metadata, WAL, caller-reference and private scratch roots. A child-owned
`TMPDIR` outside the managed data path isolates query scratch. Samples retain
logical path bytes, inode-attributed bytes, allocated blocks, observed new
inodes, positive extent growth and observed removed inodes. Bootstrap extents
are the baseline, not new measured writes. Source/index inodes take priority
over scratch hardlink aliases; shared bytes count once in attributed totals.
Concurrent disappearances and ignored symlinks are counted.

Request receipts are measured by count and logical payload bytes: UTF-8 UUID,
request ID and payload hash lengths plus sixteen bytes for the two UBIGINT
fields. This excludes row/index/compression overhead. Receipts live inside the
metadata database/WAL; their logical payload is not an extra physical byte total.
Native snapshot counts and active B counts are recorded as well.

Ingestion-only minute windows and slopes retain queue, RSS, receipt and file-size
trends. Drain samples remain separate, so a low final footprint cannot conceal
growth during writes. A finite trace reports observed trends and does not prove
that total storage remains bounded indefinitely. Durable receipts retain their
existing lifetime and hard limit; this PR does not expire them.

File extents are not per-file write counters. Rewrites within existing extents,
short-lived files between polls and inode reuse can be missed. Whole-process
`wchar`, `write_bytes` and `cancelled_write_bytes` retain their separate Linux
accounting meanings. Event-pipe bytes are removed from logical `wchar` as before.
The process write/input ratio includes query scratch, metadata/WAL and background
work and must not be labeled compaction-only amplification. Per-layer physical
I/O attribution and device writeback remain unmeasured.

The file/counter samples perturb the measured process and their elapsed cost is
reported. The final file snapshot is labeled `post_measurement`; it occurs after
the process timing boundary and before idempotent-retry checks.

## Failure evidence and qualification

A missing provider, worker error, failed drain, invalid returned row or either
recall target failure leaves a failed case and nonzero aggregate status. Worker
logs, full query events, completed phase latencies, the last source/worker state,
sampled file trends and observed query validation remain available. Partial
evidence never substitutes for final-row, retry or drain qualification.

Filesystem checks use actual hardlinks, growth, unlink and WAL shrink/checkpoint
operations. A short steady profile runs in CI alongside the existing phased
matrix. Local longer-run evidence and any failed acceptance gate are recorded
separately; the profile name alone does not establish steady-state behavior or
production throughput.

## Local baseline on 2026-11-03

The exact command above ran on Linux x86-64 with 24 logical CPUs and
`VANE_RUNNER=local-fast`, using the installed wheel closure qualified in
[DuckLake #68](https://github.com/AstroVela/ducklake/pull/68). Vane was
`0.3.0.dev1`, engine `v1.5.5-vane.e14bf92d40`, source `e957000277`; the native
Vortex revision was `122b63e0408a187a9d0ecdd129f1cbe12b6b46b2`. There were no other
owned builds or loads during the timed run. Driver, metric helper and workflow
hashes matched the files frozen before measurement.

**The run failed its ninety-second drain gate and exited 1.** The steady period
completed in 1,800.689 seconds with 1,637 acknowledged batches, 261,920 mutation
operations and no rejected attempts. It achieved 0.909 batches/second under the
closed-loop pacing described above. There were 74,239 steady queries and 74,848
queries including drain; 4,681 overlapped an acknowledged commit window.

The partial report's `observed_query_validation` checked every returned query
against the exact typed model. Canary minimum recall was 1.0 and exploration
minimum was 0.9, both above 0.8. Visibility, versions, filters, distances,
uniqueness and rank checks passed for those observations. Final-row and old
idempotent-retry checks occur after successful drain, so they did not run. The
aggregate correctly remains `completed_with_failures` and
`not_fully_qualified`; observed query success is not full acceptance.

| Steady-period latency, milliseconds | p50 | p95 | p99 |
| --- | ---: | ---: | ---: |
| ACK attempt / client wait | 94.688 | 136.115 | 159.091 |
| All queries | 66.646 | 130.092 | 159.143 |
| Canary queries | 64.097 | 127.625 | 157.673 |
| Exploration queries | 67.472 | 130.747 | 159.685 |

The thirty complete one-minute windows had ACK p99 from 124.150 to 381.898 ms
and query p99 from 150.373 to 173.509 ms. The final 0.689-second window is
retained separately. No stall capture or watchdog expiration occurred.

### Queue and storage growth

During ingestion, sampled T and F peaks were each 128 rows, active B peaked at
three, remaining builds at one and remaining generations at two. Eligible
source snapshots grew from zero to 1,597, with a sampled peak of 1,602 and a
linear-fit slope of **52.84 snapshots/minute**. Scheduled source files peaked at
two and pending deletion intents at zero. Sampled RSS peaked at 764.8 MiB;
its full-period slope was +4.97 MiB/minute and includes startup growth.

After ninety seconds without new writes, T/F, build and generation backlogs
were zero, but the timeout observation still had **1,221 eligible snapshots**
and two scheduled source files. The final monitor sample, taken independently,
had 1,223 eligible snapshots and no scheduled files. Source collection made
progress; it did not keep up with creation and did not finish by the deadline.
Transaction-blocked samples occurred as well, including at timeout; this run
does not isolate scheduling, transaction admission and per-pass cost as causes.

The table uses inode-attributed MiB. Columns are separate observations from
the roughly five-second sampler, not an atomic storage inventory.

| Area | Last ingestion sample | Ingestion peak | Last drain sample |
| --- | ---: | ---: | ---: |
| Source | 7.244 | 7.244 | 5.559 |
| Index | 0.088 | 0.149 | 0.088 |
| Metadata | 33.512 | 33.512 | 33.512 |
| WAL | 12.982 | 14.754 | 14.059 |
| Caller references | 2.886 | 2.886 | 2.887 |
| Query scratch | 0 | 0.309 | 0 |

Source, metadata and caller-reference ingestion slopes were respectively
+0.238, +1.014 and +0.096 MiB/minute. WAL checkpointing produced oscillations.
The last drain sample contained 3,276 caller-reference files occupying
12.797 MiB of allocated blocks. It also contained 1,638 durable receipts,
including setup, with 218,382 logical payload bytes. Neither receipt nor
caller-reference lifetime was changed. Clearing the source queue would not by
itself establish a bounded total footprint.

There were 362 cost samples with 44.914 seconds of sampler elapsed time,
approximately 2.38% of the measured interval including drain; elapsed sampler
time is not an isolated CPU overhead measurement. Concurrent disappearances
were counted 245 times. During steady ingestion, Linux process counters
reported 10.211 GiB `write_bytes`, 9.474 GiB `cancelled_write_bytes` and
5.630 GiB logical `wchar` after excluding the event pipe, for 15.670 MiB of
canonical mutations. The reported process write/input ratio was 667.27.
These counters do not establish physical device writes or compaction-specific
amplification, and cannot be allocated to individual layers by this sampler.

### Verification and next experiment

Five filesystem metric checks passed. The unchanged phased smoke validated
135 queries; the final steady smoke validated 688, with canary/exploration
minima 1.0/0.9. The existing 6,872-query trace retained identical original
oracle and summary results. Real missing-provider and historical failed-drain
cases retained failed partial evidence. Vane base checks passed 1,222 non-Ray
and 32 Ray tests, with fifteen optional skips, before the timed run.

The next experiment should isolate source-GC throughput and maintenance
fairness while retaining the two-object/64-MiB maintenance budget and current
read/write budgets. Maintenance currently visits build GC, generation GC and
then source GC with the remaining budget. That ordering is a candidate to
test, not a demonstrated sole cause. Reuse this exact thirty-minute workload,
both 0.8 recall targets and the ninety-second drain gate when assessing a fix.
Receipt and caller-reference retention need separate accounting; native
SPFresh online updates are not established by this baseline.

The [maintenance-fairness follow-up](native-vortex-maintenance-fairness.md)
contains controlled starvation and finite-status-admission regressions and the
subsequent installed thirty-minute result. Its unresolved recall-window and
source-drain gates remain explicit.

Local evidence is retained at
`/tmp/ducklake-vortex-steady-30m-v1-20261103/`, including the complete query
events, worker log, per-minute results and partial report. The aggregate
`report.json` SHA-256 is
`2e9fa2f376b99739e0c4a9c9eec54a14b2fa109733e9468194cc12b481977ed2`.
