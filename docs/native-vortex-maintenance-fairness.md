# Fair native Vortex maintenance and finite status admission

The [thirty-minute baseline](native-vortex-steady-cost-baseline.md) completed
ingestion and observed query validation but failed its ninety-second source-GC
drain. This change addresses a deterministic starvation case in the maintenance
scheduler, admits finite status reads and removes redundant metadata reads in
that worker. It retains write admission, ANN budgets and retention policy.

## Failure and change

Previously, every pass visited registered build cleanup, retired generation
cleanup and then source cleanup. Each class used the remaining object and byte
budget. A continuous supply of builds could consume a one-object pass before
generation or source work ran. With two objects, build and generation arrivals
could leave source work without a turn indefinitely.

Each catalog now rotates which of the three classes goes first. The remaining
classes can use that pass's unused budget. All three still share one object
limit and one byte limit; recovery remains before build deletion within the
build class. A class gets first use every three completed passes when work is
ready. Transaction deferral, file verification, publication and collection locks,
deletion intents, cancellation and explicit restart/resume rules are retained.

The cursor lives in the existing in-process catalog registration and starts
with builds on enable/resume. No catalog schema, file format, compatibility
path, fallback or automatic retry is added. Fair scheduling does not guarantee
progress through a held native transaction, an oversized object or a failed
collector, and does not establish a sustained throughput target.

## Maintenance metadata work

Each progress-state write previously loaded the full maintenance policy again
to check its run ID and preserve the enabled flag, before issuing a conditional
update that already required the same run ID and exactly one returned row. The
progress update now performs that ownership check directly and preserves the
database's enabled value in the same statement. Explicit cancellation, failure
and resume still update the enabled flag deliberately. Progress cannot
re-enable a cancelled policy.

Source readiness is evaluated before scheduling a pass. The final registry
backlog refresh no longer repeats that readiness query: its private result was
not persisted or consumed after the pass. Public build/generation backlog
counts are still refreshed, and the collector retains its independent policy,
transaction, identity and reference checks before destructive work.

The intermediate rotation-only thirty-minute run is retained separately. It
checked 76,630 queries with canary/exploration minima 1.0/0.9, but failed the
unchanged ninety-second drain with 626 eligible snapshots remaining. During
ingestion its queue ended at 1,104 snapshots with a full-period slope of
+35.66/minute. This improved the original observation of 1,221 snapshots left
at timeout, but did not qualify cleanup or a stable storage footprint. The
metadata-read changes were made after that complete trace was collected.

## Finite status reads and source admission

The optimizer and maintenance status APIs also opened ordinary DuckLake
transactions for metadata-only reads. Unlike the source status API, they did
not hold finite read admission or mark their transactions as captures. Source
readiness therefore deferred work when either status query was in flight,
including reads made by the load driver's health monitor.

Both APIs now acquire shared ReadView admission before the metadata transaction
and mark it as a finite capture. Readiness can queue source work while a status
read finishes; the existing collector closes admission and waits for it before
freezing transaction starts. Ordinary data transactions still defer collection.
The destructive collector's locks, transaction check and reference checks are
unchanged.

The regression pauses an actual status read through a generated identity column
in a temporary copy of its metadata table. It preserves the production table
initialization checks and checks the returned enabled value. For both optimizer
and maintenance status, the previous implementation reported `ready=false` and
held no admission. The corrected APIs report `ready=true` and hold admission.
With an ordinary data transaction held, both versions correctly report blocked.
The existing source-status case retains the same checks. These add
`optimizer-status-readiness` and `maintenance-status-readiness` to the installed
native source qualification.

The metadata-read-only candidate's second long trial was deliberately
interrupted at approximately 18.7 minutes to investigate this deterministic
status-admission defect. Its trace is diagnostic evidence, not a completed
thirty-minute qualification.

## Controlled regression

The native source fixture first produces ended native source history with a
real rewrite. At each of the first eight completed maintenance passes it makes
an explicit replacement index build, replenishing registered build scratch and
retired generations. Production append planning is unchanged by this fixture.
The observation ends after nine passes, before stopping the arrivals can allow
the old scheduler to catch up. A separate connection controls the worker.

Every observed pass checks the sum of collected builds, generations, expired
snapshots and collected source files against its shared object budget and
checks aggregate collected bytes against 64 MiB. The current ANN winner must
remain correct. The same fixture is linked against old and new native code.

| Scheduler | Objects/pass | Builds collected | Generations collected | Snapshots expired | Result |
| --- | ---: | ---: | ---: | ---: | --- |
| Previous fixed order | 1 | 9 | 0 | 0 | Failed: starvation |
| Rotating first class | 1 | 3 | 3 | 3 | Passed |
| Previous fixed order | 2 | 10 | 8 | 0 | Failed: starvation |
| Rotating first class | 2 | 7 | 6 | 5 | Passed |

The one-object cases used at most one object per pass and the two-object cases
at most two. No source files were deleted within these nine-pass observations;
source snapshot expiry is the source-class progress being asserted here. The
broader source-retention suite covers actual unlink, held views, transaction
deferral and failure/recovery checkpoints.

These cases run through `test_vane_vortex_source_retention.py` with the matching
native source driver as `maintenance-fairness-1` and `maintenance-fairness-2`.
They are part of the installed-provider CI qualification.

## Sustained workload

Recheck the exact installed closure with the baseline driver:

```sh
VANE_RUNNER=local-fast python -I test/vane/test_vane_vortex_sustained_load.py \
    --profile steady --seconds 1800 --write-interval 1 \
    --fixtures automatic --rows 512 --dimensions 8 --readers 4 \
    --query-pause 0.02 --recovery-query-pause 0.5 --capture-stalls \
    --output-dir /tmp/vane-vortex-fair-maintenance-30m
```

The baseline's closed-loop pacing, two-object/64-MiB maintenance budget, global
query budget, T/F limits, both 0.8 recall targets and ninety-second drain are
the acceptance contract. A controlled starvation fix alone does not qualify
that longer run or establish bounded total storage. Receipt and caller-reference
lifetimes and the limitations of sampled extent attribution remain as described
in the baseline.

## Final local qualification on 2026-11-03

The final installed candidate passed all basic, write, source, keyed, automatic
and stall-diagnostic groups, including 28 native source cases, thirteen
maintenance cases and 261 SQL assertion blocks. The Vane base launcher again
passed 1,222 non-Ray and 32 Ray tests, with fifteen optional skips in separate
processes. Formatting and copyleft checks passed. The one-minute installed
smoke checked 2,645 queries with canary/exploration minima 1.0/0.9 and drained
in 0.694 seconds.

**The final thirty-minute run exited 1 and is not fully qualified.** Its
1,800.621-second steady period acknowledged 1,628 batches, or 260,480 mutation
operations, with no rejected attempts. It observed 73,427 steady queries and
74,013 including drain, of which 4,985 overlapped a commit window. There was no
stall capture or watchdog expiration. Source, T/F and query budgets and both
0.8 targets were unchanged; final-row and old idempotent-retry checks were not
reached after the failed drain.

| Acceptance observation | Final result |
| --- | --- |
| Canary recall lower bound | 1.0; above 0.8 |
| Exploration recall lower bound | 0.7; below 0.8 |
| Observed typed versions, visibility, filters, distances, uniqueness and rank | Passed |
| Ninety-second source drain | Failed |
| Source work in timeout exception | 716 eligible snapshots, two scheduled files |
| Independent last monitor sample | 718 eligible snapshots, no scheduled files |

The T/F and registered build/generation backlogs were zero at timeout. During
ingestion the source queue ended at 1,141 snapshots, peaked at 1,143 and had a
linear-fit slope of +33.29/minute. Sampled T and F peaked at 256 rows each,
active B at three, remaining builds/generations at six each, and RSS at
682.2 MiB. The existing two-object/64-MiB per-pass maintenance budget remained
in force.

| Steady latency, milliseconds | p50 | p95 | p99 |
| --- | ---: | ---: | ---: |
| ACK attempt / client wait | 99.882 | 146.851 | 189.008 |
| All queries | 67.155 | 128.610 | 183.444 |
| Canary | 65.240 | 125.606 | 182.808 |
| Exploration | 67.817 | 129.497 | 183.565 |

### Recall-window diagnostic

Replaying the unchanged oracle found exactly one exploratory query below its
conservative 0.8 lower-bound target. It was filtered to category `B`, spanned
watermarks 15 to 18, and matched the typed rows of both cuts. Its recall was
0.7 against cut 15 and 1.0 against cut 18. No stable-watermark query was below
0.8; the minimum across the best compatible cuts was 0.9. These secondary
diagnostics do not replace the production oracle or its failed gate, and do
not establish a native ANN regression.

The next quality validation should record the actual ReadView snapshot used by
the search and resolve it to the acknowledged logical model. Choosing the most
favorable compatible cut would conceal uncertainty. Retain the 0.8 target and
use the captured snapshot to remove that uncertainty instead.

### Retained costs and interpretation

The following are inode-attributed MiB from independent samples. The last
drain sample is a failed-cleanup observation, not a settled footprint.

| Area | Last ingestion sample | Ingestion peak | Last drain sample |
| --- | ---: | ---: | ---: |
| Source | 5.212 | 5.212 | 3.408 |
| Index | 0.088 | 0.241 | 0.088 |
| Metadata | 36.262 | 36.262 | 36.262 |
| WAL | 3.749 | 15.055 | 4.981 |
| Caller references | 3.316 | 3.316 | 3.321 |
| Query scratch | 0.183 | 0.309 | 0 |

Source, metadata and caller-reference ingestion slopes were +0.151, +1.064 and
+0.114 MiB/minute. Caller references occupied 14.449 MiB of allocated blocks
in the last drain sample. The durable receipt and caller-reference lifetimes
were retained. There were 361 cost samples, 45.509 seconds of sampler elapsed
time and 189 observed concurrent disappearances. Linux steady-process counters
reported 10.460 GiB `write_bytes`, 9.662 GiB `cancelled_write_bytes` and
5.727 GiB logical `wchar` excluding the event pipe. The process write/input
ratio was 687.37. The baseline's extent-sampling and I/O-accounting limitations
apply; these are not per-layer physical writes or compaction-only amplification.

The original baseline and rotation-only run timed out with 1,221 and 626
eligible snapshots respectively. The final candidate's last monitor sample had
718, while its timeout exception reported 716. They share workload arguments
but have different publication and cleanup schedules: the original
optimizer completed 3,894 tasks, the rotation-only candidate 4,422, and the final
candidate 4,237. These single-host traces do not isolate a per-change throughput
gain. The controlled counterexamples establish the starvation and status-read
fixes; the long runs retain the unresolved throughput limit.

The next maintenance experiment should measure optimizer-probe, metadata,
snapshot-expiry, artifact and unlink time separately before changing scheduling
again. Keep every pass within the existing count/byte budget and recheck the
same ninety-second drain. No weighted cleanup rounds, compatibility mode,
fallback, receipt expiry or native SPFresh online-update API was introduced.

The [actual-snapshot and maintenance-profile follow-up](native-vortex-readview-maintenance-profile.md)
records query captures and the existing worker's stage times for that experiment.

Local complete events, worker log and report are retained under
`/tmp/ducklake-vortex-status-admission-steady-30m-v1-20261103/`.
The aggregate report SHA-256 is
`d7e0a2577f6ae354fdb60665bd5ddbe150db2cddfe04e236217d98ac9777426e`.
