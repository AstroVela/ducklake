# Wake deferred source GC when an observed transaction ends

The [metadata-probe validation](native-vortex-metadata-probes.md) measured
28.925 seconds of condition-variable timeouts in a ninety-second drain that
still left 269 eligible snapshots. This change makes transaction completion
signal an already deferred source collector. A notification requests a fresh
probe; it does not authorize collection or relax transaction protection.

## Registration and delivery

Source readiness first checks for pending snapshot or file work using metadata
reads. Before file I/O or returning ready, it retains the ordinary-transaction
check. If that check defers pending work, the transaction manager atomically
records the observed blocking transactions and one completion callback. The
readiness probe itself and finite Vortex captures are excluded as before. A
no-work or disabled source policy does not register a callback.

The collector retains its independent check after closing publication,
capture admission and transaction-start gates. A transaction that arrives after
readiness can still block collection; this late-blocker path also registers a
completion callback. The notification does not hold a source file or bypass
ownership, reference, identity/digest or deletion-intent checks.

The first observed blocker to end consumes the one-shot registration. Commit
and successful rollback remove the transaction from the active set under the
transaction-manager mutex; delivery happens only after that mutex is released.
The commit-error cleanup path retains its existing rollback/error handling and
also removes the transaction before delivery. Existing rollback failure
behavior is unchanged. A following probe must check all remaining/new
transactions and can register another notification.

Only transactions in the observed blocker set trigger that registration. The
worker's own metadata commits, unrelated transactions and consumed registrations
do not generate completion wakeups. The callback holds a weak runtime reference
and the catalog's in-process maintenance profile identity. Cancelled, stopped
or superseded registrations ignore delivery. No callback state is persisted;
restart still requires explicit maintenance resume.

## Do not lose a completion before waiting

An accepted completion increments a runtime wake epoch under the same mutex
used for the worker condition variable. Each loop records the epoch before its
probes. When no collection progress was made, it waits only if the epoch is
unchanged. A completion during the probes therefore causes an immediate fresh
iteration; a completion after waiting starts wakes the condition variable.
Checking and entering the wait remain under that mutex.

The existing 100-ms idle wait and all per-pass budgets are unchanged. There is
no extra collector, shorter polling interval, compatibility path, fallback or
cross-transaction index/policy cache. Each notification is revalidated by the
same source guards and collectors before any destructive work.

`ducklake_vortex_maintenance_profile` now includes the eighteenth fixed stage,
`source_transaction_wake`. Its count is accepted blocker-completion signals;
time is spent handling the signal in the completing thread. It overlaps worker
activity and must not be added to worker invocation/wait totals. Several signals
can be coalesced before a probe; a latched signal need not correspond to a
`loop_wait_wake` because it can prevent waiting altogether.

## Controlled causal checks

Six native cases are part of the installed source qualification:

- Commit and rollback before the worker enters its wait each produce one
  accepted notification and zero timeouts before the first guarded expiry.
- With two ordinary transactions held, ending one delivers a notification but
  cannot collect through the other. Finite capture and probe completions do not
  notify. The second blocker is rechecked and rollback delivers its own signal.
  A no-work probe does not subscribe.
- A new reader starting after the first notification blocks the fresh probe;
  its later rollback produces another signal before collection.
- A reader arriving after readiness blocks the collector's gated check and is
  registered there. Its completion allows a later guarded attempt.
- Cancellation before blocker completion ignores the signal and leaves the
  policy disabled; only explicit resume allows cleanup afterward.

The commit/rollback cases pause the worker after it has committed the deferred
probe, finish the blocker, and only then release the worker. They inspect
timeout counts at the first expiry rather than imposing a sub-100-ms wall-time
deadline. An isolated control removes only the epoch-latch check: it accepts the
same single notification but records one timeout before progress and fails the
same assertion. This establishes the missed-notification race without claiming
a complete parent performance comparison. Current ANN winners and existing
held-view, budget and crash-recovery checks remain required.

## Sustained validation contract

Rebuild and non-editably install only the DuckLake provider with the same Vane
and Vortex wheel bytes, then run the same isolated workload:

```sh
VANE_RUNNER=local-fast python -I test/vane/test_vane_vortex_sustained_load.py \
    --profile steady --seconds 1800 --write-interval 1 \
    --fixtures automatic --rows 512 --dimensions 8 --readers 4 \
    --query-pause 0.02 --recovery-query-pause 0.5 --capture-stalls \
    --output-dir /tmp/vane-vortex-transaction-wakeup-30m
```

Retain the actual ReadView snapshot oracle, both 0.8 recall gates, ninety-second
drain, two-object/64-MiB maintenance pass and existing query/T/F budgets. Native
SPFresh online updates, receipt expiry and new compatibility/fallback paths are
outside this change.

## Local regression qualification on 2026-11-03

The final installed candidate passed all basic, write, source, keyed, automatic
and stall-diagnostic groups, including 36 native source cases, thirteen
maintenance cases and 261 SQL assertion blocks. Eleven actual-snapshot/timing
oracle tests and five file-accounting tests passed. The Vane base launcher
passed 1,222 non-Ray and 32 Ray tests, with fifteen optional skips in separate
processes. Formatting and copyleft checks passed. The changed transaction
manager also syntax-checks with Vortex disabled; this is not a full OFF-runtime
qualification. Vane/Vortex wheel bytes are unchanged; only DuckLake was rebuilt.

The one-minute smoke checked 2,617 actual-snapshot queries at canary/exploration
minima 1.0/0.9, drained in 0.493 seconds and completed final logical-row and old
idempotent-retry checks. It recorded fourteen accepted blocker-completion
signals. This short smoke does not establish sustained cleanup throughput.

## Complete thirty-minute observation

The final 1,800.275-second ingestion period acknowledged 1,633 batches, or
261,280 mutation operations, without rejected attempts. It validated 76,160
steady queries and 76,595 including drain against 4,900 actual ReadView
snapshots. Of these, 4,948 queries overlapped a commit window. Typed versions,
visibility, filters, distance, uniqueness and rank matched the selected model.
Canary/exploration recall minima were 1.0/0.9, above both existing 0.8 targets.
There was no stall capture or watchdog expiry.

**This isolated sustained run passes the unchanged acceptance contract (exit
0).** Drain took 63.003 seconds, below the ninety-second deadline, and left
zero eligible snapshots, scheduled source files and pending deletion intents.
The final 512 logical rows and old idempotent retries passed. Through the final
monitor sample, source GC expired 4,900 snapshots and collected 3,266 files;
the optimizer completed 4,395 tasks and maintenance 8,182 runs. Sampled T/F
peaked at 128 rows each, active B remained at most three and sampled RSS peaked
at 766.3 MiB.

Ingestion still ends at 613 eligible snapshots, peaks at 614 and has a
full-period linear-fit slope of +16.95/minute. Passing the drain after stopping
writes does not establish a bounded source backlog during indefinite ingestion.

| Steady latency, milliseconds | p50 | p95 | p99 |
| --- | ---: | ---: | ---: |
| ACK attempt / client wait | 97.316 | 138.508 | 155.681 |
| All queries | 64.904 | 120.099 | 148.261 |
| Canary | 63.284 | 117.646 | 144.797 |
| Exploration | 65.458 | 120.908 | 149.272 |

### Completed stage costs and completion signals

The post-ingestion interval below spans 63.058 seconds from phase end to the
measured endpoint, before worker cancellation. Times are steady-clock seconds
for completed scopes. Parent stages overlap their children; `source_metadata`
counts timed segments rather than files.

| Stage | Ingestion seconds | Calls | Average ms | Post-ingestion seconds |
| --- | ---: | ---: | ---: | ---: |
| Optimizer total | 1078.082 | 7762 | 138.892 | 22.056 |
| Optimizer probe | 218.263 | 9396 | 23.229 | 14.484 |
| Optimizer metadata | 192.691 | 27077 | 7.116 | 7.382 |
| Maintenance total | 702.035 | 7763 | 90.433 | 39.287 |
| Maintenance probe | 43.729 | 15342 | 2.850 | 3.848 |
| Maintenance metadata | 135.167 | 32026 | 4.221 | 11.109 |
| Build recovery | 51.225 | 2312 | 22.156 | 0.002 |
| Build GC | 110.379 | 2312 | 47.742 | 0.034 |
| Generation GC | 168.937 | 2468 | 68.451 | 0.000 |
| Source GC | 192.593 | 4142 | 46.498 | 24.294 |
| Source wait | 43.772 | 4142 | 10.568 | 3.494 |
| Source metadata | 73.707 | 16578 | 4.446 | 11.604 |
| Source expiry | 70.551 | 2572 | 27.430 | 8.589 |
| Source artifact verification | 0.723 | 12573 | 0.057 | 0.103 |
| Source unlink / sync | 3.825 | 2856 | 1.339 | 0.501 |
| Loop timeout wait | 5.304 | 53 | 100.080 | 0.200 |
| Loop wake wait | 14.593 | 210 | 69.490 | 1.705 |

The eighteenth stage accepted 602 transaction-completion signals during
ingestion and another 294 in the post-ingestion interval, or 896 through the
measured endpoint. Handler time in the completing threads was 1.635 and 1.584
milliseconds respectively. These times are excluded from worker accounting.
There were 210 completed notified/spurious loop waits during ingestion and 259
afterward; more signals can be latched before waiting or coalesced.

The two invocation totals and two loop waits sum to 1,800.014 seconds during
ingestion and 63.249 seconds afterward, compared with elapsed periods of
1,800.275 and 63.058 seconds. Completed scopes can straddle sample boundaries,
so these are approximate accounting totals rather than an exact partition.
No CPU utilization is inferred from these wall times.

Post-ingestion timeout waiting was two waits totaling 0.200 seconds, or 0.318%
of that interval. The parent trace recorded 28.925 seconds in a 90.051-second
interval and failed its drain. In this trace, the same callback registration
and epoch-latch implementation tested by the controlled cases is active, and
the original drain succeeds. These separate traces do not isolate a per-change
throughput gain: task mixes, publication/cleanup schedules and observational
overhead differ. The controlled latch/no-latch case establishes the causal
missed-notification fix.

### Behavior over time and remaining work

First-to-last monitor samples in minute 0 cover 59.788 seconds; minute 29 also
covers 59.788 seconds. Different task mixes contribute to these averages.

| Observation | Minute 0 | Minute 29 |
| --- | ---: | ---: |
| Eligible snapshots, first to last | 0 to 3 | 548 to 613 |
| Snapshots expired / second | 2.710 | 1.639 |
| Source files collected / second | 1.806 | 1.070 |
| Optimizer probe average ms | 20.926 | 25.000 |
| Maintenance invocation average ms | 85.357 | 105.338 |
| Source GC average ms | 43.676 | 51.449 |
| Source expiry average ms | 24.996 | 30.014 |

The parent ingestion peak/slope were 821/+24.83 snapshots per minute; this
trace measures 614/+16.95. The remaining positive slope and the reduced late-run
expiry rate make sustained cleanup cost the next investigation. Profile the
repeated optimizer/maintenance metadata work and guarded source expiry as
metadata grows, then validate one measured reduction under the same budgets,
ordinary-transaction exclusion and snapshot/recall oracle. One successful drain
does not establish indefinitely stable ingestion, production readiness or
completion of the wider roadmap.

The retained extent accounting is observational: 356 samples took 44.027
seconds and saw 347 concurrent disappearances. The successful drained footprint
contains 47,552 source bytes, 92,522 index bytes, 512 retained physical rows and
1,634 request receipts; metadata/WAL and caller-reference retention are
unchanged. These samples do not measure physical per-layer write amplification.

Complete events, logs and reports are retained under
`/tmp/ducklake-vortex-transaction-wake-steady-30m-v1-20261103/`.
The aggregate report SHA-256 is
`c930d75ef9928cf3c1953227cc193e8489d25648643f4e6d3c558d6cb5ac94f4`.
The DuckLake wheel SHA-256 is
`7ff66181a19898c71f156d7e1c6389ec7550517b35adad59c052dda5b0834a50`.

The next [maintenance probe validation](native-vortex-maintenance-probe-query.md)
combines the initial policy/backlog statements under the same transaction and
retains the wakeup implementation and all original guards and budgets.
