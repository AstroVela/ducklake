# Maintenance probe query validation

**Final decision: withdraw the combined production query.** The
[controlled A/B benchmark](native-vortex-maintenance-probe-ab.md) did not meet
the predeclared material/repeatable gain threshold. Production optimizer code
is restored byte-for-byte to #73, including transaction-completion wakeup,
ordinary-transaction protection, original timing boundaries and all budgets.
The two functional native probe cases now expect the retained two statements;
the one-statement reference exists only in the benchmark executable.

## Archived candidate at 80f4110a

The sections below retain the implementation and thirty-minute observations of
`80f4110ac45c555fbcd29e5dab52d0059983a339`, before withdrawal. They document the
measured candidate and are not a description or qualification of the final
production query shape. The A/B decision and final regression results are
recorded in the linked controlled-benchmark note.

The [transaction-completion wakeup validation](native-vortex-transaction-gc-wakeup.md)
passes the original ninety-second drain in 63.003 seconds. During continuous
ingestion its eligible-snapshot queue still grows by +16.95/minute. This change
targets one repeated metadata cost: the initial maintenance probe reads the
policy and live build/generation backlog in two statements within the same
transaction.

### Initial probe and state transitions

The initial probe now selects the existing thirteen policy columns and two
scalar backlog counts in one statement. The counts use the same build table
and `retired`/`deleting` generation predicate as before. This removes one query
submission and planning boundary; it does not remove the underlying counts or
claim a stronger snapshot guarantee than the preceding single transaction.

The saved `remaining_builds`/`remaining_generations` fields are retained long
enough to determine whether a no-work probe must clear stale status. The live
counts then replace them for scheduling and publication of the current backlog.
An empty actual queue with stale saved counters still becomes idle without
creating an empty maintenance run. Policy decoding, budget validation and the
conditional `run_id` ownership update retain their existing behavior.

Only the initial worker probe uses the combined statement. Status, resume,
cancellation and error handling retain their policy-only reads. A completed
collector still recounts backlog in its fresh finishing transaction before
saving status. There is no reuse across transactions, persistent cache, schema
change, compatibility layer or fallback.

Transaction completion subscriptions, the wake epoch, ordinary-transaction
exclusion, publication/capture/transaction-start gates, source ownership and
reference checks, deletion intents, fairness and per-pass budgets are unchanged.

### Timing interpretation

The eighteen profile stages are retained. The combined initial policy/backlog
statement and its initialization checks are timed under `maintenance_probe`.
Previously the policy read was in `maintenance_metadata` and only the separate
backlog read was in `maintenance_probe`. Those individual segment totals and
averages therefore have a changed boundary. Compare full maintenance invocation
cost and actual query counts when assessing the reduction; a lower metadata
segment alone would not establish a performance gain. Parent/child stages and
the completing-thread `source_transaction_wake` retain their existing overlap
rules.

### Controlled checks

Two native cases register the engine's existing connection/query callbacks in
the test database. They record the actual relevant SELECT statements on the
first worker probe connection, excluding caller-thread status/resume queries.
No production query interception or additional timer stage is introduced.

- With two build records and one retired generation, the worker reads and
  saves the real counts before collection with the two-object/64-MiB budget.
  Cancellation, explicit resume and a fresh post-collection count converge to
  zero backlog; the current ANN winner remains unchanged.
- With actual counts zero but saved counters seven/nine, the worker clears
  both counters, stays idle and does not increment the maintenance run count.
  The current ANN winner remains unchanged.

Both cases observe one combined policy/backlog statement and retain the SQL in
`maintenance-probe-query.json`. An isolated control splits only those initial
reads back into the preceding two statements. Both control cases preserve the
state assertions but observe two statements and fail the same query-count
assertion. This establishes the query reduction, not an isolated throughput
comparison with the complete parent build. The other catalog initialization,
source-readiness, state-write and post-collection queries remain outside this
one-versus-two count.

### Sustained validation contract

Rebuild and non-editably install only DuckLake with the same Vane/Vortex wheel
bytes, then run the original isolated workload:

```sh
VANE_RUNNER=local-fast python -I test/vane/test_vane_vortex_sustained_load.py \
    --profile steady --seconds 1800 --write-interval 1 \
    --fixtures automatic --rows 512 --dimensions 8 --readers 4 \
    --query-pause 0.02 --recovery-query-pause 0.5 --capture-stalls \
    --output-dir /tmp/vane-vortex-maintenance-query-30m
```

Retain the actual ReadView snapshot oracle, both 0.8 recall gates,
ninety-second drain, two-object/64-MiB maintenance pass and existing ANN/T/F
budgets. Measure the continuous-ingestion backlog slope and query latency as
well as drain completion. Native SPFresh online updates and receipt expiry are
outside this change.

### Local regression qualification on 2026-11-03

The final non-editably installed candidate passed basic, write, source, keyed,
automatic and stall-diagnostic groups, including 38 native source cases. The
thirteen maintenance cases, 261 SQL assertion blocks, eleven actual-snapshot
and timing oracle tests and five file-accounting tests passed. The Vane base
launcher passed 1,222 non-Ray and 32 Ray tests with fifteen optional skips in
separate processes. Formatting and copyleft checks passed. Only DuckLake was
rebuilt; Vane and Vortex wheel bytes are unchanged from the parent closure.

### Complete thirty-minute observation

The final 1,800.043-second ingestion period acknowledged 1,631 batches, or
260,960 mutation operations, without rejected attempts. It validated 74,172
steady queries and 74,735 including drain against 4,894 actual ReadView
snapshots. Of these, 5,006 queries overlapped a commit window. Typed versions,
visibility, filters, distance, uniqueness and rank matched the selected model.
Canary/exploration recall minima were 1.0/0.9, above both existing 0.8 targets.
There was no stall capture or watchdog expiry.

**The unchanged sustained correctness and drain contract passes (exit 0).**
Drain took 82.070 seconds, below the ninety-second deadline, and left zero
eligible snapshots, scheduled source files and pending deletion intents. Final
512 logical rows and old idempotent retries passed. Through the final monitor
sample, source GC expired 4,894 snapshots and collected 3,262 files; the
optimizer completed 4,401 tasks and maintenance 8,248 runs. Sampled T/F peaked
at 128 rows each, active B remained at most three and sampled RSS peaked at
790.4 MiB.

**This trace does not demonstrate an end-to-end performance improvement.**
Ingestion ends and peaks at 812 eligible snapshots, with a full-period
linear-fit slope of +26.31/minute. The parent ends at 613, peaks at 614 and has
slope +16.95/minute. Full maintenance invocation average cost, query latency
and drain duration are also higher in this trace. The controlled cases prove
one fewer initial policy/backlog statement; they do not establish a sustained
throughput benefit from that query shape.

| Observation | Parent wakeup trace | Combined-query trace |
| --- | ---: | ---: |
| Acknowledged batches | 1,633 | 1,631 |
| Steady validated queries | 76,160 | 74,172 |
| Ingestion last eligible snapshots | 613 | 812 |
| Ingestion peak eligible snapshots | 614 | 812 |
| Eligible-snapshot slope / minute | +16.95 | +26.31 |
| Maintenance invocation average ms | 90.433 | 93.388 |
| Steady query p50 ms | 64.904 | 67.212 |
| Steady query p99 ms | 148.261 | 153.771 |
| Drain seconds | 63.003 | 82.070 |

These are separate single-host traces with different publication, blocking and
cleanup schedules, task mixes and observational overhead. They neither isolate
a throughput gain nor establish that the combined statement alone caused the
worse observed backlog/latency. A matched-data comparison of the initial probe's
planning/execution cost would be needed to attribute its cost separately from
the rest of maintenance. This validation should remain a draft carrying the
negative performance evidence; it does not establish stable indefinite
ingestion or production readiness.

| Steady latency, milliseconds | p50 | p95 | p99 |
| --- | ---: | ---: | ---: |
| ACK attempt / client wait | 98.414 | 142.923 | 161.455 |
| All queries | 67.212 | 125.539 | 153.771 |
| Canary | 64.974 | 122.913 | 150.674 |
| Exploration | 67.995 | 126.478 | 154.680 |

#### Completed stage costs

The post-ingestion interval below spans 82.126 seconds from phase end to the
measured endpoint, before worker cancellation. Times are steady-clock seconds
for completed scopes. Parent stages overlap their children; `source_metadata`
counts timed segments rather than files. The initial policy read moved from
`maintenance_metadata` to the combined `maintenance_probe` scope as described
above, so those segments are not unchanged-boundary comparisons with the parent.

| Stage | Ingestion seconds | Calls | Average ms | Post-ingestion seconds |
| --- | ---: | ---: | ---: | ---: |
| Optimizer total | 1067.876 | 7614 | 140.252 | 28.521 |
| Optimizer probe | 211.474 | 9245 | 22.874 | 18.951 |
| Optimizer metadata | 188.024 | 26800 | 7.016 | 9.567 |
| Maintenance total | 710.959 | 7613 | 93.388 | 51.034 |
| Maintenance probe | 47.729 | 15052 | 3.171 | 5.857 |
| Maintenance metadata | 120.751 | 31504 | 3.833 | 12.834 |
| Build recovery | 59.354 | 2339 | 25.376 | 0.003 |
| Build GC | 117.769 | 2339 | 50.350 | 0.032 |
| Generation GC | 174.166 | 2476 | 70.342 | 0.058 |
| Source GC | 191.205 | 4022 | 47.540 | 32.232 |
| Source wait | 49.104 | 4023 | 12.206 | 4.596 |
| Source metadata | 69.758 | 15856 | 4.399 | 15.390 |
| Source expiry | 68.009 | 2479 | 27.434 | 11.419 |
| Source artifact verification | 0.658 | 12006 | 0.055 | 0.124 |
| Source unlink / sync | 3.680 | 2718 | 1.354 | 0.680 |
| Loop timeout wait | 7.005 | 70 | 100.072 | 0.200 |
| Loop wake wait | 14.128 | 198 | 71.353 | 2.382 |

The eighteenth stage accepted 622 transaction-completion signals during
ingestion and 405 afterward, or 1,027 through the measured endpoint. Handler
time in the completing threads was 1.101 and 1.402 milliseconds respectively;
these durations are excluded from worker accounting. The original wakeup
implementation remains active: post-ingestion timeout waiting is two waits
totaling 0.200 seconds, while completed notified/spurious waits total 2.382
seconds. Signals may coalesce or be latched before waiting.

The two invocation totals and two loop waits sum to 1,799.968 seconds during
ingestion and 82.137 seconds afterward, compared with elapsed periods of
1,800.043 and 82.126 seconds. Completed scopes can straddle sample boundaries,
so these are approximate accounting totals rather than an exact partition.
No CPU utilization is inferred from these wall times.

#### Change over the run and retained evidence

First-to-last monitor samples in minute 0 cover 59.950 seconds; minute 29 covers
59.548 seconds. Different task mixes contribute to these averages.

| Observation | Minute 0 | Minute 29 |
| --- | ---: | ---: |
| Eligible snapshots, first to last | 0 to 4 | 766 to 812 |
| Snapshots expired / second | 2.686 | 1.931 |
| Source files collected / second | 1.768 | 1.276 |
| Optimizer probe average ms | 18.329 | 27.344 |
| Maintenance invocation average ms | 80.588 | 96.271 |
| Source GC average ms | 41.786 | 53.561 |
| Source expiry average ms | 24.837 | 30.317 |

The retained extent accounting is observational: 359 samples took 47.006
seconds and saw 360 concurrent disappearances. The successful drained footprint
contains 47,552 source bytes, 92,530 index bytes, 512 retained physical rows and
1,632 request receipts; metadata/WAL and caller-reference retention policies
are unchanged. These samples do not measure physical per-layer write
amplification.

Complete events, logs and reports are retained under
`/tmp/ducklake-vortex-maintenance-query-steady-30m-v1-20261103/`.
The aggregate report SHA-256 is
`b862a0c9ac41991ec8b325ce0ae762af00341b4eb7a035ba0ea29521d035582f`.
The DuckLake wheel SHA-256 is
`a705210e09cb910dada25ad294054e5ed0a2c0a4abd9dacf34c1207abac75adb`.
