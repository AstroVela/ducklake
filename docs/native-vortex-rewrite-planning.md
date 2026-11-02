# Serialize physical plan selection with managed mutations

The 512-row mixed-load diagnostic retained by the
[source-drain follow-up](native-vortex-source-gc-drain.md) stopped its optimizer
with `Vortex rewrite requires one nonempty source compaction`. Its durable plan
selected an indexed segment with 256 physical rows, 128 deleted rows and 128
remaining rows. Read-only inspection of that failed database found zero
visible rows in the selected source: subsequent managed replacements had
masked the rest before the rewrite captured its input.

## Reservation scope

The optimizer previously selected its rewrite plan before acquiring the
existing managed-writer reservation. Moving that reservation before the
planning probe keeps candidate selection, durable task selection, source
capture and physical publication serialized with managed mutations. The
planner's existing remaining-row checks can then select indexed rewrite,
small-output rewrite to F, prune or empty-segment retirement against stable
managed input.

The nonempty indexed-rewrite guard stays intact. There is no empty-index
fallback, error-message retry, additional reservation, compatibility mode or
budget change. Query snapshots retain their existing visibility and ownership
rules. Ordinary SQL mutations outside the managed API retain the existing
identity and snapshot checks.

When no physical rewrite is selected, the worker releases the reservation
after the probe. Flush keeps its existing reservation around the physical
change; independent B construction and merge still run without the managed
writer reservation. Every initial probe now holds the managed-writer
reservation. A physical rewrite retains it from planning through publication, so
foreground ACK latency includes that work. This change establishes input
stability; it does not qualify production write latency.

## Regression coverage

The native `automatic-planning-serialization` case pauses immediately after
durable rewrite selection, before source capture. A managed mutation tries to
delete every remaining row of that selected B. It must reach admission and
remain blocked until the rewrite publishes. Queries captured before and after
the mutation are checked against complete committed row versions. A subsequent
fresh plan must retire the now fully masked segment, and exact receipt replay
must preserve logical rows.

The same test against the preceding optimizer failed with `Managed writer
entered a physical-maintenance snapshot`. It releases the paused worker before
reporting failure. The native case runs in the existing automatic-flow CI
command alongside flush/rewrite serialization, atomic backpressure,
cancellation and real SIGKILL recovery cases.

The SQL regression replaces both halves of a 512-row B through managed writes,
checks typed F/T results, retires the fully masked B, checks the historical
snapshot, builds the new F as B, and replays the first receipt. It complements
the native checkpoint test, which exercises the concurrent planning window.

## Local qualification on 2026-11-03

The new native checkpoint failed against the preceding `86435518` optimizer
and passed after the reservation change. The signed DuckLake provider was
rebuilt and installed against the unchanged matching Vane and Vortex wheels.
The ten SQL fixtures passed all 255 assertion blocks through the installed
provider; standard SQLLogicTest remains CI. The separate native segment-cap
probe also passed its small-delta rejection and global 100,000-row ceiling
checks after correcting its stale error expectation to the 65-row minimum.

Automatic flow passed 80 query comparisons and nine native/recovery cases,
including the new planning case and four real SIGKILL boundaries. Source
retention passed 16 comparisons and 21 cases; keyed writes passed 28 and 13;
append passed 16 and seven; the basic installed flow passed 32 comparisons.

The unchanged load command was expanded locally to include 512 rows as well
as the four existing CI combinations. Each phase lasted 12 seconds, with two
readers, the same query intervals, T/F budgets and 90-second drain deadline.

| Rows / dimensions | Outcome | Checked queries | Drain, seconds | Canary / exploration minimum |
| --- | --- | ---: | ---: | --- |
| 512 / 8D | Failed, worker stopped producing events | — | — | — |
| 512 / 128D | Completed | 748 | 28.66 | 1.0 / 0.3 |
| 4,096 / 8D | Completed | 535 | 27.11 | 1.0 / 0.7 |
| 4,096 / 128D | Completed | 297 | 14.37 | 1.0 / 0.4 |
| 32,768 / 8D | Completed | 171 | 13.49 | 1.0 / 1.0 |
| 32,768 / 128D | Completed | 84 | 12.18 | 1.0 / 1.0 |

The five completed cases checked 1,835 query results against committed typed
read windows, drained T/F and reclamation work, preserved final logical rows
and replayed old receipts exactly. They all passed the unchanged 0.8 canary
gate. Small-input exploration recall remains below the separate 0.8 target.
These timings are bounded convergence observations, not production latency or
throughput qualification. For 32,768 rows / 128D, observed end-to-end write p95
was 16.84 seconds in the paced phase and 20.86 seconds in recovery; the small
number of commits does not establish a latency distribution or a regression
comparison.

The 512-row / 8D worker's last recorded optimizer state was `building`, run 19,
with T = F = 128. It was terminated by the existing outer watchdog without
reporting the original empty-compaction error. This remains a failed load
diagnostic and is excluded from qualified counts. The reservation regression
proves the planning race is closed; it does not establish the cause of that
later stop. CI retains its four existing load combinations and gains the
deterministic planning checkpoint and SQL regression. No deadline, recall
gate or budget is relaxed to include the failed case.

A separate 512-row / 8D run under a parent debugger completed normally: 53
committed batches, 780 checked queries, a 30.31-second drain, exact final rows
and receipt replay, and canary / exploration minima of 1.0 / 0.3. Read-only
replay against the failed database also completed its canary and next
exploration query. Those diagnostics did not reproduce the stop or provide
a stalled stack, so the original failure remains open. The instrumented run
is excluded from the load matrix and its performance observations.

```sh
VANE_RUNNER=local-fast python -I test/vane/test_vane_vortex_sustained_load.py \
    --fixtures automatic --rows 512 4096 32768 --dimensions 8 128 --seconds 12 \
    --output-dir /tmp/vane-vortex-rewrite-planning
```
