# Installed Vane sustained mixed-load qualification

This probe measures the [automatic managed-write flow](native-vortex-automatic-flow.md)
under a paced producer, a burst and a planned recovery period. The measured
process runs installed Vane and the matching signed providers. Its reference
model and retained event trace live in the parent process, outside the measured
RSS and storage I/O.

The [posting-coverage follow-up](native-vortex-posting-coverage.md) explains the
rejected builds below and changes head planning within the same posting budget.
The local results in this document describe the preceding fixed-head baseline.

## Workload and ownership

Each case starts with a separately prepared immutable B containing 4,096 or
32,768 rows. Bootstrap uses managed batches and an explicit initial index build;
its time and I/O are excluded. The measured phase uses only managed mutations,
queries and status reads. Workers perform flush, index append, selective merge,
source rewrite and reclamation. The end-to-end empty-table bootstrap remains
covered by the automatic-flow probe.

The `automatic` input preserves the automatic-flow vectors. The explicit
`cost` input uses the earlier cost probe's vector formula and even/odd input
order. Both remain selectable up front; there is no input switch after a
failure. Their business payloads use the same small mutation fixture.

Every committed batch replaces 96 existing keys, deletes 32 other keys and
inserts 32 new keys. The live key count stays fixed. Row generation is
deterministic and includes category changes, NULL payloads, Unicode and embedded
NUL bytes. The two dimensions used locally are 8 and 128; the vectors are
synthetic, not a representative embedding distribution.

The aggregate 100,000 physical-row, 64 MiB source/vector/artifact and three-B
limits remain unchanged. The measured write budget is T <= 256 rows / 4 MiB and
F <= 1,024 rows / 64 MiB. Both promotion thresholds are 128 rows. Compaction
requires at least 32 deleted rows and one percent dead rows; retention keeps two
snapshots with zero minimum age. Maintenance handles at most two objects and
64 MiB per pass. These deliberately small test settings are explicit.

The single bounded producer waits one second after each attempt in the paced
and recovery periods; the burst has no pacing delay. A rejected attempt is
recorded, checked for absence of a receipt, and explicitly resubmitted using
the same request and payload. Native snapshot conflicts and T/F admission
rejections are distinct outcomes. The runtime still performs zero implicit
WriteBatch commit retries.

Two query connections run throughout, including drain. Their normal pause is
20 ms after each query. Recovery and drain use a predetermined 500 ms pause to
reduce load and give conservative transaction-protected reclamation idle
windows. A failed worker fails the probe. There is no automatic resume, workload
switch after an error, compatibility path or fallback.

## Flush coordination

The burst exposed a real race: a flush captured the inline table, a managed
mutation committed an insert, and the flush later failed native conflict
checking. The optimizer entered its failed state and stopped draining T.

Flush now acquires the existing WriteBatch writer reservation before its native
snapshot and holds it through flush commit. Managed writes use that same
reservation. The reservation is released before index construction. ReadViews
remain usable during flush, and writers can commit while the subsequent index
build runs. Source rewrite and retirement also reserve that writer before
capture and release it after publication. This preserves their captured visible
input against managed mutations. A data rewrite includes its static index preparation
inside that reservation and can increase write ACK tails; pure append/merge
index construction remains outside it. No additional mutex, schema/catalog
version or retry loop is added.

The native `automatic-flush-serialization` case pauses after the flush snapshot,
starts a managed writer and proves it waits. It then holds the index-build
checkpoint and requires that writer to finish. Same-view typed queries cover
both sides, followed by automatic promotion, GC and exact receipt replay.
The separate rewrite case holds a prepared source replacement and checks that
a managed mutation touching its live input waits until publication completes.

## Concurrent SQL correctness and ANN quality

The public search result does not expose its internal ReadView snapshot. Each
query therefore reads the durable mutation watermark immediately before and
after executing the ordinary named SQL search. The parent reconstructs every
committed mutation and requires the complete returned result to match one
logical state inside that window. A query must include all writes acknowledged
before its first watermark. It cannot choose a different version for each hit.
Final native rows must match the model exactly, and old receipt replays after
drain must leave that state unchanged.

This checks public read-window consistency. It does not claim to identify the
exact internal snapshot; the automatic native probe continues checking actual
ReadView snapshot IDs. Query timing excludes watermark reads and oracle work.
The watermark reads, status monitor and event serialization still contribute
to overall process CPU and resource measurements.

One connection uses the established fixed query as a canary with the unchanged
recall@10 >= 0.8 fixture gate. Another uses rotating filtered queries to measure
quality outside that qualified point. Typed versions, visibility, uniqueness,
filters, distance and rank order are hard checks for both. The report retains
the exploratory recall distribution and whether its minimum reaches 0.8;
exploratory quality misses are not relabeled as version failures or concealed
by lowering the canary gate. Where several logical cuts fit a public read
window, recall is reported as the conservative minimum over those cuts.

A completed measurement is not production quality or performance acceptance.
New query distributions need their own agreed recall and latency targets.
The probe keeps exploratory misses as evidence for subsequent search-quality
work; it does not silently tune SPFresh options or select another backend.

## Metrics and reproduction

```sh
VANE_RUNNER=local-fast python -I test/vane/test_vane_vortex_sustained_load.py \
    --fixtures automatic --rows 4096 --dimensions 8 --seconds 12 \
    --output-dir /tmp/vane-vortex-load-ci

VANE_RUNNER=local-fast python -I test/vane/test_vane_vortex_sustained_load.py \
    --fixtures automatic cost --rows 4096 32768 --dimensions 8 128 --seconds 12 \
    --output-dir /tmp/vane-vortex-load-matrix
```

The matrix retains every failed case and continues to the remaining scheduled
inputs. A failed worker, coverage check, correctness check or canary gate leaves
an aggregate nonzero exit status. Measured latencies remain available when a
completed workload fails only its recall gate. Successful smaller cases never
substitute for a rejected larger case.

`--seconds` controls each of the three periods. This is a closed-loop producer
with bounded in-flight work, not an open-loop arrival-rate or saturation QPS
benchmark. It reports actual elapsed time, accepted batches/operations and
rejected attempts. Acknowledgement percentiles distinguish the successful
attempt from the full client wait including explicit retries and pacing.
Query p50/p95/p99 is reported for each query class and the combined workload.

The monitor records T/F rows and bytes, worker state, source GC deferral,
user/system CPU, sampled current RSS, lifetime high-water RSS and Linux process
I/O counters. High-water RSS includes bootstrap; sampled RSS covers the measured
period. Neither establishes a hard process memory bound. Kernel `write_bytes`
and `cancelled_write_bytes` are storage-accounting counters, not measured device
writeback. Logical `wchar` is also reported with the known event-pipe bytes
removed. Monitoring and native diagnostics remain part of the process costs.

The process write-byte/input-byte ratio uses committed canonical mutation bytes
as its denominator. It includes query artifact materialization, metadata/WAL
and background work; it is not solely compaction amplification. Drain is
reported separately. Actual owned source/index bytes must converge to the
retained native inventory and active generations. Metadata, WAL, receipts and
caller reference JSON keep their separately reported lifetimes and limits.

CI runs the bounded 4,096-row / 8D and 128D profiles and retains the summary, per-case
reports, worker logs and parent-written `events.jsonl`. The latter contains the
full query evidence and final logical state. The minimal SQL case checks quota
rejection, admission of the unchanged request after an explicit budget change,
named T search and exact retry. Larger matrices and longer runs are explicit;
no production defaults or throughput claims are derived from the short CI run.

## Local qualification on 2026-10-30

The final matrix used installed Vane `0.3.0.dev1`, engine
`v1.5.5-vane.e14bf92d40` / source ID `e957000277`, the pinned Vortex/SPFresh
providers, two readers and 12 seconds per phase. It finished all eight scheduled
cases with aggregate exit status 1:

| Input | Rows | Dimensions | Result |
| --- | ---: | ---: | --- |
| automatic | 4,096 | 8 | Completed; typed visibility and fixed canary passed; exploration minimum 0.6 |
| cost | 4,096 | 8 | Completed; typed visibility and fixed canary passed; exploration minimum 0.4 |
| automatic | 4,096 | 128 | Initial build rejected by mapped-ID coverage check |
| cost | 4,096 | 128 | Initial build rejected by mapped-ID coverage check |
| automatic | 32,768 | 8 | Initial build rejected by mapped-ID coverage check |
| automatic | 32,768 | 128 | Initial build rejected by mapped-ID coverage check |
| cost | 32,768 | 8 | Initial B built; subsequent source rewrite rejected by mapped-ID coverage check |
| cost | 32,768 | 128 | Initial build rejected by mapped-ID coverage check |

The coverage error is `SPFresh native files do not cover all mapped IDs`.
It remains a hard failure. These results identify rejected inputs, not a claim
that all higher-dimensional data is unsupported. Both completed cases retained
the fixed canary minimum of 1.0, while their exploration misses leave broader
search quality unqualified.

| Completed case | automatic / 4,096 / 8D | cost / 4,096 / 8D |
| --- | ---: | ---: |
| Queries / committed batches | 626 / 40 | 641 / 39 |
| Burst committed batches / budget rejections | 19 / 114 | 19 / 117 |
| Burst successful ACK p95 / p99, ms | 136.8 / 136.8 | 123.8 / 123.8 |
| Burst client wait p95 / p99, ms | 1,383.9 / 1,383.9 | 1,361.2 / 1,361.2 |
| Burst query p95 / p99, ms | 188.2 / 213.8 | 178.7 / 196.6 |
| Sampled peak RSS, KiB | 504,584 | 508,168 |
| Observed peak T / F rows | 256 / 256 | 256 / 256 |
| Drain with continuing readers, seconds | 64.0 | 62.7 |
| Final retained physical rows | 4,096 | 4,096 |
| Final source / index bytes | 273,284 / 538,766 | 277,448 / 408,029 |
| Burst process write bytes / mutation byte | 669.3 | 544.5 |

These short-run percentiles have few ACK samples. The write ratio includes
query artifact materialization and cancelled kernel writes; device writeback
and compaction-only amplification have not been measured. The two probes
observed 69 collected source files each, and exact receipt replays preserved
their final logical rows.

An earlier candidate that serialized flush alone stalled while rewriting and
was killed by its 336-second watchdog. No stack captured that stall. Subsequent
diagnostic repetitions and both final 4,096-row cases completed, but this does
not establish the cause or qualify long-run liveness.

The final provider also passed the automatic-flow regression (80 query
comparisons, four native cases and four SIGKILL recovery points), source
retention (16 comparisons and 20 native/recovery cases), managed keyed writes
(28 comparisons and 13 cases), append WriteBatch (16 comparisons and seven
recovery/fault cases), and the basic installed flow (32 comparisons). All 151
assertion blocks in the five affected SQL fixtures passed through the installed
provider probe; the standard SQLLogicTest runners remain CI checks.
The Vane base release launcher passed 1,222 non-Ray and 32 Ray tests in separate
processes, with 15 optional-dependency skips. Formatting and Vane source/native
copyleft checks passed. Replaying the retained query trace with a missing-hit
canary or an invalid typed payload also produced the required nonzero exit.

The [posting-coverage follow-up](native-vortex-posting-coverage.md) resolves the
mapped-ID failures in this matrix. Reclamation under the larger 128D load and
broader recall remain unqualified. SPFresh native online updates are separate
work.
