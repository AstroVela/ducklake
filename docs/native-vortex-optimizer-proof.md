# Native background segment optimization

This validation draft follows [DuckLake #37](https://github.com/AstroVela/ducklake/pull/37)
and the [original design](https://chatgpt.com/space/page_5ee0f8c698d08191a17198d45d152362).
It adds the first local background optimizer over the existing native builder.
The Qdrant reference is the segment lifecycle and scheduling pattern: committed
inline T stays queryable, flush creates scan-capable F, and a sealed generation
atomically promotes covered files to ANN-backed B. It does not embed Qdrant.

## Explicit policy

```sql
SELECT * FROM ducklake_initialize_vortex_indexes('lake');
SELECT * FROM ducklake_initialize_vortex_optimizer('lake');
SELECT * FROM ducklake_enable_vortex_optimizer(
    'lake', 'main', 'items', 'knn', '/absolute/existing/index-root',
    8, 128, 4194304, 128, 67108864);

SELECT * FROM ducklake_vortex_optimizer_status('lake');
SELECT * FROM ducklake_cancel_vortex_optimizer('lake','main','items','knn');
SELECT * FROM ducklake_resume_vortex_optimizer('lake','main','items','knn');
```

The last five enable arguments are dimension, T row/byte thresholds and F
row/byte thresholds. Each stage triggers when either threshold is reached.
T bytes account for vector floats, 64 bytes per row and UTF-8 business strings;
F bytes use encoded file sizes. These are scheduling metrics, not resident-memory
measurements. A worker checks committed metadata/inline aggregates every 100 ms;
it does not pin or decode every B file just to decide whether work is due.

Initialization and policy installation are explicit. Duplicate initialization,
duplicate policy, unsupported configuration and invalid thresholds fail. The
prototype requires native DuckDB metadata, the existing four business columns,
unencrypted local Vortex output and `auto_compact=true`. It binds table UUID and
embedding field ID, including the interval between scheduling and capture.
The existing canonical absolute paths and outside-managed-data root restrictions
apply. There is no migration, backend substitution or error-to-scan path.
Enable prepares the managed data root if an empty/inline-only table has not
created it yet, so the first threshold-triggered flush can write there.

## Scheduling and publication

One worker per database serializes at most 16 registered policies, with at most
one build in flight. It first flushes due committed T using DuckLake's existing
transactional flush; if the resulting F meets a threshold, it invokes the shared
native builder. A run may finish as `flushed` without building if F remains small.
Bootstrap captures all current physical files. The
[two-segment follow-up](native-vortex-segments-proof.md) then builds only new F
into an independent static SPFresh generation, preserving existing B. The
[selective merge follow-up](native-vortex-segment-merge-proof.md) permits three
segments under the same aggregate budgets. When full and F is due, a separate
task merges the two smallest B segments before a fresh append task promotes F.
It does not automatically rebuild the whole table or call native SPFresh online
insert/delete. The existing full-build API remains explicit operator maintenance.

Build and publication retain the previous source, artifact and revision-CAS
checks. Heavy construction holds no metadata transaction or publication lock.
Other connections can query, update, delete, append and flush. Queries use the
current ready generation plus current visible F/T; data committed after capture
stays in those scan routes until a later successful promotion. Before the first
ready generation, ordinary exact vector SQL can query committed data; named
`ducklake_vortex_search` still requires a published index.

Source replacement/compaction that removes B coverage stops the policy and
requires an explicit rebuild. It is not repaired by an implicit alternate plan.
Ordinary metadata checks compare file IDs, paths and row counts; the existing
capture/publication boundary performs the full source-byte identity validation.

The native build's existing 64 MiB source/vector/artifact and 100,000 physical-row
limits remain. Policy row thresholds must be 1–100,000; T byte thresholds must
be 1–4 MiB and F byte thresholds 1–64 MiB. Serial scheduling bounds task
concurrency, not total RSS or SPFresh's internal thread count. Hard ingestion
backpressure is qualified in the follow-up below; production capacity tuning
remains separate work.

## Durable task state and ownership

Policies and the latest task are persisted in `ducklake_vortex_optimizer_v2`.
The new strict schema records task kind and selected generations. It requires
fresh explicit initialization; no schema migration or old-table decoder is added.
Status exposes UUID, field-bound policy identity, state, enabled intent, run ID,
expected/candidate/published revision information, T/F metrics, run count, error
and whether the local build connection is active. It also exposes task kind
and the exact selected merge generations. Metrics describe the latest
probe and may lag a just-completed operation until the next check.

Cancellation first persists disabled intent, then interrupts the worker's
dedicated context. Cancellation is cooperative at the existing native
checkpoints; an SPFresh native build call completes before its next check.
Uncommitted failure disables the policy and preserves its reason. Explicit
resume plans work from fresh committed table state. Failed/cancelled candidates
can remain outside managed data. The
[generation GC follow-up](native-vortex-generation-gc-proof.md) collects explicitly
registered, retired published generations; unpublished candidates are excluded.

Before publication, the worker persists the sealed candidate generation and
expected revision. Failure after commit and explicit resume after process restart
check the ready catalog for that exact successor generation. A committed task is
recorded as published without rebuilding it. An uncommitted task is marked
interrupted and a fresh threshold check can create a new run/reference; the old
candidate is never blindly replayed.

After reopening a database, persisted enabled intent does not automatically start
a worker. Call `ducklake_resume_vortex_optimizer` to reconcile and activate each
policy. This keeps recovery explicit for this prototype. The worker drops all
database/connection references between checks, uses a weak database reference,
and releases its runtime at owner shutdown. A running native operation can finish
while it owns its connection; closing the last client does not create a permanent
database-reference cycle.

## Validation

`test_native_vortex_optimizer.py` drives the real background thread without a
tick API, external build worker or probe process. It covers below-threshold idle,
T→F→B, independent byte thresholds, duplicate policy/active-task rejection,
concurrent business writes and exact/ANN queries, post-capture F/T, cancellation,
terminal failure and explicit resume, lost publication ACK, SIGKILL on both sides
of commit, a mismatched vector field ID, table drop/recreate, invalid flush
configuration after an earlier success, and active/idle database shutdown.

It records task counts and generation IDs to distinguish reconciliation from
duplicate publication, and checks recall against committed visible-row ground
truth separately from visibility correctness. Minimal SQL tests cover explicit
configuration, status and cancellation; ON/OFF regression/recovery runners remain
in CI. Independent recovery scenarios explicitly consolidate after using both
segment slots. This qualifies local scheduling and recovery, not distributed
optimizer ownership, multi-replica operation or scale performance.

The original #38 qualification restricted the database weak-reference shutdown
assertion to optimizer fixtures after detecting retained references on the exact
#37 base (`236979019286cc964b0d6b3148c08376450f1378`) without an optimizer.
The [query lifecycle follow-up](native-vortex-query-lifecycle-proof.md) pins the
Vortex reader/driver lifetime fixes, restores unconditional shutdown assertions
and adds ownership checks without another Vortex runtime poll.

The [write budget follow-up](native-vortex-backpressure-proof.md) adds explicit
transactional T/F quotas and retained-version capacity checks. Quotas survive
optimizer cancellation and database restart; exhaustion rejects growth before
metadata commit while acknowledged data remains queryable.
