# Source-GC cost from restored native inputs

The [unified roadmap](ducklake-vortex-design-roadmap.md) leaves sustained source
reclamation open. The [#74 controlled benchmark](native-vortex-maintenance-probe-ab.md)
measured read probes and rejected policy/backlog query fusion; it did not execute
snapshot expiry, deletion intents or file removal. This benchmark measures one
actual guarded source-collector call with the existing two-object/64-MiB budget.
Production sources, transaction protection, ownership/reference checks and
deletion protocols are unchanged.

## Inputs and exact work

Preparation writes two 64-row Vortex files, builds a 128-row static B, deletes
sixteen rows from each file and rewrites the selected B through the native
operation. The current B has 96 typed rows, with IDs 16–63 and 80–127. Build and
retired-generation work is collected before sealing the input. A separate
one-row history table receives 8, 512 or 4,096 native updates. It either keeps
versions inline (`inline`) or uses `data_inlining_row_limit=0` to materialize
every version as a Parquet file (`file`). Source
retention keeps two snapshots with zero minimum age; no autonomous worker runs
during measurement.

Each history scale/storage combination has two tasks:

- `expire`: native preparation retains exactly the two oldest snapshots still
  protecting the retired sources. The measured call expires those two, schedules
  the old files, and unlinks none. It begins with no scheduled source files.
- `files`: preparation also completes that expiry outside timing. The measured
  call verifies, registers intents, unlinks/syncs and acknowledges exactly two
  native queued files. It expires no additional snapshots.

Expiry inputs retain 13, 517 or 4,101 snapshots; file inputs retain 11, 515 or
4,099. Inline inputs have three/one native data-file records for expiry/files;
materialized inputs have `history+4`/`history+2`. The driver requires those exact
inventories. The materialized control prevents snapshot-only growth from hiding
file-reference and snapshot/file interval-join costs. Live vector data and the
requested object count stay fixed. This does not reproduce growing schemas,
request receipts, delete-file inventories or the concurrent worker mix.

## Restore before every sample

The preparation process exits before its metadata, data and index tree is copied
into a sealed image. Every sample replaces only this fresh benchmark's working
directory with independent copies of that image, at the **same canonical paths**.
The runner checks a complete path/size/SHA-256 manifest before native open and
checks the sealed image throughout the run. Hard links and symbolic links are
rejected. No existing deletion intent is copied: intents bind device/inode, and
the collector must register the restored files' actual identities itself.

The native process checks all 96 typed business rows, the history table and the
current named ANN query outside timing. Before each measured call, an ordinary
transaction must defer collection without changing snapshots, queues, counters
or index references. After rollback, the measured call must complete its exact
two-object task. It must leave no deletion intent, preserve current business
rows/index references and advance durable counters by the reported progress.
The runner independently checks the actual deleted paths, unchanged retained
source contents and unchanged index artifacts.

Copying, hashing, opening, validation and the deferral check are excluded from
collector timing. They warm the local files and metadata. These are warm local
observations, not cold-storage I/O measurements.

## Timing and SQL nesting

The primary interval is `Source::Collect` entry to return, including admission
gates, native metadata transactions, source verification and file operations.
It uses the existing five source-phase timers: wait, metadata, expiry, artifact
and unlink. Their completed scopes must not exceed the complete collector
interval. Readiness probes, other maintenance classes, worker scheduling and
ANN execution are outside this interval.

The runner saves its measurement contract before preparing inputs. Each of twelve
cases has two excluded warmups and sixteen primary samples. Cases rotate each
round and reverse on odd rounds. Only after all 192 primary samples finish do
three separately restored diagnostic processes per case run. No added SQL
callback is registered in primary processes. The additional 24 warmups and 36
diagnostics remain identifiable in raw evidence and do not enter primary
statistics.

Diagnostic callbacks record SQL, connection identity, nesting depth, begin/end
intervals, post-prepare timing and collector checkpoints. An outer DuckLake
`COMMIT` contains a native metadata `COMMIT`; summing both would double count
transaction time. Aggregation therefore uses only nonoverlapping depth-zero
queries. Nested records remain in raw evidence and require a containing parent.
The post-prepare hook copies the AST; those instrumented times explain work but
are excluded from primary statistics. Binding/planning intervals start at
query-begin and exclude any parsing before that callback.

Five analysis regressions reject reduced restored history, zero-progress
samples, warmup/diagnostic contamination, linked inputs and nested-commit double
counting. The full measurement is a baseline attribution, with no candidate
implementation or A/B retention decision.

## Reproduce

Use the unchanged native provider closure in
[VaneNativeVortex.yml](../.github/workflows/VaneNativeVortex.yml):

```sh
cmake --build "$SKBUILD_BUILD_DIR" --target ducklake_vortex_source_gc_benchmark --parallel 2
python -I test/vane/test_vane_vortex_source_gc_stats.py
env -u VORTEX_SPFRESH_NATIVE python -I test/vane/test_vane_vortex_source_gc_benchmark.py \
    --driver "$SKBUILD_BUILD_DIR/ducklake_vortex_source_gc_benchmark" \
    --output-dir /tmp/vane-vortex-source-gc-cost
```

The output directory must be new. The benchmark retains the sealed input
images, exact manifests, per-process logs, raw samples, contract and aggregate
report. CI runs both storage modes and tasks at history 8, two primary samples,
one warmup and one diagnostic per task. It preserves evidence without applying
local latency thresholds to shared runners.

## Local observations on 2026-11-06

All twelve cases completed: 192 primary collector calls, 24 excluded warmups and
36 separate diagnostics. Input manifests, ordinary-transaction deferral, exact
progress, counter/intent checks, typed rows, current ANN, retained source bytes
and index artifacts passed. Total elapsed time including preparation, restoration
and diagnostics was 580.700 seconds.

Values below are primary medians in milliseconds, with sixteen samples per
cell. `file` means materialized history; the two deleted objects remain the same
retired Vortex sources. These are observations across independently prepared
cases, not a paired runtime-candidate comparison.

| History updates | History storage | Expire two snapshots | Reclaim two files |
| ---: | --- | ---: | ---: |
| 8 | inline | 28.647 | 37.816 |
| 8 | file | 29.200 | 37.993 |
| 512 | inline | 29.996 | 38.044 |
| 512 | file | 37.688 | 43.467 |
| 4,096 | inline | 31.230 | 37.385 |
| 4,096 | file | 197.127 | 82.882 |

The large materialized expiry case averaged 191.337 ms, of which 188.034 ms was
the expiry phase and 3.261 ms other source metadata. Its p95 was 204.770 ms.
The large materialized file case averaged 82.044 ms: metadata 79.344 ms, artifact
verification 0.381 ms and unlink/sync 2.315 ms. Its p95 was 85.446 ms. Admission
waits were below 0.002 ms on average in this uncontended fixture; this does not
measure ordinary-reader contention in the sustained workload.

The separate diagnostics identify the ended-data-file interval query in
`DeleteSnapshots` as the first target. The predicate retains an ended file while
any surviving snapshot satisfies `begin_snapshot <= snapshot_id < end_snapshot`.
For materialized histories of 8/512/4,096 updates, that one query averaged
2.024/8.943/164.928 ms per instrumented collector. This is not a primary latency
or a claim that those times can be subtracted directly from the primary scopes.

For the materialized file task, four native-reference enumerations averaged
8.519/10.472/22.722 ms in SQL callbacks. `HasFileReference` also fetches and
canonicalizes returned paths in C++, outside those query callbacks. Source
metadata phase time therefore cannot be explained by summing SQL durations
alone. Five outer commits per file collector averaged 6.822/6.727/6.505 ms in
total, rather than counting the nested metadata commits a second time.

The inline control alone would have hidden most of the growth. The materialized
fixture isolates a reproducible scale-sensitive query while retaining the same
live vector rows and two-object work. It still does not establish the complete
cause of the thirty-minute backlog: concurrent writes/readers, worker mix and
other metadata histories are excluded.

## Follow-up and qualification

Retain this baseline and first validate an equivalent ended-data-file candidate
query. Correctness must include snapshot gaps, interval boundaries, remaining
historical references, table/schema versions and deletes before a paired A/B
can justify changing production. Each A/B collector sample must restore the same
input image. Preserve ordinary-transaction exclusion, publication/capture gates,
all ownership/reference checks, deletion intents and existing budgets. Do not
remove checks, change scheduling or increase budgets on the strength of these
baseline timings.

An accepted runtime candidate would then need the existing snapshot/recall
oracle, both 0.8 quality gates, ninety-second drain, final rows and old-receipt
replay in the same thirty-minute workload. This test-only change makes no new
thirty-minute or sustained-stability claim and adds no compatibility, fallback
or runtime selector. R4 remains open.

Local report:
`/tmp/ducklake-vortex-source-gc-full-v3-20261106/report.json`.
Report SHA-256:
`48855d42a60fd423caf0df052e46bb95050003e61f0ac83b570f03be3f5cf042`.
Measured native executable SHA-256:
`168562ccee3d4cc66659d4bb8b18a5b891a33ecbdd2424f99736a22e0ec0f400`.

The final native benchmark was compiled and linked against the unchanged
qualified static provider closure. Its four-case CI-shaped smoke and all five
analysis regressions passed. Installed source-retention qualification passed
38 native cases and sixteen public oracle comparisons with minimum fixture
recall 1.0, including transaction, held-view, budget, cancellation and recovery
checks. The matching Vane base launcher passed 1,222 non-Ray and 32 Ray tests in
separate processes, with fifteen optional-dependency skips. Formatting, copyleft,
workflow YAML and documentation-link checks passed. Production `src/` and all
three installed Vane/DuckLake/Vortex wheel bytes are unchanged.

Installed-provider qualification ran in the host environment with normal root
ownership. The managed sandbox maps root-owned ancestors to UID 65534, which
correctly causes the existing verified-cache ancestor check to reject provider
loading. The original failed launch and successful host rerun are retained;
no cache, trust or signature check was bypassed.
