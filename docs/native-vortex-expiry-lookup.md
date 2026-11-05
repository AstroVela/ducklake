# Ordered retained-snapshot lookup for source expiry

**Decision on 2026-11-06: withdraw the runtime candidate.** Both target cases
improve, but two file-unlink controls exceed the unchanged 2% regression gate.
Production `src/` is byte-for-byte identical to #77
(`beaff69ac8a098be875d483b4c575e8d6c2e6d4a`). The algorithm and patch remain only
under `test/vane`, excluded from provider artifacts. All three installed wheel
bytes, transaction protection and existing budgets are unchanged.

This validation follows the [withdrawn window candidates](native-vortex-expiry-interval.md).
It targets the ended-data-file range anti-join identified by the
[actual source-GC baseline](native-vortex-source-gc-cost.md). It retains the
same restored inputs, collector budgets and predeclared A/B decision rule.

## Algorithm and scope

The frozen [lookup helper](../test/vane/common/ducklake_snapshot_lookup.hpp)
and [metadata-manager patch](../test/vane/expiry_snapshot_lookup.patch) are test
references. The [trial builder](../test/vane/build_vortex_lookup_trial.py)
compiles the patched translation unit privately against the baseline closure.

Read retained snapshot IDs in ascending order after removing the requested
snapshots in the existing metadata transaction. Read ended data-file rows,
including interval endpoints, plus every file belonging to a fully expired
table. For a file interval `[begin, end)`, find the first retained snapshot
`lower_bound(begin)`. The file is unreferenced exactly when that iterator is at
the end or its snapshot ID is at least `end`. This works across snapshot holes,
and equality at `end` does not retain the file.

A fully expired table still forces cleanup independently of its file intervals,
including NULL-ended active records. Otherwise a NULL end keeps the file;
a NULL begin cannot match a snapshot under the original SQL comparisons.
The candidate replaces only ended-data-file selection. The selected files
continue through the existing path resolution, metadata deletion and deletion
queue transaction. Delete-file and table/schema cleanup queries, publication
and ordinary-transaction gates, ownership/reference checks, durable deletion
intents and scheduling retain their existing contracts. There is no runtime
query selector, compatibility layer or fallback.

With `N` retained snapshots and `M` candidate files, the lookup performs
`O(M log N)` comparisons after obtaining sorted snapshots. The additional
retained-ID vector is `O(N)`; the materialized ended-file result is `O(M)` and
includes paths that the original SQL filtered out remotely. The existing
object, source/vector-byte and artifact budgets do not cap these metadata
allocations, process RSS or total catalog size. This remains a capacity issue
for R5 rather than a new unvalidated admission limit.

## Correctness checks

The compiled lookup is checked against an independent Python interval oracle
and the original NOT EXISTS query on native Vane and SQLite. The 356 cases
include every retained subset of boundary sets, empty catalogs, gaps,
equal/reversed/NULL endpoints, forced table cleanup, seeded random intervals,
long retained tails and signed BIGINT extremes. The native 31-block SQL fixture
exercises real expiry across holes, rename, exclusive endpoints, dropped-table
NULL-ended files and retained/dropped schema history.

## Cost measurement

A separate native probe copies only metadata from the six expiry input images,
removes the same two oldest snapshots inside a transaction, executes the actual
original query or lookup queries and rolls back. It checks identical eligible
file counts. Three fresh processes per variant and case give 36 samples.
This probe accounts for metadata query results and the retained-ID vector;
it does not time the worker, ANN or complete source collector.

The result records row/column counts, DuckDB collection size and allocation,
vector size/capacity, and current RSS at query boundaries from
`/proc/self/statm`. `ru_maxrss` is also retained but can include an inherited
startup high-water mark. Current RSS boundary differences are descriptive,
not a within-query maximum, a total-memory guarantee or an admission cap.
The snapshot query result is released before the file query; its allocation
should not be added to the file result as though both remained live.

## Actual-collector A/B contract

Both executables share the same benchmark object and dependency closure;
only the candidate metadata-manager translation unit is replaced. Each call
restores and hashes the same sealed image at its original canonical paths.
Twelve cases cover 8/512/4,096 updates, inline/materialized history and
snapshot-expiry/file-unlink tasks. Ordinary-transaction deferral, exact
object/byte progress, typed current rows, named ANN, source/index identities,
intents and counters are checked on every call.

Four excluded warmups per case precede sixteen rotated/reversed ABBA/BAAB
paired rounds, with two calls per variant per round. One separate diagnostic
per variant and case keeps added SQL callbacks out of primary timing. The
matrix contains 768 primary calls, 48 excluded warmups and 24 diagnostics.
Both materialized-expiry targets at 512/4,096 updates must improve at least
2% in median paired ratio, have a descriptive bootstrap 95% upper ratio below
1 and win at least thirteen of sixteen rounds. All ten controls must have
median regression at most 2%; otherwise the candidate is withdrawn.
These are the unchanged #77 rules, including controls that do not execute
`DeleteSnapshots`. No concurrent build or qualification runs during timing.

## Completed result and decision

The 768 primary calls, 48 excluded warmups and 24 separate diagnostics all passed
functional and input-identity checks. The full run took 653.880 seconds including
restoration and diagnostics. The materialized-expiry targets improved
22.926%/84.198% at 512/4,096 updates, each winning all sixteen rounds. Their
descriptive bootstrap 95% paired-ratio intervals were 0.75049–0.80902 and
0.15132–0.16189. The large target's individual-sample p50 was
196.707/31.100 ms, baseline/candidate. Round-mean paired ratios and ratios of
individual-sample medians are different statistics.

| History | Storage | Task | Paired ratio | Descriptive 95% interval | Winning rounds |
| ---: | --- | --- | ---: | --- | ---: |
| 8 | inline | expire | 0.97866 | 0.91925–1.01757 | 11/16 |
| 8 | inline | files | 1.02838 | 0.98636–1.05096 | 5/16 |
| 8 | file | expire | 0.96699 | 0.91986–1.02987 | 10/16 |
| 8 | file | files | 1.01035 | 0.99695–1.02187 | 5/16 |
| 512 | inline | expire | 0.95307 | 0.93096–0.98417 | 13/16 |
| 512 | inline | files | 1.01036 | 0.99046–1.01853 | 6/16 |
| 512 | file | expire | 0.77074 | 0.75049–0.80902 | 16/16 |
| 512 | file | files | 0.98550 | 0.96436–1.00009 | 12/16 |
| 4,096 | inline | expire | 0.98200 | 0.96419–1.02104 | 10/16 |
| 4,096 | inline | files | 0.99162 | 0.98641–1.00391 | 11/16 |
| 4,096 | file | expire | 0.15802 | 0.15132–0.16189 | 16/16 |
| 4,096 | file | files | 1.02354 | 0.97201–1.03912 | 5/16 |

The 8-update inline and 4,096-update materialized **file-unlink** controls
regressed 2.838%/2.354%, beyond the 2% rule. Both descriptive intervals contain
1. Every sample in these two controls records zero expiry calls, zero expiry
time and zero expired snapshots: they do not execute the modified selection.
This is a failed predeclared gate, not proof that the lookup caused either
slowdown. All snapshot-expiry controls pass the regression rule. The evidence
does not justify relaxing controls, adding a size-dependent selector or keeping
the runtime candidate regardless. The candidate is withdrawn.

Report: `/tmp/ducklake-vortex-expiry-lookup-ab-full-20261106/report.json`.
SHA-256: `4c2efac82e715958e90f1a75fce4029433d9fe9ba5312d68acedc2cf88129cef`.

## Measured metadata and memory cost

Both queries/probes still select two eligible files in every expiry case. The
lookup reads all retained IDs and all ended-file rows, including retained paths:

| History | Storage | Retained ID rows | Ended file rows, original/lookup | Lookup file-result allocation | ID-vector capacity | Current RSS delta, original/lookup MiB |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 8 | inline | 11 | 2/2 | 233,472 B | 128 B | 3.625/1.262 |
| 8 | file | 11 | 2/10 | 233,472 B | 128 B | 3.848/1.117 |
| 512 | inline | 515 | 2/2 | 233,472 B | 8,192 B | 3.703/1.059 |
| 512 | file | 515 | 2/514 | 258,048 B | 8,192 B | 5.102/1.148 |
| 4,096 | inline | 4,099 | 2/2 | 233,472 B | 65,536 B | 3.625/1.191 |
| 4,096 | file | 4,099 | 2/4,098 | 749,568 B | 65,536 B | 7.383/2.320 |

At 4,096 materialized updates, original/lookup file-result collection sizes are
68,714/522,858 bytes; their allocations are 102,400/749,568 bytes. The lookup
snapshot result has 49,920 collection bytes and 98,304 allocated bytes, and is
released before the file query. The ID vector holds 32,792 bytes with 65,536-byte
capacity. Thus the file-result allocation plus ID capacity is 815,104 bytes,
versus the original file-result allocation of 102,400 bytes; this accounting
excludes query operators, allocator retention, chunks and other live state.
The lower observed current-RSS delta is consistent with avoiding the range
anti-join, but does not prove a maximum-RSS improvement or bounded catalog
memory. Remote metadata transfer and remote backends were not qualified.

Final CMake-probe report:
`/tmp/ducklake-vortex-expiry-lookup-final-cmake-resources-20261106/report.json`.
SHA-256: `021dc12d1d6edd9648402112449fd170a0cea5fbd2e893c293a92ccd8ea39985`.

## Reproduce

Build the normal qualified native **baseline** closure from the restored
production sources, then build the test-only helpers. The trial builder applies
the frozen patch in a fresh temporary directory and links an isolated override;
it does not apply the patch to repository or provider sources. On this pinned
local closure, both generated executable hashes match the actual A/B binaries:

- baseline: `168562ccee3d4cc66659d4bb8b18a5b891a33ecbdd2424f99736a22e0ec0f400`
- lookup: `79906a68eef952a9e162ae73eca21dd610c70ca9dc33f7416ec0bae4f8fa1105`

```sh
cmake --build "$SKBUILD_BUILD_DIR" --target \
    ducklake_vortex_expiry_lookup_driver ducklake_vortex_expiry_lookup_resource
python -I test/vane/build_vortex_lookup_trial.py \
    --build-dir "$SKBUILD_BUILD_DIR" --output-dir /tmp/lookup-trial-build
env -u VORTEX_SPFRESH_NATIVE python -I test/vane/test_vane_vortex_source_gc_benchmark.py \
    --driver /tmp/lookup-trial-build/baseline --output-dir /tmp/lookup-trial-inputs
VANE_RUNNER=local-fast python -I test/vane/test_vane_vortex_expiry_lookup.py \
    --driver "$SKBUILD_BUILD_DIR/ducklake_vortex_expiry_lookup_driver" \
    --output-dir /tmp/lookup-interval-proof
python -I test/vane/test_vane_vortex_expiry_lookup_resources.py \
    --driver "$SKBUILD_BUILD_DIR/ducklake_vortex_expiry_lookup_resource" \
    --inputs /tmp/lookup-trial-inputs/report.json --output-dir /tmp/lookup-resource-proof
python -I test/vane/test_vane_vortex_source_gc_ab_stats.py
env -u VORTEX_SPFRESH_NATIVE python -I test/vane/test_vane_vortex_source_gc_ab.py \
    --inputs /tmp/lookup-trial-inputs/report.json \
    --baseline-driver /tmp/lookup-trial-build/baseline \
    --candidate-driver /tmp/lookup-trial-build/lookup --output-dir /tmp/lookup-ab
```

Output directories must be new. Restorable inputs must remain at their original
canonical paths and have no competing writer or benchmark. Shared CI runs
correctness and metadata-accounting smokes; it does not enforce a performance
gate on shared runners.

## Next validation

R4 remains open. Before another runtime candidate, run an identical-binary A/A
control with the same restored images, work, paired order and 2% resolution
target, including the two non-expiry controls that failed here. Predeclare its
pass/fail interpretation and sufficient sample count. If the harness cannot
resolve 2% on identical work, isolate scheduling/frequency/cache drift and
improve measurement precision before attempting another A/B. Do not reinterpret
this withdrawn result as a pass or simply rerun until it passes.

There is no retained runtime change, so this PR makes no new thirty-minute
sustained-load measurement or stability claim. After a later candidate clears
a justified A/B gate, run the same 30-minute load with actual ReadView oracles,
both recall thresholds >=0.8, <=90-second drain, final rows and old receipts.
R4 additionally needs repeated longer observations of backlog slope and
resource growth during continued writes.

## Final local qualification after withdrawal

Both CMake helper targets build successfully. The compiled test-only lookup,
native Vane SQL and SQLite SQL each pass all 356 oracle cases. The native
history fixture passes all 31 blocks with the candidate and again with the
restored original implementation. All four paired-analysis regressions pass,
and the final metadata probe passes all 36 samples with unchanged sealed images.
The private trial builder reproduces both actual A/B executables byte-for-byte;
no additional performance run replaces the withdrawn matrix.

Installed source qualification passes 38 native cases and sixteen public query
comparisons, with minimum fixture recall 1.0. The Vane base launcher passes
1,222 non-Ray and 32 Ray tests in separate processes, with fifteen optional
dependency skips. Installed tests preserve the host's normal ancestor ownership,
provider signatures and cache checks. Source files and all three installed wheel
bytes remain unchanged. Formatting, copyleft, workflow YAML, document links
and diff checks pass.
