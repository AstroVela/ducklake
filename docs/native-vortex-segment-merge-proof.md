# Selective native Vortex segment compaction

This proof follows [DuckLake #41](https://github.com/AstroVela/ducklake/pull/41).
It permits three B segments under the existing aggregate budgets and merges
only a selected pair. For B1 + B2 + B3, merging B2/B3 publishes B1 + B23. B1's
generation, descriptor, artifact bytes and modification times remain unchanged.
Ordinary append still builds only new F. The pinned vortex-index and static
SPFresh implementation are reused without online Add/Delete calls.

## Explicit selection and atomic replacement

```sql
SELECT * FROM ducklake_merge_vortex_index_segments(
    'lake','main','items','knn','/absolute/existing/root/B23.json',
    ['generation-B2','generation-B3'],3);
```

The selection must name exactly two distinct ready generations; the last
argument is the expected catalog revision. Build checks the complete physical
inventory against the existing row/vector/source budgets, then pins and decodes
only those segments' physical files. It includes all physical rows, including
currently deleted rows, to preserve the existing native row-map contract.
Concurrent F/T stays queryable and cannot enter the sealed merge candidate.

`ducklake_publish_vortex_index_merge` accepts the same arguments to publish an
already sealed candidate. It checks the revision, selected generation identities,
and exact selected file coverage, preserves all unselected descriptors, appends
the replacement, and atomically commits the whole catalog manifest. Stale,
partial, expanded or corrupt references fail. Construction holds no metadata
transaction or publication lock. Publication still verifies remaining B source
and artifact identities; it does not decode or rebuild unselected B.

The strict version-2 segment manifest format is unchanged. Three active segments
share the existing 100,000 physical-row and 64 MiB vector/source/artifact ceilings
and 1 MiB catalog-manifest limit. Direct append at the three-slot limit fails
explicitly. The new selected build cannot bypass admission by capturing only a
small pair from an oversized table. Ordinary new F still requires at least 65
physical rows; [head planning](native-vortex-posting-coverage.md) sizes larger
builds within the same posting page budget.

## Scheduled maintenance and recovery

When F is due and all three slots are occupied, the background optimizer plans
a merge of the two segments with the smallest physical row counts. Ties use
generation order. The dimension is fixed, so physical row count also orders
their vector byte counts. This is a separate planned task before append. After
the merge publishes, a later task rechecks current committed F and builds it
into the released slot. F arriving during the merge stays in that later capture.

The durable optimizer table is now `ducklake_vortex_optimizer_v2`, with explicit
task kind and selected-generation fields. Status exposes both. Initialization
is explicit and requires a fresh prototype catalog; there is no migration,
compatibility decoder or fallback. Failed uncommitted tasks disable the policy;
explicit resume plans from fresh state. Quotas remain active throughout.

Before publication, the optimizer persists the sealed candidate and expected
revision. A lost ACK or explicit resume after restart reconciles the exact
successor generation. A committed merge is not rebuilt. An uncommitted merge
is not replayed; a new task captures a fresh selection. Restart does not start
workers implicitly. Cancellation remains cooperative at native checkpoints.
The completed-merge notification runs after terminal state persistence and
does not recheck cancellation. Cancellation at this boundary preserves the
committed head and its disabled, error-free `cancelled` policy state through
worker release and reopen. Task claim checks the current shared registration
while holding the registration I/O lock, which resume also acquires. Completion and error paths ignore a
superseded registration, so a resumed task cannot inherit an older scheduling
pass's callbacks or terminal error.

Index compaction keeps the source file layout and row addresses. It releases
an active segment slot, not physical table capacity. Explicit full build remains
available for operator-requested replacement. Automatic source-file rewriting,
abandoned generation collection and production count/size policies remain
separate work. The [generation GC follow-up](native-vortex-generation-gc-proof.md)
adds registered retirement and bounded explicit collection of published
generations. This merge proof still removes canonical roots directly to qualify
existing view leases.

## Qualification

```sh
python3 -I test/vane/test_native_vortex_merge.py \
    --duckdb build/index-shell/duckdb \
    --driver build/index-shell/ducklake_vortex_catalog_driver \
    --probe build/index-cargo/ci/ducklake-vortex-index-probe \
    --output-dir build/index-logs/segment-merge
```

The proof validates exact B2/B3-only capture, B1 checksum/mtime invariance,
three-segment ANN with F/T, predicates and delete/update visibility, concurrent
business writes during construction, aggregate lease failure cleanup,
cancellation, corrupt/wrong/stale candidates and publication CAS races.
It runs repeated manual and background merge/append cycles under unchanged
write quotas, terminal failure, lost ACK, same-process reopen and SIGKILL before
and after commit. Old native and independent three-B views query after selective
replacement and actual retirement of both canonical generations; delayed typed
fetch checks old row versions, nulls, Unicode and duplicates. Strict database
shutdown and empty private lease directories are required. Two- and three-segment
phase checks accumulate ANN/rerank work across every segment. Their measured
observer-delay lower bounds validate attribution without a stable-load speed
comparison. SQL tests and the
existing ON/OFF regression/recovery and process proofs remain in CI.

Local Linux qualification on 2026-11-17 used functional source `8e458899`,
with reviewed #41 `32efc38c` merged in. DuckDB v1.5.5 (`d8cdaa33`), Vortex
`b20e34c2`, codec `84cef204`, and SPFresh `5893eb61` with Vortex's pinned
static-only patch were preserved. The final catalog driver SHA-256 was
`058849f939e48525f213d91b700120f7f36eb2839053ab5f6bc90a402d060258`.

The final merge proof passed 13 checks and 63 fixture queries at recall 1.0
in 28.9 seconds under the existing 180-second outer budget. The two-segment
proof passed 11 checks and 24 queries at recall 1.0. Both two- and three-segment
ANN/rerank measurements covered their accumulated observer delays. The completed
merge cancellation regression failed against the pre-fix driver and passed
after the fix; it waits for the worker's database pin to be released before
checking the durable terminal state, then checks that state again after reopen.

ON passed 22 Vortex SQL cases / 864 assertions. ON and OFF each passed 86 base
cases / 3,582 assertions with 13 optional-dependency skips. ON deletion inlining
passed 18 cases / 1,099 assertions with two optional-dependency skips; audit
passed 122 assertions. Native search/catalog/admission, retained and mixed views,
Vortex/Parquet recovery, cleanup, build, optimizer, lifecycle and all 13
write-budget cases passed. The 32-case ABI matrix passed. Query/probe Rust
formatting, workspace Clippy with all targets/features and warnings denied,
workspace tests and scoped pinned C++/Python formatting passed. Existing
Float32 tolerances (`rel=1e-5`, `abs=1e-4`) and recall threshold 0.8 were retained.

Seventeen ordinary functional gates kept 180-second budgets. The separate
counter/recall gate retained its existing 600-second outer limit and completed
in 14.9 seconds with four cases / 36 queries at recall 1.0. Native RPC and
cleanup limits remain 60 and 30 seconds. Source, header, object, link and SDK
identities qualify reused build inputs; the optimizer change was compiled and
the native products relinked normally. These checks make no stable-load
throughput or latency claim. CI was deferred.

A supplementary pre-fix experiment merging nonadjacent B3+B1 (4,224 physical
rows) exceeded the existing 60-second RPC deadline once. Its unfinished native
artifacts and logs were retained. Four subsequent pre-fix runs and three fixed
runs with the same SQL data passed, including reverse selection, two-to-one
merge and fresh append. Fixture UUIDs/generations differ across runs. The
initial timeout's root cause remains unconfirmed; passing reruns do not
establish a fix for that anomaly.
