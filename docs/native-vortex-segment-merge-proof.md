# Selective native Vortex segment compaction

This validation draft follows [DuckLake #41](https://github.com/AstroVela/ducklake/pull/41).
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
Task claim reads the current policy registration under the scheduling lock, so
an explicit resume cannot inherit callbacks copied by an earlier scheduling pass.

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
shutdown and empty private lease directories are required. SQL tests and the
existing ON/OFF regression/recovery and process proofs remain in CI.

Local qualification kept DuckDB v1.5.5 (`d8cdaa33`), Vortex `65dab44f`, the
native wrapper `b819cc03` and the qualified static SPFresh SDK unchanged from
#41. The final catalog driver SHA-256 was
`a28b341ac3d86d8ec04b8796102a41564f91751c5a9da1966fc761fddcaf4458`.
The merge proof passed 11 checks and 57 fixture queries with recall 1.0. Its
background case completed four merge/append cycles at revision 11, with nine
runs including the cancelled task, while preserving the unselected B1.
ON passed 13 Vortex SQL cases (491 assertions) and Vortex crash recovery.
ON/OFF each passed 86 base cases (3,582 assertions; 13 optional-dependency
skips) and Parquet crash recovery. Audit passed 122 assertions. Existing index,
read-view, mixed, admission, catalog, SQL search, build, optimizer, lifecycle,
write-budget and independent-segment process proofs passed. The isolated cost
proof passed four cases with nine queries each and recall 1.0; timings are
informational. `make format-fix`, Rust formatting and workspace Clippy with
all targets/features and warnings denied completed successfully.
