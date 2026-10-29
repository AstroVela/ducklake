# Explicit sparse-B compaction to bounded F

This validation follows [DuckLake #49](https://github.com/AstroVela/ducklake/pull/49).
An indexed B with only 1–64 visible rows can release its deleted physical rows
by rewriting its complete source inventory into an explicit F plan. It creates
no replacement SPFresh generation. The existing fixed 64-head build options
still need at least 65 physical rows; that constraint does not force a sparse B
to retain its entire original physical inventory.

For larger B whose deletions affect only whole files, see the subsequent
[atomic empty-file pruning proof](native-vortex-prune-files-proof.md).

## Public operation and planning

```sql
SELECT * FROM ducklake_rewrite_vortex_index_segment_to_f(
    'lake','main','items','knn','generation-<uuid>',<expected_revision>);
```

The operation requires an installed optimizer/write-budget policy for the same
named index and exactly one named index for the table. Native visibility must
confirm 1–64 selected rows. Empty B uses the existing retirement operation;
larger B uses the indexed rewrite operation. The mode is selected explicitly
before copying, and an ANN failure never selects the F path.

The existing opt-in compaction policy applies its row/percentage thresholds,
then chooses `retire` for zero remaining rows, `rewrite_f` for 1–64 rows, or the
existing indexed `rewrite` for a supported larger candidate. It retains the
single worker, largest-reclaimable candidate selection and current global
budgets. Status explains `unindexed_row_budget` or `unindexed_byte_budget` when
known F admission limits defer a candidate. Deferral creates no task, intent or
file and does not retry a failed native operation. Resolving the limiting
condition allows an enabled policy to plan fresh work.

Preflight checks current F rows plus selected visible rows and uses selected
input source sizes as a conservative byte estimate. A tight byte budget can
therefore defer an output that might compress smaller. After encoding, write
admission checks actual prospective file sizes, physical/vector limits, F/T
quotas and retained inline versions. Final commit repeats admission against
fresh committed state under the existing reservation. Concurrent growth or
input changes that invalidate prepared work stop the task; there is no
unbudgeted publication or automatic failed-task replay.

## Source/index handoff

The native compactor includes every file in the selected B, including members
with no deletions. This handles fully masked large files plus an undeleted
small file without a zero-output delete-only rewrite. The same fixed-schema,
unpartitioned, unencrypted Vortex/native DuckDB metadata and single-output
prototype scope applies.

The private output must contain exactly the captured visible row count and
replace exactly the selected source IDs. Native row IDs and typed business
columns are preserved. The existing registered workspace/source-intent protocol
owns the private file and its exclusively linked UUID destination. The build
registry reserves its normal UUID namespaces, but the F operation creates no
ANN directory, prepared bundle or native SDK build. A committed F task marks
its owner `published` with `keep_generation=false`; source retention is recorded
independently. Recovery and bounded GC preserve the published F while deleting
abandoned private data and workspace ownership.

Under write admission and exclusive publication, the protocol revalidates the
exact index revision, target schema, source identities and delete metadata.
It rebases to the fresh native snapshot and binds the output's actual file ID,
accepting independent committed T/F appends during preparation. It verifies
the output ID, path, row count, size and SHA-256, retires all selected old files,
and removes that B from the typed catalog plan in one metadata transaction.
Unselected B descriptors and artifact bytes remain unchanged.

`rewrite_f` has no candidate ANN generation or reference JSON. Its durable run
UUID is the plan publication identity, so restart/lost-ack reconciliation checks
exact revision and UUID rather than attributing another publisher's work to the
task. Cancellation, conflicts and errors stop scheduling. Restart starts no
optimizer/maintenance work; explicit resume inspects committed work and plans
fresh tasks only when necessary.

## Querying and later indexing

The new ReadView composes remaining B plus current F/T, or the existing explicit
zero-B plan when this was the last B. Visibility, category filtering, exact F/T
distances, typed take and global top-k use the current query implementation.
No compatibility layer, manifest migration, implicit fallback, dependency-pin
change or SPFresh online update API is added.

F stays queryable below 65 physical rows even when configured thresholds are
lower. Once both the native minimum and configured row/byte trigger are met,
the existing optimizer can build the new first B or append/merge through its
normal segment policy. The three-B, 100,000-row, 64-MiB source/vector/artifact,
inline, write, manifest and collection budgets are unchanged.

Old indexed and new F views retain private sources and support delayed typed
fetch after canonical index and source GC. Old data/delete files remain under
native snapshot retention. This recovers current physical row/vector quotas;
historical disk use still depends on explicit snapshot expiry and cleanup.

## Reproduction

```sh
python3 -I test/vane/test_native_vortex_sparse_rewrite.py \
    --duckdb build/index-shell/duckdb \
    --driver build/index-shell/ducklake_vortex_catalog_driver \
    --probe build/index-cargo/ci/ducklake-vortex-index-probe \
    --output-dir build/index-logs/sparse-rewrite
```

The proof covers concurrent T/flush/T, final file-ID binding, unchanged B,
current/held query and typed-fetch parity, 0/1/64-row boundaries, undeleted source
members, admission deferral and a final admission race, later automatic first-B
building, corrupt/foreign output protection, failure/cancel/input/publication
races, committed acknowledgement loss and seven SIGKILL checkpoints. A public
SQL case covers explicit admission, source/index publication, native snapshot
parity, nullable/Unicode business rows and the final zero-B query plan.

## Local result (2026-10-29)

The process proof passed 29 checks and 104 gated query comparisons with minimum
fixture recall 1.0. At the unchanged 100,000-row/vector ceiling, a 4,096-row B
became a 32-row F, released 4,064 slots, retained 95,904 pre-existing F rows and
admitted a new T. Independent T/flush/T commits consumed provisional file IDs;
publication used the actual final ID and preserved both unselected generations.
Old/new retained search and delayed typed fetch survived canonical index,
workspace and explicit source GC.

All seven SIGKILL checkpoints, failure/cancel/input/budget/publication races,
committed acknowledgement loss, corrupt-output verification and foreign-output
preservation passed. Known F row/byte exhaustion deferred without allocation or
repeated tasks. The native fixed-schema/partition DDL guards were reused and
verified. Tiny F remained queryable at 32 and 64 physical rows with configured
index threshold 1; a later batch automatically built the new first B.

All 19 prior native process proofs passed. ON passed 20 Vortex SQL cases / 762
assertions, including the new 56-assertion public-API case, plus Vortex recovery.
ON/OFF each passed 86 base cases / 3,582 assertions with 13 optional-extension
skips and Parquet recovery. OFF exposes no F rewrite API. Additional indexed
rewrite/orphan/audit groups passed 17 cases / 976 assertions with two ICU skips.
Four isolated cost cases passed nine queries each with minimum recall 1.0;
timings remain informational. Formatting and diff checks passed. The global
budgets, native dependencies and Rust query sources are unchanged.

The qualified catalog driver SHA-256 is
`0fc56b71292a459be5bba2cc8fda0f3b59764155d1716c0933e65c1c660683b7`.
