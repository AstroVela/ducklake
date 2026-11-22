# Explicit sparse-B compaction to bounded F

This validation follows [DuckLake #49](https://github.com/AstroVela/ducklake/pull/49).
An indexed B with only 1–64 visible rows can release its deleted physical rows
by rewriting its complete source inventory into an explicit F plan. It creates
no replacement SPFresh generation. The existing fixed 64-head build options
still need at least 65 physical rows; that constraint does not force a sparse B
to retain its entire original physical inventory.

## Public operation and planning

```sql
SELECT * FROM ducklake_rewrite_vortex_index_segment_to_f(
    'lake','main','items','knn','generation-<uuid>',<expected_revision>);
```

The operation requires a writable autocommit caller. It binds both the
DuckLake catalog and its metadata attachment to the writer and requires an
installed optimizer/write-budget policy and one named index for the table.
Visibility comes from the union of file and inline delete positions for each
selected file ID. The complete B must have 1–64 visible rows. Empty B uses the existing retirement operation;
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
therefore defer an output that might compress smaller. Final write admission uses the encoded file size and fresh committed
state to check physical/vector limits, F/T quotas and retained inline versions. Concurrent growth or
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

The commit marks the old source endings and the replacement file as physical.
The change feeds report zero rows at the rewrite snapshot and retain the
original insert and delete history. Adjacent-file merge carries those physical
snapshot IDs with the replacement file. The insertion scan filters those row
snapshots and retains real inserts in the same merged file. Time travel uses
the original row snapshots. Later UPDATE and DELETE on F still emit logical
changes. Snapshot expiry removes the markers with their file metadata.

`rewrite_f` has no candidate ANN generation or reference JSON. Its durable run
UUID is the plan publication identity. The same metadata commit records the
optimizer run and its published revision. Recovery can reconcile that receipt
after another publication advances the index. Cancellation, conflicts and
errors stop scheduling. Restart starts no
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
SQL case also checks caller rollback and read-only attachment guards, historical
changes across mixed delete masks, two successive mixed-file merges, and UPDATE
and DELETE after publication.
After a commit-time crash, a separate append advances the index before resume.

## Local verification against #49

DuckLake `c6a47722` incorporates #49 at `d87d5481`. The native builds
use DuckDB `d8cdaa33`, Vortex `b20e34c2`, and the
DuckDB Vortex wrapper `84cef204`.

The sparse proof passed 31 checks and 108 gated
query comparisons with minimum recall 0.9 in 60.12
seconds within the 180-second budget. The recovery case with a later append
reached the lowest recall and passed the existing 0.8 gate.
A 4,096-row B became a 32-row F and released
4,064 physical row slots. The public SQL case passed 123
assertions. It verified rollback, read-only attachment rejection, zero changes
at both rewrite snapshots, stable time travel after two adjacent-file merges,
and exact logical inserts from the two appends.

All 20 native process proofs passed. ON passed 29
Vortex SQL cases and 1294 assertions. ON and OFF passed
86 base cases and 3582 assertions each with
13 optional-extension skips per configuration. The complete
DELETE and table-change suites, Vortex and Parquet recovery and cleanup,
32 ABI cases, isolated cost checks, formatting, Clippy and Rust workspace
tests passed. The OFF metadata-only build removed the sparse file markers and
retired mask history after expiry.
Fixture retries passed within their original budgets for rewrite_append.

The qualified catalog driver SHA-256 is
`ca9d6481903ed99b9460130822cffbc0a97151d0ba802c32a27e1ef674d0f82b`.
