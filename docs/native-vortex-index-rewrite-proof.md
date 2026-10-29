# Indexed Vortex source rewrite and atomic handoff

This validation draft follows [DuckLake #45](https://github.com/AstroVela/ducklake/pull/45).
It rewrites deleted rows out of one explicitly selected B segment, builds its
replacement ANN generation privately, and commits file replacement and index
publication in the same native DuckDB metadata transaction. Unselected B
descriptors and generations remain unchanged.

## Explicit operation and boundaries

```sql
SELECT * FROM ducklake_rewrite_vortex_index_segment(
    'lake','main','items','knn','generation-selected',
    '/absolute/existing/index-root/replacement.json',3);
```

The operation requires the native four-column Vortex table, an installed write
budget selecting this index, and exactly one named index for the table. It
supports one nonempty compaction output and one schema/partition group under
the existing unpartitioned, unencrypted native Vortex writer. Its selected
segment must have deletions eligible for DuckLake's existing rewrite operator.
Unchanged selected files can remain in the replacement generation. Other B,
unindexed F and committed T are not rewritten into that generation.

The captured DuckLake snapshot fixes the planned new file ID and commit snapshot.
The first prototype uses a catalog snapshot/revision CAS: any intervening native
data commit, schema change, index publication or alias installation can cause an
explicit conflict. Other connections can read and commit while construction
runs; a losing rewrite rolls back without replay. There is no automatic rewrite
retry, source-to-F downgrade, compatibility decoder or fallback.

The prospective replacement must still fit the existing static SPFresh options,
including at least 65 physical rows. Index/source/vector budgets and write quotas
do not expand. The data directory and private workspace must share a filesystem;
the final source link is exclusive and does not overwrite an existing file.
An existing destination causes an explicit error and remains protected from
the registered build collector.

## Private construction and one metadata commit

Fresh explicit index initialization creates `ducklake_vortex_rewrites_v1` with
the other required index tables. It records a build ID, table UUID, planned
managed source root/path, capture snapshot, new file ID and retained-file flag.
Registration commits with the existing build ownership record before any output.

DuckLake's existing compactor filters candidates to the selected B and writes
the private Vortex file inside that registered build workspace. It preserves
logical row IDs while physically removing deleted versions. The existing native
SPFresh builder receives owned buffers for the prospective file inventory and
binds its row map to the planned stable file ID. It uses the normal registered
generation, scratch, sealing, verification and reference-writing path.

After construction, the operation fsyncs and exclusively links the output into
the registered managed path. The final commit takes the publication guard,
rechecks the capture and index against fresh metadata, validates the projected
write budget, and uses DuckLake's normal compaction commit. Before that metadata
connection commits, the index hook checks actual output IDs, coverage and hashes,
verifies sealed generations, replaces named references, retires removed index
generations and retains both the build generation and managed file. These
changes commit atomically with the native snapshot/file replacement.

The commit hook resolves the table through the existing transaction/snapshot
lookup. It does not issue a new session table lookup during outer transaction
commit. A lost acknowledgement after commit keeps both outputs and requires
inspection; it never turns committed data into an abandoned candidate.

## Collection and readers

The existing build collector owns an unpublished prospective managed file only
through its matching rewrite registration. Before deletion it rechecks retained
index/build ownership and native file metadata, including historical file rows.
Published or otherwise registered source paths are retained. Only an explicitly
abandoned, unreferenced planned path can be removed, using POSIX descriptors and
the same shared byte budget and bounded truncation rules. Completion removes
rewrite and build ownership rows together. Published data remains under normal
DuckLake snapshot retention and source-file cleanup.

Unpublished Vortex outputs use registered ownership rather than directory scans.
The existing generic orphan cleanup targets Parquet files and does not own these
prospective Vortex paths. A prepared cleanup during the linked-output window must
leave them untouched. Normal old-file cleanup retains its existing admission
guard and native snapshot-retention rules.

Active captures retain the collection/admission guards until source/index leases
are private. Already opened views can search and fetch after replacement, ANN GC,
snapshot expiry and canonical source-file GC. Retained historical SQL snapshots
continue using their original files until explicit expiry permits collection.

## Qualification

```sh
python3 -I test/vane/test_native_vortex_index_rewrite.py \
    --duckdb build/index-shell/duckdb \
    --driver build/index-shell/ducklake_vortex_catalog_driver \
    --probe build/index-cargo/ci/ducklake-vortex-index-probe \
    --output-dir build/index-logs/index-rewrite
```

The proof checks five selected rewrites, unchanged unselected generation bytes
and modification times, source capacity release, current B/F/T results and old
view search/delayed typed fetch. A separate case reaches the unchanged 100,000-row
ceiling using a 4,096-row B and 95,904-row F, verifies deletion alone does not
admit growth, then rewrites B to 128 rows while retaining F and successfully
appends again. A separate deletion-dense sample reports static ANN recall before
and after rewrite; search budgets and the existing post-rewrite recall gate stay
unchanged.

Failure and cancellation cover allocation, private copy, native build/seal,
reference staging, final source link and both sides of metadata staging. SIGKILL
covers registered allocation, private copy, sealing, linked source, staged atomic
metadata and committed publication. Recovery remains explicit through catalog
maintenance. Concurrent writes cause inspectable conflicts; reader admission and
prepared orphan cleanup isolation exercise complete old/new views and source
ownership. Minimal SQL also checks stable logical row IDs, retained historical
results, unchanged unselected B and an F quota smaller than the replacement B.

Qualification remains one local metadata owner on POSIX with exclusively managed
artifact/source namespaces. It adds no automatic data-compaction policy, native
SPFresh online update calls, dependency-pin changes or total-capacity increase.

### Local result (2026-10-29)

The new process proof passed 18 checks and 88 gated query comparisons with
minimum fixture recall 1.0. Five rewrites advanced revision 3 to 8 and reduced
current physical source rows from 4,352 to 383 while preserving both unselected
B generations. At the unchanged 100,000-row ceiling, rewriting the selected
4,096-row B to 128 rows released 3,968 row slots; the 95,904-row F stayed intact
and the next inline write succeeded. The separate deletion-dense sample measured
recall 0.2 before rewrite and 1.0 afterward; the pre-rewrite observation remains
visible and is not substituted for the post-rewrite gate.

- All 15 existing native process proofs passed.
- ON passed 17 Vortex SQL cases / 627 assertions, including the new 32-assertion
  SQL case. ON and OFF each passed 86 base cases / 3,582 assertions with 13
  optional-extension skips, plus Parquet recovery; ON also passed Vortex recovery.
- Native audit passed 122 assertions. Rewrite/orphan regression groups passed
  16 cases / 854 assertions with two ICU-dependent skips.
- Four isolated cost cases passed nine queries each with minimum recall 1.0;
  their timings remain informational. Formatting, Rust formatting and diff checks
  passed; this change modifies neither Rust sources nor dependency pins.

The qualified catalog driver SHA-256 is
`65489efd7cf9e9209242e02451b1428978d3db3864caaacf5ddc911ab3ccdbe4`.
