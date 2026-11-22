# Indexed Vortex source rewrite and atomic handoff

The rewrite operation extends the maintenance work in [DuckLake #45](https://github.com/AstroVela/ducklake/pull/45).
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

The operation requires autocommit on a writable DuckLake attachment with native
DuckDB metadata, the native four-column Vortex table, an installed write
budget selecting this index, and exactly one named index for the table. The
caller-bound DuckLake and metadata attachments are checked in the dedicated
transactions used for registration, publication and recovery. It
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
the registered build collector. Reference names are checked with DuckDB's
extension-filename write rule before registration.

## Private construction and one metadata commit

Fresh explicit index initialization creates `ducklake_vortex_rewrites_v1` with
the other required index tables. It records a build ID, table UUID, planned
managed source root/path, capture snapshot, new file ID and retained-file flag.
Registration commits with the existing build ownership record before any output.

Registration uses the publication lock and existing namespace checks before
creating the workspace. The compactor filters candidates to the selected B and
scans private copies of the selected files. Each copy is checked against the
sealed descriptor's URI, physical row count and content hash. The compactor
writes the output inside the workspace and preserves logical row IDs while
removing deleted versions. Staging reserves one entry each for the workspace
and output within the existing 4,096-entry collection limit. After compaction
the operation removes the staged inputs and syncs the workspace directory
before the native builder creates scratch and generation artifacts. SPFresh
receives owned buffers and binds its row map to the planned stable file ID. It uses the normal registered
generation, scratch, sealing, verification and reference-writing path.

After construction, the operation fsyncs the private output and its directory,
then exclusively links the output into the registered managed path. It keeps
the private link as durable ownership evidence for recovery. The final commit
takes the exclusive publication guard during write admission,
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

The registered build collector rechecks index/build ownership and native file
metadata, including historical file rows. A managed link can be removed after
abandonment if it is unreferenced and matches the live private file by device
and inode. Directory access is checked before opening and removing the managed
link. The collector holds source admission across the link-count check and
private unlink, serializing with the native old-file collector. It removes
an abandoned managed link first and charges bytes when it releases the last
link in the workspace. The shared budget and bounded
truncation rules also apply to staged inputs. Completion removes both registry
rows together.

Rewrite sources use the reserved `ducklake-vortex-rewrite-` file prefix.
Generic orphan cleanup excludes this namespace during candidate binding, so a
prepared cleanup can span publication safely. The build collector handles
abandoned rewrite output and the existing snapshot-based old-file cleanup
handles published sources.

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
reference staging, final source link, metadata staging and the final commit
boundary. A failure before COMMIT reports an abort. Regressions observe
cancellation during collection, write-admission and publication lock waits,
and order private-witness collection against native old-file cleanup. SIGKILL
covers registered allocation, private copy, sealing, linked source, staged atomic
metadata and committed publication. Recovery remains explicit through catalog
maintenance. Concurrent writes cause inspectable conflicts; reader admission and
prepared orphan cleanup isolation exercise complete old/new views and source
ownership. Minimal SQL also checks stable logical row IDs, retained historical
results, unchanged unselected B and an F quota smaller than the replacement B.

Qualification remains one local metadata owner on POSIX with exclusively managed
artifact/source namespaces. It adds no automatic data-compaction policy, native
SPFresh online update calls, dependency-pin changes or total-capacity increase.

### Local qualification (2026-11-18)

Linux used DuckDB v1.5.5 (`d8cdaa33`), Vortex `b20e34c2`, the native
wrapper `84cef204` and the static SPFresh SDK qualified by #45. The rewrite
proof at `5a56aeb8` passed 21 checks in
62.2 seconds within the 180-second budget. All 88
gated fixture queries attained recall 1.0.
Five rewrites advanced revision 3 to 8 and reduced current physical rows
from 4,352 to 383 while preserving both unselected B generations.

At the 100,000-row ceiling, rewriting the selected 4,096-row B to 128 rows
released 3,968 row slots. The 95,904-row F stayed intact and the next inline
write succeeded. The separate deletion-dense sample measured recall
0.2 before rewrite and passed the post-rewrite gate.

All 16 native process suites and 19 functional gates passed.
Native ON passed 26 Vortex SQL cases with 1,028 assertions.
The rewrite/orphan SQL groups passed 17 cases with
881 assertions and 2 optional skips.
ON and OFF each passed 86 base cases with 3,582 assertions and 13 optional
skips. All 32 ABI checks and four isolated query-cost cases passed.
Rust formatting, Clippy, workspace tests, changed-file formatting and diff
checks passed. The catalog driver SHA-256 was
`6d1bdafbb4166c1f1a8aaf7545ce9089f90a5da7524b9bc09f11e20c78ef3aa3`.

The first run of this matrix reached the existing 60-second call limit during
the initial B1 build in the generation GC suite's source pin cancellation fixture.
The native workspace existed and the new generation's artifact directory was
empty. The first isolated run reached the same limit while
constructing a merge candidate in a later fixture. The cause is undetermined. The same binary passed the isolated suite
in 71.0 seconds and the complete matrix replay
passed it in 70.8 seconds under the same limits.
