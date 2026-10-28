# Native DuckLake index publication and discovery

This validation draft extends the [native read-view admission proof](native-vortex-admission-proof.md)
in [DuckLake #31](https://github.com/AstroVela/ducklake/pull/31). A caller publishes
an explicitly built, sealed SPFresh generation under an index name. A native
read view discovers that generation and its source snapshot in the same
metadata transaction. Queries no longer need a caller-supplied reference path.

The implementation reuses Vortex's existing `feat/vortex-index` implementation
at `ee085f55a705cb9b2edab18d392dd822540774d7`, including its static SPFresh builder,
sealed manifests, artifact verification and private consumer leases. B/F/T
planning and visibility remain as qualified by the [mixed-query proof](native-vortex-mixed-proof.md).

## Publication contract

The experimental ON build exposes three functions:

```sql
-- Explicit initialization; a second initialization fails.
SELECT * FROM ducklake_initialize_vortex_indexes('lake');

-- Build and qualify the generation first, outside DuckLake's managed DATA_PATH.
-- Revision 0 means the name must be absent. Replacements supply its ready revision.
SELECT revision, generation
FROM ducklake_publish_vortex_index(
    'lake', 'main', 'items', 'knn', '/absolute/index-root/reference.json', 0);

-- The catalog record supplies dimension, artifact root and sealed reference.
SELECT snapshot_id, source_bytes, visible_rows, inline_rows, admission, rows
FROM ducklake_vortex_index_view('lake', 'main', 'items', 'knn', 67108864, 100000);
```

`ducklake_vortex_indexes_v1` has one ready row per `(table_uuid, index_name)`.
The row stores a positive revision, canonical artifact root and complete sealed
reference. Building candidates never enter this table. Initialization is an
explicit operation, with no metadata migration, automatic setup or alternate
catalog format. A dropped and recreated table has a different UUID and cannot
reuse the earlier table's named index.

Publication owns an idle, dedicated connection and transaction. It validates
the table UUID, vector field ID/type, dimension, squared-L2 metric, physical
snapshot fingerprint, complete sealed coverage, manifest and artifact sizes
and checksums. Every referenced B file must still exist in the current source
inventory with the same path, row count and content digest. Appended F files and
committed T rows do not require rebuilding B. Candidate reference and artifact
paths must be canonical local paths without symlinks, outside the managed data
path so source orphan cleanup cannot delete the index.

The publisher takes an exclusive publication guard before choosing its source
snapshot and holds it through metadata commit. Native source commits take the
shared guard, closing the validation-to-publication race with rewrite, table
changes and other source commits. A shared source-admission guard also protects
source verification against physical cleanup. Concurrent publications with the
same expected revision produce one ready successor and one explicit conflict.
The guards cover the loaded extension instance, including all its catalogs;
this proof does not establish publication between independent processes.

One metadata commit inserts or replaces the ready row. Publication changes the
derived index revision without creating a new DuckLake source snapshot. A reader
whose metadata transaction started earlier sees the earlier ready row even when
the source snapshot ID is unchanged. A fresh reader sees the committed successor.

## Discovery and ownership

`DuckLakeVortexReadView::Acquire` resolves the requested index name through the
same native metadata transaction that selects the source snapshot, fields,
inventory and visibility. It verifies B coverage while copying source bytes,
then embeds the name, revision, artifact root and sealed reference in the
native admission envelope. The original `reference.json` is unnecessary after
publication and can be removed.

The test-only Rust worker explicitly accepts `open_catalogued` with a capture
path and index name. It checks the named record, materializes the admitted bytes,
revalidates the physical binding and opens the existing SPFresh generation into
a private lease. It returns the admitted revision and generation for diagnostics.
The existing explicit command modes remain distinct; none probes other formats
or retries a failed open through another path.

A source rewrite that removes B makes a fresh named acquisition fail. An
explicit rebuild and publication are required for the new physical layout.
Artifact corruption likewise fails consumer admission. Neither condition moves
B to scan, changes the snapshot or triggers automatic reindexing. Opened views
retain their original encoded source buffers, T rows and leased index artifacts,
so later publication, source GC, canonical index deletion and native-owner exit
do not change their search or business-row fetch.

Publication never deletes generations. A pre-commit failure retains the old
ready row and sealed candidate; a successful commit replaces the row atomically.
If commit acknowledgement is lost or its outcome is uncertain, the caller must
inspect the catalog from a fresh connection before retrying. Candidates and old
generations remain available. Automatic index GC and background indexing are
outside this validation step.

## Validation and reproduction

Build the experimental native shell, SQL runner and catalog driver:

```sh
cmake --build build/vortex-validation --target duckdb unittest ducklake_vortex_catalog_driver
```

Build the Rust probe with the [index-proof instructions](native-vortex-index-proof.md#build-and-run),
then run:

```sh
python3 -I test/vane/test_native_vortex_catalog.py \
  --duckdb build/vortex-validation/duckdb \
  --driver build/vortex-validation/ducklake_vortex_catalog_driver \
  --probe build/index-cargo/ci/ducklake-vortex-index-probe \
  --output-dir build/catalog-proof
```

The native driver starts multiple connections against persistent DuckDB
metadata. It pauses a reader after snapshot selection while another connection
publishes, races two publishers, and pauses publication after validation while
a source commit waits. The process harness sends SIGKILL immediately before and
after metadata commit and restarts the native owner against the same catalog.
Recovery must expose the previous and successor ready rows respectively, with
sealed candidates still present. A separate injected lost acknowledgement
checks that the committed row survives the reported failure.

The fixture starts with two B files and 4,096 vectors, then appends F and commits
updates, deletes and T rows. It checks explicit initialization, invalid binding
and corrupt-artifact rejection, stale revisions, retained old discovery,
reference-file removal, compaction rejection, explicit rebuild, table UUID
isolation, real source cleanup and unseen ranked B/F/T fetch after owner exit.

The local catalog proof passed seven explicit builds, seven ready revisions,
two SIGKILL boundaries, 41 native requests with 14 expected rejections, and
35 consumer requests with three expected rejections. Eight fixture queries
passed with recall 1.0 against the same-view oracle; the threshold is 0.8.
Actual cleanup reclaimed three source files, and closing all consumer views
returned reservations to zero. These fixture results do not establish workload
performance or general ANN recall.

The ON native gate passed six Vortex SQL cases with 323 assertions, including
the new 15-assertion catalog case, plus Vortex/Parquet crash recovery. Both ON
and default-OFF base gates passed the same 86 SQL cases with 3,582 assertions and
13 optional-extension skips. The OFF gate also checks that all four native
view/catalog functions are absent. Existing snapshot-index, retained-view,
mixed-query and native-admission process proofs passed against the same shell
and Rust probe. Rust Clippy passed with warnings denied.

This remains a bounded validation interface: Linux/POSIX local unencrypted
Vortex files, native DuckDB metadata, the existing four-column business
projection, and the source/row/artifact budgets documented in the
[admission proof](native-vortex-admission-proof.md#consumer-ownership-and-limits).
Publication limits each reference/manifest to 1 MiB and its B source/artifact
inventories to 64 MiB each. Local builds reused qualified DuckDB v1.5.5 engine
objects and pinned dependencies; ON/default-OFF CMake configuration was checked
separately. The Draft PR workflow performs clean hosted builds and runs all
five process proofs. The next [native SQL search proof](native-vortex-sql-search-proof.md)
connects named discovery to an experimental in-process search table function.
