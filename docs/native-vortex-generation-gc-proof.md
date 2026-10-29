# Registered native Vortex generation collection

This validation draft follows [DuckLake #42](https://github.com/AstroVela/ducklake/pull/42).
It adds explicit collection of published generations retired by replacement or
selective merging. It reuses the existing static SPFresh generations and private
query leases. Source data and unpublished candidates are outside collection.

## Ownership and atomic retirement

Explicit index initialization now also creates
`ducklake_vortex_generations_v1` and `ducklake_vortex_generation_refs_v1` in the
native metadata catalog. The first records each published generation's canonical
root, sealed reference, authenticated manifest, artifact bytes and lifecycle
state. The second records references for each table and named index, including
generations shared by multiple index names. Both tables are required; the prototype
requires fresh explicit initialization and adds no migration or old decoder.

Publication verifies and registers the candidate, replaces the named index's
reference set, and retires removed generations that have no remaining references.
These changes commit in the same transaction as the index revision CAS. Failed
or uncommitted candidates never become GC-owned. A still-intact retired
generation can be explicitly republished and reactivated under the publication
lock. Collection and publication serialize, so collecting generations cannot
be reactivated while their files are removed.

The qualification uses a single local native DuckDB metadata catalog with an
exclusively managed artifact namespace. Cross-process catalog ownership and
roots shared with independent metadata catalogs are outside this proof.

## Bounded explicit collection

```sql
SELECT index_root,generation,state,artifact_bytes,reference_count
FROM ducklake_vortex_generation_status('lake');

SELECT * FROM ducklake_collect_vortex_generations('lake',2,67108864);
```

The last two arguments bound generations and registered artifact bytes per
call: 1–16 generations and 1–64 MiB. Eligible generations are ordered by root and
generation. Processing stops before the first generation that exceeds the
remaining budget; it does not skip ahead or expand the budget. The existing
manifest limit is 1 MiB per generation. Only `retired`/`deleting` records with
no current named-index references can enter the collection plan.

The returned columns are `collected_generations`, `collected_artifact_bytes`
and `remaining_generations`. Bytes describe the registered artifact inventory
of completed generations, including already removed files on explicit resume;
they are not a measurement of that call's physical disk delta. Source/vector
budgets, the three-B limit and write quotas remain unchanged.

The collector authenticates the cached manifest, checks the registered tree and
remaining file checksums, then durably marks that generation `deleting` before
the first unlink. POSIX directory descriptors and relative `openat`/`unlinkat`
operations reject symlink traversal. It deletes only registered files and empty
registered directories, never recursively clears a root, and syncs directory
changes. Unexpected children, changed bytes and links fail explicitly.

After deleting files, the collector removes the completed registry row. A crash
or cancellation leaves a durable `deleting` record; explicit resumption permits
missing registered files/directories in that state. A `retired` record with
unexplained missing files still fails. Each generation commits independently,
so earlier completed work stays completed if a later generation fails. Repeating
collection after completion is a no-op. There is no automatic retry or fallback.

## Protecting admission and existing views

A native indexed read registers a shared collection guard before beginning its
metadata snapshot. It holds the guard through source capture and all index
lease materialization, releasing it before the `after_open` observer once the
private copies and native handles exist. Errors and cancellation release the
guard through ownership cleanup. Selected native builds retain their guard
through publication. The collector takes the exclusive guard before the
publication lock and its first metadata snapshot. Publication can proceed while
an old reader finishes admission; collection waits for the old generation copies.

Fully opened views continue searching and fetching from their private sources
and artifacts after canonical retired generations disappear. Diagnostic JSON
capture output is an admission description, not a persistent generation lease;
an external consumer must open its own verified private view before collection.
The existing same-process ownership and strict database shutdown checks remain.

## Qualification

```sh
python3 -I test/vane/test_native_vortex_generation_gc.py \
    --duckdb build/index-shell/duckdb \
    --driver build/index-shell/ducklake_vortex_catalog_driver \
    --probe build/index-cargo/ci/ducklake-vortex-index-probe \
    --output-dir build/index-logs/generation-gc
```

The proof runs four append/merge/GC rounds, preserving unselected B1, current
winners, source bytes, unpublished candidates and unregistered directories.
It checks count/byte budgets, wrong contents and symlinks, partial cancellation,
failed completion, delayed old-view B1/B2/B3/F/T fetch, and GC competing with
query admission, a native build and publication. Query races pause before
snapshot capture, before opening, after source pinning, after the first segment
open and after all leases are ready; cancellation must unblock collection.
SIGKILL covers both sides of publication commit and five GC checkpoints,
including partial deletion and files removed before metadata completion.
SQL cases verify shared named-index references and explicit configuration.

This adds no background sweeper, orphan-directory scan, abandoned-candidate
cleanup, source-file rewriting or SPFresh online updates. Failed candidates
remain outside ownership until a later explicitly designed registration path.

Local qualification kept DuckDB v1.5.5 (`d8cdaa33`), Vortex `65dab44f`, the native
wrapper `b819cc03` and the qualified static SPFresh SDK unchanged from #42.
The final catalog driver SHA-256 was
`6ed37b3ce73a2d452726483f9638b4cec5df3e02ef8900e71da7fbb251c990cc`.
The process proof passed 20 checks and 104 fixture queries with recall 1.0,
including four append/merge/GC rounds. ON passed 14 Vortex SQL cases (534
assertions) and Vortex recovery. ON/OFF each passed 86 base cases (3,582
assertions; 13 optional-dependency skips) and Parquet recovery; audit passed 122
assertions. All existing native index, read-view, mixed, admission, catalog,
search, build, optimizer, lifecycle, write-budget, segment and merge proofs
passed. The isolated cost proof passed four cases with nine queries each and
recall 1.0; timings remain informational. `make format-fix`, Rust formatting
and workspace Clippy with all targets/features and warnings denied passed.
