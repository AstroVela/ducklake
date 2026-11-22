# Registered native Vortex generation collection

This proof follows [DuckLake #42](https://github.com/AstroVela/ducklake/pull/42).
It adds explicit collection of published generations retired by replacement or
selective merging. It reuses the existing static SPFresh generations and private
query leases. Source data and unpublished candidates are outside this generation
collector. Registered native build leftovers have a separate
[ownership and collection path](native-vortex-build-ownership-proof.md).

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
or uncommitted candidates never enter this generation registry. A still-intact retired
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

The managed DATA_PATH is expanded and resolved with the existing publication
resolver, so supported relative and symlink data directories remain valid
exclusion boundaries. Canonical and no-follow rules apply to index artifacts;
resolving the data directory does not relax those artifact checks.

The collector authenticates the cached manifest, checks the registered tree and
remaining file checksums, then durably marks that generation `deleting` before
the first unlink. POSIX directory descriptors and relative `openat`/`unlinkat`
operations reject symlink traversal. It deletes only registered files and empty
registered directories, never recursively clears a root, and syncs directory
changes. Unexpected children, changed bytes and links fail explicitly.

The collector uses the caller-context filesystem for directory access checks
and requires directory-wide authorization before native descendant access.
Disabled LocalFileSystem and external-access settings remain effective. Checks
repeat at collection checkpoints and before each unlink, including when access
is revoked after validation or after partial deletion. Permission rejection
preserves the active head and the last durable retirement/deleting checkpoint.
Authorized explicit resumption still handles missing registered directories.

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
guard through ownership cleanup. All native builds retain a shared guard through
publication and error reconciliation. The collector takes the exclusive guard before the
publication lock and its first metadata snapshot. Waiting queries, builds, and collectors
check caller cancellation before entering the next transaction. Collectors also check
cancellation while waiting for the publication lock. Publication can proceed while
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

This generation collector adds no background sweeper, orphan-directory scan,
source-file rewriting or SPFresh online updates. Abandoned native candidates
are handled by the separate registered build collector.

Local Linux qualification on 2026-11-17 used functional source `8b6be9bb`
with reviewed #42 `eb90fc7d` merged in. DuckDB v1.5.5 (`d8cdaa33`), Vortex
`b20e34c2`, codec `84cef204`, and SPFresh `5893eb61` with Vortex's pinned
static-only patch were preserved. The final catalog driver SHA-256 was
`aad678cb9f05dbce539e909200675452fcb9e61f1e63b9fbe3ccde2f279e0ccb`.

The final GC process proof passed 37 checks and 148 fixture recall checks
at 1.0 in 68.5 seconds under the existing 180-second outer budget. It includes
ten external-access/disabled-filesystem rejection and revocation cases, two
directory-allowlisted collection/resumption controls, collector cancellation
while admission/publication locks remain held, and valid search/append/merge
capture cancellation while GC stays paused with its guard. Dedicated readers
are released before GC unlock; head, artifact signatures and reopen stay valid.
Canonical, relative and symlink DATA_PATH each build/publish twice, retire
and actually collect the old generation, query before/after GC and preserve
128 source rows and the current generation/reference. These add six exact SQL
query-result checks to the fixture recall checks above.

The original permission bypass and uninterruptible collector waits reproduced
against unchanged pre-fix native archives. A first independent Astra max review
of `b135dedd` reproduced reverse shared-admission cancellation blocking. The
pinned engine has no cancellable shared StorageLock acquisition; collection
therefore keeps its own reader count and mutex with the same exclusion and
writer preference. It checks reader/caller interruption and preserves
Collection-to-Publication order and lease lifetime. A second independent review
of `fc4bcf4e` reproduced GC rejecting valid relative/symlink DATA_PATH values.
The collector now uses the existing publication resolver for that managed-data
boundary. Artifact canonical/no-follow checks remain in place. All red/control
results and successful regressions are retained.

ON passed 23 Vortex SQL cases / 907 assertions, including shared named-index
references surviving actual GC until their last owner is replaced. ON and OFF
each passed 86 base cases / 3,582 assertions with 13 optional-dependency skips.
Deletion inlining passed 18 cases / 1,099 assertions with two optional-dependency
skips; audit passed 122 assertions. Search/catalog/admission, retained and mixed
views, Vortex/Parquet recovery, cleanup, native build, optimizer, lifecycle,
all 13 write-budget cases, independent segments and selective merge proofs
passed. The 32-case ABI matrix passed. Query/probe Rust formatting, workspace
Clippy with all targets/features and warnings denied, workspace tests, and
scoped pinned C++/Python formatting passed. Existing Float32 tolerances
(`rel=1e-5`, `abs=1e-4`) and recall threshold 0.8 were retained.

Seventeen ordinary functional gates kept 180-second budgets. The separate
counter/recall gate retained its existing 600-second outer limit and completed
in 11.7 seconds with four cases / 36 queries at recall 1.0. Native RPC and
cleanup limits remain 60 and 30 seconds. No engine pin, thread setting,
transaction protection or timeout changed. Source/header/object/link/SDK
identities qualify reused inputs; changed C++ was compiled and products
relinked normally. These checks make no stable-load performance claim.
CI was deferred. The earlier #42 large nonadjacent merge timeout remains
separately tracked in [its qualification](native-vortex-segment-merge-proof.md);
this GC change does not establish that anomaly's cause or a fix.
