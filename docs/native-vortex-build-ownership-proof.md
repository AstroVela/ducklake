# Registered native Vortex build ownership

This proof extends [DuckLake #43](https://github.com/AstroVela/ducklake/pull/43).
It registers native build work before writing artifacts, reconciles stopped
tasks explicitly, and collects their owned workspaces and abandoned candidates.
It reuses the pinned Vortex index provider and static SPFresh SDK.

## Registration and publication

Fresh explicit index initialization creates `ducklake_vortex_builds_v1` alongside
the index, generation and named-reference tables. All four tables are required;
this prototype adds no migration or compatibility decoder.

Before source capture or filesystem construction, a native build commits a UUID,
table/index identity, expected revision, canonical artifact root, reference path,
planned `generation-<UUID>` and dedicated `.build-<UUID>` workspace. The workspace
is created exclusively after registration. SDK scratch and reference temporary
files stay inside it; admitted source files are copied into memory. Sealed
generations retain the existing layout under the canonical artifact root.

Registration takes the publication lock before opening its metadata snapshot.
It checks the planned workspace, generation directory and caller reference
against paths claimed by other builds and registered generations. Two builds
can share an artifact root or use sibling directories. A claim that coincides
with, contains or lies inside another owner's path is rejected. The same check
protects published generations after their workspace registration is collected.

Build state advances through `allocated`, `building` and `sealed`. Publication
sets `published` and `keep_generation=true` in the same transaction as the
generation registration, named references and index revision CAS. A synchronous
failure or cancellation rolls back pending work and reconciles the task with the
committed catalog, marking only unpublished candidates `abandoned`. A publication
that commits but loses its acknowledgement keeps its generation. A still-intact
candidate may also be explicitly published before collection claims it.

The whole native build holds the shared collection guard through publication
and error reconciliation, including bootstrap and full replacement builds.
Recovery and both collectors take the exclusive guard before the publication
lock. A caller can cancel while waiting for either lock. A stopped waiter
releases its locks and connection while the other operation still holds its lock.

## Explicit recovery and bounded collection

```sql
SELECT * FROM ducklake_vortex_build_status('lake');
SELECT * FROM ducklake_recover_vortex_builds('lake',16);
SELECT * FROM ducklake_collect_vortex_builds('lake',16,67108864);
```

Recovery examines at most 16 pending tasks per call. Under exclusive ownership
it reconciles `allocated`/`building`/`sealed` tasks against committed generation
and named-reference records. A stopped unpublished task becomes `abandoned`;
committed output is retained. Restart alone does not infer abandonment, replay
construction or delete files. Uncertain metadata errors leave ownership pending
and require inspection and explicit recovery.

Collection examines at most 16 registered tasks and reclaims at most 64 MiB of
logical file bytes per call. It rechecks committed generation ownership before
claiming each task, then durably records `deleting` and whether its generation
must be kept. A published task loses only its workspace; an abandoned task can
lose both its workspace and planned generation. Published-generation retirement
remains the responsibility of `ducklake_collect_vortex_generations`.

The collector rechecks each selected directory against other owners and the
resolved DuckLake data directory before inspecting the filesystem. It uses the
caller's DuckDB filesystem settings before inspection and at each filesystem
mutation. With external access disabled, a directory grant on a published
workspace suffices to collect it. Revoking access after `deleting` preserves
the checkpoint for an authorized call to resume.

The returned columns are `collected_builds`, `collected_bytes` and
`remaining_builds`. The last count includes pending tasks that need explicit
recovery. Task order is UUID order. Directory traversal is bounded to 4,096
entries per task, rejects symlinks and unexpected file types before deletion,
and uses POSIX directory descriptors with relative operations and fsync.
Only the exact registered workspace and eligible planned generation are walked;
unknown sibling directories are untouched. Caller reference JSON is preserved.

An unfinished SDK file may exceed the 64 MiB collection limit. Once deletion
ownership is durable, a singly linked owned file can have its tail truncated
within the remaining byte budget. A later explicit call continues cleanup;
the global budget does not grow. Multiply linked files are only unlinked, never
truncated, and contribute zero reclaimed bytes while another link exists.
Byte counts describe logical file lengths reclaimed by this call, not physical
disk blocks. Empty directories and metadata completion do not consume bytes.

Completion removes the ownership row after all selected directories are gone.
Cancellation, failure or a crash leaves resumable `deleting` state. Missing
owned files on resumption are harmless, and repeated completed collection is a
no-op. There is no background sweeper, automatic task retry, orphan scan or
fallback. An unregistered external probe build remains externally owned.

## Qualification

```sh
python3 -I test/vane/test_native_vortex_build_gc.py \
    --duckdb build/index-shell/duckdb \
    --driver build/index-shell/ducklake_vortex_catalog_driver \
    --probe build/index-cargo/ci/ducklake-vortex-index-probe \
    --output-dir build/index-logs/build-gc
```

The process proof injects failure and cancellation at eleven build boundaries and
SIGKILL at eleven allocation/build/seal/publication boundaries, including both
sides of commit and a staged reference JSON file. It checks explicit recovery,
lost acknowledgement, repeated cleanup, count limits, a 65 MiB partial scratch file, hardlinked sources,
symlink rejection and unknown directories. Failure, cancellation and SIGKILL
cover six collection boundaries, including partial truncation and metadata
completion. Concurrent recovery/collection must wait for live builders;
publication must serialize with candidate deletion. Current queries and an
already opened old view continue searching and fetching; original B1 identities,
artifact bytes/mtimes and source hashes remain unchanged.

The qualification assumes one local native DuckDB metadata owner and an
exclusively managed artifact namespace on POSIX. Independent processes/catalogs
sharing roots and external filesystem writers are outside this proof. Existing
global source, row, vector, manifest, artifact, three-B and write-admission
budgets are unchanged. It adds no SPFresh native online Add/Delete calls.

The synchronized dependency pins are DuckDB v1.5.5 (`d8cdaa33`), Vortex
`b20e34c2`, the native wrapper `84cef204` and the same static SPFresh SDK
used by #43. The process proof covers overlapping claims at admission and
collection, disjoint sibling directories, directory-scoped grants, permission
revocation during cleanup and cancellation at both maintenance locks. The
generation-GC proof also exercises workspace collection with absolute, relative
and symlinked DuckLake data paths.
