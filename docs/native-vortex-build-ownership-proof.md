# Registered native Vortex build ownership

This validation draft follows [DuckLake #43](https://github.com/AstroVela/ducklake/pull/43).
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
lock. They cannot abandon or delete a live native builder's workspace.

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

Local qualification kept DuckDB v1.5.5 (`d8cdaa33`), Vortex `65dab44f`, the native
wrapper `b819cc03` and the qualified static SPFresh SDK unchanged from #43.
The final catalog driver SHA-256 was
`d8a9d9cce462cafc36699c28c52b910a2671af86f37ed18ee7d5cd9feed43c33`.
The new process proof passed 14 checks and 14 fixture queries with recall 1.0.
ON passed 15 Vortex SQL cases (566 assertions) and Vortex recovery. ON/OFF each
passed 86 base cases (3,582 assertions; 13 optional-dependency skips) and Parquet
recovery. Audit passed 122 assertions. All 13 existing native process proofs
passed. The isolated cost proof passed four cases with nine queries each and
recall 1.0; timings remain informational. `make format-fix`, Rust formatting,
and workspace Clippy with all targets/features and warnings denied passed.
