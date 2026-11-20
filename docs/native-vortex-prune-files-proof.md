# Atomic pruning of empty files within B

This validation follows [DuckLake #50](https://github.com/AstroVela/ducklake/pull/50).
A B with at least 65 surviving rows can retire fully masked member files
without copying its healthy Vortex files. The selected B gets a new static
SPFresh generation over the retained file identities. Unselected generations
remain unchanged.

## Operation and planning

```sql
SELECT * FROM ducklake_prune_vortex_index_segment(
    'lake','main','items','knn','generation-<uuid>',
    '/absolute/index-root/new-reference.json',<expected_revision>);
```

The operation binds the writable autocommit caller and its DuckLake and
metadata attachments. It requires one named index and its installed write
budget. The union of inline and external delete positions is computed by
file ID at the native snapshot. At least one member must be empty and each
retained member fully visible, with at least 65 retained rows altogether. Partially deleted retained
members require the existing indexed data rewrite. An entirely empty B uses
retirement; a 1–64-row remainder uses the explicit bounded F rewrite.

The opt-in compaction policy keeps its existing thresholds and candidate
priority. A larger B whose deletions affect only whole files now selects the
durable task kind `prune`; previously it deferred as `empty_rewrite_output`.
Other supported larger candidates still select `rewrite`. A failed native
build stops the task. No alternate mode is selected after failure.

## Atomic handoff and ownership

The operation reuses registered build/workspace ownership and the static
vortex-index/SPFresh builder. Its input contains only retained native file
IDs, URIs, SHA-256 identities and row counts. The sealed descriptor must
match the registered generation and every retained file identity. The commit
uses that captured descriptor and the optimizer records the same registered
generation. The prune creates only index artifacts and a new reference.
Healthy source bytes, mtimes, native
row positions and begin snapshots are preserved.

The source transaction stages native drops for the empty members. Under the
existing write reservation and exclusive publication lock, final validation
checks the exact index descriptor, schema, all selected data/delete identities
and native visibility. Independent committed T/F appends are accepted by
rebasing to the fresh native snapshot. No new source file ID needs binding.
The metadata transaction retires the empty sources, marks their physical
retirement, ends their active external masks and publishes the selected B.
Coverage and artifact limits are verified before the commit. Change feeds
retain the original logical deletes and return zero events at the prune
snapshot. Snapshot expiry reclaims the retired source markers and masks.

The task's run UUID is also the manifest publication identity. The existing
optimizer receipt records the run, candidate generation and published revision
in the same metadata transaction as retirement and index publication.
Resume can recover that receipt after a later publication advances the index.
The receipt identifies the task that committed both the data-source retirement
and the generation publication, including when a separate publisher uses the
same sealed generation.

Cancellation, input/index conflicts and native errors stop scheduling. A
post-commit acknowledgement failure is recognized from committed ownership.
Restart starts no optimizer or maintenance work. Explicit resume reconciles
the committed plan before permitting fresh work. Registered build recovery
and bounded index GC retain referenced artifacts and reclaim abandoned work.

## Retention and limits

New queries compose the rebuilt B, unselected B and current F/T with the
existing visibility, filtering, distance, typed fetch and global top-k path.
Old ReadViews retain private source/index copies, including delayed fetch of
rows from a subsequently collected empty file.

Index GC does not remove historical native source data. Empty data/delete
files remain under DuckLake snapshot retention until explicit expiry and
cleanup. This operation releases current physical/vector row capacity;
historical disk space still depends on that retention policy. Unified
automatic source expiry/GC and long-running space/resource qualification
remain follow-up work.

The current local native DuckDB metadata, fixed schema, unpartitioned and
unencrypted Vortex prototype scope remains. The three-B, 100,000-row,
64-MiB source/vector/artifact and existing F/T/write/collection budgets remain
unchanged. There is no manifest decoder, compatibility layer, fallback,
dependency-pin change or SPFresh online update API in this change.

## Reproduction and local evidence

```sh
python3 -I test/vane/test_native_vortex_prune_files.py \
    --duckdb build/index-shell/duckdb \
    --driver build/index-shell/ducklake_vortex_catalog_driver \
    --probe build/index-cargo/ci/ducklake-vortex-index-probe \
    --output-dir build/index-logs/prune-files
```

The three-B proof drops one 2,048-row member from a 4,096-row B while accepting
concurrent T/flush/T commits. Only the selected generation changes. Healthy
source identities and artifact signatures of both other B remain unchanged;
current/historical rows and row IDs match native SQL. Old/new private views
and delayed typed fetch remain readable after index GC and explicit native
source cleanup.

At the unchanged 100,000-row/vector ceiling, 4,096 B rows plus 95,904 F rows
initially reject another T row. Pruning releases exactly 2,048 slots, leaves
97,952 current physical rows, and admits that T row. Fully healthy, partially
deleted, 64-row retained and entirely empty inputs reject explicit pruning
before allocation.

## Local verification against #50

DuckLake `b591a3da` incorporates #50 at `50273e90`. The native builds
use DuckDB `d8cdaa33`, Vortex `b20e34c2`, and the
DuckDB Vortex wrapper `84cef204`.

Pruning passed 24 checks and 88 gated query
comparisons with minimum fixture recall 1.0 in 55.31
seconds within the 180-second budget. The SIGKILL cases include resume after
an independent append advances the revision. The public SQL case passed
73 assertions, including writable caller checks,
the zero-change prune snapshot and exact earlier delete events.

All 21 native process proofs passed. ON passed 30
Vortex SQL cases and 1367 assertions. ON and OFF passed
86 base cases and 3582 assertions each with
13 optional-extension skips per configuration. The complete
DELETE and table-change suites, Vortex and Parquet recovery and cleanup,
32 ABI cases, isolated cost checks, formatting, Clippy and Rust workspace
tests passed. OFF metadata expiry reclaimed the prune source marker and masks
and preserved the healthy file.
The prune_files, rewrite_append fixture ran again within its original budget.

The qualified catalog driver SHA-256 is
`fd1b696739b6db29b8ae9dd7a3f361f789b43cfb5510c4430a5e79abaf2b98fb`.
