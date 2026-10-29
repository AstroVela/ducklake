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

The operation requires one named index and its installed write budget. Native
snapshot visibility must confirm at least one empty member, fully visible
retained members, and at least 65 retained rows. Partially deleted retained
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
IDs, URIs, SHA-256 identities and row counts. It creates no replacement Vortex
data file and no source-output intent. Healthy source bytes, mtimes, native
row positions and begin snapshots are preserved.

The source transaction stages native drops for the empty members. Under the
existing write reservation and exclusive publication lock, final validation
checks the exact index descriptor, schema, all selected data/delete identities
and native visibility. Independent committed T/F appends are accepted by
rebasing to the fresh native snapshot. No new source file ID needs binding.
Retiring the empty source inventory and publishing the new selected B happen
in the same metadata transaction. Coverage and artifact limits are verified
before that transaction commits.

The task's run UUID is also the manifest publication identity. Reconciliation
requires both this UUID and the candidate generation at the expected revision.
Publishing the same sealed raw candidate independently can replace the index
without retiring native source files; that publication cannot acknowledge the
interrupted atomic prune task.

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

The main proof passed 21 checks including six SIGKILL boundaries, cancellation,
metadata-stage failure, acknowledgement loss, input changes and publication
races. A separate seventh SIGKILL case verifies durable recovery after another
publisher reuses the exact sealed generation without source retirement. All
84 gated search comparisons across these cases had minimum fixture recall
1.0. These are fixture checks, not a general recall guarantee.

All 20 preceding native process proofs passed on the same catalog driver.
ON passed 21 Vortex SQL cases / 809 assertions, including the new 47-assertion
public operation case, plus Vortex recovery. ON/OFF each passed 86 base cases /
3,582 assertions with 13 optional-extension skips, and Parquet recovery. OFF
does not expose the prune API. Four isolated cost cases passed nine queries
each with minimum recall 1.0; timing measurements are informational.

The qualified catalog driver SHA-256 is
`6a6de41eafd10e3e87d9447fb6b8602f5974953861e178597c442b298232c80c`.
