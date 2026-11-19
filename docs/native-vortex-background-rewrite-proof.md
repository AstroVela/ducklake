# Explicit background Vortex data compaction

This validation follows [DuckLake #47](https://github.com/AstroVela/ducklake/pull/47).
It schedules the existing atomic single-B source rewrite from the optimizer's
current single worker. Source compaction is disabled in a new optimizer policy;
index flush/build/merge behavior remains independently available.

## Explicit configuration and task lifecycle

```sql
SELECT * FROM ducklake_set_vortex_data_compaction_policy(
    'lake','main','items','knn',true,512,40);
SELECT * FROM ducklake_resume_vortex_optimizer('lake','main','items','knn');
SELECT * FROM ducklake_vortex_optimizer_status('lake');
```

Arguments are catalog, schema, table, index, enabled, minimum reclaimable rows
(1–100,000), and minimum deleted percentage (1–100). Both thresholds are
inclusive and must be met. Configuration requires an existing optimizer policy;
enabling it also requires an installed write budget selecting the same index.
It does not resume a cancelled/stopped optimizer, authorize catalog maintenance,
expire snapshots or change write/scan/index budgets. Configure only when no
optimizer task is running. Configuration and resume use the caller's writable
catalog and metadata attachments in autocommit.

Fresh explicit optimizer initialization creates the task columns required by
this prototype. Serialized policy format 2 includes the enable flag and two
thresholds; unsupported policy versions fail explicitly. There is no migration,
legacy decoder, compatibility layer or fallback.

## Candidate selection and bounded scheduling

The planner reads current file, delete-file and inlined-deletion metadata for
the named index's active files. Counts suffice for files with one deletion
representation. For files with both, the planner subtracts overlapping positions
from the active masks under the source read guard and existing byte ceiling. The
selected B has the greatest reclaimable-row count among eligible generations.
Generation-name order breaks ties.
Scans consume every delete record at each position. When an active file has both
mask types, metadata reports that an exact count is unavailable. SQL aggregates
use visible rows, and rewrite publication skips the optional global-statistics
refresh that requires an exact metadata count. The compactor stores disjoint
masks in its commit record so its source row checks also use the unique deletion
count.

A nonempty source rewrite needs deletions, both thresholds, at least 65 remaining physical
rows for the unchanged static SPFresh options, a nonempty rewrite output and
one supported rewrite schema/partition group. Status exposes
`compaction_enabled`, thresholds, `compaction_candidate`, `reclaimable_rows`,
`remaining_rows` and `compaction_reason`. Below-threshold, below-minimum,
empty-output or unsupported-group candidates start no native task and do not
consume run IDs through repeated failing builds. Source/index identity errors
still fail explicitly; no generation or missing coverage is silently skipped.
Planning counters reflect the last probe and can lag a just-completed commit;
task publication and registered ownership determine collection safety.
Entirely masked B now uses the [metadata-only retirement protocol](native-vortex-empty-retirement-proof.md).
Native DELETE retains indexed inputs until that atomic operation removes both
source inventory and index references. A partially live B with no nonempty
rewrite output still starts no rewrite task.

An eligible source rewrite takes priority at the initial probe and at the probe
after a committed flush. If a candidate becomes eligible during flush, the
worker completes the flush and selects the rewrite before index work.
One worker pass creates one durable `rewrite` task with one selected generation,
captured index revision and unique reference path. It enters `rewriting`, calls
the existing #47 rewrite protocol, then persists its final generation and
`publishing` stage before native metadata commit. File inventory, row map, index
references and build/source ownership use the existing atomic handoff. Other
connections can commit independent T/F appends during construction; final
binding/publication serializes commits. Unselected B generations are unchanged
by this data-compaction task.

Successful work advances one index revision. A later worker pass can flush T,
selectively merge index generations or index F using existing rules. The
three-B bound, 100,000-row/64-MiB vector/source/artifact ceilings, manifest limit,
write quotas and backend options remain unchanged. There are no extra worker
threads or native build concurrency.

## Failure, cancellation and retention

Real input/index conflicts and failures stop the optimizer with a recorded
error. Probe results and pre-task failures are applied only to the policy that
requested the probe. Cancellation uses existing native checkpoints. A successful
rewrite records the task's run, generation and revision in the same metadata
commit as the source and index. Each policy retains one such receipt, including
after a newer index publication or GC. A lost acknowledgement with a matching
receipt records `published` and pauses the optimizer for explicit inspection
and resume.

Restart starts no optimizer or maintenance work. On explicit resume, the receipt
confirms a committed rewrite. For an interrupted rewrite without a receipt,
the worker plans future work from current metadata. The
independently enabled maintenance policy recovers registered builds and collects
workspaces and retired index generations under its existing shared count/byte
budget. Open private query views retain search and delayed typed fetch after GC.

Published source files continue under native snapshot retention. Automatic
snapshot expiry and source-file cleanup are not added here. Caller/reference
JSON retention remains unchanged. This proof concerns current physical
row/vector quota recovery and registered-artifact convergence, not a total disk
usage bound while historical snapshots are retained.

## Qualification

```sh
python3 -I test/vane/test_native_vortex_background_rewrite.py \
    --duckdb build/index-shell/duckdb \
    --driver build/index-shell/ducklake_vortex_catalog_driver \
    --probe build/index-cargo/ci/ducklake-vortex-index-probe \
    --output-dir build/index-logs/background-rewrite
```

The proof runs repeated background rewrites with concurrent T/flush/T commits,
current B/F/T query comparisons, unchanged unselected generations and old private
views surviving both index GC and explicit source snapshot expiry/cleanup.
Other cases cover default opt-out, exact row/percentage thresholds, mixed delete
file and inline overlap in Parquet and Puffin, post-flush priority, superseded
probes, caller attachment binding, and inspectable unsupported candidates,
100,000-row capacity recovery, the complete rewrite/flush/merge/index pipeline,
input conflicts, cancellation, and SIGKILL task boundaries. A newer explicit
index publication can supersede the rewrite before acknowledgement or restart,
including after collection of the retired generation.
The supported native metadata, local POSIX, managed namespace, fixed-schema,
single-output and static SPFresh scope remains unchanged; online updates and
dependency-pin changes are not introduced.

### Local verification against #47

The native ON and OFF builds were qualified on DuckLake `78d7040f` with
DuckDB `d8cdaa33`, Vortex `b20e34c2`, and the
DuckDB Vortex wrapper `84cef204`. Both sets of pins match #47
at `1c775d8a`.

- The background proof passed 30 checks and 124
  gated query comparisons, with minimum fixture recall 1.0.
  It completed in 93.45 seconds within the existing 180-second budget.
- The Parquet and Puffin cases each reclaimed 128 unique positions. The
  dense-overlap case retained 66 live rows through publication. The
  post-flush case reclaimed 63 rows before index merge. A later explicit
  publication and subsequent GC did not erase the committed rewrite's result.
- All 18 native process proofs, 19
  functional gates and 32 ABI checks passed. Native ON passed
  27 Vortex SQL cases with 1,080
  assertions, including the new case with 49 assertions.
- Native ON and OFF each passed 86 base cases with
  3,582 assertions and 13
  optional-extension skips. Parquet and Vortex recovery, deletion-inlining,
  cleanup, and the four informational cost cases passed.
- C++ and Python formatting, Rust formatting, Clippy, workspace tests, and
  the Git diff check passed.

A native suite reached its existing timeout on an earlier attempt. Its retry passed with the same binaries and limits.
The catalog driver SHA-256 is
`f3c63c3e574908f36c7d0ccfa8e63265b2d20b5dade7b766e1ca429e178c39a8`.
