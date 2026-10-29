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
optimizer task is running; use the existing cancel/resume API for active work.

Fresh explicit optimizer initialization creates the task columns required by
this prototype. Serialized policy format 2 includes the enable flag and two
thresholds; unsupported policy versions fail explicitly. There is no migration,
legacy decoder, compatibility layer or fallback.

## Candidate selection and bounded scheduling

The planner reads current file, delete-file and inlined-deletion metadata for
the named index's active files. It reads counts rather than vector data or all
historical deletion row IDs. The selected B is the eligible generation with the
greatest reclaimable-row count; equal counts use generation-name ordering.

A candidate needs deletions, both thresholds, at least 65 remaining physical
rows for the unchanged static SPFresh options, a nonempty rewrite output and
one supported rewrite schema/partition group. Status exposes
`compaction_enabled`, thresholds, `compaction_candidate`, `reclaimable_rows`,
`remaining_rows` and `compaction_reason`. Below-threshold, below-minimum,
empty-output or unsupported-group candidates start no native task and do not
consume run IDs through repeated failing builds. Source/index identity errors
still fail explicitly; no generation or missing coverage is silently skipped.
Planning counters reflect the last probe and can lag a just-completed commit;
task publication and registered ownership determine collection safety.
If native DELETE retires an entire indexed source file, that is an existing
missing-coverage boundary requiring an explicit rebuild. This draft does not
add empty-B retirement or a new query plan for that case. An entirely masked
but still-active input is classified as an empty rewrite output and skipped.

An eligible source rewrite takes priority over flush/index work at planning.
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
error. Cancellation uses existing native checkpoints. A lost acknowledgement
whose publication is confirmed retains the committed source/index and records
`published`, but pauses the optimizer for explicit inspection/resume.
Failed or cancelled tasks do not retry automatically.

Restart starts no optimizer/maintenance work. Explicit optimizer resume checks
whether an interrupted task already published; committed work is retained,
otherwise future work is freshly planned against current metadata. The
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
file/inlined counts, single-B priority, inspectable unsupported candidates,
100,000-row capacity recovery, the complete rewrite/flush/merge/index pipeline,
input conflicts, cancellation, injected failures and SIGKILL task boundaries.
The supported native metadata, local POSIX, managed namespace, fixed-schema,
single-output and static SPFresh scope remains unchanged; online updates and
dependency-pin changes are not introduced.

### Local result (2026-10-29)

The new proof passed 22 checks and 120 gated query comparisons with minimum
fixture recall 1.0. Five controlled background rewrites advanced revision 3 to 8,
retained concurrent appends and reduced current physical rows to 549. One
enable/resume also drove four successive delete/append rounds without per-round
intervention, ending at 643 physical rows. The unchanged 100,000-row capacity
case released 3,968 slots while retaining its 95,904-row F and admitted a new
write. A single worker completed source rewrite, T flush, selective index merge,
F indexing and authorized maintenance. Unselected B generations stayed unchanged
through each source rewrite, and held private views survived index and explicit
source GC.

Five SIGKILL cases distinguish interrupted from committed work and require
explicit optimizer/maintenance resumption. Failure, input conflict, cancellation
and committed acknowledgement loss pause work without automatic retry. The two
unsupported active-input samples retain the existing ANN and report recall 0.2
(below minimum) and 0.7 (empty output) separately; their visibility/typed-row
checks pass and they do not replace or weaken the gated query comparisons.

- All 17 prior native process proofs passed.
- ON passed 18 Vortex SQL cases / 664 assertions, including the new 34-assertion
  configuration case. ON and OFF each passed 86 base cases / 3,582 assertions
  with 13 optional-extension skips and Parquet recovery; ON passed Vortex recovery.
- Audit passed 122 assertions. Rewrite/orphan groups passed 16 cases / 854
  assertions with two ICU-dependent skips.
- Four isolated cost cases passed nine queries each with minimum recall 1.0;
  timing remains informational. Formatting, Rust formatting and diff checks
  passed. Rust sources, native dependencies and their pins are unchanged.

The qualified catalog driver SHA-256 is
`d07aefd474c59ba2d1f348afce19bc723021437be500a11b5df9183e0c56a821`.
