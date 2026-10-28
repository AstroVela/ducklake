# Ray realtime vector proof

This draft extends the native realtime vector proof through Vane's default Ray
runner. A committed metadata-inline delta can be searched together with immutable
files without waiting for a flush. It uses exact SQL distance ranking and a
separate native connection for writes and reference results.

## Execution contract

The coordinator freezes the visible inline rows, their original schema and
field IDs, and the table/snapshot identity into typed scan splits. File splits
retain their snapshot-specific deletion and visibility state. Replacement workers
receive the same frozen values and never attach the metadata catalog. Native
transactions provide the commit boundary for insert, update, delete, and flush.

The wire protocol is version 3 and rejects mismatched versions. Complete inline
rows are captured with a 64 MiB serialized-payload limit per scan. Exceeding that
limit raises an error during planning. This draft does not claim ANN performance
or a latency SLA; projection pruning and larger inline datasets need further work.

Two defects surfaced during actual Ray validation:

- A flush on another connection can retire an inline table without changing its
  schema version. Scans now read the inline-table registry in the same metadata
  transaction as the file list instead of relying on cached catalog names.
- Top-k late materialization can create a single-partition hash join over two
  scans. Vane must retain an exchange boundary on both sides even when logical
  partition counts are one; otherwise FTE can schedule the sources independently
  and fail with an empty source queue. The separate
  [Vane draft #1103](https://github.com/AstroVela/vane/pull/1103) contains that fix.
  The optimizer stays enabled in this proof.

## Local validation setup

| Component | Revision or configuration |
| --- | --- |
| DuckLake baseline | Draft #25, `4f63246ae18ee3d627cfaa6791d55ab075ad2929` |
| Native DuckDB regression engine | `d8cdaa33fda8df955cc76ef58a280f68f4cd43fa` |
| Vane validation runtime | `e14bf92d4053141697a59dcbd6098677cbddb80a`, based on v0.2.0 |
| SQLite scanner | `73254c99b1873a083bbae0640a3d8a7fef431761` |
| Python / Ray / Arrow | 3.12 / 2.59.0 / 26.0.0 |
| Catalog and data | Temporary SQLite catalog in WAL mode; local data files |
| Ray topology | One head with zero CPUs, two execution nodes with two CPUs each, one physical host |

The runtime is built and installed as a regular wheel with DuckLake and SQLite
statically linked. Baseline control uses the same Vane SDK and dependencies with
only DuckLake replaced by unchanged #25 code: committed-inline planning is
rejected there and succeeds with this draft.

The normal two-node run passes these checks:

- 4,096 committed inline vectors and business fields are consumed on both
  execution nodes before any Parquet file exists, without duplicate row IDs.
- Exact filtered top-k, virtual source/row identities, and an added column's
  default agree with native SQL.
- A frozen worker template replays the original rows on two isolated native
  worker connections after later updates, deletions, and flushes.
- Current and historical top-k remain correct after materialization; a new
  inline delta combines with files and committed deletes without stale results.
- A real Ray query paused inside a UDF retains its original snapshot across
  another connection's commit and flush; a later query sees the new commit.

The native regression selection passes 50 test cases and 1,421 assertions, with
seven skips for optional extensions (httpfs 1, icu 3, json 2, tpch 1). Three
additional Vane Ray regressions cover single-partition INNER, LEFT, and FULL OUTER
hash joins, including unmatched rows.

The existing two-execution-node DuckLake integration script also passes its
file-backed reads, deletes, schema evolution, distributed writes, and MERGE
checks. Its missing-result injector now preserves intermediate exchanges before
dropping the terminal writer result, so the intended commit-result fault still
runs after the new hash-join exchange boundaries are introduced.

The Vane base release tests were also run against this custom prototype wheel:
the non-Ray group reports 1,202 passed, 15 skipped, and 16 failed. All failures
concern the default package/extension contract or missing httpfs/icu support;
this wheel instead adds statically linked DuckLake and SQLite. The base launcher
stops after that failing group, so its same real-Ray selection was run separately
in a fresh process through `scripts/run_installed_pytest.sh`: 31 passed and five
optional-dependency skips (pandas 1, aiohttp 4). Qualification with the full
default release-wheel extension profile remains open.

## Open validation gates

Packaged-wheel CI run [40598873925](https://github.com/AstroVela/ducklake/actions/runs/40598873925)
passed the realtime vector proof and the dynamic-wheel integration. The static
wheel's complete two-worker integration failed with a worker `SIGSEGV` in
`malloc` while Ray serialized a result. That crash remains unresolved; the local
integration pass above does not qualify this CI path.

Follow-up uses the original CI wheel, Python 3.12.15, Ray 2.59.0, Arrow 26.0.0,
and msgpack 1.2.4. One full run instead hung on the first ordered read after an
intentionally failed UPDATE: all four ORDER BY sample tasks had finished, but
the driver waited for an unavailable Ray ObjectRef. Thread stacks and resource
snapshots were retained locally. A smaller failed-UPDATE/ordered-read probe
passed 30 consecutive iterations with Python memory debugging enabled. Neither
observation establishes the cause of the SIGSEGV.

A subsequent complete integration run of that same static CI wheel passed with
Python memory debugging enabled. The crash and non-debug hang were not reproduced
in that run; this is not a repair or successful recovery qualification.

The integration harness now prints its execution phase and preserves a primary
query exception when teardown also fails. All cleanup steps are still attempted;
a cleanup failure on an otherwise successful run fails the test. Both packaged
Ray jobs enable Python memory-debug hooks and preserve worker logs after a
failure for further diagnosis.

The separate [Vane fairness draft #1104](https://github.com/AstroVela/vane/pull/1104)
repairs a newer-main task-admission race found in #1103's CI. Its three new
deterministic regression cases fail on the original CI admission module, while
177 admission and local-model task-pool cases pass with the Python fix. This is
independent of the native worker crash; the development runtime pin is unchanged.

The optional `--kill-node` experiment fails after removing an execution node
while a UDF holds a query at the snapshot boundary. The runtime reports
`failed to quiesce FTE worker side effects before retry`, followed by session
cleanup errors involving a dead `RayWorkerActor`. This is a failed recovery
qualification of the instrumented query, not evidence of successful node-loss
replay. The report preserves the primary failure and separate cleanup errors.

The PostgreSQL Ray path remains blocked: the pinned PostgreSQL scanner constructs
`LogicalUpdate`, `LogicalDelete`, and `LogicalInsert` without the `ClientContext`
argument required by Vane. The native PostgreSQL COMMIT fault proofs remain
separate evidence. Multi-host execution, object storage, persistent recovery
intents, and ANN indexing also need their own qualification.

## Reproduction

Build the installed runtime from the exact development manifest, then run:

```shell
python -I test/vane/test_vane_realtime_vector.py --report ray-realtime-vector.json
python -I test/vane/test_vane_realtime_vector.py --kill-node --report ray-node-loss.json
python -I test/vane/test_vane_wheel_ray_ducklake.py
```

The scripts create and stop their own Ray clusters. The realtime proof forces
one scan split per task to exercise both execution nodes. The normal proof runs
in packaged-wheel CI and uploads its JSON report. Node-loss recovery remains an
explicit experiment; a failing run exits nonzero. The JSON durations are local
correctness-probe observations, including query completion and reference checks,
and should not be presented as a throughput or commit-visibility benchmark.

See [Vane scan implementation and build instructions](vane.md) and the
[native lifecycle proof](native-realtime-vector-proof.md).
