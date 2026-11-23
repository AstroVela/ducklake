# Vane distributed scans

DuckLake has an explicit Vane build mode for distributed, file-backed table scans. The ordinary DuckDB extension
build remains the default and does not compile Vane headers or distributed scan sources.

Initialize the independent CI tooling and build against the exact Vane and vcpkg revisions in `vane-extension.toml`:

```shell
git submodule update --init --recursive
VCPKG_TOOLCHAIN_PATH='<vcpkg>/scripts/buildsystems/vcpkg.cmake' make vane_ci
```

`make vane_wheel` builds a wheel containing the statically linked DuckLake extension. The Vane pipeline verifies the
native backend and a two-worker Ray scan from that packaged wheel.

The independent `vane-extension-ducklake` provider uses the same pinned Vane
source and shared CI tooling, and requires the exact `vane-extension-sqlite-scanner`
provider for its metadata backend. See [provider releases](vane-provider-release.md)
for dynamic-wheel qualification and the manual TestPyPI publication workflow.

The [native realtime vector proof](native-realtime-vector-proof.md) validates
inlined vectors, transaction visibility, materialization, and process recovery
with a DuckDB metadata catalog. Its SQL tests are part of the native Vane test
selection. The Ray qualification below exercises the distributed read path.

Distributed scans support committed, unencrypted data files, committed delete
state, and metadata-inlined data. During planning, the coordinator reads each
inline table at the query's DuckLake snapshot and freezes typed batches into
the scan splits. Workers receive the values, source schema and field ids, and
the same table/snapshot identity used by the file splits. They never attach
the metadata catalog or fetch the latest inline rows during execution or retry.

Inline splits preserve logical row ids, source filename/file index, and snapshot
ids across batches. Earlier inline schema versions use the existing field-id
mapping and default expressions. The scan wire protocol is version 3; mismatched
versions and split identities are rejected.

The first implementation freezes complete inline rows and limits the total
serialized inline split payload to 64 MiB per scan. It rejects larger scans
before worker execution. Projection pruning during capture and larger inline
datasets need separate qualification. Planning does not flush the table or
substitute a local query when the limit is reached.

Transaction-local changes, encrypted files, change scans, and legacy Parquet
VARIANT decoding remain unsupported. Distributed mutations still require
file-backed sources; realtime validation uses ordinary native transactions
for writes. Ray execution does not fall back to a local scan.

When a local DuckDB file stores the metadata catalog, attach DuckLake read-only before Ray queries so the coordinator
and Ray driver can open the catalog concurrently. Workers receive only the serialized scan state and never attach the
metadata catalog.

For default Ray reads and writes, load the installed DuckLake provider and attach
`ducklake:sqlite:/absolute/path/metadata.sqlite`. The provider dependency loads
SQLite automatically. The local qualification shares this file between the
client and Ray driver processes on one physical host; a DuckDB metadata file
opened for writing cannot provide that cross-process access.

## Realtime vector qualification

See the [local validation record and open gates](ray-realtime-vector-proof.md)
for the baseline comparison, runtime fix, and failed node-loss experiment.

Run the standalone proof from a matching installed Vane wheel containing the
DuckLake and SQLite scanner extensions:

```shell
python3 -I test/vane/test_vane_realtime_vector.py --report ray-realtime-vector.json
```

The validation runtime pin includes a [separate Vane fix](https://github.com/AstroVela/vane/pull/1103) that keeps hash-join
exchange boundaries even with one logical partition. Top-k late materialization
can create two scans of the same table; FTE must combine their matching rows
rather than distribute the two sources as independent work. The v0.2.0 runtime
fails this historical top-k case with an empty scan-source queue. The proof keeps
the optimizer enabled and uses the fixed runtime rather than bypassing that plan.

The proof owns a local Ray cluster with two execution nodes and uses native
SQL writes against a temporary SQLite metadata catalog in WAL journal mode,
which permits its concurrent reader/writer scenario. It compares distributed
exact-distance top-k and business-field reads against native SQL, checks that
pure-inline reads use both execution nodes before Parquet files exist, checks
schema defaults and virtual row identities, and combines inline updates with
materialized files and deletions.

It also freezes a worker template, commits and materializes later changes,
and replays the same batches on isolated worker connections without a metadata
attachment. A real Ray query is paused while another connection commits and
flushes; its original rows must remain unchanged and a new query must see the
later commit. The packaged-wheel Ray CI job runs this proof and preserves its
JSON report.

Use `--dsn '<dedicated PostgreSQL DSN>'` with a wheel that also contains the
PostgreSQL scanner to repeat the proof against a uniquely owned metadata schema.
The PostgreSQL scanner currently pinned by DuckLake does not build against
Vane's write-target constructors. This Ray path therefore still needs a
matching scanner build; the native-only PostgreSQL proofs do not qualify it.
Use `--kill-node` to additionally remove one of the proof's execution nodes
while its query is paused and check recovery on the remaining node. These
options never target an existing Ray cluster or unrelated catalog schema.

The JSON report contains functional results and query durations. The warm
mixed-read case also records the time from the native COMMIT return to completed
distributed readback, including the native oracle check. This is an upper
bound from a correctness probe, not a latency SLA or throughput benchmark.
The proof uses one physical host and local data files; object-store access,
multi-host deployments, and ANN retrieval remain separate validation gates.
