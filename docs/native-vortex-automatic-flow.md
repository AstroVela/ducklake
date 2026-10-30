# Managed writes with automatic Vortex maintenance

This qualification joins [managed upsert/delete](native-vortex-keyed-writes.md)
with the existing optimizer, selective merge, source rewrite and reclamation
workers. After explicit configuration, the foreground workload submits mutation
batches and queries. It never calls flush, index build, merge, rewrite or collect.

## Configure once, then write and query

Use the matching signed providers and fresh table/index from the
[installed Vane guide](vane-native-vortex-proof.md). Initialize the write catalog
and register the empty table before any data enters it:

```python
connection.execute("SELECT * FROM ducklake_initialize_vortex_writes('lake')")
connection.execute("SELECT * FROM ducklake_manage_vortex_writes('lake','main','items','knn')")
connection.execute(
    "SELECT * FROM ducklake_enable_vortex_optimizer('lake','main','items','knn',"
    "'/absolute/path/to/indexes',2,128,4194304,128,67108864)"
)
connection.execute(
    "SELECT * FROM ducklake_set_vortex_write_budget('lake','main','items','knn',256,4194304,1024,67108864)"
)
connection.execute(
    "SELECT * FROM ducklake_set_vortex_data_compaction_policy('lake','main','items','knn',true,32,25)"
)
connection.execute("SELECT * FROM ducklake_set_vortex_source_retention('lake',true,2,0)")
connection.execute("SELECT * FROM ducklake_enable_vortex_maintenance('lake',2,67108864)")

mutation = (
    "SELECT * FROM ducklake_mutate_vortex_batch('lake','main','items','knn',?,"
    "?::STRUCT(id BIGINT,embedding FLOAT[],category VARCHAR,payload VARCHAR)[],?::BIGINT[])"
)
rows = [{'id': 1, 'embedding': [0.125, 0.0], 'category': 'A', 'payload': '实时向量'}]
receipt = connection.execute(mutation, ['request-1', rows, []]).fetchone()
nearest = connection.execute(
    "SELECT * FROM ducklake_vortex_search('lake','main','items','knn',[0,0]::FLOAT[],10)"
).fetchall()
```

Use the actual absolute index root and the index's configured dimension. The
small thresholds, two-snapshot retention and zero minimum age deliberately
exercise every background stage in a bounded test. They are not production
sizing guidance. A successful mutation is queryable immediately in T. Thresholds
control subsequent T to F flush and independent immutable B publication. A
subthreshold tail can remain queryable in T until more data arrives.

The optimizer's existing global segment budget selects a bounded B pair to
merge when append needs room. Source rewrite removes obsolete versions from
selected files, and maintenance reclaims registered retired artifacts and
eligible native source files. The workload does not replace those ownership
protocols or call SPFresh's original online-update API.

## Qualification

```sh
cmake --build build/runtime --target ducklake_vortex_write_driver
VANE_RUNNER=local-fast python -I test/vane/test_vane_vortex_automatic_flow.py \
    --driver build/runtime/ducklake_vortex_write_driver \
    --output-dir /tmp/vane-vortex-automatic-proof
```

The installed-provider workload grows four independent batches of 128 rows.
The fourth batch requires automatic selective merge then append, preserving the
unselected B generation unchanged. Eight subsequent rounds each replace 96
current rows, delete 32 other IDs and insert 32 new IDs. There are always 512
live business keys. The unchanged fixture gate is recall@10 >= 0.8; visibility,
typed row versions, uniqueness, filters, distance and ordering are hard checks.
The public probe checks exact native row state and named search immediately
after each mutation and again after the background pipeline drains.

The native driver runs the same rolling workload alongside filtered and
unfiltered queries. Each query records its actual ReadView snapshot. Validation
uses the committed logical model at that snapshot, including physical-only
snapshots between mutations, and requires coverage of every committed mutation
phase. Stage observations prove that flush, build, selective merge, source
rewrite and source deletion all happened automatically.

A separate native case pauses the worker before flush with a full inline
budget. Another write must fail with `RESOURCE_EXHAUSTED`, leaving neither rows
nor a request receipt. Queries still return the exact committed versions. Once
the worker is released, the client resubmits that same request explicitly and
replays its receipt. The fixture waits for database references to expire before
process exit, as the existing catalog fixture does. Background observation
callbacks own their state for the worker's lifetime.

Four real SIGKILLs interrupt the flow after native mutation commit before its
response, before B publication commit, after B publication commit, and after a
source-deletion intent commits. Every case requires a nonempty native metadata
WAL. Recovery runs through installed Vane in a fresh Python process, reconstructs
the exact logical model from durable receipts and the predeclared requests,
then explicitly resumes optimizer and maintenance. Restart alone cannot start
work. A published B is not rebuilt after its lost response. Old exact request
replays cannot restore deleted or superseded row versions.

The client may explicitly resubmit the same request on a native conflict or
write-budget rejection. Unexpected errors and a failed worker fail the probe;
the runtime still has zero implicit commit retries. There is no added
compatibility layer or fallback.

Local qualification completed 20 public phases and 80 oracle comparisons with
minimum recall 1.0. The native workload completed 575 snapshot-matched
concurrent queries with minimum recall 1.0. All six native/recovery cases passed,
including the four real SIGKILLs. Related managed-write and append probes passed 28 and 16 oracle
comparisons respectively, plus their existing 13 and seven native/recovery
cases. All 23 new SQL assertion blocks and the existing 108 managed-write,
retention and append blocks passed through installed Vane. The standard native
SQLLogicTest harness runs in CI. Separate Vane release processes passed 1,222
non-Ray and 32 Ray tests, with 15 optional-dependency skips. Formatting and the
Vane source/native copyleft checks passed. These test-only changes use the
existing signed provider bytes and the rebuilt native test driver.

## Physical footprint and limits

After each drain, the probe checks actual source bytes against the complete
retained native data/delete-file inventory. Current source-file IDs must match
the published index coverage. Actual generation directories must match the
active publication, with at most three B segments, no retired directories and
no registered build scratch. Registered artifact bytes are checked separately
from each generation's manifest bytes.

The fixed-live-row workload bounds retained physical source rows and records
source/index bytes after each round. Native metadata, WAL, durable request
receipts and caller-owned reference JSON are reported separately; their
lifetimes are not governed by source/generation GC. The existing request ledger
has a hard limit and no automatic receipt expiry. This test does not claim a
bound for total disk usage, production throughput, unbounded ingestion or remote
writer fencing.

`test/sql/vortex/native_vortex_automatic_flow.test` covers the public
configuration, current T query and exact retry without timing-dependent waits.
The Vane native workflow runs the complete installed proof and retains its
report, native driver logs and recovery logs. It reuses the pinned
Vane/Vortex/SPFresh closure and signed provider build.
