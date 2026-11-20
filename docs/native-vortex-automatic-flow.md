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
The fourth batch merges the two smallest B segments in physical-row order
with generation as the tie-break. It then publishes the new F file as its
own B while preserving the unselected generation's descriptor and bytes.
Eight subsequent rounds each replace 96 current rows, delete 32 other IDs
and insert 32 new IDs. The live key count remains 512 during these rounds.

The public probe checks exact logical rows after each mutation. It qualifies
the four named and prepared searches against captured physical addresses
only when the snapshot and index revision stay fixed across the sample.
A separate frozen fixture checks strict B/F/T order for equal Float32 scores,
including when the tie crosses top-k. The ANN recall gate remains 0.8.
Exact scans must return the full expected top-k.
Visibility, typed rows, uniqueness, filters, distance and ordering are checked
independently. A child process bounds the public proof, including teardown.

The native driver runs the same rolling workload alongside filtered and
unfiltered queries. Each query records the actual ReadView snapshot and
routes. Each committed phase waits for both query variants to observe it,
while queries continue during maintenance. The native oracle uses stepwise
Float32 arithmetic and distinct scores. The first seed pauses before flush
and before build to verify inline T, scanned F and published B ANN routes.

A separate native case pauses the worker before flush with a full inline
budget. Another write must fail with `RESOURCE_EXHAUSTED`, leaving zero rows
or receipts for that request. Queries still return the exact committed
versions. Once the worker is released, the client resubmits that request
explicitly. The fixture waits for database references to expire before exit.

Four SIGKILLs interrupt the flow after native mutation commit before its
response, before B publication commit, after B publication commit, and after
a source-deletion intent commits. Each case requires a nonempty metadata WAL.
The driver saves client receipts and the observed committed state before the
checkpoint. Fresh installed Vane must recover the same receipts, index
publication and deletion intent. After explicit resume, the first pending
B is published once, an already committed B keeps its generation, and the
registered retired file is reclaimed. Replay verifies the exact receipt,
DuckLake snapshot and current logical rows after every request.

The client may explicitly resubmit the same request on a native conflict or
write-budget rejection. The runtime uses zero implicit commit retries for
these mutations. Unexpected errors fail the probe.

The default Python run covers 20 public phases, a frozen tie fixture and six
native and recovery cases. The Vane native Vortex providers workflow runs
this proof alongside managed-write, append and source-retention proofs. It
retains the report, driver logs and recovery evidence for the tested commit.

## Physical footprint and limits

After each drain, the probe checks actual source bytes against the complete
retained native data/delete-file inventory. Current source-file IDs must match
the published index coverage. Actual generation directories must match the
active publication, with at most three B segments, no retired directories and
no registered build scratch. Registered artifact bytes are checked separately
from each generation's manifest bytes.

The probe verifies that each rolling round retires prior source IDs and
reclaims retired files. It records retained rows and source and index bytes
after each round. Native metadata, WAL, durable request
receipts and caller-owned reference JSON are reported separately; their
lifetimes are not governed by source/generation GC. The existing request ledger
has a hard limit and no automatic receipt expiry. This test does not claim a
bound for total disk usage, production throughput, unbounded ingestion or remote
writer fencing.

`test/sql/vortex/native_vortex_automatic_flow.test` covers the public
configuration, current T query and exact retry without timing-dependent waits.
The Native Vortex validation workflow runs this SQL fixture. The installed
proof uses the pinned Vane/Vortex/SPFresh closure and signed provider build.
