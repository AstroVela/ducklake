# Managed native Vortex upsert and delete

The [retryable append API](native-vortex-write-batch.md) now shares its native
transaction and request ledger with an explicit managed mutation API. A managed
table has one current row per non-NULL business `id`. An upsert replaces the
whole row; a delete removes the current row for that id. Existing B generations
stay immutable while the committed replacement in T becomes queryable.

## Explicit ownership

Initialize the index, optimizer and write catalogs as in the
[installed Vane flow](vane-native-vortex-proof.md). Then opt an empty table into
managed writes before submitting any batches:

```python
connection.execute("SELECT * FROM ducklake_manage_vortex_writes('lake','main','items','knn')")
sql = (
    "SELECT * FROM ducklake_mutate_vortex_batch('lake','main','items','knn',?,"
    "?::STRUCT(id BIGINT,embedding FLOAT[],category VARCHAR,payload VARCHAR)[],?::BIGINT[])"
)
rows = [{'id': 1, 'embedding': [0.125, 0.0], 'category': 'A', 'payload': '最新向量'}]
receipt = connection.execute(sql, ['request-1', rows, [2]]).fetchone()
assert connection.execute(sql, ['request-1', rows, [2]]).fetchone() == receipt
```

The named index is matched by its exact catalog key. Enrollment records its
dimension so delete-only requests have stable canonical bytes. Opt-in requires
no current rows, B segments across its named indexes, or prior request receipts
for that table UUID.
Fresh write catalog initialization creates `ducklake_vortex_keyed_tables_v1`,
with actual native metadata primary/unique keys for UUID and table ID. Existing
catalog layouts require fresh initialization; there is no migration or inferred
write mode. Opt-in is explicit and has no automatic takeover or disable path.

Unmanaged tables retain their append API and ordinary SQL semantics. Managed
tables reject the append API, ordinary SQL INSERT/UPDATE/DELETE, and schema,
rename or drop changes at native commit. Unrelated tables remain writable.
Native flush, compaction, optimizer rewrites and source retention continue
through their existing paths. Ordinary deletes against newly flushed data still
require managed write authority at commit, including when the delete removes
the entire staging file.

The commit check reads current managed ownership while holding the existing
write-admission reservation through commit. Registration takes that same
reservation. An ordinary transaction started before opt-in is therefore still
rejected if it tries to commit after ownership is registered. Admission checks
the writer's original catalog and metadata attachments. Ordinary native commit
retries verify those same attachments before staging another attempt. Mutation
authority is private to the dedicated native connection and is cleared on every
return or failure. There is no SQL setting to bypass it.

## Batch and retry semantics

A batch supplies complete upsert rows and a list of deleted IDs. Across both
lists, IDs must be non-NULL and unique: duplicates, including an id present in
both lists, fail explicitly. Either list may be empty; the combined batch must
contain 1–1000 operations and fit 4 MiB of canonical content. Vectors retain the
existing float32 dimension/finite checks and signed-zero normalization. Category
and payload may be NULL; Unicode and embedded NUL bytes remain exact.

All mutations share the existing WriteBatch serialization lock. The dedicated
transaction deletes existing versions of the touched ids and appends the upsert
rows, then stages its request receipt on that same native metadata transaction.
Native snapshot isolation supplies visibility and stable row versions. No
second WAL or per-key version directory is introduced; key lookup reuses native
table execution and configured write budgets still apply. Configure those
budgets before a growing workload: a per-batch bound alone does not establish
a table-size or process-memory bound.

The result retains `table_uuid, request_id, payload_hash, row_count, snapshot_id`.
Here `row_count` counts requested upserts plus deletes, including missing-key
deletes. `snapshot_id` identifies the actual native snapshot: when a missing-key
delete creates no data change it reports the existing snapshot. Its receipt
still commits durably after the same admission, attachment, index and
cancellation checks for the bound lake. The request catalog keeps its existing
100,000-record limit and never automatically expires records.

Identity remains `(table_uuid, request_id)`. An exact retry returns its original
receipt without repeating any mutation. Changed content fails. Replaying an old
upsert after a later deletion cannot resurrect the row; replaying an old delete
after an explicit new upsert cannot remove that newer row. Concurrent requests
for the same id serialize and the later successful native commit wins. Ordinary
writers to other tables or maintenance may cause a native conflict; it propagates
with zero internal commit retries. The client explicitly resubmits the same
request from a fresh transaction.

The canonical mutation bytes use ASCII `ducklake-vortex-mutate-v1`, uint32
little-endian dimension, uint64 upsert count, then the existing ordered row
encoding from the append guide. This is followed by uint64 delete count and
each signed int64 id's two's-complement bits in little-endian order. Input list
order remains significant. Both operation lists contribute to the digest and
byte budget.

## Visibility, maintenance and recovery

The named search uses its existing B/F/T ReadView. Native visibility filters old
B/F versions before top-k; the new T row supplies its current vector and business
columns. Mutation does not publish or rebuild B. A later flush and independent
B2 publication preserve B1, while selective source rewrite and GC use the
existing ownership protocols. A previously pinned ReadView keeps its older
typed rows after both generation and source GC.

Native commit owns deletion masks, replacement rows and the receipt atomically.
Cancellation or failure before commit rolls back all three; a lost response
after commit is resolved by exact retry. Native metadata WAL recovery restores
the same outcome. [Source retention](native-vortex-source-retention.md) can
expire the original snapshot without expiring its receipt or reapplying data.

For managed writes and queries with the background pipeline driving every
maintenance stage, see [automatic flow](native-vortex-automatic-flow.md).

## Qualification

```sh
cmake --build build/runtime --target ducklake_vortex_write_driver
VANE_RUNNER=local-fast python -I test/vane/test_vane_vortex_keyed_writes.py \
    --driver build/runtime/ducklake_vortex_write_driver \
    --output-dir /tmp/vane-vortex-keyed-proof
```

The installed-provider probe covers managed ownership, batch validation, write
admission rollback after staged deletes, retirement of a committed unindexed
file with an active historical reader, immutable B, T replacement, old request
replay, concurrent same-key writes, table-scoped identity, independent B2,
rewrite and source expiry. Queries compare
typed rows and distances against the visible-row oracle using the existing
recall gate.

The native write driver covers pre-enrollment transactions, catalog reattachment,
enrollment with a B segment under another index name, missing-key cancellation
while waiting for publication, writes with multiple attached lakes in immediate
transaction mode, commit retries with stable and replaced attachments, a held
ReadView through rewrite and GC, failures around commit, cancellation and native
conflict. Three SIGKILL boundaries recover through installed Vane in fresh
Python processes with a nonempty metadata WAL.
The SQL fixture in `test/sql/vortex/native_vortex_keyed_writes.test` covers the
independent digest vector, enrollment under a changed default collation, and
rollback of partial and full deletes against transaction-local flush output.
Qualification also runs the inherited source retention, append and basic flow
probes and the Vane release suite against the pinned provider closure.

The scope remains fixed-schema local POSIX storage, native DuckDB metadata and
exclusive single-process catalog ownership. Direct modification of internal
metadata is outside that ownership protocol. There is no partial patch, remote
writer fencing, compatibility layer, fallback or SPFresh original online update.
