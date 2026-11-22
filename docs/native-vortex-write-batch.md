# Retryable native Vortex WriteBatch

This Draft extends the [installed Vane vector flow](vane-native-vortex-proof.md)
with a bounded, append-only SQL write API. A batch and its durable request
receipt commit in one DuckLake native metadata transaction. The original
request result remains available after later writes, flush, ANN publication
and restart.

## API

Use the same installed runtime/providers and explicit `VANE_RUNNER=local-fast`
as the basic flow. Initialize the request catalog once, after attaching the
native DuckDB-backed DuckLake catalog:

```python
connection.execute("SELECT * FROM ducklake_initialize_vortex_writes('lake')")
write_sql = (
    "SELECT * FROM ducklake_write_vortex_batch('lake','main','items','knn',?,"
    "?::STRUCT(id BIGINT,embedding FLOAT[],category VARCHAR,payload VARCHAR)[])"
)
rows = [
    {'id': 1, 'embedding': [0.125, 0.0], 'category': 'A', 'payload': '实时向量'},
    {'id': 2, 'embedding': [0.25, 0.0], 'category': None, 'payload': None},
]
receipt = connection.execute(write_sql, ['request-1', rows]).fetchone()
assert connection.execute(write_sql, ['request-1', rows]).fetchone() == receipt
```

New writes require the fixed `id BIGINT, embedding FLOAT[], category VARCHAR,
payload VARCHAR` schema, Vortex format and a named index definition with the
matching dimension. Call the API on a writable DuckLake attachment as a
standalone autocommit statement. The dedicated transaction verifies the lake
and metadata attachment identities resolved by the caller.
The same named search reads the committed T immediately, then F and B after
the existing flush/index operations. Native write budgets continue to apply.

The result columns are `table_uuid, request_id, payload_hash, row_count,
snapshot_id`. The snapshot ID identifies the batch's original native commit.
Successful repeated calls return exactly that result and append no new rows.
A later update or deletion remains visible. Retrying an older request leaves
those changes in place and verifies the original canonical input, including
its dimension. A full index replacement can change the required dimension for
new writes.

Request identity is `(table_uuid, request_id)`, independent of catalog aliases
or table names. Request IDs are compared byte for byte under the database's
collation settings. A matching identity with different canonical content fails
with `request_id payload mismatch`. An independent table may use the same
request ID. The API deduplicates requests, not business IDs: append batches
may contain repeated or NULL `id` values.

## Canonical content and bounds

The SQL entry accepts the declared row type. Python parameters use the
explicit SQL cast above to convert their double-precision lists to `FLOAT[]`
before the server computes SHA-256.
Row order is significant. Embeddings require the index dimension, finite
non-NULL float32 components and a non-NULL list. Signed float zero normalizes
to positive zero in both the digest and stored values. Business IDs, category
and payload may be NULL; text retains its exact UTF-8 bytes.

The canonical byte stream has this fixed encoding:

1. ASCII `ducklake-vortex-write-v1`, little-endian uint32 dimension and uint64 row count.
2. Per row: one-byte ID presence; if present, its signed int64 two's-complement
   bits in little-endian order; then the dimension's float32 IEEE-754 bit patterns
   in little-endian order.
3. Category followed by payload: one-byte presence; if present, little-endian
   uint64 byte length followed by the UTF-8 bytes. Presence bytes are 0 or 1.

A batch has 1–1000 rows and at most 4 MiB of canonical content. A request ID
has 1–128 bytes and contains no NUL. The request catalog admits at most 100,000
records across its tables; existing committed requests still resolve when
that catalog is full. Records have no implicit expiration. The API does not
raise the existing source/vector, pending-write or ANN segment budgets.

## Atomicity and retry

`ducklake_vortex_write_requests_v1` is an explicitly initialized native
metadata table with a real `PRIMARY KEY(table_uuid,request_id)`. It is not a
DuckLake business table with unenforced uniqueness. Same-process WriteBatch
calls serialize from lookup through commit, so concurrent identical requests
return the same result and conflicting payloads have one winner.

The request INSERT uses the same metadata connection/transaction as the
batch's native snapshot and inlined/file records. There is no second WAL or
independent receipt commit. Input, native admission or pre-commit failures
roll back both data and receipt. A crash before commit recovers neither; a
crash after commit recovers both through the existing metadata WAL.

The dedicated connection explicitly sets **session** commit retry count to
zero: arbitrary staged metadata SQL must not disappear during DuckLake's
internal rollback/retry cycle. Other connections keep their retry settings.
Concurrent ordinary SQL writers and background maintenance can cause a native
snapshot conflict. That error propagates, and the caller explicitly resubmits
the same request. A lost response or cancellation near commit is resolved
the same way. The implementation adds no automatic retry or fallback.

Before committing a new batch, native write admission checks the current
embedding field and dimension while holding the publication guard. A
concurrent publication with the same vector specification can proceed.
A change to that specification rolls back the staged rows and receipt.
The caller can interrupt both the queue and the commit admission wait.

## Verification

```sh
cmake --build build/runtime --target ducklake_vortex_write_driver
VANE_RUNNER=local-fast python -I test/vane/test_vane_vortex_write_batch.py \
    --driver build/runtime/ducklake_vortex_write_driver \
    --output-dir /tmp/vane-vortex-write-batch-proof
```

The installed-provider probe checks exact receipts, concurrent duplicate and
conflicting payloads, case-sensitive identity under `nocase`, receipt replay
after full index replacement, table scope, NULL and Unicode values, literal
text after flush, canonical zero, input and write budgets, and T/F/B searches.
A native fixture covers attachment replacement, cancellation while queued
and during commit admission, initialization cancellation, snapshot conflict,
index replacement with and without a dimension change, and pre/post-commit errors.
The crash probe kills the writer at both commit boundaries for both inline
rows and directly written Vortex files. It verifies the committed WAL,
recovered typed rows, their physical storage path and the durable receipt.
The installed probe uses one verified provider cache for its isolated
databases. The C++ fixture supplies the fault hooks.

The existing Vane native-provider workflow runs both the original basic flow
and this probe. `test/sql/vortex/native_vortex_write_batch.test` supplies the
minimal native SQL regression cases. The scope remains one process with
exclusive native catalog ownership, fixed-schema local Vortex storage and
static SPFresh generations. This adds no request-record migration, upsert API,
distributed writer ownership or SPFresh original online-update API.
