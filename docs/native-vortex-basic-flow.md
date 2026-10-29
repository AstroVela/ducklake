# Local realtime vector query flow

The basic native path now starts with an empty table. One named query works
before the first ANN generation, through inline writes (T), flushed Vortex
files (F), background static indexing (B), and subsequent B/F/T mixtures.

For the installed `vane.connect()` entry and matching optional providers, see
[Vane native realtime vector flow](vane-native-vortex-proof.md).

`ducklake_create_vortex_index` publishes a typed zero-B definition at revision
1. It requires an empty, fixed-schema Vortex table, dimension 1–4096 and an
existing absolute index directory outside managed data. It does not invoke
SPFresh or allocate a build/generation. Duplicate names and nonempty tables
fail explicitly; existing data can use the explicit build operation.

## Run locally

Use the qualified native Vortex-enabled CLI from `VortexValidation.yml`. Create
a fresh working directory first:

```sh
mkdir -p /tmp/vane-vector-basic/indexes
build/index-shell/duckdb -batch -bail
```

Run the following in that same CLI session:

```sql
SET autoinstall_known_extensions=false;
SET autoload_known_extensions=false;
LOAD parquet;
LOAD ducklake;
LOAD vortex;
ATTACH 'ducklake:/tmp/vane-vector-basic/catalog.ducklake' AS lake
  (DATA_PATH '/tmp/vane-vector-basic/data', DATA_INLINING_ROW_LIMIT 1000);
CREATE TABLE lake.items(
  id BIGINT, embedding FLOAT[], category VARCHAR, payload VARCHAR);
CALL ducklake_set_option('lake','experimental_data_file_format','vortex',table_name=>'items');
SELECT * FROM ducklake_initialize_vortex_indexes('lake');
SELECT * FROM ducklake_initialize_vortex_optimizer('lake');
SELECT * FROM ducklake_create_vortex_index(
  'lake','main','items','knn','/tmp/vane-vector-basic/indexes',2);
SELECT * FROM ducklake_enable_vortex_optimizer(
  'lake','main','items','knn','/tmp/vane-vector-basic/indexes',2,
  64,4194304,128,67108864);
SELECT * FROM ducklake_set_vortex_write_budget(
  'lake','main','items','knn',1000,4194304,512,67108864);

PREPARE nearest AS
  SELECT id,payload,distance FROM ducklake_vortex_search(
    'lake','main','items','knn',[0,0]::FLOAT[],5);
EXECUTE nearest;
```

The empty query returns no rows. Write a small batch and run the same query:

```sql
INSERT INTO lake.items
SELECT i,[i::FLOAT,0::FLOAT],'A','实时向量' FROM range(16)t(i);
EXECUTE nearest;
CALL ducklake_flush_inlined_data('lake',table_name=>'items');
EXECUTE nearest;
```

The committed batch is immediately visible as T. After explicit flush it is
visible as F through the same query. Sixteen rows remain below the ANN build
minimum; the query computes distances from those raw vectors.

Add enough rows for background flush/indexing:

```sql
INSERT INTO lake.items
SELECT 16+i,[16+i,0],'A','后台索引' FROM range(112)t(i);
SELECT state,running,published_revision,error
FROM ducklake_vortex_optimizer_status('lake');
EXECUTE nearest;
```

Poll the status query until `published_revision=2` and `running=false`. The
first B is built asynchronously while the committed F remains queryable.
Then add another T and use the unchanged query again:

```sql
INSERT INTO lake.items VALUES(10000,[0.125,0],'A','索引旁实时数据');
EXECUTE nearest;
```

The indexed rows use ANN; current F/T use exact distances and merge into the
same global top-k. Committed updates/deletes are filtered against the current
native snapshot before returning typed row values. The next eligible F batch
can build a second B while retaining the first generation.

SQL transactions are the write interface in this prototype. A public WriteBatch
API and concurrent request-id uniqueness are separate work. Restart recovers
committed rows and the named definition through native metadata WAL. The
optimizer requires explicit `ducklake_resume_vortex_optimizer` after restart.
This guide covers local native DuckDB metadata; cloud and Vane distributed
execution remain separate integration work.

## Automated end-to-end verification

```sh
python3 -I test/vane/test_native_vortex_basic_flow.py \
    --duckdb build/index-shell/duckdb \
    --driver build/index-shell/ducklake_vortex_catalog_driver \
    --probe build/index-cargo/ci/ducklake-vortex-index-probe \
    --output-dir build/index-logs/basic-flow
```

The proof exercises 11 stages: empty, committed T, hidden uncommitted writes,
atomic update/delete/insert, small F, F during first build, concurrent F/T,
first B/F/T, mutations beside B, second independent B, and reopen after SIGKILL
with nonempty metadata WAL. An old T view preserves original typed versions
through flush, indexing and restart. Native row IDs survive materialization;
the second B leaves the first descriptor and artifact signature unchanged.

All 44 gated search comparisons passed with fixture minimum recall 1.0.
`native_vortex_basic_flow.test` additionally exercises the public SQL APIs,
prepared queries across T/F/B, argument/duplicate/root checks, dimension
admission, native row-ID preservation and catalog reopen. The existing strict
manifest-v3 zero-B representation, query path, budgets and pinned native
dependencies are reused. No compatibility decoder, implicit fallback or
SPFresh online update API is added.

The guide's SQL was also run in a fresh catalog through the qualified CLI:
background publication reached revision 2, then a new T merged into B results
as IDs `[0,10000,1,2,3]`. The process proof additionally passed with apostrophes
in its working path. All 21 preceding process proofs passed on the same driver.
ON passed 22 Vortex SQL cases / 865 assertions, including the new 56-assertion
public SQL case, plus Vortex recovery. ON/OFF each passed 86 base cases / 3,582
assertions with 13 optional-extension skips and Parquet recovery; OFF exposes
no create-index entry. Formatting and diff checks passed.

Qualified catalog driver SHA-256:
`6ba3eefea1099f867fbdaaf187ba29261202264ffb17ca53ce57c0199e7342fa`.
