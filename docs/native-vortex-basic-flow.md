# Local realtime vector query flow

The basic native path now starts with an empty table. One named query works
before the first ANN generation, through inline writes (T), flushed Vortex
files (F), background static indexing (B), and subsequent B/F/T mixtures.

For the installed `vane.connect()` entry and matching optional providers, see
[Vane native realtime vector flow](vane-native-vortex-proof.md).

`ducklake_create_vortex_index` publishes a typed zero-B definition at revision
1. Use a writable DuckLake attachment in autocommit mode, an empty,
fixed-schema Vortex table, dimension 1–4096 and an existing absolute index
directory outside managed data. Creation checks the committed table format
and can be canceled before publication. Creation and optimizer enable both
require matching field IDs and dimensions whenever the other definition
already exists. Duplicate names and nonempty tables fail explicitly.
Use the explicit build operation for existing data.

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
through flush, indexing and restart. The proof compares typed SQL rows and
native row IDs across materialization and restart. The second B preserves the
first descriptor and artifact signature.

The process proof also cancels a create while it is waiting for the
publication lock, then checks the catalog before creating the index.
`native_vortex_basic_flow.test` covers writable autocommit admission,
format rollback, table type, directory validation, dimension conflicts in
both setup orders, prepared queries across T/F/B, and row IDs across flush. The proof
uses the existing manifest-v3 representation, query tolerances and
write and maintenance budgets.
