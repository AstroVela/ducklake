# Installed Vane native realtime vector flow

This validation connects the [local realtime vector flow](native-vortex-basic-flow.md)
to the public `vane.connect()` API. It uses the explicit `local-fast` runner,
a normal Vane runtime wheel and two independently installed native providers.
The DuckLake provider declares its exact Vortex provider dependency. Loading
uses Vane's existing installed-provider resolver and source/ABI/artifact
identity checks.

For bounded append with concurrent request identity and safe client retries,
see the [native WriteBatch API](native-vortex-write-batch.md).
For explicit snapshot retention and background source reclamation, see
[source retention](native-vortex-source-retention.md).
For table-owned, retryable whole-row upsert and deletion by business id, see
[managed writes](native-vortex-keyed-writes.md).
For a workload that configures the workers once and then only mutates and
queries, see [automatic flow](native-vortex-automatic-flow.md).

## Fixed inputs and packaging

`vane-native-vortex.toml` records the Vane profile. The validation fixes:

| Input | Revision |
| --- | --- |
| Vane | `e14bf92d4053141697a59dcbd6098677cbddb80a` |
| duckdb-vortex | `b819cc031be61efabeedd6ded0a814984dc56d86` |
| Vortex | `65dab44f86495d35bb46f69c8ec5cfc7a3544c1b` |
| SPFresh | `5893eb61ee3b18610b6b00f1939be7dae1af8904` with the existing static-only patch |
| LLVM OpenMP | `18.1.8`, source archives verified by SHA-256 |

The providers compile against that Vane source tree and remain separate from
the runtime's usual six linked extensions. `extension_config_vane_native_vortex.cmake`
requires `VANE_LOADABLE_EXTENSIONS=vortex;ducklake`, native DuckLake execution
and the qualified static SDK. `DUCKLAKE_VANE_NATIVE` explicitly selects the
Vane engine's context-taking MERGE planner constructors, shared with its
distributed build. It enables no distributed callbacks. The existing
distributed profile remains a separate build entry.

SPFresh needs OpenMP and zstd. The system GNU OpenMP static archive fails the
shared-object PIC link check. `scripts/build_vane_vortex_native_sdk.sh` instead
builds PIC static LLVM OpenMP and uses an explicit PIC static zstd archive.
It then runs the existing pinned SPFresh build and its four native tests,
including the shared-object link check. The SDK records the selected OpenMP
archive for the provider configuration. Provider loading needs no SDK source
directory or external OpenMP/zstd shared library.

The [Vane native provider workflow](../.github/workflows/VaneNativeVortex.yml)
contains the complete source checkout, dependency build, wheel build and
qualification commands. It uses the existing Vane wheel builder and local
provider loader. The runtime explicitly enables the existing public CI test
key, and the two providers are signed with its matching repository fixture
before packaging. Signature verification and `allow_unsigned_extensions=false`
remain enabled. The generated provider wheels are marked test-only and must
not be published. The validation uses the host's honest
`manylinux_2_39_x86_64` baseline; it does not qualify a lower glibc baseline.

## Use the installed packages

Install the matching runtime and both provider wheels from the validation
artifacts into one environment. Select native execution explicitly:

```sh
uv pip install path/to/vane_ai-*.whl path/to/vane_extension_vortex-*.whl path/to/vane_extension_ducklake-*.whl
export VANE_RUNNER=local-fast
```

Run this against a fresh directory:

```python
from pathlib import Path
import time
import vane

root = Path('/tmp/vane-native-vector-basic')
root.mkdir(mode=0o700, exist_ok=False)
(root / 'indexes').mkdir()
connection = vane.connect(config={
    'autoinstall_known_extensions': 'false',
    'autoload_known_extensions': 'false',
    'extension_directory': str(root / 'extensions'),
})
connection.execute('LOAD parquet')
vane.load_installed_extension('ducklake', connection=connection)
connection.execute(
    f"ATTACH 'ducklake:{root}/metadata.ducklake' AS lake "
    f"(DATA_PATH '{root}/data', DATA_INLINING_ROW_LIMIT 1000)"
)
connection.execute('CREATE TABLE lake.items(id BIGINT,embedding FLOAT[],category VARCHAR,payload VARCHAR)')
connection.execute("CALL ducklake_set_option('lake','experimental_data_file_format','vortex',table_name=>'items')")
connection.execute("SELECT * FROM ducklake_initialize_vortex_indexes('lake')")
connection.execute("SELECT * FROM ducklake_initialize_vortex_optimizer('lake')")
connection.execute(f"SELECT * FROM ducklake_create_vortex_index('lake','main','items','knn','{root}/indexes',2)")
connection.execute(
    f"SELECT * FROM ducklake_enable_vortex_optimizer('lake','main','items','knn','{root}/indexes',"
    "2,64,4194304,128,67108864)"
)
connection.execute("SELECT * FROM ducklake_set_vortex_write_budget('lake','main','items','knn',1000,4194304,512,67108864)")

nearest_sql = "SELECT id,payload,distance FROM ducklake_vortex_search('lake','main','items','knn',[0,0]::FLOAT[],5)"
connection.execute("INSERT INTO lake.items SELECT i,[i::FLOAT,0::FLOAT],'A','实时向量' FROM range(16)t(i)")
print(connection.sql(nearest_sql).fetchall())  # Committed T, before files or ANN.
connection.execute("CALL ducklake_flush_inlined_data('lake',table_name=>'items')")
print(connection.sql(nearest_sql).fetchall())  # Small F, before ANN.
connection.execute("INSERT INTO lake.items SELECT 16+i,[16+i,0],'A','后台索引' FROM range(112)t(i)")
deadline = time.monotonic() + 120
while True:
    state, running, revision, error = connection.execute(
        "SELECT state,running,published_revision,error FROM ducklake_vortex_optimizer_status('lake')"
    ).fetchone()
    if state == 'failed':
        raise RuntimeError(error)
    if revision == 2 and not running:
        break
    if time.monotonic() >= deadline:
        raise TimeoutError('Background ANN publication timed out')
    time.sleep(0.02)
connection.execute("INSERT INTO lake.items VALUES(10000,[0.125,0],'A','B旁实时向量')")
print(connection.sql(nearest_sql).fetchall())  # B plus current T.
connection.close()
```

All queries use one named entry. T/F compute exact distances from raw vectors;
published B use the existing static ANN path. Visibility, filtering, typed
values and top-k use the same current ReadView protocol. Later promotion
appends B2 while preserving B1.

## Installed-package verification

```sh
VANE_RUNNER=local-fast python -I test/vane/test_vane_native_vortex.py \
    --output-dir /tmp/vane-native-vortex-proof
```

The probe requires Vane from the invoking environment's installed wheel. It
loads DuckLake and its Vortex dependency from installed providers, exercises
empty/T/F/B/F/T queries and prepared statements, then tests MERGE insert/update/delete,
stable row IDs across flush, an old native transaction snapshot, and a second
B with unchanged B1 artifacts. A subprocess commits another T, is killed with
SIGKILL while metadata WAL is nonempty, and is reopened through `vane.connect()`.
The recovered typed rows and native row IDs must match the committed batch.

Local qualification on Linux x86-64 with Python 3.12 and the fixed Vane
`0.3.0.dev1` wheel passed all eight phases and 32 oracle comparisons, with
minimum fixture recall 1.0. The engine reported
`v1.5.5-vane.e14bf92d40` / source ID `e957000277`. Both the probe and the
Python example above ran from installed wheels with `VORTEX_SPFRESH_NATIVE`
unset. The SDK's four native tests and PIC check passed; the two providers
passed wheel platform checks and had no unresolved runtime symbols or
OpenMP/zstd shared-library dependencies. These are bounded local fixtures,
not a throughput or production-scale qualification.

The profile retains the native DuckDB metadata, fixed four-column schema and
existing three-B/row/byte/write budgets. SQL transactions remain the write
interface. This change adds no compatibility layer, implicit runner fallback,
manifest migration, Ray execution or SPFresh native online-update API.
