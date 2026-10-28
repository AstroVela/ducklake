# Native DuckLake SQL vector search

This validation draft extends the [native catalog proof](native-vortex-index-catalog-proof.md)
in [DuckLake #32](https://github.com/AstroVela/ducklake/pull/32). It connects named
index discovery and the existing SPFresh B/F/T query plan to an experimental
native SQL table function. Search runs inside DuckDB and returns typed business
columns and squared-L2 distance.

## SQL contract

After explicitly building and publishing an index, query it by name:

```sql
SELECT id, embedding, category, payload, distance
FROM ducklake_vortex_search(
    'lake', 'main', 'items', 'knn',
    [133, 4, 2, 0, 1, 8, 7, 16]::FLOAT[], 10,
    category => 'A');
```

The six positional arguments are catalog, schema, table, index name, query
vector and k. Omitting `category` admits every currently visible business row.
An explicit category selects rows by case-sensitive string equality before
ANN and each scan/inline top-k. NULL categories do not satisfy that predicate;
an explicitly NULL filter argument is rejected. An outer SQL WHERE filters
rows returned by the table function. General SQL predicate pushdown is outside
this step.

The return types are `id BIGINT`, `embedding FLOAT[]`, `category VARCHAR`,
`payload VARCHAR` and `distance FLOAT`. Business ID/category/payload preserve
nullability. Results are ranked by distance, with the existing physical-row
tie order. ANN may return fewer than k qualifying candidates. k must be 1–64;
the query must have 1–4,096 finite, non-NULL FLOAT components and match the
published index dimension. The category argument is limited to 64 KiB.

Each invocation acquires one owned committed read view. The native factory
resolves the ready index and source snapshot in the same metadata transaction,
then captures visible physical addresses, committed T rows and encoded B/F
bytes. Uncommitted caller writes remain outside that committed-view contract.
Prepared statements resolve the committed view and ready generation on each
execution, including after source changes and explicit index replacement.
The existing local DuckDB-metadata, unencrypted Vortex-file and four-column
business projection requirements apply.

## Shared query execution

`src/vortex_query` owns the existing physical-source adapter, admission parser,
file-bound index reference, private artifact lease and B/F/T query plan. The
validation probe imports these modules from the library. Both paths use the
existing Vortex `feat/vortex-index` implementation pinned at
`145f816102d6024a12164e37737ff8864faf5033`; no ANN implementation is added.
The Rust workspace and its root lockfile pin the shared library and probe
together.

[Vortex #25](https://github.com/AstroVela/vortex/pull/25) routes the static SDK's
SimpleLogger diagnostics to stderr, including messages emitted before main.
Its native regression reproduces stdout pollution with the preceding SDK and
requires the host's JSON output to remain exact. The query pin includes this
fix; the file-format wrapper retains its existing pinned implementation.

The C++ table function calls the library through a synchronous C ABI. The
native admission envelope is passed in memory. Queries do not launch a worker,
invoke Python, or write a JSON handoff file. Private binary source staging and
verified artifact leases retain the ownership already qualified by #31/#32.
Private staging uses the canonical system temporary directory and rejects it
when it lies inside the requested catalog's managed data path.

The explicit category predicate constructs an allow set from the admitted
business rows in bounded batches. The existing visibility exclusions and
that allow set feed B's SPFresh RowFilter and F/T eligibility before top-k.
B uses ANN; F scans unindexed files; T evaluates committed inline rows. The
bounded merge and ranked business take reuse the same plan as the process
proof. Index corruption, invalid binding and compacted-away B propagate errors
without changing source snapshot, rebuilding, probing formats or moving B to
scan.

Rust returns an owned result containing at most 64 business rows. C++ copies
them into DuckDB Values, preserving FLOAT vectors, nullable strings and Unicode,
then frees the Rust result. Source buffers, native graph handles and private
directories close before the table function emits its result chunk. LIMIT,
exceptions and cancellation release the remaining C++ state through the query
executor.

The caller's interruption state is checked during native acquisition, source
pinning, index admission, category batches, ANN completion, F batches and ranked
take. Cancellation is cooperative at these boundaries; an individual native
SPFresh call completes before its next interruption check. Callback exceptions
are caught on the C++ side, and Rust panics are caught at the C ABI boundary.
Neither unwind crosses the language boundary.

Per-query limits remain the native admission limits: 64 MiB encoded source,
100,000 rows, bounded vector/business/T data and a 160 MiB serialized envelope.
The query additionally admits at most 64 MiB of index artifacts. These counters
do not establish a process RSS or aggregate concurrent-query memory bound.
Publication and cleanup guards retain their extension-instance scope.

## Build and validation

The ON build now requires the qualified SPFresh native SDK before CMake
configuration. Follow the [pinned backend build instructions](native-vortex-index-proof.md#build-and-run),
then configure with:

```sh
-DDUCKLAKE_ENABLE_VORTEX=ON \
-DVORTEX_SPFRESH_NATIVE=/absolute/spfresh/build \
-DRust_TOOLCHAIN=1.91.0
```

The shared query library is statically linked into DuckLake. The optional native
path disables fat LTO in the pinned Vortex wrapper archive so Rust runtime
objects remain separate and the two archives can share one copy. It does not
suppress duplicate-symbol errors or change the pinned source. The default-OFF
build does not import the Rust query crate or register its SQL function.

Build the native shell, SQL runner and catalog driver as in #32, then run:

```sh
CARGO_TARGET_DIR=build/index-cargo \
  cargo +1.91.0 build --package ducklake-vortex-index-probe --profile ci --locked
python3 -I test/vane/test_native_vortex_search.py \
  --duckdb build/vortex-validation/duckdb \
  --driver build/vortex-validation/ducklake_vortex_catalog_driver \
  --probe build/index-cargo/ci/ducklake-vortex-index-probe \
  --output-dir build/sql-search-proof
```

The native owner executes real SQL searches against a persistent DuckLake
catalog. Same-view oracles cover B-only and B/F/T top-k, updates/deletes, category
prefiltering, empty eligibility, nullable/Unicode business rows and committed
writer isolation. Direct calls to the shared native execution factory expose
route diagnostics and deterministic interruption/maintenance checkpoints.

A held factory call pauses after the SPFresh generation has been leased. Other
connections publish a replacement and update/flush/rewrite/expire/clean source
files, then remove the old canonical index. Resuming must search the original
admitted B/F/T view. A fresh SQL invocation must reject the stale B layout until
an explicit rebuild/publication. Further checks reject corrupt artifacts and
dropped/recreated table UUIDs, cancel at multiple execution stages, inject an
admission failure and use SQL LIMIT. The fixture isolates private staging under
its own temporary directory and checks that each completed/failed query leaves
it empty. The probe is used only for three explicit index builds.

The local SQL proof passed 46 native requests with 12 expected rejections,
including five interruption checkpoints and two injected failures. Eight fixture
queries passed with recall 1.0 at the 0.8 threshold. Actual maintenance reclaimed
three B/F source files. The reusable prepared statement saw committed source
changes, rejected stale B and discovered the explicitly rebuilt generation.
No external query worker ran.

The ON gate passed seven Vortex SQL cases with 339 assertions, including the
new 16-assertion SQL interface case, and Vortex/Parquet process-crash recovery.
ON and default-OFF base gates each passed 86 cases with 3,582 assertions and
13 optional-extension skips. OFF rejected Vortex format selection and exposed
none of the five view/catalog/search functions. All six process proofs passed
against the final binaries and shared library; Rust Clippy with warnings denied,
Rust formatting, `make format-fix` and ON/default-OFF CMake configuration passed.

CI qualifies the pinned native SDK before the ON build, lints the shared Rust
workspace, runs ON/default-OFF gates and all six index/read-view/search process
proofs. Local validation reuses qualified DuckDB v1.5.5 engine objects while
compiling changed DuckLake code, the Rust query library and the Vortex archive
with the required link configuration. Clean hosted builds remain a separate CI
gate. Background indexing, index GC, cloud/multiprocess ownership, out-of-core
queries and general SQL optimizer integration remain later work.

The next [query-cost proof](native-vortex-query-cost-proof.md) separates SQL
visibility/T capture from the full oracle, projects category and reranking
vectors independently, and measures the change against this implementation.
