# Native DuckLake to Vortex index proof

This DL3b validation draft connects DuckLake's native Vortex files to the
existing `vortex-index` contracts and `vortex-index-spfresh` provider in
[AstroVela/vortex, feat/vortex-index](https://github.com/AstroVela/vortex/tree/feat/vortex-index).
The exact index core revision for this checkout's query library and validation
probe is `664e8cb733c5fbaa2945239e806df5b22e6d190d`. The
[DL3a file proof](native-vortex-files.md) in
[DuckLake #27](https://github.com/AstroVela/ducklake/pull/27) documents the
separately pinned DuckDB wrapper and its Vortex core.

The implementation is a test-only Rust bridge and a Python process harness.
It calls Vortex's `SpFreshIndexBuilder`, `LocalIndexStore`, `SpFreshProvider`,
`RowFilter`, and `LocalFileSource` directly. It adds no search algorithm or
production SQL function. DuckLake continues to own source transactions.

## Snapshot and file binding

The harness captures an explicit DuckLake snapshot, table UUID, schema version,
indexed field ID, complete committed file inventory, and visible physical row
addresses. File IDs come from the catalog. Paths are resolved through its schema,
table and file path metadata and checked against `ducklake_list_files` for the
same snapshot. File versions are complete SHA-256 digests.

The immutable capture digest binds the Vortex index snapshot, including DuckLake
visibility and the vector dimension. A current capture cannot open an index
built from an older capture, even when some files are unchanged. A new source
snapshot or compaction requires a new generation in this proof. The separate
[mixed top-k proof](native-vortex-mixed-proof.md) introduces an explicitly
file-bound generation with exact immutable-file and vector-field identity
checks for reuse across DuckLake snapshots.

DuckLake compaction can also replace the physical inventory used by an existing
historical snapshot number. A new reader must recapture that inventory and reject
the old generation. The harness rechecks captures before ranked SQL fetch, then
qualifies rebuilt current and historical generations. Retaining old physical
addresses across compaction requires an actively pinned catalog read view or an
explicit source-view lease; this process proof does not provide such a lease.

The separate [retained read-view proof](native-vortex-read-view-proof.md) adds a
bounded source-view lease using pinned file bytes and verified private index
artifacts. It checks old-view business take across actual DuckLake cleanup;
the one-shot proof below continues to recapture physical inventories.

The bridge indexes complete physical files, including rows hidden by DuckLake.
Its per-file Vortex `LocalFileSource` instances verify and pin encoded bytes.
They accommodate the internal row-ID and snapshot columns that flush or rewrite
can add, while selecting the business vector by DuckLake's physical field ID.
The indexed projection validates FLOAT[] lengths, row/element validity and finite
components, then presents non-null FixedSizeList<Float32> batches to the existing
builder. Every physical row must pass admission, including hidden rows.

Each search constructs a mandatory exclusion set from the captured DuckLake
visibility. Business predicates supply an additional allow-list. Exclusions win,
and the resulting `RowFilter` is passed to SPFresh before top-k. The existing
SPFresh provider supports this contract; the current hnswlib provider rejects
restricted filters and is not qualified here.

Ranked addresses are taken from the pinned Vortex files and fetched through
DuckLake at the same explicit snapshot. The proof checks vectors, business
fields, squared-L2 distances and result ordering. It evaluates its fixture's
business predicates on captured DuckLake rows; it is not a general SQL predicate
translator or an optimizer integration.

## Build and run

Use the ON shell with the repository's DuckDB v1.5.5 pin. This proof runs build
and search in an independent probe. The later [SQL search proof](native-vortex-sql-search-proof.md)
also links the backend into the optional native shell; default-OFF excludes it.

Prepare the existing core implementation and its pinned native backend:

```sh
git init build/vortex-source
git -C build/vortex-source fetch --depth 1 https://github.com/AstroVela/vortex.git \
  664e8cb733c5fbaa2945239e806df5b22e6d190d
git -C build/vortex-source checkout --detach FETCH_HEAD
bash build/vortex-source/vortex-index-spfresh/native/build.sh "$PWD/build/spfresh"
export VORTEX_SPFRESH_NATIVE="$PWD/build/spfresh/build"
export CARGO_TARGET_DIR="$PWD/build/index-cargo"
cargo +1.91.0 build --locked --profile ci \
  --manifest-path test/vane/vortex_index_probe/Cargo.toml
python3 -I test/vane/test_native_vortex_index.py \
  --duckdb build/vortex-validation/duckdb \
  --probe build/index-cargo/ci/ducklake-vortex-index-probe \
  --output-dir build/index-proof
```

This currently requires Linux x86-64, a C++17 compiler, CMake, OpenMP and zstd
development files. The backend's build script runs its native tests and records
the SPFresh revision and patch digest; Cargo verifies those inputs again.
The root workspace Cargo.lock pins the probe and shared query dependencies.

The workflow first requires the DL3a ON and default-OFF gates. Its index job
downloads the qualified ON shell, builds the existing native provider and the
probe, runs Clippy and the process proof, then uploads logs and `report.json`.
No provider wheel or release artifact is published by this workflow.

## Validation

The fixture contains 4,096 eight-dimensional vectors in two actual DuckLake
Vortex files. Independent build/search processes exercise:

- Construction, sealing, qualified reopen, immutable reference publication and
  subsequent process reopen using the existing Vortex provider.
- Stable catalog file IDs and original physical positions, ranked vector take,
  business-column fetch, selective predicates and an empty allow-list.
- An updated nearest neighbor, a deleted neighbor and a new nearest neighbor.
  The old generation rejects the new snapshot; a rebuilt generation's mandatory
  visibility mask excludes the dead physical rows even when explicitly allowed.
- Data-file rewrite and merge, new file identities, current/historical generation
  rejection after compaction, and rebuilt historical reads with logical parity.
- Wrong and empty dimensions, NULL lists/elements, NaN and infinity at build;
  invalid queries, exact-mode rejection, out-of-range addresses, duplicate
  reference publication, missing/modified source files and corrupt index files.

Recall is measured separately against an exact squared-L2 oracle, with an
explicit fixture threshold of 0.8. Every returned row must be visible and match
the predicate regardless of recall. Rebuilt generations are not required to
return identical approximate neighbor sets or tied physical addresses.

The capture is trusted fixture control data. The local prototype pins at most
64 MiB of encoded source bytes and admits at most 100,000 physical rows; these
limits are not total RSS limits. Index references are exclusively published
after seal and qualified reopen. Failed builds can leave unreferenced private
generations for owner cleanup.

This draft does not implement catalog index publication/CAS, background rebuild,
source-format adoption, mixed indexed/unindexed/inlined top-k, concurrent ingestion,
cloud access, Ray execution or a production index API. These remain separate
gates. The next query proof must compose those three sources under one DuckLake
snapshot while preserving the same visibility-before-top-k rule.
