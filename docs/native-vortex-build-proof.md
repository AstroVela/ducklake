# Native SPFresh construction and publication

This validation draft follows [DuckLake #36](https://github.com/AstroVela/ducklake/pull/36).
It connects the existing SPFresh builder to native SQL, using the same owned
binary sources as native search. A caller can build and publish a ready index
without exporting a source envelope or launching the validation probe.

The [original design](https://chatgpt.com/space/page_5ee0f8c698d08191a17198d45d152362)
references Qdrant's segment lifecycle and background optimization. This step
implements a prerequisite for that lifecycle: construction runs while other
connections keep querying and committing, then publishes a completed generation.
DuckLake owns transactions, snapshots and inline versions; Vortex/SPFresh owns
the immutable files and derived ANN generation. Automatic optimization and
write backpressure remain subsequent work.

## SQL contract

Initialize the experimental index catalog explicitly, then build:

```sql
SELECT * FROM ducklake_initialize_vortex_indexes('lake');
SELECT revision, generation FROM ducklake_build_vortex_index(
    'lake', 'main', 'items', 'knn', '/absolute/index-root/first.json', 8, 0);
```

Arguments are catalog, schema, table, index name, destination reference path,
dimension and expected ready revision. The destination parent must already
exist at a canonical local path outside DuckLake's managed data path; the
reference must not exist. The existing publisher also requires a canonical
absolute managed data path. The function returns one published revision and
generation. Revision zero requires an absent name; replacement uses the exact
ready revision. No implicit catalog initialization, retry or format probing is
added. Default-OFF builds expose none of the six native Vortex functions.

This is a synchronous explicit operation. Queries and writes on other
connections can proceed during construction. The function does not enqueue a
durable task or install a background scheduler. Its dedicated connections
capture only committed table state; caller-uncommitted writes stay outside the
build and independent publication transaction. Before the first generation is
published, ordinary exact vector SQL can query committed data. The named-index
search function still requires a ready index; construction does not add an
automatic scan mode to that entry point.

## Capture, build and publish

Build capture shares native snapshot selection, physical field validation,
source ownership and GC admission guards with query capture. It captures every
materialized B/F file at one snapshot, including physical rows currently hidden
by deletes. It does not scan visible business rows, copy T rows or flush inline
data. Pure-inline tables have no immutable-file index input and fail explicitly.
Limits remain 64 MiB encoded source bytes, 100,000 physical rows, 64 MiB vector
data and the existing artifact/manifest budgets. The four-column local
unencrypted Vortex/native DuckDB-metadata prototype restrictions still apply.

The native capture commits and releases admission protection after owning the
complete source bytes. Rust copies these through the bounded binary ABI,
verifies each file once and retains the opened reader. `source_pinned` releases
C++ source buffers. Native construction can continue after canonical files are
replaced or collected; publication must still revalidate their current identity.

The shared builder constructs a static SPFresh generation over the captured
physical files, checks exact requested identity/coverage, seals its manifest,
verifies and reopens the native index, and persists the new reference. Source
metadata travels in memory; the persisted reference is a sealed output artifact.
The probe retains its existing explicit builder entry and uses the same core.

Heavy construction holds no publication lock or metadata transaction. At the
end, the existing native publisher acquires its guards and rechecks table UUID,
field/type/dimension/metric, source files and sealed artifacts. Its revision CAS
atomically publishes the complete ready row. Compaction that removes a captured
file, corrupt artifacts or a competing revision produce explicit failure.
There is no index-to-scan error path or automatic rebuild.

Queries before publication keep the previous B/F/T plan. A fresh query after
publication treats covered F files as B and uses ANN for them. Files appended
after capture remain F; committed inline data remains T. Current visibility and
category filters still precede top-k. ANN recall is checked separately from
version correctness. This constructs a replacement static generation over the
captured file set; it does not add SPFresh dynamic updates.

## Failure and ownership

The shared C++ control boundary catches callback exceptions and checks caller
interruption; Rust catches panics before returning an owned error. Cancellation
is cooperative during capture and at source ownership, native build completion,
sealing/reference and publication boundaries. A native SPFresh build call runs
to completion before its next interruption check.

Failure before publication leaves the old ready row unchanged. Candidates,
sealed generations and reference outputs can remain outside managed data;
they are not automatically deleted or promoted. SIGKILL can also leave
unsealed construction artifacts. Automatic index GC is separate work.
After a committed publication, an observer/acknowledgement failure reports the
existing committed/uncertain-outcome contract. Inspect the ready catalog from
a fresh connection before retrying; no automatic retry is attempted.

## Validation

The native proof uses only the linked C++ driver for SQL build/search. It checks
initial publication, removal of the reference output, F-to-B promotion while
another connection updates/deletes/flushes and queries, revision-CAS competition,
cancelled/failed construction, process termination around native build and
catalog commit, and a lost acknowledgement. It removes canonical source files
after Rust ownership but before native building, requires construction to
finish from owned bytes, and rejects publication of that stale layout. Explicit
native rebuilding restores search against the rewritten files.

```sh
python3 -I test/vane/test_native_vortex_build.py \
  --driver build/vortex-validation/ducklake_vortex_catalog_driver \
  --output-dir build/native-build-proof
```

The native ON suite adds a minimal SQL build/search/rebuild case. OFF checks
include the new function's absence. CI runs this proof alongside the existing
six lifecycle proofs and cost/projection gates and uploads raw evidence. Local
builds reuse qualified DuckDB v1.5.5 engine objects and the unchanged pinned
SPFresh SDK, rebuilding affected DuckLake C++ sources and the Rust library.
Clean hosted builds remain an independent gate.

### Local result (2026-10-29)

The native construction proof passed 71 driver requests with 20 expected
rejections, 20 fixture recall checks of 1.0 at a 0.8 threshold, nine cancellation
checkpoints, three SIGKILL/restart boundaries and a lost-acknowledgement case.
Four source files were actually reclaimed during construction; the owned
source completed its native build and stale publication was rejected. The
final explicit rebuild published revision six. No probe process or external
build worker was launched. Pure inline data remained readable through exact
SQL before a file index existed.

Evidence is in `/tmp/ducklake-build-publish-proof-20261029-b/report.json` and
its native session logs. The linked driver SHA-256 is
`46034cd08c24ba766cfca69aa3073fc55334f2a2ed40e9899ca65ec169c5d288`.
The unchanged Vortex dependency is
`531fed6588d1b1b0492f1a50cad42179ce68faa5`, with the same qualified SPFresh SDK.

Native ON passed eight Vortex SQL cases / 355 assertions, including the new
16-assertion build/search/rebuild case, and Vortex restart recovery. ON and the
unchanged OFF binaries each passed 86 base SQL cases / 3,582 assertions with
13 optional-extension skips, plus Parquet recovery. OFF rejected the format
and exposed none of the six Vortex functions. All six prior process proofs
passed against the rebuilt native binaries and shared Rust library. The four
cost/projection cases passed all 36 queries with fixture recall 1.0; no query
latency or build-throughput improvement is claimed here.

Rust workspace build, all-target/all-feature Clippy with warnings denied,
Rust/C++/Python formatting, Python syntax, workflow YAML and diff checks passed.
The initial SQL fixture used a relative managed data path and hit the existing
publisher's canonical-path restriction; the fixture now configures an absolute
canonical path, with no alternate path handling added to the implementation.
