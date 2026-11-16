# Native SPFresh construction and publication

This validation follows [DuckLake #36](https://github.com/AstroVela/ducklake/pull/36).
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

The next [background optimizer validation](native-vortex-optimizer-proof.md)
connects this primitive to explicit T/F thresholds and durable task recovery.

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
reference must not exist. The managed data path is expanded and resolved for
artifact containment. Physical source URI identities retain their captured
representation, including relative and `file:` forms. The function returns one
published revision and generation. Revision zero requires an absent name; replacement uses the exact
ready revision. No implicit catalog initialization, retry or format probing is
added. Default-OFF builds expose none of the native Vortex functions.

DuckDB file-access policy is checked through the context filesystem before
capture and at construction checkpoints. With `enable_external_access=false`, `allowed_directories` must
authorize the output root and its generated descendants. The reference filename
uses DuckDB's reserved-extension write guard.

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

The pinned SPFresh bridge serializes native build, open and search entrypoints
with a process-wide mutex. An ANN request reaching that core during a native
build waits for the call to release the mutex. DuckLake's publication and
metadata locks are released during heavy construction, permitting ordinary
commits and exact SQL work on other connections. The concurrent lifecycle
proof pauses at `build_started`, after source transfer and before acquiring
the native mutex. Continuous ANN throughput during the native core call
requires separate validation. The current proof establishes lifecycle progress
at that pre-call boundary.

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

The current source includes #36 at `7e9fc536` and pins Vortex to
`307eaf56644845f45dfe6099d7148e6b6afb17a5`. Native construction uses the
same opaque URI and verified-reader contracts as binary SQL search.

The native proof uses only the linked C++ driver for SQL build/search. It checks
initial publication, removal of the reference output, F-to-B promotion while
another connection updates/deletes/flushes and queries, revision-CAS competition,
cancelled/failed construction, process termination around native build and
catalog commit, and a lost acknowledgement. It removes canonical source files
after Rust ownership but before native building, requires construction to
finish from owned bytes, and rejects publication of that stale layout. Explicit
native rebuilding restores search against the rewritten files.

The native build proof also covers canonical, dot, relative, symlink and `file:`
managed paths. It verifies exact source URI binding, ranked query results and
managed-reference containment. Three historical inline schemas, swapped
category/payload names, quoted historical columns, cache eviction and a
concurrent rename during construction exercise stable field-ID resolution.
The cancellation matrix includes the caller checks before building, at ABI
input, after publication validation and immediately before commit.

```sh
python3 -I test/vane/test_native_vortex_build.py \
  --driver build/vortex-validation/ducklake_vortex_catalog_driver \
  --output-dir build/native-build-proof
```

The native ON suite adds a minimal SQL build/search/rebuild case. OFF checks
include the new function's absence. CI runs this proof alongside the existing
six lifecycle proofs and cost/projection gates and uploads raw evidence. Local
builds reuse DuckDB v1.5.5 objects qualified against compiler arguments, source
bytes and every recorded compiler dependency. They rebuild affected DuckLake
C++ sources and the Rust library using the unchanged pinned SPFresh SDK.
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

### Synchronized validation (2026-11-16, before permission correction)

The source includes #36 at `7e9fc536` and uses Vortex
`307eaf56644845f45dfe6099d7148e6b6afb17a5`. Native construction passes
131 requests with 28 expected rejections. Its 20 lifecycle recall checks and
five historical-field recall checks return 1.0 at the existing 0.8 threshold.
All five managed-path representations preserve exact source URI identities
and return the expected nearest row. Each rejects an output reference inside
the physical managed directory while retaining the ready reference and query
results.

Concurrent update/delete/flush and queries run at `build_started`, after source
ownership transfer and before the SPFresh build call. The previous ready
revision remains queryable, captured F becomes B after publication, and later
F/T remains visible. Twelve cancellation boundaries, three SIGKILL/restart
boundaries, competing builders, lost acknowledgement and actual reclamation
of four captured source files pass. Three historical inline schemas survive
native construction, cache eviction and a category/payload rename during a
second construction.

The first two runs exposed errors in the new test harness. A positional insert
after swapping column names populated the wrong logical field. The corrected
fixture names its columns and verifies their oracle values before building.
The second run read the flat SQL generation shape from a core response. That
assertion now reads `reference.generation.generation`. The complete corrected
proof passes in fresh fixtures. The original failed reports are retained.

The six prior lifecycle proofs pass. Native ON passes 17 Vortex SQL cases with
725 assertions. ON and OFF each pass 86 base cases with 3,582 assertions and
13 optional-extension skips. Deletion inlining passes 18 cases with 1,099
assertions and two optional-extension skips. ON Vortex and ON/OFF Parquet
restart recovery, cleanup, OFF format rejection and the absence of all six
Vortex functions pass. The actual query C ABI passes 32 cases covering invalid
inputs, source identity, cancellation, protected caller buffers and canonical
source removal.

Four projection/counter cases cover 4,096 and 32,768 B rows, 128 later F rows,
two T rows, and ALL/category B queries. All 36 queries have fixture recall 1.0,
zero retained native source buffers and zero source staging bytes. The
unchanged Vortex dependency's 144 tests were re-executed using its qualified
binary after matching its checksum and source revision. DuckLake workspace
build/test, all-target/all-feature Clippy with warnings denied, Rust/Python
formatting and diff checks pass.

The local ON/OFF builds use two compiler jobs. They reuse 359/358 objects after
matching compiler arguments, source bytes and every recorded compiler
header dependency against the previous qualified build. The remaining units,
query/probe, codec and native drivers were built and linked for this source.
The unchanged SPFresh SDK and Roaring inputs retain their recorded checksums.
The final audit binds 13,226 source/build inputs, including 441 ON and 431 OFF
compile units. Float32 tolerances remain relative `1e-5` and absolute `1e-4`,
with the existing row/source/vector/artifact and timeout budgets. CI validation
is deferred under the requested scope. Timings in the counter evidence are
informational under the current host-load constraint.

Evidence is retained under
`/tmp/ducklake-pr37-fix-validation-20261116`. The corrected construction report
is `native-build-final/report.json`, full gate results are `executions.json`,
C ABI results are `abi-final/report.json`, and source/build bindings are in
`final-input-audit.json` and `verified-input-sha256.json`. The construction
driver SHA-256 is
`8c82fb16c5d9d972e4e4920d25e52914c2650b5be465d8a9570da9240b908fba`.


### Output permission correction (2026-11-16)

Fresh-context Astra/max review of `f073b677` reproduced a P1. With external
access disabled and managed source reads authorized, native construction wrote
a reference and nine generation files into an unauthorized directory before
publication rejected its read. Exact `allowed_paths` entries for the output
root and reference also left generated descendants unauthorized.

Construction now checks the complete output namespace against DuckDB's
normalized `allowed_directories` when external access is disabled. The check
runs before source capture and after construction observers, including the
boundary before SPFresh creates its scratch and generation. The reference
filename uses the existing opener filesystem's reserved-extension write guard.
The ordinary external-access-enabled path and explicitly allowed output roots
pass the same construction and publication flow.

Four deterministic policy cases cover denied output, exact root/reference
paths, an allowed output directory and access revoked at `build_started`.
All denied output trees stay empty and preserve the prior ready descriptor.
The allowed-directory case publishes its successor and executes search. A
reserved `.duckdb_extension` reference also rejects before construction writes.
The original binary fails the new authorization regression and leaves the
prohibited files, providing a recorded pre-fix reproduction.

The first reserved-extension check was unconditional and rejected normal JSON
reference paths. The existing `IsDuckDBExtensionName` predicate now guards that
check, matching DuckDB's normal writer. A fresh normal SQL construction case
and the complete corrected proof pass. A private runner's driver path was also
corrected when its evidence moved into the post-fix subdirectory. Those initial
failures and corrections are retained alongside the successful rerun.

The complete post-fix proof passes 159 requests with 35 expected rejections,
25 lifecycle/historical recall checks at 1.0, five URI cases, the four access
policy cases, 12 cancellation boundaries, three SIGKILL/restart boundaries and
actual collection of four captured sources. The six prior lifecycle proofs,
17 Vortex SQL cases / 725 assertions, ON/OFF base suites (86 / 3,582 each),
deletion inlining (18 / 1,099), recovery/cleanup/OFF gates, 32 C ABI cases and
all four projection/counter cases / 36 queries pass again. Optional-extension
skip counts, Float32 tolerances, recall threshold and budgets retain their
recorded values. Rust workspace build/test and all-target/all-feature Clippy
also pass again.

The fix recompiles the catalog translation unit through its normal Ninja
object rule and recreates the DuckLake archive and all five ON executables
using the exact recorded CMake archive/link commands. The build rehashes
13,224 unchanged inputs before reuse. Rust, SDK, codec and OFF binaries retain
their previously qualified input bindings. The final source audit binds 13,226
inputs. Post-fix evidence is under
`/tmp/ducklake-pr37-fix-validation-20261116/access-fix`, including
`native-build-final/report.json`, `executions.json`, `abi-final/report.json`,
`validation-summary.json` and `complete.json`. The archive/link records are in
the parent directory's `access-fix-build-identity.json`. The corrected driver
SHA-256 is
`85807d2740a9acdaeec4bb5b7a8f654e683cf0a88bed74f3f7d6cbbb9146ca16`.


### Context filesystem dispatch correction (2026-11-16)

A second fresh-context Astra/max review of `b2f765ee` reproduced a P1 when
another connection disabled `LocalFileSystem` at `source_pinned` or
`build_started`. The external-access-enabled fast path skipped engine filesystem
dispatch, allowing ten output files before publication rejected access.

Every directory authorization checkpoint now first uses the current context
filesystem's read-only `DirectoryExists` operation. Its opener and virtual
filesystem enforce access settings and disabled filesystem registration before
the builder reaches native writes. Directory-wide authorization and the
reserved-reference filename guard continue through their existing checks.

Three additional regressions disable `LocalFileSystem` before the call, at
`source_pinned` and at `build_started` while external access stays enabled.
Each confirms ordinary COPY rejection, an empty output tree and the prior
ready descriptor. The previous binary fails the new source-pinned regression
with ten unauthorized files. The new context filesystem reports its standard
"Cannot access directory" error for denied roots, and the policy tests now
match the shared directory rejection while checking the same state assertions.

The complete corrected native proof passes 181 requests with 41 expected
rejections. All 25 recall checks remain 1.0. The five URI, four allowlist,
three disabled-filesystem, 12 cancellation and three crash cases pass.
All six earlier lifecycle proofs, 17 Vortex SQL cases / 725 assertions,
ON/OFF base suites (86 / 3,582 each), deletion inlining (18 / 1,099),
recovery/cleanup/OFF gates, 32 C ABI cases and four counter cases / 36 queries
pass again. Workspace build/test, all-target/all-feature Clippy, formatting
and source/build identity checks pass. The same recorded compile/link
qualification applies to the updated catalog object and five ON programs.

Current complete evidence is under
`/tmp/ducklake-pr37-fix-validation-20261116/filesystem-fix`, including
`native-build-final/report.json`, `executions.json`, `abi-final/report.json`,
`validation-summary.json` and `complete.json`. The pre-fix reproduction and
focused green cases are in the parent directory's `filesystem-fix-red` and
`filesystem-fix-green`. The current catalog driver SHA-256 is
`8b684c43531c3bbc79cb4dc43f8dd7aef95656c4582f37dfbc535f6cc38f1d6a`.
