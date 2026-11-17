# Independent native Vortex B segments

This proof follows [DuckLake #40](https://github.com/AstroVela/ducklake/pull/40)
and the [original design](https://chatgpt.com/space/page_5ee0f8c698d08191a17198d45d152362).
After bootstrap creates B1, the next F promotion builds B2 independently. B1's
generation, descriptor, artifact contents and modification times stay unchanged.
It reuses the pinned vortex-index and static SPFresh backend; it does not call
SPFresh's native online update interfaces.

## Construction and publication

```sql
SELECT * FROM ducklake_append_vortex_index_segment(
    'lake', 'main', 'items', 'knn', '/absolute/existing/root/B2.json', 8, 1);
```

The last two arguments are dimension and expected catalog revision. Capture
checks the entire physical inventory against the existing global limits, then
copies and decodes only files outside current B coverage. The result is a sealed
ordinary segment reference covering only that F snapshot. Empty increments,
stale revisions, overlapping file IDs/paths, duplicate generations and changed
table/field/source identities fail explicitly. The existing build configuration
uses 64 heads and requires strictly more covered rows, so new F must contain at
least 65 physical rows. Smaller segments fail without padding or a full rebuild.
Newer F and T stay queryable.

`ducklake_publish_vortex_index_segment` accepts an already sealed reference and
expected revision. Publication verifies all referenced generations and source
identities, then atomically compare-and-swaps the whole segment set in one
metadata transaction. Publication still reads B1's encoded source and artifact
bytes for identity checking. Only F is decoded and indexed. Heavy construction
holds no metadata transaction or publication lock. Concurrent publishers have
one winner.

Catalog references now use a strict version-2 manifest with a list of
`{root, reference}` segments. Each embedded reference retains the existing
version-1 file-index format. The catalog root names the newest segment's root.
Old catalog reference values are rejected; no migration or alternate decoder
is added. Explicit full build/publication remains an operator-requested
replacement that consolidates to one segment.

The background optimizer bootstraps once, then uses the append path. The #41
qualification bounded the catalog to two B segments. The
[selective merge follow-up](native-vortex-segment-merge-proof.md) extends that
bound to three and schedules a separate two-smallest-B merge when full and F
is due. Direct append at the limit returns `RESOURCE_EXHAUSTED`. Acknowledged
B/F/T stays queryable, and durable write quotas remain active. Explicit full
consolidation remains an operator-requested replacement.

## One retained view and aggregate budgets

One ReadView owns both generation leases, the same visibility and business
predicate snapshot, remaining F, and committed T. Each B segment applies its
own row-ordinal visibility/predicate filter before ANN top-k. Its candidates are
reranked and merged into the same bounded global top-k with F/T scan results.
ANN and rerank timing closes at each segment boundary and accumulates repeated
phases. Existing query-wide cancellation checkpoints remain in place. Controlled
observer delays check that each phase includes both segments' work using measured
duration lower bounds, without requiring stable-load speed measurements.

Typed fetches use the same retained view. Segment sources are disjoint, so a
captured row cannot enter both B paths or B and F.

The existing 100,000 physical-row and 64 MiB vector/source/artifact ceilings are
aggregate limits, not multiplied per segment. The catalog manifest is bounded
at 1 MiB. Opening the second segment charges the remaining artifact budget. A
failed/cancelled open or search releases all acquired leases and reservations.
Logical/encoded limits do not claim to bound process RSS or SPFresh threads.

Publication does not invalidate an existing view. That view owns both artifact
generations and retained sources until close, even after source rewrite,
snapshot expiration, actual file GC and removal of the original artifact root.
A fresh view still validates current sources and artifacts; missing files fail.

## Qualification

```sh
python3 -I test/vane/test_native_vortex_segments.py \
    --duckdb build/index-shell/duckdb \
    --driver build/index-shell/ducklake_vortex_catalog_driver \
    --probe build/index-cargo/ci/ducklake-vortex-index-probe \
    --output-dir build/index-logs/segments
```

The process proof checks B1 checksums and nanosecond modification times while
B2 covers only the new 128-row F. It compares B1+B2+F+T SQL and core queries,
visibility, category filtering and top-k with a visible-row oracle. It injects
cancellation and failures during capture, sealing, publication and multi-lease
query work; races publication CAS; corrupts artifacts; and SIGKILLs on both
sides of commit. Reopen sees the complete old or new manifest. A committed B2
is not rebuilt, and an uncommitted candidate is never silently replayed.

The proof also holds a native SQL query and an independent retained view across
replacement, real source GC and removal of both original generations, then
queries and fetches from those old views. Aggregate artifact and physical-row
limits, background B2/B3 promotion, explicit segment-count exhaustion, persistent write
quotas and operator consolidation have separate assertions. Strict database
shutdown and empty private lease directories are required. Minimal SQL cases,
the existing process proofs and ON/OFF SQL/recovery suites run in CI.

The #41 local Linux qualification on 2026-11-17 used functional source `53a7db01`,
DuckDB v1.5.5 (`d8cdaa33`), Vortex `b20e34c2`, codec `84cef204`, and SPFresh
`5893eb61` with the pinned Vortex static-only patch. The final catalog driver
SHA-256 is `6967e52295f0091f0c5a3287039a198cc96fa9a72b1e8b56110e4a249a6acf52`.

The segment proof passed all eleven checks in 11.3 seconds under the unchanged
180-second proof budget, including 24 fixture recall checks at 1.0. It retained
B1's descriptor, generation, artifact checksums and modification times. Both
controlled phase checks included the measured delays of both segments. Single
segment test consumers continue comparing complete descriptors, and concurrent
flush checks require the expected one/two segments and exactly 256 covered rows.

ON passed 21 Vortex SQL cases / 833 assertions. ON and OFF each passed 86 base
cases / 3,582 assertions with 13 optional-dependency skips. ON deletion inlining
passed 18 cases / 1,099 assertions with two optional-dependency skips; audit
passed 122 assertions. Vortex/Parquet recovery, cleanup and all existing native
index/read-view/mixed/admission/catalog/search/build/optimizer/lifecycle and
13-case write-budget proofs passed. The 32-case FFI ABI matrix passed. Lifecycle
covered 20 query cycles and 18 same-process reopens. All measured fixture
recalls were 1.0, preserving the 0.8 threshold and existing Float32 tolerances.

Seventeen ordinary functional gates retained 180-second budgets. The separate
counter/recall gate retained its existing 600-second outer budget and completed
in 11.7 seconds with four cases / 36 queries; timings remain informational.
Scoped pinned C++/Python formatting, query/probe Rust formatting, Clippy with
warnings denied and builds passed. The Rust workspace has no unit tests; the
native and ABI proofs provide behavioral coverage. Unchanged Vortex inputs
reuse the hash-qualified #40/#39 results: 310 I/O/file tests, 164 index/SPFresh
tests and three index doctests, with one pre-existing ignored I/O doctest.

The initial phase-attribution failure and its original products were retained
as controls. Existing transaction guards, threads, source/artifact capacities
and operation deadlines were preserved. CI, Windows and stable-load performance
measurements were outside this local qualification.
