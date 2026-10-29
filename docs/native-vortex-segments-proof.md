# Independent native Vortex B segments

This validation draft follows [DuckLake #40](https://github.com/AstroVela/ducklake/pull/40)
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

The #41 local qualification used DuckDB v1.5.5 (`d8cdaa33`), Vortex `65dab44f` and the
native wrapper `b819cc03`, with the dependency pins unchanged from #40. The
segment proof passed all ten checks, including 22 fixture queries with recall
1.0. The final catalog driver SHA-256 was
`4a29d5e28c3d7d7596feba6177c61780d8ab213854a91b680eaefa414c817d22`.
ON passed 12 Vortex SQL cases (460 assertions) and Vortex crash recovery.
ON/OFF each passed 86 base cases (3,582 assertions; 13 optional-dependency
skips) and Parquet crash recovery; the audit case passed 122 assertions.
Existing native index, read-view, mixed, admission, catalog, search, build,
optimizer, lifecycle and write-budget proofs passed. The isolated cost proof
passed four cases with nine queries each; timings remain informational.
`make format-fix`, Rust formatting and workspace Clippy with warnings denied
completed successfully.
