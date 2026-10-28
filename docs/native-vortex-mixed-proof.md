# Native Vortex mixed top-k proof

This validation draft extends the [retained read-view proof](native-vortex-read-view-proof.md)
and is stacked on [DuckLake #29](https://github.com/AstroVela/ducklake/pull/29).
It reuses Vortex `feat/vortex-index` at
`ee085f55a705cb9b2edab18d392dd822540774d7`, including its existing SPFresh
builder, filtered ANN provider and authenticated local artifact store. All new
code lives in the test-only bridge and process harness.

## File-bound index and query view

`build-file-index CAPTURE REFERENCE` explicitly builds a generation over every
physical row of the captured immutable files. Its source snapshot identifies
the table UUID, indexed field ID, Float32 dimension, squared L2 metric and exact
ordered file inventory: file IDs, URIs, content digests and physical row counts.
Its visibility is all physical rows. DuckLake snapshot numbers, delete masks
and unrelated schema changes are not part of this physical index source.

The mixed query capture separately names a DuckLake snapshot, its complete file
inventory and visible physical addresses, business field IDs and visible
committed inlined business rows. Every referenced index file must occur with
exactly the same identity in that query view. Missing, replaced or changed B
files reject the open. The existing Vortex metadata snapshot check still uses
exact equality against the original physical-file snapshot; it is not relaxed.

The file-bound reference is a distinct, explicitly selected contract. The
snapshot-bound build/search commands retain their original exact capture
binding. Neither mode tries another reference format on error.

## Three declared branches

The planner forms a complete, disjoint file partition before querying:

| Branch | Source | Retrieval |
| --- | --- | --- |
| B | Files named by the verified index reference | Existing SPFresh ANN |
| F | Query-view files outside B | Exact vector scan |
| T | Visible committed inlined rows in the query capture | Exact vector scan |

Current DuckLake visibility and the query's captured business-predicate allow
list are applied before each branch's top-k. B's RowFilter is constructed in
the unchanged physical index snapshot, after checking that its files still
belong to the query view. An updated B row's old physical address is excluded;
its new visible version can occur in F or T. There is no post-search logical-ID
deduplication to mask stale versions.

Each branch contributes at most k candidates, with `1 <= k <= 64`. F and T use
bounded heaps while scanning. B candidates are rescored from their pinned
vectors using the same Float32 squared L2 calculation. The final heap merges
at most 3k candidates, ordered by `(distance, typed row address)`. Non-finite
query components, dimension mismatches and distance overflow are errors.

SPFresh remains approximate. A sparse predicate can leave it with fewer than k
hits, including zero for a far allowed row. Visibility/filter correctness is
mandatory, while ANN recall is measured separately against the exact fixture
oracle with a 0.8 threshold. There is no scan retry for ANN underfill or errors.
The report also records B-only fixture recall and an informational far,
single-row ANN predicate probe, whose recall is allowed to be zero.

F is selected because those files have no index in the declared plan. An index
open/search failure never moves B files to F. The bridge has no version
negotiation, compatibility registry, automatic rebuild or error-to-scan path.

## Retained ownership and limits

The `serve` worker adds explicit `open_mixed`, `search_mixed` and `fetch_mixed`
commands. Snapshot and mixed views require their corresponding command family;
requests carry the exact view capture digest. The view owns verified encoded
B/F source bytes, a private authenticated SPFresh artifact lease, its captured
visibility and copied T business rows until close. Both branch retrieval and
ranked business fetch use this same view.

Inlined addresses are `{kind: inline, source, row_id}` using DuckLake's inlined
source name and committed row ID. They never fabricate a file ID. File addresses
are `{kind: file, file_id, row_offset}`. Fetch preserves ordering, duplicate
addresses, nullable business fields and Unicode across the two address kinds.

The worker explicitly requires five owner limits: active views, retained encoded
source bytes, artifact bytes, inlined row count and serialized inlined bytes.
Empty T consumes zero inlined bytes. Opens account for aggregate retained inputs
and reject exceeded budgets without changing existing views. The original
64 MiB per-source bound remains; mixed captures are bounded to 64 MiB as well.
Close, shutdown and EOF release all reservations and private artifact leases.
These counters are not RSS or peak-memory limits: decoding, masks, strings,
native graphs and open-time staging also allocate memory.

Capture/open completes behind the process fixture's writer barrier. This is
still an in-memory retained view, without a held catalog transaction or a GC
registration. Concurrent capture/admission during maintenance and out-of-core
source ownership remain integration work.

The follow-up [native admission proof](native-vortex-admission-proof.md) captures
that view through one native transaction and verifies concurrent writes and
physical cleanup through an explicit self-contained source handoff.

## Validation and reproduction

Build the probe with the [index-proof instructions](native-vortex-index-proof.md#build-and-run),
then run with a new evidence directory:

```sh
python3 -I test/vane/test_native_vortex_mixed.py \
  --duckdb build/vortex-validation/duckdb \
  --probe build/index-cargo/ci/ducklake-vortex-index-probe \
  --output-dir build/mixed-proof
```

The native fixture builds one generation over two B files containing 4,096
vectors. Separate DuckLake CLI processes add a 128-row F file and commit
updates/deletes in B, F and T. T and F provide the two closest live neighbors;
deleted and superseded neighbors must never appear. The proof also checks:

- Immutable reference/artifact hashes across newer snapshots and T-to-F flush.
- Single-row prefilters in each branch, a business-category predicate, an empty
  allow list, squared L2 scores and a bounded sorted merge. The B predicate row
  is in the searched ANN neighborhood; arbitrary sparse recall is not assumed.
- Query/view/field/file/metric/inline/artifact failures without rerouting or
  leaked reservations, plus aggregate inline row/byte admission and release.
- T-to-F flush preserves logical top-k and leaves the earlier T view intact.
- Rewritten B files reject the old reference explicitly. Already-open views
  keep searching and fetching previously unqueried B/F/T rows after actual
  snapshot expiration, source-file cleanup and canonical generation removal.

CI runs this proof and the existing snapshot-index and retained-view proofs
against the same rebuilt binary after the native ON/default-OFF gates. Logs,
route counters and JSON reports are uploaded as validation evidence. This is
not a production SQL index API, automatic catalog index publication, a new ANN
engine, a source WAL, a cloud/Ray path or a throughput/latency benchmark.

Local validation reused the qualified #27 native shell and the pinned SPFresh
libraries, rebuilding the probe from its locked dependencies. The mixed proof
passed 51 SQL/probe processes and 77 worker requests, including 24 expected
rejections. Seven merged fixture queries and three B-only queries had recall
1.0; the separate far single-row predicate probe returned zero hits. Actual
cleanup removed all three captured B/F source files before retained queries
and delayed fetch were rechecked.

The same binary passed the snapshot-index regression (322 processes, 18 expected
rejections, 11 nonempty recalls of 1.0) and retained-view regression (54 SQL/probe
processes, 80 worker requests, 16 expected rejections). Rust build, Clippy with
warnings denied, rustfmt, repository `make format-fix`, workflow YAML parsing and
whitespace checks passed. Hosted native/index validation is tracked separately
by the Draft PR; these local fixture results do not establish production recall
or performance.
