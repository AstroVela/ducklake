# Retained native Vortex read views

This validation draft extends the [native Vortex index proof](native-vortex-index-proof.md)
with an explicit read-view lifetime. It is stacked on
[DuckLake #28](https://github.com/AstroVela/ducklake/pull/28) and reuses its exact
Vortex and SPFresh revisions. The implementation remains a test-only native
Rust bridge; source writes and maintenance use separate DuckLake CLI processes.

## Owned read state

Opening a view verifies its immutable capture and index reference. The capture
binds the DuckLake snapshot, table UUID, field IDs, physical file inventory,
content digests and visible addresses. It also names the business-field IDs
needed for the proof's BIGINT ID and nullable VARCHAR category/payload columns.
Older captures remain valid for the one-shot proof; retained views require the
business projection.

The view owns these resources until close:

- Vortex `LocalFileSource` instances holding verified encoded file bytes.
- A Vortex SPFresh index opened from a verified private artifact lease.
- The captured visibility mask and physical-to-business field mapping.

`LocalIndexStore::prepare_open` authenticates metadata before artifact-budget
admission. `materialize` verifies and copies the generation once, and the
existing provider takes ownership of that verified lease. Native index handles
are dropped before their private directories. Search reuses the existing
provider and its visibility-before-top-k contract.

Both vector and business-column take use the pinned Vortex bytes. They preserve
rank, duplicate addresses, NULLs and Unicode strings. An already-open view does
not reopen canonical sources or fetch business fields through a new DuckLake
`latest` lookup. Every request supplies the expected capture digest; a different
digest or an invisible address is rejected before search or fetch.

This is an in-memory source-view lease, not a held DuckLake catalog transaction
or a catalog GC registration. The fixture completes capture/open before releasing
its writer barrier. Once opened, the reader is independent of canonical source
and index paths, including after actual DuckLake snapshot expiration and file
cleanup. Concurrent capture/admission against live catalog maintenance and an
out-of-core source-view lease remain separate integration gates.

## Admission and release

The test worker explicitly selects retained mode with `serve ROOT LIMITS_JSON`.
Its serial request protocol supports open, search, ordered business fetch,
close, statistics and shutdown. One-shot build/search retain their existing
strict source and generation verification behavior.

The owner configures maximum active views, total retained encoded source bytes,
and total retained artifact bytes. Individual sources retain the original
64 MiB bound. The proof uses three views, a 64 MiB aggregate source allowance and
a 128 MiB artifact allowance; dedicated workers exercise tighter limits.
Rejected opens leave existing views, byte accounting and private directories
unchanged. Normal close, shutdown and stdin EOF release handles and reservations.

These byte counters describe retained inputs, not total RSS or peak memory.
Decoding, masks, result strings, native graphs/workspaces and open-time staging
also allocate memory. Scratch roots are owner-managed; process crashes can leave
private copies for owner cleanup. This draft does not qualify crash reclamation
or a concurrent query scheduler.

## Validation and reproduction

Build the existing probe using the [index-proof instructions](native-vortex-index-proof.md#build-and-run),
then run the process harness with a fresh output directory:

```sh
python3 -I test/vane/test_native_vortex_read_view.py \
  --duckdb build/vortex-validation/duckdb \
  --probe build/index-cargo/ci/ducklake-vortex-index-probe \
  --output-dir build/read-view-proof
```

The fixture uses 4,096 eight-dimensional vectors, multiple physical files and
nullable/Unicode business fields. It retains the reader while separate native
DuckLake processes commit updates/deletes and an inline insert, flush, rewrite,
merge, expire old snapshots and clean up retired files. It also removes the
canonical index generation after opening it.

The proof verifies:

- Old vectors, predicates and business rows remain unchanged, including an
  ordered duplicate fetch delayed until after maintenance.
- Previously unqueried vectors and business rows can be fetched after their
  canonical files are gone; the worker is not replaying saved query results.
- New views reject stale generations and return updated versions after flush
  and a qualified rebuild. Inline rows are explicitly rejected by a fresh
  file-only capture until the separate mixed-query path exists.
- Count/source/artifact limits reject a second view without altering the first.
- Canonical reference, manifest and late-artifact corruption cannot alter an
  existing view, and failed reopens leave no private copies or reservations.
- Closed/wrong-view/invisible addresses fail explicitly; closing one view leaves
  other views usable. Budgets can be reused and EOF closes outstanding leases.
- Legacy one-shot captures still work for vector search; retained mode explicitly
  requires the additional business-field mapping.

The existing index process proof is rerun against the same binary. CI runs both
proofs after the ON/default-OFF native gates and uploads per-process logs and
JSON reports. The local run reuses the #27 native shell and qualified pinned
SPFresh native libraries; the Rust bridge is rebuilt from locked dependencies.

This does not add a production SQL index API, cross-snapshot index-coverage
reuse, automatic catalog index publication, mixed ANN/unindexed/inlined top-k,
cloud storage or Ray execution. The next mixed-query proof can use this explicit
read-view lifetime to keep branch results and ranked fetch on the same source.

The follow-up [mixed top-k proof](native-vortex-mixed-proof.md) implements that
explicit B/F/T validation path while retaining this snapshot-bound proof.
Its worker configuration also requires inlined row/byte limits; snapshot-only
views consume zero from those counters.
