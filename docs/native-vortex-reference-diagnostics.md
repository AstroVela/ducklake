# Transaction-scoped source-reference diagnostics

The [repeated metadata diagnostic](native-vortex-source-gc-metadata.md) found
36.763 ms outside SQL in an 84.165-ms large-input collector mean. That envelope
does not identify a C++ cause. This study measures the existing
`HasFileReference` body, its result Fetch and its path-processing loop in a
private diagnostic executable. No production reference algorithm, guards,
transaction lifetime, COMMIT order, policy, budget or installed provider changes.

## Protocol frozen before formal sampling

Qualification amendment, also committed before replacement sampling: require
the default six base extensions and only the two explicitly linked native
providers. Set `DISABLE_EXTENSIONS_FOR_TEST=1` and explicit
`DUCKDB_DUCKLAKE_DIRECTORY` / `DUCKDB_VORTEX_DIRECTORY` when configuring or
reinstalling the runtime, as in
[the CI workflow](../.github/workflows/VaneNativeVortex.yml). Reject cached
provider source paths, extra/missing extension archives and an unexpected
runtime static-extension set before collecting a diagnostic.

An earlier 128-call collection passed native semantic/trace checks, but its
subsequent base qualification found an unintended `tpch` extension. Its raw
report is retained as **configuration-invalid**, excluded from qualified results:
`/tmp/ducklake-vortex-reference-formal-20261106/report.json`, SHA-256
`e1035583265780a5c1424d27658c7722b84011696ded32a4b0d0ef7e07f085ee`.
The cause was the missing CI environment flag, not a timing/precision result.
After fixing and checking that configuration, run one fixed replacement with
the same two inputs, CPUs, threads, budgets, ordering and sixteen rounds below.
Do not pool the two collections, change counts or use the rejected timings to
select another criterion. No qualified timing result is claimed yet.

Commit this protocol, private builder, patch, trace header and analyzer before
the one formal run. Reuse CPUs **6,7,8,9,10,11**, DuckDB `SET threads=2`, existing
SPFresh settings and inherited thread environment. Preserve exact-affinity,
physical-core/NUMA validation, child inheritance and finally restoration.
The set reserves no cores or memory and changes no governor or frequency.

Use the original twelve-case sealed corpus, SHA-256
`48855d42a60fd423caf0df052e46bb95050003e61f0ac83b570f03be3f5cf042`, and its original
producer executable, SHA-256
`168562ccee3d4cc66659d4bb8b18a5b891a33ecbdd2424f99736a22e0ec0f400`.
Select the same two file-unlink inputs: 8-inline and 4,096-materialized.
Run **16 rounds per case, four diagnostics per round: 128 measured calls and
eight excluded diagnostic warmups**. Preserve ABBA/BAAB labels and the existing
case order. Both labels run the same instrumented executable. Counts are fixed;
there is no timing gate, adaptive increase, primary sample or A/A verdict.
Stop for execution, semantic, trace or integrity failure.

Before sampling, run one four-case semantic oracle per selected input, outside
collector statistics: a live source is referenced, a `./` metadata-path alias
still resolves to the same canonical source, an absent source is unreferenced,
and a missing metadata table raises its original error. Roll back the oracle's
metadata edits and verify unchanged logical state and retained data/index bytes.
The ordinary-transaction deferral and full native GC checks still run on every
collector invocation. Keep the two-object/64-MiB budget and keep-two/min-age-zero
policy. Every call restores fresh single-link images at original canonical paths.

The functional smoke uses two history-8 file inputs, two rounds, sixteen measured
diagnostics, eight warmups and two four-case oracles. It has no timing claim.
CI uses one explicit available CPU; formal uses 6–11 with unchanged threads.

## Private build and provenance

The [builder](../test/vane/build_vortex_reference_diagnostic.py) requires exact
production metadata-source SHA-256
`97a4b1cf7d1a00c7846a974fee77647649c8b18c7ae6ae254097b9d474ffe96e`.
It applies [one test patch](../test/vane/reference_diagnostic.patch) to a private
copy with zero fuzz, compiles it with the configured metadata flags and links
that complete object explicitly before the unchanged extension archives.
The benchmark uses its configured flags plus the diagnostic-only definition.
Source/patch/header, compile commands, explicit object order, archive hashes and
executable hash are retained. No provider source/archive is patched or replaced.
The normal benchmark excludes the additional code at preprocessing time.

The [runner](../test/vane/test_vane_vortex_source_gc_reference.py) verifies the
original producer, sealed report/images, private build and input archive hashes.
It writes a **separate consumer declaration**, retaining original producer/report
hashes and canonical paths, to identify the instrumented executable consuming
those images. The sealed input report is never rewritten. An artifact copy of
the declaration is retained in the result directory. The unchanged #81 metadata
sampler restores and validates each call, then this study validates and analyzes
all added reference scopes. Native guards reject incomplete captures per call.

This build has instrumentation and an explicit metadata object, unlike #81.
No timing subtraction across those binaries or a claim of performance improvement
is valid. A later runtime A/B still needs matched explicit-object placement and
compiler flags on both sides. Builds/tests do not overlap formal sampling.

## Declared accounting

Record four reference-call scopes within the native collector, each matched to
exactly one contained known-source SQL query on the same metadata connection.
Scopes are disjoint; each deleted source is checked twice in its existing
transactions. Verify false/completed results and full row coverage against the
data-file plus delete-file inventory. Each unreferenced call has nonempty chunks
and one terminal empty Fetch. Preserve exact query and checkpoint checks from #81.

Fetch is timed per result chunk, including the terminal empty call. The **path
loop** has one timer per nonempty chunk and includes Value/string extraction,
`CanonicalizePath`, comparison and diagnostic overhead. No per-row clock is
inserted. This does not separately measure pure canonicalization, CPU, I/O or
scheduler wait. The call scope retains result/chunk destruction; bookkeeping
outside the scope remains in the collector residual.

Keep disjoint outer SQL categories unchanged and split their `outside_sql` into
reference Fetch, reference path loop, reference remainder and outside-reference
time. Reference remainder is its measured call duration minus its contained SQL,
Fetch and path loop; outside-reference is the rest of the original collector
residual. Require every part nonnegative and their sum equal to collector time.
Do not add whole reference calls, SQL and native metadata phases together.

Report counts, mean/p50/nearest-rank-p95/min/max, rows/chunks/Fetch calls, exact
SQL/planning splits, label summaries and round means. Retain all tail samples.
Signed covariance shares remain descriptive and coupled to total time, not causal
variance explained; constant total variance remains undefined. Earlier primary
and A/A decisions stay unchanged. R4 remains open, without a new 30-minute or
sustained-write stability claim.

## Reproduce

```sh
python -I test/vane/build_vortex_reference_diagnostic.py \
    --build-dir /tmp/vane-native-vortex-runtime-20261030/build/python-release \
    --output-dir /tmp/source-reference-build
python -I test/vane/test_vane_vortex_source_gc_reference_stats.py
python -I test/vane/test_vane_vortex_source_gc_reference.py \
    --producer-driver /tmp/ducklake-vortex-expiry-ab-baseline-20261106 \
    --build-report /tmp/source-reference-build/build.json \
    --inputs /tmp/ducklake-vortex-source-gc-full-v3-20261106/report.json \
    --cpus 6,7,8,9,10,11 --profile formal \
    --output-dir /tmp/source-reference-formal
```

Build and output directories must be new. Preserve the build manifest, consumer
declaration, raw native JSON/logs, shared sampling report and final analysis.

## Result

Formal sampling is pending. Retain current production checks and budgets; choose
a runtime candidate only after the new measurements identify its target.
