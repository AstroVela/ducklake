# Consume the qualified SPFresh query and build fixes

The native backend had two independently reproduced defects. A visibility or
payload filter could leave allowed rows in postings farther than the nearest
unfiltered head. SPFresh applied that head's distance cutoff before searching
those allowed postings, even when all heads fit inside the existing probe
budget. Its K-means builder also read uninitialized assignment counts and added
into uncleared center accumulators.

[Vortex #32](https://github.com/AstroVela/vortex/pull/32) removes that cutoff
only for a supplied allowed-row mask in the static bridge. It preserves the
unmasked path and the existing head, page and check budgets.
[Vortex #33](https://github.com/AstroVela/vortex/pull/33) initializes the counts
and clears the existing scratch centers before each initialization trial.
Neither changes the native format or ABI.

## Exact source closure

Pin the DuckLake Rust query crate, standalone qualification probe, SDK source
check and CI Vortex checkout to `9133de570e7ad634aea43beb1cbbd4ea559a5491`.
The workspace root `Cargo.lock` is authoritative for both Rust members; CI's
cache key now hashes that tracked file.

Pin the DuckDB Vortex wrapper to
`d266a922e58aee4f8ac7b57a38b09262f656ce36`
([wrapper #38](https://github.com/AstroVela/duckdb-vortex/pull/38)). Its ordinary and distributed Vane
workspaces both consume the same Vortex revision, including their lockfiles,
installed-wheel identity check and strict release-source check. The native
DuckLake configuration uses the ordinary wrapper; the distributed provider uses
the separate Vane workspace.

Build and sign the Vortex and DuckLake artifacts together. The DuckLake provider
wheel declares the exact new Vortex dependency wheel. Keep the installed Vane
base wheel and its engine source identity unchanged. The DuckLake native query
crate owns the qualified static SPFresh SDK; the wrapper's optional index
features stay disabled in this configuration.

The raw SPFresh source remains pinned to
`5893eb61ee3b18610b6b00f1939be7dae1af8904`, with Vortex's reviewed static patch.
The static LLVM OpenMP 18.1.8 and zstd archives retain their qualified identities.
There is one required source revision per component, without alternate pins,
compatibility branches, native retries or fallback paths.

## Scope and upstream proof

The frozen filtered-query proof reuses byte-identical native artifacts:
64 existing queries miss their expected results before the routing fix and all
64 pass after it. The native C++ regression also fails against the preceding
SDK and passes with the fix. The builder's seven workspace groups previously
reported 2,185 uninitialized-memory errors in 12 contexts; strict Memcheck
reports zero after the initialization fix, under both qualified OpenMP links.
Those proofs are upstream evidence; the installed-provider checks below qualify
this consumer revision separately.

The fixed canary gate and exploration-quality reporting remain unchanged.
A bounded regression does not establish arbitrary-data recall, production
throughput or long-run liveness. In particular, the preceding 60-second /
four-reader workload exceeded its existing 90-second drain deadline, and the
original 512-row / 8D silent stop remains unresolved.

## Installed-provider qualification

The new artifacts were signed and installed together in an isolated environment
with the byte-identical Vane `0.3.0.dev1` base wheel. That base reports engine
`v1.5.5-vane.e14bf92d40` and source ID `e957000277`. The DuckLake wheel requires
the exact new Vortex wheel version, and all probes run without an SDK-directory
environment override.

The five public flows passed 172 query-oracle comparisons: 32 basic, sixteen
write, sixteen source-retention, 28 keyed-write and eighty automatic-flow
comparisons. Their minimum recall was 0.9. The native checks passed seven write
recovery cases, 23 source cases, thirteen keyed cases and nine automatic cases,
including real SIGKILL recovery and old private views after source GC. All
thirteen standalone maintenance fault/budget cases, the controlled stall-capture
proof and ten SQL fixtures containing 260 assertion blocks passed.

All six automatic load combinations (512, 4,096 and 32,768 rows, each at 8D and
128D) completed 2,452 typed query-window checks. Fixed-canary and exploration
recall lower bounds were 1.0 in every combination; no stall capture was needed.
The unchanged Vane base release launcher passed 1,222 non-Ray and 32 Ray tests
in separate processes, with fifteen optional-dependency skips.

The separate 128D `cost` matrix remains qualified only in part. Its 4,096-row
case completed 283 consistency checks but failed the unchanged 0.8 canary gate
with a lower bound of 0.7. Its 32,768-row case completed 77 checks and passed
with a canary lower bound of 0.8. Exploration lower bounds were 1.0 in both.
This failure is retained, and broader ANN quality remains open. Input mutations
and native builds differ between runs, so these workload summaries are not a
controlled performance comparison; the frozen upstream input provides the
routing-specific old-fails/new-passes proof.

Both adapter workspaces resolve one Vortex source at the required revision.
The wrapper's source/wheel identity tests passed 66 tests and 26 subtests, and
its distributed Vane adapter passed an offline Rust 1.91 compile check against
the exact Vane headers. SDK source/patch stamps, formatting, syntax, YAML and
copyleft checks passed.
