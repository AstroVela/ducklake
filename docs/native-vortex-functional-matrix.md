# Deterministic dimensional and query contracts

The machine's variable resource load defers R4 measurement work: identical-binary
A/A, optimization A/B and sustained-load qualification. Their earlier failed or
withdrawn results remain unchanged. This R5 slice uses functional assertions
with no latency, throughput, drain-duration or resource-steady-state gate.

## Exact floating write path

The client model exposed a write-path error before ANN: an exactly representable
float32 component `-0.499725341796875` came back as `-0.4997253715991974` after
inline commit. A separate 32-row native table reproduced five changed FLOAT
values (also inside lists) and one changed DOUBLE value; for example,
`0.9397519445337031` became `0.9397519445337033`. Bound parameters retained the
correct inputs. Bare decimal literals took a DECIMAL conversion before the
floating column, introducing another rounding.

The [inline serializer](../src/common/ducklake_util.cpp) now always emits an
explicit cast from each floating value's round-trippable string to its actual
metadata type. Nested lists recurse through that same serializer. This keeps
existing signed-zero/nonfinite handling and adds no alternate path or retry.
It corrects subsequent serialization; already rounded stored values are not
reconstructed from request receipts.

The [native SQL regression](../test/sql/data_inlining/float_exact_roundtrip.test)
compares scalar FLOAT/DOUBLE, lists, structs and maps with independently
typed expected values. It includes subnormal and maximum finite values, signed
zero, nonfinite values and NULLs. Its eleven assertion blocks check committed
inline rows, flushed rows and reopened metadata. The vector mutation API retains
its existing rejection of nonfinite vector components.

## Dense squared-L2 accumulation

After the write fix, dimensions 1/8/128 passed all phases with recall 1.0.
At 4,096 dimensions the strict distance oracle found five mismatches among 64
T hits. For example, `65.00198364257812` was returned for an exact sum of
`65.00390700972639` (relative error about 2.96e-5). Stored vectors still exactly
matched the client model. The shared B/F/T reranker accumulated each term in
float32, losing precision across many dimensions.

The [shared core](../src/vortex_query/src/mixed.rs) now subtracts and accumulates
in float64, then converts the final distance to the existing float32 result.
The same finite-result/overflow rejection stays after that conversion. Input
vectors, heap scores, public types, ANN candidate collection and budgets are
unchanged.

A separate native zero-B regression uses the 4,096-component vector
`[4096,1,...,1]` and its reversal against zero. Both exact sums are
`2^24 + 4095`, whose float32 result is **16,781,312**. The old accumulator lost
the small terms in one order, returning **16,777,216**. The fixture requires the
same correctly rounded result for both orders, diagnostic/ordinary parity,
explicit rejection of an overflowing finite query and a successful next query.
It was verified to fail on the float-write-fixed, old-accumulator provider.

## Fixed matrix

The [installed-provider test](../test/vane/test_vane_vortex_functional_matrix.py)
uses dimensions **1, 8, 128 and 4,096**, `k=1/64`, and six category choices:
unfiltered, `A`, lowercase `a`, empty string, quoted Unicode and absent category.
Vectors are finite, exactly representable float32 values; high dimensions include
nonzero components beyond the first eight. They form a bounded, deterministic
fixture, not a representative corpus for general ANN quality or capacity claims.

Each dimension has nine explicit phases:

1. Empty zero-B definition.
2. Sixty-four committed T rows, queried without waiting for ANN.
3. The same 64 rows flushed to F, still without ANN.
4. A 65th row and first immutable B generation.
5. Sixty-five more rows published as independent B2, preserving B1 bytes.
6. B1+B2+four unindexed F rows+three T rows.
7. Atomic upserts and deletes touching B1/B2/F/T while an ordinary native
   transaction retains its old typed rows.
8. Every earlier request replayed with its original receipt; no resurrection or
   deletion of newer versions.
9. Reopened installed-provider connection with the same committed state.

Each phase checks all twelve category/k combinations through both named search
and its actual-ReadView diagnostic entry point: **432 ordinary and 432 diagnostic
queries** overall. The independent client mutation model must equal all native
typed rows before each phase. Every hit must match that model's visible version,
dimension, filter and independently computed squared L2, with ranked distances
and unique business keys. Zero-B results require exact top-k; B results keep the
existing fixture recall >=0.8 requirement. Diagnostic hits must equal ordinary
hits and use the expected committed snapshot. Check actual B/F/T routes, unchanged
generation bytes, and release of private query staging.

The SQL regression, dense-L2 regression and each dimension execute in their own
sequential Python processes with inherited CPU/thread settings. This isolates
native runtime state between independent fixtures; each dimension still tests
connection reopen. One early combined process hit duplicate OpenMP initialization
when providers loaded from different fixture caches. No duplicate-runtime override
was enabled; that partial run does not qualify the full matrix. Per-case reports
are checkpointed so an abnormal child exit retains completed phases.

Each dimension also rejects ten malformed queries and four invalid mutations:
k 0/65, empty/4,097/mismatched dimensions, nonfinite/NULL components, oversized and
NULL category arguments; invalid mutation vectors, operation counts and bytes.
The **56 rejections** must preserve typed data, and invalid mutations must leave
no receipt. No input is padded to qualify for ANN and no budget is raised.

This retains `SET threads=2`, existing SPFresh settings and thread variables,
native transaction protections and hard budgets. Local runs inherit CPU **6–11**;
CI explicitly selects an available CPU. Affinity is recorded and restored, and
does not require idle or exclusive cores. Source/ANN maintenance is driven by
explicit operations, not timers or a background convergence deadline.

The [oracle checks](../test/vane/test_vane_vortex_functional_oracle.py) verify
float32 fixture representation and reject incorrect visibility, stale typed
versions, distances, filtering, duplicates, ordering, missing exact hits and
ANN recall below the existing threshold.

## Reproduce

Use a non-editable installed Vane with the matching DuckLake/Vortex providers
and a fresh output directory:

```sh
VANE_RUNNER=local-fast python -I test/vane/test_vane_vortex_functional_oracle.py
VANE_RUNNER=local-fast python -I test/vane/test_vane_vortex_functional_matrix.py \
    --cpus 6,7,8,9,10,11 --output-dir /tmp/vortex-functional-matrix
```

The report retains native identity, CPU/thread settings, each actual snapshot,
route, ranked IDs and recall. Elapsed time is observational and never controls
acceptance. Failure stops the run and preserves partial evidence.

After changing C++, rebuild/reinstall the matching runtime non-editably using
the incremental Release directory and package the matching providers as in the
[development workflow](vane-native-vortex-proof.md). Keep the frozen earlier
trial environments and evidence separate from the corrected provider closure.

## Result

The corrected, isolated full run passed all **36 phases**, **432 ordinary plus
432 diagnostic searches**, and **56 malformed-query/mutation rejections**.
Every dimension's minimum fixture recall is **1.0**, against the unchanged
>=0.8 ANN gate; zero-B results remain exact. Typed client-model equality, actual
ReadView snapshots, B1 preservation, B/F/T composition, ordinary native reader
isolation, old receipts and reopened state all passed.

| Dimension | Phases | Ordinary + diagnostic | Rejections | Minimum recall | Final logical rows |
| --- | --- | --- | --- | --- | --- |
| 1 | 9 | 108 + 108 | 14 | 1.0 | 134 |
| 8 | 9 | 108 + 108 | 14 | 1.0 | 134 |
| 128 | 9 | 108 + 108 | 14 | 1.0 | 134 |
| 4,096 | 9 | 108 + 108 | 14 | 1.0 | 134 |

The eleven inline SQL assertion blocks passed through commit, flush and reattach,
including scalar and nested FLOAT/DOUBLE, signed zero, nonfinite values and NULLs.
The independent 4,096D closed-form L2 regression returned **16,781,312** in both
component orders, rejected overflow and passed a subsequent valid query. Both
regressions were shown to fail against their respective pre-fix providers.
Six oracle checks passed. Managed-write regression passed seven phases,
28 installed oracle comparisons (minimum recall 0.9) and 13 native/recovery cases;
append regression passed four phases, 16 comparisons (minimum recall 1.0) and
seven recovery checks. These include cancellation, commit conflicts, lost ACKs
and actual SIGKILL/WAL recovery. The corrected non-editable installation passed
the complete Vane base suite: **1,254 passed, 15 optional skips**, in separate
non-Ray and real-Ray processes. Formatting, Rust formatting, copyleft, workflow
parsing and 59 relative documentation links passed; all nine executed source
identities were verified unchanged after the run.

The successful native build used the existing incremental Release directory,
explicit local provider source roots, the default six-extension base profile,
fixed native pins and CI's bindgen system-header setting. The runtime was
reinstalled non-editably and both providers rebuilt, test-signed, packaged and
installed into `/tmp/ducklake-vane-functional-env-20261106`. Earlier frozen trial
environments and wheels are unchanged. Local installation explicitly replaced
the two matching provider wheels, preserving existing base Python dependencies;
full offline dependency resolution was not claimed.

| Evidence | Local path | SHA-256 |
| --- | --- | --- |
| Write/recovery/base regression | `/tmp/ducklake-vortex-functional-regressions-20261106/report.json` | `0e865798a2a071fe87c73d2da1819395cd80ae37673ce780d7125560a6266a1c` |
| Complete corrected matrix | `/tmp/ducklake-vortex-functional-matrix-v5-20261106/report.json` | `69aae11e2eb1608b355435d7e8f9aa63432db91c9c0a852eeaa0cbb3dd1b1b83` |
| Matching provider bundle | `/tmp/ducklake-vortex-functional-l2-wheels-20261106/report.json` | `98d4f26a841c35ac53e81145debe1571033327bae9420344a66bac6ab90e7c9b` |
| Pre-fix inline reproduction | `/tmp/ducklake-vortex-inline-float-before-evidence-20261106.json` | `cc477c53c4e33f86e9c792a983aef9026249d95705317be06147dcc6f72abc8b` |
| Pre-fix exact SQL regression | `/tmp/ducklake-vortex-inline-sql-negative-v2-20261106/negative-report.json` | `02a46ad5c9874506b7a85bfff1d1d21bc2b91a313a09a4f04bafa0668b87b19a` |
| Pre-fix dense-L2 reproduction | `/tmp/ducklake-vortex-l2-before-evidence-20261106.json` | `be3a0026a32634a4f1c267f546954f89a09a8f8638d1af84f0311c0d32561967` |
| Pre-fix closed-form regression | `/tmp/ducklake-vortex-dense-l2-negative-20261106/negative-report.json` | `fee7eaa8ffe8a976dfffbfdc7795f3d17bbc2291b6e0c5f3942f4b6b08391873` |

The native Vane module SHA-256 remains
`8ca20bd4c8a4203f239ff6d785c45575241d3b2c8d1f8e9fc184d90de1d54876`.
The bundle report records both exact provider wheel hashes. The matrix report
records the executed test sources and both changed runtime sources. CPU **6–11**
and all inherited thread variables were preserved; parent affinity restored to
CPU 0–23. Total elapsed time is recorded only for diagnostics, with no performance
acceptance or comparison. An initial harness directory-ownership rejection and
partial pre-fix runs remain separate evidence; none weakens the accepted checks.

R4 stays deferred and incomplete, with previous failed A/A and withdrawn A/B
results unchanged. This R5 slice supplies bounded functional/numerical evidence;
request expiry, reference/WAL reclamation, larger data admission, general
predicates, distributed ANN and SPFresh native online updates remain open.
