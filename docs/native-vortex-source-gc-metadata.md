# Repeated source-GC metadata diagnostics

**Result on 2026-11-06: all 128 diagnostics and eight excluded warmups pass;
large-input time outside SQL needs a narrower measurement.** The
[fixed-affinity A/A](native-vortex-source-gc-affinity.md) did not demonstrate
2% measurement precision. Its full phase was not started. This experiment
repeats separate callback-enabled diagnostics to describe where collector time
and variation occur. It changes no runtime code and cannot reclassify earlier
primary measurements or establish sustained-write stability. R4 remains open.

## Protocol frozen before formal sampling

Commit this protocol, [runner](../test/vane/test_vane_vortex_source_gc_metadata.py)
and [analysis checks](../test/vane/test_vane_vortex_source_gc_metadata_stats.py)
before sampling. There is one formal run with **16 rounds per case**, four calls
per round, on the existing 8-inline file-unlink and 4,096-materialized file-unlink
inputs: **128 diagnostic calls plus eight excluded diagnostic warmups**.
Even rounds use ABBA, odd rounds BAAB. Rotate cases by round and reverse odd
rounds, as in the existing A/A runner. Both labels execute one identical binary;
they are scheduling labels rather than different runtime implementations.
No primary samples, adaptive counts, precision gate or rerun-until-pass is used.
Stop for execution, semantic, trace or integrity failure.

Reuse the baseline executable SHA-256
`168562ccee3d4cc66659d4bb8b18a5b891a33ecbdd2424f99736a22e0ec0f400` and the sealed
twelve-case input report SHA-256
`48855d42a60fd423caf0df052e46bb95050003e61f0ac83b570f03be3f5cf042`.
Use **CPUs 6,7,8,9,10,11** and the existing affinity/topology validation and
finally-restoration helper. Verify inherited child affinity and the explicit
thread-environment allowlist. Keep DuckDB `SET threads=2`, SPFresh settings,
keep-two/min-age-zero retention, two-object/64-MiB budget and all transaction,
reference, ownership, intent, business-row and ANN checks unchanged. Restore
fresh single-link copies at their original canonical paths before every call.
Validate exact deletion progress and retained data/index bytes after each call.
No provider rebuild, concurrent build/test load, other canonical-path writer,
thread override, governor change, compatibility layer or fallback is introduced.

The smoke profile is functional only: two history-8 file-unlink cases, two
rounds, sixteen diagnostics and eight excluded warmups. Its explicit available
CPU may be one core; it makes no precision or performance claim.

## Analysis declared before sampling

Validate every SQL interval against the native collector boundary, exact
duration, nonnegative depth and unique enclosing parent. Require disjoint outer
scopes, the frozen 42-outer/five-nested inventory and the expected ordered source
checkpoints. Five native COMMIT events nested in outer DuckLake COMMITs are
validated and excluded from totals. Preserve exact SQL text and hashes.

Partition only **depth-zero SQL** into four known-source path queries, four
scheduled-file ownership checks, four index-reference queries, ten BEGINs,
five COMMITs and fifteen remaining statements. `outside_sql` is the collector
timer minus these disjoint SQL durations. These seven parts conserve the total.
Native wait/metadata/expiry/artifact/unlink phase distributions are a **separate
overlapping view**; do not add them to SQL categories or subtract SQL totals
from the metadata phase to invent an unmeasured residual.

For each case, report the fixed sample count, mean/p50/p95/min/max collector,
native phase and SQL-category times, round means and separate label summaries.
P95 is nearest-rank. Report category mean fractions and the descriptive signed
quantity `Cov(part,total)/Var(total)`. These additive covariance shares sum to
one, may be negative and are mathematically coupled to total time; they do not
identify causes or independent variance explained. Constant total variance is
explicitly undefined (`null`), rather than a fabricated zero.

For each exact SQL and category, retain observed bind/plan and execute/fetch
splits and their call counts. Both fields must exist together and sum to the
query duration. A query without both remains explicitly unsplit. Execution/
fetch includes waiting; existing wait phase measures admission/lock wait only.
The trace does not separate CPU execution, I/O, scheduler delay or callback
overhead. These diagnostics neither mix into previous primary samples nor
establish an A/B optimization, 2% equivalence or a 30-minute load result.

## Reproduce

```sh
python -I test/vane/test_vane_vortex_source_gc_metadata_stats.py
python -I test/vane/test_vane_vortex_source_gc_metadata.py \
    --driver /tmp/ducklake-vortex-expiry-ab-baseline-20261106 \
    --inputs /tmp/ducklake-vortex-source-gc-full-v3-20261106/report.json \
    --cpus 6,7,8,9,10,11 --profile formal \
    --output-dir /tmp/source-gc-metadata-diagnostics
```

The output directory must be new. Preserve `measurement-contract.json`, raw
per-call JSON/logs and `report.json`. CI runs analysis checks and functional smoke
against its qualified native driver/input corpus and uploads the evidence.

## Completed result

The protocol commit is `dfd71afb675d8efc17f3a02d76ad26a0c16ee2a4`.
The single formal run completes in 193.569 seconds, including restoration and
validation. It records `diagnostics_only`, sixteen completed rounds and 64
measured diagnostics per case. Every one of the 136 native invocations passes
the existing ordinary-transaction deferral, exact two-file progress, retained
data/index, current business/ANN, input-image and trace checks. The original
0–23 mask is narrowed to 6–11, verified in the child probe and restored to 0–23.
All twelve recorded thread variables remain unset. Tracked sampler, driver,
input report, image and provider-wheel identities remain unchanged.

Governor observations are `schedutil` at both boundaries; frequency observations
span approximately 1.2–3.2 GHz. These are boundary readings, not in-call frequency
measurements or evidence of exclusive cores. No host setting is changed.

| Input | Collector mean / p50 / p95 ms | Metadata phase mean ms | Unlink phase mean ms | Collector min / max ms |
| --- | --- | ---: | ---: | --- |
| 8 / inline | 38.111 / 38.181 / 40.554 | 35.400 | 2.335 | 34.018 / 45.832 |
| 4,096 / materialized | 84.165 / 83.805 / 87.278 | 81.435 | 2.348 | 78.070 / 129.278 |

The following seven disjoint parts conserve each collector total. Native
metadata phases above overlap this view and are not an eighth part.

| Outer partition | 8-inline mean / p95 ms | 4,096-file mean / p95 ms | Large mean fraction | Large signed covariance share |
| --- | --- | --- | ---: | ---: |
| Known-source paths, four calls | 8.883 / 9.428 | 23.360 / 24.598 | 27.76% | 26.13% |
| Scheduled ownership, four calls | 2.053 / 2.246 | 2.050 / 2.249 | 2.44% | 1.84% |
| Index references, four calls | 1.187 / 1.334 | 1.170 / 1.324 | 1.39% | 1.40% |
| BEGIN, ten calls | 1.048 / 1.253 | 1.052 / 1.238 | 1.25% | 0.68% |
| Outer COMMIT, five calls | 6.847 / 7.228 | 6.958 / 7.504 | 8.27% | 1.52% |
| Other SQL, fifteen calls | 12.913 / 14.383 | 12.811 / 13.707 | 15.22% | 9.52% |
| Outside SQL | 5.180 / 5.573 | 36.763 / 38.720 | 43.68% | 58.91% |

The largest exact SQL group is the four known-source path queries. All 5,376
measured outer statements have observed planning splits; none is unsplit.
The 640 nested COMMIT events are excluded from measured sums. Selected splits
are mean milliseconds per complete collector, not per statement:

| Category | 8-inline bind/plan / execute-fetch ms | 4,096-file bind/plan / execute-fetch ms |
| --- | --- | --- |
| Known-source paths | 3.393 / 5.489 | 3.299 / 20.062 |
| COMMIT | 0.175 / 6.672 | 0.173 / 6.785 |
| Other SQL | 5.402 / 7.511 | 5.419 / 7.392 |

Other SQL includes five latest-snapshot reads totaling 6.048/6.168 ms per
collector. COMMIT and remaining SQL group means are similar between these two
inputs; this is a descriptive observation across differing metadata histories,
not a controlled causal estimate. Label summaries also remain descriptive:
baseline/candidate collector means are 37.800/38.421 ms and 83.831/84.499 ms.
No label is an optimization and no equivalence interval is inferred here.

All declared samples are retained. The largest materialized sample is round 10,
block 1, candidate: 129.278 ms total, with 64.296 ms outside SQL and 35.080 ms
in known-source SQL. Its influence on covariance shares is retained. Shares
describe this finite sample, including that tail; they are not causal
attribution, robust population estimates or the earlier primary A/A statistic.

Formal evidence: `/tmp/ducklake-vortex-metadata-formal-20261106/report.json`.
SHA-256: `11451622a3e0d1779525a7b6cc8bb4f9e8385e5766cfdf94a8733938e458f8ec`.
The functional smoke separately completes sixteen diagnostics and eight warmups;
it is excluded from this result. CI uploads its own raw smoke evidence.

Local validation also passes ten new trace/accounting checks and 23 existing
affinity/A/A/A/B analysis checks, formatting, workflow YAML, relative links,
frozen identities and the copyleft inventory. The qualified installed Vane base
launcher passes 1,254 tests with fifteen optional skips in separate non-Ray and
Ray processes. No native or provider rebuild is needed for this test-only change.

## Next bounded diagnostic

Measure the complete `HasFileReference` call and its result-fetch and
`CanonicalizePath`/comparison loop separately, inside the same transaction and
with these inputs, CPU mask, thread settings, guards and budgets. For these
unreferenced inputs, the current
[implementation](../src/storage/ducklake_metadata_manager.cpp) fetches every
known path and canonicalizes rows after the query returns. This is a plausible
contributor to the unmeasured 36.763-ms outside-SQL envelope; the present trace
does **not** establish how much belongs to that loop, result destruction,
connection setup or other C++ work. Freeze repeat counts before collecting
the narrower diagnostics; preserve full GC functional checks and keep callback
measurements separate from primary results.

Do not weaken or cache reference checks across transactions, remove intent or
ownership checks, or change COMMIT ordering based on this result. After the
outside-SQL location is measured, any runtime candidate needs its own A/B with
matched explicit-object link order and the existing quality/control gates.
R4 remains open: neither diagnostic repetition nor the prior A/A studies are
a new 30-minute load or sustained-write backlog/resource-stability result.
