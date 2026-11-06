# Repeated source-GC metadata diagnostics

The [fixed-affinity A/A](native-vortex-source-gc-affinity.md) did not demonstrate
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

## Result and follow-up

Formal results are pending. Retain all existing guards and budgets regardless
of diagnostic timing. A later runtime candidate needs its own matched-link A/B
and the existing quality/control gates before any sustained-load adoption.
