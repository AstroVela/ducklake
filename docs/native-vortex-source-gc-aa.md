# Identical-binary A/A for restored source GC

This protocol follows the [withdrawn ordered lookup](native-vortex-expiry-lookup.md).
Its two materialized-expiry targets improved, while two file-unlink controls
exceeded the predeclared 2% regression gate without executing the changed
selection. Their descriptive intervals included 1. This does not identify a
causal slowdown or show that the existing benchmark resolves 2% differences.
The previous withdrawal decisions remain unchanged.

## Frozen protocol

The implementation and this protocol are committed before formal sampling.
The [A/A runner](../test/vane/test_vane_vortex_source_gc_aa.py) takes exactly one
executable argument. Both experimental labels invoke that same canonical path
and SHA-256; `baseline`/`candidate` are the existing analysis group names, not
different implementations. The original A/B runner, bootstrap and rules are
unchanged. A/A reuses its paired-round analysis and the same native functional,
image and diagnostic checks.

The formal baseline executable is
`/tmp/ducklake-vortex-expiry-ab-baseline-20261106`, SHA-256
`168562ccee3d4cc66659d4bb8b18a5b891a33ecbdd2424f99736a22e0ec0f400`.
Inputs are the twelve original sealed #76 images described by
`/tmp/ducklake-vortex-source-gc-full-v3-20261106/report.json`, SHA-256
`48855d42a60fd423caf0df052e46bb95050003e61f0ac83b570f03be3f5cf042`.
Every invocation restores fresh regular single-link copies at the original
canonical paths and hashes input/output identities. No other benchmark/writer
uses those paths, and no build or qualification runs during formal timing.

Run these phases once in order; the full phase proceeds regardless of the
focused phase's precision result. Execution, semantic or identity failures stop
the experiment. There is no adaptive sample count or rerun-until-pass:

| Phase | Cases | Paired rounds/case | Primary calls | Excluded warmups | Separate diagnostics |
| --- | --- | ---: | ---: | ---: | ---: |
| Focus | 8-update inline file-unlink; 4,096-update materialized file-unlink | 16 | 128 | 8 | 4 |
| Full | 8/512/4,096 updates × inline/materialized × expiry/file-unlink | 64 | 3,072 | 48 | 24 |
| CI smoke | Four 8-update cases | 2 | 32 | 16 | 8 |

The focused phase checks the two previous failing controls with the old round
count. The full phase uses a fixed larger sample and also reports its first
sixteen rounds separately. Based on the two previous control interval widths,
an inverse-square-root planning estimate suggests roughly 42/46 rounds to
reduce their log-ratio half-widths to about 2%; choose 64 before observing A/A.
This is a planning estimate, not a power guarantee or an assumption that every
case has the same variance. Sampling ends at 64 even if precision is inadequate.

Each round uses ABBA on even rounds and BAAB on odd rounds, two calls per label.
Cases rotate and reverse in the same order as the A/B harness. The statistic is
the median of per-round ratios of label-B/label-A **mean collector times**.
Use the unchanged seeded 10,000-resample percentile bootstrap of paired rounds.
Compute all inferential/descriptive summaries after fixed sampling ends.

## Decision rule

For each case, require the whole descriptive 95% paired-median interval inside
**[0.98, 1.02]**, inclusive. Merely containing 1 is insufficient. Also require
every consecutive 16-round block's median inside that band, so a quiet aggregate
does not hide temporal drift. The full phase has four such blocks. Report the
first sixteen full-matrix rounds independently as an audit of the old sample
count; it does not replace the complete result.

A phase records `precision_demonstrated` only when every declared case meets
both checks. Otherwise record `precision_not_demonstrated` and distinguish wide
intervals from block drift and directional label differences. Missing cases,
rounds or either label's two samples are errors. CI smoke always records
`smoke_only`, never a precision claim. Ten deterministic regressions cover equal
work, small noise, systematic shift, an interval that spans 1 but is too wide,
block drift hidden by an aggregate, missing/invalid samples, incomplete/duplicate
case inventories and short/smoke profiles.

These are per-case descriptive checks of this fixed aggregate protocol on one
host. The bootstrap is unchanged from the existing harness; these intervals are
not simultaneous family-wide coverage or proof against serially correlated
noise. Block medians provide a drift check rather than a full noise model.
A passing A/A is no guarantee for another host or time. A failed A/A does not
prove which environmental or measurement factor caused the discrepancy.

## Work and observations

Native `Source::Collect` retains two objects/64 MiB, keep-two/min-age-zero policy,
ordinary-transaction deferral and the same source/index identity, intent, typed
business-row, current ANN and exact-progress checks. There is no production
query change, compatibility path, fallback, provider rebuild or new budget.
No new thirty-minute sustained-load claim follows from this measurement study.

Primary timings remain the existing in-process collector/phase timers, without
added SQL callbacks. SQL/checkpoint diagnostics use separate restored processes.
The parent records per-child process wall time and user/system CPU, minor/major
fault and voluntary/involuntary context-switch deltas outside that interval.
These counters include startup and functional checks; they are not collector-only
CPU measurements or causal attribution. Initial/final load and inherited CPU
affinity are recorded. The experiment changes neither affinity nor governor.

The runner hashes itself, analysis/support files, executable and input report
before/after sampling and verifies inherited affinity remains the same. It saves
its decision rule before the first native call. All raw samples remain grouped
by case, label, round and block, so later diagnosis can inspect order and timing
without changing which observations are primary.

## Reproduce

Use a qualified unchanged native collector and prepare its baseline input report
as described in the [source-GC baseline](native-vortex-source-gc-cost.md). The
source report's driver hash must match the single A/A executable.

```sh
python -I test/vane/test_vane_vortex_source_gc_aa_stats.py
python -I test/vane/test_vane_vortex_source_gc_ab_stats.py
python -I test/vane/test_vane_vortex_source_gc_aa.py \
    --driver /tmp/source-gc-baseline --inputs /tmp/source-gc-inputs/report.json \
    --profile focus --output-dir /tmp/source-gc-aa-focus
python -I test/vane/test_vane_vortex_source_gc_aa.py \
    --driver /tmp/source-gc-baseline --inputs /tmp/source-gc-inputs/report.json \
    --profile full --output-dir /tmp/source-gc-aa-full
```

Output directories must be new and the original input paths must remain valid.
For CI smoke, prepare only history 8 and use `--profile smoke`; shared runners
enforce functional checks, not performance/precision thresholds.
