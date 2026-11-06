# Identical-binary A/A for restored source GC

**Result on 2026-11-06: 2% precision is not demonstrated under this protocol.**
The full 64-round matrix passes both predeclared checks in 5/12 cases; its
first-sixteen-round audit passes in 2/12. All 3,200 primary calls, 56 excluded
warmups and 28 separate diagnostics pass functional/input checks. No runtime
change is adopted and previous A/B withdrawal decisions remain unchanged.

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

## Completed focused result

The two focused controls each ran sixteen paired rounds, taking 199.452 seconds
including restoration and separate diagnostics. Neither demonstrates the
declared precision. The large materialized file-unlink control has a -2.294%
label-B/label-A median difference despite identical executable bytes. Both
intervals include 1, so this is not proof of a persistent directional bias.
It does show why a small sixteen-round label difference alone cannot establish
2% measurement precision.

| History | Storage | Task | Paired median ratio | Descriptive 95% interval | Precision |
| ---: | --- | --- | ---: | --- | --- |
| 8 | inline | files | 1.00699 | 0.97113–1.02003 | not demonstrated |
| 4,096 | file | files | 0.97706 | 0.96041–1.00418 | not demonstrated |

## Completed full result

The twelve-case, sixty-four-round phase completed once, taking 2,532.799 seconds
including restoration and diagnostics. All twelve full-phase median point
estimates fall inside [0.98,1.02]. However, only 6/12 complete intervals fit the
band, 7/12 cases keep every sixteen-round block median in the band, and 5/12
meet both checks. The first-sixteen-round audit meets both in only 2/12 cases.
The focused and full phases therefore both record `precision_not_demonstrated`.
No additional samples were selected after these results.

The table reports paired median ratio [descriptive 95% interval]. Block ranges
are the minimum/maximum of the four sixteen-round medians, not confidence
intervals. `CI` and `blocks` indicate the two predeclared checks.

| History | Storage | Task | First 16 rounds | Complete 64 rounds | Block-median range | Checks: CI / blocks |
| ---: | --- | --- | --- | --- | --- | --- |
| 8 | inline | expire | 1.01727 [0.96915, 1.04761] | 1.01579 [0.99933, 1.02481] | 0.99759–1.02393 | fail / fail |
| 8 | inline | files | 1.01751 [0.98668, 1.04587] | 0.99959 [0.99522, 1.01685] | 0.99399–1.01751 | pass / pass |
| 8 | file | expire | 0.99401 [0.96643, 1.03501] | 1.00312 [0.98667, 1.02114] | 0.99401–1.02115 | fail / fail |
| 8 | file | files | 0.99990 [0.98578, 1.01931] | 1.00079 [0.99533, 1.01194] | 0.99990–1.00911 | pass / pass |
| 512 | inline | expire | 0.98012 [0.95357, 1.03631] | 0.98463 [0.97149, 1.01762] | 0.98012–0.99722 | fail / pass |
| 512 | inline | files | 0.99586 [0.96661, 1.01012] | 0.99828 [0.99072, 1.00448] | 0.99586–1.00219 | pass / pass |
| 512 | file | expire | 1.01363 [0.96605, 1.05943] | 1.00728 [0.97322, 1.03104] | 0.96266–1.04941 | fail / fail |
| 512 | file | files | 1.00716 [0.98768, 1.02249] | 1.00053 [0.98768, 1.01000] | 0.98494–1.00716 | pass / pass |
| 4,096 | inline | expire | 0.98191 [0.96532, 1.00295] | 0.99959 [0.98858, 1.01019] | 0.97770–1.01257 | pass / fail |
| 4,096 | inline | files | 0.97633 [0.94316, 1.00670] | 0.98947 [0.97321, 1.00767] | 0.97633–1.01224 | fail / fail |
| 4,096 | file | expire | 1.01699 [0.93500, 1.05769] | 1.00371 [0.97802, 1.03113] | 0.98187–1.01699 | fail / pass |
| 4,096 | file | files | 0.99524 [0.98042, 1.01671] | 0.99832 [0.98894, 1.01024] | 0.98186–1.00610 | pass / pass |

For example, the 4,096-inline expiry case fits the aggregate interval but its
third block median is 0.97770, failing stability. The 512-materialized expiry
case has block medians 1.01363, 1.04941, 1.00448 and 0.96266. These block
deviations may reflect finite-sample noise or time variation; they do not by
themselves establish a causal drift mechanism. More rounds improved some cases
without qualifying the whole matrix.

Reports and SHA-256:

- `/tmp/ducklake-vortex-source-gc-aa-focus-20261106/report.json`: `7b9c5f794578dbc00f8bd275a4e9a173f1b0c0e13f0d107464b0a8bd8974fed3`.
- `/tmp/ducklake-vortex-source-gc-aa-full-20261106/report.json`: `beeb32e51a2013ee0f829f377661c60ae2e0b3253c5b00a8cd0efe62e4b5e359`.

The pre-sampling protocol commit is
`d92e9cffb2abc545bceeefabe5087bf09723041e`. Runner, original analysis/support,
executable and input-report hashes remain unchanged; all sealed input images
and inherited CPU affinity verify unchanged. The formal experiment comprises
3,284 timed collector invocations across the two phases, plus each native
invocation's untimed ordinary-transaction deferral check.

## Diagnostic limits and link placement

The full phase inherits CPUs 0–23. Recorded load averages changed from roughly
3.89/7.55/8.34 to 2.83/2.93/3.88. Whole-child involuntary-switch medians are
usually 2–3 and nineteen major faults occur across the full primary sample.
These observations do not identify which part of GC varies or establish a
scheduling/frequency/cache explanation.

Parent-observed process-wall medians often cluster near 265/315/365/465 ms.
The installed Python 3.12 subprocess timeout wait uses an exponentially delayed
poll capped at 50 ms; this diagnostic includes observation/polling delay. It
must not be treated as the native process's exact completion time or collector
latency quantization. Primary results use the unchanged in-process nanosecond
collector timer, outside that parent-wait observation.

A separate read-only inspection after sampling also finds a limitation of the
previous A/B builder: it extracts baseline metadata code from the static archive
but inserts a standalone metadata object only for the lookup executable. This
changes link placement. Selected public symbol addresses from those measured
executables are:

| Symbol | Original baseline | Withdrawn lookup |
| --- | --- | --- |
| Metadata manager `Query(string&)` | `0x12dffd0` | `0x11a3c60` |
| Metadata manager `DeleteSnapshots` | `0x12ffc50` | `0x11e6d90` |
| Source retention `Collect` | `0x120d7a0` | `0x1340ff0` |

The [trial builder](../test/vane/build_vortex_lookup_trial.py) makes this
insertion explicit. This symbol inspection does not prove a performance effect.
Same-binary A/A cannot separate that cross-binary placement factor; a later A/B
should use matching explicit-object link order and compiler flags on both
sides. Current precision failures cannot be attributed solely to link placement,
since the A/A labels here use one identical artifact.

## Next validation

Keep R4 open and the runtime unchanged. Predeclare a measurement-isolation
experiment before trying another runtime optimization: select and record a
fixed CPU set for the test process, preserve native thread settings and all
restore/work/budget/semantic checks, then rerun the same A/A precision checks.
Start with the problematic controls, advancing to the full matrix only under
a previously declared rule. Record scheduling/frequency observations where
available; do not change a host-wide governor as part of this experiment.

If precision remains inadequate, retain that result and diagnose the measurement
rather than merely increasing rounds or retrying until it passes. Only a
qualified method should precede a new A/B with matched link configuration.
This study neither reclassifies #77/#78 as passing nor measures sustained
write/query/maintenance stability. A later accepted runtime candidate still
needs the unchanged thirty-minute actual-ReadView/quality/drain/final-row/receipt
checks, followed by repeated longer backlog/resource observations for R4 exit.

## Final local qualification

The ten A/A analysis regressions and four unchanged A/B analysis regressions
pass. The four-case, two-round native smoke completes with `smoke_only`; it
makes no precision claim. All formal execution/functional/image checks pass,
including native ordinary-transaction deferral, exact work/counters, current
business/ANN results, retained sources/indexes and zero completed intents.
Parent timing containment and nonnegative process counters also verify across
all 3,284 formal native invocations.

The Vane base launcher passes 1,222 non-Ray and 32 Ray tests in separate
processes, with fifteen optional-dependency skips. Installed qualification
preserves the host's normal ancestor ownership and provider signature/cache
checks. Production `src/` and the three existing wheel bytes remain unchanged.
Formatting, copyleft, workflow YAML, local documentation links and diff checks
pass. There is no native rebuild or new sustained-load claim for this test-only
change.
