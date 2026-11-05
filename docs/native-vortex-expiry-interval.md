# Controlled evaluation of source-expiry window queries

**Decision on 2026-11-06: withdraw both window candidates.** Production `src/`
is restored byte-for-byte to #76 (`0fb0fbd58d881d0fbac02cba3e04b42d8847be5f`).
The bounded query remains a test-only SQL reference, excluded from provider
artifacts. The retained NOT EXISTS query, transaction/ownership/reference
checks, deletion intents, scheduling and two-object/64-MiB budget are unchanged.

The [restored source-GC baseline](native-vortex-source-gc-cost.md) found a
scale-sensitive ended-data-file query in `DeleteSnapshots`. At 4,096 materialized
history updates, its instrumented query averaged 164.928 ms; the complete
two-snapshot collector median was 197.127 ms. This experiment compares an
equivalent ordered-window query in the actual collector.

## Candidate and visibility proof

For a file interval `[begin, end)`, let `p` be the greatest retained snapshot ID
strictly below `end`. The interval has a retained reference exactly when `p`
exists and `p >= begin`. This holds with arbitrary gaps, not just a retained
suffix. Active files still follow the existing NULL-end rule; whole-table
cleanup retains its independent table-ID disjunction.

The candidate unions ended-file events with retained snapshots, orders by
boundary and event kind, and takes a running snapshot maximum. End events sort
before snapshots at the same ID, preserving the exclusive end boundary. Retained
snapshots at or beyond the greatest ended-file endpoint cannot be predecessors
for any file, so the bounded candidate excludes that irrelevant tail. With no
ended files, the maximum is NULL and the snapshot event input is empty. It
uses standard SQL CTEs, UNION ALL, a window MAX and a left join, with no backend
selector, compatibility path or fallback. Each trial changed only the
ended-data-file query; delete-file and table/schema cleanup queries retained
their existing semantics. Neither trial was adopted in production.

An independent Python interval oracle, the original NOT EXISTS query and the
[frozen bounded candidate](../test/vane/expiry_interval_window.sql) passed
260 cases each on native Vane and SQLite. Cases
cover every subset of a boundary set, snapshot holes, equal/reversed/NULL
intervals, active files and forced whole-table cleanup, plus seeded random
intervals. The public SQL regression passed all 31 blocks against both the
candidate and restored original implementations, exercising native snapshot
gaps, rename, exclusive endpoints and retained/dropped table and schema history.

## Rejected unbounded window

The initial window included all retained snapshots. Its complete 16-round
matrix passed semantic/input checks and improved the 512/4,096 materialized
expiry cases by 16.566%/81.538% in paired median ratio. It was withdrawn under
the predeclared rule: the 4,096 inline control regressed 12.875%, with only two
winning rounds. The primary mean collector time in that control was
29.647/32.623 ms, baseline/candidate. Separate diagnostics measured 5.273 ms
for the unbounded event query, sorting thousands of snapshot events despite
only two early ended files. No production adoption or sustained test is claimed
for this rejected candidate.

Report: `/tmp/ducklake-vortex-expiry-ab-full-v1-20261106/report.json`.
SHA-256: `6463f666712a5f7f4f372d12272027e2bb4a1f5a3c6136403b58a8db522ba64c`.
The bounded candidate retains the same performance rule and adds four semantic
cases for long irrelevant tails and an entirely active file inventory.

## Actual-collector comparison

The baseline is the frozen #76 executable. The candidate links the same native
benchmark object and dependency archives, replacing only the metadata-manager
translation unit. Both execute normal `Source::Collect`; there is no runtime
query selector. Their checks retain ordinary-transaction deferral, exact
two-object progress, source/index identities, durable intents/counters, current
typed rows and named ANN.

Every invocation restores #76's same sealed input at its canonical paths. Each
of twelve cases has four excluded warmups, sixteen ABBA/BAAB paired rounds with
two calls per variant per round, and one separate diagnostic per variant. Thus
the full matrix has 768 primary calls, 48 warmups and 24 diagnostics. The runner
rotates/reverses case order and keeps added SQL callbacks out of primary timing.

The rule is saved before measurement. Both medium/large materialized-expiry
cases require at least 2% median paired-round improvement, a descriptive 95%
bootstrap upper ratio below 1, and at least thirteen winning rounds out of
sixteen. Each of ten controls must have median regression at most 2%. Otherwise
the candidate is withdrawn. Four analysis regressions exercise consistent
gains, insufficient gains, opposing rounds and missing samples. Shared CI runs
semantics and functional checks, without a performance threshold.

## Completed bounded-window result

The second full matrix also passed every semantic/input check but failed the
unchanged retention rule. Medium/large materialized-expiry targets improved
12.016%/81.384%, with fifteen/sixteen winning rounds. Their descriptive 95%
intervals were 0.82631–0.89278 / 0.17511–0.19261. The large materialized case's
individual-sample p50 was 187.973/35.164 ms, baseline/candidate; paired-round
ratios use round means and differ from ratios of individual-sample medians.

| History | Storage | Task | Unbounded paired ratio | Bounded paired ratio |
| ---: | --- | --- | ---: | ---: |
| 8 | inline | expire | 1.00017 | 1.03770 |
| 8 | inline | files | 0.98925 | 1.01337 |
| 8 | file | expire | 1.01470 | 0.99761 |
| 8 | file | files | 1.00416 | 0.98335 |
| 512 | inline | expire | 1.00834 | 1.04761 |
| 512 | inline | files | 0.99879 | 0.98029 |
| 512 | file | expire | 0.83434 | 0.87984 |
| 512 | file | files | 0.98836 | 1.00677 |
| 4,096 | inline | expire | 1.12875 | 1.04151 |
| 4,096 | inline | files | 1.00626 | 1.02395 |
| 4,096 | file | expire | 0.18462 | 0.18616 |
| 4,096 | file | files | 1.01355 | 1.06095 |

Bounded inline-expiry controls exceeded the 2% rule by +3.770%/+4.761%/+4.151%
at 8/512/4,096 updates. The large inline/materialized file controls also failed.
Several control intervals include 1: this is failure to meet the predeclared
rule, not proof of a causal slowdown on every control. The bound reduces the
initial large-inline penalty without making the full matrix pass. It does not
justify a per-input selector, larger budget or relaxed gate.

The two matrices took 662.807/663.909 seconds including restoration and separate
diagnostics, with 1,536 primary calls, 96 excluded warmups and 48 diagnostics
combined. Both retain ordinary-transaction deferral, exact two-object progress,
actual deleted/retained files, typed rows and unchanged current index artifacts.

Bounded report:
`/tmp/ducklake-vortex-expiry-bounded-ab-full-v1-20261106/report.json`.
SHA-256: `8c8f16241303a531552095f229ee6a81c4c8d070322128705a4c8dd16a3d374b`.

No runtime candidate is retained, so there is no new thirty-minute measurement
or sustained-stability claim. R4 remains open. A follow-up can evaluate a simpler
ordered-snapshot lookup/merge approach, accounting for metadata transfer and
memory as well as selection latency. It must retain interval/gap semantics and
the existing reader, ownership, intent and budget contracts before paired A/B
can justify production adoption.

## Reproduce

After building the qualified native Ninja closure with compile commands, the
test builder generates private metadata-manager translation units from the
frozen query and links separate executables. Repository and provider sources
remain unchanged. Prepare new inputs with its baseline executable:

```sh
python -I test/vane/build_vortex_expiry_trial.py \
    --build-dir "$SKBUILD_BUILD_DIR" --output-dir /tmp/expiry-trial-build
env -u VORTEX_SPFRESH_NATIVE python -I test/vane/test_vane_vortex_source_gc_benchmark.py \
    --driver /tmp/expiry-trial-build/baseline --output-dir /tmp/expiry-trial-inputs
VANE_RUNNER=local-fast python -I test/vane/test_vane_vortex_expiry_interval.py \
    --output-dir /tmp/expiry-interval-semantics
python -I test/vane/test_vane_vortex_source_gc_ab_stats.py
env -u VORTEX_SPFRESH_NATIVE python -I test/vane/test_vane_vortex_source_gc_ab.py \
    --inputs /tmp/expiry-trial-inputs/report.json \
    --baseline-driver /tmp/expiry-trial-build/baseline \
    --candidate-driver /tmp/expiry-trial-build/bounded \
    --output-dir /tmp/expiry-interval-ab
```

Use `/tmp/expiry-trial-build/unbounded` and a separate output directory for the
first trial. Output directories must be new. Inputs must remain at their
original canonical paths, without another writer/benchmark using them.

## Final local qualification after withdrawal

The final test-only reference passed 260 native-Vane and 260 SQLite oracle
cases. All four paired-analysis regressions passed. The reproducible builder
compiled all three executables; its baseline bytes match the original #76
binary, and both generated candidates passed separate functional A/B smokes.
Those additional smokes do not replace or retune the retained measurements.

The restored original implementation passed the 31-block SQL regression and
installed source qualification: 38 native cases, sixteen public comparisons,
minimum fixture recall 1.0. The matching Vane base launcher passed 1,222 non-Ray
and 32 Ray tests in separate processes, with fifteen optional-dependency skips.
Production sources and all three provider wheel bytes remain unchanged.
Installed qualification used the host's normal ancestor ownership, preserving
the existing signature/cache checks. Formatting, copyleft, YAML and local
documentation-link checks passed.
