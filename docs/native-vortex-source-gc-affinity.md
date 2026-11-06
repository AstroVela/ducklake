# Fixed-process-affinity source-GC A/A

**Result on 2026-11-06: the focused phase does not demonstrate 2% precision.**
CPUs 6–11 apply as declared and thread settings remain unchanged. Both focused
median ratios fit the band, but neither complete interval does. All 128 primary
calls, eight excluded warmups and four separate diagnostics pass functional/input
checks. Under the predeclared advancement rule, the full matrix is not started.
No runtime change or new full-matrix/stability claim follows from this result.

This experiment follows the [unrestricted A/A study](native-vortex-source-gc-aa.md).
That study did not demonstrate stable 2% precision under its declared interval
and block-median checks. This step tests a fixed process CPU set while keeping
the existing native thread settings, work, transaction protection and budgets.

## Protocol frozen before sampling

Commit this protocol and launcher before formal sampling. The new
[affinity orchestrator](../test/vane/test_vane_vortex_source_gc_affinity.py)
requires an explicit CPU list. It pins its own fresh test process, lets the A/A
sampler and native subprocesses inherit that set, verifies the sampler's actual
reported affinity, and restores its original affinity on completion or error.
Unavailable/partial CPU sets, SMT siblings within the selected set, unknown or
mixed NUMA domains and formal sets smaller than two CPUs fail before sampling.
Affinity failure aborts rather than starting an unrestricted experiment.

The declared formal set is **CPUs 6,7,8,9,10,11**, package 0, physical core IDs
8,9,10,11,12,13 on node0. Their SMT siblings are 18–23 and are excluded from the
selected set; the reported shared L3 domain is 0–23. The inherited original set
is 0–23. These choices come from topology inspection before performance data,
not selection among trial results. Affinity does not reserve these cores or
prevent work on their siblings, fix frequency, bind memory or establish an
exclusive machine.

Reuse the unchanged native collector executable, SHA-256
`168562ccee3d4cc66659d4bb8b18a5b891a33ecbdd2424f99736a22e0ec0f400`, and original
sealed twelve-case input report, SHA-256
`48855d42a60fd423caf0df052e46bb95050003e61f0ac83b570f03be3f5cf042`.
Every native call still restores at the original canonical paths and checks
input/output identities. The binary continues to issue `SET threads=2`; existing
SPFresh settings and inherited thread-environment variables remain unchanged.
No thread-count override, runtime change or provider rebuild is introduced.
Scheduling constraints can affect execution, but the experiment does not tune
worker counts or change the native workload.

The A/A sampling and analysis functions from #79 remain unchanged. Its rule
metadata is clarified to describe one requested profile; this parent protocol
owns the new phase-advancement decision. Numeric criteria, ordering and samples
inside each profile are unchanged.

| Phase | Cases | Paired rounds per case | Primary calls | Excluded warmups | Separate diagnostics |
| --- | --- | ---: | ---: | ---: | ---: |
| Focus | 8-inline file-unlink and 4,096-materialized file-unlink | 16 | 128 | 8 | 4 |
| Full, only after both focus cases pass | Twelve original cases | 64 | 3,072 | 48 | 24 |
| CI smoke, functional only | Four 8-update cases | 2 | 32 | 16 | 8 |

Run the focused phase once. Require both cases to meet the existing criterion:
the whole descriptive 95% paired-bootstrap interval and each consecutive
sixteen-round block median must be inside **[0.98,1.02]**, inclusive. Only then
run the complete matrix once, with the same criterion, four block medians per
case and a separate first-sixteen-round audit. If focus fails the gate, finish
with `precision_not_demonstrated`, record
`full_phase=not_started_focus_gate_failed` and make no full-matrix claim.
Execution, semantic or identity errors fail the experiment. Do not increase
samples, change CPUs/criteria or retry until it passes.

Pair ABBA/BAAB labels and rotate/reverse cases exactly as before. Both labels
execute the same binary and unchanged collector timer. SQL callbacks remain in
separate restored diagnostics. The two-object/64-MiB budget, keep-two/min-age-zero
policy, ordinary-transaction deferral, exact progress, retained source/index
identities and current business/ANN checks remain in effect. No build or
qualification runs concurrently with formal sampling, and no other writer uses
the canonical input paths.

## Observations and scope

Record requested/original/applied/restored CPU sets, package/core/NUMA/SMT/L3
metadata, thread environment and source hashes. Sample load, governor and
current frequency at phase boundaries by read-only access. A null frequency
field means the kernel did not expose that diagnostic; it never waives affinity
or topology validation. No host-wide governor or frequency change is made.

The existing runner verifies native driver/input/code/image identities and
its inherited affinity. Its counters cover whole native subprocesses, including
startup/functional checks; parent-observed wall time can include timeout polling.
Only the native collector timer is primary. Configuration preservation is not
a promise that OS scheduling or dynamic thread lifetimes remain identical.

The result applies to this fixed CPU set and host at this time. A comparison
with #79 is historical, not a contemporaneous causal estimate of the effect of
affinity. The unchanged bootstrap is descriptive per case and does not provide
family-wide coverage or a full model of serial noise. The earlier withdrawal
and precision results remain unchanged.

## Reproduce

Use the same qualified baseline native executable and sealed input corpus.
Choose and record an available set of physical cores on one known NUMA node
before measurement; this formal run uses 6–11 explicitly:

```sh
python -I test/vane/test_vane_vortex_source_gc_affinity_stats.py
python -I test/vane/test_vane_vortex_source_gc_aa_stats.py
python -I test/vane/test_vane_vortex_source_gc_ab_stats.py
python -I test/vane/test_vane_vortex_source_gc_affinity.py \
    --driver /tmp/ducklake-vortex-expiry-ab-baseline-20261106 \
    --inputs /tmp/ducklake-vortex-source-gc-full-v3-20261106/report.json \
    --cpus 6,7,8,9,10,11 --profile formal --output-dir /tmp/source-gc-fixed-affinity
```

The output directory must be new. The launcher controls advancement, so do not
manually run the full phase after a failed focus result. For CI functional
smoke, use an explicit available CPU with `--profile smoke` and an input report
containing only the four history-8 cases. Smoke preserves configured threads
but may schedule them on one CPU; it never makes a precision claim.

## Completed result

The pre-sampling protocol is commit
`8f465c2449840e0435c835012a1481e30c119d4f`. The formal orchestrator completes in
201.406 seconds, including restoration, diagnostics and checks. Original
0–23 affinity is narrowed to 6–11, the sampler reports that exact set, and the
orchestrator restores 0–23 at the end. Package/core/NUMA/L3 records and tracked
source hashes verify unchanged. All twelve recorded thread-environment variables
are unset before/after; no override is introduced.

| History | Storage | Task | Paired median ratio | Descriptive 95% interval | Block median | Precision |
| ---: | --- | --- | ---: | --- | ---: | --- |
| 8 | inline | files | 1.000055 | 0.982032–1.020075 | 1.000055 | not demonstrated |
| 4,096 | file | files | 1.012611 | 1.002638–1.038184 | 1.012611 | not demonstrated |

Both median points are within [0.98,1.02]. The small control's upper endpoint is
1.020074641, slightly beyond the bound; it is not rounded into a pass. The large
control's upper endpoint is 1.038184134. Its descriptive interval excludes 1,
which is an observed directional label difference in this run, not a causal
attribution to code, frequency or scheduling. There is one identical binary.

The orchestrator records `precision_not_demonstrated` and
`full_phase=not_started_focus_gate_failed`. No full output directory exists.
No additional primary samples, new CPU set, alternate criterion or full run is
used to overturn that result. This does not establish what a pinned full
64-round matrix would do; that phase was not qualified or measured.

The governor is `schedutil` for every selected CPU at both boundaries. Available
frequency observations span roughly 1.2–3.0 GHz, with no governor/frequency write.
These boundary readings do not tell us the frequency during a collector call.
The CPU mask is a scheduling constraint, not exclusive hardware isolation.

Formal orchestration report:
`/tmp/ducklake-vortex-affinity-formal-20261106/report.json`.
SHA-256: `a20e8b8fefeb74dce53e5ce744746858fb08a69d4f5fc4d4fe9082d0df338bdb`.
Focused raw report:
`/tmp/ducklake-vortex-affinity-formal-20261106/focus/report.json`.
SHA-256: `6d67eabf69b25ece896d1171c692f67dff44930809df6e8a336049e3993ab625`.

## Where the observed time sits

Existing native phase timers locate most work in `SOURCE_METADATA`. These are
primary group means, not the paired-median statistic above:

| History/storage | Complete collector A/B ms | Metadata A/B ms | Unlink A/B ms |
| --- | --- | --- | --- |
| 8/inline | 37.635/37.571 | 34.943/34.876 | 2.317/2.327 |
| 4,096/file | 82.754/84.138 | 80.083/81.418 | 2.301/2.344 |

In the large control, the complete group-mean difference is 1.383 ms and the
metadata-envelope difference is 1.334 ms. This locates the observed mean
variation mostly inside that envelope; it does not identify a cause or convert
these means into the paired-median result.

Each separate diagnostic sees four normalized known-source/reference-path
queries: their total per collector is 25.880/22.525 ms in the large A/B labels.
Outer COMMIT totals are 7.165/6.715 ms. These diagnostic observations even point
in a different direction from the primary mean difference. One diagnostic per
label is insufficient to explain variance; callback overhead is excluded from
primary timing. As before, only disjoint outer SQL scopes are summed.

## Next bounded diagnostic

Keep the runtime, gates and R4 status unchanged. Rather than trying more CPU
sets or simply adding primary rounds, predeclare repeated **separate metadata
diagnostics** on the same two inputs with the same CPU mask, threads and budgets.
Use the existing native SQL/checkpoint trace to separate known-source reference
checks, transaction/COMMIT and remaining metadata work. Report within-case
variation and distinguish planning/execution/waiting where measured. Keep these
observations out of the current primary sample and do not reclassify this run.

Any later optimization must preserve transaction-scoped reference validity and
existing transaction/reader/ownership/intent checks. Once the measurement method
is qualified, a new runtime A/B must use matched explicit-object link order on
both sides. R4 still needs real sustained-write backlog/resource
validation; this focused experiment is not a thirty-minute steady-state test.

## Final local qualification

Nine affinity regressions pass: invalid/unavailable sets, SMT/NUMA constraints,
formal CPU headroom, actual child inheritance, unchanged thread environment,
normal/exceptional restoration, and rejection of failed or incomplete focus
gates. Ten existing A/A analysis and four existing A/B analysis regressions pass.
The native four-case smoke verifies one-CPU inheritance and restoration and
records only `smoke_only`.

The formal experiment passes all 140 native invocations' functional/input and
transaction-deferral checks, unchanged code/driver/corpus/topology/thread-setting
checks, exact applied/restored affinity, process-timer containment and
nonnegative process counters. Full output is absent as the advancement rule
requires. The native sampler/analysis function AST is unchanged from #79; its
one metadata sentence now scopes sampling to an invoked profile.

The Vane base launcher passes 1,222 non-Ray and 32 Ray tests in separate
processes, with fifteen optional-dependency skips. Host qualification retains
normal ancestor ownership, provider signature and cache checks. Production
`src/` and all three existing wheel bytes remain unchanged. Formatting, copyleft,
workflow YAML, local links and diff checks pass. No native rebuild, runtime
adoption or new thirty-minute sustained-load claim is made.
