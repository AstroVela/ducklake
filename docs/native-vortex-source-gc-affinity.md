# Fixed-process-affinity source-GC A/A

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
