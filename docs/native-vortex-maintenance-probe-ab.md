# Controlled A/B of maintenance policy/backlog queries

**Decision on 2026-11-03: withdraw query fusion.** The production optimizer is
restored byte-for-byte to #73 (`9819016e841ea79c8eea054415f4724fa2270e85`).
It retains the transaction-completion wakeup and the preceding two-statement
policy/backlog probe. The combined query remains only as a test benchmark
reference. No production selector, compatibility layer, cache or fallback is
introduced. All transaction guards, ownership checks, collector gates, fairness,
deletion intents and the two-object/64-MiB maintenance budget remain unchanged.

The [original candidate observation](native-vortex-maintenance-probe-query.md)
proved two-to-one query reduction but did not demonstrate a sustained gain.
This experiment compares the read-probe query shapes under matched inputs;
it does not infer cause from the two earlier independent thirty-minute traces.

## Fixed inputs and measured scope

The native executable prepares one DuckLake catalog per case and reuses that
same catalog for both query shapes. The preparation uses supported native
writes: 8, 512 or 4,096 updates to a separate one-row history table produce
14, 518 or 4,102 retained snapshots. The vector fixture has 128 two-dimensional
vectors. No optimizer worker runs during the benchmark.

Each scale covers two task inputs:

- `live`: two actual build records and one retired generation, with saved
  backlog counters zero/zero.
- `stale`: actual backlog zero/zero after guarded setup cleanup, with saved
  counters seven/nine.

Both have the same enabled maintenance policy, run identity, zero run count and
two-object/64-MiB budget. Source retention uses two snapshots and zero minimum
age. An ordinary reader is held throughout each measurement process; every
probe must report source deferral. Each sample validates the decoded policy,
budget, prior-backlog flag and live counts. Snapshot/history/task fingerprints
before and after primary and diagnostic processes must match. The timed path
contains no state writes or collection.

The test executable includes the private optimizer implementation to reuse its
native metadata connection, initialization and policy decoding without adding
a production testing API. The split shape uses the policy/backlog helpers;
the combined shape preserves the SELECT and decoding from #74 at
`80f4110ac45c555fbcd29e5dab52d0059983a339`. These are query-shape references,
not two complete revision binaries. The benchmark references are isolated from
the provider artifact.

The primary scope is one complete **read probe**: new metadata connection and
transaction, initialization/policy/backlog reads and decoding, source readiness,
commit and connection close. It excludes subsequent ownership publication,
collector work, autonomous worker scheduling and concurrent ingestion. Semantic
assertions and JSON output happen outside the timed scope. This directly tests
the proposed read-query reduction and does not measure an entire maintenance
invocation or ANN latency.

## Order, instrumentation and predeclared rule

Each case discards twelve alternating warmups, then runs sixteen paired rounds.
Even rounds use ABBA, odd rounds BAAB, with ten fresh-transaction probes per
block; A is split and B combined. Each round therefore measures twenty probes
per shape. There are 3,840 primary probes plus 72 excluded primary warmups.
Primary processes register no extra query callbacks.

After all primary measurements finish, separate processes run three diagnostic
rounds with three probes per block: 216 probes plus 72 excluded warmups.
Existing query callbacks verify two versus one relevant SELECTs and separate
binding/planning from the remaining execution/fetch interval. Query-begin to
post-prepare excludes parsing that happened before query-begin; the primary
policy/backlog scope includes parsing and initialization checks. Enabling the
post-prepare callback makes the diagnostic planner copy its AST, and no rebind
is requested. Diagnostic timings are explanatory and never enter the decision.

The runner writes `decision-rule.json` before measuring either shape. Retention
requires all medium/large task cases to have:

1. At least 2% improvement in the median paired-round complete-read-probe ratio.
2. A descriptive 95% paired bootstrap upper ratio below 1, using 10,000 resamples
   and fixed seed 74174.
3. At least thirteen winning rounds out of sixteen.

Small cases may not regress by more than 2% in median ratio. Otherwise the
decision is withdrawal. Ratios use equal-weight paired rounds, while the stage
means below are arithmetic means over samples; outliers can make them differ.
The intervals describe these sampled rounds, not a guarantee for other hosts.

## Completed matrix

All input-equivalence, budget, live-count and ordinary-transaction checks passed.
The complete experiment, including preparation and diagnostics, took 184.049
seconds. None of the four medium/large cases met the predeclared rule. The
medium stale case has an interval just below 1, but its 1.26% gain and twelve
winning rounds are insufficient. The result supports withdrawal under the
chosen rule; it does not establish that every query-fusion workload is slower.

| History updates | Task | Combined/split paired median | Descriptive 95% interval | Combined wins / 16 | Median gain |
| ---: | --- | ---: | --- | ---: | ---: |
| 8 | live | 1.001144 | 0.992646–1.015264 | 7 | -0.11% |
| 8 | stale | 0.998597 | 0.993428–1.005347 | 9 | +0.14% |
| 512 | live | 0.992215 | 0.981885–1.007151 | 10 | +0.78% |
| 512 | stale | 0.987383 | 0.980938–0.999376 | 12 | +1.26% |
| 4,096 | live | 0.993383 | 0.986044–1.002563 | 10 | +0.66% |
| 4,096 | stale | 0.998316 | 0.990908–1.031675 | 9 | +0.17% |

### Primary stage means

Values below are milliseconds per probe, split / combined. Metadata open
includes connection creation, transaction/snapshot acquisition and index-catalog
checks. Policy/backlog includes optimizer-catalog initialization checks and
policy decoding. Each probe opens a fresh transaction; no prepared plan or
policy result is cached across probes by the benchmark.

| History | Task | Metadata open | Policy/backlog | Source readiness | Commit | Close | Complete read probe |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 8 | live | 1.357 / 1.383 | 1.323 / 1.325 | 1.515 / 1.529 | 0.206 / 0.208 | 0.005 / 0.005 | 4.405 / 4.449 |
| 8 | stale | 1.340 / 1.349 | 1.309 / 1.291 | 1.498 / 1.503 | 0.204 / 0.205 | 0.004 / 0.004 | 4.356 / 4.352 |
| 512 | live | 1.354 / 1.348 | 1.307 / 1.291 | 1.523 / 1.538 | 0.203 / 0.206 | 0.004 / 0.004 | 4.392 / 4.388 |
| 512 | stale | 1.375 / 1.376 | 1.344 / 1.316 | 1.577 / 1.564 | 0.207 / 0.210 | 0.004 / 0.004 | 4.508 / 4.470 |
| 4,096 | live | 1.346 / 1.340 | 1.314 / 1.298 | 1.895 / 1.901 | 0.208 / 0.210 | 0.005 / 0.005 | 4.768 / 4.754 |
| 4,096 | stale | 1.381 / 1.386 | 1.342 / 1.347 | 1.938 / 1.978 | 0.212 / 0.208 | 0.005 / 0.004 | 4.879 / 4.924 |

### Separate instrumented diagnostic means

These are sums over the relevant statements per probe, in milliseconds,
split / combined. Every diagnostic probe observed exactly two / one statements.
The consistently smaller binding/planning component does not translate into a
material complete-read-probe improvement in this experiment.

| History | Task | Bind/plan | Execution/fetch |
| ---: | --- | ---: | ---: |
| 8 | live | 0.780 / 0.737 | 0.501 / 0.529 |
| 8 | stale | 0.748 / 0.700 | 0.464 / 0.529 |
| 512 | live | 0.773 / 0.739 | 0.496 / 0.560 |
| 512 | stale | 0.730 / 0.681 | 0.507 / 0.565 |
| 4,096 | live | 0.784 / 0.735 | 0.503 / 0.581 |
| 4,096 | stale | 0.767 / 0.718 | 0.540 / 0.605 |

## Reproduce and review the decision

Build the optional native benchmark with the same Vane/native SDK configuration
used for [native provider qualification](../.github/workflows/VaneNativeVortex.yml):

```sh
cmake --build "$SKBUILD_BUILD_DIR" --target ducklake_vortex_maintenance_probe_benchmark --parallel 2
env -u VORTEX_SPFRESH_NATIVE python -I test/vane/test_vane_vortex_maintenance_probe_benchmark.py \
    --driver "$SKBUILD_BUILD_DIR/ducklake_vortex_maintenance_probe_benchmark" \
    --output-dir /tmp/maintenance-probe-ab
python -I test/vane/test_vane_vortex_maintenance_probe_stats.py
```

CI runs the two-case functional smoke (`--histories 8 --rounds 2 --batch 2`) and
the four analysis tests. Its result is `smoke_only`; CI does not impose the
local performance threshold on a shared runner. Timing samples, diagnostic SQL,
rule and logs are retained as artifacts. Analysis tests reject linear-drift
false positives, tiny improvements and opposing rounds while accepting a
consistent gain.

The local full result and samples are under
`/tmp/ducklake-vortex-maintenance-ab-full-v1-20261103/`.
Aggregate report SHA-256:
`54a40fd170b16f8b35156e2870fa203d4b087f6f29a60c0f60bc824abcbfa4f7`.
Frozen measured executable SHA-256:
`154ae5d9e4b7720c77b988d315471238c2e2c4e1710230a408830ffd44e6b10e`.
The predeclared result was retained after withdrawal; the final executable only
reruns a functional smoke, without retuning the rule or selecting a new matrix.

## Final installed qualification after withdrawal

The final non-editably installed DuckLake candidate passed all basic, write,
source, keyed, automatic and stall-diagnostic groups. These include 38 native
source cases; the two probe cases now confirm the retained two SELECTs and the
same real/stale state transitions. Thirteen maintenance cases, 261 SQL assertion
blocks, eleven snapshot/timing oracle tests, five file-accounting tests and
four benchmark-analysis tests passed. The Vane base launcher passed 1,222
non-Ray and 32 Ray tests with fifteen optional skips in separate processes.
Final benchmark functional smoke, formatting, copyleft and workflow YAML parsing
passed. The production `src/` diff against #73 is empty.

Vane/Vortex wheel bytes are unchanged. Final DuckLake wheel SHA-256:
`124ea959e3aa2c559c342a0236fb6b962209cf81574da8e85ab4ec4a1a363275`.
The original thirty-minute #74 trace remains archived at `80f4110a`; this
update adds the controlled matrix and final functional qualification and makes
no new thirty-minute measurement claim for the restored implementation.
