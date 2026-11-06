# Filename prefilter: path oracle and matched-object A/B

The [reference-call diagnostic](native-vortex-reference-diagnostics.md) measured
30.637 ms in path extraction/canonicalization/comparison for the large file-GC
input. This trial adds a necessary filename-suffix condition before the existing
full canonical-path equality check. It preserves each transaction's full metadata
query, historical/native/delete/index references, ownership, deletion intents,
ordinary-reader deferral and object/byte budgets. No reference cache or error
fallback is introduced.

## Candidate and independent oracle

The [helper](../test/vane/common/ducklake_filename_prefilter.hpp) handles remote
paths with their existing identity semantics before requesting a filesystem
separator. For local paths it obtains that filesystem's separator and inspects
the final raw component. Empty, `.` and `..` endings remain eligible for the
unchanged full check. An ordinary final component must be a suffix of the
canonical target; otherwise equality is impossible. A suffix match is only a
necessary condition, never sufficient proof of a reference. This conservative
form also preserves unusual separator/root-prefix behavior.

The [private patch](../test/vane/filename_prefilter.patch) inserts this condition
in the actual `HasFileReference` loop. Retained rows still execute the original
canonicalization/equality expression. The production implementation is unchanged
in this validation Draft; the trial supplies a concrete candidate for review.

The [compiled oracle](../test/vane/native_vortex_filename_oracle.cpp) compares the
helper plus original check against DuckDB's native canonicalizer. The
[independent Python model](../test/vane/test_vane_vortex_filename_oracle.py) derives
expected canonical paths without using the prefilter. The frozen case generator
produces **53,322 raw/target/separator cases per executable**, including relative
and absolute paths, aliases, repeated filenames, empty paths, `.`, `..`, trailing
and repeated separators, Unicode, binary strings and every recognized remote
prefix. It covers slash/backslash and multi-byte or overlapping separators.
Remote cases reject any unexpected filesystem lookup.

The oracle also calls baseline and candidate **real metadata managers** on two
restored native inputs. Each side gets **536 cases per input**, covering all
schema/table/file relative flags, data/delete-file references, live/historical
rows, aliases and same-name/different-directory controls. Metadata changes remain
inside a rolled-back native transaction. Error propagation and original logical,
data and index state are checked afterward. There are 2,144 metadata evaluations
across two variants and two inputs, with identical results.

During oracle development the independent model initially missed native
`StringUtil::Split` behavior for separator-only strings: `/` canonicalizes to
`//`. The model was corrected and a regression check added; the helper did not
change. The accepted full oracle completes in 25.400 seconds. No performance
samples were collected before the oracle passed.

## Protocol frozen before A/B sampling

Commit this protocol, trial builder, oracle, sampler and analysis checks before
the one formal run. The [builder](../test/vane/build_vortex_filename_trial.py)
requires exact original metadata-source SHA-256
`97a4b1cf7d1a00c7846a974fee77647649c8b18c7ae6ae254097b9d474ffe96e`.
It compiles complete baseline/candidate metadata objects with the same compiler,
flags, physical source path and source coordinates outside the edited function.
Both include the same helper header; baseline retains its original body. Both
objects occupy the **same explicit link position** before identical extension
archives. Benchmark, oracle and native source-guard binaries share these objects.
This removes the old archive-versus-explicit-object difference between sides.

Use explicit local provider directories and `DISABLE_EXTENSIONS_FOR_TEST=1` as
in [CI](../.github/workflows/VaneNativeVortex.yml). Require the six default base
extensions plus explicitly linked DuckLake/Vortex and reject cached source roots.
Record compiler/link commands and all source/header/patch/archive/binary hashes.
The original sealed producer and report remain unchanged, SHA-256
`168562ccee3d4cc66659d4bb8b18a5b891a33ecbdd2424f99736a22e0ec0f400` and
`48855d42a60fd423caf0df052e46bb95050003e61f0ac83b570f03be3f5cf042` respectively.

The [A/B sampler](../test/vane/test_vane_vortex_filename_ab.py) requires a passed
oracle tied to that exact build and input report. Formal sampling is one full
twelve-case matrix: histories 8/512/4,096, inline/materialized storage, and
expiry/file-unlink tasks. Fix **sixteen paired rounds per case**, four calls per
round: **768 primary calls**, 48 excluded warmups and 24 separate diagnostics.
Use ABBA on even rounds and BAAB on odd rounds; rotate/reverse case order as
before. No optional focus gate, adaptive count or rerun-until-pass is used.
Stop for execution, semantic or integrity failure.

Pin the fresh sampler to **CPUs 6,7,8,9,10,11**, verify topology/exact affinity and
restore its original mask. Native children inherit the mask. Keep `SET threads=2`,
existing SPFresh settings and inherited thread variables. Affinity reserves no
cores/memory and changes no governor/frequency. Record boundary load/topology and
read-only frequency observations. No build or other qualification overlaps
formal sampling, and no other writer uses the sealed corpus's canonical paths.

Every call restores fresh single-link images at their original paths. Require
the same snapshots/counters, ordinary-transaction deferral, exact progress,
retained source/index bytes and current typed business/ANN results. Keep the
two-object/64-MiB budget and keep-two/min-age-zero policy. Primary timings use
only the existing collector/phase timers; SQL callbacks stay in separate restored
diagnostics and never enter primary statistics.

## Predeclared decision

Only the two **materialized-history file-unlink** cases at 512 and 4,096 updates
are targets. The other ten cases are controls. Preserve the previous numerical
rules: each target needs at least **2% paired-round median improvement**, a
descriptive bootstrap 95% upper ratio below 1, and at least **80% winning rounds**
(thirteen of sixteen). Every control must have median regression **at most 2%**.
Otherwise withdraw the candidate. Reuse the existing paired-sum/median bootstrap,
10,000 resamples and seed 76077; do not substitute individual-sample p50 ratios.

Passing qualifies the private candidate for subsequent runtime review; it does
not automatically alter the production reference implementation. The four-case,
two-round CI smoke is functional only and cannot qualify a candidate. The
[analysis checks](../test/vane/test_vane_vortex_filename_stats.py) reject missing
or duplicate cases, short runs, target-gate failures and any control regression.
Preserve all tails and report exact per-case intervals, winning rounds and native
phases. The bootstrap is descriptive, without family-wide or serial-noise claims.
Earlier failed A/A precision studies remain unchanged; this trial does not prove
2% measurement equivalence or 30-minute/sustained-write stability. R4 stays open.

## Reproduce

After configuring the matching runtime with explicit local source roots and the
default-extension profile:

```sh
python -I test/vane/build_vortex_filename_trial.py \
    --build-dir /tmp/vane-native-vortex-runtime-20261030/build/python-release \
    --output-dir /tmp/filename-build
python -I test/vane/test_vane_vortex_filename_oracle.py \
    --build-report /tmp/filename-build/build.json \
    --inputs /tmp/ducklake-vortex-source-gc-full-v3-20261106/report.json \
    --cpus 6,7,8,9,10,11 --profile formal --output-dir /tmp/filename-oracle
python -I test/vane/test_vane_vortex_filename_ab.py \
    --build-report /tmp/filename-build/build.json \
    --oracle-report /tmp/filename-oracle/report.json \
    --producer-driver /tmp/ducklake-vortex-expiry-ab-baseline-20261106 \
    --inputs /tmp/ducklake-vortex-source-gc-full-v3-20261106/report.json \
    --cpus 6,7,8,9,10,11 --profile formal --output-dir /tmp/filename-ab
```

Use new output directories. Retain the build manifest, exact oracle streams,
per-call native JSON/logs, decision rule and final report. CI runs the same
oracle and a functional A/B smoke on its history-8 corpus and uploads artifacts.

## Result: withdraw the candidate

The one precommitted formal run completed all sixteen rounds in **687.653 s**:
768 primary calls, 48 excluded warmups and 24 independent diagnostics. All
semantic and sealed-input checks passed. Both targets met every gain criterion,
with **5.28% / 15.70% paired median reductions**, and each won **16/16 rounds**.
The 4,096-inline-expire control had a **3.84% median increase**, exceeding its
predeclared 2% ceiling. The automatic decision is therefore **`withdraw`**.
The candidate remains a private test patch; production `HasFileReference` stays
unchanged. No threshold, sample count, old A/A result or transaction protection
was relaxed and no additional performance run was used to change the decision.

Ratios use paired round sums (candidate/baseline). Intervals are descriptive
bootstrap 95% intervals. Mean and p95 columns show baseline / candidate in ms;
they are individual-call summaries, not the statistic used by the gates.

| History | Storage | Task | Role | Median ratio | 95% interval | Wins | Mean ms A / B | p95 ms A / B |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 8 | inline | expire | control | 1.009302 | 0.942151–1.041843 | 8/16 | 30.328 / 29.917 | 32.805 / 33.365 |
| 8 | inline | files | control | 0.997648 | 0.972919–1.009519 | 8/16 | 38.681 / 38.157 | 40.720 / 39.543 |
| 8 | file | expire | control | 0.999202 | 0.943749–1.065217 | 8/16 | 30.178 / 30.191 | 32.703 / 34.009 |
| 8 | file | files | control | 0.999041 | 0.988265–1.013979 | 8/16 | 38.317 / 38.241 | 39.392 / 40.135 |
| 512 | inline | expire | control | 0.993848 | 0.978464–1.022709 | 8/16 | 30.533 / 30.351 | 32.980 / 32.789 |
| 512 | inline | files | control | 0.987142 | 0.967563–1.008826 | 9/16 | 46.458 / 38.153 | 41.049 / 39.483 |
| 512 | file | expire | control | 0.993886 | 0.950592–1.021956 | 8/16 | 37.783 / 37.293 | 43.108 / 41.746 |
| 512 | file | files | target | 0.947206 | 0.917264–0.958316 | 16/16 | 44.927 / 42.156 | 47.576 / 44.039 |
| 4,096 | inline | expire | failed control | 1.038398 | 0.989556–1.085532 | 5/16 | 31.301 / 32.880 | 33.757 / 39.402 |
| 4,096 | inline | files | control | 0.984224 | 0.960060–1.007742 | 10/16 | 38.828 / 38.288 | 40.019 / 39.890 |
| 4,096 | file | expire | control | 1.004223 | 0.953263–1.038521 | 8/16 | 196.009 / 194.396 | 210.532 / 210.953 |
| 4,096 | file | files | target | 0.842996 | 0.822886–0.863876 | 16/16 | 82.835 / 69.575 | 86.998 / 71.978 |

Existing native phase means, baseline / candidate in ms:

| Case | Wait | Metadata | Expire | Artifact | Unlink |
| --- | --- | --- | --- | --- | --- |
| 8/inline/expire | 0.001 / 0.002 | 3.515 / 3.445 | 26.767 / 26.427 | 0.043 / 0.042 | 0.000 / 0.000 |
| 8/inline/files | 0.002 / 0.002 | 35.955 / 35.466 | 0.000 / 0.000 | 0.380 / 0.380 | 2.344 / 2.307 |
| 8/file/expire | 0.002 / 0.002 | 3.504 / 3.463 | 26.626 / 26.681 | 0.044 / 0.043 | 0.000 / 0.000 |
| 8/file/files | 0.002 / 0.002 | 35.584 / 35.496 | 0.000 / 0.000 | 0.381 / 0.381 | 2.349 / 2.360 |
| 512/inline/expire | 0.001 / 0.002 | 3.413 / 3.393 | 27.075 / 26.913 | 0.042 / 0.042 | 0.000 / 0.000 |
| 512/inline/files | 0.002 / 0.002 | 43.723 / 35.426 | 0.000 / 0.000 | 0.383 / 0.372 | 2.347 / 2.351 |
| 512/file/expire | 0.001 / 0.002 | 3.468 / 3.506 | 34.270 / 33.742 | 0.042 / 0.042 | 0.000 / 0.000 |
| 512/file/files | 0.002 / 0.002 | 42.208 / 39.399 | 0.000 / 0.000 | 0.388 / 0.373 | 2.328 / 2.381 |
| 4,096/inline/expire | 0.002 / 0.002 | 3.424 / 3.460 | 27.830 / 29.374 | 0.044 / 0.042 | 0.000 / 0.000 |
| 4,096/inline/files | 0.002 / 0.002 | 36.065 / 35.548 | 0.000 / 0.000 | 0.405 / 0.374 | 2.354 / 2.362 |
| 4,096/file/expire | 0.001 / 0.002 | 3.286 / 3.338 | 192.680 / 191.015 | 0.040 / 0.040 | 0.000 / 0.000 |
| 4,096/file/files | 0.002 / 0.002 | 80.143 / 66.891 | 0.000 / 0.000 | 0.367 / 0.367 | 2.321 / 2.314 |

The failed control's mean increase was concentrated in `expire` (27.830 →
29.374 ms); its independent diagnostic traces contained no full reference-path
query on either side. That is a useful negative control for a later measurement
study, not proof of either a code regression or measurement noise. Its bootstrap
interval includes 1. Matching object/link positions removes the earlier
archive-versus-explicit-object asymmetry but does not prove identical execution
layout or 2% precision. Do not waive this control or extend sampling after seeing
its result. The next narrow study should predeclare an identical-baseline A/A
for this control and attribute the unchanged expiry phases before another
runtime candidate is considered. R4 remains open; no new 30-minute run is claimed.

### Qualification and evidence

The accepted path oracle reports **53,322 cases per side**, each with 6,195
excluded paths, 10,848 true matches and 36,279 conservatively retained nonmatches.
The two complete native output streams have identical SHA-256
`73a8488927778e35dcb6ae5d280709c82cdaa001d3ce67aa2b09b2963db3c344`.
All 2,144 real metadata evaluations agree and preserve original logical/data/index
state. The separate source-retention proof exercises the actual candidate native
source driver through **38 cases**, including cancellation, crash recovery and
explicit resume, with **16 installed public query comparisons**, minimum recall
**1.0** (50.715 s). Its installed query side uses the existing frozen matched
provider environment; the older default environment lacks the public diagnostics
function. No compatibility branch was added for that older environment.

The CI-equivalent four-case smoke passed 56 native invocations (32 primary,
16 warmups, eight separate diagnostics); this is functional evidence only.
Eight new analysis/model checks and thirteen existing A/B/affinity checks passed.
The unchanged production runtime was reinstalled non-editably from the incremental
Release build. The complete Vane base suite passed **1,254 tests**, with **15
optional skips**, in its separate non-Ray and real-Ray processes. This qualifies
the base installation; candidate behavior is covered by the native oracle and
source-guard proof above. Formatting, copyleft, workflow parsing and 58 relative
proof-link checks passed. All 147 formal source/input/build identities and the
original five samplers were verified unchanged after sampling.

The protocol was committed as
`e7cf242b3673588df34d0c3adb6878c7cf5534e1` before formal sampling. Artifacts are
retained under these local paths; the SHA-256 values identify the exact reports:

| Artifact | Path | SHA-256 |
| --- | --- | --- |
| Matched build | `/tmp/ducklake-vortex-filename-build-20261106/build.json` | `771ab38f932b8323c12e086925764a55a3f34b27a17baa4991a430378b8c505c` |
| Accepted oracle | `/tmp/ducklake-vortex-filename-oracle-v2-20261106/report.json` | `617a8239a7a83b1147fe7f81baa1758e5760b66dcdf62a3ae79e1234394a9e7e` |
| Source guard proof | `/tmp/ducklake-vortex-filename-source-proof-qualified-20261106/report.json` | `bc8345f6cc0cae7ddf0b21bd6c3311f86f4db8d2278c27efe9ce2c690af6212d` |
| Functional A/B smoke | `/tmp/ducklake-vortex-filename-ab-smoke-20261106/report.json` | `187734bf624cb8a05d4c7383d2f89766b7389791d5bdf3a13e56a8a93630ece7` |
| Formal A/B | `/tmp/ducklake-vortex-filename-ab-formal-20261106/report.json` | `4795f9fa1ec5f671c936efdfe9747994e01ddecb8d8c35f6942aafcddcace5fa` |

Per-call native JSON, logs, paired ratios, phase timings, tails, topology, load
and read-only governor/frequency observations remain in those artifact directories.
The formal run inherited the unchanged thread environment, applied CPU 6–11 and
restored CPU 0–23. This pins an allowed set; it reserves no physical CPU or memory.
Boundary load was approximately 5.31/17.56/15.52 before and 10.94/7.24/9.99 after,
with `schedutil` throughout. These observations do not measure interference
during every call. No build or other qualification overlapped formal sampling.
The original five source-GC samplers, producer, sealed inputs, production source
and linked archives remain unchanged.
