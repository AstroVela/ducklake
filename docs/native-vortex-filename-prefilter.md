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

## Result

Formal A/B sampling is pending. No runtime optimization or performance benefit
is adopted before that fixed experiment and its declared gates finish.
