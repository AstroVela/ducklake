# Deterministic capacity admission boundaries

This R5 validation follows the [reference/WAL lifecycle contract](native-vortex-reference-wal-lifecycle.md).
Use CPU 6–11, DuckDB threads=2, unchanged SPFresh settings and the existing limits.
Stable-load A/A, performance A/B and sustained-load qualification remain deferred.

## Accounting model

Limits are conjunctive: satisfying one does not grant admission past another.
The [independent model](../test/vane/vortex_capacity_model.py) derives row/byte
charges from the documented contracts, rather than reading production constants.

| Domain | Existing bound | Accounting |
| --- | --- | --- |
| WriteBatch | 1–1,000 operations, 4 MiB | Canonical request bytes; UTF-8 lengths, FLOAT32 bits, NULL markers and mutation delete keys |
| Visible T | Configured rows and at most 4 MiB | Current inline rows; each charges `4*d + 64 + UTF8(category) + UTF8(payload)` |
| Pending inline history | 64 MiB and remaining physical/vector row slots | All committed inline versions not yet materialized, including logically ended rows |
| Physical/vector rows | `min(100000, floor(64 MiB / (4*d)))` | Current source-file physical rows plus retained pending inline versions; logical deletion alone does not remove these inputs |
| F | Configured rows and at most 64 MiB | Unindexed current physical files and their encoded file sizes |
| Physical source bytes | 64 MiB | Encoded sizes of all current source files, including B-covered files |
| Index artifacts | 64 MiB per artifact and 64 MiB per opened plan | Authenticated opaque objects; aggregate retained query artifacts are a separate reservation from source bytes |
| Manifest | 1 MiB | Sealed-store manifest and canonical native catalog envelope have separate checks |
| Segments / receipts | 3 B segments; 100,000 catalog receipts | Counts do not multiply the source/vector/artifact limits |

Logical T capacity can be reused by replacement/deletion while historical inline
versions still consume pending capacity. Flush changes their representation to
physical files. An atomic rewrite can remove old inputs from the current file
inventory while old snapshots still retain their files. Existing GC protections
remain independent; admission is not a total disk/RSS bound.

## Acceptance

1. Check canonical byte/operation boundaries with valid requests, exact digests,
   Unicode/NULL/signed-zero inputs, stable current receipts and failed re-use.
2. Use real files at the physical/vector limits for dimensions 1, 167, 168 and
   4,096; reject the next row without an ACK, row, receipt or index change. These
   straddle the row/vector crossover. Logical deletion must not free a physical
   slot; an explicit sparse B→F rewrite must remove its selected old input before
   a new row is admitted. Ordinary readers still prevent unsafe source GC.
3. Fill the real 64 MiB pending-inline boundary using fixed-size historical
   versions. Reject further growth even when visible T is small or empty; flush
   materializes history without erasing its physical row charge.
4. Validate configured T/F/source byte budgets at real measured file sizes and
   read leases at actual authenticated artifact sums. Use true generic local-store
   objects at 64 MiB artifact and 1 MiB manifest boundaries. These opaque-store
   objects are not claimed to be usable ANN indexes. Native envelope parsing is
   validated separately from filesystem publication.
5. Exercise the last admission slot concurrently, cancellation, native commit
   conflict, budget reconfiguration rollback, actual SIGKILL before/after commit
   and after rejection. Reopen and compare typed business state, all 13 Vortex
   metadata tables, receipts, snapshots, budgets and source identities.

No compatibility or fallback is introduced. Missing schema, unsupported types
and over-budget input fail explicitly. Performance, representative large-ANN
quality, indefinite resource bounds and remote ownership are separate work.

## Reproduction and evidence layers

Build the existing native write driver and the test-only Rust probe with the
pinned dependency closure, then use the non-editable installed Vane/provider
pair. The Rust command uses profile `ci`, target `x86_64-unknown-linux-gnu`, and
Rust 1.91.0. It adds no dependencies or production entry points.

```bash
python -I test/vane/test_vane_vortex_capacity_model.py
VANE_RUNNER=local-fast python -I test/vane/test_vane_vortex_capacity.py \
  --driver /path/to/ducklake_vortex_write_driver \
  --probe /path/to/ducklake-vortex-index-probe \
  --cpus 6,7,8,9,10,11 --output-dir /new/private/capacity-proof
```

The coordinator starts fresh subprocesses serially for the real-data cases,
records CPU affinity, inherited thread settings and executable/source hashes,
and restores its original affinity. Timeouts detect hung fixtures; elapsed
seconds are observations only. CI selects an available CPU rather than assuming
the local machine's CPU IDs.

Each write rejection compares all 13 Vortex metadata tables, snapshot identities,
a typed business aggregate and hashes of all fixture source/delete/index files.
Independent generated row ranges and typed values check accepted state. Native
recovery additionally compares every business row exactly and uses a separate
process for each reopen. Generated provider caches are removed only after their
fixture process has closed; database, source, index and recovery evidence remain.

The four physical/vector cases each build a genuine 65-row B first. This keeps
F below its own row quota when the global guard is exercised. The 4,096D case
rewrites that B with 64 surviving rows and proves current admission reuse while
an ordinary reader retains the old file. GC waits for that reader independently.

Source-byte checks use authentic encoded file sizes with configurable limits
at `S-1`, `S`, `S+1`; they do not claim a real Vortex file of exactly 64 MiB.
Genuine three-B query leases likewise use authenticated artifact sums at
`A-1`, `A`, `A+1`, always within the existing 64 MiB plan ceiling. The separate
opaque-store fixture exercises the true 64 MiB artifact and 1 MiB store-manifest
limits. Native envelope fixtures test canonical declaration parsing, including
1 MiB manifests with 4,096 short paths, without publishing invented files or ANN
generations. These scopes must not be combined into a large-ANN quality claim.

The receipt fixture reuses the existing epoch proof: one real no-op deletion
receipt supplies a fixed 100,000-row catalog. This validates admission count,
retirement and stale epochs; it is not a trace of 100,000 business API writes.

## Result

The final installed proof passed on 2026-11-06, based on Draft #86
(`db72e6edff094461c73e7bdefcc042693e7a58f4`). Production `src/`, dependency pins,
thread configuration, transaction protection and hard limits are unchanged.
No compatibility layer or fallback was added.

| Layer | Passed coverage |
| --- | --- |
| Independent model | 7 checks for precomputed crossovers, canonical FLOAT32/NULL/UTF-8 identity and exact envelope sizes |
| Real-data boundary suites | 11: four physical/vector dimensions; append and mutation batches; visible T; retained pending history; F/source limits; genuine three-B leases; catalog receipts |
| Native envelope parser | 31 cases: single-B and aggregate three-B row/vector boundaries in four dimensions, segment counts 0/2/3/4, canonical 1 MiB minus/equal/plus one byte |
| Generic local store | 7 cases: real 64 MiB artifacts and 1 MiB manifests minus/equal/plus one byte, plus an over-budget aggregate lease with no partial staging |
| Native transactions and reopen | Exact capacity, cancellation, concurrent metadata commit and two writers competing for the final slot |
| Actual SIGKILL and recovery | Before commit, after commit and after rejection; positive native WAL at all three barriers; full typed state and all 13 Vortex tables compared on recovery |

The real vector inputs reached these existing limits; these are uncompressed
vector byte counts, separate from encoded source file sizes:

| Dimension | Physical plus pending row limit | Vector bytes at the limit |
| --- | --- | --- |
| 1 | 100,000 | 400,000 |
| 167 | 100,000 | 66,800,000 |
| 168 | 99,864 | 67,108,608 |
| 4,096 | 4,096 | 67,108,864 |

All four accepted the final row, rejected the next row without partial data,
receipt or file changes, retained that boundary after a logical delete, and
preserved it on reopen. The 4,096D sparse rewrite reused the current slot while
its old ordinary reader still blocked source GC.

Both APIs accepted canonical batches of 4,194,303 and 4,194,304 bytes and rejected
4,194,305 bytes; accepted receipts matched the independent digest, typed values
survived and retries did not reapply operations. Visible T reused its two-row,
154-byte quota while four stored versions still charged 308 pending bytes.
Sixty-four retained 1 MiB versions reached the real 64 MiB pending limit, even
though only one version was visible; deletion alone did not permit growth.
Flush materialized all 64 physical versions, preserved the old reader and
allowed the next current inline version.

The eight-row source fixture measured 5,044 encoded bytes. The final three-B
fixture measured 18,980 source bytes and 47,991 authenticated artifact bytes;
limits at the measured sum minus one rejected, while equal/plus one succeeded.
The accepted leases executed typed ANN queries, rejected an extra view, and
returned all reservations on close. A fourth B failed publication; explicit
bounded build cleanup restored the exact pre-attempt metadata and files.
The receipt fixture passed seven existing public epoch groups at the unchanged
100,000-record catalog limit.

The last-slot race committed exactly one winner and one receipt. Native commit
conflict was the concrete duplicate `snapshot_id` primary-key error with retry
count zero; cancellation/conflict preserved committed state before an explicit
fresh-transaction retry. All seven native/recovery cases replayed the old seed
receipt without resurrecting old values. Advancing the epoch retired receipts
but did not free data capacity; old epochs remained rejected.

| Evidence | Local path | SHA-256 |
| --- | --- | --- |
| Final capacity report | `/tmp/ducklake-vortex-capacity-proof-v2-20261106/report.json` | `461dd21972a5f8756228b380db5fa3c5111710101e5635b8c38de71f38067edc` |
| Native write driver | `/tmp/vane-native-vortex-runtime-20261030/build/python-release/ducklake_vortex_write_driver` | `9797d9b41f7293c75f58046ecdcbdc809bf7c776c4f5612ce61d256c0855ecb6` |
| Test Rust probe | `/tmp/vane-native-vortex-runtime-20261030/build/python-release/cargo/build/x86_64-unknown-linux-gnu/ci/ducklake-vortex-index-probe` | `c7cbbc2ab88e61b99a53b4fe8a2302bd04a350578d79508d79b93eda0a52d11f` |
| Installed Vane native | `/tmp/ducklake-vane-capacity-env-20261106/lib/python3.12/site-packages/vane/_native.cpython-312-x86_64-linux-gnu.so` | `8ca20bd4c8a4203f239ff6d785c45575241d3b2c8d1f8e9fc184d90de1d54876` |

Related installed-chain regressions passed all five groups: basic B/F/T,
managed writes, source retention, request epochs and reference/WAL lifecycle.
They include 77 native/recovery cases and 76 recorded fixture-oracle comparisons;
existing recall gates were preserved. The required Vane base launcher ran
non-Ray and Ray in separate processes: 1,222 + 32 passed, 11 + 4 optional-dependency
skips. Formatting, Rustfmt, YAML/file links, executed-source identity and copyleft
checks passed. No stable-load performance or sustained-load test was run.
The regression report is `/tmp/ducklake-vortex-capacity-regressions-20261106/report.json`
with SHA-256 `6b2547667d3642fe8f952b259b15a04bcbed290b65e6b21fa8c7e55bc7e3bd14`.

Matching signed, test-only provider wheels and their hashes are recorded in
`/tmp/ducklake-vortex-capacity-wheels-final-20261106/report.json`. They were
installed explicitly from the frozen local closure; this is not clean-environment
release qualification. CI retains the capacity reports, source/executable
identities, per-case logs and recovery snapshots. Remote CI status belongs to
the Draft PR.

Stable-load A/A, performance A/B, sustained throughput/latency, representative
large-ANN quality and total disk/RSS bounds remain deferred. The next independent
R5 work is explicit ownership and cleanup of crash-left query scratch, followed
by table-UUID metadata lifecycle.
