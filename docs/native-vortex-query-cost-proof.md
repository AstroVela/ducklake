# Native Vortex query projections and cost

This validation draft follows the [native SQL search proof](native-vortex-sql-search-proof.md)
in [DuckLake #33](https://github.com/AstroVela/ducklake/pull/33). It removes full
business-row work from acquisition, category filtering and ANN reranking,
then measures the remaining costs against the preceding implementation. The
SQL signature, SPFresh generation, B/F/T eligibility and ranked result stay
the same.

## Query capture and column projections

`AcquireQuery` reads only `filename`, `file_row_number` and `rowid` through the
native snapshot scan. These identify visible physical rows and committed
inline addresses. It obtains the business columns of those T rows through the
existing `DuckLakeMetadataManager::ReadInlinedData` on the same metadata
transaction and snapshot. Every returned T address must match the visibility
set exactly. The source admission guard remains held through both reads and
the complete source handoff.

The SQL path never constructs the full business-row oracle. The explicit
`Acquire` proof interface still exports it for correctness comparisons and
index-building fixtures. Both paths share snapshot selection, field validation,
encoded source ownership, file identity checks, budgets and cleanup guards.
There is no retry or format detection between them.

Category filtering requests only the physical category column in bounded
batches. T categories come from their already owned business rows. B's final
ANN candidates are reranked using a vector-only take. After the B/F/T merge,
the existing ranked take obtains all business columns for the returned file
rows; T returns its owned row versions. The final take is limited to 64 MiB of
accounted business data. Its counter does not bound decoder peak allocations
or process RSS.

The SQL factory diagnostics record native capture rows and bytes, Rust phase
times, admitted source/artifact bytes and logical rows requested by the three
projection methods. A category request must take exactly the visible file-row
set through the category-only method. Full business takes and vector-only
reranking are bounded by the final k and ANN candidate count respectively.
These counters describe logical projections; encoded file reads and decoder
work can exceed the requested row/column set.

Every query still copies and hashes complete B/F files, serializes JSON/hex,
stages binary source bytes and reopens the immutable index into a private
lease. The change does not establish out-of-core ownership, a shared index
cache or a process-wide admission budget.

## Correctness and concurrency

The SQL process proof now pauses acquisition after visibility selection. A
writer commits a changed T payload and flushes T to F before the reader resumes
its business read. The held query must return the earlier T version; a fresh
prepared execution must see the committed F version. Another interruption at
that boundary verifies transaction and guard release.

The existing gates continue to check updates/deletes before top-k, category
filtering, nullable/Unicode business fields, prepared execution, source/index
replacement and GC, private lease cleanup, malformed input and corrupt artifacts.
The cost fixture independently compares ranked IDs, distances, categories and
payload byte lengths against an exact same-view oracle on every repetition.
When a baseline driver is supplied, its ranked results must match the optimized
driver exactly. Timing is informational; correctness and projection counts are
regression gates.

## Measurement and reproduction

Build `ducklake_vortex_cost_driver` with the native ON shell and catalog driver.
It calls the same execution factory as SQL, excluding outer SQL parsing and
binding. Each process attaches one persistent fixture and runs repeated
queries. The first query is reported separately; subsequent queries reopen
sources and SPFresh on every invocation. No OS cache is dropped, and the first
query is not evidence of cold storage performance.

The two default fixtures contain 4,096 and 32,768 eight-dimensional B vectors,
1,024-byte UTF-8 payloads, 128 appended F rows, an update/delete and committed
T rows. Each runs with no category predicate and with category B, which selects
roughly a quarter of B. Nine repetitions give one first query and eight warm
samples per case. The report includes median/p95 latency, phase times, current
and process high-water RSS, page faults and Linux `/proc/self/io` counters.
`rchar`/`wchar` count logical read/write calls; `read_bytes`/`write_bytes` describe
kernel storage accounting and can be zero with cached data. Setup and index
building occur outside the measured process.

```sh
python3 -I test/vane/test_native_vortex_cost.py \
  --duckdb build/vortex-validation/duckdb \
  --driver build/vortex-validation/ducklake_vortex_catalog_driver \
  --cost-driver build/vortex-validation/ducklake_vortex_cost_driver \
  --probe build/index-cargo/ci/ducklake-vortex-index-probe \
  --output-dir build/query-cost
```

For a local before/after comparison, compile this same measurement driver
against the preceding implementation and its Rust archive, then add
`--baseline-driver /absolute/baseline-driver --baseline-revision COMMIT`.
Both drivers use the same catalog, immutable artifacts and query inputs.
Run order reverses for category B to reduce a fixed ordering bias. The report
records both driver hashes and the explicit baseline revision. CI runs the
optimized projection gates and uploads raw samples; a baseline is optional
and is never inferred from a failed command.

## Local results

The local comparison used the preceding implementation at
`c65f90cabfa5922b64f649ffe537b8c0bd31d4d7`, the same qualified SPFresh SDK and
DuckDB v1.5.5 engine objects, Linux x86-64, and eight warm samples per case.
There was no CPU pinning or storage-cache reset. Latencies measure the native
factory, including capture, Rust execution, cleanup and typed result copying.

| B rows | Predicate | Warm median ms, before → after | Warm p95 ms, before → after | Peak RSS MiB, before → after |
| --- | --- | --- | --- | --- |
| 4,096 | All | 97.87 → 71.11 | 154.25 → 92.09 | 134.3 → 101.3 |
| 4,096 | Category B | 113.61 → 69.16 | 180.82 → 74.65 | 133.7 → 105.7 |
| 32,768 | All | 671.96 → 389.27 | 805.30 → 443.02 | 354.7 → 153.5 |
| 32,768 | Category B | 847.20 → 422.76 | 898.89 → 577.75 | 355.2 → 152.5 |

All 72 queries passed the oracle recall gate at 1.0, and baseline/optimized
ranked results matched exactly. Optimized native capture materialized two T
business rows and zero oracle bytes. Without a predicate it requested zero
category rows; with category B it requested 4,222 and 32,894 visible file rows
through the category-only take. Full business takes requested nine or ten file
rows after merging, and vector-only reranking requested ten ANN candidates.

For the larger unfiltered case, median visibility/business capture fell from
217.86 to 41.47 ms, and capture finalization fell from 117.61 to 26.53 ms.
For its category-filtered case, the pre-ANN filter interval fell from 157.66 to
63.12 ms. These intervals include the corresponding visibility/RowFilter work;
they are not isolated decoder microbenchmarks.

Complete source copying and decode/staging remain substantial: the larger
optimized case still admits 6,655,620 encoded bytes in a 14,376,853-byte envelope,
with roughly 169–173 ms median decode/staging/source-open time and 18–20 ms
index-open time. Warm `rchar` fell from about 32.55 to 25.91 million bytes;
`read_bytes` was zero in both variants, so this run demonstrates no reduction
in physical storage reads. Warm write-call and kernel-write accounting remained
unchanged at about 9.25 and 9.29 million bytes respectively. Eliminating whole-file
ownership and the JSON/hex transport is separate work.

The expanded SQL proof passed nine fixture recall checks at 1.0, 51 native
requests with 13 expected rejections, and actual reclamation of four B/F files.
It includes the visibility-to-T-read commit/flush window and six interruption
checkpoints. All six existing process proofs, ON/default-OFF native gates,
seven Vortex SQL cases with 339 assertions, and 86 base SQL cases with 3,582
assertions per mode passed. Each base gate reported 13 optional-extension
skips. Rust Clippy with warnings denied, Rust formatting, `make format-fix`
and ON/default-OFF CMake configuration also passed. Local builds reused engine
objects; hosted clean builds remain an independent validation gate.

## Clean build regression

The first hosted #33 ON configuration failed because Cargo discovered the
parent DuckLake workspace for the downloaded Vortex wrapper under `build/_deps`.
The reused local wrapper had been outside that workspace. The root workspace
now explicitly excludes `build`; this fix is also pushed to #33 at
`c65f90cabfa5922b64f649ffe537b8c0bd31d4d7`. Cargo metadata reproduces the failure
before the exclusion and succeeds after it. Local ON CMake configuration now
uses a wrapper inside the repository build directory to exercise that layout.

The next [binary-source proof](native-vortex-binary-source-proof.md) replaces
the SQL source-byte envelope and staging files with owned in-memory buffers,
then measures the change against this implementation.
