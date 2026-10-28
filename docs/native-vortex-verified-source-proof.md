# Native Vortex source verification and reader reuse

This validation draft follows [DuckLake #35](https://github.com/AstroVela/ducklake/pull/35)
and the [binary-source proof](native-vortex-binary-source-proof.md). The native
source capture and binary ownership copy stay the same. The Rust adapter now
verifies and opens each encoded file once, then reuses that reader when
constructing its physical source.

The verified-reader API is in [Vortex #27](https://github.com/AstroVela/vortex/pull/27),
pinned at `531fed6588d1b1b0492f1a50cad42179ce68faa5` in the query library,
probe, lockfile and native validation workflow.

## Ownership and binding

Previously, `PhysicalSource` hashed source contents and opened a Vortex reader
to inspect dtype and row count. `LocalFileSource::from_buffers` then repeated
the content hash and opened another reader for the same bytes.

`VerifiedFile::from_buffer` checks the expected absolute source identity,
lowercase SHA-256 digest, encoded byte budget and embedded physical row count.
It owns the immutable Vortex reader and retains the verified descriptor in
private fields. The adapter inspects its dtype and validates DuckLake vector
and business field IDs/types through that existing reader.

`LocalFileSource::from_verified` consumes those objects. It checks the complete
inventory, every descriptor field, expected dtype, schema fingerprint and
aggregate byte budget before using the already opened readers. It accepts no
unchecked reader or caller assertion that bytes were verified. Assembly does
not hash encoded contents, parse footers again or access canonical paths.
The SQL constructor requires embedded dtype and has no alternate open mode.

Native capture still hashes file contents to establish its expected identity;
Rust still verifies the transferred bytes against that identity. Those checks
span the native/Rust ownership boundary and remain. Metadata/capture digests,
snapshot validation, B/F/T visibility, category prefiltering, ranked take,
source/index budgets, ownership-transfer cancellation and resource cleanup
also remain. Full source capture, one bounded binary copy and private index
artifact materialization are unchanged.

## Validation and reproduction

Vortex tests cover verified-reader ownership after canonical path removal,
scan/take order and duplicates, missing/unknown inventories, complete identity
rebinding, self-consistent wrong schemas, row-count mismatches, corrupt bytes
and aggregate budget enforcement. Existing file and buffer constructors use
the same verified-reader assembly and retain their existing contracts.

Run the native ON/OFF gates and all six lifecycle/process proofs as described
in #35. Build the Rust query library and probe together from the locked
workspace before linking the native drivers. C++ source and the C ABI are
unchanged; local validation reuses #35's qualified C++ objects, relinks ON
binaries with the new Rust archive, and reuses the unchanged OFF binaries.
Clean hosted compilation remains an independent gate.

The existing cost driver compares identical 4,096/32,768-row B/F/T fixtures,
with and without category B, against #35's
`f0228744923eaec738a76aa94578ed4831d3b833`. Supply its driver and revision:

```sh
python3 -I test/vane/test_native_vortex_cost.py \
  --duckdb build/vortex-validation/duckdb \
  --driver build/vortex-validation/ducklake_vortex_catalog_driver \
  --cost-driver build/vortex-validation/ducklake_vortex_cost_driver \
  --probe build/index-cargo/ci/ducklake-vortex-index-probe \
  --baseline-driver /absolute/35-cost-driver \
  --baseline-revision f0228744923eaec738a76aa94578ed4831d3b833 \
  --output-dir build/verified-source-cost
```

Ranked results must match exactly and every query must meet the same-view
oracle recall threshold. Retained native buffers, staging bytes and projection
counts keep their existing gates. Report latency, phase times, peak/current
RSS and logical/kernel I/O; timings remain informational. Each process reports
one first query and eight warm queries, without resetting the OS cache.

## Local results (2026-10-29)

All 72 comparison queries passed with identical ranked results and fixture
recall 1.0. The two variants used the same catalogs, immutable artifacts and
query inputs; run order reversed for category B. The fixtures contain 8D
vectors, roughly 1 KiB business payloads, two B files, 128 F rows and two T
rows. Each case has its own process, with sources and index reopened on every
query. There was no CPU pinning or storage-cache reset.

| B rows | Category | Warm median ms (#35 → verified) | Warm p95 ms | Process peak MiB |
| --- | --- | --- | --- | --- |
| 4,096 | ALL | 47.14 → 43.22 | 49.61 → 44.36 | 92.7 → 86.5 |
| 4,096 | B | 48.91 → 45.40 | 50.68 → 66.35 | 96.7 → 88.8 |
| 32,768 | ALL | 196.86 → 178.03 | 240.65 → 257.25 | 127.2 → 127.2 |
| 32,768 | B | 221.13 → 195.79 | 294.19 → 285.70 | 127.2 → 127.2 |

At 32,768 B rows, the common `before_open` → `source_pinned` interval fell
from 67.92 → 47.90 ms for ALL and 67.70 → 46.64 ms for B. This includes
metadata parsing and source opening, not only the content hash. The report's
historical `decode_stage_and_source_open` label is retained for comparison.
Index opening remained about 18–19 ms. Overall warm medians improved about
9.6% and 11.5%; first-query latency and p95 did not improve consistently, so
this small local fixture does not establish a tail-latency improvement.

Both larger cases still retained 6,655,620 encoded source bytes and moved them
through the same bounded ownership copy. Source staging and retained C++
source buffers were zero after transfer. Logical writes stayed at 2,597,252
bytes/query, equal to the private index artifacts; kernel write accounting
stayed at 2,621,440 bytes. Warm logical reads were about 12.60 million bytes
and kernel read accounting was zero for both larger cases. This removes CPU
work and duplicate readers, without establishing lower source I/O or a smaller
memory bound. RSS and timing remain informational.

Evidence is in `/tmp/ducklake-verified-source-cost-20261029-final/report.json`
and its per-case raw samples on the validation host (x86_64, Linux
6.17.0-35-generic). Optimized driver SHA-256 is
`4a409f3a2c1b464103458a533a51f3fa069c7342778a9cda28b83946edac8715`;
baseline SHA-256 is
`aef6a07eb179b23127af8e5a27db8365a1c5e983d90c5ef9fcf27669762f8144`.

Vortex passed all 140 crate tests and three doctests, plus all-target/all-feature
Clippy with warnings denied and nightly formatting. DuckLake's shared query
workspace build, Clippy, formatting and diff checks passed; registry dependency
versions were unchanged. ON passed seven Vortex SQL cases / 339 assertions
and Vortex recovery. ON and the unchanged OFF binaries each passed 86 base SQL
cases / 3,582 assertions with 13 optional-extension skips, plus Parquet recovery;
OFF exposed none of the five native Vortex functions.

All six process proofs passed against the new probe/library. SQL search
retained 54 native requests, 14 expected rejections, seven additional binary
input rejection cases, nine fixture recalls of 1.0, three index builds and
four source files reclaimed during GC. Admission and mixed-proof assertions
now expect the checksum error from the central Vortex validator; their
corrupt-input rejection and ownership checks remain in place. Publication,
T-to-F flush, prepared snapshots, cancellation and held reads across actual
source/index removal retain the #35 coverage. Automatic background indexing,
out-of-core ownership and distributed ANN remain separate work.

The next [native build proof](native-vortex-build-proof.md) connects the existing
builder to SQL and validates F-to-B promotion with concurrent queries and writes.
