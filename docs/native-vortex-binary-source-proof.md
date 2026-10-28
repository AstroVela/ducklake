# Native Vortex binary source handoff

This validation draft follows the [query-cost proof](native-vortex-query-cost-proof.md)
in [DuckLake #34](https://github.com/AstroVela/ducklake/pull/34). Native SQL passes
captured source bytes through a typed in-memory array, removing source hex
encoding/decoding and temporary source-file writes. Snapshot discovery, B/F/T
planning, category filtering and ranked business take keep their existing
contracts.

## Ownership and validation

`AcquireQuery` retains each complete encoded file as an owned C++ buffer,
alongside the existing same-transaction capture and named index metadata. Its
metadata JSON contains the capture and catalog record; it has no `sources`
field. Source ID, pointer and length travel separately through the synchronous
C ABI. Metadata is limited to 32 MiB, encoded sources to 64 MiB, and native
row/vector/T admission limits still apply.

Rust validates descriptor counts, null pointers, duplicate IDs and aggregate
lengths before copying into owned immutable `ByteBuffer` values. The binary
inventory must match every captured file ID exactly. It rechecks content
digests, row counts, field types and physical/schema identity before opening
the existing SPFresh generation. This is a bounded ownership copy, with C++
and Rust bytes briefly coexisting; it is not a zero-copy or RSS bound.

Once Vortex owns the verified sources, Rust invokes `source_pinned` and C++
releases its source buffers before filtering or ANN. C++ metadata, descriptor
storage and the callback remain alive until the call returns. Rust does not
dereference source pointers after ownership transfer. Failure before that
checkpoint leaves C++ ownership intact until stack cleanup; failure or
interruption afterwards releases Rust source and index ownership. The callback
catches C++ exceptions, and no unwind crosses the C ABI.

[Vortex #26](https://github.com/AstroVela/vortex/pull/26), pinned at
`664e8cb733c5fbaa2945239e806df5b22e6d190d`, adds
`LocalFileSource::from_buffers`. It shares verification with the filesystem
constructor and reuses its scan/take implementation, including caller order
and duplicates. Source URIs retain the catalog's physical identities and are
never opened by this constructor. Canonical path removal or replacement cannot
change an opened source.

The explicit exported proof envelopes continue to carry encoded bytes for
cross-process lifecycle tests. SQL selects only binary admission; malformed
metadata or buffers fail without probing another mode or reopening canonical
source paths. Private index artifact materialization and its temporary lease
remain, so this change does not remove all query writes or index-open work.

## Verification and measurement

The Vortex constructor passes 136 crate tests and three doctests. Its new
ownership test removes all source paths before constructing from buffers, drops
the caller's buffer map and verifies scan addresses plus ordered duplicate
take. Additional cases reject incomplete/unknown inventories, corrupt contents,
budgets, inconsistent embedded schemas, row counts and snapshot fingerprints.
Crate Clippy with warnings denied and nightly formatting pass.

The native DuckLake driver passes missing, duplicate, unknown, corrupt, null
and over-budget buffers through the actual C ABI while healthy canonical files
still exist. Each must fail explicitly. Reordering valid descriptors must work.
SQL cancellation now also interrupts at the ownership-transfer checkpoint.
Existing tests cover committed snapshot/T visibility, source/index publication
and GC during a held query, private ownership after canonical deletion,
nullable/Unicode rows, corruption, resource release and default-OFF behavior.

The [same cost driver](native-vortex-query-cost-proof.md#measurement-and-reproduction)
runs the 4,096/32,768-row B/F/T fixtures with and without category B. It checks
that binary handoff byte counts match native admission, C++ retains zero source
buffers after transfer, source staging is zero and projection gates still pass.
With a baseline driver compiled at #34's `51cfe37e940327657740a676ed5765adffb7df91`,
it requires identical ranked results and records first/warm latency, phases,
RSS and Linux read/write accounting. Timings remain informational. Both
variants use identical catalogs, index artifacts and query inputs; no OS cache
is reset.

### Local result (2026-10-29)

The explicit #34 comparison passed all 72 queries: 9 per variant, category
and fixture size, with the first query reported separately and eight warm
samples. Ranked results were identical across variants and every fixture
recall was 1.0. This is a local 8D fixture with two B files, 128 F rows, two T
rows and roughly 1 KiB business payloads; it is not a Turbopuff comparison or
a production-scale benchmark. Each case has its own process, each query
reopens sources and index, and the first query does not imply cold storage.

| B rows | Category | Warm median ms (#34 → binary) | Warm p95 ms | Process peak MiB |
| --- | --- | --- | --- | --- |
| 4,096 | ALL | 66.44 → 52.20 | 67.56 → 67.20 | 102.1 → 99.5 |
| 4,096 | B | 71.60 → 51.44 | 83.12 → 80.52 | 108.6 → 98.3 |
| 32,768 | ALL | 380.65 → 208.32 | 436.93 → 267.81 | 160.0 → 127.5 |
| 32,768 | B | 384.28 → 214.47 | 459.06 → 282.23 | 157.8 → 127.5 |

At 32,768 B rows, metadata shrank from 14,376,865 to 1,065,544 bytes and
6,655,620 encoded source bytes moved through three binary buffers. Native
ownership was released before search and source staging was zero. Warm
logical `wchar` fell from 9,252,868 to 2,597,248 bytes per query; the remaining
amount equals the private index artifact bytes. Kernel `write_bytes` fell
from 9,285,632 to 2,621,440, including filesystem allocation granularity.
Warm `rchar` was about 25.91 → 12.60 million bytes; both variants had zero
`read_bytes` in the warm OS cache. These counters do not establish cold I/O
throughput or eliminate the complete source read in native admission.

The common `before_open` → `source_pinned` interval fell from 165.61 → 68.28 ms
for ALL and 166.71 → 66.07 ms for B. The report retains its old
`decode_stage_and_source_open` label for cross-revision comparison; binary
SQL parses metadata and opens buffers in that interval. Its ownership copy
runs before `before_open`, within the reported `finish_capture` interval.
Index opening still took about 18–19 ms at the warm median. Timings and
memory remain informational; no latency threshold is asserted.

Evidence is in `/tmp/ducklake-binary-source-cost-20261029-final/report.json`
and the referenced per-case raw samples on this validation host (x86_64,
Linux 6.17.0-35-generic). Optimized driver SHA-256 is
`aef6a07eb179b23127af8e5a27db8365a1c5e983d90c5ef9fcf27669762f8144`;
baseline driver SHA-256 is
`856fee9d44c75e2e41f9d486dce5c3a3e76a7f8f6f6f7e57431249e3782d1ba0`.

Local ON/default-OFF qualification passed 86 base SQL cases and 3,582
assertions per mode, with 13 optional-extension skips. ON also passed seven
Vortex cases with 339 assertions; both formats passed restart recovery.
The first ON base-suite run timed out during parallel validation; its isolated
rerun passed in 61 seconds. All six lifecycle/process proofs passed. SQL
search exercised 54 native requests, 14 expected native rejections and seven
additional binary-input rejection cases, with nine recalls of 1.0, three
index builds and four source files reclaimed during GC. Rust workspace
Clippy (all targets/features, warnings denied), formatting and ON/OFF CMake
configuration passed. Registry dependency versions were unchanged.

Build the native shell, catalog/cost drivers and Rust probe as in #34, then run:

```sh
python3 -I test/vane/test_native_vortex_search.py \
  --duckdb build/vortex-validation/duckdb \
  --driver build/vortex-validation/ducklake_vortex_catalog_driver \
  --probe build/index-cargo/ci/ducklake-vortex-index-probe \
  --output-dir build/binary-source-search
python3 -I test/vane/test_native_vortex_cost.py \
  --duckdb build/vortex-validation/duckdb \
  --driver build/vortex-validation/ducklake_vortex_catalog_driver \
  --cost-driver build/vortex-validation/ducklake_vortex_cost_driver \
  --probe build/index-cargo/ci/ducklake-vortex-index-probe \
  --output-dir build/binary-source-cost
```

For the explicit comparison, add `--baseline-driver /absolute/baseline-driver`
and `--baseline-revision 51cfe37e940327657740a676ed5765adffb7df91` to the cost
command. CI retains ON/default-OFF checks, all process proofs and raw cost
samples. Local builds reuse qualified DuckDB v1.5.5 engine objects; clean
hosted builds remain an independent gate. Complete per-query source ownership,
index reopening, local DuckDB metadata and extension-instance GC/publication
guards remain the current scope. Out-of-core/multiprocess ownership and shared
index caching require separate work.
