# Native DuckLake read-view admission

This validation draft extends the [B/F/T mixed query proof](native-vortex-mixed-proof.md)
in [DuckLake #30](https://github.com/AstroVela/ducklake/pull/30). It acquires the
source view through native DuckLake transactions while other connections commit
and perform maintenance. The existing Vortex SPFresh revision and query plan
remain the same.

## Acquisition contract

The experimental ON build exposes:

```sql
SELECT snapshot_id, source_bytes, visible_rows, inline_rows, admission, rows
FROM ducklake_vortex_read_view('lake', 'main', 'items', 8, 67108864, 100000);
```

The arguments are catalog, schema, table, vector dimension, encoded-source byte
budget and row budget. The current proof requires unencrypted local Vortex
files, DuckDB metadata and the `id BIGINT`, `embedding FLOAT[]`, `category VARCHAR`,
`payload VARCHAR` projection. Business fields can be nullable. The function is
available only with `DUCKLAKE_ENABLE_VORTEX=ON`.

`DuckLakeVortexReadView::Acquire` starts an owned transaction on a dedicated idle
connection. It uses `DuckLakeTransaction::GetSnapshot`, the native catalog field
IDs and metadata file inventory, then copies the encoded B/F file bytes. A scan
on that same connection and explicit snapshot supplies visible physical
addresses and committed T rows. The native deletion and snapshot filters remain
responsible for visibility. The scan streams its output; it also emits business
rows for the fixture oracle. The reader commits only after the complete view is
owned. The caller's uncommitted writes are outside this committed-view contract.

The returned `admission` is a self-contained, versioned proof envelope containing
the mixed capture and hexadecimal encoded source bytes. It preserves original
file IDs, URIs, row counts and SHA-256 identities. `rows` is the same-view oracle.
Neither the test script nor a subsequent `latest` query assembles these records.
Once returned, the envelope owns its bytes independently of the acquisition
transaction and canonical source paths.

## Admission and physical cleanup

A shared admission guard covers snapshot lookup, inventory acquisition, source
reads and visible-row capture. Both old-file and orphan cleanup acquire the
corresponding exclusive guard around physical removal. Writers, flush, rewrite
and snapshot expiration can progress while the reader acquires its view; file
removal waits until the source handoff is complete.

The guard is local to the loaded extension instance and covers all its catalogs
and aliases. This deliberately bounded native proof does not establish a
cross-process/cloud GC protocol. The DuckDB metadata fixture runs multiple
connections in one database instance. It holds the reader at deterministic
observation points while allowing writer commits and maintenance to proceed;
it does not pause the writer until acquisition finishes.

The factory rolls back on failure and releases the guard and partial source
buffers. Cancellation is checked between acquisition stages and rows, and the
streaming scan uses DuckDB interruption. The proof checks cancellation after a
partial pin and a separate injected partial-pin failure, then verifies that an
exclusive guard and a fresh transaction can be obtained.

The concurrent writer test exposed an existing native metadata-cache bug:
catalog-wide positive deletion-table existence could be reused by an older
metadata transaction in which that table did not exist. Native DuckDB metadata
now uses the current metadata connection's catalog lookup and its transaction
local positive cache. It does not catch a missing-table query and retry against
a newer snapshot. A separate Parquet SQL regression exercises this independently
of the optional Vortex build.

## Consumer ownership and limits

The Rust worker explicitly selects `open_admitted`; the file builder selects
`build-admitted-file-index`. These consume only the native envelope. Decoding
requires a complete, unique source inventory and bounded lowercase hexadecimal
bytes. The existing physical adapter rechecks every content digest, row count,
dtype and vector field. Temporary source files are removed once Vortex owns the
verified encoded buffers. Original URIs still define index identity; private
staging paths serve only as the transport for those exact bytes.

Search, B/F/T prefilters, bounded top-k merge, private index leases and ranked
business take reuse #30. The worker never tries canonical paths if admitted
bytes fail verification. Index/spec/source failures propagate without rebuilding,
format probing, changing snapshots or moving B to scan. Producer and consumer
copies have independent lifetimes; close/EOF release their respective buffers
and artifact leases.

Native admission accepts at most 64 MiB of encoded source and 100,000 rows, with
an additional 64 MiB vector-work bound. It limits accounted business data to
64 MiB, accounted T data to 4 MiB and the serialized envelope to 160 MiB. Business
accounting includes vector components, strings and a fixed per-row allowance.
These are prototype admission limits, not RSS or peak-memory limits: JSON/hex
serialization, decoded batches and native graphs also allocate memory. The
worker retains its existing aggregate count/source/artifact/inline limits.

## Validation and reproduction

Build the experimental native ON shell, SQL runner and concurrency driver:

```sh
cmake --build build/vortex-validation --target duckdb unittest ducklake_vortex_view_driver
```

Build the Rust probe with the [index-proof instructions](native-vortex-index-proof.md#build-and-run),
then run:

```sh
python3 -I test/vane/test_native_vortex_admission.py \
  --duckdb build/vortex-validation/duckdb \
  --driver build/vortex-validation/ducklake_vortex_view_driver \
  --probe build/index-cargo/ci/ducklake-vortex-index-probe \
  --output-dir build/admission-proof
```

The native driver creates two B files with 4,096 vectors, then adds F and commits
updates/deletes and T versions on a separate connection while acquisition is
paused after snapshot selection. The old acquisition must retain its earlier
versions; a fresh acquisition must return the newer B/F/T state.

Another acquisition pauses after inventory lookup. A writer flushes, rewrites,
merges and expires snapshots; a third connection enumerates retired files and
attempts cleanup. Cleanup must wait for acquisition, then actually remove the
original B/F files. The consumer imports that envelope after canonical GC and
must return the frozen pre-maintenance B/F/T results. A fresh post-maintenance
view rejects the old B reference and uses a separately, explicitly built index.

Further checks cover malformed/missing/duplicate/corrupt source bytes, source
budgets, wrong command mode, native rollback/cancellation, uncommitted writer
isolation and released producer state. Producer envelopes and canonical index
generations are removed after consumer open. Previously unqueried vectors and
ordered duplicate B/F/T business fetch must still work. Recall remains a
separate fixture gate; approximate underfill does not trigger scan.

CI runs the native ON/default-OFF gates, both new SQL regressions and all four
index/read-view process proofs. This remains a validation interface with bounded
in-memory source ownership. Catalog index publication, background indexing,
out-of-core ownership and a production SQL vector-search operator are later
integration steps.

The local native admission run passed seven fixture queries with recall 1.0,
40 consumer requests with seven expected rejections, and four native
budget/failure/cancellation rejections. Actual cleanup removed three B/F files.
Both producer counters and consumer reservations returned to zero on release.
The two index builds are explicit: one for the original B files and one for
the post-compaction layout.

The ON native gate passed five Vortex SQL cases with 308 assertions, including
the new 28-assertion acquisition case, plus Parquet/Vortex process-crash recovery.
Both ON and default-OFF base gates passed 86 SQL cases with 3,582 assertions,
including the new 10-assertion metadata snapshot regression. Each reported 13
skips for unavailable optional extensions. The OFF build also rejected Vortex
format selection and exposed no acquisition function. The
deletion-inlining suite passed 18 cases with 1,099 assertions; two HTTPFS cases
were skipped because the optional extension was unavailable. Existing snapshot
index, retained-view and mixed-query process proofs passed against the rebuilt
native shell and the same Rust probe. Local builds reused qualified DuckDB
v1.5.5 engine objects and pinned native dependencies; ON/default-OFF CMake
configuration was checked separately. Hosted clean builds are tracked by the
Draft PR's workflow.
