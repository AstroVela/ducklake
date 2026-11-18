# Native background recovery and garbage collection

This validation draft follows [DuckLake #44](https://github.com/AstroVela/ducklake/pull/44).
It connects registered build recovery, workspace collection and retired-generation
collection to the existing optimizer worker. Repeated indexing and selective
merging can finish their cleanup without a manual GC call after every task.

## Explicit catalog ownership

Index initialization and optimizer initialization remain explicit. Fresh
optimizer initialization also creates `ducklake_vortex_maintenance_v1`; no
migration or compatibility decoder is provided. Enabling an indexing policy
alone does not authorize catalog-wide reclamation. Enable maintenance explicitly:

```sql
SELECT * FROM ducklake_enable_vortex_maintenance('lake',16,67108864);
SELECT * FROM ducklake_vortex_maintenance_status('lake');
SELECT * FROM ducklake_cancel_vortex_maintenance('lake');
SELECT * FROM ducklake_resume_vortex_maintenance('lake');
```

One maintenance policy covers all registered build roots and retired generations
in that native catalog, including registered manual builds. Enable, cancel and
resume require autocommit and a writable attachment. Status can be read from a
read-only attachment. Duplicate enable, missing initialization, invalid budgets
and resume while running fail explicitly.
The runtime binds each registration to the attached DuckLake and DuckDB
metadata instances. Calls using different casing find the same registration.
The controls check attachments retained by the calling transaction, including
the native metadata connection of an existing DuckLake transaction. Each
collector transaction verifies both identities before using catalog data.
Reattaching a lake requires explicit resume for the new attachment. A caller
retaining the earlier attachment is rejected.
Caller reference JSON and unregistered directories remain outside GC ownership.
The policy uses the existing database optimizer worker, with at most 16 registered
maintenance catalogs and no additional background threads or build concurrency.

Maintenance has its own enabled flag, run ID, stage, error and progress counters.
Disabling or cancelling an indexing policy does not cancel already authorized
cleanup. Cancelling maintenance interrupts its current native checkpoints and
stops further cleanup until explicit resume. Cancel and resume update the durable
policy and worker registration in the same serialized transition. Completion
records its last state and clears the running task within that boundary.
A maintenance error is recorded
and disables that policy; indexing and maintenance do not retry failed tasks.

Restart preserves configuration and durable stages but starts neither indexing
nor maintenance implicitly. Resume maintenance explicitly to reconcile and clean
stopped work, and resume each indexing policy explicitly to plan fresh work.
The catalog and generation registries determine ownership throughout; diagnostic
counters do not decide whether a generation can be deleted.

## Serialized, bounded work

The single worker serializes indexing and independent maintenance phases:

1. Inspect metadata for registered builds or retired/deleting generations. An
   empty backlog creates no new run or metadata progress writes.
2. Persist a maintenance run and `recovering` stage. Reconcile at most the
   configured object count of pending builds against committed publication.
3. Persist `collecting_builds` and invoke the existing build collector. It keeps
   published generations and deletes only registered owned leftovers.
4. Persist `collecting_generations` and pass the remaining object/byte budget
   to the existing retired-generation collector.
5. Refresh backlog counts and persist `idle` or `blocked`. Budget-limited
   unfinished work can continue on a later worker pass when it fits a full
   pass. Failed work requires explicit resume.

The object budget is 1–16 completed collections across both collectors per pass;
reconciliation separately visits at most that many pending records. The byte
budget is 1–64 MiB shared across the two collection phases, using their existing
accounting: reclaimed logical build-file lengths plus registered retired
artifact bytes. Manifest verification, directory operations and metadata reads
are not physical I/O quotas. The existing 4,096-entry build traversal limit and
bounded partial truncation apply unchanged. Generation selection excludes
entries that exceed the available bytes before applying the object limit.
Items whose combined size exceeds the budget stay registered for a later pass.
When all remaining generations are referenced or exceed the entire pass
budget, maintenance reports `blocked`. The worker observes backlog without
opening empty runs or rewriting progress. Operators can collect explicitly with
an allowed budget that fits a larger generation. Background budgets never expand.

Both collectors retain their existing publication/admission locks, authenticated
generation checks, POSIX descriptor operations, fsync and durable deletion
states. The worker calls them after native construction and its collection
guard have returned. A manually invoked live native builder remains protected
by the shared collection guard. Fully opened query leases permit canonical GC
and keep their sources and artifacts for old-view search and delayed fetch.

Counters record completed collector results acknowledged by the coordinator.
Cancellation or SIGKILL inside a collector can reclaim files before a result is
returned, so counters can undercount that work. They are not disk accounting or
a recovery journal. Registered ownership and filesystem checkpoints support
safe explicit resumption even when coordinator progress was not acknowledged.

## Qualification

```sh
python3 -I test/vane/test_native_vortex_maintenance.py \
    --duckdb build/index-shell/duckdb \
    --driver build/index-shell/ducklake_vortex_catalog_driver \
    --probe build/index-cargo/ci/ducklake-vortex-index-probe \
    --output-dir build/index-logs/maintenance
```

The proof runs seven merge/append/GC rounds including background T flush. It
checks metadata/artifact convergence, current B/F/T visibility, an old private
view, delayed typed fetch, unchanged B1 generations/hashes/mtimes and unchanged
source bytes. It retains write quotas and rejects excess F while work is paused.
Build failure/cancellation leave indexing stopped while maintenance cleans its
registered leftovers. GC failure disables maintenance and requires explicit
resume. Count and byte fixtures observe both collectors after one completed
pass. A 65 MiB partial scratch file exercises bounded continuation. The proof
also covers a reattach while GC waits, cancellation at completion, a resumed
policy surviving an old completion callback, and a blocked generation behind
which smaller work fits.

SIGKILL covers both sides of index publication and seven maintenance boundaries:
before recovery, deletion ownership, partial unlink, partial truncation,
retired-generation deletion, files removed before final progress, and coordinator
completion. Explicit resume refreshes stale backlog counts while the worker
is busy and without starting an empty cleanup run. Restart alone performs
no cleanup; explicit resume reconciles committed output before deletion and
continues without rebuilding a winning merge. Existing SQL, recovery, lifecycle,
index and GC proofs remain in CI, and the new proof's evidence is uploaded.

Qualification remains one local native DuckDB metadata owner and an exclusively
managed artifact namespace on POSIX. Static SPFresh, all dependency pins, global
source/vector/artifact budgets, the three-B limit and write admission ceilings
remain unchanged. This does not compact source data or reclaim physical table
row capacity, and it adds no SPFresh online update interface or fallback.

Local qualification used DuckDB v1.5.5 (`d8cdaa33`), Vortex `b20e34c2`,
native wrapper `84cef204` and the static SPFresh SDK qualified by #44. The
Linux build from `df32c661` passed 21 maintenance checks in
65.1 seconds against the 180-second budget. All
97 fixture queries achieved recall 1.0. Seven merge/append/GC
rounds reached revision 17 with three active generations and empty build and
retirement registries. The catalog driver SHA-256 was
`37c9caf1524aef4ab4268dcedce123272ed04388b4bd494a41e1474f4a53bb77`.

All 15 native process suites and 18 functional gates passed. Native ON passed
25 Vortex SQL cases with 990 assertions. ON and OFF each passed
86 base cases with 3,582 assertions and 13 optional-dependency skips. All
32 ABI checks passed. Four isolated query-cost cases passed with minimum
fixture recall 1.0. Rust formatting, Clippy, workspace tests and the
changed-file formatting and diff checks passed.

The first matrix run reached the existing 60-second native RPC limit while
creating the initial B1 for the stored-ownership fixture. SPFresh scratch
existed and the new generation's artifact directory was empty. The cause is
undetermined. With the same driver and limits, the isolated build-ownership
suite passed in 35.7 seconds and the complete matrix rerun passed that
suite in 35.5 seconds.
