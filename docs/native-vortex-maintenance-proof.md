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
in that native catalog, including registered manual builds. Duplicate enable,
missing initialization, invalid budgets and resume while running fail explicitly.
Caller reference JSON and unregistered directories remain outside GC ownership.
The policy uses the existing database optimizer worker, with at most 16 registered
maintenance catalogs and no additional background threads or build concurrency.

Maintenance has its own enabled flag, run ID, stage, error and progress counters.
Disabling or cancelling an indexing policy does not cancel already authorized
cleanup. Cancelling maintenance interrupts its current native checkpoints and
stops further cleanup until explicit resume. A maintenance error is recorded
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
5. Refresh backlog counts and persist `idle`. Budget-limited unfinished work
   can continue on a later worker pass; failed work requires explicit resume.

The object budget is 1–16 completed collections across both collectors per pass;
reconciliation separately visits at most that many pending records. The byte
budget is 1–64 MiB shared across the two collection phases, using their existing
accounting: reclaimed logical build-file lengths plus registered retired
artifact bytes. Manifest verification, directory operations and metadata reads
are not physical I/O quotas. The existing 4,096-entry build traversal limit and
bounded partial truncation apply unchanged. A generation that cannot fit the
remaining budget stays registered for another pass. Budgets never expand.

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
resume. Shared count/byte limits and cancellation of a 65 MiB partial scratch
file exercise bounded continuation.

SIGKILL covers both sides of index publication and seven maintenance boundaries:
before recovery, deletion ownership, partial unlink, partial truncation,
retired-generation deletion, files removed before final progress, and coordinator
completion. Explicit resume refreshes stale backlog counts without starting an
empty cleanup run. Restart alone performs
no cleanup; explicit resume reconciles committed output before deletion and
continues without rebuilding a winning merge. Existing SQL, recovery, lifecycle,
index and GC proofs remain in CI, and the new proof's evidence is uploaded.

Qualification remains one local native DuckDB metadata owner and an exclusively
managed artifact namespace on POSIX. Static SPFresh, all dependency pins, global
source/vector/artifact budgets, the three-B limit and write admission ceilings
remain unchanged. This does not compact source data or reclaim physical table
row capacity, and it adds no SPFresh online update interface or fallback.
An [explicit indexed-source rewrite](native-vortex-index-rewrite-proof.md)
provides a separate data-compaction and atomic index-handoff path; registered
leftovers from that operation use the same maintenance worker and collector.

Local qualification kept DuckDB v1.5.5 (`d8cdaa33`), Vortex `65dab44f`, native
wrapper `b819cc03` and the qualified static SPFresh SDK unchanged from #44.
The final catalog driver SHA-256 was
`94bc584ac92bdf55338fdf1b0db586d2b4b13fec8d91f329189a0d26e2d9447e`.
The process proof passed 16 checks and 97 fixture queries with recall 1.0,
finishing seven merge/append/GC rounds at revision 17 with three active
generations and no registered build/retirement backlog. ON passed 16 Vortex SQL
cases (595 assertions) and Vortex recovery. ON/OFF each passed 86 base cases
(3,582 assertions; 13 optional-dependency skips) and Parquet recovery. Audit
passed 122 assertions; all 14 existing native process proofs passed. The isolated
cost proof passed four cases with nine queries each and recall 1.0; timings
remain informational. `make format-fix`, Rust formatting checks and
`git diff --check` passed.
