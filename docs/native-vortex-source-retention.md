# Bounded native Vortex source retention

The [installed Vane vector flow](vane-native-vortex-proof.md) can retain a fixed
live dataset while rolling writes replace its contents. Source retention joins
the existing [maintenance worker](native-vortex-maintenance-proof.md): it expires
native snapshots and removes their unreferenced physical source files. The
[WriteBatch request ledger](native-vortex-write-batch.md) remains durable when
the original batch snapshot expires.

## Policy and operation

Use a fresh optimizer initialization with the matching installed providers.
Source retention defaults to disabled. Its policy applies to the entire
attached DuckLake catalog, including tables without a named vector index:

```sql
SELECT * FROM ducklake_set_vortex_source_retention('lake',true,2,3600);
SELECT * FROM ducklake_enable_vortex_maintenance('lake',2,67108864);
SELECT * FROM ducklake_vortex_source_retention_status('lake');
```

The policy retains at least the latest `keep_snapshots` native snapshots
(1–100,000). A snapshot must also be older than `min_age_seconds` (0–315,360,000)
to become eligible. Setting `enabled=false` suspends both expiration and source
file collection without forgetting pending deletion intents. The newest snapshot
always remains. All operations require exclusive single-process catalog
ownership and managed local POSIX data paths.

The existing worker performs one bounded pass at a time. Build cleanup,
generation cleanup, source file cleanup and snapshot expiration share its
1–16 object and 1–64 MiB byte budgets. A snapshot or completed file counts as
one object; source files charge their full registered size, including recovery
after unlink. Byte accounting describes completed objects, not all bytes read
for verification, metadata I/O or WAL size. An individual source file must fit
the byte budget; a smaller configured budget leaves it pending. Pending source
files take priority over further snapshot expiration within the source stage.

An explicit bounded pass uses the same implementation and policy:

```sql
SELECT * FROM ducklake_collect_vortex_sources('lake',2,67108864);
SELECT * FROM ducklake_cancel_vortex_maintenance('lake');
SELECT * FROM ducklake_resume_vortex_maintenance('lake');
```

Policy changes and explicit collection require standalone autocommit statements.
Restart preserves policy, progress and pending intents; maintenance resumes only
after the explicit resume call. Status reports eligibility even when disabled,
scheduled files, pending deletion intents, cumulative completed work and whether
ordinary native transactions currently defer collection.

## Readers and deletion ownership

The collector first takes publication ownership, then the transaction admission
gate. If any ordinary DuckLake transaction remains active, the pass returns
`blocked_by_transactions=true`. New native transactions cannot register while
expiration or deletion owns the gate. The collector then takes the existing
ReadView admission lock. It obtains its private native metadata snapshot only
after these checks, avoiding a plan captured before a concurrent write commits.

Existing native transactions keep their original source snapshot. An admitted
ReadView has private copies of its required source and index data, so delayed
typed fetch remains valid after both source and generation GC. Blocked source
work does not generate repeated empty maintenance runs. Native writes or queries
arriving during a deletion pass wait for that bounded pass.

Snapshot expiration reuses native `DeleteSnapshots`, including its metadata
reference accounting and scheduled file queue. The collector consumes only that
queue and its own durable intent records. It checks native and current named
index references before deleting. It never scans for arbitrary orphan files.

Before unlink, a native transaction commits a deletion intent containing the
exact queue key, file size, device, inode and SHA-256 digest. Descriptor-relative
opens reject symlinks and path traversal; files must be regular with one link.
The collector verifies identity and contents again, unlinks, fsyncs the parent
directory, then atomically acknowledges the native queue, intent and counters.
Changed files fail explicitly. A missing source is accepted only for a committed
intent. Independent native cleanup can finish an already registered deletion;
the collector subsequently acknowledges its remaining intent.

Cancellation preserves any committed intent. A crash before or after unlink
continues from the same native metadata WAL after explicit resume. Request
receipts are never expired: replay returns the original receipt without
reapplying business rows, even when its snapshot ID is no longer queryable.

For rolling managed mutations with automatic indexing, rewrite and reclamation,
see the [automatic flow](native-vortex-automatic-flow.md).

## Verification and scope

```sh
cmake --build build/runtime --target ducklake_vortex_source_driver
VANE_RUNNER=local-fast python -I test/vane/test_vane_vortex_source_retention.py \
    --driver build/runtime/ducklake_vortex_source_driver \
    --output-dir /tmp/vane-vortex-source-proof
```

The installed-provider probe checks policy boundaries, native transaction
deferral, worker progress, B/T and F/T oracle queries and durable receipts.
The native fixture covers held ReadViews and capture admission, new transaction
admission during unlink, shared pass limits, live references, checksum and
symlink rejection, independent manual cleanup, cancellation and explicit resume.
Eight rolling epochs keep 128 current rows while concurrent ANN results are
checked against their exact committed snapshot. After each epoch, physical
source bytes must equal the retained native file inventory plus an untouched
unregistered sentinel. Four injected boundaries and four real worker SIGKILLs
exercise snapshot expiration, intent commit, unlink and acknowledgement.
The latter recover through installed Vane with a nonempty metadata WAL.
Each installed recovery runs in a fresh Python process, exercising process
restart without initializing multiple copies of static OpenMP in one process.

Local qualification passed all 20 native/recovery cases and 16 oracle
comparisons across four phases, with minimum fixture recall 1.0. The rolling
fixture completed 1,000 snapshot-matched concurrent queries. All 35 new SQL
assertion blocks and 28 existing WriteBatch blocks passed through installed
Vane; native CI also runs them with its standard SQLLogicTest harness. The
original basic flow passed 32 comparisons, and WriteBatch passed 16 comparisons
and seven fault/recovery cases on the new provider. Separate Vane release
processes passed 1,222 non-Ray and 32 Ray tests, with 15 optional-dependency skips.

`test/sql/vortex/native_vortex_source_retention.test` supplies minimal native SQL
coverage; the native provider workflow retains reports and fixture logs. These
are bounded correctness checks. The bound covers managed source files after
drain; WAL, retained snapshot history, request receipts and caller-owned index
reference JSON have separate lifetimes. This adds no schema migration,
compatibility path, fallback, distributed reclamation or SPFresh original online
update API.
