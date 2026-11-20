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
ownership and managed local POSIX data paths. DuckDB's directory permissions
apply to file collection.

The existing worker performs one bounded pass at a time. Build cleanup,
generation cleanup, source file cleanup and snapshot expiration share its
1–16 object and 1–64 MiB byte budgets. A snapshot or completed file counts as
one object; source files charge their full registered size, including recovery
after unlink. Byte accounting describes completed objects, not all bytes read
for verification, metadata I/O or WAL size. An individual source file must fit
the byte budget; a smaller configured budget leaves it pending. Pending source
files that fit the remaining budget take priority over further snapshot
expiration within the source stage. Multiple queue entries for the same
physical file consume one object.

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

The collector takes publication ownership and tries to acquire ReadView and
transaction admission in that order. An active capture or native transaction
can defer the pass with `blocked_by_transactions=true`.
New native transactions cannot register while
expiration or deletion owns the gate. The collector obtains its private native
metadata snapshot after these checks so the cleanup plan includes concurrent
writes that finished before collection.

Existing native transactions keep their original source snapshot. An admitted
ReadView has private copies of its required source and index data, so delayed
typed fetch remains valid after both source and generation GC. Blocked source
work does not generate repeated empty maintenance runs. Native writes or queries
arriving during a deletion pass wait for that bounded pass.

Snapshot expiration reuses native `DeleteSnapshots`, including its metadata
reference accounting and scheduled file queue. The collector consumes only that
queue and its own durable intent records. It checks native and current named
index references before deleting. Shared files stay pending while any retained
native or index reference resolves to the same local path. The final deletion
acknowledges all the queue entries for that file.
The native old-file and orphan cleanup functions also check live references
for a catalog initialized for source retention.

Before unlink, a native transaction commits a deletion intent containing the
exact queue key, file size, device, inode and SHA-256 digest. Descriptor-relative
opens reject symlinks and path traversal; files must be regular with one link.
The collector verifies identity and contents again, unlinks, fsyncs the parent
directory, then atomically acknowledges the native queue, intent and counters.
When resuming after unlink, the collector syncs the parent directory before
acknowledging the intent. If the parent is gone, it syncs its nearest existing
ancestor under the managed root.
Changed files fail explicitly. A missing source is accepted only for a committed
intent. Independent native cleanup can finish an already registered deletion;
the collector subsequently acknowledges its remaining intent.

Cancellation preserves any committed intent. A crash before or after unlink
continues from the same native metadata WAL after explicit resume. Request
receipts are never expired: replay returns the original receipt without
reapplying business rows, even when its snapshot ID is no longer queryable.

## Verification and scope

```sh
cmake --build build/runtime --target ducklake_vortex_source_driver
VANE_RUNNER=local-fast python -I test/vane/test_vane_vortex_source_retention.py \
    --driver build/runtime/ducklake_vortex_source_driver \
    --output-dir /tmp/vane-vortex-source-proof
```

The installed-provider probe checks policy boundaries, native transaction
deferral, worker progress, B/T and F/T oracle queries and durable receipts.
The native fixture covers held ReadViews and capture admission, immediate
transaction mode, new transaction admission during unlink, mixed-size budgets,
shared files, directory permissions, detach, checksum and symlink rejection,
manual cleanup, cancellation and explicit resume.
Eight rolling epochs keep 128 current rows while concurrent ANN results are
checked against their exact committed snapshot. After each epoch, physical
source bytes must equal the retained native file inventory plus an untouched
unregistered sentinel. Injected failures and real worker SIGKILLs cover snapshot
expiration, intent commit, unlink before and after directory sync, and
acknowledgement.
The latter recover through installed Vane with a nonempty metadata WAL.
Each installed recovery runs in a fresh Python process, exercising process
restart without initializing multiple copies of static OpenMP in one process.

`test/sql/vortex/native_vortex_source_retention.test` supplies minimal native SQL
coverage; the native provider workflow retains reports and fixture logs. These
are bounded correctness checks. The bound covers managed source files after
drain; WAL, retained snapshot history, request receipts and caller-owned index
reference JSON have separate lifetimes. This adds no schema migration,
compatibility path, fallback, distributed reclamation or SPFresh original online
update API.
