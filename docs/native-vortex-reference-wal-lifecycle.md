# Reference ownership and native metadata WAL lifecycle

This R5 validation follows the explicit [request epoch contract](native-vortex-request-epochs.md).
It uses fixed inputs, transaction barriers and recovery invariants. R4 stable-load
A/A, performance A/B and sustained-load qualification remain deferred.

## Ownership contract

| Object | Owner and retention | Explicit lifecycle operation |
| --- | --- | --- |
| Caller reference JSON | The caller owns the output path. Publication copies its descriptor into native metadata; the path itself is not a generation lease. | After publication is durably confirmed, the caller may unlink its own file. Keeping it does not keep a retired generation alive. Existing build/generation collectors preserve it. |
| Named-index descriptor and generation references | Native metadata owns the current revision and named-reference rows. Shared generations stay active while any named index references them. | Existing publication CAS replaces the selected name's references atomically. Only a retired, unreferenced generation is eligible for bounded generation GC. |
| Build workspace and sealed artifacts | The registered build/generation protocols own only their recorded paths and inventories. | Existing explicit recovery and bounded collectors; unknown siblings are preserved. No directory sweep. |
| Fully opened ReadView | Private source buffers and authenticated generation copies belong to the live query. An exported diagnostic JSON is only a description. | Query completion/cancellation releases the private view. Removing caller JSON, collecting canonical generations and checkpointing do not revoke a fully opened view. Process SIGKILL can leave private temporary files; they do not become registered generations. |
| Request receipts and epoch fences | The native metadata transaction owns the current window, last advance result and retained receipts. | Explicit epoch advance retires logical receipts. Checkpoint must preserve the fence and exact current receipts. |
| Native metadata file and WAL | DuckDB's native storage manager owns both, including checkpoint/recovery WAL sidecars. | Explicit non-forced checkpoint of the **native metadata database**. Never unlink or truncate WAL files manually. |

An existing caller reference path cannot be overwritten by a build. Removing a
file does not remove a native named reference; retaining a file does not pin its
generation. A descriptor whose generation was collected cannot be republished
successfully. Named query admission reads the committed catalog, not that file.
Caller cleanup must be based on its own known output and confirmed publication;
uncertain or failed publication requires reconciliation first.

Query scratch lives under the caller-selected temporary root, outside managed
data. Normal completion removes it; process SIGKILL cannot run destructors.
Native metadata recovery and managed collectors neither adopt nor sweep these
unregistered private copies. The crash fixtures retain and authenticate them in
their own isolated temporary namespaces. Owner-controlled temporary-root cleanup
after process termination is a separate lifecycle from metadata checkpoint and
managed generation GC; this proof does not introduce a crash-scratch sweeper.

## Checkpoint scope and protection

For a native local catalog attached as `lake`, checkpoint its attached metadata
database explicitly, after verifying the attachment's name and path:

```sql
SELECT database_name,type,path FROM duckdb_databases()
WHERE database_name='lake';
CHECKPOINT __ducklake_metadata_lake;
```

The attachment must be the intended local DuckLake metadata file. Its hidden
native metadata database is directly addressable by this prototype's qualified
name but is omitted from `duckdb_databases()` and `duckdb_tables()` enumeration.
The proof reads all 13 declared Vortex metadata tables explicitly; missing tables
fail, rather than producing an empty or partial snapshot.

`CHECKPOINT lake` invokes DuckLake's flush, snapshot-expiration, compaction and
cleanup sequence. It is a different operation with different side effects.
This contract uses the existing native checkpoint directly, with no new worker,
automatic checkpoint retry, force escalation or fallback.

The non-forced native checkpoint rejects transaction-local changes and other
active native writers. A failed checkpoint on another connection must not abort
the writer. A caller with local changes handles its SQL error and explicitly
rolls back; pending changes must never be committed by checkpoint. Older readers
can also retain committed UPDATE transactions and their checkpoint locks. In the
tested epoch-advance case, non-forced checkpoint explicitly defers until the old
reader ends; its old receipt/business snapshot remains readable. Retry checkpoint
explicitly after releasing that reader. This does not imply every read-only
transaction always blocks native checkpoint. Source GC
still applies its independent ordinary-transaction and ReadView admission gates.
A checkpoint success never authorizes source or generation deletion.

With no concurrent writers, a successful checkpoint removes the old WAL. Later
commits can create a new WAL. With concurrent writes, native DuckDB can retain a
new WAL for those commits. WAL deletion/shrinkage is not database-file compaction,
and logical GC counts are not physical block reclamation or an overall disk/RSS
bound. Fatal injected checkpoint failures require reopening and native recovery.

## Deterministic acceptance

Use CPU 6–11, DuckDB `threads=2`, unchanged SPFresh/thread environment and all
existing budgets. Timing is observational; timeouts detect hangs only.

1. Share B1 between two names, remove the caller's original JSON, collect the
   published build workspace, and query both names from native metadata.
2. Replace one name with B2: B1 remains active and cannot be collected. Replace
   the second name: B1 becomes retired. A too-small byte budget must defer it.
   Retained caller JSON and unknown siblings survive bounded collectors.
3. Hold a fully opened B1 query through replacement, GC, receipt retirement and
   native checkpoint; it must still return its original typed versions. Exercise
   partial generation GC cancellation/failure, checkpoint the durable deletion
   intent, and explicitly resume without widening budgets.
4. With an ordinary reader retaining the old epoch window, verify checkpoint
   defers and preserves its snapshot, then succeeds after the reader commits.
   Check transaction-local changes and an active metadata writer reject
   non-forced checkpoint without committing pending changes or aborting another
   connection's writer. Source GC must
   continue deferring to an ordinary business transaction.
5. SIGKILL before/after checkpoint, during a controlled checkpoint pause, at
   native header/WAL-finish fault boundaries, and on both sides of a later write
   commit. Reopen installed matching providers and compare typed rows, current
   epoch/receipts, named revisions, generation ownership, caller files and source
   identities. Old epochs must remain rejected. Native recovery alone must not
   restart background work.

No production interfaces, metadata layouts, retention defaults or budgets change
unless this validation discovers a correctness defect. Remote ownership, power
loss, indefinite disk bounds and stable-load performance remain outside this
local single-process POSIX qualification.

After the incremental non-editable runtime install and matching-provider build
from the [native workflow](vane-native-vortex-proof.md), run:

```bash
VANE_RUNNER=local-fast python -I test/vane/test_vane_vortex_reference_wal.py \
    --driver /path/to/build/ducklake_vortex_write_driver \
    --cpus 6,7,8,9,10,11 --output-dir /tmp/vortex-reference-wal
```

The [native-provider CI workflow](../.github/workflows/VaneNativeVortex.yml)
executes the same probe on an available runner CPU and retains its report,
native/recovery logs, committed-state snapshots and file-identity manifests.

## Result

The complete [installed-provider proof](../test/vane/test_vane_vortex_reference_wal.py)
passed **4 native/reopen scenarios** and **8 actual SIGKILL/recovery boundaries**.
The [native fixture](../test/vane/native_vortex_lifecycle_fixture.hpp) shares a
256-key, 2D B1 between two names, changes business versions, replaces both names
with B2 and collects B1 while a fully opened old query remains paused. The old
query still returns `seed-0` and the later-deleted key 1 after GC and checkpoint;
fresh named queries return the new versions from the committed catalog.

Both partial generation deletion cancellation and an injected I/O failure leave
the existing durable `deleting` state. Checkpoint preserves that state and an
explicit bounded collection resumes it. Caller JSON and unknown siblings survive;
old JSON cannot revive the collected generation. Output-path reuse fails before
changing ownership. Count/byte budgets and the managed writer/ordinary-reader
protections remain intact.

The WAL guard scenario verifies rejection for another active writer and for
transaction-local changes, with explicit rollback of the latter. After advancing
an epoch under an old ordinary reader, checkpoint rejects because the committed
UPDATE's checkpoint lock is still retained. The reader still sees its two old
receipts and source GC remains blocked. Checkpoint succeeds after the reader
commits. The initial fixture had assumed this concurrent checkpoint would succeed;
the accepted test preserves the actual lock contract and never forces it.

| Boundary | Interruption | Required recovered state |
| --- | --- | --- |
| Before checkpoint | SIGKILL with a nonempty WAL | All pre-checkpoint commits |
| During checkpoint | SIGKILL while the call is active after a flushed WAL marker | All pre-checkpoint commits |
| After checkpoint | SIGKILL with no old WAL | The checkpointed state |
| Before header write | Native fatal fault, then SIGKILL without normal close | All pre-checkpoint commits |
| Before truncation, after header write | Native fatal fault, then SIGKILL | All pre-checkpoint commits |
| Before WAL finish | Native fatal fault, then SIGKILL | All pre-checkpoint commits |
| Later write before commit | SIGKILL after staging | Checkpointed state, no pending row/receipt |
| Later write after commit / lost ACK | SIGKILL after commit | New row and exact receipt together |

The in-flight case uses the engine's existing test-only checkpoint pause after
WAL start. With no other writer, the parent observes the flushed marker (WAL
74,158 → 74,207 bytes in this fixture) before SIGKILL. The three fatal boundary
cases use existing native debug fault settings; they are distinct from killing
an active checkpoint call. Neither setting changes production defaults or adds
an engine hook.

All 12 scenarios reopen through matching installed providers and compare every
row of **13 declared Vortex metadata tables**, the snapshot IDs/schema versions,
and all typed business columns. Current receipts and last epoch advance results
remain exact; unseen old epochs fail. Checkpoint does not flush or reclaim Vortex
sources/artifacts. Files are authenticated by path, size and SHA-256. Normal
query completion leaves its private namespace empty; each SIGKILL fixture leaves
eight authenticated private files unchanged by recovery, alongside the preserved
caller reference outputs. Reopen does not resume the cancelled optimizer.

The initial enumeration-based fixture was rejected by its installed snapshot
check because hidden metadata was omitted; the accepted run explicitly reads
all declared tables. No missing-table compatibility path was added.

CPU affinity was 6–11 and restored afterwards, the thread environment stayed
unchanged, and all 14 implementation/test source identities plus the exact
native driver remained fixed. Time is observational. The runtime was installed
non-editably from the incremental Release build and matching local test-signed
providers were packaged in a new isolated environment; earlier frozen trials
and their evidence were preserved. Production `src/` is unchanged from #85.

Four existing installed flow groups passed **65 native/recovery cases** and
**76 oracle comparisons** (minimum fixture recall 0.9), including the eight
public epoch contract groups. The Vane base launcher passed **1,222 non-Ray plus
32 real-Ray tests**, with 15 optional skips in separate processes. These Ray
checks qualify the base runtime, not distributed ANN. Formatting, copyleft,
workflow YAML, relative links and final source identities passed.

| Evidence | Local path | SHA-256 |
| --- | --- | --- |
| Complete lifecycle/checkpoint proof | `/tmp/ducklake-vortex-reference-wal-proof-v3-20261106/report.json` | `37e2cc5b24080ded3b0f2c9741ccfa39d03789ab02bce6026b0f0222ddaa0693` |
| Existing regressions and Vane base | `/tmp/ducklake-vortex-reference-wal-regressions-20261106/report.json` | `dbc87c739a66a8612ced1d814dbfb57ec662b5d0d2ae06bef55d3afc1f6b0b7d` |
| Matched local provider bundle | `/tmp/ducklake-vortex-reference-wal-wheels-20261106/report.json` | `24faec06a4aa9dbd391bec4c4b96124b34b0edf7aa7729a07a244520d1ca2cac` |

These results establish the stated local ownership and checkpoint contract.
Capacity admission boundaries, crashed private-root cleanup, table-UUID state
retention and total disk/RSS bounds remain separate R5 work. Stable-load R4
qualification remains deferred.
