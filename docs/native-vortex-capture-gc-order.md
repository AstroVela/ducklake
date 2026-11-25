# Order source collection after native capture admission

The [stall diagnostics](native-vortex-stall-diagnostics.md) retained two separate
observations: an unresolved silent query stop and a longer four-reader run that
continued querying but missed its historical-snapshot drain deadline.

## Lock order

A native ReadView acquires shared capture admission before beginning its
transaction. This includes DuckDB's eager transaction mode, which can open
transactions on all attached catalogs during `BEGIN`. Starting a DuckLake
catalog transaction briefly acquires the shared gate. The original collector
acquired the exclusive transaction gate before acquiring exclusive capture
admission. A query between admission and transaction registration could wait
on the gate while the collector waited on its admission lock. The synced #62
collector avoids that cycle by deferring when capture admission is busy.

Source collection acquires publication and then waits interruptibly for
exclusive capture admission. Existing captures can still enter transactions
and finish. Once admission closes, collection tries to freeze new transaction
starts and checks for active DuckLake transactions. A busy transaction gate or
an ordinary held transaction defers collection. The same snapshot and file
ownership checks, private copies, deletion intent, crash checkpoints and
shared object and byte budgets protect the collection. A queued read retains
its lake and metadata attachment identities across the admission wait.

## Regression

The native admission case pauses a real ANN query after it captures a
snapshot. The fixture observes collection waiting for admission and checks
that the transaction gate remains available. After capture resumes, the
query returns its original winner and collection expires eligible snapshots in
the same pass.

A second native case holds capture admission before transaction registration.
After collection reaches its admission wait, a real DuckLake query completes
while admission is still held. Collection advances when the fixture releases
admission. A third case interrupts the waiting collector, verifies the locks
become available and snapshots remain intact, and completes a fresh collection
after releasing admission.

With eager transactions enabled, the native test pauses a captured ReadView,
queues source collection, and then queues a second capture. It checks that
collection expires snapshots after the first capture releases admission and
that both queries return their expected rows.

A separate test reattaches a lake while a capture waits. The queued capture
rejects the new attachment and a fresh capture reads the reattached data.

A minimal SQL regression holds an ordinary DuckLake reader in a second
connection, verifies collection reports transaction deferral, commits the reader
and checks that the next bounded collection progresses. Existing native cases
cover retained private views after GC, blocked new query admission during
unlink, cancellation, ownership failures and crash recovery.

This removes a specific lock-order cycle. The original silent execution had no
usable stack; this change alone does not establish its root cause. Qualification
of the longer drain failure is recorded separately below.

## Qualification recorded at `6e05e831`

Both new native assertions fail against the frozen preceding library and pass
against the rebuilt library. The complete installed source-retention proof
passed sixteen query comparisons and twenty-two native/recovery cases. The ten
SQL fixtures passed 259 assertion blocks, including the new held-transaction
case. Keyed writes, append/receipt recovery and basic ANN passed 28, 16 and 32
query comparisons respectively. The Vane base release launcher passed 1,222
non-Ray and 32 Ray tests in separate processes, with 15 optional-dependency skips.

The automatic Python flow passed all eighty query comparisons. Its concurrent
native `automatic-live` case failed the existing 0.8 recall gate once; the other
four native serialization cases and four SIGKILL recovery cases passed when
run separately. That first failure remains retained. A diagnostic run against
the frozen preceding library reproduced a filtered miss at snapshot 17 with
recall 0.7: three of the exact top ten IDs were absent. A diagnostic run of the
new library also completed with the unchanged gate. These observations establish
that the recall miss predates this lock-order change; they do not qualify broad
ANN recall or erase the failed full-suite execution. No test threshold or CI
case was removed.

All six installed short-load combinations (512, 4,096 and 32,768 rows by 8D and
128D) completed 2,618 typed query-window checks. Every fixed canary reached 1.0;
no stall capture was needed. Exploration minima remained as low as 0.2, so this
is not a broad ANN quality qualification.

The separate 60-second/four-reader diagnostic still exceeded the unchanged
90-second drain deadline. T/F and the index-artifact backlog reached zero, but
238 eligible historical snapshots remained. Queries continued and the watchdog
did not expire. The lock-order fix therefore does not close the long-load
reclamation failure. Readiness under continuously active native captures remains
the next scheduling investigation; no deadline, workload or budget was relaxed.

The rebuilt extension was signed and installed against the same Vane and Vortex
wheels. Formatting, syntax, local documentation links and copyleft checks passed.
