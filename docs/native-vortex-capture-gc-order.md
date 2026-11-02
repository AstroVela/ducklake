# Order source collection after native capture admission

The [stall diagnostics](native-vortex-stall-diagnostics.md) retained two separate
observations: an unresolved silent query stop and a longer four-reader run that
continued querying but missed its historical-snapshot drain deadline.

## Lock order

A native ReadView acquires shared capture admission before starting its DuckLake
transaction. Starting that transaction briefly acquires the shared transaction
gate. Previously source collection acquired the exclusive transaction gate,
checked for active transactions, and then acquired exclusive capture admission.
A query between admission and transaction registration could therefore wait on
the gate while the collector waited on its admission lock. If the query had
already registered, collection instead abandoned that pass immediately.

Source collection now acquires publication, capture admission and transaction
entry in that order. Existing captures finish with transaction entry available;
new captures wait outside admission. Once admission is closed, the collector
freezes transaction starts and checks for other active DuckLake transactions.
An ordinary held transaction still causes a deferred collection result. The
collector does not wait for that ordinary transaction to end.

The change preserves the same snapshot/file ownership checks, private ReadView
copies, deletion intent, crash checkpoints and shared object/byte budgets. It
adds no alternate query path, compatibility branch, retry or fallback.

## Regression

The existing native admission case pauses a real ANN query after its snapshot
is captured. Collection must wait, leave the transaction gate available, and
then expire eligible snapshots in the same pass after capture is released. The
query must still return its original winner. The preceding implementation fails
this fixture because it abandons the pass while capture is active.

A second native case holds capture admission before transaction registration,
starts source collection, and enters a real DuckLake query while admission is
still held. Transaction entry must complete before admission is released; then
collection must progress. The preceding library fails this case because its
collector holds the transaction gate while waiting for admission. Both cases
release their held admission before reporting a failure, leaving no stuck
fixture worker.

A minimal SQL regression holds an ordinary DuckLake reader in a second
connection, verifies collection reports transaction deferral, commits the reader
and checks that the next bounded collection progresses. Existing native cases
cover retained private views after GC, blocked new query admission during
unlink, cancellation, ownership failures and crash recovery.

This removes a specific lock-order cycle. The original silent execution had no
usable stack; this change alone does not establish its root cause. Qualification
of the longer drain failure is recorded separately below.

## Qualification so far

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
