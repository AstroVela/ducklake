# Admit the finite source-status read before source GC

The source-retention status function opens its own short DuckLake transaction.
The query capture path already joins finite-read admission, but this dedicated
status owner did not. A status read could make source work appear unavailable,
or remain in flight when the collector finished waiting for query captures and
checked all other transactions. Frequent status polling could therefore defer
otherwise eligible cleanup.

Acquire shared admission before opening the status owner and mark only that
owner as a finite native read capture. Its existing commit completes before the
admission guard is released. On an exception, the owner connection is destroyed
and rolls back before the guard is released. An ordinary caller transaction is
separate and keeps its original classification.

Readiness alone excludes these marked captures. Collection still closes
admission, waits for finite readers, freezes new transaction entry, and checks
all other transactions before expiration or deletion. Status itself still counts
all other transactions. Its fields, source/version ownership, object and byte
budgets, cancellation and explicit-resume rules remain unchanged.

There are two production lines, using the existing admission lock and capture
mark. There is no new schema, API, compatibility layer, retry or fallback.
The Vane, wrapper, Vortex and native SDK revisions remain the exact versions
qualified in [the preceding consumer change](native-vortex-spfresh-fixes.md).

## Deterministic regression

The native `status-readiness` case pauses a real source-status call while its
private transaction reads the policy. The fixture temporarily replaces its own
policy table with a view using a one-shot blocking scalar; no production test
hook is added. Other policy reads proceed while that single status call waits.

With an ordinary user transaction also held, source readiness must remain false.
After committing only that user transaction, readiness must become true while
the status read stays paused. The paused status read must also hold shared
admission, then return its original row. After releasing it and restoring the
private policy table, source collection and the current ANN winner must remain
valid. The preceding library fails with `Source readiness discarded finite
status work`.

The SQL fixture additionally invokes source status from the already-held
ordinary reader connection. It must still report that user transaction as a
blocker, and physical collection must still defer. Existing cases retain the
capture/gate ordering, cancellation, old private views after GC, registered-file
ownership and real crash-recovery coverage.

## Convergence acceptance

The preceding signed consumer's 60-second/four-reader run still missed the
unchanged 90-second drain deadline with 118 eligible snapshots remaining,
T = F = 0 and no artifact backlog. The watchdog did not expire. An independent
copy of an earlier failed catalog drained 116 snapshots and 78 source files in
4.25 seconds over 97 direct passes, preserving the two-object budget. This
isolated timing motivates contention analysis; it does not measure production
throughput or prove long-load convergence.

The deterministic status regression and the unchanged longer workload qualify
separately. The prior 4,096-row/128D cost canary's 0.7 recall lower bound and the
original silent stop remain open; this admission change does not alter ANN
routing, its budget or its oracle.

## Signed-provider regression qualification

The fixed native regression passes against the rebuilt source driver. The
isolated installed environment contains the byte-identical Vane and Vortex
wheels from the preceding qualification and a newly signed DuckLake provider
with that exact Vortex wheel dependency.

All five public flows passed 172 query-oracle comparisons. The source flow
passed all 24 native/recovery cases, including the new status case; seven write,
thirteen keyed, nine automatic and thirteen standalone maintenance fault/budget
cases passed as well. Ten SQL fixtures passed 261 assertion blocks, including
the held ordinary transaction's source-status call.

All six short automatic load combinations completed 2,399 typed query-window
checks. Their fixed-canary lower bounds were 1.0; exploration lower bounds were
at least 0.9. No stall capture was needed. The unchanged Vane base launcher again
passed 1,222 non-Ray and 32 Ray tests in separate processes, with fifteen
optional-dependency skips. Formatting, syntax and copyleft checks passed.

The separate long-load result is recorded independently; these bounded checks
do not close its convergence acceptance or the retained ANN-quality issue.

## Long-load failure and retained native stacks

The separate 60-second/four-reader run did not qualify. Queries stopped during
an index merge, and the unchanged 480-second worker watchdog eventually killed
the process. The opt-in stall capture completed successfully before termination.

Its native trace places the optimizer in `vortex_spfresh_close`, waiting in
`SPTAG::Helper::ThreadPool::~ThreadPool` for a worker thread to join. That worker
is waiting on the pool's condition variable. Query threads are waiting for the
bridge's shared native mutex in `vortex_spfresh_open`. The trace does not place
the optimizer in source collection or status admission.

The SDK's unchanged shutdown code sets a plain stop flag and notifies without
the queue mutex used by waiting workers. The captured wait chain motivates a
separate SPFresh shutdown regression; this PR does not claim to fix that native
liveness failure or establish the cause of the earlier uncaptured silent stop.
The status-readiness regression, six short loads and the failed longer run are
all retained. The original budgets, query pacing and deadlines remain intact.
