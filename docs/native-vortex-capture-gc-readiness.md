# Schedule source GC around finite native captures

The [capture-order fix](native-vortex-capture-gc-order.md) removes the cycle
between capture admission and transaction entry. Its longer four-reader test
still missed the unchanged 90-second drain deadline: T/F and the artifact
backlog reached zero, while 238 eligible historical snapshots remained.

## Readiness and deletion admission

Source maintenance previously required a moment with no other DuckLake
transaction even to report work ready. A native ReadView registers such a
transaction while copying its snapshot, source bytes and inline rows. Continuous
readers therefore made this readiness check discard work that the collector
could safely perform after waiting for the finite capture.

Mark only the dedicated native capture transaction, after acquiring shared
capture admission and before reading its snapshot. The mark is an atomic,
in-memory transaction property. It ends with that transaction; it is not a
connection flag, durable catalog field or user transaction option. Acquisition
still commits before releasing admission, or rolls back on failure.

Only the readiness probe excludes these marked captures. An ordinary
transaction still makes readiness false. When dispatched, the collector closes
capture admission, waits for admitted captures, freezes transaction starts and
checks **all** other transactions before expiration or deletion. Status also
continues to count all active transactions. A readiness result grants no
permission to delete; the collector performs its existing independent checks.

A pass that actually expires snapshots, collects objects or frees artifact bytes
now continues to the next scheduling round immediately. The worker still visits
all index policies and maintenance catalogs in each round. When no pass makes
physical progress, it retains the existing 100 ms wait. Recovery counters alone
do not count as progress. Failed or cancelled work still stops and requires
explicit resume; there is no failure retry.

The object and byte budgets, source/version ownership, retained private views,
crash checkpoints, recall gate, drain deadline and workload remain unchanged.
There is no compatibility branch, retry, fallback or new storage format.

## Regression

The native `capture-readiness` case holds a real query after snapshot capture.
With an ordinary DuckLake transaction also held, readiness must be false. After
committing just that ordinary transaction, readiness must become true while the
native capture remains paused. Releasing the capture must preserve its original
query winner. The preceding library fails because it continues returning false
for the finite capture alone.

A minimal SQL check holds an ordinary reader, invokes native ANN on that same
connection, and verifies that collection still reports the ordinary transaction
as a blocker. The internal query capture must not reclassify the outer user
transaction. Existing source cases continue covering old private views, ordinary
transaction deferral without idle maintenance churn, locked deletion admission,
cancellation and SIGKILL recovery.

The longer four-reader workload and its original 90-second drain gate remain
the separate convergence acceptance. The filtered native recall miss and the
original silent stop remain open until their own evidence supports a fix.

## Intermediate evidence

Transaction classification alone passed the native readiness regression, all
23 source native/recovery cases, 260 SQL assertion blocks, and the automatic
flow's 80 query comparisons plus nine native/recovery cases. Its longer run
still missed the original 90-second drain deadline with 185 snapshots remaining.
That drain included 317 scheduling rounds, each followed by the fixed 100 ms
wait even when reclamation advanced. This intermediate failure is preserved;
progress-based scheduling is qualified separately below.

## Final candidate qualification

The rebuilt, signed provider passed all 23 source native/recovery cases and
sixteen source query comparisons. Ten SQL fixtures passed 260 assertion blocks.
The automatic flow passed eighty comparisons plus five native cases and four
real SIGKILL recovery points. All thirteen standalone maintenance cases passed,
including failure-stop, cancellation, shared object/byte budgets, partial-file
deletion and durable publication/GC crash recovery.

All six short load combinations completed 2,419 typed query-window checks.
Every fixed canary reached 1.0 and no stall capture was needed. Exploration
quality remains separate. The unchanged Vane base release launcher passed
1,222 non-Ray and 32 Ray tests in separate processes, with 15 optional-dependency
skips. Formatting, syntax, local documentation links and copyleft checks passed;
the updated transaction manager also compiled with native Vortex disabled and
warnings treated as errors.

The final candidate's separate 60-second/four-reader run still exceeded the
unchanged 90-second drain deadline. It reached T = F = 0 and an empty artifact
backlog, with 116 eligible historical snapshots and two scheduled source files
remaining. Queries continued and the watchdog did not expire. Write counts
varied between runs, so the remaining-backlog counts alone are not a throughput
comparison. This PR establishes readiness classification and productive-pass
scheduling; long-load convergence, broader ANN recall and the original silent
stop remain open.
