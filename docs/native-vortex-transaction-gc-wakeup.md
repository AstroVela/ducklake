# Wake deferred source GC after transaction completion

The maintenance worker subscribes when the source-retention policy is enabled,
snapshot or source-file work is pending, and ordinary transactions block the
probe. The pending-work probe reads metadata before checking the blocking
transactions. Source-file inspection remains behind that check. The collector
registers a late blocker only after acquiring its publication, read-admission,
and transaction-start guards. Collection rechecks ownership, references, and
the remaining object and byte budgets under those guards.

A DuckLake transaction manager records the observed blockers and a one-shot
callback under its transaction mutex. Finishing the first observed blocker
consumes the callback. Commit and successful rollback remove the transaction
before delivering the callback outside the mutex. The awakened worker probes
again and observes any remaining or newly registered readers. Worker metadata
transactions and finite captures are excluded from the readiness subscription.

The callback holds weak references to the runtime and the specific maintenance
registration bound to the lake and metadata attachments. A cancelled,
superseded, or stopped registration ignores delivery. The worker snapshots a
wake counter before probing and checks it under the condition-variable mutex
before entering its existing 100 ms wait. A completion during the probe
therefore requests an immediate new iteration.

The eighteenth profile stage, `source_transaction_wake`, records accepted
callbacks and their handling time on the thread completing the transaction.
This time overlaps worker activity. Multiple accepted callbacks can request
the same worker iteration.

## Deterministic checks

The source-retention driver pauses the worker before the wait or before the
collector's transaction check. It verifies commit and rollback, remaining
blockers, a new reader before the next probe, a reader arriving after readiness,
queued files without an eligible snapshot, and cancellation or replacement of
a maintenance registration. The first guarded expiry records completed waits
and timeouts. Disabling the wake-counter comparison exposes the missed
notification in the probe-to-wait window. In the replacement case, an idle
catalog on the same worker records the wait that still occurs after the worker
rejects the obsolete callback.

The [original qualification](https://github.com/AstroVela/ducklake/blob/9819016e841ea79c8eea054415f4724fa2270e85/docs/native-vortex-transaction-gc-wakeup.md)
records the sustained workload on the earlier stack. Sustained qualification
of the port remains deferred.

A follow-up on the earlier stack compared the maintenance policy and backlog
reads and recorded the decision to keep separate statements.
[Maintenance probe A/B decision](native-vortex-maintenance-probe-ab.md)
