# Capture mixed-load stalls before termination

The [rewrite-planning follow-up](native-vortex-rewrite-planning.md) retained
a 512-row / 8D run that stopped completing queries while metadata samples and
budget rejections continued. Its last optimizer state was `building`, run 19,
with T = F = 128. The worker eventually reached its existing outer watchdog;
its log contained no native or Python stack to identify the blocked operation.
That original stop remains unresolved.

## Diagnostic boundary

The installed load probe accepts `--capture-stalls`. This Linux diagnostic
requires GDB. The launcher passes its PID to the worker. The worker checks
its current parent before and after granting tracing permission to the
launcher and its descendants. A worker whose parent changes stops setup.
The worker registers a Python stack-dump signal and then emits
`diagnostics_ready`. The parent waits for this event before tracing or
sending the signal. During Python shutdown, removing the handler returns
`SIGUSR2` to an ignored disposition. The launcher sets the default
`SIGCHLD` disposition before starting a worker so it can retain the worker
PID until the capture and watchdog threads have finished.

During the measured workload, thirty seconds without any events records a
silent worker. Thirty seconds without a completed query while other events
continue records a query stall. A case can also trigger capture 25 seconds
before the watchdog deadline, including during startup. The event pipe
closing starts a 30-second exit deadline. If that deadline comes first, it
triggers capture with 25 seconds left. Each case makes one capture attempt
at most.

`stall.json` records the trigger, latest task/source status, last completed
query's read window and vector, pending build-directory names, and the capture
outcomes. GDB has 20 seconds to record native thread stacks in
`native-stacks.log`. The parent then requests Python stacks in `worker.log`
if the worker is still running, including when GDB fails or times out. The
watchdog terminates the worker at `3 * seconds_per_phase + 300`.

The case report marks a diagnostic attempt with `stall_capture_attempted`.
Failure messages include the worker exit status, whether the watchdog stopped
it, an expired exit deadline after the event pipe closes, and any malformed
event. The original bytes from a truncated record are available in
`events.jsonl`. Review captures when comparing workload timings.

The controlled proof verifies both kinds of stack, clean worker exit,
launcher identity, the startup deadline, capture failure, late signal
handling, worker exit during capture, a timeout after the event pipe closes,
diagnostic thread startup failure, and disabled capture with either initial
`SIGCHLD` disposition. The workflow runs that proof, enables capture in its
four load cases, and retains the reports and capture files.

```sh
python -I test/vane/test_vane_vortex_stall_capture.py \
    --output-dir /tmp/vane-vortex-stall-capture

VANE_RUNNER=local python -I test/vane/test_vane_vortex_sustained_load.py \
    --fixtures automatic --rows 512 --dimensions 8 --seconds 12 --capture-stalls \
    --output-dir /tmp/vane-vortex-stall-repro
```

## Investigation at `bb21bf8e`

The unchanged 512-row workload completed six diagnostic-enabled repetitions,
five repetitions using the preceding worker source with tracing permission set
before exec, five separate heap-perturbation diagnostics, and twenty further
repetitions in the independently frozen preceding environment. None reproduced
the original stop. These observations do not close the failure.

The failed process left two private query views containing the same 128-row B
sources, file IDs 20 and 23. Its unpublished build contained 256 physical vectors
with 64 heads. Those native files were preserved separately; all 256 build-input
vectors were recovered from their complete head/posting coverage. Direct native
open/search/close replays completed against both query bundles and the pending
build. Native memory checking reported no invalid-access or uninitialized-value
errors. The full installed Vane read-only query replay also completed under
memory checking without such errors; this is not a leak-free assertion.

A separate installed replay queried the failed database concurrently with
repeated builds from those recovered vectors: both readers completed, alongside
130 builds. Fully replaced and empty F/T probes also completed. These are
diagnostics, not a production throughput or correctness qualification for the
original failed execution.

A longer diagnostic using 60-second phases and four readers failed its unchanged
90-second drain deadline. Queries kept completing and T/F reached zero, while
264 eligible historical snapshots remained. Its worker exited with a normal
Python error, and the watchdog did not expire. This is a separate, reproducible
reclamation-throughput issue, not evidence explaining the original silent stop.

The original signed wheels and scripts are frozen in an independent local
environment for continued reproduction while subsequent changes are developed.
A root-cause fix still requires a failed-execution stack or a deterministic
regression; repeated successful runs alone are insufficient.

## Qualification at `bb21bf8e`

The controlled stack-capture proof passed. With capture enabled, all four
existing CI load combinations completed 1,075 typed query checks and the
unchanged canary gates; no diagnostic capture was needed. The ten affected SQL
fixtures passed 255 assertion blocks. The Vane base release launcher passed
1,222 non-Ray and 32 Ray tests in separate processes, with 15 optional-dependency
skips. Formatting, syntax, documentation links and source/native copyleft checks
passed. Native production sources and signed provider bytes are unchanged.

The preceding PR's standalone native CI also exposed a maintenance-budget proof
that paused immediately after build collection, before the updated counters
were persisted. That exact assertion failure reproduced locally. The proof now
pauses at maintenance completion, when counters are durable and both collectors
have had their opportunity to run. Its unchanged assertions require exactly one
build collected, zero generations collected and one retired generation still
present with an object budget of one. This checks the shared limit across a
complete pass; production scheduling and budgets are unchanged.

All thirteen standalone native maintenance cases passed, including failure,
cancellation, shared object/byte budgets, publication crashes and seven cleanup
crash checkpoints.
