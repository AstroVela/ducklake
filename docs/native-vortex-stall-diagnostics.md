# Capture mixed-load stalls before termination

The [rewrite-planning follow-up](native-vortex-rewrite-planning.md) retained
a 512-row / 8D run that stopped completing queries while metadata samples and
budget rejections continued. Its last optimizer state was `building`, run 19,
with T = F = 128. The worker eventually reached its existing outer watchdog;
its log contained no native or Python stack to identify the blocked operation.
That original stop remains unresolved.

## Diagnostic boundary

The installed load probe now accepts `--capture-stalls`. This Linux diagnostic
requires GDB explicitly. The worker permits only its test parent and that
parent's descendants to attach; it does not change system ptrace policy. It
also registers a Python stack-dump signal for that worker.

The parent tracks completed queries separately from other events. Thirty
seconds without a completed query triggers capture even if metadata heartbeats
continue. Thirty seconds without any events, or reaching 25 seconds before the
existing watchdog, also triggers capture. Each case captures at most once.
Query-progress checks stop after the worker emits its measured-phase completion.

Capture records the latest task/source status, the last completed query's read
window and vector, and pending build-directory names in `stall.json`. GDB
records all native thread stacks in `native-stacks.log`; after detaching, the
worker writes its Python stacks to `worker.log`. GDB has a 20-second limit.
Capture failures remain visible in those files. The watchdog still terminates
at `3 * seconds_per_phase + 300`; capture does not extend that deadline.

Failure messages now include the worker exit status and whether the watchdog
expired. A successful report records whether a stack capture occurred. Captured
runs require separate consideration before using their timings as performance
measurements. This adds no execution retry, compatibility branch, fallback,
larger production budget or relaxed recall/drain gate.

The controlled waiting-worker proof checks that both native and Python stacks
are present and that diagnostic capture leaves the worker alive. CI runs that
proof, enables capture in its existing four load cases, and retains both the
controlled proof and any failure captures.

```sh
python -I test/vane/test_vane_vortex_stall_capture.py \
    --output-dir /tmp/vane-vortex-stall-capture

VANE_RUNNER=local-fast python -I test/vane/test_vane_vortex_sustained_load.py \
    --fixtures automatic --rows 512 --dimensions 8 --seconds 12 --capture-stalls \
    --output-dir /tmp/vane-vortex-stall-repro
```

## Investigation evidence

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

## Qualification and maintenance-budget proof

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
