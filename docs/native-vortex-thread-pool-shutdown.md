# Consume the SPFresh thread-pool shutdown fix

The preceding signed-provider long load captured an optimizer stuck in
`vortex_spfresh_close`, joining a SPFresh pool worker that was waiting on its
condition variable. Query opens then waited for the bridge's shared native
mutex. The native worker's plain stop flag was written outside the queue mutex
used to check the wait predicate.

[Vortex #34](https://github.com/AstroVela/vortex/pull/34) fixes this shutdown
protocol: initialize an atomic stop flag, publish stop and notify while holding
the queue mutex, then release that mutex before joining workers. An independent
test of the real pool reproduces the old join/condition-wait hang. The exact new
regression reports races against the old helper and passes against the fix.
The fixed helper completes 100,000 shutdowns; focused Helgrind and
ThreadSanitizer checks report no errors. Five C++ groups, twenty native Rust
tests, two non-native tests and repeated concurrent lifetime checks pass.

Pin the native SDK source gate, query library, standalone probe and tracked root
lockfile to Vortex `4b396b4e88909cd7c87e3b27af733947fe8e8808`. Pin the adapter
to `4ddaeeecf64a659cfb6b691bd3b917f4f189f2a2`, the paired
[duckdb-vortex #39](https://github.com/AstroVela/duckdb-vortex/pull/39), which
updates both independent workspaces and strict provider-source validation.
Update CI's checkout revisions with these same exact pins.

The bridge's serialization, source-admission rules, read views, ANN search
budgets, write/maintenance budgets and load deadlines remain unchanged. There
is no compatibility layer, additional accepted revision, retry or fallback.

## Qualification boundaries

Native helper qualification isolates a shutdown defect; the installed provider
pair must separately exercise the complete automatic flow and long load.
Rebuild and sign both provider wheels because the Vortex source revision changes
their dependency closure. Preserve the existing Vane base wheel and qualify in
a new isolated installed environment.

The original retained 4,096-row/128D cost canary has a 0.7 lower bound under the
unchanged 0.8 gate. Offline replay confirms failures even at a single committed
watermark, without boundary ties. Final retained native generations return the
expected canary neighbors, so those later generations do not establish the
cause of the earlier failure.

A separate diagnostic then retained the failing generations at a stable commit
watermark. Its 512-row B contains only current visible rows and all ten correct
global neighbors. Native replay returns exactly the live query's ten business
IDs and reproduces its 0.7 recall. Disabling the unrestricted nearest-head
distance cutoff recovers 1.0 on the same bytes and budgets. That independent
correction is qualified in [Vortex #35](https://github.com/AstroVela/vortex/pull/35)
and is not part of this consumer pin. The cost gate and original uncaptured
silent stop remain separate from the qualified native shutdown regression.

## Installed-provider qualification

The new isolated environment contains the unchanged Vane base wheel and newly
signed Vortex and DuckLake test-only wheels with an exact dependency closure.
All five public flows pass 172 query-oracle comparisons with a minimum recall
of 0.9. Seven write-recovery, twenty-four source, thirteen keyed and nine
automatic native cases pass, as do thirteen standalone maintenance fault/budget
cases. Ten SQL fixtures pass 261 assertion blocks. The controlled opt-in stall
capture proof also passes.

All six short automatic-load combinations pass 2,426 query-window checks, with
canary and exploration recall lower bounds of 1.0 in every case. No stall capture
is needed. The unchanged Vane base launcher passes 1,222 non-Ray and 32 Ray tests
in separate processes, with fifteen optional-dependency skips. Formatting,
locked dependency metadata, shell syntax and copyleft checks pass.

## Original long-load gate

The separate 60-second-per-phase, four-reader automatic load passes on this
signed provider pair. All 6,754 typed query-window checks have canary and
exploration recall lower bounds of 1.0. T, F, old build/generation artifacts and
eligible source snapshots drain under the original budgets. Source cleanup
finishes in 82.46 seconds, inside the unchanged 90-second drain deadline.

The run uses the original 0.02-second query pause and 0.5-second recovery/drain
pause. No other owned builds or load tests run concurrently. No stall capture
or watchdog expiration occurs; the worker watchdog remains 480 seconds. This
qualifies the previously stalled workload without increasing a deadline or
search/maintenance budget. It does not establish production throughput or
identify the earlier uncaptured silent stop's cause. The separate 4,096-row/128D
cost canary still requires the following exact consumer pin for Vortex #35.
