# Consume the unrestricted SPFresh posting-radius correction

The preceding consumer qualifies native shutdown, the original long workload's
liveness and its canary. Two recall results remain below their existing targets:
the 4,096-row/128D cost canary reaches 0.7, and the longer automatic workload's
exploration lower bound reaches 0.6. Both targets remain 0.8.

Offline replay of the long trace finds seventeen exploration queries below the
target, all unrestricted. Fifteen have an unchanged commit watermark, and even
the best compatible committed state reaches only 0.6. These failures are not
created by choosing a conservative state within an ambiguous read window.

## Causal native regression

A separate diagnostic preserved the cost canary's failing immutable generations.
Its 512-row B contains only current visible rows and all ten correct global
neighbors. Replaying that B returns exactly the live query's ten business IDs
and reproduces 0.7 recall. Removing the nearest-head distance cutoff restores
1.0 on the same native bytes with `max_check=4096`, `internal_results=64` and
twelve posting pages. An all-allowed bitmap was used only to isolate the old
cutoff during diagnosis.

A head's distance does not bound the distance to rows in other postings.
[Vortex #35](https://github.com/AstroVela/vortex/pull/35) therefore extends the
existing static masked-search correction to unrestricted searches. It retains
valid postings selected by the same explicit budgets. Its unfiltered C ABI
regression checks the exact top ten for 128 offset queries with all 64 heads
inside the budget. The exact new regression fails against the preceding SDK.

The native correction passes five C++ groups, twenty native Rust tests, two
non-native tests and strict backend clippy. A separate all-heads-fit diagnostic
changes forty-six misses out of sixty-four queries to zero. All three replayed
native bundle file sets remain byte-identical.

## Exact consumer closure

Pin the native SDK source gate, query library, standalone probe and tracked root
lockfile to Vortex `122b63e0408a187a9d0ecdd129f1cbe12b6b46b2`. Pin the adapter
to `7eca1d48632d2e9d5c20d83a139f28241de98797`, the paired
[duckdb-vortex #40](https://github.com/AstroVela/duckdb-vortex/pull/40), and update
CI's checkouts to the same revisions. Both adapter workspaces and strict source
validation accept exactly this one Vortex revision.

Rebuild and sign both provider wheels with their exact dependency closure, and
qualify them in a new isolated environment with the unchanged Vane base wheel.
The thread-pool shutdown correction, source admission, immutable generations,
read views, write and maintenance budgets remain in place. There is no new
option, format, compatibility layer, bitmap workaround, retry or fallback.

The full qualification retains all public flows, SQL and recovery fixtures, the
six short automatic loads, both 128D cost cases, and the original longer load.
Canary and exploration results are reported independently against their existing
0.8 targets. The 90-second drain gate, 480-second long worker watchdog and query
pacing remain unchanged.

## Installed-provider regression checks

The new isolated provider pair passes all five public flows: 172 query-oracle
comparisons with a minimum recall of 0.9, seven write-recovery cases,
twenty-four source cases, thirteen keyed cases and nine automatic native cases.
Thirteen standalone maintenance fault/budget cases and ten SQL fixtures with
261 assertion blocks pass. The controlled opt-in stall-capture proof also passes.

The Vane base wheel is byte-identical to the preceding qualification. Both
provider wheels have new content identities, and DuckLake requires the exact
new Vortex wheel. All six short automatic-load cases pass 2,378 query-window
checks, with canary and exploration lower bounds of 1.0. Both 128D cost cases
also pass: 280 checks at 4,096 rows and 66 at 32,768 rows, with both lower bounds
at 1.0. The previously failing 4,096-row canary therefore meets the unchanged
0.8 gate on the installed provider pair. None of these runs needs a stall capture.

The unchanged Vane base launcher passes 1,222 non-Ray and 32 Ray tests in separate
processes, with fifteen optional-dependency skips. Formatting, locked source
metadata, shell syntax, copyleft and diff checks pass. The original longer
workload is evaluated separately as follows.

## Original long-load acceptance

The 60-second-per-phase, four-reader automatic workload passes 6,872 typed
query-window checks on this exact installed provider pair. Canary recall has a
lower bound of 1.0. Exploration recall has a lower bound of 0.8, meeting its
existing target and improving the preceding 0.6 result. It is not uniformly 1.0.

T, F, old build/generation artifacts and eligible source snapshots drain in
73.68 seconds, inside the unchanged 90-second deadline. The run retains the
0.02-second query pause, 0.5-second recovery/drain pause and 480-second worker
watchdog, with no concurrent owned builds or other load tests. No stall capture
or watchdog expiration occurs.

An independent final summary explicitly checks both existing 0.8 recall targets
for all nine load cases and the original long-run drain deadline. The six short
cases, two cost cases and longer workload complete 9,596 typed query-window
checks in total. The lower bounds, including the longer exploration result of
0.8, are retained separately from the harness's overall completion status.

## Cost and scope

Queries can visit more postings within the same budgets. Repeated native C ABI
calls including Python overhead measured p50 95.5 to 161.9 microseconds on the
frozen failure case while compilers were active. This is a diagnostic timing,
not a performance qualification. Search remains approximate outside the bounded
regression. Native replay does not replace installed-provider recall, recovery
or convergence acceptance, or establish production throughput.
