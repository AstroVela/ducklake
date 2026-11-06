# Deterministic dimensional and query contracts

The machine's variable resource load defers R4 measurement work: identical-binary
A/A, optimization A/B and sustained-load qualification. Their earlier failed or
withdrawn results remain unchanged. This R5 slice uses functional assertions
with no latency, throughput, drain-duration or resource-steady-state gate.

## Fixed matrix

The [installed-provider test](../test/vane/test_vane_vortex_functional_matrix.py)
uses dimensions **1, 8, 128 and 4,096**, `k=1/64`, and six category choices:
unfiltered, `A`, lowercase `a`, empty string, quoted Unicode and absent category.
Vectors are finite, exactly representable float32 values; high dimensions include
nonzero components beyond the first eight. They form a bounded, deterministic
fixture, not a representative corpus for general ANN quality or capacity claims.

Each dimension has nine explicit phases:

1. Empty zero-B definition.
2. Sixty-four committed T rows, queried without waiting for ANN.
3. The same 64 rows flushed to F, still without ANN.
4. A 65th row and first immutable B generation.
5. Sixty-five more rows published as independent B2, preserving B1 bytes.
6. B1+B2+four unindexed F rows+three T rows.
7. Atomic upserts and deletes touching B1/B2/F/T while an ordinary native
   transaction retains its old typed rows.
8. Every earlier request replayed with its original receipt; no resurrection or
   deletion of newer versions.
9. Reopened installed-provider connection with the same committed state.

Each phase checks all twelve category/k combinations through both named search
and its actual-ReadView diagnostic entry point: **432 ordinary and 432 diagnostic
queries** overall. The independent client mutation model must equal all native
typed rows before each phase. Every hit must match that model's visible version,
dimension, filter and independently computed squared L2, with ranked distances
and unique business keys. Zero-B results require exact top-k; B results keep the
existing fixture recall >=0.8 requirement. Diagnostic hits must equal ordinary
hits and use the expected committed snapshot. Check actual B/F/T routes, unchanged
generation bytes, and release of private query staging.

Each dimension also rejects ten malformed queries and four invalid mutations:
k 0/65, empty/4,097/mismatched dimensions, nonfinite/NULL components, oversized and
NULL category arguments; invalid mutation vectors, operation counts and bytes.
The **56 rejections** must preserve typed data, and invalid mutations must leave
no receipt. No input is padded to qualify for ANN and no budget is raised.

This retains `SET threads=2`, existing SPFresh settings and thread variables,
native transaction protections and hard budgets. Local runs inherit CPU **6–11**;
CI explicitly selects an available CPU. Affinity is recorded and restored, and
does not require idle or exclusive cores. Source/ANN maintenance is driven by
explicit operations, not timers or a background convergence deadline.

The [oracle checks](../test/vane/test_vane_vortex_functional_oracle.py) verify
float32 fixture representation and reject incorrect visibility, stale typed
versions, distances, filtering, duplicates, ordering, missing exact hits and
ANN recall below the existing threshold.

## Reproduce

Use a non-editable installed Vane with the matching DuckLake/Vortex providers
and a fresh output directory:

```sh
VANE_RUNNER=local-fast python -I test/vane/test_vane_vortex_functional_oracle.py
VANE_RUNNER=local-fast python -I test/vane/test_vane_vortex_functional_matrix.py \
    --cpus 6,7,8,9,10,11 --output-dir /tmp/vortex-functional-matrix
```

The report retains native identity, CPU/thread settings, each actual snapshot,
route, ranked IDs and recall. Elapsed time is observational and never controls
acceptance. Failure stops the run and preserves partial evidence.

## Result

Pending the fixed functional run. R4 is deferred, not complete. This slice does
not implement request expiry, reference/WAL reclamation, larger data admission,
general predicates, distributed ANN or SPFresh native online updates.
