# SPFresh head planning within the posting budget

The [sustained-load baseline](native-vortex-sustained-load.md) exposed missing
native IDs during static construction. The shared DuckLake builder requested
64 heads for every physical row count and vector width. SPFresh's pinned static
builder sorts each posting by distance and cuts it at `PostingPageLimit`.
If every replica of a non-head vector falls outside those prefixes, the native
output loses that ID. The existing coverage validator correctly rejects it.

## Root-cause reproduction

A standalone call to the unchanged native bridge used the automatic input in
ascending business-ID order, 4,096 rows, 128 Float32 components, 64 heads, four
replicas and 12 pages per posting. Each record needs `4 + 4 * 128 = 516` bytes,
so a posting can hold only 95 records. The result capped 51 postings, removed
4,792 posting records and omitted 603 distinct IDs from the union of head IDs
and postings.

A separate diagnostic build with a 256-page limit selected exactly the same
head IDs. Every 12-page posting equaled the corresponding diagnostic posting's
first 95 records, and the diagnostic output covered every ID. That proves this
failure was native posting truncation, rather than an incorrect row map or a
validator parsing error. The larger-page build was an offline diagnostic;
production construction and queries still use 12 pages.

## Build plan

Before its single native build, DuckLake derives the head count from the
captured physical row count `N` and dimension `d`:

```text
record_bytes = 4 + 4*d
head_count = clamp(ceil(N * 4 replicas * record_bytes * 4 / (12 * 4096)), 64, N-1)
```

This targets one quarter of each posting's capacity using the maximum replica
count. It reserves space for skew instead of assuming uniform assignment. The
head count uses all physical rows in the selected build input, including rows
that are currently deleted; a source rewrite uses its newly prepared input.
It does not use the whole table when constructing an independent new B or a
selected merge. Existing unselected B generations remain unchanged.

| Physical rows | Dimension | Heads |
| ---: | ---: | ---: |
| 65 | 8 | 64 |
| 4,096 | 8 | 64 |
| 32,768 | 8 | 384 |
| 4,096 | 128 | 688 |
| 32,768 | 128 | 5,504 |

The minimum build remains 65 rows. The global 100,000-row, 64 MiB
source/vector/artifact and three-B ceilings, four-replica limit, 12-page
build/search limits and query search budgets remain unchanged. The Vortex,
wrapper and SPFresh dependency pins and native format are unchanged. There is
one planned build, no larger-page retry, alternate backend or raw-vector path.

This is a capacity estimate, not a coverage guarantee for arbitrary data.
The full mapped-ID validator still runs before import, reopen and publication;
extreme skew or a different native assignment may still fail explicitly. A
half-full estimate remained insufficient for the cost input in the diagnostic
matrix, which motivated the larger fixed margin. No failed native output is
accepted or repaired by weakening validation.

More heads consume additional BKT memory and build CPU. Very wide vectors may
drive the head count close to the row count. The existing logical and artifact
ceilings do not establish a hard native RSS or latency bound. Broader ANN recall,
long-run liveness and SPFresh native online updates remain separate work.

## Reproduction and evidence

The minimal `native_vortex_posting_coverage.test` inserts the 4,096-row / 128D
input, builds and publishes it, checks a typed named ANN result, then detaches
and reopens the catalog to check the filtered query. The preceding signed
provider fails its build statement with the mapped-ID coverage error.

The installed mixed-load probe retains the baseline workload and oracle:

```sh
VANE_RUNNER=local-fast python -I test/vane/test_vane_vortex_sustained_load.py \
    --fixtures automatic cost --rows 4096 32768 --dimensions 8 128 --seconds 12 \
    --output-dir /tmp/vane-vortex-posting-matrix
```

CI exercises both 8D and 128D at 4,096 rows and retains the complete per-case
reports, worker logs and query event trace. Matrix failures still produce a
nonzero aggregate exit; exploration quality is reported separately from the
existing fixed-canary recall gate and typed visibility checks.

## Local qualification on 2026-10-30

The final signed-provider matrix used the same two input families, 4,096 and
32,768 rows, 8D and 128D, two readers and 12 seconds per phase as the preceding
baseline. All eight initial B builds succeeded. No mapped-ID coverage failure
occurred during initial construction, append, merge or source rewrite. The six
coverage failures in the preceding matrix did not recur.

The full command still exited 1 after 1,018 seconds because other acceptance
checks failed:

| Input | Rows | Dimension | Full-probe result | Canary minimum | Exploration minimum |
| --- | ---: | ---: | --- | ---: | ---: |
| automatic | 4,096 | 8 | Completed; typed consistency and exact replay passed | 1.0 | 0.6 |
| automatic | 4,096 | 128 | Completed; typed consistency and exact replay passed | 1.0 | 0.3 |
| automatic | 32,768 | 8 | Completed; typed consistency and exact replay passed | 1.0 | 1.0 |
| automatic | 32,768 | 128 | Drain exceeded 90 seconds; final oracle unqualified | — | — |
| cost | 4,096 | 8 | Completed; typed consistency and exact replay passed | 0.9 | 0.7 |
| cost | 4,096 | 128 | Typed consistency and exact replay passed; canary gate failed | 0.7 | 0.2 |
| cost | 32,768 | 8 | Completed; typed consistency and exact replay passed | 1.0 | 1.0 |
| cost | 32,768 | 128 | Drain exceeded 90 seconds; final oracle unqualified | — | — |

The six completed workers checked 2,552 query results against the public-window
oracle, including the worker that failed only its canary gate. Both 32,768-row /
128D workers published their indexes, reached T = F = 0, and reported healthy
optimizer state, but retained eligible snapshots when the drain deadline
expired. Their final rows/replay and query-quality checks did not complete.
The deadline and query workload were not changed to accept them.

The 13-block SQL regression failed at native construction on the preceding
signed provider and passed on the new provider, including detach/reopen and the
filtered typed ANN result. The seven affected SQL fixtures passed all 180
assertion blocks through the installed-provider probe. Rust formatting and
workspace clippy with all targets/features and warnings denied passed.

The rebuilt driver and signed provider also passed the automatic-flow proof
(80 query comparisons, four native cases and four SIGKILL recovery points),
source retention (16 comparisons and 20 native/recovery cases), and the basic
installed flow (32 comparisons), with minimum fixture recall 1.0 throughout.
The Vane base release launcher passed 1,222 non-Ray and 32 Ray tests in separate
processes, with 15 optional-dependency skips. Repository formatting and Vane
source/native copyleft checks passed.

Coverage construction is qualified for these inputs. Broader capacity and
search quality remain unqualified: reclamation under the larger 128D load and
the existing 0.8 recall gate need separate work. There is no claim that this
head estimate accepts every distribution within the row/vector ceilings.
