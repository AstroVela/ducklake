# Empty Vortex segment retirement and explicit zero-B views

This validation builds on [DuckLake #48](https://github.com/AstroVela/ducklake/pull/48).
A fully masked B can release its current physical row/vector quota without
constructing an empty SPFresh index. Source inventory, named-index revision and
generation references commit in the same native metadata transaction.

## DELETE and retirement

Native DELETE retains a complete delete mask for a committed source covered by
an index. The indexed file stays active, so current queries preserve coverage
and return only visible rows while retirement is pending. Other unindexed files
continue to use native full-file deletion. Final metadata commit rejects a file
drop if a concurrent index publication has since covered that source.

```sql
SELECT * FROM ducklake_retire_vortex_index_segment(
    'lake','main','items','knn','generation-<uuid>',<expected_revision>);
```

The explicit operation selects exactly one ready generation. It authenticates
its captured source files, schema and delete metadata and checks native row
visibility restricted to those source paths. Any visible row prevents
retirement. Under the existing write-admission/publication guards it validates
fresh committed inputs, the exact named-index revision and exclusive named-index
ownership. Independent T/F appends can commit during preparation. It rebases
the native snapshot, retires all selected source IDs and removes their index
references atomically. It creates no output data file, row map, workspace or
ANN generation. Other B descriptors and artifact bytes are retained.

The existing explicit data-compaction policy also selects an entirely empty B.
It journals a `retire` task in `retiring` and then `publishing` before the native
commit. The durable run UUID is the plan's publication identity. Recovery checks
both exact revision and publication identity; the absence of a selected B alone
cannot attribute another publisher's work to this task. Failures/cancellation
stop scheduling, committed acknowledgement loss records `published` and pauses,
and explicit resume reconciles interrupted work before fresh planning. Restart
starts no optimizer or maintenance worker.

## Explicit zero-B plan

Catalog segment manifest format 3 stores table UUID, vector field/dimension,
root, publication UUID and zero through three segment descriptors. A zero-B
plan retains the named index and its vector definition and has no generation.
There is no previous-version decoder or migration. Raw sealed SPFresh
references retain their existing format 1.

One ReadView captures all current files, inline rows and visibility. With zero
B it opens zero ANN leases and queries F and T through their normal exact
branches, applying visibility, category filtering, typed fetch and global top-k.
A T-only table needs no physical source buffer. A truly empty table captures
zero files/inline rows and returns no hits. Admission still rejects mismatched
table/vector definitions, invalid versions, inventory and budgets. A later F
batch can append the new first B under the same named index/revision protocol.

The existing three-B, 100,000-row, 64-MiB source/vector/artifact, inline,
manifest and write budgets remain unchanged. No compatibility layer, implicit
raw-vector fallback, new backend options, dependency pins or online SPFresh
mutation is introduced. A partially live B still uses the existing nonempty
rewrite constraints, including the native 65-row minimum.

## Retention and validation

Retired index generations use the independently authorized bounded collector.
Retired source files remain under native snapshot retention; retirement does
not authorize snapshot expiry or physical source cleanup. Open private views
retain query bytes and support delayed typed fetch after canonical index/source
GC. Historical source retention can still grow disk use until callers explicitly
expire snapshots and clean old files.

```sh
python3 -I test/vane/test_native_vortex_empty_retirement.py \
    --duckdb build/index-shell/duckdb \
    --driver build/index-shell/ducklake_vortex_catalog_driver \
    --probe build/index-cargo/ci/ducklake-vortex-index-probe \
    --output-dir build/index-logs/empty-retirement
```

The process proof covers three separate retirements, concurrent T/flush/T
appends, unchanged unselected generations, explicit 0-B empty/T/F queries,
rebuilding the first B, malformed plan rejection, retained search/fetch after
GC, quota recovery at 100,000 rows, stale DELETE/index publication races,
failure/cancel/lost-ack/publication races and six SIGKILL boundaries. A native
SQL case checks the public API, empty sources, snapshot retention and rebuilding.
The new proof is also part of the existing exact-pin native validation workflow.

## Local result (2026-10-29)

The process proof passed 17 checks and 84 gated query comparisons with minimum
fixture recall 1.0, including all six SIGKILL boundaries. Three separate B
retirements accepted concurrent appends and preserved unselected generations.
The last retirement left an explicit zero-B plan; empty/T-only/F+T reads and
rebuilding the new first B passed. Under the unchanged 100,000-row/vector limit,
retirement released all 4,096 B slots, retained 95,904 F rows and admitted a new T.
Old indexed and zero-B private views survived index and explicit source GC.

All 18 preceding native process proofs passed. ON passed 19 Vortex SQL cases /
706 assertions, including the new 42-assertion public-API case, plus Vortex
recovery. ON and OFF each passed 86 base cases / 3,582 assertions with 13
optional-extension skips and Parquet recovery. OFF exposes no retirement API.
Additional DELETE, deletion-inlining, audit, rewrite and orphan groups passed
48 cases / 2,231 assertions with five optional HTTPFS/ICU skips. Rust build,
formatting and Clippy with warnings denied passed. Four isolated cost cases
passed nine queries each with minimum recall 1.0; timings remain informational.

The qualified catalog driver SHA-256 is
`d756779172afb03cfc5f898b352aab128f6581e876c3cdd8a1489e3b9c6f8949`.
