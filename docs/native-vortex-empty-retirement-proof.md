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

The explicit operation requires a writable autocommit caller and binds its
DuckLake and metadata attachments. It selects exactly one ready generation
and checks each source against its active file ID, resolved path, row count
and content hash. It proves emptiness from the union of inline and file delete
positions. Any visible row prevents retirement. Independent T/F appends can
commit during preparation. Under the existing write-admission and publication
guards it refreshes the native write transaction, repeats the input and
visibility checks, and validates the exact revision and sole named-index
ownership. Source IDs and index references retire in that same transaction.
Other B descriptors and artifact bytes are retained.

The existing explicit data-compaction policy also selects an entirely empty B.
It journals a `retire` task in `retiring` and then `publishing` before the native
commit. The durable run UUID is the plan's publication identity. The same
transaction stores a receipt for the run and revision. Recovery can reconcile
that receipt after another publisher advances the index. Failures and
cancellation stop scheduling. Committed acknowledgement loss records
`published` and pauses. Explicit resume reconciles interrupted work before
fresh planning.

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
caller catalog replacement, relocated sources, failure/cancel/lost-ack
and six SIGKILL boundaries. A later append checks that the receipt still
identifies the committed retirement after restart. The native SQL case also
covers explicit and read-only transactions, a business column named
`filename`, and whole-file DELETE after a vector column rename.

## Local verification against #48

The native ON and OFF builds were verified on DuckLake `1b61c19c`, which
incorporates #48 at `9563536f`. The dependency pins are DuckDB
`d8cdaa33`, Vortex `b20e34c2`, and the DuckDB Vortex
wrapper `84cef204`.

Empty-segment retirement passed 20 checks and
88 query comparisons with minimum recall
1.0 in 37.55 seconds. Background
rewrite passed 30 checks and 124
comparisons with minimum recall 1.0 in
90.73 seconds. Both used the existing 180-second budget.
The retirement SQL case passed 64 assertions.

All 19 native process proofs passed. ON passed
28 Vortex SQL cases and 1144
assertions. The base matrix passed 86 cases and
3582 assertions on ON and 86 cases
and 3582 assertions on OFF, with
13 optional tests skipped on each configuration.
The deletion-inlining, audit, rewrite and orphan suites passed
37 cases and 2119 assertions. Recovery and cleanup
checks, the 32-case binary ABI matrix, the isolated cost
checks, formatting, Clippy with warnings denied, and Rust workspace tests
passed. The complete DELETE group passed 13 cases
and 156 assertions.

The qualified catalog driver SHA-256 is
`37faebf0669fb2ebb31443222995ca01b5f83fa2b5b6fa7c4a66b61c61b3036d`.
