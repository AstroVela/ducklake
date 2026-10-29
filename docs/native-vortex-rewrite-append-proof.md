# Indexed-source rewrite with concurrent appends

This validation follows [DuckLake #46](https://github.com/AstroVela/ducklake/pull/46).
An indexed B can be rewritten while other connections commit T appends and flush
them into F. The operation uses the same explicit SQL API, constructs native
SPFresh bytes once, and binds the replacement row map to the final native file
ID before committing source replacement and index publication atomically.

## Selected-input validation

The capture retains the selected B's immutable source identities and a digest
of its active data-file metadata, delete-file metadata and bytes, inlined file
deletions, and target table/column metadata. Source contents must still match
their registered index identities. State inspection retains bounded row/byte
checks and charges data/delete-file reads to the existing source-byte ceiling.

The final publication guard checks this selected-input identity, table UUID,
table ID, the exact named-index revision/descriptor and the one-index constraint
against fresh native metadata. Updates or deletes affecting the selected B fail
explicitly; a concurrent index publication or alias installation also conflicts.
An appended T/F does not change these selected inputs. Global schema_version can
advance when an inlined table is created or an unrelated table changes; target
table/column identity remains the schema check. The native writer continues to
reject mutations of the fixed-schema Vortex table itself.

## Construction and final binding

Before filesystem output, the existing build and rewrite registrations own the
workspace, eventual generation and managed source path. Construction writes the
private Vortex output and trains SPFresh inside `.build-<id>/prepared`, which is
part of the registered workspace. A source wrapper records the exact physical
row-address order delivered to the native builder, under the existing 100,000-row
bound; binding does not assume a canonical scan order.

Once private construction finishes, the source is linked to its registered path
using the existing exclusive same-filesystem operation. The final phase holds
write admission and exclusive publication, validates selected inputs, and rebases
the dedicated native transaction to the freshly checked commit snapshot. Its
`next_file_id` is now the actual replacement ID, even if concurrent flushes used
the provisional ID during construction.

Binding authenticates the prepared generation, obtains verified private native
artifact copies, and uses the existing public `import_bundle` interface to retain
the dense native-ID order with the replacement file ID. It seals and reopens the
final generation, writes the new reference, and composes it with the unchanged
unselected B descriptors. It does not invoke native training again, mutate a
sealed generation or decode a second row-map format in DuckLake.

The normal native compaction commit then checks the actual file ID, coverage and
hashes. File inventory, named index, generation references, retained ownership
and the final diagnostic `new_file_id` commit in one metadata transaction. The
original capture snapshot remains diagnostic. There is one commit attempt and
no automatic rebuild/retry, compatibility layer, source-to-F downgrade or fallback.

Appends can commit during construction; final binding/import/verification and
publication serialize commits under the existing guard. This is not a latency
promise. All source/vector/artifact budgets and the three-B bound remain intact.

## Recovery and qualification

Prepared bytes are registered workspace garbage until publication. The same
explicit maintenance policy recovers stopped tasks and collects prepared/final
leftovers and abandoned managed outputs. Published source files stay under native
snapshot retention. Restart starts neither rewrite nor collection implicitly.
Old private query views remain usable after index GC, native snapshot expiry and
canonical source-file cleanup.

```sh
python3 -I test/vane/test_native_vortex_rewrite_append.py \
    --duckdb build/index-shell/duckdb \
    --driver build/index-shell/ducklake_vortex_catalog_driver \
    --probe build/index-cargo/ci/ducklake-vortex-index-probe \
    --output-dir build/index-logs/rewrite-append
```

The proof commits T, flushes it into F, then commits another T during each of five
rewrites. Every flush consumes the provisional file ID. It checks one native
build/one binding, equality of all six prepared/published native artifacts,
unchanged unselected B bytes and mtimes, final file-ID ownership, current B/F/T
search and old private search/typed fetch after both kinds of GC.

Additional cases cover selected-input delete/update conflicts, alias conflicts,
target-schema rejection, unrelated schema commits, writers waiting at final
publication, failure/cancellation at binding boundaries, corrupt prepared bytes,
and SIGKILL before/after final seal, reference staging and metadata commit with
acknowledged concurrent appends. The existing #46 proof retains full-capacity
reclamation and its earlier atomicity/recovery checks, with its independent
append expectations updated to the new behavior.

### Local result (2026-10-29)

The new proof passed 28 checks and 124 gated query comparisons with minimum
fixture recall 1.0. Five staged append/rewrite rounds advanced revision 3 to 8
and ended with 549 physical source rows, preserving every committed append.
Their provisional-to-final file IDs were 7→10, 12→15, 17→20, 22→25 and 27→30.
A separate writer executed three T/flush/T rounds concurrently with native
construction; its final file ID advanced 6→15. The five staged rounds and the live-writer case each recorded one native
construction and one binding; their six final native artifact digests matched
the prepared artifacts. Both unselected B generations kept their bytes
and mtimes. Snapshot row counts and distinct logical row IDs also matched.

All seven SIGKILL cases retained acknowledged appends, exposed one complete old
or new source/index state, and required explicit maintenance resumption. The
same-length prepared-file corruption case failed its checksum before binding.

- All 16 prior native process proofs passed, including the updated #46 proof.
- ON passed 17 Vortex SQL cases / 630 assertions, including 35 assertions for
  indexed rewrite. ON and OFF each passed 86 base cases / 3,582 assertions with
  13 optional-extension skips, plus Parquet recovery; ON passed Vortex recovery.
- Audit passed 122 assertions. Rewrite/orphan groups passed 16 cases / 854
  assertions with two ICU skips.
- Four isolated cost cases passed nine queries each, minimum recall 1.0;
  timings remain informational. Formatting, Rust formatting, Clippy with
  `--all-targets -- -D warnings`, and diff checks passed.

The qualified catalog driver SHA-256 is
`8f35048f4bb8cf8127a75d58dbf85aaaa896fbb34be04ee5ea0a4cd15628588c`.

Scope remains native DuckDB metadata, one local POSIX owner, exclusive managed
namespaces, one selected B, one nonempty output, one named index and an installed
write budget. Static replacements still need at least 65 physical rows. This
change adds no automatic data-compaction policy or SPFresh online Add/Delete API
and keeps all dependency pins unchanged.

The [background data-compaction follow-up](native-vortex-background-rewrite-proof.md)
adds an explicit policy using the existing single worker and this atomic handoff.
