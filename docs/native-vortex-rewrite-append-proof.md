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
write admission and exclusive publication before opening the actual metadata
write transaction. That transaction validates the selected inputs, supplies the
commit snapshot and final file ID, and commits publication. A writer admitted
ahead of the rewrite is visible to validation and commit in both deferred and
immediate transaction modes. Concurrent flushes can consume the provisional
file ID during construction.

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

Private preparation and final sealing have distinct checkpoints. The build
registry remains `building` during both phases and records `published` in the
transaction that publishes the final generation. Recovery rechecks published
ownership before collecting the registered workspace.

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
acknowledged concurrent appends. In both transaction modes the proof queues the
rewrite behind an already-admitted writer and checks that the exact appended row
survives. It checks the build registry around both kinds of seal and the atomic
publication. The #46 proof exercises full-capacity reclamation, cancellation and
recovery with the late-binding implementation.

### Local qualification (2026-11-18)

Linux used DuckDB v1.5.5 (`d8cdaa33`), Vortex `b20e34c2`, native wrapper
`84cef204` and the static SPFresh SDK qualified by #46. The append/rewrite
proof at `70d4fba2` passed 31 checks in
76.7 seconds under the 180-second budget. All
132 gated queries attained recall 1.0.

The queued-writer regression preserved the exact committed row in deferred and
immediate transaction modes. Five staged rounds reached revision 8 at 549
physical rows. The provisional-to-final file IDs were 7→10, 12→15, 17→20, 22→25, 27→30.
The separate writer completed three T/flush/T rounds during construction and
advanced the final binding from 6 to 15.
Each rewrite built native bytes once and bound once. Both unselected B
generations kept their bytes and modification times. The prepared and published
native artifact digests matched in every checked round.

All seven crash cases retained acknowledged appends and recovered through
explicit maintenance. The registry remained building after private preparation
and final sealing, then published with the committed source and index. Old
private search and delayed typed fetch passed after both kinds of GC. The
indexed rewrite suite passed 21 checks and
88 gated queries.

All 17 native process suites and 19 functional checks passed.
Native ON passed 26 Vortex SQL cases with 1,031
assertions. Rewrite/orphan SQL passed 17 cases and
881 assertions. ON and OFF each passed 86 base cases with
3,582 assertions and 13 optional skips. ABI passed 32 checks.
Rust formatting, Clippy and workspace tests passed. The four query-cost cases
passed with minimum fixture recall 1.0. The catalog driver SHA-256
was `aaf3d1368e3698e1484e92079f19ba4dd32390823b772314e92c79ea2bc87c61`.

Earlier generation GC attempts reached the existing native call and suite
deadlines. The failure causes are undetermined. The rerun passed in
70.8 seconds. The matrix
resumed using the same binaries and limits.

Scope remains native DuckDB metadata, one local POSIX owner, exclusive managed
namespaces, one selected B, one nonempty output, one named index and an installed
write budget. Static replacements still need at least 65 physical rows. This
change adds no automatic data-compaction policy or SPFresh online Add/Delete API
and keeps all dependency pins unchanged.
