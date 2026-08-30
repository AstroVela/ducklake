# Vane distributed scans

DuckLake has an explicit Vane build mode for distributed, file-backed table scans. The ordinary DuckDB extension
build remains the default and does not compile Vane headers or distributed scan sources.

Initialize the independent CI tooling and build against the exact Vane and vcpkg revisions in `vane-extension.toml`:

```shell
git submodule update --init --recursive
VCPKG_TOOLCHAIN_PATH='<vcpkg>/scripts/buildsystems/vcpkg.cmake' make vane_ci
```

`make vane_wheel` builds a wheel containing the statically linked DuckLake extension. The Vane pipeline verifies the
native backend and a two-worker Ray scan from that packaged wheel.

Distributed scans support committed, unencrypted data files and committed delete state. Inlined data,
transaction-local changes, encrypted files, change scans, and legacy Parquet VARIANT decoding are rejected before
worker execution. Ray execution does not fall back to a local scan.

When a local DuckDB file stores the metadata catalog, attach DuckLake read-only before Ray queries so the coordinator
and Ray driver can open the catalog concurrently. Workers receive only the serialized scan state and never attach the
metadata catalog.
