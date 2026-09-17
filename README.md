# DuckLake for DuckDB and Vane

DuckLake stores table metadata in a SQL catalog and table data in Parquet.
This repository supports the ordinary DuckDB extension and a Vane provider
for distributed execution on Ray.

| Runtime | Guide | Execution |
| --- | --- | --- |
| DuckDB | [DUCKDB_README.md](docs/DUCKDB_README.md) | Native DuckDB extension |
| Vane | [VANE_README.md](VANE_README.md) | Default Ray runner, with distributed scans and writes |

The Vane guide starts with CTAS, INSERT, UPDATE, DELETE, and MERGE, then shows
SQL queries, the Python Relation API, and historical reads. Its examples
leave the runner unset and use SQLite metadata with file-backed table data.

Use the installation instructions for your runtime: DuckDB extension binaries
and Vane provider wheels are different artifacts. See the
[Vane integration notes](docs/vane.md) and [provider release guide](docs/vane-provider-release.md)
for source builds and qualification.

See the [DuckLake website](https://ducklake.select) for the format specification
and upstream documentation. This project is licensed under the [MIT license](LICENSE).
