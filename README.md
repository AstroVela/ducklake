# DuckLake for DuckDB and Vane

DuckLake stores table metadata in a SQL catalog and table data in Parquet.
This repository supports the ordinary DuckDB extension and a Vane provider
for distributed execution on Ray.

| Runtime | Guide | Execution |
| --- | --- | --- |
| DuckDB | [DUCKDB_README.md](docs/DUCKDB_README.md) | Native DuckDB extension |
| Vane | [VANE_README.md](VANE_README.md) | Default Ray runner, with distributed scans and writes |
