//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_distributed_scan.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/table_function.hpp"

namespace duckdb {

void ConfigureDuckLakeDistributedScan(TableFunction &function);

} // namespace duckdb
