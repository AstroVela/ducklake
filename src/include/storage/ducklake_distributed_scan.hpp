//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_distributed_scan.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/ducklake_metadata_info.hpp"
#include "common/ducklake_snapshot.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

class DuckLakeTableEntry;
struct MultiFileBindData;

void ConfigureDuckLakeDistributedScan(TableFunction &function);

bool TryGetDuckLakeDistributedMutationSource(const MultiFileBindData &bind_data, const DuckLakeTableEntry &target_table,
                                             DuckLakeSnapshot &snapshot, vector<DuckLakeFileListExtendedEntry> &files);

} // namespace duckdb
