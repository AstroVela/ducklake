//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_distributed_write.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/distributed/copy_to_file.hpp"
#include "duckdb/function/distributed_write.hpp"

namespace duckdb {

class ClientContext;
class ColumnList;
class DuckLakeFieldData;
class ExtensionLoader;
struct DuckLakePartition;
class ParsedExpression;

unique_ptr<DuckLakePartition>
PlanDuckLakeDistributedCTASPartition(const ColumnList &columns, const DuckLakeFieldData &field_data,
                                     const vector<unique_ptr<ParsedExpression>> &partition_keys);

void ValidateDuckLakeDistributedDataFileArtifacts(ClientContext &context, const string &data_path,
                                                  const DuckLakeFieldData &field_data,
                                                  const vector<string> &partition_names,
                                                  const vector<distributed::DistributedCopyFileInfo> &files,
                                                  vector<string> &cleanup_paths);

void CollectDuckLakeDistributedArtifactCleanupPaths(ClientContext &context, const string &data_path,
                                                    const vector<string> &partition_names,
                                                    const DistributedExtensionWriteInfo &write_info,
                                                    const vector<DistributedWriteTaskResult> &results,
                                                    vector<string> &cleanup_paths);

void RegisterDuckLakeDistributedWrites(ExtensionLoader &loader);

} // namespace duckdb
