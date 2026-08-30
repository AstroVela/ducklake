//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_distributed_write.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/distributed/copy_to_file.hpp"

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

string CreateDuckLakeDistributedArtifactPath(ClientContext &context, const string &data_path);

void ValidateDuckLakeDistributedArtifactPath(ClientContext &context, const string &data_path,
                                             const string &artifact_path);

void CleanupDuckLakeDistributedArtifactData(ClientContext &context, const string &data_path,
                                            const string &artifact_path);

void CleanupDuckLakeDistributedArtifacts(ClientContext &context, const string &data_path, const string &artifact_path);

void ValidateDuckLakeDistributedDataFileArtifacts(ClientContext &context, const string &data_path,
                                                  const string &artifact_path, const DuckLakeFieldData &field_data,
                                                  const vector<string> &partition_names,
                                                  const vector<distributed::DistributedCopyFileInfo> &files);

void RegisterDuckLakeDistributedWrites(ExtensionLoader &loader);

} // namespace duckdb
