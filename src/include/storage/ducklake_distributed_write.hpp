//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_distributed_write.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/ducklake_data_file.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/execution/distributed/copy_to_file.hpp"
#include "duckdb/execution/distributed/extension_write_task_provider.hpp"
#include "duckdb/function/distributed_write.hpp"

namespace duckdb {

class ClientContext;
class ColumnList;
class DuckLakeFieldData;
class DuckLakeInsertGlobalState;
class DuckLakeTableEntry;
class ExtensionLoader;
class FileSystem;
class PhysicalCopyToFile;
class PhysicalOperator;
class PhysicalPlanGenerator;
class ScalarFunction;
struct DuckLakePartition;
struct DuckLakeFileListExtendedEntry;
struct DuckLakeSnapshot;
struct DuckLakeSort;
class ParsedExpression;

enum class DuckLakeDistributedRowDeltaKind : uint8_t { DELETE = 0, UPDATE = 1, MERGE_INSERT = 2 };

struct DuckLakeDistributedDeleteFileResult {
	string data_file_path;
	string delete_file_path;
	DeleteFileFormat format = DeleteFileFormat::PARQUET;
	idx_t new_delete_count = 0;
	idx_t delete_count = 0;
	idx_t file_size_bytes = 0;
	idx_t footer_size_bytes = 0;
	idx_t pos_min_value = 0;
	idx_t pos_max_value = 0;
	optional_idx begin_snapshot;
	optional_idx max_snapshot;
};

struct DuckLakeDistributedRowDeltaResult {
	vector<distributed::DistributedCopyFileInfo> data_files;
	vector<string> data_file_artifact_roots;
	vector<DuckLakeDistributedDeleteFileResult> delete_files;
	unordered_set<string> selected_artifact_paths;
	idx_t affected_rows = 0;
};

unique_ptr<DuckLakePartition>
PlanDuckLakeDistributedCTASPartition(const ColumnList &columns, const DuckLakeFieldData &field_data,
                                     const vector<unique_ptr<ParsedExpression>> &partition_keys);

string CreateDuckLakeDistributedArtifactPath(ClientContext &context, const string &data_path);

string GetDuckLakeDistributedFieldIdentity(const DuckLakeFieldData &field_data);
string GetDuckLakeDistributedPartitionIdentity(const DuckLakePartition *partition_data);
string GetDuckLakeDistributedSortIdentity(const DuckLakeSort *sort_data);
bool DuckLakeDistributedSnapshotsMatch(const DuckLakeSnapshot &left, const DuckLakeSnapshot &right);
vector<string> GetDuckLakeDistributedPartitionNames(const PhysicalCopyToFile &copy);
void ValidateDuckLakeDistributedRowDeltaCopyShape(const PhysicalCopyToFile &copy);
void ValidateDuckLakeDistributedSnapshotBaseline(ClientContext &context, const string &catalog_name,
                                                 const DuckLakeSnapshot &expected_snapshot,
                                                 const string &operation_name);

void ValidateDuckLakeDistributedArtifactPath(ClientContext &context, const string &data_path,
                                             const string &artifact_path);

void CleanupDuckLakeDistributedArtifactData(ClientContext &context, const string &data_path,
                                            const string &artifact_path);

void CleanupDuckLakeDistributedArtifacts(FileSystem &file_system, const string &data_path, const string &artifact_path);

void CleanupDuckLakeDistributedArtifacts(ClientContext &context, const string &data_path, const string &artifact_path);

void ValidateDuckLakeDistributedDataFileArtifacts(ClientContext &context, const string &data_path,
                                                  const string &artifact_path, const DuckLakeFieldData &field_data,
                                                  const case_insensitive_set_t &not_null_fields,
                                                  const vector<string> &partition_names,
                                                  vector<distributed::DistributedCopyFileInfo> &files);

void ValidateDuckLakeDistributedDataFileArtifactsInRoot(ClientContext &context, const string &artifact_root,
                                                        const DuckLakeFieldData &field_data,
                                                        const case_insensitive_set_t &not_null_fields,
                                                        const vector<string> &partition_names,
                                                        vector<distributed::DistributedCopyFileInfo> &files,
                                                        bool expect_row_id = false);

PhysicalOperator &PlanDuckLakeDistributedRowDeltaRepartition(PhysicalPlanGenerator &planner, PhysicalOperator &input,
                                                             idx_t file_path_index);
PhysicalOperator &PlanDuckLakeDistributedRowDeltaRepartition(PhysicalPlanGenerator &planner, PhysicalOperator &input,
                                                             idx_t file_path_index,
                                                             const vector<idx_t> &null_file_path_partition_indexes);

string BuildDuckLakeDistributedDeleteBind(ClientContext &context, const DuckLakeTableEntry &table,
                                          const vector<DuckLakeFileListExtendedEntry> &source_files,
                                          const vector<idx_t> &row_id_indexes, const string &artifact_path,
                                          bool source_is_statically_empty);

string BuildDuckLakeDistributedUpdateBind(ClientContext &context, const DuckLakeTableEntry &table,
                                          const vector<DuckLakeFileListExtendedEntry> &source_files,
                                          const PhysicalCopyToFile &copy, idx_t copy_column_count,
                                          idx_t file_path_index, idx_t row_position_index, const string &artifact_path,
                                          bool source_is_statically_empty);

string BuildDuckLakeDistributedMergeInsertBind(ClientContext &context, const DuckLakeTableEntry &table,
                                               const PhysicalCopyToFile &copy, idx_t copy_column_count,
                                               const string &artifact_path, bool source_is_statically_empty);

void AddDuckLakeDistributedDataFiles(ClientContext &context, DuckLakeInsertGlobalState &global_state,
                                     const vector<distributed::DistributedCopyFileInfo> &files,
                                     optional_idx partition_id);

DistributedExtensionWriteCallbacks DuckLakeDistributedRowDeltaCallbacks();
ScalarFunction DuckLakeDistributedMergePartitionFunction();

DuckLakeDistributedRowDeltaResult
DecodeDuckLakeDistributedRowDeltaResults(ClientContext &context, const string &data_path, const string &artifact_path,
                                         const DistributedExtensionWriteInfo &info,
                                         const vector<DistributedWriteTaskResult> &results,
                                         DuckLakeDistributedRowDeltaKind expected_kind, bool expected_deletion_vectors);

void ValidateDuckLakeDistributedRowDeltaSourceBaseline(ClientContext &context, const DuckLakeTableEntry &target_table,
                                                       const vector<DuckLakeFileListExtendedEntry> &planned_files,
                                                       const string &worker_bind_data, const string &operation_name,
                                                       bool source_is_statically_empty);

vector<DuckLakeDeleteFile> BuildDuckLakeDistributedDeleteFiles(
    ClientContext &context, const vector<DuckLakeFileListExtendedEntry> &source_files, const string &worker_bind_data,
    const vector<DuckLakeDistributedDeleteFileResult> &files, const string &operation_name);

void CleanupDuckLakeDistributedRowDelta(ClientContext &context, const string &data_path, const string &artifact_path,
                                        const unordered_set<string> *paths_to_keep = nullptr);

void RegisterDuckLakeDistributedWrites(ExtensionLoader &loader);

} // namespace duckdb
