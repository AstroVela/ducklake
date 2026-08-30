//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_delete.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/ducklake_insert.hpp"
#include "storage/ducklake_delete_filter.hpp"
#include "storage/ducklake_metadata_info.hpp"
#include "common/ducklake_data_file.hpp"

namespace duckdb {
class DuckLakeTableEntry;
class DuckLakeDeleteGlobalState;
class DuckLakeTransaction;

//! A deletion position with the snapshot ID from which it became valid
struct PositionWithSnapshot {
	int64_t position;
	int64_t snapshot_id;

	bool operator<(const PositionWithSnapshot &other) const {
		return position < other.position;
	}
};

inline void MergeDeletesWithSnapshots(const DeleteFileScanResult &scan_result, idx_t fallback_snapshot,
                                      set<PositionWithSnapshot> &result) {
	for (idx_t i = 0; i < scan_result.deleted_rows.size(); i++) {
		PositionWithSnapshot pos_with_snap;
		pos_with_snap.position = static_cast<int64_t>(scan_result.deleted_rows[i]);
		if (scan_result.has_embedded_snapshots) {
			pos_with_snap.snapshot_id = static_cast<int64_t>(scan_result.snapshot_ids[i]);
		} else {
			pos_with_snap.snapshot_id = static_cast<int64_t>(fallback_snapshot);
		}
		result.insert(pos_with_snap);
	}
}

inline void MergeDeletesWithSnapshots(const DuckLakeDeleteData &delete_data, idx_t fallback_snapshot,
                                      set<PositionWithSnapshot> &result) {
	for (idx_t i = 0; i < delete_data.deleted_rows.size(); i++) {
		PositionWithSnapshot pos_with_snap;
		pos_with_snap.position = static_cast<int64_t>(delete_data.deleted_rows[i]);
		if (delete_data.HasEmbeddedSnapshots()) {
			pos_with_snap.snapshot_id = static_cast<int64_t>(delete_data.snapshot_ids[i]);
		} else {
			pos_with_snap.snapshot_id = static_cast<int64_t>(fallback_snapshot);
		}
		result.insert(pos_with_snap);
	}
}

//! Input parameters for writing a delete file
template <typename PositionType>
struct WriteDeleteFileInputBase {
	ClientContext &context;
	DuckLakeTransaction &transaction;
	FileSystem &fs;
	string data_path;
	string encryption_key;
	string data_file_path;
	set<PositionType> positions;
	DeleteFileSource source;
};

//! Input for writing a delete file without per-position snapshot IDs, used for deletion
using WriteDeleteFileInput = WriteDeleteFileInputBase<idx_t>;

//! Input for writing a delete file with per-position snapshot IDs, used for flushing
using WriteDeleteFileWithSnapshotsInput = WriteDeleteFileInputBase<PositionWithSnapshot>;

//! Util class for writing delete files
struct DuckLakeDeleteFileWriter {
	static DuckLakeDeleteFile WriteDeleteFile(ClientContext &context, WriteDeleteFileInput &input);
	static DuckLakeDeleteFile WriteDeleteFileWithSnapshots(ClientContext &context,
	                                                       WriteDeleteFileWithSnapshotsInput &input);
	//! Write a deletion vector file (puffin format) instead of a parquet delete file
	static DuckLakeDeleteFile WriteDeletionVectorFile(ClientContext &context, WriteDeleteFileInput &input);
};

struct DuckLakeDeleteMap {
	void AddExtendedFileInfo(DuckLakeFileListExtendedEntry file_entry) {
		auto filename = file_entry.file.path;
		file_map.emplace(std::move(filename), std::move(file_entry));
	}

	DuckLakeFileListExtendedEntry GetExtendedFileInfo(const string &filename) {
		auto delete_entry = file_map.find(filename);
		if (delete_entry == file_map.end()) {
			throw InternalException("Could not find matching file for written delete file");
		}
		return delete_entry->second;
	}

	optional_ptr<DuckLakeFileListExtendedEntry> TryGetExtendedFileInfo(const string &filename) {
		auto delete_entry = file_map.find(filename);
		if (delete_entry == file_map.end()) {
			return nullptr;
		}
		return &delete_entry->second;
	}

	optional_ptr<DuckLakeDeleteData> GetDeleteData(const string &filename) {
		lock_guard<mutex> guard(lock);
		auto entry = delete_data_map.find(filename);
		if (entry == delete_data_map.end()) {
			return nullptr;
		}
		return entry->second.get();
	}

	void ClearDeletes(const string &filename) {
		lock_guard<mutex> guard(lock);
		delete_data_map.erase(filename);
	}

	void AddDeleteData(const string &filename, shared_ptr<DuckLakeDeleteData> delete_data) {
		lock_guard<mutex> guard(lock);
		delete_data_map.emplace(filename, std::move(delete_data));
	}

private:
	mutex lock;
	unordered_map<string, DuckLakeFileListExtendedEntry> file_map;
	unordered_map<string, shared_ptr<DuckLakeDeleteData>> delete_data_map;
};

#ifdef DUCKLAKE_VANE_DISTRIBUTED
class DuckLakeDelete : public PhysicalOperator, public distributed::ExtensionWriteTaskProvider {
#else
class DuckLakeDelete : public PhysicalOperator {
#endif
public:
	DuckLakeDelete(PhysicalPlan &physical_plan, DuckLakeTableEntry &table, PhysicalOperator &child,
	               shared_ptr<DuckLakeDeleteMap> delete_map, vector<idx_t> row_id_indexes, string encryption_key,
	               bool allow_duplicates);

	//! The table to delete from
	DuckLakeTableEntry &table;
	//! A map of filename -> data file index and filename -> delete data
	shared_ptr<DuckLakeDeleteMap> delete_map;
	//! The column indexes for the relevant row-id columns
	vector<idx_t> row_id_indexes;
	//! The encryption key used to encrypt the written files
	string encryption_key;
	//! Whether or not we allow duplicate deletes
	bool allow_duplicates;

#ifdef DUCKLAKE_VANE_DISTRIBUTED
	distributed::DistributedExtensionWritePlan distributed_write_plan;
	string distributed_catalog_name;
	string distributed_schema_name;
	string distributed_table_name;
	string distributed_schema_uuid;
	string distributed_table_uuid;
	string distributed_data_path;
	string distributed_artifact_path;
	string distributed_field_identity;
	string distributed_partition_identity;
	string distributed_sort_identity;
	DuckLakeSnapshot distributed_snapshot;
	SchemaIndex distributed_schema_id;
	TableIndex distributed_table_id;
	DuckLakeSnapshot distributed_source_snapshot;
	vector<DuckLakeFileListExtendedEntry> distributed_source_files;
	optional_ptr<PhysicalOperator> distributed_worker_child;
	mutable atomic<bool> distributed_write_claimed {false};
	bool distributed_has_source_scan = false;
	bool distributed_source_prepared = false;
	bool distributed_source_is_statically_empty = false;
	bool distributed_worker_plan_selected = false;
#endif

public:
#ifdef DUCKLAKE_VANE_DISTRIBUTED
	optional_ptr<distributed::ExtensionWriteTaskProvider> GetExtensionWriteTaskProvider() override;
	const distributed::DistributedExtensionWritePlan &WritePlan() const override;
	void ValidateDistributedWrite(ClientContext &context) const override;
	idx_t FinalizeDistributedWrite(ClientContext &context,
	                               const vector<DistributedWriteTaskResult> &results) const override;
	void AbortDistributedWrite(ClientContext &context,
	                           const vector<DistributedWriteTaskResult> &selected_results) const override;
	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;
#endif

	// // Source interface
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}

	static PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner,
	                                    DuckLakeTableEntry &table, PhysicalOperator &child_plan,
	                                    vector<idx_t> row_id_indexes, string encryption_key,
	                                    bool allow_duplicates = true);

public:
	// Sink interface
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;

	bool IsSink() const override {
		return true;
	}

	bool ParallelSink() const override {
		return true;
	}

	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;

private:
#ifdef DUCKLAKE_VANE_DISTRIBUTED
	void InitializeDistributedSource(const DuckLakeSnapshot &source_snapshot,
	                                 vector<DuckLakeFileListExtendedEntry> source_files, bool source_prepared,
	                                 bool has_source_scan, bool source_is_statically_empty);
	void InitializeDistributedWritePlan(ClientContext &context);
	void SelectDistributedWorkerPlan();
	void ValidateDistributedWriteShape() const;
	DuckLakeTableEntry &ResolveDistributedWriteTable(ClientContext &context) const;
	void ValidateDistributedSourceBaseline(ClientContext &context, const DuckLakeTableEntry &target_table,
	                                       const string &operation_name, const string &worker_bind_data) const;

	static PhysicalOperator &PlanDeleteInternal(ClientContext &context, PhysicalPlanGenerator &planner,
	                                            DuckLakeTableEntry &table, PhysicalOperator &child_plan,
	                                            vector<idx_t> row_id_indexes, string encryption_key,
	                                            bool allow_duplicates, bool initialize_distributed_write);

	friend class DuckLakeInsert;
	friend class DuckLakeUpdate;
#endif

	void FlushDelete(DuckLakeTransaction &transaction, ClientContext &context, DuckLakeDeleteGlobalState &global_state,
	                 const string &filename, ColumnDataCollection &deleted_rows) const;
	void FlushDeleteWithSnapshots(DuckLakeTransaction &transaction, ClientContext &context,
	                              DuckLakeDeleteGlobalState &global_state, const string &filename,
	                              const DuckLakeFileListExtendedEntry &data_file_info,
	                              DuckLakeDeleteData &existing_delete_data, const set<idx_t> &sorted_deletes,
	                              DuckLakeDeleteFile &delete_file) const;
	//! Merge existing + new deletes into a single deletion vector (puffin) file
	void FlushMergedDeletionVector(DuckLakeTransaction &transaction, ClientContext &context,
	                               DuckLakeDeleteGlobalState &global_state, const string &filename,
	                               const DuckLakeFileListExtendedEntry &data_file_info,
	                               DuckLakeDeleteData &existing_delete_data, const set<idx_t> &sorted_deletes,
	                               DuckLakeDeleteFile &delete_file) const;
	//! Try to drop a file if all rows are deleted. Returns true if the file was dropped.
	bool TryDropFullyDeletedFile(DuckLakeTransaction &transaction, const DuckLakeDeleteFile &delete_file,
	                             const DuckLakeFileListExtendedEntry &data_file_info, idx_t delete_count) const;
};

} // namespace duckdb
