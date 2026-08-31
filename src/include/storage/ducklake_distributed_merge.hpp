//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_distributed_merge.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#ifdef DUCKLAKE_VANE_DISTRIBUTED

#include "common/ducklake_snapshot.hpp"
#include "duckdb/execution/distributed/extension_write_task_provider.hpp"
#include "duckdb/execution/operator/persistent/physical_merge_into.hpp"
#include "storage/ducklake_metadata_info.hpp"

namespace duckdb {

class DuckLakeDelete;
class DuckLakeTableEntry;
class PhysicalCopyToFile;

struct DuckLakeDistributedMergePlanAction {
	MergeActionCondition match_condition = MergeActionCondition::WHEN_MATCHED;
	MergeActionType action_type = MergeActionType::MERGE_DO_NOTHING;
	unique_ptr<Expression> condition;
	vector<unique_ptr<Expression>> expressions;
	vector<unique_ptr<Expression>> projections;
	optional_ptr<PhysicalCopyToFile> copy;
	optional_ptr<DuckLakeDelete> delete_op;
	string encryption_key;
	optional_idx update_row_id_index;
	optional_idx update_file_path_index;
	optional_idx update_row_position_index;
};

struct DuckLakeDistributedMergeCoordinatorAction {
	MergeActionType action_type = MergeActionType::MERGE_DO_NOTHING;
	string worker_bind_data;
	vector<string> partition_names;
	optional_idx partition_id;
	vector<DuckLakeFileListExtendedEntry> source_files;
	bool source_prepared = true;
	bool has_source_scan = false;
	bool source_is_statically_empty = false;
	bool has_encryption = false;
};

class DuckLakeDistributedMergeInto final : public PhysicalMergeInto, public distributed::ExtensionWriteTaskProvider {
public:
	DuckLakeDistributedMergeInto(PhysicalPlan &physical_plan, vector<LogicalType> types,
	                             map<MergeActionCondition, vector<unique_ptr<MergeIntoOperator>>> actions,
	                             idx_t row_id_index, optional_idx source_marker, bool parallel, bool return_chunk);

	void ConfigureDistributedMerge(ClientContext &context, DuckLakeTableEntry &table,
	                               vector<DuckLakeDistributedMergePlanAction> actions, PhysicalOperator &worker_child,
	                               const vector<LogicalType> &worker_input_types, idx_t row_id_index,
	                               optional_idx source_marker);

	optional_ptr<distributed::ExtensionWriteTaskProvider> GetExtensionWriteTaskProvider() override;
	const distributed::DistributedExtensionWritePlan &WritePlan() const override;
	void ValidateDistributedWrite(ClientContext &context) const override;
	idx_t FinalizeDistributedWrite(ClientContext &context,
	                               const vector<DistributedWriteTaskResult> &results) const override;
	void AbortDistributedWrite(ClientContext &context,
	                           const vector<DistributedWriteTaskResult> &selected_results) const override;
	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;
	string GetName() const override;

private:
	DuckLakeTableEntry &ResolveDistributedTable(ClientContext &context) const;
	void ValidateDistributedShape() const;

private:
	distributed::DistributedExtensionWritePlan distributed_write_plan;
	vector<DuckLakeDistributedMergeCoordinatorAction> distributed_actions;
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
	optional_ptr<PhysicalOperator> distributed_worker_child;
	mutable atomic<bool> distributed_write_claimed {false};
	bool distributed_worker_plan_selected = false;
};

DistributedExtensionWriteCallbacks DuckLakeDistributedMergeCallbacks();

} // namespace duckdb

#endif
