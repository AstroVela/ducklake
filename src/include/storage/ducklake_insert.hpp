//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_insert.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/persistent/physical_copy_to_file.hpp"

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/common/index_vector.hpp"
#include "storage/ducklake_stats.hpp"
#include "common/ducklake_data_file.hpp"
#include "storage/ducklake_field_data.hpp"
#ifdef DUCKLAKE_VANE_DISTRIBUTED
#include "common/ducklake_snapshot.hpp"
#include "duckdb/execution/distributed/extension_write_task_provider.hpp"
#endif

namespace duckdb {
class DuckLakeCatalog;
class DuckLakeSchemaEntry;
class DuckLakeTableEntry;
class DuckLakeFieldData;
struct DuckLakePartition;
struct DuckLakeCopyOptions;
struct DuckLakeCopyInput;

enum class InsertVirtualColumns { NONE, WRITE_ROW_ID, WRITE_SNAPSHOT_ID, WRITE_ROW_ID_AND_SNAPSHOT_ID };

class DuckLakeInsertGlobalState : public GlobalSinkState {
public:
	explicit DuckLakeInsertGlobalState(DuckLakeTableEntry &table);

	DuckLakeTableEntry &table;
	vector<DuckLakeDataFile> written_files;
	idx_t total_insert_count;
	case_insensitive_set_t not_null_fields;
	//! Total rows flushed (used by flush_inlined_data)
	idx_t rows_flushed = 0;
};

#ifdef DUCKLAKE_VANE_DISTRIBUTED
class DuckLakeInsert : public PhysicalOperator, public distributed::ExtensionWriteTaskProvider {
#else
class DuckLakeInsert : public PhysicalOperator {
#endif
public:
	//! INSERT INTO
	DuckLakeInsert(PhysicalPlan &physical_plan, const vector<LogicalType> &types, DuckLakeTableEntry &table,
	               optional_idx partition_id, string encryption_key);
	//! CREATE TABLE AS
	DuckLakeInsert(PhysicalPlan &physical_plan, const vector<LogicalType> &types, SchemaCatalogEntry &schema,
	               unique_ptr<BoundCreateTableInfo> info, string table_uuid, string table_data_path,
	               string encryption_key);

	//! The table to insert into
	optional_ptr<DuckLakeTableEntry> table;
	//! Table schema, in case of CREATE TABLE AS
	optional_ptr<SchemaCatalogEntry> schema;
	//! Create table info, in case of CREATE TABLE AS
	unique_ptr<BoundCreateTableInfo> info;
	//! The table UUID, in case of CREATE TABLE AS
	string table_uuid;
	//! The table data path, in case of CREATE TABLE AS
	string table_data_path;
	//! The partition id we are writing into (if any)
	optional_idx partition_id;
	//! The encryption key used for writing the Parquet files
	string encryption_key;

#ifdef DUCKLAKE_VANE_DISTRIBUTED
	distributed::DistributedExtensionWritePlan distributed_write_plan;
	string distributed_catalog_name;
	string distributed_schema_name;
	string distributed_table_name;
	string distributed_schema_uuid;
	string distributed_table_uuid;
	string distributed_data_path;
	string distributed_field_identity;
	string distributed_partition_identity;
	string distributed_sort_identity;
	DuckLakeSnapshot distributed_snapshot;
	SchemaIndex distributed_schema_id;
	TableIndex distributed_table_id;
	vector<string> distributed_partition_names;
	shared_ptr<DuckLakeFieldData> distributed_ctas_field_data;
	unique_ptr<DuckLakePartition> distributed_ctas_partition;
	optional_ptr<PhysicalOperator> distributed_worker_child;
	bool distributed_target_initialized = false;
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

	void ConfigureDistributedInsert(ClientContext &context, PhysicalCopyToFile &worker_copy);
	void ConfigureDistributedCTAS(ClientContext &context, PhysicalCopyToFile &worker_copy,
	                              shared_ptr<DuckLakeFieldData> field_data,
	                              unique_ptr<DuckLakePartition> partition_data);
	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;
#endif

	// // Source interface
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}

	static void InsertCasts(const vector<LogicalType> &types, ClientContext &context, PhysicalPlanGenerator &planner,
	                        optional_ptr<PhysicalOperator> &plan);
	static unique_ptr<LogicalOperator> InsertCasts(Binder &binder, unique_ptr<LogicalOperator> &plan);

	static DuckLakeColumnStats ParseColumnStats(const LogicalType &type, const vector<Value> &stats);
	static DuckLakeCopyOptions GetCopyOptions(ClientContext &context, DuckLakeCopyInput &copy_input);
	static PhysicalOperator &PlanCopyForInsert(ClientContext &context, PhysicalPlanGenerator &planner,
	                                           DuckLakeCopyInput &copy_input, optional_ptr<PhysicalOperator> plan);
	static PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner,
	                                    DuckLakeTableEntry &table, string encryption_key);
	static void AddWrittenFiles(DuckLakeInsertGlobalState &gstate, DataChunk &chunk, const string &encryption_key,
	                            optional_idx partition_id, bool set_snapshot_id = false);

	static const DuckLakeFieldId &GetTopLevelColumn(DuckLakeCopyInput &copy_input, FieldIndex field_id,
	                                                optional_idx &index);

public:
	// Sink interface
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	// SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

	bool IsSink() const override {
		return true;
	}

	bool ParallelSink() const override {
		return false;
	}

	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;

private:
#ifdef DUCKLAKE_VANE_DISTRIBUTED
	void InitializeDistributedWritePlan();
	void InitializeDistributedWriteTarget(ClientContext &context, DuckLakeTableEntry &table_entry);
	void SelectDistributedWorkerPlan();
	void ValidateDistributedWriteShape() const;
	DuckLakeSchemaEntry &ResolveDistributedWriteSchema(ClientContext &context) const;
	DuckLakeTableEntry &ResolveDistributedWriteTable(ClientContext &context) const;
	DuckLakeTableEntry &CreateDistributedCTASTable(ClientContext &context) const;
#endif
};

struct DuckLakeCopyOptions {
	DuckLakeCopyOptions(unique_ptr<CopyInfo> info, CopyFunction copy_function);

	unique_ptr<CopyInfo> info;
	CopyFunction copy_function;
	unique_ptr<FunctionData> bind_data;

	string file_path;
	bool use_tmp_file;
	FilenamePattern filename_pattern;
	string file_extension;
	CopyOverwriteMode overwrite_mode;
	bool per_thread_output;
	optional_idx file_size_bytes;
	bool rotate;
	CopyFunctionReturnType return_type;
	bool hive_file_pattern;

	bool partition_output;
	bool write_partition_columns;
	bool write_empty_file = true;
	vector<idx_t> partition_columns;
	vector<string> names;
	vector<LogicalType> expected_types;

	//! Set of projection columns to execute prior to inserting (if any)
	vector<unique_ptr<Expression>> projection_list;
};

struct DuckLakeCopyInput {
	explicit DuckLakeCopyInput(ClientContext &context, DuckLakeTableEntry &table, const string &hive_partition = "");
	DuckLakeCopyInput(ClientContext &context, DuckLakeSchemaEntry &schema, const ColumnList &columns,
	                  const string &data_path_p);

	DuckLakeCatalog &catalog;
	optional_ptr<DuckLakePartition> partition_data;
	optional_ptr<DuckLakeFieldData> field_data;
	const ColumnList &columns;
	const string data_path;
	string encryption_key;
	SchemaIndex schema_id;
	TableIndex table_id;
	InsertVirtualColumns virtual_columns = InsertVirtualColumns::NONE;
	optional_idx get_table_index;
};

} // namespace duckdb
