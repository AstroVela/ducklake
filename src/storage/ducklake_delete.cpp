#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_deletion_vector.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/common/multi_file/multi_file_function.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/planner/operator/logical_dummy_scan.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "storage/ducklake_scan.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_copy_to_file.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "storage/ducklake_delete.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_schema_entry.hpp"
#include "common/ducklake_data_file.hpp"
#include "storage/ducklake_multi_file_list.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "storage/ducklake_multi_file_reader.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "common/ducklake_util.hpp"
#ifdef DUCKLAKE_VANE_DISTRIBUTED
#include "storage/ducklake_distributed_scan.hpp"
#include "storage/ducklake_distributed_write.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/execution/distributed/copy_finalize.hpp"
#include "duckdb/main/attached_database.hpp"
#endif

namespace duckdb {

template <typename InputType>
static DuckLakeDeleteFile WriteDeleteFileInternal(ClientContext &context, InputType &input) {
	constexpr bool with_snapshots = std::is_same<InputType, WriteDeleteFileWithSnapshotsInput>::value;

	auto delete_file_uuid = "ducklake-" + input.transaction.GenerateUUID() + "-delete.parquet";
	string delete_file_path = DuckLakeUtil::JoinPath(input.fs, input.data_path, delete_file_uuid);

	auto info = make_uniq<CopyInfo>();
	info->file_path = delete_file_path;
	info->format = "parquet";
	info->is_from = false;

	// generate the field ids to be written by the parquet writer
	// these field ids follow icebergs' ids and names for the delete files
	child_list_t<Value> values;
	values.emplace_back("file_path", Value::INTEGER(MultiFileReader::FILENAME_FIELD_ID));
	values.emplace_back("pos", Value::INTEGER(MultiFileReader::ORDINAL_FIELD_ID));
	if (with_snapshots) {
		// add the snapshot_id column to track when each deletion became valid
		values.emplace_back("_ducklake_internal_snapshot_id",
		                    Value::INTEGER(MultiFileReader::LAST_UPDATED_SEQUENCE_NUMBER_ID));
	}
	auto field_ids = Value::STRUCT(std::move(values));
	vector<Value> field_input;
	field_input.push_back(std::move(field_ids));
	info->options["field_ids"] = std::move(field_input);

	if (!input.encryption_key.empty()) {
		child_list_t<Value> enc_values;
		enc_values.emplace_back("footer_key_value", Value::BLOB_RAW(input.encryption_key));
		vector<Value> encryption_input;
		encryption_input.push_back(Value::STRUCT(std::move(enc_values)));
		info->options["encryption_config"] = std::move(encryption_input);
	}

	// get the actual copy function and bind it
	auto &copy_fun = DuckLakeFunctions::GetCopyFunction(input.context, "parquet");
	CopyFunctionBindInput bind_input(*info);

	vector<string> names_to_write {"file_path", "pos"};
	vector<LogicalType> types_to_write {LogicalType::VARCHAR, LogicalType::BIGINT};
	if (with_snapshots) {
		names_to_write.push_back("_ducklake_internal_snapshot_id");
		types_to_write.push_back(LogicalType::BIGINT);
	}

	DuckLakeUtil::EnsureDirectoryExists(input.fs, input.data_path);

	auto function_data = copy_fun.function.copy_to_bind(input.context, bind_input, names_to_write, types_to_write);
	auto copy_global_state = copy_fun.function.copy_to_initialize_global(context, *function_data, delete_file_path);

	// set up stats to get them from function
	CopyFunctionFileStatistics stats;
	copy_fun.function.copy_to_get_written_statistics(context, *function_data, *copy_global_state, stats);

	ThreadContext thread_context(context);
	ExecutionContext execution_context(context, thread_context, nullptr);
	auto copy_local_state = copy_fun.function.copy_to_initialize_local(execution_context, *function_data);

	DataChunk write_chunk;
	write_chunk.Initialize(input.context, types_to_write);
	// the first vector is constant (the file name)
	Value filename_val(input.data_file_path);
	write_chunk.data[0].Reference(filename_val);

	optional_idx begin_snapshot;
	idx_t row_count = 0;
	auto pos_data = FlatVector::GetData<int64_t>(write_chunk.data[1]);
	int64_t *snapshot_data = nullptr;
	if (with_snapshots) {
		snapshot_data = FlatVector::GetData<int64_t>(write_chunk.data[2]);
	}

	for (auto &entry : input.positions) {
		if (with_snapshots) {
			// entry is PositionWithSnapshot
			auto &pos_with_snap = reinterpret_cast<const PositionWithSnapshot &>(entry);
			if (!begin_snapshot.IsValid() || pos_with_snap.snapshot_id < begin_snapshot.GetIndex()) {
				begin_snapshot = pos_with_snap.snapshot_id;
			}
			pos_data[row_count] = NumericCast<int64_t>(pos_with_snap.position);
			snapshot_data[row_count] = NumericCast<int64_t>(pos_with_snap.snapshot_id);
		} else {
			// entry is idx_t
			pos_data[row_count] = NumericCast<int64_t>(reinterpret_cast<const idx_t &>(entry));
		}
		row_count++;
		if (row_count >= STANDARD_VECTOR_SIZE) {
			write_chunk.SetCardinality(row_count);
			copy_fun.function.copy_to_sink(execution_context, *function_data, *copy_global_state, *copy_local_state,
			                               write_chunk);
			row_count = 0;
		}
	}
	if (row_count > 0) {
		write_chunk.SetCardinality(row_count);
		copy_fun.function.copy_to_sink(execution_context, *function_data, *copy_global_state, *copy_local_state,
		                               write_chunk);
	}

	copy_fun.function.copy_to_combine(execution_context, *function_data, *copy_global_state, *copy_local_state);
	copy_fun.function.copy_to_finalize(context, *function_data, *copy_global_state);

	// add to the written files
	DuckLakeDeleteFile delete_file;
	delete_file.data_file_path = input.data_file_path;
	delete_file.file_name = delete_file_path;
	delete_file.format = DeleteFileFormat::PARQUET;
	delete_file.delete_count = stats.row_count;
	delete_file.file_size_bytes = stats.file_size_bytes;
	delete_file.footer_size = stats.footer_size_bytes.GetValue<idx_t>();
	delete_file.encryption_key = input.encryption_key;
	delete_file.source = input.source;
	if (with_snapshots) {
		delete_file.begin_snapshot = begin_snapshot;
	}
	return delete_file;
}

DuckLakeDeleteFile DuckLakeDeleteFileWriter::WriteDeleteFile(ClientContext &context, WriteDeleteFileInput &input) {
	return WriteDeleteFileInternal(context, input);
}

DuckLakeDeleteFile DuckLakeDeleteFileWriter::WriteDeleteFileWithSnapshots(ClientContext &context,
                                                                          WriteDeleteFileWithSnapshotsInput &input) {
	return WriteDeleteFileInternal(context, input);
}

DuckLakeDeleteFile DuckLakeDeleteFileWriter::WriteDeletionVectorFile(ClientContext &context,
                                                                     WriteDeleteFileInput &input) {
	auto delete_file_uuid = "ducklake-" + input.transaction.GenerateUUID() + "-delete.puffin";
	string delete_file_path = DuckLakeUtil::JoinPath(input.fs, input.data_path, delete_file_uuid);

	// Serialize positions to puffin blob
	auto blob_data = DuckLakeDeletionVectorData::ToBlob(input.positions);

	// Write blob to file
	auto &fs = FileSystem::GetFileSystem(context);
	DuckLakeUtil::EnsureDirectoryExists(fs, input.data_path);
	auto file_handle =
	    fs.OpenFile(delete_file_path, FileOpenFlags::FILE_FLAGS_WRITE | FileOpenFlags::FILE_FLAGS_FILE_CREATE);
	file_handle->Write(blob_data.data(), blob_data.size());
	file_handle->Close();

	DuckLakeDeleteFile delete_file;
	delete_file.data_file_path = input.data_file_path;
	delete_file.file_name = delete_file_path;
	delete_file.format = DeleteFileFormat::PUFFIN;
	delete_file.delete_count = input.positions.size();
	delete_file.file_size_bytes = blob_data.size();
	delete_file.footer_size = 0;
	delete_file.encryption_key = input.encryption_key;
	delete_file.source = input.source;
	return delete_file;
}

//===--------------------------------------------------------------------===//
// DuckLakeDelete
//===--------------------------------------------------------------------===//
DuckLakeDelete::DuckLakeDelete(PhysicalPlan &physical_plan, DuckLakeTableEntry &table, PhysicalOperator &child,
                               shared_ptr<DuckLakeDeleteMap> delete_map_p, vector<idx_t> row_id_indexes_p,
                               string encryption_key_p, bool allow_duplicates)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, {LogicalType::BIGINT}, 1), table(table),
      delete_map(std::move(delete_map_p)), row_id_indexes(std::move(row_id_indexes_p)),
      encryption_key(std::move(encryption_key_p)), allow_duplicates(allow_duplicates) {
	children.push_back(child);
}

#ifdef DUCKLAKE_VANE_DISTRIBUTED
void DuckLakeDelete::InitializeDistributedSource(const DuckLakeSnapshot &source_snapshot,
                                                 vector<DuckLakeFileListExtendedEntry> source_files,
                                                 bool source_prepared, bool has_source_scan,
                                                 bool source_is_statically_empty) {
	distributed_source_snapshot = source_snapshot;
	distributed_source_files = std::move(source_files);
	distributed_source_prepared = source_prepared;
	distributed_has_source_scan = has_source_scan;
	distributed_source_is_statically_empty = source_is_statically_empty;
}

void DuckLakeDelete::InitializeDistributedWritePlan(ClientContext &context) {
	distributed_write_plan.extension_name = "ducklake";
	distributed_write_plan.operator_name = "delete";
	auto &schema = table.ParentSchema().Cast<DuckLakeSchemaEntry>();
	auto &catalog = table.catalog.Cast<DuckLakeCatalog>();
	auto &transaction = DuckLakeTransaction::Get(context, catalog);
	distributed_catalog_name = catalog.GetName();
	distributed_schema_name = schema.name;
	distributed_table_name = table.name;
	distributed_schema_uuid = schema.GetSchemaUUID();
	distributed_table_uuid = table.GetTableUUID();
	distributed_data_path = table.DataPath();
	distributed_artifact_path = CreateDuckLakeDistributedArtifactPath(context, distributed_data_path);
	distributed_field_identity = GetDuckLakeDistributedFieldIdentity(table.GetFieldData());
	distributed_partition_identity = GetDuckLakeDistributedPartitionIdentity(table.GetPartitionData().get());
	distributed_sort_identity = GetDuckLakeDistributedSortIdentity(table.GetSortData().get());
	distributed_snapshot = transaction.GetSnapshot();
	distributed_schema_id = schema.GetSchemaId();
	distributed_table_id = table.GetTableId();
	if (distributed_has_source_scan &&
	    !DuckLakeDistributedSnapshotsMatch(distributed_source_snapshot, distributed_snapshot)) {
		throw TransactionException("DuckLake DELETE source snapshot does not match its target snapshot");
	}
}

void DuckLakeDelete::ValidateDistributedWriteShape() const {
	if (distributed_write_plan.extension_name != "ducklake" || distributed_write_plan.operator_name != "delete" ||
	    distributed_write_plan.worker_bind_data.empty() || distributed_artifact_path.empty() ||
	    !distributed_worker_child || !distributed_worker_plan_selected || children.size() != 1 ||
	    !distributed_schema_id.IsValid() || !distributed_table_id.IsValid() || !distributed_source_prepared) {
		throw InvalidInputException("DuckLake distributed DELETE worker plan was not initialized");
	}
	if (distributed_source_is_statically_empty) {
		if (distributed_has_source_scan || !distributed_source_files.empty()) {
			throw InvalidInputException("DuckLake distributed DELETE has invalid statically-empty source state");
		}
	} else if (!distributed_has_source_scan || distributed_source_files.empty()) {
		throw InvalidInputException("DuckLake distributed DELETE is missing its planned source scan");
	}
}

DuckLakeTableEntry &DuckLakeDelete::ResolveDistributedWriteTable(ClientContext &context) const {
	ValidateDistributedWriteShape();
	auto &catalog = Catalog::GetCatalog(context, distributed_catalog_name).Cast<DuckLakeCatalog>();
	auto &resolved_schema = catalog.GetSchema(context, distributed_schema_name).Cast<DuckLakeSchemaEntry>();
	if (resolved_schema.GetSchemaId() != distributed_schema_id ||
	    resolved_schema.GetSchemaUUID() != distributed_schema_uuid) {
		throw TransactionException("DuckLake schema %s.%s changed after the distributed DELETE was planned",
		                           distributed_catalog_name, distributed_schema_name);
	}
	auto &resolved_table = Catalog::GetEntry<TableCatalogEntry>(context, distributed_catalog_name,
	                                                            distributed_schema_name, distributed_table_name)
	                           .Cast<DuckLakeTableEntry>();
	auto &transaction = DuckLakeTransaction::Get(context, catalog);
	if (resolved_table.GetTableId() != distributed_table_id ||
	    resolved_table.GetTableUUID() != distributed_table_uuid) {
		throw TransactionException("DuckLake table %s.%s.%s was replaced after the distributed DELETE was planned",
		                           distributed_catalog_name, distributed_schema_name, distributed_table_name);
	}
	if (resolved_table.IsTransactionLocal() || transaction.HasAnyLocalChanges(resolved_table.GetTableId())) {
		throw NotImplementedException("Distributed DuckLake DELETE does not support transaction-local table state");
	}
	auto &file_system = FileSystem::GetFileSystem(context);
	auto current_data_path = distributed::CanonicalDistributedCopyBasePath(file_system, resolved_table.DataPath());
	auto planned_data_path = distributed::CanonicalDistributedCopyBasePath(file_system, distributed_data_path);
	if (current_data_path.is_err() || planned_data_path.is_err() ||
	    current_data_path.value() != planned_data_path.value()) {
		throw TransactionException("DuckLake table %s.%s.%s data path changed after the distributed DELETE was planned",
		                           distributed_catalog_name, distributed_schema_name, distributed_table_name);
	}
	if (GetDuckLakeDistributedFieldIdentity(resolved_table.GetFieldData()) != distributed_field_identity ||
	    GetDuckLakeDistributedPartitionIdentity(resolved_table.GetPartitionData().get()) !=
	        distributed_partition_identity ||
	    GetDuckLakeDistributedSortIdentity(resolved_table.GetSortData().get()) != distributed_sort_identity) {
		throw TransactionException("DuckLake table %s.%s.%s layout changed after the distributed DELETE was planned",
		                           distributed_catalog_name, distributed_schema_name, distributed_table_name);
	}
	ValidateDuckLakeDistributedSnapshotBaseline(context, distributed_catalog_name, distributed_snapshot, "DELETE");
	return resolved_table;
}

void DuckLakeDelete::ValidateDistributedSourceBaseline(ClientContext &context, const DuckLakeTableEntry &target_table,
                                                       const string &operation_name,
                                                       const string &worker_bind_data) const {
	ValidateDuckLakeDistributedRowDeltaSourceBaseline(context, target_table, distributed_source_files, worker_bind_data,
	                                                  operation_name, distributed_source_is_statically_empty);
}

optional_ptr<distributed::ExtensionWriteTaskProvider> DuckLakeDelete::GetExtensionWriteTaskProvider() {
	if (!distributed_source_prepared) {
		throw NotImplementedException("Distributed DuckLake DELETE supports committed file-backed source data only");
	}
	if (!encryption_key.empty()) {
		throw NotImplementedException("Distributed DuckLake DELETE does not support encrypted tables");
	}
	if (distributed_write_plan.worker_bind_data.empty()) {
		throw InvalidInputException("DuckLake distributed DELETE is missing its frozen worker bind");
	}
	SelectDistributedWorkerPlan();
	return this;
}

void DuckLakeDelete::SelectDistributedWorkerPlan() {
	if (distributed_worker_plan_selected) {
		return;
	}
	if (children.size() != 1 || !distributed_worker_child) {
		throw InvalidInputException("DuckLake distributed DELETE requires exactly one worker child");
	}
	children[0] = *distributed_worker_child;
	distributed_worker_plan_selected = true;
}

const distributed::DistributedExtensionWritePlan &DuckLakeDelete::WritePlan() const {
	ValidateDistributedWriteShape();
	return distributed_write_plan;
}

void DuckLakeDelete::ValidateDistributedWrite(ClientContext &context) const {
	ValidateDistributedWriteShape();
	bool expected = false;
	if (!distributed_write_claimed.compare_exchange_strong(expected, true)) {
		throw InvalidInputException("A distributed DuckLake DELETE plan can only be executed once");
	}
	auto &catalog = Catalog::GetCatalog(context, distributed_catalog_name).Cast<DuckLakeCatalog>();
	if (catalog.GetAttached().IsReadOnly()) {
		throw PermissionException("Distributed DuckLake DELETE requires a writable catalog");
	}
	if (catalog.CatalogSnapshot()) {
		throw NotImplementedException("Distributed DuckLake DELETE does not support snapshot-attached catalogs");
	}
	if (catalog.RetrialsServerSide()) {
		throw NotImplementedException("Distributed DuckLake DELETE does not support server-side commit retries");
	}
	if (!encryption_key.empty()) {
		throw NotImplementedException("Distributed DuckLake DELETE does not support encrypted tables");
	}
	ValidateDuckLakeDistributedArtifactPath(context, distributed_data_path, distributed_artifact_path);
	auto &transaction = DuckLakeTransaction::Get(context, catalog);
	if (transaction.ChangesMade()) {
		throw NotImplementedException("Distributed DuckLake DELETE requires an otherwise empty catalog transaction");
	}
	auto &target_table = ResolveDistributedWriteTable(context);
	ValidateDistributedSourceBaseline(context, target_table, "DELETE", distributed_write_plan.worker_bind_data);
}

idx_t DuckLakeDelete::FinalizeDistributedWrite(ClientContext &context,
                                               const vector<DistributedWriteTaskResult> &results) const {
	ValidateDistributedWriteShape();
	bool ownership_transferred = false;
	try {
		auto &target_table = ResolveDistributedWriteTable(context);
		ValidateDistributedSourceBaseline(context, target_table, "DELETE", distributed_write_plan.worker_bind_data);
		auto &catalog = target_table.catalog.Cast<DuckLakeCatalog>();
		auto &schema = target_table.ParentSchema().Cast<DuckLakeSchemaEntry>();
		auto use_deletion_vectors = catalog.WriteDeletionVectors(schema.GetSchemaId(), target_table.GetTableId());
		auto write_info = distributed::ResolveDistributedExtensionWriteInfo(context, distributed_write_plan);
		auto decoded = DecodeDuckLakeDistributedRowDeltaResults(
		    context, distributed_data_path, distributed_artifact_path, write_info, results,
		    DuckLakeDistributedRowDeltaKind::DELETE, use_deletion_vectors);
		CleanupDuckLakeDistributedRowDelta(context, distributed_data_path, distributed_artifact_path,
		                                   &decoded.selected_artifact_paths);
		if (decoded.affected_rows == 0) {
			if (!decoded.delete_files.empty()) {
				throw InvalidInputException("DuckLake distributed DELETE returned artifacts for zero affected rows");
			}
			CleanupDuckLakeDistributedRowDelta(context, distributed_data_path, distributed_artifact_path);
			return 0;
		}
		auto delete_files = BuildDuckLakeDistributedDeleteFiles(
		    context, distributed_source_files, distributed_write_plan.worker_bind_data, decoded.delete_files, "DELETE");
		idx_t delete_count = 0;
		for (const auto &file : decoded.delete_files) {
			if (file.new_delete_count > NumericLimits<idx_t>::Maximum() - delete_count) {
				throw InvalidInputException("DuckLake distributed DELETE row count overflow");
			}
			delete_count += file.new_delete_count;
		}
		if (delete_count != decoded.affected_rows || delete_files.empty()) {
			throw InvalidInputException("DuckLake distributed DELETE produced inconsistent delete artifacts");
		}
		auto &transaction = DuckLakeTransaction::Get(context, catalog);
		transaction.FailDistributedWriteOnSnapshotConflict();
		transaction.RegisterDistributedArtifact(target_table.GetTableId(), distributed_data_path,
		                                        distributed_artifact_path);
		ownership_transferred = true;
		transaction.AddDeletes(target_table.GetTableId(), std::move(delete_files));
		return decoded.affected_rows;
	} catch (...) {
		if (!ownership_transferred) {
			try {
				CleanupDuckLakeDistributedRowDelta(context, distributed_data_path, distributed_artifact_path);
			} catch (...) {
			}
		}
		throw;
	}
}

void DuckLakeDelete::AbortDistributedWrite(ClientContext &context, const vector<DistributedWriteTaskResult> &) const {
	CleanupDuckLakeDistributedRowDelta(context, distributed_data_path, distributed_artifact_path);
}

void DuckLakeDelete::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	if (distributed_worker_plan_selected) {
		throw InvalidInputException(
		    "A distributed DuckLake DELETE worker plan cannot execute as a native coordinator operator");
	}
	PhysicalOperator::BuildPipelines(current, meta_pipeline);
}
#endif

//===--------------------------------------------------------------------===//
// States
//===--------------------------------------------------------------------===//
struct WrittenColumnInfo {
	WrittenColumnInfo() = default;
	WrittenColumnInfo(LogicalType type_p, int32_t field_id) : type(std::move(type_p)), field_id(field_id) {
	}

	LogicalType type;
	int32_t field_id;
};

class DuckLakeDeleteLocalState : public LocalSinkState {
public:
	optional_idx current_file_index;
	vector<idx_t> file_row_numbers;
	unordered_map<idx_t, string> filenames;
};

class DuckLakeDeleteGlobalState : public GlobalSinkState {
public:
	explicit DuckLakeDeleteGlobalState() {
		written_columns["file_path"] =
		    WrittenColumnInfo(LogicalType::VARCHAR, MultiFileReader::DELETE_FILE_PATH_FIELD_ID);
		written_columns["pos"] = WrittenColumnInfo(LogicalType::BIGINT, MultiFileReader::DELETE_POS_FIELD_ID);
	}

	mutex lock;
	unordered_map<string, DuckLakeDeleteFile> written_files;
	unordered_map<string, WrittenColumnInfo> written_columns;
	idx_t total_deleted_count = 0;
	unordered_map<uint64_t, unique_ptr<ColumnDataCollection>> deleted_rows;
	unordered_map<idx_t, string> filenames;

	void Flush(ClientContext &context, DuckLakeDeleteLocalState &local_state) {
		auto &local_entry = local_state.file_row_numbers;
		if (local_entry.empty()) {
			return;
		}
		lock_guard<mutex> guard(lock);
		auto deleted_row_idx = local_state.current_file_index.GetIndex();
		auto global_entry = deleted_rows.find(deleted_row_idx);
		DataChunk file_row_id_chunk;
		vector<LogicalType> row_id_types {LogicalType::UBIGINT};
		file_row_id_chunk.Initialize(context, row_id_types);

		optional_ptr<ColumnDataCollection> deleted_row_collection;
		if (global_entry == deleted_rows.end()) {
			auto collection = make_uniq<ColumnDataCollection>(context, row_id_types);
			deleted_row_collection = collection.get();
			deleted_rows[deleted_row_idx] = std::move(collection);
		} else {
			deleted_row_collection = global_entry->second.get();
		}
		ColumnDataAppendState append_state;
		deleted_row_collection->InitializeAppend(append_state);
		auto data = FlatVector::GetData<uint64_t>(file_row_id_chunk.data[0]);
		idx_t chunk_size = 0;
		for (idx_t r = 0; r < local_entry.size(); ++r) {
			data[chunk_size++] = local_entry[r];
			if (chunk_size == STANDARD_VECTOR_SIZE) {
				file_row_id_chunk.SetCardinality(chunk_size);
				deleted_row_collection->Append(append_state, file_row_id_chunk);
				chunk_size = 0;
			}
		}
		if (chunk_size > 0) {
			file_row_id_chunk.SetCardinality(chunk_size);
			deleted_row_collection->Append(append_state, file_row_id_chunk);
			chunk_size = 0;
		}
		total_deleted_count += local_entry.size();
		local_entry.clear();
	}

	void FinalFlush(ClientContext &context, DuckLakeDeleteLocalState &local_state) {
		Flush(context, local_state);
		// flush the file names to the global state
		lock_guard<mutex> guard(lock);
		for (auto &entry : local_state.filenames) {
			filenames.emplace(entry.first, entry.second);
		}
	}
};

unique_ptr<GlobalSinkState> DuckLakeDelete::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<DuckLakeDeleteGlobalState>();
}

unique_ptr<LocalSinkState> DuckLakeDelete::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<DuckLakeDeleteLocalState>();
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
SinkResultType DuckLakeDelete::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &global_state = input.global_state.Cast<DuckLakeDeleteGlobalState>();
	auto &local_state = input.local_state.Cast<DuckLakeDeleteLocalState>();

	auto &file_name_vector = chunk.data[row_id_indexes[0]];
	auto &file_index_vector = chunk.data[row_id_indexes[1]];
	auto &file_row_number = chunk.data[row_id_indexes[2]];

	UnifiedVectorFormat row_data;
	file_row_number.ToUnifiedFormat(chunk.size(), row_data);
	auto file_row_data = UnifiedVectorFormat::GetData<int64_t>(row_data);

	UnifiedVectorFormat file_name_vdata;
	file_name_vector.ToUnifiedFormat(chunk.size(), file_name_vdata);

	UnifiedVectorFormat file_index_vdata;
	file_index_vector.ToUnifiedFormat(chunk.size(), file_index_vdata);

	auto file_index_data = UnifiedVectorFormat::GetData<uint64_t>(file_index_vdata);
	for (idx_t i = 0; i < chunk.size(); i++) {
		auto file_idx = file_index_vdata.sel->get_index(i);
		auto row_idx = row_data.sel->get_index(i);
		if (!file_index_vdata.validity.RowIsValid(file_idx)) {
			throw InternalException("File index cannot be NULL!");
		}
		auto file_index = file_index_data[file_idx];
		if (!local_state.current_file_index.IsValid() || file_index != local_state.current_file_index.GetIndex()) {
			// file has changed - flush
			global_state.Flush(context.client, local_state);
			local_state.current_file_index = file_index;
			// insert the file name for the file if it has not yet been inserted
			auto entry = local_state.filenames.find(file_index);
			if (entry == local_state.filenames.end()) {
				auto file_name_idx = file_name_vdata.sel->get_index(i);
				auto file_name_data = UnifiedVectorFormat::GetData<string_t>(file_name_vdata);
				if (!file_name_vdata.validity.RowIsValid(file_name_idx)) {
					throw InternalException("Filename cannot be NULL!");
				}
				local_state.filenames.emplace(file_index, file_name_data[file_name_idx].GetString());
			}
		}
		auto row_number = file_row_data[row_idx];
		local_state.file_row_numbers.push_back(row_number);
	}
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Combine
//===--------------------------------------------------------------------===//
SinkCombineResultType DuckLakeDelete::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &global_state = input.global_state.Cast<DuckLakeDeleteGlobalState>();
	auto &local_state = input.local_state.Cast<DuckLakeDeleteLocalState>();
	global_state.FinalFlush(context.client, local_state);
	return SinkCombineResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
bool DuckLakeDelete::TryDropFullyDeletedFile(DuckLakeTransaction &transaction, const DuckLakeDeleteFile &delete_file,
                                             const DuckLakeFileListExtendedEntry &data_file_info,
                                             idx_t delete_count) const {
	if (delete_count != data_file_info.row_count) {
		return false;
	}
	// ALL rows in this file are deleted - drop the file
	if (delete_file.data_file_id.IsValid()) {
		transaction.DropFile(table.GetTableId(), delete_file.data_file_id, data_file_info.file.path);
	} else {
		transaction.DropTransactionLocalFile(table.GetTableId(), data_file_info.file.path);
	}
	return true;
}

void DuckLakeDelete::FlushMergedDeletionVector(DuckLakeTransaction &transaction, ClientContext &context,
                                               DuckLakeDeleteGlobalState &global_state, const string &filename,
                                               const DuckLakeFileListExtendedEntry &data_file_info,
                                               DuckLakeDeleteData &existing_delete_data,
                                               const set<idx_t> &sorted_deletes,
                                               DuckLakeDeleteFile &delete_file) const {
	// Deletion vectors don't support per-position snapshot tracking.
	// Merge all positions (existing + new) into a flat set, like the old pre-snapshot code.
	set<idx_t> all_deletes = sorted_deletes;
	auto &existing_deletes = existing_delete_data.deleted_rows;
	all_deletes.insert(existing_deletes.begin(), existing_deletes.end());

	// clear the deletes from the map
	delete_map->ClearDeletes(filename);

	// set the delete file as overwriting existing deletes
	delete_file.overwrites_existing_delete = true;

	if (TryDropFullyDeletedFile(transaction, delete_file, data_file_info, all_deletes.size())) {
		return;
	}

	auto &fs = FileSystem::GetFileSystem(context);
	WriteDeleteFileInput input {context,        transaction, fs,          table.DataPath(),
	                            encryption_key, filename,    all_deletes, DeleteFileSource::REGULAR};
	auto written_file = DuckLakeDeleteFileWriter::WriteDeletionVectorFile(context, input);

	written_file.data_file_id = delete_file.data_file_id;
	written_file.overwrites_existing_delete = delete_file.overwrites_existing_delete;
	// track the old delete file for deletion from metadata
	written_file.overwritten_delete_file.delete_file_id = data_file_info.delete_file_id;
	written_file.overwritten_delete_file.path = data_file_info.delete_file.path;

	global_state.written_files.emplace(filename, std::move(written_file));
}

void DuckLakeDelete::FlushDeleteWithSnapshots(DuckLakeTransaction &transaction, ClientContext &context,
                                              DuckLakeDeleteGlobalState &global_state, const string &filename,
                                              const DuckLakeFileListExtendedEntry &data_file_info,
                                              DuckLakeDeleteData &existing_delete_data,
                                              const set<idx_t> &sorted_deletes, DuckLakeDeleteFile &delete_file) const {
	auto &catalog = table.catalog.Cast<DuckLakeCatalog>();
	auto &schema = table.ParentSchema().Cast<DuckLakeSchemaEntry>();
	if (catalog.WriteDeletionVectors(schema.GetSchemaId(), table.GetTableId())) {
		FlushMergedDeletionVector(transaction, context, global_state, filename, data_file_info, existing_delete_data,
		                          sorted_deletes, delete_file);
		return;
	}

	auto existing_snapshot = data_file_info.delete_file_begin_snapshot;

	// the commit snapshot for new deletes is current_snapshot + 1
	const auto current_snapshot = transaction.GetSnapshot();
	const idx_t new_delete_snapshot = current_snapshot.snapshot_id + 1;

	set<PositionWithSnapshot> sorted_deletes_with_snapshots;
	// add existing deletes with their snapshot IDs
	MergeDeletesWithSnapshots(existing_delete_data, existing_snapshot.GetIndex(), sorted_deletes_with_snapshots);

	// add new deletes with the commit snapshot
	for (auto &pos : sorted_deletes) {
		PositionWithSnapshot pos_with_snap;
		pos_with_snap.position = static_cast<int64_t>(pos);
		pos_with_snap.snapshot_id = static_cast<int64_t>(new_delete_snapshot);
		sorted_deletes_with_snapshots.insert(pos_with_snap);
	}

	// clear the deletes from the map
	delete_map->ClearDeletes(filename);

	// set the delete file as overwriting existing deletes
	delete_file.overwrites_existing_delete = true;

	if (TryDropFullyDeletedFile(transaction, delete_file, data_file_info, sorted_deletes_with_snapshots.size())) {
		return;
	}

	auto &fs = FileSystem::GetFileSystem(context);
	WriteDeleteFileWithSnapshotsInput input {context,
	                                         transaction,
	                                         fs,
	                                         table.DataPath(),
	                                         encryption_key,
	                                         filename,
	                                         sorted_deletes_with_snapshots,
	                                         DeleteFileSource::REGULAR};
	auto written_file = DuckLakeDeleteFileWriter::WriteDeleteFileWithSnapshots(context, input);

	written_file.data_file_id = delete_file.data_file_id;
	written_file.overwrites_existing_delete = delete_file.overwrites_existing_delete;
	// track the old delete file for deletion from metadata
	written_file.overwritten_delete_file.delete_file_id = data_file_info.delete_file_id;
	written_file.overwritten_delete_file.path = data_file_info.delete_file.path;

	idx_t max_snapshot = 0;
	for (auto &entry : sorted_deletes_with_snapshots) {
		max_snapshot = MaxValue(max_snapshot, static_cast<idx_t>(entry.snapshot_id));
	}
	written_file.max_snapshot = max_snapshot;

	global_state.written_files.emplace(filename, std::move(written_file));
}

void DuckLakeDelete::FlushDelete(DuckLakeTransaction &transaction, ClientContext &context,
                                 DuckLakeDeleteGlobalState &global_state, const string &filename,
                                 ColumnDataCollection &deleted_rows) const {
	// find the matching data file for the deletion
	auto data_file_info = delete_map->GetExtendedFileInfo(filename);

	// sort and duplicate eliminate the deletes
	set<idx_t> sorted_deletes;
	for (auto &chunk : deleted_rows.Chunks()) {
		auto row_data = FlatVector::GetData<uint64_t>(chunk.data[0]);
		for (idx_t r = 0; r < chunk.size(); r++) {
			sorted_deletes.insert(row_data[r]);
		}
	}
	if (sorted_deletes.size() != deleted_rows.Count() && !allow_duplicates) {
		throw NotImplementedException("The same row was updated multiple times - this is not (yet) supported in "
		                              "DuckLake. Eliminate duplicate matches prior to running the UPDATE");
	}

	if (data_file_info.data_type == DuckLakeDataType::TRANSACTION_LOCAL_INLINED_DATA) {
		// deletes from transaction-local inlined data are directly deleted from the inlined data
		transaction.DeleteFromLocalInlinedData(table.GetTableId(), std::move(sorted_deletes));
		return;
	}
	if (data_file_info.data_type == DuckLakeDataType::INLINED_DATA) {
		// deletes from inlined data are not written to a file but pushed directly into the metadata manager
		transaction.AddNewInlinedDeletes(table.GetTableId(), data_file_info.file.path, std::move(sorted_deletes));
		return;
	}
	DuckLakeDeleteFile delete_file;
	delete_file.data_file_path = filename;
	delete_file.data_file_id = data_file_info.file_id;
	// check if the file already has deletes
	auto existing_delete_data = delete_map->GetDeleteData(filename);

	// check if we should use inlined file deletions instead of creating a delete file
	if (data_file_info.file_id.IsValid()) {
		auto &catalog = table.catalog.Cast<DuckLakeCatalog>();
		auto &schema = table.ParentSchema().Cast<DuckLakeSchemaEntry>();
		auto threshold = catalog.DataInliningRowLimit(context, schema.GetSchemaId(), table.GetTableId());
		if (threshold > 0 && sorted_deletes.size() <= threshold) {
			// use inlined file deletions
			transaction.AddNewInlinedFileDeletes(table.GetTableId(), data_file_info.file_id.index,
			                                     std::move(sorted_deletes));
			return;
		}
	}

	if (existing_delete_data) {
		// deletes already exist for this file
		auto &existing_deletes = existing_delete_data->deleted_rows;

		// we check if we need to write the snapshot information into our deletion file
		// that basically happens if the file already has embedded snapshots
		// or if it's a delete file from a different transaction (committed delete file)
		bool write_with_snapshots =
		    existing_delete_data->HasEmbeddedSnapshots() || data_file_info.delete_file_id.IsValid();

		if (write_with_snapshots) {
			FlushDeleteWithSnapshots(transaction, context, global_state, filename, data_file_info,
			                         *existing_delete_data, sorted_deletes, delete_file);
			return;
		}

		// transaction-local deletes without metadata
		sorted_deletes.insert(existing_deletes.begin(), existing_deletes.end());

		// clear the deletes
		delete_map->ClearDeletes(filename);

		// set the delete file as overwriting existing deletes
		delete_file.overwrites_existing_delete = true;
	}
	if (TryDropFullyDeletedFile(transaction, delete_file, data_file_info, sorted_deletes.size())) {
		return;
	}

	auto &fs = FileSystem::GetFileSystem(context);
	auto &catalog = table.catalog.Cast<DuckLakeCatalog>();
	auto &schema = table.ParentSchema().Cast<DuckLakeSchemaEntry>();
	bool use_deletion_vectors = catalog.WriteDeletionVectors(schema.GetSchemaId(), table.GetTableId());

	WriteDeleteFileInput input {context,
	                            transaction,
	                            fs,
	                            table.DataPath(),
	                            encryption_key,
	                            filename,
	                            sorted_deletes,
	                            DeleteFileSource::REGULAR};
	auto written_file = use_deletion_vectors ? DuckLakeDeleteFileWriter::WriteDeletionVectorFile(context, input)
	                                         : DuckLakeDeleteFileWriter::WriteDeleteFile(context, input);

	written_file.data_file_id = delete_file.data_file_id;
	written_file.overwrites_existing_delete = delete_file.overwrites_existing_delete;

	global_state.written_files.emplace(filename, std::move(written_file));
}

SinkFinalizeType DuckLakeDelete::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                          OperatorSinkFinalizeInput &input) const {
	auto &global_state = input.global_state.Cast<DuckLakeDeleteGlobalState>();
	if (global_state.deleted_rows.empty()) {
		return SinkFinalizeType::READY;
	}

	auto &transaction = DuckLakeTransaction::Get(context, table.catalog);
	// write out the delete rows
	for (auto &entry : global_state.deleted_rows) {
		auto filename_entry = global_state.filenames.find(entry.first);
		if (filename_entry == global_state.filenames.end()) {
			throw InternalException("Filename not found for file index");
		}
		FlushDelete(transaction, context, global_state, filename_entry->second, *entry.second);
	}
	vector<DuckLakeDeleteFile> delete_files;
	for (auto &entry : global_state.written_files) {
		auto &data_file_path = entry.first;
		auto delete_file = std::move(entry.second);
		if (delete_file.data_file_id.IsValid()) {
			// deleting from a committed file - add to delete files directly
			delete_files.push_back(std::move(delete_file));
		} else {
			// deleting from a transaction local file - find the file we are deleting from
			delete_file.overwrites_existing_delete = false;
			transaction.TransactionLocalDelete(table.GetTableId(), data_file_path, std::move(delete_file));
		}
	}
	transaction.AddDeletes(table.GetTableId(), std::move(delete_files));
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// GetData
//===--------------------------------------------------------------------===//
SourceResultType DuckLakeDelete::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                 OperatorSourceInput &input) const {
	auto &global_state = sink_state->Cast<DuckLakeDeleteGlobalState>();
	auto value = Value::BIGINT(NumericCast<int64_t>(global_state.total_deleted_count));
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, value);
	return SourceResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Helpers
//===--------------------------------------------------------------------===//
string DuckLakeDelete::GetName() const {
	return "DUCKLAKE_DELETE";
}

InsertionOrderPreservingMap<string> DuckLakeDelete::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Table Name"] = table.name;
	return result;
}

optional_ptr<PhysicalTableScan> FindDeleteSource(PhysicalOperator &plan) {
	if (plan.type == PhysicalOperatorType::TABLE_SCAN) {
		// does this emit the virtual columns?
		auto &scan = plan.Cast<PhysicalTableScan>();
		bool found = false;
		for (auto &col : scan.column_ids) {
			if (col.GetPrimaryIndex() == MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER) {
				found = true;
				break;
			}
		}
		if (!found) {
			return nullptr;
		}
		return scan;
	}
	for (auto &children : plan.children) {
		auto result = FindDeleteSource(children.get());
		if (result) {
			return result;
		}
	}
	return nullptr;
}

#ifdef DUCKLAKE_VANE_DISTRIBUTED
PhysicalOperator &DuckLakeDelete::PlanDeleteInternal(ClientContext &context, PhysicalPlanGenerator &planner,
                                                     DuckLakeTableEntry &table, PhysicalOperator &child_plan,
                                                     vector<idx_t> row_id_indexes, string encryption_key,
                                                     bool allow_duplicates, bool initialize_distributed_write) {
	auto delete_source = FindDeleteSource(child_plan);
	auto delete_map = make_shared_ptr<DuckLakeDeleteMap>();
	DuckLakeSnapshot source_snapshot;
	vector<DuckLakeFileListExtendedEntry> files;
	bool has_source_scan = delete_source != nullptr;
	bool source_prepared = false;
	if (delete_source) {
		auto &bind_data = delete_source->bind_data->Cast<MultiFileBindData>();
		if (!TryGetDuckLakeDistributedMutationSource(bind_data, table, source_snapshot, files)) {
			auto reader = dynamic_cast<DuckLakeMultiFileReader *>(bind_data.multi_file_reader.get());
			auto file_list = dynamic_cast<DuckLakeMultiFileList *>(bind_data.file_list.get());
			if (!reader || !file_list) {
				throw InternalException("DuckLake DELETE source scan has an unexpected physical implementation");
			}
			source_snapshot = reader->read_info.snapshot;
			auto native_files = file_list->GetFilesExtended();
			for (const auto &file_entry : native_files) {
				delete_map->AddExtendedFileInfo(file_entry);
			}
			reader->delete_map = delete_map;

			auto &catalog = table.catalog.Cast<DuckLakeCatalog>();
			auto &transaction = DuckLakeTransaction::Get(context, catalog);
			auto &scan_files = file_list->GetFiles();
			source_prepared = !table.IsTransactionLocal() && !transaction.HasAnyLocalChanges(table.GetTableId()) &&
			                  table.GetInlinedDataTables().empty() && !file_list->HasTransactionLocalData() &&
			                  scan_files.size() == native_files.size();
			unordered_set<string> scan_paths;
			for (const auto &scan_file : scan_files) {
				if (scan_file.data_type != DuckLakeDataType::DATA_FILE || !scan_file.file_id.IsValid() ||
				    !scan_file.inlined_file_deletions.empty() || scan_file.max_row_count.IsValid() ||
				    scan_file.snapshot_filter_min.IsValid() || scan_file.snapshot_filter_max.IsValid() ||
				    !scan_paths.insert(scan_file.file.path).second) {
					source_prepared = false;
				}
			}
			for (const auto &file : native_files) {
				if (file.data_type != DuckLakeDataType::DATA_FILE || !file.file_id.IsValid() ||
				    file.file.path.empty() || file.file.file_size_bytes == 0 || file.row_count == 0 ||
				    !file.file.encryption_key.empty() || !file.delete_file.encryption_key.empty() ||
				    file.delete_file.path.empty() != !file.delete_file_id.IsValid() ||
				    scan_paths.find(file.file.path) == scan_paths.end()) {
					source_prepared = false;
				}
			}
			if (source_prepared) {
				files = std::move(native_files);
			}
		} else {
			source_prepared = true;
		}
	}
	auto source_is_statically_empty = (!delete_source && child_plan.type == PhysicalOperatorType::EMPTY_RESULT) ||
	                                  (delete_source && source_prepared && files.empty());
	if (source_is_statically_empty) {
		has_source_scan = false;
		source_prepared = true;
	}
	auto &result = planner
	                   .Make<DuckLakeDelete>(table, child_plan, std::move(delete_map), std::move(row_id_indexes),
	                                         std::move(encryption_key), allow_duplicates)
	                   .Cast<DuckLakeDelete>();
	result.InitializeDistributedSource(source_snapshot, std::move(files), source_prepared, has_source_scan,
	                                   source_is_statically_empty);
	if (initialize_distributed_write) {
		result.InitializeDistributedWritePlan(context);
		if (source_prepared && result.encryption_key.empty()) {
			result.distributed_write_plan.worker_bind_data = BuildDuckLakeDistributedDeleteBind(
			    context, table, result.distributed_source_files, result.row_id_indexes,
			    result.distributed_artifact_path, source_is_statically_empty);
		}
		if (source_is_statically_empty) {
			result.distributed_worker_child = &child_plan;
		} else if (has_source_scan) {
			result.distributed_worker_child =
			    &PlanDuckLakeDistributedRowDeltaRepartition(planner, child_plan, result.row_id_indexes[0]);
		}
	}
	return result;
}
#endif

PhysicalOperator &DuckLakeDelete::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner,
                                             DuckLakeTableEntry &table, PhysicalOperator &child_plan,
                                             vector<idx_t> row_id_indexes, string encryption_key,
                                             bool allow_duplicates) {
#ifdef DUCKLAKE_VANE_DISTRIBUTED
	return PlanDeleteInternal(context, planner, table, child_plan, std::move(row_id_indexes), std::move(encryption_key),
	                          allow_duplicates, true);
#else
	auto delete_source = FindDeleteSource(child_plan);
	auto delete_map = make_shared_ptr<DuckLakeDeleteMap>();
	if (delete_source) {
		auto &bind_data = delete_source->bind_data->Cast<MultiFileBindData>();
		auto &reader = bind_data.multi_file_reader->Cast<DuckLakeMultiFileReader>();
		auto &file_list = bind_data.file_list->Cast<DuckLakeMultiFileList>();
		auto files = file_list.GetFilesExtended();
		for (auto &file_entry : files) {
			delete_map->AddExtendedFileInfo(std::move(file_entry));
		}
		reader.delete_map = delete_map;
	}
	return planner.Make<DuckLakeDelete>(table, child_plan, std::move(delete_map), std::move(row_id_indexes),
	                                    std::move(encryption_key), allow_duplicates);
#endif
}

PhysicalOperator &DuckLakeCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
                                              PhysicalOperator &child_plan) {
	if (op.return_chunk) {
		throw BinderException("RETURNING clause not yet supported for deletion of a DuckLake table");
	}
	auto encryption_key = GenerateEncryptionKey(context);
	vector<idx_t> row_id_indexes;
	for (idx_t i = 0; i < 3; i++) {
		auto &bound_ref = op.expressions[i + 1]->Cast<BoundReferenceExpression>();
		row_id_indexes.push_back(bound_ref.index);
	}
	return DuckLakeDelete::PlanDelete(context, planner, op.table.Cast<DuckLakeTableEntry>(), child_plan,
	                                  std::move(row_id_indexes), std::move(encryption_key));
}

} // namespace duckdb
