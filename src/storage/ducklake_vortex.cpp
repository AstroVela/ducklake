#include "storage/ducklake_vortex.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_insert.hpp"
#include "storage/ducklake_multi_file_reader.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_schema_entry.hpp"

#ifdef DUCKLAKE_ENABLE_VORTEX
#include "functions/ducklake_table_functions.hpp"
#include "common/ducklake_util.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/multi_file/base_file_reader.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#endif

namespace duckdb {

DataFileFormat DuckLakeVortex::GetFormat(const DuckLakeCatalog &catalog, SchemaIndex schema, TableIndex table) {
	string format;
	if (!catalog.TryGetConfigOption("experimental_data_file_format", format, schema, table)) {
		return DataFileFormat::PARQUET;
	}
	return DataFileFormatFromString(format);
}

DataFileFormat DuckLakeVortex::GetFormat(DuckLakeTableEntry &table) {
	return GetFormat(table.ParentCatalog().Cast<DuckLakeCatalog>(),
	                 table.schema.Cast<DuckLakeSchemaEntry>().GetSchemaId(), table.GetTableId());
}

void DuckLakeVortex::ValidateEnabled() {
#if !defined(DUCKLAKE_ENABLE_VORTEX) || defined(DUCKLAKE_VANE_DISTRIBUTED)
	throw NotImplementedException("Experimental Vortex files require a native DUCKLAKE_ENABLE_VORTEX build");
#endif
}

bool DuckLakeVortex::IsVortex(const OpenFileInfo &file) {
	if (!file.extended_info) {
		return false;
	}
	auto entry = file.extended_info->options.find("ducklake_data_file_format");
	return entry != file.extended_info->options.end() && entry->second.GetValue<string>() == "vortex";
}

#ifdef DUCKLAKE_ENABLE_VORTEX
namespace {

string PhysicalName(idx_t field_id) {
	return "__ducklake_vortex_v1_field_" + to_string(field_id);
}

void ValidateType(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::VARCHAR:
		return;
	case LogicalTypeId::LIST:
		if (ListType::GetChildType(type) == LogicalType::FLOAT) {
			return;
		}
		break;
	default:
		break;
	}
	throw NotImplementedException("Experimental DuckLake Vortex files do not support type %s", type);
}

string LocalWritePath(ClientContext &context, const string &path) {
	auto &fs = FileSystem::GetFileSystem(context);
	if (fs.IsRemoteFile(path) || path.find("://") != string::npos) {
		throw NotImplementedException("Experimental DuckLake Vortex writes require local paths");
	}
	auto result = fs.ExpandPath(path);
	if (!fs.IsPathAbsolute(result)) {
		result = fs.JoinPath(fs.GetWorkingDirectory(), result);
	}
	for (auto character : result) {
		auto byte = static_cast<uint8_t>(character);
		if (byte <= 0x20 || byte >= 0x7f || string("*?[\"#%<>`{}\\").find(character) != string::npos) {
			throw NotImplementedException("Experimental DuckLake Vortex write paths require unescaped local paths");
		}
	}
	return result;
}

class VortexReader : public BaseFileReader {
public:
	VortexReader(ClientContext &context, const OpenFileInfo &file, DuckLakeTableEntry &table) : BaseFileReader(file) {
		auto &fs = FileSystem::GetFileSystem(context);
		if (fs.IsRemoteFile(file.path) || file.path.find("://") != string::npos) {
			throw NotImplementedException("Experimental DuckLake Vortex reads require local files");
		}
		auto &catalog = Catalog::GetSystemCatalog(context);
		auto &entry = catalog.GetEntry<TableFunctionCatalogEntry>(context, DEFAULT_SCHEMA, "_read_vortex_file");
		scan = entry.functions.GetFunctionByArguments(context, {LogicalType::VARCHAR});
		vector<Value> inputs {Value(file.path)};
		named_parameter_map_t parameters;
		vector<LogicalType> input_types;
		vector<string> input_names;
		TableFunctionRef ref;
		TableFunctionBindInput input(inputs, parameters, input_types, input_names, scan.function_info.get(), nullptr,
		                             scan, ref);
		vector<LogicalType> types;
		vector<string> names;
		bind_data = scan.bind(context, input, types, names);
		auto schema = DuckLakeMultiFileReader::ColumnsFromFieldData(table.GetFieldData());
		unordered_map<string, MultiFileColumnDefinition *> fields;
		for (auto &column : schema) {
			ValidateType(column.type);
			fields.emplace(PhysicalName(column.identifier.GetValue<int32_t>()), &column);
		}
		for (idx_t i = 0; i < names.size(); i++) {
			auto field = fields.find(names[i]);
			if (field != fields.end() && types[i] == field->second->type) {
				columns.push_back(*field->second);
				columns.back().name = names[i];
				fields.erase(field);
			} else if ((names[i] == "_ducklake_internal_row_id" || names[i] == "_ducklake_internal_snapshot_id") &&
			           types[i] == LogicalType::BIGINT) {
				MultiFileColumnDefinition column(names[i], types[i]);
				column.identifier = Value::INTEGER(names[i] == "_ducklake_internal_row_id"
				                                       ? MultiFileReader::ROW_ID_FIELD_ID
				                                       : MultiFileReader::LAST_UPDATED_SEQUENCE_NUMBER_ID);
				columns.push_back(std::move(column));
			} else {
				throw InvalidInputException("Vortex file '%s' does not match the fixed DuckLake schema", file.path);
			}
		}
		if (!fields.empty()) {
			throw InvalidInputException("Vortex file '%s' is missing DuckLake fields", file.path);
		}
		physical_columns = columns.size();
	}

	void AddVirtualColumn(column_t id) override {
		if (id != MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER) {
			throw NotImplementedException("Unsupported Vortex virtual column");
		}
		columns.back().identifier = Value::INTEGER(MultiFileReader::ORDINAL_FIELD_ID);
	}

	bool TryInitializeScan(ClientContext &, GlobalTableFunctionState &, LocalTableFunctionState &) override {
		if (claimed) {
			return false;
		}
		claimed = true;
		return true;
	}

	void PrepareScan(ClientContext &context, GlobalTableFunctionState &, LocalTableFunctionState &) override {
		vector<column_t> ids;
		vector<LogicalType> types;
		unordered_map<column_t, idx_t> projections;
		auto project = [&](column_t id, const LogicalType &type) {
			auto entry = projections.emplace(id, ids.size());
			if (entry.second) {
				ids.push_back(id);
				types.push_back(type);
			}
			return entry.first->second;
		};
		for (idx_t i = 0; i < column_ids.size(); i++) {
			auto id = column_ids[MultiFileLocalIndex(i)];
			output_mapping.push_back(
			    project(id < physical_columns ? id : MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER,
			            id < physical_columns ? columns[id].type : LogicalType::UBIGINT));
			auto expression = expression_map.find(id);
			if (expression != expression_map.end()) {
				executors.emplace(i, make_uniq<ExpressionExecutor>(context, *expression->second));
			}
		}
		row_position_idx = project(MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER, LogicalType::UBIGINT);
		raw.Initialize(context, types);
		TableFunctionInitInput input(bind_data.get(), std::move(ids), {}, nullptr);
		global = scan.init_global(context, input);
		ThreadContext thread(context);
		ExecutionContext execution(context, thread, nullptr);
		local = scan.init_local(execution, input, global.get());
	}

	AsyncResult Scan(ClientContext &context, GlobalTableFunctionState &, LocalTableFunctionState &,
	                 DataChunk &chunk) override {
		raw.Reset();
		TableFunctionInput input(bind_data.get(), local.get(), global.get());
		scan.function(context, input, raw);
		if (raw.size()) {
			auto &positions = raw.data[row_position_idx];
			positions.Flatten(raw.size());
			auto offsets = FlatVector::GetData<uint64_t>(positions);
			for (idx_t i = 0; i < raw.size(); i++) {
				if (offsets[i] != row_offset + i) {
					throw InvalidInputException("Vortex scan did not preserve physical row order");
				}
			}
			for (idx_t i = 0; i < column_ids.size(); i++) {
				auto &value = raw.data[output_mapping[i]];
				auto executor = executors.find(i);
				if (executor != executors.end()) {
					DataChunk expression_input;
					expression_input.Initialize(context, {columns[column_ids[MultiFileLocalIndex(i)]].type});
					VectorOperations::Cast(context, value, expression_input.data[0], raw.size());
					expression_input.SetCardinality(raw.size());
					executor->second->ExecuteExpression(expression_input, chunk.data[i]);
				} else if (value.GetType() != chunk.data[i].GetType()) {
					VectorOperations::Cast(context, value, chunk.data[i], raw.size());
				} else {
					chunk.data[i].Reference(value);
				}
			}
			chunk.SetCardinality(raw.size());
			DuckLakeMultiFileReader::ApplyRowFilters(context, *this, row_offset, chunk);
			row_offset += raw.size();
		}
		if (input.async_result.GetResultType() != AsyncResultType::INVALID) {
			return std::move(input.async_result);
		}
		return raw.size() ? SourceResultType::HAVE_MORE_OUTPUT : SourceResultType::FINISHED;
	}

	string GetReaderType() const override {
		return "DuckLake Vortex";
	}

private:
	TableFunction scan;
	unique_ptr<FunctionData> bind_data;
	unique_ptr<GlobalTableFunctionState> global;
	unique_ptr<LocalTableFunctionState> local;
	DataChunk raw;
	vector<idx_t> output_mapping;
	idx_t row_position_idx = 0;
	unordered_map<idx_t, unique_ptr<ExpressionExecutor>> executors;
	idx_t physical_columns = 0;
	idx_t row_offset = 0;
	bool claimed = false;
};

struct VortexCopyBind : public FunctionData {
	VortexCopyBind(CopyFunction function, unique_ptr<FunctionData> inner, vector<string> names,
	               vector<LogicalType> types)
	    : function(std::move(function)), inner(std::move(inner)), names(std::move(names)), types(std::move(types)) {
	}
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<VortexCopyBind>(function, inner->Copy(), names, types);
	}
	bool Equals(const FunctionData &other) const override {
		auto &bind = other.Cast<VortexCopyBind>();
		return names == bind.names && types == bind.types && inner->Equals(*bind.inner);
	}
	CopyFunction function;
	unique_ptr<FunctionData> inner;
	vector<string> names;
	vector<LogicalType> types;
};

struct VortexCopyGlobal : public GlobalFunctionData {
	unique_ptr<GlobalFunctionData> inner;
	string path;
	mutex lock;
	idx_t rows = 0;
	vector<idx_t> null_counts;
	map<string, pair<int64_t, int64_t>> bounds;
	optional_ptr<CopyFunctionFileStatistics> statistics;
};

} // namespace
#endif

shared_ptr<BaseFileReader> DuckLakeVortex::CreateReader(ClientContext &context, const OpenFileInfo &file,
                                                        DuckLakeTableEntry &table) {
	ValidateEnabled();
#ifdef DUCKLAKE_ENABLE_VORTEX
	return make_shared_ptr<VortexReader>(context, file, table);
#else
	throw InternalException("Vortex support is disabled");
#endif
}

void DuckLakeVortex::ValidateTable(DuckLakeTableEntry &table) {
	ValidateEnabled();
#ifdef DUCKLAKE_ENABLE_VORTEX
	auto &catalog = table.ParentCatalog().Cast<DuckLakeCatalog>();
	if (table.GetPartitionData() || catalog.IsEncrypted()) {
		throw NotImplementedException("Experimental Vortex writes require unpartitioned, unencrypted tables");
	}
	if (!catalog.MetadataType().empty() && catalog.MetadataType() != "duckdb") {
		throw NotImplementedException("Experimental Vortex writes require a native DuckDB metadata catalog");
	}
	for (auto &column : table.GetColumns().Physical()) {
		ValidateType(column.Type());
	}
#endif
}

DuckLakeCopyOptions DuckLakeVortex::CopyOptions(ClientContext &context, DuckLakeCopyInput &input) {
	ValidateEnabled();
#ifdef DUCKLAKE_ENABLE_VORTEX
	if (input.partition_data || !input.encryption_key.empty()) {
		throw NotImplementedException("Experimental Vortex writes require unpartitioned, unencrypted tables");
	}
	if (!input.catalog.MetadataType().empty() && input.catalog.MetadataType() != "duckdb") {
		throw NotImplementedException("Experimental Vortex writes require a native DuckDB metadata catalog");
	}
	auto path = LocalWritePath(context, input.data_path);
	auto names = input.columns.GetColumnNames();
	auto types = input.columns.GetColumnTypes();
	auto fields = input.field_data ? shared_ptr<DuckLakeFieldData>() : DuckLakeFieldData::FromColumns(input.columns);
	auto &field_data = input.field_data ? *input.field_data : *fields;
	vector<string> physical_names;
	for (auto &field : field_data.GetFieldIds()) {
		ValidateType(field->Type());
		physical_names.push_back(PhysicalName(field->GetFieldIndex().index));
	}
	if (input.virtual_columns == InsertVirtualColumns::WRITE_ROW_ID ||
	    input.virtual_columns == InsertVirtualColumns::WRITE_ROW_ID_AND_SNAPSHOT_ID) {
		names.push_back("_ducklake_internal_row_id");
		physical_names.push_back(names.back());
		types.push_back(LogicalType::BIGINT);
	}
	if (input.virtual_columns == InsertVirtualColumns::WRITE_SNAPSHOT_ID ||
	    input.virtual_columns == InsertVirtualColumns::WRITE_ROW_ID_AND_SNAPSHOT_ID) {
		names.push_back("_ducklake_internal_snapshot_id");
		physical_names.push_back(names.back());
		types.push_back(LogicalType::BIGINT);
	}
	auto info = make_uniq<CopyInfo>();
	info->file_path = path;
	info->format = "vortex";
	info->is_from = false;
	auto &inner = DuckLakeFunctions::GetCopyFunction(context, "vortex").function;
	CopyFunctionBindInput bind_input(*info);
	auto bind =
	    make_uniq<VortexCopyBind>(inner, inner.copy_to_bind(context, bind_input, physical_names, types), names, types);
	DuckLakeUtil::EnsureDirectoryExists(FileSystem::GetFileSystem(context), path);
	CopyFunction function("ducklake_vortex");
	function.extension = "vortex";
	function.copy_to_initialize_global = [](ClientContext &context, FunctionData &data, const string &path) {
		auto &bind = data.Cast<VortexCopyBind>();
		auto state = make_uniq<VortexCopyGlobal>();
		state->path = path;
		state->null_counts.resize(bind.names.size(), 0);
		state->inner = bind.function.copy_to_initialize_global(context, *bind.inner, path);
		return unique_ptr<GlobalFunctionData>(std::move(state));
	};
	function.copy_to_initialize_local = [](ExecutionContext &context, FunctionData &data) {
		auto &bind = data.Cast<VortexCopyBind>();
		return bind.function.copy_to_initialize_local(context, *bind.inner);
	};
	function.copy_to_sink = [](ExecutionContext &context, FunctionData &data, GlobalFunctionData &global,
	                           LocalFunctionData &local, DataChunk &chunk) {
		auto &bind = data.Cast<VortexCopyBind>();
		auto &state = global.Cast<VortexCopyGlobal>();
		lock_guard<mutex> guard(state.lock);
		bind.function.copy_to_sink(context, *bind.inner, *state.inner, local, chunk);
		for (idx_t c = 0; c < bind.names.size(); c++) {
			UnifiedVectorFormat values;
			chunk.data[c].ToUnifiedFormat(chunk.size(), values);
			for (idx_t r = 0; r < chunk.size(); r++) {
				if (!values.validity.RowIsValid(values.sel->get_index(r))) {
					state.null_counts[c]++;
				}
			}
			if (bind.names[c] != "_ducklake_internal_row_id" && bind.names[c] != "_ducklake_internal_snapshot_id") {
				continue;
			}
			for (idx_t r = 0; r < chunk.size(); r++) {
				auto value = chunk.data[c].GetValue(r).GetValue<int64_t>();
				auto entry = state.bounds.find(bind.names[c]);
				if (entry == state.bounds.end()) {
					state.bounds.emplace(bind.names[c], make_pair(value, value));
				} else {
					entry->second.first = MinValue(entry->second.first, value);
					entry->second.second = MaxValue(entry->second.second, value);
				}
			}
		}
		state.rows += chunk.size();
	};
	function.copy_to_combine = [](ExecutionContext &context, FunctionData &data, GlobalFunctionData &global,
	                              LocalFunctionData &local) {
		auto &bind = data.Cast<VortexCopyBind>();
		auto &state = global.Cast<VortexCopyGlobal>();
		lock_guard<mutex> guard(state.lock);
		if (bind.function.copy_to_combine) {
			bind.function.copy_to_combine(context, *bind.inner, *state.inner, local);
		}
	};
	function.copy_to_get_written_statistics = [](ClientContext &, FunctionData &, GlobalFunctionData &global,
	                                             CopyFunctionFileStatistics &statistics) {
		global.Cast<VortexCopyGlobal>().statistics = statistics;
	};
	function.copy_to_finalize = [](ClientContext &context, FunctionData &data, GlobalFunctionData &global) {
		auto &bind = data.Cast<VortexCopyBind>();
		auto &state = global.Cast<VortexCopyGlobal>();
		bind.function.copy_to_finalize(context, *bind.inner, *state.inner);
		if (state.statistics) {
			state.statistics->row_count = state.rows;
			state.statistics->file_size_bytes =
			    FileSystem::GetFileSystem(context).OpenFile(state.path, FileFlags::FILE_FLAGS_READ)->GetFileSize();
			state.statistics->footer_size_bytes = 0;
			for (idx_t c = 0; c < bind.names.size(); c++) {
				auto &stats = state.statistics->column_statistics[KeywordHelper::WriteQuoted(bind.names[c], '"')];
				stats["num_values"] = Value::UBIGINT(state.rows);
				stats["null_count"] = Value::UBIGINT(state.null_counts[c]);
			}
			for (auto &entry : state.bounds) {
				auto &stats = state.statistics->column_statistics[KeywordHelper::WriteQuoted(entry.first, '"')];
				stats["min"] = Value::BIGINT(entry.second.first);
				stats["max"] = Value::BIGINT(entry.second.second);
			}
		}
	};
	function.execution_mode = [](bool, bool) {
		return CopyFunctionExecutionMode::REGULAR_COPY_TO_FILE;
	};
	function.rotate_next_file = [](GlobalFunctionData &, FunctionData &, const optional_idx &) {
		return false;
	};
	DuckLakeCopyOptions result(std::move(info), std::move(function));
	result.bind_data = std::move(bind);
	result.file_path = path;
	result.file_extension = "vortex";
	result.filename_pattern.SetFilenamePattern("ducklake-{uuidv7}");
	result.use_tmp_file = false;
	result.overwrite_mode = CopyOverwriteMode::COPY_OVERWRITE_OR_IGNORE;
	result.per_thread_output = false;
	result.rotate = true;
	result.hive_file_pattern = false;
	result.return_type = CopyFunctionReturnType::WRITTEN_FILE_STATISTICS;
	result.partition_output = false;
	result.write_partition_columns = true;
	result.write_empty_file = false;
	result.names = std::move(names);
	result.expected_types = std::move(types);
	return result;
#else
	throw InternalException("Vortex support is disabled");
#endif
}

} // namespace duckdb
