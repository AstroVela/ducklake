#include "storage/ducklake_distributed_write.hpp"

#include "common/parquet_file_scanner.hpp"
#include "common/ducklake_util.hpp"
#include "storage/ducklake_geo_stats.hpp"
#include "storage/ducklake_field_data.hpp"
#include "storage/ducklake_insert.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_partition_data.hpp"
#include "storage/ducklake_sort_data.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_variant_stats.hpp"
#include "storage/ducklake_distributed_merge.hpp"

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/hive_partitioning.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/execution/distributed/copy_finalize.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/function/distributed_write.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

namespace duckdb {

namespace {

static constexpr const char DUCKLAKE_DISTRIBUTED_ARTIFACT_PREFIX[] = ".vane-ducklake-";

static void AppendDistributedIdentity(string &result, const string &value) {
	result += to_string(value.size());
	result += ':';
	result += value;
	result += ';';
}

static void AppendDistributedFieldIdentity(string &result, const DuckLakeFieldId &field) {
	AppendDistributedIdentity(result, to_string(field.GetFieldIndex().index));
	AppendDistributedIdentity(result, field.Name());
	AppendDistributedIdentity(result, field.Type().ToString());
	AppendDistributedIdentity(result, field.GetColumnData().initial_default.ToString());
	if (field.GetColumnData().default_value) {
		AppendDistributedIdentity(result, field.GetColumnData().default_value->ToString());
	} else {
		AppendDistributedIdentity(result, string());
	}
	for (const auto &child : field.Children()) {
		AppendDistributedFieldIdentity(result, *child);
	}
	AppendDistributedIdentity(result, "end-field");
}

static string GetPartitionColumnName(const ColumnRefExpression &column_ref) {
	if (column_ref.IsQualified()) {
		throw InvalidInputException("DuckLake distributed CTAS partition columns must be unqualified");
	}
	return column_ref.GetColumnName();
}

static DuckLakePartitionField PlanPartitionField(const ColumnList &columns, const DuckLakeFieldData &field_data,
                                                 const ParsedExpression &expression) {
	string column_name;
	DuckLakePartitionField result;

	switch (expression.type) {
	case ExpressionType::COLUMN_REF: {
		auto &column_ref = expression.Cast<ColumnRefExpression>();
		column_name = GetPartitionColumnName(column_ref);
		result.transform.type = DuckLakeTransformType::IDENTITY;
		break;
	}
	case ExpressionType::FUNCTION: {
		auto &function = expression.Cast<FunctionExpression>();
		auto function_name = StringUtil::Lower(function.function_name);
		if (function_name == "bucket") {
			result.transform.type = DuckLakeTransformType::BUCKET;
			if (function.children.size() != 2 || function.children[0]->type != ExpressionType::VALUE_CONSTANT ||
			    function.children[1]->type != ExpressionType::COLUMN_REF) {
				throw InvalidInputException("Expected bucket(bucket_count, column), but got %s", expression.ToString());
			}
			auto bucket_value = function.children[0]->Cast<ConstantExpression>().value;
			if (!bucket_value.DefaultTryCastAs(LogicalType::BIGINT)) {
				throw InvalidInputException("Bucket count must be an integer");
			}
			auto bucket_count = bucket_value.GetValue<int64_t>();
			if (bucket_count <= 0 || bucket_count > NumericLimits<int32_t>::Maximum()) {
				throw InvalidInputException("Bucket count must be between 1 and %d", NumericLimits<int32_t>::Maximum());
			}
			result.transform.bucket_count = NumericCast<idx_t>(bucket_count);
			column_name = GetPartitionColumnName(function.children[1]->Cast<ColumnRefExpression>());
			break;
		}

		if (function_name == "year") {
			result.transform.type = DuckLakeTransformType::YEAR;
		} else if (function_name == "month") {
			result.transform.type = DuckLakeTransformType::MONTH;
		} else if (function_name == "day") {
			result.transform.type = DuckLakeTransformType::DAY;
		} else if (function_name == "hour") {
			result.transform.type = DuckLakeTransformType::HOUR;
		} else {
			throw NotImplementedException("Unsupported DuckLake distributed CTAS partition function %s", function_name);
		}
		if (function.children.size() != 1 || function.children[0]->type != ExpressionType::COLUMN_REF) {
			throw InvalidInputException("Expected %s(column), but got %s", function_name, expression.ToString());
		}
		column_name = GetPartitionColumnName(function.children[0]->Cast<ColumnRefExpression>());
		break;
	}
	default:
		throw NotImplementedException("Unsupported DuckLake distributed CTAS partition key %s", expression.ToString());
	}

	if (!columns.ColumnExists(column_name)) {
		throw CatalogException("Unexpected partition key - column \"%s\" does not exist", column_name);
	}
	auto &column = columns.GetColumn(column_name);
	result.field_id = field_data.GetByRootIndex(PhysicalIndex(column.StorageOid())).GetFieldIndex();
	return result;
}

static string CanonicalDuckLakePath(FileSystem &file_system, const string &path, const string &description) {
	auto canonical = distributed::CanonicalDistributedCopyBasePath(file_system, path);
	if (canonical.is_err()) {
		throw InvalidInputException("Invalid DuckLake distributed %s path: %s", description, canonical.error().what());
	}
	return std::move(canonical).value();
}

static string ValidateArtifactRoot(FileSystem &file_system, const string &data_path, const string &artifact_path) {
	auto canonical_data_path = CanonicalDuckLakePath(file_system, data_path, "data root");
	auto canonical_artifact_path = CanonicalDuckLakePath(file_system, artifact_path, "artifact root");
	auto separator = file_system.PathSeparator(canonical_data_path);
	if (separator.empty() || canonical_artifact_path == canonical_data_path ||
	    !distributed::DistributedCopyPathIsInDirectory(canonical_artifact_path, canonical_data_path, separator)) {
		throw InvalidInputException("DuckLake distributed artifact root is outside its table data root");
	}

	auto relative_path = canonical_artifact_path.substr(canonical_data_path.size());
	while (StringUtil::StartsWith(relative_path, separator)) {
		relative_path.erase(0, separator.size());
	}
	if (!StringUtil::StartsWith(relative_path, DUCKLAKE_DISTRIBUTED_ARTIFACT_PREFIX) ||
	    relative_path.find(separator) != string::npos) {
		throw InvalidInputException("DuckLake distributed artifact root has an invalid write identity");
	}
	auto write_id = relative_path.substr(sizeof(DUCKLAKE_DISTRIBUTED_ARTIFACT_PREFIX) - 1);
	hugeint_t parsed_write_id;
	if (!BaseUUID::FromString(write_id, parsed_write_id, true)) {
		throw InvalidInputException("DuckLake distributed artifact root has an invalid write identity");
	}
	return canonical_artifact_path;
}

static void CleanupArtifactPaths(FileSystem &file_system, const vector<string> &paths) {
	vector<string> errors;
	for (const auto &path : paths) {
		auto cleanup_result = distributed::CleanupDistributedCopyPrefix(file_system, path);
		if (cleanup_result.is_err()) {
			errors.push_back(cleanup_result.error().what());
		}
	}
	if (!errors.empty()) {
		throw IOException("Failed to clean DuckLake distributed artifacts: %s", StringUtil::Join(errors, "; "));
	}
}

struct ValidatedArtifactLocation {
	string canonical_path;
	vector<string> components;
};

struct ParquetArtifactMetadata {
	Value row_groups;
	Value schema;
};

struct ExpectedArtifactField {
	const DuckLakeFieldId *field;
	vector<string> path;
	optional_idx parent_field_id;
};

struct ParquetSchemaFrame {
	idx_t remaining_children;
	optional_idx field_id;
};

static constexpr idx_t PARQUET_FULL_METADATA_ROW_GROUPS = 1;
static constexpr idx_t PARQUET_FULL_METADATA_SCHEMA = 2;

static constexpr idx_t PARQUET_METADATA_COLUMN_ID = 5;
static constexpr idx_t PARQUET_METADATA_NUM_VALUES = 7;
static constexpr idx_t PARQUET_METADATA_STATS_MIN = 10;
static constexpr idx_t PARQUET_METADATA_STATS_MAX = 11;
static constexpr idx_t PARQUET_METADATA_STATS_NULL_COUNT = 12;
static constexpr idx_t PARQUET_METADATA_STATS_MIN_VALUE = 14;
static constexpr idx_t PARQUET_METADATA_STATS_MAX_VALUE = 15;
static constexpr idx_t PARQUET_METADATA_TOTAL_COMPRESSED_SIZE = 21;
static constexpr idx_t PARQUET_METADATA_MIN_IS_EXACT = 26;
static constexpr idx_t PARQUET_METADATA_MAX_IS_EXACT = 27;
static constexpr idx_t PARQUET_METADATA_GEO_BBOX = 29;
static constexpr idx_t PARQUET_METADATA_GEO_TYPES = 30;

static constexpr idx_t PARQUET_SCHEMA_NAME = 1;
static constexpr idx_t PARQUET_SCHEMA_NUM_CHILDREN = 5;
static constexpr idx_t PARQUET_SCHEMA_FIELD_ID = 9;

static ParquetArtifactMetadata ReadParquetArtifactMetadata(ClientContext &context, const string &path) {
	auto &instance = DatabaseInstance::GetDatabase(context);
	ExtensionLoader loader(instance, "ducklake");
	auto &function_entry = loader.GetTableFunction("parquet_full_metadata");
	auto function = function_entry.functions.functions[0];

	vector<Value> children {Value(path)};
	named_parameter_map_t named_parameters;
	vector<LogicalType> input_types;
	vector<string> input_names;
	TableFunctionRef empty;
	TableFunction dummy_function;
	dummy_function.name = "DuckLakeDistributedParquetMetadata";
	TableFunctionBindInput bind_input(children, named_parameters, input_types, input_names, nullptr, nullptr,
	                                  dummy_function, empty);
	vector<LogicalType> return_types;
	vector<string> return_names;
	auto bind_data = function.bind(context, bind_input, return_types, return_names);
	if (return_types.size() <= PARQUET_FULL_METADATA_SCHEMA) {
		throw InternalException("Parquet full metadata returned an invalid schema");
	}

	vector<column_t> column_ids;
	for (idx_t index = 0; index < return_types.size(); index++) {
		column_ids.push_back(index);
	}
	ThreadContext thread_context(context);
	ExecutionContext execution_context(context, thread_context, nullptr);
	TableFunctionInitInput init_input(bind_data.get(), column_ids, vector<idx_t>(), nullptr);
	auto global_state = function.init_global(context, init_input);
	auto local_state = function.init_local(execution_context, init_input, global_state.get());
	TableFunctionInput function_input(bind_data.get(), local_state.get(), global_state.get());
	DataChunk chunk;
	chunk.Initialize(context, return_types);
	function.function(context, function_input, chunk);
	if (chunk.size() != 1) {
		throw InvalidInputException("Parquet full metadata returned %s files for '%s'", to_string(chunk.size()), path);
	}

	ParquetArtifactMetadata result;
	result.row_groups = chunk.GetValue(PARQUET_FULL_METADATA_ROW_GROUPS, 0);
	result.schema = chunk.GetValue(PARQUET_FULL_METADATA_SCHEMA, 0);
	return result;
}

static void CollectExpectedArtifactFields(const DuckLakeFieldId &field, const vector<string> &parent_path,
                                          const optional_idx &parent_field_id,
                                          unordered_map<idx_t, ExpectedArtifactField> &result) {
	auto field_index = field.GetFieldIndex().index;
	if (field_index > NumericLimits<int32_t>::Maximum()) {
		throw InvalidInputException("DuckLake distributed target contains an invalid Parquet field id");
	}
	auto path = parent_path;
	path.push_back(field.Name());
	if (!result.emplace(field_index, ExpectedArtifactField {&field, path, parent_field_id}).second) {
		throw InternalException("DuckLake distributed target contains duplicate field id %s", to_string(field_index));
	}
	for (const auto &child : field.Children()) {
		CollectExpectedArtifactFields(*child, path, optional_idx(field_index), result);
	}
}

static unordered_map<idx_t, ExpectedArtifactField> GetExpectedArtifactFields(const DuckLakeFieldData &field_data) {
	unordered_map<idx_t, ExpectedArtifactField> result;
	for (const auto &field : field_data.GetFieldIds()) {
		CollectExpectedArtifactFields(*field, {}, optional_idx(), result);
	}
	return result;
}

static idx_t GetOptionalNonNegativeIndex(const Value &value, const string &description) {
	if (value.IsNull()) {
		throw InvalidInputException("DuckLake distributed Parquet metadata is missing %s", description);
	}
	auto index = BigIntValue::Get(value);
	if (index < 0) {
		throw InvalidInputException("DuckLake distributed Parquet metadata contains an invalid %s", description);
	}
	return NumericCast<idx_t>(index);
}

static vector<const ExpectedArtifactField *>
ValidateArtifactSchema(const ParquetFileScanner &scanner, const Value &schema, const DuckLakeFieldData &field_data,
                       const unordered_map<idx_t, ExpectedArtifactField> &expected_fields, const string &path) {
	auto &expected_roots = field_data.GetFieldIds();
	auto &actual_names = scanner.GetNames();
	auto &actual_types = scanner.GetTypes();
	if (actual_names.size() != expected_roots.size() || actual_types.size() != expected_roots.size()) {
		throw InvalidInputException("DuckLake distributed data-file schema mismatch for '%s'", path);
	}
	for (idx_t index = 0; index < expected_roots.size(); index++) {
		if (actual_names[index] != expected_roots[index]->Name() ||
		    actual_types[index] != expected_roots[index]->Type()) {
			throw InvalidInputException(
			    "DuckLake distributed data-file schema mismatch for '%s' at column '%s' (found '%s' %s)", path,
			    expected_roots[index]->Name(), actual_names[index], actual_types[index].ToString());
		}
	}

	auto &schema_entries = ListValue::GetChildren(schema);
	if (schema_entries.empty()) {
		throw InvalidInputException("DuckLake distributed data-file schema is empty for '%s'", path);
	}
	auto &root = StructValue::GetChildren(schema_entries[0]);
	if (root.size() <= PARQUET_SCHEMA_FIELD_ID || !root[PARQUET_SCHEMA_FIELD_ID].IsNull()) {
		throw InvalidInputException("DuckLake distributed data-file root has an invalid field id for '%s'", path);
	}
	auto root_children = GetOptionalNonNegativeIndex(root[PARQUET_SCHEMA_NUM_CHILDREN], "schema root child count");
	if (root_children != expected_roots.size()) {
		throw InvalidInputException("DuckLake distributed data-file schema mismatch for '%s'", path);
	}

	vector<ParquetSchemaFrame> stack;
	stack.push_back(ParquetSchemaFrame {root_children, optional_idx()});
	unordered_set<idx_t> seen_field_ids;
	vector<const ExpectedArtifactField *> leaf_fields;
	for (idx_t schema_index = 1; schema_index < schema_entries.size(); schema_index++) {
		while (!stack.empty() && stack.back().remaining_children == 0) {
			stack.pop_back();
		}
		if (stack.empty()) {
			throw InvalidInputException("DuckLake distributed data-file schema is unaligned for '%s'", path);
		}
		auto parent_field_id = stack.back().field_id;
		stack.back().remaining_children--;

		auto &children = StructValue::GetChildren(schema_entries[schema_index]);
		if (children.size() <= PARQUET_SCHEMA_FIELD_ID || children[PARQUET_SCHEMA_NAME].IsNull()) {
			throw InvalidInputException("DuckLake distributed data-file schema is invalid for '%s'", path);
		}
		auto &name = StringValue::Get(children[PARQUET_SCHEMA_NAME]);
		idx_t child_count = 0;
		if (!children[PARQUET_SCHEMA_NUM_CHILDREN].IsNull()) {
			child_count = GetOptionalNonNegativeIndex(children[PARQUET_SCHEMA_NUM_CHILDREN], "schema child count");
		}

		optional_idx current_field_id = parent_field_id;
		const ExpectedArtifactField *expected_field = nullptr;
		if (!children[PARQUET_SCHEMA_FIELD_ID].IsNull()) {
			auto field_id = GetOptionalNonNegativeIndex(children[PARQUET_SCHEMA_FIELD_ID], "field id");
			auto expected_entry = expected_fields.find(field_id);
			if (expected_entry == expected_fields.end() || !seen_field_ids.insert(field_id).second) {
				throw InvalidInputException("DuckLake distributed data-file schema has an invalid field id for '%s'",
				                            path);
			}
			expected_field = &expected_entry->second;
			if (name != expected_field->field->Name() || parent_field_id != expected_field->parent_field_id) {
				throw InvalidInputException(
				    "DuckLake distributed data-file schema has an invalid field-id mapping for '%s'", path);
			}
			current_field_id = field_id;
		}
		if (child_count == 0) {
			leaf_fields.push_back(expected_field);
		} else {
			stack.push_back(ParquetSchemaFrame {child_count, current_field_id});
		}
	}
	while (!stack.empty() && stack.back().remaining_children == 0) {
		stack.pop_back();
	}
	if (!stack.empty() || seen_field_ids.size() != expected_fields.size()) {
		throw InvalidInputException("DuckLake distributed data-file schema is missing field ids for '%s'", path);
	}
	return leaf_fields;
}

static void ReadGeoStatistics(const vector<Value> &metadata, DuckLakeColumnStats &statistics) {
	if (!statistics.extra_stats || statistics.extra_stats->GetStatsType() != DuckLakeExtraStatsType::GEOMETRY) {
		return;
	}
	auto &geo_statistics = statistics.extra_stats->Cast<DuckLakeColumnGeoStats>();
	if (!metadata[PARQUET_METADATA_GEO_BBOX].IsNull()) {
		auto &bbox = StructValue::GetChildren(metadata[PARQUET_METADATA_GEO_BBOX]);
		if (!bbox[0].IsNull()) {
			geo_statistics.xmin = DoubleValue::Get(bbox[0]);
		}
		if (!bbox[1].IsNull()) {
			geo_statistics.xmax = DoubleValue::Get(bbox[1]);
		}
		if (!bbox[2].IsNull()) {
			geo_statistics.ymin = DoubleValue::Get(bbox[2]);
		}
		if (!bbox[3].IsNull()) {
			geo_statistics.ymax = DoubleValue::Get(bbox[3]);
		}
		if (!bbox[4].IsNull()) {
			geo_statistics.zmin = DoubleValue::Get(bbox[4]);
		}
		if (!bbox[5].IsNull()) {
			geo_statistics.zmax = DoubleValue::Get(bbox[5]);
		}
		if (!bbox[6].IsNull()) {
			geo_statistics.mmin = DoubleValue::Get(bbox[6]);
		}
		if (!bbox[7].IsNull()) {
			geo_statistics.mmax = DoubleValue::Get(bbox[7]);
		}
	}
	if (!metadata[PARQUET_METADATA_GEO_TYPES].IsNull()) {
		for (const auto &type : ListValue::GetChildren(metadata[PARQUET_METADATA_GEO_TYPES])) {
			geo_statistics.geo_types.insert(StringValue::Get(type));
		}
	}
}

static Value SerializeArtifactStatistics(const map<string, DuckLakeColumnStats> &statistics) {
	vector<Value> column_names;
	vector<Value> column_values;
	for (const auto &column : statistics) {
		map<string, Value> values;
		auto &stats = column.second;
		values.emplace("column_size_bytes", Value::UBIGINT(stats.column_size_bytes));
		if (stats.has_num_values) {
			values.emplace("num_values", Value::UBIGINT(stats.num_values));
		}
		if (stats.has_min) {
			values.emplace("min", Value(stats.min));
		}
		if (stats.has_max) {
			values.emplace("max", Value(stats.max));
		}
		if (stats.has_null_count) {
			values.emplace("null_count", Value::UBIGINT(stats.null_count));
		}
		if (stats.extra_stats && stats.extra_stats->GetStatsType() == DuckLakeExtraStatsType::GEOMETRY) {
			auto &geo_stats = stats.extra_stats->Cast<DuckLakeColumnGeoStats>();
			if (geo_stats.xmin != NumericLimits<double>::Maximum()) {
				values.emplace("bbox_xmin", Value::DOUBLE(geo_stats.xmin));
				values.emplace("bbox_xmax", Value::DOUBLE(geo_stats.xmax));
				values.emplace("bbox_ymin", Value::DOUBLE(geo_stats.ymin));
				values.emplace("bbox_ymax", Value::DOUBLE(geo_stats.ymax));
			}
			if (geo_stats.zmin != NumericLimits<double>::Maximum()) {
				values.emplace("bbox_zmin", Value::DOUBLE(geo_stats.zmin));
				values.emplace("bbox_zmax", Value::DOUBLE(geo_stats.zmax));
			}
			if (geo_stats.mmin != NumericLimits<double>::Maximum()) {
				values.emplace("bbox_mmin", Value::DOUBLE(geo_stats.mmin));
				values.emplace("bbox_mmax", Value::DOUBLE(geo_stats.mmax));
			}
			if (!geo_stats.geo_types.empty()) {
				vector<Value> types;
				for (const auto &type : geo_stats.geo_types) {
					types.emplace_back(type);
				}
				values.emplace("geo_types", Value::LIST(LogicalType::VARCHAR, std::move(types)));
			}
		}

		vector<Value> names;
		vector<Value> stats_values;
		for (auto &entry : values) {
			names.emplace_back(entry.first);
			stats_values.push_back(std::move(entry.second));
		}
		column_names.emplace_back(column.first);
		column_values.push_back(
		    Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR, std::move(names), std::move(stats_values)));
	}
	auto stats_type = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR);
	return Value::MAP(LogicalType::VARCHAR, stats_type, std::move(column_names), std::move(column_values));
}

static void MergeArtifactStatistics(DuckLakeColumnStats &target, const DuckLakeColumnStats &source) {
	const auto signed_max = NumericCast<idx_t>(NumericLimits<int64_t>::Maximum());
	if (target.column_size_bytes > signed_max - source.column_size_bytes ||
	    (target.has_num_values && source.has_num_values && target.num_values > signed_max - source.num_values) ||
	    (target.has_null_count && source.has_null_count && target.null_count > signed_max - source.null_count)) {
		throw InvalidInputException("DuckLake distributed Parquet metadata contains overflowing column statistics");
	}
	target.MergeStats(source);
}

static Value GetArtifactStatistics(const Value &row_groups, const vector<const ExpectedArtifactField *> &leaf_fields,
                                   const string &path) {
	map<string, DuckLakeColumnStats> result;
	for (const auto &row_group : ListValue::GetChildren(row_groups)) {
		auto &metadata = StructValue::GetChildren(row_group);
		if (metadata.size() <= PARQUET_METADATA_GEO_TYPES) {
			throw InvalidInputException("DuckLake distributed Parquet metadata is invalid for '%s'", path);
		}
		auto column_id = GetOptionalNonNegativeIndex(metadata[PARQUET_METADATA_COLUMN_ID], "column id");
		if (column_id >= leaf_fields.size()) {
			throw InvalidInputException("DuckLake distributed Parquet metadata has an invalid column id for '%s'",
			                            path);
		}
		auto expected_field = leaf_fields[column_id];
		if (!expected_field) {
			continue;
		}

		DuckLakeColumnStats statistics(expected_field->field->Type());
		statistics.has_num_values = true;
		statistics.num_values = GetOptionalNonNegativeIndex(metadata[PARQUET_METADATA_NUM_VALUES], "value count");
		if (!metadata[PARQUET_METADATA_STATS_NULL_COUNT].IsNull()) {
			statistics.has_null_count = true;
			statistics.null_count =
			    GetOptionalNonNegativeIndex(metadata[PARQUET_METADATA_STATS_NULL_COUNT], "null count");
			if (statistics.null_count > statistics.num_values) {
				throw InvalidInputException(
				    "DuckLake distributed Parquet metadata has invalid value/null counts for '%s'", path);
			}
		}
		const auto min_is_exact = !metadata[PARQUET_METADATA_MIN_IS_EXACT].IsNull() &&
		                          BooleanValue::Get(metadata[PARQUET_METADATA_MIN_IS_EXACT]);
		const auto max_is_exact = !metadata[PARQUET_METADATA_MAX_IS_EXACT].IsNull() &&
		                          BooleanValue::Get(metadata[PARQUET_METADATA_MAX_IS_EXACT]);
		if (min_is_exact && !metadata[PARQUET_METADATA_STATS_MIN].IsNull()) {
			statistics.has_min = true;
			statistics.min = StringValue::Get(metadata[PARQUET_METADATA_STATS_MIN]);
		} else if (min_is_exact && !metadata[PARQUET_METADATA_STATS_MIN_VALUE].IsNull()) {
			statistics.has_min = true;
			statistics.min = StringValue::Get(metadata[PARQUET_METADATA_STATS_MIN_VALUE]);
		}
		if (max_is_exact && !metadata[PARQUET_METADATA_STATS_MAX].IsNull()) {
			statistics.has_max = true;
			statistics.max = StringValue::Get(metadata[PARQUET_METADATA_STATS_MAX]);
		} else if (max_is_exact && !metadata[PARQUET_METADATA_STATS_MAX_VALUE].IsNull()) {
			statistics.has_max = true;
			statistics.max = StringValue::Get(metadata[PARQUET_METADATA_STATS_MAX_VALUE]);
		}
		statistics.column_size_bytes =
		    GetOptionalNonNegativeIndex(metadata[PARQUET_METADATA_TOTAL_COMPRESSED_SIZE], "compressed size");
		ReadGeoStatistics(metadata, statistics);

		auto column_path = DuckLakeUtil::ToQuotedList(expected_field->path, '.');
		auto entry = result.find(column_path);
		if (entry == result.end()) {
			result.emplace(std::move(column_path), std::move(statistics));
		} else {
			MergeArtifactStatistics(entry->second, statistics);
		}
	}
	return SerializeArtifactStatistics(result);
}

static ValidatedArtifactLocation ValidateArtifactCleanupLocation(FileSystem &file_system, const string &data_path,
                                                                 const string &path,
                                                                 const vector<string> &partition_names) {
	auto canonical_root = CanonicalDuckLakePath(file_system, data_path, "data root");
	auto canonical_path = CanonicalDuckLakePath(file_system, path, "data file");
	auto separator = file_system.PathSeparator(canonical_root);
	if (separator.empty() || canonical_path == canonical_root ||
	    !distributed::DistributedCopyPathIsInDirectory(canonical_path, canonical_root, separator)) {
		throw InvalidInputException("DuckLake distributed data-file path is outside its table data root");
	}
	if (!StringUtil::EndsWith(StringUtil::Lower(canonical_path), ".parquet")) {
		throw InvalidInputException("DuckLake distributed data-file artifact must be a Parquet file");
	}

	auto relative_path = canonical_path.substr(canonical_root.size());
	while (StringUtil::StartsWith(relative_path, separator)) {
		relative_path.erase(0, separator.size());
	}
	ValidatedArtifactLocation result;
	result.canonical_path = std::move(canonical_path);
	idx_t component_start = 0;
	while (component_start <= relative_path.size()) {
		auto component_end = relative_path.find(separator, component_start);
		auto component = relative_path.substr(
		    component_start, component_end == string::npos ? string::npos : component_end - component_start);
		if (component.empty() || component == "." || component == "..") {
			throw InvalidInputException("DuckLake distributed data-file path contains an invalid component");
		}
		result.components.push_back(std::move(component));
		if (component_end == string::npos) {
			break;
		}
		component_start = component_end + separator.size();
	}
	if (result.components.size() != partition_names.size() + 1) {
		throw InvalidInputException("DuckLake distributed data-file path has an invalid partition layout");
	}
	for (idx_t index = 0; index < partition_names.size(); index++) {
		auto partition_prefix = HivePartitioning::Escape(partition_names[index]) + "=";
		if (!StringUtil::StartsWith(result.components[index], partition_prefix)) {
			throw InvalidInputException("DuckLake distributed data-file path does not match its partition values");
		}
	}
	return result;
}

static string ValidateArtifactLocation(FileSystem &file_system, const string &data_path, const string &path,
                                       const vector<string> &partition_names,
                                       const vector<string> &expected_partition_components) {
	auto location = ValidateArtifactCleanupLocation(file_system, data_path, path, partition_names);
	for (idx_t index = 0; index < expected_partition_components.size(); index++) {
		if (location.components[index] != expected_partition_components[index]) {
			throw InvalidInputException("DuckLake distributed data-file path does not match its partition values");
		}
	}
	return std::move(location.canonical_path);
}

static Value ValidateArtifactContents(ClientContext &context, FileSystem &file_system, const string &canonical_path,
                                      const string &path, idx_t expected_size, idx_t expected_footer_size,
                                      idx_t expected_row_count, const DuckLakeFieldData &field_data,
                                      const unordered_map<idx_t, ExpectedArtifactField> &expected_fields) {
	try {
		auto handle = file_system.OpenFile(canonical_path, FileFlags::FILE_FLAGS_READ);
		auto actual_size = handle->GetFileSize();
		if (actual_size != expected_size) {
			throw InvalidInputException(
			    "DuckLake distributed data-file size mismatch for '%s' (worker reported %s bytes, found %s)", path,
			    to_string(expected_size), to_string(actual_size));
		}
		if (actual_size < expected_footer_size || actual_size - expected_footer_size < 12) {
			throw InvalidInputException("DuckLake distributed data-file footer size is invalid for '%s'", path);
		}
		data_t header[4];
		data_t trailer[8];
		handle->Read(header, sizeof(header), 0);
		handle->Read(trailer, sizeof(trailer), actual_size - sizeof(trailer));
		if (memcmp(header, "PAR1", sizeof(header)) != 0 || memcmp(trailer + 4, "PAR1", 4) != 0) {
			throw InvalidInputException("DuckLake distributed data-file artifact '%s' is not a Parquet file", path);
		}
		if (LoadLE<uint32_t>(trailer) != expected_footer_size) {
			throw InvalidInputException("DuckLake distributed data-file footer mismatch for '%s'", path);
		}

		DuckLakeFileData artifact;
		artifact.path = canonical_path;
		artifact.file_size_bytes = actual_size;
		artifact.footer_size = expected_footer_size;
		ParquetFileScanner scanner(context, artifact);
		auto actual_row_count = scanner.GetRowCount();
		if (actual_row_count != expected_row_count) {
			throw InvalidInputException(
			    "DuckLake distributed data-file row-count mismatch for '%s' (worker reported %s rows, found %s)", path,
			    to_string(expected_row_count), to_string(actual_row_count));
		}
		auto metadata = ReadParquetArtifactMetadata(context, canonical_path);
		auto leaf_fields = ValidateArtifactSchema(scanner, metadata.schema, field_data, expected_fields, path);
		return GetArtifactStatistics(metadata.row_groups, leaf_fields, path);
	} catch (const InvalidInputException &) {
		throw;
	} catch (const std::exception &error) {
		throw IOException("Failed to validate DuckLake distributed data-file artifact '%s': %s", path, error.what());
	}
}

static const DuckLakeFieldId &ResolveStatsField(const DuckLakeFieldData &field_data, const vector<string> &column_names,
                                                optional_idx &variant_name_offset) {
	if (column_names.empty()) {
		throw InvalidInputException("DuckLake distributed data file contains an empty column-statistics path");
	}
	optional_idx root_index;
	for (idx_t index = 0; index < field_data.GetFieldIds().size(); index++) {
		if (StringUtil::CIEquals(field_data.GetFieldIds()[index]->Name(), column_names[0])) {
			root_index = index;
			break;
		}
	}
	if (!root_index.IsValid()) {
		throw InvalidInputException("DuckLake distributed data file contains statistics for unknown column '%s'",
		                            column_names[0]);
	}
	auto field = field_data.GetByNames(PhysicalIndex(root_index.GetIndex()), column_names, &variant_name_offset);
	if (!field) {
		throw InvalidInputException("DuckLake distributed data file contains statistics for unknown field '%s'",
		                            StringUtil::Join(column_names, "."));
	}
	return *field;
}

static void ValidateParsedColumnStatistics(const DuckLakeColumnStats &statistics) {
	const auto signed_max = NumericCast<idx_t>(NumericLimits<int64_t>::Maximum());
	if (statistics.column_size_bytes > signed_max ||
	    (statistics.has_null_count && statistics.null_count > signed_max) ||
	    (statistics.has_num_values && statistics.num_values > signed_max) ||
	    (statistics.has_null_count && statistics.has_num_values && statistics.null_count > statistics.num_values)) {
		throw InvalidInputException("DuckLake distributed data file contains invalid column-statistics counts");
	}
	if (statistics.has_min && statistics.has_max && RequiresValueComparison(statistics.type)) {
		auto minimum = Value(statistics.min).DefaultCastAs(statistics.type);
		auto maximum = Value(statistics.max).DefaultCastAs(statistics.type);
		if (maximum < minimum) {
			throw InvalidInputException("DuckLake distributed data file contains inverted column statistics");
		}
	}
	statistics.ToStats();
	if (!statistics.extra_stats || statistics.extra_stats->GetStatsType() != DuckLakeExtraStatsType::VARIANT) {
		return;
	}
	auto &variant_statistics = statistics.extra_stats->Cast<DuckLakeColumnVariantStats>();
	for (const auto &entry : variant_statistics.shredded_field_stats) {
		ValidateParsedColumnStatistics(entry.second.field_stats);
	}
}

static void ValidateColumnStatistics(const Value &column_statistics, const DuckLakeFieldData &field_data,
                                     const case_insensitive_set_t &not_null_fields, idx_t row_count) {
	case_insensitive_set_t column_paths;
	auto missing_not_null_fields = not_null_fields;
	map<FieldIndex, PartialVariantStats> variant_stats;
	for (const auto &column_entry : MapValue::GetChildren(column_statistics)) {
		if (column_entry.IsNull()) {
			throw InvalidInputException("DuckLake distributed data file contains a null column-statistics entry");
		}
		auto &column_children = StructValue::GetChildren(column_entry);
		if (column_children.size() != 2 || column_children[0].IsNull() || column_children[1].IsNull()) {
			throw InvalidInputException("DuckLake distributed data file contains an invalid column-statistics entry");
		}
		auto &column_path = StringValue::Get(column_children[0]);
		if (column_path.empty() || !column_paths.insert(column_path).second) {
			throw InvalidInputException(
			    "DuckLake distributed data file contains an empty or duplicate column-statistics path");
		}
		auto column_names = DuckLakeUtil::ParseQuotedList(column_path, '.');
		optional_idx variant_name_offset;
		auto &field = ResolveStatsField(field_data, column_names, variant_name_offset);

		case_insensitive_set_t statistic_names;
		auto &statistics = MapValue::GetChildren(column_children[1]);
		for (const auto &statistic_entry : statistics) {
			if (statistic_entry.IsNull()) {
				throw InvalidInputException("DuckLake distributed data file contains a null statistic");
			}
			auto &statistic_children = StructValue::GetChildren(statistic_entry);
			if (statistic_children.size() != 2 || statistic_children[0].IsNull() || statistic_children[1].IsNull()) {
				throw InvalidInputException("DuckLake distributed data file contains an invalid statistic");
			}
			auto &statistic_name = StringValue::Get(statistic_children[0]);
			if (statistic_name.empty() || !statistic_names.insert(statistic_name).second) {
				throw InvalidInputException(
				    "DuckLake distributed data file contains an empty or duplicate statistic name");
			}
			auto &statistic_value = StringValue::Get(statistic_children[1]);
			if (statistic_name == "has_nan" && statistic_value != "true" && statistic_value != "false") {
				throw InvalidInputException("DuckLake distributed data file contains an invalid has_nan statistic");
			}
		}
		if (variant_name_offset.IsValid()) {
			if (field.Type().id() != LogicalTypeId::VARIANT) {
				throw InvalidInputException("DuckLake distributed data file has invalid nested column statistics");
			}
			auto entry = variant_stats.find(field.GetFieldIndex());
			if (entry == variant_stats.end()) {
				entry = variant_stats.insert(make_pair(field.GetFieldIndex(), PartialVariantStats())).first;
			}
			entry->second.ParseVariantStats(column_names, variant_name_offset.GetIndex(), statistics);
			continue;
		}
		if (field.Type().id() == LogicalTypeId::VARIANT) {
			throw InvalidInputException("DuckLake distributed data file has statistics for a top-level variant");
		}
		auto parsed_statistics = DuckLakeInsert::ParseColumnStats(field.Type(), statistics);
		ValidateParsedColumnStatistics(parsed_statistics);
		if (column_names.size() == 1 && not_null_fields.count(field.Name())) {
			if (!parsed_statistics.has_null_count || !parsed_statistics.has_num_values ||
			    parsed_statistics.num_values != row_count) {
				throw InvalidInputException(
				    "DuckLake distributed data file has incomplete row/null statistics for NOT NULL column '%s'",
				    field.Name());
			}
			missing_not_null_fields.erase(field.Name());
		}
	}
	if (!missing_not_null_fields.empty()) {
		throw InvalidInputException(
		    "DuckLake distributed data file is missing column statistics for NOT NULL column '%s'",
		    *missing_not_null_fields.begin());
	}
	for (auto &entry : variant_stats) {
		auto parsed_statistics = entry.second.Finalize();
		ValidateParsedColumnStatistics(parsed_statistics);
	}
}

static vector<string> ValidatePartitionValues(const Value &partition_keys, const vector<string> &expected_names) {
	vector<string> result;
	if (partition_keys.IsNull()) {
		if (!expected_names.empty()) {
			throw InvalidInputException("DuckLake distributed data file is missing partition values");
		}
		return result;
	}
	auto &entries = MapValue::GetChildren(partition_keys);
	if (entries.size() != expected_names.size()) {
		throw InvalidInputException("DuckLake distributed data file has an incorrect number of partition values");
	}
	for (idx_t index = 0; index < entries.size(); index++) {
		if (entries[index].IsNull()) {
			throw InvalidInputException("DuckLake distributed data file contains a null partition entry");
		}
		auto &children = StructValue::GetChildren(entries[index]);
		if (children.size() != 2 || children[0].IsNull()) {
			throw InvalidInputException("DuckLake distributed data file contains an invalid partition entry");
		}
		auto &name = StringValue::Get(children[0]);
		if (name != expected_names[index]) {
			throw InvalidInputException("DuckLake distributed data-file partition '%s' is out of order; expected '%s'",
			                            name, expected_names[index]);
		}
		auto value = children[1].IsNull() ? "__HIVE_DEFAULT_PARTITION__"
		                                  : HivePartitioning::Escape(StringValue::Get(children[1]));
		result.push_back(HivePartitioning::Escape(name) + "=" + value);
	}
	return result;
}

} // namespace

string GetDuckLakeDistributedFieldIdentity(const DuckLakeFieldData &field_data) {
	string result;
	for (const auto &field : field_data.GetFieldIds()) {
		AppendDistributedFieldIdentity(result, *field);
	}
	return result;
}

string GetDuckLakeDistributedPartitionIdentity(const DuckLakePartition *partition_data) {
	if (!partition_data) {
		return string();
	}
	string result;
	AppendDistributedIdentity(result, to_string(partition_data->partition_id));
	for (const auto &field : partition_data->fields) {
		AppendDistributedIdentity(result, to_string(field.partition_key_index));
		AppendDistributedIdentity(result, to_string(field.field_id.index));
		AppendDistributedIdentity(result, to_string(static_cast<uint8_t>(field.transform.type)));
		AppendDistributedIdentity(result, to_string(field.transform.bucket_count));
	}
	return result;
}

string GetDuckLakeDistributedSortIdentity(const DuckLakeSort *sort_data) {
	if (!sort_data) {
		return string();
	}
	string result;
	AppendDistributedIdentity(result, to_string(sort_data->sort_id));
	for (const auto &field : sort_data->fields) {
		AppendDistributedIdentity(result, to_string(field.sort_key_index));
		AppendDistributedIdentity(result, field.expression);
		AppendDistributedIdentity(result, field.dialect);
		AppendDistributedIdentity(result, to_string(static_cast<uint8_t>(field.sort_direction)));
		AppendDistributedIdentity(result, to_string(static_cast<uint8_t>(field.null_order)));
	}
	return result;
}

bool DuckLakeDistributedSnapshotsMatch(const DuckLakeSnapshot &left, const DuckLakeSnapshot &right) {
	return left.snapshot_id == right.snapshot_id && left.schema_version == right.schema_version &&
	       left.next_catalog_id == right.next_catalog_id && left.next_file_id == right.next_file_id;
}

vector<string> GetDuckLakeDistributedPartitionNames(const PhysicalCopyToFile &copy) {
	vector<string> result;
	for (const auto column_index : copy.partition_columns) {
		if (column_index >= copy.names.size()) {
			throw SerializationException("Distributed DuckLake COPY contains an invalid partition column");
		}
		result.push_back(copy.names[column_index]);
	}
	return result;
}

void ValidateDuckLakeDistributedSnapshotBaseline(ClientContext &context, const string &catalog_name,
                                                 const DuckLakeSnapshot &expected_snapshot,
                                                 const string &operation_name) {
	auto &catalog = Catalog::GetCatalog(context, catalog_name).Cast<DuckLakeCatalog>();
	auto &transaction = DuckLakeTransaction::Get(context, catalog);
	if (!DuckLakeDistributedSnapshotsMatch(transaction.GetSnapshot(), expected_snapshot)) {
		throw TransactionException("DuckLake %s snapshot changed after the distributed write was planned",
		                           operation_name);
	}
	auto latest_snapshot = transaction.GetMetadataManager().GetSnapshot();
	if (!latest_snapshot || !DuckLakeDistributedSnapshotsMatch(*latest_snapshot, expected_snapshot)) {
		throw TransactionException("DuckLake %s snapshot is stale", operation_name);
	}
}

unique_ptr<DuckLakePartition>
PlanDuckLakeDistributedCTASPartition(const ColumnList &columns, const DuckLakeFieldData &field_data,
                                     const vector<unique_ptr<ParsedExpression>> &partition_keys) {
	if (partition_keys.empty()) {
		return nullptr;
	}
	auto result = make_uniq<DuckLakePartition>();
	for (idx_t index = 0; index < partition_keys.size(); index++) {
		auto field = PlanPartitionField(columns, field_data, *partition_keys[index]);
		field.partition_key_index = index;
		result->fields.push_back(std::move(field));
	}
	return result;
}

string CreateDuckLakeDistributedArtifactPath(ClientContext &context, const string &data_path) {
	auto &file_system = FileSystem::GetFileSystem(context);
	auto canonical_data_path = CanonicalDuckLakePath(file_system, data_path, "data root");
	auto write_id = UUID::ToString(UUID::GenerateRandomUUID());
	return distributed::DistributedCopyPathInDirectory(file_system, canonical_data_path,
	                                                   string(DUCKLAKE_DISTRIBUTED_ARTIFACT_PREFIX) + write_id);
}

void ValidateDuckLakeDistributedArtifactPath(ClientContext &context, const string &data_path,
                                             const string &artifact_path) {
	auto &file_system = FileSystem::GetFileSystem(context);
	ValidateArtifactRoot(file_system, data_path, artifact_path);
}

void CleanupDuckLakeDistributedArtifactData(ClientContext &context, const string &data_path,
                                            const string &artifact_path) {
	auto &file_system = FileSystem::GetFileSystem(context);
	auto canonical_artifact_path = ValidateArtifactRoot(file_system, data_path, artifact_path);
	CleanupArtifactPaths(file_system, {canonical_artifact_path});
}

void CleanupDuckLakeDistributedArtifacts(FileSystem &file_system, const string &data_path,
                                         const string &artifact_path) {
	auto canonical_artifact_path = ValidateArtifactRoot(file_system, data_path, artifact_path);
	CleanupArtifactPaths(file_system, {canonical_artifact_path, canonical_artifact_path + ".duckdb_commit"});
}

void CleanupDuckLakeDistributedArtifacts(ClientContext &context, const string &data_path, const string &artifact_path) {
	auto &file_system = FileSystem::GetFileSystem(context);
	CleanupDuckLakeDistributedArtifacts(file_system, data_path, artifact_path);
}

static void ValidateDuckLakeDistributedDataFileArtifactsInternal(ClientContext &context,
                                                                 const string &canonical_artifact_path,
                                                                 const DuckLakeFieldData &field_data,
                                                                 const case_insensitive_set_t &not_null_fields,
                                                                 const vector<string> &partition_names,
                                                                 vector<distributed::DistributedCopyFileInfo> &files) {
	auto &file_system = FileSystem::GetFileSystem(context);
	auto expected_types = GetCopyFunctionReturnLogicalTypes(CopyFunctionReturnType::WRITTEN_FILE_STATISTICS);
	auto expected_fields = GetExpectedArtifactFields(field_data);
	idx_t total_rows = 0;
	idx_t total_bytes = 0;
	unordered_set<string> artifact_paths;
	for (auto &file : files) {
		const auto &path = file.final_path.empty() ? file.staging_path : file.final_path;
		if (path.empty()) {
			throw InvalidInputException("DuckLake distributed write returned an empty data-file artifact");
		}
		if (file.partition_keys.type() != expected_types[5]) {
			throw InvalidInputException("DuckLake distributed write returned invalid data-file partition values");
		}
		auto partition_components = ValidatePartitionValues(file.partition_keys, partition_names);
		auto canonical_path =
		    ValidateArtifactLocation(file_system, canonical_artifact_path, path, partition_names, partition_components);
		if (!artifact_paths.insert(canonical_path).second) {
			throw InvalidInputException("DuckLake distributed write returned a duplicate data-file artifact");
		}

		if (file.footer_size_bytes.IsNull() || file.footer_size_bytes.type() != expected_types[3]) {
			throw InvalidInputException("DuckLake distributed write returned invalid data-file footer statistics");
		}
		auto footer_size = UBigIntValue::Get(file.footer_size_bytes);
		if (footer_size == 0 || footer_size > NumericLimits<uint32_t>::Maximum() ||
		    footer_size > file.file_size_bytes) {
			throw InvalidInputException("DuckLake distributed write returned invalid data-file footer statistics");
		}
		if (file.column_statistics.IsNull() || file.column_statistics.type() != expected_types[4]) {
			throw InvalidInputException("DuckLake distributed write returned invalid data-file column statistics");
		}
		if (file.file_size_bytes == 0) {
			throw InvalidInputException("DuckLake distributed write returned an empty data-file artifact");
		}
		const auto signed_max = NumericCast<idx_t>(NumericLimits<int64_t>::Maximum());
		if (file.row_count > signed_max || file.file_size_bytes > signed_max ||
		    file.row_count > signed_max - total_rows || file.file_size_bytes > signed_max - total_bytes) {
			throw InvalidInputException("DuckLake distributed write statistics exceed signed 64-bit limits");
		}
		file.column_statistics =
		    ValidateArtifactContents(context, file_system, canonical_path, path, file.file_size_bytes,
		                             NumericCast<idx_t>(footer_size), file.row_count, field_data, expected_fields);
		ValidateColumnStatistics(file.column_statistics, field_data, not_null_fields, file.row_count);
		total_rows += file.row_count;
		total_bytes += file.file_size_bytes;
	}
}

static unique_ptr<DuckLakeFieldData> AddDuckLakeDistributedRowIdField(const DuckLakeFieldData &field_data) {
	auto result = make_uniq<DuckLakeFieldData>();
	for (const auto &field : field_data.GetFieldIds()) {
		result->Add(field->Copy());
	}
	DuckLakeColumnData row_id_data;
	row_id_data.id = FieldIndex(MultiFileReader::ROW_ID_FIELD_ID);
	result->Add(make_uniq<DuckLakeFieldId>(std::move(row_id_data), "_ducklake_internal_row_id", LogicalType::BIGINT));
	return result;
}

void ValidateDuckLakeDistributedDataFileArtifacts(ClientContext &context, const string &data_path,
                                                  const string &artifact_path, const DuckLakeFieldData &field_data,
                                                  const case_insensitive_set_t &not_null_fields,
                                                  const vector<string> &partition_names,
                                                  vector<distributed::DistributedCopyFileInfo> &files) {
	if (data_path.empty()) {
		throw InvalidInputException("DuckLake distributed data path cannot be empty");
	}
	auto &file_system = FileSystem::GetFileSystem(context);
	auto canonical_artifact_path = ValidateArtifactRoot(file_system, data_path, artifact_path);
	ValidateDuckLakeDistributedDataFileArtifactsInternal(context, canonical_artifact_path, field_data, not_null_fields,
	                                                     partition_names, files);
}

void ValidateDuckLakeDistributedDataFileArtifactsInRoot(ClientContext &context, const string &artifact_root,
                                                        const DuckLakeFieldData &field_data,
                                                        const case_insensitive_set_t &not_null_fields,
                                                        const vector<string> &partition_names,
                                                        vector<distributed::DistributedCopyFileInfo> &files,
                                                        bool expect_row_id) {
	if (artifact_root.empty()) {
		throw InvalidInputException("DuckLake distributed data-file artifact root cannot be empty");
	}
	auto &file_system = FileSystem::GetFileSystem(context);
	auto canonical_artifact_root = CanonicalDuckLakePath(file_system, artifact_root, "data-file artifact root");
	if (!expect_row_id) {
		ValidateDuckLakeDistributedDataFileArtifactsInternal(context, canonical_artifact_root, field_data,
		                                                     not_null_fields, partition_names, files);
		return;
	}
	auto update_fields = AddDuckLakeDistributedRowIdField(field_data);
	ValidateDuckLakeDistributedDataFileArtifactsInternal(context, canonical_artifact_root, *update_fields,
	                                                     not_null_fields, partition_names, files);
}

void RegisterDuckLakeDistributedWrites(ExtensionLoader &loader) {
	auto register_file_write = [&](const string &name) {
		DistributedWriteOperatorExtension extension;
		extension.name = name;
		extension.protocol_version = 1;
		extension.mode = DistributedWriteMode::FILE_ARTIFACT;
		extension.fragment_codec = {distributed::DISTRIBUTED_FILE_WRITE_FRAGMENT_CODEC,
		                            distributed::DISTRIBUTED_FILE_WRITE_FRAGMENT_CODEC_VERSION};
		DistributedWriteOperatorExtension::Register(loader, std::move(extension));
	};
	register_file_write("insert");
	register_file_write("ctas");

	auto register_row_delta = [&](const string &name) {
		DistributedWriteOperatorExtension extension;
		extension.name = name;
		extension.protocol_version = 1;
		extension.mode = DistributedWriteMode::CALLBACK;
		extension.fragment_codec = {"ducklake.row-delta-fragment", 1};
		extension.callbacks = DuckLakeDistributedRowDeltaCallbacks();
		DistributedWriteOperatorExtension::Register(loader, std::move(extension));
	};
	register_row_delta("delete");
	register_row_delta("update");

	DistributedWriteOperatorExtension merge;
	merge.name = "merge";
	merge.protocol_version = 1;
	merge.mode = DistributedWriteMode::CALLBACK;
	merge.fragment_codec = {"ducklake.merge-fragment", 1};
	merge.callbacks = DuckLakeDistributedMergeCallbacks();
	DistributedWriteOperatorExtension::Register(loader, std::move(merge));
}

} // namespace duckdb
