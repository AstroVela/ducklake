#include "storage/ducklake_distributed_write.hpp"

#include "common/ducklake_util.hpp"
#include "storage/ducklake_field_data.hpp"
#include "storage/ducklake_insert.hpp"
#include "storage/ducklake_partition_data.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_variant_stats.hpp"

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/hive_partitioning.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/execution/distributed/copy_finalize.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/function/distributed_write.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"

namespace duckdb {

namespace {

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

struct ValidatedArtifactLocation {
	string canonical_path;
	vector<string> components;
};

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

static void ValidateArtifactContents(FileSystem &file_system, const string &canonical_path, const string &path,
                                     idx_t expected_size, idx_t expected_footer_size) {
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

static void ValidateColumnStatistics(const Value &column_statistics, const DuckLakeFieldData &field_data) {
	case_insensitive_set_t column_paths;
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

void ValidateDuckLakeDistributedDataFileArtifacts(ClientContext &context, const string &data_path,
                                                  const DuckLakeFieldData &field_data,
                                                  const vector<string> &partition_names,
                                                  const vector<distributed::DistributedCopyFileInfo> &files,
                                                  vector<string> &cleanup_paths) {
	cleanup_paths.clear();
	if (data_path.empty()) {
		throw InvalidInputException("DuckLake distributed data path cannot be empty");
	}
	auto &file_system = FileSystem::GetFileSystem(context);
	auto expected_types = GetCopyFunctionReturnLogicalTypes(CopyFunctionReturnType::WRITTEN_FILE_STATISTICS);
	idx_t total_rows = 0;
	idx_t total_bytes = 0;
	for (const auto &file : files) {
		const auto &path = file.final_path.empty() ? file.staging_path : file.final_path;
		if (path.empty()) {
			throw InvalidInputException("DuckLake distributed write returned an empty data-file artifact");
		}
		if (file.partition_keys.type() != expected_types[5]) {
			throw InvalidInputException("DuckLake distributed write returned invalid data-file partition values");
		}
		auto partition_components = ValidatePartitionValues(file.partition_keys, partition_names);
		auto canonical_path =
		    ValidateArtifactLocation(file_system, data_path, path, partition_names, partition_components);
		cleanup_paths.push_back(canonical_path);

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
		total_rows += file.row_count;
		total_bytes += file.file_size_bytes;
		ValidateColumnStatistics(file.column_statistics, field_data);
		ValidateArtifactContents(file_system, canonical_path, path, file.file_size_bytes,
		                         NumericCast<idx_t>(footer_size));
	}
}

void CollectDuckLakeDistributedArtifactCleanupPaths(ClientContext &context, const string &data_path,
                                                    const vector<string> &partition_names,
                                                    const DistributedExtensionWriteInfo &write_info,
                                                    const vector<DistributedWriteTaskResult> &results,
                                                    vector<string> &cleanup_paths) {
	auto &file_system = FileSystem::GetFileSystem(context);
	set<string> unique_paths(cleanup_paths.begin(), cleanup_paths.end());
	for (const auto &result : results) {
		if (result.capability != write_info.capability || result.fragment_codec != write_info.fragment_codec ||
		    result.query_id.empty() || result.fragments.size() != 1) {
			continue;
		}
		const auto &fragment = result.fragments[0];
		if (fragment.artifacts.size() != 1) {
			continue;
		}
		const auto &artifact = fragment.artifacts[0];
		if (artifact.artifact_id != "data_file" || artifact.codec != DistributedPayloadCodec {"duckdb.file", 1} ||
		    !artifact.payload.empty() || artifact.uri.empty() || fragment.fragment_id != artifact.uri ||
		    result.task_attempt_id != "file:" + artifact.uri) {
			continue;
		}
		try {
			auto location = ValidateArtifactCleanupLocation(file_system, data_path, artifact.uri, partition_names);
			if (unique_paths.insert(location.canonical_path).second) {
				cleanup_paths.push_back(std::move(location.canonical_path));
			}
		} catch (...) {
		}
	}
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
}

} // namespace duckdb
