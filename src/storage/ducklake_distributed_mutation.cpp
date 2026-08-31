#include "storage/ducklake_distributed_write.hpp"

#include "common/ducklake_util.hpp"
#include "common/parquet_file_scanner.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_delete.hpp"
#include "storage/ducklake_delete_filter.hpp"
#include "storage/ducklake_deletion_vector.hpp"
#include "storage/ducklake_scan.hpp"
#include "storage/ducklake_schema_entry.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_transaction.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/set.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/execution/distributed/copy_finalize.hpp"
#include "duckdb/execution/operator/exchange/physical_repartition.hpp"
#include "duckdb/execution/operator/persistent/physical_copy_to_file.hpp"
#include "duckdb/execution/physical_operator_states.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/interrupt.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parser/parsed_data/copy_info.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

namespace {

static constexpr uint32_t DUCKLAKE_ROW_DELTA_PROTOCOL_VERSION = 1;
static const string DUCKLAKE_ROW_DELTA_FRAGMENT_CODEC = "ducklake.row-delta-fragment";
static const DistributedPayloadCodec DUCKLAKE_DATA_FILE_CODEC {"ducklake.data-file", 1};
static const DistributedPayloadCodec DUCKLAKE_POSITION_DELETE_FILE_CODEC {"ducklake.position-delete-file", 1};
static const DistributedPayloadCodec DUCKLAKE_DELETION_VECTOR_FILE_CODEC {"ducklake.deletion-vector", 1};

struct DuckLakeDistributedRowDeltaSourceState {
	string scan_file_path;
	idx_t data_file_id = 0;
	idx_t data_file_size_bytes = 0;
	optional_idx data_file_footer_size;
	idx_t record_count = 0;
	optional_idx row_id_start;
	optional_idx mapping_id;
	bool has_delete_file = false;
	idx_t delete_file_id = 0;
	string delete_file_path;
	idx_t delete_file_size_bytes = 0;
	optional_idx delete_file_footer_size;
	DeleteFileFormat delete_file_format = DeleteFileFormat::PARQUET;
	optional_idx delete_file_begin_snapshot;
	vector<idx_t> existing_delete_rows;
	vector<idx_t> existing_delete_snapshots;
};

struct DuckLakeDistributedRowDeltaBind {
	DuckLakeDistributedRowDeltaKind kind = DuckLakeDistributedRowDeltaKind::DELETE;
	bool source_is_statically_empty = false;
	bool use_deletion_vectors = false;
	string catalog_name;
	string schema_name;
	string table_name;
	string schema_uuid;
	string table_uuid;
	idx_t schema_id = DConstants::INVALID_INDEX;
	idx_t table_id = DConstants::INVALID_INDEX;
	DuckLakeSnapshot snapshot;
	string field_identity;
	string partition_identity;
	string sort_identity;
	string data_path;
	string artifact_path;
	vector<idx_t> row_id_indexes;
	idx_t copy_column_count = 0;
	string copy_operator;
	idx_t new_delete_snapshot = 0;
	vector<DuckLakeDistributedRowDeltaSourceState> delete_sources;
};

static idx_t CheckedAdd(idx_t left, idx_t right, const string &description) {
	if (right > NumericLimits<idx_t>::Maximum() - left) {
		throw InvalidInputException("DuckLake distributed row mutation %s overflow", description);
	}
	return left + right;
}

static bool IsStrictUUID(const string &value) {
	hugeint_t parsed;
	return BaseUUID::FromString(value, parsed, true);
}

static void ValidateSnapshot(const DuckLakeSnapshot &snapshot) {
	if (snapshot.snapshot_id == DConstants::INVALID_INDEX || snapshot.schema_version == DConstants::INVALID_INDEX ||
	    snapshot.next_catalog_id == DConstants::INVALID_INDEX || snapshot.next_file_id == DConstants::INVALID_INDEX) {
		throw SerializationException("DuckLake distributed row mutation has an incomplete snapshot identity");
	}
}

static void ValidateBindIdentity(const DuckLakeDistributedRowDeltaBind &bind) {
	if (bind.catalog_name.empty() || bind.schema_name.empty() || bind.table_name.empty() ||
	    !IsStrictUUID(bind.schema_uuid) || !IsStrictUUID(bind.table_uuid) || bind.field_identity.empty() ||
	    bind.schema_id == DConstants::INVALID_INDEX || bind.table_id == DConstants::INVALID_INDEX ||
	    bind.schema_id >= DuckLakeConstants::TRANSACTION_LOCAL_ID_START ||
	    bind.table_id >= DuckLakeConstants::TRANSACTION_LOCAL_ID_START) {
		throw SerializationException("DuckLake distributed row mutation has an incomplete target identity");
	}
	ValidateSnapshot(bind.snapshot);
	if (bind.snapshot.snapshot_id == NumericLimits<idx_t>::Maximum() ||
	    bind.new_delete_snapshot != bind.snapshot.snapshot_id + 1) {
		throw SerializationException("DuckLake distributed row mutation has an invalid target snapshot range");
	}
}

static string BytesFromStream(MemoryStream &stream) {
	return string(reinterpret_cast<const char *>(stream.GetData()), stream.GetPosition());
}

static MemoryStream StreamFromBytes(const string &bytes) {
	MemoryStream stream(Allocator::DefaultAllocator(), bytes.size());
	stream.WriteData(reinterpret_cast<const_data_ptr_t>(bytes.data()), bytes.size());
	stream.Rewind();
	return stream;
}

static void WriteOptionalIndex(Serializer &serializer, field_id_t has_field_id, field_id_t value_field_id,
                               const string &name, const optional_idx &value) {
	auto has_name = "has_" + name;
	serializer.WriteProperty(has_field_id, has_name.c_str(), value.IsValid());
	serializer.WriteProperty(value_field_id, name.c_str(), value.IsValid() ? value.GetIndex() : 0);
}

static optional_idx ReadOptionalIndex(Deserializer &deserializer, field_id_t has_field_id, field_id_t value_field_id,
                                      const string &name) {
	auto has_name = "has_" + name;
	auto has_value = deserializer.ReadProperty<bool>(has_field_id, has_name.c_str());
	auto value = deserializer.ReadProperty<idx_t>(value_field_id, name.c_str());
	if (has_value) {
		if (value == DConstants::INVALID_INDEX) {
			throw SerializationException("DuckLake distributed row mutation has a non-canonical optional %s", name);
		}
		return optional_idx(value);
	}
	if (value != 0) {
		throw SerializationException("DuckLake distributed row mutation has a non-canonical optional %s", name);
	}
	return optional_idx();
}

static string EncodePathComponent(const string &input) {
	static const char *hex = "0123456789abcdef";
	string result;
	result.reserve(input.size() * 2);
	for (auto character : input) {
		auto byte = static_cast<uint8_t>(character);
		result.push_back(hex[byte >> 4]);
		result.push_back(hex[byte & 0x0f]);
	}
	return result;
}

static string CanonicalDuckLakeMutationPath(FileSystem &file_system, const string &path, const string &description) {
	auto canonical = distributed::CanonicalDistributedCopyBasePath(file_system, path);
	if (canonical.is_err()) {
		throw InvalidInputException("Invalid DuckLake distributed %s path: %s", description, canonical.error().what());
	}
	return std::move(canonical).value();
}

static string DuckLakeDistributedAttemptRoot(ClientContext &context, const string &artifact_path,
                                             const string &task_attempt_id) {
	auto &file_system = FileSystem::GetFileSystem(context);
	return file_system.JoinPath(artifact_path, EncodePathComponent(task_attempt_id));
}

static void ValidateFileSize(ClientContext &context, const string &path, idx_t expected_size,
                             const string &description) {
	auto &file_system = FileSystem::GetFileSystem(context);
	try {
		auto handle = file_system.OpenFile(path, FileOpenFlags::FILE_FLAGS_READ);
		auto actual_size = handle->GetFileSize();
		if (actual_size != expected_size) {
			throw InvalidInputException(
			    "DuckLake distributed %s size mismatch for '%s' (metadata reports %s bytes, found %s)", description,
			    path, to_string(expected_size), to_string(actual_size));
		}
	} catch (const InvalidInputException &) {
		throw;
	} catch (const std::exception &error) {
		throw IOException("Failed to validate DuckLake distributed %s '%s': %s", description, path, error.what());
	}
}

static void ValidateSourceEntry(const DuckLakeFileListExtendedEntry &file, const string &operation_name) {
	if (file.data_type != DuckLakeDataType::DATA_FILE || !file.file_id.IsValid() || file.file.path.empty() ||
	    file.file.file_size_bytes == 0 || file.row_count == 0) {
		throw NotImplementedException("Distributed DuckLake %s supports committed non-empty data files only",
		                              operation_name);
	}
	if (!file.file.encryption_key.empty() || !file.delete_file.encryption_key.empty()) {
		throw NotImplementedException("Distributed DuckLake %s does not support encrypted files", operation_name);
	}
	if (file.delete_file.path.empty() != !file.delete_file_id.IsValid()) {
		throw InvalidConfigurationException("DuckLake distributed %s source has inconsistent delete-file metadata",
		                                    operation_name);
	}
	if (!file.delete_file.path.empty() && file.delete_file.file_size_bytes == 0) {
		throw InvalidConfigurationException("DuckLake distributed %s source has an empty delete-file artifact",
		                                    operation_name);
	}
}

static DuckLakeDistributedRowDeltaSourceState
BuildSourceState(ClientContext &context, const DuckLakeFileListExtendedEntry &file, const string &operation_name) {
	ValidateSourceEntry(file, operation_name);

	DuckLakeDistributedRowDeltaSourceState result;
	result.scan_file_path = file.file.path;
	result.data_file_id = file.file_id.index;
	result.data_file_size_bytes = file.file.file_size_bytes;
	result.data_file_footer_size = file.file.footer_size;
	result.record_count = file.row_count;
	result.row_id_start = file.row_id_start;
	if (file.mapping_id.IsValid()) {
		result.mapping_id = file.mapping_id.index;
	}
	ValidateFileSize(context, file.file.path, file.file.file_size_bytes, "source data file");

	if (file.delete_file.path.empty()) {
		return result;
	}
	result.has_delete_file = true;
	result.delete_file_id = file.delete_file_id.index;
	result.delete_file_path = file.delete_file.path;
	result.delete_file_size_bytes = file.delete_file.file_size_bytes;
	result.delete_file_footer_size = file.delete_file.footer_size;
	result.delete_file_format = file.delete_file.format;
	result.delete_file_begin_snapshot = file.delete_file_begin_snapshot;
	ValidateFileSize(context, file.delete_file.path, file.delete_file.file_size_bytes, "source delete file");

	auto scan_result = DuckLakeDeleteFilter::ScanDeleteFile(context, file.delete_file);
	if (scan_result.deleted_rows.empty() || scan_result.deleted_rows.size() > file.row_count) {
		throw InvalidConfigurationException("DuckLake distributed %s source delete file has an invalid row count",
		                                    operation_name);
	}
	result.existing_delete_rows = std::move(scan_result.deleted_rows);
	if (scan_result.has_embedded_snapshots) {
		if (scan_result.snapshot_ids.size() != result.existing_delete_rows.size()) {
			throw InvalidConfigurationException(
			    "DuckLake distributed %s source delete file has incomplete snapshot metadata", operation_name);
		}
		result.existing_delete_snapshots = std::move(scan_result.snapshot_ids);
	} else {
		if (!file.delete_file_begin_snapshot.IsValid()) {
			throw InvalidConfigurationException(
			    "DuckLake distributed %s source delete file is missing its begin snapshot", operation_name);
		}
		result.existing_delete_snapshots.assign(result.existing_delete_rows.size(),
		                                        file.delete_file_begin_snapshot.GetIndex());
	}
	for (idx_t index = 0; index < result.existing_delete_rows.size(); index++) {
		if (result.existing_delete_rows[index] >= result.record_count || result.existing_delete_snapshots[index] == 0) {
			throw InvalidConfigurationException(
			    "DuckLake distributed %s source delete file contains invalid row metadata", operation_name);
		}
	}
	return result;
}

static vector<DuckLakeDistributedRowDeltaSourceState>
BuildSourceStates(ClientContext &context, const vector<DuckLakeFileListExtendedEntry> &source_files,
                  const string &operation_name) {
	vector<DuckLakeDistributedRowDeltaSourceState> result;
	result.reserve(source_files.size());
	unordered_set<string> paths;
	unordered_set<idx_t> file_ids;
	for (const auto &file : source_files) {
		auto source = BuildSourceState(context, file, operation_name);
		if (!paths.insert(source.scan_file_path).second || !file_ids.insert(source.data_file_id).second) {
			throw InvalidConfigurationException("DuckLake distributed %s source contains duplicate data files",
			                                    operation_name);
		}
		result.push_back(std::move(source));
	}
	return result;
}

static void ValidateDistributedRowDeltaCopyShape(const PhysicalCopyToFile &copy) {
	auto partitioned = copy.partition_output && copy.write_empty_file && !copy.rotate && !copy.per_thread_output;
	auto rotating = !copy.partition_output && !copy.write_empty_file && copy.rotate && !copy.per_thread_output &&
	                copy.file_size_bytes.IsValid();
	if (!partitioned && !rotating) {
		throw NotImplementedException(
		    "Distributed DuckLake row mutations require the canonical partitioned or rotating COPY writer");
	}
	if (copy.use_tmp_file) {
		throw NotImplementedException("Distributed DuckLake row mutations do not support temporary COPY output");
	}
	if (copy.partition_output && (!copy.hive_file_pattern || copy.partition_columns.empty())) {
		throw NotImplementedException("Distributed DuckLake row mutations require Hive partition paths");
	}
	auto statistics_types = GetCopyFunctionReturnLogicalTypes(CopyFunctionReturnType::WRITTEN_FILE_STATISTICS);
	if (copy.return_type != CopyFunctionReturnType::WRITTEN_FILE_STATISTICS || copy.types != statistics_types) {
		throw SerializationException("Distributed DuckLake row mutation COPY must return written-file statistics");
	}
	if (copy.names.size() != copy.expected_types.size()) {
		throw SerializationException("Distributed DuckLake row mutation COPY names and types have different widths");
	}
}

static string SerializeShallowCopy(const PhysicalCopyToFile &copy) {
	MemoryStream stream(Allocator::DefaultAllocator());
	BinarySerializer serializer(stream);
	serializer.Begin();
	serializer.WriteProperty(100, "type", copy.type);
	serializer.WriteProperty(101, "types", copy.types);
	serializer.WriteProperty(102, "estimated_cardinality", copy.estimated_cardinality);
	copy.SerializeOperatorData(serializer);
	serializer.WriteList(198, "children", 0, [&](Serializer::List &, idx_t) {});
	serializer.End();
	return BytesFromStream(stream);
}

static void SerializeSource(Serializer &serializer, const DuckLakeDistributedRowDeltaSourceState &source) {
	serializer.WriteProperty(1, "scan_file_path", source.scan_file_path);
	serializer.WriteProperty(2, "data_file_id", source.data_file_id);
	serializer.WriteProperty(3, "data_file_size_bytes", source.data_file_size_bytes);
	WriteOptionalIndex(serializer, 4, 5, "data_file_footer_size", source.data_file_footer_size);
	serializer.WriteProperty(6, "record_count", source.record_count);
	WriteOptionalIndex(serializer, 7, 8, "row_id_start", source.row_id_start);
	WriteOptionalIndex(serializer, 9, 10, "mapping_id", source.mapping_id);
	serializer.WriteProperty(11, "has_delete_file", source.has_delete_file);
	serializer.WriteProperty(12, "delete_file_id", source.delete_file_id);
	serializer.WriteProperty(13, "delete_file_path", source.delete_file_path);
	serializer.WriteProperty(14, "delete_file_size_bytes", source.delete_file_size_bytes);
	WriteOptionalIndex(serializer, 15, 16, "delete_file_footer_size", source.delete_file_footer_size);
	serializer.WriteProperty(17, "delete_file_format", static_cast<uint8_t>(source.delete_file_format));
	WriteOptionalIndex(serializer, 18, 19, "delete_file_begin_snapshot", source.delete_file_begin_snapshot);
	serializer.WriteProperty(20, "existing_delete_rows", source.existing_delete_rows);
	serializer.WriteProperty(21, "existing_delete_snapshots", source.existing_delete_snapshots);
}

static DuckLakeDistributedRowDeltaSourceState DeserializeSource(Deserializer &deserializer) {
	DuckLakeDistributedRowDeltaSourceState result;
	result.scan_file_path = deserializer.ReadProperty<string>(1, "scan_file_path");
	result.data_file_id = deserializer.ReadProperty<idx_t>(2, "data_file_id");
	result.data_file_size_bytes = deserializer.ReadProperty<idx_t>(3, "data_file_size_bytes");
	result.data_file_footer_size = ReadOptionalIndex(deserializer, 4, 5, "data_file_footer_size");
	result.record_count = deserializer.ReadProperty<idx_t>(6, "record_count");
	result.row_id_start = ReadOptionalIndex(deserializer, 7, 8, "row_id_start");
	result.mapping_id = ReadOptionalIndex(deserializer, 9, 10, "mapping_id");
	result.has_delete_file = deserializer.ReadProperty<bool>(11, "has_delete_file");
	result.delete_file_id = deserializer.ReadProperty<idx_t>(12, "delete_file_id");
	result.delete_file_path = deserializer.ReadProperty<string>(13, "delete_file_path");
	result.delete_file_size_bytes = deserializer.ReadProperty<idx_t>(14, "delete_file_size_bytes");
	result.delete_file_footer_size = ReadOptionalIndex(deserializer, 15, 16, "delete_file_footer_size");
	auto format = deserializer.ReadProperty<uint8_t>(17, "delete_file_format");
	if (format > static_cast<uint8_t>(DeleteFileFormat::PUFFIN)) {
		throw SerializationException("DuckLake distributed row mutation source has an invalid delete format");
	}
	result.delete_file_format = static_cast<DeleteFileFormat>(format);
	result.delete_file_begin_snapshot = ReadOptionalIndex(deserializer, 18, 19, "delete_file_begin_snapshot");
	result.existing_delete_rows = deserializer.ReadProperty<vector<idx_t>>(20, "existing_delete_rows");
	result.existing_delete_snapshots = deserializer.ReadProperty<vector<idx_t>>(21, "existing_delete_snapshots");
	return result;
}

static string SerializeBind(const DuckLakeDistributedRowDeltaBind &bind) {
	ValidateBindIdentity(bind);
	MemoryStream stream(Allocator::DefaultAllocator());
	BinarySerializer serializer(stream);
	serializer.Begin();
	serializer.WriteProperty(1, "kind", static_cast<uint8_t>(bind.kind));
	serializer.WriteProperty(2, "source_is_statically_empty", bind.source_is_statically_empty);
	serializer.WriteProperty(3, "use_deletion_vectors", bind.use_deletion_vectors);
	serializer.WriteProperty(4, "data_path", bind.data_path);
	serializer.WriteProperty(5, "artifact_path", bind.artifact_path);
	serializer.WriteProperty(6, "row_id_indexes", bind.row_id_indexes);
	serializer.WriteProperty(7, "copy_column_count", bind.copy_column_count);
	serializer.WriteProperty(8, "copy_operator", bind.copy_operator);
	serializer.WriteProperty(9, "new_delete_snapshot", bind.new_delete_snapshot);
	serializer.WriteList(10, "delete_sources", bind.delete_sources.size(), [&](Serializer::List &list, idx_t index) {
		list.WriteObject([&](Serializer &object) { SerializeSource(object, bind.delete_sources[index]); });
	});
	serializer.WriteProperty(11, "catalog_name", bind.catalog_name);
	serializer.WriteProperty(12, "schema_name", bind.schema_name);
	serializer.WriteProperty(13, "table_name", bind.table_name);
	serializer.WriteProperty(14, "schema_uuid", bind.schema_uuid);
	serializer.WriteProperty(15, "table_uuid", bind.table_uuid);
	serializer.WriteProperty(16, "schema_id", bind.schema_id);
	serializer.WriteProperty(17, "table_id", bind.table_id);
	serializer.WriteObject(18, "snapshot", [&](Serializer &object) { bind.snapshot.Serialize(object); });
	serializer.WriteProperty(19, "field_identity", bind.field_identity);
	serializer.WriteProperty(20, "partition_identity", bind.partition_identity);
	serializer.WriteProperty(21, "sort_identity", bind.sort_identity);
	serializer.End();
	return BytesFromStream(stream);
}

static void ValidateSourceState(const DuckLakeDistributedRowDeltaSourceState &source) {
	if (source.scan_file_path.empty() || source.data_file_size_bytes == 0 || source.record_count == 0 ||
	    source.data_file_id >= DuckLakeConstants::TRANSACTION_LOCAL_ID_START ||
	    source.existing_delete_rows.size() != source.existing_delete_snapshots.size()) {
		throw SerializationException("DuckLake distributed row mutation contains invalid source-file state");
	}
	if (source.data_file_footer_size.IsValid() &&
	    source.data_file_footer_size.GetIndex() > source.data_file_size_bytes) {
		throw SerializationException("DuckLake distributed row mutation contains invalid source-file footer state");
	}
	if (source.mapping_id.IsValid() && source.mapping_id.GetIndex() >= DuckLakeConstants::TRANSACTION_LOCAL_ID_START) {
		throw SerializationException("DuckLake distributed row mutation contains invalid source-file mapping state");
	}
	if (source.has_delete_file) {
		if (source.delete_file_id >= DuckLakeConstants::TRANSACTION_LOCAL_ID_START || source.delete_file_path.empty() ||
		    source.delete_file_size_bytes == 0 || !source.delete_file_begin_snapshot.IsValid()) {
			throw SerializationException("DuckLake distributed row mutation contains invalid delete-file state");
		}
		if (source.delete_file_footer_size.IsValid() &&
		    source.delete_file_footer_size.GetIndex() > source.delete_file_size_bytes) {
			throw SerializationException("DuckLake distributed row mutation contains invalid delete-file footer state");
		}
	} else if (source.delete_file_id != 0 || !source.delete_file_path.empty() || source.delete_file_size_bytes != 0 ||
	           source.delete_file_footer_size.IsValid() || source.delete_file_begin_snapshot.IsValid() ||
	           !source.existing_delete_rows.empty()) {
		throw SerializationException("DuckLake distributed row mutation has non-canonical empty delete-file state");
	}
	idx_t previous_row = 0;
	for (idx_t index = 0; index < source.existing_delete_rows.size(); index++) {
		auto row = source.existing_delete_rows[index];
		if (row >= source.record_count || source.existing_delete_snapshots[index] == 0 ||
		    (index != 0 && row <= previous_row)) {
			throw SerializationException("DuckLake distributed row mutation contains invalid existing deletes");
		}
		previous_row = row;
	}
}

static DuckLakeDistributedRowDeltaBind DeserializeBind(const string &bytes) {
	if (bytes.empty()) {
		throw SerializationException("DuckLake distributed row mutation bind data is empty");
	}
	auto stream = StreamFromBytes(bytes);
	BinaryDeserializer deserializer(stream);
	deserializer.Begin();
	auto kind = deserializer.ReadProperty<uint8_t>(1, "kind");
	if (kind > static_cast<uint8_t>(DuckLakeDistributedRowDeltaKind::MERGE_INSERT)) {
		throw SerializationException("DuckLake distributed row mutation has an invalid operation kind");
	}
	DuckLakeDistributedRowDeltaBind result;
	result.kind = static_cast<DuckLakeDistributedRowDeltaKind>(kind);
	result.source_is_statically_empty = deserializer.ReadProperty<bool>(2, "source_is_statically_empty");
	result.use_deletion_vectors = deserializer.ReadProperty<bool>(3, "use_deletion_vectors");
	result.data_path = deserializer.ReadProperty<string>(4, "data_path");
	result.artifact_path = deserializer.ReadProperty<string>(5, "artifact_path");
	result.row_id_indexes = deserializer.ReadProperty<vector<idx_t>>(6, "row_id_indexes");
	result.copy_column_count = deserializer.ReadProperty<idx_t>(7, "copy_column_count");
	result.copy_operator = deserializer.ReadProperty<string>(8, "copy_operator");
	result.new_delete_snapshot = deserializer.ReadProperty<idx_t>(9, "new_delete_snapshot");
	deserializer.ReadList(10, "delete_sources", [&](Deserializer::List &list, idx_t) {
		list.ReadObject([&](Deserializer &object) { result.delete_sources.push_back(DeserializeSource(object)); });
	});
	result.catalog_name = deserializer.ReadProperty<string>(11, "catalog_name");
	result.schema_name = deserializer.ReadProperty<string>(12, "schema_name");
	result.table_name = deserializer.ReadProperty<string>(13, "table_name");
	result.schema_uuid = deserializer.ReadProperty<string>(14, "schema_uuid");
	result.table_uuid = deserializer.ReadProperty<string>(15, "table_uuid");
	result.schema_id = deserializer.ReadProperty<idx_t>(16, "schema_id");
	result.table_id = deserializer.ReadProperty<idx_t>(17, "table_id");
	deserializer.ReadObject(18, "snapshot",
	                        [&](Deserializer &object) { result.snapshot = DuckLakeSnapshot::Deserialize(object); });
	result.field_identity = deserializer.ReadProperty<string>(19, "field_identity");
	result.partition_identity = deserializer.ReadProperty<string>(20, "partition_identity");
	result.sort_identity = deserializer.ReadProperty<string>(21, "sort_identity");
	deserializer.End();

	ValidateBindIdentity(result);
	if (result.data_path.empty() || result.artifact_path.empty() || result.new_delete_snapshot == 0) {
		throw SerializationException("DuckLake distributed row mutation bind data is invalid");
	}
	if (result.kind == DuckLakeDistributedRowDeltaKind::DELETE) {
		if (result.row_id_indexes.size() != 2 || !result.copy_operator.empty() || result.copy_column_count != 0) {
			throw SerializationException("DuckLake distributed DELETE bind has an invalid worker shape");
		}
	} else if (result.copy_operator.empty() || result.copy_column_count == 0) {
		throw SerializationException("DuckLake distributed row mutation bind is missing its COPY writer");
	} else if (result.kind == DuckLakeDistributedRowDeltaKind::UPDATE && result.row_id_indexes.size() != 2) {
		throw SerializationException("DuckLake distributed UPDATE bind has invalid row identifiers");
	} else if (result.kind == DuckLakeDistributedRowDeltaKind::MERGE_INSERT && !result.row_id_indexes.empty()) {
		throw SerializationException("DuckLake distributed MERGE INSERT bind has unexpected row identifiers");
	}
	if (result.kind == DuckLakeDistributedRowDeltaKind::MERGE_INSERT &&
	    (result.source_is_statically_empty || !result.delete_sources.empty())) {
		throw SerializationException("DuckLake distributed MERGE INSERT bind has invalid source state");
	}
	if (result.kind != DuckLakeDistributedRowDeltaKind::MERGE_INSERT && result.source_is_statically_empty &&
	    !result.delete_sources.empty()) {
		throw SerializationException("DuckLake distributed row mutation has invalid statically-empty source state");
	}
	if (result.kind != DuckLakeDistributedRowDeltaKind::MERGE_INSERT && !result.source_is_statically_empty &&
	    result.delete_sources.empty()) {
		throw SerializationException("DuckLake distributed row mutation is missing its planned source files");
	}
	unordered_set<string> paths;
	unordered_set<idx_t> file_ids;
	for (const auto &source : result.delete_sources) {
		ValidateSourceState(source);
		if (!paths.insert(source.scan_file_path).second || !file_ids.insert(source.data_file_id).second) {
			throw SerializationException("DuckLake distributed row mutation contains duplicate source-file state");
		}
	}
	return result;
}

static unique_ptr<PhysicalOperator> DeserializeShallowCopy(ClientContext &context, PhysicalPlan &physical_plan,
                                                           const string &bytes) {
	if (bytes.empty()) {
		throw SerializationException("DuckLake distributed row mutation COPY operator is empty");
	}
	auto stream = StreamFromBytes(bytes);
	BinaryDeserializer deserializer(stream);
	deserializer.Set<ClientContext &>(context);
	deserializer.Begin();
	auto result = PhysicalOperator::Deserialize(deserializer, physical_plan);
	deserializer.End();
	if (result->type != PhysicalOperatorType::COPY_TO_FILE || !result->children.empty()) {
		throw SerializationException("DuckLake distributed row mutation worker bind is not a shallow COPY operator");
	}
	ValidateDistributedRowDeltaCopyShape(result->Cast<PhysicalCopyToFile>());
	return result;
}

static void SerializeCopyFile(Serializer &serializer, const distributed::DistributedCopyFileInfo &file) {
	serializer.WriteProperty(1, "staging_path", file.staging_path);
	serializer.WriteProperty(2, "final_path", file.final_path);
	serializer.WriteProperty(3, "row_count", file.row_count);
	serializer.WriteProperty(4, "file_size_bytes", file.file_size_bytes);
	serializer.WriteProperty(5, "footer_size_bytes", file.footer_size_bytes);
	serializer.WriteProperty(6, "column_statistics", file.column_statistics);
	serializer.WriteProperty(7, "partition_keys", file.partition_keys);
}

static distributed::DistributedCopyFileInfo DeserializeCopyFile(Deserializer &deserializer) {
	distributed::DistributedCopyFileInfo result;
	result.staging_path = deserializer.ReadProperty<string>(1, "staging_path");
	result.final_path = deserializer.ReadProperty<string>(2, "final_path");
	result.row_count = deserializer.ReadProperty<idx_t>(3, "row_count");
	result.file_size_bytes = deserializer.ReadProperty<idx_t>(4, "file_size_bytes");
	result.footer_size_bytes = deserializer.ReadProperty<Value>(5, "footer_size_bytes");
	result.column_statistics = deserializer.ReadProperty<Value>(6, "column_statistics");
	result.partition_keys = deserializer.ReadProperty<Value>(7, "partition_keys");
	return result;
}

static void SerializeDeleteFile(Serializer &serializer, const DuckLakeDistributedDeleteFileResult &file) {
	serializer.WriteProperty(1, "data_file_path", file.data_file_path);
	serializer.WriteProperty(2, "delete_file_path", file.delete_file_path);
	serializer.WriteProperty(3, "format", static_cast<uint8_t>(file.format));
	serializer.WriteProperty(4, "new_delete_count", file.new_delete_count);
	serializer.WriteProperty(5, "delete_count", file.delete_count);
	serializer.WriteProperty(6, "file_size_bytes", file.file_size_bytes);
	serializer.WriteProperty(7, "footer_size_bytes", file.footer_size_bytes);
	serializer.WriteProperty(8, "pos_min_value", file.pos_min_value);
	serializer.WriteProperty(9, "pos_max_value", file.pos_max_value);
	WriteOptionalIndex(serializer, 10, 11, "begin_snapshot", file.begin_snapshot);
	WriteOptionalIndex(serializer, 12, 13, "max_snapshot", file.max_snapshot);
}

static DuckLakeDistributedDeleteFileResult DeserializeDeleteFile(Deserializer &deserializer) {
	DuckLakeDistributedDeleteFileResult result;
	result.data_file_path = deserializer.ReadProperty<string>(1, "data_file_path");
	result.delete_file_path = deserializer.ReadProperty<string>(2, "delete_file_path");
	auto format = deserializer.ReadProperty<uint8_t>(3, "format");
	if (format > static_cast<uint8_t>(DeleteFileFormat::PUFFIN)) {
		throw SerializationException("DuckLake distributed row mutation returned an invalid delete format");
	}
	result.format = static_cast<DeleteFileFormat>(format);
	result.new_delete_count = deserializer.ReadProperty<idx_t>(4, "new_delete_count");
	result.delete_count = deserializer.ReadProperty<idx_t>(5, "delete_count");
	result.file_size_bytes = deserializer.ReadProperty<idx_t>(6, "file_size_bytes");
	result.footer_size_bytes = deserializer.ReadProperty<idx_t>(7, "footer_size_bytes");
	result.pos_min_value = deserializer.ReadProperty<idx_t>(8, "pos_min_value");
	result.pos_max_value = deserializer.ReadProperty<idx_t>(9, "pos_max_value");
	result.begin_snapshot = ReadOptionalIndex(deserializer, 10, 11, "begin_snapshot");
	result.max_snapshot = ReadOptionalIndex(deserializer, 12, 13, "max_snapshot");
	return result;
}

static string SerializeFragmentPayload(DuckLakeDistributedRowDeltaKind kind,
                                       const vector<distributed::DistributedCopyFileInfo> &data_files,
                                       const vector<DuckLakeDistributedDeleteFileResult> &delete_files) {
	MemoryStream stream(Allocator::DefaultAllocator());
	BinarySerializer serializer(stream);
	serializer.Begin();
	serializer.WriteProperty(1, "kind", static_cast<uint8_t>(kind));
	serializer.WriteList(2, "data_files", data_files.size(), [&](Serializer::List &list, idx_t index) {
		list.WriteObject([&](Serializer &object) { SerializeCopyFile(object, data_files[index]); });
	});
	serializer.WriteList(3, "delete_files", delete_files.size(), [&](Serializer::List &list, idx_t index) {
		list.WriteObject([&](Serializer &object) { SerializeDeleteFile(object, delete_files[index]); });
	});
	serializer.End();
	return BytesFromStream(stream);
}

static DuckLakeDistributedRowDeltaResult DeserializeFragmentPayload(const string &bytes,
                                                                    DuckLakeDistributedRowDeltaKind expected_kind) {
	if (bytes.empty()) {
		throw SerializationException("DuckLake distributed row mutation fragment is empty");
	}
	auto stream = StreamFromBytes(bytes);
	BinaryDeserializer deserializer(stream);
	deserializer.Begin();
	auto kind = deserializer.ReadProperty<uint8_t>(1, "kind");
	if (kind != static_cast<uint8_t>(expected_kind)) {
		throw SerializationException("DuckLake distributed row mutation fragment has the wrong operation kind");
	}
	DuckLakeDistributedRowDeltaResult result;
	deserializer.ReadList(2, "data_files", [&](Deserializer::List &list, idx_t) {
		list.ReadObject([&](Deserializer &object) { result.data_files.push_back(DeserializeCopyFile(object)); });
	});
	deserializer.ReadList(3, "delete_files", [&](Deserializer::List &list, idx_t) {
		list.ReadObject([&](Deserializer &object) { result.delete_files.push_back(DeserializeDeleteFile(object)); });
	});
	deserializer.End();
	return result;
}

class DuckLakeDistributedRowDeltaGlobalState final : public DistributedWriteGlobalState {
public:
	DuckLakeDistributedRowDeltaGlobalState(ClientContext &context, DuckLakeDistributedRowDeltaBind bind_p,
	                                       const DistributedWriteTaskContext &task)
	    : bind(std::move(bind_p)), copy_plan(Allocator::Get(context)) {
		attempt_root = DuckLakeDistributedAttemptRoot(context, bind.artifact_path, task.task_attempt_id);
		if (bind.kind != DuckLakeDistributedRowDeltaKind::DELETE) {
			copy_holder = DeserializeShallowCopy(context, copy_plan, bind.copy_operator);
			copy = &copy_holder->Cast<PhysicalCopyToFile>();
			copy->file_path = attempt_root;
			if (copy->expected_types.size() != bind.copy_column_count) {
				throw SerializationException("DuckLake distributed COPY input width changed during transport");
			}
		}
		auto &file_system = FileSystem::GetFileSystem(context);
		if (!file_system.IsRemoteFile(attempt_root)) {
			file_system.CreateDirectoriesRecursive(attempt_root);
		}
		for (idx_t index = 0; index < bind.delete_sources.size(); index++) {
			if (!delete_source_indexes.emplace(bind.delete_sources[index].scan_file_path, index).second) {
				throw SerializationException("DuckLake distributed row mutation contains duplicate source-file state");
			}
		}
	}

	DuckLakeDistributedRowDeltaBind bind;
	PhysicalPlan copy_plan;
	unique_ptr<PhysicalOperator> copy_holder;
	optional_ptr<PhysicalCopyToFile> copy;
	string attempt_root;
	unordered_map<string, idx_t> delete_source_indexes;
	mutex copy_lock;
	bool copy_sink_initialized = false;
	mutex seen_rows_lock;
	unordered_map<string, unordered_set<idx_t>> seen_rows;
	mutex lock;
	unordered_map<string, vector<idx_t>> deleted_rows;
	idx_t affected_rows = 0;
};

class DuckLakeDistributedRowDeltaLocalState final : public DistributedWriteLocalState {
public:
	unique_ptr<LocalSinkState> copy_state;
	unordered_map<string, vector<idx_t>> deleted_rows;
	idx_t affected_rows = 0;
};

static void SinkRowDeltaCopy(ExecutionContext &context, DuckLakeDistributedRowDeltaGlobalState &global_state,
                             DuckLakeDistributedRowDeltaLocalState &local_state, DataChunk &input,
                             bool has_row_identifiers) {
	if (!global_state.copy) {
		throw InternalException("DuckLake distributed row mutation COPY writer is missing");
	}
	auto extra_column_count = has_row_identifiers ? 2 : 0;
	if (global_state.bind.copy_column_count > input.ColumnCount() ||
	    input.ColumnCount() - global_state.bind.copy_column_count != extra_column_count) {
		throw InvalidInputException("DuckLake distributed COPY input does not match its worker column contract");
	}
	if (has_row_identifiers && (global_state.bind.row_id_indexes[0] != global_state.bind.copy_column_count ||
	                            global_state.bind.row_id_indexes[1] != global_state.bind.copy_column_count + 1)) {
		throw InvalidInputException("DuckLake distributed UPDATE row identifiers have invalid positions");
	}
	for (idx_t index = 0; index < global_state.bind.copy_column_count; index++) {
		if (input.data[index].GetType() != global_state.copy->expected_types[index]) {
			throw InvalidInputException("DuckLake distributed COPY input does not match its column types");
		}
	}
	{
		lock_guard<mutex> guard(global_state.copy_lock);
		if (!global_state.copy_sink_initialized) {
			global_state.copy->sink_state = global_state.copy->GetGlobalSinkState(context.client);
			global_state.copy_sink_initialized = true;
		}
	}
	if (!local_state.copy_state) {
		local_state.copy_state = global_state.copy->GetLocalSinkState(context);
	}
	DataChunk copy_chunk;
	copy_chunk.InitializeEmpty(global_state.copy->expected_types);
	for (idx_t index = 0; index < global_state.bind.copy_column_count; index++) {
		copy_chunk.data[index].Reference(input.data[index]);
	}
	copy_chunk.SetCardinality(input.size());
	InterruptState interrupt_state;
	OperatorSinkInput sink_input {*global_state.copy->sink_state, *local_state.copy_state, interrupt_state};
	if (global_state.copy->Sink(context, copy_chunk, sink_input) != SinkResultType::NEED_MORE_INPUT) {
		throw InternalException("DuckLake distributed COPY stopped before consuming its input");
	}
}

static unique_ptr<DistributedWriteGlobalState>
DuckLakeRowDeltaInitializeGlobal(ClientContext &context, const DistributedExtensionWriteInfo &info,
                                 const DistributedWriteTaskContext &task) {
	task.Validate();
	auto bind = DeserializeBind(info.worker_bind_data);
	return make_uniq<DuckLakeDistributedRowDeltaGlobalState>(context, std::move(bind), task);
}

static unique_ptr<DistributedWriteLocalState> DuckLakeRowDeltaInitializeLocal(ExecutionContext &,
                                                                              const DistributedExtensionWriteInfo &,
                                                                              const DistributedWriteTaskContext &,
                                                                              DistributedWriteGlobalState &) {
	return make_uniq<DuckLakeDistributedRowDeltaLocalState>();
}

static void DuckLakeRowDeltaSink(ExecutionContext &context, const DistributedExtensionWriteInfo &,
                                 const DistributedWriteTaskContext &, DistributedWriteGlobalState &global_state_p,
                                 DistributedWriteLocalState &local_state_p, DataChunk &input) {
	auto &global_state = global_state_p.Cast<DuckLakeDistributedRowDeltaGlobalState>();
	auto &local_state = local_state_p.Cast<DuckLakeDistributedRowDeltaLocalState>();
	if (global_state.bind.kind == DuckLakeDistributedRowDeltaKind::MERGE_INSERT) {
		SinkRowDeltaCopy(context, global_state, local_state, input, false);
		local_state.affected_rows = CheckedAdd(local_state.affected_rows, input.size(), "worker affected row count");
		return;
	}
	if (global_state.bind.source_is_statically_empty) {
		if (input.size() != 0) {
			throw InvalidInputException(
			    "DuckLake distributed row mutation received rows from a statically empty source");
		}
		return;
	}
	auto input_row_count = input.size();
	for (auto index : global_state.bind.row_id_indexes) {
		if (index >= input.ColumnCount()) {
			throw InvalidInputException("DuckLake distributed row mutation row identifier index is out of bounds");
		}
	}
	if (input.data[global_state.bind.row_id_indexes[0]].GetType() != LogicalType::VARCHAR ||
	    input.data[global_state.bind.row_id_indexes[1]].GetType() != LogicalType::BIGINT) {
		throw InvalidInputException("DuckLake distributed row mutation row identifiers have invalid types");
	}
	SelectionVector selection(input.size());
	idx_t selection_count = 0;
	{
		lock_guard<mutex> guard(global_state.seen_rows_lock);
		for (idx_t row = 0; row < input.size(); row++) {
			auto file_value = input.GetValue(global_state.bind.row_id_indexes[0], row);
			auto position_value = input.GetValue(global_state.bind.row_id_indexes[1], row);
			if (file_value.IsNull() || position_value.IsNull()) {
				throw InvalidInputException("DuckLake distributed row mutation received a NULL row identifier");
			}
			auto file_path = file_value.GetValue<string>();
			auto position = position_value.GetValue<int64_t>();
			if (position < 0) {
				throw InvalidInputException("DuckLake distributed row mutation received a negative row position");
			}
			auto source = global_state.delete_source_indexes.find(file_path);
			if (source == global_state.delete_source_indexes.end() ||
			    NumericCast<idx_t>(position) >= global_state.bind.delete_sources[source->second].record_count) {
				throw InvalidInputException(
				    "DuckLake distributed row mutation received a row identifier outside its planned source files");
			}
			if (global_state.seen_rows[file_path].insert(NumericCast<idx_t>(position)).second) {
				selection.set_index(selection_count++, row);
			}
		}
	}
	if (selection_count == 0) {
		if (global_state.bind.kind == DuckLakeDistributedRowDeltaKind::DELETE) {
			local_state.affected_rows =
			    CheckedAdd(local_state.affected_rows, input_row_count, "worker affected row count");
		}
		return;
	}
	input.Slice(selection, selection_count);

	if (global_state.copy) {
		SinkRowDeltaCopy(context, global_state, local_state, input, true);
	}

	for (idx_t row = 0; row < input.size(); row++) {
		auto file_value = input.GetValue(global_state.bind.row_id_indexes[0], row);
		auto position_value = input.GetValue(global_state.bind.row_id_indexes[1], row);
		auto file_path = file_value.GetValue<string>();
		auto position = position_value.GetValue<int64_t>();
		local_state.deleted_rows[std::move(file_path)].push_back(NumericCast<idx_t>(position));
	}
	auto affected_rows =
	    global_state.bind.kind == DuckLakeDistributedRowDeltaKind::DELETE ? input_row_count : input.size();
	local_state.affected_rows = CheckedAdd(local_state.affected_rows, affected_rows, "worker affected row count");
}

static void DuckLakeRowDeltaCombine(ExecutionContext &context, const DistributedExtensionWriteInfo &,
                                    const DistributedWriteTaskContext &, DistributedWriteGlobalState &global_state_p,
                                    DistributedWriteLocalState &local_state_p) {
	auto &global_state = global_state_p.Cast<DuckLakeDistributedRowDeltaGlobalState>();
	auto &local_state = local_state_p.Cast<DuckLakeDistributedRowDeltaLocalState>();
	if (local_state.copy_state) {
		InterruptState interrupt_state;
		OperatorSinkCombineInput combine_input {*global_state.copy->sink_state, *local_state.copy_state,
		                                        interrupt_state};
		if (global_state.copy->Combine(context, combine_input) != SinkCombineResultType::FINISHED) {
			throw InternalException("DuckLake distributed row mutation COPY combine did not finish synchronously");
		}
	}
	lock_guard<mutex> guard(global_state.lock);
	for (auto &entry : local_state.deleted_rows) {
		auto &target = global_state.deleted_rows[entry.first];
		target.insert(target.end(), entry.second.begin(), entry.second.end());
	}
	global_state.affected_rows =
	    CheckedAdd(global_state.affected_rows, local_state.affected_rows, "worker affected row count");
}

static set<PositionWithSnapshot> MergePositionDeletes(const DuckLakeDistributedRowDeltaSourceState &source,
                                                      const vector<idx_t> &new_row_positions,
                                                      idx_t new_delete_snapshot) {
	set<idx_t> new_rows(new_row_positions.begin(), new_row_positions.end());
	if (new_rows.empty() || new_rows.size() != new_row_positions.size()) {
		throw NotImplementedException("The same DuckLake row was modified multiple times in one distributed write");
	}
	set<PositionWithSnapshot> result;
	for (idx_t index = 0; index < source.existing_delete_rows.size(); index++) {
		result.insert(PositionWithSnapshot {NumericCast<int64_t>(source.existing_delete_rows[index]),
		                                    NumericCast<int64_t>(source.existing_delete_snapshots[index])});
	}
	for (auto row : new_rows) {
		if (row >= source.record_count ||
		    !result.insert(PositionWithSnapshot {NumericCast<int64_t>(row), NumericCast<int64_t>(new_delete_snapshot)})
		         .second) {
			throw InvalidInputException(
			    "DuckLake distributed row mutation attempted to replace an invalid or already-deleted row");
		}
	}
	return result;
}

static DuckLakeDistributedDeleteFileResult WritePositionDeleteFile(ClientContext &context, const string &output_path,
                                                                   const DuckLakeDistributedRowDeltaSourceState &source,
                                                                   const vector<idx_t> &new_row_positions,
                                                                   idx_t new_delete_snapshot) {
	auto positions = MergePositionDeletes(source, new_row_positions, new_delete_snapshot);
	auto info = make_uniq<CopyInfo>();
	info->file_path = output_path;
	info->format = "parquet";
	info->is_from = false;
	child_list_t<Value> field_ids;
	field_ids.emplace_back("file_path", Value::INTEGER(MultiFileReader::FILENAME_FIELD_ID));
	field_ids.emplace_back("pos", Value::INTEGER(MultiFileReader::ORDINAL_FIELD_ID));
	field_ids.emplace_back("_ducklake_internal_snapshot_id",
	                       Value::INTEGER(MultiFileReader::LAST_UPDATED_SEQUENCE_NUMBER_ID));
	info->options["field_ids"].push_back(Value::STRUCT(std::move(field_ids)));

	vector<string> names {"file_path", "pos", "_ducklake_internal_snapshot_id"};
	vector<LogicalType> types {LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::BIGINT};
	auto &copy_function = DuckLakeFunctions::GetCopyFunction(context, "parquet").function;
	CopyFunctionBindInput bind_input(*info);
	auto bind_data = copy_function.copy_to_bind(context, bind_input, names, types);
	auto global_state = copy_function.copy_to_initialize_global(context, *bind_data, output_path);
	CopyFunctionFileStatistics statistics;
	copy_function.copy_to_get_written_statistics(context, *bind_data, *global_state, statistics);

	ThreadContext thread_context(context);
	ExecutionContext execution_context(context, thread_context, nullptr);
	auto local_state = copy_function.copy_to_initialize_local(execution_context, *bind_data);
	DataChunk chunk;
	chunk.Initialize(context, types);
	Value data_file_value(source.scan_file_path);
	chunk.data[0].Reference(data_file_value);
	auto position_data = FlatVector::GetData<int64_t>(chunk.data[1]);
	auto snapshot_data = FlatVector::GetData<int64_t>(chunk.data[2]);
	idx_t count = 0;
	try {
		for (const auto &position : positions) {
			position_data[count] = position.position;
			snapshot_data[count] = position.snapshot_id;
			count++;
			if (count == STANDARD_VECTOR_SIZE) {
				chunk.SetCardinality(count);
				copy_function.copy_to_sink(execution_context, *bind_data, *global_state, *local_state, chunk);
				count = 0;
			}
		}
		if (count != 0) {
			chunk.SetCardinality(count);
			copy_function.copy_to_sink(execution_context, *bind_data, *global_state, *local_state, chunk);
		}
		copy_function.copy_to_combine(execution_context, *bind_data, *global_state, *local_state);
		copy_function.copy_to_finalize(context, *bind_data, *global_state);
	} catch (...) {
		FileSystem::GetFileSystem(context).TryRemoveFile(output_path);
		throw;
	}

	DuckLakeDistributedDeleteFileResult result;
	result.data_file_path = source.scan_file_path;
	result.delete_file_path = output_path;
	result.format = DeleteFileFormat::PARQUET;
	result.new_delete_count = new_row_positions.size();
	result.delete_count = statistics.row_count;
	result.file_size_bytes = statistics.file_size_bytes;
	result.footer_size_bytes = statistics.footer_size_bytes.GetValue<idx_t>();
	result.pos_min_value = NumericCast<idx_t>(positions.begin()->position);
	result.pos_max_value = NumericCast<idx_t>(positions.rbegin()->position);
	result.begin_snapshot = NumericCast<idx_t>(positions.begin()->snapshot_id);
	idx_t max_snapshot = 0;
	for (const auto &position : positions) {
		max_snapshot = MaxValue(max_snapshot, NumericCast<idx_t>(position.snapshot_id));
		result.begin_snapshot = MinValue(result.begin_snapshot.GetIndex(), NumericCast<idx_t>(position.snapshot_id));
	}
	result.max_snapshot = max_snapshot;
	return result;
}

static DuckLakeDistributedDeleteFileResult WriteDeletionVectorFile(ClientContext &context, const string &output_path,
                                                                   const DuckLakeDistributedRowDeltaSourceState &source,
                                                                   const vector<idx_t> &new_row_positions) {
	set<idx_t> positions(source.existing_delete_rows.begin(), source.existing_delete_rows.end());
	for (auto row : new_row_positions) {
		if (row >= source.record_count || !positions.insert(row).second) {
			throw InvalidInputException(
			    "DuckLake distributed row mutation attempted to replace an invalid or already-deleted row");
		}
	}
	if (new_row_positions.empty() ||
	    positions.size() != source.existing_delete_rows.size() + new_row_positions.size()) {
		throw NotImplementedException("The same DuckLake row was modified multiple times in one distributed write");
	}
	auto blob = DuckLakeDeletionVectorData::ToBlob(positions);
	auto &file_system = FileSystem::GetFileSystem(context);
	try {
		auto handle =
		    file_system.OpenFile(output_path, FileOpenFlags::FILE_FLAGS_WRITE | FileOpenFlags::FILE_FLAGS_FILE_CREATE);
		handle->Write(blob.data(), blob.size());
		handle->Close();
	} catch (...) {
		file_system.TryRemoveFile(output_path);
		throw;
	}

	DuckLakeDistributedDeleteFileResult result;
	result.data_file_path = source.scan_file_path;
	result.delete_file_path = output_path;
	result.format = DeleteFileFormat::PUFFIN;
	result.new_delete_count = new_row_positions.size();
	result.delete_count = positions.size();
	result.file_size_bytes = blob.size();
	result.pos_min_value = *positions.begin();
	result.pos_max_value = *positions.rbegin();
	return result;
}

static vector<distributed::DistributedCopyFileInfo>
FinalizeCopyAndReadStatistics(ClientContext &context, DuckLakeDistributedRowDeltaGlobalState &global_state) {
	vector<distributed::DistributedCopyFileInfo> result;
	if (!global_state.copy) {
		return result;
	}
	if (!global_state.copy_sink_initialized || !global_state.copy->sink_state) {
		throw InternalException("DuckLake distributed row mutation received rows without initializing its COPY writer");
	}
	if (global_state.copy->FinalizeInternal(context, *global_state.copy->sink_state) != SinkFinalizeType::READY) {
		throw InternalException("DuckLake distributed row mutation COPY finalization did not finish synchronously");
	}
	auto source_global = global_state.copy->GetGlobalSourceState(context);
	ThreadContext thread_context(context);
	ExecutionContext execution_context(context, thread_context, nullptr);
	auto source_local = global_state.copy->GetLocalSourceState(execution_context, *source_global);
	InterruptState interrupt_state;
	OperatorSourceInput source_input {*source_global, *source_local, interrupt_state};
	while (true) {
		DataChunk chunk;
		chunk.Initialize(context, global_state.copy->types);
		auto state = global_state.copy->GetDataInternal(execution_context, chunk, source_input);
		for (idx_t row = 0; row < chunk.size(); row++) {
			distributed::DistributedCopyFileInfo file;
			file.final_path = chunk.GetValue(0, row).GetValue<string>();
			file.row_count = chunk.GetValue(1, row).GetValue<idx_t>();
			file.file_size_bytes = chunk.GetValue(2, row).GetValue<idx_t>();
			file.footer_size_bytes = chunk.GetValue(3, row);
			file.column_statistics = chunk.GetValue(4, row);
			file.partition_keys = chunk.GetValue(5, row);
			result.push_back(std::move(file));
		}
		if (state == SourceResultType::FINISHED) {
			break;
		}
		if (state != SourceResultType::HAVE_MORE_OUTPUT) {
			throw InternalException("DuckLake distributed row mutation COPY statistics source blocked unexpectedly");
		}
	}
	return result;
}

static vector<DistributedWriteFragment> DuckLakeRowDeltaFinalize(ClientContext &context,
                                                                 const DistributedExtensionWriteInfo &,
                                                                 const DistributedWriteTaskContext &task,
                                                                 DistributedWriteGlobalState &global_state_p) {
	auto &global_state = global_state_p.Cast<DuckLakeDistributedRowDeltaGlobalState>();
	idx_t affected_rows;
	unordered_map<string, vector<idx_t>> deleted_rows;
	{
		lock_guard<mutex> guard(global_state.lock);
		affected_rows = global_state.affected_rows;
		deleted_rows = std::move(global_state.deleted_rows);
	}
	if (affected_rows == 0) {
		return {};
	}

	auto data_files = FinalizeCopyAndReadStatistics(context, global_state);
	vector<DuckLakeDistributedDeleteFileResult> delete_files;
	for (auto &entry : deleted_rows) {
		auto source_index = global_state.delete_source_indexes.find(entry.first);
		if (source_index == global_state.delete_source_indexes.end()) {
			throw InvalidInputException(
			    "DuckLake distributed row mutation produced rows for an unplanned source data file");
		}
		auto &file_system = FileSystem::GetFileSystem(context);
		auto &source = global_state.bind.delete_sources[source_index->second];
		if (global_state.bind.use_deletion_vectors) {
			auto file_name = "ducklake-" + UUID::ToString(UUID::GenerateRandomUUID()) + "-delete.puffin";
			auto output_path = file_system.JoinPath(global_state.attempt_root, file_name);
			delete_files.push_back(WriteDeletionVectorFile(context, output_path, source, entry.second));
		} else {
			auto file_name = "ducklake-" + UUID::ToString(UUID::GenerateRandomUUID()) + "-delete.parquet";
			auto output_path = file_system.JoinPath(global_state.attempt_root, file_name);
			delete_files.push_back(WritePositionDeleteFile(context, output_path, source, entry.second,
			                                               global_state.bind.new_delete_snapshot));
		}
	}

	idx_t data_rows = 0;
	idx_t delete_rows = 0;
	idx_t byte_count = 0;
	for (const auto &file : data_files) {
		data_rows = CheckedAdd(data_rows, file.row_count, "worker data row count");
		byte_count = CheckedAdd(byte_count, file.file_size_bytes, "worker byte count");
	}
	for (const auto &file : delete_files) {
		delete_rows = CheckedAdd(delete_rows, file.new_delete_count, "worker delete row count");
		byte_count = CheckedAdd(byte_count, file.file_size_bytes, "worker byte count");
	}
	auto invalid_counts = false;
	if (global_state.bind.kind == DuckLakeDistributedRowDeltaKind::MERGE_INSERT) {
		invalid_counts = delete_rows != 0 || data_rows != affected_rows;
	} else if (global_state.bind.kind == DuckLakeDistributedRowDeltaKind::UPDATE) {
		invalid_counts = delete_rows != affected_rows || data_rows != affected_rows;
	} else {
		invalid_counts = delete_rows == 0 || delete_rows > affected_rows || data_rows != 0;
	}
	if (invalid_counts) {
		throw InternalException("DuckLake distributed row mutation worker produced inconsistent affected-row counts");
	}

	DistributedWriteFragment fragment;
	fragment.fragment_id = task.query_id + "/" + task.task_attempt_id;
	fragment.payload = SerializeFragmentPayload(global_state.bind.kind, data_files, delete_files);
	fragment.row_count = affected_rows;
	fragment.byte_count = byte_count;
	for (idx_t index = 0; index < data_files.size(); index++) {
		DistributedWriteArtifact artifact;
		artifact.artifact_id = "data:" + to_string(index);
		artifact.uri =
		    data_files[index].final_path.empty() ? data_files[index].staging_path : data_files[index].final_path;
		artifact.codec = DUCKLAKE_DATA_FILE_CODEC;
		fragment.artifacts.push_back(std::move(artifact));
	}
	for (idx_t index = 0; index < delete_files.size(); index++) {
		DistributedWriteArtifact artifact;
		artifact.artifact_id = "delete:" + to_string(index);
		artifact.uri = delete_files[index].delete_file_path;
		artifact.codec = delete_files[index].format == DeleteFileFormat::PUFFIN ? DUCKLAKE_DELETION_VECTOR_FILE_CODEC
		                                                                        : DUCKLAKE_POSITION_DELETE_FILE_CODEC;
		fragment.artifacts.push_back(std::move(artifact));
	}
	return {std::move(fragment)};
}

static string ValidateArtifactPathInRoot(ClientContext &context, const string &allowed_root, const string &path,
                                         idx_t expected_size, const string &description) {
	auto &file_system = FileSystem::GetFileSystem(context);
	auto canonical_root = CanonicalDuckLakeMutationPath(file_system, allowed_root, description + " root");
	auto canonical_path = CanonicalDuckLakeMutationPath(file_system, path, description);
	auto separator = file_system.PathSeparator(canonical_root);
	if (separator.empty() || canonical_path == canonical_root ||
	    !distributed::DistributedCopyPathIsInDirectory(canonical_path, canonical_root, separator)) {
		throw InvalidInputException("DuckLake distributed %s path is outside its allowed artifact root", description);
	}

	auto relative_path = canonical_path.substr(canonical_root.size());
	while (StringUtil::StartsWith(relative_path, separator)) {
		relative_path.erase(0, separator.size());
	}
	idx_t component_start = 0;
	while (component_start <= relative_path.size()) {
		auto component_end = relative_path.find(separator, component_start);
		auto component = relative_path.substr(
		    component_start, component_end == string::npos ? string::npos : component_end - component_start);
		if (component.empty() || component == "." || component == "..") {
			throw InvalidInputException("DuckLake distributed %s path contains an invalid component", description);
		}
		if (component_end == string::npos) {
			break;
		}
		component_start = component_end + separator.size();
	}
	ValidateFileSize(context, canonical_path, expected_size, description);
	return canonical_path;
}

static string ValidateAttemptArtifactPath(ClientContext &context, const string &artifact_path,
                                          const string &task_attempt_id, const string &path, idx_t expected_size,
                                          const string &description) {
	auto attempt_root = DuckLakeDistributedAttemptRoot(context, artifact_path, task_attempt_id);
	return ValidateArtifactPathInRoot(context, attempt_root, path, expected_size, description);
}

static void ValidateCopyFileValues(const distributed::DistributedCopyFileInfo &file) {
	auto expected_types = GetCopyFunctionReturnLogicalTypes(CopyFunctionReturnType::WRITTEN_FILE_STATISTICS);
	if (file.row_count == 0 || file.file_size_bytes == 0 || file.footer_size_bytes.IsNull() ||
	    file.footer_size_bytes.type() != expected_types[3] ||
	    UBigIntValue::Get(file.footer_size_bytes) > file.file_size_bytes || file.column_statistics.IsNull() ||
	    file.column_statistics.type() != expected_types[4] || file.partition_keys.type() != expected_types[5]) {
		throw InvalidInputException("DuckLake distributed row mutation returned invalid data-file statistics");
	}
	const auto signed_max = NumericCast<idx_t>(NumericLimits<int64_t>::Maximum());
	if (file.row_count > signed_max || file.file_size_bytes > signed_max) {
		throw InvalidInputException("DuckLake distributed row mutation data-file statistics exceed signed limits");
	}
}

} // namespace

void ValidateDuckLakeDistributedRowDeltaCopyShape(const PhysicalCopyToFile &copy) {
	ValidateDistributedRowDeltaCopyShape(copy);
}

PhysicalOperator &PlanDuckLakeDistributedRowDeltaRepartition(PhysicalPlanGenerator &planner, PhysicalOperator &input,
                                                             idx_t file_path_index) {
	if (file_path_index >= input.types.size() || input.types[file_path_index] != LogicalType::VARCHAR) {
		throw InternalException("DuckLake distributed row mutation file-path column is invalid");
	}
	vector<ExprRef> partition_by;
	partition_by.emplace_back(make_uniq<BoundReferenceExpression>(LogicalType::VARCHAR, file_path_index));
	auto repartition_spec = RepartitionSpec::create_hash(0, std::move(partition_by));
	auto &result =
	    planner.Make<PhysicalRepartition>(input.types, std::move(repartition_spec), input.estimated_cardinality);
	result.children.push_back(input);
	return result;
}

static DuckLakeDistributedRowDeltaBind BuildDuckLakeDistributedRowDeltaBind(
    ClientContext &context, const DuckLakeTableEntry &table, const vector<DuckLakeFileListExtendedEntry> &source_files,
    const string &artifact_path, bool source_is_statically_empty, DuckLakeDistributedRowDeltaKind kind) {
	if (kind == DuckLakeDistributedRowDeltaKind::MERGE_INSERT) {
		if (source_is_statically_empty || !source_files.empty()) {
			throw InternalException("DuckLake distributed MERGE INSERT source state is inconsistent");
		}
	} else if (source_is_statically_empty != source_files.empty()) {
		throw InternalException("DuckLake distributed row mutation source state is inconsistent");
	}
	DuckLakeDistributedRowDeltaBind bind;
	bind.kind = kind;
	bind.source_is_statically_empty = source_is_statically_empty;
	bind.data_path = table.DataPath();
	bind.artifact_path = artifact_path;
	auto &catalog = table.catalog.Cast<DuckLakeCatalog>();
	auto &schema = table.ParentSchema().Cast<DuckLakeSchemaEntry>();
	bind.catalog_name = catalog.GetName();
	bind.schema_name = schema.name;
	bind.table_name = table.name;
	bind.schema_uuid = schema.GetSchemaUUID();
	bind.table_uuid = table.GetTableUUID();
	bind.schema_id = schema.GetSchemaId().index;
	bind.table_id = table.GetTableId().index;
	bind.field_identity = GetDuckLakeDistributedFieldIdentity(table.GetFieldData());
	bind.partition_identity = GetDuckLakeDistributedPartitionIdentity(table.GetPartitionData().get());
	bind.sort_identity = GetDuckLakeDistributedSortIdentity(table.GetSortData().get());
	bind.use_deletion_vectors = catalog.WriteDeletionVectors(schema.GetSchemaId(), table.GetTableId());
	auto &transaction = DuckLakeTransaction::Get(context, table.catalog);
	bind.snapshot = transaction.GetSnapshot();
	bind.new_delete_snapshot = CheckedAdd(bind.snapshot.snapshot_id, 1, "snapshot id");
	if (kind != DuckLakeDistributedRowDeltaKind::MERGE_INSERT && !source_is_statically_empty) {
		bind.delete_sources = BuildSourceStates(context, source_files,
		                                        kind == DuckLakeDistributedRowDeltaKind::DELETE ? "DELETE" : "UPDATE");
	}
	return bind;
}

string BuildDuckLakeDistributedDeleteBind(ClientContext &context, const DuckLakeTableEntry &table,
                                          const vector<DuckLakeFileListExtendedEntry> &source_files,
                                          const vector<idx_t> &row_id_indexes, const string &artifact_path,
                                          bool source_is_statically_empty) {
	if (row_id_indexes.size() != 3) {
		throw InternalException("DuckLake distributed DELETE requires three native row identifiers");
	}
	auto bind =
	    BuildDuckLakeDistributedRowDeltaBind(context, table, source_files, artifact_path, source_is_statically_empty,
	                                         DuckLakeDistributedRowDeltaKind::DELETE);
	bind.row_id_indexes = {row_id_indexes[0], row_id_indexes[2]};
	return SerializeBind(bind);
}

string BuildDuckLakeDistributedUpdateBind(ClientContext &context, const DuckLakeTableEntry &table,
                                          const vector<DuckLakeFileListExtendedEntry> &source_files,
                                          const PhysicalCopyToFile &copy, idx_t copy_column_count,
                                          idx_t file_path_index, idx_t row_position_index, const string &artifact_path,
                                          bool source_is_statically_empty) {
	auto bind =
	    BuildDuckLakeDistributedRowDeltaBind(context, table, source_files, artifact_path, source_is_statically_empty,
	                                         DuckLakeDistributedRowDeltaKind::UPDATE);
	bind.row_id_indexes = {file_path_index, row_position_index};
	bind.copy_column_count = copy_column_count;
	bind.copy_operator = SerializeShallowCopy(copy);
	return SerializeBind(bind);
}

string BuildDuckLakeDistributedMergeInsertBind(ClientContext &context, const DuckLakeTableEntry &table,
                                               const PhysicalCopyToFile &copy, idx_t copy_column_count,
                                               const string &artifact_path) {
	vector<DuckLakeFileListExtendedEntry> source_files;
	auto bind = BuildDuckLakeDistributedRowDeltaBind(context, table, source_files, artifact_path, false,
	                                                 DuckLakeDistributedRowDeltaKind::MERGE_INSERT);
	bind.copy_column_count = copy_column_count;
	bind.copy_operator = SerializeShallowCopy(copy);
	return SerializeBind(bind);
}

DistributedExtensionWriteCallbacks DuckLakeDistributedRowDeltaCallbacks() {
	DistributedExtensionWriteCallbacks callbacks;
	callbacks.initialize_global = DuckLakeRowDeltaInitializeGlobal;
	callbacks.initialize_local = DuckLakeRowDeltaInitializeLocal;
	callbacks.sink = DuckLakeRowDeltaSink;
	callbacks.combine = DuckLakeRowDeltaCombine;
	callbacks.finalize = DuckLakeRowDeltaFinalize;
	return callbacks;
}

DuckLakeDistributedRowDeltaResult DecodeDuckLakeDistributedRowDeltaResults(
    ClientContext &context, const string &data_path, const string &artifact_path,
    const DistributedExtensionWriteInfo &info, const vector<DistributedWriteTaskResult> &results,
    DuckLakeDistributedRowDeltaKind expected_kind, bool expected_deletion_vectors) {
	info.Validate();
	if (info.mode != DistributedWriteMode::CALLBACK ||
	    info.fragment_codec !=
	        DistributedPayloadCodec {DUCKLAKE_ROW_DELTA_FRAGMENT_CODEC, DUCKLAKE_ROW_DELTA_PROTOCOL_VERSION}) {
		throw InvalidInputException("DuckLake distributed row mutation coordinator resolved the wrong worker protocol");
	}
	auto bind = DeserializeBind(info.worker_bind_data);
	if (bind.kind != expected_kind || bind.use_deletion_vectors != expected_deletion_vectors) {
		throw InvalidInputException("DuckLake distributed row mutation coordinator bind does not match its target");
	}
	if (results.empty() && !bind.source_is_statically_empty &&
	    expected_kind != DuckLakeDistributedRowDeltaKind::MERGE_INSERT) {
		throw InvalidInputException("DuckLake distributed row mutation returned no selected task results");
	}
	auto &file_system = FileSystem::GetFileSystem(context);
	if (CanonicalDuckLakeMutationPath(file_system, bind.data_path, "bind data root") !=
	        CanonicalDuckLakeMutationPath(file_system, data_path, "coordinator data root") ||
	    CanonicalDuckLakeMutationPath(file_system, bind.artifact_path, "bind artifact root") !=
	        CanonicalDuckLakeMutationPath(file_system, artifact_path, "coordinator artifact root")) {
		throw InvalidInputException("DuckLake distributed row mutation coordinator bind paths changed");
	}

	DuckLakeDistributedRowDeltaResult combined;
	set<string> task_attempt_ids;
	set<string> fragment_ids;
	set<string> artifact_paths;
	set<string> referenced_data_paths;
	string query_id;
	idx_t total_artifact_bytes = 0;
	const auto signed_max = NumericCast<idx_t>(NumericLimits<int64_t>::Maximum());
	for (const auto &task_result : results) {
		task_result.Validate();
		if (task_result.capability != info.capability || task_result.fragment_codec != info.fragment_codec) {
			throw InvalidInputException("DuckLake distributed row mutation received a mismatched task result protocol");
		}
		if (query_id.empty()) {
			query_id = task_result.query_id;
		} else if (task_result.query_id != query_id) {
			throw InvalidInputException("DuckLake distributed row mutation received results from multiple queries");
		}
		if (!task_attempt_ids.insert(task_result.task_attempt_id).second) {
			throw InvalidInputException("DuckLake distributed row mutation selected task attempt '%s' more than once",
			                            task_result.task_attempt_id);
		}
		if (task_result.fragments.size() > 1) {
			throw InvalidInputException(
			    "DuckLake distributed row mutation task attempt '%s' returned more than one fragment",
			    task_result.task_attempt_id);
		}
		for (const auto &fragment : task_result.fragments) {
			if (!fragment_ids.insert(fragment.fragment_id).second ||
			    fragment.fragment_id != task_result.query_id + "/" + task_result.task_attempt_id) {
				throw InvalidInputException(
				    "DuckLake distributed row mutation fragment has a duplicate or non-canonical identity");
			}
			if (fragment.row_count == 0) {
				throw InvalidInputException("DuckLake distributed row mutation returned an empty fragment");
			}
			auto decoded = DeserializeFragmentPayload(fragment.payload, expected_kind);
			if (expected_kind == DuckLakeDistributedRowDeltaKind::DELETE && !decoded.data_files.empty()) {
				throw InvalidInputException("DuckLake distributed DELETE returned a data-file artifact");
			}
			if (expected_kind == DuckLakeDistributedRowDeltaKind::MERGE_INSERT && !decoded.delete_files.empty()) {
				throw InvalidInputException("DuckLake distributed MERGE INSERT returned a delete-file artifact");
			}

			idx_t row_count = 0;
			idx_t delete_row_count = 0;
			idx_t byte_count = 0;
			vector<pair<string, DistributedPayloadCodec>> expected_artifacts;
			vector<string> expected_artifact_ids;
			auto attempt_root = DuckLakeDistributedAttemptRoot(context, artifact_path, task_result.task_attempt_id);
			idx_t data_artifact_index = 0;
			for (auto &file : decoded.data_files) {
				const auto &path = file.final_path.empty() ? file.staging_path : file.final_path;
				if (path.empty()) {
					throw InvalidInputException("DuckLake distributed row mutation returned an empty data-file path");
				}
				ValidateCopyFileValues(file);
				if (file.file_size_bytes > signed_max - total_artifact_bytes) {
					throw InvalidInputException(
					    "DuckLake distributed row mutation artifact sizes exceed signed limits");
				}
				total_artifact_bytes += file.file_size_bytes;
				auto canonical_path = ValidateAttemptArtifactPath(context, artifact_path, task_result.task_attempt_id,
				                                                  path, file.file_size_bytes, "data file");
				if (!artifact_paths.insert(std::move(canonical_path)).second) {
					throw InvalidInputException(
					    "DuckLake distributed row mutation returned a duplicate data-file path");
				}
				row_count = CheckedAdd(row_count, file.row_count, "fragment data row count");
				byte_count = CheckedAdd(byte_count, file.file_size_bytes, "fragment byte count");
				expected_artifacts.emplace_back(path, DUCKLAKE_DATA_FILE_CODEC);
				expected_artifact_ids.push_back("data:" + to_string(data_artifact_index++));
				combined.data_files.push_back(std::move(file));
				combined.data_file_artifact_roots.push_back(attempt_root);
			}

			idx_t delete_artifact_index = 0;
			for (auto &file : decoded.delete_files) {
				auto expected_format = expected_deletion_vectors ? DeleteFileFormat::PUFFIN : DeleteFileFormat::PARQUET;
				if (file.data_file_path.empty() || file.delete_file_path.empty() || file.format != expected_format ||
				    file.new_delete_count == 0 || file.delete_count < file.new_delete_count ||
				    file.file_size_bytes == 0 || file.pos_min_value > file.pos_max_value ||
				    file.delete_count > signed_max || file.pos_max_value > signed_max ||
				    file.delete_count - 1 > file.pos_max_value - file.pos_min_value ||
				    !referenced_data_paths.insert(file.data_file_path).second) {
					throw InvalidInputException(
					    "DuckLake distributed row mutation returned invalid delete-file metadata");
				}
				if (file.file_size_bytes > signed_max - total_artifact_bytes) {
					throw InvalidInputException(
					    "DuckLake distributed row mutation artifact sizes exceed signed limits");
				}
				total_artifact_bytes += file.file_size_bytes;
				if (file.format == DeleteFileFormat::PARQUET) {
					if (file.footer_size_bytes == 0 || file.footer_size_bytes >= file.file_size_bytes ||
					    !file.begin_snapshot.IsValid() || !file.max_snapshot.IsValid() ||
					    file.begin_snapshot.GetIndex() > file.max_snapshot.GetIndex()) {
						throw InvalidInputException(
						    "DuckLake distributed row mutation returned invalid positional-delete metadata");
					}
				} else if (file.footer_size_bytes != 0 || file.begin_snapshot.IsValid() ||
				           file.max_snapshot.IsValid()) {
					throw InvalidInputException(
					    "DuckLake distributed row mutation returned invalid deletion-vector metadata");
				}
				auto canonical_path =
				    ValidateAttemptArtifactPath(context, artifact_path, task_result.task_attempt_id,
				                                file.delete_file_path, file.file_size_bytes, "delete file");
				if (!artifact_paths.insert(std::move(canonical_path)).second) {
					throw InvalidInputException(
					    "DuckLake distributed row mutation returned a duplicate delete-file path");
				}
				delete_row_count = CheckedAdd(delete_row_count, file.new_delete_count, "fragment delete row count");
				byte_count = CheckedAdd(byte_count, file.file_size_bytes, "fragment byte count");
				auto codec = file.format == DeleteFileFormat::PUFFIN ? DUCKLAKE_DELETION_VECTOR_FILE_CODEC
				                                                     : DUCKLAKE_POSITION_DELETE_FILE_CODEC;
				expected_artifacts.emplace_back(file.delete_file_path, codec);
				expected_artifact_ids.push_back("delete:" + to_string(delete_artifact_index++));
				combined.delete_files.push_back(std::move(file));
			}
			auto invalid_counts = false;
			if (expected_kind == DuckLakeDistributedRowDeltaKind::MERGE_INSERT) {
				invalid_counts = delete_row_count != 0 || row_count != fragment.row_count;
			} else if (expected_kind == DuckLakeDistributedRowDeltaKind::UPDATE) {
				invalid_counts = delete_row_count != fragment.row_count || row_count != fragment.row_count;
			} else {
				invalid_counts = delete_row_count == 0 || delete_row_count > fragment.row_count || row_count != 0;
			}
			if (invalid_counts || byte_count != fragment.byte_count ||
			    fragment.artifacts.size() != expected_artifacts.size()) {
				throw InvalidInputException("DuckLake distributed row mutation fragment counts are inconsistent");
			}
			for (idx_t index = 0; index < fragment.artifacts.size(); index++) {
				const auto &artifact = fragment.artifacts[index];
				if (artifact.artifact_id != expected_artifact_ids[index] ||
				    artifact.uri != expected_artifacts[index].first ||
				    artifact.codec != expected_artifacts[index].second || !artifact.payload.empty()) {
					throw InvalidInputException(
					    "DuckLake distributed row mutation fragment has invalid artifact metadata");
				}
				combined.selected_artifact_paths.insert(artifact.uri);
			}
			combined.affected_rows =
			    CheckedAdd(combined.affected_rows, fragment.row_count, "coordinator affected row count");
			if (combined.affected_rows > NumericCast<idx_t>(NumericLimits<int64_t>::Maximum())) {
				throw InvalidInputException(
				    "DuckLake distributed row mutation affected row count exceeds signed limits");
			}
		}
	}
	return combined;
}

static bool OptionalIndexesEqual(const optional_idx &left, const optional_idx &right) {
	return left.IsValid() == right.IsValid() && (!left.IsValid() || left.GetIndex() == right.GetIndex());
}

static bool SourceStatesEqual(const DuckLakeDistributedRowDeltaSourceState &left,
                              const DuckLakeDistributedRowDeltaSourceState &right) {
	return left.scan_file_path == right.scan_file_path && left.data_file_id == right.data_file_id &&
	       left.data_file_size_bytes == right.data_file_size_bytes &&
	       OptionalIndexesEqual(left.data_file_footer_size, right.data_file_footer_size) &&
	       left.record_count == right.record_count && OptionalIndexesEqual(left.row_id_start, right.row_id_start) &&
	       OptionalIndexesEqual(left.mapping_id, right.mapping_id) && left.has_delete_file == right.has_delete_file &&
	       left.delete_file_id == right.delete_file_id && left.delete_file_path == right.delete_file_path &&
	       left.delete_file_size_bytes == right.delete_file_size_bytes &&
	       OptionalIndexesEqual(left.delete_file_footer_size, right.delete_file_footer_size) &&
	       left.delete_file_format == right.delete_file_format &&
	       OptionalIndexesEqual(left.delete_file_begin_snapshot, right.delete_file_begin_snapshot) &&
	       left.existing_delete_rows == right.existing_delete_rows &&
	       left.existing_delete_snapshots == right.existing_delete_snapshots;
}

void ValidateDuckLakeDistributedRowDeltaSourceBaseline(ClientContext &context, const DuckLakeTableEntry &target_table,
                                                       const vector<DuckLakeFileListExtendedEntry> &planned_files,
                                                       const string &worker_bind_data, const string &operation_name,
                                                       bool source_is_statically_empty) {
	auto bind = DeserializeBind(worker_bind_data);
	auto &catalog = target_table.catalog.Cast<DuckLakeCatalog>();
	auto &schema = target_table.ParentSchema().Cast<DuckLakeSchemaEntry>();
	auto &transaction = DuckLakeTransaction::Get(context, catalog);
	if (bind.catalog_name != catalog.GetName() || bind.schema_name != schema.name ||
	    bind.table_name != target_table.name || bind.schema_uuid != schema.GetSchemaUUID() ||
	    bind.table_uuid != target_table.GetTableUUID() || bind.schema_id != schema.GetSchemaId().index ||
	    bind.table_id != target_table.GetTableId().index ||
	    !DuckLakeDistributedSnapshotsMatch(bind.snapshot, transaction.GetSnapshot()) ||
	    bind.field_identity != GetDuckLakeDistributedFieldIdentity(target_table.GetFieldData()) ||
	    bind.partition_identity != GetDuckLakeDistributedPartitionIdentity(target_table.GetPartitionData().get()) ||
	    bind.sort_identity != GetDuckLakeDistributedSortIdentity(target_table.GetSortData().get())) {
		throw TransactionException("DuckLake distributed %s target state changed after planning", operation_name);
	}
	if (bind.source_is_statically_empty != source_is_statically_empty) {
		throw TransactionException("DuckLake distributed %s source state changed after planning", operation_name);
	}
	if (source_is_statically_empty) {
		if (!planned_files.empty() || !bind.delete_sources.empty()) {
			throw TransactionException("DuckLake distributed %s statically-empty source state is inconsistent",
			                           operation_name);
		}
		return;
	}
	if (planned_files.empty()) {
		throw InvalidInputException("DuckLake distributed %s is missing its planned source files", operation_name);
	}
	auto current_states = BuildSourceStates(context, planned_files, operation_name);
	if (current_states.size() != bind.delete_sources.size()) {
		throw TransactionException("DuckLake distributed %s source state changed after planning", operation_name);
	}
	unordered_map<string, const DuckLakeDistributedRowDeltaSourceState *> current_by_path;
	for (const auto &state : current_states) {
		current_by_path.emplace(state.scan_file_path, &state);
	}
	for (const auto &planned : bind.delete_sources) {
		auto current = current_by_path.find(planned.scan_file_path);
		if (current == current_by_path.end() || !SourceStatesEqual(planned, *current->second)) {
			throw TransactionException("DuckLake distributed %s source state changed after planning", operation_name);
		}
	}
	for (const auto &planned : planned_files) {
		auto current = current_by_path.find(planned.file.path);
		if (current == current_by_path.end() || current->second->data_file_id != planned.file_id.index ||
		    current->second->record_count != planned.row_count) {
			throw TransactionException("DuckLake distributed %s source files changed after planning", operation_name);
		}
	}
}

struct ValidatedDeleteRows {
	vector<idx_t> rows;
	vector<idx_t> snapshots;
};

static ValidatedDeleteRows ValidatePositionDeleteArtifact(ClientContext &context,
                                                          const DuckLakeDistributedDeleteFileResult &file,
                                                          const DuckLakeDistributedRowDeltaSourceState &source) {
	DuckLakeFileData artifact;
	artifact.path = file.delete_file_path;
	artifact.file_size_bytes = file.file_size_bytes;
	artifact.footer_size = file.footer_size_bytes;
	artifact.format = DeleteFileFormat::PARQUET;
	ParquetFileScanner scanner(context, artifact);
	auto &types = scanner.GetTypes();
	auto &names = scanner.GetNames();
	if (types != vector<LogicalType> {LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::BIGINT} ||
	    names != vector<string> {"file_path", "pos", "_ducklake_internal_snapshot_id"} ||
	    scanner.GetRowCount() != file.delete_count) {
		throw InvalidInputException(
		    "DuckLake distributed row mutation returned a positional-delete file with an invalid schema");
	}

	ValidatedDeleteRows result;
	DataChunk chunk;
	chunk.Initialize(context, types);
	while (scanner.Scan(chunk)) {
		for (idx_t row = 0; row < chunk.size(); row++) {
			auto path_value = chunk.GetValue(0, row);
			auto position_value = chunk.GetValue(1, row);
			auto snapshot_value = chunk.GetValue(2, row);
			if (path_value.IsNull() || position_value.IsNull() || snapshot_value.IsNull() ||
			    path_value.GetValue<string>() != source.scan_file_path) {
				throw InvalidInputException(
				    "DuckLake distributed row mutation returned invalid positional-delete rows");
			}
			auto position = position_value.GetValue<int64_t>();
			auto snapshot = snapshot_value.GetValue<int64_t>();
			if (position < 0 || snapshot <= 0 || NumericCast<idx_t>(position) >= source.record_count ||
			    (!result.rows.empty() && NumericCast<idx_t>(position) <= result.rows.back())) {
				throw InvalidInputException(
				    "DuckLake distributed row mutation returned invalid positional-delete metadata");
			}
			result.rows.push_back(NumericCast<idx_t>(position));
			result.snapshots.push_back(NumericCast<idx_t>(snapshot));
		}
	}
	return result;
}

static ValidatedDeleteRows ValidateDeletionVectorArtifact(ClientContext &context,
                                                          const DuckLakeDistributedDeleteFileResult &file,
                                                          const DuckLakeDistributedRowDeltaSourceState &source) {
	auto &file_system = FileSystem::GetFileSystem(context);
	auto handle = file_system.OpenFile(file.delete_file_path, FileOpenFlags::FILE_FLAGS_READ);
	auto buffer = make_unsafe_uniq_array<data_t>(file.file_size_bytes);
	handle->Read(buffer.get(), file.file_size_bytes);
	auto deletion_vector = DuckLakeDeletionVectorData::FromBlob(buffer.get(), file.file_size_bytes);
	set<idx_t> deleted_rows;
	deletion_vector->ToSet(deleted_rows);
	if (deleted_rows.size() != file.delete_count || deleted_rows.empty() ||
	    *deleted_rows.rbegin() >= source.record_count) {
		throw InvalidInputException("DuckLake distributed row mutation returned inconsistent deletion-vector rows");
	}
	ValidatedDeleteRows result;
	result.rows.assign(deleted_rows.begin(), deleted_rows.end());
	return result;
}

static void ValidateDeleteRows(const DuckLakeDistributedDeleteFileResult &file,
                               const DuckLakeDistributedRowDeltaSourceState &source, const ValidatedDeleteRows &actual,
                               idx_t new_delete_snapshot, const string &operation_name) {
	if (actual.rows.empty() || actual.rows.size() != file.delete_count || actual.rows.front() != file.pos_min_value ||
	    actual.rows.back() != file.pos_max_value ||
	    actual.rows.size() != source.existing_delete_rows.size() + file.new_delete_count) {
		throw InvalidInputException("DuckLake distributed %s delete artifact has inconsistent row counts",
		                            operation_name);
	}
	unordered_map<idx_t, idx_t> actual_snapshots;
	if (file.format == DeleteFileFormat::PARQUET) {
		if (actual.snapshots.size() != actual.rows.size()) {
			throw InvalidInputException("DuckLake distributed %s delete artifact has incomplete snapshots",
			                            operation_name);
		}
		idx_t min_snapshot = NumericLimits<idx_t>::Maximum();
		idx_t max_snapshot = 0;
		for (idx_t index = 0; index < actual.rows.size(); index++) {
			actual_snapshots.emplace(actual.rows[index], actual.snapshots[index]);
			min_snapshot = MinValue(min_snapshot, actual.snapshots[index]);
			max_snapshot = MaxValue(max_snapshot, actual.snapshots[index]);
		}
		if (!file.begin_snapshot.IsValid() || !file.max_snapshot.IsValid() ||
		    file.begin_snapshot.GetIndex() != min_snapshot || file.max_snapshot.GetIndex() != max_snapshot) {
			throw InvalidInputException("DuckLake distributed %s delete artifact has inconsistent snapshots",
			                            operation_name);
		}
	}
	unordered_map<idx_t, idx_t> existing_snapshots;
	for (idx_t index = 0; index < source.existing_delete_rows.size(); index++) {
		auto row = source.existing_delete_rows[index];
		existing_snapshots.emplace(row, source.existing_delete_snapshots[index]);
		auto actual_row = std::lower_bound(actual.rows.begin(), actual.rows.end(), row);
		if (actual_row == actual.rows.end() || *actual_row != row) {
			throw InvalidInputException("DuckLake distributed %s omitted an existing delete row", operation_name);
		}
		if (file.format == DeleteFileFormat::PARQUET &&
		    actual_snapshots[row] != source.existing_delete_snapshots[index]) {
			throw InvalidInputException("DuckLake distributed %s changed an existing delete snapshot", operation_name);
		}
	}
	if (file.format == DeleteFileFormat::PARQUET) {
		for (idx_t index = 0; index < actual.rows.size(); index++) {
			auto existing = existing_snapshots.find(actual.rows[index]);
			if (existing == existing_snapshots.end() && actual.snapshots[index] != new_delete_snapshot) {
				throw InvalidInputException("DuckLake distributed %s returned an invalid new delete snapshot",
				                            operation_name);
			}
		}
	}
}

vector<DuckLakeDeleteFile> BuildDuckLakeDistributedDeleteFiles(
    ClientContext &context, const vector<DuckLakeFileListExtendedEntry> &source_files, const string &worker_bind_data,
    const vector<DuckLakeDistributedDeleteFileResult> &files, const string &operation_name) {
	auto bind = DeserializeBind(worker_bind_data);
	unordered_map<string, const DuckLakeFileListExtendedEntry *> metadata_by_path;
	for (const auto &source : source_files) {
		if (!metadata_by_path.emplace(source.file.path, &source).second) {
			throw InternalException("DuckLake distributed %s contains duplicate planned source files", operation_name);
		}
	}
	unordered_map<string, const DuckLakeDistributedRowDeltaSourceState *> bind_by_path;
	for (const auto &source : bind.delete_sources) {
		bind_by_path.emplace(source.scan_file_path, &source);
	}

	vector<DuckLakeDeleteFile> result;
	result.reserve(files.size());
	unordered_set<string> seen_paths;
	for (const auto &file : files) {
		auto metadata = metadata_by_path.find(file.data_file_path);
		auto source = bind_by_path.find(file.data_file_path);
		if (metadata == metadata_by_path.end() || source == bind_by_path.end() ||
		    !seen_paths.insert(file.data_file_path).second) {
			throw InvalidInputException(
			    "DuckLake distributed %s returned a delete for an unplanned or duplicate source file", operation_name);
		}
		ValidatedDeleteRows actual;
		if (file.format == DeleteFileFormat::PARQUET) {
			actual = ValidatePositionDeleteArtifact(context, file, *source->second);
		} else {
			actual = ValidateDeletionVectorArtifact(context, file, *source->second);
		}
		ValidateDeleteRows(file, *source->second, actual, bind.new_delete_snapshot, operation_name);

		DuckLakeDeleteFile delete_file;
		delete_file.data_file_id = metadata->second->file_id;
		delete_file.data_file_path = file.data_file_path;
		delete_file.file_name = file.delete_file_path;
		delete_file.format = file.format;
		delete_file.delete_count = file.delete_count;
		delete_file.file_size_bytes = file.file_size_bytes;
		delete_file.footer_size = file.footer_size_bytes;
		delete_file.source = DeleteFileSource::REGULAR;
		if (file.format == DeleteFileFormat::PARQUET) {
			delete_file.begin_snapshot = file.begin_snapshot;
			delete_file.max_snapshot = file.max_snapshot;
		}
		if (source->second->has_delete_file) {
			delete_file.overwrites_existing_delete = true;
			delete_file.overwritten_delete_file.delete_file_id = metadata->second->delete_file_id;
			delete_file.overwritten_delete_file.path = metadata->second->delete_file.path;
		}
		result.push_back(std::move(delete_file));
	}
	return result;
}

void CleanupDuckLakeDistributedRowDelta(ClientContext &context, const string &data_path, const string &artifact_path,
                                        const unordered_set<string> *paths_to_keep) {
	if (data_path.empty() || artifact_path.empty()) {
		return;
	}
	ValidateDuckLakeDistributedArtifactPath(context, data_path, artifact_path);
	auto &file_system = FileSystem::GetFileSystem(context);
	if (!paths_to_keep || paths_to_keep->empty()) {
		distributed::RemoveDistributedCopyDirectoryTree(file_system, artifact_path);
		return;
	}

	unordered_set<string> canonical_paths_to_keep;
	canonical_paths_to_keep.reserve(paths_to_keep->size());
	for (const auto &path : *paths_to_keep) {
		canonical_paths_to_keep.insert(
		    CanonicalDuckLakeMutationPath(file_system, path, "retained row-mutation artifact"));
	}
	vector<string> files;
	distributed::ListDistributedCopyFilesRecursive(file_system, artifact_path, files);
	for (const auto &file : files) {
		if (canonical_paths_to_keep.find(CanonicalDuckLakeMutationPath(
		        file_system, file, "listed row-mutation artifact")) == canonical_paths_to_keep.end()) {
			file_system.TryRemoveFile(file);
		}
	}

	vector<string> remaining_files;
	distributed::ListDistributedCopyFilesRecursive(file_system, artifact_path, remaining_files);
	for (const auto &file : remaining_files) {
		if (canonical_paths_to_keep.find(CanonicalDuckLakeMutationPath(
		        file_system, file, "remaining row-mutation artifact")) == canonical_paths_to_keep.end()) {
			throw IOException("DuckLake distributed row mutation cleanup left artifact '%s'", file);
		}
	}
}

} // namespace duckdb
