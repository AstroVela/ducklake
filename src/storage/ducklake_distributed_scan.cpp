#include "storage/ducklake_distributed_scan.hpp"

#include "common/ducklake_name_map.hpp"
#include "storage/ducklake_delete_filter.hpp"
#include "storage/ducklake_metadata_info.hpp"
#include "storage/ducklake_multi_file_list.hpp"
#include "storage/ducklake_multi_file_reader.hpp"
#include "storage/ducklake_scan.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_transaction.hpp"

#include "duckdb/common/hive_partitioning.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/multi_file/multi_file_function.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/distributed_table_function.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"

#include "parquet_multi_file_info.hpp"

#include <algorithm>

namespace duckdb {

namespace {

static constexpr uint32_t DUCKLAKE_DISTRIBUTED_SCAN_PROTOCOL_VERSION = 2;
static constexpr const char *DUCKLAKE_DISTRIBUTED_SCAN_SPLIT_CODEC = "ducklake.scan-file";

enum class DuckLakeDistributedBindKind : uint8_t { PLANNED = 1, WORKER = 2 };
enum class DuckLakeDistributedScanPhase : uint8_t { PLANNED, WORKER_TEMPLATE, WORKER_ASSIGNED };

struct DuckLakeDistributedScanIdentity {
	string table_name;
	string table_uuid;
	idx_t table_id = DConstants::INVALID_INDEX;
	DuckLakeSnapshot snapshot;
	string split_set_id;
};

struct DuckLakeDistributedNameMapEntry {
	string source_name;
	idx_t target_field_id = DConstants::INVALID_INDEX;
	bool hive_partition = false;
	vector<DuckLakeDistributedNameMapEntry> children;
};

struct DuckLakeDistributedNameMap {
	idx_t mapping_id = DConstants::INVALID_INDEX;
	idx_t table_id = DConstants::INVALID_INDEX;
	vector<DuckLakeDistributedNameMapEntry> entries;
};

struct DuckLakeDistributedScanSplitEnvelope {
	string split_set_id;
	string split_id;
	idx_t coordinator_file_index = DConstants::INVALID_INDEX;
	string table_uuid;
	idx_t table_id = DConstants::INVALID_INDEX;
	DuckLakeSnapshot snapshot;
	DuckLakeFileListEntry file;
	DuckLakeFileListExtendedEntry mutation_file;
	unique_ptr<DuckLakeDistributedNameMap> name_map;
};

static bool IsCanonicalSplitId(const string &split_id) {
	if (split_id.empty() || (split_id.size() > 1 && split_id[0] == '0')) {
		return false;
	}
	for (auto character : split_id) {
		if (character < '0' || character > '9') {
			return false;
		}
	}
	return true;
}

static bool IsStrictUUID(const string &value) {
	hugeint_t parsed;
	return BaseUUID::FromString(value, parsed, true);
}

static void ValidateSnapshot(const DuckLakeSnapshot &snapshot) {
	if (snapshot.snapshot_id == DConstants::INVALID_INDEX || snapshot.schema_version == DConstants::INVALID_INDEX ||
	    snapshot.next_catalog_id == DConstants::INVALID_INDEX || snapshot.next_file_id == DConstants::INVALID_INDEX) {
		throw SerializationException("Distributed DuckLake scan has an incomplete snapshot identity");
	}
}

static void ValidateIdentity(const DuckLakeDistributedScanIdentity &identity) {
	if (identity.table_name.empty() || !IsStrictUUID(identity.table_uuid) ||
	    identity.table_id == DConstants::INVALID_INDEX ||
	    identity.table_id >= DuckLakeConstants::TRANSACTION_LOCAL_ID_START || !IsStrictUUID(identity.split_set_id)) {
		throw SerializationException("Distributed DuckLake scan has an incomplete table identity");
	}
	ValidateSnapshot(identity.snapshot);
}

static bool IdentityMatches(const DuckLakeDistributedScanIdentity &identity,
                            const DuckLakeDistributedScanSplitEnvelope &split) {
	return identity.split_set_id == split.split_set_id && identity.table_uuid == split.table_uuid &&
	       identity.table_id == split.table_id && identity.snapshot.snapshot_id == split.snapshot.snapshot_id &&
	       identity.snapshot.schema_version == split.snapshot.schema_version &&
	       identity.snapshot.next_catalog_id == split.snapshot.next_catalog_id &&
	       identity.snapshot.next_file_id == split.snapshot.next_file_id;
}

static void SerializeOptionalIndex(Serializer &serializer, field_id_t has_field, const char *has_name,
                                   field_id_t value_field, const char *value_name, optional_idx value) {
	serializer.WriteProperty(has_field, has_name, value.IsValid());
	serializer.WriteProperty(value_field, value_name, value.IsValid() ? value.GetIndex() : 0);
}

static optional_idx DeserializeOptionalIndex(Deserializer &deserializer, field_id_t has_field, const char *has_name,
                                             field_id_t value_field, const char *value_name) {
	auto has_value = deserializer.ReadProperty<bool>(has_field, has_name);
	auto value = deserializer.ReadProperty<idx_t>(value_field, value_name);
	if ((!has_value && value != 0) || (has_value && value == DConstants::INVALID_INDEX)) {
		throw SerializationException("Distributed DuckLake scan contains non-canonical absent state");
	}
	return has_value ? optional_idx(value) : optional_idx();
}

static void ValidateFileData(const DuckLakeFileData &file, bool allow_empty) {
	if (!file.encryption_key.empty()) {
		throw NotImplementedException(
		    "Distributed DuckLake scans do not transport file encryption keys; configure unencrypted data files");
	}
	if (file.path.empty()) {
		if (!allow_empty || file.file_size_bytes != 0 || file.footer_size.IsValid() ||
		    file.format != DeleteFileFormat::PARQUET) {
			throw SerializationException("Distributed DuckLake scan contains invalid empty file metadata");
		}
		return;
	}
	if (file.file_size_bytes == 0 || (!allow_empty && file.format != DeleteFileFormat::PARQUET)) {
		throw SerializationException("Distributed DuckLake scan contains invalid file metadata");
	}
	if (file.footer_size.IsValid() && file.footer_size.GetIndex() > file.file_size_bytes) {
		throw SerializationException("Distributed DuckLake scan contains an invalid file footer size");
	}
}

static void SerializeFileData(Serializer &serializer, const DuckLakeFileData &file, bool allow_empty) {
	ValidateFileData(file, allow_empty);
	serializer.WriteProperty(1, "path", file.path);
	serializer.WriteProperty(2, "file_size_bytes", file.file_size_bytes);
	SerializeOptionalIndex(serializer, 3, "has_footer_size", 4, "footer_size", file.footer_size);
	serializer.WriteProperty(5, "format", static_cast<uint8_t>(file.format));
}

static DuckLakeFileData DeserializeFileData(Deserializer &deserializer, bool allow_empty) {
	DuckLakeFileData result;
	result.path = deserializer.ReadProperty<string>(1, "path");
	result.file_size_bytes = deserializer.ReadProperty<idx_t>(2, "file_size_bytes");
	result.footer_size = DeserializeOptionalIndex(deserializer, 3, "has_footer_size", 4, "footer_size");
	auto format = deserializer.ReadProperty<uint8_t>(5, "format");
	if (format != static_cast<uint8_t>(DeleteFileFormat::PARQUET) &&
	    format != static_cast<uint8_t>(DeleteFileFormat::PUFFIN)) {
		throw SerializationException("Distributed DuckLake scan contains an invalid delete-file format");
	}
	result.format = static_cast<DeleteFileFormat>(format);
	ValidateFileData(result, allow_empty);
	return result;
}

static void ValidateFileEntry(const DuckLakeFileListEntry &file) {
	if (file.data_type != DuckLakeDataType::DATA_FILE || !file.file_id.IsValid() ||
	    file.file_id.index >= DuckLakeConstants::TRANSACTION_LOCAL_ID_START) {
		throw NotImplementedException("Distributed DuckLake scans require committed file-backed table data");
	}
	ValidateFileData(file.file, false);
	ValidateFileData(file.delete_file, true);
	if (file.snapshot_filter_min.IsValid() && file.snapshot_filter_max.IsValid() &&
	    file.snapshot_filter_min.GetIndex() > file.snapshot_filter_max.GetIndex()) {
		throw SerializationException("Distributed DuckLake scan contains an invalid snapshot filter range");
	}
	if (file.max_row_count.IsValid()) {
		for (auto row : file.inlined_file_deletions) {
			if (row >= file.max_row_count.GetIndex()) {
				throw SerializationException("Distributed DuckLake scan contains an out-of-range inlined deletion");
			}
		}
	}
}

static bool OptionalIndexesMatch(const optional_idx &left, const optional_idx &right) {
	return left.IsValid() == right.IsValid() && (!left.IsValid() || left.GetIndex() == right.GetIndex());
}

static bool FileDataMatches(const DuckLakeFileData &left, const DuckLakeFileData &right) {
	return left.path == right.path && left.encryption_key == right.encryption_key &&
	       left.file_size_bytes == right.file_size_bytes && OptionalIndexesMatch(left.footer_size, right.footer_size) &&
	       left.format == right.format;
}

static void ValidateMutationFileEntry(const DuckLakeFileListEntry &file,
                                      const DuckLakeFileListExtendedEntry &mutation_file) {
	auto mapping_matches = file.mapping_id.IsValid() == mutation_file.mapping_id.IsValid() &&
	                       (!file.mapping_id.IsValid() || file.mapping_id.index == mutation_file.mapping_id.index);
	if (mutation_file.data_type != DuckLakeDataType::DATA_FILE || !mutation_file.file_id.IsValid() ||
	    mutation_file.file_id.index != file.file_id.index || !FileDataMatches(mutation_file.file, file.file) ||
	    !FileDataMatches(mutation_file.delete_file, file.delete_file) ||
	    !OptionalIndexesMatch(mutation_file.row_id_start, file.row_id_start) || !mapping_matches) {
		throw SerializationException("Distributed DuckLake scan contains inconsistent mutation metadata");
	}
	if (file.delete_file.path.empty()) {
		if (mutation_file.delete_file_id.IsValid() || mutation_file.delete_file_begin_snapshot.IsValid()) {
			throw SerializationException("Distributed DuckLake scan contains unexpected delete-file metadata");
		}
		return;
	}
	if (!mutation_file.delete_file_id.IsValid() ||
	    mutation_file.delete_file_id.index >= DuckLakeConstants::TRANSACTION_LOCAL_ID_START ||
	    !mutation_file.delete_file_begin_snapshot.IsValid() ||
	    mutation_file.delete_file_begin_snapshot.GetIndex() == 0) {
		throw SerializationException("Distributed DuckLake scan contains incomplete delete-file metadata");
	}
}

static void SerializeFileEntry(Serializer &serializer, const DuckLakeFileListEntry &file) {
	ValidateFileEntry(file);
	SerializeOptionalIndex(serializer, 1, "has_data_file_id", 2, "data_file_id", file.data_file_id);
	serializer.WriteObject(3, "data_file", [&](Serializer &object) { SerializeFileData(object, file.file, false); });
	serializer.WriteObject(4, "delete_file",
	                       [&](Serializer &object) { SerializeFileData(object, file.delete_file, true); });
	SerializeOptionalIndex(serializer, 5, "has_row_id_start", 6, "row_id_start", file.row_id_start);
	SerializeOptionalIndex(serializer, 7, "has_snapshot_id", 8, "snapshot_id", file.snapshot_id);
	SerializeOptionalIndex(serializer, 9, "has_max_row_count", 10, "max_row_count", file.max_row_count);
	SerializeOptionalIndex(serializer, 11, "has_snapshot_filter_max", 12, "snapshot_filter_max",
	                       file.snapshot_filter_max);
	SerializeOptionalIndex(serializer, 13, "has_snapshot_filter_min", 14, "snapshot_filter_min",
	                       file.snapshot_filter_min);
	serializer.WriteProperty(15, "has_mapping_id", file.mapping_id.IsValid());
	serializer.WriteProperty(16, "mapping_id", file.mapping_id.IsValid() ? file.mapping_id.index : 0);
	serializer.WriteProperty(17, "data_type", static_cast<uint8_t>(file.data_type));
	serializer.WriteProperty(18, "file_id", file.file_id.index);
	vector<idx_t> inlined_deletions(file.inlined_file_deletions.begin(), file.inlined_file_deletions.end());
	serializer.WriteProperty(19, "inlined_file_deletions", inlined_deletions);

	vector<idx_t> stats_field_ids;
	vector<string> stats_min_values;
	vector<string> stats_max_values;
	stats_field_ids.reserve(file.column_min_max.size());
	for (const auto &entry : file.column_min_max) {
		stats_field_ids.push_back(entry.first);
	}
	std::sort(stats_field_ids.begin(), stats_field_ids.end());
	stats_min_values.reserve(stats_field_ids.size());
	stats_max_values.reserve(stats_field_ids.size());
	for (auto field_id : stats_field_ids) {
		auto &bounds = file.column_min_max.at(field_id);
		stats_min_values.push_back(bounds.first);
		stats_max_values.push_back(bounds.second);
	}
	serializer.WriteProperty(20, "stats_field_ids", stats_field_ids);
	serializer.WriteProperty(21, "stats_min_values", stats_min_values);
	serializer.WriteProperty(22, "stats_max_values", stats_max_values);
}

static DuckLakeFileListEntry DeserializeFileEntry(Deserializer &deserializer) {
	DuckLakeFileListEntry result;
	result.data_file_id = DeserializeOptionalIndex(deserializer, 1, "has_data_file_id", 2, "data_file_id");
	deserializer.ReadObject(3, "data_file",
	                        [&](Deserializer &object) { result.file = DeserializeFileData(object, false); });
	deserializer.ReadObject(4, "delete_file",
	                        [&](Deserializer &object) { result.delete_file = DeserializeFileData(object, true); });
	result.row_id_start = DeserializeOptionalIndex(deserializer, 5, "has_row_id_start", 6, "row_id_start");
	result.snapshot_id = DeserializeOptionalIndex(deserializer, 7, "has_snapshot_id", 8, "snapshot_id");
	result.max_row_count = DeserializeOptionalIndex(deserializer, 9, "has_max_row_count", 10, "max_row_count");
	result.snapshot_filter_max =
	    DeserializeOptionalIndex(deserializer, 11, "has_snapshot_filter_max", 12, "snapshot_filter_max");
	result.snapshot_filter_min =
	    DeserializeOptionalIndex(deserializer, 13, "has_snapshot_filter_min", 14, "snapshot_filter_min");
	auto has_mapping_id = deserializer.ReadProperty<bool>(15, "has_mapping_id");
	auto mapping_id = deserializer.ReadProperty<idx_t>(16, "mapping_id");
	if ((!has_mapping_id && mapping_id != 0) || (has_mapping_id && mapping_id == DConstants::INVALID_INDEX)) {
		throw SerializationException("Distributed DuckLake scan contains a non-canonical absent mapping id");
	}
	result.mapping_id = has_mapping_id ? MappingIndex(mapping_id) : MappingIndex();
	auto data_type = deserializer.ReadProperty<uint8_t>(17, "data_type");
	if (data_type != static_cast<uint8_t>(DuckLakeDataType::DATA_FILE)) {
		throw NotImplementedException("Distributed DuckLake scans do not support inlined table data");
	}
	result.data_type = DuckLakeDataType::DATA_FILE;
	result.file_id = DataFileIndex(deserializer.ReadProperty<idx_t>(18, "file_id"));
	auto inlined_deletions = deserializer.ReadProperty<vector<idx_t>>(19, "inlined_file_deletions");
	if (!std::is_sorted(inlined_deletions.begin(), inlined_deletions.end()) ||
	    std::adjacent_find(inlined_deletions.begin(), inlined_deletions.end()) != inlined_deletions.end()) {
		throw SerializationException("Distributed DuckLake scan contains invalid inlined deletions");
	}
	result.inlined_file_deletions.insert(inlined_deletions.begin(), inlined_deletions.end());
	auto stats_field_ids = deserializer.ReadProperty<vector<idx_t>>(20, "stats_field_ids");
	auto stats_min_values = deserializer.ReadProperty<vector<string>>(21, "stats_min_values");
	auto stats_max_values = deserializer.ReadProperty<vector<string>>(22, "stats_max_values");
	if (stats_field_ids.size() != stats_min_values.size() || stats_field_ids.size() != stats_max_values.size() ||
	    !std::is_sorted(stats_field_ids.begin(), stats_field_ids.end()) ||
	    std::adjacent_find(stats_field_ids.begin(), stats_field_ids.end()) != stats_field_ids.end() ||
	    std::find(stats_field_ids.begin(), stats_field_ids.end(), DConstants::INVALID_INDEX) != stats_field_ids.end()) {
		throw SerializationException("Distributed DuckLake scan contains invalid column statistics");
	}
	for (idx_t index = 0; index < stats_field_ids.size(); index++) {
		result.column_min_max.emplace(
		    stats_field_ids[index], make_pair(std::move(stats_min_values[index]), std::move(stats_max_values[index])));
	}
	ValidateFileEntry(result);
	return result;
}

static DuckLakeDistributedNameMapEntry ConvertNameMapEntry(const DuckLakeNameMapEntry &entry) {
	DuckLakeDistributedNameMapEntry result;
	result.source_name = entry.source_name;
	result.target_field_id = entry.target_field_id.index;
	result.hive_partition = entry.hive_partition;
	result.children.reserve(entry.child_entries.size());
	for (const auto &child : entry.child_entries) {
		result.children.push_back(ConvertNameMapEntry(*child));
	}
	return result;
}

static unique_ptr<DuckLakeDistributedNameMap> ConvertNameMap(const DuckLakeNameMap &mapping) {
	auto result = make_uniq<DuckLakeDistributedNameMap>();
	result->mapping_id = mapping.id.index;
	result->table_id = mapping.table_id.index;
	result->entries.reserve(mapping.column_maps.size());
	for (const auto &entry : mapping.column_maps) {
		result->entries.push_back(ConvertNameMapEntry(*entry));
	}
	return result;
}

static void ValidateNameMapEntries(const vector<DuckLakeDistributedNameMapEntry> &entries,
                                   unordered_set<idx_t> &all_target_field_ids) {
	unordered_set<string> source_names;
	for (const auto &entry : entries) {
		if (entry.source_name.empty() || entry.target_field_id == DConstants::INVALID_INDEX ||
		    !source_names.insert(entry.source_name).second ||
		    !all_target_field_ids.insert(entry.target_field_id).second) {
			throw SerializationException("Distributed DuckLake scan contains an invalid name mapping");
		}
		ValidateNameMapEntries(entry.children, all_target_field_ids);
	}
}

static void ValidateNameMap(const DuckLakeDistributedNameMap &mapping, idx_t expected_mapping_id,
                            idx_t expected_table_id) {
	if (mapping.mapping_id == DConstants::INVALID_INDEX || mapping.mapping_id != expected_mapping_id ||
	    mapping.table_id != expected_table_id || mapping.entries.empty()) {
		throw SerializationException("Distributed DuckLake scan contains an inconsistent name mapping");
	}
	unordered_set<idx_t> target_field_ids;
	ValidateNameMapEntries(mapping.entries, target_field_ids);
}

static void SerializeNameMapEntry(Serializer &serializer, const DuckLakeDistributedNameMapEntry &entry) {
	serializer.WriteProperty(1, "source_name", entry.source_name);
	serializer.WriteProperty(2, "target_field_id", entry.target_field_id);
	serializer.WriteProperty(3, "hive_partition", entry.hive_partition);
	serializer.WriteList(4, "children", entry.children.size(), [&](Serializer::List &list, idx_t index) {
		list.WriteObject([&](Serializer &object) { SerializeNameMapEntry(object, entry.children[index]); });
	});
}

static DuckLakeDistributedNameMapEntry DeserializeNameMapEntry(Deserializer &deserializer) {
	DuckLakeDistributedNameMapEntry result;
	result.source_name = deserializer.ReadProperty<string>(1, "source_name");
	result.target_field_id = deserializer.ReadProperty<idx_t>(2, "target_field_id");
	result.hive_partition = deserializer.ReadProperty<bool>(3, "hive_partition");
	deserializer.ReadList(4, "children", [&](Deserializer::List &list, idx_t) {
		list.ReadObject([&](Deserializer &object) { result.children.push_back(DeserializeNameMapEntry(object)); });
	});
	return result;
}

static void SerializeNameMap(Serializer &serializer, const DuckLakeDistributedNameMap &mapping) {
	serializer.WriteProperty(1, "mapping_id", mapping.mapping_id);
	serializer.WriteProperty(2, "table_id", mapping.table_id);
	serializer.WriteList(3, "entries", mapping.entries.size(), [&](Serializer::List &list, idx_t index) {
		list.WriteObject([&](Serializer &object) { SerializeNameMapEntry(object, mapping.entries[index]); });
	});
}

static DuckLakeDistributedNameMap DeserializeNameMap(Deserializer &deserializer) {
	DuckLakeDistributedNameMap result;
	result.mapping_id = deserializer.ReadProperty<idx_t>(1, "mapping_id");
	result.table_id = deserializer.ReadProperty<idx_t>(2, "table_id");
	deserializer.ReadList(3, "entries", [&](Deserializer::List &list, idx_t) {
		list.ReadObject([&](Deserializer &object) { result.entries.push_back(DeserializeNameMapEntry(object)); });
	});
	return result;
}

static string SerializeScanSplit(const DuckLakeDistributedScanIdentity &identity, idx_t coordinator_file_index,
                                 const DuckLakeFileListEntry &file, const DuckLakeFileListExtendedEntry &mutation_file,
                                 optional_ptr<const DuckLakeNameMap> native_name_map) {
	ValidateIdentity(identity);
	ValidateFileEntry(file);
	ValidateMutationFileEntry(file, mutation_file);
	if (coordinator_file_index == DConstants::INVALID_INDEX) {
		throw SerializationException("Distributed DuckLake scan has an invalid coordinator file index");
	}
	auto split_id = std::to_string(coordinator_file_index);
	unique_ptr<DuckLakeDistributedNameMap> name_map;
	if (file.mapping_id.IsValid()) {
		if (!native_name_map) {
			throw SerializationException("Distributed DuckLake scan is missing a required name mapping");
		}
		name_map = ConvertNameMap(*native_name_map);
		ValidateNameMap(*name_map, file.mapping_id.index, identity.table_id);
	} else if (native_name_map) {
		throw InternalException("Distributed DuckLake scan received an unexpected name mapping");
	}

	MemoryStream stream(Allocator::DefaultAllocator());
	BinarySerializer serializer(stream);
	serializer.Begin();
	serializer.WriteProperty(1, "protocol_version", DUCKLAKE_DISTRIBUTED_SCAN_PROTOCOL_VERSION);
	serializer.WriteProperty(2, "split_set_id", identity.split_set_id);
	serializer.WriteProperty(3, "split_id", split_id);
	serializer.WriteProperty(4, "table_uuid", identity.table_uuid);
	serializer.WriteProperty(5, "table_id", identity.table_id);
	serializer.WriteObject(6, "snapshot", [&](Serializer &object) { identity.snapshot.Serialize(object); });
	serializer.WriteObject(7, "file", [&](Serializer &object) { SerializeFileEntry(object, file); });
	serializer.WriteProperty(8, "has_name_map", name_map != nullptr);
	if (name_map) {
		serializer.WriteObject(9, "name_map", [&](Serializer &object) { SerializeNameMap(object, *name_map); });
	}
	serializer.WriteProperty(10, "coordinator_file_index", coordinator_file_index);
	serializer.WriteProperty(11, "record_count", mutation_file.row_count);
	serializer.WriteProperty(12, "has_delete_file_id", mutation_file.delete_file_id.IsValid());
	serializer.WriteProperty(13, "delete_file_id",
	                         mutation_file.delete_file_id.IsValid() ? mutation_file.delete_file_id.index : 0);
	SerializeOptionalIndex(serializer, 14, "has_delete_file_begin_snapshot", 15, "delete_file_begin_snapshot",
	                       mutation_file.delete_file_begin_snapshot);
	serializer.End();
	return string(reinterpret_cast<const char *>(stream.GetData()), stream.GetPosition());
}

static DuckLakeDistributedScanSplitEnvelope DeserializeScanSplit(const string &payload) {
	if (payload.empty()) {
		throw SerializationException("Cannot deserialize an empty distributed DuckLake scan split");
	}
	vector<data_t> buffer(payload.begin(), payload.end());
	MemoryStream stream(buffer.data(), buffer.size());
	BinaryDeserializer deserializer(stream);
	deserializer.Begin();
	auto protocol_version = deserializer.ReadProperty<uint32_t>(1, "protocol_version");
	if (protocol_version != DUCKLAKE_DISTRIBUTED_SCAN_PROTOCOL_VERSION) {
		throw SerializationException("Distributed DuckLake scan split has unsupported protocol version %u",
		                             protocol_version);
	}
	DuckLakeDistributedScanSplitEnvelope result;
	result.split_set_id = deserializer.ReadProperty<string>(2, "split_set_id");
	result.split_id = deserializer.ReadProperty<string>(3, "split_id");
	result.table_uuid = deserializer.ReadProperty<string>(4, "table_uuid");
	result.table_id = deserializer.ReadProperty<idx_t>(5, "table_id");
	deserializer.ReadObject(6, "snapshot",
	                        [&](Deserializer &object) { result.snapshot = DuckLakeSnapshot::Deserialize(object); });
	deserializer.ReadObject(7, "file", [&](Deserializer &object) { result.file = DeserializeFileEntry(object); });
	auto has_name_map = deserializer.ReadProperty<bool>(8, "has_name_map");
	if (has_name_map) {
		result.name_map = make_uniq<DuckLakeDistributedNameMap>();
		deserializer.ReadObject(9, "name_map",
		                        [&](Deserializer &object) { *result.name_map = DeserializeNameMap(object); });
	}
	result.coordinator_file_index = deserializer.ReadProperty<idx_t>(10, "coordinator_file_index");
	result.mutation_file.row_count = deserializer.ReadProperty<idx_t>(11, "record_count");
	auto has_delete_file_id = deserializer.ReadProperty<bool>(12, "has_delete_file_id");
	auto delete_file_id = deserializer.ReadProperty<idx_t>(13, "delete_file_id");
	if ((!has_delete_file_id && delete_file_id != 0) ||
	    (has_delete_file_id && delete_file_id == DConstants::INVALID_INDEX)) {
		throw SerializationException("Distributed DuckLake scan contains a non-canonical delete-file id");
	}
	result.mutation_file.delete_file_id = has_delete_file_id ? DataFileIndex(delete_file_id) : DataFileIndex();
	result.mutation_file.delete_file_begin_snapshot =
	    DeserializeOptionalIndex(deserializer, 14, "has_delete_file_begin_snapshot", 15, "delete_file_begin_snapshot");
	deserializer.End();
	if (!IsStrictUUID(result.split_set_id) || !IsCanonicalSplitId(result.split_id) ||
	    !IsStrictUUID(result.table_uuid) || result.table_id == DConstants::INVALID_INDEX ||
	    result.table_id >= DuckLakeConstants::TRANSACTION_LOCAL_ID_START ||
	    result.coordinator_file_index == DConstants::INVALID_INDEX ||
	    result.split_id != std::to_string(result.coordinator_file_index)) {
		throw SerializationException("Distributed DuckLake scan split contains invalid identity state");
	}
	ValidateSnapshot(result.snapshot);
	if (result.file.mapping_id.IsValid() != has_name_map) {
		throw SerializationException("Distributed DuckLake scan split has inconsistent name-map state");
	}
	if (result.name_map) {
		ValidateNameMap(*result.name_map, result.file.mapping_id.index, result.table_id);
	}
	result.mutation_file.file_id = result.file.file_id;
	result.mutation_file.file = result.file.file;
	result.mutation_file.delete_file = result.file.delete_file;
	result.mutation_file.row_id_start = result.file.row_id_start;
	result.mutation_file.snapshot_id = result.file.snapshot_id;
	result.mutation_file.mapping_id = result.file.mapping_id;
	result.mutation_file.data_type = result.file.data_type;
	ValidateMutationFileEntry(result.file, result.mutation_file);
	return result;
}

static void SerializeWorkerColumn(Serializer &serializer, const MultiFileColumnDefinition &column) {
	serializer.WriteProperty(1, "name", column.name);
	serializer.WriteProperty(2, "type", column.type);
	serializer.WriteProperty(3, "identifier", column.identifier);
	serializer.WriteProperty(4, "has_default", column.default_expression != nullptr);
	if (column.default_expression) {
		serializer.WriteProperty(5, "default_value", column.GetDefaultValue());
	}
	serializer.WriteList(6, "children", column.children.size(), [&](Serializer::List &list, idx_t index) {
		list.WriteObject([&](Serializer &object) { SerializeWorkerColumn(object, column.children[index]); });
	});
}

static MultiFileColumnDefinition DeserializeWorkerColumn(Deserializer &deserializer) {
	auto name = deserializer.ReadProperty<string>(1, "name");
	auto type = deserializer.ReadProperty<LogicalType>(2, "type");
	auto identifier = deserializer.ReadProperty<Value>(3, "identifier");
	auto has_default = deserializer.ReadProperty<bool>(4, "has_default");
	MultiFileColumnDefinition result(name, type);
	result.identifier = std::move(identifier);
	if (has_default) {
		auto default_value = deserializer.ReadProperty<Value>(5, "default_value");
		if (default_value.type() != type) {
			throw SerializationException("Distributed DuckLake worker column '%s' has an invalid default type", name);
		}
		result.default_expression = make_uniq<ConstantExpression>(std::move(default_value));
	}
	deserializer.ReadList(6, "children", [&](Deserializer::List &list, idx_t) {
		list.ReadObject([&](Deserializer &object) { result.children.push_back(DeserializeWorkerColumn(object)); });
	});
	if (result.name.empty() || result.type.id() == LogicalTypeId::INVALID || result.identifier.IsNull() ||
	    result.identifier.type().id() != LogicalTypeId::INTEGER) {
		throw SerializationException("Distributed DuckLake worker column has an invalid field-id mapping");
	}
	return result;
}

static void ValidateWorkerColumns(const vector<MultiFileColumnDefinition> &columns, unordered_set<int32_t> &field_ids) {
	for (const auto &column : columns) {
		if (column.name.empty() || column.type.id() == LogicalTypeId::INVALID || column.identifier.IsNull() ||
		    column.identifier.type().id() != LogicalTypeId::INTEGER) {
			throw SerializationException("Distributed DuckLake worker schema has an invalid field-id mapping");
		}
		auto field_id = column.identifier.GetValue<int32_t>();
		if (!field_ids.insert(field_id).second) {
			throw SerializationException("Distributed DuckLake worker schema contains duplicate field id %d", field_id);
		}
		ValidateWorkerColumns(column.children, field_ids);
	}
}

static OpenFileInfo CreateOpenFileInfo(const DuckLakeFileListEntry &file_entry) {
	OpenFileInfo result(file_entry.file.path);
	auto extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
	extended_info->options["file_size"] = Value::UBIGINT(file_entry.file.file_size_bytes);
	if (file_entry.file.footer_size.IsValid()) {
		extended_info->options["footer_size"] = Value::UBIGINT(file_entry.file.footer_size.GetIndex());
	}
	if (file_entry.row_id_start.IsValid()) {
		extended_info->options["row_id_start"] = Value::UBIGINT(file_entry.row_id_start.GetIndex());
	}
	if (file_entry.snapshot_id.IsValid()) {
		extended_info->options["snapshot_id"] = Value::BIGINT(NumericCast<int64_t>(file_entry.snapshot_id.GetIndex()));
	} else {
		extended_info->options["snapshot_id"] = Value(LogicalType::BIGINT);
	}
	extended_info->options["validate_external_file_cache"] = Value::BOOLEAN(false);
	extended_info->options["etag"] = Value("");
	extended_info->options["last_modified"] = Value::TIMESTAMP(timestamp_t(0));
	if (!file_entry.delete_file.path.empty() || file_entry.max_row_count.IsValid() ||
	    !file_entry.inlined_file_deletions.empty()) {
		extended_info->options["has_deletes"] = Value::BOOLEAN(true);
	}
	if (file_entry.mapping_id.IsValid()) {
		extended_info->options["mapping_id"] = Value::UBIGINT(file_entry.mapping_id.index);
	}
	result.extended_info = std::move(extended_info);
	return result;
}

class DuckLakeDistributedMultiFileList : public MultiFileList {
public:
	struct FileState {
		FileState(string payload_p, DuckLakeDistributedScanSplitEnvelope split)
		    : payload(std::move(payload_p)), split_id(std::move(split.split_id)),
		      coordinator_file_index(split.coordinator_file_index), file(std::move(split.file)),
		      mutation_file(std::move(split.mutation_file)), name_map(std::move(split.name_map)),
		      open_file(CreateOpenFileInfo(file)) {
		}

		string payload;
		string split_id;
		idx_t coordinator_file_index;
		DuckLakeFileListEntry file;
		DuckLakeFileListExtendedEntry mutation_file;
		unique_ptr<DuckLakeDistributedNameMap> name_map;
		OpenFileInfo open_file;
	};

	DuckLakeDistributedMultiFileList() = default;

	void InitializePlanned(vector<string> payloads, DuckLakeDistributedScanIdentity identity_p) {
		if (initialized) {
			throw InternalException("Distributed DuckLake scan list is already initialized");
		}
		ValidateIdentity(identity_p);
		identity = std::move(identity_p);
		files = DecodePayloads(std::move(payloads));
		phase = DuckLakeDistributedScanPhase::PLANNED;
		initialized = true;
	}

	void InitializeWorkerTemplate(DuckLakeDistributedScanIdentity identity_p) {
		if (initialized) {
			throw InternalException("Distributed DuckLake scan list is already initialized");
		}
		ValidateIdentity(identity_p);
		identity = std::move(identity_p);
		phase = DuckLakeDistributedScanPhase::WORKER_TEMPLATE;
		initialized = true;
	}

	void AssignWorkerSplits(vector<string> payloads) {
		if (!initialized || phase != DuckLakeDistributedScanPhase::WORKER_TEMPLATE) {
			throw InvalidInputException(
			    "Distributed DuckLake scan split assignment requires an unassigned worker template");
		}
		auto assigned_files = DecodePayloads(std::move(payloads));
		files = std::move(assigned_files);
		phase = DuckLakeDistributedScanPhase::WORKER_ASSIGNED;
	}

	bool IsPlanned() const {
		return initialized && phase == DuckLakeDistributedScanPhase::PLANNED;
	}

	bool IsWorkerTemplate() const {
		return initialized && phase == DuckLakeDistributedScanPhase::WORKER_TEMPLATE;
	}

	bool IsWorkerAssigned() const {
		return initialized && phase == DuckLakeDistributedScanPhase::WORKER_ASSIGNED;
	}

	const DuckLakeDistributedScanIdentity &GetIdentity() const {
		if (!initialized) {
			throw InternalException("Distributed DuckLake scan identity is unavailable");
		}
		return identity;
	}

	const string &GetSplitId(idx_t file_index) const {
		RequireFilesAvailable();
		if (file_index >= files.size()) {
			throw InternalException("Distributed DuckLake scan split index is out of bounds");
		}
		return files[file_index]->split_id;
	}

	const string &GetSplitPayload(idx_t file_index) const {
		RequireFilesAvailable();
		if (file_index >= files.size()) {
			throw InternalException("Distributed DuckLake scan split index is out of bounds");
		}
		return files[file_index]->payload;
	}

	const DuckLakeFileListEntry &GetFileEntry(idx_t file_index) const {
		RequireFilesAvailable();
		if (file_index >= files.size()) {
			throw InternalException("Distributed DuckLake scan file index is out of bounds");
		}
		return files[file_index]->file;
	}

	const DuckLakeFileListExtendedEntry &GetMutationFileEntry(idx_t file_index) const {
		RequireFilesAvailable();
		if (file_index >= files.size()) {
			throw InternalException("Distributed DuckLake mutation file index is out of bounds");
		}
		return files[file_index]->mutation_file;
	}

	idx_t GetCoordinatorFileIndex(idx_t file_index) const {
		RequireFilesAvailable();
		if (file_index >= files.size()) {
			throw InternalException("Distributed DuckLake scan file index is out of bounds");
		}
		return files[file_index]->coordinator_file_index;
	}

	optional_ptr<const DuckLakeDistributedNameMap> GetNameMapByCoordinatorFileIndex(idx_t file_index) const {
		RequireFilesAvailable();
		for (const auto &file : files) {
			if (file->coordinator_file_index == file_index) {
				return file->name_map.get();
			}
		}
		throw InternalException("Distributed DuckLake coordinator file index is unavailable");
	}

	vector<OpenFileInfo> GetAllFiles() const override {
		RequireFilesAvailable();
		vector<OpenFileInfo> result;
		result.reserve(files.size());
		for (const auto &file : files) {
			result.push_back(file->open_file);
		}
		return result;
	}

	FileExpandResult GetExpandResult() const override {
		RequireFilesAvailable();
		if (files.empty()) {
			return FileExpandResult::NO_FILES;
		}
		return files.size() == 1 ? FileExpandResult::SINGLE_FILE : FileExpandResult::MULTIPLE_FILES;
	}

	idx_t GetTotalFileCount() const override {
		RequireFilesAvailable();
		return files.size();
	}

	unique_ptr<NodeStatistics> GetCardinality(ClientContext &context) const override {
		RequireFilesAvailable();
		return nullptr;
	}

	unique_ptr<MultiFileList> Copy() const override {
		if (!initialized) {
			throw InternalException("Cannot copy an uninitialized distributed DuckLake scan list");
		}
		auto result = make_uniq<DuckLakeDistributedMultiFileList>();
		if (IsWorkerTemplate()) {
			result->InitializeWorkerTemplate(identity);
			return std::move(result);
		}
		vector<string> payloads;
		payloads.reserve(files.size());
		for (const auto &file : files) {
			payloads.push_back(file->payload);
		}
		if (IsPlanned()) {
			result->InitializePlanned(std::move(payloads), identity);
		} else {
			result->InitializeWorkerTemplate(identity);
			result->AssignWorkerSplits(std::move(payloads));
		}
		return std::move(result);
	}

protected:
	OpenFileInfo GetFile(idx_t file_index) const override {
		RequireFilesAvailable();
		if (file_index >= files.size()) {
			return OpenFileInfo();
		}
		return files[file_index]->open_file;
	}

private:
	vector<unique_ptr<FileState>> DecodePayloads(vector<string> payloads) const {
		vector<unique_ptr<FileState>> result;
		result.reserve(payloads.size());
		unordered_set<string> split_ids;
		unordered_set<string> paths;
		for (auto &payload : payloads) {
			auto split = DeserializeScanSplit(payload);
			if (!IdentityMatches(identity, split)) {
				throw InvalidInputException("Distributed DuckLake scan received a split from another planned scan");
			}
			if (!split_ids.insert(split.split_id).second) {
				throw InvalidInputException("Distributed DuckLake scan received duplicate split id '%s'",
				                            split.split_id);
			}
			if (!paths.insert(split.file.file.path).second) {
				throw InvalidInputException("Distributed DuckLake scan assigned data file '%s' more than once",
				                            split.file.file.path);
			}
			result.push_back(make_uniq<FileState>(std::move(payload), std::move(split)));
		}
		return result;
	}

	void RequireFilesAvailable() const {
		if (!initialized) {
			throw InternalException("Distributed DuckLake scan list is not initialized");
		}
		if (IsWorkerTemplate()) {
			throw InvalidInputException("Distributed DuckLake worker scan has no explicit split assignment");
		}
	}

private:
	bool initialized = false;
	DuckLakeDistributedScanPhase phase = DuckLakeDistributedScanPhase::PLANNED;
	DuckLakeDistributedScanIdentity identity;
	vector<unique_ptr<FileState>> files;
};

static bool ColumnsHaveFieldIds(const vector<MultiFileColumnDefinition> &columns) {
	for (const auto &column : columns) {
		if (column.identifier.IsNull()) {
			return false;
		}
	}
	return !columns.empty();
}

static bool TryFindColumnByFieldId(const vector<MultiFileColumnDefinition> &local_columns, int32_t field_id,
                                   MultiFileColumnDefinition *fallback_column,
                                   optional_ptr<MultiFileColumnDefinition> &global_column_reference) {
	for (auto &column : local_columns) {
		if (!column.identifier.IsNull() && column.identifier.type().id() == LogicalTypeId::INTEGER &&
		    column.identifier.GetValue<int32_t>() == field_id) {
			global_column_reference = fallback_column;
			return true;
		}
	}
	return false;
}

static void AddSnapshotFilter(BaseFileReader &reader, const ColumnIndex &column_index, const LogicalType &column_type,
                              idx_t snapshot_value, ExpressionType comparison_type) {
	auto constant = Value::UBIGINT(snapshot_value).DefaultCastAs(column_type);
	reader.filters->PushFilter(column_index, make_uniq<ConstantFilter>(comparison_type, std::move(constant)));
}

static void NormalizeListChildNames(vector<MultiFileColumnDefinition> &columns, bool parent_is_list = false) {
	for (auto &column : columns) {
		if (parent_is_list && (column.name == "array" || column.name == "element")) {
			column.name = "list";
		}
		if (!column.children.empty()) {
			NormalizeListChildNames(column.children, column.type.id() == LogicalTypeId::LIST);
		}
	}
}

static vector<MultiFileColumnDefinition> MapColumns(ClientContext &context, MultiFileReaderData &reader_data,
                                                    const vector<MultiFileColumnDefinition> &global_columns,
                                                    const vector<DuckLakeDistributedNameMapEntry> &mapping_entries,
                                                    bool parent_is_list = false) {
	unordered_map<idx_t, idx_t> field_id_map;
	for (idx_t index = 0; index < mapping_entries.size(); index++) {
		field_id_map.emplace(mapping_entries[index].target_field_id, index);
	}
	map<string, string> partitions;
	auto result = global_columns;
	for (auto &result_column : result) {
		if (result_column.identifier.IsNull()) {
			throw SerializationException("Distributed DuckLake reader schema is missing a field id");
		}
		auto field_id = result_column.identifier.GetValue<idx_t>();
		auto entry = field_id_map.find(field_id);
		if (entry == field_id_map.end()) {
			result_column.identifier = Value("__ducklake_unknown_identifier");
			continue;
		}
		auto &mapping = mapping_entries[entry->second];
		if (mapping.hive_partition) {
			result_column.identifier = Value("__ducklake_unknown_identifier");
			if (partitions.empty()) {
				partitions = HivePartitioning::Parse(reader_data.reader->file.path);
			}
			auto partition = partitions.find(mapping.source_name);
			if (partition == partitions.end()) {
				throw InvalidInputException("Column '%s' is missing from hive-partitioned file path '%s'",
				                            mapping.source_name, reader_data.reader->file.path);
			}
			auto partition_value =
			    HivePartitioning::GetValue(context, mapping.source_name, partition->second, result_column.type);
			result_column.default_expression = make_uniq<ConstantExpression>(std::move(partition_value));
			continue;
		}

		auto source_name = mapping.source_name;
		if (parent_is_list && (source_name == "array" || source_name == "element")) {
			source_name = "list";
		}
		result_column.identifier = Value(source_name);
		if (!mapping.children.empty()) {
			result_column.children = MapColumns(context, reader_data, result_column.children, mapping.children,
			                                    result_column.type.id() == LogicalTypeId::LIST);
		}
	}
	return result;
}

class DuckLakeDistributedMultiFileReader : public MultiFileReader {
public:
	DuckLakeDistributedMultiFileReader() {
		row_id_column = make_uniq<MultiFileColumnDefinition>("_ducklake_internal_row_id", LogicalType::BIGINT);
		row_id_column->identifier = Value::INTEGER(MultiFileReader::ROW_ID_FIELD_ID);
		snapshot_id_column =
		    make_uniq<MultiFileColumnDefinition>("_ducklake_internal_snapshot_id", LogicalType::BIGINT);
		snapshot_id_column->identifier = Value::INTEGER(MultiFileReader::LAST_UPDATED_SEQUENCE_NUMBER_ID);
	}

	static unique_ptr<MultiFileReader> CreateInstance(const TableFunction &function) {
		return make_uniq<DuckLakeDistributedMultiFileReader>();
	}

	shared_ptr<MultiFileList> CreateFileList(ClientContext &context, const vector<string> &paths,
	                                         const FileGlobInput &options) override {
		throw InternalException("Distributed DuckLake worker file lists must be assigned explicitly");
	}

	bool Bind(MultiFileOptions &options, MultiFileList &files, vector<LogicalType> &return_types, vector<string> &names,
	          MultiFileReaderBindData &bind_data) override {
		throw InternalException("Distributed DuckLake worker binds must be deserialized from coordinator state");
	}

	void BindOptions(MultiFileOptions &options, MultiFileList &files, vector<LogicalType> &return_types,
	                 vector<string> &names, MultiFileReaderBindData &bind_data) override {
	}

	ReaderInitializeType InitializeReader(MultiFileReaderData &reader_data, const MultiFileBindData &bind_data,
	                                      const vector<MultiFileColumnDefinition> &global_columns,
	                                      const vector<ColumnIndex> &global_column_ids,
	                                      optional_ptr<TableFilterSet> table_filters, ClientContext &context,
	                                      MultiFileGlobalState &global_state) override {
		auto &file_list = global_state.file_list.Cast<DuckLakeDistributedMultiFileList>();
		auto local_file_index = reader_data.reader->file_list_idx.GetIndex();
		auto &file_entry = file_list.GetFileEntry(local_file_index);
		auto &reader = *reader_data.reader;
		reader.file_list_idx = optional_idx(file_list.GetCoordinatorFileIndex(local_file_index));
		if (!file_entry.delete_file.path.empty() || file_entry.max_row_count.IsValid() ||
		    !file_entry.inlined_file_deletions.empty()) {
			auto delete_filter = make_uniq<DuckLakeDeleteFilter>();
			if (!file_entry.delete_file.path.empty()) {
				delete_filter->Initialize(context, file_entry.delete_file);
			}
			if (!file_entry.inlined_file_deletions.empty()) {
				DuckLakeInlinedDataDeletes inlined_deletes;
				inlined_deletes.rows = file_entry.inlined_file_deletions;
				delete_filter->Initialize(inlined_deletes);
			}
			if (file_entry.max_row_count.IsValid()) {
				delete_filter->SetMaxRowCount(file_entry.max_row_count.GetIndex());
			}
			delete_filter->SetSnapshotFilter(file_list.GetIdentity().snapshot.snapshot_id);
			reader.deletion_filter = std::move(delete_filter);
		}

		auto result = MultiFileReader::InitializeReader(reader_data, bind_data, global_columns, global_column_ids,
		                                                table_filters, context, global_state);
		if (!file_entry.snapshot_filter_max.IsValid() && !file_entry.snapshot_filter_min.IsValid()) {
			return result;
		}
		optional_idx snapshot_column;
		LogicalType snapshot_column_type;
		for (idx_t column_index = 0; column_index < reader.columns.size(); column_index++) {
			auto &column = reader.columns[column_index];
			if (column.identifier.type() == LogicalTypeId::INTEGER &&
			    IntegerValue::Get(column.identifier) == LAST_UPDATED_SEQUENCE_NUMBER_ID) {
				snapshot_column = column_index;
				snapshot_column_type = column.type;
				break;
			}
		}
		if (!snapshot_column.IsValid()) {
			throw InvalidInputException("Distributed DuckLake snapshot filter requires the internal snapshot column");
		}
		optional_idx snapshot_local_index;
		for (idx_t index = 0; index < reader.column_ids.size(); index++) {
			if (reader.column_indexes[index].GetPrimaryIndex() == snapshot_column.GetIndex()) {
				snapshot_local_index = index;
				break;
			}
		}
		if (!snapshot_local_index.IsValid()) {
			snapshot_local_index = reader.column_indexes.size();
			reader.column_indexes.emplace_back(snapshot_column.GetIndex());
			reader.column_ids.emplace_back(snapshot_column.GetIndex());
		}
		if (!reader.filters) {
			reader.filters = make_uniq<TableFilterSet>();
		}
		ColumnIndex snapshot_column_index(snapshot_local_index.GetIndex());
		if (file_entry.snapshot_filter_max.IsValid()) {
			AddSnapshotFilter(reader, snapshot_column_index, snapshot_column_type,
			                  file_entry.snapshot_filter_max.GetIndex(), ExpressionType::COMPARE_LESSTHANOREQUALTO);
		}
		if (file_entry.snapshot_filter_min.IsValid()) {
			AddSnapshotFilter(reader, snapshot_column_index, snapshot_column_type,
			                  file_entry.snapshot_filter_min.GetIndex(), ExpressionType::COMPARE_GREATERTHANOREQUALTO);
		}
		return result;
	}

	ReaderInitializeType CreateMapping(ClientContext &context, MultiFileReaderData &reader_data,
	                                   const vector<MultiFileColumnDefinition> &global_columns,
	                                   const vector<ColumnIndex> &global_column_ids,
	                                   optional_ptr<TableFilterSet> filters, MultiFileList &multi_file_list,
	                                   const MultiFileReaderBindData &bind_data,
	                                   const virtual_column_map_t &virtual_columns) override {
		NormalizeListChildNames(reader_data.reader->columns);
		auto &file_list = multi_file_list.Cast<DuckLakeDistributedMultiFileList>();
		auto coordinator_file_index = reader_data.reader->file_list_idx.GetIndex();
		auto name_map = file_list.GetNameMapByCoordinatorFileIndex(coordinator_file_index);
		if (name_map) {
			auto mapped_columns = MapColumns(context, reader_data, global_columns, name_map->entries);
			return MultiFileReader::CreateMapping(context, reader_data, mapped_columns, global_column_ids, filters,
			                                      multi_file_list, bind_data, virtual_columns,
			                                      MultiFileColumnMappingMode::BY_NAME);
		}
		if (!ColumnsHaveFieldIds(reader_data.reader->columns)) {
			vector<DuckLakeDistributedNameMapEntry> positional_mapping;
			auto count = MinValue(reader_data.reader->columns.size(), global_columns.size());
			positional_mapping.reserve(count);
			for (idx_t index = 0; index < count; index++) {
				DuckLakeDistributedNameMapEntry entry;
				entry.source_name = reader_data.reader->columns[index].name;
				entry.target_field_id = global_columns[index].identifier.GetValue<idx_t>();
				positional_mapping.push_back(std::move(entry));
			}
			auto mapped_columns = MapColumns(context, reader_data, global_columns, positional_mapping);
			return MultiFileReader::CreateMapping(context, reader_data, mapped_columns, global_column_ids, filters,
			                                      multi_file_list, bind_data, virtual_columns,
			                                      MultiFileColumnMappingMode::BY_NAME);
		}
		return MultiFileReader::CreateMapping(context, reader_data, global_columns, global_column_ids, filters,
		                                      multi_file_list, bind_data, virtual_columns);
	}

	unique_ptr<Expression>
	GetVirtualColumnExpression(ClientContext &context, MultiFileReaderData &reader_data,
	                           const vector<MultiFileColumnDefinition> &local_columns, idx_t &column_id,
	                           const LogicalType &type, MultiFileLocalIndex local_index,
	                           optional_ptr<MultiFileColumnDefinition> &global_column_reference) override {
		if (column_id == COLUMN_IDENTIFIER_ROW_ID) {
			if (TryFindColumnByFieldId(local_columns, MultiFileReader::ROW_ID_FIELD_ID, row_id_column.get(),
			                           global_column_reference)) {
				return nullptr;
			}
			if (!reader_data.file_to_be_opened.extended_info) {
				throw InternalException("Distributed DuckLake row id is missing extended file metadata");
			}
			auto &file_options = reader_data.file_to_be_opened.extended_info->options;
			auto row_id_start = file_options.find("row_id_start");
			if (row_id_start == file_options.end()) {
				throw InvalidInputException("Distributed DuckLake data file '%s' has no row-id source",
				                            reader_data.file_to_be_opened.path);
			}
			auto row_id_expression = make_uniq<BoundConstantExpression>(row_id_start->second);
			auto file_row_number = make_uniq<BoundReferenceExpression>(type, local_index.GetIndex());
			column_id = MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER;
			vector<unique_ptr<Expression>> children;
			children.push_back(std::move(row_id_expression));
			children.push_back(std::move(file_row_number));
			FunctionBinder binder(context);
			ErrorData error;
			auto expression = binder.BindScalarFunction(DEFAULT_SCHEMA, "+", std::move(children), error, true, nullptr);
			if (error.HasError()) {
				error.Throw();
			}
			return expression;
		}
		if (column_id == DuckLakeMultiFileReader::COLUMN_IDENTIFIER_SNAPSHOT_ID) {
			if (TryFindColumnByFieldId(local_columns, MultiFileReader::LAST_UPDATED_SEQUENCE_NUMBER_ID,
			                           snapshot_id_column.get(), global_column_reference)) {
				return nullptr;
			}
			if (!reader_data.file_to_be_opened.extended_info) {
				throw InternalException("Distributed DuckLake snapshot id is missing extended file metadata");
			}
			auto &file_options = reader_data.file_to_be_opened.extended_info->options;
			auto snapshot_id = file_options.find("snapshot_id");
			if (snapshot_id == file_options.end()) {
				throw InternalException("Distributed DuckLake snapshot id is missing from a data file");
			}
			return make_uniq<BoundConstantExpression>(snapshot_id->second);
		}
		return MultiFileReader::GetVirtualColumnExpression(context, reader_data, local_columns, column_id, type,
		                                                   local_index, global_column_reference);
	}

	unique_ptr<MultiFileReader> Copy() const override {
		return make_uniq<DuckLakeDistributedMultiFileReader>();
	}

private:
	unique_ptr<MultiFileColumnDefinition> row_id_column;
	unique_ptr<MultiFileColumnDefinition> snapshot_id_column;
};

static virtual_column_map_t DuckLakeDistributedVirtualColumnMap() {
	virtual_column_map_t result;
	result.insert(
	    make_pair(MultiFileReader::COLUMN_IDENTIFIER_FILENAME, TableColumn("filename", LogicalType::VARCHAR)));
	result.insert(make_pair(MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER,
	                        TableColumn("file_row_number", LogicalType::BIGINT)));
	result.insert(
	    make_pair(MultiFileReader::COLUMN_IDENTIFIER_FILE_INDEX, TableColumn("file_index", LogicalType::UBIGINT)));
	result.insert(make_pair(COLUMN_IDENTIFIER_ROW_ID, TableColumn("rowid", LogicalType::BIGINT)));
	result.insert(make_pair(DuckLakeMultiFileReader::COLUMN_IDENTIFIER_SNAPSHOT_ID,
	                        TableColumn("snapshot_id", LogicalType::BIGINT)));
	result.insert(make_pair(COLUMN_IDENTIFIER_EMPTY, TableColumn("", LogicalType::BOOLEAN)));
	return result;
}

static virtual_column_map_t DuckLakeDistributedVirtualColumns(ClientContext &context,
                                                              optional_ptr<FunctionData> bind_data) {
	auto result = DuckLakeDistributedVirtualColumnMap();
	if (bind_data) {
		auto multi_file_bind = dynamic_cast<MultiFileBindData *>(bind_data.get());
		if (multi_file_bind) {
			multi_file_bind->virtual_columns = result;
		}
	}
	return result;
}

static void SerializeIdentity(Serializer &serializer, const DuckLakeDistributedScanIdentity &identity) {
	ValidateIdentity(identity);
	serializer.WriteProperty(1, "table_name", identity.table_name);
	serializer.WriteProperty(2, "table_uuid", identity.table_uuid);
	serializer.WriteProperty(3, "table_id", identity.table_id);
	serializer.WriteObject(4, "snapshot", [&](Serializer &object) { identity.snapshot.Serialize(object); });
	serializer.WriteProperty(5, "split_set_id", identity.split_set_id);
}

static DuckLakeDistributedScanIdentity DeserializeIdentity(Deserializer &deserializer) {
	DuckLakeDistributedScanIdentity result;
	result.table_name = deserializer.ReadProperty<string>(1, "table_name");
	result.table_uuid = deserializer.ReadProperty<string>(2, "table_uuid");
	result.table_id = deserializer.ReadProperty<idx_t>(3, "table_id");
	deserializer.ReadObject(4, "snapshot",
	                        [&](Deserializer &object) { result.snapshot = DuckLakeSnapshot::Deserialize(object); });
	result.split_set_id = deserializer.ReadProperty<string>(5, "split_set_id");
	ValidateIdentity(result);
	return result;
}

class DuckLakeDistributedScanBindData : public TableFunctionData {
public:
	DuckLakeDistributedScanBindData() = default;

	DuckLakeDistributedScanBindData(const MultiFileBindData &source, DuckLakeDistributedScanIdentity identity_p)
	    : identity(std::move(identity_p)) {
		InitializePortableBindState(source);
		ValidateIdentity(identity);
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<DuckLakeDistributedScanBindData>(*this);
	}

	bool Equals(const FunctionData &other) const override {
		return this == &other;
	}

	bool SupportStatementCache() const override {
		return false;
	}

	void Serialize(Serializer &serializer) const {
		serializer.WriteProperty(1, "types", types);
		serializer.WriteProperty(2, "names", names);
		serializer.WriteProperty(3, "table_columns", table_columns);
		serializer.WriteProperty(4, "reader_bind", reader_bind);
		serializer.WriteProperty(5, "parquet_options", parquet_options);
		serializer.WriteProperty(6, "initial_file_row_groups", initial_file_row_groups);
		serializer.WriteProperty(7, "initial_file_cardinality", initial_file_cardinality);
		serializer.WriteObject(8, "identity", [&](Serializer &object) { SerializeIdentity(object, identity); });
		serializer.WriteList(9, "reader_schema", reader_bind.schema.size(), [&](Serializer::List &list, idx_t index) {
			list.WriteObject([&](Serializer &object) { SerializeWorkerColumn(object, reader_bind.schema[index]); });
		});
		serializer.WriteProperty(10, "reader_mapping", static_cast<uint8_t>(reader_bind.mapping));
		serializer.WriteProperty(11, "variant_legacy_encoding",
		                         parquet_options.parquet_options.variant_legacy_encoding);
	}

	static DuckLakeDistributedScanBindData Deserialize(Deserializer &deserializer) {
		DuckLakeDistributedScanBindData result;
		result.types = deserializer.ReadProperty<vector<LogicalType>>(1, "types");
		result.names = deserializer.ReadProperty<vector<string>>(2, "names");
		result.table_columns = deserializer.ReadProperty<vector<string>>(3, "table_columns");
		result.reader_bind = deserializer.ReadProperty<MultiFileReaderBindData>(4, "reader_bind");
		result.reader_bind.schema.clear();
		result.parquet_options = deserializer.ReadProperty<ParquetOptionsSerialization>(5, "parquet_options");
		result.initial_file_row_groups = deserializer.ReadProperty<idx_t>(6, "initial_file_row_groups");
		result.initial_file_cardinality = deserializer.ReadProperty<idx_t>(7, "initial_file_cardinality");
		deserializer.ReadObject(8, "identity",
		                        [&](Deserializer &object) { result.identity = DeserializeIdentity(object); });
		deserializer.ReadList(9, "reader_schema", [&](Deserializer::List &list, idx_t) {
			list.ReadObject(
			    [&](Deserializer &object) { result.reader_bind.schema.push_back(DeserializeWorkerColumn(object)); });
		});
		auto mapping = deserializer.ReadProperty<uint8_t>(10, "reader_mapping");
		if (mapping != static_cast<uint8_t>(MultiFileColumnMappingMode::BY_FIELD_ID)) {
			throw SerializationException("Distributed DuckLake worker bind must map columns by field id");
		}
		result.reader_bind.mapping = MultiFileColumnMappingMode::BY_FIELD_ID;
		auto variant_legacy_encoding = deserializer.ReadProperty<bool>(11, "variant_legacy_encoding");
		if (variant_legacy_encoding || result.parquet_options.parquet_options.variant_legacy_encoding) {
			throw SerializationException(
			    "Distributed DuckLake worker bind must not contain legacy Parquet VARIANT decoding");
		}
		result.ValidatePortableBindState();
		return result;
	}

	const DuckLakeDistributedScanIdentity &GetIdentity() const {
		return identity;
	}

private:
	void InitializePortableBindState(const MultiFileBindData &source) {
		if (!source.multi_file_reader || !source.file_list || !source.interface || !source.bind_data) {
			throw InvalidInputException("Distributed DuckLake scan requires complete multi-file bind state");
		}
		types = source.types;
		names = source.names;
		table_columns = source.table_columns;
		reader_bind = source.reader_bind;
		parquet_options =
		    ParquetMultiFileInfo::SerializeBindData(source, initial_file_row_groups, initial_file_cardinality);
		ValidatePortableBindState();
	}

	void ValidatePortableBindState() const {
		ValidateIdentity(identity);
		if (types.empty() || types.size() != names.size() || reader_bind.schema.empty() ||
		    reader_bind.mapping != MultiFileColumnMappingMode::BY_FIELD_ID) {
			throw SerializationException("Distributed DuckLake worker bind has incomplete schema state");
		}
		for (idx_t index = 0; index < types.size(); index++) {
			if (types[index].id() == LogicalTypeId::INVALID || names[index].empty()) {
				throw SerializationException("Distributed DuckLake worker bind contains an invalid output schema");
			}
		}
		unordered_set<int32_t> field_ids;
		ValidateWorkerColumns(reader_bind.schema, field_ids);
		if (parquet_options.parquet_options.encryption_config) {
			throw NotImplementedException("Distributed DuckLake scans do not transport Parquet encryption keys");
		}
		if (parquet_options.parquet_options.explicit_cardinality != 0) {
			throw NotImplementedException(
			    "Distributed DuckLake scans do not support Parquet explicit cardinality overrides");
		}
		if (parquet_options.parquet_options.variant_legacy_encoding) {
			throw NotImplementedException("Distributed DuckLake scans do not support legacy Parquet VARIANT decoding");
		}
	}

public:
	vector<LogicalType> types;
	vector<string> names;
	vector<string> table_columns;
	MultiFileReaderBindData reader_bind;
	ParquetOptionsSerialization parquet_options;
	idx_t initial_file_row_groups = 0;
	idx_t initial_file_cardinality = 0;
	DuckLakeDistributedScanIdentity identity;
};

static DuckLakeDistributedScanIdentity GetCoordinatorIdentity(const TableFunction &function) {
	if (!function.function_info) {
		throw SerializationException("Distributed DuckLake coordinator scan has no table identity");
	}
	auto &read_info = function.function_info->Cast<DuckLakeFunctionInfo>();
	if (read_info.scan_type != DuckLakeScanType::SCAN_TABLE || read_info.start_snapshot) {
		throw NotImplementedException("Distributed DuckLake scans only support ordinary table scans");
	}
	auto transaction = read_info.GetTransaction();
	if (read_info.table.IsTransactionLocal() || read_info.table_id.IsTransactionLocal() ||
	    transaction->HasAnyLocalChanges(read_info.table_id)) {
		throw NotImplementedException("Distributed DuckLake scans do not support transaction-local table state");
	}
	DuckLakeDistributedScanIdentity result;
	result.table_name = read_info.table_name;
	result.table_uuid = read_info.table.GetTableUUID();
	result.table_id = read_info.table_id.index;
	result.snapshot = read_info.snapshot;
	result.split_set_id = UUID::ToString(UUID::GenerateRandomUUID());
	ValidateIdentity(result);
	return result;
}

static vector<string> PlanCoordinatorSplitPayloads(const MultiFileBindData &bind_data,
                                                   const DuckLakeDistributedScanIdentity &identity,
                                                   DuckLakeFunctionInfo &read_info) {
	auto &file_list = bind_data.file_list->Cast<DuckLakeMultiFileList>();
	auto &files = file_list.GetFiles();
	if (files.size() != file_list.GetTotalFileCount()) {
		throw InternalException("Distributed DuckLake scan returned inconsistent file counts");
	}
	const auto &mutation_files = file_list.GetDistributedFilesExtended(identity.snapshot);
	if (files.size() != mutation_files.size()) {
		throw NotImplementedException(
		    "Distributed DuckLake scans do not support metadata-inlined or transaction-local source data");
	}
	unordered_map<string, const DuckLakeFileListExtendedEntry *> mutation_files_by_path;
	for (const auto &mutation_file : mutation_files) {
		if (!mutation_files_by_path.emplace(mutation_file.file.path, &mutation_file).second) {
			throw SerializationException("Distributed DuckLake scan contains duplicate mutation metadata");
		}
	}
	auto transaction = read_info.GetTransaction();
	vector<string> result;
	result.reserve(files.size());
	unordered_set<string> paths;
	for (idx_t index = 0; index < files.size(); index++) {
		auto &file = files[index];
		ValidateFileEntry(file);
		if (!paths.insert(file.file.path).second) {
			throw InvalidInputException("Distributed DuckLake scan planned data file '%s' more than once",
			                            file.file.path);
		}
		auto mutation_file = mutation_files_by_path.find(file.file.path);
		if (mutation_file == mutation_files_by_path.end()) {
			throw SerializationException("Distributed DuckLake scan is missing mutation metadata for '%s'",
			                             file.file.path);
		}
		ValidateMutationFileEntry(file, *mutation_file->second);
		optional_ptr<const DuckLakeNameMap> name_map;
		if (file.mapping_id.IsValid()) {
			name_map = transaction->GetMappingById(file.mapping_id);
		}
		result.push_back(SerializeScanSplit(identity, index, file, *mutation_file->second, name_map));
	}
	return result;
}

static const MultiFileBindData &RequireDistributedScanBindData(const TableFunctionDistributedScanInput &input) {
	if (!input.bind_data) {
		throw InvalidInputException("Distributed DuckLake scan requires table-function bind data");
	}
	auto &bind_data = input.bind_data->Cast<MultiFileBindData>();
	if (!bind_data.file_list || !dynamic_cast<const DuckLakeDistributedMultiFileList *>(bind_data.file_list.get())) {
		throw InvalidInputException("Distributed DuckLake scan requires a portable planned file list");
	}
	return bind_data;
}

static void DuckLakeDistributedScanSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data,
                                             const TableFunction &function) {
	if (!bind_data) {
		throw SerializationException("Cannot serialize empty distributed DuckLake scan bind data");
	}
	auto transport_bind = dynamic_cast<const DuckLakeDistributedScanBindData *>(bind_data.get());
	if (transport_bind) {
		serializer.WriteProperty(1, "protocol_version", DUCKLAKE_DISTRIBUTED_SCAN_PROTOCOL_VERSION);
		serializer.WriteProperty(2, "bind_kind", static_cast<uint8_t>(DuckLakeDistributedBindKind::WORKER));
		serializer.WriteObject(3, "scan_bind", [&](Serializer &object) { transport_bind->Serialize(object); });
		serializer.WriteProperty(4, "planned_splits", vector<string> {});
		return;
	}

	auto &multi_bind = bind_data->Cast<MultiFileBindData>();
	auto distributed_list = dynamic_cast<const DuckLakeDistributedMultiFileList *>(multi_bind.file_list.get());
	DuckLakeDistributedBindKind bind_kind;
	DuckLakeDistributedScanIdentity identity;
	vector<string> planned_splits;
	if (!distributed_list) {
		bind_kind = DuckLakeDistributedBindKind::PLANNED;
		identity = GetCoordinatorIdentity(function);
		auto &read_info = function.function_info->Cast<DuckLakeFunctionInfo>();
		planned_splits = PlanCoordinatorSplitPayloads(multi_bind, identity, read_info);
	} else if (distributed_list->IsPlanned()) {
		bind_kind = DuckLakeDistributedBindKind::PLANNED;
		identity = distributed_list->GetIdentity();
		auto split_count = distributed_list->GetTotalFileCount();
		planned_splits.reserve(split_count);
		for (idx_t index = 0; index < split_count; index++) {
			planned_splits.push_back(distributed_list->GetSplitPayload(index));
		}
	} else if (distributed_list->IsWorkerTemplate()) {
		bind_kind = DuckLakeDistributedBindKind::WORKER;
		identity = distributed_list->GetIdentity();
	} else {
		throw SerializationException("Assigned distributed DuckLake scan splits cannot enter a worker template");
	}

	DuckLakeDistributedScanBindData transport(multi_bind, std::move(identity));
	serializer.WriteProperty(1, "protocol_version", DUCKLAKE_DISTRIBUTED_SCAN_PROTOCOL_VERSION);
	serializer.WriteProperty(2, "bind_kind", static_cast<uint8_t>(bind_kind));
	serializer.WriteObject(3, "scan_bind", [&](Serializer &object) { transport.Serialize(object); });
	serializer.WriteProperty(4, "planned_splits", planned_splits);
}

static unique_ptr<FunctionData> DuckLakeDistributedScanDeserialize(Deserializer &deserializer,
                                                                   TableFunction &function) {
	auto &context = deserializer.Get<ClientContext &>();
	auto protocol_version = deserializer.ReadProperty<uint32_t>(1, "protocol_version");
	if (protocol_version != DUCKLAKE_DISTRIBUTED_SCAN_PROTOCOL_VERSION) {
		throw SerializationException("Distributed DuckLake scan bind has unsupported protocol version %u",
		                             protocol_version);
	}
	auto bind_kind_value = deserializer.ReadProperty<uint8_t>(2, "bind_kind");
	if (bind_kind_value != static_cast<uint8_t>(DuckLakeDistributedBindKind::PLANNED) &&
	    bind_kind_value != static_cast<uint8_t>(DuckLakeDistributedBindKind::WORKER)) {
		throw SerializationException("Distributed DuckLake scan bind has invalid kind %d", bind_kind_value);
	}
	DuckLakeDistributedScanBindData state;
	deserializer.ReadObject(
	    3, "scan_bind", [&](Deserializer &object) { state = DuckLakeDistributedScanBindData::Deserialize(object); });
	auto planned_splits = deserializer.ReadProperty<vector<string>>(4, "planned_splits");
	auto bind_kind = static_cast<DuckLakeDistributedBindKind>(bind_kind_value);
	if (bind_kind == DuckLakeDistributedBindKind::WORKER && !planned_splits.empty()) {
		throw SerializationException("Distributed DuckLake worker bind contains coordinator scan splits");
	}

	function.function_info.reset();
	function.get_multi_file_reader = DuckLakeDistributedMultiFileReader::CreateInstance;
	function.statistics = nullptr;
	function.get_bind_info = nullptr;
	function.get_virtual_columns = DuckLakeDistributedVirtualColumns;
	function.get_partition_stats = nullptr;
	function.dynamic_to_string = nullptr;

	auto multi_file_reader = MultiFileReader::Create(function);
	auto file_list = make_shared_ptr<DuckLakeDistributedMultiFileList>();
	if (bind_kind == DuckLakeDistributedBindKind::PLANNED) {
		file_list->InitializePlanned(std::move(planned_splits), state.GetIdentity());
	} else {
		file_list->InitializeWorkerTemplate(state.GetIdentity());
	}
	auto interface = make_uniq<ParquetMultiFileInfo>();
	interface->InitializeInterface(context, *multi_file_reader, *file_list);

	auto result = make_uniq<MultiFileBindData>();
	result->multi_file_reader = std::move(multi_file_reader);
	result->file_list = std::move(file_list);
	result->interface = std::move(interface);
	ParquetMultiFileInfo::DeserializeBindData(*result, std::move(state.parquet_options), state.initial_file_row_groups,
	                                          state.initial_file_cardinality);
	result->types = std::move(state.types);
	result->names = std::move(state.names);
	result->table_columns = std::move(state.table_columns);
	result->reader_bind = std::move(state.reader_bind);
	if (result->reader_bind.mapping != MultiFileColumnMappingMode::BY_FIELD_ID) {
		throw SerializationException("Distributed DuckLake worker bind must map columns by field id");
	}
	result->columns = MultiFileColumnDefinition::ColumnsFromNamesAndTypes(result->names, result->types);
	result->virtual_columns = DuckLakeDistributedVirtualColumnMap();
	result->interface->FinalizeBindData(*result);
	return std::move(result);
}

static vector<DistributedScanSplit>
DuckLakePlanDistributedScanSplits(const TableFunctionDistributedScanPlanningInput &input) {
	auto &bind_data = RequireDistributedScanBindData(input);
	auto &file_list = bind_data.file_list->Cast<DuckLakeDistributedMultiFileList>();
	if (!file_list.IsPlanned()) {
		throw InvalidInputException("Distributed DuckLake split planning requires a planned scan bind");
	}
	auto file_count = file_list.GetTotalFileCount();
	vector<DistributedScanSplit> result;
	result.reserve(file_count);
	for (idx_t index = 0; index < file_count; index++) {
		auto &file = file_list.GetFileEntry(index);
		DistributedScanSplit split;
		split.split_id = file_list.GetSplitId(index);
		split.payload = file_list.GetSplitPayload(index);
		split.estimated_bytes = optional_idx(file.file.file_size_bytes);
		result.push_back(std::move(split));
	}
	return result;
}

static unique_ptr<FunctionData> DuckLakeCreateDistributedWorkerBind(const TableFunctionDistributedScanInput &input) {
	auto &bind_data = RequireDistributedScanBindData(input);
	auto &file_list = bind_data.file_list->Cast<DuckLakeDistributedMultiFileList>();
	if (!file_list.IsPlanned()) {
		throw InvalidInputException("Distributed DuckLake worker bind requires a planned scan bind");
	}
	return make_uniq<DuckLakeDistributedScanBindData>(bind_data, file_list.GetIdentity());
}

static void DuckLakeApplyDistributedScanSplits(optional_ptr<FunctionData> worker_bind_data,
                                               const vector<DistributedScanSplit> &splits) {
	if (!worker_bind_data) {
		throw InvalidInputException("Distributed DuckLake scan requires worker bind data");
	}
	auto &bind_data = worker_bind_data->Cast<MultiFileBindData>();
	auto &file_list = bind_data.file_list->Cast<DuckLakeDistributedMultiFileList>();
	if (!file_list.IsWorkerTemplate()) {
		throw InvalidInputException("Distributed DuckLake scan splits require an unassigned worker bind");
	}
	unordered_set<string> split_ids;
	vector<string> payloads;
	payloads.reserve(splits.size());
	for (const auto &split : splits) {
		if (!IsCanonicalSplitId(split.split_id) || split.payload.empty() || !split_ids.insert(split.split_id).second) {
			throw InvalidInputException("Invalid or duplicate distributed DuckLake scan split '%s'", split.split_id);
		}
		auto envelope = DeserializeScanSplit(split.payload);
		if (!IdentityMatches(file_list.GetIdentity(), envelope) || envelope.split_id != split.split_id) {
			throw InvalidInputException(
			    "Distributed DuckLake scan split metadata does not match its worker bind identity");
		}
		if (split.estimated_cardinality.IsValid() || !split.estimated_bytes.IsValid() ||
		    split.estimated_bytes.GetIndex() != envelope.file.file.file_size_bytes) {
			throw InvalidInputException("Distributed DuckLake scan split estimates do not match its payload");
		}
		payloads.push_back(split.payload);
	}
	file_list.AssignWorkerSplits(std::move(payloads));
}

static unique_ptr<GlobalTableFunctionState> DuckLakeDistributedScanInitGlobal(ClientContext &context,
                                                                              TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<MultiFileBindData>();
	auto file_list = dynamic_cast<DuckLakeDistributedMultiFileList *>(bind_data.file_list.get());
	if (file_list && !file_list->IsWorkerAssigned()) {
		throw InvalidInputException(
		    "Distributed DuckLake scans require an explicit worker split assignment before execution");
	}
	return MultiFileFunction<ParquetMultiFileInfo>::MultiFileInitGlobal(context, input);
}

} // namespace

bool TryGetDuckLakeDistributedMutationSource(const MultiFileBindData &bind_data, const DuckLakeTableEntry &target_table,
                                             DuckLakeSnapshot &snapshot, vector<DuckLakeFileListExtendedEntry> &files) {
	if (!bind_data.file_list) {
		return false;
	}
	auto file_list = dynamic_cast<const DuckLakeDistributedMultiFileList *>(bind_data.file_list.get());
	if (!file_list) {
		return false;
	}
	if (!file_list->IsPlanned()) {
		throw InvalidInputException("DuckLake distributed mutation requires a coordinator-planned source scan");
	}
	const auto &identity = file_list->GetIdentity();
	if (identity.table_id != target_table.GetTableId().index || identity.table_uuid != target_table.GetTableUUID() ||
	    identity.table_name != target_table.name) {
		throw InvalidInputException("DuckLake distributed mutation source scan does not match its target table");
	}
	auto source_snapshot = identity.snapshot;
	vector<DuckLakeFileListExtendedEntry> source_files;
	auto file_count = file_list->GetTotalFileCount();
	source_files.reserve(file_count);
	for (idx_t index = 0; index < file_count; index++) {
		const auto &scan_file = file_list->GetFileEntry(index);
		if (!scan_file.inlined_file_deletions.empty() || scan_file.max_row_count.IsValid() ||
		    scan_file.snapshot_filter_min.IsValid() || scan_file.snapshot_filter_max.IsValid()) {
			throw NotImplementedException(
			    "Distributed DuckLake mutations do not support inlined deletes or partial file visibility");
		}
		source_files.push_back(file_list->GetMutationFileEntry(index));
	}
	snapshot = source_snapshot;
	files = std::move(source_files);
	return true;
}

void ConfigureDuckLakeDistributedScan(TableFunction &function) {
	function.serialize = DuckLakeDistributedScanSerialize;
	function.deserialize = DuckLakeDistributedScanDeserialize;
	function.init_global = DuckLakeDistributedScanInitGlobal;
	TableFunctionDistributedScanCallbacks callbacks;
	callbacks.protocol_version = DUCKLAKE_DISTRIBUTED_SCAN_PROTOCOL_VERSION;
	callbacks.split_codec = {DUCKLAKE_DISTRIBUTED_SCAN_SPLIT_CODEC, DUCKLAKE_DISTRIBUTED_SCAN_PROTOCOL_VERSION};
	callbacks.bind_data_mode = TableFunctionDistributedBindDataMode::BIND_DATA_REQUIRED;
	callbacks.plan_splits = DuckLakePlanDistributedScanSplits;
	callbacks.create_worker_bind = DuckLakeCreateDistributedWorkerBind;
	callbacks.apply_splits = DuckLakeApplyDistributedScanSplits;
	function.SetDistributedScanCallbacks(std::move(callbacks));
}

} // namespace duckdb
