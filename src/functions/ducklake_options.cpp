#include "functions/ducklake_table_functions.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_metadata_manager.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

namespace duckdb {

struct DuckLakeOptionMetadata {
	const char *name;
	const char *description;
};

static constexpr DuckLakeOptionMetadata DUCKLAKE_OPTIONS[] = {
    {"data_inlining_row_limit", "Maximum amount of rows to inline in a single insert"},
    {"parquet_compression",
     "Compression algorithm for Parquet files (uncompressed, snappy, gzip, zstd, brotli, lz4, lz4_raw)"},
    {"parquet_version", "Parquet format version (1 or 2)"},
    {"parquet_compression_level", "Compression level for Parquet files"},
    {"parquet_row_group_size", "Number of rows per row group in Parquet files"},
    {"parquet_row_group_size_bytes", "Number of bytes per row group in Parquet files"},
    {"hive_file_pattern", "If partitioned data should be written in a hive-like folder structure"},
    {"target_file_size", "The target data file size for insertion and compaction operations"},
    {"version", "DuckLake format version"},
    {"created_by", "Tool used to write the DuckLake"},
    {"data_path", "Path to data files"},
    {"require_commit_message", "If an explicit commit message is required for a snapshot commit."},
    {"rewrite_delete_threshold", "A threshold that determines the minimum amount of data that must be "
                                 "removed from a file before a rewrite is warranted. From 0 - 1."},
    {"delete_older_than", "How old unused files must be to be removed by the 'ducklake_delete_orphaned_files' and "
                          "'ducklake_cleanup_old_files' cleanup functions."},
    {"expire_older_than", "How old snapshots must be, by default, to be expired by: 'ducklake_expire_snapshots'"},
    {"auto_compact", "Pre-defined schema used as a default value for the following compaction functions "
                     "'ducklake_flush_inlined_data','ducklake_merge_adjacent_files', "
                     "'ducklake_rewrite_data_files', 'ducklake_delete_orphaned_files'"},
    {"encrypted", "Whether or not to encrypt Parquet files written to the data path"},
    {"per_thread_output", "Whether to create separate output files per thread during parallel insertion"},
    {"write_deletion_vectors", "[EXPERIMENTAL - do not use outside testing] Whether to write Iceberg V3 deletion "
                               "vectors (puffin) instead of positional delete files (parquet)"},
    {"sort_on_insert", "Whether to sort data on INSERT according to SET SORTED BY (default: true)"},
};

struct DuckLakeOptionInfo {
	string option_name;
	Value description;
	string value;
	string scope;
	string scope_entry;
};

static Value GetOptionDescription(const string &option_name) {
	for (auto &opt : DUCKLAKE_OPTIONS) {
		if (StringUtil::CIEquals(opt.name, option_name)) {
			return opt.description;
		}
	}
	return Value();
}

static vector<DuckLakeOptionInfo> LoadOptions(ClientContext &context, Catalog &catalog) {
	auto &transaction = DuckLakeTransaction::Get(context, catalog);
	auto &ducklake_catalog = catalog.Cast<DuckLakeCatalog>();
	auto &metadata_manager = transaction.GetMetadataManager();

	vector<DuckLakeOptionInfo> options;
	auto metadata = metadata_manager.LoadDuckLake();

	// Global options
	for (auto &tag : metadata.tags) {
		DuckLakeOptionInfo option_info;
		option_info.option_name = tag.key;
		option_info.value = tag.value;
		option_info.description = GetOptionDescription(tag.key);
		option_info.scope = "GLOBAL";
		options.push_back(std::move(option_info));
	}

	auto snapshot = transaction.GetSnapshot();

	// Schema options
	for (auto &schema_setting : metadata.schema_settings) {
		DuckLakeOptionInfo option_info;
		option_info.option_name = schema_setting.tag.key;
		option_info.value = schema_setting.tag.value;
		option_info.description = GetOptionDescription(schema_setting.tag.key);
		option_info.scope = "SCHEMA";
		auto schema_entry = ducklake_catalog.GetEntryById(transaction, snapshot, schema_setting.schema_id);
		if (schema_entry) {
			option_info.scope_entry = schema_entry->name;
		}
		options.push_back(std::move(option_info));
	}

	// Table options
	for (auto &table_setting : metadata.table_settings) {
		DuckLakeOptionInfo option_info;
		option_info.option_name = table_setting.tag.key;
		option_info.value = table_setting.tag.value;
		option_info.description = GetOptionDescription(table_setting.tag.key);
		option_info.scope = "TABLE";
		auto table_entry = ducklake_catalog.GetEntryById(transaction, snapshot, table_setting.table_id);
		if (table_entry) {
			auto &table_catalog_entry = table_entry->Cast<TableCatalogEntry>();
			option_info.scope_entry = table_catalog_entry.ParentSchema().name + "." + table_entry->name;
		}
		options.push_back(std::move(option_info));
	}

	std::sort(options.begin(), options.end(),
	          [](const DuckLakeOptionInfo &a, const DuckLakeOptionInfo &b) { return a.option_name < b.option_name; });
	return options;
}

static unique_ptr<FunctionData> DuckLakeOptionsBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	auto &catalog = DuckLakeBaseMetadataFunction::GetCatalog(context, input.inputs[0]);

	names.emplace_back("option_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("description");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("value");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("scope");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("scope_entry");
	return_types.emplace_back(LogicalType::VARCHAR);

	auto result = make_uniq<MetadataBindData>();
	for (auto &option : LoadOptions(context, catalog)) {
		result->rows.push_back({Value(option.option_name), option.description, Value(option.value), Value(option.scope),
		                        option.scope_entry.empty() ? Value(LogicalType::VARCHAR) : Value(option.scope_entry)});
	}
	return std::move(result);
}

DuckLakeOptionsFunction::DuckLakeOptionsFunction()
    : DuckLakeBaseMetadataFunction("ducklake_options", DuckLakeOptionsBind) {
}

} // namespace duckdb
