#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/storage/storage_lock.hpp"

#include <functional>

namespace duckdb {

class ExtensionLoader;
class StorageLock;
class DuckLakeTableEntry;
void RegisterDuckLakeVortexReadView(ExtensionLoader &loader);

struct DuckLakeVortexViewOptions {
	string catalog;
	string schema;
	string table;
	uint32_t dimension;
	idx_t max_source_bytes;
	idx_t max_rows;
	string index_name;
	string expected_table_uuid;
	optional_idx expected_field_id;
	bool unindexed_only;
	vector<string> merge_generations;
};

struct DuckLakeVortexSourceBuffer {
	idx_t file_id;
	string bytes;
};

struct DuckLakeVortexReadView {
	static StorageLock &AdmissionLock();
	static void ValidateFields(DuckLakeTableEntry &table);
	idx_t snapshot_id;
	idx_t source_bytes = 0;
	idx_t visible_rows = 0;
	idx_t inline_rows = 0;
	string admission_json;
	string rows_json;
	string managed_data_path;
	idx_t captured_business_rows = 0;
	vector<DuckLakeVortexSourceBuffer> sources;
	unique_ptr<StorageLockKey> generation_guard;

	static DuckLakeVortexReadView Acquire(Connection &reader, const DuckLakeVortexViewOptions &options,
	                                      const std::function<void(const string &)> &observe = nullptr,
	                                      optional_ptr<ClientContext> owner = nullptr);
	static DuckLakeVortexReadView AcquireQuery(Connection &reader, const DuckLakeVortexViewOptions &options,
	                                           const std::function<void(const string &)> &observe = nullptr,
	                                           optional_ptr<ClientContext> owner = nullptr);
	static DuckLakeVortexReadView AcquireBuild(Connection &reader, const DuckLakeVortexViewOptions &options,
	                                           const std::function<void(const string &)> &observe = nullptr,
	                                           optional_ptr<ClientContext> owner = nullptr);
};

} // namespace duckdb
