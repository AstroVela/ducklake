#pragma once

#include "duckdb/main/connection.hpp"
#include <functional>

namespace duckdb {
class ExtensionLoader;

struct DuckLakeVortexWriteResult {
	string table_uuid, request_id, payload_hash;
	idx_t row_count, snapshot_id;
};

struct DuckLakeVortexWriteBatch {
	static LogicalType RowsType();
	static void Initialize(Connection &owner, const string &catalog);
	static DuckLakeVortexWriteResult Append(Connection &owner, const string &catalog, const string &schema,
	                                        const string &table, const string &index, const string &request_id,
	                                        const Value &rows, const std::function<void(const string &)> &observe = {},
	                                        optional_ptr<ClientContext> caller = nullptr);
};

void RegisterDuckLakeVortexWrite(ExtensionLoader &loader);
} // namespace duckdb
