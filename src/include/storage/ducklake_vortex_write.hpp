#pragma once

#include "duckdb/main/connection.hpp"
#include "common/index.hpp"
#include <functional>

namespace duckdb {
class ExtensionLoader;
class DuckLakeTransaction;
class StorageLockKey;

struct DuckLakeVortexWriteResult {
	string table_uuid, request_id, payload_hash;
	idx_t row_count, snapshot_id;
};

struct DuckLakeVortexWriteBatch {
	static LogicalType RowsType();
	static unique_ptr<StorageLockKey> AcquireWriter();
	static void Initialize(Connection &owner, const string &catalog);
	static void Manage(Connection &owner, const string &catalog, const string &schema, const string &table,
	                   const string &index);
	static void CheckManagedWrites(DuckLakeTransaction &fresh, const set<TableIndex> &tables, ClientContext &context);
	static DuckLakeVortexWriteResult Append(Connection &owner, const string &catalog, const string &schema,
	                                        const string &table, const string &index, const string &request_id,
	                                        const Value &rows, const std::function<void(const string &)> &observe = {},
	                                        optional_ptr<ClientContext> caller = nullptr);
	static DuckLakeVortexWriteResult Mutate(Connection &owner, const string &catalog, const string &schema,
	                                        const string &table, const string &index, const string &request_id,
	                                        const Value &upserts, const Value &deletes,
	                                        const std::function<void(const string &)> &observe = {},
	                                        optional_ptr<ClientContext> caller = nullptr);

private:
	static DuckLakeVortexWriteResult Write(Connection &owner, const string &catalog, const string &schema,
	                                       const string &table, const string &index, const string &request_id,
	                                       const Value &rows, const Value &deletes, bool mutate,
	                                       const std::function<void(const string &)> &observe,
	                                       optional_ptr<ClientContext> caller);
};

void RegisterDuckLakeVortexWrite(ExtensionLoader &loader);
} // namespace duckdb
