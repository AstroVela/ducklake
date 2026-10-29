#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/value.hpp"
#include <functional>

namespace duckdb {
class ClientContext;
class Connection;
class ExtensionLoader;
class DuckLakeTransaction;
class StorageLockKey;

struct DuckLakeVortexWriteBudget {
	idx_t inline_rows, inline_bytes, unindexed_rows, unindexed_bytes;
};

struct DuckLakeVortexOptimizerConfig {
	string catalog, schema, table, index, root;
	uint32_t dimension = 0;
	idx_t flush_rows = 0, flush_bytes = 0, index_rows = 0, index_bytes = 0;
};

class DuckLakeVortexOptimizer {
public:
	static void Initialize(Connection &owner, const string &catalog);
	static void Enable(ClientContext &context, const DuckLakeVortexOptimizerConfig &config,
	                   const std::function<void(const string &)> &observe = nullptr);
	static string Resume(ClientContext &context, const string &catalog, const string &schema, const string &table,
	                     const string &index, const std::function<void(const string &)> &observe = nullptr);
	static void Cancel(ClientContext &context, const string &catalog, const string &schema, const string &table,
	                   const string &index);
	static vector<vector<Value>> Status(ClientContext &context, const string &catalog);
	static void SetWriteBudget(ClientContext &context, const string &catalog, const string &schema, const string &table,
	                           const string &index, const DuckLakeVortexWriteBudget &budget);
	static unique_ptr<StorageLockKey> AdmitWrite(DuckLakeTransaction &transaction, ClientContext &context);
};

void RegisterDuckLakeVortexOptimizer(ExtensionLoader &loader);
} // namespace duckdb
