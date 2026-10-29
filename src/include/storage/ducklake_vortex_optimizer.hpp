#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/value.hpp"
#include <functional>

namespace duckdb {
class ClientContext;
class Connection;
class ExtensionLoader;

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
};

void RegisterDuckLakeVortexOptimizer(ExtensionLoader &loader);
} // namespace duckdb
