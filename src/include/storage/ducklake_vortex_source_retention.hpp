#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/value.hpp"
#include <functional>

namespace duckdb {
class ClientContext;
class DuckLakeTransaction;
class StorageLock;

struct DuckLakeVortexSourceGCResult {
	idx_t expired = 0, files = 0, bytes = 0;
	bool blocked = false;
};

class DuckLakeVortexSourceRetention {
public:
	static StorageLock &TransactionGate();
	static bool IsCollector(ClientContext &context);
	static void Initialize(DuckLakeTransaction &transaction);
	static void Configure(ClientContext &context, const string &catalog, bool enabled, idx_t keep_snapshots,
	                      idx_t min_age_seconds);
	static bool WorkReady(DuckLakeTransaction &transaction, idx_t max_bytes);
	static vector<vector<Value>> Status(ClientContext &context, const string &catalog);
	static DuckLakeVortexSourceGCResult Collect(ClientContext &context, const string &catalog, idx_t max_objects,
	                                            idx_t max_bytes,
	                                            const std::function<void(const string &)> &observe = nullptr);
};
} // namespace duckdb
