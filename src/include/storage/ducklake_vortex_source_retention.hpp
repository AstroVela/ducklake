#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/value.hpp"
#include <functional>

namespace duckdb {
class ClientContext;
class Connection;
class DuckLakeCatalog;
class DuckLakeTransaction;
class DuckLakeVortexCollectionLock;
class DuckLakeVortexMaintenanceProfile;
struct DuckLakeVortexCatalogIdentity;
struct DuckLakeFileForCleanup;
enum class CleanupType;

struct DuckLakeVortexSourceGCResult {
	idx_t expired = 0, files = 0, bytes = 0;
	bool blocked = false;
};

class DuckLakeVortexSourceRetention {
public:
	static DuckLakeVortexCollectionLock &TransactionGate();
	static bool IsCollector(ClientContext &context);
	static void Initialize(DuckLakeTransaction &transaction);
	static bool IsInitialized(Connection &connection, const DuckLakeCatalog &catalog);
	static void ProtectCleanupFiles(DuckLakeTransaction &transaction, CleanupType type, const string &filter,
	                                vector<DuckLakeFileForCleanup> &files);
	static void Configure(ClientContext &context, const string &catalog, bool enabled, idx_t keep_snapshots,
	                      idx_t min_age_seconds);
	static bool WorkReady(DuckLakeTransaction &transaction, idx_t max_bytes,
	                      const std::function<void()> &on_transaction_end = nullptr,
	                      optional_ptr<bool> blocked_by_transactions = nullptr);
	static vector<vector<Value>> Status(ClientContext &context, const string &catalog);
	static DuckLakeVortexSourceGCResult Collect(ClientContext &context, const string &catalog, idx_t max_objects,
	                                            idx_t max_bytes,
	                                            const std::function<void(const string &)> &observe = nullptr,
	                                            optional_ptr<const DuckLakeVortexCatalogIdentity> identity = nullptr,
	                                            DuckLakeVortexMaintenanceProfile *profile = nullptr,
	                                            const std::function<void()> &on_transaction_end = nullptr);
};
} // namespace duckdb
