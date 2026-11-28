#pragma once

#include "duckdb/main/connection.hpp"
#include "common/index.hpp"
#include "storage/ducklake_vortex_index_catalog.hpp"
#include "storage/ducklake_vortex_optimizer.hpp"
#include <functional>
#include <mutex>

namespace duckdb {
class ExtensionLoader;
class DuckLakeTransaction;
class DuckLakeCatalog;

struct DuckLakeVortexWriteResult {
	string table_uuid, request_id, payload_hash;
	idx_t row_count, snapshot_id;
	uint64_t request_epoch;
};

struct DuckLakeVortexRequestStatus {
	string table_uuid;
	uint64_t request_epoch;
	idx_t retained_requests, catalog_requests;
};

struct DuckLakeVortexRequestAdvance {
	string table_uuid;
	uint64_t previous_epoch, request_epoch;
	idx_t expired_requests;
};

struct DuckLakeVortexWriteBatchState : public ClientContextState {
	DuckLakeVortexWriteBatchState(ClientContext &caller, DuckLakeVortexCatalogIdentity identity, idx_t table_id,
	                              const string &table_uuid, const DuckLakeVortexIndex &index, bool keyed);
	static shared_ptr<DuckLakeVortexWriteBatchState> Get(ClientContext &context, const DuckLakeCatalog &catalog);
	void CheckCurrentIndex(DuckLakeTransaction &transaction) const;

	ClientContext &caller;
	DuckLakeVortexCatalogIdentity identity;
	idx_t table_id, field_id;
	string table_uuid, index_name;
	uint32_t dimension;
	bool keyed;
};

struct DuckLakeVortexTableRetirement : public ClientContextState {
	DuckLakeVortexTableRetirement(DatabaseInstance &db, DuckLakeVortexCatalogIdentity identity, string uuid,
	                              std::function<void(const string &)> check);
	static shared_ptr<DuckLakeVortexTableRetirement> Get(ClientContext &context, const DuckLakeCatalog &catalog);
	void Stage(DuckLakeTransaction &transaction) const;

	DuckLakeVortexCatalogIdentity identity;
	TableIndex table_id;
	string uuid;
	vector<DuckLakeVortexPolicyKey> optimizer_registrations;
	std::function<void(const string &)> check;

private:
	unique_ptr<DuckLakeVortexCollectionLock::Key> write_admission, publication;
	std::unique_lock<std::mutex> optimizer_metadata;
};

struct DuckLakeVortexWriteBatch {
	static LogicalType RowsType();
	static DuckLakeVortexCollectionLock &WriterLock();
	static void Initialize(Connection &owner, ClientContext &caller, const string &catalog);
	static void Manage(Connection &owner, ClientContext &caller, const string &catalog, const string &schema,
	                   const string &table, const string &index);
	static void CheckManagedWrites(DuckLakeTransaction &fresh, const set<TableIndex> &tables, ClientContext &context);
	static DuckLakeVortexRequestStatus RequestStatus(Connection &owner, ClientContext &caller, const string &catalog,
	                                                 const string &schema, const string &table, const string &index);
	static DuckLakeVortexRequestAdvance AdvanceRequestEpoch(Connection &owner, ClientContext &caller,
	                                                        const string &catalog, const string &schema,
	                                                        const string &table, const string &index,
	                                                        uint64_t expected_epoch, const string &expected_table_uuid,
	                                                        const std::function<void(const string &)> &observe = {});
	static DuckLakeVortexWriteResult Append(Connection &owner, ClientContext &caller, const string &catalog,
	                                        const string &schema, const string &table, const string &index,
	                                        const string &request_id, const Value &rows, uint64_t request_epoch,
	                                        const string &expected_table_uuid,
	                                        const std::function<void(const string &)> &observe = {});
	static DuckLakeVortexWriteResult Mutate(Connection &owner, ClientContext &caller, const string &catalog,
	                                        const string &schema, const string &table, const string &index,
	                                        const string &request_id, const Value &upserts, const Value &deletes,
	                                        uint64_t request_epoch, const string &expected_table_uuid,
	                                        const std::function<void(const string &)> &observe = {});

	static string RetireTable(Connection &owner, ClientContext &caller, const string &catalog, const string &schema,
	                          const string &table, const string &expected_table_uuid,
	                          const std::function<void(const string &)> &observe = {});

private:
	static DuckLakeVortexWriteResult Write(Connection &owner, ClientContext &caller, const string &catalog,
	                                       const string &schema, const string &table, const string &index,
	                                       const string &request_id, const Value &rows, const Value &deletes,
	                                       bool mutate, uint64_t request_epoch, const string &expected_table_uuid,
	                                       const std::function<void(const string &)> &observe);
};

void RegisterDuckLakeVortexWrite(ExtensionLoader &loader);
} // namespace duckdb
