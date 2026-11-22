#pragma once

#include "duckdb/main/connection.hpp"
#include "common/index.hpp"
#include "storage/ducklake_vortex_index_catalog.hpp"
#include <functional>

namespace duckdb {
class ExtensionLoader;
class DuckLakeTransaction;
class DuckLakeCatalog;

struct DuckLakeVortexWriteResult {
	string table_uuid, request_id, payload_hash;
	idx_t row_count, snapshot_id;
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

struct DuckLakeVortexWriteBatch {
	static LogicalType RowsType();
	static DuckLakeVortexCollectionLock &WriterLock();
	static void Initialize(Connection &owner, ClientContext &caller, const string &catalog);
	static void Manage(Connection &owner, ClientContext &caller, const string &catalog, const string &schema,
	                   const string &table, const string &index);
	static void CheckManagedWrites(DuckLakeTransaction &fresh, const set<TableIndex> &tables, ClientContext &context);
	static DuckLakeVortexWriteResult Append(Connection &owner, ClientContext &caller, const string &catalog,
	                                        const string &schema, const string &table, const string &index,
	                                        const string &request_id, const Value &rows,
	                                        const std::function<void(const string &)> &observe = {});
	static DuckLakeVortexWriteResult Mutate(Connection &owner, ClientContext &caller, const string &catalog,
	                                        const string &schema, const string &table, const string &index,
	                                        const string &request_id, const Value &upserts, const Value &deletes,
	                                        const std::function<void(const string &)> &observe = {});

private:
	static DuckLakeVortexWriteResult Write(Connection &owner, ClientContext &caller, const string &catalog,
	                                       const string &schema, const string &table, const string &index,
	                                       const string &request_id, const Value &rows, const Value &deletes,
	                                       bool mutate, const std::function<void(const string &)> &observe);
};

void RegisterDuckLakeVortexWrite(ExtensionLoader &loader);
} // namespace duckdb
