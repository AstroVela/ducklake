#pragma once

#include "duckdb/main/connection.hpp"
#include "storage/ducklake_vortex_index_catalog.hpp"
#include <functional>

namespace duckdb {
class ExtensionLoader;

struct DuckLakeVortexWriteResult {
	string table_uuid, request_id, payload_hash;
	idx_t row_count, snapshot_id;
};

struct DuckLakeVortexWriteBatchState : public ClientContextState {
	DuckLakeVortexWriteBatchState(ClientContext &caller, DuckLakeVortexCatalogIdentity identity, idx_t table_id,
	                              const string &table_uuid, const DuckLakeVortexIndex &index);
	static shared_ptr<DuckLakeVortexWriteBatchState> Get(ClientContext &context);
	void CheckCurrentIndex(DuckLakeTransaction &transaction) const;

	ClientContext &caller;
	DuckLakeVortexCatalogIdentity identity;
	idx_t table_id, field_id;
	string table_uuid, index_name;
	uint32_t dimension;
};

struct DuckLakeVortexWriteBatch {
	static LogicalType RowsType();
	static void Initialize(Connection &owner, ClientContext &caller, const string &catalog);
	static DuckLakeVortexWriteResult Append(Connection &owner, ClientContext &caller, const string &catalog,
	                                        const string &schema, const string &table, const string &index,
	                                        const string &request_id, const Value &rows,
	                                        const std::function<void(const string &)> &observe = {});
};

void RegisterDuckLakeVortexWrite(ExtensionLoader &loader);
} // namespace duckdb
