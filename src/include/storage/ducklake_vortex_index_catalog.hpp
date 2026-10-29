#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/main/connection.hpp"
#include <functional>

namespace duckdb {
class DuckLakeTransaction;
class DuckLakeTableEntry;
class ExtensionLoader;
class StorageLock;
class ClientContext;

struct DuckLakeVortexIndexFile {
	idx_t id;
	string uri;
	string version;
	idx_t row_count;
};

struct DuckLakeVortexIndex {
	string name;
	idx_t revision;
	uint32_t field_id;
	uint32_t dimension;
	string root;
	string generation;
	string reference;
	vector<DuckLakeVortexIndexFile> files;

	string ToJSON() const;
	void ValidateCoverage(const vector<DuckLakeVortexIndexFile> &current) const;
	static StorageLock &PublicationLock();
	static DuckLakeTransaction &Begin(Connection &owner, const string &catalog);
	static void Rollback(Connection &owner);
	static void Initialized(DuckLakeTransaction &transaction);
	static unique_ptr<QueryResult> Query(DuckLakeTransaction &transaction, const string &sql);
	static void Initialize(Connection &owner, const string &catalog);
	static DuckLakeVortexIndex Lookup(DuckLakeTransaction &transaction, DuckLakeTableEntry &table, const string &name);
	static DuckLakeVortexIndex Publish(Connection &owner, const string &catalog, const string &schema,
	                                   const string &table, const string &name, const string &reference,
	                                   idx_t expected_revision,
	                                   const std::function<void(const string &)> &observe = nullptr);
	static DuckLakeVortexIndex BuildAndPublish(ClientContext &context, const string &catalog, const string &schema,
	                                           const string &table, const string &name, const string &reference,
	                                           uint32_t dimension, idx_t expected_revision,
	                                           const std::function<void(const string &)> &observe = nullptr,
	                                           const string &expected_table_uuid = "",
	                                           optional_idx expected_field_id = optional_idx());
};

void RegisterDuckLakeVortexIndexCatalog(ExtensionLoader &loader);
} // namespace duckdb
