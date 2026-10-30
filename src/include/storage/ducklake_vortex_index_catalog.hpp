#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/client_context_state.hpp"
#include <functional>

namespace duckdb {
class DuckLakeTransaction;
class DuckLakeTableEntry;
class ExtensionLoader;
class StorageLock;
class ClientContext;
struct DuckLakeSnapshot;
struct DuckLakeVortexRewriteState;

enum class DuckLakeVortexIndexOperation { REPLACE, APPEND, MERGE };
enum class DuckLakeVortexRewriteMode { INDEXED, FILES, PRUNE };

struct DuckLakeVortexGCResult {
	idx_t generations = 0, artifact_bytes = 0, remaining = 0;
};

struct DuckLakeVortexBuildGCResult {
	idx_t builds = 0, bytes = 0, remaining = 0;
};

struct DuckLakeVortexIndexFile {
	idx_t id;
	string uri;
	string version;
	idx_t row_count;
};

struct DuckLakeVortexIndexSegment {
	string root, generation, reference;
	vector<DuckLakeVortexIndexFile> files;
};

struct DuckLakeVortexIndex {
	static constexpr idx_t MAX_SEGMENTS = 3;
	static constexpr idx_t MIN_BUILD_ROWS = 65;
	string name;
	idx_t revision;
	uint32_t field_id;
	uint32_t dimension;
	string root;
	string generation;
	string reference;
	vector<DuckLakeVortexIndexFile> files;
	vector<DuckLakeVortexIndexSegment> segments;

	string ToJSON() const;
	string Publication() const;
	void ValidateCoverage(const vector<DuckLakeVortexIndexFile> &current) const;
	vector<DuckLakeVortexIndexFile> SelectedFiles(const vector<string> &generations) const;
	vector<string> SmallestSegments() const;
	static StorageLock &PublicationLock();
	static StorageLock &CollectionLock();
	static DuckLakeTransaction &Begin(Connection &owner, const string &catalog);
	static void Rollback(Connection &owner);
	static void Initialized(DuckLakeTransaction &transaction);
	static unique_ptr<QueryResult> Query(DuckLakeTransaction &transaction, const string &sql);
	static void Initialize(Connection &owner, const string &catalog);
	static DuckLakeVortexIndex Create(Connection &owner, const string &catalog, const string &schema,
	                                  const string &table, const string &name, const string &root, uint32_t dimension);
	static DuckLakeVortexGCResult Collect(ClientContext &context, const string &catalog, idx_t max_generations,
	                                      idx_t max_artifact_bytes,
	                                      const std::function<void(const string &)> &observe = nullptr);
	static vector<vector<Value>> GenerationStatus(Connection &owner, const string &catalog);
	static vector<vector<Value>> BuildStatus(Connection &owner, const string &catalog);
	static idx_t RecoverBuilds(ClientContext &context, const string &catalog, idx_t max_builds,
	                           const std::function<void(const string &)> &observe = nullptr);
	static DuckLakeVortexBuildGCResult CollectBuilds(ClientContext &context, const string &catalog, idx_t max_builds,
	                                                 idx_t max_bytes,
	                                                 const std::function<void(const string &)> &observe = nullptr);
	static DuckLakeVortexIndex ParseSegments(const string &reference, const string &table_uuid, idx_t field_id);
	static bool ReferencesSource(DuckLakeTransaction &transaction, const string &path);
	static DuckLakeVortexIndex Lookup(DuckLakeTransaction &transaction, DuckLakeTableEntry &table, const string &name);
	static bool CoversFile(DuckLakeTransaction &transaction, DuckLakeTableEntry &table, idx_t file_id);
	static void CheckFileRetirements(ClientContext &context, DuckLakeTransaction &transaction);
	static DuckLakeVortexIndex RetireEmpty(ClientContext &context, const string &catalog, const string &schema,
	                                       const string &table, const string &index, const string &generation,
	                                       idx_t expected_revision,
	                                       const std::function<void(const string &)> &observe = nullptr,
	                                       const string &publication = "");
	static DuckLakeVortexIndex Publish(Connection &owner, const string &catalog, const string &schema,
	                                   const string &table, const string &name, const string &reference,
	                                   idx_t expected_revision,
	                                   const std::function<void(const string &)> &observe = nullptr,
	                                   DuckLakeVortexIndexOperation operation = DuckLakeVortexIndexOperation::REPLACE,
	                                   const vector<string> &merge_generations = {});
	static DuckLakeVortexIndex
	BuildAndPublish(ClientContext &context, const string &catalog, const string &schema, const string &table,
	                const string &name, const string &reference, uint32_t dimension, idx_t expected_revision,
	                const std::function<void(const string &)> &observe = nullptr,
	                const string &expected_table_uuid = "", optional_idx expected_field_id = optional_idx(),
	                DuckLakeVortexIndexOperation operation = DuckLakeVortexIndexOperation::REPLACE,
	                const vector<string> &merge_generations = {});
	static DuckLakeVortexIndex Rewrite(ClientContext &context, const string &catalog, const string &schema,
	                                   const string &table, const string &index, const string &generation,
	                                   const string &reference, idx_t expected_revision,
	                                   const std::function<void(const string &)> &observe = nullptr,
	                                   DuckLakeVortexRewriteMode mode = DuckLakeVortexRewriteMode::INDEXED,
	                                   const string &publication = "");
	static void CheckRewriteCommit(ClientContext &context, DuckLakeTransaction &transaction);
	static void PublishRewrite(ClientContext &context, DuckLakeTransaction &transaction,
	                           const DuckLakeSnapshot &snapshot);
};

struct DuckLakeVortexRewriteState : public ClientContextState {
	static shared_ptr<DuckLakeVortexRewriteState> Get(ClientContext &context);
	string catalog, schema, table, table_uuid, build_id, selected, reference, workspace, private_path, source_root,
	    new_path, input_identity, output_version;
	idx_t table_id = 0, snapshot_id = 0, schema_version = 0, new_file_id = 0, commit_snapshot_id = 0;
	idx_t copy_plans = 0, new_rows = 0, new_bytes = 0;
	bool retire = false, to_f = false, prune = false;
	idx_t live_rows = 0;
	vector<idx_t> replaced;
	vector<DuckLakeVortexIndexFile> selected_files;
	DuckLakeVortexIndex before, candidate;
	std::function<void(const string &)> check;
	std::function<void(idx_t)> bind;
};

void RegisterDuckLakeVortexIndexCatalog(ExtensionLoader &loader);
} // namespace duckdb
