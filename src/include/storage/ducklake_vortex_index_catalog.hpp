#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/client_context_state.hpp"
#include <functional>

namespace duckdb {
class DuckLakeTransaction;
class DuckLakeTableEntry;
class ExtensionLoader;
class ClientContext;
struct DuckLakeSnapshot;
struct DuckLakeVortexRewriteState;

class DuckLakeVortexCollectionLock {
	struct State;

public:
	class Key {
	public:
		Key(shared_ptr<State> state, bool exclusive);
		~Key();
		Key(const Key &) = delete;
		Key &operator=(const Key &) = delete;

	private:
		shared_ptr<State> state;
		bool exclusive;
	};

	DuckLakeVortexCollectionLock();
	unique_ptr<Key> GetSharedLock(const std::function<void()> &check);
	unique_ptr<Key> TryGetSharedLock();
	idx_t SharedWaitCount() const;
	unique_ptr<Key> GetExclusiveLock(const std::function<void()> &check);
	unique_ptr<Key> TryGetExclusiveLock();

private:
	shared_ptr<State> state;
};

enum class DuckLakeVortexIndexOperation { REPLACE, APPEND, MERGE };

struct DuckLakeVortexCatalogIdentity {
	idx_t lake_oid;
	idx_t metadata_oid;
};

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
	static const char *RewriteSourcePrefix() {
		return "ducklake-vortex-rewrite-";
	}
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
	void ValidateCoverage(const vector<DuckLakeVortexIndexFile> &current) const;
	vector<DuckLakeVortexIndexFile> SelectedFiles(const vector<string> &generations) const;
	vector<string> SmallestSegments() const;
	static DuckLakeVortexCollectionLock &PublicationLock();
	static DuckLakeVortexCollectionLock &CollectionLock();
	static DuckLakeVortexCatalogIdentity ResolveCatalog(ClientContext &context, const string &name, bool write,
	                                                    const char *operation);
	static void CheckCatalogIdentity(DuckLakeTransaction &transaction, const DuckLakeVortexCatalogIdentity &identity);
	static DuckLakeTransaction &Begin(Connection &owner, const string &catalog,
	                                  optional_ptr<const DuckLakeVortexCatalogIdentity> identity = nullptr);
	static void Rollback(Connection &owner);
	static void Initialized(DuckLakeTransaction &transaction);
	static unique_ptr<QueryResult> Query(DuckLakeTransaction &transaction, const string &sql);
	static void Initialize(Connection &owner, const string &catalog, ClientContext &caller);
	static string CollectibleGenerationPredicate(idx_t max_artifact_bytes);
	static DuckLakeVortexGCResult Collect(ClientContext &context, const string &catalog, idx_t max_generations,
	                                      idx_t max_artifact_bytes,
	                                      const std::function<void(const string &)> &observe = nullptr,
	                                      optional_ptr<const DuckLakeVortexCatalogIdentity> identity = nullptr);
	static vector<vector<Value>> GenerationStatus(Connection &owner, const string &catalog);
	static vector<vector<Value>> BuildStatus(Connection &owner, const string &catalog);
	static idx_t RecoverBuilds(ClientContext &context, const string &catalog, idx_t max_builds,
	                           const std::function<void(const string &)> &observe = nullptr,
	                           optional_ptr<const DuckLakeVortexCatalogIdentity> identity = nullptr);
	static DuckLakeVortexBuildGCResult
	CollectBuilds(ClientContext &context, const string &catalog, idx_t max_builds, idx_t max_bytes,
	              const std::function<void(const string &)> &observe = nullptr,
	              optional_ptr<const DuckLakeVortexCatalogIdentity> identity = nullptr);
	static DuckLakeVortexIndex ParseSegments(const string &reference, const string &table_uuid, idx_t field_id);
	static DuckLakeVortexIndex Lookup(DuckLakeTransaction &transaction, DuckLakeTableEntry &table, const string &name);
	static DuckLakeVortexIndex Publish(Connection &owner, const string &catalog, const string &schema,
	                                   const string &table, const string &name, const string &reference,
	                                   idx_t expected_revision, ClientContext &caller,
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
	                                   const std::function<void(const string &)> &observe = nullptr);
	static void CheckRewriteCommit(ClientContext &context, DuckLakeTransaction &transaction);
	static void PublishRewrite(ClientContext &context, DuckLakeTransaction &transaction,
	                           const DuckLakeSnapshot &snapshot);
	static string StageRewriteInput(DuckLakeVortexRewriteState &plan, idx_t file_id, const string &uri, idx_t rows);
};

struct DuckLakeVortexRewriteState : public ClientContextState {
	static shared_ptr<DuckLakeVortexRewriteState> Get(ClientContext &context);
	string catalog, schema, table, table_uuid, build_id, selected, reference, workspace, private_path, source_root,
	    new_path;
	optional_ptr<ClientContext> caller;
	DuckLakeVortexCatalogIdentity identity;
	idx_t table_id = 0, snapshot_id = 0, schema_version = 0, new_file_id = 0;
	idx_t copy_plans = 0, new_rows = 0, new_bytes = 0;
	idx_t staged_source_bytes = 0;
	vector<idx_t> replaced;
	vector<DuckLakeVortexIndexFile> selected_files;
	DuckLakeVortexIndex before, candidate;
	std::function<void(const string &)> check;
};

void RegisterDuckLakeVortexIndexCatalog(ExtensionLoader &loader);
} // namespace duckdb
