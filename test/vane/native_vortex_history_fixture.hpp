#pragma once

#include "native_vortex_lifecycle_fixture.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/planner/extension_callback.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/storage/database_size.hpp"

namespace duckdb {
namespace vortex_test {

inline const vector<string> &HistoryNativeTables() {
	static const vector<string> names {"ducklake_metadata",
	                                   "ducklake_snapshot",
	                                   "ducklake_snapshot_changes",
	                                   "ducklake_schema",
	                                   "ducklake_table",
	                                   "ducklake_view",
	                                   "ducklake_tag",
	                                   "ducklake_column_tag",
	                                   "ducklake_data_file",
	                                   "ducklake_file_column_stats",
	                                   "ducklake_file_variant_stats",
	                                   "ducklake_delete_file",
	                                   "ducklake_column",
	                                   "ducklake_table_stats",
	                                   "ducklake_table_column_stats",
	                                   "ducklake_partition_info",
	                                   "ducklake_partition_column",
	                                   "ducklake_file_partition_value",
	                                   "ducklake_files_scheduled_for_deletion",
	                                   "ducklake_inlined_data_tables",
	                                   "ducklake_column_mapping",
	                                   "ducklake_name_mapping",
	                                   "ducklake_schema_versions",
	                                   "ducklake_macro",
	                                   "ducklake_macro_impl",
	                                   "ducklake_macro_parameters",
	                                   "ducklake_sort_info",
	                                   "ducklake_sort_expression"};
	return names;
}

inline void HistoryRows(vortex_detail::ViewJSON &json, duckdb_yyjson::yyjson_mut_val *object, Connection &owner,
                        const string &name) {
	auto rows = LifecycleRows(json, owner,
	                          "SELECT to_json(t) FROM __ducklake_metadata_lake." +
	                              KeywordHelper::WriteQuoted(name, '"') + " t ORDER BY ALL");
	duckdb_yyjson::yyjson_mut_obj_add(
	    object, duckdb_yyjson::yyjson_mut_strncpy(json.doc.get(), name.data(), name.size()), rows);
}

// Native-only reads deliberately avoid acquiring a DuckLake transaction while the collector owns its gate.
inline string HistorySnapshot(DuckDB &database) {
	Connection owner(database);
	SQL(owner, "BEGIN");
	vortex_detail::ViewJSON json;
	auto result = json.Object(), native = json.Object(), vortex = json.Object(), inlined = json.Object();
	for (auto &name : HistoryNativeTables()) {
		HistoryRows(json, native, owner, name);
	}
	for (auto name :
	     {"ducklake_vortex_builds_v1", "ducklake_vortex_generation_refs_v1", "ducklake_vortex_generations_v1",
	      "ducklake_vortex_indexes_v1", "ducklake_vortex_keyed_tables_v1", "ducklake_vortex_maintenance_v1",
	      "ducklake_vortex_optimizer_v2", "ducklake_vortex_request_epochs_v1", "ducklake_vortex_rewrites_v1",
	      "ducklake_vortex_source_deletions_v1", "ducklake_vortex_source_retention_v1",
	      "ducklake_vortex_write_budgets_v1", "ducklake_vortex_write_requests_v2"}) {
		HistoryRows(json, vortex, owner, name);
	}
	auto tables =
	    SQL(owner, "SELECT table_name FROM __ducklake_metadata_lake.ducklake_inlined_data_tables ORDER BY ALL");
	while (auto chunk = tables->Fetch()) {
		for (idx_t i = 0; i < chunk->size(); i++) {
			auto name = StringValue::Get(chunk->GetValue(0, i));
			HistoryRows(json, inlined, owner, name);
		}
	}
	auto ids = SQL(owner, "SELECT DISTINCT table_id FROM __ducklake_metadata_lake.ducklake_table ORDER BY ALL");
	while (auto chunk = ids->Fetch()) {
		for (idx_t i = 0; i < chunk->size(); i++) {
			auto name = "ducklake_inlined_delete_" + to_string(chunk->GetValue(0, i).GetValue<idx_t>());
			if (owner.TableInfo("__ducklake_metadata_lake", "main", name)) {
				HistoryRows(json, inlined, owner, name);
			}
		}
	}
	json.Add(result, "native", native);
	json.Add(result, "vortex", vortex);
	json.Add(result, "inline", inlined);
	SQL(owner, "COMMIT");
	return json.Finish(result);
}

struct HistoryDropHook {
	string name;
	std::function<void(const string &)> check;
	bool fired = false;
};

struct HistoryQueryState final : ClientContextState {
	shared_ptr<HistoryDropHook> hook;
	bool drop = false;
	explicit HistoryQueryState(shared_ptr<HistoryDropHook> hook_p) : hook(std::move(hook_p)) {
	}
	void QueryBegin(ClientContext &context) override {
		auto &query = context.GetCurrentQuery();
		drop = !hook->fired && StringUtil::StartsWith(query, "DROP TABLE IF EXISTS ") &&
		       query.find(hook->name) != string::npos;
	}
	void QueryEnd(ClientContext &, optional_ptr<ErrorData> error) override {
		bool hit = drop && !error;
		drop = false;
		if (hit) {
			hook->fired = true;
			hook->check("history_inline_drop_staged");
		}
	}
};

struct HistoryQueryCallback final : ExtensionCallback {
	shared_ptr<HistoryDropHook> hook;
	explicit HistoryQueryCallback(shared_ptr<HistoryDropHook> hook_p) : hook(std::move(hook_p)) {
	}
	void OnConnectionOpened(ClientContext &context) override {
		context.registered_state->Insert("history_inline_drop", make_shared_ptr<HistoryQueryState>(hook));
	}
};

struct HistoryFixture : KeyedFixture {
	bool t_only;
	string old_uuid, new_uuid;
	idx_t old_id;
	vector<string> old_inline;

	HistoryFixture(DuckDB &db, Connection &connection, const string &path, bool t_only_p)
	    : KeyedFixture(db, connection, path), t_only(t_only_p) {
		if (t_only) {
			Manage();
			SQL(setup, "SELECT * FROM ducklake_initialize_vortex_optimizer('lake');CREATE TABLE lake.other(id BIGINT)");
			Put(0, 256, "seed");
		} else {
			Seed();
			Put(256, 64, "unindexed");
			SQL(setup, "CALL ducklake_flush_inlined_data('lake',table_name=>'items')");
			DuckLakeVortexIndex::CollectBuilds(*setup.context, "lake", 16, 64 * 1024 * 1024);
			Write(root + "/indexes/caller.keep", "unregistered caller object");
		}
		Connection owner(database);
		Mutate(owner);
		SQL(setup, "INSERT INTO lake.other VALUES(1);UPDATE lake.other SET id=2");
		SQL(setup, "CALL ducklake_set_option('lake','data_inlining_row_limit',1000);"
		           "CALL ducklake_set_option('lake','data_inlining_row_limit',1000,schema=>'main')");
		old_uuid = TableUUID(setup);
		old_id = SQL(setup, "SELECT table_id FROM __ducklake_metadata_lake.ducklake_table WHERE table_uuid=" +
		                        Value(old_uuid).ToSQLString())
		             ->GetValue(0, 0)
		             .GetValue<idx_t>();
		auto tables = SQL(setup, "SELECT table_name FROM __ducklake_metadata_lake.ducklake_inlined_data_tables "
		                         "WHERE table_id=" +
		                             to_string(old_id) + " ORDER BY ALL");
		while (auto chunk = tables->Fetch()) {
			for (idx_t i = 0; i < chunk->size(); i++) {
				old_inline.push_back(StringValue::Get(chunk->GetValue(0, i)));
			}
		}
		Require(!old_inline.empty(), "History fixture did not retain native inline versions");
		auto deletion_table = "ducklake_inlined_delete_" + to_string(old_id);
		if (setup.TableInfo("__ducklake_metadata_lake", "main", deletion_table)) {
			old_inline.push_back(deletion_table);
		}
		DuckLakeVortexSourceRetention::Configure(*setup.context, "lake", true, 1, 0);
		DrainSources(); // Keep exactly the latest old-table snapshot as the later expiry fence.
		SQL(setup, "CHECKPOINT __ducklake_metadata_lake");
		Storage("old-checkpointed");
	}

	void Storage(const string &phase) {
		auto attached = DatabaseManager::Get(*setup.context->db).GetDatabase("__ducklake_metadata_lake");
		Require(bool(attached), "Native metadata attachment is missing");
		auto size = attached->GetCatalog().GetDatabaseSize(*setup.context);
		Require(size.total_blocks == size.used_blocks + size.free_blocks, "Native block accounting does not balance");
		vortex_detail::ViewJSON json;
		auto value = json.Object();
		json.Number(value, "block_size", size.block_size);
		json.Number(value, "total_blocks", size.total_blocks);
		json.Number(value, "used_blocks", size.used_blocks);
		json.Number(value, "free_blocks", size.free_blocks);
		json.Number(value, "database_bytes", LifecycleBytes(root + "/metadata.ducklake"));
		json.Number(value, "wal_bytes", LifecycleBytes(root + "/metadata.ducklake.wal"));
		Write(root + "/storage-" + phase + ".json", json.Finish(value));
	}

	void Put(idx_t start, idx_t count, const string &request) {
		auto input = SQL(setup, "SELECT list({'id':i::BIGINT,'embedding':[i::FLOAT,0::FLOAT],"
		                        "'category':'A'::VARCHAR,'payload':'seed-'||i}) FROM range(" +
		                            to_string(start) + "," + to_string(start + count) + ")t(i)");
		Connection owner(database);
		DuckLakeVortexWriteBatch::Mutate(owner, "lake", "main", "items", "knn", request, input->GetValue(0, 0),
		                                 Value::LIST(LogicalType::BIGINT, {}), 1, TableUUID(owner));
	}

	DuckLakeVortexSourceGCResult Sources(const std::function<void(const string &)> &observe = {}) {
		auto result = DuckLakeVortexSourceRetention::Collect(*setup.context, "lake", 1, 64 * 1024 * 1024, observe);
		Require(!result.blocked && result.files + result.expired <= 1 && result.bytes <= 64 * 1024 * 1024,
		        "History collector widened its budget or unexpectedly deferred");
		return result;
	}

	void DrainSources(const std::function<void(const string &)> &observe = {}) {
		for (idx_t i = 0; i < 128; i++) {
			auto status = DuckLakeVortexSourceRetention::Status(*setup.context, "lake")[0];
			if (!status[6].GetValue<idx_t>() && !status[7].GetValue<idx_t>() && !status[8].GetValue<idx_t>()) {
				return;
			}
			Sources(observe);
		}
		throw InvalidInputException("History source cleanup did not converge within the fixed work bound");
	}

	void RetireAndRecreate() {
		Connection owner(database);
		DuckLakeVortexWriteBatch::RetireTable(owner, "lake", "main", "items", old_uuid);
		SQL(setup, "CREATE TABLE lake.items(id BIGINT,embedding FLOAT[],category VARCHAR,payload VARCHAR);"
		           "CALL ducklake_set_option('lake','experimental_data_file_format','vortex',table_name=>'items');"
		           "SELECT * FROM ducklake_create_vortex_index('lake','main','items','knn'," +
		               Value(root + "/indexes").ToSQLString() + ",2)");
		Manage();
		new_uuid = TableUUID(setup);
		Require(new_uuid != old_uuid, "Recreated table reused its UUID");
		Require(SQL(setup, "SELECT count(*) FROM __ducklake_metadata_lake.ducklake_metadata "
		                   "WHERE scope='table' AND scope_id=" +
		                       to_string(old_id))
		                ->GetValue(0, 0)
		                .GetValue<idx_t>() == 1,
		        "Retirement removed format options still needed by native history");
		Put(20000, 2, "new-table");
		Write(root + "/data/unregistered.keep", "unregistered data object");
		vortex_detail::ViewJSON json;
		auto value = json.Object(), inlined = json.Array();
		json.Text(value, "old_uuid", old_uuid);
		json.Text(value, "new_uuid", new_uuid);
		json.Number(value, "old_id", old_id);
		json.Add(value, "t_only", duckdb_yyjson::yyjson_mut_bool(json.doc.get(), t_only));
		for (auto &name : old_inline) {
			duckdb_yyjson::yyjson_mut_arr_append(inlined,
			                                     duckdb_yyjson::yyjson_mut_strcpy(json.doc.get(), name.c_str()));
		}
		json.Add(value, "old_inline", inlined);
		Write(root + "/identity.json", json.Finish(value));
		Write(root + "/before.json", HistorySnapshot(database));
		Storage("retired");
	}

	void Save(const string &stage, bool wait = false) {
		Write(root + "/expected.json", HistorySnapshot(database));
		Write(root + "/checkpoint", stage);
		if (wait) {
			auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(120);
			while (!FileSystem::GetFileSystem(*setup.context).FileExists(root + "/release-" + stage)) {
				Require(std::chrono::steady_clock::now() < deadline, "History checkpoint was not released or killed");
				std::this_thread::sleep_for(std::chrono::milliseconds(5));
			}
		}
	}

	void Complete(const std::function<void(const string &)> &observe) {
		DrainSources(observe);
		auto generation = DuckLakeVortexIndex::Collect(*setup.context, "lake", 1, 64 * 1024 * 1024, observe);
		Require(generation.generations <= 1 && generation.artifact_bytes <= 64 * 1024 * 1024 && !generation.remaining,
		        "History generation cleanup violated its existing budget");
		Storage("logical-collected");
		observe("history_before_checkpoint");
		SQL(setup, "CHECKPOINT __ducklake_metadata_lake");
		Storage("collected-checkpointed");
		observe("history_after_checkpoint");
		Require(SQL(setup,
		            "SELECT count(*) FROM __ducklake_metadata_lake.ducklake_table WHERE table_id=" + to_string(old_id))
		                ->GetValue(0, 0)
		                .GetValue<idx_t>() == 0,
		        "Retired table history survived final expiry");
		Require(SQL(setup, "SELECT count(*) FROM __ducklake_metadata_lake.ducklake_metadata "
		                   "WHERE scope='table' AND scope_id=" +
		                       to_string(old_id))
		                ->GetValue(0, 0)
		                .GetValue<idx_t>() == 0,
		        "Expired table retained scoped configuration");
		for (auto &name : old_inline) {
			Require(!setup.TableInfo("__ducklake_metadata_lake", "main", name), "Native inline table was not dropped");
		}
		Require(SQL(setup, "SELECT count(*) FROM lake.items")->GetValue(0, 0).GetValue<idx_t>() == 2 &&
		            SQL(setup, "SELECT id FROM lake.other")->GetValue(0, 0).GetValue<int64_t>() == 2,
		        "Retired history cleanup changed live tables");
		Save("history_complete");
	}
};

inline void RunHistoryFixture(DuckDB &database, Connection &setup, const string &root, const string &mode) {
	const bool t_only = StringUtil::StartsWith(mode, "history-t-");
	auto action = mode.substr(t_only ? 10 : 8);
	HistoryFixture f(database, setup, root, t_only);
	Pause pause;
	ReleasePause release {pause};
	Connection reader(database);
	std::future<DuckLakeVortexSearchResult> held;
	if (action == "held") {
		held = std::async(std::launch::async, [&] {
			return f.Search(reader, [&](const string &stage) {
				if (stage == "before_fetch")
					pause.Stop();
			});
		});
		pause.Wait();
	}
	f.RetireAndRecreate();
	f.Save("history_prepared", true);
	auto expected = HistorySnapshot(database);
	bool fired = false, conflict = false;
	auto observe = [&](const string &stage) {
		if (action == "pause-" + stage) {
			f.Save(stage, true);
		}
		if (action == "conflict" && stage == "history_inline_drop_staged") {
			Connection other(database);
			SQL(other, "UPDATE __ducklake_metadata_lake.ducklake_vortex_source_retention_v1 "
			           "SET expired_snapshots=expired_snapshots+1");
			expected = HistorySnapshot(database);
			fired = conflict = true;
		}
		if (action == "fail-" + stage || action == "cancel-" + stage) {
			fired = true;
			if (StringUtil::StartsWith(action, "cancel-")) {
				setup.context->Interrupt();
				if (stage == "history_inline_drop_staged") {
					throw InterruptException(); // Interrupt the native query after its transactional DROP.
				}
				return; // Exercise the collector's existing interruption check at public cutpoints.
			}
			throw IOException("Injected retired history failure");
		}
	};
	auto hook = make_shared_ptr<HistoryDropHook>();
	hook->name = f.old_inline[0];
	hook->check = observe;
	ExtensionCallbackManager::Get(*database.instance).Register(make_shared_ptr<HistoryQueryCallback>(hook));
	try {
		f.Complete(observe);
		Require(!StringUtil::StartsWith(action, "fail-") && !StringUtil::StartsWith(action, "cancel-") &&
		            action != "conflict",
		        "History fault did not fire");
	} catch (std::exception &error) {
		Require(fired, error.what());
		auto message = StringUtil::Lower(error.what());
		Require(message.find(conflict                                    ? "conflict"
		                     : StringUtil::StartsWith(action, "cancel-") ? "interrupt"
		                                                                 : "injected retired") != string::npos,
		        "Retired history failed for an unrelated reason");
		if (conflict || action.find("history_inline_drop_staged") != string::npos ||
		    action.find("source_before_expire") != string::npos) {
			Require(HistorySnapshot(database) == expected,
			        "Failed history expiry partially committed native DDL/state");
		}
		// Disable the one-shot injected callback before explicit recovery.
		hook->fired = true;
		SQL(setup, "SELECT 1");
		f.Complete([](const string &) {});
	}
	if (held.valid()) {
		pause.Release();
		auto old = held.get();
		Require(old.rows.size() == 3 && old.rows[0][0].GetValue<int64_t>() == 0 &&
		            old.rows[0][3].ToString() == "更新" && old.rows[1][0].GetValue<int64_t>() == 10000 &&
		            old.rows[1][3].IsNull() && old.rows[2][0].GetValue<int64_t>() == 2,
		        "Retired history/physical GC/checkpoint revoked the fully opened old view");
	}
}
} // namespace vortex_test
} // namespace duckdb
