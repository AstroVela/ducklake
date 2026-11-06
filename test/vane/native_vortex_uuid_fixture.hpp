#pragma once

#include "native_vortex_lifecycle_fixture.hpp"

namespace duckdb {
namespace vortex_test {

inline string UUIDSnapshot(DuckDB &database) {
	Connection owner(database);
	SQL(owner, "BEGIN");
	vortex_detail::ViewJSON json;
	auto value = json.Object(), metadata = json.Object();
	for (auto name :
	     {"ducklake_vortex_builds_v1", "ducklake_vortex_generation_refs_v1", "ducklake_vortex_generations_v1",
	      "ducklake_vortex_indexes_v1", "ducklake_vortex_keyed_tables_v1", "ducklake_vortex_maintenance_v1",
	      "ducklake_vortex_optimizer_v2", "ducklake_vortex_request_epochs_v1", "ducklake_vortex_rewrites_v1",
	      "ducklake_vortex_source_deletions_v1", "ducklake_vortex_source_retention_v1",
	      "ducklake_vortex_write_budgets_v1", "ducklake_vortex_write_requests_v2"}) {
		json.Add(
		    metadata, name,
		    LifecycleRows(json, owner, "SELECT * FROM __ducklake_metadata_lake." + string(name) + " ORDER BY ALL"));
	}
	json.Add(value, "metadata", metadata);
	json.Add(value, "snapshots",
	         LifecycleRows(
	             json, owner,
	             "SELECT snapshot_id,schema_version FROM __ducklake_metadata_lake.ducklake_snapshot ORDER BY ALL"));
	json.Add(value, "tables",
	         LifecycleRows(json, owner,
	                       "SELECT table_uuid::VARCHAR,table_id,table_name,begin_snapshot,end_snapshot FROM "
	                       "__ducklake_metadata_lake.ducklake_table ORDER BY ALL"));
	auto current = SQL(owner, "SELECT count(*) FROM __ducklake_metadata_lake.ducklake_table WHERE table_name='items' "
	                          "AND end_snapshot IS NULL");
	json.Add(value, "rows",
	         current->GetValue(0, 0).GetValue<idx_t>()
	             ? LifecycleRows(json, owner, "SELECT id,embedding,category,payload FROM lake.items ORDER BY id")
	             : json.Array());
	SQL(owner, "COMMIT");
	return json.Finish(value);
}

inline void UUIDSave(DuckDB &database, const string &root, const string &stage) {
	Write(root + "/expected.json", UUIDSnapshot(database));
	Write(root + "/checkpoint", stage);
}

inline void UUIDPause(DuckDB &database, Connection &setup, const string &root, const string &stage) {
	UUIDSave(database, root, stage);
	auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(180);
	while (!FileSystem::GetFileSystem(*setup.context).FileExists(root + "/release")) {
		Require(std::chrono::steady_clock::now() < deadline, "UUID crash boundary was not killed");
		std::this_thread::sleep_for(std::chrono::milliseconds(5));
	}
}

inline void RunUUIDFixture(DuckDB &database, Connection &setup, const string &root, const string &mode) {
	LifecycleFixture f(database, setup, root);
	auto uuid = TableUUID(setup);
	Write(root + "/table-uuid", uuid);
	auto retire = [&](const std::function<void(const string &)> &observe = nullptr) {
		Connection owner(database);
		return DuckLakeVortexWriteBatch::RetireTable(owner, "lake", "main", "items", uuid, observe);
	};
	if (mode == "uuid-build-blocked") {
		auto before = UUIDSnapshot(database);
		bool rejected = false;
		try {
			retire();
		} catch (std::exception &error) {
			rejected = string(error.what()).find("build/rewrite") != string::npos;
		}
		Require(rejected && UUIDSnapshot(database) == before, "Pending build retirement changed state");
	}
	auto collected = DuckLakeVortexIndex::CollectBuilds(*setup.context, "lake", 16, 64 * 1024 * 1024);
	Require(collected.remaining == 0, "Fixture retained build/rewrite ownership");
	auto before = UUIDSnapshot(database);
	Write(root + "/before.json", before);
	if (mode == "uuid-held") {
		Pause pause;
		ReleasePause release {pause};
		Connection reader(database);
		auto held = std::async(std::launch::async, [&] {
			return f.Search(reader, [&](const string &stage) {
				if (stage == "before_fetch") {
					pause.Stop();
				}
			});
		});
		pause.Wait();
		retire();
		auto gc = DuckLakeVortexIndex::Collect(*setup.context, "lake", 2, 64 * 1024 * 1024);
		Require(gc.generations == 1 && gc.remaining == 0 && gc.artifact_bytes <= 64 * 1024 * 1024,
		        "Retired UUID did not release its shared named generation");
		f.Checkpoint();
		pause.Release();
		auto old = held.get();
		Require(old.rows.size() == 3 && old.rows[0][0].GetValue<int64_t>() == 0 &&
		            old.rows[0][3].ToString() == "seed-0" && old.rows[1][0].GetValue<int64_t>() == 1,
		        "Table retirement revoked the captured private view");
	} else if (mode == "uuid-optimizer-blocked") {
		Pause pause;
		ReleasePause release {pause};
		DuckLakeVortexOptimizer::Resume(*setup.context, "lake", "main", "items", "knn", [&](const string &stage) {
			if (stage == "optimizer_before_load") {
				pause.Stop();
			}
		});
		pause.Wait();
		DuckLakeVortexOptimizer::Cancel(*setup.context, "lake", "main", "items", "knn");
		auto cancelled = UUIDSnapshot(database);
		bool rejected = false;
		try {
			retire();
		} catch (std::exception &error) {
			rejected = string(error.what()).find("in-flight") != string::npos;
		}
		Require(rejected && UUIDSnapshot(database) == cancelled, "Cancelled in-flight task lost UUID ownership");
		pause.Release();
		auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
		while (DuckLakeVortexOptimizer::Status(*setup.context, "lake")[0][16].GetValue<bool>()) {
			Require(std::chrono::steady_clock::now() < deadline, "Cancelled UUID worker did not stop");
			std::this_thread::sleep_for(std::chrono::milliseconds(5));
		}
		retire();
	} else if (mode == "uuid-race") {
		vector<std::future<string>> pending;
		for (idx_t i = 0; i < 8; i++) {
			pending.push_back(std::async(std::launch::async, [&] { return retire(); }));
		}
		for (auto &task : pending) {
			Require(task.get() == uuid, "Concurrent UUID retirement changed its result");
		}
	} else {
		bool injected = false, conflict = false;
		try {
			Connection owner(database);
			DuckLakeVortexWriteBatch::RetireTable(owner, "lake", "main", "items", uuid, [&](const string &stage) {
				if (mode == "uuid-pause-" + stage) {
					UUIDPause(database, setup, root, stage);
				}
				if (mode == "uuid-fail-" + stage) {
					injected = true;
					throw IOException("Injected UUID retirement failure");
				}
				if (mode == "uuid-cancel-" + stage) {
					injected = true;
					owner.context->Interrupt();
				}
				if (mode == "uuid-conflict" && stage == "retire_after_indexes") {
					injected = conflict = true;
					Connection other(database);
					// Commit an independent, empty native snapshot at the ID the DROP will claim.
					SQL(other, "BEGIN; INSERT INTO __ducklake_metadata_lake.ducklake_snapshot "
					           "SELECT * REPLACE(snapshot_id+1 AS snapshot_id,now() AS snapshot_time) "
					           "FROM __ducklake_metadata_lake.ducklake_snapshot ORDER BY snapshot_id DESC LIMIT 1;"
					           "INSERT INTO __ducklake_metadata_lake.ducklake_snapshot_changes "
					           "SELECT max(snapshot_id),'',NULL,NULL,NULL FROM "
					           "__ducklake_metadata_lake.ducklake_snapshot;COMMIT");
					before = UUIDSnapshot(database);
				}
			});
			Require(!injected, "UUID fault did not stop retirement");
		} catch (std::exception &error) {
			Require(injected, error.what());
			auto text = string(error.what());
			if (conflict) {
				auto lower = StringUtil::Lower(text);
				Require(lower.find("conflict") != string::npos || lower.find("constraint") != string::npos ||
				            lower.find("duplicate key") != string::npos,
				        "UUID fixture did not produce a native metadata conflict: " + text);
			} else {
				Require(text.find(mode.find("cancel") != string::npos ? "Interrupt" : "Injected UUID") != string::npos,
				        "UUID retirement failed for an unrelated reason");
			}
			auto observed = UUIDSnapshot(database);
			if (observed != before) {
				Write(root + "/expected-rollback.json", before);
				Write(root + "/observed-rollback.json", observed);
				Write(root + "/rollback-error", error.what());
			}
			Require(observed == before, "Interrupted UUID retirement partially committed");
			retire();
		}
	}
	auto after = UUIDSnapshot(database);
	Require(retire() == uuid && UUIDSnapshot(database) == after, "Retirement replay advanced snapshots or metadata");
	Require(LifecycleRead(root + "/indexes/retained.json") == f.retained &&
	            LifecycleRead(root + "/indexes/caller.keep") == "unregistered caller object",
	        "UUID retirement changed caller-owned files");
	UUIDSave(database, root, "uuid-complete");
}
} // namespace vortex_test
} // namespace duckdb
