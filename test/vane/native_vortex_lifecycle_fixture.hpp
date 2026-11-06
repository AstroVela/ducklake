#pragma once

#include "native_vortex_keyed_fixture.hpp"
#include "common/ducklake_vortex_json.hpp"

namespace duckdb {
namespace vortex_test {

inline string LifecycleRead(const string &path) {
	std::ifstream stream(path, std::ios::binary);
	Require(bool(stream), "Lifecycle evidence file is missing");
	return string(std::istreambuf_iterator<char>(stream), std::istreambuf_iterator<char>());
}

inline idx_t LifecycleBytes(const string &path) {
	std::ifstream stream(path, std::ios::binary | std::ios::ate);
	return stream ? idx_t(stream.tellg()) : 0;
}

inline duckdb_yyjson::yyjson_mut_val *LifecycleValue(vortex_detail::ViewJSON &json, const Value &value) {
	using namespace duckdb_yyjson;
	if (value.IsNull()) {
		return yyjson_mut_null(json.doc.get());
	}
	switch (value.type().id()) {
	case LogicalTypeId::VARCHAR: {
		auto &text = StringValue::Get(value);
		return yyjson_mut_strncpy(json.doc.get(), text.data(), text.size());
	}
	case LogicalTypeId::BOOLEAN:
		return yyjson_mut_bool(json.doc.get(), value.GetValue<bool>());
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::INTEGER:
		return yyjson_mut_sint(json.doc.get(), value.GetValue<int64_t>());
	case LogicalTypeId::UBIGINT:
		return yyjson_mut_uint(json.doc.get(), value.GetValue<uint64_t>());
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
		return yyjson_mut_real(json.doc.get(), value.GetValue<double>());
	case LogicalTypeId::LIST: {
		auto list = json.Array();
		for (auto &child : ListValue::GetChildren(value)) {
			yyjson_mut_arr_append(list, LifecycleValue(json, child));
		}
		return list;
	}
	default:
		throw InternalException("Unsupported lifecycle evidence type: %s", value.type().ToString());
	}
}

inline duckdb_yyjson::yyjson_mut_val *LifecycleRows(vortex_detail::ViewJSON &json, Connection &owner,
                                                    const string &sql) {
	auto rows = json.Array();
	auto result = SQL(owner, sql);
	while (auto chunk = result->Fetch()) {
		for (idx_t row = 0; row < chunk->size(); row++) {
			auto values = json.Array();
			for (idx_t col = 0; col < chunk->ColumnCount(); col++) {
				duckdb_yyjson::yyjson_mut_arr_append(values, LifecycleValue(json, chunk->GetValue(col, row)));
			}
			duckdb_yyjson::yyjson_mut_arr_append(rows, values);
		}
	}
	return rows;
}

struct LifecycleFixture : KeyedFixture {
	string old_generation;
	string retained;

	LifecycleFixture(DuckDB &database, Connection &setup, const string &root) : KeyedFixture(database, setup, root) {
		Seed();
		old_generation = index.generation;
		retained = LifecycleRead(root + "/indexes/seed.json");
		Write(root + "/indexes/retained.json", retained);
		Write(root + "/indexes/caller.keep", "unregistered caller object");
		Connection owner(database);
		DuckLakeVortexIndex::Publish(owner, "lake", "main", "items", "shadow", root + "/indexes/seed.json", 0);
	}

	idx_t Count(const string &sql) {
		return SQL(setup, sql)->GetValue(0, 0).GetValue<idx_t>();
	}

	void ReferenceState(const string &generation, const string &state, idx_t references) {
		auto result = SQL(setup, "SELECT state,(SELECT count(*) FROM "
		                         "__ducklake_metadata_lake.ducklake_vortex_generation_refs_v1 r "
		                         "WHERE r.generation=g.generation) FROM "
		                         "__ducklake_metadata_lake.ducklake_vortex_generations_v1 g WHERE generation=" +
		                             Value(generation).ToSQLString());
		Require(result->RowCount() == 1 && StringValue::Get(result->GetValue(0, 0)) == state &&
		            result->GetValue(1, 0).GetValue<idx_t>() == references,
		        "Caller JSON and native generation ownership were conflated");
	}

	void CheckNames(bool changed) {
		for (auto name : {"knn", "shadow"}) {
			auto result = SQL(setup, "SELECT id,payload FROM ducklake_vortex_search('lake','main','items'," +
			                             Value(name).ToSQLString() + ",[0,0]::FLOAT[],3)");
			Require(result->GetValue(0, 0).GetValue<int64_t>() == 0 &&
			            StringValue::Get(result->GetValue(1, 0)) == (changed ? "更新" : "seed-0") &&
			            result->GetValue(0, 1).GetValue<int64_t>() == (changed ? 10000 : 1),
			        "Named query relied on caller reference JSON or lost current versions");
		}
	}

	void Checkpoint() {
		Connection owner(database);
		SQL(owner, "CHECKPOINT __ducklake_metadata_lake");
	}

	string Snapshot() {
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
		         LifecycleRows(json, owner,
		                       "SELECT snapshot_id,schema_version FROM "
		                       "__ducklake_metadata_lake.ducklake_snapshot ORDER BY ALL"));
		json.Add(value, "rows",
		         LifecycleRows(json, owner, "SELECT id,embedding,category,payload FROM lake.items ORDER BY id"));
		SQL(owner, "COMMIT");
		return json.Finish(value);
	}

	void Save(const string &stage) {
		Write(root + "/expected.json", Snapshot());
		Write(root + "/checkpoint", stage);
	}

	void Wait(const string &stage) {
		Save(stage);
		auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(120);
		while (!FileSystem::GetFileSystem(*setup.context).FileExists(root + "/release")) {
			Require(std::chrono::steady_clock::now() < deadline, "Lifecycle pause fixture was not killed");
			std::this_thread::sleep_for(std::chrono::milliseconds(5));
		}
	}

	void Retire(const string &mode) {
		auto &fs = FileSystem::GetFileSystem(*setup.context);
		fs.RemoveFile(root + "/indexes/seed.json");
		CheckNames(false);
		auto build = DuckLakeVortexIndex::CollectBuilds(*setup.context, "lake", 1, 64 * 1024 * 1024);
		Require(build.builds == 1 && build.remaining == 0 && fs.DirectoryExists(root + "/indexes/" + old_generation),
		        "Published build cleanup removed an active generation");
		ReferenceState(old_generation, "active", 2);
		Connection owner(database);
		Mutate(owner);
		SQL(setup, "CALL ducklake_flush_inlined_data('lake',table_name=>'items')");
		index = DuckLakeVortexIndex::BuildAndPublish(*setup.context, "lake", "main", "items", "knn",
		                                             root + "/indexes/current.json", 2, 2);
		ReferenceState(old_generation, "active", 1);
		Require(DuckLakeVortexIndex::Collect(*setup.context, "lake", 1, 64 * 1024 * 1024).generations == 0,
		        "A remaining named reference failed to protect B1");
		DuckLakeVortexIndex::Publish(owner, "lake", "main", "items", "shadow", root + "/indexes/current.json", 1);
		ReferenceState(old_generation, "retired", 0);
		auto small = DuckLakeVortexIndex::Collect(*setup.context, "lake", 1, 1);
		Require(!small.generations && !small.artifact_bytes && small.remaining == 1,
		        "Reference lifecycle bypassed the generation byte budget");
		bool injected = false;
		try {
			auto completed =
			    DuckLakeVortexIndex::Collect(*setup.context, "lake", 1, 64 * 1024 * 1024, [&](const string &stage) {
				    if (stage == "gc_file_deleted" && (mode == "lifecycle-gc-cancel" || mode == "lifecycle-gc-fail")) {
					    injected = true;
					    if (mode == "lifecycle-gc-cancel") {
						    setup.context->Interrupt();
					    } else {
						    throw IOException("Injected lifecycle partial generation deletion");
					    }
				    }
			    });
			Require(!injected, "Lifecycle GC fault did not stop collection");
			Require(completed.generations == 1 && !completed.remaining && completed.artifact_bytes <= 64 * 1024 * 1024,
			        "Generation collection changed its object/byte budget");
		} catch (std::exception &error) {
			Require(injected, error.what());
			Require(string(error.what()).find(mode == "lifecycle-gc-cancel" ? "Interrupt" : "Injected lifecycle") !=
			            string::npos,
			        "Lifecycle GC fault failed for an unrelated reason");
			ReferenceState(old_generation, "deleting", 0);
			Checkpoint();
			auto resumed = DuckLakeVortexIndex::Collect(*setup.context, "lake", 1, 64 * 1024 * 1024);
			Require(resumed.generations == 1 && !resumed.remaining, "Explicit partial GC recovery did not complete");
		}
		Require(!fs.DirectoryExists(root + "/indexes/" + old_generation), "Unreferenced B1 was not collected");
		ReferenceState(index.generation, "active", 2);
		DuckLakeVortexIndex::CollectBuilds(*setup.context, "lake", 1, 64 * 1024 * 1024);
		Require(LifecycleRead(root + "/indexes/retained.json") == retained &&
		            LifecycleRead(root + "/indexes/caller.keep") == "unregistered caller object",
		        "Collectors removed caller reference output or unknown siblings");
		bool stale = false;
		try {
			DuckLakeVortexIndex::Publish(owner, "lake", "main", "items", "stale", root + "/indexes/retained.json", 0);
		} catch (std::exception &) {
			stale = true;
		}
		Require(stale, "A retained JSON file revived a collected generation");
		CheckNames(true);
	}

	void Window() {
		Connection owner(database);
		auto result =
		    DuckLakeVortexWriteBatch::AdvanceRequestEpoch(owner, "lake", "main", "items", "knn", 1, TableUUID(owner));
		Require(result.expired_requests == 2, "Lifecycle fixture lost its existing receipts");
		DuckLakeVortexWriteBatch::Mutate(owner, "lake", "main", "items", "knn", "window",
		                                 Value::LIST(ListType::GetChildType(DuckLakeVortexWriteBatch::RowsType()), {}),
		                                 Value::LIST(LogicalType::BIGINT, {Value::BIGINT(-999999)}), 2,
		                                 TableUUID(owner));
	}
};

inline void LifecycleGuards(LifecycleFixture &f) {
	Connection writer(f.database), checkpoint(f.database);
	SQL(writer, "BEGIN;UPDATE __ducklake_metadata_lake.ducklake_vortex_request_epochs_v1 "
	            "SET last_expired_requests=7");
	for (auto owner : {&checkpoint, &writer}) {
		auto result = owner->Query("CHECKPOINT __ducklake_metadata_lake");
		Require(result->HasError() &&
		            result->GetError().find(owner == &writer ? "transaction local changes"
		                                                     : "other write transactions") != string::npos,
		        "Non-forced checkpoint bypassed an active native writer");
		if (owner == &checkpoint) {
			Require(SQL(writer, "SELECT last_expired_requests FROM "
			                    "__ducklake_metadata_lake.ducklake_vortex_request_epochs_v1")
			                ->GetValue(0, 0)
			                .GetValue<idx_t>() == 7,
			        "Another connection's checkpoint aborted the active writer");
		}
	}
	SQL(writer, "ROLLBACK");
	Require(f.Count("SELECT last_expired_requests FROM __ducklake_metadata_lake.ducklake_vortex_request_epochs_v1") ==
	            0,
	        "Rejected checkpoint committed transaction-local changes");
	Connection reader(f.database);
	SQL(reader, "BEGIN;SELECT count(*) FROM lake.items");
	Require(SQL(reader, "SELECT count(*) FROM __ducklake_metadata_lake.ducklake_vortex_write_requests_v2")
	                ->GetValue(0, 0)
	                .GetValue<idx_t>() == 2,
	        "Reader did not capture the old receipt window");
	f.Window();
	// The old snapshot retains the committed epoch UPDATE and its checkpoint lock.
	auto deferred = checkpoint.Query("CHECKPOINT __ducklake_metadata_lake");
	Require(deferred->HasError() && deferred->GetError().find("other write transactions") != string::npos,
	        "Checkpoint bypassed an older reader retaining committed metadata updates");
	Require(SQL(reader, "SELECT count(*) FROM __ducklake_metadata_lake.ducklake_vortex_write_requests_v2")
	                ->GetValue(0, 0)
	                .GetValue<idx_t>() == 2,
	        "Deferred native checkpoint revoked an ordinary reader snapshot");
	DuckLakeVortexOptimizer::ConfigureSourceRetention(*f.setup.context, "lake", true, 1, 0);
	Require(DuckLakeVortexSourceRetention::Collect(*f.setup.context, "lake", 2, 64 * 1024 * 1024).blocked,
	        "Checkpoint bypassed ordinary-transaction source GC protection");
	SQL(reader, "COMMIT");
	f.Checkpoint();
	DuckLakeVortexOptimizer::ConfigureSourceRetention(*f.setup.context, "lake", false, 1, 0);
}

inline void RunLifecycleFixture(DuckDB &database, Connection &setup, const string &root, const string &mode) {
	LifecycleFixture f(database, setup, root);
	Pause pause;
	Connection reader(database);
	auto held = std::async(std::launch::async, [&] {
		return f.Search(reader, [&](const string &stage) {
			if (stage == "before_fetch") {
				pause.Stop();
			}
		});
	});
	ReleasePause release {pause};
	pause.Wait();
	f.Retire(mode);
	if (mode == "lifecycle-wal-guards") {
		LifecycleGuards(f);
	} else {
		f.Window();
	}
	auto before = f.Snapshot();
	Require(LifecycleBytes(root + "/metadata.ducklake.wal") > 0, "Lifecycle fixture did not retain a native WAL");
	if (mode == "lifecycle-pause-before-checkpoint") {
		f.Wait("before-checkpoint");
	}
	if (mode == "lifecycle-pause-during-checkpoint") {
		Write(root + "/wal-before", to_string(LifecycleBytes(root + "/metadata.ducklake.wal")));
		SQL(setup, "SET debug_checkpoint_sleep_ms=300000");
		f.Save("during-checkpoint");
		f.Checkpoint();
		throw InvalidInputException("Controlled checkpoint pause was not killed");
	}
	for (auto fault : {"BEFORE_HEADER", "BEFORE_TRUNCATE", "BEFORE_WAL_FINISH"}) {
		if (mode == "lifecycle-fault-" + string(fault)) {
			SQL(setup, "SET debug_checkpoint_abort=" + Value(fault).ToSQLString());
			Write(root + "/expected.json", before);
			bool failed = false;
			try {
				f.Checkpoint();
			} catch (std::exception &error) {
				failed = string(error.what()).find("Checkpoint aborted") != string::npos;
			}
			Require(failed, "Native checkpoint fault did not fire");
			Write(root + "/checkpoint", fault);
			while (true) {
				std::this_thread::sleep_for(std::chrono::milliseconds(10));
			}
		}
	}
	f.Checkpoint();
	Require(LifecycleBytes(root + "/metadata.ducklake.wal") == 0 && f.Snapshot() == before,
	        "Native checkpoint changed logical state or retained an old WAL without writers");
	if (mode == "lifecycle-pause-after-checkpoint") {
		f.Wait("after-checkpoint");
	}
	if (mode == "lifecycle-pause-write-before_commit" || mode == "lifecycle-pause-write-after_commit") {
		Connection owner(database);
		auto rows = SQL(setup, "SELECT [{'id':12345::BIGINT,'embedding':[12345,0]::FLOAT[],"
		                       "'category':'A'::VARCHAR,'payload':'after-checkpoint'::VARCHAR}]")
		                ->GetValue(0, 0);
		DuckLakeVortexWriteBatch::Mutate(owner, "lake", "main", "items", "knn", "after-checkpoint", rows,
		                                 Value::LIST(LogicalType::BIGINT, {}), 2, TableUUID(owner),
		                                 [&](const string &stage) {
			                                 if (mode == "lifecycle-pause-write-" + stage) {
				                                 f.Wait("write-" + stage);
			                                 }
		                                 });
	}
	f.Checkpoint();
	pause.Release();
	auto previous = held.get();
	Require(previous.rows.size() == 3 && previous.rows[0][0].GetValue<int64_t>() == 0 &&
	            previous.rows[0][3].ToString() == "seed-0" && previous.rows[1][0].GetValue<int64_t>() == 1,
	        "Old private ReadView lost typed versions after reference cleanup, GC and checkpoint");
	f.CheckNames(true);
	f.Save("complete");
}

} // namespace vortex_test
} // namespace duckdb
