#pragma once

#include "native_vortex_lifecycle_fixture.hpp"

namespace duckdb {
namespace vortex_test {

inline string UUIDSnapshot(DuckDB &database) {
	Connection owner(database);
	SQL(owner, "BEGIN");
	vortex_detail::ViewJSON json;
	auto value = json.Object(), metadata = json.Object();
	AddLifecycleMetadata(json, metadata, owner);
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

inline void RunUUIDConcurrentPublication(DuckDB &database, Connection &setup, const string &root, const string &mode) {
	const bool build = mode == "uuid-concurrent-build";
	const bool rewrite = mode == "uuid-concurrent-rewrite";
	string rewrite_sql;
	if (rewrite) {
		KeyedFixture fixture(database, setup, root);
		fixture.Seed();
		Connection writer(database);
		fixture.Mutate(writer);
		SQL(setup, "CALL ducklake_flush_inlined_data('lake',table_name=>'items')");
		auto collected = DuckLakeVortexIndex::CollectBuilds(*setup.context, "lake", 16, 64 * 1024 * 1024);
		Require(collected.remaining == 0, "Initial build still owns rewrite inputs");
		rewrite_sql = "SELECT * FROM ducklake_rewrite_vortex_index_segment('lake','main','items','knn'," +
		              Value(fixture.index.segments[0].generation).ToSQLString() + "," +
		              Value(root + "/indexes/late.json").ToSQLString() + "," + to_string(fixture.index.revision) + ")";
	} else {
		SQL(setup, "SELECT * FROM ducklake_initialize_vortex_optimizer('lake')");
	}
	auto uuid = TableUUID(setup);
	Write(root + "/table-uuid", uuid);
	auto publish_sql = [&](const string &name, idx_t revision) {
		if (rewrite) {
			return rewrite_sql;
		}
		if (build) {
			return "SELECT * FROM ducklake_build_vortex_index('lake','main','items','knn'," +
			       Value(root + "/indexes/" + name + ".json").ToSQLString() + ",2," + to_string(revision) + ")";
		}
		return "SELECT * FROM ducklake_create_vortex_index('lake','main','items'," + Value(name).ToSQLString() + "," +
		       Value(root + "/indexes").ToSQLString() + ",2)";
	};
	Connection owner(database), caller(database);
	auto retire = [&](const std::function<void(const string &)> &observe = nullptr) {
		return DuckLakeVortexWriteBatch::RetireTable(owner, *caller.context, "lake", "main", "items", uuid, observe);
	};
	auto early = [&](const string &stage) {
		if (stage == "retire_acquired") {
			SQL(setup, publish_sql("early", 1));
		}
	};
	if (build) {
		SQL(setup, "INSERT INTO lake.items SELECT i,[i::FLOAT,0::FLOAT],'A','seed-'||i FROM range(128)t(i);"
		           "CALL ducklake_flush_inlined_data('lake',table_name=>'items')");
		string published;
		bool blocked = false;
		try {
			retire([&](const string &stage) {
				early(stage);
				if (stage == "retire_acquired") {
					published = UUIDSnapshot(database);
				}
			});
		} catch (const Exception &error) {
			ErrorData detail(error);
			Require(
			    detail.Type() == ExceptionType::TRANSACTION &&
			        detail.RawMessage() ==
			            "Failed to commit: Recover and collect Vortex build/rewrite ownership before table retirement",
			    error.what());
			blocked = true;
		}
		Require(blocked && !published.empty() && UUIDSnapshot(database) == published,
		        "Retirement did not preserve the preceding build and its live table");
		auto collected = DuckLakeVortexIndex::CollectBuilds(*setup.context, "lake", 16, 64 * 1024 * 1024);
		Require(collected.remaining == 0, "Preceding build could not release its ownership");
	}
	auto &lock = DuckLakeVortexIndex::PublicationLock();
	const auto previous_waiters = lock.WaitingWriterCount();
	std::future<unique_ptr<MaterializedQueryResult>> pending;
	bool held_to_commit = false;
	auto retired = retire([&](const string &stage) {
		if (!build && !rewrite) {
			early(stage);
		}
		if (stage == "retire_lock_wait" && !pending.valid()) {
			Require(!owner.context->transaction.HasActiveTransaction(),
			        "Retirement opened a snapshot before its locks");
		}
		if (stage == "retire_after_lookup") {
			pending = std::async(std::launch::async, [&] {
				Connection other(database);
				return other.Query(publish_sql("late", 2));
			});
			auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
			while (lock.WaitingWriterCount() <= previous_waiters) {
				Require(pending.wait_for(std::chrono::milliseconds(1)) != std::future_status::ready &&
				            std::chrono::steady_clock::now() < deadline,
				        "Concurrent publisher did not queue behind retirement");
			}
		}
		if (stage == "retire_before_native_commit") {
			held_to_commit = pending.valid() && lock.WaitingWriterCount() > previous_waiters;
		}
	});
	Require(retired == uuid && held_to_commit, "Retirement released the queued publisher before native commit");
	auto late = pending.get();
	if (rewrite) {
		const auto where = " WHERE table_uuid=" + Value(uuid).ToSQLString();
		auto ownership = SQL(
		    setup, "SELECT (SELECT count(*) FROM __ducklake_metadata_lake.ducklake_vortex_builds_v1" + where +
		               "),(SELECT count(*) FROM __ducklake_metadata_lake.ducklake_vortex_rewrites_v1" + where + ")");
		auto builds = ownership->GetValue(0, 0).GetValue<idx_t>();
		auto rewrites = ownership->GetValue(1, 0).GetValue<idx_t>();
		Require(builds == 0 && rewrites == 0,
		        "Queued rewrite left builds=" + to_string(builds) + " rewrites=" + to_string(rewrites));
		Require(late->HasError() && late->GetErrorObject().Type() == ExceptionType::TRANSACTION &&
		            late->GetErrorObject().RawMessage() == "Vortex rewrite table disappeared before allocation",
		        "Queued rewrite did not reject the retired table at allocation: " + late->ToString());
	} else {
		Require(late->HasError() && late->GetErrorObject().Type() == ExceptionType::CATALOG &&
		            StringUtil::Contains(late->GetErrorObject().RawMessage(), "Table with name items does not exist"),
		        "Concurrent publisher did not see the retired table: " + late->ToString());
	}
	auto after = UUIDSnapshot(database);
	Require(retire() == uuid && UUIDSnapshot(database) == after, "Concurrent publication left retired UUID state");
	UUIDSave(database, root, "uuid-complete");
}

inline void UUIDAttach(Connection &setup, const string &root) {
	SQL(setup, "ATTACH " + Value("ducklake:" + root + "/metadata.ducklake").ToSQLString() + " AS lake(DATA_PATH " +
	               Value(root + "/data").ToSQLString() + ",DATA_INLINING_ROW_LIMIT 1000)");
}

inline DuckLakeVortexOptimizerConfig UUIDOptimizerConfig(const string &catalog, const string &root, uint32_t dimension,
                                                         idx_t rows) {
	DuckLakeVortexOptimizerConfig config;
	config.catalog = catalog;
	config.schema = "main";
	config.table = "items";
	config.index = "knn";
	config.root = root + "/indexes";
	config.dimension = dimension;
	config.flush_rows = rows;
	config.flush_bytes = 4 * 1024 * 1024;
	config.index_rows = rows;
	config.index_bytes = 64 * 1024 * 1024;
	return config;
}

inline void RunUUIDForeignCatalog(DuckDB &database, Connection &setup, const string &root, bool reattach_lake) {
	SQL(setup, "SELECT * FROM ducklake_initialize_vortex_optimizer('lake'); CHECKPOINT __ducklake_metadata_lake;"
	           "DETACH lake");
	Write(root + "/mirror.ducklake", LifecycleRead(root + "/metadata.ducklake"));
	UUIDAttach(setup, root);
	SQL(setup, "ATTACH " + Value("ducklake:" + root + "/mirror.ducklake").ToSQLString() + " AS mirror(DATA_PATH " +
	               Value(root + "/mirror-data").ToSQLString() +
	               ",OVERRIDE_DATA_PATH TRUE,DATA_INLINING_ROW_LIMIT 1000)");
	auto uuid = TableUUID(setup);
	Require(TableUUID(setup, "mirror") == uuid, "Both catalogs must have the same table UUID");
	Write(root + "/table-uuid", uuid);
	auto load = std::make_shared<Pause>(), finished = std::make_shared<Pause>();
	ReleasePause release_load {*load}, release_finished {*finished};
	auto config = UUIDOptimizerConfig("mirror", root, 2, 128);
	config.flush_rows = 2;
	DuckLakeVortexOptimizer::Enable(*setup.context, config, [load, finished](const string &stage) {
		if (stage == "optimizer_before_load") {
			load->Stop();
		} else if (stage == "optimizer_finished") {
			finished->Stop();
		}
	});
	load->Wait();
	DuckLakeVortexOptimizer::Enable(*setup.context, UUIDOptimizerConfig("lake", root, 2, 128));
	if (reattach_lake) {
		// The durable policy is enabled, but this new attachment has no runtime registration.
		SQL(setup, "DETACH lake");
		UUIDAttach(setup, root);
	}
	DuckLakeVortexOptimizer::Cancel(*setup.context, "lake", "main", "items", "knn");
	Connection owner(database);
	Require(DuckLakeVortexWriteBatch::RetireTable(owner, *setup.context, "lake", "main", "items", uuid) == uuid,
	        "A task in a different live catalog blocked retirement");
	DuckLakeVortexWriteBatch::Manage(owner, *setup.context, "mirror", "main", "items", "knn");
	auto rows = SQL(setup, "SELECT list({'id':i,'embedding':[i::FLOAT,0::FLOAT],"
	                       "'category':'A'::VARCHAR,'payload':'mirror-'||i} "
	                       "ORDER BY i) FROM range(1,3)t(i)")
	                ->GetValue(0, 0);
	auto receipt = DuckLakeVortexWriteBatch::Mutate(owner, *setup.context, "mirror", "main", "items", "knn", "two",
	                                                rows, Value::LIST(LogicalType::BIGINT, {}), 1, uuid);
	Require(receipt.table_uuid == uuid && receipt.row_count == 2, "Mirror write did not commit both rows");
	load->Release();
	finished->Wait();
	auto result = SQL(setup, "SELECT enabled AND error='' AND run_count=1 AND NOT running AND "
	                         "(SELECT count(*)=1 AND sum(record_count)=2 FROM "
	                         "__ducklake_metadata_mirror.ducklake_data_file "
	                         "WHERE end_snapshot IS NULL) AND "
	                         "(SELECT list(id ORDER BY id)=[1,2] FROM mirror.items) "
	                         "FROM ducklake_vortex_optimizer_status('mirror')");
	Require(result->RowCount() == 1 && result->GetValue(0, 0).GetValue<bool>() && TableUUID(setup, "mirror") == uuid,
	        "Mirror optimizer did not flush the committed rows independently");
	DuckLakeVortexOptimizer::Cancel(*setup.context, "mirror", "main", "items", "knn");
	finished->Release();
	UUIDSave(database, root, "uuid-complete");
}

inline bool UUIDRetirementBlocked(DuckDB &database, const string &uuid, const string &reason) {
	Connection caller(database), owner(database);
	try {
		auto retired = DuckLakeVortexWriteBatch::RetireTable(owner, *caller.context, "lake", "main", "items", uuid);
		Require(retired == uuid, "Retirement acknowledged a different table UUID");
		return false;
	} catch (const Exception &error) {
		ErrorData detail(error);
		if (detail.Type() != ExceptionType::TRANSACTION || detail.RawMessage() != "Failed to commit: " + reason) {
			throw;
		}
		return true;
	}
}

inline void UUIDRetireAfterWorkerExit(DuckDB &database, const string &uuid, const string &cancelled) {
	const string reason = "Vortex table retirement is blocked by an in-flight optimizer task";
	auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
	while (UUIDRetirementBlocked(database, uuid, reason)) {
		Require(UUIDSnapshot(database) == cancelled, "In-flight retirement changed table state");
		Require(std::chrono::steady_clock::now() < deadline, "Cancelled UUID worker did not stop");
		std::this_thread::sleep_for(std::chrono::milliseconds(5));
	}
}

inline void RunUUIDReattachedCapacity(DuckDB &database, Connection &setup, const string &root) {
	SQL(setup, "SELECT * FROM ducklake_initialize_vortex_optimizer('lake')");
	const auto config = UUIDOptimizerConfig("lake", root, 8, 100000);
	const string in_flight = "Vortex table retirement is blocked by an in-flight optimizer task";
	set<string> identities;
	for (idx_t cycle = 0; cycle < 17; cycle++) {
		if (cycle) {
			SQL(setup, "CREATE TABLE lake.items(id BIGINT,embedding FLOAT[],category VARCHAR,payload VARCHAR);"
			           "CALL ducklake_set_option('lake','experimental_data_file_format','vortex',table_name=>'items');"
			           "SELECT * FROM ducklake_create_vortex_index('lake','main','items','knn'," +
			               Value(config.root).ToSQLString() + ",8)");
		}
		auto uuid = TableUUID(setup);
		Require(identities.insert(uuid).second, "Recreation reused a table UUID");
		SQL(setup, "SELECT * FROM ducklake_manage_vortex_writes('lake','main','items','knn')");
		auto receipt =
		    SQL(setup,
		        "SELECT table_uuid FROM ducklake_mutate_vortex_batch('lake','main','items','knn',"
		        "'cycle',[{'id':" +
		            to_string(cycle) +
		            "::BIGINT,"
		            "'embedding':[128,0,0,0,1,8,7,16]::FLOAT[],'category':'B'::VARCHAR,'payload':NULL::VARCHAR}],"
		            "[]::BIGINT[],1::UBIGINT," +
		            Value(uuid).ToSQLString() + ")")
		        ->GetValue(0, 0);
		Require(receipt.ToString() == uuid, "Write did not produce a receipt for the current UUID");

		auto load = std::make_shared<Pause>(), error = std::make_shared<Pause>();
		ReleasePause release_load {*load}, release_error {*error};
		DuckLakeVortexOptimizer::Enable(*setup.context, config, [load, error](const string &stage) {
			if (stage == "optimizer_before_load") {
				load->Stop();
			} else if (stage == "optimizer_before_error") {
				error->Stop();
			}
		});
		load->Wait();
		DuckLakeVortexOptimizer::Cancel(*setup.context, "lake", "main", "items", "knn");
		DuckLakeVortexOptimizer::SetWriteBudget(*setup.context, "lake", "main", "items", "knn",
		                                        {1000, 4 * 1024 * 1024, 4096, 64 * 1024 * 1024});
		auto cancelled = UUIDSnapshot(database);
		auto original = DuckLakeVortexIndex::ResolveCatalog(*setup.context, "lake", false, "UUID fixture");
		// The worker owns its registration, but has yet to open its metadata connection.
		SQL(setup, "DETACH lake");
		UUIDAttach(setup, root);
		auto rebound = DuckLakeVortexIndex::ResolveCatalog(*setup.context, "lake", false, "UUID fixture");
		Require(original.lake_oid != rebound.lake_oid && original.metadata_oid != rebound.metadata_oid &&
		            TableUUID(setup) == uuid && UUIDSnapshot(database) == cancelled,
		        "Reattachment did not preserve the table and replace both bindings");
		load->Release();
		error->Wait();
		Require(UUIDRetirementBlocked(database, uuid, in_flight) && UUIDSnapshot(database) == cancelled,
		        "Old worker lost its registration during error handling");
		error->Release();
		UUIDRetireAfterWorkerExit(database, uuid, cancelled);
		for (auto name :
		     {"ducklake_vortex_indexes_v1", "ducklake_vortex_generation_refs_v1", "ducklake_vortex_builds_v1",
		      "ducklake_vortex_rewrites_v1", "ducklake_vortex_keyed_tables_v1", "ducklake_vortex_optimizer_v2",
		      "ducklake_vortex_optimizer_rewrite_receipts_v1", "ducklake_vortex_write_budgets_v1",
		      "ducklake_vortex_request_epochs_v1", "ducklake_vortex_write_requests_v2"}) {
			auto rows = SQL(setup, "SELECT count(*) FROM __ducklake_metadata_lake." + string(name) +
			                           " WHERE table_uuid=" + Value(uuid).ToSQLString());
			Require(rows->GetValue(0, 0).GetValue<idx_t>() == 0, "Retired UUID still owns rows in " + string(name));
		}
		Write(root + "/table-uuid", uuid);
		std::cout << "Retired reattached UUID " << uuid << " in cycle " << cycle + 1 << std::endl;
	}
	UUIDSave(database, root, "uuid-complete");
}

inline void RunUUIDFixture(DuckDB &database, Connection &setup, const string &root, const string &mode) {
	if (mode == "uuid-reattached-policy-capacity-reuse") {
		RunUUIDReattachedCapacity(database, setup, root);
		return;
	}
	if (mode == "uuid-live-catalog" || mode == "uuid-rebound-catalog") {
		RunUUIDForeignCatalog(database, setup, root, mode == "uuid-rebound-catalog");
		return;
	}
	if (mode == "uuid-concurrent-index" || mode == "uuid-concurrent-build" || mode == "uuid-concurrent-rewrite") {
		RunUUIDConcurrentPublication(database, setup, root, mode);
		return;
	}
	LifecycleFixture f(database, setup, root);
	auto uuid = TableUUID(setup);
	Write(root + "/table-uuid", uuid);
	auto retire = [&](const std::function<void(const string &)> &observe = nullptr) {
		Connection caller(database), owner(database);
		return DuckLakeVortexWriteBatch::RetireTable(owner, *caller.context, "lake", "main", "items", uuid, observe);
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
		auto enabled = UUIDSnapshot(database);
		Require(UUIDRetirementBlocked(database, uuid, "Cancel Vortex optimizer tasks before retiring the table UUID") &&
		            UUIDSnapshot(database) == enabled,
		        "Enabled optimizer lost UUID ownership");
		DuckLakeVortexOptimizer::Cancel(*setup.context, "lake", "main", "items", "knn");
		const string in_flight = "Vortex table retirement is blocked by an in-flight optimizer task";
		auto cancelled = UUIDSnapshot(database);
		Require(UUIDRetirementBlocked(database, uuid, in_flight) && UUIDSnapshot(database) == cancelled,
		        "Cancelled in-flight task lost UUID ownership");
		SQL(setup, "DETACH lake");
		UUIDAttach(setup, root);
		Require(TableUUID(setup) == uuid && UUIDRetirementBlocked(database, uuid, in_flight) &&
		            UUIDSnapshot(database) == cancelled,
		        "Reattachment bypassed the cancelled in-flight task");
		pause.Release();
		UUIDRetireAfterWorkerExit(database, uuid, cancelled);
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
			DuckLakeVortexWriteBatch::RetireTable(
			    owner, *setup.context, "lake", "main", "items", uuid, [&](const string &stage) {
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
