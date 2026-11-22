#pragma once

#include "native_vortex_fixture.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_vortex_index_catalog.hpp"
#include "storage/ducklake_vortex_optimizer.hpp"
#include "storage/ducklake_vortex_search.hpp"
#include "storage/ducklake_vortex_source_retention.hpp"
#include "storage/ducklake_vortex_write.hpp"
#include <iostream>
#include <thread>

namespace duckdb {
namespace vortex_test {
struct KeyedFixture {
	DuckDB &database;
	Connection &setup;
	string root;
	DuckLakeVortexIndex index;
	Value rows, deletes;
	KeyedFixture(DuckDB &db, Connection &connection, const string &path)
	    : database(db), setup(connection), root(path),
	      rows(SQL(setup, "SELECT [{'id':0::BIGINT,'embedding':[0.125,0]::FLOAT[],'category':'A'::VARCHAR,"
	                      "'payload':'更新'::VARCHAR},{'id':10000::BIGINT,'embedding':[0.25,0]::FLOAT[],"
	                      "'category':NULL::VARCHAR,'payload':NULL::VARCHAR}]")
	               ->GetValue(0, 0)),
	      deletes(Value::LIST(LogicalType::BIGINT, {Value::BIGINT(1), Value::BIGINT(99999)})) {
	}
	void Manage() {
		Connection owner(database);
		DuckLakeVortexWriteBatch::Manage(owner, *setup.context, "lake", "main", "items", "knn");
	}
	void Seed() {
		Manage();
		SQL(setup, "SELECT * FROM ducklake_initialize_vortex_optimizer('lake');CREATE TABLE lake.other(id BIGINT)");
		auto seed = SQL(setup, "SELECT list({'id':i::BIGINT,'embedding':[i::FLOAT,0::FLOAT],"
		                       "'category':'A'::VARCHAR,'payload':'seed-'||i}) FROM range(256)t(i)")
		                ->GetValue(0, 0);
		Connection owner(database);
		DuckLakeVortexWriteBatch::Mutate(owner, *setup.context, "lake", "main", "items", "knn", "seed", seed,
		                                 Value::LIST(LogicalType::BIGINT, {}));
		SQL(setup, "CALL ducklake_flush_inlined_data('lake',table_name=>'items')");
		index = DuckLakeVortexIndex::BuildAndPublish(*setup.context, "lake", "main", "items", "knn",
		                                             root + "/indexes/seed.json", 2, 1, nullptr, "", {},
		                                             DuckLakeVortexIndexOperation::APPEND);
		SQL(setup, "SELECT * FROM ducklake_enable_vortex_optimizer('lake','main','items','knn'," +
		               Value(root + "/indexes").ToSQLString() +
		               ",2,100000,4194304,100000,67108864);"
		               "SELECT * FROM ducklake_cancel_vortex_optimizer('lake','main','items','knn');"
		               "SELECT * FROM "
		               "ducklake_set_vortex_write_budget('lake','main','items','knn',1000,4194304,4096,67108864)");
		auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
		while (DuckLakeVortexOptimizer::Status(*setup.context, "lake")[0][16].GetValue<bool>()) {
			Require(std::chrono::steady_clock::now() < deadline, "Seed optimizer did not stop");
			std::this_thread::sleep_for(std::chrono::milliseconds(5));
		}
	}
	DuckLakeVortexWriteResult Mutate(Connection &owner, const std::function<void(const string &)> &observe = {}) {
		return DuckLakeVortexWriteBatch::Mutate(owner, *setup.context, "lake", "main", "items", "knn", "change", rows,
		                                        deletes, observe);
	}
	DuckLakeVortexWriteResult DeleteMissing(Connection &owner, ClientContext &caller,
	                                        const std::function<void(const string &)> &observe = {}) {
		return DuckLakeVortexWriteBatch::Mutate(
		    owner, caller, "lake", "main", "items", "knn", "absent",
		    Value::LIST(ListType::GetChildType(DuckLakeVortexWriteBatch::RowsType()), {}),
		    Value::LIST(LogicalType::BIGINT, {Value::BIGINT(99999)}), observe);
	}
	DuckLakeVortexSearchResult Search(Connection &reader, const std::function<void(const string &)> &observe = {}) {
		DuckLakeVortexViewOptions options {};
		options.catalog = "lake";
		options.schema = "main";
		options.table = "items";
		options.index_name = "knn";
		options.max_source_bytes = 64 * 1024 * 1024;
		options.max_rows = 100000;
		DuckLakeVortexSearchRequest request;
		request.query = {0, 0};
		request.k = 3;
		return DuckLakeVortexSearch::Execute(*reader.context, options, request, observe);
	}
	void Check(bool changed) {
		auto result = SQL(setup, "SELECT payload FROM lake.items WHERE id=0");
		Require(result->GetValue(0, 0).ToString() == (changed ? "更新" : "seed-0"),
		        "Mutation exposed a partial version");
		Require(SQL(setup, "SELECT count(*) FROM lake.items")->GetValue(0, 0).GetValue<idx_t>() == 256,
		        "Mutation duplicated or lost current keys");
		Require(SQL(setup, "SELECT count(*) FROM __ducklake_metadata_lake.ducklake_vortex_write_requests_v1 "
		                   "WHERE request_id='change'")
		                ->GetValue(0, 0)
		                .GetValue<idx_t>() == (changed ? 1 : 0),
		        "Mutation separated rows from its receipt");
		Connection reader(database);
		auto search = Search(reader);
		Require(search.rows[0][0].GetValue<int64_t>() == 0 &&
		            search.rows[0][3].ToString() == (changed ? "更新" : "seed-0") &&
		            search.rows[1][0].GetValue<int64_t>() == (changed ? 10000 : 1),
		        "ANN returned an old or deleted version before top-k");
	}
};

inline void CheckKeyedRebind(KeyedFixture &fixture, bool receipt_only) {
	fixture.Manage();
	auto &setup = fixture.setup;
	auto attach = [&](const string &file, const string &data) {
		SQL(setup, "DETACH lake; ATTACH " + Value("ducklake:" + fixture.root + "/" + file).ToSQLString() +
		               " AS lake(DATA_PATH " + Value(fixture.root + "/" + data).ToSQLString() +
		               ",DATA_INLINING_ROW_LIMIT 1000)");
	};
	bool switched = false;
	auto switch_catalog = [&] {
		attach("replacement.ducklake", "replacement-data");
		switched = true;
	};
	bool rejected = false;
	{
		Connection writer(fixture.database);
		try {
			if (receipt_only) {
				fixture.DeleteMissing(writer, *setup.context, [&](const string &stage) {
					if (stage == "before_commit") {
						switch_catalog();
					}
				});
			} else {
				writer.BeginTransaction();
				SQL(writer, "INSERT INTO lake.items VALUES(7,[7,0],'A','stale')");
				switch_catalog();
				writer.Commit();
			}
		} catch (const std::exception &error) {
			Require(StringUtil::Contains(error.what(), "attachment changed"), error.what());
			rejected = true;
		}
	}
	Require(switched && rejected, "A stale attachment committed managed rows or a receipt");
	attach("metadata.ducklake", "data");
	Require(SQL(setup, "SELECT count(*) FROM lake.items")->GetValue(0, 0).GetValue<idx_t>() == 0,
	        "Stale ordinary writer changed managed rows");
	Require(SQL(setup, "SELECT count(*) FROM __ducklake_metadata_lake.ducklake_vortex_write_requests_v1")
	                ->GetValue(0, 0)
	                .GetValue<idx_t>() == 0,
	        "Stale missing-key delete committed a request receipt");
}

inline void CheckKeyedImmediateAttachments(KeyedFixture &fixture) {
	fixture.Manage();
	auto &setup = fixture.setup;
	SQL(setup, "ATTACH " + Value("ducklake:" + fixture.root + "/neighbor.ducklake").ToSQLString() +
	               " AS neighbor(DATA_PATH " + Value(fixture.root + "/neighbor-data").ToSQLString() +
	               ", DATA_INLINING_ROW_LIMIT 1000)");
	SQL(setup, "CREATE TABLE neighbor.items(id BIGINT,embedding FLOAT[],category VARCHAR,payload VARCHAR);"
	           "CALL ducklake_set_option('neighbor','experimental_data_file_format','vortex',table_name=>'items');"
	           "SELECT * FROM ducklake_initialize_vortex_indexes('neighbor');"
	           "SELECT * FROM ducklake_initialize_vortex_writes('neighbor')");
	DuckLakeVortexIndex::Create(*setup.context, "neighbor", "main", "items", "knn", fixture.root + "/indexes", 2);
	SQL(setup, "SET GLOBAL immediate_transaction_mode=true");

	Connection owner(fixture.database);
	auto missing = fixture.DeleteMissing(owner, *setup.context);
	auto retry = fixture.DeleteMissing(owner, *setup.context);
	Require(missing.row_count == 1 && retry.payload_hash == missing.payload_hash &&
	            retry.snapshot_id == missing.snapshot_id,
	        "Missing-key receipt changed with another attached lake");

	auto append = DuckLakeVortexWriteBatch::Append(owner, *setup.context, "neighbor", "main", "items", "knn", "append",
	                                               fixture.rows);
	Require(append.row_count == 2 &&
	            SQL(setup, "SELECT count(*) FROM neighbor.items")->GetValue(0, 0).GetValue<idx_t>() == 2,
	        "An unrelated lake blocked append in immediate transaction mode");
	Require(SQL(setup, "SELECT count(*) FROM lake.items")->GetValue(0, 0).GetValue<idx_t>() == 0,
	        "Append or missing-key deletion changed the wrong lake");
	for (auto name : {"lake", "neighbor"}) {
		Require(SQL(setup,
		            "SELECT count(*) FROM __ducklake_metadata_" + string(name) + ".ducklake_vortex_write_requests_v1")
		                ->GetValue(0, 0)
		                .GetValue<idx_t>() == 1,
		        "The attached catalogs did not retain their own receipts");
	}
}

inline void CheckOrdinaryRetryAttachments(KeyedFixture &fixture, bool rebind) {
	auto &setup = fixture.setup;
	auto attach = [&](const string &name, const string &path, const string &data) {
		return "ATTACH " + Value("ducklake:" + fixture.root + "/" + path).ToSQLString() + " AS " + name +
		       "(DATA_PATH " + Value(fixture.root + "/" + data).ToSQLString() + ", DATA_INLINING_ROW_LIMIT 1000)";
	};
	SQL(setup, "CREATE TABLE lake.destination(id BIGINT); CREATE TABLE lake.advance(id BIGINT)");
	if (rebind) {
		SQL(setup, attach("prepared", "replacement.ducklake", "replacement-data") +
		               "; CREATE TABLE prepared.destination(id BIGINT);"
		               "INSERT INTO prepared.destination VALUES(17); DETACH prepared");
	}

	Connection writer(fixture.database);
	writer.BeginTransaction();
	SQL(writer, "INSERT INTO lake.destination VALUES(41)");
	// Start the first commit attempt with an older snapshot than the committed metadata.
	SQL(setup, "INSERT INTO lake.advance VALUES(1)");
	auto original = DatabaseManager::Get(*setup.context).GetDatabase("lake");
	auto &catalog = original->GetCatalog().Cast<DuckLakeCatalog>();
	idx_t attempts = 0;
	catalog.SetQueryCallback([&](const string &query, std::chrono::steady_clock::duration) {
		if (!StringUtil::Contains(query, ".ducklake_snapshot VALUES")) {
			return;
		}
		attempts++;
		if (rebind && attempts == 1) {
			SQL(setup, "DETACH lake; " + attach("lake", "replacement.ducklake", "replacement-data"));
		}
	});
	bool rejected = false;
	try {
		writer.Commit();
	} catch (const std::exception &error) {
		catalog.SetQueryCallback({});
		Require(rebind && StringUtil::Contains(error.what(), "attachment changed before commit retry"), error.what());
		rejected = true;
	}
	catalog.SetQueryCallback({});
	if (!rebind) {
		Require(!rejected && attempts == 2 &&
		            SQL(setup, "SELECT id FROM lake.destination")->GetValue(0, 0).GetValue<int64_t>() == 41,
		        "Ordinary native commit did not retry on unchanged attachments");
		return;
	}
	auto replacement = SQL(setup, "SELECT count(*),min(id) FROM lake.destination");
	Require(rejected && attempts == 1 && replacement->GetValue(0, 0).GetValue<idx_t>() == 1 &&
	            replacement->GetValue(1, 0).GetValue<int64_t>() == 17,
	        "Native commit retry affected the replacement attachment");
	original.reset();
	SQL(setup, "DETACH lake; " + attach("lake", "metadata.ducklake", "data"));
	Require(SQL(setup, "SELECT count(*) FROM lake.destination")->GetValue(0, 0).GetValue<idx_t>() == 0,
	        "Failed retry changed the original lake");
}

inline void RunKeyedFixture(DuckDB &database, Connection &setup, const string &root, const string &mode) {
	KeyedFixture f(database, setup, root);
	if (mode == "keyed-retry-ordinary" || mode == "keyed-retry-rebind") {
		CheckOrdinaryRetryAttachments(f, mode == "keyed-retry-rebind");
		return;
	}
	if (mode == "keyed-immediate-attachments") {
		CheckKeyedImmediateAttachments(f);
		return;
	}
	if (mode == "keyed-rebind-ordinary" || mode == "keyed-rebind-noop") {
		CheckKeyedRebind(f, mode == "keyed-rebind-noop");
		return;
	}
	if (mode == "keyed-enroll-with-segment") {
		DuckLakeVortexIndex::Create(*setup.context, "lake", "main", "items", "empty", root + "/indexes", 2);
		SQL(setup, "INSERT INTO lake.items SELECT i,[i::FLOAT,0],'A','seed' FROM range(256)t(i);"
		           "CALL ducklake_flush_inlined_data('lake',table_name=>'items')");
		auto indexed = DuckLakeVortexIndex::BuildAndPublish(*setup.context, "lake", "main", "items", "knn",
		                                                    root + "/indexes/primary.json", 2, 1, nullptr, "", {},
		                                                    DuckLakeVortexIndexOperation::APPEND);
		Require(indexed.segments.size() == 1, "Fixture did not publish B in the original index");
		SQL(setup, "DELETE FROM lake.items WHERE id>=0");
		Require(SQL(setup, "SELECT count(*) FROM lake.items")->GetValue(0, 0).GetValue<idx_t>() == 0,
		        "Fixture still has live rows");
		Connection enrollment(database);
		bool rejected = false;
		try {
			DuckLakeVortexWriteBatch::Manage(enrollment, *setup.context, "lake", "main", "items", "empty");
		} catch (const InvalidInputException &error) {
			Require(StringUtil::Contains(error.what(), "without prior batches or B segments"), error.what());
			rejected = true;
		}
		Require(rejected && SQL(setup, "SELECT count(*) FROM __ducklake_metadata_lake.ducklake_vortex_keyed_tables_v1")
		                            ->GetValue(0, 0)
		                            .GetValue<idx_t>() == 0,
		        "Enrollment ignored another index's B segment");
		return;
	}
	if (mode == "keyed-preexisting") {
		Connection stale(database);
		stale.BeginTransaction();
		SQL(stale, "INSERT INTO lake.items VALUES(7,[7,0],'A','uncoordinated')");
		f.Manage();
		bool rejected = false;
		try {
			stale.Commit();
		} catch (std::exception &) {
			rejected = true;
		}
		Require(rejected && SQL(setup, "SELECT count(*) FROM lake.items")->GetValue(0, 0).GetValue<idx_t>() == 0,
		        "A pre-existing writer bypassed managed ownership at commit");
		Connection owner(database);
		f.Mutate(owner);
		return;
	}
	f.Seed();
	if (mode == "keyed-held") {
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
		Connection owner(database);
		f.Mutate(owner);
		f.Check(true);
		std::cout << "keyed-held: post-mutation flush" << std::endl;
		SQL(setup, "CALL ducklake_flush_inlined_data('lake',table_name=>'items')");
		auto old_source = f.index.files[0].uri;
		std::cout << "keyed-held: source rewrite" << std::endl;
		f.index = DuckLakeVortexIndex::Rewrite(*setup.context, "lake", "main", "items", "knn",
		                                       f.index.segments[0].generation, root + "/indexes/rewrite.json", 2);
		DuckLakeVortexIndex::Collect(*setup.context, "lake", 16, 64 * 1024 * 1024);
		std::cout << "keyed-held: source GC" << std::endl;
		DuckLakeVortexOptimizer::ConfigureSourceRetention(*setup.context, "lake", true, 1, 0);
		for (idx_t pass = 0; pass < 64; pass++) {
			auto collected = DuckLakeVortexSourceRetention::Collect(*setup.context, "lake", 16, 64 * 1024 * 1024);
			Require(!collected.blocked, "A fully pinned query kept a native transaction open");
			if (!collected.expired && !collected.files) {
				break;
			}
		}
		Require(!FileSystem::GetFileSystem(*setup.context).FileExists(old_source), "Old B source was not reclaimed");
		f.Check(true);
		pause.Release();
		auto previous = held.get();
		Require(previous.rows[0][0].GetValue<int64_t>() == 0 && previous.rows[0][3].ToString() == "seed-0" &&
		            previous.rows[1][0].GetValue<int64_t>() == 1,
		        "Old view lost its pre-mutation typed rows after GC");
		return;
	}
	Connection owner(database);
	bool observed = false;
	auto observe = [&](const string &stage) {
		if (stage == "after_delete" || stage == "before_commit") {
			f.Check(false);
		}
		if (mode == "keyed-conflict" && stage == "before_commit") {
			Connection other(database);
			SQL(other, "INSERT INTO lake.other VALUES(1)");
			observed = true;
		}
		if (mode == "keyed-cancel-" + stage) {
			owner.context->Interrupt();
			observed = true;
		}
		if (mode == "keyed-fail-" + stage) {
			observed = true;
			throw IOException("Injected keyed batch failure");
		}
		if (mode == "keyed-pause-" + stage) {
			Write(root + "/checkpoint", stage);
			auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
			while (!FileSystem::GetFileSystem(*setup.context).FileExists(root + "/release")) {
				Require(std::chrono::steady_clock::now() < deadline, "Keyed checkpoint was not killed");
				std::this_thread::sleep_for(std::chrono::milliseconds(5));
			}
		}
	};
	try {
		f.Mutate(owner, observe);
		Require(mode == "keyed-ok", "Keyed fault did not fire");
	} catch (std::exception &error) {
		Require(observed, error.what());
		std::cout << "expected error: " << error.what() << std::endl;
	}
	f.Check(mode == "keyed-ok" || mode == "keyed-fail-after_commit");
	Connection retry(database);
	auto receipt = f.Mutate(retry);
	Require(receipt.row_count == 4 && f.Mutate(retry).snapshot_id == receipt.snapshot_id,
	        "Keyed retry changed its receipt");
	f.Check(true);
}
} // namespace vortex_test
} // namespace duckdb
