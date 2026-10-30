#pragma once

#include "native_vortex_fixture.hpp"
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
		DuckLakeVortexWriteBatch::Manage(owner, "lake", "main", "items", "knn");
	}
	void Seed() {
		Manage();
		SQL(setup, "SELECT * FROM ducklake_initialize_vortex_optimizer('lake');CREATE TABLE lake.other(id BIGINT)");
		auto seed = SQL(setup, "SELECT list({'id':i::BIGINT,'embedding':[i::FLOAT,0::FLOAT],"
		                       "'category':'A'::VARCHAR,'payload':'seed-'||i}) FROM range(256)t(i)")
		                ->GetValue(0, 0);
		Connection owner(database);
		DuckLakeVortexWriteBatch::Mutate(owner, "lake", "main", "items", "knn", "seed", seed,
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
		return DuckLakeVortexWriteBatch::Mutate(owner, "lake", "main", "items", "knn", "change", rows, deletes,
		                                        observe);
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

inline void RunKeyedFixture(DuckDB &database, Connection &setup, const string &root, const string &mode) {
	KeyedFixture f(database, setup, root);
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
