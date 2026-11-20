#include "native_vortex_fixture.hpp"
#include "ducklake_extension.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "vortex_extension.hpp"
#include "storage/ducklake_vortex_write.hpp"

#include <future>
#include <iostream>
#include <thread>

using namespace duckdb;
using namespace duckdb::vortex_test;

static void CheckQueuedCancellation(DuckDB &database, Connection &setup, const Value &rows) {
	Connection holder(database), queued(database), caller(database);
	std::promise<void> acquired, release, waiting;
	auto acquired_ready = acquired.get_future();
	auto release_ready = release.get_future();
	auto waiting_ready = waiting.get_future();
	auto held = std::async(std::launch::async, [&] {
		return DuckLakeVortexWriteBatch::Append(
		    holder, *setup.context, "lake", "main", "items", "knn", "holder", rows, [&](const string &stage) {
			    if (stage == "acquired") {
				    acquired.set_value();
				    Require(release_ready.wait_for(std::chrono::seconds(20)) == std::future_status::ready,
				            "Timed out releasing the holder");
			    }
		    });
	});
	if (acquired_ready.wait_for(std::chrono::seconds(5)) != std::future_status::ready) {
		release.set_value();
		held.get();
		throw std::runtime_error("Holder did not acquire the write lock");
	}
	auto canceled = std::async(std::launch::async, [&] {
		bool notified = false;
		try {
			DuckLakeVortexWriteBatch::Append(queued, *caller.context, "lake", "main", "items", "knn", "queued", rows,
			                                 [&](const string &stage) {
				                                 if (stage == "lock_wait" && !notified) {
					                                 notified = true;
					                                 waiting.set_value();
				                                 }
			                                 });
		} catch (const InterruptException &) {
			return true;
		}
		return false;
	});
	auto reached_lock = waiting_ready.wait_for(std::chrono::seconds(5)) == std::future_status::ready;
	caller.Interrupt();
	auto resolved_before_release = canceled.wait_for(std::chrono::seconds(5)) == std::future_status::ready;
	release.set_value();
	Require(held.get().row_count == 2, "Holding writer changed its batch");
	Require(reached_lock && resolved_before_release && canceled.get(), "Queued writer did not cancel while locked");
	Require(SQL(setup, "SELECT count(*) FROM lake.items")->GetValue(0, 0).GetValue<idx_t>() == 2,
	        "Canceled writer added rows");
	Require(SQL(setup, "SELECT count(*) FROM __ducklake_metadata_lake.ducklake_vortex_write_requests_v1")
	                ->GetValue(0, 0)
	                .GetValue<idx_t>() == 1,
	        "Canceled writer added a receipt");
}

int main(int argc, char **argv) {
	try {
		Require(argc == 3, "Usage: ducklake_vortex_write_driver ROOT MODE");
		string root = argv[1], mode = argv[2];
		bool file_backed = StringUtil::StartsWith(mode, "file-");
		if (file_backed) {
			mode.erase(0, 5);
		}
		DuckDB database(nullptr);
		database.LoadStaticExtension<DucklakeExtension>();
		database.LoadStaticExtension<VortexExtension>();
		Connection setup(database);
		auto &fs = FileSystem::GetFileSystem(*setup.context);
		bool create = !fs.FileExists(root + "/metadata.ducklake");
		SQL(setup, "SET threads=2; LOAD parquet; ATTACH " +
		               Value("ducklake:" + root + "/metadata.ducklake").ToSQLString() + " AS lake(DATA_PATH " +
		               Value(root + "/data").ToSQLString() +
		               (file_backed ? ",DATA_INLINING_ROW_LIMIT 0)" : ",DATA_INLINING_ROW_LIMIT 1000)"));
		if (create) {
			fs.CreateDirectory(root + "/indexes");
			SQL(setup, "CREATE TABLE lake.items(id BIGINT,embedding FLOAT[],category VARCHAR,payload VARCHAR);"
			           "CALL ducklake_set_option('lake','experimental_data_file_format','vortex',table_name=>'items');"
			           "SELECT * FROM ducklake_initialize_vortex_indexes('lake');"
			           "SELECT * FROM ducklake_create_vortex_index('lake','main','items','knn'," +
			               Value(root + "/indexes").ToSQLString() + ",2)");
			if (mode != "cancel-init") {
				SQL(setup, "SELECT * FROM ducklake_initialize_vortex_writes('lake')");
			}
		}
		Connection owner(database);
		if (mode == "cancel-init") {
			Connection caller(database);
			caller.Interrupt();
			bool canceled = false;
			try {
				DuckLakeVortexWriteBatch::Initialize(owner, *caller.context, "lake");
			} catch (const InterruptException &) {
				canceled = true;
			}
			Require(canceled &&
			            !setup.TableInfo("__ducklake_metadata_lake", "main", "ducklake_vortex_write_requests_v1"),
			        "Canceled initialization created the request table");
			return 0;
		}
		auto input = SQL(setup, "SELECT [{'id':1::BIGINT,'embedding':[1,0]::FLOAT[],'category':'A'::VARCHAR,"
		                        "'payload':'一'::VARCHAR},{'id':2::BIGINT,'embedding':[2,0]::FLOAT[],"
		                        "'category':NULL::VARCHAR,'payload':NULL::VARCHAR}]");
		auto rows = input->GetValue(0, 0);
		if (mode == "queued-cancel") {
			CheckQueuedCancellation(database, setup, rows);
			return 0;
		}
		bool observed = false, conflict = false;
		auto observer = [&](const string &stage) {
			if (mode == "rebind" && stage == "waiting") {
				SQL(setup, "DETACH lake; ATTACH " + Value("ducklake:" + root + "/replacement.ducklake").ToSQLString() +
				               " AS lake(DATA_PATH " + Value(root + "/replacement-data").ToSQLString() + ")");
				observed = true;
			}
			if (mode == "conflict" && stage == "before_commit") {
				Connection other(database);
				SQL(other, "INSERT INTO lake.items VALUES(900,[900,0],'B','concurrent')");
				observed = true;
			}
			if (mode == "cancel" && stage == "before_commit") {
				owner.context->Interrupt();
				observed = true;
			}
			if (mode == "fail-" + stage) {
				observed = true;
				throw IOException("Injected WriteBatch " + stage);
			}
			if (mode == "pause-" + stage) {
				Write(root + "/checkpoint", stage);
				auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
				while (!fs.FileExists(root + "/release")) {
					Require(std::chrono::steady_clock::now() < deadline, "WriteBatch pause timed out");
					std::this_thread::sleep_for(std::chrono::milliseconds(5));
				}
			}
		};
		try {
			auto result = DuckLakeVortexWriteBatch::Append(owner, *setup.context, "lake", "main", "items", "knn",
			                                               "request", rows, observer);
			Require(mode == "ok" || mode == "retry" || StringUtil::StartsWith(mode, "pause-"),
			        "WriteBatch fault did not fire");
			std::cout << result.request_id << " " << result.row_count << " " << result.snapshot_id << std::endl;
		} catch (std::exception &error) {
			Require(observed, error.what());
			if (mode == "rebind") {
				Require(StringUtil::Contains(error.what(), "attachment changed"), error.what());
				return 0;
			}
			conflict = mode == "conflict";
			Require(mode == "conflict" || mode == "cancel" || StringUtil::StartsWith(mode, "fail-"), error.what());
			std::cout << "expected error: " << error.what() << std::endl;
		}
		if (observed) {
			auto committed = mode == "fail-after_commit";
			auto requests =
			    SQL(setup, "SELECT count(*) FROM __ducklake_metadata_lake.ducklake_vortex_write_requests_v1");
			Require(requests->GetValue(0, 0).GetValue<idx_t>() == (committed ? 1 : 0),
			        "Incorrect durable request count");
			auto count = SQL(setup, "SELECT count(*) FROM lake.items")->GetValue(0, 0).GetValue<idx_t>();
			Require(count == (committed ? 2 : conflict ? 1 : 0), "Incorrect atomic batch count");
			Connection retry(database);
			auto result = DuckLakeVortexWriteBatch::Append(retry, *setup.context, "lake", "main", "items", "knn",
			                                               "request", rows);
			Require(result.row_count == 2, "Retry changed WriteBatch result");
			Require(SQL(setup, "SELECT count(*) FROM lake.items")->GetValue(0, 0).GetValue<idx_t>() ==
			            (conflict ? 3 : 2),
			        "Retry duplicated or lost WriteBatch rows");
		}
		return 0;
	} catch (std::exception &error) {
		std::cerr << error.what() << std::endl;
		return 1;
	}
}
