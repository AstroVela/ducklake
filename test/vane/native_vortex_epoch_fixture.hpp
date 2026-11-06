#pragma once

#include "storage/ducklake_vortex_write.hpp"
#include "duckdb/common/string_util.hpp"

#include <future>
#include <thread>

namespace duckdb {
namespace vortex_test {

struct EpochFixture {
	DuckDB &database;
	Connection &setup;
	string root;
	Value seed, latest, late;

	EpochFixture(DuckDB &database, Connection &setup, string root)
	    : database(database), setup(setup), root(std::move(root)) {
		SQL(setup, "SELECT * FROM ducklake_initialize_vortex_optimizer('lake')");
		Connection owner(database);
		DuckLakeVortexWriteBatch::Manage(owner, "lake", "main", "items", "knn");
		seed = SQL(setup, "SELECT [{'id':1::BIGINT,'embedding':[1,0]::FLOAT[],'category':'A'::VARCHAR,"
		                  "'payload':'seed-1'::VARCHAR},{'id':2::BIGINT,'embedding':[2,0]::FLOAT[],"
		                  "'category':'B'::VARCHAR,'payload':'seed-2'::VARCHAR}]")
		           ->GetValue(0, 0);
		latest = SQL(setup, "SELECT [{'id':1::BIGINT,'embedding':[0.25,0]::FLOAT[],'category':'A'::VARCHAR,"
		                    "'payload':'current'::VARCHAR}]")
		             ->GetValue(0, 0);
		late = SQL(setup, "SELECT [{'id':3::BIGINT,'embedding':[3,0]::FLOAT[],'category':'B'::VARCHAR,"
		                  "'payload':'late'::VARCHAR}]")
		           ->GetValue(0, 0);
		DuckLakeVortexWriteBatch::Mutate(owner, "lake", "main", "items", "knn", "seed", seed,
		                                 Value::LIST(LogicalType::BIGINT, {}), 1);
		DuckLakeVortexWriteBatch::Mutate(owner, "lake", "main", "items", "knn", "latest", latest,
		                                 Value::LIST(LogicalType::BIGINT, {Value::BIGINT(2)}), 1);
	}

	DuckLakeVortexRequestStatus Status() {
		Connection owner(database);
		return DuckLakeVortexWriteBatch::RequestStatus(owner, "lake", "main", "items", "knn");
	}

	DuckLakeVortexRequestAdvance Advance(Connection &owner, const std::function<void(const string &)> &observe = {}) {
		return DuckLakeVortexWriteBatch::AdvanceRequestEpoch(owner, "lake", "main", "items", "knn", 1, observe);
	}

	DuckLakeVortexWriteResult Late(Connection &owner, const std::function<void(const string &)> &observe = {}) {
		return DuckLakeVortexWriteBatch::Mutate(owner, "lake", "main", "items", "knn", "late", late,
		                                        Value::LIST(LogicalType::BIGINT, {}), 1, observe);
	}

	void CurrentRows(idx_t count = 1) {
		Require(SQL(setup, "SELECT count(*) FROM lake.items")->GetValue(0, 0).GetValue<idx_t>() == count,
		        "Epoch cleanup changed business row count");
		Require(StringValue::Get(SQL(setup, "SELECT payload FROM lake.items WHERE id=1")->GetValue(0, 0)) == "current",
		        "Epoch cleanup resurrected an old business version");
	}

	static void Same(const DuckLakeVortexRequestAdvance &a, const DuckLakeVortexRequestAdvance &b) {
		Require(a.table_uuid == b.table_uuid && a.previous_epoch == b.previous_epoch &&
		            a.request_epoch == b.request_epoch && a.expired_requests == b.expired_requests,
		        "Last epoch advance retry changed its durable result");
	}
};

inline void EpochWait(std::future<void> &ready) {
	Require(ready.wait_for(std::chrono::seconds(60)) == std::future_status::ready, "Epoch fixture coordination hung");
	ready.get();
}

inline void RunEpochFixture(DuckDB &database, Connection &setup, const string &root, const string &mode) {
	EpochFixture f(database, setup, root);
	if (mode == "epoch-write-first" || mode == "epoch-advance-first") {
		std::promise<void> held, waiting, release;
		auto held_future = held.get_future(), waiting_future = waiting.get_future();
		auto released = release.get_future().share();
		auto pause = [&](const string &stage) {
			if (stage == "after_lookup") {
				held.set_value();
				released.wait();
			}
		};
		auto entered = [&](const string &stage) {
			if (stage == "waiting") {
				waiting.set_value();
			}
		};
		if (mode == "epoch-write-first") {
			auto writer = std::async(std::launch::async, [&] {
				Connection owner(database);
				return f.Late(owner, pause);
			});
			EpochWait(held_future);
			auto advance = std::async(std::launch::async, [&] {
				Connection owner(database);
				return f.Advance(owner, entered);
			});
			EpochWait(waiting_future);
			Require(advance.wait_for(std::chrono::seconds(0)) != std::future_status::ready,
			        "Advance passed a reserved writer");
			release.set_value();
			Require(writer.get().request_epoch == 1 && advance.get().expired_requests == 3,
			        "Advance did not include the preceding committed receipt");
			f.CurrentRows(2);
		} else {
			auto advance = std::async(std::launch::async, [&] {
				Connection owner(database);
				return f.Advance(owner, pause);
			});
			EpochWait(held_future);
			auto writer = std::async(std::launch::async, [&] {
				Connection owner(database);
				try {
					f.Late(owner, entered);
				} catch (std::exception &error) {
					return string(error.what());
				}
				return string();
			});
			EpochWait(waiting_future);
			Require(writer.wait_for(std::chrono::seconds(0)) != std::future_status::ready,
			        "Writer passed an epoch advance reservation");
			release.set_value();
			Require(advance.get().expired_requests == 2 && writer.get().find("EXPIRED_REQUEST") != string::npos,
			        "An old writer crossed the committed epoch fence");
			f.CurrentRows();
		}
		Require(f.Status().request_epoch == 2 && f.Status().retained_requests == 0,
		        "Race left the wrong request window");
		return;
	}
	if (mode == "epoch-parallel-advance") {
		vector<std::future<DuckLakeVortexRequestAdvance>> tasks;
		for (idx_t i = 0; i < 8; i++) {
			tasks.push_back(std::async(std::launch::async, [&] {
				Connection owner(database);
				return f.Advance(owner);
			}));
		}
		auto first = tasks[0].get();
		for (idx_t i = 1; i < tasks.size(); i++) {
			EpochFixture::Same(first, tasks[i].get());
		}
		Require(first.request_epoch == 2 && first.expired_requests == 2 && f.Status().request_epoch == 2,
		        "Identical concurrent advances moved the epoch more than once");
		f.CurrentRows();
		return;
	}
	Connection reader(database);
	SQL(reader, "BEGIN");
	Require(SQL(reader, "SELECT request_epoch FROM __ducklake_metadata_lake.ducklake_vortex_request_epochs_v1")
	                ->GetValue(0, 0)
	                .GetValue<uint64_t>() == 1,
	        "Initial epoch was not durable");
	Connection owner(database);
	bool observed = false, failed = false;
	auto observe = [&](const string &stage) {
		if (mode == "epoch-fail-" + stage) {
			observed = true;
			throw IOException("Injected epoch advance " + stage);
		}
		if (mode == "epoch-cancel" && stage == "before_commit") {
			observed = true;
			owner.context->Interrupt();
		}
		if (mode == "epoch-conflict" && stage == "after_prune") {
			observed = true;
			Connection other(database);
			SQL(other, "UPDATE __ducklake_metadata_lake.ducklake_vortex_request_epochs_v1 SET last_expired_requests=1");
		}
		if (mode == "epoch-pause-" + stage) {
			Write(root + "/checkpoint", stage);
			auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
			while (!FileSystem::GetFileSystem(*setup.context).FileExists(root + "/release")) {
				Require(std::chrono::steady_clock::now() < deadline, "Epoch pause fixture hung");
				std::this_thread::sleep_for(std::chrono::milliseconds(5));
			}
		}
	};
	DuckLakeVortexRequestAdvance advanced;
	try {
		advanced = f.Advance(owner, observe);
	} catch (std::exception &error) {
		Require(observed, error.what());
		if (mode == "epoch-conflict") {
			Require(StringUtil::Lower(error.what()).find("conflict") != string::npos,
			        "Metadata conflict fixture failed for an unrelated reason");
		}
		failed = true;
	}
	Require(failed == observed, "Epoch fault did not produce the expected failure");
	auto committed = !observed || mode == "epoch-fail-after_commit";
	auto state = f.Status();
	Require(state.request_epoch == (committed ? 2 : 1) && state.retained_requests == (committed ? 0 : 2),
	        "Epoch fence and receipt pruning did not commit together");
	Require(SQL(reader, "SELECT count(*) FROM __ducklake_metadata_lake.ducklake_vortex_write_requests_v2")
	                ->GetValue(0, 0)
	                .GetValue<idx_t>() == 2,
	        "Cleanup changed an ordinary reader's old metadata snapshot");
	SQL(reader, "COMMIT");
	f.CurrentRows();
	Connection retry(database);
	advanced = f.Advance(retry);
	Require(advanced.previous_epoch == 1 && advanced.request_epoch == 2 && advanced.expired_requests == 2,
	        "Advance recovery changed the retired receipt count");
	auto fresh = DuckLakeVortexWriteBatch::Mutate(retry, "lake", "main", "items", "knn", "new-window", f.late,
	                                              Value::LIST(LogicalType::BIGINT, {}), 2);
	EpochFixture::Same(advanced, f.Advance(retry));
	Require(fresh.request_epoch == 2 && f.Status().retained_requests == 1, "Advance retry deleted a newer receipt");
	bool expired = false;
	try {
		f.Late(retry);
	} catch (std::exception &error) {
		expired = string(error.what()).find("EXPIRED_REQUEST") != string::npos;
	}
	Require(expired, "Old epoch request was reapplied after receipt cleanup");
	f.CurrentRows(2);
}

} // namespace vortex_test
} // namespace duckdb
