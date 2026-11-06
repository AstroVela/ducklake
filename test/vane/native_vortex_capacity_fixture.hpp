#pragma once

#include "native_vortex_lifecycle_fixture.hpp"

namespace duckdb {
namespace vortex_test {

inline void CapacityBudget(LifecycleFixture &f, idx_t rows, idx_t bytes) {
	SQL(f.setup, "SELECT * FROM ducklake_set_vortex_write_budget('lake','main','items','knn'," + to_string(rows) + "," +
	                 to_string(bytes) + ",4096,67108864)");
}

inline Value CapacityExtra(Connection &connection, int64_t id) {
	return SQL(connection, "SELECT [{'id':" + to_string(id) +
	                           "::BIGINT,'embedding':[0.5,0]::FLOAT[],"
	                           "'category':NULL::VARCHAR,'payload':NULL::VARCHAR}]")
	    ->GetValue(0, 0);
}

inline DuckLakeVortexWriteResult CapacityWrite(Connection &owner, const string &id, const Value &rows,
                                               const std::function<void(const string &)> &observe = {}) {
	return DuckLakeVortexWriteBatch::Mutate(owner, "lake", "main", "items", "knn", id, rows,
	                                        Value::LIST(LogicalType::BIGINT, {}), 1, observe);
}

inline void RunCapacityFixture(DuckDB &database, Connection &setup, const string &root, const string &mode) {
	if (mode == "capacity-parse") {
		auto result = DuckLakeVortexIndex::ParseSegments(LifecycleRead(root + "/input.json"),
		                                                 "00000000-0000-0000-0000-000000000001", 1);
		idx_t rows = 0;
		for (auto &file : result.files) {
			rows += file.row_count;
		}
		vortex_detail::ViewJSON json;
		auto value = json.Object();
		json.Number(value, "rows", rows);
		json.Number(value, "dimension", result.dimension);
		json.Number(value, "segments", result.segments.size());
		json.Number(value, "manifest_bytes", result.reference.size());
		Write(root + "/parsed.json", json.Finish(value));
		return;
	}
	LifecycleFixture f(database, setup, root);
	if (mode == "capacity-race") {
		CapacityBudget(f, 1, 72);
		std::promise<void> start;
		auto ready = start.get_future().share();
		vector<std::future<std::pair<bool, string>>> tasks;
		for (idx_t i = 0; i < 2; i++) {
			auto input = CapacityExtra(setup, 20000 + i);
			tasks.push_back(std::async(std::launch::async, [&, input, i] {
				Connection owner(database);
				ready.wait();
				try {
					return std::make_pair(true, CapacityWrite(owner, "slot-" + to_string(i), input).request_id);
				} catch (std::exception &error) {
					return std::make_pair(false, string(error.what()));
				}
			}));
		}
		start.set_value();
		idx_t successful = 0;
		for (auto &task : tasks) {
			auto result = task.get();
			successful += result.first;
			Require(result.first || result.second.find("RESOURCE_EXHAUSTED") != string::npos,
			        "Capacity race failed for an unrelated reason");
		}
		Require(successful == 1 && f.Count("SELECT count(*) FROM lake.items WHERE id>=20000") == 1 &&
		            f.Count("SELECT count(*) FROM __ducklake_metadata_lake.ducklake_vortex_write_requests_v2") == 2,
		        "Concurrent writers admitted more than one final slot or lost its receipt");
		f.Save("capacity-complete");
		return;
	}
	CapacityBudget(f, 2, 151);
	auto before = f.Snapshot();
	Connection owner(database);
	bool injected = false, failed = false;
	try {
		f.Mutate(owner, [&](const string &stage) {
			if (mode == "capacity-cancel" && stage == "before_commit") {
				injected = true;
				owner.context->Interrupt();
			}
			if (mode == "capacity-conflict" && stage == "before_commit") {
				injected = true;
				Connection other(database);
				SQL(other, "INSERT INTO lake.other VALUES(7)");
				before = f.Snapshot();
			}
			if (mode == "capacity-pause-" + stage) {
				f.Wait("capacity-" + stage);
			}
		});
	} catch (std::exception &error) {
		Require(injected, error.what());
		auto message = StringUtil::Lower(error.what());
		Require(mode == "capacity-cancel"
		            ? message.find("interrupt") != string::npos
		            : message.find("duplicate key") != string::npos && message.find("snapshot_id") != string::npos &&
		                  message.find("maximum retry count of 0") != string::npos,
		        "Capacity transaction failed for an unrelated reason: " + string(error.what()));
		failed = true;
	}
	Require(injected == failed, "Capacity transaction fault was not exercised");
	if (failed) {
		Require(f.Snapshot() == before,
		        "Cancelled/conflicting admission partially committed business or receipt state");
		Connection retry(database);
		f.Mutate(retry);
	}
	f.CheckNames(true);
	auto full = f.Snapshot();
	bool rejected = false;
	try {
		Connection overflow(database);
		CapacityWrite(overflow, "blocked", CapacityExtra(setup, 30000));
	} catch (std::exception &error) {
		rejected = string(error.what()).find("RESOURCE_EXHAUSTED: Vortex write inline rows") != string::npos;
	}
	Require(rejected && f.Snapshot() == full, "Over-capacity write returned an ACK or changed committed state");
	if (mode == "capacity-pause-rejected") {
		f.Wait("capacity-rejected");
	}
	bool lowered = false;
	try {
		CapacityBudget(f, 2, 150);
	} catch (std::exception &error) {
		lowered = string(error.what()).find("RESOURCE_EXHAUSTED: Vortex write inline bytes") != string::npos;
	}
	Require(lowered && f.Snapshot() == full, "Failed budget reconfiguration changed the active contract");
	f.Save("capacity-complete");
}

} // namespace vortex_test
} // namespace duckdb
