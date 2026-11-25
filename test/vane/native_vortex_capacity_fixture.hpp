#pragma once

#include "native_vortex_epoch_fixture.hpp"
#include "native_vortex_lifecycle_fixture.hpp"
#include "duckdb/common/file_system.hpp"

#include <map>
#include <tuple>

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

inline DuckLakeVortexWriteResult CapacityWrite(Connection &owner, ClientContext &caller, const string &id,
                                               const Value &rows) {
	return DuckLakeVortexWriteBatch::Mutate(owner, caller, "lake", "main", "items", "knn", id, rows,
	                                        Value::LIST(LogicalType::BIGINT, {}), 1);
}

inline string CapacitySourceCatalog(Connection &connection) {
	vortex_detail::ViewJSON json;
	auto metadata = json.Object();
	const string catalog = "__ducklake_metadata_lake.";
	for (auto name : {"ducklake_data_file", "ducklake_delete_file", "ducklake_inlined_data_tables"}) {
		json.Add(metadata, name, LifecycleRows(json, connection, "SELECT * FROM " + catalog + name + " ORDER BY ALL"));
	}
	json.Add(metadata, "ducklake_files_scheduled_for_deletion",
	         LifecycleRows(json, connection,
	                       "SELECT data_file_id,path,path_is_relative,schedule_start::VARCHAR FROM " + catalog +
	                           "ducklake_files_scheduled_for_deletion ORDER BY ALL"));
	return json.Finish(metadata);
}

using CapacityFiles = std::map<string, string>;

inline void ReadCapacityFiles(FileSystem &fs, const string &directory, CapacityFiles &files) {
	auto listed = fs.ListFiles(directory, [&](const string &name, bool is_directory) {
		auto path = fs.JoinPath(directory, name);
		if (is_directory) {
			ReadCapacityFiles(fs, path, files);
		} else {
			files.emplace(path, LifecycleRead(path));
		}
	});
	Require(listed, "Cannot list capacity evidence files: " + directory);
}

inline std::tuple<string, string, CapacityFiles> CapacityState(LifecycleFixture &fixture) {
	auto &fs = FileSystem::GetFileSystem(*fixture.setup.context);
	CapacityFiles files;
	for (auto name : {"data", "indexes", "private"}) {
		auto directory = fs.JoinPath(fixture.root, name);
		if (fs.DirectoryExists(directory)) {
			ReadCapacityFiles(fs, directory, files);
		}
	}
	return std::make_tuple(fixture.Snapshot(), CapacitySourceCatalog(fixture.setup), std::move(files));
}

inline string ExpectedOtherCommit(const string &baseline) {
	using namespace duckdb_yyjson;
	std::unique_ptr<yyjson_doc, decltype(&yyjson_doc_free)> original(
	    yyjson_read(baseline.data(), baseline.size(), 0), yyjson_doc_free);
	Require(bool(original), "Cannot read capacity conflict baseline");
	vortex_detail::ViewJSON json;
	auto root = yyjson_val_mut_copy(json.doc.get(), yyjson_doc_get_root(original.get()));
	auto snapshots = yyjson_mut_obj_get(root, "snapshots");
	auto last = yyjson_mut_arr_get_last(snapshots);
	Require(yyjson_mut_arr_size(last) == 2, "Capacity conflict baseline requires a prior snapshot");
	auto next = json.Array();
	yyjson_mut_arr_add_sint(json.doc.get(), next, yyjson_mut_get_sint(yyjson_mut_arr_get(last, 0)) + 1);
	yyjson_mut_arr_add_sint(json.doc.get(), next, yyjson_mut_get_sint(yyjson_mut_arr_get(last, 1)));
	yyjson_mut_arr_append(snapshots, next);
	return json.Finish(root);
}

inline void CapacityBoundary(LifecycleFixture &fixture, const string &stage, bool pause = false) {
	Write(fixture.root + "/sources.json", CapacitySourceCatalog(fixture.setup));
	if (pause) {
		fixture.Wait(stage);
	} else {
		fixture.Save(stage);
	}
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
		vector<std::future<std::pair<bool, string>>> tasks;
		auto &lock = DuckLakeVortexWriteBatch::WriterLock();
		const auto previous_waiters = lock.WaitingWriterCount();
		auto admission = lock.GetExclusiveLock([] {});
		for (idx_t i = 0; i < 2; i++) {
			auto input = CapacityExtra(setup, 20000 + i);
			tasks.push_back(std::async(std::launch::async, [&, input, i] {
				Connection owner(database);
				Connection caller(database);
				try {
					return std::make_pair(true,
					                      CapacityWrite(owner, *caller.context, "slot-" + to_string(i), input).request_id);
				} catch (std::exception &error) {
					return std::make_pair(false, string(error.what()));
				}
			}));
		}
		const bool both_queued = EpochWriterQueued(tasks.front(), previous_waiters + 1);
		admission.reset();
		Require(both_queued, "Both final-slot writers must queue before admission is released");
		idx_t successful = 0;
		for (auto &task : tasks) {
			auto result = task.get();
			successful += result.first;
			Require(result.first ||
			            result.second.find("RESOURCE_EXHAUSTED: Vortex write inline rows") != string::npos,
			        "Capacity race failed for an unrelated reason");
		}
		Require(successful == 1 && f.Count("SELECT count(*) FROM lake.items WHERE id>=20000") == 1 &&
		            f.Count("SELECT count(*) FROM __ducklake_metadata_lake.ducklake_vortex_write_requests_v2") == 2,
		        "Concurrent writers admitted more than one final slot or lost its receipt");
		CapacityBoundary(f, "capacity-complete");
		return;
	}
	CapacityBudget(f, 2, 151);
	auto before = CapacityState(f);
	Connection owner(database);
	if (mode == "capacity-conflict") {
		SQL(owner, "SET SESSION ducklake_max_retry_count=3");
	}
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
				// Only the unrelated commit's new snapshot may differ from the original baseline.
				std::get<0>(before) = ExpectedOtherCommit(std::get<0>(before));
			}
			if (mode == "capacity-pause-" + stage) {
				if (stage == "before_commit") {
					Require(CapacityState(f) == before,
					        "Staged capacity write changed committed state before the crash boundary");
				}
				CapacityBoundary(f, "capacity-" + stage, true);
			}
		});
	} catch (std::exception &error) {
		Require(injected, error.what());
		auto message = StringUtil::Lower(error.what());
		Require(mode == "capacity-cancel"
		            ? message.find("interrupt") != string::npos
		            : message.find("duplicate key") != string::npos && message.find("snapshot_id") != string::npos &&
		                  message.find("primary key constraint") != string::npos,
		        "Capacity transaction failed for an unrelated reason: " + string(error.what()));
		failed = true;
	}
	if (mode == "capacity-cancel" || mode == "capacity-conflict") {
		Require(injected && failed, "Capacity transaction fault was not exercised");
	}
	if (mode == "capacity-conflict") {
		Require(SQL(owner, "SELECT current_setting('ducklake_max_retry_count')")->GetValue(0, 0).GetValue<idx_t>() == 0,
		        "Capacity write did not disable native commit retries");
		auto other = SQL(setup, "SELECT id FROM lake.other");
		Require(other->RowCount() == 1 && other->GetValue(0, 0).GetValue<int64_t>() == 7,
		        "Capacity conflict lost or duplicated the independently committed row");
	}
	if (failed) {
		Require(CapacityState(f) == before,
		        "Cancelled/conflicting admission changed business, receipts, sources or file identities");
		Connection retry(database);
		f.Mutate(retry);
	}
	f.CheckNames(true);
	auto full = CapacityState(f);
	bool rejected = false;
	try {
		Connection overflow(database);
		CapacityWrite(overflow, *setup.context, "blocked", CapacityExtra(setup, 30000));
	} catch (std::exception &error) {
		rejected = string(error.what()).find("RESOURCE_EXHAUSTED: Vortex write inline rows") != string::npos;
	}
	Require(rejected && CapacityState(f) == full, "Over-capacity write returned an ACK or changed committed state");
	if (mode == "capacity-pause-rejected") {
		CapacityBoundary(f, "capacity-rejected", true);
	}
	bool lowered = false;
	try {
		CapacityBudget(f, 2, 150);
	} catch (std::exception &error) {
		lowered = string(error.what()).find("RESOURCE_EXHAUSTED: Vortex write inline bytes") != string::npos;
	}
	Require(lowered && CapacityState(f) == full, "Failed budget reconfiguration changed the active contract");
	CapacityBoundary(f, "capacity-complete");
}

} // namespace vortex_test
} // namespace duckdb
