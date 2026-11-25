#pragma once

#include "native_vortex_lifecycle_fixture.hpp"
#include "storage/ducklake_vortex_search.hpp"

namespace duckdb {
namespace vortex_test {

inline string ScratchRoot(const DuckLakeVortexSearchResult &result) {
	using namespace duckdb_yyjson;
	std::unique_ptr<yyjson_doc, decltype(&yyjson_doc_free)> json(
	    yyjson_read(result.diagnostics.data(), result.diagnostics.size(), 0), yyjson_doc_free);
	Require(bool(json), "Missing native query diagnostics");
	auto root = yyjson_obj_get(yyjson_doc_get_root(json.get()), "scratch_root");
	Require(yyjson_is_str(root), "Native query did not expose its registered scratch namespace");
	return string(yyjson_get_str(root), yyjson_get_len(root));
}

inline void ScratchPause(Connection &setup, const string &root, const string &stage) {
	Write(root + "/checkpoint.tmp", stage);
	FileSystem::GetFileSystem(*setup.context).MoveFile(root + "/checkpoint.tmp", root + "/checkpoint");
	auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(180);
	while (!FileSystem::GetFileSystem(*setup.context).FileExists(root + "/release")) {
		Require(std::chrono::steady_clock::now() < deadline, "Scratch crash boundary was not killed");
		std::this_thread::sleep_for(std::chrono::milliseconds(5));
	}
}

inline void ScratchReport(const string &root, const vector<Value> &row) {
	vortex_detail::ViewJSON json;
	auto result = json.Object();
	json.Text(result, "root", StringValue::Get(row[0]));
	idx_t i = 1;
	for (auto name : {"registered_views", "examined_views", "blocked_views", "collected_objects", "collected_files",
	                  "collected_bytes", "removed_views", "remaining_views"}) {
		json.Number(result, name, row[i++].GetValue<idx_t>());
	}
	Write(root + "/collection.json", json.Finish(result));
}

inline vector<Value> ScratchCollect(Connection &setup, const string &catalog, const string &path, idx_t objects,
                                    idx_t bytes, const std::function<void(const string &)> &observe = nullptr) {
	vector<Value> result;
	// Match the SQL table-function execution context without creating an explicit user transaction.
	setup.context->RunFunctionInTransaction(
	    [&] { result = DuckLakeVortexSearch::CollectScratch(*setup.context, catalog, path, objects, bytes, observe); });
	return result;
}

inline void RunScratchFixture(DuckDB &database, Connection &setup, const string &root, const string &mode) {
	if (StringUtil::StartsWith(mode, "scratch-collect-")) {
		auto path = LifecycleRead(root + "/scratch-root");
		bool injected = false;
		try {
			for (idx_t i = 0; i < 32; i++) {
				auto result = ScratchCollect(setup, "lake", path, 16, 64 * 1024 * 1024, [&](const string &stage) {
					if (mode == "scratch-collect-pause-" + stage) {
						ScratchPause(setup, root, "collect-" + stage);
					}
					if (!injected && stage == "scratch_after_unlink" &&
					    (mode == "scratch-collect-cancel" || mode == "scratch-collect-fail")) {
						injected = true;
						if (mode == "scratch-collect-cancel") {
							setup.context->Interrupt();
						} else {
							throw IOException("Injected scratch collection failure");
						}
					}
				});
				ScratchReport(root, result);
				if (result[8].GetValue<idx_t>() == 0) {
					break;
				}
			}
			Require(mode == "scratch-collect-ok", "Scratch collection fault was not exercised");
		} catch (std::exception &error) {
			Require(injected, error.what());
			Require(string(error.what()).find(mode == "scratch-collect-cancel" ? "Interrupt" : "Injected scratch") !=
			            string::npos,
			        "Scratch collection failed for an unrelated reason");
			Write(root + "/checkpoint", "collect-failed");
		}
		return;
	}
	LifecycleFixture f(database, setup, root);
	Connection first(database);
	auto path = ScratchRoot(f.Search(first));
	Write(root + "/scratch-root", path);
	auto before = f.Snapshot();
	if (mode == "scratch-wait-cancel") {
		Pause pause;
		Connection blocker(database), waiting(database);
		auto blocked = std::async(std::launch::async, [&] {
			return f.Search(blocker, [&](const string &stage) {
				if (stage == "scratch_allocated") {
					pause.Stop();
				}
			});
		});
		ReleasePause release {pause};
		pause.Wait();
		auto canceled = std::async(std::launch::async, [&] {
			try {
				f.Search(waiting, [&](const string &stage) {
					if (stage == "scratch_wait_root_lock") {
						waiting.Interrupt();
					}
				});
			} catch (const InterruptException &) {
				return true;
			}
			return false;
		});
		const bool finished_while_locked = canceled.wait_for(std::chrono::seconds(60)) == std::future_status::ready;
		pause.Release();
		blocked.get();
		Require(finished_while_locked && canceled.get(), "Interrupted query waited for the namespace holder");
		Require(f.Snapshot() == before, "Cancellation while waiting changed canonical state");
		auto released = ScratchCollect(setup, "lake", path, 16, 64 * 1024 * 1024);
		Require(released[1].GetValue<idx_t>() == 0, "Interrupting admission left a scratch owner");
		ScratchReport(root, released);
		f.Save("scratch-complete");
		return;
	}
	if (mode == "scratch-held") {
		auto retained_rows = LifecycleSeedHits(setup);
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
		auto blocked = ScratchCollect(setup, "lake", path, 16, 64 * 1024 * 1024);
		Require(blocked[3].GetValue<idx_t>() == 1 && blocked[4].GetValue<idx_t>() == 0,
		        "Explicit scratch cleanup did not protect the active private view");
		f.Retire("scratch-held");
		f.Checkpoint();
		blocked = ScratchCollect(setup, "lake", path, 16, 64 * 1024 * 1024);
		Require(blocked[3].GetValue<idx_t>() == 1 && blocked[4].GetValue<idx_t>() == 0,
		        "Canonical GC or checkpoint revoked private owner protection");
		pause.Release();
		CheckLifecycleSeedHits(held.get(), *retained_rows);
		f.CheckNames(true);
		ScratchReport(root, ScratchCollect(setup, "lake", path, 16, 64 * 1024 * 1024));
		f.Save("scratch-complete");
		return;
	}
	Connection owner(database);
	bool injected = false;
	try {
		f.Search(owner, [&](const string &stage) {
			if (mode == "scratch-query-pause-" + stage) {
				Write(root + "/expected.json", f.Snapshot());
				ScratchPause(setup, root, "query-" + stage);
			}
			if ((mode == "scratch-query-cancel" && stage == "scratch_registered") ||
			    (mode == "scratch-allocation-cancel" && stage == "scratch_allocated")) {
				injected = true;
				owner.context->Interrupt();
			}
			if (mode == "scratch-query-fail" && stage == "after_open") {
				injected = true;
				throw IOException("Injected scratch query failure");
			}
		});
		Require(mode == "scratch-normal", "Scratch query fault was not exercised");
	} catch (std::exception &error) {
		Require(injected, error.what());
		Require(string(error.what()).find(mode == "scratch-query-fail" ? "Injected scratch" : "Interrupt") !=
		            string::npos,
		        "Scratch query failed for an unrelated reason");
	}
	Require(f.Snapshot() == before, "Scratch query changed business, snapshots or native metadata");
	auto released = ScratchCollect(setup, "lake", path, 16, 64 * 1024 * 1024);
	Require(released[1].GetValue<idx_t>() == 0 && released[8].GetValue<idx_t>() == 0,
	        "Completed/cancelled query left registered scratch payload");
	ScratchReport(root, released);
	f.CheckNames(false);
	f.Save("scratch-complete");
}
} // namespace vortex_test
} // namespace duckdb
