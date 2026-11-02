#include "native_vortex_fixture.hpp"
#include "common/ducklake_vortex_json.hpp"
#include "ducklake_extension.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/storage/storage_lock.hpp"
#include "storage/ducklake_vortex_index_catalog.hpp"
#include "storage/ducklake_vortex_optimizer.hpp"
#include "storage/ducklake_vortex_search.hpp"
#include "storage/ducklake_vortex_source_retention.hpp"
#include "storage/ducklake_vortex_write.hpp"
#include "vortex_extension.hpp"

#include <atomic>
#include <iostream>
#include <map>
#include <memory>
#include <thread>
#include <unistd.h>

using namespace duckdb;
using namespace duckdb::vortex_test;
using Source = DuckLakeVortexSourceRetention;
using VortexIndex = DuckLakeVortexIndex;

namespace {
string Read(const string &path) {
	std::ifstream input(path, std::ios::binary);
	Require(bool(input), "Cannot read source fixture file");
	return string(std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>());
}

struct Fixture {
	string root, old_source;
	DuckDB database;
	Connection writer;
	DuckLakeVortexIndex index;
	Fixture(const string &root_p) : root(root_p), database(nullptr), writer(database) {
		database.LoadStaticExtension<DucklakeExtension>();
		database.LoadStaticExtension<VortexExtension>();
		SQL(writer, "SET threads=2; LOAD parquet; ATTACH " +
		                Value("ducklake:" + root + "/metadata.ducklake").ToSQLString() + " AS lake(DATA_PATH " +
		                Value(root + "/data").ToSQLString() + ",DATA_INLINING_ROW_LIMIT 1000)");
		FileSystem::GetFileSystem(*writer.context).CreateDirectory(root + "/indexes");
		SQL(writer, "CREATE TABLE lake.items(id BIGINT,embedding FLOAT[],category VARCHAR,payload VARCHAR);"
		            "CALL ducklake_set_option('lake','experimental_data_file_format','vortex',table_name=>'items');"
		            "SELECT * FROM ducklake_initialize_vortex_indexes('lake');"
		            "SELECT * FROM ducklake_initialize_vortex_optimizer('lake');"
		            "SELECT * FROM ducklake_initialize_vortex_writes('lake');"
		            "SELECT * FROM ducklake_create_vortex_index('lake','main','items','knn'," +
		                Value(root + "/indexes").ToSQLString() + ",2)");
	}
	Value Rows(idx_t first) {
		return SQL(writer, "SELECT list({'id':i::BIGINT,'embedding':[i::FLOAT,0::FLOAT],'category':'A'::VARCHAR,"
		                   "'payload':'原始向量'::VARCHAR}) FROM range(" +
		                       to_string(first) + "," + to_string(first + 128) + ")t(i)")
		    ->GetValue(0, 0);
	}
	DuckLakeVortexWriteResult WriteBatch(idx_t first, const string &request) {
		Connection owner(database);
		return DuckLakeVortexWriteBatch::Append(owner, "lake", "main", "items", "knn", request, Rows(first));
	}
	void Build(idx_t revision, const string &name) {
		SQL(writer, "CALL ducklake_flush_inlined_data('lake',table_name=>'items')");
		index = VortexIndex::BuildAndPublish(*writer.context, "lake", "main", "items", "knn", root + "/indexes/" + name,
		                                     2, revision, nullptr, "", {}, DuckLakeVortexIndexOperation::APPEND);
	}
	void Seed() {
		WriteBatch(0, "seed");
		Build(1, "seed.json");
		old_source = index.files[0].uri;
		SQL(writer, "SELECT * FROM ducklake_enable_vortex_optimizer('lake','main','items','knn'," +
		                Value(root + "/indexes").ToSQLString() +
		                ",2,100000,4194304,100000,67108864);"
		                "SELECT * FROM ducklake_cancel_vortex_optimizer('lake','main','items','knn');"
		                "SELECT * FROM "
		                "ducklake_set_vortex_write_budget('lake','main','items','knn',1000,4194304,4096,67108864)");
		auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
		while (DuckLakeVortexOptimizer::Status(*writer.context, "lake")[0][16].GetValue<bool>()) {
			Require(std::chrono::steady_clock::now() < deadline, "Seed optimizer did not stop");
			std::this_thread::sleep_for(std::chrono::milliseconds(5));
		}
	}
	void Rewrite() {
		SQL(writer, "DELETE FROM lake.items WHERE id<32");
		index = VortexIndex::Rewrite(*writer.context, "lake", "main", "items", "knn", index.segments[0].generation,
		                             root + "/indexes/rewrite.json", index.revision);
	}
	idx_t Count(const string &table) {
		return SQL(writer, "SELECT count(*) FROM " + table)->GetValue(0, 0).GetValue<idx_t>();
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
		request.k = 1;
		return DuckLakeVortexSearch::Execute(*reader.context, options, request, observe);
	}
	DuckLakeVortexSourceGCResult Collect(idx_t objects = 16, idx_t bytes = 64 * 1024 * 1024,
	                                     const std::function<void(const string &)> &observe = {}) {
		Connection owner(database);
		return Source::Collect(*owner.context, "lake", objects, bytes, observe);
	}
	void Policy(idx_t keep = 1, idx_t age = 0) {
		DuckLakeVortexOptimizer::ConfigureSourceRetention(*writer.context, "lake", true, keep, age);
	}
	void Drain(idx_t limit = 16) {
		for (idx_t i = 0; i < 64; i++) {
			auto collected = Collect(limit);
			Require(collected.expired + collected.files <= limit && collected.bytes <= 64 * 1024 * 1024,
			        "Source collection exceeded its per-pass budget");
			if (!collected.expired && !collected.files) {
				Require(!collected.blocked, "Unexpected active transaction blocked fixture cleanup");
				return;
			}
		}
		throw InvalidInputException("Source collection did not converge");
	}
	void CheckReclaimed() {
		Require(!FileSystem::GetFileSystem(*writer.context).FileExists(old_source), "Expired source was not reclaimed");
		Require(Count("__ducklake_metadata_lake.ducklake_files_scheduled_for_deletion") == 0 &&
		            Count("__ducklake_metadata_lake.ducklake_vortex_source_deletions_v1") == 0,
		        "Source cleanup left registered deletion work");
		Connection reader(database);
		Require(Search(reader).rows[0][0].GetValue<int64_t>() == 32, "Current ANN query changed after source cleanup");
	}
};

idx_t DiskBytes(FileSystem &fs, const string &path) {
	idx_t bytes = 0;
	fs.ListFiles(path, [&](const string &name, bool directory) {
		auto child = fs.JoinPath(path, name);
		if (directory) {
			bytes += DiskBytes(fs, child);
		} else {
			auto handle = fs.OpenFile(child, FileFlags::FILE_FLAGS_READ);
			bytes += handle->GetFileSize();
		}
	});
	return bytes;
}

void Concurrent(Fixture &f) {
	auto seed = f.WriteBatch(0, "seed");
	f.Policy(2);
	DuckLakeVortexOptimizer::EnableMaintenance(*f.writer.context, "lake", 2, 64 * 1024 * 1024);
	Write(f.root + "/data/unregistered.keep", "not-owned");
	std::map<idx_t, int64_t> oracle;
	auto remember = [&] {
		auto snapshot = SQL(f.writer, "SELECT max(snapshot_id) FROM __ducklake_metadata_lake.ducklake_snapshot")
		                    ->GetValue(0, 0)
		                    .GetValue<idx_t>();
		oracle[snapshot] = SQL(f.writer, "SELECT min(id) FROM lake.items")->GetValue(0, 0).GetValue<int64_t>();
	};
	remember();
	std::atomic<bool> running {true};
	vector<std::pair<idx_t, int64_t>> observed;
	Connection reader(f.database);
	auto reads = std::async(std::launch::async, [&] {
		while (running && observed.size() < 1000) {
			auto result = f.Search(reader);
			using namespace duckdb_yyjson;
			std::unique_ptr<yyjson_doc, decltype(&yyjson_doc_free)> json(
			    yyjson_read(result.diagnostics.data(), result.diagnostics.size(), 0), yyjson_doc_free);
			auto capture = yyjson_obj_get(yyjson_doc_get_root(json.get()), "native_capture");
			observed.emplace_back(yyjson_get_uint(yyjson_obj_get(capture, "snapshot_id")),
			                      result.rows[0][0].GetValue<int64_t>());
			std::this_thread::sleep_for(std::chrono::milliseconds(2));
		}
	});
	try {
		for (idx_t round = 1; round <= 8; round++) {
			f.WriteBatch(round * 1000, "epoch-" + to_string(round));
			remember();
			SQL(f.writer, "DELETE FROM lake.items WHERE id<" + to_string(round * 1000));
			remember();
			f.index = VortexIndex::RetireEmpty(*f.writer.context, "lake", "main", "items", "knn",
			                                   f.index.segments[0].generation, f.index.revision);
			remember();
			f.Build(f.index.revision, "epoch-" + to_string(round) + ".json");
			remember();
			auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
			while (true) {
				auto status = Source::Status(*f.writer.context, "lake")[0];
				auto maintenance = DuckLakeVortexOptimizer::MaintenanceStatus(*f.writer.context, "lake")[0];
				Require(StringValue::Get(maintenance[0]) != "failed", maintenance.back().ToString());
				if (status[6].GetValue<idx_t>() == 0 && status[7].GetValue<idx_t>() == 0 &&
				    status[8].GetValue<idx_t>() == 0 && !maintenance[2].GetValue<bool>()) {
					break;
				}
				Require(std::chrono::steady_clock::now() < deadline, "Background source collection did not converge");
				std::this_thread::sleep_for(std::chrono::milliseconds(10));
			}
			auto recorded =
			    SQL(f.writer,
			        "SELECT coalesce((SELECT sum(file_size_bytes) FROM __ducklake_metadata_lake.ducklake_data_file),0)"
			        "+coalesce((SELECT sum(file_size_bytes) FROM __ducklake_metadata_lake.ducklake_delete_file),0)")
			        ->GetValue(0, 0)
			        .GetValue<idx_t>();
			Require(DiskBytes(FileSystem::GetFileSystem(*f.writer.context), f.root + "/data") == recorded + 9,
			        "Historical source bytes did not converge to retained native inventory");
			Require(f.Count("lake.items") == 128, "Rolling batch lost committed current data");
		}
	} catch (...) {
		running = false;
		reads.wait();
		DuckLakeVortexOptimizer::CancelMaintenance(*f.writer.context, "lake");
		throw;
	}
	running = false;
	reads.get();
	Require(!observed.empty(), "No concurrent native query completed");
	for (auto &query : observed) {
		Require(oracle.count(query.first) && oracle[query.first] == query.second,
		        "Concurrent ANN winner disagreed with its exact committed snapshot");
	}
	Require(f.WriteBatch(0, "seed").snapshot_id == seed.snapshot_id && f.Count("lake.items") == 128,
	        "Source expiry lost the request receipt or reapplied an old batch");
	Require(SQL(f.writer, "SELECT count(*) FROM __ducklake_metadata_lake.ducklake_snapshot WHERE snapshot_id=" +
	                          to_string(seed.snapshot_id))
	                ->GetValue(0, 0)
	                .GetValue<idx_t>() == 0,
	        "Receipt check did not cover an expired original snapshot");
	DuckLakeVortexOptimizer::CancelMaintenance(*f.writer.context, "lake");
	std::cout << "eight rolling epochs; " << observed.size()
	          << " snapshot-matched concurrent queries; bounded disk; retained receipt" << std::endl;
}
} // namespace

int main(int argc, char **argv) {
	try {
		Require(argc == 3, "Usage: ducklake_vortex_source_driver ROOT MODE");
		string root = argv[1], mode = argv[2];
		Fixture f(root);
		f.Seed();
		if (mode == "concurrent") {
			Concurrent(f);
			return 0;
		}
		if (mode == "capture-readiness") {
			f.Policy();
			Pause pause;
			Connection reader(f.database);
			auto pending = std::async(std::launch::async, [&] {
				return f.Search(reader, [&](const string &stage) {
					if (stage == "after_snapshot") {
						pause.Stop();
					}
				});
			});
			ReleasePause release {pause};
			pause.Wait();
			Connection held(f.database), owner(f.database);
			held.BeginTransaction();
			SQL(held, "SELECT count(*) FROM lake.items");
			auto &transaction = VortexIndex::Begin(owner, "lake");
			bool ordinary_blocked = !Source::WorkReady(transaction, 64 * 1024 * 1024);
			held.Commit();
			bool ready = Source::WorkReady(transaction, 64 * 1024 * 1024);
			owner.Commit();
			pause.Release();
			Require(pending.get().rows[0][0].GetValue<int64_t>() == 0, "Capture readiness changed its query winner");
			Require(ordinary_blocked, "Source readiness ignored an ordinary transaction");
			Require(ready, "Source readiness discarded finite native capture work");
			return 0;
		}
		if (mode == "capture-gate") {
			f.Policy();
			auto admission = DuckLakeVortexReadView::AdmissionLock().GetSharedLock();
			std::promise<void> waiting;
			auto collecting = std::async(std::launch::async, [&] {
				return f.Collect(16, 64 * 1024 * 1024, [&](const string &stage) {
					if (stage == "source_gc_waiting") {
						waiting.set_value();
					}
				});
			});
			waiting.get_future().wait();
			bool deferred = collecting.wait_for(std::chrono::milliseconds(100)) == std::future_status::timeout;
			auto entering = std::async(std::launch::async, [&] {
				Connection reader(f.database);
				return SQL(reader, "SELECT min(id) FROM lake.items")->GetValue(0, 0).GetValue<int64_t>();
			});
			bool progressed = entering.wait_for(std::chrono::seconds(1)) == std::future_status::ready;
			admission.reset();
			auto result = collecting.get();
			Require(entering.get() == 0, "Transaction entry lost current data");
			Require(deferred && progressed, "Source collector blocked transaction entry while awaiting capture");
			Require(!result.blocked && result.expired, "Source collector did not progress after admission closed");
			return 0;
		}
		if (mode == "held" || mode == "admission") {
			Pause pause;
			Connection reader(f.database);
			auto pending = std::async(std::launch::async, [&] {
				return f.Search(reader, [&](const string &stage) {
					if (stage == (mode == "held" ? "before_fetch" : "after_snapshot")) {
						pause.Stop();
					}
				});
			});
			ReleasePause release {pause};
			pause.Wait();
			f.Policy();
			if (mode == "admission") {
				std::promise<void> waiting;
				auto collecting = std::async(std::launch::async, [&] {
					return f.Collect(16, 64 * 1024 * 1024, [&](const string &stage) {
						if (stage == "source_gc_waiting") {
							waiting.set_value();
						}
					});
				});
				waiting.get_future().wait();
				bool deferred = collecting.wait_for(std::chrono::milliseconds(100)) == std::future_status::timeout;
				auto gate = Source::TransactionGate().TryGetExclusiveLock();
				bool gate_available = bool(gate);
				gate.reset();
				pause.Release();
				Require(pending.get().rows[0][0].GetValue<int64_t>() == 0, "Admitted query lost its native view");
				auto result = collecting.get();
				Require(deferred, "Source collector abandoned an in-flight native capture");
				Require(gate_available, "Source collector froze transactions before native capture completed");
				Require(!result.blocked && result.expired, "Source collector did not progress after native capture");
				return 0;
			} else {
				f.Rewrite();
				VortexIndex::Collect(*f.writer.context, "lake", 16, 64 * 1024 * 1024);
				f.Drain();
				f.CheckReclaimed();
			}
			pause.Release();
			Require(pending.get().rows[0][0].GetValue<int64_t>() == 0, "Held view lost delayed typed fetch after GC");
			return 0;
		}
		Connection old(f.database);
		if (mode == "defer") {
			old.BeginTransaction();
			Require(SQL(old, "SELECT count(*) FROM lake.items")->GetValue(0, 0).GetValue<idx_t>() == 128,
			        "Cannot establish native old transaction");
		}
		f.Rewrite();
		f.Policy();
		if (mode == "source-only") {
			VortexIndex::CollectBuilds(*f.writer.context, "lake", 16, 64 * 1024 * 1024);
			VortexIndex::Collect(*f.writer.context, "lake", 16, 64 * 1024 * 1024);
			Require(f.Count("__ducklake_metadata_lake.ducklake_vortex_builds_v1") == 0,
			        "Source-only fixture retained build work");
			auto generations = VortexIndex::CollectionLock().GetExclusiveLock();
			auto empty_phases = std::make_shared<std::atomic<idx_t>>(0);
			auto completed = std::make_shared<std::atomic<bool>>(false);
			auto database = &f.database;
			DuckLakeVortexOptimizer::EnableMaintenance(
			    *f.writer.context, "lake", 2, 64 * 1024 * 1024,
			    [empty_phases, completed, database](const string &stage) {
				    if (stage == "build_recovery_waiting" || stage == "build_gc_waiting" || stage == "gc_waiting") {
					    ++*empty_phases;
				    }
				    if (stage == "maintenance_complete") {
					    Connection probe(*database);
					    auto source = Source::Status(*probe.context, "lake")[0];
					    if (!source[6].GetValue<idx_t>() && !source[7].GetValue<idx_t>() &&
					        !source[8].GetValue<idx_t>()) {
						    *completed = true;
					    }
				    }
			    });
			auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
			while (!*completed && std::chrono::steady_clock::now() < deadline) {
				std::this_thread::sleep_for(std::chrono::milliseconds(5));
			}
			generations.reset();
			DuckLakeVortexOptimizer::CancelMaintenance(*f.writer.context, "lake");
			deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
			while (DuckLakeVortexOptimizer::MaintenanceStatus(*f.writer.context, "lake")[0][2].GetValue<bool>()) {
				Require(std::chrono::steady_clock::now() < deadline, "Source-only worker did not stop");
				std::this_thread::sleep_for(std::chrono::milliseconds(5));
			}
			Require(*completed && *empty_phases == 0, "Source-only maintenance waited on empty index collection");
			f.CheckReclaimed();
			return 0;
		}
		if (mode == "defer") {
			Require(f.Collect().blocked &&
			            SQL(old, "SELECT count(*) FROM lake.items")->GetValue(0, 0).GetValue<idx_t>() == 128,
			        "An active native transaction lost its source snapshot");
			old.Commit();
			f.Drain();
			f.CheckReclaimed();
			return 0;
		}
		if (mode == "age") {
			f.Policy(1, 315360000);
			Require(f.Collect().expired == 0, "Age policy expired retained snapshots");
			f.Policy();
			f.Drain(1);
			f.CheckReclaimed();
			return 0;
		}
		if (mode == "live") {
			auto file = f.index.files[0];
			SQL(f.writer, "INSERT INTO __ducklake_metadata_lake.ducklake_files_scheduled_for_deletion VALUES(" +
			                  to_string(file.id) + "," + Value(file.uri).ToSQLString() + ",false,now())");
			bool failed = false;
			try {
				f.Collect();
			} catch (std::exception &) {
				failed = true;
			}
			Require(failed && FileSystem::GetFileSystem(*f.writer.context).FileExists(file.uri),
			        "Live source was deleted");
			SQL(f.writer,
			    "DELETE FROM __ducklake_metadata_lake.ducklake_files_scheduled_for_deletion WHERE data_file_id=" +
			        to_string(file.id));
			f.Drain();
			f.CheckReclaimed();
			return 0;
		}
		if (mode == "budget") {
			f.Collect();
			auto limited = f.Collect(1, 1);
			Require(limited.files == 0 && limited.bytes == 0, "Source file exceeded the configured byte budget");
			f.Drain(1);
			f.CheckReclaimed();
			return 0;
		}
		if (mode == "manual") {
			f.Collect();
			bool stopped = false;
			try {
				f.Collect(16, 64 * 1024 * 1024, [&](const string &stage) {
					if (stage == "source_delete_intent_committed") {
						stopped = true;
						throw IOException("Pause before independent native cleanup");
					}
				});
			} catch (std::exception &) {
				Require(stopped, "Deletion intent did not reach the expected boundary");
			}
			SQL(f.writer, "CALL ducklake_cleanup_old_files('lake',cleanup_all=>true)");
			Require(f.Count("__ducklake_metadata_lake.ducklake_vortex_source_deletions_v1") == 1 &&
			            f.Count("__ducklake_metadata_lake.ducklake_files_scheduled_for_deletion") == 0,
			        "Native cleanup did not leave the expected independent intent acknowledgement");
			f.Drain();
			f.CheckReclaimed();
			return 0;
		}
		if (mode == "checksum" || mode == "symlink") {
			f.Collect();
			auto original = Read(f.old_source);
			Write(root + "/victim", "keep-me");
			bool changed = false, failed = false;
			try {
				f.Collect(16, 64 * 1024 * 1024, [&](const string &stage) {
					if (stage == "source_delete_intent_committed") {
						changed = true;
						if (mode == "checksum") {
							auto contents = original;
							contents[0] ^= 1;
							std::ofstream output(f.old_source, std::ios::binary);
							output << contents;
						} else {
							Require(std::rename(f.old_source.c_str(), (root + "/backup").c_str()) == 0,
							        "Cannot preserve source inode");
							Require(symlink((root + "/victim").c_str(), f.old_source.c_str()) == 0,
							        "Cannot inject source symlink");
						}
					}
				});
			} catch (std::exception &) {
				failed = true;
			}
			Require(changed && failed && Read(root + "/victim") == "keep-me", "Changed source was not rejected safely");
			if (mode == "checksum") {
				std::ofstream output(f.old_source, std::ios::binary);
				output << original;
			} else {
				Require(unlink(f.old_source.c_str()) == 0 &&
				            std::rename((root + "/backup").c_str(), f.old_source.c_str()) == 0,
				        "Cannot restore original registered source");
			}
			f.Drain();
			f.CheckReclaimed();
			return 0;
		}
		if (mode == "cancel") {
			Pause pause;
			DuckLakeVortexOptimizer::EnableMaintenance(*f.writer.context, "lake", 2, 64 * 1024 * 1024,
			                                           [&](const string &stage) {
				                                           if (stage == "source_before_unlink") {
					                                           pause.Stop();
				                                           }
			                                           });
			ReleasePause release {pause};
			pause.Wait();
			Connection cancellation(f.database);
			auto cancelled = std::async(
			    std::launch::async, [&] { DuckLakeVortexOptimizer::CancelMaintenance(*cancellation.context, "lake"); });
			Require(cancelled.wait_for(std::chrono::milliseconds(100)) == std::future_status::timeout,
			        "Cancellation crossed an active source deletion transaction");
			pause.Release();
			cancelled.get();
			auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
			while (DuckLakeVortexOptimizer::MaintenanceStatus(*f.writer.context, "lake")[0][2].GetValue<bool>()) {
				Require(std::chrono::steady_clock::now() < deadline, "Cancelled source worker did not stop");
				std::this_thread::sleep_for(std::chrono::milliseconds(5));
			}
			Require(FileSystem::GetFileSystem(*f.writer.context).FileExists(f.old_source) &&
			            f.Count("__ducklake_metadata_lake.ducklake_vortex_source_deletions_v1") == 1,
			        "Cancellation did not preserve the registered deletion intent");
			DuckLakeVortexOptimizer::ResumeMaintenance(*f.writer.context, "lake");
			while (FileSystem::GetFileSystem(*f.writer.context).FileExists(f.old_source)) {
				Require(std::chrono::steady_clock::now() < deadline, "Resumed source worker did not finish");
				std::this_thread::sleep_for(std::chrono::milliseconds(5));
			}
			DuckLakeVortexOptimizer::CancelMaintenance(*f.writer.context, "lake");
			while (DuckLakeVortexOptimizer::MaintenanceStatus(*f.writer.context, "lake")[0][2].GetValue<bool>()) {
				Require(std::chrono::steady_clock::now() < deadline, "Resumed source worker did not stop");
				std::this_thread::sleep_for(std::chrono::milliseconds(5));
			}
			f.Drain();
			f.CheckReclaimed();
			return 0;
		}
		if (mode == "gate") {
			f.Collect();
			Pause pause;
			auto collecting = std::async(std::launch::async, [&] {
				return f.Collect(16, 64 * 1024 * 1024, [&](const string &stage) {
					if (stage == "source_before_unlink") {
						pause.Stop();
					}
				});
			});
			ReleasePause release {pause};
			pause.Wait();
			Connection reader(f.database);
			auto reading = std::async(std::launch::async, [&] { return f.Search(reader); });
			Require(reading.wait_for(std::chrono::milliseconds(50)) == std::future_status::timeout,
			        "New query crossed source deletion admission");
			pause.Release();
			collecting.get();
			Require(reading.get().rows[0][0].GetValue<int64_t>() == 32, "New query lost current data after deletion");
			f.Drain();
			f.CheckReclaimed();
			return 0;
		}
		bool fired = false;
		auto observe = [&](const string &stage) {
			if (mode == "pause-" + stage || mode == "background-pause-" + stage) {
				Write(root + "/checkpoint", stage);
				auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
				while (!FileSystem::GetFileSystem(*f.writer.context).FileExists(root + "/release")) {
					Require(std::chrono::steady_clock::now() < deadline, "Source pause timed out");
					std::this_thread::sleep_for(std::chrono::milliseconds(5));
				}
			}
			if (mode == "fail-" + stage) {
				fired = true;
				throw IOException("Injected source retention failure");
			}
		};
		if (StringUtil::StartsWith(mode, "background-pause-")) {
			DuckLakeVortexOptimizer::EnableMaintenance(*f.writer.context, "lake", 2, 64 * 1024 * 1024, observe);
			std::this_thread::sleep_for(std::chrono::seconds(30));
			throw InvalidInputException("Background source checkpoint did not terminate the process");
		}
		for (idx_t i = 0; i < 32 && !fired; i++) {
			try {
				f.Collect(16, 64 * 1024 * 1024, observe);
			} catch (std::exception &) {
				Require(fired, "Unexpected source retention error");
			}
		}
		Require(fired || StringUtil::StartsWith(mode, "pause-"), "Source fault checkpoint was not reached");
		f.Drain();
		f.CheckReclaimed();
		return 0;
	} catch (std::exception &error) {
		std::cerr << error.what() << std::endl;
		return 1;
	}
}
