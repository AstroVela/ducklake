//! Persistent native catalog fixture; commands and replies are owner-local test files.
#include "native_vortex_fixture.hpp"
#include "common/ducklake_vortex_json.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection_manager.hpp"
#include "duckdb/storage/storage_lock.hpp"
#include "duckdb/transaction/transaction_context.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_vortex_index_catalog.hpp"
#include "storage/ducklake_vortex_optimizer.hpp"
#include "storage/ducklake_vortex_read_view.hpp"
#include "storage/ducklake_vortex_search.hpp"
#include "storage/ducklake_vortex_query_ffi.hpp"

#include <iostream>
#include <sstream>
#include <thread>

using namespace duckdb;
using namespace duckdb::vortex_test;
using namespace duckdb::vortex_detail;

namespace {
string ReadText(const string &path) {
	std::ifstream stream(path, std::ios::binary);
	Require(bool(stream), "Cannot read native command file");
	return string(std::istreambuf_iterator<char>(stream), std::istreambuf_iterator<char>());
}

class PublishedContext {
public:
	explicit PublishedContext(DuckDB &database)
	    : manager(database.instance->GetConnectionManager()),
	      context(make_shared_ptr<ClientContext>(database.instance)) {
		manager.AddConnection(*context);
	}
	~PublishedContext() {
		manager.RemoveConnection(*context);
	}
	PublishedContext(const PublishedContext &) = delete;
	PublishedContext &operator=(const PublishedContext &) = delete;

private:
	ConnectionManager &manager;
	shared_ptr<ClientContext> context;
};

class Fixture {
public:
	Fixture(string root_p, bool create) : root(std::move(root_p)), database(nullptr), writer(database) {
		auto &fs = FileSystem::GetFileSystem(*writer.context);
		Require(fs.IsPathAbsolute(root), "Fixture root must be absolute");
		if (create) {
			Require(!fs.DirectoryExists(root), "Fixture already exists");
			fs.CreateDirectory(root);
		} else {
			Require(fs.FileExists(root + "/metadata.ducklake"), "Persistent fixture metadata is missing");
		}
		Attach(writer, root);
		if (create) {
			CreateItems(writer);
			fs.CreateDirectory(root + "/responses");
		}
	}
	~Fixture() {
		if (optimizer_pause) {
			optimizer_pause->Release();
		}
	}
	weak_ptr<DatabaseInstance> WeakDatabase() {
		return database.instance;
	}

	string Capture(const string &name, const string &index) {
		auto function = index.empty() ? "ducklake_vortex_read_view('lake','main','items',8,67108864,100000)"
		                              : "ducklake_vortex_index_view('lake','main','items'," +
		                                    Value(index).ToSQLString() + ",67108864,100000)";
		auto result = SQL(writer, "SELECT * FROM " + function);
		DuckLakeVortexReadView view;
		view.snapshot_id = result->GetValue(0, 0).GetValue<idx_t>();
		view.source_bytes = result->GetValue(1, 0).GetValue<idx_t>();
		view.visible_rows = result->GetValue(2, 0).GetValue<idx_t>();
		view.inline_rows = result->GetValue(3, 0).GetValue<idx_t>();
		view.admission_json = StringValue::Get(result->GetValue(4, 0));
		view.rows_json = StringValue::Get(result->GetValue(5, 0));
		Save(name, view);
		ViewJSON json;
		auto value = json.Object();
		json.Number(value, "snapshot_id", view.snapshot_id);
		json.Number(value, "source_bytes", view.source_bytes);
		json.Number(value, "inline_rows", view.inline_rows);
		return json.Finish(value);
	}

	string Owners() {
		ViewJSON json;
		auto value = json.Object();
		json.Number(value, "database_references", database.instance.use_count());
		auto entries = json.Array();
		// Foreign contexts are published before ID assignment; inspect shared ownership only.
		for (auto &context : database.instance->GetConnectionManager().GetConnectionList()) {
			auto item = json.Object();
			json.Number(item, "references", context.use_count());
			// Only this command's writer context is idle and exclusively owned here.
			auto active = context == writer.context
			                  ? yyjson_mut_bool(json.doc.get(), context->transaction.HasActiveTransaction())
			                  : yyjson_mut_null(json.doc.get());
			json.Add(item, "active", active);
			yyjson_mut_arr_append(entries, item);
		}
		json.Add(value, "connections", entries);
		return json.Finish(value);
	}

	string Execute(const vector<string> &args) {
		const auto &op = args.at(1);
		if (op == "owners") {
			return Owners();
		}
		if (op == "owners-published-context") {
			// Hold construction at its published-but-unassigned prefix.
			PublishedContext pending(database);
			return Owners();
		}
		if (op == "build-wrong-field") {
			return DuckLakeVortexIndex::BuildAndPublish(*writer.context, "lake", "main", "items", "knn", args.at(2), 8,
			                                            0, nullptr, "", optional_idx(999999))
			    .ToJSON();
		}
		if (op == "optimizer-init") {
			SQL(writer, "SELECT * FROM ducklake_initialize_vortex_optimizer('lake')");
			return "{}";
		}
		if (op == "compaction-policy") {
			SQL(writer, "SELECT * FROM ducklake_set_vortex_data_compaction_policy('lake','main','items','knn'," +
			                args.at(2) + "," + args.at(3) + "," + args.at(4) + ")");
			return "{}";
		}
		if (op == "maintenance-enable") {
			if (args.size() == 4) {
				SQL(writer,
				    "SELECT * FROM ducklake_enable_vortex_maintenance('lake'," + args.at(2) + "," + args.at(3) + ")");
			} else {
				DuckLakeVortexOptimizer::EnableMaintenance(*writer.context, "lake", std::stoull(args.at(2)),
				                                           std::stoull(args.at(3)),
				                                           OptimizerObserver(args.at(4), args.at(5)));
			}
			return "{}";
		}
		if (op == "maintenance-resume") {
			if (args.size() < 4) {
				auto name = Value(args.size() == 3 ? args[2] : "lake").ToSQLString();
				SQL(writer, "SELECT * FROM ducklake_resume_vortex_maintenance(" + name + ")");
			} else {
				DuckLakeVortexOptimizer::ResumeMaintenance(*writer.context, "lake",
				                                           OptimizerObserver(args.at(2), args.at(3)));
			}
			return "{}";
		}
		if (op == "maintenance-cancel") {
			auto name = Value(args.size() == 3 ? args[2] : "lake").ToSQLString();
			SQL(writer, "SELECT * FROM ducklake_cancel_vortex_maintenance(" + name + ")");
			return "{}";
		}
		if (op == "maintenance-reattach" || op == "compaction-reattach" || op == "retire-reattach") {
			Connection caller(database);
			if (op == "maintenance-reattach") {
				caller.BeginTransaction();
				SQL(caller, "SELECT count(*) FROM lake.items");
				Require(!DuckLakeVortexOptimizer::MaintenanceStatus(*caller.context, "lake").empty(),
				        "Could not inspect maintenance on the caller's existing metadata transaction");
				caller.Commit();
			}

			caller.context->transaction.BeginTransaction();
			Require(caller.context->transaction.IsAutoCommit(), "Fixture did not retain autocommit");
			auto &original = Catalog::GetCatalog(*caller.context, "lake");
			SQL(writer, "DETACH lake");
			Attach(writer, root);
			Require(&Catalog::GetCatalog(*caller.context, "LAKE") == &original,
			        "The caller did not retain its original attachment");
			auto reject_stale = [&](const std::function<void()> &operation) {
				try {
					operation();
				} catch (const TransactionException &error) {
					Require(string(error.what()).find("caller attachment changed") != string::npos,
					        "Vortex control rejected the caller for the wrong reason");
					return;
				}
				throw InvalidInputException("Vortex control accepted the reattached catalog for a stale caller");
			};
			if (op == "retire-reattach") {
				reject_stale([&] {
					DuckLakeVortexIndex::RetireEmpty(*caller.context, "LAKE", "main", "items", "knn", args.at(2), 1);
				});
				caller.context->transaction.Rollback(nullptr);
				return "{}";
			}
			if (op == "compaction-reattach") {
				reject_stale([&] {
					DuckLakeVortexOptimizer::ConfigureCompaction(*caller.context, "LAKE", "main", "items", "knn", true,
					                                             1, 1);
				});
				reject_stale([&] { DuckLakeVortexOptimizer::Resume(*caller.context, "LAKE", "main", "items", "knn"); });
				caller.context->transaction.Rollback(nullptr);
				return "{}";
			}
			reject_stale([&] { DuckLakeVortexOptimizer::MaintenanceStatus(*caller.context, "LAKE"); });
			reject_stale([&] { DuckLakeVortexOptimizer::ResumeMaintenance(*caller.context, "LAKE"); });
			caller.context->transaction.Rollback(nullptr);
			return "{\"retained_status_rejected\":true,\"retained_resume_rejected\":true}";
		}
		if (op == "optimizer-init-active-transaction") {
			Connection caller(database);
			SQL(caller, "CREATE TEMP TABLE optimizer_transaction_guard(value INTEGER);"
			            "BEGIN;INSERT INTO optimizer_transaction_guard VALUES(1);");
			string failure;
			try {
				DuckLakeVortexOptimizer::Initialize(caller, "lake");
			} catch (const std::exception &error) {
				failure = error.what();
			}
			Require(!failure.empty(), "Optimizer initialization accepted an active transaction");
			Require(caller.context->transaction.HasActiveTransaction(),
			        "Optimizer initialization rolled back the caller's existing transaction");
			Require(
			    SQL(caller, "SELECT count(*) FROM optimizer_transaction_guard")->GetValue(0, 0).GetValue<int64_t>() ==
			        1,
			    "Optimizer initialization lost the caller's uncommitted row");
			caller.Rollback();
			Require(
			    SQL(caller, "SELECT count(*) FROM optimizer_transaction_guard")->GetValue(0, 0).GetValue<int64_t>() ==
			        0,
			    "The caller could not roll back its own transaction");
			return "{}";
		}
		if (op == "optimizer-enable") {
			if (args.size() == 7) {
				SQL(writer, "SELECT * FROM ducklake_enable_vortex_optimizer('lake','main','items','knn'," +
				                Value(args.at(2)).ToSQLString() + ",8," + args.at(3) + "," + args.at(4) + "," +
				                args.at(5) + "," + args.at(6) + ")");
			} else {
				DuckLakeVortexOptimizerConfig config;
				config.catalog = "lake";
				config.schema = "main";
				config.table = "items";
				config.index = "knn";
				config.root = args.at(2);
				config.dimension = 8;
				config.flush_rows = std::stoull(args.at(3));
				config.flush_bytes = std::stoull(args.at(4));
				config.index_rows = std::stoull(args.at(5));
				config.index_bytes = std::stoull(args.at(6));
				DuckLakeVortexOptimizer::Enable(*writer.context, config, OptimizerObserver(args.at(7), args.at(8)));
			}
			return "{}";
		}
		if (op == "optimizer-resume") {
			if (args.size() == 2) {
				SQL(writer, "SELECT * FROM ducklake_resume_vortex_optimizer('lake','main','items','knn')");
			} else {
				DuckLakeVortexOptimizer::Resume(*writer.context, "lake", "main", "items", "knn",
				                                OptimizerObserver(args.at(2), args.at(3)));
			}
			return "{}";
		}
		if (op == "optimizer-cancel") {
			SQL(writer, "SELECT * FROM ducklake_cancel_vortex_optimizer('lake','main','items','knn')");
			return "{}";
		}
		if (op == "optimizer-wait" || op == "optimizer-release") {
			Require(bool(optimizer_pause), "Optimizer pause is not installed");
			if (op == "optimizer-wait") {
				optimizer_pause->Wait();
			} else {
				optimizer_pause->Release();
			}
			return "{}";
		}
		if (op == "optimizer-status" || op == "generation-status" || op == "build-status" ||
		    op == "maintenance-status" || op == "rewrite-status" || op == "source-status") {
			auto function = op == "generation-status"    ? "ducklake_vortex_generation_status"
			                : op == "build-status"       ? "ducklake_vortex_build_status"
			                : op == "maintenance-status" ? "ducklake_vortex_maintenance_status"
			                                             : "ducklake_vortex_optimizer_status";
			auto name = Value(op == "maintenance-status" && args.size() == 3 ? args[2] : "lake").ToSQLString();
			auto query = "SELECT * FROM " + string(function) + "(" + name + ")";
			if (op == "rewrite-status") {
				query = "SELECT build_id,table_uuid,source_root,new_path,capture_snapshot::UBIGINT AS capture_snapshot,"
				        "new_file_id::UBIGINT AS new_file_id,keep_file FROM "
				        "__ducklake_metadata_lake.ducklake_vortex_rewrites_v1 ORDER BY build_id";
			} else if (op == "source-status") {
				query = "SELECT data_file_id::UBIGINT AS file_id,path,record_count::UBIGINT AS rows,"
				        "begin_snapshot::UBIGINT AS begin_snapshot,end_snapshot::UBIGINT AS end_snapshot "
				        "FROM __ducklake_metadata_lake.ducklake_data_file WHERE table_id=(SELECT table_id FROM "
				        "__ducklake_metadata_lake.ducklake_table WHERE table_name='items' AND end_snapshot IS NULL) "
				        "ORDER BY data_file_id";
			}
			auto result = SQL(writer, query);
			ViewJSON json;
			auto rows = json.Array();
			for (idx_t row = 0; row < result->RowCount(); row++) {
				auto value = json.Object();
				for (idx_t col = 0; col < result->ColumnCount(); col++) {
					auto item = result->GetValue(col, row);
					if (item.IsNull()) {
						json.Add(value, result->names[col].c_str(), yyjson_mut_null(json.doc.get()));
					} else if (item.type().id() == LogicalTypeId::BOOLEAN) {
						json.Add(value, result->names[col].c_str(),
						         yyjson_mut_bool(json.doc.get(), item.GetValue<bool>()));
					} else if (item.type().id() == LogicalTypeId::UBIGINT) {
						json.Number(value, result->names[col].c_str(), item.GetValue<idx_t>());
					} else if (item.type().id() == LogicalTypeId::LIST) {
						auto entries = json.Array();
						for (auto &entry : ListValue::GetChildren(item)) {
							auto text = StringValue::Get(entry);
							yyjson_mut_arr_append(entries,
							                      yyjson_mut_strncpy(json.doc.get(), text.data(), text.size()));
						}
						json.Add(value, result->names[col].c_str(), entries);
					} else {
						json.Text(value, result->names[col].c_str(), StringValue::Get(item));
					}
				}
				yyjson_mut_arr_append(rows, value);
			}
			return json.Finish(rows);
		}
		if (op == "retire") {
			return DuckLakeVortexIndex::RetireEmpty(*writer.context, "lake", "main", "items", "knn", args.at(2),
			                                        std::stoull(args.at(3)))
			    .ToJSON();
		}
		if (op == "rewrite-f") {
			return DuckLakeVortexIndex::Rewrite(*writer.context, "lake", "main", "items", "knn", args.at(2), "",
			                                    std::stoull(args.at(3)), nullptr, "", DuckLakeVortexRewriteMode::FILES)
			    .ToJSON();
		}
		if (op == "rewrite" || op == "prune") {
			return DuckLakeVortexIndex::Rewrite(*writer.context, "lake", "main", "items", "knn", args.at(2), args.at(3),
			                                    std::stoull(args.at(4)), nullptr, "",
			                                    op == "prune" ? DuckLakeVortexRewriteMode::PRUNE
			                                                  : DuckLakeVortexRewriteMode::INDEXED)
			    .ToJSON();
		}
		if (op == "rewrite-fail" || op == "rewrite-cancel" || op == "rewrite-crash" || op == "rewrite-collision" ||
		    op == "rewrite-crash-append" || op == "rewrite-corrupt-prepared") {
			Connection rewriter(database);
			return DuckLakeVortexIndex::Rewrite(
			           *rewriter.context, "lake", "main", "items", "knn", args.at(3), args.at(4),
			           std::stoull(args.at(5)),
			           [&](const string &stage) {
				           if (op == "rewrite-crash-append" && stage == "rewrite_candidate_ready") {
					           SQL(writer, ReadText(args.at(6)));
				           }
				           if (stage != args.at(2)) {
					           return;
				           }
				           if (op == "rewrite-fail") {
					           throw InvalidInputException("Injected indexed rewrite failure");
				           }
				           if (op == "rewrite-cancel") {
					           rewriter.Interrupt();
					           return;
				           }
				           if (op == "rewrite-collision") {
					           auto paths = SQL(writer, "SELECT new_path FROM __ducklake_metadata_lake."
					                                    "ducklake_vortex_rewrites_v1 WHERE NOT keep_file");
					           Require(paths->RowCount() == 1, "Expected one rewrite intent");
					           Write(StringValue::Get(paths->GetValue(0, 0)), "foreign-rewrite-output");
					           return;
				           }
				           if (op == "rewrite-corrupt-prepared") {
					           auto workspace =
					               SQL(writer,
					                   "SELECT workspace FROM __ducklake_metadata_lake.ducklake_vortex_builds_v1 b "
					                   "JOIN __ducklake_metadata_lake.ducklake_vortex_rewrites_v1 r USING(build_id) "
					                   "WHERE NOT r.keep_file");
					           Require(workspace->RowCount() == 1, "Expected one private rewrite workspace");
					           auto path = StringValue::Get(workspace->GetValue(0, 0)) +
					                       "/prepared/artifacts/spfresh/native/vectors.bin";
					           auto bytes = ReadText(path);
					           Require(!bytes.empty(), "Expected a nonempty native vector artifact");
					           bytes[0] ^= 1;
					           Write(path, bytes);
					           return;
				           }
				           Write(root + "/rewrite-" + stage + ".ready", "ready");
				           string line;
				           std::getline(std::cin, line);
				           throw InvalidInputException("Rewrite fixture resumed without SIGKILL");
			           })
			    .ToJSON();
		}
		if (op == "rewrite-live-append") {
			Connection rewriter(database);
			std::promise<bool> start;
			auto ready = start.get_future();
			auto writes = std::async(std::launch::async, [&] {
				if (ready.get()) {
					SQL(writer, ReadText(args.at(5)));
				}
			});
			bool started = false;
			idx_t builds = 0, bindings = 0, provisional_id = 0;
			try {
				auto result = DuckLakeVortexIndex::Rewrite(
				    *rewriter.context, "lake", "main", "items", "knn", args.at(2), args.at(3), std::stoull(args.at(4)),
				    [&](const string &stage) {
					    if (stage == "build_started") {
						    Connection inspect(database);
						    auto planned = SQL(inspect, "SELECT new_file_id FROM __ducklake_metadata_lake."
						                                "ducklake_vortex_rewrites_v1 WHERE NOT keep_file");
						    Require(planned->RowCount() == 1, "Expected one live rewrite intent");
						    provisional_id = planned->GetValue(0, 0).GetValue<idx_t>();
						    builds++;
						    start.set_value(true);
						    started = true;
					    }
					    bindings += stage == "rewrite_bind_started";
					    if (stage == "rewrite_candidate_ready") {
						    writes.get();
					    }
				    });
				return "{\"published\":" + result.ToJSON() + ",\"native_builds\":" + to_string(builds) +
				       ",\"bindings\":" + to_string(bindings) +
				       ",\"provisional_file_id\":" + to_string(provisional_id) + "}";
			} catch (...) {
				if (!started) {
					start.set_value(false);
				}
				throw;
			}
		}
		if (op == "rewrite-write-race" || op == "rewrite-orphan-race" || op == "rewrite-commit-race" ||
		    op == "rewrite-admission-race") {
			Connection rewriter(database);
			Pause pause;
			idx_t provisional_id = 0;
			auto rewrite = std::async(std::launch::async, [&] {
				try {
					idx_t builds = 0, bindings = 0;
					auto result =
					    DuckLakeVortexIndex::Rewrite(*rewriter.context, "lake", "main", "items", "knn", args.at(3),
					                                 args.at(4), std::stoull(args.at(5)), [&](const string &stage) {
						                                 builds += stage == "build_started";
						                                 bindings += stage == "rewrite_bind_started";
						                                 if (stage == args.at(2)) {
							                                 pause.Stop();
						                                 }
					                                 });
					return "{\"published\":" + result.ToJSON() + ",\"native_builds\":" + to_string(builds) +
					       ",\"bindings\":" + to_string(bindings) +
					       ",\"provisional_file_id\":" + to_string(provisional_id) + "}";
				} catch (const std::exception &error) {
					ViewJSON json;
					auto value = json.Object();
					json.Text(value, "error", error.what());
					return json.Finish(value);
				}
			});
			ReleasePause release {pause};
			pause.Wait();
			auto planned = SQL(writer, "SELECT new_file_id FROM __ducklake_metadata_lake.ducklake_vortex_rewrites_v1 "
			                           "WHERE NOT keep_file");
			Require(planned->RowCount() == 1, "Expected one unpublished rewrite intent");
			provisional_id = planned->GetValue(0, 0).GetValue<idx_t>();
			if (op == "rewrite-orphan-race") {
				SQL(writer,
				    "PREPARE stale_cleanup AS SELECT * FROM ducklake_delete_orphaned_files('lake',cleanup_all=>true)");
				pause.Release();
				auto result = rewrite.get();
				SQL(writer, "EXECUTE stale_cleanup");
				SQL(writer, "DEALLOCATE stale_cleanup");
				return result;
			}
			if (op == "rewrite-admission-race") {
				auto &publication = DuckLakeVortexIndex::PublicationLock();
				auto &admission = DuckLakeVortexOptimizer::WriteAdmissionLock(*writer.context->db);
				const auto observed = publication.SharedWaitCount();
				auto blocker = publication.GetExclusiveLock([] {});
				auto commit = std::async(std::launch::async, [&] { SQL(writer, ReadText(args.at(6))); });
				auto wait_for_lock = [](const std::function<bool()> &waiting) {
					const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
					while (!waiting() && std::chrono::steady_clock::now() < deadline) {
						std::this_thread::yield();
					}
					return waiting();
				};
				// The writer holds write admission while it waits for shared publication.
				const bool writer_waited = wait_for_lock([&] { return publication.SharedWaitCount() > observed; });
				pause.Release();
				const bool rewrite_waited = wait_for_lock([&] { return admission.WaitingWriterCount() != 0; });
				blocker.reset();
				commit.get();
				auto result = rewrite.get();
				Require(writer_waited && rewrite_waited, "Did not order both commits through write admission");
				return result;
			}
			if (op == "rewrite-commit-race") {
				std::promise<void> started;
				auto ready = started.get_future();
				auto commit = std::async(std::launch::async, [&] {
					started.set_value();
					SQL(writer, ReadText(args.at(6)));
				});
				ready.wait();
				Require(commit.wait_for(std::chrono::milliseconds(100)) == std::future_status::timeout,
				        "Source commit bypassed final rewrite publication");
				pause.Release();
				auto result = rewrite.get();
				commit.get();
				return result;
			}
			SQL(writer, ReadText(args.at(6)));
			pause.Release();
			return rewrite.get();
		}
		if (op == "rewrite-cancel-lock") {
			Connection caller(database);
			const bool collection = args.at(2) == "collection";
			const bool publication = args.at(2) == "publication";
			Require(collection || publication || args.at(2) == "write-admission", "Unknown rewrite lock fixture");
			auto &lock = collection    ? DuckLakeVortexIndex::CollectionLock()
			             : publication ? DuckLakeVortexIndex::PublicationLock()
			                           : DuckLakeVortexOptimizer::WriteAdmissionLock(*caller.context->db);
			unique_ptr<DuckLakeVortexCollectionLock::Key> guard;
			const auto observed = lock.SharedWaitCount();
			if (collection) {
				guard = lock.GetExclusiveLock([] {});
			}
			Pause pause;
			auto rewrite = std::async(std::launch::async, [&] {
				try {
					DuckLakeVortexIndex::Rewrite(*caller.context, "lake", "main", "items", "knn", args.at(3),
					                             args.at(4), std::stoull(args.at(5)), [&](const string &stage) {
						                             if (!collection && stage == "rewrite_source_linked") {
							                             pause.Stop();
						                             }
					                             });
					return false;
				} catch (const std::exception &error) {
					return StringUtil::Contains(error.what(), "Interrupt");
				}
			});
			ReleasePause release {pause};
			if (!collection) {
				pause.Wait();
				guard = lock.GetSharedLock([] {});
				pause.Release();
			}
			auto start = std::chrono::steady_clock::now();
			bool waiting = false;
			while (std::chrono::steady_clock::now() - start < std::chrono::seconds(5) &&
			       rewrite.wait_for(std::chrono::milliseconds(0)) != std::future_status::ready) {
				waiting = collection ? lock.SharedWaitCount() > observed : lock.WaitingWriterCount() != 0;
				if (waiting) {
					break;
				}
				std::this_thread::yield();
			}
			caller.Interrupt();
			const bool cancelled_before_release =
			    rewrite.wait_for(std::chrono::seconds(5)) == std::future_status::ready;
			guard.reset();
			Require(rewrite.get() && waiting && cancelled_before_release,
			        "Rewrite did not cancel after entering the actual lock wait");
			return "{\"waiting\":true,\"cancelled_before_release\":true}";
		}
		if (op == "rewrite-build-gc-source-wait") {
			Connection collector(database);
			auto &lock = DuckLakeVortexReadView::AdmissionLock();
			auto old_file_cleanup = lock.GetExclusiveLock([] {});
			const auto observed = lock.SharedWaitCount();
			auto collection = std::async(std::launch::async, [&] {
				return DuckLakeVortexIndex::CollectBuilds(*collector.context, "lake", 1, std::stoull(args.at(2)));
			});
			auto start = std::chrono::steady_clock::now();
			while (lock.SharedWaitCount() == observed &&
			       std::chrono::steady_clock::now() - start < std::chrono::seconds(5) &&
			       collection.wait_for(std::chrono::milliseconds(0)) != std::future_status::ready) {
				std::this_thread::yield();
			}
			const bool waited = lock.SharedWaitCount() > observed;
			old_file_cleanup.reset();
			auto result = collection.get();
			Require(waited, "Rewrite build collection did not wait for the old-file cleanup guard");
			return BuildGCJSON(result);
		}
		if (op == "rewrite-stage-limit") {
			DuckLakeVortexRewriteState plan;
			plan.caller = *writer.context;
			plan.staged_inputs.resize(4094);
			auto path = root + "/unopened.vortex";
			plan.selected_files.push_back({1, path, "", 1});
			DuckLakeVortexIndex::StageRewriteInput(plan, 1, path, 1);
			return "{}";
		}
		if (op == "rewrite-reader-race") {
			Connection reader(database), rewriter(database);
			Pause pause;
			DuckLakeVortexSearchRequest request;
			request.query = {133, 4, 2, 0, 1, 8, 7, 16};
			request.k = 10;
			auto pending = std::async(std::launch::async, [&] {
				return DuckLakeVortexSearch::Execute(*reader.context, Options("knn"), request,
				                                     [&](const string &stage) {
					                                     if (stage == args.at(2)) {
						                                     pause.Stop();
					                                     }
				                                     });
			});
			ReleasePause release {pause};
			pause.Wait();
			auto result = DuckLakeVortexIndex::Rewrite(*rewriter.context, "lake", "main", "items", "knn", args.at(3),
			                                           args.at(4), std::stoull(args.at(5)));
			auto gc = std::async(std::launch::async, [&] {
				Connection collector(database);
				return DuckLakeVortexIndex::Collect(*collector.context, "lake", 16, 64 * 1024 * 1024);
			});
			ReleasePause release_gc {pause};
			if (args.at(2) == "after_open") {
				Require(gc.wait_for(std::chrono::seconds(5)) == std::future_status::ready,
				        "Private view blocked rewrite GC");
			} else {
				Require(gc.wait_for(std::chrono::milliseconds(100)) == std::future_status::timeout,
				        "GC bypassed old-view admission");
			}
			pause.Release();
			auto held = pending.get();
			return "{\"published\":" + result.ToJSON() + ",\"held\":" + SearchJSON(held.rows, held.diagnostics) +
			       ",\"gc\":" + GCJSON(gc.get()) + "}";
		}
		if (op == "recover-builds") {
			auto result = SQL(writer, "SELECT * FROM ducklake_recover_vortex_builds('lake'," + args.at(2) + ")");
			return to_string(result->GetValue(0, 0).GetValue<idx_t>());
		}
		if (op == "build-status-transaction") {
			Connection caller(database);
			SQL(caller, "CREATE TEMP TABLE status_marker(i INTEGER)");
			caller.BeginTransaction();
			SQL(caller, "INSERT INTO status_marker VALUES (1)");
			bool rejected = false;
			try {
				DuckLakeVortexIndex::BuildStatus(caller, "lake");
			} catch (const InvalidInputException &error) {
				rejected = string(error.what()).find("idle dedicated connection") != string::npos;
			}
			Require(rejected && caller.context->transaction.HasActiveTransaction(),
			        "Build status did not preserve its caller transaction");
			Require(SQL(caller, "SELECT count(*) FROM status_marker")->GetValue(0, 0).GetValue<idx_t>() == 1,
			        "Build status discarded pending work");
			caller.Rollback();
			Require(SQL(caller, "SELECT count(*) FROM status_marker")->GetValue(0, 0).GetValue<idx_t>() == 0,
			        "Build status committed pending work");
			return "{\"caller_rollback\":true}";
		}
		if (op == "publication-commit-order") {
			const bool cancel = args.at(2) == "cancel";
			Require(cancel || args.at(2) == "finish", "Unknown publication ordering case");
			Connection caller(database), owner(database), later(database);
			SQL(later, "BEGIN; INSERT INTO lake.publication_markers VALUES (1)");
			auto &gate = DuckLakeVortexIndex::PublicationLock();
			auto first_commit = gate.GetSharedLock([] {});
			std::promise<void> validated, resume;
			auto inside = validated.get_future();
			auto release = resume.get_future();
			auto publisher = std::async(std::launch::async, [&] {
				return DuckLakeVortexIndex::Publish(owner, "lake", "main", "items", args.at(2), args.at(3), 0,
				                                    *caller.context, [&](const string &stage) {
					                                    if (stage == "after_validation") {
						                                    validated.set_value();
						                                    release.wait_for(std::chrono::seconds(5));
					                                    }
				                                    });
			});
			// The first commit keeps the exclusive guard unavailable. A refused shared admission signals reservation.
			const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
			bool reserved = false;
			while (std::chrono::steady_clock::now() < deadline) {
				if (!gate.TryGetSharedLock()) {
					reserved = true;
					break;
				}
				std::this_thread::yield();
			}
			const auto before = gate.SharedWaitCount();
			auto late_commit = std::async(std::launch::async, [&] { later.Commit(); });
			const auto admission_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
			while (gate.SharedWaitCount() == before && std::chrono::steady_clock::now() < admission_deadline &&
			       late_commit.wait_for(std::chrono::milliseconds(0)) == std::future_status::timeout) {
				std::this_thread::yield();
			}
			// Only the actual COMMIT can enter a shared wait in this fixture.
			const auto shared_waits = gate.SharedWaitCount() - before;
			const bool late_waited =
			    shared_waits == 1 && late_commit.wait_for(std::chrono::milliseconds(0)) == std::future_status::timeout;
			bool ordered;
			if (cancel) {
				caller.Interrupt();
				const bool stopped = publisher.wait_for(std::chrono::seconds(5)) == std::future_status::ready;
				ordered = stopped && late_commit.wait_for(std::chrono::seconds(5)) == std::future_status::ready;
				first_commit.reset();
			} else {
				first_commit.reset();
				ordered = inside.wait_for(std::chrono::seconds(5)) == std::future_status::ready &&
				          late_commit.wait_for(std::chrono::milliseconds(0)) == std::future_status::timeout;
			}
			resume.set_value();
			bool interrupted = false;
			try {
				publisher.get();
			} catch (const InterruptException &) {
				interrupted = true;
			}
			late_commit.get();
			Require(reserved && late_waited && ordered && cancel == interrupted,
			        "Publication reservation lost ordering or retained a cancelled guard");
			return "{\"ordered\":true,\"shared_waits\":" + to_string(shared_waits) + "}";
		}
		if (op == "build-cancel-source-wait") {
			const auto &stage = args.at(2);
			Require(stage == "before_source_admission" || stage == "before_publish_source_admission",
			        "Unknown source admission stage");
			Connection builder(database);
			Pause pause;
			auto pending = std::async(std::launch::async, [&] {
				return DuckLakeVortexIndex::BuildAndPublish(*builder.context, "lake", "main", "items", "knn",
				                                            args.at(3), 8, std::stoull(args.at(4)),
				                                            [&](const string &observed) {
					                                            if (observed == stage) {
						                                            pause.Stop();
					                                            }
				                                            });
			});
			ReleasePause release {pause};
			pause.Wait();
			auto source_cleanup = DuckLakeVortexReadView::AdmissionLock().GetExclusiveLock([] {});
			pause.Release();
			const bool blocked = pending.wait_for(std::chrono::milliseconds(100)) == std::future_status::timeout;
			builder.Interrupt();
			const bool cancelled = pending.wait_for(std::chrono::seconds(5)) == std::future_status::ready;
			auto maintenance = std::async(std::launch::async, [&] {
				Connection collector(database);
				return DuckLakeVortexIndex::CollectBuilds(*collector.context, "lake", 1, 64 * 1024 * 1024);
			});
			const bool collected = maintenance.wait_for(std::chrono::seconds(5)) == std::future_status::ready;
			source_cleanup.reset();
			bool interrupted = false;
			try {
				pending.get();
			} catch (const InterruptException &) {
				interrupted = true;
			}
			auto result = maintenance.get();
			Require(blocked && cancelled && interrupted, "Build cancellation waited for source cleanup");
			Require(collected && result.builds == 1, "Cancelled build retained its maintenance guard");
			return "{\"collected_before_source_unlock\":true}";
		}
		if (op == "build-gc" || op == "build-gc-cancel" || op == "build-gc-fail" || op == "build-gc-crash" ||
		    op == "build-gc-policy") {
			Connection collector(database);
			const bool controlled = op != "build-gc";
			auto result = DuckLakeVortexIndex::CollectBuilds(
			    *collector.context, "lake", std::stoull(args.at(controlled ? 3 : 2)),
			    std::stoull(args.at(controlled ? 4 : 3)), [&](const string &stage) {
				    if (!controlled || stage != args.at(2)) {
					    return;
				    }
				    if (op == "build-gc-cancel") {
					    collector.Interrupt();
				    } else if (op == "build-gc-policy") {
					    SQL(writer, ReadText(args.at(5)));
				    } else if (op == "build-gc-fail") {
					    throw InvalidInputException("Injected Vortex build collection failure");
				    } else {
					    Write(root + "/build-gc-" + stage + ".ready", "ready");
					    string line;
					    std::getline(std::cin, line);
					    throw InvalidInputException("Build collection resumed without SIGKILL");
				    }
			    });
			return BuildGCJSON(result);
		}
		if (op == "build-maintenance-race") {
			Connection builder(database);
			Pause pause;
			auto pending = std::async(std::launch::async, [&] {
				return DuckLakeVortexIndex::BuildAndPublish(
				    *builder.context, "lake", "main", "items", "knn", args.at(2), 8, std::stoull(args.at(3)),
				    [&](const string &stage) {
					    if (stage == args.at(4)) {
						    pause.Stop();
					    }
				    },
				    "", optional_idx(), DuckLakeVortexIndexOperation::APPEND);
			});
			ReleasePause release {pause};
			pause.Wait();
			std::promise<void> waiting;
			auto started = waiting.get_future();
			auto maintenance = std::async(std::launch::async, [&] {
				Connection collector(database);
				auto observe = [&](const string &stage) {
					if (stage == "build_gc_waiting" || stage == "build_recovery_waiting") {
						waiting.set_value();
					}
				};
				if (args.at(5) == "recover") {
					return to_string(DuckLakeVortexIndex::RecoverBuilds(*collector.context, "lake", 16, observe));
				}
				return BuildGCJSON(
				    DuckLakeVortexIndex::CollectBuilds(*collector.context, "lake", 16, 64 * 1024 * 1024, observe));
			});
			ReleasePause release_maintenance {pause};
			started.wait();
			Require(maintenance.wait_for(std::chrono::milliseconds(100)) == std::future_status::timeout,
			        "Build maintenance bypassed a live registered builder");
			pause.Release();
			auto published = pending.get();
			return "{\"published\":" + published.ToJSON() + ",\"maintenance\":" + maintenance.get() + "}";
		}
		if (op == "build-gc-publish-race") {
			Pause pause;
			auto gc = std::async(std::launch::async, [&] {
				Connection collector(database);
				return DuckLakeVortexIndex::CollectBuilds(*collector.context, "lake", 16, 64 * 1024 * 1024,
				                                          [&](const string &stage) {
					                                          if (stage == "build_gc_deleting") {
						                                          pause.Stop();
					                                          }
				                                          });
			});
			ReleasePause release {pause};
			pause.Wait();
			auto publish = std::async(std::launch::async, [&] {
				Connection publisher(database);
				try {
					DuckLakeVortexIndex::Publish(publisher, "lake", "main", "items", "gc-alias", args.at(2), 0,
					                             *publisher.context);
					return string();
				} catch (const std::exception &error) {
					return string(error.what());
				}
			});
			ReleasePause release_publish {pause};
			Require(publish.wait_for(std::chrono::milliseconds(100)) == std::future_status::timeout,
			        "Publication bypassed candidate collection");
			pause.Release();
			auto result = gc.get();
			auto error = publish.get();
			Require(error.find("Cannot resolve local Vortex index path") != string::npos,
			        "Publisher did not reject the collected candidate: " + error);
			return BuildGCJSON(result);
		}
		if (op == "gc" || op == "gc-cancel" || op == "gc-fail" || op == "gc-crash" || op == "gc-policy") {
			Connection collector(database);
			const bool controlled = op != "gc";
			auto result = DuckLakeVortexIndex::Collect(
			    *collector.context, "lake", std::stoull(args.at(controlled ? 3 : 2)),
			    std::stoull(args.at(controlled ? 4 : 3)), [&](const string &stage) {
				    if (!controlled || stage != args.at(2)) {
					    return;
				    }
				    if (op == "gc-cancel") {
					    collector.Interrupt();
				    } else if (op == "gc-policy") {
					    SQL(writer, ReadText(args.at(5)));
				    } else if (op == "gc-fail") {
					    throw InvalidInputException("Injected Vortex collection failure");
				    } else {
					    Write(root + "/gc-" + stage + ".ready", "ready");
					    string line;
					    std::getline(std::cin, line);
					    throw InvalidInputException("Collection crash fixture resumed without SIGKILL");
				    }
			    });
			return GCJSON(result);
		}
		if (op == "gc-cancel-wait" || op == "build-gc-cancel-wait" || op == "build-recovery-cancel-wait") {
			Require(args.at(2) == "admissions" || args.at(2) == "publication", "Unknown GC wait lock");
			unique_ptr<DuckLakeVortexCollectionLock::Key> admission_blocker;
			unique_ptr<DuckLakeVortexCollectionLock::Key> publication_blocker;
			if (args.at(2) == "admissions") {
				admission_blocker = DuckLakeVortexIndex::CollectionLock().GetSharedLock([] {});
			} else {
				publication_blocker = DuckLakeVortexIndex::PublicationLock().GetSharedLock([] {});
			}
			Connection collector(database);
			std::promise<void> waiting;
			auto started = waiting.get_future();
			auto observe = [&](const string &stage) {
				if (stage == "gc_waiting" || stage == "build_gc_waiting" || stage == "build_recovery_waiting") {
					waiting.set_value();
				}
			};
			auto pending = std::async(std::launch::async, [&] {
				if (op == "build-recovery-cancel-wait") {
					DuckLakeVortexIndex::RecoverBuilds(*collector.context, "lake", 1, observe);
				} else if (op == "build-gc-cancel-wait") {
					DuckLakeVortexIndex::CollectBuilds(*collector.context, "lake", 1, 64 * 1024 * 1024, observe);
				} else {
					DuckLakeVortexIndex::Collect(*collector.context, "lake", 1, 64 * 1024 * 1024, observe);
				}
			});
			const bool reached = started.wait_for(std::chrono::seconds(5)) == std::future_status::ready;
			const bool blocked = pending.wait_for(std::chrono::milliseconds(100)) == std::future_status::timeout;
			collector.Interrupt();
			const bool cancelled = pending.wait_for(std::chrono::seconds(5)) == std::future_status::ready;
			admission_blocker.reset();
			publication_blocker.reset();
			string error;
			try {
				pending.get();
			} catch (const std::exception &failure) {
				error = failure.what();
			}
			Require(reached && blocked, "GC did not reach the held lock");
			Require(cancelled && error.find("Interrupt") != string::npos,
			        "GC ignored cancellation while its lock remained held");
			return "{\"interrupted_before_unlock\":true}";
		}
		if (op == "gc-cancel-admission") {
			auto kind = args.at(2);
			Require(kind == "search" || kind == "replace" || kind == "append" || kind == "merge",
			        "Unknown waiting admission kind");
			auto head = Head("knn");
			Pause pause;
			auto gc = std::async(std::launch::async, [&] {
				Connection collector(database);
				return DuckLakeVortexIndex::Collect(*collector.context, "lake", 1, 64 * 1024 * 1024,
				                                    [&](const string &stage) {
					                                    if (stage == "gc_acquired") {
						                                    pause.Stop();
					                                    }
				                                    });
			});
			ReleasePause release {pause};
			pause.Wait();
			Connection caller(database);
			std::promise<void> entered;
			auto started = entered.get_future();
			auto pending = std::async(std::launch::async, [&] {
				if (kind == "search") {
					DuckLakeVortexSearchRequest request;
					request.query = {133, 4, 2, 0, 1, 8, 7, 16};
					request.k = 10;
					entered.set_value();
					DuckLakeVortexSearch::Execute(*caller.context, Options("knn"), request);
				} else {
					vector<string> selected;
					auto operation = DuckLakeVortexIndexOperation::REPLACE;
					if (kind == "merge") {
						operation = DuckLakeVortexIndexOperation::MERGE;
						selected = {head.segments[0].generation, head.segments[1].generation};
					} else if (kind == "append") {
						operation = DuckLakeVortexIndexOperation::APPEND;
					}
					DuckLakeVortexIndex::BuildAndPublish(
					    *caller.context, "lake", "main", "items", "knn", root + "/cancelled-capture.json", 8,
					    head.revision,
					    [&](const string &stage) {
						    if (stage == "before_build") {
							    entered.set_value();
						    }
					    },
					    "", optional_idx(), operation, selected);
				}
			});
			const bool reached = started.wait_for(std::chrono::seconds(5)) == std::future_status::ready;
			const bool blocked = pending.wait_for(std::chrono::milliseconds(100)) == std::future_status::timeout;
			caller.Interrupt();
			const bool cancelled = pending.wait_for(std::chrono::seconds(5)) == std::future_status::ready;
			auto references = database.instance.use_count();
			auto connections = database.instance->GetConnectionManager().GetConnectionList().size();
			pause.Release();
			string error;
			try {
				pending.get();
			} catch (const std::exception &failure) {
				error = failure.what();
			}
			auto collected = gc.get();
			Require(reached && blocked && cancelled && error.find("Interrupt") != string::npos,
			        "Admission ignored caller cancellation while GC remained paused: " + error);
			Require(references == 4 && connections == 3, "Cancelled admission retained its dedicated owner");
			Require(collected.generations == 0, "Waiting cancellation changed active generation ownership");
			return "{\"interrupted_before_gc_unlock\":true,\"database_references_before_unlock\":4,"
			       "\"connections_before_unlock\":3}";
		}
		if (op == "gc-lease-race") {
			Connection query(database), publisher(database);
			Pause pause;
			DuckLakeVortexSearchRequest request;
			request.query = {133, 4, 2, 0, 1, 8, 7, 16};
			request.k = 10;
			auto pending = std::async(std::launch::async, [&] {
				return DuckLakeVortexSearch::Execute(*query.context, Options("knn"), request, [&](const string &stage) {
					if (stage == args.at(2)) {
						pause.Stop();
					}
				});
			});
			ReleasePause release {pause};
			pause.Wait();
			DuckLakeVortexIndex::Publish(publisher, "lake", "main", "items", "knn", args.at(3), std::stoull(args.at(4)),
			                             *publisher.context, nullptr, DuckLakeVortexIndexOperation::MERGE,
			                             {args.at(5), args.at(6)});
			std::promise<void> waiting;
			auto started = waiting.get_future();
			auto gc = std::async(std::launch::async, [&] {
				Connection collector(database);
				return DuckLakeVortexIndex::Collect(*collector.context, "lake", 16, 64 * 1024 * 1024,
				                                    [&](const string &stage) {
					                                    if (stage == "gc_waiting") {
						                                    waiting.set_value();
					                                    }
				                                    });
			});
			ReleasePause release_gc {pause};
			started.wait();
			if (args.at(2) == "after_open") {
				Require(gc.wait_for(std::chrono::seconds(5)) == std::future_status::ready,
				        "Private leases blocked retired generation GC");
			} else {
				Require(gc.wait_for(std::chrono::milliseconds(100)) == std::future_status::timeout,
				        "GC bypassed in-flight query admission");
			}
			const bool cancel = args.size() > 7 && args.at(7) == "cancel";
			if (cancel) {
				query.Interrupt();
			}
			pause.Release();
			string held = "null";
			if (cancel) {
				bool interrupted = false;
				try {
					pending.get();
				} catch (const std::exception &error) {
					interrupted = string(error.what()).find("Interrupt") != string::npos;
				}
				Require(interrupted, "Query admission cancellation did not interrupt");
			} else {
				auto result = pending.get();
				held = SearchJSON(result.rows, result.diagnostics);
			}
			return "{\"held\":" + held + ",\"gc\":" + GCJSON(gc.get()) + "}";
		}
		if (op == "gc-publish-race") {
			Pause pause;
			auto gc = std::async(std::launch::async, [&] {
				Connection collector(database);
				return DuckLakeVortexIndex::Collect(*collector.context, "lake", 1, 64 * 1024 * 1024,
				                                    [&](const string &stage) {
					                                    if (stage == "gc_deleting") {
						                                    pause.Stop();
					                                    }
				                                    });
			});
			ReleasePause release {pause};
			pause.Wait();
			auto publish = std::async(std::launch::async, [&] {
				Connection publisher(database);
				try {
					DuckLakeVortexIndex::Publish(publisher, "lake", "main", "items", "gc-alias", args.at(2), 0,
					                             *publisher.context);
					return string();
				} catch (const std::exception &error) {
					return string(error.what());
				}
			});
			ReleasePause release_publish {pause};
			Require(publish.wait_for(std::chrono::milliseconds(100)) == std::future_status::timeout,
			        "Publication bypassed generation collection");
			pause.Release();
			auto result = gc.get();
			auto error = publish.get();
			Require(error.find("Cannot resolve local Vortex index path") != string::npos,
			        "Publisher did not reject the collected generation: " + error);
			return GCJSON(result);
		}
		if (op == "gc-build-race") {
			Connection builder(database);
			Pause pause;
			auto pending = std::async(std::launch::async, [&] {
				return DuckLakeVortexIndex::BuildAndPublish(
				    *builder.context, "lake", "main", "items", "knn", args.at(2), 8, std::stoull(args.at(3)),
				    [&](const string &stage) {
					    if (stage == "build_started") {
						    pause.Stop();
					    }
				    },
				    "", optional_idx(), DuckLakeVortexIndexOperation::APPEND);
			});
			ReleasePause release {pause};
			pause.Wait();
			std::promise<void> waiting;
			auto started = waiting.get_future();
			auto gc = std::async(std::launch::async, [&] {
				Connection collector(database);
				return DuckLakeVortexIndex::Collect(*collector.context, "lake", 16, 64 * 1024 * 1024,
				                                    [&](const string &stage) {
					                                    if (stage == "gc_waiting") {
						                                    waiting.set_value();
					                                    }
				                                    });
			});
			ReleasePause release_gc {pause};
			started.wait();
			Require(gc.wait_for(std::chrono::milliseconds(100)) == std::future_status::timeout,
			        "GC bypassed an active native build");
			pause.Release();
			auto published = pending.get();
			return "{\"published\":" + published.ToJSON() + ",\"gc\":" + GCJSON(gc.get()) + "}";
		}
		if (op == "build" || op == "append") {
			auto function = op == "append" ? "ducklake_append_vortex_index_segment" : "ducklake_build_vortex_index";
			auto result = SQL(writer, "SELECT * FROM " + string(function) + "('lake','main','items'," +
			                              Value(args.at(2)).ToSQLString() + "," + Value(args.at(3)).ToSQLString() +
			                              ",8," + args.at(4) + ")");
			ViewJSON json;
			auto value = json.Object();
			json.Number(value, "revision", result->GetValue(0, 0).GetValue<idx_t>());
			json.Text(value, "generation", StringValue::Get(result->GetValue(1, 0)));
			return json.Finish(value);
		}
		if (op == "merge" || op == "publish-merge") {
			auto function =
			    op == "merge" ? "ducklake_merge_vortex_index_segments" : "ducklake_publish_vortex_index_merge";
			SQL(writer, "SELECT * FROM " + string(function) + "('lake','main','items','knn'," +
			                Value(args.at(2)).ToSQLString() + ",[" + Value(args.at(4)).ToSQLString() + "," +
			                Value(args.at(5)).ToSQLString() + "]," + args.at(3) + ")");
			return Head("knn").ToJSON();
		}
		if (op == "cancel-build" || op == "fail-build" || op == "crash-build" || op == "cancel-append" ||
		    op == "fail-append" || op == "crash-append" || op == "cancel-merge" || op == "fail-merge" ||
		    op == "crash-merge") {
			Connection builder(database);
			const bool merge = op.find("merge") != string::npos;
			return DuckLakeVortexIndex::BuildAndPublish(
			           *builder.context, "lake", "main", "items", "knn", args.at(3), 8, std::stoull(args.at(4)),
			           [&](const string &stage) {
				           if (stage != args.at(2)) {
					           return;
				           }
				           if (op.find("cancel-") == 0) {
					           builder.Interrupt();
				           } else if (op.find("fail-") == 0) {
					           throw InvalidInputException("Injected native build failure");
				           } else {
					           Write(root + "/build-" + stage + ".ready", "ready");
					           string line;
					           std::getline(std::cin, line);
					           throw InvalidInputException("Build crash fixture resumed without SIGKILL");
				           }
			           },
			           "", optional_idx(),
			           merge                               ? DuckLakeVortexIndexOperation::MERGE
			           : op.find("append") != string::npos ? DuckLakeVortexIndexOperation::APPEND
			                                               : DuckLakeVortexIndexOperation::REPLACE,
			           merge ? vector<string> {args.at(5), args.at(6)} : vector<string>())
			    .ToJSON();
		}
		if (op == "split-build" || op == "mutate-build" || op == "split-append" || op == "split-merge") {
			Connection builder(database);
			Pause pause;
			auto pending = std::async(std::launch::async, [&] {
				return DuckLakeVortexIndex::BuildAndPublish(
				    *builder.context, "lake", "main", "items", "knn", args.at(2), 8, std::stoull(args.at(3)),
				    [&](const string &stage) {
					    if (stage == args.at(4)) {
						    pause.Stop();
					    }
				    },
				    "", optional_idx(),
				    op == "split-merge"    ? DuckLakeVortexIndexOperation::MERGE
				    : op == "split-append" ? DuckLakeVortexIndexOperation::APPEND
				                           : DuckLakeVortexIndexOperation::REPLACE,
				    op == "split-merge" ? vector<string> {args.at(6), args.at(7)} : vector<string>());
			});
			ReleasePause release {pause};
			pause.Wait();
			DuckLakeVortexSearchRequest request;
			request.query = {133, 4, 2, 0, 1, 8, 7, 16};
			request.k = 10;
			string before;
			if (op != "mutate-build") {
				auto rows = DuckLakeVortexSearch::Execute(*writer.context, Options("knn"), request);
				before = SearchJSON(rows.rows, rows.diagnostics);
			}
			SQL(writer, ReadText(args.at(5)));
			string during;
			if (op != "mutate-build") {
				auto rows = DuckLakeVortexSearch::Execute(*writer.context, Options("knn"), request);
				during = SearchJSON(rows.rows, rows.diagnostics);
			}
			pause.Release();
			auto published = pending.get();
			if (op == "mutate-build") {
				return published.ToJSON();
			}
			return "{\"published\":" + published.ToJSON() + ",\"before\":" + before + ",\"during\":" + during + "}";
		}
		if (op == "binary-rejections") {
			Connection reader(database);
			auto view = DuckLakeVortexReadView::AcquireQuery(reader, Options("knn"));
			vector<DuckLakeVortexQuerySource> sources;
			for (auto &source : view.sources) {
				sources.push_back(
				    {source.file_id, reinterpret_cast<const uint8_t *>(source.bytes.data()), source.bytes.size()});
			}
			Require(sources.size() >= 2, "Binary rejection fixture requires two source files");
			vector<float> query {133, 4, 2, 0, 1, 8, 7, 16};
			string name = "knn";
			DuckLakeVortexQueryInput input {reinterpret_cast<const uint8_t *>(view.admission_json.data()),
			                                view.admission_json.size(),
			                                sources.data(),
			                                sources.size(),
			                                reinterpret_cast<const uint8_t *>(view.managed_data_path.data()),
			                                view.managed_data_path.size(),
			                                reinterpret_cast<const uint8_t *>(name.data()),
			                                name.size(),
			                                query.data(),
			                                query.size(),
			                                10,
			                                nullptr,
			                                0,
			                                0,
			                                [](void *, const uint8_t *, uint64_t) -> uint8_t { return 0; },
			                                nullptr};
			auto run = [&](const vector<DuckLakeVortexQuerySource> &candidate, const string &expected) {
				input.sources = candidate.data();
				input.source_count = candidate.size();
				std::unique_ptr<DuckLakeVortexQueryOutput, decltype(&ducklake_vortex_query_free)> output(
				    ducklake_vortex_query(&input), ducklake_vortex_query_free);
				auto error = ducklake_vortex_query_error(output.get());
				Require(error && string(error).find(expected) != string::npos, "Wrong binary handoff rejection");
			};
			run({}, "Admitted source inventory must match every captured file");
			auto changed = sources;
			changed.pop_back();
			run(changed, "inventory");
			changed = sources;
			changed[1] = changed[0];
			run(changed, "duplicate");
			changed = sources;
			changed[0].file_id = 999999;
			run(changed, "Missing admitted source buffer");
			const uint8_t corrupt = 0;
			changed = sources;
			changed[0].data = &corrupt;
			changed[0].size = 1;
			run(changed, "version mismatch");
			changed = sources;
			changed[0].data = nullptr;
			run(changed, "Invalid, duplicate or over-budget");
			changed = sources;
			changed[0].size = 64 * 1024 * 1024 + 1;
			run(changed, "over-budget");
			std::reverse(sources.begin(), sources.end());
			input.sources = sources.data();
			input.source_count = sources.size();
			std::unique_ptr<DuckLakeVortexQueryOutput, decltype(&ducklake_vortex_query_free)> output(
			    ducklake_vortex_query(&input), ducklake_vortex_query_free);
			Require(!ducklake_vortex_query_error(output.get()) && ducklake_vortex_query_count(output.get()) > 0,
			        "Reordered binary buffers did not preserve source identity");
			return "{\"rejections\":7,\"reordered_open\":true}";
		}
		if (op == "search") {
			auto result = SQL(writer, ReadText(args.at(2)));
			Require(result->ColumnCount() == 5, "SQL search must return five typed business/distance columns");
			vector<vector<Value>> rows;
			for (idx_t i = 0; i < result->RowCount(); i++) {
				vector<Value> row;
				for (idx_t col = 0; col < 5; col++) {
					row.push_back(result->GetValue(col, i));
				}
				rows.push_back(std::move(row));
			}
			return SearchJSON(rows, "{}");
		}
		if (op == "timed-core-search" || op == "core-search" || op == "cache-search" || op == "cancel-search" ||
		    op == "fail-search" || op == "held-search" || op == "held-merge" || op == "split-search") {
			Connection query(database);
			DuckLakeVortexSearchRequest request;
			request.query = {133, 4, 2, 0, 1, 8, 7, 16};
			request.k = 10;
			if (op == "cache-search") {
				bool evicted = false;
				auto result =
				    DuckLakeVortexSearch::Execute(*query.context, Options("knn"), request, [&](const string &stage) {
					    if (stage == "after_visibility") {
						    auto &cache = ObjectCache::GetObjectCache(*query.context);
						    auto bytes = cache.GetCurrentMemory();
						    Require(bytes > 0 && cache.EvictToReduceMemory(bytes) > 0,
						            "Schema cache eviction did not run");
						    evicted = true;
					    }
				    });
				Require(evicted, "Cache query missed the visibility boundary");
				return SearchJSON(result.rows, result.diagnostics);
			}
			if (op == "timed-core-search") {
				auto phase = args.at(2);
				auto expected_segments = std::stoull(args.at(3));
				Require(expected_segments > 0 && expected_segments <= DuckLakeVortexIndex::MAX_SEGMENTS,
				        "Invalid timing fixture segment count");
				Require(phase == "ann_us" || phase == "rerank_us", "Unknown timing fixture phase");
				idx_t ann_calls = 0, rerank_calls = 0, observed_delay_us = 0;
				auto result =
				    DuckLakeVortexSearch::Execute(*query.context, Options("knn"), request, [&](const string &stage) {
					    ann_calls += stage == "before_ann";
					    rerank_calls += stage == "segment_searched";
					    if ((phase == "ann_us" && stage == "before_ann") ||
					        (phase == "rerank_us" && stage == "segment_searched")) {
						    auto started = std::chrono::steady_clock::now();
						    std::this_thread::sleep_for(std::chrono::milliseconds(100));
						    observed_delay_us += std::chrono::duration_cast<std::chrono::microseconds>(
						                             std::chrono::steady_clock::now() - started)
						                             .count();
					    }
				    });
				Require(ann_calls == expected_segments && rerank_calls == expected_segments,
				        "Timing fixture did not visit the expected ANN segments");
				ViewJSON json;
				auto value = json.Object();
				json.Text(value, "phase", phase);
				json.Number(value, "ann_calls", ann_calls);
				json.Number(value, "observed_delay_us", observed_delay_us);
				json.Text(value, "query_json", SearchJSON(result.rows, result.diagnostics));
				return json.Finish(value);
			}
			if (op == "core-search") {
				auto result = DuckLakeVortexSearch::Execute(*query.context, Options("knn"), request);
				return SearchJSON(result.rows, result.diagnostics);
			}
			if (op == "cancel-search" || op == "fail-search") {
				auto checkpoint = args.size() > 2 ? args.at(2) : "after_open";
				if (checkpoint == "category_filter") {
					request.filter_category = true;
					request.category = "B";
				}
				DuckLakeVortexSearch::Execute(*query.context, Options("knn"), request, [&](const string &stage) {
					if (stage == checkpoint) {
						if (op == "cancel-search") {
							query.Interrupt();
						} else {
							throw InvalidInputException("Injected query lease failure");
						}
					}
				});
				throw InvalidInputException("Query cancellation/failure injection did not fail");
			}
			Pause pause;
			auto pause_stage = op == "split-search" ? "after_visibility" : "after_open";
			auto pending = std::async(std::launch::async, [&] {
				return DuckLakeVortexSearch::Execute(*query.context, Options("knn"), request, [&](const string &stage) {
					if (stage == pause_stage) {
						pause.Stop();
					}
				});
			});
			ReleasePause release {pause};
			pause.Wait();
			if (op == "split-search") {
				SQL(writer, ReadText(args.at(2)));
			} else {
				Connection publisher(database);
				DuckLakeVortexIndex::Publish(
				    publisher, "lake", "main", "items", "knn", args.at(2), std::stoull(args.at(3)), *publisher.context,
				    nullptr,
				    op == "held-merge" ? DuckLakeVortexIndexOperation::MERGE : DuckLakeVortexIndexOperation::REPLACE,
				    op == "held-merge" ? vector<string> {args.at(6), args.at(7)} : vector<string>());
				SQL(writer, ReadText(args.at(4)));
				FileSystem::GetFileSystem(*writer.context).RemoveDirectory(args.at(5));
			}
			pause.Release();
			auto result = pending.get();
			return SearchJSON(result.rows, result.diagnostics);
		}
		if (op == "raw") {
			return Capture(args.at(2), "");
		}
		if (op == "capture") {
			return Capture(args.at(2), args.at(3));
		}
		if (op == "budget-race") {
			auto count = std::stoull(args.at(2));
			Require(count >= 2 && count <= 16, "Budget race requires two to sixteen writers");
			auto sql = ReadText(args.at(3));
			std::mutex lock;
			std::condition_variable changed;
			idx_t ready = 0;
			bool release = false;
			vector<std::future<string>> writers;
			for (idx_t i = 0; i < count; i++) {
				writers.push_back(std::async(std::launch::async, [&, i] {
					Connection connection(database);
					string error;
					try {
						connection.BeginTransaction();
						SQL(connection, StringUtil::Replace(sql, "{WRITER}", to_string(i)));
					} catch (const std::exception &ex) {
						error = ex.what();
					}
					{
						std::unique_lock<std::mutex> guard(lock);
						ready++;
						changed.notify_all();
						changed.wait(guard, [&] { return release; });
					}
					if (error.empty()) {
						try {
							connection.Commit();
						} catch (const std::exception &ex) {
							error = ex.what();
						}
					}
					return error;
				}));
			}
			{
				std::unique_lock<std::mutex> guard(lock);
				changed.wait(guard, [&] { return ready == count; });
				release = true;
				changed.notify_all();
			}
			ViewJSON json;
			auto rows = json.Array();
			for (auto &task : writers) {
				auto item = json.Object();
				json.Text(item, "error", task.get());
				yyjson_mut_arr_append(rows, item);
			}
			return json.Finish(rows);
		}
		if (op == "cancel-create-lock") {
			Connection caller(database);
			auto &publication = DuckLakeVortexIndex::PublicationLock();
			auto guard = publication.GetSharedLock([] {});
			auto statement = ReadText(args.at(2));
			auto create = std::async(std::launch::async, [&] {
				auto result = caller.Query(statement);
				return result->HasError() ? result->GetError() : string();
			});
			auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
			while (!publication.WaitingWriterCount() && std::chrono::steady_clock::now() < deadline &&
			       create.wait_for(std::chrono::milliseconds(0)) != std::future_status::ready) {
				std::this_thread::yield();
			}
			const bool waiting = publication.WaitingWriterCount() != 0;
			caller.Interrupt();
			const bool cancelled_before_release = create.wait_for(std::chrono::seconds(5)) == std::future_status::ready;
			guard.reset();
			const auto error = create.get();
			Require(waiting && cancelled_before_release && StringUtil::Contains(error, "Interrupt"),
			        "Create did not cancel in the publication lock wait");
			return "{}";
		}
		if (op == "budget-config-interrupt") {
			Connection caller(database);
			auto statement = ReadText(args.at(2));
			auto publication = DuckLakeVortexIndex::PublicationLock().GetExclusiveLock([] {});
			std::promise<void> started;
			auto ready = started.get_future();
			auto configuring = std::async(std::launch::async, [&] {
				started.set_value();
				auto result = caller.Query(statement);
				return result->HasError() ? result->GetError() : string();
			});
			ready.wait();
			auto waited = configuring.wait_for(std::chrono::milliseconds(500)) == std::future_status::timeout;
			if (!waited) {
				publication.reset();
				throw InvalidInputException("Configuration did not wait for the publication guard");
			}
			caller.context->Interrupt();
			auto interrupted = caller.context->IsInterrupted();
			publication.reset();
			auto error = configuring.get();
			ViewJSON json;
			auto value = json.Object();
			json.Text(value, "query_error", error);
			json.Add(value, "waited_for_publication", yyjson_mut_bool(json.doc.get(), waited));
			json.Add(value, "interrupted_before_release", yyjson_mut_bool(json.doc.get(), interrupted));
			return json.Finish(value);
		}
		if (op == "budget-interrupt") {
			Connection connection(database);
			connection.BeginTransaction();
			SQL(connection, ReadText(args.at(2)));
			auto &catalog = Catalog::GetCatalog(*connection.context, "lake");
			connection.context->Interrupt();
			DuckLakeTransaction::Get(*connection.context, catalog).Commit();
			throw InvalidInputException("Interrupted budget admission unexpectedly committed");
		}
		if (op == "concurrent-deletes") {
			Connection first(database), second(database);
			for (auto connection : {&first, &second}) {
				connection->BeginTransaction();
			}
			SQL(first, ReadText(args.at(2)));
			SQL(second, ReadText(args.at(3)));
			first.Commit();
			second.Commit();
			return "{}";
		}
		if (op == "sql") {
			SQL(writer, ReadText(args.at(2)));
			return "{}";
		}
		if (op == "init") {
			SQL(writer, "SELECT * FROM ducklake_initialize_vortex_indexes('lake')");
			return "{}";
		}
		if (op == "publish" || op == "publish-segment") {
			auto function =
			    op == "publish-segment" ? "ducklake_publish_vortex_index_segment" : "ducklake_publish_vortex_index";
			auto result = SQL(writer, "SELECT * FROM " + string(function) + "('lake','main','items'," +
			                              Value(args.at(2)).ToSQLString() + "," + Value(args.at(3)).ToSQLString() +
			                              "," + args.at(4) + ")");
			ViewJSON json;
			auto value = json.Object();
			json.Number(value, "revision", result->GetValue(0, 0).GetValue<idx_t>());
			json.Text(value, "generation", StringValue::Get(result->GetValue(1, 0)));
			return json.Finish(value);
		}
		if (op == "head") {
			return Head(args.at(2)).ToJSON();
		}
		if (op == "cancel-init" || op == "cancel-publish") {
			Connection caller(database);
			auto guard = DuckLakeVortexIndex::PublicationLock().GetSharedLock([] {});
			auto &connections = ConnectionManager::Get(*caller.context);
			auto existing = connections.GetConnectionList();
			auto before = existing.size();
			auto pending = std::async(std::launch::async, [&] {
				if (op == "cancel-init") {
					return caller.Query("SELECT * FROM ducklake_initialize_vortex_indexes('lake')");
				}
				return caller.Query("SELECT * FROM ducklake_publish_vortex_index('lake','main','items'," +
				                    Value(args.at(2)).ToSQLString() + "," + Value(args.at(3)).ToSQLString() + "," +
				                    args.at(4) + ")");
			});
			struct Unlock {
				decltype(guard) &key;
				~Unlock() {
					key.reset();
				}
			} release {guard};
			// The additional owner connection marks entry into ExecuteCatalog.
			// Holding the shared lock keeps its metadata transaction from starting.
			auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
			while (connections.GetConnectionCount() <= before) {
				Require(pending.wait_for(std::chrono::milliseconds(0)) == std::future_status::timeout,
				        "Catalog request exited before creating its owner");
				Require(std::chrono::steady_clock::now() < deadline, "Catalog owner creation timed out");
				std::this_thread::yield();
			}
			Require(pending.wait_for(std::chrono::milliseconds(0)) == std::future_status::timeout,
			        "Catalog request did not wait for publication lock");
			caller.Interrupt();
			guard.reset();
			auto result = pending.get();
			Require(result->HasError() && result->GetErrorType() == ExceptionType::INTERRUPT,
			        "Cancelled catalog request did not report interruption");
			Require(bool(DuckLakeVortexIndex::PublicationLock().TryGetExclusiveLock()),
			        "Cancelled catalog request retained publication lock");
			Require(!caller.context->transaction.HasActiveTransaction(),
			        "Cancelled catalog request retained caller transaction");
			SQL(caller, "SELECT 1");
			return "{}";
		}
		if (op == "cancel-stage") {
			Connection caller(database), owner(database);
			bool interrupted = false;
			try {
				DuckLakeVortexIndex::Publish(owner, "lake", "main", "items", args.at(2), args.at(3),
				                             std::stoull(args.at(4)), *caller.context, [&](const string &stage) {
					                             if (stage == args.at(5)) {
						                             caller.Interrupt();
					                             }
				                             });
			} catch (const InterruptException &) {
				interrupted = true;
			}
			Require(interrupted, "Cancelled publication stage committed");
			Require(!owner.context->transaction.HasActiveTransaction(),
			        "Cancelled publication retained owner transaction");
			Require(bool(DuckLakeVortexIndex::PublicationLock().TryGetExclusiveLock()),
			        "Cancelled publication retained publication lock");
			Require(bool(DuckLakeVortexReadView::AdmissionLock().TryGetExclusiveLock()),
			        "Cancelled publication retained source guard");
			SQL(owner, "SELECT 1");
			return "{}";
		}
		if (op == "old-read") {
			Connection reader(database), publisher(database);
			Pause pause;
			auto pending = std::async(std::launch::async, [&] {
				return DuckLakeVortexReadView::Acquire(reader, Options(args.at(2)), [&](const string &stage) {
					if (stage == "after_snapshot") {
						pause.Stop();
					}
				});
			});
			ReleasePause release {pause};
			pause.Wait();
			auto published = DuckLakeVortexIndex::Publish(publisher, "lake", "main", "items", args.at(2), args.at(3),
			                                              std::stoull(args.at(4)), *publisher.context);
			pause.Release();
			Save(args.at(5), pending.get());
			return published.ToJSON();
		}
		if (op == "race" || op == "race-build" || op == "race-append" || op == "race-merge") {
			std::promise<void> release;
			auto start = release.get_future().share();
			auto publish = [&](const string &path) {
				start.wait();
				Connection owner(database);
				try {
					if (op == "race-build") {
						DuckLakeVortexIndex::BuildAndPublish(*owner.context, "lake", "main", "items", args.at(2), path,
						                                     8, std::stoull(args.at(5)));
					} else {
						DuckLakeVortexIndex::Publish(owner, "lake", "main", "items", args.at(2), path,
						                             std::stoull(args.at(5)), *owner.context, nullptr,
						                             op == "race-merge"    ? DuckLakeVortexIndexOperation::MERGE
						                             : op == "race-append" ? DuckLakeVortexIndexOperation::APPEND
						                                                   : DuckLakeVortexIndexOperation::REPLACE,
						                             op == "race-merge" ? vector<string> {args.at(6), args.at(7)}
						                                                : vector<string>());
					}
					return true;
				} catch (const TransactionException &error) {
					Require(string(error.what()).find("revision conflict") != string::npos,
					        "Unexpected publication conflict");
					return false;
				}
			};
			auto a = std::async(std::launch::async, publish, args.at(3));
			auto b = std::async(std::launch::async, publish, args.at(4));
			release.set_value();
			auto winners = idx_t(a.get()) + idx_t(b.get());
			Require(winners == 1, "Concurrent publication did not produce exactly one winner");
			return Head(args.at(2)).ToJSON();
		}
		if (op == "budget-compaction-publication") {
			SQL(writer, "BEGIN;" + ReadText(args.at(5)));
			Connection owner(database);
			Pause pause;
			auto publish = std::async(std::launch::async, [&] {
				return DuckLakeVortexIndex::BuildAndPublish(*owner.context, "lake", "main", "items", args.at(2),
				                                            args.at(3), 8, std::stoull(args.at(4)),
				                                            [&](const string &stage) {
					                                            if (stage == "after_validation") {
						                                            pause.Stop();
					                                            }
				                                            });
			});
			ReleasePause release {pause};
			pause.Wait();
			std::promise<void> started;
			auto ready = started.get_future();
			auto commit = std::async(std::launch::async, [&] {
				started.set_value();
				try {
					SQL(writer, "COMMIT");
					return string();
				} catch (const std::exception &error) {
					return string(error.what());
				}
			});
			ReleasePause release_commit {pause};
			ready.wait();
			Require(commit.wait_for(std::chrono::milliseconds(500)) == std::future_status::timeout,
			        "Compaction committed inside publication validation");
			pause.Release();
			auto record = publish.get();
			auto failure = commit.get();
			ViewJSON json;
			auto value = json.Object();
			json.Number(value, "revision", record.revision);
			json.Text(value, "commit_error", failure);
			return json.Finish(value);
		}
		if (op == "commit-race") {
			SQL(writer,
			    "BEGIN; INSERT INTO lake.items VALUES(7001,[133.0625,4,2,0,1,8,7,16],'A','after-publication');");
			Connection owner(database);
			Pause pause;
			auto publish = std::async(std::launch::async, [&] {
				return DuckLakeVortexIndex::Publish(owner, "lake", "main", "items", args.at(2), args.at(3),
				                                    std::stoull(args.at(4)), *owner.context, [&](const string &stage) {
					                                    if (stage == "after_validation") {
						                                    pause.Stop();
					                                    }
				                                    });
			});
			ReleasePause release {pause};
			pause.Wait();
			std::promise<void> started;
			auto ready = started.get_future();
			auto commit = std::async(std::launch::async, [&] {
				started.set_value();
				SQL(writer, "COMMIT");
			});
			ReleasePause release_commit {pause};
			ready.wait();
			Require(commit.wait_for(std::chrono::milliseconds(100)) == std::future_status::timeout,
			        "Source commit bypassed publication validation");
			pause.Release();
			auto record = publish.get();
			commit.get();
			return record.ToJSON();
		}
		if (op == "ack" || op == "crash-before" || op == "crash-after") {
			Connection owner(database);
			auto result = DuckLakeVortexIndex::Publish(
			    owner, "lake", "main", "items", args.at(2), args.at(3), std::stoull(args.at(4)), *owner.context,
			    [&](const string &stage) {
				    if (op == "ack" && stage == "after_commit") {
					    throw InvalidInputException("injected lost acknowledgement");
				    }
				    if ((op == "crash-before" && stage == "before_commit") ||
				        (op == "crash-after" && stage == "after_commit")) {
					    Write(root + "/" + op + ".ready", "ready");
					    string line;
					    std::getline(std::cin, line);
					    throw InvalidInputException("Crash fixture resumed without SIGKILL");
				    }
			    });
			return result.ToJSON();
		}
		if (op == "shutdown") {
			return "{}";
		}
		throw InvalidInputException("Unknown native catalog fixture operation");
	}
	void Reply(const string &id, const string &result) {
		Write(root + "/responses/" + id + ".json", "{\"ok\":true,\"result\":" + result + "}");
	}
	void Error(const string &id, const string &error) {
		ViewJSON json;
		auto value = json.Object();
		json.Add(value, "ok", yyjson_mut_bool(json.doc.get(), false));
		json.Text(value, "error", error);
		Write(root + "/responses/" + id + ".json", json.Finish(value));
	}

private:
	string BuildGCJSON(const DuckLakeVortexBuildGCResult &result) {
		ViewJSON json;
		auto value = json.Object();
		json.Number(value, "collected_builds", result.builds);
		json.Number(value, "collected_bytes", result.bytes);
		json.Number(value, "remaining_builds", result.remaining);
		return json.Finish(value);
	}
	string GCJSON(const DuckLakeVortexGCResult &result) {
		ViewJSON json;
		auto value = json.Object();
		json.Number(value, "collected_generations", result.generations);
		json.Number(value, "collected_artifact_bytes", result.artifact_bytes);
		json.Number(value, "remaining_generations", result.remaining);
		return json.Finish(value);
	}
	std::function<void(const string &)> OptimizerObserver(string stage, string mode) {
		if (optimizer_pause) {
			optimizer_pause->Release();
		}
		optimizer_pause = std::make_shared<Pause>();
		auto pause = optimizer_pause;
		auto marker = root + "/optimizer-" + stage + ".ready";
		return [pause, marker, stage, mode](const string &current) {
			if (current != stage) {
				return;
			}
			if (mode == "fail") {
				throw InvalidInputException("Injected optimizer failure");
			}
			Write(marker, "ready");
			pause->Stop();
			if (mode == "fail-after-release") {
				throw InvalidInputException("Injected optimizer failure after checkpoint");
			}
		};
	}
	string SearchJSON(const vector<vector<Value>> &rows, const string &diagnostics) {
		ViewJSON json;
		auto value = json.Object();
		auto items = json.Array();
		json.Add(value, "rows", items);
		auto diagnostic_doc = yyjson_read(diagnostics.data(), diagnostics.size(), 0);
		Require(diagnostic_doc != nullptr, "Invalid query diagnostics");
		json.Add(value, "diagnostics", yyjson_val_mut_copy(json.doc.get(), yyjson_doc_get_root(diagnostic_doc)));
		yyjson_doc_free(diagnostic_doc);
		for (auto &row : rows) {
			auto item = json.Object();
			if (row[0].IsNull()) {
				json.Add(item, "id", yyjson_mut_null(json.doc.get()));
			} else {
				json.Add(item, "id", yyjson_mut_sint(json.doc.get(), row[0].GetValue<int64_t>()));
			}
			auto vector = json.Array();
			for (auto &component : ListValue::GetChildren(row[1])) {
				yyjson_mut_arr_append(vector, yyjson_mut_real(json.doc.get(), component.GetValue<float>()));
			}
			json.Add(item, "embedding", vector);
			json.NullableText(item, "category", row[2]);
			json.NullableText(item, "payload", row[3]);
			json.Add(item, "distance", yyjson_mut_real(json.doc.get(), row[4].GetValue<float>()));
			yyjson_mut_arr_append(items, item);
		}
		return json.Finish(value);
	}
	DuckLakeVortexViewOptions Options(const string &name) {
		return {"lake", "main", "items", 0, 64 * 1024 * 1024, 100000, name};
	}
	void Save(const string &name, const DuckLakeVortexReadView &view) {
		Write(root + "/" + name + ".admitted.json", view.admission_json);
		Write(root + "/" + name + ".rows.json", view.rows_json);
	}
	DuckLakeVortexIndex Head(const string &name) {
		Connection reader(database);
		reader.BeginTransaction();
		auto &catalog = Catalog::GetCatalog(*reader.context, "lake").Cast<DuckLakeCatalog>();
		auto &transaction = DuckLakeTransaction::Get(*reader.context, catalog);
		transaction.GetSnapshot();
		auto entry = catalog.GetEntry(*reader.context, CatalogType::TABLE_ENTRY, "main", "items",
		                              OnEntryNotFound::THROW_EXCEPTION);
		auto result = DuckLakeVortexIndex::Lookup(transaction, entry->Cast<DuckLakeTableEntry>(), name);
		reader.Commit();
		return result;
	}
	string root;
	DuckDB database;
	Connection writer;
	std::shared_ptr<Pause> optimizer_pause;
};

void CloseFixture(unique_ptr<Fixture> &fixture) {
	auto weak = fixture->WeakDatabase();
	fixture.reset();
	auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
	while (!weak.expired() && std::chrono::steady_clock::now() < deadline) {
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}
	Require(weak.expired(), "Native catalog fixture retained database resources after close");
}
} // namespace

int main(int argc, char **argv) {
	try {
		Require(argc == 3, "usage: native_vortex_catalog_driver ROOT create|resume");
		Require(string(argv[2]) == "create" || string(argv[2]) == "resume", "Unknown fixture mode");
		auto fixture = make_uniq<Fixture>(argv[1], string(argv[2]) == "create");
		string line;
		while (std::getline(std::cin, line)) {
			auto args = StringUtil::Split(line, '\t');
			Require(args.size() >= 2, "Invalid fixture command");
			try {
				string result;
				if (args[1] == "reopen") {
					CloseFixture(fixture);
					fixture = make_uniq<Fixture>(argv[1], false);
					result = "{}";
				} else {
					result = fixture->Execute(args);
				}
				fixture->Reply(args[0], result);
			} catch (const std::exception &error) {
				if (!fixture) {
					throw;
				}
				fixture->Error(args[0], error.what());
			}
			if (args[1] == "shutdown") {
				break;
			}
		}
		CloseFixture(fixture);
		return 0;
	} catch (const std::exception &error) {
		std::cerr << error.what() << std::endl;
		return 1;
	}
}
