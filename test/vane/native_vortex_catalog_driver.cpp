//! Persistent native catalog fixture; commands and replies are owner-local test files.
#include "native_vortex_fixture.hpp"
#include "common/ducklake_vortex_json.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/connection_manager.hpp"
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

using namespace duckdb;
using namespace duckdb::vortex_test;
using namespace duckdb::vortex_detail;

namespace {
string ReadText(const string &path) {
	std::ifstream stream(path, std::ios::binary);
	Require(bool(stream), "Cannot read native command file");
	return string(std::istreambuf_iterator<char>(stream), std::istreambuf_iterator<char>());
}

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

	string Execute(const vector<string> &args) {
		const auto &op = args.at(1);
		if (op == "owners") {
			ViewJSON json;
			auto value = json.Object();
			json.Number(value, "database_references", database.instance.use_count());
			auto entries = json.Array();
			for (auto &context : database.instance->GetConnectionManager().GetConnectionList()) {
				auto item = json.Object();
				json.Number(item, "connection_id", context->GetConnectionId());
				json.Number(item, "references", context.use_count());
				json.Add(item, "active", yyjson_mut_bool(json.doc.get(), context->transaction.HasActiveTransaction()));
				yyjson_mut_arr_append(entries, item);
			}
			json.Add(value, "connections", entries);
			return json.Finish(value);
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
		if (op == "optimizer-status") {
			auto result = SQL(writer, "SELECT * FROM ducklake_vortex_optimizer_status('lake')");
			ViewJSON json;
			auto rows = json.Array();
			for (idx_t row = 0; row < result->RowCount(); row++) {
				auto value = json.Object();
				for (idx_t col = 0; col < result->ColumnCount(); col++) {
					auto item = result->GetValue(col, row);
					if (item.type().id() == LogicalTypeId::BOOLEAN) {
						json.Add(value, result->names[col].c_str(),
						         yyjson_mut_bool(json.doc.get(), item.GetValue<bool>()));
					} else if (item.type().id() == LogicalTypeId::UBIGINT) {
						json.Number(value, result->names[col].c_str(), item.GetValue<idx_t>());
					} else {
						json.Text(value, result->names[col].c_str(), StringValue::Get(item));
					}
				}
				yyjson_mut_arr_append(rows, value);
			}
			return json.Finish(rows);
		}
		if (op == "build") {
			auto result = SQL(writer, "SELECT * FROM ducklake_build_vortex_index('lake','main','items'," +
			                              Value(args.at(2)).ToSQLString() + "," + Value(args.at(3)).ToSQLString() +
			                              ",8," + args.at(4) + ")");
			ViewJSON json;
			auto value = json.Object();
			json.Number(value, "revision", result->GetValue(0, 0).GetValue<idx_t>());
			json.Text(value, "generation", StringValue::Get(result->GetValue(1, 0)));
			return json.Finish(value);
		}
		if (op == "cancel-build" || op == "fail-build" || op == "crash-build") {
			Connection builder(database);
			return DuckLakeVortexIndex::BuildAndPublish(
			           *builder.context, "lake", "main", "items", "knn", args.at(3), 8, std::stoull(args.at(4)),
			           [&](const string &stage) {
				           if (stage != args.at(2)) {
					           return;
				           }
				           if (op == "cancel-build") {
					           builder.Interrupt();
				           } else if (op == "fail-build") {
					           throw InvalidInputException("Injected native build failure");
				           } else {
					           Write(root + "/build-" + stage + ".ready", "ready");
					           string line;
					           std::getline(std::cin, line);
					           throw InvalidInputException("Build crash fixture resumed without SIGKILL");
				           }
			           })
			    .ToJSON();
		}
		if (op == "split-build" || op == "mutate-build") {
			Connection builder(database);
			Pause pause;
			auto pending = std::async(std::launch::async, [&] {
				return DuckLakeVortexIndex::BuildAndPublish(*builder.context, "lake", "main", "items", "knn",
				                                            args.at(2), 8, std::stoull(args.at(3)),
				                                            [&](const string &stage) {
					                                            if (stage == args.at(4)) {
						                                            pause.Stop();
					                                            }
				                                            });
			});
			ReleasePause release {pause};
			pause.Wait();
			DuckLakeVortexSearchRequest request;
			request.query = {133, 4, 2, 0, 1, 8, 7, 16};
			request.k = 10;
			string before;
			if (op == "split-build") {
				auto rows = DuckLakeVortexSearch::Execute(*writer.context, Options("knn"), request);
				before = SearchJSON(rows.rows, rows.diagnostics);
			}
			SQL(writer, ReadText(args.at(5)));
			string during;
			if (op == "split-build") {
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
			run({}, "Invalid native binary source inventory");
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
		if (op == "core-search" || op == "cancel-search" || op == "fail-search" || op == "held-search" ||
		    op == "split-search") {
			Connection query(database);
			DuckLakeVortexSearchRequest request;
			request.query = {133, 4, 2, 0, 1, 8, 7, 16};
			request.k = 10;
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
				DuckLakeVortexIndex::Publish(publisher, "lake", "main", "items", "knn", args.at(2),
				                             std::stoull(args.at(3)));
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
		if (op == "sql") {
			SQL(writer, ReadText(args.at(2)));
			return "{}";
		}
		if (op == "init") {
			SQL(writer, "SELECT * FROM ducklake_initialize_vortex_indexes('lake')");
			return "{}";
		}
		if (op == "publish") {
			auto result = SQL(writer, "SELECT * FROM ducklake_publish_vortex_index('lake','main','items'," +
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
			                                              std::stoull(args.at(4)));
			pause.Release();
			Save(args.at(5), pending.get());
			return published.ToJSON();
		}
		if (op == "race" || op == "race-build") {
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
						                             std::stoull(args.at(5)));
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
		if (op == "commit-race") {
			SQL(writer,
			    "BEGIN; INSERT INTO lake.items VALUES(7001,[133.0625,4,2,0,1,8,7,16],'A','after-publication');");
			Connection owner(database);
			Pause pause;
			auto publish = std::async(std::launch::async, [&] {
				return DuckLakeVortexIndex::Publish(owner, "lake", "main", "items", args.at(2), args.at(3),
				                                    std::stoull(args.at(4)), [&](const string &stage) {
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
			    owner, "lake", "main", "items", args.at(2), args.at(3), std::stoull(args.at(4)),
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
