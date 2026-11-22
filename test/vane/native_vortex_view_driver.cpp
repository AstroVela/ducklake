//! Native multi-connection fixture; the Python harness queries the retained Rust views.

#include "duckdb.hpp"
#include "duckdb/common/file_system.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_vortex_read_view.hpp"
#include "duckdb/storage/storage_lock.hpp"

#include <chrono>
#include <condition_variable>
#include <fstream>
#include <future>
#include <iostream>
#include <mutex>

using namespace duckdb;

namespace {

void Require(bool condition, const string &message) {
	if (!condition) {
		throw InvalidInputException(message);
	}
}

unique_ptr<MaterializedQueryResult> SQL(Connection &connection, const string &text) {
	auto result = connection.Query(text);
	if (result->HasError()) {
		result->ThrowError();
	}
	return result;
}

void Write(const string &path, const string &text) {
	std::ofstream stream(path + ".pending", std::ios::binary);
	stream << text;
	stream.close();
	Require(bool(stream), "Cannot write native view evidence");
	Require(std::rename((path + ".pending").c_str(), path.c_str()) == 0, "Cannot publish native view evidence");
}

struct Pause {
	std::mutex lock;
	std::condition_variable changed;
	bool reached = false;
	bool released = false;

	void Stop() {
		std::unique_lock<std::mutex> guard(lock);
		if (reached) {
			return;
		}
		reached = true;
		changed.notify_all();
		Require(changed.wait_for(guard, std::chrono::seconds(30), [&] { return released; }), "Pause release timed out");
	}
	void Wait() {
		std::unique_lock<std::mutex> guard(lock);
		Require(changed.wait_for(guard, std::chrono::seconds(15), [&] { return reached; }),
		        "Admission stage timed out");
	}
	void Release() {
		std::lock_guard<std::mutex> guard(lock);
		released = true;
		changed.notify_all();
	}
};

struct ReleasePause {
	Pause &pause;
	~ReleasePause() {
		pause.Release();
	}
};

class Fixture {
public:
	explicit Fixture(const string &root_p) : root(root_p), database(nullptr), writer(database), gc(database) {
		auto &fs = FileSystem::GetFileSystem(*writer.context);
		Require(fs.IsPathAbsolute(root), "Native fixture root must be absolute");
		Require(!fs.DirectoryExists(root), "Native fixture root already exists");
		fs.CreateDirectory(root);
		SQL(writer, "SET autoinstall_known_extensions=false; SET autoload_known_extensions=false;"
		            "LOAD parquet; LOAD ducklake; LOAD vortex; SET threads=2; ATTACH " +
		                Value("ducklake:" + root + "/metadata.ducklake").ToSQLString() + " AS lake (DATA_PATH " +
		                Value(root + "/data").ToSQLString() + ", DATA_INLINING_ROW_LIMIT 0);");
		SQL(writer, "CREATE TABLE lake.items(id BIGINT NOT NULL,embedding FLOAT[],category VARCHAR,payload VARCHAR);"
		            "CALL ducklake_set_option('lake','experimental_data_file_format','vortex',table_name=>'items');");
		for (idx_t parity = 0; parity < 2; parity++) {
			SQL(writer, "INSERT INTO lake.items SELECT id,[id::FLOAT,((id*4)%12)::FLOAT,((id*5)%13)::FLOAT,"
			            "((id*6)%14)::FLOAT,((id*7)%15)::FLOAT,((id*8)%16)::FLOAT,((id*9)%17)::FLOAT,"
			            "((id*10)%18)::FLOAT],CASE WHEN id%4=0 THEN 'B' ELSE 'A' END,'中文-row-'||id "
			            "FROM (SELECT i*2+" +
			                to_string(parity) + " AS id FROM range(2048)t(i));");
		}
		Connection reader(database);
		auto result = SQL(reader, "SELECT * FROM ducklake_vortex_read_view('lake','main','items',8,67108864,100000)");
		DuckLakeVortexReadView initial;
		initial.snapshot_id = result->GetValue(0, 0).GetValue<idx_t>();
		initial.source_bytes = result->GetValue(1, 0).GetValue<idx_t>();
		initial.visible_rows = result->GetValue(2, 0).GetValue<idx_t>();
		initial.inline_rows = result->GetValue(3, 0).GetValue<idx_t>();
		initial.admission_json = StringValue::Get(result->GetValue(4, 0));
		initial.rows_json = StringValue::Get(result->GetValue(5, 0));
		Keep("initial", std::move(initial));
		Phase("bootstrap");
	}

	void Advance() {
		SQL(writer, "CALL ducklake_set_option('lake','data_inlining_row_limit',100,table_name=>'items');"
		            "INSERT INTO lake.items SELECT 5000+i,CASE WHEN i=0 THEN [133.25,4,2,0,1,8,7,16] "
		            "WHEN i=1 THEN [133,4,2,0,1,8,7,16] ELSE [16000+i,0,0,0,0,0,0,0] END,'B','未索引-'||i "
		            "FROM range(128)t(i);");
		Connection reader(database);
		Pause pause;
		auto pending = std::async(std::launch::async, [&] {
			return DuckLakeVortexReadView::Acquire(reader, Options(), [&](const string &stage) {
				if (stage == "after_snapshot") {
					pause.Stop();
				}
			});
		});
		ReleasePause release {pause};
		pause.Wait();
		SQL(writer,
		    "BEGIN; UPDATE lake.items SET embedding=[7000,0,0,0,0,0,0,0],category='B',payload=NULL WHERE id=133;"
		    "DELETE FROM lake.items WHERE id IN (134,5001);"
		    "INSERT INTO lake.items VALUES(6000,[133.125,4,2,0,1,8,7,16],'A','inline-old'),"
		    "(6001,[133,4,2,0,1,8,7,16],'B','inline-deleted'); COMMIT;"
		    "BEGIN; UPDATE lake.items SET payload='内联-live' WHERE id=6000;"
		    "DELETE FROM lake.items WHERE id=6001; COMMIT;");
		pause.Release();
		Keep("during", pending.get());
		Keep("current", DuckLakeVortexReadView::Acquire(reader, Options()));
		Require(views.at("during").snapshot_id < views.at("current").snapshot_id,
		        "Concurrent commit did not advance the native snapshot");
		Require(views.at("during").inline_rows == 0 && views.at("current").inline_rows == 2,
		        "Native admission mixed old and new inline visibility");
		Phase("advance");
	}

	void Maintain() {
		Connection reader(database);
		Pause pause;
		auto pending = std::async(std::launch::async, [&] {
			return DuckLakeVortexReadView::Acquire(reader, Options(), [&](const string &stage) {
				if (stage == "after_inventory") {
					pause.Stop();
				}
			});
		});
		ReleasePause release {pause};
		pause.Wait();
		SQL(writer, "CALL ducklake_flush_inlined_data('lake');"
		            "CALL ducklake_rewrite_data_files('lake',delete_threshold=>0);"
		            "CALL ducklake_merge_adjacent_files('lake');"
		            "SELECT count(*) FROM ducklake_expire_snapshots('lake',older_than=>now());");
		std::mutex event_lock;
		std::condition_variable event;
		bool enumerated = false;
		writer.BeginTransaction();
		auto &catalog = Catalog::GetCatalog(*writer.context, "lake").Cast<DuckLakeCatalog>();
		writer.Commit();
		catalog.SetQueryCallback([&](const string &query, std::chrono::steady_clock::duration) {
			if (query.find("ducklake_files_scheduled_for_deletion") != string::npos) {
				std::lock_guard<std::mutex> guard(event_lock);
				enumerated = true;
				event.notify_all();
			}
		});
		auto cleanup = std::async(std::launch::async, [&] {
			return SQL(gc, "SELECT count(*) FROM ducklake_cleanup_old_files('lake',cleanup_all=>true)");
		});
		ReleasePause release_cleanup {pause};
		{
			std::unique_lock<std::mutex> guard(event_lock);
			Require(event.wait_for(guard, std::chrono::seconds(15), [&] { return enumerated; }),
			        "Concurrent cleanup did not enumerate retired files");
		}
		Require(cleanup.wait_for(std::chrono::milliseconds(100)) == std::future_status::timeout,
		        "Physical cleanup bypassed native read view admission");
		pause.Release();
		Keep("window", pending.get());
		auto cleaned = cleanup.get();
		catalog.SetQueryCallback(nullptr);
		Require(cleaned->GetValue(0, 0).GetValue<idx_t>() >= 3, "Concurrent cleanup did not reclaim native inputs");
		Keep("latest", DuckLakeVortexReadView::Acquire(reader, Options()));
		Phase("maintain");
	}

	void Failures() {
		Connection reader(database);
		auto reject = [&](const DuckLakeVortexViewOptions &options, const std::function<void(const string &)> &observe,
		                  const string &expected) {
			try {
				auto unused = DuckLakeVortexReadView::Acquire(reader, options, observe);
				throw InternalException("Expected native admission failure");
			} catch (const InvalidInputException &error) {
				Require(string(error.what()).find(expected) != string::npos, "Wrong native admission error");
				rejections++;
			}
			Require(SQL(reader, "SELECT 1")->GetValue(0, 0).GetValue<int32_t>() == 1,
			        "Failed admission left a transaction on its connection");
			Require(bool(DuckLakeVortexReadView::AdmissionLock().TryGetExclusiveLock()),
			        "Failed admission retained its source guard");
		};
		auto bytes = Options();
		bytes.max_source_bytes = 1;
		reject(bytes, nullptr, "byte budget");
		auto rows = Options();
		rows.max_rows = 1;
		reject(rows, nullptr, "row budget");
		reject(
		    Options(),
		    [&](const string &stage) {
			    if (stage == "after_pin") {
				    throw InvalidInputException("injected partial pin failure");
			    }
		    },
		    "partial pin failure");
		Pause pause;
		auto pending = std::async(std::launch::async, [&] {
			return DuckLakeVortexReadView::Acquire(reader, Options(), [&](const string &stage) {
				if (stage == "after_pin") {
					pause.Stop();
				}
			});
		});
		ReleasePause release {pause};
		pause.Wait();
		reader.Interrupt();
		pause.Release();
		try {
			auto unused = pending.get();
			throw InternalException("Cancelled native admission succeeded");
		} catch (const InterruptException &) {
			rejections++;
		}
		Require(SQL(reader, "SELECT 1")->GetValue(0, 0).GetValue<int32_t>() == 1,
		        "Cancellation retained a transaction");
		Require(bool(DuckLakeVortexReadView::AdmissionLock().TryGetExclusiveLock()),
		        "Cancellation retained its source guard");
		SQL(writer, "BEGIN; INSERT INTO lake.items VALUES(9999,[1,0,0,0,0,0,0,0],'A','uncommitted');");
		Keep("uncommitted", DuckLakeVortexReadView::Acquire(reader, Options()));
		SQL(writer, "ROLLBACK");
		Require(views.at("uncommitted").visible_rows == views.at("latest").visible_rows,
		        "Native acquisition exposed another connection's uncommitted rows");
		Keep("after-failure", DuckLakeVortexReadView::Acquire(reader, Options()));
		Phase("failures");
	}

	void Drop() {
		views.clear();
		Phase("drop");
	}

private:
	DuckLakeVortexViewOptions Options() const {
		return {"lake", "main", "items", 8, 64 * 1024 * 1024, 100000};
	}
	void Keep(const string &name, DuckLakeVortexReadView view) {
		Write(root + "/" + name + ".admitted.json", view.admission_json);
		Write(root + "/" + name + ".rows.json", view.rows_json);
		views.emplace(name, std::move(view));
	}
	void Phase(const string &phase) {
		idx_t bytes = 0;
		for (auto &entry : views) {
			bytes += entry.second.source_bytes;
		}
		Write(root + "/" + phase + ".ready.json", "{\"ok\":true,\"active_views\":" + to_string(views.size()) +
		                                              ",\"source_bytes\":" + to_string(bytes) +
		                                              ",\"native_rejections\":" + to_string(rejections) + "}");
	}
	string root;
	DuckDB database;
	Connection writer;
	Connection gc;
	map<string, DuckLakeVortexReadView> views;
	idx_t rejections = 0;
};

} // namespace

int main(int argc, char **argv) {
	try {
		Require(argc == 2, "usage: native_vortex_view_driver ROOT");
		Fixture fixture(argv[1]);
		string command;
		while (std::getline(std::cin, command)) {
			if (command == "advance") {
				fixture.Advance();
			} else if (command == "maintain") {
				fixture.Maintain();
			} else if (command == "failures") {
				fixture.Failures();
			} else if (command == "drop") {
				fixture.Drop();
			} else if (command == "shutdown") {
				fixture.Drop();
				return 0;
			} else {
				throw InvalidInputException("Unknown native fixture command");
			}
		}
		fixture.Drop();
		return 0;
	} catch (const std::exception &error) {
		std::cerr << error.what() << std::endl;
		return 1;
	}
}
