#pragma once

#include "duckdb.hpp"
#include <chrono>
#include <condition_variable>
#include <fstream>
#include <future>
#include <mutex>

namespace duckdb {
namespace vortex_test {
inline void Require(bool condition, const string &message) {
	if (!condition) {
		throw InvalidInputException(message);
	}
}

inline unique_ptr<MaterializedQueryResult> SQL(Connection &connection, const string &text) {
	auto result = connection.Query(text);
	if (result->HasError()) {
		result->ThrowError();
	}
	return result;
}

inline void Write(const string &path, const string &text) {
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

inline void Attach(Connection &writer, const string &root) {
	SQL(writer, "SET autoinstall_known_extensions=false; SET autoload_known_extensions=false;"
	            "LOAD parquet; LOAD ducklake; LOAD vortex; SET threads=2; ATTACH " +
	                Value("ducklake:" + root + "/metadata.ducklake").ToSQLString() + " AS lake (DATA_PATH " +
	                Value(root + "/data").ToSQLString() + ", DATA_INLINING_ROW_LIMIT 0);");
}
inline void CreateItems(Connection &writer) {
	SQL(writer, "CREATE TABLE lake.items(id BIGINT NOT NULL,embedding FLOAT[],category VARCHAR,payload VARCHAR);"
	            "CALL ducklake_set_option('lake','experimental_data_file_format','vortex',table_name=>'items');");
	for (idx_t parity = 0; parity < 2; parity++) {
		SQL(writer, "INSERT INTO lake.items SELECT id,[id::FLOAT,((id*4)%12)::FLOAT,((id*5)%13)::FLOAT,"
		            "((id*6)%14)::FLOAT,((id*7)%15)::FLOAT,((id*8)%16)::FLOAT,((id*9)%17)::FLOAT,"
		            "((id*10)%18)::FLOAT],CASE WHEN id%4=0 THEN 'B' ELSE 'A' END,'中文-row-'||id "
		            "FROM (SELECT i*2+" +
		                to_string(parity) + " AS id FROM range(2048)t(i));");
	}
}
} // namespace vortex_test
} // namespace duckdb
