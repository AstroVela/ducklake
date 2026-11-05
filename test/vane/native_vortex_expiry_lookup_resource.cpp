#include "common/ducklake_snapshot_lookup.hpp"
#include "common/ducklake_vortex_json.hpp"
#include "duckdb.hpp"
#include "duckdb/main/materialized_query_result.hpp"

#include <fstream>
#include <iostream>
#include <stdexcept>
#include <sys/resource.h>
#include <unistd.h>

using namespace duckdb;
using namespace duckdb::vortex_detail;

namespace {
idx_t CurrentRSS() {
	idx_t virtual_pages, resident_pages;
	std::ifstream status("/proc/self/statm");
	if (!(status >> virtual_pages >> resident_pages)) {
		throw std::runtime_error("Cannot read current RSS");
	}
	return resident_pages * idx_t(sysconf(_SC_PAGESIZE));
}
idx_t PeakRSS() {
	struct rusage usage;
	if (getrusage(RUSAGE_SELF, &usage) != 0) {
		throw std::runtime_error("getrusage failed");
	}
	return idx_t(usage.ru_maxrss) * 1024;
}
unique_ptr<MaterializedQueryResult> Query(Connection &connection, const string &query) {
	auto result = connection.Query(query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Resource query failed [" + query + "]: ");
	}
	return result;
}
void ReadCost(ViewJSON &json, yyjson_mut_val *report, const char *name, MaterializedQueryResult &result) {
	auto value = json.Object();
	json.Number(value, "rows", result.RowCount());
	json.Number(value, "columns", result.ColumnCount());
	json.Number(value, "collection_bytes", result.Collection().SizeInBytes());
	json.Number(value, "allocation_bytes", result.Collection().AllocationSize());
	json.Add(report, name, value);
}
} // namespace

// Test-only metadata accounting; neither a worker nor the complete collector is timed here.
int main(int argc, char **argv) {
	try {
		if (argc != 4 || (string(argv[2]) != "baseline" && string(argv[2]) != "lookup")) {
			throw std::runtime_error("Usage: lookup_resource COPIED_CATALOG baseline|lookup OUTPUT");
		}
		DuckDB database(argv[1]);
		Connection connection(database);
		Query(connection, "SET threads=2; BEGIN TRANSACTION");
		Query(connection, "DELETE FROM ducklake_snapshot WHERE snapshot_id IN "
		                  "(SELECT snapshot_id FROM ducklake_snapshot ORDER BY snapshot_id LIMIT 2)");
		ViewJSON json;
		auto report = json.Object();
		json.Text(report, "variant", argv[2]);
		json.Number(report, "peak_rss_before_bytes", PeakRSS());
		json.Number(report, "rss_before_bytes", CurrentRSS());
		vector<int64_t> snapshots;
		if (string(argv[2]) == "lookup") {
			auto result = Query(connection, "SELECT snapshot_id FROM ducklake_snapshot ORDER BY snapshot_id");
			ReadCost(json, report, "retained_snapshots", *result);
			json.Number(report, "rss_with_snapshot_result_bytes", CurrentRSS());
			for (auto &row : *result) {
				snapshots.push_back(row.GetValue<int64_t>(0));
			}
		}
		json.Number(report, "snapshot_vector_bytes", snapshots.size() * sizeof(int64_t));
		json.Number(report, "snapshot_vector_capacity_bytes", snapshots.capacity() * sizeof(int64_t));
		auto files = Query(connection, string(argv[2]) == "lookup"
		                                   ? "SELECT data_file_id, table_id, path, path_is_relative, begin_snapshot, "
		                                     "end_snapshot FROM ducklake_data_file WHERE end_snapshot IS NOT NULL"
		                                   : "SELECT data_file_id, table_id, path, path_is_relative FROM "
		                                     "ducklake_data_file WHERE end_snapshot IS NOT NULL AND NOT EXISTS "
		                                     "(SELECT snapshot_id FROM ducklake_snapshot WHERE snapshot_id >= "
		                                     "begin_snapshot AND snapshot_id < end_snapshot)");
		ReadCost(json, report, "data_files", *files);
		json.Number(report, "rss_with_file_result_bytes", CurrentRSS());
		idx_t eligible = 0;
		for (auto &row : *files) {
			if (string(argv[2]) == "baseline" ||
			    DuckLakeFileCanExpire(snapshots, row.GetBaseValue(4), row.GetBaseValue(5), false)) {
				eligible++;
			}
		}
		json.Number(report, "eligible_files", eligible);
		json.Number(report, "peak_rss_after_bytes", PeakRSS());
		json.Number(report, "rss_after_bytes", CurrentRSS());
		Query(connection, "ROLLBACK");
		std::ofstream output(argv[3]);
		output.exceptions(std::ios::badbit | std::ios::failbit);
		output << json.Finish(report) << '\n';
		return 0;
	} catch (const std::exception &error) {
		std::cerr << error.what() << std::endl;
		return 1;
	}
}
