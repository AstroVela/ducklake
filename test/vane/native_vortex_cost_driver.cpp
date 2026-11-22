//! Isolated-process measurements of the existing native execution factory.
#include "native_vortex_fixture.hpp"
#include "common/ducklake_vortex_json.hpp"
#include "storage/ducklake_vortex_search.hpp"
#include <sys/resource.h>
#include <sstream>

using namespace duckdb;
using namespace duckdb::vortex_test;
using namespace duckdb::vortex_detail;

namespace {
using Clock = std::chrono::steady_clock;
void Number(ViewJSON &json, duckdb_yyjson::yyjson_mut_val *parent, const string &key, uint64_t value) {
	using namespace duckdb_yyjson;
	yyjson_mut_obj_add(parent, yyjson_mut_strcpy(json.doc.get(), key.c_str()), yyjson_mut_uint(json.doc.get(), value));
}
map<string, uint64_t> ReadCounters(const string &path) {
	std::ifstream stream(path);
	map<string, uint64_t> result;
	string line;
	while (std::getline(stream, line)) {
		std::istringstream input(line);
		string key;
		uint64_t number;
		if (input >> key >> number) {
			result[key] = number;
		}
	}
	return result;
}
void AddJSON(ViewJSON &json, duckdb_yyjson::yyjson_mut_val *parent, const char *key, const string &bytes) {
	using namespace duckdb_yyjson;
	std::unique_ptr<yyjson_doc, decltype(&yyjson_doc_free)> parsed(yyjson_read(bytes.data(), bytes.size(), 0),
	                                                               yyjson_doc_free);
	Require(bool(parsed), "Invalid measurement diagnostics");
	json.Add(parent, key, yyjson_val_mut_copy(json.doc.get(), yyjson_doc_get_root(parsed.get())));
}
} // namespace

int main(int argc, char **argv) {
	try {
		Require(argc == 5, "usage: native_vortex_cost_driver ROOT OUTPUT REPEATS CATEGORY|ALL");
		auto repeats = std::stoull(argv[3]);
		Require(repeats >= 3 && repeats <= 100, "Invalid benchmark repetition count");
		DuckDB db(nullptr);
		Connection connection(db);
		Attach(connection, argv[1]);
		DuckLakeVortexViewOptions options {"lake", "main", "items", 0, 64 * 1024 * 1024, 100000, "knn"};
		DuckLakeVortexSearchRequest request;
		request.query = {133, 4, 2, 0, 1, 8, 7, 16};
		request.k = 10;
		request.filter_category = string(argv[4]) != "ALL";
		request.category = request.filter_category ? argv[4] : "";
		ViewJSON json;
		auto root = json.Object();
		auto samples = json.Array();
		json.Add(root, "samples", samples);
		for (idx_t i = 0; i < repeats; i++) {
			auto io_before = ReadCounters("/proc/self/io");
			auto rss_before = ReadCounters("/proc/self/status");
			rusage usage_before, usage_after;
			Require(getrusage(RUSAGE_SELF, &usage_before) == 0, "Cannot read process usage");
			auto started = Clock::now();
			map<string, uint64_t> events;
			auto result =
			    DuckLakeVortexSearch::Execute(*connection.context, options, request, [&](const string &stage) {
				    if (stage != "row" && stage != "inline_row" && stage != "category_filter" &&
				        stage != "scan_batch") {
					    events[stage] =
					        std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - started).count();
				    }
			    });
			auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - started).count();
			Require(getrusage(RUSAGE_SELF, &usage_after) == 0, "Cannot read process usage");
			auto io_after = ReadCounters("/proc/self/io");
			auto rss_after = ReadCounters("/proc/self/status");
			auto sample = json.Object();
			json.Number(sample, "elapsed_us", elapsed);
			json.Number(sample, "peak_rss_kib", usage_after.ru_maxrss);
			json.Number(sample, "rss_before_kib", rss_before.at("VmRSS:"));
			json.Number(sample, "rss_after_kib", rss_after.at("VmRSS:"));
			json.Number(sample, "minor_faults", usage_after.ru_minflt - usage_before.ru_minflt);
			json.Number(sample, "major_faults", usage_after.ru_majflt - usage_before.ru_majflt);
			for (auto &key : {"rchar:", "wchar:", "read_bytes:", "write_bytes:"}) {
				auto label = string(key);
				label.pop_back();
				Number(json, sample, label, io_after.at(key) - io_before.at(key));
			}
			auto phases = json.Object();
			for (auto &event : events) {
				Number(json, phases, event.first, event.second);
			}
			json.Add(sample, "event_us", phases);
			AddJSON(json, sample, "diagnostics", result.diagnostics);
			auto hits = json.Array();
			for (auto &row : result.rows) {
				auto hit = json.Object();
				json.Add(hit, "id", yyjson_mut_sint(json.doc.get(), row[0].GetValue<int64_t>()));
				json.Add(hit, "distance", yyjson_mut_real(json.doc.get(), row[4].GetValue<float>()));
				json.NullableText(hit, "category", row[2]);
				json.Number(hit, "payload_bytes", StringValue::Get(row[3]).size());
				yyjson_mut_arr_append(hits, hit);
			}
			json.Add(sample, "hits", hits);
			yyjson_mut_arr_append(samples, sample);
		}
		Write(argv[2], json.Finish(root));
		return 0;
	} catch (const std::exception &error) {
		std::cerr << error.what() << '\n';
		return 1;
	}
}
