#pragma once

#include "common/ducklake_vortex_json.hpp"

#include <chrono>
#include <exception>
#include <stdexcept>

// Included only by the private, patched metadata translation unit and diagnostic driver.
namespace duckdb {
namespace vortex_test {
namespace reference_trace {
using namespace duckdb_yyjson;
inline uint64_t Now() {
	return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
	    .count();
}

struct Sample {
	string path;
	idx_t connection = 0, rows = 0, chunks = 0, fetch_calls = 0;
	uint64_t begin = 0, end = 0, fetch_ns = 0, path_loop_ns = 0;
	bool found = false, completed = false;
};
struct Capture {
	bool active = false;
	vector<Sample> samples;
};
inline Capture &Get() {
	static thread_local Capture capture;
	return capture;
}

inline void Start() {
	auto &capture = Get();
	if (capture.active) {
		throw std::logic_error("Reference capture is already active");
	}
	capture.samples.clear();
	capture.samples.reserve(16);
	capture.active = true;
}
inline void Stop() {
	auto &capture = Get();
	if (!capture.active) {
		throw std::logic_error("Reference capture is not active");
	}
	capture.active = false;
}

struct Scope {
	Sample sample;
	explicit Scope(const string &path) {
		auto begin = Now();
		auto &capture = Get();
		if (!capture.active || capture.samples.size() >= 16) {
			throw std::logic_error("Reference call outside the bounded diagnostic capture");
		}
		sample.path = path;
		sample.begin = begin;
	}
	~Scope() {
		sample.end = Now();
		sample.completed = !std::uncaught_exception();
		Get().samples.push_back(std::move(sample));
	}
	void Fetch(uint64_t begin, bool chunk) {
		sample.fetch_ns += Now() - begin;
		sample.fetch_calls++;
		sample.chunks += chunk ? 1 : 0;
	}
	void PathLoop(uint64_t begin, idx_t rows) {
		sample.path_loop_ns += Now() - begin;
		sample.rows += rows;
	}
};

inline void Add(vortex_detail::ViewJSON &json, yyjson_mut_val *report, uint64_t origin, uint64_t end, bool collector) {
	auto &capture = Get();
	if (capture.active || (collector && capture.samples.size() != 4)) {
		throw std::logic_error("Incomplete reference diagnostic capture");
	}
	auto samples = json.Array();
	uint64_t previous = origin;
	for (auto &sample : capture.samples) {
		if (sample.begin < previous || sample.end < sample.begin || sample.end > end ||
		    sample.fetch_ns + sample.path_loop_ns > sample.end - sample.begin ||
		    (collector && (!sample.completed || sample.found || sample.fetch_calls != sample.chunks + 1))) {
			throw std::logic_error("Reference diagnostic interval or completion is invalid");
		}
		previous = sample.end;
		auto value = json.Object();
		json.Text(value, "path", sample.path);
		json.Number(value, "connection_id", sample.connection);
		json.Number(value, "begin_ns", sample.begin - origin);
		json.Number(value, "end_ns", sample.end - origin);
		json.Number(value, "call_ns", sample.end - sample.begin);
		json.Number(value, "fetch_ns", sample.fetch_ns);
		json.Number(value, "path_loop_ns", sample.path_loop_ns);
		json.Number(value, "fetch_calls", sample.fetch_calls);
		json.Number(value, "chunks", sample.chunks);
		json.Number(value, "rows", sample.rows);
		json.Add(value, "found", yyjson_mut_bool(json.doc.get(), sample.found));
		json.Add(value, "completed", yyjson_mut_bool(json.doc.get(), sample.completed));
		yyjson_mut_arr_append(samples, value);
	}
	json.Add(report, "reference_calls", samples);
}
} // namespace reference_trace
} // namespace vortex_test
} // namespace duckdb
