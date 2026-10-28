#pragma once

#include "storage/ducklake_vortex_read_view.hpp"
#include "duckdb/main/client_context.hpp"
#include <exception>

namespace duckdb {
struct DuckLakeVortexControl {
	ClientContext &context;
	const std::function<void(const string &)> &observe;
	std::exception_ptr error;
	vector<DuckLakeVortexSourceBuffer> &sources;

	static uint8_t Check(void *pointer, const uint8_t *stage, uint64_t length) {
		auto &control = *static_cast<DuckLakeVortexControl *>(pointer);
		try {
			auto name = string(reinterpret_cast<const char *>(stage), length);
			if (name == "source_pinned") {
				control.sources.clear();
			}
			if (control.observe) {
				control.observe(name);
			}
		} catch (...) {
			control.error = std::current_exception();
			return 1;
		}
		return control.context.IsInterrupted() ? 1 : 0;
	}
};
} // namespace duckdb
