#pragma once

#include "storage/ducklake_vortex_read_view.hpp"

namespace duckdb {
struct DuckLakeVortexSearchRequest {
	vector<float> query;
	idx_t k;
	bool filter_category = false;
	string category;
};

struct DuckLakeVortexSearchResult {
	vector<vector<Value>> rows;
	string diagnostics;
};

class DuckLakeVortexSearch {
public:
	static vector<Value> CollectScratch(ClientContext &context, const string &catalog, const string &root,
	                                    idx_t max_objects, idx_t max_bytes,
	                                    const std::function<void(const string &)> &observe = nullptr);
	static DuckLakeVortexSearchResult Execute(ClientContext &context, const DuckLakeVortexViewOptions &options,
	                                          const DuckLakeVortexSearchRequest &request,
	                                          const std::function<void(const string &)> &observe = nullptr);
};

void RegisterDuckLakeVortexSearch(ExtensionLoader &loader);
} // namespace duckdb
