#pragma once

#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector.hpp"

#include <algorithm>

namespace duckdb {

// Retained snapshots must be sorted. Visibility is [begin, end), including across gaps.
inline bool DuckLakeFileCanExpire(const vector<int64_t> &retained_snapshots, const Value &begin, const Value &end,
                                  bool table_expired) {
	if (table_expired) {
		return true;
	}
	if (end.IsNull()) {
		return false;
	}
	// SQL comparisons with a NULL begin cannot match a retained snapshot.
	if (begin.IsNull()) {
		return true;
	}
	auto next = std::lower_bound(retained_snapshots.begin(), retained_snapshots.end(), begin.GetValue<int64_t>());
	return next == retained_snapshots.end() || *next >= end.GetValue<int64_t>();
}

} // namespace duckdb
