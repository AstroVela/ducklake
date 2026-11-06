#pragma once

#include "duckdb/common/file_system.hpp"

namespace duckdb {
// A necessary suffix condition only. Retained rows still use the original full check.
inline bool DuckLakeFilenameMayMatch(FileSystem &fs, const string &known, const string &target) {
	if (FileSystem::IsRemoteFile(known)) {
		return known == target;
	}
	auto separator = fs.PathSeparator(known);
	auto last = known.rfind(separator);
	auto begin = last == string::npos ? 0 : last + separator.size();
	auto length = known.size() - begin;
	if (!length || known.compare(begin, length, ".") == 0 || known.compare(begin, length, "..") == 0) {
		return true;
	}
	return target.size() >= length && target.compare(target.size() - length, length, known, begin, length) == 0;
}
} // namespace duckdb
