#pragma once

#include "common/ducklake_data_file.hpp"
#include "duckdb/common/open_file_info.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"

namespace duckdb {

class BaseFileReader;
class ClientContext;
class DuckLakeCatalog;
class DuckLakeTableEntry;
struct DuckLakeCopyInput;
struct DuckLakeCopyOptions;
struct DuckLakeFileData;

struct DuckLakeVortex {
	static DataFileFormat GetFormat(ClientContext &context, DuckLakeCatalog &catalog, TableIndex table);
	static DataFileFormat GetFormat(ClientContext &context, DuckLakeTableEntry &table);
	static void ValidateEnabled();
	static void ValidateTable(DuckLakeTableEntry &table);
	static bool IsVortex(const OpenFileInfo &file);
	static shared_ptr<BaseFileReader> CreateReader(ClientContext &context, const OpenFileInfo &file,
	                                               DuckLakeTableEntry &table);
	static DuckLakeCopyOptions CopyOptions(ClientContext &context, DuckLakeCopyInput &input);
	static unordered_map<idx_t, idx_t> ScanRowIds(ClientContext &context, const DuckLakeFileData &file,
	                                              const unordered_set<idx_t> &file_positions);
};

} // namespace duckdb
