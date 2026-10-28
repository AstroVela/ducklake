#pragma once

#include "common/ducklake_data_file.hpp"
#include "duckdb/common/open_file_info.hpp"

namespace duckdb {

class BaseFileReader;
class ClientContext;
class DuckLakeCatalog;
class DuckLakeTableEntry;
struct DuckLakeCopyInput;
struct DuckLakeCopyOptions;

struct DuckLakeVortex {
	static DataFileFormat GetFormat(const DuckLakeCatalog &catalog, SchemaIndex schema, TableIndex table);
	static DataFileFormat GetFormat(DuckLakeTableEntry &table);
	static void ValidateEnabled();
	static void ValidateTable(DuckLakeTableEntry &table);
	static bool IsVortex(const OpenFileInfo &file);
	static shared_ptr<BaseFileReader> CreateReader(ClientContext &context, const OpenFileInfo &file,
	                                               DuckLakeTableEntry &table);
	static DuckLakeCopyOptions CopyOptions(ClientContext &context, DuckLakeCopyInput &input);
};

} // namespace duckdb
