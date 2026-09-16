#include "functions/ducklake_table_functions.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"

#ifdef DUCKLAKE_VANE_DISTRIBUTED
#include "duckdb/function/distributed_table_function.hpp"
#endif

namespace duckdb {

unique_ptr<FunctionData> MetadataBindData::Copy() const {
	auto result = make_uniq<MetadataBindData>();
	result->rows = rows;
	return std::move(result);
}

bool MetadataBindData::Equals(const FunctionData &other) const {
	return rows == other.Cast<MetadataBindData>().rows;
}

void MetadataBindData::Serialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data,
                                 const TableFunction &) {
	serializer.WriteProperty(100, "rows", bind_data->Cast<MetadataBindData>().rows);
}

unique_ptr<FunctionData> MetadataBindData::Deserialize(Deserializer &deserializer, TableFunction &) {
	auto result = make_uniq<MetadataBindData>();
	result->rows = deserializer.ReadProperty<vector<vector<Value>>>(100, "rows");
	return std::move(result);
}

Catalog &DuckLakeBaseMetadataFunction::GetCatalog(ClientContext &context, const Value &input) {
	if (input.IsNull()) {
		throw BinderException("Catalog cannot be NULL");
	}
	// look up the database to query
	auto db_name = input.GetValue<string>();
	auto &db_manager = DatabaseManager::Get(context);
	auto db = db_manager.GetDatabase(context, db_name);
	if (!db) {
		throw BinderException("Failed to find attached database \"%s\"", db_name);
	}
	auto &catalog = db->GetCatalog();
	if (catalog.GetCatalogType() != "ducklake") {
		throw BinderException("Attached database \"%s\" does not refer to a DuckLake database", db_name);
	}
	return catalog;
}

struct MetadataFunctionData : public GlobalTableFunctionState {
	MetadataFunctionData() : offset(0) {
	}

	idx_t offset;
};

static unique_ptr<GlobalTableFunctionState> MetadataFunctionInit(ClientContext &context,
                                                                 TableFunctionInitInput &input) {
	auto result = make_uniq<MetadataFunctionData>();
	return std::move(result);
}

static void MetadataFunctionExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->Cast<MetadataBindData>();
	auto &state = data_p.global_state->Cast<MetadataFunctionData>();
	if (state.offset >= data.rows.size()) {
		// finished returning values
		return;
	}
	// start returning values
	// either fill up the chunk or return all the remaining columns
	idx_t count = 0;
	while (state.offset < data.rows.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.rows[state.offset++];
		if (entry.size() != output.ColumnCount()) {
			throw InternalException("Unaligned metadata row in result");
		}

		for (idx_t c = 0; c < entry.size(); c++) {
			output.SetValue(c, count, entry[c]);
		}
		count++;
	}
	output.SetCardinality(count);
}

DuckLakeBaseMetadataFunction::DuckLakeBaseMetadataFunction(string name_p, table_function_bind_t bind)
    : TableFunction(std::move(name_p), {LogicalType::VARCHAR}, MetadataFunctionExecute, bind, MetadataFunctionInit) {
	serialize = MetadataBindData::Serialize;
	deserialize = MetadataBindData::Deserialize;
#ifdef DUCKLAKE_VANE_DISTRIBUTED
	// MetadataBindData contains completed rows, with no live catalog dependency.
	SetDistributedScanCallbacks(MakeDistributedSingletonSourceCallbacks());
#endif
}

} // namespace duckdb
