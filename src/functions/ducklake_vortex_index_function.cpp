#include "storage/ducklake_vortex_index_catalog.hpp"
#include "storage/ducklake_vortex_read_view.hpp"

#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

struct IndexCatalogBind : public TableFunctionData {
	vector<Value> args;
	bool initialize;
	bool append_segment = false;
};
struct IndexCatalogState : public GlobalTableFunctionState {
	bool finished = false;
};

static unique_ptr<GlobalTableFunctionState> InitCatalog(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<IndexCatalogState>();
}

static unique_ptr<FunctionData> BindCatalog(ClientContext &, TableFunctionBindInput &input, vector<LogicalType> &types,
                                            vector<string> &names) {
	for (auto &value : input.inputs) {
		if (value.IsNull()) {
			throw InvalidInputException("Vortex index catalog arguments cannot be NULL");
		}
	}
	auto data = make_uniq<IndexCatalogBind>();
	data->args = input.inputs;
	data->initialize = input.inputs.size() == 1;
	if (data->initialize) {
		types = {LogicalType::BOOLEAN};
		names = {"initialized"};
	} else {
		types = {LogicalType::UBIGINT, LogicalType::VARCHAR};
		names = {"revision", "generation"};
	}
	return std::move(data);
}

static void ExecuteCatalog(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &state = input.global_state->Cast<IndexCatalogState>();
	if (state.finished) {
		return;
	}
	auto &data = input.bind_data->Cast<IndexCatalogBind>();
	Connection owner(*context.db);
	if (data.initialize) {
		DuckLakeVortexIndex::Initialize(owner, StringValue::Get(data.args[0]));
		output.SetValue(0, 0, Value::BOOLEAN(true));
	} else {
		auto record = DuckLakeVortexIndex::Publish(owner, StringValue::Get(data.args[0]),
		                                           StringValue::Get(data.args[1]), StringValue::Get(data.args[2]),
		                                           StringValue::Get(data.args[3]), StringValue::Get(data.args[4]),
		                                           data.args[5].GetValue<idx_t>(), nullptr, data.append_segment);
		output.SetValue(0, 0, Value::UBIGINT(record.revision));
		output.SetValue(1, 0, Value(record.generation));
	}
	output.SetCardinality(1);
	state.finished = true;
}

struct IndexViewBind : public TableFunctionData {
	DuckLakeVortexViewOptions options;
};

static unique_ptr<FunctionData> BindBuild(ClientContext &context, TableFunctionBindInput &input,
                                          vector<LogicalType> &types, vector<string> &names) {
	auto result = BindCatalog(context, input, types, names);
	auto dimension = input.inputs[5].GetValue<int64_t>();
	if (dimension < 1 || dimension > 4096 || input.inputs[6].GetValue<int64_t>() < 0) {
		throw InvalidInputException("Invalid Vortex build dimension or revision");
	}
	return result;
}

static unique_ptr<FunctionData> BindAppend(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &types, vector<string> &names) {
	auto result =
	    input.inputs.size() == 7 ? BindBuild(context, input, types, names) : BindCatalog(context, input, types, names);
	result->Cast<IndexCatalogBind>().append_segment = true;
	return result;
}

static void ExecuteBuild(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &state = input.global_state->Cast<IndexCatalogState>();
	if (state.finished) {
		return;
	}
	auto &bind = input.bind_data->Cast<IndexCatalogBind>();
	auto &args = bind.args;
	auto record = DuckLakeVortexIndex::BuildAndPublish(
	    context, StringValue::Get(args[0]), StringValue::Get(args[1]), StringValue::Get(args[2]),
	    StringValue::Get(args[3]), StringValue::Get(args[4]), uint32_t(args[5].GetValue<int64_t>()),
	    args[6].GetValue<idx_t>(), nullptr, "", optional_idx(), bind.append_segment);
	output.SetValue(0, 0, Value::UBIGINT(record.revision));
	output.SetValue(1, 0, Value(record.generation));
	output.SetCardinality(1);
	state.finished = true;
}

static unique_ptr<FunctionData> BindIndexView(ClientContext &, TableFunctionBindInput &input,
                                              vector<LogicalType> &types, vector<string> &names) {
	for (auto &value : input.inputs) {
		if (value.IsNull()) {
			throw InvalidInputException("Catalogued read view arguments cannot be NULL");
		}
	}
	auto data = make_uniq<IndexViewBind>();
	data->options = {
	    StringValue::Get(input.inputs[0]), StringValue::Get(input.inputs[1]), StringValue::Get(input.inputs[2]), 0,
	    input.inputs[4].GetValue<idx_t>(), input.inputs[5].GetValue<idx_t>(), StringValue::Get(input.inputs[3])};
	if (data->options.index_name.empty()) {
		throw InvalidInputException("Vortex index name cannot be empty");
	}
	types = {LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::UBIGINT,
	         LogicalType::UBIGINT, LogicalType::VARCHAR, LogicalType::VARCHAR};
	names = {"snapshot_id", "source_bytes", "visible_rows", "inline_rows", "admission", "rows"};
	return std::move(data);
}

static void ExecuteIndexView(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &state = input.global_state->Cast<IndexCatalogState>();
	if (state.finished) {
		return;
	}
	Connection reader(*context.db);
	auto view =
	    DuckLakeVortexReadView::Acquire(reader, input.bind_data->Cast<IndexViewBind>().options, nullptr, &context);
	output.SetValue(0, 0, Value::UBIGINT(view.snapshot_id));
	output.SetValue(1, 0, Value::UBIGINT(view.source_bytes));
	output.SetValue(2, 0, Value::UBIGINT(view.visible_rows));
	output.SetValue(3, 0, Value::UBIGINT(view.inline_rows));
	output.SetValue(4, 0, Value(std::move(view.admission_json)));
	output.SetValue(5, 0, Value(std::move(view.rows_json)));
	output.SetCardinality(1);
	state.finished = true;
}

void RegisterDuckLakeVortexIndexCatalog(ExtensionLoader &loader) {
	loader.RegisterFunction(
	    TableFunction("ducklake_append_vortex_index_segment",
	                  {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                   LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::BIGINT},
	                  ExecuteBuild, BindAppend, InitCatalog));
	loader.RegisterFunction(TableFunction("ducklake_publish_vortex_index_segment",
	                                      {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                                       LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT},
	                                      ExecuteCatalog, BindAppend, InitCatalog));
	loader.RegisterFunction(
	    TableFunction("ducklake_build_vortex_index",
	                  {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                   LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::BIGINT},
	                  ExecuteBuild, BindBuild, InitCatalog));
	loader.RegisterFunction(TableFunction("ducklake_initialize_vortex_indexes", {LogicalType::VARCHAR}, ExecuteCatalog,
	                                      BindCatalog, InitCatalog));
	loader.RegisterFunction(TableFunction("ducklake_publish_vortex_index",
	                                      {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                                       LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT},
	                                      ExecuteCatalog, BindCatalog, InitCatalog));
	loader.RegisterFunction(TableFunction("ducklake_vortex_index_view",
	                                      {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                                       LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::BIGINT},
	                                      ExecuteIndexView, BindIndexView, InitCatalog));
}
} // namespace duckdb
