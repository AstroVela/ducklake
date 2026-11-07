#include "storage/ducklake_vortex_read_view.hpp"

#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

struct VortexViewBindData : public TableFunctionData {
	DuckLakeVortexViewOptions options;
};

struct VortexViewState : public GlobalTableFunctionState {
	bool finished = false;
};

static unique_ptr<FunctionData> BindVortexView(ClientContext &, TableFunctionBindInput &input,
                                               vector<LogicalType> &types, vector<string> &names) {
	for (auto &value : input.inputs) {
		if (value.IsNull()) {
			throw InvalidInputException("Native read view arguments cannot be NULL");
		}
	}
	auto result = make_uniq<VortexViewBindData>();
	result->options = {StringValue::Get(input.inputs[0]), StringValue::Get(input.inputs[1]),
	                   StringValue::Get(input.inputs[2]), input.inputs[3].GetValue<uint32_t>(),
	                   input.inputs[4].GetValue<idx_t>(), input.inputs[5].GetValue<idx_t>()};
	names = {"snapshot_id", "source_bytes", "visible_rows", "inline_rows", "admission", "rows"};
	types = {LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::UBIGINT,
	         LogicalType::UBIGINT, LogicalType::VARCHAR, LogicalType::VARCHAR};
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> InitVortexView(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<VortexViewState>();
}

static void ExecuteVortexView(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &state = input.global_state->Cast<VortexViewState>();
	if (state.finished) {
		return;
	}
	auto &data = input.bind_data->Cast<VortexViewBindData>();
	Connection reader(*context.db);
	auto view = DuckLakeVortexReadView::Acquire(reader, data.options, nullptr, &context);
	output.SetValue(0, 0, Value::UBIGINT(view.snapshot_id));
	output.SetValue(1, 0, Value::UBIGINT(view.source_bytes));
	output.SetValue(2, 0, Value::UBIGINT(view.visible_rows));
	output.SetValue(3, 0, Value::UBIGINT(view.inline_rows));
	output.SetValue(4, 0, Value(std::move(view.admission_json)));
	output.SetValue(5, 0, Value(std::move(view.rows_json)));
	output.SetCardinality(1);
	state.finished = true;
}

void RegisterDuckLakeVortexReadView(ExtensionLoader &loader) {
	TableFunction function("ducklake_vortex_read_view",
	                       {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
	                        LogicalType::BIGINT, LogicalType::BIGINT},
	                       ExecuteVortexView, BindVortexView, InitVortexView);
	loader.RegisterFunction(function);
}

} // namespace duckdb
