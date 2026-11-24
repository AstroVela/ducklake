#include "storage/ducklake_vortex_search.hpp"

#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include <cmath>

namespace duckdb {
struct VortexSearchBind : public TableFunctionData {
	DuckLakeVortexViewOptions options;
	DuckLakeVortexSearchRequest request;
	bool diagnostics = false;
};
struct VortexSearchState : public GlobalTableFunctionState {
	bool initialized = false;
	idx_t offset = 0;
	vector<vector<Value>> rows;
};

static unique_ptr<FunctionData> BindSearch(ClientContext &, TableFunctionBindInput &input, vector<LogicalType> &types,
                                           vector<string> &names) {
	for (auto &value : input.inputs) {
		if (value.IsNull()) {
			throw InvalidInputException("Vortex SQL search arguments cannot be NULL");
		}
	}
	auto data = make_uniq<VortexSearchBind>();
	data->options = {StringValue::Get(input.inputs[0]),
	                 StringValue::Get(input.inputs[1]),
	                 StringValue::Get(input.inputs[2]),
	                 0,
	                 64 * 1024 * 1024,
	                 100000,
	                 StringValue::Get(input.inputs[3])};
	if (data->options.index_name.empty()) {
		throw InvalidInputException("Vortex SQL index name cannot be empty");
	}
	auto &query = ListValue::GetChildren(input.inputs[4]);
	if (query.empty() || query.size() > 4096) {
		throw InvalidInputException("Vortex SQL query dimension must be between 1 and 4096");
	}
	for (auto &value : query) {
		if (value.IsNull() || !std::isfinite(value.GetValue<float>())) {
			throw InvalidInputException("Vortex SQL query requires finite non-NULL FLOAT components");
		}
		data->request.query.push_back(value.GetValue<float>());
	}
	auto k = input.inputs[5].GetValue<int64_t>();
	if (k < 1 || k > 64) {
		throw InvalidInputException("Vortex SQL k must be between 1 and 64");
	}
	data->request.k = idx_t(k);
	auto category = input.named_parameters.find("category");
	if (category != input.named_parameters.end()) {
		if (category->second.IsNull()) {
			throw InvalidInputException("Vortex SQL category filter cannot be NULL");
		}
		data->request.filter_category = true;
		data->request.category = StringValue::Get(category->second);
		if (data->request.category.size() > 64 * 1024) {
			throw InvalidInputException("Vortex SQL category filter exceeds its byte budget");
		}
	}
	types = {LogicalType::BIGINT, LogicalType::LIST(LogicalType::FLOAT), LogicalType::VARCHAR, LogicalType::VARCHAR,
	         LogicalType::FLOAT};
	names = {"id", "embedding", "category", "payload", "distance"};
	return std::move(data);
}

static unique_ptr<FunctionData> BindDiagnostics(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &types, vector<string> &names) {
	auto data = BindSearch(context, input, types, names);
	data->Cast<VortexSearchBind>().diagnostics = true;
	child_list_t<LogicalType> fields;
	for (idx_t i = 0; i < names.size(); i++) {
		fields.emplace_back(names[i], types[i]);
	}
	types = {LogicalType::VARCHAR, LogicalType::LIST(LogicalType::STRUCT(std::move(fields)))};
	names = {"diagnostics", "hits"};
	return data;
}

static unique_ptr<GlobalTableFunctionState> InitSearch(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<VortexSearchState>();
}

static void ExecuteSearch(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &state = input.global_state->Cast<VortexSearchState>();
	if (!state.initialized) {
		auto &bind = input.bind_data->Cast<VortexSearchBind>();
		auto result = DuckLakeVortexSearch::Execute(context, bind.options, bind.request);
		if (bind.diagnostics) {
			auto &hit_type = ListType::GetChildType(output.data[1].GetType());
			vector<Value> hits;
			for (auto &row : result.rows) {
				hits.push_back(Value::STRUCT(hit_type, std::move(row)));
			}
			state.rows.push_back({Value(result.diagnostics), Value::LIST(hit_type, std::move(hits))});
		} else {
			state.rows = std::move(result.rows);
		}
		state.initialized = true;
	}
	if (context.IsInterrupted()) {
		throw InterruptException();
	}
	auto count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, state.rows.size() - state.offset);
	for (idx_t row = 0; row < count; row++) {
		for (idx_t col = 0; col < output.ColumnCount(); col++) {
			output.SetValue(col, row, state.rows[state.offset + row][col]);
		}
	}
	output.SetCardinality(count);
	state.offset += count;
}

void RegisterDuckLakeVortexSearch(ExtensionLoader &loader) {
	for (auto &name : {"ducklake_vortex_search", "ducklake_vortex_search_diagnostics"}) {
		auto bind = string(name) == "ducklake_vortex_search" ? BindSearch : BindDiagnostics;
		TableFunction function(name,
		                       {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
		                        LogicalType::LIST(LogicalType::FLOAT), LogicalType::BIGINT},
		                       ExecuteSearch, bind, InitSearch);
		function.named_parameters["category"] = LogicalType::VARCHAR;
		loader.RegisterFunction(function);
	}
}
} // namespace duckdb
