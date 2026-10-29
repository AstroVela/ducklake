#include "storage/ducklake_vortex_index_catalog.hpp"
#include "storage/ducklake_vortex_read_view.hpp"

#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

struct IndexCatalogBind : public TableFunctionData {
	vector<Value> args;
	bool initialize;
	DuckLakeVortexIndexOperation operation = DuckLakeVortexIndexOperation::REPLACE;
	vector<string> merge_generations;
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
		auto record = DuckLakeVortexIndex::Publish(
		    owner, StringValue::Get(data.args[0]), StringValue::Get(data.args[1]), StringValue::Get(data.args[2]),
		    StringValue::Get(data.args[3]), StringValue::Get(data.args[4]),
		    data.args[data.operation == DuckLakeVortexIndexOperation::MERGE ? 6 : 5].GetValue<idx_t>(), nullptr,
		    data.operation, data.merge_generations);
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
	result->Cast<IndexCatalogBind>().operation = DuckLakeVortexIndexOperation::APPEND;
	return result;
}

static unique_ptr<FunctionData> BindMerge(ClientContext &context, TableFunctionBindInput &input,
                                          vector<LogicalType> &types, vector<string> &names) {
	auto result = BindCatalog(context, input, types, names);
	auto &bind = result->Cast<IndexCatalogBind>();
	bind.operation = DuckLakeVortexIndexOperation::MERGE;
	for (auto &value : ListValue::GetChildren(input.inputs[5])) {
		if (value.IsNull() || StringValue::Get(value).empty()) {
			throw InvalidInputException("Vortex merge generation cannot be NULL or empty");
		}
		bind.merge_generations.push_back(StringValue::Get(value));
	}
	if (bind.merge_generations.size() != 2 || bind.merge_generations[0] == bind.merge_generations[1] ||
	    input.inputs[6].GetValue<int64_t>() < 0) {
		throw InvalidInputException("Vortex merge requires two distinct ready generations and a valid revision");
	}
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
	    StringValue::Get(args[3]), StringValue::Get(args[4]),
	    bind.operation == DuckLakeVortexIndexOperation::MERGE ? 0 : uint32_t(args[5].GetValue<int64_t>()),
	    args[6].GetValue<idx_t>(), nullptr, "", optional_idx(), bind.operation, bind.merge_generations);
	output.SetValue(0, 0, Value::UBIGINT(record.revision));
	output.SetValue(1, 0, Value(record.generation));
	output.SetCardinality(1);
	state.finished = true;
}

static unique_ptr<FunctionData> BindRewrite(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &types, vector<string> &names) {
	auto result = BindCatalog(context, input, types, names);
	if (input.inputs[6].GetValue<int64_t>() < 0) {
		throw InvalidInputException("Invalid Vortex rewrite revision");
	}
	return result;
}

static void ExecuteRewrite(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &state = input.global_state->Cast<IndexCatalogState>();
	if (state.finished) {
		return;
	}
	auto &args = input.bind_data->Cast<IndexCatalogBind>().args;
	auto result = DuckLakeVortexIndex::Rewrite(
	    context, StringValue::Get(args[0]), StringValue::Get(args[1]), StringValue::Get(args[2]),
	    StringValue::Get(args[3]), StringValue::Get(args[4]), StringValue::Get(args[5]), args[6].GetValue<idx_t>());
	output.SetValue(0, 0, Value::UBIGINT(result.revision));
	output.SetValue(1, 0, Value(result.generation));
	output.SetCardinality(1);
	state.finished = true;
}

static unique_ptr<FunctionData> BindRewriteToF(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &types, vector<string> &names) {
	auto result = BindCatalog(context, input, types, names);
	if (input.inputs[5].GetValue<int64_t>() < 0) {
		throw InvalidInputException("Invalid Vortex F rewrite revision");
	}
	names = {"revision", "publication"};
	return result;
}

static void ExecuteRewriteToF(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &state = input.global_state->Cast<IndexCatalogState>();
	if (state.finished) {
		return;
	}
	auto &args = input.bind_data->Cast<IndexCatalogBind>().args;
	auto result =
	    DuckLakeVortexIndex::Rewrite(context, StringValue::Get(args[0]), StringValue::Get(args[1]),
	                                 StringValue::Get(args[2]), StringValue::Get(args[3]), StringValue::Get(args[4]),
	                                 "", args[5].GetValue<idx_t>(), nullptr, DuckLakeVortexRewriteMode::FILES);
	output.SetValue(0, 0, Value::UBIGINT(result.revision));
	output.SetValue(1, 0, Value(result.Publication()));
	output.SetCardinality(1);
	state.finished = true;
}

static unique_ptr<FunctionData> BindRetire(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &types, vector<string> &names) {
	auto result = BindCatalog(context, input, types, names);
	if (input.inputs[5].GetValue<int64_t>() < 0) {
		throw InvalidInputException("Invalid Vortex retirement revision");
	}
	names = {"revision", "publication"};
	return result;
}

static void ExecuteRetire(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &state = input.global_state->Cast<IndexCatalogState>();
	if (state.finished) {
		return;
	}
	auto &args = input.bind_data->Cast<IndexCatalogBind>().args;
	auto result = DuckLakeVortexIndex::RetireEmpty(context, StringValue::Get(args[0]), StringValue::Get(args[1]),
	                                               StringValue::Get(args[2]), StringValue::Get(args[3]),
	                                               StringValue::Get(args[4]), args[5].GetValue<idx_t>());
	output.SetValue(0, 0, Value::UBIGINT(result.revision));
	output.SetValue(1, 0, Value(result.Publication()));
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

enum class MaintenanceOperation { GENERATIONS, COLLECT_GENERATIONS, BUILDS, RECOVER_BUILDS, COLLECT_BUILDS };

struct GenerationBind : public TableFunctionData {
	MaintenanceOperation operation;
	vector<Value> args;
};

struct GenerationState : public GlobalTableFunctionState {
	bool loaded = false;
	idx_t offset = 0;
	vector<vector<Value>> rows;
};

static unique_ptr<GlobalTableFunctionState> InitGeneration(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<GenerationState>();
}

template <MaintenanceOperation OPERATION>
static unique_ptr<FunctionData> BindGeneration(ClientContext &, TableFunctionBindInput &input,
                                               vector<LogicalType> &types, vector<string> &names) {
	for (auto &arg : input.inputs) {
		if (arg.IsNull()) {
			throw InvalidInputException("Vortex maintenance arguments cannot be NULL");
		}
	}
	const bool status = OPERATION == MaintenanceOperation::GENERATIONS || OPERATION == MaintenanceOperation::BUILDS;
	const bool recovery = OPERATION == MaintenanceOperation::RECOVER_BUILDS;
	if (!status && (input.inputs[1].GetValue<int64_t>() < 1 || input.inputs[1].GetValue<int64_t>() > 16 ||
	                (!recovery && (input.inputs[2].GetValue<int64_t>() < 1 ||
	                               input.inputs[2].GetValue<int64_t>() > 64 * 1024 * 1024)))) {
		throw InvalidInputException("Invalid Vortex collection count or artifact byte budget");
	}
	auto bind = make_uniq<GenerationBind>();
	bind->operation = OPERATION;
	bind->args = input.inputs;
	if (OPERATION == MaintenanceOperation::GENERATIONS) {
		names = {"index_root", "generation", "state", "artifact_bytes", "reference_count"};
		types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::UBIGINT,
		         LogicalType::UBIGINT};
	} else if (OPERATION == MaintenanceOperation::BUILDS) {
		names = {"build_id",        "index_root",        "generation",     "workspace",  "state",
		         "keep_generation", "expected_revision", "reference_path", "table_uuid", "index_name"};
		types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
		         LogicalType::VARCHAR, LogicalType::BOOLEAN, LogicalType::UBIGINT, LogicalType::VARCHAR,
		         LogicalType::VARCHAR, LogicalType::VARCHAR};
	} else if (recovery) {
		names = {"reconciled_builds"};
		types = {LogicalType::UBIGINT};
	} else {
		names = {"collected_generations", "collected_artifact_bytes", "remaining_generations"};
		if (OPERATION == MaintenanceOperation::COLLECT_BUILDS) {
			names = {"collected_builds", "collected_bytes", "remaining_builds"};
		}
		types = {LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::UBIGINT};
	}
	return std::move(bind);
}

static void ExecuteGeneration(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &bind = input.bind_data->Cast<GenerationBind>();
	auto &state = input.global_state->Cast<GenerationState>();
	if (!state.loaded) {
		auto catalog = StringValue::Get(bind.args[0]);
		if (bind.operation == MaintenanceOperation::GENERATIONS || bind.operation == MaintenanceOperation::BUILDS) {
			Connection owner(*context.db);
			state.rows = bind.operation == MaintenanceOperation::BUILDS
			                 ? DuckLakeVortexIndex::BuildStatus(owner, catalog)
			                 : DuckLakeVortexIndex::GenerationStatus(owner, catalog);
		} else if (bind.operation == MaintenanceOperation::RECOVER_BUILDS) {
			state.rows.push_back(
			    {Value::UBIGINT(DuckLakeVortexIndex::RecoverBuilds(context, catalog, bind.args[1].GetValue<idx_t>()))});
		} else if (bind.operation == MaintenanceOperation::COLLECT_BUILDS) {
			auto result = DuckLakeVortexIndex::CollectBuilds(context, catalog, bind.args[1].GetValue<idx_t>(),
			                                                 bind.args[2].GetValue<idx_t>());
			state.rows.push_back(
			    {Value::UBIGINT(result.builds), Value::UBIGINT(result.bytes), Value::UBIGINT(result.remaining)});
		} else {
			auto result = DuckLakeVortexIndex::Collect(context, catalog, bind.args[1].GetValue<idx_t>(),
			                                           bind.args[2].GetValue<idx_t>());
			state.rows.push_back({Value::UBIGINT(result.generations), Value::UBIGINT(result.artifact_bytes),
			                      Value::UBIGINT(result.remaining)});
		}
		state.loaded = true;
	}
	idx_t count = 0;
	while (count < STANDARD_VECTOR_SIZE && state.offset < state.rows.size()) {
		auto &row = state.rows[state.offset++];
		for (idx_t i = 0; i < row.size(); i++) {
			output.SetValue(i, count, row[i]);
		}
		count++;
	}
	output.SetCardinality(count);
}

void RegisterDuckLakeVortexIndexCatalog(ExtensionLoader &loader) {
	loader.RegisterFunction(TableFunction("ducklake_rewrite_vortex_index_segment_to_f",
	                                      {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                                       LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT},
	                                      ExecuteRewriteToF, BindRewriteToF, InitCatalog));
	loader.RegisterFunction(TableFunction("ducklake_retire_vortex_index_segment",
	                                      {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                                       LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT},
	                                      ExecuteRetire, BindRetire, InitCatalog));
	loader.RegisterFunction(
	    TableFunction("ducklake_rewrite_vortex_index_segment",
	                  {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                   LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT},
	                  ExecuteRewrite, BindRewrite, InitCatalog));
	loader.RegisterFunction(TableFunction(
	    "ducklake_collect_vortex_generations", {LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::BIGINT},
	    ExecuteGeneration, BindGeneration<MaintenanceOperation::COLLECT_GENERATIONS>, InitGeneration));
	loader.RegisterFunction(TableFunction("ducklake_vortex_generation_status", {LogicalType::VARCHAR},
	                                      ExecuteGeneration, BindGeneration<MaintenanceOperation::GENERATIONS>,
	                                      InitGeneration));
	loader.RegisterFunction(TableFunction("ducklake_vortex_build_status", {LogicalType::VARCHAR}, ExecuteGeneration,
	                                      BindGeneration<MaintenanceOperation::BUILDS>, InitGeneration));
	loader.RegisterFunction(TableFunction("ducklake_recover_vortex_builds", {LogicalType::VARCHAR, LogicalType::BIGINT},
	                                      ExecuteGeneration, BindGeneration<MaintenanceOperation::RECOVER_BUILDS>,
	                                      InitGeneration));
	loader.RegisterFunction(TableFunction(
	    "ducklake_collect_vortex_builds", {LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::BIGINT},
	    ExecuteGeneration, BindGeneration<MaintenanceOperation::COLLECT_BUILDS>, InitGeneration));
	for (auto &name : {"ducklake_merge_vortex_index_segments", "ducklake_publish_vortex_index_merge"}) {
		loader.RegisterFunction(
		    TableFunction(name,
		                  {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
		                   LogicalType::VARCHAR, LogicalType::LIST(LogicalType::VARCHAR), LogicalType::BIGINT},
		                  string(name) == "ducklake_merge_vortex_index_segments" ? ExecuteBuild : ExecuteCatalog,
		                  BindMerge, InitCatalog));
	}
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
