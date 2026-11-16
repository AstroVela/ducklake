#include "storage/ducklake_vortex_optimizer.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {
namespace {
enum class Operation { INITIALIZE, ENABLE, RESUME, CANCEL, STATUS };

struct OptimizerBind : public TableFunctionData {
	Operation operation;
	vector<Value> args;
};

struct OptimizerState : public GlobalTableFunctionState {
	bool loaded = false;
	idx_t offset = 0;
	vector<vector<Value>> rows;
};

unique_ptr<GlobalTableFunctionState> Init(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<OptimizerState>();
}

template <Operation OP>
unique_ptr<FunctionData> Bind(ClientContext &, TableFunctionBindInput &input, vector<LogicalType> &types,
                              vector<string> &names) {
	for (auto &value : input.inputs) {
		if (value.IsNull()) {
			throw InvalidInputException("Vortex optimizer arguments cannot be NULL");
		}
	}
	if (OP == Operation::ENABLE) {
		for (idx_t i = 5; i < input.inputs.size(); i++) {
			if (input.inputs[i].GetValue<int64_t>() <= 0) {
				throw InvalidInputException("Vortex optimizer dimension and thresholds must be positive");
			}
		}
		if (input.inputs[5].GetValue<int64_t>() > 4096) {
			throw InvalidInputException("Vortex optimizer dimension exceeds 4096");
		}
	}
	auto bind = make_uniq<OptimizerBind>();
	bind->operation = OP;
	bind->args = input.inputs;
	if (OP == Operation::STATUS) {
		names = {"table_uuid",  "schema_name",  "table_name",        "index_name",           "state",
		         "enabled",     "run_id",       "expected_revision", "candidate_generation", "published_revision",
		         "inline_rows", "inline_bytes", "unindexed_rows",    "unindexed_bytes",      "error",
		         "run_count",   "running"};
		types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
		         LogicalType::VARCHAR, LogicalType::BOOLEAN, LogicalType::VARCHAR, LogicalType::UBIGINT,
		         LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::UBIGINT,
		         LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::VARCHAR, LogicalType::UBIGINT,
		         LogicalType::BOOLEAN};
	} else {
		names = {"state"};
		types = {LogicalType::VARCHAR};
	}
	return std::move(bind);
}

void Execute(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &state = input.global_state->Cast<OptimizerState>();
	auto &bind = input.bind_data->Cast<OptimizerBind>();
	auto &args = bind.args;
	auto text = [&](idx_t i) {
		return StringValue::Get(args[i]);
	};
	if (!state.loaded) {
		string result;
		switch (bind.operation) {
		case Operation::INITIALIZE: {
			Connection owner(*context.db);
			DuckLakeVortexOptimizer::Initialize(owner, text(0));
			result = "initialized";
			break;
		}
		case Operation::ENABLE: {
			DuckLakeVortexOptimizerConfig config;
			config.catalog = text(0);
			config.schema = text(1);
			config.table = text(2);
			config.index = text(3);
			config.root = text(4);
			config.dimension = uint32_t(args[5].GetValue<idx_t>());
			config.flush_rows = args[6].GetValue<idx_t>();
			config.flush_bytes = args[7].GetValue<idx_t>();
			config.index_rows = args[8].GetValue<idx_t>();
			config.index_bytes = args[9].GetValue<idx_t>();
			DuckLakeVortexOptimizer::Enable(context, config);
			result = "enabled";
			break;
		}
		case Operation::RESUME:
			result = DuckLakeVortexOptimizer::Resume(context, text(0), text(1), text(2), text(3));
			break;
		case Operation::CANCEL:
			DuckLakeVortexOptimizer::Cancel(context, text(0), text(1), text(2), text(3));
			result = "cancellation_requested";
			break;
		case Operation::STATUS:
			state.rows = DuckLakeVortexOptimizer::Status(context, text(0));
			break;
		}
		if (bind.operation != Operation::STATUS) {
			state.rows.push_back({Value(result)});
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
} // namespace

void RegisterDuckLakeVortexOptimizer(ExtensionLoader &loader) {
	loader.RegisterFunction(TableFunction("ducklake_initialize_vortex_optimizer", {LogicalType::VARCHAR}, Execute,
	                                      Bind<Operation::INITIALIZE>, Init));
	loader.RegisterFunction(TableFunction(
	    "ducklake_enable_vortex_optimizer",
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	     LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT},
	    Execute, Bind<Operation::ENABLE>, Init));
	for (auto &name : {"ducklake_resume_vortex_optimizer", "ducklake_cancel_vortex_optimizer"}) {
		auto bind =
		    string(name) == "ducklake_resume_vortex_optimizer" ? Bind<Operation::RESUME> : Bind<Operation::CANCEL>;
		loader.RegisterFunction(TableFunction(
		    name, {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, Execute,
		    bind, Init));
	}
	loader.RegisterFunction(TableFunction("ducklake_vortex_optimizer_status", {LogicalType::VARCHAR}, Execute,
	                                      Bind<Operation::STATUS>, Init));
}
} // namespace duckdb
