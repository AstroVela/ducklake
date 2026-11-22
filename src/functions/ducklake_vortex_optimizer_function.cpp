#include "storage/ducklake_vortex_optimizer.hpp"
#include "storage/ducklake_vortex_source_retention.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/transaction/transaction_context.hpp"

namespace duckdb {
namespace {
enum class Operation {
	INITIALIZE,
	ENABLE,
	RESUME,
	CANCEL,
	STATUS,
	WRITE_BUDGET,
	COMPACTION_POLICY,
	ENABLE_MAINTENANCE,
	RESUME_MAINTENANCE,
	CANCEL_MAINTENANCE,
	MAINTENANCE_STATUS,
	SOURCE_RETENTION_POLICY,
	SOURCE_RETENTION_STATUS,
	COLLECT_SOURCES
};

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
	if (OP == Operation::WRITE_BUDGET) {
		for (idx_t i = 4; i < input.inputs.size(); i++) {
			if (input.inputs[i].GetValue<int64_t>() <= 0) {
				throw InvalidInputException("Vortex write budgets must be positive");
			}
		}
	}
	if (OP == Operation::COMPACTION_POLICY &&
	    (input.inputs[5].GetValue<int64_t>() < 1 || input.inputs[5].GetValue<int64_t>() > 100000 ||
	     input.inputs[6].GetValue<int64_t>() < 1 || input.inputs[6].GetValue<int64_t>() > 100)) {
		throw InvalidInputException("Invalid Vortex compaction thresholds");
	}
	if (OP == Operation::ENABLE_MAINTENANCE &&
	    (input.inputs[1].GetValue<int64_t>() < 1 || input.inputs[1].GetValue<int64_t>() > 16 ||
	     input.inputs[2].GetValue<int64_t>() < 1 || input.inputs[2].GetValue<int64_t>() > 64 * 1024 * 1024)) {
		throw InvalidInputException("Invalid Vortex maintenance count or byte budget");
	}
	if (OP == Operation::SOURCE_RETENTION_POLICY &&
	    (input.inputs[2].GetValue<int64_t>() < 1 || input.inputs[2].GetValue<int64_t>() > 100000 ||
	     input.inputs[3].GetValue<int64_t>() < 0 || input.inputs[3].GetValue<int64_t>() > 315360000)) {
		throw InvalidInputException("Invalid Vortex source retention policy");
	}
	if (OP == Operation::COLLECT_SOURCES &&
	    (input.inputs[1].GetValue<int64_t>() < 1 || input.inputs[1].GetValue<int64_t>() > 16 ||
	     input.inputs[2].GetValue<int64_t>() < 1 || input.inputs[2].GetValue<int64_t>() > 64 * 1024 * 1024)) {
		throw InvalidInputException("Invalid Vortex source collection budgets");
	}
	auto bind = make_uniq<OptimizerBind>();
	bind->operation = OP;
	bind->args = input.inputs;
	if (OP == Operation::STATUS) {
		names = {"table_uuid",
		         "schema_name",
		         "table_name",
		         "index_name",
		         "state",
		         "enabled",
		         "run_id",
		         "expected_revision",
		         "candidate_generation",
		         "published_revision",
		         "inline_rows",
		         "inline_bytes",
		         "unindexed_rows",
		         "unindexed_bytes",
		         "error",
		         "run_count",
		         "running",
		         "max_inline_rows",
		         "max_inline_bytes",
		         "max_unindexed_rows",
		         "max_unindexed_bytes",
		         "task_kind",
		         "selected_generations",
		         "compaction_enabled",
		         "min_deleted_rows",
		         "min_deleted_percent",
		         "compaction_candidate",
		         "reclaimable_rows",
		         "remaining_rows",
		         "compaction_reason"};
		types = {LogicalType::VARCHAR,
		         LogicalType::VARCHAR,
		         LogicalType::VARCHAR,
		         LogicalType::VARCHAR,
		         LogicalType::VARCHAR,
		         LogicalType::BOOLEAN,
		         LogicalType::VARCHAR,
		         LogicalType::UBIGINT,
		         LogicalType::VARCHAR,
		         LogicalType::UBIGINT,
		         LogicalType::UBIGINT,
		         LogicalType::UBIGINT,
		         LogicalType::UBIGINT,
		         LogicalType::UBIGINT,
		         LogicalType::VARCHAR,
		         LogicalType::UBIGINT,
		         LogicalType::BOOLEAN,
		         LogicalType::UBIGINT,
		         LogicalType::UBIGINT,
		         LogicalType::UBIGINT,
		         LogicalType::UBIGINT,
		         LogicalType::VARCHAR,
		         LogicalType::LIST(LogicalType::VARCHAR),
		         LogicalType::BOOLEAN,
		         LogicalType::UBIGINT,
		         LogicalType::UBIGINT,
		         LogicalType::VARCHAR,
		         LogicalType::UBIGINT,
		         LogicalType::UBIGINT,
		         LogicalType::VARCHAR};
	} else if (OP == Operation::MAINTENANCE_STATUS) {
		names = {"state",
		         "enabled",
		         "running",
		         "run_id",
		         "run_count",
		         "max_objects",
		         "max_bytes",
		         "reconciled_builds",
		         "collected_builds",
		         "collected_generations",
		         "collected_bytes",
		         "remaining_builds",
		         "remaining_generations",
		         "error"};
		types = {LogicalType::VARCHAR, LogicalType::BOOLEAN, LogicalType::BOOLEAN, LogicalType::VARCHAR,
		         LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::UBIGINT,
		         LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::UBIGINT,
		         LogicalType::UBIGINT, LogicalType::VARCHAR};
	} else if (OP == Operation::SOURCE_RETENTION_STATUS) {
		names = {"enabled",           "keep_snapshots",         "min_age_seconds",    "expired_snapshots",
		         "collected_files",   "collected_bytes",        "eligible_snapshots", "scheduled_files",
		         "pending_deletions", "blocked_by_transactions"};
		types = {LogicalType::BOOLEAN, LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::UBIGINT,
		         LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::UBIGINT,
		         LogicalType::UBIGINT, LogicalType::BOOLEAN};
	} else if (OP == Operation::COLLECT_SOURCES) {
		names = {"expired_snapshots", "collected_files", "collected_bytes", "blocked_by_transactions"};
		types = {LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::BOOLEAN};
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
		if ((bind.operation == Operation::SOURCE_RETENTION_POLICY || bind.operation == Operation::COLLECT_SOURCES) &&
		    !context.transaction.IsAutoCommit()) {
			throw InvalidInputException("Vortex source retention operations require autocommit mode");
		}
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
		case Operation::ENABLE_MAINTENANCE:
			DuckLakeVortexOptimizer::EnableMaintenance(context, text(0), args[1].GetValue<idx_t>(),
			                                           args[2].GetValue<idx_t>());
			result = "enabled";
			break;
		case Operation::RESUME_MAINTENANCE:
			DuckLakeVortexOptimizer::ResumeMaintenance(context, text(0));
			result = "resumed";
			break;
		case Operation::CANCEL_MAINTENANCE:
			DuckLakeVortexOptimizer::CancelMaintenance(context, text(0));
			result = "cancellation_requested";
			break;
		case Operation::MAINTENANCE_STATUS:
			state.rows = DuckLakeVortexOptimizer::MaintenanceStatus(context, text(0));
			break;
		case Operation::SOURCE_RETENTION_POLICY:
			DuckLakeVortexOptimizer::ConfigureSourceRetention(context, text(0), args[1].GetValue<bool>(),
			                                                  args[2].GetValue<idx_t>(), args[3].GetValue<idx_t>());
			result = "configured";
			break;
		case Operation::SOURCE_RETENTION_STATUS:
			state.rows = DuckLakeVortexSourceRetention::Status(context, text(0));
			break;
		case Operation::COLLECT_SOURCES: {
			auto collected = DuckLakeVortexSourceRetention::Collect(context, text(0), args[1].GetValue<idx_t>(),
			                                                        args[2].GetValue<idx_t>());
			state.rows = {{Value::UBIGINT(collected.expired), Value::UBIGINT(collected.files),
			               Value::UBIGINT(collected.bytes), Value::BOOLEAN(collected.blocked)}};
			break;
		}
		case Operation::WRITE_BUDGET: {
			DuckLakeVortexWriteBudget budget {args[4].GetValue<idx_t>(), args[5].GetValue<idx_t>(),
			                                  args[6].GetValue<idx_t>(), args[7].GetValue<idx_t>()};
			DuckLakeVortexOptimizer::SetWriteBudget(context, text(0), text(1), text(2), text(3), budget);
			result = "configured";
			break;
		}
		case Operation::COMPACTION_POLICY:
			DuckLakeVortexOptimizer::ConfigureCompaction(context, text(0), text(1), text(2), text(3),
			                                             args[4].GetValue<bool>(), args[5].GetValue<idx_t>(),
			                                             args[6].GetValue<idx_t>());
			result = "configured";
			break;
		}
		if (bind.operation != Operation::STATUS && bind.operation != Operation::MAINTENANCE_STATUS &&
		    bind.operation != Operation::SOURCE_RETENTION_STATUS && bind.operation != Operation::COLLECT_SOURCES) {
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
	loader.RegisterFunction(
	    TableFunction("ducklake_set_vortex_source_retention",
	                  {LogicalType::VARCHAR, LogicalType::BOOLEAN, LogicalType::BIGINT, LogicalType::BIGINT}, Execute,
	                  Bind<Operation::SOURCE_RETENTION_POLICY>, Init));
	loader.RegisterFunction(TableFunction("ducklake_vortex_source_retention_status", {LogicalType::VARCHAR}, Execute,
	                                      Bind<Operation::SOURCE_RETENTION_STATUS>, Init));
	loader.RegisterFunction(TableFunction("ducklake_collect_vortex_sources",
	                                      {LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::BIGINT}, Execute,
	                                      Bind<Operation::COLLECT_SOURCES>, Init));
	loader.RegisterFunction(
	    TableFunction("ducklake_set_vortex_data_compaction_policy",
	                  {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                   LogicalType::BOOLEAN, LogicalType::BIGINT, LogicalType::BIGINT},
	                  Execute, Bind<Operation::COMPACTION_POLICY>, Init));
	loader.RegisterFunction(TableFunction("ducklake_enable_vortex_maintenance",
	                                      {LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::BIGINT}, Execute,
	                                      Bind<Operation::ENABLE_MAINTENANCE>, Init));
	loader.RegisterFunction(TableFunction("ducklake_resume_vortex_maintenance", {LogicalType::VARCHAR}, Execute,
	                                      Bind<Operation::RESUME_MAINTENANCE>, Init));
	loader.RegisterFunction(TableFunction("ducklake_cancel_vortex_maintenance", {LogicalType::VARCHAR}, Execute,
	                                      Bind<Operation::CANCEL_MAINTENANCE>, Init));
	loader.RegisterFunction(TableFunction("ducklake_vortex_maintenance_status", {LogicalType::VARCHAR}, Execute,
	                                      Bind<Operation::MAINTENANCE_STATUS>, Init));
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
	loader.RegisterFunction(
	    TableFunction("ducklake_set_vortex_write_budget",
	                  {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                   LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT},
	                  Execute, Bind<Operation::WRITE_BUDGET>, Init));
}
} // namespace duckdb
