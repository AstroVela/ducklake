#include "storage/ducklake_vortex_write.hpp"

#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/transaction/transaction_context.hpp"

namespace duckdb {
namespace {
struct WriteBind final : public TableFunctionData {
	vector<Value> args;
};
struct WriteState final : public GlobalTableFunctionState {
	bool finished = false;
};

unique_ptr<FunctionData> BindWrite(ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &types,
                                   vector<string> &names) {
	for (auto &arg : input.inputs) {
		if (arg.IsNull()) {
			throw InvalidInputException("Vortex WriteBatch arguments cannot be NULL");
		}
	}
	auto result = make_uniq<WriteBind>();
	result->args = input.inputs;
	if (input.inputs.size() == 1 || input.inputs.size() == 4) {
		types = {LogicalType::BOOLEAN};
		names = {input.inputs.size() == 1 ? "initialized" : "managed"};
	} else {
		result->args[5] = input.inputs[5].CastAs(context, DuckLakeVortexWriteBatch::RowsType());
		if (input.inputs.size() == 7) {
			result->args[6] = input.inputs[6].CastAs(context, LogicalType::LIST(LogicalType::BIGINT));
		}
		types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::UBIGINT,
		         LogicalType::UBIGINT};
		names = {"table_uuid", "request_id", "payload_hash", "row_count", "snapshot_id"};
	}
	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> InitWrite(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<WriteState>();
}

void ExecuteWrite(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &state = input.global_state->Cast<WriteState>();
	if (state.finished) {
		return;
	}
	if (!context.transaction.IsAutoCommit()) {
		throw InvalidInputException("Vortex WriteBatch owns its transaction; call it in autocommit mode");
	}
	auto &args = input.bind_data->Cast<WriteBind>().args;
	Connection owner(*context.db);
	if (args.size() == 1) {
		DuckLakeVortexWriteBatch::Initialize(owner, StringValue::Get(args[0]));
		output.SetValue(0, 0, Value::BOOLEAN(true));
	} else if (args.size() == 4) {
		DuckLakeVortexWriteBatch::Manage(owner, StringValue::Get(args[0]), StringValue::Get(args[1]),
		                                 StringValue::Get(args[2]), StringValue::Get(args[3]));
		output.SetValue(0, 0, Value::BOOLEAN(true));
	} else {
		auto result =
		    args.size() == 7
		        ? DuckLakeVortexWriteBatch::Mutate(owner, StringValue::Get(args[0]), StringValue::Get(args[1]),
		                                           StringValue::Get(args[2]), StringValue::Get(args[3]),
		                                           StringValue::Get(args[4]), args[5], args[6], {}, context)
		        : DuckLakeVortexWriteBatch::Append(owner, StringValue::Get(args[0]), StringValue::Get(args[1]),
		                                           StringValue::Get(args[2]), StringValue::Get(args[3]),
		                                           StringValue::Get(args[4]), args[5], {}, context);
		output.SetValue(0, 0, Value(result.table_uuid));
		output.SetValue(1, 0, Value(result.request_id));
		output.SetValue(2, 0, Value(result.payload_hash));
		output.SetValue(3, 0, Value::UBIGINT(result.row_count));
		output.SetValue(4, 0, Value::UBIGINT(result.snapshot_id));
	}
	output.SetCardinality(1);
	state.finished = true;
}
} // namespace

void RegisterDuckLakeVortexWrite(ExtensionLoader &loader) {
	loader.RegisterFunction(
	    TableFunction("ducklake_manage_vortex_writes",
	                  {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                  ExecuteWrite, BindWrite, InitWrite));
	loader.RegisterFunction(TableFunction(
	    "ducklake_mutate_vortex_batch",
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	     DuckLakeVortexWriteBatch::RowsType(), LogicalType::LIST(LogicalType::BIGINT)},
	    ExecuteWrite, BindWrite, InitWrite));
	loader.RegisterFunction(
	    TableFunction("ducklake_initialize_vortex_writes", {LogicalType::VARCHAR}, ExecuteWrite, BindWrite, InitWrite));
	loader.RegisterFunction(
	    TableFunction("ducklake_write_vortex_batch",
	                  {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                   LogicalType::VARCHAR, DuckLakeVortexWriteBatch::RowsType()},
	                  ExecuteWrite, BindWrite, InitWrite));
}
} // namespace duckdb
