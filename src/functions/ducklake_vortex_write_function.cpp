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
struct RequestBind final : public TableFunctionData {
	vector<Value> args;
	bool advance;
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
		if (input.inputs.size() == 8) {
			result->args[6] = input.inputs[6].CastAs(context, LogicalType::LIST(LogicalType::BIGINT));
		}
		types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
		         LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::UBIGINT};
		names = {"table_uuid", "request_id", "payload_hash", "row_count", "snapshot_id", "request_epoch"};
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
		auto result = args.size() == 8
		                  ? DuckLakeVortexWriteBatch::Mutate(
		                        owner, StringValue::Get(args[0]), StringValue::Get(args[1]), StringValue::Get(args[2]),
		                        StringValue::Get(args[3]), StringValue::Get(args[4]), args[5], args[6],
		                        args[7].GetValue<uint64_t>(), {}, context)
		                  : DuckLakeVortexWriteBatch::Append(owner, StringValue::Get(args[0]),
		                                                     StringValue::Get(args[1]), StringValue::Get(args[2]),
		                                                     StringValue::Get(args[3]), StringValue::Get(args[4]),
		                                                     args[5], args[6].GetValue<uint64_t>(), {}, context);
		output.SetValue(0, 0, Value(result.table_uuid));
		output.SetValue(1, 0, Value(result.request_id));
		output.SetValue(2, 0, Value(result.payload_hash));
		output.SetValue(3, 0, Value::UBIGINT(result.row_count));
		output.SetValue(4, 0, Value::UBIGINT(result.snapshot_id));
		output.SetValue(5, 0, Value::UBIGINT(result.request_epoch));
	}
	output.SetCardinality(1);
	state.finished = true;
}

unique_ptr<FunctionData> BindRequests(TableFunctionBindInput &input, vector<LogicalType> &types, vector<string> &names,
                                      bool advance) {
	for (auto &arg : input.inputs) {
		if (arg.IsNull()) {
			throw InvalidInputException("Vortex request lifecycle arguments cannot be NULL");
		}
	}
	auto result = make_uniq<RequestBind>();
	result->args = input.inputs;
	result->advance = advance;
	types = {LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::UBIGINT};
	names = advance ? vector<string> {"table_uuid", "previous_epoch", "request_epoch", "expired_requests"}
	                : vector<string> {"table_uuid", "request_epoch", "retained_requests", "catalog_requests"};
	return std::move(result);
}

unique_ptr<FunctionData> BindRequestStatus(ClientContext &, TableFunctionBindInput &input, vector<LogicalType> &types,
                                           vector<string> &names) {
	return BindRequests(input, types, names, false);
}

unique_ptr<FunctionData> BindRequestAdvance(ClientContext &, TableFunctionBindInput &input, vector<LogicalType> &types,
                                            vector<string> &names) {
	return BindRequests(input, types, names, true);
}

void ExecuteRequests(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &state = input.global_state->Cast<WriteState>();
	if (state.finished) {
		return;
	}
	if (!context.transaction.IsAutoCommit()) {
		throw InvalidInputException("Vortex request lifecycle owns its transaction; call it in autocommit mode");
	}
	auto &bind = input.bind_data->Cast<RequestBind>();
	auto &args = bind.args;
	Connection owner(*context.db);
	if (bind.advance) {
		auto result = DuckLakeVortexWriteBatch::AdvanceRequestEpoch(
		    owner, StringValue::Get(args[0]), StringValue::Get(args[1]), StringValue::Get(args[2]),
		    StringValue::Get(args[3]), args[4].GetValue<uint64_t>(), {}, context);
		output.SetValue(0, 0, Value(result.table_uuid));
		output.SetValue(1, 0, Value::UBIGINT(result.previous_epoch));
		output.SetValue(2, 0, Value::UBIGINT(result.request_epoch));
		output.SetValue(3, 0, Value::UBIGINT(result.expired_requests));
	} else {
		auto result =
		    DuckLakeVortexWriteBatch::RequestStatus(owner, StringValue::Get(args[0]), StringValue::Get(args[1]),
		                                            StringValue::Get(args[2]), StringValue::Get(args[3]));
		output.SetValue(0, 0, Value(result.table_uuid));
		output.SetValue(1, 0, Value::UBIGINT(result.request_epoch));
		output.SetValue(2, 0, Value::UBIGINT(result.retained_requests));
		output.SetValue(3, 0, Value::UBIGINT(result.catalog_requests));
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
	     DuckLakeVortexWriteBatch::RowsType(), LogicalType::LIST(LogicalType::BIGINT), LogicalType::UBIGINT},
	    ExecuteWrite, BindWrite, InitWrite));
	loader.RegisterFunction(
	    TableFunction("ducklake_initialize_vortex_writes", {LogicalType::VARCHAR}, ExecuteWrite, BindWrite, InitWrite));
	loader.RegisterFunction(
	    TableFunction("ducklake_write_vortex_batch",
	                  {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                   LogicalType::VARCHAR, DuckLakeVortexWriteBatch::RowsType(), LogicalType::UBIGINT},
	                  ExecuteWrite, BindWrite, InitWrite));
	loader.RegisterFunction(
	    TableFunction("ducklake_vortex_request_status",
	                  {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                  ExecuteRequests, BindRequestStatus, InitWrite));
	loader.RegisterFunction(TableFunction(
	    "ducklake_advance_vortex_request_epoch",
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::UBIGINT},
	    ExecuteRequests, BindRequestAdvance, InitWrite));
}
} // namespace duckdb
