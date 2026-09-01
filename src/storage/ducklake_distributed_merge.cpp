#ifdef DUCKLAKE_VANE_DISTRIBUTED

#include "storage/ducklake_distributed_merge.hpp"

#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_delete.hpp"
#include "storage/ducklake_distributed_write.hpp"
#include "storage/ducklake_insert.hpp"
#include "storage/ducklake_schema_entry.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_transaction.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/execution/distributed/copy_finalize.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/operator/persistent/physical_copy_to_file.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/interrupt.hpp"
#include "duckdb/parser/statement/merge_into_statement.hpp"

namespace duckdb {

namespace {

static constexpr uint32_t DUCKLAKE_MERGE_PROTOCOL_VERSION = 1;
static const string DUCKLAKE_MERGE_FRAGMENT_CODEC = "ducklake.merge-fragment";
static const DistributedPayloadCodec DUCKLAKE_ROW_DELTA_FRAGMENT_CODEC {"ducklake.row-delta-fragment", 1};

struct DuckLakeDistributedMergeActionBind {
	MergeActionCondition match_condition = MergeActionCondition::WHEN_MATCHED;
	MergeActionType action_type = MergeActionType::MERGE_DO_NOTHING;
	unique_ptr<Expression> condition;
	vector<unique_ptr<Expression>> expressions;
	vector<unique_ptr<Expression>> projections;
	vector<LogicalType> copy_types;
	string worker_bind_data;
	optional_idx update_row_id_index;
	optional_idx update_file_path_index;
	optional_idx update_row_position_index;
};

struct DuckLakeDistributedMergeBind {
	vector<LogicalType> input_types;
	idx_t row_id_index = DConstants::INVALID_INDEX;
	optional_idx source_marker;
	vector<DuckLakeDistributedMergeActionBind> actions;
};

struct DuckLakeEmbeddedMergeFragment {
	idx_t action_index = DConstants::INVALID_INDEX;
	DistributedWriteFragment fragment;
};

struct DuckLakeDistributedMergeResult {
	vector<DuckLakeDistributedRowDeltaResult> actions;
	unordered_set<string> selected_artifact_paths;
	idx_t affected_rows = 0;
};

static idx_t CheckedAdd(idx_t left, idx_t right, const string &description) {
	if (right > NumericLimits<idx_t>::Maximum() - left) {
		throw InvalidInputException("DuckLake distributed MERGE %s overflow", description);
	}
	return left + right;
}

static string BytesFromStream(MemoryStream &stream) {
	return string(reinterpret_cast<const char *>(stream.GetData()), stream.GetPosition());
}

static MemoryStream StreamFromBytes(const string &bytes) {
	MemoryStream stream(Allocator::DefaultAllocator(), bytes.size());
	stream.WriteData(reinterpret_cast<const_data_ptr_t>(bytes.data()), bytes.size());
	stream.Rewind();
	return stream;
}

static bool IsWriteAction(MergeActionType action_type) {
	return action_type == MergeActionType::MERGE_INSERT || action_type == MergeActionType::MERGE_UPDATE ||
	       action_type == MergeActionType::MERGE_DELETE;
}

static DuckLakeDistributedRowDeltaKind RowDeltaKind(MergeActionType action_type) {
	switch (action_type) {
	case MergeActionType::MERGE_INSERT:
		return DuckLakeDistributedRowDeltaKind::MERGE_INSERT;
	case MergeActionType::MERGE_UPDATE:
		return DuckLakeDistributedRowDeltaKind::UPDATE;
	case MergeActionType::MERGE_DELETE:
		return DuckLakeDistributedRowDeltaKind::DELETE;
	default:
		throw InternalException("DuckLake MERGE action is not a row-delta write");
	}
}

static idx_t OptionalIndexValue(const optional_idx &index) {
	return index.IsValid() ? index.GetIndex() : DConstants::INVALID_INDEX;
}

static optional_idx ReadOptionalIndex(Deserializer &deserializer, field_id_t field_id, const char *name) {
	auto value = deserializer.ReadProperty<idx_t>(field_id, name);
	if (value == DConstants::INVALID_INDEX) {
		return optional_idx();
	}
	return optional_idx(value);
}

static void SerializeMergeAction(Serializer &serializer, const DuckLakeDistributedMergeActionBind &action) {
	serializer.WriteProperty(1, "match_condition", static_cast<uint8_t>(action.match_condition));
	serializer.WriteProperty(2, "action_type", static_cast<uint8_t>(action.action_type));
	serializer.WritePropertyWithDefault<unique_ptr<Expression>>(3, "condition", action.condition);
	serializer.WritePropertyWithDefault<vector<unique_ptr<Expression>>>(4, "expressions", action.expressions);
	serializer.WritePropertyWithDefault<vector<unique_ptr<Expression>>>(5, "projections", action.projections);
	serializer.WriteProperty(6, "copy_types", action.copy_types);
	serializer.WriteProperty(7, "worker_bind_data", action.worker_bind_data);
	serializer.WriteProperty(8, "update_row_id_index", OptionalIndexValue(action.update_row_id_index));
	serializer.WriteProperty(9, "update_file_path_index", OptionalIndexValue(action.update_file_path_index));
	serializer.WriteProperty(10, "update_row_position_index", OptionalIndexValue(action.update_row_position_index));
}

static DuckLakeDistributedMergeActionBind DeserializeMergeAction(Deserializer &deserializer) {
	DuckLakeDistributedMergeActionBind result;
	auto match_condition = deserializer.ReadProperty<uint8_t>(1, "match_condition");
	auto action_type = deserializer.ReadProperty<uint8_t>(2, "action_type");
	if (match_condition > static_cast<uint8_t>(MergeActionCondition::WHEN_NOT_MATCHED_BY_TARGET) ||
	    action_type > static_cast<uint8_t>(MergeActionType::MERGE_ERROR)) {
		throw SerializationException("DuckLake distributed MERGE action has an invalid kind");
	}
	result.match_condition = static_cast<MergeActionCondition>(match_condition);
	result.action_type = static_cast<MergeActionType>(action_type);
	deserializer.ReadPropertyWithDefault<unique_ptr<Expression>>(3, "condition", result.condition);
	deserializer.ReadPropertyWithDefault<vector<unique_ptr<Expression>>>(4, "expressions", result.expressions);
	deserializer.ReadPropertyWithDefault<vector<unique_ptr<Expression>>>(5, "projections", result.projections);
	result.copy_types = deserializer.ReadProperty<vector<LogicalType>>(6, "copy_types");
	result.worker_bind_data = deserializer.ReadProperty<string>(7, "worker_bind_data");
	result.update_row_id_index = ReadOptionalIndex(deserializer, 8, "update_row_id_index");
	result.update_file_path_index = ReadOptionalIndex(deserializer, 9, "update_file_path_index");
	result.update_row_position_index = ReadOptionalIndex(deserializer, 10, "update_row_position_index");
	return result;
}

static void ValidateMergeAction(const DuckLakeDistributedMergeActionBind &action,
                                const vector<LogicalType> &input_types) {
	if (IsWriteAction(action.action_type) != !action.worker_bind_data.empty()) {
		throw SerializationException("DuckLake distributed MERGE action has an invalid worker bind");
	}
	if (action.action_type == MergeActionType::MERGE_INSERT) {
		if (action.expressions.empty() || action.copy_types.empty() || action.update_row_id_index.IsValid() ||
		    action.update_file_path_index.IsValid() || action.update_row_position_index.IsValid()) {
			throw SerializationException("DuckLake distributed MERGE INSERT action has an invalid shape");
		}
		return;
	}
	if (action.action_type == MergeActionType::MERGE_UPDATE) {
		if (action.expressions.empty() || action.copy_types.empty() || !action.update_row_id_index.IsValid() ||
		    !action.update_file_path_index.IsValid() || !action.update_row_position_index.IsValid() ||
		    action.update_row_id_index.GetIndex() >= input_types.size() ||
		    action.update_file_path_index.GetIndex() >= input_types.size() ||
		    action.update_row_position_index.GetIndex() >= input_types.size() ||
		    input_types[action.update_row_id_index.GetIndex()] != LogicalType::BIGINT ||
		    input_types[action.update_file_path_index.GetIndex()] != LogicalType::VARCHAR ||
		    input_types[action.update_row_position_index.GetIndex()] != LogicalType::BIGINT) {
			throw SerializationException("DuckLake distributed MERGE UPDATE action has an invalid shape");
		}
		return;
	}
	if (action.action_type == MergeActionType::MERGE_DELETE) {
		if (!action.expressions.empty() || !action.projections.empty() || !action.copy_types.empty() ||
		    action.update_row_id_index.IsValid() || action.update_file_path_index.IsValid() ||
		    action.update_row_position_index.IsValid()) {
			throw SerializationException("DuckLake distributed MERGE DELETE action has an invalid shape");
		}
		return;
	}
	if (!action.projections.empty() || !action.copy_types.empty() || action.update_row_id_index.IsValid() ||
	    action.update_file_path_index.IsValid() || action.update_row_position_index.IsValid()) {
		throw SerializationException("DuckLake distributed MERGE non-write action has an invalid shape");
	}
	if (action.action_type == MergeActionType::MERGE_DO_NOTHING && !action.expressions.empty()) {
		throw SerializationException("DuckLake distributed MERGE DO NOTHING action has expressions");
	}
}

static string SerializeMergeBind(const DuckLakeDistributedMergeBind &bind) {
	if (bind.input_types.empty() || bind.row_id_index >= bind.input_types.size() || bind.actions.empty()) {
		throw InternalException("DuckLake distributed MERGE bind is incomplete");
	}
	MemoryStream stream(Allocator::DefaultAllocator());
	BinarySerializer serializer(stream);
	serializer.Begin();
	serializer.WriteProperty(1, "input_types", bind.input_types);
	serializer.WriteProperty(2, "row_id_index", bind.row_id_index);
	serializer.WriteProperty(3, "source_marker", OptionalIndexValue(bind.source_marker));
	serializer.WriteList(4, "actions", bind.actions.size(), [&](Serializer::List &list, idx_t index) {
		list.WriteObject([&](Serializer &object) { SerializeMergeAction(object, bind.actions[index]); });
	});
	serializer.End();
	return BytesFromStream(stream);
}

static DuckLakeDistributedMergeBind DeserializeMergeBind(ClientContext &context, const string &bytes) {
	if (bytes.empty()) {
		throw SerializationException("DuckLake distributed MERGE bind is empty");
	}
	auto stream = StreamFromBytes(bytes);
	BinaryDeserializer deserializer(stream);
	deserializer.Set<ClientContext &>(context);
	deserializer.Begin();
	DuckLakeDistributedMergeBind result;
	result.input_types = deserializer.ReadProperty<vector<LogicalType>>(1, "input_types");
	result.row_id_index = deserializer.ReadProperty<idx_t>(2, "row_id_index");
	result.source_marker = ReadOptionalIndex(deserializer, 3, "source_marker");
	deserializer.ReadList(4, "actions", [&](Deserializer::List &list, idx_t) {
		list.ReadObject([&](Deserializer &object) { result.actions.push_back(DeserializeMergeAction(object)); });
	});
	deserializer.End();
	if (result.input_types.empty() || result.row_id_index >= result.input_types.size() || result.actions.empty() ||
	    (result.source_marker.IsValid() && result.source_marker.GetIndex() >= result.input_types.size())) {
		throw SerializationException("DuckLake distributed MERGE bind has an invalid input shape");
	}
	for (const auto &action : result.actions) {
		ValidateMergeAction(action, result.input_types);
	}
	return result;
}

static DistributedExtensionWriteInfo RowDeltaInfo(const DistributedExtensionWriteInfo &merge_info,
                                                  const string &worker_bind_data) {
	DistributedExtensionWriteInfo result;
	result.capability = merge_info.capability;
	result.mode = DistributedWriteMode::CALLBACK;
	result.fragment_codec = DUCKLAKE_ROW_DELTA_FRAGMENT_CODEC;
	result.worker_bind_data = worker_bind_data;
	return result;
}

static string SerializeEmbeddedFragments(const vector<DuckLakeEmbeddedMergeFragment> &fragments) {
	MemoryStream stream(Allocator::DefaultAllocator());
	BinarySerializer serializer(stream);
	serializer.Begin();
	serializer.WriteList(1, "fragments", fragments.size(), [&](Serializer::List &list, idx_t index) {
		list.WriteObject([&](Serializer &object) {
			object.WriteProperty(1, "action_index", fragments[index].action_index);
			object.WriteObject(2, "fragment", [&](Serializer &fragment_serializer) {
				fragments[index].fragment.Serialize(fragment_serializer);
			});
		});
	});
	serializer.End();
	return BytesFromStream(stream);
}

static vector<DuckLakeEmbeddedMergeFragment> DeserializeEmbeddedFragments(const string &bytes) {
	if (bytes.empty()) {
		throw SerializationException("DuckLake distributed MERGE fragment payload is empty");
	}
	auto stream = StreamFromBytes(bytes);
	BinaryDeserializer deserializer(stream);
	deserializer.Begin();
	vector<DuckLakeEmbeddedMergeFragment> result;
	deserializer.ReadList(1, "fragments", [&](Deserializer::List &list, idx_t) {
		DuckLakeEmbeddedMergeFragment entry;
		list.ReadObject([&](Deserializer &object) {
			entry.action_index = object.ReadProperty<idx_t>(1, "action_index");
			object.ReadObject(2, "fragment", [&](Deserializer &fragment_deserializer) {
				entry.fragment = DistributedWriteFragment::Deserialize(fragment_deserializer);
			});
		});
		result.push_back(std::move(entry));
	});
	deserializer.End();
	return result;
}

class DuckLakeDistributedMergeGlobalState final : public DistributedWriteGlobalState {
public:
	DuckLakeDistributedMergeGlobalState(ClientContext &context, DuckLakeDistributedMergeBind bind_p,
	                                    const DistributedExtensionWriteInfo &info,
	                                    const DistributedWriteTaskContext &task)
	    : bind(std::move(bind_p)) {
		auto callbacks = DuckLakeDistributedRowDeltaCallbacks();
		sub_infos.resize(bind.actions.size());
		sub_states.resize(bind.actions.size());
		for (idx_t index = 0; index < bind.actions.size(); index++) {
			const auto &action = bind.actions[index];
			if (!IsWriteAction(action.action_type)) {
				continue;
			}
			sub_infos[index] = RowDeltaInfo(info, action.worker_bind_data);
			sub_states[index] = callbacks.initialize_global(context, sub_infos[index], task);
		}
	}

	DuckLakeDistributedMergeBind bind;
	vector<DistributedExtensionWriteInfo> sub_infos;
	vector<unique_ptr<DistributedWriteGlobalState>> sub_states;
	mutex seen_update_rows_lock;
	unordered_map<string, unordered_set<idx_t>> seen_update_rows;
};

struct DuckLakeDistributedMergeActionLocalState {
	unique_ptr<ExpressionExecutor> condition_executor;
	unique_ptr<ExpressionExecutor> expression_executor;
	unique_ptr<ExpressionExecutor> projection_executor;
	unique_ptr<DistributedWriteLocalState> sub_state;
	DataChunk expression_chunk;
	DataChunk value_chunk;
	DataChunk projected_chunk;
	DataChunk cast_chunk;
	DataChunk callback_chunk;
};

class DuckLakeDistributedMergeLocalState final : public DistributedWriteLocalState {
public:
	DuckLakeDistributedMergeLocalState(ExecutionContext &context, const DistributedExtensionWriteInfo &,
	                                   const DistributedWriteTaskContext &task,
	                                   DuckLakeDistributedMergeGlobalState &global_state) {
		auto callbacks = DuckLakeDistributedRowDeltaCallbacks();
		for (idx_t index = 0; index < global_state.bind.actions.size(); index++) {
			const auto &action = global_state.bind.actions[index];
			auto state = make_uniq<DuckLakeDistributedMergeActionLocalState>();
			if (action.condition) {
				state->condition_executor = make_uniq<ExpressionExecutor>(context.client, *action.condition);
			}
			if (!action.expressions.empty()) {
				state->expression_executor = make_uniq<ExpressionExecutor>(context.client, action.expressions);
				vector<LogicalType> expression_types;
				for (const auto &expression : action.expressions) {
					expression_types.push_back(expression->return_type);
				}
				state->expression_chunk.Initialize(context.client, expression_types);
				if (action.action_type == MergeActionType::MERGE_UPDATE) {
					expression_types.push_back(LogicalType::BIGINT);
					state->value_chunk.Initialize(context.client, expression_types);
				}
			}
			if (!action.projections.empty()) {
				state->projection_executor = make_uniq<ExpressionExecutor>(context.client, action.projections);
				vector<LogicalType> projection_types;
				for (const auto &projection : action.projections) {
					projection_types.push_back(projection->return_type);
				}
				state->projected_chunk.Initialize(context.client, projection_types);
			}
			if (!action.copy_types.empty()) {
				state->cast_chunk.Initialize(context.client, action.copy_types);
				if (action.action_type == MergeActionType::MERGE_UPDATE) {
					auto callback_types = action.copy_types;
					callback_types.push_back(LogicalType::VARCHAR);
					callback_types.push_back(LogicalType::BIGINT);
					state->callback_chunk.Initialize(context.client, callback_types);
				}
			}
			if (IsWriteAction(action.action_type)) {
				state->sub_state = callbacks.initialize_local(context, global_state.sub_infos[index], task,
				                                              *global_state.sub_states[index]);
			}
			actions.push_back(std::move(state));
		}
		for (idx_t index = 0; index < 3; index++) {
			match_selections[index].Initialize(STANDARD_VECTOR_SIZE);
			auto chunk = make_uniq<DataChunk>();
			chunk->Initialize(context.client, global_state.bind.input_types);
			match_chunks.push_back(std::move(chunk));
		}
		selected.Initialize(STANDARD_VECTOR_SIZE);
		remaining.Initialize(STANDARD_VECTOR_SIZE);
		action_chunk.Initialize(context.client, global_state.bind.input_types);
	}

	vector<unique_ptr<DuckLakeDistributedMergeActionLocalState>> actions;
	vector<unique_ptr<DataChunk>> match_chunks;
	DataChunk action_chunk;
	SelectionVector match_selections[3];
	SelectionVector selected;
	SelectionVector remaining;
};

static void ComputeMatches(const DuckLakeDistributedMergeBind &bind, DuckLakeDistributedMergeLocalState &local_state,
                           DataChunk &input, idx_t (&counts)[3]) {
	if (input.ColumnCount() != bind.input_types.size() || bind.row_id_index >= input.ColumnCount()) {
		throw InvalidInputException("DuckLake distributed MERGE worker input has an invalid shape");
	}
	counts[0] = 0;
	counts[1] = 0;
	counts[2] = 0;
	UnifiedVectorFormat row_id_data;
	input.data[bind.row_id_index].ToUnifiedFormat(input.size(), row_id_data);
	if (bind.source_marker.IsValid()) {
		UnifiedVectorFormat source_marker_data;
		input.data[bind.source_marker.GetIndex()].ToUnifiedFormat(input.size(), source_marker_data);
		for (idx_t row = 0; row < input.size(); row++) {
			auto row_id_index = row_id_data.sel->get_index(row);
			auto source_index = source_marker_data.sel->get_index(row);
			if (!source_marker_data.validity.RowIsValid(source_index)) {
				local_state.match_selections[2].set_index(counts[2]++, row);
			} else if (!row_id_data.validity.RowIsValid(row_id_index)) {
				local_state.match_selections[1].set_index(counts[1]++, row);
			} else {
				local_state.match_selections[0].set_index(counts[0]++, row);
			}
		}
	} else {
		for (idx_t row = 0; row < input.size(); row++) {
			auto row_id_index = row_id_data.sel->get_index(row);
			if (row_id_data.validity.RowIsValid(row_id_index)) {
				local_state.match_selections[0].set_index(counts[0]++, row);
			} else {
				local_state.match_selections[1].set_index(counts[1]++, row);
			}
		}
	}
	for (idx_t index = 0; index < 3; index++) {
		if (counts[index] == 0) {
			continue;
		}
		local_state.match_chunks[index]->Reset();
		local_state.match_chunks[index]->Slice(input, local_state.match_selections[index], counts[index]);
	}
}

static void CastForCopy(ClientContext &context, const DuckLakeDistributedMergeActionBind &action,
                        DuckLakeDistributedMergeActionLocalState &local_state, DataChunk &input, DataChunk &result) {
	reference<DataChunk> source = input;
	if (local_state.projection_executor) {
		local_state.projected_chunk.Reset();
		local_state.projection_executor->Execute(input, local_state.projected_chunk);
		source = local_state.projected_chunk;
	}
	if (source.get().ColumnCount() != action.copy_types.size()) {
		throw InvalidInputException("DuckLake distributed MERGE COPY projection has an invalid width");
	}
	result.Reset();
	for (idx_t index = 0; index < action.copy_types.size(); index++) {
		if (source.get().data[index].GetType() == action.copy_types[index]) {
			result.data[index].Reference(source.get().data[index]);
		} else {
			VectorOperations::Cast(context, source.get().data[index], result.data[index], source.get().size());
		}
	}
	result.SetCardinality(source.get().size());
}

static void SinkMergeInsert(ExecutionContext &context, const DistributedWriteTaskContext &task,
                            DuckLakeDistributedMergeGlobalState &global_state,
                            DuckLakeDistributedMergeLocalState &local_state, idx_t action_index, DataChunk &input) {
	auto &action = global_state.bind.actions[action_index];
	auto &action_state = *local_state.actions[action_index];
	if (!action_state.expression_executor) {
		throw InternalException("DuckLake distributed MERGE INSERT expression executor is missing");
	}
	action_state.expression_chunk.Reset();
	action_state.expression_executor->Execute(input, action_state.expression_chunk);
	CastForCopy(context.client, action, action_state, action_state.expression_chunk, action_state.cast_chunk);
	auto callbacks = DuckLakeDistributedRowDeltaCallbacks();
	callbacks.sink(context, global_state.sub_infos[action_index], task, *global_state.sub_states[action_index],
	               *action_state.sub_state, action_state.cast_chunk);
}

static void SinkMergeUpdate(ExecutionContext &context, const DistributedWriteTaskContext &task,
                            DuckLakeDistributedMergeGlobalState &global_state,
                            DuckLakeDistributedMergeLocalState &local_state, idx_t action_index, DataChunk &input) {
	auto &action = global_state.bind.actions[action_index];
	auto &action_state = *local_state.actions[action_index];
	if (!action_state.expression_executor || !action.update_row_id_index.IsValid() ||
	    !action.update_file_path_index.IsValid() || !action.update_row_position_index.IsValid()) {
		throw InternalException("DuckLake distributed MERGE UPDATE state is incomplete");
	}
	SelectionVector selection(input.size());
	idx_t selection_count = 0;
	{
		lock_guard<mutex> guard(global_state.seen_update_rows_lock);
		for (idx_t row = 0; row < input.size(); row++) {
			auto file_value = input.GetValue(action.update_file_path_index.GetIndex(), row);
			auto position_value = input.GetValue(action.update_row_position_index.GetIndex(), row);
			if (file_value.IsNull() || position_value.IsNull()) {
				throw InvalidInputException("DuckLake distributed MERGE UPDATE received a NULL row identifier");
			}
			auto position = position_value.GetValue<int64_t>();
			if (position < 0) {
				throw InvalidInputException("DuckLake distributed MERGE UPDATE received a negative row position");
			}
			auto file_path = file_value.GetValue<string>();
			if (global_state.seen_update_rows[file_path].insert(NumericCast<idx_t>(position)).second) {
				selection.set_index(selection_count++, row);
			}
		}
	}
	if (selection_count == 0) {
		return;
	}
	input.Slice(selection, selection_count);
	action_state.expression_chunk.Reset();
	action_state.expression_executor->Execute(input, action_state.expression_chunk);
	action_state.value_chunk.Reset();
	for (idx_t index = 0; index < action_state.expression_chunk.ColumnCount(); index++) {
		action_state.value_chunk.data[index].Reference(action_state.expression_chunk.data[index]);
	}
	action_state.value_chunk.data.back().Reference(input.data[action.update_row_id_index.GetIndex()]);
	action_state.value_chunk.SetCardinality(input.size());
	CastForCopy(context.client, action, action_state, action_state.value_chunk, action_state.cast_chunk);

	action_state.callback_chunk.Reset();
	for (idx_t index = 0; index < action.copy_types.size(); index++) {
		action_state.callback_chunk.data[index].Reference(action_state.cast_chunk.data[index]);
	}
	action_state.callback_chunk.data[action.copy_types.size()].Reference(
	    input.data[action.update_file_path_index.GetIndex()]);
	action_state.callback_chunk.data[action.copy_types.size() + 1].Reference(
	    input.data[action.update_row_position_index.GetIndex()]);
	action_state.callback_chunk.SetCardinality(input.size());
	auto callbacks = DuckLakeDistributedRowDeltaCallbacks();
	callbacks.sink(context, global_state.sub_infos[action_index], task, *global_state.sub_states[action_index],
	               *action_state.sub_state, action_state.callback_chunk);
}

static void SinkMergeAction(ExecutionContext &context, const DistributedWriteTaskContext &task,
                            DuckLakeDistributedMergeGlobalState &global_state,
                            DuckLakeDistributedMergeLocalState &local_state, idx_t action_index, DataChunk &input) {
	auto &action = global_state.bind.actions[action_index];
	auto &action_state = *local_state.actions[action_index];
	switch (action.action_type) {
	case MergeActionType::MERGE_INSERT:
		SinkMergeInsert(context, task, global_state, local_state, action_index, input);
		return;
	case MergeActionType::MERGE_UPDATE:
		SinkMergeUpdate(context, task, global_state, local_state, action_index, input);
		return;
	case MergeActionType::MERGE_DELETE: {
		auto callbacks = DuckLakeDistributedRowDeltaCallbacks();
		callbacks.sink(context, global_state.sub_infos[action_index], task, *global_state.sub_states[action_index],
		               *action_state.sub_state, input);
		return;
	}
	case MergeActionType::MERGE_DO_NOTHING:
		return;
	case MergeActionType::MERGE_ERROR: {
		string merge_condition = MergeIntoStatement::ActionConditionToString(action.match_condition);
		if (action.condition) {
			merge_condition += " AND " + action.condition->ToString();
		}
		if (action_state.expression_executor) {
			action_state.expression_chunk.Reset();
			action_state.expression_executor->Execute(input, action_state.expression_chunk);
			if (action_state.expression_chunk.ColumnCount() != 0 && action_state.expression_chunk.size() != 0) {
				merge_condition += ": " + action_state.expression_chunk.data[0].GetValue(0).ToString();
			}
		}
		throw ConstraintException("Merge error condition %s", merge_condition);
	}
	default:
		throw InternalException("DuckLake distributed MERGE action is invalid");
	}
}

static void SinkMatchGroup(ExecutionContext &context, const DistributedWriteTaskContext &task,
                           DuckLakeDistributedMergeGlobalState &global_state,
                           DuckLakeDistributedMergeLocalState &local_state, MergeActionCondition match_condition,
                           DataChunk &input) {
	SelectionVector current;
	idx_t current_count = input.size();
	for (idx_t action_index = 0; action_index < global_state.bind.actions.size(); action_index++) {
		auto &action = global_state.bind.actions[action_index];
		if (action.match_condition != match_condition || current_count == 0) {
			continue;
		}
		auto &action_state = *local_state.actions[action_index];
		idx_t selected_count;
		if (action_state.condition_executor) {
			selected_count = action_state.condition_executor->SelectExpression(
			    input, local_state.selected, local_state.remaining, current, current_count);
			if (selected_count == 0) {
				continue;
			}
			current_count -= selected_count;
			current.Initialize(local_state.remaining);
		} else {
			selected_count = current_count;
			local_state.selected.Initialize(current);
			current_count = 0;
		}
		local_state.action_chunk.Reset();
		local_state.action_chunk.Slice(input, local_state.selected, selected_count);
		SinkMergeAction(context, task, global_state, local_state, action_index, local_state.action_chunk);
	}
}

static unique_ptr<DistributedWriteGlobalState> DuckLakeMergeInitializeGlobal(ClientContext &context,
                                                                             const DistributedExtensionWriteInfo &info,
                                                                             const DistributedWriteTaskContext &task) {
	task.Validate();
	auto bind = DeserializeMergeBind(context, info.worker_bind_data);
	return make_uniq<DuckLakeDistributedMergeGlobalState>(context, std::move(bind), info, task);
}

static unique_ptr<DistributedWriteLocalState>
DuckLakeMergeInitializeLocal(ExecutionContext &context, const DistributedExtensionWriteInfo &info,
                             const DistributedWriteTaskContext &task, DistributedWriteGlobalState &global_state_p) {
	auto &global_state = global_state_p.Cast<DuckLakeDistributedMergeGlobalState>();
	return make_uniq<DuckLakeDistributedMergeLocalState>(context, info, task, global_state);
}

static void DuckLakeMergeSink(ExecutionContext &context, const DistributedExtensionWriteInfo &,
                              const DistributedWriteTaskContext &task, DistributedWriteGlobalState &global_state_p,
                              DistributedWriteLocalState &local_state_p, DataChunk &input) {
	auto &global_state = global_state_p.Cast<DuckLakeDistributedMergeGlobalState>();
	auto &local_state = local_state_p.Cast<DuckLakeDistributedMergeLocalState>();
	idx_t match_counts[3];
	ComputeMatches(global_state.bind, local_state, input, match_counts);
	const MergeActionCondition match_conditions[3] = {MergeActionCondition::WHEN_MATCHED,
	                                                  MergeActionCondition::WHEN_NOT_MATCHED_BY_TARGET,
	                                                  MergeActionCondition::WHEN_NOT_MATCHED_BY_SOURCE};
	for (idx_t index = 0; index < 3; index++) {
		if (match_counts[index] == 0) {
			continue;
		}
		SinkMatchGroup(context, task, global_state, local_state, match_conditions[index],
		               *local_state.match_chunks[index]);
	}
}

static void DuckLakeMergeCombine(ExecutionContext &context, const DistributedExtensionWriteInfo &,
                                 const DistributedWriteTaskContext &task, DistributedWriteGlobalState &global_state_p,
                                 DistributedWriteLocalState &local_state_p) {
	auto &global_state = global_state_p.Cast<DuckLakeDistributedMergeGlobalState>();
	auto &local_state = local_state_p.Cast<DuckLakeDistributedMergeLocalState>();
	auto callbacks = DuckLakeDistributedRowDeltaCallbacks();
	for (idx_t index = 0; index < global_state.bind.actions.size(); index++) {
		if (!IsWriteAction(global_state.bind.actions[index].action_type)) {
			continue;
		}
		callbacks.combine(context, global_state.sub_infos[index], task, *global_state.sub_states[index],
		                  *local_state.actions[index]->sub_state);
	}
}

static vector<DistributedWriteFragment> DuckLakeMergeFinalize(ClientContext &context,
                                                              const DistributedExtensionWriteInfo &,
                                                              const DistributedWriteTaskContext &task,
                                                              DistributedWriteGlobalState &global_state_p) {
	auto &global_state = global_state_p.Cast<DuckLakeDistributedMergeGlobalState>();
	auto callbacks = DuckLakeDistributedRowDeltaCallbacks();
	vector<DuckLakeEmbeddedMergeFragment> embedded;
	idx_t row_count = 0;
	idx_t byte_count = 0;
	for (idx_t index = 0; index < global_state.bind.actions.size(); index++) {
		if (!IsWriteAction(global_state.bind.actions[index].action_type)) {
			continue;
		}
		auto fragments =
		    callbacks.finalize(context, global_state.sub_infos[index], task, *global_state.sub_states[index]);
		if (fragments.size() > 1) {
			throw InternalException("DuckLake distributed MERGE action returned multiple worker fragments");
		}
		if (fragments.empty()) {
			continue;
		}
		row_count = CheckedAdd(row_count, fragments[0].row_count, "worker affected row count");
		byte_count = CheckedAdd(byte_count, fragments[0].byte_count, "worker byte count");
		embedded.push_back(DuckLakeEmbeddedMergeFragment {index, std::move(fragments[0])});
	}
	if (row_count == 0) {
		return {};
	}
	DistributedWriteFragment result;
	result.fragment_id = task.query_id + "/" + task.task_attempt_id;
	result.payload = SerializeEmbeddedFragments(embedded);
	result.row_count = row_count;
	result.byte_count = byte_count;
	for (const auto &entry : embedded) {
		for (const auto &artifact : entry.fragment.artifacts) {
			auto outer_artifact = artifact;
			outer_artifact.artifact_id = "action:" + to_string(entry.action_index) + ":" + artifact.artifact_id;
			result.artifacts.push_back(std::move(outer_artifact));
		}
	}
	return {std::move(result)};
}

static DuckLakeDistributedMergeResult
DecodeDistributedMergeResults(ClientContext &context, const DistributedExtensionWriteInfo &info,
                              const vector<DistributedWriteTaskResult> &results, const string &data_path,
                              const string &artifact_path, bool use_deletion_vectors) {
	info.Validate();
	if (info.mode != DistributedWriteMode::CALLBACK ||
	    info.fragment_codec !=
	        DistributedPayloadCodec {DUCKLAKE_MERGE_FRAGMENT_CODEC, DUCKLAKE_MERGE_PROTOCOL_VERSION}) {
		throw InvalidInputException("DuckLake distributed MERGE resolved the wrong worker protocol");
	}
	auto bind = DeserializeMergeBind(context, info.worker_bind_data);
	vector<vector<DistributedWriteTaskResult>> action_results(bind.actions.size());
	set<string> task_attempt_ids;
	set<string> fragment_ids;
	string query_id;
	idx_t outer_affected_rows = 0;

	for (const auto &task_result : results) {
		task_result.Validate();
		if (task_result.capability != info.capability || task_result.fragment_codec != info.fragment_codec) {
			throw InvalidInputException("DuckLake distributed MERGE received a mismatched task result protocol");
		}
		if (query_id.empty()) {
			query_id = task_result.query_id;
		} else if (task_result.query_id != query_id) {
			throw InvalidInputException("DuckLake distributed MERGE received results from multiple queries");
		}
		if (!task_attempt_ids.insert(task_result.task_attempt_id).second) {
			throw InvalidInputException("DuckLake distributed MERGE selected task attempt '%s' more than once",
			                            task_result.task_attempt_id);
		}
		for (idx_t action_index = 0; action_index < bind.actions.size(); action_index++) {
			if (!IsWriteAction(bind.actions[action_index].action_type)) {
				continue;
			}
			DistributedWriteTaskResult sub_result;
			sub_result.capability = info.capability;
			sub_result.fragment_codec = DUCKLAKE_ROW_DELTA_FRAGMENT_CODEC;
			sub_result.query_id = task_result.query_id;
			sub_result.task_attempt_id = task_result.task_attempt_id;
			action_results[action_index].push_back(std::move(sub_result));
		}
		if (task_result.fragments.size() > 1) {
			throw InvalidInputException("DuckLake distributed MERGE task attempt '%s' returned more than one fragment",
			                            task_result.task_attempt_id);
		}
		if (task_result.fragments.empty()) {
			continue;
		}

		const auto &outer = task_result.fragments[0];
		if (!fragment_ids.insert(outer.fragment_id).second ||
		    outer.fragment_id != task_result.query_id + "/" + task_result.task_attempt_id || outer.row_count == 0) {
			throw InvalidInputException("DuckLake distributed MERGE fragment has an invalid identity or row count");
		}
		auto embedded = DeserializeEmbeddedFragments(outer.payload);
		set<idx_t> action_indexes;
		vector<DistributedWriteArtifact> expected_artifacts;
		vector<string> expected_artifact_ids;
		idx_t row_count = 0;
		idx_t byte_count = 0;
		idx_t previous_action_index = 0;
		bool has_previous_action = false;
		for (auto &entry : embedded) {
			if (entry.action_index >= bind.actions.size() ||
			    !IsWriteAction(bind.actions[entry.action_index].action_type) ||
			    !action_indexes.insert(entry.action_index).second ||
			    (has_previous_action && entry.action_index <= previous_action_index)) {
				throw InvalidInputException("DuckLake distributed MERGE fragment has invalid action ordering");
			}
			has_previous_action = true;
			previous_action_index = entry.action_index;
			entry.fragment.Validate();
			if (entry.fragment.fragment_id != outer.fragment_id || entry.fragment.row_count == 0) {
				throw InvalidInputException("DuckLake distributed MERGE embedded fragment has an invalid identity");
			}
			row_count = CheckedAdd(row_count, entry.fragment.row_count, "fragment affected row count");
			byte_count = CheckedAdd(byte_count, entry.fragment.byte_count, "fragment byte count");
			for (const auto &artifact : entry.fragment.artifacts) {
				expected_artifacts.push_back(artifact);
				expected_artifact_ids.push_back("action:" + to_string(entry.action_index) + ":" + artifact.artifact_id);
			}
			action_results[entry.action_index].back().fragments.push_back(std::move(entry.fragment));
		}
		if (row_count != outer.row_count || byte_count != outer.byte_count ||
		    expected_artifacts.size() != outer.artifacts.size()) {
			throw InvalidInputException("DuckLake distributed MERGE fragment counts are inconsistent");
		}
		for (idx_t index = 0; index < outer.artifacts.size(); index++) {
			const auto &actual = outer.artifacts[index];
			const auto &expected = expected_artifacts[index];
			if (actual.artifact_id != expected_artifact_ids[index] || actual.uri != expected.uri ||
			    actual.codec != expected.codec || actual.payload != expected.payload) {
				throw InvalidInputException("DuckLake distributed MERGE fragment has invalid artifact metadata");
			}
		}
		outer_affected_rows = CheckedAdd(outer_affected_rows, outer.row_count, "affected row count");
	}

	DuckLakeDistributedMergeResult result;
	result.actions.resize(bind.actions.size());
	for (idx_t action_index = 0; action_index < bind.actions.size(); action_index++) {
		const auto &action = bind.actions[action_index];
		if (!IsWriteAction(action.action_type)) {
			continue;
		}
		auto sub_info = RowDeltaInfo(info, action.worker_bind_data);
		result.actions[action_index] = DecodeDuckLakeDistributedRowDeltaResults(
		    context, data_path, artifact_path, sub_info, action_results[action_index], RowDeltaKind(action.action_type),
		    use_deletion_vectors);
		result.affected_rows =
		    CheckedAdd(result.affected_rows, result.actions[action_index].affected_rows, "affected row count");
		for (const auto &path : result.actions[action_index].selected_artifact_paths) {
			if (!result.selected_artifact_paths.insert(path).second) {
				throw InvalidInputException("DuckLake distributed MERGE actions returned a duplicate artifact path");
			}
		}
	}
	if (result.affected_rows != outer_affected_rows) {
		throw InvalidInputException("DuckLake distributed MERGE affected-row counts are inconsistent");
	}
	return result;
}

static void ValidateActionDataFiles(ClientContext &context, const DuckLakeTableEntry &table,
                                    const DuckLakeDistributedMergeCoordinatorAction &action,
                                    DuckLakeDistributedRowDeltaResult &result, bool expect_row_id) {
	if (result.data_files.size() != result.data_file_artifact_roots.size()) {
		throw InvalidInputException("DuckLake distributed MERGE returned incomplete data-file ownership");
	}
	for (idx_t index = 0; index < result.data_files.size(); index++) {
		vector<distributed::DistributedCopyFileInfo> one_file;
		one_file.push_back(std::move(result.data_files[index]));
		ValidateDuckLakeDistributedDataFileArtifactsInRoot(context, result.data_file_artifact_roots[index],
		                                                   table.GetFieldData(), table.GetNotNullFields(),
		                                                   action.partition_names, one_file, expect_row_id);
		result.data_files[index] = std::move(one_file[0]);
	}
}

} // namespace

DistributedExtensionWriteCallbacks DuckLakeDistributedMergeCallbacks() {
	DistributedExtensionWriteCallbacks callbacks;
	callbacks.initialize_global = DuckLakeMergeInitializeGlobal;
	callbacks.initialize_local = DuckLakeMergeInitializeLocal;
	callbacks.sink = DuckLakeMergeSink;
	callbacks.combine = DuckLakeMergeCombine;
	callbacks.finalize = DuckLakeMergeFinalize;
	return callbacks;
}

DuckLakeDistributedMergeInto::DuckLakeDistributedMergeInto(
    PhysicalPlan &physical_plan, vector<LogicalType> types,
    map<MergeActionCondition, vector<unique_ptr<MergeIntoOperator>>> actions, idx_t row_id_index,
    optional_idx source_marker, bool parallel, bool return_chunk)
    : PhysicalMergeInto(physical_plan, std::move(types), std::move(actions), row_id_index, source_marker, parallel,
                        return_chunk) {
	type = PhysicalOperatorType::EXTENSION;
	distributed_write_plan.extension_name = "ducklake";
	distributed_write_plan.operator_name = "merge";
}

void DuckLakeDistributedMergeInto::ConfigureDistributedMerge(ClientContext &context, DuckLakeTableEntry &table,
                                                             vector<DuckLakeDistributedMergePlanAction> actions,
                                                             PhysicalOperator &worker_child,
                                                             const vector<LogicalType> &worker_input_types,
                                                             idx_t row_id_index, optional_idx source_marker) {
	auto &catalog = table.catalog.Cast<DuckLakeCatalog>();
	auto &schema = table.ParentSchema().Cast<DuckLakeSchemaEntry>();
	auto &transaction = DuckLakeTransaction::Get(context, catalog);
	distributed_catalog_name = catalog.GetName();
	distributed_schema_name = schema.name;
	distributed_table_name = table.name;
	distributed_schema_uuid = schema.GetSchemaUUID();
	distributed_table_uuid = table.GetTableUUID();
	distributed_data_path = table.DataPath();
	distributed_artifact_path = CreateDuckLakeDistributedArtifactPath(context, distributed_data_path);
	distributed_field_identity = GetDuckLakeDistributedFieldIdentity(table.GetFieldData());
	distributed_partition_identity = GetDuckLakeDistributedPartitionIdentity(table.GetPartitionData().get());
	distributed_sort_identity = GetDuckLakeDistributedSortIdentity(table.GetSortData().get());
	distributed_snapshot = transaction.GetSnapshot();
	distributed_schema_id = schema.GetSchemaId();
	distributed_table_id = table.GetTableId();
	distributed_worker_child = &worker_child;

	DuckLakeDistributedMergeBind bind;
	bind.input_types = worker_input_types;
	bind.row_id_index = row_id_index;
	bind.source_marker = source_marker;
	for (auto &planned_action : actions) {
		DuckLakeDistributedMergeActionBind action;
		action.match_condition = planned_action.match_condition;
		action.action_type = planned_action.action_type;
		action.condition = std::move(planned_action.condition);
		action.expressions = std::move(planned_action.expressions);
		action.projections = std::move(planned_action.projections);
		action.update_row_id_index = planned_action.update_row_id_index;
		action.update_file_path_index = planned_action.update_file_path_index;
		action.update_row_position_index = planned_action.update_row_position_index;

		DuckLakeDistributedMergeCoordinatorAction coordinator_action;
		coordinator_action.action_type = planned_action.action_type;
		coordinator_action.has_encryption = !planned_action.encryption_key.empty();
		if (planned_action.copy) {
			ValidateDuckLakeDistributedRowDeltaCopyShape(*planned_action.copy);
			action.copy_types = planned_action.copy->expected_types;
			coordinator_action.partition_names = GetDuckLakeDistributedPartitionNames(*planned_action.copy);
			if (table.GetPartitionData()) {
				coordinator_action.partition_id = table.GetPartitionData()->partition_id;
			}
		}
		if (planned_action.delete_op) {
			auto &delete_op = *planned_action.delete_op;
			coordinator_action.source_files = delete_op.distributed_source_files;
			coordinator_action.source_prepared = delete_op.distributed_source_prepared;
			coordinator_action.has_source_scan = delete_op.distributed_has_source_scan;
			coordinator_action.source_is_statically_empty = delete_op.distributed_source_is_statically_empty;
			coordinator_action.has_encryption = coordinator_action.has_encryption || !delete_op.encryption_key.empty();
			if (delete_op.distributed_has_source_scan &&
			    !DuckLakeDistributedSnapshotsMatch(delete_op.distributed_source_snapshot, distributed_snapshot)) {
				throw TransactionException("DuckLake MERGE source snapshot does not match its target snapshot");
			}
		}

		if (!coordinator_action.has_encryption) {
			switch (planned_action.action_type) {
			case MergeActionType::MERGE_INSERT:
				if (!planned_action.copy) {
					throw InternalException("DuckLake distributed MERGE INSERT COPY writer is missing");
				}
				action.worker_bind_data = BuildDuckLakeDistributedMergeInsertBind(
				    context, table, *planned_action.copy, planned_action.copy->expected_types.size(),
				    distributed_artifact_path);
				break;
			case MergeActionType::MERGE_UPDATE:
				if (planned_action.copy && planned_action.delete_op && coordinator_action.source_prepared) {
					action.worker_bind_data = BuildDuckLakeDistributedUpdateBind(
					    context, table, coordinator_action.source_files, *planned_action.copy,
					    planned_action.copy->expected_types.size(), planned_action.copy->expected_types.size(),
					    planned_action.copy->expected_types.size() + 1, distributed_artifact_path,
					    coordinator_action.source_is_statically_empty);
				}
				break;
			case MergeActionType::MERGE_DELETE:
				if (planned_action.delete_op && coordinator_action.source_prepared) {
					action.worker_bind_data = BuildDuckLakeDistributedDeleteBind(
					    context, table, coordinator_action.source_files, planned_action.delete_op->row_id_indexes,
					    distributed_artifact_path, coordinator_action.source_is_statically_empty);
				}
				break;
			default:
				break;
			}
		}
		coordinator_action.worker_bind_data = action.worker_bind_data;
		distributed_actions.push_back(std::move(coordinator_action));
		bind.actions.push_back(std::move(action));
	}
	distributed_write_plan.worker_bind_data = SerializeMergeBind(bind);
}

void DuckLakeDistributedMergeInto::ValidateDistributedShape() const {
	if (distributed_write_plan.extension_name != "ducklake" || distributed_write_plan.operator_name != "merge" ||
	    distributed_write_plan.worker_bind_data.empty() || distributed_actions.empty() ||
	    distributed_artifact_path.empty() || !distributed_worker_child || !distributed_worker_plan_selected ||
	    children.size() != 1 || !distributed_schema_id.IsValid() || !distributed_table_id.IsValid()) {
		throw InvalidInputException("DuckLake distributed MERGE worker plan was not initialized");
	}
	for (const auto &action : distributed_actions) {
		if (IsWriteAction(action.action_type) && action.worker_bind_data.empty()) {
			throw InvalidInputException("DuckLake distributed MERGE action is missing its frozen worker bind");
		}
		if (action.action_type == MergeActionType::MERGE_UPDATE ||
		    action.action_type == MergeActionType::MERGE_DELETE) {
			if (!action.source_prepared) {
				throw NotImplementedException(
				    "Distributed DuckLake MERGE supports committed file-backed target rows only");
			}
			if (action.source_is_statically_empty) {
				if (action.has_source_scan || !action.source_files.empty()) {
					throw InvalidInputException("DuckLake distributed MERGE has invalid empty target state");
				}
			} else if (!action.has_source_scan || action.source_files.empty()) {
				throw InvalidInputException("DuckLake distributed MERGE is missing its planned target scan");
			}
		}
	}
}

optional_ptr<distributed::ExtensionWriteTaskProvider> DuckLakeDistributedMergeInto::GetExtensionWriteTaskProvider() {
	if (distributed_worker_plan_selected) {
		ValidateDistributedShape();
		return this;
	}
	for (const auto &action : distributed_actions) {
		if (action.has_encryption) {
			throw NotImplementedException("Distributed DuckLake MERGE does not support encrypted tables");
		}
		if ((action.action_type == MergeActionType::MERGE_UPDATE ||
		     action.action_type == MergeActionType::MERGE_DELETE) &&
		    !action.source_prepared) {
			throw NotImplementedException("Distributed DuckLake MERGE supports committed file-backed target rows only");
		}
	}
	if (children.size() != 1 || !distributed_worker_child) {
		throw InvalidInputException("DuckLake distributed MERGE requires exactly one worker child");
	}
	children[0] = *distributed_worker_child;
	distributed_worker_plan_selected = true;
	ValidateDistributedShape();
	return this;
}

const distributed::DistributedExtensionWritePlan &DuckLakeDistributedMergeInto::WritePlan() const {
	ValidateDistributedShape();
	return distributed_write_plan;
}

DuckLakeTableEntry &DuckLakeDistributedMergeInto::ResolveDistributedTable(ClientContext &context) const {
	auto &catalog = Catalog::GetCatalog(context, distributed_catalog_name).Cast<DuckLakeCatalog>();
	auto &schema = catalog.GetSchema(context, distributed_schema_name).Cast<DuckLakeSchemaEntry>();
	if (schema.GetSchemaId() != distributed_schema_id || schema.GetSchemaUUID() != distributed_schema_uuid) {
		throw TransactionException("DuckLake schema %s.%s changed after the distributed MERGE was planned",
		                           distributed_catalog_name, distributed_schema_name);
	}
	auto &table = Catalog::GetEntry<TableCatalogEntry>(context, distributed_catalog_name, distributed_schema_name,
	                                                   distributed_table_name)
	                  .Cast<DuckLakeTableEntry>();
	auto &transaction = DuckLakeTransaction::Get(context, catalog);
	if (table.GetTableId() != distributed_table_id || table.GetTableUUID() != distributed_table_uuid) {
		throw TransactionException("DuckLake table %s.%s.%s was replaced after the distributed MERGE was planned",
		                           distributed_catalog_name, distributed_schema_name, distributed_table_name);
	}
	if (table.IsTransactionLocal() || transaction.HasAnyLocalChanges(table.GetTableId())) {
		throw NotImplementedException("Distributed DuckLake MERGE does not support transaction-local table state");
	}
	auto &file_system = FileSystem::GetFileSystem(context);
	auto current_data_path = distributed::CanonicalDistributedCopyBasePath(file_system, table.DataPath());
	auto planned_data_path = distributed::CanonicalDistributedCopyBasePath(file_system, distributed_data_path);
	if (current_data_path.is_err() || planned_data_path.is_err() ||
	    current_data_path.value() != planned_data_path.value()) {
		throw TransactionException("DuckLake table %s.%s.%s data path changed after the distributed MERGE was planned",
		                           distributed_catalog_name, distributed_schema_name, distributed_table_name);
	}
	if (GetDuckLakeDistributedFieldIdentity(table.GetFieldData()) != distributed_field_identity ||
	    GetDuckLakeDistributedPartitionIdentity(table.GetPartitionData().get()) != distributed_partition_identity ||
	    GetDuckLakeDistributedSortIdentity(table.GetSortData().get()) != distributed_sort_identity) {
		throw TransactionException("DuckLake table %s.%s.%s layout changed after the distributed MERGE was planned",
		                           distributed_catalog_name, distributed_schema_name, distributed_table_name);
	}
	ValidateDuckLakeDistributedSnapshotBaseline(context, distributed_catalog_name, distributed_snapshot, "MERGE");
	return table;
}

void DuckLakeDistributedMergeInto::ValidateDistributedWrite(ClientContext &context) const {
	ValidateDistributedShape();
	bool expected = false;
	if (!distributed_write_claimed.compare_exchange_strong(expected, true)) {
		throw InvalidInputException("A distributed DuckLake MERGE plan can only be executed once");
	}
	auto &catalog = Catalog::GetCatalog(context, distributed_catalog_name).Cast<DuckLakeCatalog>();
	if (catalog.GetAttached().IsReadOnly()) {
		throw PermissionException("Distributed DuckLake MERGE requires a writable catalog");
	}
	if (catalog.CatalogSnapshot()) {
		throw NotImplementedException("Distributed DuckLake MERGE does not support snapshot-attached catalogs");
	}
	if (catalog.RetrialsServerSide()) {
		throw NotImplementedException("Distributed DuckLake MERGE does not support server-side commit retries");
	}
	ValidateDuckLakeDistributedArtifactPath(context, distributed_data_path, distributed_artifact_path);
	auto &transaction = DuckLakeTransaction::Get(context, catalog);
	if (transaction.ChangesMade()) {
		throw NotImplementedException("Distributed DuckLake MERGE requires an otherwise empty catalog transaction");
	}
	auto &table = ResolveDistributedTable(context);
	for (const auto &action : distributed_actions) {
		if (action.action_type != MergeActionType::MERGE_UPDATE &&
		    action.action_type != MergeActionType::MERGE_DELETE) {
			continue;
		}
		ValidateDuckLakeDistributedRowDeltaSourceBaseline(context, table, action.source_files, action.worker_bind_data,
		                                                  "MERGE", action.source_is_statically_empty);
	}
}

idx_t DuckLakeDistributedMergeInto::FinalizeDistributedWrite(ClientContext &context,
                                                             const vector<DistributedWriteTaskResult> &results) const {
	ValidateDistributedShape();
	bool ownership_transferred = false;
	try {
		auto &table = ResolveDistributedTable(context);
		auto &catalog = table.catalog.Cast<DuckLakeCatalog>();
		auto &schema = table.ParentSchema().Cast<DuckLakeSchemaEntry>();
		for (const auto &action : distributed_actions) {
			if (action.action_type != MergeActionType::MERGE_UPDATE &&
			    action.action_type != MergeActionType::MERGE_DELETE) {
				continue;
			}
			ValidateDuckLakeDistributedRowDeltaSourceBaseline(context, table, action.source_files,
			                                                  action.worker_bind_data, "MERGE",
			                                                  action.source_is_statically_empty);
		}
		auto use_deletion_vectors = catalog.WriteDeletionVectors(schema.GetSchemaId(), table.GetTableId());
		auto write_info = distributed::ResolveDistributedExtensionWriteInfo(context, distributed_write_plan);
		auto decoded = DecodeDistributedMergeResults(context, write_info, results, distributed_data_path,
		                                             distributed_artifact_path, use_deletion_vectors);
		CleanupDuckLakeDistributedRowDelta(context, distributed_data_path, distributed_artifact_path,
		                                   &decoded.selected_artifact_paths);
		if (decoded.affected_rows == 0) {
			CleanupDuckLakeDistributedRowDelta(context, distributed_data_path, distributed_artifact_path);
			return 0;
		}

		DuckLakeInsertGlobalState insert_state(table);
		vector<DuckLakeDeleteFile> delete_files;
		idx_t affected_rows = 0;
		for (idx_t action_index = 0; action_index < distributed_actions.size(); action_index++) {
			const auto &action = distributed_actions[action_index];
			auto &action_result = decoded.actions[action_index];
			if (!IsWriteAction(action.action_type)) {
				continue;
			}
			idx_t data_row_count = 0;
			for (const auto &file : action_result.data_files) {
				data_row_count = CheckedAdd(data_row_count, file.row_count, "data row count");
			}
			idx_t delete_row_count = 0;
			for (const auto &file : action_result.delete_files) {
				delete_row_count = CheckedAdd(delete_row_count, file.new_delete_count, "delete row count");
			}

			if (action.action_type == MergeActionType::MERGE_INSERT) {
				if (data_row_count != action_result.affected_rows || delete_row_count != 0) {
					throw InvalidInputException("DuckLake distributed MERGE INSERT returned inconsistent artifacts");
				}
				ValidateActionDataFiles(context, table, action, action_result, false);
				AddDuckLakeDistributedDataFiles(context, insert_state, action_result.data_files, action.partition_id);
			} else if (action.action_type == MergeActionType::MERGE_UPDATE) {
				if (data_row_count != action_result.affected_rows || delete_row_count != action_result.affected_rows) {
					throw InvalidInputException("DuckLake distributed MERGE UPDATE returned inconsistent artifacts");
				}
				ValidateActionDataFiles(context, table, action, action_result, true);
				AddDuckLakeDistributedDataFiles(context, insert_state, action_result.data_files, action.partition_id);
				auto action_delete_files = BuildDuckLakeDistributedDeleteFiles(
				    context, action.source_files, action.worker_bind_data, action_result.delete_files, "MERGE UPDATE");
				for (auto &file : action_delete_files) {
					delete_files.push_back(std::move(file));
				}
			} else {
				if (data_row_count != 0 || delete_row_count == 0 || delete_row_count > action_result.affected_rows) {
					throw InvalidInputException("DuckLake distributed MERGE DELETE returned inconsistent artifacts");
				}
				auto action_delete_files = BuildDuckLakeDistributedDeleteFiles(
				    context, action.source_files, action.worker_bind_data, action_result.delete_files, "MERGE DELETE");
				for (auto &file : action_delete_files) {
					delete_files.push_back(std::move(file));
				}
			}
			affected_rows = CheckedAdd(affected_rows, action_result.affected_rows, "affected row count");
		}
		if (affected_rows != decoded.affected_rows || (insert_state.written_files.empty() && delete_files.empty())) {
			throw InvalidInputException("DuckLake distributed MERGE returned inconsistent mutation artifacts");
		}

		auto &transaction = DuckLakeTransaction::Get(context, catalog);
		transaction.FailDistributedWriteOnSnapshotConflict();
		transaction.RegisterDistributedArtifact(table.GetTableId(), distributed_data_path, distributed_artifact_path);
		ownership_transferred = true;
		if (!insert_state.written_files.empty()) {
			transaction.AppendFiles(table.GetTableId(), std::move(insert_state.written_files));
		}
		if (!delete_files.empty()) {
			transaction.AddDeletes(table.GetTableId(), std::move(delete_files));
		}
		return affected_rows;
	} catch (...) {
		if (!ownership_transferred) {
			try {
				CleanupDuckLakeDistributedRowDelta(context, distributed_data_path, distributed_artifact_path);
			} catch (...) {
			}
		}
		throw;
	}
}

void DuckLakeDistributedMergeInto::AbortDistributedWrite(ClientContext &context,
                                                         const vector<DistributedWriteTaskResult> &) const {
	CleanupDuckLakeDistributedRowDelta(context, distributed_data_path, distributed_artifact_path);
}

void DuckLakeDistributedMergeInto::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	if (distributed_worker_plan_selected) {
		throw InvalidInputException(
		    "A distributed DuckLake MERGE worker plan cannot execute as a native coordinator operator");
	}
	PhysicalOperator::BuildPipelines(current, meta_pipeline);
}

string DuckLakeDistributedMergeInto::GetName() const {
	return "DUCKLAKE_MERGE_INTO";
}

} // namespace duckdb

#endif
