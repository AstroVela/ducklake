#include "storage/ducklake_vortex_write.hpp"

#include "common/ducklake_vortex_json.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/main/prepared_statement.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/storage/storage_lock.hpp"
#include "duckdb/transaction/transaction_context.hpp"
#include "mbedtls_wrapper.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_vortex.hpp"
#include "storage/ducklake_vortex_index_catalog.hpp"
#include "storage/ducklake_vortex_optimizer.hpp"
#include "storage/ducklake_vortex_read_view.hpp"

#include <cmath>
#include <cstring>
#include <limits>

namespace duckdb {
namespace {
constexpr const char *REQUESTS = "ducklake_vortex_write_requests_v2";
constexpr const char *EPOCHS = "ducklake_vortex_request_epochs_v1";
constexpr const char *MANAGED = "ducklake_vortex_keyed_tables_v1";
constexpr const char *MUTATION = "ducklake_vortex_mutation";
constexpr const char *RETIREMENT = "ducklake_vortex_table_retirement";
constexpr idx_t MAX_ROWS = 1000;
constexpr idx_t MAX_BYTES = 4 * 1024 * 1024;
constexpr idx_t MAX_REQUESTS = 100000;

struct MutationContext final : public ClientContextState {
	string uuid;
};

struct MutationScope {
	shared_ptr<MutationContext> state;
	explicit MutationScope(ClientContext &context)
	    : state(context.registered_state->GetOrCreate<MutationContext>(MUTATION)) {
	}
	~MutationScope() {
		state->uuid.clear();
	}
};

struct RetirementContext final : public ClientContextState {
	string catalog, uuid;
	idx_t table_id = 0;
	std::function<void(const string &)> check;
};

struct RetirementScope {
	shared_ptr<RetirementContext> state;
	explicit RetirementScope(ClientContext &context)
	    : state(context.registered_state->GetOrCreate<RetirementContext>(RETIREMENT)) {
	}
	~RetirementScope() {
		state->uuid.clear();
		state->check = nullptr;
	}
};

string Qualified(const string &catalog, const string &schema, const string &table) {
	return KeywordHelper::WriteOptionallyQuoted(catalog) + "." + KeywordHelper::WriteOptionallyQuoted(schema) + "." +
	       KeywordHelper::WriteOptionallyQuoted(table);
}

StorageLock &WriteLock() {
	static StorageLock lock;
	return lock;
}

void BoundedAppend(string &bytes, const char *data, idx_t size) {
	if (size > MAX_BYTES - bytes.size()) {
		throw InvalidInputException("RESOURCE_EXHAUSTED: Vortex WriteBatch byte budget exceeded");
	}
	bytes.append(data, size);
}

void Number(string &bytes, uint64_t value, idx_t size) {
	for (idx_t i = 0; i < size; i++) {
		auto byte = char((value >> (i * 8)) & 255);
		BoundedAppend(bytes, &byte, 1);
	}
}

void Nullable(string &bytes, const Value &value) {
	Number(bytes, value.IsNull() ? 0 : 1, 1);
}

Value CanonicalRows(const Value &input, uint32_t dimension, string &bytes, bool mutate) {
	const auto &rows = ListValue::GetChildren(input);
	if ((!mutate && rows.empty()) || rows.size() > MAX_ROWS) {
		throw InvalidInputException("Vortex WriteBatch requires 1 to 1000 rows");
	}
	bytes = mutate ? "ducklake-vortex-mutate-v1" : "ducklake-vortex-write-v1";
	Number(bytes, dimension, 4);
	Number(bytes, rows.size(), 8);
	vector<Value> canonical;
	for (auto &row : rows) {
		if (row.IsNull()) {
			throw InvalidInputException("Vortex WriteBatch row cannot be NULL");
		}
		auto fields = StructValue::GetChildren(row);
		Nullable(bytes, fields[0]);
		if (!fields[0].IsNull()) {
			Number(bytes, uint64_t(fields[0].GetValue<int64_t>()), 8);
		}
		if (fields[1].IsNull() || ListValue::GetChildren(fields[1]).size() != dimension) {
			throw InvalidInputException("Vortex WriteBatch vector dimension or NULL list mismatch");
		}
		vector<Value> embedding;
		for (auto &component : ListValue::GetChildren(fields[1])) {
			if (component.IsNull() || !std::isfinite(component.GetValue<float>())) {
				throw InvalidInputException("Vortex WriteBatch requires finite non-NULL vector components");
			}
			auto value = component.GetValue<float>();
			value = value == 0 ? 0.0f : value;
			uint32_t bits;
			std::memcpy(&bits, &value, sizeof(bits));
			Number(bytes, bits, 4);
			embedding.push_back(Value::FLOAT(value));
		}
		fields[1] = Value::LIST(LogicalType::FLOAT, std::move(embedding));
		for (idx_t col = 2; col < 4; col++) {
			Nullable(bytes, fields[col]);
			if (!fields[col].IsNull()) {
				auto &text = StringValue::Get(fields[col]);
				Number(bytes, text.size(), 8);
				BoundedAppend(bytes, text.data(), text.size());
			}
		}
		canonical.push_back(Value::STRUCT(ListType::GetChildType(DuckLakeVortexWriteBatch::RowsType()), fields));
	}
	return Value::LIST(ListType::GetChildType(DuckLakeVortexWriteBatch::RowsType()), std::move(canonical));
}

Value MutationKeys(const Value &rows, const Value &deletes, string &bytes) {
	auto &upserts = ListValue::GetChildren(rows);
	auto &removed = ListValue::GetChildren(deletes);
	if (upserts.size() + removed.size() == 0 || upserts.size() + removed.size() > MAX_ROWS) {
		throw InvalidInputException("Vortex mutation requires 1 to 1000 operations");
	}
	set<int64_t> seen;
	vector<Value> keys;
	auto key = [&](const Value &value) {
		if (value.IsNull() || !seen.insert(value.GetValue<int64_t>()).second) {
			throw InvalidInputException("Vortex mutation requires non-NULL unique ids across upserts and deletes");
		}
		keys.push_back(value);
	};
	for (auto &row : upserts) {
		key(StructValue::GetChildren(row)[0]);
	}
	Number(bytes, removed.size(), 8);
	for (auto &value : removed) {
		key(value);
		Number(bytes, uint64_t(value.GetValue<int64_t>()), 8);
	}
	return Value::LIST(LogicalType::BIGINT, std::move(keys));
}

void Initialized(DuckLakeTransaction &transaction) {
	auto &catalog = transaction.GetCatalog();
	for (auto name : {REQUESTS, EPOCHS, MANAGED}) {
		if (!transaction.GetConnection().TableInfo(catalog.MetadataDatabaseName(), catalog.MetadataSchemaName(),
		                                           name)) {
			throw InvalidInputException("Vortex WriteBatch catalog is not initialized");
		}
	}
}

struct WriteTarget {
	DuckLakeTableEntry &table;
	uint32_t dimension;
	bool managed;
};

WriteTarget Target(Connection &owner, DuckLakeTransaction &transaction, const string &schema, const string &table_name,
                   const string &index_name) {
	auto entry = transaction.GetCatalog().GetEntry(*owner.context, CatalogType::TABLE_ENTRY, schema, table_name,
	                                               OnEntryNotFound::THROW_EXCEPTION);
	auto &table = entry->Cast<DuckLakeTableEntry>();
	DuckLakeVortex::ValidateTable(table);
	DuckLakeVortexReadView::ValidateFields(table);
	if (DuckLakeVortex::GetFormat(table) != DataFileFormat::VORTEX || table.GetColumns().PhysicalColumnCount() != 4 ||
	    table.GetColumns().LogicalColumnCount() != 4) {
		throw InvalidInputException("Vortex WriteBatch requires a four-column Vortex table");
	}
	auto index = DuckLakeVortexIndex::Lookup(transaction, table, index_name);
	auto managed =
	    DuckLakeVortexIndex::Query(transaction, "SELECT index_name FROM {METADATA_CATALOG}." + string(MANAGED) +
	                                                " WHERE table_uuid=" + Value(table.GetTableUUID()).ToSQLString())
	        ->Fetch();
	if (managed && StringValue::Get(managed->GetValue(0, 0)) != index_name) {
		throw InvalidInputException("Vortex batch operation does not match the table's explicitly managed write mode");
	}
	return {table, index.dimension, bool(managed)};
}

struct EpochState {
	uint64_t epoch = 1;
	idx_t expired = 0;
	bool registered = false;
};

EpochState ReadEpoch(DuckLakeTransaction &transaction, const string &uuid) {
	EpochState state;
	auto record =
	    DuckLakeVortexIndex::Query(transaction, "SELECT request_epoch,last_expired_requests FROM "
	                                            "{METADATA_CATALOG}." +
	                                                string(EPOCHS) + " WHERE table_uuid=" + Value(uuid).ToSQLString())
	        ->Fetch();
	if (record) {
		state.epoch = record->GetValue(0, 0).GetValue<uint64_t>();
		state.expired = record->GetValue(1, 0).GetValue<idx_t>();
		state.registered = true;
	}
	return state;
}

idx_t RequestCount(DuckLakeTransaction &transaction, const string &where = "") {
	return DuckLakeVortexIndex::Query(transaction,
	                                  "SELECT count(*) FROM {METADATA_CATALOG}." + string(REQUESTS) + where)
	    ->Fetch()
	    ->GetValue(0, 0)
	    .GetValue<idx_t>();
}

void CheckTableUUID(const string &expected, const string &actual) {
	if (UUID::ToString(UUID::FromString(expected)) != expected) {
		throw InvalidInputException("Vortex table UUID must be canonical");
	}
	if (expected != actual) {
		throw InvalidInputException("STALE_TABLE: Vortex table UUID does not match the current table");
	}
}

void NoCommitRetry(Connection &owner) {
	auto setting = owner.Query("SET SESSION ducklake_max_retry_count=0");
	if (setting->HasError()) {
		setting->ThrowError();
	}
}
} // namespace

unique_ptr<StorageLockKey> DuckLakeVortexWriteBatch::AcquireWriter() {
	return WriteLock().GetExclusiveLock();
}

LogicalType DuckLakeVortexWriteBatch::RowsType() {
	return LogicalType::LIST(LogicalType::STRUCT({{"id", LogicalType::BIGINT},
	                                              {"embedding", LogicalType::LIST(LogicalType::FLOAT)},
	                                              {"category", LogicalType::VARCHAR},
	                                              {"payload", LogicalType::VARCHAR}}));
}

void DuckLakeVortexWriteBatch::Initialize(Connection &owner, const string &catalog) {
	if (owner.context->transaction.HasActiveTransaction()) {
		throw InvalidInputException("Vortex WriteBatch initialization requires an idle dedicated connection");
	}
	auto guard = AcquireWriter();
	try {
		auto &transaction = DuckLakeVortexIndex::Begin(owner, catalog);
		auto &metadata = transaction.GetCatalog();
		if (transaction.GetConnection().TableInfo(metadata.MetadataDatabaseName(), metadata.MetadataSchemaName(),
		                                          "ducklake_vortex_write_requests_v1")) {
			throw InvalidInputException(
			    "Vortex request epochs require a fresh write catalog; legacy receipts are unsupported");
		}
		DuckLakeVortexIndex::Query(transaction, "CREATE TABLE {METADATA_CATALOG}." + string(EPOCHS) +
		                                            "(table_uuid VARCHAR PRIMARY KEY,request_epoch UBIGINT NOT NULL "
		                                            "CHECK(request_epoch>0),last_expired_requests UBIGINT NOT NULL)");
		DuckLakeVortexIndex::Query(transaction,
		                           "CREATE TABLE {METADATA_CATALOG}." + string(REQUESTS) +
		                               "(table_uuid VARCHAR NOT NULL,request_id VARCHAR NOT NULL,"
		                               "payload_hash VARCHAR NOT NULL,row_count UBIGINT NOT NULL,"
		                               "snapshot_id UBIGINT NOT NULL,request_epoch UBIGINT NOT NULL "
		                               "CHECK(request_epoch>0),PRIMARY KEY(table_uuid,request_epoch,request_id))");
		DuckLakeVortexIndex::Query(transaction, "CREATE TABLE {METADATA_CATALOG}." + string(MANAGED) +
		                                            "(table_uuid VARCHAR PRIMARY KEY,table_id BIGINT UNIQUE NOT NULL,"
		                                            "index_name VARCHAR NOT NULL)");
		owner.Commit();
	} catch (...) {
		DuckLakeVortexIndex::Rollback(owner);
		throw;
	}
}

void DuckLakeVortexWriteBatch::Manage(Connection &owner, const string &catalog_name, const string &schema,
                                      const string &table_name, const string &index_name) {
	if (owner.context->transaction.HasActiveTransaction()) {
		throw InvalidInputException("Vortex managed writes require an idle dedicated connection");
	}
	auto guard = AcquireWriter();
	auto admission = DuckLakeVortexOptimizer::ReserveWrite(*owner.context);
	try {
		auto &transaction = DuckLakeVortexIndex::Begin(owner, catalog_name);
		Initialized(transaction);
		auto entry = transaction.GetCatalog().GetEntry(*owner.context, CatalogType::TABLE_ENTRY, schema, table_name,
		                                               OnEntryNotFound::THROW_EXCEPTION);
		auto &table = entry->Cast<DuckLakeTableEntry>();
		DuckLakeVortex::ValidateTable(table);
		DuckLakeVortexReadView::ValidateFields(table);
		auto index = DuckLakeVortexIndex::Lookup(transaction, table, index_name);
		auto rows = owner.Query("SELECT 1 FROM " + Qualified(catalog_name, schema, table_name) + " LIMIT 1");
		if (rows->HasError()) {
			rows->ThrowError();
		}
		auto requests = DuckLakeVortexIndex::Query(
		    transaction, "SELECT count(*) FROM {METADATA_CATALOG}." + string(REQUESTS) +
		                     " WHERE table_uuid=" + Value(table.GetTableUUID()).ToSQLString());
		if (rows->Fetch() || !index.segments.empty() || requests->Fetch()->GetValue(0, 0).GetValue<idx_t>()) {
			throw InvalidInputException(
			    "Vortex managed writes require an empty table without prior batches or B segments");
		}
		DuckLakeVortexIndex::Query(transaction, "INSERT INTO {METADATA_CATALOG}." + string(MANAGED) + " VALUES(" +
		                                            Value(table.GetTableUUID()).ToSQLString() + "," +
		                                            to_string(table.GetTableId().index) + "," +
		                                            Value(index_name).ToSQLString() + ")");
		owner.Commit();
	} catch (...) {
		DuckLakeVortexIndex::Rollback(owner);
		throw;
	}
}

void DuckLakeVortexWriteBatch::CheckManagedWrites(DuckLakeTransaction &fresh, const set<TableIndex> &tables,
                                                  ClientContext &context) {
	auto &catalog = fresh.GetCatalog();
	if (tables.empty() ||
	    !fresh.GetConnection().TableInfo(catalog.MetadataDatabaseName(), catalog.MetadataSchemaName(), MANAGED)) {
		return;
	}
	auto owner = context.registered_state->Get<MutationContext>(MUTATION);
	auto rewrite = DuckLakeVortexRewriteState::Get(context);
	for (auto &id : tables) {
		auto result = DuckLakeVortexIndex::Query(fresh, "SELECT table_uuid FROM {METADATA_CATALOG}." + string(MANAGED) +
		                                                    " WHERE table_id=" + to_string(id.index));
		auto row = result->Fetch();
		if (!row) {
			continue;
		}
		auto uuid = StringValue::Get(row->GetValue(0, 0));
		if ((!owner || owner->uuid != uuid) && (!rewrite || rewrite->table_uuid != uuid)) {
			throw InvalidInputException(
			    "Managed Vortex tables require ducklake_mutate_vortex_batch for logical writes and fixed schema");
		}
	}
}

DuckLakeVortexRequestStatus DuckLakeVortexWriteBatch::RequestStatus(Connection &owner, const string &catalog,
                                                                    const string &schema, const string &table_name,
                                                                    const string &index) {
	if (owner.context->transaction.HasActiveTransaction()) {
		throw InvalidInputException("Vortex request status requires an idle dedicated connection");
	}
	auto guard = AcquireWriter();
	try {
		auto &transaction = DuckLakeVortexIndex::Begin(owner, catalog);
		Initialized(transaction);
		auto target = Target(owner, transaction, schema, table_name, index);
		auto uuid = target.table.GetTableUUID();
		auto epoch = ReadEpoch(transaction, uuid);
		auto where = " WHERE table_uuid=" + Value(uuid).ToSQLString() + " AND request_epoch=" + to_string(epoch.epoch);
		DuckLakeVortexRequestStatus result {uuid, epoch.epoch, RequestCount(transaction, where),
		                                    RequestCount(transaction)};
		owner.Commit();
		return result;
	} catch (...) {
		DuckLakeVortexIndex::Rollback(owner);
		throw;
	}
}

DuckLakeVortexRequestAdvance DuckLakeVortexWriteBatch::AdvanceRequestEpoch(
    Connection &owner, const string &catalog, const string &schema, const string &table_name, const string &index,
    uint64_t expected_epoch, const string &expected_table_uuid, const std::function<void(const string &)> &observe,
    optional_ptr<ClientContext> caller) {
	if (owner.context->transaction.HasActiveTransaction() || expected_epoch == 0) {
		throw InvalidInputException("Vortex request epoch advance requires an idle connection and a positive epoch");
	}
	auto check = [&](const string &stage) {
		if (observe) {
			observe(stage);
		}
		if (owner.context->IsInterrupted() || (caller && caller->IsInterrupted())) {
			throw InterruptException();
		}
	};
	check("waiting");
	auto guard = AcquireWriter();
	check("acquired");
	NoCommitRetry(owner);
	try {
		auto &transaction = DuckLakeVortexIndex::Begin(owner, catalog);
		Initialized(transaction);
		auto target = Target(owner, transaction, schema, table_name, index);
		auto uuid = target.table.GetTableUUID();
		CheckTableUUID(expected_table_uuid, uuid);
		auto epoch = ReadEpoch(transaction, uuid);
		check("after_lookup");
		if (expected_epoch < std::numeric_limits<uint64_t>::max() && epoch.epoch == expected_epoch + 1) {
			DuckLakeVortexRequestAdvance result {uuid, expected_epoch, epoch.epoch, epoch.expired};
			owner.Commit();
			return result;
		}
		if (expected_epoch != epoch.epoch) {
			throw InvalidInputException("Vortex request epoch advance expected epoch does not match current epoch");
		}
		if (expected_epoch == std::numeric_limits<uint64_t>::max()) {
			throw InvalidInputException("RESOURCE_EXHAUSTED: Vortex request epoch is exhausted");
		}
		auto where =
		    " WHERE table_uuid=" + Value(uuid).ToSQLString() + " AND request_epoch=" + to_string(expected_epoch);
		auto count = RequestCount(transaction, where);
		DuckLakeVortexIndex::Query(transaction, "DELETE FROM {METADATA_CATALOG}." + string(REQUESTS) + where);
		check("after_prune");
		auto next = expected_epoch + 1;
		if (epoch.registered) {
			DuckLakeVortexIndex::Query(transaction, "UPDATE {METADATA_CATALOG}." + string(EPOCHS) +
			                                            " SET request_epoch=" + to_string(next) +
			                                            ",last_expired_requests=" + to_string(count) +
			                                            " WHERE table_uuid=" + Value(uuid).ToSQLString());
		} else {
			DuckLakeVortexIndex::Query(transaction, "INSERT INTO {METADATA_CATALOG}." + string(EPOCHS) + " VALUES(" +
			                                            Value(uuid).ToSQLString() + "," + to_string(next) + "," +
			                                            to_string(count) + ")");
		}
		check("after_epoch");
		DuckLakeVortexRequestAdvance result {uuid, expected_epoch, next, count};
		check("before_commit");
		owner.Commit();
		check("after_commit");
		return result;
	} catch (...) {
		DuckLakeVortexIndex::Rollback(owner);
		throw;
	}
}

DuckLakeVortexWriteResult DuckLakeVortexWriteBatch::Append(Connection &owner, const string &catalog_name,
                                                           const string &schema, const string &table_name,
                                                           const string &index_name, const string &request_id,
                                                           const Value &rows, uint64_t request_epoch,
                                                           const string &expected_table_uuid,
                                                           const std::function<void(const string &)> &observe,
                                                           optional_ptr<ClientContext> caller) {
	return Write(owner, catalog_name, schema, table_name, index_name, request_id, rows,
	             Value::LIST(LogicalType::BIGINT, {}), false, request_epoch, expected_table_uuid, observe, caller);
}

DuckLakeVortexWriteResult DuckLakeVortexWriteBatch::Mutate(Connection &owner, const string &catalog,
                                                           const string &schema, const string &table,
                                                           const string &index, const string &request_id,
                                                           const Value &rows, const Value &deletes,
                                                           uint64_t request_epoch, const string &expected_table_uuid,
                                                           const std::function<void(const string &)> &observe,
                                                           optional_ptr<ClientContext> caller) {
	return Write(owner, catalog, schema, table, index, request_id, rows, deletes, true, request_epoch,
	             expected_table_uuid, observe, caller);
}

DuckLakeVortexWriteResult DuckLakeVortexWriteBatch::Write(Connection &owner, const string &catalog_name,
                                                          const string &schema, const string &table_name,
                                                          const string &index_name, const string &request_id,
                                                          const Value &rows, const Value &deletes, bool mutate,
                                                          uint64_t request_epoch, const string &expected_table_uuid,
                                                          const std::function<void(const string &)> &observe,
                                                          optional_ptr<ClientContext> caller) {
	if (owner.context->transaction.HasActiveTransaction()) {
		throw InvalidInputException("Vortex WriteBatch requires an idle dedicated connection");
	}
	if (request_epoch == 0 || request_id.empty() || request_id.size() > 128 || request_id.find('\0') != string::npos ||
	    rows.IsNull() || rows.type() != RowsType() || deletes.IsNull() ||
	    deletes.type() != LogicalType::LIST(LogicalType::BIGINT)) {
		throw InvalidInputException("Invalid Vortex WriteBatch request epoch, request ID or typed rows");
	}
	auto check = [&](const string &stage) {
		if (observe) {
			observe(stage);
		}
		if (owner.context->IsInterrupted() || (caller && caller->IsInterrupted())) {
			throw InterruptException();
		}
	};
	check("waiting");
	auto guard = AcquireWriter();
	MutationScope mutation(*owner.context);
	check("acquired");
	// Metadata-side request staging must not be discarded by a native commit retry.
	NoCommitRetry(owner);
	try {
		auto &transaction = DuckLakeVortexIndex::Begin(owner, catalog_name);
		Initialized(transaction);
		auto target = Target(owner, transaction, schema, table_name, index_name);
		auto &table = target.table;
		CheckTableUUID(expected_table_uuid, table.GetTableUUID());
		if (mutate != target.managed) {
			throw InvalidInputException(
			    "Vortex batch operation does not match the table's explicitly managed write mode");
		}
		auto epoch = ReadEpoch(transaction, table.GetTableUUID());
		if (request_epoch < epoch.epoch) {
			throw InvalidInputException("EXPIRED_REQUEST: Vortex request epoch has been retired");
		}
		if (request_epoch != epoch.epoch) {
			throw InvalidInputException("Vortex request epoch does not match current epoch");
		}
		if (mutate) {
			mutation.state->uuid = table.GetTableUUID();
		}
		DuckLakeVortexWriteResult result;
		result.table_uuid = table.GetTableUUID();
		result.request_id = request_id;
		result.request_epoch = request_epoch;
		string bytes;
		auto canonical = CanonicalRows(rows, target.dimension, bytes, mutate);
		auto keys = mutate ? MutationKeys(canonical, deletes, bytes) : Value::LIST(LogicalType::BIGINT, {});
		result.payload_hash = "sha256:" + vortex_detail::Hex(duckdb_mbedtls::MbedTlsWrapper::ComputeSha256Hash(bytes));
		result.row_count = ListValue::GetChildren(canonical).size();
		if (mutate) {
			result.row_count += ListValue::GetChildren(deletes).size();
		}
		auto where = " WHERE table_uuid=" + Value(result.table_uuid).ToSQLString() +
		             " AND request_epoch=" + to_string(request_epoch) +
		             " AND request_id=" + Value(request_id).ToSQLString();
		auto prior = DuckLakeVortexIndex::Query(transaction, "SELECT payload_hash,row_count,snapshot_id FROM "
		                                                     "{METADATA_CATALOG}." +
		                                                         string(REQUESTS) + where);
		auto record = prior->Fetch();
		if (record) {
			if (record->GetValue(0, 0).GetValue<string>() != result.payload_hash ||
			    record->GetValue(1, 0).GetValue<idx_t>() != result.row_count) {
				throw InvalidInputException("Vortex WriteBatch request_id payload mismatch");
			}
			result.snapshot_id = record->GetValue(2, 0).GetValue<idx_t>();
			owner.Commit();
			return result;
		}
		if (RequestCount(transaction) >= MAX_REQUESTS) {
			throw InvalidInputException("RESOURCE_EXHAUSTED: Vortex WriteBatch request catalog budget exceeded");
		}
		if (!epoch.registered) {
			DuckLakeVortexIndex::Query(transaction, "INSERT INTO {METADATA_CATALOG}." + string(EPOCHS) + " VALUES(" +
			                                            Value(result.table_uuid).ToSQLString() + ",1,0)");
		}
		check("after_lookup");
		auto qualified = Qualified(catalog_name, schema, table_name);
		if (mutate) {
			auto remove = owner.Prepare("DELETE FROM " + qualified + " WHERE id IN (SELECT unnest($1))");
			if (remove->HasError()) {
				remove->GetErrorObject().Throw();
			}
			vector<Value> parameters {std::move(keys)};
			auto removed = remove->Execute(parameters, false);
			if (removed->HasError()) {
				removed->ThrowError();
			}
			check("after_delete");
		}
		if (!ListValue::GetChildren(canonical).empty()) {
			auto insert =
			    owner.Prepare("INSERT INTO " + qualified +
			                  "(id,embedding,category,payload) SELECT row.id,row.embedding,row.category,row.payload "
			                  "FROM unnest($1) AS batch(row)");
			if (insert->HasError()) {
				insert->GetErrorObject().Throw();
			}
			vector<Value> parameters {std::move(canonical)};
			auto appended = insert->Execute(parameters, false);
			if (appended->HasError()) {
				appended->ThrowError();
			}
		}
		check("after_write");
		result.snapshot_id = transaction.GetSnapshot().snapshot_id + (transaction.ChangesMade() ? 1 : 0);
		DuckLakeVortexIndex::Query(
		    transaction, "INSERT INTO {METADATA_CATALOG}." + string(REQUESTS) + " VALUES(" +
		                     Value(result.table_uuid).ToSQLString() + "," + Value(request_id).ToSQLString() + "," +
		                     Value(result.payload_hash).ToSQLString() + "," + to_string(result.row_count) + "," +
		                     to_string(result.snapshot_id) + "," + to_string(result.request_epoch) + ")");
		check("before_commit");
		owner.Commit();
		check("after_commit");
		return result;
	} catch (...) {
		DuckLakeVortexIndex::Rollback(owner);
		throw;
	}
}
string DuckLakeVortexWriteBatch::RetiringTable(ClientContext &context, const string &catalog) {
	auto state = context.registered_state->Get<RetirementContext>(RETIREMENT);
	return state && state->catalog == catalog ? state->uuid : string();
}

void DuckLakeVortexWriteBatch::StageTableRetirement(ClientContext &context, DuckLakeTransaction &transaction) {
	auto state = context.registered_state->Get<RetirementContext>(RETIREMENT);
	if (!state || state->uuid.empty() || state->catalog != transaction.GetCatalog().GetName() ||
	    transaction.GetDroppedTables().size() != 1 ||
	    !transaction.GetDroppedTables().count(TableIndex(state->table_id))) {
		throw InternalException("Vortex table retirement lost its exact DROP ownership");
	}
	Connection reader(*context.db);
	try {
		auto &fresh = DuckLakeVortexIndex::Begin(reader, state->catalog);
		auto entry = fresh.GetCatalog().GetEntryById(fresh, fresh.GetSnapshot(), TableIndex(state->table_id));
		if (!entry || entry->type != CatalogType::TABLE_ENTRY ||
		    entry->Cast<DuckLakeTableEntry>().GetTableUUID() != state->uuid) {
			throw TransactionException("STALE_TABLE: Vortex retirement target no longer exists");
		}
		reader.Commit();
	} catch (...) {
		DuckLakeVortexIndex::Rollback(reader);
		throw;
	}
	state->check("retire_before_stage");
	DuckLakeVortexIndex::RetireTableMetadata(transaction, state->uuid);
	state->check("retire_after_indexes");
	auto where = " WHERE table_uuid=" + Value(state->uuid).ToSQLString();
	for (auto name : {REQUESTS, EPOCHS, MANAGED, "ducklake_vortex_write_budgets_v1", "ducklake_vortex_optimizer_v2"}) {
		DuckLakeVortexIndex::Query(transaction, "DELETE FROM {METADATA_CATALOG}." + string(name) + where);
		if (string(name) == REQUESTS) {
			state->check("retire_after_requests");
		}
	}
	state->check("retire_after_stage");
}

string DuckLakeVortexWriteBatch::RetireTable(Connection &owner, const string &catalog_name, const string &schema,
                                             const string &table_name, const string &expected_table_uuid,
                                             const std::function<void(const string &)> &observe,
                                             optional_ptr<ClientContext> caller) {
	if (owner.context->transaction.HasActiveTransaction()) {
		throw InvalidInputException("Vortex table retirement requires an idle dedicated connection");
	}
	CheckTableUUID(expected_table_uuid, expected_table_uuid);
	auto check = [&](const string &stage) {
		if (observe) {
			observe(stage);
		}
		if (owner.context->IsInterrupted() || (caller && caller->IsInterrupted())) {
			throw InterruptException();
		}
	};
	check("retire_waiting");
	auto writer = AcquireWriter();
	check("retire_acquired");
	NoCommitRetry(owner);
	MutationScope mutation(*owner.context);
	RetirementScope retirement(*owner.context);
	try {
		auto &transaction = DuckLakeVortexIndex::Begin(owner, catalog_name);
		Initialized(transaction);
		DuckLakeVortexIndex::Initialized(transaction);
		auto &catalog = transaction.GetCatalog();
		auto entry = catalog.GetEntry(*owner.context, CatalogType::TABLE_ENTRY, schema, table_name,
		                              OnEntryNotFound::RETURN_NULL);
		if (!entry) {
			auto prior =
			    DuckLakeVortexIndex::Query(
			        transaction,
			        "SELECT 1 FROM {METADATA_CATALOG}.ducklake_table t JOIN {METADATA_CATALOG}.ducklake_schema s "
			        "ON t.schema_id=s.schema_id WHERE t.table_uuid=" +
			            Value(expected_table_uuid).ToSQLString() +
			            " AND t.table_name=" + Value(table_name).ToSQLString() +
			            " AND s.schema_name=" + Value(schema).ToSQLString() + " AND t.end_snapshot IS NOT NULL LIMIT 1")
			        ->Fetch();
			if (!prior) {
				throw InvalidInputException("Unknown Vortex table retirement UUID");
			}
			for (auto name :
			     {REQUESTS, EPOCHS, MANAGED, "ducklake_vortex_write_budgets_v1", "ducklake_vortex_optimizer_v2",
			      "ducklake_vortex_indexes_v1", "ducklake_vortex_generation_refs_v1", "ducklake_vortex_builds_v1",
			      "ducklake_vortex_rewrites_v1"}) {
				auto residual =
				    DuckLakeVortexIndex::Query(
				        transaction, "SELECT 1 FROM {METADATA_CATALOG}." + string(name) +
				                         " WHERE table_uuid=" + Value(expected_table_uuid).ToSQLString() + " LIMIT 1")
				        ->Fetch();
				if (residual) {
					throw InvalidInputException("Dropped Vortex table still owns state; retirement replay refused");
				}
			}
			owner.Commit();
			return expected_table_uuid;
		}
		auto &table = entry->Cast<DuckLakeTableEntry>();
		CheckTableUUID(expected_table_uuid, table.GetTableUUID());
		DuckLakeVortex::ValidateTable(table);
		DuckLakeVortexReadView::ValidateFields(table);
		if (DuckLakeVortex::GetFormat(table) != DataFileFormat::VORTEX ||
		    table.GetColumns().PhysicalColumnCount() != 4 || table.GetColumns().LogicalColumnCount() != 4) {
			throw InvalidInputException("Vortex table retirement requires the four-column Vortex schema");
		}
		mutation.state->uuid = expected_table_uuid;
		retirement.state->catalog = catalog_name;
		retirement.state->uuid = expected_table_uuid;
		retirement.state->table_id = table.GetTableId().index;
		retirement.state->check = check;
		check("retire_after_lookup");
		auto dropped = owner.Query("DROP TABLE " + Qualified(catalog_name, schema, table_name));
		if (dropped->HasError()) {
			dropped->ThrowError();
		}
		check("retire_before_commit");
		owner.Commit();
		check("retire_after_commit");
		return expected_table_uuid;
	} catch (...) {
		DuckLakeVortexIndex::Rollback(owner);
		throw;
	}
}

} // namespace duckdb
