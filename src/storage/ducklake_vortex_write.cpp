#include "storage/ducklake_vortex_write.hpp"

#include "common/ducklake_vortex_json.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/main/prepared_statement.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/parser/keyword_helper.hpp"
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
constexpr const char *LEGACY_REQUESTS = "ducklake_vortex_write_requests_v1";
constexpr const char *REQUESTS = "ducklake_vortex_write_requests_v2";
constexpr const char *EPOCHS = "ducklake_vortex_request_epochs_v1";
constexpr const char *RETIREMENT = "ducklake_vortex_table_retirement";
constexpr const char *MANAGED = "ducklake_vortex_keyed_tables_v1";
constexpr idx_t MAX_ROWS = 1000;
constexpr idx_t MAX_BYTES = 4 * 1024 * 1024;
constexpr idx_t MAX_REQUESTS = 100000;
constexpr const char *WRITE_STATE = "ducklake_vortex_write_batch";

string Qualified(const string &catalog, const string &schema, const string &table) {
	return KeywordHelper::WriteOptionallyQuoted(catalog) + "." + KeywordHelper::WriteOptionallyQuoted(schema) + "." +
	       KeywordHelper::WriteOptionallyQuoted(table);
}

void CheckWriteInterrupted(Connection &owner, ClientContext &caller) {
	if (owner.context->IsInterrupted() || caller.IsInterrupted()) {
		throw InterruptException();
	}
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

Value CanonicalRows(const Value &input, uint32_t &dimension, string &bytes, bool mutate) {
	const auto &rows = ListValue::GetChildren(input);
	if ((!mutate && rows.empty()) || rows.size() > MAX_ROWS) {
		throw InvalidInputException("Vortex WriteBatch requires 1 to 1000 rows");
	}
	if (!rows.empty()) {
		if (rows.front().IsNull()) {
			throw InvalidInputException("Vortex WriteBatch row cannot be NULL");
		}
		const auto &first_vector = StructValue::GetChildren(rows.front())[1];
		if (first_vector.IsNull()) {
			throw InvalidInputException("Vortex WriteBatch vector dimension or NULL list mismatch");
		}
		auto components = ListValue::GetChildren(first_vector).size();
		if (!components || components > 4096) {
			throw InvalidInputException("Vortex WriteBatch vector dimension must be 1 to 4096");
		}
		dimension = static_cast<uint32_t>(components);
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

void ValidateWriteTable(ClientContext &context, DuckLakeTableEntry &table) {
	DuckLakeVortex::ValidateTable(table);
	if (DuckLakeVortex::GetFormat(context, table) != DataFileFormat::VORTEX ||
	    table.GetColumns().PhysicalColumnCount() != 4 || table.GetColumns().LogicalColumnCount() != 4) {
		throw InvalidInputException("Vortex WriteBatch requires a four-column Vortex table");
	}
	for (auto name : {"id", "embedding", "category", "payload"}) {
		DuckLakeVortexReadView::ResolveField(table, name);
	}
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
string RequestEpochWhere(const string &uuid, uint64_t epoch) {
	return " WHERE table_uuid COLLATE C=" + Value(uuid).ToSQLString() + " AND request_epoch=" + to_string(epoch);
}

void ValidateTableUUID(const string &expected) {
	if (UUID::ToString(UUID::FromString(expected)) != expected) {
		throw InvalidInputException("Vortex table UUID must be canonical");
	}
}

void CheckTableUUID(const string &expected, const string &actual) {
	ValidateTableUUID(expected);
	if (expected != actual) {
		throw InvalidInputException("STALE_TABLE: Vortex table UUID does not match the current table");
	}
}

struct RequestTarget {
	DuckLakeTableEntry &table;
	DuckLakeVortexIndex index;
};

RequestTarget ResolveRequestTable(Connection &owner, DuckLakeTransaction &transaction, const string &schema,
                                  const string &name, const string &index, const string *expected_uuid) {
	auto &table = transaction.GetCatalog()
	                  .GetEntry<TableCatalogEntry>(*owner.context, schema, name)
	                  .Cast<DuckLakeTableEntry>();
	if (expected_uuid) {
		CheckTableUUID(*expected_uuid, table.GetTableUUID());
	}
	ValidateWriteTable(*owner.context, table);
	auto descriptor = DuckLakeVortexIndex::Lookup(transaction, table, index);
	auto uuid = table.GetTableUUID();
	auto managed = DuckLakeVortexIndex::Query(
	                   transaction, "SELECT index_name FROM {METADATA_CATALOG}." + string(MANAGED) +
	                                    " WHERE table_uuid COLLATE C=" + Value(uuid).ToSQLString())
	                   ->Fetch();
	if (managed && StringValue::Get(managed->GetValue(0, 0)) != index) {
		throw InvalidInputException("Vortex request lifecycle must use the table's managed index");
	}
	return {table, std::move(descriptor)};
}

struct EpochState {
	uint64_t epoch = 1;
	idx_t expired = 0;
	bool registered = false;
	bool has_batches = false;
};

EpochState ReadEpoch(DuckLakeTransaction &transaction, const string &uuid) {
	EpochState state;
	auto record =
	    DuckLakeVortexIndex::Query(transaction, "SELECT request_epoch,last_expired_requests,has_batches FROM "
	                                            "{METADATA_CATALOG}." +
	                                                string(EPOCHS) + " WHERE table_uuid COLLATE C=" + Value(uuid).ToSQLString())
	        ->Fetch();
	if (record) {
		state.epoch = record->GetValue(0, 0).GetValue<uint64_t>();
		state.expired = record->GetValue(1, 0).GetValue<idx_t>();
		state.registered = true;
		state.has_batches = record->GetValue(2, 0).GetValue<bool>();
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

void NoCommitRetry(Connection &owner) {
	auto setting = owner.Query("SET SESSION ducklake_max_retry_count=0");
	if (setting->HasError()) {
		setting->ThrowError();
	}
}
} // namespace

DuckLakeVortexCollectionLock &DuckLakeVortexWriteBatch::WriterLock() {
	static DuckLakeVortexCollectionLock lock;
	return lock;
}

DuckLakeVortexWriteBatchState::DuckLakeVortexWriteBatchState(ClientContext &caller,
                                                             DuckLakeVortexCatalogIdentity identity, idx_t table_id,
                                                             const string &table_uuid, const DuckLakeVortexIndex &index,
                                                             bool keyed)
    : caller(caller), identity(identity), table_id(table_id), field_id(index.field_id), table_uuid(table_uuid),
      index_name(index.name), dimension(index.dimension), keyed(keyed) {
}

shared_ptr<DuckLakeVortexWriteBatchState> DuckLakeVortexWriteBatchState::Get(ClientContext &context,
                                                                             const DuckLakeCatalog &catalog) {
	auto state = context.registered_state->Get<DuckLakeVortexWriteBatchState>(WRITE_STATE);
	return state && state->identity.lake_oid == catalog.GetAttached().oid ? state : nullptr;
}

void DuckLakeVortexWriteBatchState::CheckCurrentIndex(DuckLakeTransaction &transaction) const {
	auto entry = transaction.GetCatalog().GetEntryById(transaction, transaction.GetSnapshot(), TableIndex(table_id));
	if (!entry || entry->type != CatalogType::TABLE_ENTRY ||
	    entry->Cast<DuckLakeTableEntry>().GetTableUUID() != table_uuid) {
		throw TransactionException("Vortex WriteBatch table identity changed before commit");
	}
	auto current = DuckLakeVortexIndex::Lookup(transaction, entry->Cast<DuckLakeTableEntry>(), index_name);
	if (current.dimension != dimension || current.field_id != field_id) {
		throw TransactionException("Vortex WriteBatch index specification changed before commit");
	}
}

LogicalType DuckLakeVortexWriteBatch::RowsType() {
	return LogicalType::LIST(LogicalType::STRUCT({{"id", LogicalType::BIGINT},
	                                              {"embedding", LogicalType::LIST(LogicalType::FLOAT)},
	                                              {"category", LogicalType::VARCHAR},
	                                              {"payload", LogicalType::VARCHAR}}));
}

void DuckLakeVortexWriteBatch::Initialize(Connection &owner, ClientContext &caller, const string &catalog) {
	if (owner.context->transaction.HasActiveTransaction()) {
		throw InvalidInputException("Vortex WriteBatch initialization requires an idle dedicated connection");
	}
	auto identity = DuckLakeVortexIndex::ResolveCatalog(caller, catalog, true, "Vortex WriteBatch initialization");
	auto interrupt = [&] {
		CheckWriteInterrupted(owner, caller);
	};
	auto guard = WriterLock().GetExclusiveLock(interrupt);
	try {
		auto &transaction = DuckLakeVortexIndex::Begin(owner, catalog, &identity);
		auto &metadata = transaction.GetCatalog();
		if (transaction.GetConnection().TableInfo(metadata.MetadataDatabaseName(), metadata.MetadataSchemaName(),
		                                          LEGACY_REQUESTS)) {
			throw InvalidInputException(
			    "Vortex request epochs require a fresh write catalog; legacy receipts are unsupported");
		}
		DuckLakeVortexIndex::Query(transaction, "CREATE TABLE {METADATA_CATALOG}." + string(EPOCHS) +
		                                            "(table_uuid VARCHAR PRIMARY KEY,request_epoch UBIGINT NOT NULL "
		                                            "CHECK(request_epoch>0),last_expired_requests UBIGINT NOT NULL,"
		                                            "has_batches BOOLEAN NOT NULL)");
		DuckLakeVortexIndex::Query(transaction,
		                           "CREATE TABLE {METADATA_CATALOG}." + string(REQUESTS) +
		                               "(table_uuid VARCHAR NOT NULL,request_id VARCHAR NOT NULL,"
		                               "payload_hash VARCHAR NOT NULL,row_count UBIGINT NOT NULL,"
		                               "snapshot_id UBIGINT NOT NULL,request_epoch UBIGINT NOT NULL "
		                               "CHECK(request_epoch>0),PRIMARY KEY(table_uuid,request_epoch,request_id))");
		DuckLakeVortexIndex::Query(transaction, "CREATE TABLE {METADATA_CATALOG}." + string(MANAGED) +
		                                            "(table_uuid VARCHAR PRIMARY KEY,table_id BIGINT UNIQUE NOT NULL,"
		                                            "index_name VARCHAR NOT NULL,dimension UINTEGER NOT NULL)");
		interrupt();
		owner.Commit();
	} catch (...) {
		DuckLakeVortexIndex::Rollback(owner);
		throw;
	}
}

void DuckLakeVortexWriteBatch::Manage(Connection &owner, ClientContext &caller, const string &catalog_name,
                                      const string &schema, const string &table_name, const string &index_name) {
	if (owner.context->transaction.HasActiveTransaction()) {
		throw InvalidInputException("Vortex managed writes require an idle dedicated connection");
	}
	auto identity = DuckLakeVortexIndex::ResolveCatalog(caller, catalog_name, true, "Vortex managed writes");
	auto interrupt = [&] {
		CheckWriteInterrupted(owner, caller);
	};
	auto guard = WriterLock().GetExclusiveLock(interrupt);
	auto admission = DuckLakeVortexOptimizer::WriteAdmissionLock(*owner.context->db).GetExclusiveLock(interrupt);
	auto publication = DuckLakeVortexIndex::PublicationLock().GetSharedLock(interrupt);
	try {
		auto &transaction = DuckLakeVortexIndex::Begin(owner, catalog_name, &identity);
		Initialized(transaction);
		auto &table = transaction.GetCatalog()
		                  .GetEntry<TableCatalogEntry>(*owner.context, schema, table_name)
		                  .Cast<DuckLakeTableEntry>();
		ValidateWriteTable(*owner.context, table);
		auto uuid = table.GetTableUUID();
		auto table_id = table.GetTableId();
		auto index = DuckLakeVortexIndex::Lookup(transaction, table, index_name);
		auto rows = owner.Query("SELECT 1 FROM " + Qualified(catalog_name, schema, table_name) + " LIMIT 1");
		if (rows->HasError()) {
			rows->ThrowError();
		}
		if (rows->Fetch() || ReadEpoch(transaction, uuid).has_batches ||
		    DuckLakeVortexIndex::HasSegments(transaction, uuid)) {
			throw InvalidInputException(
			    "Vortex managed writes require an empty table without prior batches or B segments");
		}
		DuckLakeVortexIndex::Query(transaction, "INSERT INTO {METADATA_CATALOG}." + string(MANAGED) + " VALUES(" +
		                                            Value(uuid).ToSQLString() + "," + to_string(table_id.index) + "," +
		                                            Value(index.name).ToSQLString() + "," + to_string(index.dimension) +
		                                            ")");
		interrupt();
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
	auto owner = DuckLakeVortexWriteBatchState::Get(context, catalog);
	auto retirement = DuckLakeVortexTableRetirement::Get(context, catalog);
	auto rewrite = DuckLakeVortexRewriteState::Get(context);
	for (auto &id : tables) {
		auto result = DuckLakeVortexIndex::Query(fresh, "SELECT table_uuid FROM {METADATA_CATALOG}." + string(MANAGED) +
		                                                    " WHERE table_id=" + to_string(id.index));
		auto row = result->Fetch();
		if (!row) {
			continue;
		}
		auto uuid = StringValue::Get(row->GetValue(0, 0));
		if ((!owner || !owner->keyed || owner->table_uuid != uuid) && (!rewrite || rewrite->table_uuid != uuid) &&
		    (!retirement || retirement->table_id != id || retirement->uuid != uuid)) {
			throw InvalidInputException(
			    "Managed Vortex tables require ducklake_mutate_vortex_batch for logical writes and fixed schema");
		}
	}
}

DuckLakeVortexRequestStatus DuckLakeVortexWriteBatch::RequestStatus(
    Connection &owner, ClientContext &caller, const string &catalog, const string &schema, const string &table,
    const string &index) {
	if (owner.context->transaction.HasActiveTransaction() || !caller.transaction.IsAutoCommit()) {
		throw InvalidInputException("Vortex request status requires autocommit and an idle dedicated connection");
	}
	auto identity = DuckLakeVortexIndex::ResolveCatalog(caller, catalog, false, "Vortex request status");
	auto interrupt = [&] { CheckWriteInterrupted(owner, caller); };
	auto guard = WriterLock().GetExclusiveLock(interrupt);
	interrupt();
	try {
		auto &transaction = DuckLakeVortexIndex::Begin(owner, catalog, &identity);
		Initialized(transaction);
		auto target = ResolveRequestTable(owner, transaction, schema, table, index, nullptr);
		auto uuid = target.table.GetTableUUID();
		auto epoch = ReadEpoch(transaction, uuid);
		DuckLakeVortexRequestStatus result {
		    uuid, epoch.epoch, RequestCount(transaction, RequestEpochWhere(uuid, epoch.epoch)),
		    RequestCount(transaction)};
		interrupt();
		owner.Commit();
		return result;
	} catch (...) {
		DuckLakeVortexIndex::Rollback(owner);
		throw;
	}
}

DuckLakeVortexRequestAdvance
DuckLakeVortexWriteBatch::AdvanceRequestEpoch(Connection &owner, ClientContext &caller, const string &catalog,
                                              const string &schema, const string &table, const string &index,
                                              uint64_t expected_epoch, const string &expected_table_uuid,
                                              const std::function<void(const string &)> &observe) {
	if (owner.context->transaction.HasActiveTransaction() || !caller.transaction.IsAutoCommit() || !expected_epoch) {
		throw InvalidInputException("Vortex request epoch advance requires autocommit, an idle dedicated connection "
		                            "and a positive epoch");
	}
	auto identity = DuckLakeVortexIndex::ResolveCatalog(caller, catalog, true, "Vortex request epoch advance");
	auto check = [&](const string &stage) {
		if (observe) {
			observe(stage);
		}
		CheckWriteInterrupted(owner, caller);
	};
	check("waiting");
	auto guard = WriterLock().GetExclusiveLock([&] { check("lock_wait"); });
	check("acquired");
	NoCommitRetry(owner);
	try {
		auto &transaction = DuckLakeVortexIndex::Begin(owner, catalog, &identity);
		Initialized(transaction);
		auto target = ResolveRequestTable(owner, transaction, schema, table, index, &expected_table_uuid);
		auto uuid = target.table.GetTableUUID();
		auto epoch = ReadEpoch(transaction, uuid);
		check("after_lookup");
		if (expected_epoch < std::numeric_limits<uint64_t>::max() && epoch.epoch == expected_epoch + 1) {
			DuckLakeVortexRequestAdvance result {uuid, expected_epoch, epoch.epoch, epoch.expired};
			CheckWriteInterrupted(owner, caller);
			owner.Commit();
			return result;
		}
		if (expected_epoch != epoch.epoch) {
			throw InvalidInputException("Vortex request epoch advance expected epoch does not match current epoch");
		}
		if (expected_epoch == std::numeric_limits<uint64_t>::max()) {
			throw InvalidInputException("RESOURCE_EXHAUSTED: Vortex request epoch is exhausted");
		}
		auto where = RequestEpochWhere(uuid, expected_epoch);
		auto count = RequestCount(transaction, where);
		DuckLakeVortexIndex::Query(transaction, "DELETE FROM {METADATA_CATALOG}." + string(REQUESTS) + where);
		check("after_prune");
		auto next = expected_epoch + 1;
		if (epoch.registered) {
			DuckLakeVortexIndex::Query(transaction, "UPDATE {METADATA_CATALOG}." + string(EPOCHS) +
			                                            " SET request_epoch=" + to_string(next) +
			                                            ",last_expired_requests=" + to_string(count) +
			                                            " WHERE table_uuid COLLATE C=" + Value(uuid).ToSQLString());
		} else {
			DuckLakeVortexIndex::Query(transaction, "INSERT INTO {METADATA_CATALOG}." + string(EPOCHS) + " VALUES(" +
			                                            Value(uuid).ToSQLString() + "," + to_string(next) + "," +
			                                            to_string(count) + ",false)");
		}
		check("after_epoch");
		DuckLakeVortexRequestAdvance result {uuid, expected_epoch, next, count};
		auto commit_state = make_shared_ptr<DuckLakeVortexWriteBatchState>(
		    caller, identity, target.table.GetTableId().index, uuid, target.index, false);
		check("before_commit");
		// Epoch retirement only writes metadata. Apply the same fresh commit checks as a receipt-only batch.
		owner.context->registered_state->Insert(WRITE_STATE, std::move(commit_state));
		owner.Commit();
		owner.context->registered_state->Remove(WRITE_STATE);
		check("after_commit");
		return result;
	} catch (...) {
		owner.context->registered_state->Remove(WRITE_STATE);
		DuckLakeVortexIndex::Rollback(owner);
		throw;
	}
}

DuckLakeVortexWriteResult DuckLakeVortexWriteBatch::Append(Connection &owner, ClientContext &caller,
                                                           const string &catalog_name, const string &schema,
                                                           const string &table_name, const string &index_name,
                                                           const string &request_id, const Value &rows,
                                                           uint64_t request_epoch, const string &expected_table_uuid,
                                                           const std::function<void(const string &)> &observe) {
	return Write(owner, caller, catalog_name, schema, table_name, index_name, request_id, rows,
	             Value::LIST(LogicalType::BIGINT, {}), false, request_epoch, expected_table_uuid, observe);
}

DuckLakeVortexWriteResult
DuckLakeVortexWriteBatch::Mutate(Connection &owner, ClientContext &caller, const string &catalog, const string &schema,
                                 const string &table, const string &index, const string &request_id, const Value &rows,
                                 const Value &deletes, uint64_t request_epoch, const string &expected_table_uuid,
                                 const std::function<void(const string &)> &observe) {
	return Write(owner, caller, catalog, schema, table, index, request_id, rows, deletes, true, request_epoch,
	             expected_table_uuid, observe);
}

DuckLakeVortexWriteResult DuckLakeVortexWriteBatch::Write(Connection &owner, ClientContext &caller,
                                                          const string &catalog_name, const string &schema,
                                                          const string &table_name, const string &index_name,
                                                          const string &request_id, const Value &rows,
                                                          const Value &deletes, bool mutate, uint64_t request_epoch,
                                                          const string &expected_table_uuid,
                                                          const std::function<void(const string &)> &observe) {
	if (owner.context->transaction.HasActiveTransaction()) {
		throw InvalidInputException("Vortex WriteBatch requires an idle dedicated connection");
	}
	if (!request_epoch || request_id.empty() || request_id.size() > 128 || request_id.find('\0') != string::npos || rows.IsNull() ||
	    rows.type() != RowsType() || deletes.IsNull() || deletes.type() != LogicalType::LIST(LogicalType::BIGINT)) {
		throw InvalidInputException("Invalid Vortex WriteBatch request epoch, request ID or typed rows");
	}
	auto identity = DuckLakeVortexIndex::ResolveCatalog(caller, catalog_name, true, "Vortex WriteBatch");
	auto interrupt = [&] {
		CheckWriteInterrupted(owner, caller);
	};
	auto check = [&](const string &stage) {
		if (observe) {
			observe(stage);
		}
		interrupt();
	};
	check("waiting");
	auto guard = WriterLock().GetExclusiveLock([&] { check("lock_wait"); });
	check("acquired");
	// Metadata-side request staging must not be discarded by a native commit retry.
	NoCommitRetry(owner);
	try {
		auto &transaction = DuckLakeVortexIndex::Begin(owner, catalog_name, &identity);
		Initialized(transaction);
		auto &catalog = transaction.GetCatalog();
		auto &table =
		    catalog.GetEntry<TableCatalogEntry>(*owner.context, schema, table_name).Cast<DuckLakeTableEntry>();
		DuckLakeVortexWriteResult result;
		result.table_uuid = table.GetTableUUID();
		CheckTableUUID(expected_table_uuid, result.table_uuid);
		result.request_id = request_id;
		result.request_epoch = request_epoch;
		auto epoch = ReadEpoch(transaction, result.table_uuid);
		if (request_epoch < epoch.epoch) {
			throw InvalidInputException("EXPIRED_REQUEST: Vortex request epoch has been retired");
		}
		if (request_epoch != epoch.epoch) {
			throw InvalidInputException("Vortex request epoch does not match current epoch");
		}
		auto managed = DuckLakeVortexIndex::Query(
		                   transaction, "SELECT index_name,dimension FROM {METADATA_CATALOG}." + string(MANAGED) +
		                                    " WHERE table_uuid COLLATE C=" + Value(result.table_uuid).ToSQLString())
		                   ->Fetch();
		if ((mutate && (!managed || StringValue::Get(managed->GetValue(0, 0)) != index_name)) || (!mutate && managed)) {
			throw InvalidInputException(
			    "Vortex batch operation does not match the table's explicitly managed write mode");
		}
		auto managed_dimension = managed ? managed->GetValue(1, 0).GetValue<uint32_t>() : 0;
		auto dimension = managed_dimension;
		string bytes;
		auto canonical = CanonicalRows(rows, dimension, bytes, mutate);
		auto keys = mutate ? MutationKeys(canonical, deletes, bytes) : Value::LIST(LogicalType::BIGINT, {});
		result.payload_hash = "sha256:" + vortex_detail::Hex(duckdb_mbedtls::MbedTlsWrapper::ComputeSha256Hash(bytes));
		result.row_count = ListValue::GetChildren(canonical).size();
		if (mutate) {
			result.row_count += ListValue::GetChildren(deletes).size();
		}
		auto where = RequestEpochWhere(result.table_uuid, request_epoch) +
		             " AND request_id COLLATE C=" + Value(request_id).ToSQLString();
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
			interrupt();
			owner.Commit();
			return result;
		}
		ValidateWriteTable(*owner.context, table);
		auto index = DuckLakeVortexIndex::Lookup(transaction, table, index_name);
		if (dimension != index.dimension || (mutate && managed_dimension != index.dimension)) {
			throw InvalidInputException("Vortex WriteBatch vector dimension mismatch");
		}
		auto commit_state = make_shared_ptr<DuckLakeVortexWriteBatchState>(caller, identity, table.GetTableId().index,
		                                                                   result.table_uuid, index, mutate);
		if (RequestCount(transaction) >= MAX_REQUESTS) {
			throw InvalidInputException("RESOURCE_EXHAUSTED: Vortex WriteBatch request catalog budget exceeded");
		}
		if (!epoch.registered) {
			DuckLakeVortexIndex::Query(transaction, "INSERT INTO {METADATA_CATALOG}." + string(EPOCHS) + " VALUES(" +
			                                            Value(result.table_uuid).ToSQLString() + ",1,0,true)");
		} else if (!epoch.has_batches) {
			DuckLakeVortexIndex::Query(transaction, "UPDATE {METADATA_CATALOG}." + string(EPOCHS) +
			                                            " SET has_batches=true WHERE table_uuid COLLATE C=" +
			                                            Value(result.table_uuid).ToSQLString());
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
		DuckLakeVortexIndex::Query(transaction,
		                           "INSERT INTO {METADATA_CATALOG}." + string(REQUESTS) + " VALUES(" +
		                               Value(result.table_uuid).ToSQLString() + "," + Value(request_id).ToSQLString() +
		                               "," + Value(result.payload_hash).ToSQLString() + "," +
		                               to_string(result.row_count) + "," + to_string(result.snapshot_id) + "," +
		                               to_string(result.request_epoch) + ")");
		check("before_commit");
		owner.context->registered_state->Insert(WRITE_STATE, std::move(commit_state));
		owner.Commit();
		owner.context->registered_state->Remove(WRITE_STATE);
		check("after_commit");
		return result;
	} catch (...) {
		owner.context->registered_state->Remove(WRITE_STATE);
		DuckLakeVortexIndex::Rollback(owner);
		throw;
	}
}
shared_ptr<DuckLakeVortexTableRetirement> DuckLakeVortexTableRetirement::Get(ClientContext &context,
                                                                             const DuckLakeCatalog &catalog) {
	auto state = context.registered_state->Get<DuckLakeVortexTableRetirement>(RETIREMENT);
	return state && state->identity.lake_oid == catalog.GetAttached().oid ? state : nullptr;
}

void DuckLakeVortexTableRetirement::Stage(DuckLakeTransaction &transaction) const {
	if (transaction.GetDroppedTables().size() != 1 || !transaction.GetDroppedTables().count(table_id)) {
		throw InternalException("Vortex table retirement lost its exact DROP ownership");
	}
	check("retire_before_stage");
	DuckLakeVortexIndex::RetireTableMetadata(transaction, uuid);
	check("retire_after_indexes");
	auto where = " WHERE table_uuid COLLATE C=" + Value(uuid).ToSQLString();
	DuckLakeVortexIndex::Query(transaction, "DELETE FROM {METADATA_CATALOG}." + string(REQUESTS) + where);
	check("retire_after_requests");
	for (auto name : {EPOCHS, MANAGED, "ducklake_vortex_write_budgets_v1",
	                  "ducklake_vortex_optimizer_rewrite_receipts_v1", "ducklake_vortex_optimizer_v2"}) {
		DuckLakeVortexIndex::Query(transaction, "DELETE FROM {METADATA_CATALOG}." + string(name) + where);
	}
	check("retire_after_stage");
}

string DuckLakeVortexWriteBatch::RetireTable(Connection &owner, ClientContext &caller, const string &catalog_name,
                                             const string &schema, const string &table_name,
                                             const string &expected_table_uuid,
                                             const std::function<void(const string &)> &observe) {
	if (owner.context->transaction.HasActiveTransaction()) {
		throw InvalidInputException("Vortex table retirement requires an idle dedicated connection");
	}
	auto identity = DuckLakeVortexIndex::ResolveCatalog(caller, catalog_name, true, "Vortex table retirement");
	ValidateTableUUID(expected_table_uuid);
	auto check = [&](const string &stage) {
		if (observe) {
			observe(stage);
		}
		CheckWriteInterrupted(owner, caller);
	};
	check("retire_waiting");
	auto writer = WriterLock().GetExclusiveLock([&] { check("retire_lock_wait"); });
	check("retire_acquired");
	NoCommitRetry(owner);
	auto state =
	    make_shared_ptr<DuckLakeVortexTableRetirement>(*owner.context->db, identity, expected_table_uuid, check);
	try {
		auto &transaction = DuckLakeVortexIndex::Begin(owner, catalog_name, &identity);
		Initialized(transaction);
		DuckLakeVortexIndex::Initialized(transaction);
		auto &catalog = transaction.GetCatalog();
		auto entry =
		    catalog.GetEntry<TableCatalogEntry>(*owner.context, schema, table_name, OnEntryNotFound::RETURN_NULL);
		if (!entry) {
			// Every snapshot from this version's end through the latest must survive: a gap could hide a successor.
			auto prior =
			    DuckLakeVortexIndex::Query(
			        transaction,
			        "SELECT named.table_uuid::VARCHAR FROM (SELECT t.table_uuid,t.begin_snapshot,t.end_snapshot "
			        "FROM {METADATA_CATALOG}.ducklake_table t JOIN "
			        "{METADATA_CATALOG}.ducklake_schema s ON t.schema_id=s.schema_id WHERE "
			        "translate(t.table_name,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz') COLLATE C=" +
			            Value(StringUtil::Lower(table_name)).ToSQLString() +
			            " AND "
			            "translate(s.schema_name,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz') COLLATE "
			            "C=" +
			            Value(StringUtil::Lower(schema)).ToSQLString() +
			            " AND "
			            "t.end_snapshot IS NOT NULL AND s.begin_snapshot < t.end_snapshot AND "
			            "(s.end_snapshot IS NULL OR s.end_snapshot >= t.end_snapshot) "
			            "ORDER BY t.end_snapshot DESC,t.begin_snapshot DESC LIMIT 1) named "
			            "WHERE NOT EXISTS (SELECT 1 FROM {METADATA_CATALOG}.ducklake_table v "
			            "WHERE v.table_uuid=named.table_uuid AND "
			            "(v.end_snapshot IS NULL OR v.begin_snapshot>named.begin_snapshot)) "
			            "AND (SELECT count(*) = max(snapshot_id) - named.end_snapshot + 1 "
			            "FROM {METADATA_CATALOG}.ducklake_snapshot WHERE snapshot_id>=named.end_snapshot)")
			        ->Fetch();
			if (!prior || StringValue::Get(prior->GetValue(0, 0)) != expected_table_uuid) {
				throw InvalidInputException("Unknown Vortex table retirement UUID");
			}
			for (auto name :
			     {REQUESTS, EPOCHS, MANAGED, "ducklake_vortex_write_budgets_v1", "ducklake_vortex_optimizer_v2",
			      "ducklake_vortex_optimizer_rewrite_receipts_v1", "ducklake_vortex_indexes_v1",
			      "ducklake_vortex_generation_refs_v1", "ducklake_vortex_builds_v1", "ducklake_vortex_rewrites_v1"}) {
				if (DuckLakeVortexIndex::Query(transaction, "SELECT 1 FROM {METADATA_CATALOG}." + string(name) +
				                                                " WHERE table_uuid COLLATE C=" +
				                                                Value(expected_table_uuid).ToSQLString() + " LIMIT 1")
				        ->Fetch()) {
					throw InvalidInputException("Dropped Vortex table still owns state; retirement replay refused");
				}
			}
			CheckWriteInterrupted(owner, caller);
			owner.Commit();
			return expected_table_uuid;
		}
		auto &table = entry->Cast<DuckLakeTableEntry>();
		CheckTableUUID(expected_table_uuid, table.GetTableUUID());
		ValidateWriteTable(*owner.context, table);
		state->table_id = table.GetTableId();
		owner.context->registered_state->Insert(RETIREMENT, state);
		check("retire_after_lookup");
		auto dropped = owner.Query("DROP TABLE " + Qualified(catalog_name, schema, table_name));
		if (dropped->HasError()) {
			dropped->ThrowError();
		}
		check("retire_before_commit");
		owner.Commit();
		owner.context->registered_state->Remove(RETIREMENT);
		state.reset();
		check("retire_after_commit");
		return expected_table_uuid;
	} catch (...) {
		owner.context->registered_state->Remove(RETIREMENT);
		DuckLakeVortexIndex::Rollback(owner);
		throw;
	}
}

} // namespace duckdb
