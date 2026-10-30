#include "storage/ducklake_vortex_write.hpp"

#include "common/ducklake_vortex_json.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/prepared_statement.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/storage/storage_lock.hpp"
#include "duckdb/transaction/transaction_context.hpp"
#include "mbedtls_wrapper.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_vortex.hpp"
#include "storage/ducklake_vortex_index_catalog.hpp"
#include "storage/ducklake_vortex_read_view.hpp"

#include <cmath>
#include <cstring>

namespace duckdb {
namespace {
constexpr const char *REQUESTS = "ducklake_vortex_write_requests_v1";
constexpr idx_t MAX_ROWS = 1000;
constexpr idx_t MAX_BYTES = 4 * 1024 * 1024;
constexpr idx_t MAX_REQUESTS = 100000;

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

Value CanonicalRows(const Value &input, uint32_t dimension, string &hash) {
	const auto &rows = ListValue::GetChildren(input);
	if (rows.empty() || rows.size() > MAX_ROWS) {
		throw InvalidInputException("Vortex WriteBatch requires 1 to 1000 rows");
	}
	string bytes = "ducklake-vortex-write-v1";
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
	hash = "sha256:" + vortex_detail::Hex(duckdb_mbedtls::MbedTlsWrapper::ComputeSha256Hash(bytes));
	return Value::LIST(ListType::GetChildType(DuckLakeVortexWriteBatch::RowsType()), std::move(canonical));
}

void Initialized(DuckLakeTransaction &transaction) {
	auto &catalog = transaction.GetCatalog();
	if (!transaction.GetConnection().TableInfo(catalog.MetadataDatabaseName(), catalog.MetadataSchemaName(),
	                                           REQUESTS)) {
		throw InvalidInputException("Vortex WriteBatch catalog is not initialized");
	}
}
} // namespace

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
	auto guard = WriteLock().GetExclusiveLock();
	try {
		auto &transaction = DuckLakeVortexIndex::Begin(owner, catalog);
		DuckLakeVortexIndex::Query(transaction, "CREATE TABLE {METADATA_CATALOG}." + string(REQUESTS) +
		                                            "(table_uuid VARCHAR NOT NULL,request_id VARCHAR NOT NULL,"
		                                            "payload_hash VARCHAR NOT NULL,row_count UBIGINT NOT NULL,"
		                                            "snapshot_id UBIGINT NOT NULL,PRIMARY KEY(table_uuid,request_id))");
		owner.Commit();
	} catch (...) {
		DuckLakeVortexIndex::Rollback(owner);
		throw;
	}
}

DuckLakeVortexWriteResult DuckLakeVortexWriteBatch::Append(Connection &owner, const string &catalog_name,
                                                           const string &schema, const string &table_name,
                                                           const string &index_name, const string &request_id,
                                                           const Value &rows,
                                                           const std::function<void(const string &)> &observe,
                                                           optional_ptr<ClientContext> caller) {
	if (owner.context->transaction.HasActiveTransaction()) {
		throw InvalidInputException("Vortex WriteBatch requires an idle dedicated connection");
	}
	if (request_id.empty() || request_id.size() > 128 || request_id.find('\0') != string::npos || rows.IsNull() ||
	    rows.type() != RowsType()) {
		throw InvalidInputException("Invalid Vortex WriteBatch request ID or typed rows");
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
	auto guard = WriteLock().GetExclusiveLock();
	check("acquired");
	// Metadata-side request staging must not be discarded by a native commit retry.
	auto setting = owner.Query("SET SESSION ducklake_max_retry_count=0");
	if (setting->HasError()) {
		setting->ThrowError();
	}
	try {
		auto &transaction = DuckLakeVortexIndex::Begin(owner, catalog_name);
		Initialized(transaction);
		auto &catalog = transaction.GetCatalog();
		auto entry = catalog.GetEntry(*owner.context, CatalogType::TABLE_ENTRY, schema, table_name,
		                              OnEntryNotFound::THROW_EXCEPTION);
		auto &table = entry->Cast<DuckLakeTableEntry>();
		DuckLakeVortex::ValidateTable(table);
		DuckLakeVortexReadView::ValidateFields(table);
		if (DuckLakeVortex::GetFormat(table) != DataFileFormat::VORTEX ||
		    table.GetColumns().PhysicalColumnCount() != 4 || table.GetColumns().LogicalColumnCount() != 4) {
			throw InvalidInputException("Vortex WriteBatch requires a four-column Vortex table");
		}
		auto index = DuckLakeVortexIndex::Lookup(transaction, table, index_name);
		DuckLakeVortexWriteResult result;
		result.table_uuid = table.GetTableUUID();
		result.request_id = request_id;
		auto canonical = CanonicalRows(rows, index.dimension, result.payload_hash);
		result.row_count = ListValue::GetChildren(canonical).size();
		auto where = " WHERE table_uuid=" + Value(result.table_uuid).ToSQLString() +
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
		auto count =
		    DuckLakeVortexIndex::Query(transaction, "SELECT count(*) FROM {METADATA_CATALOG}." + string(REQUESTS));
		if (count->Fetch()->GetValue(0, 0).GetValue<idx_t>() >= MAX_REQUESTS) {
			throw InvalidInputException("RESOURCE_EXHAUSTED: Vortex WriteBatch request catalog budget exceeded");
		}
		result.snapshot_id = transaction.GetSnapshot().snapshot_id + 1;
		check("after_lookup");
		auto qualified = KeywordHelper::WriteOptionallyQuoted(catalog_name) + "." +
		                 KeywordHelper::WriteOptionallyQuoted(schema) + "." +
		                 KeywordHelper::WriteOptionallyQuoted(table_name);
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
		DuckLakeVortexIndex::Query(transaction,
		                           "INSERT INTO {METADATA_CATALOG}." + string(REQUESTS) + " VALUES(" +
		                               Value(result.table_uuid).ToSQLString() + "," + Value(request_id).ToSQLString() +
		                               "," + Value(result.payload_hash).ToSQLString() + "," +
		                               to_string(result.row_count) + "," + to_string(result.snapshot_id) + ")");
		check("before_commit");
		owner.Commit();
		check("after_commit");
		return result;
	} catch (...) {
		DuckLakeVortexIndex::Rollback(owner);
		throw;
	}
}
} // namespace duckdb
