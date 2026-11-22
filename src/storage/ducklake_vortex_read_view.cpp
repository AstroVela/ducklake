#include "storage/ducklake_vortex_read_view.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/transaction/transaction_context.hpp"
#include "duckdb/storage/storage_lock.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_transaction.hpp"
#include "mbedtls_wrapper.hpp"
#include "common/ducklake_vortex_json.hpp"
#include "storage/ducklake_vortex_index_catalog.hpp"

#include <algorithm>
#include <cmath>
#include <memory>

namespace duckdb {

using namespace duckdb_yyjson;
using namespace vortex_detail;

namespace {

yyjson_mut_val *Business(ViewJSON &json, const vector<Value> &values, uint32_t dimension) {
	auto row = json.Object();
	if (values[0].IsNull()) {
		json.Add(row, "id", yyjson_mut_null(json.doc.get()));
	} else {
		json.Add(row, "id", yyjson_mut_sint(json.doc.get(), values[0].GetValue<int64_t>()));
	}
	if (values[1].IsNull() || ListValue::GetChildren(values[1]).size() != dimension) {
		throw InvalidInputException("Read view vector dimension or NULL list mismatch");
	}
	auto embedding = json.Array();
	for (auto &component : ListValue::GetChildren(values[1])) {
		if (component.IsNull() || !std::isfinite(component.GetValue<float>())) {
			throw InvalidInputException("Read view vectors require finite non-NULL components");
		}
		yyjson_mut_arr_append(embedding, yyjson_mut_real(json.doc.get(), component.GetValue<float>()));
	}
	json.Add(row, "embedding", embedding);
	json.NullableText(row, "category", values[2]);
	json.NullableText(row, "payload", values[3]);
	return row;
}

} // namespace

StorageLock &DuckLakeVortexReadView::AdmissionLock() {
	static StorageLock lock;
	return lock;
}

DuckLakeVortexReadView DuckLakeVortexReadView::Acquire(Connection &reader, const DuckLakeVortexViewOptions &options,
                                                       const std::function<void(const string &)> &observe,
                                                       optional_ptr<ClientContext> owner) {
	const bool invalid_dimension =
	    options.index_name.empty() ? options.dimension == 0 || options.dimension > 4096 : options.dimension != 0;
	if (invalid_dimension || options.max_source_bytes == 0 || options.max_source_bytes > 64 * 1024 * 1024 ||
	    options.max_rows == 0 || options.max_rows > 100000) {
		throw InvalidInputException("Invalid native Vortex read view budgets or dimension");
	}
	if (reader.context->transaction.HasActiveTransaction()) {
		throw InvalidInputException("Read view acquisition requires an idle dedicated connection");
	}
	auto checkpoint = [&](const string &stage) {
		if (observe) {
			observe(stage);
		}
		if (reader.context->IsInterrupted() || (owner && owner->IsInterrupted())) {
			throw InterruptException();
		}
	};
	reader.BeginTransaction();
	try {
		auto &catalog = Catalog::GetCatalog(*reader.context, options.catalog).Cast<DuckLakeCatalog>();
		if (!catalog.MetadataType().empty() && catalog.MetadataType() != "duckdb") {
			throw InvalidInputException("Native Vortex read views require DuckDB metadata");
		}
		auto guard = AdmissionLock().GetSharedLock();
		auto &transaction = DuckLakeTransaction::Get(*reader.context, catalog);
		auto snapshot = transaction.GetSnapshot();
		checkpoint("after_snapshot");
		auto entry = catalog.GetEntry(*reader.context, CatalogType::TABLE_ENTRY, options.schema, options.table,
		                              OnEntryNotFound::THROW_EXCEPTION);
		auto &table = entry->Cast<DuckLakeTableEntry>();
		unique_ptr<DuckLakeVortexIndex> index;
		auto dimension = options.dimension;
		if (!options.index_name.empty()) {
			index = make_uniq<DuckLakeVortexIndex>(DuckLakeVortexIndex::Lookup(transaction, table, options.index_name));
			dimension = index->dimension;
		}
		ViewJSON json;
		auto admission = json.Object();
		if (index) {
			auto record = index->ToJSON();
			std::unique_ptr<yyjson_doc, decltype(&yyjson_doc_free)> parsed(yyjson_read(record.data(), record.size(), 0),
			                                                               yyjson_doc_free);
			if (!parsed) {
				throw InvalidInputException("Invalid catalog index record");
			}
			json.Add(admission, "index", yyjson_val_mut_copy(json.doc.get(), yyjson_doc_get_root(parsed.get())));
		}
		json.Number(admission, "format_version", 1);
		auto mixed = json.Object();
		json.Number(mixed, "format_version", 1);
		json.Add(admission, "capture", mixed);
		auto captured = json.Object();
		json.Add(mixed, "file_capture", captured);
		json.Number(captured, "format_version", 1);
		json.Text(captured, "table_uuid", table.GetTableUUID());
		json.Number(captured, "snapshot_id", snapshot.snapshot_id);
		json.Number(captured, "schema_version", snapshot.schema_version);
		json.Number(captured, "dimension", dimension);
		auto business = json.Object();
		json.Add(captured, "business_fields", business);
		for (auto &name : {"id", "embedding", "category", "payload"}) {
			auto &field = table.GetFieldId(vector<string> {name});
			auto expected = string(name) == "id"          ? LogicalType::BIGINT
			                : string(name) == "embedding" ? LogicalType::LIST(LogicalType::FLOAT)
			                                              : LogicalType::VARCHAR;
			if (field.Type() != expected) {
				throw InvalidInputException("Native read view field type mismatch: %s", name);
			}
			json.Number(string(name) == "embedding" ? captured : business,
			            string(name) == "embedding" ? "field_id" : name, field.GetFieldIndex().index);
		}
		auto files = transaction.GetMetadataManager().GetExtendedFilesForTable(table, snapshot);
		std::sort(files.begin(), files.end(),
		          [](const DuckLakeFileListExtendedEntry &a, const DuckLakeFileListExtendedEntry &b) {
			          return a.file_id < b.file_id;
		          });
		checkpoint("after_inventory");
		auto inventory = json.Array();
		auto sources = json.Array();
		json.Add(captured, "files", inventory);
		json.Add(admission, "sources", sources);
		map<string, idx_t> by_path;
		DuckLakeVortexReadView result;
		result.snapshot_id = snapshot.snapshot_id;
		auto &fs = FileSystem::GetFileSystem(*reader.context);
		idx_t physical_rows = 0;
		vector<DuckLakeVortexIndexFile> identities;
		const auto row_limit = MinValue<idx_t>(options.max_rows, 64 * 1024 * 1024 / sizeof(float) / dimension);
		for (auto &file : files) {
			if (file.data_type != DuckLakeDataType::DATA_FILE) {
				continue;
			}
			if (file.file.data_format != DataFileFormat::VORTEX || !file.file.encryption_key.empty()) {
				throw InvalidInputException("Native read views require unencrypted Vortex files");
			}
			if (file.row_count > row_limit - physical_rows) {
				throw InvalidInputException("Read view physical row budget exceeded");
			}
			physical_rows += file.row_count;
			auto handle = fs.OpenFile(file.file.path, FileFlags::FILE_FLAGS_READ);
			auto size = handle->GetFileSize();
			if (size < 0 || idx_t(size) != file.file.file_size_bytes ||
			    idx_t(size) > options.max_source_bytes - result.source_bytes) {
				throw InvalidInputException("Read view source byte budget or file size mismatch");
			}
			string bytes(idx_t(size), '\0');
			if (size) {
				handle->Read(&bytes[0], size, 0);
			}
			result.source_bytes += size;
			auto descriptor = json.Object();
			json.Number(descriptor, "id", file.file_id.index);
			json.Text(descriptor, "uri", file.file.path);
			auto version = "sha256:" + Hex(duckdb_mbedtls::MbedTlsWrapper::ComputeSha256Hash(bytes));
			json.Text(descriptor, "version", version);
			identities.push_back({file.file_id.index, file.file.path, version, file.row_count});
			json.Number(descriptor, "row_count", file.row_count);
			yyjson_mut_arr_append(inventory, descriptor);
			auto source = json.Object();
			json.Number(source, "file_id", file.file_id.index);
			json.Text(source, "hex", Hex(bytes));
			yyjson_mut_arr_append(sources, source);
			if (!by_path.emplace(file.file.path, file.file_id.index).second) {
				throw InvalidInputException("Duplicate native read view file path");
			}
			checkpoint("after_pin");
		}
		if (index) {
			index->ValidateCoverage(identities);
		}
		auto visible = json.Array();
		auto inlined = json.Array();
		json.Add(captured, "visible", visible);
		json.Add(mixed, "inline_rows", inlined);
		auto name = KeywordHelper::WriteOptionallyQuoted(options.catalog) + "." +
		            KeywordHelper::WriteOptionallyQuoted(options.schema) + "." +
		            KeywordHelper::WriteOptionallyQuoted(options.table);
		checkpoint("before_rows");
		auto rows = reader.SendQuery("SELECT id, embedding, category, payload, filename, file_row_number, rowid FROM " +
		                             name + " AT (VERSION => " + to_string(snapshot.snapshot_id) + ") LIMIT " +
		                             to_string(row_limit + 1));
		if (rows->HasError()) {
			rows->ThrowError();
		}
		ViewJSON oracle;
		auto expected = oracle.Array();
		idx_t business_bytes = 0;
		idx_t inline_bytes = 0;
		while (auto chunk = rows->Fetch()) {
			for (idx_t row_idx = 0; row_idx < chunk->size(); row_idx++) {
				checkpoint("row");
				if (++result.visible_rows > row_limit) {
					throw InvalidInputException("Read view visible row budget exceeded");
				}
				vector<Value> values;
				for (idx_t col = 0; col < 4; col++) {
					values.push_back(chunk->GetValue(col, row_idx));
				}
				idx_t row_bytes = dimension * sizeof(float) + 64;
				for (idx_t col = 2; col < 4; col++) {
					if (!values[col].IsNull()) {
						row_bytes += StringValue::Get(values[col]).size();
					}
				}
				if (row_bytes > 64 * 1024 * 1024 - business_bytes) {
					throw InvalidInputException("Read view business byte budget exceeded");
				}
				business_bytes += row_bytes;
				auto filename = chunk->GetValue(4, row_idx).GetValue<string>();
				auto address = json.Object();
				auto oracle_address = oracle.Object();
				auto file = by_path.find(filename);
				if (file != by_path.end()) {
					auto offset = chunk->GetValue(5, row_idx).GetValue<idx_t>();
					json.Number(address, "file_id", file->second);
					json.Number(address, "row_offset", offset);
					yyjson_mut_arr_append(visible, address);
					oracle.Text(oracle_address, "kind", "file");
					oracle.Number(oracle_address, "file_id", file->second);
					oracle.Number(oracle_address, "row_offset", offset);
				} else if (StringUtil::StartsWith(filename, "ducklake_inlined_data_")) {
					if (row_bytes > 4 * 1024 * 1024 - inline_bytes) {
						throw InvalidInputException("Read view inline byte budget exceeded");
					}
					inline_bytes += row_bytes;
					json.Text(address, "kind", "inline");
					json.Text(address, "source", filename);
					json.Number(address, "row_id", chunk->GetValue(6, row_idx).GetValue<idx_t>());
					auto value = json.Object();
					json.Add(value, "row_ref", address);
					json.Add(value, "row", Business(json, values, dimension));
					yyjson_mut_arr_append(inlined, value);
					result.inline_rows++;
					oracle.Text(oracle_address, "kind", "inline");
					oracle.Text(oracle_address, "source", filename);
					oracle.Number(oracle_address, "row_id", chunk->GetValue(6, row_idx).GetValue<idx_t>());
				} else {
					throw InvalidInputException("Visible read view row is outside the native file inventory");
				}
				auto value = oracle.Object();
				oracle.Add(value, "row_ref", oracle_address);
				oracle.Add(value, "row", Business(oracle, values, dimension));
				yyjson_mut_arr_append(expected, value);
			}
		}
		checkpoint("before_publish");
		result.admission_json = json.Finish(admission);
		if (result.admission_json.size() > 160 * 1024 * 1024) {
			throw InvalidInputException("Read view admission byte budget exceeded");
		}
		result.rows_json = oracle.Finish(expected);
		reader.Commit();
		return result;
	} catch (...) {
		if (reader.context->transaction.HasActiveTransaction()) {
			reader.Rollback();
		}
		throw;
	}
}

} // namespace duckdb
