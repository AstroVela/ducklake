#include "storage/ducklake_vortex_index_catalog.hpp"

#include "common/ducklake_vortex_json.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/storage_lock.hpp"
#include "duckdb/transaction/transaction_context.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_vortex_read_view.hpp"
#include "storage/ducklake_vortex_control.hpp"
#include "storage/ducklake_vortex_query_ffi.hpp"
#include "mbedtls_wrapper.hpp"

#include <algorithm>
#include <cstdlib>
#include <set>

namespace duckdb {
using namespace duckdb_yyjson;
using namespace vortex_detail;

namespace {
const char *CATALOG_TABLE = "ducklake_vortex_indexes_v1";
const idx_t MAX_MANIFEST = 1024 * 1024;
const idx_t MAX_ARTIFACTS = 64 * 1024 * 1024;

string Hash(const string &bytes) {
	return "sha256:" + Hex(duckdb_mbedtls::MbedTlsWrapper::ComputeSha256Hash(bytes));
}

struct Parsed {
	explicit Parsed(const string &bytes) : doc(yyjson_read(bytes.data(), bytes.size(), 0), yyjson_doc_free) {
		if (!doc) {
			throw InvalidInputException("Invalid Vortex index JSON");
		}
	}
	yyjson_val *Root() const {
		return yyjson_doc_get_root(doc.get());
	}
	std::unique_ptr<yyjson_doc, decltype(&yyjson_doc_free)> doc;
};

yyjson_val *Member(yyjson_val *object, const char *name) {
	auto value = yyjson_obj_get(object, name);
	if (!value) {
		throw InvalidInputException("Missing Vortex index member: %s", name);
	}
	return value;
}

void Object(yyjson_val *value, idx_t count) {
	if (!yyjson_is_obj(value) || yyjson_obj_size(value) != count) {
		throw InvalidInputException("Vortex index object shape mismatch");
	}
}

string Text(yyjson_val *value) {
	if (!yyjson_is_str(value)) {
		throw InvalidInputException("Vortex index requires a string");
	}
	return string(yyjson_get_str(value), yyjson_get_len(value));
}
string Text(yyjson_val *object, const char *name) {
	return Text(Member(object, name));
}
idx_t Number(yyjson_val *value) {
	if (!yyjson_is_uint(value)) {
		throw InvalidInputException("Vortex index requires an unsigned integer");
	}
	return yyjson_get_uint(value);
}
idx_t Number(yyjson_val *object, const char *name) {
	return Number(Member(object, name));
}
void Array(yyjson_val *value) {
	if (!yyjson_is_arr(value)) {
		throw InvalidInputException("Vortex index requires an array");
	}
}

void Name(const string &name) {
	if (name.empty() || name.size() > 128 || name.find('\0') != string::npos) {
		throw InvalidInputException("Invalid Vortex index name");
	}
}

void Relative(const string &path, bool component) {
	if (path.empty() || path[0] == '/' || path.find('\\') != string::npos || path.find(':') != string::npos ||
	    path.find('\0') != string::npos || (component && path.find('/') != string::npos)) {
		throw InvalidInputException("Invalid local index artifact path");
	}
	for (auto &part : StringUtil::Split(path, '/')) {
		if (part.empty() || part == "." || part == "..") {
			throw InvalidInputException("Invalid local index path component");
		}
	}
}

string Canonical(const string &path) {
#ifdef _WIN32
	throw NotImplementedException("Native Vortex index publication is qualified on POSIX paths");
#else
	std::unique_ptr<char, decltype(&free)> resolved(realpath(path.c_str(), nullptr), free);
	if (!resolved || path != resolved.get()) {
		throw InvalidInputException("Vortex index paths must be canonical and contain no symlinks: %s", path);
	}
	return path;
#endif
}

string Read(FileSystem &fs, const string &path, idx_t limit, bool canonical) {
	if (!fs.IsPathAbsolute(path)) {
		throw InvalidInputException("Vortex index paths must be absolute");
	}
	if (canonical) {
		Canonical(path);
	}
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
	auto size = handle->GetFileSize();
	if (handle->GetType() != FileType::FILE_TYPE_REGULAR || size < 0 || idx_t(size) > limit) {
		throw InvalidInputException("Vortex index file exceeds its byte budget or is not regular");
	}
	string bytes(idx_t(size), '\0');
	if (size) {
		handle->Read(&bytes[0], size, 0);
	}
	return bytes;
}

string Verify(FileSystem &fs, const string &path, yyjson_val *artifact, idx_t limit) {
	Object(artifact, 3);
	auto bytes = Read(fs, path, limit, true);
	if (bytes.size() != Number(artifact, "size") || Hash(bytes) != Text(artifact, "checksum")) {
		throw InvalidInputException("Sealed Vortex artifact size/checksum mismatch");
	}
	return bytes;
}

DuckLakeVortexIndex ParseReference(const string &bytes, const string &uuid, idx_t field_id) {
	Parsed parsed(bytes);
	auto value = parsed.Root();
	Object(value, 7);
	if (Number(value, "format_version") != 1 || Text(value, "table_uuid") != uuid ||
	    Number(value, "field_id") != field_id || Text(value, "metric") != "squared_l2") {
		throw InvalidInputException("Vortex index table, field or metric mismatch");
	}
	auto dim = Number(value, "dimension");
	if (dim == 0 || dim > 4096 || field_id > NumericLimits<uint32_t>::Maximum()) {
		throw InvalidInputException("Invalid Vortex index dimension or field ID");
	}
	DuckLakeVortexIndex result;
	result.field_id = uint32_t(field_id);
	result.dimension = uint32_t(dim);
	result.reference = bytes;
	result.revision = 0;
	auto snapshot = Member(value, "snapshot");
	Object(snapshot, 4);
	auto files = Member(snapshot, "files");
	Array(files);
	ViewJSON canonical;
	auto inventory = canonical.Array();
	idx_t previous = 0;
	for (idx_t i = 0; i < yyjson_arr_size(files); i++) {
		auto file = yyjson_arr_get(files, i);
		Object(file, 4);
		DuckLakeVortexIndexFile entry {Number(file, "id"), Text(file, "uri"), Text(file, "version"),
		                               Number(file, "row_count")};
		if ((i && entry.id <= previous) || entry.uri.empty()) {
			throw InvalidInputException("Invalid Vortex index file ordering");
		}
		previous = entry.id;
		auto item = canonical.Object();
		canonical.Number(item, "id", entry.id);
		canonical.Text(item, "uri", entry.uri);
		canonical.Text(item, "version", entry.version);
		canonical.Number(item, "row_count", entry.row_count);
		yyjson_mut_arr_append(inventory, item);
		result.files.push_back(std::move(entry));
	}
	if (result.files.empty() || Text(snapshot, "dataset_id") != "ducklake-physical-files:" + uuid ||
	    Text(snapshot, "schema_fingerprint") !=
	        "ducklake-file-vector-v1:" + to_string(field_id) + ":float32:" + to_string(dim) + ":squared_l2" ||
	    Text(snapshot, "version") != Hash(canonical.Finish(inventory))) {
		throw InvalidInputException("Vortex index physical snapshot mismatch");
	}
	auto generation = Member(value, "generation");
	Object(generation, 2);
	result.generation = Text(generation, "generation");
	Relative(result.generation, true);
	auto manifest = Member(generation, "manifest");
	Object(manifest, 3);
	if (Text(manifest, "path") != "manifest.json") {
		throw InvalidInputException("Invalid Vortex manifest path");
	}
	return result;
}

string JSON(yyjson_val *value) {
	ViewJSON copy;
	return copy.Finish(yyjson_val_mut_copy(copy.doc.get(), value));
}

void Compose(DuckLakeVortexIndex &index, vector<DuckLakeVortexIndexSegment> segments) {
	if (segments.empty() || segments.size() > 2) {
		throw InvalidInputException("RESOURCE_EXHAUSTED: Vortex segment count budget exceeded");
	}
	std::set<idx_t> files;
	std::set<string> generations;
	std::set<string> paths;
	idx_t remaining_rows = MinValue<idx_t>(100000, MAX_ARTIFACTS / sizeof(float) / index.dimension);
	index.files.clear();
	ViewJSON json;
	auto value = json.Object();
	json.Number(value, "format_version", 2);
	auto entries = json.Array();
	json.Add(value, "segments", entries);
	for (auto &segment : segments) {
		if (!generations.insert(segment.generation).second) {
			throw InvalidInputException("Duplicate Vortex segment generation");
		}
		for (auto &file : segment.files) {
			if (!files.insert(file.id).second) {
				throw InvalidInputException("Vortex segment file coverage overlaps");
			}
			if (!paths.insert(file.uri).second) {
				throw InvalidInputException("Vortex segment source paths overlap");
			}
			if (file.row_count > remaining_rows) {
				throw InvalidInputException("RESOURCE_EXHAUSTED: Vortex segment row/vector budget exceeded");
			}
			remaining_rows -= file.row_count;
			index.files.push_back(file);
		}
		auto entry = json.Object();
		json.Text(entry, "root", segment.root);
		Parsed parsed(segment.reference);
		json.Add(entry, "reference", yyjson_val_mut_copy(json.doc.get(), parsed.Root()));
		yyjson_mut_arr_append(entries, entry);
	}
	std::sort(index.files.begin(), index.files.end(),
	          [](const DuckLakeVortexIndexFile &a, const DuckLakeVortexIndexFile &b) { return a.id < b.id; });
	index.root = segments.back().root;
	index.generation = segments.back().generation;
	index.reference = json.Finish(value);
	if (index.reference.size() > MAX_MANIFEST) {
		throw InvalidInputException("RESOURCE_EXHAUSTED: Vortex segment manifest byte budget exceeded");
	}
	index.segments = std::move(segments);
}

DuckLakeVortexIndex ParseCatalogReference(const string &bytes, const string &uuid, idx_t field_id) {
	Parsed parsed(bytes);
	auto value = parsed.Root();
	Object(value, 2);
	if (Number(value, "format_version") != 2) {
		throw InvalidInputException("Unsupported Vortex segment manifest version");
	}
	auto entries = Member(value, "segments");
	Array(entries);
	if (!yyjson_arr_size(entries) || yyjson_arr_size(entries) > 2) {
		throw InvalidInputException("Invalid Vortex segment count");
	}
	DuckLakeVortexIndex result;
	vector<DuckLakeVortexIndexSegment> segments;
	for (idx_t i = 0; i < yyjson_arr_size(entries); i++) {
		auto entry = yyjson_arr_get(entries, i);
		Object(entry, 2);
		auto segment = ParseReference(JSON(Member(entry, "reference")), uuid, field_id);
		if (i && (segment.dimension != result.dimension || segment.field_id != result.field_id)) {
			throw InvalidInputException("Vortex segment vector specification mismatch");
		}
		result.field_id = segment.field_id;
		result.dimension = segment.dimension;
		auto root = Text(entry, "root");
		if (root.empty() || root[0] != '/' || root.find('\0') != string::npos) {
			throw InvalidInputException("Invalid Vortex segment root");
		}
		segments.push_back({root, segment.generation, segment.reference, segment.files});
	}
	Compose(result, std::move(segments));
	return result;
}

void VerifyGeneration(FileSystem &fs, const DuckLakeVortexIndex &index, idx_t &remaining) {
	Parsed reference(index.reference);
	auto descriptor = Member(reference.Root(), "generation");
	auto directory = fs.JoinPath(index.root, index.generation);
	Canonical(directory);
	auto bytes = Verify(fs, fs.JoinPath(directory, "manifest.json"), Member(descriptor, "manifest"), MAX_MANIFEST);
	Parsed parsed(bytes);
	auto metadata = parsed.Root();
	Object(metadata, 9);
	if (Number(metadata, "format_version") != 1 || Number(metadata, "backend_version") != 1 ||
	    Text(metadata, "backend") != "spfresh.static" || Text(metadata, "generation") != index.generation ||
	    Text(metadata, "name").empty()) {
		throw InvalidInputException("Vortex sealed backend/generation mismatch");
	}
	// Parse the sealed snapshot through the same exact physical binding contract.
	ViewJSON copy;
	auto synthetic = yyjson_val_mut_copy(copy.doc.get(), reference.Root());
	yyjson_mut_obj_remove_key(synthetic, "snapshot");
	copy.Add(synthetic, "snapshot", yyjson_val_mut_copy(copy.doc.get(), Member(metadata, "snapshot")));
	auto sealed = ParseReference(copy.Finish(synthetic), Text(reference.Root(), "table_uuid"), index.field_id);
	if (sealed.files.size() != index.files.size()) {
		throw InvalidInputException("Sealed Vortex coverage mismatch");
	}
	index.ValidateCoverage(sealed.files);
	auto fields = Member(metadata, "fields");
	auto covered = Member(metadata, "covered_files");
	Array(fields);
	Array(covered);
	if (yyjson_arr_size(fields) != 1 ||
	    Text(yyjson_arr_get(fields, 0)) != "__ducklake_vortex_v1_field_" + to_string(index.field_id) ||
	    yyjson_arr_size(covered) != index.files.size()) {
		throw InvalidInputException("Sealed Vortex field/coverage mismatch");
	}
	for (idx_t i = 0; i < index.files.size(); i++) {
		if (Number(yyjson_arr_get(covered, i)) != index.files[i].id) {
			throw InvalidInputException("Sealed Vortex file coverage mismatch");
		}
	}
	auto artifacts = Member(metadata, "artifacts");
	Array(artifacts);
	if (yyjson_arr_size(artifacts) == 0) {
		throw InvalidInputException("Sealed Vortex artifact inventory is empty");
	}
	std::set<string> paths;
	for (idx_t i = 0; i < yyjson_arr_size(artifacts); i++) {
		auto artifact = yyjson_arr_get(artifacts, i);
		auto path = Text(artifact, "path");
		Relative(path, false);
		if (!paths.insert(path).second) {
			throw InvalidInputException("Duplicate sealed Vortex artifact");
		}
		auto data = Verify(fs, fs.JoinPath(directory, "artifacts", path), artifact, remaining);
		remaining -= data.size();
	}
}

} // namespace

DuckLakeTransaction &DuckLakeVortexIndex::Begin(Connection &owner, const string &name) {
	owner.BeginTransaction();
	auto &catalog = Catalog::GetCatalog(*owner.context, name).Cast<DuckLakeCatalog>();
	if (!catalog.MetadataType().empty() && catalog.MetadataType() != "duckdb") {
		throw InvalidInputException("Vortex index catalog requires native DuckDB metadata");
	}
	auto &transaction = DuckLakeTransaction::Get(*owner.context, catalog);
	transaction.GetSnapshot();
	return transaction;
}

void DuckLakeVortexIndex::Rollback(Connection &owner) {
	if (owner.context->transaction.HasActiveTransaction()) {
		owner.Rollback();
	}
}

void DuckLakeVortexIndex::Initialized(DuckLakeTransaction &transaction) {
	auto &catalog = transaction.GetCatalog();
	if (!transaction.GetConnection().TableInfo(catalog.MetadataDatabaseName(), catalog.MetadataSchemaName(),
	                                           CATALOG_TABLE)) {
		throw InvalidInputException("Vortex index catalog is not initialized");
	}
}

unique_ptr<QueryResult> DuckLakeVortexIndex::Query(DuckLakeTransaction &transaction, const string &sql) {
	auto result = transaction.Query(sql);
	if (result->HasError()) {
		result->ThrowError();
	}
	return result;
}

StorageLock &DuckLakeVortexIndex::PublicationLock() {
	static StorageLock lock;
	return lock;
}

void DuckLakeVortexIndex::Initialize(Connection &owner, const string &catalog) {
	if (owner.context->transaction.HasActiveTransaction()) {
		throw InvalidInputException("Vortex index catalog requires an idle dedicated connection");
	}
	auto guard = PublicationLock().GetExclusiveLock();
	try {
		auto &transaction = Begin(owner, catalog);
		Query(transaction,
		      "CREATE TABLE {METADATA_CATALOG}.ducklake_vortex_indexes_v1("
		      "table_uuid VARCHAR NOT NULL,index_name VARCHAR NOT NULL,revision BIGINT NOT NULL CHECK(revision>0),"
		      "index_root VARCHAR NOT NULL,reference VARCHAR NOT NULL,PRIMARY KEY(table_uuid,index_name))");
		owner.Commit();
	} catch (...) {
		Rollback(owner);
		throw;
	}
}

DuckLakeVortexIndex DuckLakeVortexIndex::ParseSegments(const string &reference, const string &uuid, idx_t field_id) {
	return ParseCatalogReference(reference, uuid, field_id);
}

DuckLakeVortexIndex DuckLakeVortexIndex::Lookup(DuckLakeTransaction &transaction, DuckLakeTableEntry &table,
                                                const string &name) {
	Name(name);
	Initialized(transaction);
	auto result = Query(transaction,
	                    "SELECT revision,index_root,reference FROM {METADATA_CATALOG}.ducklake_vortex_indexes_v1 "
	                    "WHERE table_uuid=" +
	                        Value(table.GetTableUUID()).ToSQLString() + " AND index_name=" + Value(name).ToSQLString());
	auto chunk = result->Fetch();
	if (!chunk || chunk->size() != 1) {
		throw InvalidInputException("No published Vortex index with the requested name");
	}
	auto &field = table.GetFieldId(vector<string> {"embedding"});
	if (field.Type() != LogicalType::LIST(LogicalType::FLOAT)) {
		throw InvalidInputException("Published vector field type mismatch");
	}
	auto index =
	    ParseSegments(StringValue::Get(chunk->GetValue(2, 0)), table.GetTableUUID(), field.GetFieldIndex().index);
	index.name = name;
	index.revision = chunk->GetValue(0, 0).GetValue<idx_t>();
	if (index.root != StringValue::Get(chunk->GetValue(1, 0))) {
		throw InvalidInputException("Vortex catalog root mismatch");
	}
	if (!index.revision || index.root.empty()) {
		throw InvalidInputException("Invalid published Vortex index record");
	}
	return index;
}

void DuckLakeVortexIndex::ValidateCoverage(const vector<DuckLakeVortexIndexFile> &current) const {
	for (auto &indexed : files) {
		auto found = std::find_if(current.begin(), current.end(),
		                          [&](const DuckLakeVortexIndexFile &file) { return file.id == indexed.id; });
		if (found == current.end() || found->uri != indexed.uri || found->version != indexed.version ||
		    found->row_count != indexed.row_count) {
			throw InvalidInputException("Referenced Vortex index file is absent or its identity changed");
		}
	}
}

string DuckLakeVortexIndex::ToJSON() const {
	ViewJSON json;
	Parsed parsed(reference);
	auto value = json.Object();
	json.Text(value, "name", name);
	json.Number(value, "revision", revision);
	json.Text(value, "root", root);
	json.Add(value, "reference", yyjson_val_mut_copy(json.doc.get(), parsed.Root()));
	return json.Finish(value);
}

DuckLakeVortexIndex DuckLakeVortexIndex::BuildAndPublish(ClientContext &context, const string &catalog_name,
                                                         const string &schema, const string &table_name,
                                                         const string &name, const string &reference_path,
                                                         uint32_t dimension, idx_t expected_revision,
                                                         const std::function<void(const string &)> &observe,
                                                         const string &expected_table_uuid,
                                                         optional_idx expected_field_id, bool append_segment) {
	Name(name);
	if (!dimension || dimension > 4096 || reference_path.empty() || reference_path.size() > 65536 ||
	    reference_path.find('\0') != string::npos || expected_revision > idx_t(NumericLimits<int64_t>::Maximum())) {
		throw InvalidInputException("Invalid Vortex build dimension, reference or revision");
	}
	auto check = [&](const string &stage) {
		if (observe) {
			observe(stage);
		}
		if (context.IsInterrupted()) {
			throw InterruptException();
		}
	};
	check("before_build");
	Connection owner(*context.db);
	try {
		auto &transaction = Begin(owner, catalog_name);
		Initialized(transaction);
		if (append_segment) {
			auto &catalog = transaction.GetCatalog();
			auto entry = catalog.GetEntry(*owner.context, CatalogType::TABLE_ENTRY, schema, table_name,
			                              OnEntryNotFound::THROW_EXCEPTION);
			auto &table = entry->Cast<DuckLakeTableEntry>();
			if ((!expected_table_uuid.empty() && table.GetTableUUID() != expected_table_uuid) ||
			    (expected_field_id.IsValid() && table.GetFieldId(vector<string> {"embedding"}).GetFieldIndex().index !=
			                                        expected_field_id.GetIndex())) {
				throw InvalidInputException("Vortex optimizer table or vector field identity changed");
			}
			auto previous = Lookup(transaction, entry->Cast<DuckLakeTableEntry>(), name);
			if (previous.dimension != dimension) {
				throw InvalidInputException("Vortex segment dimension mismatch");
			}
			if (previous.revision != expected_revision) {
				throw TransactionException("Vortex index publication revision conflict");
			}
		}
		owner.Commit();
	} catch (...) {
		Rollback(owner);
		throw;
	}
	DuckLakeVortexViewOptions options {catalog_name,
	                                   schema,
	                                   table_name,
	                                   append_segment ? 0 : dimension,
	                                   MAX_ARTIFACTS,
	                                   100000,
	                                   append_segment ? name : ""};
	options.unindexed_only = append_segment;
	options.expected_table_uuid = expected_table_uuid;
	options.expected_field_id = expected_field_id;
	auto view = DuckLakeVortexReadView::AcquireBuild(owner, options, observe, &context);
	vector<DuckLakeVortexQuerySource> sources;
	for (auto &source : view.sources) {
		sources.push_back(
		    {source.file_id, reinterpret_cast<const uint8_t *>(source.bytes.data()), source.bytes.size()});
	}
	DuckLakeVortexControl control {context, observe, nullptr, view.sources};
	DuckLakeVortexBuildInput input {reinterpret_cast<const uint8_t *>(view.admission_json.data()),
	                                view.admission_json.size(),
	                                sources.data(),
	                                sources.size(),
	                                reinterpret_cast<const uint8_t *>(view.managed_data_path.data()),
	                                view.managed_data_path.size(),
	                                reinterpret_cast<const uint8_t *>(reference_path.data()),
	                                reference_path.size(),
	                                DuckLakeVortexControl::Check,
	                                &control};
	std::unique_ptr<char, decltype(&ducklake_vortex_build_error_free)> error(ducklake_vortex_build(&input),
	                                                                         ducklake_vortex_build_error_free);
	if (control.error) {
		std::rethrow_exception(control.error);
	}
	if (context.IsInterrupted()) {
		throw InterruptException();
	}
	if (error) {
		throw InvalidInputException("%s", error.get());
	}
	check("before_build_publish");
	return Publish(owner, catalog_name, schema, table_name, name, reference_path, expected_revision, check,
	               append_segment);
}

DuckLakeVortexIndex DuckLakeVortexIndex::Publish(Connection &owner, const string &catalog_name, const string &schema,
                                                 const string &table_name, const string &name,
                                                 const string &reference_path, idx_t expected_revision,
                                                 const std::function<void(const string &)> &observe,
                                                 bool append_segment) {
	if (owner.context->transaction.HasActiveTransaction()) {
		throw InvalidInputException("Vortex index publication requires an idle dedicated connection");
	}
	auto publication = PublicationLock().GetExclusiveLock();
	bool attempted = false;
	bool committed = false;
	try {
		Name(name);
		auto &transaction = Begin(owner, catalog_name);
		Initialized(transaction);
		auto &catalog = transaction.GetCatalog();
		auto entry = catalog.GetEntry(*owner.context, CatalogType::TABLE_ENTRY, schema, table_name,
		                              OnEntryNotFound::THROW_EXCEPTION);
		auto &table = entry->Cast<DuckLakeTableEntry>();
		auto &field = table.GetFieldId(vector<string> {"embedding"});
		if (field.Type() != LogicalType::LIST(LogicalType::FLOAT)) {
			throw InvalidInputException("Vortex index field requires FLOAT[]");
		}
		auto &fs = FileSystem::GetFileSystem(*owner.context);
		Canonical(reference_path);
		auto candidate = ParseReference(Read(fs, reference_path, MAX_MANIFEST, true), table.GetTableUUID(),
		                                field.GetFieldIndex().index);
		candidate.root = Canonical(
		    reference_path.substr(0, reference_path.find_last_of('/') == 0 ? 1 : reference_path.find_last_of('/')));
		auto data_path = fs.ExpandPath(catalog.DataPath());
		while (data_path.size() > 1 && data_path.back() == '/') {
			data_path.pop_back();
		}
		data_path = Canonical(data_path);
		auto managed_prefix = data_path == "/" ? data_path : data_path + "/";
		if (candidate.root == data_path || StringUtil::StartsWith(candidate.root, managed_prefix)) {
			throw InvalidInputException("Vortex index artifacts must be outside the managed data path");
		}
		auto where = " WHERE table_uuid=" + Value(table.GetTableUUID()).ToSQLString() +
		             " AND index_name=" + Value(name).ToSQLString();
		auto existing =
		    Query(transaction, "SELECT revision FROM {METADATA_CATALOG}.ducklake_vortex_indexes_v1" + where);
		auto previous = existing->Fetch();
		idx_t revision = previous && previous->size() ? previous->GetValue(0, 0).GetValue<idx_t>() : 0;
		if (revision != expected_revision || revision >= idx_t(NumericLimits<int64_t>::Maximum())) {
			throw TransactionException("Vortex index publication revision conflict");
		}
		vector<DuckLakeVortexIndexSegment> segments;
		if (append_segment) {
			if (!revision) {
				throw InvalidInputException("Segment append requires a published index");
			}
			auto previous_index = Lookup(transaction, table, name);
			if (previous_index.dimension != candidate.dimension || previous_index.field_id != candidate.field_id) {
				throw InvalidInputException("Vortex segment vector specification mismatch");
			}
			segments = std::move(previous_index.segments);
		}
		segments.push_back({candidate.root, candidate.generation, candidate.reference, candidate.files});
		Compose(candidate, std::move(segments));
		idx_t artifact_budget = MAX_ARTIFACTS;
		for (auto &segment : candidate.segments) {
			auto part = ParseReference(segment.reference, table.GetTableUUID(), field.GetFieldIndex().index);
			part.root = segment.root;
			VerifyGeneration(fs, part, artifact_budget);
		}
		auto source_guard = DuckLakeVortexReadView::AdmissionLock().GetSharedLock();
		auto current = transaction.GetMetadataManager().GetExtendedFilesForTable(table, transaction.GetSnapshot());
		vector<DuckLakeVortexIndexFile> files;
		idx_t remaining = MAX_ARTIFACTS;
		for (auto &indexed : candidate.files) {
			auto found = std::find_if(current.begin(), current.end(), [&](const DuckLakeFileListExtendedEntry &file) {
				return file.file_id.index == indexed.id;
			});
			if (found == current.end() || found->file.path != indexed.uri || found->row_count != indexed.row_count ||
			    found->file.data_format != DataFileFormat::VORTEX || !found->file.encryption_key.empty()) {
				throw InvalidInputException("Referenced Vortex index file is absent or its identity changed");
			}
			auto bytes = Read(fs, indexed.uri, remaining, false);
			remaining -= bytes.size();
			files.push_back({indexed.id, indexed.uri, Hash(bytes), indexed.row_count});
		}
		candidate.ValidateCoverage(files);
		candidate.name = name;
		candidate.revision = revision + 1;
		if (observe) {
			observe("after_validation");
		}
		if (!revision) {
			Query(transaction, "INSERT INTO {METADATA_CATALOG}.ducklake_vortex_indexes_v1 VALUES(" +
			                       Value(table.GetTableUUID()).ToSQLString() + "," + Value(name).ToSQLString() + ",1," +
			                       Value(candidate.root).ToSQLString() + "," +
			                       Value(candidate.reference).ToSQLString() + ")");
		} else {
			Query(transaction,
			      "UPDATE {METADATA_CATALOG}.ducklake_vortex_indexes_v1 SET revision=" + to_string(candidate.revision) +
			          ",index_root=" + Value(candidate.root).ToSQLString() + ",reference=" +
			          Value(candidate.reference).ToSQLString() + where + " AND revision=" + to_string(revision));
		}
		if (observe) {
			observe("before_commit");
		}
		attempted = true;
		owner.Commit();
		committed = true;
		if (observe) {
			observe("after_commit");
		}
		return candidate;
	} catch (...) {
		Rollback(owner);
		if (committed) {
			throw IOException("Vortex index publication committed but acknowledgement failed; inspect catalog before "
			                  "retry; generations retained");
		}
		if (attempted) {
			throw IOException("Vortex index publication commit outcome is uncertain; inspect catalog before retry; "
			                  "generations retained");
		}
		throw;
	}
}
} // namespace duckdb
