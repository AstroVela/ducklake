#include "storage/ducklake_vortex_index_catalog.hpp"

#include "common/ducklake_vortex_json.hpp"
#include "common/ducklake_vortex_gc_io.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/storage_lock.hpp"
#include "duckdb/transaction/transaction_context.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_vortex_read_view.hpp"
#include "storage/ducklake_vortex_optimizer.hpp"
#include "storage/ducklake_vortex_control.hpp"
#include "storage/ducklake_vortex_query_ffi.hpp"
#include "storage/ducklake_metadata_manager.hpp"
#include "storage/ducklake_vortex.hpp"
#include "mbedtls_wrapper.hpp"

#include <algorithm>
#include <cstdlib>
#include <set>
#ifndef _WIN32
#include <cerrno>
#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace duckdb {
using namespace duckdb_yyjson;
using namespace vortex_detail;

namespace {
const char *CATALOG_TABLE = "ducklake_vortex_indexes_v1";
const char *GENERATIONS = "ducklake_vortex_generations_v1";
const char *GENERATION_REFS = "ducklake_vortex_generation_refs_v1";
const char *BUILDS = "ducklake_vortex_builds_v1";
const char *REWRITES = "ducklake_vortex_rewrites_v1";
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
	CheckRelative(result.generation, true);
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

void Compose(DuckLakeVortexIndex &index, vector<DuckLakeVortexIndexSegment> segments, const string &uuid,
             const string &publication = "") {
	if (segments.size() > DuckLakeVortexIndex::MAX_SEGMENTS) {
		throw InvalidInputException("RESOURCE_EXHAUSTED: Vortex segment count budget exceeded");
	}
	std::set<idx_t> files;
	std::set<string> generations;
	std::set<string> paths;
	idx_t remaining_rows = MinValue<idx_t>(100000, MAX_ARTIFACTS / sizeof(float) / index.dimension);
	index.files.clear();
	ViewJSON json;
	auto value = json.Object();
	if (!segments.empty()) {
		index.root = segments.back().root;
	}
	index.generation = segments.empty() ? "" : segments.back().generation;
	json.Number(value, "format_version", 3);
	json.Text(value, "table_uuid", uuid);
	json.Number(value, "field_id", index.field_id);
	json.Number(value, "dimension", index.dimension);
	json.Text(value, "root", index.root);
	json.Text(value, "publication", publication.empty() ? UUID::ToString(UUID::GenerateRandomUUID()) : publication);
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
	index.reference = json.Finish(value);
	if (index.reference.size() > MAX_MANIFEST) {
		throw InvalidInputException("RESOURCE_EXHAUSTED: Vortex segment manifest byte budget exceeded");
	}
	index.segments = std::move(segments);
}

DuckLakeVortexIndex ParseCatalogReference(const string &bytes, const string &uuid, idx_t field_id) {
	Parsed parsed(bytes);
	auto value = parsed.Root();
	Object(value, 7);
	if (Number(value, "format_version") != 3 || Text(value, "table_uuid") != uuid ||
	    Number(value, "field_id") != field_id || !Number(value, "dimension") || Number(value, "dimension") > 4096) {
		throw InvalidInputException("Unsupported Vortex segment manifest version or vector definition");
	}
	auto publication = Text(value, "publication");
	if (UUID::ToString(UUID::FromString(publication)) != publication) {
		throw InvalidInputException("Invalid Vortex plan publication identity");
	}
	auto entries = Member(value, "segments");
	Array(entries);
	if (yyjson_arr_size(entries) > DuckLakeVortexIndex::MAX_SEGMENTS) {
		throw InvalidInputException("Invalid Vortex segment count");
	}
	DuckLakeVortexIndex result;
	result.field_id = uint32_t(field_id);
	result.dimension = uint32_t(Number(value, "dimension"));
	result.root = Text(value, "root");
	if (result.root.empty() || result.root[0] != '/' || result.root.find('\0') != string::npos) {
		throw InvalidInputException("Invalid Vortex plan root");
	}
	vector<DuckLakeVortexIndexSegment> segments;
	for (idx_t i = 0; i < yyjson_arr_size(entries); i++) {
		auto entry = yyjson_arr_get(entries, i);
		Object(entry, 2);
		auto segment = ParseReference(JSON(Member(entry, "reference")), uuid, field_id);
		if (segment.dimension != result.dimension || segment.field_id != result.field_id) {
			throw InvalidInputException("Vortex segment vector specification mismatch");
		}
		auto root = Text(entry, "root");
		if (root.empty() || root[0] != '/' || root.find('\0') != string::npos) {
			throw InvalidInputException("Invalid Vortex segment root");
		}
		segments.push_back({root, segment.generation, segment.reference, segment.files});
	}
	if (!segments.empty() && result.root != segments.back().root) {
		throw InvalidInputException("Vortex plan root mismatch");
	}
	Compose(result, std::move(segments), uuid, publication);
	return result;
}

string VerifyGeneration(FileSystem &fs, const DuckLakeVortexIndex &index, idx_t &remaining) {
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
		CheckRelative(path, false);
		if (!paths.insert(path).second) {
			throw InvalidInputException("Duplicate sealed Vortex artifact");
		}
		auto data = Verify(fs, fs.JoinPath(directory, "artifacts", path), artifact, remaining);
		remaining -= data.size();
	}
	return bytes;
}

struct NativeBuildInput {
	vector<DuckLakeVortexQuerySource> sources;
	DuckLakeVortexControl control;
	DuckLakeVortexBuildInput input;

	NativeBuildInput(ClientContext &context, DuckLakeVortexReadView &view, const string &reference_path,
	                 const string &generation, const string &workspace,
	                 const std::function<void(const string &)> &check)
	    : control {context, check, nullptr, view.sources} {
		for (auto &source : view.sources) {
			sources.push_back(
			    {source.file_id, reinterpret_cast<const uint8_t *>(source.bytes.data()), source.bytes.size()});
		}
		input = {reinterpret_cast<const uint8_t *>(view.admission_json.data()),
		         view.admission_json.size(),
		         sources.data(),
		         sources.size(),
		         reinterpret_cast<const uint8_t *>(view.managed_data_path.data()),
		         view.managed_data_path.size(),
		         reinterpret_cast<const uint8_t *>(reference_path.data()),
		         reference_path.size(),
		         reinterpret_cast<const uint8_t *>(generation.data()),
		         generation.size(),
		         reinterpret_cast<const uint8_t *>(workspace.data()),
		         workspace.size(),
		         DuckLakeVortexControl::Check,
		         &control};
	}

	void Check(const char *error = nullptr) {
		if (control.error) {
			std::rethrow_exception(control.error);
		}
		if (control.context.IsInterrupted()) {
			throw InterruptException();
		}
		if (error) {
			throw InvalidInputException("%s", error);
		}
	}
};

void BuildNative(ClientContext &context, DuckLakeVortexReadView &view, const string &reference_path,
                 const string &generation, const string &workspace, const std::function<void(const string &)> &check) {
	NativeBuildInput build(context, view, reference_path, generation, workspace, check);
	std::unique_ptr<char, decltype(&ducklake_vortex_build_error_free)> error(ducklake_vortex_build(&build.input),
	                                                                         ducklake_vortex_build_error_free);
	build.Check(error.get());
}

string RewriteInputs(ClientContext &context, DuckLakeTransaction &transaction, DuckLakeTableEntry &table,
                     const DuckLakeVortexRewriteState &plan) {
	string ids, identity;
	set<idx_t> selected;
	for (auto &file : plan.selected_files) {
		if (!ids.empty()) {
			ids += ",";
		}
		ids += to_string(file.id);
		selected.insert(file.id);
	}
	auto snapshot = transaction.GetSnapshot();
	auto append = [&](const string &query) {
		auto result = transaction.Query(snapshot, query);
		if (result->HasError()) {
			result->ThrowError();
		}
		idx_t count = 0;
		identity += "#";
		while (auto chunk = result->Fetch()) {
			for (idx_t row = 0; row < chunk->size(); row++) {
				if (++count > 100000) {
					throw InvalidInputException("Vortex rewrite input state exceeds row budget");
				}
				for (idx_t col = 0; col < chunk->ColumnCount(); col++) {
					auto value = chunk->GetValue(col, row).ToSQLString();
					if (value.size() + identity.size() + 32 > MAX_ARTIFACTS) {
						throw InvalidInputException("Vortex rewrite input state exceeds byte budget");
					}
					identity += to_string(value.size()) + ":" + value;
				}
			}
		}
		return count;
	};
	auto active = " AND begin_snapshot<={SNAPSHOT_ID} AND (end_snapshot IS NULL OR end_snapshot>{SNAPSHOT_ID})";
	append("SELECT * FROM {METADATA_CATALOG}.ducklake_table WHERE table_id=" + to_string(plan.table_id) + active);
	append("SELECT * FROM {METADATA_CATALOG}.ducklake_column WHERE table_id=" + to_string(plan.table_id) + active +
	       " ORDER BY column_id");
	if (append("SELECT * FROM {METADATA_CATALOG}.ducklake_data_file WHERE data_file_id IN (" + ids + ")" + active +
	           " ORDER BY data_file_id") != plan.selected_files.size()) {
		throw TransactionException("Vortex rewrite input files changed before commit");
	}
	append("SELECT * FROM {METADATA_CATALOG}.ducklake_delete_file WHERE data_file_id IN (" + ids + ")" + active +
	       " ORDER BY delete_file_id");
	auto inlined = transaction.GetMetadataManager().GetInlinedDeletionTableName(table.GetTableId(), snapshot);
	identity += "#inline";
	if (!inlined.empty()) {
		append("SELECT file_id,row_id,begin_snapshot FROM {METADATA_CATALOG}." + inlined + " WHERE file_id IN (" + ids +
		       ") AND begin_snapshot<={SNAPSHOT_ID} ORDER BY file_id,row_id,begin_snapshot");
	} else {
		identity += "#";
	}
	idx_t remaining = MAX_ARTIFACTS;
	auto &fs = FileSystem::GetFileSystem(context);
	for (auto &file : plan.selected_files) {
		auto bytes = Read(fs, file.uri, remaining, false);
		remaining -= bytes.size();
		if (Hash(bytes) != file.version) {
			throw TransactionException("Vortex rewrite input content changed before commit");
		}
	}
	map<idx_t, string> masks;
	for (auto &file : transaction.GetMetadataManager().GetExtendedFilesForTable(table, snapshot)) {
		if (selected.count(file.file_id.index) && !file.delete_file.path.empty()) {
			masks[file.delete_file_id.index] = file.delete_file.path;
		}
	}
	for (auto &mask : masks) {
		auto bytes = Read(fs, mask.second, remaining, false);
		remaining -= bytes.size();
		identity += ":delete:" + to_string(mask.first) + ":" + Hash(bytes);
	}
	return Hash(identity);
}

idx_t VisibleRows(Connection &reader, const DuckLakeVortexRewriteState &plan, idx_t limit,
                  optional_ptr<const DuckLakeVortexIndexFile> selected = nullptr) {
	vector<string> paths;
	for (auto &file : plan.selected_files) {
		if (!selected || selected->id == file.id) {
			paths.push_back(Value(file.uri).ToSQLString());
		}
	}
	auto name = KeywordHelper::WriteOptionallyQuoted(plan.catalog) + "." +
	            KeywordHelper::WriteOptionallyQuoted(plan.schema) + "." +
	            KeywordHelper::WriteOptionallyQuoted(plan.table);
	auto rows = reader.Query("SELECT 1 FROM " + name + " WHERE filename IN (" + StringUtil::Join(paths, ",") +
	                         ") LIMIT " + to_string(limit));
	if (rows->HasError()) {
		rows->ThrowError();
	}
	idx_t count = 0;
	while (auto chunk = rows->Fetch()) {
		count += chunk->size();
	}
	return count;
}

void CheckPrune(Connection &reader, DuckLakeVortexRewriteState &plan, bool capture) {
	vector<idx_t> empty;
	idx_t live_rows = 0;
	for (auto &file : plan.selected_files) {
		auto visible = VisibleRows(reader, plan, file.row_count + 1, &file);
		if (!visible) {
			empty.push_back(file.id);
		} else if (visible != file.row_count) {
			throw InvalidInputException("Vortex pruning requires fully visible retained files");
		} else {
			live_rows += visible;
		}
	}
	if (empty.empty() || live_rows < DuckLakeVortexIndex::MIN_BUILD_ROWS) {
		throw InvalidInputException("Vortex pruning requires empty member files and at least 65 retained rows");
	}
	if (capture) {
		plan.replaced = std::move(empty);
		plan.live_rows = live_rows;
	} else if (plan.replaced != empty || plan.live_rows != live_rows) {
		throw TransactionException("Vortex prune visibility changed before commit");
	}
}

void CheckEmpty(Connection &reader, const DuckLakeVortexRewriteState &plan) {
	if (VisibleRows(reader, plan, 1)) {
		throw InvalidInputException("Vortex segment still has visible rows; retirement requires an empty B");
	}
}

string GenerationWhere(const string &root, const string &generation) {
	return " WHERE index_root=" + Value(root).ToSQLString() + " AND generation=" + Value(generation).ToSQLString();
}

bool PublishedGeneration(DuckLakeTransaction &transaction, const string &root, const string &generation) {
	auto result = DuckLakeVortexIndex::Query(transaction, "SELECT count(*) FROM {METADATA_CATALOG}." +
	                                                          string(GENERATIONS) + GenerationWhere(root, generation));
	if (result->Fetch()->GetValue(0, 0).GetValue<idx_t>()) {
		return true;
	}
	result = DuckLakeVortexIndex::Query(transaction, "SELECT count(*) FROM {METADATA_CATALOG}." +
	                                                     string(GENERATION_REFS) + GenerationWhere(root, generation));
	return result->Fetch()->GetValue(0, 0).GetValue<idx_t>() != 0;
}

void ReconcileBuild(Connection &owner, const string &catalog, const string &id) {
	try {
		auto &transaction = DuckLakeVortexIndex::Begin(owner, catalog);
		auto result =
		    DuckLakeVortexIndex::Query(transaction, "SELECT index_root,generation,keep_generation,state FROM "
		                                            "{METADATA_CATALOG}." +
		                                                string(BUILDS) + " WHERE build_id=" + Value(id).ToSQLString());
		auto row = result->Fetch();
		if (!row || row->size() != 1) {
			throw InvalidInputException("Native Vortex build ownership disappeared");
		}
		bool keep_generation = row->GetValue(2, 0).GetValue<bool>() ||
		                       PublishedGeneration(transaction, StringValue::Get(row->GetValue(0, 0)),
		                                           StringValue::Get(row->GetValue(1, 0)));
		bool published = keep_generation || StringValue::Get(row->GetValue(3, 0)) == "published";
		DuckLakeVortexIndex::Query(transaction, "UPDATE {METADATA_CATALOG}." + string(BUILDS) + " SET state=" +
		                                            Value(published ? "published" : "abandoned").ToSQLString() +
		                                            ",keep_generation=" + string(keep_generation ? "true" : "false") +
		                                            " WHERE build_id=" + Value(id).ToSQLString());
		owner.Commit();
	} catch (...) {
		DuckLakeVortexIndex::Rollback(owner);
		throw;
	}
}

void BuildState(Connection &owner, const string &catalog, const string &id, const string &state) {
	try {
		auto &transaction = DuckLakeVortexIndex::Begin(owner, catalog);
		DuckLakeVortexIndex::Query(
		    transaction, "UPDATE {METADATA_CATALOG}." + string(BUILDS) + " SET state=" + Value(state).ToSQLString() +
		                     " WHERE build_id=" + Value(id).ToSQLString() +
		                     " AND keep_generation=false AND state IN ('allocated','building','sealed')");
		owner.Commit();
	} catch (...) {
		DuckLakeVortexIndex::Rollback(owner);
		throw;
	}
}

void RegisterGeneration(DuckLakeTransaction &transaction, const DuckLakeVortexIndex &index, const string &manifest,
                        idx_t artifact_bytes) {
	auto where = GenerationWhere(index.root, index.generation);
	auto build = DuckLakeVortexIndex::Query(transaction, "SELECT state,keep_generation FROM {METADATA_CATALOG}." +
	                                                         string(BUILDS) + where);
	auto build_row = build->Fetch();
	if (build_row && build_row->size() && StringValue::Get(build_row->GetValue(0, 0)) == "deleting" &&
	    !build_row->GetValue(1, 0).GetValue<bool>()) {
		throw InvalidInputException("Native Vortex candidate collection has started");
	}
	auto result = DuckLakeVortexIndex::Query(
	    transaction, "SELECT manifest,artifact_bytes,state FROM {METADATA_CATALOG}." + string(GENERATIONS) + where);
	auto row = result->Fetch();
	if (row && row->size()) {
		if (StringValue::Get(row->GetValue(2, 0)) == "deleting") {
			throw InvalidInputException("Vortex generation collection has already started");
		}
		if (StringValue::Get(row->GetValue(0, 0)) != manifest ||
		    row->GetValue(1, 0).GetValue<idx_t>() != artifact_bytes) {
			throw InvalidInputException("Registered Vortex generation identity changed");
		}
		DuckLakeVortexIndex::Query(transaction,
		                           "UPDATE {METADATA_CATALOG}." + string(GENERATIONS) + " SET state='active'" + where);
	} else {
		DuckLakeVortexIndex::Query(transaction,
		                           "INSERT INTO {METADATA_CATALOG}." + string(GENERATIONS) + " VALUES(" +
		                               Value(index.root).ToSQLString() + "," + Value(index.generation).ToSQLString() +
		                               "," + Value(index.reference).ToSQLString() + "," +
		                               Value(manifest).ToSQLString() + "," + to_string(artifact_bytes) + ",'active')");
	}
	DuckLakeVortexIndex::Query(transaction, "UPDATE {METADATA_CATALOG}." + string(BUILDS) +
	                                            " SET state='published',keep_generation=true" + where);
}

void UpdateGenerationRefs(DuckLakeTransaction &transaction, const string &uuid, const DuckLakeVortexIndex &index) {
	auto owner =
	    " WHERE table_uuid=" + Value(uuid).ToSQLString() + " AND index_name=" + Value(index.name).ToSQLString();
	auto previous = DuckLakeVortexIndex::Query(transaction, "SELECT index_root,generation FROM {METADATA_CATALOG}." +
	                                                            string(GENERATION_REFS) + owner);
	vector<std::pair<string, string>> old;
	while (auto chunk = previous->Fetch()) {
		for (idx_t row = 0; row < chunk->size(); row++) {
			old.emplace_back(StringValue::Get(chunk->GetValue(0, row)), StringValue::Get(chunk->GetValue(1, row)));
		}
	}
	DuckLakeVortexIndex::Query(transaction, "DELETE FROM {METADATA_CATALOG}." + string(GENERATION_REFS) + owner);
	for (auto &segment : index.segments) {
		DuckLakeVortexIndex::Query(
		    transaction, "INSERT INTO {METADATA_CATALOG}." + string(GENERATION_REFS) + " VALUES(" +
		                     Value(uuid).ToSQLString() + "," + Value(index.name).ToSQLString() + "," +
		                     Value(segment.root).ToSQLString() + "," + Value(segment.generation).ToSQLString() + ")");
	}
	for (auto &entry : old) {
		auto where = GenerationWhere(entry.first, entry.second);
		auto result = DuckLakeVortexIndex::Query(transaction, "SELECT count(*) FROM {METADATA_CATALOG}." +
		                                                          string(GENERATION_REFS) + where);
		if (!result->Fetch()->GetValue(0, 0).GetValue<idx_t>()) {
			DuckLakeVortexIndex::Query(transaction, "UPDATE {METADATA_CATALOG}." + string(GENERATIONS) +
			                                            " SET state='retired'" + where);
		}
	}
}

#ifndef _WIN32
struct CollectedFile {
	string path, checksum;
	idx_t size;
};

struct CollectionPlan {
	vector<CollectedFile> files;
	std::set<string> directories;
	std::set<string> paths;
};

CollectionPlan Plan(const string &reference, const string &manifest, idx_t artifact_bytes) {
	if (reference.size() > MAX_MANIFEST || manifest.size() > MAX_MANIFEST || artifact_bytes > MAX_ARTIFACTS) {
		throw InvalidInputException("Registered Vortex generation exceeds its byte budget");
	}
	Parsed parsed_reference(reference);
	auto descriptor = Member(Member(parsed_reference.Root(), "generation"), "manifest");
	if (Number(descriptor, "size") != manifest.size() || Text(descriptor, "checksum") != Hash(manifest)) {
		throw InvalidInputException("Registered Vortex manifest identity changed");
	}
	Parsed parsed(manifest);
	auto artifacts = Member(parsed.Root(), "artifacts");
	Array(artifacts);
	CollectionPlan plan;
	idx_t remaining = artifact_bytes;
	for (idx_t i = 0; i < yyjson_arr_size(artifacts); i++) {
		auto artifact = yyjson_arr_get(artifacts, i);
		Object(artifact, 3);
		auto path = Text(artifact, "path");
		CheckRelative(path, false);
		auto size = Number(artifact, "size");
		if (size > remaining || !plan.paths.insert("artifacts/" + path).second) {
			throw InvalidInputException("Invalid registered Vortex artifact inventory");
		}
		remaining -= size;
		plan.files.push_back({"artifacts/" + path, Text(artifact, "checksum"), size});
	}
	if (remaining || plan.files.empty()) {
		throw InvalidInputException("Registered Vortex artifact byte count mismatch");
	}
	plan.files.push_back({"manifest.json", Hash(manifest), manifest.size()});
	plan.paths.insert("manifest.json");
	for (auto &file : plan.files) {
		for (auto pos = file.path.find('/'); pos != string::npos; pos = file.path.find('/', pos + 1)) {
			plan.directories.insert(file.path.substr(0, pos));
		}
	}
	return plan;
}

void InspectDirectory(int32_t fd, const string &prefix, const CollectionPlan &plan) {
	auto copy = dup(fd);
	if (copy < 0) {
		throw IOException("Cannot inspect registered Vortex generation");
	}
	auto raw = fdopendir(copy);
	if (!raw) {
		close(copy);
		throw IOException("Cannot enumerate registered Vortex generation");
	}
	std::unique_ptr<DIR, decltype(&closedir)> directory(raw, closedir);
	while (true) {
		errno = 0;
		auto entry = readdir(directory.get());
		if (!entry) {
			if (errno) {
				throw IOException("Cannot enumerate registered Vortex generation");
			}
			break;
		}
		auto name = string(entry->d_name);
		if (name == "." || name == "..") {
			continue;
		}
		auto path = prefix.empty() ? name : prefix + "/" + name;
		struct stat info;
		if (fstatat(fd, name.c_str(), &info, AT_SYMLINK_NOFOLLOW)) {
			throw IOException("Cannot inspect registered Vortex path: %s", path);
		}
		if (plan.paths.count(path) && S_ISREG(info.st_mode)) {
			continue;
		}
		if (plan.directories.count(path) && S_ISDIR(info.st_mode)) {
			auto child = OpenDirectory(fd, name, false);
			InspectDirectory(child->fd, path, plan);
			continue;
		}
		throw InvalidInputException("Unexpected path in registered Vortex generation: %s", path);
	}
}

void VerifyCollectedFile(int32_t root, const CollectedFile &file, bool missing) {
	auto parent = OpenParent(root, file.path, missing);
	if (!parent.directory) {
		return;
	}
	auto raw = openat(parent.directory->fd, parent.name.c_str(), O_RDONLY | O_NOFOLLOW | O_CLOEXEC | O_NONBLOCK);
	if (raw < 0) {
		if (missing && errno == ENOENT) {
			return;
		}
		throw IOException("Cannot open registered Vortex artifact: %s", file.path);
	}
	Descriptor descriptor(raw);
	struct stat info;
	if (fstat(raw, &info) || !S_ISREG(info.st_mode) || info.st_size < 0 || idx_t(info.st_size) != file.size) {
		throw InvalidInputException("Registered Vortex artifact size/type changed: %s", file.path);
	}
	string bytes(file.size, '\0');
	idx_t offset = 0;
	while (offset < bytes.size()) {
		auto count = pread(raw, &bytes[offset], bytes.size() - offset, offset);
		if (count < 0 && errno == EINTR) {
			continue;
		}
		if (count <= 0) {
			throw IOException("Cannot read registered Vortex artifact: %s", file.path);
		}
		offset += idx_t(count);
	}
	if (Hash(bytes) != file.checksum) {
		throw InvalidInputException("Registered Vortex artifact checksum changed: %s", file.path);
	}
}

void Unlink(int32_t root, const string &path, bool directory) {
	auto parent = OpenParent(root, path, true);
	if (!parent.directory) {
		return;
	}
	if (unlinkat(parent.directory->fd, parent.name.c_str(), directory ? AT_REMOVEDIR : 0) && errno != ENOENT) {
		throw IOException("Cannot remove registered Vortex path: %s", path);
	}
	SyncDirectory(parent.directory->fd);
}

void InspectBuild(int32_t fd, const string &prefix, CollectionPlan &plan,
                  const std::function<void(const string &)> &check) {
	auto copy = dup(fd);
	if (copy < 0) {
		throw IOException("Cannot retain owned Vortex build directory");
	}
	auto raw = fdopendir(copy);
	if (!raw) {
		close(copy);
		throw IOException("Cannot enumerate owned Vortex build directory");
	}
	std::unique_ptr<DIR, decltype(&closedir)> directory(raw, closedir);
	while (true) {
		errno = 0;
		auto entry = readdir(directory.get());
		if (!entry) {
			if (errno) {
				throw IOException("Cannot enumerate owned Vortex build directory");
			}
			break;
		}
		auto name = string(entry->d_name);
		if (name == "." || name == "..") {
			continue;
		}
		check("build_gc_inspecting");
		CheckRelative(name, true);
		auto path = prefix + "/" + name;
		if (plan.files.size() + plan.directories.size() >= 4096) {
			throw InvalidInputException("RESOURCE_EXHAUSTED: Vortex build cleanup entry budget exceeded");
		}
		struct stat info;
		if (fstatat(fd, name.c_str(), &info, AT_SYMLINK_NOFOLLOW)) {
			throw IOException("Cannot inspect owned Vortex build path");
		}
		if (S_ISDIR(info.st_mode)) {
			plan.directories.insert(path);
			auto child = OpenDirectory(fd, name, false);
			InspectBuild(child->fd, path, plan, check);
		} else if (S_ISREG(info.st_mode) && info.st_size >= 0) {
			plan.files.push_back({path, "", idx_t(info.st_size)});
		} else {
			throw InvalidInputException("Unexpected link/type in owned Vortex build path: %s", path);
		}
	}
}

bool RemoveBuildFile(int32_t root, const CollectedFile &file, idx_t &remaining, idx_t &reclaimed,
                     const std::function<void(const string &)> &check) {
	auto parent = OpenParent(root, file.path, true);
	if (!parent.directory) {
		return true;
	}
	struct stat info;
	if (fstatat(parent.directory->fd, parent.name.c_str(), &info, AT_SYMLINK_NOFOLLOW)) {
		if (errno == ENOENT) {
			return true;
		}
		throw IOException("Cannot inspect owned Vortex cleanup file");
	}
	if (!S_ISREG(info.st_mode) || info.st_size < 0) {
		throw InvalidInputException("Owned Vortex build file type changed");
	}
	auto bytes = info.st_nlink > 1 ? idx_t(0) : idx_t(info.st_size);
	if (bytes > remaining) {
		if (!remaining) {
			return false;
		}
		auto fd = openat(parent.directory->fd, parent.name.c_str(), O_WRONLY | O_NOFOLLOW | O_CLOEXEC | O_NONBLOCK);
		if (fd < 0) {
			throw IOException("Cannot open owned Vortex scratch file for bounded cleanup");
		}
		Descriptor descriptor(fd);
		struct stat current;
		if (fstat(fd, &current) || current.st_dev != info.st_dev || current.st_ino != info.st_ino ||
		    current.st_nlink != 1 || current.st_size != info.st_size || ftruncate(fd, info.st_size - remaining) ||
		    fsync(fd)) {
			throw IOException("Cannot truncate owned Vortex scratch file safely");
		}
		SyncDirectory(parent.directory->fd);
		reclaimed += remaining;
		remaining = 0;
		check("build_gc_truncated");
		return false;
	}
	Unlink(root, file.path, false);
	remaining -= bytes;
	reclaimed += bytes;
	check("build_gc_file_deleted");
	return true;
}
#endif

} // namespace

shared_ptr<DuckLakeVortexRewriteState> DuckLakeVortexRewriteState::Get(ClientContext &context) {
	return context.registered_state->Get<DuckLakeVortexRewriteState>("ducklake_vortex_rewrite");
}

void DuckLakeVortexIndex::CheckRewriteCommit(ClientContext &context, DuckLakeTransaction &transaction) {
	auto plan = DuckLakeVortexRewriteState::Get(context);
	if (!plan || (!plan->retire && !plan->bind)) {
		throw InvalidInputException("Vortex rewrite candidate is not prepared");
	}
	Connection probe(*context.db);
	try {
		auto &fresh = Begin(probe, plan->catalog);
		auto snapshot = fresh.GetSnapshot();
		auto entry = fresh.GetCatalog().GetEntry(*probe.context, CatalogType::TABLE_ENTRY, plan->schema, plan->table,
		                                         OnEntryNotFound::THROW_EXCEPTION);
		auto &table = entry->Cast<DuckLakeTableEntry>();
		auto current = Lookup(fresh, table, plan->before.name);
		auto aliases = Query(fresh, "SELECT count(*) FROM {METADATA_CATALOG}." + string(CATALOG_TABLE) +
		                                " WHERE table_uuid=" + Value(plan->table_uuid).ToSQLString());
		if (table.GetTableUUID() != plan->table_uuid || table.GetTableId().index != plan->table_id ||
		    current.ToJSON() != plan->before.ToJSON() || aliases->Fetch()->GetValue(0, 0).GetValue<idx_t>() != 1) {
			throw TransactionException("Vortex rewrite capture or index changed before commit");
		}
		if (RewriteInputs(*probe.context, fresh, table, *plan) != plan->input_identity) {
			throw TransactionException("Vortex rewrite input or schema changed before commit");
		}
		if (plan->retire) {
			CheckEmpty(probe, *plan);
		} else if (plan->prune) {
			CheckPrune(probe, *plan, false);
		}
		probe.Commit();
		transaction.RebaseVortexRewrite(snapshot);
		if (!plan->prune) {
			plan->new_file_id = snapshot.next_file_id;
		}
		plan->commit_snapshot_id = snapshot.snapshot_id + 1;
		if (!plan->retire) {
			plan->bind(plan->new_file_id);
		}
		plan->check(plan->retire ? "retire_before_commit" : "rewrite_before_commit");
	} catch (...) {
		Rollback(probe);
		throw;
	}
}

void DuckLakeVortexIndex::PublishRewrite(ClientContext &context, DuckLakeTransaction &transaction,
                                         const DuckLakeSnapshot &snapshot) {
	auto plan = DuckLakeVortexRewriteState::Get(context);
	if (!plan || snapshot.snapshot_id != plan->commit_snapshot_id) {
		throw TransactionException("Vortex rewrite commit snapshot mismatch");
	}
	auto entry = transaction.GetCatalog().GetEntryById(transaction, snapshot, TableIndex(plan->table_id));
	if (!entry) {
		throw TransactionException("Vortex rewrite table disappeared before publication");
	}
	auto &table = entry->Cast<DuckLakeTableEntry>();
	auto current = Lookup(transaction, table, plan->before.name);
	if (current.ToJSON() != plan->before.ToJSON()) {
		throw TransactionException("Vortex rewrite publication revision conflict");
	}
	auto &fs = FileSystem::GetFileSystem(context);
	auto files = transaction.GetMetadataManager().GetExtendedFilesForTable(table, snapshot);
	vector<DuckLakeVortexIndexFile> identities;
	idx_t remaining = MAX_ARTIFACTS;
	bool found_output = false;
	for (auto &file : files) {
		if (file.data_type != DuckLakeDataType::DATA_FILE) {
			continue;
		}
		if (std::find(plan->replaced.begin(), plan->replaced.end(), file.file_id.index) != plan->replaced.end()) {
			throw InvalidInputException("Vortex rewrite source stayed active");
		}
		auto bytes = Read(fs, file.file.path, remaining, false);
		remaining -= bytes.size();
		identities.push_back({file.file_id.index, file.file.path, Hash(bytes), file.row_count});
		if (file.file.path == plan->new_path) {
			found_output = file.file_id.index == plan->new_file_id && file.row_count == plan->new_rows &&
			               file.file.file_size_bytes == plan->new_bytes &&
			               (!plan->to_f || identities.back().version == plan->output_version);
		}
	}
	if (!plan->retire && !plan->prune && !found_output) {
		throw InvalidInputException("Vortex rewrite output file identity mismatch");
	}
	plan->candidate.ValidateCoverage(identities);
	remaining = MAX_ARTIFACTS;
	for (auto &segment : plan->candidate.segments) {
		auto part = ParseReference(segment.reference, plan->table_uuid, plan->before.field_id);
		part.root = segment.root;
		auto before = remaining;
		auto manifest = VerifyGeneration(fs, part, remaining);
		RegisterGeneration(transaction, part, manifest, before - remaining);
	}
	UpdateGenerationRefs(transaction, plan->table_uuid, plan->candidate);
	auto updated = Query(transaction, "UPDATE {METADATA_CATALOG}." + string(CATALOG_TABLE) +
	                                      " SET revision=" + to_string(plan->candidate.revision) +
	                                      ",index_root=" + Value(plan->candidate.root).ToSQLString() +
	                                      ",reference=" + Value(plan->candidate.reference).ToSQLString() +
	                                      " WHERE table_uuid=" + Value(plan->table_uuid).ToSQLString() +
	                                      " AND index_name=" + Value(plan->before.name).ToSQLString() +
	                                      " AND revision=" + to_string(plan->before.revision) + " RETURNING revision");
	if (!updated->Fetch()) {
		throw TransactionException("Vortex rewrite publication ownership changed");
	}
	if (!plan->retire && !plan->prune) {
		Query(transaction, "UPDATE {METADATA_CATALOG}." + string(REWRITES) + " SET keep_file=true,new_file_id=" +
		                       to_string(plan->new_file_id) + " WHERE build_id=" + Value(plan->build_id).ToSQLString());
	}
	if (plan->to_f) {
		Query(transaction, "UPDATE {METADATA_CATALOG}." + string(BUILDS) +
		                       " SET state='published' WHERE build_id=" + Value(plan->build_id).ToSQLString());
	}
	plan->check(plan->retire ? "retire_after_stage" : "rewrite_after_stage");
}

DuckLakeTransaction &DuckLakeVortexIndex::Begin(Connection &owner, const string &name) {
	owner.BeginTransaction();
	auto &entry = Catalog::GetCatalog(*owner.context, name);
	if (entry.GetCatalogType() != "ducklake") {
		throw InvalidInputException("Vortex operations require an attached DuckLake catalog");
	}
	auto &catalog = entry.Cast<DuckLakeCatalog>();
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
	for (auto &name : {CATALOG_TABLE, GENERATIONS, GENERATION_REFS, BUILDS, REWRITES}) {
		if (!transaction.GetConnection().TableInfo(catalog.MetadataDatabaseName(), catalog.MetadataSchemaName(),
		                                           name)) {
			throw InvalidInputException("Vortex index catalog is not initialized");
		}
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

StorageLock &DuckLakeVortexIndex::CollectionLock() {
	static StorageLock lock;
	return lock;
}

DuckLakeVortexIndex DuckLakeVortexIndex::Create(Connection &owner, const string &catalog_name, const string &schema,
                                                const string &table_name, const string &name, const string &root,
                                                uint32_t dimension) {
	Name(name);
	if (!dimension || dimension > 4096 || root.empty() || root.size() > 65536 || root.find('\0') != string::npos) {
		throw InvalidInputException("Invalid Vortex index definition dimension or root");
	}
	if (owner.context->transaction.HasActiveTransaction()) {
		throw InvalidInputException("Vortex index creation requires an idle dedicated connection");
	}
	auto publication = PublicationLock().GetExclusiveLock();
	try {
		auto &transaction = Begin(owner, catalog_name);
		Initialized(transaction);
		auto &catalog = transaction.GetCatalog();
		auto entry = catalog.GetEntry(*owner.context, CatalogType::TABLE_ENTRY, schema, table_name,
		                              OnEntryNotFound::THROW_EXCEPTION);
		auto &table = entry->Cast<DuckLakeTableEntry>();
		DuckLakeVortex::ValidateTable(table);
		DuckLakeVortexReadView::ValidateFields(table);
		if (DuckLakeVortex::GetFormat(table) != DataFileFormat::VORTEX ||
		    table.GetColumns().PhysicalColumnCount() != 4 || table.GetColumns().LogicalColumnCount() != 4) {
			throw InvalidInputException("Vortex index definition requires a Vortex table with four business columns");
		}
		auto existing = Query(transaction, "SELECT count(*) FROM {METADATA_CATALOG}." + string(CATALOG_TABLE) +
		                                       " WHERE table_uuid=" + Value(table.GetTableUUID()).ToSQLString() +
		                                       " AND index_name=" + Value(name).ToSQLString());
		if (existing->Fetch()->GetValue(0, 0).GetValue<idx_t>()) {
			throw InvalidInputException("Vortex named index already exists");
		}
		auto qualified = KeywordHelper::WriteOptionallyQuoted(catalog_name) + "." +
		                 KeywordHelper::WriteOptionallyQuoted(schema) + "." +
		                 KeywordHelper::WriteOptionallyQuoted(table_name);
		auto rows = owner.Query("SELECT 1 FROM " + qualified + " LIMIT 1");
		if (rows->HasError()) {
			rows->ThrowError();
		}
		if (rows->Fetch()) {
			throw InvalidInputException(
			    "Vortex index definition requires an empty table; build existing data explicitly");
		}
		auto &fs = FileSystem::GetFileSystem(*owner.context);
		if (!fs.IsPathAbsolute(root)) {
			throw InvalidInputException("Vortex index root must be absolute");
		}
		auto canonical_root = Canonical(root);
		auto managed = fs.ExpandPath(catalog.DataPath());
		while (managed.size() > 1 && managed.back() == '/') {
			managed.pop_back();
		}
		if (!fs.DirectoryExists(managed)) {
			fs.CreateDirectory(managed);
		}
		managed = Canonical(managed);
		if (canonical_root == managed ||
		    StringUtil::StartsWith(canonical_root, managed == "/" ? managed : managed + "/")) {
			throw InvalidInputException("Vortex index root must be outside managed data");
		}
		DuckLakeVortexIndex result;
		result.name = name;
		result.revision = 1;
		result.field_id = table.GetFieldId(vector<string> {"embedding"}).GetFieldIndex().index;
		result.dimension = dimension;
		result.root = canonical_root;
		Compose(result, {}, table.GetTableUUID());
		Query(transaction, "INSERT INTO {METADATA_CATALOG}." + string(CATALOG_TABLE) + " VALUES(" +
		                       Value(table.GetTableUUID()).ToSQLString() + "," + Value(name).ToSQLString() + ",1," +
		                       Value(result.root).ToSQLString() + "," + Value(result.reference).ToSQLString() + ")");
		owner.Commit();
		return result;
	} catch (...) {
		Rollback(owner);
		throw;
	}
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
		Query(transaction, "CREATE TABLE {METADATA_CATALOG}.ducklake_vortex_generations_v1("
		                   "index_root VARCHAR NOT NULL,generation VARCHAR NOT NULL,reference VARCHAR NOT "
		                   "NULL,manifest VARCHAR NOT NULL,"
		                   "artifact_bytes BIGINT NOT NULL CHECK(artifact_bytes>=0),state VARCHAR NOT NULL "
		                   "CHECK(state IN ('active','retired','deleting')),PRIMARY KEY(index_root,generation))");
		Query(transaction, "CREATE TABLE {METADATA_CATALOG}.ducklake_vortex_generation_refs_v1("
		                   "table_uuid VARCHAR NOT NULL,index_name VARCHAR NOT NULL,index_root VARCHAR NOT "
		                   "NULL,generation VARCHAR NOT NULL,"
		                   "PRIMARY KEY(table_uuid,index_name,index_root,generation))");
		Query(transaction, "CREATE TABLE {METADATA_CATALOG}.ducklake_vortex_builds_v1("
		                   "build_id VARCHAR PRIMARY KEY,table_uuid VARCHAR NOT NULL,index_name VARCHAR NOT NULL,"
		                   "expected_revision BIGINT NOT NULL,index_root VARCHAR NOT NULL,generation VARCHAR NOT NULL,"
		                   "workspace VARCHAR NOT NULL,reference_path VARCHAR NOT NULL,state VARCHAR NOT NULL "
		                   "CHECK(state IN ('allocated','building','sealed','published','abandoned','deleting')),"
		                   "keep_generation BOOLEAN NOT NULL,UNIQUE(index_root,generation))");
		Query(transaction, "CREATE TABLE {METADATA_CATALOG}.ducklake_vortex_rewrites_v1("
		                   "build_id VARCHAR PRIMARY KEY,table_uuid VARCHAR NOT NULL,source_root VARCHAR NOT NULL,"
		                   "new_path VARCHAR NOT NULL,capture_snapshot BIGINT NOT NULL,new_file_id BIGINT NOT NULL,"
		                   "keep_file BOOLEAN NOT NULL)");
		owner.Commit();
	} catch (...) {
		Rollback(owner);
		throw;
	}
}

vector<vector<Value>> DuckLakeVortexIndex::GenerationStatus(Connection &owner, const string &catalog) {
	try {
		auto &transaction = Begin(owner, catalog);
		Initialized(transaction);
		auto result = Query(
		    transaction,
		    "SELECT g.index_root,g.generation,g.state,g.artifact_bytes,count(r.generation) FROM {METADATA_CATALOG}." +
		        string(GENERATIONS) + " g LEFT JOIN {METADATA_CATALOG}." + GENERATION_REFS +
		        " r ON g.index_root=r.index_root AND g.generation=r.generation "
		        "GROUP BY g.index_root,g.generation,g.state,g.artifact_bytes ORDER BY g.index_root,g.generation");
		vector<vector<Value>> rows;
		while (auto chunk = result->Fetch()) {
			for (idx_t row = 0; row < chunk->size(); row++) {
				rows.push_back({chunk->GetValue(0, row), chunk->GetValue(1, row), chunk->GetValue(2, row),
				                Value::UBIGINT(chunk->GetValue(3, row).GetValue<idx_t>()),
				                Value::UBIGINT(chunk->GetValue(4, row).GetValue<idx_t>())});
			}
		}
		owner.Commit();
		return rows;
	} catch (...) {
		Rollback(owner);
		throw;
	}
}

DuckLakeVortexGCResult DuckLakeVortexIndex::Collect(ClientContext &context, const string &catalog,
                                                    idx_t max_generations, idx_t max_artifact_bytes,
                                                    const std::function<void(const string &)> &observe) {
	if (!max_generations || max_generations > 16 || !max_artifact_bytes || max_artifact_bytes > MAX_ARTIFACTS) {
		throw InvalidInputException("Invalid Vortex collection count or artifact byte budget");
	}
#ifdef _WIN32
	throw NotImplementedException("Native Vortex generation collection is qualified on POSIX paths");
#else
	auto check = [&](const string &stage) {
		if (observe) {
			observe(stage);
		}
		if (context.IsInterrupted()) {
			throw InterruptException();
		}
	};
	check("gc_waiting");
	auto admissions = CollectionLock().GetExclusiveLock();
	auto publication = PublicationLock().GetExclusiveLock();
	check("gc_acquired");
	Connection owner(*context.db);
	DuckLakeVortexGCResult collected;
	try {
		auto &transaction = Begin(owner, catalog);
		Initialized(transaction);
		auto result =
		    Query(transaction,
		          "SELECT index_root,generation,reference,manifest,artifact_bytes,state FROM {METADATA_CATALOG}." +
		              string(GENERATIONS) +
		              " g WHERE state IN ('retired','deleting') AND NOT EXISTS "
		              "(SELECT 1 FROM {METADATA_CATALOG}." +
		              GENERATION_REFS +
		              " r WHERE r.index_root=g.index_root AND r.generation=g.generation) "
		              "ORDER BY index_root,generation LIMIT " +
		              to_string(max_generations));
		vector<vector<Value>> entries;
		while (auto chunk = result->Fetch()) {
			for (idx_t row = 0; row < chunk->size(); row++) {
				vector<Value> entry;
				for (idx_t col = 0; col < 6; col++) {
					entry.push_back(chunk->GetValue(col, row));
				}
				entries.push_back(std::move(entry));
			}
		}
		auto managed = transaction.GetCatalog().DataPath();
		owner.Commit();
		if (!entries.empty()) {
			managed = FileSystem::GetFileSystem(context).ExpandPath(managed);
			while (managed.size() > 1 && managed.back() == '/') {
				managed.pop_back();
			}
			managed = Canonical(managed);
		}
		for (auto &entry : entries) {
			auto bytes = entry[4].GetValue<idx_t>();
			if (bytes > max_artifact_bytes - collected.artifact_bytes) {
				break;
			}
			auto root = StringValue::Get(entry[0]);
			if (root == managed || StringUtil::StartsWith(root, managed == "/" ? managed : managed + "/")) {
				throw InvalidInputException("Vortex collection root must be outside managed data");
			}
			auto generation = StringValue::Get(entry[1]);
			auto reference = StringValue::Get(entry[2]);
			auto manifest = StringValue::Get(entry[3]);
			const bool resume = StringValue::Get(entry[5]) == "deleting";
			CheckRelative(generation, true);
			Parsed parsed(reference);
			auto index =
			    ParseReference(reference, Text(parsed.Root(), "table_uuid"), Number(parsed.Root(), "field_id"));
			if (index.generation != generation) {
				throw InvalidInputException("Registered Vortex generation identity changed");
			}
			auto plan = Plan(reference, manifest, bytes);
			auto directory = OpenRoot(root, resume);
			auto stored = directory ? OpenDirectory(directory->fd, generation, resume) : nullptr;
			struct stat identity;
			if (stored) {
				if (fstat(stored->fd, &identity)) {
					throw IOException("Cannot identify registered Vortex generation");
				}
				InspectDirectory(stored->fd, "", plan);
				for (auto &file : plan.files) {
					check("gc_verifying");
					VerifyCollectedFile(stored->fd, file, resume);
				}
			}
			check("gc_validated");
			auto where = GenerationWhere(root, generation);
			auto &mark = Begin(owner, catalog);
			Query(mark, "UPDATE {METADATA_CATALOG}." + string(GENERATIONS) + " SET state='deleting'" + where);
			owner.Commit();
			check("gc_deleting");
			if (stored) {
				for (auto &file : plan.files) {
					check("gc_before_unlink");
					Unlink(stored->fd, file.path, false);
					check("gc_file_deleted");
				}
				for (auto path = plan.directories.rbegin(); path != plan.directories.rend(); ++path) {
					Unlink(stored->fd, *path, true);
				}
				struct stat current;
				if (fstatat(directory->fd, generation.c_str(), &current, AT_SYMLINK_NOFOLLOW) ||
				    current.st_dev != identity.st_dev || current.st_ino != identity.st_ino) {
					throw IOException("Registered Vortex directory identity changed during collection");
				}
				Unlink(directory->fd, generation, true);
			}
			check("gc_files_deleted");
			auto &finish = Begin(owner, catalog);
			Query(finish, "DELETE FROM {METADATA_CATALOG}." + string(GENERATIONS) + where);
			check("gc_before_finish");
			owner.Commit();
			collected.generations++;
			collected.artifact_bytes += bytes;
			check("gc_after_finish");
		}
		auto &remaining = Begin(owner, catalog);
		auto count = Query(remaining, "SELECT count(*) FROM {METADATA_CATALOG}." + string(GENERATIONS) +
		                                  " WHERE state IN ('retired','deleting')");
		collected.remaining = count->Fetch()->GetValue(0, 0).GetValue<idx_t>();
		owner.Commit();
		return collected;
	} catch (...) {
		Rollback(owner);
		throw;
	}
#endif
}

vector<vector<Value>> DuckLakeVortexIndex::BuildStatus(Connection &owner, const string &catalog) {
	try {
		auto &transaction = Begin(owner, catalog);
		Initialized(transaction);
		auto result =
		    Query(transaction, "SELECT build_id,index_root,generation,workspace,state,keep_generation,"
		                       "expected_revision,reference_path,table_uuid,index_name FROM {METADATA_CATALOG}." +
		                           string(BUILDS) + " ORDER BY build_id");
		vector<vector<Value>> rows;
		while (auto chunk = result->Fetch()) {
			for (idx_t row = 0; row < chunk->size(); row++) {
				vector<Value> values;
				for (idx_t col = 0; col < 10; col++) {
					values.push_back(col == 6 ? Value::UBIGINT(chunk->GetValue(col, row).GetValue<idx_t>())
					                          : chunk->GetValue(col, row));
				}
				rows.push_back(std::move(values));
			}
		}
		owner.Commit();
		return rows;
	} catch (...) {
		Rollback(owner);
		throw;
	}
}

idx_t DuckLakeVortexIndex::RecoverBuilds(ClientContext &context, const string &catalog, idx_t max_builds,
                                         const std::function<void(const string &)> &observe) {
	if (!max_builds || max_builds > 16) {
		throw InvalidInputException("Invalid Vortex build recovery count budget");
	}
	auto check = [&](const string &stage) {
		if (observe) {
			observe(stage);
		}
		if (context.IsInterrupted()) {
			throw InterruptException();
		}
	};
	check("build_recovery_waiting");
	auto admissions = CollectionLock().GetExclusiveLock();
	auto publication = PublicationLock().GetExclusiveLock();
	Connection owner(*context.db);
	try {
		auto &transaction = Begin(owner, catalog);
		Initialized(transaction);
		auto result =
		    Query(transaction, "SELECT build_id FROM {METADATA_CATALOG}." + string(BUILDS) +
		                           " WHERE state IN ('allocated','building','sealed') ORDER BY build_id LIMIT " +
		                           to_string(max_builds));
		vector<string> ids;
		while (auto chunk = result->Fetch()) {
			for (idx_t row = 0; row < chunk->size(); row++) {
				ids.push_back(StringValue::Get(chunk->GetValue(0, row)));
			}
		}
		owner.Commit();
		for (auto &id : ids) {
			check("build_recovery_before_reconcile");
			ReconcileBuild(owner, catalog, id);
			check("build_recovery_reconciled");
		}
		return ids.size();
	} catch (...) {
		Rollback(owner);
		throw;
	}
}

DuckLakeVortexBuildGCResult DuckLakeVortexIndex::CollectBuilds(ClientContext &context, const string &catalog,
                                                               idx_t max_builds, idx_t max_bytes,
                                                               const std::function<void(const string &)> &observe) {
	if (!max_builds || max_builds > 16 || !max_bytes || max_bytes > MAX_ARTIFACTS) {
		throw InvalidInputException("Invalid Vortex build collection count or byte budget");
	}
#ifdef _WIN32
	throw NotImplementedException("Native Vortex build collection is qualified on POSIX paths");
#else
	std::function<void(const string &)> check = [&](const string &stage) {
		if (observe) {
			observe(stage);
		}
		if (context.IsInterrupted()) {
			throw InterruptException();
		}
	};
	check("build_gc_waiting");
	auto admissions = CollectionLock().GetExclusiveLock();
	auto publication = PublicationLock().GetExclusiveLock();
	check("build_gc_acquired");
	Connection owner(*context.db);
	DuckLakeVortexBuildGCResult collected;
	try {
		auto &transaction = Begin(owner, catalog);
		Initialized(transaction);
		auto result =
		    Query(transaction, "SELECT build_id,index_root,generation,workspace,keep_generation FROM "
		                       "{METADATA_CATALOG}." +
		                           string(BUILDS) +
		                           " WHERE state IN ('published','abandoned','deleting') ORDER BY build_id LIMIT " +
		                           to_string(max_builds));
		vector<vector<Value>> entries;
		while (auto chunk = result->Fetch()) {
			for (idx_t row = 0; row < chunk->size(); row++) {
				vector<Value> values;
				for (idx_t col = 0; col < 5; col++) {
					values.push_back(chunk->GetValue(col, row));
				}
				entries.push_back(std::move(values));
			}
		}
		auto managed = transaction.GetCatalog().DataPath();
		owner.Commit();
		if (!entries.empty()) {
			managed = FileSystem::GetFileSystem(context).ExpandPath(managed);
			while (managed.size() > 1 && managed.back() == '/') {
				managed.pop_back();
			}
			managed = Canonical(managed);
		}
		idx_t remaining = max_bytes;
		for (auto &entry : entries) {
			auto id = StringValue::Get(entry[0]);
			auto root = StringValue::Get(entry[1]);
			auto generation = StringValue::Get(entry[2]);
			auto workspace = StringValue::Get(entry[3]);
			CheckRelative(id, true);
			if (generation != "generation-" + id ||
			    workspace != FileSystem::GetFileSystem(context).JoinPath(root, ".build-" + id) || root == managed ||
			    StringUtil::StartsWith(root, managed == "/" ? managed : managed + "/")) {
				throw InvalidInputException("Registered Vortex build namespace mismatch");
			}
			auto &inspect = Begin(owner, catalog);
			bool keep = entry[4].GetValue<bool>() || PublishedGeneration(inspect, root, generation);
			string source_root, source_name;
			bool keep_source = true;
			auto rewrite = Query(inspect, "SELECT source_root,new_path,keep_file FROM {METADATA_CATALOG}." +
			                                  string(REWRITES) + " WHERE build_id=" + Value(id).ToSQLString());
			auto rewrite_row = rewrite->Fetch();
			if (rewrite_row && rewrite_row->size()) {
				source_root = StringValue::Get(rewrite_row->GetValue(0, 0));
				source_name = "ducklake-" + id + ".vortex";
				auto source_path = StringValue::Get(rewrite_row->GetValue(1, 0));
				if ((source_root != managed && !StringUtil::StartsWith(source_root, managed + "/")) ||
				    source_path != FileSystem::GetFileSystem(context).JoinPath(source_root, source_name)) {
					throw InvalidInputException("Registered Vortex rewrite source namespace mismatch");
				}
				auto references = Query(
				    inspect, "SELECT count(*) FROM {METADATA_CATALOG}.ducklake_data_file WHERE path=" +
				                 Value(source_path).ToSQLString() + " OR path=" + Value(source_name).ToSQLString() +
				                 " OR path LIKE " + Value("%/" + source_name).ToSQLString());
				keep_source = keep || rewrite_row->GetValue(2, 0).GetValue<bool>() ||
				              references->Fetch()->GetValue(0, 0).GetValue<idx_t>() != 0;
			}
			owner.Commit();
			auto directory = OpenRoot(root, true);
			CollectionPlan plan;
			if (directory) {
				vector<string> paths = {".build-" + id};
				if (!keep) {
					paths.push_back(generation);
				}
				for (auto &path : paths) {
					auto child = OpenDirectory(directory->fd, path, true);
					if (child) {
						plan.directories.insert(path);
						InspectBuild(child->fd, path, plan, check);
					}
				}
			}
			check("build_gc_validated");
			auto &mark = Begin(owner, catalog);
			Query(mark, "UPDATE {METADATA_CATALOG}." + string(BUILDS) + " SET state='deleting',keep_generation=" +
			                string(keep ? "true" : "false") + " WHERE build_id=" + Value(id).ToSQLString());
			if (!source_root.empty()) {
				Query(mark, "UPDATE {METADATA_CATALOG}." + string(REWRITES) + " SET keep_file=" +
				                string(keep_source ? "true" : "false") + " WHERE build_id=" + Value(id).ToSQLString());
			}
			owner.Commit();
			check("build_gc_deleting");
			bool complete = true;
			if (directory) {
				for (auto &file : plan.files) {
					check("build_gc_before_unlink");
					if (!RemoveBuildFile(directory->fd, file, remaining, collected.bytes, check)) {
						complete = false;
						break;
					}
				}
			}
			if (complete && !source_root.empty() && !keep_source) {
				auto source_directory = OpenRoot(source_root, true);
				if (source_directory) {
					complete =
					    RemoveBuildFile(source_directory->fd, {source_name, "", 0}, remaining, collected.bytes, check);
				}
			}
			if (complete && directory) {
				for (auto path = plan.directories.rbegin(); path != plan.directories.rend(); ++path) {
					Unlink(directory->fd, *path, true);
				}
			}
			if (!complete) {
				break;
			}
			check("build_gc_files_deleted");
			auto &finish = Begin(owner, catalog);
			Query(finish,
			      "DELETE FROM {METADATA_CATALOG}." + string(REWRITES) + " WHERE build_id=" + Value(id).ToSQLString());
			Query(finish,
			      "DELETE FROM {METADATA_CATALOG}." + string(BUILDS) + " WHERE build_id=" + Value(id).ToSQLString());
			check("build_gc_before_finish");
			owner.Commit();
			collected.builds++;
			check("build_gc_after_finish");
		}
		auto &finish = Begin(owner, catalog);
		auto count = Query(finish, "SELECT count(*) FROM {METADATA_CATALOG}." + string(BUILDS));
		collected.remaining = count->Fetch()->GetValue(0, 0).GetValue<idx_t>();
		owner.Commit();
		return collected;
	} catch (...) {
		Rollback(owner);
		throw;
	}
#endif
}

DuckLakeVortexIndex DuckLakeVortexIndex::ParseSegments(const string &reference, const string &uuid, idx_t field_id) {
	return ParseCatalogReference(reference, uuid, field_id);
}

bool DuckLakeVortexIndex::ReferencesSource(DuckLakeTransaction &transaction, const string &path) {
	auto result = Query(transaction, "SELECT table_uuid,reference FROM {METADATA_CATALOG}.ducklake_vortex_indexes_v1");
	while (auto chunk = result->Fetch()) {
		for (idx_t row = 0; row < chunk->size(); row++) {
			auto reference = StringValue::Get(chunk->GetValue(1, row));
			Parsed parsed(reference);
			auto index =
			    ParseSegments(reference, StringValue::Get(chunk->GetValue(0, row)), Number(parsed.Root(), "field_id"));
			for (auto &file : index.files) {
				auto &fs = FileSystem::GetFileSystem(*transaction.GetConnection().context);
				if (fs.FileSystem::CanonicalizePath(file.uri) == path) {
					return true;
				}
			}
		}
	}
	return false;
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

vector<DuckLakeVortexIndexFile> DuckLakeVortexIndex::SelectedFiles(const vector<string> &generations) const {
	if (generations.size() != 2 || generations[0].empty() || generations[0] == generations[1]) {
		throw InvalidInputException("Vortex merge requires two distinct ready generations");
	}
	vector<DuckLakeVortexIndexFile> selected;
	idx_t found = 0;
	for (auto &segment : segments) {
		if (std::find(generations.begin(), generations.end(), segment.generation) != generations.end()) {
			found++;
			selected.insert(selected.end(), segment.files.begin(), segment.files.end());
		}
	}
	if (found != generations.size()) {
		throw TransactionException("Vortex merge selected generation is no longer ready");
	}
	std::sort(selected.begin(), selected.end(),
	          [](const DuckLakeVortexIndexFile &a, const DuckLakeVortexIndexFile &b) { return a.id < b.id; });
	return selected;
}

vector<string> DuckLakeVortexIndex::SmallestSegments() const {
	if (segments.size() != MAX_SEGMENTS) {
		throw InvalidInputException("Vortex automatic merge requires a full segment set");
	}
	vector<std::pair<idx_t, string>> sizes;
	for (auto &segment : segments) {
		idx_t rows = 0;
		for (auto &file : segment.files) {
			rows += file.row_count;
		}
		sizes.emplace_back(rows, segment.generation);
	}
	std::sort(sizes.begin(), sizes.end());
	return {sizes[0].second, sizes[1].second};
}

bool DuckLakeVortexIndex::CoversFile(DuckLakeTransaction &transaction, DuckLakeTableEntry &table, idx_t file_id) {
	auto &catalog = transaction.GetCatalog();
	if (!transaction.GetConnection().TableInfo(catalog.MetadataDatabaseName(), catalog.MetadataSchemaName(),
	                                           CATALOG_TABLE)) {
		return false;
	}
	auto records = Query(transaction, "SELECT reference FROM {METADATA_CATALOG}." + string(CATALOG_TABLE) +
	                                      " WHERE table_uuid=" + Value(table.GetTableUUID()).ToSQLString());
	while (auto chunk = records->Fetch()) {
		for (idx_t row = 0; row < chunk->size(); row++) {
			auto index = ParseSegments(StringValue::Get(chunk->GetValue(0, row)), table.GetTableUUID(),
			                           table.GetFieldId(vector<string> {"embedding"}).GetFieldIndex().index);
			for (auto &file : index.files) {
				if (file.id == file_id) {
					return true;
				}
			}
		}
	}
	return false;
}

void DuckLakeVortexIndex::CheckFileRetirements(ClientContext &context, DuckLakeTransaction &transaction) {
	if (!transaction.HasDroppedFiles()) {
		return;
	}
	auto &catalog = transaction.GetCatalog();
	if (!catalog.MetadataType().empty() && catalog.MetadataType() != "duckdb") {
		return;
	}
	Connection reader(*context.db);
	try {
		auto &fresh = Begin(reader, catalog.GetName());
		for (auto id : transaction.GetTablesDeletedFrom()) {
			auto entry = fresh.GetCatalog().GetEntryById(fresh, fresh.GetSnapshot(), id);
			if (!entry || entry->type != CatalogType::TABLE_ENTRY) {
				continue;
			}
			auto &table = entry->Cast<DuckLakeTableEntry>();
			for (auto &file : transaction.GetDroppedFiles()) {
				if (CoversFile(fresh, table, file.second.index)) {
					throw TransactionException(
					    "Indexed Vortex source cannot be retired without atomic index publication");
				}
			}
		}
		reader.Commit();
	} catch (...) {
		Rollback(reader);
		throw;
	}
}

string DuckLakeVortexIndex::Publication() const {
	Parsed parsed(reference);
	return Text(parsed.Root(), "publication");
}

DuckLakeVortexIndex DuckLakeVortexIndex::RetireEmpty(ClientContext &context, const string &catalog,
                                                     const string &schema, const string &table_name, const string &name,
                                                     const string &selected, idx_t expected_revision,
                                                     const std::function<void(const string &)> &observe,
                                                     const string &publication) {
	Name(name);
	if (selected.empty() || expected_revision >= idx_t(NumericLimits<int64_t>::Maximum())) {
		throw InvalidInputException("Invalid Vortex retirement arguments");
	}
	auto guard = CollectionLock().GetSharedLock();
	Connection source(*context.db);
	auto plan = source.context->registered_state->GetOrCreate<DuckLakeVortexRewriteState>("ducklake_vortex_rewrite");
	plan->retire = true;
	plan->catalog = catalog;
	plan->schema = schema;
	plan->table = table_name;
	plan->selected = selected;
	plan->build_id = publication.empty() ? UUID::ToString(UUID::GenerateRandomUUID()) : publication;
	if (UUID::ToString(UUID::FromString(plan->build_id)) != plan->build_id) {
		throw InvalidInputException("Invalid Vortex retirement publication identity");
	}
	plan->check = [&](const string &stage) {
		if (observe) {
			observe(stage);
		}
		if (context.IsInterrupted() || source.context->IsInterrupted()) {
			throw InterruptException();
		}
	};
	bool committed = false;
	try {
		auto &transaction = Begin(source, catalog);
		auto entry = transaction.GetCatalog().GetEntry(*source.context, CatalogType::TABLE_ENTRY, schema, table_name,
		                                               OnEntryNotFound::THROW_EXCEPTION);
		auto &table = entry->Cast<DuckLakeTableEntry>();
		plan->table_uuid = table.GetTableUUID();
		plan->table_id = table.GetTableId().index;
		plan->before = Lookup(transaction, table, name);
		if (plan->before.revision != expected_revision) {
			throw TransactionException("Vortex retirement publication revision conflict");
		}
		vector<DuckLakeVortexIndexSegment> remaining;
		for (auto &segment : plan->before.segments) {
			if (segment.generation == selected) {
				plan->selected_files = segment.files;
			} else {
				remaining.push_back(segment);
			}
		}
		if (plan->selected_files.empty()) {
			throw InvalidInputException("Vortex retirement selected generation is not ready");
		}
		plan->input_identity = RewriteInputs(*source.context, transaction, table, *plan);
		CheckEmpty(source, *plan);
		plan->candidate = plan->before;
		plan->candidate.revision = expected_revision + 1;
		Compose(plan->candidate, std::move(remaining), plan->table_uuid, plan->build_id);
		for (auto &file : plan->selected_files) {
			plan->replaced.push_back(file.id);
			transaction.DropFile(table.GetTableId(), DataFileIndex(file.id), file.uri);
		}
		plan->check("retire_candidate_ready");
		source.Commit();
		committed = true;
		plan->check("retire_after_commit");
		return plan->candidate;
	} catch (...) {
		Rollback(source);
		if (committed) {
			throw IOException("Vortex retirement committed but acknowledgement failed; inspect catalog before retry");
		}
		throw;
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

DuckLakeVortexIndex DuckLakeVortexIndex::BuildAndPublish(
    ClientContext &context, const string &catalog_name, const string &schema, const string &table_name,
    const string &name, const string &reference_path, uint32_t dimension, idx_t expected_revision,
    const std::function<void(const string &)> &observe, const string &expected_table_uuid,
    optional_idx expected_field_id, DuckLakeVortexIndexOperation operation, const vector<string> &merge_generations) {
	Name(name);
	const bool merge = operation == DuckLakeVortexIndexOperation::MERGE;
	const bool select_index = operation != DuckLakeVortexIndexOperation::REPLACE;
	if ((!dimension && !merge) || dimension > 4096 || reference_path.empty() || reference_path.size() > 65536 ||
	    reference_path.find('\0') != string::npos || expected_revision > idx_t(NumericLimits<int64_t>::Maximum())) {
		throw InvalidInputException("Invalid Vortex build dimension, reference or revision");
	}
	if (!merge && !merge_generations.empty()) {
		throw InvalidInputException("Only a Vortex merge can select generations");
	}
	auto collection = CollectionLock().GetSharedLock();
	Connection owner(*context.db);
	auto id = UUID::ToString(UUID::GenerateRandomUUID());
	auto generation = "generation-" + id;
	string root, workspace;
	bool registered = false;
	std::function<void(const string &)> check = [&](const string &stage) {
		if (registered && stage == "generation_sealed") {
			BuildState(owner, catalog_name, id, "sealed");
		}
		if (observe) {
			observe(stage);
		}
		if (context.IsInterrupted()) {
			throw InterruptException();
		}
	};
	check("before_build");
	try {
		auto &transaction = Begin(owner, catalog_name);
		Initialized(transaction);
		auto &catalog = transaction.GetCatalog();
		auto entry = catalog.GetEntry(*owner.context, CatalogType::TABLE_ENTRY, schema, table_name,
		                              OnEntryNotFound::THROW_EXCEPTION);
		auto &table = entry->Cast<DuckLakeTableEntry>();
		if ((!expected_table_uuid.empty() && table.GetTableUUID() != expected_table_uuid) ||
		    (expected_field_id.IsValid() &&
		     table.GetFieldId(vector<string> {"embedding"}).GetFieldIndex().index != expected_field_id.GetIndex())) {
			throw InvalidInputException("Vortex optimizer table or vector field identity changed");
		}
		if (select_index) {
			auto previous = Lookup(transaction, table, name);
			if (dimension && previous.dimension != dimension) {
				throw InvalidInputException("Vortex segment dimension mismatch");
			}
			dimension = previous.dimension;
			if (previous.revision != expected_revision) {
				throw TransactionException("Vortex index publication revision conflict");
			}
			if (merge) {
				previous.SelectedFiles(merge_generations);
			}
		}
		auto &fs = FileSystem::GetFileSystem(context);
		if (!fs.IsPathAbsolute(reference_path) || reference_path.back() == '/' || fs.FileExists(reference_path)) {
			throw InvalidInputException("Build reference must be new, canonical and outside the managed data path");
		}
		root = Canonical(
		    reference_path.substr(0, reference_path.find_last_of('/') == 0 ? 1 : reference_path.find_last_of('/')));
		auto managed = fs.ExpandPath(catalog.DataPath());
		while (managed.size() > 1 && managed.back() == '/') {
			managed.pop_back();
		}
		if (!fs.DirectoryExists(managed)) {
			throw InvalidInputException("Vortex index build requires materialized Vortex files");
		}
		managed = Canonical(managed);
		if (root == managed || StringUtil::StartsWith(root, managed == "/" ? managed : managed + "/")) {
			throw InvalidInputException("Build reference must be new, canonical and outside the managed data path");
		}
		workspace = fs.JoinPath(root, ".build-" + id);
		if (fs.DirectoryExists(workspace) || fs.FileExists(workspace) ||
		    fs.DirectoryExists(fs.JoinPath(root, generation)) || fs.FileExists(fs.JoinPath(root, generation))) {
			throw InvalidInputException("Planned Vortex build namespace already exists");
		}
		Query(transaction, "INSERT INTO {METADATA_CATALOG}." + string(BUILDS) + " VALUES(" + Value(id).ToSQLString() +
		                       "," + Value(table.GetTableUUID()).ToSQLString() + "," + Value(name).ToSQLString() + "," +
		                       to_string(expected_revision) + "," + Value(root).ToSQLString() + "," +
		                       Value(generation).ToSQLString() + "," + Value(workspace).ToSQLString() + "," +
		                       Value(reference_path).ToSQLString() + ",'allocated',false)");
		owner.Commit();
		registered = true;
		check("build_allocated");
#ifndef _WIN32
		auto directory = OpenRoot(root, false);
		if (mkdirat(directory->fd, (".build-" + id).c_str(), 0700)) {
			throw IOException("Cannot create exclusive native Vortex build workspace");
		}
		SyncDirectory(directory->fd);
#else
		throw NotImplementedException("Native Vortex builds are qualified on POSIX paths");
#endif
		check("build_workspace_created");
		BuildState(owner, catalog_name, id, "building");
		DuckLakeVortexViewOptions options {catalog_name,
		                                   schema,
		                                   table_name,
		                                   select_index ? 0 : dimension,
		                                   MAX_ARTIFACTS,
		                                   100000,
		                                   select_index ? name : ""};
		options.unindexed_only = operation == DuckLakeVortexIndexOperation::APPEND;
		options.merge_generations = merge_generations;
		options.expected_table_uuid = expected_table_uuid;
		options.expected_field_id = expected_field_id;
		auto view = DuckLakeVortexReadView::AcquireBuild(owner, options, check, &context);
		BuildNative(context, view, reference_path, generation, workspace, check);
		check("before_build_publish");
		return Publish(owner, catalog_name, schema, table_name, name, reference_path, expected_revision, check,
		               operation, merge_generations);
	} catch (...) {
		auto error = std::current_exception();
		Rollback(owner);
		if (registered) {
			try {
				ReconcileBuild(owner, catalog_name, id);
			} catch (const std::exception &failure) {
				throw IOException(
				    "Native Vortex build failed and ownership reconciliation failed: %s; inspect build status",
				    failure.what());
			}
		}
		std::rethrow_exception(error);
	}
}

DuckLakeVortexIndex DuckLakeVortexIndex::Rewrite(ClientContext &context, const string &catalog_name,
                                                 const string &schema, const string &table_name, const string &name,
                                                 const string &selected, const string &reference_path,
                                                 idx_t expected_revision,
                                                 const std::function<void(const string &)> &observe,
                                                 DuckLakeVortexRewriteMode mode, const string &publication) {
	Name(name);
	const bool to_f = mode == DuckLakeVortexRewriteMode::FILES;
	const bool prune = mode == DuckLakeVortexRewriteMode::PRUNE;
	if (selected.empty() || (to_f ? !reference_path.empty() : reference_path.empty()) ||
	    reference_path.size() > 65536 || reference_path.find('\0') != string::npos ||
	    (!to_f && !prune && !publication.empty()) || expected_revision >= idx_t(NumericLimits<int64_t>::Maximum())) {
		throw InvalidInputException("Invalid Vortex rewrite arguments");
	}
	auto guard = CollectionLock().GetSharedLock();
	Connection source(*context.db), owner(*context.db);
	auto plan = source.context->registered_state->GetOrCreate<DuckLakeVortexRewriteState>("ducklake_vortex_rewrite");
	plan->catalog = catalog_name;
	plan->schema = schema;
	plan->table = table_name;
	plan->selected = selected;
	plan->reference = reference_path;
	plan->to_f = to_f;
	plan->prune = prune;
	plan->build_id = publication.empty() ? UUID::ToString(UUID::GenerateRandomUUID()) : publication;
	if (UUID::ToString(UUID::FromString(plan->build_id)) != plan->build_id) {
		throw InvalidInputException("Invalid Vortex rewrite publication identity");
	}
	auto generation = "generation-" + plan->build_id;
	bool registered = false, committed = false;
	plan->check = [&](const string &stage) {
		if (registered && stage == "generation_sealed") {
			BuildState(owner, catalog_name, plan->build_id, "sealed");
		}
		if (observe) {
			observe(stage);
		}
		if (context.IsInterrupted() || source.context->IsInterrupted()) {
			throw InterruptException();
		}
	};
	try {
		auto &transaction = Begin(source, catalog_name);
		Initialized(transaction);
		auto snapshot = transaction.GetSnapshot();
		plan->snapshot_id = snapshot.snapshot_id;
		plan->schema_version = snapshot.schema_version;
		plan->new_file_id = snapshot.next_file_id;
		auto &catalog = transaction.GetCatalog();
		auto entry = catalog.GetEntry(*source.context, CatalogType::TABLE_ENTRY, schema, table_name,
		                              OnEntryNotFound::THROW_EXCEPTION);
		auto &table = entry->Cast<DuckLakeTableEntry>();
		plan->table_uuid = table.GetTableUUID();
		plan->table_id = table.GetTableId().index;
		plan->before = Lookup(transaction, table, name);
		if (plan->before.revision != expected_revision) {
			throw TransactionException("Vortex rewrite publication revision conflict");
		}
		for (auto &segment : plan->before.segments) {
			if (segment.generation == selected) {
				plan->selected_files = segment.files;
			}
		}
		if (plan->selected_files.empty()) {
			throw InvalidInputException("Vortex rewrite selected generation is not ready");
		}
		plan->input_identity = RewriteInputs(*source.context, transaction, table, *plan);
		auto aliases = Query(transaction, "SELECT count(*) FROM {METADATA_CATALOG}." + string(CATALOG_TABLE) +
		                                      " WHERE table_uuid=" + Value(plan->table_uuid).ToSQLString());
		if (aliases->Fetch()->GetValue(0, 0).GetValue<idx_t>() != 1) {
			throw InvalidInputException("Vortex rewrite requires one named index for the table");
		}
		if (!transaction.GetConnection().TableInfo(catalog.MetadataDatabaseName(), catalog.MetadataSchemaName(),
		                                           "ducklake_vortex_write_budgets_v1")) {
			throw InvalidInputException("Vortex rewrite requires an installed write budget");
		}
		auto budget = Query(transaction, "SELECT count(*) FROM {METADATA_CATALOG}.ducklake_vortex_write_budgets_v1"
		                                 " WHERE table_uuid=" +
		                                     Value(plan->table_uuid).ToSQLString() +
		                                     " AND index_name=" + Value(name).ToSQLString());
		if (budget->Fetch()->GetValue(0, 0).GetValue<idx_t>() != 1) {
			throw InvalidInputException("Vortex rewrite requires an installed write budget for this index");
		}
		if (to_f) {
			plan->live_rows = VisibleRows(source, *plan, MIN_BUILD_ROWS);
			if (!plan->live_rows || plan->live_rows >= MIN_BUILD_ROWS) {
				throw InvalidInputException("Vortex F rewrite requires 1..64 visible rows");
			}
			DuckLakeVortexOptimizer::CheckRewriteToF(context, *plan);
		} else if (prune) {
			CheckPrune(source, *plan, true);
			for (auto &file : plan->selected_files) {
				if (std::find(plan->replaced.begin(), plan->replaced.end(), file.id) != plan->replaced.end()) {
					transaction.DropFile(table.GetTableId(), DataFileIndex(file.id), file.uri);
				}
			}
		}
		auto &fs = FileSystem::GetFileSystem(context);
		if (!to_f &&
		    (!fs.IsPathAbsolute(reference_path) || reference_path.back() == '/' || fs.FileExists(reference_path))) {
			throw InvalidInputException("Vortex rewrite requires a new absolute reference path");
		}
		auto root =
		    Canonical(to_f ? plan->before.root
		                   : reference_path.substr(
		                         0, reference_path.find_last_of('/') == 0 ? 1 : reference_path.find_last_of('/')));
		auto managed = fs.ExpandPath(catalog.DataPath());
		while (managed.size() > 1 && managed.back() == '/') {
			managed.pop_back();
		}
		managed = Canonical(managed);
		if (root == managed || StringUtil::StartsWith(root, managed + "/")) {
			throw InvalidInputException("Vortex rewrite reference must be outside managed data");
		}
		auto source_root = fs.ExpandPath(table.DataPath());
		while (source_root.size() > 1 && source_root.back() == '/') {
			source_root.pop_back();
		}
		plan->source_root = Canonical(source_root);
		if (plan->source_root != managed && !StringUtil::StartsWith(plan->source_root, managed + "/")) {
			throw InvalidInputException("Vortex rewrite requires a managed table data path");
		}
		plan->workspace = fs.JoinPath(root, ".build-" + plan->build_id);
		plan->private_path = fs.JoinPath(plan->workspace, "rewrite.vortex");
		plan->new_path = fs.JoinPath(plan->source_root, "ducklake-" + plan->build_id + ".vortex");
		for (auto &path : {plan->workspace, fs.JoinPath(root, generation), plan->new_path}) {
			if (fs.FileExists(path) || fs.DirectoryExists(path)) {
				throw InvalidInputException("Vortex rewrite owned namespace already exists");
			}
		}
		auto &allocation = Begin(owner, catalog_name);
		Query(allocation, "INSERT INTO {METADATA_CATALOG}." + string(BUILDS) + " VALUES(" +
		                      Value(plan->build_id).ToSQLString() + "," + Value(plan->table_uuid).ToSQLString() + "," +
		                      Value(name).ToSQLString() + "," + to_string(expected_revision) + "," +
		                      Value(root).ToSQLString() + "," + Value(generation).ToSQLString() + "," +
		                      Value(plan->workspace).ToSQLString() + "," + Value(reference_path).ToSQLString() +
		                      ",'allocated',false)");
		if (!prune) {
			Query(allocation, "INSERT INTO {METADATA_CATALOG}." + string(REWRITES) + " VALUES(" +
			                      Value(plan->build_id).ToSQLString() + "," + Value(plan->table_uuid).ToSQLString() +
			                      "," + Value(plan->source_root).ToSQLString() + "," +
			                      Value(plan->new_path).ToSQLString() + "," + to_string(plan->snapshot_id) + "," +
			                      to_string(plan->new_file_id) + ",false)");
		}
		owner.Commit();
		registered = true;
		plan->check("rewrite_allocated");
#ifndef _WIN32
		auto root_fd = OpenRoot(root, false);
		if (mkdirat(root_fd->fd, (".build-" + plan->build_id).c_str(), 0700)) {
			throw IOException("Cannot create Vortex rewrite workspace");
		}
		SyncDirectory(root_fd->fd);
#else
		throw NotImplementedException("Native Vortex rewrite requires POSIX paths");
#endif
		BuildState(owner, catalog_name, plan->build_id, "building");
		plan->check("rewrite_before_copy");
		if (!prune) {
			auto result = source.Query("SELECT * FROM ducklake_rewrite_data_files(" +
			                           Value(catalog_name).ToSQLString() + "," + Value(table_name).ToSQLString() +
			                           ",schema=>" + Value(schema).ToSQLString() + ",delete_threshold=>0)");
			if (result->HasError()) {
				result->ThrowError();
			}
			if (plan->copy_plans != 1 || plan->replaced.empty() || !plan->new_rows ||
			    !transaction.GetNewNameMaps().name_maps.empty()) {
				throw InvalidInputException("Vortex rewrite requires one nonempty compaction without new name maps");
			}
		}
		if (to_f) {
			set<idx_t> selected_ids, replaced_ids(plan->replaced.begin(), plan->replaced.end());
			for (auto &file : plan->selected_files) {
				selected_ids.insert(file.id);
			}
			if (plan->new_rows != plan->live_rows || replaced_ids != selected_ids) {
				throw InvalidInputException(
				    "Vortex F rewrite must replace the complete selected B and preserve visible rows");
			}
		}
		if (to_f) {
			auto admission = DuckLakeVortexOptimizer::AdmitWrite(transaction, *source.context);
		}
		plan->check("rewrite_written");
		DuckLakeVortexReadView view;
		unique_ptr<NativeBuildInput> build;
		std::unique_ptr<DuckLakeVortexPrepared, decltype(&ducklake_vortex_prepare_free)> prepared(
		    nullptr, ducklake_vortex_prepare_free);
		if (to_f) {
			plan->output_version = Hash(Read(fs, plan->private_path, MAX_ARTIFACTS, false));
			plan->bind = [&](idx_t) {
				vector<DuckLakeVortexIndexSegment> segments;
				for (auto &segment : plan->before.segments) {
					if (segment.generation != selected) {
						segments.push_back(segment);
					}
				}
				plan->candidate = plan->before;
				plan->candidate.revision++;
				Compose(plan->candidate, std::move(segments), plan->table_uuid, plan->build_id);
				plan->check("rewrite_f_bound");
			};
		} else {
			view.managed_data_path = managed;
			ViewJSON json;
			auto admission = json.Object(), capture = json.Object(), business = json.Object(), inventory = json.Array();
			json.Number(admission, "format_version", 1);
			json.Add(admission, "capture", capture);
			json.Number(capture, "format_version", 1);
			json.Text(capture, "table_uuid", plan->table_uuid);
			json.Number(capture, "snapshot_id", plan->snapshot_id + 1);
			json.Number(capture, "schema_version", plan->schema_version);
			json.Number(capture, "field_id", plan->before.field_id);
			json.Number(capture, "dimension", plan->before.dimension);
			for (auto &field : {"id", "category", "payload"}) {
				json.Number(business, field, table.GetFieldId(vector<string> {field}).GetFieldIndex().index);
			}
			json.Add(capture, "business_fields", business);
			json.Add(capture, "files", inventory);
			json.Add(capture, "visible", json.Array());
			vector<DuckLakeVortexIndexFile> inputs;
			for (auto &file : plan->selected_files) {
				if (std::find(plan->replaced.begin(), plan->replaced.end(), file.id) == plan->replaced.end()) {
					inputs.push_back(file);
				}
			}
			if (!prune) {
				inputs.push_back({plan->new_file_id, plan->new_path, "", plan->new_rows});
			}
			idx_t remaining = MAX_ARTIFACTS;
			for (auto &file : inputs) {
				const bool output = !prune && file.id == plan->new_file_id;
				auto bytes = Read(fs, output ? plan->private_path : file.uri, remaining, false);
				remaining -= bytes.size();
				if (!output && Hash(bytes) != file.version) {
					throw InvalidInputException("Vortex rewrite retained source identity changed");
				}
				auto item = json.Object();
				json.Number(item, "id", file.id);
				json.Text(item, "uri", file.uri);
				json.Text(item, "version", Hash(bytes));
				json.Number(item, "row_count", file.row_count);
				yyjson_mut_arr_append(inventory, item);
				view.sources.push_back({file.id, std::move(bytes)});
			}
			view.admission_json = json.Finish(admission);
			if (prune) {
				BuildNative(context, view, reference_path, generation, plan->workspace, plan->check);
			} else {
				build = make_uniq<NativeBuildInput>(context, view, reference_path, generation, plan->workspace,
				                                    plan->check);
				prepared.reset(ducklake_vortex_prepare(&build->input));
				build->Check(ducklake_vortex_prepare_error(prepared.get()));
			}
			auto provisional_id = plan->new_file_id;
			plan->bind = [&, provisional_id](idx_t final_id) {
				if (!prune) {
					DuckLakeVortexBindInput input {prepared.get(), provisional_id, final_id,
					                               DuckLakeVortexControl::Check, &build->control};
					std::unique_ptr<char, decltype(&ducklake_vortex_build_error_free)> error(
					    ducklake_vortex_bind(&input), ducklake_vortex_build_error_free);
					build->Check(error.get());
				}
				auto replacement = ParseReference(Read(fs, reference_path, MAX_MANIFEST, true), plan->table_uuid,
				                                  plan->before.field_id);
				replacement.root = root;
				vector<DuckLakeVortexIndexSegment> segments;
				for (auto &segment : plan->before.segments) {
					if (segment.generation != selected) {
						segments.push_back(segment);
					}
				}
				segments.push_back({root, replacement.generation, replacement.reference, replacement.files});
				Compose(replacement, std::move(segments), plan->table_uuid, prune ? plan->build_id : "");
				replacement.name = name;
				replacement.revision = expected_revision + 1;
				plan->candidate = std::move(replacement);
			};
		}
		plan->check("rewrite_candidate_ready");
#ifndef _WIN32
		if (!prune) {
			auto work_fd = OpenRoot(plan->workspace, false), source_fd = OpenRoot(plan->source_root, false);
			auto fd = openat(work_fd->fd, "rewrite.vortex", O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
			if (fd < 0) {
				throw IOException("Cannot open private Vortex rewrite source");
			}
			Descriptor private_file(fd);
			struct stat info;
			if (fstat(fd, &info) || !S_ISREG(info.st_mode) || info.st_size < 0 ||
			    idx_t(info.st_size) != plan->new_bytes || fsync(fd)) {
				throw IOException("Cannot validate and sync private Vortex rewrite source");
			}
			if (linkat(work_fd->fd, "rewrite.vortex", source_fd->fd, ("ducklake-" + plan->build_id + ".vortex").c_str(),
			           0)) {
				if (errno == EEXIST) {
					// A failed exclusive link does not confer ownership of an existing destination.
					auto &preserve = Begin(owner, catalog_name);
					Query(preserve, "UPDATE {METADATA_CATALOG}." + string(REWRITES) +
					                    " SET keep_file=true WHERE build_id=" + Value(plan->build_id).ToSQLString());
					owner.Commit();
					throw IOException("Vortex rewrite destination already exists; existing file retained");
				}
				throw IOException("Cannot link Vortex rewrite output; roots must share a filesystem");
			}
			SyncDirectory(source_fd->fd);
			Unlink(work_fd->fd, "rewrite.vortex", false);
		}
#endif
		plan->check("rewrite_source_linked");
		transaction.GetConnection().Rollback();
		transaction.GetConnection().BeginTransaction();
		source.Commit();
		committed = true;
		plan->check("rewrite_after_commit");
		return plan->candidate;
	} catch (...) {
		auto failure = std::current_exception();
		Rollback(source);
		Rollback(owner);
		if (registered) {
			ReconcileBuild(owner, catalog_name, plan->build_id);
		}
		if (committed) {
			throw IOException("Vortex rewrite committed but acknowledgement failed; inspect catalog before retry");
		}
		std::rethrow_exception(failure);
	}
}

DuckLakeVortexIndex DuckLakeVortexIndex::Publish(Connection &owner, const string &catalog_name, const string &schema,
                                                 const string &table_name, const string &name,
                                                 const string &reference_path, idx_t expected_revision,
                                                 const std::function<void(const string &)> &observe,
                                                 DuckLakeVortexIndexOperation operation,
                                                 const vector<string> &merge_generations) {
	if (owner.context->transaction.HasActiveTransaction()) {
		throw InvalidInputException("Vortex index publication requires an idle dedicated connection");
	}
	auto publication = PublicationLock().GetExclusiveLock();
	bool attempted = false;
	bool committed = false;
	try {
		Name(name);
		if (operation != DuckLakeVortexIndexOperation::MERGE && !merge_generations.empty()) {
			throw InvalidInputException("Only a Vortex merge can select generations");
		}
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
		if (operation != DuckLakeVortexIndexOperation::REPLACE) {
			if (!revision) {
				throw InvalidInputException("Segment append requires a published index");
			}
			auto previous_index = Lookup(transaction, table, name);
			if (previous_index.dimension != candidate.dimension || previous_index.field_id != candidate.field_id) {
				throw InvalidInputException("Vortex segment vector specification mismatch");
			}
			if (operation == DuckLakeVortexIndexOperation::MERGE) {
				auto selected = previous_index.SelectedFiles(merge_generations);
				if (selected.size() != candidate.files.size()) {
					throw InvalidInputException("Vortex merge must cover exactly the selected segments");
				}
				candidate.ValidateCoverage(selected);
				for (auto &segment : previous_index.segments) {
					if (std::find(merge_generations.begin(), merge_generations.end(), segment.generation) ==
					    merge_generations.end()) {
						segments.push_back(std::move(segment));
					}
				}
			} else {
				segments = std::move(previous_index.segments);
			}
		}
		segments.push_back({candidate.root, candidate.generation, candidate.reference, candidate.files});
		Compose(candidate, std::move(segments), table.GetTableUUID());
		idx_t artifact_budget = MAX_ARTIFACTS;
		for (auto &segment : candidate.segments) {
			auto part = ParseReference(segment.reference, table.GetTableUUID(), field.GetFieldIndex().index);
			part.root = segment.root;
			auto before = artifact_budget;
			auto manifest = VerifyGeneration(fs, part, artifact_budget);
			RegisterGeneration(transaction, part, manifest, before - artifact_budget);
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
		UpdateGenerationRefs(transaction, table.GetTableUUID(), candidate);
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
