#include "storage/ducklake_vortex_optimizer.hpp"
#include "storage/ducklake_vortex_index_catalog.hpp"
#include "common/ducklake_vortex_json.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_delete_filter.hpp"
#include "storage/ducklake_vortex_read_view.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_metadata_manager.hpp"
#include "storage/ducklake_schema_entry.hpp"
#include "storage/ducklake_vortex.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/storage/object_cache.hpp"
#include "duckdb/transaction/transaction_context.hpp"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstdlib>
#include <map>
#include <memory>
#include <set>
#include <thread>

namespace duckdb {
namespace {
using VortexCatalog = DuckLakeVortexIndex;
using Config = DuckLakeVortexOptimizerConfig;
using namespace duckdb_yyjson;
using namespace vortex_detail;
using Key = std::pair<string, string>;
const char *POLICIES = "ducklake_vortex_optimizer_v2";
const char *REWRITE_RECEIPTS = "ducklake_vortex_optimizer_rewrite_receipts_v1";
const char *MAINTENANCE = "ducklake_vortex_maintenance_v1";
const char *WRITE_BUDGETS = "ducklake_vortex_write_budgets_v1";
const char *BUDGET_COLUMNS = "max_inline_rows,max_inline_bytes,max_unindexed_rows,max_unindexed_bytes";
const idx_t SOURCE_LIMIT = 64 * 1024 * 1024;
const idx_t INLINE_LIMIT = 4 * 1024 * 1024;

struct Policy {
	Config config;
	string uuid;
	idx_t field_id = 0;
	bool enabled = false;
	string state = "idle", run, generation, reference, error;
	string kind = "none";
	vector<string> selected, merge_candidates;
	idx_t expected = 0, published = 0, t_rows = 0, t_bytes = 0, f_rows = 0, f_bytes = 0, runs = 0;
	idx_t source_rows = 0, source_bytes = 0;
	idx_t pending_rows = 0, pending_bytes = 0;
	bool compaction_enabled = false;
	idx_t min_deleted_rows = 1, min_deleted_percent = 50;
	string rewrite_candidate, rewrite_reason = "disabled";
	idx_t rewrite_deleted = 0, rewrite_remaining = 0;
	Key Id() const {
		return {uuid, config.index};
	}
};

void Validate(const Policy &p);

struct Metadata {
	Connection connection;
	DuckLakeTransaction &transaction;
	static DuckLakeTransaction &Begin(Connection &connection, const string &catalog, bool require_index,
	                                  optional_ptr<const DuckLakeVortexCatalogIdentity> identity) {
		try {
			auto &transaction = VortexCatalog::Begin(connection, catalog, identity);
			if (require_index) {
				VortexCatalog::Initialized(transaction);
			}
			return transaction;
		} catch (...) {
			VortexCatalog::Rollback(connection);
			throw;
		}
	}
	Metadata(DatabaseInstance &db, const string &catalog, bool require_index = true,
	         optional_ptr<const DuckLakeVortexCatalogIdentity> identity = nullptr)
	    : connection(db), transaction(Begin(connection, catalog, require_index, identity)) {
	}
	unique_ptr<QueryResult> Query(const string &sql) {
		return VortexCatalog::Query(transaction, sql);
	}
	void Commit() {
		connection.Commit();
	}
};

string Quote(const string &value) {
	return Value(value).ToSQLString();
}
string Where(const Policy &p) {
	return " WHERE table_uuid=" + Quote(p.uuid) + " AND index_name=" + Quote(p.config.index);
}

void Initialized(Metadata &meta) {
	auto &catalog = meta.transaction.GetCatalog();
	if (!meta.transaction.GetConnection().TableInfo(catalog.MetadataDatabaseName(), catalog.MetadataSchemaName(),
	                                                POLICIES)) {
		throw InvalidInputException("Vortex optimizer catalog is not initialized");
	}
}

using MaintenanceId = std::pair<idx_t, idx_t>;

struct MaintenanceCatalog {
	string name;
	DuckLakeVortexCatalogIdentity identity;
	MaintenanceId Id() const {
		return {identity.lake_oid, identity.metadata_oid};
	}
};

MaintenanceCatalog ResolveMaintenanceCatalog(ClientContext &context, const string &name, bool write) {
	return {name, VortexCatalog::ResolveCatalog(context, name, write, "Vortex maintenance")};
}

struct Maintenance {
	string state = "idle", run, error;
	bool enabled = true;
	idx_t max_objects = 0, max_bytes = 0, runs = 0, recovered = 0, builds = 0, generations = 0, bytes = 0;
	idx_t remaining_builds = 0, remaining_generations = 0;
};

const char *MAINTENANCE_COLUMNS =
    "enabled,state,run_id,max_objects,max_bytes,run_count,reconciled_builds,collected_builds,"
    "collected_generations,collected_bytes,remaining_builds,remaining_generations,error";

Maintenance ReadMaintenance(DataChunk &chunk) {
	Maintenance m;
	m.enabled = chunk.GetValue(0, 0).GetValue<bool>();
	m.state = StringValue::Get(chunk.GetValue(1, 0));
	m.run = StringValue::Get(chunk.GetValue(2, 0));
	idx_t *numbers[] = {&m.max_objects, &m.max_bytes,        &m.runs,
	                    &m.recovered,   &m.builds,           &m.generations,
	                    &m.bytes,       &m.remaining_builds, &m.remaining_generations};
	for (idx_t i = 0; i < 9; i++) {
		*numbers[i] = chunk.GetValue(i + 3, 0).GetValue<idx_t>();
	}
	m.error = StringValue::Get(chunk.GetValue(12, 0));
	if (!m.max_objects || m.max_objects > 16 || !m.max_bytes || m.max_bytes > SOURCE_LIMIT) {
		throw InvalidInputException("Invalid Vortex maintenance count or byte budget");
	}
	return m;
}

Maintenance LoadMaintenance(Metadata &meta) {
	Initialized(meta);
	auto result = meta.Query("SELECT " + string(MAINTENANCE_COLUMNS) + " FROM {METADATA_CATALOG}." + MAINTENANCE);
	auto chunk = result->Fetch();
	if (!chunk || chunk->size() != 1) {
		throw InvalidInputException("No Vortex maintenance policy in this catalog");
	}
	return ReadMaintenance(*chunk);
}

void SaveMaintenance(Metadata &meta, const Maintenance &m, const string &previous_run) {
	auto result = meta.Query(
	    "UPDATE {METADATA_CATALOG}." + string(MAINTENANCE) + " SET enabled=" + string(m.enabled ? "true" : "false") +
	    ",state=" + Quote(m.state) + ",run_id=" + Quote(m.run) + ",run_count=" + to_string(m.runs) +
	    ",reconciled_builds=" + to_string(m.recovered) + ",collected_builds=" + to_string(m.builds) +
	    ",collected_generations=" + to_string(m.generations) + ",collected_bytes=" + to_string(m.bytes) +
	    ",remaining_builds=" + to_string(m.remaining_builds) +
	    ",remaining_generations=" + to_string(m.remaining_generations) + ",error=" + Quote(m.error) +
	    " WHERE run_id=" + Quote(previous_run) + " RETURNING state");
	auto chunk = result->Fetch();
	if (!chunk || chunk->size() != 1) {
		throw TransactionException("Vortex maintenance ownership changed");
	}
	meta.Commit();
}

bool MaintenanceBacklog(Metadata &meta, Maintenance &m) {
	auto result = meta.Query("SELECT (SELECT count(*) FROM "
	                         "{METADATA_CATALOG}.ducklake_vortex_builds_v1),"
	                         "count(*),count(*) FILTER (WHERE " +
	                         VortexCatalog::CollectibleGenerationPredicate(m.max_bytes) +
	                         ") FROM {METADATA_CATALOG}.ducklake_vortex_generations_v1 g"
	                         " WHERE g.state IN ('retired','deleting')");
	auto chunk = result->Fetch();
	m.remaining_builds = chunk->GetValue(0, 0).GetValue<idx_t>();
	m.remaining_generations = chunk->GetValue(1, 0).GetValue<idx_t>();
	return m.remaining_builds || chunk->GetValue(2, 0).GetValue<idx_t>() != 0;
}

void SetMaintenanceReadyState(Maintenance &m, bool actionable) {
	const bool blocked = !actionable && m.remaining_generations;
	m.state = blocked ? "blocked" : "idle";
	m.error = blocked ? "Retired Vortex generations are referenced or exceed the "
	                    "maintenance byte budget"
	                  : "";
}

string Encode(const Policy &p) {
	ViewJSON json;
	auto value = json.Object();
	json.Number(value, "format_version", 2);
	json.Text(value, "schema", p.config.schema);
	json.Text(value, "table", p.config.table);
	json.Text(value, "root", p.config.root);
	json.Number(value, "dimension", p.config.dimension);
	json.Number(value, "field_id", p.field_id);
	json.Number(value, "flush_rows", p.config.flush_rows);
	json.Number(value, "flush_bytes", p.config.flush_bytes);
	json.Number(value, "index_rows", p.config.index_rows);
	json.Number(value, "index_bytes", p.config.index_bytes);
	json.Add(value, "compaction_enabled", yyjson_mut_bool(json.doc.get(), p.compaction_enabled));
	json.Number(value, "min_deleted_rows", p.min_deleted_rows);
	json.Number(value, "min_deleted_percent", p.min_deleted_percent);
	return json.Finish(value);
}

void Decode(Policy &p, const string &bytes) {
	std::unique_ptr<yyjson_doc, decltype(&yyjson_doc_free)> doc(yyjson_read(bytes.data(), bytes.size(), 0),
	                                                            yyjson_doc_free);
	auto root = doc ? yyjson_doc_get_root(doc.get()) : nullptr;
	if (!root || !yyjson_is_obj(root) || yyjson_obj_size(root) != 13) {
		throw InvalidInputException("Invalid Vortex optimizer policy shape");
	}
	auto number = [&](const char *name) -> idx_t {
		auto value = yyjson_obj_get(root, name);
		if (!value || !yyjson_is_uint(value)) {
			throw InvalidInputException("Invalid optimizer policy number");
		}
		return yyjson_get_uint(value);
	};
	auto text = [&](const char *name) {
		auto value = yyjson_obj_get(root, name);
		if (!value || !yyjson_is_str(value)) {
			throw InvalidInputException("Invalid optimizer policy string");
		}
		return string(yyjson_get_str(value), yyjson_get_len(value));
	};
	if (number("format_version") != 2 || number("dimension") > 4096) {
		throw InvalidInputException("Unsupported optimizer policy version or dimension");
	}
	p.config.schema = text("schema");
	p.config.table = text("table");
	p.config.root = text("root");
	p.config.dimension = uint32_t(number("dimension"));
	p.field_id = number("field_id");
	p.config.flush_rows = number("flush_rows");
	p.config.flush_bytes = number("flush_bytes");
	p.config.index_rows = number("index_rows");
	p.config.index_bytes = number("index_bytes");
	auto enabled = yyjson_obj_get(root, "compaction_enabled");
	if (!enabled || !yyjson_is_bool(enabled)) {
		throw InvalidInputException("Invalid compaction enable flag");
	}
	p.compaction_enabled = yyjson_get_bool(enabled);
	p.min_deleted_rows = number("min_deleted_rows");
	p.min_deleted_percent = number("min_deleted_percent");
}

const char *COLUMNS =
    "table_uuid,index_name,config,enabled,state,run_id,expected_revision,candidate_generation,"
    "published_revision,reference_path,error,inline_rows,inline_bytes,unindexed_rows,unindexed_bytes,run_count,"
    "task_kind,selected_generations,rewrite_candidate,rewrite_deleted,rewrite_remaining,rewrite_reason";

string Selection(const vector<string> &generations) {
	ViewJSON json;
	auto value = json.Array();
	for (auto &generation : generations) {
		yyjson_mut_arr_append(value, yyjson_mut_strncpy(json.doc.get(), generation.data(), generation.size()));
	}
	return json.Finish(value);
}

vector<string> ReadSelection(const string &bytes) {
	std::unique_ptr<yyjson_doc, decltype(&yyjson_doc_free)> doc(yyjson_read(bytes.data(), bytes.size(), 0),
	                                                            yyjson_doc_free);
	auto root = doc ? yyjson_doc_get_root(doc.get()) : nullptr;
	if (!root || !yyjson_is_arr(root) || yyjson_arr_size(root) > 2) {
		throw InvalidInputException("Invalid optimizer generation selection");
	}
	vector<string> result;
	for (idx_t i = 0; i < yyjson_arr_size(root); i++) {
		auto value = yyjson_arr_get(root, i);
		if (!yyjson_is_str(value) || !yyjson_get_len(value)) {
			throw InvalidInputException("Invalid optimizer selected generation");
		}
		result.emplace_back(yyjson_get_str(value), yyjson_get_len(value));
	}
	return result;
}

Policy Read(DataChunk &chunk, idx_t row, const string &catalog) {
	Policy p;
	auto text = [&](idx_t i) {
		return StringValue::Get(chunk.GetValue(i, row));
	};
	auto number = [&](idx_t i) {
		return chunk.GetValue(i, row).GetValue<idx_t>();
	};
	p.uuid = text(0);
	p.config.catalog = catalog;
	p.config.index = text(1);
	Decode(p, text(2));
	p.enabled = chunk.GetValue(3, row).GetValue<bool>();
	p.state = text(4);
	p.run = text(5);
	p.expected = number(6);
	p.generation = text(7);
	p.published = number(8);
	p.reference = text(9);
	p.error = text(10);
	p.t_rows = number(11);
	p.t_bytes = number(12);
	p.f_rows = number(13);
	p.f_bytes = number(14);
	p.runs = number(15);
	p.kind = text(16);
	p.selected = ReadSelection(text(17));
	p.rewrite_candidate = text(18);
	p.rewrite_deleted = number(19);
	p.rewrite_remaining = number(20);
	p.rewrite_reason = text(21);
	Validate(p);
	return p;
}

Policy Load(Metadata &meta, const Policy &key) {
	Initialized(meta);
	auto result = meta.Query("SELECT " + string(COLUMNS) + " FROM {METADATA_CATALOG}." + POLICIES + Where(key));
	auto chunk = result->Fetch();
	if (!chunk || chunk->size() != 1) {
		throw InvalidInputException("No Vortex optimizer policy with this table identity");
	}
	return Read(*chunk, 0, key.config.catalog);
}

void Save(Metadata &meta, const Policy &p, const string &previous_run) {
	auto result = meta.Query(
	    "UPDATE {METADATA_CATALOG}." + string(POLICIES) + " SET config=" + Quote(Encode(p)) +
	    ",enabled=" + string(p.enabled ? "true" : "false") + ",state=" + Quote(p.state) + ",run_id=" + Quote(p.run) +
	    ",expected_revision=" + to_string(p.expected) + ",candidate_generation=" + Quote(p.generation) +
	    ",published_revision=" + to_string(p.published) + ",reference_path=" + Quote(p.reference) +
	    ",error=" + Quote(p.error) + ",inline_rows=" + to_string(p.t_rows) + ",inline_bytes=" + to_string(p.t_bytes) +
	    ",unindexed_rows=" + to_string(p.f_rows) + ",unindexed_bytes=" + to_string(p.f_bytes) + ",run_count=" +
	    to_string(p.runs) + ",task_kind=" + Quote(p.kind) + ",selected_generations=" + Quote(Selection(p.selected)) +
	    ",rewrite_candidate=" + Quote(p.rewrite_candidate) + ",rewrite_deleted=" + to_string(p.rewrite_deleted) +
	    ",rewrite_remaining=" + to_string(p.rewrite_remaining) + ",rewrite_reason=" + Quote(p.rewrite_reason) +
	    Where(p) + " AND run_id=" + Quote(previous_run) + " RETURNING state");
	auto chunk = result->Fetch();
	if (!chunk || chunk->size() != 1) {
		throw TransactionException("Optimizer task ownership changed");
	}
	meta.Commit();
}

DuckLakeTableEntry &Table(Metadata &meta, Policy &p) {
	auto &catalog = meta.transaction.GetCatalog();
	optional_ptr<CatalogEntry> entry;
	if (p.uuid.empty()) {
		entry = catalog.GetEntry(*meta.connection.context, CatalogType::TABLE_ENTRY, p.config.schema, p.config.table,
		                         OnEntryNotFound::THROW_EXCEPTION);
	} else {
		auto snapshot = meta.transaction.GetSnapshot();
		auto identity = meta.transaction.Query(
		    snapshot, "SELECT table_id FROM {METADATA_CATALOG}.ducklake_table WHERE table_uuid=" + Quote(p.uuid) +
		                  " AND begin_snapshot<={SNAPSHOT_ID} AND "
		                  "(end_snapshot IS NULL OR end_snapshot>{SNAPSHOT_ID})");
		if (identity->HasError()) {
			identity->ThrowError();
		}
		auto row = identity->Fetch();
		if (!row || row->size() != 1) {
			throw InvalidInputException("Missing optimizer table identity");
		}
		entry = catalog.GetEntryById(meta.transaction, snapshot, TableIndex(row->GetValue(0, 0).GetValue<idx_t>()));
		if (!entry) {
			throw InvalidInputException("Missing optimizer table identity");
		}
	}
	auto &table = entry->Cast<DuckLakeTableEntry>();
	if ((!p.uuid.empty() && p.uuid != table.GetTableUUID()) ||
	    (p.field_id && p.field_id != table.GetFieldId(vector<string> {"embedding"}).GetFieldIndex().index)) {
		throw InvalidInputException("Optimizer table or vector field identity changed");
	}
	for (auto &name : {"id", "embedding", "category", "payload"}) {
		auto expected = string(name) == "id"          ? LogicalType::BIGINT
		                : string(name) == "embedding" ? LogicalType::LIST(LogicalType::FLOAT)
		                                              : LogicalType::VARCHAR;
		if (table.GetFieldId(vector<string> {name}).Type() != expected) {
			throw InvalidInputException("Optimizer business/vector field type changed");
		}
	}
	p.config.schema = table.schema.name;
	p.config.table = table.name;
	return table;
}

void FlushConfiguration(ClientContext &context, DuckLakeTableEntry &table) {
	auto &catalog = table.ParentCatalog().Cast<DuckLakeCatalog>();
	auto schema = table.schema.Cast<DuckLakeSchemaEntry>().GetSchemaId();
	if (DuckLakeVortex::GetFormat(context, table) != DataFileFormat::VORTEX ||
	    catalog.GetConfigOption<string>("auto_compact", schema, table.GetTableId(), "true") != "true") {
		throw InvalidInputException("Optimizer requires Vortex output and auto_compact=true");
	}
}

idx_t ReadyRevision(Metadata &meta, const Policy &p) {
	auto result = meta.Query("SELECT revision FROM {METADATA_CATALOG}.ducklake_vortex_indexes_v1" + Where(p));
	auto chunk = result->Fetch();
	return chunk && chunk->size() ? chunk->GetValue(0, 0).GetValue<idx_t>() : 0;
}

vector<string> InlineSourceColumns(Metadata &meta, DuckLakeTableEntry &table, const DuckLakeInlinedTableInfo &source) {
	auto &catalog = meta.transaction.GetCatalog();
	DuckLakeSnapshot source_snapshot(
	    catalog.GetBeginSnapshotForSchemaVersion(table.GetTableId(), source.schema_version, meta.transaction),
	    source.schema_version, 0, 0);
	auto source_entry = catalog.GetEntryById(meta.transaction, source_snapshot, table.GetTableId());
	if (!source_entry) {
		throw InvalidInputException("Missing optimizer inline source schema");
	}
	auto &source_schema = source_entry->Cast<DuckLakeTableEntry>();
	vector<string> columns;
	for (auto &name : {"embedding", "category", "payload"}) {
		auto &field = table.GetFieldId(vector<string> {name});
		auto source_field = source_schema.GetFieldId(field.GetFieldIndex());
		if (!source_field || source_field->Type() != field.Type()) {
			throw InvalidInputException("Optimizer inline source field mismatch: %llu", field.GetFieldIndex().index);
		}
		columns.push_back(KeywordHelper::WriteOptionallyQuoted(source_field->Name()));
	}
	return columns;
}

void SelectCompaction(Metadata &meta, Policy &p, DuckLakeTableEntry &table, const DuckLakeVortexIndex &index,
                      const vector<DuckLakeFileListExtendedEntry> &active_files) {
	auto aliases = meta.Query("SELECT count(*) FROM {METADATA_CATALOG}.ducklake_vortex_indexes_v1 WHERE table_uuid=" +
	                          Quote(p.uuid));
	if (aliases->Fetch()->GetValue(0, 0).GetValue<idx_t>() != 1) {
		throw InvalidInputException("Background Vortex compaction requires one named index");
	}
	auto budget = meta.Query("SELECT count(*) FROM {METADATA_CATALOG}." + string(WRITE_BUDGETS) + Where(p));
	if (budget->Fetch()->GetValue(0, 0).GetValue<idx_t>() != 1) {
		throw InvalidInputException("Background Vortex compaction requires an installed write budget for this index");
	}
	vector<string> ids;
	for (auto &file : index.files) {
		ids.push_back(to_string(file.id));
	}
	auto list = StringUtil::Join(ids, ",");
	struct FileState {
		idx_t rows, deleted, schema;
		bool partitioned;
	};
	map<idx_t, FileState> files;
	auto snapshot_query = [&](const string &query) {
		auto result = meta.transaction.Query(meta.transaction.GetSnapshot(), query);
		if (result->HasError()) {
			result->ThrowError();
		}
		return result;
	};
	auto state = snapshot_query(
	    "SELECT d.data_file_id,d.record_count,(SELECT max(s.schema_version) FROM "
	    "{METADATA_CATALOG}.ducklake_schema_versions s "
	    "WHERE s.table_id=d.table_id AND "
	    "s.begin_snapshot<=d.begin_snapshot),d.partition_id,coalesce((SELECT del.delete_count "
	    "FROM {METADATA_CATALOG}.ducklake_delete_file del WHERE del.data_file_id=d.data_file_id AND "
	    "del.begin_snapshot<={SNAPSHOT_ID} AND (del.end_snapshot IS NULL OR del.end_snapshot>{SNAPSHOT_ID}) "
	    "ORDER BY del.begin_snapshot DESC,del.delete_file_id DESC LIMIT 1),0) "
	    "FROM {METADATA_CATALOG}.ducklake_data_file d WHERE d.data_file_id IN (" +
	    list +
	    ") "
	    "AND d.begin_snapshot<={SNAPSHOT_ID} AND (d.end_snapshot IS NULL OR d.end_snapshot>{SNAPSHOT_ID})");
	vector<string> masked_ids;
	while (auto chunk = state->Fetch()) {
		for (idx_t row = 0; row < chunk->size(); row++) {
			if (chunk->GetValue(2, row).IsNull()) {
				throw InvalidInputException("Compaction source schema version is missing");
			}
			auto id = chunk->GetValue(0, row).GetValue<idx_t>();
			auto deleted = chunk->GetValue(4, row).GetValue<idx_t>();
			files.emplace(id, FileState {chunk->GetValue(1, row).GetValue<idx_t>(), deleted,
			                             chunk->GetValue(2, row).GetValue<idx_t>(), !chunk->GetValue(3, row).IsNull()});
			if (deleted) {
				masked_ids.push_back(to_string(id));
			}
		}
	}
	auto inlined = meta.transaction.GetMetadataManager().GetInlinedDeletionTableName(table.GetTableId(),
	                                                                                 meta.transaction.GetSnapshot());
	if (!inlined.empty()) {
		// Materialize inline positions only for files whose external mask can overlap.
		auto mixed = masked_ids.empty() ? "false" : "file_id IN (" + StringUtil::Join(masked_ids, ",") + ")";
		auto counts =
		    snapshot_query("SELECT file_id,count(DISTINCT row_id),max(row_id),list(DISTINCT row_id) FILTER (WHERE " +
		                   mixed + ") FROM {METADATA_CATALOG}." + inlined + " WHERE file_id IN (" + list +
		                   ") AND begin_snapshot<={SNAPSHOT_ID} GROUP BY file_id");
		idx_t mask_bytes = SOURCE_LIMIT;
		while (auto chunk = counts->Fetch()) {
			for (idx_t row = 0; row < chunk->size(); row++) {
				auto id = chunk->GetValue(0, row).GetValue<idx_t>();
				auto file = files.find(id);
				if (file == files.end()) {
					throw InvalidInputException("Compaction deletion source is no longer active");
				}
				auto &metrics = file->second;
				if (chunk->GetValue(2, row).GetValue<idx_t>() >= metrics.rows || metrics.deleted > metrics.rows) {
					throw InvalidInputException("Compaction deletion position or count exceeds source rows");
				}
				auto inline_count = chunk->GetValue(1, row).GetValue<idx_t>();
				auto inline_positions = chunk->GetValue(3, row);
				if (!inline_positions.IsNull()) {
					auto source = std::find_if(
					    active_files.begin(), active_files.end(),
					    [&](const DuckLakeFileListExtendedEntry &entry) { return entry.file_id.index == id; });
					if (source == active_files.end() || source->delete_file.path.empty() ||
					    source->delete_file.file_size_bytes > mask_bytes) {
						throw InvalidInputException("Compaction delete file missing or exceeds source budget");
					}
					mask_bytes -= source->delete_file.file_size_bytes;
					auto external = DuckLakeDeleteFilter::ScanDeleteFile(*meta.connection.context, source->delete_file,
					                                                     optional_idx(), optional_idx(), metrics.rows);
					if (external.deleted_rows.size() != metrics.deleted) {
						throw InvalidInputException("Compaction delete file row count changed");
					}
					for (auto &position : ListValue::GetChildren(inline_positions)) {
						if (std::binary_search(external.deleted_rows.begin(), external.deleted_rows.end(),
						                       position.GetValue<idx_t>())) {
							inline_count--;
						}
					}
				}
				metrics.deleted += inline_count;
			}
		}
	}
	string eligible, blocked, reason;
	idx_t eligible_deleted = 0, eligible_remaining = 0, blocked_deleted = 0, blocked_remaining = 0;
	for (auto &segment : index.segments) {
		idx_t rows = 0, deleted = 0, output = 0;
		set<idx_t> groups;
		bool partitioned = false;
		for (auto &file : segment.files) {
			auto entry = files.find(file.id);
			if (entry == files.end() || entry->second.rows != file.row_count ||
			    entry->second.deleted > file.row_count) {
				throw InvalidInputException("Compaction source identity or deletion count changed");
			}
			auto &metrics = entry->second;
			rows += metrics.rows;
			deleted += metrics.deleted;
			if (metrics.deleted) {
				groups.insert(metrics.schema);
				partitioned = partitioned || metrics.partitioned;
				output += metrics.rows - metrics.deleted;
			}
		}
		if (!deleted) {
			continue;
		}
		auto remaining = rows - deleted;
		auto why = deleted < p.min_deleted_rows || 100 * deleted < p.min_deleted_percent * rows ? "below_threshold"
		           : !output                                                                    ? "empty_rewrite_output"
		           : remaining < 65                                                             ? "below_native_minimum"
		           : partitioned        ? "unsupported_partition"
		           : groups.size() != 1 ? "multiple_schema_groups"
		                                : "eligible";
		if (string(why) == "eligible") {
			if (eligible.empty() || deleted > eligible_deleted ||
			    (deleted == eligible_deleted && segment.generation < eligible)) {
				eligible = segment.generation;
				eligible_deleted = deleted;
				eligible_remaining = remaining;
			}
		} else if (blocked.empty() || deleted > blocked_deleted ||
		           (deleted == blocked_deleted && segment.generation < blocked)) {
			blocked = segment.generation;
			blocked_deleted = deleted;
			blocked_remaining = remaining;
			reason = why;
		}
	}
	p.rewrite_candidate = eligible.empty() ? blocked : eligible;
	p.rewrite_deleted = eligible.empty() ? blocked_deleted : eligible_deleted;
	p.rewrite_remaining = eligible.empty() ? blocked_remaining : eligible_remaining;
	p.rewrite_reason = !eligible.empty() ? "eligible" : blocked.empty() ? "no_deletions" : reason;
}

void Probe(Metadata &meta, Policy &p, bool select_compaction = false,
           optional_ptr<const LocalTableDataChanges> changes = nullptr,
           optional_ptr<const vector<FlushedInlinedTableInfo>> flushed = nullptr,
           optional_ptr<const unordered_map<string, DataFileIndex>> dropped = nullptr,
           optional_ptr<const DuckLakeVortexRewriteState> rewrite = nullptr) {
	auto &table = Table(meta, p);
	FlushConfiguration(*meta.connection.context, table);
	auto snapshot = meta.transaction.GetSnapshot();
	auto &manager = meta.transaction.GetMetadataManager();
	p.t_rows = p.t_bytes = p.f_rows = p.f_bytes = 0;
	p.source_rows = p.source_bytes = 0;
	for (auto &source : table.GetInlinedDataTables()) {
		auto columns = InlineSourceColumns(meta, table, source);
		string eligible = "true";
		if (changes) {
			auto deleted = changes->new_inlined_data_deletes.find(source.table_name);
			if (deleted != changes->new_inlined_data_deletes.end() && !deleted->second->rows.empty()) {
				vector<string> ids;
				for (auto id : deleted->second->rows) {
					ids.push_back(to_string(id));
				}
				eligible += " AND row_id NOT IN (" + StringUtil::Join(ids, ",") + ")";
			}
		}
		if (flushed) {
			for (auto &entry : *flushed) {
				if (entry.inlined_table.table_name == source.table_name) {
					eligible += " AND begin_snapshot>" + to_string(entry.flush_snapshot_id);
				}
			}
		}
		auto &embedding = columns[0];
		auto sql = DuckLakeMetadataManager::ReadInlinedDataAggregatesSql(
		    source.table_name,
		    "count(*) FILTER(WHERE " + eligible + "),coalesce(sum(coalesce(array_length(" + embedding +
		        "),0)*4+64+"
		        "coalesce(octet_length(encode(" +
		        columns[1] + ")),0)+coalesce(octet_length(encode(" + columns[2] + ")),0)) FILTER(WHERE " + eligible +
		        "),0),count(*) FILTER(WHERE (" + eligible + ") AND (" + embedding + " IS NULL OR array_length(" +
		        embedding + ")<>" + to_string(p.config.dimension) + "))");
		auto result = meta.transaction.Query(snapshot, sql);
		if (result->HasError()) {
			result->ThrowError();
		}
		auto values = result->Fetch();
		if (values->GetValue(2, 0).GetValue<idx_t>()) {
			throw InvalidInputException("Invalid optimizer inline vector dimension");
		}
		p.t_rows += values->GetValue(0, 0).GetValue<idx_t>();
		p.t_bytes += values->GetValue(1, 0).GetValue<idx_t>();
	}
	auto revision = ReadyRevision(meta, p);
	unique_ptr<DuckLakeVortexIndex> index;
	p.merge_candidates.clear();
	if (revision) {
		index = make_uniq<DuckLakeVortexIndex>(VortexCatalog::Lookup(meta.transaction, table, p.config.index));
		if (index->dimension != p.config.dimension || index->field_id != p.field_id) {
			throw InvalidInputException("Optimizer ready index vector specification changed");
		}
		if (index->segments.size() == VortexCatalog::MAX_SEGMENTS) {
			p.merge_candidates = index->SmallestSegments();
		}
	}
	std::set<idx_t> covered, found;
	if (index) {
		for (auto &file : index->files) {
			covered.insert(file.id);
		}
	}
	if (rewrite) {
		if (!index || p.uuid != rewrite->table_uuid || p.config.index != rewrite->before.name ||
		    revision != rewrite->before.revision) {
			throw TransactionException("Vortex rewrite budget index identity changed");
		}
		for (auto id : rewrite->replaced) {
			if (!covered.erase(id)) {
				throw InvalidInputException("Vortex rewrite includes an uncovered source");
			}
		}
	}
	std::set<idx_t> replaced;
	if (changes) {
		for (auto &compaction : changes->compactions) {
			for (auto &source : compaction.source_files) {
				replaced.insert(source.file.id.index);
			}
		}
	}
	if (dropped) {
		for (auto &file : *dropped) {
			replaced.insert(file.second.index);
		}
	}
	auto files = manager.GetExtendedFilesForTable(table, snapshot);
	for (auto &file : files) {
		if (file.data_type != DuckLakeDataType::DATA_FILE) {
			continue;
		}
		if (file.file.data_format != DataFileFormat::VORTEX || !file.file.encryption_key.empty()) {
			throw InvalidInputException("Optimizer requires unencrypted Vortex files");
		}
		if (replaced.count(file.file_id.index)) {
			if (covered.count(file.file_id.index)) {
				throw InvalidInputException("Vortex write budget cannot replace an indexed source");
			}
			continue;
		}
		p.source_rows += file.row_count;
		p.source_bytes += file.file.file_size_bytes;
		if (!covered.count(file.file_id.index)) {
			p.f_rows += file.row_count;
			p.f_bytes += file.file.file_size_bytes;
		} else {
			found.insert(file.file_id.index);
			for (auto &old : index->files) {
				if (old.id == file.file_id.index && (old.uri != file.file.path || old.row_count != file.row_count)) {
					throw InvalidInputException("Optimizer indexed source identity changed");
				}
			}
		}
	}
	if (found != covered) {
		throw InvalidInputException("Optimizer indexed source was replaced; explicit rebuild required");
	}
	p.expected = revision;
	if (select_compaction) {
		p.rewrite_candidate.clear();
		p.rewrite_deleted = p.rewrite_remaining = 0;
		p.rewrite_reason = p.compaction_enabled ? "no_index" : "disabled";
		if (p.compaction_enabled && index) {
			SelectCompaction(meta, p, table, *index, files);
		}
	}
}

void Probe(DatabaseInstance &db, Policy &p, const std::function<void()> &check) {
	unique_ptr<DuckLakeVortexCollectionLock::Key> admission;
	if (p.compaction_enabled) {
		admission = DuckLakeVortexReadView::AdmissionLock().GetSharedLock(check);
	}
	Metadata meta(db, p.config.catalog);
	Probe(meta, p, true);
	meta.Commit();
}

bool Active(const string &state) {
	return state == "flushing" || state == "building" || state == "merging" || state == "rewriting" ||
	       state == "publishing" || state == "cancelling";
}
bool FlushDue(const Policy &p) {
	return p.t_rows >= p.config.flush_rows || p.t_bytes >= p.config.flush_bytes;
}
bool IndexDue(const Policy &p) {
	return p.f_rows >= p.config.index_rows || p.f_bytes >= p.config.index_bytes;
}

bool Published(Metadata &meta, const Policy &p) {
	if (p.generation.empty()) {
		return false;
	}
	if (p.kind == "rewrite") {
		auto result = meta.Query("SELECT revision FROM {METADATA_CATALOG}." + string(REWRITE_RECEIPTS) + Where(p) +
		                         " AND run_id=" + Quote(p.run) + " AND generation=" + Quote(p.generation));
		auto chunk = result->Fetch();
		return chunk && chunk->size() && chunk->GetValue(0, 0).GetValue<idx_t>() == p.expected + 1;
	}
	auto result = meta.Query("SELECT revision,reference FROM {METADATA_CATALOG}.ducklake_vortex_indexes_v1" + Where(p));
	auto chunk = result->Fetch();
	if (!chunk || !chunk->size() || chunk->GetValue(0, 0).GetValue<idx_t>() != p.expected + 1) {
		return false;
	}
	auto ready = VortexCatalog::ParseSegments(StringValue::Get(chunk->GetValue(1, 0)), p.uuid, p.field_id);
	return p.generation == ready.generation;
}

string Candidate(Connection &connection, const string &path) {
	auto &fs = FileSystem::GetFileSystem(*connection.context);
	auto file = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
	auto size = file->GetFileSize();
	if (size < 0 || size > 1024 * 1024) {
		throw InvalidInputException("Optimizer reference exceeds its budget");
	}
	string bytes(idx_t(size), '\0');
	if (size) {
		file->Read(&bytes[0], size, 0);
	}
	std::unique_ptr<yyjson_doc, decltype(&yyjson_doc_free)> doc(yyjson_read(bytes.data(), bytes.size(), 0),
	                                                            yyjson_doc_free);
	auto generation = doc ? yyjson_obj_get(yyjson_doc_get_root(doc.get()), "generation") : nullptr;
	auto value = generation ? yyjson_obj_get(generation, "generation") : nullptr;
	if (!value || !yyjson_is_str(value)) {
		throw InvalidInputException("Invalid optimizer candidate generation");
	}
	return string(yyjson_get_str(value), yyjson_get_len(value));
}

struct Pending {
	Policy policy;
	std::function<void(const string &)> observe;
};

struct PendingMaintenance {
	MaintenanceCatalog catalog;
	std::function<void(const string &)> observe;
};

struct Runtime : public std::enable_shared_from_this<Runtime> {
	std::mutex lock, io;
	std::condition_variable changed, finished;
	bool started = false, stopping = false, done = false;
	std::thread::id worker_id;
	std::map<Key, std::shared_ptr<Pending>> policies;
	std::set<Key> cancelled;
	std::map<Key, string> errors;
	std::map<MaintenanceId, std::shared_ptr<PendingMaintenance>> maintenance;
	std::set<MaintenanceId> maintenance_cancelled;
	std::map<MaintenanceId, string> maintenance_errors;
	std::shared_ptr<PendingMaintenance> active_maintenance;
	Key active;
	shared_ptr<ClientContext> context;

	void Start(DatabaseInstance &db) {
		std::lock_guard<std::mutex> guard(lock);
		if (started) {
			return;
		}
		weak_ptr<DatabaseInstance> weak(db.shared_from_this());
		auto self = shared_from_this();
		std::thread([self, weak] { self->Loop(weak); }).detach();
		started = true;
	}
	void Stop() {
		std::unique_lock<std::mutex> guard(lock);
		stopping = true;
		if (context) {
			context->Interrupt();
		}
		changed.notify_all();
		if (started && worker_id != std::this_thread::get_id()) {
			finished.wait(guard, [&] { return done; });
		}
	}
	void Add(const Policy &p, const std::function<void(const string &)> &observe) {
		std::lock_guard<std::mutex> guard(lock);
		if (policies.size() >= 16 && !policies.count(p.Id())) {
			throw InvalidInputException("Optimizer policy limit exceeded");
		}
		policies[p.Id()] = std::make_shared<Pending>(Pending {p, observe});
		cancelled.erase(p.Id());
		errors.erase(p.Id());
		changed.notify_all();
	}
	bool IsCurrent(const std::shared_ptr<Pending> &pending) {
		std::lock_guard<std::mutex> guard(lock);
		return policies.at(pending->policy.Id()) == pending;
	}
	void Capacity(const Policy &p) {
		std::lock_guard<std::mutex> guard(lock);
		if (policies.size() >= 16 && !policies.count(p.Id())) {
			throw InvalidInputException("Optimizer policy limit exceeded");
		}
	}
	void AddMaintenance(const MaintenanceCatalog &catalog, const std::function<void(const string &)> &observe) {
		std::lock_guard<std::mutex> guard(lock);
		maintenance[catalog.Id()] = std::make_shared<PendingMaintenance>(PendingMaintenance {catalog, observe});
		maintenance_cancelled.erase(catalog.Id());
		maintenance_errors.erase(catalog.Id());
		changed.notify_all();
	}
	void MaintenanceCapacity(DatabaseInstance &db, const MaintenanceCatalog &catalog) {
		std::lock_guard<std::mutex> guard(lock);
		auto &manager = DatabaseManager::Get(db);
		for (auto entry = maintenance.begin(); entry != maintenance.end();) {
			auto lake = manager.GetDatabase(entry->second->catalog.name);
			if (lake && lake->oid == entry->first.first) {
				auto &attached = lake->GetCatalog().Cast<DuckLakeCatalog>();
				auto metadata = manager.GetDatabase(attached.MetadataDatabaseName());
				if (metadata && metadata->oid == entry->first.second) {
					++entry;
					continue;
				}
			}
			maintenance_cancelled.erase(entry->first);
			maintenance_errors.erase(entry->first);
			entry = maintenance.erase(entry);
		}
		if (maintenance.size() >= 16 && !maintenance.count(catalog.Id())) {
			throw InvalidInputException("Vortex maintenance catalog limit exceeded");
		}
	}
	bool IsCurrentMaintenance(const std::shared_ptr<PendingMaintenance> &pending) {
		std::lock_guard<std::mutex> guard(lock);
		auto entry = maintenance.find(pending->catalog.Id());
		return entry != maintenance.end() && entry->second == pending;
	}
	bool IsMaintenanceActive(const MaintenanceId &id) {
		std::lock_guard<std::mutex> guard(lock);
		return active_maintenance && active_maintenance->catalog.Id() == id;
	}
	bool MaintenanceCancelled(const MaintenanceId &id) {
		std::lock_guard<std::mutex> guard(lock);
		return stopping || maintenance_cancelled.count(id);
	}
	void CheckMaintenance(const MaintenanceId &id) {
		if (MaintenanceCancelled(id)) {
			throw InterruptException();
		}
	}
	void CancelMaintenance(const MaintenanceId &id) {
		std::lock_guard<std::mutex> guard(lock);
		if (maintenance.count(id)) {
			maintenance_cancelled.insert(id);
		}
		if (active_maintenance && active_maintenance->catalog.Id() == id) {
			context->Interrupt();
		}
		changed.notify_all();
	}
	bool IsActive(const Key &key) {
		std::lock_guard<std::mutex> guard(lock);
		return active == key;
	}
	void Check(const Key &key) {
		std::lock_guard<std::mutex> guard(lock);
		if (stopping || cancelled.count(key)) {
			throw InterruptException();
		}
	}
	void Cancel(const Key &key) {
		std::lock_guard<std::mutex> guard(lock);
		cancelled.insert(key);
		if (active == key && context) {
			context->Interrupt();
		}
		changed.notify_all();
	}
	string Error(const Key &key) {
		std::lock_guard<std::mutex> guard(lock);
		auto found = errors.find(key);
		return found == errors.end() ? string() : found->second;
	}
	void Use(Connection &connection, const Key &key) {
		std::lock_guard<std::mutex> guard(lock);
		active = key;
		context = connection.context;
	}
	void Clear() {
		std::lock_guard<std::mutex> guard(lock);
		context.reset();
		active = Key();
	}
	void ClearMaintenance(const std::shared_ptr<PendingMaintenance> &pending) {
		std::lock_guard<std::mutex> guard(lock);
		if (active_maintenance == pending) {
			context.reset();
			active_maintenance.reset();
		}
	}
	void StoreMaintenance(DatabaseInstance &db, const MaintenanceCatalog &catalog, Maintenance &m,
	                      const string &state) {
		std::lock_guard<std::mutex> guard(io);
		CheckMaintenance(catalog.Id());
		Metadata meta(db, catalog.name, true, &catalog.identity);
		auto current = LoadMaintenance(meta);
		if (current.run != m.run) {
			throw TransactionException("Vortex maintenance ownership changed");
		}
		if (!current.enabled) {
			throw InterruptException();
		}
		m.state = state;
		SaveMaintenance(meta, m, m.run);
	}
	bool FinishMaintenance(DatabaseInstance &db, const std::shared_ptr<PendingMaintenance> &pending, Maintenance &m,
	                       const string &error) {
		std::lock_guard<std::mutex> guard(io);
		// Controls and status use io too, so completion and the active marker
		// change together.
		ClearMaintenance(pending);
		if (!IsCurrentMaintenance(pending)) {
			return false;
		}
		auto &catalog = pending->catalog;
		Metadata meta(db, catalog.name, true, &catalog.identity);
		auto current = LoadMaintenance(meta);
		if (current.run != m.run) {
			throw TransactionException("Vortex maintenance ownership changed");
		}
		const bool cancelled_job = !current.enabled || MaintenanceCancelled(catalog.Id());
		m.enabled = !cancelled_job && error.empty();
		if (m.enabled) {
			SetMaintenanceReadyState(m, MaintenanceBacklog(meta, m));
		} else {
			m.state = cancelled_job ? "cancelled" : "failed";
			m.error = error;
		}
		SaveMaintenance(meta, m, m.run);
		if (!m.enabled) {
			CancelMaintenance(catalog.Id());
		}
		return m.enabled;
	}
	void RunMaintenance(DatabaseInstance &db, const std::shared_ptr<PendingMaintenance> &pending) {
		auto &catalog = pending->catalog;
		Maintenance m;
		unique_ptr<Connection> job;
		{
			std::lock_guard<std::mutex> guard(io);
			if (!IsCurrentMaintenance(pending) || MaintenanceCancelled(catalog.Id())) {
				return;
			}
			Metadata meta(db, catalog.name, true, &catalog.identity);
			m = LoadMaintenance(meta);
			if (!m.enabled) {
				meta.Commit();
				return;
			}
			auto builds = m.remaining_builds;
			auto generations = m.remaining_generations;
			auto state = m.state;
			if (!MaintenanceBacklog(meta, m)) {
				SetMaintenanceReadyState(m, false);
				if (state != m.state || builds != m.remaining_builds || generations != m.remaining_generations) {
					SaveMaintenance(meta, m, m.run);
				} else {
					meta.Commit();
				}
				return;
			}
			CheckMaintenance(catalog.Id());
			job = make_uniq<Connection>(db);
			auto previous = m.run;
			m.run = UUID::ToString(UUID::GenerateRandomUUID());
			m.state = "recovering";
			m.error.clear();
			m.runs++;
			SaveMaintenance(meta, m, previous);
			std::lock_guard<std::mutex> guard_runtime(lock);
			active_maintenance = pending;
			context = job->context;
		}
		string error;
		try {
			auto check = [&](const string &stage) {
				if (pending->observe) {
					pending->observe(stage);
				}
				CheckMaintenance(catalog.Id());
				if (job->context->IsInterrupted()) {
					throw InterruptException();
				}
			};
			check("maintenance_before_recovery");
			m.recovered +=
			    VortexCatalog::RecoverBuilds(*job->context, catalog.name, m.max_objects, check, &catalog.identity);
			StoreMaintenance(db, catalog, m, "collecting_builds");
			check("maintenance_before_build_gc");
			auto builds = VortexCatalog::CollectBuilds(*job->context, catalog.name, m.max_objects, m.max_bytes, check,
			                                           &catalog.identity);
			m.builds += builds.builds;
			m.bytes += builds.bytes;
			StoreMaintenance(db, catalog, m, "collecting_generations");
			check("maintenance_after_build_gc");
			if (builds.builds < m.max_objects && builds.bytes < m.max_bytes) {
				check("maintenance_before_generation_gc");
				auto generations = VortexCatalog::Collect(*job->context, catalog.name, m.max_objects - builds.builds,
				                                          m.max_bytes - builds.bytes, check, &catalog.identity);
				m.generations += generations.generations;
				m.bytes += generations.artifact_bytes;
			}
			check("maintenance_after_generation_gc");
		} catch (const std::exception &failure) {
			error = failure.what();
		}
		if (FinishMaintenance(db, pending, m, error) && pending->observe) {
			try {
				pending->observe("maintenance_complete");
			} catch (const std::exception &failure) {
				FinishMaintenance(db, pending, m, failure.what());
			}
		}
	}
	void Stage(DatabaseInstance &db, const Pending &pending, Policy &p, const string &stage) {
		if (stage == "before_build_publish" || (p.kind == "rewrite" && stage == "rewrite_before_commit")) {
			std::lock_guard<std::mutex> guard(io);
			Metadata meta(db, p.config.catalog);
			auto current = Load(meta, p);
			if (current.run != p.run) {
				throw TransactionException("Optimizer task ownership changed");
			}
			current.generation = Candidate(meta.connection, p.reference);
			current.state = "publishing";
			Save(meta, current, p.run);
			p.generation = current.generation;
		}
		if (pending.observe) {
			pending.observe(stage);
		}
		Check(p.Id());
	}
	void Finish(DatabaseInstance &db, const std::shared_ptr<Pending> &pending, Policy &p, const string &state,
	            const string &error, bool owns_task) {
		std::lock_guard<std::mutex> guard(io);
		Clear();
		if (!IsCurrent(pending)) {
			return;
		}
		Metadata meta(db, p.config.catalog);
		auto current = Load(meta, p);
		if (current.run != p.run) {
			throw TransactionException("Optimizer task ownership changed");
		}
		if (!owns_task && (!current.enabled || Encode(current) != Encode(p))) {
			meta.Commit();
			return;
		}
		bool published = owns_task && Published(meta, current);
		current.state = published ? "published" : state;
		current.error = error;
		if (published) {
			current.published = current.expected + 1;
		} else if (!error.empty() || state == "cancelled") {
			current.enabled = false;
		}
		if (current.kind == "rewrite" && !error.empty()) {
			current.enabled = false;
		}
		Save(meta, current, p.run);
	}
	void Run(DatabaseInstance &db, const std::shared_ptr<Pending> &pending) {
		Policy p;
		{
			std::lock_guard<std::mutex> guard(io);
			if (!IsCurrent(pending)) {
				return;
			}
			Metadata meta(db, pending->policy.config.catalog);
			p = Load(meta, pending->policy);
			meta.Commit();
		}
		if (!p.enabled) {
			return;
		}
		Check(p.Id());
		auto metrics = p;
		bool owns_task = false, rewrite = false;
		auto check = [&] {
			Check(p.Id());
		};
		try {
			Stage(db, *pending, p, "optimizer_before_probe");
			Probe(db, metrics, check);
			rewrite = metrics.rewrite_reason == "eligible";
			bool flush = !rewrite && FlushDue(metrics), build = !rewrite && IndexDue(metrics);
			{
				std::lock_guard<std::mutex> guard(io);
				Metadata meta(db, p.config.catalog);
				auto current = Load(meta, p);
				if (!IsCurrent(pending) || !current.enabled || current.run != p.run) {
					meta.Commit();
					return;
				}
				if (Encode(current) != Encode(p)) {
					meta.Commit();
					return;
				}
				bool modified = current.t_rows != metrics.t_rows || current.t_bytes != metrics.t_bytes ||
				                current.f_rows != metrics.f_rows || current.f_bytes != metrics.f_bytes ||
				                current.rewrite_candidate != metrics.rewrite_candidate ||
				                current.rewrite_deleted != metrics.rewrite_deleted ||
				                current.rewrite_remaining != metrics.rewrite_remaining ||
				                current.rewrite_reason != metrics.rewrite_reason;
				current.t_rows = metrics.t_rows;
				current.t_bytes = metrics.t_bytes;
				current.f_rows = metrics.f_rows;
				current.f_bytes = metrics.f_bytes;
				current.rewrite_candidate = metrics.rewrite_candidate;
				current.rewrite_deleted = metrics.rewrite_deleted;
				current.rewrite_remaining = metrics.rewrite_remaining;
				current.rewrite_reason = metrics.rewrite_reason;
				if (flush || build || rewrite) {
					current.run = UUID::ToString(UUID::GenerateRandomUUID());
					current.reference = current.config.root + "/optimizer-" + current.run + ".json";
					current.generation.clear();
					current.error.clear();
					current.expected = metrics.expected;
					current.kind = rewrite                            ? "rewrite"
					               : flush                            ? "flush"
					               : metrics.merge_candidates.empty() ? "build"
					                                                  : "merge";
					current.selected = rewrite                   ? vector<string> {metrics.rewrite_candidate}
					                   : current.kind == "merge" ? metrics.merge_candidates
					                                             : vector<string>();
					current.state = rewrite                   ? "rewriting"
					                : flush                   ? "flushing"
					                : current.kind == "merge" ? "merging"
					                                          : "building";
					current.runs++;
				}
				if (modified || flush || build || rewrite) {
					Save(meta, current, p.run);
				} else {
					meta.Commit();
				}
				if (flush || build || rewrite) {
					std::lock_guard<std::mutex> guard(lock);
					active = current.Id();
					owns_task = true;
				}
				p = current;
			}
			if (!flush && !build && !rewrite) {
				return;
			}
			{
				Connection job(db);
				Use(job, p.Id());
				if (rewrite) {
					Stage(db, *pending, p, "optimizer_before_rewrite");
					VortexCatalog::Rewrite(
					    *job.context, p.config.catalog, p.config.schema, p.config.table, p.config.index, p.selected[0],
					    p.reference, p.expected, [&](const string &stage) { Stage(db, *pending, p, stage); }, p.run);
				}
				if (flush) {
					Stage(db, *pending, p, "optimizer_before_flush");
					job.BeginTransaction();
					auto &catalog = Catalog::GetCatalog(*job.context, p.config.catalog).Cast<DuckLakeCatalog>();
					auto entry = catalog.GetEntry(*job.context, CatalogType::TABLE_ENTRY, p.config.schema,
					                              p.config.table, OnEntryNotFound::THROW_EXCEPTION);
					if (entry->Cast<DuckLakeTableEntry>().GetTableUUID() != p.uuid ||
					    entry->Cast<DuckLakeTableEntry>()
					            .GetFieldId(vector<string> {"embedding"})
					            .GetFieldIndex()
					            .index != p.field_id) {
						throw InvalidInputException("Optimizer flush table or field identity changed");
					}
					FlushConfiguration(*job.context, entry->Cast<DuckLakeTableEntry>());
					auto result =
					    job.Query("CALL ducklake_flush_inlined_data(" + Quote(p.config.catalog) + ",schema_name=>" +
					              Quote(p.config.schema) + ",table_name=>" + Quote(p.config.table) + ")");
					if (result->HasError()) {
						result->ThrowError();
					}
					// Another connection may have flushed T already. Plan from committed T/F below.
					job.Commit();
					Stage(db, *pending, p, "optimizer_flushed");
					metrics = p;
					Probe(db, metrics, check);
					build = metrics.rewrite_reason != "eligible" && IndexDue(metrics);
				}
				if (build) {
					{
						std::lock_guard<std::mutex> guard(io);
						Metadata meta(db, p.config.catalog);
						auto current = Load(meta, p);
						current.expected = metrics.expected;
						current.kind = metrics.merge_candidates.empty() ? "build" : "merge";
						current.selected = metrics.merge_candidates;
						current.state = current.kind == "merge" ? "merging" : "building";
						Save(meta, current, p.run);
						p = current;
					}
					Stage(db, *pending, p, p.kind == "merge" ? "optimizer_before_merge" : "optimizer_before_build");
					VortexCatalog::BuildAndPublish(
					    *job.context, p.config.catalog, p.config.schema, p.config.table, p.config.index, p.reference,
					    p.config.dimension, p.expected, [&](const string &stage) { Stage(db, *pending, p, stage); },
					    p.uuid, optional_idx(p.field_id),
					    p.kind == "merge" ? DuckLakeVortexIndexOperation::MERGE
					    : p.expected      ? DuckLakeVortexIndexOperation::APPEND
					                      : DuckLakeVortexIndexOperation::REPLACE,
					    p.selected);
				}
			}
			Finish(db, pending, p, build || rewrite ? "published" : "flushed", "", true);
		} catch (const std::exception &error) {
			bool cancelled_job;
			{
				std::lock_guard<std::mutex> guard(lock);
				cancelled_job = stopping || cancelled.count(p.Id());
			}
			Finish(db, pending, p, cancelled_job ? "cancelled" : "failed", error.what(), owns_task);
			return;
		}
		if (pending->observe) {
			if (rewrite) {
				pending->observe("optimizer_rewritten");
			} else if (p.kind == "merge") {
				pending->observe("optimizer_merged");
			}
			pending->observe("optimizer_finished");
		}
	}
	void Loop(weak_ptr<DatabaseInstance> weak) {
		{
			std::lock_guard<std::mutex> guard(lock);
			worker_id = std::this_thread::get_id();
		}
		while (true) {
			vector<std::shared_ptr<Pending>> work;
			vector<std::shared_ptr<PendingMaintenance>> cleanup;
			{
				std::lock_guard<std::mutex> guard(lock);
				if (stopping) {
					break;
				}
				for (auto &entry : policies) {
					if (!cancelled.count(entry.first)) {
						work.push_back(entry.second);
					}
				}
				for (auto &entry : maintenance) {
					if (!maintenance_cancelled.count(entry.first)) {
						cleanup.push_back(entry.second);
					}
				}
			}
			for (auto &pending : work) {
				auto db = weak.lock();
				if (!db) {
					break;
				}
				try {
					try {
						Run(*db, pending);
					} catch (...) {
						if (pending->observe) {
							pending->observe("optimizer_before_error");
						}
						throw;
					}
				} catch (const std::exception &error) {
					Clear();
					std::lock_guard<std::mutex> guard(lock);
					if (policies.at(pending->policy.Id()) == pending) {
						cancelled.insert(pending->policy.Id());
						errors[pending->policy.Id()] = error.what();
					}
				}
			}
			for (auto &pending : cleanup) {
				auto db = weak.lock();
				if (!db) {
					break;
				}
				try {
					RunMaintenance(*db, pending);
				} catch (const std::exception &error) {
					std::lock_guard<std::mutex> guard_io(io);
					ClearMaintenance(pending);
					std::lock_guard<std::mutex> guard(lock);
					auto entry = maintenance.find(pending->catalog.Id());
					if (entry != maintenance.end() && entry->second == pending) {
						maintenance_cancelled.insert(entry->first);
						maintenance_errors[entry->first] = error.what();
					}
				}
			}
			std::unique_lock<std::mutex> guard(lock);
			if (stopping || weak.expired()) {
				break;
			}
			changed.wait_for(guard, std::chrono::milliseconds(100));
		}
		{
			std::lock_guard<std::mutex> guard(lock);
			done = true;
			context.reset();
			active = Key();
			active_maintenance.reset();
		}
		finished.notify_all();
	}
};

struct Owner : public ObjectCacheEntry {
	std::shared_ptr<Runtime> runtime = std::make_shared<Runtime>();
	DuckLakeVortexCollectionLock write_admission;
	~Owner() override {
		runtime->Stop();
	}
	static string ObjectType() {
		return "ducklake_vortex_optimizer";
	}
	string GetObjectType() override {
		return ObjectType();
	}
	optional_idx GetEstimatedCacheMemory() const override {
		return optional_idx();
	}
};

shared_ptr<Owner> GetOwner(DatabaseInstance &db) {
	return db.GetObjectCache().GetOrCreate<Owner>("ducklake_vortex_optimizer_owner_v1");
}

string Canonical(const string &path) {
#ifdef _WIN32
	throw NotImplementedException("Native Vortex optimizer requires POSIX paths");
#else
	std::unique_ptr<char, decltype(&free)> resolved(realpath(path.c_str(), nullptr), free);
	if (!resolved || path != resolved.get()) {
		throw InvalidInputException("Optimizer paths must be canonical and absolute");
	}
	return path;
#endif
}

Policy Identify(Metadata &meta, const string &catalog, const string &schema, const string &table, const string &index) {
	Policy p;
	p.config.catalog = catalog;
	p.config.schema = schema;
	p.config.table = table;
	p.config.index = index;
	auto &entry = Table(meta, p);
	p.uuid = entry.GetTableUUID();
	p.field_id = entry.GetFieldId(vector<string> {"embedding"}).GetFieldIndex().index;
	return p;
}

void Validate(const Policy &p) {
	if ((p.kind != "none" && p.kind != "flush" && p.kind != "build" && p.kind != "merge" && p.kind != "rewrite") ||
	    (p.kind == "merge"     ? p.selected.size() != 2 || p.selected[0] == p.selected[1]
	     : p.kind == "rewrite" ? p.selected.size() != 1
	                           : !p.selected.empty())) {
		throw InvalidInputException("Invalid optimizer task kind or generation selection");
	}
	auto &c = p.config;
	for (auto &value : {c.catalog, c.schema, c.table, c.index, c.root}) {
		if (value.empty() || value.size() > 4096 || value.find('\0') != string::npos) {
			throw InvalidInputException("Invalid optimizer name or path");
		}
	}
	if (c.index.size() > 128 || !c.dimension || c.dimension > 4096 || !c.flush_rows || c.flush_rows > 100000 ||
	    !c.index_rows || c.index_rows > 100000 || !c.flush_bytes || c.flush_bytes > INLINE_LIMIT || !c.index_bytes ||
	    c.index_bytes > SOURCE_LIMIT) {
		throw InvalidInputException("Invalid optimizer dimension or thresholds");
	}
	if (!p.min_deleted_rows || p.min_deleted_rows > 100000 || !p.min_deleted_percent || p.min_deleted_percent > 100) {
		throw InvalidInputException("Invalid Vortex compaction thresholds");
	}
}

bool HasWriteBudgetTable(Metadata &meta) {
	auto &catalog = meta.transaction.GetCatalog();
	return bool(meta.transaction.GetConnection().TableInfo(catalog.MetadataDatabaseName(), catalog.MetadataSchemaName(),
	                                                       WRITE_BUDGETS));
}

DuckLakeVortexWriteBudget ReadBudget(DataChunk &chunk, idx_t row, idx_t offset = 0) {
	return {chunk.GetValue(offset, row).GetValue<idx_t>(), chunk.GetValue(offset + 1, row).GetValue<idx_t>(),
	        chunk.GetValue(offset + 2, row).GetValue<idx_t>(), chunk.GetValue(offset + 3, row).GetValue<idx_t>()};
}

void CheckLimit(const char *name, idx_t used, idx_t limit) {
	if (used > limit) {
		throw InvalidInputException("RESOURCE_EXHAUSTED: Vortex write %s budget exceeded (%llu > %llu)", name, used,
		                            limit);
	}
}

void CheckWriteBudget(const Policy &p, const DuckLakeVortexWriteBudget &budget) {
	CheckLimit("inline rows", p.t_rows, budget.inline_rows);
	CheckLimit("inline bytes", p.t_bytes, budget.inline_bytes);
	CheckLimit("unindexed rows", p.f_rows, budget.unindexed_rows);
	CheckLimit("unindexed bytes", p.f_bytes, budget.unindexed_bytes);
	CheckLimit("physical and inline rows", p.source_rows + p.pending_rows,
	           MinValue<idx_t>(100000, SOURCE_LIMIT / sizeof(float) / p.config.dimension));
	CheckLimit("physical source bytes", p.source_bytes, SOURCE_LIMIT);
	CheckLimit("pending inline bytes", p.pending_bytes, SOURCE_LIMIT);
}

void PendingInline(Metadata &meta, Policy &p, const vector<FlushedInlinedTableInfo> &flushed) {
	auto &table = Table(meta, p);
	auto snapshot = meta.transaction.GetSnapshot();
	p.pending_rows = p.pending_bytes = 0;
	for (auto &source : table.GetInlinedDataTables()) {
		auto columns = InlineSourceColumns(meta, table, source);
		string filter;
		for (auto &entry : flushed) {
			if (entry.inlined_table.table_name == source.table_name) {
				filter += " AND begin_snapshot>" + to_string(entry.flush_snapshot_id);
			}
		}
		// Flush materializes ended versions too; visible T alone underestimates build capacity.
		auto result =
		    meta.transaction.Query(snapshot, "SELECT count(*),coalesce(sum(coalesce(array_length(" + columns[0] +
		                                         "),0)*4+64+"
		                                         "coalesce(octet_length(encode(" +
		                                         columns[1] + ")),0)+coalesce(octet_length(encode(" + columns[2] +
		                                         ")),0)),0) FROM {METADATA_CATALOG}." + source.table_name +
		                                         " WHERE begin_snapshot<={SNAPSHOT_ID}" + filter);
		if (result->HasError()) {
			result->ThrowError();
		}
		auto values = result->Fetch();
		p.pending_rows += values->GetValue(0, 0).GetValue<idx_t>();
		p.pending_bytes += values->GetValue(1, 0).GetValue<idx_t>();
	}
}

void AddWrite(Policy &p, DuckLakeTableEntry &table, const LocalTableDataChanges &changes,
              optional_ptr<const DuckLakeVortexRewriteState> rewrite = nullptr) {
	auto file = [&](const DuckLakeDataFile &data) {
		if (data.format != DataFileFormat::VORTEX || !data.encryption_key.empty()) {
			throw InvalidInputException("Vortex write budgets require unencrypted Vortex output");
		}
		p.source_rows += data.row_count;
		p.source_bytes += data.file_size_bytes;
		if (!rewrite || data.file_name != rewrite->new_path) {
			p.f_rows += data.row_count;
			p.f_bytes += data.file_size_bytes;
		}
	};
	for (auto &data : changes.new_data_files) {
		file(data);
	}
	for (auto &compaction : changes.compactions) {
		if (!compaction.written_file.file_name.empty()) {
			file(compaction.written_file);
		}
	}
	if (!changes.new_inlined_data) {
		return;
	}
	auto &data = *changes.new_inlined_data;
	if (!data.data || table.GetColumns().PhysicalColumnCount() != 4 || data.data->ColumnCount() != 4) {
		throw InvalidInputException("Vortex write budgets require native four-column inline data");
	}
	auto column = [&](string name) {
		return table.GetColumns().LogicalToPhysical(table.GetColumns().GetColumnIndex(name)).index;
	};
	auto embedding = column("embedding"), category = column("category"), payload = column("payload");
	p.t_rows += data.Count();
	p.pending_rows += data.Count();
	for (auto &chunk : data.data->Chunks()) {
		for (idx_t row = 0; row < chunk.size(); row++) {
			auto vector = chunk.GetValue(embedding, row);
			if (vector.IsNull() || ListValue::GetChildren(vector).size() != p.config.dimension) {
				throw InvalidInputException("Invalid write budget inline vector dimension");
			}
			for (auto &value : ListValue::GetChildren(vector)) {
				if (value.IsNull() || !std::isfinite(value.GetValue<float>())) {
					throw InvalidInputException("Write budget vectors require finite non-NULL components");
				}
			}
			idx_t bytes = p.config.dimension * sizeof(float) + 64;
			for (auto col : {category, payload}) {
				auto value = chunk.GetValue(col, row);
				if (!value.IsNull()) {
					bytes += StringValue::Get(value).size();
				}
			}
			p.t_bytes += bytes;
			p.pending_bytes += bytes;
		}
	}
}

bool Grows(const LocalTableDataChanges &changes) {
	return changes.new_inlined_data || !changes.new_data_files.empty() || !changes.compactions.empty();
}

} // namespace

DuckLakeVortexCollectionLock &DuckLakeVortexOptimizer::WriteAdmissionLock(DatabaseInstance &db) {
	return GetOwner(db)->write_admission;
}

unique_ptr<DuckLakeVortexCollectionLock::Key>
DuckLakeVortexOptimizer::AdmitWrite(DuckLakeTransaction &transaction, ClientContext &context,
                                    unique_ptr<DuckLakeVortexCollectionLock::Key> &publication_guard) {
	auto rewrite = DuckLakeVortexRewriteState::Get(context);
	auto interrupt = [&] {
		if (context.IsInterrupted() || (rewrite && rewrite->caller->IsInterrupted())) {
			throw InterruptException();
		}
	};
	interrupt();
	bool grows = false;
	for (auto &entry : transaction.GetLocalChanges().Changes()) {
		grows |= Grows(entry.GetTableChanges());
	}
	auto &catalog = transaction.GetCatalog();
	unique_ptr<DuckLakeVortexCollectionLock::Key> admission;
	if (grows && (catalog.MetadataType().empty() || catalog.MetadataType() == "duckdb")) {
		admission = WriteAdmissionLock(*context.db).GetExclusiveLock(interrupt);
	}
	// Use the same admission/publication order as configuration, and keep coverage stable through commit.
	publication_guard = rewrite ? VortexCatalog::PublicationLock().GetExclusiveLock(interrupt)
	                            : VortexCatalog::PublicationLock().GetSharedLock(interrupt);
	if (!admission) {
		return nullptr;
	}
	Metadata meta(*context.db, rewrite ? rewrite->catalog : catalog.GetName(), false,
	              rewrite ? &rewrite->identity : nullptr);
	try {
		if (HasWriteBudgetTable(meta)) {
			for (auto &entry : transaction.GetLocalChanges().Changes()) {
				auto &changes = entry.GetTableChanges();
				if (!Grows(changes) || entry.GetTableIndex().IsTransactionLocal() ||
				    transaction.GetDroppedTables().count(entry.GetTableIndex())) {
					continue;
				}
				auto rows =
				    meta.Query("SELECT table_uuid,index_name," + string(BUDGET_COLUMNS) + " FROM {METADATA_CATALOG}." +
				               WRITE_BUDGETS + " WHERE table_id=" + to_string(entry.GetTableIndex().index));
				auto row = rows->Fetch();
				if (!row || !row->size()) {
					continue;
				}
				Policy key;
				key.uuid = StringValue::Get(row->GetValue(0, 0));
				key.config.index = StringValue::Get(row->GetValue(1, 0));
				key.config.catalog = catalog.GetName();
				auto p = Load(meta, key);
				auto &table = Table(meta, p);
				if (table.GetTableId() != entry.GetTableIndex()) {
					throw InvalidInputException("Vortex write budget table identity changed");
				}
				Probe(meta, p, false, &changes, &transaction.GetFlushedInlinedTables(), &transaction.GetDroppedFiles(),
				      rewrite.get());
				PendingInline(meta, p, transaction.GetFlushedInlinedTables());
				AddWrite(p, table, changes, rewrite.get());
				CheckWriteBudget(p, ReadBudget(*row, 0, 2));
			}
		}
		meta.Commit();
		interrupt();
		return admission;
	} catch (...) {
		VortexCatalog::Rollback(meta.connection);
		throw;
	}
}

void DuckLakeVortexOptimizer::SetWriteBudget(ClientContext &context, const string &catalog, const string &schema,
                                             const string &table, const string &index,
                                             const DuckLakeVortexWriteBudget &budget) {
	if (!budget.inline_rows || budget.inline_rows > 100000 || !budget.inline_bytes ||
	    budget.inline_bytes > INLINE_LIMIT || !budget.unindexed_rows || budget.unindexed_rows > 100000 ||
	    !budget.unindexed_bytes || budget.unindexed_bytes > SOURCE_LIMIT) {
		throw InvalidInputException("Invalid Vortex write budget limits");
	}
	auto interrupt = [&] {
		if (context.IsInterrupted()) {
			throw InterruptException();
		}
	};
	auto owner = GetOwner(*context.db);
	auto admission = owner->write_admission.GetExclusiveLock(interrupt);
	auto publication = VortexCatalog::PublicationLock().GetSharedLock(interrupt);
	std::lock_guard<std::mutex> guard(owner->runtime->io);
	if (context.IsInterrupted()) {
		throw InterruptException();
	}
	Metadata meta(*context.db, catalog);
	try {
		Initialized(meta);
		auto p = Load(meta, Identify(meta, catalog, schema, table, index));
		auto &entry = Table(meta, p);
		if (entry.GetColumns().PhysicalColumnCount() != 4 || entry.GetColumns().LogicalColumnCount() != 4) {
			throw InvalidInputException("Vortex write budgets require the four business columns");
		}
		Probe(meta, p);
		PendingInline(meta, p, {});
		CheckWriteBudget(p, budget);
		meta.Query("INSERT INTO {METADATA_CATALOG}." + string(WRITE_BUDGETS) + " VALUES(" + Quote(p.uuid) + "," +
		           to_string(entry.GetTableId().index) + "," + Quote(index) + "," + to_string(budget.inline_rows) +
		           "," + to_string(budget.inline_bytes) + "," + to_string(budget.unindexed_rows) + "," +
		           to_string(budget.unindexed_bytes) +
		           ") ON CONFLICT(table_uuid) DO UPDATE SET "
		           "index_name=excluded.index_name,max_inline_rows=excluded.max_inline_rows,"
		           "max_inline_bytes=excluded.max_inline_bytes,max_unindexed_rows=excluded.max_unindexed_rows,"
		           "max_unindexed_bytes=excluded.max_unindexed_bytes");
		if (context.IsInterrupted()) {
			throw InterruptException();
		}
		meta.Commit();
	} catch (...) {
		VortexCatalog::Rollback(meta.connection);
		throw;
	}
}

void DuckLakeVortexOptimizer::Initialize(Connection &owner, const string &catalog) {
	if (owner.context->transaction.HasActiveTransaction()) {
		throw InvalidInputException("Vortex optimizer catalog requires an idle dedicated connection");
	}
	auto admission = WriteAdmissionLock(*owner.context->db).GetExclusiveLock([&] {
		if (owner.context->IsInterrupted()) {
			throw InterruptException();
		}
	});
	try {
		auto &transaction = VortexCatalog::Begin(owner, catalog);
		VortexCatalog::Initialized(transaction);
		VortexCatalog::Query(
		    transaction,
		    "CREATE TABLE {METADATA_CATALOG}.ducklake_vortex_optimizer_v2("
		    "table_uuid VARCHAR NOT NULL,index_name VARCHAR NOT NULL,config VARCHAR NOT NULL,enabled BOOLEAN NOT NULL,"
		    "state VARCHAR NOT NULL,run_id VARCHAR NOT NULL,expected_revision BIGINT NOT NULL,candidate_generation "
		    "VARCHAR NOT NULL,"
		    "published_revision BIGINT NOT NULL,reference_path VARCHAR NOT NULL,error VARCHAR NOT NULL,"
		    "inline_rows BIGINT NOT NULL,inline_bytes BIGINT NOT NULL,unindexed_rows BIGINT NOT NULL,unindexed_bytes "
		    "BIGINT NOT NULL,"
		    "run_count BIGINT NOT NULL,task_kind VARCHAR NOT NULL,selected_generations VARCHAR NOT NULL,"
		    "rewrite_candidate VARCHAR NOT NULL,rewrite_deleted BIGINT NOT NULL,rewrite_remaining BIGINT NOT NULL,"
		    "rewrite_reason VARCHAR NOT NULL,"
		    "PRIMARY KEY(table_uuid,index_name))");
		VortexCatalog::Query(transaction,
		                     "CREATE TABLE {METADATA_CATALOG}." + string(REWRITE_RECEIPTS) +
		                         "(table_uuid VARCHAR NOT NULL,index_name VARCHAR NOT NULL,"
		                         "run_id VARCHAR NOT NULL,generation VARCHAR NOT NULL,revision BIGINT NOT NULL,"
		                         "PRIMARY KEY(table_uuid,index_name))");
		VortexCatalog::Query(transaction,
		                     "CREATE TABLE {METADATA_CATALOG}.ducklake_vortex_write_budgets_v1("
		                     "table_uuid VARCHAR PRIMARY KEY,table_id BIGINT NOT NULL,index_name VARCHAR NOT NULL,"
		                     "max_inline_rows BIGINT NOT NULL,max_inline_bytes BIGINT NOT NULL,"
		                     "max_unindexed_rows BIGINT NOT NULL,max_unindexed_bytes BIGINT NOT NULL)");
		VortexCatalog::Query(
		    transaction,
		    "CREATE TABLE {METADATA_CATALOG}.ducklake_vortex_maintenance_v1("
		    "id BOOLEAN PRIMARY KEY CHECK(id),enabled BOOLEAN NOT NULL,state VARCHAR NOT NULL "
		    "CHECK(state IN "
		    "('idle','blocked','recovering','collecting_builds','collecting_generations',"
		    "'cancelling','cancelled','failed')),"
		    "run_id VARCHAR NOT NULL,"
		    "max_objects BIGINT NOT NULL,max_bytes BIGINT NOT NULL,run_count BIGINT NOT NULL,"
		    "reconciled_builds BIGINT NOT NULL,collected_builds BIGINT NOT NULL,collected_generations BIGINT NOT NULL,"
		    "collected_bytes BIGINT NOT NULL,remaining_builds BIGINT NOT NULL,remaining_generations BIGINT NOT NULL,"
		    "error VARCHAR NOT NULL)");
		owner.Commit();
	} catch (...) {
		VortexCatalog::Rollback(owner);
		throw;
	}
}

void DuckLakeVortexOptimizer::Enable(ClientContext &context, const Config &config,
                                     const std::function<void(const string &)> &observe) {
	auto owner = GetOwner(*context.db);
	Policy p;
	{
		std::lock_guard<std::mutex> guard(owner->runtime->io);
		Metadata meta(*context.db, config.catalog);
		Initialized(meta);
		p = Identify(meta, config.catalog, config.schema, config.table, config.index);
		p.config = config;
		Validate(p);
		FlushConfiguration(*meta.connection.context, Table(meta, p));
		auto root = Canonical(config.root);
		auto &fs = FileSystem::GetFileSystem(context);
		if (!fs.DirectoryExists(root)) {
			throw InvalidInputException("Optimizer output root must be an existing directory");
		}
		auto data = meta.transaction.GetCatalog().DataPath();
		while (data.size() > 1 && data.back() == '/') {
			data.pop_back();
		}
		if (!fs.IsPathAbsolute(data)) {
			throw InvalidInputException("Optimizer managed data path must be absolute");
		}
		if (!fs.DirectoryExists(data)) {
			fs.CreateDirectory(data);
		}
		data = Canonical(data);
		if (root == data || StringUtil::StartsWith(root, data == "/" ? data : data + "/")) {
			throw InvalidInputException("Optimizer root must be outside managed data");
		}
		auto count = meta.Query("SELECT count(*) FROM {METADATA_CATALOG}." + string(POLICIES));
		if (count->Fetch()->GetValue(0, 0).GetValue<idx_t>() >= 16) {
			throw InvalidInputException("Optimizer policy limit exceeded");
		}
		owner->runtime->Capacity(p);
		p.enabled = true;
		meta.Query("INSERT INTO {METADATA_CATALOG}." + string(POLICIES) + " VALUES(" + Quote(p.uuid) + "," +
		           Quote(config.index) + "," + Quote(Encode(p)) +
		           ",true,'idle','',0,'',0,'','',0,0,0,0,0,'none','[]','',0,0,'disabled')");
		meta.Commit();
		owner->runtime->Add(p, observe);
	}
	owner->runtime->Start(*context.db);
}

string DuckLakeVortexOptimizer::Resume(ClientContext &context, const string &catalog, const string &schema,
                                       const string &table, const string &index,
                                       const std::function<void(const string &)> &observe) {
	auto binding = VortexCatalog::ResolveCatalog(context, catalog, true, "Vortex optimizer resume");
	auto owner = GetOwner(*context.db);
	Policy p;
	{
		std::lock_guard<std::mutex> guard(owner->runtime->io);
		Metadata meta(*context.db, catalog, true, &binding);
		p = Load(meta, Identify(meta, catalog, schema, table, index));
		Validate(p);
		FlushConfiguration(*meta.connection.context, Table(meta, p));
		if (owner->runtime->IsActive(p.Id())) {
			throw InvalidInputException("Optimizer task is still running");
		}
		owner->runtime->Capacity(p);
		if (Active(p.state)) {
			bool published = Published(meta, p);
			p.state = published ? "published" : "interrupted";
			p.error = published ? "" : "Previous task interrupted; future work is planned from current committed state";
			if (published) {
				p.published = p.expected + 1;
			}
		} else {
			p.state = "idle";
			p.error.clear();
		}
		p.enabled = true;
		Save(meta, p, p.run);
		owner->runtime->Add(p, observe);
	}
	owner->runtime->Start(*context.db);
	return p.state;
}

void DuckLakeVortexOptimizer::Cancel(ClientContext &context, const string &catalog, const string &schema,
                                     const string &table, const string &index) {
	auto owner = GetOwner(*context.db);
	Policy p;
	{
		std::lock_guard<std::mutex> guard(owner->runtime->io);
		Metadata meta(*context.db, catalog);
		p = Load(meta, Identify(meta, catalog, schema, table, index));
		p.enabled = false;
		p.state = owner->runtime->IsActive(p.Id()) ? "cancelling" : "cancelled";
		Save(meta, p, p.run);
		owner->runtime->Cancel(p.Id());
	}
}

void DuckLakeVortexOptimizer::ConfigureCompaction(ClientContext &context, const string &catalog, const string &schema,
                                                  const string &table, const string &index, bool enabled,
                                                  idx_t min_deleted_rows, idx_t min_deleted_percent) {
	auto binding = VortexCatalog::ResolveCatalog(context, catalog, true, "Vortex compaction policy");
	auto owner = GetOwner(*context.db);
	std::lock_guard<std::mutex> guard(owner->runtime->io);
	Metadata meta(*context.db, catalog, true, &binding);
	auto p = Load(meta, Identify(meta, catalog, schema, table, index));
	if (owner->runtime->IsActive(p.Id())) {
		throw InvalidInputException("Cancel the running optimizer task before changing compaction policy");
	}
	p.compaction_enabled = enabled;
	p.min_deleted_rows = min_deleted_rows;
	p.min_deleted_percent = min_deleted_percent;
	Validate(p);
	if (enabled) {
		if (!HasWriteBudgetTable(meta)) {
			throw InvalidInputException("Background Vortex compaction requires an installed write budget");
		}
		auto budget = meta.Query("SELECT count(*) FROM {METADATA_CATALOG}." + string(WRITE_BUDGETS) + Where(p));
		if (budget->Fetch()->GetValue(0, 0).GetValue<idx_t>() != 1) {
			throw InvalidInputException(
			    "Background Vortex compaction requires an installed write budget for this index");
		}
	}
	p.rewrite_candidate.clear();
	p.rewrite_deleted = p.rewrite_remaining = 0;
	p.rewrite_reason = enabled ? "awaiting_probe" : "disabled";
	Save(meta, p, p.run);
	owner->runtime->changed.notify_all();
}

void DuckLakeVortexOptimizer::RecordRewritePublication(DuckLakeTransaction &transaction,
                                                       const DuckLakeVortexRewriteState &plan) {
	// Keep the last committed rewrite per policy outside the mutable task row.
	// The source and index commit makes the receipt visible at the same time.
	auto result = VortexCatalog::Query(
	    transaction, "INSERT OR REPLACE INTO {METADATA_CATALOG}." + string(REWRITE_RECEIPTS) +
	                     " SELECT table_uuid,index_name,run_id," + Quote(plan.candidate.generation) + "," +
	                     to_string(plan.candidate.revision) + " FROM {METADATA_CATALOG}." + POLICIES +
	                     " WHERE table_uuid=" + Quote(plan.table_uuid) + " AND index_name=" + Quote(plan.before.name) +
	                     " AND task_kind='rewrite' AND run_id=" + Quote(plan.optimizer_run_id) +
	                     " AND expected_revision=" + to_string(plan.before.revision) + " RETURNING run_id");
	if (!result->Fetch()) {
		throw TransactionException("Optimizer rewrite publication ownership changed");
	}
}

void DuckLakeVortexOptimizer::EnableMaintenance(ClientContext &context, const string &catalog, idx_t max_objects,
                                                idx_t max_bytes, const std::function<void(const string &)> &observe) {
	if (!max_objects || max_objects > 16 || !max_bytes || max_bytes > SOURCE_LIMIT) {
		throw InvalidInputException("Invalid Vortex maintenance count or byte budget");
	}
	auto binding = ResolveMaintenanceCatalog(context, catalog, true);
	auto owner = GetOwner(*context.db);
	{
		std::lock_guard<std::mutex> guard(owner->runtime->io);
		owner->runtime->MaintenanceCapacity(*context.db, binding);
		Metadata meta(*context.db, catalog, true, &binding.identity);
		Initialized(meta);
		meta.Query("INSERT INTO {METADATA_CATALOG}." + string(MAINTENANCE) + " VALUES(true,true,'idle',''," +
		           to_string(max_objects) + "," + to_string(max_bytes) + ",0,0,0,0,0,0,0,'')");
		meta.Commit();
		owner->runtime->AddMaintenance(binding, observe);
	}
	owner->runtime->Start(*context.db);
}

void DuckLakeVortexOptimizer::ResumeMaintenance(ClientContext &context, const string &catalog,
                                                const std::function<void(const string &)> &observe) {
	auto binding = ResolveMaintenanceCatalog(context, catalog, true);
	auto owner = GetOwner(*context.db);
	{
		std::lock_guard<std::mutex> guard(owner->runtime->io);
		Metadata meta(*context.db, catalog, true, &binding.identity);
		auto m = LoadMaintenance(meta);
		if (owner->runtime->IsMaintenanceActive(binding.Id())) {
			throw InvalidInputException("Vortex maintenance is still running");
		}
		owner->runtime->MaintenanceCapacity(*context.db, binding);
		m.enabled = true;
		SetMaintenanceReadyState(m, MaintenanceBacklog(meta, m));
		SaveMaintenance(meta, m, m.run);
		owner->runtime->AddMaintenance(binding, observe);
	}
	owner->runtime->Start(*context.db);
}

void DuckLakeVortexOptimizer::CancelMaintenance(ClientContext &context, const string &catalog) {
	auto binding = ResolveMaintenanceCatalog(context, catalog, true);
	auto owner = GetOwner(*context.db);
	std::lock_guard<std::mutex> guard(owner->runtime->io);
	Metadata meta(*context.db, catalog, true, &binding.identity);
	auto m = LoadMaintenance(meta);
	m.enabled = false;
	m.state = owner->runtime->IsMaintenanceActive(binding.Id()) ? "cancelling" : "cancelled";
	SaveMaintenance(meta, m, m.run);
	owner->runtime->CancelMaintenance(binding.Id());
}

vector<vector<Value>> DuckLakeVortexOptimizer::MaintenanceStatus(ClientContext &context, const string &catalog) {
	auto binding = ResolveMaintenanceCatalog(context, catalog, false);
	auto owner = GetOwner(*context.db);
	std::lock_guard<std::mutex> guard(owner->runtime->io);
	Metadata meta(*context.db, catalog, true, &binding.identity);
	Initialized(meta);
	auto result = meta.Query("SELECT " + string(MAINTENANCE_COLUMNS) + " FROM {METADATA_CATALOG}." + MAINTENANCE);
	auto chunk = result->Fetch();
	vector<vector<Value>> rows;
	if (chunk && chunk->size()) {
		auto m = ReadMaintenance(*chunk);
		std::lock_guard<std::mutex> guard_runtime(owner->runtime->lock);
		auto found = owner->runtime->maintenance_errors.find(binding.Id());
		auto &active = owner->runtime->active_maintenance;
		rows.push_back(
		    {Value(m.state), Value::BOOLEAN(m.enabled), Value::BOOLEAN(active && active->catalog.Id() == binding.Id()),
		     Value(m.run), Value::UBIGINT(m.runs), Value::UBIGINT(m.max_objects), Value::UBIGINT(m.max_bytes),
		     Value::UBIGINT(m.recovered), Value::UBIGINT(m.builds), Value::UBIGINT(m.generations),
		     Value::UBIGINT(m.bytes), Value::UBIGINT(m.remaining_builds), Value::UBIGINT(m.remaining_generations),
		     Value(found == owner->runtime->maintenance_errors.end() ? m.error : found->second)});
	}
	meta.Commit();
	return rows;
}

vector<vector<Value>> DuckLakeVortexOptimizer::Status(ClientContext &context, const string &catalog) {
	auto owner = GetOwner(*context.db);
	std::lock_guard<std::mutex> guard(owner->runtime->io);
	Metadata meta(*context.db, catalog);
	Initialized(meta);
	auto result = meta.Query("SELECT " + string(COLUMNS) + " FROM {METADATA_CATALOG}." + POLICIES +
	                         " ORDER BY table_uuid,index_name");
	vector<vector<Value>> rows;
	while (auto chunk = result->Fetch()) {
		for (idx_t r = 0; r < chunk->size(); r++) {
			auto p = Read(*chunk, r, catalog);
			auto local_error = owner->runtime->Error(p.Id());
			rows.push_back({Value(p.uuid), Value(p.config.schema), Value(p.config.table), Value(p.config.index),
			                Value(p.state), Value::BOOLEAN(p.enabled), Value(p.run), Value::UBIGINT(p.expected),
			                Value(p.generation), Value::UBIGINT(p.published), Value::UBIGINT(p.t_rows),
			                Value::UBIGINT(p.t_bytes), Value::UBIGINT(p.f_rows), Value::UBIGINT(p.f_bytes),
			                Value(local_error.empty() ? p.error : local_error), Value::UBIGINT(p.runs),
			                Value::BOOLEAN(owner->runtime->IsActive(p.Id()))});
			auto budget =
			    meta.Query("SELECT " + string(BUDGET_COLUMNS) + " FROM {METADATA_CATALOG}." + WRITE_BUDGETS + Where(p));
			auto limits = budget->Fetch();
			for (idx_t col = 0; col < 4; col++) {
				rows.back().push_back(limits && limits->size() ? limits->GetValue(col, 0)
				                                               : Value(LogicalType::UBIGINT));
			}
			vector<Value> selected;
			for (auto &generation : p.selected) {
				selected.emplace_back(generation);
			}
			rows.back().push_back(Value(p.kind));
			rows.back().push_back(Value::LIST(LogicalType::VARCHAR, std::move(selected)));
			rows.back().push_back(Value::BOOLEAN(p.compaction_enabled));
			rows.back().push_back(Value::UBIGINT(p.min_deleted_rows));
			rows.back().push_back(Value::UBIGINT(p.min_deleted_percent));
			rows.back().push_back(Value(p.rewrite_candidate));
			rows.back().push_back(Value::UBIGINT(p.rewrite_deleted));
			rows.back().push_back(Value::UBIGINT(p.rewrite_remaining));
			rows.back().push_back(Value(p.rewrite_reason));
		}
	}
	meta.Commit();
	return rows;
}
} // namespace duckdb
