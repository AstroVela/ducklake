#include "storage/ducklake_vortex_optimizer.hpp"
#include "storage/ducklake_vortex_index_catalog.hpp"
#include "common/ducklake_vortex_json.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_metadata_manager.hpp"
#include "storage/ducklake_schema_entry.hpp"
#include "storage/ducklake_vortex.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/object_cache.hpp"
#include "duckdb/storage/storage_lock.hpp"

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
	Key Id() const {
		return {uuid, config.index};
	}
};

void Validate(const Policy &p);

struct Metadata {
	Connection connection;
	DuckLakeTransaction &transaction;
	static DuckLakeTransaction &Begin(Connection &connection, const string &catalog, bool require_index) {
		try {
			auto &transaction = VortexCatalog::Begin(connection, catalog);
			if (require_index) {
				VortexCatalog::Initialized(transaction);
			}
			return transaction;
		} catch (...) {
			VortexCatalog::Rollback(connection);
			throw;
		}
	}
	Metadata(DatabaseInstance &db, const string &catalog, bool require_index = true)
	    : connection(db), transaction(Begin(connection, catalog, require_index)) {
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

string Encode(const Policy &p) {
	ViewJSON json;
	auto value = json.Object();
	json.Number(value, "format_version", 1);
	json.Text(value, "schema", p.config.schema);
	json.Text(value, "table", p.config.table);
	json.Text(value, "root", p.config.root);
	json.Number(value, "dimension", p.config.dimension);
	json.Number(value, "field_id", p.field_id);
	json.Number(value, "flush_rows", p.config.flush_rows);
	json.Number(value, "flush_bytes", p.config.flush_bytes);
	json.Number(value, "index_rows", p.config.index_rows);
	json.Number(value, "index_bytes", p.config.index_bytes);
	return json.Finish(value);
}

void Decode(Policy &p, const string &bytes) {
	std::unique_ptr<yyjson_doc, decltype(&yyjson_doc_free)> doc(yyjson_read(bytes.data(), bytes.size(), 0),
	                                                            yyjson_doc_free);
	auto root = doc ? yyjson_doc_get_root(doc.get()) : nullptr;
	if (!root || !yyjson_is_obj(root) || yyjson_obj_size(root) != 10) {
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
	if (number("format_version") != 1 || number("dimension") > 4096) {
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
}

const char *COLUMNS =
    "table_uuid,index_name,config,enabled,state,run_id,expected_revision,candidate_generation,"
    "published_revision,reference_path,error,inline_rows,inline_bytes,unindexed_rows,unindexed_bytes,run_count,"
    "task_kind,selected_generations";

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
	if (!root || !yyjson_is_arr(root) || (yyjson_arr_size(root) != 0 && yyjson_arr_size(root) != 2)) {
		throw InvalidInputException("Invalid optimizer merge selection");
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
	    "UPDATE {METADATA_CATALOG}." + string(POLICIES) + " SET enabled=" + string(p.enabled ? "true" : "false") +
	    ",state=" + Quote(p.state) + ",run_id=" + Quote(p.run) + ",expected_revision=" + to_string(p.expected) +
	    ",candidate_generation=" + Quote(p.generation) + ",published_revision=" + to_string(p.published) +
	    ",reference_path=" + Quote(p.reference) + ",error=" + Quote(p.error) + ",inline_rows=" + to_string(p.t_rows) +
	    ",inline_bytes=" + to_string(p.t_bytes) + ",unindexed_rows=" + to_string(p.f_rows) +
	    ",unindexed_bytes=" + to_string(p.f_bytes) + ",run_count=" + to_string(p.runs) + ",task_kind=" + Quote(p.kind) +
	    ",selected_generations=" + Quote(Selection(p.selected)) + Where(p) + " AND run_id=" + Quote(previous_run) +
	    " RETURNING state");
	auto chunk = result->Fetch();
	if (!chunk || chunk->size() != 1) {
		throw TransactionException("Optimizer task ownership changed");
	}
	meta.Commit();
}

DuckLakeTableEntry &Table(Metadata &meta, const Policy &p) {
	auto &catalog = meta.transaction.GetCatalog();
	auto entry = catalog.GetEntry(*meta.connection.context, CatalogType::TABLE_ENTRY, p.config.schema, p.config.table,
	                              OnEntryNotFound::THROW_EXCEPTION);
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
	return table;
}

void FlushConfiguration(DuckLakeTableEntry &table) {
	auto &catalog = table.ParentCatalog().Cast<DuckLakeCatalog>();
	auto schema = table.schema.Cast<DuckLakeSchemaEntry>().GetSchemaId();
	if (DuckLakeVortex::GetFormat(table) != DataFileFormat::VORTEX ||
	    catalog.GetConfigOption<string>("auto_compact", schema, table.GetTableId(), "true") != "true") {
		throw InvalidInputException("Optimizer requires Vortex output and auto_compact=true");
	}
}

idx_t ReadyRevision(Metadata &meta, const Policy &p) {
	auto result = meta.Query("SELECT revision FROM {METADATA_CATALOG}.ducklake_vortex_indexes_v1" + Where(p));
	auto chunk = result->Fetch();
	return chunk && chunk->size() ? chunk->GetValue(0, 0).GetValue<idx_t>() : 0;
}

void Probe(Metadata &meta, Policy &p, optional_ptr<const LocalTableDataChanges> changes = nullptr,
           optional_ptr<const vector<FlushedInlinedTableInfo>> flushed = nullptr) {
	auto &table = Table(meta, p);
	FlushConfiguration(table);
	auto snapshot = meta.transaction.GetSnapshot();
	auto &manager = meta.transaction.GetMetadataManager();
	p.t_rows = p.t_bytes = p.f_rows = p.f_bytes = 0;
	p.source_rows = p.source_bytes = 0;
	auto names = meta.Query(DuckLakeMetadataManager::GetInlinedTableNamesSql(table.GetTableId()));
	while (auto chunk = names->Fetch()) {
		for (idx_t row = 0; row < chunk->size(); row++) {
			auto name = StringValue::Get(chunk->GetValue(0, row));
			string eligible = "true";
			if (changes) {
				auto deleted = changes->new_inlined_data_deletes.find(name);
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
					if (entry.inlined_table.table_name == name) {
						eligible += " AND begin_snapshot>" + to_string(entry.flush_snapshot_id);
					}
				}
			}
			auto sql = DuckLakeMetadataManager::ReadInlinedDataAggregatesSql(
			    name, "count(*) FILTER(WHERE " + eligible +
			              "),coalesce(sum(coalesce(array_length(embedding),0)*4+64+"
			              "coalesce(octet_length(encode(category)),0)+coalesce(octet_length(encode(payload)),0)) "
			              "FILTER(WHERE " +
			              eligible + "),0),count(*) FILTER(WHERE (" + eligible +
			              ") AND (embedding IS NULL OR array_length(embedding)<>" + to_string(p.config.dimension) +
			              "))");
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
	auto files = manager.GetExtendedFilesForTable(table, snapshot);
	for (auto &file : files) {
		if (file.data_type != DuckLakeDataType::DATA_FILE) {
			continue;
		}
		if (file.file.data_format != DataFileFormat::VORTEX || !file.file.encryption_key.empty()) {
			throw InvalidInputException("Optimizer requires unencrypted Vortex files");
		}
		bool replaced = false;
		if (changes) {
			for (auto &compaction : changes->compactions) {
				for (auto &source : compaction.source_files) {
					if (source.file.id == file.file_id) {
						if (covered.count(file.file_id.index)) {
							throw InvalidInputException("Vortex write budget cannot replace an indexed source");
						}
						replaced = true;
					}
				}
			}
		}
		if (replaced) {
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
}

void Probe(DatabaseInstance &db, Policy &p) {
	Metadata meta(db, p.config.catalog);
	Probe(meta, p);
	meta.Commit();
}

bool Active(const string &state) {
	return state == "flushing" || state == "building" || state == "merging" || state == "publishing" ||
	       state == "cancelling";
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

struct Runtime : public std::enable_shared_from_this<Runtime> {
	std::mutex lock, io;
	std::condition_variable changed, finished;
	bool started = false, stopping = false, done = false;
	std::thread::id worker_id;
	std::map<Key, Pending> policies;
	std::set<Key> cancelled;
	std::map<Key, string> errors;
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
		policies[p.Id()] = {p, observe};
		cancelled.erase(p.Id());
		errors.erase(p.Id());
		changed.notify_all();
	}
	void Capacity(const Policy &p) {
		std::lock_guard<std::mutex> guard(lock);
		if (policies.size() >= 16 && !policies.count(p.Id())) {
			throw InvalidInputException("Optimizer policy limit exceeded");
		}
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
	void Stage(DatabaseInstance &db, const Pending &pending, Policy &p, const string &stage) {
		if (stage == "before_build_publish") {
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
	void Finish(DatabaseInstance &db, Policy &p, const string &state, const string &error, bool owns_task) {
		std::lock_guard<std::mutex> guard(io);
		Metadata meta(db, p.config.catalog);
		auto current = Load(meta, p);
		if (current.run != p.run) {
			throw TransactionException("Optimizer task ownership changed");
		}
		bool published = owns_task && !current.generation.empty() && Published(meta, current);
		current.state = published ? "published" : state;
		current.error = error;
		if (published) {
			current.published = current.expected + 1;
		} else if (!error.empty() || state == "cancelled") {
			current.enabled = false;
		}
		Save(meta, current, p.run);
	}
	void Run(DatabaseInstance &db, Pending pending) {
		Policy p;
		{
			std::lock_guard<std::mutex> guard(io);
			Metadata meta(db, pending.policy.config.catalog);
			p = Load(meta, pending.policy);
			meta.Commit();
		}
		if (!p.enabled) {
			return;
		}
		Check(p.Id());
		auto metrics = p;
		bool owns_task = false;
		try {
			Probe(db, metrics);
			bool flush = FlushDue(metrics), build = IndexDue(metrics);
			{
				std::lock_guard<std::mutex> guard(io);
				Metadata meta(db, p.config.catalog);
				auto current = Load(meta, p);
				if (!current.enabled || current.run != p.run) {
					meta.Commit();
					return;
				}
				bool modified = current.t_rows != metrics.t_rows || current.t_bytes != metrics.t_bytes ||
				                current.f_rows != metrics.f_rows || current.f_bytes != metrics.f_bytes;
				current.t_rows = metrics.t_rows;
				current.t_bytes = metrics.t_bytes;
				current.f_rows = metrics.f_rows;
				current.f_bytes = metrics.f_bytes;
				if (flush || build) {
					current.run = UUID::ToString(UUID::GenerateRandomUUID());
					current.reference = current.config.root + "/optimizer-" + current.run + ".json";
					current.generation.clear();
					current.error.clear();
					current.expected = metrics.expected;
					current.kind = flush ? "flush" : metrics.merge_candidates.empty() ? "build" : "merge";
					current.selected = current.kind == "merge" ? metrics.merge_candidates : vector<string>();
					current.state = flush ? "flushing" : current.kind == "merge" ? "merging" : "building";
					current.runs++;
				}
				if (modified || flush || build) {
					Save(meta, current, p.run);
				} else {
					meta.Commit();
				}
				if (flush || build) {
					std::lock_guard<std::mutex> guard(lock);
					pending = policies.at(current.Id());
					active = current.Id();
					owns_task = true;
				}
				p = current;
			}
			if (!flush && !build) {
				return;
			}
			{
				Connection job(db);
				Use(job, p.Id());
				if (flush) {
					Stage(db, pending, p, "optimizer_before_flush");
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
					FlushConfiguration(entry->Cast<DuckLakeTableEntry>());
					auto result =
					    job.Query("CALL ducklake_flush_inlined_data(" + Quote(p.config.catalog) + ",schema_name=>" +
					              Quote(p.config.schema) + ",table_name=>" + Quote(p.config.table) + ")");
					if (result->HasError()) {
						result->ThrowError();
					}
					idx_t flushed = 0;
					while (auto chunk = result->Fetch()) {
						for (idx_t r = 0; r < chunk->size(); r++) {
							flushed += chunk->GetValue(2, r).GetValue<idx_t>();
						}
					}
					if (!flushed) {
						throw InvalidInputException("Optimizer flush produced no rows");
					}
					job.Commit();
					Stage(db, pending, p, "optimizer_flushed");
					metrics = p;
					Probe(db, metrics);
					build = IndexDue(metrics);
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
					Stage(db, pending, p, p.kind == "merge" ? "optimizer_before_merge" : "optimizer_before_build");
					VortexCatalog::BuildAndPublish(
					    *job.context, p.config.catalog, p.config.schema, p.config.table, p.config.index, p.reference,
					    p.config.dimension, p.expected, [&](const string &stage) { Stage(db, pending, p, stage); },
					    p.uuid, optional_idx(p.field_id),
					    p.kind == "merge" ? DuckLakeVortexIndexOperation::MERGE
					    : p.expected      ? DuckLakeVortexIndexOperation::APPEND
					                      : DuckLakeVortexIndexOperation::REPLACE,
					    p.selected);
				}
			}
			Finish(db, p, build ? "published" : "flushed", "", true);
			if (p.kind == "merge") {
				Stage(db, pending, p, "optimizer_merged");
			}
			Clear();
		} catch (const std::exception &error) {
			bool cancelled_job;
			{
				std::lock_guard<std::mutex> guard(lock);
				cancelled_job = stopping || cancelled.count(p.Id());
			}
			try {
				Finish(db, p, cancelled_job ? "cancelled" : "failed", error.what(), owns_task);
			} catch (...) {
				Clear();
				throw;
			}
			Clear();
		}
	}
	void Loop(weak_ptr<DatabaseInstance> weak) {
		{
			std::lock_guard<std::mutex> guard(lock);
			worker_id = std::this_thread::get_id();
		}
		while (true) {
			vector<Pending> work;
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
			}
			for (auto &pending : work) {
				auto db = weak.lock();
				if (!db) {
					break;
				}
				try {
					Run(*db, pending);
				} catch (const std::exception &error) {
					Clear();
					std::lock_guard<std::mutex> guard(lock);
					cancelled.insert(pending.policy.Id());
					errors[pending.policy.Id()] = error.what();
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
		}
		finished.notify_all();
	}
};

struct Owner : public ObjectCacheEntry {
	std::shared_ptr<Runtime> runtime = std::make_shared<Runtime>();
	StorageLock write_admission;
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
	if ((p.kind != "none" && p.kind != "flush" && p.kind != "build" && p.kind != "merge") ||
	    (p.kind == "merge" ? p.selected.size() != 2 || p.selected[0] == p.selected[1] : !p.selected.empty())) {
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
	auto names = meta.Query(DuckLakeMetadataManager::GetInlinedTableNamesSql(table.GetTableId()));
	while (auto chunk = names->Fetch()) {
		for (idx_t row = 0; row < chunk->size(); row++) {
			auto name = StringValue::Get(chunk->GetValue(0, row));
			string filter;
			for (auto &entry : flushed) {
				if (entry.inlined_table.table_name == name) {
					filter += " AND begin_snapshot>" + to_string(entry.flush_snapshot_id);
				}
			}
			// Flush materializes ended versions too; visible T alone underestimates build capacity.
			auto result = meta.transaction.Query(
			    snapshot, "SELECT count(*),coalesce(sum(coalesce(array_length(embedding),0)*4+64+"
			              "coalesce(octet_length(encode(category)),0)+coalesce(octet_length(encode(payload)),0)),0) "
			              "FROM {METADATA_CATALOG}." +
			                  name + " WHERE begin_snapshot<={SNAPSHOT_ID}" + filter);
			if (result->HasError()) {
				result->ThrowError();
			}
			auto values = result->Fetch();
			p.pending_rows += values->GetValue(0, 0).GetValue<idx_t>();
			p.pending_bytes += values->GetValue(1, 0).GetValue<idx_t>();
		}
	}
}

void AddWrite(Policy &p, DuckLakeTableEntry &table, const LocalTableDataChanges &changes) {
	auto file = [&](const DuckLakeDataFile &data) {
		if (data.format != DataFileFormat::VORTEX || !data.encryption_key.empty()) {
			throw InvalidInputException("Vortex write budgets require unencrypted Vortex output");
		}
		p.source_rows += data.row_count;
		p.source_bytes += data.file_size_bytes;
		p.f_rows += data.row_count;
		p.f_bytes += data.file_size_bytes;
	};
	for (auto &data : changes.new_data_files) {
		file(data);
	}
	for (auto &compaction : changes.compactions) {
		file(compaction.written_file);
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

unique_ptr<StorageLockKey> DuckLakeVortexOptimizer::AdmitWrite(DuckLakeTransaction &transaction,
                                                               ClientContext &context) {
	bool grows = false;
	for (auto &entry : transaction.GetLocalChanges().Changes()) {
		grows |= Grows(entry.GetTableChanges());
	}
	auto &catalog = transaction.GetCatalog();
	if (!grows || (!catalog.MetadataType().empty() && catalog.MetadataType() != "duckdb")) {
		return nullptr;
	}
	// Hold the reservation until metadata commit; every following writer probes fresh committed state.
	auto admission = GetOwner(*context.db)->write_admission.GetExclusiveLock();
	if (context.IsInterrupted()) {
		throw InterruptException();
	}
	Metadata meta(*context.db, catalog.GetName(), false);
	try {
		if (HasWriteBudgetTable(meta)) {
			for (auto &entry : transaction.GetLocalChanges().Changes()) {
				auto &changes = entry.GetTableChanges();
				if (!Grows(changes) || entry.GetTableIndex().IsTransactionLocal()) {
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
				Probe(meta, p, &changes, &transaction.GetFlushedInlinedTables());
				PendingInline(meta, p, transaction.GetFlushedInlinedTables());
				AddWrite(p, table, changes);
				CheckWriteBudget(p, ReadBudget(*row, 0, 2));
			}
		}
		meta.Commit();
		if (context.IsInterrupted()) {
			throw InterruptException();
		}
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
	auto owner = GetOwner(*context.db);
	auto admission = owner->write_admission.GetExclusiveLock();
	auto publication = VortexCatalog::PublicationLock().GetSharedLock();
	std::lock_guard<std::mutex> guard(owner->runtime->io);
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
		meta.Commit();
	} catch (...) {
		VortexCatalog::Rollback(meta.connection);
		throw;
	}
}

void DuckLakeVortexOptimizer::Initialize(Connection &owner, const string &catalog) {
	auto admission = GetOwner(*owner.context->db)->write_admission.GetExclusiveLock();
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
		    "PRIMARY KEY(table_uuid,index_name))");
		VortexCatalog::Query(transaction,
		                     "CREATE TABLE {METADATA_CATALOG}.ducklake_vortex_write_budgets_v1("
		                     "table_uuid VARCHAR PRIMARY KEY,table_id BIGINT NOT NULL,index_name VARCHAR NOT NULL,"
		                     "max_inline_rows BIGINT NOT NULL,max_inline_bytes BIGINT NOT NULL,"
		                     "max_unindexed_rows BIGINT NOT NULL,max_unindexed_bytes BIGINT NOT NULL)");
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
		FlushConfiguration(Table(meta, p));
		auto root = Canonical(config.root);
		auto data = meta.transaction.GetCatalog().DataPath();
		while (data.size() > 1 && data.back() == '/') {
			data.pop_back();
		}
		auto &fs = FileSystem::GetFileSystem(context);
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
		           ",true,'idle','',0,'',0,'','',0,0,0,0,0,'none','[]')");
		meta.Commit();
		owner->runtime->Add(p, observe);
	}
	owner->runtime->Start(*context.db);
}

string DuckLakeVortexOptimizer::Resume(ClientContext &context, const string &catalog, const string &schema,
                                       const string &table, const string &index,
                                       const std::function<void(const string &)> &observe) {
	auto owner = GetOwner(*context.db);
	Policy p;
	{
		std::lock_guard<std::mutex> guard(owner->runtime->io);
		Metadata meta(*context.db, catalog);
		auto identity = Identify(meta, catalog, schema, table, index);
		p = Load(meta, identity);
		Validate(p);
		FlushConfiguration(Table(meta, p));
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
	}
	owner->runtime->Cancel(p.Id());
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
		}
	}
	meta.Commit();
	return rows;
}
} // namespace duckdb
