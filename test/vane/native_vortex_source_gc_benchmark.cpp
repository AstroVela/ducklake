#include "native_vortex_fixture.hpp"
#include "common/ducklake_vortex_json.hpp"
#include "ducklake_extension.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/planner/extension_callback.hpp"
#include "storage/ducklake_vortex_index_catalog.hpp"
#include "storage/ducklake_vortex_optimizer.hpp"
#include "storage/ducklake_vortex_profile.hpp"
#include "storage/ducklake_vortex_source_retention.hpp"
#include "vortex_extension.hpp"

#include <iostream>
#include <thread>

#ifdef DUCKLAKE_REFERENCE_DIAGNOSTIC
#include "native_vortex_reference_trace.hpp"
#include "storage/ducklake_metadata_manager.hpp"
#include "storage/ducklake_transaction.hpp"
#endif

using namespace duckdb;
using namespace duckdb::vortex_test;
using namespace duckdb::vortex_detail;

namespace {
using Source = DuckLakeVortexSourceRetention;
using Index = DuckLakeVortexIndex;
constexpr idx_t BYTE_BUDGET = 64 * 1024 * 1024;
constexpr const char *META = "__ducklake_metadata_lake.";

uint64_t Now() {
	return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
	    .count();
}

idx_t Scalar(Connection &connection, const string &sql) {
	return SQL(connection, sql)->GetValue(0, 0).GetValue<idx_t>();
}

string Rows(Connection &connection, const string &sql) {
	auto result = SQL(connection, sql);
	string rows;
	for (idx_t row = 0; row < result->RowCount(); row++) {
		for (idx_t column = 0; column < result->ColumnCount(); column++) {
			auto value = result->GetValue(column, row).ToSQLString();
			rows += to_string(value.size()) + ":" + value;
		}
	}
	return rows;
}

struct State {
	vector<idx_t> snapshots;
	idx_t scheduled, intents, expired, files, bytes, data_files, delete_files;
	string business, index, queue;
	State(Connection &connection) {
		auto result =
		    SQL(connection, "SELECT snapshot_id FROM " + string(META) + "ducklake_snapshot ORDER BY snapshot_id");
		for (idx_t row = 0; row < result->RowCount(); row++) {
			snapshots.push_back(result->GetValue(0, row).GetValue<idx_t>());
		}
		scheduled =
		    Scalar(connection, "SELECT count(*) FROM " + string(META) + "ducklake_files_scheduled_for_deletion");
		intents = Scalar(connection, "SELECT count(*) FROM " + string(META) + "ducklake_vortex_source_deletions_v1");
		data_files = Scalar(connection, "SELECT count(*) FROM " + string(META) + "ducklake_data_file");
		delete_files = Scalar(connection, "SELECT count(*) FROM " + string(META) + "ducklake_delete_file");
		auto counts = SQL(connection, "SELECT expired_snapshots,collected_files,collected_bytes FROM " + string(META) +
		                                  "ducklake_vortex_source_retention_v1");
		expired = counts->GetValue(0, 0).GetValue<idx_t>();
		files = counts->GetValue(1, 0).GetValue<idx_t>();
		bytes = counts->GetValue(2, 0).GetValue<idx_t>();
		business = Rows(connection, "SELECT id,embedding,category,payload FROM lake.items ORDER BY id");
		index = Rows(connection, "SELECT * FROM " + string(META) + "ducklake_vortex_indexes_v1");
		queue = Rows(connection, "SELECT * FROM " + string(META) +
		                             "ducklake_files_scheduled_for_deletion ORDER BY data_file_id,path");
	}
	bool operator==(const State &other) const {
		return snapshots == other.snapshots && scheduled == other.scheduled && intents == other.intents &&
		       expired == other.expired && files == other.files && bytes == other.bytes && business == other.business &&
		       index == other.index && queue == other.queue && data_files == other.data_files &&
		       delete_files == other.delete_files;
	}
	yyjson_mut_val *JSON(ViewJSON &json) const {
		auto value = json.Object();
		json.Number(value, "snapshots", snapshots.size());
		json.Number(value, "first_snapshot", snapshots.front());
		json.Number(value, "last_snapshot", snapshots.back());
		json.Number(value, "scheduled", scheduled);
		json.Number(value, "intents", intents);
		json.Number(value, "data_files", data_files);
		json.Number(value, "delete_files", delete_files);
		json.Number(value, "expired_total", expired);
		json.Number(value, "files_total", files);
		json.Number(value, "bytes_total", bytes);
		return value;
	}
};

struct Statement {
	string sql;
	uint64_t begin = 0, planned = 0, end = 0;
	idx_t connection = 0, depth = 0;
};
struct Trace {
	bool active = false;
	idx_t depth = 0;
	vector<Statement> queries;
	vector<std::pair<string, uint64_t>> checkpoints;
};
struct QueryState : public ClientContextState {
	explicit QueryState(shared_ptr<Trace> trace_p) : trace(std::move(trace_p)) {
	}
	shared_ptr<Trace> trace;
	Statement statement;
	bool CanRequestRebind() override {
		return true;
	}
	void QueryBegin(ClientContext &context) override {
		statement = Statement();
		if (trace->active) {
			statement.sql = context.GetCurrentQuery();
			statement.connection = context.GetConnectionId();
			statement.depth = trace->depth++;
			statement.begin = Now();
		}
	}
	RebindQueryInfo OnFinalizePrepare(ClientContext &, PreparedStatementData &, PreparedStatementMode) override {
		if (statement.begin) {
			statement.planned = Now();
		}
		return RebindQueryInfo::DO_NOT_REBIND;
	}
	void QueryEnd(ClientContext &) override {
		if (statement.begin && trace->active) {
			statement.end = Now();
			Require(trace->depth == statement.depth + 1, "Diagnostic SQL callbacks did not unwind in order");
			trace->depth--;
			trace->queries.push_back(std::move(statement));
		}
		statement = Statement();
	}
};
struct QueryCallback : public ExtensionCallback {
	explicit QueryCallback(shared_ptr<Trace> trace_p) : trace(std::move(trace_p)) {
	}
	shared_ptr<Trace> trace;
	void OnConnectionOpened(ClientContext &context) override {
		context.registered_state->Insert("source_gc_benchmark", make_shared_ptr<QueryState>(trace));
	}
};

void CheckBusiness(Connection &connection, idx_t history) {
	auto rows = SQL(connection, "SELECT id,embedding,category,payload FROM lake.items ORDER BY id");
	Require(rows->RowCount() == 96, "GC changed current business row count");
	for (idx_t row = 0; row < 96; row++) {
		auto id = row + 16 + (row >= 48 ? 16 : 0);
		Require(rows->GetValue(0, row).GetValue<int64_t>() == int64_t(id) &&
		            rows->GetValue(1, row) ==
		                Value::LIST(LogicalType::FLOAT, {Value::FLOAT(float(id)), Value::FLOAT(0)}) &&
		            rows->GetValue(2, row).ToString() == "A" && rows->GetValue(3, row).ToString() == "benchmark",
		        "GC changed typed business row contents");
	}
	Require(Scalar(connection, "SELECT value FROM lake.gc_history") == history, "GC changed the history table");
	auto hits = SQL(connection, "SELECT id FROM ducklake_vortex_search('lake','main','items','knn',[0,0]::FLOAT[],1)");
	Require(hits->RowCount() == 1 && hits->GetValue(0, 0).GetValue<int64_t>() == 16, "Current named ANN query changed");
}

void Prepare(Connection &writer, const string &root, idx_t history, const string &task, const string &storage) {
	FileSystem::GetFileSystem(*writer.context).CreateDirectory(root + "/indexes");
	SQL(writer, "CREATE TABLE lake.items(id BIGINT,embedding FLOAT[],category VARCHAR,payload VARCHAR);"
	            "CALL ducklake_set_option('lake','experimental_data_file_format','vortex',table_name=>'items');"
	            "SELECT * FROM ducklake_initialize_vortex_indexes('lake');"
	            "SELECT * FROM ducklake_initialize_vortex_optimizer('lake');"
	            "SELECT * FROM ducklake_initialize_vortex_writes('lake');"
	            "SELECT * FROM ducklake_create_vortex_index('lake','main','items','knn'," +
	                Value(root + "/indexes").ToSQLString() + ",2)");
	for (idx_t first : {0, 64}) {
		SQL(writer, "INSERT INTO lake.items SELECT i,[i::FLOAT,0::FLOAT],'A','benchmark' FROM range(" +
		                to_string(first) + "," + to_string(first + 64) +
		                ")t(i);"
		                "CALL ducklake_flush_inlined_data('lake',table_name=>'items')");
	}
	auto index =
	    Index::BuildAndPublish(*writer.context, "lake", "main", "items", "knn", root + "/indexes/seed.json", 2, 1);
	Require(index.files.size() == 2, "Benchmark requires two initial physical sources");
	SQL(writer,
	    "SELECT * FROM ducklake_enable_vortex_optimizer('lake','main','items','knn'," +
	        Value(root + "/indexes").ToSQLString() +
	        ",2,100000,4194304,100000,67108864);"
	        "SELECT * FROM ducklake_cancel_vortex_optimizer('lake','main','items','knn');"
	        "SELECT * FROM ducklake_set_vortex_write_budget('lake','main','items','knn',1000,4194304,4096,67108864)");
	auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
	while (DuckLakeVortexOptimizer::Status(*writer.context, "lake")[0][16].GetValue<bool>()) {
		Require(std::chrono::steady_clock::now() < deadline, "Preparation optimizer did not stop");
		std::this_thread::sleep_for(std::chrono::milliseconds(5));
	}
	SQL(writer, "DELETE FROM lake.items WHERE id<16 OR (id>=64 AND id<80)");
	Index::Rewrite(*writer.context, "lake", "main", "items", "knn", index.segments[0].generation,
	               root + "/indexes/rewrite.json", index.revision);
	for (idx_t i = 0; i < 16; i++) {
		Index::CollectBuilds(*writer.context, "lake", 2, BYTE_BUDGET);
		Index::Collect(*writer.context, "lake", 2, BYTE_BUDGET);
		if (!Scalar(writer, "SELECT count(*) FROM " + string(META) + "ducklake_vortex_builds_v1") &&
		    !Scalar(writer, "SELECT count(*) FROM " + string(META) +
		                        "ducklake_vortex_generations_v1 WHERE state IN ('retired','deleting')")) {
			break;
		}
	}
	Require(!Scalar(writer, "SELECT count(*) FROM " + string(META) + "ducklake_vortex_builds_v1") &&
	            !Scalar(writer, "SELECT count(*) FROM " + string(META) +
	                                "ducklake_vortex_generations_v1 WHERE state IN ('retired','deleting')"),
	        "Restorable input retains unrelated build or retired-generation work");
	auto end = Scalar(writer, "SELECT min(end_snapshot) FROM " + string(META) +
	                              "ducklake_data_file WHERE end_snapshot IS NOT NULL");
	Require(end >= 2 && end == Scalar(writer, "SELECT max(end_snapshot) FROM " + string(META) +
	                                              "ducklake_data_file WHERE end_snapshot IS NOT NULL"),
	        "Benchmark sources must retire together");
	SQL(writer, "CREATE TABLE lake.gc_history(value BIGINT)");
	if (storage == "file") {
		SQL(writer, "CALL ducklake_set_option('lake','data_inlining_row_limit',0,table_name=>'gc_history')");
	}
	SQL(writer, "INSERT INTO lake.gc_history VALUES(0)");
	for (idx_t i = 0; i < history; i++) {
		SQL(writer, "UPDATE lake.gc_history SET value=value+1");
	}
	Source::Configure(*writer.context, "lake", true, 2, 0);
	auto prefix = Scalar(writer, "SELECT count(*) FROM " + string(META) + "ducklake_snapshot WHERE snapshot_id<" +
	                                 to_string(end - 2));
	Require(prefix && prefix <= 16, "Unexpected preparation snapshot prefix");
	auto trimmed = Source::Collect(*writer.context, "lake", prefix, BYTE_BUDGET);
	Require(!trimmed.blocked && trimmed.expired == prefix && !trimmed.files && State(writer).scheduled == 0,
	        "Preparation did not retain the last two snapshots protecting old sources");
	if (task == "files") {
		auto expired = Source::Collect(*writer.context, "lake", 2, BYTE_BUDGET);
		Require(!expired.blocked && expired.expired == 2 && !expired.files && State(writer).scheduled >= 2,
		        "File task requires native expiry to schedule its sources");
	}
	CheckBusiness(writer, history);
	State input(writer);
	Require(input.intents == 0 && (task == "files" ? input.scheduled >= 2 : input.scheduled == 0),
	        "Restorable input must have no inode-bound deletion intents");
	Require(input.data_files == (task == "expire" ? 3 : 1) + (storage == "file" ? history + 1 : 0),
	        "History storage did not produce the expected native file inventory");
	ViewJSON json;
	auto value = input.JSON(json);
	json.Number(value, "history", history);
	json.Text(value, "task", task);
	json.Text(value, "history_storage", storage);
	Write(root + "/input.json", json.Finish(value));
}

#ifdef DUCKLAKE_REFERENCE_DIAGNOSTIC
void ReferenceOracle(DuckDB &database, Connection &writer, const string &root) {
	State before(writer);
	Connection owner(database);
	auto &transaction = Index::Begin(owner, "lake");
	auto known = Index::Query(
	    transaction, "SELECT f.data_file_id,s.path||t.path||f.path FROM {METADATA_CATALOG}.ducklake_data_file f "
	                 "JOIN {METADATA_CATALOG}.ducklake_table t ON f.table_id=t.table_id "
	                 "JOIN {METADATA_CATALOG}.ducklake_schema s ON t.schema_id=s.schema_id "
	                 "WHERE f.path_is_relative AND t.path_is_relative AND s.path_is_relative "
	                 "ORDER BY f.data_file_id LIMIT 1");
	auto row = known->Fetch();
	Require(row && row->size() == 1, "Reference oracle requires a relative live source");
	auto id = row->GetValue(0, 0).GetValue<idx_t>();
	auto path = FileSystem::GetFileSystem(*writer.context)
	                .FileSystem::CanonicalizePath(root + "/data/" + row->GetValue(1, 0).ToString());
	Require(FileSystem::GetFileSystem(*writer.context).FileExists(path), "Reference oracle source is missing");
	reference_trace::Start();
	auto begin = Now();
	auto &metadata = transaction.GetMetadataManager();
	Require(metadata.HasFileReference(path, "/"), "Reference oracle lost a live source");
	Index::Query(transaction, "UPDATE {METADATA_CATALOG}.ducklake_data_file SET path='./'||path WHERE data_file_id=" +
	                              to_string(id));
	Require(metadata.HasFileReference(path, "/"), "Reference oracle lost a canonical path alias");
	Require(!metadata.HasFileReference(root + "/data/absent-reference-oracle", "/"),
	        "Reference oracle accepted an absent source");
	Index::Query(transaction,
	             "ALTER TABLE {METADATA_CATALOG}.ducklake_data_file RENAME TO reference_oracle_hidden_file");
	bool raised = false;
	try {
		metadata.HasFileReference(path, "/");
	} catch (Exception &) {
		raised = true;
	}
	auto end = Now();
	reference_trace::Stop();
	owner.Rollback();
	Require(raised && reference_trace::Get().samples.size() == 4 && !reference_trace::Get().samples.back().completed,
	        "Reference oracle swallowed the query error");
	Require(State(writer) == before, "Rolled-back reference oracle changed the input");
	ViewJSON json;
	auto report = json.Object();
	json.Text(report, "status", "passed");
	json.Number(report, "cases", 4);
	reference_trace::Add(json, report, begin, end, false);
	Write(root + "/reference-oracle.json", json.Finish(report));
}
#endif

void Measure(DuckDB &database, Connection &writer, const string &root, idx_t history, const string &task,
             const string &storage, bool diagnostic, const shared_ptr<Trace> &trace) {
	CheckBusiness(writer, history);
	State before(writer);
	Require(before.snapshots.size() > 4 && before.intents == 0 &&
	            (task == "files" ? before.scheduled >= 2 : before.scheduled == 0),
	        "Invalid restored task input");
	{
		Connection held(database);
		held.BeginTransaction();
		SQL(held, "SELECT count(*) FROM lake.items");
		auto deferred = Source::Collect(*writer.context, "lake", 2, BYTE_BUDGET);
		Require(deferred.blocked && !deferred.expired && !deferred.files && !deferred.bytes,
		        "Ordinary transaction no longer blocks source GC");
		held.Rollback();
	}
	Require(State(writer) == before, "Deferred collection changed its input");
	vector<string> target_paths;
	if (task == "files") {
		auto paths = SQL(writer, "SELECT path,path_is_relative FROM " + string(META) +
		                             "ducklake_files_scheduled_for_deletion ORDER BY data_file_id,path LIMIT 2");
		for (idx_t row = 0; row < paths->RowCount(); row++) {
			auto path = paths->GetValue(0, row).ToString();
			if (paths->GetValue(1, row).GetValue<bool>()) {
				path = root + "/data/" + path;
			}
			Require(FileSystem::GetFileSystem(*writer.context).FileExists(path), "Scheduled input file is missing");
			target_paths.push_back(std::move(path));
		}
	}
	DuckLakeVortexMaintenanceProfile profile("source-gc-benchmark");
	std::function<void(const string &)> observe;
	if (diagnostic) {
		observe = [&](const string &stage) {
			trace->checkpoints.emplace_back(stage, Now());
		};
	}
	trace->active = diagnostic;
#ifdef DUCKLAKE_REFERENCE_DIAGNOSTIC
	reference_trace::Start();
#endif
	auto begin = Now();
	auto collected = Source::Collect(*writer.context, "lake", 2, BYTE_BUDGET, observe, &profile);
	auto end = Now();
#ifdef DUCKLAKE_REFERENCE_DIAGNOSTIC
	reference_trace::Stop();
#endif
	trace->active = false;
	Require(trace->depth == 0, "Diagnostic SQL callback was not completed");
	State after(writer);
	Require(!collected.blocked && collected.bytes <= BYTE_BUDGET && after.intents == 0 &&
	            before.business == after.business && before.index == after.index &&
	            after.expired == before.expired + collected.expired && after.files == before.files + collected.files &&
	            after.bytes == before.bytes + collected.bytes,
	        "GC changed logical/index state or violated its intent, counter or byte contract");
	if (task == "expire") {
		auto remaining = before.snapshots;
		remaining.erase(remaining.begin(), remaining.begin() + 2);
		Require(collected.expired == 2 && !collected.files && !collected.bytes && after.snapshots == remaining &&
		            after.scheduled >= 2,
		        "Expiry task did not expire exactly its two oldest snapshots and schedule old sources");
	} else {
		Require(collected.files == 2 && !collected.expired && collected.bytes && before.snapshots == after.snapshots &&
		            after.scheduled + 2 == before.scheduled,
		        "File task did not complete exactly two native deletions");
	}
	CheckBusiness(writer, history);
	ViewJSON json;
	auto report = json.Object();
	json.Text(report, "task", task);
	json.Number(report, "history", history);
	json.Text(report, "history_storage", storage);
	json.Add(report, "diagnostic", yyjson_mut_bool(json.doc.get(), diagnostic));
	json.Add(report, "before", before.JSON(json));
	json.Add(report, "after", after.JSON(json));
	json.Number(report, "complete_source_gc_ns", end - begin);
	json.Number(report, "expired", collected.expired);
	json.Number(report, "files", collected.files);
	json.Number(report, "bytes", collected.bytes);
	auto targets = json.Array();
	for (auto &path : target_paths) {
		Require(!FileSystem::GetFileSystem(*writer.context).FileExists(path),
		        "Completed source deletion left its file");
		yyjson_mut_arr_append(targets, yyjson_mut_strcpy(json.doc.get(), path.c_str()));
	}
	json.Add(report, "deleted_paths", targets);
	auto phases = json.Array();
	uint64_t accounted = 0;
	auto samples = profile.Snapshot();
	vector<string> names {"wait", "metadata", "expire", "artifact", "unlink"};
	for (idx_t i = 0; i < names.size(); i++) {
		auto &sample = samples[idx_t(DuckLakeVortexPhase::SOURCE_WAIT) + i];
		auto phase = json.Object();
		json.Text(phase, "stage", names[i]);
		json.Number(phase, "calls", sample.calls);
		json.Number(phase, "total_ns", sample.total_ns);
		accounted += sample.total_ns;
		yyjson_mut_arr_append(phases, phase);
	}
	Require(accounted <= end - begin, "Source phase times overlap or exceed the measured collector");
	json.Add(report, "phases", phases);
	auto queries = json.Array();
	for (auto &statement : trace->queries) {
		Require(statement.end >= statement.begin, "Incomplete diagnostic query");
		auto query = json.Object();
		json.Text(query, "sql", statement.sql);
		json.Number(query, "connection_id", statement.connection);
		json.Number(query, "depth", statement.depth);
		json.Number(query, "begin_ns", statement.begin - begin);
		json.Number(query, "end_ns", statement.end - begin);
		json.Number(query, "query_ns", statement.end - statement.begin);
		if (statement.planned) {
			Require(statement.planned >= statement.begin && statement.end >= statement.planned,
			        "Incomplete diagnostic planning callback");
			json.Number(query, "bind_plan_ns", statement.planned - statement.begin);
			json.Number(query, "execute_fetch_ns", statement.end - statement.planned);
		}
		yyjson_mut_arr_append(queries, query);
	}
	Require(!diagnostic || !trace->queries.empty(), "Diagnostic SQL callbacks did not observe collection");
	json.Add(report, "queries", queries);
	auto checkpoints = json.Array();
	for (auto &point : trace->checkpoints) {
		auto checkpoint = json.Object();
		json.Text(checkpoint, "stage", point.first);
		json.Number(checkpoint, "elapsed_ns", point.second - begin);
		yyjson_mut_arr_append(checkpoints, checkpoint);
	}
	json.Add(report, "checkpoints", checkpoints);
#ifdef DUCKLAKE_REFERENCE_DIAGNOSTIC
	for (auto &call : reference_trace::Get().samples) {
		Require(call.rows == before.data_files + before.delete_files && call.chunks,
		        "Reference diagnostic did not scan the full native inventory");
		idx_t matches = 0;
		for (auto &statement : trace->queries) {
			if (statement.depth || statement.begin >= call.end || statement.end <= call.begin) {
				continue;
			}
			Require(statement.begin >= call.begin && statement.end <= call.end &&
			            statement.sql.find("SELECT REPLACE(") == 0 && statement.connection == call.connection &&
			            statement.end - statement.begin + call.fetch_ns + call.path_loop_ns <= call.end - call.begin,
			        "Reference call does not contain its complete metadata SQL scope");
			matches++;
		}
		Require(matches == 1, "Reference call must contain exactly one known-source SQL query");
	}
	reference_trace::Add(json, report, begin, end, true);
#endif
	Write(root + "/sample.json", json.Finish(report));
}
} // namespace

int main(int argc, char **argv) {
	try {
		Require(argc == 7, "Usage: benchmark ROOT prepare|run HISTORY expire|files DIAGNOSTIC inline|file");
		string root = argv[1], mode = argv[2], task = argv[4], storage = argv[6];
		auto history = std::stoull(argv[3]);
		bool valid_mode = mode == "prepare" || mode == "run";
#ifdef DUCKLAKE_REFERENCE_DIAGNOSTIC
		valid_mode = mode == "run" || mode == "reference-oracle";
		Require(task == "files" && string(argv[5]) == "1", "Reference driver requires diagnostic file collection");
#endif
		Require(valid_mode && (task == "expire" || task == "files") && history >= 8 && history <= 10000 &&
		            (string(argv[5]) == "0" || string(argv[5]) == "1") && (storage == "inline" || storage == "file"),
		        "Invalid source GC benchmark parameters");
		bool diagnostic = string(argv[5]) == "1";
		DuckDB database(nullptr);
		database.LoadStaticExtension<DucklakeExtension>();
		database.LoadStaticExtension<VortexExtension>();
		auto trace = make_shared_ptr<Trace>();
		if (diagnostic) {
			ExtensionCallbackManager::Get(*database.instance).Register(make_shared_ptr<QueryCallback>(trace));
		}
		Connection writer(database);
		SQL(writer, "SET threads=2; LOAD parquet; ATTACH " +
		                Value("ducklake:" + root + "/metadata.ducklake").ToSQLString() + " AS lake(DATA_PATH " +
		                Value(root + "/data").ToSQLString() + ",DATA_INLINING_ROW_LIMIT 1000)");
#ifdef DUCKLAKE_REFERENCE_DIAGNOSTIC
		const string base_extensions = "'core_functions','file','httpfs','icu','json','parquet'";
		Require(Scalar(writer, "SELECT count(*) FROM duckdb_extensions() WHERE install_mode='STATICALLY_LINKED' "
		                       "AND extension_name IN (" +
		                           base_extensions + ")") == 6 &&
		            !Scalar(writer, "SELECT count(*) FROM duckdb_extensions() WHERE install_mode='STATICALLY_LINKED' "
		                            "AND extension_name NOT IN (" +
		                                base_extensions + ",'ducklake','vortex')"),
		        "Reference driver requires the default base extension profile; use DISABLE_EXTENSIONS_FOR_TEST=1");
		if (mode == "reference-oracle") {
			ReferenceOracle(database, writer, root);
			return 0;
		}
#endif
		if (mode == "prepare") {
			Prepare(writer, root, history, task, storage);
		} else {
			Measure(database, writer, root, history, task, storage, diagnostic, trace);
		}
		return 0;
	} catch (std::exception &error) {
		std::cerr << error.what() << std::endl;
		return 1;
	}
}
