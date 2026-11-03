// The benchmark reuses private metadata setup/decoding without adding a production test API.
// Its optimizer definitions satisfy the executable before the static extension archive is linked.
#include "../../src/storage/ducklake_vortex_optimizer.cpp"
#include "native_vortex_fixture.hpp"
#include "ducklake_extension.hpp"
#include "vortex_extension.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/planner/extension_callback.hpp"

#include <iostream>

namespace duckdb {
namespace {
using namespace vortex_test;
using BenchClock = std::chrono::steady_clock;

uint64_t BenchNow() {
	return std::chrono::duration_cast<std::chrono::nanoseconds>(BenchClock::now().time_since_epoch()).count();
}

// Frozen combined query from 80f4110ac45c555fbcd29e5dab52d0059983a339. Keep it test-only if rejected.
Maintenance BenchCombined(Metadata &meta, bool &had_backlog) {
	Initialized(meta);
	auto result = meta.Query(
	    "SELECT " + string(MAINTENANCE_COLUMNS) +
	    ",(SELECT count(*) FROM {METADATA_CATALOG}.ducklake_vortex_builds_v1),"
	    "(SELECT count(*) FROM {METADATA_CATALOG}.ducklake_vortex_generations_v1 WHERE state IN ('retired','deleting'))"
	    " FROM {METADATA_CATALOG}.ducklake_vortex_maintenance_v1");
	auto chunk = result->Fetch();
	Require(chunk && chunk->size() == 1, "Benchmark maintenance policy missing");
	auto m = ReadMaintenance(*chunk, "lake");
	had_backlog = m.remaining_builds || m.remaining_generations;
	m.remaining_builds = chunk->GetValue(13, 0).GetValue<idx_t>();
	m.remaining_generations = chunk->GetValue(14, 0).GetValue<idx_t>();
	return m;
}

struct BenchStatement {
	string sql;
	uint64_t begin = 0, planned = 0, end = 0;
};
struct BenchTrace {
	vector<BenchStatement> statements;
};
struct BenchTraceOwner {
	BenchTrace *active = nullptr;
};
struct BenchTraceGuard {
	BenchTraceOwner &owner;
	BenchTraceGuard(BenchTraceOwner &owner_p, BenchTrace &trace) : owner(owner_p) {
		owner.active = &trace;
	}
	~BenchTraceGuard() {
		owner.active = nullptr;
	}
};
struct BenchQueryState : public ClientContextState {
	explicit BenchQueryState(shared_ptr<BenchTraceOwner> owner_p) : owner(std::move(owner_p)) {
	}
	shared_ptr<BenchTraceOwner> owner;
	BenchStatement statement;
	bool CanRequestRebind() override {
		// Enables the post-plan callback only in the separately instrumented diagnostic process.
		return true;
	}
	void QueryBegin(ClientContext &context) override {
		statement = BenchStatement();
		if (!owner->active) {
			return;
		}
		auto &sql = context.GetCurrentQuery();
		bool policy = StringUtil::StartsWith(sql, "SELECT enabled,state,run_id,") &&
		              sql.find(".ducklake_vortex_maintenance_v1") != string::npos;
		bool backlog = sql.find("SELECT count(*) FROM ") != string::npos &&
		               sql.find(".ducklake_vortex_builds_v1") != string::npos &&
		               sql.find(".ducklake_vortex_generations_v1") != string::npos;
		if (policy || backlog) {
			statement.sql = sql;
			statement.begin = BenchNow();
		}
	}
	RebindQueryInfo OnFinalizePrepare(ClientContext &, PreparedStatementData &, PreparedStatementMode) override {
		if (statement.begin) {
			statement.planned = BenchNow();
		}
		return RebindQueryInfo::DO_NOT_REBIND;
	}
	void QueryEnd(ClientContext &) override {
		if (statement.begin && owner->active) {
			statement.end = BenchNow();
			owner->active->statements.push_back(statement);
		}
		statement = BenchStatement();
	}
};
struct BenchQueryCallback : public ExtensionCallback {
	explicit BenchQueryCallback(shared_ptr<BenchTraceOwner> owner_p) : owner(std::move(owner_p)) {
	}
	shared_ptr<BenchTraceOwner> owner;
	void OnConnectionOpened(ClientContext &context) override {
		context.registered_state->Insert("maintenance_probe_benchmark", make_shared_ptr<BenchQueryState>(owner));
	}
};

idx_t BenchCount(Connection &connection, const string &from) {
	return SQL(connection, "SELECT count(*) FROM " + from)->GetValue(0, 0).GetValue<idx_t>();
}

void BenchPrepare(Connection &writer, const string &root, idx_t history, bool stale) {
	auto &fs = FileSystem::GetFileSystem(*writer.context);
	fs.CreateDirectory(root + "/indexes");
	SQL(writer, "CREATE TABLE lake.items(id BIGINT,embedding FLOAT[],category VARCHAR,payload VARCHAR);"
	            "CALL ducklake_set_option('lake','experimental_data_file_format','vortex',table_name=>'items');"
	            "SELECT * FROM ducklake_initialize_vortex_indexes('lake');"
	            "SELECT * FROM ducklake_initialize_vortex_optimizer('lake');"
	            "SELECT * FROM ducklake_create_vortex_index('lake','main','items','knn'," +
	                Value(root + "/indexes").ToSQLString() +
	                ",2);"
	                "INSERT INTO lake.items SELECT i::BIGINT,[i::FLOAT,0::FLOAT],'A','benchmark' FROM range(128)t(i);"
	                "CALL ducklake_flush_inlined_data('lake',table_name=>'items')");
	auto first =
	    VortexCatalog::BuildAndPublish(*writer.context, "lake", "main", "items", "knn", root + "/indexes/a.json", 2, 1);
	VortexCatalog::BuildAndPublish(*writer.context, "lake", "main", "items", "knn", root + "/indexes/b.json", 2,
	                               first.revision);
	if (stale) {
		for (idx_t i = 0; i < 8; i++) {
			VortexCatalog::CollectBuilds(*writer.context, "lake", 2, SOURCE_LIMIT);
			VortexCatalog::Collect(*writer.context, "lake", 2, SOURCE_LIMIT);
			if (!BenchCount(writer, "__ducklake_metadata_lake.ducklake_vortex_builds_v1") &&
			    !BenchCount(writer, "__ducklake_metadata_lake.ducklake_vortex_generations_v1 WHERE state IN "
			                        "('retired','deleting')")) {
				break;
			}
		}
	}
	SQL(writer, "CREATE TABLE lake.probe_history(value BIGINT);INSERT INTO lake.probe_history VALUES(0)");
	for (idx_t i = 0; i < history; i++) {
		SQL(writer, "UPDATE lake.probe_history SET value=value+1");
	}
	DuckLakeVortexOptimizer::ConfigureSourceRetention(*writer.context, "lake", true, 2, 0);
	SQL(writer, "INSERT INTO __ducklake_metadata_lake.ducklake_vortex_maintenance_v1 "
	            "VALUES(true,true,'idle','ab-input',2,67108864,0,0,0,0,0," +
	                string(stale ? "7,9" : "0,0") + ",'')");
}

string BenchInput(Connection &writer) {
	auto rows =
	    SQL(writer, "SELECT (SELECT count(*) FROM __ducklake_metadata_lake.ducklake_snapshot),"
	                "(SELECT max(snapshot_id) FROM __ducklake_metadata_lake.ducklake_snapshot),"
	                "(SELECT count(*) FROM __ducklake_metadata_lake.ducklake_vortex_builds_v1),"
	                "(SELECT count(*) FROM __ducklake_metadata_lake.ducklake_vortex_generations_v1 WHERE state IN "
	                "('retired','deleting')),(SELECT value FROM lake.probe_history),"
	                "max_objects,max_bytes,run_id,run_count,remaining_builds,remaining_generations "
	                "FROM __ducklake_metadata_lake.ducklake_vortex_maintenance_v1");
	string result;
	for (idx_t col = 0; col < rows->ColumnCount(); col++) {
		result += (col ? "|" : "") + rows->GetValue(col, 0).ToString();
	}
	return result;
}

yyjson_mut_val *BenchMeasure(DatabaseInstance &db, bool combined, bool stale, BenchTraceOwner &trace_owner,
                             bool diagnostic, ViewJSON &json) {
	BenchTrace trace;
	BenchTraceGuard trace_guard(trace_owner, trace);
	bool had_backlog, blocked = false;
	Maintenance m;
	uint64_t opened, read, ready, committed;
	auto begin = BenchNow();
	{
		Metadata meta(db, "lake");
		opened = BenchNow();
		if (combined) {
			m = BenchCombined(meta, had_backlog);
		} else {
			m = LoadMaintenance(meta, "lake");
			had_backlog = m.remaining_builds || m.remaining_generations;
			MaintenanceBacklog(meta, m);
		}
		read = BenchNow();
		m.source_work = DuckLakeVortexSourceRetention::WorkReady(
		    meta.transaction, m.max_bytes, [] {}, &blocked);
		ready = BenchNow();
		meta.Commit();
		committed = BenchNow();
	}
	auto end = BenchNow();
	Require(m.enabled && m.state == "idle" && m.run == "ab-input" && m.runs == 0 && m.max_objects == 2 &&
	            m.max_bytes == SOURCE_LIMIT && had_backlog == stale && m.remaining_builds == (stale ? 0 : 2) &&
	            m.remaining_generations == (stale ? 0 : 1) && blocked && !m.source_work,
	        "Probe changed its task inputs, budget, live counts or ordinary-transaction deferral");
	Require(!diagnostic || trace.statements.size() == (combined ? 1 : 2), "Unexpected diagnostic query count");
	auto sample = json.Object();
	json.Number(sample, "metadata_open_ns", opened - begin);
	json.Number(sample, "policy_backlog_ns", read - opened);
	json.Number(sample, "source_readiness_ns", ready - read);
	json.Number(sample, "commit_ns", committed - ready);
	json.Number(sample, "connection_close_ns", end - committed);
	json.Number(sample, "complete_read_probe_ns", end - begin);
	auto queries = json.Array();
	for (auto &statement : trace.statements) {
		Require(statement.planned >= statement.begin && statement.end >= statement.planned,
		        "Query callback timing is incomplete");
		auto query = json.Object();
		json.Text(query, "sql", statement.sql);
		json.Number(query, "bind_plan_ns", statement.planned - statement.begin);
		json.Number(query, "execute_fetch_ns", statement.end - statement.planned);
		json.Number(query, "query_scope_ns", statement.end - statement.begin);
		yyjson_mut_arr_append(queries, query);
	}
	json.Add(sample, "queries", queries);
	return sample;
}

void BenchRun(DuckDB &database, Connection &writer, const string &root, idx_t rounds, idx_t batch, bool diagnostic,
              bool stale) {
	auto trace_owner = make_shared_ptr<BenchTraceOwner>();
	if (diagnostic) {
		ExtensionCallbackManager::Get(*database.instance).Register(make_shared_ptr<BenchQueryCallback>(trace_owner));
	}
	auto input = BenchInput(writer);
	Connection held(database);
	held.BeginTransaction();
	SQL(held, "SELECT count(*) FROM lake.items");
	ViewJSON json;
	auto report = json.Object();
	json.Text(report, "input_before", input);
	json.Text(report, "task", stale ? "stale" : "live");
	json.Add(report, "diagnostic", yyjson_mut_bool(json.doc.get(), diagnostic));
	json.Number(report, "rounds", rounds);
	json.Number(report, "batch", batch);
	for (idx_t i = 0; i < 12; i++) {
		ViewJSON warm;
		BenchMeasure(*database.instance, i % 2, stale, *trace_owner, diagnostic, warm);
	}
	auto samples = json.Array();
	for (idx_t round = 0; round < rounds; round++) {
		for (idx_t block = 0; block < 4; block++) {
			bool combined = (block == 1 || block == 2) != bool(round % 2);
			for (idx_t repeat = 0; repeat < batch; repeat++) {
				auto sample = BenchMeasure(*database.instance, combined, stale, *trace_owner, diagnostic, json);
				json.Number(sample, "round", round);
				json.Number(sample, "block", block);
				json.Number(sample, "repeat", repeat);
				json.Text(sample, "variant", combined ? "combined" : "split");
				yyjson_mut_arr_append(samples, sample);
			}
		}
	}
	json.Add(report, "samples", samples);
	held.Rollback();
	auto after = BenchInput(writer);
	Require(input == after, "Read-only benchmark mutated metadata or task inputs");
	json.Text(report, "input_after", after);
	Write(root + (diagnostic ? "/diagnostic.json" : "/timing.json"), json.Finish(report));
}
} // namespace
} // namespace duckdb

int main(int argc, char **argv) {
	using namespace duckdb;
	using namespace duckdb::vortex_test;
	try {
		Require(argc == 8, "Usage: benchmark ROOT prepare|run HISTORY live|stale ROUNDS BATCH DIAGNOSTIC");
		string root = argv[1], mode = argv[2], task = argv[4];
		auto history = std::stoull(argv[3]), rounds = std::stoull(argv[5]), batch = std::stoull(argv[6]);
		bool diagnostic = string(argv[7]) == "1";
		Require((mode == "prepare" || mode == "run") && (task == "live" || task == "stale") && history <= 10000 &&
		            rounds && rounds <= 128 && batch && batch <= 100,
		        "Invalid benchmark parameters");
		DuckDB database(nullptr);
		database.LoadStaticExtension<DucklakeExtension>();
		database.LoadStaticExtension<VortexExtension>();
		Connection writer(database);
		SQL(writer, "SET threads=2; LOAD parquet; ATTACH " +
		                Value("ducklake:" + root + "/metadata.ducklake").ToSQLString() + " AS lake(DATA_PATH " +
		                Value(root + "/data").ToSQLString() + ",DATA_INLINING_ROW_LIMIT 1000)");
		if (mode == "prepare") {
			BenchPrepare(writer, root, history, task == "stale");
			std::cout << BenchInput(writer) << std::endl;
		} else {
			BenchRun(database, writer, root, rounds, batch, diagnostic, task == "stale");
		}
		return 0;
	} catch (std::exception &error) {
		std::cerr << error.what() << std::endl;
		return 1;
	}
}
