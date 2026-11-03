#include "native_vortex_fixture.hpp"
#include "common/ducklake_vortex_json.hpp"
#include "ducklake_extension.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/planner/extension_callback.hpp"
#include "duckdb/storage/storage_lock.hpp"
#include "storage/ducklake_vortex_index_catalog.hpp"
#include "storage/ducklake_vortex_optimizer.hpp"
#include "storage/ducklake_vortex_search.hpp"
#include "storage/ducklake_vortex_source_retention.hpp"
#include "storage/ducklake_vortex_write.hpp"
#include "storage/ducklake_transaction_manager.hpp"
#include "vortex_extension.hpp"

#include <atomic>
#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <thread>
#include <unistd.h>

using namespace duckdb;
using namespace duckdb::vortex_test;
using Source = DuckLakeVortexSourceRetention;
using VortexIndex = DuckLakeVortexIndex;

namespace {
Pause *status_pause = nullptr;
std::atomic<bool> pause_status {false};
std::atomic<idx_t> probe_revision_reads {0};
int64_t ProbeRevision(int64_t value) {
	probe_revision_reads++;
	return value;
}

struct MaintenanceProbeLog {
	std::thread::id caller = std::this_thread::get_id();
	std::mutex lock;
	optional_idx connection;
	vector<string> statements;
	void Record(ClientContext &context) {
		if (std::this_thread::get_id() == caller) {
			return;
		}
		auto &sql = context.GetCurrentQuery();
		bool policy = StringUtil::StartsWith(sql, "SELECT enabled,state,run_id,") &&
		              sql.find(".ducklake_vortex_maintenance_v1") != string::npos;
		bool backlog = sql.find("SELECT count(*) FROM ") != string::npos &&
		               sql.find(".ducklake_vortex_builds_v1") != string::npos &&
		               sql.find(".ducklake_vortex_generations_v1") != string::npos;
		if (!policy && !backlog) {
			return;
		}
		std::lock_guard<std::mutex> guard(lock);
		if (!connection.IsValid()) {
			connection = context.GetConnectionId();
		}
		if (connection.GetIndex() == context.GetConnectionId()) {
			statements.push_back(sql);
		}
	}
	vector<string> Snapshot() {
		std::lock_guard<std::mutex> guard(lock);
		return statements;
	}
};

struct MaintenanceQueryState : public ClientContextState {
	explicit MaintenanceQueryState(shared_ptr<MaintenanceProbeLog> log_p) : log(std::move(log_p)) {
	}
	void QueryBegin(ClientContext &context) override {
		log->Record(context);
	}
	shared_ptr<MaintenanceProbeLog> log;
};

struct MaintenanceQueryCallback : public ExtensionCallback {
	explicit MaintenanceQueryCallback(shared_ptr<MaintenanceProbeLog> log_p) : log(std::move(log_p)) {
	}
	void OnConnectionOpened(ClientContext &context) override {
		context.registered_state->Insert("maintenance_probe_queries", make_shared_ptr<MaintenanceQueryState>(log));
	}
	shared_ptr<MaintenanceProbeLog> log;
};
bool StatusEnabled(bool enabled) {
	if (pause_status.exchange(false)) {
		status_pause->Stop();
	}
	return enabled;
}
string Read(const string &path) {
	std::ifstream input(path, std::ios::binary);
	Require(bool(input), "Cannot read source fixture file");
	return string(std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>());
}

struct Fixture {
	string root, old_source;
	DuckDB database;
	Connection writer;
	DuckLakeVortexIndex index;
	Fixture(const string &root_p) : root(root_p), database(nullptr), writer(database) {
		database.LoadStaticExtension<DucklakeExtension>();
		database.LoadStaticExtension<VortexExtension>();
		SQL(writer, "SET threads=2; LOAD parquet; ATTACH " +
		                Value("ducklake:" + root + "/metadata.ducklake").ToSQLString() + " AS lake(DATA_PATH " +
		                Value(root + "/data").ToSQLString() + ",DATA_INLINING_ROW_LIMIT 1000)");
		FileSystem::GetFileSystem(*writer.context).CreateDirectory(root + "/indexes");
		SQL(writer, "CREATE TABLE lake.items(id BIGINT,embedding FLOAT[],category VARCHAR,payload VARCHAR);"
		            "CALL ducklake_set_option('lake','experimental_data_file_format','vortex',table_name=>'items');"
		            "SELECT * FROM ducklake_initialize_vortex_indexes('lake');"
		            "SELECT * FROM ducklake_initialize_vortex_optimizer('lake');"
		            "SELECT * FROM ducklake_initialize_vortex_writes('lake');"
		            "SELECT * FROM ducklake_create_vortex_index('lake','main','items','knn'," +
		                Value(root + "/indexes").ToSQLString() + ",2)");
	}
	Value Rows(idx_t first) {
		return SQL(writer, "SELECT list({'id':i::BIGINT,'embedding':[i::FLOAT,0::FLOAT],'category':'A'::VARCHAR,"
		                   "'payload':'原始向量'::VARCHAR}) FROM range(" +
		                       to_string(first) + "," + to_string(first + 128) + ")t(i)")
		    ->GetValue(0, 0);
	}
	DuckLakeVortexWriteResult WriteBatch(idx_t first, const string &request) {
		Connection owner(database);
		return DuckLakeVortexWriteBatch::Append(owner, "lake", "main", "items", "knn", request, Rows(first));
	}
	void Build(idx_t revision, const string &name) {
		SQL(writer, "CALL ducklake_flush_inlined_data('lake',table_name=>'items')");
		index = VortexIndex::BuildAndPublish(*writer.context, "lake", "main", "items", "knn", root + "/indexes/" + name,
		                                     2, revision, nullptr, "", {}, DuckLakeVortexIndexOperation::APPEND);
	}
	void Seed() {
		WriteBatch(0, "seed");
		Build(1, "seed.json");
		old_source = index.files[0].uri;
		SQL(writer, "SELECT * FROM ducklake_enable_vortex_optimizer('lake','main','items','knn'," +
		                Value(root + "/indexes").ToSQLString() +
		                ",2,100000,4194304,100000,67108864);"
		                "SELECT * FROM ducklake_cancel_vortex_optimizer('lake','main','items','knn');"
		                "SELECT * FROM "
		                "ducklake_set_vortex_write_budget('lake','main','items','knn',1000,4194304,4096,67108864)");
		auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
		while (DuckLakeVortexOptimizer::Status(*writer.context, "lake")[0][16].GetValue<bool>()) {
			Require(std::chrono::steady_clock::now() < deadline, "Seed optimizer did not stop");
			std::this_thread::sleep_for(std::chrono::milliseconds(5));
		}
	}
	void Rewrite() {
		SQL(writer, "DELETE FROM lake.items WHERE id<32");
		index = VortexIndex::Rewrite(*writer.context, "lake", "main", "items", "knn", index.segments[0].generation,
		                             root + "/indexes/rewrite.json", index.revision);
	}
	idx_t Count(const string &table) {
		return SQL(writer, "SELECT count(*) FROM " + table)->GetValue(0, 0).GetValue<idx_t>();
	}
	DuckLakeVortexSearchResult Search(Connection &reader, const std::function<void(const string &)> &observe = {}) {
		DuckLakeVortexViewOptions options {};
		options.catalog = "lake";
		options.schema = "main";
		options.table = "items";
		options.index_name = "knn";
		options.max_source_bytes = 64 * 1024 * 1024;
		options.max_rows = 100000;
		DuckLakeVortexSearchRequest request;
		request.query = {0, 0};
		request.k = 1;
		return DuckLakeVortexSearch::Execute(*reader.context, options, request, observe);
	}
	DuckLakeVortexSourceGCResult Collect(idx_t objects = 16, idx_t bytes = 64 * 1024 * 1024,
	                                     const std::function<void(const string &)> &observe = {}) {
		Connection owner(database);
		return Source::Collect(*owner.context, "lake", objects, bytes, observe);
	}
	void Policy(idx_t keep = 1, idx_t age = 0) {
		DuckLakeVortexOptimizer::ConfigureSourceRetention(*writer.context, "lake", true, keep, age);
	}
	void Drain(idx_t limit = 16) {
		for (idx_t i = 0; i < 64; i++) {
			auto collected = Collect(limit);
			Require(collected.expired + collected.files <= limit && collected.bytes <= 64 * 1024 * 1024,
			        "Source collection exceeded its per-pass budget");
			if (!collected.expired && !collected.files) {
				Require(!collected.blocked, "Unexpected active transaction blocked fixture cleanup");
				return;
			}
		}
		throw InvalidInputException("Source collection did not converge");
	}
	void CheckReclaimed() {
		Require(!FileSystem::GetFileSystem(*writer.context).FileExists(old_source), "Expired source was not reclaimed");
		Require(Count("__ducklake_metadata_lake.ducklake_files_scheduled_for_deletion") == 0 &&
		            Count("__ducklake_metadata_lake.ducklake_vortex_source_deletions_v1") == 0,
		        "Source cleanup left registered deletion work");
		Connection reader(database);
		Require(Search(reader).rows[0][0].GetValue<int64_t>() == 32, "Current ANN query changed after source cleanup");
	}
};

void MaintenanceProbeQuery(Fixture &f, bool stale) {
	if (stale) {
		VortexIndex::CollectBuilds(*f.writer.context, "lake", 16, 64 * 1024 * 1024);
		Require(f.Count("__ducklake_metadata_lake.ducklake_vortex_builds_v1") == 0,
		        "Stale probe fixture still has real build work");
		SQL(f.writer, "INSERT INTO __ducklake_metadata_lake.ducklake_vortex_maintenance_v1 "
		              "VALUES(true,false,'idle','',2,67108864,0,0,0,0,0,7,9,'')");
	} else {
		f.Rewrite();
	}
	auto builds = f.Count("__ducklake_metadata_lake.ducklake_vortex_builds_v1");
	auto generations = f.Count("__ducklake_metadata_lake.ducklake_vortex_generations_v1 WHERE state IN "
	                           "('retired','deleting')");
	Require(stale ? !builds && !generations : builds && generations, "Unexpected initial maintenance backlog");
	auto log = make_shared_ptr<MaintenanceProbeLog>();
	ExtensionCallbackManager::Get(*f.database.instance).Register(make_shared_ptr<MaintenanceQueryCallback>(log));
	Connection control(f.database);
	auto stopped = [&] {
		auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
		while (DuckLakeVortexOptimizer::MaintenanceStatus(*control.context, "lake")[0][2].GetValue<bool>()) {
			Require(std::chrono::steady_clock::now() < deadline, "Probe fixture maintenance did not stop");
			std::this_thread::sleep_for(std::chrono::milliseconds(5));
		}
	};
	if (stale) {
		DuckLakeVortexOptimizer::ResumeMaintenance(*control.context, "lake");
		auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
		while (true) {
			auto status = DuckLakeVortexOptimizer::MaintenanceStatus(*control.context, "lake")[0];
			Require(StringValue::Get(status[0]) != "failed", "Stale probe failed");
			if (!status[11].GetValue<idx_t>() && !status[12].GetValue<idx_t>()) {
				Require(status[4].GetValue<idx_t>() == 0 && StringValue::Get(status[0]) == "idle",
				        "Clearing stale backlog created an empty maintenance run");
				break;
			}
			Require(std::chrono::steady_clock::now() < deadline, "Probe did not clear persisted stale counters");
			std::this_thread::sleep_for(std::chrono::milliseconds(5));
		}
		DuckLakeVortexOptimizer::CancelMaintenance(*control.context, "lake");
		stopped();
	} else {
		auto pause = std::make_shared<Pause>();
		DuckLakeVortexOptimizer::EnableMaintenance(*control.context, "lake", 2, 64 * 1024 * 1024,
		                                           [pause](const string &stage) {
			                                           if (stage == "maintenance_before_recovery") {
				                                           pause->Stop();
			                                           }
		                                           });
		ReleasePause release {*pause};
		pause->Wait();
		auto status = DuckLakeVortexOptimizer::MaintenanceStatus(*control.context, "lake")[0];
		Require(status[11].GetValue<idx_t>() == builds && status[12].GetValue<idx_t>() == generations,
		        "Probe did not save the actual build/generation backlog");
		Require(status[5].GetValue<idx_t>() == 2 && status[6].GetValue<idx_t>() == 64 * 1024 * 1024,
		        "Probe changed the maintenance budget");
		DuckLakeVortexOptimizer::CancelMaintenance(*control.context, "lake");
		pause->Release();
		stopped();
		DuckLakeVortexOptimizer::ResumeMaintenance(*control.context, "lake");
		auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
		while (true) {
			auto status = DuckLakeVortexOptimizer::MaintenanceStatus(*control.context, "lake")[0];
			Require(StringValue::Get(status[0]) != "failed", "Probe cleanup failed");
			if (!status[2].GetValue<bool>() && !status[11].GetValue<idx_t>() && !status[12].GetValue<idx_t>()) {
				break;
			}
			Require(std::chrono::steady_clock::now() < deadline, "Post-collection backlog did not refresh");
			std::this_thread::sleep_for(std::chrono::milliseconds(5));
		}
		DuckLakeVortexOptimizer::CancelMaintenance(*control.context, "lake");
		stopped();
	}
	auto statements = log->Snapshot();
	vortex_detail::ViewJSON json;
	auto facts = json.Object();
	json.Number(facts, "policy_backlog_statements", statements.size());
	json.Number(facts, "initial_builds", builds);
	json.Number(facts, "initial_generations", generations);
	json.Number(facts, "persisted_builds", stale ? 7 : 0);
	json.Number(facts, "persisted_generations", stale ? 9 : 0);
	auto queries = json.Array();
	for (auto &statement : statements) {
		yyjson_mut_arr_append(queries, yyjson_mut_strncpy(json.doc.get(), statement.data(), statement.size()));
	}
	json.Add(facts, "statements", queries);
	Write(f.root + "/maintenance-probe-query.json", json.Finish(facts));
	Require(statements.size() == 2, "Production maintenance probe did not retain its two statements");
	Connection reader(f.database);
	Require(f.Search(reader).rows[0][0].GetValue<int64_t>() == (stale ? 0 : 32),
	        "Maintenance probe changed the current ANN winner");
}

idx_t DiskBytes(FileSystem &fs, const string &path) {
	idx_t bytes = 0;
	fs.ListFiles(path, [&](const string &name, bool directory) {
		auto child = fs.JoinPath(path, name);
		if (directory) {
			bytes += DiskBytes(fs, child);
		} else {
			auto handle = fs.OpenFile(child, FileFlags::FILE_FLAGS_READ);
			bytes += handle->GetFileSize();
		}
	});
	return bytes;
}

void MaintenanceFairness(Fixture &f, idx_t objects) {
	f.Rewrite();
	f.Policy();
	Connection control(f.database);
	struct Progress {
		std::atomic<idx_t> passes {0};
		idx_t builds = 0, generations = 0, expired = 0, files = 0, bytes = 0;
		idx_t max_objects = 0, max_bytes = 0;
	};
	auto progress = std::make_shared<Progress>();
	auto fixture = &f;
	const idx_t passes = 9;
	DuckLakeVortexOptimizer::EnableMaintenance(
	    *f.writer.context, "lake", objects, 64 * 1024 * 1024,
	    [progress, fixture, objects, passes](const string &stage) {
		    if (stage != "maintenance_complete" || progress->passes >= passes) {
			    return;
		    }
		    Connection probe(fixture->database);
		    auto m = DuckLakeVortexOptimizer::MaintenanceStatus(*probe.context, "lake")[0];
		    auto s = Source::Status(*probe.context, "lake")[0];
		    idx_t builds = m[8].GetValue<idx_t>(), generations = m[9].GetValue<idx_t>();
		    idx_t expired = s[3].GetValue<idx_t>(), files = s[4].GetValue<idx_t>();
		    idx_t bytes = m[10].GetValue<idx_t>();
		    auto spent = builds - progress->builds + generations - progress->generations + expired - progress->expired +
		                 files - progress->files;
		    auto written = bytes - progress->bytes;
		    progress->max_objects = MaxValue(progress->max_objects, spent);
		    progress->max_bytes = MaxValue(progress->max_bytes, written);
		    Require(spent <= objects && written <= 64 * 1024 * 1024,
		            "Fair maintenance exceeded its shared per-pass object or byte budget");
		    progress->builds = builds;
		    progress->generations = generations;
		    progress->expired = expired;
		    progress->files = files;
		    progress->bytes = bytes;
		    auto pass = progress->passes.load() + 1;
		    if (pass < passes) {
			    fixture->index = VortexIndex::BuildAndPublish(
			        *fixture->writer.context, "lake", "main", "items", "knn",
			        fixture->root + "/indexes/fairness-" + to_string(pass) + ".json", 2, fixture->index.revision);
		    }
		    progress->passes = pass;
	    });
	auto stop = [&] {
		DuckLakeVortexOptimizer::CancelMaintenance(*control.context, "lake");
		auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
		while (DuckLakeVortexOptimizer::MaintenanceStatus(*control.context, "lake")[0][2].GetValue<bool>()) {
			Require(std::chrono::steady_clock::now() < deadline, "Fairness worker did not stop");
			std::this_thread::sleep_for(std::chrono::milliseconds(5));
		}
	};
	try {
		auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
		while (progress->passes < passes) {
			auto state = DuckLakeVortexOptimizer::MaintenanceStatus(*control.context, "lake")[0];
			Require(StringValue::Get(state[0]) != "failed", "Fairness worker failed: " + state[13].ToString());
			Require(std::chrono::steady_clock::now() < deadline, "Fairness worker did not complete bounded passes");
			std::this_thread::sleep_for(std::chrono::milliseconds(5));
		}
	} catch (...) {
		stop();
		throw;
	}
	stop();
	auto timings = DuckLakeVortexOptimizer::MaintenanceProfile(*control.context, "lake");
	Require(timings.size() == 18, "Missing bounded maintenance profile stages");
	std::set<string> measured;
	for (auto &row : timings) {
		auto calls = row[2].GetValue<uint64_t>(), total = row[3].GetValue<uint64_t>();
		Require(row[0] == timings[0][0] && row[4].GetValue<uint64_t>() <= total && (!calls || total),
		        "Invalid maintenance profile identity or timing counters");
		if (calls) {
			measured.insert(StringValue::Get(row[1]));
		}
	}
	for (auto &stage : {"maintenance_total", "maintenance_probe", "maintenance_metadata", "build_recovery", "build_gc",
	                    "generation_gc", "source_gc", "source_wait", "source_metadata", "source_expire"}) {
		Require(measured.count(stage), "Maintenance profile missed an executed stage");
	}
	vortex_detail::ViewJSON json;
	auto result = json.Object();
	json.Number(result, "passes", passes);
	json.Number(result, "object_budget", objects);
	json.Number(result, "collected_builds", progress->builds);
	json.Number(result, "collected_generations", progress->generations);
	json.Number(result, "expired_snapshots", progress->expired);
	json.Number(result, "collected_source_files", progress->files);
	json.Number(result, "max_pass_objects", progress->max_objects);
	json.Number(result, "max_pass_bytes", progress->max_bytes);
	Write(f.root + "/fairness.json", json.Finish(result));
	Connection reader(f.database);
	Require(f.Search(reader).rows[0][0].GetValue<int64_t>() == 32, "Fair maintenance changed the current ANN winner");
	Require(progress->builds && progress->generations && progress->expired,
	        "Continuous build/generation arrivals starved a maintenance class within nine bounded passes");
}

idx_t ProfileCount(Connection &connection, const string &stage) {
	for (auto &row : DuckLakeVortexOptimizer::MaintenanceProfile(*connection.context, "lake")) {
		if (StringValue::Get(row[1]) == stage) {
			return row[2].GetValue<idx_t>();
		}
	}
	throw InvalidInputException("Missing source-wake profile stage");
}

void WakeGuards(Fixture &f) {
	f.Rewrite();
	f.Policy();
	Connection first(f.database), second(f.database), owner(f.database), capture(f.database);
	first.BeginTransaction();
	second.BeginTransaction();
	SQL(first, "SELECT count(*) FROM lake.items");
	SQL(second, "SELECT count(*) FROM lake.items");
	auto admission = DuckLakeVortexReadView::AdmissionLock().GetSharedLock();
	auto &finite = VortexIndex::Begin(capture, "lake");
	finite.MarkVortexReadCapture();
	auto &transaction = VortexIndex::Begin(owner, "lake");
	auto &manager = TransactionManager::Get(transaction.GetCatalog().GetAttached()).Cast<DuckLakeTransactionManager>();
	idx_t notifications = 0;
	bool last_saw_other = false;
	auto wake = [&] {
		notifications++;
		// Reentering the manager proves delivery is outside its transaction lock.
		last_saw_other = manager.HasOtherTransactions(nullptr, false);
	};
	Require(!Source::WorkReady(transaction, 64 * 1024 * 1024, wake), "Wake registration ignored held transactions");
	capture.Commit();
	admission.reset();
	owner.Commit();
	Require(notifications == 0, "Probe/capture completion woke its own deferred source work");
	first.Commit();
	Require(notifications == 1 && last_saw_other, "Commit did not deliver one out-of-lock blocker notification");
	Require(f.Collect().blocked && FileSystem::GetFileSystem(*f.writer.context).FileExists(f.old_source),
	        "A notification bypassed another ordinary transaction");
	Connection unrelated(f.database);
	unrelated.BeginTransaction();
	SQL(unrelated, "SELECT count(*) FROM lake.items");
	unrelated.Commit();
	Require(notifications == 1, "A consumed notification fired for an unrelated transaction");
	auto &retry = VortexIndex::Begin(owner, "lake");
	Require(!Source::WorkReady(retry, 64 * 1024 * 1024, wake), "Remaining blocker was not rechecked");
	owner.Commit();
	second.Rollback();
	Require(notifications == 2 && !last_saw_other, "Rollback did not finish the second blocker notification");
	f.Drain();
	f.CheckReclaimed();
	second.BeginTransaction();
	SQL(second, "SELECT count(*) FROM lake.items");
	auto &idle = VortexIndex::Begin(owner, "lake");
	bool blocked = true;
	Require(!Source::WorkReady(idle, 64 * 1024 * 1024, wake, &blocked) && !blocked,
	        "An idle source probe subscribed without pending work");
	owner.Commit();
	second.Commit();
	Require(notifications == 2, "No-work source probe subscribed to transaction completion");
	vortex_detail::ViewJSON json;
	auto facts = json.Object();
	json.Number(facts, "notifications", notifications);
	Write(f.root + "/wake-guards.json", json.Finish(facts));
}

void WakeRuntime(Fixture &f, const string &mode) {
	f.Rewrite();
	f.Policy();
	VortexIndex::CollectBuilds(*f.writer.context, "lake", 16, 64 * 1024 * 1024);
	VortexIndex::Collect(*f.writer.context, "lake", 16, 64 * 1024 * 1024);
	const bool late = mode == "transaction-wake-late-reader";
	const bool cancelled = mode == "transaction-wake-cancel";
	struct State {
		Pause pause;
		std::atomic<idx_t> deferred {0};
		std::atomic<bool> released {false}, progressed {false};
		std::promise<idx_t> first_progress;
	};
	auto state = std::make_shared<State>();
	auto progress = state->first_progress.get_future();
	Connection held(f.database), newcomer(f.database), control(f.database);
	if (!late) {
		held.BeginTransaction();
		SQL(held, "SELECT count(*) FROM lake.items");
	}
	auto fixture = &f;
	DuckLakeVortexOptimizer::EnableMaintenance(
	    *f.writer.context, "lake", 2, 64 * 1024 * 1024, [state, fixture, late](const string &stage) {
		    if ((!late && stage == "maintenance_source_deferred") || (late && stage == "source_gc_waiting")) {
			    state->deferred++;
			    state->pause.Stop();
			    state->released = true;
		    }
		    if (late && stage == "source_gc_deferred") {
			    state->deferred++;
		    }
		    if (stage == "source_before_expire" && !state->progressed.exchange(true)) {
			    Connection probe(fixture->database);
			    state->first_progress.set_value(ProfileCount(probe, "loop_wait_timeout"));
		    }
	    });
	ReleasePause release {state->pause};
	state->pause.Wait();
	auto before = ProfileCount(control, "loop_wait_timeout");
	if (late) {
		held.BeginTransaction();
		SQL(held, "SELECT count(*) FROM lake.items");
		state->pause.Release();
		auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
		while (state->deferred < 2) {
			Require(std::chrono::steady_clock::now() < deadline, "Late collector did not register its blocker");
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
		}
	}
	if (cancelled) {
		DuckLakeVortexOptimizer::CancelMaintenance(*control.context, "lake");
	}
	Require(FileSystem::GetFileSystem(*f.writer.context).FileExists(f.old_source),
	        "Source cleanup crossed its held ordinary transaction");
	if (mode == "transaction-wake-rollback") {
		held.Rollback();
	} else {
		held.Commit();
	}
	auto notifications = ProfileCount(control, "source_transaction_wake");
	Require(notifications == (cancelled ? 0 : 1), "Transaction completion did not respect maintenance registration");
	Require(late || FileSystem::GetFileSystem(*f.writer.context).FileExists(f.old_source),
	        "Notification performed GC while its deferred callback was paused");
	if (mode == "transaction-wake-new-reader") {
		newcomer.BeginTransaction();
		SQL(newcomer, "SELECT count(*) FROM lake.items");
	}
	state->pause.Release();
	if (cancelled) {
		auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
		while (!state->released) {
			Require(std::chrono::steady_clock::now() < deadline, "Cancelled deferred callback did not return");
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
		}
		Require(!DuckLakeVortexOptimizer::MaintenanceStatus(*control.context, "lake")[0][1].GetValue<bool>(),
		        "Completion notification resumed cancelled maintenance");
		DuckLakeVortexOptimizer::ResumeMaintenance(*control.context, "lake");
	} else if (mode == "transaction-wake-new-reader") {
		auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
		while (state->deferred < 2) {
			Require(std::chrono::steady_clock::now() < deadline,
			        "New transaction was not rechecked after notification");
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
		}
		Require(f.Collect().blocked && FileSystem::GetFileSystem(*f.writer.context).FileExists(f.old_source),
		        "A latched wake bypassed a newly active transaction");
		newcomer.Rollback();
		notifications = ProfileCount(control, "source_transaction_wake");
		Require(notifications == 2, "The newly checked blocker did not produce its own completion notification");
	}
	if (!cancelled) {
		Require(progress.wait_for(std::chrono::seconds(10)) == std::future_status::ready,
		        "Transaction completion never led to guarded source progress");
		auto timeout_delta = progress.get() - before;
		vortex_detail::ViewJSON json;
		auto facts = json.Object();
		json.Number(facts, "accepted_notifications", notifications);
		json.Number(facts, "timeouts_before_first_progress", timeout_delta);
		Write(f.root + "/transaction-wake.json", json.Finish(facts));
		if (mode == "transaction-wake-commit" || mode == "transaction-wake-rollback") {
			Require(timeout_delta == 0, "A transaction-end notification before wait was lost");
		}
	}
	auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
	while (FileSystem::GetFileSystem(*f.writer.context).FileExists(f.old_source)) {
		Require(std::chrono::steady_clock::now() < deadline, "Woken source worker did not reclaim its old source");
		std::this_thread::sleep_for(std::chrono::milliseconds(5));
	}
	DuckLakeVortexOptimizer::CancelMaintenance(*control.context, "lake");
	while (DuckLakeVortexOptimizer::MaintenanceStatus(*control.context, "lake")[0][2].GetValue<bool>()) {
		Require(std::chrono::steady_clock::now() < deadline, "Woken source worker did not stop");
		std::this_thread::sleep_for(std::chrono::milliseconds(5));
	}
	Connection reader(f.database);
	Require(f.Search(reader).rows[0][0].GetValue<int64_t>() == 32, "Source wake changed the current ANN winner");
}

void Concurrent(Fixture &f) {
	auto seed = f.WriteBatch(0, "seed");
	f.Policy(2);
	DuckLakeVortexOptimizer::EnableMaintenance(*f.writer.context, "lake", 2, 64 * 1024 * 1024);
	Write(f.root + "/data/unregistered.keep", "not-owned");
	std::map<idx_t, int64_t> oracle;
	auto remember = [&] {
		auto snapshot = SQL(f.writer, "SELECT max(snapshot_id) FROM __ducklake_metadata_lake.ducklake_snapshot")
		                    ->GetValue(0, 0)
		                    .GetValue<idx_t>();
		oracle[snapshot] = SQL(f.writer, "SELECT min(id) FROM lake.items")->GetValue(0, 0).GetValue<int64_t>();
	};
	remember();
	std::atomic<bool> running {true};
	vector<std::pair<idx_t, int64_t>> observed;
	Connection reader(f.database);
	auto reads = std::async(std::launch::async, [&] {
		while (running && observed.size() < 1000) {
			auto result = f.Search(reader);
			using namespace duckdb_yyjson;
			std::unique_ptr<yyjson_doc, decltype(&yyjson_doc_free)> json(
			    yyjson_read(result.diagnostics.data(), result.diagnostics.size(), 0), yyjson_doc_free);
			auto capture = yyjson_obj_get(yyjson_doc_get_root(json.get()), "native_capture");
			observed.emplace_back(yyjson_get_uint(yyjson_obj_get(capture, "snapshot_id")),
			                      result.rows[0][0].GetValue<int64_t>());
			std::this_thread::sleep_for(std::chrono::milliseconds(2));
		}
	});
	try {
		for (idx_t round = 1; round <= 8; round++) {
			f.WriteBatch(round * 1000, "epoch-" + to_string(round));
			remember();
			SQL(f.writer, "DELETE FROM lake.items WHERE id<" + to_string(round * 1000));
			remember();
			f.index = VortexIndex::RetireEmpty(*f.writer.context, "lake", "main", "items", "knn",
			                                   f.index.segments[0].generation, f.index.revision);
			remember();
			f.Build(f.index.revision, "epoch-" + to_string(round) + ".json");
			remember();
			auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
			while (true) {
				auto status = Source::Status(*f.writer.context, "lake")[0];
				auto maintenance = DuckLakeVortexOptimizer::MaintenanceStatus(*f.writer.context, "lake")[0];
				Require(StringValue::Get(maintenance[0]) != "failed", maintenance.back().ToString());
				if (status[6].GetValue<idx_t>() == 0 && status[7].GetValue<idx_t>() == 0 &&
				    status[8].GetValue<idx_t>() == 0 && !maintenance[2].GetValue<bool>()) {
					break;
				}
				Require(std::chrono::steady_clock::now() < deadline, "Background source collection did not converge");
				std::this_thread::sleep_for(std::chrono::milliseconds(10));
			}
			auto recorded =
			    SQL(f.writer,
			        "SELECT coalesce((SELECT sum(file_size_bytes) FROM __ducklake_metadata_lake.ducklake_data_file),0)"
			        "+coalesce((SELECT sum(file_size_bytes) FROM __ducklake_metadata_lake.ducklake_delete_file),0)")
			        ->GetValue(0, 0)
			        .GetValue<idx_t>();
			Require(DiskBytes(FileSystem::GetFileSystem(*f.writer.context), f.root + "/data") == recorded + 9,
			        "Historical source bytes did not converge to retained native inventory");
			Require(f.Count("lake.items") == 128, "Rolling batch lost committed current data");
		}
	} catch (...) {
		running = false;
		reads.wait();
		DuckLakeVortexOptimizer::CancelMaintenance(*f.writer.context, "lake");
		throw;
	}
	running = false;
	reads.get();
	Require(!observed.empty(), "No concurrent native query completed");
	for (auto &query : observed) {
		Require(oracle.count(query.first) && oracle[query.first] == query.second,
		        "Concurrent ANN winner disagreed with its exact committed snapshot");
	}
	Require(f.WriteBatch(0, "seed").snapshot_id == seed.snapshot_id && f.Count("lake.items") == 128,
	        "Source expiry lost the request receipt or reapplied an old batch");
	Require(SQL(f.writer, "SELECT count(*) FROM __ducklake_metadata_lake.ducklake_snapshot WHERE snapshot_id=" +
	                          to_string(seed.snapshot_id))
	                ->GetValue(0, 0)
	                .GetValue<idx_t>() == 0,
	        "Receipt check did not cover an expired original snapshot");
	DuckLakeVortexOptimizer::CancelMaintenance(*f.writer.context, "lake");
	std::cout << "eight rolling epochs; " << observed.size()
	          << " snapshot-matched concurrent queries; bounded disk; retained receipt" << std::endl;
}
} // namespace

int main(int argc, char **argv) {
	try {
		Require(argc == 3, "Usage: ducklake_vortex_source_driver ROOT MODE");
		string root = argv[1], mode = argv[2];
		Fixture f(root);
		f.Seed();
		if (mode == "maintenance-probe-query" || mode == "maintenance-probe-stale") {
			MaintenanceProbeQuery(f, mode == "maintenance-probe-stale");
			return 0;
		}
		if (mode == "transaction-wake-guards") {
			WakeGuards(f);
			return 0;
		}
		if (StringUtil::StartsWith(mode, "transaction-wake-")) {
			WakeRuntime(f, mode);
			return 0;
		}
		if (mode == "probe-index-reuse") {
			f.writer.CreateScalarFunction<int64_t, int64_t>("probe_revision_gate", &ProbeRevision);
			SQL(f.writer,
			    "ALTER TABLE __ducklake_metadata_lake.ducklake_vortex_indexes_v1 RENAME TO probe_index_backing");
			auto columns = SQL(f.writer, "DESCRIBE __ducklake_metadata_lake.probe_index_backing");
			string definitions, inputs, outputs;
			for (auto &column : *columns) {
				auto name = column.GetValue<string>(0);
				auto input = "\"" + StringUtil::Replace(name, "\"", "\"\"") + "\"";
				auto output = name == "revision" ? string("revision_backing") : input;
				if (!definitions.empty()) {
					definitions += ",";
					inputs += ",";
					outputs += ",";
				}
				definitions += output + " " + column.GetValue<string>(1);
				inputs += input;
				outputs += output;
			}
			definitions += ",revision BIGINT GENERATED ALWAYS AS(probe_revision_gate(revision_backing))";
			SQL(f.writer, "CREATE TABLE __ducklake_metadata_lake.ducklake_vortex_indexes_v1(" + definitions +
			                  ");INSERT INTO __ducklake_metadata_lake.ducklake_vortex_indexes_v1(" + outputs +
			                  ") SELECT " + inputs + " FROM __ducklake_metadata_lake.probe_index_backing");
			DuckLakeVortexOptimizer::ConfigureCompaction(*f.writer.context, "lake", "main", "items", "knn", true, 1, 1);
			probe_revision_reads = 0;
			Pause pause;
			DuckLakeVortexOptimizer::Resume(*f.writer.context, "lake", "main", "items", "knn",
			                                [&](const string &stage) {
				                                if (stage == "optimizer_probed") {
					                                pause.Stop();
				                                }
			                                });
			ReleasePause release {pause};
			pause.Wait();
			auto reads = probe_revision_reads.load();
			DuckLakeVortexOptimizer::Cancel(*f.writer.context, "lake", "main", "items", "knn");
			pause.Release();
			vortex_detail::ViewJSON json;
			auto facts = json.Object();
			json.Number(facts, "revision_reads", reads);
			Write(root + "/probe-reuse.json", json.Finish(facts));
			Require(reads == 2, "One compaction probe reread its ready index");
			Connection reader(f.database);
			Require(f.Search(reader).rows[0][0].GetValue<int64_t>() == 0,
			        "Reused probe changed the current ANN winner");
			return 0;
		}
		if (mode == "maintenance-idle-wait") {
			f.Policy(1, 315360000);
			DuckLakeVortexOptimizer::EnableMaintenance(*f.writer.context, "lake", 2, 64 * 1024 * 1024);
			auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
			bool observed = false;
			while (!observed) {
				for (auto &row : DuckLakeVortexOptimizer::MaintenanceProfile(*f.writer.context, "lake")) {
					if (StringValue::Get(row[1]) == "loop_wait_timeout") {
						observed = row[2].GetValue<uint64_t>() && row[3].GetValue<uint64_t>() >= 50 * 1000 * 1000;
					}
				}
				Require(std::chrono::steady_clock::now() < deadline, "Idle worker did not report timed loop waits");
				std::this_thread::sleep_for(std::chrono::milliseconds(5));
			}
			DuckLakeVortexOptimizer::CancelMaintenance(*f.writer.context, "lake");
			return 0;
		}
		if (mode == "maintenance-fairness-1" || mode == "maintenance-fairness-2") {
			MaintenanceFairness(f, mode == "maintenance-fairness-1" ? 1 : 2);
			return 0;
		}
		if (mode == "concurrent") {
			Concurrent(f);
			return 0;
		}
		if (mode == "status-readiness" || mode == "optimizer-status-readiness" ||
		    mode == "maintenance-status-readiness") {
			if (mode == "maintenance-status-readiness") {
				DuckLakeVortexOptimizer::EnableMaintenance(*f.writer.context, "lake", 2, 64 * 1024 * 1024);
				DuckLakeVortexOptimizer::CancelMaintenance(*f.writer.context, "lake");
				auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
				while (DuckLakeVortexOptimizer::MaintenanceStatus(*f.writer.context, "lake")[0][2].GetValue<bool>()) {
					Require(std::chrono::steady_clock::now() < deadline, "Status fixture maintenance did not stop");
					std::this_thread::sleep_for(std::chrono::milliseconds(5));
				}
			}
			string table = mode == "status-readiness"             ? "ducklake_vortex_source_retention_v1"
			               : mode == "optimizer-status-readiness" ? "ducklake_vortex_optimizer_v2"
			                                                      : "ducklake_vortex_maintenance_v1";
			f.Policy();
			f.writer.CreateScalarFunction<bool, bool>("source_status_gate", &StatusEnabled);
			SQL(f.writer, "ALTER TABLE __ducklake_metadata_lake." + table + " RENAME TO status_policy_backing");
			if (mode == "status-readiness") {
				SQL(f.writer, "CREATE VIEW __ducklake_metadata_lake." + table +
				                  " AS SELECT * REPLACE(source_status_gate(enabled) AS enabled) FROM "
				                  "__ducklake_metadata_lake.status_policy_backing");
			} else {
				auto columns = SQL(f.writer, "DESCRIBE __ducklake_metadata_lake.status_policy_backing");
				string definitions, inputs, outputs;
				for (auto &column : *columns) {
					auto name = column.GetValue<string>(0);
					auto input = "\"" + StringUtil::Replace(name, "\"", "\"\"") + "\"";
					auto output = name == "enabled" ? string("enabled_backing") : input;
					if (!definitions.empty()) {
						definitions += ",";
						inputs += ",";
						outputs += ",";
					}
					definitions += output + " " + column.GetValue<string>(1);
					inputs += input;
					outputs += output;
				}
				definitions += ",enabled BOOLEAN GENERATED ALWAYS AS(source_status_gate(enabled_backing))";
				SQL(f.writer, "CREATE TABLE __ducklake_metadata_lake." + table + "(" + definitions +
				                  ");"
				                  "INSERT INTO __ducklake_metadata_lake." +
				                  table + "(" + outputs + ") SELECT " + inputs +
				                  " FROM __ducklake_metadata_lake.status_policy_backing");
			}
			Connection held(f.database), owner(f.database);
			held.BeginTransaction();
			SQL(held, "SELECT count(*) FROM lake.items");
			Pause pause;
			status_pause = &pause;
			pause_status = true;
			auto pending = std::async(std::launch::async, [&] {
				if (mode == "optimizer-status-readiness") {
					return DuckLakeVortexOptimizer::Status(*f.writer.context, "lake");
				}
				if (mode == "maintenance-status-readiness") {
					return DuckLakeVortexOptimizer::MaintenanceStatus(*f.writer.context, "lake");
				}
				return Source::Status(*f.writer.context, "lake");
			});
			ReleasePause release {pause};
			if (pending.wait_for(std::chrono::milliseconds(50)) == std::future_status::ready) {
				pending.get();
				throw InvalidInputException("Status query did not reach its controlled checkpoint");
			}
			pause.Wait();
			auto admission = DuckLakeVortexReadView::AdmissionLock().TryGetExclusiveLock();
			bool admitted = !admission;
			admission.reset();
			auto &transaction = VortexIndex::Begin(owner, "lake");
			bool ordinary_blocked = !Source::WorkReady(transaction, 64 * 1024 * 1024);
			held.Commit();
			bool ready = Source::WorkReady(transaction, 64 * 1024 * 1024);
			owner.Commit();
			pause.Release();
			auto status = pending.get();
			SQL(f.writer, string(mode == "status-readiness" ? "DROP VIEW " : "DROP TABLE ") +
			                  "__ducklake_metadata_lake." + table +
			                  ";ALTER TABLE __ducklake_metadata_lake.status_policy_backing RENAME TO " + table);
			vortex_detail::ViewJSON json;
			auto facts = json.Object();
			json.Add(facts, "ordinary_transaction_blocked",
			         duckdb_yyjson::yyjson_mut_bool(json.doc.get(), ordinary_blocked));
			json.Add(facts, "finite_status_ready", duckdb_yyjson::yyjson_mut_bool(json.doc.get(), ready));
			json.Add(facts, "admission_held", duckdb_yyjson::yyjson_mut_bool(json.doc.get(), admitted));
			Write(root + "/status-readiness.json", json.Finish(facts));
			idx_t enabled_column = mode == "status-readiness" ? 0 : mode == "optimizer-status-readiness" ? 5 : 1;
			Require(status.size() == 1 && status[0][enabled_column].GetValue<bool>() == (mode == "status-readiness"),
			        "Finite status capture lost its result");
			Require(ordinary_blocked, "Status readiness ignored an ordinary transaction");
			Require(ready, "Source readiness discarded finite status work");
			Require(admitted, "Source status did not hold finite read admission");
			f.Drain();
			Connection reader(f.database);
			Require(f.Search(reader).rows[0][0].GetValue<int64_t>() == 0, "Status capture changed the query winner");
			return 0;
		}
		if (mode == "capture-readiness") {
			f.Policy();
			Pause pause;
			Connection reader(f.database);
			auto pending = std::async(std::launch::async, [&] {
				return f.Search(reader, [&](const string &stage) {
					if (stage == "after_snapshot") {
						pause.Stop();
					}
				});
			});
			ReleasePause release {pause};
			pause.Wait();
			Connection held(f.database), owner(f.database);
			held.BeginTransaction();
			SQL(held, "SELECT count(*) FROM lake.items");
			auto &transaction = VortexIndex::Begin(owner, "lake");
			bool ordinary_blocked = !Source::WorkReady(transaction, 64 * 1024 * 1024);
			held.Commit();
			bool ready = Source::WorkReady(transaction, 64 * 1024 * 1024);
			owner.Commit();
			pause.Release();
			Require(pending.get().rows[0][0].GetValue<int64_t>() == 0, "Capture readiness changed its query winner");
			Require(ordinary_blocked, "Source readiness ignored an ordinary transaction");
			Require(ready, "Source readiness discarded finite native capture work");
			return 0;
		}
		if (mode == "capture-gate") {
			f.Policy();
			auto admission = DuckLakeVortexReadView::AdmissionLock().GetSharedLock();
			std::promise<void> waiting;
			auto collecting = std::async(std::launch::async, [&] {
				return f.Collect(16, 64 * 1024 * 1024, [&](const string &stage) {
					if (stage == "source_gc_waiting") {
						waiting.set_value();
					}
				});
			});
			waiting.get_future().wait();
			bool deferred = collecting.wait_for(std::chrono::milliseconds(100)) == std::future_status::timeout;
			auto entering = std::async(std::launch::async, [&] {
				Connection reader(f.database);
				return SQL(reader, "SELECT min(id) FROM lake.items")->GetValue(0, 0).GetValue<int64_t>();
			});
			bool progressed = entering.wait_for(std::chrono::seconds(1)) == std::future_status::ready;
			admission.reset();
			auto result = collecting.get();
			Require(entering.get() == 0, "Transaction entry lost current data");
			Require(deferred && progressed, "Source collector blocked transaction entry while awaiting capture");
			Require(!result.blocked && result.expired, "Source collector did not progress after admission closed");
			return 0;
		}
		if (mode == "held" || mode == "admission") {
			Pause pause;
			Connection reader(f.database);
			auto pending = std::async(std::launch::async, [&] {
				return f.Search(reader, [&](const string &stage) {
					if (stage == (mode == "held" ? "before_fetch" : "after_snapshot")) {
						pause.Stop();
					}
				});
			});
			ReleasePause release {pause};
			pause.Wait();
			f.Policy();
			if (mode == "admission") {
				std::promise<void> waiting;
				auto collecting = std::async(std::launch::async, [&] {
					return f.Collect(16, 64 * 1024 * 1024, [&](const string &stage) {
						if (stage == "source_gc_waiting") {
							waiting.set_value();
						}
					});
				});
				waiting.get_future().wait();
				bool deferred = collecting.wait_for(std::chrono::milliseconds(100)) == std::future_status::timeout;
				auto gate = Source::TransactionGate().TryGetExclusiveLock();
				bool gate_available = bool(gate);
				gate.reset();
				pause.Release();
				Require(pending.get().rows[0][0].GetValue<int64_t>() == 0, "Admitted query lost its native view");
				auto result = collecting.get();
				Require(deferred, "Source collector abandoned an in-flight native capture");
				Require(gate_available, "Source collector froze transactions before native capture completed");
				Require(!result.blocked && result.expired, "Source collector did not progress after native capture");
				return 0;
			} else {
				f.Rewrite();
				VortexIndex::Collect(*f.writer.context, "lake", 16, 64 * 1024 * 1024);
				f.Drain();
				f.CheckReclaimed();
			}
			pause.Release();
			Require(pending.get().rows[0][0].GetValue<int64_t>() == 0, "Held view lost delayed typed fetch after GC");
			return 0;
		}
		Connection old(f.database);
		if (mode == "defer") {
			old.BeginTransaction();
			Require(SQL(old, "SELECT count(*) FROM lake.items")->GetValue(0, 0).GetValue<idx_t>() == 128,
			        "Cannot establish native old transaction");
		}
		f.Rewrite();
		f.Policy();
		if (mode == "source-only") {
			VortexIndex::CollectBuilds(*f.writer.context, "lake", 16, 64 * 1024 * 1024);
			VortexIndex::Collect(*f.writer.context, "lake", 16, 64 * 1024 * 1024);
			Require(f.Count("__ducklake_metadata_lake.ducklake_vortex_builds_v1") == 0,
			        "Source-only fixture retained build work");
			auto generations = VortexIndex::CollectionLock().GetExclusiveLock();
			auto empty_phases = std::make_shared<std::atomic<idx_t>>(0);
			auto completed = std::make_shared<std::atomic<bool>>(false);
			auto database = &f.database;
			DuckLakeVortexOptimizer::EnableMaintenance(
			    *f.writer.context, "lake", 2, 64 * 1024 * 1024,
			    [empty_phases, completed, database](const string &stage) {
				    if (stage == "build_recovery_waiting" || stage == "build_gc_waiting" || stage == "gc_waiting") {
					    ++*empty_phases;
				    }
				    if (stage == "maintenance_complete") {
					    Connection probe(*database);
					    auto source = Source::Status(*probe.context, "lake")[0];
					    if (!source[6].GetValue<idx_t>() && !source[7].GetValue<idx_t>() &&
					        !source[8].GetValue<idx_t>()) {
						    *completed = true;
					    }
				    }
			    });
			auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
			while (!*completed && std::chrono::steady_clock::now() < deadline) {
				std::this_thread::sleep_for(std::chrono::milliseconds(5));
			}
			generations.reset();
			DuckLakeVortexOptimizer::CancelMaintenance(*f.writer.context, "lake");
			deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
			while (DuckLakeVortexOptimizer::MaintenanceStatus(*f.writer.context, "lake")[0][2].GetValue<bool>()) {
				Require(std::chrono::steady_clock::now() < deadline, "Source-only worker did not stop");
				std::this_thread::sleep_for(std::chrono::milliseconds(5));
			}
			Require(*completed && *empty_phases == 0, "Source-only maintenance waited on empty index collection");
			f.CheckReclaimed();
			return 0;
		}
		if (mode == "defer") {
			Require(f.Collect().blocked &&
			            SQL(old, "SELECT count(*) FROM lake.items")->GetValue(0, 0).GetValue<idx_t>() == 128,
			        "An active native transaction lost its source snapshot");
			old.Commit();
			f.Drain();
			f.CheckReclaimed();
			return 0;
		}
		if (mode == "age") {
			f.Policy(1, 315360000);
			Require(f.Collect().expired == 0, "Age policy expired retained snapshots");
			f.Policy();
			f.Drain(1);
			f.CheckReclaimed();
			return 0;
		}
		if (mode == "live") {
			auto file = f.index.files[0];
			SQL(f.writer, "INSERT INTO __ducklake_metadata_lake.ducklake_files_scheduled_for_deletion VALUES(" +
			                  to_string(file.id) + "," + Value(file.uri).ToSQLString() + ",false,now())");
			bool failed = false;
			try {
				f.Collect();
			} catch (std::exception &) {
				failed = true;
			}
			Require(failed && FileSystem::GetFileSystem(*f.writer.context).FileExists(file.uri),
			        "Live source was deleted");
			SQL(f.writer,
			    "DELETE FROM __ducklake_metadata_lake.ducklake_files_scheduled_for_deletion WHERE data_file_id=" +
			        to_string(file.id));
			f.Drain();
			f.CheckReclaimed();
			return 0;
		}
		if (mode == "budget") {
			f.Collect();
			auto limited = f.Collect(1, 1);
			Require(limited.files == 0 && limited.bytes == 0, "Source file exceeded the configured byte budget");
			f.Drain(1);
			f.CheckReclaimed();
			return 0;
		}
		if (mode == "manual") {
			f.Collect();
			bool stopped = false;
			try {
				f.Collect(16, 64 * 1024 * 1024, [&](const string &stage) {
					if (stage == "source_delete_intent_committed") {
						stopped = true;
						throw IOException("Pause before independent native cleanup");
					}
				});
			} catch (std::exception &) {
				Require(stopped, "Deletion intent did not reach the expected boundary");
			}
			SQL(f.writer, "CALL ducklake_cleanup_old_files('lake',cleanup_all=>true)");
			Require(f.Count("__ducklake_metadata_lake.ducklake_vortex_source_deletions_v1") == 1 &&
			            f.Count("__ducklake_metadata_lake.ducklake_files_scheduled_for_deletion") == 0,
			        "Native cleanup did not leave the expected independent intent acknowledgement");
			f.Drain();
			f.CheckReclaimed();
			return 0;
		}
		if (mode == "checksum" || mode == "symlink") {
			f.Collect();
			auto original = Read(f.old_source);
			Write(root + "/victim", "keep-me");
			bool changed = false, failed = false;
			try {
				f.Collect(16, 64 * 1024 * 1024, [&](const string &stage) {
					if (stage == "source_delete_intent_committed") {
						changed = true;
						if (mode == "checksum") {
							auto contents = original;
							contents[0] ^= 1;
							std::ofstream output(f.old_source, std::ios::binary);
							output << contents;
						} else {
							Require(std::rename(f.old_source.c_str(), (root + "/backup").c_str()) == 0,
							        "Cannot preserve source inode");
							Require(symlink((root + "/victim").c_str(), f.old_source.c_str()) == 0,
							        "Cannot inject source symlink");
						}
					}
				});
			} catch (std::exception &) {
				failed = true;
			}
			Require(changed && failed && Read(root + "/victim") == "keep-me", "Changed source was not rejected safely");
			if (mode == "checksum") {
				std::ofstream output(f.old_source, std::ios::binary);
				output << original;
			} else {
				Require(unlink(f.old_source.c_str()) == 0 &&
				            std::rename((root + "/backup").c_str(), f.old_source.c_str()) == 0,
				        "Cannot restore original registered source");
			}
			f.Drain();
			f.CheckReclaimed();
			return 0;
		}
		if (mode == "cancel") {
			Pause pause;
			DuckLakeVortexOptimizer::EnableMaintenance(*f.writer.context, "lake", 2, 64 * 1024 * 1024,
			                                           [&](const string &stage) {
				                                           if (stage == "source_before_unlink") {
					                                           pause.Stop();
				                                           }
			                                           });
			ReleasePause release {pause};
			pause.Wait();
			Connection cancellation(f.database);
			auto before_profile = DuckLakeVortexOptimizer::MaintenanceProfile(*cancellation.context, "lake");
			Require(before_profile.size() == 18, "Maintenance profile read blocked behind source collection");
			Require(SQL(cancellation, "SELECT count(*) FROM ducklake_vortex_maintenance_profile('lake')")
			                ->GetValue(0, 0)
			                .GetValue<idx_t>() == 18,
			        "SQL maintenance profile read blocked behind source collection");
			auto cancelled = std::async(
			    std::launch::async, [&] { DuckLakeVortexOptimizer::CancelMaintenance(*cancellation.context, "lake"); });
			Require(cancelled.wait_for(std::chrono::milliseconds(100)) == std::future_status::timeout,
			        "Cancellation crossed an active source deletion transaction");
			pause.Release();
			cancelled.get();
			auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
			while (DuckLakeVortexOptimizer::MaintenanceStatus(*f.writer.context, "lake")[0][2].GetValue<bool>()) {
				Require(std::chrono::steady_clock::now() < deadline, "Cancelled source worker did not stop");
				std::this_thread::sleep_for(std::chrono::milliseconds(5));
			}
			Require(FileSystem::GetFileSystem(*f.writer.context).FileExists(f.old_source) &&
			            f.Count("__ducklake_metadata_lake.ducklake_vortex_source_deletions_v1") == 1,
			        "Cancellation did not preserve the registered deletion intent");
			auto cancelled_profile = DuckLakeVortexOptimizer::MaintenanceProfile(*cancellation.context, "lake");
			bool timed_cancellation = false;
			for (auto &row : cancelled_profile) {
				if (StringValue::Get(row[1]) == "source_metadata") {
					timed_cancellation = row[4].GetValue<uint64_t>() >= 50 * 1000 * 1000;
				}
			}
			Require(timed_cancellation, "Failed/cancelled source scope lost its held wall time");
			DuckLakeVortexOptimizer::ResumeMaintenance(*f.writer.context, "lake");
			auto resumed_profile = DuckLakeVortexOptimizer::MaintenanceProfile(*cancellation.context, "lake");
			Require(resumed_profile.size() == 18 && resumed_profile[0][0] != before_profile[0][0],
			        "Explicit maintenance resume did not identify the new in-memory profile");
			while (FileSystem::GetFileSystem(*f.writer.context).FileExists(f.old_source)) {
				Require(std::chrono::steady_clock::now() < deadline, "Resumed source worker did not finish");
				std::this_thread::sleep_for(std::chrono::milliseconds(5));
			}
			DuckLakeVortexOptimizer::CancelMaintenance(*f.writer.context, "lake");
			while (DuckLakeVortexOptimizer::MaintenanceStatus(*f.writer.context, "lake")[0][2].GetValue<bool>()) {
				Require(std::chrono::steady_clock::now() < deadline, "Resumed source worker did not stop");
				std::this_thread::sleep_for(std::chrono::milliseconds(5));
			}
			auto completed_profile = DuckLakeVortexOptimizer::MaintenanceProfile(*cancellation.context, "lake");
			for (auto &row : completed_profile) {
				if (StringValue::Get(row[1]) == "source_artifact" || StringValue::Get(row[1]) == "source_unlink") {
					Require(row[2].GetValue<uint64_t>() && row[3].GetValue<uint64_t>(),
					        "Resumed file collection lost artifact or unlink timing");
				}
			}
			f.Drain();
			f.CheckReclaimed();
			return 0;
		}
		if (mode == "gate") {
			f.Collect();
			Pause pause;
			auto collecting = std::async(std::launch::async, [&] {
				return f.Collect(16, 64 * 1024 * 1024, [&](const string &stage) {
					if (stage == "source_before_unlink") {
						pause.Stop();
					}
				});
			});
			ReleasePause release {pause};
			pause.Wait();
			Connection reader(f.database);
			auto reading = std::async(std::launch::async, [&] { return f.Search(reader); });
			Require(reading.wait_for(std::chrono::milliseconds(50)) == std::future_status::timeout,
			        "New query crossed source deletion admission");
			pause.Release();
			collecting.get();
			Require(reading.get().rows[0][0].GetValue<int64_t>() == 32, "New query lost current data after deletion");
			f.Drain();
			f.CheckReclaimed();
			return 0;
		}
		bool fired = false;
		auto observe = [&](const string &stage) {
			if (mode == "pause-" + stage || mode == "background-pause-" + stage) {
				Write(root + "/checkpoint", stage);
				auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
				while (!FileSystem::GetFileSystem(*f.writer.context).FileExists(root + "/release")) {
					Require(std::chrono::steady_clock::now() < deadline, "Source pause timed out");
					std::this_thread::sleep_for(std::chrono::milliseconds(5));
				}
			}
			if (mode == "fail-" + stage) {
				fired = true;
				throw IOException("Injected source retention failure");
			}
		};
		if (StringUtil::StartsWith(mode, "background-pause-")) {
			DuckLakeVortexOptimizer::EnableMaintenance(*f.writer.context, "lake", 2, 64 * 1024 * 1024, observe);
			std::this_thread::sleep_for(std::chrono::seconds(30));
			throw InvalidInputException("Background source checkpoint did not terminate the process");
		}
		for (idx_t i = 0; i < 32 && !fired; i++) {
			try {
				f.Collect(16, 64 * 1024 * 1024, observe);
			} catch (std::exception &) {
				Require(fired, "Unexpected source retention error");
			}
		}
		Require(fired || StringUtil::StartsWith(mode, "pause-"), "Source fault checkpoint was not reached");
		f.Drain();
		f.CheckReclaimed();
		return 0;
	} catch (std::exception &error) {
		std::cerr << error.what() << std::endl;
		return 1;
	}
}
