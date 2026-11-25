#include "native_vortex_fixture.hpp"
#include "common/ducklake_vortex_json.hpp"
#include "ducklake_extension.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/connection_manager.hpp"
#include "duckdb/main/db_instance_cache.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_metadata_manager.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_transaction_manager.hpp"
#include "storage/ducklake_vortex_index_catalog.hpp"
#include "storage/ducklake_vortex_optimizer.hpp"
#include "storage/ducklake_vortex_search.hpp"
#include "storage/ducklake_vortex_source_retention.hpp"
#include "storage/ducklake_vortex_write.hpp"
#include "vortex_extension.hpp"

#include <algorithm>
#include <atomic>
#include <exception>
#include <future>
#include <iostream>
#include <map>
#include <memory>
#include <thread>
#include <sys/stat.h>
#include <unistd.h>

using namespace duckdb;
using namespace duckdb::vortex_test;
using Source = DuckLakeVortexSourceRetention;
using VortexIndex = DuckLakeVortexIndex;

namespace {
constexpr const char *STATUS_POLICY_READ_ERROR = "Injected status metadata read error";
Pause *status_pause = nullptr;
std::atomic<bool> pause_status {false};
bool fail_status_read = false;

template <class T>
T StatusField(T value) {
	if (pause_status.exchange(false)) {
		status_pause->Stop();
	}
	if (fail_status_read) {
		throw InvalidInputException(STATUS_POLICY_READ_ERROR);
	}
	return value;
}

void WaitForSourceAdmission(std::future<DuckLakeVortexSourceGCResult> &collecting, idx_t previous_waiters) {
	auto &admission = DuckLakeVortexReadView::AdmissionLock();
	const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
	while (admission.WaitingWriterCount() <= previous_waiters) {
		if (collecting.wait_for(std::chrono::milliseconds(1)) == std::future_status::ready) {
			collecting.get();
			throw InvalidInputException("Source collection finished before waiting for capture admission");
		}
		Require(std::chrono::steady_clock::now() < deadline, "Source collection did not reach capture admission");
	}
}

template <class Result>
void WaitForCaptureAdmission(std::future<Result> &capture, idx_t previous_waits) {
	auto &admission = DuckLakeVortexReadView::AdmissionLock();
	const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
	while (admission.SharedWaitCount() == previous_waits) {
		if (capture.wait_for(std::chrono::milliseconds(1)) == std::future_status::ready) {
			capture.get();
			throw InvalidInputException("The capture finished before waiting for admission");
		}
		Require(std::chrono::steady_clock::now() < deadline, "The capture did not wait for admission");
	}
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
	Fixture(const string &root_p, const string &data_path, bool initialize, DBConfig *config = nullptr)
	    : root(root_p), database(nullptr, config), writer(database) {
		database.LoadStaticExtension<DucklakeExtension>();
		database.LoadStaticExtension<VortexExtension>();
		SQL(writer, "SET threads=2; LOAD parquet; ATTACH " +
		                Value("ducklake:" + root + "/metadata.ducklake").ToSQLString() + " AS lake(DATA_PATH " +
		                Value(data_path).ToSQLString() + ",DATA_INLINING_ROW_LIMIT 1000)");
		FileSystem::GetFileSystem(*writer.context).CreateDirectory(root + "/indexes");
		SQL(writer, "CREATE TABLE lake.items(id BIGINT,embedding FLOAT[],category VARCHAR,payload VARCHAR);"
		            "CALL ducklake_set_option('lake','experimental_data_file_format','vortex',table_name=>'items')");
		if (initialize) {
			Initialize();
		}
	}
	void Initialize() {
		SQL(writer, "SELECT * FROM ducklake_initialize_vortex_indexes('lake');"
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
		return DuckLakeVortexWriteBatch::Append(owner, *writer.context, "lake", "main", "items", "knn", request,
		                                       Rows(first), 1);
	}
	void Build(idx_t revision, const string &name) {
		SQL(writer, "CALL ducklake_flush_inlined_data('lake',table_name=>'items')");
		index = VortexIndex::BuildAndPublish(*writer.context, "lake", "main", "items", "knn", root + "/indexes/" + name,
		                                     2, revision, nullptr, "", {}, DuckLakeVortexIndexOperation::APPEND);
	}
	void SeedRows() {
		WriteBatch(0, "seed");
		Build(1, "seed.json");
		old_source = index.files[0].uri;
	}
	void FinishSeed(const string &enable_sql) {
		SQL(writer, enable_sql +
		                "SELECT * FROM ducklake_cancel_vortex_optimizer('lake','main','items','knn');"
		                "SELECT * FROM "
		                "ducklake_set_vortex_write_budget('lake','main','items','knn',1000,4194304,4096,67108864)");
		auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
		while (DuckLakeVortexOptimizer::Status(*writer.context, "lake")[0][16].GetValue<bool>()) {
			Require(std::chrono::steady_clock::now() < deadline, "Seed optimizer did not stop");
			std::this_thread::sleep_for(std::chrono::milliseconds(5));
		}
	}
	void Seed() {
		SeedRows();
		FinishSeed("SELECT * FROM ducklake_enable_vortex_optimizer('lake','main','items','knn'," +
		           Value(root + "/indexes").ToSQLString() + ",2,100000,4194304,100000,67108864);");
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

idx_t CapturedSnapshot(const DuckLakeVortexSearchResult &result) {
	using namespace duckdb_yyjson;
	std::unique_ptr<yyjson_doc, decltype(&yyjson_doc_free)> json(
	    yyjson_read(result.diagnostics.data(), result.diagnostics.size(), 0), yyjson_doc_free);
	Require(bool(json), "Search diagnostics are invalid");
	auto capture = yyjson_obj_get(yyjson_doc_get_root(json.get()), "native_capture");
	auto snapshot = yyjson_obj_get(capture, "snapshot_id");
	Require(yyjson_is_uint(snapshot), "Search diagnostics lost the captured snapshot");
	return yyjson_get_uint(snapshot);
}

constexpr const char *PROFILE_SQL = "SELECT * FROM ducklake_vortex_maintenance_profile('lake')";

struct ProfileSample {
	idx_t calls, total_ns, max_ns;
};

struct ProfileSnapshot {
	string id;
	std::map<string, ProfileSample> stages;
	idx_t CompletedLoopWaits() const {
		return stages.at("loop_wait_timeout").calls + stages.at("loop_wait_wake").calls;
	}
};

ProfileSnapshot ReadMaintenanceProfile(Connection &reader, const string &catalog = "lake") {
	auto rows = SQL(reader, "SELECT * FROM ducklake_vortex_maintenance_profile(" + Value(catalog).ToSQLString() + ")");
	Require(rows->RowCount() == 18, "Missing fixed maintenance profile stages");
	ProfileSnapshot profile;
	profile.id = rows->GetValue(0, 0).GetValue<string>();
	for (idx_t i = 0; i < rows->RowCount(); i++) {
		auto name = rows->GetValue(1, i).GetValue<string>();
		ProfileSample sample {rows->GetValue(2, i).GetValue<idx_t>(), rows->GetValue(3, i).GetValue<idx_t>(),
		                      rows->GetValue(4, i).GetValue<idx_t>()};
		Require(rows->GetValue(0, i).GetValue<string>() == profile.id && sample.max_ns <= sample.total_ns &&
		            (sample.calls || !sample.total_ns) && profile.stages.emplace(name, sample).second,
		        "Invalid maintenance profile identity or counters");
	}
	return profile;
}

void SourceMetadataProbes(Fixture &f) {
	constexpr idx_t max_bytes = 64 * 1024 * 1024;
	const string metadata = "__ducklake_metadata_lake.";
	const string snapshots = metadata + "ducklake_snapshot";
	SQL(f.writer, "CREATE TABLE lake.checkpoints(i INTEGER);"
	              "INSERT INTO lake.checkpoints VALUES (1);"
	              "INSERT INTO lake.checkpoints VALUES (2);"
	              "INSERT INTO lake.checkpoints VALUES (3)");
	SQL(f.writer, "UPDATE " + snapshots + " SET snapshot_time=now()-INTERVAL '2 hours';"
	              "UPDATE " + snapshots + " SET snapshot_time=now() WHERE snapshot_id=(SELECT min(snapshot_id) FROM " +
	                  snapshots + ")");
	const auto snapshot_count = f.Count(snapshots);
	Require(snapshot_count > 3, "The fixture needs retained, young and eligible snapshots");

	auto statements = std::make_shared<vector<string>>();
	f.writer.BeginTransaction();
	auto &catalog = Catalog::GetCatalog(*f.writer.context, "lake").Cast<DuckLakeCatalog>();
	f.writer.Commit();
	catalog.SetQueryCallback([statements](const string &sql, std::chrono::steady_clock::duration) {
		if (StringUtil::Contains(sql, "ducklake_vortex_source_retention_v1") ||
		    StringUtil::Contains(sql, "snapshot_id NOT IN") ||
		    StringUtil::Contains(sql, "ducklake_files_scheduled_for_deletion") ||
		    StringUtil::Contains(sql, "ducklake_vortex_source_deletions_v1")) {
			statements->push_back(sql);
		}
	});
	auto ready = [&] {
		Connection owner(f.database);
		auto &transaction = VortexIndex::Begin(owner, "lake");
		const auto result = Source::WorkReady(transaction, max_bytes);
		owner.Commit();
		return result;
	};
	f.Policy(2, 10800);
	Require(!ready(), "Readiness ignored the minimum snapshot age");
	f.Policy(100000, 3600);
	Require(!ready(), "Readiness ignored the retained snapshot count");
	f.Policy(2, 3600);
	statements->clear();
	Require(ready(), "Readiness missed eligible snapshots");
	Require(std::any_of(statements->begin(), statements->end(),
	                    [](const string &sql) {
		                    return StringUtil::StartsWith(sql, "SELECT EXISTS(") &&
		                           StringUtil::Contains(sql, "snapshot_id NOT IN");
	                    }),
	        "Source readiness must probe for existence");

	SQL(f.writer, "UPDATE " + metadata + "ducklake_vortex_source_retention_v1 "
	              "SET expired_snapshots=7,collected_files=3,collected_bytes=4096;"
	              "INSERT INTO " + metadata + "ducklake_files_scheduled_for_deletion VALUES "
	              "(101,'queued-a',true,now()),(102,'queued-b',true,now());"
	              "INSERT INTO " + metadata + "ducklake_vortex_source_deletions_v1 VALUES "
	              "(101,'queued-a',true,4096,1,2,'fixture')");
	Connection held(f.database);
	held.BeginTransaction();
	SQL(held, "SELECT count(*) FROM lake.checkpoints");
	Require(!ready(), "Readiness crossed an ordinary DuckLake transaction");

	statements->clear();
	auto status = SQL(f.writer, "SELECT * FROM ducklake_vortex_source_retention_status('lake')");
	catalog.SetQueryCallback({});
	Require(status->RowCount() == 1 && status->ColumnCount() == 10 && status->GetValue(0, 0).GetValue<bool>() &&
	            status->GetValue(9, 0).GetValue<bool>(),
	        "Source status lost the enabled or transaction blocker flag");
	const idx_t expected[] = {2, 3600, 7, 3, 4096, snapshot_count - 3, 2, 1};
	for (idx_t i = 0; i < sizeof(expected) / sizeof(expected[0]); i++) {
		Require(status->GetValue(i + 1, 0).GetValue<idx_t>() == expected[i],
		        "Source status changed a retention field or exact count");
	}
	held.Commit();
	std::cout << "Source status retention statements: " << statements->size() << std::endl;
	Require(statements->size() == 2, "Source status must read retention metadata in two statements");
	auto plan = SQL(f.writer, "EXPLAIN " + statements->back())->GetValue(1, 0).GetValue<string>();
	Require(StringUtil::Contains(plan, "TOP_N") && !StringUtil::Contains(plan, "ORDER_BY"),
	        "Source status must retain snapshots with a bounded TOP_N");
}

void MaintenanceIdleWait(Fixture &f) {
	f.Policy(1, 315360000);
	DuckLakeVortexOptimizer::EnableMaintenance(*f.writer.context, "lake", 2, 64 * 1024 * 1024);
	auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
	auto profile = ReadMaintenanceProfile(f.writer);
	while (!profile.stages.at("loop_wait_timeout").calls) {
		Require(std::chrono::steady_clock::now() < deadline, "Idle worker did not report a timed-out wait");
		std::this_thread::sleep_for(std::chrono::milliseconds(5));
		profile = ReadMaintenanceProfile(f.writer);
	}
	const auto timeout = profile.stages.at("loop_wait_timeout");
	const auto minimum_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::milliseconds(100)).count();
	Require(timeout.max_ns >= idx_t(minimum_ns), "The timeout did not include the completed wait");

	const auto previous_wakes = profile.stages.at("loop_wait_wake").calls;
	const auto id = profile.id;
	deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
	do {
		Require(std::chrono::steady_clock::now() < deadline, "Worker did not report a wait ending with a wake");
		f.Policy(1, 315360000);
		std::this_thread::sleep_for(std::chrono::milliseconds(5));
		profile = ReadMaintenanceProfile(f.writer);
	} while (profile.stages.at("loop_wait_wake").calls == previous_wakes);
	const auto wake = profile.stages.at("loop_wait_wake");
	Require(profile.id == id && wake.total_ns && wake.max_ns, "The wake lost its elapsed time or profile identity");
	std::cout << "Worker wait timeout max ns: " << timeout.max_ns << ", wake max ns: " << wake.max_ns << std::endl;
	DuckLakeVortexOptimizer::CancelMaintenance(*f.writer.context, "lake");
}

void WakeGuards(Fixture &f) {
	f.Rewrite();
	f.Policy();
	Connection first(f.database), second(f.database), owner(f.database), capture(f.database), unrelated(f.database);
	first.BeginTransaction();
	second.BeginTransaction();
	SQL(first, "SELECT count(*) FROM lake.items");
	SQL(second, "SELECT count(*) FROM lake.items");
	auto admission = DuckLakeVortexReadView::AdmissionLock().GetSharedLock([] {});
	DuckLakeVortexReadCaptureScope capture_scope(*capture.context);
	auto &finite = VortexIndex::Begin(capture, "lake");
	Require(finite.IsVortexReadCapture(), "The fixture did not register a finite capture");
	auto &transaction = VortexIndex::Begin(owner, "lake");
	auto &manager = TransactionManager::Get(transaction.GetCatalog().GetAttached()).Cast<DuckLakeTransactionManager>();
	struct Notifications {
		idx_t count = 0;
		bool saw_other = false;
	};
	auto notifications = std::make_shared<Notifications>();
	auto wake = [&manager, notifications] {
		notifications->count++;
		// Reentering the manager checks that delivery happens after transaction_lock is released.
		notifications->saw_other = manager.HasOtherTransactions(nullptr, false);
	};
	Require(!Source::WorkReady(transaction, 64 * 1024 * 1024, wake), "Wake registration ignored held transactions");
	capture.Commit();
	admission.reset();
	owner.Commit();
	unrelated.BeginTransaction();
	SQL(unrelated, "SELECT count(*) FROM lake.items");
	unrelated.Commit();
	Require(notifications->count == 0, "Finishing an unobserved transaction sent a source notification");
	first.Commit();
	Require(notifications->count == 1 && notifications->saw_other,
	        "Commit did not notify outside the lock while another transaction remained");
	Require(f.Collect().blocked && FileSystem::GetFileSystem(*f.writer.context).FileExists(f.old_source),
	        "A notification bypassed another ordinary transaction");
	auto &retry = VortexIndex::Begin(owner, "lake");
	Require(!Source::WorkReady(retry, 64 * 1024 * 1024, wake), "Remaining blocker was not rechecked");
	owner.Commit();
	second.Rollback();
	Require(notifications->count == 2 && !notifications->saw_other, "Rollback did not notify after removing the blocker");

	const auto expired = f.Collect(16, 1);
	auto status = Source::Status(*f.writer.context, "lake");
	Require(expired.expired && !expired.files && !status[0][6].GetValue<idx_t>() && status[0][7].GetValue<idx_t>(),
	        "The fixture needs queued source files without eligible snapshots");
	second.BeginTransaction();
	SQL(second, "SELECT count(*) FROM lake.items");
	auto &queued = VortexIndex::Begin(owner, "lake");
	bool blocked = false;
	Require(!Source::WorkReady(queued, 64 * 1024 * 1024, wake, &blocked) && blocked,
	        "Queued files did not register a source transaction blocker");
	owner.Commit();
	second.Commit();
	Require(notifications->count == 3 && !notifications->saw_other, "Queued source files lost their blocker notification");
	f.Drain();
	f.CheckReclaimed();

	second.BeginTransaction();
	SQL(second, "SELECT count(*) FROM lake.items");
	auto &idle = VortexIndex::Begin(owner, "lake");
	blocked = true;
	Require(!Source::WorkReady(idle, 64 * 1024 * 1024, wake, &blocked) && !blocked,
	        "An idle source probe subscribed without pending work");
	owner.Commit();
	second.Commit();
	Require(notifications->count == 3, "An idle source probe registered a wake");
	vortex_detail::ViewJSON json;
	auto facts = json.Object();
	json.Number(facts, "notifications", notifications->count);
	Write(f.root + "/wake-guards.json", json.Finish(facts));
}

void WakeRuntime(Fixture &f, const string &mode) {
	f.Rewrite();
	f.Policy();
	VortexIndex::CollectBuilds(*f.writer.context, "lake", 16, 64 * 1024 * 1024);
	VortexIndex::Collect(*f.writer.context, "lake", 16, 64 * 1024 * 1024);
	const bool late_reader = mode == "transaction-wake-late-reader";
	const bool new_reader = mode == "transaction-wake-new-reader";
	const bool replaced = mode == "transaction-wake-replaced";
	const bool cancelled = replaced || mode == "transaction-wake-cancel";
	if (replaced) {
		SQL(f.writer, "ATTACH " + Value("ducklake:" + f.root + "/mirror.ducklake").ToSQLString() +
		                  " AS mirror (DATA_PATH " + Value(f.root + "/mirror-data").ToSQLString() + ");"
		                  "SELECT * FROM ducklake_initialize_vortex_indexes('mirror');"
		                  "SELECT * FROM ducklake_initialize_vortex_optimizer('mirror')");
		DuckLakeVortexOptimizer::EnableMaintenance(*f.writer.context, "mirror", 2, 64 * 1024 * 1024);
	}
	struct State {
		Pause first, retry;
		idx_t deferrals = 0;
		bool progressed = false;
		std::promise<ProfileSnapshot> first_expiry;
	};
	auto state = std::make_shared<State>();
	auto progress = state->first_expiry.get_future();
	Connection held(f.database), newcomer(f.database), control(f.database);
	if (!late_reader) {
		held.BeginTransaction();
		SQL(held, "SELECT count(*) FROM lake.items");
	}
	weak_ptr<DatabaseInstance> database = f.database.instance;
	auto observe = [state, database, late_reader, new_reader, replaced](const string &stage) {
		if (late_reader && stage == "source_gc_waiting") {
			state->first.Stop();
		}
		if (late_reader && stage == "source_gc_deferred") {
			state->retry.Stop();
		}
		if (!late_reader && stage == "maintenance_source_deferred") {
			state->deferrals++;
			if (state->deferrals == 1) {
				state->first.Stop();
			} else if (new_reader) {
				state->retry.Stop();
			}
		}
		if (stage == "source_before_expire" && !state->progressed) {
			state->progressed = true;
			auto db = database.lock();
			Require(bool(db), "Database expired before the first source expiry");
			Connection probe(*db);
			state->first_expiry.set_value(ReadMaintenanceProfile(probe, replaced ? "mirror" : "lake"));
		}
	};
	DuckLakeVortexOptimizer::EnableMaintenance(*f.writer.context, "lake", 2, 64 * 1024 * 1024, observe);
	ReleasePause release_first {state->first};
	ReleasePause release_retry {state->retry};
	state->first.Wait();
	const auto before = ReadMaintenanceProfile(control);
	if (late_reader) {
		held.BeginTransaction();
		SQL(held, "SELECT count(*) FROM lake.items");
		state->first.Release();
		state->retry.Wait();
	}

	auto &fs = FileSystem::GetFileSystem(*f.writer.context);
	Require(fs.FileExists(f.old_source), "Source cleanup crossed its held ordinary transaction");
	if (cancelled) {
		DuckLakeVortexOptimizer::CancelMaintenance(*control.context, "lake");
		Require(!DuckLakeVortexOptimizer::MaintenanceStatus(*control.context, "lake")[0][1].GetValue<bool>(),
		        "Maintenance did not become disabled");
		if (replaced) {
			DuckLakeVortexOptimizer::ResumeMaintenance(*control.context, "lake", observe);
		}
	}
	if (mode == "transaction-wake-rollback") {
		held.Rollback();
	} else {
		held.Commit();
	}
	auto after = ReadMaintenanceProfile(control);
	auto notifications = after.stages.at("source_transaction_wake").calls;
	Require(notifications == (cancelled ? 0 : 1) && (after.id != before.id) == replaced,
	        "Transaction completion did not respect maintenance registration identity");
	Require(fs.FileExists(f.old_source), "The completion callback itself performed cleanup");

	if (new_reader) {
		newcomer.BeginTransaction();
		SQL(newcomer, "SELECT count(*) FROM lake.items");
	}
	const auto mirror_waits = replaced ? ReadMaintenanceProfile(control, "mirror").CompletedLoopWaits() : 0;
	state->first.Release();
	if (late_reader) {
		state->retry.Release();
	}
	if (new_reader) {
		state->retry.Wait();
		Require(f.Collect().blocked && fs.FileExists(f.old_source),
		        "A latched wake bypassed a reader that started before the retry");
		newcomer.Rollback();
		notifications = ReadMaintenanceProfile(control).stages.at("source_transaction_wake").calls;
		Require(notifications == 2, "The fresh probe did not register the new blocker");
		state->retry.Release();
	}
	if (cancelled && !replaced) {
		Require(!DuckLakeVortexOptimizer::MaintenanceStatus(*control.context, "lake")[0][1].GetValue<bool>(),
		        "Transaction completion resumed cancelled maintenance");
		DuckLakeVortexOptimizer::ResumeMaintenance(*control.context, "lake");
	}

	vortex_detail::ViewJSON json;
	auto facts = json.Object();
	json.Number(facts, "accepted_notifications", notifications);
	if (replaced) {
		Require(progress.wait_for(std::chrono::seconds(10)) == std::future_status::ready,
		        "Replacement did not reach its first guarded expiry");
		// The idle peer remembers the old iteration's wait after the lake changes profiles.
		const auto recorded_waits = progress.get().CompletedLoopWaits() - mirror_waits;
		json.Number(facts, "peer_waits_before_first_progress", recorded_waits);
		Write(f.root + "/transaction-wake.json", json.Finish(facts));
		Require(recorded_waits == 1, "An obsolete callback skipped the old iteration's wait");
	} else if (!cancelled) {
		Require(progress.wait_for(std::chrono::seconds(10)) == std::future_status::ready,
		        "Transaction completion never led to guarded source progress");
		const auto first_expiry = progress.get();
		const auto wait_delta = first_expiry.CompletedLoopWaits() - before.CompletedLoopWaits();
		const auto timeout_delta =
		    first_expiry.stages.at("loop_wait_timeout").calls - before.stages.at("loop_wait_timeout").calls;
		json.Number(facts, "waits_before_first_progress", wait_delta);
		json.Number(facts, "timeouts_before_first_progress", timeout_delta);
		Write(f.root + "/transaction-wake.json", json.Finish(facts));
		Require(wait_delta == 0, "A transaction-end notification before wait was lost");
	} else {
		Write(f.root + "/transaction-wake.json", json.Finish(facts));
	}
	auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
	while (true) {
		auto status = Source::Status(*control.context, "lake");
		if (!fs.FileExists(f.old_source) && !status[0][6].GetValue<idx_t>() && !status[0][7].GetValue<idx_t>() &&
		    !status[0][8].GetValue<idx_t>()) {
			break;
		}
		Require(std::chrono::steady_clock::now() < deadline, "Woken source worker did not reclaim its old source");
		std::this_thread::sleep_for(std::chrono::milliseconds(5));
	}
	DuckLakeVortexOptimizer::CancelMaintenance(*control.context, "lake");
	if (replaced) {
		DuckLakeVortexOptimizer::CancelMaintenance(*control.context, "mirror");
	}
	f.CheckReclaimed();
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
	constexpr idx_t max_bytes = 64 * 1024 * 1024;
	constexpr idx_t passes = 3;
	f.Rewrite();
	// Leave enough work for fixed build-first scheduling to starve both other classes.
	for (idx_t i = 0; i < passes * objects; i++) {
		f.index = VortexIndex::BuildAndPublish(*f.writer.context, "lake", "main", "items", "knn",
		                                     f.root + "/indexes/fairness-" + to_string(i) + ".json", 2,
		                                     f.index.revision);
	}
	f.Policy();
	Require(f.Count("__ducklake_metadata_lake.ducklake_vortex_builds_v1") >= passes * objects &&
	            f.Count("__ducklake_metadata_lake.ducklake_vortex_generations_v1 WHERE state='retired'") >= objects &&
	            Source::Status(*f.writer.context, "lake")[0][6].GetValue<idx_t>() >= objects,
	        "The fairness fixture did not queue all three GC classes");

	struct Counters {
		idx_t builds, generations, expired, files, bytes;
	};
	struct Progress {
		vector<Counters> completions;
		Pause finished;
	};
	auto progress = std::make_shared<Progress>();
	auto database = f.database.instance.get();
	DuckLakeVortexOptimizer::EnableMaintenance(
	    *f.writer.context, "lake", objects, max_bytes, [progress, database](const string &stage) {
		    if (stage != "maintenance_complete") {
			    return;
		    }
		    Connection probe(*database);
		    auto m = DuckLakeVortexOptimizer::MaintenanceStatus(*probe.context, "lake").at(0);
		    auto s = Source::Status(*probe.context, "lake").at(0);
		    progress->completions.push_back({m[8].GetValue<idx_t>(), m[9].GetValue<idx_t>(), s[3].GetValue<idx_t>(),
		                                     s[4].GetValue<idx_t>(), m[10].GetValue<idx_t>()});
		    if (progress->completions.size() == passes) {
			    progress->finished.Stop();
		    }
	    });
	ReleasePause release {progress->finished};
	progress->finished.Wait();
	DuckLakeVortexOptimizer::CancelMaintenance(*f.writer.context, "lake");
	progress->finished.Release();

	Counters previous {};
	for (idx_t i = 0; i < passes; i++) {
		auto &current = progress->completions[i];
		auto builds = current.builds - previous.builds;
		auto generations = current.generations - previous.generations;
		auto expired = current.expired - previous.expired;
		auto files = current.files - previous.files;
		auto bytes = current.bytes - previous.bytes;
		std::cout << "Pass " << i + 1 << ": builds=" << builds << " generations=" << generations
		          << " expired=" << expired << " files=" << files << " bytes=" << bytes << std::endl;
		Require(builds + generations + expired + files <= objects && bytes <= max_bytes,
		        "Maintenance exceeded its shared per-pass budget");
		Require(builds == (i == 0 ? objects : 0) && generations == (i == 1 ? objects : 0) &&
		            expired + files == (i == 2 ? objects : 0) && (i != 2 || expired > 0),
		        "The first three maintenance passes did not rotate budget priority");
		previous = current;
	}
	Connection reader(f.database);
	Require(f.Search(reader).rows.at(0).at(0).GetValue<int64_t>() == 32,
	        "Maintenance changed the current ANN winner");
	auto profile = ReadMaintenanceProfile(reader);
	for (auto stage : {"maintenance_total", "maintenance_probe", "maintenance_metadata", "build_recovery", "build_gc",
	                   "generation_gc", "source_gc", "source_wait", "source_metadata", "source_expire"}) {
		auto sample = profile.stages.at(stage);
		Require(sample.calls && sample.total_ns, "Missing measured maintenance stage " + string(stage));
	}
	SQL(f.writer, "DETACH lake");
	Attach(f.writer, f.root);
	Require(SQL(reader, PROFILE_SQL)->RowCount() == 0, "A reattached catalog inherited a stale profile");
}

void OptimizerProfile(const string &root, bool missing_policy) {
	struct Probe {
		idx_t calls = 0;
		Pause pending;
		bool registered = false; // Accessed only by the optimizer worker.
		std::promise<void> registered_exit, worker_exit;
	};
	auto probe = std::make_shared<Probe>();
	auto registered_exit = probe->registered_exit.get_future();
	auto worker_exit = probe->worker_exit.get_future();
	weak_ptr<DatabaseCacheEntry> database_close;
	std::exception_ptr failure;
	bool worker_started = false, paused = false;
	try {
		DBConfig config;
		// DuckDB releases its cache entry at the end of database teardown.
		config.db_cache_entry = make_shared_ptr<DatabaseCacheEntry>();
		database_close = config.db_cache_entry;
		Fixture f(root, root + "/data", true, &config);
		f.SeedRows();
		// Install the receipt on the first worker before enabling maintenance or resuming a policy.
		DuckLakeVortexOptimizerConfig seed;
		seed.catalog = "lake";
		seed.schema = "main";
		seed.table = "items";
		seed.index = "knn";
		seed.root = root + "/indexes";
		seed.dimension = 2;
		seed.flush_rows = seed.index_rows = 100000;
		seed.flush_bytes = 4194304;
		seed.index_bytes = 67108864;
		DuckLakeVortexOptimizer::Enable(*f.writer.context, seed, [probe](const string &) {
			if (!probe->registered) {
				probe->worker_exit.set_value_at_thread_exit();
				probe->registered = true;
				probe->registered_exit.set_value();
			}
		});
		worker_started = true;
		Require(registered_exit.wait_for(std::chrono::seconds(15)) == std::future_status::ready,
		        "Seed optimizer did not register its thread exit");
		registered_exit.get();
		f.FinishSeed("");
		Require(SQL(f.writer, PROFILE_SQL)->RowCount() == 0, "Profile appeared before maintenance registration");
		DuckLakeVortexOptimizer::EnableMaintenance(*f.writer.context, "lake", 2, 64 * 1024 * 1024,
		                                           [probe, missing_policy](const string &stage) {
			                                           if (missing_policy && stage == "maintenance_complete") {
				                                           probe->pending.Stop();
			                                           }
		                                           });
		if (missing_policy) {
			probe->pending.Wait();
			paused = true;
		}
		DuckLakeVortexOptimizer::Resume(*f.writer.context, "lake", "main", "items", missing_policy ? "missing" : "knn",
		                                [probe](const string &stage) {
			                                if (stage == "optimizer_before_probe" && ++probe->calls == 2) {
				                                probe->pending.Stop();
			                                }
		                                });
		probe->pending.Wait();
		paused = true;
		auto profile = ReadMaintenanceProfile(f.writer);
		for (auto stage : {"optimizer_total", "optimizer_probe", "optimizer_metadata"}) {
			auto sample = profile.stages.at(stage);
			Require(sample.calls && sample.total_ns, "Missing measured optimizer stage " + string(stage));
		}
		DuckLakeVortexOptimizer::Cancel(*f.writer.context, "lake", "main", "items", "knn");
		DuckLakeVortexOptimizer::CancelMaintenance(*f.writer.context, "lake");
	} catch (...) {
		failure = std::current_exception();
	}
	// The paused worker owns the database after the last client has closed.
	const bool database_held = !database_close.expired();
	const bool worker_running = worker_exit.wait_for(std::chrono::seconds(0)) == std::future_status::timeout;
	probe->pending.Release();
	if (worker_started) {
		Require(worker_exit.wait_for(std::chrono::seconds(20)) == std::future_status::ready,
		        "Optimizer worker did not exit after closing the profile fixture");
		worker_exit.get();
	}
	Require(database_close.expired(), "Database teardown was incomplete after closing the profile fixture");
	if (paused) {
		Require(database_held && worker_running, "Profile fixture did not close with background work in flight");
	}
	if (worker_started) {
		std::cout << "optimizer-profile worker exited and database teardown completed" << std::endl;
	}
	if (failure) {
		std::rethrow_exception(failure);
	}
}

string AddParquet(Fixture &f, const string &table, const string &name) {
	auto path = f.root + "/data/" + name;
	SQL(f.writer, "CREATE TABLE lake." + table + "(i BIGINT)");
	SQL(f.writer, "COPY (SELECT 7::BIGINT AS i) TO " + Value(path).ToSQLString() + " (FORMAT PARQUET)");
	SQL(f.writer,
	    "CALL ducklake_add_data_files('lake'," + Value(table).ToSQLString() + "," + Value(path).ToSQLString() + ")");
	return path;
}

void SharedSource(Fixture &f) {
	auto path = f.root + "/data/shared.parquet";
	SQL(f.writer, "CREATE TABLE lake.shared_first(i BIGINT); CREATE TABLE lake.shared_last(i BIGINT)");
	SQL(f.writer, "COPY (SELECT 7::BIGINT AS i) TO " + Value(path).ToSQLString() + " (FORMAT PARQUET)");
	SQL(f.writer, "CALL ducklake_add_data_files('lake','shared_first'," +
	                  Value("file://" + f.root + "/data//./shared.parquet").ToSQLString() + ")");
	SQL(f.writer,
	    "CALL ducklake_add_data_files('lake','shared_last'," + Value("file://localhost" + path).ToSQLString() + ")");
	f.Policy();
	SQL(f.writer, "DROP TABLE lake.shared_first");
	f.Drain();
	Require(Read(path).size() > 0 && f.Count("lake.shared_last") == 1,
	        "Expiry removed a file with another live registration");
	Require(f.Count("__ducklake_metadata_lake.ducklake_files_scheduled_for_deletion") == 1,
	        "Shared source was acknowledged while still referenced");

	Require(SQL(f.writer, "CALL ducklake_cleanup_old_files('lake',cleanup_all=>true,dry_run=>true)")->RowCount() == 0,
	        "Native cleanup preview included a shared source");
	SQL(f.writer, "CALL ducklake_cleanup_old_files('lake',cleanup_all=>true)");
	SQL(f.writer, "CALL ducklake_delete_orphaned_files('lake',cleanup_all=>true)");
	Require(Read(path).size() > 0 && f.Count("lake.shared_last") == 1,
	        "Native cleanup removed a file with another live registration");
	Require(f.Count("__ducklake_metadata_lake.ducklake_files_scheduled_for_deletion") == 1,
	        "Native cleanup acknowledged a shared source");

	SQL(f.writer, "DROP TABLE lake.shared_last");
	f.Collect();
	Require(f.Count("__ducklake_metadata_lake.ducklake_files_scheduled_for_deletion") == 2,
	        "The last expiry did not queue both registrations");
	SQL(f.writer, "UPDATE __ducklake_metadata_lake.ducklake_files_scheduled_for_deletion "
	              "SET schedule_start=TIMESTAMPTZ '2000-01-01 00:00:00+00' WHERE data_file_id = ("
	              "SELECT min(data_file_id) FROM __ducklake_metadata_lake.ducklake_files_scheduled_for_deletion)");
	SQL(f.writer, "CALL ducklake_cleanup_old_files('lake',older_than=>TIMESTAMPTZ '2010-01-01 00:00:00+00')");
	Require(Read(path).size() > 0 && f.Count("__ducklake_metadata_lake.ducklake_files_scheduled_for_deletion") == 2,
	        "Age-filtered native cleanup partially removed a shared source");
	f.Drain(1);
	Require(!FileSystem::GetFileSystem(*f.writer.context).FileExists(path), "Last registration left a shared source");
	Require(f.Count("__ducklake_metadata_lake.ducklake_files_scheduled_for_deletion") == 0 &&
	            f.Count("__ducklake_metadata_lake.ducklake_vortex_source_deletions_v1") == 0,
	        "Shared source left duplicate deletion work");
	Require(Source::Status(*f.writer.context, "lake")[0][4].GetValue<idx_t>() == 1,
	        "One shared source consumed more than one file from the budget");
}

void LinkedNativeCleanup(Fixture &f) {
	auto real = f.root + "/data/real";
	auto alias = f.root + "/data/alias";
	auto path = real + "/shared.parquet";
	FileSystem::GetFileSystem(*f.writer.context).CreateDirectory(real);
	Require(symlink(real.c_str(), alias.c_str()) == 0, "Could not create a shared directory alias");
	SQL(f.writer, "CREATE TABLE lake.shared_first(i BIGINT); CREATE TABLE lake.shared_last(i BIGINT)");
	SQL(f.writer, "COPY (SELECT 7::BIGINT AS i) TO " + Value(path).ToSQLString() + " (FORMAT PARQUET)");
	SQL(f.writer,
	    "CALL ducklake_add_data_files('lake','shared_first'," + Value(alias + "/shared.parquet").ToSQLString() + ")");
	SQL(f.writer, "CALL ducklake_add_data_files('lake','shared_last'," + Value(path).ToSQLString() + ")");
	SQL(f.writer, "DROP TABLE lake.shared_first");
	SQL(f.writer, "CALL ducklake_expire_snapshots('lake',older_than=>now())");
	Require(f.Count("__ducklake_metadata_lake.ducklake_files_scheduled_for_deletion") == 1,
	        "The native cleanup fixture did not queue the shared alias");
	SQL(f.writer, "CALL ducklake_cleanup_old_files('lake',cleanup_all=>true)");
	SQL(f.writer, "CALL ducklake_delete_orphaned_files('lake',cleanup_all=>true)");
	Require(f.Count("lake.shared_last") == 1 && Read(path).size() > 0,
	        "Native cleanup removed a file through an alias of its parent directory");
	Require(f.Count("__ducklake_metadata_lake.ducklake_files_scheduled_for_deletion") == 1,
	        "Native cleanup acknowledged a shared directory alias");

	auto outside = f.root + "/unmanaged";
	auto managed_alias = f.root + "/data/managed-alias";
	FileSystem::GetFileSystem(*f.writer.context).CreateDirectory(outside);
	Require(symlink(outside.c_str(), managed_alias.c_str()) == 0, "Could not create the managed namespace fixture");
	auto outside_file = outside + "/ordinary.parquet";
	SQL(f.writer, "CREATE TABLE lake.expired_alias(i BIGINT); CREATE TABLE lake.expired_direct(i BIGINT)");
	SQL(f.writer, "COPY (SELECT 9::BIGINT AS i) TO " + Value(outside_file).ToSQLString() + " (FORMAT PARQUET)");
	SQL(f.writer, "CALL ducklake_add_data_files('lake','expired_alias'," +
	                  Value(managed_alias + "/ordinary.parquet").ToSQLString() + ")");
	SQL(f.writer, "CALL ducklake_add_data_files('lake','expired_direct'," + Value(outside_file).ToSQLString() + ")");
	SQL(f.writer, "DROP TABLE lake.expired_alias; DROP TABLE lake.expired_direct");
	SQL(f.writer, "CALL ducklake_expire_snapshots('lake',older_than=>now())");
	f.Policy();
	Require(SQL(f.writer, "CALL ducklake_cleanup_old_files('lake',cleanup_all=>true,dry_run=>true)")->RowCount() == 0,
	        "Native cleanup listed a queued file owned by the managed namespace");
	SQL(f.writer, "CALL ducklake_cleanup_old_files('lake',cleanup_all=>true)");
	Require(Read(outside_file).size() > 0 &&
	            f.Count("__ducklake_metadata_lake.ducklake_files_scheduled_for_deletion") == 3,
	        "Native cleanup changed a file or queue key owned by the managed namespace");

	auto managed_link = AddParquet(f, "managed_link", "original-link.parquet");
	auto independent_link = outside + "/independent-link.parquet";
	Require(link(managed_link.c_str(), independent_link.c_str()) == 0,
	        "Could not create an independent directory entry");
	SQL(f.writer, "CREATE TABLE lake.external_link(i BIGINT)");
	SQL(f.writer, "CALL ducklake_add_data_files('lake','external_link'," + Value(independent_link).ToSQLString() + ")");
	SQL(f.writer, "DROP TABLE lake.managed_link; DROP TABLE lake.external_link");
	SQL(f.writer, "CALL ducklake_expire_snapshots('lake',older_than=>now())");
	SQL(f.writer, "CALL ducklake_cleanup_old_files('lake',cleanup_all=>true)");
	Require(FileSystem::GetFileSystem(*f.writer.context).FileExists(managed_link) &&
	            !FileSystem::GetFileSystem(*f.writer.context).FileExists(independent_link) &&
	            f.Count("__ducklake_metadata_lake.ducklake_files_scheduled_for_deletion") == 4,
	        "Native cleanup conflated independent hard links");
}

void FilenameReference(Fixture &f) {
	auto target = AddParquet(f, "filename_reference", "registered.parquet");
	auto alias = f.root + "/data/different-name.parquet";
	auto loop = f.root + "/data/cyclic-name.parquet";
	Require(symlink(target.c_str(), alias.c_str()) == 0 && symlink(loop.c_str(), loop.c_str()) == 0,
	        "Could not create the filename reference fixtures");
	Require(f.Count("__ducklake_metadata_lake.ducklake_data_file") == 1,
	        "Filename oracle requires exactly one native source");

	Connection owner(f.database);
	auto &transaction = VortexIndex::Begin(owner, "lake");
	auto change_path = [&](const string &path) {
		VortexIndex::Query(transaction, "UPDATE {METADATA_CATALOG}.ducklake_data_file SET path=" +
		                                    Value(path).ToSQLString() + ",path_is_relative=false");
	};
	auto &metadata = transaction.GetMetadataManager();
	change_path(alias);
	Require(metadata.HasFileReference(target, "/", false), "A differently named leaf alias lost its target reference");
	Require(!metadata.HasFileReference(f.root + "/data/unreferenced.parquet", "/", false),
	        "The filename oracle accepted an unreferenced path");

	change_path(loop);
	bool raised = false;
	try {
		metadata.HasFileReference(target, "/", false);
	} catch (const IOException &error) {
		raised = StringUtil::Contains(error.what(), "Cannot resolve native Vortex source reference: " + loop);
	}
	Require(raised, "The cyclic leaf must report its resolution error");
	owner.Rollback();
	Require(f.Count("lake.filename_reference") == 1 &&
	            f.Count("__ducklake_metadata_lake.ducklake_data_file WHERE suffix(path, 'registered.parquet')") == 1,
	        "The filename oracle changed its original registration");
}

void SeparateLeafLinks(Fixture &f) {
	auto path = AddParquet(f, "regular_source", "regular-source.parquet");
	auto alias = f.root + "/data/leaf-link.parquet";
	Require(symlink(path.c_str(), alias.c_str()) == 0, "Could not create the leaf-link fixture");
	SQL(f.writer, "CREATE TABLE lake.leaf_source(i BIGINT)");
	SQL(f.writer, "CALL ducklake_add_data_files('lake','leaf_source'," + Value(alias).ToSQLString() + ")");
	SQL(f.writer, "DROP TABLE lake.regular_source; DROP TABLE lake.leaf_source");
	f.Policy();
	Require(f.Collect().expired, "Leaf-link fixture did not expire the old snapshots");
	bool stopped = false;
	try {
		f.Collect(1, 64 * 1024 * 1024, [&](const string &stage) {
			if (stage == "source_delete_intent_committed") {
				stopped = true;
				throw IOException("Pause before unlink of the regular entry");
			}
		});
	} catch (const IOException &) {
		Require(stopped, "Could not reach the regular entry's acknowledgement");
	}
	auto &fs = FileSystem::GetFileSystem(*f.writer.context);
	Require(stopped && fs.FileExists(path) && fs.FileExists(alias),
	        "The recorded intent did not preserve both leaves before retry");
	Require(f.Collect(1).files == 1, "The collector did not acknowledge the regular entry");
	struct stat info;
	Require(!fs.FileExists(path) && !lstat(alias.c_str(), &info) && S_ISLNK(info.st_mode) &&
	            f.Count("__ducklake_metadata_lake.ducklake_files_scheduled_for_deletion") == 1 &&
	            f.Count("__ducklake_metadata_lake.ducklake_files_scheduled_for_deletion WHERE "
	                    "suffix(path,'leaf-link.parquet')") == 1,
	        "The collector conflated a symlink with its target's directory entry");
}

void PreparedNativeCleanup(Fixture &f) {
	FileSystem::GetFileSystem(*f.writer.context).CreateDirectory(f.root + "/data");
	auto path = AddParquet(f, "shared_first", "shared.parquet");
	SQL(f.writer, "CREATE TABLE lake.shared_last(i BIGINT)");
	SQL(f.writer, "CALL ducklake_add_data_files('lake','shared_last'," + Value(path).ToSQLString() + ")");
	SQL(f.writer, "DROP TABLE lake.shared_first");
	SQL(f.writer, "CALL ducklake_expire_snapshots('lake',older_than=>now())");
	Require(f.Count("__ducklake_metadata_lake.ducklake_files_scheduled_for_deletion") == 1,
	        "The prepared cleanup fixture did not queue a shared file");
	SQL(f.writer, "PREPARE prune_old AS SELECT * FROM ducklake_cleanup_old_files('lake',cleanup_all=>true)");
	f.Initialize();
	SQL(f.writer, "EXECUTE prune_old");
	Require(f.Count("lake.shared_last") == 1 && Read(path).size() > 0,
	        "Prepared cleanup skipped the newly initialized source guard");

	auto ordinary = AddParquet(f, "timestamped", "data-12:30.parquet");
	SQL(f.writer, "DROP TABLE lake.timestamped");
	SQL(f.writer, "CALL ducklake_expire_snapshots('lake',older_than=>now())");
	SQL(f.writer, "CALL ducklake_cleanup_old_files('lake',cleanup_all=>true)");
	Require(!FileSystem::GetFileSystem(*f.writer.context).FileExists(ordinary),
	        "Native cleanup rejected an ordinary POSIX filename");
	Require(f.Count("lake.shared_last") == 1 && Read(path).size() > 0,
	        "Native cleanup of an ordinary file removed a shared source");

	auto reusable = AddParquet(f, "reusable", "managed-12:31.parquet");
	SQL(f.writer, "DROP TABLE lake.reusable");
	SQL(f.writer, "CALL ducklake_expire_snapshots('lake',older_than=>now())");
	SQL(f.writer, "PREPARE stale_cleanup AS SELECT * FROM ducklake_cleanup_old_files('lake',cleanup_all=>true)");
	f.Policy();
	f.Drain();
	Require(!FileSystem::GetFileSystem(*f.writer.context).FileExists(reusable),
	        "Source GC did not retire the prepared cleanup candidate");
	Write(reusable, "unregistered replacement");
	SQL(f.writer, "EXECUTE stale_cleanup");
	Require(Read(reusable) == "unregistered replacement", "Prepared cleanup used an expired queue identity");

	auto rebound = AddParquet(f, "rebound", "rebound.parquet");
	SQL(f.writer, "DROP TABLE lake.rebound");
	SQL(f.writer, "CALL ducklake_expire_snapshots('lake',older_than=>now())");
	SQL(f.writer, "PREPARE rebound_cleanup AS SELECT * FROM ducklake_cleanup_old_files('lake',cleanup_all=>true)");
	Connection control(f.database);
	SQL(control, "DETACH __ducklake_metadata_lake");
	SQL(control, "ATTACH ':memory:' AS __ducklake_metadata_lake");
	bool rejected = false;
	try {
		SQL(f.writer, "EXECUTE rebound_cleanup");
	} catch (const std::exception &) {
		rejected = true;
	}
	Require(rejected && Read(rebound).size() > 0, "Prepared cleanup continued across a metadata reattachment");
}

void InlineOnlyRetention(Fixture &f) {
	SQL(f.writer, "SELECT * FROM ducklake_initialize_vortex_indexes('lake');"
	              "SELECT * FROM ducklake_initialize_vortex_optimizer('lake')");
	auto &fs = FileSystem::GetFileSystem(*f.writer.context);
	Require(!fs.DirectoryExists(f.root + "/data"), "Inline-only fixture unexpectedly has a managed directory");
	f.Policy();
	auto result = f.Collect();
	Require(result.expired && !result.files && f.Count("lake.items") == 0 && !fs.DirectoryExists(f.root + "/data"),
	        "Metadata-only expiry required or created a managed directory");
	SQL(f.writer, "INSERT INTO lake.items VALUES(1,[1,0],'A','inline')");
	Require(f.Count("lake.items") == 1, "Metadata-only expiry changed the current table");
}

void CleanupInitializationOrder(Fixture &f) {
	FileSystem::GetFileSystem(*f.writer.context).CreateDirectory(f.root + "/data");
	auto path = AddParquet(f, "expired", "expired.parquet");
	SQL(f.writer, "DROP TABLE lake.expired");
	SQL(f.writer, "CALL ducklake_expire_snapshots('lake',older_than=>now())");
	SQL(f.writer, "SELECT * FROM ducklake_initialize_vortex_indexes('lake')");
	auto &admission = DuckLakeVortexReadView::AdmissionLock();
	auto &publication = VortexIndex::PublicationLock();
	auto held = admission.GetSharedLock([] {});
	Connection cleaner(f.database), initializer(f.database);
	auto waiting_cleanup = admission.WaitingWriterCount();
	auto cleanup = std::async(std::launch::async,
	                          [&] { SQL(cleaner, "CALL ducklake_cleanup_old_files('lake',cleanup_all=>true)"); });
	std::future<void> initialization;
	try {
		auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
		while (admission.WaitingWriterCount() == waiting_cleanup) {
			Require(std::chrono::steady_clock::now() < deadline, "Cleanup did not reach ReadView admission");
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
		}
		auto waiting_init = publication.WaitingWriterCount();
		initialization =
		    std::async(std::launch::async, [&] { DuckLakeVortexOptimizer::Initialize(initializer, "lake"); });
		while (publication.WaitingWriterCount() == waiting_init) {
			Require(std::chrono::steady_clock::now() < deadline,
			        "Source policy initialization did not wait for cleanup");
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
		}
		held.reset();
		cleanup.get();
		initialization.get();
	} catch (...) {
		held.reset();
		throw;
	}
	Require(!FileSystem::GetFileSystem(*f.writer.context).FileExists(path) &&
	            !Source::Status(*f.writer.context, "lake")[0][0].GetValue<bool>(),
	        "Ordinary cleanup and source policy initialization did not both finish");
}

void MixedSizes(Fixture &f) {
	auto small = AddParquet(f, "small_source", "small.parquet");
	SQL(f.writer, "DROP TABLE lake.small_source");
	f.Collect();
	Require(f.Count("__ducklake_metadata_lake.ducklake_files_scheduled_for_deletion") == 2,
	        "Mixed-size fixture did not queue both expired files");
	{
		std::fstream oversized(f.old_source, std::ios::binary | std::ios::in | std::ios::out);
		oversized.seekp(64 * 1024 * 1024);
		oversized.put(' ');
		Require(bool(oversized), "Could not make an oversized expired source");
	}
	DuckLakeVortexOptimizer::EnableMaintenance(*f.writer.context, "lake", 1, 64 * 1024 * 1024);
	auto &fs = FileSystem::GetFileSystem(*f.writer.context);
	auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
	while (true) {
		auto status = DuckLakeVortexOptimizer::MaintenanceStatus(*f.writer.context, "lake")[0];
		Require(StringValue::Get(status[0]) != "failed", status.back().ToString());
		if (!status[2].GetValue<bool>() && !fs.FileExists(small)) {
			break;
		}
		Require(std::chrono::steady_clock::now() < deadline, "Oversized source blocked a file that fits the budget");
		std::this_thread::sleep_for(std::chrono::milliseconds(5));
	}
	DuckLakeVortexOptimizer::CancelMaintenance(*f.writer.context, "lake");
	Require(fs.FileExists(f.old_source) &&
	            f.Count("__ducklake_metadata_lake.ducklake_files_scheduled_for_deletion") == 1,
	        "Oversized source did not remain pending");
}

void InstallStatusGate(Fixture &f, const string &table, const string &column, Pause &pause, bool fail_read = false) {
	string gated;
	if (column == "enabled") {
		f.writer.CreateScalarFunction<bool, bool>("source_status_gate", &StatusField<bool>);
		gated = "source_status_gate(enabled)";
	} else {
		f.writer.CreateScalarFunction<int64_t, int64_t>("source_status_gate", &StatusField<int64_t>);
		gated = "source_status_gate(" + column + "::BIGINT)::UBIGINT";
	}
	auto qualified = "__ducklake_metadata_lake." + table;
	SQL(f.writer, "ALTER TABLE " + qualified + " RENAME TO status_backing;"
	              "CREATE VIEW " + qualified + " AS SELECT * REPLACE(" + gated + " AS " + column +
	                  ") FROM __ducklake_metadata_lake.status_backing");
	status_pause = &pause;
	fail_status_read = fail_read;
	pause_status = true;
}

void RestoreStatusTable(Fixture &f, const string &table) {
	SQL(f.writer, "DROP VIEW __ducklake_metadata_lake." + table +
	                  ";ALTER TABLE __ducklake_metadata_lake.status_backing RENAME TO " + table);
}

using StatusReader = vector<vector<Value>> (*)(ClientContext &, const string &);

void StatusReadiness(Fixture &f, StatusReader read, const string &table, const string &field, idx_t column,
                     const Value &expected) {
	f.Policy();
	Pause pause;
	InstallStatusGate(f, table, field, pause);
	auto pending = std::async(std::launch::async, [&] { return read(*f.writer.context, "lake"); });
	ReleasePause release {pause};
	pause.Wait();
	auto &admission = DuckLakeVortexReadView::AdmissionLock();
	auto exclusive = admission.TryGetExclusiveLock();
	const bool admitted = !exclusive;
	exclusive.reset();

	Connection held(f.database), owner(f.database);
	held.BeginTransaction();
	SQL(held, "SELECT count(*) FROM lake.items");
	auto &transaction = VortexIndex::Begin(owner, "lake");
	const bool ordinary_blocked = !Source::WorkReady(transaction, 64 * 1024 * 1024);
	held.Commit();
	const bool ready = Source::WorkReady(transaction, 64 * 1024 * 1024);
	owner.Commit();
	pause.Release();
	auto status = pending.get();
	RestoreStatusTable(f, table);
	Require(status.size() == 1 && status[0][column] == expected, "Finite status capture lost its result");
	Require(ordinary_blocked, "Status readiness ignored an ordinary transaction");
	Require(ready, "Source readiness discarded finite status work");
	Require(admitted, "Status did not hold finite read admission");
	f.Drain();
	Connection reader(f.database);
	Require(f.Search(reader).rows[0][0].GetValue<int64_t>() == 0, "Status capture changed the query winner");
}

void StatusErrorRollback(Fixture &f, const string &table, const string &field, const string &function) {
	f.Policy();
	Pause pause;
	InstallStatusGate(f, table, field, pause, true);
	auto pending = std::async(std::launch::async, [&] {
		return f.writer.Query("SELECT * FROM " + function + "('lake')");
	});
	ReleasePause release {pause};
	pause.Wait();

	auto clients = ConnectionManager::Get(*f.database.instance).GetConnectionList();
	shared_ptr<ClientContext> observed_status;
	for (auto &client : clients) {
		if (DuckLakeVortexReadView::IsCapturing(*client)) {
			Require(!observed_status, "Expected one private status capture");
			observed_status = client;
		}
	}
	Require(observed_status && observed_status->transaction.HasActiveTransaction(),
	        "The observer did not retain the status transaction");
	pause.Release();
	auto result = pending.get();
	Require(result->HasError(), "The public status query swallowed the injected metadata read error");
	Require(result->GetError().find(STATUS_POLICY_READ_ERROR) != string::npos,
	        "The public status query replaced the injected error: " + result->GetError());

	auto admission = DuckLakeVortexReadView::AdmissionLock().TryGetExclusiveLock();
	Require(bool(admission), "Failed status did not release shared read admission");
	const auto active_after_admission = observed_status->transaction.HasActiveTransaction();
	admission.reset();

	RestoreStatusTable(f, table);
	const auto collected = f.Collect(1, 1);
	Require(!collected.blocked, "Failed status transaction blocked eligible source GC");
	Require(collected.expired == 1 && collected.files == 0 && collected.bytes == 0,
	        "Source GC did not spend exactly one object on expiry");
	Require(!active_after_admission, "Status released admission before rolling back its transaction");
}

void CaptureReadiness(Fixture &f, bool eager) {
	const string catalog = eager ? "mirror" : "lake";
	if (eager) {
		SQL(f.writer, "ATTACH " + Value("ducklake:" + f.root + "/mirror.ducklake").ToSQLString() +
		                  " AS mirror (DATA_PATH " + Value(f.root + "/mirror-data").ToSQLString() +
		                  ");"
		                  "CREATE TABLE mirror.items(id BIGINT); INSERT INTO mirror.items VALUES (7);"
		                  "SELECT * FROM ducklake_initialize_vortex_indexes('mirror');"
		                  "SELECT * FROM ducklake_initialize_vortex_optimizer('mirror');"
		                  "SELECT * FROM ducklake_set_vortex_source_retention('mirror',true,1,0);"
		                  "SET GLOBAL immediate_transaction_mode=true");
	} else {
		f.Policy();
	}
	Pause capture;
	Connection reader(f.database), status_reader(f.database);
	auto pending = std::async(std::launch::async, [&] {
		return f.Search(reader, [&](const string &stage) {
			if (stage == "after_snapshot") {
				capture.Stop();
			}
		});
	});
	std::future<vector<Value>> status;
	ReleasePause release_capture {capture};
	capture.Wait();

	Connection held(f.database), owner(f.database);
	held.BeginTransaction();
	SQL(held, "SELECT count(*) FROM " + catalog + ".items");
	Require(SQL(held, "SELECT id FROM ducklake_vortex_search('lake','main','items','knn',[0,0]::FLOAT[],1)")
	                ->GetValue(0, 0)
	                .GetValue<int64_t>() == 0,
	        "ANN from a held transaction changed its query winner");
	auto &transaction = VortexIndex::Begin(owner, catalog);
	Require(!Source::WorkReady(transaction, 64 * 1024 * 1024), "ANN reclassified its ordinary caller");
	held.Commit();
	Require(Source::WorkReady(transaction, 64 * 1024 * 1024), "Readiness ignored work during a finite capture");
	owner.Commit();
	const auto before = Source::Status(*f.writer.context, catalog).at(0);
	Require(before.back().GetValue<bool>(), "Status did not count an active capture");

	if (!eager) {
		capture.Release();
		Require(pending.get().rows.at(0).at(0).GetValue<int64_t>() == 0, "The paused capture lost its query winner");
		return;
	}

	auto &admission = DuckLakeVortexReadView::AdmissionLock();
	const auto previous_waiters = admission.WaitingWriterCount();
	auto completed = std::make_shared<Pause>();
	ReleasePause release_completed {*completed};
	DuckLakeVortexOptimizer::EnableMaintenance(*f.writer.context, catalog, 1, 64 * 1024 * 1024,
	                                           [completed](const string &stage) {
		                                           if (stage == "maintenance_complete") {
			                                           completed->Stop();
		                                           }
	                                           });
	const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
	while (admission.WaitingWriterCount() == previous_waiters) {
		Require(std::chrono::steady_clock::now() < deadline, "The other lake never reached capture admission");
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
	}
	const auto previous_waits = admission.SharedWaitCount();
	status = std::async(std::launch::async, [&] { return Source::Status(*status_reader.context, catalog).at(0); });
	WaitForCaptureAdmission(status, previous_waits);
	auto queued = SQL(f.writer,
	                  "SELECT expired_snapshots FROM __ducklake_metadata_mirror.ducklake_vortex_source_retention_v1");
	Require(queued->GetValue(0, 0) == before[3],
	        "Maintenance expired snapshots before the other lake's capture completed");
	capture.Release();
	Require(pending.get().rows.at(0).at(0).GetValue<int64_t>() == 0, "The eager capture lost its query winner");
	completed->Wait();
	auto after = status.get();
	auto maintenance = DuckLakeVortexOptimizer::MaintenanceStatus(*f.writer.context, catalog).at(0);
	Require(after[3].GetValue<idx_t>() == before[3].GetValue<idx_t>() + 1 && maintenance[4].GetValue<idx_t>() == 1,
	        "The first source pass did not expire one snapshot with an object budget of one");
	DuckLakeVortexOptimizer::CancelMaintenance(*f.writer.context, catalog);
	completed->Release();
}

void CaptureGate(Fixture &f, bool cancel) {
	f.Policy();
	Connection caller(f.database);
	std::future<DuckLakeVortexSourceGCResult> collecting;
	auto &admission = DuckLakeVortexReadView::AdmissionLock();
	auto held = admission.GetSharedLock([] {});
	const auto previous_waiters = admission.WaitingWriterCount();
	const auto snapshots = f.Count("__ducklake_metadata_lake.ducklake_snapshot");
	collecting =
	    std::async(std::launch::async, [&] { return Source::Collect(*caller.context, "lake", 16, 64 * 1024 * 1024); });
	WaitForSourceAdmission(collecting, previous_waiters);
	Require(bool(Source::TransactionGate().TryGetExclusiveLock()), "Source collection froze transaction entry");
	Connection reader(f.database);
	Require(SQL(reader, "SELECT min(id) FROM lake.items")->GetValue(0, 0).GetValue<int64_t>() == 0,
	        "Transaction entry lost current data");
	if (cancel) {
		caller.context->Interrupt();
		Require(collecting.wait_for(std::chrono::seconds(15)) == std::future_status::ready,
		        "An interrupted collector kept waiting for capture admission");
		bool interrupted = false;
		try {
			collecting.get();
		} catch (const InterruptException &) {
			interrupted = true;
		}
		Require(interrupted && admission.WaitingWriterCount() == previous_waiters &&
		            bool(VortexIndex::PublicationLock().TryGetExclusiveLock()),
		        "Source collection interruption did not release its locks");
		Require(f.Count("__ducklake_metadata_lake.ducklake_snapshot") == snapshots,
		        "Interrupted source collection expired snapshots");
	}
	held.reset();
	auto result = cancel ? f.Collect() : collecting.get();
	Require(!result.blocked && result.expired, "Source collection did not progress after capture admission closed");
}

void EagerCaptureOrder(Fixture &f) {
	f.Policy();
	SQL(f.writer, "SET GLOBAL immediate_transaction_mode=true");
	Connection first(f.database), second(f.database);
	Pause pause;
	std::future<DuckLakeVortexSearchResult> capturing, queued;
	std::future<DuckLakeVortexSourceGCResult> collecting;
	ReleasePause release {pause};
	capturing = std::async(std::launch::async, [&] {
		return f.Search(first, [&](const string &stage) {
			if (stage == "after_snapshot") {
				pause.Stop();
			}
		});
	});
	pause.Wait();

	auto &admission = DuckLakeVortexReadView::AdmissionLock();
	const auto waiting_writers = admission.WaitingWriterCount();
	collecting = std::async(std::launch::async, [&] { return f.Collect(); });
	WaitForSourceAdmission(collecting, waiting_writers);

	const auto shared_waits = admission.SharedWaitCount();
	queued = std::async(std::launch::async, [&] { return f.Search(second); });
	WaitForCaptureAdmission(queued, shared_waits);
	pause.Release();
	auto result = collecting.get();
	Require(capturing.get().rows.at(0).at(0).GetValue<int64_t>() == 0 &&
	            queued.get().rows.at(0).at(0).GetValue<int64_t>() == 0,
	        "Source collection changed either admitted query result");
	Require(!result.blocked && result.expired, "A queued eager capture registered before source collection");
}

void ReboundCapture(Fixture &f) {
	Connection reader(f.database);
	std::future<DuckLakeVortexSearchResult> capture;
	auto &admission = DuckLakeVortexReadView::AdmissionLock();
	auto held = admission.GetExclusiveLock([] {});
	const auto previous_waits = admission.SharedWaitCount();
	capture = std::async(std::launch::async, [&] { return f.Search(reader); });
	WaitForCaptureAdmission(capture, previous_waits);
	SQL(f.writer, "DETACH lake");
	Attach(f.writer, f.root);
	held.reset();
	bool rejected = false;
	try {
		capture.get();
	} catch (const TransactionException &error) {
		rejected = StringUtil::Contains(error.what(), "catalog attachment changed");
	}
	Require(rejected, "A queued capture used the replacement catalog");
	Require(f.Search(reader).rows.at(0).at(0).GetValue<int64_t>() == 0, "A fresh capture lost the reattached data");
}

void ReboundOptimizerStatus(Fixture &f) {
	const auto replacement = f.root + "/replacement";
	FileSystem::GetFileSystem(*f.writer.context).CreateDirectory(replacement);
	{
		Fixture other(replacement, replacement + "/data", true);
		other.Seed();
		SQL(other.writer,
		    "SELECT * FROM ducklake_set_vortex_write_budget('lake','main','items','knn',777,4194304,4096,67108864)");
	}

	const string query = "SELECT max_inline_rows FROM ducklake_vortex_optimizer_status('lake')";
	Connection reader(f.database);
	Require(SQL(reader, query)->GetValue(0, 0).GetValue<idx_t>() == 1000, "Wrong original optimizer policy");
	std::future<unique_ptr<MaterializedQueryResult>> pending;
	auto &admission = DuckLakeVortexReadView::AdmissionLock();
	auto held = admission.GetExclusiveLock([] {});
	const auto previous_waits = admission.SharedWaitCount();
	pending = std::async(std::launch::async, [&] { return reader.Query(query); });
	WaitForCaptureAdmission(pending, previous_waits);
	SQL(f.writer, "DETACH lake");
	Attach(f.writer, replacement);
	held.reset();
	auto queued = pending.get();
	Require(queued->HasError() && StringUtil::Contains(queued->GetError(), "catalog attachment changed"),
	        "Queued optimizer status did not reject the replacement catalog");
	Require(SQL(reader, query)->GetValue(0, 0).GetValue<idx_t>() == 777, "A fresh status lost the replacement policy");
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
			observed.emplace_back(CapturedSnapshot(result), result.rows[0][0].GetValue<int64_t>());
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
		if (mode == "optimizer-profile" || mode == "optimizer-profile-missing-policy") {
			OptimizerProfile(root, mode == "optimizer-profile-missing-policy");
			return 0;
		}
		Fixture f(root, mode == "shared-source" ? root + "//./data" : root + "/data",
		          mode != "prepared-native-cleanup" && mode != "cleanup-init-order" && mode != "inline-only");
		if (mode == "shared-source") {
			SharedSource(f);
			return 0;
		}
		if (mode == "linked-native-cleanup") {
			LinkedNativeCleanup(f);
			return 0;
		}
		if (mode == "filename-reference") {
			FilenameReference(f);
			return 0;
		}
		if (mode == "leaf-link") {
			SeparateLeafLinks(f);
			return 0;
		}
		if (mode == "prepared-native-cleanup") {
			PreparedNativeCleanup(f);
			return 0;
		}
		if (mode == "cleanup-init-order") {
			CleanupInitializationOrder(f);
			return 0;
		}
		if (mode == "inline-only") {
			InlineOnlyRetention(f);
			return 0;
		}
		if (mode == "source-metadata-probes") {
			SourceMetadataProbes(f);
			return 0;
		}
		f.Seed();
	if (mode == "transaction-wake-guards") {
		WakeGuards(f);
		return 0;
	}
	if (StringUtil::StartsWith(mode, "transaction-wake-")) {
		WakeRuntime(f, mode);
		return 0;
	}
		if (mode == "maintenance-fairness-1" || mode == "maintenance-fairness-2") {
			MaintenanceFairness(f, mode == "maintenance-fairness-1" ? 1 : 2);
			return 0;
		}
		if (mode == "maintenance-idle-wait") {
			MaintenanceIdleWait(f);
			return 0;
		}
		if (mode == "concurrent") {
			Concurrent(f);
			return 0;
		}
		if (mode == "status-readiness") {
			StatusReadiness(f, Source::Status, "ducklake_vortex_source_retention_v1", "enabled", 0, Value::BOOLEAN(true));
			return 0;
		}
		if (mode == "optimizer-status-readiness") {
			SQL(f.writer, "SET GLOBAL immediate_transaction_mode=true");
			StatusReadiness(f, DuckLakeVortexOptimizer::Status, "ducklake_vortex_write_budgets_v1", "max_inline_rows",
			                17, Value::UBIGINT(1000));
			return 0;
		}
		if (mode == "maintenance-status-readiness") {
			VortexIndex::CollectBuilds(*f.writer.context, "lake", 16, 64 * 1024 * 1024);
			DuckLakeVortexOptimizer::EnableMaintenance(*f.writer.context, "lake", 2, 64 * 1024 * 1024);
			DuckLakeVortexOptimizer::CancelMaintenance(*f.writer.context, "lake");
			StatusReadiness(f, DuckLakeVortexOptimizer::MaintenanceStatus, "ducklake_vortex_maintenance_v1", "enabled",
			                1, Value::BOOLEAN(false));
			return 0;
		}
		if (mode == "status-error-rollback") {
			StatusErrorRollback(f, "ducklake_vortex_source_retention_v1", "enabled",
			                    "ducklake_vortex_source_retention_status");
			return 0;
		}
		if (mode == "optimizer-status-error-rollback") {
			StatusErrorRollback(f, "ducklake_vortex_write_budgets_v1", "max_inline_rows",
			                    "ducklake_vortex_optimizer_status");
			return 0;
		}
		if (mode == "capture-readiness" || mode == "capture-readiness-eager") {
			CaptureReadiness(f, mode == "capture-readiness-eager");
			return 0;
		}
		if (mode == "capture-gate" || mode == "cancel-capture-wait") {
			CaptureGate(f, mode == "cancel-capture-wait");
			return 0;
		}
		if (mode == "eager-capture-order") {
			EagerCaptureOrder(f);
			return 0;
		}
		if (mode == "rebound-capture") {
			ReboundCapture(f);
			return 0;
		}
		if (mode == "rebound-optimizer-status") {
			ReboundOptimizerStatus(f);
			return 0;
		}
		if (mode == "held" || mode == "admission") {
			const string latest = "SELECT max(snapshot_id) FROM __ducklake_metadata_lake.ducklake_snapshot";
			const auto snapshot = SQL(f.writer, latest)->GetValue(0, 0).GetValue<idx_t>();
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
				const auto waiting = DuckLakeVortexReadView::AdmissionLock().WaitingWriterCount();
				auto collecting = std::async(std::launch::async, [&] { return f.Collect(); });
				ReleasePause release_on_error {pause};
				WaitForSourceAdmission(collecting, waiting);
				Require(bool(Source::TransactionGate().TryGetExclusiveLock()),
				        "Source collection froze transactions before native capture completed");
				pause.Release();
				Require(pending.get().rows[0][0].GetValue<int64_t>() == 0, "Admitted query lost its native view");
				auto result = collecting.get();
				Require(!result.blocked && result.expired, "Source collector did not progress after native capture");
				return 0;
			} else {
				f.Rewrite();
				VortexIndex::Collect(*f.writer.context, "lake", 16, 64 * 1024 * 1024);
				f.Drain();
				f.CheckReclaimed();
			}
			pause.Release();
			auto captured = pending.get();
			Require(captured.rows[0][0].GetValue<int64_t>() == 0 && CapturedSnapshot(captured) == snapshot,
			        "Held view lost its typed result or captured snapshot after GC");
			Require(SQL(f.writer, latest)->GetValue(0, 0).GetValue<idx_t>() > snapshot,
			        "The fixture did not publish a newer snapshot while the query was held");
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
		const bool blocked_generation = mode == "source-only-blocked-generation";
		if (mode == "source-only" || blocked_generation) {
			auto &fs = FileSystem::GetFileSystem(*f.writer.context);
			const idx_t expected_retired = blocked_generation ? 1 : 0;
			auto byte_budget = blocked_generation ? fs.OpenFile(f.old_source, FileFlags::FILE_FLAGS_READ)->GetFileSize()
			                                      : 64 * 1024 * 1024;
			const string retired =
			    "__ducklake_metadata_lake.ducklake_vortex_generations_v1 WHERE state IN ('retired','deleting')";
			VortexIndex::CollectBuilds(*f.writer.context, "lake", 16, 64 * 1024 * 1024);
			VortexIndex::Collect(*f.writer.context, "lake", 16, byte_budget);
			Require(f.Count("__ducklake_metadata_lake.ducklake_vortex_builds_v1") == 0,
			        "Source-only fixture retained build work");
			Require(f.Count(retired) == expected_retired &&
			            f.Count(retired + " AND artifact_bytes>" + to_string(byte_budget)) == expected_retired,
			        "Source-only fixture has unexpected generation work");
			auto generations = VortexIndex::CollectionLock().GetExclusiveLock([] {});
			auto empty_phases = std::make_shared<std::atomic<idx_t>>(0);
			auto completed = std::make_shared<std::atomic<bool>>(false);
			auto database = f.database.instance.get();
			DuckLakeVortexOptimizer::EnableMaintenance(
			    *f.writer.context, "lake", 2, byte_budget, [empty_phases, completed, database](const string &stage) {
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
			const bool finished_while_held = *completed && *empty_phases == 0;
			generations.reset();
			DuckLakeVortexOptimizer::CancelMaintenance(*f.writer.context, "lake");
			deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
			while (DuckLakeVortexOptimizer::MaintenanceStatus(*f.writer.context, "lake")[0][2].GetValue<bool>()) {
				Require(std::chrono::steady_clock::now() < deadline, "Source-only worker did not stop");
				std::this_thread::sleep_for(std::chrono::milliseconds(5));
			}
			Require(finished_while_held, "Source-only maintenance waited on empty index collection");
			Require(f.Count(retired) == expected_retired, "Source-only maintenance changed the blocked generation");
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
		if (mode == "mixed-size") {
			MixedSizes(f);
			return 0;
		}
		if (mode == "immediate") {
			SQL(f.writer, "SET GLOBAL immediate_transaction_mode=true");
			Require(f.Collect(1).expired == 1, "Explicit source expiry stalled in immediate transaction mode");
			DuckLakeVortexOptimizer::EnableMaintenance(*f.writer.context, "lake", 1, 64 * 1024 * 1024);
			auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
			while (true) {
				auto source = Source::Status(*f.writer.context, "lake")[0];
				auto maintenance = DuckLakeVortexOptimizer::MaintenanceStatus(*f.writer.context, "lake")[0];
				Require(StringValue::Get(maintenance[0]) != "failed", maintenance.back().ToString());
				if (!source[6].GetValue<idx_t>() && !source[7].GetValue<idx_t>() && !maintenance[2].GetValue<bool>()) {
					break;
				}
				Require(std::chrono::steady_clock::now() < deadline,
				        "Source maintenance stalled in immediate transaction mode");
				std::this_thread::sleep_for(std::chrono::milliseconds(5));
			}
			DuckLakeVortexOptimizer::CancelMaintenance(*f.writer.context, "lake");
			f.CheckReclaimed();
			return 0;
		}
		if (mode == "permissions") {
			f.Collect();
			auto contents = Read(f.old_source);
			SQL(f.writer, "SET allowed_directories=[" + Value(f.root + "/indexes").ToSQLString() + "]");
			SQL(f.writer, "SET enable_external_access=false");
			bool denied = false;
			try {
				f.Collect();
			} catch (const PermissionException &error) {
				denied = string(error.what()).find("disabled by configuration") != string::npos;
			}
			Require(denied && Read(f.old_source) == contents, "Restricted managed directory was not protected");
			return 0;
		}
		if (mode == "detach") {
			f.Collect();
			bool detached = false, rejected = false;
			try {
				f.Collect(16, 64 * 1024 * 1024, [&](const string &stage) {
					if (stage == "source_before_unlink" && !detached) {
						Connection control(f.database);
						SQL(control, "DETACH lake");
						detached = true;
					}
				});
			} catch (const std::exception &) {
				rejected = true;
			}
			Require(detached && rejected && FileSystem::GetFileSystem(*f.writer.context).FileExists(f.old_source),
			        "Collection lost attachment ownership before unlink");
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
			Require(f.Collect().files == 0 && FileSystem::GetFileSystem(*f.writer.context).FileExists(file.uri),
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
		if (mode == "readiness-error") {
			f.Collect();
			auto backup = root + "/retired-source.backup";
			Require(std::rename(f.old_source.c_str(), backup.c_str()) == 0, "Could not park the expired fixture file");
			DuckLakeVortexOptimizer::EnableMaintenance(*f.writer.context, "lake", 16, 64 * 1024 * 1024);
			auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
			while (true) {
				auto now = DuckLakeVortexOptimizer::MaintenanceStatus(*f.writer.context, "lake")[0];
				if (StringValue::Get(now[0]) == "failed" && !now[2].GetValue<bool>()) {
					break;
				}
				Require(std::chrono::steady_clock::now() < deadline, "Missing-source failure was not reported");
				std::this_thread::sleep_for(std::chrono::milliseconds(5));
			}
			Require(
			    f.Count(
			        "__ducklake_metadata_lake.ducklake_vortex_maintenance_v1 WHERE NOT enabled AND state='failed'") ==
			        1,
			    "Missing-source failure was not persisted");
			Require(std::rename(backup.c_str(), f.old_source.c_str()) == 0,
			        "Could not restore the expired fixture file");
			f.Drain();
			f.CheckReclaimed();
			return 0;
		}
		if (mode == "manual") {
			f.Collect();
			SQL(f.writer, "BEGIN");
			Require(SQL(f.writer, "CALL ducklake_cleanup_old_files('lake',cleanup_all=>true)")->RowCount() == 0,
			        "Native cleanup attempted a managed source deletion");
			SQL(f.writer, "ROLLBACK");
			Require(FileSystem::GetFileSystem(*f.writer.context).FileExists(f.old_source) &&
			            f.Count("__ducklake_metadata_lake.ducklake_files_scheduled_for_deletion") == 1,
			        "Rollback left a managed source missing without a committed intent");
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
			            f.Count("__ducklake_metadata_lake.ducklake_files_scheduled_for_deletion") == 1 &&
			            FileSystem::GetFileSystem(*f.writer.context).FileExists(f.old_source),
			        "Native cleanup crossed durable source deletion ownership");
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
				auto changed_bytes = Read(f.old_source);
				SQL(f.writer, "CALL ducklake_cleanup_old_files('lake',cleanup_all=>true)");
				Require(Read(f.old_source) == changed_bytes, "Native cleanup removed a changed intent-owned source");
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
			const auto held_at = std::chrono::steady_clock::now();
			auto before_profile = ReadMaintenanceProfile(cancellation);
			auto &gate = Source::TransactionGate();
			const auto waits = gate.SharedWaitCount();
			auto cancelled = std::async(
			    std::launch::async, [&] { DuckLakeVortexOptimizer::CancelMaintenance(*cancellation.context, "lake"); });
			ReleasePause release_on_error {pause};
			auto waiting_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
			while (gate.SharedWaitCount() == waits) {
				Require(std::chrono::steady_clock::now() < waiting_deadline &&
				            cancelled.wait_for(std::chrono::milliseconds(1)) != std::future_status::ready,
				        "Cancellation did not reach transaction admission");
			}
			const auto held_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
			                         std::chrono::steady_clock::now() - held_at)
			                         .count();
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
			auto cancelled_profile = ReadMaintenanceProfile(cancellation);
			Require(cancelled_profile.id == before_profile.id &&
			            cancelled_profile.stages.at("source_metadata").max_ns >= idx_t(held_ns),
			        "Cancelled source collection lost its held wall time");
			DuckLakeVortexOptimizer::ResumeMaintenance(*f.writer.context, "lake");
			Require(ReadMaintenanceProfile(cancellation).id != before_profile.id,
			        "Explicit resume reused the cancelled maintenance profile");
			while (FileSystem::GetFileSystem(*f.writer.context).FileExists(f.old_source)) {
				Require(std::chrono::steady_clock::now() < deadline, "Resumed source worker did not finish");
				std::this_thread::sleep_for(std::chrono::milliseconds(5));
			}
			DuckLakeVortexOptimizer::CancelMaintenance(*f.writer.context, "lake");
			while (DuckLakeVortexOptimizer::MaintenanceStatus(*f.writer.context, "lake")[0][2].GetValue<bool>()) {
				Require(std::chrono::steady_clock::now() < deadline, "Resumed source worker did not stop");
				std::this_thread::sleep_for(std::chrono::milliseconds(5));
			}
			auto completed_profile = ReadMaintenanceProfile(cancellation);
			for (auto stage : {"source_artifact", "source_unlink"}) {
				auto sample = completed_profile.stages.at(stage);
				Require(sample.calls && sample.total_ns, "Resumed source collection lost timing for " + string(stage));
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
			Connection native(f.database), reader(f.database);
			auto &gate = Source::TransactionGate();
			auto &admission = DuckLakeVortexReadView::AdmissionLock();
			const auto native_waits = gate.SharedWaitCount();
			const auto view_waits = admission.SharedWaitCount();
			auto native_read = std::async(std::launch::async, [&] {
				return SQL(native, "SELECT min(id) FROM lake.items")->GetValue(0, 0).GetValue<int64_t>();
			});
			auto view = std::async(std::launch::async, [&] { return f.Search(reader); });
			ReleasePause release_on_error {pause};
			auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
			while (gate.SharedWaitCount() == native_waits || admission.SharedWaitCount() == view_waits) {
				Require(std::chrono::steady_clock::now() < deadline, "Readers did not reach their admission locks");
				std::this_thread::sleep_for(std::chrono::milliseconds(1));
			}
			pause.Release();
			collecting.get();
			Require(native_read.get() == 32 && view.get().rows[0][0].GetValue<int64_t>() == 32,
			        "Readers lost current data after deletion");
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
