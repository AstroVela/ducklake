#include "native_vortex_fixture.hpp"
#include "common/ducklake_vortex_json.hpp"
#include "ducklake_extension.hpp"
#include "duckdb/common/file_system.hpp"
#include "storage/ducklake_vortex_index_catalog.hpp"
#include "storage/ducklake_vortex_optimizer.hpp"
#include "storage/ducklake_vortex_search.hpp"
#include "storage/ducklake_vortex_source_retention.hpp"
#include "storage/ducklake_vortex_write.hpp"
#include "vortex_extension.hpp"

#include <atomic>
#include <iostream>
#include <map>
#include <thread>
#include <sys/stat.h>
#include <unistd.h>

using namespace duckdb;
using namespace duckdb::vortex_test;
using Source = DuckLakeVortexSourceRetention;
using VortexIndex = DuckLakeVortexIndex;

namespace {
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
	Fixture(const string &root_p, const string &data_path, bool initialize)
	    : root(root_p), database(nullptr), writer(database) {
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
		                                       Rows(first));
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
		f.Seed();
		if (mode == "concurrent") {
			Concurrent(f);
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
				Require(f.Collect().blocked, "Capture admission was not protected from source expiry");
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
			DuckLakeVortexOptimizer::ResumeMaintenance(*f.writer.context, "lake");
			while (FileSystem::GetFileSystem(*f.writer.context).FileExists(f.old_source)) {
				Require(std::chrono::steady_clock::now() < deadline, "Resumed source worker did not finish");
				std::this_thread::sleep_for(std::chrono::milliseconds(5));
			}
			DuckLakeVortexOptimizer::CancelMaintenance(*f.writer.context, "lake");
			while (DuckLakeVortexOptimizer::MaintenanceStatus(*f.writer.context, "lake")[0][2].GetValue<bool>()) {
				Require(std::chrono::steady_clock::now() < deadline, "Resumed source worker did not stop");
				std::this_thread::sleep_for(std::chrono::milliseconds(5));
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
