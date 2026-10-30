#include "storage/ducklake_vortex_source_retention.hpp"

#include "common/ducklake_vortex_gc_io.hpp"
#include "common/ducklake_vortex_json.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/storage/storage_lock.hpp"
#include "mbedtls_wrapper.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_metadata_manager.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_transaction_manager.hpp"
#include "storage/ducklake_vortex_index_catalog.hpp"
#include "storage/ducklake_vortex_read_view.hpp"

namespace duckdb {
namespace {
using VortexIndex = DuckLakeVortexIndex;
using namespace vortex_detail;
constexpr const char *POLICY = "ducklake_vortex_source_retention_v1";
constexpr const char *DELETIONS = "ducklake_vortex_source_deletions_v1";
constexpr const char *SCHEDULED = "ducklake_files_scheduled_for_deletion";
constexpr const char *COLLECTOR = "ducklake_vortex_source_collector";
constexpr idx_t MAX_BYTES = 64 * 1024 * 1024;

struct CollectorContext final : public ClientContextState {};
struct Policy {
	bool enabled;
	idx_t keep, age;
};

Policy Load(DuckLakeTransaction &transaction) {
	auto result = VortexIndex::Query(
	    transaction, "SELECT enabled,keep_snapshots,min_age_seconds FROM {METADATA_CATALOG}." + string(POLICY));
	auto row = result->Fetch();
	if (!row || row->size() != 1) {
		throw InvalidInputException("Vortex source retention is not initialized");
	}
	Policy p {row->GetValue(0, 0).GetValue<bool>(), row->GetValue(1, 0).GetValue<idx_t>(),
	          row->GetValue(2, 0).GetValue<idx_t>()};
	if (!p.keep || p.keep > 100000 || p.age > 315360000) {
		throw InvalidInputException("Invalid Vortex source retention policy");
	}
	return p;
}

string Eligible(const Policy &p) {
	return "snapshot_id NOT IN (SELECT snapshot_id FROM {METADATA_CATALOG}.ducklake_snapshot "
	       "ORDER BY snapshot_id DESC LIMIT " +
	       to_string(p.keep) + ") AND epoch_us(snapshot_time) < epoch_us(now())-" + to_string(p.age * 1000000);
}

idx_t Count(DuckLakeTransaction &transaction, const string &sql) {
	return VortexIndex::Query(transaction, sql)->Fetch()->GetValue(0, 0).GetValue<idx_t>();
}

DuckLakeTransactionManager &Manager(DuckLakeCatalog &catalog) {
	return TransactionManager::Get(catalog.GetAttached()).Cast<DuckLakeTransactionManager>();
}

struct Owner {
	Connection connection;
	DuckLakeTransaction &transaction;
	Owner(ClientContext &context, const string &catalog)
	    : connection(*context.db), transaction(VortexIndex::Begin(connection, catalog)) {
	}
};

struct File {
	idx_t id = 0, bytes = 0, device = 0, inode = 0;
	string raw, path, relative, digest;
	bool is_relative = false, registered = false;
};

string Where(const File &file) {
	return " WHERE data_file_id=" + to_string(file.id) + " AND path=" + Value(file.raw).ToSQLString() +
	       " AND path_is_relative=" + string(file.is_relative ? "true" : "false");
}

string Root(DuckLakeTransaction &transaction) {
	auto path =
	    FileSystem::GetFileSystem(*transaction.GetConnection().context).ExpandPath(transaction.GetCatalog().DataPath());
	while (path.size() > 1 && path.back() == '/') {
		path.pop_back();
	}
	return path;
}

vector<File> Candidates(DuckLakeTransaction &transaction, const string &root, idx_t limit) {
	auto result = VortexIndex::Query(
	    transaction, "SELECT coalesce(s.data_file_id,d.data_file_id),coalesce(s.path,d.path),"
	                 "coalesce(s.path_is_relative,d.path_is_relative),d.file_bytes,d.device,d.inode,d.digest "
	                 "FROM {METADATA_CATALOG}." +
	                     string(SCHEDULED) + " s FULL OUTER JOIN {METADATA_CATALOG}." + DELETIONS +
	                     " d USING(data_file_id,path,path_is_relative) ORDER BY d.digest IS NULL,"
	                     "coalesce(s.data_file_id,d.data_file_id),coalesce(s.path,d.path) LIMIT " +
	                     to_string(limit));
	vector<File> files;
	while (auto chunk = result->Fetch()) {
		for (idx_t row = 0; row < chunk->size(); row++) {
			File file;
			file.id = chunk->GetValue(0, row).GetValue<idx_t>();
			file.raw = StringValue::Get(chunk->GetValue(1, row));
			file.is_relative = chunk->GetValue(2, row).GetValue<bool>();
			file.path = file.is_relative ? root + "/" + file.raw : file.raw;
			while (file.path.find("//") != string::npos) {
				file.path = StringUtil::Replace(file.path, "//", "/");
			}
			if (!StringUtil::StartsWith(file.path, root + "/")) {
				throw InvalidInputException("Vortex source cleanup path is outside managed data");
			}
			file.relative = file.path.substr(root.size() + 1);
			CheckRelative(file.relative, false);
			file.registered = !chunk->GetValue(6, row).IsNull();
			if (file.registered) {
				file.bytes = chunk->GetValue(3, row).GetValue<idx_t>();
				file.device = chunk->GetValue(4, row).GetValue<idx_t>();
				file.inode = chunk->GetValue(5, row).GetValue<idx_t>();
				file.digest = StringValue::Get(chunk->GetValue(6, row));
			}
			files.push_back(std::move(file));
		}
	}
	return files;
}

bool Inspect(Parent &parent, File &file, bool contents) {
	struct stat info;
	if (!parent.directory || fstatat(parent.directory->fd, parent.name.c_str(), &info, AT_SYMLINK_NOFOLLOW)) {
		if (file.registered && (!parent.directory || errno == ENOENT)) {
			return false;
		}
		throw IOException("Scheduled Vortex source is missing without a registered deletion intent");
	}
	if (!S_ISREG(info.st_mode) || info.st_nlink != 1 || info.st_size < 0 || idx_t(info.st_size) > MAX_BYTES) {
		throw InvalidInputException("Vortex source cleanup requires a bounded regular file without links");
	}
	if (file.registered &&
	    (file.bytes != idx_t(info.st_size) || file.device != idx_t(info.st_dev) || file.inode != idx_t(info.st_ino))) {
		throw InvalidInputException("Registered Vortex source identity changed");
	}
	file.bytes = info.st_size;
	file.device = info.st_dev;
	file.inode = info.st_ino;
	if (contents) {
		auto fd = openat(parent.directory->fd, parent.name.c_str(), O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
		if (fd < 0) {
			throw IOException("Cannot open scheduled Vortex source without following links");
		}
		Descriptor handle(fd);
		struct stat opened;
		if (fstat(fd, &opened) || opened.st_dev != info.st_dev || opened.st_ino != info.st_ino ||
		    opened.st_size != info.st_size) {
			throw InvalidInputException("Vortex source changed during deletion verification");
		}
		string bytes(file.bytes, '\0');
		idx_t offset = 0;
		while (offset < file.bytes) {
			auto n = pread(fd, &bytes[offset], file.bytes - offset, offset);
			if (n <= 0) {
				throw IOException("Cannot read scheduled Vortex source");
			}
			offset += n;
		}
		auto digest = "sha256:" + Hex(duckdb_mbedtls::MbedTlsWrapper::ComputeSha256Hash(bytes));
		if (file.registered && file.digest != digest) {
			throw InvalidInputException("Registered Vortex source checksum changed");
		}
		file.digest = std::move(digest);
	}
	return true;
}

void Unreferenced(DuckLakeTransaction &transaction, const File &file) {
	auto scheduled = Count(transaction, "SELECT count(*) FROM {METADATA_CATALOG}." + string(SCHEDULED) + Where(file));
	if (scheduled > 1 || (!file.registered && scheduled != 1) ||
	    transaction.GetMetadataManager().HasFileReference(file.path, "/")) {
		throw InvalidInputException("Scheduled Vortex source still has native references or changed ownership");
	}
	if (VortexIndex::ReferencesSource(transaction, file.path)) {
		throw InvalidInputException("Scheduled Vortex source is still referenced by a named index");
	}
}
} // namespace

StorageLock &DuckLakeVortexSourceRetention::TransactionGate() {
	static StorageLock lock;
	return lock;
}

bool DuckLakeVortexSourceRetention::IsCollector(ClientContext &context) {
	return bool(context.registered_state->Get<CollectorContext>(COLLECTOR));
}

void DuckLakeVortexSourceRetention::Initialize(DuckLakeTransaction &transaction) {
	VortexIndex::Query(transaction, "CREATE TABLE {METADATA_CATALOG}." + string(POLICY) +
	                                    "(id BOOLEAN PRIMARY KEY CHECK(id),enabled BOOLEAN NOT NULL,"
	                                    "keep_snapshots BIGINT NOT NULL,min_age_seconds BIGINT NOT NULL,"
	                                    "expired_snapshots BIGINT NOT NULL,collected_files BIGINT NOT NULL,"
	                                    "collected_bytes BIGINT NOT NULL)");
	VortexIndex::Query(transaction,
	                   "INSERT INTO {METADATA_CATALOG}." + string(POLICY) + " VALUES(true,false,1,0,0,0,0)");
	VortexIndex::Query(transaction, "CREATE TABLE {METADATA_CATALOG}." + string(DELETIONS) +
	                                    "(data_file_id BIGINT,path VARCHAR,path_is_relative BOOLEAN,"
	                                    "file_bytes UBIGINT NOT NULL,device UBIGINT NOT NULL,inode UBIGINT NOT NULL,"
	                                    "digest VARCHAR NOT NULL,PRIMARY KEY(data_file_id,path,path_is_relative))");
}

void DuckLakeVortexSourceRetention::Configure(ClientContext &context, const string &catalog, bool enabled,
                                              idx_t keep_snapshots, idx_t min_age_seconds) {
	if (!keep_snapshots || keep_snapshots > 100000 || min_age_seconds > 315360000) {
		throw InvalidInputException("Invalid Vortex source retention policy");
	}
	Owner owner(context, catalog);
	Load(owner.transaction);
	OpenRoot(Root(owner.transaction), false);
	VortexIndex::Query(owner.transaction, "UPDATE {METADATA_CATALOG}." + string(POLICY) +
	                                          " SET enabled=" + string(enabled ? "true" : "false") +
	                                          ",keep_snapshots=" + to_string(keep_snapshots) +
	                                          ",min_age_seconds=" + to_string(min_age_seconds));
	owner.connection.Commit();
}

bool DuckLakeVortexSourceRetention::WorkReady(DuckLakeTransaction &transaction, idx_t max_bytes) {
	auto policy = Load(transaction);
	if (!policy.enabled || Manager(transaction.GetCatalog()).HasOtherTransactions(&transaction)) {
		return false;
	}
	if (Count(transaction, "SELECT count(*) FROM {METADATA_CATALOG}.ducklake_snapshot WHERE " + Eligible(policy))) {
		return true;
	}
	auto root = Root(transaction);
	auto files = Candidates(transaction, root, 1);
	if (files.empty()) {
		return false;
	}
	auto directory = OpenRoot(root, false);
	auto parent = OpenParent(directory->fd, files[0].relative, files[0].registered);
	Inspect(parent, files[0], false);
	return files[0].bytes <= max_bytes;
}

vector<vector<Value>> DuckLakeVortexSourceRetention::Status(ClientContext &context, const string &catalog) {
	Owner owner(context, catalog);
	auto policy = Load(owner.transaction);
	auto counts = VortexIndex::Query(
	    owner.transaction,
	    "SELECT expired_snapshots,collected_files,collected_bytes FROM {METADATA_CATALOG}." + string(POLICY));
	auto values = counts->Fetch();
	vector<Value> row {Value::BOOLEAN(policy.enabled), Value::UBIGINT(policy.keep), Value::UBIGINT(policy.age)};
	for (idx_t col = 0; col < 3; col++) {
		row.push_back(Value::UBIGINT(values->GetValue(col, 0).GetValue<idx_t>()));
	}
	for (auto &sql : {"SELECT count(*) FROM {METADATA_CATALOG}.ducklake_snapshot WHERE " + Eligible(policy),
	                  "SELECT count(*) FROM {METADATA_CATALOG}." + string(SCHEDULED),
	                  "SELECT count(*) FROM {METADATA_CATALOG}." + string(DELETIONS)}) {
		row.push_back(Value::UBIGINT(Count(owner.transaction, sql)));
	}
	row.push_back(Value::BOOLEAN(Manager(owner.transaction.GetCatalog()).HasOtherTransactions(&owner.transaction)));
	owner.connection.Commit();
	return {std::move(row)};
}

DuckLakeVortexSourceGCResult
DuckLakeVortexSourceRetention::Collect(ClientContext &context, const string &catalog_name, idx_t max_objects,
                                       idx_t max_bytes, const std::function<void(const string &)> &observe) {
	if (!max_objects || max_objects > 16 || !max_bytes || max_bytes > MAX_BYTES) {
		throw InvalidInputException("Invalid Vortex source collection budgets");
	}
	auto check = [&](const string &stage) {
		if (observe) {
			observe(stage);
		}
		if (context.IsInterrupted()) {
			throw InterruptException();
		}
	};
	auto attached = DatabaseManager::Get(*context.db).GetDatabase(catalog_name);
	if (!attached || attached->GetCatalog().GetCatalogType() != "ducklake") {
		throw InvalidInputException("Vortex source collection requires an attached DuckLake catalog");
	}
	auto &catalog = attached->GetCatalog().Cast<DuckLakeCatalog>();
	check("source_gc_waiting");
	auto publication = VortexIndex::PublicationLock().GetExclusiveLock();
	auto transactions = TransactionGate().GetExclusiveLock();
	DuckLakeVortexSourceGCResult result;
	if (Manager(catalog).HasOtherTransactions(nullptr)) {
		result.blocked = true;
		return result;
	}
	auto admissions = DuckLakeVortexReadView::AdmissionLock().GetExclusiveLock();
	Connection owner(*context.db);
	owner.context->registered_state->GetOrCreate<CollectorContext>(COLLECTOR);
	try {
		auto &transaction = VortexIndex::Begin(owner, catalog_name);
		if (&transaction.GetCatalog() != &catalog) {
			throw TransactionException("Vortex source catalog ownership changed");
		}
		auto policy = Load(transaction);
		if (!policy.enabled) {
			owner.Commit();
			return result;
		}
		auto root = Root(transaction);
		auto directory = OpenRoot(root, false);
		auto files = Candidates(transaction, root, max_objects);
		owner.Commit();
		for (auto &file : files) {
			check("source_before_plan");
			auto parent = OpenParent(directory->fd, file.relative, file.registered);
			Inspect(parent, file, false);
			if (file.bytes > max_bytes - result.bytes) {
				break;
			}
			{
				auto &mark = VortexIndex::Begin(owner, catalog_name);
				Unreferenced(mark, file);
				Inspect(parent, file, true);
				if (!file.registered) {
					VortexIndex::Query(mark, "INSERT INTO {METADATA_CATALOG}." + string(DELETIONS) + " VALUES(" +
					                             to_string(file.id) + "," + Value(file.raw).ToSQLString() + "," +
					                             string(file.is_relative ? "true" : "false") + "," +
					                             to_string(file.bytes) + "," + to_string(file.device) + "," +
					                             to_string(file.inode) + "," + Value(file.digest).ToSQLString() + ")");
				}
				owner.Commit();
				file.registered = true;
			}
			check("source_delete_intent_committed");
			check("source_before_unlink");
			if (Inspect(parent, file, true)) {
				if (unlinkat(parent.directory->fd, parent.name.c_str(), 0)) {
					throw IOException("Cannot unlink registered Vortex source");
				}
				SyncDirectory(parent.directory->fd);
			}
			check("source_after_unlink");
			{
				auto &finish = VortexIndex::Begin(owner, catalog_name);
				Unreferenced(finish, file);
				VortexIndex::Query(finish, "DELETE FROM {METADATA_CATALOG}." + string(SCHEDULED) + Where(file));
				VortexIndex::Query(finish, "DELETE FROM {METADATA_CATALOG}." + string(DELETIONS) + Where(file));
				VortexIndex::Query(finish,
				                   "UPDATE {METADATA_CATALOG}." + string(POLICY) +
				                       " SET collected_files=collected_files+1,collected_bytes=collected_bytes+" +
				                       to_string(file.bytes));
				owner.Commit();
			}
			result.files++;
			result.bytes += file.bytes;
			check("source_delete_completed");
		}
		if (result.files < max_objects) {
			auto &expire = VortexIndex::Begin(owner, catalog_name);
			auto snapshots = expire.GetMetadataManager().GetAllSnapshots(
			    "snapshot_id IN (SELECT snapshot_id FROM {METADATA_CATALOG}.ducklake_snapshot WHERE " +
			    Eligible(policy) + " ORDER BY snapshot_id LIMIT " + to_string(max_objects - result.files) + ")");
			if (!snapshots.empty()) {
				check("source_before_expire");
				expire.DeleteSnapshots(snapshots);
				VortexIndex::Query(expire, "UPDATE {METADATA_CATALOG}." + string(POLICY) +
				                               " SET expired_snapshots=expired_snapshots+" +
				                               to_string(snapshots.size()));
			}
			owner.Commit();
			result.expired += snapshots.size();
			if (!snapshots.empty()) {
				check("source_expiry_committed");
			}
		}
		return result;
	} catch (...) {
		VortexIndex::Rollback(owner);
		throw;
	}
}
} // namespace duckdb
