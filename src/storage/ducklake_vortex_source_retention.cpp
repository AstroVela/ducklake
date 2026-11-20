#include "storage/ducklake_vortex_source_retention.hpp"

#include "common/ducklake_vortex_gc_io.hpp"
#include "common/ducklake_vortex_json.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/opener_file_system.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/settings.hpp"
#include "mbedtls_wrapper.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_metadata_manager.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_transaction_manager.hpp"
#include "storage/ducklake_vortex_index_catalog.hpp"
#include "storage/ducklake_vortex_read_view.hpp"

#include <algorithm>
#include <cstdlib>

namespace duckdb {
namespace vortex_detail {
string AbsoluteLocalPath(FileSystem &fs, const string &path) {
	if (path.empty() || path.find('\0') != string::npos) {
		throw InvalidInputException("Invalid Vortex source path");
	}
	auto local = fs.ExpandPath(path);
	if (local.find("://") != string::npos || fs.IsRemoteFile(local)) {
		throw InvalidInputException("Vortex source collection requires local paths");
	}
	if (!fs.IsPathAbsolute(local)) {
		local = fs.JoinPath(fs.GetWorkingDirectory(), local);
	}
	return local;
}

bool MatchesSourcePath(ClientContext &context, const string &reference, const string &path) {
	auto &fs = FileSystem::GetFileSystem(context);
	if (fs.IsRemoteFile(reference) && !StringUtil::StartsWith(reference, "file:/")) {
		return false;
	}
	auto local = AbsoluteLocalPath(fs, reference);
	if (local == path) {
		return true;
	}
	fs.Cast<OpenerFileSystem>().VerifyCanAccessFile(local);
	std::unique_ptr<char, decltype(&free)> resolved(realpath(local.c_str(), nullptr), free);
	if (!resolved) {
		if (errno == ENOENT || errno == ENOTDIR) {
			return false;
		}
		throw IOException("Cannot resolve native Vortex source reference: %s", local);
	}
	return resolved.get() == path;
}
} // namespace vortex_detail

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
	Owner(ClientContext &context, const string &catalog, const DuckLakeVortexCatalogIdentity &identity)
	    : connection(*context.db), transaction(VortexIndex::Begin(connection, catalog, &identity)) {
	}
};

struct FileKey {
	idx_t id;
	string path;
	bool is_relative;
};

struct File {
	idx_t bytes = 0, device = 0, inode = 0;
	string path, relative, digest;
	vector<FileKey> keys;
	idx_t scheduled = 0;
	bool registered = false;
};

string Where(const File &file) {
	string filter;
	for (auto &key : file.keys) {
		if (!filter.empty()) {
			filter += " OR ";
		}
		filter += "(data_file_id=" + to_string(key.id) + " AND path COLLATE C=" + Value(key.path).ToSQLString() +
		          " AND path_is_relative=" + string(key.is_relative ? "true)" : "false)");
	}
	return " WHERE (" + filter + ")";
}

string NormalizeManagedPath(const string &absolute) {
	string path = "/";
	for (auto &part : StringUtil::Split(absolute, '/')) {
		if (part.empty() || part == ".") {
			continue;
		}
		CheckRelative(part, true, "Vortex source");
		if (path.size() > 1) {
			path += "/";
		}
		path += part;
	}
	return path;
}

string Root(DuckLakeTransaction &transaction) {
	auto &fs = FileSystem::GetFileSystem(*transaction.GetConnection().context);
	return NormalizeManagedPath(AbsoluteLocalPath(fs, transaction.GetCatalog().DataPath()));
}

unique_ptr<Descriptor> OpenManagedRoot(ClientContext &context, const string &root) {
	CheckDirectoryAccess(context, root);
	return OpenRoot(root, false, "Vortex source");
}

vector<File> Candidates(DuckLakeTransaction &transaction, const string &root) {
	auto result = VortexIndex::Query(
	    transaction, "SELECT coalesce(s.data_file_id,d.data_file_id),coalesce(s.path,d.path),"
	                 "coalesce(s.path_is_relative,d.path_is_relative),d.file_bytes,d.device,d.inode,d.digest,"
	                 "s.data_file_id IS NOT NULL FROM {METADATA_CATALOG}." +
	                     string(SCHEDULED) + " s FULL OUTER JOIN {METADATA_CATALOG}." + DELETIONS +
	                     " d ON s.data_file_id=d.data_file_id AND s.path COLLATE C=d.path COLLATE C "
	                     "AND s.path_is_relative=d.path_is_relative ORDER BY d.digest IS NULL,"
	                     "coalesce(s.data_file_id,d.data_file_id),coalesce(s.path,d.path),"
	                     "coalesce(s.path_is_relative,d.path_is_relative)");
	auto &fs = FileSystem::GetFileSystem(*transaction.GetConnection().context);
	auto prefix = root == "/" ? root : root + "/";
	unordered_map<string, idx_t> positions;
	vector<File> files;
	while (auto chunk = result->Fetch()) {
		for (idx_t row = 0; row < chunk->size(); row++) {
			FileKey key {chunk->GetValue(0, row).GetValue<idx_t>(), StringValue::Get(chunk->GetValue(1, row)),
			             chunk->GetValue(2, row).GetValue<bool>()};
			if (key.is_relative && (key.path.empty() || key.path[0] == '/')) {
				throw InvalidInputException("Invalid relative Vortex source path");
			}
			auto path = NormalizeManagedPath(key.is_relative ? prefix + key.path : AbsoluteLocalPath(fs, key.path));
			if (path == root || !StringUtil::StartsWith(path, prefix)) {
				throw InvalidInputException("Vortex source cleanup path is outside managed data");
			}
			auto position = positions.emplace(path, files.size());
			if (position.second) {
				File file;
				file.path = path;
				file.relative = path.substr(prefix.size());
				CheckRelative(file.relative, false, "Vortex source");
				files.push_back(std::move(file));
			}
			auto &file = files[position.first->second];
			file.scheduled += chunk->GetValue(7, row).GetValue<bool>();
			file.keys.push_back(std::move(key));
			if (!chunk->GetValue(6, row).IsNull()) {
				const auto bytes = chunk->GetValue(3, row).GetValue<idx_t>();
				const auto device = chunk->GetValue(4, row).GetValue<idx_t>();
				const auto inode = chunk->GetValue(5, row).GetValue<idx_t>();
				auto digest = StringValue::Get(chunk->GetValue(6, row));
				if (file.registered &&
				    (bytes != file.bytes || device != file.device || inode != file.inode || digest != file.digest)) {
					throw InvalidInputException("Conflicting deletion intents for the same native Vortex source");
				}
				file.registered = true;
				file.bytes = bytes;
				file.device = device;
				file.inode = inode;
				file.digest = std::move(digest);
			}
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
	if (!S_ISREG(info.st_mode) || info.st_nlink != 1 || info.st_size < 0) {
		throw InvalidInputException("Vortex source cleanup requires a regular file without links");
	}
	if ((file.registered || contents) &&
	    (file.bytes != idx_t(info.st_size) || file.device != idx_t(info.st_dev) || file.inode != idx_t(info.st_ino))) {
		throw InvalidInputException("Registered Vortex source identity changed");
	}
	file.bytes = info.st_size;
	file.device = info.st_dev;
	file.inode = info.st_ino;
	if (contents) {
		auto fd = openat(parent.directory->fd, parent.name.c_str(), O_RDONLY | O_NOFOLLOW | O_CLOEXEC | O_NONBLOCK);
		if (fd < 0) {
			throw IOException("Cannot open scheduled Vortex source without following links");
		}
		Descriptor handle(fd);
		struct stat opened;
		if (fstat(fd, &opened) || !S_ISREG(opened.st_mode) || opened.st_nlink != 1 || opened.st_dev != info.st_dev ||
		    opened.st_ino != info.st_ino || opened.st_size != info.st_size) {
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

bool Referenced(DuckLakeTransaction &transaction, const File &file) {
	return transaction.GetMetadataManager().HasFileReference(file.path, "/", false) ||
	       VortexIndex::ReferencesSource(transaction, file.path);
}

void CheckCleanupOwnership(DuckLakeTransaction &transaction, const File &file) {
	auto scheduled = Count(transaction, "SELECT count(*) FROM {METADATA_CATALOG}." + string(SCHEDULED) + Where(file));
	if (scheduled != file.scheduled) {
		throw InvalidInputException("Scheduled Vortex source changed ownership");
	}
}
} // namespace

DuckLakeVortexCollectionLock &DuckLakeVortexSourceRetention::TransactionGate() {
	static DuckLakeVortexCollectionLock lock;
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

bool DuckLakeVortexSourceRetention::IsInitialized(DuckLakeTransaction &transaction) {
	auto &catalog = transaction.GetCatalog();
	return (catalog.MetadataType().empty() || catalog.MetadataType() == "duckdb") &&
	       transaction.GetConnection().TableInfo(catalog.MetadataDatabaseName(), catalog.MetadataSchemaName(), POLICY);
}

void DuckLakeVortexSourceRetention::ProtectCleanupFiles(DuckLakeTransaction &transaction, CleanupType type,
                                                        vector<DuckLakeFileForCleanup> &files) {
	auto &context = *transaction.GetConnection().context;
	auto &fs = FileSystem::GetFileSystem(context);
	const auto include_scheduled = type == CleanupType::ORPHANED_FILES;
	auto &metadata = transaction.GetMetadataManager();
	unordered_set<string> pending_paths;
	if (!include_scheduled) {
		unordered_set<idx_t> selected;
		for (auto &file : files) {
			selected.insert(file.id.index);
		}
		// An age-filtered native cleanup must leave shared paths whose other keys are still pending.
		for (auto &entry : metadata.GetFilesForCleanup("", CleanupType::OLD_FILES, "/")) {
			if (!selected.count(entry.id.index) && !fs.IsRemoteFile(entry.path)) {
				pending_paths.insert(NormalizeManagedPath(AbsoluteLocalPath(fs, entry.path)));
			}
		}
	}
	files.erase(std::remove_if(files.begin(), files.end(),
	                           [&](const DuckLakeFileForCleanup &file) {
		                           if (fs.IsRemoteFile(file.path)) {
			                           return false;
		                           }
		                           auto path = NormalizeManagedPath(AbsoluteLocalPath(fs, file.path));
		                           return pending_paths.count(path) ||
		                                  metadata.HasFileReference(path, "/", include_scheduled) ||
		                                  VortexIndex::ReferencesSource(transaction, path);
	                           }),
	            files.end());
}

void DuckLakeVortexSourceRetention::Configure(ClientContext &context, const string &catalog, bool enabled,
                                              idx_t keep_snapshots, idx_t min_age_seconds) {
	if (!keep_snapshots || keep_snapshots > 100000 || min_age_seconds > 315360000) {
		throw InvalidInputException("Invalid Vortex source retention policy");
	}
	auto identity = VortexIndex::ResolveCatalog(context, catalog, true, "Vortex source retention");
	Owner owner(context, catalog, identity);
	Load(owner.transaction);
	if (enabled) {
		OpenManagedRoot(context, Root(owner.transaction));
	}
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
	auto files = Candidates(transaction, root);
	if (files.empty()) {
		return false;
	}
	auto &context = *transaction.GetConnection().context;
	auto directory = OpenManagedRoot(context, root);
	for (auto &file : files) {
		if (Referenced(transaction, file)) {
			continue;
		}
		auto parent = OpenParent(directory->fd, file.relative, file.registered, "Vortex source");
		Inspect(parent, file, false);
		if (file.bytes <= max_bytes) {
			return true;
		}
	}
	return false;
}

vector<vector<Value>> DuckLakeVortexSourceRetention::Status(ClientContext &context, const string &catalog) {
	auto identity = VortexIndex::ResolveCatalog(context, catalog, false, "Vortex source retention status");
	Owner owner(context, catalog, identity);
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
                                       idx_t max_bytes, const std::function<void(const string &)> &observe,
                                       optional_ptr<const DuckLakeVortexCatalogIdentity> identity) {
	if (!max_objects || max_objects > 16 || !max_bytes || max_bytes > MAX_BYTES) {
		throw InvalidInputException("Invalid Vortex source collection budgets");
	}
	auto interrupt = [&] {
		if (context.IsInterrupted()) {
			throw InterruptException();
		}
	};
	auto check = [&](const string &stage) {
		if (observe) {
			observe(stage);
		}
		interrupt();
	};
	auto expected =
	    identity ? *identity : VortexIndex::ResolveCatalog(context, catalog_name, true, "Vortex source collection");
	auto &manager = DatabaseManager::Get(*context.db);
	auto attached = manager.GetDatabase(catalog_name);
	if (!attached || attached->oid != expected.lake_oid) {
		throw TransactionException("Vortex source catalog attachment changed");
	}
	auto &catalog = attached->GetCatalog().Cast<DuckLakeCatalog>();
	auto check_attachments = [&] {
		auto lake = manager.GetDatabase(catalog_name);
		auto metadata = manager.GetDatabase(catalog.MetadataDatabaseName());
		if (lake != attached || !metadata || metadata->oid != expected.metadata_oid) {
			throw TransactionException("Vortex source catalog attachment changed");
		}
	};
	DuckLakeVortexSourceGCResult result;
	check("source_gc_waiting");
	auto publication = VortexIndex::PublicationLock().GetExclusiveLock(interrupt);
	auto admissions = DuckLakeVortexReadView::AdmissionLock().TryGetExclusiveLock();
	auto transactions = admissions ? TransactionGate().TryGetExclusiveLock() : nullptr;
	if (!transactions || Manager(catalog).HasOtherTransactions(nullptr)) {
		result.blocked = true;
		return result;
	}
	Connection owner(*context.db);
	owner.context->registered_state->GetOrCreate<CollectorContext>(COLLECTOR);
	ClientConfig::GetConfig(*owner.context)
	    .user_settings.SetUserSetting(ImmediateTransactionModeSetting::SettingIndex, Value::BOOLEAN(false));
	try {
		auto &transaction = VortexIndex::Begin(owner, catalog_name, &expected);
		auto policy = Load(transaction);
		if (!policy.enabled) {
			owner.Commit();
			return result;
		}
		auto root = Root(transaction);
		auto directory = OpenManagedRoot(context, root);
		auto files = Candidates(transaction, root);
		owner.Commit();
		for (auto &file : files) {
			if (result.files == max_objects) {
				break;
			}
			check("source_before_plan");
			auto &mark = VortexIndex::Begin(owner, catalog_name, &expected);
			if (Referenced(mark, file)) {
				owner.Commit();
				continue;
			}
			auto parent = OpenParent(directory->fd, file.relative, file.registered, "Vortex source");
			Inspect(parent, file, false);
			if (file.bytes > max_bytes - result.bytes) {
				owner.Commit();
				continue;
			}
			CheckCleanupOwnership(mark, file);
			Inspect(parent, file, true);
			if (!file.registered) {
				auto &key = file.keys.front();
				VortexIndex::Query(mark, "INSERT INTO {METADATA_CATALOG}." + string(DELETIONS) + " VALUES(" +
				                             to_string(key.id) + "," + Value(key.path).ToSQLString() + "," +
				                             string(key.is_relative ? "true" : "false") + "," + to_string(file.bytes) +
				                             "," + to_string(file.device) + "," + to_string(file.inode) + "," +
				                             Value(file.digest).ToSQLString() + ")");
			}
			interrupt();
			owner.Commit();
			file.registered = true;
			check("source_delete_intent_committed");
			check("source_before_unlink");
			// Keep the original attachment and metadata transaction pinned through the filesystem change.
			auto &finish = VortexIndex::Begin(owner, catalog_name, &expected);
			CheckCleanupOwnership(finish, file);
			if (Referenced(finish, file) ||
			    !Count(finish, "SELECT count(*) FROM {METADATA_CATALOG}." + string(DELETIONS) + Where(file) +
			                       " AND device=" + to_string(file.device) + " AND inode=" + to_string(file.inode) +
			                       " AND file_bytes=" + to_string(file.bytes) +
			                       " AND digest=" + Value(file.digest).ToSQLString())) {
				throw InvalidInputException("Registered Vortex source changed ownership");
			}
			auto unlink_root = OpenManagedRoot(context, root);
			parent = OpenParent(unlink_root->fd, file.relative, true, "Vortex source");
			const auto present = Inspect(parent, file, true);
			interrupt();
			check_attachments();
			if (present && unlinkat(parent.directory->fd, parent.name.c_str(), 0)) {
				throw IOException("Cannot unlink registered Vortex source");
			}
			check("source_unlinked_before_sync");
			SyncDirectory((parent.directory ? parent.directory : parent.missing_parent)->fd, "Vortex source directory");
			check("source_after_unlink");
			VortexIndex::Query(finish, "DELETE FROM {METADATA_CATALOG}." + string(SCHEDULED) + Where(file));
			VortexIndex::Query(finish, "DELETE FROM {METADATA_CATALOG}." + string(DELETIONS) + Where(file));
			VortexIndex::Query(finish, "UPDATE {METADATA_CATALOG}." + string(POLICY) +
			                               " SET collected_files=collected_files+1,collected_bytes=collected_bytes+" +
			                               to_string(file.bytes));
			interrupt();
			check_attachments();
			owner.Commit();
			result.files++;
			result.bytes += file.bytes;
			check("source_delete_completed");
		}
		if (result.files < max_objects) {
			auto &expire = VortexIndex::Begin(owner, catalog_name, &expected);
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
			interrupt();
			check_attachments();
			owner.Commit();
			for (auto &snapshot : snapshots) {
				catalog.InvalidateSchemaCache(snapshot.schema_version);
			}
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
