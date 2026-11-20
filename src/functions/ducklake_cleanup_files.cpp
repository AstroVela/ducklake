#include "functions/ducklake_table_functions.hpp"

#include "duckdb/common/operator/subtract.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_transaction.hpp"
#ifdef DUCKLAKE_ENABLE_VORTEX
#include "storage/ducklake_vortex_read_view.hpp"
#include "storage/ducklake_vortex_source_retention.hpp"
#endif

namespace duckdb {

struct CleanupBindData : public TableFunctionData {
	explicit CleanupBindData(Catalog &catalog, CleanupType type) : catalog(catalog), type(type) {
	}

	string GetFilter() const {
		if (timestamp_filter.empty()) {
			return "";
		}
		switch (type) {
		case CleanupType::OLD_FILES:
			return StringUtil::Format("WHERE schedule_start < '%s'", timestamp_filter);
		case CleanupType::ORPHANED_FILES:
			return StringUtil::Format(" AND last_modified < '%s'", timestamp_filter);
		default:
			throw InternalException("Unknown Cleanup type for GetFilter()");
		}
	}

	string GetFunctionName() const {
		switch (type) {
		case CleanupType::OLD_FILES:
			return "ducklake_cleanup_old_files";
		case CleanupType::ORPHANED_FILES:
			return "ducklake_delete_orphaned_files";
		default:
			throw InternalException("Unknown Cleanup type for GetFunctionName()");
		}
	}

	Catalog &catalog;
	vector<DuckLakeFileForCleanup> files;
#ifdef DUCKLAKE_ENABLE_VORTEX
	unique_ptr<DuckLakeVortexCatalogIdentity> source_identity;
#endif
	//! If we are going to delete the files for real or not
	bool dry_run = false;

	CleanupType type;
	string timestamp_filter;
};

static unique_ptr<FunctionData> CleanupBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names,
                                            CleanupType type) {
	auto &catalog = DuckLakeBaseMetadataFunction::GetCatalog(context, input.inputs[0]);
	auto result = make_uniq<CleanupBindData>(catalog, type);

	auto &ducklake_catalog = reinterpret_cast<DuckLakeCatalog &>(catalog);
	const auto older_than_default = ducklake_catalog.GetConfigOption<string>("delete_older_than", {}, {}, "2 days");

	timestamp_tz_t from_timestamp;
	bool has_timestamp = false;
	bool cleanup_all = false;
	for (auto &entry : input.named_parameters) {
		if (StringUtil::CIEquals(entry.first, "dry_run")) {
			result->dry_run = entry.second.GetValue<bool>();
			;
		} else if (StringUtil::CIEquals(entry.first, "cleanup_all")) {
			cleanup_all = entry.second.GetValue<bool>();
		} else if (StringUtil::CIEquals(entry.first, "older_than")) {
			from_timestamp = entry.second.GetValue<timestamp_tz_t>();
			has_timestamp = true;
		} else {
			throw InternalException("Unsupported named parameter for %s", result->GetFunctionName());
		}
	}
	if ((cleanup_all == has_timestamp && cleanup_all == true) ||
	    (cleanup_all == has_timestamp && cleanup_all == false && older_than_default.empty())) {
		throw InvalidInputException(
		    "%s: either cleanup_all OR older_than must be specified.\nYou can also set a default value for file "
		    "deletion via e.g., CALL ducklake.set_option('delete_older_than', '1 week');",
		    result->GetFunctionName());
	}

	if (has_timestamp) {
		result->timestamp_filter = DuckLakeTableFunctionUtil::FormatTimestampISO8601(timestamp_t(from_timestamp.value));
	} else if (!cleanup_all && !older_than_default.empty()) {
		interval_t interval;
		if (!Interval::FromString(older_than_default, interval)) {
			throw InvalidInputException("Failed to parse interval: '%s'", older_than_default);
		}
		auto current_time = Timestamp::GetCurrentTimestamp();
		auto target_timestamp =
		    SubtractOperator::Operation<timestamp_t, interval_t, timestamp_t>(current_time, interval);
		result->timestamp_filter = DuckLakeTableFunctionUtil::FormatTimestampISO8601(target_timestamp);
	}

	auto &transaction = DuckLakeTransaction::Get(context, catalog);
	auto &metadata_manager = transaction.GetMetadataManager();
	result->files = metadata_manager.GetFilesForCleanup(result->GetFilter(), type, ducklake_catalog.Separator());
#ifdef DUCKLAKE_ENABLE_VORTEX
	if (!result->files.empty() && DuckLakeVortexSourceRetention::IsInitialized(transaction)) {
		result->source_identity = make_uniq<DuckLakeVortexCatalogIdentity>(
		    DuckLakeVortexIndex::ResolveCatalog(context, catalog.GetName(), false, "Vortex source cleanup"));
	}
#endif

	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("path");

	return std::move(result);
}
static unique_ptr<FunctionData> DuckLakeCleanupOldFilesBind(ClientContext &context, TableFunctionBindInput &input,
                                                            vector<LogicalType> &return_types, vector<string> &names) {
	return CleanupBind(context, input, return_types, names, CleanupType::OLD_FILES);
}

static unique_ptr<FunctionData> DuckLakeCleanupOrphanedFilesBind(ClientContext &context, TableFunctionBindInput &input,
                                                                 vector<LogicalType> &return_types,
                                                                 vector<string> &names) {
	return CleanupBind(context, input, return_types, names, CleanupType::ORPHANED_FILES);
}

struct DuckLakeCleanupData : public GlobalTableFunctionState {
	DuckLakeCleanupData() : offset(0), executed(false) {
	}

	idx_t offset;
	bool executed;
	vector<DuckLakeFileForCleanup> files;
};

unique_ptr<GlobalTableFunctionState> DuckLakeCleanupInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<DuckLakeCleanupData>();
	return std::move(result);
}

void DuckLakeCleanupExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->Cast<CleanupBindData>();
	auto &state = data_p.global_state->Cast<DuckLakeCleanupData>();
	if (!state.executed && !data.files.empty()) {
		state.files = data.files;
#ifdef DUCKLAKE_ENABLE_VORTEX
		auto interrupt = [&] {
			if (context.IsInterrupted()) {
				throw InterruptException();
			}
		};
		unique_ptr<DuckLakeVortexCollectionLock::Key> publication_guard, admission_guard;
		unique_ptr<Connection> fresh;
		if (data.source_identity) {
			publication_guard = DuckLakeVortexIndex::PublicationLock().GetExclusiveLock(interrupt);
		}
		if (!data.dry_run || data.source_identity) {
			admission_guard = DuckLakeVortexReadView::AdmissionLock().GetExclusiveLock(interrupt);
		}
		if (data.source_identity) {
			fresh = make_uniq<Connection>(*context.db);
			auto &transaction = DuckLakeVortexIndex::Begin(*fresh, data.catalog.GetName(), data.source_identity.get());
			DuckLakeVortexSourceRetention::ProtectCleanupFiles(transaction, data.type, state.files);
			interrupt();
		}
#endif
		if (!data.dry_run && !state.files.empty()) {
			auto &fs = FileSystem::GetFileSystem(context);
			vector<string> paths;
			paths.reserve(state.files.size());
			for (const auto &file : state.files) {
				paths.push_back(file.path);
			}
			fs.RemoveFiles(paths);
			if (data.type == CleanupType::OLD_FILES) {
				auto &transaction = DuckLakeTransaction::Get(context, data.catalog);
				transaction.GetMetadataManager().RemoveFilesScheduledForCleanup(state.files);
			}
		}
#ifdef DUCKLAKE_ENABLE_VORTEX
		if (fresh) {
			fresh->Commit();
		}
#endif
		state.executed = true;
	}
	idx_t count = 0;
	while (state.offset < state.files.size() && count < STANDARD_VECTOR_SIZE) {
		auto &file = state.files[state.offset++];
		output.SetValue(0, count, file.path);
		count++;
	}
	output.SetCardinality(count);
}

DuckLakeCleanupOldFilesFunction::DuckLakeCleanupOldFilesFunction()
    : TableFunction("ducklake_cleanup_old_files", {LogicalType::VARCHAR}, DuckLakeCleanupExecute,
                    DuckLakeCleanupOldFilesBind, DuckLakeCleanupInit) {
	named_parameters["older_than"] = LogicalType::TIMESTAMP_TZ;
	named_parameters["cleanup_all"] = LogicalType::BOOLEAN;
	named_parameters["dry_run"] = LogicalType::BOOLEAN;
}

DuckLakeCleanupOrphanedFilesFunction::DuckLakeCleanupOrphanedFilesFunction()
    : TableFunction("ducklake_delete_orphaned_files", {LogicalType::VARCHAR}, DuckLakeCleanupExecute,
                    DuckLakeCleanupOrphanedFilesBind, DuckLakeCleanupInit) {
	named_parameters["older_than"] = LogicalType::TIMESTAMP_TZ;
	named_parameters["cleanup_all"] = LogicalType::BOOLEAN;
	named_parameters["dry_run"] = LogicalType::BOOLEAN;
}

} // namespace duckdb
