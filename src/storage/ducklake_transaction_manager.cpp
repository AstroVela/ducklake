#include "storage/ducklake_transaction_manager.hpp"

#include "duckdb/main/settings.hpp"
#ifdef DUCKLAKE_ENABLE_VORTEX
#include "storage/ducklake_vortex_source_retention.hpp"
#include "duckdb/storage/storage_lock.hpp"
#endif

namespace duckdb {

DuckLakeTransactionManager::DuckLakeTransactionManager(AttachedDatabase &db_p, DuckLakeCatalog &ducklake_catalog)
    : TransactionManager(db_p), ducklake_catalog(ducklake_catalog) {
}

Transaction &DuckLakeTransactionManager::StartTransaction(ClientContext &context) {
#ifdef DUCKLAKE_ENABLE_VORTEX
	unique_ptr<StorageLockKey> gate;
	if (!DuckLakeVortexSourceRetention::IsCollector(context)) {
		gate = DuckLakeVortexSourceRetention::TransactionGate().GetSharedLock();
	}
#endif
	auto transaction = make_shared_ptr<DuckLakeTransaction>(ducklake_catalog, *this, context);
	transaction->Start();
	if (Settings::Get<ImmediateTransactionModeSetting>(context) && get_snapshot) {
		get_snapshot = false;
		// no snapshot loaded yet for this transaction - load it
		transaction->GetSnapshot();
		get_snapshot = true;
	}
	auto &result = *transaction;
	lock_guard<mutex> l(transaction_lock);
	transactions[result] = std::move(transaction);
	return result;
}

bool DuckLakeTransactionManager::HasOtherTransactions(const DuckLakeTransaction *exclude) {
	lock_guard<mutex> guard(transaction_lock);
	for (auto &entry : transactions) {
		if (entry.second.get() != exclude) {
			return true;
		}
	}
	return false;
}

ErrorData DuckLakeTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	auto &ducklake_transaction = transaction.Cast<DuckLakeTransaction>();
	ErrorData error;
	try {
		ducklake_transaction.Commit();
	} catch (std::exception &ex) {
		error = ErrorData(ex);
		try {
			ducklake_transaction.Rollback();
		} catch (std::exception &rollback_error) {
			error.Merge(ErrorData(rollback_error));
		}
	}
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
	return error;
}

void DuckLakeTransactionManager::RollbackTransaction(Transaction &transaction) {
	auto &ducklake_transaction = transaction.Cast<DuckLakeTransaction>();
	ducklake_transaction.Rollback();
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
}

} // namespace duckdb
