#include "storage/ducklake_transaction_manager.hpp"

#include "duckdb/main/settings.hpp"
#ifdef DUCKLAKE_ENABLE_VORTEX
#include "storage/ducklake_vortex_source_retention.hpp"
#include "storage/ducklake_vortex_index_catalog.hpp"
#include "storage/ducklake_vortex_read_view.hpp"
#endif

namespace duckdb {

DuckLakeTransactionManager::DuckLakeTransactionManager(AttachedDatabase &db_p, DuckLakeCatalog &ducklake_catalog)
    : TransactionManager(db_p), ducklake_catalog(ducklake_catalog) {
}

Transaction &DuckLakeTransactionManager::StartTransaction(ClientContext &context) {
#ifdef DUCKLAKE_ENABLE_VORTEX
	unique_ptr<DuckLakeVortexCollectionLock::Key> gate;
	if (!DuckLakeVortexSourceRetention::IsCollector(context)) {
		gate = DuckLakeVortexSourceRetention::TransactionGate().GetSharedLock([&] {
			if (context.IsInterrupted()) {
				throw InterruptException();
			}
		});
	}
#endif
	auto transaction = make_shared_ptr<DuckLakeTransaction>(ducklake_catalog, *this, context);
#ifdef DUCKLAKE_ENABLE_VORTEX
	// Classify every transaction created by the dedicated capture before it is registered.
	transaction->vortex_read_capture = DuckLakeVortexReadView::IsCapturing(context);
#endif
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

bool DuckLakeTransactionManager::HasOtherTransactions(const DuckLakeTransaction *exclude, bool exclude_vortex_captures,
                                                      const std::function<void()> &on_end) {
	lock_guard<mutex> guard(transaction_lock);
#ifdef DUCKLAKE_ENABLE_VORTEX
	std::unordered_set<const DuckLakeTransaction *> blockers;
#else
	(void)exclude_vortex_captures;
	(void)on_end;
#endif
	for (auto &entry : transactions) {
		auto transaction = entry.second.get();
		if (transaction == exclude) {
			continue;
		}
#ifdef DUCKLAKE_ENABLE_VORTEX
		if (exclude_vortex_captures && transaction->IsVortexReadCapture()) {
			continue;
		}
		if (on_end) {
			blockers.insert(transaction);
			continue;
		}
#endif
		return true;
	}
#ifdef DUCKLAKE_ENABLE_VORTEX
	if (!blockers.empty()) {
		vortex_gc_wake = on_end;
		vortex_gc_blockers = std::move(blockers);
		return true;
	}
#endif
	return false;
}

void DuckLakeTransactionManager::RemoveTransaction(Transaction &transaction) {
	std::function<void()> wake;
	{
		lock_guard<mutex> guard(transaction_lock);
#ifdef DUCKLAKE_ENABLE_VORTEX
		if (vortex_gc_blockers.erase(&transaction.Cast<DuckLakeTransaction>())) {
			wake = std::move(vortex_gc_wake);
			vortex_gc_blockers.clear();
		}
#endif
		transactions.erase(transaction);
	}
	if (wake) {
		wake();
	}
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
	RemoveTransaction(transaction);
	return error;
}

void DuckLakeTransactionManager::RollbackTransaction(Transaction &transaction) {
	auto &ducklake_transaction = transaction.Cast<DuckLakeTransaction>();
	ducklake_transaction.Rollback();
	RemoveTransaction(transaction);
}

} // namespace duckdb
