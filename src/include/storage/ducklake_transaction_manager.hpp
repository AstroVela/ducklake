//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storageducklake_transaction_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/transaction_manager.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_catalog.hpp"

#include <functional>
#ifdef DUCKLAKE_ENABLE_VORTEX
#include <unordered_set>
#endif

namespace duckdb {

class DuckLakeTransactionManager : public TransactionManager {
public:
	DuckLakeTransactionManager(AttachedDatabase &db_p, DuckLakeCatalog &ducklake_catalog);

	Transaction &StartTransaction(ClientContext &context) override;
	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
	void RollbackTransaction(Transaction &transaction) override;

	void Checkpoint(ClientContext &context, bool force = false) override;
	bool HasOtherTransactions(const DuckLakeTransaction *exclude, bool exclude_vortex_captures,
	                          const std::function<void()> &on_end = nullptr);

private:
	std::function<void()> RemoveTransaction(Transaction &transaction);
	DuckLakeCatalog &ducklake_catalog;
	mutex transaction_lock;
	reference_map_t<Transaction, shared_ptr<DuckLakeTransaction>> transactions;
	bool get_snapshot = true;
#ifdef DUCKLAKE_ENABLE_VORTEX
	std::unordered_set<const DuckLakeTransaction *> vortex_gc_blockers;
	std::function<void()> vortex_gc_wake;
#endif
};

} // namespace duckdb
