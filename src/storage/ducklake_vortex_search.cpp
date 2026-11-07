#include "storage/ducklake_vortex_search.hpp"
#include "storage/ducklake_vortex_query_ffi.hpp"
#include "duckdb/main/client_context.hpp"

#include <cmath>
#include <exception>

namespace duckdb {
namespace {
struct QueryControl {
	ClientContext &context;
	const std::function<void(const string &)> &observe;
	std::exception_ptr error;
};

uint8_t CheckQuery(void *pointer, const uint8_t *stage, uint64_t length) {
	auto &control = *static_cast<QueryControl *>(pointer);
	try {
		if (control.observe) {
			control.observe(string(reinterpret_cast<const char *>(stage), length));
		}
	} catch (...) {
		control.error = std::current_exception();
		return 1;
	}
	return control.context.IsInterrupted() ? 1 : 0;
}
} // namespace

DuckLakeVortexSearchResult DuckLakeVortexSearch::Execute(ClientContext &context,
                                                         const DuckLakeVortexViewOptions &options,
                                                         const DuckLakeVortexSearchRequest &request,
                                                         const std::function<void(const string &)> &observe) {
	if (options.index_name.empty() || request.query.empty() || request.query.size() > 4096 || request.k == 0 ||
	    request.k > 64 || request.category.size() > 64 * 1024) {
		throw InvalidInputException("Invalid Vortex SQL index name, query, k or category");
	}
	for (auto value : request.query) {
		if (!std::isfinite(value)) {
			throw InvalidInputException("Vortex SQL query requires finite FLOAT components");
		}
	}
	Connection reader(*context.db);
	auto view = DuckLakeVortexReadView::Acquire(reader, options, observe, &context);
	view.rows_json = string();
	QueryControl control {context, observe, nullptr};
	DuckLakeVortexQueryInput input {reinterpret_cast<const uint8_t *>(view.admission_json.data()),
	                                view.admission_json.size(),
	                                reinterpret_cast<const uint8_t *>(view.managed_data_path.data()),
	                                view.managed_data_path.size(),
	                                reinterpret_cast<const uint8_t *>(options.index_name.data()),
	                                options.index_name.size(),
	                                request.query.data(),
	                                request.query.size(),
	                                request.k,
	                                reinterpret_cast<const uint8_t *>(request.category.data()),
	                                request.category.size(),
	                                uint8_t(request.filter_category),
	                                CheckQuery,
	                                &control};
	std::unique_ptr<DuckLakeVortexQueryOutput, decltype(&ducklake_vortex_query_free)> result(
	    ducklake_vortex_query(&input), ducklake_vortex_query_free);
	if (!result) {
		throw InternalException("Native Vortex query returned no owned result");
	}
	if (control.error) {
		std::rethrow_exception(control.error);
	}
	if (context.IsInterrupted()) {
		throw InterruptException();
	}
	if (auto error = ducklake_vortex_query_error(result.get())) {
		throw InvalidInputException("%s", error);
	}
	DuckLakeVortexSearchResult output;
	output.diagnostics = ducklake_vortex_query_diagnostics(result.get());
	auto count = ducklake_vortex_query_count(result.get());
	if (count > request.k) {
		throw InternalException("Native Vortex query returned excess rows");
	}
	for (idx_t i = 0; i < count; i++) {
		DuckLakeVortexQueryRow row;
		if (ducklake_vortex_query_row(result.get(), i, &row) || row.dimension != request.query.size()) {
			throw InternalException("Native Vortex query returned an invalid typed row");
		}
		vector<Value> embedding;
		for (idx_t j = 0; j < row.dimension; j++) {
			embedding.push_back(Value::FLOAT(row.embedding[j]));
		}
		output.rows.push_back(
		    {row.id_null ? Value(LogicalType::BIGINT) : Value::BIGINT(row.id),
		     Value::LIST(LogicalType::FLOAT, std::move(embedding)),
		     row.category_null ? Value(LogicalType::VARCHAR)
		                       : Value(string(reinterpret_cast<const char *>(row.category), row.category_len)),
		     row.payload_null ? Value(LogicalType::VARCHAR)
		                      : Value(string(reinterpret_cast<const char *>(row.payload), row.payload_len)),
		     Value::FLOAT(row.distance)});
	}
	return output;
}
} // namespace duckdb
