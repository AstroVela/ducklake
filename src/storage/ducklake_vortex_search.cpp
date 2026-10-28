#include "storage/ducklake_vortex_search.hpp"
#include "storage/ducklake_vortex_query_ffi.hpp"
#include "storage/ducklake_vortex_control.hpp"
#include "duckdb/main/client_context.hpp"
#include "common/ducklake_vortex_json.hpp"

#include <cmath>
#include <exception>

namespace duckdb {

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
	auto view = DuckLakeVortexReadView::AcquireQuery(reader, options, observe, &context);
	vector<DuckLakeVortexQuerySource> sources;
	for (auto &source : view.sources) {
		sources.push_back(
		    {source.file_id, reinterpret_cast<const uint8_t *>(source.bytes.data()), source.bytes.size()});
	}
	DuckLakeVortexControl control {context, observe, nullptr, view.sources};
	DuckLakeVortexQueryInput input {reinterpret_cast<const uint8_t *>(view.admission_json.data()),
	                                view.admission_json.size(),
	                                sources.data(),
	                                sources.size(),
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
	                                DuckLakeVortexControl::Check,
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
	using namespace duckdb_yyjson;
	auto diagnostics = ducklake_vortex_query_diagnostics(result.get());
	std::unique_ptr<yyjson_doc, decltype(&yyjson_doc_free)> parsed(yyjson_read(diagnostics, strlen(diagnostics), 0),
	                                                               yyjson_doc_free);
	if (!parsed) {
		throw InternalException("Invalid native query diagnostics");
	}
	vortex_detail::ViewJSON json;
	auto details = yyjson_val_mut_copy(json.doc.get(), yyjson_doc_get_root(parsed.get()));
	auto capture = json.Object();
	json.Number(capture, "source_bytes", view.source_bytes);
	json.Number(capture, "visible_rows", view.visible_rows);
	json.Number(capture, "inline_rows", view.inline_rows);
	json.Number(capture, "business_rows", view.captured_business_rows);
	json.Number(capture, "oracle_bytes", view.rows_json.size());
	json.Number(capture, "admission_bytes", view.admission_json.size());
	json.Number(capture, "binary_source_count", sources.size());
	json.Number(capture, "binary_source_bytes", view.source_bytes);
	json.Number(capture, "retained_native_source_buffers", view.sources.size());
	json.Add(details, "native_capture", capture);
	output.diagnostics = json.Finish(details);
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
