#include "storage/ducklake_vortex_search.hpp"
#include "storage/ducklake_vortex_query_ffi.hpp"
#include "storage/ducklake_vortex_control.hpp"
#include "duckdb/main/client_context.hpp"
#include "common/ducklake_vortex_json.hpp"
#include "storage/ducklake_catalog.hpp"
#include "duckdb/transaction/transaction_context.hpp"

#include <cmath>
#include <exception>

namespace duckdb {

vector<Value> DuckLakeVortexSearch::CollectScratch(ClientContext &context, const string &catalog_name,
                                                   const string &root, idx_t max_objects, idx_t max_bytes,
                                                   const std::function<void(const string &)> &observe) {
	if (!context.transaction.IsAutoCommit()) {
		throw InvalidInputException("Vortex query scratch collection requires autocommit");
	}
	if (root.empty() || max_objects < 1 || max_objects > 16 || max_bytes < 1 || max_bytes > 64 * 1024 * 1024) {
		throw InvalidInputException("Invalid Vortex query scratch collection budgets or root");
	}
	auto &catalog = Catalog::GetCatalog(context, catalog_name).Cast<DuckLakeCatalog>();
	if (!catalog.MetadataType().empty() && catalog.MetadataType() != "duckdb") {
		throw InvalidInputException("Native query scratch ownership requires DuckDB metadata");
	}
	auto data_path = catalog.DataPath();
	vector<DuckLakeVortexSourceBuffer> sources;
	DuckLakeVortexControl control {context, observe, nullptr, sources};
	DuckLakeVortexScratchInput input {reinterpret_cast<const uint8_t *>(data_path.data()),
	                                  data_path.size(),
	                                  reinterpret_cast<const uint8_t *>(root.data()),
	                                  root.size(),
	                                  max_objects,
	                                  max_bytes,
	                                  DuckLakeVortexControl::Check,
	                                  &control};
	std::unique_ptr<DuckLakeVortexQueryOutput, decltype(&ducklake_vortex_query_free)> result(
	    ducklake_vortex_collect_query_scratch(&input), ducklake_vortex_query_free);
	if (!result) {
		throw InternalException("Native scratch collection returned no owned result");
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
	using namespace duckdb_yyjson;
	auto report = ducklake_vortex_query_diagnostics(result.get());
	std::unique_ptr<yyjson_doc, decltype(&yyjson_doc_free)> parsed(yyjson_read(report, strlen(report), 0),
	                                                               yyjson_doc_free);
	if (!parsed) {
		throw InternalException("Invalid native scratch collection report");
	}
	auto object = yyjson_doc_get_root(parsed.get());
	auto root_value = yyjson_obj_get(object, "root");
	if (!yyjson_is_str(root_value)) {
		throw InternalException("Missing native scratch collection root");
	}
	vector<Value> values {Value(string(yyjson_get_str(root_value), yyjson_get_len(root_value)))};
	for (auto name : {"registered_views", "examined_views", "blocked_views", "collected_objects", "collected_files",
	                  "collected_bytes", "removed_views", "remaining_views"}) {
		auto value = yyjson_obj_get(object, name);
		if (!yyjson_is_uint(value)) {
			throw InternalException("Invalid native scratch collection count: %s", name);
		}
		values.push_back(Value::UBIGINT(yyjson_get_uint(value)));
	}
	return values;
}

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
	std::function<void(const string &)> lease_observe = [&](const string &stage) {
		if (stage == "after_open") {
			view.generation_guard.reset();
		}
		if (observe) {
			observe(stage);
		}
	};
	DuckLakeVortexControl control {context, lease_observe, nullptr, view.sources};
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
	json.Number(capture, "snapshot_id", view.snapshot_id);
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
