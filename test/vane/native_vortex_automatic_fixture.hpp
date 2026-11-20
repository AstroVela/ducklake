#pragma once

#include "native_vortex_fixture.hpp"
#include "common/ducklake_vortex_json.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_vortex_index_catalog.hpp"
#include "storage/ducklake_vortex_optimizer.hpp"
#include "storage/ducklake_vortex_search.hpp"
#include "storage/ducklake_vortex_source_retention.hpp"
#include "storage/ducklake_vortex_write.hpp"
#include <atomic>
#include <cmath>
#include <iostream>
#include <limits>
#include <map>
#include <thread>

namespace duckdb {
namespace vortex_test {
constexpr float AUTOMATIC_QUERY_X = -1.0f;

struct AutomaticRow {
	int64_t id;
	float x, y;
	string category, payload;
	Value ToValue() const {
		return Value::STRUCT(ListType::GetChildType(DuckLakeVortexWriteBatch::RowsType()),
		                     {Value::BIGINT(id), Value::LIST(LogicalType::FLOAT, {Value::FLOAT(x), Value::FLOAT(y)}),
		                      Value(category), Value(payload)});
	}
};

struct AutomaticPlan {
	string request;
	vector<AutomaticRow> rows;
	vector<int64_t> deletes;
	DuckLakeVortexWriteResult receipt;
};

struct AutomaticObservation {
	idx_t snapshot;
	bool filtered;
	DuckLakeVortexSearchResult result;
	idx_t indexed_files, scan_files, inline_rows, scanned_rows, ann_calls;
};

struct AutomaticEvents {
	std::mutex lock;
	std::map<string, idx_t> stages;
};

inline duckdb_yyjson::yyjson_mut_val *AutomaticReceipts(vortex_detail::ViewJSON &json,
                                                        const vector<DuckLakeVortexWriteResult> &receipts) {
	auto list = json.Array();
	for (auto &receipt : receipts) {
		auto record = json.Object();
		json.Text(record, "table_uuid", receipt.table_uuid);
		json.Text(record, "request_id", receipt.request_id);
		json.Text(record, "payload_hash", receipt.payload_hash);
		json.Number(record, "row_count", receipt.row_count);
		json.Number(record, "snapshot_id", receipt.snapshot_id);
		duckdb_yyjson::yyjson_mut_arr_append(list, record);
	}
	return list;
}

inline void SaveAutomaticCheckpoint(DuckDB &database, const string &root) {
	Connection reader(database);
	reader.BeginTransaction();
	auto recorded = SQL(reader, "SELECT table_uuid,request_id,payload_hash,row_count,snapshot_id FROM "
	                            "__ducklake_metadata_lake.ducklake_vortex_write_requests_v1 ORDER BY snapshot_id");
	vector<DuckLakeVortexWriteResult> receipts;
	for (auto &row : *recorded) {
		receipts.push_back({row.GetValue<string>(0), row.GetValue<string>(1), row.GetValue<string>(2),
		                    row.GetValue<idx_t>(3), row.GetValue<idx_t>(4)});
	}
	auto index = SQL(reader, "SELECT revision,reference FROM __ducklake_metadata_lake.ducklake_vortex_indexes_v1");
	Require(index->RowCount() == 1, "Checkpoint requires one named index");
	auto deletions = SQL(reader, "SELECT data_file_id,path,path_is_relative,file_bytes,device,inode,digest FROM "
	                             "__ducklake_metadata_lake.ducklake_vortex_source_deletions_v1");
	reader.Commit();
	vortex_detail::ViewJSON json;
	auto body = json.Object();
	json.Add(body, "receipts", AutomaticReceipts(json, receipts));
	json.Number(body, "revision", index->GetValue(0, 0).GetValue<idx_t>());
	json.Text(body, "reference", index->GetValue(1, 0).GetValue<string>());
	auto pending = json.Array();
	for (auto &row : *deletions) {
		auto intent = json.Object();
		json.Number(intent, "data_file_id", row.GetValue<idx_t>(0));
		json.Text(intent, "path", row.GetValue<string>(1));
		json.Add(intent, "path_is_relative", duckdb_yyjson::yyjson_mut_bool(json.doc.get(), row.GetValue<bool>(2)));
		json.Number(intent, "file_bytes", row.GetValue<idx_t>(3));
		json.Number(intent, "device", row.GetValue<idx_t>(4));
		json.Number(intent, "inode", row.GetValue<idx_t>(5));
		json.Text(intent, "digest", row.GetValue<string>(6));
		duckdb_yyjson::yyjson_mut_arr_append(pending, intent);
	}
	json.Add(body, "source_intents", pending);
	Write(root + "/checkpoint-state.json", json.Finish(body));
}

struct AutomaticFixture {
	using Model = std::map<int64_t, AutomaticRow>;
	DuckDB &database;
	Connection &setup;
	string root;
	Model model;
	std::map<idx_t, Model> models;
	vector<AutomaticPlan> plans;
	vector<DuckLakeVortexWriteResult> acknowledged;
	std::shared_ptr<AutomaticEvents> events = std::make_shared<AutomaticEvents>();
	bool configured = false;

	AutomaticFixture(DuckDB &db, Connection &connection, const string &path)
	    : database(db), setup(connection), root(path) {
		models.emplace(0, Model {});
		SaveAcknowledged();
	}
	~AutomaticFixture() {
		try {
			Stop();
		} catch (...) {
		}
	}
	void Stop() {
		if (!configured) {
			return;
		}
		DuckLakeVortexOptimizer::Cancel(*setup.context, "lake", "main", "items", "knn");
		DuckLakeVortexOptimizer::CancelMaintenance(*setup.context, "lake");
		auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
		while (DuckLakeVortexOptimizer::Status(*setup.context, "lake")[0][16].GetValue<bool>() ||
		       DuckLakeVortexOptimizer::MaintenanceStatus(*setup.context, "lake")[0][2].GetValue<bool>()) {
			Require(std::chrono::steady_clock::now() < deadline, "Automatic worker did not stop at teardown");
			std::this_thread::sleep_for(std::chrono::milliseconds(5));
		}
		configured = false;
	}
	void Configure(const std::function<void(const string &)> &observe, idx_t inline_limit = 256) {
		SQL(setup, "SELECT * FROM ducklake_initialize_vortex_optimizer('lake')");
		Connection owner(database);
		DuckLakeVortexWriteBatch::Manage(owner, *setup.context, "lake", "main", "items", "knn");
		DuckLakeVortexOptimizerConfig c;
		c.catalog = "lake";
		c.schema = "main";
		c.table = "items";
		c.index = "knn";
		c.root = root + "/indexes";
		c.dimension = 2;
		c.flush_rows = 128;
		c.flush_bytes = 4 * 1024 * 1024;
		c.index_rows = 128;
		c.index_bytes = 64 * 1024 * 1024;
		DuckLakeVortexOptimizer::Enable(*setup.context, c, observe);
		DuckLakeVortexWriteBudget budget {inline_limit, 4 * 1024 * 1024, 1024, 64 * 1024 * 1024};
		DuckLakeVortexOptimizer::SetWriteBudget(*setup.context, "lake", "main", "items", "knn", budget);
		DuckLakeVortexOptimizer::ConfigureCompaction(*setup.context, "lake", "main", "items", "knn", true, 32, 25);
		DuckLakeVortexOptimizer::ConfigureSourceRetention(*setup.context, "lake", true, 2, 0);
		DuckLakeVortexOptimizer::EnableMaintenance(*setup.context, "lake", 2, 64 * 1024 * 1024, observe);
		configured = true;
	}
	AutomaticRow Row(int64_t id, idx_t epoch) {
		return {id, float(id), float(epoch) / 16, (idx_t(id) + epoch) % 4 ? "A" : "B",
		        "epoch-" + to_string(epoch) + ":" + to_string(id) + string("\0", 1) + "向量"};
	}
	Value Rows(const vector<AutomaticRow> &rows) {
		vector<Value> values;
		for (auto &row : rows) {
			values.push_back(row.ToValue());
		}
		return Value::LIST(ListType::GetChildType(DuckLakeVortexWriteBatch::RowsType()), std::move(values));
	}
	Value Deletes(const vector<int64_t> &ids) {
		vector<Value> values;
		for (auto id : ids) {
			values.push_back(Value::BIGINT(id));
		}
		return Value::LIST(LogicalType::BIGINT, std::move(values));
	}
	void SavePlans() {
		using namespace vortex_detail;
		ViewJSON json;
		auto list = json.Array();
		for (auto &plan : plans) {
			auto entry = json.Object(), rows = json.Array(), deletes = json.Array();
			json.Text(entry, "request_id", plan.request);
			for (auto &row : plan.rows) {
				auto object = json.Object(), embedding = json.Array();
				json.Number(object, "id", row.id);
				duckdb_yyjson::yyjson_mut_arr_add_real(json.doc.get(), embedding, row.x);
				duckdb_yyjson::yyjson_mut_arr_add_real(json.doc.get(), embedding, row.y);
				json.Add(object, "embedding", embedding);
				json.Text(object, "category", row.category);
				json.Text(object, "payload", row.payload);
				duckdb_yyjson::yyjson_mut_arr_append(rows, object);
			}
			for (auto id : plan.deletes) {
				duckdb_yyjson::yyjson_mut_arr_add_sint(json.doc.get(), deletes, id);
			}
			json.Add(entry, "rows", rows);
			json.Add(entry, "deletes", deletes);
			duckdb_yyjson::yyjson_mut_arr_append(list, entry);
		}
		Write(root + "/plans.json", json.Finish(list));
	}
	void SaveAcknowledged() {
		vortex_detail::ViewJSON json;
		Write(root + "/acknowledged.json", json.Finish(AutomaticReceipts(json, acknowledged)));
	}
	DuckLakeVortexWriteResult Submit(const string &request, vector<AutomaticRow> rows, vector<int64_t> deletes = {},
	                                 const std::function<void(const string &)> &observe = {}) {
		AutomaticPlan plan;
		plan.request = request;
		plan.rows = std::move(rows);
		plan.deletes = std::move(deletes);
		plans.push_back(std::move(plan));
		SavePlans();
		auto &pending = plans.back();
		auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
		while (true) {
			Connection owner(database);
			try {
				pending.receipt =
				    DuckLakeVortexWriteBatch::Mutate(owner, *setup.context, "lake", "main", "items", "knn", request,
				                                     Rows(pending.rows), Deletes(pending.deletes), observe);
				break;
			} catch (std::exception &error) {
				auto message = StringUtil::Lower(error.what());
				Require(message.find("conflict") != string::npos || message.find("resource_exhausted") != string::npos,
				        error.what());
				Healthy();
				Require(std::chrono::steady_clock::now() < deadline, "Explicit client retry never regained admission");
				std::this_thread::sleep_for(std::chrono::milliseconds(20));
			}
		}
		acknowledged.push_back(pending.receipt);
		SaveAcknowledged();
		for (auto id : pending.deletes) {
			model.erase(id);
		}
		for (auto &row : pending.rows) {
			model[row.id] = row;
		}
		models.emplace(pending.receipt.snapshot_id, model);
		return pending.receipt;
	}
	DuckLakeVortexIndex Index() {
		Connection owner(database);
		auto &transaction = DuckLakeVortexIndex::Begin(owner, "lake");
		auto entry = transaction.GetCatalog().GetEntry(*owner.context, CatalogType::TABLE_ENTRY, "main", "items",
		                                               OnEntryNotFound::THROW_EXCEPTION);
		auto result = DuckLakeVortexIndex::Lookup(transaction, entry->Cast<DuckLakeTableEntry>(), "knn");
		owner.Commit();
		return result;
	}
	void Healthy() {
		auto p = DuckLakeVortexOptimizer::Status(*setup.context, "lake")[0];
		auto m = DuckLakeVortexOptimizer::MaintenanceStatus(*setup.context, "lake")[0];
		Require(p[5].GetValue<bool>() && p[4].ToString() != "failed",
		        "Automatic optimizer stopped: " + p[14].ToString());
		Require(m[1].GetValue<bool>() && m[0].ToString() != "failed",
		        "Automatic maintenance stopped: " + m[13].ToString());
	}
	void Wait(idx_t revision) {
		auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
		while (true) {
			Healthy();
			auto p = DuckLakeVortexOptimizer::Status(*setup.context, "lake")[0];
			auto m = DuckLakeVortexOptimizer::MaintenanceStatus(*setup.context, "lake")[0];
			auto gc = DuckLakeVortexSourceRetention::Status(*setup.context, "lake")[0];
			auto index = Index();
			set<idx_t> covered, active;
			for (auto &file : index.files) {
				covered.insert(file.id);
			}
			auto files =
			    SQL(setup,
			        "SELECT data_file_id FROM __ducklake_metadata_lake.ducklake_data_file WHERE end_snapshot IS NULL");
			for (auto &row : *files) {
				active.insert(row.GetValue<idx_t>(0));
			}
			auto inlined =
			    SQL(setup, "SELECT count(*) FROM lake.items WHERE starts_with(filename,'ducklake_inlined_data_')")
			        ->GetValue(0, 0)
			        .GetValue<idx_t>();
			if (index.revision >= revision && !p[16].GetValue<bool>() && !m[2].GetValue<bool>() &&
			    p[10].GetValue<idx_t>() == 0 && p[12].GetValue<idx_t>() == 0 && p[29].ToString() != "eligible" &&
			    !inlined && covered == active && m[11].GetValue<idx_t>() == 0 && m[12].GetValue<idx_t>() == 0 &&
			    gc[6].GetValue<idx_t>() == 0 && gc[7].GetValue<idx_t>() == 0 && gc[8].GetValue<idx_t>() == 0) {
				return;
			}
			Require(std::chrono::steady_clock::now() < deadline, "Automatic pipeline did not drain");
			std::this_thread::sleep_for(std::chrono::milliseconds(20));
		}
	}
	DuckLakeVortexSearchResult Read(Connection &reader, bool filtered) {
		DuckLakeVortexViewOptions options {};
		options.catalog = "lake";
		options.schema = "main";
		options.table = "items";
		options.index_name = "knn";
		options.max_source_bytes = 64 * 1024 * 1024;
		options.max_rows = 100000;
		DuckLakeVortexSearchRequest request;
		request.query = {AUTOMATIC_QUERY_X, 0};
		request.k = 10;
		request.filter_category = filtered;
		request.category = "B";
		return DuckLakeVortexSearch::Execute(*reader.context, options, request);
	}
	AutomaticObservation Observe(Connection &reader, bool filtered) {
		AutomaticObservation observation;
		observation.filtered = filtered;
		observation.result = Read(reader, filtered);
		using namespace duckdb_yyjson;
		std::unique_ptr<yyjson_doc, decltype(&yyjson_doc_free)> json(
		    yyjson_read(observation.result.diagnostics.data(), observation.result.diagnostics.size(), 0),
		    yyjson_doc_free);
		Require(bool(json), "Missing automatic query diagnostics");
		auto snapshot =
		    yyjson_obj_get(yyjson_obj_get(yyjson_doc_get_root(json.get()), "native_capture"), "snapshot_id");
		Require(yyjson_is_uint(snapshot), "Missing exact automatic query snapshot");
		observation.snapshot = yyjson_get_uint(snapshot);
		auto routes = yyjson_obj_get(yyjson_doc_get_root(json.get()), "routes");
		auto files = [&](const char *key) {
			auto value = yyjson_obj_get(routes, key);
			Require(yyjson_is_arr(value), "Missing automatic file route");
			return yyjson_arr_size(value);
		};
		auto count = [&](const char *key) {
			auto value = yyjson_obj_get(routes, key);
			Require(yyjson_is_uint(value), "Missing automatic query route counter");
			return yyjson_get_uint(value);
		};
		observation.indexed_files = files("indexed_files");
		observation.scan_files = files("scan_files");
		observation.inline_rows = count("inline_rows");
		observation.scanned_rows = count("scanned_rows");
		observation.ann_calls = count("ann_calls");
		return observation;
	}
	idx_t Grow(idx_t wave, const std::function<void(const string &)> &observe = {}) {
		vector<AutomaticRow> rows;
		for (idx_t id = wave * 128; id < (wave + 1) * 128; id++) {
			rows.push_back(Row(id, 0));
		}
		return Submit("seed-" + to_string(wave), std::move(rows), {}, observe).snapshot_id;
	}
	idx_t Wave(idx_t wave) {
		auto index = Index();
		auto largest =
		    std::max_element(index.segments.begin(), index.segments.end(),
		                     [](const DuckLakeVortexIndexSegment &left, const DuckLakeVortexIndexSegment &right) {
			                     idx_t a = 0, b = 0;
			                     for (auto &file : left.files) {
				                     a += file.row_count;
			                     }
			                     for (auto &file : right.files) {
				                     b += file.row_count;
			                     }
			                     return a < b;
		                     });
		set<string> paths;
		for (auto &file : largest->files) {
			paths.insert(file.uri);
		}
		auto current = SQL(setup, "SELECT id,filename FROM lake.items ORDER BY id");
		set<int64_t> selected;
		for (auto &row : *current) {
			if (paths.count(row.GetValue<string>(1)) && selected.size() < 96) {
				selected.insert(row.GetValue<int64_t>(0));
			}
		}
		Require(selected.size() == 96, "Automatic wave has insufficient live B rows");
		vector<AutomaticRow> rows;
		vector<int64_t> deletes;
		for (auto id : selected) {
			rows.push_back(Row(id, wave + 1));
		}
		for (auto &entry : model) {
			if (!selected.count(entry.first) && deletes.size() < 32) {
				deletes.push_back(entry.first);
			}
		}
		for (idx_t id = 512 + wave * 32; id < 544 + wave * 32; id++) {
			rows.push_back(Row(id, wave + 1));
		}
		auto receipt = Submit("wave-" + to_string(wave), std::move(rows), std::move(deletes));
		Require(model.size() == 512, "Automatic wave changed the live key count");
		return receipt.snapshot_id;
	}
	void CheckCurrentModel() {
		auto rows = SQL(setup, "SELECT {'id':id,'embedding':embedding,'category':category,'payload':payload} "
		                       "FROM lake.items ORDER BY id");
		Require(rows->RowCount() == model.size(), "Automatic replay changed the live key count");
		idx_t offset = 0;
		for (auto &entry : model) {
			Require(rows->GetValue(0, offset++) == entry.second.ToValue(), "Automatic replay changed a live row");
		}
	}
	void Replay() {
		const string snapshot_sql = "SELECT max(snapshot_id) FROM lake.snapshots()";
		auto before = SQL(setup, snapshot_sql)->GetValue(0, 0);
		CheckCurrentModel();
		for (auto &plan : plans) {
			Connection owner(database);
			auto result = DuckLakeVortexWriteBatch::Mutate(owner, *setup.context, "lake", "main", "items", "knn",
			                                               plan.request, Rows(plan.rows), Deletes(plan.deletes));
			Require(result.snapshot_id == plan.receipt.snapshot_id &&
			            result.payload_hash == plan.receipt.payload_hash &&
			            result.table_uuid == plan.receipt.table_uuid && result.request_id == plan.receipt.request_id &&
			            result.row_count == plan.receipt.row_count,
			        "Old automatic-flow request changed its receipt");
			Require(SQL(setup, snapshot_sql)->GetValue(0, 0) == before,
			        "Automatic receipt replay changed the snapshot");
			CheckCurrentModel();
		}
	}
	static float Distance(const AutomaticRow &row) {
		// Keep the Float32 arithmetic boundaries even with contraction enabled.
		const volatile float dx = row.x - AUTOMATIC_QUERY_X;
		const volatile float xx = dx * dx;
		const volatile float yy = row.y * row.y;
		return xx + yy;
	}
	float Validate(const vector<AutomaticObservation> &observations) {
		float minimum = 1;
		const auto embedding_type = LogicalType::LIST(LogicalType::FLOAT);
		for (auto &query : observations) {
			auto state = models.upper_bound(query.snapshot);
			Require(state != models.begin(), "Missing exact snapshot oracle");
			const auto &expected = (--state)->second;
			vector<std::pair<float, int64_t>> ranked;
			for (auto &entry : expected) {
				auto &row = entry.second;
				if (!query.filtered || row.category == "B") {
					ranked.emplace_back(Distance(row), row.id);
				}
			}
			std::sort(ranked.begin(), ranked.end());
			for (idx_t i = 1; i < ranked.size(); i++) {
				Require(ranked[i - 1].first < ranked[i].first, "Concurrent oracle inputs must have unique scores");
			}
			auto count = MinValue<idx_t>(10, ranked.size());
			set<int64_t> wanted, found;
			for (idx_t i = 0; i < count; i++) {
				wanted.insert(ranked[i].second);
			}
			Require(query.result.rows.size() <= count && (query.ann_calls || query.result.rows.size() == count),
			        "Automatic query returned an invalid number of hits");
			double previous = -1;
			for (auto &values : query.result.rows) {
				Require(values.size() == 5 && values[0].type() == LogicalType::BIGINT &&
				            values[1].type() == embedding_type && values[2].type() == LogicalType::VARCHAR &&
				            values[3].type() == LogicalType::VARCHAR && values[4].type() == LogicalType::FLOAT,
				        "Automatic query changed its typed result shape");
				for (auto &value : values) {
					Require(!value.IsNull(), "Automatic query returned NULL for a non-NULL fixture field");
				}
				auto id = values[0].GetValue<int64_t>();
				auto row = expected.find(id);
				Require(row != expected.end() && found.insert(id).second,
				        "Automatic query returned an invisible or duplicate key");
				auto &value = row->second;
				auto &embedding = ListValue::GetChildren(values[1]);
				Require(embedding.size() == 2 && !embedding[0].IsNull() && !embedding[1].IsNull() &&
				            embedding[0].GetValue<float>() == value.x && embedding[1].GetValue<float>() == value.y &&
				            StringValue::Get(values[2]) == value.category &&
				            StringValue::Get(values[3]) == value.payload && (!query.filtered || value.category == "B"),
				        "Automatic query mixed row versions or filters");
				auto distance = static_cast<double>(values[4].GetValue<float>());
				auto exact = static_cast<double>(Distance(value));
				auto tolerance = MaxValue(1e-4, 1e-5 * MaxValue(std::fabs(distance), std::fabs(exact)));
				Require(std::isfinite(distance) && distance > previous && std::fabs(distance - exact) <= tolerance,
				        "Automatic query changed distance or order");
				previous = distance;
			}
			idx_t hits = 0;
			for (auto id : found) {
				hits += wanted.count(id);
			}
			auto recall = count ? float(hits) / count : 1.0f;
			const auto recall_gate = query.ann_calls ? 0.8f : 1.0f;
			Require(recall >= recall_gate, "Automatic query recall fell below its route's fixture gate");
			minimum = MinValue(minimum, recall);
		}
		return minimum;
	}
	void CheckExactOracle(const AutomaticObservation &query) {
		auto rejected = [&](const AutomaticObservation &bad, const string &reason) {
			try {
				Validate({bad});
			} catch (const InvalidInputException &error) {
				Require(StringUtil::Contains(error.what(), reason), error.what());
				return;
			}
			throw InvalidInputException("Automatic oracle accepted an invalid exact result");
		};
		Require(query.result.rows.size() == 10, "Exact oracle fixture requires ten results");
		auto wrong_key = query;
		auto &outside = model.at(10);
		auto replacement = StructValue::GetChildren(outside.ToValue());
		replacement.push_back(Value::FLOAT(Distance(outside)));
		wrong_key.result.rows.back() = std::move(replacement);
		rejected(wrong_key, "route's fixture gate");
		auto infinite = query;
		infinite.result.rows.back()[4] = Value::FLOAT(std::numeric_limits<float>::infinity());
		rejected(infinite, "changed distance or order");
		auto truncated = query;
		truncated.result.rows.back()[1] = Value::LIST(LogicalType::FLOAT, {Value::FLOAT(9)});
		rejected(truncated, "mixed row versions or filters");
		auto fractional_key = query;
		fractional_key.result.rows.back()[0] = Value::DOUBLE(9.25);
		rejected(fractional_key, "changed its typed result shape");
	}
};

inline void RunAutomaticFixture(DuckDB &database, Connection &setup, const string &root, const string &mode) {
	auto before_flush = std::make_shared<Pause>();
	auto before_build = std::make_shared<Pause>();
	auto physical_lock = std::make_shared<Pause>();
	auto pause_physical = std::make_shared<std::atomic<bool>>(false);
	AutomaticFixture f(database, setup, root);
	ReleasePause release_flush {*before_flush}, release_build {*before_build}, release_physical {*physical_lock};
	auto wait_for_kill = [&database, root](const string &stage) {
		SaveAutomaticCheckpoint(database, root);
		Write(root + "/checkpoint", stage);
		auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(45);
		while (std::chrono::steady_clock::now() < deadline) {
			std::this_thread::sleep_for(std::chrono::milliseconds(5));
		}
		throw IOException("Automatic checkpoint was not killed");
	};
	auto events = f.events;
	auto observe = [events, before_flush, before_build, physical_lock, pause_physical, mode,
	                wait_for_kill](const string &stage) {
		{
			std::lock_guard<std::mutex> guard(events->lock);
			events->stages[stage]++;
		}
		if (mode == "automatic-live" && stage == "optimizer_before_flush") {
			before_flush->Stop();
		}
		if (stage == "optimizer_before_writer_lock") {
			if (mode == "automatic-backpressure") {
				before_flush->Stop();
			}
			if (*pause_physical) {
				physical_lock->Stop();
			}
		}
		if (mode == "automatic-live" && stage == "optimizer_before_build") {
			before_build->Stop();
		}
		if (mode == "automatic-flush-serialization" && stage == "optimizer_flush_started") {
			before_flush->Stop();
		}
		if (mode == "automatic-flush-serialization" && stage == "build_started") {
			before_build->Stop();
		}
		if ((mode == "automatic-reject-rebound-build" && stage == "before_build") ||
		    (mode == "automatic-reject-rebound-publication" && stage == "before_build_publish") ||
		    (mode == "automatic-reject-nonlake-capture" && stage == "before_build_capture")) {
			before_build->Stop();
		}
		if (mode == "automatic-rewrite-serialization" && stage == "rewrite_candidate_ready") {
			before_flush->Stop();
		}
		if (mode == "automatic-pause-" + stage) {
			wait_for_kill(stage);
		}
	};
	f.Configure(observe, mode == "automatic-backpressure" ? 128 : 256);
	const bool rebound_waiter = mode == "automatic-reject-rebound-catalog";
	const bool nonlake_capture = mode == "automatic-reject-nonlake-capture";
	if (rebound_waiter || mode == "automatic-reject-rebound-build" || mode == "automatic-reject-rebound-publication" ||
	    nonlake_capture) {
		*pause_physical = rebound_waiter;
		f.Grow(0);
		auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
		unique_ptr<DuckLakeVortexCollectionLock::Key> writer;
		if (rebound_waiter) {
			physical_lock->Wait();
			auto &lock = DuckLakeVortexWriteBatch::WriterLock();
			writer = lock.GetExclusiveLock([] {});
			physical_lock->Release();
			while (!lock.WaitingWriterCount()) {
				Require(std::chrono::steady_clock::now() < deadline, "Optimizer did not wait before catalog rebound");
				std::this_thread::sleep_for(std::chrono::milliseconds(1));
			}
		} else {
			before_build->Wait();
		}
		const string builds =
		    "SELECT build_id,state,keep_generation FROM __ducklake_metadata_lake.ducklake_vortex_builds_v1 ORDER BY 1";
		auto before = SQL(setup, builds)->ToString();
		SQL(setup, "DETACH lake");
		if (nonlake_capture) {
			SQL(setup, "ATTACH ':memory:' AS lake");
			before_build->Release();
			while (true) {
				{
					std::lock_guard<std::mutex> guard(events->lock);
					if (events->stages["optimizer_before_error"]) {
						break;
					}
				}
				Require(std::chrono::steady_clock::now() < deadline,
				        "Build capture accepted a non-DuckLake attachment");
				std::this_thread::sleep_for(std::chrono::milliseconds(1));
			}
			SQL(setup, "DETACH lake");
		}
		Attach(setup, root);
		before_build->Release();
		writer.reset();
		while (true) {
			auto p = DuckLakeVortexOptimizer::Status(*setup.context, "lake")[0];
			if (StringUtil::Contains(p[14].ToString(), "catalog attachment changed")) {
				Require(!p[16].GetValue<bool>() && p[15].GetValue<idx_t>() == (rebound_waiter ? 0 : 1) &&
				            f.Index().revision == 1 && SQL(setup, builds)->ToString() == before,
				        "Stale optimizer claimed, reconciled or published a build in the rebound catalog");
				break;
			}
			Require(std::chrono::steady_clock::now() < deadline, "Stale optimizer did not reject the rebound catalog");
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
		}
		f.Stop();
		f.Replay();
		std::cout << mode << ": optimizer rejected a new OID for the same bound metadata" << std::endl;
		return;
	}
	if (mode == "automatic-replan-after-writer" || mode == "automatic-cancel-writer-wait") {
		const bool cancel = mode == "automatic-cancel-writer-wait";
		f.Grow(0);
		f.Wait(2);
		Require(f.Index().segments.size() == 1, "Replan fixture needs one sealed source");
		*pause_physical = true;
		vector<int64_t> first, remaining;
		for (int64_t id = 0; id < 128; id++) {
			(id < 64 ? first : remaining).push_back(id);
		}
		f.Submit("partial-delete", {}, first);
		physical_lock->Wait();
		Require(f.model.size() == 64, "The first probe must see a files-only rewrite candidate");
		Pause before_commit;
		auto &lock = DuckLakeVortexWriteBatch::WriterLock();
		auto queued = lock.WaitingWriterCount();
		auto writer = std::async(std::launch::async, [&] {
			return f.Submit("last-delete", {}, remaining, [&](const string &stage) {
				if (stage == "before_commit") {
					before_commit.Stop();
				}
			});
		});
		ReleasePause unblock_writer {before_commit}, unblock_optimizer {*physical_lock};
		before_commit.Wait();
		physical_lock->Release();
		auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
		while (lock.WaitingWriterCount() <= queued) {
			Require(std::chrono::steady_clock::now() < deadline, "Optimizer never queued behind managed write");
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
		}
		if (cancel) {
			DuckLakeVortexOptimizer::Cancel(*setup.context, "lake", "main", "items", "knn");
			while (lock.WaitingWriterCount() > queued) {
				Require(std::chrono::steady_clock::now() < deadline, "Cancelled optimizer kept waiting for writer");
				std::this_thread::sleep_for(std::chrono::milliseconds(1));
			}
		}
		before_commit.Release();
		writer.get();
		if (cancel) {
			auto p = DuckLakeVortexOptimizer::Status(*setup.context, "lake")[0];
			Require(!p[5].GetValue<bool>() && p[4].ToString() == "cancelled" && f.Index().revision == 2,
			        "Cancelled optimizer published after its reservation wait");
		} else {
			f.Wait(3);
			auto index = f.Index();
			Require(index.revision == 3 && index.segments.empty(), "Reprobe did not retire the empty source");
			std::lock_guard<std::mutex> guard(events->lock);
			Require(events->stages["retire_candidate_ready"] == 1 && !events->stages["rewrite_candidate_ready"],
			        "Optimizer ran the stale files-only rewrite");
		}
		f.Stop();
		f.Replay();
		std::cout << mode << ": managed deletion crossed the rewrite-to-retire boundary" << std::endl;
		return;
	}
	if (mode == "automatic-flush-serialization" || mode == "automatic-rewrite-serialization") {
		bool flush_case = mode == "automatic-flush-serialization";
		idx_t expected_revision = 3;
		if (flush_case) {
			f.Grow(0);
		} else {
			for (idx_t wave = 0; wave < 4; wave++) {
				auto before = f.Index().revision;
				f.Grow(wave);
				f.Wait(before + 1);
			}
			expected_revision = f.Index().revision + 1;
			f.Wave(0);
		}
		before_flush->Wait();
		auto &lock = DuckLakeVortexWriteBatch::WriterLock();
		auto queued_before = lock.WaitingWriterCount();
		std::promise<void> acquired;
		auto acquired_event = acquired.get_future();
		vector<AutomaticRow> rows;
		if (flush_case) {
			for (idx_t id = 128; id < 256; id++) {
				rows.push_back(f.Row(id, 0));
			}
		} else {
			auto selected = DuckLakeVortexOptimizer::Status(*setup.context, "lake")[0][26].ToString();
			set<string> paths;
			for (auto &segment : f.Index().segments) {
				if (segment.generation == selected) {
					for (auto &file : segment.files) {
						paths.insert(file.uri);
					}
				}
			}
			auto live = SQL(setup, "SELECT id,filename FROM lake.items ORDER BY id");
			for (auto &entry : *live) {
				if (paths.count(entry.GetValue<string>(1)) && rows.size() < 128) {
					rows.push_back(f.Row(entry.GetValue<int64_t>(0), 99));
				}
			}
			Require(rows.size() == 128, "Serialization fixture lost its live rewrite input");
		}
		auto writer = std::async(std::launch::async, [&] {
			bool notified = false;
			return f.Submit("serialized", rows, {}, [&](const string &stage) {
				if (stage == "acquired" && !notified) {
					notified = true;
					acquired.set_value();
				}
			});
		});
		ReleasePause unblock_writer {*before_flush}, unblock_builder {*before_build};
		auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
		while (lock.WaitingWriterCount() <= queued_before &&
		       acquired_event.wait_for(std::chrono::seconds(0)) != std::future_status::ready) {
			Require(std::chrono::steady_clock::now() < deadline, "Managed writer never reached the reservation");
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
		}
		Require(lock.WaitingWriterCount() > queued_before &&
		            acquired_event.wait_for(std::chrono::seconds(0)) != std::future_status::ready,
		        "Managed writer entered a physical-maintenance snapshot");
		Connection reader(database);
		auto before = f.Observe(reader, false);
		before_flush->Release();
		if (flush_case) {
			before_build->Wait();
		}
		Require(writer.wait_for(std::chrono::seconds(10)) == std::future_status::ready,
		        "Index construction retained the flush writer reservation");
		writer.get();
		if (flush_case) {
			auto during_build =
			    std::async(std::launch::async, [&] { return f.Submit("during-build", {f.Row(64, 99)}); });
			ReleasePause unblock_build {*before_build};
			Require(during_build.wait_for(std::chrono::seconds(10)) == std::future_status::ready,
			        "A managed writer starting after native build capture could not commit");
			during_build.get();
		}
		f.Validate({before, f.Observe(reader, false), f.Observe(reader, true)});
		before_build->Release();
		f.Wait(expected_revision);
		f.Stop();
		f.Replay();
		std::cout << mode << ": physical changes serialize managed commits; captured queries remain valid" << std::endl;
		return;
	}
	if (mode == "automatic-backpressure") {
		f.Grow(0);
		before_flush->Wait();
		Connection owner(database);
		bool rejected = false;
		try {
			DuckLakeVortexWriteBatch::Mutate(owner, *setup.context, "lake", "main", "items", "knn", "blocked",
			                                 f.Rows({f.Row(10000, 1)}), f.Deletes({}));
		} catch (std::exception &error) {
			rejected = string(error.what()).find("RESOURCE_EXHAUSTED") != string::npos;
		}
		Require(rejected &&
		            SQL(setup, "SELECT count(*) FROM __ducklake_metadata_lake.ducklake_vortex_write_requests_v1 WHERE "
		                       "request_id='blocked'")
		                    ->GetValue(0, 0)
		                    .GetValue<idx_t>() == 0,
		        "Blocked automatic write left a receipt or was admitted");
		Require(SQL(setup, "SELECT count(*) FROM lake.items")->GetValue(0, 0).GetValue<idx_t>() == 128,
		        "Blocked automatic write changed committed rows");
		Connection reader(database);
		auto blocked_view = f.Observe(reader, false);
		Require(blocked_view.inline_rows == 128 && !blocked_view.indexed_files && !blocked_view.scan_files &&
		            !blocked_view.ann_calls,
		        "Backpressure query did not use T");
		f.Validate({blocked_view, f.Observe(reader, true)});
		before_flush->Release();
		f.Wait(2);
		f.Submit("blocked", {f.Row(10000, 1)});
		f.Stop();
		f.Replay();
		std::cout << "automatic backpressure rejected atomically; queries continued; worker released admission"
		          << std::endl;
		return;
	}
	if (StringUtil::StartsWith(mode, "automatic-pause-")) {
		f.Grow(0, [&](const string &stage) {
			if (mode == "automatic-pause-ack" && stage == "after_commit") {
				wait_for_kill("ack");
			}
		});
		f.Wait(2);
		for (idx_t wave = 1; wave < 4; wave++) {
			auto before = f.Index().revision;
			f.Grow(wave);
			f.Wait(before + 1);
		}
		for (idx_t wave = 0; wave < 8; wave++) {
			auto before = f.Index().revision;
			f.Wave(wave);
			f.Wait(before + 1);
		}
		throw InvalidInputException("Automatic kill checkpoint did not fire");
	}
	Require(mode == "automatic-live", "Unknown automatic fixture mode");
	std::atomic<bool> running {true};
	vector<AutomaticObservation> observations;
	std::mutex observation_lock;
	std::condition_variable sampled;
	idx_t last_sample[2] = {0, 0};
	std::exception_ptr reader_error;
	Connection reader(database);
	auto reads = std::async(std::launch::async, [&] {
		bool filtered = false;
		try {
			while (running) {
				auto observation = f.Observe(reader, filtered);
				{
					std::lock_guard<std::mutex> guard(observation_lock);
					last_sample[filtered] = observation.snapshot;
					observations.push_back(std::move(observation));
				}
				sampled.notify_one();
				filtered = !filtered;
				std::this_thread::sleep_for(std::chrono::milliseconds(25));
			}
		} catch (...) {
			{
				std::lock_guard<std::mutex> guard(observation_lock);
				reader_error = std::current_exception();
			}
			sampled.notify_one();
			throw;
		}
	});
	auto wait_for_samples = [&](idx_t snapshot) {
		std::unique_lock<std::mutex> guard(observation_lock);
		auto completed = sampled.wait_for(guard, std::chrono::seconds(60), [&] {
			return reader_error || (last_sample[0] >= snapshot && last_sample[1] >= snapshot);
		});
		if (reader_error) {
			std::rethrow_exception(reader_error);
		}
		Require(completed, "Filtered and unfiltered queries missed a committed automatic phase");
	};
	try {
		auto first = f.Grow(0);
		Connection route_reader(database);
		before_flush->Wait();
		auto t = f.Observe(route_reader, false);
		Require(t.snapshot == first && t.inline_rows == 128 && !t.indexed_files && !t.scan_files && !t.ann_calls,
		        "Automatic query did not see the committed T view");
		f.CheckExactOracle(t);
		before_flush->Release();
		before_build->Wait();
		auto unindexed = f.Observe(route_reader, false);
		Require(!unindexed.inline_rows && unindexed.scan_files == 1 && unindexed.scanned_rows == 128 &&
		            !unindexed.indexed_files && !unindexed.ann_calls,
		        "Automatic query did not scan the flushed F file before B publication");
		before_build->Release();
		f.Wait(2);
		auto indexed = f.Observe(route_reader, false);
		Require(!indexed.inline_rows && !indexed.scan_files && !indexed.scanned_rows && indexed.indexed_files == 1 &&
		            indexed.ann_calls == 1,
		        "Automatic query did not use ANN after B publication");
		f.Validate({std::move(t), std::move(unindexed), std::move(indexed)});
		std::cout << "automatic routes: T inline, F scan, B ANN" << std::endl;
		wait_for_samples(first);
		for (idx_t wave = 1; wave < 4; wave++) {
			auto before = f.Index().revision;
			auto snapshot = f.Grow(wave);
			f.Wait(before + 1);
			wait_for_samples(snapshot);
		}
		for (idx_t wave = 0; wave < 8; wave++) {
			auto before = f.Index().revision;
			auto snapshot = f.Wave(wave);
			f.Wait(before + 1);
			wait_for_samples(snapshot);
		}
	} catch (...) {
		before_flush->Release();
		before_build->Release();
		running = false;
		reads.wait();
		throw;
	}
	running = false;
	reads.get();
	Require(observations.size() >= 20, "Too few concurrent automatic queries completed");
	auto recall = f.Validate(observations);
	set<idx_t> observed_states;
	for (auto &observation : observations) {
		auto state = f.models.upper_bound(observation.snapshot);
		Require(state != f.models.begin(), "Missing concurrent query state");
		observed_states.insert((--state)->first);
	}
	for (auto &state : f.models) {
		Require(state.second.empty() || observed_states.count(state.first),
		        "Concurrent queries missed a committed mutation phase");
	}
	f.Stop();
	f.Replay();
	std::lock_guard<std::mutex> guard(events->lock);
	Require(events->stages["optimizer_flushed"] >= 4 && events->stages["optimizer_before_build"] >= 4 &&
	            events->stages["optimizer_before_merge"] > 0 && events->stages["optimizer_rewritten"] > 0 &&
	            events->stages["source_delete_completed"] > 0,
	        "Automatic fixture did not exercise the complete worker pipeline");
	std::cout << "automatic pipeline: " << observations.size() << " exact-snapshot concurrent queries; minimum recall "
	          << recall << std::endl;
	for (auto &event : events->stages) {
		std::cout << event.first << " " << event.second << std::endl;
	}
}
} // namespace vortex_test
} // namespace duckdb
