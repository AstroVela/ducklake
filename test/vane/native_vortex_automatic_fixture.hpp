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
#include <map>
#include <thread>

namespace duckdb {
namespace vortex_test {
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
};

struct AutomaticEvents {
	std::mutex lock;
	std::map<string, idx_t> stages;
};

struct AutomaticFixture {
	using Model = std::map<int64_t, AutomaticRow>;
	DuckDB &database;
	Connection &setup;
	string root;
	Model model;
	std::map<idx_t, Model> models;
	vector<AutomaticPlan> plans;
	std::shared_ptr<AutomaticEvents> events = std::make_shared<AutomaticEvents>();
	bool configured = false;

	AutomaticFixture(DuckDB &db, Connection &connection, const string &path)
	    : database(db), setup(connection), root(path) {
		models.emplace(0, Model {});
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
		DuckLakeVortexWriteBatch::Manage(owner, "lake", "main", "items", "knn");
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
				    DuckLakeVortexWriteBatch::Mutate(owner, "lake", "main", "items", "knn", request, Rows(pending.rows),
				                                     Deletes(pending.deletes), observe);
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
		request.query = {133, 0};
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
		return observation;
	}
	void Grow(idx_t wave, const std::function<void(const string &)> &observe = {}) {
		vector<AutomaticRow> rows;
		for (idx_t id = wave * 128; id < (wave + 1) * 128; id++) {
			rows.push_back(Row(id, 0));
		}
		Submit("seed-" + to_string(wave), std::move(rows), {}, observe);
	}
	void Wave(idx_t wave) {
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
		Submit("wave-" + to_string(wave), std::move(rows), std::move(deletes));
		Require(model.size() == 512, "Automatic wave changed the live key count");
	}
	void Replay() {
		for (auto &plan : plans) {
			Connection owner(database);
			auto result = DuckLakeVortexWriteBatch::Mutate(owner, "lake", "main", "items", "knn", plan.request,
			                                               Rows(plan.rows), Deletes(plan.deletes));
			Require(result.snapshot_id == plan.receipt.snapshot_id && result.payload_hash == plan.receipt.payload_hash,
			        "Old automatic-flow request changed its receipt");
		}
		Require(SQL(setup, "SELECT count(*) FROM lake.items")->GetValue(0, 0).GetValue<idx_t>() == model.size(),
		        "Old automatic-flow retry duplicated data");
	}
	float Validate(const vector<AutomaticObservation> &observations) {
		float minimum = 1;
		for (auto &query : observations) {
			auto state = models.upper_bound(query.snapshot);
			Require(state != models.begin(), "Missing exact snapshot oracle");
			const auto &expected = (--state)->second;
			vector<std::pair<double, int64_t>> ranked;
			for (auto &entry : expected) {
				auto &row = entry.second;
				if (!query.filtered || row.category == "B") {
					ranked.emplace_back((row.x - 133.0) * (row.x - 133.0) + row.y * row.y, row.id);
				}
			}
			std::sort(ranked.begin(), ranked.end());
			auto count = MinValue<idx_t>(10, ranked.size());
			set<int64_t> wanted, found;
			for (idx_t i = 0; i < count; i++) {
				wanted.insert(ranked[i].second);
			}
			Require(query.result.rows.size() == count, "Automatic query lost top-k coverage");
			double previous = -1;
			for (auto &values : query.result.rows) {
				auto id = values[0].GetValue<int64_t>();
				auto row = expected.find(id);
				Require(row != expected.end() && found.insert(id).second,
				        "Automatic query returned an invisible or duplicate key");
				auto &value = row->second;
				auto &embedding = ListValue::GetChildren(values[1]);
				Require(embedding[0].GetValue<float>() == value.x && embedding[1].GetValue<float>() == value.y &&
				            StringValue::Get(values[2]) == value.category &&
				            StringValue::Get(values[3]) == value.payload && (!query.filtered || value.category == "B"),
				        "Automatic query mixed row versions or filters");
				auto distance = values[4].GetValue<double>();
				auto exact = (value.x - 133.0) * (value.x - 133.0) + value.y * value.y;
				Require(distance >= previous && std::fabs(distance - exact) <= 1e-4 + 1e-5 * exact,
				        "Automatic query changed distance or order");
				previous = distance;
			}
			idx_t hits = 0;
			for (auto id : found) {
				hits += wanted.count(id);
			}
			auto recall = count ? float(hits) / count : 1.0f;
			Require(recall >= 0.8f, "Automatic query recall fell below the unchanged fixture gate");
			minimum = MinValue(minimum, recall);
		}
		return minimum;
	}
};

inline void RunAutomaticFixture(DuckDB &database, Connection &setup, const string &root, const string &mode) {
	auto pause = std::make_shared<Pause>();
	auto build_pause = std::make_shared<Pause>();
	AutomaticFixture f(database, setup, root);
	ReleasePause release {*pause};
	auto wait_for_kill = [root](const string &stage) {
		Write(root + "/checkpoint", stage);
		auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(45);
		while (std::chrono::steady_clock::now() < deadline) {
			std::this_thread::sleep_for(std::chrono::milliseconds(5));
		}
		throw IOException("Automatic checkpoint was not killed");
	};
	auto events = f.events;
	auto observe = [events, pause, build_pause, root, mode, wait_for_kill](const string &stage) {
		{
			std::lock_guard<std::mutex> guard(events->lock);
			events->stages[stage]++;
		}
		if (mode == "automatic-backpressure" && stage == "optimizer_before_flush") {
			pause->Stop();
		}
		if (mode == "automatic-flush-serialization" && stage == "optimizer_flush_started") {
			pause->Stop();
		}
		if (mode == "automatic-flush-serialization" && stage == "optimizer_before_build") {
			build_pause->Stop();
		}
		if (mode == "automatic-rewrite-serialization" && stage == "rewrite_candidate_ready") {
			pause->Stop();
		}
		if (mode == "automatic-pause-" + stage) {
			wait_for_kill(stage);
		}
	};
	f.Configure(observe, mode == "automatic-backpressure" ? 128 : 256);
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
		pause->Wait();
		std::promise<void> waiting, acquired;
		auto waiting_event = waiting.get_future(), acquired_event = acquired.get_future();
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
			return f.Submit("serialized", rows, {}, [&](const string &stage) {
				if (stage == "waiting") {
					waiting.set_value();
				} else if (stage == "acquired") {
					acquired.set_value();
				}
			});
		});
		ReleasePause release_flush {*pause}, release_build {*build_pause};
		Require(waiting_event.wait_for(std::chrono::seconds(10)) == std::future_status::ready,
		        "Managed writer never reached flush admission");
		Require(acquired_event.wait_for(std::chrono::milliseconds(100)) == std::future_status::timeout,
		        "Managed writer entered a physical-maintenance snapshot");
		Connection reader(database);
		auto before = f.Observe(reader, false);
		pause->Release();
		if (flush_case) {
			build_pause->Wait();
		}
		Require(writer.wait_for(std::chrono::seconds(10)) == std::future_status::ready,
		        "Index construction retained the flush writer reservation");
		writer.get();
		f.Validate({before, f.Observe(reader, false), f.Observe(reader, true)});
		build_pause->Release();
		f.Wait(expected_revision);
		f.Replay();
		f.Stop();
		std::cout << mode << ": physical changes serialize managed commits; captured queries remain valid" << std::endl;
		return;
	}
	if (mode == "automatic-backpressure") {
		f.Grow(0);
		pause->Wait();
		Connection owner(database);
		bool rejected = false;
		try {
			DuckLakeVortexWriteBatch::Mutate(owner, "lake", "main", "items", "knn", "blocked",
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
		f.Validate({f.Observe(reader, false), f.Observe(reader, true)});
		pause->Release();
		f.Wait(2);
		f.Submit("blocked", {f.Row(10000, 1)});
		f.Replay();
		f.Stop();
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
	Connection reader(database);
	auto reads = std::async(std::launch::async, [&] {
		while (running) {
			observations.push_back(f.Observe(reader, observations.size() % 2 != 0));
			std::this_thread::sleep_for(std::chrono::milliseconds(25));
		}
	});
	try {
		for (idx_t wave = 0; wave < 4; wave++) {
			auto before = f.Index().revision;
			f.Grow(wave);
			f.Wait(before + 1);
		}
		for (idx_t wave = 0; wave < 8; wave++) {
			auto before = f.Index().revision;
			f.Wave(wave);
			f.Wait(before + 1);
		}
		f.Replay();
	} catch (...) {
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
