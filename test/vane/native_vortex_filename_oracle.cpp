#define main SourceBenchmarkEntry
#include "native_vortex_source_gc_benchmark.cpp"
#undef main

#include "common/ducklake_filename_prefilter.hpp"
#include "storage/ducklake_metadata_manager.hpp"
#include "storage/ducklake_transaction.hpp"

namespace {
string Decode(const string &value) {
	if (value == "-") {
		return "";
	}
	Require(value.size() % 2 == 0, "Odd hex input");
	string result;
	for (idx_t i = 0; i < value.size(); i += 2) {
		result.push_back(char(std::stoul(value.substr(i, 2), nullptr, 16)));
	}
	return result;
}
string Encode(const string &value) {
	if (value.empty()) {
		return "-";
	}
	const char *digits = "0123456789abcdef";
	string result;
	for (unsigned char c : value) {
		result += digits[c >> 4];
		result += digits[c & 15];
	}
	return result;
}
class OracleFileSystem : public FileSystem {
public:
	explicit OracleFileSystem(string separator_p) : separator(std::move(separator_p)) {
	}
	string separator;
	string PathSeparator(const string &path) override {
		Require(!FileSystem::IsRemoteFile(path), "Remote path unexpectedly requested a filesystem lookup");
		return separator;
	}
	string GetName() const override {
		return "filename-oracle";
	}
};

void Paths(const string &input_path, const string &output_path) {
	std::ifstream input(input_path);
	std::ofstream output(output_path);
	output.exceptions(std::ios::badbit | std::ios::failbit);
	idx_t id;
	string sep, raw, target;
	while (input >> id >> sep >> raw >> target) {
		OracleFileSystem fs(Decode(sep));
		Require(!fs.separator.empty(), "Oracle separator must be nonempty");
		auto known = Decode(raw), wanted = Decode(target);
		auto canonical = fs.FileSystem::CanonicalizePath(known);
		auto may = DuckLakeFilenameMayMatch(fs, known, wanted);
		auto candidate = may && fs.FileSystem::CanonicalizePath(known) == wanted;
		output << id << ' ' << may << ' ' << (canonical == wanted) << ' ' << candidate << ' ' << Encode(canonical)
		       << '\n';
	}
	Require(input.eof() && bool(output), "Incomplete path oracle stream");
}

void Metadata(const string &root, const string &input_path, const string &output_path) {
	DuckDB database(nullptr);
	database.LoadStaticExtension<DucklakeExtension>();
	database.LoadStaticExtension<VortexExtension>();
	Connection writer(database);
	SQL(writer, "SET threads=2; LOAD parquet; ATTACH " +
	                Value("ducklake:" + root + "/metadata.ducklake").ToSQLString() + " AS lake(DATA_PATH " +
	                Value(root + "/data").ToSQLString() + ",DATA_INLINING_ROW_LIMIT 1000)");
	State before(writer);
	Connection owner(database);
	auto &transaction = Index::Begin(owner, "lake");
	auto source = Index::Query(
	    transaction, "SELECT f.data_file_id,f.table_id,t.schema_id FROM {METADATA_CATALOG}.ducklake_data_file f "
	                 "JOIN {METADATA_CATALOG}.ducklake_table t ON f.table_id=t.table_id "
	                 "ORDER BY f.data_file_id LIMIT 1");
	auto source_row = source->Fetch();
	Require(source_row && source_row->size() == 1, "Missing metadata oracle seed");
	auto file_id = source_row->GetValue(0, 0).ToSQLString();
	auto table_id = source_row->GetValue(1, 0).ToSQLString();
	auto schema_id = source_row->GetValue(2, 0).ToSQLString();
	Index::Query(transaction, "CREATE TEMP TABLE filename_original_data AS SELECT * FROM "
	                          "{METADATA_CATALOG}.ducklake_data_file WHERE data_file_id=" +
	                              file_id);
	std::ifstream input(input_path);
	std::ofstream output(output_path);
	output.exceptions(std::ios::badbit | std::ios::failbit);
	idx_t id;
	int kind, historical, file_relative, table_relative, schema_relative;
	string schema, table, file, target;
	while (input >> id >> kind >> historical >> file_relative >> table_relative >> schema_relative >> schema >> table >>
	       file >> target) {
		Index::Query(transaction, "DELETE FROM {METADATA_CATALOG}.ducklake_data_file");
		Index::Query(transaction, "DELETE FROM {METADATA_CATALOG}.ducklake_delete_file");
		Index::Query(transaction,
		             "UPDATE {METADATA_CATALOG}.ducklake_schema SET path=" + Value(Decode(schema)).ToSQLString() +
		                 ",path_is_relative=" + (schema_relative ? "true" : "false") + " WHERE schema_id=" + schema_id);
		Index::Query(transaction,
		             "UPDATE {METADATA_CATALOG}.ducklake_table SET path=" + Value(Decode(table)).ToSQLString() +
		                 ",path_is_relative=" + (table_relative ? "true" : "false") + " WHERE table_id=" + table_id);
		auto end = historical ? to_string(before.snapshots.back()) : "NULL";
		if (kind == 0) {
			Index::Query(transaction,
			             "INSERT INTO {METADATA_CATALOG}.ducklake_data_file SELECT * FROM filename_original_data");
			Index::Query(transaction,
			             "UPDATE {METADATA_CATALOG}.ducklake_data_file SET path=" + Value(Decode(file)).ToSQLString() +
			                 ",path_is_relative=" + (file_relative ? "true" : "false") + ",end_snapshot=" + end);
		} else {
			Require(kind == 1, "Unknown reference row kind");
			Index::Query(
			    transaction,
			    "INSERT INTO {METADATA_CATALOG}.ducklake_delete_file"
			    "(delete_file_id,table_id,begin_snapshot,end_snapshot,data_file_id,path,path_is_relative) VALUES(1," +
			        table_id + "," + to_string(before.snapshots.front()) + "," + end + "," + file_id + "," +
			        Value(Decode(file)).ToSQLString() + "," + (file_relative ? "true" : "false") + ")");
		}
		auto actual = transaction.GetMetadataManager().HasFileReference(Decode(target), "/");
		output << id << ' ' << actual << '\n';
	}
	Require(input.eof(), "Incomplete metadata oracle stream");
	Index::Query(transaction, "ALTER TABLE {METADATA_CATALOG}.ducklake_data_file RENAME TO filename_oracle_hidden");
	bool raised = false;
	try {
		transaction.GetMetadataManager().HasFileReference(root + "/data/absent", "/");
	} catch (Exception &) {
		raised = true;
	}
	Require(raised, "Reference query error was suppressed");
	owner.Rollback();
	Require(State(writer) == before, "Metadata oracle rollback changed its original state");
	output << "error_propagation 1\nrollback_state 1\n";
}
} // namespace

int main(int argc, char **argv) {
	try {
		Require(argc >= 2, "Missing filename oracle mode");
		string mode = argv[1];
		if (mode == "paths") {
			Require(argc == 4, "Usage: oracle paths INPUT OUTPUT");
			Paths(argv[2], argv[3]);
		} else {
			Require(mode == "metadata" && argc == 5, "Usage: oracle metadata ROOT INPUT OUTPUT");
			Metadata(argv[2], argv[3], argv[4]);
		}
		return 0;
	} catch (const std::exception &error) {
		std::cerr << error.what() << std::endl;
		return 1;
	}
}
