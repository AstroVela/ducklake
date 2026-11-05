#include "common/ducklake_snapshot_lookup.hpp"

#include <fstream>
#include <iostream>
#include <stdexcept>

using namespace duckdb;

int main(int argc, char **argv) {
	try {
		if (argc != 3) {
			throw std::runtime_error("Usage: expiry_lookup INPUT OUTPUT");
		}
		std::ifstream input(argv[1]);
		std::ofstream output(argv[2]);
		input.exceptions(std::ios::badbit);
		output.exceptions(std::ios::badbit | std::ios::failbit);
		idx_t case_id, snapshot_count, file_count;
		while (input >> case_id >> snapshot_count >> file_count) {
			vector<int64_t> snapshots(snapshot_count);
			for (auto &snapshot : snapshots) {
				input >> snapshot;
			}
			output << case_id;
			for (idx_t row = 0; row < file_count; row++) {
				idx_t id;
				int64_t begin, end;
				bool begin_null, end_null, table_expired;
				if (!(input >> id >> begin_null >> begin >> end_null >> end >> table_expired)) {
					throw std::runtime_error("Incomplete interval input");
				}
				if (DuckLakeFileCanExpire(snapshots, begin_null ? Value() : Value::BIGINT(begin),
				                          end_null ? Value() : Value::BIGINT(end), table_expired)) {
					output << ' ' << id;
				}
			}
			output << '\n';
		}
		if (!input.eof()) {
			throw std::runtime_error("Invalid interval input");
		}
		return 0;
	} catch (const std::exception &error) {
		std::cerr << error.what() << std::endl;
		return 1;
	}
}
