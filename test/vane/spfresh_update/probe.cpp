// Deterministic component probe. Not linked into the production provider.
#include "inc/Core/SPANN/Index.h"
#include "inc/Core/SPANN/ExtraRocksDBController.h"
#include "inc/Helper/SimpleIniReader.h"

#include <cmath>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <vector>

using SPTAG::ErrorCode;
namespace fs = std::filesystem;
constexpr int DIM = 8;
constexpr int ROWS = 100000;

static void require(bool yes, const char *message) {
	if (!yes)
		throw std::runtime_error(message);
}
static void check(ErrorCode code, const char *operation) {
	if (code != ErrorCode::Success)
		throw std::runtime_error(std::string(operation) + ":" + std::to_string(int(code)));
}
static void set(SPTAG::SPANN::Index<float> &index, const char *section, const char *name, const std::string &value) {
	check(index.SetParameter(name, value.c_str(), section), name);
}
static void paths(SPTAG::SPANN::Index<float> &index, const fs::path &root) {
	set(index, "Base", "IndexDirectory", root.string());
	set(index, "Base", "DeletedIDs", (root / "versions.bin").string());
	set(index, "BuildSSDIndex", "KVPath", (root / "kv").string());
	set(index, "BuildSSDIndex", "SsdInfoFile", (root / "posting-sizes.bin").string());
	set(index, "BuildSSDIndex", "TmpDir", root.string());
}
static std::shared_ptr<SPTAG::SPANN::Index<float>> open(const fs::path &root) {
	auto index = std::make_shared<SPTAG::SPANN::Index<float>>();
	SPTAG::Helper::IniReader config;
	check(config.LoadIniFile((root / "indexloader.ini").string()), "LoadIni");
	check(index->LoadConfig(config), "LoadConfig");
	require(index->GetOptions()->m_useKV && !index->GetOptions()->m_useSPDK && index->GetOptions()->m_inPlace &&
	            !index->GetOptions()->m_update,
	        "Unsupported dynamic probe profile");
	paths(*index, root);
	std::vector<std::shared_ptr<SPTAG::Helper::DiskIO>> handles;
	auto files = index->GetIndexFiles();
	for (auto &name : *files) {
		auto handle = SPTAG::f_createIO();
		require(handle && handle->Initialize((root / name).c_str(), std::ios::binary | std::ios::in),
		        "Index file missing");
		handles.push_back(std::move(handle));
	}
	check(index->LoadIndexData(handles), "LoadData");
	index->SetReady(true);
	return index;
}
static void checkpoint(SPTAG::SPANN::Index<float> &index, const fs::path &root) {
	check(index.GetDiskIndex()->Checkpoint(), "CheckpointPostings");
	check(index.SaveIndex(root.string()), "SaveIndex");
}
static std::shared_ptr<SPTAG::SPANN::Index<float>> seed(const fs::path &root, const fs::path &input) {
	require(fs::create_directory(root), "Seed requires a new directory");
	const auto bytes = fs::file_size(input);
	require(bytes % (4 * DIM) == 0 && bytes >= 65 * 4 * DIM && bytes <= ROWS * 4 * DIM, "Seed budget");
	std::vector<float> vectors(bytes / 4);
	std::ifstream in(input, std::ios::binary);
	require(bool(in.read(reinterpret_cast<char *>(vectors.data()), bytes)), "Cannot read seed");
	for (float v : vectors)
		require(std::isfinite(v), "Non-finite seed");
	auto index = std::make_shared<SPTAG::SPANN::Index<float>>();
	set(*index, "Base", "IndexAlgoType", "BKT");
	set(*index, "Base", "ValueType", "Float");
	set(*index, "Base", "DistCalcMethod", "L2");
	set(*index, "Base", "DataBlockSize", "1024");
	set(*index, "Base", "DataCapacity", std::to_string(ROWS));
	paths(*index, root);
	set(*index, "SelectHead", "isExecute", "true");
	set(*index, "SelectHead", "SelectHeadType", "Random");
	set(*index, "SelectHead", "Count", "64");
	set(*index, "SelectHead", "NumberOfThreads", "1");
	set(*index, "BuildHead", "isExecute", "true");
	set(*index, "BuildHead", "DistCalcMethod", "L2");
	set(*index, "BuildHead", "NumberOfThreads", "1");
	set(*index, "BuildHead", "BKTKmeansK", "4");
	set(*index, "BuildHead", "TPTNumber", "4");
	set(*index, "BuildHead", "TPTLeafSize", "32");
	set(*index, "BuildHead", "NeighborhoodSize", "16");
	set(*index, "BuildHead", "RefineIterations", "2");
	set(*index, "BuildHead", "DataBlockSize", "1024");
	set(*index, "BuildHead", "DataCapacity", std::to_string(ROWS));
	set(*index, "BuildSSDIndex", "isExecute", "true");
	set(*index, "BuildSSDIndex", "BuildSsdIndex", "true");
	set(*index, "BuildSSDIndex", "NumberOfThreads", "1");
	set(*index, "BuildSSDIndex", "IOThreadsPerHandler", "1");
	set(*index, "BuildSSDIndex", "UseKV", "true");
	set(*index, "BuildSSDIndex", "UseSPDK", "false");
	set(*index, "BuildSSDIndex", "InPlace", "true");
	set(*index, "BuildSSDIndex", "Update", "false");
	set(*index, "BuildSSDIndex", "ExcludeHead", "false");
	set(*index, "BuildSSDIndex", "PostingPageLimit", "12");
	set(*index, "BuildSSDIndex", "SearchPostingPageLimit", "12");
	set(*index, "BuildSSDIndex", "ReplicaCount", "4");
	set(*index, "BuildSSDIndex", "InternalResultNum", "32");
	set(*index, "BuildSSDIndex", "SearchInternalResultNum", "64");
	check(index->BuildIndex(vectors.data(), vectors.size() / DIM, DIM, false, true), "BuildIndex");
	checkpoint(*index, root);
	return index;
}

int main(int argc, char **argv) {
	try {
		require(argc == 3 || argc == 4, "Usage: probe seed ROOT VECTORS | probe open|update ROOT");
		fs::path root(argv[2]);
		require(root.is_absolute(), "Absolute root required");
		std::shared_ptr<SPTAG::SPANN::Index<float>> index;
		if (std::string(argv[1]) == "seed" && argc == 4)
			index = seed(root, argv[3]);
		else {
			require((std::string(argv[1]) == "open" || std::string(argv[1]) == "update") && argc == 3, "Unknown mode");
			SPTAG::SPANN::RocksDBIO::ReadOnly = std::string(argv[1]) == "open";
			index = open(root);
		}
		std::cout << "R7 {\"ready\":true,\"live\":" << index->GetNumSamples() << "}" << std::endl;
		std::string line;
		while (std::getline(std::cin, line)) {
			require(line.size() <= 4 * 1024 * 1024, "Command budget");
			std::istringstream in(line);
			std::string op;
			in >> op;
			std::ostringstream out;
			out << std::setprecision(9);
			if (op == "add") {
				require(!SPTAG::SPANN::RocksDBIO::ReadOnly, "Read-only view");
				int count;
				require(bool(in >> count) && count > 0 && count <= 1000 &&
				            index->GetNumSamples() + index->GetNumDeleted() + count <= ROWS,
				        "Add budget");
				std::vector<float> vectors(count * DIM);
				for (auto &v : vectors)
					require(bool(in >> v) && std::isfinite(v), "Invalid vector");
				std::vector<SPTAG::SizeType> ids(count);
				check(index->AddIndexSPFresh(vectors.data(), count, DIM, ids.data()), "AddIndexSPFresh");
				out << "{\"ids\":[";
				for (int i = 0; i < count; i++)
					out << (i ? "," : "") << ids[i];
				out << "]}";
			} else if (op == "delete") {
				require(!SPTAG::SPANN::RocksDBIO::ReadOnly, "Read-only view");
				int id;
				require(bool(in >> id) && id >= 0 && id < index->GetNumSamples() + index->GetNumDeleted(),
				        "Invalid VID");
				auto result = index->DeleteIndex(id);
				require(result == ErrorCode::Success || result == ErrorCode::VectorNotFound, "Delete failed");
				out << "{\"deleted\":" << (result == ErrorCode::Success ? "true" : "false") << "}";
			} else if (op == "search") {
				int k;
				require(bool(in >> k) && k >= 1 && k <= 64, "Invalid k");
				std::vector<float> q(DIM);
				for (auto &v : q)
					require(bool(in >> v) && std::isfinite(v), "Invalid query");
				SPTAG::QueryResult result(q.data(), k, false);
				check(index->SearchIndex(result), "SearchIndex");
				out << "{\"hits\":[";
				bool comma = false;
				for (int i = 0; i < k; i++) {
					auto hit = result.GetResult(i);
					if (hit->VID < 0)
						continue;
					require(std::isfinite(hit->Dist), "Non-finite distance");
					out << (comma ? "," : "") << "[" << hit->VID << "," << hit->Dist << "]";
					comma = true;
				}
				out << "]}";
			} else if (op == "checkpoint") {
				require(!SPTAG::SPANN::RocksDBIO::ReadOnly, "Read-only view");
				checkpoint(*index, root);
				out << "{\"saved\":true}";
			} else if (op == "close") {
				index.reset();
				std::cout << "R7 {\"closed\":true}" << std::endl;
				return 0;
			} else
				throw std::runtime_error("Unknown command");
			std::string extra;
			require(!(in >> extra), "Trailing input");
			std::cout << "R7 " << out.str() << std::endl;
		}
		throw std::runtime_error("Unconfirmed input disconnect");
	} catch (const std::exception &e) {
		std::cerr << "R7 rejected: " << e.what() << std::endl;
		return 1;
	}
}
