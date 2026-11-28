// Deterministic component probe. Not linked into the production provider.
#include "inc/Core/SPANN/Index.h"
#include "inc/Core/SPANN/ExtraRocksDBController.h"
#include "inc/Core/SPANN/ExtraDynamicSearcher.h"
#include "inc/Helper/SimpleIniReader.h"

#include <algorithm>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <utility>
#include <vector>

using SPTAG::ErrorCode;
namespace fs = std::filesystem;
constexpr int DIM = 8;
constexpr int HEADS = 64;
constexpr int ROWS = 100000;
constexpr int BLOCK_ROWS = 1024;
constexpr int POSTING_PAGES = 12;

static void require(bool yes, const char *message) {
	if (!yes)
		throw std::runtime_error(message);
}
static void check(ErrorCode code, const char *operation) {
	if (code != ErrorCode::Success)
		throw std::runtime_error(std::string(operation) + ":" + std::to_string(int(code)));
}
static auto dynamic(SPTAG::SPANN::Index<float> &index) {
	auto result = std::dynamic_pointer_cast<SPTAG::SPANN::ExtraDynamicSearcher<float>>(index.GetDiskIndex());
	require(bool(result), "Dynamic backend required");
	return result;
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
static std::shared_ptr<SPTAG::SPANN::Index<float>> open(const fs::path &root, bool lire) {
	auto index = std::make_shared<SPTAG::SPANN::Index<float>>();
	SPTAG::Helper::IniReader config;
	check(config.LoadIniFile((root / "indexloader.ini").string()), "LoadIni");
	check(index->LoadConfig(config), "LoadConfig");
	const auto &options = *index->GetOptions();
	require(options.m_useKV && !options.m_useSPDK && options.m_inPlace != lire && !options.m_update &&
	            !options.m_preReassign && !options.m_excludehead &&
	            options.m_dim == DIM && options.m_headVectorCount == HEADS && options.m_datasetCapacity == ROWS &&
	            options.m_datasetRowsInBlock == BLOCK_ROWS && options.m_postingPageLimit == POSTING_PAGES &&
	            options.m_searchPostingPageLimit == POSTING_PAGES,
	        "Unsupported dynamic probe profile");
	paths(*index, root);
	std::vector<std::shared_ptr<SPTAG::Helper::DiskIO>> handles;
	auto files = index->GetIndexFiles();
	for (auto &name : *files) {
		const fs::path relative(name);
		require(!relative.is_absolute() && std::find(relative.begin(), relative.end(), "..") == relative.end(),
		        "Head file outside checkpoint");
		auto handle = SPTAG::f_createIO();
		require(handle && handle->Initialize((root / name).c_str(), std::ios::binary | std::ios::in),
		        "Index file missing");
		handles.push_back(std::move(handle));
	}
	check(index->LoadIndexData(handles), "LoadData");
	auto heads = index->GetMemoryIndex();
	const auto head_slots = heads->GetNumSamples();
	if (lire) {
		require(head_slots >= HEADS && head_slots <= ROWS, "Unexpected native head count");
		require(dynamic(*index)->ProbeValidatePostings(head_slots, heads.get()), "Invalid LIRE postings");
	} else {
		require(head_slots == HEADS, "Unexpected native head count");
	}
	index->SetReady(true);
	return index;
}
static void checkpoint(SPTAG::SPANN::Index<float> &index, const fs::path &root) {
	check(index.GetDiskIndex()->Checkpoint(), "CheckpointPostings");
	check(index.SaveIndex(root.string()), "SaveIndex");
}
static std::shared_ptr<SPTAG::SPANN::Index<float>> seed(const fs::path &root, const fs::path &input, bool lire) {
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
	set(*index, "Base", "DataBlockSize", std::to_string(BLOCK_ROWS));
	set(*index, "Base", "DataCapacity", std::to_string(ROWS));
	paths(*index, root);
	set(*index, "SelectHead", "isExecute", "true");
	set(*index, "SelectHead", "SelectHeadType", "Random");
	set(*index, "SelectHead", "Count", std::to_string(HEADS));
	set(*index, "SelectHead", "NumberOfThreads", "1");
	set(*index, "BuildHead", "isExecute", "true");
	set(*index, "BuildHead", "DistCalcMethod", "L2");
	set(*index, "BuildHead", "NumberOfThreads", "1");
	set(*index, "BuildHead", "BKTKmeansK", "4");
	set(*index, "BuildHead", "TPTNumber", "4");
	set(*index, "BuildHead", "TPTLeafSize", "32");
	set(*index, "BuildHead", "NeighborhoodSize", "16");
	set(*index, "BuildHead", "RefineIterations", "2");
	set(*index, "BuildHead", "DataBlockSize", std::to_string(BLOCK_ROWS));
	set(*index, "BuildHead", "DataCapacity", std::to_string(ROWS));
	set(*index, "BuildSSDIndex", "isExecute", "true");
	set(*index, "BuildSSDIndex", "BuildSsdIndex", "true");
	set(*index, "BuildSSDIndex", "NumberOfThreads", "1");
	set(*index, "BuildSSDIndex", "IOThreadsPerHandler", "1");
	set(*index, "BuildSSDIndex", "UseKV", "true");
	set(*index, "BuildSSDIndex", "UseSPDK", "false");
	set(*index, "BuildSSDIndex", "InPlace", lire ? "false" : "true");
	set(*index, "BuildSSDIndex", "Update", "false");
	set(*index, "BuildSSDIndex", "ExcludeHead", "false");
	set(*index, "BuildSSDIndex", "PostingPageLimit", std::to_string(POSTING_PAGES));
	set(*index, "BuildSSDIndex", "SearchPostingPageLimit", std::to_string(POSTING_PAGES));
	set(*index, "BuildSSDIndex", "ReplicaCount", "4");
	set(*index, "BuildSSDIndex", "InternalResultNum", "32");
	set(*index, "BuildSSDIndex", "SearchInternalResultNum", "64");
	if (lire) {
		SPTAG::rg.seed(20261107);
		std::srand(20261107);
		set(*index, "BuildSSDIndex", "ReassignK", "64");
		set(*index, "BuildSSDIndex", "DisableReassign", "false");
	}
	check(index->BuildIndex(vectors.data(), vectors.size() / DIM, DIM, false, true), "BuildIndex");
	checkpoint(*index, root);
	return index;
}

class LireAppend {
public:
	size_t remaining() const {
		return vectors.size() / DIM - appended;
	}

	void start(SPTAG::SPANN::Index<float> &index, std::vector<float> input) {
		require(SPTAG::SPANN::LireControl::jobs.empty(), "LIRE_APPEND_PENDING");
		first_vid = index.GetNumVids();
		vectors = std::move(input);
		appended = 0;
		advance(index, false);
	}

	void advance(SPTAG::SPANN::Index<float> &index, bool metered) {
		if (!remaining())
			return;
		auto heads = index.GetMemoryIndex();
		auto extra = dynamic(index);
		namespace control = SPTAG::SPANN::LireControl;
		while (remaining() && (!metered || control::steps < control::max_steps)) {
			const auto expected = first_vid + static_cast<SPTAG::SizeType>(appended);
			auto vector = vectors.data() + appended * DIM;
			// Preflight all replicas before allocating a VID or writing any replica of this row.
			if (!extra->ProbePrepareAppend(heads.get(), vector, expected))
				return;
			if (metered)
				control::enter();
			SPTAG::SizeType assigned;
			check(index.AddIndexSPFresh(vector, 1, DIM, &assigned), "LIRE append");
			require(assigned == expected, "LIRE append VID changed");
			++appended;
		}
	}

	void reply(std::ostream &out) const {
		out << "{\"ids\":[";
		for (size_t i = 0; i < appended; ++i)
			out << (i ? "," : "") << first_vid + i;
		out << "]";
		if (remaining())
			out << ",\"pending_add\":" << remaining();
		out << "}";
	}

private:
	std::vector<float> vectors;
	size_t appended = 0;
	SPTAG::SizeType first_vid = 0;
};

static std::string state(SPTAG::SPANN::Index<float> &index, const LireAppend &append) {
	auto extra = dynamic(index);
	auto heads = index.GetMemoryIndex();
	std::ostringstream out;
	out << std::setprecision(9) << "{\"queued\":" << SPTAG::SPANN::LireControl::jobs.size()
	    << ",\"pending_add\":" << append.remaining() << ",\"vids\":" << index.GetNumVids()
	    << ",\"steps\":" << SPTAG::SPANN::LireControl::steps
	    << ",\"posting_bytes\":" << SPTAG::SPANN::LireControl::bytes << ",\"splits\":" << extra->ProbeSplits()
	    << ",\"merges\":" << extra->ProbeMerges() << ",\"reassigns\":" << extra->ProbeReassigns()
	    << ",\"version_changes\":" << SPTAG::SPANN::LireControl::version_changes << ",\"heads\":[";
	for (int i = 0; i < heads->GetNumSamples(); i++) {
		out << (i ? "," : "") << "{\"id\":" << i << ",\"active\":" << (heads->ContainSample(i) ? "true" : "false")
		    << ",\"size\":" << extra->ProbePostingSize(i) << ",\"vector\":[";
		auto vector = reinterpret_cast<const float *>(heads->GetSample(i));
		for (int d = 0; d < DIM; d++)
			out << (d ? "," : "") << vector[d];
		out << "]}";
	}
	out << "]}";
	return out.str();
}
static void dump(const fs::path &root) {
	SPTAG::SPANN::RocksDBIO::ReadOnly = true;
	SPTAG::SPANN::RocksDBIO db((root / "kv").c_str(), false);
	std::ostringstream out;
	out << "{\"postings\":[";
	bool comma = false;
	constexpr char hex[] = "0123456789abcdef";
	for (const auto &[key, value] : db.ProbeRecords()) {
		require(static_cast<uint64_t>(out.tellp()) + 2 * value.size() + 64 < 32ULL * 1024 * 1024,
		        "Dump metadata budget");
		out << (comma ? "," : "") << "[" << key << ",\"";
		for (unsigned char c : value)
			out << hex[c >> 4] << hex[c & 15];
		out << "\"]";
		comma = true;
	}
	out << "]}";
	std::cout << "R7 " << out.str() << std::endl;
}

int main(int argc, char **argv) {
	try {
		require(argc == 3 || argc == 4, "Usage: probe seed|seed-lire ROOT VECTORS | "
		                                "probe open|update|open-lire|update-lire|dump|remove-postings ROOT");
		fs::path root(argv[2]);
		require(root.is_absolute(), "Absolute root required");
		const std::string mode(argv[1]);
		if (mode == "remove-postings" && argc == 3) {
			// The regression calls this on a disposable copy of a closed seed.
			require(fs::is_directory(root / "kv"), "Posting database missing");
			SPTAG::SPANN::RocksDBIO db((root / "kv").c_str(), false);
			for (SPTAG::SizeType head = 0; head < HEADS; ++head)
				check(db.Delete(head), "RemovePosting");
			check(db.Checkpoint(), "CheckpointPostings");
			return 0;
		}
		if (mode == "dump" && argc == 3) {
			dump(root);
			return 0;
		}
		const bool lire = mode == "seed-lire" || mode == "open-lire" || mode == "update-lire";
		const bool read_only = mode == "open" || mode == "open-lire";
		SPTAG::SPANN::LireControl::enabled = lire && !read_only;
		std::shared_ptr<SPTAG::SPANN::Index<float>> index;
		if ((mode == "seed" || mode == "seed-lire") && argc == 4)
			index = seed(root, argv[3], lire);
		else {
			require((read_only || mode == "update" || mode == "update-lire") && argc == 3,
			        "Unknown mode");
			SPTAG::SPANN::RocksDBIO::ReadOnly = read_only;
			index = open(root, lire);
		}
		const auto total = index->GetNumVids();
		const auto live = index->GetNumSamples();
		require(total >= 0 && total <= ROWS && live >= 0 && live <= total, "Native VID budget");
		std::ostringstream ready;
		ready << "R7 {\"ready\":true,\"live\":" << live;
		if (read_only) {
			std::string visibility(total, '0');
			for (SPTAG::SizeType vid = 0; vid < total; ++vid)
				if (!index->IsDeleted(vid))
					visibility[vid] = '1';
			ready << ",\"visibility\":\"" << visibility << "\"";
		}
		std::cout << ready.str() << "}" << std::endl;
		LireAppend append;
		std::string line;
		while (std::getline(std::cin, line)) {
			require(line.size() <= 4 * 1024 * 1024, "Command budget");
			std::istringstream in(line);
			std::string op;
			in >> op;
			if (append.remaining()) {
				require(op == "state" || op == "drain" || op == "pause",
				        op == "checkpoint" || op == "close" ? "LIRE_CHECKPOINT_BUSY" : "LIRE_APPEND_PENDING");
			}
			std::ostringstream out;
			out << std::setprecision(9);
			if (op == "state") {
				out << state(*index, append);
			} else if (op == "drain") {
				uint32_t jobs;
				uint64_t bytes;
				require(lire && !SPTAG::SPANN::RocksDBIO::ReadOnly && bool(in >> jobs >> bytes), "Invalid drain");
				SPTAG::SPANN::LireControl::drain(jobs, bytes, [&] { append.advance(*index, true); });
				out << state(*index, append);
			} else if (op == "merge") {
				int head;
				require(lire && !SPTAG::SPANN::RocksDBIO::ReadOnly && bool(in >> head) && head >= 0 &&
				            head < index->GetMemoryIndex()->GetNumSamples() &&
				            index->GetMemoryIndex()->ContainSample(head),
				        "Invalid merge head");
				dynamic(*index)->ProbeMerge(index->GetMemoryIndex().get(), head);
				out << state(*index, append);
			} else if (op == "pause") {
				std::string stage;
				require(lire && !SPTAG::SPANN::RocksDBIO::ReadOnly && bool(in >> stage) &&
				            (stage == "after_split" || stage == "after_merge" || stage == "after_reassign_version"),
				        "Invalid pause stage");
				SPTAG::SPANN::LireControl::pause = stage;
				out << "{\"armed\":true}";
			} else if (op == "add") {
				require(!SPTAG::SPANN::RocksDBIO::ReadOnly, "Read-only view");
				int count;
				require(bool(in >> count) && count > 0 && count <= 1000 &&
				            index->GetNumVids() + count <= ROWS,
				        "Add budget");
				std::vector<float> vectors(count * DIM);
				for (auto &v : vectors)
					require(bool(in >> v) && std::isfinite(v), "Invalid vector");
				if (lire) {
					append.start(*index, std::move(vectors));
					append.reply(out);
				} else {
					std::vector<SPTAG::SizeType> ids(count);
					const auto status = index->AddIndexSPFresh(vectors.data(), count, DIM, ids.data());
					if (status == ErrorCode::PostingCapacityExceeded) {
						std::cout << "R7 {\"error\":\"POSTING_CAPACITY\"}" << std::endl;
						return 1;
					}
					check(status, "AddIndexSPFresh");
					out << "{\"ids\":[";
					for (int i = 0; i < count; i++)
						out << (i ? "," : "") << ids[i];
					out << "]}";
				}
			} else if (op == "delete") {
				require(!SPTAG::SPANN::RocksDBIO::ReadOnly, "Read-only view");
				int id;
				require(bool(in >> id) && id >= 0 && id < index->GetNumVids(),
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
				SPTAG::SPANN::LireControl::quiescent();
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
