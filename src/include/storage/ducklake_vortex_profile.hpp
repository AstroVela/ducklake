#pragma once

#include "duckdb/common/common.hpp"

#include <array>
#include <chrono>
#include <mutex>

namespace duckdb {

enum class DuckLakeVortexPhase : uint8_t {
	OPTIMIZER_TOTAL,
	OPTIMIZER_PROBE,
	OPTIMIZER_METADATA,
	MAINTENANCE_TOTAL,
	MAINTENANCE_PROBE,
	MAINTENANCE_METADATA,
	BUILD_RECOVERY,
	BUILD_GC,
	GENERATION_GC,
	SOURCE_GC,
	SOURCE_WAIT,
	SOURCE_METADATA,
	SOURCE_EXPIRE,
	SOURCE_ARTIFACT,
	SOURCE_UNLINK,
	COUNT
};

struct DuckLakeVortexTiming {
	uint64_t calls = 0, total_ns = 0, max_ns = 0;
};

class DuckLakeVortexMaintenanceProfile {
public:
	explicit DuckLakeVortexMaintenanceProfile(string id_p) : id(std::move(id_p)) {
	}
	const string id;
	using Samples = std::array<DuckLakeVortexTiming, idx_t(DuckLakeVortexPhase::COUNT)>;

	void Record(DuckLakeVortexPhase phase, uint64_t ns) {
		std::lock_guard<std::mutex> guard(lock);
		auto &sample = samples[idx_t(phase)];
		sample.calls++;
		sample.total_ns += ns;
		sample.max_ns = MaxValue(sample.max_ns, ns);
	}
	Samples Snapshot() {
		std::lock_guard<std::mutex> guard(lock);
		return samples;
	}

private:
	std::mutex lock;
	Samples samples {};
};

// Steady-clock wall time, including waits and exceptions. Counters stay in memory.
class DuckLakeVortexPhaseTimer {
public:
	DuckLakeVortexPhaseTimer(DuckLakeVortexMaintenanceProfile *profile_p, DuckLakeVortexPhase phase_p)
	    : profile(profile_p), phase(phase_p), started(std::chrono::steady_clock::now()) {
	}
	~DuckLakeVortexPhaseTimer() {
		Finish();
	}
	DuckLakeVortexPhaseTimer(const DuckLakeVortexPhaseTimer &) = delete;
	DuckLakeVortexPhaseTimer &operator=(const DuckLakeVortexPhaseTimer &) = delete;

	void Switch(DuckLakeVortexPhase next) {
		auto now = std::chrono::steady_clock::now();
		Finish(now);
		phase = next;
		started = now;
	}

private:
	void Finish(std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now()) {
		if (profile) {
			profile->Record(phase, std::chrono::duration_cast<std::chrono::nanoseconds>(now - started).count());
		}
	}
	DuckLakeVortexMaintenanceProfile *profile;
	DuckLakeVortexPhase phase;
	std::chrono::steady_clock::time_point started;
};
} // namespace duckdb
