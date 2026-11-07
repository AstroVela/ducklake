// Deterministic scheduling and fault hooks for the standalone LIRE probe only.
#pragma once
#include "inc/Core/Common.h"
#include "inc/Helper/ThreadPool.h"
#include <deque>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>

namespace SPTAG::SPANN::LireControl {
inline bool enabled = false;
inline bool metering = false;
inline bool failed = false;
inline uint32_t max_steps = 0;
inline uint32_t steps = 0;
inline uint32_t completed = 0;
inline uint64_t max_bytes = 0;
inline uint64_t bytes = 0;
inline uint64_t version_changes = 0;
inline std::string pause;
struct Work {
	std::unique_ptr<Helper::ThreadPool::Job> job;
	uint32_t bound;
};
inline std::deque<Work> jobs;

inline void enqueue(Helper::ThreadPool::Job *job, uint32_t bound = 1) {
	std::unique_ptr<Helper::ThreadPool::Job> owned(job);
	if (jobs.size() >= 100000)
		throw std::runtime_error("LIRE_QUEUE_BUDGET");
	jobs.push_back({std::move(owned), bound});
}
inline void enter() {
	if (!enabled)
		return;
	if (!metering || failed || steps >= max_steps)
		throw std::runtime_error("LIRE_WORK_BUDGET");
	++steps;
}
inline void charge(uint64_t size) {
	if (!metering)
		return;
	if (failed || size > max_bytes - bytes)
		throw std::runtime_error("LIRE_BYTE_BUDGET");
	bytes += size;
}
inline void point(const std::string &stage) {
	if (!enabled || pause != stage)
		return;
	pause.clear();
	std::cout << "R7 {\"pause\":\"" << stage << "\"}" << std::endl;
	std::string response;
	if (!std::getline(std::cin, response) || response != "continue")
		throw std::runtime_error("LIRE_PAUSE_CLOSED");
}
inline void drain(uint32_t work, uint64_t payload) {
	if (!enabled || failed || work < 1 || work > 16 || payload < 1 || payload > 64ULL * 1024 * 1024)
		throw std::runtime_error("LIRE_BUDGET");
	max_steps = work;
	max_bytes = payload;
	steps = completed = 0;
	bytes = 0;
	metering = true;
	try {
		Helper::ThreadPool::Abort abort(false);
		while (!jobs.empty() && jobs.front().bound <= max_steps - steps) {
			auto work_item = std::move(jobs.front());
			jobs.pop_front();
			work_item.job->exec(&abort);
			++completed;
		}
		metering = false;
	} catch (...) {
		failed = true;
		metering = false;
		throw;
	}
}
inline void quiescent() {
	if (enabled && (failed || metering || !jobs.empty()))
		throw std::runtime_error("LIRE_CHECKPOINT_BUSY");
}
} // namespace SPTAG::SPANN::LireControl
