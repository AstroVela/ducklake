#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

#ifndef _WIN32
#include <cerrno>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace duckdb {
namespace vortex_detail {
inline void CheckRelative(const string &path, bool component) {
	if (path.empty() || path[0] == '/' || path.find('\\') != string::npos || path.find(':') != string::npos ||
	    path.find('\0') != string::npos || (component && path.find('/') != string::npos)) {
		throw InvalidInputException("Invalid local Vortex path");
	}
	for (auto &part : StringUtil::Split(path, '/')) {
		if (part.empty() || part == "." || part == "..") {
			throw InvalidInputException("Invalid local Vortex path component");
		}
	}
}

struct Descriptor {
	explicit Descriptor(int32_t fd_p) : fd(fd_p) {
	}
	~Descriptor() {
		close(fd);
	}
	int32_t fd;
};

inline unique_ptr<Descriptor> OpenDirectory(int32_t parent, const string &name, bool missing) {
	auto fd = openat(parent, name.c_str(), O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
	if (fd < 0) {
		if (missing && errno == ENOENT) {
			return nullptr;
		}
		throw IOException("Cannot open registered Vortex directory without following links: %s", name);
	}
	return make_uniq<Descriptor>(fd);
}

inline unique_ptr<Descriptor> OpenRoot(const string &path, bool missing) {
	if (path.empty() || path[0] != '/' || path.find('\0') != string::npos) {
		throw InvalidInputException("Invalid registered Vortex root");
	}
	auto result = OpenDirectory(AT_FDCWD, "/", false);
	for (auto &part : StringUtil::Split(path, '/')) {
		if (!part.empty()) {
			CheckRelative(part, true);
			result = OpenDirectory(result->fd, part, missing);
			if (!result) {
				return nullptr;
			}
		}
	}
	return result;
}

struct Parent {
	unique_ptr<Descriptor> directory;
	string name;
};

inline Parent OpenParent(int32_t root, const string &path, bool missing) {
	CheckRelative(path, false);
	auto parts = StringUtil::Split(path, '/');
	auto fd = dup(root);
	if (fd < 0) {
		throw IOException("Cannot retain registered Vortex directory");
	}
	auto directory = make_uniq<Descriptor>(fd);
	for (idx_t i = 0; i + 1 < parts.size(); i++) {
		directory = OpenDirectory(directory->fd, parts[i], missing);
		if (!directory) {
			break;
		}
	}
	return {std::move(directory), parts.back()};
}

inline void SyncDirectory(int32_t fd) {
	if (fsync(fd)) {
		throw IOException("Cannot sync Vortex collection directory");
	}
}
} // namespace vortex_detail
} // namespace duckdb
#endif
