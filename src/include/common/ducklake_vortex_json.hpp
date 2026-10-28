#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/value.hpp"
#include "yyjson.hpp"
#include <memory>

namespace duckdb {
namespace vortex_detail {
using namespace duckdb_yyjson;

struct ViewJSON {
	ViewJSON() : doc(yyjson_mut_doc_new(nullptr), yyjson_mut_doc_free) {
		if (!doc) {
			throw OutOfMemoryException("Cannot allocate Vortex read view JSON");
		}
	}

	yyjson_mut_val *Object() {
		return yyjson_mut_obj(doc.get());
	}
	yyjson_mut_val *Array() {
		return yyjson_mut_arr(doc.get());
	}
	void Add(yyjson_mut_val *object, const char *key, yyjson_mut_val *value) {
		yyjson_mut_obj_add_val(doc.get(), object, key, value);
	}
	void Number(yyjson_mut_val *object, const char *key, idx_t value) {
		Add(object, key, yyjson_mut_uint(doc.get(), value));
	}
	void Text(yyjson_mut_val *object, const char *key, const string &value) {
		Add(object, key, yyjson_mut_strncpy(doc.get(), value.data(), value.size()));
	}
	void NullableText(yyjson_mut_val *object, const char *key, const Value &value) {
		if (value.IsNull()) {
			Add(object, key, yyjson_mut_null(doc.get()));
		} else {
			Text(object, key, StringValue::Get(value));
		}
	}
	string Finish(yyjson_mut_val *root) {
		yyjson_mut_doc_set_root(doc.get(), root);
		size_t length = 0;
		std::unique_ptr<char, decltype(&free)> text(yyjson_mut_write(doc.get(), 0, &length), free);
		if (!text) {
			throw OutOfMemoryException("Cannot serialize Vortex read view");
		}
		return string(text.get(), length);
	}
	std::unique_ptr<yyjson_mut_doc, decltype(&yyjson_mut_doc_free)> doc;
};

inline string Hex(const string &bytes) {
	static const char digits[] = "0123456789abcdef";
	string result(bytes.size() * 2, '0');
	for (idx_t i = 0; i < bytes.size(); i++) {
		auto value = static_cast<uint8_t>(bytes[i]);
		result[i * 2] = digits[value >> 4];
		result[i * 2 + 1] = digits[value & 15];
	}
	return result;
}

} // namespace vortex_detail
} // namespace duckdb
