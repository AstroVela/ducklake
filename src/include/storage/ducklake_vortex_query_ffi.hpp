#pragma once

#include <cstdint>

extern "C" {
struct DuckLakeVortexQuerySource {
	// Bytes live until source_pinned or return; descriptors live until return.
	uint64_t file_id;
	const uint8_t *data;
	uint64_t size;
};
struct DuckLakeVortexQueryInput {
	const uint8_t *admission;
	uint64_t admission_len;
	const DuckLakeVortexQuerySource *sources;
	uint64_t source_count;
	const uint8_t *data_path;
	uint64_t data_path_len;
	const uint8_t *index_name;
	uint64_t index_name_len;
	const float *query;
	uint64_t dimension;
	uint64_t k;
	const uint8_t *category;
	uint64_t category_len;
	uint8_t filter_category;
	uint8_t (*control)(void *, const uint8_t *, uint64_t);
	void *context;
};
struct DuckLakeVortexQueryRow {
	int64_t id;
	float distance;
	const float *embedding;
	uint64_t dimension;
	const uint8_t *category;
	uint64_t category_len;
	const uint8_t *payload;
	uint64_t payload_len;
	uint8_t id_null;
	uint8_t category_null;
	uint8_t payload_null;
};
struct DuckLakeVortexQueryOutput;
struct DuckLakeVortexBuildInput {
	const uint8_t *admission;
	uint64_t admission_len;
	const DuckLakeVortexQuerySource *sources;
	uint64_t source_count;
	const uint8_t *data_path;
	uint64_t data_path_len;
	const uint8_t *reference_path;
	uint64_t reference_path_len;
	const uint8_t *generation;
	uint64_t generation_len;
	const uint8_t *workspace;
	uint64_t workspace_len;
	uint8_t (*control)(void *, const uint8_t *, uint64_t);
	void *context;
};
// NULL means success; an owned error must be freed with build_error_free.
char *ducklake_vortex_build(const DuckLakeVortexBuildInput *input);
void ducklake_vortex_build_error_free(char *error);
DuckLakeVortexQueryOutput *ducklake_vortex_query(const DuckLakeVortexQueryInput *input);
const char *ducklake_vortex_query_error(const DuckLakeVortexQueryOutput *output);
const char *ducklake_vortex_query_diagnostics(const DuckLakeVortexQueryOutput *output);
uint64_t ducklake_vortex_query_count(const DuckLakeVortexQueryOutput *output);
uint8_t ducklake_vortex_query_row(const DuckLakeVortexQueryOutput *output, uint64_t index, DuckLakeVortexQueryRow *row);
void ducklake_vortex_query_free(DuckLakeVortexQueryOutput *output);
}
