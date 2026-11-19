foreach(_DUCKLAKE_VANE_PROVIDER IN ITEMS vortex ducklake)
  if(NOT _DUCKLAKE_VANE_PROVIDER IN_LIST VANE_LOADABLE_EXTENSIONS)
    message(FATAL_ERROR "Vane native Vortex requires VANE_LOADABLE_EXTENSIONS=vortex;ducklake")
  endif()
endforeach()
set(DUCKLAKE_VANE_DISTRIBUTED OFF CACHE BOOL
    "Use the native DuckLake transaction and query path" FORCE)
set(DUCKLAKE_VANE_NATIVE ON CACHE BOOL
    "Use the explicitly pinned Vane engine interface" FORCE)
set(DUCKLAKE_ENABLE_VORTEX ON CACHE BOOL
    "Build native Vortex storage and static ANN search" FORCE)
if(NOT VORTEX_SPFRESH_NATIVE)
  set(VORTEX_SPFRESH_NATIVE "$ENV{VORTEX_SPFRESH_NATIVE}" CACHE PATH
      "Qualified self-contained native SPFresh SDK")
endif()
if(NOT IS_ABSOLUTE "${VORTEX_SPFRESH_NATIVE}" OR
   NOT EXISTS "${VORTEX_SPFRESH_NATIVE}/vane-openmp-library.txt")
  message(FATAL_ERROR "Vane native Vortex requires scripts/build_vane_vortex_native_sdk.sh output")
endif()
file(STRINGS "${VORTEX_SPFRESH_NATIVE}/vane-openmp-library.txt"
     _DUCKLAKE_VANE_OPENMP LIMIT_COUNT 1)
file(STRINGS "${VORTEX_SPFRESH_NATIVE}/zstd-library.txt"
     _DUCKLAKE_VANE_ZSTD LIMIT_COUNT 1)
foreach(_DUCKLAKE_VANE_LIBRARY IN ITEMS "${_DUCKLAKE_VANE_OPENMP}" "${_DUCKLAKE_VANE_ZSTD}")
  if(NOT IS_ABSOLUTE "${_DUCKLAKE_VANE_LIBRARY}" OR
     NOT _DUCKLAKE_VANE_LIBRARY MATCHES "\\.a$" OR NOT EXISTS "${_DUCKLAKE_VANE_LIBRARY}")
    message(FATAL_ERROR "Vane native provider dependencies must be existing absolute PIC static archives")
  endif()
endforeach()
set(OpenMP_CXX_LIB_NAMES omp CACHE STRING "Self-contained OpenMP runtime" FORCE)
set(OpenMP_omp_LIBRARY "${_DUCKLAKE_VANE_OPENMP}" CACHE FILEPATH
    "Qualified PIC OpenMP runtime" FORCE)
include(${CMAKE_CURRENT_LIST_DIR}/extension_config.cmake)

get_filename_component(_DUCKLAKE_VANE_SOURCE "${DUCKDB_EXTENSION_DUCKLAKE_PATH}" REALPATH)
get_filename_component(_DUCKLAKE_CONFIG_SOURCE "${CMAKE_CURRENT_LIST_DIR}" REALPATH)
if(NOT _DUCKLAKE_VANE_SOURCE STREQUAL _DUCKLAKE_CONFIG_SOURCE)
  message(FATAL_ERROR "Vane must register DuckLake from the checkout containing this config")
endif()
execute_process(
  COMMAND git -C "${DUCKDB_EXTENSION_VORTEX_PATH}" rev-parse HEAD
  RESULT_VARIABLE _DUCKLAKE_VANE_VORTEX_RESULT
  OUTPUT_VARIABLE _DUCKLAKE_VANE_VORTEX_HEAD
  OUTPUT_STRIP_TRAILING_WHITESPACE
)
if(NOT _DUCKLAKE_VANE_VORTEX_RESULT EQUAL 0 OR
   NOT _DUCKLAKE_VANE_VORTEX_HEAD STREQUAL DUCKLAKE_DUCKDB_VORTEX_REVISION)
  message(FATAL_ERROR "Vane must register duckdb-vortex at ${DUCKLAKE_DUCKDB_VORTEX_REVISION}")
endif()
execute_process(
  COMMAND git -C "${DUCKDB_EXTENSION_VORTEX_PATH}" diff --quiet HEAD
  RESULT_VARIABLE _DUCKLAKE_VANE_VORTEX_DIRTY
)
if(NOT _DUCKLAKE_VANE_VORTEX_DIRTY EQUAL 0)
  message(FATAL_ERROR "Vane native Vortex requires a clean pinned duckdb-vortex checkout")
endif()

file(STRINGS "${CMAKE_CURRENT_LIST_DIR}/src/vortex_query/Cargo.toml"
     _DUCKLAKE_VORTEX_CRATE REGEX "^vortex-index =")
string(REGEX MATCH "rev = \"([0-9a-f]+)\"" _DUCKLAKE_VORTEX_PIN_MATCH "${_DUCKLAKE_VORTEX_CRATE}")
set(_DUCKLAKE_VORTEX_CRATE_REVISION "${CMAKE_MATCH_1}")
file(STRINGS "${VORTEX_SPFRESH_NATIVE}/vane-vortex-revision.txt"
     _DUCKLAKE_SDK_VORTEX_REVISION LIMIT_COUNT 1)
if(NOT _DUCKLAKE_VORTEX_CRATE_REVISION OR
   NOT _DUCKLAKE_SDK_VORTEX_REVISION STREQUAL _DUCKLAKE_VORTEX_CRATE_REVISION)
  message(FATAL_ERROR "SPFresh SDK and DuckLake query crate require the same pinned Vortex source")
endif()

set(_DUCKLAKE_VANE_NATIVE_EXTENSIONS)
foreach(_DUCKLAKE_EXTENSION_NAME IN LISTS DUCKDB_EXTENSION_NAMES)
  string(TOUPPER "${_DUCKLAKE_EXTENSION_NAME}" _DUCKLAKE_EXTENSION_UPPER)
  if(DUCKDB_EXTENSION_${_DUCKLAKE_EXTENSION_UPPER}_SHOULD_LINK)
    list(APPEND _DUCKLAKE_VANE_NATIVE_EXTENSIONS "${_DUCKLAKE_EXTENSION_NAME}")
  endif()
endforeach()
set(BUILD_EXTENSIONS "${_DUCKLAKE_VANE_NATIVE_EXTENSIONS}" PARENT_SCOPE)
