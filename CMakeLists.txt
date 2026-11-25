cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME ducklake)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

option(DUCKLAKE_ENABLE_VORTEX "Enable experimental native Vortex data files" OFF)
if(DUCKLAKE_ENABLE_VORTEX)
  if(DUCKLAKE_VANE_DISTRIBUTED)
    message(FATAL_ERROR "Experimental DuckLake Vortex files require native execution")
  endif()
  add_definitions(-DDUCKLAKE_ENABLE_VORTEX)
  if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux" OR NOT CMAKE_SYSTEM_PROCESSOR MATCHES "^(x86_64|amd64|AMD64)$")
    message(FATAL_ERROR "Native DuckLake SPFresh search requires Linux x86_64")
  endif()
  set(VORTEX_SPFRESH_NATIVE "$ENV{VORTEX_SPFRESH_NATIVE}" CACHE PATH "Qualified SPFresh native build directory")
  if(NOT IS_ABSOLUTE "${VORTEX_SPFRESH_NATIVE}" OR NOT EXISTS "${VORTEX_SPFRESH_NATIVE}/libspfresh_core.a"
     OR NOT EXISTS "${VORTEX_SPFRESH_NATIVE}/libspfresh_distance.a")
    message(FATAL_ERROR "DUCKLAKE_ENABLE_VORTEX requires a qualified VORTEX_SPFRESH_NATIVE build")
  endif()
  find_package(OpenMP REQUIRED COMPONENTS CXX)
  file(STRINGS "${VORTEX_SPFRESH_NATIVE}/zstd-library.txt" DUCKLAKE_SPFRESH_ZSTD LIMIT_COUNT 1)
  if(NOT IS_ABSOLUTE "${DUCKLAKE_SPFRESH_ZSTD}" OR NOT EXISTS "${DUCKLAKE_SPFRESH_ZSTD}")
    message(FATAL_ERROR "SPFresh qualification must identify an existing zstd library")
  endif()
  corrosion_import_crate(
    MANIFEST_PATH "${CMAKE_CURRENT_SOURCE_DIR}/src/vortex_query/Cargo.toml"
    CRATES ducklake-vortex-query CRATE_TYPES staticlib FLAGS --locked PROFILE ci)
  corrosion_set_env_vars(ducklake_vortex_query "VORTEX_SPFRESH_NATIVE=${VORTEX_SPFRESH_NATIVE}")
  # Keep Rust runtime objects separate so both static archives share one copy.
  corrosion_set_cargo_flags(vortex_duckdb --locked --config=profile.release-lto.lto=false)
endif()

option(DUCKLAKE_VANE_DISTRIBUTED
       "Build the explicit Vane distributed DuckLake scan interface" OFF)
option(DUCKLAKE_VANE_NATIVE "Build against Vane's native DuckDB engine interface" OFF)
if(DUCKLAKE_VANE_NATIVE OR DUCKLAKE_VANE_DISTRIBUTED)
	if(NOT EXISTS "${DUCKDB_SOURCE_PATH}/src/include/duckdb/function/distributed_table_function.hpp")
		message(FATAL_ERROR "The Vane DuckLake interfaces require the Vane DuckDB source tree")
	endif()
	add_definitions(-DDUCKLAKE_VANE_ENGINE)
endif()
if(DUCKLAKE_VANE_DISTRIBUTED)
	add_definitions(-DDUCKLAKE_VANE_DISTRIBUTED)
endif()

# Roaring is installed via vcpkg and used for deletion vectors
find_package(roaring CONFIG REQUIRED)
if(TARGET roaring::roaring-headers)
  get_target_property(ROARING_INCLUDE_DIRS roaring::roaring-headers INTERFACE_INCLUDE_DIRECTORIES)
else()
  get_target_property(ROARING_INCLUDE_DIRS roaring::roaring INTERFACE_INCLUDE_DIRECTORIES)
endif()
if(NOT ROARING_INCLUDE_DIRS OR ROARING_INCLUDE_DIRS STREQUAL "ROARING_INCLUDE_DIRS-NOTFOUND")
  set(ROARING_INCLUDE_DIRS "${roaring_DIR}/../../include")
endif()
include_directories(${ROARING_INCLUDE_DIRS})

add_subdirectory(src)
set(EXTENSION_SOURCES ${ALL_OBJECT_FILES})

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

if(DUCKLAKE_ENABLE_VORTEX)
  target_link_libraries(${EXTENSION_NAME} ducklake_vortex_query-static OpenMP::OpenMP_CXX "${DUCKLAKE_SPFRESH_ZSTD}")
  target_link_libraries(${LOADABLE_EXTENSION_NAME} ducklake_vortex_query-static OpenMP::OpenMP_CXX "${DUCKLAKE_SPFRESH_ZSTD}")
endif()

# Distributed scans call Parquet's C++ interface, which Vane does not export.
if(DUCKLAKE_VANE_DISTRIBUTED AND EXTENSION_STATIC_BUILD)
  if(NOT TARGET parquet_extension)
    message(FATAL_ERROR "The self-contained DuckLake extension requires parquet_extension")
  endif()
  target_link_libraries(${LOADABLE_EXTENSION_NAME} parquet_extension)
endif()

if(TARGET roaring::roaring-headers)
  target_link_libraries(${EXTENSION_NAME} roaring::roaring roaring::roaring-headers roaring::roaring-headers-cpp)
  target_link_libraries(${TARGET_NAME}_loadable_extension roaring::roaring roaring::roaring-headers roaring::roaring-headers-cpp)
else()
  target_link_libraries(${EXTENSION_NAME} roaring::roaring)
  target_link_libraries(${TARGET_NAME}_loadable_extension roaring::roaring)
endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

if(DUCKLAKE_ENABLE_VORTEX)
  add_executable(ducklake_vortex_write_driver EXCLUDE_FROM_ALL test/vane/native_vortex_write_driver.cpp)
  get_target_property(DUCKLAKE_VORTEX_SOURCE vortex_extension SOURCE_DIR)
  target_include_directories(ducklake_vortex_write_driver PRIVATE "${DUCKLAKE_VORTEX_SOURCE}/src/include")
  target_link_libraries(ducklake_vortex_write_driver ducklake_extension vortex_extension duckdb_static ${DUCKDB_EXTRA_LINK_FLAGS})
  link_threads(ducklake_vortex_write_driver "")
  link_extension_libraries(ducklake_vortex_write_driver "")
  set_target_properties(ducklake_vortex_write_driver PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}")
  add_executable(ducklake_vortex_source_driver EXCLUDE_FROM_ALL test/vane/native_vortex_source_driver.cpp)
  target_include_directories(ducklake_vortex_source_driver PRIVATE "${DUCKLAKE_VORTEX_SOURCE}/src/include")
  target_link_libraries(ducklake_vortex_source_driver ducklake_extension vortex_extension duckdb_static ${DUCKDB_EXTRA_LINK_FLAGS})
  link_threads(ducklake_vortex_source_driver "")
  link_extension_libraries(ducklake_vortex_source_driver "")
  set_target_properties(ducklake_vortex_source_driver PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}")
endif()

if(DUCKLAKE_ENABLE_VORTEX AND BUILD_UNITTESTS)
  add_executable(ducklake_vortex_view_driver EXCLUDE_FROM_ALL
                 test/vane/native_vortex_view_driver.cpp)
  target_link_libraries(ducklake_vortex_view_driver duckdb_static ${DUCKDB_EXTRA_LINK_FLAGS})
  link_threads(ducklake_vortex_view_driver "")
  link_extension_libraries(ducklake_vortex_view_driver "")
  set_target_properties(ducklake_vortex_view_driver PROPERTIES
                        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}")
  add_executable(ducklake_vortex_catalog_driver EXCLUDE_FROM_ALL
                 test/vane/native_vortex_catalog_driver.cpp)
  target_link_libraries(ducklake_vortex_catalog_driver duckdb_static ${DUCKDB_EXTRA_LINK_FLAGS})
  link_threads(ducklake_vortex_catalog_driver "")
  link_extension_libraries(ducklake_vortex_catalog_driver "")
  set_target_properties(ducklake_vortex_catalog_driver PROPERTIES
                        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}")
  add_executable(ducklake_vortex_cost_driver EXCLUDE_FROM_ALL test/vane/native_vortex_cost_driver.cpp)
  target_link_libraries(ducklake_vortex_cost_driver duckdb_static ${DUCKDB_EXTRA_LINK_FLAGS})
  link_threads(ducklake_vortex_cost_driver "")
  link_extension_libraries(ducklake_vortex_cost_driver "")
  set_target_properties(ducklake_vortex_cost_driver PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}")
endif()
