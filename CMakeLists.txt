cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME ducklake)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

option(DUCKLAKE_VANE_DISTRIBUTED
       "Build the explicit Vane distributed DuckLake scan interface" OFF)
if(DUCKLAKE_VANE_DISTRIBUTED)
	if(NOT EXISTS "${DUCKDB_SOURCE_PATH}/src/include/duckdb/function/distributed_table_function.hpp")
		message(FATAL_ERROR "DUCKLAKE_VANE_DISTRIBUTED requires the Vane DuckDB source tree")
	endif()
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
