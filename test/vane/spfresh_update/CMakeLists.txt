cmake_minimum_required(VERSION 3.20)
project(ducklake_spfresh_update_probe LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
find_package(OpenMP REQUIRED)
find_package(Threads REQUIRED)
find_package(TBB REQUIRED)
find_library(ROCKSDB_LIBRARY NAMES rocksdb REQUIRED)
find_path(ROCKSDB_INCLUDE_DIR rocksdb/db.h REQUIRED)
find_library(ZSTD_LIBRARY NAMES zstd REQUIRED)
find_path(ZSTD_INCLUDE_DIR zstd.h REQUIRED)

file(GLOB_RECURSE core CONFIGURE_DEPENDS
     "${SPFRESH_SOURCE}/AnnService/src/Core/*.cpp"
     "${SPFRESH_SOURCE}/AnnService/src/Helper/*.cpp")
list(FILTER core EXCLUDE REGEX "ExtraSPDKController\\.cpp$")
add_library(spfresh_update_core STATIC ${core})
target_compile_definitions(spfresh_update_core
                           PUBLIC ROCKSDB SPTAG_ROCKSDB_ONLY SPTAG_DISABLE_NUMA)
target_include_directories(
  spfresh_update_core PUBLIC "${SPFRESH_SOURCE}/AnnService"
                             "${ROCKSDB_INCLUDE_DIR}" "${ZSTD_INCLUDE_DIR}")
target_link_libraries(
  spfresh_update_core PUBLIC OpenMP::OpenMP_CXX Threads::Threads TBB::tbb
                             "${ROCKSDB_LIBRARY}" "${ZSTD_LIBRARY}")
add_executable(ducklake_spfresh_update_probe probe.cpp)
target_link_libraries(ducklake_spfresh_update_probe PRIVATE spfresh_update_core)
target_link_options(ducklake_spfresh_update_probe PRIVATE
                    "LINKER:--disable-new-dtags")
