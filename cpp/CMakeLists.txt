#[[
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
]]
cmake_minimum_required(VERSION 3.11)
project(TsFile_CPP)

if (DEFINED ToolChain)
    include(${CMAKE_SOURCE_DIR}/cmake/ToolChain.cmake)
    message(STATUS "Using ToolChain: ${CMAKE_TOOLCHAIN_FILE}")
else()
    message(STATUS "Not using ToolChain")
endif ()

cmake_policy(SET CMP0079 NEW)
set(TsFile_CPP_VERSION 2.2.1.dev)
set(CMAKE_CXX_FLAGS "$ENV{CXXFLAGS} -Wall")

if (CMAKE_CXX_COMPILER_ID MATCHES "GNU")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wunused -Wuninitialized -D__STDC_FORMAT_MACROS")
endif ()

message("cmake using: USE_CPP11=${USE_CPP11}")
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

if (DEFINED ENV{CXX})
    set(CMAKE_CXX_COMPILER $ENV{CXX})
    message("cmake using: CXX=${CMAKE_CXX_COMPILER}")
endif ()

if (DEFINED ENV{CC})
    set(CMAKE_C_COMPILER $ENV{CC})
    message("cmake using: CC=${CMAKE_C_COMPILER}")
endif ()

message("cmake using: DEBUG_SE=${DEBUG_SE}")
if (${DEBUG_SE})
    add_definitions(-DDEBUG_SE=1)
    message("add_definitions -DDEBUG_SE=1")
endif ()

if (${COV_ENABLED})
    add_definitions(-DCOV_ENABLED=1)
    message("add_definitions -DCOV_ENABLED=1")
endif ()

option(ENABLE_MEM_STAT "Enable memory status" ON)

if (ENABLE_MEM_STAT)
    add_definitions(-DENABLE_MEM_STAT)
    message("add_definitions -DENABLE_MEM_STAT")
endif ()


if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Release" CACHE STRING "Choose the type of build." FORCE)
endif ()

if (NOT DEFINED CMAKE_BUILD_PARALLEL_LEVEL)
    include(ProcessorCount)
    ProcessorCount(N)
    if (N EQUAL 0)
        set(N 1)
    endif ()
    set(CMAKE_BUILD_PARALLEL_LEVEL ${N} CACHE STRING "Number of parallel build jobs")
    message("CMAKE BUILD PARALLEL LEVEL: ${CMAKE_BUILD_PARALLEL_LEVEL} (auto-detected)")
else ()
    message("CMAKE BUILD PARALLEL LEVEL: ${CMAKE_BUILD_PARALLEL_LEVEL} (from environment)")
endif ()

message("CMAKE BUILD TYPE " ${CMAKE_BUILD_TYPE})
if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -g")
elseif (CMAKE_BUILD_TYPE STREQUAL "Release")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O2")
elseif (CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo")
    set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -O2 -g")
elseif (CMAKE_BUILD_TYPE STREQUAL "MinSizeRel")
    set(CMAKE_CXX_FLAGS_MINSIZEREL "${CMAKE_CXX_FLAGS_MINSIZEREL} -ffunction-sections -fdata-sections -Os")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,--gc-sections")
endif ()
message("CMAKE DEBUG: CMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}")

# disable asan by default.
option(ENABLE_ASAN "Enable Address Sanitizer" OFF)

if (NOT WIN32)
    if (ENABLE_ASAN)
        message("Address Sanitizer is enabled.")

        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address,undefined -fno-omit-frame-pointer")

        if (NOT APPLE)
            set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -static-libasan")
            set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address,undefined -static-libasan -static-libubsan")
        else ()
            set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address,undefined")
        endif ()
    else ()
        message("Address Sanitizer is disabled.")
    endif ()
endif ()

option(BUILD_TEST "Build tests" ON)
message("cmake using: BUILD_TEST=${BUILD_TEST}")

option(ENABLE_ANTLR4 "Enable ANTLR4 runtime" ON)
message("cmake using: ENABLE_ANTLR4=${ENABLE_ANTLR4}")

option(ENABLE_SNAPPY "Enable Google Snappy compression" ON)
message("cmake using: ENABLE_SNAPPY=${ENABLE_SNAPPY}")

if (ENABLE_SNAPPY)
    add_definitions(-DENABLE_SNAPPY)
endif()

option(ENABLE_LZ4 "Enable LZ4 compression" ON)
message("cmake using: ENABLE_LZ4=${ENABLE_LZ4}")

if (ENABLE_LZ4)
    add_definitions(-DENABLE_LZ4)
endif()

option(ENABLE_LZOKAY "Enable LZOKAY compression" ON)
message("cmake using: ENABLE_LZOKAY=${ENABLE_LZOKAY}")

if (ENABLE_LZOKAY)
    add_definitions(-DENABLE_LZOKAY)
endif()

option(ENABLE_ZLIB "Enable Zlib compression" ON)
message("cmake using: ENABLE_ZLIB=${ENABLE_ZLIB}")

if (ENABLE_ZLIB)
    add_definitions(-DENABLE_ZLIB)
    add_definitions(-DENABLE_GZIP)
endif()

# Default Homebrew locations on Apple Silicon (/opt/homebrew) and Intel Mac (/usr/local);
# improves detection of keg-only headers/libs compared to plain check_include_file_cxx.
set(TSFILE_HOMEBREW_HINT_PREFIXES "")
if(APPLE)
    foreach(_hb_prefix "/opt/homebrew" "/usr/local")
        if(EXISTS "${_hb_prefix}")
            list(APPEND CMAKE_PREFIX_PATH "${_hb_prefix}")
            list(APPEND TSFILE_HOMEBREW_HINT_PREFIXES "${_hb_prefix}")
        endif()
    endforeach()
endif()

# Extra search roots for ZSTD / LZMA (Windows + MinGW: Scoop alone has no dev headers;
# MSYS2 exports MINGW_PREFIX, or use typical install paths).
set(TSFILE_ZSTD_LZMA_HINT_PREFIXES "${TSFILE_HOMEBREW_HINT_PREFIXES}")
if(WIN32)
    if(DEFINED ENV{MINGW_PREFIX} AND NOT "$ENV{MINGW_PREFIX}" STREQUAL "")
        list(APPEND TSFILE_ZSTD_LZMA_HINT_PREFIXES "$ENV{MINGW_PREFIX}")
    endif()
    foreach(_mingw_root "C:/msys64/mingw64" "C:/msys64/ucrt64" "C:/msys64/clang64")
        if(EXISTS "${_mingw_root}")
            list(APPEND TSFILE_ZSTD_LZMA_HINT_PREFIXES "${_mingw_root}")
        endif()
    endforeach()
endif()

option(ENABLE_ZSTD "Enable ZSTD compression" ON)
message("cmake using: ENABLE_ZSTD=${ENABLE_ZSTD}")
if (ENABLE_ZSTD)
    find_path(ZSTD_INCLUDE_DIR NAMES zstd.h HINTS ${TSFILE_ZSTD_LZMA_HINT_PREFIXES} PATH_SUFFIXES include)
    find_library(ZSTD_LIBRARY NAMES zstd libzstd
            HINTS ${TSFILE_ZSTD_LZMA_HINT_PREFIXES} PATH_SUFFIXES lib)
    if (ZSTD_INCLUDE_DIR AND ZSTD_LIBRARY)
        add_definitions(-DENABLE_ZSTD)
        include_directories(${ZSTD_INCLUDE_DIR})
    else()
        message(WARNING "ENABLE_ZSTD=ON but zstd headers/library not found; disabling ZSTD")
        set(ENABLE_ZSTD OFF CACHE BOOL "Enable ZSTD compression" FORCE)
        unset(ZSTD_LIBRARY CACHE)
        unset(ZSTD_INCLUDE_DIR CACHE)
    endif()
endif()

option(ENABLE_LZMA "Enable LZMA compression" ON)
message("cmake using: ENABLE_LZMA=${ENABLE_LZMA}")
if (ENABLE_LZMA)
    find_path(LZMA_INCLUDE_DIR NAMES lzma.h HINTS ${TSFILE_ZSTD_LZMA_HINT_PREFIXES} PATH_SUFFIXES include)
    find_library(LZMA_LIBRARY NAMES lzma liblzma
            HINTS ${TSFILE_ZSTD_LZMA_HINT_PREFIXES} PATH_SUFFIXES lib)
    if (LZMA_INCLUDE_DIR AND LZMA_LIBRARY)
        add_definitions(-DENABLE_LZMA)
        include_directories(${LZMA_INCLUDE_DIR})
    else()
        message(WARNING "ENABLE_LZMA=ON but lzma headers/library not found; disabling LZMA")
        set(ENABLE_LZMA OFF CACHE BOOL "Enable LZMA compression" FORCE)
        unset(LZMA_LIBRARY CACHE)
        unset(LZMA_INCLUDE_DIR CACHE)
    endif()
endif()

option(ENABLE_THREADS "Enable multi-threaded read/write (requires pthreads)" ON)
message("cmake using: ENABLE_THREADS=${ENABLE_THREADS}")

if (ENABLE_THREADS)
    add_definitions(-DENABLE_THREADS)
    find_package(Threads REQUIRED)
    link_libraries(Threads::Threads)
endif()

option(ENABLE_SIMDE "Enable SIMDe (SIMD Everywhere)" OFF)
message("cmake using: ENABLE_SIMDE=${ENABLE_SIMDE}")

if (ENABLE_SIMDE)
    add_definitions(-DENABLE_SIMDE)
endif()

# All libs will be stored here, including libtsfile, compress-encoding lib.
set(LIBRARY_OUTPUT_PATH ${PROJECT_BINARY_DIR}/lib)

# TsFile code will be stored here.
set(PROJECT_SRC_DIR ${PROJECT_SOURCE_DIR}/src)

# All include files will be installed here.
# Use global var so that tests may also use this
set(LIBRARY_INCLUDE_DIR ${PROJECT_BINARY_DIR}/include CACHE STRING "TsFile includes")
set(THIRD_PARTY_INCLUDE ${PROJECT_BINARY_DIR}/third_party)

set(SAVED_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
set(CMAKE_CXX_FLAGS "$ENV{CXXFLAGS} -Wall -std=c++11")
add_subdirectory(third_party)
set(CMAKE_CXX_FLAGS "${SAVED_CXX_FLAGS}")

add_subdirectory(src)
if (BUILD_TEST)
    add_subdirectory(test)
    if (TESTS_ENABLED)
        add_dependencies(TsFile_Test tsfile)
    endif ()
else()
    message("BUILD_TEST is OFF, skipping test directory")
endif ()

add_subdirectory(examples)

