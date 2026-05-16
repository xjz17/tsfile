#[[
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file to
you under the Apache License, Version 2.0.
]]
#
# Optional bundled zstd / liblzma for hosts without system dev packages (e.g. MinGW
# on Windows). Compatible with CMake 3.11 (parent project minimum):
#   - zstd: FetchContent_Populate + add_subdirectory(build/cmake)
#   - XZ Utils: ExternalProject (sub-build may require CMake >= 3.20; uses your cmake
#     executable, does not change TsFile's declared minimum).
#
# Default Git URLs are SSH; override TSFILE_*_GIT_REPOSITORY with HTTPS if needed.

include(FetchContent)
include(ExternalProject)

set(TSFILE_ZSTD_GIT_REPOSITORY "git@github.com:facebook/zstd.git"
        CACHE STRING "Git URL for bundled zstd")
set(TSFILE_ZSTD_GIT_TAG "v1.5.7"
        CACHE STRING "Git tag/commit for bundled zstd")
set(TSFILE_XZ_GIT_REPOSITORY "git@github.com:tukaani-project/xz.git"
        CACHE STRING "Git URL for bundled XZ Utils (liblzma)")
set(TSFILE_XZ_GIT_TAG "v5.6.4"
        CACHE STRING "Git tag/commit for bundled XZ Utils")

macro(tsfile_fetch_zstd)
    if(ENABLE_ZSTD AND TSFILE_USE_BUNDLED_ZSTD)
        FetchContent_Declare(tsfile_zstd
                GIT_REPOSITORY "${TSFILE_ZSTD_GIT_REPOSITORY}"
                GIT_TAG "${TSFILE_ZSTD_GIT_TAG}"
                GIT_SHALLOW TRUE)
        if(NOT TARGET libzstd_static)
            message(STATUS "TsFile: bundling zstd (${TSFILE_ZSTD_GIT_TAG})")
            set(ZSTD_BUILD_PROGRAMS OFF CACHE BOOL "" FORCE)
            set(ZSTD_BUILD_SHARED OFF CACHE BOOL "" FORCE)
            set(ZSTD_BUILD_STATIC ON CACHE BOOL "" FORCE)
            set(ZSTD_BUILD_TESTS OFF CACHE BOOL "" FORCE)
            set(ZSTD_BUILD_CONTRIB OFF CACHE BOOL "" FORCE)

            FetchContent_GetProperties(tsfile_zstd)
            if(NOT tsfile_zstd_POPULATED)
                FetchContent_Populate(tsfile_zstd)
            endif()
            add_subdirectory(${tsfile_zstd_SOURCE_DIR}/build/cmake
                    ${CMAKE_BINARY_DIR}/tsfile_zstd-build)
        endif()
        set(ZSTD_INCLUDE_DIR "${tsfile_zstd_SOURCE_DIR}/lib")
        set(ZSTD_LIBRARY libzstd_static)
    endif()
endmacro()

macro(tsfile_fetch_liblzma)
    if(ENABLE_LZMA AND TSFILE_USE_BUNDLED_LIBLZMA)
        set(TSFILE_XZ_INSTALL "${CMAKE_BINARY_DIR}/tsfile_bundled/xz-install")
        set(TSFILE_XZ_STAMP "${CMAKE_BINARY_DIR}/tsfile_bundled/xz-ext-prefix")

        if(MSVC)
            set(TSFILE_LIBLZMA_IMPORTED "${TSFILE_XZ_INSTALL}/lib/liblzma.lib")
        else()
            set(TSFILE_LIBLZMA_IMPORTED "${TSFILE_XZ_INSTALL}/lib/liblzma.a")
        endif()

        if(NOT TARGET tsfile_xz_ext)
            message(STATUS "TsFile: bundling liblzma via ExternalProject (${TSFILE_XZ_GIT_TAG})")
            if(CMAKE_BUILD_TYPE)
                set(_tsfile_xz_build_type "${CMAKE_BUILD_TYPE}")
            else()
                set(_tsfile_xz_build_type "Release")
            endif()
            ExternalProject_Add(tsfile_xz_ext
                    PREFIX "${TSFILE_XZ_STAMP}"
                    GIT_REPOSITORY "${TSFILE_XZ_GIT_REPOSITORY}"
                    GIT_TAG "${TSFILE_XZ_GIT_TAG}"
                    GIT_SHALLOW TRUE
                    CMAKE_GENERATOR "${CMAKE_GENERATOR}"
                    CMAKE_ARGS
                    -DCMAKE_INSTALL_PREFIX:PATH=${TSFILE_XZ_INSTALL}
                    -DCMAKE_BUILD_TYPE:STRING=${_tsfile_xz_build_type}
                    -DCMAKE_C_COMPILER:FILEPATH=${CMAKE_C_COMPILER}
                    -DBUILD_SHARED_LIBS:BOOL=OFF
                    -DXZ_TOOL_XZ:BOOL=OFF
                    -DXZ_TOOL_XZDEC:BOOL=OFF
                    -DXZ_TOOL_LZMADEC:BOOL=OFF
                    -DXZ_TOOL_LZMAINFO:BOOL=OFF
                    -DXZ_TOOL_SYMLINKS:BOOL=OFF
                    -DXZ_TOOL_SYMLINKS_LZMA:BOOL=OFF
                    -DXZ_TOOL_SCRIPTS:BOOL=OFF
                    -DXZ_NLS:BOOL=OFF
                    -DXZ_DOC:BOOL=OFF
                    BUILD_BYPRODUCTS "${TSFILE_LIBLZMA_IMPORTED}")
        endif()

        set(LZMA_INCLUDE_DIR "${TSFILE_XZ_INSTALL}/include")
        set(LZMA_LIBRARY "${TSFILE_LIBLZMA_IMPORTED}")
    endif()
endmacro()

# Object libraries that include compressor_factory.h need lzma.h from bundled install.
macro(tsfile_apply_bundled_lzma_dependencies)
    if(TARGET tsfile_xz_ext)
        foreach(_lzdep_obj IN ITEMS compress_obj read_obj write_obj file_obj)
            if(TARGET ${_lzdep_obj})
                add_dependencies(${_lzdep_obj} tsfile_xz_ext)
            endif()
        endforeach()
        if(TARGET tsfile)
            add_dependencies(tsfile tsfile_xz_ext)
        endif()
    endif()
endmacro()
