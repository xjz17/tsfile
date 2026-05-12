/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "zstd_compressor.h"

#include "common/allocator/alloc_base.h"

using namespace common;

namespace storage {

int ZSTDCompressor::reset(bool for_compress) {
    UNUSED(for_compress);
    return E_OK;
}

void ZSTDCompressor::destroy() {
    if (compressed_buf_ != nullptr) {
        mem_free(compressed_buf_);
        compressed_buf_ = nullptr;
    }
    if (uncompressed_buf_ != nullptr) {
        mem_free(uncompressed_buf_);
        uncompressed_buf_ = nullptr;
    }
}

int ZSTDCompressor::compress(char* uncompressed_buf, uint32_t uncompressed_buf_len,
                             char*& compressed_buf,
                             uint32_t& compressed_buf_len) {
    const size_t dst_bound = ZSTD_compressBound(uncompressed_buf_len);
    compressed_buf_ = (char*)mem_alloc(dst_bound, MOD_COMPRESSOR_OBJ);
    if (compressed_buf_ == nullptr) {
        return E_OOM;
    }

    const size_t compressed_size =
        ZSTD_compress(compressed_buf_, dst_bound, uncompressed_buf,
                      uncompressed_buf_len, 3 /* default level */);
    if (ZSTD_isError(compressed_size)) {
        mem_free(compressed_buf_);
        compressed_buf_ = nullptr;
        return E_COMPRESS_ERR;
    }

    char* shrink = (char*)mem_realloc(compressed_buf_, compressed_size);
    if (shrink == nullptr) {
        mem_free(compressed_buf_);
        compressed_buf_ = nullptr;
        return E_OOM;
    }

    compressed_buf_ = shrink;
    compressed_buf = compressed_buf_;
    compressed_buf_len = static_cast<uint32_t>(compressed_size);
    return E_OK;
}

void ZSTDCompressor::after_compress(char* compressed_buf) {
    UNUSED(compressed_buf);
    if (compressed_buf_ != nullptr) {
        mem_free(compressed_buf_);
        compressed_buf_ = nullptr;
    }
}

int ZSTDCompressor::uncompress(char* compressed_buf, uint32_t compressed_buf_len,
                               char*& uncompressed_buf,
                               uint32_t& uncompressed_buf_len) {
    const unsigned long long frame_size =
        ZSTD_getFrameContentSize(compressed_buf, compressed_buf_len);
    if (frame_size == ZSTD_CONTENTSIZE_ERROR ||
        frame_size == ZSTD_CONTENTSIZE_UNKNOWN || frame_size > UINT32_MAX) {
        return E_COMPRESS_ERR;
    }

    uncompressed_buf_ =
        (char*)mem_alloc(static_cast<size_t>(frame_size), MOD_COMPRESSOR_OBJ);
    if (uncompressed_buf_ == nullptr) {
        return E_OOM;
    }

    const size_t decoded_size =
        ZSTD_decompress(uncompressed_buf_, static_cast<size_t>(frame_size),
                        compressed_buf, compressed_buf_len);
    if (ZSTD_isError(decoded_size) || decoded_size != frame_size) {
        mem_free(uncompressed_buf_);
        uncompressed_buf_ = nullptr;
        return E_COMPRESS_ERR;
    }

    uncompressed_buf = uncompressed_buf_;
    uncompressed_buf_len = static_cast<uint32_t>(decoded_size);
    return E_OK;
}

void ZSTDCompressor::after_uncompress(char* uncompressed_buf) {
    UNUSED(uncompressed_buf);
    if (uncompressed_buf_ != nullptr) {
        mem_free(uncompressed_buf_);
        uncompressed_buf_ = nullptr;
    }
}

}  // namespace storage
