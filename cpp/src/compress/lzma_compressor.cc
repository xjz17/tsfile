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

#include "lzma_compressor.h"

#include "common/allocator/alloc_base.h"

using namespace common;

namespace storage {
namespace {
static const uint32_t LZMA_BUFFER_SIZE = 8192;
}

int LZMACompressor::reset(bool for_compress) {
    UNUSED(for_compress);
    return E_OK;
}

void LZMACompressor::destroy() {
    if (compressed_buf_ != nullptr) {
        mem_free(compressed_buf_);
        compressed_buf_ = nullptr;
    }
    if (uncompressed_buf_ != nullptr) {
        mem_free(uncompressed_buf_);
        uncompressed_buf_ = nullptr;
    }
}

int LZMACompressor::compress(char* uncompressed_buf, uint32_t uncompressed_buf_len,
                             char*& compressed_buf,
                             uint32_t& compressed_buf_len) {
    lzma_stream stream = LZMA_STREAM_INIT;
    lzma_ret lzma_ret_code =
        lzma_easy_encoder(&stream, 3, LZMA_CHECK_CRC64);
    if (lzma_ret_code != LZMA_OK) {
        return E_COMPRESS_ERR;
    }

    ByteStream out(LZMA_BUFFER_SIZE, MOD_COMPRESSOR_OBJ);
    uint8_t out_buffer[LZMA_BUFFER_SIZE];
    stream.next_in = reinterpret_cast<const uint8_t*>(uncompressed_buf);
    stream.avail_in = uncompressed_buf_len;

    int ret = E_OK;
    while (true) {
        stream.next_out = out_buffer;
        stream.avail_out = LZMA_BUFFER_SIZE;

        const lzma_action action = (stream.avail_in == 0) ? LZMA_FINISH : LZMA_RUN;
        lzma_ret_code = lzma_code(&stream, action);
        const size_t produced = LZMA_BUFFER_SIZE - stream.avail_out;
        if (produced > 0) {
            out.write_buf(reinterpret_cast<const char*>(out_buffer), produced);
        }

        if (lzma_ret_code == LZMA_STREAM_END) {
            break;
        }
        if (lzma_ret_code != LZMA_OK) {
            ret = E_COMPRESS_ERR;
            break;
        }
    }

    lzma_end(&stream);
    if (ret != E_OK) {
        out.destroy();
        return ret;
    }

    compressed_buf_ = get_bytes_from_bytestream(out);
    compressed_buf_len = out.total_size();
    compressed_buf = compressed_buf_;
    out.destroy();
    return E_OK;
}

void LZMACompressor::after_compress(char* compressed_buf) {
    UNUSED(compressed_buf);
    if (compressed_buf_ != nullptr) {
        mem_free(compressed_buf_);
        compressed_buf_ = nullptr;
    }
}

int LZMACompressor::uncompress(char* compressed_buf, uint32_t compressed_buf_len,
                               char*& uncompressed_buf,
                               uint32_t& uncompressed_buf_len) {
    lzma_stream stream = LZMA_STREAM_INIT;
    lzma_ret lzma_ret_code = lzma_stream_decoder(&stream, UINT64_MAX, 0);
    if (lzma_ret_code != LZMA_OK) {
        return E_COMPRESS_ERR;
    }

    ByteStream out(LZMA_BUFFER_SIZE, MOD_COMPRESSOR_OBJ);
    uint8_t out_buffer[LZMA_BUFFER_SIZE];
    stream.next_in = reinterpret_cast<const uint8_t*>(compressed_buf);
    stream.avail_in = compressed_buf_len;

    int ret = E_OK;
    while (true) {
        stream.next_out = out_buffer;
        stream.avail_out = LZMA_BUFFER_SIZE;
        lzma_ret_code = lzma_code(&stream, LZMA_RUN);

        const size_t produced = LZMA_BUFFER_SIZE - stream.avail_out;
        if (produced > 0) {
            out.write_buf(reinterpret_cast<const char*>(out_buffer), produced);
        }

        if (lzma_ret_code == LZMA_STREAM_END) {
            break;
        }
        if (lzma_ret_code != LZMA_OK) {
            ret = E_COMPRESS_ERR;
            break;
        }
    }

    lzma_end(&stream);
    if (ret != E_OK) {
        out.destroy();
        return ret;
    }

    uncompressed_buf_ = get_bytes_from_bytestream(out);
    uncompressed_buf_len = out.total_size();
    uncompressed_buf = uncompressed_buf_;
    out.destroy();
    return E_OK;
}

void LZMACompressor::after_uncompress(char* uncompressed_buf) {
    UNUSED(uncompressed_buf);
    if (uncompressed_buf_ != nullptr) {
        mem_free(uncompressed_buf_);
        uncompressed_buf_ = nullptr;
    }
}

}  // namespace storage
