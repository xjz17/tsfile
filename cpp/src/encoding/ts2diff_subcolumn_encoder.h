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

#ifndef ENCODING_TS2DIFF_SUBCOLUMN_ENCODER_H
#define ENCODING_TS2DIFF_SUBCOLUMN_ENCODER_H

#include <vector>

#include "common/allocator/byte_stream.h"
#include "subcolumn_encoder.h"
#include "ts2diff_encoder.h"

namespace storage {

template <typename TS2DIFFEncoderType>
class TS2DIFFSubcolumnEncoderBase : public Encoder {
   public:
    TS2DIFFSubcolumnEncoderBase()
        : ts2diff_cache_(1024, common::MOD_ENCODER_OBJ) {}

    ~TS2DIFFSubcolumnEncoderBase() override = default;

    void reset() override {
        ts2diff_encoder_.reset();
        subcolumn_encoder_.reset();
        ts2diff_cache_.reset();
    }

    void destroy() override {}

    int flush(common::ByteStream& out_stream) override {
        int ret = ts2diff_encoder_.flush(ts2diff_cache_);
        if (ret != common::E_OK) {
            return ret;
        }

        const uint32_t ts2diff_size = ts2diff_cache_.total_size();
        if (ts2diff_size == 0) {
            ts2diff_cache_.reset();
            return common::E_OK;
        }

        if (RET_FAIL(common::SerializationUtil::write_var_uint(ts2diff_size,
                                                               out_stream))) {
            return common::E_FILE_WRITE_ERR;
        }

        std::vector<uint8_t> bytes(ts2diff_size);
        uint32_t read_len = 0;
        ret = ts2diff_cache_.read_buf(bytes.data(), ts2diff_size, read_len);
        if (ret != common::E_OK || read_len != ts2diff_size) {
            return common::E_PARTIAL_READ;
        }

        for (uint32_t i = 0; i < ts2diff_size; ++i) {
            if (RET_FAIL(subcolumn_encoder_.encode(
                    static_cast<int32_t>(bytes[i]), out_stream))) {
                return common::E_ENCODE_ERR;
            }
        }
        ret = subcolumn_encoder_.flush(out_stream);
        if (ret != common::E_OK) {
            return ret;
        }

        ts2diff_cache_.reset();
        return common::E_OK;
    }

    int get_max_byte_size() override {
        return ts2diff_encoder_.get_max_byte_size() * 2 + 64;
    }

    int encode(bool value, common::ByteStream& out_stream) override {
        (void)value;
        (void)out_stream;
        return common::E_TYPE_NOT_MATCH;
    }
    int encode(int32_t value, common::ByteStream& out_stream) override {
        (void)value;
        (void)out_stream;
        return common::E_TYPE_NOT_MATCH;
    }
    int encode(int64_t value, common::ByteStream& out_stream) override {
        (void)value;
        (void)out_stream;
        return common::E_TYPE_NOT_MATCH;
    }
    int encode(float value, common::ByteStream& out_stream) override {
        (void)value;
        (void)out_stream;
        return common::E_TYPE_NOT_MATCH;
    }
    int encode(double value, common::ByteStream& out_stream) override {
        (void)value;
        (void)out_stream;
        return common::E_TYPE_NOT_MATCH;
    }
    int encode(common::String value, common::ByteStream& out_stream) override {
        (void)value;
        (void)out_stream;
        return common::E_TYPE_NOT_MATCH;
    }

   protected:
    TS2DIFFEncoderType ts2diff_encoder_{};
    IntSubcolumnEncoder subcolumn_encoder_{};
    common::ByteStream ts2diff_cache_;
};

class Int32TS2DIFFSubcolumnEncoder
    : public TS2DIFFSubcolumnEncoderBase<IntTS2DIFFEncoder> {
   public:
    int encode(int32_t value, common::ByteStream& out_stream) override {
        (void)out_stream;
        return ts2diff_encoder_.encode(value, ts2diff_cache_);
    }
};

class Int64TS2DIFFSubcolumnEncoder
    : public TS2DIFFSubcolumnEncoderBase<LongTS2DIFFEncoder> {
   public:
    int encode(int64_t value, common::ByteStream& out_stream) override {
        (void)out_stream;
        return ts2diff_encoder_.encode(value, ts2diff_cache_);
    }
};

class FloatTS2DIFFSubcolumnEncoder
    : public TS2DIFFSubcolumnEncoderBase<FloatTS2DIFFEncoder> {
   public:
    int encode(float value, common::ByteStream& out_stream) override {
        (void)out_stream;
        return ts2diff_encoder_.encode(value, ts2diff_cache_);
    }
};

class DoubleTS2DIFFSubcolumnEncoder
    : public TS2DIFFSubcolumnEncoderBase<DoubleTS2DIFFEncoder> {
   public:
    int encode(double value, common::ByteStream& out_stream) override {
        (void)out_stream;
        return ts2diff_encoder_.encode(value, ts2diff_cache_);
    }
};

}  // namespace storage

#endif  // ENCODING_TS2DIFF_SUBCOLUMN_ENCODER_H
