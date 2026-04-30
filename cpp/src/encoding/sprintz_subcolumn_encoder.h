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

#ifndef ENCODING_SPRINTZ_SUBCOLUMN_ENCODER_H
#define ENCODING_SPRINTZ_SUBCOLUMN_ENCODER_H

#include <vector>

#include "common/allocator/byte_stream.h"
#include "double_sprintz_encoder.h"
#include "float_sprintz_encoder.h"
#include "int32_sprintz_encoder.h"
#include "int64_sprintz_encoder.h"
#include "subcolumn_encoder.h"

namespace storage {

template <typename SprintzEncoderType>
class SprintzSubcolumnEncoderBase : public Encoder {
   public:
    SprintzSubcolumnEncoderBase()
        : sprintz_cache_(1024, common::MOD_ENCODER_OBJ) {}

    ~SprintzSubcolumnEncoderBase() override = default;

    void reset() override {
        sprintz_encoder_.reset();
        subcolumn_encoder_.reset();
        sprintz_cache_.reset();
    }

    void destroy() override {}

    int flush(common::ByteStream& out_stream) override {
        int ret = sprintz_encoder_.flush(sprintz_cache_);
        if (ret != common::E_OK) {
            return ret;
        }

        const uint32_t sprintz_size = sprintz_cache_.total_size();
        if (sprintz_size == 0) {
            sprintz_cache_.reset();
            return common::E_OK;
        }

        if (RET_FAIL(
                common::SerializationUtil::write_var_uint(sprintz_size,
                                                          out_stream))) {
            return common::E_FILE_WRITE_ERR;
        }

        std::vector<uint8_t> bytes(sprintz_size);
        uint32_t read_len = 0;
        ret = sprintz_cache_.read_buf(bytes.data(), sprintz_size, read_len);
        if (ret != common::E_OK || read_len != sprintz_size) {
            return common::E_PARTIAL_READ;
        }

        for (uint32_t i = 0; i < sprintz_size; ++i) {
            if (RET_FAIL(subcolumn_encoder_.encode(
                    static_cast<int32_t>(bytes[i]), out_stream))) {
                return common::E_ENCODE_ERR;
            }
        }
        ret = subcolumn_encoder_.flush(out_stream);
        if (ret != common::E_OK) {
            return ret;
        }

        sprintz_cache_.reset();
        return common::E_OK;
    }

    int get_max_byte_size() override {
        return sprintz_encoder_.get_max_byte_size() * 2 + 64;
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
    SprintzEncoderType sprintz_encoder_{};
    IntSubcolumnEncoder subcolumn_encoder_{};
    common::ByteStream sprintz_cache_;
};

class Int32SprintzSubcolumnEncoder
    : public SprintzSubcolumnEncoderBase<Int32SprintzEncoder> {
   public:
    int encode(int32_t value, common::ByteStream& out_stream) override {
        (void)out_stream;
        return sprintz_encoder_.encode(value, sprintz_cache_);
    }
};

class Int64SprintzSubcolumnEncoder
    : public SprintzSubcolumnEncoderBase<Int64SprintzEncoder> {
   public:
    int encode(int64_t value, common::ByteStream& out_stream) override {
        (void)out_stream;
        return sprintz_encoder_.encode(value, sprintz_cache_);
    }
};

class FloatSprintzSubcolumnEncoder
    : public SprintzSubcolumnEncoderBase<FloatSprintzEncoder> {
   public:
    int encode(float value, common::ByteStream& out_stream) override {
        (void)out_stream;
        return sprintz_encoder_.encode(value, sprintz_cache_);
    }
};

class DoubleSprintzSubcolumnEncoder
    : public SprintzSubcolumnEncoderBase<DoubleSprintzEncoder> {
   public:
    int encode(double value, common::ByteStream& out_stream) override {
        (void)out_stream;
        return sprintz_encoder_.encode(value, sprintz_cache_);
    }
};

}  // namespace storage

#endif  // ENCODING_SPRINTZ_SUBCOLUMN_ENCODER_H
