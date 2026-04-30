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

#ifndef ENCODING_SPRINTZ_SUBCOLUMN_DECODER_H
#define ENCODING_SPRINTZ_SUBCOLUMN_DECODER_H

#include "common/allocator/byte_stream.h"
#include "double_sprintz_decoder.h"
#include "float_sprintz_decoder.h"
#include "int32_sprintz_decoder.h"
#include "int64_sprintz_decoder.h"
#include "subcolumn_decoder.h"

namespace storage {

template <typename SprintzDecoderType>
class SprintzSubcolumnDecoderBase : public Decoder {
   public:
    SprintzSubcolumnDecoderBase()
        : sprintz_stream_(1024, common::MOD_DECODER_OBJ), loaded_(false) {}

    ~SprintzSubcolumnDecoderBase() override = default;

    void reset() override {
        sprintz_decoder_.reset();
        subcolumn_decoder_.reset();
        sprintz_stream_.reset();
        loaded_ = false;
    }

    bool has_remaining(const common::ByteStream& in) override {
        if (loaded_) {
            return sprintz_decoder_.has_remaining(sprintz_stream_);
        }
        return in.has_remaining();
    }

    int read_boolean(bool& ret_value, common::ByteStream& in) override {
        (void)ret_value;
        (void)in;
        return common::E_TYPE_NOT_MATCH;
    }
    int read_int32(int32_t& ret_value, common::ByteStream& in) override {
        (void)ret_value;
        (void)in;
        return common::E_TYPE_NOT_MATCH;
    }
    int read_int64(int64_t& ret_value, common::ByteStream& in) override {
        (void)ret_value;
        (void)in;
        return common::E_TYPE_NOT_MATCH;
    }
    int read_float(float& ret_value, common::ByteStream& in) override {
        (void)ret_value;
        (void)in;
        return common::E_TYPE_NOT_MATCH;
    }
    int read_double(double& ret_value, common::ByteStream& in) override {
        (void)ret_value;
        (void)in;
        return common::E_TYPE_NOT_MATCH;
    }
    int read_String(common::String& ret_value, common::PageArena& pa,
                    common::ByteStream& in) override {
        (void)ret_value;
        (void)pa;
        (void)in;
        return common::E_TYPE_NOT_MATCH;
    }

   protected:
    int ensure_loaded(common::ByteStream& in) {
        if (loaded_) {
            return common::E_OK;
        }

        int ret = common::E_OK;
        uint32_t sprintz_size = 0;
        ret = common::SerializationUtil::read_var_uint(sprintz_size, in);
        if (ret != common::E_OK) {
            return common::E_PARTIAL_READ;
        }

        sprintz_stream_.reset();
        for (uint32_t i = 0; i < sprintz_size; ++i) {
            int32_t value = 0;
            ret = subcolumn_decoder_.read_int32(value, in);
            if (ret != common::E_OK) {
                return ret;
            }
            const uint8_t byte = static_cast<uint8_t>(value & 0xFF);
            sprintz_stream_.write_buf(
                reinterpret_cast<const char*>(&byte), 1);
        }

        loaded_ = true;
        return common::E_OK;
    }

    SprintzDecoderType sprintz_decoder_{};
    IntSubcolumnDecoder subcolumn_decoder_{};
    common::ByteStream sprintz_stream_;
    bool loaded_;
};

class Int32SprintzSubcolumnDecoder
    : public SprintzSubcolumnDecoderBase<Int32SprintzDecoder> {
   public:
    int read_int32(int32_t& ret_value, common::ByteStream& in) override {
        int ret = ensure_loaded(in);
        if (ret != common::E_OK) {
            return ret;
        }
        return sprintz_decoder_.read_int32(ret_value, sprintz_stream_);
    }
};

class Int64SprintzSubcolumnDecoder
    : public SprintzSubcolumnDecoderBase<Int64SprintzDecoder> {
   public:
    int read_int64(int64_t& ret_value, common::ByteStream& in) override {
        int ret = ensure_loaded(in);
        if (ret != common::E_OK) {
            return ret;
        }
        return sprintz_decoder_.read_int64(ret_value, sprintz_stream_);
    }
};

class FloatSprintzSubcolumnDecoder
    : public SprintzSubcolumnDecoderBase<FloatSprintzDecoder> {
   public:
    int read_float(float& ret_value, common::ByteStream& in) override {
        int ret = ensure_loaded(in);
        if (ret != common::E_OK) {
            return ret;
        }
        return sprintz_decoder_.read_float(ret_value, sprintz_stream_);
    }
};

class DoubleSprintzSubcolumnDecoder
    : public SprintzSubcolumnDecoderBase<DoubleSprintzDecoder> {
   public:
    int read_double(double& ret_value, common::ByteStream& in) override {
        int ret = ensure_loaded(in);
        if (ret != common::E_OK) {
            return ret;
        }
        return sprintz_decoder_.read_double(ret_value, sprintz_stream_);
    }
};

}  // namespace storage

#endif  // ENCODING_SPRINTZ_SUBCOLUMN_DECODER_H
