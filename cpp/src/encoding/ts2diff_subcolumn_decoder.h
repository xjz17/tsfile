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

#ifndef ENCODING_TS2DIFF_SUBCOLUMN_DECODER_H
#define ENCODING_TS2DIFF_SUBCOLUMN_DECODER_H

#include "common/allocator/byte_stream.h"
#include "subcolumn_decoder.h"
#include "ts2diff_decoder.h"

namespace storage {

template <typename TS2DIFFDecoderType>
class TS2DIFFSubcolumnDecoderBase : public Decoder {
   public:
    TS2DIFFSubcolumnDecoderBase()
        : ts2diff_stream_(1024, common::MOD_DECODER_OBJ), loaded_(false) {}

    ~TS2DIFFSubcolumnDecoderBase() override = default;

    void reset() override {
        ts2diff_decoder_.reset();
        subcolumn_decoder_.reset();
        ts2diff_stream_.reset();
        loaded_ = false;
    }

    bool has_remaining(const common::ByteStream& in) override {
        if (loaded_) {
            return ts2diff_decoder_.has_remaining(ts2diff_stream_);
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

        uint32_t ts2diff_size = 0;
        int ret = common::SerializationUtil::read_var_uint(ts2diff_size, in);
        if (ret != common::E_OK) {
            return common::E_PARTIAL_READ;
        }

        ts2diff_stream_.reset();
        for (uint32_t i = 0; i < ts2diff_size; ++i) {
            int32_t value = 0;
            ret = subcolumn_decoder_.read_int32(value, in);
            if (ret != common::E_OK) {
                return ret;
            }
            const uint8_t byte = static_cast<uint8_t>(value & 0xFF);
            ts2diff_stream_.write_buf(
                reinterpret_cast<const char*>(&byte), 1);
        }

        loaded_ = true;
        return common::E_OK;
    }

    TS2DIFFDecoderType ts2diff_decoder_{};
    IntSubcolumnDecoder subcolumn_decoder_{};
    common::ByteStream ts2diff_stream_;
    bool loaded_;
};

class Int32TS2DIFFSubcolumnDecoder
    : public TS2DIFFSubcolumnDecoderBase<IntTS2DIFFDecoder> {
   public:
    int read_int32(int32_t& ret_value, common::ByteStream& in) override {
        int ret = ensure_loaded(in);
        if (ret != common::E_OK) {
            return ret;
        }
        return ts2diff_decoder_.read_int32(ret_value, ts2diff_stream_);
    }
};

class Int64TS2DIFFSubcolumnDecoder
    : public TS2DIFFSubcolumnDecoderBase<LongTS2DIFFDecoder> {
   public:
    int read_int64(int64_t& ret_value, common::ByteStream& in) override {
        int ret = ensure_loaded(in);
        if (ret != common::E_OK) {
            return ret;
        }
        return ts2diff_decoder_.read_int64(ret_value, ts2diff_stream_);
    }
};

class FloatTS2DIFFSubcolumnDecoder
    : public TS2DIFFSubcolumnDecoderBase<FloatTS2DIFFDecoder> {
   public:
    int read_float(float& ret_value, common::ByteStream& in) override {
        int ret = ensure_loaded(in);
        if (ret != common::E_OK) {
            return ret;
        }
        return ts2diff_decoder_.read_float(ret_value, ts2diff_stream_);
    }
};

class DoubleTS2DIFFSubcolumnDecoder
    : public TS2DIFFSubcolumnDecoderBase<DoubleTS2DIFFDecoder> {
   public:
    int read_double(double& ret_value, common::ByteStream& in) override {
        int ret = ensure_loaded(in);
        if (ret != common::E_OK) {
            return ret;
        }
        return ts2diff_decoder_.read_double(ret_value, ts2diff_stream_);
    }
};

}  // namespace storage

#endif  // ENCODING_TS2DIFF_SUBCOLUMN_DECODER_H
