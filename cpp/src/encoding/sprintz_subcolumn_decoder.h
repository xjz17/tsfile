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

#include <limits>
#include <vector>

#include "common/allocator/byte_stream.h"
#include "subcolumn_decoder.h"
#include "utils/db_utils.h"

namespace storage {

template <typename T>
class SprintzSubcolumnDecoderBase : public Decoder {
   public:
    SprintzSubcolumnDecoderBase() = default;
    ~SprintzSubcolumnDecoderBase() override = default;

    void reset() override {
        subcolumn_decoder_.reset();
        values_.clear();
        read_index_ = 0;
    }

    bool has_remaining(const common::ByteStream& in) override {
        return read_index_ < values_.size() || in.has_remaining();
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
    static int64_t dezigzag(uint64_t value) {
        return (value & 1U) == 0U
                   ? static_cast<int64_t>(value >> 1U)
                   : -static_cast<int64_t>((value + 1U) >> 1U);
    }

    static bool in_range(int64_t value) {
        const int64_t lo = static_cast<int64_t>(std::numeric_limits<T>::min());
        const int64_t hi = static_cast<int64_t>(std::numeric_limits<T>::max());
        return value >= lo && value <= hi;
    }

    int ensure_loaded(common::ByteStream& in) {
        if (read_index_ < values_.size()) {
            return common::E_OK;
        }

        values_.clear();
        read_index_ = 0;

        int32_t count = 0;
        if (common::SerializationUtil::read_i32(count, in) != common::E_OK) {
            return common::E_PARTIAL_READ;
        }
        if (count <= 0) {
            return common::E_PARTIAL_READ;
        }
        values_.reserve(static_cast<size_t>(count));

        T first = 0;
        if constexpr (sizeof(T) == sizeof(int32_t)) {
            int32_t v = 0;
            if (common::SerializationUtil::read_i32(v, in) != common::E_OK) {
                return common::E_PARTIAL_READ;
            }
            first = static_cast<T>(v);
        } else {
            int64_t v = 0;
            if (common::SerializationUtil::read_i64(v, in) != common::E_OK) {
                return common::E_PARTIAL_READ;
            }
            first = static_cast<T>(v);
        }
        values_.push_back(first);
        if (count == 1) {
            return common::E_OK;
        }

        uint64_t min_delta = 0;
        if constexpr (sizeof(T) == sizeof(int32_t)) {
            int32_t v = 0;
            if (common::SerializationUtil::read_i32(v, in) != common::E_OK) {
                return common::E_PARTIAL_READ;
            }
            min_delta = static_cast<uint64_t>(static_cast<uint32_t>(v));
        } else {
            int64_t v = 0;
            if (common::SerializationUtil::read_i64(v, in) != common::E_OK) {
                return common::E_PARTIAL_READ;
            }
            min_delta = static_cast<uint64_t>(v);
        }

        subcolumn_decoder_.reset();
        for (int i = 1; i < count; ++i) {
            T norm = 0;
            int ret = common::E_OK;
            if constexpr (sizeof(T) == sizeof(int32_t)) {
                int32_t v = 0;
                ret = subcolumn_decoder_.read_int32(v, in);
                norm = static_cast<T>(v);
            } else {
                int64_t v = 0;
                ret = subcolumn_decoder_.read_int64(v, in);
                norm = static_cast<T>(v);
            }
            if (ret != common::E_OK) {
                return ret;
            }

            const uint64_t zigzag = static_cast<uint64_t>(static_cast<int64_t>(norm)) + min_delta;
            const int64_t delta = dezigzag(zigzag);
            const int64_t prev = static_cast<int64_t>(values_[i - 1]);
            const int64_t cur = prev + delta;
            if (!in_range(cur)) {
                return common::E_OVERFLOW;
            }
            values_.push_back(static_cast<T>(cur));
        }
        return common::E_OK;
    }

    std::vector<T> values_;
    size_t read_index_ = 0;
    SubcolumnDecoder<T> subcolumn_decoder_{};
};

class Int32SprintzSubcolumnDecoder
    : public SprintzSubcolumnDecoderBase<int32_t> {
   public:
    int read_int32(int32_t& ret_value, common::ByteStream& in) override {
        int ret = ensure_loaded(in);
        if (ret != common::E_OK) {
            return ret;
        }
        ret_value = values_[read_index_++];
        return common::E_OK;
    }
};

class Int64SprintzSubcolumnDecoder
    : public SprintzSubcolumnDecoderBase<int64_t> {
   public:
    int read_int64(int64_t& ret_value, common::ByteStream& in) override {
        int ret = ensure_loaded(in);
        if (ret != common::E_OK) {
            return ret;
        }
        ret_value = values_[read_index_++];
        return common::E_OK;
    }
};

class FloatSprintzSubcolumnDecoder
    : public SprintzSubcolumnDecoderBase<int32_t> {
   public:
    int read_float(float& ret_value, common::ByteStream& in) override {
        int32_t raw = 0;
        int ret = ensure_loaded(in);
        if (ret != common::E_OK) {
            return ret;
        }
        raw = values_[read_index_++];
        ret_value = common::int_to_float(raw);
        return common::E_OK;
    }
};

class DoubleSprintzSubcolumnDecoder
    : public SprintzSubcolumnDecoderBase<int64_t> {
   public:
    int read_double(double& ret_value, common::ByteStream& in) override {
        int64_t raw = 0;
        int ret = ensure_loaded(in);
        if (ret != common::E_OK) {
            return ret;
        }
        raw = values_[read_index_++];
        ret_value = common::long_to_double(raw);
        return common::E_OK;
    }
};

}  // namespace storage

#endif  // ENCODING_SPRINTZ_SUBCOLUMN_DECODER_H
