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

#include <limits>
#include <vector>

#include "common/allocator/byte_stream.h"
#include "subcolumn_encoder.h"
#include "utils/db_utils.h"

namespace storage {

template <typename T>
class TS2DIFFSubcolumnEncoderBase : public Encoder {
   public:
    static constexpr int BLOCK_SIZE = 512;

    TS2DIFFSubcolumnEncoderBase() = default;
    ~TS2DIFFSubcolumnEncoderBase() override = default;

    void reset() override {
        subcolumn_encoder_.reset();
        values_.clear();
    }

    void destroy() override {}

    int flush(common::ByteStream& out_stream) override {
        if (values_.empty()) {
            return common::E_OK;
        }
        return flush_block(out_stream);
    }

    int get_max_byte_size() override {
        return static_cast<int>(sizeof(T) * BLOCK_SIZE * 2 + 64);
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
    int encode_value(T value, common::ByteStream& out_stream) {
        values_.push_back(value);
        if (static_cast<int>(values_.size()) >= BLOCK_SIZE) {
            return flush_block(out_stream);
        }
        return common::E_OK;
    }

    static bool in_range(int64_t value) {
        const int64_t lo = static_cast<int64_t>(std::numeric_limits<T>::min());
        const int64_t hi = static_cast<int64_t>(std::numeric_limits<T>::max());
        return value >= lo && value <= hi;
    }

    int flush_block(common::ByteStream& out_stream) {
        const int count = static_cast<int>(values_.size());
        if (common::SerializationUtil::write_i32(count, out_stream) != common::E_OK) {
            return common::E_FILE_WRITE_ERR;
        }
        if (count <= 0) {
            values_.clear();
            return common::E_OK;
        }

        if constexpr (sizeof(T) == sizeof(int32_t)) {
            if (common::SerializationUtil::write_i32(
                    static_cast<int32_t>(values_[0]), out_stream) != common::E_OK) {
                return common::E_FILE_WRITE_ERR;
            }
        } else {
            if (common::SerializationUtil::write_i64(
                    static_cast<int64_t>(values_[0]), out_stream) != common::E_OK) {
                return common::E_FILE_WRITE_ERR;
            }
        }
        if (count == 1) {
            values_.clear();
            return common::E_OK;
        }

        int64_t min_delta = std::numeric_limits<int64_t>::max();
        std::vector<int64_t> deltas;
        deltas.reserve(count - 1);
        for (int i = 1; i < count; ++i) {
            const int64_t prev = static_cast<int64_t>(values_[i - 1]);
            const int64_t cur = static_cast<int64_t>(values_[i]);
            const int64_t d = cur - prev;
            deltas.push_back(d);
            if (d < min_delta) {
                min_delta = d;
            }
        }

        if constexpr (sizeof(T) == sizeof(int32_t)) {
            if (!in_range(min_delta) ||
                common::SerializationUtil::write_i32(
                    static_cast<int32_t>(min_delta), out_stream) != common::E_OK) {
                return common::E_OVERFLOW;
            }
        } else {
            if (common::SerializationUtil::write_i64(min_delta, out_stream) != common::E_OK) {
                return common::E_FILE_WRITE_ERR;
            }
        }

        for (int i = 0; i < count - 1; ++i) {
            const int64_t norm = deltas[i] - min_delta;
            if (!in_range(norm)) {
                return common::E_OVERFLOW;
            }
            const int ret = subcolumn_encoder_.encode(static_cast<T>(norm), out_stream);
            if (ret != common::E_OK) {
                return ret;
            }
        }
        const int ret = subcolumn_encoder_.flush(out_stream);
        if (ret != common::E_OK) {
            return ret;
        }
        values_.clear();
        return common::E_OK;
    }

    SubcolumnEncoder<T> subcolumn_encoder_{};
    std::vector<T> values_;
};

class Int32TS2DIFFSubcolumnEncoder
    : public TS2DIFFSubcolumnEncoderBase<int32_t> {
   public:
    int encode(int32_t value, common::ByteStream& out_stream) override {
        return encode_value(value, out_stream);
    }
};

class Int64TS2DIFFSubcolumnEncoder
    : public TS2DIFFSubcolumnEncoderBase<int64_t> {
   public:
    int encode(int64_t value, common::ByteStream& out_stream) override {
        return encode_value(value, out_stream);
    }
};

class FloatTS2DIFFSubcolumnEncoder
    : public TS2DIFFSubcolumnEncoderBase<int32_t> {
   public:
    int encode(float value, common::ByteStream& out_stream) override {
        return encode_value(common::float_to_int(value), out_stream);
    }
};

class DoubleTS2DIFFSubcolumnEncoder
    : public TS2DIFFSubcolumnEncoderBase<int64_t> {
   public:
    int encode(double value, common::ByteStream& out_stream) override {
        return encode_value(common::double_to_long(value), out_stream);
    }
};

}  // namespace storage

#endif  // ENCODING_TS2DIFF_SUBCOLUMN_ENCODER_H
