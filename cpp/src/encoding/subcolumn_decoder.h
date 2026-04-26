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

#ifndef ENCODING_SUBCOLUMN_DECODER_H
#define ENCODING_SUBCOLUMN_DECODER_H

#include <algorithm>
#include <type_traits>

#include "common/allocator/alloc_base.h"
#include "common/allocator/byte_stream.h"
#include "decoder.h"
#include "utils/db_utils.h"

namespace storage {

template <typename T>
class SubcolumnDecoder : public Decoder {
   public:
    static constexpr int BLOCK_SIZE = 512;

    SubcolumnDecoder() { init(); }
    ~SubcolumnDecoder() {}

    void reset() override {
        current_index_ = 0;
        value_count_ = 0;
        bit_width_ = 0;
        min_value_ = 0;
        bits_left_ = 0;
        buffer_ = 0;
    }

    bool has_remaining(const common::ByteStream &buffer) override {
        (void)buffer;
        return current_index_ < value_count_;
    }

    int read_boolean(bool &ret_value, common::ByteStream &in) override {
        (void)ret_value;
        (void)in;
        return common::E_NOT_SUPPORT;
    }
    int read_int32(int32_t &ret_value, common::ByteStream &in) override {
        (void)ret_value;
        (void)in;
        return common::E_NOT_SUPPORT;
    }
    int read_int64(int64_t &ret_value, common::ByteStream &in) override {
        (void)ret_value;
        (void)in;
        return common::E_NOT_SUPPORT;
    }
    int read_float(float &ret_value, common::ByteStream &in) override {
        (void)ret_value;
        (void)in;
        return common::E_NOT_SUPPORT;
    }
    int read_double(double &ret_value, common::ByteStream &in) override {
        (void)ret_value;
        (void)in;
        return common::E_NOT_SUPPORT;
    }
    int read_String(common::String &ret_value, common::PageArena &pa,
                    common::ByteStream &in) override {
        (void)ret_value;
        (void)pa;
        (void)in;
        return common::E_NOT_SUPPORT;
    }

   protected:
    using UnsignedT = typename std::make_unsigned<T>::type;
    T values_[BLOCK_SIZE];
    UnsignedT delta_values_[BLOCK_SIZE];
    int current_index_ = 0;
    int value_count_ = 0;
    int bit_width_ = 0;
    T min_value_ = 0;

    uint8_t buffer_ = 0;
    int bits_left_ = 0;

    void init() { reset(); }

    int do_decode(T &ret_value, common::ByteStream &in) {
        if (current_index_ >= value_count_) {
            int ret = load_next_block(in);
            if (ret != common::E_OK) {
                return ret;
            }
        }
        ret_value = values_[current_index_++];
        return common::E_OK;
    }

    int load_next_block(common::ByteStream &in) {
        int ret = common::SerializationUtil::read_i32(value_count_, in);
        if (ret != common::E_OK) {
            return ret;
        }
        if (value_count_ <= 0 || value_count_ > BLOCK_SIZE) {
            return common::E_BUF_NOT_ENOUGH;
        }
        if ((ret = common::SerializationUtil::read_i32(bit_width_, in)) != common::E_OK) {
            return ret;
        }
        if (sizeof(T) == sizeof(int32_t)) {
            int32_t min_v = 0;
            if ((ret = common::SerializationUtil::read_i32(min_v, in)) != common::E_OK) {
                return ret;
            }
            min_value_ = static_cast<T>(min_v);
        } else {
            int64_t min_v = 0;
            if ((ret = common::SerializationUtil::read_i64(min_v, in)) != common::E_OK) {
                return ret;
            }
            min_value_ = static_cast<T>(min_v);
        }

        for (int i = 0; i < value_count_; ++i) {
            delta_values_[i] = 0;
        }

        bits_left_ = 0;
        buffer_ = 0;
        if (bit_width_ > 0 && !decode_adaptive_subcolumns(in)) {
            return common::E_BUF_NOT_ENOUGH;
        }
        const UnsignedT min_u = static_cast<UnsignedT>(min_value_);
        for (int i = 0; i < value_count_; ++i) {
            values_[i] = static_cast<T>(min_u + delta_values_[i]);
        }
        current_index_ = 0;
        return common::E_OK;
    }

    int read_bit(common::ByteStream &in) {
        if (bits_left_ == 0) {
            uint32_t read_len = 0;
            in.read_buf(&buffer_, 1, read_len);
            if (read_len != 1) {
                return -1;
            }
            bits_left_ = 8;
        }
        const int bit = (buffer_ >> (bits_left_ - 1)) & 1U;
        bits_left_--;
        return bit;
    }

    bool read_bits(common::ByteStream &in, int bit_width, uint64_t &value) {
        value = 0;
        if (bit_width <= 0) {
            return true;
        }
        for (int i = 0; i < bit_width; ++i) {
            int b = read_bit(in);
            if (b < 0) {
                return false;
            }
            value = (value << 1U) | static_cast<uint64_t>(b);
        }
        return true;
    }

    bool read_values(
        common::ByteStream &in, int bit_width, int count, uint16_t *out) {
        if (bit_width <= 0) {
            for (int i = 0; i < count; ++i) {
                out[i] = 0;
            }
            return true;
        }
        for (int i = 0; i < count; ++i) {
            uint64_t v = 0;
            if (!read_bits(in, bit_width, v)) {
                return false;
            }
            out[i] = static_cast<uint16_t>(v);
        }
        return true;
    }

    static int cal_bit_width(uint64_t n) {
        int width = 0;
        while (n > 0) {
            width++;
            n >>= 1U;
        }
        return width;
    }

    bool decode_adaptive_subcolumns(common::ByteStream &in) {
        uint8_t beta_u8 = 0;
        if (common::SerializationUtil::read_ui8(beta_u8, in) != common::E_OK) {
            return false;
        }
        const int beta = static_cast<int>(beta_u8);
        if (beta <= 0 || beta > 8) {
            return false;
        }
        const int groups = (bit_width_ + beta - 1) / beta;
        uint8_t bit_width_list[64] = {0};
        for (int g = 0; g < groups; ++g) {
            if (common::SerializationUtil::read_ui8(bit_width_list[g], in) != common::E_OK) {
                return false;
            }
        }

        uint8_t types[64] = {0};
        bits_left_ = 0;
        buffer_ = 0;
        for (int g = 0; g < groups; ++g) {
            uint64_t type_v = 0;
            if (!read_bits(in, 2, type_v)) {
                return false;
            }
            types[g] = static_cast<uint8_t>(type_v);
        }
        bits_left_ = 0;
        buffer_ = 0;

        const int run_len_bw = cal_bit_width(static_cast<uint64_t>(value_count_));
        for (int g = 0; g < groups; ++g) {
            const int shift = g * beta;
            uint16_t group_values[BLOCK_SIZE] = {0};
            const int bw = static_cast<int>(bit_width_list[g]);
            if (types[g] == 0) {
                if (!read_values(in, bw, value_count_, group_values)) {
                    return false;
                }
            } else if (types[g] == 1) {
                uint16_t run_count = 0;
                if (common::SerializationUtil::read_ui16(run_count, in) != common::E_OK) {
                    return false;
                }
                if (run_count == 0 || run_count > static_cast<uint16_t>(value_count_)) {
                    return false;
                }
                uint16_t run_ends[BLOCK_SIZE] = {0};
                uint16_t run_vals[BLOCK_SIZE] = {0};
                if (!read_values(in, run_len_bw, run_count, run_ends)) {
                    return false;
                }
                if (!read_values(in, bw, run_count, run_vals)) {
                    return false;
                }
                int start = 0;
                for (int r = 0; r < run_count; ++r) {
                    const int end = std::min<int>(run_ends[r], value_count_);
                    if (end < start) {
                        return false;
                    }
                    for (int i = start; i < end; ++i) {
                        group_values[i] = run_vals[r];
                    }
                    start = end;
                }
                if (start != value_count_) {
                    return false;
                }
            } else if (types[g] == 2) {
                uint16_t cardinality = 0;
                if (common::SerializationUtil::read_ui16(cardinality, in) != common::E_OK) {
                    return false;
                }
                if (cardinality == 0 || cardinality > 256) {
                    return false;
                }
                uint16_t dict[256] = {0};
                if (!read_values(in, bw, cardinality, dict)) {
                    return false;
                }
                const int code_bw = cal_bit_width(static_cast<uint64_t>(cardinality - 1));
                uint16_t codes[BLOCK_SIZE] = {0};
                if (!read_values(in, code_bw, value_count_, codes)) {
                    return false;
                }
                for (int i = 0; i < value_count_; ++i) {
                    if (codes[i] >= cardinality) {
                        return false;
                    }
                    group_values[i] = dict[codes[i]];
                }
            } else {
                return false;
            }

            for (int i = 0; i < value_count_; ++i) {
                delta_values_[i] |=
                    (static_cast<UnsignedT>(group_values[i]) << shift);
            }
            bits_left_ = 0;
            buffer_ = 0;
        }
        return true;
    }
};

class FloatSubcolumnDecoder : public SubcolumnDecoder<int32_t> {
   public:
    int read_float(float &ret_value, common::ByteStream &in) override {
        int32_t raw = 0;
        int ret = do_decode(raw, in);
        if (ret != common::E_OK) {
            return ret;
        }
        ret_value = common::int_to_float(raw);
        return common::E_OK;
    }
};

class DoubleSubcolumnDecoder : public SubcolumnDecoder<int64_t> {
   public:
    int read_double(double &ret_value, common::ByteStream &in) override {
        int64_t raw = 0;
        int ret = do_decode(raw, in);
        if (ret != common::E_OK) {
            return ret;
        }
        ret_value = common::long_to_double(raw);
        return common::E_OK;
    }
};

typedef SubcolumnDecoder<int32_t> IntSubcolumnDecoder;
typedef SubcolumnDecoder<int64_t> LongSubcolumnDecoder;

template <>
inline int IntSubcolumnDecoder::read_int32(int32_t &ret_value, common::ByteStream &in) {
    return do_decode(ret_value, in);
}
template <>
inline int LongSubcolumnDecoder::read_int64(int64_t &ret_value, common::ByteStream &in) {
    return do_decode(ret_value, in);
}

}  // namespace storage

#endif  // ENCODING_SUBCOLUMN_DECODER_H
