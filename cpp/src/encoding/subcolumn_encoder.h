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

#ifndef ENCODING_SUBCOLUMN_ENCODER_H
#define ENCODING_SUBCOLUMN_ENCODER_H

#include <algorithm>
#include <limits>
#include <type_traits>
#include <vector>

#include "common/allocator/alloc_base.h"
#include "common/allocator/byte_stream.h"
#include "encoder.h"
#include "utils/db_utils.h"

namespace storage {

template <typename T>
class SubcolumnEncoder : public Encoder {
   public:
    static constexpr int BLOCK_SIZE = 512;
    static constexpr int BETA_LIST_SIZE = 3;
    static constexpr int BETA_LIST[BETA_LIST_SIZE] = {2, 3, 4};

    SubcolumnEncoder() { init(); }
    ~SubcolumnEncoder() {}

    void reset() override {
        write_index_ = 0;
        min_value_ = std::numeric_limits<T>::max();
        max_value_ = std::numeric_limits<T>::min();
    }

    void destroy() override {
        if (value_arr_ != nullptr) {
            common::mem_free(value_arr_);
            value_arr_ = nullptr;
        }
        if (delta_arr_ != nullptr) {
            common::mem_free(delta_arr_);
            delta_arr_ = nullptr;
        }
    }

    int flush(common::ByteStream &out_stream) override {
        if (write_index_ == 0) {
            return common::E_OK;
        }

        const int count = write_index_;
        using UnsignedT = typename std::make_unsigned<T>::type;
        UnsignedT max_delta = 0;
        for (int i = 0; i < count; ++i) {
            const UnsignedT delta = static_cast<UnsignedT>(value_arr_[i]) -
                                    static_cast<UnsignedT>(min_value_);
            delta_arr_[i] = delta;
            if (delta > max_delta) {
                max_delta = delta;
            }
        }

        const int bit_width = cal_bit_width(max_delta);
        common::SerializationUtil::write_i32(count, out_stream);
        common::SerializationUtil::write_i32(bit_width, out_stream);
        if (sizeof(T) == sizeof(int32_t)) {
            common::SerializationUtil::write_i32(
                static_cast<int32_t>(min_value_), out_stream);
        } else {
            common::SerializationUtil::write_i64(
                static_cast<int64_t>(min_value_), out_stream);
        }

        if (bit_width > 0 && !encode_adaptive_subcolumns(
                                 out_stream, count, bit_width)) {
            return common::E_OOM;
        }

        reset();
        return common::E_OK;
    }

    int get_max_byte_size() override {
        return static_cast<int>(sizeof(T) * BLOCK_SIZE * 2 + 32);
    }

    int encode(bool value, common::ByteStream &out_stream) override {
        (void)value;
        (void)out_stream;
        return common::E_TYPE_NOT_MATCH;
    }
    int encode(int32_t value, common::ByteStream &out_stream) override {
        (void)value;
        (void)out_stream;
        return common::E_TYPE_NOT_MATCH;
    }
    int encode(int64_t value, common::ByteStream &out_stream) override {
        (void)value;
        (void)out_stream;
        return common::E_TYPE_NOT_MATCH;
    }
    int encode(float value, common::ByteStream &out_stream) override {
        (void)value;
        (void)out_stream;
        return common::E_TYPE_NOT_MATCH;
    }
    int encode(double value, common::ByteStream &out_stream) override {
        (void)value;
        (void)out_stream;
        return common::E_TYPE_NOT_MATCH;
    }
    int encode(common::String value, common::ByteStream &out_stream) override {
        (void)value;
        (void)out_stream;
        return common::E_TYPE_NOT_MATCH;
    }

   protected:
    using UnsignedT = typename std::make_unsigned<T>::type;
    T *value_arr_ = nullptr;
    UnsignedT *delta_arr_ = nullptr;
    int write_index_ = 0;
    T min_value_ = std::numeric_limits<T>::max();
    T max_value_ = std::numeric_limits<T>::min();

    uint8_t buffer_ = 0;
    int bits_left_ = 8;

    void init() {
        value_arr_ = static_cast<T *>(common::mem_alloc(
            sizeof(T) * BLOCK_SIZE, common::MOD_TS2DIFF_OBJ));
        delta_arr_ = static_cast<UnsignedT *>(common::mem_alloc(
            sizeof(UnsignedT) * BLOCK_SIZE, common::MOD_TS2DIFF_OBJ));
        reset();
    }

    int do_encode(T value, common::ByteStream &out_stream) {
        value_arr_[write_index_++] = value;
        if (value < min_value_) {
            min_value_ = value;
        }
        if (value > max_value_) {
            max_value_ = value;
        }
        if (write_index_ >= BLOCK_SIZE) {
            return flush(out_stream);
        }
        return common::E_OK;
    }

    static int cal_bit_width(UnsignedT n) {
        int bit_width = 0;
        while (n > 0) {
            bit_width++;
            n >>= 1U;
        }
        return bit_width;
    }

    struct GroupPlan {
        int bit_width = 0;
        int type = 0;  // 0: BP, 1: RLE, 2: DICT
        int run_count = 0;
        int cardinality = 0;
    };

    static int get_threshold(int block_size, int beta) {
        static const int DEFAULT_THRESHOLD[32] = {
            2, 3, 5, 8, 9, 11, 14, 16, 17, 17, 18, 19, 20, 21, 22, 22,
            23, 24, 24, 24, 25, 25, 26, 26, 26, 26, 27, 27, 27, 27, 27, 27};
        static const int THRESHOLD_64[32] = {
            2, 3, 5, 9, 13, 17, 19, 24, 29, 32, 33, 33, 35, 37, 39, 40,
            42, 43, 44, 45, 46, 47, 48, 48, 49, 50, 50, 51, 51, 52, 52, 52};
        static const int THRESHOLD_128[32] = {
            2, 3, 5, 9, 17, 22, 33, 33, 43, 52, 59, 64, 65, 65, 69, 72,
            76, 79, 81, 84, 86, 88, 90, 91, 93, 94, 95, 96, 98, 99, 100, 100};
        static const int THRESHOLD_256[32] = {
            2, 3, 5, 9, 17, 33, 37, 64, 65, 77, 94, 107, 119, 128, 129, 129,
            136, 143, 149, 154, 159, 163, 167, 171, 175, 178, 181, 183, 186, 188, 190, 192};
        static const int THRESHOLD_512[32] = {
            2, 3, 5, 9, 17, 33, 65, 65, 114, 129, 140, 171, 197, 220, 239, 256,
            257, 257, 270, 282, 293, 303, 312, 320, 328, 335, 342, 348, 354, 359, 364, 368};
        static const int THRESHOLD_1024[32] = {
            2, 3, 5, 9, 17, 33, 65, 128, 129, 205, 257, 257, 316, 366, 410, 448,
            482, 512, 513, 513, 537, 559, 579, 598, 615, 631, 645, 659, 671, 683, 694, 704};
        static const int THRESHOLD_2048[32] = {
            2, 3, 5, 9, 17, 33, 65, 129, 228, 257, 373, 512, 513, 586, 683, 768,
            844, 911, 971, 1024, 1025, 1025, 1069, 1110, 1147, 1182, 1214, 1244, 1272, 1298, 1322, 1344};
        static const int THRESHOLD_4096[32] = {
            2, 3, 5, 9, 17, 33, 65, 129, 257, 410, 513, 683, 946, 1025, 1093, 1280,
            1446, 1593, 1725, 1844, 1951, 2048, 2049, 2049, 2130, 2206, 2276, 2341, 2402, 2458, 2511, 2560};
        static const int THRESHOLD_8192[32] = {
            2, 3, 5, 9, 17, 33, 65, 129, 257, 513, 745, 1025, 1261, 1756, 2049, 2049,
            2410, 2731, 3019, 3277, 3511, 3724, 3918, 4096, 4097, 4097, 4248, 4389, 4520, 4643, 4757, 4864};
        const int *threshold = DEFAULT_THRESHOLD;
        switch (block_size) {
            case 64:
                threshold = THRESHOLD_64;
                break;
            case 128:
                threshold = THRESHOLD_128;
                break;
            case 256:
                threshold = THRESHOLD_256;
                break;
            case 512:
                threshold = THRESHOLD_512;
                break;
            case 1024:
                threshold = THRESHOLD_1024;
                break;
            case 2048:
                threshold = THRESHOLD_2048;
                break;
            case 4096:
                threshold = THRESHOLD_4096;
                break;
            case 8192:
                threshold = THRESHOLD_8192;
                break;
            default:
                break;
        }
        int idx = std::max(0, std::min(beta - 1, 31));
        return threshold[idx];
    }

    int bytes_for_bitpacking(int bit_width, int num_values) const {
        if (bit_width <= 0 || num_values <= 0) {
            return 0;
        }
        return (bit_width * num_values + 7) / 8;
    }

    uint64_t read_bits_from_value(UnsignedT value, int shift, int beta) const {
        const uint64_t mask = (beta == 64) ? ~0ULL : ((1ULL << beta) - 1ULL);
        return (static_cast<uint64_t>(value) >> shift) & mask;
    }

    int choose_beta_and_types(
        int count, int write_width, std::vector<GroupPlan> &plans) {
        if (write_width == 0) {
            plans.clear();
            return 1;
        }
        const int length_bit_width = cal_bit_width(static_cast<UnsignedT>(count));
        const int max_bits = static_cast<int>(sizeof(T) * 8);
        int bpe_cost_single[64] = {0};
        int rle_cost_single[64] = {0};
        int de_cost_single[64] = {0};
        int default_type[64] = {0};
        int candidate_type[64] = {0};

        int cost1 = 0;
        for (int bit = 0; bit < write_width; ++bit) {
            int current = static_cast<int>(read_bits_from_value(delta_arr_[0], bit, 1));
            bool has_one = (current == 1);
            int run_count = 1;
            bool changed = false;
            for (int i = 1; i < count; ++i) {
                int v = static_cast<int>(read_bits_from_value(delta_arr_[i], bit, 1));
                if (v == 1) {
                    has_one = true;
                }
                if (v != current) {
                    run_count++;
                    current = v;
                    changed = true;
                }
            }
            bpe_cost_single[bit] = has_one ? count : 0;
            rle_cost_single[bit] = run_count * (1 + length_bit_width);
            de_cost_single[bit] = changed ? count * 2 + 2 : count + 2;

            if (bpe_cost_single[bit] <= rle_cost_single[bit] &&
                bpe_cost_single[bit] <= de_cost_single[bit]) {
                default_type[bit] = 0;
                cost1 += bpe_cost_single[bit];
            } else if (rle_cost_single[bit] < bpe_cost_single[bit] &&
                       rle_cost_single[bit] <= de_cost_single[bit]) {
                default_type[bit] = 1;
                cost1 += rle_cost_single[bit];
            } else {
                default_type[bit] = 2;
                cost1 += de_cost_single[bit];
            }
        }

        int best_beta = 1;
        int best_cost = cost1;
        int best_groups = write_width;
        plans.assign(best_groups, GroupPlan());
        for (int i = 0; i < best_groups; ++i) {
            plans[i].type = default_type[i];
        }

        for (int b = 0; b < BETA_LIST_SIZE; ++b) {
            const int beta = BETA_LIST[b];
            if (beta > write_width) {
                continue;
            }
            const int groups = (write_width + beta - 1) / beta;
            int cost = 0;
            std::fill(candidate_type, candidate_type + groups, 0);
            for (int g = 0; g < groups; ++g) {
                const int group_start = g * beta;
                const int group_end = std::min(write_width, group_start + beta);
                int beta_start = group_end - 1;
                while (beta_start >= group_start && bpe_cost_single[beta_start] == 0) {
                    beta_start--;
                }
                if (beta_start < group_start) {
                    beta_start = group_start;
                }
                int current_cost =
                    bpe_cost_single[beta_start] * (beta_start - group_start + 1);

                int rle_cost_max = 0;
                for (int j = group_start; j < group_end; ++j) {
                    rle_cost_max = std::max(rle_cost_max, rle_cost_single[j]);
                }
                if (rle_cost_max < current_cost) {
                    int run_count = 1;
                    int prev = static_cast<int>(read_bits_from_value(delta_arr_[0], group_start, beta));
                    for (int i = 1; i < count; ++i) {
                        int cur =
                            static_cast<int>(read_bits_from_value(delta_arr_[i], group_start, beta));
                        if (cur != prev) {
                            run_count++;
                            prev = cur;
                        }
                    }
                    int rle_cost = run_count * (beta + length_bit_width);
                    if (rle_cost < current_cost) {
                        current_cost = rle_cost;
                        candidate_type[g] = 1;
                    }
                }

                int de_cost_max = 0;
                for (int j = group_start; j < group_end; ++j) {
                    de_cost_max = std::max(de_cost_max, de_cost_single[j]);
                }
                if (de_cost_max < current_cost) {
                    const int threshold = get_threshold(BLOCK_SIZE, beta);
                    bool seen[16] = {false};
                    int distinct = 0;
                    for (int i = 0; i < count; ++i) {
                        int cur =
                            static_cast<int>(read_bits_from_value(delta_arr_[i], group_start, beta));
                        if (!seen[cur]) {
                            seen[cur] = true;
                            distinct++;
                            if (distinct >= threshold) {
                                break;
                            }
                        }
                    }
                    if (distinct < threshold) {
                        int de_cost =
                            count * cal_bit_width(static_cast<UnsignedT>(distinct)) +
                            distinct * beta;
                        if (de_cost < current_cost) {
                            current_cost = de_cost;
                            candidate_type[g] = 2;
                        }
                    }
                }
                cost += current_cost;
            }

            if (cost < best_cost) {
                best_cost = cost;
                best_beta = beta;
                best_groups = groups;
                plans.assign(best_groups, GroupPlan());
                for (int i = 0; i < best_groups; ++i) {
                    plans[i].type = candidate_type[i];
                }
            }
        }

        (void)max_bits;
        return best_beta;
    }

    void write_bits_to_stream(uint64_t value, int bits, common::ByteStream &out_stream) {
        write_bits(value, bits, out_stream);
    }

    void pack_values(
        const uint16_t *vals, int count, int bit_width, common::ByteStream &out_stream) {
        if (bit_width <= 0) {
            return;
        }
        for (int i = 0; i < count; ++i) {
            write_bits_to_stream(static_cast<uint64_t>(vals[i]), bit_width, out_stream);
        }
    }

    bool encode_adaptive_subcolumns(
        common::ByteStream &out_stream, int count, int write_width) {
        std::vector<GroupPlan> plans;
        const int beta = choose_beta_and_types(count, write_width, plans);
        const int groups = static_cast<int>(plans.size());
        if (groups <= 0) {
            return true;
        }
        const int mask = (1 << beta) - 1;
        for (int g = 0; g < groups; ++g) {
            const int shift = g * beta;
            uint64_t max_part = 0;
            for (int i = 0; i < count; ++i) {
                uint64_t v = static_cast<uint64_t>(
                    (delta_arr_[i] >> shift) & static_cast<UnsignedT>(mask));
                if (v > max_part) {
                    max_part = v;
                }
            }
            plans[g].bit_width = cal_bit_width(static_cast<UnsignedT>(max_part));
        }

        common::SerializationUtil::write_ui8(static_cast<uint8_t>(beta), out_stream);
        for (int g = 0; g < groups; ++g) {
            common::SerializationUtil::write_ui8(
                static_cast<uint8_t>(plans[g].bit_width), out_stream);
        }

        bits_left_ = 8;
        buffer_ = 0;
        for (int g = 0; g < groups; ++g) {
            write_bits_to_stream(static_cast<uint64_t>(plans[g].type), 2, out_stream);
        }
        flush_remaining(out_stream);

        const int run_len_bw = cal_bit_width(static_cast<UnsignedT>(count));
        for (int g = 0; g < groups; ++g) {
            const int shift = g * beta;
            uint16_t values[BLOCK_SIZE] = {0};
            for (int i = 0; i < count; ++i) {
                values[i] = static_cast<uint16_t>(
                    read_bits_from_value(delta_arr_[i], shift, beta));
            }

            if (plans[g].type == 0) {
                bits_left_ = 8;
                buffer_ = 0;
                pack_values(values, count, plans[g].bit_width, out_stream);
                flush_remaining(out_stream);
                continue;
            }

            if (plans[g].type == 1) {
                uint16_t run_ends[BLOCK_SIZE];
                uint16_t run_vals[BLOCK_SIZE];
                int run_count = 0;
                uint16_t prev = values[0];
                for (int i = 1; i < count; ++i) {
                    if (values[i] != prev) {
                        run_ends[run_count] = static_cast<uint16_t>(i);
                        run_vals[run_count] = prev;
                        run_count++;
                        prev = values[i];
                    }
                }
                run_ends[run_count] = static_cast<uint16_t>(count);
                run_vals[run_count] = prev;
                run_count++;
                common::SerializationUtil::write_ui16(
                    static_cast<uint16_t>(run_count), out_stream);

                bits_left_ = 8;
                buffer_ = 0;
                pack_values(run_ends, run_count, run_len_bw, out_stream);
                pack_values(run_vals, run_count, plans[g].bit_width, out_stream);
                flush_remaining(out_stream);
                continue;
            }

            bool seen[16] = {false};
            int cardinality = 0;
            for (int i = 0; i < count; ++i) {
                uint16_t v = values[i];
                if (!seen[v]) {
                    seen[v] = true;
                    cardinality++;
                }
            }
            uint16_t dict[16];
            uint16_t mapping[16] = {0};
            int dict_size = 0;
            for (int v = 0; v < 16; ++v) {
                if (seen[v]) {
                    dict[dict_size] = static_cast<uint16_t>(v);
                    mapping[v] = static_cast<uint16_t>(dict_size);
                    dict_size++;
                }
            }
            uint16_t codes[BLOCK_SIZE];
            for (int i = 0; i < count; ++i) {
                codes[i] = mapping[values[i]];
            }
            common::SerializationUtil::write_ui16(
                static_cast<uint16_t>(cardinality), out_stream);
            const int code_bw =
                cal_bit_width(static_cast<UnsignedT>(std::max(0, cardinality - 1)));

            bits_left_ = 8;
            buffer_ = 0;
            pack_values(dict, cardinality, plans[g].bit_width, out_stream);
            pack_values(codes, count, code_bw, out_stream);
            flush_remaining(out_stream);
        }
        return true;
    }

    void write_bits(uint64_t value, int bits, common::ByteStream &out_stream) {
        while (bits > 0) {
            const int take = std::min(bits_left_, bits);
            const int shift = bits - take;
            const uint64_t mask = (take == 64) ? ~0ULL : ((1ULL << take) - 1ULL);
            const uint8_t chunk =
                static_cast<uint8_t>((value >> shift) & mask);
            buffer_ |= static_cast<uint8_t>(chunk << (bits_left_ - take));
            bits_left_ -= take;
            bits -= take;
            if (bits_left_ == 0) {
                out_stream.write_buf(&buffer_, 1);
                buffer_ = 0;
                bits_left_ = 8;
            }
        }
    }

    void flush_remaining(common::ByteStream &out_stream) {
        if (bits_left_ != 8) {
            out_stream.write_buf(&buffer_, 1);
            buffer_ = 0;
            bits_left_ = 8;
        }
    }
};

class FloatSubcolumnEncoder : public SubcolumnEncoder<int32_t> {
   public:
    int encode(bool value, common::ByteStream &out_stream) override {
        (void)value;
        (void)out_stream;
        return common::E_TYPE_NOT_MATCH;
    }
    int encode(int32_t value, common::ByteStream &out_stream) override {
        (void)value;
        (void)out_stream;
        return common::E_TYPE_NOT_MATCH;
    }
    int encode(int64_t value, common::ByteStream &out_stream) override {
        (void)value;
        (void)out_stream;
        return common::E_TYPE_NOT_MATCH;
    }
    int encode(float value, common::ByteStream &out_stream) override {
        return do_encode(common::float_to_int(value), out_stream);
    }
    int encode(double value, common::ByteStream &out_stream) override {
        (void)value;
        (void)out_stream;
        return common::E_TYPE_NOT_MATCH;
    }
    int encode(common::String value, common::ByteStream &out_stream) override {
        (void)value;
        (void)out_stream;
        return common::E_TYPE_NOT_MATCH;
    }
};

class DoubleSubcolumnEncoder : public SubcolumnEncoder<int64_t> {
   public:
    int encode(bool value, common::ByteStream &out_stream) override {
        (void)value;
        (void)out_stream;
        return common::E_TYPE_NOT_MATCH;
    }
    int encode(int32_t value, common::ByteStream &out_stream) override {
        (void)value;
        (void)out_stream;
        return common::E_TYPE_NOT_MATCH;
    }
    int encode(int64_t value, common::ByteStream &out_stream) override {
        (void)value;
        (void)out_stream;
        return common::E_TYPE_NOT_MATCH;
    }
    int encode(float value, common::ByteStream &out_stream) override {
        (void)value;
        (void)out_stream;
        return common::E_TYPE_NOT_MATCH;
    }
    int encode(double value, common::ByteStream &out_stream) override {
        return do_encode(common::double_to_long(value), out_stream);
    }
    int encode(common::String value, common::ByteStream &out_stream) override {
        (void)value;
        (void)out_stream;
        return common::E_TYPE_NOT_MATCH;
    }
};

typedef SubcolumnEncoder<int32_t> IntSubcolumnEncoder;
typedef SubcolumnEncoder<int64_t> LongSubcolumnEncoder;

template <typename T>
constexpr int SubcolumnEncoder<T>::BETA_LIST[SubcolumnEncoder<T>::BETA_LIST_SIZE];

template <>
inline int IntSubcolumnEncoder::encode(int32_t value, common::ByteStream &out_stream) {
    return do_encode(value, out_stream);
}
template <>
inline int LongSubcolumnEncoder::encode(int64_t value, common::ByteStream &out_stream) {
    return do_encode(value, out_stream);
}

}  // namespace storage

#endif  // ENCODING_SUBCOLUMN_ENCODER_H
