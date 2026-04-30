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
#ifndef ENCODING_DICTIONARY_DECODER_H
#define ENCODING_DICTIONARY_DECODER_H

#include <limits>
#include <string>
#include <vector>

#include "common/allocator/byte_stream.h"
#include "decoder.h"
#include "encoder.h"
#include "encoding/int32_rle_decoder.h"

namespace storage {

class DictionaryDecoder : public Decoder {
   private:
    enum class ValueKind { UNKNOWN, STRING, INT32, INT64, FLOAT, DOUBLE };

    Int32RleDecoder value_decoder_;
    std::vector<std::string> string_entry_index_;
    std::vector<int32_t> int32_entry_index_;
    std::vector<int64_t> int64_entry_index_;
    std::vector<float> float_entry_index_;
    std::vector<double> double_entry_index_;
    ValueKind value_kind_;
    bool map_initialized_;

    bool try_set_value_kind(ValueKind expected_kind) {
        if (value_kind_ == ValueKind::UNKNOWN) {
            value_kind_ = expected_kind;
            return true;
        }
        return value_kind_ == expected_kind;
    }

    bool is_map_initialized() const { return map_initialized_; }

    static bool get_scale_factor(int scale, long double& factor) {
        if (scale < 0) {
            return false;
        }

        factor = 1.0;
        for (int i = 0; i < scale; i++) {
            factor *= 10.0;
            if (factor > std::numeric_limits<int64_t>::max()) {
                return false;
            }
        }
        return true;
    }

    int init_string_map(common::ByteStream& buffer) {
        int ret = common::E_OK;
        int length = 0;
        if (RET_FAIL(common::SerializationUtil::read_var_int(length, buffer))) {
            return common::E_PARTIAL_READ;
        }
        for (int i = 0; i < length; i++) {
            std::string str;
            if (RET_FAIL(
                    common::SerializationUtil::read_var_str(str, buffer))) {
                return common::E_PARTIAL_READ;
            }
            string_entry_index_.push_back(str);
        }
        map_initialized_ = true;
        return ret;
    }

    int init_int32_map(common::ByteStream& buffer) {
        int ret = common::E_OK;
        int length = 0;
        if (RET_FAIL(common::SerializationUtil::read_var_int(length, buffer))) {
            return common::E_PARTIAL_READ;
        }
        for (int i = 0; i < length; i++) {
            int32_t value = 0;
            if (RET_FAIL(
                    common::SerializationUtil::read_var_int(value, buffer))) {
                return common::E_PARTIAL_READ;
            }
            int32_entry_index_.push_back(value);
        }
        map_initialized_ = true;
        return ret;
    }

    int init_int64_map(common::ByteStream& buffer) {
        int ret = common::E_OK;
        int length = 0;
        if (RET_FAIL(common::SerializationUtil::read_var_int(length, buffer))) {
            return common::E_PARTIAL_READ;
        }
        for (int i = 0; i < length; i++) {
            int64_t value = 0;
            if (RET_FAIL(common::SerializationUtil::read_i64(value, buffer))) {
                return common::E_PARTIAL_READ;
            }
            int64_entry_index_.push_back(value);
        }
        map_initialized_ = true;
        return ret;
    }

    int init_float_map(common::ByteStream& buffer) {
        int ret = common::E_OK;
        int length = 0;
        int scale = 0;
        if (RET_FAIL(common::SerializationUtil::read_var_int(length, buffer))) {
            return common::E_PARTIAL_READ;
        }
        if (RET_FAIL(common::SerializationUtil::read_var_int(scale, buffer))) {
            return common::E_PARTIAL_READ;
        }
        long double factor = 1.0;
        if (!get_scale_factor(scale, factor)) {
            return common::E_OUT_OF_RANGE;
        }
        for (int i = 0; i < length; i++) {
            int64_t scaled_value = 0;
            if (RET_FAIL(
                    common::SerializationUtil::read_i64(scaled_value, buffer))) {
                return common::E_PARTIAL_READ;
            }
            float_entry_index_.push_back((float)((long double)scaled_value /
                                                 factor));
        }
        map_initialized_ = true;
        return ret;
    }

    int init_double_map(common::ByteStream& buffer) {
        int ret = common::E_OK;
        int length = 0;
        int scale = 0;
        if (RET_FAIL(common::SerializationUtil::read_var_int(length, buffer))) {
            return common::E_PARTIAL_READ;
        }
        if (RET_FAIL(common::SerializationUtil::read_var_int(scale, buffer))) {
            return common::E_PARTIAL_READ;
        }
        long double factor = 1.0;
        if (!get_scale_factor(scale, factor)) {
            return common::E_OUT_OF_RANGE;
        }
        for (int i = 0; i < length; i++) {
            int64_t scaled_value = 0;
            if (RET_FAIL(
                    common::SerializationUtil::read_i64(scaled_value, buffer))) {
                return common::E_PARTIAL_READ;
            }
            double_entry_index_.push_back((double)((long double)scaled_value /
                                                   factor));
        }
        map_initialized_ = true;
        return ret;
    }

   public:
    ~DictionaryDecoder() override = default;
    bool has_remaining(const common::ByteStream& buffer) override {
        return (is_map_initialized() && value_decoder_.has_next_package()) ||
               buffer.has_remaining();
    }
    int read_boolean(bool& ret_value, common::ByteStream& in) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int read_int32(int32_t& ret_value, common::ByteStream& in) override {
        if (!try_set_value_kind(ValueKind::INT32)) {
            return common::E_TYPE_NOT_MATCH;
        }
        if (!is_map_initialized()) {
            int ret = init_int32_map(in);
            if (ret != common::E_OK) {
                return ret;
            }
        }
        int32_t code = 0;
        int ret = value_decoder_.read_int(code, in);
        if (ret != common::E_OK) {
            return ret;
        }
        if (code < 0 || code >= (int32_t)int32_entry_index_.size()) {
            return common::E_OUT_OF_RANGE;
        }
        ret_value = int32_entry_index_[code];
        return common::E_OK;
    }
    int read_int64(int64_t& ret_value, common::ByteStream& in) override {
        if (!try_set_value_kind(ValueKind::INT64)) {
            return common::E_TYPE_NOT_MATCH;
        }
        if (!is_map_initialized()) {
            int ret = init_int64_map(in);
            if (ret != common::E_OK) {
                return ret;
            }
        }
        int32_t code = 0;
        int ret = value_decoder_.read_int(code, in);
        if (ret != common::E_OK) {
            return ret;
        }
        if (code < 0 || code >= (int32_t)int64_entry_index_.size()) {
            return common::E_OUT_OF_RANGE;
        }
        ret_value = int64_entry_index_[code];
        return common::E_OK;
    }
    int read_float(float& ret_value, common::ByteStream& in) override {
        if (!try_set_value_kind(ValueKind::FLOAT)) {
            return common::E_TYPE_NOT_MATCH;
        }
        if (!is_map_initialized()) {
            int ret = init_float_map(in);
            if (ret != common::E_OK) {
                return ret;
            }
        }
        int32_t code = 0;
        int ret = value_decoder_.read_int(code, in);
        if (ret != common::E_OK) {
            return ret;
        }
        if (code < 0 || code >= (int32_t)float_entry_index_.size()) {
            return common::E_OUT_OF_RANGE;
        }
        ret_value = float_entry_index_[code];
        return common::E_OK;
    }
    int read_double(double& ret_value, common::ByteStream& in) override {
        if (!try_set_value_kind(ValueKind::DOUBLE)) {
            return common::E_TYPE_NOT_MATCH;
        }
        if (!is_map_initialized()) {
            int ret = init_double_map(in);
            if (ret != common::E_OK) {
                return ret;
            }
        }
        int32_t code = 0;
        int ret = value_decoder_.read_int(code, in);
        if (ret != common::E_OK) {
            return ret;
        }
        if (code < 0 || code >= (int32_t)double_entry_index_.size()) {
            return common::E_OUT_OF_RANGE;
        }
        ret_value = double_entry_index_[code];
        return common::E_OK;
    }
    int read_String(common::String& ret_value, common::PageArena& pa,
                    common::ByteStream& in) override {
        if (!try_set_value_kind(ValueKind::STRING)) {
            return common::E_TYPE_NOT_MATCH;
        }
        auto std_str = read_string(in);
        return ret_value.dup_from(std_str, pa);
    }

    void init() {
        value_kind_ = ValueKind::UNKNOWN;
        map_initialized_ = false;
        value_decoder_.init();
    }

    void reset() override {
        value_decoder_.reset();
        string_entry_index_.clear();
        int32_entry_index_.clear();
        int64_entry_index_.clear();
        float_entry_index_.clear();
        double_entry_index_.clear();
        value_kind_ = ValueKind::UNKNOWN;
        map_initialized_ = false;
    }

    std::string read_string(common::ByteStream& buffer) {
        if (!try_set_value_kind(ValueKind::STRING)) {
            return "";
        }
        if (!is_map_initialized()) {
            if (init_string_map(buffer) != common::E_OK) {
                return "";
            }
        }
        int32_t code = 0;
        value_decoder_.read_int(code, buffer);
        return string_entry_index_[code];
    }

    bool has_next(common::ByteStream& buffer) {
        if (!try_set_value_kind(ValueKind::STRING)) {
            return false;
        }
        if (!is_map_initialized()) {
            if (init_string_map(buffer) != common::E_OK) {
                return false;
            }
        }
        return value_decoder_.has_next(buffer);
    }

    int init_map(common::ByteStream& buffer) {
        if (!try_set_value_kind(ValueKind::STRING)) {
            return common::E_TYPE_NOT_MATCH;
        }
        return init_string_map(buffer);
    }
};

}  // end namespace storage
#endif  // ENCODING_DICTIONARY_DECODER_H