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

#ifndef ENCODING_DICTIONARY_ENCODER_H
#define ENCODING_DICTIONARY_ENCODER_H

#include <map>
#include <string>
#include <vector>

#include "common/allocator/byte_stream.h"
#include "encoder.h"
#include "encoding/int32_rle_encoder.h"

namespace storage {

class DictionaryEncoder : public Encoder {
   private:
    enum class ValueKind { UNKNOWN, STRING, INT32, INT64 };

    std::map<std::string, int> string_entry_index_;
    std::vector<std::string> string_index_entry_;
    std::map<int32_t, int> int32_entry_index_;
    std::vector<int32_t> int32_index_entry_;
    std::map<int64_t, int> int64_entry_index_;
    std::vector<int64_t> int64_index_entry_;
    Int32RleEncoder values_encoder_;
    int map_size_;
    ValueKind value_kind_;

    static int get_var_int32_size(int32_t value) {
        uint32_t encoded = static_cast<uint32_t>(value) << 1;
        if (value < 0) {
            encoded = ~encoded;
        }

        int size = 1;
        while ((encoded & 0xFFFFFF80) != 0) {
            size++;
            encoded = encoded >> 7;
        }
        return size;
    }

    bool try_set_value_kind(ValueKind expected_kind) {
        if (value_kind_ == ValueKind::UNKNOWN) {
            value_kind_ = expected_kind;
            return true;
        }
        return value_kind_ == expected_kind;
    }

   public:
    DictionaryEncoder() {}
    ~DictionaryEncoder() override {}

    int encode(bool value, common::ByteStream& out_stream) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int encode(int32_t value, common::ByteStream& out_stream) override {
        if (!try_set_value_kind(ValueKind::INT32)) {
            return common::E_TYPE_NOT_MATCH;
        }

        if (int32_entry_index_.count(value) == 0) {
            int32_index_entry_.push_back(value);
            map_size_ += get_var_int32_size(value);
            int32_entry_index_[value] = int32_entry_index_.size();
        }
        values_encoder_.encode(int32_entry_index_[value], out_stream);
        return common::E_OK;
    }
    int encode(int64_t value, common::ByteStream& out_stream) override {
        if (!try_set_value_kind(ValueKind::INT64)) {
            return common::E_TYPE_NOT_MATCH;
        }

        if (int64_entry_index_.count(value) == 0) {
            int64_index_entry_.push_back(value);
            map_size_ += sizeof(int64_t);
            int64_entry_index_[value] = int64_entry_index_.size();
        }
        values_encoder_.encode(int64_entry_index_[value], out_stream);
        return common::E_OK;
    }
    int encode(float value, common::ByteStream& out_stream) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int encode(double value, common::ByteStream& out_stream) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int encode(common::String value, common::ByteStream& out_stream) override {
        encode(value.to_std_string(), out_stream);
        return common::E_OK;
    }

    void init() {
        map_size_ = 0;
        value_kind_ = ValueKind::UNKNOWN;
        values_encoder_.init();
    }

    void destroy() override {}

    void reset() override {
        string_entry_index_.clear();
        string_index_entry_.clear();
        int32_entry_index_.clear();
        int32_index_entry_.clear();
        int64_entry_index_.clear();
        int64_index_entry_.clear();
        map_size_ = 0;
        value_kind_ = ValueKind::UNKNOWN;
        values_encoder_.reset();
    }

    int encode(const char* value, common::ByteStream& out) {
        return encode(std::string(value), out);
    }

    int encode(std::string value, common::ByteStream& out) {
        if (!try_set_value_kind(ValueKind::STRING)) {
            return common::E_TYPE_NOT_MATCH;
        }

        if (string_entry_index_.count(value) == 0) {
            string_index_entry_.push_back(value);
            map_size_ = map_size_ + value.length();
            string_entry_index_[value] = string_entry_index_.size();
        }
        values_encoder_.encode(string_entry_index_[value], out);
        return common::E_OK;
    }

    int flush(common::ByteStream& out) override {
        int ret = common::E_OK;
        ret = write_map(out);
        if (ret != common::E_OK) {
            return ret;
        } else {
            write_encoded_data(out);
        }
        return ret;
    }

    int write_map(common::ByteStream& out) {
        int ret = common::E_OK;
        if (value_kind_ == ValueKind::INT32) {
            if (RET_FAIL(common::SerializationUtil::write_var_int(
                    (int)int32_index_entry_.size(), out))) {
                return ret;
            }
            for (int i = 0; i < (int)int32_index_entry_.size(); i++) {
                if (RET_FAIL(common::SerializationUtil::write_var_int(
                        int32_index_entry_[i], out))) {
                    return common::E_FILE_WRITE_ERR;
                }
            }
            return common::E_OK;
        }

        if (value_kind_ == ValueKind::INT64) {
            if (RET_FAIL(common::SerializationUtil::write_var_int(
                    (int)int64_index_entry_.size(), out))) {
                return ret;
            }
            for (int i = 0; i < (int)int64_index_entry_.size(); i++) {
                if (RET_FAIL(
                        common::SerializationUtil::write_i64(
                            int64_index_entry_[i], out))) {
                    return common::E_FILE_WRITE_ERR;
                }
            }
            return common::E_OK;
        }

        if (RET_FAIL(common::SerializationUtil::write_var_int(
                (int)string_index_entry_.size(), out))) {
            return ret;
        } else {
            for (int i = 0; i < (int)string_index_entry_.size(); i++) {
                if (RET_FAIL(common::SerializationUtil::write_var_str(
                        string_index_entry_[i], out))) {
                    return common::E_FILE_WRITE_ERR;
                }
            }
        }
        return common::E_OK;
    }

    void write_encoded_data(common::ByteStream& out) {
        values_encoder_.flush(out);
    }

    int get_max_byte_size() override {
        // 4 bytes for storing dictionary size
        return 4 + map_size_ + values_encoder_.get_max_byte_size();
    }
};

}  // end namespace storage
#endif  // ENCODING_DICTIONARY_ENCODER_H