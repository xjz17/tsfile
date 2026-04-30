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

#ifndef ENCODING_BITPACKING_DECODER_H
#define ENCODING_BITPACKING_DECODER_H

#include <cstdint>
#include <vector>

#include "common/allocator/byte_stream.h"
#include "decoder.h"
#include "int32_packer.h"
#include "int64_packer.h"

namespace storage {

class Int32BitpackingDecoder : public Decoder {
   public:
    Int32BitpackingDecoder() : read_index_(0), block_count_(0) {}

    void reset() override {
        read_index_ = 0;
        block_count_ = 0;
    }

    bool has_remaining(const common::ByteStream& in) override {
        return read_index_ < block_count_ || in.has_remaining();
    }

    int read_boolean(bool& ret_value, common::ByteStream& in) override {
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

    int read_int32(int32_t& ret_value, common::ByteStream& in) override {
        int ret = ensure_block(in);
        if (ret != common::E_OK) {
            return ret;
        }
        ret_value = dezigzag(static_cast<uint32_t>(block_values_[read_index_++]));
        return common::E_OK;
    }

   private:
    int32_t block_values_[8] = {0};
    int read_index_;
    int block_count_;

    static int32_t dezigzag(uint32_t value) {
        int32_t v = static_cast<int32_t>(value >> 1);
        if ((value & 1U) != 0) {
            v = ~v;
        }
        return v;
    }

    int ensure_block(common::ByteStream& in) {
        if (read_index_ < block_count_) {
            return common::E_OK;
        }

        uint8_t count_u8 = 0;
        uint8_t bw_u8 = 0;
        if (common::SerializationUtil::read_ui8(count_u8, in) != common::E_OK) {
            return common::E_PARTIAL_READ;
        }
        if (common::SerializationUtil::read_ui8(bw_u8, in) != common::E_OK) {
            return common::E_PARTIAL_READ;
        }
        if (count_u8 == 0 || count_u8 > 8) {
            return common::E_DECODE_ERR;
        }
        const int bw = static_cast<int>(bw_u8);
        if (bw < 0 || bw > 32) {
            return common::E_DECODE_ERR;
        }

        int32_t tmp[8] = {0};
        if (bw > 0) {
            std::vector<uint8_t> bytes(bw);
            uint32_t read_len = 0;
            int ret = in.read_buf(bytes.data(), static_cast<uint32_t>(bytes.size()),
                                  read_len);
            if (ret != common::E_OK || read_len != bytes.size()) {
                return common::E_PARTIAL_READ;
            }
            Int32Packer packer(bw);
            packer.unpack_8values(bytes.data(), 0, tmp);
        }
        for (int i = 0; i < 8; ++i) {
            block_values_[i] = tmp[i];
        }
        block_count_ = static_cast<int>(count_u8);
        read_index_ = 0;
        return common::E_OK;
    }
};

class Int64BitpackingDecoder : public Decoder {
   public:
    Int64BitpackingDecoder() : read_index_(0), block_count_(0) {}

    void reset() override {
        read_index_ = 0;
        block_count_ = 0;
    }

    bool has_remaining(const common::ByteStream& in) override {
        return read_index_ < block_count_ || in.has_remaining();
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

    int read_int64(int64_t& ret_value, common::ByteStream& in) override {
        int ret = ensure_block(in);
        if (ret != common::E_OK) {
            return ret;
        }
        ret_value = dezigzag(static_cast<uint64_t>(block_values_[read_index_++]));
        return common::E_OK;
    }

   private:
    int64_t block_values_[8] = {0};
    int read_index_;
    int block_count_;

    static int64_t dezigzag(uint64_t value) {
        int64_t v = static_cast<int64_t>(value >> 1);
        if ((value & 1ULL) != 0) {
            v = ~v;
        }
        return v;
    }

    int ensure_block(common::ByteStream& in) {
        if (read_index_ < block_count_) {
            return common::E_OK;
        }

        uint8_t count_u8 = 0;
        uint8_t bw_u8 = 0;
        if (common::SerializationUtil::read_ui8(count_u8, in) != common::E_OK) {
            return common::E_PARTIAL_READ;
        }
        if (common::SerializationUtil::read_ui8(bw_u8, in) != common::E_OK) {
            return common::E_PARTIAL_READ;
        }
        if (count_u8 == 0 || count_u8 > 8) {
            return common::E_DECODE_ERR;
        }
        const int bw = static_cast<int>(bw_u8);
        if (bw < 0 || bw > 64) {
            return common::E_DECODE_ERR;
        }

        int64_t tmp[8] = {0};
        if (bw > 0) {
            std::vector<uint8_t> bytes(bw);
            uint32_t read_len = 0;
            int ret = in.read_buf(bytes.data(), static_cast<uint32_t>(bytes.size()),
                                  read_len);
            if (ret != common::E_OK || read_len != bytes.size()) {
                return common::E_PARTIAL_READ;
            }
            Int64Packer packer(bw);
            packer.unpack_8values(bytes.data(), 0, tmp);
        }
        for (int i = 0; i < 8; ++i) {
            block_values_[i] = tmp[i];
        }
        block_count_ = static_cast<int>(count_u8);
        read_index_ = 0;
        return common::E_OK;
    }
};

class FloatBitpackingDecoder : public Decoder {
   public:
    FloatBitpackingDecoder() : read_index_(0), block_count_(0) {}

    void reset() override {
        read_index_ = 0;
        block_count_ = 0;
    }

    bool has_remaining(const common::ByteStream& in) override {
        return read_index_ < block_count_ || in.has_remaining();
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

    int read_float(float& ret_value, common::ByteStream& in) override {
        int ret = ensure_block(in);
        if (ret != common::E_OK) {
            return ret;
        }
        ret_value = common::int_to_float(block_values_[read_index_++]);
        return common::E_OK;
    }

   private:
    int32_t block_values_[8] = {0};
    int read_index_;
    int block_count_;

    int ensure_block(common::ByteStream& in) {
        if (read_index_ < block_count_) {
            return common::E_OK;
        }

        uint8_t count_u8 = 0;
        uint8_t bw_u8 = 0;
        if (common::SerializationUtil::read_ui8(count_u8, in) != common::E_OK) {
            return common::E_PARTIAL_READ;
        }
        if (common::SerializationUtil::read_ui8(bw_u8, in) != common::E_OK) {
            return common::E_PARTIAL_READ;
        }
        if (count_u8 == 0 || count_u8 > 8) {
            return common::E_DECODE_ERR;
        }
        const int bw = static_cast<int>(bw_u8);
        if (bw < 0 || bw > 32) {
            return common::E_DECODE_ERR;
        }

        int32_t tmp[8] = {0};
        if (bw > 0) {
            std::vector<uint8_t> bytes(bw);
            uint32_t read_len = 0;
            int ret = in.read_buf(bytes.data(), static_cast<uint32_t>(bytes.size()),
                                  read_len);
            if (ret != common::E_OK || read_len != bytes.size()) {
                return common::E_PARTIAL_READ;
            }
            Int32Packer packer(bw);
            packer.unpack_8values(bytes.data(), 0, tmp);
        }
        for (int i = 0; i < 8; ++i) {
            block_values_[i] = tmp[i];
        }
        block_count_ = static_cast<int>(count_u8);
        read_index_ = 0;
        return common::E_OK;
    }
};

class DoubleBitpackingDecoder : public Decoder {
   public:
    DoubleBitpackingDecoder() : read_index_(0), block_count_(0) {}

    void reset() override {
        read_index_ = 0;
        block_count_ = 0;
    }

    bool has_remaining(const common::ByteStream& in) override {
        return read_index_ < block_count_ || in.has_remaining();
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
    int read_String(common::String& ret_value, common::PageArena& pa,
                    common::ByteStream& in) override {
        (void)ret_value;
        (void)pa;
        (void)in;
        return common::E_TYPE_NOT_MATCH;
    }

    int read_double(double& ret_value, common::ByteStream& in) override {
        int ret = ensure_block(in);
        if (ret != common::E_OK) {
            return ret;
        }
        ret_value = common::long_to_double(block_values_[read_index_++]);
        return common::E_OK;
    }

   private:
    int64_t block_values_[8] = {0};
    int read_index_;
    int block_count_;

    int ensure_block(common::ByteStream& in) {
        if (read_index_ < block_count_) {
            return common::E_OK;
        }

        uint8_t count_u8 = 0;
        uint8_t bw_u8 = 0;
        if (common::SerializationUtil::read_ui8(count_u8, in) != common::E_OK) {
            return common::E_PARTIAL_READ;
        }
        if (common::SerializationUtil::read_ui8(bw_u8, in) != common::E_OK) {
            return common::E_PARTIAL_READ;
        }
        if (count_u8 == 0 || count_u8 > 8) {
            return common::E_DECODE_ERR;
        }
        const int bw = static_cast<int>(bw_u8);
        if (bw < 0 || bw > 64) {
            return common::E_DECODE_ERR;
        }

        int64_t tmp[8] = {0};
        if (bw > 0) {
            std::vector<uint8_t> bytes(bw);
            uint32_t read_len = 0;
            int ret = in.read_buf(bytes.data(), static_cast<uint32_t>(bytes.size()),
                                  read_len);
            if (ret != common::E_OK || read_len != bytes.size()) {
                return common::E_PARTIAL_READ;
            }
            Int64Packer packer(bw);
            packer.unpack_8values(bytes.data(), 0, tmp);
        }
        for (int i = 0; i < 8; ++i) {
            block_values_[i] = tmp[i];
        }
        block_count_ = static_cast<int>(count_u8);
        read_index_ = 0;
        return common::E_OK;
    }
};

}  // namespace storage

#endif  // ENCODING_BITPACKING_DECODER_H
