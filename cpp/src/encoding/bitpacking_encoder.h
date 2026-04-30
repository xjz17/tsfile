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

#ifndef ENCODING_BITPACKING_ENCODER_H
#define ENCODING_BITPACKING_ENCODER_H

#include <cstdint>
#include <vector>

#include "common/allocator/byte_stream.h"
#include "encoder.h"
#include "int32_packer.h"
#include "int64_packer.h"

namespace storage {

class Int32BitpackingEncoder : public Encoder {
   public:
    Int32BitpackingEncoder() : count_(0) {}

    void reset() override { count_ = 0; }
    void destroy() override {}

    int encode(bool value, common::ByteStream& out_stream) override {
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

    int encode(int32_t value, common::ByteStream& out_stream) override {
        values_[count_++] = value;
        if (count_ == 8) {
            return flush_block(out_stream, count_);
        }
        return common::E_OK;
    }

    int flush(common::ByteStream& out_stream) override {
        if (count_ > 0) {
            return flush_block(out_stream, count_);
        }
        return common::E_OK;
    }

    int get_max_byte_size() override { return 2 + 8 * sizeof(int32_t); }

   private:
    int32_t values_[8] = {0};
    int count_;

    static uint32_t zigzag(int32_t value) {
        uint32_t encoded = static_cast<uint32_t>(value) << 1;
        if (value < 0) {
            encoded = ~encoded;
        }
        return encoded;
    }

    static int bit_width(uint32_t v) {
        int w = 0;
        while (v > 0) {
            ++w;
            v >>= 1U;
        }
        return w;
    }

    int flush_block(common::ByteStream& out_stream, int count) {
        int32_t tmp[8] = {0};
        uint32_t maxv = 0;
        for (int i = 0; i < count; ++i) {
            const uint32_t zz = zigzag(values_[i]);
            tmp[i] = static_cast<int32_t>(zz);
            if (zz > maxv) {
                maxv = zz;
            }
        }
        const int bw = bit_width(maxv);
        if (common::SerializationUtil::write_ui8(static_cast<uint8_t>(count),
                                                 out_stream) != common::E_OK) {
            return common::E_FILE_WRITE_ERR;
        }
        if (common::SerializationUtil::write_ui8(static_cast<uint8_t>(bw),
                                                 out_stream) != common::E_OK) {
            return common::E_FILE_WRITE_ERR;
        }
        if (bw > 0) {
            Int32Packer packer(bw);
            std::vector<uint8_t> bytes(bw);
            packer.pack_8values(tmp, 0, bytes.data());
            if (out_stream.write_buf(reinterpret_cast<const char*>(bytes.data()),
                                     static_cast<uint32_t>(bytes.size())) !=
                common::E_OK) {
                return common::E_FILE_WRITE_ERR;
            }
        }
        count_ = 0;
        return common::E_OK;
    }
};

class Int64BitpackingEncoder : public Encoder {
   public:
    Int64BitpackingEncoder() : count_(0) {}

    void reset() override { count_ = 0; }
    void destroy() override {}

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

    int encode(int64_t value, common::ByteStream& out_stream) override {
        values_[count_++] = value;
        if (count_ == 8) {
            return flush_block(out_stream, count_);
        }
        return common::E_OK;
    }

    int flush(common::ByteStream& out_stream) override {
        if (count_ > 0) {
            return flush_block(out_stream, count_);
        }
        return common::E_OK;
    }

    int get_max_byte_size() override { return 2 + 8 * sizeof(int64_t); }

   private:
    int64_t values_[8] = {0};
    int count_;

    static uint64_t zigzag(int64_t value) {
        uint64_t encoded = static_cast<uint64_t>(value) << 1;
        if (value < 0) {
            encoded = ~encoded;
        }
        return encoded;
    }

    static int bit_width(uint64_t v) {
        int w = 0;
        while (v > 0) {
            ++w;
            v >>= 1U;
        }
        return w;
    }

    int flush_block(common::ByteStream& out_stream, int count) {
        int64_t tmp[8] = {0};
        uint64_t maxv = 0;
        for (int i = 0; i < count; ++i) {
            const uint64_t zz = zigzag(values_[i]);
            tmp[i] = static_cast<int64_t>(zz);
            if (zz > maxv) {
                maxv = zz;
            }
        }
        const int bw = bit_width(maxv);
        if (common::SerializationUtil::write_ui8(static_cast<uint8_t>(count),
                                                 out_stream) != common::E_OK) {
            return common::E_FILE_WRITE_ERR;
        }
        if (common::SerializationUtil::write_ui8(static_cast<uint8_t>(bw),
                                                 out_stream) != common::E_OK) {
            return common::E_FILE_WRITE_ERR;
        }
        if (bw > 0) {
            Int64Packer packer(bw);
            std::vector<uint8_t> bytes(bw);
            packer.pack_8values(tmp, 0, bytes.data());
            if (out_stream.write_buf(reinterpret_cast<const char*>(bytes.data()),
                                     static_cast<uint32_t>(bytes.size())) !=
                common::E_OK) {
                return common::E_FILE_WRITE_ERR;
            }
        }
        count_ = 0;
        return common::E_OK;
    }
};

class FloatBitpackingEncoder : public Encoder {
   public:
    FloatBitpackingEncoder() : count_(0) {}

    void reset() override { count_ = 0; }
    void destroy() override {}

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

    int encode(float value, common::ByteStream& out_stream) override {
        values_[count_++] = common::float_to_int(value);
        if (count_ == 8) {
            return flush_block(out_stream, count_);
        }
        return common::E_OK;
    }

    int flush(common::ByteStream& out_stream) override {
        if (count_ > 0) {
            return flush_block(out_stream, count_);
        }
        return common::E_OK;
    }

    int get_max_byte_size() override { return 2 + 8 * sizeof(int32_t); }

   private:
    int32_t values_[8] = {0};
    int count_;

    static int bit_width(uint32_t v) {
        int w = 0;
        while (v > 0) {
            ++w;
            v >>= 1U;
        }
        return w;
    }

    int flush_block(common::ByteStream& out_stream, int count) {
        int32_t tmp[8] = {0};
        uint32_t maxv = 0;
        for (int i = 0; i < count; ++i) {
            const uint32_t bits = static_cast<uint32_t>(values_[i]);
            tmp[i] = values_[i];
            if (bits > maxv) {
                maxv = bits;
            }
        }
        const int bw = bit_width(maxv);
        if (common::SerializationUtil::write_ui8(static_cast<uint8_t>(count),
                                                 out_stream) != common::E_OK) {
            return common::E_FILE_WRITE_ERR;
        }
        if (common::SerializationUtil::write_ui8(static_cast<uint8_t>(bw),
                                                 out_stream) != common::E_OK) {
            return common::E_FILE_WRITE_ERR;
        }
        if (bw > 0) {
            Int32Packer packer(bw);
            std::vector<uint8_t> bytes(bw);
            packer.pack_8values(tmp, 0, bytes.data());
            if (out_stream.write_buf(reinterpret_cast<const char*>(bytes.data()),
                                     static_cast<uint32_t>(bytes.size())) !=
                common::E_OK) {
                return common::E_FILE_WRITE_ERR;
            }
        }
        count_ = 0;
        return common::E_OK;
    }
};

class DoubleBitpackingEncoder : public Encoder {
   public:
    DoubleBitpackingEncoder() : count_(0) {}

    void reset() override { count_ = 0; }
    void destroy() override {}

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
    int encode(common::String value, common::ByteStream& out_stream) override {
        (void)value;
        (void)out_stream;
        return common::E_TYPE_NOT_MATCH;
    }

    int encode(double value, common::ByteStream& out_stream) override {
        values_[count_++] = common::double_to_long(value);
        if (count_ == 8) {
            return flush_block(out_stream, count_);
        }
        return common::E_OK;
    }

    int flush(common::ByteStream& out_stream) override {
        if (count_ > 0) {
            return flush_block(out_stream, count_);
        }
        return common::E_OK;
    }

    int get_max_byte_size() override { return 2 + 8 * sizeof(int64_t); }

   private:
    int64_t values_[8] = {0};
    int count_;

    static int bit_width(uint64_t v) {
        int w = 0;
        while (v > 0) {
            ++w;
            v >>= 1U;
        }
        return w;
    }

    int flush_block(common::ByteStream& out_stream, int count) {
        int64_t tmp[8] = {0};
        uint64_t maxv = 0;
        for (int i = 0; i < count; ++i) {
            const uint64_t bits = static_cast<uint64_t>(values_[i]);
            tmp[i] = values_[i];
            if (bits > maxv) {
                maxv = bits;
            }
        }
        const int bw = bit_width(maxv);
        if (common::SerializationUtil::write_ui8(static_cast<uint8_t>(count),
                                                 out_stream) != common::E_OK) {
            return common::E_FILE_WRITE_ERR;
        }
        if (common::SerializationUtil::write_ui8(static_cast<uint8_t>(bw),
                                                 out_stream) != common::E_OK) {
            return common::E_FILE_WRITE_ERR;
        }
        if (bw > 0) {
            Int64Packer packer(bw);
            std::vector<uint8_t> bytes(bw);
            packer.pack_8values(tmp, 0, bytes.data());
            if (out_stream.write_buf(reinterpret_cast<const char*>(bytes.data()),
                                     static_cast<uint32_t>(bytes.size())) !=
                common::E_OK) {
                return common::E_FILE_WRITE_ERR;
            }
        }
        count_ = 0;
        return common::E_OK;
    }
};

}  // namespace storage

#endif  // ENCODING_BITPACKING_ENCODER_H
