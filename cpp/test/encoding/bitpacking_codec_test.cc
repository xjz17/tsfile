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

#include <gtest/gtest.h>

#include <vector>

#include "encoding/bitpacking_decoder.h"
#include "encoding/bitpacking_encoder.h"

namespace storage {

class BitpackingCodecTest : public ::testing::Test {};

TEST_F(BitpackingCodecTest, Int32RoundTrip) {
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    Int32BitpackingEncoder encoder;
    Int32BitpackingDecoder decoder;

    std::vector<int32_t> values;
    for (int32_t i = 0; i < 2049; ++i) {
        values.push_back((i % 2 == 0) ? (i * 9) : (-i * 7));
    }

    for (const auto value : values) {
        ASSERT_EQ(common::E_OK, encoder.encode(value, stream));
    }
    ASSERT_EQ(common::E_OK, encoder.flush(stream));

    for (const auto expected : values) {
        int32_t actual = 0;
        ASSERT_EQ(common::E_OK, decoder.read_int32(actual, stream));
        ASSERT_EQ(expected, actual);
    }
}

TEST_F(BitpackingCodecTest, Int64RoundTrip) {
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    Int64BitpackingEncoder encoder;
    Int64BitpackingDecoder decoder;

    std::vector<int64_t> values;
    for (int64_t i = 0; i < 1541; ++i) {
        values.push_back((i % 3 == 0) ? (i * i * 11) : (-i * 12345));
    }

    for (const auto value : values) {
        ASSERT_EQ(common::E_OK, encoder.encode(value, stream));
    }
    ASSERT_EQ(common::E_OK, encoder.flush(stream));

    for (const auto expected : values) {
        int64_t actual = 0;
        ASSERT_EQ(common::E_OK, decoder.read_int64(actual, stream));
        ASSERT_EQ(expected, actual);
    }
}

TEST_F(BitpackingCodecTest, FloatRoundTrip) {
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    FloatBitpackingEncoder encoder;
    FloatBitpackingDecoder decoder;

    std::vector<float> values;
    for (int i = 0; i < 1537; ++i) {
        values.push_back(((i % 97) - 31) * 0.125f);
    }

    for (const auto value : values) {
        ASSERT_EQ(common::E_OK, encoder.encode(value, stream));
    }
    ASSERT_EQ(common::E_OK, encoder.flush(stream));

    for (const auto expected : values) {
        float actual = 0;
        ASSERT_EQ(common::E_OK, decoder.read_float(actual, stream));
        ASSERT_FLOAT_EQ(expected, actual);
    }
}

TEST_F(BitpackingCodecTest, DoubleRoundTrip) {
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    DoubleBitpackingEncoder encoder;
    DoubleBitpackingDecoder decoder;

    std::vector<double> values;
    for (int i = 0; i < 1539; ++i) {
        values.push_back(((i % 211) - 105) * 0.015625);
    }

    for (const auto value : values) {
        ASSERT_EQ(common::E_OK, encoder.encode(value, stream));
    }
    ASSERT_EQ(common::E_OK, encoder.flush(stream));

    for (const auto expected : values) {
        double actual = 0;
        ASSERT_EQ(common::E_OK, decoder.read_double(actual, stream));
        ASSERT_DOUBLE_EQ(expected, actual);
    }
}

}  // namespace storage
