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

#include "encoding/ts2diff_subcolumn_decoder.h"
#include "encoding/ts2diff_subcolumn_encoder.h"

namespace storage {

class TS2DIFFSubcolumnCodecTest : public ::testing::Test {};

TEST_F(TS2DIFFSubcolumnCodecTest, Int32RoundTrip) {
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    Int32TS2DIFFSubcolumnEncoder encoder;
    Int32TS2DIFFSubcolumnDecoder decoder;

    std::vector<int32_t> values;
    for (int i = 0; i < 2048; ++i) {
        values.push_back(1000 + i * 7);
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

TEST_F(TS2DIFFSubcolumnCodecTest, Int64RoundTrip) {
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    Int64TS2DIFFSubcolumnEncoder encoder;
    Int64TS2DIFFSubcolumnDecoder decoder;

    std::vector<int64_t> values;
    for (int64_t i = 0; i < 2048; ++i) {
        values.push_back(1000000000LL + i * 13);
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

TEST_F(TS2DIFFSubcolumnCodecTest, FloatRoundTrip) {
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    FloatTS2DIFFSubcolumnEncoder encoder;
    FloatTS2DIFFSubcolumnDecoder decoder;

    std::vector<float> values;
    for (int i = 0; i < 1536; ++i) {
        values.push_back(0.5f + i * 0.125f);
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

TEST_F(TS2DIFFSubcolumnCodecTest, DoubleRoundTrip) {
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    DoubleTS2DIFFSubcolumnEncoder encoder;
    DoubleTS2DIFFSubcolumnDecoder decoder;

    std::vector<double> values;
    for (int i = 0; i < 1536; ++i) {
        values.push_back(10.0 + i * 0.015625);
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
