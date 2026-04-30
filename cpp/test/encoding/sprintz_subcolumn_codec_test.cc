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

#include "encoding/sprintz_subcolumn_decoder.h"
#include "encoding/sprintz_subcolumn_encoder.h"

namespace storage {

class SprintzSubcolumnCodecTest : public ::testing::Test {};

TEST_F(SprintzSubcolumnCodecTest, Int32RoundTrip) {
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    Int32SprintzSubcolumnEncoder encoder;
    Int32SprintzSubcolumnDecoder decoder;

    std::vector<int32_t> values;
    for (int i = 0; i < 2048; ++i) {
        values.push_back(i * 3 - 10000);
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

TEST_F(SprintzSubcolumnCodecTest, Int64RoundTrip) {
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    Int64SprintzSubcolumnEncoder encoder;
    Int64SprintzSubcolumnDecoder decoder;

    std::vector<int64_t> values;
    for (int64_t i = 0; i < 2048; ++i) {
        values.push_back(i * i - 1000000);
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

TEST_F(SprintzSubcolumnCodecTest, FloatRoundTrip) {
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    FloatSprintzSubcolumnEncoder encoder;
    FloatSprintzSubcolumnDecoder decoder;

    std::vector<float> values;
    for (int i = 0; i < 1536; ++i) {
        values.push_back((i % 113) * 0.125f - 50.0f);
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

TEST_F(SprintzSubcolumnCodecTest, DoubleRoundTrip) {
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    DoubleSprintzSubcolumnEncoder encoder;
    DoubleSprintzSubcolumnDecoder decoder;

    std::vector<double> values;
    for (int i = 0; i < 1536; ++i) {
        values.push_back((i % 211) * 0.015625 - 500.125);
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
