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

#include "encoding/subcolumn_decoder.h"
#include "encoding/subcolumn_encoder.h"

namespace storage {

class SubcolumnCodecTest : public ::testing::Test {};

TEST_F(SubcolumnCodecTest, Int64RoundTrip) {
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    LongSubcolumnEncoder encoder;
    LongSubcolumnDecoder decoder;

    const int n = 2049;
    for (int i = 0; i < n; ++i) {
        int64_t value = static_cast<int64_t>(i) * i - 123456;
        EXPECT_EQ(encoder.encode(value, stream), common::E_OK);
    }
    EXPECT_EQ(encoder.flush(stream), common::E_OK);

    for (int i = 0; i < n; ++i) {
        int64_t value = 0;
        int64_t expected = static_cast<int64_t>(i) * i - 123456;
        EXPECT_EQ(decoder.read_int64(value, stream), common::E_OK);
        EXPECT_EQ(value, expected);
    }
}

TEST_F(SubcolumnCodecTest, FloatRoundTrip) {
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    FloatSubcolumnEncoder encoder;
    FloatSubcolumnDecoder decoder;

    const int n = 1537;
    for (int i = 0; i < n; ++i) {
        float value = static_cast<float>(i % 97) * 0.125f - 64.5f;
        EXPECT_EQ(encoder.encode(value, stream), common::E_OK);
    }
    EXPECT_EQ(encoder.flush(stream), common::E_OK);

    for (int i = 0; i < n; ++i) {
        float value = 0.0f;
        float expected = static_cast<float>(i % 97) * 0.125f - 64.5f;
        EXPECT_EQ(decoder.read_float(value, stream), common::E_OK);
        EXPECT_EQ(value, expected);
    }
}

}  // namespace storage
