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
#include "compress/zstd_compressor.h"

#include <gtest/gtest.h>

#include <random>
#include <string>
#include <vector>

namespace {

class ZSTDTest : public ::testing::Test {
   protected:
    std::string RandomString(int length) {
        static std::random_device rd;
        static std::mt19937 generator(rd());
        static std::uniform_int_distribution<> dis(33, 127);

        std::string result;
        result.reserve(length);
        for (int i = 0; i < length; ++i) {
            result.push_back(static_cast<char>(dis(generator)));
        }
        return result;
    }
};

TEST_F(ZSTDTest, CompressAndDecompress) {
    std::string input = RandomString(500000);
    std::vector<char> uncompressed(input.begin(), input.end());

    storage::ZSTDCompressor compressor;
    compressor.reset(true);

    char* compressed_buf = nullptr;
    uint32_t compressed_buf_len = 0;
    ASSERT_EQ(common::E_OK,
              compressor.compress(uncompressed.data(), uncompressed.size(),
                                  compressed_buf, compressed_buf_len));
    ASSERT_GT(compressed_buf_len, 0u);

    char* decompressed_buf = nullptr;
    uint32_t decompressed_buf_len = 0;
    compressor.reset(false);
    ASSERT_EQ(common::E_OK,
              compressor.uncompress(compressed_buf, compressed_buf_len,
                                    decompressed_buf, decompressed_buf_len));

    std::vector<char> decompressed(decompressed_buf,
                                   decompressed_buf + decompressed_buf_len);
    EXPECT_EQ(uncompressed, decompressed);

    compressor.after_compress(compressed_buf);
    compressor.after_uncompress(decompressed_buf);
}

}  // namespace
