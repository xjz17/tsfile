/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License"); you may not use this file except in compliance
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

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <string>
#include <vector>

#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>

#include "encoding/subcolumn_decoder.h"
#include "encoding/subcolumn_encoder.h"
#include "utils/errno_define.h"

#ifdef ENABLE_LZ4
#include "compress/lz4_compressor.h"
#endif
#ifdef ENABLE_GZIP
#include "compress/gzip_compressor.h"
#endif
#ifdef ENABLE_ZSTD
#include "compress/zstd_compressor.h"
#endif
#ifdef ENABLE_LZMA
#include "compress/lzma_compressor.h"
#endif

namespace {

/*
 * LZ4/GZIP/ZSTD/LZMA: raw payload is concatenated int64 (little-endian, native layout).
 * SUBCOLUMN: TsFile LongSubcolumn encode/decompress (same semantics as subcolumn_codec_test).
 * CSV "Compress"/"Decompress" columns mean compressor compress/uncompress for codecs,
 * and subcolumn encode/decode respectively.
 */

/** Root folder containing dataset CSV files (must end with `/` or a segment joined below). */
constexpr const char kDatasetDir[] = "/home/allen/xjz17/subcolumn/dataset_tsfile";

/** Output CSV path (parent dirs created best-effort). */
constexpr const char kResultCsvPath[] =
    "/home/allen/xjz17/subcolumn/result/compression_codec_benchmark/"
    "compress_decompress_benchmark.csv";

/** Repetitions for timing compress and decompress separately (averaged). */
constexpr int kRepeatTimes = 10;

constexpr int kMaxDecimalPrecision = 8;

constexpr uint32_t kByteStreamPageSize = 1024 * 1024;

std::string trim(const std::string &s) {
    size_t start = 0;
    while (start < s.size() &&
           std::isspace(static_cast<unsigned char>(s[start]))) {
        ++start;
    }
    size_t end = s.size();
    while (end > start &&
           std::isspace(static_cast<unsigned char>(s[end - 1]))) {
        --end;
    }
    return s.substr(start, end - start);
}

std::string first_column(const std::string &line) {
    const size_t comma_pos = line.find(',');
    if (comma_pos == std::string::npos) {
        return trim(line);
    }
    return trim(line.substr(0, comma_pos));
}

int decimal_precision(const std::string &value) {
    const size_t dot = value.find('.');
    if (dot == std::string::npos) {
        return 0;
    }
    return static_cast<int>(value.size() - dot - 1);
}

int64_t multiplier_for_precision(int precision) {
    int64_t multiplier = 1;
    for (int i = 0; i < precision; ++i) {
        multiplier *= 10;
    }
    return multiplier;
}

int64_t to_scaled_int64(const std::string &raw, int64_t multiplier) {
    const double d = std::strtod(raw.c_str(), nullptr);
    return static_cast<int64_t>(std::llround(d * static_cast<double>(multiplier)));
}

bool dir_exists(const std::string &path) {
    struct stat st {};
    return ::stat(path.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

bool ends_with(const std::string &s, const char *suffix) {
    const size_t n = s.size();
    const size_t m = std::strlen(suffix);
    return n >= m && s.compare(n - m, m, suffix) == 0;
}

std::string basename_no_ext(const std::string &path) {
    size_t sep = path.find_last_of("/\\");
    std::string name = (sep == std::string::npos) ? path : path.substr(sep + 1);
    size_t dot = name.find_last_of('.');
    if (dot == std::string::npos) {
        return name;
    }
    return name.substr(0, dot);
}

void ensure_dir_recursive(const std::string &path) {
    if (path.empty()) {
        return;
    }
    std::string cur;
    cur.reserve(path.size());
    for (size_t i = 0; i < path.size(); ++i) {
        char c = path[i];
        cur.push_back(c);
        if (c == '/') {
            if (!cur.empty() && cur != "/" && !dir_exists(cur)) {
                (void)::mkdir(cur.c_str(), 0755);
            }
        }
    }
    if (!dir_exists(cur)) {
        (void)::mkdir(cur.c_str(), 0755);
    }
}

std::vector<std::string> list_csv_files(const std::string &dir_in) {
    std::string dir = dir_in;
    if (!dir.empty() && dir.back() != '/') {
        dir.push_back('/');
    }
    std::vector<std::string> files;
    DIR *dp = ::opendir(dir.c_str());
    if (dp == nullptr) {
        return files;
    }
    struct dirent *de = nullptr;
    while ((de = ::readdir(dp)) != nullptr) {
        const std::string name = de->d_name;
        if (name == "." || name == "..") {
            continue;
        }
        if (!ends_with(name, ".csv")) {
            continue;
        }
        files.push_back(dir + name);
    }
    ::closedir(dp);
    std::sort(files.begin(), files.end());
    return files;
}

void load_scaled_int64_column(const std::string &dataset_file,
                              std::vector<int64_t> &out_values,
                              int &max_decimal_precision) {
    std::ifstream in(dataset_file.c_str());
    ASSERT_TRUE(in.good()) << "open dataset: " << dataset_file;
    max_decimal_precision = 0;
    std::string line;
    while (std::getline(in, line)) {
        const std::string value = first_column(line);
        if (value.empty()) {
            continue;
        }
        max_decimal_precision =
            std::max(max_decimal_precision, decimal_precision(value));
    }
    in.clear();
    in.seekg(0, std::ios::beg);
    const int64_t mult =
        multiplier_for_precision(std::min(max_decimal_precision, kMaxDecimalPrecision));
    while (std::getline(in, line)) {
        const std::string value = first_column(line);
        if (value.empty()) {
            continue;
        }
        out_values.push_back(to_scaled_int64(value, mult));
    }
}

void byte_stream_to_vector(common::ByteStream &s, std::vector<uint8_t> &out) {
    const uint32_t n = s.total_size();
    out.resize(n);
    uint32_t off = 0;
    while (off < n) {
        uint32_t got = 0;
        ASSERT_EQ(s.read_buf(out.data() + off, n - off, got), common::E_OK);
        ASSERT_GT(got, 0u);
        off += got;
    }
}

void append_csv_row(std::ofstream &out,
                    const std::string &dataset,
                    const std::string &algorithm,
                    size_t points,
                    int64_t raw_bytes,
                    int64_t payload_bytes,
                    double compress_ns_avg,
                    double decompress_ns_avg,
                    int max_precision) {
    out << dataset << ',' << algorithm << ',' << points << ',' << raw_bytes << ','
        << payload_bytes << ',' << std::llround(compress_ns_avg) << ','
        << std::llround(decompress_ns_avg) << ',' << max_precision << '\n';
}

template <typename CompressorT>
void benchmark_compressor_roundtrip(const std::vector<int64_t> &values,
                                      CompressorT &compressor,
                                      int repeat_times,
                                      int64_t &out_avg_compress_ns,
                                      int64_t &out_avg_decompress_ns,
                                      uint32_t &out_payload_bytes) {
    ASSERT_FALSE(values.empty());
    const uint32_t raw_len =
        static_cast<uint32_t>(values.size() * sizeof(int64_t));
    char *raw_ptr = reinterpret_cast<char *>(const_cast<int64_t *>(values.data()));

    std::vector<char> saved_compressed;
    int64_t sum_compress = 0;

    for (int r = 0; r < repeat_times; ++r) {
        ASSERT_EQ(compressor.reset(true), common::E_OK);
        char *compressed_buf = nullptr;
        uint32_t compressed_len = 0;
        const auto t0 = std::chrono::steady_clock::now();
        ASSERT_EQ(compressor.compress(raw_ptr, raw_len, compressed_buf,
                                      compressed_len),
                  common::E_OK);
        const auto t1 = std::chrono::steady_clock::now();
        sum_compress +=
            std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
        if (r == repeat_times - 1) {
            saved_compressed.assign(compressed_buf,
                                    compressed_buf + compressed_len);
            out_payload_bytes = compressed_len;
        }
        compressor.after_compress(compressed_buf);
    }

    int64_t sum_decompress = 0;

    for (int r = 0; r < repeat_times; ++r) {
        ASSERT_EQ(compressor.reset(false), common::E_OK);
        char *out_buf = nullptr;
        uint32_t out_len = 0;
        const auto t0 = std::chrono::steady_clock::now();
        ASSERT_EQ(compressor.uncompress(saved_compressed.data(),
                                          static_cast<uint32_t>(saved_compressed.size()),
                                          out_buf, out_len),
                  common::E_OK);
        const auto t1 = std::chrono::steady_clock::now();
        sum_decompress +=
            std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
        if (r == 0) {
            ASSERT_EQ(static_cast<size_t>(out_len), static_cast<size_t>(raw_len));
            ASSERT_EQ(std::memcmp(out_buf, raw_ptr, raw_len), 0);
        }
        compressor.after_uncompress(out_buf);
    }

    out_avg_compress_ns = sum_compress / repeat_times;
    out_avg_decompress_ns = sum_decompress / repeat_times;
}

void benchmark_subcolumn_roundtrip(const std::vector<int64_t> &values,
                                   int repeat_times,
                                   int64_t &out_avg_encode_ns,
                                   int64_t &out_avg_decode_ns,
                                   uint32_t &out_payload_bytes) {
    ASSERT_FALSE(values.empty());
    storage::LongSubcolumnEncoder encoder;
    storage::LongSubcolumnDecoder decoder;

    std::vector<uint8_t> blob;
    int64_t sum_enc = 0;

    for (int r = 0; r < repeat_times; ++r) {
        common::ByteStream stream(kByteStreamPageSize, common::MOD_DEFAULT);
        encoder.reset();
        const auto t0 = std::chrono::steady_clock::now();
        for (int64_t v : values) {
            ASSERT_EQ(encoder.encode(v, stream), common::E_OK);
        }
        ASSERT_EQ(encoder.flush(stream), common::E_OK);
        const auto t1 = std::chrono::steady_clock::now();
        sum_enc +=
            std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
        if (r == repeat_times - 1) {
            byte_stream_to_vector(stream, blob);
            out_payload_bytes = static_cast<uint32_t>(blob.size());
        }
    }

    int64_t sum_dec = 0;

    for (int r = 0; r < repeat_times; ++r) {
        common::ByteStream in(kByteStreamPageSize, common::MOD_DEFAULT);
        in.wrap_from(reinterpret_cast<char *>(blob.data()),
                     static_cast<int32_t>(blob.size()));
        decoder.reset();
        std::vector<int64_t> decoded;
        decoded.reserve(values.size());
        const auto t0 = std::chrono::steady_clock::now();
        for (size_t i = 0; i < values.size(); ++i) {
            int64_t v = 0;
            ASSERT_EQ(decoder.read_int64(v, in), common::E_OK);
            decoded.push_back(v);
        }
        const auto t1 = std::chrono::steady_clock::now();
        sum_dec +=
            std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
        if (r == 0) {
            ASSERT_EQ(decoded.size(), values.size());
            ASSERT_EQ(decoded, values);
        }
    }

    out_avg_encode_ns = sum_enc / repeat_times;
    out_avg_decode_ns = sum_dec / repeat_times;
}

}  // namespace

TEST(DatasetCompressCodecBench, CompressAndDecompressFromDatasetCsv) {
    const std::vector<std::string> csv_paths = list_csv_files(kDatasetDir);
    ASSERT_FALSE(csv_paths.empty())
        << "No CSV under dataset dir (check kDatasetDir): " << kDatasetDir;

    const std::string result_csv(kResultCsvPath);
    const size_t slash = result_csv.find_last_of('/');
    ASSERT_NE(slash, std::string::npos) << "kResultCsvPath must include a directory";
    ensure_dir_recursive(result_csv.substr(0, slash));

    std::ofstream out(result_csv.c_str(), std::ios::out | std::ios::trunc);
    ASSERT_TRUE(out.good()) << "open result csv: " << result_csv;
    out << "Dataset,Algorithm,Points,Raw Size Bytes,Payload Size Bytes,"
           "Compress Time Nanos Avg,Decompress Time Nanos Avg,Max Decimal Precision\n";

    for (const std::string &path : csv_paths) {
        std::vector<int64_t> values;
        int max_prec = 0;
        load_scaled_int64_column(path, values, max_prec);
        if (values.empty()) {
            continue;
        }
        const std::string dataset_name = basename_no_ext(path);
        const int64_t raw_bytes =
            static_cast<int64_t>(values.size() * sizeof(int64_t));

#ifdef ENABLE_LZ4
        {
            storage::LZ4Compressor compressor;
            int64_t c_ns = 0, d_ns = 0;
            uint32_t payload = 0;
            benchmark_compressor_roundtrip(values, compressor, kRepeatTimes, c_ns,
                                           d_ns, payload);
            append_csv_row(out, dataset_name, "LZ4", values.size(), raw_bytes,
                           payload, static_cast<double>(c_ns),
                           static_cast<double>(d_ns), max_prec);
        }
#endif
#ifdef ENABLE_GZIP
        {
            storage::GZIPCompressor compressor;
            int64_t c_ns = 0, d_ns = 0;
            uint32_t payload = 0;
            benchmark_compressor_roundtrip(values, compressor, kRepeatTimes, c_ns,
                                           d_ns, payload);
            append_csv_row(out, dataset_name, "GZIP", values.size(), raw_bytes,
                           payload, static_cast<double>(c_ns),
                           static_cast<double>(d_ns), max_prec);
        }
#endif
#ifdef ENABLE_ZSTD
        {
            storage::ZSTDCompressor compressor;
            int64_t c_ns = 0, d_ns = 0;
            uint32_t payload = 0;
            benchmark_compressor_roundtrip(values, compressor, kRepeatTimes, c_ns,
                                           d_ns, payload);
            append_csv_row(out, dataset_name, "ZSTD", values.size(), raw_bytes,
                           payload, static_cast<double>(c_ns),
                           static_cast<double>(d_ns), max_prec);
        }
#endif
#ifdef ENABLE_LZMA
        {
            storage::LZMACompressor compressor;
            int64_t c_ns = 0, d_ns = 0;
            uint32_t payload = 0;
            benchmark_compressor_roundtrip(values, compressor, kRepeatTimes, c_ns,
                                           d_ns, payload);
            append_csv_row(out, dataset_name, "LZMA", values.size(), raw_bytes,
                           payload, static_cast<double>(c_ns),
                           static_cast<double>(d_ns), max_prec);
        }
#endif
        {
            int64_t e_ns = 0, d_ns = 0;
            uint32_t payload = 0;
            benchmark_subcolumn_roundtrip(values, kRepeatTimes, e_ns, d_ns,
                                          payload);
            append_csv_row(out, dataset_name, "SUBCOLUMN", values.size(),
                           raw_bytes, payload, static_cast<double>(e_ns),
                           static_cast<double>(d_ns), max_prec);
        }
    }

    out.close();
    ASSERT_TRUE(out.good()) << "failed writing " << result_csv;
}
