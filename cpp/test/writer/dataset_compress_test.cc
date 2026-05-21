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
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <limits>
#include <map>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>

#if defined(_WIN32)
#include <direct.h>
#endif

namespace {
int tsfile_test_mkdir(const char *path) {
#if defined(_WIN32)
    return ::_mkdir(path);
#else
    return ::mkdir(path, 0755);
#endif
}
}  // namespace

#include "common/allocator/byte_stream.h"
#include "common/db_common.h"
#include "common/global.h"
#include "compress/compressor_factory.h"
#include "encoding/decoder_factory.h"
#include "encoding/encoder_factory.h"
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

#if defined(_WIN32)
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#ifndef NOGDI
#define NOGDI
#endif
#include <windows.h>
#endif

namespace {

// constexpr const char kDatasetDir[] = "/home/allen/xjz17/subcolumn/dataset";
// constexpr const char kDatasetDir[] = "D:/github/xjz17/subcolumn/dataset";
// constexpr const char kDatasetDir[] = "D:/github/xjz17/subcolumn/dataset_big_combined";
constexpr const char kDatasetDir[] = "E:/xjz/dataset";

constexpr const char kBinOutputDir[] =
    // "/home/allen/xjz17/subcolumn/result/encode_compress/bins";
    // "D:/github/xjz17/subcolumn/result/encode_compress/bins";
    "E:/xjz/encode_compress/bins";

constexpr const char kWriteMetricsCsvPath[] =
    // "/home/allen/xjz17/subcolumn/result/encode_compress/"
    // "encoder_compress_roundtrip_write_metrics.csv";

    // "D:/github/xjz17/subcolumn/result/encode_compress/"
    // "encoder_compress_roundtrip_write_metrics3.csv";

    "D:/github/xjz17/subcolumn/result/encode_compress/"
    "encoder_compress_roundtrip_write_metrics4.csv";

constexpr const char kReadMetricsCsvPath[] =
    // "/home/allen/xjz17/subcolumn/result/encode_compress/"
    // "encoder_compress_roundtrip_read_metrics.csv";

    // "D:/github/xjz17/subcolumn/result/encode_compress/"
    // "encoder_compress_roundtrip_read_metrics3.csv";

    "D:/github/xjz17/subcolumn/result/encode_compress/"
    "encoder_compress_roundtrip_read_metrics4.csv";

constexpr const char kCompressManifestCsvPath[] =
    // "/home/allen/xjz17/subcolumn/result/encode_compress/"
    // "encoder_compress_roundtrip_compress_manifest.csv";

    // "D:/github/xjz17/subcolumn/result/encode_compress/"
    // "encoder_compress_roundtrip_compress_manifest1.csv";

    "D:/github/xjz17/subcolumn/result/encode_compress/"
    "encoder_compress_roundtrip_compress_manifest2.csv";

constexpr const char kDecodedCsvDir[] =
    // "/home/allen/xjz17/subcolumn/result/encode_compress/decoded_csv";
    // "D:/github/xjz17/subcolumn/result/encode_compress/decoded_csv";
    "E:/xjz/encode_compress/decoded_csv";

constexpr int kBenchPhaseRepeats = 40;

constexpr int kMaxDecimalPrecision = 8;

constexpr uint32_t kByteStreamPageSize = 1024 * 1024;

constexpr size_t kCompressedBinIoChunkBytes = 64 * 1024;
// constexpr size_t kCompressedBinIoChunkBytes = 1 * 128;

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

static void scale_tokens_to_int64_per_subcolumn_block(
    const std::vector<std::string> &tokens, std::vector<int64_t> &out_values) {
    const int bs = storage::LongSubcolumnEncoder::BLOCK_SIZE;
    const size_t n = tokens.size();
    out_values.resize(n);
    size_t offset = 0;
    while (offset < n) {
        const size_t end = std::min(offset + static_cast<size_t>(bs), n);
        int block_max_prec = 0;
        for (size_t i = offset; i < end; ++i) {
            block_max_prec =
                std::max(block_max_prec, decimal_precision(tokens[i]));
        }
        const int capped = std::min(block_max_prec, kMaxDecimalPrecision);
        const int64_t mult = multiplier_for_precision(capped);
        for (size_t i = offset; i < end; ++i) {
            out_values[i] = to_scaled_int64(tokens[i], mult);
        }
        offset = end;
    }
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
                (void)tsfile_test_mkdir(cur.c_str());
            }
        }
    }
    if (!dir_exists(cur)) {
        (void)tsfile_test_mkdir(cur.c_str());
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

std::string parent_directory(const std::string &path) {
    size_t end = path.size();
    while (end > 0 && (path[end - 1] == '/' || path[end - 1] == '\\')) {
        --end;
    }
    const size_t pos = path.find_last_of("/\\", end - 1);
    if (pos == std::string::npos) {
        return "";
    }
    return path.substr(0, pos);
}

bool merge_source_csvs_into_combined(const std::string &source_dataset_dir,
                                     std::string &out_combined_csv_path) {
    const std::vector<std::string> sources = list_csv_files(source_dataset_dir);
    if (sources.empty()) {
        return false;
    }

    const std::string parent = parent_directory(source_dataset_dir);
    const std::string combined_dir =
        parent.empty() ? "dataset_big_combined" : parent + "/dataset_big_combined";
    ensure_dir_recursive(combined_dir);
    out_combined_csv_path = combined_dir + "/combined.csv";

    std::ofstream out(out_combined_csv_path.c_str(),
                      std::ios::out | std::ios::trunc);
    if (!out.good()) {
        return false;
    }

    for (const std::string &src_path : sources) {
        std::ifstream in(src_path.c_str());
        if (!in.good()) {
            return false;
        }
        std::string line;
        while (std::getline(in, line)) {
            if (!line.empty() && line.back() == '\r') {
                line.pop_back();
            }
            out << line << '\n';
        }
        if (in.bad()) {
            return false;
        }
    }

    out.flush();
    return static_cast<bool>(out);
}

bool prepare_combined_benchmark_dataset(std::string &out_combined_csv_path) {
    if (!merge_source_csvs_into_combined(kDatasetDir, out_combined_csv_path)) {
        return false;
    }
    std::fflush(stderr);
    return true;
}

static bool load_scaled_int64_column_csv_rw_dataset_read_timing(
    const std::string &dataset_file,
    std::vector<int64_t> &out_values,
    int &max_decimal_precision,
    int64_t *out_dataset_read_ns) {
    const auto read_t0 = std::chrono::steady_clock::now();
    std::ifstream in(dataset_file.c_str());
    if (!in.good()) {
        return false;
    }
    std::vector<std::string> column_tokens;
    column_tokens.reserve(1024);
    max_decimal_precision = 0;
    std::string line;
    while (std::getline(in, line)) {
        const std::string value = first_column(line);
        if (value.empty()) {
            continue;
        }
        max_decimal_precision =
            std::max(max_decimal_precision, decimal_precision(value));
        column_tokens.push_back(value);
    }
    const auto read_t1 = std::chrono::steady_clock::now();
    const int64_t dataset_read_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(read_t1 - read_t0)
            .count();
    if (out_dataset_read_ns != nullptr) {
        *out_dataset_read_ns = dataset_read_ns;
    }
    if (in.bad()) {
        return false;
    }

    scale_tokens_to_int64_per_subcolumn_block(column_tokens, out_values);
    return true;
}

static bool load_double_column_csv_rw_dataset_read_timing(
    const std::string &dataset_file,
    std::vector<double> &out_values,
    int &max_decimal_precision,
    int64_t *out_dataset_read_ns) {
    const auto read_t0 = std::chrono::steady_clock::now();
    std::ifstream in(dataset_file.c_str());
    if (!in.good()) {
        return false;
    }
    std::vector<std::string> column_tokens;
    column_tokens.reserve(1024);
    max_decimal_precision = 0;
    std::string line;
    while (std::getline(in, line)) {
        const std::string value = first_column(line);
        if (value.empty()) {
            continue;
        }
        max_decimal_precision =
            std::max(max_decimal_precision, decimal_precision(value));
        column_tokens.push_back(value);
    }
    const auto read_t1 = std::chrono::steady_clock::now();
    const int64_t dataset_read_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(read_t1 - read_t0)
            .count();
    if (out_dataset_read_ns != nullptr) {
        *out_dataset_read_ns = dataset_read_ns;
    }
    if (in.bad()) {
        return false;
    }
    out_values.clear();
    out_values.reserve(column_tokens.size());
    for (const std::string &raw : column_tokens) {
        out_values.push_back(std::strtod(raw.c_str(), nullptr));
    }
    return true;
}

static bool write_decoded_values_csv_timed(const std::string &csv_path,
                                           const std::vector<int64_t> &values,
                                           int64_t *out_write_ns) {
    const auto t0 = std::chrono::steady_clock::now();
    std::ofstream out(csv_path.c_str(), std::ios::out | std::ios::trunc);
    if (!out.good()) {
        return false;
    }
    for (size_t i = 0; i < values.size(); ++i) {
        out << values[i] << '\n';
    }
    const auto t1 = std::chrono::steady_clock::now();
    if (out_write_ns != nullptr) {
        *out_write_ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
    }
    out.flush();
    return static_cast<bool>(out);
}

static bool write_decoded_doubles_csv_timed(const std::string &csv_path,
                                            const std::vector<double> &values,
                                            int64_t *out_write_ns) {
    const auto t0 = std::chrono::steady_clock::now();
    std::ofstream out(csv_path.c_str(), std::ios::out | std::ios::trunc);
    if (!out.good()) {
        return false;
    }
    out << std::setprecision(std::numeric_limits<double>::max_digits10);
    for (size_t i = 0; i < values.size(); ++i) {
        out << values[i] << '\n';
    }
    const auto t1 = std::chrono::steady_clock::now();
    if (out_write_ns != nullptr) {
        *out_write_ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
    }
    out.flush();
    return static_cast<bool>(out);
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

struct BenchParallelConfig {
    common::TSEncoding encoding;
    common::CompressionType compression;
    const char *algo_csv_name;
    const char *file_suffix;
};

std::vector<BenchParallelConfig> build_parallel_benchmark_configs() {
    const BenchParallelConfig candidates[] = {
        {common::PLAIN, common::LZ4, "LZ4", "plain_lz4"},
        {common::PLAIN, common::GZIP, "GZIP", "plain_gzip"},
        {common::PLAIN, common::ZSTD, "ZSTD", "plain_zstd"},
        {common::PLAIN, common::LZMA, "LZMA", "plain_lzma"},
        {common::GORILLA, common::UNCOMPRESSED, "GORILLA", "gorilla"},
        {common::RLE, common::UNCOMPRESSED, "RLE", "rle"},
        {common::BITPACKING, common::UNCOMPRESSED, "BITPACKING", "bitpacking"},
        {common::DICTIONARY, common::UNCOMPRESSED, "DICTIONARY", "dictionary"},
        {common::SUBCOLUMN, common::UNCOMPRESSED, "SUBCOLUMN", "subcolumn"},
        {common::SPRINTZ, common::UNCOMPRESSED, "SPRINTZ", "sprintz"},
        {common::SPRINTZ_SUBCOLUMN, common::UNCOMPRESSED, "SPRINTZ_SUBCOLUMN",
         "sprintz_subcolumn"},
        {common::TS_2DIFF, common::UNCOMPRESSED, "TS_2DIFF", "ts_2diff"},
        {common::TS_2DIFF_SUBCOLUMN, common::UNCOMPRESSED, "TS_2DIFF_SUBCOLUMN",
         "ts_2diff_subcolumn"},
    };
    std::vector<BenchParallelConfig> out;
    for (const BenchParallelConfig &c : candidates) {
        common::TSDataType probe_dt =
            (c.encoding == common::GORILLA) ? common::DOUBLE : common::INT64;
        storage::Encoder *te =
            storage::EncoderFactory::alloc_value_encoder(c.encoding, probe_dt);
        if (te == nullptr) {
            continue;
        }
        te->destroy();
        storage::EncoderFactory::free(te);
        storage::Decoder *td =
            storage::DecoderFactory::alloc_value_decoder(c.encoding, probe_dt);
        if (td == nullptr) {
            continue;
        }
        td->~Decoder();
        common::mem_free(td);
        storage::Compressor *tc =
            storage::CompressorFactory::alloc_compressor(c.compression);
        if (tc == nullptr) {
            continue;
        }
        tc->destroy();
        storage::CompressorFactory::free(tc);
        out.push_back(c);
    }
    return out;
}

const char *encoding_label(common::TSEncoding enc) {
    if (enc <= common::BITPACKING) {
        return common::get_encoding_name(enc);
    }
    return "UNKNOWN_ENCODING";
}

const char *compression_label(common::CompressionType c) {
    if (c <= common::LZMA) {
        return common::get_compression_name(c);
    }
    return "UNKNOWN_COMPRESSION";
}

static bool plain_codec_uses_raw_int64_compress_input(common::CompressionType c) {
    switch (c) {
#ifdef ENABLE_LZ4
        case common::LZ4:
            return true;
#endif
#ifdef ENABLE_GZIP
        case common::GZIP:
            return true;
#endif
#ifdef ENABLE_ZSTD
        case common::ZSTD:
            return true;
#endif
#ifdef ENABLE_LZMA
        case common::LZMA:
            return true;
#endif
        default:
            return false;
    }
}

template <typename CompressorT>
static bool compressor_compress_repeated(
    CompressorT &compressor,
    char *src,
    uint32_t src_len,
    int timing_repeats,
    int64_t &sum_compress_ns,
    std::vector<char> &compressed_owned) {
    sum_compress_ns = 0;
    compressed_owned.clear();
    if (timing_repeats < 1) {
        return false;
    }
    for (int rep = 0; rep < timing_repeats; ++rep) {
        if (compressor.reset(true) != common::E_OK) {
            return false;
        }
        char *compressed_ptr = nullptr;
        uint32_t compressed_len = 0;
        const auto t_cmp0 = std::chrono::steady_clock::now();
        if (compressor.compress(src, src_len, compressed_ptr, compressed_len) !=
            common::E_OK) {
            return false;
        }
        const auto t_cmp1 = std::chrono::steady_clock::now();
        sum_compress_ns +=
            std::chrono::duration_cast<std::chrono::nanoseconds>(t_cmp1 - t_cmp0)
                .count();
        if (rep == timing_repeats - 1) {
            compressed_owned.assign(compressed_ptr,
                                    compressed_ptr + compressed_len);
        }
        compressor.after_compress(compressed_ptr);
    }
    return true;
}

static bool factory_compress_repeated(
    storage::Compressor *compressor,
    char *src,
    uint32_t src_len,
    int timing_repeats,
    int64_t &sum_compress_ns,
    std::vector<char> &compressed_owned) {
    sum_compress_ns = 0;
    compressed_owned.clear();
    if (timing_repeats < 1) {
        return false;
    }
    for (int rep = 0; rep < timing_repeats; ++rep) {
        if (compressor->reset(true) != common::E_OK) {
            return false;
        }
        char *compressed_ptr = nullptr;
        uint32_t compressed_len = 0;
        const auto t_cmp0 = std::chrono::steady_clock::now();
        const int cr =
            compressor->compress(src, src_len, compressed_ptr, compressed_len);
        const auto t_cmp1 = std::chrono::steady_clock::now();
        if (cr != common::E_OK) {
            return false;
        }
        sum_compress_ns +=
            std::chrono::duration_cast<std::chrono::nanoseconds>(t_cmp1 - t_cmp0)
                .count();
        if (rep == timing_repeats - 1) {
            compressed_owned.assign(compressed_ptr,
                                    compressed_ptr + compressed_len);
        }
        compressor->after_compress(compressed_ptr);
    }
    return true;
}

template <typename CompressorT>
static bool template_decompress_raw_double_repeated(
    CompressorT &decompressor,
    const std::vector<char> &compressed_payload,
    uint32_t expected_raw_len,
    const std::vector<double> &values,
    int timing_repeats,
    int64_t &sum_unc_ns,
    int64_t &sum_dec_ns,
    std::vector<double> &decoded) {
    sum_unc_ns = 0;
    sum_dec_ns = 0;
    decoded.clear();
    decoded.resize(values.size());
    if (timing_repeats < 1) {
        return false;
    }
    for (int rep = 0; rep < timing_repeats; ++rep) {
        if (decompressor.reset(false) != common::E_OK) {
            return false;
        }
        char *uncompressed_ptr = nullptr;
        uint32_t uncompressed_len = 0;
        const auto t_unc0 = std::chrono::steady_clock::now();
        const int ur = decompressor.uncompress(
            const_cast<char *>(compressed_payload.data()),
            static_cast<uint32_t>(compressed_payload.size()), uncompressed_ptr,
            uncompressed_len);
        const auto t_unc1 = std::chrono::steady_clock::now();
        if (ur != common::E_OK) {
            return false;
        }
        sum_unc_ns +=
            std::chrono::duration_cast<std::chrono::nanoseconds>(t_unc1 - t_unc0)
                .count();

        if (uncompressed_len != expected_raw_len) {
            decompressor.after_uncompress(uncompressed_ptr);
            return false;
        }

        const auto t_dec0 = std::chrono::steady_clock::now();
        std::memcpy(reinterpret_cast<char *>(decoded.data()), uncompressed_ptr,
                    static_cast<size_t>(expected_raw_len));
        const auto t_dec1 = std::chrono::steady_clock::now();
        sum_dec_ns +=
            std::chrono::duration_cast<std::chrono::nanoseconds>(t_dec1 - t_dec0)
                .count();

        if (rep == 0) {
            if (decoded.size() != values.size() || decoded != values) {
                decompressor.after_uncompress(uncompressed_ptr);
                ADD_FAILURE() << "decompressed raw double layout mismatch";
                return false;
            }
        }

        decompressor.after_uncompress(uncompressed_ptr);
    }
    return true;
}

static bool factory_decompress_decode_repeated(
    storage::Compressor *decompressor,
    storage::Decoder *decoder,
    const std::vector<char> &compressed_payload,
    size_t expected_uncompressed_bytes,
    const std::vector<int64_t> &values,
    int timing_repeats,
    int64_t &sum_unc_ns,
    int64_t &sum_dec_ns,
    std::vector<int64_t> &decoded) {
    sum_unc_ns = 0;
    sum_dec_ns = 0;
    decoded.clear();
    decoded.reserve(values.size());
    if (timing_repeats < 1) {
        return false;
    }
    for (int rep = 0; rep < timing_repeats; ++rep) {
        if (decompressor->reset(false) != common::E_OK) {
            decoder->~Decoder();
            common::mem_free(decoder);
            decompressor->destroy();
            storage::CompressorFactory::free(decompressor);
            return false;
        }
        char *uncompressed_ptr = nullptr;
        uint32_t uncompressed_len = 0;
        const auto t_unc0 = std::chrono::steady_clock::now();
        const int ur = decompressor->uncompress(
            const_cast<char *>(compressed_payload.data()),
            static_cast<uint32_t>(compressed_payload.size()), uncompressed_ptr,
            uncompressed_len);
        const auto t_unc1 = std::chrono::steady_clock::now();
        if (ur != common::E_OK) {
            decoder->~Decoder();
            common::mem_free(decoder);
            decompressor->destroy();
            storage::CompressorFactory::free(decompressor);
            return false;
        }
        sum_unc_ns +=
            std::chrono::duration_cast<std::chrono::nanoseconds>(t_unc1 - t_unc0)
                .count();

        if (static_cast<size_t>(uncompressed_len) != expected_uncompressed_bytes) {
            decompressor->after_uncompress(uncompressed_ptr);
            decoder->~Decoder();
            common::mem_free(decoder);
            decompressor->destroy();
            storage::CompressorFactory::free(decompressor);
            return false;
        }

        common::ByteStream decode_stream(kByteStreamPageSize, common::MOD_DEFAULT);
        decode_stream.wrap_from(uncompressed_ptr,
                                static_cast<int32_t>(uncompressed_len));
        decoder->reset();
        decoded.clear();

        const auto t_dec0 = std::chrono::steady_clock::now();
        for (size_t i = 0; i < values.size(); ++i) {
            int64_t v = 0;
            if (decoder->read_int64(v, decode_stream) != common::E_OK) {
                decoder->~Decoder();
                common::mem_free(decoder);
                decompressor->after_uncompress(uncompressed_ptr);
                decompressor->destroy();
                storage::CompressorFactory::free(decompressor);
                return false;
            }
            decoded.push_back(v);
        }
        const auto t_dec1 = std::chrono::steady_clock::now();
        sum_dec_ns +=
            std::chrono::duration_cast<std::chrono::nanoseconds>(t_dec1 - t_dec0)
                .count();

        if (decoder->has_remaining(decode_stream)) {
            decoder->~Decoder();
            common::mem_free(decoder);
            decompressor->after_uncompress(uncompressed_ptr);
            decompressor->destroy();
            storage::CompressorFactory::free(decompressor);
            return false;
        }

        if (rep == 0) {
            if (decoded.size() != values.size() || decoded != values) {
                decompressor->after_uncompress(uncompressed_ptr);
                decoder->~Decoder();
                common::mem_free(decoder);
                decompressor->destroy();
                storage::CompressorFactory::free(decompressor);
                ADD_FAILURE() << "decoded mismatch";
                return false;
            }
        }

        decompressor->after_uncompress(uncompressed_ptr);
    }
    return true;
}

static bool factory_decompress_decode_repeated_double(
    storage::Compressor *decompressor,
    storage::Decoder *decoder,
    const std::vector<char> &compressed_payload,
    size_t expected_uncompressed_bytes,
    const std::vector<double> &values,
    int timing_repeats,
    int64_t &sum_unc_ns,
    int64_t &sum_dec_ns,
    std::vector<double> &decoded) {
    sum_unc_ns = 0;
    sum_dec_ns = 0;
    decoded.clear();
    decoded.reserve(values.size());
    if (timing_repeats < 1) {
        return false;
    }
    for (int rep = 0; rep < timing_repeats; ++rep) {
        if (decompressor->reset(false) != common::E_OK) {
            decoder->~Decoder();
            common::mem_free(decoder);
            decompressor->destroy();
            storage::CompressorFactory::free(decompressor);
            return false;
        }
        char *uncompressed_ptr = nullptr;
        uint32_t uncompressed_len = 0;
        const auto t_unc0 = std::chrono::steady_clock::now();
        const int ur = decompressor->uncompress(
            const_cast<char *>(compressed_payload.data()),
            static_cast<uint32_t>(compressed_payload.size()), uncompressed_ptr,
            uncompressed_len);
        const auto t_unc1 = std::chrono::steady_clock::now();
        if (ur != common::E_OK) {
            decoder->~Decoder();
            common::mem_free(decoder);
            decompressor->destroy();
            storage::CompressorFactory::free(decompressor);
            return false;
        }
        sum_unc_ns +=
            std::chrono::duration_cast<std::chrono::nanoseconds>(t_unc1 - t_unc0)
                .count();

        if (static_cast<size_t>(uncompressed_len) != expected_uncompressed_bytes) {
            decompressor->after_uncompress(uncompressed_ptr);
            decoder->~Decoder();
            common::mem_free(decoder);
            decompressor->destroy();
            storage::CompressorFactory::free(decompressor);
            return false;
        }

        common::ByteStream decode_stream(kByteStreamPageSize, common::MOD_DEFAULT);
        decode_stream.wrap_from(uncompressed_ptr,
                                static_cast<int32_t>(uncompressed_len));
        decoder->reset();
        decoded.clear();

        const auto t_dec0 = std::chrono::steady_clock::now();
        for (size_t i = 0; i < values.size(); ++i) {
            double v = 0.0;
            if (decoder->read_double(v, decode_stream) != common::E_OK) {
                decoder->~Decoder();
                common::mem_free(decoder);
                decompressor->after_uncompress(uncompressed_ptr);
                decompressor->destroy();
                storage::CompressorFactory::free(decompressor);
                return false;
            }
            decoded.push_back(v);
        }
        const auto t_dec1 = std::chrono::steady_clock::now();
        sum_dec_ns +=
            std::chrono::duration_cast<std::chrono::nanoseconds>(t_dec1 - t_dec0)
                .count();

        if (decoder->has_remaining(decode_stream)) {
            decoder->~Decoder();
            common::mem_free(decoder);
            decompressor->after_uncompress(uncompressed_ptr);
            decompressor->destroy();
            storage::CompressorFactory::free(decompressor);
            return false;
        }

        if (rep == 0) {
            if (decoded.size() != values.size() || decoded != values) {
                decompressor->after_uncompress(uncompressed_ptr);
                decoder->~Decoder();
                common::mem_free(decoder);
                decompressor->destroy();
                storage::CompressorFactory::free(decompressor);
                ADD_FAILURE() << "decoded double mismatch";
                return false;
            }
        }

        decompressor->after_uncompress(uncompressed_ptr);
    }
    return true;
}

static bool tsfile_read_bin_buffered_ifstream(const std::string &bin_path,
                                              std::vector<char> &out);

#if defined(_WIN32)
static std::wstring tsfile_utf8_to_wide_path(const std::string &utf8) {
    if (utf8.empty()) {
        return std::wstring();
    }
    int n = MultiByteToWideChar(CP_UTF8, 0, utf8.c_str(),
                                static_cast<int>(utf8.size()), nullptr, 0);
    if (n <= 0) {
        n = MultiByteToWideChar(CP_ACP, 0, utf8.c_str(),
                                static_cast<int>(utf8.size()), nullptr, 0);
        if (n <= 0) {
            return std::wstring();
        }
        std::wstring w(static_cast<size_t>(n), L'\0');
        MultiByteToWideChar(CP_ACP, 0, utf8.c_str(),
                            static_cast<int>(utf8.size()), &w[0], n);
        return w;
    }
    std::wstring w(static_cast<size_t>(n), L'\0');
    MultiByteToWideChar(CP_UTF8, 0, utf8.c_str(),
                        static_cast<int>(utf8.size()), &w[0], n);
    return w;
}

static bool tsfile_win_write_bin_through(const std::string &path_utf8,
                                         const std::vector<char> &data) {
    const std::wstring wpath = tsfile_utf8_to_wide_path(path_utf8);
    if (wpath.empty()) {
        return false;
    }
    HANDLE h = ::CreateFileW(wpath.c_str(), GENERIC_WRITE, 0, nullptr, CREATE_ALWAYS,
                             FILE_ATTRIBUTE_NORMAL | FILE_FLAG_WRITE_THROUGH,
                             nullptr);
    if (h == INVALID_HANDLE_VALUE) {
        return false;
    }
    size_t off = 0;
    while (off < data.size()) {
        const DWORD chunk = static_cast<DWORD>(
            std::min<size_t>(data.size() - off, static_cast<size_t>(1u << 22)));
        DWORD bw = 0;
        if (!::WriteFile(h, data.data() + off, chunk, &bw, nullptr) ||
            bw != chunk) {
            (void)::CloseHandle(h);
            return false;
        }
        off += static_cast<size_t>(chunk);
    }
    (void)::FlushFileBuffers(h);
    (void)::CloseHandle(h);
    return true;
}

static DWORD tsfile_win_logical_sector_bytes(const std::wstring &file_path_w) {
    wchar_t volume[MAX_PATH + 4]{};
    if (!GetVolumePathNameW(file_path_w.c_str(), volume, MAX_PATH)) {
        return 512;
    }
    DWORD sectors_per_cluster = 0;
    DWORD bytes_per_sector = 0;
    DWORD free_clusters = 0;
    DWORD total_clusters = 0;
    if (!GetDiskFreeSpaceW(volume, &sectors_per_cluster, &bytes_per_sector,
                           &free_clusters, &total_clusters)) {
        return 512;
    }
    if (bytes_per_sector == 0 || bytes_per_sector > 65536u) {
        return 512;
    }
    return bytes_per_sector;
}

static bool tsfile_read_bin_win32_buffered(const std::string &path_utf8,
                                           std::vector<char> &out) {
    const std::wstring wpath = tsfile_utf8_to_wide_path(path_utf8);
    if (wpath.empty()) {
        return false;
    }
    HANDLE h = ::CreateFileW(
        wpath.c_str(), GENERIC_READ,
        FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, nullptr,
        OPEN_EXISTING,
        FILE_ATTRIBUTE_NORMAL | FILE_FLAG_SEQUENTIAL_SCAN, nullptr);
    if (h == INVALID_HANDLE_VALUE) {
        return false;
    }
    LARGE_INTEGER li{};
    if (!::GetFileSizeEx(h, &li)) {
        (void)::CloseHandle(h);
        return false;
    }
    if (li.QuadPart <= 0) {
        (void)::CloseHandle(h);
        return false;
    }
    const auto file_sz_u64 = static_cast<uint64_t>(li.QuadPart);
    if (file_sz_u64 > static_cast<uint64_t>(SIZE_MAX)) {
        (void)::CloseHandle(h);
        return false;
    }
    const size_t file_sz = static_cast<size_t>(file_sz_u64);
    out.resize(file_sz);
    size_t total_read = 0;
    while (total_read < file_sz) {
        const size_t remaining = file_sz - total_read;
        const DWORD chunk = static_cast<DWORD>(
            std::min<size_t>(remaining, static_cast<size_t>(1u << 22)));
        DWORD br = 0;
        if (!::ReadFile(h, out.data() + total_read, chunk, &br, nullptr)) {
            (void)::CloseHandle(h);
            return false;
        }
        if (br == 0) {
            (void)::CloseHandle(h);
            return false;
        }
        total_read += static_cast<size_t>(br);
    }
    (void)::CloseHandle(h);
    return total_read == file_sz;
}

static bool tsfile_read_bin_win32_no_buffering(const std::string &path_utf8,
                                               std::vector<char> &out) {
    const std::wstring wpath = tsfile_utf8_to_wide_path(path_utf8);
    if (wpath.empty()) {
        return false;
    }
    const DWORD sector = tsfile_win_logical_sector_bytes(wpath);
    HANDLE h = ::CreateFileW(
        wpath.c_str(), GENERIC_READ,
        FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, nullptr,
        OPEN_EXISTING,
        FILE_ATTRIBUTE_NORMAL | FILE_FLAG_NO_BUFFERING | FILE_FLAG_SEQUENTIAL_SCAN,
        nullptr);
    if (h == INVALID_HANDLE_VALUE) {
        return false;
    }
    LARGE_INTEGER li{};
    if (!::GetFileSizeEx(h, &li)) {
        (void)::CloseHandle(h);
        return false;
    }
    if (li.QuadPart <= 0) {
        (void)::CloseHandle(h);
        return false;
    }
    const auto file_sz_u64 = static_cast<uint64_t>(li.QuadPart);
    if (file_sz_u64 > static_cast<uint64_t>(SIZE_MAX)) {
        (void)::CloseHandle(h);
        return false;
    }
    const size_t file_sz = static_cast<size_t>(file_sz_u64);

    const uint64_t rounded_u64 =
        (file_sz_u64 + static_cast<uint64_t>(sector) - 1ULL) /
        static_cast<uint64_t>(sector) * static_cast<uint64_t>(sector);
    if (rounded_u64 > static_cast<uint64_t>(SIZE_MAX)) {
        (void)::CloseHandle(h);
        return false;
    }
    const size_t rounded_sz = static_cast<size_t>(rounded_u64);

    void *raw =
        ::VirtualAlloc(nullptr, rounded_sz, MEM_COMMIT | MEM_RESERVE, PAGE_READWRITE);
    if (raw == nullptr) {
        (void)::CloseHandle(h);
        return false;
    }

    size_t total_read = 0;
    while (total_read < rounded_sz) {
        const size_t remaining = rounded_sz - total_read;
        DWORD chunk = static_cast<DWORD>(
            std::min<size_t>(remaining, static_cast<size_t>(1u << 22)));
        chunk = (chunk / sector) * sector;
        if (chunk == 0) {
            chunk = sector;
        }
        DWORD br = 0;
        if (!::ReadFile(h, static_cast<char *>(raw) + total_read, chunk, &br,
                        nullptr)) {
            (void)::VirtualFree(raw, 0, MEM_RELEASE);
            (void)::CloseHandle(h);
            return false;
        }
        if (br == 0) {
            (void)::VirtualFree(raw, 0, MEM_RELEASE);
            (void)::CloseHandle(h);
            return false;
        }
        total_read += static_cast<size_t>(br);
    }
    if (total_read != rounded_sz) {
        (void)::VirtualFree(raw, 0, MEM_RELEASE);
        (void)::CloseHandle(h);
        return false;
    }
    (void)::CloseHandle(h);

    out.resize(file_sz);
    if (file_sz > 0) {
        std::memcpy(out.data(), raw, file_sz);
    }
    (void)::VirtualFree(raw, 0, MEM_RELEASE);
    return true;
}

static int bench_windows_bin_read_mode() {
    static int cached = -2;
    if (cached != -2) {
        return cached;
    }
    const char *e = std::getenv("TSFILE_BENCH_BIN_READ_MODE");
    if (!e || !e[0]) {
        cached = -1;
        return cached;
    }
    if (std::strcmp(e, "ifstream") == 0) {
        cached = 0;
    } else if (std::strcmp(e, "win32") == 0) {
        cached = 1;
    } else if (std::strcmp(e, "nobuf") == 0 ||
               std::strcmp(e, "no_buffering") == 0) {
        cached = -1;
    } else {
        cached = std::atoi(e);
        if (cached < 0 || cached > 1) {
            cached = -1;
        }
    }
    return cached;
}

static bool bench_windows_bin_read_shrink_before_read() {
    static int cached = -2;
    if (cached != -2) {
        return cached != 0;
    }
    const char *e = std::getenv("TSFILE_BENCH_BIN_READ_SHRINK");
    if (!e || !e[0]) {
        cached = 1;
        return true;
    }
    cached = (std::strcmp(e, "0") == 0) ? 0 : 1;
    return cached != 0;
}

static bool tsfile_bench_read_bin_windows(const std::string &path,
                                          std::vector<char> &out) {
    const int mode = bench_windows_bin_read_mode();
    if (mode == 0) {
        return tsfile_read_bin_buffered_ifstream(path, out);
    }
    if (mode == 1) {
        return tsfile_read_bin_win32_buffered(path, out);
    }
    if (tsfile_read_bin_win32_no_buffering(path, out)) {
        return true;
    }
    return tsfile_read_bin_win32_buffered(path, out);
}
#endif  // _WIN32

static bool tsfile_read_bin_buffered_ifstream(const std::string &bin_path,
                                              std::vector<char> &out) {
    std::ifstream bin(bin_path.c_str(),
                      std::ios::in | std::ios::binary | std::ios::ate);
    if (!bin.good()) {
        return false;
    }
    const auto sz = bin.tellg();
    if (sz <= 0) {
        return false;
    }
    bin.seekg(0);
    const size_t total = static_cast<size_t>(sz);
    out.resize(total);
    size_t roff = 0;
    while (roff < total) {
        const size_t want =
            std::min(kCompressedBinIoChunkBytes, total - roff);
        bin.read(out.data() + roff, static_cast<std::streamsize>(want));
        if (bin.bad()) {
            return false;
        }
        const std::streamsize got = bin.gcount();
        if (got == 0) {
            return false;
        }
        roff += static_cast<size_t>(got);
    }
    return roff == total;
}

bool compress_write_bin_once(const std::string &dataset_csv_path,
                               common::TSEncoding encoding,
                               common::CompressionType compression,
                               const std::string &bin_path,
                               size_t &out_point_count,
                               int &out_max_decimal_precision,
                               int64_t &out_dataset_read_ns,
                               int64_t &out_encode_ns,
                               int64_t &out_encode_flush_ns,
                               int64_t &out_compress_ns,
                               int64_t &out_bin_write_ns,
                               size_t &out_encoded_bytes,
                               size_t &out_compressed_bytes,
                               bool &out_raw_plain_codec) {
    out_point_count = 0;
    out_max_decimal_precision = 0;
    out_dataset_read_ns = out_encode_ns = out_encode_flush_ns = out_compress_ns = 0;
    out_bin_write_ns = 0;
    out_encoded_bytes = out_compressed_bytes = 0;
    out_raw_plain_codec = false;

    const bool raw_plain_compress =
        encoding == common::PLAIN &&
        plain_codec_uses_raw_int64_compress_input(compression);
    const bool gorilla_double_path = (encoding == common::GORILLA);

    std::vector<double> dvalues;
    std::vector<int64_t> values;
    int max_decimal_precision = 0;

    int64_t sum_dataset_read_ns = 0;
    if (raw_plain_compress || gorilla_double_path) {
        for (int ph = 0; ph < kBenchPhaseRepeats; ++ph) {
            int64_t one_dr = 0;
            if (!load_double_column_csv_rw_dataset_read_timing(
                    dataset_csv_path, dvalues, max_decimal_precision, &one_dr)) {
                return false;
            }
            if (dvalues.empty()) {
                return false;
            }
            sum_dataset_read_ns += one_dr;
        }
        out_point_count = dvalues.size();
    } else {
        for (int ph = 0; ph < kBenchPhaseRepeats; ++ph) {
            int64_t one_dr = 0;
            if (!load_scaled_int64_column_csv_rw_dataset_read_timing(
                    dataset_csv_path, values, max_decimal_precision, &one_dr)) {
                return false;
            }
            if (values.empty()) {
                return false;
            }
            sum_dataset_read_ns += one_dr;
        }
        out_point_count = values.size();
    }
    out_max_decimal_precision = max_decimal_precision;

    if (raw_plain_compress) {
        out_raw_plain_codec = true;
        const uint64_t raw_sz_u64 =
            static_cast<uint64_t>(dvalues.size()) * sizeof(double);
        if (raw_sz_u64 > static_cast<uint64_t>(UINT32_MAX)) {
            return false;
        }
        const uint32_t compress_len = static_cast<uint32_t>(raw_sz_u64);
        char *compress_src = reinterpret_cast<char *>(dvalues.data());

        int64_t sum_compress_ns = 0;
        std::vector<char> compressed_owned;

#ifdef ENABLE_LZ4
        if (compression == common::LZ4) {
            storage::LZ4Compressor compressor_local;
            if (!compressor_compress_repeated(compressor_local, compress_src,
                                              compress_len, kBenchPhaseRepeats,
                                              sum_compress_ns, compressed_owned)) {
                return false;
            }
        } else
#endif
#ifdef ENABLE_GZIP
            if (compression == common::GZIP) {
            storage::GZIPCompressor compressor_local;
            if (!compressor_compress_repeated(compressor_local, compress_src,
                                              compress_len, kBenchPhaseRepeats,
                                              sum_compress_ns, compressed_owned)) {
                return false;
            }
        } else
#endif
#ifdef ENABLE_ZSTD
            if (compression == common::ZSTD) {
            storage::ZSTDCompressor compressor_local;
            if (!compressor_compress_repeated(compressor_local, compress_src,
                                              compress_len, kBenchPhaseRepeats,
                                              sum_compress_ns, compressed_owned)) {
                return false;
            }
        } else
#endif
#ifdef ENABLE_LZMA
            if (compression == common::LZMA) {
            storage::LZMACompressor compressor_local;
            if (!compressor_compress_repeated(compressor_local, compress_src,
                                              compress_len, kBenchPhaseRepeats,
                                              sum_compress_ns, compressed_owned)) {
                return false;
            }
        } else
#endif
        {
            ADD_FAILURE() << "raw double codec compress: unsupported compression "
                          << compression_label(compression);
            return false;
        }

        int64_t sum_bin_write_ns = 0;
        for (int ph = 0; ph < kBenchPhaseRepeats; ++ph) {
            const auto t_bw0 = std::chrono::steady_clock::now();
#if defined(_WIN32)
            const bool win_wr_ok =
                tsfile_win_write_bin_through(bin_path, compressed_owned);
            if (!win_wr_ok) {
                std::ofstream bout(bin_path.c_str(),
                                   std::ios::out | std::ios::trunc | std::ios::binary);
                if (!bout.good()) {
                    return false;
                }
                const char *wp = compressed_owned.data();
                size_t wremain = compressed_owned.size();
                while (wremain > 0) {
                    const size_t n =
                        std::min(kCompressedBinIoChunkBytes, wremain);
                    bout.write(wp, static_cast<std::streamsize>(n));
                    if (!bout.good()) {
                        return false;
                    }
                    wp += n;
                    wremain -= n;
                }
            }
#else
            {
                std::ofstream bout(bin_path.c_str(),
                                   std::ios::out | std::ios::trunc | std::ios::binary);
                if (!bout.good()) {
                    return false;
                }
                const char *wp = compressed_owned.data();
                size_t wremain = compressed_owned.size();
                while (wremain > 0) {
                    const size_t n =
                        std::min(kCompressedBinIoChunkBytes, wremain);
                    bout.write(wp, static_cast<std::streamsize>(n));
                    if (!bout.good()) {
                        return false;
                    }
                    wp += n;
                    wremain -= n;
                }
            }
#endif
            const auto t_bw1 = std::chrono::steady_clock::now();
            sum_bin_write_ns +=
                std::chrono::duration_cast<std::chrono::nanoseconds>(t_bw1 - t_bw0)
                    .count();
        }

        out_dataset_read_ns = sum_dataset_read_ns / kBenchPhaseRepeats;
        out_encode_ns = 0;
        out_encode_flush_ns = 0;
        out_compress_ns = sum_compress_ns / kBenchPhaseRepeats;
        out_bin_write_ns = sum_bin_write_ns / kBenchPhaseRepeats;
        out_encoded_bytes = static_cast<size_t>(compress_len);
        out_compressed_bytes = compressed_owned.size();
        return true;
    }

    common::TSDataType value_dt =
        gorilla_double_path ? common::DOUBLE : common::INT64;
    storage::Encoder *encoder =
        storage::EncoderFactory::alloc_value_encoder(encoding, value_dt);
    if (encoder == nullptr) {
        return false;
    }

    int64_t sum_encode_ns = 0;
    int64_t sum_encode_flush_ns = 0;
    std::vector<uint8_t> encoded;

    for (int rep = 0; rep < kBenchPhaseRepeats; ++rep) {
        common::ByteStream encode_stream(kByteStreamPageSize, common::MOD_DEFAULT);
        encoder->reset();
        const auto t_enc0 = std::chrono::steady_clock::now();
        if (gorilla_double_path) {
            for (double d : dvalues) {
                if (encoder->encode(d, encode_stream) != common::E_OK) {
                    encoder->destroy();
                    storage::EncoderFactory::free(encoder);
                    return false;
                }
            }
        } else {
            for (int64_t v : values) {
                if (encoder->encode(v, encode_stream) != common::E_OK) {
                    encoder->destroy();
                    storage::EncoderFactory::free(encoder);
                    return false;
                }
            }
        }
        const auto t_enc1 = std::chrono::steady_clock::now();
        sum_encode_ns +=
            std::chrono::duration_cast<std::chrono::nanoseconds>(t_enc1 - t_enc0)
                .count();

        const auto t_fl0 = std::chrono::steady_clock::now();
        if (encoder->flush(encode_stream) != common::E_OK) {
            encoder->destroy();
            storage::EncoderFactory::free(encoder);
            return false;
        }
        const auto t_fl1 = std::chrono::steady_clock::now();
        sum_encode_flush_ns +=
            std::chrono::duration_cast<std::chrono::nanoseconds>(t_fl1 - t_fl0)
                .count();

        byte_stream_to_vector(encode_stream, encoded);
    }
    encoder->destroy();
    storage::EncoderFactory::free(encoder);

    if (encoded.size() > static_cast<size_t>(UINT32_MAX)) {
        return false;
    }

    char *compress_src = reinterpret_cast<char *>(encoded.data());
    uint32_t compress_len = static_cast<uint32_t>(encoded.size());

    out_raw_plain_codec = false;

    int64_t sum_compress_ns = 0;
    std::vector<char> compressed_owned;

    storage::Compressor *compressor =
        storage::CompressorFactory::alloc_compressor(compression);
    if (compressor == nullptr) {
        return false;
    }
    if (!factory_compress_repeated(compressor, compress_src, compress_len,
                                   kBenchPhaseRepeats, sum_compress_ns,
                                   compressed_owned)) {
        compressor->destroy();
        storage::CompressorFactory::free(compressor);
        return false;
    }
    compressor->destroy();
    storage::CompressorFactory::free(compressor);

    int64_t sum_bin_write_ns = 0;
    for (int ph = 0; ph < kBenchPhaseRepeats; ++ph) {
        const auto t_bw0 = std::chrono::steady_clock::now();
#if defined(_WIN32)
        const bool win_wr_ok =
            tsfile_win_write_bin_through(bin_path, compressed_owned);
        if (!win_wr_ok) {
            std::ofstream bout(bin_path.c_str(),
                               std::ios::out | std::ios::trunc | std::ios::binary);
            if (!bout.good()) {
                return false;
            }
            const char *wp = compressed_owned.data();
            size_t wremain = compressed_owned.size();
            while (wremain > 0) {
                const size_t n =
                    std::min(kCompressedBinIoChunkBytes, wremain);
                bout.write(wp, static_cast<std::streamsize>(n));
                if (!bout.good()) {
                    return false;
                }
                wp += n;
                wremain -= n;
            }
        }
#else
        {
            std::ofstream bout(bin_path.c_str(),
                               std::ios::out | std::ios::trunc | std::ios::binary);
            if (!bout.good()) {
                return false;
            }
            const char *wp = compressed_owned.data();
            size_t wremain = compressed_owned.size();
            while (wremain > 0) {
                const size_t n =
                    std::min(kCompressedBinIoChunkBytes, wremain);
                bout.write(wp, static_cast<std::streamsize>(n));
                if (!bout.good()) {
                    return false;
                }
                wp += n;
                wremain -= n;
            }
        }
#endif
        const auto t_bw1 = std::chrono::steady_clock::now();
        sum_bin_write_ns +=
            std::chrono::duration_cast<std::chrono::nanoseconds>(t_bw1 - t_bw0)
                .count();
    }

    out_dataset_read_ns = sum_dataset_read_ns / kBenchPhaseRepeats;
    out_encode_ns = sum_encode_ns / kBenchPhaseRepeats;
    out_encode_flush_ns = sum_encode_flush_ns / kBenchPhaseRepeats;
    out_compress_ns = sum_compress_ns / kBenchPhaseRepeats;
    out_bin_write_ns = sum_bin_write_ns / kBenchPhaseRepeats;
    out_encoded_bytes = encoded.size();
    out_compressed_bytes = compressed_owned.size();
    return true;
}

bool decompress_from_bin_once(const std::string &dataset_csv_path_for_verify,
                                common::TSEncoding encoding,
                                common::CompressionType compression,
                                const std::string &bin_path,
                                const std::string &decoded_csv_path,
                                size_t expected_points,
                                size_t encoded_uncompressed_bytes,
                                bool raw_plain_codec,
                                int64_t &out_dataset_verify_read_ns,
                                int64_t &out_bin_read_ns,
                                int64_t &out_decompress_ns,
                                int64_t &out_decode_ns,
                                int64_t &out_csv_write_ns,
                                size_t &out_compressed_bytes) {
    out_dataset_verify_read_ns = out_bin_read_ns = out_decompress_ns = out_decode_ns =
        out_csv_write_ns = 0;
    out_compressed_bytes = 0;

    const bool gorilla_double_path = (encoding == common::GORILLA);

    std::vector<double> dvalues;
    std::vector<int64_t> values;
    int max_decimal_precision_dummy = 0;
    int64_t sum_verify_read_ns = 0;
    if (raw_plain_codec || gorilla_double_path) {
        for (int ph = 0; ph < kBenchPhaseRepeats; ++ph) {
            int64_t one_dr = 0;
            if (!load_double_column_csv_rw_dataset_read_timing(
                    dataset_csv_path_for_verify, dvalues,
                    max_decimal_precision_dummy, &one_dr)) {
                return false;
            }
            if (dvalues.size() != expected_points) {
                ADD_FAILURE() << "decompress verify: point count mismatch manifest vs dataset "
                              << dvalues.size() << " vs " << expected_points;
                return false;
            }
            sum_verify_read_ns += one_dr;
        }
    } else {
        for (int ph = 0; ph < kBenchPhaseRepeats; ++ph) {
            int64_t one_dr = 0;
            if (!load_scaled_int64_column_csv_rw_dataset_read_timing(
                    dataset_csv_path_for_verify, values, max_decimal_precision_dummy,
                    &one_dr)) {
                return false;
            }
            if (values.size() != expected_points) {
                ADD_FAILURE() << "decompress verify: point count mismatch manifest vs dataset "
                              << values.size() << " vs " << expected_points;
                return false;
            }
            sum_verify_read_ns += one_dr;
        }
    }
    out_dataset_verify_read_ns = sum_verify_read_ns / kBenchPhaseRepeats;

    int64_t sum_bin_read_ns = 0;
    std::vector<char> file_blob;
    for (int ph = 0; ph < kBenchPhaseRepeats; ++ph) {
        const auto t_br0 = std::chrono::steady_clock::now();
#if defined(_WIN32)
        if (bench_windows_bin_read_shrink_before_read()) {
            file_blob.clear();
            file_blob.shrink_to_fit();
        }
        const bool ok_bin_read = tsfile_bench_read_bin_windows(bin_path, file_blob);
#else
        const bool ok_bin_read =
            tsfile_read_bin_buffered_ifstream(bin_path, file_blob);
#endif
        const auto t_br1 = std::chrono::steady_clock::now();
        if (!ok_bin_read) {
            return false;
        }
        sum_bin_read_ns +=
            std::chrono::duration_cast<std::chrono::nanoseconds>(t_br1 - t_br0)
                .count();
    }
    out_bin_read_ns = sum_bin_read_ns / kBenchPhaseRepeats;
    out_compressed_bytes = file_blob.size();

    if (file_blob.size() > static_cast<size_t>(UINT32_MAX)) {
        return false;
    }

    int64_t sum_unc_ns = 0;
    int64_t sum_dec_ns = 0;

    if (raw_plain_codec) {
        std::vector<double> decoded;
        const uint32_t raw_payload_len =
            static_cast<uint32_t>(dvalues.size() * sizeof(double));

#ifdef ENABLE_LZ4
        if (compression == common::LZ4) {
            storage::LZ4Compressor decompressor_local;
            if (!template_decompress_raw_double_repeated(
                    decompressor_local, file_blob, raw_payload_len,
                    dvalues, kBenchPhaseRepeats, sum_unc_ns, sum_dec_ns, decoded)) {
                return false;
            }
        } else
#endif
#ifdef ENABLE_GZIP
            if (compression == common::GZIP) {
            storage::GZIPCompressor decompressor_local;
            if (!template_decompress_raw_double_repeated(
                    decompressor_local, file_blob, raw_payload_len,
                    dvalues, kBenchPhaseRepeats, sum_unc_ns, sum_dec_ns, decoded)) {
                return false;
            }
        } else
#endif
#ifdef ENABLE_ZSTD
            if (compression == common::ZSTD) {
            storage::ZSTDCompressor decompressor_local;
            if (!template_decompress_raw_double_repeated(
                    decompressor_local, file_blob, raw_payload_len,
                    dvalues, kBenchPhaseRepeats, sum_unc_ns, sum_dec_ns, decoded)) {
                return false;
            }
        } else
#endif
#ifdef ENABLE_LZMA
            if (compression == common::LZMA) {
            storage::LZMACompressor decompressor_local;
            if (!template_decompress_raw_double_repeated(
                    decompressor_local, file_blob, raw_payload_len,
                    dvalues, kBenchPhaseRepeats, sum_unc_ns, sum_dec_ns, decoded)) {
                return false;
            }
        } else
#endif
        {
            ADD_FAILURE() << "raw double codec decompress: unsupported compression "
                          << compression_label(compression);
            return false;
        }

        int64_t sum_csv_write_ns = 0;
        for (int ph = 0; ph < kBenchPhaseRepeats; ++ph) {
            int64_t one_csv = 0;
            if (!write_decoded_doubles_csv_timed(decoded_csv_path, decoded, &one_csv)) {
                return false;
            }
            sum_csv_write_ns += one_csv;
        }
        out_csv_write_ns = sum_csv_write_ns / kBenchPhaseRepeats;

        out_decompress_ns = sum_unc_ns / kBenchPhaseRepeats;
        out_decode_ns = sum_dec_ns / kBenchPhaseRepeats;
        return true;
    }

    if (gorilla_double_path) {
        std::vector<double> decoded;
        storage::Decoder *decoder =
            storage::DecoderFactory::alloc_value_decoder(encoding, common::DOUBLE);
        if (decoder == nullptr) {
            return false;
        }
        storage::Compressor *decompressor =
            storage::CompressorFactory::alloc_compressor(compression);
        if (decompressor == nullptr) {
            decoder->~Decoder();
            common::mem_free(decoder);
            return false;
        }
        if (!factory_decompress_decode_repeated_double(
                decompressor, decoder, file_blob, encoded_uncompressed_bytes,
                dvalues, kBenchPhaseRepeats, sum_unc_ns, sum_dec_ns, decoded)) {
            return false;
        }
        decompressor->destroy();
        storage::CompressorFactory::free(decompressor);
        decoder->~Decoder();
        common::mem_free(decoder);

        int64_t sum_csv_write_ns = 0;
        for (int ph = 0; ph < kBenchPhaseRepeats; ++ph) {
            int64_t one_csv = 0;
            if (!write_decoded_doubles_csv_timed(decoded_csv_path, decoded, &one_csv)) {
                return false;
            }
            sum_csv_write_ns += one_csv;
        }
        out_csv_write_ns = sum_csv_write_ns / kBenchPhaseRepeats;

        out_decompress_ns = sum_unc_ns / kBenchPhaseRepeats;
        out_decode_ns = sum_dec_ns / kBenchPhaseRepeats;
        return true;
    }

    std::vector<int64_t> decoded;

    storage::Decoder *decoder =
        storage::DecoderFactory::alloc_value_decoder(encoding, common::INT64);
    if (decoder == nullptr) {
        return false;
    }
    storage::Compressor *decompressor =
        storage::CompressorFactory::alloc_compressor(compression);
    if (decompressor == nullptr) {
        decoder->~Decoder();
        common::mem_free(decoder);
        return false;
    }
    if (!factory_decompress_decode_repeated(
            decompressor, decoder, file_blob, encoded_uncompressed_bytes,
            values, kBenchPhaseRepeats, sum_unc_ns, sum_dec_ns, decoded)) {
        return false;
    }
    decompressor->destroy();
    storage::CompressorFactory::free(decompressor);
    decoder->~Decoder();
    common::mem_free(decoder);

    int64_t sum_csv_write_ns = 0;
    for (int ph = 0; ph < kBenchPhaseRepeats; ++ph) {
        int64_t one_csv = 0;
        if (!write_decoded_values_csv_timed(decoded_csv_path, decoded, &one_csv)) {
            return false;
        }
        sum_csv_write_ns += one_csv;
    }
    out_csv_write_ns = sum_csv_write_ns / kBenchPhaseRepeats;

    out_decompress_ns = sum_unc_ns / kBenchPhaseRepeats;
    out_decode_ns = sum_dec_ns / kBenchPhaseRepeats;
    return true;
}

struct CompressManifestRecord {
    std::string source_csv_path;
    size_t points = 0;
    size_t encoded_uncompressed_bytes = 0;
    bool raw_plain_codec = false;
};

static std::string trim_manifest_field(std::string s) {
    s = trim(s);
    if (!s.empty() && s.back() == '\r') {
        s.pop_back();
    }
    return s;
}

static bool parse_manifest_data_line(const std::string &line,
                                     std::string &dataset,
                                     std::string &algo,
                                     std::string &src_path,
                                     size_t &points,
                                     size_t &encoded_uncompressed_bytes,
                                     bool &raw_plain_codec) {
    std::istringstream iss(line);
    if (!std::getline(iss, dataset, ',')) {
        return false;
    }
    if (!std::getline(iss, algo, ',')) {
        return false;
    }
    if (!std::getline(iss, src_path, ',')) {
        return false;
    }
    std::string pts_s;
    std::string enc_s;
    std::string raw_s;
    if (!std::getline(iss, pts_s, ',')) {
        return false;
    }
    if (!std::getline(iss, enc_s, ',')) {
        return false;
    }
    if (!std::getline(iss, raw_s, ',')) {
        return false;
    }
    dataset = trim_manifest_field(dataset);
    algo = trim_manifest_field(algo);
    src_path = trim_manifest_field(src_path);
    pts_s = trim_manifest_field(pts_s);
    enc_s = trim_manifest_field(enc_s);
    raw_s = trim_manifest_field(raw_s);
    points = static_cast<size_t>(std::strtoull(pts_s.c_str(), nullptr, 10));
    encoded_uncompressed_bytes =
        static_cast<size_t>(std::strtoull(enc_s.c_str(), nullptr, 10));
    raw_plain_codec = (std::atoi(raw_s.c_str()) != 0);
    return true;
}

static bool load_compress_manifest_map(
    const std::string &manifest_path,
    std::map<std::pair<std::string, std::string>, CompressManifestRecord>
        &out_map) {
    out_map.clear();
    std::ifstream in(manifest_path.c_str());
    if (!in.good()) {
        return false;
    }
    std::string line;
    if (!std::getline(in, line)) {
        return false;
    }
    while (std::getline(in, line)) {
        if (trim(line).empty()) {
            continue;
        }
        std::string dataset;
        std::string algo;
        std::string src_path;
        size_t points = 0;
        size_t enc_bytes = 0;
        bool raw_plain = false;
        if (!parse_manifest_data_line(line, dataset, algo, src_path, points,
                                      enc_bytes, raw_plain)) {
            return false;
        }
        CompressManifestRecord rec;
        rec.source_csv_path = std::move(src_path);
        rec.points = points;
        rec.encoded_uncompressed_bytes = enc_bytes;
        rec.raw_plain_codec = raw_plain;
        out_map[{dataset, algo}] = std::move(rec);
    }
    return static_cast<bool>(in.eof() || in.good());
}

bool benchmark_compress_row(const std::string &dataset_path,
                            const std::string &dataset_name,
                            const BenchParallelConfig &cfg,
                            size_t &point_count,
                            int &max_decimal_precision_out,
                            int64_t &avg_dataset_read_ns,
                            int64_t &avg_encode_ns,
                            int64_t &avg_encode_flush_ns,
                            int64_t &avg_compress_ns,
                            int64_t &avg_bin_write_ns,
                            size_t &encoded_bytes,
                            size_t &compressed_bytes,
                            bool &raw_plain_codec_out) {
    const std::string bin_path = std::string(kBinOutputDir) + "/" + dataset_name +
                                 "_" + cfg.file_suffix + ".bin";
    const std::string csv_path = std::string(kDecodedCsvDir) + "/" + dataset_name +
                                 "_" + cfg.file_suffix + "_decoded.csv";

    (void)::remove(bin_path.c_str());
    (void)::remove(csv_path.c_str());
    int64_t ns_dr = 0, ns_enc = 0, ns_enc_flush = 0, ns_cmp = 0, ns_bw = 0;
    size_t enc_sz = 0, cmp_sz = 0;
    bool raw_plain = false;
    if (!compress_write_bin_once(dataset_path, cfg.encoding, cfg.compression, bin_path,
                                 point_count, max_decimal_precision_out, ns_dr, ns_enc,
                                 ns_enc_flush, ns_cmp, ns_bw, enc_sz, cmp_sz,
                                 raw_plain)) {
        ADD_FAILURE() << dataset_name << ' ' << cfg.algo_csv_name << " ("
                      << encoding_label(cfg.encoding) << "+"
                      << compression_label(cfg.compression) << ")";
        return false;
    }

    avg_dataset_read_ns = ns_dr;
    avg_encode_ns = ns_enc;
    avg_encode_flush_ns = ns_enc_flush;
    avg_compress_ns = ns_cmp;
    avg_bin_write_ns = ns_bw;
    encoded_bytes = enc_sz;
    compressed_bytes = cmp_sz;
    raw_plain_codec_out = raw_plain;
    return true;
}

bool benchmark_decompress_row(const CompressManifestRecord &manifest,
                              const std::string &dataset_name,
                              const BenchParallelConfig &cfg,
                              int64_t &avg_dataset_verify_read_ns,
                              int64_t &avg_bin_read_ns,
                              int64_t &avg_decompress_ns,
                              int64_t &avg_decode_ns,
                              int64_t &avg_csv_write_ns,
                              size_t &compressed_bytes) {
    const std::string bin_path = std::string(kBinOutputDir) + "/" + dataset_name +
                                 "_" + cfg.file_suffix + ".bin";
    const std::string csv_path = std::string(kDecodedCsvDir) + "/" + dataset_name +
                                 "_" + cfg.file_suffix + "_decoded.csv";

    (void)::remove(csv_path.c_str());
    int64_t ns_verify = 0, ns_br = 0, ns_unc = 0, ns_dec = 0, ns_csv = 0;
    size_t cmp_sz = 0;
    if (!decompress_from_bin_once(
            manifest.source_csv_path, cfg.encoding, cfg.compression, bin_path,
            csv_path, manifest.points, manifest.encoded_uncompressed_bytes,
            manifest.raw_plain_codec, ns_verify, ns_br, ns_unc, ns_dec, ns_csv,
            cmp_sz)) {
        ADD_FAILURE() << dataset_name << ' ' << cfg.algo_csv_name << " ("
                      << encoding_label(cfg.encoding) << "+"
                      << compression_label(cfg.compression) << ")";
        return false;
    }

    avg_dataset_verify_read_ns = ns_verify;
    avg_bin_read_ns = ns_br;
    avg_decompress_ns = ns_unc;
    avg_decode_ns = ns_dec;
    avg_csv_write_ns = ns_csv;
    compressed_bytes = cmp_sz;
    return true;
}

}

TEST(DatasetEncoderCompressBinScaledCsv, PerBlockScaling_SubcolumnRoundTripTmpCsv) {
    namespace fs = std::filesystem;
    const fs::path dir =
        fs::temp_directory_path() / "tsfile_dataset_scaled_block_smoke";
    std::error_code ec;
    fs::create_directories(dir, ec);
    ASSERT_FALSE(ec);

    const std::string csv = (dir / "smoke.csv").string();
    const std::string bin = (dir / "smoke_subcolumn.bin").string();
    const std::string decoded = (dir / "smoke_decoded.csv").string();

    {
        std::ofstream out(csv.c_str(), std::ios::out | std::ios::trunc);
        ASSERT_TRUE(out.good());
        for (int i = 0; i < storage::LongSubcolumnEncoder::BLOCK_SIZE; ++i) {
            out << "1\n";
        }
        out << "2.12345678\n";
    }

    size_t points = 0;
    int max_prec = 0;
    int64_t ns_dr = 0, ns_enc = 0, ns_ef = 0, ns_cmp = 0, ns_bw = 0;
    size_t enc_b = 0, cmp_b = 0;
    bool raw_plain = false;

    ASSERT_TRUE(compress_write_bin_once(csv, common::SUBCOLUMN, common::UNCOMPRESSED,
                                        bin, points, max_prec, ns_dr, ns_enc, ns_ef,
                                        ns_cmp, ns_bw, enc_b, cmp_b, raw_plain));
    ASSERT_EQ(points,
              static_cast<size_t>(storage::LongSubcolumnEncoder::BLOCK_SIZE + 1));
    EXPECT_GE(max_prec, 1);
    ASSERT_FALSE(raw_plain);
    ASSERT_GT(enc_b, 0u);

    int64_t v_dr = 0, v_br = 0, v_unc = 0, v_dec = 0, v_csv = 0;
    size_t cmp_read = 0;
    ASSERT_TRUE(decompress_from_bin_once(csv, common::SUBCOLUMN, common::UNCOMPRESSED,
                                         bin, decoded, points, enc_b, raw_plain, v_dr,
                                         v_br, v_unc, v_dec, v_csv, cmp_read));
}

TEST(DatasetCompressBench, EncodeCompressWrite) {
    std::string combined_csv_path;
    ASSERT_TRUE(prepare_combined_benchmark_dataset(combined_csv_path))
        << "Failed to merge CSVs from kDatasetDir into dataset_big_combined: "
        << kDatasetDir;

    const std::string dataset_name = basename_no_ext(combined_csv_path);
    const std::string &path = combined_csv_path;

    ensure_dir_recursive(kBinOutputDir);
    ensure_dir_recursive(kDecodedCsvDir);
    const std::string metrics_parent =
        std::string(kWriteMetricsCsvPath).substr(
            0, std::string(kWriteMetricsCsvPath).find_last_of("/\\"));
    const std::string manifest_parent =
        std::string(kCompressManifestCsvPath).substr(
            0, std::string(kCompressManifestCsvPath).find_last_of("/\\"));
    ensure_dir_recursive(metrics_parent);
    ensure_dir_recursive(manifest_parent);

    const std::vector<BenchParallelConfig> configs = build_parallel_benchmark_configs();
    ASSERT_FALSE(configs.empty())
        << "No benchmark configs (check build flags ENABLE_* vs csv_read_write parallel set)";

    std::ofstream write_metrics(kWriteMetricsCsvPath, std::ios::out | std::ios::trunc);
    std::ofstream manifest_out(kCompressManifestCsvPath, std::ios::out | std::ios::trunc);
    ASSERT_TRUE(write_metrics.good()) << kWriteMetricsCsvPath;
    ASSERT_TRUE(manifest_out.good()) << kCompressManifestCsvPath;

    write_metrics << "Dataset,Encoding Algorithm,Write Total Time Nanos,Dataset Read "
                     "Time Nanos,Write CPU Time Nanos,Write IO Time Nanos,Points,Max "
                     "Decimal Precision,Multiplier,TsFile Size Bytes,Write Encode Loop Time "
                     "Nanos,Write Encode Flush Time Nanos,Write Compress Time Nanos\n";
    manifest_out << "Dataset,Encoding Algorithm,Source CSV Path,Points,Encoded "
                    "Uncompressed Bytes,Raw Plain Codec\n";

    for (size_t cfg_idx = 0; cfg_idx < configs.size(); ++cfg_idx) {
        const BenchParallelConfig &cfg = configs[cfg_idx];
        if (cfg_idx == 0) {
                size_t warm_points = 0;
                int warm_max_prec = 0;
                int64_t warm_dr = 0, warm_enc = 0, warm_enc_flush = 0, warm_cmp = 0;
                int64_t warm_bw = 0;
                size_t warm_enc_b = 0, warm_cmp_b = 0;
                bool warm_raw = false;
                std::fprintf(stderr,
                             "[EncodeCompressWrite] dataset=%s warmup (discarded) "
                             "algorithm=%s (%s + %s)\n",
                             dataset_name.c_str(), cfg.algo_csv_name,
                             encoding_label(cfg.encoding),
                             compression_label(cfg.compression));
                std::fflush(stderr);
                if (!benchmark_compress_row(path, dataset_name, cfg, warm_points,
                                            warm_max_prec, warm_dr, warm_enc,
                                            warm_enc_flush, warm_cmp, warm_bw,
                                            warm_enc_b, warm_cmp_b, warm_raw)) {
                    continue;
                }
            }

            int64_t avg_dr = 0, avg_enc = 0, avg_enc_flush = 0, avg_cmp = 0;
            int64_t avg_bw = 0;
            size_t enc_b = 0, cmp_b = 0;
            size_t points = 0;
            int max_prec = 0;
            bool raw_plain_out = false;
            std::fprintf(stderr,
                         "[EncodeCompressWrite] dataset=%s path=%s algorithm=%s "
                         "(%s + %s)\n",
                         dataset_name.c_str(), path.c_str(), cfg.algo_csv_name,
                         encoding_label(cfg.encoding), compression_label(cfg.compression));
            std::fflush(stderr);
            if (!benchmark_compress_row(path, dataset_name, cfg, points, max_prec, avg_dr,
                                        avg_enc, avg_enc_flush, avg_cmp, avg_bw, enc_b,
                                        cmp_b, raw_plain_out)) {
                continue;
            }

            std::fprintf(stderr,
                         "[EncodeCompressWrite] compressed_size_bytes=%zu "
                         "encoded_uncompressed_bytes=%zu "
                         "(compressed / encoded = %.6f)\n",
                         cmp_b, enc_b,
                         enc_b > 0 ? static_cast<double>(cmp_b) /
                                         static_cast<double>(enc_b)
                                   : 0.0);
            std::fflush(stderr);

            const int bounded_precision = std::min(max_prec, kMaxDecimalPrecision);
            const int64_t multiplier = multiplier_for_precision(bounded_precision);

            const int64_t write_total_ns =
                avg_dr + avg_enc + avg_enc_flush + avg_cmp + avg_bw;
            const int64_t write_cpu_ns = avg_enc + avg_enc_flush + avg_cmp;

            const int64_t bin_size_bytes = static_cast<int64_t>(cmp_b);

            write_metrics << dataset_name << ',' << cfg.algo_csv_name << ','
                          << write_total_ns << ',' << avg_dr << ',' << write_cpu_ns << ','
                          << avg_bw << ',' << points << ',' << bounded_precision << ','
                          << multiplier << ',' << bin_size_bytes << ',' << avg_enc << ','
                          << avg_enc_flush << ',' << avg_cmp << '\n';

            manifest_out << dataset_name << ',' << cfg.algo_csv_name << ',' << path << ','
                         << points << ',' << enc_b << ','
                         << (raw_plain_out ? 1 : 0) << '\n';
    }

    write_metrics.close();
    manifest_out.close();
    ASSERT_TRUE(write_metrics.good());
    ASSERT_TRUE(manifest_out.good());
}

TEST(DatasetCompressBench, DecodeWriteDecoded) {
    ensure_dir_recursive(kDecodedCsvDir);
    const std::string metrics_parent =
        std::string(kReadMetricsCsvPath).substr(
            0, std::string(kReadMetricsCsvPath).find_last_of("/\\"));
    ensure_dir_recursive(metrics_parent);

    std::map<std::pair<std::string, std::string>, CompressManifestRecord> manifest_map;
    ASSERT_TRUE(load_compress_manifest_map(kCompressManifestCsvPath, manifest_map))
        << "Manifest missing or invalid (run EncodeCompressWrite first): "
        << kCompressManifestCsvPath;
    ASSERT_FALSE(manifest_map.empty()) << "Manifest has no data rows";

    const std::string dataset_name = manifest_map.begin()->first.first;

    const std::vector<BenchParallelConfig> configs = build_parallel_benchmark_configs();
    ASSERT_FALSE(configs.empty())
        << "No benchmark configs (check build flags ENABLE_* vs csv_read_write parallel set)";

    std::ofstream read_metrics(kReadMetricsCsvPath, std::ios::out | std::ios::trunc);
    ASSERT_TRUE(read_metrics.good()) << kReadMetricsCsvPath;

    read_metrics << "Dataset,Encoding Algorithm,Read Total Time Nanos,Read CPU Time "
                    "Nanos,Dataset Verify Read Time Nanos,Read IO Time Nanos,Decoded CSV "
                    "Write Time Nanos,Points,"
                    "TsFile Size Bytes\n";

    for (size_t cfg_idx = 0; cfg_idx < configs.size(); ++cfg_idx) {
        const BenchParallelConfig &cfg = configs[cfg_idx];
        const auto it =
            manifest_map.find(std::make_pair(dataset_name, cfg.algo_csv_name));
        if (it == manifest_map.end()) {
            ADD_FAILURE() << "No manifest row for dataset=" << dataset_name
                          << " algorithm=" << cfg.algo_csv_name;
            continue;
        }
        const CompressManifestRecord &manifest_rec = it->second;

        if (cfg_idx == 0) {
                int64_t warm_verify = 0, warm_br = 0, warm_unc = 0, warm_dec = 0,
                        warm_csv = 0;
                size_t warm_cmp = 0;
                std::fprintf(stderr,
                             "[DecodeWriteDecoded] dataset=%s warmup (discarded) "
                             "algorithm=%s (%s + %s)\n",
                             dataset_name.c_str(), cfg.algo_csv_name,
                             encoding_label(cfg.encoding),
                             compression_label(cfg.compression));
                std::fflush(stderr);
                if (!benchmark_decompress_row(manifest_rec, dataset_name, cfg, warm_verify,
                                              warm_br, warm_unc, warm_dec, warm_csv,
                                              warm_cmp)) {
                    continue;
                }
            }

            int64_t avg_verify = 0, avg_br = 0, avg_unc = 0, avg_dec = 0, avg_csv = 0;
            size_t cmp_b = 0;
            std::fprintf(stderr,
                         "[DecodeWriteDecoded] dataset=%s algorithm=%s (%s + %s)\n",
                         dataset_name.c_str(), cfg.algo_csv_name,
                         encoding_label(cfg.encoding), compression_label(cfg.compression));
            std::fflush(stderr);
            if (!benchmark_decompress_row(manifest_rec, dataset_name, cfg, avg_verify,
                                          avg_br, avg_unc, avg_dec, avg_csv, cmp_b)) {
                continue;
            }

            const int64_t read_total_ns =
                avg_verify + avg_br + avg_unc + avg_dec + avg_csv;
            const int64_t read_cpu_ns = avg_unc + avg_dec;
            const int64_t bin_size_bytes = static_cast<int64_t>(cmp_b);

            read_metrics << dataset_name << ',' << cfg.algo_csv_name << ',' << read_total_ns
                         << ',' << read_cpu_ns << ',' << avg_verify << ',' << avg_br << ','
                         << avg_csv << ',' << manifest_rec.points << ',' << bin_size_bytes
                         << '\n';
    }

    read_metrics.close();
    ASSERT_TRUE(read_metrics.good());
}
