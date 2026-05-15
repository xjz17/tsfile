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
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>

#include "common/allocator/byte_stream.h"
#include "common/db_common.h"
#include "common/global.h"
#include "compress/compressor_factory.h"
#include "encoding/decoder_factory.h"
#include "encoding/encoder_factory.h"
#include "utils/errno_define.h"

namespace {

constexpr const char kDatasetDir[] = "/home/allen/xjz17/subcolumn/dataset_tsfile";

constexpr const char kBinOutputDir[] =
    "/home/allen/xjz17/subcolumn/result/encoder_compress_bin/bins";

constexpr const char kWriteMetricsCsvPath[] =
    "/home/allen/xjz17/subcolumn/result/encoder_compress_bin/"
    "encoder_compress_roundtrip_write_metrics.csv";

constexpr const char kReadMetricsCsvPath[] =
    "/home/allen/xjz17/subcolumn/result/encoder_compress_bin/"
    "encoder_compress_roundtrip_read_metrics.csv";

constexpr const char kDecodedCsvDir[] =
    "/home/allen/xjz17/subcolumn/result/encoder_compress_bin/decoded_csv";

constexpr int kCodecBenchTimingRepeats = 10;

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

/** Timed section: text-mode {@code std::getline} loop only (lines stored for later parse). */
static bool read_csv_lines_timed(const std::string &path,
                                 std::vector<std::string> &lines,
                                 int64_t *out_line_read_ns) {
    lines.clear();
    std::ifstream in(path.c_str());
    if (!in.good()) {
        return false;
    }
    std::string line;
    line.reserve(256);
    const auto t0 = std::chrono::steady_clock::now();
    while (std::getline(in, line)) {
        lines.push_back(std::move(line));
    }
    const auto t1 = std::chrono::steady_clock::now();
    if (out_line_read_ns != nullptr) {
        *out_line_read_ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
    }
    return !in.bad();
}

static bool read_entire_file_bytes(const std::string &path, std::string &out,
                                   int64_t *out_read_ns) {
    std::ifstream in(path.c_str(), std::ios::binary | std::ios::ate);
    if (!in.good()) {
        return false;
    }
    const std::streamoff sz_off = in.tellg();
    if (sz_off < 0) {
        return false;
    }
    const auto sz = static_cast<size_t>(sz_off);
    in.seekg(0, std::ios::beg);
    out.resize(sz);
    const auto t0 = std::chrono::steady_clock::now();
    if (sz > 0) {
        in.read(out.data(), static_cast<std::streamsize>(sz));
    }
    const auto t1 = std::chrono::steady_clock::now();
    if (out_read_ns != nullptr) {
        *out_read_ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
    }
    return static_cast<bool>(in);
}

static void parse_scaled_int64_from_csv_lines(const std::vector<std::string> &lines,
                                              std::vector<int64_t> &out_values,
                                              int &max_decimal_precision) {
    max_decimal_precision = 0;
    out_values.clear();
    for (const std::string &ln : lines) {
        const std::string value = first_column(ln);
        if (value.empty()) {
            continue;
        }
        max_decimal_precision =
            std::max(max_decimal_precision, decimal_precision(value));
    }
    const int64_t mult =
        multiplier_for_precision(std::min(max_decimal_precision, kMaxDecimalPrecision));
    for (const std::string &ln : lines) {
        const std::string value = first_column(ln);
        if (value.empty()) {
            continue;
        }
        out_values.push_back(to_scaled_int64(value, mult));
    }
}

static void parse_scaled_int64_from_csv_content(const std::string &content,
                                                std::vector<int64_t> &out_values,
                                                int &max_decimal_precision) {
    std::vector<std::string> lines;
    std::istringstream iss(content);
    std::string line;
    while (std::getline(iss, line)) {
        lines.push_back(std::move(line));
    }
    parse_scaled_int64_from_csv_lines(lines, out_values, max_decimal_precision);
}

void load_scaled_int64_column(const std::string &dataset_file,
                              std::vector<int64_t> &out_values,
                              int &max_decimal_precision) {
    std::string blob;
    ASSERT_TRUE(read_entire_file_bytes(dataset_file, blob, nullptr))
        << "open/read dataset: " << dataset_file;
    parse_scaled_int64_from_csv_content(blob, out_values, max_decimal_precision);
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

/** Same 13-way parallel layout as {@code csv_read_write_test.cc} CompareCsvReadWriteEncodings. */
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
        storage::Encoder *te =
            storage::EncoderFactory::alloc_value_encoder(c.encoding, common::INT64);
        if (te == nullptr) {
            continue;
        }
        te->destroy();
        storage::EncoderFactory::free(te);
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

bool roundtrip_once(const std::string &dataset_csv_path,
                    common::TSEncoding encoding,
                    common::CompressionType compression,
                    const std::string &bin_path,
                    const std::string &decoded_csv_path,
                    int64_t &out_dataset_read_ns,
                    int64_t &out_encode_ns,
                    int64_t &out_compress_ns,
                    int64_t &out_bin_write_ns,
                    int64_t &out_bin_read_ns,
                    int64_t &out_decompress_ns,
                    int64_t &out_decode_ns,
                    int64_t &out_csv_write_ns,
                    size_t &out_encoded_bytes,
                    size_t &out_compressed_bytes) {
    out_dataset_read_ns = out_encode_ns = out_compress_ns = 0;
    out_bin_write_ns = out_bin_read_ns = 0;
    out_decompress_ns = out_decode_ns = out_csv_write_ns = 0;
    out_encoded_bytes = out_compressed_bytes = 0;

    std::vector<int64_t> values;
    int max_decimal_precision = 0;

    std::vector<std::string> csv_lines;
    if (!read_csv_lines_timed(dataset_csv_path, csv_lines, &out_dataset_read_ns)) {
        return false;
    }
    parse_scaled_int64_from_csv_lines(csv_lines, values, max_decimal_precision);

    if (values.empty()) {
        return false;
    }
    (void)max_decimal_precision;

    storage::Encoder *encoder =
        storage::EncoderFactory::alloc_value_encoder(encoding, common::INT64);
    if (encoder == nullptr) {
        return false;
    }

    int64_t sum_encode_ns = 0;
    std::vector<uint8_t> encoded;

    for (int rep = 0; rep < kCodecBenchTimingRepeats; ++rep) {
        common::ByteStream encode_stream(kByteStreamPageSize, common::MOD_DEFAULT);
        encoder->reset();
        const auto t_enc0 = std::chrono::steady_clock::now();
        for (int64_t v : values) {
            if (encoder->encode(v, encode_stream) != common::E_OK) {
                encoder->destroy();
                storage::EncoderFactory::free(encoder);
                return false;
            }
        }
        if (encoder->flush(encode_stream) != common::E_OK) {
            encoder->destroy();
            storage::EncoderFactory::free(encoder);
            return false;
        }
        const auto t_enc1 = std::chrono::steady_clock::now();
        sum_encode_ns +=
            std::chrono::duration_cast<std::chrono::nanoseconds>(t_enc1 - t_enc0)
                .count();
        byte_stream_to_vector(encode_stream, encoded);
    }
    encoder->destroy();
    storage::EncoderFactory::free(encoder);

    if (encoded.size() > static_cast<size_t>(UINT32_MAX)) {
        return false;
    }

    storage::Compressor *compressor =
        storage::CompressorFactory::alloc_compressor(compression);
    if (compressor == nullptr) {
        return false;
    }

    int64_t sum_compress_ns = 0;
    std::vector<char> compressed_owned;

    for (int rep = 0; rep < kCodecBenchTimingRepeats; ++rep) {
        if (compressor->reset(true) != common::E_OK) {
            compressor->destroy();
            storage::CompressorFactory::free(compressor);
            return false;
        }
        char *compressed_ptr = nullptr;
        uint32_t compressed_len = 0;
        const auto t_cmp0 = std::chrono::steady_clock::now();
        const int cr =
            compressor->compress(reinterpret_cast<char *>(encoded.data()),
                                 static_cast<uint32_t>(encoded.size()),
                                 compressed_ptr, compressed_len);
        const auto t_cmp1 = std::chrono::steady_clock::now();
        if (cr != common::E_OK) {
            compressor->destroy();
            storage::CompressorFactory::free(compressor);
            return false;
        }
        sum_compress_ns +=
            std::chrono::duration_cast<std::chrono::nanoseconds>(t_cmp1 - t_cmp0)
                .count();
        if (rep == kCodecBenchTimingRepeats - 1) {
            compressed_owned.assign(compressed_ptr,
                                    compressed_ptr + compressed_len);
        }
        compressor->after_compress(compressed_ptr);
    }
    compressor->destroy();
    storage::CompressorFactory::free(compressor);

    const auto t_bw0 = std::chrono::steady_clock::now();
    {
        std::ofstream bout(bin_path.c_str(),
                           std::ios::out | std::ios::trunc | std::ios::binary);
        if (!bout.good()) {
            return false;
        }
        bout.write(compressed_owned.data(),
                   static_cast<std::streamsize>(compressed_owned.size()));
        if (!bout.good()) {
            return false;
        }
    }
    const auto t_bw1 = std::chrono::steady_clock::now();

    const auto t_br0 = std::chrono::steady_clock::now();
    std::vector<char> file_blob;
    {
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
        file_blob.resize(static_cast<size_t>(sz));
        bin.read(file_blob.data(), sz);
        if (!bin.good()) {
            return false;
        }
    }
    const auto t_br1 = std::chrono::steady_clock::now();

    if (file_blob.size() != compressed_owned.size() ||
        std::memcmp(file_blob.data(), compressed_owned.data(),
                    compressed_owned.size()) != 0) {
        ADD_FAILURE() << "bin content mismatch vs compressed payload";
        return false;
    }

    storage::Compressor *decompressor =
        storage::CompressorFactory::alloc_compressor(compression);
    if (decompressor == nullptr) {
        return false;
    }

    storage::Decoder *decoder =
        storage::DecoderFactory::alloc_value_decoder(encoding, common::INT64);
    if (decoder == nullptr) {
        decompressor->destroy();
        storage::CompressorFactory::free(decompressor);
        return false;
    }

    int64_t sum_unc_ns = 0;
    int64_t sum_dec_ns = 0;
    std::vector<int64_t> decoded;
    decoded.reserve(values.size());

    for (int rep = 0; rep < kCodecBenchTimingRepeats; ++rep) {
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
            compressed_owned.data(),
            static_cast<uint32_t>(compressed_owned.size()),
            uncompressed_ptr, uncompressed_len);
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

        if (static_cast<size_t>(uncompressed_len) != encoded.size()) {
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

    decoder->~Decoder();
    common::mem_free(decoder);
    decompressor->destroy();
    storage::CompressorFactory::free(decompressor);

    /* Decoded CSV: open + flush outside timer; timed interval is only formatted writes into
     * the stream buffer (header + value lines), excluding flush to storage. */
    std::ofstream csv_out(decoded_csv_path.c_str(),
                          std::ios::out | std::ios::trunc);
    if (!csv_out.good()) {
        return false;
    }
    const auto t_csv0 = std::chrono::steady_clock::now();
    csv_out << "value\n";
    for (int64_t v : decoded) {
        csv_out << v << '\n';
    }
    const auto t_csv1 = std::chrono::steady_clock::now();
    csv_out.flush();
    if (!csv_out.good()) {
        return false;
    }

    out_encode_ns = sum_encode_ns / kCodecBenchTimingRepeats;
    out_compress_ns = sum_compress_ns / kCodecBenchTimingRepeats;
    out_bin_write_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(t_bw1 - t_bw0)
            .count();
    out_bin_read_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(t_br1 - t_br0)
            .count();
    out_decompress_ns = sum_unc_ns / kCodecBenchTimingRepeats;
    out_decode_ns = sum_dec_ns / kCodecBenchTimingRepeats;
    out_csv_write_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(t_csv1 - t_csv0)
            .count();
    out_encoded_bytes = encoded.size();
    out_compressed_bytes = compressed_owned.size();
    return true;
}

bool benchmark_parallel_row(const std::string &dataset_path,
                            const std::string &dataset_name,
                            const BenchParallelConfig &cfg,
                            size_t &point_count,
                            int &max_decimal_precision_out,
                            int64_t &avg_dataset_read_ns,
                            int64_t &avg_encode_ns,
                            int64_t &avg_compress_ns,
                            int64_t &avg_bin_write_ns,
                            int64_t &avg_bin_read_ns,
                            int64_t &avg_decompress_ns,
                            int64_t &avg_decode_ns,
                            int64_t &avg_csv_write_ns,
                            size_t &encoded_bytes,
                            size_t &compressed_bytes) {
    std::vector<int64_t> probe;
    int probe_prec = 0;
    load_scaled_int64_column(dataset_path, probe, probe_prec);
    if (probe.empty()) {
        return false;
    }
    point_count = probe.size();
    max_decimal_precision_out = probe_prec;

    const std::string bin_path = std::string(kBinOutputDir) + "/" + dataset_name +
                                 "_" + cfg.file_suffix + ".bin";
    const std::string csv_path = std::string(kDecodedCsvDir) + "/" + dataset_name +
                                 "_" + cfg.file_suffix + "_decoded.csv";

    (void)::remove(bin_path.c_str());
    (void)::remove(csv_path.c_str());
    int64_t ns_dr = 0, ns_enc = 0, ns_cmp = 0, ns_bw = 0, ns_br = 0;
    int64_t ns_unc = 0, ns_dec = 0, ns_csv = 0;
    size_t enc_sz = 0, cmp_sz = 0;
    if (!roundtrip_once(dataset_path, cfg.encoding, cfg.compression, bin_path, csv_path,
                        ns_dr, ns_enc, ns_cmp, ns_bw, ns_br, ns_unc, ns_dec, ns_csv,
                        enc_sz, cmp_sz)) {
        ADD_FAILURE() << dataset_name << ' ' << cfg.algo_csv_name << " ("
                      << encoding_label(cfg.encoding) << "+"
                      << compression_label(cfg.compression) << ")";
        return false;
    }

    avg_dataset_read_ns = ns_dr;
    avg_encode_ns = ns_enc;
    avg_compress_ns = ns_cmp;
    avg_bin_write_ns = ns_bw;
    avg_bin_read_ns = ns_br;
    avg_decompress_ns = ns_unc;
    avg_decode_ns = ns_dec;
    avg_csv_write_ns = ns_csv;
    encoded_bytes = enc_sz;
    compressed_bytes = cmp_sz;
    return true;
}

}

TEST(DatasetEncoderCompressBinBench, EncodeCompressBinRoundtripCsv) {
    const std::vector<std::string> csv_paths = list_csv_files(kDatasetDir);
    ASSERT_FALSE(csv_paths.empty())
        << "No CSV under dataset dir (check kDatasetDir): " << kDatasetDir;

    ensure_dir_recursive(kBinOutputDir);
    ensure_dir_recursive(kDecodedCsvDir);
    const std::string metrics_parent =
        std::string(kWriteMetricsCsvPath).substr(
            0, std::string(kWriteMetricsCsvPath).find_last_of('/'));
    ensure_dir_recursive(metrics_parent);

    const std::vector<BenchParallelConfig> configs = build_parallel_benchmark_configs();
    ASSERT_FALSE(configs.empty())
        << "No benchmark configs (check build flags ENABLE_* vs csv_read_write parallel set)";

    std::ofstream write_metrics(kWriteMetricsCsvPath, std::ios::out | std::ios::trunc);
    std::ofstream read_metrics(kReadMetricsCsvPath, std::ios::out | std::ios::trunc);
    ASSERT_TRUE(write_metrics.good()) << kWriteMetricsCsvPath;
    ASSERT_TRUE(read_metrics.good()) << kReadMetricsCsvPath;

    write_metrics << "Dataset,Encoding Algorithm,Write Total Time Nanos,Dataset Read "
                     "Time Nanos,Write CPU Time Nanos,Write IO Time Nanos,Points,Max "
                     "Decimal Precision,Multiplier,TsFile Size Bytes\n";
    read_metrics << "Dataset,Encoding Algorithm,Read Total Time Nanos,Read CPU Time "
                    "Nanos,Read IO Time Nanos,Decoded CSV Write Time Nanos,Points,"
                    "TsFile Size Bytes\n";

    for (const std::string &path : csv_paths) {
        const std::string dataset_name = basename_no_ext(path);
        for (const BenchParallelConfig &cfg : configs) {
            int64_t avg_dr = 0, avg_enc = 0, avg_cmp = 0, avg_bw = 0, avg_br = 0;
            int64_t avg_unc = 0, avg_dec = 0, avg_csv = 0;
            size_t enc_b = 0, cmp_b = 0;
            size_t points = 0;
            int max_prec = 0;
            if (!benchmark_parallel_row(path, dataset_name, cfg, points, max_prec, avg_dr,
                                        avg_enc, avg_cmp, avg_bw, avg_br, avg_unc,
                                        avg_dec, avg_csv, enc_b, cmp_b)) {
                continue;
            }

            const int bounded_precision = std::min(max_prec, kMaxDecimalPrecision);
            const int64_t multiplier = multiplier_for_precision(bounded_precision);

            const int64_t write_total_ns =
                avg_dr + avg_enc + avg_cmp + avg_bw;
            const int64_t write_cpu_ns = avg_enc + avg_cmp;

            const int64_t read_total_ns =
                avg_br + avg_unc + avg_dec + avg_csv;
            const int64_t read_cpu_ns = avg_unc + avg_dec;

            const int64_t bin_size_bytes = static_cast<int64_t>(cmp_b);
            (void)enc_b;

            write_metrics << dataset_name << ',' << cfg.algo_csv_name << ','
                          << write_total_ns << ',' << avg_dr << ',' << write_cpu_ns << ','
                          << avg_bw << ',' << points << ',' << bounded_precision << ','
                          << multiplier << ',' << bin_size_bytes << '\n';

            read_metrics << dataset_name << ',' << cfg.algo_csv_name << ','
                         << read_total_ns << ',' << read_cpu_ns << ',' << avg_br << ','
                         << avg_csv << ',' << points << ',' << bin_size_bytes << '\n';
        }
    }

    write_metrics.close();
    read_metrics.close();
    ASSERT_TRUE(write_metrics.good());
    ASSERT_TRUE(read_metrics.good());
}
