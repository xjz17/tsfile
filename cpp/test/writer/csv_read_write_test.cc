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

#include <algorithm>
#include <array>
#include <cctype>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include "common/schema.h"
#include "common/global.h"
#include "common/tsfile_common.h"
#include "reader/qds_without_timegenerator.h"
#include "reader/tsfile_reader.h"
#include "writer/tsfile_writer.h"

namespace storage {
namespace fs = std::filesystem;

class CsvReadWriteTest : public ::testing::Test {
   protected:
    static const std::string kParentDir;
    static const std::string kInputParentDir;
    static const std::string kOutputParentDir;
    static const std::string kTsFileOutputDir;
    static const std::string kWriteResultCsvPath;
    static const std::string kReadResultCsvPath;
    static const std::string kDeviceName;
    static const std::string kMeasurementName;
    static const int kRepeatTimes = 20;
    static const int kMaxDecimalPrecision = 8;

    struct EncodingConfig {
        common::TSEncoding encoding;
        const char *name;
        const char *file_suffix;
    };

    struct DatasetProfile {
        std::string file_path;
        std::string dataset_name;
        int64_t point_count = 0;
        int max_decimal_precision = 0;
    };

    struct WriteBenchmarkResult {
        int64_t total_time_ns = 0;
        int64_t dataset_read_time_ns = 0;
        int64_t cpu_time_ns = 0;
        int64_t io_time_ns = 0;
        int64_t tsfile_size_bytes = 0;
    };

    struct ReadBenchmarkResult {
        int64_t total_time_ns = 0;
        int64_t cpu_time_ns = 0;
        int64_t io_time_ns = 0;
        int64_t point_count = 0;
        int64_t tsfile_size_bytes = 0;
    };

    struct WriteIoConfig {
        bool cold_io = true;
        bool write_o_sync = true;
        int64_t fsync_chunk_kb = 64;
    };

    static bool env_bool(const char *name, bool default_value) {
        const char *raw = std::getenv(name);
        if (raw == nullptr || *raw == '\0') {
            return default_value;
        }
        std::string s(raw);
        while (!s.empty() && std::isspace(static_cast<unsigned char>(s.front()))) {
            s.erase(s.begin());
        }
        while (!s.empty() && std::isspace(static_cast<unsigned char>(s.back()))) {
            s.pop_back();
        }
        if (s == "1" || s == "true" || s == "TRUE" || s == "on" || s == "ON") {
            return true;
        }
        if (s == "0" || s == "false" || s == "FALSE" || s == "off" || s == "OFF") {
            return false;
        }
        return default_value;
    }

    static int64_t env_int64(const char *name, int64_t default_value) {
        const char *raw = std::getenv(name);
        if (raw == nullptr || *raw == '\0') {
            return default_value;
        }
        char *end = nullptr;
        const long long value = std::strtoll(raw, &end, 10);
        if (end == raw) {
            return default_value;
        }
        return static_cast<int64_t>(value);
    }

    static WriteIoConfig load_write_io_config() {
        WriteIoConfig cfg;
        cfg.cold_io = env_bool("TSFILE_BENCHMARK_COLD_IO", true);
        cfg.write_o_sync = env_bool("TSFILE_WRITE_O_SYNC", cfg.cold_io);
        cfg.fsync_chunk_kb = env_int64("TSFILE_WRITE_FSYNC_CHUNK_KB", cfg.cold_io ? 64 : 0);
        if (cfg.fsync_chunk_kb < 0) {
            cfg.fsync_chunk_kb = 0;
        }
        return cfg;
    }

    static std::string trim(const std::string &s) {
        size_t start = 0;
        while (start < s.size() && std::isspace(static_cast<unsigned char>(s[start]))) {
            ++start;
        }
        size_t end = s.size();
        while (end > start && std::isspace(static_cast<unsigned char>(s[end - 1]))) {
            --end;
        }
        return s.substr(start, end - start);
    }

    static std::string first_column(const std::string &line) {
        const size_t comma_pos = line.find(',');
        if (comma_pos == std::string::npos) {
            return trim(line);
        }
        return trim(line.substr(0, comma_pos));
    }

    static int decimal_precision(const std::string &value) {
        const size_t dot = value.find('.');
        if (dot == std::string::npos) {
            return 0;
        }
        return static_cast<int>(value.size() - dot - 1);
    }

    static int64_t multiplier_for_precision(int precision) {
        int64_t multiplier = 1;
        for (int i = 0; i < precision; ++i) {
            multiplier *= 10;
        }
        return multiplier;
    }

    static int64_t to_scaled_int64(const std::string &raw, int64_t multiplier) {
        const double d = std::strtod(raw.c_str(), nullptr);
        return static_cast<int64_t>(std::llround(d * static_cast<double>(multiplier)));
    }

    static bool dir_exists(const std::string &path) {
        std::error_code ec;
        return fs::is_directory(fs::path(path), ec);
    }

    static int64_t file_size(const std::string &path) {
        std::error_code ec;
        const auto size = fs::file_size(fs::path(path), ec);
        return ec ? 0 : static_cast<int64_t>(size);
    }

    static std::string basename_no_ext(const std::string &path) {
        return fs::path(path).stem().string();
    }

    static std::vector<std::string> list_csv_files(const std::string &dir) {
        std::vector<std::string> files;
        std::error_code ec;
        if (!fs::is_directory(fs::path(dir), ec)) {
            return files;
        }
        for (const auto &entry : fs::directory_iterator(fs::path(dir), ec)) {
            if (ec) {
                break;
            }
            if (entry.is_regular_file(ec) && !ec &&
                entry.path().extension() == ".csv") {
                files.push_back(entry.path().string());
            }
        }
        std::sort(files.begin(), files.end());
        return files;
    }

    static int64_t count_points_and_precision(
        const std::string &dataset_file, int &max_decimal_precision) {
        std::ifstream in(dataset_file.c_str());
        int64_t points = 0;
        max_decimal_precision = 0;
        std::string line;
        while (std::getline(in, line)) {
            const std::string value = first_column(line);
            if (value.empty()) {
                continue;
            }
            max_decimal_precision = std::max(max_decimal_precision, decimal_precision(value));
            ++points;
        }
        return points;
    }

    static void ensure_dir(const std::string &path) {
        std::error_code ec;
        fs::create_directories(fs::path(path), ec);
    }

    static WriteBenchmarkResult benchmark_write(
        const DatasetProfile &dataset,
        int64_t multiplier,
        common::TSEncoding encoding,
        const std::string &tsfile_path) {
        WriteBenchmarkResult result;
        const WriteIoConfig io_cfg = load_write_io_config();
        int64_t total_total_ns = 0;
        int64_t total_dataset_read_ns = 0;
        int64_t total_cpu_ns = 0;
        int64_t total_io_ns = 0;

        for (int repeat = 0; repeat < kRepeatTimes; ++repeat) {
            std::remove(tsfile_path.c_str());
            TsFileWriter writer;
            const int flags = O_WRONLY | O_CREAT | O_TRUNC
#ifndef _WIN32
                              | (io_cfg.write_o_sync ? O_SYNC : 0)
#endif
#ifdef _WIN32
                              | O_BINARY
#endif
                ;
            if (writer.open(tsfile_path, flags, 0666) != common::E_OK) {
                ADD_FAILURE() << "Failed to open tsfile: " << tsfile_path;
                return result;
            }
            if (writer.register_timeseries(
                    kDeviceName, MeasurementSchema(kMeasurementName, common::INT64,
                                                   encoding, common::LZ4)) != common::E_OK) {
                ADD_FAILURE() << "Failed to register timeseries for: " << tsfile_path;
                return result;
            }

            std::ifstream in(dataset.file_path.c_str());
            if (!in.good()) {
                ADD_FAILURE() << "Failed to open dataset file: " << dataset.file_path;
                return result;
            }

            int64_t ts = 1;
            int64_t dataset_read_ns = 0;
            int64_t cpu_ns = 0;
            int64_t io_ns = 0;
            const int64_t chunk_bytes = io_cfg.fsync_chunk_kb * 1024;
            int64_t pending_chunk_bytes = 0;
            const auto iter_begin = std::chrono::steady_clock::now();
            std::string line;
            while (std::getline(in, line)) {
                const auto read_t0 = std::chrono::steady_clock::now();
                const std::string value = first_column(line);
                int64_t scaled_value = 0;
                if (!value.empty()) {
                    scaled_value = to_scaled_int64(value, multiplier);
                }
                const auto read_t1 = std::chrono::steady_clock::now();
                dataset_read_ns +=
                    std::chrono::duration_cast<std::chrono::nanoseconds>(read_t1 - read_t0).count();
                if (value.empty()) {
                    continue;
                }

                TsRecord record(ts++, kDeviceName);
                record.add_point(kMeasurementName, scaled_value);
                const auto cpu_t0 = std::chrono::steady_clock::now();
                if (writer.write_record(record) != common::E_OK) {
                    ADD_FAILURE() << "Failed to write record for dataset: " << dataset.dataset_name;
                    return result;
                }
                const auto cpu_t1 = std::chrono::steady_clock::now();
                cpu_ns +=
                    std::chrono::duration_cast<std::chrono::nanoseconds>(cpu_t1 - cpu_t0).count();

                if (chunk_bytes > 0) {
                    pending_chunk_bytes += static_cast<int64_t>(line.size()) + 1;
                    if (pending_chunk_bytes >= chunk_bytes) {
                        const auto io_t0 = std::chrono::steady_clock::now();
                        if (writer.flush() != common::E_OK) {
                            ADD_FAILURE() << "Failed to flush tsfile chunk: " << tsfile_path;
                            return result;
                        }
                        const auto io_t1 = std::chrono::steady_clock::now();
                        io_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(io_t1 - io_t0)
                                     .count();
                        pending_chunk_bytes = 0;
                    }
                }
            }

            const auto io_t0 = std::chrono::steady_clock::now();
            if (writer.flush() != common::E_OK || writer.close() != common::E_OK) {
                ADD_FAILURE() << "Failed to flush/close tsfile: " << tsfile_path;
                return result;
            }
            const auto io_t1 = std::chrono::steady_clock::now();
            const auto iter_end = std::chrono::steady_clock::now();

            total_dataset_read_ns += dataset_read_ns;
            total_cpu_ns += cpu_ns;
            io_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(io_t1 - io_t0).count();
            total_io_ns += io_ns;
            total_total_ns +=
                std::chrono::duration_cast<std::chrono::nanoseconds>(iter_end - iter_begin).count();
        }

        result.total_time_ns = total_total_ns / kRepeatTimes;
        result.dataset_read_time_ns = total_dataset_read_ns / kRepeatTimes;
        result.cpu_time_ns = total_cpu_ns / kRepeatTimes;
        result.io_time_ns = total_io_ns / kRepeatTimes;
        result.tsfile_size_bytes = file_size(tsfile_path);
        return result;
    }

    static ReadBenchmarkResult benchmark_read(const std::string &tsfile_path) {
        ReadBenchmarkResult result;
        int64_t total_total_ns = 0;
        int64_t total_cpu_ns = 0;
        int64_t points = 0;

        for (int repeat = 0; repeat < kRepeatTimes; ++repeat) {
            TsFileReader reader;
            if (reader.open(tsfile_path) != common::E_OK) {
                ADD_FAILURE() << "Failed to open tsfile reader: " << tsfile_path;
                return result;
            }
            std::vector<std::string> select_list;
            select_list.push_back(kDeviceName + "." + kMeasurementName);
            ResultSet *tmp_qds = nullptr;
            if (reader.query(select_list, INT64_MIN, INT64_MAX, tmp_qds) != common::E_OK) {
                ADD_FAILURE() << "Failed to query tsfile: " << tsfile_path;
                return result;
            }
            auto *qds = static_cast<QDSWithoutTimeGenerator *>(tmp_qds);

            const auto t0 = std::chrono::steady_clock::now();
            bool has_next = false;
            int64_t current_points = 0;
            int64_t cpu_ns = 0;
            do {
                if (IS_FAIL(qds->next(has_next)) || !has_next) {
                    break;
                }
                const auto cpu_t0 = std::chrono::steady_clock::now();
                RowRecord *record = qds->get_row_record();
                if (record != nullptr && !record->get_fields()->empty()) {
                    ++current_points;
                }
                const auto cpu_t1 = std::chrono::steady_clock::now();
                cpu_ns +=
                    std::chrono::duration_cast<std::chrono::nanoseconds>(cpu_t1 - cpu_t0).count();
            } while (true);
            const auto t1 = std::chrono::steady_clock::now();
            reader.destroy_query_data_set(qds);
            reader.close();

            total_total_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
            total_cpu_ns += cpu_ns;
            points = current_points;
        }
        result.total_time_ns = total_total_ns / kRepeatTimes;
        result.cpu_time_ns = total_cpu_ns / kRepeatTimes;
        result.io_time_ns = std::max<int64_t>(0, result.total_time_ns - result.cpu_time_ns);
        result.point_count = points;
        result.tsfile_size_bytes = file_size(tsfile_path);
        return result;
    }

    static void write_csv_header(std::ofstream &out, const std::vector<std::string> &headers) {
        for (size_t i = 0; i < headers.size(); ++i) {
            if (i > 0) {
                out << ',';
            }
            out << headers[i];
        }
        out << '\n';
    }
};

// const std::string CsvReadWriteTest::kParentDir = "D:/github/xjz17/subcolumn/";
const std::string CsvReadWriteTest::kParentDir = "/home/allen/xjz17/subcolumn/";
const std::string CsvReadWriteTest::kInputParentDir =
    CsvReadWriteTest::kParentDir + "dataset_tsfile/";
const std::string CsvReadWriteTest::kOutputParentDir =
    CsvReadWriteTest::kParentDir + "result/tsfile_read_write_cpp_v2/";
const std::string CsvReadWriteTest::kTsFileOutputDir =
    CsvReadWriteTest::kOutputParentDir + "tsfiles_v2/";
const std::string CsvReadWriteTest::kWriteResultCsvPath =
    CsvReadWriteTest::kOutputParentDir + "write_time_tsfile_v2.csv";
const std::string CsvReadWriteTest::kReadResultCsvPath =
    CsvReadWriteTest::kOutputParentDir + "read_time_tsfile_v2.csv";
const std::string CsvReadWriteTest::kDeviceName = "device_1";
const std::string CsvReadWriteTest::kMeasurementName = "sensor_1";

TEST_F(CsvReadWriteTest, CompareCsvReadWriteEncodings) {
    libtsfile_init();
    ensure_dir(kOutputParentDir);
    ensure_dir(kTsFileOutputDir);
    if (!dir_exists(kInputParentDir)) {
        GTEST_SKIP() << "Input dataset directory not found: " << kInputParentDir;
    }
    const std::vector<std::string> dataset_files = list_csv_files(kInputParentDir);
    if (dataset_files.empty()) {
        GTEST_SKIP() << "No dataset csv files found under: " << kInputParentDir;
    }

    const std::array<EncodingConfig, 1> encodings = {{
        // {common::TS_2DIFF, "TS_2DIFF", "ts_2diff"},
        // {common::RLE, "RLE", "rle"},
        // {common::GORILLA, "GORILLA", "gorilla"},
        // {common::SPRINTZ, "SPRINTZ", "sprintz"},
        // {common::DICTIONARY, "DICTIONARY", "dictionary"},
        // {common::SUBCOLUMN, "SUBCOLUMN", "subcolumn"},
        // {common::SPRINTZ_SUBCOLUMN, "SPRINTZ_SUBCOLUMN", "sprintz_subcolumn"},
        // {common::TS_2DIFF_SUBCOLUMN, "TS_2DIFF_SUBCOLUMN", "ts_2diff_subcolumn"},
        {common::BITPACKING, "BITPACKING", "bitpacking"},
    }};

    std::ofstream write_csv(kWriteResultCsvPath.c_str(), std::ios::out | std::ios::trunc);
    std::ofstream read_csv(kReadResultCsvPath.c_str(), std::ios::out | std::ios::trunc);
    ASSERT_TRUE(write_csv.good());
    ASSERT_TRUE(read_csv.good());
    write_csv_header(
        write_csv,
        {"Dataset", "Encoding Algorithm", "Write Total Time Nanos", "Dataset Read Time Nanos",
         "Write CPU Time Nanos", "Write IO Time Nanos", "Points", "Max Decimal Precision",
         "Multiplier", "TsFile Size Bytes"});
    write_csv_header(
        read_csv,
        {"Dataset", "Encoding Algorithm", "Read Total Time Nanos", "Read CPU Time Nanos",
         "Read IO Time Nanos", "Points", "TsFile Size Bytes"});

    for (size_t i = 0; i < dataset_files.size(); ++i) {
        DatasetProfile profile;
        profile.file_path = dataset_files[i];
        profile.dataset_name = basename_no_ext(profile.file_path);
        profile.point_count =
            count_points_and_precision(profile.file_path, profile.max_decimal_precision);
        const int bounded_precision =
            std::min(profile.max_decimal_precision, kMaxDecimalPrecision);
        const int64_t multiplier = multiplier_for_precision(bounded_precision);
        std::cout << "[CsvReadWriteTest] Dataset " << (i + 1) << "/"
                  << dataset_files.size() << ": " << profile.dataset_name
                  << ", file=" << profile.file_path
                  << ", points=" << profile.point_count
                  << ", max_decimal_precision=" << profile.max_decimal_precision
                  << ", bounded_precision=" << bounded_precision
                  << ", multiplier=" << multiplier << std::endl;

        for (const auto &cfg : encodings) {
            const std::string tsfile_path = kTsFileOutputDir + profile.dataset_name + "_" +
                                            cfg.file_suffix + "_cpp_v2.tsfile";
            std::cout << "[CsvReadWriteTest]   Encoding=" << cfg.name
                      << ", output=" << tsfile_path << std::endl;
            const WriteBenchmarkResult write_result =
                benchmark_write(profile, multiplier, cfg.encoding, tsfile_path);
            std::cout << "[CsvReadWriteTest]   Write done: total_ns="
                      << write_result.total_time_ns
                      << ", cpu_ns=" << write_result.cpu_time_ns
                      << ", io_ns=" << write_result.io_time_ns
                      << ", tsfile_size=" << write_result.tsfile_size_bytes
                      << std::endl;
            const ReadBenchmarkResult read_result = benchmark_read(tsfile_path);
            std::cout << "[CsvReadWriteTest]   Read done: total_ns="
                      << read_result.total_time_ns
                      << ", cpu_ns=" << read_result.cpu_time_ns
                      << ", io_ns=" << read_result.io_time_ns
                      << ", points=" << read_result.point_count << std::endl;

            write_csv << profile.dataset_name << "," << cfg.name << ","
                      << write_result.total_time_ns << "," << write_result.dataset_read_time_ns
                      << "," << write_result.cpu_time_ns << "," << write_result.io_time_ns
                      << "," << profile.point_count << "," << bounded_precision << ","
                      << multiplier << "," << write_result.tsfile_size_bytes << '\n';

            read_csv << profile.dataset_name << "," << cfg.name << ","
                     << read_result.total_time_ns << "," << read_result.cpu_time_ns << ","
                     << read_result.io_time_ns << "," << read_result.point_count << ","
                     << read_result.tsfile_size_bytes << '\n';
        }
    }
}

}  // namespace storage
