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

#ifndef READER_READ_PATH_CPU_STATS_H
#define READER_READ_PATH_CPU_STATS_H

#include <cstdint>

namespace storage {

/** Wall time spent inside page decompress + TV decode (excludes pread in chunk readers). */
void reset_read_path_cpu_stats();
void add_read_path_cpu_ns(int64_t ns);
int64_t get_read_path_cpu_ns();

}  // namespace storage

#endif  // READER_READ_PATH_CPU_STATS_H
