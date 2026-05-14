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

#include "read_path_cpu_stats.h"

#include <atomic>

namespace storage {

namespace {
std::atomic<int64_t> g_read_path_cpu_ns{0};
}  // namespace

void reset_read_path_cpu_stats() {
    g_read_path_cpu_ns.store(0, std::memory_order_relaxed);
}

void add_read_path_cpu_ns(int64_t ns) {
    if (ns > 0) {
        g_read_path_cpu_ns.fetch_add(ns, std::memory_order_relaxed);
    }
}

int64_t get_read_path_cpu_ns() {
    return g_read_path_cpu_ns.load(std::memory_order_relaxed);
}

}  // namespace storage
