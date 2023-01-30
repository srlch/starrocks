// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <memory>
#include <vector>

#include "common/config.h"
#include "common/statusor.h"
#include "gutil/macros.h"

namespace starrocks {
class MemTracker;
class SlotDescriptor;
class Chunk;
} // namespace starrocks

namespace starrocks::lake {

class DeltaWriterImpl;
class TabletManager;
class TabletWriter;

struct LakeDeltaWriterOptions {
    int64_t tablet_id = -1;
    int64_t txn_id = -1;
    int64_t partition_id = -1;
    int64_t table_id = -1;
    int64_t max_buffer_size = config::write_buffer_size;
    const std::vector<SlotDescriptor*>* slots = nullptr;
    std::string merge_condition;
    bool miss_auto_increment_column = false;
    bool abort_delete = false;
};

class DeltaWriter {
    using Chunk = starrocks::Chunk;

public:
    using Ptr = std::unique_ptr<DeltaWriter>;

    // Does NOT take the ownership of |tablet_manager| and |mem_tracker| and params in |option|
    static Ptr create(const LakeDeltaWriterOptions& option, TabletManager* tablet_manager, MemTracker* mem_tracker);

    explicit DeltaWriter(DeltaWriterImpl* impl) : _impl(impl) {}

    ~DeltaWriter();

    DISALLOW_COPY_AND_MOVE(DeltaWriter);

    // NOTE: It's ok to invoke this method in a bthread, there is no I/O operation in this method.
    [[nodiscard]] Status open();

    // NOTE: Do NOT invoke this method in a bthread.
    [[nodiscard]] Status write(const Chunk& chunk, const uint32_t* indexes, uint32_t indexes_size);

    // NOTE: Do NOT invoke this method in a bthread.
    [[nodiscard]] Status finish();

    // Manual flush, mainly used in UT
    // NOTE: Do NOT invoke this method in a bthread.
    [[nodiscard]] Status flush();

    // Manual flush, mainly used in UT
    // NOTE: Do NOT invoke this method in a bthread.
    [[nodiscard]] Status flush_async();

    // NOTE: Do NOT invoke this method in a bthread unless you are sure that `write()` has never been called.
    void close();

    [[nodiscard]] int64_t partition_id() const;

    [[nodiscard]] int64_t tablet_id() const;

    [[nodiscard]] int64_t txn_id() const;

    [[nodiscard]] MemTracker* mem_tracker();

    [[nodiscard]] TabletWriter* tablet_writer();

private:
    DeltaWriterImpl* _impl;
};

} // namespace starrocks::lake
