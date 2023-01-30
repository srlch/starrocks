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

#include "storage/lake/delta_writer.h"

#include <bthread/bthread.h>

#include <memory>

#include "column/chunk.h"
#include "column/column.h"
#include "gutil/strings/util.h"
#include "runtime/current_thread.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "storage/lake/filenames.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_writer.h"
#include "storage/lake/txn_log.h"
#include "storage/memtable.h"
#include "storage/memtable_flush_executor.h"
#include "storage/memtable_sink.h"
#include "storage/primary_key_encoder.h"
#include "storage/storage_engine.h"

namespace starrocks::lake {

using Chunk = starrocks::Chunk;
using Column = starrocks::Column;
using MemTable = starrocks::MemTable;
using MemTableSink = starrocks::MemTableSink;

class TabletWriterSink : public MemTableSink {
public:
    explicit TabletWriterSink(TabletWriter* w) : _writer(w) {}

    ~TabletWriterSink() override = default;

    DISALLOW_COPY_AND_MOVE(TabletWriterSink);

    Status flush_chunk(const Chunk& chunk, starrocks::SegmentPB* segment = nullptr) override {
        RETURN_IF_ERROR(_writer->write(chunk));
        return _writer->flush();
    }

    Status flush_chunk_with_deletes(const Chunk& upserts, const Column& deletes,
                                    starrocks::SegmentPB* segment = nullptr) override {
        RETURN_IF_ERROR(_writer->flush_del_file(deletes));
        RETURN_IF_ERROR(_writer->write(upserts));
        return _writer->flush();
    }

private:
    TabletWriter* _writer;
};

/// DeltaWriterImpl

class DeltaWriterImpl {
public:
    explicit DeltaWriterImpl(const LakeDeltaWriterOptions& option, TabletManager* tablet_manager,
                             MemTracker* mem_tracker)
            : _tablet_manager(tablet_manager),
              _tablet_id(option.tablet_id),
              _txn_id(option.txn_id),
              _partition_id(option.partition_id),
              _mem_tracker(mem_tracker),
              _slots(option.slots),
              _max_buffer_size(option.max_buffer_size),
              _schema_initialized(false),
              _merge_condition(option.merge_condition),
              _miss_auto_increment_column(option.miss_auto_increment_column),
              _abort_delete(option.abort_delete),
              _table_id(option.table_id) {}

    ~DeltaWriterImpl() = default;

    DISALLOW_COPY_AND_MOVE(DeltaWriterImpl);

    [[nodiscard]] Status open();

    [[nodiscard]] Status write(const Chunk& chunk, const uint32_t* indexes, uint32_t indexes_size);

    [[nodiscard]] Status finish();

    void close();

    [[nodiscard]] int64_t partition_id() const { return _partition_id; }

    [[nodiscard]] int64_t tablet_id() const { return _tablet_id; }

    [[nodiscard]] int64_t txn_id() const { return _txn_id; }

    [[nodiscard]] MemTracker* mem_tracker() { return _mem_tracker; }

    [[nodiscard]] TabletWriter* tablet_writer() { return _tablet_writer.get(); }

    [[nodiscard]] Status flush();

    [[nodiscard]] Status flush_async();

    Status handle_partial_update();
    Status build_schema_and_writer();

private:
    Status reset_memtable();

    Status _fill_auto_increment_id(const Chunk& chunk);

    TabletManager* _tablet_manager;
    const int64_t _tablet_id;
    const int64_t _txn_id;
    const int64_t _partition_id;
    MemTracker* const _mem_tracker;

    // for load
    const std::vector<SlotDescriptor*>* const _slots;

    // for schema change
    int64_t _max_buffer_size = config::write_buffer_size;

    std::unique_ptr<TabletWriter> _tablet_writer;
    std::unique_ptr<MemTable> _mem_table;
    std::unique_ptr<MemTableSink> _mem_table_sink;
    std::unique_ptr<FlushToken> _flush_token;
    std::shared_ptr<const TabletSchema> _tablet_schema;
    Schema _vectorized_schema;
    bool _schema_initialized;

    // for partial update
    std::shared_ptr<const TabletSchema> _partial_update_tablet_schema;
    std::vector<int32_t> _referenced_column_ids;

    // for condition update
    std::string _merge_condition;

    // for auto increment
    bool _miss_auto_increment_column;
    bool _abort_delete;
    const int64_t _table_id;
};

Status DeltaWriterImpl::build_schema_and_writer() {
    if (_mem_table_sink == nullptr) {
        DCHECK(_tablet_writer == nullptr);
        ASSIGN_OR_RETURN(auto tablet, _tablet_manager->get_tablet(_tablet_id));
        ASSIGN_OR_RETURN(_tablet_schema, tablet.get_schema());
        RETURN_IF_ERROR(handle_partial_update());
        ASSIGN_OR_RETURN(_tablet_writer, tablet.new_writer());
        if (_partial_update_tablet_schema != nullptr) {
            _tablet_writer->set_tablet_schema(_partial_update_tablet_schema);
        }
        RETURN_IF_ERROR(_tablet_writer->open());
        _mem_table_sink = std::make_unique<TabletWriterSink>(_tablet_writer.get());
    }
    return Status::OK();
}

inline Status DeltaWriterImpl::reset_memtable() {
    RETURN_IF_ERROR(build_schema_and_writer());
    if (!_schema_initialized) {
        _vectorized_schema = MemTable::convert_schema(_tablet_schema.get(), _slots);
        _schema_initialized = true;
    }
    if (_slots != nullptr) {
        _mem_table = std::make_unique<MemTable>(_tablet_id, &_vectorized_schema, _slots, _mem_table_sink.get(),
                                                _mem_tracker);
    } else {
        _mem_table = std::make_unique<MemTable>(_tablet_id, &_vectorized_schema, _mem_table_sink.get(),
                                                _max_buffer_size, _mem_tracker);
    }
    _mem_table->set_abort_delete(_abort_delete);
    return Status::OK();
}

inline Status DeltaWriterImpl::flush_async() {
    Status st;
    if (_mem_table != nullptr) {
        RETURN_IF_ERROR(_mem_table->finalize());
        if (_miss_auto_increment_column && _mem_table->get_result_chunk() != nullptr) {
            _fill_auto_increment_id(*_mem_table->get_result_chunk());
        }
        st = _flush_token->submit(std::move(_mem_table));
        _mem_table.reset(nullptr);
    }
    return st;
}

inline Status DeltaWriterImpl::flush() {
    RETURN_IF_ERROR(flush_async());
    return _flush_token->wait();
}

// To developers: Do NOT perform any I/O in this method, because this method may be invoked
// in a bthread.
Status DeltaWriterImpl::open() {
    SCOPED_THREAD_LOCAL_MEM_SETTER(_mem_tracker, false);
    _flush_token = StorageEngine::instance()->memtable_flush_executor()->create_flush_token();
    if (_flush_token == nullptr) {
        return Status::InternalError("fail to create flush token");
    }
    return Status::OK();
}

Status DeltaWriterImpl::write(const Chunk& chunk, const uint32_t* indexes, uint32_t indexes_size) {
    SCOPED_THREAD_LOCAL_MEM_SETTER(_mem_tracker, false);

    if (_mem_table == nullptr) {
        RETURN_IF_ERROR(reset_memtable());
    }
    Status st;
    bool full = _mem_table->insert(chunk, indexes, 0, indexes_size);
    if (_mem_tracker->limit_exceeded()) {
        VLOG(2) << "Flushing memory table due to memory limit exceeded";
        st = flush();
    } else if (_mem_tracker->parent() && _mem_tracker->parent()->limit_exceeded()) {
        VLOG(2) << "Flushing memory table due to parent memory limit exceeded";
        st = flush();
    } else if (full) {
        st = flush_async();
    }
    return st;
}

Status DeltaWriterImpl::handle_partial_update() {
    if (_slots == nullptr) return Status::OK();
    const std::size_t partial_cols_num = [this]() {
        if (this->_slots->size() > 0 && this->_slots->back()->col_name() == "__op") {
            return this->_slots->size() - 1;
        } else {
            return this->_slots->size();
        }
    }();
    // maybe partial update, change to partial tablet schema
    if (_tablet_schema->keys_type() == KeysType::PRIMARY_KEYS && partial_cols_num < _tablet_schema->num_columns()) {
        _referenced_column_ids.reserve(partial_cols_num);
        for (auto i = 0; i < partial_cols_num; ++i) {
            const auto& slot_col_name = (*_slots)[i]->col_name();
            int32_t index = _tablet_schema->field_index(slot_col_name);
            if (index < 0) {
                return Status::InvalidArgument(strings::Substitute("Invalid column name: $0", slot_col_name));
            }
            _referenced_column_ids.push_back(index);
        }
        _partial_update_tablet_schema = TabletSchema::create(*_tablet_schema, _referenced_column_ids);
        auto sort_key_idxes = _tablet_schema->sort_key_idxes();
        std::sort(sort_key_idxes.begin(), sort_key_idxes.end());
        if (!std::includes(_referenced_column_ids.begin(), _referenced_column_ids.end(), sort_key_idxes.begin(),
                           sort_key_idxes.end())) {
            LOG(WARNING) << "table with sort key do not support partial update";
            return Status::NotSupported("table with sort key do not support partial update");
        }
        _tablet_schema = _partial_update_tablet_schema;
    }

    auto sort_key_idxes = _tablet_schema->sort_key_idxes();
    std::sort(sort_key_idxes.begin(), sort_key_idxes.end());
    bool auto_increment_in_sort_key = false;
    for (auto& idx : sort_key_idxes) {
        auto& col = _tablet_schema->column(idx);
        if (col.is_auto_increment()) {
            auto_increment_in_sort_key = true;
            break;
        }
    }

    if (auto_increment_in_sort_key && _miss_auto_increment_column) {
        LOG(WARNING) << "table with sort key do not support partial update";
        return Status::NotSupported("table with sort key do not support partial update");
    }
    return Status::OK();
}

Status DeltaWriterImpl::finish() {
    SCOPED_THREAD_LOCAL_MEM_SETTER(_mem_tracker, false);
    RETURN_IF_ERROR(build_schema_and_writer());

    // TODO: move file type checking to a common place
    auto is_seg_file = [](const std::string& name) -> bool { return HasSuffixString(name, ".dat"); };
    auto is_del_file = [](const std::string& name) -> bool { return HasSuffixString(name, ".del"); };

    RETURN_IF_ERROR(flush());
    RETURN_IF_ERROR(_tablet_writer->finish());
    ASSIGN_OR_RETURN(auto tablet, _tablet_manager->get_tablet(_tablet_id));
    auto txn_log = std::make_shared<TxnLog>();
    txn_log->set_tablet_id(_tablet_id);
    txn_log->set_txn_id(_txn_id);
    auto op_write = txn_log->mutable_op_write();
    for (auto& f : _tablet_writer->files()) {
        if (is_seg_file(f)) {
            op_write->mutable_rowset()->add_segments(std::move(f));
        } else if (is_del_file(f)) {
            op_write->add_dels(std::move(f));
        } else {
            return Status::InternalError(fmt::format("unknown file {}", f));
        }
    }
    op_write->mutable_rowset()->set_num_rows(_tablet_writer->num_rows());
    op_write->mutable_rowset()->set_data_size(_tablet_writer->data_size());
    op_write->mutable_rowset()->set_overlapped(op_write->rowset().segments_size() > 1);
    // not support handle partial update and condition update at the same time
    if (_partial_update_tablet_schema != nullptr && _merge_condition != "") {
        return Status::NotSupported("partial update and condition update at the same time");
    }
    // handle partial update
    RowsetTxnMetaPB* rowset_txn_meta = _tablet_writer->rowset_txn_meta();
    if (rowset_txn_meta != nullptr && _partial_update_tablet_schema != nullptr) {
        op_write->mutable_txn_meta()->CopyFrom(*rowset_txn_meta);
        for (auto i = 0; i < _partial_update_tablet_schema->columns().size(); ++i) {
            const auto& tablet_column = _partial_update_tablet_schema->column(i);
            op_write->mutable_txn_meta()->add_partial_update_column_ids(_referenced_column_ids[i]);
            op_write->mutable_txn_meta()->add_partial_update_column_unique_ids(tablet_column.unique_id());
        }
        // generate rewrite segment names to avoid gc in rewrite operation
        for (auto i = 0; i < op_write->rowset().segments_size(); i++) {
            op_write->add_rewrite_segments(random_segment_filename());
        }
    }
    // handle condition update
    if (_merge_condition != "") {
        op_write->mutable_txn_meta()->set_merge_condition(_merge_condition);
    }
    // handle auto increment
    if (_miss_auto_increment_column) {
        for (auto i = 0; i < _tablet_schema->num_columns(); ++i) {
            auto col = _tablet_schema->column(i);
            if (col.is_auto_increment()) {
                op_write->mutable_txn_meta()->set_auto_increment_partial_update_column_id(i);
                break;
            }
        }
    }
    RETURN_IF_ERROR(tablet.put_txn_log(std::move(txn_log)));
    return Status::OK();
}

Status DeltaWriterImpl::_fill_auto_increment_id(const Chunk& chunk) {
    ASSIGN_OR_RETURN(auto tablet, _tablet_manager->get_tablet(_tablet_id));
    
    // 1. get pk column from chunk
    vector<uint32_t> pk_columns;
    for (size_t i = 0; i < _tablet_schema->num_key_columns(); i++) {
        pk_columns.push_back((uint32_t)i);
    }
    Schema pkey_schema = ChunkHelper::convert_schema(*_tablet_schema, pk_columns);
    std::unique_ptr<Column> pk_column;
    if (!PrimaryKeyEncoder::create_column(pkey_schema, &pk_column).ok()) {
        CHECK(false) << "create column for primary key encoder failed";
    }
    auto col = pk_column->clone();

    PrimaryKeyEncoder::encode(pkey_schema, chunk, 0, chunk.num_rows(), col.get());
    std::vector<std::unique_ptr<Column>> upserts;
    upserts.resize(1);
    upserts[0] = std::move(col);

    std::vector<uint64_t> rss_rowid_map;
    rss_rowid_map.resize(upserts[0]->size());
    std::vector<std::vector<uint64_t>*> rss_rowids;
    rss_rowids.resize(1);
    rss_rowids[0] = &rss_rowid_map;

    // 2. probe index
    int64_t version = tablet.update_mgr()->get_version();
    auto res = tablet.get_metadata(version);
    auto metadata = std::make_shared<TabletMetadata>(*res.value());
    metadata->set_version(version + 1);
    std::unique_ptr<MetaFileBuilder> builder = std::make_unique<MetaFileBuilder>(metadata, tablet.update_mgr());

    tablet.update_mgr()->get_rowids_from_pkindex(&tablet, *metadata.get(), upserts, version, builder.get(), &rss_rowids);

    std::vector<uint8_t> filter;
    uint32_t gen_num = 0;
    for (uint32_t i = 0; i < rss_rowid_map.size(); i++) {
        uint64_t v = rss_rowid_map[i];
        uint32_t rssid = v >> 32;
        if (rssid == (uint32_t)-1) {
            filter.emplace_back(1);
            ++gen_num;
        } else {
            filter.emplace_back(0);
        }
    }

    // 3. fill the non-existing rows
    std::vector<int64_t> ids(gen_num);
    RETURN_IF_ERROR(StorageEngine::instance()->get_next_increment_id_interval(_table_id, gen_num, ids));

    for (int i = 0; i < _vectorized_schema.num_fields(); i++) {
        const TabletColumn& tablet_column = _tablet_schema->column(i);
        if (tablet_column.is_auto_increment()) {
            auto& column = chunk.get_column_by_index(i);
            RETURN_IF_ERROR((std::dynamic_pointer_cast<Int64Column>(column))->fill_range(ids, filter));
            break;
        }
    }

    return Status::OK();
}

void DeltaWriterImpl::close() {
    SCOPED_THREAD_LOCAL_MEM_SETTER(_mem_tracker, false);

    if (_flush_token != nullptr) {
        (void)_flush_token->wait();
    }

    // Destruct variables manually for counting memory usage into |_mem_tracker|
    if (_tablet_writer != nullptr) {
        _tablet_writer->close();
    }
    _tablet_writer.reset();
    _mem_table.reset();
    _mem_table_sink.reset();
    _flush_token.reset();
    _tablet_schema.reset();
    _partial_update_tablet_schema.reset();
}

//// DeltaWriter

DeltaWriter::~DeltaWriter() {
    delete _impl;
}

Status DeltaWriter::open() {
    return _impl->open();
}

Status DeltaWriter::write(const Chunk& chunk, const uint32_t* indexes, uint32_t indexes_size) {
    DCHECK_EQ(0, bthread_self()) << "Should not invoke DeltaWriter::write() in a bthread";
    return _impl->write(chunk, indexes, indexes_size);
}

Status DeltaWriter::finish() {
    DCHECK_EQ(0, bthread_self()) << "Should not invoke DeltaWriter::finish() in a bthread";
    return _impl->finish();
}

void DeltaWriter::close() {
    DCHECK_EQ(0, bthread_self()) << "Should not invoke DeltaWriter::close() in a bthread";
    _impl->close();
}

int64_t DeltaWriter::partition_id() const {
    return _impl->partition_id();
}

int64_t DeltaWriter::tablet_id() const {
    return _impl->tablet_id();
}

int64_t DeltaWriter::txn_id() const {
    return _impl->txn_id();
}

MemTracker* DeltaWriter::mem_tracker() {
    return _impl->mem_tracker();
}

TabletWriter* DeltaWriter::tablet_writer() {
    return _impl->tablet_writer();
}

Status DeltaWriter::flush() {
    DCHECK_EQ(0, bthread_self()) << "Should not invoke DeltaWriter::flush() in a bthread";
    return _impl->flush();
}

Status DeltaWriter::flush_async() {
    DCHECK_EQ(0, bthread_self()) << "Should not invoke DeltaWriter::flush_async() in a bthread";
    return _impl->flush_async();
}

std::unique_ptr<DeltaWriter> DeltaWriter::create(const LakeDeltaWriterOptions& option, TabletManager* tablet_manager,
                                                 MemTracker* mem_tracker) {
    return std::make_unique<DeltaWriter>(new DeltaWriterImpl(option, tablet_manager, mem_tracker));
}

} // namespace starrocks::lake
