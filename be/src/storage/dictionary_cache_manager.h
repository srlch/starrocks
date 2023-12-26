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

#include <map>
#include <memory>
#include <unordered_map>

#include "column/chunk.h"
#include "column/column.h"
#include "column/datum.h"
#include "common/status.h"
#include "exec/dictionary_cache_writer.h"
#include "fmt/format.h"
#include "service/internal_service.h"
#include "storage/chunk_helper.h"
#include "storage/primary_key_encoder.h"
#include "storage/primary_key_encoder.h"
#include "util/phmap/phmap.h"
#include "util/xxh3.h"

namespace starrocks {

constexpr uint8_t PREFETCHABLE_MAX_SIZE = 64;
constexpr uint8_t PREFETCHN = 8;

enum DictionaryCacheEncoderType {
    PK_ENCODE = 0,
};

template <LogicalType logical_type, bool Prefetchable>
struct DictionaryCacheTypeTraits {
    using CppType = typename CppTypeTraits<logical_type>::CppType;
};

template <>
struct DictionaryCacheTypeTraits<TYPE_VARCHAR, true> {
    using CppType = std::array<uint8_t, PREFETCHABLE_MAX_SIZE>;
};

template <>
struct DictionaryCacheTypeTraits<TYPE_VARCHAR, false> {
    using CppType = std::string;
};

template <bool Prefetchable>
struct DictionaryCacheTypeTraits<TYPE_DATE> {
    using CppType = int32_t;
};

template <bool Prefetchable>
struct DictionaryCacheTypeTraits<TYPE_DATETIME> {
    using CppType = int64_t;
};

template <LogicalType logical_type, bool Prefetchable>
struct DictionaryCacheHashTraits {
    using CppType = typename DictionaryCacheTypeTraits<logical_type>::CppType;
    inline size_t operator()(const CppType& v) const { return StdHashWithSeed<CppType, PhmapSeed1>()(v); }
};

template <>
struct DictionaryCacheHashTraits<TYPE_VARCHAR, true> {
    inline size_t operator()(const std::array<uint8_t, 64>& v) const { return XXH3_64bits(v.data(), v.size()); }
};

template <>
struct DictionaryCacheHashTraits<TYPE_VARCHAR, false> {
    inline size_t operator()(const std::string& v) const { return XXH3_64bits(v.data(), v.length()); }
};

class DictionaryCache {
public:
    DictionaryCache(DictionaryCacheEncoderType type) : _type(type) {}
    virtual ~DictionaryCache() = default;

    virtual inline Status insert(const Datum& k, const Datum& v) = 0;

    virtual inline Status lookup(const Datum& k, Column* src, Column* dest) = 0;

    virtual inline size_t memory_usage() = 0;

protected:
    DictionaryCacheEncoderType _type;
};

template <LogicalType KeyLogicalType, LogicalType ValueLogicalType, bool Prefetchable>
class DictionaryCacheImpl final : public DictionaryCache {
public:
    DictionaryCacheImpl(DictionaryCacheEncoderType type) : DictionaryCache(type), _total_memory_useage(0) {}
    virtual ~DictionaryCacheImpl() override = default;

    using KeyCppType = typename DictionaryCacheTypeTraits<KeyLogicalType, Prefetchable>::CppType;
    using ValueCppType = typename DictionaryCacheTypeTraits<ValueLogicalType, Prefetchable>::CppType;

    virtual inline Status insert(const Datum& k, const Datum& v) override {
        KeyCppType key;
        ValueCppType value;
        _get<KeyCppType>(k, key);
        _get<ValueCppType>(v, value);
        auto r = _dictionary.insert({key, value});
        if (!r.second) {
            return Status::InternalError("duplicate key found when refreshing dictionary");
        }
        _total_memory_useage.fetch_add(_get_element_memory_usage<KeyCppType, ValueCppType>(key, value));
        return Status::OK();
    }

    virtual inline Status lookup(const Datum& k, Column* src, Column* dest) override {
        bool not_found = false;
        size_t size = src->size();
        if constexpr (std::is_same_v<KeyCppType, std::array>) {
            const auto* raw_data = reinterpret_cast<const Slice*>(pks.raw_data());
            uint32_t n = idx_end - idx_begin;
            if (n >= PREFETCHN * 2) {
                FixSlice<S> prefetch_keys[PREFETCHN];
                size_t prefetch_hashes[PREFETCHN];
                for (uint32_t i = 0; i < PREFETCHN; i++) {
                    prefetch_keys[i].assign(keys[idx_begin + i]);
                    prefetch_hashes[i] = FixSliceHash<S>()(prefetch_keys[i]);
                    _map.prefetch_hash(prefetch_hashes[i]);
                }
                for (auto i = idx_begin; i < idx_end; i++) {
                    uint32_t pslot = (i - idx_begin) % PREFETCHN;
                    auto iter = _map.find(prefetch_keys[pslot], prefetch_hashes[pslot]);
                    if (iter != _map.end()) {
                        (*rowids)[i] = iter->second.value;
                    } else {
                        (*rowids)[i] = -1;
                    }
                    uint32_t prefetch_i = i + PREFETCHN;
                    if (LIKELY(prefetch_i < idx_end)) {
                        prefetch_keys[pslot].assign(keys[prefetch_i]);
                        prefetch_hashes[pslot] = FixSliceHash<S>()(prefetch_keys[pslot]);
                        _map.prefetch_hash(prefetch_hashes[pslot]);
                    }
                }
            } else {
                for (auto i = idx_begin; i < idx_end; i++) {
                    auto iter = _map.find(FixSlice<S>(keys[i]));
                    if (iter != _map.end()) {
                        (*rowids)[i] = iter->second.value;
                    } else {
                        (*rowids)[i] = -1;
                    }
                }
            }
        } else if constexpr (std::is_same_v<KeyCppType, std::string>) {
            KeyCppType key;
            key = std::move(k.get<Slice>().to_string());
            auto iter = _dictionary.find(key);
            if (iter != _dictionary.end()) {
                not_found = true;
                break;
            }
            dest->append_datum(*_get_datum(iter->second));
        } else {
            auto* raw_data = reinterpret_cast<const KeyCppType*>(src.raw_data());
            for (auto i = 0; i < size; i++) {
                uint32_t prefetch_i = i + PREFETCHN;
                if (LIKELY(prefetch_i < size)) _dictionary.prefetch(raw_data[prefetch_i]);
                auto iter = _dictionary.find(raw_data[i]);
                if (iter != _dictionary.end()) {
                    not_found = true;
                    break;
                }
                dest->append_datum(*_get_datum(iter->second));
            }
        }

        if (not_found) {
            return Status::NotFound("key not found in dictionary cache");
        }
        return Status::OK();
    }

    virtual inline size_t memory_usage() override { return _total_memory_useage.load(); }

private:
    template <class CppType>
    inline void _get(const Datum& d, CppType& v) {
        switch (_type) {
        case DictionaryCacheEncoderType::PK_ENCODE: {
            if constexpr (std::is_same_v<CppType, std::string>) {
                v = std::move(d.get<Slice>().to_string());
            } else {
                v = d.get<CppType>();
            }
            break;
        }
        default:
            break;
        }
        return;
    }

    inline std::shared_ptr<Datum> _get_datum(const ValueCppType& v) {
        switch (_type) {
        case DictionaryCacheEncoderType::PK_ENCODE: {
            if constexpr (std::is_same_v<ValueCppType, std::string>) {
                return std::make_shared<Datum>(Slice(v));
            } else {
                return std::make_shared<Datum>(v);
            }
        }
        default:
            break;
        }
        return nullptr;
    }

    template <class KeyCppType, class ValueCppType>
    inline size_t _get_element_memory_usage(const KeyCppType& k, const ValueCppType& v) {
        switch (_type) {
        case DictionaryCacheEncoderType::PK_ENCODE: {
            return 1;
        }
        default:
            break;
        }
        return 0;
    }

    phmap::parallel_flat_hash_map<KeyCppType, ValueCppType, DictionaryCacheHashTraits<KeyLogicalType>,
                                  phmap::priv::hash_default_eq<KeyCppType>,
                                  phmap::priv::Allocator<phmap::priv::Pair<const KeyCppType, ValueCppType>>, 4,
                                  phmap::NullMutex, true>
            _dictionary;
    std::atomic<size_t> _total_memory_useage;
};

using DictionaryCachePtr = std::shared_ptr<DictionaryCache>;

class DictionaryCacheUtil {
public:
    DictionaryCacheUtil() = default;
    ~DictionaryCacheUtil();

    // using primary key encoding function
    static std::unique_ptr<Column> encode_columns(const Schema& schema, const Chunk* chunk,
                                                  const DictionaryCacheEncoderType& encoder_type = PK_ENCODE) {
        switch (encoder_type) {
        case PK_ENCODE: {
            std::unique_ptr<Column> encoded_columns;
            if (!PrimaryKeyEncoder::create_column(schema, &encoded_columns).ok()) {
                std::stringstream ss;
                ss << "create column for primary key encoder failed";
                LOG(WARNING) << ss.str();
                return nullptr;
            }
            PrimaryKeyEncoder::encode(schema, *chunk, 0, chunk->num_rows(), encoded_columns.get());
            return encoded_columns;
        }
        default:
            break;
        }
        return nullptr;
    }

    static Status decode_columns(const Schema& schema, Column* column, Chunk* decoded_chunk,
                                 const DictionaryCacheEncoderType& encoder_type = PK_ENCODE) {
        switch (encoder_type) {
        case PK_ENCODE: {
            return PrimaryKeyEncoder::decode(schema, *column, 0, column->size(), decoded_chunk);
        }
        default:
            break;
        }
        return Status::InternalError("decode failed for dictionary");
    }

    static LogicalType get_encoded_type(const Schema& schema,
                                        const DictionaryCacheEncoderType& encoder_type = PK_ENCODE) {
        switch (encoder_type) {
        case PK_ENCODE: {
            std::vector<uint32_t> idxes;
            for (size_t i = 0; i < schema.fields().size(); i++) {
                idxes.push_back((uint32_t)i);
            }
            return PrimaryKeyEncoder::encoded_primary_key_type(schema, idxes);
        }
        default:
            break;
        }
        return TYPE_NONE;
    }

    template <bool Prefetchable>
    static DictionaryCachePtr create_dictionary_cache(
            const std::pair<LogicalType, LogicalType>& type,
            const DictionaryCacheEncoderType& encoder_type = PK_ENCODE) {
        switch (encoder_type) {
        case PK_ENCODE: {
#define CASE_TYPE(key_type, value_type) \
    case (std::make_pair(key_type, value_type)):                        \
        return std::make_shared<DictionaryCacheImpl<key_type, value_type, Prefetchable>>(encoder_type)

            switch (type) {
                CASE_TYPE(TYPE_BOOLEAN, TYPE_BOOLEAN);
                CASE_TYPE(TYPE_BOOLEAN, TYPE_TINYINT);
                CASE_TYPE(TYPE_BOOLEAN, TYPE_SMALLINT);
                CASE_TYPE(TYPE_BOOLEAN, TYPE_INT);
                CASE_TYPE(TYPE_BOOLEAN, TYPE_BIGINT);
                CASE_TYPE(TYPE_BOOLEAN, TYPE_LARGEINT);
                CASE_TYPE(TYPE_BOOLEAN, TYPE_VARCHAR);
                CASE_TYPE(TYPE_BOOLEAN, TYPE_DATE);
                CASE_TYPE(TYPE_BOOLEAN, TYPE_DATETIME);

                CASE_TYPE(TYPE_TINYINT, TYPE_BOOLEAN);
                CASE_TYPE(TYPE_TINYINT, TYPE_TINYINT);
                CASE_TYPE(TYPE_TINYINT, TYPE_SMALLINT);
                CASE_TYPE(TYPE_TINYINT, TYPE_INT);
                CASE_TYPE(TYPE_TINYINT, TYPE_BIGINT);
                CASE_TYPE(TYPE_TINYINT, TYPE_LARGEINT);
                CASE_TYPE(TYPE_TINYINT, TYPE_VARCHAR);
                CASE_TYPE(TYPE_TINYINT, TYPE_DATE);
                CASE_TYPE(TYPE_TINYINT, TYPE_DATETIME);

                CASE_TYPE(TYPE_SMALLINT, TYPE_BOOLEAN);
                CASE_TYPE(TYPE_SMALLINT, TYPE_TINYINT);
                CASE_TYPE(TYPE_SMALLINT, TYPE_SMALLINT);
                CASE_TYPE(TYPE_SMALLINT, TYPE_INT);
                CASE_TYPE(TYPE_SMALLINT, TYPE_BIGINT);
                CASE_TYPE(TYPE_SMALLINT, TYPE_LARGEINT);
                CASE_TYPE(TYPE_SMALLINT, TYPE_VARCHAR);
                CASE_TYPE(TYPE_SMALLINT, TYPE_DATE);
                CASE_TYPE(TYPE_SMALLINT, TYPE_DATETIME);

                CASE_TYPE(TYPE_INT, TYPE_BOOLEAN);
                CASE_TYPE(TYPE_INT, TYPE_TINYINT);
                CASE_TYPE(TYPE_INT, TYPE_SMALLINT);
                CASE_TYPE(TYPE_INT, TYPE_INT);
                CASE_TYPE(TYPE_INT, TYPE_BIGINT);
                CASE_TYPE(TYPE_INT, TYPE_LARGEINT);
                CASE_TYPE(TYPE_INT, TYPE_VARCHAR);
                CASE_TYPE(TYPE_INT, TYPE_DATE);
                CASE_TYPE(TYPE_INT, TYPE_DATETIME);

                CASE_TYPE(TYPE_BIGINT, TYPE_BOOLEAN);
                CASE_TYPE(TYPE_BIGINT, TYPE_TINYINT);
                CASE_TYPE(TYPE_BIGINT, TYPE_SMALLINT);
                CASE_TYPE(TYPE_BIGINT, TYPE_INT);
                CASE_TYPE(TYPE_BIGINT, TYPE_BIGINT);
                CASE_TYPE(TYPE_BIGINT, TYPE_LARGEINT);
                CASE_TYPE(TYPE_BIGINT, TYPE_VARCHAR);
                CASE_TYPE(TYPE_BIGINT, TYPE_DATE);
                CASE_TYPE(TYPE_BIGINT, TYPE_DATETIME);

                CASE_TYPE(TYPE_LARGEINT, TYPE_BOOLEAN);
                CASE_TYPE(TYPE_LARGEINT, TYPE_TINYINT);
                CASE_TYPE(TYPE_LARGEINT, TYPE_SMALLINT);
                CASE_TYPE(TYPE_LARGEINT, TYPE_INT);
                CASE_TYPE(TYPE_LARGEINT, TYPE_BIGINT);
                CASE_TYPE(TYPE_LARGEINT, TYPE_LARGEINT);
                CASE_TYPE(TYPE_LARGEINT, TYPE_VARCHAR);
                CASE_TYPE(TYPE_LARGEINT, TYPE_DATE);
                CASE_TYPE(TYPE_LARGEINT, TYPE_DATETIME);

                CASE_TYPE(TYPE_VARCHAR, TYPE_BOOLEAN);
                CASE_TYPE(TYPE_VARCHAR, TYPE_TINYINT);
                CASE_TYPE(TYPE_VARCHAR, TYPE_SMALLINT);
                CASE_TYPE(TYPE_VARCHAR, TYPE_INT);
                CASE_TYPE(TYPE_VARCHAR, TYPE_BIGINT);
                CASE_TYPE(TYPE_VARCHAR, TYPE_LARGEINT);
                CASE_TYPE(TYPE_VARCHAR, TYPE_VARCHAR);
                CASE_TYPE(TYPE_VARCHAR, TYPE_DATE);
                CASE_TYPE(TYPE_VARCHAR, TYPE_DATETIME);

                CASE_TYPE(TYPE_DATE, TYPE_BOOLEAN);
                CASE_TYPE(TYPE_DATE, TYPE_TINYINT);
                CASE_TYPE(TYPE_DATE, TYPE_SMALLINT);
                CASE_TYPE(TYPE_DATE, TYPE_INT);
                CASE_TYPE(TYPE_DATE, TYPE_BIGINT);
                CASE_TYPE(TYPE_DATE, TYPE_LARGEINT);
                CASE_TYPE(TYPE_DATE, TYPE_VARCHAR);
                CASE_TYPE(TYPE_DATE, TYPE_DATE);
                CASE_TYPE(TYPE_DATE, TYPE_DATETIME);

                CASE_TYPE(TYPE_DATETIME, TYPE_BOOLEAN);
                CASE_TYPE(TYPE_DATETIME, TYPE_TINYINT);
                CASE_TYPE(TYPE_DATETIME, TYPE_SMALLINT);
                CASE_TYPE(TYPE_DATETIME, TYPE_INT);
                CASE_TYPE(TYPE_DATETIME, TYPE_BIGINT);
                CASE_TYPE(TYPE_DATETIME, TYPE_LARGEINT);
                CASE_TYPE(TYPE_DATETIME, TYPE_VARCHAR);
                CASE_TYPE(TYPE_DATETIME, TYPE_DATE);
                CASE_TYPE(TYPE_DATETIME, TYPE_DATETIME);

#undef CASE_TYPE
            }
            break;
        }
        default:
            break;
        }
        return nullptr;
    }

    static size_t get_encoded_fixed_size(const Schema& schema) {
        return PrimaryKeyEncoder::get_encoded_fixed_size(schema);
    }
};

/*
    Manager all dictionary cache in current BE node,
    the read/write for the cache satisfy the following properties:
    1. WRITE is atomic for a REFRESH request, no partial write at all
    2. Read is consistent between all BEs(with the cache) in a single transaction
*/
 class DictionaryCacheManager {
public:
    using DictionaryId = int64_t;
    // DictionaryCacheTxnId is the monotonically increasing id allocated by FE.
    // It is used to identify every single dictionary cache refresh task.
    // DictionaryCacheTxnId is globally unique cross all dictionaries.
    using DictionaryCacheTxnId = int64_t;
    // DictionaryCacheTxnId -> DictionaryCache, ordered by DictionaryCacheTxnId for the same dictionary
    using OrderedMutableDictionaryCache = std::map<DictionaryCacheTxnId, DictionaryCachePtr>;

    DictionaryCacheManager() = default;
    ~DictionaryCacheManager() = default;

    DictionaryCacheManager(const DictionaryCacheManager&) = delete;
    const DictionaryCacheManager& operator=(const DictionaryCacheManager&) = delete;

    Status begin(const PProcessDictionaryCacheRequest* request);

    Status refresh(const PProcessDictionaryCacheRequest* request);

    Status commit(const PProcessDictionaryCacheRequest* request);

    void clear(DictionaryId dict_id, bool is_cancel = false /* trigger cancel */);

    void get_info(DictionaryId dict_id, PProcessDictionaryCacheResult& response);

    SchemaPtr get_dictionary_schema_by_id(const DictionaryId& dict_id);
    StatusOr<DictionaryCachePtr> get_dictionary_by_version(const DictionaryId& dict_id,
                                                           const DictionaryCacheTxnId& txn_id);

    inline static Status probe_given_dictionary_cache(const Schema& key_schema, const Schema& value_schema,
                                                      DictionaryCachePtr dictionary, const ChunkPtr& key_chunk,
                                                      ChunkPtr& value_chunk) {
        DCHECK(value_chunk->num_rows() == 0);
        size_t size = key_chunk->num_rows();

        auto encoded_key_column = DictionaryCacheUtil::encode_columns(key_schema, key_chunk.get());

        ChunkUniquePtr clone_value_chunk = value_chunk->clone_empty();
        auto encoded_value_column = DictionaryCacheUtil::encode_columns(value_schema, clone_value_chunk.get());

        if (encoded_key_column == nullptr || encoded_value_column == nullptr) {
            return Status::InternalError("encode dictionary cache column failed when probing the dictionary cache");
        }

        RETURN_IF_ERROR(dictionary->lookup(key, encoded_key_column.get(), encoded_value_column.get()));
        DCHECK(encoded_value_column->size() == size);

        return DictionaryCacheUtil::decode_columns(value_schema, encoded_value_column.get(), value_chunk.get());
    }

private:
    Status _refresh_encoded_chunk(DictionaryId dict_id, DictionaryCacheTxnId txn_id, const Column* encoded_key_column,
                                  const Column* encoded_value_column, const SchemaPtr& schema,
                                  LogicalType key_encoded_type, LogicalType value_encoded_type, long memory_limit,
                                  size_t fix_size);

    // dictionary id -> DictionaryCache
    std::unordered_map<DictionaryId, DictionaryCachePtr> _dict_cache;
    // dictionary id -> cache version number
    std::unordered_map<DictionaryId, long> _dict_cache_versions;
    // protect _dict_cache && _dict_cache_versions
    std::shared_mutex _lock;

    // dictionary id -> dictionary cache schema (pre-encoded full schema), used for expression evaluation
    std::unordered_map<DictionaryId, SchemaPtr> _dict_cache_schema;
    // protext _dict_cache_schema
    std::shared_mutex _schema_lock;

    // dictionary id -> OrderedMutableDcitionaryCache
    // _mutable_dict_caches It is used to save the intermediate
    // results of the refresh and perform atomic replacement when completed.
    // Why we need OrderedMutableDictionaryCache for every dictionary:
    // For a certain dictionary, there may be multiple caches that cache intermediate results
    // which are ordered by DictionaryCacheTxnId. Because the dictionary cache sink will be failed or
    // cancelled so that the intermediate results cache will be left. If we must alloc a new memory
    // intermediate cache for another retry task with the larger txn id. If the task success, we should
    // swap the immutable cache using the mutable cache with the max txn id. So we need the ordered
    // data structure to maintance the mutable cache.
    std::unordered_map<DictionaryId, std::shared_ptr<OrderedMutableDictionaryCache>> _mutable_dict_caches;
    // indicate dictionary is cancelled or not, this should be erase before return error status
    std::set<DictionaryId> _dict_cancel;
    // protect _mutable_dict_caches && _dict_cancel
    std::shared_mutex _refresh_lock;
};

} // namespace starrocks