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

#include "exec/schema_scanner/schema_temp_tables_scanner.h"

#include <protocol/TDebugProtocol.h>

#include "exec/schema_scanner/schema_helper.h"
#include "gen_cpp/FrontendService_types.h"
#include "runtime/runtime_state.h"

namespace starrocks {

const int32_t DEF_NULL_NUM = -1;

SchemaScanner::ColumnDesc SchemaTempTablesScanner::_s_tbls_columns[] = {
        //   name,       type,          size,     is_null
        {"TABLE_CATALOG", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"TABLE_SCHEMA", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"TABLE_NAME", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"TABLE_TYPE", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"ENGINE", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"VERSION", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), true},
        {"ROW_FORMAT", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"TABLE_ROWS", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), true},
        {"AVG_ROW_LENGTH", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), true},
        {"DATA_LENGTH", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), true},
        {"MAX_DATA_LENGTH", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), true},
        {"INDEX_LENGTH", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), true},
        {"DATA_FREE", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), true},
        {"AUTO_INCREMENT", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), true},
        {"CREATE_TIME", TypeDescriptor::from_logical_type(TYPE_DATETIME), sizeof(DateTimeValue), true},
        {"UPDATE_TIME", TypeDescriptor::from_logical_type(TYPE_DATETIME), sizeof(DateTimeValue), true},
        {"CHECK_TIME", TypeDescriptor::from_logical_type(TYPE_DATETIME), sizeof(DateTimeValue), true},
        {"TABLE_COLLATION", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"CHECKSUM", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), true},
        {"CREATE_OPTIONS", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"TABLE_COMMENT", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"SESSION", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"TABLE_ID", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), true}};

SchemaTempTablesScanner::SchemaTempTablesScanner()
        : SchemaScanner(_s_tbls_columns, sizeof(_s_tbls_columns) / sizeof(SchemaScanner::ColumnDesc)) {}

SchemaTempTablesScanner::~SchemaTempTablesScanner() = default;

Status SchemaTempTablesScanner::start(RuntimeState* state) {
    RETURN_IF_ERROR(SchemaScanner::start(state));
    TAuthInfo auth_info;
    if (nullptr != _param->catalog) {
        auth_info.__set_catalog_name(*(_param->catalog));
    }
    if (nullptr != _param->db) {
        auth_info.__set_pattern(*(_param->db));
    }
    if (nullptr != _param->current_user_ident) {
        auth_info.__set_current_user_ident(*(_param->current_user_ident));
    } else {
        if (nullptr != _param->user) {
            auth_info.__set_user(*(_param->user));
        }
        if (nullptr != _param->user_ip) {
            auth_info.__set_user_ip(*(_param->user_ip));
        }
    }

    TGetTemporaryTablesInfoRequest request;
    request.__set_auth_info(auth_info);
    if (_param->limit > 0) {
        request.__set_limit(_param->limit);
    }
    // init schema scanner state
    RETURN_IF_ERROR(SchemaScanner::init_schema_scanner_state(state));
    RETURN_IF_ERROR(SchemaHelper::get_temporary_tables_info(_ss_state, request, &_temp_tables_info_response));
    return Status::OK();
}

Status SchemaTempTablesScanner::get_next(ChunkPtr* chunk, bool* eos) {
    DCHECK(_is_init) << "call init() before get_next()";
    DCHECK(chunk != nullptr && eos != nullptr) << "input should not be nullptr";

    if (_temp_tables_info_index >= _temp_tables_info_response.tables_infos.size()) {
        *eos = true;
        return Status::OK();
    }
    *eos = false;
    return fill_chunk(chunk);
}

Status SchemaTempTablesScanner::fill_chunk(ChunkPtr* chunk) {
    const TTableInfo& table_info = _temp_tables_info_response.tables_infos[_temp_tables_info_index];
    const auto& slot_id_to_index_map = (*chunk)->get_slot_id_to_index_map();
    for (const auto& [slot_id, index] : slot_id_to_index_map) {
        switch (slot_id) {
        case 1: {
            // table_catalog
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(1);
                const std::string* str = &table_info.table_catalog;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 2: {
            // table_schema
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(2);
                const std::string* str = &table_info.table_schema;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 3: {
            // table_name
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(3);
                const std::string* str = &table_info.table_name;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 4: {
            // table_type
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(4);
                const std::string* str = &table_info.table_type;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 5: {
            // engine
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(5);
                const std::string* str = &table_info.engine;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 6: {
            // version
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(6);
                if (table_info.version == DEF_NULL_NUM) {
                    fill_data_column_with_null(column.get());
                } else {
                    fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&table_info.version);
                }
            }
            break;
        }
        case 7: {
            // row_format
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(7);
                const std::string* str = &table_info.row_format;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 8: {
            // table_rows
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(8);
                if (table_info.table_rows == DEF_NULL_NUM) {
                    fill_data_column_with_null(column.get());
                } else {
                    fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&table_info.table_rows);
                }
            }
            break;
        }
        case 9: {
            // avg_row_length
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(9);
                if (table_info.avg_row_length == DEF_NULL_NUM) {
                    fill_data_column_with_null(column.get());
                } else {
                    fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&table_info.avg_row_length);
                }
            }
            break;
        }
        case 10: {
            // data_length
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(10);
                if (table_info.data_length == DEF_NULL_NUM) {
                    fill_data_column_with_null(column.get());
                } else {
                    fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&table_info.data_length);
                }
            }
            break;
        }
        case 11: {
            // max_data_length
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(11);
                if (table_info.max_data_length == DEF_NULL_NUM) {
                    fill_data_column_with_null(column.get());
                } else {
                    fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&table_info.max_data_length);
                }
            }
            break;
        }
        case 12: {
            // index_length
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(12);
                if (table_info.index_length == DEF_NULL_NUM) {
                    fill_data_column_with_null(column.get());
                } else {
                    fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&table_info.index_length);
                }
            }
            break;
        }
        case 13: {
            // data_free
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(13);
                if (table_info.data_free == DEF_NULL_NUM) {
                    fill_data_column_with_null(column.get());
                } else {
                    fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&table_info.data_free);
                }
            }
            break;
        }
        case 14: {
            // auto_increment
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(14);
                if (table_info.auto_increment == DEF_NULL_NUM) {
                    fill_data_column_with_null(column.get());
                } else {
                    fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&table_info.auto_increment);
                }
            }
            break;
        }
        case 15: {
            // create_time
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(15);
                auto* nullable_column = down_cast<NullableColumn*>(column.get());
                if (table_info.__isset.create_time) {
                    int64_t create_time = table_info.create_time;
                    if (create_time <= 0) {
                        nullable_column->append_nulls(1);
                    } else {
                        DateTimeValue t;
                        t.from_unixtime(create_time, _runtime_state->timezone_obj());
                        fill_column_with_slot<TYPE_DATETIME>(column.get(), (void*)&t);
                    }
                } else {
                    nullable_column->append_nulls(1);
                }
            }
            break;
        }
        case 16: {
            // update_time
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(16);
                auto* nullable_column = down_cast<NullableColumn*>(column.get());
                if (table_info.__isset.update_time) {
                    int64_t create_time = table_info.update_time;
                    if (create_time <= 0) {
                        nullable_column->append_nulls(1);
                    } else {
                        DateTimeValue t;
                        t.from_unixtime(create_time, _runtime_state->timezone_obj());
                        fill_column_with_slot<TYPE_DATETIME>(column.get(), (void*)&t);
                    }
                } else {
                    nullable_column->append_nulls(1);
                }
            }
            break;
        }
        case 17: {
            // check_time
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(17);
                auto* nullable_column = down_cast<NullableColumn*>(column.get());
                if (table_info.__isset.check_time) {
                    int64_t check_time = table_info.check_time;
                    if (check_time <= 0) {
                        nullable_column->append_nulls(1);
                    } else {
                        DateTimeValue t;
                        t.from_unixtime(check_time, _runtime_state->timezone_obj());
                        fill_column_with_slot<TYPE_DATETIME>(column.get(), (void*)&t);
                    }
                } else {
                    nullable_column->append_nulls(1);
                }
            }
            break;
        }
        case 18: {
            // table_collation
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(18);
                const std::string* str = &table_info.table_collation;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 19: {
            // checksum
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(19);
                if (table_info.checksum == DEF_NULL_NUM) {
                    fill_data_column_with_null(column.get());
                } else {
                    fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&table_info.checksum);
                }
            }
            break;
        }
        case 20: {
            // create_options
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(20);
                const std::string* str = &table_info.create_options;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 21: {
            // table_comment
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(21);
                const std::string* str = &table_info.table_comment;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 22: {
            // fill session id
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(22);
                const std::string* str = &table_info.session_id;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 23: {
            // fill table id
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(23);
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&table_info.table_id);
            }
        }
        default:
            break;
        }
    }
    _temp_tables_info_index++;
    return Status::OK();
}

} // namespace starrocks