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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/persist/AutoIncrementInfo.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.persist;

import com.starrocks.common.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AutoIncrementInfo implements Writable {
    private final ConcurrentHashMap<Long, Long> tableIdToIncrementId = new ConcurrentHashMap<>();

    public AutoIncrementInfo(ConcurrentHashMap<Long, Long> tableIdToIncrementId) {
        if (tableIdToIncrementId != null) {
            // for persist
            for (Map.Entry<Long, Long> entry : tableIdToIncrementId.entrySet()) {
                Long tableId = entry.getKey();
                Long id = entry.getValue();

                this.tableIdToIncrementId.put(tableId, id);
            }
        }
    }

    public ConcurrentHashMap<Long, Long> tableIdToIncrementId() {
        return this.tableIdToIncrementId;
    }

    public AutoIncrementInfo read(DataInput in) throws IOException {
        this.readFields(in);
        return this;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        long autoInrementTableCount = this.tableIdToIncrementId.size();
        out.writeLong(autoInrementTableCount);

        for (Map.Entry<Long, Long> entry : this.tableIdToIncrementId.entrySet()) {
            Long tableId = entry.getKey();
            Long id = entry.getValue();

            out.writeLong(tableId);
            out.writeLong(id);
        }
    }

    public void readFields(DataInput in) throws IOException {
        long autoInrementTableCount = in.readLong();

        for (long i = 0; i < autoInrementTableCount; ++i) {
            Long tableId = in.readLong();
            Long id = in.readLong();
            this.tableIdToIncrementId.put(tableId, id);
        }
    }
}
