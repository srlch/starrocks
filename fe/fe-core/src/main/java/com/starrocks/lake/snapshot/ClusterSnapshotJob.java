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

package com.starrocks.lake.snapshot;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.ClusterSnapshotLog;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TClusterSnapshotJobsItem;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ClusterSnapshotJob implements Writable {
    /*
     * INITIALIZING: INIT state for the snapshot.
     * SNAPSHOTING: Doing checkpoint/image generation by replaying log for image both for FE and StarMgr and
     *              then upload the image into remote storage
     * FINISHED: Finish backup snapshot                 
     */
    public enum ClusterSnapshotJobState { INITIALIZING, SNAPSHOTING, FINISHED, ERROR }

    @SerializedName(value = "jobId")
    private long jobId;
    @SerializedName(value = "snapshotNamePrefix")
    private String snapshotNamePrefix;
    @SerializedName(value = "snapshotName")
    private String snapshotName;
    @SerializedName(value = "storageVolumeName")
    private String storageVolumeName;
    @SerializedName(value = "createTime")
    private long createTime;
    @SerializedName(value = "successTime")
    private long successTime;
    @SerializedName(value = "feJournalId")
    private long feJournalId;
    @SerializedName(value = "starMgrJournalId")
    private long starMgrJournalId;
    @SerializedName(value = "state")
    private ClusterSnapshotJobState state;
    @SerializedName(value = "errMsg")
    private String errMsg;

    public ClusterSnapshotJob(long jobId, String snapshotNamePrefix, String snapshotName, String storageVolumeName) {
        this.jobId = jobId;
        this.snapshotNamePrefix = snapshotNamePrefix;
        this.snapshotName = snapshotName;
        this.storageVolumeName = storageVolumeName;
        this.createTime = System.currentTimeMillis();
        this.successTime = -1;
        this.feJournalId = 0;
        this.starMgrJournalId = 0;
        this.state = ClusterSnapshotJobState.INITIALIZING;
        this.errMsg = "";
    }

    public void setState(ClusterSnapshotJobState state, boolean isReplay) {
        this.state = state;
        if (state == ClusterSnapshotJobState.FINISHED) {
            this.successTime = System.currentTimeMillis();
        }

        if (!isReplay) {
            logJob();
        }

        if (state == ClusterSnapshotJobState.FINISHED) {
            createSnapshotIfJobIsFinished();
        }
    }

    public void setJournalIds(long feJournalId, long starMgrJournalId) {
        this.feJournalId = feJournalId;
        this.starMgrJournalId = starMgrJournalId;
    }

    public void setErrMsg(String errMsg) {
        this.errMsg = errMsg;
    }

    public String getSnapshotNamePrefix() {
        return snapshotNamePrefix;
    }

    public String getSnapshotName() {
        return snapshotName;
    }

    public String getStorageVolumeName() {
        return storageVolumeName;
    }

    public long getCreateTime() {
        return createTime;
    }

    public long getSuccessTime() {
        return successTime;
    }

    public long getFeJournalId() {
        return feJournalId;
    }

    public long getStarMgrJournalId() {
        return starMgrJournalId;
    }

    public long getJobId() {
        return jobId;
    }

    public ClusterSnapshotJobState getState() {
        return state;
    }

    public void logJob() {
        ClusterSnapshotLog log = new ClusterSnapshotLog();
        log.setSnapshotJob(this);
        GlobalStateMgr.getCurrentState().getEditLog().logClusterSnapshotLog(log);
    }

    private void createSnapshotIfJobIsFinished() {
        GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().createAutomatedSnaphot(this);
    }

    public TClusterSnapshotJobsItem getInfo() {
        TClusterSnapshotJobsItem item = new TClusterSnapshotJobsItem();
        item.setSnapshot_name(snapshotName);
        item.setJob_id(jobId);
        item.setCreated_time(createTime);
        item.setFinished_time(successTime);
        item.setState(state.name());
        item.setDetail_info("");
        item.setError_message(errMsg);
        return item;
    }

    public static ClusterSnapshotJob read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, ClusterSnapshotJob.class);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }
}
