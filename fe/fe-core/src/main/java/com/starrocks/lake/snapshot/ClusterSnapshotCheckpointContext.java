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

import com.google.common.base.Preconditions;
import com.starrocks.backup.Status;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.journal.Journal;
import com.starrocks.lake.snapshot.ClusterSnapshotJob.ClusterSnapshotJobState;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// Helper class to coordinate CheckpointController between FE and starMgr
// for clould native snapshot backup
public class ClusterSnapshotCheckpointContext {
    public static final Logger LOG = LogManager.getLogger(ClusterSnapshotCheckpointContext.class);
    public static int INVALID_JOURANL_ID = -1;
    private static int CAPTURE_ID_RETRY_TIME = 10;

    private Journal feJournal;
    private Journal starMgrJournal;
    private long curFEJouranlId;
    private long curStarMgrJouranlId;
    private boolean previousFERoundFinished;
    private boolean previousStarMgrRoundFinished;
    private String curErrMsg;

    // runtime param to represent automated snapshot job
    private ClusterSnapshotJob job;

    // Synchronization flag
    Boolean responsibleForCancelRound;

    public ClusterSnapshotCheckpointContext() {
        this.feJournal = null;
        this.starMgrJournal = null;
        this.curFEJouranlId = INVALID_JOURANL_ID;
        this.curStarMgrJouranlId = INVALID_JOURANL_ID;
        this.previousFERoundFinished = true;
        this.previousStarMgrRoundFinished = true;
        this.curErrMsg = "";
        this.job = null;
        this.responsibleForCancelRound = null;
    }

    public void setJournal(Journal journal, boolean belongToGlobalStateMgr) {
        if (!RunMode.isSharedDataMode()) {
            return;
        }

        if (belongToGlobalStateMgr) {
            this.feJournal = journal;
        } else {
            this.starMgrJournal = journal;
        }
    }

    public synchronized long coordinateTwoCheckpointsIfNeeded(boolean belongToGlobalStateMgr, boolean checkpointIsReady) {
        long journalId = INVALID_JOURANL_ID;
        do {
            if (!RunMode.isSharedDataMode() ||
                    !GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().isAutomatedSnapshotOn()) {
                if (roundHasBegan()) {
                    setCurErrMsg("Cancelled by user");
                    resetRound(true);
                }
                break;
            }

            // step 1: if the checkpoint id has been allocated by the peer, use it
            journalId = acquireAllocatedIdByPeer(belongToGlobalStateMgr);
            if (journalId != INVALID_JOURANL_ID) {
                break;
            }

            // step 2: check should responsible for cancelling the round
            if (checkResponsibleForCancelRound(belongToGlobalStateMgr)) {
                break;
            }

            // step 3: check self status from the previous round
            if (!selfPreviousRoundIsSuccess(belongToGlobalStateMgr)) {
                if (responsibleForCancelRound == null) {
                    // if self-previous round has failed, current round should be
                    // reset by the peer
                    responsibleForCancelRound = !belongToGlobalStateMgr;
                }
                break;
            }

            // step 4: check the peer status from the previous round
            if (!peerPreviousRoundIsSuccess(belongToGlobalStateMgr)) {
                break;
            }

            // step 5: capture consistent id
            Preconditions.checkState(curFEJouranlId == INVALID_JOURANL_ID);
            Preconditions.checkState(curStarMgrJouranlId == INVALID_JOURANL_ID);
            Preconditions.checkState(previousFERoundFinished);
            Preconditions.checkState(previousStarMgrRoundFinished);

            if (job == null) {
                job = GlobalStateMgr.getCurrentState().getClusterSnapshotMgr()
                                                      .createNewAutomatedSnapshotJob(); /* INITIALIZING state */
            }

            Pair<Long, Long> consistentIds = captureConsistentCheckpointIdBetweenFEAndStarMgr();

            ClusterSnapshot snapshot = GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().getAutomatedSnapshot();
            boolean forceCheckpoint = (snapshot != null && snapshot.getSuccessTime() +
                                       Config.automated_cluster_snapshot_interval_seconds * 1000 > System.currentTimeMillis());

            if (consistentIds == null || (!checkpointIsReady && !forceCheckpoint)) {
                // continue normal checkpoint here if and only if
                // 1. can not allocate the consistent ids.
                // 2. checkpoint is not ready and it’s not long enough since the last snapshot
                break;
            }

            // step 6: new round begin
            previousFERoundFinished = false;
            previousStarMgrRoundFinished = false;
            job.setState(ClusterSnapshotJobState.SNAPSHOTING, false);
            job.setJournalIds(consistentIds.first, consistentIds.second);
            if (belongToGlobalStateMgr) {
                this.curStarMgrJouranlId = consistentIds.second;
                journalId = consistentIds.first;
            } else {
                this.curFEJouranlId = consistentIds.first;
                journalId = consistentIds.second;
            }
        } while (false);

        return journalId;
    }

    public void handleImageUpload(boolean createImageFailed, String createImageFailedErrMsg, String imageDir,
                                  boolean belongToGlobalStateMgr) {
        if (createImageFailed) {
            setCurErrMsg(createImageFailedErrMsg);
            return;
        }

        if (uploadImageForSnapshot(belongToGlobalStateMgr, imageDir)) {
            updateRoundIdAndMarkJobFinishedFromLaggard(belongToGlobalStateMgr);
        }
    }

    private long acquireAllocatedIdByPeer(boolean belongToGlobalStateMgr) {
        if (belongToGlobalStateMgr && curFEJouranlId != INVALID_JOURANL_ID) {
            long id = curFEJouranlId;
            curFEJouranlId = INVALID_JOURANL_ID;
            return id;
        } else if (!belongToGlobalStateMgr && curStarMgrJouranlId != INVALID_JOURANL_ID) {
            long id = curStarMgrJouranlId;
            curStarMgrJouranlId = INVALID_JOURANL_ID;
            return id;
        }

        return INVALID_JOURANL_ID;
    }

    private boolean checkResponsibleForCancelRound(boolean belongToGlobalStateMgr) {
        if (responsibleForCancelRound != null &&
                ((responsibleForCancelRound.booleanValue() && belongToGlobalStateMgr) ||
                 (!responsibleForCancelRound.booleanValue() && !belongToGlobalStateMgr))) {
            resetRound(true);
            return true;
        }

        return false;
    }

    private boolean selfPreviousRoundIsSuccess(boolean belongToGlobalStateMgr) {
        return !(belongToGlobalStateMgr && !previousFERoundFinished || !belongToGlobalStateMgr && !previousStarMgrRoundFinished);
    }

    private boolean peerPreviousRoundIsSuccess(boolean belongToGlobalStateMgr) {
        return !(belongToGlobalStateMgr && !previousStarMgrRoundFinished || !belongToGlobalStateMgr && !previousFERoundFinished);
    }

    private boolean isLaggard(boolean belongToGlobalStateMgr) {
        return previousFERoundFinished && !belongToGlobalStateMgr || previousStarMgrRoundFinished && belongToGlobalStateMgr;
    }

    private boolean roundHasBegan() {
        return !previousFERoundFinished || !previousStarMgrRoundFinished;
    }

    private boolean uploadImageForSnapshot(boolean belongToGlobalStateMgr, String imageDir) {
        Preconditions.checkState(job != null);
        LOG.info("Begin upload snapshot for image for {}", belongToGlobalStateMgr ? "FE image" : "StarMgr image");
        Status st = GlobalStateMgr.getCurrentState().getClusterSnapshotMgr()
                    .actualUploadImageForSnapshot(belongToGlobalStateMgr, job.getSnapshotName(), imageDir);
        if (!st.ok()) {
            LOG.warn("upload snapshot for image for {} has failed: {}", belongToGlobalStateMgr ? "FE image" : "StarMgr image",
                     st.getErrMsg());
            setCurErrMsg(st.getErrMsg());
            // The failure should be handled by coordinateTwoCheckpointsIfNeeded
            return false;
        }

        LOG.info("Finished upload snapshot for image for {}", belongToGlobalStateMgr ? "FE image" : "StarMgr image");
        return true;
    }

    // If the caller is the laggard in current round, this function will set the job
    // into finished state to create a successful snapshot. This function will also update the round id.
    private synchronized void updateRoundIdAndMarkJobFinishedFromLaggard(boolean belongToGlobalStateMgr) {
        if (isLaggard(belongToGlobalStateMgr)) {
            // all images have been finished
            // finish job and create cluster snapshot into Mgr
            long feJournalId = job.getFeJournalId();
            long starMgrJournalId = job.getStarMgrJournalId();
            job.setState(ClusterSnapshotJobState.FINISHED, false);
            resetRound(false);

            LOG.info("Finish upload all image file for snapshot, " + "FE jouranl id: " + String.valueOf(feJournalId) +
                     " starMgr jouranl id: " + String.valueOf(starMgrJournalId));
            return;
        }

        if (belongToGlobalStateMgr) {
            previousFERoundFinished = true;
        } else {
            previousStarMgrRoundFinished = true;
        }
    }

    private synchronized void setCurErrMsg(String curErrMsg) {
        this.curErrMsg = curErrMsg;
    }

    private void resetRound(boolean error) {
        if (error) {
            job.setState(ClusterSnapshotJobState.ERROR, false);
            job.setErrMsg(curErrMsg);
        } else {
            Preconditions.checkState(previousFERoundFinished || previousStarMgrRoundFinished);
            Preconditions.checkState(curFEJouranlId == INVALID_JOURANL_ID);
            Preconditions.checkState(curStarMgrJouranlId == INVALID_JOURANL_ID);
        }

        job = null;
        curErrMsg = "";
        responsibleForCancelRound = null;
        curFEJouranlId = INVALID_JOURANL_ID;
        curStarMgrJouranlId = INVALID_JOURANL_ID;
        previousFERoundFinished = true;
        previousStarMgrRoundFinished = true;
    }

    /*
     * Definition of consistent: Suppose there are two images generated by FE and StarMgr, call FEImageNew
     * and StarMgrImageNew and satisfy:
     * FEImageNew = FEImageOld + editlog(i) + ... + editlog(j)
     * StarMgrImageNew = StarMgrImageOld + editlog(k) + ... + editlog(m)
     * 
     * Define Tj = generated time of editlog(j), Tmax = max(Tj, Tm)
     * Consistency means all editlogs generated before Tmax (no matter the editlog is belong to FE or starMgr)
     * should be included in the image generated by checkpoint.
     * In other words, there must be no holes before the `maximum` editlog contained in the two images
     * generated by checkpoint.
     * 
     * How to get the consistent id: because editlog is generated and flush in a synchronous way, so we can simply
     * get the `snapshot` of maxJouranlId for both FE side and StarMgr side.
     * We get the `snapshot` in a lock-free way. As shown in the code below:
     * (1) if feCheckpointIdT1 == feCheckpointIdT3 means in [T1, T3], no editlog added for FE side
     * (2) if starMgrCheckpointIdT2 == starMgrCheckpointIdT4 means in [T2, T4], no editlog added for StarMgr side
     * 
     * Because T1 < T2 < T3 < T4, from (1),(2) -> [T2, T3] no editlog added for FE side and StarMgr side
     * So we get the snapshots are feCheckpointIdT3 and starMgrCheckpointIdT2
    */
    private Pair<Long, Long> captureConsistentCheckpointIdBetweenFEAndStarMgr() {
        if (feJournal == null || starMgrJournal == null) {
            return null;
        }

        int retryTime = CAPTURE_ID_RETRY_TIME;
        while (retryTime > 0) {
            long feCheckpointIdT1 = feJournal.getMaxJournalId();
            long starMgrCheckpointIdT2 = starMgrJournal.getMaxJournalId();
            long feCheckpointIdT3 = feJournal.getMaxJournalId();
            long starMgrCheckpointIdT4 = starMgrJournal.getMaxJournalId();
    
            if (feCheckpointIdT1 == feCheckpointIdT3 && starMgrCheckpointIdT2 == starMgrCheckpointIdT4) {
                return Pair.create(feCheckpointIdT3, starMgrCheckpointIdT2);
            }
    
            try {
                Thread.sleep(100);
            } catch (Exception ignore) {
            }
            --retryTime;
        }
        return null;
    }
}