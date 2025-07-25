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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/transaction/GlobalTransactionMgrTest.java

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

package com.starrocks.transaction;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.stream.JsonReader;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.FakeEditLog;
import com.starrocks.catalog.FakeGlobalStateMgr;
import com.starrocks.catalog.GlobalStateMgrTestUtil;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Replica;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DuplicatedRequestException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.LabelAlreadyUsedException;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.common.util.concurrent.lock.LockTimeoutException;
import com.starrocks.load.loadv2.ManualLoadTxnCommitAttachment;
import com.starrocks.load.routineload.KafkaProgress;
import com.starrocks.load.routineload.KafkaRoutineLoadJob;
import com.starrocks.load.routineload.KafkaTaskInfo;
import com.starrocks.load.routineload.RLTaskTxnCommitAttachment;
import com.starrocks.load.routineload.RoutineLoadJob;
import com.starrocks.load.routineload.RoutineLoadMgr;
import com.starrocks.load.routineload.RoutineLoadTaskInfo;
import com.starrocks.metric.MetricRepo;
import com.starrocks.metric.TableMetricsEntity;
import com.starrocks.metric.TableMetricsRegistry;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockReaderV2;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TKafkaRLTaskProgress;
import com.starrocks.thrift.TLoadSourceType;
import com.starrocks.thrift.TManualLoadTxnCommitAttachment;
import com.starrocks.thrift.TRLTaskTxnCommitAttachment;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.transaction.TransactionState.LoadJobSourceType;
import com.starrocks.transaction.TransactionState.TxnCoordinator;
import com.starrocks.transaction.TransactionState.TxnSourceType;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class GlobalTransactionMgrTest {

    private static FakeEditLog fakeEditLog;
    private static FakeGlobalStateMgr fakeGlobalStateMgr;
    private static FakeTransactionIDGenerator fakeTransactionIDGenerator;
    private static GlobalTransactionMgr masterTransMgr;
    private static GlobalTransactionMgr slaveTransMgr;
    private static GlobalStateMgr masterGlobalStateMgr;
    private static GlobalStateMgr slaveGlobalStateMgr;

    private TransactionState.TxnCoordinator transactionSource =
            new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE, "localfe");

    private TransactionState.TxnCoordinator transactionSourceBe =
            new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.BE, "localbe");

    @BeforeEach
    public void setUp() throws InstantiationException, IllegalAccessException, IllegalArgumentException,
            InvocationTargetException, NoSuchMethodException, SecurityException {
        fakeEditLog = new FakeEditLog();
        fakeGlobalStateMgr = new FakeGlobalStateMgr();
        fakeTransactionIDGenerator = new FakeTransactionIDGenerator();
        masterGlobalStateMgr = GlobalStateMgrTestUtil.createTestState();
        slaveGlobalStateMgr = GlobalStateMgrTestUtil.createTestState();
        masterTransMgr = masterGlobalStateMgr.getGlobalTransactionMgr();
        slaveTransMgr = slaveGlobalStateMgr.getGlobalTransactionMgr();
        MetricRepo.init();

        UtFrameUtils.setUpForPersistTest();
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void testBeginTransaction() throws LabelAlreadyUsedException, AnalysisException,
            RunningTxnExceedException, DuplicatedRequestException {
        FakeGlobalStateMgr.setGlobalStateMgr(masterGlobalStateMgr);
        long transactionId = masterTransMgr
                .beginTransaction(GlobalStateMgrTestUtil.testDbId1, Lists.newArrayList(GlobalStateMgrTestUtil.testTableId1),
                        GlobalStateMgrTestUtil.testTxnLable1,
                        transactionSource,
                        LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        TransactionState transactionState =
                masterTransMgr.getTransactionState(GlobalStateMgrTestUtil.testDbId1, transactionId);
        assertNotNull(transactionState);
        assertEquals(transactionId, transactionState.getTransactionId());
        assertEquals(TransactionStatus.PREPARE, transactionState.getTransactionStatus());
        assertEquals(GlobalStateMgrTestUtil.testDbId1, transactionState.getDbId());
        assertEquals(transactionSource.toString(), transactionState.getCoordinator().toString());
    }

    @Test
    public void testBeginTransactionWithSameLabel() throws LabelAlreadyUsedException, AnalysisException,
            RunningTxnExceedException, DuplicatedRequestException {
        FakeGlobalStateMgr.setGlobalStateMgr(masterGlobalStateMgr);
        long transactionId = 0;
        try {
            transactionId = masterTransMgr
                    .beginTransaction(GlobalStateMgrTestUtil.testDbId1,
                            Lists.newArrayList(GlobalStateMgrTestUtil.testTableId1),
                            GlobalStateMgrTestUtil.testTxnLable1,
                            transactionSource,
                            LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        } catch (AnalysisException | LabelAlreadyUsedException e) {
            e.printStackTrace();
        }
        TransactionState transactionState =
                masterTransMgr.getTransactionState(GlobalStateMgrTestUtil.testDbId1, transactionId);
        assertNotNull(transactionState);
        assertEquals(transactionId, transactionState.getTransactionId());
        assertEquals(TransactionStatus.PREPARE, transactionState.getTransactionStatus());
        assertEquals(GlobalStateMgrTestUtil.testDbId1, transactionState.getDbId());
        assertEquals(transactionSource.toString(), transactionState.getCoordinator().toString());

        try {
            transactionId = masterTransMgr
                    .beginTransaction(GlobalStateMgrTestUtil.testDbId1,
                            Lists.newArrayList(GlobalStateMgrTestUtil.testTableId1),
                            GlobalStateMgrTestUtil.testTxnLable1,
                            transactionSource,
                            LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        } catch (Exception e) {
            // TODO: handle exception
        }
    }

    // all replica committed success
    @Test
    public void testCommitTransaction1() throws StarRocksException {
        FakeGlobalStateMgr.setGlobalStateMgr(masterGlobalStateMgr);
        long transactionId = masterTransMgr
                .beginTransaction(GlobalStateMgrTestUtil.testDbId1, Lists.newArrayList(GlobalStateMgrTestUtil.testTableId1),
                        GlobalStateMgrTestUtil.testTxnLable1,
                        transactionSource,
                        LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        // commit a transaction
        TabletCommitInfo tabletCommitInfo1 = new TabletCommitInfo(GlobalStateMgrTestUtil.testTabletId1,
                GlobalStateMgrTestUtil.testBackendId1);
        TabletCommitInfo tabletCommitInfo2 = new TabletCommitInfo(GlobalStateMgrTestUtil.testTabletId1,
                GlobalStateMgrTestUtil.testBackendId2);
        TabletCommitInfo tabletCommitInfo3 = new TabletCommitInfo(GlobalStateMgrTestUtil.testTabletId1,
                GlobalStateMgrTestUtil.testBackendId3);
        List<TabletCommitInfo> transTablets = Lists.newArrayList();
        transTablets.add(tabletCommitInfo1);
        transTablets.add(tabletCommitInfo2);
        transTablets.add(tabletCommitInfo3);
        masterTransMgr.commitTransaction(GlobalStateMgrTestUtil.testDbId1, transactionId, transTablets,
                Lists.newArrayList(), null);
        TransactionState transactionState = fakeEditLog.getTransaction(transactionId);
        // check status is committed
        assertEquals(TransactionStatus.COMMITTED, transactionState.getTransactionStatus());
        // check replica version
        Partition testPartition = masterGlobalStateMgr.getLocalMetastore()
                .getTable(GlobalStateMgrTestUtil.testDbId1, GlobalStateMgrTestUtil.testTableId1)
                .getPartition(GlobalStateMgrTestUtil.testPartition1);
        // check partition version
        assertEquals(GlobalStateMgrTestUtil.testStartVersion, testPartition.getDefaultPhysicalPartition().getVisibleVersion());
        assertEquals(GlobalStateMgrTestUtil.testStartVersion + 2,
                testPartition.getDefaultPhysicalPartition().getNextVersion());
        // check partition next version
        LocalTablet tablet =
                (LocalTablet) testPartition.getDefaultPhysicalPartition().getIndex(GlobalStateMgrTestUtil.testIndexId1)
                        .getTablet(GlobalStateMgrTestUtil.testTabletId1);
        for (Replica replica : tablet.getImmutableReplicas()) {
            assertEquals(GlobalStateMgrTestUtil.testStartVersion, replica.getVersion());
        }
        // slave replay new state and compare globalStateMgr
        FakeGlobalStateMgr.setGlobalStateMgr(slaveGlobalStateMgr);
        slaveTransMgr.replayUpsertTransactionState(transactionState);
        assertTrue(GlobalStateMgrTestUtil.compareState(masterGlobalStateMgr, slaveGlobalStateMgr));
    }

    // commit with only two replicas
    @Test
    public void testCommitTransactionWithOneFailed() throws StarRocksException {
        TransactionState transactionState = null;
        FakeGlobalStateMgr.setGlobalStateMgr(masterGlobalStateMgr);
        long transactionId = masterTransMgr
                .beginTransaction(GlobalStateMgrTestUtil.testDbId1, Lists.newArrayList(GlobalStateMgrTestUtil.testTableId1),
                        GlobalStateMgrTestUtil.testTxnLable1,
                        transactionSource,
                        LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        // commit a transaction with 1,2 success
        TabletCommitInfo tabletCommitInfo1 = new TabletCommitInfo(GlobalStateMgrTestUtil.testTabletId1,
                GlobalStateMgrTestUtil.testBackendId1);
        TabletCommitInfo tabletCommitInfo2 = new TabletCommitInfo(GlobalStateMgrTestUtil.testTabletId1,
                GlobalStateMgrTestUtil.testBackendId2);
        List<TabletCommitInfo> transTablets = Lists.newArrayList();
        transTablets.add(tabletCommitInfo1);
        transTablets.add(tabletCommitInfo2);
        masterTransMgr.commitTransaction(GlobalStateMgrTestUtil.testDbId1, transactionId, transTablets,
                Lists.newArrayList(), null);

        // follower globalStateMgr replay the transaction
        transactionState = fakeEditLog.getTransaction(transactionId);
        FakeGlobalStateMgr.setGlobalStateMgr(slaveGlobalStateMgr);
        slaveTransMgr.replayUpsertTransactionState(transactionState);
        assertTrue(GlobalStateMgrTestUtil.compareState(masterGlobalStateMgr, slaveGlobalStateMgr));

        FakeGlobalStateMgr.setGlobalStateMgr(masterGlobalStateMgr);
        // commit another transaction with 1,3 success
        long transactionId2 = masterTransMgr
                .beginTransaction(GlobalStateMgrTestUtil.testDbId1, Lists.newArrayList(GlobalStateMgrTestUtil.testTableId1),
                        GlobalStateMgrTestUtil.testTxnLable2,
                        transactionSource,
                        LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        tabletCommitInfo1 = new TabletCommitInfo(GlobalStateMgrTestUtil.testTabletId1, GlobalStateMgrTestUtil.testBackendId1);
        TabletCommitInfo tabletCommitInfo3 = new TabletCommitInfo(GlobalStateMgrTestUtil.testTabletId1,
                GlobalStateMgrTestUtil.testBackendId3);
        transTablets = Lists.newArrayList();
        transTablets.add(tabletCommitInfo1);
        transTablets.add(tabletCommitInfo3);
        try {
            masterTransMgr.commitTransaction(GlobalStateMgrTestUtil.testDbId1, transactionId2, transTablets,
                    Lists.newArrayList(), null);
            Assertions.fail();
        } catch (TabletQuorumFailedException e) {
            transactionState = masterTransMgr.getTransactionState(GlobalStateMgrTestUtil.testDbId1, transactionId2);
            // check status is prepare, because the commit failed
            assertEquals(TransactionStatus.PREPARE, transactionState.getTransactionStatus());
        }
        // check replica version
        Partition testPartition =
                masterGlobalStateMgr.getLocalMetastore()
                        .getTable(GlobalStateMgrTestUtil.testDbId1, GlobalStateMgrTestUtil.testTableId1)
                        .getPartition(GlobalStateMgrTestUtil.testPartition1);
        // check partition version
        assertEquals(GlobalStateMgrTestUtil.testStartVersion, testPartition.getDefaultPhysicalPartition().getVisibleVersion());
        assertEquals(GlobalStateMgrTestUtil.testStartVersion + 2, testPartition.getDefaultPhysicalPartition().getNextVersion());
        // check partition next version
        LocalTablet tablet = (LocalTablet) testPartition.getDefaultPhysicalPartition()
                .getIndex(GlobalStateMgrTestUtil.testIndexId1)
                .getTablet(GlobalStateMgrTestUtil.testTabletId1);
        for (Replica replica : tablet.getImmutableReplicas()) {
            assertEquals(GlobalStateMgrTestUtil.testStartVersion, replica.getVersion());
        }
        // the transaction not committed, so that globalStateMgr should be equal
        assertTrue(GlobalStateMgrTestUtil.compareState(masterGlobalStateMgr, slaveGlobalStateMgr));

        // commit the second transaction with 1,2,3 success
        tabletCommitInfo1 = new TabletCommitInfo(GlobalStateMgrTestUtil.testTabletId1, GlobalStateMgrTestUtil.testBackendId1);
        tabletCommitInfo2 = new TabletCommitInfo(GlobalStateMgrTestUtil.testTabletId1, GlobalStateMgrTestUtil.testBackendId2);
        tabletCommitInfo3 = new TabletCommitInfo(GlobalStateMgrTestUtil.testTabletId1, GlobalStateMgrTestUtil.testBackendId3);
        transTablets = Lists.newArrayList();
        transTablets.add(tabletCommitInfo1);
        transTablets.add(tabletCommitInfo2);
        transTablets.add(tabletCommitInfo3);
        masterTransMgr.commitTransaction(GlobalStateMgrTestUtil.testDbId1, transactionId2, transTablets,
                Lists.newArrayList(), null);
        transactionState = fakeEditLog.getTransaction(transactionId2);
        // check status is commit
        assertEquals(TransactionStatus.COMMITTED, transactionState.getTransactionStatus());
        // check replica version
        testPartition = masterGlobalStateMgr.getLocalMetastore()
                .getTable(GlobalStateMgrTestUtil.testDbId1, GlobalStateMgrTestUtil.testTableId1)
                .getPartition(GlobalStateMgrTestUtil.testPartition1);
        // check partition version
        assertEquals(GlobalStateMgrTestUtil.testStartVersion, testPartition.getDefaultPhysicalPartition().getVisibleVersion());
        assertEquals(GlobalStateMgrTestUtil.testStartVersion + 3, testPartition.getDefaultPhysicalPartition().getNextVersion());
        // check partition next version
        tablet = (LocalTablet) testPartition.getDefaultPhysicalPartition().getIndex(GlobalStateMgrTestUtil.testIndexId1)
                .getTablet(GlobalStateMgrTestUtil.testTabletId1);
        for (Replica replica : tablet.getImmutableReplicas()) {
            assertEquals(GlobalStateMgrTestUtil.testStartVersion, replica.getVersion());
        }
        Replica replcia1 = tablet.getReplicaById(GlobalStateMgrTestUtil.testReplicaId1);
        Replica replcia2 = tablet.getReplicaById(GlobalStateMgrTestUtil.testReplicaId2);
        Replica replcia3 = tablet.getReplicaById(GlobalStateMgrTestUtil.testReplicaId3);
        assertEquals(GlobalStateMgrTestUtil.testStartVersion, replcia1.getVersion());
        assertEquals(GlobalStateMgrTestUtil.testStartVersion, replcia2.getVersion());
        assertEquals(GlobalStateMgrTestUtil.testStartVersion, replcia3.getVersion());
        assertEquals(-1, replcia1.getLastFailedVersion());
        assertEquals(-1, replcia2.getLastFailedVersion());
        assertEquals(GlobalStateMgrTestUtil.testStartVersion + 1, replcia3.getLastFailedVersion());

        // last success version not change, because not published
        assertEquals(GlobalStateMgrTestUtil.testStartVersion, replcia1.getLastSuccessVersion());
        assertEquals(GlobalStateMgrTestUtil.testStartVersion, replcia2.getLastSuccessVersion());
        assertEquals(GlobalStateMgrTestUtil.testStartVersion, replcia3.getLastSuccessVersion());
        // check partition version
        assertEquals(GlobalStateMgrTestUtil.testStartVersion, testPartition.getDefaultPhysicalPartition().getVisibleVersion());
        assertEquals(GlobalStateMgrTestUtil.testStartVersion + 3, testPartition.getDefaultPhysicalPartition().getNextVersion());

        transactionState = fakeEditLog.getTransaction(transactionId2);
        FakeGlobalStateMgr.setGlobalStateMgr(slaveGlobalStateMgr);
        slaveTransMgr.replayUpsertTransactionState(transactionState);
        assertTrue(GlobalStateMgrTestUtil.compareState(masterGlobalStateMgr, slaveGlobalStateMgr));
    }

    @Test
    public void testCommitRoutineLoadTransaction(@Injectable TabletCommitInfo tabletCommitInfo,
                                                 @Mocked EditLog editLog)
            throws StarRocksException {
        FakeGlobalStateMgr.setGlobalStateMgr(masterGlobalStateMgr);

        TabletCommitInfo tabletCommitInfo1 =
                new TabletCommitInfo(GlobalStateMgrTestUtil.testTabletId1, GlobalStateMgrTestUtil.testBackendId1);
        TabletCommitInfo tabletCommitInfo2 =
                new TabletCommitInfo(GlobalStateMgrTestUtil.testTabletId1, GlobalStateMgrTestUtil.testBackendId2);
        TabletCommitInfo tabletCommitInfo3 =
                new TabletCommitInfo(GlobalStateMgrTestUtil.testTabletId1, GlobalStateMgrTestUtil.testBackendId3);
        List<TabletCommitInfo> transTablets = Lists.newArrayList();
        transTablets.add(tabletCommitInfo1);
        transTablets.add(tabletCommitInfo2);
        transTablets.add(tabletCommitInfo3);

        KafkaRoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob(1L, "test", 1L, 1L, "host:port",
                "topic");
        List<RoutineLoadTaskInfo> routineLoadTaskInfoList =
                Deencapsulation.getField(routineLoadJob, "routineLoadTaskInfoList");
        Map<Integer, Long> partitionIdToOffset = Maps.newHashMap();
        partitionIdToOffset.put(1, 0L);
        KafkaTaskInfo routineLoadTaskInfo =
                new KafkaTaskInfo(UUIDUtil.genUUID(), routineLoadJob, 20000, System.currentTimeMillis(),
                        partitionIdToOffset, Config.routine_load_task_timeout_second);
        Deencapsulation.setField(routineLoadTaskInfo, "txnId", 1L);
        routineLoadTaskInfoList.add(routineLoadTaskInfo);
        TransactionState transactionState = new TransactionState(1L, Lists.newArrayList(1L), 1L, "label", null,
                LoadJobSourceType.ROUTINE_LOAD_TASK, new TxnCoordinator(TxnSourceType.BE, "be1"),
                routineLoadJob.getId(),
                Config.stream_load_default_timeout_second);
        transactionState.setTransactionStatus(TransactionStatus.PREPARE);
        masterTransMgr.getCallbackFactory().addCallback(routineLoadJob);
        // Deencapsulation.setField(transactionState, "txnStateChangeListener", routineLoadJob);
        Map<Long, TransactionState> idToTransactionState = Maps.newHashMap();
        idToTransactionState.put(1L, transactionState);
        Deencapsulation.setField(routineLoadJob, "maxErrorNum", 10);

        Map<Integer, Long> oldKafkaProgressMap = Maps.newHashMap();
        oldKafkaProgressMap.put(1, 0L);
        KafkaProgress oldkafkaProgress = new KafkaProgress(oldKafkaProgressMap);
        Deencapsulation.setField(routineLoadJob, "progress", oldkafkaProgress);

        Map<Integer, Long> kafkaTimestampProgressMap = Maps.newHashMap();
        kafkaTimestampProgressMap.put(1, 1701411701409L);
        KafkaProgress oldKafkaTimestampProgress = new KafkaProgress(kafkaTimestampProgressMap);
        Deencapsulation.setField(routineLoadJob, "timestampProgress", oldKafkaTimestampProgress);

        Deencapsulation.setField(routineLoadJob, "state", RoutineLoadJob.JobState.RUNNING);

        TRLTaskTxnCommitAttachment rlTaskTxnCommitAttachment = new TRLTaskTxnCommitAttachment();
        rlTaskTxnCommitAttachment.setId(new TUniqueId());
        rlTaskTxnCommitAttachment.setLoadedRows(100);
        rlTaskTxnCommitAttachment.setReceivedBytes(10000);
        rlTaskTxnCommitAttachment.setFilteredRows(1);
        rlTaskTxnCommitAttachment.setUnselectedRows(1);
        rlTaskTxnCommitAttachment.setJobId(Deencapsulation.getField(routineLoadJob, "id"));
        rlTaskTxnCommitAttachment.setLoadSourceType(TLoadSourceType.KAFKA);

        TKafkaRLTaskProgress tKafkaRLTaskProgress = new TKafkaRLTaskProgress();

        Map<Integer, Long> kafkaProgress = Maps.newHashMap();
        kafkaProgress.put(1, 100L); // start from 0, so rows number is 101, and consumed offset is 100
        tKafkaRLTaskProgress.setPartitionCmtOffset(kafkaProgress);
        rlTaskTxnCommitAttachment.setKafkaRLTaskProgress(tKafkaRLTaskProgress);

        Map<Integer, Long> kafkaTimestampProgress = Maps.newHashMap();
        kafkaTimestampProgress.put(1, 1701411701509L); // start from 0, so rows number is 101, and consumed offset is 100
        tKafkaRLTaskProgress.setPartitionCmtOffsetTimestamp(kafkaTimestampProgress);

        rlTaskTxnCommitAttachment.setKafkaRLTaskProgress(tKafkaRLTaskProgress);

        TxnCommitAttachment txnCommitAttachment = new RLTaskTxnCommitAttachment(rlTaskTxnCommitAttachment);

        RoutineLoadMgr routineLoadManager = new RoutineLoadMgr();
        Map<Long, Integer> beTasksNum = routineLoadManager.getNodeTasksNum();
        beTasksNum.put(1L, 0);
        routineLoadManager.addRoutineLoadJob(routineLoadJob, "db");

        Deencapsulation.setField(masterTransMgr.getDatabaseTransactionMgr(GlobalStateMgrTestUtil.testDbId1),
                "idToRunningTransactionState", idToTransactionState);
        masterTransMgr.commitTransaction(1L, 1L, transTablets, Lists.newArrayList(),
                txnCommitAttachment);

        Assertions.assertEquals(Long.valueOf(102), Deencapsulation.getField(routineLoadJob, "currentTotalRows"));
        Assertions.assertEquals(Long.valueOf(1), Deencapsulation.getField(routineLoadJob, "currentErrorRows"));
        Assertions.assertEquals(Long.valueOf(101L), ((KafkaProgress) routineLoadJob.getProgress()).getOffsetByPartition(1));
        // todo(ml): change to assert queue
        // Assert.assertEquals(1, routineLoadManager.getNeedScheduleTasksQueue().size());
        // Assert.assertNotEquals("label", routineLoadManager.getNeedScheduleTasksQueue().peek().getId());
        boolean oldValue = Config.lock_manager_enabled;
        Config.lock_manager_enabled = false;
        masterTransMgr.finishTransactionNew(transactionState, Sets.newHashSet());
        TableMetricsEntity entity = TableMetricsRegistry.getInstance().getMetricsEntity(1L);
        assertEquals(100, entity.counterRoutineLoadRowsTotal.getValue().intValue());
        assertEquals(10000, entity.counterRoutineLoadBytesTotal.getValue().intValue());
        assertEquals(1, entity.counterRoutineLoadFinishedTotal.getValue().intValue());
        assertEquals(1, entity.counterRoutineLoadErrorRowsTotal.getValue().intValue());
        assertEquals(1, entity.counterRoutineLoadUnselectedRowsTotal.getValue().intValue());
        Config.lock_manager_enabled = oldValue;
    }

    @Test
    public void testCommitRoutineLoadTransactionWithErrorMax(@Injectable TabletCommitInfo tabletCommitInfo,
                                                             @Mocked EditLog editLog) throws StarRocksException {

        FakeGlobalStateMgr.setGlobalStateMgr(masterGlobalStateMgr);

        TabletCommitInfo tabletCommitInfo1 =
                new TabletCommitInfo(GlobalStateMgrTestUtil.testTabletId1, GlobalStateMgrTestUtil.testBackendId1);
        TabletCommitInfo tabletCommitInfo2 =
                new TabletCommitInfo(GlobalStateMgrTestUtil.testTabletId1, GlobalStateMgrTestUtil.testBackendId2);
        TabletCommitInfo tabletCommitInfo3 =
                new TabletCommitInfo(GlobalStateMgrTestUtil.testTabletId1, GlobalStateMgrTestUtil.testBackendId3);
        List<TabletCommitInfo> transTablets = Lists.newArrayList();
        transTablets.add(tabletCommitInfo1);
        transTablets.add(tabletCommitInfo2);
        transTablets.add(tabletCommitInfo3);

        KafkaRoutineLoadJob routineLoadJob =
                new KafkaRoutineLoadJob(1L, "test", 1L, 1L, "host:port", "topic");
        List<RoutineLoadTaskInfo> routineLoadTaskInfoList =
                Deencapsulation.getField(routineLoadJob, "routineLoadTaskInfoList");
        Map<Integer, Long> partitionIdToOffset = Maps.newHashMap();
        partitionIdToOffset.put(1, 0L);
        KafkaTaskInfo routineLoadTaskInfo =
                new KafkaTaskInfo(UUIDUtil.genUUID(), routineLoadJob, 20000, System.currentTimeMillis(),
                        partitionIdToOffset, Config.routine_load_task_timeout_second);
        Deencapsulation.setField(routineLoadTaskInfo, "txnId", 1L);
        routineLoadTaskInfoList.add(routineLoadTaskInfo);
        TransactionState transactionState = new TransactionState(1L, Lists.newArrayList(1L), 1L, "label", null,
                LoadJobSourceType.ROUTINE_LOAD_TASK, new TxnCoordinator(TxnSourceType.BE, "be1"),
                routineLoadJob.getId(),
                Config.stream_load_default_timeout_second);
        transactionState.setTransactionStatus(TransactionStatus.PREPARE);
        masterTransMgr.getCallbackFactory().addCallback(routineLoadJob);
        Map<Long, TransactionState> idToTransactionState = Maps.newHashMap();
        idToTransactionState.put(1L, transactionState);
        Deencapsulation.setField(routineLoadJob, "maxErrorNum", 10);

        Map<Integer, Long> oldKafkaProgressMap = Maps.newHashMap();
        oldKafkaProgressMap.put(1, 0L);
        KafkaProgress oldkafkaProgress = new KafkaProgress(oldKafkaProgressMap);
        Deencapsulation.setField(routineLoadJob, "progress", oldkafkaProgress);

        Map<Integer, Long> oldKafkaTimestampProgressMap = Maps.newHashMap();
        oldKafkaTimestampProgressMap.put(1, 1701411701409L);
        KafkaProgress oldKafkaTimestampProgress = new KafkaProgress(oldKafkaTimestampProgressMap);
        Deencapsulation.setField(routineLoadJob, "timestampProgress", oldKafkaTimestampProgress);

        Deencapsulation.setField(routineLoadJob, "state", RoutineLoadJob.JobState.RUNNING);

        TRLTaskTxnCommitAttachment rlTaskTxnCommitAttachment = new TRLTaskTxnCommitAttachment();
        rlTaskTxnCommitAttachment.setId(new TUniqueId());
        rlTaskTxnCommitAttachment.setLoadedRows(100);
        rlTaskTxnCommitAttachment.setFilteredRows(11);
        rlTaskTxnCommitAttachment.setJobId(Deencapsulation.getField(routineLoadJob, "id"));
        rlTaskTxnCommitAttachment.setLoadSourceType(TLoadSourceType.KAFKA);

        TKafkaRLTaskProgress tKafkaRLTaskProgress = new TKafkaRLTaskProgress();

        Map<Integer, Long> kafkaProgress = Maps.newHashMap();
        kafkaProgress.put(1, 110L); // start from 0, so rows number is 101, and consumed offset is 100
        tKafkaRLTaskProgress.setPartitionCmtOffset(kafkaProgress);
        rlTaskTxnCommitAttachment.setKafkaRLTaskProgress(tKafkaRLTaskProgress);

        Map<Integer, Long> kafkaTimestampProgress = Maps.newHashMap();
        kafkaTimestampProgress.put(1, 1701411701609L); // start from 0, so rows number is 101, and consumed offset is 100
        tKafkaRLTaskProgress.setPartitionCmtOffsetTimestamp(kafkaTimestampProgress);

        rlTaskTxnCommitAttachment.setKafkaRLTaskProgress(tKafkaRLTaskProgress);

        TxnCommitAttachment txnCommitAttachment = new RLTaskTxnCommitAttachment(rlTaskTxnCommitAttachment);

        RoutineLoadMgr routineLoadManager = new RoutineLoadMgr();
        Map<Long, Integer> beTasksNum = routineLoadManager.getNodeTasksNum();
        beTasksNum.put(1L, 0);
        routineLoadManager.addRoutineLoadJob(routineLoadJob, "db");

        Deencapsulation.setField(masterTransMgr.getDatabaseTransactionMgr(GlobalStateMgrTestUtil.testDbId1),
                "idToRunningTransactionState", idToTransactionState);
        masterTransMgr.commitTransaction(1L, 1L, transTablets, Lists.newArrayList(),
                txnCommitAttachment);

        Assertions.assertEquals(Long.valueOf(0), Deencapsulation.getField(routineLoadJob, "currentTotalRows"));
        Assertions.assertEquals(Long.valueOf(0), Deencapsulation.getField(routineLoadJob, "currentErrorRows"));
        Assertions.assertEquals(Long.valueOf(111L),
                ((KafkaProgress) routineLoadJob.getProgress()).getOffsetByPartition(1));
        // todo(ml): change to assert queue
        // Assert.assertEquals(0, routineLoadManager.getNeedScheduleTasksQueue().size());
        Assertions.assertEquals(RoutineLoadJob.JobState.PAUSED, routineLoadJob.getState());
    }

    @Test
    public void testFinishTransaction() throws StarRocksException {
        FakeGlobalStateMgr.setGlobalStateMgr(masterGlobalStateMgr);

        long transactionId = masterTransMgr
                .beginTransaction(GlobalStateMgrTestUtil.testDbId1, Lists.newArrayList(GlobalStateMgrTestUtil.testTableId1),
                        GlobalStateMgrTestUtil.testTxnLable1,
                        transactionSource,
                        LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        // commit a transaction
        TabletCommitInfo tabletCommitInfo1 = new TabletCommitInfo(GlobalStateMgrTestUtil.testTabletId1,
                GlobalStateMgrTestUtil.testBackendId1);
        TabletCommitInfo tabletCommitInfo2 = new TabletCommitInfo(GlobalStateMgrTestUtil.testTabletId1,
                GlobalStateMgrTestUtil.testBackendId2);
        TabletCommitInfo tabletCommitInfo3 = new TabletCommitInfo(GlobalStateMgrTestUtil.testTabletId1,
                GlobalStateMgrTestUtil.testBackendId3);
        List<TabletCommitInfo> transTablets = Lists.newArrayList();
        transTablets.add(tabletCommitInfo1);
        transTablets.add(tabletCommitInfo2);
        transTablets.add(tabletCommitInfo3);
        masterTransMgr.commitTransaction(GlobalStateMgrTestUtil.testDbId1, transactionId, transTablets,
                Lists.newArrayList(), null);
        TransactionState transactionState = fakeEditLog.getTransaction(transactionId);
        slaveTransMgr.replayUpsertTransactionState(transactionState);
        assertEquals(TransactionStatus.COMMITTED, transactionState.getTransactionStatus());
        Set<Long> errorReplicaIds = Sets.newHashSet();
        errorReplicaIds.add(GlobalStateMgrTestUtil.testReplicaId1);
        masterTransMgr.finishTransaction(GlobalStateMgrTestUtil.testDbId1, transactionId, errorReplicaIds);
        transactionState = fakeEditLog.getTransaction(transactionId);
        assertEquals(TransactionStatus.VISIBLE, transactionState.getTransactionStatus());
        // check replica version
        Partition testPartition = masterGlobalStateMgr.getLocalMetastore()
                .getDb(GlobalStateMgrTestUtil.testDbId1).getTable(GlobalStateMgrTestUtil.testTableId1)
                .getPartition(GlobalStateMgrTestUtil.testPartition1);
        // check partition version
        assertEquals(GlobalStateMgrTestUtil.testStartVersion + 1,
                testPartition.getDefaultPhysicalPartition().getVisibleVersion());
        assertEquals(GlobalStateMgrTestUtil.testStartVersion + 2, testPartition.getDefaultPhysicalPartition().getNextVersion());
        // check partition next version
        LocalTablet tablet =
                (LocalTablet) testPartition.getDefaultPhysicalPartition().getIndex(GlobalStateMgrTestUtil.testIndexId1)
                        .getTablet(GlobalStateMgrTestUtil.testTabletId1);
        for (Replica replica : tablet.getImmutableReplicas()) {
            if (replica.getId() == GlobalStateMgrTestUtil.testReplicaId1) {
                assertEquals(GlobalStateMgrTestUtil.testStartVersion, replica.getVersion());
            } else {
                assertEquals(GlobalStateMgrTestUtil.testStartVersion + 1, replica.getVersion());
            }

        }
        // slave replay new state and compare globalStateMgr
        slaveTransMgr.replayUpsertTransactionState(transactionState);
        assertTrue(GlobalStateMgrTestUtil.compareState(masterGlobalStateMgr, slaveGlobalStateMgr));
    }

    @Test
    public void testFinishTransactionWithOneFailed() throws StarRocksException {
        TransactionState transactionState = null;
        Partition testPartition =
                masterGlobalStateMgr.getLocalMetastore()
                        .getTable(GlobalStateMgrTestUtil.testDbId1, GlobalStateMgrTestUtil.testTableId1)
                        .getPartition(GlobalStateMgrTestUtil.testPartition1);
        LocalTablet tablet =
                (LocalTablet) testPartition.getDefaultPhysicalPartition().getIndex(GlobalStateMgrTestUtil.testIndexId1)
                        .getTablet(GlobalStateMgrTestUtil.testTabletId1);
        FakeGlobalStateMgr.setGlobalStateMgr(masterGlobalStateMgr);
        long transactionId = masterTransMgr
                .beginTransaction(GlobalStateMgrTestUtil.testDbId1, Lists.newArrayList(GlobalStateMgrTestUtil.testTableId1),
                        GlobalStateMgrTestUtil.testTxnLable1,
                        transactionSource,
                        LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        // commit a transaction with 1,2 success
        TabletCommitInfo tabletCommitInfo1 = new TabletCommitInfo(GlobalStateMgrTestUtil.testTabletId1,
                GlobalStateMgrTestUtil.testBackendId1);
        TabletCommitInfo tabletCommitInfo2 = new TabletCommitInfo(GlobalStateMgrTestUtil.testTabletId1,
                GlobalStateMgrTestUtil.testBackendId2);
        List<TabletCommitInfo> transTablets = Lists.newArrayList();
        transTablets.add(tabletCommitInfo1);
        transTablets.add(tabletCommitInfo2);
        masterTransMgr.commitTransaction(GlobalStateMgrTestUtil.testDbId1, transactionId, transTablets,
                Lists.newArrayList(), null);

        // follower globalStateMgr replay the transaction
        transactionState = fakeEditLog.getTransaction(transactionId);
        FakeGlobalStateMgr.setGlobalStateMgr(slaveGlobalStateMgr);
        slaveTransMgr.replayUpsertTransactionState(transactionState);
        assertTrue(GlobalStateMgrTestUtil.compareState(masterGlobalStateMgr, slaveGlobalStateMgr));

        // master finish the transaction failed
        FakeGlobalStateMgr.setGlobalStateMgr(masterGlobalStateMgr);
        Set<Long> errorReplicaIds = Sets.newHashSet();
        errorReplicaIds.add(GlobalStateMgrTestUtil.testReplicaId2);
        assertEquals(masterTransMgr.canTxnFinished(transactionState, errorReplicaIds, Sets.newHashSet()), false);
        masterTransMgr.finishTransaction(GlobalStateMgrTestUtil.testDbId1, transactionId, errorReplicaIds);
        assertEquals(TransactionStatus.COMMITTED, transactionState.getTransactionStatus());
        Replica replcia1 = tablet.getReplicaById(GlobalStateMgrTestUtil.testReplicaId1);
        Replica replcia2 = tablet.getReplicaById(GlobalStateMgrTestUtil.testReplicaId2);
        Replica replcia3 = tablet.getReplicaById(GlobalStateMgrTestUtil.testReplicaId3);
        assertEquals(GlobalStateMgrTestUtil.testStartVersion + 1, replcia1.getVersion());
        assertEquals(GlobalStateMgrTestUtil.testStartVersion, replcia2.getVersion());
        assertEquals(GlobalStateMgrTestUtil.testStartVersion, replcia3.getVersion());
        assertEquals(-1, replcia1.getLastFailedVersion());
        assertEquals(-1, replcia2.getLastFailedVersion());
        assertEquals(GlobalStateMgrTestUtil.testStartVersion + 1, replcia3.getLastFailedVersion());

        errorReplicaIds = Sets.newHashSet();
        Set<Long> unfinishedBackends = Sets.newHashSet(replcia2.getBackendId(), replcia3.getBackendId());
        assertEquals(masterTransMgr.canTxnFinished(transactionState, errorReplicaIds, unfinishedBackends), false);
        errorReplicaIds = Sets.newHashSet();
        assertEquals(masterTransMgr.canTxnFinished(transactionState, errorReplicaIds, Sets.newHashSet()), true);
        masterTransMgr.finishTransaction(GlobalStateMgrTestUtil.testDbId1, transactionId, errorReplicaIds);
        assertEquals(TransactionStatus.VISIBLE, transactionState.getTransactionStatus());
        assertEquals(GlobalStateMgrTestUtil.testStartVersion + 1, replcia1.getVersion());
        assertEquals(GlobalStateMgrTestUtil.testStartVersion + 1, replcia2.getVersion());
        assertEquals(GlobalStateMgrTestUtil.testStartVersion, replcia3.getVersion());
        assertEquals(-1, replcia1.getLastFailedVersion());
        assertEquals(-1, replcia2.getLastFailedVersion());
        assertEquals(GlobalStateMgrTestUtil.testStartVersion + 1, replcia3.getLastFailedVersion());

        // follower globalStateMgr replay the transaction
        transactionState = fakeEditLog.getTransaction(transactionId);
        FakeGlobalStateMgr.setGlobalStateMgr(slaveGlobalStateMgr);
        slaveTransMgr.replayUpsertTransactionState(transactionState);
        assertTrue(GlobalStateMgrTestUtil.compareState(masterGlobalStateMgr, slaveGlobalStateMgr));

        FakeGlobalStateMgr.setGlobalStateMgr(masterGlobalStateMgr);
        // commit another transaction with 1,3 success
        long transactionId2 = masterTransMgr
                .beginTransaction(GlobalStateMgrTestUtil.testDbId1, Lists.newArrayList(GlobalStateMgrTestUtil.testTableId1),
                        GlobalStateMgrTestUtil.testTxnLable2,
                        transactionSource,
                        LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        tabletCommitInfo1 = new TabletCommitInfo(GlobalStateMgrTestUtil.testTabletId1, GlobalStateMgrTestUtil.testBackendId1);
        TabletCommitInfo tabletCommitInfo3 = new TabletCommitInfo(GlobalStateMgrTestUtil.testTabletId1,
                GlobalStateMgrTestUtil.testBackendId3);
        transTablets = Lists.newArrayList();
        transTablets.add(tabletCommitInfo1);
        transTablets.add(tabletCommitInfo3);
        try {
            masterTransMgr.commitTransaction(GlobalStateMgrTestUtil.testDbId1, transactionId2, transTablets,
                    Lists.newArrayList(), null);
            Assertions.fail();
        } catch (TabletQuorumFailedException e) {
            transactionState = masterTransMgr.getTransactionState(GlobalStateMgrTestUtil.testDbId1, transactionId2);
            // check status is prepare, because the commit failed
            assertEquals(TransactionStatus.PREPARE, transactionState.getTransactionStatus());
        }

        // commit the second transaction with 1,2,3 success
        tabletCommitInfo1 = new TabletCommitInfo(GlobalStateMgrTestUtil.testTabletId1, GlobalStateMgrTestUtil.testBackendId1);
        tabletCommitInfo2 = new TabletCommitInfo(GlobalStateMgrTestUtil.testTabletId1, GlobalStateMgrTestUtil.testBackendId2);
        tabletCommitInfo3 = new TabletCommitInfo(GlobalStateMgrTestUtil.testTabletId1, GlobalStateMgrTestUtil.testBackendId3);
        transTablets = Lists.newArrayList();
        transTablets.add(tabletCommitInfo1);
        transTablets.add(tabletCommitInfo2);
        transTablets.add(tabletCommitInfo3);
        masterTransMgr.commitTransaction(GlobalStateMgrTestUtil.testDbId1, transactionId2, transTablets,
                Lists.newArrayList(), null);
        transactionState = fakeEditLog.getTransaction(transactionId2);
        // check status is commit
        assertEquals(TransactionStatus.COMMITTED, transactionState.getTransactionStatus());
        // check replica version
        testPartition = masterGlobalStateMgr.getLocalMetastore()
                .getTable(GlobalStateMgrTestUtil.testDbId1, GlobalStateMgrTestUtil.testTableId1)
                .getPartition(GlobalStateMgrTestUtil.testPartition1);
        // check partition version
        assertEquals(GlobalStateMgrTestUtil.testStartVersion + 1,
                testPartition.getDefaultPhysicalPartition().getVisibleVersion());
        assertEquals(GlobalStateMgrTestUtil.testStartVersion + 3, testPartition.getDefaultPhysicalPartition().getNextVersion());

        // follower globalStateMgr replay the transaction
        transactionState = fakeEditLog.getTransaction(transactionId2);
        FakeGlobalStateMgr.setGlobalStateMgr(slaveGlobalStateMgr);
        slaveTransMgr.replayUpsertTransactionState(transactionState);
        assertTrue(GlobalStateMgrTestUtil.compareState(masterGlobalStateMgr, slaveGlobalStateMgr));

        // master finish the transaction2
        errorReplicaIds = Sets.newHashSet();
        assertEquals(masterTransMgr.canTxnFinished(transactionState, errorReplicaIds, unfinishedBackends), false);
        errorReplicaIds = Sets.newHashSet();
        FakeGlobalStateMgr.setGlobalStateMgr(masterGlobalStateMgr);
        masterTransMgr.finishTransaction(GlobalStateMgrTestUtil.testDbId1, transactionId2, errorReplicaIds);
        assertEquals(TransactionStatus.VISIBLE, transactionState.getTransactionStatus());
        assertEquals(GlobalStateMgrTestUtil.testStartVersion + 2, replcia1.getVersion());
        assertEquals(GlobalStateMgrTestUtil.testStartVersion + 2, replcia2.getVersion());
        assertEquals(GlobalStateMgrTestUtil.testStartVersion, replcia3.getVersion());
        assertEquals(-1, replcia1.getLastFailedVersion());
        assertEquals(-1, replcia2.getLastFailedVersion());
        assertEquals(GlobalStateMgrTestUtil.testStartVersion + 1, replcia3.getLastFailedVersion());

        assertEquals(GlobalStateMgrTestUtil.testStartVersion + 2, replcia1.getLastSuccessVersion());
        assertEquals(GlobalStateMgrTestUtil.testStartVersion + 2, replcia2.getLastSuccessVersion());
        assertEquals(GlobalStateMgrTestUtil.testStartVersion + 2, replcia3.getLastSuccessVersion());
        // check partition version
        assertEquals(GlobalStateMgrTestUtil.testStartVersion + 2,
                testPartition.getDefaultPhysicalPartition().getVisibleVersion());
        assertEquals(GlobalStateMgrTestUtil.testStartVersion + 3, testPartition.getDefaultPhysicalPartition().getNextVersion());

        transactionState = fakeEditLog.getTransaction(transactionId2);
        FakeGlobalStateMgr.setGlobalStateMgr(slaveGlobalStateMgr);
        slaveTransMgr.replayUpsertTransactionState(transactionState);
        assertTrue(GlobalStateMgrTestUtil.compareState(masterGlobalStateMgr, slaveGlobalStateMgr));
    }

    @Test
    public void replayWithExpiredJob() throws Exception {
        Config.label_keep_max_second = 1;
        long dbId = 1;
        Assertions.assertEquals(0, masterTransMgr.getDatabaseTransactionMgr(dbId).getTransactionNum());

        // 1. replay a normal finished transaction
        TransactionState state =
                new TransactionState(dbId, new ArrayList<>(), 1, "label_a", null, LoadJobSourceType.BACKEND_STREAMING,
                        transactionSource, -1, -1);
        state.setTransactionStatus(TransactionStatus.ABORTED);
        state.setReason("fake reason");
        state.setFinishTime(System.currentTimeMillis() - 2000);
        masterTransMgr.replayUpsertTransactionState(state);
        Assertions.assertEquals(0, masterTransMgr.getDatabaseTransactionMgr(dbId).getTransactionNum());

        // 2. replay a expired transaction
        TransactionState state2 =
                new TransactionState(dbId, new ArrayList<>(), 2, "label_b", null, LoadJobSourceType.BACKEND_STREAMING,
                        transactionSource, -1, -1);
        state2.setTransactionStatus(TransactionStatus.ABORTED);
        state2.setReason("fake reason");
        state2.setFinishTime(System.currentTimeMillis());
        masterTransMgr.replayUpsertTransactionState(state2);
        Assertions.assertEquals(1, masterTransMgr.getDatabaseTransactionMgr(dbId).getTransactionNum());

        Thread.sleep(2000);
        // 3. replay a valid transaction, let state expire
        TransactionState state3 =
                new TransactionState(dbId, new ArrayList<>(), 3, "label_c", null, LoadJobSourceType.BACKEND_STREAMING,
                        transactionSource, -1, -1);
        state3.setTransactionStatus(TransactionStatus.ABORTED);
        state3.setReason("fake reason");
        state3.setFinishTime(System.currentTimeMillis());
        masterTransMgr.replayUpsertTransactionState(state3);
        Assertions.assertEquals(2, masterTransMgr.getDatabaseTransactionMgr(dbId).getTransactionNum());

        // 4. write (state, state2) to image
        File tempFile = File.createTempFile("GlobalTransactionMgrTest", ".image");
        System.err.println("write image " + tempFile.getAbsolutePath());
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(tempFile));
        ImageWriter imageWriter = new ImageWriter("", 0);
        imageWriter.setOutputStream(dos);
        masterTransMgr.saveTransactionStateV2(imageWriter);
        dos.close();

        masterTransMgr.removeDatabaseTransactionMgr(dbId);
        masterTransMgr.addDatabaseTransactionMgr(dbId);

        // 4. read & check if expired
        DataInputStream dis = new DataInputStream(new FileInputStream(tempFile));
        Assertions.assertEquals(0, masterTransMgr.getDatabaseTransactionMgr(dbId).getTransactionNum());
        SRMetaBlockReader srMetaBlockReader = new SRMetaBlockReaderV2(new JsonReader(new InputStreamReader(dis)));
        masterTransMgr.loadTransactionStateV2(srMetaBlockReader);
        dis.close();
        Assertions.assertEquals(1, masterTransMgr.getDatabaseTransactionMgr(dbId).getTransactionNum());
        tempFile.delete();
    }

    @Test
    public void testPrepareTransaction() throws StarRocksException {
        FakeGlobalStateMgr.setGlobalStateMgr(masterGlobalStateMgr);

        long transactionId = masterTransMgr
                .beginTransaction(GlobalStateMgrTestUtil.testDbId1, Lists.newArrayList(GlobalStateMgrTestUtil.testTableId1),
                        GlobalStateMgrTestUtil.testTxnLable1,
                        transactionSource,
                        LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        // commit a transaction
        TabletCommitInfo tabletCommitInfo1 = new TabletCommitInfo(GlobalStateMgrTestUtil.testTabletId1,
                GlobalStateMgrTestUtil.testBackendId1);
        TabletCommitInfo tabletCommitInfo2 = new TabletCommitInfo(GlobalStateMgrTestUtil.testTabletId1,
                GlobalStateMgrTestUtil.testBackendId2);
        TabletCommitInfo tabletCommitInfo3 = new TabletCommitInfo(GlobalStateMgrTestUtil.testTabletId1,
                GlobalStateMgrTestUtil.testBackendId3);
        List<TabletCommitInfo> transTablets = Lists.newArrayList();
        transTablets.add(tabletCommitInfo1);
        transTablets.add(tabletCommitInfo2);
        transTablets.add(tabletCommitInfo3);

        TManualLoadTxnCommitAttachment loadTxnCommitAttachment = new TManualLoadTxnCommitAttachment();
        loadTxnCommitAttachment.setLoadedRows(100);
        loadTxnCommitAttachment.setLoadedBytes(10000);
        loadTxnCommitAttachment.setFilteredRows(0);
        TxnCommitAttachment txnCommitAttachment = new ManualLoadTxnCommitAttachment(loadTxnCommitAttachment);
        TableMetricsEntity entity = TableMetricsRegistry.getInstance().getMetricsEntity(GlobalStateMgrTestUtil.testTableId1);
        assertEquals(0, entity.counterStreamLoadRowsTotal.getValue().intValue());
        assertEquals(0, entity.counterStreamLoadBytesTotal.getValue().intValue());
        assertEquals(0, entity.counterStreamLoadFinishedTotal.getValue().intValue());
        masterTransMgr.prepareTransaction(GlobalStateMgrTestUtil.testDbId1, transactionId, transTablets,
                Lists.newArrayList(), txnCommitAttachment);
        TransactionState transactionState = fakeEditLog.getTransaction(transactionId);
        assertEquals(TransactionStatus.PREPARED, transactionState.getTransactionStatus());

        try {
            masterTransMgr.commitPreparedTransaction(
                    masterGlobalStateMgr.getLocalMetastore().getDb(GlobalStateMgrTestUtil.testDbId1), transactionId,
                    (long) 1000);
            Assertions.fail("should throw publish timeout exception");
        } catch (StarRocksException e) {
        }
        transactionState = fakeEditLog.getTransaction(transactionId);
        assertEquals(TransactionStatus.COMMITTED, transactionState.getTransactionStatus());
        slaveTransMgr.replayUpsertTransactionState(transactionState);

        Set<Long> errorReplicaIds = Sets.newHashSet();
        errorReplicaIds.add(GlobalStateMgrTestUtil.testReplicaId1);
        masterTransMgr.finishTransaction(GlobalStateMgrTestUtil.testDbId1, transactionId, errorReplicaIds);
        assertEquals(100, entity.counterStreamLoadRowsTotal.getValue().intValue());
        assertEquals(10000, entity.counterStreamLoadBytesTotal.getValue().intValue());
        assertEquals(1, entity.counterStreamLoadFinishedTotal.getValue().intValue());
        transactionState = fakeEditLog.getTransaction(transactionId);
        assertEquals(TransactionStatus.VISIBLE, transactionState.getTransactionStatus());
        // check replica version
        Partition testPartition =
                masterGlobalStateMgr.getLocalMetastore()
                        .getTable(GlobalStateMgrTestUtil.testDbId1, GlobalStateMgrTestUtil.testTableId1)
                        .getPartition(GlobalStateMgrTestUtil.testPartition1);
        // check partition version
        assertEquals(GlobalStateMgrTestUtil.testStartVersion + 1,
                testPartition.getDefaultPhysicalPartition().getVisibleVersion());
        assertEquals(GlobalStateMgrTestUtil.testStartVersion + 2, testPartition.getDefaultPhysicalPartition().getNextVersion());
        // check partition next version
        LocalTablet tablet =
                (LocalTablet) testPartition.getDefaultPhysicalPartition().getIndex(GlobalStateMgrTestUtil.testIndexId1)
                        .getTablet(GlobalStateMgrTestUtil.testTabletId1);
        for (Replica replica : tablet.getImmutableReplicas()) {
            if (replica.getId() == GlobalStateMgrTestUtil.testReplicaId1) {
                assertEquals(GlobalStateMgrTestUtil.testStartVersion, replica.getVersion());
            } else {
                assertEquals(GlobalStateMgrTestUtil.testStartVersion + 1, replica.getVersion());
            }

        }
        // slave replay new state and compare globalStateMgr
        slaveTransMgr.replayUpsertTransactionState(transactionState);
        assertTrue(GlobalStateMgrTestUtil.compareState(masterGlobalStateMgr, slaveGlobalStateMgr));
    }

    @Test
    public void testSaveLoadJsonFormatImage() throws Exception {
        long transactionId = masterTransMgr
                .beginTransaction(GlobalStateMgrTestUtil.testDbId1, Lists.newArrayList(GlobalStateMgrTestUtil.testTableId1),
                        GlobalStateMgrTestUtil.testTxnLable1,
                        transactionSource,
                        LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);

        UtFrameUtils.PseudoImage pseudoImage = new UtFrameUtils.PseudoImage();
        masterTransMgr.saveTransactionStateV2(pseudoImage.getImageWriter());

        GlobalTransactionMgr followerTransMgr = new GlobalTransactionMgr(masterGlobalStateMgr);
        followerTransMgr.addDatabaseTransactionMgr(GlobalStateMgrTestUtil.testDbId1);
        SRMetaBlockReader reader = new SRMetaBlockReaderV2(pseudoImage.getJsonReader());
        followerTransMgr.loadTransactionStateV2(reader);
        reader.close();

        Assertions.assertEquals(1, followerTransMgr.getTransactionNum());
    }

    @Test
    public void testRetryCommitOnRateLimitExceededTimeout()
            throws StarRocksException {
        Database db = new Database(10, "db0");
        GlobalTransactionMgr globalTransactionMgr = spy(new GlobalTransactionMgr(GlobalStateMgr.getCurrentState()));
        DatabaseTransactionMgr dbTransactionMgr = spy(new DatabaseTransactionMgr(10L, GlobalStateMgr.getCurrentState()));
        TransactionState transactionState = spy(new TransactionState());

        long now = System.currentTimeMillis();
        doReturn(transactionState).when(globalTransactionMgr).getTransactionState(db.getId(), 1001);
        doReturn(dbTransactionMgr).when(globalTransactionMgr).getDatabaseTransactionMgr(db.getId());
        doThrow(new CommitRateExceededException(1001, now + 60 * 1000L))
                .when(dbTransactionMgr)
                .commitTransaction(1001L, Collections.emptyList(), Collections.emptyList(), null);
        Assertions.assertThrows(CommitRateExceededException.class,
                () -> globalTransactionMgr.commitAndPublishTransaction(db, 1001,
                Collections.emptyList(), Collections.emptyList(), 10, null));
    }

    @Test
    public void testPublishVersionTimeout()
            throws StarRocksException, LockTimeoutException {
        Database db = new Database(10, "db0");
        GlobalTransactionMgr globalTransactionMgr = spy(new GlobalTransactionMgr(GlobalStateMgr.getCurrentState()));
        DatabaseTransactionMgr dbTransactionMgr = spy(new DatabaseTransactionMgr(10L, GlobalStateMgr.getCurrentState()));
        TransactionState transactionState = spy(new TransactionState());

        long now = System.currentTimeMillis();
        doReturn(dbTransactionMgr).when(globalTransactionMgr).getDatabaseTransactionMgr(db.getId());
        doReturn(transactionState).when(globalTransactionMgr).getTransactionState(db.getId(), 1001);
        doReturn(new VisibleStateWaiter(new TransactionState()))
                .when(dbTransactionMgr)
                .commitTransaction(1001L, Collections.emptyList(), Collections.emptyList(), null);
        Assertions.assertFalse(globalTransactionMgr.commitAndPublishTransaction(db, 1001,
                Collections.emptyList(), Collections.emptyList(), 2, null));
    }

    @Test
    public void testRetryCommitOnRateLimitExceededThrowUnexpectedException()
            throws StarRocksException {
        Database db = new Database(10, "db0");
        GlobalTransactionMgr globalTransactionMgr = spy(new GlobalTransactionMgr(GlobalStateMgr.getCurrentState()));
        DatabaseTransactionMgr dbTransactionMgr = spy(new DatabaseTransactionMgr(10L, GlobalStateMgr.getCurrentState()));

        doReturn(dbTransactionMgr).when(globalTransactionMgr).getDatabaseTransactionMgr(db.getId());
        doThrow(NullPointerException.class)
                .when(dbTransactionMgr)
                .commitTransaction(1001L, Collections.emptyList(), Collections.emptyList(), null);
        Assertions.assertThrows(StarRocksException.class, () -> globalTransactionMgr.commitAndPublishTransaction(db, 1001,
                Collections.emptyList(), Collections.emptyList(), 10, null));
    }

    @Test
    public void testRetryCommitOnRateLimitExceededThrowLockTimeoutException()
            throws StarRocksException, LockTimeoutException {
        Database db = new Database(10L, "db0");
        GlobalTransactionMgr globalTransactionMgr = spy(new GlobalTransactionMgr(GlobalStateMgr.getCurrentState()));
        TransactionState transactionState = new TransactionState();
        doReturn(transactionState)
                .when(globalTransactionMgr)
                .getTransactionState(10L, 1001L);

        doThrow(LockTimeoutException.class)
                .when(globalTransactionMgr)
                .retryCommitOnRateLimitExceeded(db, 1001L, Collections.emptyList(), Collections.emptyList(), null, 10L);
        Assertions.assertThrows(LockTimeoutException.class, () -> globalTransactionMgr.commitAndPublishTransaction(db, 1001L,
                Collections.emptyList(), Collections.emptyList(), 10L, null));
    }

    @Test
    public void testGetTransactionNumByCoordinateBe() throws LabelAlreadyUsedException, AnalysisException,
            RunningTxnExceedException, DuplicatedRequestException {
        masterTransMgr
                .beginTransaction(GlobalStateMgrTestUtil.testDbId1, Lists.newArrayList(GlobalStateMgrTestUtil.testTableId1),
                        GlobalStateMgrTestUtil.testTxnLable1,
                        transactionSourceBe,
                        LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        long res = masterTransMgr.getTransactionNumByCoordinateBe("localbe");
        assertEquals(1, res);
    }

    @Test
    public void testBeginTransactionFailed() {
        Config.disable_load_job = true;
        boolean exceptionThrown = false;
        try {
            masterTransMgr.beginTransaction(1L, null, "xxx", null, null, LoadJobSourceType.FRONTEND, 1L, 1000L);
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof ErrorReportException);
            Assertions.assertEquals(ErrorCode.ERR_BEGIN_TXN_FAILED, ((ErrorReportException) e).getErrorCode());
            exceptionThrown = true;
        } finally {
            Config.disable_load_job = false;
        }
        Assertions.assertTrue(exceptionThrown);

        Config.metadata_enable_recovery_mode = true;
        exceptionThrown = false;
        try {
            masterTransMgr.beginTransaction(1L, null, "xxx", null, null, LoadJobSourceType.FRONTEND, 1L, 1000L);
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof ErrorReportException);
            Assertions.assertEquals(ErrorCode.ERR_BEGIN_TXN_FAILED, ((ErrorReportException) e).getErrorCode());
            exceptionThrown = true;
        } finally {
            Config.metadata_enable_recovery_mode = false;
        }
        Assertions.assertTrue(exceptionThrown);

        GlobalStateMgr.getCurrentState().setSafeMode(true);
        exceptionThrown = false;
        try {
            masterTransMgr.beginTransaction(1L, null, "xxx", null, null, LoadJobSourceType.FRONTEND, 1L, 1000L);
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof ErrorReportException);
            Assertions.assertEquals(ErrorCode.ERR_BEGIN_TXN_FAILED, ((ErrorReportException) e).getErrorCode());
            exceptionThrown = true;
        } finally {
            GlobalStateMgr.getCurrentState().setSafeMode(false);
        }
        Assertions.assertTrue(exceptionThrown);
    }

    @Test
    public void testCommitLockTimeout() throws StarRocksException, LockTimeoutException {
        Database db = new Database(10L, "db0");
        GlobalTransactionMgr globalTransactionMgr = spy(new GlobalTransactionMgr(GlobalStateMgr.getCurrentState()));
        doThrow(LockTimeoutException.class)
                .when(globalTransactionMgr)
                .commitAndPublishTransaction(db, 1001L, Collections.emptyList(), Collections.emptyList(), 10L, null);
        Assertions.assertThrows(ErrorReportException.class, () -> globalTransactionMgr.commitAndPublishTransaction(db, 1001L,
                Collections.emptyList(), Collections.emptyList(), 10L));
    }

    @Test
    public void testCheckValidTimeoutSecond() {
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class,
                "Invalid timeout: '1'. Expected values should be between 2 and 3 seconds",
                () -> GlobalTransactionMgr.checkValidTimeoutSecond(1, 3, 2));
    }

    @Test
    public void testGetLabelStatus() throws Exception {
        FakeGlobalStateMgr.setGlobalStateMgr(masterGlobalStateMgr);
        String label = UUIDUtil.genUUID().toString();
        TransactionStateSnapshot state1 = masterTransMgr.getLabelStatus(GlobalStateMgrTestUtil.testDbId1, label);
        Assertions.assertNotNull(state1);
        Assertions.assertEquals(TransactionStatus.UNKNOWN, state1.getStatus());
        Assertions.assertNull(state1.getReason());

        masterTransMgr.beginTransaction(GlobalStateMgrTestUtil.testDbId1, Lists.newArrayList(GlobalStateMgrTestUtil.testTableId1),
                        label, transactionSource, LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        TransactionStateSnapshot state2 = masterTransMgr.getLabelStatus(GlobalStateMgrTestUtil.testDbId1, label);
        Assertions.assertNotNull(state2);
        Assertions.assertEquals(TransactionStatus.PREPARE, state2.getStatus());
        Assertions.assertEquals("", state2.getReason());

        masterTransMgr.abortTransaction(GlobalStateMgrTestUtil.testDbId1, label, "artificial failure");
        TransactionStateSnapshot state3 = masterTransMgr.getLabelStatus(GlobalStateMgrTestUtil.testDbId1, label);
        Assertions.assertNotNull(state3);
        Assertions.assertEquals(TransactionStatus.ABORTED, state3.getStatus());
        Assertions.assertEquals("artificial failure", state3.getReason());
    }
}
