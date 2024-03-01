/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.indexing.kinesis.supervisor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskInfoProvider;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.common.task.RealtimeIndexTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.kinesis.KinesisDataSourceMetadata;
import org.apache.druid.indexing.kinesis.KinesisIndexTask;
import org.apache.druid.indexing.kinesis.KinesisIndexTaskClientFactory;
import org.apache.druid.indexing.kinesis.KinesisIndexTaskIOConfig;
import org.apache.druid.indexing.kinesis.KinesisIndexTaskTuningConfig;
import org.apache.druid.indexing.kinesis.KinesisRecordSupplier;
import org.apache.druid.indexing.kinesis.KinesisSequenceNumber;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskQueue;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.indexing.overlord.TaskRunnerListener;
import org.apache.druid.indexing.overlord.TaskRunnerWorkItem;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.supervisor.SupervisorReport;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManager;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManagerConfig;
import org.apache.druid.indexing.overlord.supervisor.autoscaler.SupervisorTaskAutoScaler;
import org.apache.druid.indexing.seekablestream.SeekableStreamEndSequenceNumbers;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskClient;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskRunner;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskTuningConfig;
import org.apache.druid.indexing.seekablestream.SeekableStreamStartSequenceNumbers;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.indexing.seekablestream.common.StreamPartition;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorStateManager;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorTuningConfig;
import org.apache.druid.indexing.seekablestream.supervisor.TaskReportData;
import org.apache.druid.indexing.seekablestream.supervisor.autoscaler.AutoScalerConfig;
import org.apache.druid.indexing.seekablestream.supervisor.autoscaler.LagBasedAutoScalerConfig;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.AlertBuilder;
import org.apache.druid.java.util.emitter.service.AlertEvent;
import org.apache.druid.java.util.metrics.DruidMonitorSchedulerConfig;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.metadata.EntryExistsException;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.incremental.RowIngestionMetersFactory;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.RealtimeIOConfig;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.segment.realtime.FireDepartment;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.easymock.Capture;
import org.easymock.CaptureType;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.easymock.IAnswer;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

public class KinesisSupervisorTest extends EasyMockSupport
{
  private static final ObjectMapper OBJECT_MAPPER = TestHelper.makeJsonMapper();
  private static final InputFormat INPUT_FORMAT = new JsonInputFormat(
      new JSONPathSpec(true, ImmutableList.of()),
      ImmutableMap.of(),
      false,
      false,
      false
  );
  private static final String DATASOURCE = "testDS";
  private static final long TEST_CHAT_RETRIES = 9L;
  private static final Period TEST_HTTP_TIMEOUT = new Period("PT10S");
  private static final Period TEST_SHUTDOWN_TIMEOUT = new Period("PT80S");
  private static final String STREAM = "stream";
  private static final String SHARD_ID0 = "shardId-000000000000";
  private static final String SHARD_ID1 = "shardId-000000000001";
  private static final String SHARD_ID2 = "shardId-000000000002";
  private static final StreamPartition<String> SHARD0_PARTITION = StreamPartition.of(STREAM, SHARD_ID0);
  private static final StreamPartition<String> SHARD1_PARTITION = StreamPartition.of(STREAM, SHARD_ID1);
  private static final StreamPartition<String> SHARD2_PARTITION = StreamPartition.of(STREAM, SHARD_ID2);
  private static DataSchema dataSchema;
  private KinesisRecordSupplier supervisorRecordSupplier;

  private final int numThreads;
  private TestableKinesisSupervisor supervisor;
  private KinesisSupervisorTuningConfig tuningConfig;
  private TaskStorage taskStorage;
  private TaskMaster taskMaster;
  private TaskRunner taskRunner;
  private IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator;
  private SeekableStreamIndexTaskClient<String, String> taskClient;
  private TaskQueue taskQueue;
  private RowIngestionMetersFactory rowIngestionMetersFactory;
  private StubServiceEmitter serviceEmitter;
  private SupervisorStateManagerConfig supervisorConfig;

  public KinesisSupervisorTest()
  {
    this.numThreads = 1;
  }

  @BeforeClass
  public static void setupClass()
  {
    dataSchema = getDataSchema(DATASOURCE);
  }

  @Before
  public void setupTest()
  {
    taskStorage = createMock(TaskStorage.class);
    taskMaster = createMock(TaskMaster.class);
    taskRunner = createMock(TaskRunner.class);
    indexerMetadataStorageCoordinator = createMock(IndexerMetadataStorageCoordinator.class);
    taskClient = createMock(SeekableStreamIndexTaskClient.class);
    EasyMock.expect(taskClient.resumeAsync(EasyMock.anyString())).andReturn(Futures.immediateFuture(true)).anyTimes();
    taskQueue = createMock(TaskQueue.class);
    supervisorRecordSupplier = createMock(KinesisRecordSupplier.class);

    tuningConfig = new KinesisSupervisorTuningConfig(
        null,
        1000,
        null,
        null,
        50000,
        null,
        new Period("P1Y"),
        null,
        null,
        null,
        false,
        null,
        null,
        null,
        null,
        numThreads,
        TEST_CHAT_RETRIES,
        TEST_HTTP_TIMEOUT,
        TEST_SHUTDOWN_TIMEOUT,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null
    );
    rowIngestionMetersFactory = new TestUtils().getRowIngestionMetersFactory();
    serviceEmitter = new StubServiceEmitter("KinesisSupervisorTest", "localhost");
    EmittingLogger.registerEmitter(serviceEmitter);
    supervisorConfig = new SupervisorStateManagerConfig();
  }

  @After
  public void tearDownTest()
  {
    supervisor = null;
  }

  @Test
  public void testNoInitialState() throws Exception
  {
    supervisor = getTestableSupervisor(1, 1, true, "PT1H", null, null);

    supervisorRecordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getPartitionIds(STREAM))
            .andReturn(ImmutableSet.of(SHARD_ID1, SHARD_ID0))
            .anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getAssignment())
            .andReturn(ImmutableSet.of(SHARD1_PARTITION, SHARD0_PARTITION))
            .anyTimes();
    supervisorRecordSupplier.seekToLatest(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();
    supervisorRecordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    Capture<KinesisIndexTask> captured = Capture.newInstance();
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
            null
        )
    ).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.capture(captured))).andReturn(true);
    taskRunner.registerListener(EasyMock.anyObject(TaskRunnerListener.class), EasyMock.anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    KinesisIndexTask task = captured.getValue();
    Assert.assertEquals(dataSchema, task.getDataSchema());
    Assert.assertEquals(tuningConfig.convertToTaskTuningConfig(), task.getTuningConfig());

    KinesisIndexTaskIOConfig taskConfig = task.getIOConfig();
    Assert.assertEquals("sequenceName-0", taskConfig.getBaseSequenceName());
    Assert.assertTrue("isUseTransaction", taskConfig.isUseTransaction());
    Assert.assertFalse("minimumMessageTime", taskConfig.getMinimumMessageTime().isPresent());
    Assert.assertFalse("maximumMessageTime", taskConfig.getMaximumMessageTime().isPresent());

    Assert.assertEquals(STREAM, taskConfig.getStartSequenceNumbers().getStream());
    Assert.assertEquals(
        "0",
        taskConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap().get(SHARD_ID1)
    );
    Assert.assertEquals(
        "0",
        taskConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap().get(SHARD_ID0)
    );

    Assert.assertEquals(STREAM, taskConfig.getEndSequenceNumbers().getStream());
    Assert.assertEquals(
        KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER,
        taskConfig.getEndSequenceNumbers().getPartitionSequenceNumberMap().get(SHARD_ID1)
    );
    Assert.assertEquals(
        KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER,
        taskConfig.getEndSequenceNumbers().getPartitionSequenceNumberMap().get(SHARD_ID0)
    );
  }

  @Test
  public void testNoInitialStateWithAutoScaleOut() throws Exception
  {
    HashMap<String, Object> autoScalerConfigMap = new HashMap<>();
    autoScalerConfigMap.put("enableTaskAutoScaler", true);
    autoScalerConfigMap.put("lagCollectionIntervalMillis", 500);
    autoScalerConfigMap.put("lagCollectionRangeMillis", 500);
    autoScalerConfigMap.put("scaleOutThreshold", 0);
    autoScalerConfigMap.put("triggerScaleOutFractionThreshold", 0.0);
    autoScalerConfigMap.put("scaleInThreshold", 1000000);
    autoScalerConfigMap.put("triggerScaleInFractionThreshold", 0.8);
    autoScalerConfigMap.put("scaleActionStartDelayMillis", 0);
    autoScalerConfigMap.put("scaleActionPeriodMillis", 100);
    autoScalerConfigMap.put("taskCountMax", 2);
    autoScalerConfigMap.put("taskCountMin", 1);
    autoScalerConfigMap.put("scaleInStep", 1);
    autoScalerConfigMap.put("scaleOutStep", 2);
    autoScalerConfigMap.put("minTriggerScaleActionFrequencyMillis", 1200000);

    AutoScalerConfig autoScalerConfig = OBJECT_MAPPER.convertValue(autoScalerConfigMap, AutoScalerConfig.class);
    supervisor = getTestableSupervisor(
            1,
            1,
            true,
            "PT1H",
            null,
            null,
            false,
            null,
            autoScalerConfig
            );
    KinesisSupervisorSpec kinesisSupervisorSpec = supervisor.getKinesisSupervisorSpec();
    SupervisorTaskAutoScaler autoscaler = kinesisSupervisorSpec.createAutoscaler(supervisor);

    supervisorRecordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getPartitionIds(STREAM))
            .andReturn(ImmutableSet.of(SHARD_ID1, SHARD_ID0))
            .anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getAssignment())
            .andReturn(ImmutableSet.of(SHARD1_PARTITION, SHARD0_PARTITION))
            .anyTimes();
    supervisorRecordSupplier.seekToLatest(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();
    supervisorRecordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    Capture<KinesisIndexTask> captured = Capture.newInstance();
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
            new KinesisDataSourceMetadata(
                    null
            )
    ).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.capture(captured))).andReturn(true);
    taskRunner.registerListener(EasyMock.anyObject(TaskRunnerListener.class), EasyMock.anyObject(Executor.class));
    replayAll();

    supervisor.start();
    int taskCountBeforeScale = supervisor.getIoConfig().getTaskCount();
    Assert.assertEquals(1, taskCountBeforeScale);
    autoscaler.start();

    supervisor.runInternal();
    verifyAll();
    Thread.sleep(1 * 1000);
    int taskCountAfterScale = supervisor.getIoConfig().getTaskCount();
    Assert.assertEquals(2, taskCountAfterScale);
  }

  @Test
  public void testNoInitialStateWithAutoScaleIn() throws Exception
  {
    HashMap<String, Object> autoScalerConfigMap = new HashMap<>();
    autoScalerConfigMap.put("enableTaskAutoScaler", true);
    autoScalerConfigMap.put("lagCollectionIntervalMillis", 500);
    autoScalerConfigMap.put("lagCollectionRangeMillis", 500);
    autoScalerConfigMap.put("scaleOutThreshold", 1000000);
    autoScalerConfigMap.put("triggerScaleOutFractionThreshold", 0.8);
    autoScalerConfigMap.put("scaleInThreshold", 0);
    autoScalerConfigMap.put("triggerScaleInFractionThreshold", 0.0);
    autoScalerConfigMap.put("scaleActionStartDelayMillis", 0);
    autoScalerConfigMap.put("scaleActionPeriodMillis", 100);
    autoScalerConfigMap.put("taskCountMax", 2);
    autoScalerConfigMap.put("taskCountMin", 1);
    autoScalerConfigMap.put("scaleInStep", 1);
    autoScalerConfigMap.put("scaleOutStep", 2);
    autoScalerConfigMap.put("minTriggerScaleActionFrequencyMillis", 1200000);

    AutoScalerConfig autoScalerConfig = OBJECT_MAPPER.convertValue(autoScalerConfigMap, AutoScalerConfig.class);
    supervisor = getTestableSupervisor(
            1,
            2,
            true,
            "PT1H",
            null,
            null,
            false,
            null,
            autoScalerConfig
    );

    KinesisSupervisorSpec kinesisSupervisorSpec = supervisor.getKinesisSupervisorSpec();
    SupervisorTaskAutoScaler autoscaler = kinesisSupervisorSpec.createAutoscaler(supervisor);

    supervisorRecordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getPartitionIds(STREAM))
            .andReturn(ImmutableSet.of(SHARD_ID1, SHARD_ID0))
            .anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getAssignment())
            .andReturn(ImmutableSet.of(SHARD1_PARTITION, SHARD0_PARTITION))
            .anyTimes();
    supervisorRecordSupplier.seekToLatest(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();
    supervisorRecordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    Capture<KinesisIndexTask> captured = Capture.newInstance(CaptureType.ALL);
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    EasyMock
            .expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE))
            .andReturn(new KinesisDataSourceMetadata(null))
            .anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.capture(captured))).andReturn(true).times(2);
    replayAll();

    int taskCountInit = supervisor.getIoConfig().getTaskCount();
    // when enable autoScaler the init taskCount will be equal to taskCountMin
    Assert.assertEquals(1, taskCountInit);
    supervisor.getIoConfig().setTaskCount(2);

    supervisor.start();
    int taskCountBeforeScale = supervisor.getIoConfig().getTaskCount();
    Assert.assertEquals(2, taskCountBeforeScale);
    autoscaler.start();

    supervisor.runInternal();
    verifyAll();
    Thread.sleep(1 * 1000);
    int taskCountAfterScale = supervisor.getIoConfig().getTaskCount();
    Assert.assertEquals(1, taskCountAfterScale);
  }

  @Test
  public void testRecordSupplier()
  {
    KinesisSupervisorIOConfig kinesisSupervisorIOConfig = new KinesisSupervisorIOConfig(
        STREAM,
        INPUT_FORMAT,
        "awsEndpoint",
        null,
        1,
        1,
        new Period("PT30M"),
        new Period("P1D"),
        new Period("PT30S"),
        false,
        new Period("PT30M"),
        null,
        null,
        null,
        100,
        1000,
        null,
        null,
        null,
        false
    );
    KinesisIndexTaskClientFactory clientFactory = new KinesisIndexTaskClientFactory(null, OBJECT_MAPPER);
    KinesisSupervisor supervisor = new KinesisSupervisor(
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        clientFactory,
        OBJECT_MAPPER,
        new KinesisSupervisorSpec(
            null,
            dataSchema,
            tuningConfig,
            kinesisSupervisorIOConfig,
            null,
            false,
            taskStorage,
            taskMaster,
            indexerMetadataStorageCoordinator,
            clientFactory,
            OBJECT_MAPPER,
            new NoopServiceEmitter(),
            new DruidMonitorSchedulerConfig(),
            rowIngestionMetersFactory,
            null,
            new SupervisorStateManagerConfig()
        ),
        rowIngestionMetersFactory,
        null
    );

    KinesisRecordSupplier supplier = (KinesisRecordSupplier) supervisor.setupRecordSupplier();
    Assert.assertNotNull(supplier);
    Assert.assertEquals(0, supplier.bufferSize());
    Assert.assertEquals(Collections.emptySet(), supplier.getAssignment());
    // background fetch should not be enabled for supervisor supplier
    supplier.start();
    Assert.assertFalse(supplier.isBackgroundFetchRunning());
  }

  @Test
  public void testKinesisIOConfigInitAndAutoscalerConfigCreation()
  {
    // create KinesisSupervisorIOConfig with autoScalerConfig null
    KinesisSupervisorIOConfig kinesisSupervisorIOConfigWithNullAutoScalerConfig = new KinesisSupervisorIOConfig(
        STREAM,
        INPUT_FORMAT,
        "awsEndpoint",
        null,
        1,
        1,
        new Period("PT30M"),
        new Period("P1D"),
        new Period("PT30S"),
        false,
        new Period("PT30M"),
        null,
        null,
        null,
        100,
        1000,
        null,
        null,
        null,
        false
    );

    AutoScalerConfig autoscalerConfigNull = kinesisSupervisorIOConfigWithNullAutoScalerConfig.getAutoScalerConfig();
    Assert.assertNull(autoscalerConfigNull);

    // create KinesisSupervisorIOConfig with autoScalerConfig Empty
    KinesisSupervisorIOConfig kinesisSupervisorIOConfigWithEmptyAutoScalerConfig = new KinesisSupervisorIOConfig(
        STREAM,
        INPUT_FORMAT,
        "awsEndpoint",
        null,
        1,
        1,
        new Period("PT30M"),
        new Period("P1D"),
        new Period("PT30S"),
        false,
        new Period("PT30M"),
        null,
        null,
        null,
        100,
        1000,
        null,
        null,
        OBJECT_MAPPER.convertValue(new HashMap<>(), AutoScalerConfig.class),
        false
    );

    AutoScalerConfig autoscalerConfig = kinesisSupervisorIOConfigWithEmptyAutoScalerConfig.getAutoScalerConfig();
    Assert.assertNotNull(autoscalerConfig);
    Assert.assertTrue(autoscalerConfig instanceof LagBasedAutoScalerConfig);
    Assert.assertFalse(autoscalerConfig.getEnableTaskAutoScaler());
    Assert.assertTrue(autoscalerConfig.toString().contains("autoScalerConfig"));
  }

  @Test
  public void testMultiTask() throws Exception
  {
    supervisor = getTestableSupervisor(1, 2, true, "PT1H", null, null);

    supervisorRecordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getPartitionIds(STREAM))
            .andReturn(ImmutableSet.of(SHARD_ID1, SHARD_ID0))
            .anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getAssignment())
            .andReturn(ImmutableSet.of(SHARD1_PARTITION, SHARD0_PARTITION))
            .anyTimes();
    supervisorRecordSupplier.seekToLatest(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();
    supervisorRecordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    Capture<KinesisIndexTask> captured = Capture.newInstance(CaptureType.ALL);
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    EasyMock
        .expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE))
        .andReturn(new KinesisDataSourceMetadata(null))
        .anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.capture(captured))).andReturn(true).times(2);
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    KinesisIndexTask task1 = captured.getValues().get(0);
    Assert.assertEquals(1, task1.getIOConfig().getStartSequenceNumbers().getPartitionSequenceNumberMap().size());
    Assert.assertEquals(1, task1.getIOConfig().getEndSequenceNumbers().getPartitionSequenceNumberMap().size());
    Assert.assertEquals(
        "0",
        task1.getIOConfig().getStartSequenceNumbers().getPartitionSequenceNumberMap().get(SHARD_ID1)
    );
    Assert.assertEquals(
        KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER,
        task1.getIOConfig().getEndSequenceNumbers().getPartitionSequenceNumberMap().get(SHARD_ID1)
    );

    KinesisIndexTask task2 = captured.getValues().get(1);
    Assert.assertEquals(1, task2.getIOConfig().getStartSequenceNumbers().getPartitionSequenceNumberMap().size());
    Assert.assertEquals(1, task2.getIOConfig().getEndSequenceNumbers().getPartitionSequenceNumberMap().size());
    Assert.assertEquals(
        "0",
        task2.getIOConfig().getStartSequenceNumbers().getPartitionSequenceNumberMap().get(SHARD_ID0)
    );
    Assert.assertEquals(
        KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER,
        task2.getIOConfig().getEndSequenceNumbers().getPartitionSequenceNumberMap().get(SHARD_ID0)
    );
  }

  @Test
  public void testReplicas() throws Exception
  {
    supervisor = getTestableSupervisor(2, 1, true, "PT1H", null, null);

    supervisorRecordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getPartitionIds(STREAM))
            .andReturn(ImmutableSet.of(SHARD_ID1, SHARD_ID0))
            .anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getAssignment())
            .andReturn(ImmutableSet.of(SHARD1_PARTITION, SHARD0_PARTITION))
            .anyTimes();
    supervisorRecordSupplier.seekToLatest(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();
    supervisorRecordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    Capture<KinesisIndexTask> captured = Capture.newInstance(CaptureType.ALL);
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
            null
        )
    ).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.capture(captured))).andReturn(true).times(2);
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    KinesisIndexTask task1 = captured.getValues().get(0);
    Assert.assertEquals(2, task1.getIOConfig().getStartSequenceNumbers().getPartitionSequenceNumberMap().size());
    Assert.assertEquals(2, task1.getIOConfig().getEndSequenceNumbers().getPartitionSequenceNumberMap().size());
    Assert.assertEquals(
        "0",
        task1.getIOConfig().getStartSequenceNumbers().getPartitionSequenceNumberMap().get(SHARD_ID0)
    );
    Assert.assertEquals(
        KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER,
        task1.getIOConfig().getEndSequenceNumbers().getPartitionSequenceNumberMap().get(SHARD_ID0)
    );
    Assert.assertEquals(
        "0",
        task1.getIOConfig().getStartSequenceNumbers().getPartitionSequenceNumberMap().get(SHARD_ID1)
    );
    Assert.assertEquals(
        KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER,
        task1.getIOConfig().getEndSequenceNumbers().getPartitionSequenceNumberMap().get(SHARD_ID1)
    );

    KinesisIndexTask task2 = captured.getValues().get(1);
    Assert.assertEquals(2, task2.getIOConfig().getStartSequenceNumbers().getPartitionSequenceNumberMap().size());
    Assert.assertEquals(2, task2.getIOConfig().getEndSequenceNumbers().getPartitionSequenceNumberMap().size());
    Assert.assertEquals(
        "0",
        task2.getIOConfig().getStartSequenceNumbers().getPartitionSequenceNumberMap().get(SHARD_ID0)
    );
    Assert.assertEquals(
        KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER,
        task2.getIOConfig().getEndSequenceNumbers().getPartitionSequenceNumberMap().get(SHARD_ID0)
    );
    Assert.assertEquals(
        "0",
        task2.getIOConfig().getStartSequenceNumbers().getPartitionSequenceNumberMap().get(SHARD_ID1)
    );
    Assert.assertEquals(
        KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER,
        task2.getIOConfig().getEndSequenceNumbers().getPartitionSequenceNumberMap().get(SHARD_ID1)
    );

  }

  @Test
  public void testLateMessageRejectionPeriod() throws Exception
  {
    supervisor = getTestableSupervisor(2, 1, true, "PT1H", new Period("PT1H"), null);

    supervisorRecordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getPartitionIds(STREAM))
            .andReturn(ImmutableSet.of(SHARD_ID1, SHARD_ID0))
            .anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getAssignment())
            .andReturn(ImmutableSet.of(SHARD1_PARTITION, SHARD0_PARTITION))
            .anyTimes();
    supervisorRecordSupplier.seekToLatest(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();
    supervisorRecordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    Capture<KinesisIndexTask> captured = Capture.newInstance(CaptureType.ALL);
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
            null
        )
    ).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.capture(captured))).andReturn(true).times(2);
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    KinesisIndexTask task1 = captured.getValues().get(0);
    KinesisIndexTask task2 = captured.getValues().get(1);

    Assert.assertTrue(
        "minimumMessageTime",
        task1.getIOConfig().getMinimumMessageTime().get().plusMinutes(59).isBeforeNow()
    );
    Assert.assertTrue(
        "minimumMessageTime",
        task1.getIOConfig().getMinimumMessageTime().get().plusMinutes(61).isAfterNow()
    );
    Assert.assertEquals(
        task1.getIOConfig().getMinimumMessageTime().get(),
        task2.getIOConfig().getMinimumMessageTime().get()
    );
  }

  @Test
  public void testEarlyMessageRejectionPeriod() throws Exception
  {
    supervisor = getTestableSupervisor(2, 1, true, "PT1H", null, new Period("PT1H"));

    supervisorRecordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getPartitionIds(STREAM))
            .andReturn(ImmutableSet.of(SHARD_ID1, SHARD_ID0))
            .anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getAssignment())
            .andReturn(ImmutableSet.of(SHARD1_PARTITION, SHARD0_PARTITION))
            .anyTimes();
    supervisorRecordSupplier.seekToLatest(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();
    supervisorRecordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    Capture<KinesisIndexTask> captured = Capture.newInstance(CaptureType.ALL);
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
            null
        )
    ).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.capture(captured))).andReturn(true).times(2);
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    KinesisIndexTask task1 = captured.getValues().get(0);
    KinesisIndexTask task2 = captured.getValues().get(1);

    Assert.assertTrue(
        "maximumMessageTime",
        task1.getIOConfig().getMaximumMessageTime().get().minusMinutes(59 + 60).isAfterNow()
    );
    Assert.assertTrue(
        "maximumMessageTime",
        task1.getIOConfig().getMaximumMessageTime().get().minusMinutes(61 + 60).isBeforeNow()
    );
    Assert.assertEquals(
        task1.getIOConfig().getMaximumMessageTime().get(),
        task2.getIOConfig().getMaximumMessageTime().get()
    );
  }


  /**
   * Test generating the starting sequences from the partition data stored in druid_dataSource which contains the
   * sequences of the last built segments.
   */
  @Test
  public void testDatasourceMetadata() throws Exception
  {
    supervisor = getTestableSupervisor(1, 1, true, "PT1H", null, null);

    supervisorRecordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getPartitionIds(STREAM))
            .andReturn(ImmutableSet.of(SHARD_ID1, SHARD_ID0))
            .anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getAssignment())
            .andReturn(ImmutableSet.of(SHARD1_PARTITION, SHARD0_PARTITION))
            .anyTimes();
    supervisorRecordSupplier.seekToLatest(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getEarliestSequenceNumber(SHARD1_PARTITION)).andReturn("2").anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getEarliestSequenceNumber(SHARD0_PARTITION)).andReturn("1").anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getLatestSequenceNumber(EasyMock.anyObject())).andReturn("100").anyTimes();
    supervisorRecordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.isOffsetAvailable(EasyMock.anyObject(), EasyMock.anyObject()))
            .andReturn(true)
            .anyTimes();

    Capture<KinesisIndexTask> captured = Capture.newInstance();
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
            new SeekableStreamStartSequenceNumbers<>(
                STREAM,
                ImmutableMap.of(SHARD_ID1, "2", SHARD_ID0, "1"),
                ImmutableSet.of()
            )
        )
    ).anyTimes();

    EasyMock.expect(taskQueue.add(EasyMock.capture(captured))).andReturn(true);
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    KinesisIndexTask task = captured.getValue();
    KinesisIndexTaskIOConfig taskConfig = task.getIOConfig();
    Assert.assertEquals("sequenceName-0", taskConfig.getBaseSequenceName());
    Assert.assertEquals(
        "2",
        taskConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap().get(SHARD_ID1)
    );
    Assert.assertEquals(
        "1",
        taskConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap().get(SHARD_ID0)
    );
  }

  @Test
  public void testBadMetadataOffsets() throws Exception
  {
    supervisor = getTestableSupervisor(1, 1, true, "PT1H", null, null);

    supervisorRecordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getPartitionIds(STREAM))
            .andReturn(ImmutableSet.of(SHARD_ID1, SHARD_ID0))
            .anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getAssignment())
            .andReturn(ImmutableSet.of(SHARD1_PARTITION, SHARD0_PARTITION))
            .anyTimes();
    supervisorRecordSupplier.seekToLatest(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getLatestSequenceNumber(EasyMock.anyObject())).andReturn("100").anyTimes();
    supervisorRecordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();
    // Any sequence number greater than or equal to 0 must be available
    EasyMock.expect(supervisorRecordSupplier.isOffsetAvailable(EasyMock.anyObject(),
                                                               EasyMock.eq(KinesisSequenceNumber.of("101"))))
            .andReturn(true)
            .anyTimes();
    EasyMock.expect(supervisorRecordSupplier.isOffsetAvailable(EasyMock.anyObject(),
                                                               EasyMock.eq(KinesisSequenceNumber.of("-1"))))
            .andReturn(false)
            .anyTimes();


    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
            new SeekableStreamStartSequenceNumbers<>(
                STREAM,
                ImmutableMap.of(SHARD_ID1, "101", SHARD_ID0, "-1"),
                ImmutableSet.of()
            )
        )
    ).anyTimes();
    replayAll();

    supervisor.start();
    supervisor.runInternal();

    Assert.assertEquals(
        "org.apache.druid.java.util.common.ISE",
        supervisor.getStateManager().getExceptionEvents().get(0).getExceptionClass()
    );
  }

  @Test
  public void testDontKillTasksWithMismatchedType() throws Exception
  {
    supervisor = getTestableSupervisor(2, 1, true, "PT1H", null, null);

    supervisorRecordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getPartitionIds(STREAM))
            .andReturn(ImmutableSet.of(SHARD_ID1, SHARD_ID0))
            .anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getAssignment())
            .andReturn(ImmutableSet.of(SHARD1_PARTITION, SHARD0_PARTITION))
            .anyTimes();
    supervisorRecordSupplier.seekToLatest(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getLatestSequenceNumber(EasyMock.anyObject())).andReturn("100").anyTimes();
    supervisorRecordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    // non KinesisIndexTask (don't kill)
    Task id2 = new RealtimeIndexTask(
        "id2",
        null,
        new FireDepartment(
            dataSchema,
            new RealtimeIOConfig(null, null),
            null
        ),
        null
    );

    List<Task> existingTasks = ImmutableList.of(id2);

    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(Collections.emptyList()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(existingTasks).anyTimes();
    EasyMock.expect(taskClient.getStatusAsync(EasyMock.anyString()))
            .andReturn(Futures.immediateFuture(SeekableStreamIndexTaskRunner.Status.NOT_STARTED))
            .anyTimes();
    EasyMock.expect(taskClient.getStartTimeAsync(EasyMock.anyString()))
            .andReturn(Futures.immediateFuture(DateTimes.nowUtc()))
            .anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
            null
        )
    ).anyTimes();
    taskRunner.registerListener(EasyMock.anyObject(TaskRunnerListener.class), EasyMock.anyObject(Executor.class));

    EasyMock.expect(taskQueue.add(EasyMock.anyObject(Task.class))).andReturn(true).times(2);

    replayAll();
    supervisor.start();
    supervisor.runInternal();
    verifyAll();
  }

  @Test
  public void testKillBadPartitionAssignment() throws Exception
  {
    supervisor = getTestableSupervisor(
        1,
        2,
        true,
        "PT1H",
        null,
        null
    );

    supervisorRecordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getPartitionIds(STREAM))
            .andReturn(ImmutableSet.of(SHARD_ID1, SHARD_ID0))
            .anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getAssignment())
            .andReturn(ImmutableSet.of(SHARD1_PARTITION, SHARD0_PARTITION))
            .anyTimes();
    supervisorRecordSupplier.seekToLatest(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getLatestSequenceNumber(EasyMock.anyObject())).andReturn("100").anyTimes();
    supervisorRecordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    Task id1 = createKinesisIndexTask(
        "id1",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "0"), ImmutableSet.of()),
        new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "12")),
        null,
        null
    );
    Task id2 = createKinesisIndexTask(
        "id2",
        DATASOURCE,
        1,
        new SeekableStreamStartSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID0, "0"), ImmutableSet.of(SHARD_ID0)),
        new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID0, "1")),
        null,
        null
    );
    Task id3 = createKinesisIndexTask(
        "id3",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>(
            STREAM,
            ImmutableMap.of(SHARD_ID0, "0", SHARD_ID1, "0"), ImmutableSet.of(SHARD_ID0, SHARD_ID1)
        ),
        new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID0, "1", SHARD_ID1, "12")),
        null,
        null
    );
    Task id4 = createKinesisIndexTask(
        "id4",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID0, "0"), ImmutableSet.of(SHARD_ID0)),
        new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID0, "1")),
        null,
        null
    );

    List<Task> existingTasks = ImmutableList.of(id1, id2, id3, id4);

    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(Collections.emptyList()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(existingTasks).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id1")).andReturn(Optional.of(TaskStatus.running("id1"))).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id2")).andReturn(Optional.of(TaskStatus.running("id2"))).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id3")).andReturn(Optional.of(TaskStatus.running("id3"))).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id4")).andReturn(Optional.of(TaskStatus.running("id4"))).anyTimes();
    EasyMock.expect(taskStorage.getTask("id1")).andReturn(Optional.of(id1)).anyTimes();
    EasyMock.expect(taskStorage.getTask("id2")).andReturn(Optional.of(id2)).anyTimes();
    EasyMock.expect(taskStorage.getTask("id3")).andReturn(Optional.of(id3)).anyTimes();
    EasyMock.expect(taskStorage.getTask("id4")).andReturn(Optional.of(id4)).anyTimes();
    EasyMock.expect(taskClient.getStatusAsync(EasyMock.anyString()))
            .andReturn(Futures.immediateFuture(SeekableStreamIndexTaskRunner.Status.NOT_STARTED))
            .anyTimes();
    EasyMock.expect(taskClient.getStartTimeAsync(EasyMock.anyString()))
            .andReturn(Futures.immediateFuture(DateTimes.nowUtc()))
            .anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
            null
        )
    ).anyTimes();
    EasyMock.expect(taskClient.stopAsync("id3", false)).andReturn(Futures.immediateFuture(true));

    TreeMap<Integer, Map<String, String>> checkpoints1 = new TreeMap<>();
    checkpoints1.put(0, ImmutableMap.of(SHARD_ID1, "0"));
    TreeMap<Integer, Map<String, String>> checkpoints2 = new TreeMap<>();
    checkpoints2.put(0, ImmutableMap.of(SHARD_ID0, "0"));
    TreeMap<Integer, Map<String, String>> checkpoints4 = new TreeMap<>();
    checkpoints4.put(0, ImmutableMap.of(SHARD_ID0, "0"));
    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("id1"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpoints1))
            .times(1);
    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("id2"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpoints2))
            .times(1);
    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("id4"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpoints4))
            .times(1);


    taskRunner.registerListener(EasyMock.anyObject(TaskRunnerListener.class), EasyMock.anyObject(Executor.class));
    taskQueue.shutdown("id4", "Task[%s] failed to return status, killing task", "id4");
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();
  }

  @Test
  public void testRequeueTaskWhenFailed() throws Exception
  {
    supervisor = getTestableSupervisor(2, 2, true, "PT1H", null, null);
    supervisorRecordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getPartitionIds(STREAM))
            .andReturn(ImmutableSet.of(SHARD_ID1, SHARD_ID0))
            .anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getAssignment())
            .andReturn(ImmutableSet.of(SHARD1_PARTITION, SHARD0_PARTITION))
            .anyTimes();
    supervisorRecordSupplier.seekToLatest(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getLatestSequenceNumber(EasyMock.anyObject())).andReturn("100").anyTimes();
    supervisorRecordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    Capture<Task> captured = Capture.newInstance(CaptureType.ALL);
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(Collections.emptyList()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(taskClient.getStatusAsync(EasyMock.anyString()))
            .andReturn(Futures.immediateFuture(SeekableStreamIndexTaskRunner.Status.NOT_STARTED))
            .anyTimes();
    EasyMock.expect(taskClient.getStartTimeAsync(EasyMock.anyString()))
            .andReturn(Futures.immediateFuture(DateTimes.nowUtc()))
            .anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
            null
        )
    ).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.capture(captured))).andReturn(true).times(4);

    TreeMap<Integer, Map<String, String>> checkpoints1 = new TreeMap<>();
    checkpoints1.put(
        0,
        ImmutableMap.of(
            SHARD_ID1,
            "0"
        )
    );
    TreeMap<Integer, Map<String, String>> checkpoints2 = new TreeMap<>();
    checkpoints2.put(0, ImmutableMap.of(
        SHARD_ID0,
        "0"
    ));
    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("sequenceName-0"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpoints1))
            .anyTimes();
    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("sequenceName-1"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpoints2))
            .anyTimes();

    taskRunner.registerListener(EasyMock.anyObject(TaskRunnerListener.class), EasyMock.anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    List<Task> tasks = captured.getValues();

    // test that running the main loop again checks the status of the tasks that were created and does nothing if they
    // are all still running
    EasyMock.reset(taskStorage);
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(tasks).anyTimes();
    for (Task task : tasks) {
      EasyMock.expect(taskStorage.getStatus(task.getId()))
              .andReturn(Optional.of(TaskStatus.running(task.getId())))
              .anyTimes();
      EasyMock.expect(taskStorage.getTask(task.getId())).andReturn(Optional.of(task)).anyTimes();
    }
    EasyMock.replay(taskStorage);

    supervisor.runInternal();
    verifyAll();

    // test that a task failing causes a new task to be re-queued with the same parameters
    Capture<Task> aNewTaskCapture = Capture.newInstance();
    List<Task> imStillAlive = tasks.subList(0, 3);
    KinesisIndexTask iHaveFailed = (KinesisIndexTask) tasks.get(3);
    EasyMock.reset(taskStorage);
    EasyMock.reset(taskQueue);
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(imStillAlive).anyTimes();
    for (Task task : imStillAlive) {
      EasyMock.expect(taskStorage.getStatus(task.getId()))
              .andReturn(Optional.of(TaskStatus.running(task.getId())))
              .anyTimes();
      EasyMock.expect(taskStorage.getTask(task.getId())).andReturn(Optional.of(task)).anyTimes();
    }
    EasyMock.expect(taskStorage.getStatus(iHaveFailed.getId()))
            .andReturn(Optional.of(TaskStatus.failure(iHaveFailed.getId(), "Dummy task status failure err message")));
    EasyMock.expect(taskStorage.getTask(iHaveFailed.getId())).andReturn(Optional.of(iHaveFailed)).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.capture(aNewTaskCapture))).andReturn(true);
    EasyMock.replay(taskStorage);
    EasyMock.replay(taskQueue);

    supervisor.runInternal();
    verifyAll();

    Assert.assertNotEquals(iHaveFailed.getId(), aNewTaskCapture.getValue().getId());
    Assert.assertEquals(
        iHaveFailed.getIOConfig().getBaseSequenceName(),
        ((KinesisIndexTask) aNewTaskCapture.getValue()).getIOConfig().getBaseSequenceName()
    );
  }

  @Test
  public void testRequeueAdoptedTaskWhenFailed() throws Exception
  {
    supervisor = getTestableSupervisor(2, 1, true, "PT1H", null, null);
    supervisorRecordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getPartitionIds(STREAM))
            .andReturn(ImmutableSet.of(SHARD_ID1, SHARD_ID0))
            .anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getAssignment())
            .andReturn(ImmutableSet.of(SHARD1_PARTITION, SHARD0_PARTITION))
            .anyTimes();
    supervisorRecordSupplier.seekToLatest(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getLatestSequenceNumber(EasyMock.anyObject())).andReturn("100").anyTimes();
    supervisorRecordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    DateTime now = DateTimes.nowUtc();
    DateTime maxi = now.plusMinutes(60);
    Task id1 = createKinesisIndexTask(
        "id1",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>(
            "stream",
            ImmutableMap.of(SHARD_ID1, "0", SHARD_ID0, "0"),
            ImmutableSet.of()
        ),
        new SeekableStreamEndSequenceNumbers<>(
            "stream",
            ImmutableMap.of(
                SHARD_ID1,
                KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER,
                SHARD_ID0,
                KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER
            )
        ),
        now,
        maxi
    );

    List<Task> existingTasks = ImmutableList.of(id1);

    Capture<Task> captured = Capture.newInstance();
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(Collections.emptyList()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(existingTasks).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id1")).andReturn(Optional.of(TaskStatus.running("id1"))).anyTimes();
    EasyMock.expect(taskStorage.getTask("id1")).andReturn(Optional.of(id1)).anyTimes();
    EasyMock.expect(taskClient.getStatusAsync("id1"))
            .andReturn(Futures.immediateFuture(SeekableStreamIndexTaskRunner.Status.READING));
    EasyMock.expect(taskClient.getStartTimeAsync("id1")).andReturn(Futures.immediateFuture(now)).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.capture(captured))).andReturn(true);
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
            null
        )
    ).anyTimes();

    TreeMap<Integer, Map<String, String>> checkpoints = new TreeMap<>();
    checkpoints.put(0, ImmutableMap.of(
        SHARD_ID1,
        "0",
        SHARD_ID0,
        "0"
    ));
    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("id1"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpoints))
            .times(2);

    taskRunner.registerListener(EasyMock.anyObject(TaskRunnerListener.class), EasyMock.anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    // check that replica tasks are created with the same minimumMessageTime as tasks inherited from another supervisor
    Assert.assertEquals(now, ((KinesisIndexTask) captured.getValue()).getIOConfig().getMinimumMessageTime().get());

    // test that a task failing causes a new task to be re-queued with the same parameters
    String runningTaskId = captured.getValue().getId();
    Capture<Task> aNewTaskCapture = Capture.newInstance();
    KinesisIndexTask iHaveFailed = (KinesisIndexTask) existingTasks.get(0);
    EasyMock.reset(taskStorage);
    EasyMock.reset(taskQueue);
    EasyMock.reset(taskClient);

    // for the newly created replica task
    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("sequenceName-0"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpoints))
            .times(2);
    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("id1"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpoints))
            .times(1);


    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of(captured.getValue())).anyTimes();
    EasyMock.expect(taskStorage.getStatus(iHaveFailed.getId()))
            .andReturn(Optional.of(TaskStatus.failure(iHaveFailed.getId(), "Dummy task status failure err message")));
    EasyMock.expect(taskStorage.getStatus(runningTaskId))
            .andReturn(Optional.of(TaskStatus.running(runningTaskId)))
            .anyTimes();
    EasyMock.expect(taskStorage.getTask(iHaveFailed.getId())).andReturn(Optional.of(iHaveFailed)).anyTimes();
    EasyMock.expect(taskStorage.getTask(runningTaskId)).andReturn(Optional.of(captured.getValue())).anyTimes();
    EasyMock.expect(taskClient.getStatusAsync(runningTaskId))
            .andReturn(Futures.immediateFuture(SeekableStreamIndexTaskRunner.Status.READING));
    EasyMock.expect(taskClient.getStartTimeAsync(runningTaskId)).andReturn(Futures.immediateFuture(now)).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.capture(aNewTaskCapture))).andReturn(true);
    EasyMock.replay(taskStorage);
    EasyMock.replay(taskQueue);
    EasyMock.replay(taskClient);

    supervisor.runInternal();
    verifyAll();

    Assert.assertNotEquals(iHaveFailed.getId(), aNewTaskCapture.getValue().getId());
    Assert.assertEquals(
        iHaveFailed.getIOConfig().getBaseSequenceName(),
        ((KinesisIndexTask) aNewTaskCapture.getValue()).getIOConfig().getBaseSequenceName()
    );

    // check that failed tasks are recreated with the same minimumMessageTime as the task it replaced, even if that
    // task came from another supervisor
    Assert.assertEquals(
        now,
        ((KinesisIndexTask) aNewTaskCapture.getValue()).getIOConfig().getMinimumMessageTime().get()
    );
    Assert.assertEquals(
        maxi,
        ((KinesisIndexTask) aNewTaskCapture.getValue()).getIOConfig().getMaximumMessageTime().get()
    );
  }

  @Test
  public void testQueueNextTasksOnSuccess() throws Exception
  {
    supervisor = getTestableSupervisor(2, 2, true, "PT1H", null, null);
    supervisorRecordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getPartitionIds(STREAM))
            .andReturn(ImmutableSet.of(SHARD_ID1, SHARD_ID0))
            .anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getAssignment())
            .andReturn(ImmutableSet.of(SHARD1_PARTITION, SHARD0_PARTITION))
            .anyTimes();
    supervisorRecordSupplier.seekToLatest(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getLatestSequenceNumber(EasyMock.anyObject())).andReturn("100").anyTimes();
    supervisorRecordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    Capture<Task> captured = Capture.newInstance(CaptureType.ALL);
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(Collections.emptyList()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(taskClient.getStatusAsync(EasyMock.anyString()))
            .andReturn(Futures.immediateFuture(SeekableStreamIndexTaskRunner.Status.NOT_STARTED))
            .anyTimes();
    EasyMock.expect(taskClient.getStartTimeAsync(EasyMock.anyString()))
            .andReturn(Futures.immediateFuture(DateTimes.nowUtc()))
            .anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
            null
        )
    ).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.capture(captured))).andReturn(true).times(4);
    taskRunner.registerListener(EasyMock.anyObject(TaskRunnerListener.class), EasyMock.anyObject(Executor.class));

    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    List<Task> tasks = captured.getValues();

    EasyMock.reset(taskStorage);
    EasyMock.reset(taskClient);

    EasyMock.expect(taskClient.getStatusAsync(EasyMock.anyString()))
            .andReturn(Futures.immediateFuture(SeekableStreamIndexTaskRunner.Status.NOT_STARTED))
            .anyTimes();
    EasyMock.expect(taskClient.getStartTimeAsync(EasyMock.anyString()))
            .andReturn(Futures.immediateFuture(DateTimes.nowUtc()))
            .anyTimes();
    TreeMap<Integer, Map<String, String>> checkpoints1 = new TreeMap<>();
    checkpoints1.put(0, ImmutableMap.of(
        SHARD_ID1,
        "0",
        SHARD_ID0,
        "0"
    ));
    TreeMap<Integer, Map<String, String>> checkpoints2 = new TreeMap<>();
    checkpoints2.put(0, ImmutableMap.of(
        SHARD_ID1,
        "0"
    ));
    // there would be 4 tasks, 2 for each task group
    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("sequenceName-0"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpoints1))
            .times(2);
    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("sequenceName-1"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpoints2))
            .times(2);


    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(tasks).anyTimes();
    for (Task task : tasks) {
      EasyMock.expect(taskStorage.getStatus(task.getId()))
              .andReturn(Optional.of(TaskStatus.running(task.getId())))
              .anyTimes();
      EasyMock.expect(taskStorage.getTask(task.getId())).andReturn(Optional.of(task)).anyTimes();
    }
    EasyMock.replay(taskStorage);
    EasyMock.replay(taskClient);

    supervisor.runInternal();
    verifyAll();

    // test that a task succeeding causes a new task to be re-queued with the next stream range and causes any replica
    // tasks to be shutdown
    Capture<Task> newTasksCapture = Capture.newInstance(CaptureType.ALL);
    Capture<String> shutdownTaskIdCapture = Capture.newInstance();
    List<Task> imStillRunning = tasks.subList(1, 4);
    KinesisIndexTask iAmSuccess = (KinesisIndexTask) tasks.get(0);
    EasyMock.reset(taskStorage);
    EasyMock.reset(taskQueue);
    EasyMock.reset(taskClient);
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(imStillRunning).anyTimes();
    for (Task task : imStillRunning) {
      EasyMock.expect(taskStorage.getStatus(task.getId()))
              .andReturn(Optional.of(TaskStatus.running(task.getId())))
              .anyTimes();
      EasyMock.expect(taskStorage.getTask(task.getId())).andReturn(Optional.of(task)).anyTimes();
    }
    EasyMock.expect(taskStorage.getStatus(iAmSuccess.getId()))
            .andReturn(Optional.of(TaskStatus.success(iAmSuccess.getId())));
    EasyMock.expect(taskStorage.getTask(iAmSuccess.getId())).andReturn(Optional.of(iAmSuccess)).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.capture(newTasksCapture))).andReturn(true).times(2);
    EasyMock.expect(taskClient.stopAsync(EasyMock.capture(shutdownTaskIdCapture), EasyMock.eq(false)))
            .andReturn(Futures.immediateFuture(true));
    EasyMock.replay(taskStorage);
    EasyMock.replay(taskQueue);
    EasyMock.replay(taskClient);

    supervisor.runInternal();
    verifyAll();

    // make sure we killed the right task (sequenceName for replicas are the same)
    Assert.assertTrue(shutdownTaskIdCapture.getValue().contains(iAmSuccess.getIOConfig().getBaseSequenceName()));
  }

  @Test
  public void testBeginPublishAndQueueNextTasks() throws Exception
  {
    final TaskLocation location = TaskLocation.create("testHost", 1234, -1);

    supervisor = getTestableSupervisor(2, 2, true, "PT1M", null, null);

    supervisorRecordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getPartitionIds(STREAM))
            .andReturn(ImmutableSet.of(SHARD_ID1, SHARD_ID0))
            .anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getAssignment())
            .andReturn(ImmutableSet.of(SHARD1_PARTITION, SHARD0_PARTITION))
            .anyTimes();
    supervisorRecordSupplier.seekToLatest(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getLatestSequenceNumber(SHARD1_PARTITION)).andReturn("12").anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getLatestSequenceNumber(SHARD0_PARTITION)).andReturn("1").anyTimes();
    supervisorRecordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    final Capture<Task> firstTasks = Capture.newInstance(CaptureType.ALL);
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(Collections.emptyList()).anyTimes();
    EasyMock.expect(taskRunner.getTaskLocation(EasyMock.anyString())).andReturn(TaskLocation.unknown()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(null)
    ).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.capture(firstTasks))).andReturn(true).times(4);
    taskRunner.registerListener(EasyMock.anyObject(TaskRunnerListener.class), EasyMock.anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    final List<Task> tasks = firstTasks.getValues();
    Collection workItems = new ArrayList<>();
    for (Task task : tasks) {
      workItems.add(new TestTaskRunnerWorkItem(task, null, location));
    }

    EasyMock.reset(taskStorage, taskRunner, taskClient, taskQueue);
    final Capture<Task> secondTasks = Capture.newInstance(CaptureType.ALL);
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(tasks).anyTimes();
    for (Task task : tasks) {
      EasyMock.expect(taskStorage.getStatus(task.getId()))
              .andReturn(Optional.of(TaskStatus.running(task.getId())))
              .anyTimes();
      EasyMock.expect(taskStorage.getTask(task.getId())).andReturn(Optional.of(task)).anyTimes();
    }
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(workItems).anyTimes();
    EasyMock.expect(taskRunner.getTaskLocation(EasyMock.anyString())).andReturn(location).anyTimes();
    EasyMock.expect(taskClient.getStatusAsync(EasyMock.anyString()))
            .andReturn(Futures.immediateFuture(SeekableStreamIndexTaskRunner.Status.READING))
            .anyTimes();
    EasyMock.expect(taskClient.getStartTimeAsync(EasyMock.contains("sequenceName-0")))
            .andReturn(Futures.immediateFuture(DateTimes.nowUtc().minusMinutes(2)))
            .andReturn(Futures.immediateFuture(DateTimes.nowUtc()));
    EasyMock.expect(taskClient.getStartTimeAsync(EasyMock.contains("sequenceName-1")))
            .andReturn(Futures.immediateFuture(DateTimes.nowUtc()))
            .times(2);
    EasyMock.expect(taskClient.pauseAsync(EasyMock.contains("sequenceName-0")))
            .andReturn(Futures.immediateFuture(ImmutableMap.of(
                SHARD_ID1,
                "1"
            )))
            .andReturn(Futures.immediateFuture(ImmutableMap.of(
                SHARD_ID1,
                "3"
            )));
    EasyMock.expect(
        taskClient.setEndOffsetsAsync(
            EasyMock.contains("sequenceName-0"),
            EasyMock.eq(ImmutableMap.of(
                SHARD_ID1,
                "3"
            )),
            EasyMock.eq(true)
        )
    ).andReturn(Futures.immediateFuture(true)).times(2);
    EasyMock.expect(taskQueue.add(EasyMock.capture(secondTasks))).andReturn(true).times(2);

    TreeMap<Integer, Map<String, String>> checkpoints1 = new TreeMap<>();
    checkpoints1.put(0, ImmutableMap.of(
        SHARD_ID1,
        "0"
    ));
    TreeMap<Integer, Map<String, String>> checkpoints2 = new TreeMap<>();
    checkpoints2.put(0, ImmutableMap.of(
        SHARD_ID0,
        "0"
    ));
    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("sequenceName-0"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpoints1))
            .times(2);
    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("sequenceName-1"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpoints2))
            .times(2);

    EasyMock.replay(taskStorage, taskRunner, taskClient, taskQueue);

    supervisor.runInternal();
    verifyAll();

    for (Task task : secondTasks.getValues()) {
      KinesisIndexTask kinesisIndexTask = (KinesisIndexTask) task;
      Assert.assertEquals(dataSchema, kinesisIndexTask.getDataSchema());
      Assert.assertEquals(tuningConfig.convertToTaskTuningConfig(), kinesisIndexTask.getTuningConfig());

      KinesisIndexTaskIOConfig taskConfig = kinesisIndexTask.getIOConfig();
      Assert.assertEquals("sequenceName-0", taskConfig.getBaseSequenceName());
      Assert.assertTrue("isUseTransaction", taskConfig.isUseTransaction());

      Assert.assertEquals(STREAM, taskConfig.getStartSequenceNumbers().getStream());
      Assert.assertEquals(
          "3",
          taskConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap().get(SHARD_ID1)
      );
      // start sequenceNumbers should be exclusive for the second batch of tasks
      Assert.assertEquals(
          ImmutableSet.of(SHARD_ID1),
          ((KinesisIndexTask) task).getIOConfig().getStartSequenceNumbers().getExclusivePartitions()
      );
    }
  }

  @Test
  public void testDiscoverExistingPublishingTask() throws Exception
  {
    final TaskLocation location = TaskLocation.create("testHost", 1234, -1);
    final Map<String, Long> timeLag = ImmutableMap.of(SHARD_ID1, 0L, SHARD_ID0, 20000000L);

    supervisor = getTestableSupervisor(1, 1, true, "PT1H", null, null);

    supervisorRecordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getPartitionIds(STREAM))
            .andReturn(ImmutableSet.of(SHARD_ID1, SHARD_ID0))
            .anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getAssignment())
            .andReturn(ImmutableSet.of(SHARD1_PARTITION, SHARD0_PARTITION))
            .anyTimes();
    supervisorRecordSupplier.seekToLatest(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getLatestSequenceNumber(SHARD1_PARTITION)).andReturn("12").anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getLatestSequenceNumber(SHARD0_PARTITION)).andReturn("1").anyTimes();
    supervisorRecordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getPartitionsTimeLag(EasyMock.anyString(), EasyMock.anyObject()))
            .andReturn(timeLag)
            .atLeastOnce();

    Task task = createKinesisIndexTask(
        "id1",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>(
            "stream",
            ImmutableMap.of(SHARD_ID1, "0", SHARD_ID0, "0"),
            ImmutableSet.of()
        ),
        new SeekableStreamEndSequenceNumbers<>(
            "stream",
            ImmutableMap.of(
                SHARD_ID1,
                KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER,
                SHARD_ID0,
                KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER
            )
        ),
        null,
        null
    );

    Collection workItems = new ArrayList<>();
    workItems.add(new TestTaskRunnerWorkItem(task, null, location));

    Capture<KinesisIndexTask> captured = Capture.newInstance();
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(workItems).anyTimes();
    EasyMock.expect(taskRunner.getTaskLocation(EasyMock.anyString())).andReturn(location).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of(task)).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id1")).andReturn(Optional.of(TaskStatus.running("id1"))).anyTimes();
    EasyMock.expect(taskStorage.getTask("id1")).andReturn(Optional.of(task)).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
            null
        )
    ).anyTimes();
    EasyMock.expect(taskClient.getStatusAsync("id1"))
            .andReturn(Futures.immediateFuture(SeekableStreamIndexTaskRunner.Status.PUBLISHING));
    EasyMock.expect(taskClient.getCurrentOffsetsAsync("id1", false))
            .andReturn(Futures.immediateFuture(ImmutableMap.of(
                SHARD_ID1,
                "2",
                SHARD_ID0,
                "1"
            )));
    EasyMock.expect(taskClient.getEndOffsetsAsync("id1")).andReturn(Futures.immediateFuture(ImmutableMap.of(
        SHARD_ID1,
        "2",
        SHARD_ID0,
        "1"
    )));
    EasyMock.expect(taskQueue.add(EasyMock.capture(captured))).andReturn(true);

    TreeMap<Integer, Map<String, String>> checkpoints = new TreeMap<>();
    checkpoints.put(0, ImmutableMap.of(
        SHARD_ID1,
        "0",
        SHARD_ID0,
        "0"
    ));
    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.anyString(), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpoints))
            .anyTimes();

    taskRunner.registerListener(EasyMock.anyObject(TaskRunnerListener.class), EasyMock.anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    supervisor.updateCurrentAndLatestOffsets();
    SupervisorReport<KinesisSupervisorReportPayload> report = supervisor.getStatus();
    verifyAll();

    Assert.assertEquals(DATASOURCE, report.getId());

    KinesisSupervisorReportPayload payload = report.getPayload();

    Assert.assertEquals(DATASOURCE, payload.getDataSource());
    Assert.assertEquals(3600L, payload.getDurationSeconds());
    Assert.assertEquals(2, payload.getPartitions());
    Assert.assertEquals(1, payload.getReplicas());
    Assert.assertEquals(STREAM, payload.getStream());
    Assert.assertEquals(0, payload.getActiveTasks().size());
    Assert.assertEquals(1, payload.getPublishingTasks().size());
    Assert.assertEquals(SupervisorStateManager.BasicState.RUNNING, payload.getDetailedState());
    Assert.assertEquals(0, payload.getRecentErrors().size());

    TaskReportData publishingReport = payload.getPublishingTasks().get(0);

    Assert.assertEquals("id1", publishingReport.getId());
    Assert.assertEquals(ImmutableMap.of(
        SHARD_ID1,
        "0",
        SHARD_ID0,
        "0"
    ), publishingReport.getStartingOffsets());
    Assert.assertEquals(ImmutableMap.of(
        SHARD_ID1,
        "2",
        SHARD_ID0,
        "1"
    ), publishingReport.getCurrentOffsets());

    KinesisIndexTask capturedTask = captured.getValue();
    Assert.assertEquals(dataSchema, capturedTask.getDataSchema());
    Assert.assertEquals(tuningConfig.convertToTaskTuningConfig(), capturedTask.getTuningConfig());

    KinesisIndexTaskIOConfig capturedTaskConfig = capturedTask.getIOConfig();
    Assert.assertEquals("awsEndpoint", capturedTaskConfig.getEndpoint());
    Assert.assertEquals("sequenceName-0", capturedTaskConfig.getBaseSequenceName());
    Assert.assertTrue("isUseTransaction", capturedTaskConfig.isUseTransaction());

    // check that the new task was created with starting sequences matching where the publishing task finished
    Assert.assertEquals(STREAM, capturedTaskConfig.getStartSequenceNumbers().getStream());
    Assert.assertEquals(
        "2",
        capturedTaskConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap().get(SHARD_ID1)
    );
    Assert.assertEquals(
        "1",
        capturedTaskConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap().get(SHARD_ID0)
    );

    Assert.assertEquals(STREAM, capturedTaskConfig.getEndSequenceNumbers().getStream());
    Assert.assertEquals(
        KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER,
        capturedTaskConfig.getEndSequenceNumbers().getPartitionSequenceNumberMap().get(SHARD_ID1)
    );
    Assert.assertEquals(
        KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER,
        capturedTaskConfig.getEndSequenceNumbers().getPartitionSequenceNumberMap().get(SHARD_ID0)
    );
  }

  @Test
  public void testDiscoverExistingPublishingTaskWithDifferentPartitionAllocation() throws Exception
  {
    final TaskLocation location = TaskLocation.create("testHost", 1234, -1);
    final Map<String, Long> timeLag = ImmutableMap.of(SHARD_ID1, 9000L, SHARD_ID0, 1234L);

    supervisor = getTestableSupervisor(1, 1, true, "PT1H", null, null);
    supervisorRecordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getPartitionIds(STREAM))
            .andReturn(ImmutableSet.of(SHARD_ID1, SHARD_ID0))
            .anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getAssignment())
            .andReturn(ImmutableSet.of(SHARD1_PARTITION, SHARD0_PARTITION))
            .anyTimes();
    supervisorRecordSupplier.seekToLatest(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getLatestSequenceNumber(SHARD1_PARTITION)).andReturn("12").anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getLatestSequenceNumber(SHARD0_PARTITION)).andReturn("1").anyTimes();
    supervisorRecordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getPartitionsTimeLag(EasyMock.anyString(), EasyMock.anyObject()))
            .andReturn(timeLag)
            .atLeastOnce();

    Task task = createKinesisIndexTask(
        "id1",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>(
            "stream",
            ImmutableMap.of(SHARD_ID1, "0", SHARD_ID0, "0"),
            ImmutableSet.of()
        ),
        new SeekableStreamEndSequenceNumbers<>(
            "stream",
            ImmutableMap.of(
                SHARD_ID1,
                KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER,
                SHARD_ID0,
                KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER
            )
        ),
        null,
        null
    );

    Collection workItems = new ArrayList<>();
    workItems.add(new TestTaskRunnerWorkItem(task, null, location));

    Capture<KinesisIndexTask> captured = Capture.newInstance();
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(workItems).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of(task)).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id1")).andReturn(Optional.of(TaskStatus.running("id1"))).anyTimes();
    EasyMock.expect(taskStorage.getTask("id1")).andReturn(Optional.of(task)).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
            null
        )
    ).anyTimes();
    EasyMock.expect(taskClient.getStatusAsync("id1"))
            .andReturn(Futures.immediateFuture(SeekableStreamIndexTaskRunner.Status.PUBLISHING));
    EasyMock.expect(taskClient.getCurrentOffsetsAsync("id1", false))
            .andReturn(Futures.immediateFuture(ImmutableMap.of(
                SHARD_ID1,
                "2",
                SHARD_ID0,
                "1"
            )));
    EasyMock.expect(taskClient.getEndOffsetsAsync("id1")).andReturn(Futures.immediateFuture(ImmutableMap.of(
        SHARD_ID1,
        "2",
        SHARD_ID0,
        "1"
    )));
    EasyMock.expect(taskQueue.add(EasyMock.capture(captured))).andReturn(true);

    taskRunner.registerListener(EasyMock.anyObject(TaskRunnerListener.class), EasyMock.anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    supervisor.updateCurrentAndLatestOffsets();
    SupervisorReport<KinesisSupervisorReportPayload> report = supervisor.getStatus();
    verifyAll();

    Assert.assertEquals(DATASOURCE, report.getId());

    KinesisSupervisorReportPayload payload = report.getPayload();

    Assert.assertEquals(DATASOURCE, payload.getDataSource());
    Assert.assertEquals(3600L, payload.getDurationSeconds());
    Assert.assertEquals(2, payload.getPartitions());
    Assert.assertEquals(1, payload.getReplicas());
    Assert.assertEquals(STREAM, payload.getStream());
    Assert.assertEquals(0, payload.getActiveTasks().size());
    Assert.assertEquals(1, payload.getPublishingTasks().size());
    Assert.assertEquals(SupervisorStateManager.BasicState.RUNNING, payload.getDetailedState());
    Assert.assertEquals(0, payload.getRecentErrors().size());

    TaskReportData publishingReport = payload.getPublishingTasks().get(0);

    Assert.assertEquals("id1", publishingReport.getId());
    Assert.assertEquals(ImmutableMap.of(
        SHARD_ID1,
        "0",
        SHARD_ID0,
        "0"
    ), publishingReport.getStartingOffsets());
    Assert.assertEquals(ImmutableMap.of(
        SHARD_ID1,
        "2",
        SHARD_ID0,
        "1"
    ), publishingReport.getCurrentOffsets());

    KinesisIndexTask capturedTask = captured.getValue();
    Assert.assertEquals(dataSchema, capturedTask.getDataSchema());
    Assert.assertEquals(tuningConfig.convertToTaskTuningConfig(), capturedTask.getTuningConfig());

    KinesisIndexTaskIOConfig capturedTaskConfig = capturedTask.getIOConfig();
    Assert.assertEquals("awsEndpoint", capturedTaskConfig.getEndpoint());
    Assert.assertEquals("sequenceName-0", capturedTaskConfig.getBaseSequenceName());
    Assert.assertTrue("isUseTransaction", capturedTaskConfig.isUseTransaction());

    // check that the new task was created with starting sequences matching where the publishing task finished
    Assert.assertEquals(STREAM, capturedTaskConfig.getStartSequenceNumbers().getStream());
    Assert.assertEquals(
        "2",
        capturedTaskConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap().get(SHARD_ID1)
    );
    Assert.assertEquals(
        "1",
        capturedTaskConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap().get(SHARD_ID0)
    );

    Assert.assertEquals(STREAM, capturedTaskConfig.getEndSequenceNumbers().getStream());
    Assert.assertEquals(
        KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER,
        capturedTaskConfig.getEndSequenceNumbers().getPartitionSequenceNumberMap().get(SHARD_ID1)
    );
    Assert.assertEquals(
        KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER,
        capturedTaskConfig.getEndSequenceNumbers().getPartitionSequenceNumberMap().get(SHARD_ID0)
    );
  }

  @Test
  public void testDiscoverExistingPublishingAndReadingTask() throws Exception
  {
    final TaskLocation location1 = TaskLocation.create("testHost", 1234, -1);
    final TaskLocation location2 = TaskLocation.create("testHost2", 145, -1);
    final DateTime startTime = DateTimes.nowUtc();
    final Map<String, Long> timeLag = ImmutableMap.of(SHARD_ID0, 100L, SHARD_ID1, 200L);

    supervisor = getTestableSupervisor(1, 1, true, "PT1H", null, null);

    supervisorRecordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getPartitionIds(STREAM))
            .andReturn(ImmutableSet.of(SHARD_ID1, SHARD_ID0))
            .anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getAssignment())
            .andReturn(ImmutableSet.of(SHARD1_PARTITION, SHARD0_PARTITION))
            .anyTimes();
    supervisorRecordSupplier.seekToLatest(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getLatestSequenceNumber(SHARD1_PARTITION)).andReturn("12").anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getLatestSequenceNumber(SHARD0_PARTITION)).andReturn("1").anyTimes();
    supervisorRecordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getPartitionsTimeLag(EasyMock.anyString(), EasyMock.anyObject()))
            .andReturn(timeLag)
            .atLeastOnce();
    Task id1 = createKinesisIndexTask(
        "id1",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>(
            "stream",
            ImmutableMap.of(SHARD_ID1, "0", SHARD_ID0, "0"),
            ImmutableSet.of()
        ),
        new SeekableStreamEndSequenceNumbers<>(
            "stream",
            ImmutableMap.of(
                SHARD_ID1,
                KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER,
                SHARD_ID0,
                KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER
            )
        ),
        null,
        null
    );

    Task id2 = createKinesisIndexTask(
        "id2",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>(
            "stream",
            ImmutableMap.of(SHARD_ID1, "2", SHARD_ID0, "1"), ImmutableSet.of(SHARD_ID0, SHARD_ID1)
        ),
        new SeekableStreamEndSequenceNumbers<>(
            "stream",
            ImmutableMap.of(
                SHARD_ID1,
                KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER,
                SHARD_ID0,
                KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER
            )
        ),
        null,
        null
    );

    Collection workItems = new ArrayList<>();
    workItems.add(new TestTaskRunnerWorkItem(id1, null, location1));
    workItems.add(new TestTaskRunnerWorkItem(id2, null, location2));

    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(workItems).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of(id1, id2)).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id1")).andReturn(Optional.of(TaskStatus.running("id1"))).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id2")).andReturn(Optional.of(TaskStatus.running("id2"))).anyTimes();
    EasyMock.expect(taskStorage.getTask("id1")).andReturn(Optional.of(id1)).anyTimes();
    EasyMock.expect(taskStorage.getTask("id2")).andReturn(Optional.of(id2)).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
            null
        )
    ).anyTimes();
    EasyMock.expect(taskClient.getStatusAsync("id1"))
            .andReturn(Futures.immediateFuture(SeekableStreamIndexTaskRunner.Status.PUBLISHING));
    EasyMock.expect(taskClient.getStatusAsync("id2"))
            .andReturn(Futures.immediateFuture(SeekableStreamIndexTaskRunner.Status.READING));
    EasyMock.expect(taskClient.getStartTimeAsync("id2")).andReturn(Futures.immediateFuture(startTime));
    EasyMock.expect(taskClient.getCurrentOffsetsAsync("id1", false))
            .andReturn(Futures.immediateFuture(ImmutableMap.of(
                SHARD_ID1,
                "2",
                SHARD_ID0,
                "1"
            )));
    EasyMock.expect(taskClient.getEndOffsetsAsync("id1")).andReturn(Futures.immediateFuture(ImmutableMap.of(
        SHARD_ID1,
        "2",
        SHARD_ID0,
        "1"
    )));
    EasyMock.expect(taskClient.getCurrentOffsetsAsync("id2", false))
            .andReturn(Futures.immediateFuture(ImmutableMap.of(
                SHARD_ID1,
                "12",
                SHARD_ID0,
                "1"
            )));

    taskRunner.registerListener(EasyMock.anyObject(TaskRunnerListener.class), EasyMock.anyObject(Executor.class));

    // since id1 is publishing, so getCheckpoints wouldn't be called for it
    TreeMap<Integer, Map<String, String>> checkpoints = new TreeMap<>();
    checkpoints.put(0, ImmutableMap.of(
        SHARD_ID1,
        "2",
        SHARD_ID0,
        "1"
    ));
    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("id2"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpoints))
            .times(1);

    replayAll();

    supervisor.start();
    supervisor.runInternal();
    supervisor.updateCurrentAndLatestOffsets();
    SupervisorReport<KinesisSupervisorReportPayload> report = supervisor.getStatus();
    verifyAll();

    Assert.assertEquals(DATASOURCE, report.getId());

    KinesisSupervisorReportPayload payload = report.getPayload();

    Assert.assertEquals(DATASOURCE, payload.getDataSource());
    Assert.assertEquals(3600L, payload.getDurationSeconds());
    Assert.assertEquals(2, payload.getPartitions());
    Assert.assertEquals(1, payload.getReplicas());
    Assert.assertEquals(STREAM, payload.getStream());
    Assert.assertEquals(1, payload.getActiveTasks().size());
    Assert.assertEquals(1, payload.getPublishingTasks().size());
    Assert.assertEquals(SupervisorStateManager.BasicState.RUNNING, payload.getDetailedState());
    Assert.assertEquals(0, payload.getRecentErrors().size());

    TaskReportData activeReport = payload.getActiveTasks().get(0);
    TaskReportData publishingReport = payload.getPublishingTasks().get(0);

    Assert.assertEquals("id2", activeReport.getId());
    Assert.assertEquals(startTime, activeReport.getStartTime());
    Assert.assertEquals(ImmutableMap.of(
        SHARD_ID1,
        "2",
        SHARD_ID0,
        "1"
    ), activeReport.getStartingOffsets());
    Assert.assertEquals(ImmutableMap.of(
        SHARD_ID1,
        "12",
        SHARD_ID0,
        "1"
    ), activeReport.getCurrentOffsets());
    Assert.assertEquals(timeLag, activeReport.getLagMillis());

    Assert.assertEquals("id1", publishingReport.getId());
    Assert.assertEquals(ImmutableMap.of(
        SHARD_ID1,
        "0",
        SHARD_ID0,
        "0"
    ), publishingReport.getStartingOffsets());
    Assert.assertEquals(ImmutableMap.of(
        SHARD_ID1,
        "2",
        SHARD_ID0,
        "1"
    ), publishingReport.getCurrentOffsets());
  }

  @Test
  public void testKillUnresponsiveTasksWhileGettingStartTime() throws Exception
  {
    supervisor = getTestableSupervisor(2, 2, true, "PT1H", null, null);
    supervisorRecordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getPartitionIds(STREAM))
            .andReturn(ImmutableSet.of(SHARD_ID1, SHARD_ID0))
            .anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getAssignment())
            .andReturn(ImmutableSet.of(SHARD1_PARTITION, SHARD0_PARTITION))
            .anyTimes();
    supervisorRecordSupplier.seekToLatest(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getLatestSequenceNumber(SHARD1_PARTITION)).andReturn("12").anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getLatestSequenceNumber(SHARD0_PARTITION)).andReturn("1").anyTimes();
    supervisorRecordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    Capture<Task> captured = Capture.newInstance(CaptureType.ALL);
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(Collections.emptyList()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
            null
        )
    ).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.capture(captured))).andReturn(true).times(4);
    taskRunner.registerListener(EasyMock.anyObject(TaskRunnerListener.class), EasyMock.anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    List<Task> tasks = captured.getValues();

    EasyMock.reset(taskStorage, taskClient, taskQueue);

    TreeMap<Integer, Map<String, String>> checkpoints1 = new TreeMap<>();
    checkpoints1.put(0, ImmutableMap.of(
        SHARD_ID1,

        "0",

        SHARD_ID0,

        "0"
    ));
    TreeMap<Integer, Map<String, String>> checkpoints2 = new TreeMap<>();
    checkpoints2.put(0, ImmutableMap.of(SHARD_ID0, "0"));
    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("sequenceName-0"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpoints1))
            .times(2);
    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("sequenceName-1"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpoints2))
            .times(2);


    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(tasks).anyTimes();
    for (Task task : tasks) {
      EasyMock.expect(taskStorage.getStatus(task.getId()))
              .andReturn(Optional.of(TaskStatus.running(task.getId())))
              .anyTimes();
      EasyMock.expect(taskStorage.getTask(task.getId())).andReturn(Optional.of(task)).anyTimes();
      EasyMock.expect(taskClient.getStatusAsync(task.getId()))
              .andReturn(Futures.immediateFuture(SeekableStreamIndexTaskRunner.Status.NOT_STARTED));
      EasyMock.expect(taskClient.getStartTimeAsync(task.getId()))
              .andReturn(Futures.immediateFailedFuture(new RuntimeException()));
      taskQueue.shutdown(task.getId(), "Task [%s] failed to return start time, killing task", task.getId());
    }
    EasyMock.replay(taskStorage, taskClient, taskQueue);

    supervisor.runInternal();
    verifyAll();
  }

  @Test
  public void testKillUnresponsiveTasksWhilePausing() throws Exception
  {
    final TaskLocation location = TaskLocation.create("testHost", 1234, -1);

    supervisor = getTestableSupervisor(2, 2, true, "PT1M", null, null);
    supervisorRecordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getPartitionIds(STREAM))
            .andReturn(ImmutableSet.of(SHARD_ID1, SHARD_ID0))
            .anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getAssignment())
            .andReturn(ImmutableSet.of(SHARD1_PARTITION, SHARD0_PARTITION))
            .anyTimes();
    supervisorRecordSupplier.seekToLatest(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getLatestSequenceNumber(SHARD1_PARTITION)).andReturn("12").anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getLatestSequenceNumber(SHARD0_PARTITION)).andReturn("1").anyTimes();
    supervisorRecordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    Capture<Task> captured = Capture.newInstance(CaptureType.ALL);
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(Collections.emptyList()).anyTimes();
    EasyMock.expect(taskRunner.getTaskLocation(EasyMock.anyString())).andReturn(TaskLocation.unknown()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
            null
        )
    ).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.capture(captured))).andReturn(true).times(4);
    taskRunner.registerListener(EasyMock.anyObject(TaskRunnerListener.class), EasyMock.anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    List<Task> tasks = captured.getValues();
    Collection workItems = new ArrayList<>();
    for (Task task : tasks) {
      workItems.add(new TestTaskRunnerWorkItem(task, null, location));
    }

    EasyMock.reset(taskStorage, taskRunner, taskClient, taskQueue);

    TreeMap<Integer, Map<String, String>> checkpoints1 = new TreeMap<>();
    checkpoints1.put(0, ImmutableMap.of(
        SHARD_ID1,
        "0"
    ));
    TreeMap<Integer, Map<String, String>> checkpoints2 = new TreeMap<>();
    checkpoints2.put(0, ImmutableMap.of(SHARD_ID0, "0"));
    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("sequenceName-0"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpoints1))
            .times(2);
    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("sequenceName-1"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpoints2))
            .times(2);

    captured = Capture.newInstance(CaptureType.ALL);
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(tasks).anyTimes();
    for (Task task : tasks) {
      EasyMock.expect(taskStorage.getStatus(task.getId()))
              .andReturn(Optional.of(TaskStatus.running(task.getId())))
              .anyTimes();
      EasyMock.expect(taskStorage.getTask(task.getId())).andReturn(Optional.of(task)).anyTimes();
    }
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(workItems).anyTimes();
    EasyMock.expect(taskRunner.getTaskLocation(EasyMock.anyString())).andReturn(location).anyTimes();
    EasyMock.expect(taskClient.getStatusAsync(EasyMock.anyString()))
            .andReturn(Futures.immediateFuture(SeekableStreamIndexTaskRunner.Status.READING))
            .anyTimes();
    EasyMock.expect(taskClient.getStartTimeAsync(EasyMock.contains("sequenceName-0")))
            .andReturn(Futures.immediateFuture(DateTimes.nowUtc().minusMinutes(2)))
            .andReturn(Futures.immediateFuture(DateTimes.nowUtc()));
    EasyMock.expect(taskClient.getStartTimeAsync(EasyMock.contains("sequenceName-1")))
            .andReturn(Futures.immediateFuture(DateTimes.nowUtc()))
            .times(2);
    EasyMock.expect(taskClient.pauseAsync(EasyMock.contains("sequenceName-0")))
            .andReturn(Futures.immediateFailedFuture(new RuntimeException())).times(2);
    taskQueue.shutdown(
        EasyMock.contains("sequenceName-0"),
        EasyMock.eq("An exception occurred while waiting for task [%s] to pause: [%s]"),
        EasyMock.contains("sequenceName-0"),
        EasyMock.anyString()
    );
    EasyMock.expectLastCall().times(2);
    EasyMock.expect(taskQueue.add(EasyMock.capture(captured))).andReturn(true).times(2);

    EasyMock.replay(taskStorage, taskRunner, taskClient, taskQueue);

    supervisor.runInternal();
    verifyAll();

    for (Task task : captured.getValues()) {
      KinesisIndexTaskIOConfig taskConfig = ((KinesisIndexTask) task).getIOConfig();
      Assert.assertEquals(
          "0",
          taskConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap().get(SHARD_ID1)
      );
      Assert.assertNull(
          taskConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap().get(SHARD_ID0)
      );
    }
  }

  @Test
  public void testKillUnresponsiveTasksWhileSettingEndOffsets() throws Exception
  {
    final TaskLocation location = TaskLocation.create("testHost", 1234, -1);

    supervisor = getTestableSupervisor(2, 2, true, "PT1M", null, null);
    supervisorRecordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getPartitionIds(STREAM))
            .andReturn(ImmutableSet.of(SHARD_ID1, SHARD_ID0))
            .anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getAssignment())
            .andReturn(ImmutableSet.of(SHARD1_PARTITION, SHARD0_PARTITION))
            .anyTimes();
    supervisorRecordSupplier.seekToLatest(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getLatestSequenceNumber(SHARD1_PARTITION)).andReturn("12").anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getLatestSequenceNumber(SHARD0_PARTITION)).andReturn("1").anyTimes();
    supervisorRecordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    Capture<Task> captured = Capture.newInstance(CaptureType.ALL);
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(Collections.emptyList()).anyTimes();
    EasyMock.expect(taskRunner.getTaskLocation(EasyMock.anyString())).andReturn(TaskLocation.unknown()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
            null
        )
    ).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.capture(captured))).andReturn(true).times(4);
    taskRunner.registerListener(EasyMock.anyObject(TaskRunnerListener.class), EasyMock.anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    List<Task> tasks = captured.getValues();
    Collection workItems = new ArrayList<>();
    for (Task task : tasks) {
      workItems.add(new TestTaskRunnerWorkItem(task, null, location));
    }

    EasyMock.reset(taskStorage, taskRunner, taskClient, taskQueue);

    EasyMock.expect(taskRunner.getTaskLocation(EasyMock.anyString())).andReturn(location).anyTimes();

    TreeMap<Integer, Map<String, String>> checkpoints1 = new TreeMap<>();
    checkpoints1.put(0, ImmutableMap.of(
        SHARD_ID1,
        "0"
    ));
    TreeMap<Integer, Map<String, String>> checkpoints2 = new TreeMap<>();
    checkpoints2.put(0, ImmutableMap.of(SHARD_ID0, "0"));
    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("sequenceName-0"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpoints1))
            .times(2);
    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("sequenceName-1"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpoints2))
            .times(2);

    captured = Capture.newInstance(CaptureType.ALL);
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(tasks).anyTimes();
    for (Task task : tasks) {
      EasyMock.expect(taskStorage.getStatus(task.getId()))
              .andReturn(Optional.of(TaskStatus.running(task.getId())))
              .anyTimes();
      EasyMock.expect(taskStorage.getTask(task.getId())).andReturn(Optional.of(task)).anyTimes();
    }
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(workItems).anyTimes();
    EasyMock.expect(taskClient.getStatusAsync(EasyMock.anyString()))
            .andReturn(Futures.immediateFuture(SeekableStreamIndexTaskRunner.Status.READING))
            .anyTimes();
    EasyMock.expect(taskClient.getStartTimeAsync(EasyMock.contains("sequenceName-0")))
            .andReturn(Futures.immediateFuture(DateTimes.nowUtc().minusMinutes(2)))
            .andReturn(Futures.immediateFuture(DateTimes.nowUtc()));
    EasyMock.expect(taskClient.getStartTimeAsync(EasyMock.contains("sequenceName-1")))
            .andReturn(Futures.immediateFuture(DateTimes.nowUtc()))
            .times(2);
    EasyMock.expect(taskClient.pauseAsync(EasyMock.contains("sequenceName-0")))
            .andReturn(Futures.immediateFuture(ImmutableMap.of(
                SHARD_ID1,
                "1"
            )))
            .andReturn(Futures.immediateFuture(ImmutableMap.of(
                SHARD_ID1,
                "3"
            )));
    EasyMock.expect(
        taskClient.setEndOffsetsAsync(
            EasyMock.contains("sequenceName-0"),
            EasyMock.eq(ImmutableMap.of(
                SHARD_ID1,
                "3"
            )),
            EasyMock.eq(true)
        )
    ).andReturn(Futures.immediateFailedFuture(new RuntimeException())).times(2);
    taskQueue.shutdown(
        EasyMock.contains("sequenceName-0"),
        EasyMock.eq("Failed to set end offsets, killing task")
    );
    EasyMock.expectLastCall().times(2);
    EasyMock.expect(taskQueue.add(EasyMock.capture(captured))).andReturn(true).times(2);

    EasyMock.replay(taskStorage, taskRunner, taskClient, taskQueue);

    supervisor.runInternal();
    verifyAll();

    for (Task task : captured.getValues()) {
      KinesisIndexTaskIOConfig taskConfig = ((KinesisIndexTask) task).getIOConfig();
      Assert.assertEquals(
          "0",
          taskConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap().get(SHARD_ID1)
      );
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testStopNotStarted()
  {
    supervisor = getTestableSupervisor(1, 1, true, "PT1H", null, null);
    supervisor.stop(false);
  }

  @Test
  public void testStop()
  {
    supervisorRecordSupplier.close();
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    taskClient.close();
    taskRunner.unregisterListener(StringUtils.format("KinesisSupervisor-%s", DATASOURCE));
    replayAll();

    supervisor = getTestableSupervisor(1, 1, true, "PT1H", null, null);
    supervisor.start();
    supervisor.stop(false);

    verifyAll();
  }

  @Test
  public void testStopGracefully() throws Exception
  {
    final TaskLocation location1 = TaskLocation.create("testHost", 1234, -1);
    final TaskLocation location2 = TaskLocation.create("testHost2", 145, -1);
    final DateTime startTime = DateTimes.nowUtc();

    supervisor = getTestableSupervisor(2, 1, true, "PT1H", null, null);
    supervisorRecordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getPartitionIds(STREAM))
            .andReturn(ImmutableSet.of(SHARD_ID1, SHARD_ID0))
            .anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getAssignment())
            .andReturn(ImmutableSet.of(SHARD1_PARTITION, SHARD0_PARTITION))
            .anyTimes();
    supervisorRecordSupplier.seekToLatest(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getLatestSequenceNumber(SHARD1_PARTITION)).andReturn("12").anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getLatestSequenceNumber(SHARD0_PARTITION)).andReturn("1").anyTimes();
    supervisorRecordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    Task id1 = createKinesisIndexTask(
        "id1",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>(
            "stream",
            ImmutableMap.of(SHARD_ID1, "0", SHARD_ID0, "0"),
            ImmutableSet.of()
        ),
        new SeekableStreamEndSequenceNumbers<>(
            "stream",
            ImmutableMap.of(
                SHARD_ID1,
                KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER,
                SHARD_ID0,
                KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER
            )
        ),
        null,
        null
    );

    Task id2 = createKinesisIndexTask(
        "id2",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>(
            "stream",
            ImmutableMap.of(
                SHARD_ID1,
                "3",
                SHARD_ID0,
                "1"
            ),
            ImmutableSet.of(SHARD_ID0, SHARD_ID1)
        ),
        new SeekableStreamEndSequenceNumbers<>(
            "stream",
            ImmutableMap.of(
                SHARD_ID1,
                KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER,
                SHARD_ID0,
                KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER
            )
        ),
        null,
        null
    );

    Task id3 = createKinesisIndexTask(
        "id3",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>(
            "stream",
            ImmutableMap.of(SHARD_ID1, "3", SHARD_ID0, "1"),
            ImmutableSet.of(SHARD_ID0, SHARD_ID1)
        ),
        new SeekableStreamEndSequenceNumbers<>(
            "stream",
            ImmutableMap.of(
                SHARD_ID1,
                KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER,
                SHARD_ID0,
                KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER
            )
        ),
        null,
        null
    );

    Collection workItems = new ArrayList<>();
    workItems.add(new TestTaskRunnerWorkItem(id1, null, location1));
    workItems.add(new TestTaskRunnerWorkItem(id2, null, location2));

    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(workItems).anyTimes();
    EasyMock.expect(taskRunner.getTaskLocation(id1.getId())).andReturn(location1).anyTimes();
    EasyMock.expect(taskRunner.getTaskLocation(id2.getId())).andReturn(location2).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of(id1, id2, id3)).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id1")).andReturn(Optional.of(TaskStatus.running("id1"))).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id2")).andReturn(Optional.of(TaskStatus.running("id2"))).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id3")).andReturn(Optional.of(TaskStatus.running("id3"))).anyTimes();
    EasyMock.expect(taskStorage.getTask("id1")).andReturn(Optional.of(id1)).anyTimes();
    EasyMock.expect(taskStorage.getTask("id2")).andReturn(Optional.of(id2)).anyTimes();
    EasyMock.expect(taskStorage.getTask("id3")).andReturn(Optional.of(id3)).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
            null
        )
    ).anyTimes();
    EasyMock.expect(taskClient.getStatusAsync("id1"))
            .andReturn(Futures.immediateFuture(SeekableStreamIndexTaskRunner.Status.PUBLISHING));
    EasyMock.expect(taskClient.getStatusAsync("id2"))
            .andReturn(Futures.immediateFuture(SeekableStreamIndexTaskRunner.Status.READING));
    EasyMock.expect(taskClient.getStatusAsync("id3"))
            .andReturn(Futures.immediateFuture(SeekableStreamIndexTaskRunner.Status.READING));
    EasyMock.expect(taskClient.getStartTimeAsync("id2")).andReturn(Futures.immediateFuture(startTime));
    EasyMock.expect(taskClient.getStartTimeAsync("id3")).andReturn(Futures.immediateFuture(startTime));
    EasyMock.expect(taskClient.getEndOffsetsAsync("id1")).andReturn(Futures.immediateFuture(ImmutableMap.of(
        SHARD_ID1,
        "3",
        SHARD_ID0,
        "1"
    )));

    // getCheckpoints will not be called for id1 as it is in publishing state
    TreeMap<Integer, Map<String, String>> checkpoints = new TreeMap<>();
    checkpoints.put(0, ImmutableMap.of(
        SHARD_ID1,
        "3",
        SHARD_ID0,
        "1"
    ));
    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("id2"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpoints))
            .times(1);
    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("id3"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpoints))
            .times(1);

    taskRunner.registerListener(EasyMock.anyObject(TaskRunnerListener.class), EasyMock.anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    EasyMock.reset(taskRunner, taskClient, taskQueue);
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(workItems).anyTimes();
    EasyMock.expect(taskRunner.getTaskLocation(id1.getId())).andReturn(location1).anyTimes();
    EasyMock.expect(taskRunner.getTaskLocation(id2.getId())).andReturn(location2).anyTimes();
    EasyMock.expect(taskRunner.getTaskLocation(id3.getId())).andReturn(TaskLocation.unknown()).anyTimes();
    EasyMock.expect(taskClient.pauseAsync("id2"))
            .andReturn(Futures.immediateFuture(ImmutableMap.of(
                SHARD_ID1,
                "12",
                SHARD_ID0,
                "1"
            )));
    EasyMock.expect(taskClient.setEndOffsetsAsync("id2", ImmutableMap.of(
        SHARD_ID1,
        "12",
        SHARD_ID0,
        "1"
    ), true))
            .andReturn(Futures.immediateFuture(true));
    taskQueue.shutdown("id3", "Killing task for graceful shutdown");
    EasyMock.expectLastCall().times(1);
    taskQueue.shutdown("id3", "Killing task [%s] which hasn't been assigned to a worker", "id3");
    EasyMock.expectLastCall().times(1);

    EasyMock.replay(taskRunner, taskClient, taskQueue);

    supervisor.gracefulShutdownInternal();
    verifyAll();
  }

  @Test
  public void testResetNoTasks()
  {
    EasyMock.expect(supervisorRecordSupplier.getPartitionIds(EasyMock.anyObject()))
            .andReturn(Collections.emptySet())
            .anyTimes();

    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(Collections.emptyList()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    taskRunner.registerListener(EasyMock.anyObject(TaskRunnerListener.class), EasyMock.anyObject(Executor.class));
    replayAll();

    supervisor = getTestableSupervisor(1, 1, true, "PT1H", null, null);


    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    EasyMock.reset(indexerMetadataStorageCoordinator);
    EasyMock.expect(indexerMetadataStorageCoordinator.deleteDataSourceMetadata(DATASOURCE)).andReturn(true);
    EasyMock.replay(indexerMetadataStorageCoordinator);

    supervisor.resetInternal(null);
    verifyAll();
  }

  @Test
  public void testResetDataSourceMetadata() throws Exception
  {
    EasyMock.expect(supervisorRecordSupplier.getPartitionIds(EasyMock.anyObject()))
            .andReturn(Collections.emptySet())
            .anyTimes();
    supervisor = getTestableSupervisor(1, 1, true, "PT1H", null, null);
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(Collections.emptyList()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    taskRunner.registerListener(EasyMock.anyObject(TaskRunnerListener.class), EasyMock.anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    Capture<String> captureDataSource = EasyMock.newCapture();
    Capture<DataSourceMetadata> captureDataSourceMetadata = EasyMock.newCapture();

    KinesisDataSourceMetadata kinesisDataSourceMetadata = new KinesisDataSourceMetadata(
        new SeekableStreamStartSequenceNumbers<>(
            STREAM,
            ImmutableMap.of(
                SHARD_ID1,
                KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER,
                SHARD_ID0,
                KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER
            ),
            ImmutableSet.of()
        )
    );

    KinesisDataSourceMetadata resetMetadata = new KinesisDataSourceMetadata(
        new SeekableStreamStartSequenceNumbers<>(
            STREAM,
            ImmutableMap.of(
                SHARD_ID0,
                KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER
            ),
            ImmutableSet.of()
        )
    );

    KinesisDataSourceMetadata expectedMetadata = new KinesisDataSourceMetadata(
        new SeekableStreamStartSequenceNumbers<>(
            STREAM,
            ImmutableMap.of(
                SHARD_ID1,
                KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER
            ),
            ImmutableSet.of()
        )
    );

    EasyMock.reset(indexerMetadataStorageCoordinator);
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE))
            .andReturn(kinesisDataSourceMetadata);
    EasyMock.expect(indexerMetadataStorageCoordinator.resetDataSourceMetadata(
        EasyMock.capture(captureDataSource),
        EasyMock.capture(captureDataSourceMetadata)
    )).andReturn(true);
    EasyMock.replay(indexerMetadataStorageCoordinator);

    try {
      supervisor.resetInternal(resetMetadata);
    }
    catch (NullPointerException npe) {
      // Expected as there will be an attempt to EasyMock.reset partitionGroups sequences to NOT_SET
      // however there would be no entries in the map as we have not put nay data in kafka
      Assert.assertNull(npe.getCause());
    }
    verifyAll();

    Assert.assertEquals(captureDataSource.getValue(), DATASOURCE);
    Assert.assertEquals(captureDataSourceMetadata.getValue(), expectedMetadata);
  }

  @Test
  public void testResetNoDataSourceMetadata()
  {
    EasyMock.expect(supervisorRecordSupplier.getPartitionIds(EasyMock.anyObject()))
            .andReturn(Collections.emptySet())
            .anyTimes();
    supervisor = getTestableSupervisor(1, 1, true, "PT1H", null, null);
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(Collections.emptyList()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    taskRunner.registerListener(EasyMock.anyObject(TaskRunnerListener.class), EasyMock.anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    KinesisDataSourceMetadata resetMetadata = new KinesisDataSourceMetadata(
        new SeekableStreamStartSequenceNumbers<>(
            STREAM,
            ImmutableMap.of(SHARD_ID0, KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER),
            ImmutableSet.of()
        )
    );

    EasyMock.reset(indexerMetadataStorageCoordinator);
    // no DataSourceMetadata in metadata store
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(null);
    EasyMock.replay(indexerMetadataStorageCoordinator);

    supervisor.resetInternal(resetMetadata);
    verifyAll();
  }

  @Test
  public void testGetOffsetFromStorageForPartitionWithResetOffsetAutomatically() throws Exception
  {
    supervisor = getTestableSupervisor(1, 1, true, true, "PT1H", null, null, false);

    supervisorRecordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getPartitionIds(STREAM))
            .andReturn(ImmutableSet.of(SHARD_ID1, SHARD_ID0))
            .anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getAssignment())
            .andReturn(ImmutableSet.of(SHARD1_PARTITION, SHARD0_PARTITION))
            .anyTimes();
    supervisorRecordSupplier.seekToLatest(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("300").anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getLatestSequenceNumber(EasyMock.anyObject())).andReturn("400").anyTimes();
    supervisorRecordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();
    // Only sequence numbers >= 300 are available
    EasyMock.expect(supervisorRecordSupplier.isOffsetAvailable(EasyMock.anyObject(),
                                                               EasyMock.eq(KinesisSequenceNumber.of("400"))))
            .andReturn(true)
            .anyTimes();
    EasyMock.expect(supervisorRecordSupplier.isOffsetAvailable(EasyMock.anyObject(),
                                                               EasyMock.eq(KinesisSequenceNumber.of("200"))))
            .andReturn(false)
            .anyTimes();
    EasyMock.expect(supervisorRecordSupplier.isOffsetAvailable(EasyMock.anyObject(),
                                                               EasyMock.eq(KinesisSequenceNumber.of("100"))))
            .andReturn(false)
            .anyTimes();

    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(Collections.emptyList()).anyTimes();
    taskRunner.registerListener(EasyMock.anyObject(TaskRunnerListener.class), EasyMock.anyObject(Executor.class));

    EasyMock.reset(indexerMetadataStorageCoordinator);
    // unknown DataSourceMetadata in metadata store
    EasyMock
        .expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE))
        .andReturn(
            new KinesisDataSourceMetadata(
                new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "100", SHARD_ID2, "200"))
            )
        )
        .times(2);

    // Since shard 2 was in metadata before but is not in the list of shards returned by the record supplier,
    // it gets deleted from metadata (it is an expired shard)
    EasyMock
        .expect(
            indexerMetadataStorageCoordinator.resetDataSourceMetadata(
                DATASOURCE,
                new KinesisDataSourceMetadata(
                    new SeekableStreamEndSequenceNumbers<>(
                        STREAM,
                        ImmutableMap.of(SHARD_ID1, "100", SHARD_ID2, KinesisSequenceNumber.EXPIRED_MARKER)
                    )
                )
            )
        )
        .andReturn(true)
        .times(1);

    // getOffsetFromStorageForPartition() throws an exception when the offsets are automatically reset.
    // Since getOffsetFromStorageForPartition() is called per partition, all partitions can't be reset at the same time.
    // Instead, subsequent partitions will be reset in the following supervisor runs.
    EasyMock
        .expect(
            indexerMetadataStorageCoordinator.resetDataSourceMetadata(
                DATASOURCE,
                new KinesisDataSourceMetadata(
                    // Only one partition is reset in a single supervisor run.
                    new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of())
                )
            )
        )
        .andReturn(true)
        .times(1);

    EasyMock
        .expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE))
        .andReturn(
            new KinesisDataSourceMetadata(
                new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "100"))
            )
        )
        .times(2);

    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();
  }

  @Test
  public void testResetRunningTasks() throws Exception
  {
    final TaskLocation location1 = TaskLocation.create("testHost", 1234, -1);
    final TaskLocation location2 = TaskLocation.create("testHost2", 145, -1);
    final DateTime startTime = DateTimes.nowUtc();

    supervisor = getTestableSupervisor(2, 1, true, "PT1H", null, null);
    supervisorRecordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getPartitionIds(STREAM))
            .andReturn(ImmutableSet.of(SHARD_ID1, SHARD_ID0))
            .anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getAssignment())
            .andReturn(ImmutableSet.of(SHARD1_PARTITION, SHARD0_PARTITION))
            .anyTimes();
    supervisorRecordSupplier.seekToLatest(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getLatestSequenceNumber(SHARD1_PARTITION)).andReturn("12").anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getLatestSequenceNumber(SHARD0_PARTITION)).andReturn("1").anyTimes();
    supervisorRecordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    Task id1 = createKinesisIndexTask(
        "id1",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>(
            "stream",
            ImmutableMap.of(SHARD_ID1, "0", SHARD_ID0, "0"),
            ImmutableSet.of()
        ),
        new SeekableStreamEndSequenceNumbers<>(
            "stream",
            ImmutableMap.of(
                SHARD_ID1,
                KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER,
                SHARD_ID0,
                KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER
            )
        ),
        null,
        null
    );

    Task id2 = createKinesisIndexTask(
        "id2",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>(
            "stream",
            ImmutableMap.of(SHARD_ID1, "3", SHARD_ID0, "1"),
            ImmutableSet.of(SHARD_ID0, SHARD_ID1)
        ),
        new SeekableStreamEndSequenceNumbers<>(
            "stream",
            ImmutableMap.of(
                SHARD_ID1,
                KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER,
                SHARD_ID0,
                KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER
            )
        ),
        null,
        null
    );

    Task id3 = createKinesisIndexTask(
        "id3",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>(
            "stream",
            ImmutableMap.of(SHARD_ID1, "3", SHARD_ID0, "1"),
            ImmutableSet.of(SHARD_ID0, SHARD_ID1)
        ),
        new SeekableStreamEndSequenceNumbers<>(
            "stream",
            ImmutableMap.of(
                SHARD_ID1,
                KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER,
                SHARD_ID0,
                KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER
            )
        ),
        null,
        null
    );

    Collection workItems = new ArrayList<>();
    workItems.add(new TestTaskRunnerWorkItem(id1, null, location1));
    workItems.add(new TestTaskRunnerWorkItem(id2, null, location2));

    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(workItems).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of(id1, id2, id3)).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id1")).andReturn(Optional.of(TaskStatus.running("id1"))).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id2")).andReturn(Optional.of(TaskStatus.running("id2"))).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id3")).andReturn(Optional.of(TaskStatus.running("id3"))).anyTimes();
    EasyMock.expect(taskStorage.getTask("id1")).andReturn(Optional.of(id1)).anyTimes();
    EasyMock.expect(taskStorage.getTask("id2")).andReturn(Optional.of(id2)).anyTimes();
    EasyMock.expect(taskStorage.getTask("id3")).andReturn(Optional.of(id3)).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
            null
        )
    ).anyTimes();
    EasyMock.expect(taskClient.getStatusAsync("id1"))
            .andReturn(Futures.immediateFuture(SeekableStreamIndexTaskRunner.Status.PUBLISHING));
    EasyMock.expect(taskClient.getStatusAsync("id2"))
            .andReturn(Futures.immediateFuture(SeekableStreamIndexTaskRunner.Status.READING));
    EasyMock.expect(taskClient.getStatusAsync("id3"))
            .andReturn(Futures.immediateFuture(SeekableStreamIndexTaskRunner.Status.READING));
    EasyMock.expect(taskClient.getStartTimeAsync("id2")).andReturn(Futures.immediateFuture(startTime));
    EasyMock.expect(taskClient.getStartTimeAsync("id3")).andReturn(Futures.immediateFuture(startTime));
    EasyMock.expect(taskClient.getEndOffsetsAsync("id1")).andReturn(Futures.immediateFuture(ImmutableMap.of(
        SHARD_ID1,

        "3",

        SHARD_ID0,

        "1"
    )));

    TreeMap<Integer, Map<String, String>> checkpoints = new TreeMap<>();
    checkpoints.put(0, ImmutableMap.of(
        SHARD_ID1,

        "3",

        SHARD_ID0,

        "1"
    ));
    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("id2"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpoints))
            .times(1);
    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("id3"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpoints))
            .times(1);

    taskRunner.registerListener(EasyMock.anyObject(TaskRunnerListener.class), EasyMock.anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    EasyMock.reset(taskQueue, indexerMetadataStorageCoordinator);
    EasyMock.expect(indexerMetadataStorageCoordinator.deleteDataSourceMetadata(DATASOURCE)).andReturn(true);
    taskQueue.shutdown("id2", "DataSourceMetadata is not found while reset");
    taskQueue.shutdown("id3", "DataSourceMetadata is not found while reset");
    EasyMock.replay(taskQueue, indexerMetadataStorageCoordinator);

    supervisor.resetInternal(null);
    verifyAll();
  }

  @Test
  public void testNoDataIngestionTasks() throws Exception
  {
    final DateTime startTime = DateTimes.nowUtc();
    supervisor = getTestableSupervisor(2, 1, true, "PT1H", null, null);

    //not adding any events
    Task id1 = createKinesisIndexTask(
        "id1",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>(
            "stream",
            ImmutableMap.of(SHARD_ID1, "0", SHARD_ID0, "0"),
            ImmutableSet.of()
        ),
        new SeekableStreamEndSequenceNumbers<>(
            "stream",
            ImmutableMap.of(
                SHARD_ID1,
                KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER,
                SHARD_ID0,
                KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER
            )
        ),
        null,
        null
    );

    Task id2 = createKinesisIndexTask(
        "id2",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>(
            "stream",
            ImmutableMap.of(SHARD_ID1, "10", SHARD_ID0, "20"),
            ImmutableSet.of(SHARD_ID0, SHARD_ID1)
        ),
        new SeekableStreamEndSequenceNumbers<>(
            "stream",
            ImmutableMap.of(
                SHARD_ID1,
                KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER,
                SHARD_ID0,
                KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER
            )
        ),
        null,
        null
    );

    Task id3 = createKinesisIndexTask(
        "id3",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>(
            "stream",
            ImmutableMap.of(SHARD_ID1, "10", SHARD_ID0, "20"),
            ImmutableSet.of(SHARD_ID0, SHARD_ID1)
        ),
        new SeekableStreamEndSequenceNumbers<>(
            "stream",
            ImmutableMap.of(
                SHARD_ID1,
                KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER,
                SHARD_ID0,
                KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER
            )
        ),
        null,
        null
    );

    supervisorRecordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getPartitionIds(STREAM))
            .andReturn(ImmutableSet.of(SHARD_ID1, SHARD_ID0))
            .anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getAssignment())
            .andReturn(ImmutableSet.of(SHARD1_PARTITION, SHARD0_PARTITION))
            .anyTimes();
    supervisorRecordSupplier.seekToLatest(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getLatestSequenceNumber(SHARD1_PARTITION)).andReturn("12").anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getLatestSequenceNumber(SHARD0_PARTITION)).andReturn("1").anyTimes();
    supervisorRecordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    final Collection workItems = new ArrayList();
    workItems.add(new TestTaskRunnerWorkItem(id1, null, TaskLocation.create(id1.getId(), 8100, 8100)));
    workItems.add(new TestTaskRunnerWorkItem(id2, null, TaskLocation.create(id2.getId(), 8100, 8100)));
    workItems.add(new TestTaskRunnerWorkItem(id3, null, TaskLocation.create(id3.getId(), 8100, 8100)));
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(workItems);
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of(id1, id2, id3)).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id1")).andReturn(Optional.of(TaskStatus.running("id1"))).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id2")).andReturn(Optional.of(TaskStatus.running("id2"))).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id3")).andReturn(Optional.of(TaskStatus.running("id3"))).anyTimes();
    EasyMock.expect(taskStorage.getTask("id1")).andReturn(Optional.of(id1)).anyTimes();
    EasyMock.expect(taskStorage.getTask("id2")).andReturn(Optional.of(id2)).anyTimes();
    EasyMock.expect(taskStorage.getTask("id3")).andReturn(Optional.of(id3)).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
            null
        )
    ).anyTimes();
    EasyMock.expect(taskClient.getStatusAsync("id1"))
            .andReturn(Futures.immediateFuture(SeekableStreamIndexTaskRunner.Status.READING));
    EasyMock.expect(taskClient.getStatusAsync("id2"))
            .andReturn(Futures.immediateFuture(SeekableStreamIndexTaskRunner.Status.READING));
    EasyMock.expect(taskClient.getStatusAsync("id3"))
            .andReturn(Futures.immediateFuture(SeekableStreamIndexTaskRunner.Status.READING));
    EasyMock.expect(taskClient.getStartTimeAsync("id1")).andReturn(Futures.immediateFuture(startTime));
    EasyMock.expect(taskClient.getStartTimeAsync("id2")).andReturn(Futures.immediateFuture(startTime));
    EasyMock.expect(taskClient.getStartTimeAsync("id3")).andReturn(Futures.immediateFuture(startTime));

    TreeMap<Integer, Map<String, String>> checkpoints = new TreeMap<>();
    checkpoints.put(0, ImmutableMap.of(
        SHARD_ID1,
        "10",
        SHARD_ID0,
        "20"
    ));
    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("id1"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpoints))
            .times(1);
    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("id2"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpoints))
            .times(1);
    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("id3"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpoints))
            .times(1);

    taskRunner.registerListener(EasyMock.anyObject(TaskRunnerListener.class), EasyMock.anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    EasyMock.reset(taskQueue, indexerMetadataStorageCoordinator);
    EasyMock.expect(indexerMetadataStorageCoordinator.deleteDataSourceMetadata(DATASOURCE)).andReturn(true);
    taskQueue.shutdown("id1", "DataSourceMetadata is not found while reset");
    taskQueue.shutdown("id2", "DataSourceMetadata is not found while reset");
    taskQueue.shutdown("id3", "DataSourceMetadata is not found while reset");
    EasyMock.replay(taskQueue, indexerMetadataStorageCoordinator);

    supervisor.resetInternal(null);
    verifyAll();
  }


  @Test(timeout = 60_000L)
  public void testCheckpointForInactiveTaskGroup() throws InterruptedException
  {
    supervisor = getTestableSupervisor(2, 1, true, "PT1S", null, null, false);
    //not adding any events
    final KinesisIndexTask id1;
    id1 = createKinesisIndexTask(
        "id1",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>(
            "stream",
            ImmutableMap.of(SHARD_ID1, "0", SHARD_ID0, "0"),
            ImmutableSet.of()
        ),
        new SeekableStreamEndSequenceNumbers<>(
            "stream",
            ImmutableMap.of(
                SHARD_ID1,
                KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER,
                SHARD_ID0,
                KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER
            )
        ),
        null,
        null
    );

    final Task id2 = createKinesisIndexTask(
        "id2",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>(
            "stream",
            ImmutableMap.of(SHARD_ID1, "10", SHARD_ID0, "20"),
            ImmutableSet.of(SHARD_ID0, SHARD_ID1)
        ),
        new SeekableStreamEndSequenceNumbers<>(
            "stream",
            ImmutableMap.of(
                SHARD_ID1,
                KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER,
                SHARD_ID0,
                KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER
            )
        ),
        null,
        null
    );

    final Task id3 = createKinesisIndexTask(
        "id3",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>(
            "stream",
            ImmutableMap.of(SHARD_ID1, "10", SHARD_ID0, "20"),
            ImmutableSet.of(SHARD_ID0, SHARD_ID1)
        ),
        new SeekableStreamEndSequenceNumbers<>(
            "stream",
            ImmutableMap.of(
                SHARD_ID1,
                KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER,
                SHARD_ID0,
                KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER
            )
        ),
        null,
        null
    );

    final TaskLocation location1 = TaskLocation.create("testHost", 1234, -1);
    final TaskLocation location2 = TaskLocation.create("testHost2", 145, -1);
    Collection workItems = new ArrayList<>();
    workItems.add(new TestTaskRunnerWorkItem(id1, null, location1));
    workItems.add(new TestTaskRunnerWorkItem(id2, null, location2));
    workItems.add(new TestTaskRunnerWorkItem(id2, null, location2));

    supervisorRecordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getPartitionIds(STREAM))
            .andReturn(ImmutableSet.of(SHARD_ID1, SHARD_ID0))
            .anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getAssignment())
            .andReturn(ImmutableSet.of(SHARD1_PARTITION, SHARD0_PARTITION))
            .anyTimes();
    supervisorRecordSupplier.seekToLatest(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getLatestSequenceNumber(SHARD1_PARTITION)).andReturn("12").anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getLatestSequenceNumber(SHARD0_PARTITION)).andReturn("1").anyTimes();
    supervisorRecordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(workItems).anyTimes();
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of(id1, id2, id3)).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id1")).andReturn(Optional.of(TaskStatus.running("id1"))).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id2")).andReturn(Optional.of(TaskStatus.running("id2"))).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id3")).andReturn(Optional.of(TaskStatus.running("id3"))).anyTimes();
    EasyMock.expect(taskStorage.getTask("id1")).andReturn(Optional.of(id1)).anyTimes();
    EasyMock.expect(taskStorage.getTask("id2")).andReturn(Optional.of(id2)).anyTimes();
    EasyMock.expect(taskStorage.getTask("id3")).andReturn(Optional.of(id3)).anyTimes();
    EasyMock.expect(
        indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(new KinesisDataSourceMetadata(
        null)
    ).anyTimes();
    EasyMock.expect(taskClient.getStatusAsync("id1"))
            .andReturn(Futures.immediateFuture(SeekableStreamIndexTaskRunner.Status.READING));
    EasyMock.expect(taskClient.getStatusAsync("id2"))
            .andReturn(Futures.immediateFuture(SeekableStreamIndexTaskRunner.Status.READING));
    EasyMock.expect(taskClient.getStatusAsync("id3"))
            .andReturn(Futures.immediateFuture(SeekableStreamIndexTaskRunner.Status.READING));

    final DateTime startTime = DateTimes.nowUtc();
    EasyMock.expect(taskClient.getStartTimeAsync("id1")).andReturn(Futures.immediateFuture(startTime));
    EasyMock.expect(taskClient.getStartTimeAsync("id2")).andReturn(Futures.immediateFuture(startTime));
    EasyMock.expect(taskClient.getStartTimeAsync("id3")).andReturn(Futures.immediateFuture(startTime));

    final TreeMap<Integer, Map<String, String>> checkpoints = new TreeMap<>();
    checkpoints.put(0, ImmutableMap.of(SHARD_ID1, "10", SHARD_ID0, "20"));
    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("id1"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpoints))
            .times(1);
    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("id2"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpoints))
            .times(1);
    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("id3"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpoints))
            .times(1);

    taskRunner.registerListener(EasyMock.anyObject(TaskRunnerListener.class), EasyMock.anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();

    supervisor.moveTaskGroupToPendingCompletion(0);
    supervisor.checkpoint(
        0,
        new KinesisDataSourceMetadata(
            new SeekableStreamStartSequenceNumbers<>(STREAM, checkpoints.get(0), checkpoints.get(0).keySet())
        )
    );

    while (supervisor.getNoticesQueueSize() > 0) {
      Thread.sleep(100);
    }

    verifyAll();

    Assert.assertTrue(serviceEmitter.getAlerts().isEmpty());
  }

  @Test(timeout = 60_000L)
  public void testCheckpointForUnknownTaskGroup()
      throws InterruptedException
  {
    supervisor = getTestableSupervisor(2, 1, true, "PT1S", null, null, false);


    supervisorRecordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getPartitionIds(STREAM))
            .andReturn(ImmutableSet.of(SHARD_ID1, SHARD_ID0))
            .anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getAssignment())
            .andReturn(ImmutableSet.of(SHARD1_PARTITION, SHARD0_PARTITION))
            .anyTimes();
    supervisorRecordSupplier.seekToLatest(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getLatestSequenceNumber(SHARD1_PARTITION)).andReturn("12").anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getLatestSequenceNumber(SHARD0_PARTITION)).andReturn("1").anyTimes();
    supervisorRecordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    //not adding any events
    final KinesisIndexTask id1 = createKinesisIndexTask(
        "id1",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>(
            "stream",
            ImmutableMap.of(SHARD_ID1, "0", SHARD_ID0, "0"),
            ImmutableSet.of()
        ),
        new SeekableStreamEndSequenceNumbers<>(
            "stream",
            ImmutableMap.of(
                SHARD_ID1,
                KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER,
                SHARD_ID0,
                KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER
            )
        ),
        null,
        null
    );

    final Task id2 = createKinesisIndexTask(
        "id2",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>(
            "stream",
            ImmutableMap.of(SHARD_ID1, "10", SHARD_ID0, "20"),
            ImmutableSet.of(SHARD_ID0, SHARD_ID1)
        ),
        new SeekableStreamEndSequenceNumbers<>(
            "stream",
            ImmutableMap.of(
                SHARD_ID1,
                KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER,
                SHARD_ID0,
                KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER
            )
        ),
        null,
        null
    );

    final Task id3 = createKinesisIndexTask(
        "id3",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>(
            "stream",
            ImmutableMap.of(SHARD_ID1, "10", SHARD_ID0, "20"),
            ImmutableSet.of(SHARD_ID0, SHARD_ID1)
        ),
        new SeekableStreamEndSequenceNumbers<>(
            "stream",
            ImmutableMap.of(
                SHARD_ID1,
                KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER,
                SHARD_ID0,
                KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER
            )
        ),
        null,
        null
    );

    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of(id1, id2, id3)).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id1")).andReturn(Optional.of(TaskStatus.running("id1"))).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id2")).andReturn(Optional.of(TaskStatus.running("id2"))).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id3")).andReturn(Optional.of(TaskStatus.running("id3"))).anyTimes();
    EasyMock.expect(taskStorage.getTask("id1")).andReturn(Optional.of(id1)).anyTimes();
    EasyMock.expect(taskStorage.getTask("id2")).andReturn(Optional.of(id2)).anyTimes();
    EasyMock.expect(taskStorage.getTask("id3")).andReturn(Optional.of(id3)).anyTimes();
    EasyMock.expect(
        indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(new KinesisDataSourceMetadata(
        null)
    ).anyTimes();

    replayAll();

    supervisor.start();

    supervisor.checkpoint(
        0,
        new KinesisDataSourceMetadata(
            new SeekableStreamStartSequenceNumbers<>(STREAM, Collections.emptyMap(), ImmutableSet.of())
        )
    );

    while (supervisor.getNoticesQueueSize() > 0) {
      Thread.sleep(100);
    }

    verifyAll();

    while (serviceEmitter.getAlerts().isEmpty()) {
      Thread.sleep(100);
    }

    final AlertEvent alert = serviceEmitter.getAlerts().get(0);
    Assert.assertEquals(
        "SeekableStreamSupervisor[testDS] failed to handle notice",
        alert.getDescription()
    );
    Assert.assertEquals(
        "Cannot find taskGroup [0] among all activelyReadingTaskGroups [{}]",
        alert.getDataMap().get(AlertBuilder.EXCEPTION_MESSAGE_KEY)
    );
  }

  @Test
  public void testSuspendedNoRunningTasks()
  {
    supervisor = getTestableSupervisor(1, 1, true, "PT1H", null, null, true);

    EasyMock.expect(supervisorRecordSupplier.getPartitionIds(EasyMock.anyObject()))
            .andReturn(Collections.emptySet())
            .anyTimes();
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
            null
        )
    ).anyTimes();
    // this asserts that taskQueue.add does not in fact get called because supervisor should be suspended
    EasyMock.expect(taskQueue.add(EasyMock.anyObject())).andAnswer((IAnswer) () -> {
      Assert.fail();
      return null;
    }).anyTimes();
    taskRunner.registerListener(EasyMock.anyObject(TaskRunnerListener.class), EasyMock.anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();
  }

  @Test
  public void testSuspendedRunningTasks() throws Exception
  {
    // graceful shutdown is expected to be called on running tasks since state is suspended

    final TaskLocation location1 = TaskLocation.create("testHost", 1234, -1);
    final TaskLocation location2 = TaskLocation.create("testHost2", 145, -1);
    final DateTime startTime = DateTimes.nowUtc();

    supervisor = getTestableSupervisor(2, 1, true, "PT1H", null, null, true);

    supervisorRecordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getPartitionIds(STREAM))
            .andReturn(ImmutableSet.of(SHARD_ID1, SHARD_ID0))
            .anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getAssignment())
            .andReturn(ImmutableSet.of(SHARD1_PARTITION, SHARD0_PARTITION))
            .anyTimes();
    supervisorRecordSupplier.seekToLatest(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getLatestSequenceNumber(SHARD1_PARTITION)).andReturn("12").anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getLatestSequenceNumber(SHARD0_PARTITION)).andReturn("1").anyTimes();
    supervisorRecordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    Task id1 = createKinesisIndexTask(
        "id1",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>(
            "stream",
            ImmutableMap.of(SHARD_ID1, "0", SHARD_ID0, "0"),
            ImmutableSet.of()
        ),
        new SeekableStreamEndSequenceNumbers<>(
            "stream",
            ImmutableMap.of(
                SHARD_ID1,
                KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER,
                SHARD_ID0,
                KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER
            )
        ),
        null,
        null
    );

    Task id2 = createKinesisIndexTask(
        "id2",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>(
            "stream",
            ImmutableMap.of(SHARD_ID1, "3", SHARD_ID0, "1"),
            ImmutableSet.of(SHARD_ID0, SHARD_ID1)
        ),
        new SeekableStreamEndSequenceNumbers<>(
            "stream",
            ImmutableMap.of(
                SHARD_ID1,
                KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER,
                SHARD_ID0,
                KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER
            )
        ),
        null,
        null
    );

    Task id3 = createKinesisIndexTask(
        "id3",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>(
            "stream",
            ImmutableMap.of(SHARD_ID1, "3", SHARD_ID0, "1"),
            ImmutableSet.of(SHARD_ID0, SHARD_ID1)
        ),
        new SeekableStreamEndSequenceNumbers<>(
            "stream",
            ImmutableMap.of(
                SHARD_ID1,
                KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER,
                SHARD_ID0,
                KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER
            )
        ),
        null,
        null
    );

    Collection workItems = new ArrayList<>();
    workItems.add(new TestTaskRunnerWorkItem(id1, null, location1));
    workItems.add(new TestTaskRunnerWorkItem(id2, null, location2));

    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(workItems).anyTimes();
    EasyMock.expect(taskRunner.getTaskLocation(id1.getId())).andReturn(location1).anyTimes();
    EasyMock.expect(taskRunner.getTaskLocation(id2.getId())).andReturn(location2).anyTimes();
    EasyMock.expect(taskRunner.getTaskLocation(id3.getId())).andReturn(TaskLocation.unknown()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of(id1, id2, id3)).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id1")).andReturn(Optional.of(TaskStatus.running("id1"))).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id2")).andReturn(Optional.of(TaskStatus.running("id2"))).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id3")).andReturn(Optional.of(TaskStatus.running("id3"))).anyTimes();
    EasyMock.expect(taskStorage.getTask("id1")).andReturn(Optional.of(id1)).anyTimes();
    EasyMock.expect(taskStorage.getTask("id2")).andReturn(Optional.of(id2)).anyTimes();
    EasyMock.expect(taskStorage.getTask("id3")).andReturn(Optional.of(id3)).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
            null
        )
    ).anyTimes();
    EasyMock.expect(taskClient.getStatusAsync("id1"))
            .andReturn(Futures.immediateFuture(SeekableStreamIndexTaskRunner.Status.PUBLISHING));
    EasyMock.expect(taskClient.getStatusAsync("id2"))
            .andReturn(Futures.immediateFuture(SeekableStreamIndexTaskRunner.Status.READING));
    EasyMock.expect(taskClient.getStatusAsync("id3"))
            .andReturn(Futures.immediateFuture(SeekableStreamIndexTaskRunner.Status.READING));
    EasyMock.expect(taskClient.getStartTimeAsync("id2")).andReturn(Futures.immediateFuture(startTime));
    EasyMock.expect(taskClient.getStartTimeAsync("id3")).andReturn(Futures.immediateFuture(startTime));
    EasyMock.expect(taskClient.getEndOffsetsAsync("id1")).andReturn(Futures.immediateFuture(ImmutableMap.of(
        SHARD_ID1,
        "3",
        SHARD_ID0,
        "1"
    )));

    // getCheckpoints will not be called for id1 as it is in publishing state
    TreeMap<Integer, Map<String, String>> checkpoints = new TreeMap<>();
    checkpoints.put(0, ImmutableMap.of(
        SHARD_ID1,
        "3",
        SHARD_ID0,
        "1"
    ));
    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("id2"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpoints))
            .times(1);
    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("id3"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpoints))
            .times(1);

    taskRunner.registerListener(EasyMock.anyObject(TaskRunnerListener.class), EasyMock.anyObject(Executor.class));

    EasyMock.expect(taskClient.pauseAsync("id2"))
            .andReturn(Futures.immediateFuture(ImmutableMap.of(
                SHARD_ID1,
                "12",
                SHARD_ID0,
                "1"
            )));
    EasyMock.expect(taskClient.setEndOffsetsAsync("id2", ImmutableMap.of(
        SHARD_ID1,
        "12",
        SHARD_ID0,
        "1"
    ), true))
            .andReturn(Futures.immediateFuture(true));
    taskQueue.shutdown("id3", "Killing task for graceful shutdown");
    EasyMock.expectLastCall().times(1);
    taskQueue.shutdown("id3", "Killing task [%s] which hasn't been assigned to a worker", "id3");
    EasyMock.expectLastCall().times(1);

    replayAll();
    supervisor.start();
    supervisor.runInternal();
    verifyAll();
  }

  @Test
  public void testResetSuspended()
  {
    EasyMock.expect(supervisorRecordSupplier.getPartitionIds(EasyMock.anyObject()))
            .andReturn(Collections.emptySet())
            .anyTimes();
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(Collections.emptyList()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    taskRunner.registerListener(EasyMock.anyObject(TaskRunnerListener.class), EasyMock.anyObject(Executor.class));
    replayAll();

    supervisor = getTestableSupervisor(1, 1, true, "PT1H", null, null, true);
    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    EasyMock.reset(indexerMetadataStorageCoordinator);
    EasyMock.expect(indexerMetadataStorageCoordinator.deleteDataSourceMetadata(DATASOURCE)).andReturn(true);
    EasyMock.replay(indexerMetadataStorageCoordinator);

    supervisor.resetInternal(null);
    verifyAll();
  }


  @Test
  public void testGetCurrentTotalStats()
  {
    supervisor = getTestableSupervisor(1, 2, true, "PT1H", null, null, false);
    supervisor.setPartitionIdsForTests(
        ImmutableList.of(SHARD_ID0, SHARD_ID1)
    );
    supervisor.addTaskGroupToActivelyReadingTaskGroup(
        supervisor.getTaskGroupIdForPartition(SHARD_ID0),
        ImmutableMap.of("0", "0"),
        Optional.absent(),
        Optional.absent(),
        ImmutableSet.of("task1"),
        ImmutableSet.of()
    );

    supervisor.addTaskGroupToPendingCompletionTaskGroup(
        supervisor.getTaskGroupIdForPartition(SHARD_ID1),
        ImmutableMap.of("0", "0"),
        Optional.absent(),
        Optional.absent(),
        ImmutableSet.of("task2"),
        ImmutableSet.of()
    );

    EasyMock.expect(taskClient.getMovingAveragesAsync("task1")).andReturn(Futures.immediateFuture(ImmutableMap.of(
        "prop1",
        "val1"
    ))).times(1);

    EasyMock.expect(taskClient.getMovingAveragesAsync("task2")).andReturn(Futures.immediateFuture(ImmutableMap.of(
        "prop2",
        "val2"
    ))).times(1);

    replayAll();

    Map<String, Map<String, Object>> stats = supervisor.getStats();

    verifyAll();

    Assert.assertEquals(2, stats.size());
    Assert.assertEquals(ImmutableSet.of("0", "1"), stats.keySet());
    Assert.assertEquals(ImmutableMap.of("task1", ImmutableMap.of("prop1", "val1")), stats.get("0"));
    Assert.assertEquals(ImmutableMap.of("task2", ImmutableMap.of("prop2", "val2")), stats.get("1"));
  }

  @Test
  public void testDoNotKillCompatibleTasks()
      throws InterruptedException, EntryExistsException
  {
    // This supervisor always returns true for isTaskCurrent -> it should not kill its tasks
    int numReplicas = 2;
    supervisor = getTestableSupervisorCustomIsTaskCurrent(
        numReplicas,
        1,
        true,
        "PT1H",
        new Period("P1D"),
        new Period("P1D"),
        false,
        1000,
        true
    );

    supervisorRecordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getPartitionIds(STREAM))
            .andReturn(ImmutableSet.of(SHARD_ID1, SHARD_ID0)).anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getAssignment())
            .andReturn(ImmutableSet.of(SHARD1_PARTITION, SHARD0_PARTITION))
            .anyTimes();
    supervisorRecordSupplier.seekToLatest(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getLatestSequenceNumber(EasyMock.anyObject())).andReturn("100").anyTimes();
    supervisorRecordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    Task task = createKinesisIndexTask(
        "id2",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>(
            STREAM,
            ImmutableMap.of(
                SHARD_ID0,
                "0",
                SHARD_ID1,
                "0"
            ),
            ImmutableSet.of()
        ),
        new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(
            SHARD_ID0,
            "1",
            SHARD_ID1,
            "12"
        )),
        null,
        null
    );

    List<Task> existingTasks = ImmutableList.of(task);

    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(Collections.emptyList()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(existingTasks).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id2")).andReturn(Optional.of(TaskStatus.running("id2"))).anyTimes();
    EasyMock.expect(taskStorage.getTask("id2")).andReturn(Optional.of(task)).anyTimes();
    EasyMock.expect(taskClient.getStatusAsync(EasyMock.anyString()))
            .andReturn(Futures.immediateFuture(SeekableStreamIndexTaskRunner.Status.NOT_STARTED))
            .anyTimes();
    EasyMock.expect(taskClient.getStartTimeAsync(EasyMock.anyString()))
            .andReturn(Futures.immediateFuture(DateTimes.nowUtc()))
            .anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
            null
        )
    ).anyTimes();

    taskRunner.registerListener(EasyMock.anyObject(TaskRunnerListener.class), EasyMock.anyObject(Executor.class));
    EasyMock.expect(taskQueue.add(EasyMock.anyObject(Task.class))).andReturn(true);

    TreeMap<Integer, Map<String, String>> checkpoints = new TreeMap<>();
    checkpoints.put(0, ImmutableMap.of(
        SHARD_ID0,
        "0",
        SHARD_ID1,
        "0"
    ));

    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("id2"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpoints))
            .times(numReplicas);

    replayAll();
    supervisor.start();
    supervisor.runInternal();
    verifyAll();
  }

  @Test
  public void testKillIncompatibleTasks()
      throws InterruptedException, EntryExistsException
  {
    // This supervisor always returns false for isTaskCurrent -> it should kill its tasks
    int numReplicas = 2;
    supervisor = getTestableSupervisorCustomIsTaskCurrent(
        numReplicas,
        1,
        true,
        "PT1H",
        new Period("P1D"),
        new Period("P1D"),
        false,
        1000,
        false
    );
    supervisorRecordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getPartitionIds(STREAM))
            .andReturn(ImmutableSet.of(SHARD_ID1, SHARD_ID0)).anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getAssignment())
            .andReturn(ImmutableSet.of(SHARD1_PARTITION, SHARD0_PARTITION))
            .anyTimes();
    supervisorRecordSupplier.seekToLatest(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getLatestSequenceNumber(EasyMock.anyObject())).andReturn("100").anyTimes();
    supervisorRecordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    Task task = createKinesisIndexTask(
        "id1",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>(
            STREAM,
            ImmutableMap.of(
                SHARD_ID0,
                "0",
                SHARD_ID1,
                "0"
            ),
            ImmutableSet.of()
        ),
        new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(
            SHARD_ID0,
            "1",
            SHARD_ID1,
            "12"
        )),
        null,
        null
    );

    List<Task> existingTasks = ImmutableList.of(task);

    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(Collections.emptyList()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(existingTasks).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id1")).andReturn(Optional.of(TaskStatus.running("id1"))).anyTimes();
    EasyMock.expect(taskStorage.getTask("id1")).andReturn(Optional.of(task)).anyTimes();
    EasyMock.expect(taskClient.getStatusAsync(EasyMock.anyString()))
            .andReturn(Futures.immediateFuture(SeekableStreamIndexTaskRunner.Status.NOT_STARTED))
            .anyTimes();
    EasyMock.expect(taskClient.getStartTimeAsync(EasyMock.anyString()))
            .andReturn(Futures.immediateFuture(DateTimes.nowUtc()))
            .anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
            null
        )
    ).anyTimes();
    EasyMock.expect(taskClient.stopAsync("id1", false)).andReturn(Futures.immediateFuture(true));
    taskRunner.registerListener(EasyMock.anyObject(TaskRunnerListener.class), EasyMock.anyObject(Executor.class));
    EasyMock.expect(taskQueue.add(EasyMock.anyObject(Task.class))).andReturn(true).times(2);

    replayAll();
    supervisor.start();
    supervisor.runInternal();
    verifyAll();
  }

  @Test
  public void testIsTaskCurrent()
  {
    DateTime minMessageTime = DateTimes.nowUtc();
    DateTime maxMessageTime = DateTimes.nowUtc().plus(10000);

    KinesisSupervisor supervisor = createSupervisor(
        1,
        1,
        true,
        "PT1H",
        new Period("P1D"),
        new Period("P1D"),
        false,
        42,
        dataSchema,
        tuningConfig
    );

    supervisor.addTaskGroupToActivelyReadingTaskGroup(
        42,
        ImmutableMap.of(SHARD_ID1, "3"),
        Optional.of(minMessageTime),
        Optional.of(maxMessageTime),
        ImmutableSet.of("id1", "id2", "id3", "id4"),
        ImmutableSet.of()
    );

    DataSchema modifiedDataSchema = getDataSchema("some other datasource");

    KinesisSupervisorTuningConfig modifiedTuningConfig = new KinesisSupervisorTuningConfig(
        null,
        1000,
        null,
        null,
        50000,
        null,
        new Period("P1Y"),
        null,
        null,
        null,
        false,
        null,
        null,
        null,
        null,
        numThreads,
        TEST_CHAT_RETRIES,
        TEST_HTTP_TIMEOUT,
        TEST_SHUTDOWN_TIMEOUT,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        42, // This property is different from tuningConfig
        1_000_000,
        null,
        null,
        null,
        null
    );

    KinesisIndexTask taskFromStorage = createKinesisIndexTask(
        "id1",
        0,
        new SeekableStreamStartSequenceNumbers<>("stream", ImmutableMap.of(
            SHARD_ID1,
            "3"
        ), ImmutableSet.of()),
        new SeekableStreamEndSequenceNumbers<>("stream", ImmutableMap.of(
            SHARD_ID1,
            KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER
        )),
        minMessageTime,
        maxMessageTime,
        dataSchema
    );

    KinesisIndexTask taskFromStorageMismatchedDataSchema = createKinesisIndexTask(
        "id2",
        0,
        new SeekableStreamStartSequenceNumbers<>("stream", ImmutableMap.of(
            SHARD_ID1,
            "3"
        ), ImmutableSet.of()),
        new SeekableStreamEndSequenceNumbers<>("stream", ImmutableMap.of(
            SHARD_ID1,
            KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER
        )),
        minMessageTime,
        maxMessageTime,
        modifiedDataSchema
    );

    KinesisIndexTask taskFromStorageMismatchedTuningConfig = createKinesisIndexTask(
        "id3",
        0,
        new SeekableStreamStartSequenceNumbers<>("stream", ImmutableMap.of(
            SHARD_ID1,
            "3"
        ), ImmutableSet.of()),
        new SeekableStreamEndSequenceNumbers<>("stream", ImmutableMap.of(
            SHARD_ID1,
            KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER
        )),
        minMessageTime,
        maxMessageTime,
        dataSchema,
        modifiedTuningConfig
    );

    KinesisIndexTask taskFromStorageMismatchedPartitionsWithTaskGroup = createKinesisIndexTask(
        "id4",
        0,
        new SeekableStreamStartSequenceNumbers<>(
            "stream",
            ImmutableMap.of(
                SHARD_ID1,
                "4" // this is the mismatch
            ),
            ImmutableSet.of()
        ),
        new SeekableStreamEndSequenceNumbers<>("stream", ImmutableMap.of(
            SHARD_ID1,
            KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER
        )),
        minMessageTime,
        maxMessageTime,
        dataSchema
    );

    Map<String, Task> taskMap = ImmutableMap.of(
        taskFromStorage.getId(), taskFromStorage,
        taskFromStorageMismatchedDataSchema.getId(), taskFromStorageMismatchedDataSchema,
        taskFromStorageMismatchedTuningConfig.getId(), taskFromStorageMismatchedTuningConfig,
        taskFromStorageMismatchedPartitionsWithTaskGroup.getId(), taskFromStorageMismatchedPartitionsWithTaskGroup
    );

    replayAll();

    Assert.assertTrue(supervisor.isTaskCurrent(42, "id1", taskMap));
    Assert.assertFalse(supervisor.isTaskCurrent(42, "id2", taskMap));
    Assert.assertFalse(supervisor.isTaskCurrent(42, "id3", taskMap));
    Assert.assertFalse(supervisor.isTaskCurrent(42, "id4", taskMap));
    verifyAll();
  }

  @Test
  public void testSequenceNameDoesNotChangeWithTaskId()
  {
    final DateTime minMessageTime = DateTimes.nowUtc();
    final DateTime maxMessageTime = DateTimes.nowUtc().plus(10000);

    KinesisSupervisor supervisor = createSupervisor(
        1,
        1,
        true,
        "PT1H",
        new Period("P1D"),
        new Period("P1D"),
        false,
        42,
        dataSchema,
        tuningConfig
    );

    // Create task1 with some start and end offsets
    final KinesisIndexTask task1 = createKinesisIndexTask(
        "id0",
        0,
        new SeekableStreamStartSequenceNumbers<>("stream", ImmutableMap.of(SHARD_ID1, "3"), ImmutableSet.of()),
        new SeekableStreamEndSequenceNumbers<>(
            "stream",
            ImmutableMap.of(SHARD_ID1, KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER)
        ),
        minMessageTime,
        maxMessageTime,
        dataSchema
    );

    // Create task2 with same offsets
    final KinesisIndexTask task2 = createKinesisIndexTask(
        "id1",
        0,
        task1.getIOConfig().getStartSequenceNumbers(),
        task1.getIOConfig().getEndSequenceNumbers(),
        task1.getIOConfig().getMinimumMessageTime().get(),
        task1.getIOConfig().getMaximumMessageTime().get(),
        dataSchema
    );

    replayAll();

    final String sequenceTask1 = supervisor.generateSequenceName(
        task1.getIOConfig().getStartSequenceNumbers().getPartitionSequenceNumberMap(),
        task1.getIOConfig().getMinimumMessageTime(),
        task1.getIOConfig().getMaximumMessageTime(),
        task1.getDataSchema(),
        task1.getTuningConfig()
    );
    Assert.assertNotNull(sequenceTask1);

    final String sequenceTask2 = supervisor.generateSequenceName(
        task2.getIOConfig().getStartSequenceNumbers().getPartitionSequenceNumberMap(),
        task2.getIOConfig().getMinimumMessageTime(),
        task2.getIOConfig().getMaximumMessageTime(),
        task2.getDataSchema(),
        task2.getTuningConfig()
    );
    Assert.assertNotNull(sequenceTask2);

    Assert.assertNotEquals(task1.getId(), task2.getId());
    Assert.assertEquals(sequenceTask1, sequenceTask2);

    verifyAll();
  }

  @Test
  public void testShardSplit() throws Exception
  {
    supervisor = getTestableSupervisor(1, 2, true, "PT1H", null, null);
    taskRunner.registerListener(EasyMock.anyObject(TaskRunnerListener.class), EasyMock.anyObject(Executor.class));

    List<Task> phaseOneTasks = testShardSplitPhaseOne();

    List<Task> phaseTwoTasks = testShardSplitPhaseTwo(phaseOneTasks);

    testShardSplitPhaseThree(phaseTwoTasks);
  }

  @Test
  public void testCorrectInputSources()
  {
    KinesisSupervisorSpec supervisorSpec = new KinesisSupervisorSpec(
        null,
        dataSchema,
        null,
        new KinesisSupervisorIOConfig(
            STREAM,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            true,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            false
        ),
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null
    );

    Assert.assertEquals(
        Collections.singleton(
            new ResourceAction(
                new Resource(KinesisSupervisorSpec.SUPERVISOR_TYPE, ResourceType.EXTERNAL),
                Action.READ
            )),
        supervisorSpec.getInputSourceResources()
    );
  }

  private List<Task> testShardSplitPhaseOne() throws Exception
  {
    supervisorRecordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(supervisorRecordSupplier.getPartitionIds(STREAM))
            .andReturn(ImmutableSet.of(SHARD_ID0))
            .anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getAssignment())
            .andReturn(ImmutableSet.of(SHARD0_PARTITION))
            .anyTimes();

    supervisorRecordSupplier.seekToLatest(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getLatestSequenceNumber(EasyMock.anyObject())).andReturn("100").anyTimes();
    supervisorRecordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.isOffsetAvailable(EasyMock.anyObject(), EasyMock.anyObject()))
            .andReturn(true)
            .anyTimes();

    Capture<Task> captured = Capture.newInstance(CaptureType.ALL);
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(Collections.emptyList()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(taskClient.getStatusAsync(EasyMock.anyString()))
            .andReturn(Futures.immediateFuture(SeekableStreamIndexTaskRunner.Status.NOT_STARTED))
            .anyTimes();
    EasyMock.expect(taskClient.getStartTimeAsync(EasyMock.anyString()))
            .andReturn(Futures.immediateFuture(DateTimes.nowUtc()))
            .anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
            null
        )
    ).anyTimes();

    EasyMock.expect(taskQueue.add(EasyMock.capture(captured))).andReturn(true).times(1);

    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    List<Task> tasks = captured.getValues();

    EasyMock.reset(taskStorage);
    EasyMock.reset(taskClient);

    EasyMock.expect(taskClient.getStatusAsync(EasyMock.anyString()))
            .andReturn(Futures.immediateFuture(SeekableStreamIndexTaskRunner.Status.NOT_STARTED))
            .anyTimes();
    EasyMock.expect(taskClient.getStartTimeAsync(EasyMock.anyString()))
            .andReturn(Futures.immediateFuture(DateTimes.nowUtc()))
            .anyTimes();
    TreeMap<Integer, Map<String, String>> checkpoints1 = new TreeMap<>();
    checkpoints1.put(0, ImmutableMap.of(
        SHARD_ID0,
        "0"
    ));
    // there would be 1 task, since there is only 1 shard
    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("sequenceName-0"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpoints1))
            .times(1);

    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(tasks).anyTimes();

    for (Task task : tasks) {
      EasyMock.expect(taskStorage.getStatus(task.getId()))
              .andReturn(Optional.of(TaskStatus.running(task.getId())))
              .anyTimes();
      EasyMock.expect(taskStorage.getTask(task.getId())).andReturn(Optional.of(task)).anyTimes();
    }
    EasyMock.replay(taskStorage);
    EasyMock.replay(taskClient);

    supervisor.runInternal();
    verifyAll();

    return tasks;
  }

  /**
   * Test task creation after a shard split with a closed shard
   *
   * @param phaseOneTasks List of tasks from the initial phase where only one shard was present
   */
  private List<Task> testShardSplitPhaseTwo(List<Task> phaseOneTasks) throws Exception
  {
    EasyMock.reset(indexerMetadataStorageCoordinator);
    EasyMock.reset(taskStorage);
    EasyMock.reset(taskQueue);
    EasyMock.reset(taskClient);
    EasyMock.reset(taskMaster);
    EasyMock.reset(taskRunner);
    EasyMock.reset(supervisorRecordSupplier);

    // first task ran, its shard 0 has reached EOS
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
            new SeekableStreamEndSequenceNumbers<String, String>(
                STREAM,
                ImmutableMap.of(SHARD_ID0, KinesisSequenceNumber.END_OF_SHARD_MARKER)
            )
        )
    ).anyTimes();

    EasyMock.expect(supervisorRecordSupplier.getPartitionIds(STREAM))
            .andReturn(ImmutableSet.of(SHARD_ID0, SHARD_ID1, SHARD_ID2))
            .anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getAssignment())
            .andReturn(ImmutableSet.of(SHARD0_PARTITION, SHARD1_PARTITION, SHARD2_PARTITION))
            .anyTimes();

    supervisorRecordSupplier.seekToLatest(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getLatestSequenceNumber(new StreamPartition<>(STREAM, SHARD_ID0)))
            .andReturn(KinesisSequenceNumber.END_OF_SHARD_MARKER).anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getLatestSequenceNumber(new StreamPartition<>(STREAM, SHARD_ID1)))
            .andReturn("100").anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getLatestSequenceNumber(new StreamPartition<>(STREAM, SHARD_ID2)))
            .andReturn("100").anyTimes();

    supervisorRecordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    Capture<Task> postSplitCaptured = Capture.newInstance(CaptureType.ALL);

    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(Collections.emptyList()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(taskClient.getStatusAsync(EasyMock.anyString()))
            .andReturn(Futures.immediateFuture(SeekableStreamIndexTaskRunner.Status.NOT_STARTED))
            .anyTimes();
    Task successfulTask = phaseOneTasks.get(0);
    EasyMock.expect(taskStorage.getStatus(successfulTask.getId()))
            .andReturn(Optional.of(TaskStatus.success(successfulTask.getId())));
    EasyMock.expect(taskStorage.getTask(successfulTask.getId())).andReturn(Optional.of(successfulTask)).anyTimes();
    EasyMock.expect(taskClient.getStartTimeAsync(EasyMock.anyString()))
            .andReturn(Futures.immediateFuture(DateTimes.nowUtc()))
            .anyTimes();

    EasyMock.expect(taskQueue.add(EasyMock.capture(postSplitCaptured))).andReturn(true).times(2);

    replayAll();

    supervisor.runInternal();
    verifyAll();

    EasyMock.reset(taskStorage);
    EasyMock.reset(taskClient);

    EasyMock.expect(taskClient.getStatusAsync(EasyMock.anyString()))
            .andReturn(Futures.immediateFuture(SeekableStreamIndexTaskRunner.Status.NOT_STARTED))
            .anyTimes();
    EasyMock.expect(taskClient.getStartTimeAsync(EasyMock.anyString()))
            .andReturn(Futures.immediateFuture(DateTimes.nowUtc()))
            .anyTimes();
    TreeMap<Integer, Map<String, String>> checkpointsGroup0 = new TreeMap<>();
    checkpointsGroup0.put(0, ImmutableMap.of(
        SHARD_ID1, "0"
    ));
    TreeMap<Integer, Map<String, String>> checkpointsGroup1 = new TreeMap<>();
    checkpointsGroup1.put(1, ImmutableMap.of(
        SHARD_ID2, "0"
    ));
    // there would be 2 tasks, 1 for each task group
    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("sequenceName-0"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpointsGroup0))
            .times(1);
    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("sequenceName-1"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpointsGroup1))
            .times(1);

    List<Task> postSplitTasks = postSplitCaptured.getValues();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(postSplitTasks).anyTimes();
    for (Task task : postSplitTasks) {
      EasyMock.expect(taskStorage.getStatus(task.getId()))
              .andReturn(Optional.of(TaskStatus.running(task.getId())))
              .anyTimes();
      EasyMock.expect(taskStorage.getTask(task.getId())).andReturn(Optional.of(task)).anyTimes();
    }
    EasyMock.replay(taskStorage);
    EasyMock.replay(taskClient);

    supervisor.runInternal();
    verifyAll();

    // Check that shardId-000000000000 which has hit EOS is not included in the sequences sent to the task for group 0
    SeekableStreamStartSequenceNumbers<String, String> group0ExpectedStartSequenceNumbers =
        new SeekableStreamStartSequenceNumbers<>(
            STREAM,
            ImmutableMap.of(
                SHARD_ID1, "0"
            ),
            ImmutableSet.of()
        );

    SeekableStreamEndSequenceNumbers<String, String> group0ExpectedEndSequenceNumbers =
        new SeekableStreamEndSequenceNumbers<>(
            STREAM,
            ImmutableMap.of(
                SHARD_ID1, KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER
            )
        );

    SeekableStreamStartSequenceNumbers<String, String> group1ExpectedStartSequenceNumbers =
        new SeekableStreamStartSequenceNumbers<>(
            STREAM,
            ImmutableMap.of(
                SHARD_ID2, "0"
            ),
            ImmutableSet.of()
        );

    SeekableStreamEndSequenceNumbers<String, String> group1ExpectedEndSequenceNumbers =
        new SeekableStreamEndSequenceNumbers<>(
            STREAM,
            ImmutableMap.of(
                SHARD_ID2, KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER
            )
        );

    Assert.assertEquals(2, postSplitTasks.size());
    KinesisIndexTaskIOConfig group0Config = ((KinesisIndexTask) postSplitTasks.get(0)).getIOConfig();
    KinesisIndexTaskIOConfig group1Config = ((KinesisIndexTask) postSplitTasks.get(1)).getIOConfig();
    Assert.assertEquals((Integer) 0, group0Config.getTaskGroupId());
    Assert.assertEquals((Integer) 1, group1Config.getTaskGroupId());
    Assert.assertEquals(group0ExpectedStartSequenceNumbers, group0Config.getStartSequenceNumbers());
    Assert.assertEquals(group0ExpectedEndSequenceNumbers, group0Config.getEndSequenceNumbers());
    Assert.assertEquals(group1ExpectedStartSequenceNumbers, group1Config.getStartSequenceNumbers());
    Assert.assertEquals(group1ExpectedEndSequenceNumbers, group1Config.getEndSequenceNumbers());

    return postSplitTasks;
  }

  /**
   * Test task creation after a shard split with a closed shard, with the closed shards expiring and no longer
   * being returned from record supplier.
   *
   * @param phaseTwoTasks List of tasks from the second phase where closed but not expired shards were present.
   */
  private void testShardSplitPhaseThree(List<Task> phaseTwoTasks) throws Exception
  {
    EasyMock.reset(indexerMetadataStorageCoordinator);
    EasyMock.reset(taskStorage);
    EasyMock.reset(taskQueue);
    EasyMock.reset(taskClient);
    EasyMock.reset(taskMaster);
    EasyMock.reset(taskRunner);
    EasyMock.reset(supervisorRecordSupplier);

    // second set of tasks ran, shard 0 has expired, but shard 1 and 2 have data
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
            new SeekableStreamEndSequenceNumbers<String, String>(
                STREAM,
                ImmutableMap.of(
                    SHARD_ID0, KinesisSequenceNumber.END_OF_SHARD_MARKER,
                    SHARD_ID1, "100",
                    SHARD_ID2, "100"
                )
            )
        )
    ).anyTimes();

    EasyMock.expect(
        indexerMetadataStorageCoordinator.resetDataSourceMetadata(
            DATASOURCE,
            new KinesisDataSourceMetadata(
                new SeekableStreamEndSequenceNumbers<String, String>(
                    STREAM,
                    ImmutableMap.of(
                        SHARD_ID0, KinesisSequenceNumber.EXPIRED_MARKER,
                        SHARD_ID1, "100",
                        SHARD_ID2, "100"
                    )
                )
            )
        )
    ).andReturn(true).anyTimes();

    EasyMock.expect(supervisorRecordSupplier.getPartitionIds(STREAM))
            .andReturn(ImmutableSet.of(SHARD_ID1, SHARD_ID2))
            .anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getAssignment())
            .andReturn(ImmutableSet.of(SHARD1_PARTITION, SHARD2_PARTITION))
            .anyTimes();

    supervisorRecordSupplier.seekToLatest(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getLatestSequenceNumber(new StreamPartition<>(STREAM, SHARD_ID1)))
            .andReturn("200").anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getLatestSequenceNumber(new StreamPartition<>(STREAM, SHARD_ID2)))
            .andReturn("200").anyTimes();

    supervisorRecordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.isOffsetAvailable(EasyMock.anyObject(), EasyMock.anyObject()))
            .andReturn(true)
            .anyTimes();

    Capture<Task> postSplitCaptured = Capture.newInstance(CaptureType.ALL);

    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(Collections.emptyList()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(taskClient.getStatusAsync(EasyMock.anyString()))
            .andReturn(Futures.immediateFuture(SeekableStreamIndexTaskRunner.Status.NOT_STARTED))
            .anyTimes();

    Task successfulTask0 = phaseTwoTasks.get(0);
    EasyMock.expect(taskStorage.getStatus(successfulTask0.getId()))
            .andReturn(Optional.of(TaskStatus.success(successfulTask0.getId())));
    EasyMock.expect(taskStorage.getTask(successfulTask0.getId())).andReturn(Optional.of(successfulTask0)).anyTimes();

    Task successfulTask1 = phaseTwoTasks.get(1);
    EasyMock.expect(taskStorage.getStatus(successfulTask1.getId()))
            .andReturn(Optional.of(TaskStatus.success(successfulTask1.getId())));
    EasyMock.expect(taskStorage.getTask(successfulTask1.getId())).andReturn(Optional.of(successfulTask1)).anyTimes();

    EasyMock.expect(taskClient.getStartTimeAsync(EasyMock.anyString()))
            .andReturn(Futures.immediateFuture(DateTimes.nowUtc()))
            .anyTimes();

    EasyMock.expect(taskQueue.add(EasyMock.capture(postSplitCaptured))).andReturn(true).times(2);

    replayAll();

    supervisor.runInternal();
    verifyAll();

    EasyMock.reset(taskStorage);
    EasyMock.reset(taskClient);

    EasyMock.expect(taskClient.getStatusAsync(EasyMock.anyString()))
            .andReturn(Futures.immediateFuture(SeekableStreamIndexTaskRunner.Status.NOT_STARTED))
            .anyTimes();
    EasyMock.expect(taskClient.getStartTimeAsync(EasyMock.anyString()))
            .andReturn(Futures.immediateFuture(DateTimes.nowUtc()))
            .anyTimes();
    TreeMap<Integer, Map<String, String>> checkpointsGroup0 = new TreeMap<>();
    checkpointsGroup0.put(0, ImmutableMap.of(
        SHARD_ID2, "100"
    ));
    TreeMap<Integer, Map<String, String>> checkpointsGroup1 = new TreeMap<>();
    checkpointsGroup1.put(1, ImmutableMap.of(
        SHARD_ID1, "100"
    ));
    // there would be 2 tasks, 1 for each task group
    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("sequenceName-0"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpointsGroup0))
            .times(1);
    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("sequenceName-1"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpointsGroup1))
            .times(1);

    List<Task> postSplitTasks = postSplitCaptured.getValues();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(postSplitTasks).anyTimes();
    for (Task task : postSplitTasks) {
      EasyMock.expect(taskStorage.getStatus(task.getId()))
              .andReturn(Optional.of(TaskStatus.running(task.getId())))
              .anyTimes();
      EasyMock.expect(taskStorage.getTask(task.getId())).andReturn(Optional.of(task)).anyTimes();
    }
    EasyMock.replay(taskStorage);
    EasyMock.replay(taskClient);

    supervisor.runInternal();
    verifyAll();


    // Check that shardId-000000000000 which has hit EOS is not included in the sequences sent to the task for group 0
    SeekableStreamStartSequenceNumbers<String, String> group0ExpectedStartSequenceNumbers =
        new SeekableStreamStartSequenceNumbers<>(
            STREAM,
            ImmutableMap.of(
                SHARD_ID1, "100"
            ),
            ImmutableSet.of(SHARD_ID1)
        );

    SeekableStreamEndSequenceNumbers<String, String> group0ExpectedEndSequenceNumbers =
        new SeekableStreamEndSequenceNumbers<>(
            STREAM,
            ImmutableMap.of(
                SHARD_ID1, KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER
            )
        );

    SeekableStreamStartSequenceNumbers<String, String> group1ExpectedStartSequenceNumbers =
        new SeekableStreamStartSequenceNumbers<>(
            STREAM,
            ImmutableMap.of(
                SHARD_ID2, "100"
            ),
            ImmutableSet.of(SHARD_ID2)
        );

    SeekableStreamEndSequenceNumbers<String, String> group1ExpectedEndSequenceNumbers =
        new SeekableStreamEndSequenceNumbers<>(
            STREAM,
            ImmutableMap.of(
                SHARD_ID2, KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER
            )
        );

    Assert.assertEquals(2, postSplitTasks.size());
    KinesisIndexTaskIOConfig group0Config = ((KinesisIndexTask) postSplitTasks.get(0)).getIOConfig();
    KinesisIndexTaskIOConfig group1Config = ((KinesisIndexTask) postSplitTasks.get(1)).getIOConfig();
    Assert.assertEquals((Integer) 0, group0Config.getTaskGroupId());
    Assert.assertEquals((Integer) 1, group1Config.getTaskGroupId());
    Assert.assertEquals(group0ExpectedStartSequenceNumbers, group0Config.getStartSequenceNumbers());
    Assert.assertEquals(group0ExpectedEndSequenceNumbers, group0Config.getEndSequenceNumbers());
    Assert.assertEquals(group1ExpectedStartSequenceNumbers, group1Config.getStartSequenceNumbers());
    Assert.assertEquals(group1ExpectedEndSequenceNumbers, group1Config.getEndSequenceNumbers());

    Map<Integer, Set<String>> expectedPartitionGroups = ImmutableMap.of(
        0, ImmutableSet.of(SHARD_ID1),
        1, ImmutableSet.of(SHARD_ID2)
    );
    Assert.assertEquals(expectedPartitionGroups, supervisor.getPartitionGroups());

    ConcurrentHashMap<String, String> expectedPartitionOffsets = new ConcurrentHashMap<>(
        ImmutableMap.of(
            SHARD_ID2, "-1",
            SHARD_ID1, "-1",
            SHARD_ID0, "-1"
        )
    );
    Assert.assertEquals(expectedPartitionOffsets, supervisor.getPartitionOffsets());

  }

  @Test
  public void testShardMerge() throws Exception
  {
    supervisor = getTestableSupervisor(1, 2, true, "PT1H", null, null);
    taskRunner.registerListener(EasyMock.anyObject(TaskRunnerListener.class), EasyMock.anyObject(Executor.class));

    List<Task> phaseOneTasks = testShardMergePhaseOne();
    List<Task> phaseTwoTasks = testShardMergePhaseTwo(phaseOneTasks);
    testShardMergePhaseThree(phaseTwoTasks);
  }

  private List<Task> testShardMergePhaseOne() throws Exception
  {
    supervisorRecordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(supervisorRecordSupplier.getPartitionIds(STREAM))
            .andReturn(ImmutableSet.of(SHARD_ID0, SHARD_ID1))
            .anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getAssignment())
            .andReturn(ImmutableSet.of(SHARD0_PARTITION, SHARD1_PARTITION))
            .anyTimes();

    supervisorRecordSupplier.seekToLatest(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getLatestSequenceNumber(EasyMock.anyObject())).andReturn("100").anyTimes();
    supervisorRecordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    Capture<Task> captured = Capture.newInstance(CaptureType.ALL);
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(Collections.emptyList()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(taskClient.getStatusAsync(EasyMock.anyString()))
            .andReturn(Futures.immediateFuture(SeekableStreamIndexTaskRunner.Status.NOT_STARTED))
            .anyTimes();
    EasyMock.expect(taskClient.getStartTimeAsync(EasyMock.anyString()))
            .andReturn(Futures.immediateFuture(DateTimes.nowUtc()))
            .anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
            null
        )
    ).anyTimes();

    EasyMock.expect(taskQueue.add(EasyMock.capture(captured))).andReturn(true).times(2);

    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    List<Task> tasks = captured.getValues();

    EasyMock.reset(taskStorage);
    EasyMock.reset(taskClient);

    EasyMock.expect(taskClient.getStatusAsync(EasyMock.anyString()))
            .andReturn(Futures.immediateFuture(SeekableStreamIndexTaskRunner.Status.NOT_STARTED))
            .anyTimes();
    EasyMock.expect(taskClient.getStartTimeAsync(EasyMock.anyString()))
            .andReturn(Futures.immediateFuture(DateTimes.nowUtc()))
            .anyTimes();

    TreeMap<Integer, Map<String, String>> checkpoints0 = new TreeMap<>();
    checkpoints0.put(0, ImmutableMap.of(
        SHARD_ID1,
        "0"
    ));
    TreeMap<Integer, Map<String, String>> checkpoints1 = new TreeMap<>();
    checkpoints1.put(0, ImmutableMap.of(
        SHARD_ID0,
        "0"
    ));
    // there would be 2 tasks, 1 for each task group
    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("sequenceName-0"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpoints0))
            .times(1);
    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("sequenceName-1"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpoints1))
            .times(1);

    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(tasks).anyTimes();
    for (Task task : tasks) {
      EasyMock.expect(taskStorage.getStatus(task.getId()))
              .andReturn(Optional.of(TaskStatus.running(task.getId())))
              .anyTimes();
      EasyMock.expect(taskStorage.getTask(task.getId())).andReturn(Optional.of(task)).anyTimes();
    }
    EasyMock.replay(taskStorage);
    EasyMock.replay(taskClient);

    supervisor.runInternal();
    verifyAll();

    return tasks;
  }

  /**
   * Test task creation after a shard split with a closed shard
   *
   * @param phaseOneTasks List of tasks from the initial phase where only one shard was present
   */
  private List<Task> testShardMergePhaseTwo(List<Task> phaseOneTasks) throws Exception
  {
    EasyMock.reset(indexerMetadataStorageCoordinator);
    EasyMock.reset(taskStorage);
    EasyMock.reset(taskQueue);
    EasyMock.reset(taskClient);
    EasyMock.reset(taskMaster);
    EasyMock.reset(taskRunner);
    EasyMock.reset(supervisorRecordSupplier);

    // first tasks ran, both shard 0 and shard 1 have reached EOS, merged into shard 2
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
            new SeekableStreamEndSequenceNumbers<String, String>(
                STREAM,
                ImmutableMap.of(
                    SHARD_ID0, KinesisSequenceNumber.END_OF_SHARD_MARKER,
                    SHARD_ID1, KinesisSequenceNumber.END_OF_SHARD_MARKER
                )
            )
        )
    ).anyTimes();

    EasyMock.expect(supervisorRecordSupplier.getPartitionIds(STREAM))
            .andReturn(ImmutableSet.of(SHARD_ID0, SHARD_ID1, SHARD_ID2))
            .anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getAssignment())
            .andReturn(ImmutableSet.of(SHARD0_PARTITION, SHARD1_PARTITION, SHARD2_PARTITION))
            .anyTimes();

    supervisorRecordSupplier.seekToLatest(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getLatestSequenceNumber(new StreamPartition<>(STREAM, SHARD_ID0)))
            .andReturn(KinesisSequenceNumber.END_OF_SHARD_MARKER).anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getLatestSequenceNumber(new StreamPartition<>(STREAM, SHARD_ID1)))
            .andReturn(KinesisSequenceNumber.END_OF_SHARD_MARKER).anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getLatestSequenceNumber(new StreamPartition<>(STREAM, SHARD_ID2)))
            .andReturn("100").anyTimes();

    supervisorRecordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    Capture<Task> postMergeCaptured = Capture.newInstance(CaptureType.ALL);

    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(Collections.emptyList()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(taskClient.getStatusAsync(EasyMock.anyString()))
            .andReturn(Futures.immediateFuture(SeekableStreamIndexTaskRunner.Status.NOT_STARTED))
            .anyTimes();

    Task successfulTask0 = phaseOneTasks.get(0);
    Task successfulTask1 = phaseOneTasks.get(1);
    EasyMock.expect(taskStorage.getStatus(successfulTask0.getId()))
            .andReturn(Optional.of(TaskStatus.success(successfulTask0.getId())));
    EasyMock.expect(taskStorage.getTask(successfulTask0.getId())).andReturn(Optional.of(successfulTask0)).anyTimes();

    EasyMock.expect(taskStorage.getStatus(successfulTask1.getId()))
            .andReturn(Optional.of(TaskStatus.success(successfulTask1.getId())));
    EasyMock.expect(taskStorage.getTask(successfulTask1.getId())).andReturn(Optional.of(successfulTask1)).anyTimes();

    EasyMock.expect(taskClient.getStartTimeAsync(EasyMock.anyString()))
            .andReturn(Futures.immediateFuture(DateTimes.nowUtc()))
            .anyTimes();

    EasyMock.expect(taskQueue.add(EasyMock.capture(postMergeCaptured))).andReturn(true).times(1);

    replayAll();

    supervisor.runInternal();
    verifyAll();

    EasyMock.reset(taskStorage);
    EasyMock.reset(taskClient);

    EasyMock.expect(taskClient.getStatusAsync(EasyMock.anyString()))
            .andReturn(Futures.immediateFuture(SeekableStreamIndexTaskRunner.Status.NOT_STARTED))
            .anyTimes();
    EasyMock.expect(taskClient.getStartTimeAsync(EasyMock.anyString()))
            .andReturn(Futures.immediateFuture(DateTimes.nowUtc()))
            .anyTimes();
    TreeMap<Integer, Map<String, String>> checkpointsGroup0 = new TreeMap<>();
    checkpointsGroup0.put(0, ImmutableMap.of(
        SHARD_ID2, "0",
        SHARD_ID1, KinesisSequenceNumber.END_OF_SHARD_MARKER
    ));

    // there would be 1 tasks, 1 for each task group, but task group 1 only has closed shards, so no task is created
    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("sequenceName-0"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpointsGroup0))
            .times(1);

    List<Task> postMergeTasks = postMergeCaptured.getValues();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(postMergeTasks).anyTimes();
    for (Task task : postMergeTasks) {
      EasyMock.expect(taskStorage.getStatus(task.getId()))
              .andReturn(Optional.of(TaskStatus.running(task.getId())))
              .anyTimes();
      EasyMock.expect(taskStorage.getTask(task.getId())).andReturn(Optional.of(task)).anyTimes();
    }
    EasyMock.replay(taskStorage);
    EasyMock.replay(taskClient);

    supervisor.runInternal();
    verifyAll();

    // Check that shardId-000000000000 which has hit EOS is not included in the sequences sent to the task for group 0
    SeekableStreamStartSequenceNumbers<String, String> group0ExpectedStartSequenceNumbers =
        new SeekableStreamStartSequenceNumbers<>(
            STREAM,
            ImmutableMap.of(
                SHARD_ID2, "0"
            ),
            ImmutableSet.of()
        );

    SeekableStreamEndSequenceNumbers<String, String> group0ExpectedEndSequenceNumbers =
        new SeekableStreamEndSequenceNumbers<>(
            STREAM,
            ImmutableMap.of(
                SHARD_ID2, KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER
            )
        );

    Assert.assertEquals(1, postMergeTasks.size());
    KinesisIndexTaskIOConfig group0Config = ((KinesisIndexTask) postMergeTasks.get(0)).getIOConfig();
    Assert.assertEquals((Integer) 0, group0Config.getTaskGroupId());
    Assert.assertEquals(group0ExpectedStartSequenceNumbers, group0Config.getStartSequenceNumbers());
    Assert.assertEquals(group0ExpectedEndSequenceNumbers, group0Config.getEndSequenceNumbers());

    return postMergeTasks;
  }

  /**
   * Test task creation after a shard merge with two closed shards and one open shard, with the closed shards
   * expiring and no longer being returned from record supplier.
   *
   * @param phaseTwoTasks List of tasks from the second phase where closed but not expired shards were present.
   */
  private void testShardMergePhaseThree(List<Task> phaseTwoTasks) throws Exception
  {
    EasyMock.reset(indexerMetadataStorageCoordinator);
    EasyMock.reset(taskStorage);
    EasyMock.reset(taskQueue);
    EasyMock.reset(taskClient);
    EasyMock.reset(taskMaster);
    EasyMock.reset(taskRunner);
    EasyMock.reset(supervisorRecordSupplier);

    // second set of tasks ran, shard 0 has expired, but shard 1 and 2 have data
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
            new SeekableStreamEndSequenceNumbers<String, String>(
                STREAM,
                ImmutableMap.of(
                    SHARD_ID0, KinesisSequenceNumber.END_OF_SHARD_MARKER,
                    SHARD_ID1, KinesisSequenceNumber.END_OF_SHARD_MARKER,
                    SHARD_ID2, "100"
                )
            )
        )
    ).anyTimes();

    EasyMock.expect(
        indexerMetadataStorageCoordinator.resetDataSourceMetadata(
            DATASOURCE,
            new KinesisDataSourceMetadata(
                new SeekableStreamEndSequenceNumbers<String, String>(
                    STREAM,
                    ImmutableMap.of(
                        SHARD_ID0, KinesisSequenceNumber.EXPIRED_MARKER,
                        SHARD_ID1, KinesisSequenceNumber.EXPIRED_MARKER,
                        SHARD_ID2, "100"
                    )
                )
            )
        )
    ).andReturn(true).anyTimes();

    EasyMock.expect(supervisorRecordSupplier.getPartitionIds(STREAM))
            .andReturn(ImmutableSet.of(SHARD_ID2))
            .anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getAssignment())
            .andReturn(ImmutableSet.of(SHARD2_PARTITION))
            .anyTimes();

    supervisorRecordSupplier.seekToLatest(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();
    EasyMock.expect(supervisorRecordSupplier.getLatestSequenceNumber(new StreamPartition<>(STREAM, SHARD_ID2)))
            .andReturn("200").anyTimes();
    EasyMock.expect(supervisorRecordSupplier.isOffsetAvailable(EasyMock.anyObject(), EasyMock.anyObject()))
            .andReturn(true)
            .anyTimes();

    supervisorRecordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    Capture<Task> postSplitCaptured = Capture.newInstance(CaptureType.ALL);

    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(Collections.emptyList()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(taskClient.getStatusAsync(EasyMock.anyString()))
            .andReturn(Futures.immediateFuture(SeekableStreamIndexTaskRunner.Status.NOT_STARTED))
            .anyTimes();

    Task successfulTask0 = phaseTwoTasks.get(0);
    EasyMock.expect(taskStorage.getStatus(successfulTask0.getId()))
            .andReturn(Optional.of(TaskStatus.success(successfulTask0.getId())));
    EasyMock.expect(taskStorage.getTask(successfulTask0.getId())).andReturn(Optional.of(successfulTask0)).anyTimes();

    EasyMock.expect(taskClient.getStartTimeAsync(EasyMock.anyString()))
            .andReturn(Futures.immediateFuture(DateTimes.nowUtc()))
            .anyTimes();

    EasyMock.expect(taskQueue.add(EasyMock.capture(postSplitCaptured))).andReturn(true).times(1);

    replayAll();

    supervisor.runInternal();
    verifyAll();

    EasyMock.reset(taskStorage);
    EasyMock.reset(taskClient);

    EasyMock.expect(taskClient.getStatusAsync(EasyMock.anyString()))
            .andReturn(Futures.immediateFuture(SeekableStreamIndexTaskRunner.Status.NOT_STARTED))
            .anyTimes();
    EasyMock.expect(taskClient.getStartTimeAsync(EasyMock.anyString()))
            .andReturn(Futures.immediateFuture(DateTimes.nowUtc()))
            .anyTimes();
    TreeMap<Integer, Map<String, String>> checkpointsGroup0 = new TreeMap<>();
    checkpointsGroup0.put(0, ImmutableMap.of(
        SHARD_ID2, "100"
    ));

    // there would be 1 task, only task group 0 has a shard
    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("sequenceName-0"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpointsGroup0))
            .times(1);

    List<Task> postSplitTasks = postSplitCaptured.getValues();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(postSplitTasks).anyTimes();
    for (Task task : postSplitTasks) {
      EasyMock.expect(taskStorage.getStatus(task.getId()))
              .andReturn(Optional.of(TaskStatus.running(task.getId())))
              .anyTimes();
      EasyMock.expect(taskStorage.getTask(task.getId())).andReturn(Optional.of(task)).anyTimes();
    }
    EasyMock.replay(taskStorage);
    EasyMock.replay(taskClient);

    supervisor.runInternal();
    verifyAll();


    // Check that shardId-000000000000 which has hit EOS is not included in the sequences sent to the task for group 0
    SeekableStreamStartSequenceNumbers<String, String> group0ExpectedStartSequenceNumbers =
        new SeekableStreamStartSequenceNumbers<>(
            STREAM,
            ImmutableMap.of(
                SHARD_ID2, "100"
            ),
            ImmutableSet.of(SHARD_ID2)
        );

    SeekableStreamEndSequenceNumbers<String, String> group0ExpectedEndSequenceNumbers =
        new SeekableStreamEndSequenceNumbers<>(
            STREAM,
            ImmutableMap.of(
                SHARD_ID2, KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER
            )
        );

    Assert.assertEquals(1, postSplitTasks.size());
    KinesisIndexTaskIOConfig group0Config = ((KinesisIndexTask) postSplitTasks.get(0)).getIOConfig();
    Assert.assertEquals((Integer) 0, group0Config.getTaskGroupId());
    Assert.assertEquals(group0ExpectedStartSequenceNumbers, group0Config.getStartSequenceNumbers());
    Assert.assertEquals(group0ExpectedEndSequenceNumbers, group0Config.getEndSequenceNumbers());

    Map<Integer, Set<String>> expectedPartitionGroups = ImmutableMap.of(
        0, ImmutableSet.of(SHARD_ID2),
        1, ImmutableSet.of()
    );
    ConcurrentHashMap<String, String> expectedPartitionOffsets = new ConcurrentHashMap<>(
        ImmutableMap.of(
            SHARD_ID2, "-1",
            SHARD_ID1, "-1",
            SHARD_ID0, "-1"
        )
    );
    Assert.assertEquals(expectedPartitionGroups, supervisor.getPartitionGroups());
    Assert.assertEquals(expectedPartitionOffsets, supervisor.getPartitionOffsets());
  }

  private TestableKinesisSupervisor getTestableSupervisor(
      int replicas,
      int taskCount,
      boolean useEarliestOffset,
      String duration,
      Period lateMessageRejectionPeriod,
      Period earlyMessageRejectionPeriod,
      boolean suspended
  )
  {
    return getTestableSupervisor(
        replicas,
        taskCount,
        useEarliestOffset,
        false,
        duration,
        lateMessageRejectionPeriod,
        earlyMessageRejectionPeriod,
        suspended
    );
  }

  private TestableKinesisSupervisor getTestableSupervisor(
      int replicas,
      int taskCount,
      boolean useEarliestOffset,
      boolean resetOffsetAutomatically,
      String duration,
      Period lateMessageRejectionPeriod,
      Period earlyMessageRejectionPeriod,
      boolean suspended
  )
  {
    KinesisSupervisorIOConfig kinesisSupervisorIOConfig = new KinesisSupervisorIOConfig(
        STREAM,
        INPUT_FORMAT,
        "awsEndpoint",
        null,
        replicas,
        taskCount,
        new Period(duration),
        new Period("P1D"),
        new Period("PT30S"),
        useEarliestOffset,
        new Period("PT30M"),
        lateMessageRejectionPeriod,
        earlyMessageRejectionPeriod,
        null,
        null,
        null,
        null,
        null,
        null,
        false
    );

    KinesisIndexTaskClientFactory taskClientFactory = new KinesisIndexTaskClientFactory(
        null,
        null
    )
    {
      @Override
      public SeekableStreamIndexTaskClient<String, String> build(
          String dataSource,
          TaskInfoProvider taskInfoProvider,
          int maxNumTasks,
          SeekableStreamSupervisorTuningConfig tuningConfig
      )
      {
        Assert.assertEquals(replicas * taskCount, maxNumTasks);
        Assert.assertEquals(TEST_HTTP_TIMEOUT.toStandardDuration(), tuningConfig.getHttpTimeout());
        Assert.assertEquals(TEST_CHAT_RETRIES, (long) tuningConfig.getChatRetries());
        return taskClient;
      }
    };

    final KinesisSupervisorTuningConfig tuningConfig = new KinesisSupervisorTuningConfig(
        null,
        1000,
        null,
        null,
        50000,
        null,
        new Period("P1Y"),
        null,
        null,
        null,
        false,
        null,
        resetOffsetAutomatically,
        null,
        null,
        numThreads,
        TEST_CHAT_RETRIES,
        TEST_HTTP_TIMEOUT,
        TEST_SHUTDOWN_TIMEOUT,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null
    );

    return new TestableKinesisSupervisor(
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        taskClientFactory,
        OBJECT_MAPPER,
        new KinesisSupervisorSpec(
            null,
            dataSchema,
            tuningConfig,
            kinesisSupervisorIOConfig,
            null,
            suspended,
            taskStorage,
            taskMaster,
            indexerMetadataStorageCoordinator,
            taskClientFactory,
            OBJECT_MAPPER,
            new NoopServiceEmitter(),
            new DruidMonitorSchedulerConfig(),
            rowIngestionMetersFactory,
            null,
            new SupervisorStateManagerConfig()
        ),
        rowIngestionMetersFactory
    );
  }

  private TestableKinesisSupervisor getTestableSupervisor(
      int replicas,
      int taskCount,
      boolean useEarliestOffset,
      String duration,
      Period lateMessageRejectionPeriod,
      Period earlyMessageRejectionPeriod
  )
  {
    return getTestableSupervisor(
        replicas,
        taskCount,
        useEarliestOffset,
        duration,
        lateMessageRejectionPeriod,
        earlyMessageRejectionPeriod,
        false,
        null,
        null
    );
  }

  private TestableKinesisSupervisor getTestableSupervisor(
      int replicas,
      int taskCount,
      boolean useEarliestOffset,
      String duration,
      Period lateMessageRejectionPeriod,
      Period earlyMessageRejectionPeriod,
      boolean suspended,
      Integer fetchDelayMillis,
      AutoScalerConfig autoScalerConfig
  )
  {
    KinesisSupervisorIOConfig kinesisSupervisorIOConfig = new KinesisSupervisorIOConfig(
        STREAM,
        INPUT_FORMAT,
        "awsEndpoint",
        null,
        replicas,
        taskCount,
        new Period(duration),
        new Period("P1D"),
        new Period("PT30S"),
        useEarliestOffset,
        new Period("PT30M"),
        lateMessageRejectionPeriod,
        earlyMessageRejectionPeriod,
        null,
        null,
        fetchDelayMillis,
        null,
        null,
         autoScalerConfig,
        false
    );

    KinesisIndexTaskClientFactory taskClientFactory = new KinesisIndexTaskClientFactory(
        null,
        null
    )
    {
      @Override
      public SeekableStreamIndexTaskClient<String, String> build(
          String dataSource,
          TaskInfoProvider taskInfoProvider,
          int maxNumTasks,
          SeekableStreamSupervisorTuningConfig tuningConfig
      )
      {
        Assert.assertEquals(
            replicas * (autoScalerConfig != null ? autoScalerConfig.getTaskCountMax() : taskCount),
            maxNumTasks
        );
        Assert.assertEquals(TEST_HTTP_TIMEOUT.toStandardDuration(), tuningConfig.getHttpTimeout());
        Assert.assertEquals(TEST_CHAT_RETRIES, (long) tuningConfig.getChatRetries());
        return taskClient;
      }
    };

    return new TestableKinesisSupervisor(
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        taskClientFactory,
        OBJECT_MAPPER,
        new KinesisSupervisorSpec(
            null,
            dataSchema,
            tuningConfig,
            kinesisSupervisorIOConfig,
            null,
            suspended,
            taskStorage,
            taskMaster,
            indexerMetadataStorageCoordinator,
            taskClientFactory,
            OBJECT_MAPPER,
            new NoopServiceEmitter(),
            new DruidMonitorSchedulerConfig(),
            rowIngestionMetersFactory,
            null,
            supervisorConfig
        ),
        rowIngestionMetersFactory
    );
  }

  /**
   * Use when you want to mock the return value of SeekableStreamSupervisor#isTaskCurrent()
   */
  private TestableKinesisSupervisor getTestableSupervisorCustomIsTaskCurrent(
      int replicas,
      int taskCount,
      boolean useEarliestOffset,
      String duration,
      Period lateMessageRejectionPeriod,
      Period earlyMessageRejectionPeriod,
      boolean suspended,
      Integer fetchDelayMillis,
      boolean isTaskCurrentReturn
  )
  {
    KinesisSupervisorIOConfig kinesisSupervisorIOConfig = new KinesisSupervisorIOConfig(
        STREAM,
        INPUT_FORMAT,
        "awsEndpoint",
        null,
        replicas,
        taskCount,
        new Period(duration),
        new Period("P1D"),
        new Period("PT30S"),
        useEarliestOffset,
        new Period("PT30M"),
        lateMessageRejectionPeriod,
        earlyMessageRejectionPeriod,
        null,
        null,
        fetchDelayMillis,
        null,
        null,
        null,
        false
    );

    KinesisIndexTaskClientFactory taskClientFactory = new KinesisIndexTaskClientFactory(
        null,
        null
    )
    {
      @Override
      public SeekableStreamIndexTaskClient<String, String> build(
          String dataSource,
          TaskInfoProvider taskInfoProvider,
          int maxNumTasks,
          SeekableStreamSupervisorTuningConfig tuningConfig
      )
      {
        Assert.assertEquals(replicas * taskCount, maxNumTasks);
        Assert.assertEquals(TEST_HTTP_TIMEOUT.toStandardDuration(), tuningConfig.getHttpTimeout());
        Assert.assertEquals(TEST_CHAT_RETRIES, (long) tuningConfig.getChatRetries());
        return taskClient;
      }
    };

    return new TestableKinesisSupervisorWithCustomIsTaskCurrent(
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        taskClientFactory,
        OBJECT_MAPPER,
        new KinesisSupervisorSpec(
            null,
            dataSchema,
            tuningConfig,
            kinesisSupervisorIOConfig,
            null,
            suspended,
            taskStorage,
            taskMaster,
            indexerMetadataStorageCoordinator,
            taskClientFactory,
            OBJECT_MAPPER,
            new NoopServiceEmitter(),
            new DruidMonitorSchedulerConfig(),
            rowIngestionMetersFactory,
            null,
            supervisorConfig
        ),
        rowIngestionMetersFactory,
        isTaskCurrentReturn
    );
  }

  /**
   * Use for tests where you don't want generateSequenceName to be overridden out
   */
  private KinesisSupervisor createSupervisor(
      int replicas,
      int taskCount,
      boolean useEarliestOffset,
      String duration,
      Period lateMessageRejectionPeriod,
      Period earlyMessageRejectionPeriod,
      boolean suspended,
      Integer fetchDelayMillis,
      DataSchema dataSchema,
      KinesisSupervisorTuningConfig tuningConfig
  )
  {
    KinesisSupervisorIOConfig kinesisSupervisorIOConfig = new KinesisSupervisorIOConfig(
        STREAM,
        INPUT_FORMAT,
        "awsEndpoint",
        null,
        replicas,
        taskCount,
        new Period(duration),
        new Period("P1D"),
        new Period("PT30S"),
        useEarliestOffset,
        new Period("PT30M"),
        lateMessageRejectionPeriod,
        earlyMessageRejectionPeriod,
        null,
        null,
        fetchDelayMillis,
        null,
        null,
        null,
        false
    );

    KinesisIndexTaskClientFactory taskClientFactory = new KinesisIndexTaskClientFactory(
        null,
        null
    )
    {
      @Override
      public SeekableStreamIndexTaskClient<String, String> build(
          String dataSource,
          TaskInfoProvider taskInfoProvider,
          int maxNumTasks,
          SeekableStreamSupervisorTuningConfig tuningConfig
      )
      {
        Assert.assertEquals(replicas * taskCount, maxNumTasks);
        Assert.assertEquals(TEST_HTTP_TIMEOUT.toStandardDuration(), tuningConfig.getHttpTimeout());
        Assert.assertEquals(TEST_CHAT_RETRIES, (long) tuningConfig.getChatRetries());
        return taskClient;
      }
    };

    return new KinesisSupervisor(
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        taskClientFactory,
        OBJECT_MAPPER,
        new KinesisSupervisorSpec(
            null,
            dataSchema,
            tuningConfig,
            kinesisSupervisorIOConfig,
            null,
            suspended,
            taskStorage,
            taskMaster,
            indexerMetadataStorageCoordinator,
            taskClientFactory,
            OBJECT_MAPPER,
            new NoopServiceEmitter(),
            new DruidMonitorSchedulerConfig(),
            rowIngestionMetersFactory,
            null,
            supervisorConfig
        ),
        rowIngestionMetersFactory,
        null
    );
  }

  private static DataSchema getDataSchema(String dataSource)
  {
    List<DimensionSchema> dimensions = new ArrayList<>();
    dimensions.add(StringDimensionSchema.create("dim1"));
    dimensions.add(StringDimensionSchema.create("dim2"));

    return new DataSchema(
        dataSource,
        new TimestampSpec("timestamp", "iso", null),
        new DimensionsSpec(dimensions),
        new AggregatorFactory[]{new CountAggregatorFactory("rows")},
        new UniformGranularitySpec(
            Granularities.HOUR,
            Granularities.NONE,
            ImmutableList.of()
        ),
        null
    );
  }


  private KinesisIndexTask createKinesisIndexTask(
      String id,
      String dataSource,
      int taskGroupId,
      SeekableStreamStartSequenceNumbers<String, String> startPartitions,
      SeekableStreamEndSequenceNumbers<String, String> endPartitions,
      DateTime minimumMessageTime,
      DateTime maximumMessageTime
  )
  {
    return createKinesisIndexTask(
        id,
        taskGroupId,
        startPartitions,
        endPartitions,
        minimumMessageTime,
        maximumMessageTime,
        getDataSchema(dataSource)
    );
  }

  private KinesisIndexTask createKinesisIndexTask(
      String id,
      int taskGroupId,
      SeekableStreamStartSequenceNumbers<String, String> startPartitions,
      SeekableStreamEndSequenceNumbers<String, String> endPartitions,
      DateTime minimumMessageTime,
      DateTime maximumMessageTime,
      DataSchema dataSchema
  )
  {
    return createKinesisIndexTask(
        id,
        taskGroupId,
        startPartitions,
        endPartitions,
        minimumMessageTime,
        maximumMessageTime,
        dataSchema,
        (KinesisIndexTaskTuningConfig) tuningConfig.convertToTaskTuningConfig()
    );
  }

  private KinesisIndexTask createKinesisIndexTask(
      String id,
      int taskGroupId,
      SeekableStreamStartSequenceNumbers<String, String> startPartitions,
      SeekableStreamEndSequenceNumbers<String, String> endPartitions,
      DateTime minimumMessageTime,
      DateTime maximumMessageTime,
      DataSchema dataSchema,
      KinesisIndexTaskTuningConfig tuningConfig
  )
  {
    return new KinesisIndexTask(
        id,
        null,
        dataSchema,
        tuningConfig,
        new KinesisIndexTaskIOConfig(
            0,
            "sequenceName-" + taskGroupId,
            startPartitions,
            endPartitions,
            true,
            minimumMessageTime,
            maximumMessageTime,
            INPUT_FORMAT,
            "awsEndpoint",
            null,
            null,
            null
        ),
        Collections.emptyMap(),
        false,
        null
    );
  }

  private static class TestTaskRunnerWorkItem extends TaskRunnerWorkItem
  {
    private final String taskType;
    private final TaskLocation location;
    private final String dataSource;

    public TestTaskRunnerWorkItem(Task task, ListenableFuture<TaskStatus> result, TaskLocation location)
    {
      super(task.getId(), result);
      this.taskType = task.getType();
      this.location = location;
      this.dataSource = task.getDataSource();
    }

    @Override
    public TaskLocation getLocation()
    {
      return location;
    }

    @Override
    public String getTaskType()
    {
      return taskType;
    }

    @Override
    public String getDataSource()
    {
      return dataSource;
    }

  }

  private class TestableKinesisSupervisor extends KinesisSupervisor
  {
    private KinesisSupervisorSpec spec;

    TestableKinesisSupervisor(
        TaskStorage taskStorage,
        TaskMaster taskMaster,
        IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator,
        KinesisIndexTaskClientFactory taskClientFactory,
        ObjectMapper mapper,
        KinesisSupervisorSpec spec,
        RowIngestionMetersFactory rowIngestionMetersFactory
    )
    {
      super(
          taskStorage,
          taskMaster,
          indexerMetadataStorageCoordinator,
          taskClientFactory,
          mapper,
          spec,
          rowIngestionMetersFactory,
          null
      );
      this.spec = spec;
    }

    protected KinesisSupervisorSpec getKinesisSupervisorSpec()
    {
      return spec;
    }

    @Override
    public String generateSequenceName(
        Map<String, String> startPartitions,
        Optional<DateTime> minimumMessageTime,
        Optional<DateTime> maximumMessageTime,
        DataSchema dataSchema,
        SeekableStreamIndexTaskTuningConfig tuningConfig
    )
    {
      if (startPartitions.isEmpty()) {
        return StringUtils.format("sequenceName-NOT-USED");
      }
      final int groupId = getTaskGroupIdForPartition(startPartitions.keySet().iterator().next());
      return StringUtils.format("sequenceName-%d", groupId);
    }

    @Override
    protected RecordSupplier<String, String, ByteEntity> setupRecordSupplier()
    {
      return supervisorRecordSupplier;
    }

    private SeekableStreamSupervisorStateManager getStateManager()
    {
      return stateManager;
    }
  }

  private class TestableKinesisSupervisorWithCustomIsTaskCurrent extends TestableKinesisSupervisor
  {
    private final boolean isTaskCurrentReturn;

    TestableKinesisSupervisorWithCustomIsTaskCurrent(
        TaskStorage taskStorage,
        TaskMaster taskMaster,
        IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator,
        KinesisIndexTaskClientFactory taskClientFactory,
        ObjectMapper mapper,
        KinesisSupervisorSpec spec,
        RowIngestionMetersFactory rowIngestionMetersFactory,
        boolean isTaskCurrentReturn
    )
    {
      super(
          taskStorage,
          taskMaster,
          indexerMetadataStorageCoordinator,
          taskClientFactory,
          mapper,
          spec,
          rowIngestionMetersFactory
      );
      this.isTaskCurrentReturn = isTaskCurrentReturn;
    }

    @Override
    public boolean isTaskCurrent(int taskGroupId, String taskId, Map<String, Task> taskMap)
    {
      return isTaskCurrentReturn;
    }
  }
}
