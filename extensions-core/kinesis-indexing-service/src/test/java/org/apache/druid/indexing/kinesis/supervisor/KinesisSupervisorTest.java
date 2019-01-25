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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JSONParseSpec;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskInfoProvider;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.common.stats.RowIngestionMetersFactory;
import org.apache.druid.indexing.common.task.RealtimeIndexTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.kinesis.KinesisDataSourceMetadata;
import org.apache.druid.indexing.kinesis.KinesisIndexTask;
import org.apache.druid.indexing.kinesis.KinesisIndexTaskClient;
import org.apache.druid.indexing.kinesis.KinesisIndexTaskClientFactory;
import org.apache.druid.indexing.kinesis.KinesisIndexTaskIOConfig;
import org.apache.druid.indexing.kinesis.KinesisRecordSupplier;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskQueue;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.indexing.overlord.TaskRunnerListener;
import org.apache.druid.indexing.overlord.TaskRunnerWorkItem;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.supervisor.SupervisorReport;
import org.apache.druid.indexing.seekablestream.SeekableStreamPartitions;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.indexing.seekablestream.common.StreamPartition;
import org.apache.druid.indexing.seekablestream.supervisor.TaskReportData;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.RealtimeIOConfig;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.segment.realtime.FireDepartment;
import org.apache.druid.server.metrics.DruidMonitorSchedulerConfig;
import org.apache.druid.server.metrics.ExceptionCapturingServiceEmitter;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.easymock.Capture;
import org.easymock.CaptureType;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.easymock.IAnswer;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;

import static org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskRunner.Status;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;

public class KinesisSupervisorTest extends EasyMockSupport
{


  private static final ObjectMapper objectMapper = TestHelper.makeJsonMapper();
  private static final String DATASOURCE = "testDS";
  private static final int TEST_CHAT_THREADS = 3;
  private static final long TEST_CHAT_RETRIES = 9L;
  private static final Period TEST_HTTP_TIMEOUT = new Period("PT10S");
  private static final Period TEST_SHUTDOWN_TIMEOUT = new Period("PT80S");
  private static final String stream = "stream";
  private static String shardId1 = "1";
  private static String shardId0 = "0";
  private static StreamPartition<String> shard1Partition = StreamPartition.of(stream, shardId1);
  private static StreamPartition<String> shard0Partition = StreamPartition.of(stream, shardId0);

  private static DataSchema dataSchema;
  private KinesisRecordSupplier supervisorRecordSupplier;

  private final int numThreads;
  private KinesisSupervisor supervisor;
  private KinesisSupervisorTuningConfig tuningConfig;
  private TaskStorage taskStorage;
  private TaskMaster taskMaster;
  private TaskRunner taskRunner;
  private IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator;
  private KinesisIndexTaskClient taskClient;
  private TaskQueue taskQueue;
  private RowIngestionMetersFactory rowIngestionMetersFactory;
  private ExceptionCapturingServiceEmitter serviceEmitter;

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
    taskClient = createMock(KinesisIndexTaskClient.class);
    taskQueue = createMock(TaskQueue.class);
    supervisorRecordSupplier = createMock(KinesisRecordSupplier.class);

    tuningConfig = new KinesisSupervisorTuningConfig(
        1000,
        null,
        50000,
        null,
        new Period("P1Y"),
        new File("/test"),
        null,
        null,
        true,
        false,
        null,
        null,
        null,
        null,
        numThreads,
        TEST_CHAT_THREADS,
        TEST_CHAT_RETRIES,
        TEST_HTTP_TIMEOUT,
        TEST_SHUTDOWN_TIMEOUT,
        null,
        null,
        null,
        5000,
        null,
        null,
        null,
        null,
        null,
        null
    );
    rowIngestionMetersFactory = new TestUtils().getRowIngestionMetersFactory();
    serviceEmitter = new ExceptionCapturingServiceEmitter();
    EmittingLogger.registerEmitter(serviceEmitter);
  }

  @After
  public void tearDownTest()
  {
    supervisor = null;
  }


  @Test
  public void testNoInitialState() throws Exception
  {
    supervisor = getSupervisor(1, 1, true, "PT1H", null, null);

    supervisorRecordSupplier.assign(anyObject());
    expectLastCall().anyTimes();
    expect(supervisorRecordSupplier.getPartitionIds(stream)).andReturn(ImmutableSet.of(shardId1, shardId0)).anyTimes();
    expect(supervisorRecordSupplier.getAssignment()).andReturn(ImmutableSet.of(shard1Partition, shard0Partition))
                                                    .anyTimes();
    supervisorRecordSupplier.seekToLatest(anyObject());
    expectLastCall().anyTimes();
    expect(supervisorRecordSupplier.getEarliestSequenceNumber(anyObject())).andReturn("0").anyTimes();
    supervisorRecordSupplier.seek(anyObject(), anyString());
    expectLastCall().anyTimes();

    Capture<KinesisIndexTask> captured = Capture.newInstance();
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
            null
        )
    ).anyTimes();
    expect(taskQueue.add(EasyMock.capture(captured))).andReturn(true);
    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));
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

    Assert.assertEquals(stream, taskConfig.getStartPartitions().getStream());
    Assert.assertEquals(
        "0",
        taskConfig.getStartPartitions().getPartitionSequenceNumberMap().get(shardId1)
    );
    Assert.assertEquals(
        "0",
        taskConfig.getStartPartitions().getPartitionSequenceNumberMap().get(shardId0)
    );

    Assert.assertEquals(stream, taskConfig.getEndPartitions().getStream());
    Assert.assertEquals(
        SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER,
        taskConfig.getEndPartitions().getPartitionSequenceNumberMap().get(shardId1)
    );
    Assert.assertEquals(
        SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER,
        taskConfig.getEndPartitions().getPartitionSequenceNumberMap().get(shardId0)
    );
  }


  @Test
  public void testMultiTask() throws Exception
  {
    supervisor = getSupervisor(1, 2, true, "PT1H", null, null);

    supervisorRecordSupplier.assign(anyObject());
    expectLastCall().anyTimes();
    expect(supervisorRecordSupplier.getPartitionIds(stream)).andReturn(ImmutableSet.of(shardId1, shardId0)).anyTimes();
    expect(supervisorRecordSupplier.getAssignment()).andReturn(ImmutableSet.of(shard1Partition, shard0Partition))
                                                    .anyTimes();
    supervisorRecordSupplier.seekToLatest(anyObject());
    expectLastCall().anyTimes();
    expect(supervisorRecordSupplier.getEarliestSequenceNumber(anyObject())).andReturn("0").anyTimes();
    supervisorRecordSupplier.seek(anyObject(), anyString());
    expectLastCall().anyTimes();

    Capture<KinesisIndexTask> captured = Capture.newInstance(CaptureType.ALL);
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
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
    Assert.assertEquals(1, task1.getIOConfig().getStartPartitions().getPartitionSequenceNumberMap().size());
    Assert.assertEquals(1, task1.getIOConfig().getEndPartitions().getPartitionSequenceNumberMap().size());
    Assert.assertEquals(
        "0",
        task1.getIOConfig().getStartPartitions().getPartitionSequenceNumberMap().get(shardId1)
    );
    Assert.assertEquals(
        SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER,
        task1.getIOConfig().getEndPartitions().getPartitionSequenceNumberMap().get(shardId1)
    );

    KinesisIndexTask task2 = captured.getValues().get(1);
    Assert.assertEquals(1, task2.getIOConfig().getStartPartitions().getPartitionSequenceNumberMap().size());
    Assert.assertEquals(1, task2.getIOConfig().getEndPartitions().getPartitionSequenceNumberMap().size());
    Assert.assertEquals(
        "0",
        task2.getIOConfig().getStartPartitions().getPartitionSequenceNumberMap().get(shardId0)
    );
    Assert.assertEquals(
        SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER,
        task2.getIOConfig().getEndPartitions().getPartitionSequenceNumberMap().get(shardId0)
    );
  }

  @Test
  public void testReplicas() throws Exception
  {
    supervisor = getSupervisor(2, 1, true, "PT1H", null, null);

    supervisorRecordSupplier.assign(anyObject());
    expectLastCall().anyTimes();
    expect(supervisorRecordSupplier.getPartitionIds(stream)).andReturn(ImmutableSet.of(shardId1, shardId0)).anyTimes();
    expect(supervisorRecordSupplier.getAssignment()).andReturn(ImmutableSet.of(shard1Partition, shard0Partition))
                                                    .anyTimes();
    supervisorRecordSupplier.seekToLatest(anyObject());
    expectLastCall().anyTimes();
    expect(supervisorRecordSupplier.getEarliestSequenceNumber(anyObject())).andReturn("0").anyTimes();
    supervisorRecordSupplier.seek(anyObject(), anyString());
    expectLastCall().anyTimes();

    Capture<KinesisIndexTask> captured = Capture.newInstance(CaptureType.ALL);
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
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
    Assert.assertEquals(2, task1.getIOConfig().getStartPartitions().getPartitionSequenceNumberMap().size());
    Assert.assertEquals(2, task1.getIOConfig().getEndPartitions().getPartitionSequenceNumberMap().size());
    Assert.assertEquals(
        "0",
        task1.getIOConfig().getStartPartitions().getPartitionSequenceNumberMap().get(shardId0)
    );
    Assert.assertEquals(
        SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER,
        task1.getIOConfig().getEndPartitions().getPartitionSequenceNumberMap().get(shardId0)
    );
    Assert.assertEquals(
        "0",
        task1.getIOConfig().getStartPartitions().getPartitionSequenceNumberMap().get(shardId1)
    );
    Assert.assertEquals(
        SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER,
        task1.getIOConfig().getEndPartitions().getPartitionSequenceNumberMap().get(shardId1)
    );

    KinesisIndexTask task2 = captured.getValues().get(1);
    Assert.assertEquals(2, task2.getIOConfig().getStartPartitions().getPartitionSequenceNumberMap().size());
    Assert.assertEquals(2, task2.getIOConfig().getEndPartitions().getPartitionSequenceNumberMap().size());
    Assert.assertEquals(
        "0",
        task2.getIOConfig().getStartPartitions().getPartitionSequenceNumberMap().get(shardId0)
    );
    Assert.assertEquals(
        SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER,
        task2.getIOConfig().getEndPartitions().getPartitionSequenceNumberMap().get(shardId0)
    );
    Assert.assertEquals(
        "0",
        task2.getIOConfig().getStartPartitions().getPartitionSequenceNumberMap().get(shardId1)
    );
    Assert.assertEquals(
        SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER,
        task2.getIOConfig().getEndPartitions().getPartitionSequenceNumberMap().get(shardId1)
    );

  }

  @Test
  public void testLateMessageRejectionPeriod() throws Exception
  {
    supervisor = getSupervisor(2, 1, true, "PT1H", new Period("PT1H"), null);

    supervisorRecordSupplier.assign(anyObject());
    expectLastCall().anyTimes();
    expect(supervisorRecordSupplier.getPartitionIds(stream)).andReturn(ImmutableSet.of(shardId1, shardId0)).anyTimes();
    expect(supervisorRecordSupplier.getAssignment()).andReturn(ImmutableSet.of(shard1Partition, shard0Partition))
                                                    .anyTimes();
    supervisorRecordSupplier.seekToLatest(anyObject());
    expectLastCall().anyTimes();
    expect(supervisorRecordSupplier.getEarliestSequenceNumber(anyObject())).andReturn("0").anyTimes();
    supervisorRecordSupplier.seek(anyObject(), anyString());
    expectLastCall().anyTimes();

    Capture<KinesisIndexTask> captured = Capture.newInstance(CaptureType.ALL);
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
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
    supervisor = getSupervisor(2, 1, true, "PT1H", null, new Period("PT1H"));

    supervisorRecordSupplier.assign(anyObject());
    expectLastCall().anyTimes();
    expect(supervisorRecordSupplier.getPartitionIds(stream)).andReturn(ImmutableSet.of(shardId1, shardId0)).anyTimes();
    expect(supervisorRecordSupplier.getAssignment()).andReturn(ImmutableSet.of(shard1Partition, shard0Partition))
                                                    .anyTimes();
    supervisorRecordSupplier.seekToLatest(anyObject());
    expectLastCall().anyTimes();
    expect(supervisorRecordSupplier.getEarliestSequenceNumber(anyObject())).andReturn("0").anyTimes();
    supervisorRecordSupplier.seek(anyObject(), anyString());
    expectLastCall().anyTimes();

    Capture<KinesisIndexTask> captured = Capture.newInstance(CaptureType.ALL);
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
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


  @Test
  /**
   * Test generating the starting sequences from the partition data stored in druid_dataSource which contains the
   * sequences of the last built segments.
   */
  public void testDatasourceMetadata() throws Exception
  {
    supervisor = getSupervisor(1, 1, true, "PT1H", null, null);

    supervisorRecordSupplier.assign(anyObject());
    expectLastCall().anyTimes();
    expect(supervisorRecordSupplier.getPartitionIds(stream)).andReturn(ImmutableSet.of(shardId1, shardId0)).anyTimes();
    expect(supervisorRecordSupplier.getAssignment()).andReturn(ImmutableSet.of(shard1Partition, shard0Partition))
                                                    .anyTimes();
    supervisorRecordSupplier.seekToLatest(anyObject());
    expectLastCall().anyTimes();
    expect(supervisorRecordSupplier.getEarliestSequenceNumber(shard1Partition)).andReturn("2").anyTimes();
    expect(supervisorRecordSupplier.getEarliestSequenceNumber(shard0Partition)).andReturn("1").anyTimes();
    expect(supervisorRecordSupplier.getLatestSequenceNumber(anyObject())).andReturn("100").anyTimes();
    supervisorRecordSupplier.seek(anyObject(), anyString());
    expectLastCall().anyTimes();

    Capture<KinesisIndexTask> captured = Capture.newInstance();
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
            new SeekableStreamPartitions<>(stream, ImmutableMap.of(
                shardId1,
                "2",
                shardId0,
                "1"
            ))
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
        taskConfig.getStartPartitions().getPartitionSequenceNumberMap().get(shardId1)
    );
    Assert.assertEquals(
        "1",
        taskConfig.getStartPartitions().getPartitionSequenceNumberMap().get(shardId0)
    );
  }

  @Test(expected = ISE.class)
  public void testBadMetadataOffsets() throws Exception
  {
    supervisor = getSupervisor(1, 1, true, "PT1H", null, null);

    supervisorRecordSupplier.assign(anyObject());
    expectLastCall().anyTimes();
    expect(supervisorRecordSupplier.getPartitionIds(stream)).andReturn(ImmutableSet.of(shardId1, shardId0)).anyTimes();
    expect(supervisorRecordSupplier.getAssignment()).andReturn(ImmutableSet.of(shard1Partition, shard0Partition))
                                                    .anyTimes();
    supervisorRecordSupplier.seekToLatest(anyObject());
    expectLastCall().anyTimes();
    expect(supervisorRecordSupplier.getEarliestSequenceNumber(anyObject())).andReturn("0").anyTimes();
    expect(supervisorRecordSupplier.getLatestSequenceNumber(anyObject())).andReturn("100").anyTimes();
    supervisorRecordSupplier.seek(anyObject(), anyString());
    expectLastCall().anyTimes();


    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
            new SeekableStreamPartitions<>(stream, ImmutableMap.of(
                shardId1,
                "101",
                shardId0,
                "-1"
            ))
        )
    ).anyTimes();
    replayAll();

    supervisor.start();
    supervisor.runInternal();
  }

  @Test
  public void testKillIncompatibleTasks() throws Exception
  {
    supervisor = getSupervisor(2, 1, true, "PT1H", null, null);

    supervisorRecordSupplier.assign(anyObject());
    expectLastCall().anyTimes();
    expect(supervisorRecordSupplier.getPartitionIds(stream)).andReturn(ImmutableSet.of(shardId1, shardId0)).anyTimes();
    expect(supervisorRecordSupplier.getAssignment()).andReturn(ImmutableSet.of(shard1Partition, shard0Partition))
                                                    .anyTimes();
    supervisorRecordSupplier.seekToLatest(anyObject());
    expectLastCall().anyTimes();
    expect(supervisorRecordSupplier.getEarliestSequenceNumber(anyObject())).andReturn("0").anyTimes();
    expect(supervisorRecordSupplier.getLatestSequenceNumber(anyObject())).andReturn("100").anyTimes();
    supervisorRecordSupplier.seek(anyObject(), anyString());
    expectLastCall().anyTimes();

    // unexpected # of partitions (kill)
    Task id1 = createKinesisIndexTask(
        "id1",
        DATASOURCE,
        1,
        new SeekableStreamPartitions<>(stream, ImmutableMap.of(
            shardId0,
            "0"
        )),
        new SeekableStreamPartitions<>(stream, ImmutableMap.of(
            shardId0,
            "1"
        )),
        null,
        null
    );

    // correct number of partitions and ranges (don't kill)
    Task id2 = createKinesisIndexTask(
        "id2",
        DATASOURCE,
        0,
        new SeekableStreamPartitions<>(stream, ImmutableMap.of(
            shardId0,
            "0",
            shardId1,
            "0"
        )),
        new SeekableStreamPartitions<>(stream, ImmutableMap.of(
            shardId0,
            "1",
            shardId1,
            "12"
        )),
        null,
        null
    );

    // unexpected range on partition 2 (kill)
    Task id3 = createKinesisIndexTask(
        "id3",
        DATASOURCE,
        1,
        new SeekableStreamPartitions<>(stream, ImmutableMap.of(
            shardId0,
            "0",
            shardId1,
            "1"
        )),
        new SeekableStreamPartitions<>(stream, ImmutableMap.of(
            shardId0,
            "1",
            shardId1,
            "11"
        )),
        null,
        null
    );

    // different datasource (don't kill)
    Task id4 = createKinesisIndexTask(
        "id4",
        "other-datasource",
        2,
        new SeekableStreamPartitions<>(stream, ImmutableMap.of(
            shardId0,
            "0",
            shardId1,
            "0"
        )),
        new SeekableStreamPartitions<>(stream, ImmutableMap.of(
            shardId0,
            "1",
            shardId1,
            "12"
        )),
        null,
        null
    );

    // non KinesisIndexTask (don't kill)
    Task id5 = new RealtimeIndexTask(
        "id5",
        null,
        new FireDepartment(
            dataSchema,
            new RealtimeIOConfig(null, null, null),
            null
        ),
        null
    );

    List<Task> existingTasks = ImmutableList.of(id1, id2, id3, id4, id5);

    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(Collections.EMPTY_LIST).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasks()).andReturn(existingTasks).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id1")).andReturn(Optional.of(TaskStatus.running("id1"))).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id2")).andReturn(Optional.of(TaskStatus.running("id2"))).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id3")).andReturn(Optional.of(TaskStatus.running("id3"))).anyTimes();
    EasyMock.expect(taskStorage.getTask("id1")).andReturn(Optional.of(id1)).anyTimes();
    EasyMock.expect(taskStorage.getTask("id2")).andReturn(Optional.of(id2)).anyTimes();
    EasyMock.expect(taskStorage.getTask("id3")).andReturn(Optional.of(id3)).anyTimes();
    EasyMock.expect(taskClient.getStatusAsync(EasyMock.anyString()))
            .andReturn(Futures.immediateFuture(Status.NOT_STARTED))
            .anyTimes();
    EasyMock.expect(taskClient.getStartTimeAsync(EasyMock.anyString()))
            .andReturn(Futures.immediateFuture(DateTimes.nowUtc()))
            .anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
            null
        )
    ).anyTimes();
    EasyMock.expect(taskClient.stopAsync("id1", false)).andReturn(Futures.immediateFuture(true));
    EasyMock.expect(taskClient.stopAsync("id3", false)).andReturn(Futures.immediateFuture(false));
    taskRunner.registerListener(EasyMock.anyObject(TaskRunnerListener.class), EasyMock.anyObject(Executor.class));
    taskQueue.shutdown("id3", "Task [%s] failed to stop in a timely manner, killing task", "id3");

    EasyMock.expect(taskQueue.add(EasyMock.anyObject(Task.class))).andReturn(true);

    TreeMap<Integer, Map<String, String>> checkpoints = new TreeMap<>();
    checkpoints.put(0, ImmutableMap.of(
        shardId0,
        "0",
        shardId1,
        "0"
    ));

    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("id2"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpoints))
            .times(2);

    replayAll();
    supervisor.start();
    supervisor.runInternal();
    verifyAll();
  }

  @Test
  public void testKillBadPartitionAssignment() throws Exception
  {
    supervisor = getSupervisor(1, 2, true, "PT1H", null, null);

    supervisorRecordSupplier.assign(anyObject());
    expectLastCall().anyTimes();
    expect(supervisorRecordSupplier.getPartitionIds(stream)).andReturn(ImmutableSet.of(shardId1, shardId0)).anyTimes();
    expect(supervisorRecordSupplier.getAssignment()).andReturn(ImmutableSet.of(shard1Partition, shard0Partition))
                                                    .anyTimes();
    supervisorRecordSupplier.seekToLatest(anyObject());
    expectLastCall().anyTimes();
    expect(supervisorRecordSupplier.getEarliestSequenceNumber(anyObject())).andReturn("0").anyTimes();
    expect(supervisorRecordSupplier.getLatestSequenceNumber(anyObject())).andReturn("100").anyTimes();
    supervisorRecordSupplier.seek(anyObject(), anyString());
    expectLastCall().anyTimes();

    Task id1 = createKinesisIndexTask(
        "id1",
        DATASOURCE,
        0,
        new SeekableStreamPartitions<>(stream, ImmutableMap.of(
            shardId1,
            "0"
        )),
        new SeekableStreamPartitions<>(stream, ImmutableMap.of(
            shardId1,
            "12"
        )),
        null,
        null
    );
    Task id2 = createKinesisIndexTask(
        "id2",
        DATASOURCE,
        1,
        new SeekableStreamPartitions<>(stream, ImmutableMap.of(
            shardId0,
            "0"
        )),
        new SeekableStreamPartitions<>(stream, ImmutableMap.of(
            shardId0,
            "1"
        )),
        null,
        null
    );
    Task id3 = createKinesisIndexTask(
        "id3",
        DATASOURCE,
        0,
        new SeekableStreamPartitions<>(stream, ImmutableMap.of(
            shardId0,
            "0",
            shardId1,
            "0"
        )),
        new SeekableStreamPartitions<>(stream, ImmutableMap.of(
            shardId0,
            "1",
            shardId1,
            "12"
        )),
        null,
        null
    );
    Task id4 = createKinesisIndexTask(
        "id4",
        DATASOURCE,
        0,
        new SeekableStreamPartitions<>(stream, ImmutableMap.of(
            shardId0,
            "0"
        )),
        new SeekableStreamPartitions<>(stream, ImmutableMap.of(
            shardId0,
            "1"
        )),
        null,
        null
    );
    Task id5 = createKinesisIndexTask(
        "id5",
        DATASOURCE,
        0,
        new SeekableStreamPartitions<>(stream, ImmutableMap.of(
            shardId0,
            "0"
        )),
        new SeekableStreamPartitions<>(stream, ImmutableMap.of(
            shardId0,
            "1"
        )),
        null,
        null
    );

    List<Task> existingTasks = ImmutableList.of(id1, id2, id3, id4, id5);

    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(Collections.EMPTY_LIST).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasks()).andReturn(existingTasks).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id1")).andReturn(Optional.of(TaskStatus.running("id1"))).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id2")).andReturn(Optional.of(TaskStatus.running("id2"))).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id3")).andReturn(Optional.of(TaskStatus.running("id3"))).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id4")).andReturn(Optional.of(TaskStatus.running("id4"))).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id5")).andReturn(Optional.of(TaskStatus.running("id5"))).anyTimes();
    EasyMock.expect(taskStorage.getTask("id1")).andReturn(Optional.of(id1)).anyTimes();
    EasyMock.expect(taskStorage.getTask("id2")).andReturn(Optional.of(id2)).anyTimes();
    EasyMock.expect(taskStorage.getTask("id3")).andReturn(Optional.of(id3)).anyTimes();
    EasyMock.expect(taskStorage.getTask("id4")).andReturn(Optional.of(id4)).anyTimes();
    EasyMock.expect(taskStorage.getTask("id5")).andReturn(Optional.of(id5)).anyTimes();
    EasyMock.expect(taskClient.getStatusAsync(EasyMock.anyString()))
            .andReturn(Futures.immediateFuture(Status.NOT_STARTED))
            .anyTimes();
    EasyMock.expect(taskClient.getStartTimeAsync(EasyMock.anyString()))
            .andReturn(Futures.immediateFuture(DateTimes.nowUtc()))
            .anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
            null
        )
    ).anyTimes();
    EasyMock.expect(taskClient.stopAsync("id3", false)).andReturn(Futures.immediateFuture(true));
    EasyMock.expect(taskClient.stopAsync("id4", false)).andReturn(Futures.immediateFuture(false));
    EasyMock.expect(taskClient.stopAsync("id5", false)).andReturn(Futures.immediateFuture((Boolean) null));

    TreeMap<Integer, Map<String, String>> checkpoints1 = new TreeMap<>();
    checkpoints1.put(0, ImmutableMap.of(shardId1, "0"));
    TreeMap<Integer, Map<String, String>> checkpoints2 = new TreeMap<>();
    checkpoints2.put(0, ImmutableMap.of(shardId0, "0"));
    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("id1"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpoints1))
            .times(1);
    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("id2"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpoints2))
            .times(1);


    taskRunner.registerListener(EasyMock.anyObject(TaskRunnerListener.class), EasyMock.anyObject(Executor.class));
    taskQueue.shutdown("id4", "Task [%s] failed to stop in a timely manner, killing task", "id4");
    taskQueue.shutdown("id5", "Task [%s] failed to stop in a timely manner, killing task", "id5");
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();
  }

  @Test
  public void testRequeueTaskWhenFailed() throws Exception
  {
    supervisor = getSupervisor(2, 2, true, "PT1H", null, null);
    supervisorRecordSupplier.assign(anyObject());
    expectLastCall().anyTimes();
    expect(supervisorRecordSupplier.getPartitionIds(stream)).andReturn(ImmutableSet.of(shardId1, shardId0)).anyTimes();
    expect(supervisorRecordSupplier.getAssignment()).andReturn(ImmutableSet.of(shard1Partition, shard0Partition))
                                                    .anyTimes();
    supervisorRecordSupplier.seekToLatest(anyObject());
    expectLastCall().anyTimes();
    expect(supervisorRecordSupplier.getEarliestSequenceNumber(anyObject())).andReturn("0").anyTimes();
    expect(supervisorRecordSupplier.getLatestSequenceNumber(anyObject())).andReturn("100").anyTimes();
    supervisorRecordSupplier.seek(anyObject(), anyString());
    expectLastCall().anyTimes();


    Capture<Task> captured = Capture.newInstance(CaptureType.ALL);
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(Collections.EMPTY_LIST).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(taskClient.getStatusAsync(EasyMock.anyString()))
            .andReturn(Futures.immediateFuture(Status.NOT_STARTED))
            .anyTimes();
    EasyMock.expect(taskClient.getStartTimeAsync(EasyMock.anyString()))
            .andReturn(Futures.immediateFuture(DateTimes.nowUtc()))
            .anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
            null
        )
    ).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.capture(captured))).andReturn(true).times(4);

    TreeMap<Integer, Map<String, String>> checkpoints1 = new TreeMap<>();
    checkpoints1.put(
        0,
        ImmutableMap.of(
            shardId1,
            "0"
        )
    );
    TreeMap<Integer, Map<String, String>> checkpoints2 = new TreeMap<>();
    checkpoints2.put(0, ImmutableMap.of(
        shardId0,
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
    EasyMock.expect(taskStorage.getActiveTasks()).andReturn(tasks).anyTimes();
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
    EasyMock.expect(taskStorage.getActiveTasks()).andReturn(imStillAlive).anyTimes();
    for (Task task : imStillAlive) {
      EasyMock.expect(taskStorage.getStatus(task.getId()))
              .andReturn(Optional.of(TaskStatus.running(task.getId())))
              .anyTimes();
      EasyMock.expect(taskStorage.getTask(task.getId())).andReturn(Optional.of(task)).anyTimes();
    }
    EasyMock.expect(taskStorage.getStatus(iHaveFailed.getId()))
            .andReturn(Optional.of(TaskStatus.failure(iHaveFailed.getId())));
    EasyMock.expect(taskStorage.getTask(iHaveFailed.getId())).andReturn(Optional.of((Task) iHaveFailed)).anyTimes();
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
    supervisor = getSupervisor(2, 1, true, "PT1H", null, null);
    supervisorRecordSupplier.assign(anyObject());
    expectLastCall().anyTimes();
    expect(supervisorRecordSupplier.getPartitionIds(stream)).andReturn(ImmutableSet.of(shardId1, shardId0)).anyTimes();
    expect(supervisorRecordSupplier.getAssignment()).andReturn(ImmutableSet.of(shard1Partition, shard0Partition))
                                                    .anyTimes();
    supervisorRecordSupplier.seekToLatest(anyObject());
    expectLastCall().anyTimes();
    expect(supervisorRecordSupplier.getEarliestSequenceNumber(anyObject())).andReturn("0").anyTimes();
    expect(supervisorRecordSupplier.getLatestSequenceNumber(anyObject())).andReturn("100").anyTimes();
    supervisorRecordSupplier.seek(anyObject(), anyString());
    expectLastCall().anyTimes();

    DateTime now = DateTimes.nowUtc();
    DateTime maxi = now.plusMinutes(60);
    Task id1 = createKinesisIndexTask(
        "id1",
        DATASOURCE,
        0,
        new SeekableStreamPartitions<>("stream", ImmutableMap.of(
            shardId1,
            "0",
            shardId0,
            "0"
        )),
        new SeekableStreamPartitions<>("stream", ImmutableMap.of(
            shardId1,
            SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER,
            shardId0,
            SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER
        )),
        now,
        maxi
    );

    List<Task> existingTasks = ImmutableList.of(id1);

    Capture<Task> captured = Capture.newInstance();
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(Collections.EMPTY_LIST).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasks()).andReturn(existingTasks).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id1")).andReturn(Optional.of(TaskStatus.running("id1"))).anyTimes();
    EasyMock.expect(taskStorage.getTask("id1")).andReturn(Optional.of(id1)).anyTimes();
    EasyMock.expect(taskClient.getStatusAsync("id1"))
            .andReturn(Futures.immediateFuture(Status.READING));
    EasyMock.expect(taskClient.getStartTimeAsync("id1")).andReturn(Futures.immediateFuture(now)).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.capture(captured))).andReturn(true);
    EasyMock.expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
            null
        )
    ).anyTimes();

    TreeMap<Integer, Map<String, String>> checkpoints = new TreeMap<>();
    checkpoints.put(0, ImmutableMap.of(
        shardId1,
        "0",
        shardId0,
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


    EasyMock.expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of(captured.getValue())).anyTimes();
    EasyMock.expect(taskStorage.getStatus(iHaveFailed.getId()))
            .andReturn(Optional.of(TaskStatus.failure(iHaveFailed.getId())));
    EasyMock.expect(taskStorage.getStatus(runningTaskId))
            .andReturn(Optional.of(TaskStatus.running(runningTaskId)))
            .anyTimes();
    EasyMock.expect(taskStorage.getTask(iHaveFailed.getId())).andReturn(Optional.of((Task) iHaveFailed)).anyTimes();
    EasyMock.expect(taskStorage.getTask(runningTaskId)).andReturn(Optional.of(captured.getValue())).anyTimes();
    EasyMock.expect(taskClient.getStatusAsync(runningTaskId))
            .andReturn(Futures.immediateFuture(Status.READING));
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
    supervisor = getSupervisor(2, 2, true, "PT1H", null, null);
    supervisorRecordSupplier.assign(anyObject());
    expectLastCall().anyTimes();
    expect(supervisorRecordSupplier.getPartitionIds(stream)).andReturn(ImmutableSet.of(shardId1, shardId0)).anyTimes();
    expect(supervisorRecordSupplier.getAssignment()).andReturn(ImmutableSet.of(shard1Partition, shard0Partition))
                                                    .anyTimes();
    supervisorRecordSupplier.seekToLatest(anyObject());
    expectLastCall().anyTimes();
    expect(supervisorRecordSupplier.getEarliestSequenceNumber(anyObject())).andReturn("0").anyTimes();
    expect(supervisorRecordSupplier.getLatestSequenceNumber(anyObject())).andReturn("100").anyTimes();
    supervisorRecordSupplier.seek(anyObject(), anyString());
    expectLastCall().anyTimes();

    Capture<Task> captured = Capture.newInstance(CaptureType.ALL);
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(Collections.EMPTY_LIST).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(taskClient.getStatusAsync(EasyMock.anyString()))
            .andReturn(Futures.immediateFuture(Status.NOT_STARTED))
            .anyTimes();
    EasyMock.expect(taskClient.getStartTimeAsync(EasyMock.anyString()))
            .andReturn(Futures.immediateFuture(DateTimes.nowUtc()))
            .anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
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
            .andReturn(Futures.immediateFuture(Status.NOT_STARTED))
            .anyTimes();
    EasyMock.expect(taskClient.getStartTimeAsync(EasyMock.anyString()))
            .andReturn(Futures.immediateFuture(DateTimes.nowUtc()))
            .anyTimes();
    TreeMap<Integer, Map<String, String>> checkpoints1 = new TreeMap<>();
    checkpoints1.put(0, ImmutableMap.of(
        shardId1,
        "0",
        shardId0,
        "0"
    ));
    TreeMap<Integer, Map<String, String>> checkpoints2 = new TreeMap<>();
    checkpoints2.put(0, ImmutableMap.of(
        shardId1,
        "0"
    ));
    // there would be 4 tasks, 2 for each task group
    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("sequenceName-0"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpoints1))
            .times(2);
    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("sequenceName-1"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpoints2))
            .times(2);


    EasyMock.expect(taskStorage.getActiveTasks()).andReturn(tasks).anyTimes();
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
    EasyMock.expect(taskStorage.getActiveTasks()).andReturn(imStillRunning).anyTimes();
    for (Task task : imStillRunning) {
      EasyMock.expect(taskStorage.getStatus(task.getId()))
              .andReturn(Optional.of(TaskStatus.running(task.getId())))
              .anyTimes();
      EasyMock.expect(taskStorage.getTask(task.getId())).andReturn(Optional.of(task)).anyTimes();
    }
    EasyMock.expect(taskStorage.getStatus(iAmSuccess.getId()))
            .andReturn(Optional.of(TaskStatus.success(iAmSuccess.getId())));
    EasyMock.expect(taskStorage.getTask(iAmSuccess.getId())).andReturn(Optional.of((Task) iAmSuccess)).anyTimes();
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
    final TaskLocation location = new TaskLocation("testHost", 1234, -1);

    supervisor = getSupervisor(2, 2, true, "PT1M", null, null);

    supervisorRecordSupplier.assign(anyObject());
    expectLastCall().anyTimes();
    expect(supervisorRecordSupplier.getPartitionIds(stream)).andReturn(ImmutableSet.of(shardId1, shardId0)).anyTimes();
    expect(supervisorRecordSupplier.getAssignment()).andReturn(ImmutableSet.of(shard1Partition, shard0Partition))
                                                    .anyTimes();
    supervisorRecordSupplier.seekToLatest(anyObject());
    expectLastCall().anyTimes();
    expect(supervisorRecordSupplier.getEarliestSequenceNumber(anyObject())).andReturn("0").anyTimes();
    expect(supervisorRecordSupplier.getLatestSequenceNumber(shard1Partition)).andReturn("12").anyTimes();
    expect(supervisorRecordSupplier.getLatestSequenceNumber(shard0Partition)).andReturn("1").anyTimes();
    supervisorRecordSupplier.seek(anyObject(), anyString());
    expectLastCall().anyTimes();

    Capture<Task> captured = Capture.newInstance(CaptureType.ALL);
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(Collections.EMPTY_LIST).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
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
    captured = Capture.newInstance(CaptureType.ALL);
    EasyMock.expect(taskStorage.getActiveTasks()).andReturn(tasks).anyTimes();
    for (Task task : tasks) {
      EasyMock.expect(taskStorage.getStatus(task.getId()))
              .andReturn(Optional.of(TaskStatus.running(task.getId())))
              .anyTimes();
      EasyMock.expect(taskStorage.getTask(task.getId())).andReturn(Optional.of(task)).anyTimes();
    }
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(workItems).anyTimes();
    EasyMock.expect(taskClient.getStatusAsync(EasyMock.anyString()))
            .andReturn(Futures.immediateFuture(Status.READING))
            .anyTimes();
    EasyMock.expect(taskClient.getStartTimeAsync(EasyMock.contains("sequenceName-0")))
            .andReturn(Futures.immediateFuture(DateTimes.nowUtc().minusMinutes(2)))
            .andReturn(Futures.immediateFuture(DateTimes.nowUtc()));
    EasyMock.expect(taskClient.getStartTimeAsync(EasyMock.contains("sequenceName-1")))
            .andReturn(Futures.immediateFuture(DateTimes.nowUtc()))
            .times(2);
    EasyMock.expect(taskClient.pauseAsync(EasyMock.contains("sequenceName-0")))
            .andReturn(Futures.immediateFuture(ImmutableMap.of(
                shardId1,
                "1",
                shardId0,
                "0"
            )))
            .andReturn(Futures.immediateFuture(ImmutableMap.of(
                shardId1,
                "3",
                shardId0,
                "1"
            )));
    EasyMock.expect(
        taskClient.setEndOffsetsAsync(
            EasyMock.contains("sequenceName-0"),
            EasyMock.eq(ImmutableMap.of(
                shardId1,
                "3",
                shardId0,
                "1"
            )),
            EasyMock.eq(true)
        )
    ).andReturn(Futures.immediateFuture(true)).times(2);
    EasyMock.expect(taskQueue.add(EasyMock.capture(captured))).andReturn(true).times(2);

    TreeMap<Integer, Map<String, String>> checkpoints1 = new TreeMap<>();
    checkpoints1.put(0, ImmutableMap.of(
        shardId1,
        "0"
    ));
    TreeMap<Integer, Map<String, String>> checkpoints2 = new TreeMap<>();
    checkpoints2.put(0, ImmutableMap.of(
        shardId0,
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

    for (Task task : captured.getValues()) {
      KinesisIndexTask KinesisIndexTask = (KinesisIndexTask) task;
      Assert.assertEquals(dataSchema, KinesisIndexTask.getDataSchema());
      Assert.assertEquals(tuningConfig.convertToTaskTuningConfig(), KinesisIndexTask.getTuningConfig());

      KinesisIndexTaskIOConfig taskConfig = KinesisIndexTask.getIOConfig();
      Assert.assertEquals("sequenceName-1", taskConfig.getBaseSequenceName());
      Assert.assertTrue("isUseTransaction", taskConfig.isUseTransaction());

      Assert.assertEquals(stream, taskConfig.getStartPartitions().getStream());
      Assert.assertEquals(
          "3",
          taskConfig.getStartPartitions().getPartitionSequenceNumberMap().get(shardId1)
      );
      Assert.assertEquals(
          "1",
          taskConfig.getStartPartitions().getPartitionSequenceNumberMap().get(shardId0)
      );
    }
  }

  @Test
  public void testDiscoverExistingPublishingTask() throws Exception
  {
    final TaskLocation location = new TaskLocation("testHost", 1234, -1);

    supervisor = getSupervisor(1, 1, true, "PT1H", null, null);

    supervisorRecordSupplier.assign(anyObject());
    expectLastCall().anyTimes();
    expect(supervisorRecordSupplier.getPartitionIds(stream)).andReturn(ImmutableSet.of(shardId1, shardId0)).anyTimes();
    expect(supervisorRecordSupplier.getAssignment()).andReturn(ImmutableSet.of(shard1Partition, shard0Partition))
                                                    .anyTimes();
    supervisorRecordSupplier.seekToLatest(anyObject());
    expectLastCall().anyTimes();
    expect(supervisorRecordSupplier.getEarliestSequenceNumber(anyObject())).andReturn("0").anyTimes();
    expect(supervisorRecordSupplier.getLatestSequenceNumber(shard1Partition)).andReturn("12").anyTimes();
    expect(supervisorRecordSupplier.getLatestSequenceNumber(shard0Partition)).andReturn("1").anyTimes();
    supervisorRecordSupplier.seek(anyObject(), anyString());
    expectLastCall().anyTimes();

    Task task = createKinesisIndexTask(
        "id1",
        DATASOURCE,
        0,
        new SeekableStreamPartitions<>("stream", ImmutableMap.of(
            shardId1,
            "0",
            shardId0,
            "0"
        )),
        new SeekableStreamPartitions<>("stream", ImmutableMap.of(
            shardId1,
            SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER,
            shardId0,
            SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER
        )),
        null,
        null
    );

    Collection workItems = new ArrayList<>();
    workItems.add(new TestTaskRunnerWorkItem(task, null, location));

    Capture<KinesisIndexTask> captured = Capture.newInstance();
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(workItems).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of(task)).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id1")).andReturn(Optional.of(TaskStatus.running("id1"))).anyTimes();
    EasyMock.expect(taskStorage.getTask("id1")).andReturn(Optional.of(task)).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
            null
        )
    ).anyTimes();
    EasyMock.expect(taskClient.getStatusAsync("id1"))
            .andReturn(Futures.immediateFuture(Status.PUBLISHING));
    EasyMock.expect(taskClient.getCurrentOffsetsAsync("id1", false))
            .andReturn(Futures.immediateFuture(ImmutableMap.of(
                shardId1,
                "2",
                shardId0,
                "1"
            )));
    EasyMock.expect(taskClient.getEndOffsets("id1")).andReturn(ImmutableMap.of(
        shardId1,
        "2",
        shardId0,
        "1"
    ));
    EasyMock.expect(taskQueue.add(EasyMock.capture(captured))).andReturn(true);

    TreeMap<Integer, Map<String, String>> checkpoints = new TreeMap<>();
    checkpoints.put(0, ImmutableMap.of(
        shardId1,
        "0",
        shardId0,
        "0"
    ));
    EasyMock.expect(taskClient.getCheckpoints(EasyMock.anyString(), EasyMock.anyBoolean()))
            .andReturn(checkpoints)
            .anyTimes();

    taskRunner.registerListener(EasyMock.anyObject(TaskRunnerListener.class), EasyMock.anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    supervisor.updateCurrentAndLatestOffsets().run();
    SupervisorReport<KinesisSupervisorReportPayload> report = supervisor.getStatus();
    verifyAll();

    Assert.assertEquals(DATASOURCE, report.getId());

    KinesisSupervisorReportPayload payload = report.getPayload();

    Assert.assertEquals(DATASOURCE, payload.getDataSource());
    Assert.assertEquals(3600L, (long) payload.getDurationSeconds());
    Assert.assertEquals(2, (int) payload.getPartitions());
    Assert.assertEquals(1, (int) payload.getReplicas());
    Assert.assertEquals(stream, payload.getStream());
    Assert.assertEquals(0, payload.getActiveTasks().size());
    Assert.assertEquals(1, payload.getPublishingTasks().size());

    TaskReportData publishingReport = payload.getPublishingTasks().get(0);

    Assert.assertEquals("id1", publishingReport.getId());
    Assert.assertEquals(ImmutableMap.of(
        shardId1,
        "0",
        shardId0,
        "0"
    ), publishingReport.getStartingOffsets());
    Assert.assertEquals(ImmutableMap.of(
        shardId1,
        "2",
        shardId0,
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
    Assert.assertEquals(stream, capturedTaskConfig.getStartPartitions().getStream());
    Assert.assertEquals(
        "2",
        capturedTaskConfig.getStartPartitions().getPartitionSequenceNumberMap().get(shardId1)
    );
    Assert.assertEquals(
        "1",
        capturedTaskConfig.getStartPartitions().getPartitionSequenceNumberMap().get(shardId0)
    );

    Assert.assertEquals(stream, capturedTaskConfig.getEndPartitions().getStream());
    Assert.assertEquals(
        SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER,
        capturedTaskConfig.getEndPartitions().getPartitionSequenceNumberMap().get(shardId1)
    );
    Assert.assertEquals(
        SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER,
        capturedTaskConfig.getEndPartitions().getPartitionSequenceNumberMap().get(shardId0)
    );
  }

  @Test
  public void testDiscoverExistingPublishingTaskWithDifferentPartitionAllocation() throws Exception
  {
    final TaskLocation location = new TaskLocation("testHost", 1234, -1);

    supervisor = getSupervisor(1, 1, true, "PT1H", null, null);
    supervisorRecordSupplier.assign(anyObject());
    expectLastCall().anyTimes();
    expect(supervisorRecordSupplier.getPartitionIds(stream)).andReturn(ImmutableSet.of(shardId1, shardId0)).anyTimes();
    expect(supervisorRecordSupplier.getAssignment()).andReturn(ImmutableSet.of(shard1Partition, shard0Partition))
                                                    .anyTimes();
    supervisorRecordSupplier.seekToLatest(anyObject());
    expectLastCall().anyTimes();
    expect(supervisorRecordSupplier.getEarliestSequenceNumber(anyObject())).andReturn("0").anyTimes();
    expect(supervisorRecordSupplier.getLatestSequenceNumber(shard1Partition)).andReturn("12").anyTimes();
    expect(supervisorRecordSupplier.getLatestSequenceNumber(shard0Partition)).andReturn("1").anyTimes();
    supervisorRecordSupplier.seek(anyObject(), anyString());
    expectLastCall().anyTimes();

    Task task = createKinesisIndexTask(
        "id1",
        DATASOURCE,
        0,
        new SeekableStreamPartitions<>("stream", ImmutableMap.of(
            shardId1,
            "0",
            shardId0,
            "0"
        )),
        new SeekableStreamPartitions<>("stream", ImmutableMap.of(
            shardId1,
            SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER,
            shardId0,
            SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER
        )),
        null,
        null
    );

    Collection workItems = new ArrayList<>();
    workItems.add(new TestTaskRunnerWorkItem(task, null, location));

    Capture<KinesisIndexTask> captured = Capture.newInstance();
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(workItems).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of(task)).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id1")).andReturn(Optional.of(TaskStatus.running("id1"))).anyTimes();
    EasyMock.expect(taskStorage.getTask("id1")).andReturn(Optional.of(task)).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
            null
        )
    ).anyTimes();
    EasyMock.expect(taskClient.getStatusAsync("id1"))
            .andReturn(Futures.immediateFuture(Status.PUBLISHING));
    EasyMock.expect(taskClient.getCurrentOffsetsAsync("id1", false))
            .andReturn(Futures.immediateFuture(ImmutableMap.of(
                shardId1,
                "2",
                shardId0,
                "1"
            )));
    EasyMock.expect(taskClient.getEndOffsets("id1")).andReturn(ImmutableMap.of(
        shardId1,
        "2",
        shardId0,
        "1"
    ));
    EasyMock.expect(taskQueue.add(EasyMock.capture(captured))).andReturn(true);

    taskRunner.registerListener(EasyMock.anyObject(TaskRunnerListener.class), EasyMock.anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    supervisor.updateCurrentAndLatestOffsets().run();
    SupervisorReport<KinesisSupervisorReportPayload> report = supervisor.getStatus();
    verifyAll();

    Assert.assertEquals(DATASOURCE, report.getId());

    KinesisSupervisorReportPayload payload = report.getPayload();

    Assert.assertEquals(DATASOURCE, payload.getDataSource());
    Assert.assertEquals(3600L, (long) payload.getDurationSeconds());
    Assert.assertEquals(2, (int) payload.getPartitions());
    Assert.assertEquals(1, (int) payload.getReplicas());
    Assert.assertEquals(stream, payload.getStream());
    Assert.assertEquals(0, payload.getActiveTasks().size());
    Assert.assertEquals(1, payload.getPublishingTasks().size());

    TaskReportData publishingReport = payload.getPublishingTasks().get(0);

    Assert.assertEquals("id1", publishingReport.getId());
    Assert.assertEquals(ImmutableMap.of(
        shardId1,
        "0",
        shardId0,
        "0"
    ), publishingReport.getStartingOffsets());
    Assert.assertEquals(ImmutableMap.of(
        shardId1,
        "2",
        shardId0,
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
    Assert.assertEquals(stream, capturedTaskConfig.getStartPartitions().getStream());
    Assert.assertEquals(
        "2",
        capturedTaskConfig.getStartPartitions().getPartitionSequenceNumberMap().get(shardId1)
    );
    Assert.assertEquals(
        "1",
        capturedTaskConfig.getStartPartitions().getPartitionSequenceNumberMap().get(shardId0)
    );

    Assert.assertEquals(stream, capturedTaskConfig.getEndPartitions().getStream());
    Assert.assertEquals(
        SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER,
        capturedTaskConfig.getEndPartitions().getPartitionSequenceNumberMap().get(shardId1)
    );
    Assert.assertEquals(
        SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER,
        capturedTaskConfig.getEndPartitions().getPartitionSequenceNumberMap().get(shardId0)
    );
  }

  @Test
  public void testDiscoverExistingPublishingAndReadingTask() throws Exception
  {
    final TaskLocation location1 = new TaskLocation("testHost", 1234, -1);
    final TaskLocation location2 = new TaskLocation("testHost2", 145, -1);
    final DateTime startTime = DateTimes.nowUtc();

    supervisor = getSupervisor(1, 1, true, "PT1H", null, null);

    supervisorRecordSupplier.assign(anyObject());
    expectLastCall().anyTimes();
    expect(supervisorRecordSupplier.getPartitionIds(stream)).andReturn(ImmutableSet.of(shardId1, shardId0)).anyTimes();
    expect(supervisorRecordSupplier.getAssignment()).andReturn(ImmutableSet.of(shard1Partition, shard0Partition))
                                                    .anyTimes();
    supervisorRecordSupplier.seekToLatest(anyObject());
    expectLastCall().anyTimes();
    expect(supervisorRecordSupplier.getEarliestSequenceNumber(anyObject())).andReturn("0").anyTimes();
    expect(supervisorRecordSupplier.getLatestSequenceNumber(shard1Partition)).andReturn("12").anyTimes();
    expect(supervisorRecordSupplier.getLatestSequenceNumber(shard0Partition)).andReturn("1").anyTimes();
    supervisorRecordSupplier.seek(anyObject(), anyString());
    expectLastCall().anyTimes();
    Task id1 = createKinesisIndexTask(
        "id1",
        DATASOURCE,
        0,
        new SeekableStreamPartitions<>(
            "stream",
            ImmutableMap.of(
                shardId1,
                "0",
                shardId0,
                "0"
            )
        ),
        new SeekableStreamPartitions<>("stream", ImmutableMap.of(
            shardId1,
            SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER,
            shardId0,
            SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER
        )),
        null,
        null
    );

    Task id2 = createKinesisIndexTask(
        "id2",
        DATASOURCE,
        0,
        new SeekableStreamPartitions<>("stream", ImmutableMap.of(
            shardId1,
            "2",
            shardId0,
            "1"
        )),
        new SeekableStreamPartitions<>("stream", ImmutableMap.of(
            shardId1,
            SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER,
            shardId0,
            SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER
        )),
        null,
        null
    );

    Collection workItems = new ArrayList<>();
    workItems.add(new TestTaskRunnerWorkItem(id1, null, location1));
    workItems.add(new TestTaskRunnerWorkItem(id2, null, location2));

    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(workItems).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of(id1, id2)).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id1")).andReturn(Optional.of(TaskStatus.running("id1"))).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id2")).andReturn(Optional.of(TaskStatus.running("id2"))).anyTimes();
    EasyMock.expect(taskStorage.getTask("id1")).andReturn(Optional.of(id1)).anyTimes();
    EasyMock.expect(taskStorage.getTask("id2")).andReturn(Optional.of(id2)).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
            null
        )
    ).anyTimes();
    EasyMock.expect(taskClient.getStatusAsync("id1"))
            .andReturn(Futures.immediateFuture(Status.PUBLISHING));
    EasyMock.expect(taskClient.getStatusAsync("id2"))
            .andReturn(Futures.immediateFuture(Status.READING));
    EasyMock.expect(taskClient.getStartTimeAsync("id2")).andReturn(Futures.immediateFuture(startTime));
    EasyMock.expect(taskClient.getCurrentOffsetsAsync("id1", false))
            .andReturn(Futures.immediateFuture(ImmutableMap.of(
                shardId1,
                "2",
                shardId0,
                "1"
            )));
    EasyMock.expect(taskClient.getEndOffsets("id1")).andReturn(ImmutableMap.of(
        shardId1,
        "2",
        shardId0,
        "1"
    ));
    EasyMock.expect(taskClient.getCurrentOffsetsAsync("id2", false))
            .andReturn(Futures.immediateFuture((Map<String, String>) ImmutableMap.of(
                shardId1,
                "12",
                shardId0,
                "1"
            )));

    taskRunner.registerListener(EasyMock.anyObject(TaskRunnerListener.class), EasyMock.anyObject(Executor.class));

    // since id1 is publishing, so getCheckpoints wouldn't be called for it
    TreeMap<Integer, Map<String, String>> checkpoints = new TreeMap<>();
    checkpoints.put(0, ImmutableMap.of(
        shardId1,
        "2",
        shardId0,
        "1"
    ));
    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("id2"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpoints))
            .times(1);

    replayAll();

    supervisor.start();
    supervisor.runInternal();
    supervisor.updateCurrentAndLatestOffsets().run();
    SupervisorReport<KinesisSupervisorReportPayload> report = supervisor.getStatus();
    verifyAll();

    Assert.assertEquals(DATASOURCE, report.getId());

    KinesisSupervisorReportPayload payload = report.getPayload();

    Assert.assertEquals(DATASOURCE, payload.getDataSource());
    Assert.assertEquals(3600L, (long) payload.getDurationSeconds());
    Assert.assertEquals(2, (int) payload.getPartitions());
    Assert.assertEquals(1, (int) payload.getReplicas());
    Assert.assertEquals(stream, payload.getStream());
    Assert.assertEquals(1, payload.getActiveTasks().size());
    Assert.assertEquals(1, payload.getPublishingTasks().size());

    TaskReportData activeReport = payload.getActiveTasks().get(0);
    TaskReportData publishingReport = payload.getPublishingTasks().get(0);

    Assert.assertEquals("id2", activeReport.getId());
    Assert.assertEquals(startTime, activeReport.getStartTime());
    Assert.assertEquals(ImmutableMap.of(
        shardId1,
        "2",
        shardId0,
        "1"
    ), activeReport.getStartingOffsets());
    Assert.assertEquals(ImmutableMap.of(
        shardId1,
        "12",
        shardId0,
        "1"
    ), activeReport.getCurrentOffsets());

    Assert.assertEquals("id1", publishingReport.getId());
    Assert.assertEquals(ImmutableMap.of(
        shardId1,
        "0",
        shardId0,
        "0"
    ), publishingReport.getStartingOffsets());
    Assert.assertEquals(ImmutableMap.of(
        shardId1,
        "2",
        shardId0,
        "1"
    ), publishingReport.getCurrentOffsets());
  }

  @Test
  public void testKillUnresponsiveTasksWhileGettingStartTime() throws Exception
  {
    supervisor = getSupervisor(2, 2, true, "PT1H", null, null);
    supervisorRecordSupplier.assign(anyObject());
    expectLastCall().anyTimes();
    expect(supervisorRecordSupplier.getPartitionIds(stream)).andReturn(ImmutableSet.of(shardId1, shardId0)).anyTimes();
    expect(supervisorRecordSupplier.getAssignment()).andReturn(ImmutableSet.of(shard1Partition, shard0Partition))
                                                    .anyTimes();
    supervisorRecordSupplier.seekToLatest(anyObject());
    expectLastCall().anyTimes();
    expect(supervisorRecordSupplier.getEarliestSequenceNumber(anyObject())).andReturn("0").anyTimes();
    expect(supervisorRecordSupplier.getLatestSequenceNumber(shard1Partition)).andReturn("12").anyTimes();
    expect(supervisorRecordSupplier.getLatestSequenceNumber(shard0Partition)).andReturn("1").anyTimes();
    supervisorRecordSupplier.seek(anyObject(), anyString());
    expectLastCall().anyTimes();

    Capture<Task> captured = Capture.newInstance(CaptureType.ALL);
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(Collections.EMPTY_LIST).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
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
        shardId1,

        "0",

        shardId0,

        "0"
    ));
    TreeMap<Integer, Map<String, String>> checkpoints2 = new TreeMap<>();
    checkpoints2.put(0, ImmutableMap.of(shardId0, "0"));
    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("sequenceName-0"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpoints1))
            .times(2);
    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("sequenceName-1"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpoints2))
            .times(2);


    EasyMock.expect(taskStorage.getActiveTasks()).andReturn(tasks).anyTimes();
    for (Task task : tasks) {
      EasyMock.expect(taskStorage.getStatus(task.getId()))
              .andReturn(Optional.of(TaskStatus.running(task.getId())))
              .anyTimes();
      EasyMock.expect(taskStorage.getTask(task.getId())).andReturn(Optional.of(task)).anyTimes();
      EasyMock.expect(taskClient.getStatusAsync(task.getId()))
              .andReturn(Futures.immediateFuture(Status.NOT_STARTED));
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
    final TaskLocation location = new TaskLocation("testHost", 1234, -1);

    supervisor = getSupervisor(2, 2, true, "PT1M", null, null);
    supervisorRecordSupplier.assign(anyObject());
    expectLastCall().anyTimes();
    expect(supervisorRecordSupplier.getPartitionIds(stream)).andReturn(ImmutableSet.of(shardId1, shardId0)).anyTimes();
    expect(supervisorRecordSupplier.getAssignment()).andReturn(ImmutableSet.of(shard1Partition, shard0Partition))
                                                    .anyTimes();
    supervisorRecordSupplier.seekToLatest(anyObject());
    expectLastCall().anyTimes();
    expect(supervisorRecordSupplier.getEarliestSequenceNumber(anyObject())).andReturn("0").anyTimes();
    expect(supervisorRecordSupplier.getLatestSequenceNumber(shard1Partition)).andReturn("12").anyTimes();
    expect(supervisorRecordSupplier.getLatestSequenceNumber(shard0Partition)).andReturn("1").anyTimes();
    supervisorRecordSupplier.seek(anyObject(), anyString());
    expectLastCall().anyTimes();

    Capture<Task> captured = Capture.newInstance(CaptureType.ALL);
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(Collections.EMPTY_LIST).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
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
        shardId1,
        "0"
    ));
    TreeMap<Integer, Map<String, String>> checkpoints2 = new TreeMap<>();
    checkpoints2.put(0, ImmutableMap.of(shardId0, "0"));
    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("sequenceName-0"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpoints1))
            .times(2);
    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("sequenceName-1"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpoints2))
            .times(2);

    captured = Capture.newInstance(CaptureType.ALL);
    EasyMock.expect(taskStorage.getActiveTasks()).andReturn(tasks).anyTimes();
    for (Task task : tasks) {
      EasyMock.expect(taskStorage.getStatus(task.getId()))
              .andReturn(Optional.of(TaskStatus.running(task.getId())))
              .anyTimes();
      EasyMock.expect(taskStorage.getTask(task.getId())).andReturn(Optional.of(task)).anyTimes();
    }
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(workItems).anyTimes();
    EasyMock.expect(taskClient.getStatusAsync(EasyMock.anyString()))
            .andReturn(Futures.immediateFuture(Status.READING))
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
        EasyMock.eq("An exception occured while waiting for task [%s] to pause: [%s]"),
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
          taskConfig.getStartPartitions().getPartitionSequenceNumberMap().get(shardId1)
      );
      Assert.assertNull(
          taskConfig.getStartPartitions().getPartitionSequenceNumberMap().get(shardId0)
      );
    }
  }

  @Test
  public void testKillUnresponsiveTasksWhileSettingEndOffsets() throws Exception
  {
    final TaskLocation location = new TaskLocation("testHost", 1234, -1);

    supervisor = getSupervisor(2, 2, true, "PT1M", null, null);
    supervisorRecordSupplier.assign(anyObject());
    expectLastCall().anyTimes();
    expect(supervisorRecordSupplier.getPartitionIds(stream)).andReturn(ImmutableSet.of(shardId1, shardId0)).anyTimes();
    expect(supervisorRecordSupplier.getAssignment()).andReturn(ImmutableSet.of(shard1Partition, shard0Partition))
                                                    .anyTimes();
    supervisorRecordSupplier.seekToLatest(anyObject());
    expectLastCall().anyTimes();
    expect(supervisorRecordSupplier.getEarliestSequenceNumber(anyObject())).andReturn("0").anyTimes();
    expect(supervisorRecordSupplier.getLatestSequenceNumber(shard1Partition)).andReturn("12").anyTimes();
    expect(supervisorRecordSupplier.getLatestSequenceNumber(shard0Partition)).andReturn("1").anyTimes();
    supervisorRecordSupplier.seek(anyObject(), anyString());
    expectLastCall().anyTimes();

    Capture<Task> captured = Capture.newInstance(CaptureType.ALL);
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(Collections.EMPTY_LIST).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
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
        shardId1,
        "0"
    ));
    TreeMap<Integer, Map<String, String>> checkpoints2 = new TreeMap<>();
    checkpoints2.put(0, ImmutableMap.of(shardId0, "0"));
    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("sequenceName-0"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpoints1))
            .times(2);
    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("sequenceName-1"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpoints2))
            .times(2);

    captured = Capture.newInstance(CaptureType.ALL);
    EasyMock.expect(taskStorage.getActiveTasks()).andReturn(tasks).anyTimes();
    for (Task task : tasks) {
      EasyMock.expect(taskStorage.getStatus(task.getId()))
              .andReturn(Optional.of(TaskStatus.running(task.getId())))
              .anyTimes();
      EasyMock.expect(taskStorage.getTask(task.getId())).andReturn(Optional.of(task)).anyTimes();
    }
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(workItems).anyTimes();
    EasyMock.expect(taskClient.getStatusAsync(EasyMock.anyString()))
            .andReturn(Futures.immediateFuture(Status.READING))
            .anyTimes();
    EasyMock.expect(taskClient.getStartTimeAsync(EasyMock.contains("sequenceName-0")))
            .andReturn(Futures.immediateFuture(DateTimes.nowUtc().minusMinutes(2)))
            .andReturn(Futures.immediateFuture(DateTimes.nowUtc()));
    EasyMock.expect(taskClient.getStartTimeAsync(EasyMock.contains("sequenceName-1")))
            .andReturn(Futures.immediateFuture(DateTimes.nowUtc()))
            .times(2);
    EasyMock.expect(taskClient.pauseAsync(EasyMock.contains("sequenceName-0")))
            .andReturn(Futures.immediateFuture(ImmutableMap.of(
                shardId1,
                "1"
            )))
            .andReturn(Futures.immediateFuture(ImmutableMap.of(
                shardId1,
                "3"
            )));
    EasyMock.expect(
        taskClient.setEndOffsetsAsync(
            EasyMock.contains("sequenceName-0"),
            EasyMock.eq(ImmutableMap.of(
                shardId1,
                "3"
            )),
            EasyMock.eq(true)
        )
    ).andReturn(Futures.immediateFailedFuture(new RuntimeException())).times(2);
    taskQueue.shutdown(
        EasyMock.contains("sequenceName-0"),
        EasyMock.eq("All tasks in group [%s] failed to transition to publishing state"),
        EasyMock.eq(0)
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
          taskConfig.getStartPartitions().getPartitionSequenceNumberMap().get(shardId1)
      );
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testStopNotStarted()
  {
    supervisor = getSupervisor(1, 1, true, "PT1H", null, null);
    supervisor.stop(false);
  }

  @Test
  public void testStop()
  {
    supervisorRecordSupplier.close();
    expectLastCall().anyTimes();

    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    taskClient.close();
    taskRunner.unregisterListener(StringUtils.format("KinesisSupervisor-%s", DATASOURCE));
    replayAll();

    supervisor = getSupervisor(1, 1, true, "PT1H", null, null);
    supervisor.start();
    supervisor.stop(false);

    verifyAll();
  }

  @Test
  public void testStopGracefully() throws Exception
  {
    final TaskLocation location1 = new TaskLocation("testHost", 1234, -1);
    final TaskLocation location2 = new TaskLocation("testHost2", 145, -1);
    final DateTime startTime = DateTimes.nowUtc();

    supervisor = getSupervisor(2, 1, true, "PT1H", null, null);
    supervisorRecordSupplier.assign(anyObject());
    expectLastCall().anyTimes();
    expect(supervisorRecordSupplier.getPartitionIds(stream)).andReturn(ImmutableSet.of(shardId1, shardId0)).anyTimes();
    expect(supervisorRecordSupplier.getAssignment()).andReturn(ImmutableSet.of(shard1Partition, shard0Partition))
                                                    .anyTimes();
    supervisorRecordSupplier.seekToLatest(anyObject());
    expectLastCall().anyTimes();
    expect(supervisorRecordSupplier.getEarliestSequenceNumber(anyObject())).andReturn("0").anyTimes();
    expect(supervisorRecordSupplier.getLatestSequenceNumber(shard1Partition)).andReturn("12").anyTimes();
    expect(supervisorRecordSupplier.getLatestSequenceNumber(shard0Partition)).andReturn("1").anyTimes();
    supervisorRecordSupplier.seek(anyObject(), anyString());
    expectLastCall().anyTimes();

    Task id1 = createKinesisIndexTask(
        "id1",
        DATASOURCE,
        0,
        new SeekableStreamPartitions<>("stream", ImmutableMap.of(
            shardId1,
            "0",
            shardId0,
            "0"
        )),
        new SeekableStreamPartitions<>("stream", ImmutableMap.of(
            shardId1,
            SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER,
            shardId0,
            SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER
        )),
        null,
        null
    );

    Task id2 = createKinesisIndexTask(
        "id2",
        DATASOURCE,
        0,
        new SeekableStreamPartitions<>("stream", ImmutableMap.of(
            shardId1,
            "3",
            shardId0,
            "1"
        )),
        new SeekableStreamPartitions<>("stream", ImmutableMap.of(
            shardId1,
            SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER,
            shardId0,
            SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER
        )),
        null,
        null
    );

    Task id3 = createKinesisIndexTask(
        "id3",
        DATASOURCE,
        0,
        new SeekableStreamPartitions<>("stream", ImmutableMap.of(
            shardId1,
            "3",
            shardId0,
            "1"
        )),
        new SeekableStreamPartitions<>("stream", ImmutableMap.of(
            shardId1,
            SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER,
            shardId0,
            SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER
        )),
        null,
        null
    );

    Collection workItems = new ArrayList<>();
    workItems.add(new TestTaskRunnerWorkItem(id1, null, location1));
    workItems.add(new TestTaskRunnerWorkItem(id2, null, location2));

    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(workItems).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of(id1, id2, id3)).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id1")).andReturn(Optional.of(TaskStatus.running("id1"))).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id2")).andReturn(Optional.of(TaskStatus.running("id2"))).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id3")).andReturn(Optional.of(TaskStatus.running("id3"))).anyTimes();
    EasyMock.expect(taskStorage.getTask("id1")).andReturn(Optional.of(id1)).anyTimes();
    EasyMock.expect(taskStorage.getTask("id2")).andReturn(Optional.of(id2)).anyTimes();
    EasyMock.expect(taskStorage.getTask("id3")).andReturn(Optional.of(id3)).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
            null
        )
    ).anyTimes();
    EasyMock.expect(taskClient.getStatusAsync("id1"))
            .andReturn(Futures.immediateFuture(Status.PUBLISHING));
    EasyMock.expect(taskClient.getStatusAsync("id2"))
            .andReturn(Futures.immediateFuture(Status.READING));
    EasyMock.expect(taskClient.getStatusAsync("id3"))
            .andReturn(Futures.immediateFuture(Status.READING));
    EasyMock.expect(taskClient.getStartTimeAsync("id2")).andReturn(Futures.immediateFuture(startTime));
    EasyMock.expect(taskClient.getStartTimeAsync("id3")).andReturn(Futures.immediateFuture(startTime));
    EasyMock.expect(taskClient.getEndOffsets("id1")).andReturn(ImmutableMap.of(
        shardId1,
        "3",
        shardId0,
        "1"
    ));

    // getCheckpoints will not be called for id1 as it is in publishing state
    TreeMap<Integer, Map<String, String>> checkpoints = new TreeMap<>();
    checkpoints.put(0, ImmutableMap.of(
        shardId1,
        "3",
        shardId0,
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
    EasyMock.expect(taskClient.pauseAsync("id2"))
            .andReturn(Futures.immediateFuture((Map<String, String>) ImmutableMap.of(
                shardId1,
                "12",
                shardId0,
                "1"
            )));
    EasyMock.expect(taskClient.setEndOffsetsAsync("id2", ImmutableMap.of(
        shardId1,
        "12",
        shardId0,
        "1"
    ), true))
            .andReturn(Futures.immediateFuture(true));
    taskQueue.shutdown("id3", "Killing task for graceful shutdown");
    expectLastCall().times(1);
    taskQueue.shutdown("id3", "Killing task [%s] which hasn't been assigned to a worker", "id3");
    expectLastCall().times(1);

    EasyMock.replay(taskRunner, taskClient, taskQueue);

    supervisor.gracefulShutdownInternal();
    verifyAll();
  }

  @Test
  public void testResetNoTasks() throws Exception
  {
    expect(supervisorRecordSupplier.getPartitionIds(anyObject())).andReturn(Collections.emptySet()).anyTimes();

    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(Collections.EMPTY_LIST).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();
    taskRunner.registerListener(EasyMock.anyObject(TaskRunnerListener.class), EasyMock.anyObject(Executor.class));
    replayAll();

    supervisor = getSupervisor(1, 1, true, "PT1H", null, null);


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
    expect(supervisorRecordSupplier.getPartitionIds(anyObject())).andReturn(Collections.emptySet()).anyTimes();
    supervisor = getSupervisor(1, 1, true, "PT1H", null, null);
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(Collections.EMPTY_LIST).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();
    taskRunner.registerListener(EasyMock.anyObject(TaskRunnerListener.class), EasyMock.anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    Capture<String> captureDataSource = EasyMock.newCapture();
    Capture<DataSourceMetadata> captureDataSourceMetadata = EasyMock.newCapture();

    KinesisDataSourceMetadata KinesisDataSourceMetadata = new KinesisDataSourceMetadata(new SeekableStreamPartitions<>(
        stream,
        ImmutableMap.of(
            shardId1,
            SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER,
            shardId0,
            SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER
        )
    ));

    KinesisDataSourceMetadata resetMetadata = new KinesisDataSourceMetadata(new SeekableStreamPartitions<>(
        stream,
        ImmutableMap.of(
            shardId0,
            SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER
        )
    ));

    KinesisDataSourceMetadata expectedMetadata = new KinesisDataSourceMetadata(new SeekableStreamPartitions<>(
        stream,
        ImmutableMap.of(
            shardId1,
            SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER
        )
    ));

    EasyMock.reset(indexerMetadataStorageCoordinator);
    EasyMock.expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE))
            .andReturn(KinesisDataSourceMetadata);
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
      Assert.assertTrue(npe.getCause() == null);
    }
    verifyAll();

    Assert.assertEquals(captureDataSource.getValue(), DATASOURCE);
    Assert.assertEquals(captureDataSourceMetadata.getValue(), expectedMetadata);
  }

  @Test
  public void testResetNoDataSourceMetadata() throws Exception
  {
    expect(supervisorRecordSupplier.getPartitionIds(anyObject())).andReturn(Collections.emptySet()).anyTimes();
    supervisor = getSupervisor(1, 1, true, "PT1H", null, null);
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(Collections.EMPTY_LIST).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();
    taskRunner.registerListener(EasyMock.anyObject(TaskRunnerListener.class), EasyMock.anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    KinesisDataSourceMetadata resetMetadata = new KinesisDataSourceMetadata(new SeekableStreamPartitions<>(
        stream,
        ImmutableMap.of(
            shardId0,
            SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER
        )
    ));

    EasyMock.reset(indexerMetadataStorageCoordinator);
    // no DataSourceMetadata in metadata store
    EasyMock.expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(null);
    EasyMock.replay(indexerMetadataStorageCoordinator);

    supervisor.resetInternal(resetMetadata);
    verifyAll();
  }

  @Test
  public void testResetRunningTasks() throws Exception
  {
    final TaskLocation location1 = new TaskLocation("testHost", 1234, -1);
    final TaskLocation location2 = new TaskLocation("testHost2", 145, -1);
    final DateTime startTime = DateTimes.nowUtc();

    supervisor = getSupervisor(2, 1, true, "PT1H", null, null);
    supervisorRecordSupplier.assign(anyObject());
    expectLastCall().anyTimes();
    expect(supervisorRecordSupplier.getPartitionIds(stream)).andReturn(ImmutableSet.of(shardId1, shardId0)).anyTimes();
    expect(supervisorRecordSupplier.getAssignment()).andReturn(ImmutableSet.of(shard1Partition, shard0Partition))
                                                    .anyTimes();
    supervisorRecordSupplier.seekToLatest(anyObject());
    expectLastCall().anyTimes();
    expect(supervisorRecordSupplier.getEarliestSequenceNumber(anyObject())).andReturn("0").anyTimes();
    expect(supervisorRecordSupplier.getLatestSequenceNumber(shard1Partition)).andReturn("12").anyTimes();
    expect(supervisorRecordSupplier.getLatestSequenceNumber(shard0Partition)).andReturn("1").anyTimes();
    supervisorRecordSupplier.seek(anyObject(), anyString());
    expectLastCall().anyTimes();

    Task id1 = createKinesisIndexTask(
        "id1",
        DATASOURCE,
        0,
        new SeekableStreamPartitions<>("stream", ImmutableMap.of(
            shardId1,

            "0",

            shardId0,

            "0"
        )),
        new SeekableStreamPartitions<>("stream", ImmutableMap.of(
            shardId1,
            SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER,
            shardId0,
            SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER
        )),
        null,
        null
    );

    Task id2 = createKinesisIndexTask(
        "id2",
        DATASOURCE,
        0,
        new SeekableStreamPartitions<>("stream", ImmutableMap.of(
            shardId1,

            "3",

            shardId0,

            "1"
        )),
        new SeekableStreamPartitions<>("stream", ImmutableMap.of(
            shardId1,
            SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER,
            shardId0,
            SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER
        )),
        null,
        null
    );

    Task id3 = createKinesisIndexTask(
        "id3",
        DATASOURCE,
        0,
        new SeekableStreamPartitions<>("stream", ImmutableMap.of(
            shardId1,

            "3",

            shardId0,

            "1"
        )),
        new SeekableStreamPartitions<>("stream", ImmutableMap.of(
            shardId1,
            SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER,
            shardId0,
            SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER
        )),
        null,
        null
    );

    Collection workItems = new ArrayList<>();
    workItems.add(new TestTaskRunnerWorkItem(id1, null, location1));
    workItems.add(new TestTaskRunnerWorkItem(id2, null, location2));

    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(workItems).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of(id1, id2, id3)).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id1")).andReturn(Optional.of(TaskStatus.running("id1"))).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id2")).andReturn(Optional.of(TaskStatus.running("id2"))).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id3")).andReturn(Optional.of(TaskStatus.running("id3"))).anyTimes();
    EasyMock.expect(taskStorage.getTask("id1")).andReturn(Optional.of(id1)).anyTimes();
    EasyMock.expect(taskStorage.getTask("id2")).andReturn(Optional.of(id2)).anyTimes();
    EasyMock.expect(taskStorage.getTask("id3")).andReturn(Optional.of(id3)).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
            null
        )
    ).anyTimes();
    EasyMock.expect(taskClient.getStatusAsync("id1"))
            .andReturn(Futures.immediateFuture(Status.PUBLISHING));
    EasyMock.expect(taskClient.getStatusAsync("id2"))
            .andReturn(Futures.immediateFuture(Status.READING));
    EasyMock.expect(taskClient.getStatusAsync("id3"))
            .andReturn(Futures.immediateFuture(Status.READING));
    EasyMock.expect(taskClient.getStartTimeAsync("id2")).andReturn(Futures.immediateFuture(startTime));
    EasyMock.expect(taskClient.getStartTimeAsync("id3")).andReturn(Futures.immediateFuture(startTime));
    EasyMock.expect(taskClient.getEndOffsets("id1")).andReturn(ImmutableMap.of(
        shardId1,

        "3",

        shardId0,

        "1"
    ));

    TreeMap<Integer, Map<String, String>> checkpoints = new TreeMap<>();
    checkpoints.put(0, ImmutableMap.of(
        shardId1,

        "3",

        shardId0,

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
    supervisor = getSupervisor(2, 1, true, "PT1H", null, null);

    //not adding any events
    Task id1 = createKinesisIndexTask(
        "id1",
        DATASOURCE,
        0,
        new SeekableStreamPartitions<>("stream", ImmutableMap.of(
            shardId1,
            "0",
            shardId0,
            "0"
        )),
        new SeekableStreamPartitions<>("stream", ImmutableMap.of(
            shardId1,
            SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER,
            shardId0,
            SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER
        )),
        null,
        null
    );

    Task id2 = createKinesisIndexTask(
        "id2",
        DATASOURCE,
        0,
        new SeekableStreamPartitions<>("stream", ImmutableMap.of(
            shardId1,
            "10",
            shardId0,
            "20"
        )),
        new SeekableStreamPartitions<>("stream", ImmutableMap.of(
            shardId1,
            SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER,
            shardId0,
            SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER
        )),
        null,
        null
    );

    Task id3 = createKinesisIndexTask(
        "id3",
        DATASOURCE,
        0,
        new SeekableStreamPartitions<>("stream", ImmutableMap.of(
            shardId1,
            "10",
            shardId0,
            "20"
        )),
        new SeekableStreamPartitions<>("stream", ImmutableMap.of(
            shardId1,
            SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER,
            shardId0,
            SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER
        )),
        null,
        null
    );

    supervisorRecordSupplier.assign(anyObject());
    expectLastCall().anyTimes();
    expect(supervisorRecordSupplier.getPartitionIds(stream)).andReturn(ImmutableSet.of(shardId1, shardId0)).anyTimes();
    expect(supervisorRecordSupplier.getAssignment()).andReturn(ImmutableSet.of(shard1Partition, shard0Partition))
                                                    .anyTimes();
    supervisorRecordSupplier.seekToLatest(anyObject());
    expectLastCall().anyTimes();
    expect(supervisorRecordSupplier.getEarliestSequenceNumber(anyObject())).andReturn("0").anyTimes();
    expect(supervisorRecordSupplier.getLatestSequenceNumber(shard1Partition)).andReturn("12").anyTimes();
    expect(supervisorRecordSupplier.getLatestSequenceNumber(shard0Partition)).andReturn("1").anyTimes();
    supervisorRecordSupplier.seek(anyObject(), anyString());
    expectLastCall().anyTimes();
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of(id1, id2, id3)).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id1")).andReturn(Optional.of(TaskStatus.running("id1"))).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id2")).andReturn(Optional.of(TaskStatus.running("id2"))).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id3")).andReturn(Optional.of(TaskStatus.running("id3"))).anyTimes();
    EasyMock.expect(taskStorage.getTask("id1")).andReturn(Optional.of(id1)).anyTimes();
    EasyMock.expect(taskStorage.getTask("id2")).andReturn(Optional.of(id2)).anyTimes();
    EasyMock.expect(taskStorage.getTask("id3")).andReturn(Optional.of(id3)).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
            null
        )
    ).anyTimes();
    EasyMock.expect(taskClient.getStatusAsync("id1"))
            .andReturn(Futures.immediateFuture(Status.READING));
    EasyMock.expect(taskClient.getStatusAsync("id2"))
            .andReturn(Futures.immediateFuture(Status.READING));
    EasyMock.expect(taskClient.getStatusAsync("id3"))
            .andReturn(Futures.immediateFuture(Status.READING));
    EasyMock.expect(taskClient.getStartTimeAsync("id1")).andReturn(Futures.immediateFuture(startTime));
    EasyMock.expect(taskClient.getStartTimeAsync("id2")).andReturn(Futures.immediateFuture(startTime));
    EasyMock.expect(taskClient.getStartTimeAsync("id3")).andReturn(Futures.immediateFuture(startTime));

    TreeMap<Integer, Map<String, String>> checkpoints = new TreeMap<>();
    checkpoints.put(0, ImmutableMap.of(
        shardId1,
        "10",
        shardId0,
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
  public void testCheckpointForInactiveTaskGroup()
      throws InterruptedException, ExecutionException, TimeoutException, JsonProcessingException
  {
    supervisor = getSupervisor(2, 1, true, "PT1S", null, null, false);
    //not adding any events
    final Task id1;
    id1 = createKinesisIndexTask(
        "id1",
        DATASOURCE,
        0,
        new SeekableStreamPartitions<>("stream", ImmutableMap.of(
            shardId1,
            "0",
            shardId0,
            "0"
        )),
        new SeekableStreamPartitions<>("stream", ImmutableMap.of(
            shardId1,
            SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER,
            shardId0,
            SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER
        )),
        null,
        null
    );

    final Task id2 = createKinesisIndexTask(
        "id2",
        DATASOURCE,
        0,
        new SeekableStreamPartitions<>("stream", ImmutableMap.of(
            shardId1,
            "10",
            shardId0,
            "20"
        )),
        new SeekableStreamPartitions<>("stream", ImmutableMap.of(
            shardId1,
            SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER,
            shardId0,
            SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER
        )),
        null,
        null
    );

    final Task id3 = createKinesisIndexTask(
        "id3",
        DATASOURCE,
        0,
        new SeekableStreamPartitions<>("stream", ImmutableMap.of(
            shardId1,
            "10",
            shardId0,
            "20"
        )),
        new SeekableStreamPartitions<>("stream", ImmutableMap.of(
            shardId1,
            SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER,
            shardId0,
            SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER
        )),
        null,
        null
    );

    final TaskLocation location1 = new TaskLocation("testHost", 1234, -1);
    final TaskLocation location2 = new TaskLocation("testHost2", 145, -1);
    Collection workItems = new ArrayList<>();
    workItems.add(new TestTaskRunnerWorkItem(id1, null, location1));
    workItems.add(new TestTaskRunnerWorkItem(id2, null, location2));
    workItems.add(new TestTaskRunnerWorkItem(id2, null, location2));

    supervisorRecordSupplier.assign(anyObject());
    expectLastCall().anyTimes();
    expect(supervisorRecordSupplier.getPartitionIds(stream)).andReturn(ImmutableSet.of(shardId1, shardId0)).anyTimes();
    expect(supervisorRecordSupplier.getAssignment()).andReturn(ImmutableSet.of(shard1Partition, shard0Partition))
                                                    .anyTimes();
    supervisorRecordSupplier.seekToLatest(anyObject());
    expectLastCall().anyTimes();
    expect(supervisorRecordSupplier.getEarliestSequenceNumber(anyObject())).andReturn("0").anyTimes();
    expect(supervisorRecordSupplier.getLatestSequenceNumber(shard1Partition)).andReturn("12").anyTimes();
    expect(supervisorRecordSupplier.getLatestSequenceNumber(shard0Partition)).andReturn("1").anyTimes();
    supervisorRecordSupplier.seek(anyObject(), anyString());
    expectLastCall().anyTimes();

    expect(taskRunner.getRunningTasks()).andReturn(workItems).anyTimes();
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of(id1, id2, id3)).anyTimes();
    expect(taskStorage.getStatus("id1")).andReturn(Optional.of(TaskStatus.running("id1"))).anyTimes();
    expect(taskStorage.getStatus("id2")).andReturn(Optional.of(TaskStatus.running("id2"))).anyTimes();
    expect(taskStorage.getStatus("id3")).andReturn(Optional.of(TaskStatus.running("id3"))).anyTimes();
    expect(taskStorage.getTask("id1")).andReturn(Optional.of(id1)).anyTimes();
    expect(taskStorage.getTask("id2")).andReturn(Optional.of(id2)).anyTimes();
    expect(taskStorage.getTask("id3")).andReturn(Optional.of(id3)).anyTimes();
    expect(
        indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(new KinesisDataSourceMetadata(
        null)
    ).anyTimes();
    expect(taskClient.getStatusAsync("id1")).andReturn(Futures.immediateFuture(Status.READING));
    expect(taskClient.getStatusAsync("id2")).andReturn(Futures.immediateFuture(Status.READING));
    expect(taskClient.getStatusAsync("id3")).andReturn(Futures.immediateFuture(Status.READING));

    final DateTime startTime = DateTimes.nowUtc();
    expect(taskClient.getStartTimeAsync("id1")).andReturn(Futures.immediateFuture(startTime));
    expect(taskClient.getStartTimeAsync("id2")).andReturn(Futures.immediateFuture(startTime));
    expect(taskClient.getStartTimeAsync("id3")).andReturn(Futures.immediateFuture(startTime));

    final TreeMap<Integer, Map<String, String>> checkpoints = new TreeMap<>();
    checkpoints.put(0, ImmutableMap.of(
        shardId1,
        "10",
        shardId0,
        "20"
    ));
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("id1"), EasyMock.anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints))
        .times(1);
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("id2"), EasyMock.anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints))
        .times(1);
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("id3"), EasyMock.anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints))
        .times(1);

    taskRunner.registerListener(EasyMock.anyObject(TaskRunnerListener.class), EasyMock.anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();

    final Map<String, String> fakeCheckpoints = Collections.emptyMap();
    supervisor.moveTaskGroupToPendingCompletion(0);
    supervisor.checkpoint(
        0,
        ((KinesisIndexTask) id1).getIOConfig().getBaseSequenceName(),
        new KinesisDataSourceMetadata(new SeekableStreamPartitions<>(stream, checkpoints.get(0))),
        new KinesisDataSourceMetadata(new SeekableStreamPartitions<>(stream, fakeCheckpoints))
    );

    while (supervisor.getNoticesQueueSize() > 0) {
      Thread.sleep(100);
    }

    verifyAll();

    Assert.assertNull(serviceEmitter.getStackTrace(), serviceEmitter.getStackTrace());
    Assert.assertNull(serviceEmitter.getExceptionMessage(), serviceEmitter.getExceptionMessage());
    Assert.assertNull(serviceEmitter.getExceptionClass());
  }

  @Test(timeout = 60_000L)
  public void testCheckpointForUnknownTaskGroup()
      throws InterruptedException
  {
    supervisor = getSupervisor(2, 1, true, "PT1S", null, null, false);


    supervisorRecordSupplier.assign(anyObject());
    expectLastCall().anyTimes();
    expect(supervisorRecordSupplier.getPartitionIds(stream)).andReturn(ImmutableSet.of(shardId1, shardId0)).anyTimes();
    expect(supervisorRecordSupplier.getAssignment()).andReturn(ImmutableSet.of(shard1Partition, shard0Partition))
                                                    .anyTimes();
    supervisorRecordSupplier.seekToLatest(anyObject());
    expectLastCall().anyTimes();
    expect(supervisorRecordSupplier.getEarliestSequenceNumber(anyObject())).andReturn("0").anyTimes();
    expect(supervisorRecordSupplier.getLatestSequenceNumber(shard1Partition)).andReturn("12").anyTimes();
    expect(supervisorRecordSupplier.getLatestSequenceNumber(shard0Partition)).andReturn("1").anyTimes();
    supervisorRecordSupplier.seek(anyObject(), anyString());
    expectLastCall().anyTimes();

    //not adding any events
    final Task id1 = createKinesisIndexTask(
        "id1",
        DATASOURCE,
        0,
        new SeekableStreamPartitions<>("stream", ImmutableMap.of(
            shardId1,
            "0",
            shardId0,
            "0"
        )),
        new SeekableStreamPartitions<>("stream", ImmutableMap.of(
            shardId1,
            SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER,
            shardId0,
            SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER
        )),
        null,
        null
    );

    final Task id2 = createKinesisIndexTask(
        "id2",
        DATASOURCE,
        0,
        new SeekableStreamPartitions<>("stream", ImmutableMap.of(
            shardId1,
            "10",
            shardId0,
            "20"
        )),
        new SeekableStreamPartitions<>("stream", ImmutableMap.of(
            shardId1,
            SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER,
            shardId0,
            SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER
        )),
        null,
        null
    );

    final Task id3 = createKinesisIndexTask(
        "id3",
        DATASOURCE,
        0,
        new SeekableStreamPartitions<>("stream", ImmutableMap.of(
            shardId1,
            "10",
            shardId0,
            "20"
        )),
        new SeekableStreamPartitions<>("stream", ImmutableMap.of(
            shardId1,
            SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER,
            shardId0,
            SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER
        )),
        null,
        null
    );

    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of(id1, id2, id3)).anyTimes();
    expect(taskStorage.getStatus("id1")).andReturn(Optional.of(TaskStatus.running("id1"))).anyTimes();
    expect(taskStorage.getStatus("id2")).andReturn(Optional.of(TaskStatus.running("id2"))).anyTimes();
    expect(taskStorage.getStatus("id3")).andReturn(Optional.of(TaskStatus.running("id3"))).anyTimes();
    expect(taskStorage.getTask("id1")).andReturn(Optional.of(id1)).anyTimes();
    expect(taskStorage.getTask("id2")).andReturn(Optional.of(id2)).anyTimes();
    expect(taskStorage.getTask("id3")).andReturn(Optional.of(id3)).anyTimes();
    expect(
        indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(new KinesisDataSourceMetadata(
        null)
    ).anyTimes();

    replayAll();

    supervisor.start();

    supervisor.checkpoint(
        0,
        ((KinesisIndexTask) id1).getIOConfig().getBaseSequenceName(),
        new KinesisDataSourceMetadata(new SeekableStreamPartitions<>(stream, Collections.emptyMap())),
        new KinesisDataSourceMetadata(new SeekableStreamPartitions<>(stream, Collections.emptyMap()))
    );

    while (supervisor.getNoticesQueueSize() > 0) {
      Thread.sleep(100);
    }

    verifyAll();

    while (serviceEmitter.getStackTrace() == null) {
      Thread.sleep(100);
    }

    Assert.assertTrue(serviceEmitter.getStackTrace()
                                    .startsWith("org.apache.druid.java.util.common.ISE: WTH?! cannot find"));
    Assert.assertEquals(
        "WTH?! cannot find taskGroup [0] among all activelyReadingTaskGroups [{}]",
        serviceEmitter.getExceptionMessage()
    );
    Assert.assertEquals(ISE.class, serviceEmitter.getExceptionClass());
  }

  @Test(timeout = 60_000L)
  public void testCheckpointWithNullTaskGroupId()
      throws InterruptedException, ExecutionException, TimeoutException, JsonProcessingException
  {
    supervisor = getSupervisor(1, 3, true, "PT1S", null, null, false);
    //not adding any events
    final Task id1 = createKinesisIndexTask(
        "id1",
        DATASOURCE,
        0,
        new SeekableStreamPartitions<>("stream", ImmutableMap.of(
            shardId1,
            "0"
        )),
        new SeekableStreamPartitions<>("stream", ImmutableMap.of(
            shardId1,
            SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER
        )),
        null,
        null
    );

    final Task id2 = createKinesisIndexTask(
        "id2",
        DATASOURCE,
        0,
        new SeekableStreamPartitions<>("stream", ImmutableMap.of(
            shardId1,
            "0"
        )),
        new SeekableStreamPartitions<>("stream", ImmutableMap.of(
            shardId1,
            SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER
        )),
        null,
        null
    );

    final Task id3 = createKinesisIndexTask(
        "id3",
        DATASOURCE,
        0,
        new SeekableStreamPartitions<>("stream", ImmutableMap.of(
            shardId1,
            "0"
        )),
        new SeekableStreamPartitions<>("stream", ImmutableMap.of(
            shardId1,
            SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER
        )),
        null,
        null
    );

    expect(supervisorRecordSupplier.getPartitionIds(stream)).andReturn(Collections.emptySet()).anyTimes();
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of(id1, id2, id3)).anyTimes();
    expect(taskStorage.getStatus("id1")).andReturn(Optional.of(TaskStatus.running("id1"))).anyTimes();
    expect(taskStorage.getStatus("id2")).andReturn(Optional.of(TaskStatus.running("id2"))).anyTimes();
    expect(taskStorage.getStatus("id3")).andReturn(Optional.of(TaskStatus.running("id3"))).anyTimes();
    expect(taskStorage.getTask("id1")).andReturn(Optional.of(id1)).anyTimes();
    expect(taskStorage.getTask("id2")).andReturn(Optional.of(id2)).anyTimes();
    expect(taskStorage.getTask("id3")).andReturn(Optional.of(id3)).anyTimes();
    expect(
        indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(new KinesisDataSourceMetadata(
        null)
    ).anyTimes();
    taskRunner.registerListener(EasyMock.anyObject(TaskRunnerListener.class), EasyMock.anyObject(Executor.class));
    expect(taskClient.getStatusAsync(EasyMock.anyString()))
        .andReturn(Futures.immediateFuture(Status.READING))
        .anyTimes();
    final TreeMap<Integer, Map<String, String>> checkpoints = new TreeMap<>();
    checkpoints.put(0, ImmutableMap.of(shardId1, "0"));
    expect(taskClient.getCheckpointsAsync(EasyMock.anyString(), EasyMock.anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints))
        .times(3);
    expect(taskClient.getStartTimeAsync(EasyMock.anyString()))
        .andReturn(Futures.immediateFuture(DateTimes.nowUtc()))
        .anyTimes();
    expect(taskClient.pauseAsync(EasyMock.anyString()))
        .andReturn(Futures.immediateFuture(ImmutableMap.of(shardId1, "10")))
        .anyTimes();
    expect(taskClient.setEndOffsetsAsync(
        EasyMock.anyString(),
        EasyMock.eq(ImmutableMap.of("0", "10")),
        EasyMock.anyBoolean()
    ))
        .andReturn(Futures.immediateFuture(true))
        .anyTimes();

    replayAll();

    supervisor.start();

    supervisor.runInternal();

    final TreeMap<Integer, Map<String, String>> newCheckpoints = new TreeMap<>();
    newCheckpoints.put(0, ImmutableMap.of(shardId1, "10"));
    supervisor.checkpoint(
        null,
        ((KinesisIndexTask) id1).getIOConfig().getBaseSequenceName(),
        new KinesisDataSourceMetadata(new SeekableStreamPartitions<>(stream, checkpoints.get(0))),
        new KinesisDataSourceMetadata(new SeekableStreamPartitions<>(stream, newCheckpoints.get(0)))
    );

    while (supervisor.getNoticesQueueSize() > 0) {
      Thread.sleep(100);
    }

    verifyAll();
  }


  @Test
  public void testSuspendedNoRunningTasks() throws Exception
  {
    supervisor = getSupervisor(1, 1, true, "PT1H", null, null, true);

    expect(supervisorRecordSupplier.getPartitionIds(anyObject())).andReturn(Collections.emptySet()).anyTimes();
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
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

    final TaskLocation location1 = new TaskLocation("testHost", 1234, -1);
    final TaskLocation location2 = new TaskLocation("testHost2", 145, -1);
    final DateTime startTime = DateTimes.nowUtc();

    supervisor = getSupervisor(2, 1, true, "PT1H", null, null, true);

    supervisorRecordSupplier.assign(anyObject());
    expectLastCall().anyTimes();
    expect(supervisorRecordSupplier.getPartitionIds(stream)).andReturn(ImmutableSet.of(shardId1, shardId0)).anyTimes();
    expect(supervisorRecordSupplier.getAssignment()).andReturn(ImmutableSet.of(shard1Partition, shard0Partition))
                                                    .anyTimes();
    supervisorRecordSupplier.seekToLatest(anyObject());
    expectLastCall().anyTimes();
    expect(supervisorRecordSupplier.getEarliestSequenceNumber(anyObject())).andReturn("0").anyTimes();
    expect(supervisorRecordSupplier.getLatestSequenceNumber(shard1Partition)).andReturn("12").anyTimes();
    expect(supervisorRecordSupplier.getLatestSequenceNumber(shard0Partition)).andReturn("1").anyTimes();
    supervisorRecordSupplier.seek(anyObject(), anyString());
    expectLastCall().anyTimes();


    Task id1 = createKinesisIndexTask(
        "id1",
        DATASOURCE,
        0,
        new SeekableStreamPartitions<>("stream", ImmutableMap.of(
            shardId1,
            "0",
            shardId0,
            "0"
        )),
        new SeekableStreamPartitions<>("stream", ImmutableMap.of(
            shardId1,
            SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER,
            shardId0,
            SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER
        )),
        null,
        null
    );

    Task id2 = createKinesisIndexTask(
        "id2",
        DATASOURCE,
        0,
        new SeekableStreamPartitions<>("stream", ImmutableMap.of(
            shardId1,
            "3",
            shardId0,
            "1"
        )),
        new SeekableStreamPartitions<>("stream", ImmutableMap.of(
            shardId1,
            SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER,
            shardId0,
            SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER
        )),
        null,
        null
    );

    Task id3 = createKinesisIndexTask(
        "id3",
        DATASOURCE,
        0,
        new SeekableStreamPartitions<>("stream", ImmutableMap.of(
            shardId1,
            "3",
            shardId0,
            "1"
        )),
        new SeekableStreamPartitions<>("stream", ImmutableMap.of(
            shardId1,
            SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER,
            shardId0,
            SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER
        )),
        null,
        null
    );

    Collection workItems = new ArrayList<>();
    workItems.add(new TestTaskRunnerWorkItem(id1, null, location1));
    workItems.add(new TestTaskRunnerWorkItem(id2, null, location2));

    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(workItems).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of(id1, id2, id3)).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id1")).andReturn(Optional.of(TaskStatus.running("id1"))).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id2")).andReturn(Optional.of(TaskStatus.running("id2"))).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id3")).andReturn(Optional.of(TaskStatus.running("id3"))).anyTimes();
    EasyMock.expect(taskStorage.getTask("id1")).andReturn(Optional.of(id1)).anyTimes();
    EasyMock.expect(taskStorage.getTask("id2")).andReturn(Optional.of(id2)).anyTimes();
    EasyMock.expect(taskStorage.getTask("id3")).andReturn(Optional.of(id3)).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
            null
        )
    ).anyTimes();
    EasyMock.expect(taskClient.getStatusAsync("id1"))
            .andReturn(Futures.immediateFuture(Status.PUBLISHING));
    EasyMock.expect(taskClient.getStatusAsync("id2"))
            .andReturn(Futures.immediateFuture(Status.READING));
    EasyMock.expect(taskClient.getStatusAsync("id3"))
            .andReturn(Futures.immediateFuture(Status.READING));
    EasyMock.expect(taskClient.getStartTimeAsync("id2")).andReturn(Futures.immediateFuture(startTime));
    EasyMock.expect(taskClient.getStartTimeAsync("id3")).andReturn(Futures.immediateFuture(startTime));
    EasyMock.expect(taskClient.getEndOffsets("id1")).andReturn(ImmutableMap.of(
        shardId1,
        "3",
        shardId0,
        "1"
    ));

    // getCheckpoints will not be called for id1 as it is in publishing state
    TreeMap<Integer, Map<String, String>> checkpoints = new TreeMap<>();
    checkpoints.put(0, ImmutableMap.of(
        shardId1,
        "3",
        shardId0,
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
                shardId1,
                "12",
                shardId0,
                "1"
            )));
    EasyMock.expect(taskClient.setEndOffsetsAsync("id2", ImmutableMap.of(
        shardId1,
        "12",
        shardId0,
        "1"
    ), true))
            .andReturn(Futures.immediateFuture(true));
    taskQueue.shutdown("id3", "Killing task for graceful shutdown");
    expectLastCall().times(1);
    taskQueue.shutdown("id3", "Killing task [%s] which hasn't been assigned to a worker", "id3");
    expectLastCall().times(1);

    replayAll();
    supervisor.start();
    supervisor.runInternal();
    verifyAll();
  }

  @Test
  public void testResetSuspended() throws Exception
  {
    expect(supervisorRecordSupplier.getPartitionIds(anyObject())).andReturn(Collections.emptySet()).anyTimes();
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(Collections.EMPTY_LIST).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();
    taskRunner.registerListener(EasyMock.anyObject(TaskRunnerListener.class), EasyMock.anyObject(Executor.class));
    replayAll();

    supervisor = getSupervisor(1, 1, true, "PT1H", null, null, true);
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
    supervisor = getSupervisor(1, 2, true, "PT1H", null, null, false);
    supervisor.addTaskGroupToActivelyReadingTaskGroup(
        supervisor.getTaskGroupIdForPartition("0"),
        ImmutableMap.of("0", "0"),
        Optional.absent(),
        Optional.absent(),
        ImmutableSet.of("task1"),
        ImmutableSet.of()
    );

    supervisor.addTaskGroupToPendingCompletionTaskGroup(
        supervisor.getTaskGroupIdForPartition("1"),
        ImmutableMap.of("0", "0"),
        Optional.absent(),
        Optional.absent(),
        ImmutableSet.of("task2"),
        ImmutableSet.of()
    );

    expect(taskClient.getMovingAveragesAsync("task1")).andReturn(Futures.immediateFuture(ImmutableMap.of(
        "prop1",
        "val1"
    ))).times(1);

    expect(taskClient.getMovingAveragesAsync("task2")).andReturn(Futures.immediateFuture(ImmutableMap.of(
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

  private KinesisSupervisor getSupervisor(
      int replicas,
      int taskCount,
      boolean useEarliestOffset,
      String duration,
      Period lateMessageRejectionPeriod,
      Period earlyMessageRejectionPeriod
  )
  {
    return getSupervisor(
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

  private KinesisSupervisor getSupervisor(
      int replicas,
      int taskCount,
      boolean useEarliestOffset,
      String duration,
      Period lateMessageRejectionPeriod,
      Period earlyMessageRejectionPeriod,
      boolean suspended,
      Integer recordsPerFetch,
      Integer fetchDelayMillis

  )
  {
    KinesisSupervisorIOConfig KinesisSupervisorIOConfig = new KinesisSupervisorIOConfig(
        stream,
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
        recordsPerFetch,
        fetchDelayMillis,
        null,
        null
    );

    KinesisIndexTaskClientFactory taskClientFactory = new KinesisIndexTaskClientFactory(
        null,
        null
    )
    {
      @Override
      public KinesisIndexTaskClient build(
          TaskInfoProvider taskInfoProvider,
          String dataSource,
          int numThreads,
          Duration httpTimeout,
          long numRetries
      )
      {
        Assert.assertEquals(TEST_CHAT_THREADS, numThreads);
        Assert.assertEquals(TEST_HTTP_TIMEOUT.toStandardDuration(), httpTimeout);
        Assert.assertEquals(TEST_CHAT_RETRIES, numRetries);
        return taskClient;
      }
    };

    return new TestableKinesisSupervisor(
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        taskClientFactory,
        objectMapper,
        new KinesisSupervisorSpec(
            dataSchema,
            tuningConfig,
            KinesisSupervisorIOConfig,
            null,
            suspended,
            taskStorage,
            taskMaster,
            indexerMetadataStorageCoordinator,
            taskClientFactory,
            objectMapper,
            new NoopServiceEmitter(),
            new DruidMonitorSchedulerConfig(),
            rowIngestionMetersFactory,
            null
        ),
        rowIngestionMetersFactory
    );
  }

  private static DataSchema getDataSchema(String dataSource)
  {
    List<DimensionSchema> dimensions = new ArrayList<>();
    dimensions.add(StringDimensionSchema.create("dim1"));
    dimensions.add(StringDimensionSchema.create("dim2"));

    return new DataSchema(
        dataSource,
        objectMapper.convertValue(
            new StringInputRowParser(
                new JSONParseSpec(
                    new TimestampSpec("timestamp", "iso", null),
                    new DimensionsSpec(
                        dimensions,
                        null,
                        null
                    ),
                    new JSONPathSpec(true, ImmutableList.of()),
                    ImmutableMap.of()
                ),
                StandardCharsets.UTF_8.name()
            ),
            Map.class
        ),
        new AggregatorFactory[]{new CountAggregatorFactory("rows")},
        new UniformGranularitySpec(
            Granularities.HOUR,
            Granularities.NONE,
            ImmutableList.of()
        ),
        null,
        objectMapper
    );
  }

  private static List<byte[]> JB(
      String timestamp,
      String dim1,
      String dim2,
      String dimLong,
      String dimFloat,
      String met1
  )
  {
    try {
      return Collections.singletonList(new ObjectMapper().writeValueAsBytes(
          ImmutableMap.builder()
                      .put("timestamp", timestamp)
                      .put("dim1", dim1)
                      .put("dim2", dim2)
                      .put("dimLong", dimLong)
                      .put("dimFloat", dimFloat)
                      .put("met1", met1)
                      .build()
      ));
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private KinesisIndexTask createKinesisIndexTask(
      String id,
      String dataSource,
      int taskGroupId,
      SeekableStreamPartitions<String, String> startPartitions,
      SeekableStreamPartitions<String, String> endPartitions,
      DateTime minimumMessageTime,
      DateTime maximumMessageTime
  )
  {
    return new KinesisIndexTask(
        id,
        null,
        getDataSchema(dataSource),
        tuningConfig,
        new KinesisIndexTaskIOConfig(
            null,
            "sequenceName-" + taskGroupId,
            startPartitions,
            endPartitions,
            true,
            minimumMessageTime,
            maximumMessageTime,
            "awsEndpoint",
            null,
            null,
            null,
            null,
            null
        ),
        Collections.emptyMap(),
        null,
        null,
        rowIngestionMetersFactory,
        null
    );
  }

  private KinesisSupervisor getSupervisor(
      int replicas,
      int taskCount,
      boolean useEarliestOffset,
      String duration,
      Period lateMessageRejectionPeriod,
      Period earlyMessageRejectionPeriod,
      boolean suspended
  )
  {
    KinesisSupervisorIOConfig KinesisSupervisorIOConfig = new KinesisSupervisorIOConfig(
        stream,
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
        null
    );

    KinesisIndexTaskClientFactory taskClientFactory = new KinesisIndexTaskClientFactory(
        null,
        null
    )
    {
      @Override
      public KinesisIndexTaskClient build(
          TaskInfoProvider taskInfoProvider,
          String dataSource,
          int numThreads,
          Duration httpTimeout,
          long numRetries
      )
      {
        Assert.assertEquals(TEST_CHAT_THREADS, numThreads);
        Assert.assertEquals(TEST_HTTP_TIMEOUT.toStandardDuration(), httpTimeout);
        Assert.assertEquals(TEST_CHAT_RETRIES, numRetries);
        return taskClient;
      }
    };

    return new TestableKinesisSupervisor(
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        taskClientFactory,
        objectMapper,
        new KinesisSupervisorSpec(
            dataSchema,
            tuningConfig,
            KinesisSupervisorIOConfig,
            null,
            suspended,
            taskStorage,
            taskMaster,
            indexerMetadataStorageCoordinator,
            taskClientFactory,
            objectMapper,
            new NoopServiceEmitter(),
            new DruidMonitorSchedulerConfig(),
            rowIngestionMetersFactory,
            null
        ),
        rowIngestionMetersFactory
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
    private final KinesisSupervisorSpec spec;

    public TestableKinesisSupervisor(
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

    @Override
    protected String generateSequenceName(
        Map<String, String> startPartitions,
        Optional<DateTime> minimumMessageTime,
        Optional<DateTime> maximumMessageTime
    )
    {
      final int groupId = getTaskGroupIdForPartition(startPartitions.keySet().iterator().next());
      return StringUtils.format("sequenceName-%d", groupId);
    }

    @Override
    protected RecordSupplier<String, String> setupRecordSupplier()
    {
      return supervisorRecordSupplier;
    }


  }
}
