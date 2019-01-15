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

package org.apache.druid.indexing.kafka.supervisor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZkUtils;
import org.apache.curator.test.TestingCluster;
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
import org.apache.druid.indexing.kafka.KafkaDataSourceMetadata;
import org.apache.druid.indexing.kafka.KafkaIndexTask;
import org.apache.druid.indexing.kafka.KafkaIndexTaskClient;
import org.apache.druid.indexing.kafka.KafkaIndexTaskClientFactory;
import org.apache.druid.indexing.kafka.KafkaIndexTaskIOConfig;
import org.apache.druid.indexing.kafka.test.TestBroker;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskQueue;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.indexing.overlord.TaskRunnerListener;
import org.apache.druid.indexing.overlord.TaskRunnerWorkItem;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.supervisor.SupervisorReport;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskRunner.Status;
import org.apache.druid.indexing.seekablestream.SeekableStreamPartitions;
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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.security.JaasUtils;
import org.easymock.Capture;
import org.easymock.CaptureType;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.easymock.IAnswer;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Period;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;

@RunWith(Parameterized.class)
public class KafkaSupervisorTest extends EasyMockSupport
{
  private static final ObjectMapper objectMapper = TestHelper.makeJsonMapper();
  private static final String TOPIC_PREFIX = "testTopic";
  private static final String DATASOURCE = "testDS";
  private static final int NUM_PARTITIONS = 3;
  private static final int TEST_CHAT_THREADS = 3;
  private static final long TEST_CHAT_RETRIES = 9L;
  private static final Period TEST_HTTP_TIMEOUT = new Period("PT10S");
  private static final Period TEST_SHUTDOWN_TIMEOUT = new Period("PT80S");

  private static TestingCluster zkServer;
  private static TestBroker kafkaServer;
  private static String kafkaHost;
  private static DataSchema dataSchema;
  private static int topicPostfix;
  private static ZkUtils zkUtils;


  private final int numThreads;

  private KafkaSupervisor supervisor;
  private KafkaSupervisorTuningConfig tuningConfig;
  private TaskStorage taskStorage;
  private TaskMaster taskMaster;
  private TaskRunner taskRunner;
  private IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator;
  private KafkaIndexTaskClient taskClient;
  private TaskQueue taskQueue;
  private String topic;
  private RowIngestionMetersFactory rowIngestionMetersFactory;
  private ExceptionCapturingServiceEmitter serviceEmitter;

  private static String getTopic()
  {
    return TOPIC_PREFIX + topicPostfix++;
  }

  @Parameterized.Parameters(name = "numThreads = {0}")
  public static Iterable<Object[]> constructorFeeder()
  {
    return ImmutableList.of(new Object[]{1}, new Object[]{8});
  }

  public KafkaSupervisorTest(int numThreads)
  {
    this.numThreads = numThreads;
  }

  @BeforeClass
  public static void setupClass() throws Exception
  {
    zkServer = new TestingCluster(1);
    zkServer.start();

    kafkaServer = new TestBroker(
        zkServer.getConnectString(),
        null,
        1,
        ImmutableMap.of(
            "num.partitions",
            String.valueOf(NUM_PARTITIONS),
            "auto.create.topics.enable",
            String.valueOf(false)
        )
    );
    kafkaServer.start();
    kafkaHost = StringUtils.format("localhost:%d", kafkaServer.getPort());

    dataSchema = getDataSchema(DATASOURCE);

    zkUtils = ZkUtils.apply(zkServer.getConnectString(), 30000, 30000, JaasUtils.isZkSecurityEnabled());
  }

  @Before
  public void setupTest()
  {
    taskStorage = createMock(TaskStorage.class);
    taskMaster = createMock(TaskMaster.class);
    taskRunner = createMock(TaskRunner.class);
    indexerMetadataStorageCoordinator = createMock(IndexerMetadataStorageCoordinator.class);
    taskClient = createMock(KafkaIndexTaskClient.class);
    taskQueue = createMock(TaskQueue.class);

    tuningConfig = new KafkaSupervisorTuningConfig(
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
        numThreads,
        TEST_CHAT_THREADS,
        TEST_CHAT_RETRIES,
        TEST_HTTP_TIMEOUT,
        TEST_SHUTDOWN_TIMEOUT,
        null,
        null,
        null,
        null,
        null
    );

    topic = getTopic();
    rowIngestionMetersFactory = new TestUtils().getRowIngestionMetersFactory();
    serviceEmitter = new ExceptionCapturingServiceEmitter();
    EmittingLogger.registerEmitter(serviceEmitter);
  }

  @After
  public void tearDownTest()
  {
    supervisor = null;
  }

  @AfterClass
  public static void tearDownClass() throws IOException
  {
    kafkaServer.close();
    kafkaServer = null;

    zkServer.stop();
    zkServer = null;

    zkUtils.close();
    zkUtils = null;
  }

  @Test
  public void testNoInitialState() throws Exception
  {
    supervisor = getSupervisor(1, 1, true, "PT1H", null, null, false);
    addSomeEvents(1);

    Capture<KafkaIndexTask> captured = Capture.newInstance();
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            null
        )
    ).anyTimes();
    expect(taskQueue.add(capture(captured))).andReturn(true);
    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    KafkaIndexTask task = captured.getValue();
    Assert.assertEquals(dataSchema, task.getDataSchema());
    Assert.assertEquals(tuningConfig.convertToTaskTuningConfig(), task.getTuningConfig());

    KafkaIndexTaskIOConfig taskConfig = task.getIOConfig();
    Assert.assertEquals(kafkaHost, taskConfig.getConsumerProperties().get("bootstrap.servers"));
    Assert.assertEquals("myCustomValue", taskConfig.getConsumerProperties().get("myCustomKey"));
    Assert.assertEquals("sequenceName-0", taskConfig.getBaseSequenceName());
    Assert.assertTrue("isUseTransaction", taskConfig.isUseTransaction());
    Assert.assertFalse("minimumMessageTime", taskConfig.getMinimumMessageTime().isPresent());
    Assert.assertFalse("maximumMessageTime", taskConfig.getMaximumMessageTime().isPresent());
    Assert.assertFalse("skipOffsetGaps", taskConfig.isSkipOffsetGaps());

    Assert.assertEquals(topic, taskConfig.getStartPartitions().getStream());
    Assert.assertEquals(0L, (long) taskConfig.getStartPartitions().getPartitionSequenceNumberMap().get(0));
    Assert.assertEquals(0L, (long) taskConfig.getStartPartitions().getPartitionSequenceNumberMap().get(1));
    Assert.assertEquals(0L, (long) taskConfig.getStartPartitions().getPartitionSequenceNumberMap().get(2));

    Assert.assertEquals(topic, taskConfig.getEndPartitions().getStream());
    Assert.assertEquals(Long.MAX_VALUE, (long) taskConfig.getEndPartitions().getPartitionSequenceNumberMap().get(0));
    Assert.assertEquals(Long.MAX_VALUE, (long) taskConfig.getEndPartitions().getPartitionSequenceNumberMap().get(1));
    Assert.assertEquals(Long.MAX_VALUE, (long) taskConfig.getEndPartitions().getPartitionSequenceNumberMap().get(2));
  }

  @Test
  public void testSkipOffsetGaps() throws Exception
  {
    supervisor = getSupervisor(1, 1, true, "PT1H", null, null, true);
    addSomeEvents(1);

    Capture<KafkaIndexTask> captured = Capture.newInstance();
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            null
        )
    ).anyTimes();
    expect(taskQueue.add(capture(captured))).andReturn(true);
    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    KafkaIndexTask task = captured.getValue();
    KafkaIndexTaskIOConfig taskConfig = task.getIOConfig();

    Assert.assertTrue("skipOffsetGaps", taskConfig.isSkipOffsetGaps());
  }

  @Test
  public void testMultiTask() throws Exception
  {
    supervisor = getSupervisor(1, 2, true, "PT1H", null, null, false);
    addSomeEvents(1);

    Capture<KafkaIndexTask> captured = Capture.newInstance(CaptureType.ALL);
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            null
        )
    ).anyTimes();
    expect(taskQueue.add(capture(captured))).andReturn(true).times(2);
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    KafkaIndexTask task1 = captured.getValues().get(0);
    Assert.assertEquals(2, task1.getIOConfig().getStartPartitions().getPartitionSequenceNumberMap().size());
    Assert.assertEquals(2, task1.getIOConfig().getEndPartitions().getPartitionSequenceNumberMap().size());
    Assert.assertEquals(0L, (long) task1.getIOConfig().getStartPartitions().getPartitionSequenceNumberMap().get(0));
    Assert.assertEquals(
        Long.MAX_VALUE,
        (long) task1.getIOConfig().getEndPartitions().getPartitionSequenceNumberMap().get(0)
    );
    Assert.assertEquals(0L, (long) task1.getIOConfig().getStartPartitions().getPartitionSequenceNumberMap().get(2));
    Assert.assertEquals(
        Long.MAX_VALUE,
        (long) task1.getIOConfig().getEndPartitions().getPartitionSequenceNumberMap().get(2)
    );

    KafkaIndexTask task2 = captured.getValues().get(1);
    Assert.assertEquals(1, task2.getIOConfig().getStartPartitions().getPartitionSequenceNumberMap().size());
    Assert.assertEquals(1, task2.getIOConfig().getEndPartitions().getPartitionSequenceNumberMap().size());
    Assert.assertEquals(0L, (long) task2.getIOConfig().getStartPartitions().getPartitionSequenceNumberMap().get(1));
    Assert.assertEquals(
        Long.MAX_VALUE,
        (long) task2.getIOConfig().getEndPartitions().getPartitionSequenceNumberMap().get(1)
    );
  }

  @Test
  public void testReplicas() throws Exception
  {
    supervisor = getSupervisor(2, 1, true, "PT1H", null, null, false);
    addSomeEvents(1);

    Capture<KafkaIndexTask> captured = Capture.newInstance(CaptureType.ALL);
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            null
        )
    ).anyTimes();
    expect(taskQueue.add(capture(captured))).andReturn(true).times(2);
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    KafkaIndexTask task1 = captured.getValues().get(0);
    Assert.assertEquals(3, task1.getIOConfig().getStartPartitions().getPartitionSequenceNumberMap().size());
    Assert.assertEquals(3, task1.getIOConfig().getEndPartitions().getPartitionSequenceNumberMap().size());
    Assert.assertEquals(0L, (long) task1.getIOConfig().getStartPartitions().getPartitionSequenceNumberMap().get(0));
    Assert.assertEquals(0L, (long) task1.getIOConfig().getStartPartitions().getPartitionSequenceNumberMap().get(1));
    Assert.assertEquals(0L, (long) task1.getIOConfig().getStartPartitions().getPartitionSequenceNumberMap().get(2));

    KafkaIndexTask task2 = captured.getValues().get(1);
    Assert.assertEquals(3, task2.getIOConfig().getStartPartitions().getPartitionSequenceNumberMap().size());
    Assert.assertEquals(3, task2.getIOConfig().getEndPartitions().getPartitionSequenceNumberMap().size());
    Assert.assertEquals(0L, (long) task2.getIOConfig().getStartPartitions().getPartitionSequenceNumberMap().get(0));
    Assert.assertEquals(0L, (long) task2.getIOConfig().getStartPartitions().getPartitionSequenceNumberMap().get(1));
    Assert.assertEquals(0L, (long) task2.getIOConfig().getStartPartitions().getPartitionSequenceNumberMap().get(2));
  }

  @Test
  public void testLateMessageRejectionPeriod() throws Exception
  {
    supervisor = getSupervisor(2, 1, true, "PT1H", new Period("PT1H"), null, false);
    addSomeEvents(1);

    Capture<KafkaIndexTask> captured = Capture.newInstance(CaptureType.ALL);
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            null
        )
    ).anyTimes();
    expect(taskQueue.add(capture(captured))).andReturn(true).times(2);
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    KafkaIndexTask task1 = captured.getValues().get(0);
    KafkaIndexTask task2 = captured.getValues().get(1);

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
    supervisor = getSupervisor(2, 1, true, "PT1H", null, new Period("PT1H"), false);
    addSomeEvents(1);

    Capture<KafkaIndexTask> captured = Capture.newInstance(CaptureType.ALL);
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            null
        )
    ).anyTimes();
    expect(taskQueue.add(capture(captured))).andReturn(true).times(2);
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    KafkaIndexTask task1 = captured.getValues().get(0);
    KafkaIndexTask task2 = captured.getValues().get(1);

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
   * Test generating the starting offsets from the partition high water marks in Kafka.
   */
  public void testLatestOffset() throws Exception
  {
    supervisor = getSupervisor(1, 1, false, "PT1H", null, null, false);
    addSomeEvents(1100);

    Capture<KafkaIndexTask> captured = Capture.newInstance();
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            null
        )
    ).anyTimes();
    expect(taskQueue.add(capture(captured))).andReturn(true);
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    KafkaIndexTask task = captured.getValue();
    Assert.assertEquals(1100L, (long) task.getIOConfig().getStartPartitions().getPartitionSequenceNumberMap().get(0));
    Assert.assertEquals(1100L, (long) task.getIOConfig().getStartPartitions().getPartitionSequenceNumberMap().get(1));
    Assert.assertEquals(1100L, (long) task.getIOConfig().getStartPartitions().getPartitionSequenceNumberMap().get(2));
  }

  @Test
  /**
   * Test generating the starting offsets from the partition data stored in druid_dataSource which contains the
   * offsets of the last built segments.
   */
  public void testDatasourceMetadata() throws Exception
  {
    supervisor = getSupervisor(1, 1, true, "PT1H", null, null, false);
    addSomeEvents(100);

    Capture<KafkaIndexTask> captured = Capture.newInstance();
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 10L, 1, 20L, 2, 30L))
        )
    ).anyTimes();
    expect(taskQueue.add(capture(captured))).andReturn(true);
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    KafkaIndexTask task = captured.getValue();
    KafkaIndexTaskIOConfig taskConfig = task.getIOConfig();
    Assert.assertEquals("sequenceName-0", taskConfig.getBaseSequenceName());
    Assert.assertEquals(10L, (long) taskConfig.getStartPartitions().getPartitionSequenceNumberMap().get(0));
    Assert.assertEquals(20L, (long) taskConfig.getStartPartitions().getPartitionSequenceNumberMap().get(1));
    Assert.assertEquals(30L, (long) taskConfig.getStartPartitions().getPartitionSequenceNumberMap().get(2));
  }

  @Test(expected = ISE.class)
  public void testBadMetadataOffsets() throws Exception
  {
    supervisor = getSupervisor(1, 1, true, "PT1H", null, null, false);
    addSomeEvents(1);

    expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 10L, 1, 20L, 2, 30L))
        )
    ).anyTimes();
    replayAll();

    supervisor.start();
    supervisor.runInternal();
  }

  @Test
  public void testKillIncompatibleTasks() throws Exception
  {
    supervisor = getSupervisor(2, 1, true, "PT1H", null, null, false);
    addSomeEvents(1);

    // unexpected # of partitions (kill)
    Task id1 = createKafkaIndexTask(
        "id1",
        DATASOURCE,
        1,
        new SeekableStreamPartitions<>("topic", ImmutableMap.of(0, 0L)),
        new SeekableStreamPartitions<>("topic", ImmutableMap.of(0, 10L)),
        null,
        null
    );

    // correct number of partitions and ranges (don't kill)
    Task id2 = createKafkaIndexTask(
        "id2",
        DATASOURCE,
        0,
        new SeekableStreamPartitions<>("topic", ImmutableMap.of(0, 0L, 1, 0L, 2, 0L)),
        new SeekableStreamPartitions<>("topic", ImmutableMap.of(0, 333L, 1, 333L, 2, 333L)),
        null,
        null
    );

    // unexpected range on partition 2 (kill)
    Task id3 = createKafkaIndexTask(
        "id3",
        DATASOURCE,
        1,
        new SeekableStreamPartitions<>("topic", ImmutableMap.of(0, 0L, 1, 0L, 2, 1L)),
        new SeekableStreamPartitions<>("topic", ImmutableMap.of(0, 333L, 1, 333L, 2, 330L)),
        null,
        null
    );

    // different datasource (don't kill)
    Task id4 = createKafkaIndexTask(
        "id4",
        "other-datasource",
        2,
        new SeekableStreamPartitions<>("topic", ImmutableMap.of(0, 0L)),
        new SeekableStreamPartitions<>("topic", ImmutableMap.of(0, 10L)),
        null,
        null
    );

    // non KafkaIndexTask (don't kill)
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

    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskRunner.getRunningTasks()).andReturn(Collections.EMPTY_LIST).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(existingTasks).anyTimes();
    expect(taskStorage.getStatus("id1")).andReturn(Optional.of(TaskStatus.running("id1"))).anyTimes();
    expect(taskStorage.getStatus("id2")).andReturn(Optional.of(TaskStatus.running("id2"))).anyTimes();
    expect(taskStorage.getStatus("id3")).andReturn(Optional.of(TaskStatus.running("id3"))).anyTimes();
    expect(taskStorage.getTask("id1")).andReturn(Optional.of(id1)).anyTimes();
    expect(taskStorage.getTask("id2")).andReturn(Optional.of(id2)).anyTimes();
    expect(taskStorage.getTask("id3")).andReturn(Optional.of(id3)).anyTimes();
    expect(taskClient.getStatusAsync(EasyMock.anyString()))
        .andReturn(Futures.immediateFuture(Status.NOT_STARTED))
        .anyTimes();
    expect(taskClient.getStartTimeAsync(EasyMock.anyString()))
        .andReturn(Futures.immediateFuture(DateTimes.nowUtc()))
        .anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            null
        )
    ).anyTimes();
    expect(taskClient.stopAsync("id1", false)).andReturn(Futures.immediateFuture(true));
    expect(taskClient.stopAsync("id3", false)).andReturn(Futures.immediateFuture(false));
    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));
    taskQueue.shutdown("id3", "Task [%s] failed to stop in a timely manner, killing task", "id3");

    expect(taskQueue.add(anyObject(Task.class))).andReturn(true);

    TreeMap<Integer, Map<Integer, Long>> checkpoints = new TreeMap<>();
    checkpoints.put(0, ImmutableMap.of(0, 0L, 1, 0L, 2, 0L));
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("id2"), EasyMock.anyBoolean()))
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
    supervisor = getSupervisor(1, 2, true, "PT1H", null, null, false);
    addSomeEvents(1);

    Task id1 = createKafkaIndexTask(
        "id1",
        DATASOURCE,
        0,
        new SeekableStreamPartitions<>("topic", ImmutableMap.of(0, 0L, 2, 0L)),
        new SeekableStreamPartitions<>("topic", ImmutableMap.of(0, Long.MAX_VALUE, 2, Long.MAX_VALUE)),
        null,
        null
    );
    Task id2 = createKafkaIndexTask(
        "id2",
        DATASOURCE,
        1,
        new SeekableStreamPartitions<>("topic", ImmutableMap.of(1, 0L)),
        new SeekableStreamPartitions<>("topic", ImmutableMap.of(1, Long.MAX_VALUE)),
        null,
        null
    );
    Task id3 = createKafkaIndexTask(
        "id3",
        DATASOURCE,
        0,
        new SeekableStreamPartitions<>("topic", ImmutableMap.of(0, 0L, 1, 0L, 2, 0L)),
        new SeekableStreamPartitions<>(
            "topic",
            ImmutableMap.of(0, Long.MAX_VALUE, 1, Long.MAX_VALUE, 2, Long.MAX_VALUE)
        ),
        null,
        null
    );
    Task id4 = createKafkaIndexTask(
        "id4",
        DATASOURCE,
        0,
        new SeekableStreamPartitions<>("topic", ImmutableMap.of(0, 0L, 1, 0L)),
        new SeekableStreamPartitions<>("topic", ImmutableMap.of(0, Long.MAX_VALUE, 1, Long.MAX_VALUE)),
        null,
        null
    );
    Task id5 = createKafkaIndexTask(
        "id5",
        DATASOURCE,
        0,
        new SeekableStreamPartitions<>("topic", ImmutableMap.of(1, 0L, 2, 0L)),
        new SeekableStreamPartitions<>("topic", ImmutableMap.of(1, Long.MAX_VALUE, 2, Long.MAX_VALUE)),
        null,
        null
    );

    List<Task> existingTasks = ImmutableList.of(id1, id2, id3, id4, id5);

    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskRunner.getRunningTasks()).andReturn(Collections.EMPTY_LIST).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(existingTasks).anyTimes();
    expect(taskStorage.getStatus("id1")).andReturn(Optional.of(TaskStatus.running("id1"))).anyTimes();
    expect(taskStorage.getStatus("id2")).andReturn(Optional.of(TaskStatus.running("id2"))).anyTimes();
    expect(taskStorage.getStatus("id3")).andReturn(Optional.of(TaskStatus.running("id3"))).anyTimes();
    expect(taskStorage.getStatus("id4")).andReturn(Optional.of(TaskStatus.running("id4"))).anyTimes();
    expect(taskStorage.getStatus("id5")).andReturn(Optional.of(TaskStatus.running("id5"))).anyTimes();
    expect(taskStorage.getTask("id1")).andReturn(Optional.of(id1)).anyTimes();
    expect(taskStorage.getTask("id2")).andReturn(Optional.of(id2)).anyTimes();
    expect(taskStorage.getTask("id3")).andReturn(Optional.of(id3)).anyTimes();
    expect(taskStorage.getTask("id4")).andReturn(Optional.of(id4)).anyTimes();
    expect(taskStorage.getTask("id5")).andReturn(Optional.of(id5)).anyTimes();
    expect(taskClient.getStatusAsync(EasyMock.anyString()))
        .andReturn(Futures.immediateFuture(Status.NOT_STARTED))
        .anyTimes();
    expect(taskClient.getStartTimeAsync(EasyMock.anyString()))
        .andReturn(Futures.immediateFuture(DateTimes.nowUtc()))
        .anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            null
        )
    ).anyTimes();
    expect(taskClient.stopAsync("id3", false)).andReturn(Futures.immediateFuture(true));
    expect(taskClient.stopAsync("id4", false)).andReturn(Futures.immediateFuture(false));
    expect(taskClient.stopAsync("id5", false)).andReturn(Futures.immediateFuture((Boolean) null));

    TreeMap<Integer, Map<Integer, Long>> checkpoints1 = new TreeMap<>();
    checkpoints1.put(0, ImmutableMap.of(0, 0L, 2, 0L));
    TreeMap<Integer, Map<Integer, Long>> checkpoints2 = new TreeMap<>();
    checkpoints2.put(0, ImmutableMap.of(1, 0L));
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("id1"), EasyMock.anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints1))
        .times(1);
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("id2"), EasyMock.anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints2))
        .times(1);

    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));
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
    supervisor = getSupervisor(2, 2, true, "PT1H", null, null, false);
    addSomeEvents(1);

    Capture<Task> captured = Capture.newInstance(CaptureType.ALL);
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskRunner.getRunningTasks()).andReturn(Collections.EMPTY_LIST).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();
    expect(taskClient.getStatusAsync(EasyMock.anyString()))
        .andReturn(Futures.immediateFuture(Status.NOT_STARTED))
        .anyTimes();
    expect(taskClient.getStartTimeAsync(EasyMock.anyString()))
        .andReturn(Futures.immediateFuture(DateTimes.nowUtc()))
        .anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            null
        )
    ).anyTimes();
    expect(taskQueue.add(capture(captured))).andReturn(true).times(4);

    TreeMap<Integer, Map<Integer, Long>> checkpoints1 = new TreeMap<>();
    checkpoints1.put(0, ImmutableMap.of(0, 0L, 2, 0L));
    TreeMap<Integer, Map<Integer, Long>> checkpoints2 = new TreeMap<>();
    checkpoints2.put(0, ImmutableMap.of(1, 0L));
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("sequenceName-0"), EasyMock.anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints1))
        .anyTimes();
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("sequenceName-1"), EasyMock.anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints2))
        .anyTimes();

    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    List<Task> tasks = captured.getValues();

    // test that running the main loop again checks the status of the tasks that were created and does nothing if they
    // are all still running
    EasyMock.reset(taskStorage);
    expect(taskStorage.getActiveTasks()).andReturn(tasks).anyTimes();
    for (Task task : tasks) {
      expect(taskStorage.getStatus(task.getId()))
          .andReturn(Optional.of(TaskStatus.running(task.getId())))
          .anyTimes();
      expect(taskStorage.getTask(task.getId())).andReturn(Optional.of(task)).anyTimes();
    }
    replay(taskStorage);

    supervisor.runInternal();
    verifyAll();

    // test that a task failing causes a new task to be re-queued with the same parameters
    Capture<Task> aNewTaskCapture = Capture.newInstance();
    List<Task> imStillAlive = tasks.subList(0, 3);
    KafkaIndexTask iHaveFailed = (KafkaIndexTask) tasks.get(3);
    EasyMock.reset(taskStorage);
    EasyMock.reset(taskQueue);
    expect(taskStorage.getActiveTasks()).andReturn(imStillAlive).anyTimes();
    for (Task task : imStillAlive) {
      expect(taskStorage.getStatus(task.getId()))
          .andReturn(Optional.of(TaskStatus.running(task.getId())))
          .anyTimes();
      expect(taskStorage.getTask(task.getId())).andReturn(Optional.of(task)).anyTimes();
    }
    expect(taskStorage.getStatus(iHaveFailed.getId()))
        .andReturn(Optional.of(TaskStatus.failure(iHaveFailed.getId())));
    expect(taskStorage.getTask(iHaveFailed.getId())).andReturn(Optional.of((Task) iHaveFailed)).anyTimes();
    expect(taskQueue.add(capture(aNewTaskCapture))).andReturn(true);
    replay(taskStorage);
    replay(taskQueue);

    supervisor.runInternal();
    verifyAll();

    Assert.assertNotEquals(iHaveFailed.getId(), aNewTaskCapture.getValue().getId());
    Assert.assertEquals(
        iHaveFailed.getIOConfig().getBaseSequenceName(),
        ((KafkaIndexTask) aNewTaskCapture.getValue()).getIOConfig().getBaseSequenceName()
    );
  }

  @Test
  public void testRequeueAdoptedTaskWhenFailed() throws Exception
  {
    supervisor = getSupervisor(2, 1, true, "PT1H", null, null, false);
    addSomeEvents(1);

    DateTime now = DateTimes.nowUtc();
    DateTime maxi = now.plusMinutes(60);
    Task id1 = createKafkaIndexTask(
        "id1",
        DATASOURCE,
        0,
        new SeekableStreamPartitions<>("topic", ImmutableMap.of(0, 0L, 2, 0L)),
        new SeekableStreamPartitions<>("topic", ImmutableMap.of(0, Long.MAX_VALUE, 2, Long.MAX_VALUE)),
        now,
        maxi
    );

    List<Task> existingTasks = ImmutableList.of(id1);

    Capture<Task> captured = Capture.newInstance();
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskRunner.getRunningTasks()).andReturn(Collections.EMPTY_LIST).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(existingTasks).anyTimes();
    expect(taskStorage.getStatus("id1")).andReturn(Optional.of(TaskStatus.running("id1"))).anyTimes();
    expect(taskStorage.getTask("id1")).andReturn(Optional.of(id1)).anyTimes();
    expect(taskClient.getStatusAsync("id1")).andReturn(Futures.immediateFuture(Status.READING));
    expect(taskClient.getStartTimeAsync("id1")).andReturn(Futures.immediateFuture(now)).anyTimes();
    expect(taskQueue.add(capture(captured))).andReturn(true);
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            null
        )
    ).anyTimes();

    TreeMap<Integer, Map<Integer, Long>> checkpoints = new TreeMap<>();
    checkpoints.put(0, ImmutableMap.of(0, 0L, 2, 0L));
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("id1"), EasyMock.anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints))
        .times(2);

    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    // check that replica tasks are created with the same minimumMessageTime as tasks inherited from another supervisor
    Assert.assertEquals(now, ((KafkaIndexTask) captured.getValue()).getIOConfig().getMinimumMessageTime().get());

    // test that a task failing causes a new task to be re-queued with the same parameters
    String runningTaskId = captured.getValue().getId();
    Capture<Task> aNewTaskCapture = Capture.newInstance();
    KafkaIndexTask iHaveFailed = (KafkaIndexTask) existingTasks.get(0);
    EasyMock.reset(taskStorage);
    EasyMock.reset(taskQueue);
    EasyMock.reset(taskClient);

    // for the newly created replica task
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("sequenceName-0"), EasyMock.anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints))
        .times(2);
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("id1"), EasyMock.anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints))
        .times(1);

    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of(captured.getValue())).anyTimes();
    expect(taskStorage.getStatus(iHaveFailed.getId()))
        .andReturn(Optional.of(TaskStatus.failure(iHaveFailed.getId())));
    expect(taskStorage.getStatus(runningTaskId))
        .andReturn(Optional.of(TaskStatus.running(runningTaskId)))
        .anyTimes();
    expect(taskStorage.getTask(iHaveFailed.getId())).andReturn(Optional.of((Task) iHaveFailed)).anyTimes();
    expect(taskStorage.getTask(runningTaskId)).andReturn(Optional.of(captured.getValue())).anyTimes();
    expect(taskClient.getStatusAsync(runningTaskId)).andReturn(Futures.immediateFuture(Status.READING));
    expect(taskClient.getStartTimeAsync(runningTaskId)).andReturn(Futures.immediateFuture(now)).anyTimes();
    expect(taskQueue.add(capture(aNewTaskCapture))).andReturn(true);
    replay(taskStorage);
    replay(taskQueue);
    replay(taskClient);

    supervisor.runInternal();
    verifyAll();

    Assert.assertNotEquals(iHaveFailed.getId(), aNewTaskCapture.getValue().getId());
    Assert.assertEquals(
        iHaveFailed.getIOConfig().getBaseSequenceName(),
        ((KafkaIndexTask) aNewTaskCapture.getValue()).getIOConfig().getBaseSequenceName()
    );

    // check that failed tasks are recreated with the same minimumMessageTime as the task it replaced, even if that
    // task came from another supervisor
    Assert.assertEquals(now, ((KafkaIndexTask) aNewTaskCapture.getValue()).getIOConfig().getMinimumMessageTime().get());
    Assert.assertEquals(
        maxi,
        ((KafkaIndexTask) aNewTaskCapture.getValue()).getIOConfig().getMaximumMessageTime().get()
    );
  }

  @Test
  public void testQueueNextTasksOnSuccess() throws Exception
  {
    supervisor = getSupervisor(2, 2, true, "PT1H", null, null, false);
    addSomeEvents(1);

    Capture<Task> captured = Capture.newInstance(CaptureType.ALL);
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskRunner.getRunningTasks()).andReturn(Collections.EMPTY_LIST).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();
    expect(taskClient.getStatusAsync(EasyMock.anyString()))
        .andReturn(Futures.immediateFuture(Status.NOT_STARTED))
        .anyTimes();
    expect(taskClient.getStartTimeAsync(EasyMock.anyString()))
        .andReturn(Futures.immediateFuture(DateTimes.nowUtc()))
        .anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            null
        )
    ).anyTimes();
    expect(taskQueue.add(capture(captured))).andReturn(true).times(4);
    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));

    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    List<Task> tasks = captured.getValues();

    EasyMock.reset(taskStorage);
    EasyMock.reset(taskClient);

    expect(taskClient.getStatusAsync(EasyMock.anyString()))
        .andReturn(Futures.immediateFuture(Status.NOT_STARTED))
        .anyTimes();
    expect(taskClient.getStartTimeAsync(EasyMock.anyString()))
        .andReturn(Futures.immediateFuture(DateTimes.nowUtc()))
        .anyTimes();
    TreeMap<Integer, Map<Integer, Long>> checkpoints1 = new TreeMap<>();
    checkpoints1.put(0, ImmutableMap.of(0, 0L, 2, 0L));
    TreeMap<Integer, Map<Integer, Long>> checkpoints2 = new TreeMap<>();
    checkpoints2.put(0, ImmutableMap.of(1, 0L));
    // there would be 4 tasks, 2 for each task group
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("sequenceName-0"), EasyMock.anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints1))
        .times(2);
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("sequenceName-1"), EasyMock.anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints2))
        .times(2);

    expect(taskStorage.getActiveTasks()).andReturn(tasks).anyTimes();
    for (Task task : tasks) {
      expect(taskStorage.getStatus(task.getId()))
          .andReturn(Optional.of(TaskStatus.running(task.getId())))
          .anyTimes();
      expect(taskStorage.getTask(task.getId())).andReturn(Optional.of(task)).anyTimes();
    }
    replay(taskStorage);
    replay(taskClient);

    supervisor.runInternal();
    verifyAll();

    // test that a task succeeding causes a new task to be re-queued with the next offset range and causes any replica
    // tasks to be shutdown
    Capture<Task> newTasksCapture = Capture.newInstance(CaptureType.ALL);
    Capture<String> shutdownTaskIdCapture = Capture.newInstance();
    List<Task> imStillRunning = tasks.subList(1, 4);
    KafkaIndexTask iAmSuccess = (KafkaIndexTask) tasks.get(0);
    EasyMock.reset(taskStorage);
    EasyMock.reset(taskQueue);
    EasyMock.reset(taskClient);
    expect(taskStorage.getActiveTasks()).andReturn(imStillRunning).anyTimes();
    for (Task task : imStillRunning) {
      expect(taskStorage.getStatus(task.getId()))
          .andReturn(Optional.of(TaskStatus.running(task.getId())))
          .anyTimes();
      expect(taskStorage.getTask(task.getId())).andReturn(Optional.of(task)).anyTimes();
    }
    expect(taskStorage.getStatus(iAmSuccess.getId()))
        .andReturn(Optional.of(TaskStatus.success(iAmSuccess.getId())));
    expect(taskStorage.getTask(iAmSuccess.getId())).andReturn(Optional.of((Task) iAmSuccess)).anyTimes();
    expect(taskQueue.add(capture(newTasksCapture))).andReturn(true).times(2);
    expect(taskClient.stopAsync(capture(shutdownTaskIdCapture), EasyMock.eq(false)))
        .andReturn(Futures.immediateFuture(true));
    replay(taskStorage);
    replay(taskQueue);
    replay(taskClient);

    supervisor.runInternal();
    verifyAll();

    // make sure we killed the right task (sequenceName for replicas are the same)
    Assert.assertTrue(shutdownTaskIdCapture.getValue().contains(iAmSuccess.getIOConfig().getBaseSequenceName()));
  }

  @Test
  public void testBeginPublishAndQueueNextTasks() throws Exception
  {
    final TaskLocation location = new TaskLocation("testHost", 1234, -1);

    supervisor = getSupervisor(2, 2, true, "PT1M", null, null, false);
    addSomeEvents(100);

    Capture<Task> captured = Capture.newInstance(CaptureType.ALL);
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskRunner.getRunningTasks()).andReturn(Collections.EMPTY_LIST).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            null
        )
    ).anyTimes();
    expect(taskQueue.add(capture(captured))).andReturn(true).times(4);
    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));
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
    expect(taskStorage.getActiveTasks()).andReturn(tasks).anyTimes();
    for (Task task : tasks) {
      expect(taskStorage.getStatus(task.getId()))
          .andReturn(Optional.of(TaskStatus.running(task.getId())))
          .anyTimes();
      expect(taskStorage.getTask(task.getId())).andReturn(Optional.of(task)).anyTimes();
    }
    expect(taskRunner.getRunningTasks()).andReturn(workItems).anyTimes();
    expect(taskClient.getStatusAsync(EasyMock.anyString()))
        .andReturn(Futures.immediateFuture(Status.READING))
        .anyTimes();
    expect(taskClient.getStartTimeAsync(EasyMock.contains("sequenceName-0")))
        .andReturn(Futures.immediateFuture(DateTimes.nowUtc().minusMinutes(2)))
        .andReturn(Futures.immediateFuture(DateTimes.nowUtc()));
    expect(taskClient.getStartTimeAsync(EasyMock.contains("sequenceName-1")))
        .andReturn(Futures.immediateFuture(DateTimes.nowUtc()))
        .times(2);
    expect(taskClient.pauseAsync(EasyMock.contains("sequenceName-0")))
        .andReturn(Futures.immediateFuture((Map<Integer, Long>) ImmutableMap.of(0, 10L, 1, 20L, 2, 30L)))
        .andReturn(Futures.immediateFuture((Map<Integer, Long>) ImmutableMap.of(0, 10L, 1, 15L, 2, 35L)));
    expect(
        taskClient.setEndOffsetsAsync(
            EasyMock.contains("sequenceName-0"),
            EasyMock.eq(ImmutableMap.of(0, 10L, 1, 20L, 2, 35L)),
            EasyMock.eq(true)
        )
    ).andReturn(Futures.immediateFuture(true)).times(2);
    expect(taskQueue.add(capture(captured))).andReturn(true).times(2);

    TreeMap<Integer, Map<Integer, Long>> checkpoints1 = new TreeMap<>();
    checkpoints1.put(0, ImmutableMap.of(0, 0L, 2, 0L));
    TreeMap<Integer, Map<Integer, Long>> checkpoints2 = new TreeMap<>();
    checkpoints2.put(0, ImmutableMap.of(1, 0L));
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("sequenceName-0"), EasyMock.anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints1))
        .times(2);
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("sequenceName-1"), EasyMock.anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints2))
        .times(2);

    replay(taskStorage, taskRunner, taskClient, taskQueue);

    supervisor.runInternal();
    verifyAll();

    for (Task task : captured.getValues()) {
      KafkaIndexTask kafkaIndexTask = (KafkaIndexTask) task;
      Assert.assertEquals(dataSchema, kafkaIndexTask.getDataSchema());
      Assert.assertEquals(tuningConfig.convertToTaskTuningConfig(), kafkaIndexTask.getTuningConfig());

      KafkaIndexTaskIOConfig taskConfig = kafkaIndexTask.getIOConfig();
      Assert.assertEquals("sequenceName-0", taskConfig.getBaseSequenceName());
      Assert.assertTrue("isUseTransaction", taskConfig.isUseTransaction());

      Assert.assertEquals(topic, taskConfig.getStartPartitions().getStream());
      Assert.assertEquals(10L, (long) taskConfig.getStartPartitions().getPartitionSequenceNumberMap().get(0));
      Assert.assertEquals(20L, (long) taskConfig.getStartPartitions().getPartitionSequenceNumberMap().get(1));
      Assert.assertEquals(35L, (long) taskConfig.getStartPartitions().getPartitionSequenceNumberMap().get(2));
    }
  }

  @Test
  public void testDiscoverExistingPublishingTask() throws Exception
  {
    final TaskLocation location = new TaskLocation("testHost", 1234, -1);

    supervisor = getSupervisor(1, 1, true, "PT1H", null, null, false);
    addSomeEvents(1);

    Task task = createKafkaIndexTask(
        "id1",
        DATASOURCE,
        0,
        new SeekableStreamPartitions<>("topic", ImmutableMap.of(0, 0L, 1, 0L, 2, 0L)),
        new SeekableStreamPartitions<>(
            "topic",
            ImmutableMap.of(0, Long.MAX_VALUE, 1, Long.MAX_VALUE, 2, Long.MAX_VALUE)
        ),
        null,
        null
    );

    Collection workItems = new ArrayList<>();
    workItems.add(new TestTaskRunnerWorkItem(task, null, location));

    Capture<KafkaIndexTask> captured = Capture.newInstance();
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskRunner.getRunningTasks()).andReturn(workItems).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of(task)).anyTimes();
    expect(taskStorage.getStatus("id1")).andReturn(Optional.of(TaskStatus.running("id1"))).anyTimes();
    expect(taskStorage.getTask("id1")).andReturn(Optional.of(task)).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            null
        )
    ).anyTimes();
    expect(taskClient.getStatusAsync("id1")).andReturn(Futures.immediateFuture(Status.PUBLISHING));
    expect(taskClient.getCurrentOffsetsAsync("id1", false))
        .andReturn(Futures.immediateFuture((Map<Integer, Long>) ImmutableMap.of(0, 10L, 1, 20L, 2, 30L)));
    expect(taskClient.getEndOffsets("id1")).andReturn(ImmutableMap.of(0, 10L, 1, 20L, 2, 30L));
    expect(taskQueue.add(capture(captured))).andReturn(true);

    TreeMap<Integer, Map<Integer, Long>> checkpoints = new TreeMap<>();
    checkpoints.put(0, ImmutableMap.of(0, 0L, 1, 0L, 2, 0L));
    expect(taskClient.getCheckpoints(EasyMock.anyString(), EasyMock.anyBoolean()))
        .andReturn(checkpoints)
        .anyTimes();

    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    supervisor.updateCurrentAndLatestOffsets().run();
    SupervisorReport<KafkaSupervisorReportPayload> report = supervisor.getStatus();
    verifyAll();

    Assert.assertEquals(DATASOURCE, report.getId());

    KafkaSupervisorReportPayload payload = report.getPayload();

    Assert.assertEquals(DATASOURCE, payload.getDataSource());
    Assert.assertEquals(3600L, (long) payload.getDurationSeconds());
    Assert.assertEquals(NUM_PARTITIONS, (int) payload.getPartitions());
    Assert.assertEquals(1, (int) payload.getReplicas());
    Assert.assertEquals(topic, payload.getStream());
    Assert.assertEquals(0, payload.getActiveTasks().size());
    Assert.assertEquals(1, payload.getPublishingTasks().size());

    TaskReportData publishingReport = payload.getPublishingTasks().get(0);

    Assert.assertEquals("id1", publishingReport.getId());
    Assert.assertEquals(ImmutableMap.of(0, 0L, 1, 0L, 2, 0L), publishingReport.getStartingOffsets());
    Assert.assertEquals(ImmutableMap.of(0, 10L, 1, 20L, 2, 30L), publishingReport.getCurrentOffsets());

    KafkaIndexTask capturedTask = captured.getValue();
    Assert.assertEquals(dataSchema, capturedTask.getDataSchema());
    Assert.assertEquals(tuningConfig.convertToTaskTuningConfig(), capturedTask.getTuningConfig());

    KafkaIndexTaskIOConfig capturedTaskConfig = capturedTask.getIOConfig();
    Assert.assertEquals(kafkaHost, capturedTaskConfig.getConsumerProperties().get("bootstrap.servers"));
    Assert.assertEquals("myCustomValue", capturedTaskConfig.getConsumerProperties().get("myCustomKey"));
    Assert.assertEquals("sequenceName-0", capturedTaskConfig.getBaseSequenceName());
    Assert.assertTrue("isUseTransaction", capturedTaskConfig.isUseTransaction());

    // check that the new task was created with starting offsets matching where the publishing task finished
    Assert.assertEquals(topic, capturedTaskConfig.getStartPartitions().getStream());
    Assert.assertEquals(10L, (long) capturedTaskConfig.getStartPartitions().getPartitionSequenceNumberMap().get(0));
    Assert.assertEquals(20L, (long) capturedTaskConfig.getStartPartitions().getPartitionSequenceNumberMap().get(1));
    Assert.assertEquals(30L, (long) capturedTaskConfig.getStartPartitions().getPartitionSequenceNumberMap().get(2));

    Assert.assertEquals(topic, capturedTaskConfig.getEndPartitions().getStream());
    Assert.assertEquals(
        Long.MAX_VALUE,
        (long) capturedTaskConfig.getEndPartitions().getPartitionSequenceNumberMap().get(0)
    );
    Assert.assertEquals(
        Long.MAX_VALUE,
        (long) capturedTaskConfig.getEndPartitions().getPartitionSequenceNumberMap().get(1)
    );
    Assert.assertEquals(
        Long.MAX_VALUE,
        (long) capturedTaskConfig.getEndPartitions().getPartitionSequenceNumberMap().get(2)
    );
  }

  @Test
  public void testDiscoverExistingPublishingTaskWithDifferentPartitionAllocation() throws Exception
  {
    final TaskLocation location = new TaskLocation("testHost", 1234, -1);

    supervisor = getSupervisor(1, 1, true, "PT1H", null, null, false);
    addSomeEvents(1);

    Task task = createKafkaIndexTask(
        "id1",
        DATASOURCE,
        0,
        new SeekableStreamPartitions<>("topic", ImmutableMap.of(0, 0L, 2, 0L)),
        new SeekableStreamPartitions<>("topic", ImmutableMap.of(0, Long.MAX_VALUE, 2, Long.MAX_VALUE)),
        null,
        null
    );

    Collection workItems = new ArrayList<>();
    workItems.add(new TestTaskRunnerWorkItem(task, null, location));

    Capture<KafkaIndexTask> captured = Capture.newInstance();
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskRunner.getRunningTasks()).andReturn(workItems).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of(task)).anyTimes();
    expect(taskStorage.getStatus("id1")).andReturn(Optional.of(TaskStatus.running("id1"))).anyTimes();
    expect(taskStorage.getTask("id1")).andReturn(Optional.of(task)).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            null
        )
    ).anyTimes();
    expect(taskClient.getStatusAsync("id1")).andReturn(Futures.immediateFuture(Status.PUBLISHING));
    expect(taskClient.getCurrentOffsetsAsync("id1", false))
        .andReturn(Futures.immediateFuture((Map<Integer, Long>) ImmutableMap.of(0, 10L, 2, 30L)));
    expect(taskClient.getEndOffsets("id1")).andReturn(ImmutableMap.of(0, 10L, 2, 30L));
    expect(taskQueue.add(capture(captured))).andReturn(true);

    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    supervisor.updateCurrentAndLatestOffsets().run();
    SupervisorReport<KafkaSupervisorReportPayload> report = supervisor.getStatus();
    verifyAll();

    Assert.assertEquals(DATASOURCE, report.getId());

    KafkaSupervisorReportPayload payload = report.getPayload();

    Assert.assertEquals(DATASOURCE, payload.getDataSource());
    Assert.assertEquals(3600L, (long) payload.getDurationSeconds());
    Assert.assertEquals(NUM_PARTITIONS, (int) payload.getPartitions());
    Assert.assertEquals(1, (int) payload.getReplicas());
    Assert.assertEquals(topic, payload.getStream());
    Assert.assertEquals(0, payload.getActiveTasks().size());
    Assert.assertEquals(1, payload.getPublishingTasks().size());

    TaskReportData publishingReport = payload.getPublishingTasks().get(0);

    Assert.assertEquals("id1", publishingReport.getId());
    Assert.assertEquals(ImmutableMap.of(0, 0L, 2, 0L), publishingReport.getStartingOffsets());
    Assert.assertEquals(ImmutableMap.of(0, 10L, 2, 30L), publishingReport.getCurrentOffsets());

    KafkaIndexTask capturedTask = captured.getValue();
    Assert.assertEquals(dataSchema, capturedTask.getDataSchema());
    Assert.assertEquals(tuningConfig.convertToTaskTuningConfig(), capturedTask.getTuningConfig());

    KafkaIndexTaskIOConfig capturedTaskConfig = capturedTask.getIOConfig();
    Assert.assertEquals(kafkaHost, capturedTaskConfig.getConsumerProperties().get("bootstrap.servers"));
    Assert.assertEquals("myCustomValue", capturedTaskConfig.getConsumerProperties().get("myCustomKey"));
    Assert.assertEquals("sequenceName-0", capturedTaskConfig.getBaseSequenceName());
    Assert.assertTrue("isUseTransaction", capturedTaskConfig.isUseTransaction());

    // check that the new task was created with starting offsets matching where the publishing task finished
    Assert.assertEquals(topic, capturedTaskConfig.getStartPartitions().getStream());
    Assert.assertEquals(10L, (long) capturedTaskConfig.getStartPartitions().getPartitionSequenceNumberMap().get(0));
    Assert.assertEquals(0L, (long) capturedTaskConfig.getStartPartitions().getPartitionSequenceNumberMap().get(1));
    Assert.assertEquals(30L, (long) capturedTaskConfig.getStartPartitions().getPartitionSequenceNumberMap().get(2));

    Assert.assertEquals(topic, capturedTaskConfig.getEndPartitions().getStream());
    Assert.assertEquals(
        Long.MAX_VALUE,
        (long) capturedTaskConfig.getEndPartitions().getPartitionSequenceNumberMap().get(0)
    );
    Assert.assertEquals(
        Long.MAX_VALUE,
        (long) capturedTaskConfig.getEndPartitions().getPartitionSequenceNumberMap().get(1)
    );
    Assert.assertEquals(
        Long.MAX_VALUE,
        (long) capturedTaskConfig.getEndPartitions().getPartitionSequenceNumberMap().get(2)
    );
  }

  @Test
  public void testDiscoverExistingPublishingAndReadingTask() throws Exception
  {
    final TaskLocation location1 = new TaskLocation("testHost", 1234, -1);
    final TaskLocation location2 = new TaskLocation("testHost2", 145, -1);
    final DateTime startTime = DateTimes.nowUtc();

    supervisor = getSupervisor(1, 1, true, "PT1H", null, null, false);
    addSomeEvents(6);

    Task id1 = createKafkaIndexTask(
        "id1",
        DATASOURCE,
        0,
        new SeekableStreamPartitions<>("topic", ImmutableMap.of(0, 0L, 1, 0L, 2, 0L)),
        new SeekableStreamPartitions<>(
            "topic",
            ImmutableMap.of(0, Long.MAX_VALUE, 1, Long.MAX_VALUE, 2, Long.MAX_VALUE)
        ),
        null,
        null
    );

    Task id2 = createKafkaIndexTask(
        "id2",
        DATASOURCE,
        0,
        new SeekableStreamPartitions<>("topic", ImmutableMap.of(0, 1L, 1, 2L, 2, 3L)),
        new SeekableStreamPartitions<>(
            "topic",
            ImmutableMap.of(0, Long.MAX_VALUE, 1, Long.MAX_VALUE, 2, Long.MAX_VALUE)
        ),
        null,
        null
    );

    Collection workItems = new ArrayList<>();
    workItems.add(new TestTaskRunnerWorkItem(id1, null, location1));
    workItems.add(new TestTaskRunnerWorkItem(id2, null, location2));

    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskRunner.getRunningTasks()).andReturn(workItems).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of(id1, id2)).anyTimes();
    expect(taskStorage.getStatus("id1")).andReturn(Optional.of(TaskStatus.running("id1"))).anyTimes();
    expect(taskStorage.getStatus("id2")).andReturn(Optional.of(TaskStatus.running("id2"))).anyTimes();
    expect(taskStorage.getTask("id1")).andReturn(Optional.of(id1)).anyTimes();
    expect(taskStorage.getTask("id2")).andReturn(Optional.of(id2)).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            null
        )
    ).anyTimes();
    expect(taskClient.getStatusAsync("id1")).andReturn(Futures.immediateFuture(Status.PUBLISHING));
    expect(taskClient.getStatusAsync("id2")).andReturn(Futures.immediateFuture(Status.READING));
    expect(taskClient.getStartTimeAsync("id2")).andReturn(Futures.immediateFuture(startTime));
    expect(taskClient.getCurrentOffsetsAsync("id1", false))
        .andReturn(Futures.immediateFuture((Map<Integer, Long>) ImmutableMap.of(0, 1L, 1, 2L, 2, 3L)));
    expect(taskClient.getEndOffsets("id1")).andReturn(ImmutableMap.of(0, 1L, 1, 2L, 2, 3L));
    expect(taskClient.getCurrentOffsetsAsync("id2", false))
        .andReturn(Futures.immediateFuture((Map<Integer, Long>) ImmutableMap.of(0, 4L, 1, 5L, 2, 6L)));

    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));

    // since id1 is publishing, so getCheckpoints wouldn't be called for it
    TreeMap<Integer, Map<Integer, Long>> checkpoints = new TreeMap<>();
    checkpoints.put(0, ImmutableMap.of(0, 1L, 1, 2L, 2, 3L));
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("id2"), EasyMock.anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints))
        .times(1);

    replayAll();

    supervisor.start();
    supervisor.runInternal();
    supervisor.updateCurrentAndLatestOffsets().run();
    SupervisorReport<KafkaSupervisorReportPayload> report = supervisor.getStatus();
    verifyAll();

    Assert.assertEquals(DATASOURCE, report.getId());

    KafkaSupervisorReportPayload payload = report.getPayload();

    Assert.assertEquals(DATASOURCE, payload.getDataSource());
    Assert.assertEquals(3600L, (long) payload.getDurationSeconds());
    Assert.assertEquals(NUM_PARTITIONS, (int) payload.getPartitions());
    Assert.assertEquals(1, (int) payload.getReplicas());
    Assert.assertEquals(topic, payload.getStream());
    Assert.assertEquals(1, payload.getActiveTasks().size());
    Assert.assertEquals(1, payload.getPublishingTasks().size());

    TaskReportData activeReport = payload.getActiveTasks().get(0);
    TaskReportData publishingReport = payload.getPublishingTasks().get(0);

    Assert.assertEquals("id2", activeReport.getId());
    Assert.assertEquals(startTime, activeReport.getStartTime());
    Assert.assertEquals(ImmutableMap.of(0, 1L, 1, 2L, 2, 3L), activeReport.getStartingOffsets());
    Assert.assertEquals(ImmutableMap.of(0, 4L, 1, 5L, 2, 6L), activeReport.getCurrentOffsets());
    Assert.assertEquals(ImmutableMap.of(0, 2L, 1, 1L, 2, 0L), activeReport.getLag());

    Assert.assertEquals("id1", publishingReport.getId());
    Assert.assertEquals(ImmutableMap.of(0, 0L, 1, 0L, 2, 0L), publishingReport.getStartingOffsets());
    Assert.assertEquals(ImmutableMap.of(0, 1L, 1, 2L, 2, 3L), publishingReport.getCurrentOffsets());
    Assert.assertEquals(null, publishingReport.getLag());

    Assert.assertEquals(ImmutableMap.of(0, 6L, 1, 6L, 2, 6L), payload.getLatestOffsets());
    Assert.assertEquals(ImmutableMap.of(0, 2L, 1, 1L, 2, 0L), payload.getMinimumLag());
    Assert.assertEquals(3L, (long) payload.getAggregateLag());
    Assert.assertTrue(payload.getOffsetsLastUpdated().plusMinutes(1).isAfterNow());
  }

  @Test
  public void testKillUnresponsiveTasksWhileGettingStartTime() throws Exception
  {
    supervisor = getSupervisor(2, 2, true, "PT1H", null, null, false);
    addSomeEvents(1);

    Capture<Task> captured = Capture.newInstance(CaptureType.ALL);
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskRunner.getRunningTasks()).andReturn(Collections.EMPTY_LIST).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            null
        )
    ).anyTimes();
    expect(taskQueue.add(capture(captured))).andReturn(true).times(4);
    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    List<Task> tasks = captured.getValues();

    EasyMock.reset(taskStorage, taskClient, taskQueue);

    TreeMap<Integer, Map<Integer, Long>> checkpoints1 = new TreeMap<>();
    checkpoints1.put(0, ImmutableMap.of(0, 0L, 2, 0L));
    TreeMap<Integer, Map<Integer, Long>> checkpoints2 = new TreeMap<>();
    checkpoints2.put(0, ImmutableMap.of(1, 0L));
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("sequenceName-0"), EasyMock.anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints1))
        .times(2);
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("sequenceName-1"), EasyMock.anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints2))
        .times(2);

    expect(taskStorage.getActiveTasks()).andReturn(tasks).anyTimes();
    for (Task task : tasks) {
      expect(taskStorage.getStatus(task.getId()))
          .andReturn(Optional.of(TaskStatus.running(task.getId())))
          .anyTimes();
      expect(taskStorage.getTask(task.getId())).andReturn(Optional.of(task)).anyTimes();
      expect(taskClient.getStatusAsync(task.getId()))
          .andReturn(Futures.immediateFuture(Status.NOT_STARTED));
      expect(taskClient.getStartTimeAsync(task.getId()))
          .andReturn(Futures.immediateFailedFuture(new RuntimeException()));
      taskQueue.shutdown(task.getId(), "Task [%s] failed to return start time, killing task", task.getId());
    }
    replay(taskStorage, taskClient, taskQueue);

    supervisor.runInternal();
    verifyAll();
  }

  @Test
  public void testKillUnresponsiveTasksWhilePausing() throws Exception
  {
    final TaskLocation location = new TaskLocation("testHost", 1234, -1);

    supervisor = getSupervisor(2, 2, true, "PT1M", null, null, false);
    addSomeEvents(100);

    Capture<Task> captured = Capture.newInstance(CaptureType.ALL);
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskRunner.getRunningTasks()).andReturn(Collections.EMPTY_LIST).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            null
        )
    ).anyTimes();
    expect(taskQueue.add(capture(captured))).andReturn(true).times(4);
    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));
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

    TreeMap<Integer, Map<Integer, Long>> checkpoints1 = new TreeMap<>();
    checkpoints1.put(0, ImmutableMap.of(0, 0L, 2, 0L));
    TreeMap<Integer, Map<Integer, Long>> checkpoints2 = new TreeMap<>();
    checkpoints2.put(0, ImmutableMap.of(1, 0L));
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("sequenceName-0"), EasyMock.anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints1))
        .times(2);
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("sequenceName-1"), EasyMock.anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints2))
        .times(2);

    captured = Capture.newInstance(CaptureType.ALL);
    expect(taskStorage.getActiveTasks()).andReturn(tasks).anyTimes();
    for (Task task : tasks) {
      expect(taskStorage.getStatus(task.getId()))
          .andReturn(Optional.of(TaskStatus.running(task.getId())))
          .anyTimes();
      expect(taskStorage.getTask(task.getId())).andReturn(Optional.of(task)).anyTimes();
    }
    expect(taskRunner.getRunningTasks()).andReturn(workItems).anyTimes();
    expect(taskClient.getStatusAsync(EasyMock.anyString()))
        .andReturn(Futures.immediateFuture(Status.READING))
        .anyTimes();
    expect(taskClient.getStartTimeAsync(EasyMock.contains("sequenceName-0")))
        .andReturn(Futures.immediateFuture(DateTimes.nowUtc().minusMinutes(2)))
        .andReturn(Futures.immediateFuture(DateTimes.nowUtc()));
    expect(taskClient.getStartTimeAsync(EasyMock.contains("sequenceName-1")))
        .andReturn(Futures.immediateFuture(DateTimes.nowUtc()))
        .times(2);
    expect(taskClient.pauseAsync(EasyMock.contains("sequenceName-0")))
        .andReturn(Futures.immediateFailedFuture(new RuntimeException())).times(2);
    taskQueue.shutdown(
        EasyMock.contains("sequenceName-0"),
        EasyMock.eq("An exception occured while waiting for task [%s] to pause: [%s]"),
        EasyMock.contains("sequenceName-0"),
        EasyMock.anyString()
    );
    expectLastCall().times(2);
    expect(taskQueue.add(capture(captured))).andReturn(true).times(2);

    replay(taskStorage, taskRunner, taskClient, taskQueue);

    supervisor.runInternal();
    verifyAll();

    for (Task task : captured.getValues()) {
      KafkaIndexTaskIOConfig taskConfig = ((KafkaIndexTask) task).getIOConfig();
      Assert.assertEquals(0L, (long) taskConfig.getStartPartitions().getPartitionSequenceNumberMap().get(0));
      Assert.assertEquals(0L, (long) taskConfig.getStartPartitions().getPartitionSequenceNumberMap().get(2));
    }
  }

  @Test
  public void testKillUnresponsiveTasksWhileSettingEndOffsets() throws Exception
  {
    final TaskLocation location = new TaskLocation("testHost", 1234, -1);

    supervisor = getSupervisor(2, 2, true, "PT1M", null, null, false);
    addSomeEvents(100);

    Capture<Task> captured = Capture.newInstance(CaptureType.ALL);
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskRunner.getRunningTasks()).andReturn(Collections.EMPTY_LIST).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            null
        )
    ).anyTimes();
    expect(taskQueue.add(capture(captured))).andReturn(true).times(4);
    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));
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

    TreeMap<Integer, Map<Integer, Long>> checkpoints1 = new TreeMap<>();
    checkpoints1.put(0, ImmutableMap.of(0, 0L, 2, 0L));
    TreeMap<Integer, Map<Integer, Long>> checkpoints2 = new TreeMap<>();
    checkpoints2.put(0, ImmutableMap.of(1, 0L));
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("sequenceName-0"), EasyMock.anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints1))
        .times(2);
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("sequenceName-1"), EasyMock.anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints2))
        .times(2);

    captured = Capture.newInstance(CaptureType.ALL);
    expect(taskStorage.getActiveTasks()).andReturn(tasks).anyTimes();
    for (Task task : tasks) {
      expect(taskStorage.getStatus(task.getId()))
          .andReturn(Optional.of(TaskStatus.running(task.getId())))
          .anyTimes();
      expect(taskStorage.getTask(task.getId())).andReturn(Optional.of(task)).anyTimes();
    }
    expect(taskRunner.getRunningTasks()).andReturn(workItems).anyTimes();
    expect(taskClient.getStatusAsync(EasyMock.anyString()))
        .andReturn(Futures.immediateFuture(Status.READING))
        .anyTimes();
    expect(taskClient.getStartTimeAsync(EasyMock.contains("sequenceName-0")))
        .andReturn(Futures.immediateFuture(DateTimes.nowUtc().minusMinutes(2)))
        .andReturn(Futures.immediateFuture(DateTimes.nowUtc()));
    expect(taskClient.getStartTimeAsync(EasyMock.contains("sequenceName-1")))
        .andReturn(Futures.immediateFuture(DateTimes.nowUtc()))
        .times(2);
    expect(taskClient.pauseAsync(EasyMock.contains("sequenceName-0")))
        .andReturn(Futures.immediateFuture((Map<Integer, Long>) ImmutableMap.of(0, 10L, 1, 20L, 2, 30L)))
        .andReturn(Futures.immediateFuture((Map<Integer, Long>) ImmutableMap.of(0, 10L, 1, 15L, 2, 35L)));
    expect(
        taskClient.setEndOffsetsAsync(
            EasyMock.contains("sequenceName-0"),
            EasyMock.eq(ImmutableMap.of(0, 10L, 1, 20L, 2, 35L)),
            EasyMock.eq(true)
        )
    ).andReturn(Futures.immediateFailedFuture(new RuntimeException())).times(2);
    taskQueue.shutdown(
        EasyMock.contains("sequenceName-0"),
        EasyMock.eq("All tasks in group [%s] failed to transition to publishing state"),
        EasyMock.eq(0)
    );
    expectLastCall().times(2);
    expect(taskQueue.add(capture(captured))).andReturn(true).times(2);

    replay(taskStorage, taskRunner, taskClient, taskQueue);

    supervisor.runInternal();
    verifyAll();

    for (Task task : captured.getValues()) {
      KafkaIndexTaskIOConfig taskConfig = ((KafkaIndexTask) task).getIOConfig();
      Assert.assertEquals(0L, (long) taskConfig.getStartPartitions().getPartitionSequenceNumberMap().get(0));
      Assert.assertEquals(0L, (long) taskConfig.getStartPartitions().getPartitionSequenceNumberMap().get(2));
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testStopNotStarted()
  {
    supervisor = getSupervisor(1, 1, true, "PT1H", null, null, false);
    supervisor.stop(false);
  }

  @Test
  public void testStop()
  {
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    taskClient.close();
    taskRunner.unregisterListener(StringUtils.format("KafkaSupervisor-%s", DATASOURCE));
    replayAll();

    supervisor = getSupervisor(1, 1, true, "PT1H", null, null, false);
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

    supervisor = getSupervisor(2, 1, true, "PT1H", null, null, false);
    addSomeEvents(1);

    Task id1 = createKafkaIndexTask(
        "id1",
        DATASOURCE,
        0,
        new SeekableStreamPartitions<>("topic", ImmutableMap.of(0, 0L, 1, 0L, 2, 0L)),
        new SeekableStreamPartitions<>(
            "topic",
            ImmutableMap.of(0, Long.MAX_VALUE, 1, Long.MAX_VALUE, 2, Long.MAX_VALUE)
        ),
        null,
        null
    );

    Task id2 = createKafkaIndexTask(
        "id2",
        DATASOURCE,
        0,
        new SeekableStreamPartitions<>("topic", ImmutableMap.of(0, 10L, 1, 20L, 2, 30L)),
        new SeekableStreamPartitions<>(
            "topic",
            ImmutableMap.of(0, Long.MAX_VALUE, 1, Long.MAX_VALUE, 2, Long.MAX_VALUE)
        ),
        null,
        null
    );

    Task id3 = createKafkaIndexTask(
        "id3",
        DATASOURCE,
        0,
        new SeekableStreamPartitions<>("topic", ImmutableMap.of(0, 10L, 1, 20L, 2, 30L)),
        new SeekableStreamPartitions<>(
            "topic",
            ImmutableMap.of(0, Long.MAX_VALUE, 1, Long.MAX_VALUE, 2, Long.MAX_VALUE)
        ),
        null,
        null
    );

    Collection workItems = new ArrayList<>();
    workItems.add(new TestTaskRunnerWorkItem(id1, null, location1));
    workItems.add(new TestTaskRunnerWorkItem(id2, null, location2));

    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskRunner.getRunningTasks()).andReturn(workItems).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of(id1, id2, id3)).anyTimes();
    expect(taskStorage.getStatus("id1")).andReturn(Optional.of(TaskStatus.running("id1"))).anyTimes();
    expect(taskStorage.getStatus("id2")).andReturn(Optional.of(TaskStatus.running("id2"))).anyTimes();
    expect(taskStorage.getStatus("id3")).andReturn(Optional.of(TaskStatus.running("id3"))).anyTimes();
    expect(taskStorage.getTask("id1")).andReturn(Optional.of(id1)).anyTimes();
    expect(taskStorage.getTask("id2")).andReturn(Optional.of(id2)).anyTimes();
    expect(taskStorage.getTask("id3")).andReturn(Optional.of(id3)).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            null
        )
    ).anyTimes();
    expect(taskClient.getStatusAsync("id1")).andReturn(Futures.immediateFuture(Status.PUBLISHING));
    expect(taskClient.getStatusAsync("id2")).andReturn(Futures.immediateFuture(Status.READING));
    expect(taskClient.getStatusAsync("id3")).andReturn(Futures.immediateFuture(Status.READING));
    expect(taskClient.getStartTimeAsync("id2")).andReturn(Futures.immediateFuture(startTime));
    expect(taskClient.getStartTimeAsync("id3")).andReturn(Futures.immediateFuture(startTime));
    expect(taskClient.getEndOffsets("id1")).andReturn(ImmutableMap.of(0, 10L, 1, 20L, 2, 30L));

    // getCheckpoints will not be called for id1 as it is in publishing state
    TreeMap<Integer, Map<Integer, Long>> checkpoints = new TreeMap<>();
    checkpoints.put(0, ImmutableMap.of(0, 10L, 1, 20L, 2, 30L));
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("id2"), EasyMock.anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints))
        .times(1);
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("id3"), EasyMock.anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints))
        .times(1);

    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    EasyMock.reset(taskRunner, taskClient, taskQueue);
    expect(taskRunner.getRunningTasks()).andReturn(workItems).anyTimes();
    expect(taskClient.pauseAsync("id2"))
        .andReturn(Futures.immediateFuture((Map<Integer, Long>) ImmutableMap.of(0, 15L, 1, 25L, 2, 30L)));
    expect(taskClient.setEndOffsetsAsync("id2", ImmutableMap.of(0, 15L, 1, 25L, 2, 30L), true))
        .andReturn(Futures.immediateFuture(true));
    taskQueue.shutdown("id3", "Killing task for graceful shutdown");
    expectLastCall().times(1);
    taskQueue.shutdown("id3", "Killing task [%s] which hasn't been assigned to a worker", "id3");
    expectLastCall().times(1);

    replay(taskRunner, taskClient, taskQueue);

    supervisor.gracefulShutdownInternal();
    verifyAll();
  }

  @Test
  public void testResetNoTasks() throws Exception
  {
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskRunner.getRunningTasks()).andReturn(Collections.EMPTY_LIST).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();
    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));
    replayAll();

    supervisor = getSupervisor(1, 1, true, "PT1H", null, null, false);
    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    EasyMock.reset(indexerMetadataStorageCoordinator);
    expect(indexerMetadataStorageCoordinator.deleteDataSourceMetadata(DATASOURCE)).andReturn(true);
    replay(indexerMetadataStorageCoordinator);

    supervisor.resetInternal(null);
    verifyAll();

  }

  @Test
  public void testResetDataSourceMetadata() throws Exception
  {
    supervisor = getSupervisor(1, 1, true, "PT1H", null, null, false);
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskRunner.getRunningTasks()).andReturn(Collections.EMPTY_LIST).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();
    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    Capture<String> captureDataSource = EasyMock.newCapture();
    Capture<DataSourceMetadata> captureDataSourceMetadata = EasyMock.newCapture();

    KafkaDataSourceMetadata kafkaDataSourceMetadata = new KafkaDataSourceMetadata(new SeekableStreamPartitions<>(
        topic,
        ImmutableMap.of(0, 1000L, 1, 1000L, 2, 1000L)
    ));

    KafkaDataSourceMetadata resetMetadata = new KafkaDataSourceMetadata(new SeekableStreamPartitions<>(
        topic,
        ImmutableMap.of(1, 1000L, 2, 1000L)
    ));

    KafkaDataSourceMetadata expectedMetadata = new KafkaDataSourceMetadata(new SeekableStreamPartitions<>(
        topic,
        ImmutableMap.of(0, 1000L)
    ));

    EasyMock.reset(indexerMetadataStorageCoordinator);
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE))
        .andReturn(kafkaDataSourceMetadata);
    expect(indexerMetadataStorageCoordinator.resetDataSourceMetadata(
        capture(captureDataSource),
        capture(captureDataSourceMetadata)
    )).andReturn(true);
    replay(indexerMetadataStorageCoordinator);

    try {
      supervisor.resetInternal(resetMetadata);
    }
    catch (NullPointerException npe) {
      // Expected as there will be an attempt to EasyMock.reset partitionGroups offsets to NOT_SET
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
    supervisor = getSupervisor(1, 1, true, "PT1H", null, null, false);
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskRunner.getRunningTasks()).andReturn(Collections.EMPTY_LIST).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();
    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    KafkaDataSourceMetadata resetMetadata = new KafkaDataSourceMetadata(new SeekableStreamPartitions<>(
        topic,
        ImmutableMap.of(1, 1000L, 2, 1000L)
    ));

    EasyMock.reset(indexerMetadataStorageCoordinator);
    // no DataSourceMetadata in metadata store
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(null);
    replay(indexerMetadataStorageCoordinator);

    supervisor.resetInternal(resetMetadata);
    verifyAll();
  }

  @Test
  public void testResetRunningTasks() throws Exception
  {
    final TaskLocation location1 = new TaskLocation("testHost", 1234, -1);
    final TaskLocation location2 = new TaskLocation("testHost2", 145, -1);
    final DateTime startTime = DateTimes.nowUtc();

    supervisor = getSupervisor(2, 1, true, "PT1H", null, null, false);
    addSomeEvents(1);

    Task id1 = createKafkaIndexTask(
        "id1",
        DATASOURCE,
        0,
        new SeekableStreamPartitions<>("topic", ImmutableMap.of(0, 0L, 1, 0L, 2, 0L)),
        new SeekableStreamPartitions<>(
            "topic",
            ImmutableMap.of(0, Long.MAX_VALUE, 1, Long.MAX_VALUE, 2, Long.MAX_VALUE)
        ),
        null,
        null
    );

    Task id2 = createKafkaIndexTask(
        "id2",
        DATASOURCE,
        0,
        new SeekableStreamPartitions<>("topic", ImmutableMap.of(0, 10L, 1, 20L, 2, 30L)),
        new SeekableStreamPartitions<>(
            "topic",
            ImmutableMap.of(0, Long.MAX_VALUE, 1, Long.MAX_VALUE, 2, Long.MAX_VALUE)
        ),
        null,
        null
    );

    Task id3 = createKafkaIndexTask(
        "id3",
        DATASOURCE,
        0,
        new SeekableStreamPartitions<>("topic", ImmutableMap.of(0, 10L, 1, 20L, 2, 30L)),
        new SeekableStreamPartitions<>(
            "topic",
            ImmutableMap.of(0, Long.MAX_VALUE, 1, Long.MAX_VALUE, 2, Long.MAX_VALUE)
        ),
        null,
        null
    );

    Collection workItems = new ArrayList<>();
    workItems.add(new TestTaskRunnerWorkItem(id1, null, location1));
    workItems.add(new TestTaskRunnerWorkItem(id2, null, location2));

    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskRunner.getRunningTasks()).andReturn(workItems).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of(id1, id2, id3)).anyTimes();
    expect(taskStorage.getStatus("id1")).andReturn(Optional.of(TaskStatus.running("id1"))).anyTimes();
    expect(taskStorage.getStatus("id2")).andReturn(Optional.of(TaskStatus.running("id2"))).anyTimes();
    expect(taskStorage.getStatus("id3")).andReturn(Optional.of(TaskStatus.running("id3"))).anyTimes();
    expect(taskStorage.getTask("id1")).andReturn(Optional.of(id1)).anyTimes();
    expect(taskStorage.getTask("id2")).andReturn(Optional.of(id2)).anyTimes();
    expect(taskStorage.getTask("id3")).andReturn(Optional.of(id3)).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            null
        )
    ).anyTimes();
    expect(taskClient.getStatusAsync("id1")).andReturn(Futures.immediateFuture(Status.PUBLISHING));
    expect(taskClient.getStatusAsync("id2")).andReturn(Futures.immediateFuture(Status.READING));
    expect(taskClient.getStatusAsync("id3")).andReturn(Futures.immediateFuture(Status.READING));
    expect(taskClient.getStartTimeAsync("id2")).andReturn(Futures.immediateFuture(startTime));
    expect(taskClient.getStartTimeAsync("id3")).andReturn(Futures.immediateFuture(startTime));
    expect(taskClient.getEndOffsets("id1")).andReturn(ImmutableMap.of(0, 10L, 1, 20L, 2, 30L));

    TreeMap<Integer, Map<Integer, Long>> checkpoints = new TreeMap<>();
    checkpoints.put(0, ImmutableMap.of(0, 10L, 1, 20L, 2, 30L));
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("id2"), EasyMock.anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints))
        .times(1);
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("id3"), EasyMock.anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints))
        .times(1);

    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    EasyMock.reset(taskQueue, indexerMetadataStorageCoordinator);
    expect(indexerMetadataStorageCoordinator.deleteDataSourceMetadata(DATASOURCE)).andReturn(true);
    taskQueue.shutdown("id2", "DataSourceMetadata is not found while reset");
    taskQueue.shutdown("id3", "DataSourceMetadata is not found while reset");
    replay(taskQueue, indexerMetadataStorageCoordinator);

    supervisor.resetInternal(null);
    verifyAll();
  }

  @Test
  public void testNoDataIngestionTasks() throws Exception
  {
    final DateTime startTime = DateTimes.nowUtc();
    supervisor = getSupervisor(2, 1, true, "PT1S", null, null, false);
    //not adding any events
    Task id1 = createKafkaIndexTask(
        "id1",
        DATASOURCE,
        0,
        new SeekableStreamPartitions<>("topic", ImmutableMap.of(0, 0L, 1, 0L, 2, 0L)),
        new SeekableStreamPartitions<>(
            "topic",
            ImmutableMap.of(0, Long.MAX_VALUE, 1, Long.MAX_VALUE, 2, Long.MAX_VALUE)
        ),
        null,
        null
    );

    Task id2 = createKafkaIndexTask(
        "id2",
        DATASOURCE,
        0,
        new SeekableStreamPartitions<>("topic", ImmutableMap.of(0, 10L, 1, 20L, 2, 30L)),
        new SeekableStreamPartitions<>(
            "topic",
            ImmutableMap.of(0, Long.MAX_VALUE, 1, Long.MAX_VALUE, 2, Long.MAX_VALUE)
        ),
        null,
        null
    );

    Task id3 = createKafkaIndexTask(
        "id3",
        DATASOURCE,
        0,
        new SeekableStreamPartitions<>("topic", ImmutableMap.of(0, 10L, 1, 20L, 2, 30L)),
        new SeekableStreamPartitions<>(
            "topic",
            ImmutableMap.of(0, Long.MAX_VALUE, 1, Long.MAX_VALUE, 2, Long.MAX_VALUE)
        ),
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
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            null
        )
    ).anyTimes();
    expect(taskClient.getStatusAsync("id1")).andReturn(Futures.immediateFuture(Status.READING));
    expect(taskClient.getStatusAsync("id2")).andReturn(Futures.immediateFuture(Status.READING));
    expect(taskClient.getStatusAsync("id3")).andReturn(Futures.immediateFuture(Status.READING));
    expect(taskClient.getStartTimeAsync("id1")).andReturn(Futures.immediateFuture(startTime));
    expect(taskClient.getStartTimeAsync("id2")).andReturn(Futures.immediateFuture(startTime));
    expect(taskClient.getStartTimeAsync("id3")).andReturn(Futures.immediateFuture(startTime));

    TreeMap<Integer, Map<Integer, Long>> checkpoints = new TreeMap<>();
    checkpoints.put(0, ImmutableMap.of(0, 10L, 1, 20L, 2, 30L));
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("id1"), EasyMock.anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints))
        .times(1);
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("id2"), EasyMock.anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints))
        .times(1);
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("id3"), EasyMock.anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints))
        .times(1);

    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    EasyMock.reset(taskQueue, indexerMetadataStorageCoordinator);
    expect(indexerMetadataStorageCoordinator.deleteDataSourceMetadata(DATASOURCE)).andReturn(true);
    taskQueue.shutdown("id1", "DataSourceMetadata is not found while reset");
    taskQueue.shutdown("id2", "DataSourceMetadata is not found while reset");
    taskQueue.shutdown("id3", "DataSourceMetadata is not found while reset");
    replay(taskQueue, indexerMetadataStorageCoordinator);

    supervisor.resetInternal(null);
    verifyAll();
  }

  @Test(timeout = 60_000L)
  public void testCheckpointForInactiveTaskGroup()
      throws InterruptedException, ExecutionException, TimeoutException, JsonProcessingException
  {
    supervisor = getSupervisor(2, 1, true, "PT1S", null, null, false);
    //not adding any events
    final Task id1 = createKafkaIndexTask(
        "id1",
        DATASOURCE,
        0,
        new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 0L, 1, 0L, 2, 0L)),
        new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, Long.MAX_VALUE, 1, Long.MAX_VALUE, 2, Long.MAX_VALUE)),
        null,
        null
    );

    final Task id2 = createKafkaIndexTask(
        "id2",
        DATASOURCE,
        0,
        new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 10L, 1, 20L, 2, 30L)),
        new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, Long.MAX_VALUE, 1, Long.MAX_VALUE, 2, Long.MAX_VALUE)),
        null,
        null
    );

    final Task id3 = createKafkaIndexTask(
        "id3",
        DATASOURCE,
        0,
        new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 10L, 1, 20L, 2, 30L)),
        new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, Long.MAX_VALUE, 1, Long.MAX_VALUE, 2, Long.MAX_VALUE)),
        null,
        null
    );

    final TaskLocation location1 = new TaskLocation("testHost", 1234, -1);
    final TaskLocation location2 = new TaskLocation("testHost2", 145, -1);
    Collection workItems = new ArrayList<>();
    workItems.add(new TestTaskRunnerWorkItem(id1, null, location1));
    workItems.add(new TestTaskRunnerWorkItem(id2, null, location2));
    workItems.add(new TestTaskRunnerWorkItem(id2, null, location2));

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
        indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(new KafkaDataSourceMetadata(null)
    ).anyTimes();
    expect(taskClient.getStatusAsync("id1")).andReturn(Futures.immediateFuture(Status.READING));
    expect(taskClient.getStatusAsync("id2")).andReturn(Futures.immediateFuture(Status.READING));
    expect(taskClient.getStatusAsync("id3")).andReturn(Futures.immediateFuture(Status.READING));

    final DateTime startTime = DateTimes.nowUtc();
    expect(taskClient.getStartTimeAsync("id1")).andReturn(Futures.immediateFuture(startTime));
    expect(taskClient.getStartTimeAsync("id2")).andReturn(Futures.immediateFuture(startTime));
    expect(taskClient.getStartTimeAsync("id3")).andReturn(Futures.immediateFuture(startTime));

    final TreeMap<Integer, Map<Integer, Long>> checkpoints = new TreeMap<>();
    checkpoints.put(0, ImmutableMap.of(0, 10L, 1, 20L, 2, 30L));
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("id1"), EasyMock.anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints))
        .times(1);
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("id2"), EasyMock.anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints))
        .times(1);
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("id3"), EasyMock.anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints))
        .times(1);

    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();

    final Map<Integer, Long> fakeCheckpoints = Collections.emptyMap();
    supervisor.moveTaskGroupToPendingCompletion(0);
    supervisor.checkpoint(
        0,
        ((KafkaIndexTask) id1).getIOConfig().getBaseSequenceName(),
        new KafkaDataSourceMetadata(new SeekableStreamPartitions<>(topic, checkpoints.get(0))),
        new KafkaDataSourceMetadata(new SeekableStreamPartitions<>(topic, fakeCheckpoints))
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
    //not adding any events
    final Task id1 = createKafkaIndexTask(
        "id1",
        DATASOURCE,
        0,
        new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 0L, 1, 0L, 2, 0L)),
        new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, Long.MAX_VALUE, 1, Long.MAX_VALUE, 2, Long.MAX_VALUE)),
        null,
        null
    );

    final Task id2 = createKafkaIndexTask(
        "id2",
        DATASOURCE,
        0,
        new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 10L, 1, 20L, 2, 30L)),
        new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, Long.MAX_VALUE, 1, Long.MAX_VALUE, 2, Long.MAX_VALUE)),
        null,
        null
    );

    final Task id3 = createKafkaIndexTask(
        "id3",
        DATASOURCE,
        0,
        new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 10L, 1, 20L, 2, 30L)),
        new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, Long.MAX_VALUE, 1, Long.MAX_VALUE, 2, Long.MAX_VALUE)),
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
        indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(new KafkaDataSourceMetadata(null)
    ).anyTimes();

    replayAll();

    supervisor.start();

    supervisor.checkpoint(
        0,
        ((KafkaIndexTask) id1).getIOConfig().getBaseSequenceName(),
        new KafkaDataSourceMetadata(new SeekableStreamPartitions<>(topic, Collections.emptyMap())),
        new KafkaDataSourceMetadata(new SeekableStreamPartitions<>(topic, Collections.emptyMap()))
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
    final Task id1 = createKafkaIndexTask(
        "id1",
        DATASOURCE,
        0,
        new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 0L)),
        new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, Long.MAX_VALUE)),
        null,
        null
    );

    final Task id2 = createKafkaIndexTask(
        "id2",
        DATASOURCE,
        0,
        new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 0L)),
        new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, Long.MAX_VALUE)),
        null,
        null
    );

    final Task id3 = createKafkaIndexTask(
        "id3",
        DATASOURCE,
        0,
        new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 0L)),
        new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, Long.MAX_VALUE)),
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
        indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(new KafkaDataSourceMetadata(null)
    ).anyTimes();
    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));
    expect(taskClient.getStatusAsync(EasyMock.anyString()))
        .andReturn(Futures.immediateFuture(Status.READING))
        .anyTimes();
    final TreeMap<Integer, Map<Integer, Long>> checkpoints = new TreeMap<>();
    checkpoints.put(0, ImmutableMap.of(0, 0L));
    expect(taskClient.getCheckpointsAsync(EasyMock.anyString(), EasyMock.anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints))
        .times(3);
    expect(taskClient.getStartTimeAsync(EasyMock.anyString()))
        .andReturn(Futures.immediateFuture(DateTimes.nowUtc()))
        .anyTimes();
    expect(taskClient.pauseAsync(EasyMock.anyString()))
        .andReturn(Futures.immediateFuture(ImmutableMap.of(0, 10L)))
        .anyTimes();
    expect(taskClient.setEndOffsetsAsync(
        EasyMock.anyString(),
        EasyMock.eq(ImmutableMap.of(0, 10L)),
        EasyMock.anyBoolean()
    ))
        .andReturn(Futures.immediateFuture(true))
        .anyTimes();

    replayAll();

    supervisor.start();

    supervisor.runInternal();

    final TreeMap<Integer, Map<Integer, Long>> newCheckpoints = new TreeMap<>();
    newCheckpoints.put(0, ImmutableMap.of(0, 10L));
    supervisor.checkpoint(
        null,
        ((KafkaIndexTask) id1).getIOConfig().getBaseSequenceName(),
        new KafkaDataSourceMetadata(new SeekableStreamPartitions<>(topic, checkpoints.get(0))),
        new KafkaDataSourceMetadata(new SeekableStreamPartitions<>(topic, newCheckpoints.get(0)))
    );

    while (supervisor.getNoticesQueueSize() > 0) {
      Thread.sleep(100);
    }

    verifyAll();
  }

  @Test
  public void testSuspendedNoRunningTasks() throws Exception
  {
    supervisor = getSupervisor(1, 1, true, "PT1H", null, null, false, true, kafkaHost);
    addSomeEvents(1);

    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            null
        )
    ).anyTimes();
    // this asserts that taskQueue.add does not in fact get called because supervisor should be suspended
    expect(taskQueue.add(anyObject())).andAnswer((IAnswer) () -> {
      Assert.fail();
      return null;
    }).anyTimes();
    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));
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

    supervisor = getSupervisor(2, 1, true, "PT1H", null, null, false, true, kafkaHost);
    addSomeEvents(1);

    Task id1 = createKafkaIndexTask(
        "id1",
        DATASOURCE,
        0,
        new SeekableStreamPartitions<>("topic", ImmutableMap.of(0, 0L, 1, 0L, 2, 0L)),
        new SeekableStreamPartitions<>(
            "topic",
            ImmutableMap.of(0, Long.MAX_VALUE, 1, Long.MAX_VALUE, 2, Long.MAX_VALUE)
        ),
        null,
        null
    );

    Task id2 = createKafkaIndexTask(
        "id2",
        DATASOURCE,
        0,
        new SeekableStreamPartitions<>("topic", ImmutableMap.of(0, 10L, 1, 20L, 2, 30L)),
        new SeekableStreamPartitions<>(
            "topic",
            ImmutableMap.of(0, Long.MAX_VALUE, 1, Long.MAX_VALUE, 2, Long.MAX_VALUE)
        ),
        null,
        null
    );

    Task id3 = createKafkaIndexTask(
        "id3",
        DATASOURCE,
        0,
        new SeekableStreamPartitions<>("topic", ImmutableMap.of(0, 10L, 1, 20L, 2, 30L)),
        new SeekableStreamPartitions<>(
            "topic",
            ImmutableMap.of(0, Long.MAX_VALUE, 1, Long.MAX_VALUE, 2, Long.MAX_VALUE)
        ),
        null,
        null
    );

    Collection workItems = new ArrayList<>();
    workItems.add(new TestTaskRunnerWorkItem(id1, null, location1));
    workItems.add(new TestTaskRunnerWorkItem(id2, null, location2));

    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskRunner.getRunningTasks()).andReturn(workItems).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of(id1, id2, id3)).anyTimes();
    expect(taskStorage.getStatus("id1")).andReturn(Optional.of(TaskStatus.running("id1"))).anyTimes();
    expect(taskStorage.getStatus("id2")).andReturn(Optional.of(TaskStatus.running("id2"))).anyTimes();
    expect(taskStorage.getStatus("id3")).andReturn(Optional.of(TaskStatus.running("id3"))).anyTimes();
    expect(taskStorage.getTask("id1")).andReturn(Optional.of(id1)).anyTimes();
    expect(taskStorage.getTask("id2")).andReturn(Optional.of(id2)).anyTimes();
    expect(taskStorage.getTask("id3")).andReturn(Optional.of(id3)).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            null
        )
    ).anyTimes();
    expect(taskClient.getStatusAsync("id1"))
        .andReturn(Futures.immediateFuture(Status.PUBLISHING));
    expect(taskClient.getStatusAsync("id2"))
        .andReturn(Futures.immediateFuture(Status.READING));
    expect(taskClient.getStatusAsync("id3"))
        .andReturn(Futures.immediateFuture(Status.READING));
    expect(taskClient.getStartTimeAsync("id2")).andReturn(Futures.immediateFuture(startTime));
    expect(taskClient.getStartTimeAsync("id3")).andReturn(Futures.immediateFuture(startTime));
    expect(taskClient.getEndOffsets("id1")).andReturn(ImmutableMap.of(0, 10L, 1, 20L, 2, 30L));

    // getCheckpoints will not be called for id1 as it is in publishing state
    TreeMap<Integer, Map<Integer, Long>> checkpoints = new TreeMap<>();
    checkpoints.put(0, ImmutableMap.of(0, 10L, 1, 20L, 2, 30L));
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("id2"), EasyMock.anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints))
        .times(1);
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("id3"), EasyMock.anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints))
        .times(1);

    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));

    expect(taskClient.pauseAsync("id2"))
        .andReturn(Futures.immediateFuture(ImmutableMap.of(0, 15L, 1, 25L, 2, 30L)));
    expect(taskClient.setEndOffsetsAsync("id2", ImmutableMap.of(0, 15L, 1, 25L, 2, 30L), true))
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
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskRunner.getRunningTasks()).andReturn(Collections.EMPTY_LIST).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();
    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));
    replayAll();

    supervisor = getSupervisor(1, 1, true, "PT1H", null, null, false, true, kafkaHost);
    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    EasyMock.reset(indexerMetadataStorageCoordinator);
    expect(indexerMetadataStorageCoordinator.deleteDataSourceMetadata(DATASOURCE)).andReturn(true);
    replay(indexerMetadataStorageCoordinator);

    supervisor.resetInternal(null);
    verifyAll();
  }

  @Test
  public void testFailedInitializationAndRecovery() throws Exception
  {
    // Block the supervisor initialization with a bad hostname config, make sure this doesn't block the lifecycle
    supervisor = getSupervisor(
        1,
        1,
        true,
        "PT1H",
        null,
        null,
        false,
        false,
        StringUtils.format("badhostname:%d", kafkaServer.getPort())
    );
    addSomeEvents(1);

    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            null
        )
    ).anyTimes();

    replayAll();

    supervisor.start();

    Assert.assertTrue(supervisor.isLifecycleStarted());
    Assert.assertFalse(supervisor.isStarted());

    verifyAll();

    while (supervisor.getInitRetryCounter() < 3) {
      Thread.sleep(1000);
    }

    // Portion below is the same test as testNoInitialState(), testing the supervisor after the initialiation is fixed
    resetAll();

    Capture<KafkaIndexTask> captured = Capture.newInstance();
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            null
        )
    ).anyTimes();
    expect(taskQueue.add(capture(captured))).andReturn(true);
    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));
    replayAll();

    // Fix the bad hostname during the initialization retries and finish the supervisor start.
    // This is equivalent to supervisor.start() in testNoInitialState().
    // The test supervisor has a P1D period, so we need to manually trigger the initialization retry.
    supervisor.getIoConfig().getConsumerProperties().put("bootstrap.servers", kafkaHost);
    supervisor.tryInit();

    Assert.assertTrue(supervisor.isLifecycleStarted());
    Assert.assertTrue(supervisor.isStarted());

    supervisor.runInternal();
    verifyAll();

    KafkaIndexTask task = captured.getValue();
    Assert.assertEquals(dataSchema, task.getDataSchema());
    Assert.assertEquals(tuningConfig.convertToTaskTuningConfig(), task.getTuningConfig());

    KafkaIndexTaskIOConfig taskConfig = task.getIOConfig();
    Assert.assertEquals(kafkaHost, taskConfig.getConsumerProperties().get("bootstrap.servers"));
    Assert.assertEquals("myCustomValue", taskConfig.getConsumerProperties().get("myCustomKey"));
    Assert.assertEquals("sequenceName-0", taskConfig.getBaseSequenceName());
    Assert.assertTrue("isUseTransaction", taskConfig.isUseTransaction());
    Assert.assertFalse("minimumMessageTime", taskConfig.getMinimumMessageTime().isPresent());
    Assert.assertFalse("maximumMessageTime", taskConfig.getMaximumMessageTime().isPresent());
    Assert.assertFalse("skipOffsetGaps", taskConfig.isSkipOffsetGaps());

    Assert.assertEquals(topic, taskConfig.getStartPartitions().getStream());
    Assert.assertEquals(0L, (long) taskConfig.getStartPartitions().getPartitionSequenceNumberMap().get(0));
    Assert.assertEquals(0L, (long) taskConfig.getStartPartitions().getPartitionSequenceNumberMap().get(1));
    Assert.assertEquals(0L, (long) taskConfig.getStartPartitions().getPartitionSequenceNumberMap().get(2));

    Assert.assertEquals(topic, taskConfig.getEndPartitions().getStream());
    Assert.assertEquals(Long.MAX_VALUE, (long) taskConfig.getEndPartitions().getPartitionSequenceNumberMap().get(0));
    Assert.assertEquals(Long.MAX_VALUE, (long) taskConfig.getEndPartitions().getPartitionSequenceNumberMap().get(1));
    Assert.assertEquals(Long.MAX_VALUE, (long) taskConfig.getEndPartitions().getPartitionSequenceNumberMap().get(2));
  }

  @Test
  public void testGetCurrentTotalStats()
  {
    supervisor = getSupervisor(1, 2, true, "PT1H", null, null, false);
    supervisor.addTaskGroupToActivelyReadingTaskGroup(
        supervisor.getTaskGroupIdForPartition(0),
        ImmutableMap.of(0, 0L),
        Optional.absent(),
        Optional.absent(),
        ImmutableSet.of("task1"),
        ImmutableSet.of()
    );

    supervisor.addTaskGroupToPendingCompletionTaskGroup(
        supervisor.getTaskGroupIdForPartition(1),
        ImmutableMap.of(0, 0L),
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


  private void addSomeEvents(int numEventsPerPartition) throws Exception
  {
    //create topic manually
    AdminUtils.createTopic(zkUtils, topic, NUM_PARTITIONS, 1, new Properties(), RackAwareMode.Enforced$.MODULE$);

    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      for (int i = 0; i < NUM_PARTITIONS; i++) {
        for (int j = 0; j < numEventsPerPartition; j++) {
          kafkaProducer.send(
              new ProducerRecord<>(
                  topic,
                  i,
                  null,
                  StringUtils.toUtf8(StringUtils.format("event-%d", j))
              )
          ).get();
        }
      }
    }
  }

  private KafkaSupervisor getSupervisor(
      int replicas,
      int taskCount,
      boolean useEarliestOffset,
      String duration,
      Period lateMessageRejectionPeriod,
      Period earlyMessageRejectionPeriod,
      boolean skipOffsetGaps
  )
  {
    return getSupervisor(
        replicas,
        taskCount,
        useEarliestOffset,
        duration,
        lateMessageRejectionPeriod,
        earlyMessageRejectionPeriod,
        skipOffsetGaps,
        false,
        kafkaHost
    );
  }

  private KafkaSupervisor getSupervisor(
      int replicas,
      int taskCount,
      boolean useEarliestOffset,
      String duration,
      Period lateMessageRejectionPeriod,
      Period earlyMessageRejectionPeriod,
      boolean skipOffsetGaps,
      boolean suspended,
      String kafkaHost
  )
  {
    Map<String, Object> consumerProperties = new HashMap<>();
    consumerProperties.put("myCustomKey", "myCustomValue");
    consumerProperties.put("bootstrap.servers", kafkaHost);
    KafkaSupervisorIOConfig kafkaSupervisorIOConfig = new KafkaSupervisorIOConfig(
        topic,
        replicas,
        taskCount,
        new Period(duration),
        consumerProperties,
        KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
        new Period("P1D"),
        new Period("PT30S"),
        useEarliestOffset,
        new Period("PT30M"),
        lateMessageRejectionPeriod,
        earlyMessageRejectionPeriod,
        skipOffsetGaps
    );

    KafkaIndexTaskClientFactory taskClientFactory = new KafkaIndexTaskClientFactory(
        null,
        null
    )
    {
      @Override
      public KafkaIndexTaskClient build(
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

    return new TestableKafkaSupervisor(
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        taskClientFactory,
        objectMapper,
        new KafkaSupervisorSpec(
            dataSchema,
            tuningConfig,
            kafkaSupervisorIOConfig,
            null,
            suspended,
            taskStorage,
            taskMaster,
            indexerMetadataStorageCoordinator,
            taskClientFactory,
            objectMapper,
            new NoopServiceEmitter(),
            new DruidMonitorSchedulerConfig(),
            rowIngestionMetersFactory
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

  private KafkaIndexTask createKafkaIndexTask(
      String id,
      String dataSource,
      int taskGroupId,
      SeekableStreamPartitions<Integer, Long> startPartitions,
      SeekableStreamPartitions<Integer, Long> endPartitions,
      DateTime minimumMessageTime,
      DateTime maximumMessageTime
  )
  {
    return new KafkaIndexTask(
        id,
        null,
        getDataSchema(dataSource),
        tuningConfig,
        new KafkaIndexTaskIOConfig(
            taskGroupId,
            "sequenceName-" + taskGroupId,
            startPartitions,
            endPartitions,
            ImmutableMap.of("bootstrap.servers", kafkaHost),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            minimumMessageTime,
            maximumMessageTime,
            false
        ),
        Collections.emptyMap(),
        null,
        null,
        rowIngestionMetersFactory,
        objectMapper
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

  private static class TestableKafkaSupervisor extends KafkaSupervisor
  {
    public TestableKafkaSupervisor(
        TaskStorage taskStorage,
        TaskMaster taskMaster,
        IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator,
        KafkaIndexTaskClientFactory taskClientFactory,
        ObjectMapper mapper,
        KafkaSupervisorSpec spec,
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
          rowIngestionMetersFactory
      );
    }

    @Override
    protected String generateSequenceName(
        Map<Integer, Long> startPartitions,
        Optional<DateTime> minimumMessageTime,
        Optional<DateTime> maximumMessageTime
    )
    {
      final int groupId = getTaskGroupIdForPartition(startPartitions.keySet().iterator().next());
      return StringUtils.format("sequenceName-%d", groupId);
    }
  }


}
