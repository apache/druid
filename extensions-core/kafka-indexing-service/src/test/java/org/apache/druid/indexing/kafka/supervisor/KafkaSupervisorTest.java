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
import org.apache.curator.test.TestingCluster;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskInfoProvider;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.common.stats.RowIngestionMetersFactory;
import org.apache.druid.indexing.common.task.RealtimeIndexTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.kafka.KafkaConsumerConfigs;
import org.apache.druid.indexing.kafka.KafkaDataSourceMetadata;
import org.apache.druid.indexing.kafka.KafkaIndexTask;
import org.apache.druid.indexing.kafka.KafkaIndexTaskClient;
import org.apache.druid.indexing.kafka.KafkaIndexTaskClientFactory;
import org.apache.druid.indexing.kafka.KafkaIndexTaskIOConfig;
import org.apache.druid.indexing.kafka.KafkaIndexTaskTuningConfig;
import org.apache.druid.indexing.kafka.KafkaRecordSupplier;
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
import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManager;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManagerConfig;
import org.apache.druid.indexing.seekablestream.SeekableStreamEndSequenceNumbers;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskRunner.Status;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskTuningConfig;
import org.apache.druid.indexing.seekablestream.SeekableStreamStartSequenceNumbers;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorStateManager;
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
import org.apache.druid.segment.realtime.appenderator.DummyForInjectionAppenderatorsManager;
import org.apache.druid.server.metrics.DruidMonitorSchedulerConfig;
import org.apache.druid.server.metrics.ExceptionCapturingServiceEmitter;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.Executor;

@RunWith(Parameterized.class)
public class KafkaSupervisorTest extends EasyMockSupport
{
  private static final ObjectMapper OBJECT_MAPPER = TestHelper.makeJsonMapper();
  private static final InputFormat INPUT_FORMAT = new JsonInputFormat(
      new JSONPathSpec(true, ImmutableList.of()),
      ImmutableMap.of(),
      false
  );
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

  private final int numThreads;

  private TestableKafkaSupervisor supervisor;
  private TaskStorage taskStorage;
  private TaskMaster taskMaster;
  private TaskRunner taskRunner;
  private IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator;
  private KafkaIndexTaskClient taskClient;
  private TaskQueue taskQueue;
  private String topic;
  private RowIngestionMetersFactory rowIngestionMetersFactory;
  private ExceptionCapturingServiceEmitter serviceEmitter;
  private SupervisorStateManagerConfig supervisorConfig;

  private static String getTopic()
  {
    //noinspection StringConcatenationMissingWhitespace
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

    topic = getTopic();
    rowIngestionMetersFactory = new TestUtils().getRowIngestionMetersFactory();
    serviceEmitter = new ExceptionCapturingServiceEmitter();
    EmittingLogger.registerEmitter(serviceEmitter);
    supervisorConfig = new SupervisorStateManagerConfig();
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
  }

  @Test
  public void testCreateBaseTaskContexts() throws JsonProcessingException
  {
    supervisor = getTestableSupervisor(1, 1, true, "PT1H", null, null);
    final Map<String, Object> contexts = supervisor.createIndexTasks(
        1,
        "seq",
        OBJECT_MAPPER,
        new TreeMap<>(),
        new KafkaIndexTaskIOConfig(
            0,
            "seq",
            new SeekableStreamStartSequenceNumbers<>("test", Collections.emptyMap(), Collections.emptySet()),
            new SeekableStreamEndSequenceNumbers<>("test", Collections.emptyMap()),
            Collections.emptyMap(),
            null,
            null,
            null,
            null,
            INPUT_FORMAT
        ),
        new KafkaIndexTaskTuningConfig(
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
            null,
            null,
            null,
            null,
            null
        ),
        null
    ).get(0).getContext();
    final Boolean contextValue = (Boolean) contexts.get("IS_INCREMENTAL_HANDOFF_SUPPORTED");
    Assert.assertNotNull(contextValue);
    Assert.assertTrue(contextValue);
  }

  @Test
  public void testNoInitialState() throws Exception
  {
    supervisor = getTestableSupervisor(1, 1, true, "PT1H", null, null);
    final KafkaSupervisorTuningConfig tuningConfig = supervisor.getTuningConfig();
    addSomeEvents(1);

    Capture<KafkaIndexTask> captured = Capture.newInstance();
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            null
        )
    ).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.capture(captured))).andReturn(true);
    taskRunner.registerListener(EasyMock.anyObject(TaskRunnerListener.class), EasyMock.anyObject(Executor.class));
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

    Assert.assertEquals(topic, taskConfig.getStartSequenceNumbers().getStream());
    Assert.assertEquals(0L, (long) taskConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap().get(0));
    Assert.assertEquals(0L, (long) taskConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap().get(1));
    Assert.assertEquals(0L, (long) taskConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap().get(2));

    Assert.assertEquals(topic, taskConfig.getEndSequenceNumbers().getStream());
    Assert.assertEquals(
        Long.MAX_VALUE,
        (long) taskConfig.getEndSequenceNumbers().getPartitionSequenceNumberMap().get(0)
    );
    Assert.assertEquals(
        Long.MAX_VALUE,
        (long) taskConfig.getEndSequenceNumbers().getPartitionSequenceNumberMap().get(1)
    );
    Assert.assertEquals(
        Long.MAX_VALUE,
        (long) taskConfig.getEndSequenceNumbers().getPartitionSequenceNumberMap().get(2)
    );
  }

  @Test
  public void testSkipOffsetGaps() throws Exception
  {
    supervisor = getTestableSupervisor(1, 1, true, "PT1H", null, null);
    addSomeEvents(1);

    Capture<KafkaIndexTask> captured = Capture.newInstance();
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            null
        )
    ).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.capture(captured))).andReturn(true);
    taskRunner.registerListener(EasyMock.anyObject(TaskRunnerListener.class), EasyMock.anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();
  }

  @Test
  public void testMultiTask() throws Exception
  {
    supervisor = getTestableSupervisor(1, 2, true, "PT1H", null, null);
    addSomeEvents(1);

    Capture<KafkaIndexTask> captured = Capture.newInstance(CaptureType.ALL);
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            null
        )
    ).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.capture(captured))).andReturn(true).times(2);
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    KafkaIndexTask task1 = captured.getValues().get(0);
    Assert.assertEquals(2, task1.getIOConfig().getStartSequenceNumbers().getPartitionSequenceNumberMap().size());
    Assert.assertEquals(2, task1.getIOConfig().getEndSequenceNumbers().getPartitionSequenceNumberMap().size());
    Assert.assertEquals(
        0L,
        task1.getIOConfig().getStartSequenceNumbers().getPartitionSequenceNumberMap().get(0).longValue()
    );
    Assert.assertEquals(
        Long.MAX_VALUE,
        task1.getIOConfig().getEndSequenceNumbers().getPartitionSequenceNumberMap().get(0).longValue()
    );
    Assert.assertEquals(
        0L,
        task1.getIOConfig().getStartSequenceNumbers().getPartitionSequenceNumberMap().get(2).longValue()
    );
    Assert.assertEquals(
        Long.MAX_VALUE,
        task1.getIOConfig().getEndSequenceNumbers().getPartitionSequenceNumberMap().get(2).longValue()
    );

    KafkaIndexTask task2 = captured.getValues().get(1);
    Assert.assertEquals(1, task2.getIOConfig().getStartSequenceNumbers().getPartitionSequenceNumberMap().size());
    Assert.assertEquals(1, task2.getIOConfig().getEndSequenceNumbers().getPartitionSequenceNumberMap().size());
    Assert.assertEquals(
        0L,
        task2.getIOConfig().getStartSequenceNumbers().getPartitionSequenceNumberMap().get(1).longValue()
    );
    Assert.assertEquals(
        Long.MAX_VALUE,
        task2.getIOConfig().getEndSequenceNumbers().getPartitionSequenceNumberMap().get(1).longValue()
    );
  }

  @Test
  public void testReplicas() throws Exception
  {
    supervisor = getTestableSupervisor(2, 1, true, "PT1H", null, null);
    addSomeEvents(1);

    Capture<KafkaIndexTask> captured = Capture.newInstance(CaptureType.ALL);
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            null
        )
    ).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.capture(captured))).andReturn(true).times(2);
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    KafkaIndexTask task1 = captured.getValues().get(0);
    Assert.assertEquals(3, task1.getIOConfig().getStartSequenceNumbers().getPartitionSequenceNumberMap().size());
    Assert.assertEquals(3, task1.getIOConfig().getEndSequenceNumbers().getPartitionSequenceNumberMap().size());
    Assert.assertEquals(
        0L,
        task1.getIOConfig().getStartSequenceNumbers().getPartitionSequenceNumberMap().get(0).longValue()
    );
    Assert.assertEquals(
        0L,
        task1.getIOConfig().getStartSequenceNumbers().getPartitionSequenceNumberMap().get(1).longValue()
    );
    Assert.assertEquals(
        0L,
        task1.getIOConfig().getStartSequenceNumbers().getPartitionSequenceNumberMap().get(2).longValue()
    );

    KafkaIndexTask task2 = captured.getValues().get(1);
    Assert.assertEquals(3, task2.getIOConfig().getStartSequenceNumbers().getPartitionSequenceNumberMap().size());
    Assert.assertEquals(3, task2.getIOConfig().getEndSequenceNumbers().getPartitionSequenceNumberMap().size());
    Assert.assertEquals(
        0L,
        task2.getIOConfig().getStartSequenceNumbers().getPartitionSequenceNumberMap().get(0).longValue()
    );
    Assert.assertEquals(
        0L,
        task2.getIOConfig().getStartSequenceNumbers().getPartitionSequenceNumberMap().get(1).longValue()
    );
    Assert.assertEquals(
        0L,
        task2.getIOConfig().getStartSequenceNumbers().getPartitionSequenceNumberMap().get(2).longValue()
    );
  }

  @Test
  public void testLateMessageRejectionPeriod() throws Exception
  {
    supervisor = getTestableSupervisor(2, 1, true, "PT1H", new Period("PT1H"), null);
    addSomeEvents(1);

    Capture<KafkaIndexTask> captured = Capture.newInstance(CaptureType.ALL);
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            null
        )
    ).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.capture(captured))).andReturn(true).times(2);
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
    supervisor = getTestableSupervisor(2, 1, true, "PT1H", null, new Period("PT1H"));
    addSomeEvents(1);

    Capture<KafkaIndexTask> captured = Capture.newInstance(CaptureType.ALL);
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            null
        )
    ).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.capture(captured))).andReturn(true).times(2);
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

  /**
   * Test generating the starting offsets from the partition high water marks in Kafka.
   */
  @Test
  public void testLatestOffset() throws Exception
  {
    supervisor = getTestableSupervisor(1, 1, false, "PT1H", null, null);
    addSomeEvents(1100);

    Capture<KafkaIndexTask> captured = Capture.newInstance();
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            null
        )
    ).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.capture(captured))).andReturn(true);
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    KafkaIndexTask task = captured.getValue();
    Assert.assertEquals(
        1101L,
        task.getIOConfig().getStartSequenceNumbers().getPartitionSequenceNumberMap().get(0).longValue()
    );
    Assert.assertEquals(
        1101L,
        task.getIOConfig().getStartSequenceNumbers().getPartitionSequenceNumberMap().get(1).longValue()
    );
    Assert.assertEquals(
        1101L,
        task.getIOConfig().getStartSequenceNumbers().getPartitionSequenceNumberMap().get(2).longValue()
    );
  }

  /**
   * Test if partitionIds get updated
   */
  @Test
  public void testPartitionIdsUpdates() throws Exception
  {
    supervisor = getTestableSupervisor(1, 1, false, "PT1H", null, null);
    addSomeEvents(1100);

    Capture<KafkaIndexTask> captured = Capture.newInstance();
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            null
        )
    ).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.capture(captured))).andReturn(true);
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    Assert.assertFalse(supervisor.isPartitionIdsEmpty());
  }


  @Test
  public void testAlwaysUsesEarliestOffsetForNewlyDiscoveredPartitions() throws Exception
  {
    supervisor = getTestableSupervisor(1, 1, false, "PT1H", null, null);
    addSomeEvents(9);

    Capture<KafkaIndexTask> captured = Capture.newInstance();
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            null
        )
    ).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.capture(captured))).andReturn(true);
    replayAll();
    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    KafkaIndexTask task = captured.getValue();
    Assert.assertEquals(
        10,
        task.getIOConfig().getStartSequenceNumbers().getPartitionSequenceNumberMap().get(0).longValue()
    );
    Assert.assertEquals(
        10,
        task.getIOConfig().getStartSequenceNumbers().getPartitionSequenceNumberMap().get(1).longValue()
    );
    Assert.assertEquals(
        10,
        task.getIOConfig().getStartSequenceNumbers().getPartitionSequenceNumberMap().get(2).longValue()
    );

    addMoreEvents(9, 6);
    EasyMock.reset(taskQueue, taskStorage);
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    Capture<KafkaIndexTask> tmp = Capture.newInstance();
    EasyMock.expect(taskQueue.add(EasyMock.capture(tmp))).andReturn(true);
    EasyMock.replay(taskStorage, taskQueue);
    supervisor.runInternal();
    verifyAll();

    EasyMock.reset(taskQueue, taskStorage);
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    Capture<KafkaIndexTask> newcaptured = Capture.newInstance();
    EasyMock.expect(taskQueue.add(EasyMock.capture(newcaptured))).andReturn(true);
    EasyMock.replay(taskStorage, taskQueue);
    supervisor.runInternal();
    verifyAll();

    //check if start from earliest offset
    task = newcaptured.getValue();
    Assert.assertEquals(
        0,
        task.getIOConfig().getStartSequenceNumbers().getPartitionSequenceNumberMap().get(3).longValue()
    );
    Assert.assertEquals(
        0,
        task.getIOConfig().getStartSequenceNumbers().getPartitionSequenceNumberMap().get(4).longValue()
    );
    Assert.assertEquals(
        0,
        task.getIOConfig().getStartSequenceNumbers().getPartitionSequenceNumberMap().get(5).longValue()
    );
  }

  /**
   * Test generating the starting offsets from the partition data stored in druid_dataSource which contains the
   * offsets of the last built segments.
   */
  @Test
  public void testDatasourceMetadata() throws Exception
  {
    supervisor = getTestableSupervisor(1, 1, true, "PT1H", null, null);
    addSomeEvents(100);

    Capture<KafkaIndexTask> captured = Capture.newInstance();
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(0, 10L, 1, 20L, 2, 30L), ImmutableSet.of())
        )
    ).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.capture(captured))).andReturn(true);
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    KafkaIndexTask task = captured.getValue();
    KafkaIndexTaskIOConfig taskConfig = task.getIOConfig();
    Assert.assertEquals("sequenceName-0", taskConfig.getBaseSequenceName());
    Assert.assertEquals(
        10L,
        taskConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap().get(0).longValue()
    );
    Assert.assertEquals(
        20L,
        taskConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap().get(1).longValue()
    );
    Assert.assertEquals(
        30L,
        taskConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap().get(2).longValue()
    );
  }

  @Test
  public void testBadMetadataOffsets() throws Exception
  {
    supervisor = getTestableSupervisor(1, 1, true, "PT1H", null, null);
    addSomeEvents(1);

    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    // for simplicity in testing the offset availability check, we use negative stored offsets in metadata here,
    // because the stream's earliest offset is 0, although that would not happen in real usage.
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            new SeekableStreamStartSequenceNumbers<>(
                topic,
                ImmutableMap.of(0, -10L, 1, -20L, 2, -30L),
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
    addSomeEvents(1);

    // non KafkaIndexTask (don't kill)
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
            .andReturn(Futures.immediateFuture(Status.NOT_STARTED))
            .anyTimes();
    EasyMock.expect(taskClient.getStartTimeAsync(EasyMock.anyString()))
            .andReturn(Futures.immediateFuture(DateTimes.nowUtc()))
            .anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            null
        )
    ).anyTimes();

    taskRunner.registerListener(EasyMock.anyObject(TaskRunnerListener.class), EasyMock.anyObject(Executor.class));

    EasyMock.expect(taskQueue.add(EasyMock.anyObject(Task.class))).andReturn(true).anyTimes();

    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();
  }

  @Test
  public void testKillBadPartitionAssignment() throws Exception
  {
    supervisor = getTestableSupervisor(1, 2, true, "PT1H", null, null);
    final KafkaSupervisorTuningConfig tuningConfig = supervisor.getTuningConfig();
    addSomeEvents(1);

    Task id1 = createKafkaIndexTask(
        "id1",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>("topic", ImmutableMap.of(0, 0L, 2, 0L), ImmutableSet.of()),
        new SeekableStreamEndSequenceNumbers<>("topic", ImmutableMap.of(0, Long.MAX_VALUE, 2, Long.MAX_VALUE)),
        null,
        null,
        tuningConfig
    );
    Task id2 = createKafkaIndexTask(
        "id2",
        DATASOURCE,
        1,
        new SeekableStreamStartSequenceNumbers<>("topic", ImmutableMap.of(1, 0L), ImmutableSet.of()),
        new SeekableStreamEndSequenceNumbers<>("topic", ImmutableMap.of(1, Long.MAX_VALUE)),
        null,
        null,
        tuningConfig
    );
    Task id3 = createKafkaIndexTask(
        "id3",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>("topic", ImmutableMap.of(0, 0L, 1, 0L, 2, 0L), ImmutableSet.of()),
        new SeekableStreamEndSequenceNumbers<>(
            "topic",
            ImmutableMap.of(0, Long.MAX_VALUE, 1, Long.MAX_VALUE, 2, Long.MAX_VALUE)
        ),
        null,
        null,
        tuningConfig
    );
    Task id4 = createKafkaIndexTask(
        "id4",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>("topic", ImmutableMap.of(0, 0L, 1, 0L), ImmutableSet.of()),
        new SeekableStreamEndSequenceNumbers<>("topic", ImmutableMap.of(0, Long.MAX_VALUE, 1, Long.MAX_VALUE)),
        null,
        null,
        tuningConfig
    );
    Task id5 = createKafkaIndexTask(
        "id5",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>("topic", ImmutableMap.of(1, 0L, 2, 0L), ImmutableSet.of()),
        new SeekableStreamEndSequenceNumbers<>("topic", ImmutableMap.of(1, Long.MAX_VALUE, 2, Long.MAX_VALUE)),
        null,
        null,
        tuningConfig
    );

    List<Task> existingTasks = ImmutableList.of(id1, id2, id3, id4, id5);

    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(Collections.emptyList()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(existingTasks).anyTimes();
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
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            null
        )
    ).anyTimes();
    EasyMock.expect(taskClient.stopAsync("id3", false)).andReturn(Futures.immediateFuture(true));
    EasyMock.expect(taskClient.stopAsync("id4", false)).andReturn(Futures.immediateFuture(false));
    EasyMock.expect(taskClient.stopAsync("id5", false)).andReturn(Futures.immediateFuture(null));

    TreeMap<Integer, Map<Integer, Long>> checkpoints1 = new TreeMap<>();
    checkpoints1.put(0, ImmutableMap.of(0, 0L, 2, 0L));
    TreeMap<Integer, Map<Integer, Long>> checkpoints2 = new TreeMap<>();
    checkpoints2.put(0, ImmutableMap.of(1, 0L));
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
    supervisor = getTestableSupervisor(2, 2, true, "PT1H", null, null);
    addSomeEvents(1);

    Capture<Task> captured = Capture.newInstance(CaptureType.ALL);
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(Collections.emptyList()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(taskClient.getStatusAsync(EasyMock.anyString()))
            .andReturn(Futures.immediateFuture(Status.NOT_STARTED))
            .anyTimes();
    EasyMock.expect(taskClient.getStartTimeAsync(EasyMock.anyString()))
            .andReturn(Futures.immediateFuture(DateTimes.nowUtc()))
            .anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            null
        )
    ).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.capture(captured))).andReturn(true).times(4);

    TreeMap<Integer, Map<Integer, Long>> checkpoints1 = new TreeMap<>();
    checkpoints1.put(0, ImmutableMap.of(0, 0L, 2, 0L));
    TreeMap<Integer, Map<Integer, Long>> checkpoints2 = new TreeMap<>();
    checkpoints2.put(0, ImmutableMap.of(1, 0L));
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
    KafkaIndexTask iHaveFailed = (KafkaIndexTask) tasks.get(3);
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
            .andReturn(Optional.of(TaskStatus.failure(iHaveFailed.getId())));
    EasyMock.expect(taskStorage.getTask(iHaveFailed.getId())).andReturn(Optional.of(iHaveFailed)).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.capture(aNewTaskCapture))).andReturn(true);
    EasyMock.replay(taskStorage);
    EasyMock.replay(taskQueue);

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
    supervisor = getTestableSupervisor(2, 1, true, "PT1H", null, null);
    addSomeEvents(1);

    DateTime now = DateTimes.nowUtc();
    DateTime maxi = now.plusMinutes(60);
    Task id1 = createKafkaIndexTask(
        "id1",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>("topic", ImmutableMap.of(0, 0L, 2, 0L), ImmutableSet.of()),
        new SeekableStreamEndSequenceNumbers<>("topic", ImmutableMap.of(0, Long.MAX_VALUE, 2, Long.MAX_VALUE)),
        now,
        maxi,
        supervisor.getTuningConfig()
    );

    List<Task> existingTasks = ImmutableList.of(id1);

    Capture<Task> captured = Capture.newInstance();
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(Collections.emptyList()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(existingTasks).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id1")).andReturn(Optional.of(TaskStatus.running("id1"))).anyTimes();
    EasyMock.expect(taskStorage.getTask("id1")).andReturn(Optional.of(id1)).anyTimes();
    EasyMock.expect(taskClient.getStatusAsync("id1")).andReturn(Futures.immediateFuture(Status.READING));
    EasyMock.expect(taskClient.getStartTimeAsync("id1")).andReturn(Futures.immediateFuture(now)).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.capture(captured))).andReturn(true);
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            null
        )
    ).anyTimes();

    TreeMap<Integer, Map<Integer, Long>> checkpoints = new TreeMap<>();
    checkpoints.put(0, ImmutableMap.of(0, 0L, 2, 0L));
    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("id1"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpoints))
            .times(2);

    taskRunner.registerListener(EasyMock.anyObject(TaskRunnerListener.class), EasyMock.anyObject(Executor.class));
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
    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("sequenceName-0"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpoints))
            .times(2);
    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("id1"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpoints))
            .times(1);

    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE))
            .andReturn(ImmutableList.of(captured.getValue()))
            .anyTimes();
    EasyMock.expect(taskStorage.getStatus(iHaveFailed.getId()))
            .andReturn(Optional.of(TaskStatus.failure(iHaveFailed.getId())));
    EasyMock.expect(taskStorage.getStatus(runningTaskId))
            .andReturn(Optional.of(TaskStatus.running(runningTaskId)))
            .anyTimes();
    EasyMock.expect(taskStorage.getTask(iHaveFailed.getId())).andReturn(Optional.of(iHaveFailed)).anyTimes();
    EasyMock.expect(taskStorage.getTask(runningTaskId)).andReturn(Optional.of(captured.getValue())).anyTimes();
    EasyMock.expect(taskClient.getStatusAsync(runningTaskId)).andReturn(Futures.immediateFuture(Status.READING));
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
    supervisor = getTestableSupervisor(2, 2, true, "PT1H", null, null);
    addSomeEvents(1);

    Capture<Task> captured = Capture.newInstance(CaptureType.ALL);
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(Collections.emptyList()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(taskClient.getStatusAsync(EasyMock.anyString()))
            .andReturn(Futures.immediateFuture(Status.NOT_STARTED))
            .anyTimes();
    EasyMock.expect(taskClient.getStartTimeAsync(EasyMock.anyString()))
            .andReturn(Futures.immediateFuture(DateTimes.nowUtc()))
            .anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
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
    TreeMap<Integer, Map<Integer, Long>> checkpoints1 = new TreeMap<>();
    checkpoints1.put(0, ImmutableMap.of(0, 0L, 2, 0L));
    TreeMap<Integer, Map<Integer, Long>> checkpoints2 = new TreeMap<>();
    checkpoints2.put(0, ImmutableMap.of(1, 0L));
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

    // test that a task succeeding causes a new task to be re-queued with the next offset range and causes any replica
    // tasks to be shutdown
    Capture<Task> newTasksCapture = Capture.newInstance(CaptureType.ALL);
    Capture<String> shutdownTaskIdCapture = Capture.newInstance();
    List<Task> imStillRunning = tasks.subList(1, 4);
    KafkaIndexTask iAmSuccess = (KafkaIndexTask) tasks.get(0);
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
    final TaskLocation location = new TaskLocation("testHost", 1234, -1);

    supervisor = getTestableSupervisor(2, 2, true, "PT1M", null, null);
    final KafkaSupervisorTuningConfig tuningConfig = supervisor.getTuningConfig();
    addSomeEvents(100);

    Capture<Task> captured = Capture.newInstance(CaptureType.ALL);
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(Collections.emptyList()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
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
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(tasks).anyTimes();
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
            .andReturn(Futures.immediateFuture(ImmutableMap.of(0, 10L, 2, 30L)))
            .andReturn(Futures.immediateFuture(ImmutableMap.of(0, 10L, 2, 35L)));
    EasyMock.expect(
        taskClient.setEndOffsetsAsync(
            EasyMock.contains("sequenceName-0"),
            EasyMock.eq(ImmutableMap.of(0, 10L, 2, 35L)),
            EasyMock.eq(true)
        )
    ).andReturn(Futures.immediateFuture(true)).times(2);
    EasyMock.expect(taskQueue.add(EasyMock.capture(captured))).andReturn(true).times(2);

    TreeMap<Integer, Map<Integer, Long>> checkpoints1 = new TreeMap<>();
    checkpoints1.put(0, ImmutableMap.of(0, 0L, 2, 0L));
    TreeMap<Integer, Map<Integer, Long>> checkpoints2 = new TreeMap<>();
    checkpoints2.put(0, ImmutableMap.of(1, 0L));
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
      KafkaIndexTask kafkaIndexTask = (KafkaIndexTask) task;
      Assert.assertEquals(dataSchema, kafkaIndexTask.getDataSchema());
      Assert.assertEquals(tuningConfig.convertToTaskTuningConfig(), kafkaIndexTask.getTuningConfig());

      KafkaIndexTaskIOConfig taskConfig = kafkaIndexTask.getIOConfig();
      Assert.assertEquals("sequenceName-0", taskConfig.getBaseSequenceName());
      Assert.assertTrue("isUseTransaction", taskConfig.isUseTransaction());

      Assert.assertEquals(topic, taskConfig.getStartSequenceNumbers().getStream());
      Assert.assertEquals(10L, (long) taskConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap().get(0));
      Assert.assertEquals(35L, (long) taskConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap().get(2));
    }
  }

  @Test
  public void testDiscoverExistingPublishingTask() throws Exception
  {
    final TaskLocation location = new TaskLocation("testHost", 1234, -1);

    supervisor = getTestableSupervisor(1, 1, true, "PT1H", null, null);
    final KafkaSupervisorTuningConfig tuningConfig = supervisor.getTuningConfig();
    addSomeEvents(1);

    Task task = createKafkaIndexTask(
        "id1",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>("topic", ImmutableMap.of(0, 0L, 1, 0L, 2, 0L), ImmutableSet.of()),
        new SeekableStreamEndSequenceNumbers<>(
            "topic",
            ImmutableMap.of(0, Long.MAX_VALUE, 1, Long.MAX_VALUE, 2, Long.MAX_VALUE)
        ),
        null,
        null,
        supervisor.getTuningConfig()
    );

    Collection workItems = new ArrayList<>();
    workItems.add(new TestTaskRunnerWorkItem(task, null, location));

    Capture<KafkaIndexTask> captured = Capture.newInstance();
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(workItems).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of(task)).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id1")).andReturn(Optional.of(TaskStatus.running("id1"))).anyTimes();
    EasyMock.expect(taskStorage.getTask("id1")).andReturn(Optional.of(task)).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            null
        )
    ).anyTimes();
    EasyMock.expect(taskClient.getStatusAsync("id1")).andReturn(Futures.immediateFuture(Status.PUBLISHING));
    EasyMock.expect(taskClient.getCurrentOffsetsAsync("id1", false))
            .andReturn(Futures.immediateFuture(ImmutableMap.of(0, 10L, 1, 20L, 2, 30L)));
    EasyMock.expect(taskClient.getEndOffsets("id1")).andReturn(ImmutableMap.of(0, 10L, 1, 20L, 2, 30L));
    EasyMock.expect(taskQueue.add(EasyMock.capture(captured))).andReturn(true);

    TreeMap<Integer, Map<Integer, Long>> checkpoints = new TreeMap<>();
    checkpoints.put(0, ImmutableMap.of(0, 0L, 1, 0L, 2, 0L));
    EasyMock.expect(taskClient.getCheckpoints(EasyMock.anyString(), EasyMock.anyBoolean()))
            .andReturn(checkpoints)
            .anyTimes();

    taskRunner.registerListener(EasyMock.anyObject(TaskRunnerListener.class), EasyMock.anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    supervisor.updateCurrentAndLatestOffsets();
    SupervisorReport<KafkaSupervisorReportPayload> report = supervisor.getStatus();
    verifyAll();

    Assert.assertEquals(DATASOURCE, report.getId());

    KafkaSupervisorReportPayload payload = report.getPayload();

    Assert.assertEquals(DATASOURCE, payload.getDataSource());
    Assert.assertEquals(3600L, payload.getDurationSeconds());
    Assert.assertEquals(NUM_PARTITIONS, payload.getPartitions());
    Assert.assertEquals(1, payload.getReplicas());
    Assert.assertEquals(topic, payload.getStream());
    Assert.assertEquals(0, payload.getActiveTasks().size());
    Assert.assertEquals(1, payload.getPublishingTasks().size());
    Assert.assertEquals(SupervisorStateManager.BasicState.RUNNING, payload.getDetailedState());
    Assert.assertEquals(0, payload.getRecentErrors().size());

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
    Assert.assertEquals(topic, capturedTaskConfig.getStartSequenceNumbers().getStream());
    Assert.assertEquals(
        10L,
        capturedTaskConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap().get(0).longValue()
    );
    Assert.assertEquals(
        20L,
        capturedTaskConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap().get(1).longValue()
    );
    Assert.assertEquals(
        30L,
        capturedTaskConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap().get(2).longValue()
    );

    Assert.assertEquals(topic, capturedTaskConfig.getEndSequenceNumbers().getStream());
    Assert.assertEquals(
        Long.MAX_VALUE,
        capturedTaskConfig.getEndSequenceNumbers().getPartitionSequenceNumberMap().get(0).longValue()
    );
    Assert.assertEquals(
        Long.MAX_VALUE,
        capturedTaskConfig.getEndSequenceNumbers().getPartitionSequenceNumberMap().get(1).longValue()
    );
    Assert.assertEquals(
        Long.MAX_VALUE,
        capturedTaskConfig.getEndSequenceNumbers().getPartitionSequenceNumberMap().get(2).longValue()
    );
  }

  @Test
  public void testDiscoverExistingPublishingTaskWithDifferentPartitionAllocation() throws Exception
  {
    final TaskLocation location = new TaskLocation("testHost", 1234, -1);

    supervisor = getTestableSupervisor(1, 1, true, "PT1H", null, null);
    final KafkaSupervisorTuningConfig tuningConfig = supervisor.getTuningConfig();
    addSomeEvents(1);

    Task task = createKafkaIndexTask(
        "id1",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>("topic", ImmutableMap.of(0, 0L, 2, 0L), ImmutableSet.of()),
        new SeekableStreamEndSequenceNumbers<>("topic", ImmutableMap.of(0, Long.MAX_VALUE, 2, Long.MAX_VALUE)),
        null,
        null,
        supervisor.getTuningConfig()
    );

    Collection workItems = new ArrayList<>();
    workItems.add(new TestTaskRunnerWorkItem(task, null, location));

    Capture<KafkaIndexTask> captured = Capture.newInstance();
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(workItems).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of(task)).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id1")).andReturn(Optional.of(TaskStatus.running("id1"))).anyTimes();
    EasyMock.expect(taskStorage.getTask("id1")).andReturn(Optional.of(task)).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            null
        )
    ).anyTimes();
    EasyMock.expect(taskClient.getStatusAsync("id1")).andReturn(Futures.immediateFuture(Status.PUBLISHING));
    EasyMock.expect(taskClient.getCurrentOffsetsAsync("id1", false))
            .andReturn(Futures.immediateFuture(ImmutableMap.of(0, 10L, 2, 30L)));
    EasyMock.expect(taskClient.getEndOffsets("id1")).andReturn(ImmutableMap.of(0, 10L, 2, 30L));
    EasyMock.expect(taskQueue.add(EasyMock.capture(captured))).andReturn(true);

    taskRunner.registerListener(EasyMock.anyObject(TaskRunnerListener.class), EasyMock.anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    supervisor.updateCurrentAndLatestOffsets();
    SupervisorReport<KafkaSupervisorReportPayload> report = supervisor.getStatus();
    verifyAll();

    Assert.assertEquals(DATASOURCE, report.getId());

    KafkaSupervisorReportPayload payload = report.getPayload();

    Assert.assertEquals(DATASOURCE, payload.getDataSource());
    Assert.assertEquals(3600L, payload.getDurationSeconds());
    Assert.assertEquals(NUM_PARTITIONS, payload.getPartitions());
    Assert.assertEquals(1, payload.getReplicas());
    Assert.assertEquals(topic, payload.getStream());
    Assert.assertEquals(0, payload.getActiveTasks().size());
    Assert.assertEquals(1, payload.getPublishingTasks().size());
    Assert.assertEquals(SupervisorStateManager.BasicState.RUNNING, payload.getDetailedState());
    Assert.assertEquals(0, payload.getRecentErrors().size());

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
    Assert.assertEquals(topic, capturedTaskConfig.getStartSequenceNumbers().getStream());
    Assert.assertEquals(
        10L,
        capturedTaskConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap().get(0).longValue()
    );
    Assert.assertEquals(
        0L,
        capturedTaskConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap().get(1).longValue()
    );
    Assert.assertEquals(
        30L,
        capturedTaskConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap().get(2).longValue()
    );

    Assert.assertEquals(topic, capturedTaskConfig.getEndSequenceNumbers().getStream());
    Assert.assertEquals(
        Long.MAX_VALUE,
        capturedTaskConfig.getEndSequenceNumbers().getPartitionSequenceNumberMap().get(0).longValue()
    );
    Assert.assertEquals(
        Long.MAX_VALUE,
        capturedTaskConfig.getEndSequenceNumbers().getPartitionSequenceNumberMap().get(1).longValue()
    );
    Assert.assertEquals(
        Long.MAX_VALUE,
        capturedTaskConfig.getEndSequenceNumbers().getPartitionSequenceNumberMap().get(2).longValue()
    );
  }

  @Test
  public void testDiscoverExistingPublishingAndReadingTask() throws Exception
  {
    final TaskLocation location1 = new TaskLocation("testHost", 1234, -1);
    final TaskLocation location2 = new TaskLocation("testHost2", 145, -1);
    final DateTime startTime = DateTimes.nowUtc();

    supervisor = getTestableSupervisor(1, 1, true, "PT1H", null, null);
    final KafkaSupervisorTuningConfig tuningConfig = supervisor.getTuningConfig();
    addSomeEvents(6);

    Task id1 = createKafkaIndexTask(
        "id1",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>("topic", ImmutableMap.of(0, 0L, 1, 0L, 2, 0L), ImmutableSet.of()),
        new SeekableStreamEndSequenceNumbers<>(
            "topic",
            ImmutableMap.of(0, Long.MAX_VALUE, 1, Long.MAX_VALUE, 2, Long.MAX_VALUE)
        ),
        null,
        null,
        tuningConfig
    );

    Task id2 = createKafkaIndexTask(
        "id2",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>("topic", ImmutableMap.of(0, 1L, 1, 2L, 2, 3L), ImmutableSet.of()),
        new SeekableStreamEndSequenceNumbers<>(
            "topic",
            ImmutableMap.of(0, Long.MAX_VALUE, 1, Long.MAX_VALUE, 2, Long.MAX_VALUE)
        ),
        null,
        null,
        tuningConfig
    );

    Collection workItems = new ArrayList<>();
    workItems.add(new TestTaskRunnerWorkItem(id1, null, location1));
    workItems.add(new TestTaskRunnerWorkItem(id2, null, location2));

    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(workItems).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE))
            .andReturn(ImmutableList.of(id1, id2))
            .anyTimes();
    EasyMock.expect(taskStorage.getStatus("id1")).andReturn(Optional.of(TaskStatus.running("id1"))).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id2")).andReturn(Optional.of(TaskStatus.running("id2"))).anyTimes();
    EasyMock.expect(taskStorage.getTask("id1")).andReturn(Optional.of(id1)).anyTimes();
    EasyMock.expect(taskStorage.getTask("id2")).andReturn(Optional.of(id2)).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            null
        )
    ).anyTimes();
    EasyMock.expect(taskClient.getStatusAsync("id1")).andReturn(Futures.immediateFuture(Status.PUBLISHING));
    EasyMock.expect(taskClient.getStatusAsync("id2")).andReturn(Futures.immediateFuture(Status.READING));
    EasyMock.expect(taskClient.getStartTimeAsync("id2")).andReturn(Futures.immediateFuture(startTime));
    EasyMock.expect(taskClient.getCurrentOffsetsAsync("id1", false))
            .andReturn(Futures.immediateFuture(ImmutableMap.of(0, 1L, 1, 2L, 2, 3L)));
    EasyMock.expect(taskClient.getEndOffsets("id1")).andReturn(ImmutableMap.of(0, 1L, 1, 2L, 2, 3L));
    EasyMock.expect(taskClient.getCurrentOffsetsAsync("id2", false))
            .andReturn(Futures.immediateFuture(ImmutableMap.of(0, 4L, 1, 5L, 2, 6L)));

    taskRunner.registerListener(EasyMock.anyObject(TaskRunnerListener.class), EasyMock.anyObject(Executor.class));

    // since id1 is publishing, so getCheckpoints wouldn't be called for it
    TreeMap<Integer, Map<Integer, Long>> checkpoints = new TreeMap<>();
    checkpoints.put(0, ImmutableMap.of(0, 1L, 1, 2L, 2, 3L));
    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("id2"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpoints))
            .times(1);

    replayAll();

    supervisor.start();
    supervisor.runInternal();
    supervisor.updateCurrentAndLatestOffsets();
    SupervisorReport<KafkaSupervisorReportPayload> report = supervisor.getStatus();
    verifyAll();

    Assert.assertEquals(DATASOURCE, report.getId());

    KafkaSupervisorReportPayload payload = report.getPayload();

    Assert.assertEquals(DATASOURCE, payload.getDataSource());
    Assert.assertEquals(3600L, payload.getDurationSeconds());
    Assert.assertEquals(NUM_PARTITIONS, payload.getPartitions());
    Assert.assertEquals(1, payload.getReplicas());
    Assert.assertEquals(topic, payload.getStream());
    Assert.assertEquals(1, payload.getActiveTasks().size());
    Assert.assertEquals(1, payload.getPublishingTasks().size());
    Assert.assertEquals(SupervisorStateManager.BasicState.RUNNING, payload.getDetailedState());
    Assert.assertEquals(0, payload.getRecentErrors().size());

    TaskReportData activeReport = payload.getActiveTasks().get(0);
    TaskReportData publishingReport = payload.getPublishingTasks().get(0);

    Assert.assertEquals("id2", activeReport.getId());
    Assert.assertEquals(startTime, activeReport.getStartTime());
    Assert.assertEquals(ImmutableMap.of(0, 1L, 1, 2L, 2, 3L), activeReport.getStartingOffsets());
    Assert.assertEquals(ImmutableMap.of(0, 4L, 1, 5L, 2, 6L), activeReport.getCurrentOffsets());
    Assert.assertEquals(ImmutableMap.of(0, 3L, 1, 2L, 2, 1L), activeReport.getLag());

    Assert.assertEquals("id1", publishingReport.getId());
    Assert.assertEquals(ImmutableMap.of(0, 0L, 1, 0L, 2, 0L), publishingReport.getStartingOffsets());
    Assert.assertEquals(ImmutableMap.of(0, 1L, 1, 2L, 2, 3L), publishingReport.getCurrentOffsets());
    Assert.assertNull(publishingReport.getLag());

    Assert.assertEquals(ImmutableMap.of(0, 7L, 1, 7L, 2, 7L), payload.getLatestOffsets());
    Assert.assertEquals(ImmutableMap.of(0, 3L, 1, 2L, 2, 1L), payload.getMinimumLag());
    Assert.assertEquals(6L, (long) payload.getAggregateLag());
    Assert.assertTrue(payload.getOffsetsLastUpdated().plusMinutes(1).isAfterNow());
  }

  @Test
  public void testKillUnresponsiveTasksWhileGettingStartTime() throws Exception
  {
    supervisor = getTestableSupervisor(2, 2, true, "PT1H", null, null);
    addSomeEvents(1);

    Capture<Task> captured = Capture.newInstance(CaptureType.ALL);
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(Collections.emptyList()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
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

    TreeMap<Integer, Map<Integer, Long>> checkpoints1 = new TreeMap<>();
    checkpoints1.put(0, ImmutableMap.of(0, 0L, 2, 0L));
    TreeMap<Integer, Map<Integer, Long>> checkpoints2 = new TreeMap<>();
    checkpoints2.put(0, ImmutableMap.of(1, 0L));
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

    supervisor = getTestableSupervisor(2, 2, true, "PT1M", null, null);
    addSomeEvents(100);

    Capture<Task> captured = Capture.newInstance(CaptureType.ALL);
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(Collections.emptyList()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
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

    TreeMap<Integer, Map<Integer, Long>> checkpoints1 = new TreeMap<>();
    checkpoints1.put(0, ImmutableMap.of(0, 0L, 2, 0L));
    TreeMap<Integer, Map<Integer, Long>> checkpoints2 = new TreeMap<>();
    checkpoints2.put(0, ImmutableMap.of(1, 0L));
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
      KafkaIndexTaskIOConfig taskConfig = ((KafkaIndexTask) task).getIOConfig();
      Assert.assertEquals(0L, (long) taskConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap().get(0));
      Assert.assertEquals(0L, (long) taskConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap().get(2));
    }
  }

  @Test
  public void testKillUnresponsiveTasksWhileSettingEndOffsets() throws Exception
  {
    final TaskLocation location = new TaskLocation("testHost", 1234, -1);

    supervisor = getTestableSupervisor(2, 2, true, "PT1M", null, null);
    addSomeEvents(100);

    Capture<Task> captured = Capture.newInstance(CaptureType.ALL);
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(Collections.emptyList()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
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

    TreeMap<Integer, Map<Integer, Long>> checkpoints1 = new TreeMap<>();
    checkpoints1.put(0, ImmutableMap.of(0, 0L, 2, 0L));
    TreeMap<Integer, Map<Integer, Long>> checkpoints2 = new TreeMap<>();
    checkpoints2.put(0, ImmutableMap.of(1, 0L));
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
            .andReturn(Futures.immediateFuture(Status.READING))
            .anyTimes();
    EasyMock.expect(taskClient.getStartTimeAsync(EasyMock.contains("sequenceName-0")))
            .andReturn(Futures.immediateFuture(DateTimes.nowUtc().minusMinutes(2)))
            .andReturn(Futures.immediateFuture(DateTimes.nowUtc()));
    EasyMock.expect(taskClient.getStartTimeAsync(EasyMock.contains("sequenceName-1")))
            .andReturn(Futures.immediateFuture(DateTimes.nowUtc()))
            .times(2);
    EasyMock.expect(taskClient.pauseAsync(EasyMock.contains("sequenceName-0")))
            .andReturn(Futures.immediateFuture(ImmutableMap.of(0, 10L, 1, 20L, 2, 30L)))
            .andReturn(Futures.immediateFuture(ImmutableMap.of(0, 10L, 1, 15L, 2, 35L)));
    EasyMock.expect(
        taskClient.setEndOffsetsAsync(
            EasyMock.contains("sequenceName-0"),
            EasyMock.eq(ImmutableMap.of(0, 10L, 1, 20L, 2, 35L)),
            EasyMock.eq(true)
        )
    ).andReturn(Futures.immediateFailedFuture(new RuntimeException())).times(2);
    taskQueue.shutdown(
        EasyMock.contains("sequenceName-0"),
        EasyMock.eq("Task [%s] failed to respond to [set end offsets] in a timely manner, killing task"),
        EasyMock.contains("sequenceName-0")
    );
    EasyMock.expectLastCall().times(2);
    EasyMock.expect(taskQueue.add(EasyMock.capture(captured))).andReturn(true).times(2);

    EasyMock.replay(taskStorage, taskRunner, taskClient, taskQueue);

    supervisor.runInternal();
    verifyAll();

    for (Task task : captured.getValues()) {
      KafkaIndexTaskIOConfig taskConfig = ((KafkaIndexTask) task).getIOConfig();
      Assert.assertEquals(0L, (long) taskConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap().get(0));
      Assert.assertEquals(0L, (long) taskConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap().get(2));
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
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    taskClient.close();
    taskRunner.unregisterListener(StringUtils.format("KafkaSupervisor-%s", DATASOURCE));
    replayAll();

    supervisor = getTestableSupervisor(1, 1, true, "PT1H", null, null);
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

    supervisor = getTestableSupervisor(2, 1, true, "PT1H", null, null);
    final KafkaSupervisorTuningConfig tuningConfig = supervisor.getTuningConfig();
    addSomeEvents(1);

    Task id1 = createKafkaIndexTask(
        "id1",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>("topic", ImmutableMap.of(0, 0L, 1, 0L, 2, 0L), ImmutableSet.of()),
        new SeekableStreamEndSequenceNumbers<>(
            "topic",
            ImmutableMap.of(0, Long.MAX_VALUE, 1, Long.MAX_VALUE, 2, Long.MAX_VALUE)
        ),
        null,
        null,
        tuningConfig
    );

    Task id2 = createKafkaIndexTask(
        "id2",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>("topic", ImmutableMap.of(0, 10L, 1, 20L, 2, 30L), ImmutableSet.of()),
        new SeekableStreamEndSequenceNumbers<>(
            "topic",
            ImmutableMap.of(0, Long.MAX_VALUE, 1, Long.MAX_VALUE, 2, Long.MAX_VALUE)
        ),
        null,
        null,
        tuningConfig
    );

    Task id3 = createKafkaIndexTask(
        "id3",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>("topic", ImmutableMap.of(0, 10L, 1, 20L, 2, 30L), ImmutableSet.of()),
        new SeekableStreamEndSequenceNumbers<>(
            "topic",
            ImmutableMap.of(0, Long.MAX_VALUE, 1, Long.MAX_VALUE, 2, Long.MAX_VALUE)
        ),
        null,
        null,
        tuningConfig
    );

    Collection workItems = new ArrayList<>();
    workItems.add(new TestTaskRunnerWorkItem(id1, null, location1));
    workItems.add(new TestTaskRunnerWorkItem(id2, null, location2));

    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(workItems).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE))
            .andReturn(ImmutableList.of(id1, id2, id3))
            .anyTimes();
    EasyMock.expect(taskStorage.getStatus("id1")).andReturn(Optional.of(TaskStatus.running("id1"))).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id2")).andReturn(Optional.of(TaskStatus.running("id2"))).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id3")).andReturn(Optional.of(TaskStatus.running("id3"))).anyTimes();
    EasyMock.expect(taskStorage.getTask("id1")).andReturn(Optional.of(id1)).anyTimes();
    EasyMock.expect(taskStorage.getTask("id2")).andReturn(Optional.of(id2)).anyTimes();
    EasyMock.expect(taskStorage.getTask("id3")).andReturn(Optional.of(id3)).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            null
        )
    ).anyTimes();
    EasyMock.expect(taskClient.getStatusAsync("id1")).andReturn(Futures.immediateFuture(Status.PUBLISHING));
    EasyMock.expect(taskClient.getStatusAsync("id2")).andReturn(Futures.immediateFuture(Status.READING));
    EasyMock.expect(taskClient.getStatusAsync("id3")).andReturn(Futures.immediateFuture(Status.READING));
    EasyMock.expect(taskClient.getStartTimeAsync("id2")).andReturn(Futures.immediateFuture(startTime));
    EasyMock.expect(taskClient.getStartTimeAsync("id3")).andReturn(Futures.immediateFuture(startTime));
    EasyMock.expect(taskClient.getEndOffsets("id1")).andReturn(ImmutableMap.of(0, 10L, 1, 20L, 2, 30L));

    // getCheckpoints will not be called for id1 as it is in publishing state
    TreeMap<Integer, Map<Integer, Long>> checkpoints = new TreeMap<>();
    checkpoints.put(0, ImmutableMap.of(0, 10L, 1, 20L, 2, 30L));
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
            .andReturn(Futures.immediateFuture(ImmutableMap.of(0, 15L, 1, 25L, 2, 30L)));
    EasyMock.expect(taskClient.setEndOffsetsAsync("id2", ImmutableMap.of(0, 15L, 1, 25L, 2, 30L), true))
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

    KafkaDataSourceMetadata kafkaDataSourceMetadata = new KafkaDataSourceMetadata(
        new SeekableStreamStartSequenceNumbers<>(
            topic,
            ImmutableMap.of(0, 1000L, 1, 1000L, 2, 1000L),
            ImmutableSet.of()
        )
    );

    KafkaDataSourceMetadata resetMetadata = new KafkaDataSourceMetadata(
        new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(1, 1000L, 2, 1000L), ImmutableSet.of())
    );

    KafkaDataSourceMetadata expectedMetadata = new KafkaDataSourceMetadata(
        new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(0, 1000L), ImmutableSet.of()));

    EasyMock.reset(indexerMetadataStorageCoordinator);
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE))
            .andReturn(kafkaDataSourceMetadata);
    EasyMock.expect(indexerMetadataStorageCoordinator.resetDataSourceMetadata(
        EasyMock.capture(captureDataSource),
        EasyMock.capture(captureDataSourceMetadata)
    )).andReturn(true);
    EasyMock.replay(indexerMetadataStorageCoordinator);

    try {
      supervisor.resetInternal(resetMetadata);
    }
    catch (NullPointerException npe) {
      // Expected as there will be an attempt to EasyMock.reset partitionGroups offsets to NOT_SET
      // however there would be no entries in the map as we have not put nay data in kafka
      Assert.assertNull(npe.getCause());
    }
    verifyAll();

    Assert.assertEquals(DATASOURCE, captureDataSource.getValue());
    Assert.assertEquals(expectedMetadata, captureDataSourceMetadata.getValue());
  }

  @Test
  public void testResetNoDataSourceMetadata()
  {
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

    KafkaDataSourceMetadata resetMetadata = new KafkaDataSourceMetadata(
        new SeekableStreamStartSequenceNumbers<>(
            topic,
            ImmutableMap.of(1, 1000L, 2, 1000L),
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
    addSomeEvents(2);
    supervisor = getTestableSupervisor(1, 1, true, true, "PT1H", null, null);
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(Collections.emptyList()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    taskRunner.registerListener(EasyMock.anyObject(TaskRunnerListener.class), EasyMock.anyObject(Executor.class));

    EasyMock.reset(indexerMetadataStorageCoordinator);
    // unknown DataSourceMetadata in metadata store
    // for simplicity in testing the offset availability check, we use negative stored offsets in metadata here,
    // because the stream's earliest offset is 0, although that would not happen in real usage.
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE))
            .andReturn(
                new KafkaDataSourceMetadata(
                    new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(1, -100L, 2, 200L))
                )
            ).times(4);
    // getOffsetFromStorageForPartition() throws an exception when the offsets are automatically reset.
    // Since getOffsetFromStorageForPartition() is called per partition, all partitions can't be reset at the same time.
    // Instead, subsequent partitions will be reset in the following supervisor runs.
    EasyMock.expect(
        indexerMetadataStorageCoordinator.resetDataSourceMetadata(
            DATASOURCE,
            new KafkaDataSourceMetadata(
                // Only one partition is reset in a single supervisor run.
                new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(2, 200L))
            )
        )
    ).andReturn(true);
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();
  }

  @Test
  public void testResetRunningTasks() throws Exception
  {
    final TaskLocation location1 = new TaskLocation("testHost", 1234, -1);
    final TaskLocation location2 = new TaskLocation("testHost2", 145, -1);
    final DateTime startTime = DateTimes.nowUtc();

    supervisor = getTestableSupervisor(2, 1, true, "PT1H", null, null);
    final KafkaSupervisorTuningConfig tuningConfig = supervisor.getTuningConfig();
    addSomeEvents(1);

    Task id1 = createKafkaIndexTask(
        "id1",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>("topic", ImmutableMap.of(0, 0L, 1, 0L, 2, 0L), ImmutableSet.of()),
        new SeekableStreamEndSequenceNumbers<>(
            "topic",
            ImmutableMap.of(0, Long.MAX_VALUE, 1, Long.MAX_VALUE, 2, Long.MAX_VALUE)
        ),
        null,
        null,
        tuningConfig
    );

    Task id2 = createKafkaIndexTask(
        "id2",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>("topic", ImmutableMap.of(0, 10L, 1, 20L, 2, 30L), ImmutableSet.of()),
        new SeekableStreamEndSequenceNumbers<>(
            "topic",
            ImmutableMap.of(0, Long.MAX_VALUE, 1, Long.MAX_VALUE, 2, Long.MAX_VALUE)
        ),
        null,
        null,
        tuningConfig
    );

    Task id3 = createKafkaIndexTask(
        "id3",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>("topic", ImmutableMap.of(0, 10L, 1, 20L, 2, 30L), ImmutableSet.of()),
        new SeekableStreamEndSequenceNumbers<>(
            "topic",
            ImmutableMap.of(0, Long.MAX_VALUE, 1, Long.MAX_VALUE, 2, Long.MAX_VALUE)
        ),
        null,
        null,
        tuningConfig
    );

    Collection workItems = new ArrayList<>();
    workItems.add(new TestTaskRunnerWorkItem(id1, null, location1));
    workItems.add(new TestTaskRunnerWorkItem(id2, null, location2));

    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(workItems).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE))
            .andReturn(ImmutableList.of(id1, id2, id3))
            .anyTimes();
    EasyMock.expect(taskStorage.getStatus("id1")).andReturn(Optional.of(TaskStatus.running("id1"))).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id2")).andReturn(Optional.of(TaskStatus.running("id2"))).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id3")).andReturn(Optional.of(TaskStatus.running("id3"))).anyTimes();
    EasyMock.expect(taskStorage.getTask("id1")).andReturn(Optional.of(id1)).anyTimes();
    EasyMock.expect(taskStorage.getTask("id2")).andReturn(Optional.of(id2)).anyTimes();
    EasyMock.expect(taskStorage.getTask("id3")).andReturn(Optional.of(id3)).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            null
        )
    ).anyTimes();
    EasyMock.expect(taskClient.getStatusAsync("id1")).andReturn(Futures.immediateFuture(Status.PUBLISHING));
    EasyMock.expect(taskClient.getStatusAsync("id2")).andReturn(Futures.immediateFuture(Status.READING));
    EasyMock.expect(taskClient.getStatusAsync("id3")).andReturn(Futures.immediateFuture(Status.READING));
    EasyMock.expect(taskClient.getStartTimeAsync("id2")).andReturn(Futures.immediateFuture(startTime));
    EasyMock.expect(taskClient.getStartTimeAsync("id3")).andReturn(Futures.immediateFuture(startTime));
    EasyMock.expect(taskClient.getEndOffsets("id1")).andReturn(ImmutableMap.of(0, 10L, 1, 20L, 2, 30L));

    TreeMap<Integer, Map<Integer, Long>> checkpoints = new TreeMap<>();
    checkpoints.put(0, ImmutableMap.of(0, 10L, 1, 20L, 2, 30L));
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
  public void testNoDataIngestionTasks()
  {
    final DateTime startTime = DateTimes.nowUtc();
    supervisor = getTestableSupervisor(2, 1, true, "PT1S", null, null);
    final KafkaSupervisorTuningConfig tuningConfig = supervisor.getTuningConfig();
    supervisor.getStateManager().markRunFinished();

    //not adding any events
    Task id1 = createKafkaIndexTask(
        "id1",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>("topic", ImmutableMap.of(0, 0L, 1, 0L, 2, 0L), ImmutableSet.of()),
        new SeekableStreamEndSequenceNumbers<>(
            "topic",
            ImmutableMap.of(0, Long.MAX_VALUE, 1, Long.MAX_VALUE, 2, Long.MAX_VALUE)
        ),
        null,
        null,
        tuningConfig
    );

    Task id2 = createKafkaIndexTask(
        "id2",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>("topic", ImmutableMap.of(0, 10L, 1, 20L, 2, 30L), ImmutableSet.of()),
        new SeekableStreamEndSequenceNumbers<>(
            "topic",
            ImmutableMap.of(0, Long.MAX_VALUE, 1, Long.MAX_VALUE, 2, Long.MAX_VALUE)
        ),
        null,
        null,
        tuningConfig
    );

    Task id3 = createKafkaIndexTask(
        "id3",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>("topic", ImmutableMap.of(0, 10L, 1, 20L, 2, 30L), ImmutableSet.of()),
        new SeekableStreamEndSequenceNumbers<>(
            "topic",
            ImmutableMap.of(0, Long.MAX_VALUE, 1, Long.MAX_VALUE, 2, Long.MAX_VALUE)
        ),
        null,
        null,
        tuningConfig
    );

    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE))
            .andReturn(ImmutableList.of(id1, id2, id3))
            .anyTimes();
    EasyMock.expect(taskStorage.getStatus("id1")).andReturn(Optional.of(TaskStatus.running("id1"))).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id2")).andReturn(Optional.of(TaskStatus.running("id2"))).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id3")).andReturn(Optional.of(TaskStatus.running("id3"))).anyTimes();
    EasyMock.expect(taskStorage.getTask("id1")).andReturn(Optional.of(id1)).anyTimes();
    EasyMock.expect(taskStorage.getTask("id2")).andReturn(Optional.of(id2)).anyTimes();
    EasyMock.expect(taskStorage.getTask("id3")).andReturn(Optional.of(id3)).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            null
        )
    ).anyTimes();
    EasyMock.expect(taskClient.getStatusAsync("id1")).andReturn(Futures.immediateFuture(Status.READING));
    EasyMock.expect(taskClient.getStatusAsync("id2")).andReturn(Futures.immediateFuture(Status.READING));
    EasyMock.expect(taskClient.getStatusAsync("id3")).andReturn(Futures.immediateFuture(Status.READING));
    EasyMock.expect(taskClient.getStartTimeAsync("id1")).andReturn(Futures.immediateFuture(startTime));
    EasyMock.expect(taskClient.getStartTimeAsync("id2")).andReturn(Futures.immediateFuture(startTime));
    EasyMock.expect(taskClient.getStartTimeAsync("id3")).andReturn(Futures.immediateFuture(startTime));

    TreeMap<Integer, Map<Integer, Long>> checkpoints = new TreeMap<>();
    checkpoints.put(0, ImmutableMap.of(0, 10L, 1, 20L, 2, 30L));
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
      throws InterruptedException
  {
    supervisor = getTestableSupervisor(2, 1, true, "PT1S", null, null);
    final KafkaSupervisorTuningConfig tuningConfig = supervisor.getTuningConfig();
    supervisor.getStateManager().markRunFinished();

    //not adding any events
    final KafkaIndexTask id1 = createKafkaIndexTask(
        "id1",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(0, 0L, 1, 0L, 2, 0L), ImmutableSet.of()),
        new SeekableStreamEndSequenceNumbers<>(
            topic,
            ImmutableMap.of(0, Long.MAX_VALUE, 1, Long.MAX_VALUE, 2, Long.MAX_VALUE)
        ),
        null,
        null,
        tuningConfig
    );

    final Task id2 = createKafkaIndexTask(
        "id2",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(0, 10L, 1, 20L, 2, 30L), ImmutableSet.of()),
        new SeekableStreamEndSequenceNumbers<>(
            topic,
            ImmutableMap.of(0, Long.MAX_VALUE, 1, Long.MAX_VALUE, 2, Long.MAX_VALUE)
        ),
        null,
        null,
        tuningConfig
    );

    final Task id3 = createKafkaIndexTask(
        "id3",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(0, 10L, 1, 20L, 2, 30L), ImmutableSet.of()),
        new SeekableStreamEndSequenceNumbers<>(
            topic,
            ImmutableMap.of(0, Long.MAX_VALUE, 1, Long.MAX_VALUE, 2, Long.MAX_VALUE)
        ),
        null,
        null,
        tuningConfig
    );

    final TaskLocation location1 = new TaskLocation("testHost", 1234, -1);
    final TaskLocation location2 = new TaskLocation("testHost2", 145, -1);
    Collection workItems = new ArrayList<>();
    workItems.add(new TestTaskRunnerWorkItem(id1, null, location1));
    workItems.add(new TestTaskRunnerWorkItem(id2, null, location2));
    workItems.add(new TestTaskRunnerWorkItem(id2, null, location2));

    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(workItems).anyTimes();
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE))
            .andReturn(ImmutableList.of(id1, id2, id3))
            .anyTimes();
    EasyMock.expect(taskStorage.getStatus("id1")).andReturn(Optional.of(TaskStatus.running("id1"))).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id2")).andReturn(Optional.of(TaskStatus.running("id2"))).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id3")).andReturn(Optional.of(TaskStatus.running("id3"))).anyTimes();
    EasyMock.expect(taskStorage.getTask("id1")).andReturn(Optional.of(id1)).anyTimes();
    EasyMock.expect(taskStorage.getTask("id2")).andReturn(Optional.of(id2)).anyTimes();
    EasyMock.expect(taskStorage.getTask("id3")).andReturn(Optional.of(id3)).anyTimes();
    EasyMock.expect(
        indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(new KafkaDataSourceMetadata(null)
    ).anyTimes();
    EasyMock.expect(taskClient.getStatusAsync("id1")).andReturn(Futures.immediateFuture(Status.READING));
    EasyMock.expect(taskClient.getStatusAsync("id2")).andReturn(Futures.immediateFuture(Status.READING));
    EasyMock.expect(taskClient.getStatusAsync("id3")).andReturn(Futures.immediateFuture(Status.READING));

    final DateTime startTime = DateTimes.nowUtc();
    EasyMock.expect(taskClient.getStartTimeAsync("id1")).andReturn(Futures.immediateFuture(startTime));
    EasyMock.expect(taskClient.getStartTimeAsync("id2")).andReturn(Futures.immediateFuture(startTime));
    EasyMock.expect(taskClient.getStartTimeAsync("id3")).andReturn(Futures.immediateFuture(startTime));

    final TreeMap<Integer, Map<Integer, Long>> checkpoints = new TreeMap<>();
    checkpoints.put(0, ImmutableMap.of(0, 10L, 1, 20L, 2, 30L));
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
        new KafkaDataSourceMetadata(
            new SeekableStreamStartSequenceNumbers<>(topic, checkpoints.get(0), ImmutableSet.of())
        )
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
    supervisor = getTestableSupervisor(2, 1, true, "PT1S", null, null);
    final KafkaSupervisorTuningConfig tuningConfig = supervisor.getTuningConfig();
    //not adding any events
    final KafkaIndexTask id1 = createKafkaIndexTask(
        "id1",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(0, 0L, 1, 0L, 2, 0L), ImmutableSet.of()),
        new SeekableStreamEndSequenceNumbers<>(
            topic,
            ImmutableMap.of(0, Long.MAX_VALUE, 1, Long.MAX_VALUE, 2, Long.MAX_VALUE)
        ),
        null,
        null,
        tuningConfig
    );

    final Task id2 = createKafkaIndexTask(
        "id2",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(0, 10L, 1, 20L, 2, 30L), ImmutableSet.of()),
        new SeekableStreamEndSequenceNumbers<>(
            topic,
            ImmutableMap.of(0, Long.MAX_VALUE, 1, Long.MAX_VALUE, 2, Long.MAX_VALUE)
        ),
        null,
        null,
        tuningConfig
    );

    final Task id3 = createKafkaIndexTask(
        "id3",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(0, 10L, 1, 20L, 2, 30L), ImmutableSet.of()),
        new SeekableStreamEndSequenceNumbers<>(
            topic,
            ImmutableMap.of(0, Long.MAX_VALUE, 1, Long.MAX_VALUE, 2, Long.MAX_VALUE)
        ),
        null,
        null,
        tuningConfig
    );

    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE))
            .andReturn(ImmutableList.of(id1, id2, id3))
            .anyTimes();
    EasyMock.expect(taskStorage.getStatus("id1")).andReturn(Optional.of(TaskStatus.running("id1"))).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id2")).andReturn(Optional.of(TaskStatus.running("id2"))).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id3")).andReturn(Optional.of(TaskStatus.running("id3"))).anyTimes();
    EasyMock.expect(taskStorage.getTask("id1")).andReturn(Optional.of(id1)).anyTimes();
    EasyMock.expect(taskStorage.getTask("id2")).andReturn(Optional.of(id2)).anyTimes();
    EasyMock.expect(taskStorage.getTask("id3")).andReturn(Optional.of(id3)).anyTimes();
    EasyMock.expect(
        indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(new KafkaDataSourceMetadata(null)
    ).anyTimes();

    replayAll();

    supervisor.start();

    supervisor.checkpoint(
        0,
        new KafkaDataSourceMetadata(
            new SeekableStreamStartSequenceNumbers<>(topic, Collections.emptyMap(), ImmutableSet.of())
        )
    );

    while (supervisor.getNoticesQueueSize() > 0) {
      Thread.sleep(100);
    }

    verifyAll();

    while (serviceEmitter.getStackTrace() == null) {
      Thread.sleep(100);
    }

    Assert.assertTrue(
        serviceEmitter.getStackTrace().startsWith("org.apache.druid.java.util.common.ISE: WTH?! cannot find")
    );
    Assert.assertEquals(
        "WTH?! cannot find taskGroup [0] among all activelyReadingTaskGroups [{}]",
        serviceEmitter.getExceptionMessage()
    );
    Assert.assertEquals(ISE.class, serviceEmitter.getExceptionClass());
  }

  @Test
  public void testSuspendedNoRunningTasks() throws Exception
  {
    supervisor = getTestableSupervisor(1, 1, true, "PT1H", null, null, true, kafkaHost);
    addSomeEvents(1);

    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
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

    supervisor = getTestableSupervisor(2, 1, true, "PT1H", null, null, true, kafkaHost);
    final KafkaSupervisorTuningConfig tuningConfig = supervisor.getTuningConfig();
    addSomeEvents(1);

    Task id1 = createKafkaIndexTask(
        "id1",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>("topic", ImmutableMap.of(0, 0L, 1, 0L, 2, 0L), ImmutableSet.of()),
        new SeekableStreamEndSequenceNumbers<>(
            "topic",
            ImmutableMap.of(0, Long.MAX_VALUE, 1, Long.MAX_VALUE, 2, Long.MAX_VALUE)
        ),
        null,
        null,
        tuningConfig
    );

    Task id2 = createKafkaIndexTask(
        "id2",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>("topic", ImmutableMap.of(0, 10L, 1, 20L, 2, 30L), ImmutableSet.of()),
        new SeekableStreamEndSequenceNumbers<>(
            "topic",
            ImmutableMap.of(0, Long.MAX_VALUE, 1, Long.MAX_VALUE, 2, Long.MAX_VALUE)
        ),
        null,
        null,
        tuningConfig
    );

    Task id3 = createKafkaIndexTask(
        "id3",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>("topic", ImmutableMap.of(0, 10L, 1, 20L, 2, 30L), ImmutableSet.of()),
        new SeekableStreamEndSequenceNumbers<>(
            "topic",
            ImmutableMap.of(0, Long.MAX_VALUE, 1, Long.MAX_VALUE, 2, Long.MAX_VALUE)
        ),
        null,
        null,
        tuningConfig
    );

    Collection workItems = new ArrayList<>();
    workItems.add(new TestTaskRunnerWorkItem(id1, null, location1));
    workItems.add(new TestTaskRunnerWorkItem(id2, null, location2));

    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(workItems).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE))
            .andReturn(ImmutableList.of(id1, id2, id3))
            .anyTimes();
    EasyMock.expect(taskStorage.getStatus("id1")).andReturn(Optional.of(TaskStatus.running("id1"))).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id2")).andReturn(Optional.of(TaskStatus.running("id2"))).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id3")).andReturn(Optional.of(TaskStatus.running("id3"))).anyTimes();
    EasyMock.expect(taskStorage.getTask("id1")).andReturn(Optional.of(id1)).anyTimes();
    EasyMock.expect(taskStorage.getTask("id2")).andReturn(Optional.of(id2)).anyTimes();
    EasyMock.expect(taskStorage.getTask("id3")).andReturn(Optional.of(id3)).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
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
    EasyMock.expect(taskClient.getEndOffsets("id1")).andReturn(ImmutableMap.of(0, 10L, 1, 20L, 2, 30L));

    // getCheckpoints will not be called for id1 as it is in publishing state
    TreeMap<Integer, Map<Integer, Long>> checkpoints = new TreeMap<>();
    checkpoints.put(0, ImmutableMap.of(0, 10L, 1, 20L, 2, 30L));
    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("id2"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpoints))
            .times(1);
    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("id3"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpoints))
            .times(1);

    taskRunner.registerListener(EasyMock.anyObject(TaskRunnerListener.class), EasyMock.anyObject(Executor.class));

    EasyMock.expect(taskClient.pauseAsync("id2"))
            .andReturn(Futures.immediateFuture(ImmutableMap.of(0, 15L, 1, 25L, 2, 30L)));
    EasyMock.expect(taskClient.setEndOffsetsAsync("id2", ImmutableMap.of(0, 15L, 1, 25L, 2, 30L), true))
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
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(Collections.emptyList()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    taskRunner.registerListener(EasyMock.anyObject(TaskRunnerListener.class), EasyMock.anyObject(Executor.class));
    replayAll();

    supervisor = getTestableSupervisor(1, 1, true, "PT1H", null, null, true, kafkaHost);
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
  public void testFailedInitializationAndRecovery() throws Exception
  {
    // Block the supervisor initialization with a bad hostname config, make sure this doesn't block the lifecycle
    supervisor = getTestableSupervisor(
        1,
        1,
        true,
        "PT1H",
        null,
        null,
        false,
        StringUtils.format("badhostname:%d", kafkaServer.getPort())
    );
    final KafkaSupervisorTuningConfig tuningConfig = supervisor.getTuningConfig();
    addSomeEvents(1);

    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
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
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            null
        )
    ).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.capture(captured))).andReturn(true);
    taskRunner.registerListener(EasyMock.anyObject(TaskRunnerListener.class), EasyMock.anyObject(Executor.class));
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

    Assert.assertEquals(topic, taskConfig.getStartSequenceNumbers().getStream());
    Assert.assertEquals(
        0L,
        taskConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap().get(0).longValue()
    );
    Assert.assertEquals(
        0L,
        taskConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap().get(1).longValue()
    );
    Assert.assertEquals(
        0L,
        taskConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap().get(2).longValue()
    );

    Assert.assertEquals(topic, taskConfig.getEndSequenceNumbers().getStream());
    Assert.assertEquals(
        Long.MAX_VALUE,
        taskConfig.getEndSequenceNumbers().getPartitionSequenceNumberMap().get(0).longValue()
    );
    Assert.assertEquals(
        Long.MAX_VALUE,
        taskConfig.getEndSequenceNumbers().getPartitionSequenceNumberMap().get(1).longValue()
    );
    Assert.assertEquals(
        Long.MAX_VALUE,
        taskConfig.getEndSequenceNumbers().getPartitionSequenceNumberMap().get(2).longValue()
    );
  }

  @Test
  public void testGetCurrentTotalStats()
  {
    supervisor = getTestableSupervisor(1, 2, true, "PT1H", null, null, false, kafkaHost);
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
      throws Exception
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
        true
    );

    addSomeEvents(1);

    Task task = createKafkaIndexTask(
        "id1",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>(
            "topic",
            ImmutableMap.of(0, 0L, 2, 0L),
            ImmutableSet.of()
        ),
        new SeekableStreamEndSequenceNumbers<>(
            "topic",
            ImmutableMap.of(0, Long.MAX_VALUE, 2, Long.MAX_VALUE)
        ),
        null,
        null,
        supervisor.getTuningConfig()
    );

    List<Task> existingTasks = ImmutableList.of(task);

    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(Collections.emptyList()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(existingTasks).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id1")).andReturn(Optional.of(TaskStatus.running("id1"))).anyTimes();
    EasyMock.expect(taskStorage.getTask("id1")).andReturn(Optional.of(task)).anyTimes();
    EasyMock.expect(taskClient.getStatusAsync(EasyMock.anyString()))
            .andReturn(Futures.immediateFuture(Status.NOT_STARTED))
            .anyTimes();
    EasyMock.expect(taskClient.getStartTimeAsync(EasyMock.anyString()))
            .andReturn(Futures.immediateFuture(DateTimes.nowUtc()))
            .anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            null
        )
    ).anyTimes();

    taskRunner.registerListener(EasyMock.anyObject(TaskRunnerListener.class), EasyMock.anyObject(Executor.class));
    EasyMock.expect(taskQueue.add(EasyMock.anyObject(Task.class))).andReturn(true);

    TreeMap<Integer, Map<Integer, Long>> checkpoints1 = new TreeMap<>();
    checkpoints1.put(0, ImmutableMap.of(0, 0L, 2, 0L));

    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("id1"), EasyMock.anyBoolean()))
            .andReturn(Futures.immediateFuture(checkpoints1))
            .times(numReplicas);

    replayAll();
    supervisor.start();
    supervisor.runInternal();
    verifyAll();
  }

  @Test
  public void testKillIncompatibleTasks()
      throws Exception
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
        false
    );

    addSomeEvents(1);

    Task task = createKafkaIndexTask(
        "id1",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>(
            "topic",
            ImmutableMap.of(0, 0L, 2, 0L),
            ImmutableSet.of()
        ),
        new SeekableStreamEndSequenceNumbers<>("topic", ImmutableMap.of(0, Long.MAX_VALUE, 2, Long.MAX_VALUE)),
        null,
        null,
        supervisor.getTuningConfig()
    );

    List<Task> existingTasks = ImmutableList.of(task);
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(Collections.emptyList()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(existingTasks).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id1")).andReturn(Optional.of(TaskStatus.running("id1"))).anyTimes();
    EasyMock.expect(taskStorage.getTask("id1")).andReturn(Optional.of(task)).anyTimes();
    EasyMock.expect(taskClient.getStatusAsync(EasyMock.anyString()))
            .andReturn(Futures.immediateFuture(Status.NOT_STARTED))
            .anyTimes();
    EasyMock.expect(taskClient.getStartTimeAsync(EasyMock.anyString()))
            .andReturn(Futures.immediateFuture(DateTimes.nowUtc()))
            .anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
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

    KafkaSupervisor supervisor = getSupervisor(
        2,
        1,
        true,
        "PT1H",
        new Period("P1D"),
        new Period("P1D"),
        false,
        kafkaHost,
        dataSchema,
        new KafkaSupervisorTuningConfig(
            1000,
            null,
            50000,
            null,
            new Period("P1Y"),
            new File("/test"),
            null,
            null,
            null,
            true,
            false,
            null,
            false,
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
        )
    );

    supervisor.addTaskGroupToActivelyReadingTaskGroup(
        42,
        ImmutableMap.of(0, 0L, 2, 0L),
        Optional.of(minMessageTime),
        Optional.of(maxMessageTime),
        ImmutableSet.of("id1", "id2", "id3", "id4"),
        ImmutableSet.of()
    );

    DataSchema modifiedDataSchema = getDataSchema("some other datasource");

    KafkaSupervisorTuningConfig modifiedTuningConfig = new KafkaSupervisorTuningConfig(
        42, // This is different
        null,
        50000,
        null,
        new Period("P1Y"),
        new File("/test"),
        null,
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

    KafkaIndexTask taskFromStorage = createKafkaIndexTask(
        "id1",
        0,
        new SeekableStreamStartSequenceNumbers<>(
            "topic",
            ImmutableMap.of(0, 0L, 2, 0L),
            ImmutableSet.of()
        ),
        new SeekableStreamEndSequenceNumbers<>(
            "topic",
            ImmutableMap.of(0, Long.MAX_VALUE, 2, Long.MAX_VALUE)
        ),
        minMessageTime,
        maxMessageTime,
        dataSchema,
        supervisor.getTuningConfig()
    );

    KafkaIndexTask taskFromStorageMismatchedDataSchema = createKafkaIndexTask(
        "id2",
        0,
        new SeekableStreamStartSequenceNumbers<>(
            "topic",
            ImmutableMap.of(0, 0L, 2, 0L),
            ImmutableSet.of()
        ),
        new SeekableStreamEndSequenceNumbers<>(
            "topic",
            ImmutableMap.of(0, Long.MAX_VALUE, 2, Long.MAX_VALUE)
        ),
        minMessageTime,
        maxMessageTime,
        modifiedDataSchema,
        supervisor.getTuningConfig()
    );

    KafkaIndexTask taskFromStorageMismatchedTuningConfig = createKafkaIndexTask(
        "id3",
        0,
        new SeekableStreamStartSequenceNumbers<>(
            "topic",
            ImmutableMap.of(0, 0L, 2, 0L),
            ImmutableSet.of()
        ),
        new SeekableStreamEndSequenceNumbers<>(
            "topic",
            ImmutableMap.of(0, Long.MAX_VALUE, 2, Long.MAX_VALUE)
        ),
        minMessageTime,
        maxMessageTime,
        dataSchema,
        modifiedTuningConfig
    );

    KafkaIndexTask taskFromStorageMismatchedPartitionsWithTaskGroup = createKafkaIndexTask(
        "id4",
        0,
        new SeekableStreamStartSequenceNumbers<>(
            "topic",
            ImmutableMap.of(0, 0L, 2, 6L),
            ImmutableSet.of()
        ),
        new SeekableStreamEndSequenceNumbers<>(
            "topic",
            ImmutableMap.of(0, Long.MAX_VALUE, 2, Long.MAX_VALUE)
        ),
        minMessageTime,
        maxMessageTime,
        dataSchema,
        supervisor.getTuningConfig()
    );

    EasyMock.expect(taskStorage.getTask("id1"))
            .andReturn(Optional.of(taskFromStorage))
            .once();
    EasyMock.expect(taskStorage.getTask("id2"))
            .andReturn(Optional.of(taskFromStorageMismatchedDataSchema))
            .once();
    EasyMock.expect(taskStorage.getTask("id3"))
            .andReturn(Optional.of(taskFromStorageMismatchedTuningConfig))
            .once();
    EasyMock.expect(taskStorage.getTask("id4"))
            .andReturn(Optional.of(taskFromStorageMismatchedPartitionsWithTaskGroup))
            .once();

    replayAll();

    Assert.assertTrue(supervisor.isTaskCurrent(42, "id1"));
    Assert.assertFalse(supervisor.isTaskCurrent(42, "id2"));
    Assert.assertFalse(supervisor.isTaskCurrent(42, "id3"));
    Assert.assertFalse(supervisor.isTaskCurrent(42, "id4"));
    verifyAll();
  }

  private void addSomeEvents(int numEventsPerPartition) throws Exception
  {
    // create topic manually
    try (Admin admin = kafkaServer.newAdminClient()) {
      admin.createTopics(
          Collections.singletonList(new NewTopic(topic, NUM_PARTITIONS, (short) 1))
      ).all().get();
    }

    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      kafkaProducer.initTransactions();
      kafkaProducer.beginTransaction();
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
      kafkaProducer.commitTransaction();
    }
  }

  private void addMoreEvents(int numEventsPerPartition, int num_partitions) throws Exception
  {
    try (Admin admin = kafkaServer.newAdminClient()) {
      admin.createPartitions(Collections.singletonMap(topic, NewPartitions.increaseTo(num_partitions))).all().get();
    }

    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      kafkaProducer.initTransactions();
      kafkaProducer.beginTransaction();
      for (int i = NUM_PARTITIONS; i < num_partitions; i++) {
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
      kafkaProducer.commitTransaction();
    }
  }


  private TestableKafkaSupervisor getTestableSupervisor(
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
        false,
        duration,
        lateMessageRejectionPeriod,
        earlyMessageRejectionPeriod
    );
  }

  private TestableKafkaSupervisor getTestableSupervisor(
      int replicas,
      int taskCount,
      boolean useEarliestOffset,
      boolean resetOffsetAutomatically,
      String duration,
      Period lateMessageRejectionPeriod,
      Period earlyMessageRejectionPeriod
  )
  {
    return getTestableSupervisor(
        replicas,
        taskCount,
        useEarliestOffset,
        resetOffsetAutomatically,
        duration,
        lateMessageRejectionPeriod,
        earlyMessageRejectionPeriod,
        false,
        kafkaHost
    );
  }

  private TestableKafkaSupervisor getTestableSupervisor(
      int replicas,
      int taskCount,
      boolean useEarliestOffset,
      String duration,
      Period lateMessageRejectionPeriod,
      Period earlyMessageRejectionPeriod,
      boolean suspended,
      String kafkaHost
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
        suspended,
        kafkaHost
    );
  }

  private TestableKafkaSupervisor getTestableSupervisor(
      int replicas,
      int taskCount,
      boolean useEarliestOffset,
      boolean resetOffsetAutomatically,
      String duration,
      Period lateMessageRejectionPeriod,
      Period earlyMessageRejectionPeriod,
      boolean suspended,
      String kafkaHost
  )
  {
    final Map<String, Object> consumerProperties = KafkaConsumerConfigs.getConsumerProperties();
    consumerProperties.put("myCustomKey", "myCustomValue");
    consumerProperties.put("bootstrap.servers", kafkaHost);
    KafkaSupervisorIOConfig kafkaSupervisorIOConfig = new KafkaSupervisorIOConfig(
        topic,
        INPUT_FORMAT,
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
        null
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

    final KafkaSupervisorTuningConfig tuningConfig = new KafkaSupervisorTuningConfig(
        1000,
        null,
        50000,
        null,
        new Period("P1Y"),
        new File("/test"),
        null,
        null,
        null,
        true,
        false,
        null,
        resetOffsetAutomatically,
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

    return new TestableKafkaSupervisor(
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        taskClientFactory,
        OBJECT_MAPPER,
        new KafkaSupervisorSpec(
            null,
            dataSchema,
            tuningConfig,
            kafkaSupervisorIOConfig,
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
            new SupervisorStateManagerConfig()
        ),
        rowIngestionMetersFactory
    );
  }

  /**
   * Use when you want to mock the return value of SeekableStreamSupervisor#isTaskCurrent()
   */
  private TestableKafkaSupervisor getTestableSupervisorCustomIsTaskCurrent(
      int replicas,
      int taskCount,
      boolean useEarliestOffset,
      String duration,
      Period lateMessageRejectionPeriod,
      Period earlyMessageRejectionPeriod,
      boolean suspended,
      boolean isTaskCurrentReturn
  )
  {
    Map<String, Object> consumerProperties = new HashMap<>();
    consumerProperties.put("myCustomKey", "myCustomValue");
    consumerProperties.put("bootstrap.servers", kafkaHost);
    consumerProperties.put("isolation.level", "read_committed");
    KafkaSupervisorIOConfig kafkaSupervisorIOConfig = new KafkaSupervisorIOConfig(
        topic,
        INPUT_FORMAT,
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
        null
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

    final KafkaSupervisorTuningConfig tuningConfig = new KafkaSupervisorTuningConfig(
        1000,
        null,
        50000,
        null,
        new Period("P1Y"),
        new File("/test"),
        null,
        null,
        null,
        true,
        false,
        null,
        false,
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

    return new TestableKafkaSupervisorWithCustomIsTaskCurrent(
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        taskClientFactory,
        OBJECT_MAPPER,
        new KafkaSupervisorSpec(
            null,
            dataSchema,
            tuningConfig,
            kafkaSupervisorIOConfig,
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
            supervisorConfig
        ),
        rowIngestionMetersFactory,
        isTaskCurrentReturn
    );
  }

  /**
   * Use when you don't want generateSequenceNumber overridden
   */

  private KafkaSupervisor getSupervisor(
      int replicas,
      int taskCount,
      boolean useEarliestOffset,
      String duration,
      Period lateMessageRejectionPeriod,
      Period earlyMessageRejectionPeriod,
      boolean suspended,
      String kafkaHost,
      DataSchema dataSchema,
      KafkaSupervisorTuningConfig tuningConfig
  )
  {
    Map<String, Object> consumerProperties = new HashMap<>();
    consumerProperties.put("myCustomKey", "myCustomValue");
    consumerProperties.put("bootstrap.servers", kafkaHost);
    consumerProperties.put("isolation.level", "read_committed");
    KafkaSupervisorIOConfig kafkaSupervisorIOConfig = new KafkaSupervisorIOConfig(
        topic,
        INPUT_FORMAT,
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
        null
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

    return new KafkaSupervisor(
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        taskClientFactory,
        OBJECT_MAPPER,
        new KafkaSupervisorSpec(
            null,
            dataSchema,
            tuningConfig,
            kafkaSupervisorIOConfig,
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
            supervisorConfig
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
        new TimestampSpec("timestamp", "iso", null),
        new DimensionsSpec(
            dimensions,
            null,
            null
        ),
        new AggregatorFactory[]{new CountAggregatorFactory("rows")},
        new UniformGranularitySpec(
            Granularities.HOUR,
            Granularities.NONE,
            ImmutableList.of()
        ),
        null
    );
  }

  private KafkaIndexTask createKafkaIndexTask(
      String id,
      String dataSource,
      int taskGroupId,
      SeekableStreamStartSequenceNumbers<Integer, Long> startPartitions,
      SeekableStreamEndSequenceNumbers<Integer, Long> endPartitions,
      DateTime minimumMessageTime,
      DateTime maximumMessageTime,
      KafkaSupervisorTuningConfig tuningConfig
  )
  {
    return createKafkaIndexTask(
        id,
        taskGroupId,
        startPartitions,
        endPartitions,
        minimumMessageTime,
        maximumMessageTime,
        getDataSchema(dataSource),
        tuningConfig
    );
  }

  private KafkaIndexTask createKafkaIndexTask(
      String id,
      int taskGroupId,
      SeekableStreamStartSequenceNumbers<Integer, Long> startPartitions,
      SeekableStreamEndSequenceNumbers<Integer, Long> endPartitions,
      DateTime minimumMessageTime,
      DateTime maximumMessageTime,
      DataSchema schema,
      KafkaSupervisorTuningConfig tuningConfig
  )
  {
    return createKafkaIndexTask(
        id,
        taskGroupId,
        startPartitions,
        endPartitions,
        minimumMessageTime,
        maximumMessageTime,
        schema,
        tuningConfig.convertToTaskTuningConfig()
    );
  }

  private KafkaIndexTask createKafkaIndexTask(
      String id,
      int taskGroupId,
      SeekableStreamStartSequenceNumbers<Integer, Long> startPartitions,
      SeekableStreamEndSequenceNumbers<Integer, Long> endPartitions,
      DateTime minimumMessageTime,
      DateTime maximumMessageTime,
      DataSchema schema,
      KafkaIndexTaskTuningConfig tuningConfig
  )
  {
    return new KafkaIndexTask(
        id,
        null,
        schema,
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
            INPUT_FORMAT
        ),
        Collections.emptyMap(),
        null,
        null,
        rowIngestionMetersFactory,
        OBJECT_MAPPER,
        new DummyForInjectionAppenderatorsManager()
    );
  }

  private static class TestTaskRunnerWorkItem extends TaskRunnerWorkItem
  {
    private final String taskType;
    private final TaskLocation location;
    private final String dataSource;

    TestTaskRunnerWorkItem(Task task, ListenableFuture<TaskStatus> result, TaskLocation location)
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
    private final Map<String, Object> consumerProperties;

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
      this.consumerProperties = spec.getIoConfig().getConsumerProperties();
    }

    @Override
    protected RecordSupplier<Integer, Long> setupRecordSupplier()
    {
      final Map<String, Object> consumerConfigs = KafkaConsumerConfigs.getConsumerProperties();
      consumerConfigs.put("metadata.max.age.ms", "1");
      final Properties props = new Properties();
      KafkaRecordSupplier.addConsumerPropertiesFromConfig(props, sortingMapper, consumerProperties);
      props.putAll(consumerConfigs);
      Deserializer keyDeserializerObject = new ByteArrayDeserializer();
      Deserializer valueDeserializerObject = new ByteArrayDeserializer();
      return new KafkaRecordSupplier(
          new KafkaConsumer<>(props, keyDeserializerObject, valueDeserializerObject)
      );
    }

    @Override
    protected String generateSequenceName(
        Map<Integer, Long> startPartitions,
        Optional<DateTime> minimumMessageTime,
        Optional<DateTime> maximumMessageTime,
        DataSchema dataSchema,
        SeekableStreamIndexTaskTuningConfig tuningConfig
    )
    {
      final int groupId = getTaskGroupIdForPartition(startPartitions.keySet().iterator().next());
      return StringUtils.format("sequenceName-%d", groupId);
    }

    private SeekableStreamSupervisorStateManager getStateManager()
    {
      return stateManager;
    }
  }

  private static class TestableKafkaSupervisorWithCustomIsTaskCurrent extends TestableKafkaSupervisor
  {
    private final boolean isTaskCurrentReturn;

    public TestableKafkaSupervisorWithCustomIsTaskCurrent(
        TaskStorage taskStorage,
        TaskMaster taskMaster,
        IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator,
        KafkaIndexTaskClientFactory taskClientFactory,
        ObjectMapper mapper,
        KafkaSupervisorSpec spec,
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
    public boolean isTaskCurrent(int taskGroupId, String taskId)
    {
      return isTaskCurrentReturn;
    }
  }
}
