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

package org.apache.druid.indexing.rocketmq.supervisor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.data.input.rocketmq.PartitionUtil;
import org.apache.druid.data.input.rocketmq.RocketMQRecordEntity;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskInfoProvider;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.common.task.RealtimeIndexTask;
import org.apache.druid.indexing.common.task.Task;
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
import org.apache.druid.indexing.rocketmq.RocketMQConsumerConfigs;
import org.apache.druid.indexing.rocketmq.RocketMQDataSourceMetadata;
import org.apache.druid.indexing.rocketmq.RocketMQIndexTask;
import org.apache.druid.indexing.rocketmq.RocketMQIndexTaskClient;
import org.apache.druid.indexing.rocketmq.RocketMQIndexTaskClientFactory;
import org.apache.druid.indexing.rocketmq.RocketMQIndexTaskIOConfig;
import org.apache.druid.indexing.rocketmq.RocketMQIndexTaskTuningConfig;
import org.apache.druid.indexing.rocketmq.RocketMQRecordSupplier;
import org.apache.druid.indexing.rocketmq.test.TestBroker;
import org.apache.druid.indexing.seekablestream.SeekableStreamEndSequenceNumbers;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskRunner.Status;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskTuningConfig;
import org.apache.druid.indexing.seekablestream.SeekableStreamStartSequenceNumbers;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorSpec;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorStateManager;
import org.apache.druid.indexing.seekablestream.supervisor.TaskReportData;
import org.apache.druid.indexing.seekablestream.supervisor.autoscaler.LagBasedAutoScalerConfig;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.incremental.RowIngestionMetersFactory;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.RealtimeIOConfig;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.segment.realtime.FireDepartment;
import org.apache.druid.server.metrics.DruidMonitorSchedulerConfig;
import org.apache.druid.server.metrics.ExceptionCapturingServiceEmitter;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
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
public class RocketMQSupervisorTest extends EasyMockSupport
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
  private static final String brokerName = "broker-a";


  private static TestBroker rocketmqServer;
  private static String rocketmqHost;
  private static DataSchema dataSchema;
  private static int topicPostfix;

  private final int numThreads;

  private TestableRocketMQSupervisor supervisor;
  private TaskStorage taskStorage;
  private TaskMaster taskMaster;
  private TaskRunner taskRunner;
  private IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator;
  private RocketMQIndexTaskClient taskClient;
  private TaskQueue taskQueue;
  private String topic;
  private RowIngestionMetersFactory rowIngestionMetersFactory;
  private ExceptionCapturingServiceEmitter serviceEmitter;
  private SupervisorStateManagerConfig supervisorConfig;
  private RocketMQSupervisorIngestionSpec ingestionSchema;

  private static String getTopic()
  {
    //noinspection StringConcatenationMissingWhitespace
    return TOPIC_PREFIX + topicPostfix++;
  }

  @Parameterized.Parameters(name = "numThreads = {0}")
  public static Iterable<Object[]> constructorFeeder()
  {
    return ImmutableList.of(new Object[] {1}, new Object[] {8});
  }

  public RocketMQSupervisorTest(int numThreads)
  {
    this.numThreads = numThreads;
  }

  @BeforeClass
  public static void setupClass() throws Exception
  {
    rocketmqServer = new TestBroker(
        null,
        ImmutableMap.of("default.topic.queue.nums", 2)
    );
    rocketmqServer.start();

    dataSchema = getDataSchema(DATASOURCE);
    rocketmqHost = StringUtils.format("127.0.0.1:%d", rocketmqServer.getPort());
  }

  @Before
  public void setupTest()
  {
    taskStorage = createMock(TaskStorage.class);
    taskMaster = createMock(TaskMaster.class);
    taskRunner = createMock(TaskRunner.class);
    indexerMetadataStorageCoordinator = createMock(IndexerMetadataStorageCoordinator.class);
    taskClient = createMock(RocketMQIndexTaskClient.class);
    taskQueue = createMock(TaskQueue.class);

    topic = getTopic();
    rowIngestionMetersFactory = new TestUtils().getRowIngestionMetersFactory();
    serviceEmitter = new ExceptionCapturingServiceEmitter();
    EmittingLogger.registerEmitter(serviceEmitter);
    supervisorConfig = new SupervisorStateManagerConfig();
    ingestionSchema = EasyMock.createMock(RocketMQSupervisorIngestionSpec.class);
  }

  @After
  public void tearDownTest()
  {
    supervisor = null;
  }

  @AfterClass
  public static void tearDownClass() throws IOException
  {
    rocketmqServer.close();
    rocketmqServer = null;
  }

  @Test
  public void testNoInitialStateWithAutoscaler() throws Exception
  {
    RocketMQIndexTaskClientFactory taskClientFactory = new RocketMQIndexTaskClientFactory(
        null,
        null
    )
    {
      @Override
      public RocketMQIndexTaskClient build(
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

    HashMap<String, Object> autoScalerConfig = new HashMap<>();
    autoScalerConfig.put("enableTaskAutoScaler", true);
    autoScalerConfig.put("lagCollectionIntervalMillis", 500);
    autoScalerConfig.put("lagCollectionRangeMillis", 500);
    autoScalerConfig.put("scaleOutThreshold", 0);
    autoScalerConfig.put("triggerScaleOutFractionThreshold", 0.0);
    autoScalerConfig.put("scaleInThreshold", 1000000);
    autoScalerConfig.put("triggerScaleInFractionThreshold", 0.8);
    autoScalerConfig.put("scaleActionStartDelayMillis", 0);
    autoScalerConfig.put("scaleActionPeriodMillis", 100);
    autoScalerConfig.put("taskCountMax", 2);
    autoScalerConfig.put("taskCountMin", 1);
    autoScalerConfig.put("scaleInStep", 1);
    autoScalerConfig.put("scaleOutStep", 2);
    autoScalerConfig.put("minTriggerScaleActionFrequencyMillis", 1200000);

    final Map<String, Object> consumerProperties = RocketMQConsumerConfigs.getConsumerProperties();
    consumerProperties.put("myCustomKey", "myCustomValue");
    consumerProperties.put("nameserver.url", "127.0.0.1:9876");

    RocketMQSupervisorIOConfig rocketmqSupervisorIOConfig = new RocketMQSupervisorIOConfig(
        topic,
        INPUT_FORMAT,
        1,
        1,
        new Period("PT1H"),
        consumerProperties,
        OBJECT_MAPPER.convertValue(autoScalerConfig, LagBasedAutoScalerConfig.class),
        RocketMQSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
        new Period("P1D"),
        new Period("PT30S"),
        true,
        new Period("PT30M"),
        null,
        null,
        null
    );

    final RocketMQSupervisorTuningConfig tuningConfigOri = new RocketMQSupervisorTuningConfig(
        null,
        1000,
        null,
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

    EasyMock.expect(ingestionSchema.getIOConfig()).andReturn(rocketmqSupervisorIOConfig).anyTimes();
    EasyMock.expect(ingestionSchema.getDataSchema()).andReturn(dataSchema).anyTimes();
    EasyMock.expect(ingestionSchema.getTuningConfig()).andReturn(tuningConfigOri).anyTimes();
    EasyMock.replay(ingestionSchema);

    SeekableStreamSupervisorSpec testableSupervisorSpec = new RocketMQSupervisorSpec(
        ingestionSchema,
        dataSchema,
        tuningConfigOri,
        rocketmqSupervisorIOConfig,
        null,
        false,
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        taskClientFactory,
        OBJECT_MAPPER,
        new NoopServiceEmitter(),
        new DruidMonitorSchedulerConfig(),
        rowIngestionMetersFactory,
        new SupervisorStateManagerConfig()
    );

    supervisor = new TestableRocketMQSupervisor(
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        taskClientFactory,
        OBJECT_MAPPER,
        (RocketMQSupervisorSpec) testableSupervisorSpec,
        rowIngestionMetersFactory
    );

    SupervisorTaskAutoScaler autoscaler = testableSupervisorSpec.createAutoscaler(supervisor);


    final RocketMQSupervisorTuningConfig tuningConfig = supervisor.getTuningConfig();
    addSomeEvents(1);

    Capture<RocketMQIndexTask> captured = Capture.newInstance();
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new RocketMQDataSourceMetadata(
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
    Thread.sleep(1 * 1000);
    verifyAll();

    int taskCountAfterScale = supervisor.getIoConfig().getTaskCount();
    Assert.assertEquals(2, taskCountAfterScale);


    RocketMQIndexTask task = captured.getValue();
    Assert.assertEquals(RocketMQSupervisorTest.dataSchema, task.getDataSchema());
    Assert.assertEquals(tuningConfig.convertToTaskTuningConfig(), task.getTuningConfig());

    RocketMQIndexTaskIOConfig taskConfig = task.getIOConfig();
    Assert.assertEquals(rocketmqHost, taskConfig.getConsumerProperties().get("nameserver.url"));
    Assert.assertEquals("myCustomValue", taskConfig.getConsumerProperties().get("myCustomKey"));
    Assert.assertEquals("sequenceName-0", taskConfig.getBaseSequenceName());
    Assert.assertTrue("isUseTransaction", taskConfig.isUseTransaction());
    Assert.assertFalse("minimumMessageTime", taskConfig.getMinimumMessageTime().isPresent());
    Assert.assertFalse("maximumMessageTime", taskConfig.getMaximumMessageTime().isPresent());

    Assert.assertEquals(topic, taskConfig.getStartSequenceNumbers().getStream());
    Assert.assertEquals(0L, (long) taskConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap().get(PartitionUtil.genPartition(brokerName, 0)));
    Assert.assertEquals(0L, (long) taskConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap().get(PartitionUtil.genPartition(brokerName, 1)));
    Assert.assertEquals(0L, (long) taskConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap().get(PartitionUtil.genPartition(brokerName, 2)));

    Assert.assertEquals(topic, taskConfig.getEndSequenceNumbers().getStream());
    Assert.assertEquals(
        Long.MAX_VALUE,
        (long) taskConfig.getEndSequenceNumbers().getPartitionSequenceNumberMap().get(PartitionUtil.genPartition(brokerName, 0))
    );
    Assert.assertEquals(
        Long.MAX_VALUE,
        (long) taskConfig.getEndSequenceNumbers().getPartitionSequenceNumberMap().get(PartitionUtil.genPartition(brokerName, 1))
    );
    Assert.assertEquals(
        Long.MAX_VALUE,
        (long) taskConfig.getEndSequenceNumbers().getPartitionSequenceNumberMap().get(PartitionUtil.genPartition(brokerName, 2))
    );

    autoscaler.reset();
    autoscaler.stop();
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
        new RocketMQIndexTaskIOConfig(
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
        new RocketMQIndexTaskTuningConfig(
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
    final RocketMQSupervisorTuningConfig tuningConfig = supervisor.getTuningConfig();
    addSomeEvents(1);

    Capture<RocketMQIndexTask> captured = Capture.newInstance();
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new RocketMQDataSourceMetadata(
            null
        )
    ).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.capture(captured))).andReturn(true);
    taskRunner.registerListener(EasyMock.anyObject(TaskRunnerListener.class), EasyMock.anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    RocketMQIndexTask task = captured.getValue();
    Assert.assertEquals(dataSchema, task.getDataSchema());
    Assert.assertEquals(tuningConfig.convertToTaskTuningConfig(), task.getTuningConfig());

    RocketMQIndexTaskIOConfig taskConfig = task.getIOConfig();
    Assert.assertEquals(rocketmqHost, taskConfig.getConsumerProperties().get("nameserver.url"));
    Assert.assertEquals("myCustomValue", taskConfig.getConsumerProperties().get("myCustomKey"));
    Assert.assertEquals("sequenceName-0", taskConfig.getBaseSequenceName());
    Assert.assertTrue("isUseTransaction", taskConfig.isUseTransaction());
    Assert.assertFalse("minimumMessageTime", taskConfig.getMinimumMessageTime().isPresent());
    Assert.assertFalse("maximumMessageTime", taskConfig.getMaximumMessageTime().isPresent());

    Assert.assertEquals(topic, taskConfig.getStartSequenceNumbers().getStream());
    Assert.assertEquals(0L, (long) taskConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap().get(PartitionUtil.genPartition(brokerName, 0)));
    Assert.assertEquals(0L, (long) taskConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap().get(PartitionUtil.genPartition(brokerName, 1)));
    Assert.assertEquals(0L, (long) taskConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap().get(PartitionUtil.genPartition(brokerName, 2)));

    Assert.assertEquals(topic, taskConfig.getEndSequenceNumbers().getStream());
    Assert.assertEquals(
        Long.MAX_VALUE,
        (long) taskConfig.getEndSequenceNumbers().getPartitionSequenceNumberMap().get(PartitionUtil.genPartition(brokerName, 0))
    );
    Assert.assertEquals(
        Long.MAX_VALUE,
        (long) taskConfig.getEndSequenceNumbers().getPartitionSequenceNumberMap().get(PartitionUtil.genPartition(brokerName, 1))
    );
    Assert.assertEquals(
        Long.MAX_VALUE,
        (long) taskConfig.getEndSequenceNumbers().getPartitionSequenceNumberMap().get(PartitionUtil.genPartition(brokerName, 2))
    );
  }

  @Test
  public void testSkipOffsetGaps() throws Exception
  {
    supervisor = getTestableSupervisor(1, 1, true, "PT1H", null, null);
    addSomeEvents(1);

    Capture<RocketMQIndexTask> captured = Capture.newInstance();
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new RocketMQDataSourceMetadata(
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

    Capture<RocketMQIndexTask> captured = Capture.newInstance(CaptureType.ALL);
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new RocketMQDataSourceMetadata(
            null
        )
    ).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.capture(captured))).andReturn(true).times(2);
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    RocketMQIndexTask task1 = captured.getValues().get(0);
    Assert.assertEquals(2, task1.getIOConfig().getStartSequenceNumbers().getPartitionSequenceNumberMap().size());
    Assert.assertEquals(2, task1.getIOConfig().getEndSequenceNumbers().getPartitionSequenceNumberMap().size());
    Assert.assertEquals(
        0L,
        task1.getIOConfig().getStartSequenceNumbers().getPartitionSequenceNumberMap().get(PartitionUtil.genPartition(brokerName, 0)).longValue()
    );
    Assert.assertEquals(
        Long.MAX_VALUE,
        task1.getIOConfig().getEndSequenceNumbers().getPartitionSequenceNumberMap().get(PartitionUtil.genPartition(brokerName, 0)).longValue()
    );
    Assert.assertEquals(
        0L,
        task1.getIOConfig().getStartSequenceNumbers().getPartitionSequenceNumberMap().get(PartitionUtil.genPartition(brokerName, 2)).longValue()
    );
    Assert.assertEquals(
        Long.MAX_VALUE,
        task1.getIOConfig().getEndSequenceNumbers().getPartitionSequenceNumberMap().get(PartitionUtil.genPartition(brokerName, 2)).longValue()
    );

    RocketMQIndexTask task2 = captured.getValues().get(1);
    Assert.assertEquals(1, task2.getIOConfig().getStartSequenceNumbers().getPartitionSequenceNumberMap().size());
    Assert.assertEquals(1, task2.getIOConfig().getEndSequenceNumbers().getPartitionSequenceNumberMap().size());
    Assert.assertEquals(
        0L,
        task2.getIOConfig().getStartSequenceNumbers().getPartitionSequenceNumberMap().get(PartitionUtil.genPartition(brokerName, 1)).longValue()
    );
    Assert.assertEquals(
        Long.MAX_VALUE,
        task2.getIOConfig().getEndSequenceNumbers().getPartitionSequenceNumberMap().get(PartitionUtil.genPartition(brokerName, 1)).longValue()
    );
  }

  @Test
  public void testReplicas() throws Exception
  {
    supervisor = getTestableSupervisor(2, 1, true, "PT1H", null, null);
    addSomeEvents(1);

    Capture<RocketMQIndexTask> captured = Capture.newInstance(CaptureType.ALL);
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new RocketMQDataSourceMetadata(
            null
        )
    ).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.capture(captured))).andReturn(true).times(2);
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    RocketMQIndexTask task1 = captured.getValues().get(0);
    Assert.assertEquals(3, task1.getIOConfig().getStartSequenceNumbers().getPartitionSequenceNumberMap().size());
    Assert.assertEquals(3, task1.getIOConfig().getEndSequenceNumbers().getPartitionSequenceNumberMap().size());
    Assert.assertEquals(
        0L,
        task1.getIOConfig().getStartSequenceNumbers().getPartitionSequenceNumberMap().get(PartitionUtil.genPartition(brokerName, 0)).longValue()
    );
    Assert.assertEquals(
        0L,
        task1.getIOConfig().getStartSequenceNumbers().getPartitionSequenceNumberMap().get(PartitionUtil.genPartition(brokerName, 1)).longValue()
    );
    Assert.assertEquals(
        0L,
        task1.getIOConfig().getStartSequenceNumbers().getPartitionSequenceNumberMap().get(PartitionUtil.genPartition(brokerName, 2)).longValue()
    );

    RocketMQIndexTask task2 = captured.getValues().get(1);
    Assert.assertEquals(3, task2.getIOConfig().getStartSequenceNumbers().getPartitionSequenceNumberMap().size());
    Assert.assertEquals(3, task2.getIOConfig().getEndSequenceNumbers().getPartitionSequenceNumberMap().size());
    Assert.assertEquals(
        0L,
        task2.getIOConfig().getStartSequenceNumbers().getPartitionSequenceNumberMap().get(PartitionUtil.genPartition(brokerName, 0)).longValue()
    );
    Assert.assertEquals(
        0L,
        task2.getIOConfig().getStartSequenceNumbers().getPartitionSequenceNumberMap().get(PartitionUtil.genPartition(brokerName, 1)).longValue()
    );
    Assert.assertEquals(
        0L,
        task2.getIOConfig().getStartSequenceNumbers().getPartitionSequenceNumberMap().get(PartitionUtil.genPartition(brokerName, 2)).longValue()
    );
  }

  @Test
  public void testLateMessageRejectionPeriod() throws Exception
  {
    supervisor = getTestableSupervisor(2, 1, true, "PT1H", new Period("PT1H"), null);
    addSomeEvents(1);

    Capture<RocketMQIndexTask> captured = Capture.newInstance(CaptureType.ALL);
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new RocketMQDataSourceMetadata(
            null
        )
    ).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.capture(captured))).andReturn(true).times(2);
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    RocketMQIndexTask task1 = captured.getValues().get(0);
    RocketMQIndexTask task2 = captured.getValues().get(1);

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

    Capture<RocketMQIndexTask> captured = Capture.newInstance(CaptureType.ALL);
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new RocketMQDataSourceMetadata(
            null
        )
    ).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.capture(captured))).andReturn(true).times(2);
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    RocketMQIndexTask task1 = captured.getValues().get(0);
    RocketMQIndexTask task2 = captured.getValues().get(1);

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
   * Test generating the starting offsets from the partition high water marks in RocketMQ.
   */
  @Test
  public void testLatestOffset() throws Exception
  {
    supervisor = getTestableSupervisor(1, 1, false, "PT1H", null, null);
    addSomeEvents(1100);

    Capture<RocketMQIndexTask> captured = Capture.newInstance();
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new RocketMQDataSourceMetadata(
            null
        )
    ).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.capture(captured))).andReturn(true);
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    RocketMQIndexTask task = captured.getValue();
    Assert.assertEquals(
        1101L,
        task.getIOConfig().getStartSequenceNumbers().getPartitionSequenceNumberMap().get(PartitionUtil.genPartition(brokerName, 0)).longValue()
    );
    Assert.assertEquals(
        1101L,
        task.getIOConfig().getStartSequenceNumbers().getPartitionSequenceNumberMap().get(PartitionUtil.genPartition(brokerName, 1)).longValue()
    );
    Assert.assertEquals(
        1101L,
        task.getIOConfig().getStartSequenceNumbers().getPartitionSequenceNumberMap().get(PartitionUtil.genPartition(brokerName, 2)).longValue()
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

    Capture<RocketMQIndexTask> captured = Capture.newInstance();
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new RocketMQDataSourceMetadata(
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

    Capture<RocketMQIndexTask> captured = Capture.newInstance();
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new RocketMQDataSourceMetadata(
            null
        )
    ).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.capture(captured))).andReturn(true);
    replayAll();
    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    RocketMQIndexTask task = captured.getValue();
    Assert.assertEquals(
        10,
        task.getIOConfig().getStartSequenceNumbers().getPartitionSequenceNumberMap().get(PartitionUtil.genPartition(brokerName, 0)).longValue()
    );
    Assert.assertEquals(
        10,
        task.getIOConfig().getStartSequenceNumbers().getPartitionSequenceNumberMap().get(PartitionUtil.genPartition(brokerName, 1)).longValue()
    );
    Assert.assertEquals(
        10,
        task.getIOConfig().getStartSequenceNumbers().getPartitionSequenceNumberMap().get(PartitionUtil.genPartition(brokerName, 2)).longValue()
    );

    addMoreEvents(9, 6);
    EasyMock.reset(taskQueue, taskStorage);
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    Capture<RocketMQIndexTask> tmp = Capture.newInstance();
    EasyMock.expect(taskQueue.add(EasyMock.capture(tmp))).andReturn(true);
    EasyMock.replay(taskStorage, taskQueue);
    supervisor.runInternal();
    verifyAll();

    EasyMock.reset(taskQueue, taskStorage);
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    Capture<RocketMQIndexTask> newcaptured = Capture.newInstance();
    EasyMock.expect(taskQueue.add(EasyMock.capture(newcaptured))).andReturn(true);
    EasyMock.replay(taskStorage, taskQueue);
    supervisor.runInternal();
    verifyAll();

    //check if start from earliest offset
    task = newcaptured.getValue();
    Assert.assertEquals(
        0,
        task.getIOConfig().getStartSequenceNumbers().getPartitionSequenceNumberMap().get(PartitionUtil.genPartition(brokerName, 3)).longValue()
    );
    Assert.assertEquals(
        0,
        task.getIOConfig().getStartSequenceNumbers().getPartitionSequenceNumberMap().get(PartitionUtil.genPartition(brokerName, 4)).longValue()
    );
    Assert.assertEquals(
        0,
        task.getIOConfig().getStartSequenceNumbers().getPartitionSequenceNumberMap().get(PartitionUtil.genPartition(brokerName, 5)).longValue()
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

    Capture<RocketMQIndexTask> captured = Capture.newInstance();
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new RocketMQDataSourceMetadata(
            new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 10L, PartitionUtil.genPartition(brokerName, 1), 20L, PartitionUtil.genPartition(brokerName, 2), 30L), ImmutableSet.of())
        )
    ).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.capture(captured))).andReturn(true);
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    RocketMQIndexTask task = captured.getValue();
    RocketMQIndexTaskIOConfig taskConfig = task.getIOConfig();
    Assert.assertEquals("sequenceName-0", taskConfig.getBaseSequenceName());
    Assert.assertEquals(
        10L,
        taskConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap().get(PartitionUtil.genPartition(brokerName, 0)).longValue()
    );
    Assert.assertEquals(
        20L,
        taskConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap().get(PartitionUtil.genPartition(brokerName, 1)).longValue()
    );
    Assert.assertEquals(
        30L,
        taskConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap().get(PartitionUtil.genPartition(brokerName, 2)).longValue()
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
        new RocketMQDataSourceMetadata(
            new SeekableStreamStartSequenceNumbers<>(
                topic,
                ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), -10L, PartitionUtil.genPartition(brokerName, 1), -20L, PartitionUtil.genPartition(brokerName, 2), -30L),
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

    // non RocketMQIndexTask (don't kill)
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
        new RocketMQDataSourceMetadata(
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
    final RocketMQSupervisorTuningConfig tuningConfig = supervisor.getTuningConfig();
    addSomeEvents(1);

    Task id1 = createRocketMQIndexTask(
        "id1",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>("topic", ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 0L, PartitionUtil.genPartition(brokerName, 2), 0L), ImmutableSet.of()),
        new SeekableStreamEndSequenceNumbers<>("topic", ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), Long.MAX_VALUE, PartitionUtil.genPartition(brokerName, 2), Long.MAX_VALUE)),
        null,
        null,
        tuningConfig
    );
    Task id2 = createRocketMQIndexTask(
        "id2",
        DATASOURCE,
        1,
        new SeekableStreamStartSequenceNumbers<>("topic", ImmutableMap.of(PartitionUtil.genPartition(brokerName, 1), 0L), ImmutableSet.of()),
        new SeekableStreamEndSequenceNumbers<>("topic", ImmutableMap.of(PartitionUtil.genPartition(brokerName, 1), Long.MAX_VALUE)),
        null,
        null,
        tuningConfig
    );
    Task id3 = createRocketMQIndexTask(
        "id3",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>("topic", ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 0L, PartitionUtil.genPartition(brokerName, 1), 0L, PartitionUtil.genPartition(brokerName, 2), 0L), ImmutableSet.of()),
        new SeekableStreamEndSequenceNumbers<>(
            "topic",
            ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), Long.MAX_VALUE, PartitionUtil.genPartition(brokerName, 1), Long.MAX_VALUE, PartitionUtil.genPartition(brokerName, 2), Long.MAX_VALUE)
        ),
        null,
        null,
        tuningConfig
    );
    Task id4 = createRocketMQIndexTask(
        "id4",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>("topic", ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 0L, PartitionUtil.genPartition(brokerName, 1), 0L), ImmutableSet.of()),
        new SeekableStreamEndSequenceNumbers<>("topic", ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), Long.MAX_VALUE, PartitionUtil.genPartition(brokerName, 1), Long.MAX_VALUE)),
        null,
        null,
        tuningConfig
    );
    Task id5 = createRocketMQIndexTask(
        "id5",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>("topic", ImmutableMap.of(PartitionUtil.genPartition(brokerName, 1), 0L, PartitionUtil.genPartition(brokerName, 2), 0L), ImmutableSet.of()),
        new SeekableStreamEndSequenceNumbers<>("topic", ImmutableMap.of(PartitionUtil.genPartition(brokerName, 1), Long.MAX_VALUE, PartitionUtil.genPartition(brokerName, 2), Long.MAX_VALUE)),
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
        new RocketMQDataSourceMetadata(
            null
        )
    ).anyTimes();
    EasyMock.expect(taskClient.stopAsync("id3", false)).andReturn(Futures.immediateFuture(true));
    EasyMock.expect(taskClient.stopAsync("id4", false)).andReturn(Futures.immediateFuture(false));
    EasyMock.expect(taskClient.stopAsync("id5", false)).andReturn(Futures.immediateFuture(null));

    TreeMap<Integer, Map<String, Long>> checkpoints1 = new TreeMap<>();
    checkpoints1.put(0, ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 0L, PartitionUtil.genPartition(brokerName, 2), 0L));
    TreeMap<Integer, Map<String, Long>> checkpoints2 = new TreeMap<>();
    checkpoints2.put(0, ImmutableMap.of(PartitionUtil.genPartition(brokerName, 1), 0L));
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
        new RocketMQDataSourceMetadata(
            null
        )
    ).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.capture(captured))).andReturn(true).times(4);

    TreeMap<Integer, Map<String, Long>> checkpoints1 = new TreeMap<>();
    checkpoints1.put(0, ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 0L, PartitionUtil.genPartition(brokerName, 2), 0L));
    TreeMap<Integer, Map<String, Long>> checkpoints2 = new TreeMap<>();
    checkpoints2.put(0, ImmutableMap.of(PartitionUtil.genPartition(brokerName, 1), 0L));
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
    RocketMQIndexTask iHaveFailed = (RocketMQIndexTask) tasks.get(3);
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
        ((RocketMQIndexTask) aNewTaskCapture.getValue()).getIOConfig().getBaseSequenceName()
    );
  }

  @Test
  public void testRequeueAdoptedTaskWhenFailed() throws Exception
  {
    supervisor = getTestableSupervisor(2, 1, true, "PT1H", null, null);
    addSomeEvents(1);

    DateTime now = DateTimes.nowUtc();
    DateTime maxi = now.plusMinutes(60);
    Task id1 = createRocketMQIndexTask(
        "id1",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>("topic", ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 0L, PartitionUtil.genPartition(brokerName, 2), 0L), ImmutableSet.of()),
        new SeekableStreamEndSequenceNumbers<>("topic", ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), Long.MAX_VALUE, PartitionUtil.genPartition(brokerName, 2), Long.MAX_VALUE)),
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
        new RocketMQDataSourceMetadata(
            null
        )
    ).anyTimes();

    TreeMap<Integer, Map<String, Long>> checkpoints = new TreeMap<>();
    checkpoints.put(0, ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 0L, PartitionUtil.genPartition(brokerName, 2), 0L));
    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("id1"), EasyMock.anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints))
        .times(2);

    taskRunner.registerListener(EasyMock.anyObject(TaskRunnerListener.class), EasyMock.anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    // check that replica tasks are created with the same minimumMessageTime as tasks inherited from another supervisor
    Assert.assertEquals(now, ((RocketMQIndexTask) captured.getValue()).getIOConfig().getMinimumMessageTime().get());

    // test that a task failing causes a new task to be re-queued with the same parameters
    String runningTaskId = captured.getValue().getId();
    Capture<Task> aNewTaskCapture = Capture.newInstance();
    RocketMQIndexTask iHaveFailed = (RocketMQIndexTask) existingTasks.get(0);
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
        ((RocketMQIndexTask) aNewTaskCapture.getValue()).getIOConfig().getBaseSequenceName()
    );

    // check that failed tasks are recreated with the same minimumMessageTime as the task it replaced, even if that
    // task came from another supervisor
    Assert.assertEquals(now, ((RocketMQIndexTask) aNewTaskCapture.getValue()).getIOConfig().getMinimumMessageTime().get());
    Assert.assertEquals(
        maxi,
        ((RocketMQIndexTask) aNewTaskCapture.getValue()).getIOConfig().getMaximumMessageTime().get()
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
        new RocketMQDataSourceMetadata(
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
    TreeMap<Integer, Map<String, Long>> checkpoints1 = new TreeMap<>();
    checkpoints1.put(0, ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 0L, PartitionUtil.genPartition(brokerName, 2), 0L));
    TreeMap<Integer, Map<String, Long>> checkpoints2 = new TreeMap<>();
    checkpoints2.put(0, ImmutableMap.of(PartitionUtil.genPartition(brokerName, 1), 0L));
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
    RocketMQIndexTask iAmSuccess = (RocketMQIndexTask) tasks.get(0);
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
    final RocketMQSupervisorTuningConfig tuningConfig = supervisor.getTuningConfig();
    addSomeEvents(100);

    Capture<Task> captured = Capture.newInstance(CaptureType.ALL);
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(Collections.emptyList()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new RocketMQDataSourceMetadata(
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
        .andReturn(Futures.immediateFuture(ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 10L, PartitionUtil.genPartition(brokerName, 2), 30L)))
        .andReturn(Futures.immediateFuture(ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 10L, PartitionUtil.genPartition(brokerName, 2), 35L)));
    EasyMock.expect(
        taskClient.setEndOffsetsAsync(
            EasyMock.contains("sequenceName-0"),
            EasyMock.eq(ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 10L, PartitionUtil.genPartition(brokerName, 2), 35L)),
            EasyMock.eq(true)
        )
    ).andReturn(Futures.immediateFuture(true)).times(2);
    EasyMock.expect(taskQueue.add(EasyMock.capture(captured))).andReturn(true).times(2);

    TreeMap<Integer, Map<String, Long>> checkpoints1 = new TreeMap<>();
    checkpoints1.put(0, ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 0L, PartitionUtil.genPartition(brokerName, 2), 0L));
    TreeMap<Integer, Map<String, Long>> checkpoints2 = new TreeMap<>();
    checkpoints2.put(0, ImmutableMap.of(PartitionUtil.genPartition(brokerName, 1), 0L));
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
      RocketMQIndexTask rocketmqIndexTask = (RocketMQIndexTask) task;
      Assert.assertEquals(dataSchema, rocketmqIndexTask.getDataSchema());
      Assert.assertEquals(tuningConfig.convertToTaskTuningConfig(), rocketmqIndexTask.getTuningConfig());

      RocketMQIndexTaskIOConfig taskConfig = rocketmqIndexTask.getIOConfig();
      Assert.assertEquals("sequenceName-0", taskConfig.getBaseSequenceName());
      Assert.assertTrue("isUseTransaction", taskConfig.isUseTransaction());

      Assert.assertEquals(topic, taskConfig.getStartSequenceNumbers().getStream());
      Assert.assertEquals(10L, (long) taskConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap().get(PartitionUtil.genPartition(brokerName, 0)));
      Assert.assertEquals(35L, (long) taskConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap().get(PartitionUtil.genPartition(brokerName, 2)));
    }
  }

  @Test
  public void testDiscoverExistingPublishingTask() throws Exception
  {
    final TaskLocation location = new TaskLocation("testHost", 1234, -1);

    supervisor = getTestableSupervisor(1, 1, true, "PT1H", null, null);
    final RocketMQSupervisorTuningConfig tuningConfig = supervisor.getTuningConfig();
    addSomeEvents(1);

    Task task = createRocketMQIndexTask(
        "id1",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>("topic", ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 0L, PartitionUtil.genPartition(brokerName, 1), 0L, PartitionUtil.genPartition(brokerName, 2), 0L), ImmutableSet.of()),
        new SeekableStreamEndSequenceNumbers<>(
            "topic",
            ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), Long.MAX_VALUE, PartitionUtil.genPartition(brokerName, 1), Long.MAX_VALUE, PartitionUtil.genPartition(brokerName, 2), Long.MAX_VALUE)
        ),
        null,
        null,
        supervisor.getTuningConfig()
    );

    Collection workItems = new ArrayList<>();
    workItems.add(new TestTaskRunnerWorkItem(task, null, location));

    Capture<RocketMQIndexTask> captured = Capture.newInstance();
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(workItems).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of(task)).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id1")).andReturn(Optional.of(TaskStatus.running("id1"))).anyTimes();
    EasyMock.expect(taskStorage.getTask("id1")).andReturn(Optional.of(task)).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new RocketMQDataSourceMetadata(
            null
        )
    ).anyTimes();
    EasyMock.expect(taskClient.getStatusAsync("id1")).andReturn(Futures.immediateFuture(Status.PUBLISHING));
    EasyMock.expect(taskClient.getCurrentOffsetsAsync("id1", false))
        .andReturn(Futures.immediateFuture(ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 10L, PartitionUtil.genPartition(brokerName, 1), 20L, PartitionUtil.genPartition(brokerName, 2), 30L)));
    EasyMock.expect(taskClient.getEndOffsets("id1")).andReturn(ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 10L, PartitionUtil.genPartition(brokerName, 1), 20L, PartitionUtil.genPartition(brokerName, 2), 30L));
    EasyMock.expect(taskQueue.add(EasyMock.capture(captured))).andReturn(true);

    TreeMap<Integer, Map<String, Long>> checkpoints = new TreeMap<>();
    checkpoints.put(0, ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 0L, PartitionUtil.genPartition(brokerName, 1), 0L, PartitionUtil.genPartition(brokerName, 2), 0L));
    EasyMock.expect(taskClient.getCheckpoints(EasyMock.anyString(), EasyMock.anyBoolean()))
        .andReturn(checkpoints)
        .anyTimes();

    taskRunner.registerListener(EasyMock.anyObject(TaskRunnerListener.class), EasyMock.anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    supervisor.updateCurrentAndLatestOffsets();
    SupervisorReport<RocketMQSupervisorReportPayload> report = supervisor.getStatus();
    verifyAll();

    Assert.assertEquals(DATASOURCE, report.getId());

    RocketMQSupervisorReportPayload payload = report.getPayload();

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
    Assert.assertEquals(ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 0L, PartitionUtil.genPartition(brokerName, 1), 0L, PartitionUtil.genPartition(brokerName, 2), 0L), publishingReport.getStartingOffsets());
    Assert.assertEquals(ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 10L, PartitionUtil.genPartition(brokerName, 1), 20L, PartitionUtil.genPartition(brokerName, 2), 30L), publishingReport.getCurrentOffsets());

    RocketMQIndexTask capturedTask = captured.getValue();
    Assert.assertEquals(dataSchema, capturedTask.getDataSchema());
    Assert.assertEquals(tuningConfig.convertToTaskTuningConfig(), capturedTask.getTuningConfig());

    RocketMQIndexTaskIOConfig capturedTaskConfig = capturedTask.getIOConfig();
    Assert.assertEquals(rocketmqHost, capturedTaskConfig.getConsumerProperties().get("nameserver.url"));
    Assert.assertEquals("myCustomValue", capturedTaskConfig.getConsumerProperties().get("myCustomKey"));
    Assert.assertEquals("sequenceName-0", capturedTaskConfig.getBaseSequenceName());
    Assert.assertTrue("isUseTransaction", capturedTaskConfig.isUseTransaction());

    // check that the new task was created with starting offsets matching where the publishing task finished
    Assert.assertEquals(topic, capturedTaskConfig.getStartSequenceNumbers().getStream());
    Assert.assertEquals(
        10L,
        capturedTaskConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap().get(PartitionUtil.genPartition(brokerName, 0)).longValue()
    );
    Assert.assertEquals(
        20L,
        capturedTaskConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap().get(PartitionUtil.genPartition(brokerName, 1)).longValue()
    );
    Assert.assertEquals(
        30L,
        capturedTaskConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap().get(PartitionUtil.genPartition(brokerName, 2)).longValue()
    );

    Assert.assertEquals(topic, capturedTaskConfig.getEndSequenceNumbers().getStream());
    Assert.assertEquals(
        Long.MAX_VALUE,
        capturedTaskConfig.getEndSequenceNumbers().getPartitionSequenceNumberMap().get(PartitionUtil.genPartition(brokerName, 0)).longValue()
    );
    Assert.assertEquals(
        Long.MAX_VALUE,
        capturedTaskConfig.getEndSequenceNumbers().getPartitionSequenceNumberMap().get(PartitionUtil.genPartition(brokerName, 1)).longValue()
    );
    Assert.assertEquals(
        Long.MAX_VALUE,
        capturedTaskConfig.getEndSequenceNumbers().getPartitionSequenceNumberMap().get(PartitionUtil.genPartition(brokerName, 2)).longValue()
    );
  }

  @Test
  public void testDiscoverExistingPublishingTaskWithDifferentPartitionAllocation() throws Exception
  {
    final TaskLocation location = new TaskLocation("testHost", 1234, -1);

    supervisor = getTestableSupervisor(1, 1, true, "PT1H", null, null);
    final RocketMQSupervisorTuningConfig tuningConfig = supervisor.getTuningConfig();
    addSomeEvents(1);

    Task task = createRocketMQIndexTask(
        "id1",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>("topic", ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 0L, PartitionUtil.genPartition(brokerName, 2), 0L), ImmutableSet.of()),
        new SeekableStreamEndSequenceNumbers<>("topic", ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), Long.MAX_VALUE, PartitionUtil.genPartition(brokerName, 2), Long.MAX_VALUE)),
        null,
        null,
        supervisor.getTuningConfig()
    );

    Collection workItems = new ArrayList<>();
    workItems.add(new TestTaskRunnerWorkItem(task, null, location));

    Capture<RocketMQIndexTask> captured = Capture.newInstance();
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(workItems).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of(task)).anyTimes();
    EasyMock.expect(taskStorage.getStatus("id1")).andReturn(Optional.of(TaskStatus.running("id1"))).anyTimes();
    EasyMock.expect(taskStorage.getTask("id1")).andReturn(Optional.of(task)).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new RocketMQDataSourceMetadata(
            null
        )
    ).anyTimes();
    EasyMock.expect(taskClient.getStatusAsync("id1")).andReturn(Futures.immediateFuture(Status.PUBLISHING));
    EasyMock.expect(taskClient.getCurrentOffsetsAsync("id1", false))
        .andReturn(Futures.immediateFuture(ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 10L, PartitionUtil.genPartition(brokerName, 2), 30L)));
    EasyMock.expect(taskClient.getEndOffsets("id1")).andReturn(ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 10L, PartitionUtil.genPartition(brokerName, 2), 30L));
    EasyMock.expect(taskQueue.add(EasyMock.capture(captured))).andReturn(true);

    taskRunner.registerListener(EasyMock.anyObject(TaskRunnerListener.class), EasyMock.anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    supervisor.updateCurrentAndLatestOffsets();
    SupervisorReport<RocketMQSupervisorReportPayload> report = supervisor.getStatus();
    verifyAll();

    Assert.assertEquals(DATASOURCE, report.getId());

    RocketMQSupervisorReportPayload payload = report.getPayload();

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
    Assert.assertEquals(ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 0L, PartitionUtil.genPartition(brokerName, 2), 0L), publishingReport.getStartingOffsets());
    Assert.assertEquals(ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 10L, PartitionUtil.genPartition(brokerName, 2), 30L), publishingReport.getCurrentOffsets());

    RocketMQIndexTask capturedTask = captured.getValue();
    Assert.assertEquals(dataSchema, capturedTask.getDataSchema());
    Assert.assertEquals(tuningConfig.convertToTaskTuningConfig(), capturedTask.getTuningConfig());

    RocketMQIndexTaskIOConfig capturedTaskConfig = capturedTask.getIOConfig();
    Assert.assertEquals(rocketmqHost, capturedTaskConfig.getConsumerProperties().get("nameserver.url"));
    Assert.assertEquals("myCustomValue", capturedTaskConfig.getConsumerProperties().get("myCustomKey"));
    Assert.assertEquals("sequenceName-0", capturedTaskConfig.getBaseSequenceName());
    Assert.assertTrue("isUseTransaction", capturedTaskConfig.isUseTransaction());

    // check that the new task was created with starting offsets matching where the publishing task finished
    Assert.assertEquals(topic, capturedTaskConfig.getStartSequenceNumbers().getStream());
    Assert.assertEquals(
        10L,
        capturedTaskConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap().get(PartitionUtil.genPartition(brokerName, 0)).longValue()
    );
    Assert.assertEquals(
        0L,
        capturedTaskConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap().get(PartitionUtil.genPartition(brokerName, 1)).longValue()
    );
    Assert.assertEquals(
        30L,
        capturedTaskConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap().get(PartitionUtil.genPartition(brokerName, 2)).longValue()
    );

    Assert.assertEquals(topic, capturedTaskConfig.getEndSequenceNumbers().getStream());
    Assert.assertEquals(
        Long.MAX_VALUE,
        capturedTaskConfig.getEndSequenceNumbers().getPartitionSequenceNumberMap().get(PartitionUtil.genPartition(brokerName, 0)).longValue()
    );
    Assert.assertEquals(
        Long.MAX_VALUE,
        capturedTaskConfig.getEndSequenceNumbers().getPartitionSequenceNumberMap().get(PartitionUtil.genPartition(brokerName, 1)).longValue()
    );
    Assert.assertEquals(
        Long.MAX_VALUE,
        capturedTaskConfig.getEndSequenceNumbers().getPartitionSequenceNumberMap().get(PartitionUtil.genPartition(brokerName, 2)).longValue()
    );
  }

  @Test
  public void testDiscoverExistingPublishingAndReadingTask() throws Exception
  {
    final TaskLocation location1 = new TaskLocation("testHost", 1234, -1);
    final TaskLocation location2 = new TaskLocation("testHost2", 145, -1);
    final DateTime startTime = DateTimes.nowUtc();

    supervisor = getTestableSupervisor(1, 1, true, "PT1H", null, null);
    final RocketMQSupervisorTuningConfig tuningConfig = supervisor.getTuningConfig();
    addSomeEvents(6);

    Task id1 = createRocketMQIndexTask(
        "id1",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>("topic", ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 0L, PartitionUtil.genPartition(brokerName, 1), 0L, PartitionUtil.genPartition(brokerName, 2), 0L), ImmutableSet.of()),
        new SeekableStreamEndSequenceNumbers<>(
            "topic",
            ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), Long.MAX_VALUE, PartitionUtil.genPartition(brokerName, 1), Long.MAX_VALUE, PartitionUtil.genPartition(brokerName, 2), Long.MAX_VALUE)
        ),
        null,
        null,
        tuningConfig
    );

    Task id2 = createRocketMQIndexTask(
        "id2",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>("topic", ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 1L, PartitionUtil.genPartition(brokerName, 1), 2L, PartitionUtil.genPartition(brokerName, 2), 3L), ImmutableSet.of()),
        new SeekableStreamEndSequenceNumbers<>(
            "topic",
            ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), Long.MAX_VALUE, PartitionUtil.genPartition(brokerName, 1), Long.MAX_VALUE, PartitionUtil.genPartition(brokerName, 2), Long.MAX_VALUE)
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
        new RocketMQDataSourceMetadata(
            null
        )
    ).anyTimes();
    EasyMock.expect(taskClient.getStatusAsync("id1")).andReturn(Futures.immediateFuture(Status.PUBLISHING));
    EasyMock.expect(taskClient.getStatusAsync("id2")).andReturn(Futures.immediateFuture(Status.READING));
    EasyMock.expect(taskClient.getStartTimeAsync("id2")).andReturn(Futures.immediateFuture(startTime));
    EasyMock.expect(taskClient.getCurrentOffsetsAsync("id1", false))
        .andReturn(Futures.immediateFuture(ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 1L, PartitionUtil.genPartition(brokerName, 1), 2L, PartitionUtil.genPartition(brokerName, 2), 3L)));
    EasyMock.expect(taskClient.getEndOffsets("id1")).andReturn(ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 1L, PartitionUtil.genPartition(brokerName, 1), 2L, PartitionUtil.genPartition(brokerName, 2), 3L));
    EasyMock.expect(taskClient.getCurrentOffsetsAsync("id2", false))
        .andReturn(Futures.immediateFuture(ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 4L, PartitionUtil.genPartition(brokerName, 1), 5L, PartitionUtil.genPartition(brokerName, 2), 6L)));

    taskRunner.registerListener(EasyMock.anyObject(TaskRunnerListener.class), EasyMock.anyObject(Executor.class));

    // since id1 is publishing, so getCheckpoints wouldn't be called for it
    TreeMap<Integer, Map<String, Long>> checkpoints = new TreeMap<>();
    checkpoints.put(0, ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 1L, PartitionUtil.genPartition(brokerName, 1), 2L, PartitionUtil.genPartition(brokerName, 2), 3L));
    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("id2"), EasyMock.anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints))
        .times(1);

    replayAll();

    supervisor.start();
    supervisor.runInternal();
    supervisor.updateCurrentAndLatestOffsets();
    SupervisorReport<RocketMQSupervisorReportPayload> report = supervisor.getStatus();
    verifyAll();

    Assert.assertEquals(DATASOURCE, report.getId());

    RocketMQSupervisorReportPayload payload = report.getPayload();

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
    Assert.assertEquals(ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 1L, PartitionUtil.genPartition(brokerName, 1), 2L, PartitionUtil.genPartition(brokerName, 2), 3L), activeReport.getStartingOffsets());
    Assert.assertEquals(ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 4L, PartitionUtil.genPartition(brokerName, 1), 5L, PartitionUtil.genPartition(brokerName, 2), 6L), activeReport.getCurrentOffsets());
    Assert.assertEquals(ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 3L, PartitionUtil.genPartition(brokerName, 1), 2L, PartitionUtil.genPartition(brokerName, 2), 1L), activeReport.getLag());

    Assert.assertEquals("id1", publishingReport.getId());
    Assert.assertEquals(ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 0L, PartitionUtil.genPartition(brokerName, 1), 0L, PartitionUtil.genPartition(brokerName, 2), 0L), publishingReport.getStartingOffsets());
    Assert.assertEquals(ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 1L, PartitionUtil.genPartition(brokerName, 1), 2L, PartitionUtil.genPartition(brokerName, 2), 3L), publishingReport.getCurrentOffsets());
    Assert.assertNull(publishingReport.getLag());

    Assert.assertEquals(ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 7L, PartitionUtil.genPartition(brokerName, 1), 7L, PartitionUtil.genPartition(brokerName, 2), 7L), payload.getLatestOffsets());
    Assert.assertEquals(ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 3L, PartitionUtil.genPartition(brokerName, 1), 2L, PartitionUtil.genPartition(brokerName, 2), 1L), payload.getMinimumLag());
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
        new RocketMQDataSourceMetadata(
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

    TreeMap<Integer, Map<String, Long>> checkpoints1 = new TreeMap<>();
    checkpoints1.put(0, ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 0L, PartitionUtil.genPartition(brokerName, 2), 0L));
    TreeMap<Integer, Map<String, Long>> checkpoints2 = new TreeMap<>();
    checkpoints2.put(0, ImmutableMap.of(PartitionUtil.genPartition(brokerName, 1), 0L));
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
        new RocketMQDataSourceMetadata(
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

    TreeMap<Integer, Map<String, Long>> checkpoints1 = new TreeMap<>();
    checkpoints1.put(0, ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 0L, PartitionUtil.genPartition(brokerName, 2), 0L));
    TreeMap<Integer, Map<String, Long>> checkpoints2 = new TreeMap<>();
    checkpoints2.put(0, ImmutableMap.of(PartitionUtil.genPartition(brokerName, 1), 0L));
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
      RocketMQIndexTaskIOConfig taskConfig = ((RocketMQIndexTask) task).getIOConfig();
      Assert.assertEquals(0L, (long) taskConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap().get(PartitionUtil.genPartition(brokerName, 0)));
      Assert.assertEquals(0L, (long) taskConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap().get(PartitionUtil.genPartition(brokerName, 2)));
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
        new RocketMQDataSourceMetadata(
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

    TreeMap<Integer, Map<String, Long>> checkpoints1 = new TreeMap<>();
    checkpoints1.put(0, ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 0L, PartitionUtil.genPartition(brokerName, 2), 0L));
    TreeMap<Integer, Map<String, Long>> checkpoints2 = new TreeMap<>();
    checkpoints2.put(0, ImmutableMap.of(PartitionUtil.genPartition(brokerName, 1), 0L));
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
        .andReturn(Futures.immediateFuture(ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 10L, PartitionUtil.genPartition(brokerName, 1), 20L, PartitionUtil.genPartition(brokerName, 2), 30L)))
        .andReturn(Futures.immediateFuture(ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 10L, PartitionUtil.genPartition(brokerName, 1), 15L, PartitionUtil.genPartition(brokerName, 2), 35L)));
    EasyMock.expect(
        taskClient.setEndOffsetsAsync(
            EasyMock.contains("sequenceName-0"),
            EasyMock.eq(ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 10L, PartitionUtil.genPartition(brokerName, 1), 20L, PartitionUtil.genPartition(brokerName, 2), 35L)),
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
      RocketMQIndexTaskIOConfig taskConfig = ((RocketMQIndexTask) task).getIOConfig();
      Assert.assertEquals(0L, (long) taskConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap().get(PartitionUtil.genPartition(brokerName, 0)));
      Assert.assertEquals(0L, (long) taskConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap().get(PartitionUtil.genPartition(brokerName, 2)));
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
    taskRunner.unregisterListener(StringUtils.format("RocketMQSupervisor-%s", DATASOURCE));
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
    final RocketMQSupervisorTuningConfig tuningConfig = supervisor.getTuningConfig();
    addSomeEvents(1);

    Task id1 = createRocketMQIndexTask(
        "id1",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>("topic", ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 0L, PartitionUtil.genPartition(brokerName, 1), 0L, PartitionUtil.genPartition(brokerName, 2), 0L), ImmutableSet.of()),
        new SeekableStreamEndSequenceNumbers<>(
            "topic",
            ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), Long.MAX_VALUE, PartitionUtil.genPartition(brokerName, 1), Long.MAX_VALUE, PartitionUtil.genPartition(brokerName, 2), Long.MAX_VALUE)
        ),
        null,
        null,
        tuningConfig
    );

    Task id2 = createRocketMQIndexTask(
        "id2",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>("topic", ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 10L, PartitionUtil.genPartition(brokerName, 1), 20L, PartitionUtil.genPartition(brokerName, 2), 30L), ImmutableSet.of()),
        new SeekableStreamEndSequenceNumbers<>(
            "topic",
            ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), Long.MAX_VALUE, PartitionUtil.genPartition(brokerName, 1), Long.MAX_VALUE, PartitionUtil.genPartition(brokerName, 2), Long.MAX_VALUE)
        ),
        null,
        null,
        tuningConfig
    );

    Task id3 = createRocketMQIndexTask(
        "id3",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>("topic", ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 10L, PartitionUtil.genPartition(brokerName, 1), 20L, PartitionUtil.genPartition(brokerName, 2), 30L), ImmutableSet.of()),
        new SeekableStreamEndSequenceNumbers<>(
            "topic",
            ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), Long.MAX_VALUE, PartitionUtil.genPartition(brokerName, 1), Long.MAX_VALUE, PartitionUtil.genPartition(brokerName, 2), Long.MAX_VALUE)
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
        new RocketMQDataSourceMetadata(
            null
        )
    ).anyTimes();
    EasyMock.expect(taskClient.getStatusAsync("id1")).andReturn(Futures.immediateFuture(Status.PUBLISHING));
    EasyMock.expect(taskClient.getStatusAsync("id2")).andReturn(Futures.immediateFuture(Status.READING));
    EasyMock.expect(taskClient.getStatusAsync("id3")).andReturn(Futures.immediateFuture(Status.READING));
    EasyMock.expect(taskClient.getStartTimeAsync("id2")).andReturn(Futures.immediateFuture(startTime));
    EasyMock.expect(taskClient.getStartTimeAsync("id3")).andReturn(Futures.immediateFuture(startTime));
    EasyMock.expect(taskClient.getEndOffsets("id1")).andReturn(ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 10L, PartitionUtil.genPartition(brokerName, 1), 20L, PartitionUtil.genPartition(brokerName, 2), 30L));

    // getCheckpoints will not be called for id1 as it is in publishing state
    TreeMap<Integer, Map<String, Long>> checkpoints = new TreeMap<>();
    checkpoints.put(0, ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 10L, PartitionUtil.genPartition(brokerName, 1), 20L, PartitionUtil.genPartition(brokerName, 2), 30L));
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
        .andReturn(Futures.immediateFuture(ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 15L, PartitionUtil.genPartition(brokerName, 1), 25L, PartitionUtil.genPartition(brokerName, 2), 30L)));
    EasyMock.expect(taskClient.setEndOffsetsAsync("id2", ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 15L, PartitionUtil.genPartition(brokerName, 1), 25L, PartitionUtil.genPartition(brokerName, 2), 30L), true))
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

    RocketMQDataSourceMetadata rocketmqDataSourceMetadata = new RocketMQDataSourceMetadata(
        new SeekableStreamStartSequenceNumbers<>(
            topic,
            ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 1000L, PartitionUtil.genPartition(brokerName, 1), 1000L, PartitionUtil.genPartition(brokerName, 2), 1000L),
            ImmutableSet.of()
        )
    );

    RocketMQDataSourceMetadata resetMetadata = new RocketMQDataSourceMetadata(
        new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(PartitionUtil.genPartition(brokerName, 1), 1000L, PartitionUtil.genPartition(brokerName, 2), 1000L), ImmutableSet.of())
    );

    RocketMQDataSourceMetadata expectedMetadata = new RocketMQDataSourceMetadata(
        new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 1000L), ImmutableSet.of()));

    EasyMock.reset(indexerMetadataStorageCoordinator);
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE))
        .andReturn(rocketmqDataSourceMetadata);
    EasyMock.expect(indexerMetadataStorageCoordinator.resetDataSourceMetadata(
        EasyMock.capture(captureDataSource),
        EasyMock.capture(captureDataSourceMetadata)
    )).andReturn(true);
    EasyMock.replay(indexerMetadataStorageCoordinator);

    try {
      supervisor.resetInternal(resetMetadata);
    } catch (NullPointerException npe) {
      // Expected as there will be an attempt to EasyMock.reset partitionGroups offsets to NOT_SET
      // however there would be no entries in the map as we have not put nay data in rocketmq
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

    RocketMQDataSourceMetadata resetMetadata = new RocketMQDataSourceMetadata(
        new SeekableStreamStartSequenceNumbers<>(
            topic,
            ImmutableMap.of(PartitionUtil.genPartition(brokerName, 1), 1000L, PartitionUtil.genPartition(brokerName, 2), 1000L),
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
            new RocketMQDataSourceMetadata(
                new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(PartitionUtil.genPartition(brokerName, 1), -100L, PartitionUtil.genPartition(brokerName, 2), 200L))
            )
        ).times(4);
    // getOffsetFromStorageForPartition() throws an exception when the offsets are automatically reset.
    // Since getOffsetFromStorageForPartition() is called per partition, all partitions can't be reset at the same time.
    // Instead, subsequent partitions will be reset in the following supervisor runs.
    EasyMock.expect(
        indexerMetadataStorageCoordinator.resetDataSourceMetadata(
            DATASOURCE,
            new RocketMQDataSourceMetadata(
                // Only one partition is reset in a single supervisor run.
                new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(PartitionUtil.genPartition(brokerName, 2), 200L))
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
    final RocketMQSupervisorTuningConfig tuningConfig = supervisor.getTuningConfig();
    addSomeEvents(1);

    Task id1 = createRocketMQIndexTask(
        "id1",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>("topic", ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 0L, PartitionUtil.genPartition(brokerName, 1), 0L, PartitionUtil.genPartition(brokerName, 2), 0L), ImmutableSet.of()),
        new SeekableStreamEndSequenceNumbers<>(
            "topic",
            ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), Long.MAX_VALUE, PartitionUtil.genPartition(brokerName, 1), Long.MAX_VALUE, PartitionUtil.genPartition(brokerName, 2), Long.MAX_VALUE)
        ),
        null,
        null,
        tuningConfig
    );

    Task id2 = createRocketMQIndexTask(
        "id2",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>("topic", ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 10L, PartitionUtil.genPartition(brokerName, 0), 20L, PartitionUtil.genPartition(brokerName, 2), 30L), ImmutableSet.of()),
        new SeekableStreamEndSequenceNumbers<>(
            "topic",
            ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), Long.MAX_VALUE, PartitionUtil.genPartition(brokerName, 1), Long.MAX_VALUE, PartitionUtil.genPartition(brokerName, 2), Long.MAX_VALUE)
        ),
        null,
        null,
        tuningConfig
    );

    Task id3 = createRocketMQIndexTask(
        "id3",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>("topic", ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 10L, PartitionUtil.genPartition(brokerName, 0), 20L, PartitionUtil.genPartition(brokerName, 2), 30L), ImmutableSet.of()),
        new SeekableStreamEndSequenceNumbers<>(
            "topic",
            ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), Long.MAX_VALUE, PartitionUtil.genPartition(brokerName, 1), Long.MAX_VALUE, PartitionUtil.genPartition(brokerName, 2), Long.MAX_VALUE)
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
        new RocketMQDataSourceMetadata(
            null
        )
    ).anyTimes();
    EasyMock.expect(taskClient.getStatusAsync("id1")).andReturn(Futures.immediateFuture(Status.PUBLISHING));
    EasyMock.expect(taskClient.getStatusAsync("id2")).andReturn(Futures.immediateFuture(Status.READING));
    EasyMock.expect(taskClient.getStatusAsync("id3")).andReturn(Futures.immediateFuture(Status.READING));
    EasyMock.expect(taskClient.getStartTimeAsync("id2")).andReturn(Futures.immediateFuture(startTime));
    EasyMock.expect(taskClient.getStartTimeAsync("id3")).andReturn(Futures.immediateFuture(startTime));
    EasyMock.expect(taskClient.getEndOffsets("id1")).andReturn(ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 10L, PartitionUtil.genPartition(brokerName, 1), 20L, PartitionUtil.genPartition(brokerName, 2), 30L));

    TreeMap<Integer, Map<String, Long>> checkpoints = new TreeMap<>();
    checkpoints.put(0, ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 10L, PartitionUtil.genPartition(brokerName, 1), 20L, PartitionUtil.genPartition(brokerName, 2), 30L));
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
    final RocketMQSupervisorTuningConfig tuningConfig = supervisor.getTuningConfig();
    supervisor.getStateManager().markRunFinished();

    //not adding any events
    Task id1 = createRocketMQIndexTask(
        "id1",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>("topic", ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 0L, PartitionUtil.genPartition(brokerName, 1), 0L, PartitionUtil.genPartition(brokerName, 2), 0L), ImmutableSet.of()),
        new SeekableStreamEndSequenceNumbers<>(
            "topic",
            ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), Long.MAX_VALUE, PartitionUtil.genPartition(brokerName, 1), Long.MAX_VALUE, PartitionUtil.genPartition(brokerName, 2), Long.MAX_VALUE)
        ),
        null,
        null,
        tuningConfig
    );

    Task id2 = createRocketMQIndexTask(
        "id2",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>("topic", ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 10L, PartitionUtil.genPartition(brokerName, 1), 20L, PartitionUtil.genPartition(brokerName, 2), 30L), ImmutableSet.of()),
        new SeekableStreamEndSequenceNumbers<>(
            "topic",
            ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), Long.MAX_VALUE, PartitionUtil.genPartition(brokerName, 1), Long.MAX_VALUE, PartitionUtil.genPartition(brokerName, 2), Long.MAX_VALUE)
        ),
        null,
        null,
        tuningConfig
    );

    Task id3 = createRocketMQIndexTask(
        "id3",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>("topic", ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 10L, PartitionUtil.genPartition(brokerName, 1), 20L, PartitionUtil.genPartition(brokerName, 2), 30L), ImmutableSet.of()),
        new SeekableStreamEndSequenceNumbers<>(
            "topic",
            ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), Long.MAX_VALUE, PartitionUtil.genPartition(brokerName, 1), Long.MAX_VALUE, PartitionUtil.genPartition(brokerName, 2), Long.MAX_VALUE)
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
        new RocketMQDataSourceMetadata(
            null
        )
    ).anyTimes();
    EasyMock.expect(taskClient.getStatusAsync("id1")).andReturn(Futures.immediateFuture(Status.READING));
    EasyMock.expect(taskClient.getStatusAsync("id2")).andReturn(Futures.immediateFuture(Status.READING));
    EasyMock.expect(taskClient.getStatusAsync("id3")).andReturn(Futures.immediateFuture(Status.READING));
    EasyMock.expect(taskClient.getStartTimeAsync("id1")).andReturn(Futures.immediateFuture(startTime));
    EasyMock.expect(taskClient.getStartTimeAsync("id2")).andReturn(Futures.immediateFuture(startTime));
    EasyMock.expect(taskClient.getStartTimeAsync("id3")).andReturn(Futures.immediateFuture(startTime));

    TreeMap<Integer, Map<String, Long>> checkpoints = new TreeMap<>();
    checkpoints.put(0, ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 10L, PartitionUtil.genPartition(brokerName, 1), 20L, PartitionUtil.genPartition(brokerName, 2), 30L));
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
    final RocketMQSupervisorTuningConfig tuningConfig = supervisor.getTuningConfig();
    supervisor.getStateManager().markRunFinished();

    //not adding any events
    final RocketMQIndexTask id1 = createRocketMQIndexTask(
        "id1",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 0L, PartitionUtil.genPartition(brokerName, 1), 0L, PartitionUtil.genPartition(brokerName, 2), 0L), ImmutableSet.of()),
        new SeekableStreamEndSequenceNumbers<>(
            topic,
            ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), Long.MAX_VALUE, PartitionUtil.genPartition(brokerName, 1), Long.MAX_VALUE, PartitionUtil.genPartition(brokerName, 2), Long.MAX_VALUE)
        ),
        null,
        null,
        tuningConfig
    );

    final Task id2 = createRocketMQIndexTask(
        "id2",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 10L, PartitionUtil.genPartition(brokerName, 1), 20L, PartitionUtil.genPartition(brokerName, 2), 30L), ImmutableSet.of()),
        new SeekableStreamEndSequenceNumbers<>(
            topic,
            ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), Long.MAX_VALUE, PartitionUtil.genPartition(brokerName, 1), Long.MAX_VALUE, PartitionUtil.genPartition(brokerName, 2), Long.MAX_VALUE)
        ),
        null,
        null,
        tuningConfig
    );

    final Task id3 = createRocketMQIndexTask(
        "id3",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 10L, PartitionUtil.genPartition(brokerName, 1), 20L, PartitionUtil.genPartition(brokerName, 2), 30L), ImmutableSet.of()),
        new SeekableStreamEndSequenceNumbers<>(
            topic,
            ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), Long.MAX_VALUE, PartitionUtil.genPartition(brokerName, 1), Long.MAX_VALUE, PartitionUtil.genPartition(brokerName, 2), Long.MAX_VALUE)
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
        indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(new RocketMQDataSourceMetadata(null)
    ).anyTimes();
    EasyMock.expect(taskClient.getStatusAsync("id1")).andReturn(Futures.immediateFuture(Status.READING));
    EasyMock.expect(taskClient.getStatusAsync("id2")).andReturn(Futures.immediateFuture(Status.READING));
    EasyMock.expect(taskClient.getStatusAsync("id3")).andReturn(Futures.immediateFuture(Status.READING));

    final DateTime startTime = DateTimes.nowUtc();
    EasyMock.expect(taskClient.getStartTimeAsync("id1")).andReturn(Futures.immediateFuture(startTime));
    EasyMock.expect(taskClient.getStartTimeAsync("id2")).andReturn(Futures.immediateFuture(startTime));
    EasyMock.expect(taskClient.getStartTimeAsync("id3")).andReturn(Futures.immediateFuture(startTime));

    final TreeMap<Integer, Map<String, Long>> checkpoints = new TreeMap<>();
    checkpoints.put(0, ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 10L, PartitionUtil.genPartition(brokerName, 1), 20L, PartitionUtil.genPartition(brokerName, 2), 30L));
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
        new RocketMQDataSourceMetadata(
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
    final RocketMQSupervisorTuningConfig tuningConfig = supervisor.getTuningConfig();
    //not adding any events
    final RocketMQIndexTask id1 = createRocketMQIndexTask(
        "id1",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 0L, PartitionUtil.genPartition(brokerName, 1), 0L, PartitionUtil.genPartition(brokerName, 2), 0L), ImmutableSet.of()),
        new SeekableStreamEndSequenceNumbers<>(
            topic,
            ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), Long.MAX_VALUE, PartitionUtil.genPartition(brokerName, 1), Long.MAX_VALUE, PartitionUtil.genPartition(brokerName, 2), Long.MAX_VALUE)
        ),
        null,
        null,
        tuningConfig
    );

    final Task id2 = createRocketMQIndexTask(
        "id2",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 10L, PartitionUtil.genPartition(brokerName, 1), 20L, PartitionUtil.genPartition(brokerName, 2), 30L), ImmutableSet.of()),
        new SeekableStreamEndSequenceNumbers<>(
            topic,
            ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), Long.MAX_VALUE, PartitionUtil.genPartition(brokerName, 1), Long.MAX_VALUE, PartitionUtil.genPartition(brokerName, 2), Long.MAX_VALUE)
        ),
        null,
        null,
        tuningConfig
    );

    final Task id3 = createRocketMQIndexTask(
        "id3",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 10L, PartitionUtil.genPartition(brokerName, 1), 20L, PartitionUtil.genPartition(brokerName, 2), 30L), ImmutableSet.of()),
        new SeekableStreamEndSequenceNumbers<>(
            topic,
            ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), Long.MAX_VALUE, PartitionUtil.genPartition(brokerName, 1), Long.MAX_VALUE, PartitionUtil.genPartition(brokerName, 2), Long.MAX_VALUE)
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
        indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(new RocketMQDataSourceMetadata(null)
    ).anyTimes();

    replayAll();

    supervisor.start();

    supervisor.checkpoint(
        0,
        new RocketMQDataSourceMetadata(
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
        serviceEmitter.getStackTrace().startsWith("org.apache.druid.java.util.common.ISE: Cannot find")
    );
    Assert.assertEquals(
        "Cannot find taskGroup [0] among all activelyReadingTaskGroups [{}]",
        serviceEmitter.getExceptionMessage()
    );
    Assert.assertEquals(ISE.class, serviceEmitter.getExceptionClass());
  }

  @Test
  public void testSuspendedNoRunningTasks() throws Exception
  {
    supervisor = getTestableSupervisor(1, 1, true, "PT1H", null, null, true, rocketmqHost);
    addSomeEvents(1);

    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new RocketMQDataSourceMetadata(
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

    supervisor = getTestableSupervisor(2, 1, true, "PT1H", null, null, true, rocketmqHost);
    final RocketMQSupervisorTuningConfig tuningConfig = supervisor.getTuningConfig();
    addSomeEvents(1);

    Task id1 = createRocketMQIndexTask(
        "id1",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>("topic", ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 0L, PartitionUtil.genPartition(brokerName, 1), 0L, PartitionUtil.genPartition(brokerName, 2), 0L), ImmutableSet.of()),
        new SeekableStreamEndSequenceNumbers<>(
            "topic",
            ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), Long.MAX_VALUE, PartitionUtil.genPartition(brokerName, 1), Long.MAX_VALUE, PartitionUtil.genPartition(brokerName, 2), Long.MAX_VALUE)
        ),
        null,
        null,
        tuningConfig
    );

    Task id2 = createRocketMQIndexTask(
        "id2",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>("topic", ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 10L, PartitionUtil.genPartition(brokerName, 1), 20L, PartitionUtil.genPartition(brokerName, 2), 30L), ImmutableSet.of()),
        new SeekableStreamEndSequenceNumbers<>(
            "topic",
            ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), Long.MAX_VALUE, PartitionUtil.genPartition(brokerName, 1), Long.MAX_VALUE, PartitionUtil.genPartition(brokerName, 2), Long.MAX_VALUE)
        ),
        null,
        null,
        tuningConfig
    );

    Task id3 = createRocketMQIndexTask(
        "id3",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>("topic", ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 10L, PartitionUtil.genPartition(brokerName, 1), 20L, PartitionUtil.genPartition(brokerName, 2), 30L), ImmutableSet.of()),
        new SeekableStreamEndSequenceNumbers<>(
            "topic",
            ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), Long.MAX_VALUE, PartitionUtil.genPartition(brokerName, 1), Long.MAX_VALUE, PartitionUtil.genPartition(brokerName, 2), Long.MAX_VALUE)
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
        new RocketMQDataSourceMetadata(
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
    EasyMock.expect(taskClient.getEndOffsets("id1")).andReturn(ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 10L, PartitionUtil.genPartition(brokerName, 1), 20L, PartitionUtil.genPartition(brokerName, 2), 30L));

    // getCheckpoints will not be called for id1 as it is in publishing state
    TreeMap<Integer, Map<String, Long>> checkpoints = new TreeMap<>();
    checkpoints.put(0, ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 10L, PartitionUtil.genPartition(brokerName, 1), 20L, PartitionUtil.genPartition(brokerName, 2), 30L));
    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("id2"), EasyMock.anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints))
        .times(1);
    EasyMock.expect(taskClient.getCheckpointsAsync(EasyMock.contains("id3"), EasyMock.anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints))
        .times(1);

    taskRunner.registerListener(EasyMock.anyObject(TaskRunnerListener.class), EasyMock.anyObject(Executor.class));

    EasyMock.expect(taskClient.pauseAsync("id2"))
        .andReturn(Futures.immediateFuture(ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 15L, PartitionUtil.genPartition(brokerName, 1), 25L, PartitionUtil.genPartition(brokerName, 2), 30L)));
    EasyMock.expect(taskClient.setEndOffsetsAsync("id2", ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 15L, PartitionUtil.genPartition(brokerName, 1), 25L, PartitionUtil.genPartition(brokerName, 2), 30L), true))
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

    supervisor = getTestableSupervisor(1, 1, true, "PT1H", null, null, true, rocketmqHost);
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
        StringUtils.format("badhostname:%d", rocketmqServer.getPort())
    );
    final RocketMQSupervisorTuningConfig tuningConfig = supervisor.getTuningConfig();
    addSomeEvents(1);

    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new RocketMQDataSourceMetadata(
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

    Capture<RocketMQIndexTask> captured = Capture.newInstance();
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasksByDatasource(DATASOURCE)).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(DATASOURCE)).andReturn(
        new RocketMQDataSourceMetadata(
            null
        )
    ).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.capture(captured))).andReturn(true);
    taskRunner.registerListener(EasyMock.anyObject(TaskRunnerListener.class), EasyMock.anyObject(Executor.class));
    replayAll();

    // Fix the bad hostname during the initialization retries and finish the supervisor start.
    // This is equivalent to supervisor.start() in testNoInitialState().
    // The test supervisor has a P1D period, so we need to manually trigger the initialization retry.
    supervisor.getIoConfig().getConsumerProperties().put("nameserver.url", rocketmqHost);
    supervisor.tryInit();

    Assert.assertTrue(supervisor.isLifecycleStarted());
    Assert.assertTrue(supervisor.isStarted());

    supervisor.runInternal();
    verifyAll();

    RocketMQIndexTask task = captured.getValue();
    Assert.assertEquals(dataSchema, task.getDataSchema());
    Assert.assertEquals(tuningConfig.convertToTaskTuningConfig(), task.getTuningConfig());

    RocketMQIndexTaskIOConfig taskConfig = task.getIOConfig();
    Assert.assertEquals(rocketmqHost, taskConfig.getConsumerProperties().get("nameserver.url"));
    Assert.assertEquals("myCustomValue", taskConfig.getConsumerProperties().get("myCustomKey"));
    Assert.assertEquals("sequenceName-0", taskConfig.getBaseSequenceName());
    Assert.assertTrue("isUseTransaction", taskConfig.isUseTransaction());
    Assert.assertFalse("minimumMessageTime", taskConfig.getMinimumMessageTime().isPresent());
    Assert.assertFalse("maximumMessageTime", taskConfig.getMaximumMessageTime().isPresent());

    Assert.assertEquals(topic, taskConfig.getStartSequenceNumbers().getStream());
    Assert.assertEquals(
        0L,
        taskConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap().get(PartitionUtil.genPartition(brokerName, 0)).longValue()
    );
    Assert.assertEquals(
        0L,
        taskConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap().get(PartitionUtil.genPartition(brokerName, 1)).longValue()
    );
    Assert.assertEquals(
        0L,
        taskConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap().get(PartitionUtil.genPartition(brokerName, 2)).longValue()
    );

    Assert.assertEquals(topic, taskConfig.getEndSequenceNumbers().getStream());
    Assert.assertEquals(
        Long.MAX_VALUE,
        taskConfig.getEndSequenceNumbers().getPartitionSequenceNumberMap().get(PartitionUtil.genPartition(brokerName, 0)).longValue()
    );
    Assert.assertEquals(
        Long.MAX_VALUE,
        taskConfig.getEndSequenceNumbers().getPartitionSequenceNumberMap().get(PartitionUtil.genPartition(brokerName, 1)).longValue()
    );
    Assert.assertEquals(
        Long.MAX_VALUE,
        taskConfig.getEndSequenceNumbers().getPartitionSequenceNumberMap().get(PartitionUtil.genPartition(brokerName, 2)).longValue()
    );
  }

  @Test
  public void testGetCurrentTotalStats()
  {
    supervisor = getTestableSupervisor(1, 2, true, "PT1H", null, null, false, rocketmqHost);
    supervisor.addTaskGroupToActivelyReadingTaskGroup(
        supervisor.getTaskGroupIdForPartition(PartitionUtil.genPartition(brokerName, 0)),
        ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 0L),
        Optional.absent(),
        Optional.absent(),
        ImmutableSet.of("task1"),
        ImmutableSet.of()
    );

    supervisor.addTaskGroupToPendingCompletionTaskGroup(
        supervisor.getTaskGroupIdForPartition(PartitionUtil.genPartition(brokerName, 1)),
        ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 0L),
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

    Task task = createRocketMQIndexTask(
        "id1",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>(
            "topic",
            ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 0L, PartitionUtil.genPartition(brokerName, 2), 0L),
            ImmutableSet.of()
        ),
        new SeekableStreamEndSequenceNumbers<>(
            "topic",
            ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), Long.MAX_VALUE, PartitionUtil.genPartition(brokerName, 2), Long.MAX_VALUE)
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
        new RocketMQDataSourceMetadata(
            null
        )
    ).anyTimes();

    taskRunner.registerListener(EasyMock.anyObject(TaskRunnerListener.class), EasyMock.anyObject(Executor.class));
    EasyMock.expect(taskQueue.add(EasyMock.anyObject(Task.class))).andReturn(true);

    TreeMap<Integer, Map<String, Long>> checkpoints1 = new TreeMap<>();
    checkpoints1.put(0, ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 0L, PartitionUtil.genPartition(brokerName, 2), 0L));

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

    Task task = createRocketMQIndexTask(
        "id1",
        DATASOURCE,
        0,
        new SeekableStreamStartSequenceNumbers<>(
            "topic",
            ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 0L, PartitionUtil.genPartition(brokerName, 2), 0L),
            ImmutableSet.of()
        ),
        new SeekableStreamEndSequenceNumbers<>("topic", ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), Long.MAX_VALUE, PartitionUtil.genPartition(brokerName, 2), Long.MAX_VALUE)),
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
        new RocketMQDataSourceMetadata(
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

    RocketMQSupervisor supervisor = getSupervisor(
        2,
        1,
        true,
        "PT1H",
        new Period("P1D"),
        new Period("P1D"),
        false,
        rocketmqHost,
        dataSchema,
        new RocketMQSupervisorTuningConfig(
            null,
            1000,
            null,
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
        ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 0L, PartitionUtil.genPartition(brokerName, 2), 0L),
        Optional.of(minMessageTime),
        Optional.of(maxMessageTime),
        ImmutableSet.of("id1", "id2", "id3", "id4"),
        ImmutableSet.of()
    );

    DataSchema modifiedDataSchema = getDataSchema("some other datasource");

    RocketMQSupervisorTuningConfig modifiedTuningConfig = new RocketMQSupervisorTuningConfig(
        null,
        42, // This is different
        null,
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

    RocketMQIndexTask taskFromStorage = createRocketMQIndexTask(
        "id1",
        0,
        new SeekableStreamStartSequenceNumbers<>(
            "topic",
            ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 0L, PartitionUtil.genPartition(brokerName, 2), 0L),
            ImmutableSet.of()
        ),
        new SeekableStreamEndSequenceNumbers<>(
            "topic",
            ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), Long.MAX_VALUE, PartitionUtil.genPartition(brokerName, 2), Long.MAX_VALUE)
        ),
        minMessageTime,
        maxMessageTime,
        dataSchema,
        supervisor.getTuningConfig()
    );

    RocketMQIndexTask taskFromStorageMismatchedDataSchema = createRocketMQIndexTask(
        "id2",
        0,
        new SeekableStreamStartSequenceNumbers<>(
            "topic",
            ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 0L, PartitionUtil.genPartition(brokerName, 2), 0L),
            ImmutableSet.of()
        ),
        new SeekableStreamEndSequenceNumbers<>(
            "topic",
            ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), Long.MAX_VALUE, PartitionUtil.genPartition(brokerName, 2), Long.MAX_VALUE)
        ),
        minMessageTime,
        maxMessageTime,
        modifiedDataSchema,
        supervisor.getTuningConfig()
    );

    RocketMQIndexTask taskFromStorageMismatchedTuningConfig = createRocketMQIndexTask(
        "id3",
        0,
        new SeekableStreamStartSequenceNumbers<>(
            "topic",
            ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 0L, PartitionUtil.genPartition(brokerName, 2), 0L),
            ImmutableSet.of()
        ),
        new SeekableStreamEndSequenceNumbers<>(
            "topic",
            ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), Long.MAX_VALUE, PartitionUtil.genPartition(brokerName, 2), Long.MAX_VALUE)
        ),
        minMessageTime,
        maxMessageTime,
        dataSchema,
        modifiedTuningConfig
    );

    RocketMQIndexTask taskFromStorageMismatchedPartitionsWithTaskGroup = createRocketMQIndexTask(
        "id4",
        0,
        new SeekableStreamStartSequenceNumbers<>(
            "topic",
            ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), 0L, PartitionUtil.genPartition(brokerName, 2), 6L),
            ImmutableSet.of()
        ),
        new SeekableStreamEndSequenceNumbers<>(
            "topic",
            ImmutableMap.of(PartitionUtil.genPartition(brokerName, 0), Long.MAX_VALUE, PartitionUtil.genPartition(brokerName, 2), Long.MAX_VALUE)
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
    final DefaultMQProducer producer = rocketmqServer.newProducer();
    producer.start();
    for (int i = NUM_PARTITIONS; i < numEventsPerPartition; i++) {
      for (int j = 0; j < numEventsPerPartition; j++) {
        producer.send(new Message(topic, "TagA", StringUtils.toUtf8(StringUtils.format("event-%d", j))), new MessageQueue(topic, brokerName, i)).getMsgId();
      }
    }
    producer.shutdown();
  }

  private void addMoreEvents(int numEventsPerPartition, int num_partitions) throws Exception
  {
    final DefaultMQProducer producer = rocketmqServer.newProducer();
    producer.start();
    for (int i = NUM_PARTITIONS; i < num_partitions; i++) {
      for (int j = 0; j < numEventsPerPartition; j++) {
        producer.send(new Message(topic, "TagA", StringUtils.toUtf8(StringUtils.format("event-%d", j))), new MessageQueue(topic, brokerName, i)).getMsgId();
      }
    }
    producer.shutdown();
  }


  private TestableRocketMQSupervisor getTestableSupervisor(
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

  private TestableRocketMQSupervisor getTestableSupervisor(
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
        rocketmqHost
    );
  }

  private TestableRocketMQSupervisor getTestableSupervisor(
      int replicas,
      int taskCount,
      boolean useEarliestOffset,
      String duration,
      Period lateMessageRejectionPeriod,
      Period earlyMessageRejectionPeriod,
      boolean suspended,
      String rocketmqHost
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
        rocketmqHost
    );
  }

  private TestableRocketMQSupervisor getTestableSupervisor(
      int replicas,
      int taskCount,
      boolean useEarliestOffset,
      boolean resetOffsetAutomatically,
      String duration,
      Period lateMessageRejectionPeriod,
      Period earlyMessageRejectionPeriod,
      boolean suspended,
      String rocketmqHost
  )
  {
    final Map<String, Object> consumerProperties = RocketMQConsumerConfigs.getConsumerProperties();
    consumerProperties.put("myCustomKey", "myCustomValue");
    consumerProperties.put("nameserver.url", rocketmqHost);
    RocketMQSupervisorIOConfig rocketmqSupervisorIOConfig = new RocketMQSupervisorIOConfig(
        topic,
        INPUT_FORMAT,
        replicas,
        taskCount,
        new Period(duration),
        consumerProperties,
        null,
        RocketMQSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
        new Period("P1D"),
        new Period("PT30S"),
        useEarliestOffset,
        new Period("PT30M"),
        lateMessageRejectionPeriod,
        earlyMessageRejectionPeriod,
        null
    );

    RocketMQIndexTaskClientFactory taskClientFactory = new RocketMQIndexTaskClientFactory(
        null,
        null
    )
    {
      @Override
      public RocketMQIndexTaskClient build(
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

    final RocketMQSupervisorTuningConfig tuningConfig = new RocketMQSupervisorTuningConfig(
        null,
        1000,
        null,
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

    return new TestableRocketMQSupervisor(
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        taskClientFactory,
        OBJECT_MAPPER,
        new RocketMQSupervisorSpec(
            null,
            dataSchema,
            tuningConfig,
            rocketmqSupervisorIOConfig,
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
  private TestableRocketMQSupervisor getTestableSupervisorCustomIsTaskCurrent(
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
    consumerProperties.put("nameserver.url", rocketmqHost);
    consumerProperties.put("isolation.level", "read_committed");
    RocketMQSupervisorIOConfig rocketmqSupervisorIOConfig = new RocketMQSupervisorIOConfig(
        topic,
        INPUT_FORMAT,
        replicas,
        taskCount,
        new Period(duration),
        consumerProperties,
        null,
        RocketMQSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
        new Period("P1D"),
        new Period("PT30S"),
        useEarliestOffset,
        new Period("PT30M"),
        lateMessageRejectionPeriod,
        earlyMessageRejectionPeriod,
        null
    );

    RocketMQIndexTaskClientFactory taskClientFactory = new RocketMQIndexTaskClientFactory(
        null,
        null
    )
    {
      @Override
      public RocketMQIndexTaskClient build(
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

    final RocketMQSupervisorTuningConfig tuningConfig = new RocketMQSupervisorTuningConfig(
        null,
        1000,
        null,
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

    return new TestableRocketMQSupervisorWithCustomIsTaskCurrent(
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        taskClientFactory,
        OBJECT_MAPPER,
        new RocketMQSupervisorSpec(
            null,
            dataSchema,
            tuningConfig,
            rocketmqSupervisorIOConfig,
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

  private RocketMQSupervisor getSupervisor(
      int replicas,
      int taskCount,
      boolean useEarliestOffset,
      String duration,
      Period lateMessageRejectionPeriod,
      Period earlyMessageRejectionPeriod,
      boolean suspended,
      String rocketmqHost,
      DataSchema dataSchema,
      RocketMQSupervisorTuningConfig tuningConfig
  )
  {
    Map<String, Object> consumerProperties = new HashMap<>();
    consumerProperties.put("myCustomKey", "myCustomValue");
    consumerProperties.put("nameserver.url", rocketmqHost);
    consumerProperties.put("isolation.level", "read_committed");
    RocketMQSupervisorIOConfig rocketmqSupervisorIOConfig = new RocketMQSupervisorIOConfig(
        topic,
        INPUT_FORMAT,
        replicas,
        taskCount,
        new Period(duration),
        consumerProperties,
        null,
        RocketMQSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
        new Period("P1D"),
        new Period("PT30S"),
        useEarliestOffset,
        new Period("PT30M"),
        lateMessageRejectionPeriod,
        earlyMessageRejectionPeriod,
        null
    );

    RocketMQIndexTaskClientFactory taskClientFactory = new RocketMQIndexTaskClientFactory(
        null,
        null
    )
    {
      @Override
      public RocketMQIndexTaskClient build(
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

    return new RocketMQSupervisor(
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        taskClientFactory,
        OBJECT_MAPPER,
        new RocketMQSupervisorSpec(
            null,
            dataSchema,
            tuningConfig,
            rocketmqSupervisorIOConfig,
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
        new AggregatorFactory[] {new CountAggregatorFactory("rows")},
        new UniformGranularitySpec(
            Granularities.HOUR,
            Granularities.NONE,
            ImmutableList.of()
        ),
        null
    );
  }

  private RocketMQIndexTask createRocketMQIndexTask(
      String id,
      String dataSource,
      int taskGroupId,
      SeekableStreamStartSequenceNumbers<String, Long> startPartitions,
      SeekableStreamEndSequenceNumbers<String, Long> endPartitions,
      DateTime minimumMessageTime,
      DateTime maximumMessageTime,
      RocketMQSupervisorTuningConfig tuningConfig
  )
  {
    return createRocketMQIndexTask(
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

  private RocketMQIndexTask createRocketMQIndexTask(
      String id,
      int taskGroupId,
      SeekableStreamStartSequenceNumbers<String, Long> startPartitions,
      SeekableStreamEndSequenceNumbers<String, Long> endPartitions,
      DateTime minimumMessageTime,
      DateTime maximumMessageTime,
      DataSchema schema,
      RocketMQSupervisorTuningConfig tuningConfig
  )
  {
    return createRocketMQIndexTask(
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

  private RocketMQIndexTask createRocketMQIndexTask(
      String id,
      int taskGroupId,
      SeekableStreamStartSequenceNumbers<String, Long> startPartitions,
      SeekableStreamEndSequenceNumbers<String, Long> endPartitions,
      DateTime minimumMessageTime,
      DateTime maximumMessageTime,
      DataSchema schema,
      RocketMQIndexTaskTuningConfig tuningConfig
  )
  {
    return new RocketMQIndexTask(
        id,
        null,
        schema,
        tuningConfig,
        new RocketMQIndexTaskIOConfig(
            taskGroupId,
            "sequenceName-" + taskGroupId,
            startPartitions,
            endPartitions,
            ImmutableMap.of("nameserver.url", rocketmqHost),
            RocketMQSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            minimumMessageTime,
            maximumMessageTime,
            INPUT_FORMAT
        ),
        Collections.emptyMap(),
        OBJECT_MAPPER
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

  private static class TestableRocketMQSupervisor extends RocketMQSupervisor
  {
    private final Map<String, Object> consumerProperties;

    public TestableRocketMQSupervisor(
        TaskStorage taskStorage,
        TaskMaster taskMaster,
        IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator,
        RocketMQIndexTaskClientFactory taskClientFactory,
        ObjectMapper mapper,
        RocketMQSupervisorSpec spec,
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
    protected RecordSupplier<String, Long, RocketMQRecordEntity> setupRecordSupplier()
    {
      final Map<String, Object> consumerConfigs = RocketMQConsumerConfigs.getConsumerProperties();
      consumerConfigs.put("metadata.max.age.ms", "1");
      DefaultLitePullConsumer consumer = new DefaultLitePullConsumer(consumerConfigs.get("consumer.group").toString());
      consumer.setNamesrvAddr(consumerProperties.get("nameserver.url").toString());
      return new RocketMQRecordSupplier(
          consumer
      );
    }

    @Override
    protected String generateSequenceName(
        Map<String, Long> startPartitions,
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

  private static class TestableRocketMQSupervisorWithCustomIsTaskCurrent extends TestableRocketMQSupervisor
  {
    private final boolean isTaskCurrentReturn;

    public TestableRocketMQSupervisorWithCustomIsTaskCurrent(
        TaskStorage taskStorage,
        TaskMaster taskMaster,
        IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator,
        RocketMQIndexTaskClientFactory taskClientFactory,
        ObjectMapper mapper,
        RocketMQSupervisorSpec spec,
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
