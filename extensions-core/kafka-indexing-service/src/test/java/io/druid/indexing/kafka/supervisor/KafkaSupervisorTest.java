/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexing.kafka.supervisor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.druid.java.util.common.StringUtils;
import io.druid.data.input.impl.DimensionSchema;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.JSONParseSpec;
import io.druid.data.input.impl.JSONPathFieldSpec;
import io.druid.data.input.impl.JSONPathSpec;
import io.druid.data.input.impl.StringDimensionSchema;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.indexing.common.TaskInfoProvider;
import io.druid.indexing.common.TaskLocation;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.task.RealtimeIndexTask;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.kafka.KafkaDataSourceMetadata;
import io.druid.indexing.kafka.KafkaIOConfig;
import io.druid.indexing.kafka.KafkaIndexTask;
import io.druid.indexing.kafka.KafkaIndexTaskClient;
import io.druid.indexing.kafka.KafkaIndexTaskClientFactory;
import io.druid.indexing.kafka.KafkaPartitions;
import io.druid.indexing.kafka.KafkaTuningConfig;
import io.druid.indexing.kafka.test.TestBroker;
import io.druid.indexing.overlord.DataSourceMetadata;
import io.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import io.druid.indexing.overlord.TaskMaster;
import io.druid.indexing.overlord.TaskQueue;
import io.druid.indexing.overlord.TaskRunner;
import io.druid.indexing.overlord.TaskRunnerListener;
import io.druid.indexing.overlord.TaskRunnerWorkItem;
import io.druid.indexing.overlord.TaskStorage;
import io.druid.indexing.overlord.supervisor.SupervisorReport;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.segment.TestHelper;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeIOConfig;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import io.druid.segment.realtime.FireDepartment;
import io.druid.server.metrics.DruidMonitorSchedulerConfig;
import io.druid.server.metrics.NoopServiceEmitter;
import org.apache.curator.test.TestingCluster;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.easymock.Capture;
import org.easymock.CaptureType;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;

@RunWith(Parameterized.class)
public class KafkaSupervisorTest extends EasyMockSupport
{
  private static final ObjectMapper objectMapper = TestHelper.getJsonMapper();
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

  private KafkaSupervisor supervisor;
  private KafkaSupervisorTuningConfig tuningConfig;
  private TaskStorage taskStorage;
  private TaskMaster taskMaster;
  private TaskRunner taskRunner;
  private IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator;
  private KafkaIndexTaskClient taskClient;
  private TaskQueue taskQueue;
  private String topic;

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
        ImmutableMap.of("num.partitions", String.valueOf(NUM_PARTITIONS))
    );
    kafkaServer.start();
    kafkaHost = StringUtils.format("localhost:%d", kafkaServer.getPort());

    dataSchema = getDataSchema(DATASOURCE);
  }

  @Before
  public void setupTest() throws Exception
  {
    taskStorage = createMock(TaskStorage.class);
    taskMaster = createMock(TaskMaster.class);
    taskRunner = createMock(TaskRunner.class);
    indexerMetadataStorageCoordinator = createMock(IndexerMetadataStorageCoordinator.class);
    taskClient = createMock(KafkaIndexTaskClient.class);
    taskQueue = createMock(TaskQueue.class);

    tuningConfig = new KafkaSupervisorTuningConfig(
        1000,
        50000,
        new Period("P1Y"),
        new File("/test"),
        null,
        null,
        true,
        false,
        null,
        null,
        numThreads,
        TEST_CHAT_THREADS,
        TEST_CHAT_RETRIES,
        TEST_HTTP_TIMEOUT,
        TEST_SHUTDOWN_TIMEOUT,
        null
    );

    topic = getTopic();
  }

  @After
  public void tearDownTest() throws Exception
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
  public void testNoInitialState() throws Exception
  {
    supervisor = getSupervisor(1, 1, true, "PT1H", null, false);
    addSomeEvents(1);

    Capture<KafkaIndexTask> captured = Capture.newInstance();
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.<Task>of()).anyTimes();
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
    Assert.assertEquals(KafkaTuningConfig.copyOf(tuningConfig), task.getTuningConfig());

    KafkaIOConfig taskConfig = task.getIOConfig();
    Assert.assertEquals(kafkaHost, taskConfig.getConsumerProperties().get("bootstrap.servers"));
    Assert.assertEquals("myCustomValue", taskConfig.getConsumerProperties().get("myCustomKey"));
    Assert.assertEquals("sequenceName-0", taskConfig.getBaseSequenceName());
    Assert.assertTrue("isUseTransaction", taskConfig.isUseTransaction());
    Assert.assertFalse("pauseAfterRead", taskConfig.isPauseAfterRead());
    Assert.assertFalse("minimumMessageTime", taskConfig.getMinimumMessageTime().isPresent());
    Assert.assertFalse("skipOffsetGaps", taskConfig.isSkipOffsetGaps());

    Assert.assertEquals(topic, taskConfig.getStartPartitions().getTopic());
    Assert.assertEquals(0L, (long) taskConfig.getStartPartitions().getPartitionOffsetMap().get(0));
    Assert.assertEquals(0L, (long) taskConfig.getStartPartitions().getPartitionOffsetMap().get(1));
    Assert.assertEquals(0L, (long) taskConfig.getStartPartitions().getPartitionOffsetMap().get(2));

    Assert.assertEquals(topic, taskConfig.getEndPartitions().getTopic());
    Assert.assertEquals(Long.MAX_VALUE, (long) taskConfig.getEndPartitions().getPartitionOffsetMap().get(0));
    Assert.assertEquals(Long.MAX_VALUE, (long) taskConfig.getEndPartitions().getPartitionOffsetMap().get(1));
    Assert.assertEquals(Long.MAX_VALUE, (long) taskConfig.getEndPartitions().getPartitionOffsetMap().get(2));
  }

  @Test
  public void testSkipOffsetGaps() throws Exception
  {
    supervisor = getSupervisor(1, 1, true, "PT1H", null, true);
    addSomeEvents(1);

    Capture<KafkaIndexTask> captured = Capture.newInstance();
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.<Task>of()).anyTimes();
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
    KafkaIOConfig taskConfig = task.getIOConfig();

    Assert.assertTrue("skipOffsetGaps", taskConfig.isSkipOffsetGaps());
  }

  @Test
  public void testMultiTask() throws Exception
  {
    supervisor = getSupervisor(1, 2, true, "PT1H", null, false);
    addSomeEvents(1);

    Capture<KafkaIndexTask> captured = Capture.newInstance(CaptureType.ALL);
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.<TaskRunner>absent()).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.<Task>of()).anyTimes();
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
    Assert.assertEquals(2, task1.getIOConfig().getStartPartitions().getPartitionOffsetMap().size());
    Assert.assertEquals(2, task1.getIOConfig().getEndPartitions().getPartitionOffsetMap().size());
    Assert.assertEquals(0L, (long) task1.getIOConfig().getStartPartitions().getPartitionOffsetMap().get(0));
    Assert.assertEquals(Long.MAX_VALUE, (long) task1.getIOConfig().getEndPartitions().getPartitionOffsetMap().get(0));
    Assert.assertEquals(0L, (long) task1.getIOConfig().getStartPartitions().getPartitionOffsetMap().get(2));
    Assert.assertEquals(Long.MAX_VALUE, (long) task1.getIOConfig().getEndPartitions().getPartitionOffsetMap().get(2));

    KafkaIndexTask task2 = captured.getValues().get(1);
    Assert.assertEquals(1, task2.getIOConfig().getStartPartitions().getPartitionOffsetMap().size());
    Assert.assertEquals(1, task2.getIOConfig().getEndPartitions().getPartitionOffsetMap().size());
    Assert.assertEquals(0L, (long) task2.getIOConfig().getStartPartitions().getPartitionOffsetMap().get(1));
    Assert.assertEquals(Long.MAX_VALUE, (long) task2.getIOConfig().getEndPartitions().getPartitionOffsetMap().get(1));
  }

  @Test
  public void testReplicas() throws Exception
  {
    supervisor = getSupervisor(2, 1, true, "PT1H", null, false);
    addSomeEvents(1);

    Capture<KafkaIndexTask> captured = Capture.newInstance(CaptureType.ALL);
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.<TaskRunner>absent()).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.<Task>of()).anyTimes();
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
    Assert.assertEquals(3, task1.getIOConfig().getStartPartitions().getPartitionOffsetMap().size());
    Assert.assertEquals(3, task1.getIOConfig().getEndPartitions().getPartitionOffsetMap().size());
    Assert.assertEquals(0L, (long) task1.getIOConfig().getStartPartitions().getPartitionOffsetMap().get(0));
    Assert.assertEquals(0L, (long) task1.getIOConfig().getStartPartitions().getPartitionOffsetMap().get(1));
    Assert.assertEquals(0L, (long) task1.getIOConfig().getStartPartitions().getPartitionOffsetMap().get(2));

    KafkaIndexTask task2 = captured.getValues().get(1);
    Assert.assertEquals(3, task2.getIOConfig().getStartPartitions().getPartitionOffsetMap().size());
    Assert.assertEquals(3, task2.getIOConfig().getEndPartitions().getPartitionOffsetMap().size());
    Assert.assertEquals(0L, (long) task2.getIOConfig().getStartPartitions().getPartitionOffsetMap().get(0));
    Assert.assertEquals(0L, (long) task2.getIOConfig().getStartPartitions().getPartitionOffsetMap().get(1));
    Assert.assertEquals(0L, (long) task2.getIOConfig().getStartPartitions().getPartitionOffsetMap().get(2));
  }

  @Test
  public void testLateMessageRejectionPeriod() throws Exception
  {
    supervisor = getSupervisor(2, 1, true, "PT1H", new Period("PT1H"), false);
    addSomeEvents(1);

    Capture<KafkaIndexTask> captured = Capture.newInstance(CaptureType.ALL);
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.<TaskRunner>absent()).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.<Task>of()).anyTimes();
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
  /**
   * Test generating the starting offsets from the partition high water marks in Kafka.
   */
  public void testLatestOffset() throws Exception
  {
    supervisor = getSupervisor(1, 1, false, "PT1H", null, false);
    addSomeEvents(1100);

    Capture<KafkaIndexTask> captured = Capture.newInstance();
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.<TaskRunner>absent()).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.<Task>of()).anyTimes();
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
    Assert.assertEquals(1100L, (long) task.getIOConfig().getStartPartitions().getPartitionOffsetMap().get(0));
    Assert.assertEquals(1100L, (long) task.getIOConfig().getStartPartitions().getPartitionOffsetMap().get(1));
    Assert.assertEquals(1100L, (long) task.getIOConfig().getStartPartitions().getPartitionOffsetMap().get(2));
  }

  @Test
  /**
   * Test generating the starting offsets from the partition data stored in druid_dataSource which contains the
   * offsets of the last built segments.
   */
  public void testDatasourceMetadata() throws Exception
  {
    supervisor = getSupervisor(1, 1, true, "PT1H", null, false);
    addSomeEvents(100);

    Capture<KafkaIndexTask> captured = Capture.newInstance();
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.<TaskRunner>absent()).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.<Task>of()).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            new KafkaPartitions(topic, ImmutableMap.of(0, 10L, 1, 20L, 2, 30L))
        )
    ).anyTimes();
    expect(taskQueue.add(capture(captured))).andReturn(true);
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    KafkaIndexTask task = captured.getValue();
    KafkaIOConfig taskConfig = task.getIOConfig();
    Assert.assertEquals("sequenceName-0", taskConfig.getBaseSequenceName());
    Assert.assertEquals(10L, (long) taskConfig.getStartPartitions().getPartitionOffsetMap().get(0));
    Assert.assertEquals(20L, (long) taskConfig.getStartPartitions().getPartitionOffsetMap().get(1));
    Assert.assertEquals(30L, (long) taskConfig.getStartPartitions().getPartitionOffsetMap().get(2));
  }

  @Test(expected = ISE.class)
  public void testBadMetadataOffsets() throws Exception
  {
    supervisor = getSupervisor(1, 1, true, "PT1H", null, false);
    addSomeEvents(1);

    expect(taskMaster.getTaskRunner()).andReturn(Optional.<TaskRunner>absent()).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.<Task>of()).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            new KafkaPartitions(topic, ImmutableMap.of(0, 10L, 1, 20L, 2, 30L))
        )
    ).anyTimes();
    replayAll();

    supervisor.start();
    supervisor.runInternal();
  }

  @Test
  public void testKillIncompatibleTasks() throws Exception
  {
    supervisor = getSupervisor(2, 1, true, "PT1H", null, false);
    addSomeEvents(1);

    Task id1 = createKafkaIndexTask( // unexpected # of partitions (kill)
                                     "id1",
                                     DATASOURCE,
                                     "index_kafka_testDS__some_other_sequenceName",
                                     new KafkaPartitions("topic", ImmutableMap.of(0, 0L)),
                                     new KafkaPartitions("topic", ImmutableMap.of(0, 10L)),
                                     null
    );

    Task id2 = createKafkaIndexTask( // correct number of partitions and ranges (don't kill)
                                     "id2",
                                     DATASOURCE,
                                     "sequenceName-0",
                                     new KafkaPartitions("topic", ImmutableMap.of(0, 0L, 1, 0L, 2, 0L)),
                                     new KafkaPartitions("topic", ImmutableMap.of(0, 333L, 1, 333L, 2, 333L)),
                                     null
    );

    Task id3 = createKafkaIndexTask( // unexpected range on partition 2 (kill)
                                     "id3",
                                     DATASOURCE,
                                     "index_kafka_testDS__some_other_sequenceName",
                                     new KafkaPartitions("topic", ImmutableMap.of(0, 0L, 1, 0L, 2, 1L)),
                                     new KafkaPartitions("topic", ImmutableMap.of(0, 333L, 1, 333L, 2, 330L)),
                                     null
    );

    Task id4 = createKafkaIndexTask( // different datasource (don't kill)
                                     "id4",
                                     "other-datasource",
                                     "index_kafka_testDS_d927edff33c4b3f",
                                     new KafkaPartitions("topic", ImmutableMap.of(0, 0L)),
                                     new KafkaPartitions("topic", ImmutableMap.of(0, 10L)),
                                     null
    );

    Task id5 = new RealtimeIndexTask( // non KafkaIndexTask (don't kill)
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
    expect(taskClient.getStatusAsync(anyString())).andReturn(Futures.immediateFuture(KafkaIndexTask.Status.NOT_STARTED))
                                                  .anyTimes();
    expect(taskClient.getStartTimeAsync(anyString())).andReturn(Futures.immediateFuture(DateTime.now())).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            null
        )
    ).anyTimes();
    expect(taskClient.stopAsync("id1", false)).andReturn(Futures.immediateFuture(true));
    expect(taskClient.stopAsync("id3", false)).andReturn(Futures.immediateFuture(false));
    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));
    taskQueue.shutdown("id3");

    expect(taskQueue.add(anyObject(Task.class))).andReturn(true);
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();
  }

  @Test
  public void testKillBadPartitionAssignment() throws Exception
  {
    supervisor = getSupervisor(1, 2, true, "PT1H", null, false);
    addSomeEvents(1);

    Task id1 = createKafkaIndexTask(
        "id1",
        DATASOURCE,
        "sequenceName-0",
        new KafkaPartitions("topic", ImmutableMap.of(0, 0L, 2, 0L)),
        new KafkaPartitions("topic", ImmutableMap.of(0, Long.MAX_VALUE, 2, Long.MAX_VALUE)),
        null
    );
    Task id2 = createKafkaIndexTask(
        "id2",
        DATASOURCE,
        "sequenceName-1",
        new KafkaPartitions("topic", ImmutableMap.of(1, 0L)),
        new KafkaPartitions("topic", ImmutableMap.of(1, Long.MAX_VALUE)),
        null
    );
    Task id3 = createKafkaIndexTask(
        "id3",
        DATASOURCE,
        "sequenceName-0",
        new KafkaPartitions("topic", ImmutableMap.of(0, 0L, 1, 0L, 2, 0L)),
        new KafkaPartitions("topic", ImmutableMap.of(0, Long.MAX_VALUE, 1, Long.MAX_VALUE, 2, Long.MAX_VALUE)),
        null
    );
    Task id4 = createKafkaIndexTask(
        "id4",
        DATASOURCE,
        "sequenceName-0",
        new KafkaPartitions("topic", ImmutableMap.of(0, 0L, 1, 0L)),
        new KafkaPartitions("topic", ImmutableMap.of(0, Long.MAX_VALUE, 1, Long.MAX_VALUE)),
        null
    );
    Task id5 = createKafkaIndexTask(
        "id5",
        DATASOURCE,
        "sequenceName-0",
        new KafkaPartitions("topic", ImmutableMap.of(1, 0L, 2, 0L)),
        new KafkaPartitions("topic", ImmutableMap.of(1, Long.MAX_VALUE, 2, Long.MAX_VALUE)),
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
    expect(taskStorage.getTask("id4")).andReturn(Optional.of(id3)).anyTimes();
    expect(taskStorage.getTask("id5")).andReturn(Optional.of(id3)).anyTimes();
    expect(taskClient.getStatusAsync(anyString())).andReturn(Futures.immediateFuture(KafkaIndexTask.Status.NOT_STARTED))
                                                  .anyTimes();
    expect(taskClient.getStartTimeAsync(anyString())).andReturn(Futures.immediateFuture(DateTime.now())).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            null
        )
    ).anyTimes();
    expect(taskClient.stopAsync("id3", false)).andReturn(Futures.immediateFuture(true));
    expect(taskClient.stopAsync("id4", false)).andReturn(Futures.immediateFuture(false));
    expect(taskClient.stopAsync("id5", false)).andReturn(Futures.immediateFuture((Boolean) null));
    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));
    taskQueue.shutdown("id4");
    taskQueue.shutdown("id5");
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();
  }

  @Test
  public void testRequeueTaskWhenFailed() throws Exception
  {
    supervisor = getSupervisor(2, 2, true, "PT1H", null, false);
    addSomeEvents(1);

    Capture<Task> captured = Capture.newInstance(CaptureType.ALL);
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskRunner.getRunningTasks()).andReturn(Collections.EMPTY_LIST).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.<Task>of()).anyTimes();
    expect(taskClient.getStatusAsync(anyString())).andReturn(Futures.immediateFuture(KafkaIndexTask.Status.NOT_STARTED))
                                                  .anyTimes();
    expect(taskClient.getStartTimeAsync(anyString())).andReturn(Futures.immediateFuture(DateTime.now())).anyTimes();
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

    // test that running the main loop again checks the status of the tasks that were created and does nothing if they
    // are all still running
    reset(taskStorage);
    expect(taskStorage.getActiveTasks()).andReturn(tasks).anyTimes();
    for (Task task : tasks) {
      expect(taskStorage.getStatus(task.getId())).andReturn(Optional.of(TaskStatus.running(task.getId()))).anyTimes();
      expect(taskStorage.getTask(task.getId())).andReturn(Optional.of(task)).anyTimes();
    }
    replay(taskStorage);

    supervisor.runInternal();
    verifyAll();

    // test that a task failing causes a new task to be re-queued with the same parameters
    Capture<Task> aNewTaskCapture = Capture.newInstance();
    List<Task> imStillAlive = tasks.subList(0, 3);
    KafkaIndexTask iHaveFailed = (KafkaIndexTask) tasks.get(3);
    reset(taskStorage);
    reset(taskQueue);
    expect(taskStorage.getActiveTasks()).andReturn(imStillAlive).anyTimes();
    for (Task task : imStillAlive) {
      expect(taskStorage.getStatus(task.getId())).andReturn(Optional.of(TaskStatus.running(task.getId()))).anyTimes();
      expect(taskStorage.getTask(task.getId())).andReturn(Optional.of(task)).anyTimes();
    }
    expect(taskStorage.getStatus(iHaveFailed.getId())).andReturn(Optional.of(TaskStatus.failure(iHaveFailed.getId())));
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
    supervisor = getSupervisor(2, 1, true, "PT1H", null, false);
    addSomeEvents(1);

    DateTime now = DateTime.now();
    Task id1 = createKafkaIndexTask(
        "id1",
        DATASOURCE,
        "sequenceName-0",
        new KafkaPartitions("topic", ImmutableMap.of(0, 0L, 2, 0L)),
        new KafkaPartitions("topic", ImmutableMap.of(0, Long.MAX_VALUE, 2, Long.MAX_VALUE)),
        now
    );

    List<Task> existingTasks = ImmutableList.of(id1);

    Capture<Task> captured = Capture.newInstance();
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskRunner.getRunningTasks()).andReturn(Collections.EMPTY_LIST).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(existingTasks).anyTimes();
    expect(taskStorage.getStatus("id1")).andReturn(Optional.of(TaskStatus.running("id1"))).anyTimes();
    expect(taskStorage.getTask("id1")).andReturn(Optional.of(id1)).anyTimes();
    expect(taskClient.getStatusAsync("id1")).andReturn(Futures.immediateFuture(KafkaIndexTask.Status.READING));
    expect(taskClient.getStartTimeAsync("id1")).andReturn(Futures.immediateFuture(now)).anyTimes();
    expect(taskQueue.add(capture(captured))).andReturn(true);
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            null
        )
    ).anyTimes();
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
    reset(taskStorage);
    reset(taskQueue);
    reset(taskClient);
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of(captured.getValue())).anyTimes();
    expect(taskStorage.getStatus(iHaveFailed.getId())).andReturn(Optional.of(TaskStatus.failure(iHaveFailed.getId())));
    expect(taskStorage.getStatus(runningTaskId)).andReturn(Optional.of(TaskStatus.running(runningTaskId))).anyTimes();
    expect(taskStorage.getTask(iHaveFailed.getId())).andReturn(Optional.of((Task) iHaveFailed)).anyTimes();
    expect(taskStorage.getTask(runningTaskId)).andReturn(Optional.of(captured.getValue())).anyTimes();
    expect(taskClient.getStatusAsync(runningTaskId)).andReturn(Futures.immediateFuture(KafkaIndexTask.Status.READING));
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
  }

  @Test
  public void testQueueNextTasksOnSuccess() throws Exception
  {
    supervisor = getSupervisor(2, 2, true, "PT1H", null, false);
    addSomeEvents(1);

    Capture<Task> captured = Capture.newInstance(CaptureType.ALL);
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskRunner.getRunningTasks()).andReturn(Collections.EMPTY_LIST).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.<Task>of()).anyTimes();
    expect(taskClient.getStatusAsync(anyString())).andReturn(Futures.immediateFuture(KafkaIndexTask.Status.NOT_STARTED))
                                                  .anyTimes();
    expect(taskClient.getStartTimeAsync(anyString())).andReturn(Futures.immediateFuture(DateTime.now())).anyTimes();
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

    reset(taskStorage);
    expect(taskStorage.getActiveTasks()).andReturn(tasks).anyTimes();
    for (Task task : tasks) {
      expect(taskStorage.getStatus(task.getId())).andReturn(Optional.of(TaskStatus.running(task.getId()))).anyTimes();
      expect(taskStorage.getTask(task.getId())).andReturn(Optional.of(task)).anyTimes();
    }
    replay(taskStorage);

    supervisor.runInternal();
    verifyAll();

    // test that a task succeeding causes a new task to be re-queued with the next offset range and causes any replica
    // tasks to be shutdown
    Capture<Task> newTasksCapture = Capture.newInstance(CaptureType.ALL);
    Capture<String> shutdownTaskIdCapture = Capture.newInstance();
    List<Task> imStillRunning = tasks.subList(1, 4);
    KafkaIndexTask iAmSuccess = (KafkaIndexTask) tasks.get(0);
    reset(taskStorage);
    reset(taskQueue);
    reset(taskClient);
    expect(taskStorage.getActiveTasks()).andReturn(imStillRunning).anyTimes();
    for (Task task : imStillRunning) {
      expect(taskStorage.getStatus(task.getId())).andReturn(Optional.of(TaskStatus.running(task.getId()))).anyTimes();
      expect(taskStorage.getTask(task.getId())).andReturn(Optional.of(task)).anyTimes();
    }
    expect(taskStorage.getStatus(iAmSuccess.getId())).andReturn(Optional.of(TaskStatus.success(iAmSuccess.getId())));
    expect(taskStorage.getTask(iAmSuccess.getId())).andReturn(Optional.of((Task) iAmSuccess)).anyTimes();
    expect(taskQueue.add(capture(newTasksCapture))).andReturn(true).times(2);
    expect(taskClient.stopAsync(capture(shutdownTaskIdCapture), eq(false))).andReturn(Futures.immediateFuture(true));
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

    supervisor = getSupervisor(2, 2, true, "PT1M", null, false);
    addSomeEvents(100);

    Capture<Task> captured = Capture.newInstance(CaptureType.ALL);
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskRunner.getRunningTasks()).andReturn(Collections.EMPTY_LIST).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.<Task>of()).anyTimes();
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
      workItems.add(new TestTaskRunnerWorkItem(task.getId(), null, location));
    }

    reset(taskStorage, taskRunner, taskClient, taskQueue);
    captured = Capture.newInstance(CaptureType.ALL);
    expect(taskStorage.getActiveTasks()).andReturn(tasks).anyTimes();
    for (Task task : tasks) {
      expect(taskStorage.getStatus(task.getId())).andReturn(Optional.of(TaskStatus.running(task.getId()))).anyTimes();
      expect(taskStorage.getTask(task.getId())).andReturn(Optional.of(task)).anyTimes();
    }
    expect(taskRunner.getRunningTasks()).andReturn(workItems).anyTimes();
    expect(taskClient.getStatusAsync(anyString()))
        .andReturn(Futures.immediateFuture(KafkaIndexTask.Status.READING))
        .anyTimes();
    expect(taskClient.getStartTimeAsync(EasyMock.contains("sequenceName-0")))
        .andReturn(Futures.immediateFuture(DateTime.now().minusMinutes(2)))
        .andReturn(Futures.immediateFuture(DateTime.now()));
    expect(taskClient.getStartTimeAsync(EasyMock.contains("sequenceName-1")))
        .andReturn(Futures.immediateFuture(DateTime.now()))
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

    replay(taskStorage, taskRunner, taskClient, taskQueue);

    supervisor.runInternal();
    verifyAll();

    for (Task task : captured.getValues()) {
      KafkaIndexTask kafkaIndexTask = (KafkaIndexTask) task;
      Assert.assertEquals(dataSchema, kafkaIndexTask.getDataSchema());
      Assert.assertEquals(KafkaTuningConfig.copyOf(tuningConfig), kafkaIndexTask.getTuningConfig());

      KafkaIOConfig taskConfig = kafkaIndexTask.getIOConfig();
      Assert.assertEquals("sequenceName-0", taskConfig.getBaseSequenceName());
      Assert.assertTrue("isUseTransaction", taskConfig.isUseTransaction());
      Assert.assertFalse("pauseAfterRead", taskConfig.isPauseAfterRead());

      Assert.assertEquals(topic, taskConfig.getStartPartitions().getTopic());
      Assert.assertEquals(10L, (long) taskConfig.getStartPartitions().getPartitionOffsetMap().get(0));
      Assert.assertEquals(20L, (long) taskConfig.getStartPartitions().getPartitionOffsetMap().get(1));
      Assert.assertEquals(35L, (long) taskConfig.getStartPartitions().getPartitionOffsetMap().get(2));
    }
  }

  @Test
  public void testDiscoverExistingPublishingTask() throws Exception
  {
    final TaskLocation location = new TaskLocation("testHost", 1234, -1);

    supervisor = getSupervisor(1, 1, true, "PT1H", null, false);
    addSomeEvents(1);

    Task task = createKafkaIndexTask(
        "id1",
        DATASOURCE,
        "sequenceName-0",
        new KafkaPartitions("topic", ImmutableMap.of(0, 0L, 1, 0L, 2, 0L)),
        new KafkaPartitions("topic", ImmutableMap.of(0, Long.MAX_VALUE, 1, Long.MAX_VALUE, 2, Long.MAX_VALUE)),
        null
    );

    Collection workItems = new ArrayList<>();
    workItems.add(new TestTaskRunnerWorkItem(task.getId(), null, location));

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
    expect(taskClient.getStatusAsync("id1")).andReturn(Futures.immediateFuture(KafkaIndexTask.Status.PUBLISHING));
    expect(taskClient.getCurrentOffsetsAsync("id1", false))
        .andReturn(Futures.immediateFuture((Map<Integer, Long>) ImmutableMap.of(0, 10L, 1, 20L, 2, 30L)));
    expect(taskClient.getCurrentOffsets("id1", true)).andReturn(ImmutableMap.of(0, 10L, 1, 20L, 2, 30L));
    expect(taskQueue.add(capture(captured))).andReturn(true);

    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    supervisor.updateCurrentAndLatestOffsets().run();
    SupervisorReport report = supervisor.getStatus();
    verifyAll();

    Assert.assertEquals(DATASOURCE, report.getId());
    Assert.assertTrue(report.getPayload() instanceof KafkaSupervisorReport.KafkaSupervisorReportPayload);

    KafkaSupervisorReport.KafkaSupervisorReportPayload payload = (KafkaSupervisorReport.KafkaSupervisorReportPayload)
        report.getPayload();

    Assert.assertEquals(DATASOURCE, payload.getDataSource());
    Assert.assertEquals(3600L, (long) payload.getDurationSeconds());
    Assert.assertEquals(NUM_PARTITIONS, (int) payload.getPartitions());
    Assert.assertEquals(1, (int) payload.getReplicas());
    Assert.assertEquals(topic, payload.getTopic());
    Assert.assertEquals(0, payload.getActiveTasks().size());
    Assert.assertEquals(1, payload.getPublishingTasks().size());

    TaskReportData publishingReport = payload.getPublishingTasks().get(0);

    Assert.assertEquals("id1", publishingReport.getId());
    Assert.assertEquals(ImmutableMap.of(0, 0L, 1, 0L, 2, 0L), publishingReport.getStartingOffsets());
    Assert.assertEquals(ImmutableMap.of(0, 10L, 1, 20L, 2, 30L), publishingReport.getCurrentOffsets());

    KafkaIndexTask capturedTask = captured.getValue();
    Assert.assertEquals(dataSchema, capturedTask.getDataSchema());
    Assert.assertEquals(KafkaTuningConfig.copyOf(tuningConfig), capturedTask.getTuningConfig());

    KafkaIOConfig capturedTaskConfig = capturedTask.getIOConfig();
    Assert.assertEquals(kafkaHost, capturedTaskConfig.getConsumerProperties().get("bootstrap.servers"));
    Assert.assertEquals("myCustomValue", capturedTaskConfig.getConsumerProperties().get("myCustomKey"));
    Assert.assertEquals("sequenceName-0", capturedTaskConfig.getBaseSequenceName());
    Assert.assertTrue("isUseTransaction", capturedTaskConfig.isUseTransaction());
    Assert.assertFalse("pauseAfterRead", capturedTaskConfig.isPauseAfterRead());

    // check that the new task was created with starting offsets matching where the publishing task finished
    Assert.assertEquals(topic, capturedTaskConfig.getStartPartitions().getTopic());
    Assert.assertEquals(10L, (long) capturedTaskConfig.getStartPartitions().getPartitionOffsetMap().get(0));
    Assert.assertEquals(20L, (long) capturedTaskConfig.getStartPartitions().getPartitionOffsetMap().get(1));
    Assert.assertEquals(30L, (long) capturedTaskConfig.getStartPartitions().getPartitionOffsetMap().get(2));

    Assert.assertEquals(topic, capturedTaskConfig.getEndPartitions().getTopic());
    Assert.assertEquals(Long.MAX_VALUE, (long) capturedTaskConfig.getEndPartitions().getPartitionOffsetMap().get(0));
    Assert.assertEquals(Long.MAX_VALUE, (long) capturedTaskConfig.getEndPartitions().getPartitionOffsetMap().get(1));
    Assert.assertEquals(Long.MAX_VALUE, (long) capturedTaskConfig.getEndPartitions().getPartitionOffsetMap().get(2));
  }

  @Test
  public void testDiscoverExistingPublishingTaskWithDifferentPartitionAllocation() throws Exception
  {
    final TaskLocation location = new TaskLocation("testHost", 1234, -1);

    supervisor = getSupervisor(1, 1, true, "PT1H", null, false);
    addSomeEvents(1);

    Task task = createKafkaIndexTask(
        "id1",
        DATASOURCE,
        "sequenceName-0",
        new KafkaPartitions("topic", ImmutableMap.of(0, 0L, 2, 0L)),
        new KafkaPartitions("topic", ImmutableMap.of(0, Long.MAX_VALUE, 2, Long.MAX_VALUE)),
        null
    );

    Collection workItems = new ArrayList<>();
    workItems.add(new TestTaskRunnerWorkItem(task.getId(), null, location));

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
    expect(taskClient.getStatusAsync("id1")).andReturn(Futures.immediateFuture(KafkaIndexTask.Status.PUBLISHING));
    expect(taskClient.getCurrentOffsetsAsync("id1", false))
        .andReturn(Futures.immediateFuture((Map<Integer, Long>) ImmutableMap.of(0, 10L, 2, 30L)));
    expect(taskClient.getCurrentOffsets("id1", true)).andReturn(ImmutableMap.of(0, 10L, 2, 30L));
    expect(taskQueue.add(capture(captured))).andReturn(true);

    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    supervisor.updateCurrentAndLatestOffsets().run();
    SupervisorReport report = supervisor.getStatus();
    verifyAll();

    Assert.assertEquals(DATASOURCE, report.getId());
    Assert.assertTrue(report.getPayload() instanceof KafkaSupervisorReport.KafkaSupervisorReportPayload);

    KafkaSupervisorReport.KafkaSupervisorReportPayload payload = (KafkaSupervisorReport.KafkaSupervisorReportPayload)
        report.getPayload();

    Assert.assertEquals(DATASOURCE, payload.getDataSource());
    Assert.assertEquals(3600L, (long) payload.getDurationSeconds());
    Assert.assertEquals(NUM_PARTITIONS, (int) payload.getPartitions());
    Assert.assertEquals(1, (int) payload.getReplicas());
    Assert.assertEquals(topic, payload.getTopic());
    Assert.assertEquals(0, payload.getActiveTasks().size());
    Assert.assertEquals(1, payload.getPublishingTasks().size());

    TaskReportData publishingReport = payload.getPublishingTasks().get(0);

    Assert.assertEquals("id1", publishingReport.getId());
    Assert.assertEquals(ImmutableMap.of(0, 0L, 2, 0L), publishingReport.getStartingOffsets());
    Assert.assertEquals(ImmutableMap.of(0, 10L, 2, 30L), publishingReport.getCurrentOffsets());

    KafkaIndexTask capturedTask = captured.getValue();
    Assert.assertEquals(dataSchema, capturedTask.getDataSchema());
    Assert.assertEquals(KafkaTuningConfig.copyOf(tuningConfig), capturedTask.getTuningConfig());

    KafkaIOConfig capturedTaskConfig = capturedTask.getIOConfig();
    Assert.assertEquals(kafkaHost, capturedTaskConfig.getConsumerProperties().get("bootstrap.servers"));
    Assert.assertEquals("myCustomValue", capturedTaskConfig.getConsumerProperties().get("myCustomKey"));
    Assert.assertEquals("sequenceName-0", capturedTaskConfig.getBaseSequenceName());
    Assert.assertTrue("isUseTransaction", capturedTaskConfig.isUseTransaction());
    Assert.assertFalse("pauseAfterRead", capturedTaskConfig.isPauseAfterRead());

    // check that the new task was created with starting offsets matching where the publishing task finished
    Assert.assertEquals(topic, capturedTaskConfig.getStartPartitions().getTopic());
    Assert.assertEquals(10L, (long) capturedTaskConfig.getStartPartitions().getPartitionOffsetMap().get(0));
    Assert.assertEquals(0L, (long) capturedTaskConfig.getStartPartitions().getPartitionOffsetMap().get(1));
    Assert.assertEquals(30L, (long) capturedTaskConfig.getStartPartitions().getPartitionOffsetMap().get(2));

    Assert.assertEquals(topic, capturedTaskConfig.getEndPartitions().getTopic());
    Assert.assertEquals(Long.MAX_VALUE, (long) capturedTaskConfig.getEndPartitions().getPartitionOffsetMap().get(0));
    Assert.assertEquals(Long.MAX_VALUE, (long) capturedTaskConfig.getEndPartitions().getPartitionOffsetMap().get(1));
    Assert.assertEquals(Long.MAX_VALUE, (long) capturedTaskConfig.getEndPartitions().getPartitionOffsetMap().get(2));
  }

  @Test
  public void testDiscoverExistingPublishingAndReadingTask() throws Exception
  {
    final TaskLocation location1 = new TaskLocation("testHost", 1234, -1);
    final TaskLocation location2 = new TaskLocation("testHost2", 145, -1);
    final DateTime startTime = new DateTime();

    supervisor = getSupervisor(1, 1, true, "PT1H", null, false);
    addSomeEvents(6);

    Task id1 = createKafkaIndexTask(
        "id1",
        DATASOURCE,
        "sequenceName-0",
        new KafkaPartitions("topic", ImmutableMap.of(0, 0L, 1, 0L, 2, 0L)),
        new KafkaPartitions("topic", ImmutableMap.of(0, Long.MAX_VALUE, 1, Long.MAX_VALUE, 2, Long.MAX_VALUE)),
        null
    );

    Task id2 = createKafkaIndexTask(
        "id2",
        DATASOURCE,
        "sequenceName-0",
        new KafkaPartitions("topic", ImmutableMap.of(0, 1L, 1, 2L, 2, 3L)),
        new KafkaPartitions("topic", ImmutableMap.of(0, Long.MAX_VALUE, 1, Long.MAX_VALUE, 2, Long.MAX_VALUE)),
        null
    );

    Collection workItems = new ArrayList<>();
    workItems.add(new TestTaskRunnerWorkItem(id1.getId(), null, location1));
    workItems.add(new TestTaskRunnerWorkItem(id2.getId(), null, location2));

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
    expect(taskClient.getStatusAsync("id1")).andReturn(Futures.immediateFuture(KafkaIndexTask.Status.PUBLISHING));
    expect(taskClient.getStatusAsync("id2")).andReturn(Futures.immediateFuture(KafkaIndexTask.Status.READING));
    expect(taskClient.getStartTimeAsync("id2")).andReturn(Futures.immediateFuture(startTime));
    expect(taskClient.getCurrentOffsetsAsync("id1", false))
        .andReturn(Futures.immediateFuture((Map<Integer, Long>) ImmutableMap.of(0, 1L, 1, 2L, 2, 3L)));
    expect(taskClient.getCurrentOffsets("id1", true)).andReturn(ImmutableMap.of(0, 1L, 1, 2L, 2, 3L));
    expect(taskClient.getCurrentOffsetsAsync("id2", false))
        .andReturn(Futures.immediateFuture((Map<Integer, Long>) ImmutableMap.of(0, 4L, 1, 5L, 2, 6L)));

    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    supervisor.updateCurrentAndLatestOffsets().run();
    SupervisorReport report = supervisor.getStatus();
    verifyAll();

    Assert.assertEquals(DATASOURCE, report.getId());
    Assert.assertTrue(report.getPayload() instanceof KafkaSupervisorReport.KafkaSupervisorReportPayload);

    KafkaSupervisorReport.KafkaSupervisorReportPayload payload = (KafkaSupervisorReport.KafkaSupervisorReportPayload)
        report.getPayload();

    Assert.assertEquals(DATASOURCE, payload.getDataSource());
    Assert.assertEquals(3600L, (long) payload.getDurationSeconds());
    Assert.assertEquals(NUM_PARTITIONS, (int) payload.getPartitions());
    Assert.assertEquals(1, (int) payload.getReplicas());
    Assert.assertEquals(topic, payload.getTopic());
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
    supervisor = getSupervisor(2, 2, true, "PT1H", null, false);
    addSomeEvents(1);

    Capture<Task> captured = Capture.newInstance(CaptureType.ALL);
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskRunner.getRunningTasks()).andReturn(Collections.EMPTY_LIST).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.<Task>of()).anyTimes();
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

    reset(taskStorage, taskClient, taskQueue);
    expect(taskStorage.getActiveTasks()).andReturn(tasks).anyTimes();
    for (Task task : tasks) {
      expect(taskStorage.getStatus(task.getId())).andReturn(Optional.of(TaskStatus.running(task.getId()))).anyTimes();
      expect(taskStorage.getTask(task.getId())).andReturn(Optional.of(task)).anyTimes();
      expect(taskClient.getStatusAsync(task.getId()))
          .andReturn(Futures.immediateFuture(KafkaIndexTask.Status.NOT_STARTED));
      expect(taskClient.getStartTimeAsync(task.getId()))
          .andReturn(Futures.<DateTime>immediateFailedFuture(new RuntimeException()));
      taskQueue.shutdown(task.getId());
    }
    replay(taskStorage, taskClient, taskQueue);

    supervisor.runInternal();
    verifyAll();
  }

  @Test
  public void testKillUnresponsiveTasksWhilePausing() throws Exception
  {
    final TaskLocation location = new TaskLocation("testHost", 1234, -1);

    supervisor = getSupervisor(2, 2, true, "PT1M", null, false);
    addSomeEvents(100);

    Capture<Task> captured = Capture.newInstance(CaptureType.ALL);
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskRunner.getRunningTasks()).andReturn(Collections.EMPTY_LIST).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.<Task>of()).anyTimes();
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
      workItems.add(new TestTaskRunnerWorkItem(task.getId(), null, location));
    }

    reset(taskStorage, taskRunner, taskClient, taskQueue);
    captured = Capture.newInstance(CaptureType.ALL);
    expect(taskStorage.getActiveTasks()).andReturn(tasks).anyTimes();
    for (Task task : tasks) {
      expect(taskStorage.getStatus(task.getId())).andReturn(Optional.of(TaskStatus.running(task.getId()))).anyTimes();
      expect(taskStorage.getTask(task.getId())).andReturn(Optional.of(task)).anyTimes();
    }
    expect(taskRunner.getRunningTasks()).andReturn(workItems).anyTimes();
    expect(taskClient.getStatusAsync(anyString()))
        .andReturn(Futures.immediateFuture(KafkaIndexTask.Status.READING))
        .anyTimes();
    expect(taskClient.getStartTimeAsync(EasyMock.contains("sequenceName-0")))
        .andReturn(Futures.immediateFuture(DateTime.now().minusMinutes(2)))
        .andReturn(Futures.immediateFuture(DateTime.now()));
    expect(taskClient.getStartTimeAsync(EasyMock.contains("sequenceName-1")))
        .andReturn(Futures.immediateFuture(DateTime.now()))
        .times(2);
    expect(taskClient.pauseAsync(EasyMock.contains("sequenceName-0")))
        .andReturn(Futures.<Map<Integer, Long>>immediateFailedFuture(new RuntimeException())).times(2);
    taskQueue.shutdown(EasyMock.contains("sequenceName-0"));
    expectLastCall().times(2);
    expect(taskQueue.add(capture(captured))).andReturn(true).times(2);

    replay(taskStorage, taskRunner, taskClient, taskQueue);

    supervisor.runInternal();
    verifyAll();

    for (Task task : captured.getValues()) {
      KafkaIOConfig taskConfig = ((KafkaIndexTask) task).getIOConfig();
      Assert.assertEquals(0L, (long) taskConfig.getStartPartitions().getPartitionOffsetMap().get(0));
      Assert.assertEquals(0L, (long) taskConfig.getStartPartitions().getPartitionOffsetMap().get(2));
    }
  }

  @Test
  public void testKillUnresponsiveTasksWhileSettingEndOffsets() throws Exception
  {
    final TaskLocation location = new TaskLocation("testHost", 1234, -1);

    supervisor = getSupervisor(2, 2, true, "PT1M", null, false);
    addSomeEvents(100);

    Capture<Task> captured = Capture.newInstance(CaptureType.ALL);
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskRunner.getRunningTasks()).andReturn(Collections.EMPTY_LIST).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.<Task>of()).anyTimes();
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
      workItems.add(new TestTaskRunnerWorkItem(task.getId(), null, location));
    }

    reset(taskStorage, taskRunner, taskClient, taskQueue);
    captured = Capture.newInstance(CaptureType.ALL);
    expect(taskStorage.getActiveTasks()).andReturn(tasks).anyTimes();
    for (Task task : tasks) {
      expect(taskStorage.getStatus(task.getId())).andReturn(Optional.of(TaskStatus.running(task.getId()))).anyTimes();
      expect(taskStorage.getTask(task.getId())).andReturn(Optional.of(task)).anyTimes();
    }
    expect(taskRunner.getRunningTasks()).andReturn(workItems).anyTimes();
    expect(taskClient.getStatusAsync(anyString()))
        .andReturn(Futures.immediateFuture(KafkaIndexTask.Status.READING))
        .anyTimes();
    expect(taskClient.getStartTimeAsync(EasyMock.contains("sequenceName-0")))
        .andReturn(Futures.immediateFuture(DateTime.now().minusMinutes(2)))
        .andReturn(Futures.immediateFuture(DateTime.now()));
    expect(taskClient.getStartTimeAsync(EasyMock.contains("sequenceName-1")))
        .andReturn(Futures.immediateFuture(DateTime.now()))
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
    ).andReturn(Futures.<Boolean>immediateFailedFuture(new RuntimeException())).times(2);
    taskQueue.shutdown(EasyMock.contains("sequenceName-0"));
    expectLastCall().times(2);
    expect(taskQueue.add(capture(captured))).andReturn(true).times(2);

    replay(taskStorage, taskRunner, taskClient, taskQueue);

    supervisor.runInternal();
    verifyAll();

    for (Task task : captured.getValues()) {
      KafkaIOConfig taskConfig = ((KafkaIndexTask) task).getIOConfig();
      Assert.assertEquals(0L, (long) taskConfig.getStartPartitions().getPartitionOffsetMap().get(0));
      Assert.assertEquals(0L, (long) taskConfig.getStartPartitions().getPartitionOffsetMap().get(2));
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testStopNotStarted() throws Exception
  {
    supervisor = getSupervisor(1, 1, true, "PT1H", null, false);
    supervisor.stop(false);
  }

  @Test
  public void testStop() throws Exception
  {
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    taskClient.close();
    taskRunner.unregisterListener(StringUtils.format("KafkaSupervisor-%s", DATASOURCE));
    replayAll();

    supervisor = getSupervisor(1, 1, true, "PT1H", null, false);
    supervisor.start();
    supervisor.stop(false);

    verifyAll();
  }

  @Test
  public void testStopGracefully() throws Exception
  {
    final TaskLocation location1 = new TaskLocation("testHost", 1234, -1);
    final TaskLocation location2 = new TaskLocation("testHost2", 145, -1);
    final DateTime startTime = new DateTime();

    supervisor = getSupervisor(2, 1, true, "PT1H", null, false);
    addSomeEvents(1);

    Task id1 = createKafkaIndexTask(
        "id1",
        DATASOURCE,
        "sequenceName-0",
        new KafkaPartitions("topic", ImmutableMap.of(0, 0L, 1, 0L, 2, 0L)),
        new KafkaPartitions("topic", ImmutableMap.of(0, Long.MAX_VALUE, 1, Long.MAX_VALUE, 2, Long.MAX_VALUE)),
        null
    );

    Task id2 = createKafkaIndexTask(
        "id2",
        DATASOURCE,
        "sequenceName-0",
        new KafkaPartitions("topic", ImmutableMap.of(0, 10L, 1, 20L, 2, 30L)),
        new KafkaPartitions("topic", ImmutableMap.of(0, Long.MAX_VALUE, 1, Long.MAX_VALUE, 2, Long.MAX_VALUE)),
        null
    );

    Task id3 = createKafkaIndexTask(
        "id3",
        DATASOURCE,
        "sequenceName-0",
        new KafkaPartitions("topic", ImmutableMap.of(0, 10L, 1, 20L, 2, 30L)),
        new KafkaPartitions("topic", ImmutableMap.of(0, Long.MAX_VALUE, 1, Long.MAX_VALUE, 2, Long.MAX_VALUE)),
        null
    );

    Collection workItems = new ArrayList<>();
    workItems.add(new TestTaskRunnerWorkItem(id1.getId(), null, location1));
    workItems.add(new TestTaskRunnerWorkItem(id2.getId(), null, location2));

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
    expect(taskClient.getStatusAsync("id1")).andReturn(Futures.immediateFuture(KafkaIndexTask.Status.PUBLISHING));
    expect(taskClient.getStatusAsync("id2")).andReturn(Futures.immediateFuture(KafkaIndexTask.Status.READING));
    expect(taskClient.getStatusAsync("id3")).andReturn(Futures.immediateFuture(KafkaIndexTask.Status.READING));
    expect(taskClient.getStartTimeAsync("id2")).andReturn(Futures.immediateFuture(startTime));
    expect(taskClient.getStartTimeAsync("id3")).andReturn(Futures.immediateFuture(startTime));
    expect(taskClient.getCurrentOffsets("id1", true)).andReturn(ImmutableMap.of(0, 10L, 1, 20L, 2, 30L));

    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    reset(taskRunner, taskClient, taskQueue);
    expect(taskRunner.getRunningTasks()).andReturn(workItems).anyTimes();
    expect(taskClient.pauseAsync("id2"))
        .andReturn(Futures.immediateFuture((Map<Integer, Long>) ImmutableMap.of(0, 15L, 1, 25L, 2, 30L)));
    expect(taskClient.setEndOffsetsAsync("id2", ImmutableMap.of(0, 15L, 1, 25L, 2, 30L), true))
        .andReturn(Futures.immediateFuture(true));
    taskQueue.shutdown("id3");
    expectLastCall().times(2);

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
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.<Task>of()).anyTimes();
    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));
    replayAll();

    supervisor = getSupervisor(1, 1, true, "PT1H", null, false);
    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    reset(indexerMetadataStorageCoordinator);
    expect(indexerMetadataStorageCoordinator.deleteDataSourceMetadata(DATASOURCE)).andReturn(true);
    replay(indexerMetadataStorageCoordinator);

    supervisor.resetInternal(null);
    verifyAll();

  }

  @Test
  public void testResetDataSourceMetadata() throws Exception
  {
    supervisor = getSupervisor(1, 1, true, "PT1H", null, false);
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskRunner.getRunningTasks()).andReturn(Collections.EMPTY_LIST).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.<Task>of()).anyTimes();
    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    Capture<String> captureDataSource = EasyMock.newCapture();
    Capture<DataSourceMetadata> captureDataSourceMetadata = EasyMock.newCapture();

    KafkaDataSourceMetadata kafkaDataSourceMetadata = new KafkaDataSourceMetadata(new KafkaPartitions(
        topic,
        ImmutableMap.of(0, 1000L, 1, 1000L, 2, 1000L)
    ));

    KafkaDataSourceMetadata resetMetadata = new KafkaDataSourceMetadata(new KafkaPartitions(
        topic,
        ImmutableMap.of(1, 1000L, 2, 1000L)
    ));

    KafkaDataSourceMetadata expectedMetadata = new KafkaDataSourceMetadata(new KafkaPartitions(
        topic,
        ImmutableMap.of(0, 1000L)
    ));

    reset(indexerMetadataStorageCoordinator);
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(kafkaDataSourceMetadata);
    expect(indexerMetadataStorageCoordinator.resetDataSourceMetadata(
        EasyMock.capture(captureDataSource),
        EasyMock.capture(captureDataSourceMetadata)
    )).andReturn(true);
    replay(indexerMetadataStorageCoordinator);

    supervisor.resetInternal(resetMetadata);
    verifyAll();

    Assert.assertEquals(captureDataSource.getValue(), DATASOURCE);
    Assert.assertEquals(captureDataSourceMetadata.getValue(), expectedMetadata);
  }

  @Test
  public void testResetNoDataSourceMetadata() throws Exception
  {
    supervisor = getSupervisor(1, 1, true, "PT1H", null, false);
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskRunner.getRunningTasks()).andReturn(Collections.EMPTY_LIST).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.<Task>of()).anyTimes();
    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    KafkaDataSourceMetadata resetMetadata = new KafkaDataSourceMetadata(new KafkaPartitions(
        topic,
        ImmutableMap.of(1, 1000L, 2, 1000L)
    ));

    reset(indexerMetadataStorageCoordinator);
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
    final DateTime startTime = new DateTime();

    supervisor = getSupervisor(2, 1, true, "PT1H", null, false);
    addSomeEvents(1);

    Task id1 = createKafkaIndexTask(
        "id1",
        DATASOURCE,
        "sequenceName-0",
        new KafkaPartitions("topic", ImmutableMap.of(0, 0L, 1, 0L, 2, 0L)),
        new KafkaPartitions("topic", ImmutableMap.of(0, Long.MAX_VALUE, 1, Long.MAX_VALUE, 2, Long.MAX_VALUE)),
        null
    );

    Task id2 = createKafkaIndexTask(
        "id2",
        DATASOURCE,
        "sequenceName-0",
        new KafkaPartitions("topic", ImmutableMap.of(0, 10L, 1, 20L, 2, 30L)),
        new KafkaPartitions("topic", ImmutableMap.of(0, Long.MAX_VALUE, 1, Long.MAX_VALUE, 2, Long.MAX_VALUE)),
        null
    );

    Task id3 = createKafkaIndexTask(
        "id3",
        DATASOURCE,
        "sequenceName-0",
        new KafkaPartitions("topic", ImmutableMap.of(0, 10L, 1, 20L, 2, 30L)),
        new KafkaPartitions("topic", ImmutableMap.of(0, Long.MAX_VALUE, 1, Long.MAX_VALUE, 2, Long.MAX_VALUE)),
        null
    );

    Collection workItems = new ArrayList<>();
    workItems.add(new TestTaskRunnerWorkItem(id1.getId(), null, location1));
    workItems.add(new TestTaskRunnerWorkItem(id2.getId(), null, location2));

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
    expect(taskClient.getStatusAsync("id1")).andReturn(Futures.immediateFuture(KafkaIndexTask.Status.PUBLISHING));
    expect(taskClient.getStatusAsync("id2")).andReturn(Futures.immediateFuture(KafkaIndexTask.Status.READING));
    expect(taskClient.getStatusAsync("id3")).andReturn(Futures.immediateFuture(KafkaIndexTask.Status.READING));
    expect(taskClient.getStartTimeAsync("id2")).andReturn(Futures.immediateFuture(startTime));
    expect(taskClient.getStartTimeAsync("id3")).andReturn(Futures.immediateFuture(startTime));
    expect(taskClient.getCurrentOffsets("id1", true)).andReturn(ImmutableMap.of(0, 10L, 1, 20L, 2, 30L));

    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    reset(taskQueue, indexerMetadataStorageCoordinator);
    expect(indexerMetadataStorageCoordinator.deleteDataSourceMetadata(DATASOURCE)).andReturn(true);
    taskQueue.shutdown("id2");
    taskQueue.shutdown("id3");
    replay(taskQueue, indexerMetadataStorageCoordinator);

    supervisor.resetInternal(null);
    verifyAll();
  }

  private void addSomeEvents(int numEventsPerPartition) throws Exception
  {
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      for (int i = 0; i < NUM_PARTITIONS; i++) {
        for (int j = 0; j < numEventsPerPartition; j++) {
          kafkaProducer.send(
              new ProducerRecord<byte[], byte[]>(
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
      boolean skipOffsetGaps
  )
  {
    KafkaSupervisorIOConfig kafkaSupervisorIOConfig = new KafkaSupervisorIOConfig(
        topic,
        replicas,
        taskCount,
        new Period(duration),
        ImmutableMap.of("myCustomKey", "myCustomValue", "bootstrap.servers", kafkaHost),
        new Period("P1D"),
        new Period("PT30S"),
        useEarliestOffset,
        new Period("PT30M"),
        lateMessageRejectionPeriod,
        skipOffsetGaps
    );

    KafkaIndexTaskClientFactory taskClientFactory = new KafkaIndexTaskClientFactory(null, null)
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
            taskStorage,
            taskMaster,
            indexerMetadataStorageCoordinator,
            taskClientFactory,
            objectMapper,
            new NoopServiceEmitter(),
            new DruidMonitorSchedulerConfig()
        )
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
                    new JSONPathSpec(true, ImmutableList.<JSONPathFieldSpec>of()),
                    ImmutableMap.<String, Boolean>of()
                ),
                Charsets.UTF_8.name()
            ),
            Map.class
        ),
        new AggregatorFactory[]{new CountAggregatorFactory("rows")},
        new UniformGranularitySpec(
            Granularities.HOUR,
            Granularities.NONE,
            ImmutableList.<Interval>of()
        ),
        objectMapper
    );
  }

  private KafkaIndexTask createKafkaIndexTask(
      String id,
      String dataSource,
      String sequenceName,
      KafkaPartitions startPartitions,
      KafkaPartitions endPartitions,
      DateTime minimumMessageTime
  )
  {
    return new KafkaIndexTask(
        id,
        null,
        getDataSchema(dataSource),
        tuningConfig,
        new KafkaIOConfig(
            sequenceName,
            startPartitions,
            endPartitions,
            ImmutableMap.<String, String>of(),
            true,
            false,
            minimumMessageTime,
            false
        ),
        ImmutableMap.<String, Object>of(),
        null
    );
  }

  private static class TestTaskRunnerWorkItem extends TaskRunnerWorkItem
  {

    private TaskLocation location;

    public TestTaskRunnerWorkItem(String taskId, ListenableFuture<TaskStatus> result, TaskLocation location)
    {
      super(taskId, result);
      this.location = location;
    }

    @Override
    public TaskLocation getLocation()
    {
      return location;
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
        KafkaSupervisorSpec spec
    )
    {
      super(taskStorage, taskMaster, indexerMetadataStorageCoordinator, taskClientFactory, mapper, spec);
    }

    @Override
    protected String generateSequenceName(int groupId)
    {
      return StringUtils.format("sequenceName-%d", groupId);
    }
  }
}
