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

import cloud.localstack.Localstack;
import cloud.localstack.docker.LocalstackDockerTestRunner;
import cloud.localstack.docker.annotation.LocalstackDockerProperties;
import com.amazonaws.http.SdkHttpMetadata;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
import org.apache.druid.indexing.kinesis.KinesisIOConfig;
import org.apache.druid.indexing.kinesis.KinesisIndexTask;
import org.apache.druid.indexing.kinesis.KinesisIndexTaskClient;
import org.apache.druid.indexing.kinesis.KinesisIndexTaskClientFactory;
import org.apache.druid.indexing.kinesis.KinesisPartitions;
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
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTask;
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
import org.junit.runner.RunWith;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import static org.easymock.EasyMock.anyBoolean;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;

// TODO: improve helper methods like insertData(...)
@RunWith(LocalstackDockerTestRunner.class)
@LocalstackDockerProperties(services = {"kinesis"})
public class KinesisSupervisorTest extends EasyMockSupport
{
  private static final ObjectMapper objectMapper = TestHelper.makeJsonMapper();
  private static final String TOPIC_PREFIX = "testTopic";
  private static final String DATASOURCE = "testDS";
  private static final int TEST_CHAT_THREADS = 3;
  private static final long TEST_CHAT_RETRIES = 9L;
  private static final Period TEST_HTTP_TIMEOUT = new Period("PT10S");
  private static final Period TEST_SHUTDOWN_TIMEOUT = new Period("PT80S");
  private static final List<PutRecordsRequestEntry> records = ImmutableList.of(
      generateRequestEntry(
          "1",
          JB("2008", "a", "y", "10", "20.0", "1.0")
      ),
      generateRequestEntry(
          "1",
          JB("2009", "b", "y", "10", "20.0", "1.0")
      ),
      generateRequestEntry(
          "1",
          JB("2010", "c", "y", "10", "20.0", "1.0")
      ),
      generateRequestEntry(
          "1",
          JB("2011", "d", "y", "10", "20.0", "1.0")
      ),
      generateRequestEntry(
          "1",
          JB("2011", "e", "y", "10", "20.0", "1.0")
      ),
      generateRequestEntry(
          "1",
          JB("246140482-04-24T15:36:27.903Z", "x", "z", "10", "20.0", "1.0")
      ),
      generateRequestEntry("1", StringUtils.toUtf8("unparseable")),
      generateRequestEntry(
          "1",
          StringUtils.toUtf8("unparseable2")
      ),
      generateRequestEntry("1", "{}".getBytes()),
      generateRequestEntry(
          "1",
          JB("2013", "f", "y", "10", "20.0", "1.0")
      ),
      generateRequestEntry(
          "1",
          JB("2049", "f", "y", "notanumber", "20.0", "1.0")
      ),
      generateRequestEntry(
          "1",
          JB("2049", "f", "y", "10", "notanumber", "1.0")
      ),
      generateRequestEntry(
          "1",
          JB("2049", "f", "y", "10", "20.0", "notanumber")
      ),
      generateRequestEntry(
          "123123",
          JB("2012", "g", "y", "10", "20.0", "1.0")
      ),
      generateRequestEntry(
          "123123",
          JB("2011", "h", "y", "10", "20.0", "1.0")
      )
  );
  private static String shardId1 = "shardId-000000000001";
  private static String shardId0 = "shardId-000000000000";


  private static DataSchema dataSchema;
  private static int topicPostfix;

  private final int numThreads;

  private KinesisSupervisor supervisor;
  private KinesisSupervisorTuningConfig tuningConfig;
  private TaskStorage taskStorage;
  private TaskMaster taskMaster;
  private TaskRunner taskRunner;
  private IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator;
  private KinesisIndexTaskClient taskClient;
  private TaskQueue taskQueue;
  private String stream;
  private RowIngestionMetersFactory rowIngestionMetersFactory;
  private ExceptionCapturingServiceEmitter serviceEmitter;

  private static String getStream()
  {
    return TOPIC_PREFIX + topicPostfix++;
  }

  public KinesisSupervisorTest()
  {
    this.numThreads = 1;
  }

  @BeforeClass
  public static void setupClass()
  {
    /*
     * Need to disable CBOR protocol, see:
     * https://github.com/mhart/kinesalite/blob/master/README.md#cbor-protocol-issues-with-the-java-sdk
     */
    cloud.localstack.TestUtils.setEnv("AWS_CBOR_DISABLE", "1");
    /* Disable SSL certificate checks for local testing */
    if (Localstack.useSSL()) {
      cloud.localstack.TestUtils.disableSslCertChecking();
    }

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

    tuningConfig = new KinesisSupervisorTuningConfig(
        1000,
        null,
        50000,
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
        null,
        null,
        null,
        null,
        null
    );

    stream = getStream();
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
    AmazonKinesis kinesis = getKinesisClientInstance();
    List<PutRecordsResultEntry> res = insertData(kinesis, generateRecordsRequests(stream));

    Capture<KinesisIndexTask> captured = Capture.newInstance();
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
            null
        )
    ).anyTimes();
    expect(taskQueue.add(capture(captured))).andReturn(true);
    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    KinesisIndexTask task = captured.getValue();
    Assert.assertEquals(dataSchema, task.getDataSchema());
    Assert.assertEquals(tuningConfig.copyOf(), task.getTuningConfig());

    KinesisIOConfig taskConfig = task.getIOConfig();
    Assert.assertEquals("sequenceName-0", taskConfig.getBaseSequenceName());
    Assert.assertTrue("isUseTransaction", taskConfig.isUseTransaction());
    Assert.assertFalse("minimumMessageTime", taskConfig.getMinimumMessageTime().isPresent());
    Assert.assertFalse("maximumMessageTime", taskConfig.getMaximumMessageTime().isPresent());

    Assert.assertEquals(stream, taskConfig.getStartPartitions().getStream());
    Assert.assertEquals(
        getSequenceNumber(res, shardId1, 0),
        taskConfig.getStartPartitions().getPartitionSequenceNumberMap().get(shardId1)
    );
    Assert.assertEquals(
        getSequenceNumber(res, shardId0, 0),
        taskConfig.getStartPartitions().getPartitionSequenceNumberMap().get(shardId0)
    );

    Assert.assertEquals(stream, taskConfig.getEndPartitions().getStream());
    Assert.assertEquals(
        KinesisPartitions.NO_END_SEQUENCE_NUMBER,
        taskConfig.getEndPartitions().getPartitionSequenceNumberMap().get(shardId1)
    );
    Assert.assertEquals(
        KinesisPartitions.NO_END_SEQUENCE_NUMBER,
        taskConfig.getEndPartitions().getPartitionSequenceNumberMap().get(shardId0)
    );
  }


  @Test
  public void testMultiTask() throws Exception
  {
    supervisor = getSupervisor(1, 2, true, "PT1H", null, null);
    AmazonKinesis kinesis = getKinesisClientInstance();
    List<PutRecordsResultEntry> res = insertData(kinesis, generateRecordsRequests(stream));

    Capture<KinesisIndexTask> captured = Capture.newInstance(CaptureType.ALL);
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
            null
        )
    ).anyTimes();
    expect(taskQueue.add(capture(captured))).andReturn(true).times(2);
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    KinesisIndexTask task1 = captured.getValues().get(0);
    Assert.assertEquals(1, task1.getIOConfig().getStartPartitions().getPartitionSequenceNumberMap().size());
    Assert.assertEquals(1, task1.getIOConfig().getEndPartitions().getPartitionSequenceNumberMap().size());
    Assert.assertEquals(
        getSequenceNumber(res, shardId1, 0),
        task1.getIOConfig().getStartPartitions().getPartitionSequenceNumberMap().get(shardId1)
    );
    Assert.assertEquals(
        KinesisPartitions.NO_END_SEQUENCE_NUMBER,
        task1.getIOConfig().getEndPartitions().getPartitionSequenceNumberMap().get(shardId1)
    );

    KinesisIndexTask task2 = captured.getValues().get(1);
    Assert.assertEquals(1, task2.getIOConfig().getStartPartitions().getPartitionSequenceNumberMap().size());
    Assert.assertEquals(1, task2.getIOConfig().getEndPartitions().getPartitionSequenceNumberMap().size());
    Assert.assertEquals(
        getSequenceNumber(res, shardId0, 0),
        task2.getIOConfig().getStartPartitions().getPartitionSequenceNumberMap().get(shardId0)
    );
    Assert.assertEquals(
        KinesisPartitions.NO_END_SEQUENCE_NUMBER,
        task2.getIOConfig().getEndPartitions().getPartitionSequenceNumberMap().get(shardId0)
    );
  }

  @Test
  public void testReplicas() throws Exception
  {
    supervisor = getSupervisor(2, 1, true, "PT1H", null, null);
    AmazonKinesis kinesis = getKinesisClientInstance();
    List<PutRecordsResultEntry> res = insertData(kinesis, generateRecordsRequests(stream));

    Capture<KinesisIndexTask> captured = Capture.newInstance(CaptureType.ALL);
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
            null
        )
    ).anyTimes();
    expect(taskQueue.add(capture(captured))).andReturn(true).times(2);
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    KinesisIndexTask task1 = captured.getValues().get(0);
    Assert.assertEquals(2, task1.getIOConfig().getStartPartitions().getPartitionSequenceNumberMap().size());
    Assert.assertEquals(2, task1.getIOConfig().getEndPartitions().getPartitionSequenceNumberMap().size());
    Assert.assertEquals(
        getSequenceNumber(res, shardId0, 0),
        task1.getIOConfig().getStartPartitions().getPartitionSequenceNumberMap().get(shardId0)
    );
    Assert.assertEquals(
        KinesisPartitions.NO_END_SEQUENCE_NUMBER,
        task1.getIOConfig().getEndPartitions().getPartitionSequenceNumberMap().get(shardId0)
    );
    Assert.assertEquals(
        getSequenceNumber(res, shardId1, 0),
        task1.getIOConfig().getStartPartitions().getPartitionSequenceNumberMap().get(shardId1)
    );
    Assert.assertEquals(
        KinesisPartitions.NO_END_SEQUENCE_NUMBER,
        task1.getIOConfig().getEndPartitions().getPartitionSequenceNumberMap().get(shardId1)
    );

    KinesisIndexTask task2 = captured.getValues().get(1);
    Assert.assertEquals(2, task2.getIOConfig().getStartPartitions().getPartitionSequenceNumberMap().size());
    Assert.assertEquals(2, task2.getIOConfig().getEndPartitions().getPartitionSequenceNumberMap().size());
    Assert.assertEquals(
        getSequenceNumber(res, shardId0, 0),
        task2.getIOConfig().getStartPartitions().getPartitionSequenceNumberMap().get(shardId0)
    );
    Assert.assertEquals(
        KinesisPartitions.NO_END_SEQUENCE_NUMBER,
        task2.getIOConfig().getEndPartitions().getPartitionSequenceNumberMap().get(shardId0)
    );
    Assert.assertEquals(
        getSequenceNumber(res, shardId1, 0),
        task2.getIOConfig().getStartPartitions().getPartitionSequenceNumberMap().get(shardId1)
    );
    Assert.assertEquals(
        KinesisPartitions.NO_END_SEQUENCE_NUMBER,
        task2.getIOConfig().getEndPartitions().getPartitionSequenceNumberMap().get(shardId1)
    );

  }

  @Test
  public void testLateMessageRejectionPeriod() throws Exception
  {
    supervisor = getSupervisor(2, 1, true, "PT1H", new Period("PT1H"), null);
    AmazonKinesis kinesis = getKinesisClientInstance();
    List<PutRecordsResultEntry> res = insertData(kinesis, generateRecordsRequests(stream));

    Capture<KinesisIndexTask> captured = Capture.newInstance(CaptureType.ALL);
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
            null
        )
    ).anyTimes();
    expect(taskQueue.add(capture(captured))).andReturn(true).times(2);
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
    AmazonKinesis kinesis = getKinesisClientInstance();
    List<PutRecordsResultEntry> res = insertData(kinesis, generateRecordsRequests(stream));

    Capture<KinesisIndexTask> captured = Capture.newInstance(CaptureType.ALL);
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
            null
        )
    ).anyTimes();
    expect(taskQueue.add(capture(captured))).andReturn(true).times(2);
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
   * Test generating the starting offsets from the partition data stored in druid_dataSource which contains the
   * offsets of the last built segments.
   */
  public void testDatasourceMetadata() throws Exception
  {
    supervisor = getSupervisor(1, 1, true, "PT1H", null, null);
    AmazonKinesis kinesis = getKinesisClientInstance();
    List<PutRecordsResultEntry> res = insertData(kinesis, generateRecordsRequests(stream));

    Capture<KinesisIndexTask> captured = Capture.newInstance();
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
            new KinesisPartitions(stream, ImmutableMap.of(
                shardId1,
                getSequenceNumber(res, shardId1, 2),
                shardId0,
                getSequenceNumber(res, shardId0, 1)
            ))
        )
    ).anyTimes();

    expect(taskQueue.add(capture(captured))).andReturn(true);
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    KinesisIndexTask task = captured.getValue();
    KinesisIOConfig taskConfig = task.getIOConfig();
    Assert.assertEquals("sequenceName-0", taskConfig.getBaseSequenceName());
    Assert.assertEquals(
        getSequenceNumber(res, shardId1, 2),
        taskConfig.getStartPartitions().getPartitionSequenceNumberMap().get(shardId1)
    );
    Assert.assertEquals(
        getSequenceNumber(res, shardId0, 1),
        taskConfig.getStartPartitions().getPartitionSequenceNumberMap().get(shardId0)
    );
  }

  @Test(expected = ISE.class)
  public void testBadMetadataOffsets() throws Exception
  {
    supervisor = getSupervisor(1, 1, true, "PT1H", null, null);
    AmazonKinesis kinesis = getKinesisClientInstance();
    List<PutRecordsResultEntry> res = insertData(kinesis, generateRecordsRequests(stream));


    expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
            new KinesisPartitions(stream, ImmutableMap.of(
                shardId1,
                "00000000000000000000000000000000000000000000000000000000",
                shardId0,
                "00000000000000000000000000000000000000000000000000000000"
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
    AmazonKinesis kinesis = getKinesisClientInstance();
    List<PutRecordsResultEntry> res = insertData(kinesis, generateRecordsRequests(stream));

    // unexpected # of partitions (kill)
    Task id1 = createKinesisIndexTask(
        "id1",
        DATASOURCE,
        1,
        new KinesisPartitions(stream, ImmutableMap.of(
            shardId0,
            getSequenceNumber(res, shardId0, 0)
        )),
        new KinesisPartitions(stream, ImmutableMap.of(
            shardId0,
            getSequenceNumber(res, shardId0, 1)
        )),
        null,
        null
    );

    // correct number of partitions and ranges (don't kill)
    Task id2 = createKinesisIndexTask(
        "id2",
        DATASOURCE,
        0,
        new KinesisPartitions(stream, ImmutableMap.of(
            shardId0,
            getSequenceNumber(res, shardId0, 0),
            shardId1,
            getSequenceNumber(res, shardId1, 0)
        )),
        new KinesisPartitions(stream, ImmutableMap.of(
            shardId0,
            getSequenceNumber(res, shardId0, 1),
            shardId1,
            getSequenceNumber(res, shardId1, 12)
        )),
        null,
        null
    );

    // unexpected range on partition 2 (kill)
    Task id3 = createKinesisIndexTask(
        "id3",
        DATASOURCE,
        1,
        new KinesisPartitions(stream, ImmutableMap.of(
            shardId0,
            getSequenceNumber(res, shardId0, 0),
            shardId1,
            getSequenceNumber(res, shardId1, 1)
        )),
        new KinesisPartitions(stream, ImmutableMap.of(
            shardId0,
            getSequenceNumber(res, shardId0, 1),
            shardId1,
            getSequenceNumber(res, shardId1, 11)
        )),
        null,
        null
    );

    // different datasource (don't kill)
    Task id4 = createKinesisIndexTask(
        "id4",
        "other-datasource",
        2,
        new KinesisPartitions(stream, ImmutableMap.of(
            shardId0,
            getSequenceNumber(res, shardId0, 0),
            shardId1,
            getSequenceNumber(res, shardId1, 0)
        )),
        new KinesisPartitions(stream, ImmutableMap.of(
            shardId0,
            getSequenceNumber(res, shardId0, 1),
            shardId1,
            getSequenceNumber(res, shardId1, 12)
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
    expect(taskClient.getStatusAsync(anyString())).andReturn(Futures.immediateFuture(SeekableStreamIndexTask.Status.NOT_STARTED))
                                                  .anyTimes();
    expect(taskClient.getStartTimeAsync(anyString())).andReturn(Futures.immediateFuture(DateTimes.nowUtc())).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
            null
        )
    ).anyTimes();
    expect(taskClient.stopAsync("id1", false)).andReturn(Futures.immediateFuture(true));
    expect(taskClient.stopAsync("id3", false)).andReturn(Futures.immediateFuture(false));
    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));
    taskQueue.shutdown("id3");

    expect(taskQueue.add(anyObject(Task.class))).andReturn(true);

    TreeMap<Integer, Map<String, String>> checkpoints = new TreeMap<>();
    checkpoints.put(0, ImmutableMap.of(
        shardId0,
        getSequenceNumber(res, shardId0, 0),
        shardId1,
        getSequenceNumber(res, shardId1, 0)
    ));

    expect(taskClient.getCheckpointsAsync(EasyMock.contains("id2"), anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints))
        .times(2);

    replayAll();
    supervisor.start();
    supervisor.runInternal();
    verifyAll();
  }

  // TODO: delete redundant tasks
  @Test
  public void testKillBadPartitionAssignment() throws Exception
  {
    supervisor = getSupervisor(1, 2, true, "PT1H", null, null);
    AmazonKinesis kinesis = getKinesisClientInstance();
    List<PutRecordsResultEntry> res = insertData(kinesis, generateRecordsRequests(stream));

    Task id1 = createKinesisIndexTask(
        "id1",
        DATASOURCE,
        0,
        new KinesisPartitions(stream, ImmutableMap.of(
            shardId1,
            getSequenceNumber(res, shardId1, 0)
        )),
        new KinesisPartitions(stream, ImmutableMap.of(
            shardId1,
            getSequenceNumber(res, shardId1, 12)
        )),
        null,
        null
    );
    Task id2 = createKinesisIndexTask(
        "id2",
        DATASOURCE,
        1,
        new KinesisPartitions(stream, ImmutableMap.of(
            shardId0,
            getSequenceNumber(res, shardId0, 0)
        )),
        new KinesisPartitions(stream, ImmutableMap.of(
            shardId0,
            getSequenceNumber(res, shardId0, 1)
        )),
        null,
        null
    );
    Task id3 = createKinesisIndexTask(
        "id3",
        DATASOURCE,
        0,
        new KinesisPartitions(stream, ImmutableMap.of(
            shardId0,
            getSequenceNumber(res, shardId0, 0),
            shardId1,
            getSequenceNumber(res, shardId1, 0)
        )),
        new KinesisPartitions(stream, ImmutableMap.of(
            shardId0,
            getSequenceNumber(res, shardId0, 1),
            shardId1,
            getSequenceNumber(res, shardId1, 12)
        )),
        null,
        null
    );
    Task id4 = createKinesisIndexTask(
        "id4",
        DATASOURCE,
        0,
        new KinesisPartitions(stream, ImmutableMap.of(
            shardId0,
            getSequenceNumber(res, shardId0, 0)
        )),
        new KinesisPartitions(stream, ImmutableMap.of(
            shardId0,
            getSequenceNumber(res, shardId0, 1)
        )),
        null,
        null
    );
    Task id5 = createKinesisIndexTask(
        "id5",
        DATASOURCE,
        0,
        new KinesisPartitions(stream, ImmutableMap.of(
            shardId0,
            getSequenceNumber(res, shardId0, 0)
        )),
        new KinesisPartitions(stream, ImmutableMap.of(
            shardId0,
            getSequenceNumber(res, shardId0, 1)
        )),
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
    expect(taskClient.getStatusAsync(anyString())).andReturn(Futures.immediateFuture(SeekableStreamIndexTask.Status.NOT_STARTED))
                                                  .anyTimes();
    expect(taskClient.getStartTimeAsync(anyString())).andReturn(Futures.immediateFuture(DateTimes.nowUtc())).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
            null
        )
    ).anyTimes();
    expect(taskClient.stopAsync("id3", false)).andReturn(Futures.immediateFuture(true));
    expect(taskClient.stopAsync("id4", false)).andReturn(Futures.immediateFuture(false));
    expect(taskClient.stopAsync("id5", false)).andReturn(Futures.immediateFuture((Boolean) null));

    TreeMap<Integer, Map<String, String>> checkpoints1 = new TreeMap<>();
    checkpoints1.put(0, ImmutableMap.of(shardId1, getSequenceNumber(res, shardId1, 0)));
    TreeMap<Integer, Map<String, String>> checkpoints2 = new TreeMap<>();
    checkpoints2.put(0, ImmutableMap.of(shardId0, getSequenceNumber(res, shardId0, 0)));
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("id1"), anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints1))
        .times(1);
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("id2"), anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints2))
        .times(1);

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
    supervisor = getSupervisor(2, 2, true, "PT1H", null, null);
    AmazonKinesis kinesis = getKinesisClientInstance();
    List<PutRecordsResultEntry> res = insertData(kinesis, generateRecordsRequests(stream));

    Capture<Task> captured = Capture.newInstance(CaptureType.ALL);
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskRunner.getRunningTasks()).andReturn(Collections.EMPTY_LIST).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();
    expect(taskClient.getStatusAsync(anyString())).andReturn(Futures.immediateFuture(SeekableStreamIndexTask.Status.NOT_STARTED))
                                                  .anyTimes();
    expect(taskClient.getStartTimeAsync(anyString())).andReturn(Futures.immediateFuture(DateTimes.nowUtc())).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
            null
        )
    ).anyTimes();
    expect(taskQueue.add(capture(captured))).andReturn(true).times(4);

    TreeMap<Integer, Map<String, String>> checkpoints1 = new TreeMap<>();
    checkpoints1.put(
        0,
        ImmutableMap.of(
            shardId1,
            getSequenceNumber(res, shardId1, 0)
        )
    );
    TreeMap<Integer, Map<String, String>> checkpoints2 = new TreeMap<>();
    checkpoints2.put(0, ImmutableMap.of(
        shardId0,
        getSequenceNumber(res, shardId0, 0)
    ));
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("sequenceName-0"), anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints1))
        .anyTimes();
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("sequenceName-1"), anyBoolean()))
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
    KinesisIndexTask iHaveFailed = (KinesisIndexTask) tasks.get(3);
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
        ((KinesisIndexTask) aNewTaskCapture.getValue()).getIOConfig().getBaseSequenceName()
    );
  }

  @Test
  public void testRequeueAdoptedTaskWhenFailed() throws Exception
  {
    supervisor = getSupervisor(2, 1, true, "PT1H", null, null);
    AmazonKinesis kinesis = getKinesisClientInstance();
    List<PutRecordsResultEntry> res = insertData(kinesis, generateRecordsRequests(stream));

    DateTime now = DateTimes.nowUtc();
    DateTime maxi = now.plusMinutes(60);
    Task id1 = createKinesisIndexTask(
        "id1",
        DATASOURCE,
        0,
        new KinesisPartitions("stream", ImmutableMap.of(
            shardId1,
            getSequenceNumber(res, shardId1, 0),
            shardId0,
            getSequenceNumber(res, shardId0, 0)
        )),
        new KinesisPartitions("stream", ImmutableMap.of(
            shardId1,
            KinesisPartitions.NO_END_SEQUENCE_NUMBER,
            shardId0,
            KinesisPartitions.NO_END_SEQUENCE_NUMBER
        )),
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
    expect(taskClient.getStatusAsync("id1")).andReturn(Futures.immediateFuture(SeekableStreamIndexTask.Status.READING));
    expect(taskClient.getStartTimeAsync("id1")).andReturn(Futures.immediateFuture(now)).anyTimes();
    expect(taskQueue.add(capture(captured))).andReturn(true);
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
            null
        )
    ).anyTimes();

    TreeMap<Integer, Map<String, String>> checkpoints = new TreeMap<>();
    checkpoints.put(0, ImmutableMap.of(
        shardId1,
        getSequenceNumber(res, shardId1, 0),
        shardId0,
        getSequenceNumber(res, shardId0, 0)
    ));
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("id1"), anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints))
        .times(2);

    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));
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
    reset(taskStorage);
    reset(taskQueue);
    reset(taskClient);

    // for the newly created replica task
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("sequenceName-0"), anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints))
        .times(2);
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("id1"), anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints))
        .times(1);

    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of(captured.getValue())).anyTimes();
    expect(taskStorage.getStatus(iHaveFailed.getId())).andReturn(Optional.of(TaskStatus.failure(iHaveFailed.getId())));
    expect(taskStorage.getStatus(runningTaskId)).andReturn(Optional.of(TaskStatus.running(runningTaskId))).anyTimes();
    expect(taskStorage.getTask(iHaveFailed.getId())).andReturn(Optional.of((Task) iHaveFailed)).anyTimes();
    expect(taskStorage.getTask(runningTaskId)).andReturn(Optional.of(captured.getValue())).anyTimes();
    expect(taskClient.getStatusAsync(runningTaskId)).andReturn(Futures.immediateFuture(SeekableStreamIndexTask.Status.READING));
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
    AmazonKinesis kinesis = getKinesisClientInstance();
    List<PutRecordsResultEntry> res = insertData(kinesis, generateRecordsRequests(stream));

    Capture<Task> captured = Capture.newInstance(CaptureType.ALL);
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskRunner.getRunningTasks()).andReturn(Collections.EMPTY_LIST).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();
    expect(taskClient.getStatusAsync(anyString())).andReturn(Futures.immediateFuture(SeekableStreamIndexTask.Status.NOT_STARTED))
                                                  .anyTimes();
    expect(taskClient.getStartTimeAsync(anyString())).andReturn(Futures.immediateFuture(DateTimes.nowUtc())).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
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
    reset(taskClient);

    expect(taskClient.getStatusAsync(anyString())).andReturn(Futures.immediateFuture(SeekableStreamIndexTask.Status.NOT_STARTED))
                                                  .anyTimes();
    expect(taskClient.getStartTimeAsync(anyString())).andReturn(Futures.immediateFuture(DateTimes.nowUtc())).anyTimes();
    TreeMap<Integer, Map<String, String>> checkpoints1 = new TreeMap<>();
    checkpoints1.put(0, ImmutableMap.of(
        shardId1,
        getSequenceNumber(res, shardId1, 0),
        shardId0,
        getSequenceNumber(res, shardId0, 0)
    ));
    TreeMap<Integer, Map<String, String>> checkpoints2 = new TreeMap<>();
    checkpoints2.put(0, ImmutableMap.of(
        shardId1,
        getSequenceNumber(res, shardId1, 0)
    ));
    // there would be 4 tasks, 2 for each task group
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("sequenceName-0"), anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints1))
        .times(2);
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("sequenceName-1"), anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints2))
        .times(2);

    expect(taskStorage.getActiveTasks()).andReturn(tasks).anyTimes();
    for (Task task : tasks) {
      expect(taskStorage.getStatus(task.getId())).andReturn(Optional.of(TaskStatus.running(task.getId()))).anyTimes();
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
    KinesisIndexTask iAmSuccess = (KinesisIndexTask) tasks.get(0);
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

    supervisor = getSupervisor(2, 2, true, "PT1M", null, null);
    AmazonKinesis kinesis = getKinesisClientInstance();
    List<PutRecordsResultEntry> res = insertData(kinesis, generateRecordsRequests(stream));

    Capture<Task> captured = Capture.newInstance(CaptureType.ALL);
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskRunner.getRunningTasks()).andReturn(Collections.EMPTY_LIST).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
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

    reset(taskStorage, taskRunner, taskClient, taskQueue);
    captured = Capture.newInstance(CaptureType.ALL);
    expect(taskStorage.getActiveTasks()).andReturn(tasks).anyTimes();
    for (Task task : tasks) {
      expect(taskStorage.getStatus(task.getId())).andReturn(Optional.of(TaskStatus.running(task.getId()))).anyTimes();
      expect(taskStorage.getTask(task.getId())).andReturn(Optional.of(task)).anyTimes();
    }
    expect(taskRunner.getRunningTasks()).andReturn(workItems).anyTimes();
    expect(taskClient.getStatusAsync(anyString()))
        .andReturn(Futures.immediateFuture(SeekableStreamIndexTask.Status.READING))
        .anyTimes();
    expect(taskClient.getStartTimeAsync(EasyMock.contains("sequenceName-0")))
        .andReturn(Futures.immediateFuture(DateTimes.nowUtc().minusMinutes(2)))
        .andReturn(Futures.immediateFuture(DateTimes.nowUtc()));
    expect(taskClient.getStartTimeAsync(EasyMock.contains("sequenceName-1")))
        .andReturn(Futures.immediateFuture(DateTimes.nowUtc()))
        .times(2);
    expect(taskClient.pauseAsync(EasyMock.contains("sequenceName-0")))
        .andReturn(Futures.immediateFuture(ImmutableMap.of(
            shardId1,
            getSequenceNumber(res, shardId1, 1),
            shardId0,
            getSequenceNumber(res, shardId0, 0)
        )))
        .andReturn(Futures.immediateFuture(ImmutableMap.of(
            shardId1,
            getSequenceNumber(res, shardId1, 3),
            shardId0,
            getSequenceNumber(res, shardId0, 1)
        )));
    expect(
        taskClient.setEndOffsetsAsync(
            EasyMock.contains("sequenceName-0"),
            EasyMock.eq(ImmutableMap.of(
                shardId1,
                getSequenceNumber(res, shardId1, 3),
                shardId0,
                getSequenceNumber(res, shardId0, 1)
            )),
            EasyMock.eq(true)
        )
    ).andReturn(Futures.immediateFuture(true)).times(2);
    expect(taskQueue.add(capture(captured))).andReturn(true).times(2);

    TreeMap<Integer, Map<String, String>> checkpoints1 = new TreeMap<>();
    checkpoints1.put(0, ImmutableMap.of(
        shardId1,
        getSequenceNumber(res, shardId1, 0)
    ));
    TreeMap<Integer, Map<String, String>> checkpoints2 = new TreeMap<>();
    checkpoints2.put(0, ImmutableMap.of(
        shardId0,
        getSequenceNumber(res, shardId0, 0)
    ));
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("sequenceName-0"), anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints1))
        .times(2);
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("sequenceName-1"), anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints2))
        .times(2);

    replay(taskStorage, taskRunner, taskClient, taskQueue);

    supervisor.runInternal();
    verifyAll();

    for (Task task : captured.getValues()) {
      KinesisIndexTask KinesisIndexTask = (KinesisIndexTask) task;
      Assert.assertEquals(dataSchema, KinesisIndexTask.getDataSchema());
      Assert.assertEquals(tuningConfig.copyOf(), KinesisIndexTask.getTuningConfig());

      KinesisIOConfig taskConfig = KinesisIndexTask.getIOConfig();
      Assert.assertEquals("sequenceName-0", taskConfig.getBaseSequenceName());
      Assert.assertTrue("isUseTransaction", taskConfig.isUseTransaction());

      Assert.assertEquals(stream, taskConfig.getStartPartitions().getStream());
      Assert.assertEquals(
          getSequenceNumber(res, shardId1, 3),
          taskConfig.getStartPartitions().getPartitionSequenceNumberMap().get(shardId1)
      );
      Assert.assertEquals(
          getSequenceNumber(res, shardId0, 1),
          taskConfig.getStartPartitions().getPartitionSequenceNumberMap().get(shardId0)
      );
    }
  }

  @Test
  public void testDiscoverExistingPublishingTask() throws Exception
  {
    final TaskLocation location = new TaskLocation("testHost", 1234, -1);

    supervisor = getSupervisor(1, 1, true, "PT1H", null, null);
    AmazonKinesis kinesis = getKinesisClientInstance();
    List<PutRecordsResultEntry> res = insertData(kinesis, generateRecordsRequests(stream));

    Task task = createKinesisIndexTask(
        "id1",
        DATASOURCE,
        0,
        new KinesisPartitions("stream", ImmutableMap.of(
            shardId1,
            getSequenceNumber(res, shardId1, 0),
            shardId0,
            getSequenceNumber(res, shardId0, 0)
        )),
        new KinesisPartitions("stream", ImmutableMap.of(
            shardId1,
            KinesisPartitions.NO_END_SEQUENCE_NUMBER,
            shardId0,
            KinesisPartitions.NO_END_SEQUENCE_NUMBER
        )),
        null,
        null
    );

    Collection workItems = new ArrayList<>();
    workItems.add(new TestTaskRunnerWorkItem(task, null, location));

    Capture<KinesisIndexTask> captured = Capture.newInstance();
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskRunner.getRunningTasks()).andReturn(workItems).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of(task)).anyTimes();
    expect(taskStorage.getStatus("id1")).andReturn(Optional.of(TaskStatus.running("id1"))).anyTimes();
    expect(taskStorage.getTask("id1")).andReturn(Optional.of(task)).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
            null
        )
    ).anyTimes();
    expect(taskClient.getStatusAsync("id1")).andReturn(Futures.immediateFuture(SeekableStreamIndexTask.Status.PUBLISHING));
    expect(taskClient.getCurrentOffsetsAsync("id1", false))
        .andReturn(Futures.immediateFuture(ImmutableMap.of(
            shardId1,
            getSequenceNumber(res, shardId1, 2),
            shardId0,
            getSequenceNumber(res, shardId0, 1)
        )));
    expect(taskClient.getEndOffsets("id1")).andReturn(ImmutableMap.of(
        shardId1,
        getSequenceNumber(res, shardId1, 2),
        shardId0,
        getSequenceNumber(res, shardId0, 1)
    ));
    expect(taskQueue.add(capture(captured))).andReturn(true);

    TreeMap<Integer, Map<String, String>> checkpoints = new TreeMap<>();
    checkpoints.put(0, ImmutableMap.of(
        shardId1,
        getSequenceNumber(res, shardId1, 0),
        shardId0,
        getSequenceNumber(res, shardId0, 0)
    ));
    expect(taskClient.getCheckpoints(anyString(), anyBoolean())).andReturn(checkpoints).anyTimes();

    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));
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
    Assert.assertEquals(stream, payload.getId());
    Assert.assertEquals(0, payload.getActiveTasks().size());
    Assert.assertEquals(1, payload.getPublishingTasks().size());

    TaskReportData publishingReport = payload.getPublishingTasks().get(0);

    Assert.assertEquals("id1", publishingReport.getId());
    Assert.assertEquals(ImmutableMap.of(
        shardId1,
        getSequenceNumber(res, shardId1, 0),
        shardId0,
        getSequenceNumber(res, shardId0, 0)
    ), publishingReport.getStartingOffsets());
    Assert.assertEquals(ImmutableMap.of(
        shardId1,
        getSequenceNumber(res, shardId1, 2),
        shardId0,
        getSequenceNumber(res, shardId0, 1)
    ), publishingReport.getCurrentOffsets());

    KinesisIndexTask capturedTask = captured.getValue();
    Assert.assertEquals(dataSchema, capturedTask.getDataSchema());
    Assert.assertEquals(tuningConfig.copyOf(), capturedTask.getTuningConfig());

    KinesisIOConfig capturedTaskConfig = capturedTask.getIOConfig();
    Assert.assertEquals(cloud.localstack.TestUtils.TEST_ACCESS_KEY, capturedTaskConfig.getAwsAccessKeyId());
    Assert.assertEquals(cloud.localstack.TestUtils.TEST_SECRET_KEY, capturedTaskConfig.getAwsSecretAccessKey());
    Assert.assertEquals(Localstack.getEndpointKinesis(), capturedTaskConfig.getEndpoint());
    Assert.assertEquals("sequenceName-0", capturedTaskConfig.getBaseSequenceName());
    Assert.assertTrue("isUseTransaction", capturedTaskConfig.isUseTransaction());

    // check that the new task was created with starting offsets matching where the publishing task finished
    Assert.assertEquals(stream, capturedTaskConfig.getStartPartitions().getStream());
    Assert.assertEquals(
        getSequenceNumber(res, shardId1, 2),
        capturedTaskConfig.getStartPartitions().getPartitionSequenceNumberMap().get(shardId1)
    );
    Assert.assertEquals(
        getSequenceNumber(res, shardId0, 1),
        capturedTaskConfig.getStartPartitions().getPartitionSequenceNumberMap().get(shardId0)
    );

    Assert.assertEquals(stream, capturedTaskConfig.getEndPartitions().getStream());
    Assert.assertEquals(
        KinesisPartitions.NO_END_SEQUENCE_NUMBER,
        capturedTaskConfig.getEndPartitions().getPartitionSequenceNumberMap().get(shardId1)
    );
    Assert.assertEquals(
        KinesisPartitions.NO_END_SEQUENCE_NUMBER,
        capturedTaskConfig.getEndPartitions().getPartitionSequenceNumberMap().get(shardId0)
    );
  }

  @Test
  public void testDiscoverExistingPublishingTaskWithDifferentPartitionAllocation() throws Exception
  {
    final TaskLocation location = new TaskLocation("testHost", 1234, -1);

    supervisor = getSupervisor(1, 1, true, "PT1H", null, null);
    AmazonKinesis kinesis = getKinesisClientInstance();
    List<PutRecordsResultEntry> res = insertData(kinesis, generateRecordsRequests(stream));

    Task task = createKinesisIndexTask(
        "id1",
        DATASOURCE,
        0,
        new KinesisPartitions("stream", ImmutableMap.of(
            shardId1,
            getSequenceNumber(res, shardId1, 0),
            shardId0,
            getSequenceNumber(res, shardId0, 0)
        )),
        new KinesisPartitions("stream", ImmutableMap.of(
            shardId1,
            KinesisPartitions.NO_END_SEQUENCE_NUMBER,
            shardId0,
            KinesisPartitions.NO_END_SEQUENCE_NUMBER
        )),
        null,
        null
    );

    Collection workItems = new ArrayList<>();
    workItems.add(new TestTaskRunnerWorkItem(task, null, location));

    Capture<KinesisIndexTask> captured = Capture.newInstance();
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskRunner.getRunningTasks()).andReturn(workItems).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of(task)).anyTimes();
    expect(taskStorage.getStatus("id1")).andReturn(Optional.of(TaskStatus.running("id1"))).anyTimes();
    expect(taskStorage.getTask("id1")).andReturn(Optional.of(task)).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
            null
        )
    ).anyTimes();
    expect(taskClient.getStatusAsync("id1")).andReturn(Futures.immediateFuture(SeekableStreamIndexTask.Status.PUBLISHING));
    expect(taskClient.getCurrentOffsetsAsync("id1", false))
        .andReturn(Futures.immediateFuture(ImmutableMap.of(
            shardId1,
            getSequenceNumber(res, shardId1, 2),
            shardId0,
            getSequenceNumber(res, shardId0, 1)
        )));
    expect(taskClient.getEndOffsets("id1")).andReturn(ImmutableMap.of(
        shardId1,
        getSequenceNumber(res, shardId1, 2),
        shardId0,
        getSequenceNumber(res, shardId0, 1)
    ));
    expect(taskQueue.add(capture(captured))).andReturn(true);

    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));
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
    Assert.assertEquals(stream, payload.getId());
    Assert.assertEquals(0, payload.getActiveTasks().size());
    Assert.assertEquals(1, payload.getPublishingTasks().size());

    TaskReportData publishingReport = payload.getPublishingTasks().get(0);

    Assert.assertEquals("id1", publishingReport.getId());
    Assert.assertEquals(ImmutableMap.of(
        shardId1,
        getSequenceNumber(res, shardId1, 0),
        shardId0,
        getSequenceNumber(res, shardId0, 0)
    ), publishingReport.getStartingOffsets());
    Assert.assertEquals(ImmutableMap.of(
        shardId1,
        getSequenceNumber(res, shardId1, 2),
        shardId0,
        getSequenceNumber(res, shardId0, 1)
    ), publishingReport.getCurrentOffsets());

    KinesisIndexTask capturedTask = captured.getValue();
    Assert.assertEquals(dataSchema, capturedTask.getDataSchema());
    Assert.assertEquals(tuningConfig.copyOf(), capturedTask.getTuningConfig());

    KinesisIOConfig capturedTaskConfig = capturedTask.getIOConfig();
    Assert.assertEquals(cloud.localstack.TestUtils.TEST_ACCESS_KEY, capturedTaskConfig.getAwsAccessKeyId());
    Assert.assertEquals(cloud.localstack.TestUtils.TEST_SECRET_KEY, capturedTaskConfig.getAwsSecretAccessKey());
    Assert.assertEquals(Localstack.getEndpointKinesis(), capturedTaskConfig.getEndpoint());
    Assert.assertEquals("sequenceName-0", capturedTaskConfig.getBaseSequenceName());
    Assert.assertTrue("isUseTransaction", capturedTaskConfig.isUseTransaction());

    // check that the new task was created with starting offsets matching where the publishing task finished
    Assert.assertEquals(stream, capturedTaskConfig.getStartPartitions().getStream());
    Assert.assertEquals(
        getSequenceNumber(res, shardId1, 2),
        capturedTaskConfig.getStartPartitions().getPartitionSequenceNumberMap().get(shardId1)
    );
    Assert.assertEquals(
        getSequenceNumber(res, shardId0, 1),
        capturedTaskConfig.getStartPartitions().getPartitionSequenceNumberMap().get(shardId0)
    );

    Assert.assertEquals(stream, capturedTaskConfig.getEndPartitions().getStream());
    Assert.assertEquals(
        KinesisPartitions.NO_END_SEQUENCE_NUMBER,
        capturedTaskConfig.getEndPartitions().getPartitionSequenceNumberMap().get(shardId1)
    );
    Assert.assertEquals(
        KinesisPartitions.NO_END_SEQUENCE_NUMBER,
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
    AmazonKinesis kinesis = getKinesisClientInstance();
    List<PutRecordsResultEntry> res = insertData(kinesis, generateRecordsRequests(stream));

    Task id1 = createKinesisIndexTask(
        "id1",
        DATASOURCE,
        0,
        new KinesisPartitions(
            "stream",
            ImmutableMap.of(
                shardId1,
                getSequenceNumber(res, shardId1, 0),
                shardId0,
                getSequenceNumber(res, shardId0, 0)
            )
        ),
        new KinesisPartitions("stream", ImmutableMap.of(
            shardId1,
            KinesisPartitions.NO_END_SEQUENCE_NUMBER,
            shardId0,
            KinesisPartitions.NO_END_SEQUENCE_NUMBER
        )),
        null,
        null
    );

    Task id2 = createKinesisIndexTask(
        "id2",
        DATASOURCE,
        0,
        new KinesisPartitions("stream", ImmutableMap.of(
            shardId1,
            getSequenceNumber(res, shardId1, 2),
            shardId0,
            getSequenceNumber(res, shardId0, 1)
        )),
        new KinesisPartitions("stream", ImmutableMap.of(
            shardId1,
            KinesisPartitions.NO_END_SEQUENCE_NUMBER,
            shardId0,
            KinesisPartitions.NO_END_SEQUENCE_NUMBER
        )),
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
        new KinesisDataSourceMetadata(
            null
        )
    ).anyTimes();
    expect(taskClient.getStatusAsync("id1")).andReturn(Futures.immediateFuture(SeekableStreamIndexTask.Status.PUBLISHING));
    expect(taskClient.getStatusAsync("id2")).andReturn(Futures.immediateFuture(SeekableStreamIndexTask.Status.READING));
    expect(taskClient.getStartTimeAsync("id2")).andReturn(Futures.immediateFuture(startTime));
    expect(taskClient.getCurrentOffsetsAsync("id1", false))
        .andReturn(Futures.immediateFuture(ImmutableMap.of(
            shardId1,
            getSequenceNumber(res, shardId1, 2),
            shardId0,
            getSequenceNumber(res, shardId0, 1)
        )));
    expect(taskClient.getEndOffsets("id1")).andReturn(ImmutableMap.of(
        shardId1,
        getSequenceNumber(res, shardId1, 2),
        shardId0,
        getSequenceNumber(res, shardId0, 1)
    ));
    expect(taskClient.getCurrentOffsetsAsync("id2", false))
        .andReturn(Futures.immediateFuture((Map<String, String>) ImmutableMap.of(
            shardId1,
            getSequenceNumber(res, shardId1, 12),
            shardId0,
            getSequenceNumber(res, shardId0, 1)
        )));

    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));

    // since id1 is publishing, so getCheckpoints wouldn't be called for it
    TreeMap<Integer, Map<String, String>> checkpoints = new TreeMap<>();
    checkpoints.put(0, ImmutableMap.of(
        shardId1,
        getSequenceNumber(res, shardId1, 2),
        shardId0,
        getSequenceNumber(res, shardId0, 1)
    ));
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("id2"), anyBoolean()))
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
    Assert.assertEquals(stream, payload.getId());
    Assert.assertEquals(1, payload.getActiveTasks().size());
    Assert.assertEquals(1, payload.getPublishingTasks().size());

    TaskReportData activeReport = payload.getActiveTasks().get(0);
    TaskReportData publishingReport = payload.getPublishingTasks().get(0);

    Assert.assertEquals("id2", activeReport.getId());
    Assert.assertEquals(startTime, activeReport.getStartTime());
    Assert.assertEquals(ImmutableMap.of(
        shardId1,
        getSequenceNumber(res, shardId1, 2),
        shardId0,
        getSequenceNumber(res, shardId0, 1)
    ), activeReport.getStartingOffsets());
    Assert.assertEquals(ImmutableMap.of(
        shardId1,
        getSequenceNumber(res, shardId1, 12),
        shardId0,
        getSequenceNumber(res, shardId0, 1)
    ), activeReport.getCurrentOffsets());

    Assert.assertEquals("id1", publishingReport.getId());
    Assert.assertEquals(ImmutableMap.of(
        shardId1,
        getSequenceNumber(res, shardId1, 0),
        shardId0,
        getSequenceNumber(res, shardId0, 0)
    ), publishingReport.getStartingOffsets());
    Assert.assertEquals(ImmutableMap.of(
        shardId1,
        getSequenceNumber(res, shardId1, 2),
        shardId0,
        getSequenceNumber(res, shardId0, 1)
    ), publishingReport.getCurrentOffsets());
  }

  @Test
  public void testKillUnresponsiveTasksWhileGettingStartTime() throws Exception
  {
    supervisor = getSupervisor(2, 2, true, "PT1H", null, null);
    AmazonKinesis kinesis = getKinesisClientInstance();
    List<PutRecordsResultEntry> res = insertData(kinesis, generateRecordsRequests(stream));

    Capture<Task> captured = Capture.newInstance(CaptureType.ALL);
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskRunner.getRunningTasks()).andReturn(Collections.EMPTY_LIST).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
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

    TreeMap<Integer, Map<String, String>> checkpoints1 = new TreeMap<>();
    checkpoints1.put(0, ImmutableMap.of(
        shardId1,

        getSequenceNumber(res, shardId1, 0),

        shardId0,

        getSequenceNumber(res, shardId0, 0)
    ));
    TreeMap<Integer, Map<String, String>> checkpoints2 = new TreeMap<>();
    checkpoints2.put(0, ImmutableMap.of(shardId0, getSequenceNumber(res, shardId0, 0)));
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("sequenceName-0"), anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints1))
        .times(2);
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("sequenceName-1"), anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints2))
        .times(2);

    expect(taskStorage.getActiveTasks()).andReturn(tasks).anyTimes();
    for (Task task : tasks) {
      expect(taskStorage.getStatus(task.getId())).andReturn(Optional.of(TaskStatus.running(task.getId()))).anyTimes();
      expect(taskStorage.getTask(task.getId())).andReturn(Optional.of(task)).anyTimes();
      expect(taskClient.getStatusAsync(task.getId()))
          .andReturn(Futures.immediateFuture(SeekableStreamIndexTask.Status.NOT_STARTED));
      expect(taskClient.getStartTimeAsync(task.getId()))
          .andReturn(Futures.immediateFailedFuture(new RuntimeException()));
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

    supervisor = getSupervisor(2, 2, true, "PT1M", null, null);
    AmazonKinesis kinesis = getKinesisClientInstance();
    List<PutRecordsResultEntry> res = insertData(kinesis, generateRecordsRequests(stream));

    Capture<Task> captured = Capture.newInstance(CaptureType.ALL);
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskRunner.getRunningTasks()).andReturn(Collections.EMPTY_LIST).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
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

    reset(taskStorage, taskRunner, taskClient, taskQueue);

    TreeMap<Integer, Map<String, String>> checkpoints1 = new TreeMap<>();
    checkpoints1.put(0, ImmutableMap.of(
        shardId1,
        getSequenceNumber(res, shardId1, 0)
    ));
    TreeMap<Integer, Map<String, String>> checkpoints2 = new TreeMap<>();
    checkpoints2.put(0, ImmutableMap.of(shardId0, getSequenceNumber(res, shardId0, 0)));
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("sequenceName-0"), anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints1))
        .times(2);
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("sequenceName-1"), anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints2))
        .times(2);

    captured = Capture.newInstance(CaptureType.ALL);
    expect(taskStorage.getActiveTasks()).andReturn(tasks).anyTimes();
    for (Task task : tasks) {
      expect(taskStorage.getStatus(task.getId())).andReturn(Optional.of(TaskStatus.running(task.getId()))).anyTimes();
      expect(taskStorage.getTask(task.getId())).andReturn(Optional.of(task)).anyTimes();
    }
    expect(taskRunner.getRunningTasks()).andReturn(workItems).anyTimes();
    expect(taskClient.getStatusAsync(anyString()))
        .andReturn(Futures.immediateFuture(SeekableStreamIndexTask.Status.READING))
        .anyTimes();
    expect(taskClient.getStartTimeAsync(EasyMock.contains("sequenceName-0")))
        .andReturn(Futures.immediateFuture(DateTimes.nowUtc().minusMinutes(2)))
        .andReturn(Futures.immediateFuture(DateTimes.nowUtc()));
    expect(taskClient.getStartTimeAsync(EasyMock.contains("sequenceName-1")))
        .andReturn(Futures.immediateFuture(DateTimes.nowUtc()))
        .times(2);
    expect(taskClient.pauseAsync(EasyMock.contains("sequenceName-0")))
        .andReturn(Futures.immediateFailedFuture(new RuntimeException())).times(2);
    taskQueue.shutdown(EasyMock.contains("sequenceName-0"));
    expectLastCall().times(2);
    expect(taskQueue.add(capture(captured))).andReturn(true).times(2);

    replay(taskStorage, taskRunner, taskClient, taskQueue);

    supervisor.runInternal();
    verifyAll();

    for (Task task : captured.getValues()) {
      KinesisIOConfig taskConfig = ((KinesisIndexTask) task).getIOConfig();
      Assert.assertEquals(
          getSequenceNumber(res, shardId1, 0),
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
    AmazonKinesis kinesis = getKinesisClientInstance();
    List<PutRecordsResultEntry> res = insertData(kinesis, generateRecordsRequests(stream));

    Capture<Task> captured = Capture.newInstance(CaptureType.ALL);
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskRunner.getRunningTasks()).andReturn(Collections.EMPTY_LIST).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
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

    reset(taskStorage, taskRunner, taskClient, taskQueue);

    TreeMap<Integer, Map<String, String>> checkpoints1 = new TreeMap<>();
    checkpoints1.put(0, ImmutableMap.of(
        shardId1,
        getSequenceNumber(res, shardId1, 0)
    ));
    TreeMap<Integer, Map<String, String>> checkpoints2 = new TreeMap<>();
    checkpoints2.put(0, ImmutableMap.of(shardId0, getSequenceNumber(res, shardId0, 0)));
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("sequenceName-0"), anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints1))
        .times(2);
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("sequenceName-1"), anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints2))
        .times(2);

    captured = Capture.newInstance(CaptureType.ALL);
    expect(taskStorage.getActiveTasks()).andReturn(tasks).anyTimes();
    for (Task task : tasks) {
      expect(taskStorage.getStatus(task.getId())).andReturn(Optional.of(TaskStatus.running(task.getId()))).anyTimes();
      expect(taskStorage.getTask(task.getId())).andReturn(Optional.of(task)).anyTimes();
    }
    expect(taskRunner.getRunningTasks()).andReturn(workItems).anyTimes();
    expect(taskClient.getStatusAsync(anyString()))
        .andReturn(Futures.immediateFuture(SeekableStreamIndexTask.Status.READING))
        .anyTimes();
    expect(taskClient.getStartTimeAsync(EasyMock.contains("sequenceName-0")))
        .andReturn(Futures.immediateFuture(DateTimes.nowUtc().minusMinutes(2)))
        .andReturn(Futures.immediateFuture(DateTimes.nowUtc()));
    expect(taskClient.getStartTimeAsync(EasyMock.contains("sequenceName-1")))
        .andReturn(Futures.immediateFuture(DateTimes.nowUtc()))
        .times(2);
    expect(taskClient.pauseAsync(EasyMock.contains("sequenceName-0")))
        .andReturn(Futures.immediateFuture(ImmutableMap.of(
            shardId1,
            getSequenceNumber(res, shardId1, 1)
        )))
        .andReturn(Futures.immediateFuture(ImmutableMap.of(
            shardId1,
            getSequenceNumber(res, shardId1, 3)
        )));
    expect(
        taskClient.setEndOffsetsAsync(
            EasyMock.contains("sequenceName-0"),
            EasyMock.eq(ImmutableMap.of(
                shardId1,
                getSequenceNumber(res, shardId1, 3)
            )),
            EasyMock.eq(true)
        )
    ).andReturn(Futures.immediateFailedFuture(new RuntimeException())).times(2);
    taskQueue.shutdown(EasyMock.contains("sequenceName-0"));
    expectLastCall().times(2);
    expect(taskQueue.add(capture(captured))).andReturn(true).times(2);

    replay(taskStorage, taskRunner, taskClient, taskQueue);

    supervisor.runInternal();
    verifyAll();

    for (Task task : captured.getValues()) {
      KinesisIOConfig taskConfig = ((KinesisIndexTask) task).getIOConfig();
      Assert.assertEquals(
          getSequenceNumber(res, shardId1, 0),
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
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
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
    AmazonKinesis kinesis = getKinesisClientInstance();
    List<PutRecordsResultEntry> res = insertData(kinesis, generateRecordsRequests(stream));

    Task id1 = createKinesisIndexTask(
        "id1",
        DATASOURCE,
        0,
        new KinesisPartitions("stream", ImmutableMap.of(
            shardId1,
            getSequenceNumber(res, shardId1, 0),
            shardId0,
            getSequenceNumber(res, shardId0, 0)
        )),
        new KinesisPartitions("stream", ImmutableMap.of(
            shardId1,
            KinesisPartitions.NO_END_SEQUENCE_NUMBER,
            shardId0,
            KinesisPartitions.NO_END_SEQUENCE_NUMBER
        )),
        null,
        null
    );

    Task id2 = createKinesisIndexTask(
        "id2",
        DATASOURCE,
        0,
        new KinesisPartitions("stream", ImmutableMap.of(
            shardId1,
            getSequenceNumber(res, shardId1, 3),
            shardId0,
            getSequenceNumber(res, shardId0, 1)
        )),
        new KinesisPartitions("stream", ImmutableMap.of(
            shardId1,
            KinesisPartitions.NO_END_SEQUENCE_NUMBER,
            shardId0,
            KinesisPartitions.NO_END_SEQUENCE_NUMBER
        )),
        null,
        null
    );

    Task id3 = createKinesisIndexTask(
        "id3",
        DATASOURCE,
        0,
        new KinesisPartitions("stream", ImmutableMap.of(
            shardId1,
            getSequenceNumber(res, shardId1, 3),
            shardId0,
            getSequenceNumber(res, shardId0, 1)
        )),
        new KinesisPartitions("stream", ImmutableMap.of(
            shardId1,
            KinesisPartitions.NO_END_SEQUENCE_NUMBER,
            shardId0,
            KinesisPartitions.NO_END_SEQUENCE_NUMBER
        )),
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
        new KinesisDataSourceMetadata(
            null
        )
    ).anyTimes();
    expect(taskClient.getStatusAsync("id1")).andReturn(Futures.immediateFuture(SeekableStreamIndexTask.Status.PUBLISHING));
    expect(taskClient.getStatusAsync("id2")).andReturn(Futures.immediateFuture(SeekableStreamIndexTask.Status.READING));
    expect(taskClient.getStatusAsync("id3")).andReturn(Futures.immediateFuture(SeekableStreamIndexTask.Status.READING));
    expect(taskClient.getStartTimeAsync("id2")).andReturn(Futures.immediateFuture(startTime));
    expect(taskClient.getStartTimeAsync("id3")).andReturn(Futures.immediateFuture(startTime));
    expect(taskClient.getEndOffsets("id1")).andReturn(ImmutableMap.of(
        shardId1,
        getSequenceNumber(res, shardId1, 3),
        shardId0,
        getSequenceNumber(res, shardId0, 1)
    ));

    // getCheckpoints will not be called for id1 as it is in publishing state
    TreeMap<Integer, Map<String, String>> checkpoints = new TreeMap<>();
    checkpoints.put(0, ImmutableMap.of(
        shardId1,
        getSequenceNumber(res, shardId1, 3),
        shardId0,
        getSequenceNumber(res, shardId0, 1)
    ));
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("id2"), anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints))
        .times(1);
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("id3"), anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints))
        .times(1);

    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    reset(taskRunner, taskClient, taskQueue);
    expect(taskRunner.getRunningTasks()).andReturn(workItems).anyTimes();
    expect(taskClient.pauseAsync("id2"))
        .andReturn(Futures.immediateFuture((Map<String, String>) ImmutableMap.of(
            shardId1,
            getSequenceNumber(res, shardId1, 12),
            shardId0,
            getSequenceNumber(res, shardId0, 1)
        )));
    expect(taskClient.setEndOffsetsAsync("id2", ImmutableMap.of(
        shardId1,
        getSequenceNumber(res, shardId1, 12),
        shardId0,
        getSequenceNumber(res, shardId0, 1)
    ), true))
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
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();
    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));
    replayAll();

    supervisor = getSupervisor(1, 1, true, "PT1H", null, null);

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
    supervisor = getSupervisor(1, 1, true, "PT1H", null, null);
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

    KinesisDataSourceMetadata KinesisDataSourceMetadata = new KinesisDataSourceMetadata(new KinesisPartitions(
        stream,
        ImmutableMap.of(
            shardId1,
            KinesisPartitions.NO_END_SEQUENCE_NUMBER,
            shardId0,
            KinesisPartitions.NO_END_SEQUENCE_NUMBER
        )
    ));

    KinesisDataSourceMetadata resetMetadata = new KinesisDataSourceMetadata(new KinesisPartitions(
        stream,
        ImmutableMap.of(
            shardId0,
            KinesisPartitions.NO_END_SEQUENCE_NUMBER
        )
    ));

    KinesisDataSourceMetadata expectedMetadata = new KinesisDataSourceMetadata(new KinesisPartitions(
        stream,
        ImmutableMap.of(
            shardId1,
            KinesisPartitions.NO_END_SEQUENCE_NUMBER
        )
    ));

    reset(indexerMetadataStorageCoordinator);
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(KinesisDataSourceMetadata);
    expect(indexerMetadataStorageCoordinator.resetDataSourceMetadata(
        EasyMock.capture(captureDataSource),
        EasyMock.capture(captureDataSourceMetadata)
    )).andReturn(true);
    replay(indexerMetadataStorageCoordinator);

    try {
      supervisor.resetInternal(resetMetadata);
    }
    catch (NullPointerException npe) {
      // Expected as there will be an attempt to reset partitionGroups offsets to NOT_SET
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
    supervisor = getSupervisor(1, 1, true, "PT1H", null, null);
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskRunner.getRunningTasks()).andReturn(Collections.EMPTY_LIST).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();
    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    KinesisDataSourceMetadata resetMetadata = new KinesisDataSourceMetadata(new KinesisPartitions(
        stream,
        ImmutableMap.of(
            shardId0,
            KinesisPartitions.NO_END_SEQUENCE_NUMBER
        )
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
    final DateTime startTime = DateTimes.nowUtc();

    supervisor = getSupervisor(2, 1, true, "PT1H", null, null);
    AmazonKinesis kinesis = getKinesisClientInstance();
    List<PutRecordsResultEntry> res = insertData(kinesis, generateRecordsRequests(stream));

    Task id1 = createKinesisIndexTask(
        "id1",
        DATASOURCE,
        0,
        new KinesisPartitions("stream", ImmutableMap.of(
            shardId1,

            getSequenceNumber(res, shardId1, 0),

            shardId0,

            getSequenceNumber(res, shardId0, 0)
        )),
        new KinesisPartitions("stream", ImmutableMap.of(
            shardId1,
            KinesisPartitions.NO_END_SEQUENCE_NUMBER,
            shardId0,
            KinesisPartitions.NO_END_SEQUENCE_NUMBER
        )),
        null,
        null
    );

    Task id2 = createKinesisIndexTask(
        "id2",
        DATASOURCE,
        0,
        new KinesisPartitions("stream", ImmutableMap.of(
            shardId1,

            getSequenceNumber(res, shardId1, 3),

            shardId0,

            getSequenceNumber(res, shardId0, 1)
        )),
        new KinesisPartitions("stream", ImmutableMap.of(
            shardId1,
            KinesisPartitions.NO_END_SEQUENCE_NUMBER,
            shardId0,
            KinesisPartitions.NO_END_SEQUENCE_NUMBER
        )),
        null,
        null
    );

    Task id3 = createKinesisIndexTask(
        "id3",
        DATASOURCE,
        0,
        new KinesisPartitions("stream", ImmutableMap.of(
            shardId1,

            getSequenceNumber(res, shardId1, 3),

            shardId0,

            getSequenceNumber(res, shardId0, 1)
        )),
        new KinesisPartitions("stream", ImmutableMap.of(
            shardId1,
            KinesisPartitions.NO_END_SEQUENCE_NUMBER,
            shardId0,
            KinesisPartitions.NO_END_SEQUENCE_NUMBER
        )),
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
        new KinesisDataSourceMetadata(
            null
        )
    ).anyTimes();
    expect(taskClient.getStatusAsync("id1")).andReturn(Futures.immediateFuture(SeekableStreamIndexTask.Status.PUBLISHING));
    expect(taskClient.getStatusAsync("id2")).andReturn(Futures.immediateFuture(SeekableStreamIndexTask.Status.READING));
    expect(taskClient.getStatusAsync("id3")).andReturn(Futures.immediateFuture(SeekableStreamIndexTask.Status.READING));
    expect(taskClient.getStartTimeAsync("id2")).andReturn(Futures.immediateFuture(startTime));
    expect(taskClient.getStartTimeAsync("id3")).andReturn(Futures.immediateFuture(startTime));
    expect(taskClient.getEndOffsets("id1")).andReturn(ImmutableMap.of(
        shardId1,

        getSequenceNumber(res, shardId1, 3),

        shardId0,

        getSequenceNumber(res, shardId0, 1)
    ));

    TreeMap<Integer, Map<String, String>> checkpoints = new TreeMap<>();
    checkpoints.put(0, ImmutableMap.of(
        shardId1,

        getSequenceNumber(res, shardId1, 3),

        shardId0,

        getSequenceNumber(res, shardId0, 1)
    ));
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("id2"), anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints))
        .times(1);
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("id3"), anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints))
        .times(1);

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

  @Test
  public void testNoDataIngestionTasks() throws Exception
  {
    final DateTime startTime = DateTimes.nowUtc();
    supervisor = getSupervisor(2, 1, true, "PT1H", null, null);
    AmazonKinesis kinesis = getKinesisClientInstance();
    List<PutRecordsResultEntry> res = insertData(kinesis, generateRecordsRequests(stream));
    //not adding any events
    Task id1 = createKinesisIndexTask(
        "id1",
        DATASOURCE,
        0,
        new KinesisPartitions("stream", ImmutableMap.of(
            shardId1,

            getSequenceNumber(res, shardId1, 0),

            shardId0,

            getSequenceNumber(res, shardId0, 0)
        )),
        new KinesisPartitions("stream", ImmutableMap.of(
            shardId1,
            KinesisPartitions.NO_END_SEQUENCE_NUMBER,
            shardId0,
            KinesisPartitions.NO_END_SEQUENCE_NUMBER
        )),
        null,
        null
    );

    Task id2 = createKinesisIndexTask(
        "id2",
        DATASOURCE,
        0,
        new KinesisPartitions("stream", ImmutableMap.of(
            shardId1,

            getSequenceNumber(res, shardId1, 3),

            shardId0,

            getSequenceNumber(res, shardId0, 1)
        )),
        new KinesisPartitions("stream", ImmutableMap.of(
            shardId1,
            KinesisPartitions.NO_END_SEQUENCE_NUMBER,
            shardId0,
            KinesisPartitions.NO_END_SEQUENCE_NUMBER
        )),
        null,
        null
    );

    Task id3 = createKinesisIndexTask(
        "id3",
        DATASOURCE,
        0,
        new KinesisPartitions("stream", ImmutableMap.of(
            shardId1,

            getSequenceNumber(res, shardId1, 3),

            shardId0,

            getSequenceNumber(res, shardId0, 1)
        )),
        new KinesisPartitions("stream", ImmutableMap.of(
            shardId1,
            KinesisPartitions.NO_END_SEQUENCE_NUMBER,
            shardId0,
            KinesisPartitions.NO_END_SEQUENCE_NUMBER
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
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
            null
        )
    ).anyTimes();
    expect(taskClient.getStatusAsync("id1")).andReturn(Futures.immediateFuture(SeekableStreamIndexTask.Status.READING));
    expect(taskClient.getStatusAsync("id2")).andReturn(Futures.immediateFuture(SeekableStreamIndexTask.Status.READING));
    expect(taskClient.getStatusAsync("id3")).andReturn(Futures.immediateFuture(SeekableStreamIndexTask.Status.READING));
    expect(taskClient.getStartTimeAsync("id1")).andReturn(Futures.immediateFuture(startTime));
    expect(taskClient.getStartTimeAsync("id2")).andReturn(Futures.immediateFuture(startTime));
    expect(taskClient.getStartTimeAsync("id3")).andReturn(Futures.immediateFuture(startTime));

    TreeMap<Integer, Map<String, String>> checkpoints = new TreeMap<>();
    checkpoints.put(0, ImmutableMap.of(
        shardId1,

        getSequenceNumber(res, shardId1, 3),

        shardId0,

        getSequenceNumber(res, shardId0, 1)
    ));
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("id1"), anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints))
        .times(1);
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("id2"), anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints))
        .times(1);
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("id3"), anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints))
        .times(1);

    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    reset(taskQueue, indexerMetadataStorageCoordinator);
    expect(indexerMetadataStorageCoordinator.deleteDataSourceMetadata(DATASOURCE)).andReturn(true);
    taskQueue.shutdown("id1");
    taskQueue.shutdown("id2");
    taskQueue.shutdown("id3");
    replay(taskQueue, indexerMetadataStorageCoordinator);

    supervisor.resetInternal(null);
    verifyAll();
  }

  /*
  @Test(timeout = 60_000L)
  public void testCheckpointForInactiveTaskGroup()
      throws InterruptedException, ExecutionException, TimeoutException, JsonProcessingException
  {
    supervisor = getSupervisor(2, 1, true, "PT1H", null, null);
    AmazonKinesis kinesis = getKinesisClientInstance();
    List<PutRecordsResultEntry> res = insertData(kinesis, generateRecordsRequests(stream));
    //not adding any events
    final Task id1 = createKinesisIndexTask(
        "id1",
        DATASOURCE,
        0,
        new KinesisPartitions(stream, ImmutableMap.of(
            shardId1,

            getSequenceNumber(res, shardId1, 0),

            shardId0,

            getSequenceNumber(res, shardId0, 0)
        )),
        new KinesisPartitions(stream, ImmutableMap.of(
            shardId1,
            KinesisPartitions.NO_END_SEQUENCE_NUMBER,
            shardId0,
            KinesisPartitions.NO_END_SEQUENCE_NUMBER
        )),
        null,
        null
    );

    final Task id2 = createKinesisIndexTask(
        "id2",
        DATASOURCE,
        0,
        new KinesisPartitions(stream, ImmutableMap.of(
            shardId1,

            getSequenceNumber(res, shardId1, 3),

            shardId0,

            getSequenceNumber(res, shardId0, 1)
        )),
        new KinesisPartitions(stream, ImmutableMap.of(
            shardId1,
            KinesisPartitions.NO_END_SEQUENCE_NUMBER,
            shardId0,
            KinesisPartitions.NO_END_SEQUENCE_NUMBER
        )),
        null,
        null
    );

    final Task id3 = createKinesisIndexTask(
        "id3",
        DATASOURCE,
        0,
        new KinesisPartitions(stream, ImmutableMap.of(
            shardId1,

            getSequenceNumber(res, shardId1, 3),

            shardId0,

            getSequenceNumber(res, shardId0, 1)
        )),
        new KinesisPartitions(stream, ImmutableMap.of(
            shardId1,
            KinesisPartitions.NO_END_SEQUENCE_NUMBER,
            shardId0,
            KinesisPartitions.NO_END_SEQUENCE_NUMBER
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
    expect(taskClient.getStatusAsync("id1")).andReturn(Futures.immediateFuture(SeekableStreamIndexTask.Status.READING));
    expect(taskClient.getStatusAsync("id2")).andReturn(Futures.immediateFuture(SeekableStreamIndexTask.Status.READING));
    expect(taskClient.getStatusAsync("id3")).andReturn(Futures.immediateFuture(SeekableStreamIndexTask.Status.READING));

    final DateTime startTime = DateTimes.nowUtc();
    expect(taskClient.getStartTimeAsync("id1")).andReturn(Futures.immediateFuture(startTime));
    expect(taskClient.getStartTimeAsync("id2")).andReturn(Futures.immediateFuture(startTime));
    expect(taskClient.getStartTimeAsync("id3")).andReturn(Futures.immediateFuture(startTime));

    final TreeMap<Integer, Map<String, String>> checkpoints = new TreeMap<>();
    checkpoints.put(0, ImmutableMap.of(
        shardId1,

        getSequenceNumber(res, shardId1, 3),

        shardId0,

        getSequenceNumber(res, shardId0, 1)
    ));
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("id1"), anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints))
        .times(1);
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("id2"), anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints))
        .times(1);
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("id3"), anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints))
        .times(1);

    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();

    final Map<String, String> fakeCheckpoints = Collections.emptyMap();
    supervisor.moveTaskGroupToPendingCompletion(0);
    supervisor.checkpoint(
        0,
        ((KinesisIndexTask) id1).getIOConfig().getBaseSequenceName(),
        new KinesisDataSourceMetadata(new KinesisPartitions(stream, checkpoints.get(0))),
        new KinesisDataSourceMetadata(new KinesisPartitions(stream, fakeCheckpoints))
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
  public void testCheckpointForUnknownTaskGroup() throws InterruptedException
  {
    supervisor = getSupervisor(2, 1, true, "PT1S", null, null, false);
    //not adding any events
    final Task id1 = createKinesisIndexTask(
        "id1",
        DATASOURCE,
        0,
        new KinesisPartitions(stream, ImmutableMap.of(
  shardId1,

  getSequenceNumber(res, shardId1, 0),

  shardId0,

  getSequenceNumber(res, shardId0, 0)
    )),
        new KinesisPartitions(stream, ImmutableMap.of(
  shardId1,
  KinesisPartitions.NO_END_SEQUENCE_NUMBER,
  shardId0,
  KinesisPartitions.NO_END_SEQUENCE_NUMBER
        )),
        null,
        null
    );

    final Task id2 = createKinesisIndexTask(
        "id2",
        DATASOURCE,
        0,
        new KinesisPartitions(stream, ImmutableMap.of(
  shardId1,

  getSequenceNumber(res, shardId1, 3),

  shardId0,

  getSequenceNumber(res, shardId0, 1)
    )),
        new KinesisPartitions(stream, ImmutableMap.of(
  shardId1,
  KinesisPartitions.NO_END_SEQUENCE_NUMBER,
  shardId0,
  KinesisPartitions.NO_END_SEQUENCE_NUMBER
        )),
        null,
        null
    );

    final Task id3 = createKinesisIndexTask(
        "id3",
        DATASOURCE,
        0,
        new KinesisPartitions(stream, ImmutableMap.of(
  shardId1,

  getSequenceNumber(res, shardId1, 3),

  shardId0,

  getSequenceNumber(res, shardId0, 1)
    )),
        new KinesisPartitions(stream, ImmutableMap.of(
  shardId1,
  KinesisPartitions.NO_END_SEQUENCE_NUMBER,
  shardId0,
  KinesisPartitions.NO_END_SEQUENCE_NUMBER
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
        new KinesisDataSourceMetadata(new KinesisPartitions(stream, Collections.emptyMap())),
        new KinesisDataSourceMetadata(new KinesisPartitions(stream, Collections.emptyMap()))
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
        "WTH?! cannot find taskGroup [0] among all taskGroups [{}]",
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
        new KinesisPartitions(stream, ImmutableMap.of(0, 0L)),
        new KinesisPartitions(stream, ImmutableMap.of(0, Long.MAX_VALUE)),
        null,
        null
    );

    final Task id2 = createKinesisIndexTask(
        "id2",
        DATASOURCE,
        0,
        new KinesisPartitions(stream, ImmutableMap.of(0, 0L)),
        new KinesisPartitions(stream, ImmutableMap.of(0, Long.MAX_VALUE)),
        null,
        null
    );

    final Task id3 = createKinesisIndexTask(
        "id3",
        DATASOURCE,
        0,
        new KinesisPartitions(stream, ImmutableMap.of(0, 0L)),
        new KinesisPartitions(stream, ImmutableMap.of(0, Long.MAX_VALUE)),
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
    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));
    expect(taskClient.getStatusAsync(anyString()))
        .andReturn(Futures.immediateFuture(Status.READING))
        .anyTimes();
    final TreeMap<Integer, Map<String, String>> checkpoints = new TreeMap<>();
    checkpoints.put(0, ImmutableMap.of(0, 0L));
    expect(taskClient.getCheckpointsAsync(anyString(), anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints))
        .times(3);
    expect(taskClient.getStartTimeAsync(anyString())).andReturn(Futures.immediateFuture(DateTimes.nowUtc())).anyTimes();
    expect(taskClient.pauseAsync(anyString()))
        .andReturn(Futures.immediateFuture(ImmutableMap.of(0, 10L)))
        .anyTimes();
    expect(taskClient.setEndOffsetsAsync(anyString(), EasyMock.eq(ImmutableMap.of(0, 10L)), anyBoolean()))
        .andReturn(Futures.immediateFuture(true))
        .anyTimes();

    replayAll();

    supervisor.start();

    supervisor.runInternal();

    final TreeMap<Integer, Map<String, String>> newCheckpoints = new TreeMap<>();
    newCheckpoints.put(0, ImmutableMap.of(0, 10L));
    supervisor.checkpoint(
        null,
        ((KinesisIndexTask) id1).getIOConfig().getBaseSequenceName(),
        new KinesisDataSourceMetadata(new KinesisPartitions(stream, checkpoints.get(0))),
        new KinesisDataSourceMetadata(new KinesisPartitions(stream, newCheckpoints.get(0)))
    );

    while (supervisor.getNoticesQueueSize() > 0) {
      Thread.sleep(100);
    }

    verifyAll();
  }
  */

  @Test
  public void testSuspendedNoRunningTasks() throws Exception
  {
    supervisor = getSupervisor(1, 1, true, "PT1H", null, null, true);

    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KinesisDataSourceMetadata(
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

    supervisor = getSupervisor(2, 1, true, "PT1H", null, null, true);
    AmazonKinesis kinesis = getKinesisClientInstance();
    List<PutRecordsResultEntry> res = insertData(kinesis, generateRecordsRequests(stream));

    Task id1 = createKinesisIndexTask(
        "id1",
        DATASOURCE,
        0,
        new KinesisPartitions("stream", ImmutableMap.of(
            shardId1,
            getSequenceNumber(res, shardId1, 0),
            shardId0,
            getSequenceNumber(res, shardId0, 0)
        )),
        new KinesisPartitions("stream", ImmutableMap.of(
            shardId1,
            KinesisPartitions.NO_END_SEQUENCE_NUMBER,
            shardId0,
            KinesisPartitions.NO_END_SEQUENCE_NUMBER
        )),
        null,
        null
    );

    Task id2 = createKinesisIndexTask(
        "id2",
        DATASOURCE,
        0,
        new KinesisPartitions("stream", ImmutableMap.of(
            shardId1,
            getSequenceNumber(res, shardId1, 3),
            shardId0,
            getSequenceNumber(res, shardId0, 1)
        )),
        new KinesisPartitions("stream", ImmutableMap.of(
            shardId1,
            KinesisPartitions.NO_END_SEQUENCE_NUMBER,
            shardId0,
            KinesisPartitions.NO_END_SEQUENCE_NUMBER
        )),
        null,
        null
    );

    Task id3 = createKinesisIndexTask(
        "id3",
        DATASOURCE,
        0,
        new KinesisPartitions("stream", ImmutableMap.of(
            shardId1,
            getSequenceNumber(res, shardId1, 3),
            shardId0,
            getSequenceNumber(res, shardId0, 1)
        )),
        new KinesisPartitions("stream", ImmutableMap.of(
            shardId1,
            KinesisPartitions.NO_END_SEQUENCE_NUMBER,
            shardId0,
            KinesisPartitions.NO_END_SEQUENCE_NUMBER
        )),
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
        new KinesisDataSourceMetadata(
            null
        )
    ).anyTimes();
    expect(taskClient.getStatusAsync("id1")).andReturn(Futures.immediateFuture(SeekableStreamIndexTask.Status.PUBLISHING));
    expect(taskClient.getStatusAsync("id2")).andReturn(Futures.immediateFuture(SeekableStreamIndexTask.Status.READING));
    expect(taskClient.getStatusAsync("id3")).andReturn(Futures.immediateFuture(SeekableStreamIndexTask.Status.READING));
    expect(taskClient.getStartTimeAsync("id2")).andReturn(Futures.immediateFuture(startTime));
    expect(taskClient.getStartTimeAsync("id3")).andReturn(Futures.immediateFuture(startTime));
    expect(taskClient.getEndOffsets("id1")).andReturn(ImmutableMap.of(
        shardId1,
        getSequenceNumber(res, shardId1, 3),
        shardId0,
        getSequenceNumber(res, shardId0, 1)
    ));

    // getCheckpoints will not be called for id1 as it is in publishing state
    TreeMap<Integer, Map<String, String>> checkpoints = new TreeMap<>();
    checkpoints.put(0, ImmutableMap.of(
        shardId1,
        getSequenceNumber(res, shardId1, 3),
        shardId0,
        getSequenceNumber(res, shardId0, 1)
    ));
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("id2"), anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints))
        .times(1);
    expect(taskClient.getCheckpointsAsync(EasyMock.contains("id3"), anyBoolean()))
        .andReturn(Futures.immediateFuture(checkpoints))
        .times(1);

    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));

    expect(taskClient.pauseAsync("id2"))
        .andReturn(Futures.immediateFuture(ImmutableMap.of(
            shardId1,
            getSequenceNumber(res, shardId1, 12),
            shardId0,
            getSequenceNumber(res, shardId0, 1)
        )));
    expect(taskClient.setEndOffsetsAsync("id2", ImmutableMap.of(
        shardId1,
        getSequenceNumber(res, shardId1, 12),
        shardId0,
        getSequenceNumber(res, shardId0, 1)
    ), true))
        .andReturn(Futures.immediateFuture(true));
    taskQueue.shutdown("id3");
    expectLastCall().times(2);

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

    supervisor = getSupervisor(1, 1, true, "PT1H", null, null, true);
    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    reset(indexerMetadataStorageCoordinator);
    expect(indexerMetadataStorageCoordinator.deleteDataSourceMetadata(DATASOURCE)).andReturn(true);
    replay(indexerMetadataStorageCoordinator);

    supervisor.resetInternal(null);
    verifyAll();
  }


  private static List<PutRecordsResultEntry> insertData(
      AmazonKinesis kinesis,
      PutRecordsRequest req
  )
  {
    PutRecordsResult res = kinesis.putRecords(req);
    Assert.assertTrue(isResponseOk(res.getSdkHttpMetadata()));
    Assert.assertEquals((int) res.getFailedRecordCount(), 0);
    return res.getRecords();
  }

  private AmazonKinesis getKinesisClientInstance() throws InterruptedException
  {
    AmazonKinesis kinesis = cloud.localstack.TestUtils.getClientKinesis();
    SdkHttpMetadata createRes = kinesis.createStream(stream, 2).getSdkHttpMetadata();
    // sleep required because of kinesalite
    Thread.sleep(500);
    Assert.assertTrue(isResponseOk(createRes));
    return kinesis;
  }

  private static boolean isResponseOk(SdkHttpMetadata sdkHttpMetadata)
  {
    return sdkHttpMetadata.getHttpStatusCode() == 200;
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
        Localstack.getEndpointKinesis(),
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
        cloud.localstack.TestUtils.TEST_ACCESS_KEY,
        cloud.localstack.TestUtils.TEST_SECRET_KEY,
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

  private static PutRecordsRequestEntry generateRequestEntry(String partition, byte[] data)
  {
    return new PutRecordsRequestEntry().withPartitionKey(partition)
                                       .withData(ByteBuffer.wrap(data));
  }

  private static PutRecordsRequest generateRecordsRequests(String stream, int first, int last)
  {
    return new PutRecordsRequest()
        .withStreamName(stream)
        .withRecords(records.subList(first, last));
  }

  private static PutRecordsRequest generateRecordsRequests(String stream)
  {
    return new PutRecordsRequest()
        .withStreamName(stream)
        .withRecords(records);
  }

  private static byte[] JB(String timestamp, String dim1, String dim2, String dimLong, String dimFloat, String met1)
  {
    try {
      return new ObjectMapper().writeValueAsBytes(
          ImmutableMap.builder()
                      .put("timestamp", timestamp)
                      .put("dim1", dim1)
                      .put("dim2", dim2)
                      .put("dimLong", dimLong)
                      .put("dimFloat", dimFloat)
                      .put("met1", met1)
                      .build()
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private static String getSequenceNumber(List<PutRecordsResultEntry> entries, String shardId, int offset)
  {
    List<PutRecordsResultEntry> sortedEntries = entries.stream()
                                                       .filter(e -> e.getShardId().equals(shardId))
                                                       .sorted(Comparator.comparing(e -> KinesisSequenceNumber.of(e.getSequenceNumber())))
                                                       .collect(Collectors.toList());
    return sortedEntries.get(offset).getSequenceNumber();
  }

  private KinesisIndexTask createKinesisIndexTask(
      String id,
      String dataSource,
      int taskGroupId,
      KinesisPartitions startPartitions,
      KinesisPartitions endPartitions,
      DateTime minimumMessageTime,
      DateTime maximumMessageTime
  )
  {
    return new KinesisIndexTask(
        id,
        null,
        getDataSchema(dataSource),
        tuningConfig,
        new KinesisIOConfig(
            "sequenceName-" + taskGroupId,
            startPartitions,
            endPartitions,
            true,
            null,
            minimumMessageTime,
            maximumMessageTime,
            Localstack.getEndpointKinesis(),
            null,
            null,
            cloud.localstack.TestUtils.TEST_ACCESS_KEY,
            cloud.localstack.TestUtils.TEST_SECRET_KEY,
            null,
            null,
            null,
            false
        ),
        Collections.emptyMap(),
        null,
        null,
        rowIngestionMetersFactory
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
        Localstack.getEndpointKinesis(),
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
        cloud.localstack.TestUtils.TEST_ACCESS_KEY,
        cloud.localstack.TestUtils.TEST_SECRET_KEY,
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
            rowIngestionMetersFactory
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

  private static class TestableKinesisSupervisor extends KinesisSupervisor
  {
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
          rowIngestionMetersFactory
      );
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
  }

}
