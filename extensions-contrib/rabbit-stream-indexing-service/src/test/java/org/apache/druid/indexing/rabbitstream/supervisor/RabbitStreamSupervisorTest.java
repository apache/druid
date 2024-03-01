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

package org.apache.druid.indexing.rabbitstream.supervisor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskQueue;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManagerConfig;
import org.apache.druid.indexing.rabbitstream.RabbitStreamIndexTaskClientFactory;
import org.apache.druid.indexing.rabbitstream.RabbitStreamRecordSupplier;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskClient;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorReportPayload;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.metrics.DruidMonitorSchedulerConfig;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.incremental.RowIngestionMetersFactory;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.easymock.EasyMockSupport;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class RabbitStreamSupervisorTest extends EasyMockSupport
{
  private static final ObjectMapper OBJECT_MAPPER = TestHelper.makeJsonMapper();
  private static final InputFormat INPUT_FORMAT = new JsonInputFormat(
      new JSONPathSpec(true, ImmutableList.of()),
      ImmutableMap.of(),
      false,
      false,
      false,
      false);
  private static final String DATASOURCE = "testDS";
  private static final int TEST_CHAT_THREADS = 3;
  private static final long TEST_CHAT_RETRIES = 9L;
  private static final Period TEST_HTTP_TIMEOUT = new Period("PT10S");
  private static final Period TEST_SHUTDOWN_TIMEOUT = new Period("PT80S");
  private static final String STREAM = "stream";
  private static final String URI = "rabbitmq-stream://localhost:5552";

  private static DataSchema dataSchema;
  private RabbitStreamRecordSupplier supervisorRecordSupplier;

  private final int numThreads = 1;
  private RabbitStreamSupervisor supervisor;
  private RabbitStreamSupervisorTuningConfig tuningConfig;
  private TaskStorage taskStorage;
  private TaskMaster taskMaster;
  private TaskRunner taskRunner;
  private IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator;
  private SeekableStreamIndexTaskClient<String, String> taskClient;
  private TaskQueue taskQueue;
  private RowIngestionMetersFactory rowIngestionMetersFactory;
  private StubServiceEmitter serviceEmitter;
  private SupervisorStateManagerConfig supervisorConfig;

  private static DataSchema getDataSchema(String dataSource)
  {
    List<DimensionSchema> dimensions = new ArrayList<>();
    dimensions.add(StringDimensionSchema.create("dim1"));
    dimensions.add(StringDimensionSchema.create("dim2"));

    return new DataSchema(
        dataSource,
        new TimestampSpec("timestamp", "iso", null),
        new DimensionsSpec(dimensions),
        new AggregatorFactory[] {new CountAggregatorFactory("rows")},
        new UniformGranularitySpec(
            Granularities.HOUR,
            Granularities.NONE,
            ImmutableList.of()),
        null);
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
    taskQueue = createMock(TaskQueue.class);
    supervisorRecordSupplier = createMock(RabbitStreamRecordSupplier.class);

    tuningConfig = new RabbitStreamSupervisorTuningConfig(
        null,
        1000, // max rows in memory
        null, // max bytes
        null, // skipBytes
        50000, // max rows per seg
        null, // max total rows
        new Period("P1Y"), // intermediatepersistPeriod
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        numThreads, // worker threads
        null,
        TEST_CHAT_THREADS,
        TEST_CHAT_RETRIES,
        TEST_HTTP_TIMEOUT,
        TEST_SHUTDOWN_TIMEOUT,
        1000,
        100,
        null,
        null,
        null,
        null,
        null,
        null,
        100);
    rowIngestionMetersFactory = new TestUtils().getRowIngestionMetersFactory();
    serviceEmitter = new StubServiceEmitter("RabbitStreamSupervisorTest", "localhost");
    EmittingLogger.registerEmitter(serviceEmitter);
    supervisorConfig = new SupervisorStateManagerConfig();
  }

  @After
  public void tearDownTest()
  {
    supervisor = null;
  }

  /**
   * Use for tests where you don't want generateSequenceName to be overridden out
   */
  private RabbitStreamSupervisor getSupervisor(
      int replicas,
      int taskCount,
      boolean useEarliestOffset,
      String duration,
      Period lateMessageRejectionPeriod,
      Period earlyMessageRejectionPeriod,
      DataSchema dataSchema,
      RabbitStreamSupervisorTuningConfig tuningConfig)
  {
    RabbitStreamSupervisorIOConfig rabbitStreamSupervisorIOConfig = new RabbitStreamSupervisorIOConfig(
        STREAM, // stream
        URI, // uri
        INPUT_FORMAT, // inputFormat
        replicas, // replicas
        taskCount, // taskCount
        new Period(duration), // taskDuration
        null, // consumerProperties
        null, // autoscalerConfig
        400L, // poll timeout
        new Period("P1D"), // start delat
        new Period("PT30M"), // period
        new Period("PT30S"), // completiontimeout
        false, // useearliest
        lateMessageRejectionPeriod, // latemessagerejection
        earlyMessageRejectionPeriod, // early message rejection
        null, // latemessagerejectionstartdatetime
        1
    );
    RabbitStreamIndexTaskClientFactory clientFactory = new RabbitStreamIndexTaskClientFactory(null,
        OBJECT_MAPPER);
    RabbitStreamSupervisor supervisor = new RabbitStreamSupervisor(
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        clientFactory,
        OBJECT_MAPPER,
        new RabbitStreamSupervisorSpec(
            null,
            dataSchema,
            tuningConfig,
            rabbitStreamSupervisorIOConfig,
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
            new SupervisorStateManagerConfig()),
        rowIngestionMetersFactory);
    return supervisor;
  }

  public RabbitStreamSupervisor getDefaultSupervisor()
  {
    return getSupervisor(
        1,
        1,
        false,
        "PT30M",
        null,
        null,
        RabbitStreamSupervisorTest.dataSchema,
        tuningConfig);
  }

  @Test
  public void testRecordSupplier()
  {
    RabbitStreamSupervisorIOConfig rabbitStreamSupervisorIOConfig = new RabbitStreamSupervisorIOConfig(
        STREAM, // stream
        URI, // uri
        INPUT_FORMAT, // inputFormat
        1, // replicas
        1, // taskCount
        new Period("PT30M"), // taskDuration
        null, // consumerProperties
        null, // autoscalerConfig
        400L, // poll timeout
        new Period("P1D"), // start delat
        new Period("PT30M"), // period
        new Period("PT30S"), // completiontimeout
        false, // useearliest
        null, // latemessagerejection
        null, // early message rejection
        null, // latemessagerejectionstartdatetime
        1
    );
    RabbitStreamIndexTaskClientFactory clientFactory = new RabbitStreamIndexTaskClientFactory(null,
        OBJECT_MAPPER);
    RabbitStreamSupervisor supervisor = new RabbitStreamSupervisor(
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        clientFactory,
        OBJECT_MAPPER,
        new RabbitStreamSupervisorSpec(
            null,
            dataSchema,
            tuningConfig,
            rabbitStreamSupervisorIOConfig,
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
            new SupervisorStateManagerConfig()),
        rowIngestionMetersFactory);

    RabbitStreamRecordSupplier supplier = (RabbitStreamRecordSupplier) supervisor.setupRecordSupplier();
    Assert.assertNotNull(supplier);
    Assert.assertEquals(0, supplier.bufferSize());
    Assert.assertEquals(Collections.emptySet(), supplier.getAssignment());
    Assert.assertEquals(false, supplier.isRunning());
  }

  @Test
  public void testGetters()
  {
    supervisor = getDefaultSupervisor();
    Assert.assertNull(supervisor.getPartitionTimeLag());

    Assert.assertNull(supervisor.getTimeLagPerPartition(null));
    Assert.assertFalse(supervisor.isEndOfShard(null));
    Assert.assertFalse(supervisor.isShardExpirationMarker(null));

    Assert.assertEquals(Long.valueOf(Long.MAX_VALUE), supervisor.getEndOfPartitionMarker());

    Assert.assertEquals("index_rabbit", supervisor.baseTaskName());

    Assert.assertEquals(Long.valueOf(-1L), supervisor.getNotSetMarker());
    Assert.assertEquals(false, supervisor.useExclusiveStartSequenceNumberForNonFirstSequence());

  }

  @Test
  public void testTaskGroupID()
  {

    List<Integer> taskCounts = ImmutableList.of(1, 2, 3, 4);
    List<String> partitions = ImmutableList.of("a", "b", "c");

    for (Integer taskCount : taskCounts) {
      supervisor = getSupervisor(
          1,
          taskCount,
          false,
          "PT30M",
          null,
          null,
          RabbitStreamSupervisorTest.dataSchema,
          tuningConfig);
      for (String partition : partitions) {
        Assert.assertEquals(partition.hashCode() % taskCount, supervisor.getTaskGroupIdForPartition(partition));
      }
    }
  }

  @Test
  public void testReportPayload()
  {
    supervisor = getSupervisor(
        1,
        1,
        false,
        "PT30M",
        null,
        null,
        RabbitStreamSupervisorTest.dataSchema,
        tuningConfig);

    SeekableStreamSupervisorReportPayload<String, Long> payload = supervisor.createReportPayload(1, false);
    Assert.assertEquals(STREAM, payload.getStream());
    Assert.assertEquals(1, payload.getPartitions());
    Assert.assertEquals(1, payload.getReplicas());
    Assert.assertEquals(false, payload.isSuspended());
    Assert.assertEquals(true, payload.isHealthy());
    Assert.assertEquals(30 * 60, payload.getDurationSeconds());
  }

}
