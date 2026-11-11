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
import com.google.common.collect.ImmutableSet;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.granularity.UniformGranularitySpec;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskQueue;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManagerConfig;
import org.apache.druid.indexing.rabbitstream.RabbitStreamIndexTask;
import org.apache.druid.indexing.rabbitstream.RabbitStreamIndexTaskClientFactory;
import org.apache.druid.indexing.rabbitstream.RabbitStreamRecordSupplier;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTask;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskClient;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskIOConfig;
import org.apache.druid.indexing.seekablestream.supervisor.BoundedStreamConfig;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorReportPayload;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.metrics.DruidMonitorSchedulerConfig;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.incremental.RowIngestionMetersFactory;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.joda.time.Period;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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

    return DataSchema.builder()
                     .withDataSource(dataSource)
                     .withTimestamp(new TimestampSpec("timestamp", "iso", null))
                     .withDimensions(dimensions)
                     .withAggregators(new CountAggregatorFactory("rows"))
                     .withGranularity(
                         new UniformGranularitySpec(
                             Granularities.HOUR,
                             Granularities.NONE,
                             ImmutableList.of()
                         )
                     )
                     .build();
  }

  @BeforeAll
  public static void setupClass()
  {
    dataSchema = getDataSchema(DATASOURCE);
  }

  @BeforeEach
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
        100,
        null
    );
    rowIngestionMetersFactory = new TestUtils().getRowIngestionMetersFactory();
    serviceEmitter = new StubServiceEmitter("RabbitStreamSupervisorTest", "localhost");
    EmittingLogger.registerEmitter(serviceEmitter);
    supervisorConfig = new SupervisorStateManagerConfig();
  }

  @AfterEach
  public void tearDownTest()
  {
    supervisor = null;
  }

  /**
   * Use for tests where you don't want generateSequenceName to be overridden out
   */
  private RabbitStreamSupervisor getSupervisor(
      final @Nullable String id,
      int replicas,
      int taskCount,
      boolean useEarliestOffset,
      String duration,
      Period lateMessageRejectionPeriod,
      Period earlyMessageRejectionPeriod,
      DataSchema dataSchema,
      RabbitStreamSupervisorTuningConfig tuningConfig)
  {
    RabbitStreamSupervisorIOConfig rabbitStreamSupervisorIOConfig = new RabbitStreamIOConfigBuilder()
        .withStream(STREAM)
        .withUri(URI)
        .withInputFormat(INPUT_FORMAT)
        .withReplicas(replicas)
        .withTaskCount(taskCount)
        .withTaskDuration(new Period(duration))
        .withPollTimeout(400L)
        .withStartDelay(new Period("P1D"))
        .withSupervisorRunPeriod(new Period("PT30M"))
        .withCompletionTimeout(new Period("PT30S"))
        .withUseEarliestSequenceNumber(false)
        .withLateMessageRejectionPeriod(lateMessageRejectionPeriod)
        .withEarlyMessageRejectionPeriod(earlyMessageRejectionPeriod)
        .withStopTaskCount(1)
        .build();
    RabbitStreamIndexTaskClientFactory clientFactory = new RabbitStreamIndexTaskClientFactory(null,
        OBJECT_MAPPER);
    RabbitStreamSupervisor supervisor = new RabbitStreamSupervisor(
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        clientFactory,
        OBJECT_MAPPER,
        new RabbitStreamSupervisorSpec(
            id,
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
        null,
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
    RabbitStreamSupervisorIOConfig rabbitStreamSupervisorIOConfig = new RabbitStreamIOConfigBuilder()
        .withStream(STREAM)
        .withUri(URI)
        .withInputFormat(INPUT_FORMAT)
        .withReplicas(1)
        .withTaskCount(1)
        .withTaskDuration(new Period("PT30M"))
        .withPollTimeout(400L)
        .withStartDelay(new Period("P1D"))
        .withSupervisorRunPeriod(new Period("PT30M"))
        .withCompletionTimeout(new Period("PT30S"))
        .withUseEarliestSequenceNumber(false)
        .withStopTaskCount(1)
        .build();
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
    Assertions.assertNotNull(supplier);
    Assertions.assertEquals(0, supplier.bufferSize());
    Assertions.assertEquals(Collections.emptySet(), supplier.getAssignment());
    Assertions.assertEquals(false, supplier.isRunning());
  }

  @Test
  public void testGetters()
  {
    supervisor = getDefaultSupervisor();
    Assertions.assertNull(supervisor.getPartitionTimeLag());

    Assertions.assertNull(supervisor.getTimeLagPerPartition(null));
    Assertions.assertFalse(supervisor.isEndOfShard(null));
    Assertions.assertFalse(supervisor.isShardExpirationMarker(null));

    Assertions.assertEquals(Long.valueOf(Long.MAX_VALUE), supervisor.getEndOfPartitionMarker());

    Assertions.assertEquals("index_rabbit", supervisor.baseTaskName());

    Assertions.assertEquals(Long.valueOf(-1L), supervisor.getNotSetMarker());
    Assertions.assertEquals(false, supervisor.useExclusiveStartSequenceNumberForNonFirstSequence());

  }

  @Test
  public void testTaskGroupID()
  {

    List<Integer> taskCounts = ImmutableList.of(1, 2, 3, 4);
    List<String> partitions = ImmutableList.of("a", "b", "c");

    for (Integer taskCount : taskCounts) {
      supervisor = getSupervisor(
          null,
          1,
          taskCount,
          false,
          "PT30M",
          null,
          null,
          RabbitStreamSupervisorTest.dataSchema,
          tuningConfig);
      for (String partition : partitions) {
        Assertions.assertEquals(partition.hashCode() % taskCount, supervisor.getTaskGroupIdForPartition(partition));
      }
    }
  }

  @Test
  public void testReportPayload()
  {
    supervisor = getSupervisor(
        null,
        1,
        1,
        false,
        "PT30M",
        null,
        null,
        RabbitStreamSupervisorTest.dataSchema,
        tuningConfig);

    SeekableStreamSupervisorReportPayload<String, Long> payload = supervisor.createReportPayload(1, false);
    Assertions.assertEquals(STREAM, payload.getStream());
    Assertions.assertEquals(1, payload.getPartitions());
    Assertions.assertEquals(1, payload.getReplicas());
    Assertions.assertEquals(false, payload.isSuspended());
    Assertions.assertEquals(true, payload.isHealthy());
    Assertions.assertEquals(30 * 60, payload.getDurationSeconds());
  }

  @Test
  public void testCreateTaskIOConfig()
  {
    supervisor = getSupervisor(
        null,
        1,
        1,
        false,
        "PT30M",
        null,
        null,
        RabbitStreamSupervisorTest.dataSchema,
        tuningConfig
    );

    SeekableStreamIndexTaskIOConfig ioConfig = supervisor.createTaskIoConfig(
        1,
        ImmutableMap.of(),
        ImmutableMap.of(),
        "test",
        null,
        null,
        ImmutableSet.of(),
        new RabbitStreamIOConfigBuilder()
            .withStream(STREAM)
            .withUri(URI)
            .withInputFormat(INPUT_FORMAT)
            .withReplicas(1)
            .withTaskCount(1)
            .withTaskDuration(new Period("PT30M"))
            .withPollTimeout(400L)
            .withStartDelay(new Period("P1D"))
            .withSupervisorRunPeriod(new Period("PT30M"))
            .withCompletionTimeout(new Period("PT30S"))
            .withUseEarliestSequenceNumber(false)
            .withStopTaskCount(1)
            .build()
    );

    Assertions.assertEquals(30L, ioConfig.getRefreshRejectionPeriodsInMinutes().longValue());
  }

  @Test
  public void test_doesTaskMatchSupervisor()
  {
    supervisor = getSupervisor(
        "supervisorId",
        1,
        1,
        false,
        "PT30M",
        null,
        null,
        RabbitStreamSupervisorTest.dataSchema,
        tuningConfig
    );

    RabbitStreamIndexTask rabbitTaskMatch = createMock(RabbitStreamIndexTask.class);
    EasyMock.expect(rabbitTaskMatch.getSupervisorId()).andReturn("supervisorId");
    EasyMock.replay(rabbitTaskMatch);

    Assertions.assertTrue(supervisor.doesTaskMatchSupervisor(rabbitTaskMatch));

    RabbitStreamIndexTask rabbitTaskNoMatch = createMock(RabbitStreamIndexTask.class);
    EasyMock.expect(rabbitTaskNoMatch.getSupervisorId()).andReturn(dataSchema.getDataSource());
    EasyMock.replay(rabbitTaskNoMatch);

    Assertions.assertFalse(supervisor.doesTaskMatchSupervisor(rabbitTaskNoMatch));

    SeekableStreamIndexTask differentTaskType = createMock(SeekableStreamIndexTask.class);
    EasyMock.expect(differentTaskType.getSupervisorId()).andReturn("supervisorId");
    EasyMock.replay(differentTaskType);

    Assertions.assertFalse(supervisor.doesTaskMatchSupervisor(differentTaskType));
  }

  @Test
  public void testBoundedModeCreateTasksWithCorrectOffsets()
  {
    Map<String, Object> startOffsets = ImmutableMap.of(
        "queue-0", 100,
        "queue-1", 200
    );
    Map<String, Object> endOffsets = ImmutableMap.of(
        "queue-0", 500,
        "queue-1", 600
    );

    final RabbitStreamSupervisorIOConfig rabbitSupervisorIOConfig = new RabbitStreamIOConfigBuilder()
        .withStream(STREAM)
        .withUri(URI)
        .withInputFormat(INPUT_FORMAT)
        .withReplicas(1)
        .withTaskCount(1)
        .withTaskDuration(new Period("PT30S"))
        .withStartDelay(new Period("PT30M"))
        .withStopTaskCount(1000)
        .withBoundedStreamConfig(new BoundedStreamConfig(startOffsets, endOffsets))
        .build();

    Assertions.assertTrue(rabbitSupervisorIOConfig.isBounded());

    final RabbitStreamIndexTaskClientFactory taskClientFactory = new RabbitStreamIndexTaskClientFactory(null, OBJECT_MAPPER);
    final RabbitStreamSupervisorSpec spec = new RabbitStreamSupervisorSpec(
        null,
        null,
        dataSchema,
        tuningConfig,
        rabbitSupervisorIOConfig,
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

    supervisor = new RabbitStreamSupervisor(
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        taskClientFactory,
        OBJECT_MAPPER,
        spec,
        rowIngestionMetersFactory
    );

    // Test type conversion methods
    String queueName = supervisor.createPartitionIdFromString("queue-0");
    Assertions.assertEquals("queue-0", queueName);

    Long offset = supervisor.createSequenceOffsetFromObject(100);
    Assertions.assertEquals(Long.valueOf(100L), offset);

    offset = supervisor.createSequenceOffsetFromObject("200");
    Assertions.assertEquals(Long.valueOf(200L), offset);

    // Test isOffsetAtOrBeyond
    Assertions.assertTrue(supervisor.isOffsetAtOrBeyond(500L, 100L));
    Assertions.assertTrue(supervisor.isOffsetAtOrBeyond(100L, 100L));
    Assertions.assertFalse(supervisor.isOffsetAtOrBeyond(50L, 100L));
  }

  @Test
  public void testCreateSequenceOffsetFromObject_invalidType()
  {
    supervisor = getDefaultSupervisor();

    Exception e = Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> supervisor.createSequenceOffsetFromObject(new Object())
    );
    Assertions.assertTrue(e.getMessage().contains("Cannot convert"));
  }
}
