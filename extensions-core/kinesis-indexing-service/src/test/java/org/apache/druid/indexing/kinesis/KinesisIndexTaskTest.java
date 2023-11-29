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

package org.apache.druid.indexing.kinesis;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.name.Named;
import org.apache.druid.common.aws.AWSCredentialsConfig;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.FloatDimensionSchema;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.IngestionStatsAndErrorsTaskReportData;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.common.task.IndexTaskTest;
import org.apache.druid.indexing.common.task.ParseExceptionReport;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.indexing.kinesis.supervisor.KinesisSupervisor;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.seekablestream.SeekableStreamEndSequenceNumbers;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskRunner;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskTestBase;
import org.apache.druid.indexing.seekablestream.SeekableStreamStartSequenceNumbers;
import org.apache.druid.indexing.seekablestream.SequenceMetadata;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.indexing.seekablestream.common.StreamPartition;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisor;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.core.NoopEmitter;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.DefaultQueryRunnerFactoryConglomerate;
import org.apache.druid.query.DruidProcessingConfigTest;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryEngine;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.query.timeseries.TimeseriesQueryRunnerFactory;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.incremental.RowMeters;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.transform.ExpressionTransform;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.timeline.DataSegment;
import org.easymock.EasyMock;
import org.joda.time.Period;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.stream.Collectors;


@SuppressWarnings("unchecked")
@RunWith(Parameterized.class)
public class KinesisIndexTaskTest extends SeekableStreamIndexTaskTestBase
{
  private static final ObjectMapper OBJECT_MAPPER = TestHelper.makeJsonMapper();
  private static final String STREAM = "stream";
  private static final String SHARD_ID1 = "1";
  private static final String SHARD_ID0 = "0";

  private static final List<KinesisRecord> RECORDS = Arrays.asList(
      createRecord("1", "0", jb("2008", "a", "y", "10", "20.0", "1.0")),
      createRecord("1", "1", jb("2009", "b", "y", "10", "20.0", "1.0")),
      createRecord("1", "2", jb("2010", "c", "y", "10", "20.0", "1.0")),
      createRecord("1", "3", jb("2011", "d", "y", "10", "20.0", "1.0")),
      createRecord("1", "4", jb("2011", "e", "y", "10", "20.0", "1.0")),
      createRecord("1", "5", jb("246140482-04-24T15:36:27.903Z", "x", "z", "10", "20.0", "1.0")),
      createRecord("1", "6", new ByteEntity(StringUtils.toUtf8("unparseable"))),
      createRecord("1", "7", new ByteEntity(StringUtils.toUtf8(""))),
      createRecord("1", "8", new ByteEntity(StringUtils.toUtf8("{}"))),
      createRecord("1", "9", jb("2013", "f", "y", "10", "20.0", "1.0")),
      createRecord("1", "10", jb("2049", "f", "y", "notanumber", "20.0", "1.0")),
      createRecord("1", "11", jb("2049", "f", "y", "10", "notanumber", "1.0")),
      createRecord("1", "12", jb("2049", "f", "y", "10", "20.0", "notanumber")),
      createRecord("0", "0", jb("2012", "g", "y", "10", "20.0", "1.0")),
      createRecord("0", "1", jb("2011", "h", "y", "10", "20.0", "1.0"))
  );

  private static final List<KinesisRecord> SINGLE_PARTITION_RECORDS = Arrays.asList(
      createRecord("1", "0", jb("2008", "a", "y", "10", "20.0", "1.0")),
      createRecord("1", "1", jb("2009", "b", "y", "10", "20.0", "1.0")),
      createRecord("1", "2", jb("2010", "c", "y", "10", "20.0", "1.0")),
      createRecord("1", "3", jb("2011", "d", "y", "10", "20.0", "1.0")),
      createRecord("1", "4", jb("2011", "e", "y", "10", "20.0", "1.0")),
      createRecord("1", "5", jb("2012", "a", "y", "10", "20.0", "1.0")),
      createRecord("1", "6", jb("2013", "b", "y", "10", "20.0", "1.0")),
      createRecord("1", "7", jb("2010", "c", "y", "10", "20.0", "1.0")),
      createRecord("1", "8", jb("2011", "d", "y", "10", "20.0", "1.0")),
      createRecord("1", "9", jb("2011", "e", "y", "10", "20.0", "1.0")),
      createRecord("1", "10", jb("2008", "a", "y", "10", "20.0", "1.0")),
      createRecord("1", "11", jb("2009", "b", "y", "10", "20.0", "1.0")),
      createRecord("1", "12", jb("2010", "c", "y", "10", "20.0", "1.0")),
      createRecord("1", "13", jb("2012", "d", "y", "10", "20.0", "1.0")),
      createRecord("1", "14", jb("2013", "e", "y", "10", "20.0", "1.0"))
  );

  private static KinesisRecordSupplier recordSupplier;
  private static ServiceEmitter emitter;

  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> constructorFeeder()
  {
    return ImmutableList.of(
        new Object[]{LockGranularity.TIME_CHUNK},
        new Object[]{LockGranularity.SEGMENT}
    );
  }

  private long handoffConditionTimeout = 0;
  private boolean reportParseExceptions = false;
  private boolean logParseExceptions = true;
  private Integer maxParseExceptions = null;
  private Integer maxSavedParseExceptions = null;
  private boolean doHandoff = true;
  private Integer maxRowsPerSegment = null;
  private Long maxTotalRows = null;
  private final Period intermediateHandoffPeriod = null;
  private int maxRecordsPerPoll;
  private int maxBytesPerPoll;

  @BeforeClass
  public static void setupClass()
  {
    emitter = new ServiceEmitter(
        "service",
        "host",
        new NoopEmitter()
    );
    emitter.start();
    EmittingLogger.registerEmitter(emitter);
    taskExec = MoreExecutors.listeningDecorator(
        Executors.newCachedThreadPool(
            Execs.makeThreadFactory("kinesis-task-test-%d")
        )
    );
  }

  public KinesisIndexTaskTest(LockGranularity lockGranularity)
  {
    super(lockGranularity);
  }

  @Before
  public void setupTest() throws IOException, InterruptedException
  {
    handoffConditionTimeout = 0;
    reportParseExceptions = false;
    logParseExceptions = true;
    maxParseExceptions = null;
    maxSavedParseExceptions = null;
    doHandoff = true;
    reportsFile = File.createTempFile("KinesisIndexTaskTestReports-" + System.currentTimeMillis(), "json");
    maxRecordsPerPoll = 1;
    maxBytesPerPoll = 1_000_000;

    recordSupplier = mock(KinesisRecordSupplier.class);

    // sleep required because of kinesalite
    Thread.sleep(500);
    makeToolboxFactory();
  }

  @After
  public void tearDownTest()
  {
    synchronized (runningTasks) {
      for (Task task : runningTasks) {
        task.stopGracefully(toolboxFactory.build(task).getConfig());
      }

      runningTasks.clear();
    }
    reportsFile.delete();
    destroyToolboxFactory();
  }

  @AfterClass
  public static void tearDownClass() throws Exception
  {
    taskExec.shutdown();
    taskExec.awaitTermination(20, TimeUnit.MINUTES);
    emitter.close();
  }

  private void waitUntil(KinesisIndexTask task, Predicate<KinesisIndexTask> predicate)
      throws InterruptedException
  {
    while (!predicate.test(task)) {
      Thread.sleep(10);
    }
  }

  private long getTotalSize(List<KinesisRecord> records, int startIndexInclusive, int endIndexExclusive)
  {
    return records.subList(startIndexInclusive, endIndexExclusive)
                  .stream().flatMap(record -> record.getData().stream())
                  .mapToLong(entity -> entity.getBuffer().remaining()).sum();
  }

  private static KinesisRecord clone(KinesisRecord record)
  {
    return new KinesisRecord(
        record.getStream(),
        record.getPartitionId(),
        record.getSequenceNumber(),
        record.getData().stream()
              .map(entity -> new ByteEntity(entity.getBuffer()))
              .collect(Collectors.toList())
    );
  }

  private static List<OrderedPartitionableRecord<String, String, ByteEntity>> clone(
      List<KinesisRecord> records,
      int start,
      int end
  )
  {
    return clone(records).subList(start, end);
  }

  /**
   * Records can only be read once, hence we must use fresh records every time.
   */
  private static List<OrderedPartitionableRecord<String, String, ByteEntity>> clone(
      List<KinesisRecord> records
  )
  {
    return records.stream().map(KinesisIndexTaskTest::clone).collect(Collectors.toList());
  }

  private static KinesisRecord createRecord(String partitionId, String sequenceNumber, ByteEntity entity)
  {
    return new KinesisRecord(STREAM, partitionId, sequenceNumber, Collections.singletonList(entity));
  }

  @Test(timeout = 120_000L)
  public void testRunAfterDataInserted() throws Exception
  {
    recordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();

    recordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.poll(EasyMock.anyLong()))
            .andReturn(clone(RECORDS, 2, 5)).once();

    recordSupplier.close();
    EasyMock.expectLastCall().once();

    replayAll();

    final KinesisIndexTask task = createTask(
        0,
        ImmutableMap.of(SHARD_ID1, "2"),
        ImmutableMap.of(SHARD_ID1, "4")
    );
    Assert.assertTrue(task.supportsQueries());

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    verifyAll();

    verifyTaskMetrics(task, RowMeters.with().bytes(getTotalSize(RECORDS, 2, 5))
                                     .totalProcessed(3));

    // Check published metadata and segments in deep storage
    assertEqualsExceptVersion(
        ImmutableList.of(
            sdd("2010/P1D", 0, ImmutableList.of("c")),
            sdd("2011/P1D", 0, ImmutableList.of("d", "e"))
        ),
        publishedDescriptors()
    );
    Assert.assertEquals(
        new KinesisDataSourceMetadata(
            new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "4"))
        ),
        newDataSchemaMetadata()
    );
  }

  @Test(timeout = 120_000L)
  public void testIngestNullColumnAfterDataInserted() throws Exception
  {
    recordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();

    recordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.poll(EasyMock.anyLong()))
            .andReturn(clone(RECORDS, 2, 5)).once();

    recordSupplier.close();
    EasyMock.expectLastCall().once();

    replayAll();

    final DimensionsSpec dimensionsSpec = new DimensionsSpec(
        ImmutableList.of(
            new StringDimensionSchema("dim1"),
            new StringDimensionSchema("dim1t"),
            new StringDimensionSchema("dim2"),
            new LongDimensionSchema("dimLong"),
            new FloatDimensionSchema("dimFloat"),
            new StringDimensionSchema("unknownDim")
        )
    );
    final KinesisIndexTask task = createTask(
        NEW_DATA_SCHEMA.withDimensionsSpec(dimensionsSpec),
        ImmutableMap.of(SHARD_ID1, "2"),
        ImmutableMap.of(SHARD_ID1, "4")
    );
    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    verifyAll();

    final Collection<DataSegment> segments = publishedSegments();
    for (DataSegment segment : segments) {
      for (int i = 0; i < dimensionsSpec.getDimensions().size(); i++) {
        Assert.assertEquals(dimensionsSpec.getDimensionNames().get(i), segment.getDimensions().get(i));
      }
    }
  }

  @Test(timeout = 120_000L)
  public void testIngestNullColumnAfterDataInserted_storeEmptyColumnsOff_shouldNotStoreEmptyColumns() throws Exception
  {
    recordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();

    recordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.poll(EasyMock.anyLong()))
            .andReturn(clone(RECORDS, 2, 5)).once();

    recordSupplier.close();
    EasyMock.expectLastCall().once();

    replayAll();

    final KinesisIndexTask task = createTask(
        NEW_DATA_SCHEMA.withDimensionsSpec(
            new DimensionsSpec(
                ImmutableList.of(
                    new StringDimensionSchema("dim1"),
                    new StringDimensionSchema("dim1t"),
                    new StringDimensionSchema("dim2"),
                    new LongDimensionSchema("dimLong"),
                    new FloatDimensionSchema("dimFloat"),
                    new StringDimensionSchema("unknownDim")
                )
            )
        ),
        ImmutableMap.of(SHARD_ID1, "2"),
        ImmutableMap.of(SHARD_ID1, "4")
    );
    task.addToContext(Tasks.STORE_EMPTY_COLUMNS_KEY, false);
    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    verifyAll();

    final Collection<DataSegment> segments = publishedSegments();
    for (DataSegment segment : segments) {
      Assert.assertFalse(segment.getDimensions().contains("unknownDim"));
    }
  }

  @Test(timeout = 120_000L)
  public void testRunAfterDataInsertedWithLegacyParser() throws Exception
  {
    recordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();

    recordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.poll(EasyMock.anyLong()))
            .andReturn(clone(RECORDS, 2, 5)).once();

    recordSupplier.close();
    EasyMock.expectLastCall().once();

    replayAll();

    final KinesisIndexTask task = createTask(
        OLD_DATA_SCHEMA,
        ImmutableMap.of(SHARD_ID1, "2"),
        ImmutableMap.of(SHARD_ID1, "4")
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    verifyAll();
    verifyTaskMetrics(task, RowMeters.with().bytes(getTotalSize(RECORDS, 2, 5))
                                     .totalProcessed(3));

    // Check published metadata and segments in deep storage
    assertEqualsExceptVersion(
        ImmutableList.of(
            sdd("2010/P1D", 0, ImmutableList.of("c")),
            sdd("2011/P1D", 0, ImmutableList.of("d", "e"))
        ),
        publishedDescriptors()
    );
    Assert.assertEquals(
        new KinesisDataSourceMetadata(
            new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "4"))
        ),
        newDataSchemaMetadata()
    );
  }

  DataSourceMetadata newDataSchemaMetadata()
  {
    return metadataStorageCoordinator.retrieveDataSourceMetadata(NEW_DATA_SCHEMA.getDataSource());
  }

  @Test(timeout = 120_000L)
  public void testRunBeforeDataInserted() throws Exception
  {

    recordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();

    recordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.poll(EasyMock.anyLong()))
            .andReturn(Collections.emptyList()).times(5)
            .andReturn(clone(RECORDS, 13, 15)).once();

    recordSupplier.close();
    EasyMock.expectLastCall().once();

    replayAll();

    final KinesisIndexTask task = createTask(
        0,
        ImmutableMap.of(SHARD_ID0, "0"),
        ImmutableMap.of(SHARD_ID0, "1")
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    verifyAll();
    verifyTaskMetrics(task, RowMeters.with().bytes(getTotalSize(RECORDS, 13, 15))
                                     .totalProcessed(2));

    // Check published metadata and segments in deep storage
    assertEqualsExceptVersion(
        ImmutableList.of(
            sdd("2011/P1D", 0, ImmutableList.of("h")),
            sdd("2012/P1D", 0, ImmutableList.of("g"))
        ),
        publishedDescriptors()
    );
    Assert.assertEquals(
        new KinesisDataSourceMetadata(
            new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID0, "1"))
        ),
        newDataSchemaMetadata()
    );
  }

  @Test(timeout = 120_000L)
  public void testIncrementalHandOff() throws Exception
  {
    // as soon as any segment has more than one record, incremental publishing should happen
    maxRowsPerSegment = 2;
    maxRecordsPerPoll = 1;
    maxBytesPerPoll = 1_000_000;

    recordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();

    recordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.poll(EasyMock.anyLong()))
            .andReturn(clone(RECORDS, 0, 5)).once()
            .andReturn(clone(RECORDS, 4, 15)).once();

    recordSupplier.close();
    EasyMock.expectLastCall().once();

    replayAll();

    final SeekableStreamStartSequenceNumbers<String, String> startPartitions = new SeekableStreamStartSequenceNumbers<>(
        STREAM,
        ImmutableMap.of(SHARD_ID1, "0", SHARD_ID0, "0"),
        ImmutableSet.of()
    );

    final SeekableStreamEndSequenceNumbers<String, String> checkpoint1 = new SeekableStreamEndSequenceNumbers<>(
        STREAM,
        ImmutableMap.of(SHARD_ID1, "4", SHARD_ID0, "0")
    );

    final SeekableStreamEndSequenceNumbers<String, String> endPartitions = new SeekableStreamEndSequenceNumbers<>(
        STREAM,
        ImmutableMap.of(SHARD_ID1, "9", SHARD_ID0, "1")
    );
    final KinesisIndexTask task = createTask(
        0,
        startPartitions.getPartitionSequenceNumberMap(),
        endPartitions.getPartitionSequenceNumberMap()
    );
    final ListenableFuture<TaskStatus> future = runTask(task);
    waitUntil(task, this::isTaskPaused);

    final Map<String, String> currentOffsets = ImmutableMap.copyOf(task.getRunner().getCurrentOffsets());
    Assert.assertEquals(checkpoint1.getPartitionSequenceNumberMap(), currentOffsets);
    task.getRunner().setEndOffsets(currentOffsets, false);

    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    verifyAll();

    Assert.assertEquals(1, checkpointRequestsHash.size());
    Assert.assertTrue(
        checkpointRequestsHash.contains(
            Objects.hash(
                NEW_DATA_SCHEMA.getDataSource(),
                0,
                new KinesisDataSourceMetadata(startPartitions)
            )
        )
    );

    final long totalRecordBytes = getTotalSize(RECORDS, 0, 15) - getTotalSize(RECORDS, 10, 13);
    verifyTaskMetrics(task, RowMeters.with().bytes(totalRecordBytes).unparseable(4).totalProcessed(8));

    // Check published metadata and segments in deep storage
    assertEqualsExceptVersion(
        ImmutableList.of(
            sdd("2008/P1D", 0, ImmutableList.of("a")),
            sdd("2009/P1D", 0, ImmutableList.of("b")),
            sdd("2010/P1D", 0, ImmutableList.of("c")),
            sdd("2011/P1D", 0, ImmutableList.of("d", "e"), ImmutableList.of("d", "h")),
            sdd("2011/P1D", 1, ImmutableList.of("h"), ImmutableList.of("e")),
            sdd("2012/P1D", 0, ImmutableList.of("g")),
            sdd("2013/P1D", 0, ImmutableList.of("f"))
        ),
        publishedDescriptors()
    );
    Assert.assertEquals(
        new KinesisDataSourceMetadata(
            new SeekableStreamEndSequenceNumbers<>(
                STREAM,
                ImmutableMap.of(SHARD_ID1, "9", SHARD_ID0, "1")
            )
        ),
        newDataSchemaMetadata()
    );
  }

  @Test(timeout = 120_000L)
  public void testIncrementalHandOffMaxTotalRows() throws Exception
  {
    // incremental publish should happen every 3 records
    maxRowsPerSegment = Integer.MAX_VALUE;
    maxTotalRows = 3L;

    recordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();

    recordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.poll(EasyMock.anyLong()))
            .andReturn(clone(RECORDS, 0, 3)).once()
            .andReturn(clone(RECORDS, 2, 10)).once()
            .andReturn(clone(RECORDS, 9, 11));

    recordSupplier.close();
    EasyMock.expectLastCall().once();

    replayAll();

    final Map<String, String> startOffsets = ImmutableMap.of(SHARD_ID1, "0");
    // Checkpointing will happen at either checkpoint1 or checkpoint2 depending on ordering
    // of events fetched across two partitions from Kafka
    final Map<String, String> checkpointOffsets1 = ImmutableMap.of(SHARD_ID1, "2");
    final Map<String, String> checkpointOffsets2 = ImmutableMap.of(SHARD_ID1, "9");
    final Map<String, String> endOffsets = ImmutableMap.of(SHARD_ID1, "10");

    final KinesisIndexTask task = createTask(0, startOffsets, endOffsets);

    final ListenableFuture<TaskStatus> future = runTask(task);
    waitUntil(task, this::isTaskPaused);

    final Map<String, String> currentOffsets = ImmutableMap.copyOf(task.getRunner().getCurrentOffsets());

    Assert.assertEquals(checkpointOffsets1, currentOffsets);
    task.getRunner().setEndOffsets(currentOffsets, false);

    waitUntil(task, this::isTaskPaused);

    final Map<String, String> nextOffsets = ImmutableMap.copyOf(task.getRunner().getCurrentOffsets());

    Assert.assertEquals(checkpointOffsets2, nextOffsets);

    task.getRunner().setEndOffsets(nextOffsets, false);

    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    verifyAll();

    Assert.assertEquals(2, checkpointRequestsHash.size());
    Assert.assertTrue(
        checkpointRequestsHash.contains(
            Objects.hash(
                NEW_DATA_SCHEMA.getDataSource(),
                0,
                new KinesisDataSourceMetadata(
                    new SeekableStreamStartSequenceNumbers<>(STREAM, startOffsets, Collections.emptySet())
                )
            )
        )
    );
    Assert.assertTrue(
        checkpointRequestsHash.contains(
            Objects.hash(
                NEW_DATA_SCHEMA.getDataSource(),
                0,
                new KinesisDataSourceMetadata(
                    new SeekableStreamStartSequenceNumbers<>(STREAM, currentOffsets, currentOffsets.keySet()))
            )
        )
    );

    verifyTaskMetrics(task, RowMeters.with().bytes(getTotalSize(RECORDS, 0, 11))
                                     .errors(1).unparseable(4).totalProcessed(6));

    // Check published metadata and segments in deep storage
    assertEqualsExceptVersion(
        ImmutableList.of(
            sdd("2008/P1D", 0, ImmutableList.of("a")),
            sdd("2009/P1D", 0, ImmutableList.of("b")),
            sdd("2010/P1D", 0, ImmutableList.of("c")),
            sdd("2011/P1D", 0, ImmutableList.of("d", "e")),
            sdd("2049/P1D", 0, ImmutableList.of("f")),
            sdd("2013/P1D", 0, ImmutableList.of("f"))
        ),
        publishedDescriptors()
    );
    Assert.assertEquals(
        new KinesisDataSourceMetadata(new SeekableStreamEndSequenceNumbers<>(STREAM, endOffsets)),
        newDataSchemaMetadata()
    );
  }


  @Test(timeout = 120_000L)
  public void testRunWithMinimumMessageTime() throws Exception
  {
    recordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();

    recordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.poll(EasyMock.anyLong())).andReturn(clone(RECORDS, 0, 13)).once();

    recordSupplier.close();
    EasyMock.expectLastCall().once();

    replayAll();

    final KinesisIndexTask task = createTask(
        new KinesisIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "0"), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "4")),
            true,
            DateTimes.of("2010"),
            null,
            INPUT_FORMAT,
            "awsEndpoint",
            null,
            null,
            null
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    waitUntil(task, this::isTaskReading);

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    verifyAll();

    verifyTaskMetrics(task, RowMeters.with().bytes(getTotalSize(RECORDS, 0, 5))
                                     .thrownAway(2).totalProcessed(3));

    // Check published metadata
    assertEqualsExceptVersion(
        ImmutableList.of(
            sdd("2010/P1D", 0, ImmutableList.of("c")),
            sdd("2011/P1D", 0, ImmutableList.of("d", "e"))
        ),
        publishedDescriptors()
    );
    Assert.assertEquals(
        new KinesisDataSourceMetadata(new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "4"))),
        newDataSchemaMetadata()
    );
  }


  @Test(timeout = 120_000L)
  public void testRunWithMaximumMessageTime() throws Exception
  {
    recordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();

    recordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.poll(EasyMock.anyLong())).andReturn(clone(RECORDS, 0, 13)).once();

    recordSupplier.close();
    EasyMock.expectLastCall().once();

    replayAll();

    final KinesisIndexTask task = createTask(
        new KinesisIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "0"), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "4")),
            true,
            null,
            DateTimes.of("2010"),
            INPUT_FORMAT,
            "awsEndpoint",
            null,
            null,
            null
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    waitUntil(task, this::isTaskReading);

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    verifyAll();

    verifyTaskMetrics(task, RowMeters.with().bytes(getTotalSize(RECORDS, 0, 5))
                                     .thrownAway(2).totalProcessed(3));

    // Check published metadata and segments in deep storage
    assertEqualsExceptVersion(
        ImmutableList.of(
            sdd("2008/P1D", 0, ImmutableList.of("a")),
            sdd("2009/P1D", 0, ImmutableList.of("b")),
            sdd("2010/P1D", 0, ImmutableList.of("c"))
        ),
        publishedDescriptors()
    );
    Assert.assertEquals(
        new KinesisDataSourceMetadata(
            new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "4"))),
        newDataSchemaMetadata()
    );
  }


  @Test(timeout = 120_000L)
  public void testRunWithTransformSpec() throws Exception
  {
    recordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();

    recordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.poll(EasyMock.anyLong())).andReturn(clone(RECORDS, 0, 13)).once();

    recordSupplier.close();
    EasyMock.expectLastCall().once();

    replayAll();

    final KinesisIndexTask task = createTask(
        NEW_DATA_SCHEMA.withTransformSpec(
            new TransformSpec(
                new SelectorDimFilter("dim1", "b", null),
                ImmutableList.of(
                    new ExpressionTransform("dim1t", "concat(dim1,dim1)", ExprMacroTable.nil())
                )
            )
        ),
        ImmutableMap.of(SHARD_ID1, "0"),
        ImmutableMap.of(SHARD_ID1, "4")
    );

    final ListenableFuture<TaskStatus> future = runTask(task);
    waitUntil(task, this::isTaskReading);

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    verifyAll();

    verifyTaskMetrics(task, RowMeters.with().bytes(getTotalSize(RECORDS, 0, 5))
                                     .thrownAway(4).totalProcessed(1));

    // Check published metadata
    assertEqualsExceptVersion(ImmutableList.of(sdd("2009/P1D", 0)), publishedDescriptors());
    Assert.assertEquals(
        new KinesisDataSourceMetadata(
            new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "4"))),
        newDataSchemaMetadata()
    );

    // Check segments in deep storage
    final List<SegmentDescriptor> publishedDescriptors = publishedDescriptors();
    Assert.assertEquals(ImmutableList.of("b"), readSegmentColumn("dim1", publishedDescriptors.get(0)));
    Assert.assertEquals(ImmutableList.of("bb"), readSegmentColumn("dim1t", publishedDescriptors.get(0)));
  }


  @Test(timeout = 120_000L)
  public void testRunOnSingletonRange() throws Exception
  {
    recordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();

    recordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.poll(EasyMock.anyLong())).andReturn(clone(RECORDS, 2, 3)).once();

    recordSupplier.close();
    EasyMock.expectLastCall().once();

    replayAll();

    // When start and end offsets are the same, it means we need to read one message (since in Kinesis, end offsets
    // are inclusive).
    final KinesisIndexTask task = createTask(
        0,
        ImmutableMap.of(SHARD_ID1, "2"),
        ImmutableMap.of(SHARD_ID1, "2")
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    verifyAll();

    verifyTaskMetrics(task, RowMeters.with().bytes(getTotalSize(RECORDS, 2, 3))
                                     .totalProcessed(1));

    // Check published metadata
    assertEqualsExceptVersion(ImmutableList.of(sdd("2010/P1D", 0)), publishedDescriptors());
  }


  @Test(timeout = 60_000L)
  public void testHandoffConditionTimeoutWhenHandoffOccurs() throws Exception
  {
    handoffConditionTimeout = 5_000;

    recordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();

    recordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.poll(EasyMock.anyLong())).andReturn(clone(RECORDS, 2, 13)).once();

    recordSupplier.close();
    EasyMock.expectLastCall().once();

    replayAll();

    final KinesisIndexTask task = createTask(
        0,
        ImmutableMap.of(SHARD_ID1, "2"),
        ImmutableMap.of(SHARD_ID1, "4")
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    verifyAll();

    verifyTaskMetrics(task, RowMeters.with().bytes(getTotalSize(RECORDS, 2, 5))
                                     .totalProcessed(3));

    // Check published metadata and segments in deep storage
    assertEqualsExceptVersion(
        ImmutableList.of(
            sdd("2010/P1D", 0, ImmutableList.of("c")),
            sdd("2011/P1D", 0, ImmutableList.of("d", "e"))
        ),
        publishedDescriptors()
    );
    Assert.assertEquals(
        new KinesisDataSourceMetadata(
            new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "4"))
        ),
        newDataSchemaMetadata()
    );
  }


  @Test(timeout = 60_000L)
  public void testHandoffConditionTimeoutWhenHandoffDoesNotOccur() throws Exception
  {
    doHandoff = false;
    handoffConditionTimeout = 100;

    recordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();

    recordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.poll(EasyMock.anyLong())).andReturn(clone(RECORDS, 2, 13)).once();

    recordSupplier.close();
    EasyMock.expectLastCall().once();

    replayAll();

    final KinesisIndexTask task = createTask(
        0,
        ImmutableMap.of(SHARD_ID1, "2"),
        ImmutableMap.of(SHARD_ID1, "4")
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    verifyAll();

    verifyTaskMetrics(task, RowMeters.with().bytes(getTotalSize(RECORDS, 2, 5))
                                     .totalProcessed(3));

    // Check published metadata and segments in deep storage
    assertEqualsExceptVersion(
        ImmutableList.of(
            sdd("2010/P1D", 0, ImmutableList.of("c")),
            sdd("2011/P1D", 0, ImmutableList.of("d", "e"))
        ),
        publishedDescriptors()
    );
    Assert.assertEquals(
        new KinesisDataSourceMetadata(
            new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "4"))
        ),
        newDataSchemaMetadata()
    );
  }


  @Test(timeout = 120_000L)
  public void testReportParseExceptions() throws Exception
  {
    reportParseExceptions = true;

    // these will be ignored because reportParseExceptions is true
    maxParseExceptions = 1000;
    maxSavedParseExceptions = 2;

    recordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();

    recordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.poll(EasyMock.anyLong())).andReturn(clone(RECORDS, 2, 13)).once();

    recordSupplier.close();
    EasyMock.expectLastCall().once();

    replayAll();

    final KinesisIndexTask task = createTask(
        0,
        ImmutableMap.of(SHARD_ID1, "2"),
        ImmutableMap.of(SHARD_ID1, "5")
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for task to exit
    Assert.assertEquals(TaskState.FAILED, future.get().getStatusCode());

    verifyAll();

    verifyTaskMetrics(task, RowMeters.with().bytes(getTotalSize(RECORDS, 2, 6))
                                     .unparseable(1).totalProcessed(3));

    // Check published metadata
    Assert.assertEquals(ImmutableList.of(), publishedDescriptors());
    Assert.assertNull(newDataSchemaMetadata());
  }


  @Test(timeout = 120_000L)
  public void testMultipleParseExceptionsSuccess() throws Exception
  {
    reportParseExceptions = false;
    maxParseExceptions = 7;
    maxSavedParseExceptions = 7;

    recordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();

    recordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.poll(EasyMock.anyLong())).andReturn(clone(RECORDS, 2, 13)).once();

    recordSupplier.close();
    EasyMock.expectLastCall().once();

    replayAll();

    final KinesisIndexTask task = createTask(
        0,
        ImmutableMap.of(SHARD_ID1, "2"),
        ImmutableMap.of(SHARD_ID1, "12")
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    TaskStatus status = future.get();

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, status.getStatusCode());

    verifyAll();

    Assert.assertNull(status.getErrorMsg());

    verifyTaskMetrics(task, RowMeters.with().bytes(getTotalSize(RECORDS, 2, 13))
                                     .errors(3).unparseable(4).totalProcessed(4));

    // Check published metadata
    assertEqualsExceptVersion(
        ImmutableList.of(sdd("2010/P1D", 0), sdd("2011/P1D", 0), sdd("2013/P1D", 0), sdd("2049/P1D", 0)),
        publishedDescriptors()
    );
    Assert.assertEquals(
        new KinesisDataSourceMetadata(
            new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "12"))
        ),
        newDataSchemaMetadata()
    );

    IngestionStatsAndErrorsTaskReportData reportData = getTaskReportData();

    Map<String, Object> expectedMetrics = ImmutableMap.of(
        RowIngestionMeters.BUILD_SEGMENTS,
        ImmutableMap.of(
            RowIngestionMeters.PROCESSED, 4,
            RowIngestionMeters.PROCESSED_BYTES, 763,
            RowIngestionMeters.PROCESSED_WITH_ERROR, 3,
            RowIngestionMeters.UNPARSEABLE, 4,
            RowIngestionMeters.THROWN_AWAY, 0
        )
    );
    Assert.assertEquals(expectedMetrics, reportData.getRowStats());

    ParseExceptionReport parseExceptionReport =
        ParseExceptionReport.forPhase(reportData, RowIngestionMeters.BUILD_SEGMENTS);

    List<String> expectedMessages = Arrays.asList(
        "Unable to parse value[notanumber] for field[met1]",
        "could not convert value [notanumber] to float",
        "could not convert value [notanumber] to long",
        "Timestamp[null] is unparseable! Event: {} (Record: 1)",
        "Unable to parse [] as the intermediateRow resulted in empty input row (Record: 1)",
        "Unable to parse row [unparseable] (Record: 1)",
        "Encountered row with timestamp[246140482-04-24T15:36:27.903Z] that cannot be represented as a long: [{timestamp=246140482-04-24T15:36:27.903Z, dim1=x, dim2=z, dimLong=10, dimFloat=20.0, met1=1.0}] (Record: 1)"
    );
    Assert.assertEquals(expectedMessages, parseExceptionReport.getErrorMessages());

    List<String> expectedInputs = Arrays.asList(
        "{timestamp=2049, dim1=f, dim2=y, dimLong=10, dimFloat=20.0, met1=notanumber}",
        "{timestamp=2049, dim1=f, dim2=y, dimLong=10, dimFloat=notanumber, met1=1.0}",
        "{timestamp=2049, dim1=f, dim2=y, dimLong=notanumber, dimFloat=20.0, met1=1.0}",
        "{}",
        "",
        "unparseable",
        "{timestamp=246140482-04-24T15:36:27.903Z, dim1=x, dim2=z, dimLong=10, dimFloat=20.0, met1=1.0}"
    );
    Assert.assertEquals(expectedInputs, parseExceptionReport.getInputs());
  }


  @Test(timeout = 120_000L)
  public void testMultipleParseExceptionsFailure() throws Exception
  {
    reportParseExceptions = false;
    maxParseExceptions = 2;
    maxSavedParseExceptions = 2;

    recordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();

    recordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.poll(EasyMock.anyLong()))
            .andReturn(clone(RECORDS, 2, 13)).once();

    recordSupplier.close();
    EasyMock.expectLastCall().once();

    replayAll();

    final KinesisIndexTask task = createTask(
        0,
        ImmutableMap.of(SHARD_ID1, "2"),
        ImmutableMap.of(SHARD_ID1, "9")
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    TaskStatus status = future.get();

    // Wait for task to exit
    Assert.assertEquals(TaskState.FAILED, status.getStatusCode());
    verifyAll();
    IndexTaskTest.checkTaskStatusErrorMsgForParseExceptionsExceeded(status);

    long totalBytes = getTotalSize(RECORDS, 2, 8);
    verifyTaskMetrics(task, RowMeters.with().bytes(totalBytes).unparseable(3).totalProcessed(3));

    // Check published metadata
    Assert.assertEquals(ImmutableList.of(), publishedDescriptors());
    Assert.assertNull(newDataSchemaMetadata());

    IngestionStatsAndErrorsTaskReportData reportData = getTaskReportData();

    Map<String, Object> expectedMetrics = ImmutableMap.of(
        RowIngestionMeters.BUILD_SEGMENTS,
        ImmutableMap.of(
            RowIngestionMeters.PROCESSED, 3,
            RowIngestionMeters.PROCESSED_BYTES, (int) totalBytes,
            RowIngestionMeters.PROCESSED_WITH_ERROR, 0,
            RowIngestionMeters.UNPARSEABLE, 3,
            RowIngestionMeters.THROWN_AWAY, 0
        )
    );
    Assert.assertEquals(expectedMetrics, reportData.getRowStats());

    ParseExceptionReport parseExceptionReport =
        ParseExceptionReport.forPhase(reportData, RowIngestionMeters.BUILD_SEGMENTS);

    List<String> expectedMessages = Arrays.asList(
        "Unable to parse [] as the intermediateRow resulted in empty input row (Record: 1)",
        "Unable to parse row [unparseable] (Record: 1)"
    );
    Assert.assertEquals(expectedMessages, parseExceptionReport.getErrorMessages());
    Assert.assertEquals(Arrays.asList("", "unparseable"), parseExceptionReport.getInputs());
  }


  @Test(timeout = 120_000L)
  public void testRunReplicas() throws Exception
  {
    // Insert data
    recordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();

    recordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.poll(EasyMock.anyLong()))
            .andReturn(clone(RECORDS, 2, 13)).once()
            .andReturn(clone(RECORDS, 2, 13)).once();

    recordSupplier.close();
    EasyMock.expectLastCall().times(2);

    replayAll();

    final KinesisIndexTask task1 = createTask(
        0,
        ImmutableMap.of(SHARD_ID1, "2"),
        ImmutableMap.of(SHARD_ID1, "4")
    );
    final KinesisIndexTask task2 = createTask(
        0,
        ImmutableMap.of(SHARD_ID1, "2"),
        ImmutableMap.of(SHARD_ID1, "4")
    );

    final ListenableFuture<TaskStatus> future1 = runTask(task1);
    final ListenableFuture<TaskStatus> future2 = runTask(task2);

    // Wait for tasks to exit
    Assert.assertEquals(TaskState.SUCCESS, future1.get().getStatusCode());
    Assert.assertEquals(TaskState.SUCCESS, future2.get().getStatusCode());

    verifyAll();

    verifyTaskMetrics(task1, RowMeters.with().bytes(getTotalSize(RECORDS, 2, 5))
                                      .totalProcessed(3));
    verifyTaskMetrics(task2, RowMeters.with().bytes(getTotalSize(RECORDS, 2, 5))
                                      .totalProcessed(3));

    // Check published segments & metadata
    assertEqualsExceptVersion(
        ImmutableList.of(
            sdd("2010/P1D", 0, ImmutableList.of("c")),
            sdd("2011/P1D", 0, ImmutableList.of("d", "e"))
        ),
        publishedDescriptors()
    );
    Assert.assertEquals(
        new KinesisDataSourceMetadata(
            new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "4"))
        ),
        newDataSchemaMetadata()
    );
  }


  @Test(timeout = 120_000L)
  public void testRunConflicting() throws Exception
  {
    recordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();

    recordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.poll(EasyMock.anyLong())).andReturn(clone(RECORDS, 2, 13))
            .once()
            .andReturn(clone(RECORDS, 3, 13))
            .once();

    recordSupplier.close();
    EasyMock.expectLastCall().atLeastOnce();

    replayAll();

    final KinesisIndexTask task1 = createTask(
        0,
        ImmutableMap.of(SHARD_ID1, "2"),
        ImmutableMap.of(SHARD_ID1, "4")
    );
    final KinesisIndexTask task2 = createTask(
        1,
        ImmutableMap.of(SHARD_ID1, "3"),
        ImmutableMap.of(SHARD_ID1, "9")
    );

    // Run first task
    final ListenableFuture<TaskStatus> future1 = runTask(task1);
    Assert.assertEquals(TaskState.SUCCESS, future1.get().getStatusCode());

    // Run second task
    final ListenableFuture<TaskStatus> future2 = runTask(task2);
    Assert.assertEquals(TaskState.FAILED, future2.get().getStatusCode());

    verifyAll();
    verifyTaskMetrics(task1, RowMeters.with().bytes(getTotalSize(RECORDS, 2, 5))
                                      .totalProcessed(3));
    verifyTaskMetrics(task2, RowMeters.with().bytes(getTotalSize(RECORDS, 3, 10))
                                      .unparseable(4).totalProcessed(3));

    // Check published segments & metadata, should all be from the first task
    assertEqualsExceptVersion(
        ImmutableList.of(
            sdd("2010/P1D", 0, ImmutableList.of("c")),
            sdd("2011/P1D", 0, ImmutableList.of("d", "e"))
        ),
        publishedDescriptors()
    );
    Assert.assertEquals(
        new KinesisDataSourceMetadata(new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "4"))),
        newDataSchemaMetadata()
    );
  }


  @Test(timeout = 120_000L)
  public void testRunConflictingWithoutTransactions() throws Exception
  {
    recordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();

    recordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.poll(EasyMock.anyLong())).andReturn(clone(RECORDS, 2, 13))
            .once()
            .andReturn(clone(RECORDS, 3, 13))
            .once();

    recordSupplier.close();
    EasyMock.expectLastCall().times(2);

    replayAll();

    final KinesisIndexTask task1 = createTask(
        0,
        null,
        NEW_DATA_SCHEMA,
        ImmutableMap.of(SHARD_ID1, "2"),
        ImmutableMap.of(SHARD_ID1, "4"),
        false
    );
    final KinesisIndexTask task2 = createTask(
        1,
        null,
        NEW_DATA_SCHEMA,
        ImmutableMap.of(SHARD_ID1, "3"),
        ImmutableMap.of(SHARD_ID1, "9"),
        false
    );

    // Run first task
    final ListenableFuture<TaskStatus> future1 = runTask(task1);
    Assert.assertEquals(TaskState.SUCCESS, future1.get().getStatusCode());

    // Check published segments & metadata
    SegmentDescriptorAndExpectedDim1Values desc1 = sdd("2010/P1D", 0, ImmutableList.of("c"));
    SegmentDescriptorAndExpectedDim1Values desc2 = sdd("2011/P1D", 0, ImmutableList.of("d", "e"));
    assertEqualsExceptVersion(ImmutableList.of(desc1, desc2), publishedDescriptors());
    Assert.assertNull(newDataSchemaMetadata());

    // Run second task
    final ListenableFuture<TaskStatus> future2 = runTask(task2);
    Assert.assertEquals(TaskState.SUCCESS, future2.get().getStatusCode());

    verifyAll();

    verifyTaskMetrics(task1, RowMeters.with().bytes(getTotalSize(RECORDS, 2, 5))
                                      .totalProcessed(3));
    verifyTaskMetrics(task2, RowMeters.with().bytes(getTotalSize(RECORDS, 3, 10))
                                      .unparseable(4).totalProcessed(3));

    // Check published segments & metadata
    SegmentDescriptorAndExpectedDim1Values desc3 = sdd("2011/P1D", 1, ImmutableList.of("d", "e"));
    SegmentDescriptorAndExpectedDim1Values desc4 = sdd("2013/P1D", 0, ImmutableList.of("f"));
    assertEqualsExceptVersion(ImmutableList.of(desc1, desc2, desc3, desc4), publishedDescriptors());
    Assert.assertNull(newDataSchemaMetadata());
  }


  @Test(timeout = 120_000L)
  public void testRunOneTaskTwoPartitions() throws Exception
  {
    // Insert data
    recordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();

    recordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.poll(EasyMock.anyLong()))
            .andReturn(clone(RECORDS, 2, 15)).once();

    recordSupplier.close();
    EasyMock.expectLastCall().once();

    replayAll();

    final KinesisIndexTask task = createTask(
        0,
        ImmutableMap.of(SHARD_ID1, "2", SHARD_ID0, "0"),
        ImmutableMap.of(SHARD_ID1, "4", SHARD_ID0, "1")
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    waitUntil(task, t -> countEvents(task) >= 5);

    // Wait for tasks to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    verifyAll();

    long totalBytes = getTotalSize(RECORDS, 2, 5) + getTotalSize(RECORDS, 13, 15);
    verifyTaskMetrics(task, RowMeters.with().bytes(totalBytes).totalProcessed(5));

    // Check published segments & metadata
    assertEqualsExceptVersion(
        ImmutableList.of(
            sdd("2010/P1D", 0, ImmutableList.of("c")),
            sdd("2011/P1D", 0, ImmutableList.of("d", "e", "h")),
            sdd("2012/P1D", 0, ImmutableList.of("g"))
        ),
        publishedDescriptors()
    );
    Assert.assertEquals(
        new KinesisDataSourceMetadata(
            new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "4", SHARD_ID0, "1"))
        ),
        newDataSchemaMetadata()
    );
  }


  @Test(timeout = 120_000L)
  public void testRunTwoTasksTwoPartitions() throws Exception
  {
    recordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();

    recordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.poll(EasyMock.anyLong()))
            .andReturn(clone(RECORDS, 2, 13)).once()
            .andReturn(clone(RECORDS, 13, 15)).once();

    recordSupplier.close();
    EasyMock.expectLastCall().times(2);

    replayAll();

    final KinesisIndexTask task1 = createTask(
        0,
        ImmutableMap.of(SHARD_ID1, "2"),
        ImmutableMap.of(SHARD_ID1, "4")
    );
    final KinesisIndexTask task2 = createTask(
        1,
        ImmutableMap.of(SHARD_ID0, "0"),
        ImmutableMap.of(SHARD_ID0, "1")
    );

    final ListenableFuture<TaskStatus> future1 = runTask(task1);
    Assert.assertEquals(TaskState.SUCCESS, future1.get().getStatusCode());

    final ListenableFuture<TaskStatus> future2 = runTask(task2);
    Assert.assertEquals(TaskState.SUCCESS, future2.get().getStatusCode());

    verifyAll();

    verifyTaskMetrics(task1, RowMeters.with().bytes(getTotalSize(RECORDS, 2, 5)).totalProcessed(3));
    verifyTaskMetrics(task2, RowMeters.with().bytes(getTotalSize(RECORDS, 13, 15)).totalProcessed(2));

    // Check published segments & metadata
    assertEqualsExceptVersion(
        ImmutableList.of(
            sdd("2010/P1D", 0, ImmutableList.of("c")),
            // These two partitions are interleaved nondeterministically so they both may contain ("d", "e") or "h"
            sdd("2011/P1D", 0, ImmutableList.of("d", "e"), ImmutableList.of("h")),
            sdd("2011/P1D", 1, ImmutableList.of("d", "e"), ImmutableList.of("h")),
            sdd("2012/P1D", 0, ImmutableList.of("g"))
        ),
        publishedDescriptors()
    );
    Assert.assertEquals(
        new KinesisDataSourceMetadata(
            new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "4", SHARD_ID0, "1"))
        ),
        newDataSchemaMetadata()
    );
  }


  @Test(timeout = 120_000L)
  public void testRestore() throws Exception
  {
    final StreamPartition<String> streamPartition = StreamPartition.of(STREAM, SHARD_ID1);
    recordSupplier.assign(ImmutableSet.of(streamPartition));
    EasyMock.expectLastCall();
    recordSupplier.seek(streamPartition, "2");
    EasyMock.expectLastCall();
    EasyMock.expect(recordSupplier.poll(EasyMock.anyLong()))
            .andReturn(clone(RECORDS, 2, 4)).once()
            .andReturn(Collections.emptyList()).anyTimes();

    recordSupplier.close();
    EasyMock.expectLastCall().once();

    replayAll();

    final KinesisIndexTask task1 = createTask(
        0,
        "task1",
        ImmutableMap.of(SHARD_ID1, "2"),
        ImmutableMap.of(SHARD_ID1, "5")
    );

    final ListenableFuture<TaskStatus> future1 = runTask(task1);

    waitUntil(task1, t -> countEvents(t) == 2);
    Assert.assertEquals(2, countEvents(task1));

    // Stop without publishing segment
    task1.stopGracefully(toolboxFactory.build(task1).getConfig());
    unlockAppenderatorBasePersistDirForTask(task1);

    Assert.assertEquals(TaskState.SUCCESS, future1.get().getStatusCode());

    verifyAll();
    EasyMock.reset(recordSupplier);

    recordSupplier.assign(ImmutableSet.of(streamPartition));
    EasyMock.expectLastCall();
    recordSupplier.seek(streamPartition, "3");
    EasyMock.expectLastCall();
    EasyMock.expect(recordSupplier.poll(EasyMock.anyLong()))
            .andReturn(clone(RECORDS, 3, 6)).once();
    recordSupplier.assign(ImmutableSet.of());
    EasyMock.expectLastCall();
    recordSupplier.close();
    EasyMock.expectLastCall();

    replayAll();

    // Start a new task
    final KinesisIndexTask task2 = createTask(
        0,
        task1.getId(),
        ImmutableMap.of(SHARD_ID1, "2"),
        ImmutableMap.of(SHARD_ID1, "5")
    );

    final ListenableFuture<TaskStatus> future2 = runTask(task2);
    waitUntil(task2, t -> countEvents(t) >= 3);
    Assert.assertEquals(3, countEvents(task2));

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future2.get().getStatusCode());

    verifyAll();

    verifyTaskMetrics(task1, RowMeters.with().bytes(getTotalSize(RECORDS, 2, 4))
                                      .totalProcessed(2));
    verifyTaskMetrics(task2, RowMeters.with().bytes(getTotalSize(RECORDS, 4, 6))
                                      .unparseable(1).totalProcessed(1));

    // Check published metadata and segments in deep storage
    assertEqualsExceptVersion(
        ImmutableList.of(
            sdd("2010/P1D", 0, ImmutableList.of("c")),
            sdd("2011/P1D", 0, ImmutableList.of("d", "e"))
        ),
        publishedDescriptors()
    );
    Assert.assertEquals(
        new KinesisDataSourceMetadata(
            new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "5"))),
        newDataSchemaMetadata()
    );
  }

  @Test(timeout = 120_000L)
  public void testRestoreAfterPersistingSequences() throws Exception
  {
    maxRowsPerSegment = 2;
    maxRecordsPerPoll = 1;
    maxBytesPerPoll = 1_000_000;
    List<OrderedPartitionableRecord<String, String, ByteEntity>> records =
        clone(SINGLE_PARTITION_RECORDS);

    recordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();

    recordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    recordSupplier.close();
    EasyMock.expectLastCall().once();

    // simulate 1 record at a time
    EasyMock.expect(recordSupplier.poll(EasyMock.anyLong()))
            .andReturn(Collections.singletonList(records.get(0))).once()
            .andReturn(Collections.singletonList(records.get(1))).once()
            .andReturn(Collections.singletonList(records.get(2))).once()
            .andReturn(Collections.singletonList(records.get(3))).once()
            .andReturn(Collections.singletonList(records.get(4))).once()
            .andReturn(Collections.emptyList()).anyTimes();

    EasyMock.expect(recordSupplier.getPartitionsTimeLag(EasyMock.anyString(), EasyMock.anyObject()))
            .andReturn(null)
            .anyTimes();

    replayAll();


    final KinesisIndexTask task1 = createTask(
        0,
        "task1",
        ImmutableMap.of(SHARD_ID1, "0"),
        ImmutableMap.of(SHARD_ID1, "6")
    );

    final Map<String, String> checkpointOffsets1 = ImmutableMap.of(SHARD_ID1, "4");
    final ListenableFuture<TaskStatus> future1 = runTask(task1);

    waitUntil(task1, this::isTaskPaused);

    final Map<String, String> currentOffsets = ImmutableMap.copyOf(task1.getRunner().getCurrentOffsets());
    Assert.assertEquals(checkpointOffsets1, currentOffsets);
    task1.getRunner().setEndOffsets(currentOffsets, false);

    // Stop without publishing segment
    task1.stopGracefully(toolboxFactory.build(task1).getConfig());
    unlockAppenderatorBasePersistDirForTask(task1);

    Assert.assertEquals(TaskState.SUCCESS, future1.get().getStatusCode());

    verifyAll();
    EasyMock.reset(recordSupplier);

    recordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();

    recordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.poll(EasyMock.anyLong()))
            .andReturn(Collections.singletonList(records.get(5))).once()
            .andReturn(Collections.singletonList(records.get(6))).once()
            .andReturn(Collections.emptyList()).anyTimes();

    recordSupplier.close();
    EasyMock.expectLastCall();

    replayAll();

    // Start a new task
    final KinesisIndexTask task2 = createTask(
        0,
        task1.getId(),
        ImmutableMap.of(SHARD_ID1, "0"),
        ImmutableMap.of(SHARD_ID1, "6")
    );

    final ListenableFuture<TaskStatus> future2 = runTask(task2);

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future2.get().getStatusCode());

    verifyAll();

    verifyTaskMetrics(task1, RowMeters.with().bytes(getTotalSize(SINGLE_PARTITION_RECORDS, 0, 5))
                                      .totalProcessed(5));
    verifyTaskMetrics(task2, RowMeters.with().bytes(getTotalSize(SINGLE_PARTITION_RECORDS, 5, 7))
                                      .totalProcessed(2));

    // Check published segments & metadata
    assertEqualsExceptVersion(
        ImmutableList.of(
            sdd("2008/P1D", 0),
            sdd("2009/P1D", 0),
            sdd("2010/P1D", 0),
            sdd("2011/P1D", 0),
            sdd("2012/P1D", 0),
            sdd("2013/P1D", 0)
        ),
        publishedDescriptors()
    );
    Assert.assertEquals(
        new KinesisDataSourceMetadata(
            new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "6"))
        ),
        newDataSchemaMetadata()
    );
  }

  @Test(timeout = 120_000L)
  public void testRunWithPauseAndResume() throws Exception
  {
    final StreamPartition<String> streamPartition = StreamPartition.of(STREAM, SHARD_ID1);
    recordSupplier.assign(ImmutableSet.of(streamPartition));
    EasyMock.expectLastCall();
    recordSupplier.seek(streamPartition, "2");
    EasyMock.expectLastCall();
    EasyMock.expect(recordSupplier.poll(EasyMock.anyLong()))
            .andReturn(clone(RECORDS, 2, 5)).once()
            .andReturn(Collections.emptyList()).anyTimes();

    replayAll();

    final KinesisIndexTask task = createTask(
        0,
        "task1",
        ImmutableMap.of(SHARD_ID1, "2"),
        ImmutableMap.of(SHARD_ID1, "13")
    );

    final ListenableFuture<TaskStatus> future = runTask(task);
    waitUntil(task, t -> countEvents(t) == 3);
    Assert.assertEquals(3, countEvents(task));
    Assert.assertTrue(isTaskReading(task));

    task.getRunner().pause();

    waitUntil(task, this::isTaskPaused);
    Assert.assertTrue(isTaskPaused(task));

    verifyAll();

    ConcurrentMap<String, String> currentOffsets = task.getRunner().getCurrentOffsets();

    try {
      future.get(10, TimeUnit.SECONDS);
      Assert.fail("Task completed when it should have been paused");
    }
    catch (TimeoutException e) {
      // carry on..
    }

    Assert.assertEquals(currentOffsets, task.getRunner().getCurrentOffsets());

    EasyMock.reset(recordSupplier);

    recordSupplier.assign(ImmutableSet.of());
    EasyMock.expectLastCall();
    recordSupplier.close();
    EasyMock.expectLastCall().once();

    replayAll();

    task.getRunner().setEndOffsets(currentOffsets, true);

    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    verifyAll();
    Assert.assertEquals(task.getRunner().getEndOffsets(), task.getRunner().getCurrentOffsets());

    verifyTaskMetrics(task, RowMeters.with().bytes(getTotalSize(RECORDS, 2, 5))
                                     .totalProcessed(3));

    // Check published metadata and segments in deep storage
    assertEqualsExceptVersion(
        ImmutableList.of(
            sdd("2010/P1D", 0, ImmutableList.of("c")),
            sdd("2011/P1D", 0, ImmutableList.of("d", "e"))
        ),
        publishedDescriptors()
    );
    Assert.assertEquals(
        new KinesisDataSourceMetadata(new SeekableStreamEndSequenceNumbers<>(
            STREAM,
            ImmutableMap.of(SHARD_ID1, currentOffsets.get(SHARD_ID1))
        )),
        newDataSchemaMetadata()
    );
  }

  @Test(timeout = 60_000L)
  public void testRunContextSequenceAheadOfStartingOffsets() throws Exception
  {
    // This tests the case when a replacement task is created in place of a failed test
    // which has done some incremental handoffs, thus the context will contain starting
    // sequence sequences from which the task should start reading and ignore the start sequences
    // Insert data
    recordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();

    recordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.poll(EasyMock.anyLong()))
            .andReturn(clone(RECORDS, 2, 13)).once();

    recordSupplier.close();
    EasyMock.expectLastCall();

    replayAll();

    final TreeMap<Integer, Map<String, String>> sequences = new TreeMap<>();
    // Here the sequence number is 1 meaning that one incremental handoff was done by the failed task
    // and this task should start reading from offset 2 for partition 0 (not offset 1, because end is inclusive)
    sequences.put(1, ImmutableMap.of(SHARD_ID1, "1"));
    final Map<String, Object> context = new HashMap<>();
    context.put(
        SeekableStreamSupervisor.CHECKPOINTS_CTX_KEY,
        OBJECT_MAPPER.writerFor(KinesisSupervisor.CHECKPOINTS_TYPE_REF).writeValueAsString(sequences)
    );

    final KinesisIndexTask task = createTask(
        "task1",
        NEW_DATA_SCHEMA,
        new KinesisIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "0"), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "4")),
            true,
            null,
            null,
            INPUT_FORMAT,
            "awsEndpoint",
            null,
            null,
            null
        ),
        context
    );
    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());
    verifyTaskMetrics(task, RowMeters.with().bytes(getTotalSize(RECORDS, 2, 5))
                                     .totalProcessed(3));

    // Check published metadata and segments in deep storage
    assertEqualsExceptVersion(
        ImmutableList.of(
            sdd("2010/P1D", 0, ImmutableList.of("c")),
            sdd("2011/P1D", 0, ImmutableList.of("d", "e"))
        ),
        publishedDescriptors()
    );
    Assert.assertEquals(
        new KinesisDataSourceMetadata(new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "4"))),
        newDataSchemaMetadata()
    );
  }

  @Test(timeout = 5000L)
  public void testIncrementalHandOffReadsThroughEndOffsets() throws Exception
  {
    // as soon as any segment has more than one record, incremental publishing should happen
    maxRowsPerSegment = 2;

    final KinesisRecordSupplier recordSupplier1 = mock(KinesisRecordSupplier.class);
    recordSupplier1.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(recordSupplier1.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();
    recordSupplier1.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(recordSupplier1.poll(EasyMock.anyLong()))
            .andReturn(clone(SINGLE_PARTITION_RECORDS, 0, 5)).once()
            .andReturn(clone(SINGLE_PARTITION_RECORDS, 4, 10)).once();
    recordSupplier1.close();
    EasyMock.expectLastCall().once();
    final KinesisRecordSupplier recordSupplier2 = mock(KinesisRecordSupplier.class);
    recordSupplier2.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(recordSupplier2.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();
    recordSupplier2.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(recordSupplier2.poll(EasyMock.anyLong()))
            .andReturn(clone(SINGLE_PARTITION_RECORDS, 0, 5)).once()
            .andReturn(clone(SINGLE_PARTITION_RECORDS, 4, 10)).once();
    recordSupplier2.close();
    EasyMock.expectLastCall().once();

    replayAll();

    final Map<String, String> startOffsets = ImmutableMap.of(SHARD_ID1, "0");
    final Map<String, String> checkpointOffsets1 = ImmutableMap.of(SHARD_ID1, "4");
    final Map<String, String> checkpointOffsets2 = ImmutableMap.of(SHARD_ID1, "9");
    final Map<String, String> endOffsets = ImmutableMap.of(SHARD_ID1, "100"); // simulating unlimited

    final KinesisIndexTask normalReplica = createTask(0, startOffsets, endOffsets);
    ((TestableKinesisIndexTask) normalReplica).setLocalSupplier(recordSupplier1);
    final KinesisIndexTask staleReplica = createTask(0, startOffsets, endOffsets);
    ((TestableKinesisIndexTask) staleReplica).setLocalSupplier(recordSupplier2);
    final ListenableFuture<TaskStatus> normalReplicaFuture = runTask(normalReplica);
    // Simulating one replica is slower than the other
    final ListenableFuture<TaskStatus> staleReplicaFuture = Futures.transformAsync(
        taskExec.submit(() -> {
          Thread.sleep(1000);
          return staleReplica;
        }),
        (AsyncFunction<Task, TaskStatus>) this::runTask,
        MoreExecutors.directExecutor()
    );

    waitUntil(normalReplica, this::isTaskPaused);
    staleReplica.getRunner().pause();
    waitUntil(staleReplica, this::isTaskPaused);

    Map<String, String> currentOffsets = ImmutableMap.copyOf(normalReplica.getRunner().getCurrentOffsets());
    Assert.assertEquals(checkpointOffsets1, currentOffsets);

    normalReplica.getRunner().setEndOffsets(currentOffsets, false);
    staleReplica.getRunner().setEndOffsets(currentOffsets, false);

    waitUntil(normalReplica, this::isTaskPaused);
    waitUntil(staleReplica, this::isTaskPaused);

    currentOffsets = ImmutableMap.copyOf(normalReplica.getRunner().getCurrentOffsets());
    Assert.assertEquals(checkpointOffsets2, currentOffsets);
    currentOffsets = ImmutableMap.copyOf(staleReplica.getRunner().getCurrentOffsets());
    Assert.assertEquals(checkpointOffsets2, currentOffsets);

    normalReplica.getRunner().setEndOffsets(currentOffsets, true);
    staleReplica.getRunner().setEndOffsets(currentOffsets, true);

    Assert.assertEquals(TaskState.SUCCESS, normalReplicaFuture.get().getStatusCode());
    Assert.assertEquals(TaskState.SUCCESS, staleReplicaFuture.get().getStatusCode());

    verifyAll();

    Assert.assertEquals(2, checkpointRequestsHash.size());

    long totalRecordBytes = getTotalSize(SINGLE_PARTITION_RECORDS, 0, 10);
    verifyTaskMetrics(normalReplica, RowMeters.with().bytes(totalRecordBytes).totalProcessed(10));
    verifyTaskMetrics(staleReplica, RowMeters.with().bytes(totalRecordBytes).totalProcessed(10));

    // Check published metadata
    assertEqualsExceptVersion(
        Arrays.asList(
            sdd("2008/P1D", 0),
            sdd("2009/P1D", 0),
            sdd("2010/P1D", 0),
            sdd("2010/P1D", 1),
            sdd("2011/P1D", 0),
            sdd("2011/P1D", 1),
            sdd("2012/P1D", 0),
            sdd("2013/P1D", 0)
        ),
        publishedDescriptors()
    );
    Assert.assertEquals(
        new KinesisDataSourceMetadata(
            new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "9"))
        ),
        newDataSchemaMetadata()
    );
  }

  @Test
  public void testSequencesFromContext() throws IOException
  {
    final TreeMap<Integer, Map<String, String>> checkpoints = new TreeMap<>();
    // Here the sequence number is 1 meaning that one incremental handoff was done by the failed task
    // and this task should start reading from offset 2 for partition 0 (not offset 1, because end is inclusive)
    checkpoints.put(0, ImmutableMap.of(SHARD_ID0, "0", SHARD_ID1, "0"));
    checkpoints.put(1, ImmutableMap.of(SHARD_ID0, "0", SHARD_ID1, "1"));
    checkpoints.put(2, ImmutableMap.of(SHARD_ID0, "1", SHARD_ID1, "3"));
    final Map<String, Object> context = new HashMap<>();
    context.put("checkpoints", OBJECT_MAPPER.writerFor(KinesisSupervisor.CHECKPOINTS_TYPE_REF)
                                            .writeValueAsString(checkpoints));

    final KinesisIndexTask task = createTask(
        "task1",
        NEW_DATA_SCHEMA,
        new KinesisIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(
                STREAM,
                ImmutableMap.of(SHARD_ID0, "0", SHARD_ID1, "0"),
                ImmutableSet.of(SHARD_ID0)
            ),
            new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID0, "1", SHARD_ID1, "5")),
            true,
            null,
            null,
            INPUT_FORMAT,
            "awsEndpoint",
            null,
            null,
            null
        ),
        context
    );

    task.getRunner().setToolbox(toolboxFactory.build(task));
    task.getRunner().initializeSequences();
    final CopyOnWriteArrayList<SequenceMetadata<String, String>> sequences = task.getRunner().getSequences();

    Assert.assertEquals(3, sequences.size());

    SequenceMetadata<String, String> sequenceMetadata = sequences.get(0);
    Assert.assertEquals(checkpoints.get(0), sequenceMetadata.getStartOffsets());
    Assert.assertEquals(checkpoints.get(1), sequenceMetadata.getEndOffsets());
    Assert.assertEquals(
        task.getIOConfig().getStartSequenceNumbers().getExclusivePartitions(),
        sequenceMetadata.getExclusiveStartPartitions()
    );
    Assert.assertTrue(sequenceMetadata.isCheckpointed());

    sequenceMetadata = sequences.get(1);
    Assert.assertEquals(checkpoints.get(1), sequenceMetadata.getStartOffsets());
    Assert.assertEquals(checkpoints.get(2), sequenceMetadata.getEndOffsets());
    Assert.assertEquals(checkpoints.get(1).keySet(), sequenceMetadata.getExclusiveStartPartitions());
    Assert.assertTrue(sequenceMetadata.isCheckpointed());

    sequenceMetadata = sequences.get(2);
    Assert.assertEquals(checkpoints.get(2), sequenceMetadata.getStartOffsets());
    Assert.assertEquals(
        task.getIOConfig().getEndSequenceNumbers().getPartitionSequenceNumberMap(),
        sequenceMetadata.getEndOffsets()
    );
    Assert.assertEquals(checkpoints.get(2).keySet(), sequenceMetadata.getExclusiveStartPartitions());
    Assert.assertFalse(sequenceMetadata.isCheckpointed());
  }

  /**
   * Tests handling of a closed shard. The task is initially given an unlimited end sequence number and
   * eventually gets an EOS marker which causes it to stop reading.
   */
  @Test(timeout = 120_000L)
  public void testEndOfShard() throws Exception
  {
    recordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();

    recordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    List<OrderedPartitionableRecord<String, String, ByteEntity>> eosRecord = ImmutableList.of(
        new OrderedPartitionableRecord<>(STREAM, SHARD_ID1, KinesisSequenceNumber.END_OF_SHARD_MARKER, null)
    );

    EasyMock.expect(recordSupplier.poll(EasyMock.anyLong()))
            .andReturn(clone(RECORDS, 2, 5)).once()
            .andReturn(eosRecord).once();

    recordSupplier.close();
    EasyMock.expectLastCall().once();

    replayAll();

    final KinesisIndexTask task = createTask(
        0,
        ImmutableMap.of(SHARD_ID1, "2"),
        ImmutableMap.of(SHARD_ID1, KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER)
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    verifyAll();

    verifyTaskMetrics(task, RowMeters.with().bytes(getTotalSize(RECORDS, 2, 5))
                                     .totalProcessed(3));

    // Check published metadata and segments in deep storage
    assertEqualsExceptVersion(
        ImmutableList.of(
            sdd("2010/P1D", 0, ImmutableList.of("c")),
            sdd("2011/P1D", 0, ImmutableList.of("d", "e"))
        ),
        publishedDescriptors()
    );
    Assert.assertEquals(
        new KinesisDataSourceMetadata(
            new SeekableStreamEndSequenceNumbers<>(
                STREAM,
                ImmutableMap.of(SHARD_ID1, KinesisSequenceNumber.END_OF_SHARD_MARKER)
            )
        ),
        newDataSchemaMetadata()
    );
  }

  @Test(timeout = 60_000L)
  public void testRunWithoutDataInserted() throws Exception
  {
    recordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();

    recordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.poll(EasyMock.anyLong()))
            .andReturn(Collections.emptyList())
            .times(1, Integer.MAX_VALUE);

    recordSupplier.close();
    EasyMock.expectLastCall().once();

    replayAll();

    final KinesisIndexTask task = createTask(
        0,
        ImmutableMap.of(SHARD_ID0, "0"),
        ImmutableMap.of(SHARD_ID0, "1")
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    Thread.sleep(1000);

    Assert.assertEquals(0, countEvents(task));
    Assert.assertEquals(SeekableStreamIndexTaskRunner.Status.READING, task.getRunner().getStatus());

    task.getRunner().stopGracefully();

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    verifyAll();
    verifyTaskMetrics(task, RowMeters.with().totalProcessed(0));

    // Check published metadata and segments in deep storage
    assertEqualsExceptVersion(Collections.emptyList(), publishedDescriptors());
    Assert.assertNull(newDataSchemaMetadata());
  }

  @Test
  public void testComputeFetchThreads()
  {
    final DruidProcessingConfigTest.MockRuntimeInfo runtimeInfo =
        new DruidProcessingConfigTest.MockRuntimeInfo(3, 1000, 10_000_000_000L);

    Assert.assertEquals(6, KinesisIndexTask.computeFetchThreads(runtimeInfo, null));
    Assert.assertEquals(2, KinesisIndexTask.computeFetchThreads(runtimeInfo, 2));

    final DruidProcessingConfigTest.MockRuntimeInfo runtimeInfo2 =
        new DruidProcessingConfigTest.MockRuntimeInfo(3, 1000, 1_000_000_000);
    Assert.assertEquals(5, KinesisIndexTask.computeFetchThreads(runtimeInfo2, null));
    Assert.assertEquals(5, KinesisIndexTask.computeFetchThreads(runtimeInfo2, 6));
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> KinesisIndexTask.computeFetchThreads(runtimeInfo, 0)
    );
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> KinesisIndexTask.computeFetchThreads(runtimeInfo, -1)
    );
  }

  private KinesisIndexTask createTask(
      int groupId,
      Map<String, String> startSequenceNumbers,
      Map<String, String> endSequenceNumbers
  ) throws JsonProcessingException
  {
    return createTask(groupId, null, startSequenceNumbers, endSequenceNumbers);
  }

  private KinesisIndexTask createTask(
      int groupId,
      String taskId,
      DataSchema dataSchema,
      Map<String, String> startSequenceNumbers,
      Map<String, String> endSequenceNumbers,
      boolean useTransaction
  ) throws JsonProcessingException
  {
    return createTask(
        taskId,
        dataSchema,
        new KinesisIndexTaskIOConfig(
            groupId,
            "sequence_" + groupId,
            new SeekableStreamStartSequenceNumbers<>(STREAM, startSequenceNumbers, ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(STREAM, endSequenceNumbers),
            useTransaction,
            null,
            null,
            INPUT_FORMAT,
            "awsEndpoint",
            null,
            null,
            null
        ),
        null
    );
  }

  private KinesisIndexTask createTask(
      int groupId,
      String taskId,
      Map<String, String> startSequenceNumbers,
      Map<String, String> endSequenceNumbers
  ) throws JsonProcessingException
  {
    return createTask(groupId, taskId, NEW_DATA_SCHEMA, startSequenceNumbers, endSequenceNumbers, true);
  }

  private KinesisIndexTask createTask(
      DataSchema dataSchema,
      Map<String, String> startSequenceNumbers,
      Map<String, String> endSequenceNumbers
  ) throws JsonProcessingException
  {
    return createTask(0, null, dataSchema, startSequenceNumbers, endSequenceNumbers, true);
  }

  private KinesisIndexTask createTask(KinesisIndexTaskIOConfig ioConfig)
      throws JsonProcessingException
  {
    return createTask(null, NEW_DATA_SCHEMA, ioConfig, null);
  }

  private KinesisIndexTask createTask(
      final String taskId,
      final DataSchema dataSchema,
      final KinesisIndexTaskIOConfig ioConfig,
      @Nullable final Map<String, Object> context
  ) throws JsonProcessingException
  {
    final KinesisIndexTaskTuningConfig tuningConfig = new KinesisIndexTaskTuningConfig(
        null,
        1000,
        null,
        null,
        maxRowsPerSegment,
        maxTotalRows,
        new Period("P1Y"),
        null,
        null,
        null,
        null,
        reportParseExceptions,
        handoffConditionTimeout,
        false,
        true,
        null,
        null,
        null,
        null,
        null,
        null,
        logParseExceptions,
        maxParseExceptions,
        maxSavedParseExceptions,
        maxRecordsPerPoll,
        maxBytesPerPoll,
        intermediateHandoffPeriod
    );
    return createTask(taskId, dataSchema, ioConfig, tuningConfig, context);
  }

  private KinesisIndexTask createTask(
      final String taskId,
      final DataSchema dataSchema,
      final KinesisIndexTaskIOConfig ioConfig,
      final KinesisIndexTaskTuningConfig tuningConfig,
      @Nullable final Map<String, Object> context
  ) throws JsonProcessingException
  {
    if (context != null) {
      if (!context.containsKey(SeekableStreamSupervisor.CHECKPOINTS_CTX_KEY)) {
        final TreeMap<Integer, Map<String, String>> checkpoints = new TreeMap<>();
        checkpoints.put(0, ioConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap());
        final String checkpointsJson = OBJECT_MAPPER
            .writerFor(KinesisSupervisor.CHECKPOINTS_TYPE_REF)
            .writeValueAsString(checkpoints);
        context.put(SeekableStreamSupervisor.CHECKPOINTS_CTX_KEY, checkpointsJson);
      }
    }

    return new TestableKinesisIndexTask(
        taskId,
        null,
        cloneDataSchema(dataSchema),
        tuningConfig,
        ioConfig,
        context,
        null
    );
  }

  private static DataSchema cloneDataSchema(final DataSchema dataSchema)
  {
    return new DataSchema(
        dataSchema.getDataSource(),
        dataSchema.getTimestampSpec(),
        dataSchema.getDimensionsSpec(),
        dataSchema.getAggregators(),
        dataSchema.getGranularitySpec(),
        dataSchema.getTransformSpec(),
        dataSchema.getParserMap(),
        OBJECT_MAPPER
    );
  }

  @Override
  protected QueryRunnerFactoryConglomerate makeQueryRunnerConglomerate()
  {
    return new DefaultQueryRunnerFactoryConglomerate(
        ImmutableMap.of(
            TimeseriesQuery.class,
            new TimeseriesQueryRunnerFactory(
                new TimeseriesQueryQueryToolChest(),
                new TimeseriesQueryEngine(),
                (query, future) -> {
                  // do nothing
                }
            )
        )
    );
  }

  private void makeToolboxFactory() throws IOException
  {
    directory = tempFolder.newFolder();
    final TestUtils testUtils = new TestUtils();
    final ObjectMapper objectMapper = testUtils.getTestObjectMapper();
    objectMapper.setInjectableValues(((InjectableValues.Std) objectMapper.getInjectableValues()).addValue(
        AWSCredentialsConfig.class,
        new AWSCredentialsConfig()
    ));
    for (Module module : new KinesisIndexingServiceModule().getJacksonModules()) {
      objectMapper.registerModule(module);
    }
    makeToolboxFactory(testUtils, emitter, doHandoff);
  }

  private boolean isTaskPaused(KinesisIndexTask task)
  {
    return task.getRunner().getStatus() == SeekableStreamIndexTaskRunner.Status.PAUSED;
  }

  private boolean isTaskReading(KinesisIndexTask task)
  {
    return task.getRunner().getStatus() == SeekableStreamIndexTaskRunner.Status.READING;
  }

  @JsonTypeName("index_kinesis")
  private static class TestableKinesisIndexTask extends KinesisIndexTask
  {
    private KinesisRecordSupplier localSupplier;

    @JsonCreator
    private TestableKinesisIndexTask(
        @JsonProperty("id") String id,
        @JsonProperty("resource") TaskResource taskResource,
        @JsonProperty("dataSchema") DataSchema dataSchema,
        @JsonProperty("tuningConfig") KinesisIndexTaskTuningConfig tuningConfig,
        @JsonProperty("ioConfig") KinesisIndexTaskIOConfig ioConfig,
        @JsonProperty("context") Map<String, Object> context,
        @JacksonInject @Named(KinesisIndexingServiceModule.AWS_SCOPE) AWSCredentialsConfig awsCredentialsConfig
    )
    {
      super(
          id,
          taskResource,
          dataSchema,
          tuningConfig,
          ioConfig,
          context,
          false,
          awsCredentialsConfig
      );
    }

    private void setLocalSupplier(KinesisRecordSupplier recordSupplier)
    {
      this.localSupplier = recordSupplier;
    }

    @Override
    protected KinesisRecordSupplier newTaskRecordSupplier(final TaskToolbox toolbox)
    {
      return localSupplier == null ? recordSupplier : localSupplier;
    }
  }

  /**
   * Utility class to keep the test code more readable.
   */
  private static class KinesisRecord extends OrderedPartitionableRecord<String, String, ByteEntity>
  {
    private final List<ByteEntity> data;

    public KinesisRecord(
        String stream,
        String partitionId,
        String sequenceNumber,
        List<ByteEntity> data
    )
    {
      super(stream, partitionId, sequenceNumber, data);
      this.data = data;
    }

    @Nonnull
    @Override
    public List<ByteEntity> getData()
    {
      return data;
    }
  }

}
