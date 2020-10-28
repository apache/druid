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

package org.apache.druid.indexing.common.task;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.google.common.io.Files;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.client.indexing.NoopIndexingServiceClient;
import org.apache.druid.data.input.impl.CSVParseSpec;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.ParseSpec;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.RetryPolicyConfig;
import org.apache.druid.indexing.common.RetryPolicyFactory;
import org.apache.druid.indexing.common.SegmentLoaderFactory;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.common.task.CompactionTask.Builder;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexTuningConfig;
import org.apache.druid.indexing.firehose.IngestSegmentFirehoseFactory;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.segment.join.NoopJoinableFactory;
import org.apache.druid.segment.loading.LocalDataSegmentPuller;
import org.apache.druid.segment.loading.LocalDataSegmentPusher;
import org.apache.druid.segment.loading.LocalDataSegmentPusherConfig;
import org.apache.druid.segment.loading.LocalLoadSpec;
import org.apache.druid.segment.loading.NoopDataSegmentKiller;
import org.apache.druid.segment.loading.SegmentLoader;
import org.apache.druid.segment.loading.SegmentLoaderConfig;
import org.apache.druid.segment.loading.SegmentLoaderLocalCacheManager;
import org.apache.druid.segment.loading.StorageLocationConfig;
import org.apache.druid.segment.realtime.firehose.NoopChatHandlerProvider;
import org.apache.druid.segment.realtime.firehose.WindowedStorageAdapter;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.timeline.CompactionState;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.HashBasedNumberedShardSpec;
import org.apache.druid.timeline.partition.NumberedOverwriteShardSpec;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.timeline.partition.PartitionIds;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

@RunWith(Parameterized.class)
public class CompactionTaskRunTest extends IngestionTestBase
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  public static final ParseSpec DEFAULT_PARSE_SPEC = new CSVParseSpec(
      new TimestampSpec("ts", "auto", null),
      new DimensionsSpec(DimensionsSpec.getDefaultSchemas(Arrays.asList("ts", "dim"))),
      "|",
      Arrays.asList("ts", "dim", "val"),
      false,
      0
  );

  // Expecte compaction state to exist after compaction as we store compaction state by default
  private static CompactionState DEFAULT_COMPACTION_STATE;

  private static final List<String> TEST_ROWS = ImmutableList.of(
      "2014-01-01T00:00:10Z,a,1\n",
      "2014-01-01T00:00:10Z,b,2\n",
      "2014-01-01T00:00:10Z,c,3\n",
      "2014-01-01T01:00:20Z,a,1\n",
      "2014-01-01T01:00:20Z,b,2\n",
      "2014-01-01T01:00:20Z,c,3\n",
      "2014-01-01T02:00:30Z,a,1\n",
      "2014-01-01T02:00:30Z,b,2\n",
      "2014-01-01T02:00:30Z,c,3\n",
      "2014-01-01T02:00:30Z,c|d|e,3\n"
  );

  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> constructorFeeder()
  {
    return ImmutableList.of(
        new Object[]{LockGranularity.TIME_CHUNK},
        new Object[]{LockGranularity.SEGMENT}
    );
  }

  private static final String DATA_SOURCE = "test";
  private static final RetryPolicyFactory RETRY_POLICY_FACTORY = new RetryPolicyFactory(new RetryPolicyConfig());
  private final IndexingServiceClient indexingServiceClient;
  private final CoordinatorClient coordinatorClient;
  private final SegmentLoaderFactory segmentLoaderFactory;
  private final LockGranularity lockGranularity;
  private final TestUtils testUtils;

  private ExecutorService exec;
  private File localDeepStorage;

  public CompactionTaskRunTest(LockGranularity lockGranularity)
  {
    testUtils = new TestUtils();
    indexingServiceClient = new NoopIndexingServiceClient();
    coordinatorClient = new CoordinatorClient(null, null)
    {
      @Override
      public Collection<DataSegment> fetchUsedSegmentsInDataSourceForIntervals(
          String dataSource,
          List<Interval> intervals
      )
      {
        return getStorageCoordinator().retrieveUsedSegmentsForIntervals(dataSource, intervals, Segments.ONLY_VISIBLE);
      }
    };
    segmentLoaderFactory = new SegmentLoaderFactory(getIndexIO(), getObjectMapper());
    this.lockGranularity = lockGranularity;
  }

  @BeforeClass
  public static void setupClass() throws JsonProcessingException
  {
    ObjectMapper mapper = new DefaultObjectMapper();

    DEFAULT_COMPACTION_STATE = new CompactionState(
      new DynamicPartitionsSpec(5000000, Long.MAX_VALUE),
      mapper.readValue(mapper.writeValueAsString(new IndexSpec()), Map.class)
    );
  }

  @Before
  public void setup() throws IOException
  {
    exec = Execs.multiThreaded(2, "compaction-task-run-test-%d");
    localDeepStorage = temporaryFolder.newFolder();
  }

  @After
  public void teardown()
  {
    exec.shutdownNow();
    temporaryFolder.delete();
  }

  @Test
  public void testRunWithDynamicPartitioning() throws Exception
  {
    runIndexTask();

    final Builder builder = new Builder(
        DATA_SOURCE,
        segmentLoaderFactory,
        RETRY_POLICY_FACTORY
    );

    final CompactionTask compactionTask = builder
        .interval(Intervals.of("2014-01-01/2014-01-02"))
        .build();

    final Pair<TaskStatus, List<DataSegment>> resultPair = runTask(compactionTask);

    Assert.assertTrue(resultPair.lhs.isSuccess());

    final List<DataSegment> segments = resultPair.rhs;
    Assert.assertEquals(3, segments.size());

    for (int i = 0; i < 3; i++) {
      Assert.assertEquals(
          Intervals.of("2014-01-01T0%d:00:00/2014-01-01T0%d:00:00", i, i + 1),
          segments.get(i).getInterval()
      );
      Assert.assertEquals(DEFAULT_COMPACTION_STATE, segments.get(i).getLastCompactionState());
      if (lockGranularity == LockGranularity.SEGMENT) {
        Assert.assertEquals(
            new NumberedOverwriteShardSpec(32768, 0, 2, (short) 1, (short) 1),
            segments.get(i).getShardSpec()
        );
      } else {
        Assert.assertEquals(new NumberedShardSpec(0, 1), segments.get(i).getShardSpec());
      }
    }

    List<String> rowsFromSegment = getCSVFormatRowsFromSegments(segments);
    Assert.assertEquals(TEST_ROWS, rowsFromSegment);
  }

  @Test
  public void testRunWithHashPartitioning() throws Exception
  {
    // Hash partitioning is not supported with segment lock yet
    if (lockGranularity == LockGranularity.SEGMENT) {
      return;
    }
    runIndexTask();

    final Builder builder = new Builder(
        DATA_SOURCE,
        segmentLoaderFactory,
        RETRY_POLICY_FACTORY
    );

    final CompactionTask compactionTask = builder
        .interval(Intervals.of("2014-01-01/2014-01-02"))
        .tuningConfig(
            new ParallelIndexTuningConfig(
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                new HashedPartitionsSpec(null, 3, null),
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
                null,
                null,
                null,
                null,
                null
            )
        )
        .build();

    final Pair<TaskStatus, List<DataSegment>> resultPair = runTask(compactionTask);

    Assert.assertTrue(resultPair.lhs.isSuccess());

    final List<DataSegment> segments = resultPair.rhs;
    Assert.assertEquals(6, segments.size());
    final CompactionState expectedState = new CompactionState(
        new HashedPartitionsSpec(null, 3, null),
        compactionTask.getTuningConfig().getIndexSpec().asMap(getObjectMapper())
    );

    for (int i = 0; i < 3; i++) {
      final Interval interval = Intervals.of("2014-01-01T0%d:00:00/2014-01-01T0%d:00:00", i, i + 1);
      for (int j = 0; j < 2; j++) {
        final int segmentIdx = i * 2 + j;
        Assert.assertEquals(
            interval,
            segments.get(segmentIdx).getInterval()
        );
        Assert.assertEquals(expectedState, segments.get(segmentIdx).getLastCompactionState());
        Assert.assertSame(HashBasedNumberedShardSpec.class, segments.get(segmentIdx).getShardSpec().getClass());
      }
    }

    List<String> rowsFromSegment = getCSVFormatRowsFromSegments(segments);
    rowsFromSegment.sort(Ordering.natural());
    Assert.assertEquals(TEST_ROWS, rowsFromSegment);
  }

  @Test
  public void testRunCompactionTwice() throws Exception
  {
    runIndexTask();

    final Builder builder = new Builder(
        DATA_SOURCE,
        segmentLoaderFactory,
        RETRY_POLICY_FACTORY
    );

    final CompactionTask compactionTask1 = builder
        .interval(Intervals.of("2014-01-01/2014-01-02"))
        .build();

    Pair<TaskStatus, List<DataSegment>> resultPair = runTask(compactionTask1);

    Assert.assertTrue(resultPair.lhs.isSuccess());

    List<DataSegment> segments = resultPair.rhs;
    Assert.assertEquals(3, segments.size());

    for (int i = 0; i < 3; i++) {
      Assert.assertEquals(
          Intervals.of("2014-01-01T0%d:00:00/2014-01-01T0%d:00:00", i, i + 1),
          segments.get(i).getInterval()
      );
      Assert.assertEquals(DEFAULT_COMPACTION_STATE, segments.get(i).getLastCompactionState());
      if (lockGranularity == LockGranularity.SEGMENT) {
        Assert.assertEquals(
            new NumberedOverwriteShardSpec(PartitionIds.NON_ROOT_GEN_START_PARTITION_ID, 0, 2, (short) 1, (short) 1),
            segments.get(i).getShardSpec()
        );
      } else {
        Assert.assertEquals(new NumberedShardSpec(0, 1), segments.get(i).getShardSpec());
      }
    }

    final CompactionTask compactionTask2 = builder
        .interval(Intervals.of("2014-01-01/2014-01-02"))
        .build();

    resultPair = runTask(compactionTask2);

    Assert.assertTrue(resultPair.lhs.isSuccess());

    segments = resultPair.rhs;
    Assert.assertEquals(3, segments.size());

    for (int i = 0; i < 3; i++) {
      Assert.assertEquals(
          Intervals.of("2014-01-01T0%d:00:00/2014-01-01T0%d:00:00", i, i + 1),
          segments.get(i).getInterval()
      );
      Assert.assertEquals(DEFAULT_COMPACTION_STATE, segments.get(i).getLastCompactionState());
      if (lockGranularity == LockGranularity.SEGMENT) {
        Assert.assertEquals(
            new NumberedOverwriteShardSpec(
                PartitionIds.NON_ROOT_GEN_START_PARTITION_ID + 1,
                0,
                2,
                (short) 2,
                (short) 1
            ),
            segments.get(i).getShardSpec()
        );
      } else {
        Assert.assertEquals(new NumberedShardSpec(0, 1), segments.get(i).getShardSpec());
      }
    }
  }

  @Test
  public void testRunIndexAndCompactAtTheSameTimeForDifferentInterval() throws Exception
  {
    runIndexTask();

    final Builder builder = new Builder(
        DATA_SOURCE,
        segmentLoaderFactory,
        RETRY_POLICY_FACTORY
    );

    final CompactionTask compactionTask = builder
        .interval(Intervals.of("2014-01-01T00:00:00/2014-01-02T03:00:00"))
        .build();

    File tmpDir = temporaryFolder.newFolder();
    File tmpFile = File.createTempFile("druid", "index", tmpDir);

    try (BufferedWriter writer = Files.newWriter(tmpFile, StandardCharsets.UTF_8)) {
      writer.write("2014-01-01T03:00:10Z,a,1\n");
      writer.write("2014-01-01T03:00:10Z,b,2\n");
      writer.write("2014-01-01T03:00:10Z,c,3\n");
      writer.write("2014-01-01T04:00:20Z,a,1\n");
      writer.write("2014-01-01T04:00:20Z,b,2\n");
      writer.write("2014-01-01T04:00:20Z,c,3\n");
      writer.write("2014-01-01T05:00:30Z,a,1\n");
      writer.write("2014-01-01T05:00:30Z,b,2\n");
      writer.write("2014-01-01T05:00:30Z,c,3\n");
    }

    IndexTask indexTask = new IndexTask(
        null,
        null,
        IndexTaskTest.createIngestionSpec(
            getObjectMapper(),
            tmpDir,
            DEFAULT_PARSE_SPEC,
            null,
            new UniformGranularitySpec(
                Granularities.HOUR,
                Granularities.MINUTE,
                null
            ),
            IndexTaskTest.createTuningConfig(2, 2, null, 2L, null, false, true),
            false
        ),
        null
    );

    final Future<Pair<TaskStatus, List<DataSegment>>> compactionFuture = exec.submit(
        () -> runTask(compactionTask)
    );

    final Future<Pair<TaskStatus, List<DataSegment>>> indexFuture = exec.submit(
        () -> runTask(indexTask)
    );

    Assert.assertTrue(indexFuture.get().lhs.isSuccess());

    List<DataSegment> segments = indexFuture.get().rhs;
    Assert.assertEquals(6, segments.size());

    for (int i = 0; i < 6; i++) {
      Assert.assertEquals(Intervals.of("2014-01-01T0%d:00:00/2014-01-01T0%d:00:00", 3 + i / 2, 3 + i / 2 + 1), segments.get(i).getInterval());
      if (lockGranularity == LockGranularity.SEGMENT) {
        Assert.assertEquals(new NumberedShardSpec(i % 2, 0), segments.get(i).getShardSpec());
      } else {
        Assert.assertEquals(new NumberedShardSpec(i % 2, 2), segments.get(i).getShardSpec());
      }
    }

    Assert.assertTrue(compactionFuture.get().lhs.isSuccess());

    segments = compactionFuture.get().rhs;
    Assert.assertEquals(3, segments.size());

    for (int i = 0; i < 3; i++) {
      Assert.assertEquals(
          Intervals.of("2014-01-01T0%d:00:00/2014-01-01T0%d:00:00", i, i + 1),
          segments.get(i).getInterval()
      );
      Assert.assertEquals(DEFAULT_COMPACTION_STATE, segments.get(i).getLastCompactionState());
      if (lockGranularity == LockGranularity.SEGMENT) {
        Assert.assertEquals(
            new NumberedOverwriteShardSpec(PartitionIds.NON_ROOT_GEN_START_PARTITION_ID, 0, 2, (short) 1, (short) 1),
            segments.get(i).getShardSpec()
        );
      } else {
        Assert.assertEquals(new NumberedShardSpec(0, 1), segments.get(i).getShardSpec());
      }
    }
  }

  @Test
  public void testWithSegmentGranularity() throws Exception
  {
    runIndexTask();

    final Builder builder = new Builder(
        DATA_SOURCE,
        segmentLoaderFactory,
        RETRY_POLICY_FACTORY
    );

    // day segmentGranularity
    final CompactionTask compactionTask1 = builder
        .interval(Intervals.of("2014-01-01/2014-01-02"))
        .segmentGranularity(Granularities.DAY)
        .build();

    Pair<TaskStatus, List<DataSegment>> resultPair = runTask(compactionTask1);

    Assert.assertTrue(resultPair.lhs.isSuccess());

    List<DataSegment> segments = resultPair.rhs;

    Assert.assertEquals(1, segments.size());

    Assert.assertEquals(Intervals.of("2014-01-01/2014-01-02"), segments.get(0).getInterval());
    Assert.assertEquals(new NumberedShardSpec(0, 1), segments.get(0).getShardSpec());
    Assert.assertEquals(DEFAULT_COMPACTION_STATE, segments.get(0).getLastCompactionState());

    // hour segmentGranularity
    final CompactionTask compactionTask2 = builder
        .interval(Intervals.of("2014-01-01/2014-01-02"))
        .segmentGranularity(Granularities.HOUR)
        .build();

    resultPair = runTask(compactionTask2);

    Assert.assertTrue(resultPair.lhs.isSuccess());

    segments = resultPair.rhs;
    Assert.assertEquals(3, segments.size());

    for (int i = 0; i < 3; i++) {
      Assert.assertEquals(Intervals.of("2014-01-01T0%d:00:00/2014-01-01T0%d:00:00", i, i + 1), segments.get(i).getInterval());
      Assert.assertEquals(new NumberedShardSpec(0, 1), segments.get(i).getShardSpec());
      Assert.assertEquals(DEFAULT_COMPACTION_STATE, segments.get(i).getLastCompactionState());
    }
  }

  @Test
  public void testCompactThenAppend() throws Exception
  {
    runIndexTask();

    final Builder builder = new Builder(
        DATA_SOURCE,
        segmentLoaderFactory,
        RETRY_POLICY_FACTORY
    );

    final CompactionTask compactionTask = builder
        .interval(Intervals.of("2014-01-01/2014-01-02"))
        .build();

    final Pair<TaskStatus, List<DataSegment>> compactionResult = runTask(compactionTask);
    Assert.assertTrue(compactionResult.lhs.isSuccess());
    final Set<DataSegment> expectedSegments = new HashSet<>(compactionResult.rhs);

    final Pair<TaskStatus, List<DataSegment>> appendResult = runAppendTask();
    Assert.assertTrue(appendResult.lhs.isSuccess());
    expectedSegments.addAll(appendResult.rhs);

    final Set<DataSegment> usedSegments = new HashSet<>(
        getStorageCoordinator().retrieveUsedSegmentsForIntervals(
            DATA_SOURCE,
            Collections.singletonList(Intervals.of("2014-01-01/2014-01-02")),
            Segments.ONLY_VISIBLE
        )
    );

    Assert.assertEquals(expectedSegments, usedSegments);
  }

  @Test
  public void testRunIndexAndCompactForSameSegmentAtTheSameTime() throws Exception
  {
    runIndexTask();

    // make sure that indexTask becomes ready first, then compactionTask becomes ready, then indexTask runs
    final CountDownLatch compactionTaskReadyLatch = new CountDownLatch(1);
    final CountDownLatch indexTaskStartLatch = new CountDownLatch(1);
    final Future<Pair<TaskStatus, List<DataSegment>>> indexFuture = exec.submit(
        () -> runIndexTask(compactionTaskReadyLatch, indexTaskStartLatch, false)
    );

    final Builder builder = new Builder(
        DATA_SOURCE,
        segmentLoaderFactory,
        RETRY_POLICY_FACTORY
    );

    final CompactionTask compactionTask = builder
        .interval(Intervals.of("2014-01-01T00:00:00/2014-01-02T03:00:00"))
        .build();

    final Future<Pair<TaskStatus, List<DataSegment>>> compactionFuture = exec.submit(
        () -> {
          compactionTaskReadyLatch.await();
          return runTask(compactionTask, indexTaskStartLatch, null);
        }
    );

    Assert.assertTrue(indexFuture.get().lhs.isSuccess());

    List<DataSegment> segments = indexFuture.get().rhs;
    Assert.assertEquals(6, segments.size());

    for (int i = 0; i < 6; i++) {
      Assert.assertEquals(
          Intervals.of("2014-01-01T0%d:00:00/2014-01-01T0%d:00:00", i / 2, i / 2 + 1),
          segments.get(i).getInterval()
      );
      if (lockGranularity == LockGranularity.SEGMENT) {
        Assert.assertEquals(
            new NumberedOverwriteShardSpec(
                PartitionIds.NON_ROOT_GEN_START_PARTITION_ID + i % 2,
                0,
                2,
                (short) 1,
                (short) 2
            ),
            segments.get(i).getShardSpec()
        );
      } else {
        Assert.assertEquals(new NumberedShardSpec(i % 2, 2), segments.get(i).getShardSpec());
      }
    }

    final Pair<TaskStatus, List<DataSegment>> compactionResult = compactionFuture.get();
    Assert.assertEquals(TaskState.FAILED, compactionResult.lhs.getStatusCode());
  }

  @Test
  public void testRunIndexAndCompactForSameSegmentAtTheSameTime2() throws Exception
  {
    runIndexTask();

    final Builder builder = new Builder(
        DATA_SOURCE,
        segmentLoaderFactory,
        RETRY_POLICY_FACTORY
    );

    final CompactionTask compactionTask = builder
        .interval(Intervals.of("2014-01-01T00:00:00/2014-01-02T03:00:00"))
        .build();

    // make sure that compactionTask becomes ready first, then the indexTask becomes ready, then compactionTask runs
    final CountDownLatch indexTaskReadyLatch = new CountDownLatch(1);
    final CountDownLatch compactionTaskStartLatch = new CountDownLatch(1);
    final Future<Pair<TaskStatus, List<DataSegment>>> compactionFuture = exec.submit(
        () -> {
          final Pair<TaskStatus, List<DataSegment>> pair = runTask(
              compactionTask,
              indexTaskReadyLatch,
              compactionTaskStartLatch
          );
          return pair;
        }
    );

    final Future<Pair<TaskStatus, List<DataSegment>>> indexFuture = exec.submit(
        () -> {
          indexTaskReadyLatch.await();
          return runIndexTask(compactionTaskStartLatch, null, false);
        }
    );

    Assert.assertTrue(indexFuture.get().lhs.isSuccess());

    List<DataSegment> segments = indexFuture.get().rhs;
    Assert.assertEquals(6, segments.size());

    for (int i = 0; i < 6; i++) {
      Assert.assertEquals(
          Intervals.of("2014-01-01T0%d:00:00/2014-01-01T0%d:00:00", i / 2, i / 2 + 1),
          segments.get(i).getInterval()
      );
      if (lockGranularity == LockGranularity.SEGMENT) {
        Assert.assertEquals(
            new NumberedOverwriteShardSpec(
                PartitionIds.NON_ROOT_GEN_START_PARTITION_ID + i % 2,
                0,
                2,
                (short) 1,
                (short) 2
            ),
            segments.get(i).getShardSpec()
        );
      } else {
        Assert.assertEquals(new NumberedShardSpec(i % 2, 2), segments.get(i).getShardSpec());
      }
    }

    final Pair<TaskStatus, List<DataSegment>> compactionResult = compactionFuture.get();
    Assert.assertEquals(TaskState.FAILED, compactionResult.lhs.getStatusCode());
  }

  /**
   * Run a regular index task that's equivalent to the compaction task in {@link #testRunWithDynamicPartitioning()},
   * using {@link IngestSegmentFirehoseFactory}.
   *
   * This is not entirely CompactionTask related, but it's similar conceptually and it requires
   * similar setup to what this test suite already has.
   *
   * It could be moved to a separate test class if needed.
   */
  @Test
  public void testRunRegularIndexTaskWithIngestSegmentFirehose() throws Exception
  {
    runIndexTask();

    IndexTask indexTask = new IndexTask(
        null,
        null,
        new IndexTask.IndexIngestionSpec(
            new DataSchema(
                "test",
                getObjectMapper().convertValue(
                    new StringInputRowParser(
                        DEFAULT_PARSE_SPEC,
                        null
                    ),
                    Map.class
                ),
                new AggregatorFactory[]{
                    new LongSumAggregatorFactory("val", "val")
                },
                new UniformGranularitySpec(
                    Granularities.HOUR,
                    Granularities.MINUTE,
                    null
                ),
                null,
                getObjectMapper()
            ),
            new IndexTask.IndexIOConfig(
                new IngestSegmentFirehoseFactory(
                    DATA_SOURCE,
                    Intervals.of("2014-01-01/2014-01-02"),
                    null,
                    null,
                    null,
                    null,
                    null,
                    getIndexIO(),
                    coordinatorClient,
                    segmentLoaderFactory,
                    RETRY_POLICY_FACTORY
                ),
                false
            ),
            IndexTaskTest.createTuningConfig(5000000, null, null, Long.MAX_VALUE, null, false, true)
        ),
        null
    );

    // This is a regular index so we need to explicitly add this context to store the CompactionState
    indexTask.addToContext(Tasks.STORE_COMPACTION_STATE_KEY, true);

    final Pair<TaskStatus, List<DataSegment>> resultPair = runTask(indexTask);

    Assert.assertTrue(resultPair.lhs.isSuccess());

    final List<DataSegment> segments = resultPair.rhs;
    Assert.assertEquals(3, segments.size());

    for (int i = 0; i < 3; i++) {
      Assert.assertEquals(
          Intervals.of("2014-01-01T0%d:00:00/2014-01-01T0%d:00:00", i, i + 1),
          segments.get(i).getInterval()
      );
      Assert.assertEquals(DEFAULT_COMPACTION_STATE, segments.get(i).getLastCompactionState());
      if (lockGranularity == LockGranularity.SEGMENT) {
        Assert.assertEquals(
            new NumberedOverwriteShardSpec(32768, 0, 2, (short) 1, (short) 1),
            segments.get(i).getShardSpec()
        );
      } else {
        Assert.assertEquals(new NumberedShardSpec(0, 1), segments.get(i).getShardSpec());
      }
    }
  }

  private Pair<TaskStatus, List<DataSegment>> runIndexTask() throws Exception
  {
    return runIndexTask(null, null, false);
  }

  private Pair<TaskStatus, List<DataSegment>> runAppendTask() throws Exception
  {
    return runIndexTask(null, null, true);
  }

  private Pair<TaskStatus, List<DataSegment>> runIndexTask(
      @Nullable CountDownLatch readyLatchToCountDown,
      @Nullable CountDownLatch latchToAwaitBeforeRun,
      boolean appendToExisting
  ) throws Exception
  {
    File tmpDir = temporaryFolder.newFolder();
    File tmpFile = File.createTempFile("druid", "index", tmpDir);

    try (BufferedWriter writer = Files.newWriter(tmpFile, StandardCharsets.UTF_8)) {
      for (String testRow : TEST_ROWS) {
        writer.write(testRow);
      }
    }

    IndexTask indexTask = new IndexTask(
        null,
        null,
        IndexTaskTest.createIngestionSpec(
            getObjectMapper(),
            tmpDir,
            DEFAULT_PARSE_SPEC,
            null,
            new UniformGranularitySpec(
                Granularities.HOUR,
                Granularities.MINUTE,
                null
            ),
            IndexTaskTest.createTuningConfig(2, 2, null, 2L, null, false, true),
            appendToExisting
        ),
        null
    );

    return runTask(indexTask, readyLatchToCountDown, latchToAwaitBeforeRun);
  }

  private Pair<TaskStatus, List<DataSegment>> runTask(Task task) throws Exception
  {
    return runTask(task, null, null);
  }

  private Pair<TaskStatus, List<DataSegment>> runTask(
      Task task,
      @Nullable CountDownLatch readyLatchToCountDown,
      @Nullable CountDownLatch latchToAwaitBeforeRun
  ) throws Exception
  {
    getLockbox().add(task);
    getTaskStorage().insert(task, TaskStatus.running(task.getId()));

    final ObjectMapper objectMapper = getObjectMapper();
    objectMapper.registerSubtypes(new NamedType(LocalLoadSpec.class, "local"));
    objectMapper.registerSubtypes(LocalDataSegmentPuller.class);

    final TaskToolbox box = createTaskToolbox(objectMapper, task);

    task.addToContext(Tasks.FORCE_TIME_CHUNK_LOCK_KEY, lockGranularity == LockGranularity.TIME_CHUNK);
    if (task.isReady(box.getTaskActionClient())) {
      if (readyLatchToCountDown != null) {
        readyLatchToCountDown.countDown();
      }
      if (latchToAwaitBeforeRun != null) {
        latchToAwaitBeforeRun.await();
      }
      TaskStatus status = task.run(box);
      shutdownTask(task);
      final List<DataSegment> segments = new ArrayList<>(
          ((TestLocalTaskActionClient) box.getTaskActionClient()).getPublishedSegments()
      );
      Collections.sort(segments);
      return Pair.of(status, segments);
    } else {
      throw new ISE("task[%s] is not ready", task.getId());
    }
  }

  private TaskToolbox createTaskToolbox(ObjectMapper objectMapper, Task task) throws IOException
  {
    final SegmentLoader loader = new SegmentLoaderLocalCacheManager(
        getIndexIO(),
        new SegmentLoaderConfig() {
          @Override
          public List<StorageLocationConfig> getLocations()
          {
            return ImmutableList.of(new StorageLocationConfig(localDeepStorage, null, null));
          }
        },
        objectMapper
    );

    return new TaskToolbox(
        null,
        null,
        createActionClient(task),
        null,
        new LocalDataSegmentPusher(new LocalDataSegmentPusherConfig()),
        new NoopDataSegmentKiller(),
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        NoopJoinableFactory.INSTANCE,
        null,
        loader,
        objectMapper,
        temporaryFolder.newFolder(),
        getIndexIO(),
        null,
        null,
        null,
        getIndexMerger(),
        null,
        null,
        null,
        null,
        new NoopTestTaskReportFileWriter(),
        null,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        new NoopChatHandlerProvider(),
        testUtils.getRowIngestionMetersFactory(),
        new TestAppenderatorsManager(),
        indexingServiceClient,
        coordinatorClient,
        null,
        null
    );
  }

  private List<String> getCSVFormatRowsFromSegments(List<DataSegment> segments) throws Exception
  {

    final File cacheDir = temporaryFolder.newFolder();
    final SegmentLoader segmentLoader = segmentLoaderFactory.manufacturate(cacheDir);

    List<Cursor> cursors = new ArrayList<>();
    for (DataSegment segment : segments) {
      final File segmentFile = segmentLoader.getSegmentFiles(segment);

      final WindowedStorageAdapter adapter = new WindowedStorageAdapter(
          new QueryableIndexStorageAdapter(testUtils.getTestIndexIO().loadIndex(segmentFile)),
          segment.getInterval()
      );
      final Sequence<Cursor> cursorSequence = adapter.getAdapter().makeCursors(
          null,
          segment.getInterval(),
          VirtualColumns.EMPTY,
          Granularities.ALL,
          false,
          null
      );
      cursors.addAll(cursorSequence.toList());
    }

    List<String> rowsFromSegment = new ArrayList<>();
    for (Cursor cursor : cursors) {
      cursor.reset();
      while (!cursor.isDone()) {
        final DimensionSelector selector1 = cursor.getColumnSelectorFactory()
                                                  .makeDimensionSelector(new DefaultDimensionSpec("ts", "ts"));
        final DimensionSelector selector2 = cursor.getColumnSelectorFactory()
                                                  .makeDimensionSelector(new DefaultDimensionSpec("dim", "dim"));
        final DimensionSelector selector3 = cursor.getColumnSelectorFactory()
                                                  .makeDimensionSelector(new DefaultDimensionSpec("val", "val"));

        Object dimObject = selector2.getObject();
        String dimVal = null;
        if (dimObject instanceof String) {
          dimVal = (String) dimObject;
        } else if (dimObject instanceof List) {
          dimVal = String.join("|", (List<String>) dimObject);
        }

        rowsFromSegment.add(
            makeCSVFormatRow(
                selector1.getObject().toString(),
                dimVal,
                selector3.defaultGetObject().toString()
            )
        );

        cursor.advance();
      }
    }
    return rowsFromSegment;
  }

  private static String makeCSVFormatRow(
      String ts,
      String dim,
      String val
  )
  {
    return StringUtils.format("%s,%s,%s\n", ts, dim, val);
  }
}
