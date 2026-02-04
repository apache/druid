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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.client.coordinator.NoopCoordinatorClient;
import org.apache.druid.client.indexing.ClientCompactionTaskGranularitySpec;
import org.apache.druid.data.input.impl.CSVParseSpec;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.NewSpatialDimensionSchema;
import org.apache.druid.data.input.impl.ParseSpec;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.granularity.UniformGranularitySpec;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexer.report.SingleFileTaskReportFileWriter;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.SegmentCacheManagerFactory;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.common.actions.LocalTaskActionClient;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.actions.TaskActionTestKit;
import org.apache.druid.indexing.common.config.TaskConfigBuilder;
import org.apache.druid.indexing.common.task.CompactionTask.Builder;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.rpc.indexing.NoopOverlordClient;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.segment.AutoTypeColumnSchema;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.CursorHolder;
import org.apache.druid.segment.DataSegmentsWithSchemas;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexCursorFactory;
import org.apache.druid.segment.SegmentSchemaMapping;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.handoff.NoopSegmentHandoffNotifierFactory;
import org.apache.druid.segment.join.NoopJoinableFactory;
import org.apache.druid.segment.loading.LeastBytesUsedStorageLocationSelectorStrategy;
import org.apache.druid.segment.loading.LocalDataSegmentPuller;
import org.apache.druid.segment.loading.LocalDataSegmentPusher;
import org.apache.druid.segment.loading.LocalDataSegmentPusherConfig;
import org.apache.druid.segment.loading.LocalLoadSpec;
import org.apache.druid.segment.loading.NoopDataSegmentKiller;
import org.apache.druid.segment.loading.SegmentCacheManager;
import org.apache.druid.segment.loading.SegmentLoaderConfig;
import org.apache.druid.segment.loading.SegmentLocalCacheManager;
import org.apache.druid.segment.loading.StorageLocation;
import org.apache.druid.segment.loading.StorageLocationConfig;
import org.apache.druid.segment.loading.TombstoneLoadSpec;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.segment.nested.NestedCommonFormatColumnFormatSpec;
import org.apache.druid.segment.realtime.NoopChatHandlerProvider;
import org.apache.druid.segment.realtime.WindowedCursorFactory;
import org.apache.druid.segment.transform.CompactionTransformSpec;
import org.apache.druid.server.metrics.NoopServiceEmitter;
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
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public abstract class CompactionTaskRunBase
{
  protected static final String DATA_SOURCE = "test";
  protected static final ParseSpec DEFAULT_PARSE_SPEC = new CSVParseSpec(
      new TimestampSpec("ts", "auto", null),
      new DimensionsSpec(DimensionsSpec.getDefaultSchemas(Arrays.asList("ts", "dim"))),
      "|",
      Arrays.asList("ts", "dim", "val"),
      false,
      0
  );
  protected static final Granularity DEFAULT_SEGMENT_GRAN = Granularities.HOUR;
  protected static final Granularity DEFAULT_QUERY_GRAN = Granularities.MINUTE;
  protected static final DimensionsSpec DEFAULT_DIMENSIONS_SPEC = new DimensionsSpec(
      List.of(
          new StringDimensionSchema("ts", DimensionSchema.MultiValueHandling.ARRAY, null),
          new StringDimensionSchema("dim", DimensionSchema.MultiValueHandling.ARRAY, null)
      ));
  protected static final AggregatorFactory DEFAULT_AGGREGATOR = new LongSumAggregatorFactory("val", "val");

  private static final NestedCommonFormatColumnFormatSpec DEFAULT_NESTED_SPEC =
      NestedCommonFormatColumnFormatSpec.getEffectiveFormatSpec(null, IndexSpec.getDefault().getEffectiveSpec());

  protected static final Interval TEST_INTERVAL_DAY = Intervals.of("2014-01-01/2014-01-02");
  protected static final Interval TEST_INTERVAL = Intervals.of("2014-01-01T00:00:00Z/2014-01-01T03:00:00Z");
  protected static final List<String> TEST_ROWS = ImmutableList.of(
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
  protected static final int TOTAL_TEST_ROWS = 10;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TaskActionTestKit taskActionTestKit = new TaskActionTestKit();

  protected ObjectMapper objectMapper;
  protected File reportsFile;

  protected final OverlordClient overlordClient;
  protected final CoordinatorClient coordinatorClient;
  protected final SegmentCacheManagerFactory segmentCacheManagerFactory;

  protected final LockGranularity lockGranularity;
  protected final boolean useCentralizedDatasourceSchema;
  protected final boolean useConcurrentLocks;
  protected final Interval inputInterval;
  protected final Granularity segmentGranularity;
  protected final TestUtils testUtils;

  protected ExecutorService exec;
  protected File localDeepStorage;

  public CompactionTaskRunBase(
      String name,
      LockGranularity lockGranularity,
      boolean useCentralizedDatasourceSchema,
      boolean useSegmentMetadataCache,
      boolean useConcurrentLocks,
      Interval inputInterval,
      Granularity segmentGranularity
  ) throws IOException
  {
    this.lockGranularity = lockGranularity;
    this.useCentralizedDatasourceSchema = useCentralizedDatasourceSchema;
    taskActionTestKit.setUseCentralizedDatasourceSchema(useCentralizedDatasourceSchema)
                     .setUseSegmentMetadataCache(useSegmentMetadataCache)
                     .setBatchSegmentAllocation(false);
    this.useConcurrentLocks = useConcurrentLocks;
    this.inputInterval = inputInterval;
    this.segmentGranularity = segmentGranularity;

    temporaryFolder.create();
    reportsFile = temporaryFolder.newFile();
    testUtils = new TestUtils();
    segmentCacheManagerFactory = new SegmentCacheManagerFactory(TestIndex.INDEX_IO, testUtils.getTestObjectMapper());

    objectMapper = testUtils.getTestObjectMapper();
    objectMapper.registerSubtypes(new NamedType(LocalLoadSpec.class, "local"));
    objectMapper.registerSubtypes(LocalDataSegmentPuller.class);
    objectMapper.registerSubtypes(TombstoneLoadSpec.class);

    overlordClient = new NoopOverlordClient();
    coordinatorClient = new NoopCoordinatorClient()
    {
      @Override
      public ListenableFuture<List<DataSegment>> fetchUsedSegments(
          String dataSource,
          List<Interval> intervals
      )
      {
        return Futures.immediateFuture(
            List.copyOf(taskActionTestKit.getTaskActionToolbox()
                                         .getIndexerMetadataStorageCoordinator()
                                         .retrieveUsedSegmentsForIntervals(
                                             dataSource,
                                             intervals,
                                             Segments.ONLY_VISIBLE
                                         )));
      }

      @Override
      public ListenableFuture<DataSegment> fetchSegment(String dataSource, String segmentId, boolean includeUnused)
      {
        return Futures.immediateFuture(
            taskActionTestKit.getTaskActionToolbox()
                             .getIndexerMetadataStorageCoordinator()
                             .retrieveUsedSegmentsForIntervals(
                                 dataSource,
                                 List.of(Intervals.ETERNITY),
                                 Segments.INCLUDING_OVERSHADOWED
                             )
                             .stream()
                             .filter(s -> s.getId().toString().equals(segmentId))
                             .findFirst()
                             .get());
      }
    };
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
    verifyTaskSuccessRowsAndSchemaMatch(runIndexTask(), TOTAL_TEST_ROWS);

    final CompactionTask compactionTask =
        compactionTaskBuilder(segmentGranularity).interval(inputInterval, true).build();

    final Pair<TaskStatus, DataSegmentsWithSchemas> resultPair = runTask(compactionTask);
    verifyTaskSuccessRowsAndSchemaMatch(resultPair, TOTAL_TEST_ROWS);

    final DataSegmentsWithSchemas dataSegmentsWithSchemas = resultPair.rhs;
    final List<DataSegment> segments = new ArrayList<>(dataSegmentsWithSchemas.getSegments());
    List<String> rowsFromSegment = getCSVFormatRowsFromSegments(segments);
    Assert.assertEquals(TEST_ROWS, rowsFromSegment);
    verifyCompactedSegment(segments, segmentGranularity, DEFAULT_QUERY_GRAN, false);
  }

  @Test
  public void testRunWithHashPartitioning() throws Exception
  {
    // Hash partitioning is not supported with segment lock yet
    Assume.assumeTrue(
        "Hash partitioning is not supported with segment lock yet",
        lockGranularity != LockGranularity.SEGMENT
    );
    Assume.assumeTrue("Test null segment granularity is sufficient", segmentGranularity == null);
    verifyTaskSuccessRowsAndSchemaMatch(runIndexTask(), TOTAL_TEST_ROWS);

    final CompactionTask compactionTask =
        compactionTaskBuilder(segmentGranularity)
            .interval(inputInterval, true)
            .tuningConfig(TuningConfigBuilder.forParallelIndexTask()
                                             .withForceGuaranteedRollup(true)
                                             .withPartitionsSpec(new HashedPartitionsSpec(null, 3, null))
                                             .build())
            .build();

    final Pair<TaskStatus, DataSegmentsWithSchemas> resultPair = runTask(compactionTask);
    verifyTaskSuccessRowsAndSchemaMatch(resultPair, TOTAL_TEST_ROWS);

    final List<DataSegment> segments = List.copyOf(resultPair.rhs.getSegments());
    List<String> rowsFromSegment = getCSVFormatRowsFromSegments(segments);
    rowsFromSegment.sort(Ordering.natural());
    Assert.assertEquals(TEST_ROWS, rowsFromSegment);
    Assert.assertEquals(6, segments.size());

    for (int i = 0; i < 3; i++) {
      final Interval interval = Intervals.of("2014-01-01T0%d:00:00/2014-01-01T0%d:00:00", i, i + 1);
      for (int j = 0; j < 2; j++) {
        final int segmentIdx = i * 2 + j;
        Assert.assertEquals(interval, segments.get(segmentIdx).getInterval());
        CompactionState expectedState =
            getDefaultCompactionState(Granularities.HOUR, Granularities.MINUTE, List.of(interval))
                .toBuilder()
                .partitionsSpec(new HashedPartitionsSpec(null, 3, null))
                .indexSpec(compactionTask.getTuningConfig().getIndexSpec().getEffectiveSpec())
                .build();
        Assert.assertEquals(expectedState, segments.get(segmentIdx).getLastCompactionState());
        Assert.assertSame(HashBasedNumberedShardSpec.class, segments.get(segmentIdx).getShardSpec().getClass());
      }
    }
  }

  @Test
  public void testRunCompactionTwice() throws Exception
  {
    Assume.assumeTrue(lockGranularity == LockGranularity.TIME_CHUNK);
    verifyTaskSuccessRowsAndSchemaMatch(runIndexTask(), TOTAL_TEST_ROWS);

    final CompactionTask compactionTask1 =
        compactionTaskBuilder(segmentGranularity).interval(inputInterval, true).build();

    final Pair<TaskStatus, DataSegmentsWithSchemas> resultPair1 = runTask(compactionTask1);
    verifyTaskSuccessRowsAndSchemaMatch(resultPair1, TOTAL_TEST_ROWS);
    verifyCompactedSegment(List.copyOf(resultPair1.rhs.getSegments()), segmentGranularity, DEFAULT_QUERY_GRAN, false);

    final CompactionTask compactionTask2 =
        compactionTaskBuilder(segmentGranularity).interval(inputInterval, true).build();

    final Pair<TaskStatus, DataSegmentsWithSchemas> resultPair2 = runTask(compactionTask2);
    verifyTaskSuccessRowsAndSchemaMatch(resultPair2, TOTAL_TEST_ROWS);
    verifyCompactedSegment(List.copyOf(resultPair2.rhs.getSegments()), segmentGranularity, DEFAULT_QUERY_GRAN, false);
  }

  @Test
  public void testRunCompactionTwiceWithSegmentLock() throws Exception
  {
    Assume.assumeTrue(lockGranularity == LockGranularity.SEGMENT);
    verifyTaskSuccessRowsAndSchemaMatch(runIndexTask(), TOTAL_TEST_ROWS);

    final CompactionTask compactionTask1 =
        compactionTaskBuilder(segmentGranularity).interval(inputInterval, false).build();

    final Pair<TaskStatus, DataSegmentsWithSchemas> resultPair1 = runTask(compactionTask1);
    verifyTaskSuccessRowsAndSchemaMatch(resultPair1, TOTAL_TEST_ROWS);
    verifyCompactedSegment(List.copyOf(resultPair1.rhs.getSegments()), segmentGranularity, DEFAULT_QUERY_GRAN, true);

    final CompactionTask compactionTask2 =
        compactionTaskBuilder(segmentGranularity).interval(inputInterval, false).build();

    final Pair<TaskStatus, DataSegmentsWithSchemas> resultPair2 = runTask(compactionTask2);
    verifyTaskSuccessRowsAndSchemaMatch(resultPair2, TOTAL_TEST_ROWS);
    List<DataSegment> segments = List.copyOf(resultPair2.rhs.getSegments());
    if (segmentGranularity == null || segmentGranularity.equals(Granularities.HOUR)) {
      Assert.assertEquals(3, segments.size());

      for (int i = 0; i < 3; i++) {
        Interval interval = Intervals.of("2014-01-01T0%d:00:00/2014-01-01T0%d:00:00", i, i + 1);
        Assert.assertEquals(interval, segments.get(i).getInterval());
        Interval inputInterval = segmentGranularity == null ? interval : this.inputInterval;
        Assert.assertEquals(
            getDefaultCompactionState(DEFAULT_SEGMENT_GRAN, DEFAULT_QUERY_GRAN, List.of(inputInterval)),
            segments.get(i).getLastCompactionState()
        );
        // overwrite shard starts at NON_ROOT_GEN_START_PARTITION_ID + 1, and minor version 2 for the second compaction
        Assert.assertEquals(new NumberedOverwriteShardSpec(
            PartitionIds.NON_ROOT_GEN_START_PARTITION_ID + 1,
            0,
            2,
            (short) 2,
            (short) 1
        ), segments.get(i).getShardSpec());
      }
    } else if (segmentGranularity.equals(Granularities.THREE_HOUR)) {
      Assert.assertEquals(1, segments.size());
      Assert.assertEquals(TEST_INTERVAL, segments.get(0).getInterval());
      Assert.assertEquals(
          getDefaultCompactionState(segmentGranularity, DEFAULT_QUERY_GRAN, List.of(inputInterval)),
          segments.get(0).getLastCompactionState()
      );
      // use overwrite shard for the second compaction
      Assert.assertEquals(new NumberedOverwriteShardSpec(
          PartitionIds.NON_ROOT_GEN_START_PARTITION_ID,
          0,
          1,
          (short) 1,
          (short) 1
      ), segments.get(0).getShardSpec());
    } else {
      throw new RE("Gran[%s] is not supported", segmentGranularity);
    }
  }

  @Test
  public void testRunIndexAndCompactAtTheSameTimeForDifferentInterval() throws Exception
  {
    Assume.assumeTrue("Use 3 hr interval to compact", TEST_INTERVAL.equals(inputInterval));
    verifyTaskSuccessRowsAndSchemaMatch(runIndexTask(), TOTAL_TEST_ROWS);

    final CompactionTask compactionTask =
        compactionTaskBuilder(segmentGranularity).interval(inputInterval, true).build();
    List<String> rows = new ArrayList<>();
    rows.add("2014-01-01T03:00:10Z,a,1\n");
    rows.add("2014-01-01T03:00:10Z,b,2\n");
    rows.add("2014-01-01T03:00:10Z,c,3\n");
    rows.add("2014-01-01T04:00:20Z,a,1\n");
    rows.add("2014-01-01T04:00:20Z,b,2\n");
    rows.add("2014-01-01T04:00:20Z,c,3\n");
    rows.add("2014-01-01T05:00:30Z,a,1\n");
    rows.add("2014-01-01T05:00:30Z,b,2\n");
    rows.add("2014-01-01T05:00:30Z,c,3\n");
    final IndexTask indexTask = buildIndexTask(
        DEFAULT_PARSE_SPEC,
        rows,
        Intervals.of("2014-01-01T03:00:00Z/2014-01-01T06:00:00Z"),
        false
    );

    final Future<Pair<TaskStatus, DataSegmentsWithSchemas>> compactionFuture = exec.submit(
        () -> runTask(compactionTask)
    );

    final Future<Pair<TaskStatus, DataSegmentsWithSchemas>> indexFuture = exec.submit(
        () -> runTask(indexTask)
    );

    Pair<TaskStatus, DataSegmentsWithSchemas> indexResult = indexFuture.get();
    verifyTaskSuccessRowsAndSchemaMatch(indexResult, 9);

    List<DataSegment> segments = new ArrayList<>(indexResult.rhs.getSegments());
    Assert.assertEquals(6, segments.size());

    for (int i = 0; i < 6; i++) {
      Assert.assertEquals(
          Intervals.of("2014-01-01T0%d:00:00/2014-01-01T0%d:00:00", 3 + i / 2, 3 + i / 2 + 1),
          segments.get(i).getInterval()
      );
      if (lockGranularity == LockGranularity.SEGMENT) {
        Assert.assertEquals(new NumberedShardSpec(i % 2, 0), segments.get(i).getShardSpec());
      } else {
        Assert.assertEquals(new NumberedShardSpec(i % 2, 2), segments.get(i).getShardSpec());
      }
    }

    Pair<TaskStatus, DataSegmentsWithSchemas> compactionResult = compactionFuture.get();
    verifyTaskSuccessRowsAndSchemaMatch(compactionResult, TOTAL_TEST_ROWS);
    verifyCompactedSegment(
        List.copyOf(compactionResult.rhs.getSegments()),
        segmentGranularity,
        DEFAULT_QUERY_GRAN,
        false
    );
  }

  @Test
  public void testWithSegmentGranularityMisalignedInterval() throws Exception
  {
    Assume.assumeTrue(
        "use Granularities.WEEK segment granularity in this test",
        Granularities.THREE_HOUR.equals(segmentGranularity)
    );
    verifyTaskSuccessRowsAndSchemaMatch(runIndexTask(), TOTAL_TEST_ROWS);
    // Test when inputInterval is less than Granularities.WEEK is not allowed
    final CompactionTask compactionTask1 =
        compactionTaskBuilder(Granularities.WEEK)
            .ioConfig(new CompactionIOConfig(new CompactionIntervalSpec(inputInterval, null), false, true))
            .build();

    final IllegalArgumentException e = Assert.assertThrows(
        IllegalArgumentException.class,
        () -> runTask(compactionTask1)
    );
    Assert.assertTrue(e.getMessage().contains(inputInterval.toString()));
    Assert.assertTrue(e.getMessage().contains("is not aligned with segmentGranularity"));
    Assert.assertTrue(e.getMessage().contains(Granularities.WEEK.toString()));
  }

  @Test
  public void testWithSegmentGranularityMisalignedIntervalAllowed() throws Exception
  {
    Assume.assumeTrue(
        "use Granularities.WEEK segment granularity in this test",
        Granularities.THREE_HOUR.equals(segmentGranularity)
    );
    verifyTaskSuccessRowsAndSchemaMatch(runIndexTask(), TOTAL_TEST_ROWS);
    // Test when inputInterval is less than Granularities.WEEK is allowed
    final CompactionTask compactionTask1 =
        compactionTaskBuilder(Granularities.WEEK)
            .ioConfig(new CompactionIOConfig(new CompactionIntervalSpec(inputInterval, null), true, null))
            .build();

    Pair<TaskStatus, DataSegmentsWithSchemas> resultPair = runTask(compactionTask1);
    verifyTaskSuccessRowsAndSchemaMatch(resultPair, TOTAL_TEST_ROWS);

    List<DataSegment> segments = new ArrayList<>(resultPair.rhs.getSegments());
    Assert.assertEquals(1, segments.size());
    Assert.assertEquals(Intervals.of("2013-12-30/2014-01-06"), segments.get(0).getInterval());
    Assert.assertEquals(new NumberedShardSpec(0, 1), segments.get(0).getShardSpec());
    Assert.assertEquals(
        getDefaultCompactionState(
            Granularities.WEEK,
            DEFAULT_QUERY_GRAN,
            List.of(inputInterval)
        ),
        segments.get(0).getLastCompactionState()
    );
  }

  @Test
  public void testWithSegmentGranularityMisalignedIntervalAllowed2() throws Exception
  {
    Assume.assumeTrue(
        "test with defined segment granularity and interval in this test",
        Granularities.THREE_HOUR.equals(segmentGranularity) && TEST_INTERVAL.equals(inputInterval)
        && lockGranularity != LockGranularity.SEGMENT
    );
    verifyTaskSuccessRowsAndSchemaMatch(runIndexTask(), TOTAL_TEST_ROWS);
    // Test when inputInterval doesn't align with segment granularity
    final Interval interval = Intervals.of("2014-01-01T00:30:00Z/2014-01-01T01:30:00Z");
    final CompactionTask compactionTask1 =
        compactionTaskBuilder(Granularities.HOUR)
            .ioConfig(new CompactionIOConfig(new CompactionIntervalSpec(interval, null), true, null))
            .build();

    Pair<TaskStatus, DataSegmentsWithSchemas> resultPair = runTask(compactionTask1);
    verifyTaskSuccessRowsAndSchemaMatch(resultPair, 3);

    List<DataSegment> segments = new ArrayList<>(resultPair.rhs.getSegments());
    Assert.assertEquals(1, segments.size());
    Assert.assertEquals(Intervals.of("2014-01-01T01:00:00Z/2014-01-01T02:00:00Z"), segments.get(0).getInterval());
    Assert.assertEquals(new NumberedShardSpec(0, 1), segments.get(0).getShardSpec());
    Assert.assertEquals(
        getDefaultCompactionState(
            Granularities.HOUR,
            DEFAULT_QUERY_GRAN,
            List.of(Intervals.of("2014-01-01T00:00:00Z/2014-01-01T02:00:00Z"))
        ),
        segments.get(0).getLastCompactionState()
    );
  }

  @Test
  public void testCompactionWithFilterInTransformSpec() throws Exception
  {
    Assume.assumeTrue(
        "test with three hour granularity is enough",
        Granularities.THREE_HOUR.equals(segmentGranularity)
    );
    verifyTaskSuccessRowsAndSchemaMatch(runIndexTask(), TOTAL_TEST_ROWS);

    final CompactionTask compactionTask = compactionTaskBuilder(segmentGranularity)
        .interval(inputInterval, true)
        .transformSpec(new CompactionTransformSpec(new SelectorDimFilter("dim", "a", null)))
        .build();

    Pair<TaskStatus, DataSegmentsWithSchemas> resultPair = runTask(compactionTask);
    verifyTaskSuccessRowsAndSchemaMatch(resultPair, 3);

    List<DataSegment> segments = new ArrayList<>(resultPair.rhs.getSegments());

    Assert.assertEquals(1, segments.size());
    Assert.assertEquals(TEST_INTERVAL, segments.get(0).getInterval());
    Assert.assertEquals(new NumberedShardSpec(0, 1), segments.get(0).getShardSpec());

    CompactionState expectedCompactionState = getDefaultCompactionState(
        segmentGranularity,
        DEFAULT_QUERY_GRAN,
        List.of(inputInterval)
    ).toBuilder().transformSpec(new CompactionTransformSpec(new SelectorDimFilter("dim", "a", null))).build();
    Assert.assertEquals(expectedCompactionState, segments.get(0).getLastCompactionState());
  }

  public void validateCompactionState(CompactionState expected, CompactionState actual)
  {
    Assert.assertEquals(new CompactionState(
        expected.getPartitionsSpec(),
        expected.getDimensionsSpec().toBuilder().setDimensionExclusions(List.of()).build(),
        expected.getMetricsSpec(),
        null,
        expected.getIndexSpec(),
        expected.getGranularitySpec().withIntervals(List.of()),
        expected.getProjections()
    ), new CompactionState(
        actual.getPartitionsSpec(),
        actual.getDimensionsSpec().toBuilder().setDimensionExclusions(List.of()).build(),
        actual.getMetricsSpec(),
        null,
        actual.getIndexSpec(),
        actual.getGranularitySpec().withIntervals(List.of()),
        actual.getProjections()
    ));
  }

  @Test
  public void testCompactionWithNewMetricInMetricsSpec() throws Exception
  {
    Assume.assumeTrue(
        "test with three hour granularity is enough",
        Granularities.THREE_HOUR.equals(segmentGranularity)
    );
    verifyTaskSuccessRowsAndSchemaMatch(runIndexTask(), TOTAL_TEST_ROWS);

    final CompactionTask compactionTask =
        compactionTaskBuilder(new ClientCompactionTaskGranularitySpec(segmentGranularity, null, true))
            .interval(inputInterval, true)
            .metricsSpec(new AggregatorFactory[]{
                new CountAggregatorFactory("cnt"),
                new LongSumAggregatorFactory("val", "val")
            })
            .build();

    Pair<TaskStatus, DataSegmentsWithSchemas> resultPair = runTask(compactionTask);
    verifyTaskSuccessRowsAndSchemaMatch(resultPair, TOTAL_TEST_ROWS);

    List<DataSegment> segments = new ArrayList<>(resultPair.rhs.getSegments());
    Assert.assertEquals(1, segments.size());

    Assert.assertEquals(TEST_INTERVAL, segments.get(0).getInterval());
    Assert.assertEquals(new NumberedShardSpec(0, 1), segments.get(0).getShardSpec());

    AggregatorFactory expectedCountMetric = new CountAggregatorFactory("cnt");
    AggregatorFactory expectedLongSumMetric = new LongSumAggregatorFactory("val", "val");
    CompactionState expectedCompactionState =
        getDefaultCompactionState(segmentGranularity, Granularities.MINUTE, List.of(inputInterval))
            .toBuilder()
            .metricsSpec(List.of(expectedCountMetric, expectedLongSumMetric))
            .build();
    Assert.assertEquals(expectedCompactionState, segments.get(0).getLastCompactionState());
  }

  @Test
  public void testWithGranularitySpecNonNullQueryGranularity() throws Exception
  {
    verifyTaskSuccessRowsAndSchemaMatch(runIndexTask(), TOTAL_TEST_ROWS);

    // second queryGranularity
    final CompactionTask compactionTask1 =
        compactionTaskBuilder(new ClientCompactionTaskGranularitySpec(segmentGranularity, Granularities.SECOND, null))
            .interval(inputInterval, true)
            .build();

    Pair<TaskStatus, DataSegmentsWithSchemas> resultPair = runTask(compactionTask1);
    verifyTaskSuccessRowsAndSchemaMatch(resultPair, TOTAL_TEST_ROWS);

    List<DataSegment> segments = new ArrayList<>(resultPair.rhs.getSegments());
    verifyCompactedSegment(segments, segmentGranularity, Granularities.SECOND, false);
  }

  @Test
  public void testWithGranularitySpecNonNullQueryGranularityAndCoarseSegmentGranularity() throws Exception
  {
    Assume.assumeTrue(
        "test with defined segment granularity and interval in this test",
        Granularities.THREE_HOUR.equals(segmentGranularity) && TEST_INTERVAL.equals(inputInterval)
    );
    verifyTaskSuccessRowsAndSchemaMatch(runIndexTask(), TOTAL_TEST_ROWS);

    // day segmentGranularity and day queryGranularity
    final CompactionTask compactionTask1 =
        compactionTaskBuilder(new ClientCompactionTaskGranularitySpec(Granularities.DAY, Granularities.DAY, null))
            .inputSpec(new CompactionIntervalSpec(TEST_INTERVAL_DAY, null), true)
            .build();

    Pair<TaskStatus, DataSegmentsWithSchemas> resultPair = runTask(compactionTask1);
    verifyTaskSuccessRowsAndSchemaMatch(resultPair, TOTAL_TEST_ROWS);

    List<DataSegment> segments = List.copyOf(resultPair.rhs.getSegments());
    Assert.assertEquals(1, segments.size());
    Assert.assertEquals(TEST_INTERVAL_DAY, segments.get(0).getInterval());
    Assert.assertEquals(
        getDefaultCompactionState(Granularities.DAY, Granularities.DAY, List.of(TEST_INTERVAL_DAY)),
        segments.get(0).getLastCompactionState()
    );
    Assert.assertEquals(new NumberedShardSpec(0, 1), segments.get(0).getShardSpec());
  }

  @Test
  public void testCompactThenAppend() throws Exception
  {
    Assume.assumeTrue(
        "test three hour segment granularity is enough",
        Granularities.THREE_HOUR.equals(segmentGranularity)
    );
    verifyTaskSuccessRowsAndSchemaMatch(runIndexTask(), TOTAL_TEST_ROWS);

    final CompactionTask compactionTask =
        compactionTaskBuilder(segmentGranularity).interval(inputInterval, true).build();

    final Pair<TaskStatus, DataSegmentsWithSchemas> compactionResult = runTask(compactionTask);
    verifyTaskSuccessRowsAndSchemaMatch(compactionResult, TOTAL_TEST_ROWS);

    final Pair<TaskStatus, DataSegmentsWithSchemas> appendResult = runAppendTask();
    verifyTaskSuccessRowsAndSchemaMatch(appendResult, TOTAL_TEST_ROWS);

    final Set<DataSegment> expectedSegments = Sets.union(
        compactionResult.rhs.getSegments(),
        appendResult.rhs.getSegments()
    );

    final Set<DataSegment> usedSegments = new HashSet<>(
        coordinatorClient.fetchUsedSegments(DATA_SOURCE, List.of(Intervals.of("2014-01-01/2014-01-02"))).get());

    Assert.assertEquals(expectedSegments, usedSegments);
  }

  @Test
  public void testPartialIntervalCompactWithFinerSegmentGranularityThanFullIntervalCompactWithDropExistingTrue()
      throws Exception
  {
    // This test fails with segment lock because of the bug reported in https://github.com/apache/druid/issues/10911.
    Assume.assumeTrue(lockGranularity != LockGranularity.SEGMENT);
    Assume.assumeTrue(
        "test with defined segment granularity and interval in this test",
        Granularities.THREE_HOUR.equals(segmentGranularity) && TEST_INTERVAL.equals(inputInterval)
    );

    // The following task creates (several, more than three, last time I checked, six) HOUR segments with intervals of
    // - 2014-01-01T00:00:00/2014-01-01T01:00:00
    // - 2014-01-01T01:00:00/2014-01-01T02:00:00
    // - 2014-01-01T02:00:00/2014-01-01T03:00:00
    // The six segments are:
    // three rows in hour 00:
    // 2014-01-01T00:00:00.000Z_2014-01-01T01:00:00.000Z with two rows
    // 2014-01-01T00:00:00.000Z_2014-01-01T01:00:00.000Z_1 with one row
    // three rows in hour 01:
    // 2014-01-01T01:00:00.000Z_2014-01-01T02:00:00.000Z with two rows
    // 2014-01-01T01:00:00.000Z_2014-01-01T02:00:00.000Z_1 with one row
    // four rows in hour 02:
    // 2014-01-01T02:00:00.000Z_2014-01-01T03:00:00.000Z with two rows
    // 2014-01-01T02:00:00.000Z_2014-01-01T03:00:00.000Z_1 with two rows
    // there are 10 rows total in data set

    // maxRowsPerSegment is set to 2 inside the runIndexTask methods
    Pair<TaskStatus, DataSegmentsWithSchemas> result = runIndexTask();
    verifyTaskSuccessRowsAndSchemaMatch(result, TOTAL_TEST_ROWS);
    Assert.assertEquals(6, result.rhs.getSegments().size());

    // Setup partial compaction:
    // Change the granularity from HOUR to MINUTE through compaction for hour 01, there are three rows in the compaction interval,
    // all three in the same timestamp (see TEST_ROWS), this should generate one segments (task will now use
    // the default rows per segments since compaction's tuning config is null) in same minute and
    // 59 tombstones to completely overshadow the existing hour 01 segment. Since the segments outside the
    // compaction interval should remanin unchanged there should be a total of 1 + (2 + 59) + 2 = 64 segments

    // **** PARTIAL COMPACTION: hour -> minute ****
    final Interval compactionPartialInterval = Intervals.of("2014-01-01T01:00:00/2014-01-01T02:00:00");
    final CompactionTask partialCompactionTask =
        compactionTaskBuilder(Granularities.MINUTE)
            // Set dropExisting to true
            .inputSpec(new CompactionIntervalSpec(compactionPartialInterval, null), true)
            .build();
    final Pair<TaskStatus, DataSegmentsWithSchemas> partialCompactionResult = runTask(partialCompactionTask);
    verifyTaskSuccessRowsAndSchemaMatch(partialCompactionResult, 3);

    // Segments that did not belong in the compaction interval (hours 00 and 02) are expected unchanged
    // add 2 unchanged segments for hour 00:
    final Set<DataSegment> expectedSegments = new HashSet<>();
    expectedSegments.addAll(
        coordinatorClient.fetchUsedSegments(
            DATA_SOURCE,
            List.of(Intervals.of("2014-01-01T00:00:00/2014-01-01T01:00:00"))
        ).get());
    // add 2 unchanged segments for hour 02:
    expectedSegments.addAll(
        coordinatorClient.fetchUsedSegments(
            DATA_SOURCE,
            List.of(Intervals.of("2014-01-01T02:00:00/2014-01-01T03:00:00"))
        ).get());
    expectedSegments.addAll(partialCompactionResult.rhs.getSegments());
    Assert.assertEquals(64, expectedSegments.size());

    // New segments that were compacted are expected. However, old segments of the compacted interval should be
    // overshadowed by the new tombstones (59) being created for all minutes other than 01:01
    final Set<DataSegment> segmentsAfterPartialCompaction = new HashSet<>(
        coordinatorClient.fetchUsedSegments(DATA_SOURCE, List.of(Intervals.of("2014-01-01/2014-01-02"))).get());
    Assert.assertEquals(expectedSegments, segmentsAfterPartialCompaction);
    final List<DataSegment> realSegmentsAfterPartialCompaction =
        segmentsAfterPartialCompaction.stream().filter(s -> !s.isTombstone()).collect(Collectors.toList());
    final List<DataSegment> tombstonesAfterPartialCompaction =
        segmentsAfterPartialCompaction.stream().filter(s -> s.isTombstone()).collect(Collectors.toList());
    Assert.assertEquals(59, tombstonesAfterPartialCompaction.size());
    Assert.assertEquals(5, realSegmentsAfterPartialCompaction.size());
    Assert.assertEquals(64, segmentsAfterPartialCompaction.size());

    // Setup full compaction:
    // Full Compaction with null segmentGranularity meaning that the original segmentGranularity is preserved.
    // For the intervals, 2014-01-01T00:00:00.000Z/2014-01-01T01:00:00.000Z and 2014-01-01T02:00:00.000Z/2014-01-01T03:00:00.000Z
    // the original segmentGranularity is HOUR from the initial ingestion.
    // For the interval, 2014-01-01T01:00:00.000Z/2014-01-01T01:01:00.000Z, the original segmentGranularity is
    // MINUTE from the partial compaction done earlier.
    // Again since the tuningconfig for the compaction is null, the maxRowsPerSegment is the default so
    // for hour 00 one real HOUR segment will be generated;
    // for hour 01, one real minute segment plus 59 minute tombstones;
    // and hour 02 one real HOUR segment for a total of 1 + (1+59) + 1 = 62 total segments
    final CompactionTask fullCompactionTask =
        compactionTaskBuilder((Granularity) null)
            // Set dropExisting to true
            .inputSpec(new CompactionIntervalSpec(Intervals.of("2014-01-01/2014-01-02"), null), true)
            .build();

    // **** FULL COMPACTION ****
    final Pair<TaskStatus, DataSegmentsWithSchemas> fullCompactionResult = runTask(fullCompactionTask);
    verifyTaskSuccessRowsAndSchemaMatch(fullCompactionResult, TOTAL_TEST_ROWS);

    final List<DataSegment> segmentsAfterFullCompaction = new ArrayList<>(
        coordinatorClient.fetchUsedSegments(DATA_SOURCE, List.of(Intervals.of("2014-01-01/2014-01-02"))).get());
    segmentsAfterFullCompaction.sort(
        (s1, s2) -> Comparators.intervalsByStartThenEnd().compare(s1.getInterval(), s2.getInterval())
    );
    Assert.assertEquals(62, segmentsAfterFullCompaction.size());

    final List<DataSegment> tombstonesAfterFullCompaction =
        segmentsAfterFullCompaction.stream().filter(s -> s.isTombstone()).collect(Collectors.toList());
    Assert.assertEquals(59, tombstonesAfterFullCompaction.size());

    final List<DataSegment> realSegmentsAfterFullCompaction =
        segmentsAfterFullCompaction.stream().filter(s -> !s.isTombstone()).collect(Collectors.toList());
    Assert.assertEquals(3, realSegmentsAfterFullCompaction.size());

    Assert.assertEquals(
        Intervals.of("2014-01-01T00:00:00.000Z/2014-01-01T01:00:00.000Z"),
        realSegmentsAfterFullCompaction.get(0).getInterval()
    );
    Assert.assertEquals(
        Intervals.of("2014-01-01T01:00:00.000Z/2014-01-01T01:01:00.000Z"),
        realSegmentsAfterFullCompaction.get(1).getInterval()
    );
    Assert.assertEquals(
        Intervals.of("2014-01-01T02:00:00.000Z/2014-01-01T03:00:00.000Z"),
        realSegmentsAfterFullCompaction.get(2).getInterval()
    );
  }

  @Test
  public void testCompactDatasourceOverIntervalWithOnlyTombstones() throws Exception
  {
    // This test fails with segment lock because of the bug reported in https://github.com/apache/druid/issues/10911.
    Assume.assumeTrue(lockGranularity != LockGranularity.SEGMENT);
    Assume.assumeTrue(
        "test with defined segment granularity and interval in this test",
        Granularities.THREE_HOUR.equals(segmentGranularity) && TEST_INTERVAL.equals(inputInterval)
    );

    // The following task creates (several, more than three, last time I checked, six) HOUR segments with intervals of
    // - 2014-01-01T00:00:00/2014-01-01T01:00:00
    // - 2014-01-01T01:00:00/2014-01-01T02:00:00
    // - 2014-01-01T02:00:00/2014-01-01T03:00:00
    // The six segments are:
    // three rows in hour 00:
    // 2014-01-01T00:00:00.000Z_2014-01-01T01:00:00.000Z with two rows
    // 2014-01-01T00:00:00.000Z_2014-01-01T01:00:00.000Z_1 with one row
    // three rows in hour 01:
    // 2014-01-01T01:00:00.000Z_2014-01-01T02:00:00.000Z with two rows
    // 2014-01-01T01:00:00.000Z_2014-01-01T02:00:00.000Z_1 with one row
    // four rows in hour 02:
    // 2014-01-01T02:00:00.000Z_2014-01-01T03:00:00.000Z with two rows
    // 2014-01-01T02:00:00.000Z_2014-01-01T03:00:00.000Z_1 with two rows
    // there are 10 rows total in data set

    // maxRowsPerSegment is set to 2 inside the runIndexTask methods
    Pair<TaskStatus, DataSegmentsWithSchemas> result = runIndexTask();
    verifyTaskSuccessRowsAndSchemaMatch(result, TOTAL_TEST_ROWS);
    Assert.assertEquals(6, result.rhs.getSegments().size());

    // Setup partial interval compaction:
    // Change the granularity from HOUR to MINUTE through compaction for hour 01, there are three rows in the compaction
    // interval,
    // all three in the same timestamp (see TEST_ROWS), this should generate one segment in same, first, minute
    // (task will now use
    // the default rows per segments since compaction's tuning config is null) and
    // 59 tombstones to completely overshadow the existing hour 01 segment. Since the segments outside the
    // compaction interval should remanin unchanged there should be a total of 1 + (2 + 59) + 2 = 64 segments

    // **** PARTIAL COMPACTION: hour -> minute ****
    final Interval compactionPartialInterval = Intervals.of("2014-01-01T01:00:00/2014-01-01T02:00:00");
    final CompactionTask partialCompactionTask = compactionTaskBuilder(Granularities.MINUTE)
        // Set dropExisting to true
        .inputSpec(new CompactionIntervalSpec(compactionPartialInterval, null), true)
        .build();
    final Pair<TaskStatus, DataSegmentsWithSchemas> partialCompactionResult = runTask(partialCompactionTask);
    verifyTaskSuccessRowsAndSchemaMatch(partialCompactionResult, 3);

    // Segments that did not belong in the compaction interval (hours 00 and 02) are expected unchanged
    // add 2 unchanged segments for hour 00:
    final Set<DataSegment> expectedSegments = new HashSet<>();
    expectedSegments.addAll(
        coordinatorClient.fetchUsedSegments(
            DATA_SOURCE,
            List.of(Intervals.of("2014-01-01T00:00:00/2014-01-01T01:00:00"))
        ).get());
    // add 2 unchanged segments for hour 02:
    expectedSegments.addAll(
        coordinatorClient.fetchUsedSegments(
            DATA_SOURCE,
            List.of(Intervals.of("2014-01-01T02:00:00/2014-01-01T03:00:00"))
        ).get());
    expectedSegments.addAll(partialCompactionResult.rhs.getSegments());
    Assert.assertEquals(64, expectedSegments.size());

    // New segments that were compacted are expected. However, old segments of the compacted interval should be
    // overshadowed by the new tombstones (59) being created for all minutes other than 01:01
    final Set<DataSegment> segmentsAfterPartialCompaction = new HashSet<>(
        coordinatorClient.fetchUsedSegments(DATA_SOURCE, List.of(TEST_INTERVAL)).get());
    Assert.assertEquals(expectedSegments, segmentsAfterPartialCompaction);
    final List<DataSegment> realSegmentsAfterPartialCompaction =
        segmentsAfterPartialCompaction.stream().filter(s -> !s.isTombstone()).collect(Collectors.toList());
    final List<DataSegment> tombstonesAfterPartialCompaction =
        segmentsAfterPartialCompaction.stream().filter(s -> s.isTombstone()).collect(Collectors.toList());
    Assert.assertEquals(59, tombstonesAfterPartialCompaction.size());
    Assert.assertEquals(5, realSegmentsAfterPartialCompaction.size());
    Assert.assertEquals(64, segmentsAfterPartialCompaction.size());

    // Now setup compaction over an interval with only tombstones, keeping same, minute granularity
    final CompactionTask compactionTaskOverOnlyTombstones =
        compactionTaskBuilder(Granularities.MINUTE)
            // Set dropExisting to true
            // last 59 minutes of our 01, should be all tombstones
            .inputSpec(new CompactionIntervalSpec(Intervals.of("2014-01-01T01:01:00/2014-01-01T02:00:00"), null), true)
            .build();

    // **** Compaction over tombstones ****
    final Pair<TaskStatus, DataSegmentsWithSchemas> resultOverOnlyTombstones = runTask(compactionTaskOverOnlyTombstones);
    verifyTaskSuccessRowsAndSchemaMatch(resultOverOnlyTombstones, 0);

    // compaction should not fail but since it is over the same granularity it should leave
    // the tombstones unchanged
    Assert.assertEquals(59, resultOverOnlyTombstones.rhs.getSegments().size());
    resultOverOnlyTombstones.rhs.getSegments().forEach(t -> Assert.assertTrue(t.isTombstone()));
  }

  @Test
  public void testPartialIntervalCompactWithFinerSegmentGranularityThenFullIntervalCompactWithDropExistingFalse()
      throws Exception
  {
    // This test fails with segment lock because of the bug reported in https://github.com/apache/druid/issues/10911.
    Assume.assumeTrue(lockGranularity != LockGranularity.SEGMENT);
    Assume.assumeTrue(
        "test with defined segment granularity and interval in this test",
        Granularities.THREE_HOUR.equals(segmentGranularity) && TEST_INTERVAL.equals(inputInterval)
    );
    verifyTaskSuccessRowsAndSchemaMatch(runIndexTask(), TOTAL_TEST_ROWS);

    final Set<DataSegment> expectedSegments = new HashSet<>(
        coordinatorClient.fetchUsedSegments(DATA_SOURCE, List.of(Intervals.of("2014-01-01/2014-01-02"))).get());
    Assert.assertEquals(6, expectedSegments.size());

    final Interval partialInterval = Intervals.of("2014-01-01T01:00:00/2014-01-01T02:00:00");
    final CompactionTask partialCompactionTask = compactionTaskBuilder(Granularities.MINUTE)
        // Set dropExisting to false
        .inputSpec(new CompactionIntervalSpec(partialInterval, null), false)
        .build();

    final Pair<TaskStatus, DataSegmentsWithSchemas> partialCompactionResult = runTask(partialCompactionTask);
    verifyTaskSuccessRowsAndSchemaMatch(partialCompactionResult, 3);
    // All segments in the previous expectedSegments should still appear as they have larger segment granularity.
    expectedSegments.addAll(partialCompactionResult.rhs.getSegments());
    Assert.assertEquals(7, expectedSegments.size());

    final Set<DataSegment> segmentsAfterPartialCompaction = new HashSet<>(
        coordinatorClient.fetchUsedSegments(DATA_SOURCE, List.of(TEST_INTERVAL)).get());
    Assert.assertEquals(expectedSegments, segmentsAfterPartialCompaction);
    // the lower version of hour01 segment is visible, but the 3 rows is not because hour01minite00 is overshadowed by a higher version segment.
    Assert.assertEquals(13, expectedSegments.stream().mapToInt(DataSegment::getTotalRows).sum());

    final CompactionTask fullCompactionTask = compactionTaskBuilder(Granularities.HOUR)
        // Set dropExisting to false
        .inputSpec(new CompactionIntervalSpec(Intervals.of("2014-01-01/2014-01-02"), null), false)
        .build();

    final Pair<TaskStatus, DataSegmentsWithSchemas> fullCompactionResult = runTask(fullCompactionTask);
    verifyTaskSuccessRowsAndSchemaMatch(fullCompactionResult, TOTAL_TEST_ROWS);

    final List<DataSegment> segmentsAfterFullCompaction = new ArrayList<>(new HashSet<>(
        coordinatorClient.fetchUsedSegments(DATA_SOURCE, List.of(TEST_INTERVAL)).get()));
    segmentsAfterFullCompaction.sort(
        (s1, s2) -> Comparators.intervalsByStartThenEnd().compare(s1.getInterval(), s2.getInterval())
    );

    Assert.assertEquals(3, segmentsAfterFullCompaction.size());
    for (int i = 0; i < segmentsAfterFullCompaction.size(); i++) {
      Assert.assertEquals(
          Intervals.of(StringUtils.format("2014-01-01T%02d/2014-01-01T%02d", i, i + 1)),
          segmentsAfterFullCompaction.get(i).getInterval()
      );
    }
  }

  @Test
  public void testRunIndexAndCompactForSameSegmentAtTheSameTime() throws Exception
  {
    verifyTaskSuccessRowsAndSchemaMatch(runIndexTask(), TOTAL_TEST_ROWS);

    // make sure that indexTask becomes ready first, then compactionTask becomes ready, then indexTask runs
    final CountDownLatch compactionTaskReadyLatch = new CountDownLatch(1);
    final CountDownLatch indexTaskStartLatch = new CountDownLatch(1);
    final Future<Pair<TaskStatus, DataSegmentsWithSchemas>> indexFuture = exec.submit(
        () -> runIndexTask(compactionTaskReadyLatch, indexTaskStartLatch, false)
    );

    final CompactionTask compactionTask =
        compactionTaskBuilder(segmentGranularity).interval(inputInterval, true).build();

    final Future<Pair<TaskStatus, DataSegmentsWithSchemas>> compactionFuture = exec.submit(
        () -> {
          compactionTaskReadyLatch.await(5, TimeUnit.SECONDS);
          return runTask(compactionTask, indexTaskStartLatch, null);
        }
    );

    verifyTaskSuccessRowsAndSchemaMatch(indexFuture.get(), TOTAL_TEST_ROWS);
    List<DataSegment> segments = new ArrayList<>(indexFuture.get().rhs.getSegments());
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

    Exception e = Assert.assertThrows(Exception.class, () -> compactionFuture.get());
    Assert.assertTrue(e.getMessage().contains("not ready"));
  }

  @Test
  public void testRunIndexAndCompactForSameSegmentAtTheSameTime2() throws Exception
  {
    verifyTaskSuccessRowsAndSchemaMatch(runIndexTask(), TOTAL_TEST_ROWS);

    final CompactionTask compactionTask =
        compactionTaskBuilder(segmentGranularity).interval(inputInterval, true).build();

    // make sure that compactionTask becomes ready first, then the indexTask becomes ready, then compactionTask runs
    final CountDownLatch indexTaskReadyLatch = new CountDownLatch(1);
    final CountDownLatch compactionTaskStartLatch = new CountDownLatch(1);
    final Future<Pair<TaskStatus, DataSegmentsWithSchemas>> compactionFuture = exec.submit(
        () -> {
          final Pair<TaskStatus, DataSegmentsWithSchemas> pair = runTask(
              compactionTask,
              indexTaskReadyLatch,
              compactionTaskStartLatch
          );
          return pair;
        }
    );

    final Future<Pair<TaskStatus, DataSegmentsWithSchemas>> indexFuture = exec.submit(
        () -> {
          indexTaskReadyLatch.await(5, TimeUnit.SECONDS);
          return runIndexTask(compactionTaskStartLatch, null, false);
        }
    );

    verifyTaskSuccessRowsAndSchemaMatch(indexFuture.get(), TOTAL_TEST_ROWS);
    List<DataSegment> segments = new ArrayList<>(indexFuture.get().rhs.getSegments());
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

    final Pair<TaskStatus, DataSegmentsWithSchemas> compactionResult = compactionFuture.get();
    Assert.assertEquals(TaskState.FAILED, compactionResult.lhs.getStatusCode());
  }

  @Test
  public void testRunWithSpatialDimensions() throws Exception
  {
    Assume.assumeTrue(
        "test with defined segment granularity and interval in this test",
        Granularities.THREE_HOUR.equals(segmentGranularity) && TEST_INTERVAL.equals(inputInterval)
    );
    final List<String> spatialrows = ImmutableList.of(
        "2014-01-01T00:00:10Z,a,10,100,1\n",
        "2014-01-01T00:00:10Z,b,20,110,2\n",
        "2014-01-01T00:00:10Z,c,30,120,3\n",
        "2014-01-01T01:00:20Z,a,10,100,1\n",
        "2014-01-01T01:00:20Z,b,20,110,2\n",
        "2014-01-01T01:00:20Z,c,30,120,3\n"
    );
    final ParseSpec spatialSpec = new CSVParseSpec(
        new TimestampSpec("ts", "auto", null),
        DimensionsSpec.builder()
                      .setDimensions(Arrays.asList(
                          new StringDimensionSchema("ts"),
                          new StringDimensionSchema("dim"),
                          new NewSpatialDimensionSchema("spatial", Arrays.asList("x", "y"))
                      ))
                      .build(),
        "|",
        Arrays.asList("ts", "dim", "x", "y", "val"),
        false,
        0
    );
    Pair<TaskStatus, DataSegmentsWithSchemas> indexTaskResult = runTask(
        buildIndexTask(spatialSpec, spatialrows, Intervals.of("2014-01-01T00:00:00Z/2014-01-01T02:00:00Z"), false),
        null,
        null
    );
    verifyTaskSuccessRowsAndSchemaMatch(indexTaskResult, 6);

    final CompactionTask compactionTask =
        compactionTaskBuilder(Granularities.THREE_HOUR).interval(TEST_INTERVAL, true).build();

    final Pair<TaskStatus, DataSegmentsWithSchemas> resultPair = runTask(compactionTask);
    verifyTaskSuccessRowsAndSchemaMatch(resultPair, 6);

    final List<DataSegment> segments = new ArrayList<>(resultPair.rhs.getSegments());
    Assert.assertEquals(1, segments.size());

    Assert.assertEquals(TEST_INTERVAL, segments.get(0).getInterval());
    CompactionState defaultCompactionState = getDefaultCompactionState(
        Granularities.THREE_HOUR,
        Granularities.MINUTE,
        List.of(TEST_INTERVAL)
    );
    CompactionState newCompactionState =
        defaultCompactionState.toBuilder().dimensionsSpec(
            defaultCompactionState.getDimensionsSpec()
                                  .toBuilder()
                                  .setDimensions(Arrays.asList(
                                      new StringDimensionSchema("ts", DimensionSchema.MultiValueHandling.ARRAY, null),
                                      new StringDimensionSchema("dim", DimensionSchema.MultiValueHandling.ARRAY, null),
                                      new NewSpatialDimensionSchema("spatial", Collections.singletonList("spatial"))
                                  ))
                                  .build()).build();
    Assert.assertEquals(newCompactionState, segments.get(0).getLastCompactionState());
    Assert.assertEquals(new NumberedShardSpec(0, 1), segments.get(0).getShardSpec());

    final File cacheDir = temporaryFolder.newFolder();
    final SegmentCacheManager segmentCacheManager = segmentCacheManagerFactory.manufacturate(cacheDir, false);

    List<String> rowsFromSegment = new ArrayList<>();
    for (DataSegment segment : segments) {
      segmentCacheManager.load(segment);
      final File segmentFile = segmentCacheManager.getSegmentFiles(segment);

      final WindowedCursorFactory windowed = new WindowedCursorFactory(
          new QueryableIndexCursorFactory(testUtils.getTestIndexIO().loadIndex(segmentFile)),
          segment.getInterval()
      );
      try (final CursorHolder cursorHolder = windowed.getCursorFactory().makeCursorHolder(CursorBuildSpec.FULL_SCAN)) {
        final Cursor cursor = cursorHolder.asCursor();
        Assert.assertNotNull(cursor);
        cursor.reset();
        final ColumnSelectorFactory factory = cursor.getColumnSelectorFactory();
        Assert.assertTrue(factory.getColumnCapabilities("spatial").hasSpatialIndexes());
        while (!cursor.isDone()) {
          final ColumnValueSelector<?> selector1 = factory.makeColumnValueSelector("ts");
          final DimensionSelector selector2 = factory.makeDimensionSelector(new DefaultDimensionSpec("dim", "dim"));
          final DimensionSelector selector3 = factory.makeDimensionSelector(new DefaultDimensionSpec(
              "spatial",
              "spatial"
          ));
          final DimensionSelector selector4 = factory.makeDimensionSelector(new DefaultDimensionSpec("val", "val"));

          rowsFromSegment.add(StringUtils.format(
              "%s,%s,%s,%s\n",
              selector1.getObject(),
              selector2.getObject(),
              selector3.getObject(),
              selector4.getObject()
          ));

          cursor.advance();
        }
      }
    }
    Assert.assertEquals(spatialrows, rowsFromSegment);
  }

  @Test
  public void testRunWithAutoCastDimensions() throws Exception
  {
    Assume.assumeTrue(
        "test with defined segment granularity and interval in this test",
        Granularities.THREE_HOUR.equals(segmentGranularity) && TEST_INTERVAL.equals(inputInterval)
    );
    final List<String> rows = ImmutableList.of(
        "2014-01-01T00:00:10Z,a,10,100,1\n",
        "2014-01-01T00:00:10Z,b,20,110,2\n",
        "2014-01-01T00:00:10Z,c,30,120,3\n",
        "2014-01-01T01:00:20Z,a,10,100,1\n",
        "2014-01-01T01:00:20Z,b,20,110,2\n",
        "2014-01-01T01:00:20Z,c,30,120,3\n"
    );
    final ParseSpec spec = new CSVParseSpec(
        new TimestampSpec("ts", "auto", null),
        DimensionsSpec.builder()
                      .setDimensions(Arrays.asList(
                          new AutoTypeColumnSchema("ts", ColumnType.STRING, null),
                          AutoTypeColumnSchema.of("dim"),
                          new AutoTypeColumnSchema("x", ColumnType.LONG, null),
                          new AutoTypeColumnSchema("y", ColumnType.LONG, null)
                      ))
                      .build(),
        "|",
        Arrays.asList("ts", "dim", "x", "y", "val"),
        false,
        0
    );
    Pair<TaskStatus, DataSegmentsWithSchemas> indexTaskResult = runTask(buildIndexTask(
        spec,
        rows,
        Intervals.of("2014-01-01T00:00:00Z/2014-01-01T02:00:00Z"),
        false
    ), null, null);
    verifyTaskSuccessRowsAndSchemaMatch(indexTaskResult, 6);

    final CompactionTask compactionTask =
        compactionTaskBuilder(Granularities.THREE_HOUR).interval(TEST_INTERVAL, true).build();

    final Pair<TaskStatus, DataSegmentsWithSchemas> resultPair = runTask(compactionTask);
    verifyTaskSuccessRowsAndSchemaMatch(resultPair, 6);

    final List<DataSegment> segments = new ArrayList<>(resultPair.rhs.getSegments());
    Assert.assertEquals(1, segments.size());
    Assert.assertEquals(TEST_INTERVAL, segments.get(0).getInterval());

    final List<String> dimensionExclusions =
        compactionTask.getCompactionRunner() instanceof NativeCompactionRunner ? List.of() : List.of("__time", "val");
    CompactionState expectedState =
        getDefaultCompactionState(
            Granularities.THREE_HOUR,
            Granularities.MINUTE,
            List.of(TEST_INTERVAL)
        ).toBuilder()
         .dimensionsSpec(
             new DimensionsSpec(Arrays.asList(/* check explicitly specified types are preserved */
                 new AutoTypeColumnSchema("ts", ColumnType.STRING, DEFAULT_NESTED_SPEC),
                 new AutoTypeColumnSchema("dim", null, DEFAULT_NESTED_SPEC),
                 new AutoTypeColumnSchema("x", ColumnType.LONG, DEFAULT_NESTED_SPEC),
                 new AutoTypeColumnSchema("y", ColumnType.LONG, DEFAULT_NESTED_SPEC)
             )).toBuilder().setDimensionExclusions(dimensionExclusions).build())
         .build();
    Assert.assertEquals(expectedState, segments.get(0).getLastCompactionState());
    Assert.assertEquals(new NumberedShardSpec(0, 1), segments.get(0).getShardSpec());

    final File cacheDir = temporaryFolder.newFolder();
    final SegmentCacheManager segmentCacheManager = segmentCacheManagerFactory.manufacturate(cacheDir, false);

    List<String> rowsFromSegment = new ArrayList<>();
    for (DataSegment segment : segments) {
      segmentCacheManager.load(segment);
      final File segmentFile = segmentCacheManager.getSegmentFiles(segment);

      final WindowedCursorFactory windowed = new WindowedCursorFactory(
          new QueryableIndexCursorFactory(testUtils.getTestIndexIO().loadIndex(segmentFile)),
          segment.getInterval()
      );
      try (final CursorHolder cursorHolder = windowed.getCursorFactory().makeCursorHolder(CursorBuildSpec.FULL_SCAN)) {
        final Cursor cursor = cursorHolder.asCursor();
        Assert.assertNotNull(cursor);
        cursor.reset();
        final ColumnSelectorFactory factory = cursor.getColumnSelectorFactory();
        Assert.assertEquals(ColumnType.STRING, factory.getColumnCapabilities("ts").toColumnType());
        Assert.assertEquals(ColumnType.STRING, factory.getColumnCapabilities("dim").toColumnType());
        Assert.assertEquals(ColumnType.LONG, factory.getColumnCapabilities("x").toColumnType());
        Assert.assertEquals(ColumnType.LONG, factory.getColumnCapabilities("y").toColumnType());
        while (!cursor.isDone()) {
          final ColumnValueSelector<?> selector1 = factory.makeColumnValueSelector("ts");
          final DimensionSelector selector2 = factory.makeDimensionSelector(new DefaultDimensionSpec("dim", "dim"));
          final DimensionSelector selector3 = factory.makeDimensionSelector(new DefaultDimensionSpec("x", "x"));
          final DimensionSelector selector4 = factory.makeDimensionSelector(new DefaultDimensionSpec("y", "y"));
          final DimensionSelector selector5 = factory.makeDimensionSelector(new DefaultDimensionSpec("val", "val"));

          rowsFromSegment.add(
              StringUtils.format(
                  "%s,%s,%s,%s,%s\n",
                  selector1.getObject(),
                  selector2.getObject(),
                  selector3.getObject(),
                  selector4.getObject(),
                  selector5.getObject()
              )
          );

          cursor.advance();
        }
      }
    }
    Assert.assertEquals(rows, rowsFromSegment);
  }

  @Test
  public void testRunWithAutoCastDimensionsSortByDimension() throws Exception
  {
    Assume.assumeTrue(
        "test with defined segment granularity and interval in this test",
        Granularities.THREE_HOUR.equals(segmentGranularity) && TEST_INTERVAL.equals(inputInterval)
    );
    // Compaction will produce one segment sorted by [x, __time], even though input rows are sorted by __time.
    final List<String> rows = ImmutableList.of(
        "2014-01-01T00:00:10Z,a,10,100,1\n",
        "2014-01-01T00:00:10Z,b,20,110,2\n",
        "2014-01-01T00:00:10Z,c,30,120,3\n",
        "2014-01-01T00:01:20Z,a,10,100,1\n",
        "2014-01-01T00:01:20Z,b,20,110,2\n",
        "2014-01-01T00:01:20Z,c,30,120,3\n"
    );
    final ParseSpec spec = new CSVParseSpec(
        new TimestampSpec("ts", "auto", null),
        DimensionsSpec.builder()
                      .setDimensions(Arrays.asList(
                          new AutoTypeColumnSchema("x", ColumnType.LONG, null),
                          new LongDimensionSchema("__time"),
                          new AutoTypeColumnSchema("ts", ColumnType.STRING, null),
                          AutoTypeColumnSchema.of("dim"),
                          new AutoTypeColumnSchema("y", ColumnType.LONG, null)
                      ))
                      .setForceSegmentSortByTime(false)
                      .build(),
        "|",
        Arrays.asList("ts", "dim", "x", "y", "val"),
        false,
        0
    );
    Pair<TaskStatus, DataSegmentsWithSchemas> indexTaskResult = runTask(buildIndexTask(
        spec,
        rows,
        Intervals.of("2014-01-01T00:00:00Z/2014-01-01T02:00:00Z"),
        false
    ), null, null);
    verifyTaskSuccessRowsAndSchemaMatch(indexTaskResult, 6);

    Interval interval = Intervals.of("2014-01-01T00:00:00/2014-01-01T01:00:00");
    final CompactionTask compactionTask = compactionTaskBuilder(Granularities.HOUR).interval(interval, true).build();

    final Pair<TaskStatus, DataSegmentsWithSchemas> resultPair = runTask(compactionTask);
    verifyTaskSuccessRowsAndSchemaMatch(resultPair, 6);

    final List<DataSegment> segments = new ArrayList<>(resultPair.rhs.getSegments());
    Assert.assertEquals(1, segments.size());

    final DataSegment compactSegment = Iterables.getOnlyElement(segments);
    Assert.assertEquals(interval, compactSegment.getInterval());
    final List<String> dimensionExclusions =
        compactionTask.getCompactionRunner() instanceof NativeCompactionRunner ? List.of() : List.of("val");
    CompactionState expectedState =
        getDefaultCompactionState(
            Granularities.HOUR,
            Granularities.MINUTE,
            List.of(interval)
        ).toBuilder()
         .dimensionsSpec(
             new DimensionsSpec(Arrays.asList(/* check explicitly that time ordering is preserved */
                 new AutoTypeColumnSchema("x", ColumnType.LONG, DEFAULT_NESTED_SPEC),
                 new LongDimensionSchema("__time"),
                 new AutoTypeColumnSchema("ts", ColumnType.STRING, DEFAULT_NESTED_SPEC),
                 new AutoTypeColumnSchema("dim", null, DEFAULT_NESTED_SPEC),
                 new AutoTypeColumnSchema("y", ColumnType.LONG, DEFAULT_NESTED_SPEC)
             )).toBuilder().setDimensionExclusions(dimensionExclusions).setForceSegmentSortByTime(false).build())
         .build();
    Assert.assertEquals(expectedState, compactSegment.getLastCompactionState());
    Assert.assertEquals(new NumberedShardSpec(0, 1), compactSegment.getShardSpec());

    final File cacheDir = temporaryFolder.newFolder();
    final SegmentCacheManager segmentCacheManager = segmentCacheManagerFactory.manufacturate(cacheDir, false);

    List<String> rowsFromSegment = new ArrayList<>();
    segmentCacheManager.load(compactSegment);
    final File segmentFile = segmentCacheManager.getSegmentFiles(compactSegment);

    final QueryableIndex queryableIndex = testUtils.getTestIndexIO().loadIndex(segmentFile);
    final WindowedCursorFactory windowed = new WindowedCursorFactory(
        new QueryableIndexCursorFactory(queryableIndex),
        compactSegment.getInterval()
    );
    Assert.assertEquals(
        ImmutableList.of(
            OrderBy.ascending("x"),
            OrderBy.ascending("__time"),
            OrderBy.ascending("ts"),
            OrderBy.ascending("dim"),
            OrderBy.ascending("y")
        ),
        queryableIndex.getOrdering()
    );

    try (final CursorHolder cursorHolder =
             windowed.getCursorFactory()
                     .makeCursorHolder(CursorBuildSpec.builder().setInterval(compactSegment.getInterval()).build())) {
      final Cursor cursor = cursorHolder.asCursor();
      cursor.reset();
      final ColumnSelectorFactory factory = cursor.getColumnSelectorFactory();
      Assert.assertEquals(ColumnType.STRING, factory.getColumnCapabilities("ts").toColumnType());
      Assert.assertEquals(ColumnType.STRING, factory.getColumnCapabilities("dim").toColumnType());
      Assert.assertEquals(ColumnType.LONG, factory.getColumnCapabilities("x").toColumnType());
      Assert.assertEquals(ColumnType.LONG, factory.getColumnCapabilities("y").toColumnType());
      while (!cursor.isDone()) {
        final ColumnValueSelector<?> selector1 = factory.makeColumnValueSelector("ts");
        final DimensionSelector selector2 = factory.makeDimensionSelector(new DefaultDimensionSpec("dim", "dim"));
        final DimensionSelector selector3 = factory.makeDimensionSelector(new DefaultDimensionSpec("x", "x"));
        final DimensionSelector selector4 = factory.makeDimensionSelector(new DefaultDimensionSpec("y", "y"));
        final DimensionSelector selector5 = factory.makeDimensionSelector(new DefaultDimensionSpec("val", "val"));

        rowsFromSegment.add(
            StringUtils.format(
                "%s,%s,%s,%s,%s",
                selector1.getObject(),
                selector2.getObject(),
                selector3.getObject(),
                selector4.getObject(),
                selector5.getObject()
            )
        );

        cursor.advance();
      }
    }

    Assert.assertEquals(
        ImmutableList.of(
            "2014-01-01T00:00:10Z,a,10,100,1",
            "2014-01-01T00:01:20Z,a,10,100,1",
            "2014-01-01T00:00:10Z,b,20,110,2",
            "2014-01-01T00:01:20Z,b,20,110,2",
            "2014-01-01T00:00:10Z,c,30,120,3",
            "2014-01-01T00:01:20Z,c,30,120,3"
        ),
        rowsFromSegment
    );
  }

  protected Pair<TaskStatus, DataSegmentsWithSchemas> runIndexTask() throws Exception
  {
    return runIndexTask(null, null, false);
  }

  protected Pair<TaskStatus, DataSegmentsWithSchemas> runAppendTask() throws Exception
  {
    return runIndexTask(null, null, true);
  }

  protected Pair<TaskStatus, DataSegmentsWithSchemas> runIndexTask(
      @Nullable CountDownLatch readyLatchToCountDown,
      @Nullable CountDownLatch latchToAwaitBeforeRun,
      boolean appendToExisting
  ) throws Exception
  {
    return runTask(
        buildIndexTask(DEFAULT_PARSE_SPEC, TEST_ROWS, TEST_INTERVAL, appendToExisting),
        readyLatchToCountDown,
        latchToAwaitBeforeRun
    );
  }

  protected IndexTask buildIndexTask(
      ParseSpec parseSpec,
      List<String> rows,
      Interval interval,
      boolean appendToExisting
  ) throws Exception
  {
    File tmpDir = temporaryFolder.newFolder();
    File tmpFile = File.createTempFile("druid", "index", tmpDir);

    try (BufferedWriter writer = Files.newWriter(tmpFile, StandardCharsets.UTF_8)) {
      for (String testRow : rows) {
        writer.write(testRow);
      }
    }
    return new IndexTask(
        null,
        null,
        IndexTaskTest.createIngestionSpec(
            objectMapper,
            tmpDir,
            parseSpec,
            null,
            new UniformGranularitySpec(DEFAULT_SEGMENT_GRAN, DEFAULT_QUERY_GRAN, List.of(interval)),
            IndexTaskTest.createTuningConfig(2, 2, 2L, null, false, true),
            appendToExisting,
            false
        ),
        Map.of("useConcurrentLocks", useConcurrentLocks)
    );
  }

  protected Pair<TaskStatus, DataSegmentsWithSchemas> runTask(Task task) throws Exception
  {
    return runTask(task, null, null);
  }

  public void registerTaskActionClient(String taskId, TaskActionClient taskActionClient)
  {
  }

  protected Pair<TaskStatus, DataSegmentsWithSchemas> runTask(
      Task task,
      @Nullable CountDownLatch readyLatchToCountDown,
      @Nullable CountDownLatch latchToAwaitBeforeRun
  ) throws Exception
  {
    taskActionTestKit.getTaskLockbox().add(task);
    taskActionTestKit.getTaskStorage().insert(task, TaskStatus.running(task.getId()));

    TestSpyTaskActionClient spyTaskActionClient =
        new TestSpyTaskActionClient(new LocalTaskActionClient(task, taskActionTestKit.getTaskActionToolbox()));
    registerTaskActionClient(task.getId(), spyTaskActionClient);
    final TaskToolbox box = createTaskToolbox(objectMapper, spyTaskActionClient);


    task.addToContext(Tasks.FORCE_TIME_CHUNK_LOCK_KEY, lockGranularity == LockGranularity.TIME_CHUNK);
    if (task.isReady(box.getTaskActionClient())) {
      if (readyLatchToCountDown != null) {
        readyLatchToCountDown.countDown();
      }
      if (latchToAwaitBeforeRun != null) {
        latchToAwaitBeforeRun.await();
      }
      TaskStatus status = task.run(box);
      taskActionTestKit.getTaskLockbox().remove(task);
      return Pair.of(
          status,
          new DataSegmentsWithSchemas(
              new TreeSet<>(spyTaskActionClient.getPublishedSegments()),
              spyTaskActionClient.getSegmentSchemas()
          )
      );
    } else {
      if (readyLatchToCountDown != null) {
        readyLatchToCountDown.countDown();
      }
      throw new ISE("task[%s] is not ready", task.getId());
    }
  }

  protected Builder compactionTaskBuilder(Granularity segmentGranularity1)
  {
    return compactionTaskBuilder(segmentGranularity1 == null
                                 ? null
                                 : new ClientCompactionTaskGranularitySpec(segmentGranularity1, null, null));
  }

  protected abstract Builder compactionTaskBuilder(ClientCompactionTaskGranularitySpec granularitySpec);

  private TaskToolbox createTaskToolbox(ObjectMapper objectMapper, TaskActionClient taskActionClient) throws IOException
  {
    final SegmentLoaderConfig loaderConfig = new SegmentLoaderConfig()
    {
      @Override
      public List<StorageLocationConfig> getLocations()
      {
        return ImmutableList.of(new StorageLocationConfig(localDeepStorage, null, null));
      }

      @Override
      public boolean isVirtualStorage()
      {
        return true;
      }

      @Override
      public boolean isVirtualStorageEphemeral()
      {
        return true;
      }
    };
    final List<StorageLocation> storageLocations = loaderConfig.toStorageLocations();
    final SegmentCacheManager cacheManager = new SegmentLocalCacheManager(
        storageLocations,
        loaderConfig,
        new LeastBytesUsedStorageLocationSelectorStrategy(storageLocations),
        TestIndex.INDEX_IO,
        objectMapper
    );

    return new TaskToolbox.Builder()
        .config(new TaskConfigBuilder().build())
        .emitter(NoopServiceEmitter.instance())
        .taskActionClient(taskActionClient)
        .segmentPusher(new LocalDataSegmentPusher(new LocalDataSegmentPusherConfig()))
        .dataSegmentKiller(new NoopDataSegmentKiller())
        .joinableFactory(NoopJoinableFactory.INSTANCE)
        .segmentCacheManager(cacheManager)
        .jsonMapper(objectMapper)
        .taskWorkDir(temporaryFolder.newFolder())
        .indexIO(testUtils.getTestIndexIO())
        .handoffNotifierFactory(new NoopSegmentHandoffNotifierFactory())
        .indexMerger(testUtils.getIndexMergerV9Factory().create(true))
        .taskReportFileWriter(new SingleFileTaskReportFileWriter(reportsFile))
        .authorizerMapper(AuthTestUtils.TEST_AUTHORIZER_MAPPER)
        .chatHandlerProvider(new NoopChatHandlerProvider())
        .rowIngestionMetersFactory(testUtils.getRowIngestionMetersFactory())
        .appenderatorsManager(new TestAppenderatorsManager())
        .overlordClient(overlordClient)
        .coordinatorClient(coordinatorClient)
        .taskLogPusher(null)
        .attemptId("1")
        .centralizedTableSchemaConfig(CentralizedDatasourceSchemaConfig.enabled(useCentralizedDatasourceSchema))
        .build();
  }

  protected List<String> getCSVFormatRowsFromSegments(List<DataSegment> segments) throws Exception
  {
    final File cacheDir = temporaryFolder.newFolder();
    final SegmentCacheManager segmentCacheManager = segmentCacheManagerFactory.manufacturate(cacheDir, false);

    List<String> rowsFromSegment = new ArrayList<>();
    for (DataSegment segment : segments) {
      segmentCacheManager.load(segment);
      final File segmentFile = segmentCacheManager.getSegmentFiles(segment);

      final WindowedCursorFactory windowed = new WindowedCursorFactory(
          new QueryableIndexCursorFactory(testUtils.getTestIndexIO().loadIndex(segmentFile)),
          segment.getInterval()
      );
      try (final CursorHolder cursorHolder = windowed.getCursorFactory().makeCursorHolder(CursorBuildSpec.FULL_SCAN)) {
        final Cursor cursor = cursorHolder.asCursor();
        Assert.assertNotNull(cursor);
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
    }

    return rowsFromSegment;
  }

  public void verifyTaskSuccessRowsAndSchemaMatch(Pair<TaskStatus, DataSegmentsWithSchemas> resultPair, int totalRows)
  {
    Assert.assertTrue(resultPair.lhs.isSuccess());
    DataSegmentsWithSchemas dataSegmentsWithSchemas = resultPair.rhs;
    if (useCentralizedDatasourceSchema) {
      verifySchema(dataSegmentsWithSchemas.getSegments(), dataSegmentsWithSchemas.getSegmentSchemaMapping());
    }
    Assert.assertEquals(
        totalRows,
        dataSegmentsWithSchemas.getSegments().stream().mapToInt(DataSegment::getTotalRows).sum()
    );
  }

  protected void verifyCompactedSegment(
      List<DataSegment> segments,
      Granularity gran,
      Granularity queryGran,
      boolean useOverwriteShard
  )
  {
    if (gran == null || gran.equals(Granularities.HOUR)) {
      Assert.assertEquals(3, segments.size());

      for (int i = 0; i < 3; i++) {
        Interval interval = Intervals.of("2014-01-01T0%d:00:00/2014-01-01T0%d:00:00", i, i + 1);
        Assert.assertEquals(interval, segments.get(i).getInterval());
        Interval inputInterval = gran == null ? interval : this.inputInterval;
        Assert.assertEquals(
            getDefaultCompactionState(DEFAULT_SEGMENT_GRAN, queryGran, List.of(inputInterval)),
            segments.get(i).getLastCompactionState()
        );
        if (useOverwriteShard) {
          Assert.assertEquals(
              new NumberedOverwriteShardSpec(PartitionIds.NON_ROOT_GEN_START_PARTITION_ID, 0, 2, (short) 1, (short) 1),
              segments.get(i).getShardSpec()
          );
        } else {
          Assert.assertEquals(new NumberedShardSpec(0, 1), segments.get(i).getShardSpec());
        }
      }
    } else if (gran.equals(Granularities.THREE_HOUR)) {
      Assert.assertEquals(1, segments.size());
      Assert.assertEquals(TEST_INTERVAL, segments.get(0).getInterval());
      Assert.assertEquals(
          getDefaultCompactionState(gran, queryGran, List.of(inputInterval)),
          segments.get(0).getLastCompactionState()
      );
      Assert.assertEquals(new NumberedShardSpec(0, 1), segments.get(0).getShardSpec());
    } else {
      throw new RE("Gran[%s] is not supported", gran);
    }
  }

  protected CompactionState getDefaultCompactionState(
      Granularity segmentGranularity,
      Granularity queryGranularity,
      List<Interval> intervals
  )
  {
    return getDefaultCompactionState(
        segmentGranularity,
        queryGranularity,
        intervals,
        DEFAULT_DIMENSIONS_SPEC,
        List.of(DEFAULT_AGGREGATOR)
    );
  }

  protected CompactionState getDefaultCompactionState(
      Granularity segmentGranularity,
      Granularity queryGranularity,
      List<Interval> intervals,
      DimensionsSpec expectedDims,
      List<AggregatorFactory> expectedMetrics
  )
  {
    // Expected compaction state to exist after compaction as we store compaction state by default
    return new CompactionState(
        new DynamicPartitionsSpec(5000000, Long.MAX_VALUE),
        expectedDims,
        expectedMetrics,
        null,
        IndexSpec.getDefault().getEffectiveSpec(),
        new UniformGranularitySpec(
            segmentGranularity,
            queryGranularity,
            true,
            intervals
        ),
        null
    );
  }

  private static void verifySchema(Set<DataSegment> segments, SegmentSchemaMapping segmentSchemaMapping)
  {
    int nonTombstoneSegments = 0;
    for (DataSegment segment : segments) {
      if (segment.isTombstone()) {
        continue;
      }
      nonTombstoneSegments++;
      Assert.assertTrue(segmentSchemaMapping.getSegmentIdToMetadataMap().containsKey(segment.getId().toString()));
    }
    Assert.assertEquals(nonTombstoneSegments, segmentSchemaMapping.getSegmentIdToMetadataMap().size());
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
