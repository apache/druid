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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.client.coordinator.NoopCoordinatorClient;
import org.apache.druid.client.indexing.ClientCompactionTaskGranularitySpec;
import org.apache.druid.client.indexing.ClientCompactionTaskTransformSpec;
import org.apache.druid.client.indexing.NoopOverlordClient;
import org.apache.druid.data.input.impl.CSVParseSpec;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.NewSpatialDimensionSchema;
import org.apache.druid.data.input.impl.ParseSpec;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.RetryPolicyConfig;
import org.apache.druid.indexing.common.RetryPolicyFactory;
import org.apache.druid.indexing.common.SegmentCacheManagerFactory;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.config.TaskConfigBuilder;
import org.apache.druid.indexing.common.task.CompactionTask.Builder;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexTuningConfig;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.segment.AutoTypeColumnSchema;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.segment.join.NoopJoinableFactory;
import org.apache.druid.segment.loading.LocalDataSegmentPuller;
import org.apache.druid.segment.loading.LocalDataSegmentPusher;
import org.apache.druid.segment.loading.LocalDataSegmentPusherConfig;
import org.apache.druid.segment.loading.LocalLoadSpec;
import org.apache.druid.segment.loading.NoopDataSegmentKiller;
import org.apache.druid.segment.loading.SegmentCacheManager;
import org.apache.druid.segment.loading.SegmentLoaderConfig;
import org.apache.druid.segment.loading.SegmentLocalCacheManager;
import org.apache.druid.segment.loading.StorageLocationConfig;
import org.apache.druid.segment.loading.TombstoneLoadSpec;
import org.apache.druid.segment.realtime.firehose.NoopChatHandlerProvider;
import org.apache.druid.segment.realtime.firehose.WindowedStorageAdapter;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.timeline.CompactionState;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.HashBasedNumberedShardSpec;
import org.apache.druid.timeline.partition.NumberedOverwriteShardSpec;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.timeline.partition.PartitionIds;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableMessageMatcher;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

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
  private final OverlordClient overlordClient;
  private final CoordinatorClient coordinatorClient;
  private final SegmentCacheManagerFactory segmentCacheManagerFactory;
  private final LockGranularity lockGranularity;
  private final TestUtils testUtils;

  private ExecutorService exec;
  private File localDeepStorage;

  public CompactionTaskRunTest(LockGranularity lockGranularity)
  {
    testUtils = new TestUtils();
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
            ImmutableList.copyOf(
                getStorageCoordinator().retrieveUsedSegmentsForIntervals(dataSource, intervals, Segments.ONLY_VISIBLE)
            )
        );
      }
    };
    segmentCacheManagerFactory = new SegmentCacheManagerFactory(getObjectMapper());
    this.lockGranularity = lockGranularity;
  }

  public static CompactionState getDefaultCompactionState(
      Granularity segmentGranularity,
      Granularity queryGranularity,
      List<Interval> intervals
  ) throws JsonProcessingException
  {
    Map<String, String> expectedLongSumMetric = new HashMap<>();
    expectedLongSumMetric.put("type", "longSum");
    expectedLongSumMetric.put("name", "val");
    expectedLongSumMetric.put("fieldName", "val");
    return getDefaultCompactionState(
        segmentGranularity,
        queryGranularity,
        intervals,
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("ts", "dim"))),
        expectedLongSumMetric
    );
  }

  public static CompactionState getDefaultCompactionState(
      Granularity segmentGranularity,
      Granularity queryGranularity,
      List<Interval> intervals,
      DimensionsSpec expectedDims,
      Map<String, String> expectedMetric
  ) throws JsonProcessingException
  {
    ObjectMapper mapper = new DefaultObjectMapper();
    // Expected compaction state to exist after compaction as we store compaction state by default
    return new CompactionState(
        new DynamicPartitionsSpec(5000000, Long.MAX_VALUE),
        expectedDims,
        ImmutableList.of(expectedMetric),
        null,
        IndexSpec.DEFAULT.asMap(mapper),
        mapper.readValue(
            mapper.writeValueAsString(
                new UniformGranularitySpec(
                    segmentGranularity,
                    queryGranularity,
                    true,
                    intervals
                )
            ),
            Map.class
        )
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
        segmentCacheManagerFactory,
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
      Assert.assertEquals(
          getDefaultCompactionState(
              Granularities.HOUR,
              Granularities.MINUTE,
              ImmutableList.of(Intervals.of("2014-01-01T0%d:00:00/2014-01-01T0%d:00:00",
                                            i,
                                            i + 1
              ))
          ),
          segments.get(i).getLastCompactionState()
      );
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
        segmentCacheManagerFactory,
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

    for (int i = 0; i < 3; i++) {
      final Interval interval = Intervals.of("2014-01-01T0%d:00:00/2014-01-01T0%d:00:00", i, i + 1);
      for (int j = 0; j < 2; j++) {
        final int segmentIdx = i * 2 + j;
        Assert.assertEquals(
            interval,
            segments.get(segmentIdx).getInterval()
        );
        Map<String, String> expectedLongSumMetric = new HashMap<>();
        expectedLongSumMetric.put("type", "longSum");
        expectedLongSumMetric.put("name", "val");
        expectedLongSumMetric.put("fieldName", "val");
        CompactionState expectedState = new CompactionState(
            new HashedPartitionsSpec(null, 3, null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("ts", "dim"))),
            ImmutableList.of(expectedLongSumMetric),
            null,
            compactionTask.getTuningConfig().getIndexSpec().asMap(getObjectMapper()),
            getObjectMapper().readValue(
                getObjectMapper().writeValueAsString(
                    new UniformGranularitySpec(
                        Granularities.HOUR,
                        Granularities.MINUTE,
                        true,
                        ImmutableList.of(Intervals.of("2014-01-01T0%d:00:00/2014-01-01T0%d:00:00", i, i + 1))
                    )
                ),
                Map.class
            )
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
        segmentCacheManagerFactory,
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
      Assert.assertEquals(
          getDefaultCompactionState(
              Granularities.HOUR,
              Granularities.MINUTE,
              ImmutableList.of(Intervals.of("2014-01-01T0%d:00:00/2014-01-01T0%d:00:00",
                                            i,
                                            i + 1
              ))
          ),
          segments.get(i).getLastCompactionState()
      );
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
      Assert.assertEquals(
          getDefaultCompactionState(
              Granularities.HOUR,
              Granularities.MINUTE,
              ImmutableList.of(Intervals.of("2014-01-01T0%d:00:00/2014-01-01T0%d:00:00",
                                            i,
                                            i + 1
              ))
          ),
          segments.get(i).getLastCompactionState()
      );
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
        segmentCacheManagerFactory,
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
            false,
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

    Assert.assertTrue(compactionFuture.get().lhs.isSuccess());

    segments = compactionFuture.get().rhs;
    Assert.assertEquals(3, segments.size());

    for (int i = 0; i < 3; i++) {
      Assert.assertEquals(
          Intervals.of("2014-01-01T0%d:00:00/2014-01-01T0%d:00:00", i, i + 1),
          segments.get(i).getInterval()
      );
      Assert.assertEquals(
          getDefaultCompactionState(
              Granularities.HOUR,
              Granularities.MINUTE,
              ImmutableList.of(Intervals.of("2014-01-01T0%d:00:00/2014-01-01T0%d:00:00",
                                            i,
                                            i + 1
              ))
          ),
          segments.get(i).getLastCompactionState()
      );
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
        segmentCacheManagerFactory,
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
    Assert.assertEquals(
        getDefaultCompactionState(
            Granularities.DAY,
            Granularities.MINUTE,
            ImmutableList.of(Intervals.of("2014-01-01T00:00:00/2014-01-01T03:00:00"))
        ),
        segments.get(0).getLastCompactionState()
    );

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
      Assert.assertEquals(
          Intervals.of("2014-01-01T0%d:00:00/2014-01-01T0%d:00:00", i, i + 1),
          segments.get(i).getInterval()
      );
      Assert.assertEquals(new NumberedShardSpec(0, 1), segments.get(i).getShardSpec());
      Assert.assertEquals(
          getDefaultCompactionState(
              Granularities.HOUR,
              Granularities.MINUTE,
              ImmutableList.of(Intervals.of("2014-01-01/2014-01-02"))
          ),
          segments.get(i).getLastCompactionState()
      );
    }
  }

  @Test
  public void testWithSegmentGranularityMisalignedInterval() throws Exception
  {
    runIndexTask();

    final Builder builder = new Builder(
        DATA_SOURCE,
        segmentCacheManagerFactory,
        RETRY_POLICY_FACTORY
    );

    final CompactionTask compactionTask1 = builder
        .ioConfig(
            new CompactionIOConfig(
                new CompactionIntervalSpec(Intervals.of("2014-01-01/2014-01-02"), null),
                false,
                null
            )
        )
        .segmentGranularity(Granularities.WEEK)
        .build();

    final IllegalArgumentException e = Assert.assertThrows(
        IllegalArgumentException.class,
        () -> runTask(compactionTask1)
    );

    MatcherAssert.assertThat(
        e,
        ThrowableMessageMatcher.hasMessage(CoreMatchers.startsWith(
            "Interval[2014-01-01T00:00:00.000Z/2014-01-02T00:00:00.000Z] to compact is not aligned with segmentGranularity"))
    );
  }

  @Test
  public void testWithSegmentGranularityMisalignedIntervalAllowed() throws Exception
  {
    runIndexTask();

    final Builder builder = new Builder(
        DATA_SOURCE,
        segmentCacheManagerFactory,
        RETRY_POLICY_FACTORY
    );

    // day segmentGranularity
    final CompactionTask compactionTask1 = builder
        .ioConfig(
            new CompactionIOConfig(
                new CompactionIntervalSpec(Intervals.of("2014-01-01/2014-01-02"), null),
                true,
                null
            )
        )
        .segmentGranularity(Granularities.WEEK)
        .build();

    Pair<TaskStatus, List<DataSegment>> resultPair = runTask(compactionTask1);

    Assert.assertTrue(resultPair.lhs.isSuccess());

    List<DataSegment> segments = resultPair.rhs;

    Assert.assertEquals(1, segments.size());

    Assert.assertEquals(Intervals.of("2013-12-30/2014-01-06"), segments.get(0).getInterval());
    Assert.assertEquals(new NumberedShardSpec(0, 1), segments.get(0).getShardSpec());
    Assert.assertEquals(
        getDefaultCompactionState(
            Granularities.WEEK,
            Granularities.MINUTE,
            ImmutableList.of(Intervals.of("2014-01-01/2014-01-01T03"))
        ),
        segments.get(0).getLastCompactionState()
    );
  }

  @Test
  public void testCompactionWithFilterInTransformSpec() throws Exception
  {
    runIndexTask();

    final Builder builder = new Builder(
        DATA_SOURCE,
        segmentCacheManagerFactory,
        RETRY_POLICY_FACTORY
    );

    // day segmentGranularity
    final CompactionTask compactionTask = builder
        .interval(Intervals.of("2014-01-01/2014-01-02"))
        .granularitySpec(new ClientCompactionTaskGranularitySpec(Granularities.DAY, null, null))
        .transformSpec(new ClientCompactionTaskTransformSpec(new SelectorDimFilter("dim", "a", null)))
        .build();

    Pair<TaskStatus, List<DataSegment>> resultPair = runTask(compactionTask);

    Assert.assertTrue(resultPair.lhs.isSuccess());

    List<DataSegment> segments = resultPair.rhs;

    Assert.assertEquals(1, segments.size());

    Assert.assertEquals(Intervals.of("2014-01-01/2014-01-02"), segments.get(0).getInterval());
    Assert.assertEquals(new NumberedShardSpec(0, 1), segments.get(0).getShardSpec());

    ObjectMapper mapper = new DefaultObjectMapper();
    Map<String, String> expectedLongSumMetric = new HashMap<>();
    expectedLongSumMetric.put("type", "longSum");
    expectedLongSumMetric.put("name", "val");
    expectedLongSumMetric.put("fieldName", "val");
    CompactionState expectedCompactionState = new CompactionState(
        new DynamicPartitionsSpec(5000000, Long.MAX_VALUE),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("ts", "dim"))),
        ImmutableList.of(expectedLongSumMetric),
        getObjectMapper().readValue(getObjectMapper().writeValueAsString(compactionTask.getTransformSpec()), Map.class),
        IndexSpec.DEFAULT.asMap(mapper),
        mapper.readValue(
            mapper.writeValueAsString(
                new UniformGranularitySpec(
                    Granularities.DAY,
                    Granularities.MINUTE,
                    true,
                    ImmutableList.of(Intervals.of("2014-01-01T00:00:00/2014-01-01T03:00:00"))
                )
            ),
            Map.class
        )
    );
    Assert.assertEquals(
        expectedCompactionState,
        segments.get(0).getLastCompactionState()
    );
  }

  @Test
  public void testCompactionWithNewMetricInMetricsSpec() throws Exception
  {
    runIndexTask();

    final Builder builder = new Builder(
        DATA_SOURCE,
        segmentCacheManagerFactory,
        RETRY_POLICY_FACTORY
    );

    // day segmentGranularity
    final CompactionTask compactionTask = builder
        .interval(Intervals.of("2014-01-01/2014-01-02"))
        .granularitySpec(new ClientCompactionTaskGranularitySpec(Granularities.DAY, null, null))
        .metricsSpec(new AggregatorFactory[]{
            new CountAggregatorFactory("cnt"),
            new LongSumAggregatorFactory("val", "val")
        })
        .build();

    Pair<TaskStatus, List<DataSegment>> resultPair = runTask(compactionTask);

    Assert.assertTrue(resultPair.lhs.isSuccess());

    List<DataSegment> segments = resultPair.rhs;

    Assert.assertEquals(1, segments.size());

    Assert.assertEquals(Intervals.of("2014-01-01/2014-01-02"), segments.get(0).getInterval());
    Assert.assertEquals(new NumberedShardSpec(0, 1), segments.get(0).getShardSpec());

    ObjectMapper mapper = new DefaultObjectMapper();
    Map<String, String> expectedCountMetric = new HashMap<>();
    expectedCountMetric.put("type", "count");
    expectedCountMetric.put("name", "cnt");
    Map<String, String> expectedLongSumMetric = new HashMap<>();
    expectedLongSumMetric.put("type", "longSum");
    expectedLongSumMetric.put("name", "val");
    expectedLongSumMetric.put("fieldName", "val");
    CompactionState expectedCompactionState = new CompactionState(
        new DynamicPartitionsSpec(5000000, Long.MAX_VALUE),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("ts", "dim"))),
        ImmutableList.of(expectedCountMetric, expectedLongSumMetric),
        getObjectMapper().readValue(getObjectMapper().writeValueAsString(compactionTask.getTransformSpec()), Map.class),
        IndexSpec.DEFAULT.asMap(mapper),
        mapper.readValue(
            mapper.writeValueAsString(
                new UniformGranularitySpec(
                    Granularities.DAY,
                    Granularities.MINUTE,
                    true,
                    ImmutableList.of(Intervals.of("2014-01-01T00:00:00/2014-01-01T03:00:00"))
                )
            ),
            Map.class
        )
    );
    Assert.assertEquals(
        expectedCompactionState,
        segments.get(0).getLastCompactionState()
    );
  }

  @Test
  public void testWithGranularitySpecNonNullSegmentGranularityAndNullQueryGranularity() throws Exception
  {
    runIndexTask();

    final Builder builder = new Builder(
        DATA_SOURCE,
        segmentCacheManagerFactory,
        RETRY_POLICY_FACTORY
    );

    // day segmentGranularity
    final CompactionTask compactionTask1 = builder
        .interval(Intervals.of("2014-01-01/2014-01-02"))
        .granularitySpec(new ClientCompactionTaskGranularitySpec(Granularities.DAY, null, null))
        .build();

    Pair<TaskStatus, List<DataSegment>> resultPair = runTask(compactionTask1);

    Assert.assertTrue(resultPair.lhs.isSuccess());

    List<DataSegment> segments = resultPair.rhs;

    Assert.assertEquals(1, segments.size());

    Assert.assertEquals(Intervals.of("2014-01-01/2014-01-02"), segments.get(0).getInterval());
    Assert.assertEquals(new NumberedShardSpec(0, 1), segments.get(0).getShardSpec());
    Assert.assertEquals(
        getDefaultCompactionState(
            Granularities.DAY,
            Granularities.MINUTE,
            ImmutableList.of(Intervals.of("2014-01-01T00:00:00/2014-01-01T03:00:00"))
        ),
        segments.get(0).getLastCompactionState()
    );

    // hour segmentGranularity
    final CompactionTask compactionTask2 = builder
        .interval(Intervals.of("2014-01-01/2014-01-02"))
        .granularitySpec(new ClientCompactionTaskGranularitySpec(Granularities.HOUR, null, null))
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
      Assert.assertEquals(new NumberedShardSpec(0, 1), segments.get(i).getShardSpec());
      Assert.assertEquals(
          getDefaultCompactionState(
              Granularities.HOUR,
              Granularities.MINUTE,
              ImmutableList.of(Intervals.of("2014-01-01/2014-01-02"))
          ),
          segments.get(i).getLastCompactionState()
      );
    }
  }

  @Test
  public void testWithGranularitySpecNonNullQueryGranularityAndNullSegmentGranularity() throws Exception
  {
    runIndexTask();

    final Builder builder = new Builder(
        DATA_SOURCE,
        segmentCacheManagerFactory,
        RETRY_POLICY_FACTORY
    );

    // day queryGranularity
    final CompactionTask compactionTask1 = builder
        .interval(Intervals.of("2014-01-01/2014-01-02"))
        .granularitySpec(new ClientCompactionTaskGranularitySpec(null, Granularities.SECOND, null))
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
      Assert.assertEquals(
          getDefaultCompactionState(
              Granularities.HOUR,
              Granularities.SECOND,
              ImmutableList.of(Intervals.of("2014-01-01T0%d:00:00/2014-01-01T0%d:00:00",
                                            i,
                                            i + 1
              ))
          ),
          segments.get(i).getLastCompactionState()
      );
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

  @Test
  public void testWithGranularitySpecNonNullQueryGranularityAndNonNullSegmentGranularity() throws Exception
  {
    runIndexTask();

    final Builder builder = new Builder(
        DATA_SOURCE,
        segmentCacheManagerFactory,
        RETRY_POLICY_FACTORY
    );

    // day segmentGranularity and day queryGranularity
    final CompactionTask compactionTask1 = builder
        .interval(Intervals.of("2014-01-01/2014-01-02"))
        .granularitySpec(new ClientCompactionTaskGranularitySpec(Granularities.DAY, Granularities.DAY, null))
        .build();

    Pair<TaskStatus, List<DataSegment>> resultPair = runTask(compactionTask1);

    Assert.assertTrue(resultPair.lhs.isSuccess());

    List<DataSegment> segments = resultPair.rhs;

    Assert.assertEquals(1, segments.size());

    Assert.assertEquals(Intervals.of("2014-01-01/2014-01-02"), segments.get(0).getInterval());
    Assert.assertEquals(new NumberedShardSpec(0, 1), segments.get(0).getShardSpec());
    Assert.assertEquals(
        getDefaultCompactionState(
            Granularities.DAY,
            Granularities.DAY,
            ImmutableList.of(Intervals.of("2014-01-01T00:00:00/2014-01-01T03:00:00"))
        ),
        segments.get(0).getLastCompactionState()
    );
  }

  @Test
  public void testWithGranularitySpecNullQueryGranularityAndNullSegmentGranularity() throws Exception
  {
    runIndexTask();

    final Builder builder = new Builder(
        DATA_SOURCE,
        segmentCacheManagerFactory,
        RETRY_POLICY_FACTORY
    );

    final CompactionTask compactionTask1 = builder
        .interval(Intervals.of("2014-01-01/2014-01-02"))
        .granularitySpec(new ClientCompactionTaskGranularitySpec(null, null, null))
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
      Assert.assertEquals(
          getDefaultCompactionState(
              Granularities.HOUR,
              Granularities.MINUTE,
              ImmutableList.of(Intervals.of("2014-01-01T0%d:00:00/2014-01-01T0%d:00:00",
                                            i,
                                            i + 1
              ))
          ),
          segments.get(i).getLastCompactionState()
      );
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

  @Test
  public void testCompactThenAppend() throws Exception
  {
    runIndexTask();

    final Builder builder = new Builder(
        DATA_SOURCE,
        segmentCacheManagerFactory,
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
  public void testPartialIntervalCompactWithFinerSegmentGranularityThenFullIntervalCompactWithDropExistingTrue()
      throws Exception
  {
    // This test fails with segment lock because of the bug reported in https://github.com/apache/druid/issues/10911.
    if (lockGranularity == LockGranularity.SEGMENT) {
      return;
    }

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
    Pair<TaskStatus, List<DataSegment>> result = runIndexTask();
    Assert.assertEquals(6, result.rhs.size());

    final Builder builder = new Builder(
        DATA_SOURCE,
        segmentCacheManagerFactory,
        RETRY_POLICY_FACTORY
    );

    // Setup partial compaction:
    // Change the granularity from HOUR to MINUTE through compaction for hour 01, there are three rows in the compaction interval,
    // all three in the same timestamp (see TEST_ROWS), this should generate one segments (task will now use
    // the default rows per segments since compaction's tuning config is null) in same minute and
    // 59 tombstones to completely overshadow the existing hour 01 segment. Since the segments outside the
    // compaction interval should remanin unchanged there should be a total of 1 + (2 + 59) + 2 = 64 segments

    // **** PARTIAL COMPACTION: hour -> minute ****
    final Interval compactionPartialInterval = Intervals.of("2014-01-01T01:00:00/2014-01-01T02:00:00");
    final CompactionTask partialCompactionTask = builder
        .segmentGranularity(Granularities.MINUTE)
        // Set dropExisting to true
        .inputSpec(new CompactionIntervalSpec(compactionPartialInterval, null), true)
        .build();
    final Pair<TaskStatus, List<DataSegment>> partialCompactionResult = runTask(partialCompactionTask);
    Assert.assertTrue(partialCompactionResult.lhs.isSuccess());

    // Segments that did not belong in the compaction interval (hours 00 and 02) are expected unchanged
    // add 2 unchanged segments for hour 00:
    final Set<DataSegment> expectedSegments = new HashSet<>();
    expectedSegments.addAll(
        getStorageCoordinator().retrieveUsedSegmentsForIntervals(
            DATA_SOURCE,
            Collections.singletonList(Intervals.of("2014-01-01T00:00:00/2014-01-01T01:00:00")),
            Segments.ONLY_VISIBLE
        )
    );
    // add 2 unchanged segments for hour 02:
    expectedSegments.addAll(
        getStorageCoordinator().retrieveUsedSegmentsForIntervals(
            DATA_SOURCE,
            Collections.singletonList(Intervals.of("2014-01-01T02:00:00/2014-01-01T03:00:00")),
            Segments.ONLY_VISIBLE
        )
    );
    expectedSegments.addAll(partialCompactionResult.rhs);
    Assert.assertEquals(64, expectedSegments.size());

    // New segments that were compacted are expected. However, old segments of the compacted interval should be
    // overshadowed by the new tombstones (59) being created for all minutes other than 01:01
    final Set<DataSegment> segmentsAfterPartialCompaction = new HashSet<>(
        getStorageCoordinator().retrieveUsedSegmentsForIntervals(
            DATA_SOURCE,
            Collections.singletonList(Intervals.of("2014-01-01/2014-01-02")),
            Segments.ONLY_VISIBLE
        )
    );
    Assert.assertEquals(expectedSegments, segmentsAfterPartialCompaction);
    final List<DataSegment> realSegmentsAfterPartialCompaction =
        segmentsAfterPartialCompaction.stream()
                                      .filter(s -> !s.isTombstone())
                                      .collect(Collectors.toList());
    final List<DataSegment> tombstonesAfterPartialCompaction =
        segmentsAfterPartialCompaction.stream()
                                      .filter(s -> s.isTombstone())
                                      .collect(Collectors.toList());
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
    final CompactionTask fullCompactionTask = builder
        .segmentGranularity(null)
        // Set dropExisting to true
        .inputSpec(new CompactionIntervalSpec(Intervals.of("2014-01-01/2014-01-02"), null), true)
        .build();

    // **** FULL COMPACTION ****
    final Pair<TaskStatus, List<DataSegment>> fullCompactionResult = runTask(fullCompactionTask);
    Assert.assertTrue(fullCompactionResult.lhs.isSuccess());


    final List<DataSegment> segmentsAfterFullCompaction = new ArrayList<>(
        getStorageCoordinator().retrieveUsedSegmentsForIntervals(
            DATA_SOURCE,
            Collections.singletonList(Intervals.of("2014-01-01/2014-01-02")),
            Segments.ONLY_VISIBLE
        )
    );
    segmentsAfterFullCompaction.sort(
        (s1, s2) -> Comparators.intervalsByStartThenEnd().compare(s1.getInterval(), s2.getInterval())
    );
    Assert.assertEquals(62, segmentsAfterFullCompaction.size());

    final List<DataSegment> tombstonesAfterFullCompaction =
        segmentsAfterFullCompaction.stream()
                                   .filter(s -> s.isTombstone())
                                   .collect(Collectors.toList());
    Assert.assertEquals(59, tombstonesAfterFullCompaction.size());

    final List<DataSegment> realSegmentsAfterFullCompaction =
        segmentsAfterFullCompaction.stream()
                                   .filter(s -> !s.isTombstone())
                                   .collect(Collectors.toList());
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
    if (lockGranularity == LockGranularity.SEGMENT) {
      return;
    }

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
    Pair<TaskStatus, List<DataSegment>> result = runIndexTask();
    Assert.assertEquals(6, result.rhs.size());

    final Builder builder = new Builder(
        DATA_SOURCE,
        segmentCacheManagerFactory,
        RETRY_POLICY_FACTORY
    );

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
    final CompactionTask partialCompactionTask = builder
        .segmentGranularity(Granularities.MINUTE)
        // Set dropExisting to true
        .inputSpec(new CompactionIntervalSpec(compactionPartialInterval, null), true)
        .build();
    final Pair<TaskStatus, List<DataSegment>> partialCompactionResult = runTask(partialCompactionTask);
    Assert.assertTrue(partialCompactionResult.lhs.isSuccess());

    // Segments that did not belong in the compaction interval (hours 00 and 02) are expected unchanged
    // add 2 unchanged segments for hour 00:
    final Set<DataSegment> expectedSegments = new HashSet<>();
    expectedSegments.addAll(
        getStorageCoordinator().retrieveUsedSegmentsForIntervals(
            DATA_SOURCE,
            Collections.singletonList(Intervals.of("2014-01-01T00:00:00/2014-01-01T01:00:00")),
            Segments.ONLY_VISIBLE
        )
    );
    // add 2 unchanged segments for hour 02:
    expectedSegments.addAll(
        getStorageCoordinator().retrieveUsedSegmentsForIntervals(
            DATA_SOURCE,
            Collections.singletonList(Intervals.of("2014-01-01T02:00:00/2014-01-01T03:00:00")),
            Segments.ONLY_VISIBLE
        )
    );
    expectedSegments.addAll(partialCompactionResult.rhs);
    Assert.assertEquals(64, expectedSegments.size());

    // New segments that were compacted are expected. However, old segments of the compacted interval should be
    // overshadowed by the new tombstones (59) being created for all minutes other than 01:01
    final Set<DataSegment> segmentsAfterPartialCompaction = new HashSet<>(
        getStorageCoordinator().retrieveUsedSegmentsForIntervals(
            DATA_SOURCE,
            Collections.singletonList(Intervals.of("2014-01-01/2014-01-02")),
            Segments.ONLY_VISIBLE
        )
    );
    Assert.assertEquals(expectedSegments, segmentsAfterPartialCompaction);
    final List<DataSegment> realSegmentsAfterPartialCompaction =
        segmentsAfterPartialCompaction.stream()
                                      .filter(s -> !s.isTombstone())
                                      .collect(Collectors.toList());
    final List<DataSegment> tombstonesAfterPartialCompaction =
        segmentsAfterPartialCompaction.stream()
                                      .filter(s -> s.isTombstone())
                                      .collect(Collectors.toList());
    Assert.assertEquals(59, tombstonesAfterPartialCompaction.size());
    Assert.assertEquals(5, realSegmentsAfterPartialCompaction.size());
    Assert.assertEquals(64, segmentsAfterPartialCompaction.size());

    // Now setup compaction over an interval with only tombstones, keeping same, minute granularity
    final CompactionTask compactionTaskOverOnlyTombstones = builder
        .segmentGranularity(null)
        // Set dropExisting to true
        // last 59 minutes of our 01, should be all tombstones
        .inputSpec(new CompactionIntervalSpec(Intervals.of("2014-01-01T01:01:00/2014-01-01T02:00:00"), null), true)
        .build();

    // **** Compaction over tombstones ****
    final Pair<TaskStatus, List<DataSegment>> resultOverOnlyTombstones = runTask(compactionTaskOverOnlyTombstones);
    Assert.assertTrue(resultOverOnlyTombstones.lhs.isSuccess());

    // compaction should not fail but since it is over the same granularity it should leave
    // the tombstones unchanged
    Assert.assertEquals(59, resultOverOnlyTombstones.rhs.size());
    resultOverOnlyTombstones.rhs.forEach(t -> Assert.assertTrue(t.isTombstone()));
  }

  @Test
  public void testPartialIntervalCompactWithFinerSegmentGranularityThenFullIntervalCompactWithDropExistingFalse()
      throws Exception
  {
    // This test fails with segment lock because of the bug reported in https://github.com/apache/druid/issues/10911.
    if (lockGranularity == LockGranularity.SEGMENT) {
      return;
    }

    runIndexTask();

    final Set<DataSegment> expectedSegments = new HashSet<>(
        getStorageCoordinator().retrieveUsedSegmentsForIntervals(
            DATA_SOURCE,
            Collections.singletonList(Intervals.of("2014-01-01/2014-01-02")),
            Segments.ONLY_VISIBLE
        )
    );

    final Builder builder = new Builder(
        DATA_SOURCE,
        segmentCacheManagerFactory,
        RETRY_POLICY_FACTORY
    );

    final Interval partialInterval = Intervals.of("2014-01-01T01:00:00/2014-01-01T02:00:00");
    final CompactionTask partialCompactionTask = builder
        .segmentGranularity(Granularities.MINUTE)
        // Set dropExisting to false
        .inputSpec(new CompactionIntervalSpec(partialInterval, null), false)
        .build();

    final Pair<TaskStatus, List<DataSegment>> partialCompactionResult = runTask(partialCompactionTask);
    Assert.assertTrue(partialCompactionResult.lhs.isSuccess());
    // All segments in the previous expectedSegments should still appear as they have larger segment granularity.
    expectedSegments.addAll(partialCompactionResult.rhs);

    final Set<DataSegment> segmentsAfterPartialCompaction = new HashSet<>(
        getStorageCoordinator().retrieveUsedSegmentsForIntervals(
            DATA_SOURCE,
            Collections.singletonList(Intervals.of("2014-01-01/2014-01-02")),
            Segments.ONLY_VISIBLE
        )
    );

    Assert.assertEquals(expectedSegments, segmentsAfterPartialCompaction);

    final CompactionTask fullCompactionTask = builder
        .segmentGranularity(null)
        // Set dropExisting to false
        .inputSpec(new CompactionIntervalSpec(Intervals.of("2014-01-01/2014-01-02"), null), false)
        .build();

    final Pair<TaskStatus, List<DataSegment>> fullCompactionResult = runTask(fullCompactionTask);
    Assert.assertTrue(fullCompactionResult.lhs.isSuccess());

    final List<DataSegment> segmentsAfterFullCompaction = new ArrayList<>(
        getStorageCoordinator().retrieveUsedSegmentsForIntervals(
            DATA_SOURCE,
            Collections.singletonList(Intervals.of("2014-01-01/2014-01-02")),
            Segments.ONLY_VISIBLE
        )
    );
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
    runIndexTask();

    // make sure that indexTask becomes ready first, then compactionTask becomes ready, then indexTask runs
    final CountDownLatch compactionTaskReadyLatch = new CountDownLatch(1);
    final CountDownLatch indexTaskStartLatch = new CountDownLatch(1);
    final Future<Pair<TaskStatus, List<DataSegment>>> indexFuture = exec.submit(
        () -> runIndexTask(compactionTaskReadyLatch, indexTaskStartLatch, false)
    );

    final Builder builder = new Builder(
        DATA_SOURCE,
        segmentCacheManagerFactory,
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
        segmentCacheManagerFactory,
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

  @Test
  public void testRunWithSpatialDimensions() throws Exception
  {
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
    runIndexTask(null, null, spatialSpec, spatialrows, false);

    final Builder builder = new Builder(
        DATA_SOURCE,
        segmentCacheManagerFactory,
        RETRY_POLICY_FACTORY
    );

    final CompactionTask compactionTask = builder
        .interval(Intervals.of("2014-01-01/2014-01-02"))
        .build();

    final Pair<TaskStatus, List<DataSegment>> resultPair = runTask(compactionTask);

    Assert.assertTrue(resultPair.lhs.isSuccess());

    final List<DataSegment> segments = resultPair.rhs;
    Assert.assertEquals(2, segments.size());

    for (int i = 0; i < 2; i++) {
      Assert.assertEquals(
          Intervals.of("2014-01-01T0%d:00:00/2014-01-01T0%d:00:00", i, i + 1),
          segments.get(i).getInterval()
      );
      Map<String, String> expectedLongSumMetric = new HashMap<>();
      expectedLongSumMetric.put("name", "val");
      expectedLongSumMetric.put("type", "longSum");
      expectedLongSumMetric.put("fieldName", "val");
      Assert.assertEquals(
          getDefaultCompactionState(
              Granularities.HOUR,
              Granularities.MINUTE,
              ImmutableList.of(Intervals.of("2014-01-01T0%d:00:00/2014-01-01T0%d:00:00", i, i + 1)),
              DimensionsSpec.builder()
                            .setDimensions(Arrays.asList(
                                new StringDimensionSchema("ts"),
                                new StringDimensionSchema("dim"),
                                new NewSpatialDimensionSchema("spatial", Collections.singletonList("spatial"))
                            ))
                            .build(),
              expectedLongSumMetric
          ),
          segments.get(i).getLastCompactionState()
      );
      if (lockGranularity == LockGranularity.SEGMENT) {
        Assert.assertEquals(
            new NumberedOverwriteShardSpec(32768, 0, 2, (short) 1, (short) 1),
            segments.get(i).getShardSpec()
        );
      } else {
        Assert.assertEquals(new NumberedShardSpec(0, 1), segments.get(i).getShardSpec());
      }
    }

    final File cacheDir = temporaryFolder.newFolder();
    final SegmentCacheManager segmentCacheManager = segmentCacheManagerFactory.manufacturate(cacheDir);

    List<String> rowsFromSegment = new ArrayList<>();
    for (DataSegment segment : segments) {
      final File segmentFile = segmentCacheManager.getSegmentFiles(segment);

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

      cursorSequence.accumulate(rowsFromSegment, (accumulated, cursor) -> {
        cursor.reset();
        final ColumnSelectorFactory factory = cursor.getColumnSelectorFactory();
        Assert.assertTrue(factory.getColumnCapabilities("spatial").hasSpatialIndexes());
        while (!cursor.isDone()) {
          final ColumnValueSelector<?> selector1 = factory.makeColumnValueSelector("ts");
          final DimensionSelector selector2 = factory.makeDimensionSelector(new DefaultDimensionSpec("dim", "dim"));
          final DimensionSelector selector3 = factory.makeDimensionSelector(new DefaultDimensionSpec("spatial", "spatial"));
          final DimensionSelector selector4 = factory.makeDimensionSelector(new DefaultDimensionSpec("val", "val"));


          rowsFromSegment.add(
              StringUtils.format(
                  "%s,%s,%s,%s\n",
                  selector1.getObject(),
                  selector2.getObject(),
                  selector3.getObject(),
                  selector4.getObject()
              )
          );

          cursor.advance();
        }

        return accumulated;
      });
    }
    Assert.assertEquals(spatialrows, rowsFromSegment);
  }

  @Test
  public void testRunWithAutoCastDimensions() throws Exception
  {
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
                          new AutoTypeColumnSchema("ts", ColumnType.STRING),
                          new AutoTypeColumnSchema("dim", null),
                          new AutoTypeColumnSchema("x", ColumnType.LONG),
                          new AutoTypeColumnSchema("y", ColumnType.LONG)
                      ))
                      .build(),
        "|",
        Arrays.asList("ts", "dim", "x", "y", "val"),
        false,
        0
    );
    runIndexTask(null, null, spec, rows, false);

    final Builder builder = new Builder(
        DATA_SOURCE,
        segmentCacheManagerFactory,
        RETRY_POLICY_FACTORY
    );

    final CompactionTask compactionTask = builder
        .interval(Intervals.of("2014-01-01/2014-01-02"))
        .build();

    final Pair<TaskStatus, List<DataSegment>> resultPair = runTask(compactionTask);

    Assert.assertTrue(resultPair.lhs.isSuccess());

    final List<DataSegment> segments = resultPair.rhs;
    Assert.assertEquals(2, segments.size());

    for (int i = 0; i < 2; i++) {
      Assert.assertEquals(
          Intervals.of("2014-01-01T0%d:00:00/2014-01-01T0%d:00:00", i, i + 1),
          segments.get(i).getInterval()
      );
      Map<String, String> expectedLongSumMetric = new HashMap<>();
      expectedLongSumMetric.put("name", "val");
      expectedLongSumMetric.put("type", "longSum");
      expectedLongSumMetric.put("fieldName", "val");
      Assert.assertEquals(
          getDefaultCompactionState(
              Granularities.HOUR,
              Granularities.MINUTE,
              ImmutableList.of(Intervals.of("2014-01-01T0%d:00:00/2014-01-01T0%d:00:00", i, i + 1)),
              DimensionsSpec.builder()
                            .setDimensions(Arrays.asList(
                                // check explicitly specified types are preserved
                                new AutoTypeColumnSchema("ts", ColumnType.STRING),
                                new AutoTypeColumnSchema("dim", null),
                                new AutoTypeColumnSchema("x", ColumnType.LONG),
                                new AutoTypeColumnSchema("y", ColumnType.LONG)
                            ))
                            .build(),
              expectedLongSumMetric
          ),
          segments.get(i).getLastCompactionState()
      );
      if (lockGranularity == LockGranularity.SEGMENT) {
        Assert.assertEquals(
            new NumberedOverwriteShardSpec(32768, 0, 2, (short) 1, (short) 1),
            segments.get(i).getShardSpec()
        );
      } else {
        Assert.assertEquals(new NumberedShardSpec(0, 1), segments.get(i).getShardSpec());
      }
    }

    final File cacheDir = temporaryFolder.newFolder();
    final SegmentCacheManager segmentCacheManager = segmentCacheManagerFactory.manufacturate(cacheDir);

    List<String> rowsFromSegment = new ArrayList<>();
    for (DataSegment segment : segments) {
      final File segmentFile = segmentCacheManager.getSegmentFiles(segment);

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

      cursorSequence.accumulate(rowsFromSegment, (accumulated, cursor) -> {
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

        return accumulated;
      });
    }
    Assert.assertEquals(rows, rowsFromSegment);
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
            appendToExisting,
            false
        ),
        null
    );

    return runTask(indexTask, readyLatchToCountDown, latchToAwaitBeforeRun);
  }

  private Pair<TaskStatus, List<DataSegment>> runIndexTask(
      @Nullable CountDownLatch readyLatchToCountDown,
      @Nullable CountDownLatch latchToAwaitBeforeRun,
      ParseSpec parseSpec,
      List<String> rows,
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

    IndexTask indexTask = new IndexTask(
        null,
        null,
        IndexTaskTest.createIngestionSpec(
            getObjectMapper(),
            tmpDir,
            parseSpec,
            null,
            new UniformGranularitySpec(
                Granularities.HOUR,
                Granularities.MINUTE,
                null
            ),
            IndexTaskTest.createTuningConfig(2, 2, null, 2L, null, false, true),
            appendToExisting,
            false
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
    objectMapper.registerSubtypes(TombstoneLoadSpec.class);

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
    final SegmentCacheManager loader = new SegmentLocalCacheManager(
        new SegmentLoaderConfig()
        {
          @Override
          public List<StorageLocationConfig> getLocations()
          {
            return ImmutableList.of(new StorageLocationConfig(localDeepStorage, null, null));
          }
        },
        objectMapper
    );

    final TaskConfig config = new TaskConfigBuilder()
            .setBatchProcessingMode(TaskConfig.BATCH_PROCESSING_MODE_DEFAULT.name())
            .build();
    return new TaskToolbox.Builder()
        .config(config)
        .taskActionClient(createActionClient(task))
        .segmentPusher(new LocalDataSegmentPusher(new LocalDataSegmentPusherConfig()))
        .dataSegmentKiller(new NoopDataSegmentKiller())
        .joinableFactory(NoopJoinableFactory.INSTANCE)
        .segmentCacheManager(loader)
        .jsonMapper(objectMapper)
        .taskWorkDir(temporaryFolder.newFolder())
        .indexIO(getIndexIO())
        .indexMergerV9(getIndexMergerV9Factory().create(task.getContextValue(Tasks.STORE_EMPTY_COLUMNS_KEY, true)))
        .taskReportFileWriter(new NoopTestTaskReportFileWriter())
        .authorizerMapper(AuthTestUtils.TEST_AUTHORIZER_MAPPER)
        .chatHandlerProvider(new NoopChatHandlerProvider())
        .rowIngestionMetersFactory(testUtils.getRowIngestionMetersFactory())
        .appenderatorsManager(new TestAppenderatorsManager())
        .overlordClient(overlordClient)
        .coordinatorClient(coordinatorClient)
        .taskLogPusher(null)
        .attemptId("1")
        .build();
  }

  private List<String> getCSVFormatRowsFromSegments(List<DataSegment> segments) throws Exception
  {
    final File cacheDir = temporaryFolder.newFolder();
    final SegmentCacheManager segmentCacheManager = segmentCacheManagerFactory.manufacturate(cacheDir);

    List<String> rowsFromSegment = new ArrayList<>();
    for (DataSegment segment : segments) {
      final File segmentFile = segmentCacheManager.getSegmentFiles(segment);

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

      cursorSequence.accumulate(rowsFromSegment, (accumulated, cursor) -> {
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

        return accumulated;
      });
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
