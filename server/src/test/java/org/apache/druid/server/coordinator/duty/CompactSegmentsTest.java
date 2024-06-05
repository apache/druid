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

package org.apache.druid.server.coordinator.duty;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.druid.client.DataSourcesSnapshot;
import org.apache.druid.client.indexing.ClientCompactionIOConfig;
import org.apache.druid.client.indexing.ClientCompactionIntervalSpec;
import org.apache.druid.client.indexing.ClientCompactionTaskGranularitySpec;
import org.apache.druid.client.indexing.ClientCompactionTaskQuery;
import org.apache.druid.client.indexing.ClientCompactionTaskQueryTuningConfig;
import org.apache.druid.client.indexing.ClientTaskQuery;
import org.apache.druid.client.indexing.IndexingTotalWorkerCapacityInfo;
import org.apache.druid.client.indexing.NoopOverlordClient;
import org.apache.druid.client.indexing.TaskPayloadResponse;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.indexer.RunnerTaskState;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexer.partitions.SingleDimensionPartitionsSpec;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.metadata.LockFilterPolicy;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.segment.incremental.OnheapIncrementalIndex;
import org.apache.druid.segment.indexing.BatchIOConfig;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.server.coordinator.AutoCompactionSnapshot;
import org.apache.druid.server.coordinator.CoordinatorCompactionConfig;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.UserCompactionTaskDimensionsConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskGranularityConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskIOConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskQueryTuningConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskTransformConfig;
import org.apache.druid.server.coordinator.compact.CompactionSegmentSearchPolicy;
import org.apache.druid.server.coordinator.compact.NewestSegmentFirstPolicy;
import org.apache.druid.server.coordinator.config.DruidCoordinatorConfig;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.Stats;
import org.apache.druid.timeline.CompactionState;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentTimeline;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.HashBasedNumberedShardSpec;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.apache.druid.timeline.partition.ShardSpec;
import org.apache.druid.timeline.partition.SingleDimensionShardSpec;
import org.apache.druid.utils.Streams;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@RunWith(Parameterized.class)
public class CompactSegmentsTest
{
  private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();
  private static final DruidCoordinatorConfig COORDINATOR_CONFIG = Mockito.mock(DruidCoordinatorConfig.class);
  private static final String DATA_SOURCE_PREFIX = "dataSource_";
  private static final int PARTITION_PER_TIME_INTERVAL = 4;
  // Each dataSource starts with 440 byte, 44 segments, and 11 intervals needing compaction
  private static final int TOTAL_BYTE_PER_DATASOURCE = 440;
  private static final int TOTAL_SEGMENT_PER_DATASOURCE = 44;
  private static final int TOTAL_INTERVAL_PER_DATASOURCE = 11;
  private static final int MAXIMUM_CAPACITY_WITH_AUTO_SCALE = 10;
  private static final NewestSegmentFirstPolicy SEARCH_POLICY = new NewestSegmentFirstPolicy(JSON_MAPPER);

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> constructorFeeder()
  {
    final MutableInt nextRangePartitionBoundary = new MutableInt(0);
    return ImmutableList.of(
        new Object[]{
            new DynamicPartitionsSpec(300000, Long.MAX_VALUE),
            (BiFunction<Integer, Integer, ShardSpec>) NumberedShardSpec::new
        },
        new Object[]{
            new HashedPartitionsSpec(null, 2, ImmutableList.of("dim")),
            (BiFunction<Integer, Integer, ShardSpec>) (bucketId, numBuckets) -> new HashBasedNumberedShardSpec(
                bucketId,
                numBuckets,
                bucketId,
                numBuckets,
                ImmutableList.of("dim"),
                null,
                JSON_MAPPER
            )
        },
        new Object[]{
            new SingleDimensionPartitionsSpec(300000, null, "dim", false),
            (BiFunction<Integer, Integer, ShardSpec>) (bucketId, numBuckets) -> new SingleDimensionShardSpec(
                "dim",
                bucketId == 0 ? null : String.valueOf(nextRangePartitionBoundary.getAndIncrement()),
                bucketId.equals(numBuckets) ? null : String.valueOf(nextRangePartitionBoundary.getAndIncrement()),
                bucketId,
                numBuckets
            )
        }
    );
  }

  private final PartitionsSpec partitionsSpec;
  private final BiFunction<Integer, Integer, ShardSpec> shardSpecFactory;

  private DataSourcesSnapshot dataSources;
  Map<String, List<DataSegment>> datasourceToSegments = new HashMap<>();

  public CompactSegmentsTest(PartitionsSpec partitionsSpec, BiFunction<Integer, Integer, ShardSpec> shardSpecFactory)
  {
    this.partitionsSpec = partitionsSpec;
    this.shardSpecFactory = shardSpecFactory;
  }

  @Before
  public void setup()
  {
    List<DataSegment> allSegments = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      final String dataSource = DATA_SOURCE_PREFIX + i;
      for (int j : new int[]{0, 1, 2, 3, 7, 8}) {
        for (int k = 0; k < PARTITION_PER_TIME_INTERVAL; k++) {
          List<DataSegment> segmentForDatasource = datasourceToSegments.computeIfAbsent(dataSource, key -> new ArrayList<>());
          DataSegment dataSegment = createSegment(dataSource, j, true, k);
          allSegments.add(dataSegment);
          segmentForDatasource.add(dataSegment);
          dataSegment = createSegment(dataSource, j, false, k);
          allSegments.add(dataSegment);
          segmentForDatasource.add(dataSegment);
        }
      }
    }
    dataSources = DataSourcesSnapshot.fromUsedSegments(allSegments, ImmutableMap.of());
  }

  private DataSegment createSegment(String dataSource, int startDay, boolean beforeNoon, int partition)
  {
    final ShardSpec shardSpec = shardSpecFactory.apply(partition, 2);
    final Interval interval = beforeNoon ?
                              Intervals.of(
                                  StringUtils.format(
                                      "2017-01-%02dT00:00:00/2017-01-%02dT12:00:00",
                                      startDay + 1,
                                      startDay + 1
                                  )
                              ) :
                              Intervals.of(
                                  StringUtils.format(
                                      "2017-01-%02dT12:00:00/2017-01-%02dT00:00:00",
                                      startDay + 1,
                                      startDay + 2
                                  )
                              );
    return new DataSegment(
        dataSource,
        interval,
        "version",
        null,
        Collections.emptyList(),
        Collections.emptyList(),
        shardSpec,
        0,
        10L
    );
  }

  @Test
  public void testSerde() throws Exception
  {
    final TestOverlordClient overlordClient = new TestOverlordClient(JSON_MAPPER);

    JSON_MAPPER.setInjectableValues(
        new InjectableValues.Std()
            .addValue(DruidCoordinatorConfig.class, COORDINATOR_CONFIG)
            .addValue(OverlordClient.class, overlordClient)
            .addValue(CompactionSegmentSearchPolicy.class, SEARCH_POLICY)
    );

    final CompactSegments compactSegments = new CompactSegments(SEARCH_POLICY, overlordClient);
    String compactSegmentString = JSON_MAPPER.writeValueAsString(compactSegments);
    CompactSegments serdeCompactSegments = JSON_MAPPER.readValue(compactSegmentString, CompactSegments.class);

    Assert.assertNotNull(serdeCompactSegments);
    Assert.assertSame(overlordClient, serdeCompactSegments.getOverlordClient());
  }

  @Test
  public void testRun()
  {
    final TestOverlordClient overlordClient = new TestOverlordClient(JSON_MAPPER);
    final CompactSegments compactSegments = new CompactSegments(SEARCH_POLICY, overlordClient);

    final Supplier<String> expectedVersionSupplier = new Supplier<String>()
    {
      private int i = 0;

      @Override
      public String get()
      {
        return "newVersion_" + i++;
      }
    };
    int expectedCompactTaskCount = 1;
    int expectedRemainingSegments = 400;

    // compact for 2017-01-08T12:00:00.000Z/2017-01-09T12:00:00.000Z
    assertCompactSegments(
        compactSegments,
        Intervals.of("2017-01-%02dT00:00:00/2017-01-%02dT12:00:00", 9, 9),
        expectedRemainingSegments,
        expectedCompactTaskCount,
        expectedVersionSupplier
    );
    expectedRemainingSegments -= 40;
    assertCompactSegments(
        compactSegments,
        Intervals.of("2017-01-%02dT12:00:00/2017-01-%02dT00:00:00", 8, 9),
        expectedRemainingSegments,
        expectedCompactTaskCount,
        expectedVersionSupplier
    );

    // compact for 2017-01-07T12:00:00.000Z/2017-01-08T12:00:00.000Z
    expectedRemainingSegments -= 40;
    assertCompactSegments(
        compactSegments,
        Intervals.of("2017-01-%02dT00:00:00/2017-01-%02dT12:00:00", 8, 8),
        expectedRemainingSegments,
        expectedCompactTaskCount,
        expectedVersionSupplier
    );
    expectedRemainingSegments -= 40;
    assertCompactSegments(
        compactSegments,
        Intervals.of("2017-01-%02dT12:00:00/2017-01-%02dT00:00:00", 4, 5),
        expectedRemainingSegments,
        expectedCompactTaskCount,
        expectedVersionSupplier
    );

    for (int endDay = 4; endDay > 1; endDay -= 1) {
      expectedRemainingSegments -= 40;
      assertCompactSegments(
          compactSegments,
          Intervals.of("2017-01-%02dT00:00:00/2017-01-%02dT12:00:00", endDay, endDay),
          expectedRemainingSegments,
          expectedCompactTaskCount,
          expectedVersionSupplier
      );
      expectedRemainingSegments -= 40;
      assertCompactSegments(
          compactSegments,
          Intervals.of("2017-01-%02dT12:00:00/2017-01-%02dT00:00:00", endDay - 1, endDay),
          expectedRemainingSegments,
          expectedCompactTaskCount,
          expectedVersionSupplier
      );
    }

    assertLastSegmentNotCompacted(compactSegments);
  }

  @Test
  public void testMakeStats()
  {
    final TestOverlordClient overlordClient = new TestOverlordClient(JSON_MAPPER);
    final CompactSegments compactSegments = new CompactSegments(SEARCH_POLICY, overlordClient);

    // Before any compaction, we do not have any snapshot of compactions
    Map<String, AutoCompactionSnapshot> autoCompactionSnapshots = compactSegments.getAutoCompactionSnapshot();
    Assert.assertEquals(0, autoCompactionSnapshots.size());

    for (int compactionRunCount = 0; compactionRunCount < 11; compactionRunCount++) {
      doCompactionAndAssertCompactSegmentStatistics(compactSegments, compactionRunCount);
    }
    // Test that stats does not change (and is still correct) when auto compaction runs with everything is fully compacted
    final CoordinatorRunStats stats = doCompactSegments(compactSegments);
    Assert.assertEquals(
        0,
        stats.get(Stats.Compaction.SUBMITTED_TASKS)
    );
    for (int i = 0; i < 3; i++) {
      verifySnapshot(
          compactSegments,
          AutoCompactionSnapshot.AutoCompactionScheduleStatus.RUNNING,
          DATA_SOURCE_PREFIX + i,
          0,
          TOTAL_BYTE_PER_DATASOURCE,
          40,
          0,
          TOTAL_INTERVAL_PER_DATASOURCE,
          1,
          0,
          TOTAL_SEGMENT_PER_DATASOURCE / 2,
          4
      );
    }

    // Test run auto compaction with one datasource auto compaction disabled
    // Snapshot should not contain datasource with auto compaction disabled
    List<DataSourceCompactionConfig> removedOneConfig = createCompactionConfigs();
    removedOneConfig.remove(0);
    doCompactSegments(compactSegments, removedOneConfig);
    for (int i = 1; i < 3; i++) {
      verifySnapshot(
          compactSegments,
          AutoCompactionSnapshot.AutoCompactionScheduleStatus.RUNNING,
          DATA_SOURCE_PREFIX + i,
          0,
          TOTAL_BYTE_PER_DATASOURCE,
          40,
          0,
          TOTAL_INTERVAL_PER_DATASOURCE,
          1,
          0,
          TOTAL_SEGMENT_PER_DATASOURCE / 2,
          4
      );
    }

    // Run auto compaction without any dataSource in the compaction config
    // Snapshot should be empty
    doCompactSegments(compactSegments, new ArrayList<>());
    Assert.assertEquals(
        0,
        stats.get(Stats.Compaction.SUBMITTED_TASKS)
    );
    Assert.assertTrue(compactSegments.getAutoCompactionSnapshot().isEmpty());

    assertLastSegmentNotCompacted(compactSegments);
  }

  @Test
  public void testMakeStatsForDataSourceWithCompactedIntervalBetweenNonCompactedIntervals()
  {
    // Only test and validate for one datasource for simplicity.
    // This dataSource has three intervals already compacted (3 intervals, 120 byte, 12 segments already compacted)
    String dataSourceName = DATA_SOURCE_PREFIX + 1;
    List<DataSegment> segments = new ArrayList<>();
    for (int j : new int[]{0, 1, 2, 3, 7, 8}) {
      for (int k = 0; k < PARTITION_PER_TIME_INTERVAL; k++) {
        DataSegment beforeNoon = createSegment(dataSourceName, j, true, k);
        DataSegment afterNoon = createSegment(dataSourceName, j, false, k);
        if (j == 3) {
          // Make two intervals on this day compacted (two compacted intervals back-to-back)
          beforeNoon = beforeNoon.withLastCompactionState(new CompactionState(partitionsSpec, null, null, null, ImmutableMap.of(), ImmutableMap.of()));
          afterNoon = afterNoon.withLastCompactionState(new CompactionState(partitionsSpec, null, null, null, ImmutableMap.of(), ImmutableMap.of()));
        }
        if (j == 1) {
          // Make one interval on this day compacted
          afterNoon = afterNoon.withLastCompactionState(new CompactionState(partitionsSpec, null, null, null, ImmutableMap.of(), ImmutableMap.of()));
        }
        segments.add(beforeNoon);
        segments.add(afterNoon);
      }
    }

    dataSources = DataSourcesSnapshot.fromUsedSegments(segments, ImmutableMap.of());

    final TestOverlordClient overlordClient = new TestOverlordClient(JSON_MAPPER);
    final CompactSegments compactSegments = new CompactSegments(SEARCH_POLICY, overlordClient);

    // Before any compaction, we do not have any snapshot of compactions
    Map<String, AutoCompactionSnapshot> autoCompactionSnapshots = compactSegments.getAutoCompactionSnapshot();
    Assert.assertEquals(0, autoCompactionSnapshots.size());

    // 3 intervals, 120 byte, 12 segments already compacted before the run
    for (int compactionRunCount = 0; compactionRunCount < 8; compactionRunCount++) {
      // Do a cycle of auto compaction which creates one compaction task
      final CoordinatorRunStats stats = doCompactSegments(compactSegments);
      Assert.assertEquals(
          1,
          stats.get(Stats.Compaction.SUBMITTED_TASKS)
      );

      verifySnapshot(
          compactSegments,
          AutoCompactionSnapshot.AutoCompactionScheduleStatus.RUNNING,
          dataSourceName,
          TOTAL_BYTE_PER_DATASOURCE - 120 - 40 * (compactionRunCount + 1),
          120 + 40 * (compactionRunCount + 1),
          40,
          TOTAL_INTERVAL_PER_DATASOURCE - 3 - (compactionRunCount + 1),
          3 + (compactionRunCount + 1),
          1,
          TOTAL_SEGMENT_PER_DATASOURCE - 12 - 4 * (compactionRunCount + 1),
          // 12 segments was compressed before any auto compaction
          // 4 segments was compressed in this run of auto compaction
          // Each previous auto compaction run resulted in 2 compacted segments (4 segments compacted into 2 segments)
          12 + 4 + 2 * (compactionRunCount),
          4
      );
    }

    // Test that stats does not change (and is still correct) when auto compaction runs with everything is fully compacted
    final CoordinatorRunStats stats = doCompactSegments(compactSegments);
    Assert.assertEquals(
        0,
        stats.get(Stats.Compaction.SUBMITTED_TASKS)
    );
    verifySnapshot(
        compactSegments,
        AutoCompactionSnapshot.AutoCompactionScheduleStatus.RUNNING,
        dataSourceName,
        0,
        TOTAL_BYTE_PER_DATASOURCE,
        40,
        0,
        TOTAL_INTERVAL_PER_DATASOURCE,
        1,
        0,
        // 12 segments was compressed before any auto compaction
        // 32 segments needs compaction which is now compacted into 16 segments (4 segments compacted into 2 segments each run)
        12 + 16,
        4
    );
  }

  @Test
  public void testMakeStatsWithDeactivatedDatasource()
  {
    final TestOverlordClient overlordClient = new TestOverlordClient(JSON_MAPPER);
    final CompactSegments compactSegments = new CompactSegments(SEARCH_POLICY, overlordClient);

    // Before any compaction, we do not have any snapshot of compactions
    Map<String, AutoCompactionSnapshot> autoCompactionSnapshots = compactSegments.getAutoCompactionSnapshot();
    Assert.assertEquals(0, autoCompactionSnapshots.size());

    for (int compactionRunCount = 0; compactionRunCount < 11; compactionRunCount++) {
      doCompactionAndAssertCompactSegmentStatistics(compactSegments, compactionRunCount);
    }
    // Test that stats does not change (and is still correct) when auto compaction runs with everything is fully compacted
    final CoordinatorRunStats stats = doCompactSegments(compactSegments);
    Assert.assertEquals(
        0,
        stats.get(Stats.Compaction.SUBMITTED_TASKS)
    );
    for (int i = 0; i < 3; i++) {
      verifySnapshot(
          compactSegments,
          AutoCompactionSnapshot.AutoCompactionScheduleStatus.RUNNING,
          DATA_SOURCE_PREFIX + i,
          0,
          TOTAL_BYTE_PER_DATASOURCE,
          40,
          0,
          TOTAL_INTERVAL_PER_DATASOURCE,
          1,
          0,
          TOTAL_SEGMENT_PER_DATASOURCE / 2,
          4
      );
    }

    // Deactivate one datasource (datasource 0 no longer exist in timeline)
    dataSources.getUsedSegmentsTimelinesPerDataSource()
               .remove(DATA_SOURCE_PREFIX + 0);

    // Test run auto compaction with one datasource deactivated
    // Snapshot should not contain deactivated datasource
    doCompactSegments(compactSegments);
    for (int i = 1; i < 3; i++) {
      verifySnapshot(
          compactSegments,
          AutoCompactionSnapshot.AutoCompactionScheduleStatus.RUNNING,
          DATA_SOURCE_PREFIX + i,
          0,
          TOTAL_BYTE_PER_DATASOURCE,
          40,
          0,
          TOTAL_INTERVAL_PER_DATASOURCE,
          1,
          0,
          TOTAL_SEGMENT_PER_DATASOURCE / 2,
          4
      );
    }

    Assert.assertEquals(2, compactSegments.getAutoCompactionSnapshot().size());
    Assert.assertTrue(compactSegments.getAutoCompactionSnapshot().containsKey(DATA_SOURCE_PREFIX + 1));
    Assert.assertTrue(compactSegments.getAutoCompactionSnapshot().containsKey(DATA_SOURCE_PREFIX + 2));
    Assert.assertFalse(compactSegments.getAutoCompactionSnapshot().containsKey(DATA_SOURCE_PREFIX + 0));
  }

  @Test
  public void testMakeStatsForDataSourceWithSkipped()
  {
    // Only test and validate for one datasource for simplicity.
    // This dataSource has three intervals skipped (3 intervals, 1200 byte, 12 segments skipped by auto compaction)
    // Note that these segment used to be 10 bytes each in other tests, we are increasing it to 100 bytes each here
    // so that they will be skipped by the auto compaction.
    String dataSourceName = DATA_SOURCE_PREFIX + 1;
    List<DataSegment> segments = new ArrayList<>();
    for (int j : new int[]{0, 1, 2, 3, 7, 8}) {
      for (int k = 0; k < 4; k++) {
        DataSegment beforeNoon = createSegment(dataSourceName, j, true, k);
        DataSegment afterNoon = createSegment(dataSourceName, j, false, k);
        if (j == 3) {
          // Make two intervals on this day skipped (two skipped intervals back-to-back)
          beforeNoon = beforeNoon.withSize(100);
          afterNoon = afterNoon.withSize(100);
        }
        if (j == 1) {
          // Make one interval on this day skipped
          afterNoon = afterNoon.withSize(100);
        }
        segments.add(beforeNoon);
        segments.add(afterNoon);
      }
    }

    dataSources = DataSourcesSnapshot.fromUsedSegments(segments, ImmutableMap.of());

    final TestOverlordClient overlordClient = new TestOverlordClient(JSON_MAPPER);
    final CompactSegments compactSegments = new CompactSegments(SEARCH_POLICY, overlordClient);

    // Before any compaction, we do not have any snapshot of compactions
    Map<String, AutoCompactionSnapshot> autoCompactionSnapshots = compactSegments.getAutoCompactionSnapshot();
    Assert.assertEquals(0, autoCompactionSnapshots.size());

    // 3 intervals, 1200 byte (each segment is 100 bytes), 12 segments will be skipped by auto compaction
    for (int compactionRunCount = 0; compactionRunCount < 8; compactionRunCount++) {
      // Do a cycle of auto compaction which creates one compaction task
      final CoordinatorRunStats stats = doCompactSegments(compactSegments);
      Assert.assertEquals(
          1,
          stats.get(Stats.Compaction.SUBMITTED_TASKS)
      );

      verifySnapshot(
          compactSegments,
          AutoCompactionSnapshot.AutoCompactionScheduleStatus.RUNNING,
          dataSourceName,
          // Minus 120 bytes accounting for the three skipped segments' original size
          TOTAL_BYTE_PER_DATASOURCE - 120 - 40 * (compactionRunCount + 1),
          40 * (compactionRunCount + 1),
          1240,
          TOTAL_INTERVAL_PER_DATASOURCE - 3 - (compactionRunCount + 1),
          (compactionRunCount + 1),
          4,
          TOTAL_SEGMENT_PER_DATASOURCE - 12 - 4 * (compactionRunCount + 1),
          4 + 2 * (compactionRunCount),
          16
      );
    }

    // Test that stats does not change (and is still correct) when auto compaction runs with everything is fully compacted
    final CoordinatorRunStats stats = doCompactSegments(compactSegments);
    Assert.assertEquals(
        0,
        stats.get(Stats.Compaction.SUBMITTED_TASKS)
    );
    verifySnapshot(
        compactSegments,
        AutoCompactionSnapshot.AutoCompactionScheduleStatus.RUNNING,
        dataSourceName,
        0,
        // Minus 120 bytes accounting for the three skipped segments' original size
        TOTAL_BYTE_PER_DATASOURCE - 120,
        1240,
        0,
        TOTAL_INTERVAL_PER_DATASOURCE - 3,
        4,
        0,
        16,
        16
    );
  }

  @Test
  public void testRunMultipleCompactionTaskSlots()
  {
    final TestOverlordClient overlordClient = new TestOverlordClient(JSON_MAPPER);
    final CompactSegments compactSegments = new CompactSegments(SEARCH_POLICY, overlordClient);

    final CoordinatorRunStats stats = doCompactSegments(compactSegments, 3);
    Assert.assertEquals(3, stats.get(Stats.Compaction.AVAILABLE_SLOTS));
    Assert.assertEquals(3, stats.get(Stats.Compaction.MAX_SLOTS));
    Assert.assertEquals(3, stats.get(Stats.Compaction.SUBMITTED_TASKS));
  }

  @Test
  public void testRunMultipleCompactionTaskSlotsWithUseAutoScaleSlotsOverMaxSlot()
  {
    int maxCompactionSlot = 3;
    Assert.assertTrue(maxCompactionSlot < MAXIMUM_CAPACITY_WITH_AUTO_SCALE);
    final TestOverlordClient overlordClient = new TestOverlordClient(JSON_MAPPER);
    final CompactSegments compactSegments = new CompactSegments(SEARCH_POLICY, overlordClient);
    final CoordinatorRunStats stats =
        doCompactSegments(compactSegments, createCompactionConfigs(), maxCompactionSlot, true);
    Assert.assertEquals(maxCompactionSlot, stats.get(Stats.Compaction.AVAILABLE_SLOTS));
    Assert.assertEquals(maxCompactionSlot, stats.get(Stats.Compaction.MAX_SLOTS));
    Assert.assertEquals(maxCompactionSlot, stats.get(Stats.Compaction.SUBMITTED_TASKS));
  }

  @Test
  public void testRunMultipleCompactionTaskSlotsWithUseAutoScaleSlotsUnderMaxSlot()
  {
    int maxCompactionSlot = 100;
    Assert.assertFalse(maxCompactionSlot < MAXIMUM_CAPACITY_WITH_AUTO_SCALE);
    final TestOverlordClient overlordClient = new TestOverlordClient(JSON_MAPPER);
    final CompactSegments compactSegments = new CompactSegments(SEARCH_POLICY, overlordClient);
    final CoordinatorRunStats stats =
        doCompactSegments(compactSegments, createCompactionConfigs(), maxCompactionSlot, true);
    Assert.assertEquals(MAXIMUM_CAPACITY_WITH_AUTO_SCALE, stats.get(Stats.Compaction.AVAILABLE_SLOTS));
    Assert.assertEquals(MAXIMUM_CAPACITY_WITH_AUTO_SCALE, stats.get(Stats.Compaction.MAX_SLOTS));
    Assert.assertEquals(MAXIMUM_CAPACITY_WITH_AUTO_SCALE, stats.get(Stats.Compaction.SUBMITTED_TASKS));
  }

  @Test
  public void testCompactWithoutGranularitySpec()
  {
    final OverlordClient mockClient = Mockito.mock(OverlordClient.class);
    final ArgumentCaptor<Object> payloadCaptor = setUpMockClient(mockClient);
    final CompactSegments compactSegments = new CompactSegments(SEARCH_POLICY, mockClient);
    final List<DataSourceCompactionConfig> compactionConfigs = new ArrayList<>();
    final String dataSource = DATA_SOURCE_PREFIX + 0;
    compactionConfigs.add(
        new DataSourceCompactionConfig(
            dataSource,
            0,
            500L,
            null,
            new Period("PT0H"), // smaller than segment interval
            new UserCompactionTaskQueryTuningConfig(
                null,
                null,
                null,
                null,
                null,
                partitionsSpec,
                null,
                null,
                null,
                null,
                null,
                3,
                null,
                null,
                null,
                null,
                null,
                null,
                null
            ),
            null,
            null,
            null,
            null,
            null,
            null
        )
    );
    doCompactSegments(compactSegments, compactionConfigs);

    ClientCompactionTaskQuery taskPayload = (ClientCompactionTaskQuery) payloadCaptor.getValue();

    Assert.assertEquals(
        Intervals.of("2017-01-09T12:00:00.000Z/2017-01-10T00:00:00.000Z"),
        taskPayload.getIoConfig().getInputSpec().getInterval()
    );
    Assert.assertNull(taskPayload.getGranularitySpec().getSegmentGranularity());
    Assert.assertNull(taskPayload.getGranularitySpec().getQueryGranularity());
    Assert.assertNull(taskPayload.getGranularitySpec().isRollup());
  }

  @Test
  public void testCompactWithNotNullIOConfig()
  {
    final OverlordClient mockClient = Mockito.mock(OverlordClient.class);
    final ArgumentCaptor<Object> payloadCaptor = setUpMockClient(mockClient);
    final CompactSegments compactSegments = new CompactSegments(SEARCH_POLICY, mockClient);
    final List<DataSourceCompactionConfig> compactionConfigs = new ArrayList<>();
    final String dataSource = DATA_SOURCE_PREFIX + 0;
    compactionConfigs.add(
        new DataSourceCompactionConfig(
            dataSource,
            0,
            500L,
            null,
            new Period("PT0H"), // smaller than segment interval
            new UserCompactionTaskQueryTuningConfig(
                null,
                null,
                null,
                null,
                null,
                partitionsSpec,
                null,
                null,
                null,
                null,
                null,
                3,
                null,
                null,
                null,
                null,
                null,
                null,
                null
            ),
            null,
            null,
            null,
            null,
            new UserCompactionTaskIOConfig(true),
            null
        )
    );
    doCompactSegments(compactSegments, compactionConfigs);
    ClientCompactionTaskQuery taskPayload = (ClientCompactionTaskQuery) payloadCaptor.getValue();
    Assert.assertTrue(taskPayload.getIoConfig().isDropExisting());
  }

  @Test
  public void testCompactWithNullIOConfig()
  {
    final OverlordClient mockClient = Mockito.mock(OverlordClient.class);
    final ArgumentCaptor<Object> payloadCaptor = setUpMockClient(mockClient);
    final CompactSegments compactSegments = new CompactSegments(SEARCH_POLICY, mockClient);
    final List<DataSourceCompactionConfig> compactionConfigs = new ArrayList<>();
    final String dataSource = DATA_SOURCE_PREFIX + 0;
    compactionConfigs.add(
        new DataSourceCompactionConfig(
            dataSource,
            0,
            500L,
            null,
            new Period("PT0H"), // smaller than segment interval
            new UserCompactionTaskQueryTuningConfig(
                null,
                null,
                null,
                null,
                null,
                partitionsSpec,
                null,
                null,
                null,
                null,
                null,
                3,
                null,
                null,
                null,
                null,
                null,
                null,
                null
            ),
            null,
            null,
            null,
            null,
            null,
            null
        )
    );
    doCompactSegments(compactSegments, compactionConfigs);
    ClientCompactionTaskQuery taskPayload = (ClientCompactionTaskQuery) payloadCaptor.getValue();
    Assert.assertEquals(BatchIOConfig.DEFAULT_DROP_EXISTING, taskPayload.getIoConfig().isDropExisting());
  }

  @Test
  public void testCompactWithGranularitySpec()
  {
    final OverlordClient mockClient = Mockito.mock(OverlordClient.class);
    final ArgumentCaptor<Object> payloadCaptor = setUpMockClient(mockClient);
    final CompactSegments compactSegments = new CompactSegments(SEARCH_POLICY, mockClient);
    final List<DataSourceCompactionConfig> compactionConfigs = new ArrayList<>();
    final String dataSource = DATA_SOURCE_PREFIX + 0;
    compactionConfigs.add(
        new DataSourceCompactionConfig(
            dataSource,
            0,
            500L,
            null,
            new Period("PT0H"), // smaller than segment interval
            new UserCompactionTaskQueryTuningConfig(
                null,
                null,
                null,
                null,
                null,
                partitionsSpec,
                null,
                null,
                null,
                null,
                null,
                3,
                null,
                null,
                null,
                null,
                null,
                null,
                null
            ),
            new UserCompactionTaskGranularityConfig(Granularities.YEAR, null, null),
            null,
            null,
            null,
            null,
            null
        )
    );
    doCompactSegments(compactSegments, compactionConfigs);

    ClientCompactionTaskQuery taskPayload = (ClientCompactionTaskQuery) payloadCaptor.getValue();

    // All segments is compact at the same time since we changed the segment granularity to YEAR and all segment
    // are within the same year
    Assert.assertEquals(
        ClientCompactionIntervalSpec.fromSegments(datasourceToSegments.get(dataSource), Granularities.YEAR),
        taskPayload.getIoConfig().getInputSpec()
    );

    ClientCompactionTaskGranularitySpec expectedGranularitySpec =
        new ClientCompactionTaskGranularitySpec(Granularities.YEAR, null, null);
    Assert.assertEquals(expectedGranularitySpec, taskPayload.getGranularitySpec());
  }

  @Test
  public void testCompactWithDimensionSpec()
  {
    final OverlordClient mockClient = Mockito.mock(OverlordClient.class);
    final ArgumentCaptor<Object> payloadCaptor = setUpMockClient(mockClient);
    final CompactSegments compactSegments = new CompactSegments(SEARCH_POLICY, mockClient);
    final List<DataSourceCompactionConfig> compactionConfigs = new ArrayList<>();
    final String dataSource = DATA_SOURCE_PREFIX + 0;
    compactionConfigs.add(
        new DataSourceCompactionConfig(
            dataSource,
            0,
            500L,
            null,
            new Period("PT0H"), // smaller than segment interval
            new UserCompactionTaskQueryTuningConfig(
                null,
                null,
                null,
                null,
                null,
                partitionsSpec,
                null,
                null,
                null,
                null,
                null,
                3,
                null,
                null,
                null,
                null,
                null,
                null,
                null
            ),
            null,
            new UserCompactionTaskDimensionsConfig(DimensionsSpec.getDefaultSchemas(ImmutableList.of("bar", "foo"))),
            null,
            null,
            null,
            null
        )
    );
    doCompactSegments(compactSegments, compactionConfigs);
    ClientCompactionTaskQuery taskPayload = (ClientCompactionTaskQuery) payloadCaptor.getValue();
    Assert.assertEquals(
        DimensionsSpec.getDefaultSchemas(ImmutableList.of("bar", "foo")),
        taskPayload.getDimensionsSpec().getDimensions()
    );
  }

  @Test
  public void testCompactWithoutDimensionSpec()
  {
    final OverlordClient mockClient = Mockito.mock(OverlordClient.class);
    final ArgumentCaptor<Object> payloadCaptor = setUpMockClient(mockClient);
    final CompactSegments compactSegments = new CompactSegments(SEARCH_POLICY, mockClient);
    final List<DataSourceCompactionConfig> compactionConfigs = new ArrayList<>();
    final String dataSource = DATA_SOURCE_PREFIX + 0;
    compactionConfigs.add(
        new DataSourceCompactionConfig(
            dataSource,
            0,
            500L,
            null,
            new Period("PT0H"), // smaller than segment interval
            new UserCompactionTaskQueryTuningConfig(
                null,
                null,
                null,
                null,
                null,
                partitionsSpec,
                null,
                null,
                null,
                null,
                null,
                3,
                null,
                null,
                null,
                null,
                null,
                null,
                null
            ),
            null,
            null,
            null,
            null,
            null,
            null
        )
    );
    doCompactSegments(compactSegments, compactionConfigs);
    ClientCompactionTaskQuery taskPayload = (ClientCompactionTaskQuery) payloadCaptor.getValue();
    Assert.assertNull(taskPayload.getDimensionsSpec());
  }

  @Test
  public void testCompactWithRollupInGranularitySpec()
  {
    final OverlordClient mockClient = Mockito.mock(OverlordClient.class);
    final ArgumentCaptor<Object> payloadCaptor = setUpMockClient(mockClient);
    final CompactSegments compactSegments = new CompactSegments(SEARCH_POLICY, mockClient);
    final List<DataSourceCompactionConfig> compactionConfigs = new ArrayList<>();
    final String dataSource = DATA_SOURCE_PREFIX + 0;
    compactionConfigs.add(
        new DataSourceCompactionConfig(
            dataSource,
            0,
            500L,
            null,
            new Period("PT0H"), // smaller than segment interval
            new UserCompactionTaskQueryTuningConfig(
                null,
                null,
                null,
                null,
                null,
                partitionsSpec,
                null,
                null,
                null,
                null,
                null,
                3,
                null,
                null,
                null,
                null,
                null,
                null,
                null
            ),
            new UserCompactionTaskGranularityConfig(Granularities.YEAR, null, true),
            null,
            null,
            null,
            null,
            null
        )
    );
    doCompactSegments(compactSegments, compactionConfigs);
    ClientCompactionTaskQuery taskPayload = (ClientCompactionTaskQuery) payloadCaptor.getValue();

    // All segments is compact at the same time since we changed the segment granularity to YEAR and all segment
    // are within the same year
    Assert.assertEquals(
        ClientCompactionIntervalSpec.fromSegments(datasourceToSegments.get(dataSource), Granularities.YEAR),
        taskPayload.getIoConfig().getInputSpec()
    );

    ClientCompactionTaskGranularitySpec expectedGranularitySpec =
        new ClientCompactionTaskGranularitySpec(Granularities.YEAR, null, true);
    Assert.assertEquals(expectedGranularitySpec, taskPayload.getGranularitySpec());
  }

  @Test
  public void testCompactWithGranularitySpecConflictWithActiveCompactionTask()
  {
    final String dataSource = DATA_SOURCE_PREFIX + 0;
    final String conflictTaskId = "taskIdDummy";
    final TaskStatusPlus runningConflictCompactionTask = new TaskStatusPlus(
        conflictTaskId,
        "groupId",
        "compact",
        DateTimes.EPOCH,
        DateTimes.EPOCH,
        TaskState.RUNNING,
        RunnerTaskState.RUNNING,
        -1L,
        TaskLocation.unknown(),
        dataSource,
        null
    );
    final TaskPayloadResponse runningConflictCompactionTaskPayload = new TaskPayloadResponse(
        conflictTaskId,
        new ClientCompactionTaskQuery(
            conflictTaskId,
            dataSource,
            new ClientCompactionIOConfig(
                new ClientCompactionIntervalSpec(
                    Intervals.of("2000/2099"),
                    "testSha256OfSortedSegmentIds"
                ),
                null
            ),
            null,
            new ClientCompactionTaskGranularitySpec(Granularities.DAY, null, null),
            null,
            null,
            null,
            null
        )
    );

    final OverlordClient mockClient = Mockito.mock(OverlordClient.class);
    final ArgumentCaptor<Object> payloadCaptor = ArgumentCaptor.forClass(Object.class);
    Mockito.when(mockClient.runTask(ArgumentMatchers.anyString(), payloadCaptor.capture()))
           .thenReturn(Futures.immediateFuture(null));
    Mockito.when(mockClient.taskStatuses(null, null, 0))
           .thenReturn(
               Futures.immediateFuture(
                   CloseableIterators.withEmptyBaggage(ImmutableList.of(runningConflictCompactionTask).iterator())));
    Mockito.when(mockClient.findLockedIntervals(ArgumentMatchers.any()))
           .thenReturn(Futures.immediateFuture(Collections.emptyMap()));
    Mockito.when(mockClient.cancelTask(conflictTaskId))
           .thenReturn(Futures.immediateFuture(null));
    Mockito.when(mockClient.getTotalWorkerCapacity())
           .thenReturn(Futures.immediateFuture(new IndexingTotalWorkerCapacityInfo(0, 0)));
    Mockito.when(mockClient.taskPayload(ArgumentMatchers.eq(conflictTaskId)))
           .thenReturn(Futures.immediateFuture(runningConflictCompactionTaskPayload));

    final CompactSegments compactSegments = new CompactSegments(SEARCH_POLICY, mockClient);
    final List<DataSourceCompactionConfig> compactionConfigs = new ArrayList<>();
    compactionConfigs.add(
        new DataSourceCompactionConfig(
            dataSource,
            0,
            500L,
            null,
            new Period("PT0H"), // smaller than segment interval
            new UserCompactionTaskQueryTuningConfig(
                null,
                null,
                null,
                null,
                null,
                partitionsSpec,
                null,
                null,
                null,
                null,
                null,
                3,
                null,
                null,
                null,
                null,
                null,
                null,
                null
            ),
            new UserCompactionTaskGranularityConfig(Granularities.YEAR, null, null),
            null,
            null,
            null,
            null,
            null
        )
    );
    doCompactSegments(compactSegments, compactionConfigs);
    // Verify that conflict task was canceled
    Mockito.verify(mockClient).cancelTask(conflictTaskId);
    // The active conflict task has interval of 2000/2099
    // Make sure that we do not skip interval of conflict task.
    // Since we cancel the task and will have to compact those intervals with the new segmentGranulartity
    ClientCompactionTaskQuery taskPayload = (ClientCompactionTaskQuery) payloadCaptor.getValue();

    // All segments is compact at the same time since we changed the segment granularity to YEAR and all segment
    // are within the same year
    Assert.assertEquals(
        ClientCompactionIntervalSpec.fromSegments(datasourceToSegments.get(dataSource), Granularities.YEAR),
        taskPayload.getIoConfig().getInputSpec()
    );

    ClientCompactionTaskGranularitySpec expectedGranularitySpec =
        new ClientCompactionTaskGranularitySpec(Granularities.YEAR, null, null);
    Assert.assertEquals(expectedGranularitySpec, taskPayload.getGranularitySpec());
  }

  @Test
  public void testRunParallelCompactionMultipleCompactionTaskSlots()
  {
    final TestOverlordClient overlordClient = new TestOverlordClient(JSON_MAPPER);
    final CompactSegments compactSegments = new CompactSegments(SEARCH_POLICY, overlordClient);

    final CoordinatorRunStats stats = doCompactSegments(compactSegments, createCompactionConfigs(2), 4);
    Assert.assertEquals(4, stats.get(Stats.Compaction.AVAILABLE_SLOTS));
    Assert.assertEquals(4, stats.get(Stats.Compaction.MAX_SLOTS));
    Assert.assertEquals(2, stats.get(Stats.Compaction.SUBMITTED_TASKS));
  }

  @Test
  public void testRunWithLockedIntervals()
  {
    final TestOverlordClient overlordClient = new TestOverlordClient(JSON_MAPPER);

    // Lock all intervals for dataSource_1 and dataSource_2
    final String datasource1 = DATA_SOURCE_PREFIX + 1;
    overlordClient.lockedIntervals
        .computeIfAbsent(datasource1, k -> new ArrayList<>())
        .add(Intervals.of("2017/2018"));

    final String datasource2 = DATA_SOURCE_PREFIX + 2;
    overlordClient.lockedIntervals
        .computeIfAbsent(datasource2, k -> new ArrayList<>())
        .add(Intervals.of("2017/2018"));

    // Lock all intervals but one for dataSource_0
    final String datasource0 = DATA_SOURCE_PREFIX + 0;
    overlordClient.lockedIntervals
        .computeIfAbsent(datasource0, k -> new ArrayList<>())
        .add(Intervals.of("2017-01-01T13:00:00Z/2017-02-01"));

    // Verify that locked intervals are skipped and only one compaction task
    // is submitted for dataSource_0
    CompactSegments compactSegments = new CompactSegments(SEARCH_POLICY, overlordClient);
    final CoordinatorRunStats stats =
        doCompactSegments(compactSegments, createCompactionConfigs(2), 4);
    Assert.assertEquals(1, stats.get(Stats.Compaction.SUBMITTED_TASKS));
    Assert.assertEquals(1, overlordClient.submittedCompactionTasks.size());

    final ClientCompactionTaskQuery compactionTask = overlordClient.submittedCompactionTasks.get(0);
    Assert.assertEquals(datasource0, compactionTask.getDataSource());
    Assert.assertEquals(
        Intervals.of("2017-01-01T00:00:00/2017-01-01T12:00:00"),
        compactionTask.getIoConfig().getInputSpec().getInterval()
    );
  }

  @Test
  public void testCompactWithTransformSpec()
  {
    NullHandling.initializeForTests();
    final OverlordClient mockClient = Mockito.mock(OverlordClient.class);
    final ArgumentCaptor<Object> payloadCaptor = setUpMockClient(mockClient);
    final CompactSegments compactSegments = new CompactSegments(SEARCH_POLICY, mockClient);
    final List<DataSourceCompactionConfig> compactionConfigs = new ArrayList<>();
    final String dataSource = DATA_SOURCE_PREFIX + 0;
    compactionConfigs.add(
        new DataSourceCompactionConfig(
            dataSource,
            0,
            500L,
            null,
            new Period("PT0H"), // smaller than segment interval
            new UserCompactionTaskQueryTuningConfig(
                null,
                null,
                null,
                null,
                null,
                partitionsSpec,
                null,
                null,
                null,
                null,
                null,
                3,
                null,
                null,
                null,
                null,
                null,
                null,
                null
            ),
            null,
            null,
            null,
            new UserCompactionTaskTransformConfig(new SelectorDimFilter("dim1", "foo", null)),
            null,
            null
        )
    );
    doCompactSegments(compactSegments, compactionConfigs);
    ClientCompactionTaskQuery taskPayload = (ClientCompactionTaskQuery) payloadCaptor.getValue();
    Assert.assertNotNull(taskPayload.getTransformSpec());
    Assert.assertEquals(new SelectorDimFilter("dim1", "foo", null), taskPayload.getTransformSpec().getFilter());
  }

  @Test
  public void testCompactWithoutCustomSpecs()
  {
    final OverlordClient mockClient = Mockito.mock(OverlordClient.class);
    final ArgumentCaptor<Object> payloadCaptor = setUpMockClient(mockClient);
    final CompactSegments compactSegments = new CompactSegments(SEARCH_POLICY, mockClient);
    final List<DataSourceCompactionConfig> compactionConfigs = new ArrayList<>();
    final String dataSource = DATA_SOURCE_PREFIX + 0;
    compactionConfigs.add(
        new DataSourceCompactionConfig(
            dataSource,
            0,
            500L,
            null,
            new Period("PT0H"), // smaller than segment interval
            new UserCompactionTaskQueryTuningConfig(
                null,
                null,
                null,
                null,
                null,
                partitionsSpec,
                null,
                null,
                null,
                null,
                null,
                3,
                null,
                null,
                null,
                null,
                null,
                null,
                null
            ),
            null,
            null,
            null,
            null,
            null,
            null
        )
    );
    doCompactSegments(compactSegments, compactionConfigs);
    ClientCompactionTaskQuery taskPayload = (ClientCompactionTaskQuery) payloadCaptor.getValue();
    Assert.assertNull(taskPayload.getTransformSpec());
    Assert.assertNull(taskPayload.getMetricsSpec());
  }

  @Test
  public void testCompactWithMetricsSpec()
  {
    NullHandling.initializeForTests();
    AggregatorFactory[] aggregatorFactories = new AggregatorFactory[] {new CountAggregatorFactory("cnt")};
    final OverlordClient mockClient = Mockito.mock(OverlordClient.class);
    final ArgumentCaptor<Object> payloadCaptor = setUpMockClient(mockClient);
    final CompactSegments compactSegments = new CompactSegments(SEARCH_POLICY, mockClient);
    final List<DataSourceCompactionConfig> compactionConfigs = new ArrayList<>();
    final String dataSource = DATA_SOURCE_PREFIX + 0;
    compactionConfigs.add(
        new DataSourceCompactionConfig(
            dataSource,
            0,
            500L,
            null,
            new Period("PT0H"), // smaller than segment interval
            new UserCompactionTaskQueryTuningConfig(
                null,
                null,
                null,
                null,
                null,
                partitionsSpec,
                null,
                null,
                null,
                null,
                null,
                3,
                null,
                null,
                null,
                null,
                null,
                null,
                null
            ),
            null,
            null,
            aggregatorFactories,
            null,
            null,
            null
        )
    );
    doCompactSegments(compactSegments, compactionConfigs);
    ClientCompactionTaskQuery taskPayload = (ClientCompactionTaskQuery) payloadCaptor.getValue();
    AggregatorFactory[] actual = taskPayload.getMetricsSpec();
    Assert.assertNotNull(actual);
    Assert.assertArrayEquals(aggregatorFactories, actual);
  }

  @Test
  public void testDetermineSegmentGranularityFromSegmentsToCompact()
  {
    String dataSourceName = DATA_SOURCE_PREFIX + 1;
    List<DataSegment> segments = new ArrayList<>();
    segments.add(
        new DataSegment(
            dataSourceName,
            Intervals.of("2017-01-01T00:00:00/2017-01-02T00:00:00"),
            "1",
            null,
            ImmutableList.of(),
            ImmutableList.of(),
            shardSpecFactory.apply(0, 2),
            0,
            10L
        )
    );
    segments.add(
        new DataSegment(
            dataSourceName,
            Intervals.of("2017-01-01T00:00:00/2017-01-02T00:00:00"),
            "1",
            null,
            ImmutableList.of(),
            ImmutableList.of(),
            shardSpecFactory.apply(1, 2),
            0,
            10L
        )
    );
    dataSources = DataSourcesSnapshot.fromUsedSegments(segments, ImmutableMap.of());

    final OverlordClient mockClient = Mockito.mock(OverlordClient.class);
    final ArgumentCaptor<Object> payloadCaptor = setUpMockClient(mockClient);
    final CompactSegments compactSegments = new CompactSegments(SEARCH_POLICY, mockClient);
    final List<DataSourceCompactionConfig> compactionConfigs = new ArrayList<>();
    compactionConfigs.add(
        new DataSourceCompactionConfig(
            dataSourceName,
            0,
            500L,
            null,
            new Period("PT0H"), // smaller than segment interval
            new UserCompactionTaskQueryTuningConfig(
                null,
                null,
                null,
                null,
                null,
                partitionsSpec,
                null,
                null,
                null,
                null,
                null,
                3,
                null,
                null,
                null,
                null,
                null,
                null,
                null
            ),
            null,
            null,
            null,
            null,
            null,
            null
        )
    );
    doCompactSegments(compactSegments, compactionConfigs);
    ClientCompactionTaskQuery taskPayload = (ClientCompactionTaskQuery) payloadCaptor.getValue();

    Assert.assertEquals(
        ClientCompactionIntervalSpec.fromSegments(segments, Granularities.DAY),
        taskPayload.getIoConfig().getInputSpec()
    );

    ClientCompactionTaskGranularitySpec expectedGranularitySpec =
        new ClientCompactionTaskGranularitySpec(Granularities.DAY, null, null);
    Assert.assertEquals(expectedGranularitySpec, taskPayload.getGranularitySpec());
  }

  @Test
  public void testDetermineSegmentGranularityFromSegmentGranularityInCompactionConfig()
  {
    String dataSourceName = DATA_SOURCE_PREFIX + 1;
    List<DataSegment> segments = new ArrayList<>();
    segments.add(
        new DataSegment(
            dataSourceName,
            Intervals.of("2017-01-01T00:00:00/2017-01-02T00:00:00"),
            "1",
            null,
            ImmutableList.of(),
            ImmutableList.of(),
            shardSpecFactory.apply(0, 2),
            0,
            10L
        )
    );
    segments.add(
        new DataSegment(
            dataSourceName,
            Intervals.of("2017-01-01T00:00:00/2017-01-02T00:00:00"),
            "1",
            null,
            ImmutableList.of(),
            ImmutableList.of(),
            shardSpecFactory.apply(1, 2),
            0,
            10L
        )
    );
    dataSources = DataSourcesSnapshot.fromUsedSegments(segments, ImmutableMap.of());

    final OverlordClient mockClient = Mockito.mock(OverlordClient.class);
    final ArgumentCaptor<Object> payloadCaptor = setUpMockClient(mockClient);
    final CompactSegments compactSegments = new CompactSegments(SEARCH_POLICY, mockClient);
    final List<DataSourceCompactionConfig> compactionConfigs = new ArrayList<>();
    compactionConfigs.add(
        new DataSourceCompactionConfig(
            dataSourceName,
            0,
            500L,
            null,
            new Period("PT0H"), // smaller than segment interval
            new UserCompactionTaskQueryTuningConfig(
                null,
                null,
                null,
                null,
                null,
                partitionsSpec,
                null,
                null,
                null,
                null,
                null,
                3,
                null,
                null,
                null,
                null,
                null,
                null,
                null
            ),
            new UserCompactionTaskGranularityConfig(Granularities.YEAR, null, null),
            null,
            null,
            null,
            null,
            null
        )
    );
    doCompactSegments(compactSegments, compactionConfigs);
    ClientCompactionTaskQuery taskPayload = (ClientCompactionTaskQuery) payloadCaptor.getValue();

    Assert.assertEquals(
        ClientCompactionIntervalSpec.fromSegments(segments, Granularities.YEAR),
        taskPayload.getIoConfig().getInputSpec()
    );

    ClientCompactionTaskGranularitySpec expectedGranularitySpec =
        new ClientCompactionTaskGranularitySpec(Granularities.YEAR, null, null);
    Assert.assertEquals(expectedGranularitySpec, taskPayload.getGranularitySpec());
  }

  @Test
  public void testCompactWithMetricsSpecShouldSetPreserveExistingMetricsTrue()
  {
    final OverlordClient mockClient = Mockito.mock(OverlordClient.class);
    final ArgumentCaptor<Object> payloadCaptor = setUpMockClient(mockClient);
    final CompactSegments compactSegments = new CompactSegments(SEARCH_POLICY, mockClient);
    final List<DataSourceCompactionConfig> compactionConfigs = new ArrayList<>();
    final String dataSource = DATA_SOURCE_PREFIX + 0;
    compactionConfigs.add(
        new DataSourceCompactionConfig(
            dataSource,
            0,
            500L,
            null,
            new Period("PT0H"), // smaller than segment interval
            new UserCompactionTaskQueryTuningConfig(
                null,
                null,
                null,
                null,
                null,
                partitionsSpec,
                null,
                null,
                null,
                null,
                null,
                3,
                null,
                null,
                null,
                null,
                null,
                null,
                null
            ),
            null,
            null,
            new AggregatorFactory[] {new CountAggregatorFactory("cnt")},
            null,
            null,
            null
        )
    );
    doCompactSegments(compactSegments, compactionConfigs);
    ClientCompactionTaskQuery taskPayload = (ClientCompactionTaskQuery) payloadCaptor.getValue();
    Assert.assertNotNull(taskPayload.getTuningConfig());
    Assert.assertNotNull(taskPayload.getTuningConfig().getAppendableIndexSpec());
    Assert.assertTrue(((OnheapIncrementalIndex.Spec) taskPayload.getTuningConfig()
                                                                .getAppendableIndexSpec()).isPreserveExistingMetrics());
  }

  @Test
  public void testCompactWithoutMetricsSpecShouldSetPreserveExistingMetricsFalse()
  {
    final OverlordClient mockClient = Mockito.mock(OverlordClient.class);
    final ArgumentCaptor<Object> payloadCaptor = setUpMockClient(mockClient);
    final CompactSegments compactSegments = new CompactSegments(SEARCH_POLICY, mockClient);
    final List<DataSourceCompactionConfig> compactionConfigs = new ArrayList<>();
    final String dataSource = DATA_SOURCE_PREFIX + 0;
    compactionConfigs.add(
        new DataSourceCompactionConfig(
            dataSource,
            0,
            500L,
            null,
            new Period("PT0H"), // smaller than segment interval
            new UserCompactionTaskQueryTuningConfig(
                null,
                null,
                null,
                null,
                null,
                partitionsSpec,
                null,
                null,
                null,
                null,
                null,
                3,
                null,
                null,
                null,
                null,
                null,
                null,
                null
            ),
            null,
            null,
            null,
            null,
            null,
            null
        )
    );
    doCompactSegments(compactSegments, compactionConfigs);
    ClientCompactionTaskQuery taskPayload = (ClientCompactionTaskQuery) payloadCaptor.getValue();
    Assert.assertNotNull(taskPayload.getTuningConfig());
    Assert.assertNotNull(taskPayload.getTuningConfig().getAppendableIndexSpec());
    Assert.assertFalse(((OnheapIncrementalIndex.Spec) taskPayload.getTuningConfig()
                                                                 .getAppendableIndexSpec()).isPreserveExistingMetrics());
  }

  private void verifySnapshot(
      CompactSegments compactSegments,
      AutoCompactionSnapshot.AutoCompactionScheduleStatus scheduleStatus,
      String dataSourceName,
      long expectedByteCountAwaitingCompaction,
      long expectedByteCountCompressed,
      long expectedByteCountSkipped,
      long expectedIntervalCountAwaitingCompaction,
      long expectedIntervalCountCompressed,
      long expectedIntervalCountSkipped,
      long expectedSegmentCountAwaitingCompaction,
      long expectedSegmentCountCompressed,
      long expectedSegmentCountSkipped
  )
  {
    Map<String, AutoCompactionSnapshot> autoCompactionSnapshots = compactSegments.getAutoCompactionSnapshot();
    AutoCompactionSnapshot snapshot = autoCompactionSnapshots.get(dataSourceName);
    Assert.assertEquals(dataSourceName, snapshot.getDataSource());
    Assert.assertEquals(scheduleStatus, snapshot.getScheduleStatus());
    Assert.assertEquals(expectedByteCountAwaitingCompaction, snapshot.getBytesAwaitingCompaction());
    Assert.assertEquals(expectedByteCountCompressed, snapshot.getBytesCompacted());
    Assert.assertEquals(expectedByteCountSkipped, snapshot.getBytesSkipped());
    Assert.assertEquals(expectedIntervalCountAwaitingCompaction, snapshot.getIntervalCountAwaitingCompaction());
    Assert.assertEquals(expectedIntervalCountCompressed, snapshot.getIntervalCountCompacted());
    Assert.assertEquals(expectedIntervalCountSkipped, snapshot.getIntervalCountSkipped());
    Assert.assertEquals(expectedSegmentCountAwaitingCompaction, snapshot.getSegmentCountAwaitingCompaction());
    Assert.assertEquals(expectedSegmentCountCompressed, snapshot.getSegmentCountCompacted());
    Assert.assertEquals(expectedSegmentCountSkipped, snapshot.getSegmentCountSkipped());
  }

  private void doCompactionAndAssertCompactSegmentStatistics(CompactSegments compactSegments, int compactionRunCount)
  {
    for (int dataSourceIndex = 0; dataSourceIndex < 3; dataSourceIndex++) {
      // One compaction task triggered
      final CoordinatorRunStats stats = doCompactSegments(compactSegments);
      Assert.assertEquals(
          1,
          stats.get(Stats.Compaction.SUBMITTED_TASKS)
      );
      // Note: Subsequent compaction run after the dataSource was compacted will show different numbers than
      // on the run it was compacted. For example, in a compaction run, if a dataSource had 4 segments compacted,
      // on the same compaction run the segment compressed count will be 4 but on subsequent run it might be 2
      // (assuming the 4 segments was compacted into 2 segments).
      for (int i = 0; i <= dataSourceIndex; i++) {
        // dataSource up to dataSourceIndex now compacted. Check that the stats match the expectedAfterCompaction values
        // This verify that dataSource which got slot to compact has correct statistics
        if (i != dataSourceIndex) {
          verifySnapshot(
              compactSegments,
              AutoCompactionSnapshot.AutoCompactionScheduleStatus.RUNNING,
              DATA_SOURCE_PREFIX + i,
              TOTAL_BYTE_PER_DATASOURCE - 40L * (compactionRunCount + 1),
              40L * (compactionRunCount + 1),
              40,
              TOTAL_INTERVAL_PER_DATASOURCE - (compactionRunCount + 1),
              (compactionRunCount + 1),
              1,
              TOTAL_SEGMENT_PER_DATASOURCE - 4L * (compactionRunCount + 1),
              2L * (compactionRunCount + 1),
              4
          );
        } else {
          verifySnapshot(
              compactSegments,
              AutoCompactionSnapshot.AutoCompactionScheduleStatus.RUNNING,
              DATA_SOURCE_PREFIX + i,
              TOTAL_BYTE_PER_DATASOURCE - 40L * (compactionRunCount + 1),
              40L * (compactionRunCount + 1),
              40,
              TOTAL_INTERVAL_PER_DATASOURCE - (compactionRunCount + 1),
              (compactionRunCount + 1),
              1,
              TOTAL_SEGMENT_PER_DATASOURCE - 4L * (compactionRunCount + 1),
              2L * compactionRunCount + 4,
              4
          );
        }
      }
      for (int i = dataSourceIndex + 1; i < 3; i++) {
        // dataSource after dataSourceIndex is not yet compacted. Check that the stats match the expectedBeforeCompaction values
        // This verify that dataSource that ran out of slot has correct statistics
        verifySnapshot(
            compactSegments,
            AutoCompactionSnapshot.AutoCompactionScheduleStatus.RUNNING,
            DATA_SOURCE_PREFIX + i,
            TOTAL_BYTE_PER_DATASOURCE - 40L * compactionRunCount,
            40L * compactionRunCount,
            40,
            TOTAL_INTERVAL_PER_DATASOURCE - compactionRunCount,
            compactionRunCount,
            1,
            TOTAL_SEGMENT_PER_DATASOURCE - 4L * compactionRunCount,
            2L * compactionRunCount,
            4
        );
      }
    }
  }

  private CoordinatorRunStats doCompactSegments(CompactSegments compactSegments)
  {
    return doCompactSegments(compactSegments, (Integer) null);
  }

  private CoordinatorRunStats doCompactSegments(CompactSegments compactSegments, @Nullable Integer numCompactionTaskSlots)
  {
    return doCompactSegments(compactSegments, createCompactionConfigs(), numCompactionTaskSlots);
  }

  private void doCompactSegments(
      CompactSegments compactSegments,
      List<DataSourceCompactionConfig> compactionConfigs
  )
  {
    doCompactSegments(compactSegments, compactionConfigs, null);
  }

  private CoordinatorRunStats doCompactSegments(
      CompactSegments compactSegments,
      List<DataSourceCompactionConfig> compactionConfigs,
      @Nullable Integer numCompactionTaskSlots
  )
  {
    return doCompactSegments(compactSegments, compactionConfigs, numCompactionTaskSlots, false);
  }

  private CoordinatorRunStats doCompactSegments(
      CompactSegments compactSegments,
      List<DataSourceCompactionConfig> compactionConfigs,
      @Nullable Integer numCompactionTaskSlots,
      boolean useAutoScaleSlots
  )
  {
    DruidCoordinatorRuntimeParams params = DruidCoordinatorRuntimeParams
        .newBuilder(DateTimes.nowUtc())
        .withDataSourcesSnapshot(dataSources)
        .withCompactionConfig(
            new CoordinatorCompactionConfig(
                compactionConfigs,
                numCompactionTaskSlots == null ? null : 1.0, // 100% when numCompactionTaskSlots is not null
                numCompactionTaskSlots,
                useAutoScaleSlots
            )
        )
        .build();
    return compactSegments.run(params).getCoordinatorStats();
  }

  private void assertCompactSegments(
      CompactSegments compactSegments,
      Interval expectedInterval,
      int expectedRemainingSegments,
      int expectedCompactTaskCount,
      Supplier<String> expectedVersionSupplier
  )
  {
    for (int i = 0; i < 3; i++) {
      final CoordinatorRunStats stats = doCompactSegments(compactSegments);
      Assert.assertEquals(
          expectedCompactTaskCount,
          stats.get(Stats.Compaction.SUBMITTED_TASKS)
      );

      // If expectedRemainingSegments is positive, we count the number of datasources
      // which have that many segments waiting for compaction. Otherwise, we count
      // all the datasources in the coordinator stats
      final AtomicInteger numDatasources = new AtomicInteger();
      stats.forEachStat(
          (stat, rowKey, value) -> {
            if (stat.equals(Stats.Compaction.PENDING_BYTES)
                && (expectedRemainingSegments <= 0 || value == expectedRemainingSegments)) {
              numDatasources.incrementAndGet();
            }
          }
      );

      if (expectedRemainingSegments > 0) {
        Assert.assertEquals(i + 1, numDatasources.get());
      } else {
        Assert.assertEquals(2 - i, numDatasources.get());
      }
    }

    final Map<String, SegmentTimeline> dataSourceToTimeline
        = dataSources.getUsedSegmentsTimelinesPerDataSource();
    for (int i = 0; i < 3; i++) {
      final String dataSource = DATA_SOURCE_PREFIX + i;
      List<TimelineObjectHolder<String, DataSegment>> holders = dataSourceToTimeline.get(dataSource).lookup(expectedInterval);
      Assert.assertEquals(1, holders.size());
      List<PartitionChunk<DataSegment>> chunks = Lists.newArrayList(holders.get(0).getObject());
      Assert.assertEquals(2, chunks.size());
      final String expectedVersion = expectedVersionSupplier.get();
      for (PartitionChunk<DataSegment> chunk : chunks) {
        Assert.assertEquals(expectedInterval, chunk.getObject().getInterval());
        Assert.assertEquals(expectedVersion, chunk.getObject().getVersion());
      }
    }
  }

  private void assertLastSegmentNotCompacted(CompactSegments compactSegments)
  {
    // Segments of the latest interval should not be compacted
    final Map<String, SegmentTimeline> dataSourceToTimeline
        = dataSources.getUsedSegmentsTimelinesPerDataSource();
    for (int i = 0; i < 3; i++) {
      final String dataSource = DATA_SOURCE_PREFIX + i;
      final Interval interval = Intervals.of(StringUtils.format("2017-01-09T12:00:00/2017-01-10"));
      List<TimelineObjectHolder<String, DataSegment>> holders = dataSourceToTimeline.get(dataSource).lookup(interval);
      Assert.assertEquals(1, holders.size());
      for (TimelineObjectHolder<String, DataSegment> holder : holders) {
        List<PartitionChunk<DataSegment>> chunks = Lists.newArrayList(holder.getObject());
        Assert.assertEquals(4, chunks.size());
        for (PartitionChunk<DataSegment> chunk : chunks) {
          DataSegment segment = chunk.getObject();
          Assert.assertEquals(interval, segment.getInterval());
          Assert.assertEquals("version", segment.getVersion());
        }
      }
    }

    // Emulating realtime dataSource
    final String dataSource = DATA_SOURCE_PREFIX + 0;
    addMoreData(dataSource, 9);

    CoordinatorRunStats stats = doCompactSegments(compactSegments);
    Assert.assertEquals(
        1,
        stats.get(Stats.Compaction.SUBMITTED_TASKS)
    );

    addMoreData(dataSource, 10);

    stats = doCompactSegments(compactSegments);
    Assert.assertEquals(
        1,
        stats.get(Stats.Compaction.SUBMITTED_TASKS)
    );
  }

  private void addMoreData(String dataSource, int day)
  {
    final SegmentTimeline timeline
        = dataSources.getUsedSegmentsTimelinesPerDataSource().get(dataSource);
    for (int i = 0; i < 2; i++) {
      DataSegment newSegment = createSegment(dataSource, day, true, i);
      timeline.add(
          newSegment.getInterval(),
          newSegment.getVersion(),
          newSegment.getShardSpec().createChunk(newSegment)
      );
      newSegment = createSegment(dataSource, day, false, i);
      timeline.add(
          newSegment.getInterval(),
          newSegment.getVersion(),
          newSegment.getShardSpec().createChunk(newSegment)
      );
    }
  }

  private List<DataSourceCompactionConfig> createCompactionConfigs()
  {
    return createCompactionConfigs(null);
  }

  private List<DataSourceCompactionConfig> createCompactionConfigs(@Nullable Integer maxNumConcurrentSubTasks)
  {
    final List<DataSourceCompactionConfig> compactionConfigs = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      final String dataSource = DATA_SOURCE_PREFIX + i;
      compactionConfigs.add(
          new DataSourceCompactionConfig(
              dataSource,
              0,
              50L,
              null,
              new Period("PT1H"), // smaller than segment interval
              new UserCompactionTaskQueryTuningConfig(
                  null,
                  null,
                  null,
                  null,
                  null,
                  partitionsSpec,
                  null,
                  null,
                  null,
                  null,
                  null,
                  maxNumConcurrentSubTasks,
                  null,
                  null,
                  null,
                  null,
                  null,
                  null,
                  null
              ),
              null,
              null,
              null,
              null,
              null,
              null
          )
      );
    }
    return compactionConfigs;
  }

  private class TestOverlordClient extends NoopOverlordClient
  {
    private final ObjectMapper jsonMapper;

    // Map from Task Id to the intervals locked by that task
    private final Map<String, List<Interval>> lockedIntervals = new HashMap<>();

    // List of submitted compaction tasks for verification in the tests
    private final List<ClientCompactionTaskQuery> submittedCompactionTasks = new ArrayList<>();

    private int compactVersionSuffix = 0;

    private TestOverlordClient(ObjectMapper jsonMapper)
    {
      this.jsonMapper = jsonMapper;
    }

    @Override
    public ListenableFuture<URI> findCurrentLeader()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public ListenableFuture<Void> runTask(String taskId, Object taskObject)
    {
      final ClientTaskQuery taskQuery = jsonMapper.convertValue(taskObject, ClientTaskQuery.class);
      if (!(taskQuery instanceof ClientCompactionTaskQuery)) {
        throw new IAE("Cannot run non-compaction task");
      }
      final ClientCompactionTaskQuery compactionTaskQuery = (ClientCompactionTaskQuery) taskQuery;
      submittedCompactionTasks.add(compactionTaskQuery);

      final Interval intervalToCompact = compactionTaskQuery.getIoConfig().getInputSpec().getInterval();
      final SegmentTimeline timeline = dataSources.getUsedSegmentsTimelinesPerDataSource()
                                                  .get(compactionTaskQuery.getDataSource());
      final List<DataSegment> segments = timeline.lookup(intervalToCompact)
                                                 .stream()
                                                 .flatMap(holder -> Streams.sequentialStreamFrom(holder.getObject()))
                                                 .map(PartitionChunk::getObject)
                                                 .collect(Collectors.toList());

      compactSegments(timeline, segments, compactionTaskQuery);
      return Futures.immediateFuture(null);
    }


    @Override
    public ListenableFuture<Map<String, List<Interval>>> findLockedIntervals(
        List<LockFilterPolicy> lockFilterPolicies
    )
    {
      return Futures.immediateFuture(lockedIntervals);
    }

    @Override
    public ListenableFuture<CloseableIterator<TaskStatusPlus>> taskStatuses(
        @Nullable String state,
        @Nullable String dataSource,
        @Nullable Integer maxCompletedTasks
    )
    {
      return Futures.immediateFuture(CloseableIterators.withEmptyBaggage(Collections.emptyIterator()));
    }

    @Override
    public ListenableFuture<IndexingTotalWorkerCapacityInfo> getTotalWorkerCapacity()
    {
      return Futures.immediateFuture(new IndexingTotalWorkerCapacityInfo(5, 10));
    }

    private void compactSegments(
        VersionedIntervalTimeline<String, DataSegment> timeline,
        List<DataSegment> segments,
        ClientCompactionTaskQuery clientCompactionTaskQuery
    )
    {
      Preconditions.checkArgument(segments.size() > 1);
      DateTime minStart = DateTimes.MAX, maxEnd = DateTimes.MIN;
      for (DataSegment segment : segments) {
        if (segment.getInterval().getStart().compareTo(minStart) < 0) {
          minStart = segment.getInterval().getStart();
        }
        if (segment.getInterval().getEnd().compareTo(maxEnd) > 0) {
          maxEnd = segment.getInterval().getEnd();
        }
      }
      Interval compactInterval = new Interval(minStart, maxEnd);
      segments.forEach(
          segment -> timeline.remove(
              segment.getInterval(),
              segment.getVersion(),
              segment.getShardSpec().createChunk(segment)
          )
      );
      final String version = "newVersion_" + compactVersionSuffix++;
      final long segmentSize = segments.stream().mapToLong(DataSegment::getSize).sum() / 2;
      final PartitionsSpec compactionPartitionsSpec;
      if (clientCompactionTaskQuery.getTuningConfig().getPartitionsSpec() instanceof DynamicPartitionsSpec) {
        compactionPartitionsSpec = new DynamicPartitionsSpec(
            clientCompactionTaskQuery.getTuningConfig().getPartitionsSpec().getMaxRowsPerSegment(),
            ((DynamicPartitionsSpec) clientCompactionTaskQuery.getTuningConfig().getPartitionsSpec()).getMaxTotalRowsOr(Long.MAX_VALUE)
        );
      } else {
        compactionPartitionsSpec = clientCompactionTaskQuery.getTuningConfig().getPartitionsSpec();
      }

      Map<String, Object> transformSpec = null;
      try {
        if (clientCompactionTaskQuery.getTransformSpec() != null) {
          transformSpec = jsonMapper.readValue(
              jsonMapper.writeValueAsString(new TransformSpec(clientCompactionTaskQuery.getTransformSpec()
                                                                                       .getFilter(), null)),
              new TypeReference<Map<String, Object>>()
              {
              }
          );
        }
      }
      catch (JsonProcessingException e) {
        throw new IAE("Invalid Json payload");
      }

      List<Object> metricsSpec = null;
      if (clientCompactionTaskQuery.getMetricsSpec() != null) {
        metricsSpec = jsonMapper.convertValue(clientCompactionTaskQuery.getMetricsSpec(), new TypeReference<List<Object>>() {});
      }

      for (int i = 0; i < 2; i++) {
        DataSegment compactSegment = new DataSegment(
            segments.get(0).getDataSource(),
            compactInterval,
            version,
            null,
            segments.get(0).getDimensions(),
            segments.get(0).getMetrics(),
            shardSpecFactory.apply(i, 2),
            new CompactionState(
                compactionPartitionsSpec,
                clientCompactionTaskQuery.getDimensionsSpec() == null ? null : new DimensionsSpec(
                    clientCompactionTaskQuery.getDimensionsSpec().getDimensions()
                ),
                metricsSpec,
                transformSpec,
                ImmutableMap.of(
                    "bitmap",
                    ImmutableMap.of("type", "roaring"),
                    "dimensionCompression",
                    "lz4",
                    "metricCompression",
                    "lz4",
                    "longEncoding",
                    "longs"
                ),
                ImmutableMap.of()
            ),
            1,
            segmentSize
        );

        timeline.add(
            compactInterval,
            compactSegment.getVersion(),
            compactSegment.getShardSpec().createChunk(compactSegment)
        );
      }
    }
  }

  public static class StaticUtilsTest
  {
    @Test
    public void testIsParalleModeNullTuningConfigReturnFalse()
    {
      Assert.assertFalse(CompactSegments.isParallelMode(null));
    }

    @Test
    public void testIsParallelModeNullPartitionsSpecReturnFalse()
    {
      ClientCompactionTaskQueryTuningConfig tuningConfig = Mockito.mock(ClientCompactionTaskQueryTuningConfig.class);
      Mockito.when(tuningConfig.getPartitionsSpec()).thenReturn(null);
      Assert.assertFalse(CompactSegments.isParallelMode(tuningConfig));
    }

    @Test
    public void testIsParallelModeNonRangePartitionVaryingMaxNumConcurrentSubTasks()
    {
      ClientCompactionTaskQueryTuningConfig tuningConfig = Mockito.mock(ClientCompactionTaskQueryTuningConfig.class);
      Mockito.when(tuningConfig.getPartitionsSpec()).thenReturn(Mockito.mock(PartitionsSpec.class));

      Mockito.when(tuningConfig.getMaxNumConcurrentSubTasks()).thenReturn(null);
      Assert.assertFalse(CompactSegments.isParallelMode(tuningConfig));

      Mockito.when(tuningConfig.getMaxNumConcurrentSubTasks()).thenReturn(1);
      Assert.assertFalse(CompactSegments.isParallelMode(tuningConfig));

      Mockito.when(tuningConfig.getMaxNumConcurrentSubTasks()).thenReturn(2);
      Assert.assertTrue(CompactSegments.isParallelMode(tuningConfig));
    }

    @Test
    public void testIsParallelModeRangePartitionVaryingMaxNumConcurrentSubTasks()
    {
      ClientCompactionTaskQueryTuningConfig tuningConfig = Mockito.mock(ClientCompactionTaskQueryTuningConfig.class);
      Mockito.when(tuningConfig.getPartitionsSpec()).thenReturn(Mockito.mock(SingleDimensionPartitionsSpec.class));

      Mockito.when(tuningConfig.getMaxNumConcurrentSubTasks()).thenReturn(null);
      Assert.assertFalse(CompactSegments.isParallelMode(tuningConfig));

      Mockito.when(tuningConfig.getMaxNumConcurrentSubTasks()).thenReturn(1);
      Assert.assertTrue(CompactSegments.isParallelMode(tuningConfig));

      Mockito.when(tuningConfig.getMaxNumConcurrentSubTasks()).thenReturn(2);
      Assert.assertTrue(CompactSegments.isParallelMode(tuningConfig));
    }

    @Test
    public void testFindMaxNumTaskSlotsUsedByOneCompactionTaskWhenIsParallelMode()
    {
      ClientCompactionTaskQueryTuningConfig tuningConfig = Mockito.mock(ClientCompactionTaskQueryTuningConfig.class);
      Mockito.when(tuningConfig.getPartitionsSpec()).thenReturn(Mockito.mock(PartitionsSpec.class));
      Mockito.when(tuningConfig.getMaxNumConcurrentSubTasks()).thenReturn(2);
      Assert.assertEquals(3, CompactSegments.findMaxNumTaskSlotsUsedByOneCompactionTask(tuningConfig));
    }

    @Test
    public void testFindMaxNumTaskSlotsUsedByOneCompactionTaskWhenIsSequentialMode()
    {
      ClientCompactionTaskQueryTuningConfig tuningConfig = Mockito.mock(ClientCompactionTaskQueryTuningConfig.class);
      Mockito.when(tuningConfig.getPartitionsSpec()).thenReturn(Mockito.mock(PartitionsSpec.class));
      Mockito.when(tuningConfig.getMaxNumConcurrentSubTasks()).thenReturn(1);
      Assert.assertEquals(1, CompactSegments.findMaxNumTaskSlotsUsedByOneCompactionTask(tuningConfig));
    }
  }

  private static ArgumentCaptor<Object> setUpMockClient(final OverlordClient mockClient)
  {
    final ArgumentCaptor<Object> payloadCaptor = ArgumentCaptor.forClass(Object.class);
    Mockito.when(mockClient.taskStatuses(null, null, 0))
           .thenReturn(Futures.immediateFuture(CloseableIterators.withEmptyBaggage(Collections.emptyIterator())));
    Mockito.when(mockClient.findLockedIntervals(ArgumentMatchers.any()))
           .thenReturn(Futures.immediateFuture(Collections.emptyMap()));
    Mockito.when(mockClient.getTotalWorkerCapacity())
           .thenReturn(Futures.immediateFuture(new IndexingTotalWorkerCapacityInfo(0, 0)));
    Mockito.when(mockClient.runTask(ArgumentMatchers.anyString(), payloadCaptor.capture()))
           .thenReturn(Futures.immediateFuture(null));
    return payloadCaptor;
  }
}
