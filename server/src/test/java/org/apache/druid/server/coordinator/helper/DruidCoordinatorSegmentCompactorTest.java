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

package org.apache.druid.server.coordinator.helper;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.druid.client.DataSourcesSnapshot;
import org.apache.druid.client.indexing.ClientCompactQueryTuningConfig;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.client.indexing.NoopIndexingServiceClient;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.server.coordinator.CoordinatorCompactionConfig;
import org.apache.druid.server.coordinator.CoordinatorRuntimeParamsTestHelpers;
import org.apache.druid.server.coordinator.CoordinatorStats;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.timeline.CompactionState;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.apache.druid.timeline.partition.ShardSpec;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class DruidCoordinatorSegmentCompactorTest
{
  private static final String DATA_SOURCE_PREFIX = "dataSource_";

  private final IndexingServiceClient indexingServiceClient = new NoopIndexingServiceClient()
  {
    private int compactVersionSuffix = 0;
    private int idSuffix = 0;

    @Override
    public String compactSegments(
        List<DataSegment> segments,
        int compactionTaskPriority,
        ClientCompactQueryTuningConfig tuningConfig,
        Map<String, Object> context
    )
    {
      Preconditions.checkArgument(segments.size() > 1);
      Collections.sort(segments);
      Interval compactInterval = new Interval(
          segments.get(0).getInterval().getStart(),
          segments.get(segments.size() - 1).getInterval().getEnd()
      );
      final VersionedIntervalTimeline<String, DataSegment> timeline = dataSources.get(segments.get(0).getDataSource());
      segments.forEach(
          segment -> timeline.remove(
              segment.getInterval(),
              segment.getVersion(),
              segment.getShardSpec().createChunk(segment)
          )
      );
      final String version = "newVersion_" + compactVersionSuffix++;
      final long segmentSize = segments.stream().mapToLong(DataSegment::getSize).sum() / 2;
      for (int i = 0; i < 2; i++) {
        DataSegment compactSegment = new DataSegment(
            segments.get(0).getDataSource(),
            compactInterval,
            version,
            null,
            segments.get(0).getDimensions(),
            segments.get(0).getMetrics(),
            new NumberedShardSpec(i, 0),
            new CompactionState(
                new DynamicPartitionsSpec(
                    tuningConfig.getMaxRowsPerSegment(),
                    tuningConfig.getMaxTotalRowsOr(Long.MAX_VALUE)
                ),
                ImmutableMap.of(
                    "bitmap",
                    ImmutableMap.of("type", "concise"),
                    "dimensionCompression",
                    "lz4",
                    "metricCompression",
                    "lz4",
                    "longEncoding",
                    "longs"
                )
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

      return "task_" + idSuffix++;
    }

    @Override
    public List<TaskStatusPlus> getActiveTasks()
    {
      return Collections.emptyList();
    }

    @Override
    public int getTotalWorkerCapacity()
    {
      return 10;
    }
  };

  private Map<String, VersionedIntervalTimeline<String, DataSegment>> dataSources;

  @Before
  public void setup()
  {
    List<DataSegment> segments = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      final String dataSource = DATA_SOURCE_PREFIX + i;
      for (int j : new int[] {0, 1, 2, 3, 7, 8}) {
        for (int k = 0; k < 4; k++) {
          segments.add(createSegment(dataSource, j, true, k));
          segments.add(createSegment(dataSource, j, false, k));
        }
      }
    }
    dataSources = DataSourcesSnapshot
        .fromUsedSegments(segments, ImmutableMap.of())
        .getUsedSegmentsTimelinesPerDataSource();
  }

  private static DataSegment createSegment(String dataSource, int startDay, boolean beforeNoon, int partition)
  {
    final ShardSpec shardSpec = new NumberedShardSpec(partition, 2);
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
        ImmutableList.of(),
        ImmutableList.of(),
        shardSpec,
        0,
        10L
    );
  }

  @Test
  public void testRun()
  {
    final DruidCoordinatorSegmentCompactor compactor = new DruidCoordinatorSegmentCompactor(
        new DefaultObjectMapper(),
        indexingServiceClient
    );

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
        compactor,
        Intervals.of("2017-01-%02dT00:00:00/2017-01-%02dT12:00:00", 9, 9),
        expectedRemainingSegments,
        expectedCompactTaskCount,
        expectedVersionSupplier
    );
    expectedRemainingSegments -= 40;
    assertCompactSegments(
        compactor,
        Intervals.of("2017-01-%02dT12:00:00/2017-01-%02dT00:00:00", 8, 9),
        expectedRemainingSegments,
        expectedCompactTaskCount,
        expectedVersionSupplier
    );

    // compact for 2017-01-07T12:00:00.000Z/2017-01-08T12:00:00.000Z
    expectedRemainingSegments -= 40;
    assertCompactSegments(
        compactor,
        Intervals.of("2017-01-%02dT00:00:00/2017-01-%02dT12:00:00", 8, 8),
        expectedRemainingSegments,
        expectedCompactTaskCount,
        expectedVersionSupplier
    );
    expectedRemainingSegments -= 40;
    assertCompactSegments(
        compactor,
        Intervals.of("2017-01-%02dT12:00:00/2017-01-%02dT00:00:00", 4, 5),
        expectedRemainingSegments,
        expectedCompactTaskCount,
        expectedVersionSupplier
    );

    for (int endDay = 4; endDay > 1; endDay -= 1) {
      expectedRemainingSegments -= 40;
      assertCompactSegments(
          compactor,
          Intervals.of("2017-01-%02dT00:00:00/2017-01-%02dT12:00:00", endDay, endDay),
          expectedRemainingSegments,
          expectedCompactTaskCount,
          expectedVersionSupplier
      );
      expectedRemainingSegments -= 40;
      assertCompactSegments(
          compactor,
          Intervals.of("2017-01-%02dT12:00:00/2017-01-%02dT00:00:00", endDay - 1, endDay),
          expectedRemainingSegments,
          expectedCompactTaskCount,
          expectedVersionSupplier
      );
    }

    assertLastSegmentNotCompacted(compactor);
  }

  private CoordinatorStats runCompactor(DruidCoordinatorSegmentCompactor compactor)
  {
    DruidCoordinatorRuntimeParams params = CoordinatorRuntimeParamsTestHelpers
        .newBuilder()
        .withUsedSegmentsTimelinesPerDataSourceInTest(dataSources)
        .withCompactionConfig(CoordinatorCompactionConfig.from(createCompactionConfigs()))
        .build();
    return compactor.run(params).getCoordinatorStats();
  }

  private void assertCompactSegments(
      DruidCoordinatorSegmentCompactor compactor,
      Interval expectedInterval,
      int expectedRemainingSegments,
      int expectedCompactTaskCount,
      Supplier<String> expectedVersionSupplier
  )
  {
    for (int i = 0; i < 3; i++) {
      final CoordinatorStats stats = runCompactor(compactor);
      Assert.assertEquals(
          expectedCompactTaskCount,
          stats.getGlobalStat(DruidCoordinatorSegmentCompactor.COMPACT_TASK_COUNT)
      );

      // One of dataSource is compacted
      if (expectedRemainingSegments > 0) {
        // If expectedRemainingSegments is positive, we check how many dataSources have the segments waiting for
        // compaction.
        long numDataSourceOfExpectedRemainingSegments = stats
            .getDataSources(DruidCoordinatorSegmentCompactor.SEGMENT_SIZE_WAIT_COMPACT)
            .stream()
            .mapToLong(ds -> stats.getDataSourceStat(DruidCoordinatorSegmentCompactor.SEGMENT_SIZE_WAIT_COMPACT, ds))
            .filter(stat -> stat == expectedRemainingSegments)
            .count();
        Assert.assertEquals(i + 1, numDataSourceOfExpectedRemainingSegments);
      } else {
        // Otherwise, we check how many dataSources are in the coordinator stats.
        Assert.assertEquals(
            2 - i,
            stats.getDataSources(DruidCoordinatorSegmentCompactor.SEGMENT_SIZE_WAIT_COMPACT).size()
        );
      }
    }

    for (int i = 0; i < 3; i++) {
      final String dataSource = DATA_SOURCE_PREFIX + i;
      List<TimelineObjectHolder<String, DataSegment>> holders = dataSources.get(dataSource).lookup(expectedInterval);
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

  private void assertLastSegmentNotCompacted(DruidCoordinatorSegmentCompactor compactor)
  {
    // Segments of the latest interval should not be compacted
    for (int i = 0; i < 3; i++) {
      final String dataSource = DATA_SOURCE_PREFIX + i;
      final Interval interval = Intervals.of(StringUtils.format("2017-01-09T12:00:00/2017-01-10"));
      List<TimelineObjectHolder<String, DataSegment>> holders = dataSources.get(dataSource).lookup(interval);
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

    CoordinatorStats stats = runCompactor(compactor);
    Assert.assertEquals(
        1,
        stats.getGlobalStat(DruidCoordinatorSegmentCompactor.COMPACT_TASK_COUNT)
    );

    addMoreData(dataSource, 10);

    stats = runCompactor(compactor);
    Assert.assertEquals(
        1,
        stats.getGlobalStat(DruidCoordinatorSegmentCompactor.COMPACT_TASK_COUNT)
    );
  }

  private void addMoreData(String dataSource, int day)
  {
    for (int i = 0; i < 2; i++) {
      DataSegment newSegment = createSegment(dataSource, day, true, i);
      dataSources.get(dataSource).add(
          newSegment.getInterval(),
          newSegment.getVersion(),
          newSegment.getShardSpec().createChunk(newSegment)
      );
      newSegment = createSegment(dataSource, day, false, i);
      dataSources.get(dataSource).add(
          newSegment.getInterval(),
          newSegment.getVersion(),
          newSegment.getShardSpec().createChunk(newSegment)
      );
    }
  }

  private static List<DataSourceCompactionConfig> createCompactionConfigs()
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
              null,
              null
          )
      );
    }
    return compactionConfigs;
  }
}
