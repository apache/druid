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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.apache.druid.client.DruidServer;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.balancer.RandomBalancerStrategy;
import org.apache.druid.server.coordinator.loading.SegmentLoadQueueManager;
import org.apache.druid.server.coordinator.loading.TestLoadQueuePeon;
import org.apache.druid.server.coordinator.simulate.TestSegmentsMetadataManager;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.Dimension;
import org.apache.druid.server.coordinator.stats.RowKey;
import org.apache.druid.server.coordinator.stats.Stats;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentTimeline;
import org.apache.druid.timeline.partition.TombstoneShardSpec;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;

public class MarkEternityTombstonesAsUnusedTest
{
  private final String ds1 = "foo";
  private final String ds2 = "bar";

  // The verbose variable names follow the convention for readability in the tests:
  // datasource name - shard spec type - interval - version
  private final DataSegment ds1NumberedSegmentMinToMaxV0 = DataSegment.builder().dataSource(ds1)
                                                                      .interval(Intervals.ETERNITY)
                                                                      .version("0")
                                                                      .size(0)
                                                                      .build();

  private final DataSegment ds1TombstoneSegmentMinToMaxV1 = DataSegment.builder().dataSource(ds1)
                                                                       .shardSpec(new TombstoneShardSpec())
                                                                       .interval(Intervals.ETERNITY)
                                                                       .version("1")
                                                                       .size(0)
                                                                       .build();

  private final DataSegment ds1TombstoneSegmentMinTo2000V1 = DataSegment.builder().dataSource(ds1)
                                                                        .shardSpec(new TombstoneShardSpec())
                                                                        .interval(new Interval(DateTimes.MIN, DateTimes.of("2000")))
                                                                        .version("1")
                                                                        .size(0)
                                                                        .build();

  private final DataSegment ds1NumberedSegment2000To2001V1 = DataSegment.builder().dataSource(ds1)
                                                                        .interval(Intervals.of("2000/2001"))
                                                                        .version("1")
                                                                        .size(0)
                                                                        .build();

  private final DataSegment ds1TombstoneSegment2001ToMaxV1 = DataSegment.builder().dataSource(ds1)
                                                                        .shardSpec(new TombstoneShardSpec())
                                                                        .interval(new Interval(DateTimes.of("2001"), DateTimes.MAX))
                                                                        .version("1")
                                                                        .size(0)
                                                                        .build();

  final DataSegment ds1TombstoneSegmentMinTo2000V2 = ds1TombstoneSegmentMinTo2000V1.withVersion("2");
  final DataSegment ds1TombstoneSegment2001ToMaxV2 = ds1TombstoneSegment2001ToMaxV1.withVersion("2");

  private final DataSegment ds2TombstoneSegment1995To2005V0 = DataSegment.builder().dataSource(ds2)
                                                                         .shardSpec(new TombstoneShardSpec())
                                                                         .interval(Intervals.of("1995/2005"))
                                                                         .version("0")
                                                                         .size(0)
                                                                         .build();

  private final DataSegment ds2TombstoneSegmentMinTo2000V1 = DataSegment.builder().dataSource(ds2)
                                                                        .shardSpec(new TombstoneShardSpec())
                                                                        .interval(new Interval(DateTimes.MIN, DateTimes.of("2000")))
                                                                        .version("1")
                                                                        .size(0)
                                                                        .build();

  private final DataSegment ds2NumberedSegment3000To4000V1 = DataSegment.builder().dataSource(ds2)
                                                                        .interval(Intervals.of("3000/4000"))
                                                                        .version("1")
                                                                        .size(0)
                                                                        .build();

  private final DataSegment ds2TombstoneSegment4000ToMaxV1 = DataSegment.builder().dataSource(ds2)
                                                                        .shardSpec(new TombstoneShardSpec())
                                                                        .interval(new Interval(DateTimes.of("4000"), DateTimes.MAX))
                                                                        .version("1")
                                                                        .size(0)
                                                                        .build();

  private final DataSegment ds2TombstoneSegment4000To4001V1 = DataSegment.builder().dataSource(ds2)
                                                                        .shardSpec(new TombstoneShardSpec())
                                                                        .interval(new Interval(DateTimes.of("4000"), DateTimes.of("4001")))
                                                                        .version("1")
                                                                        .size(0)
                                                                        .build();

  private final DataSegment ds2NumberedSegment1999To2500V2 = DataSegment.builder().dataSource(ds2)
                                                                        .interval(Intervals.of("1999/2500"))
                                                                        .version("2")
                                                                        .size(0)
                                                                        .build();


  /**
   * An old generation tombstone with 1 core partition instead of the default 0.
   */
  private final DataSegment ds2TombstoneSegment4000ToMaxV1With1CorePartition = ds2TombstoneSegment4000ToMaxV1.withShardSpec(
      new TombstoneShardSpec() {
        @Override
        @JsonProperty("partitions")
        public int getNumCorePartitions()
        {
          return 1;
        }
      });


  private TestSegmentsMetadataManager segmentsMetadataManager;

  @Before
  public void setup()
  {
    segmentsMetadataManager = new TestSegmentsMetadataManager();
  }

  /**
   * Half-inifinity tombstones overlapping with overshadowed segments shouldn't be marked as unused.
   */
  @Test
  public void testCandidateTombstonesWithUsedOvershadowedSegments()
  {
    final ImmutableList<DataSegment> allUsedSegments = ImmutableList.of(
        ds1NumberedSegmentMinToMaxV0,
        ds1TombstoneSegmentMinTo2000V1,
        ds1NumberedSegment2000To2001V1,
        ds1TombstoneSegment2001ToMaxV1
    );
    final ImmutableList<DataSegment> expectedUsedSegments = ImmutableList.copyOf(allUsedSegments);
    final DruidCoordinatorRuntimeParams params = initializeServerAndGetParams(allUsedSegments);

    final SegmentTimeline timeline = segmentsMetadataManager.getSnapshotOfDataSourcesWithAllUsedSegments()
                                                            .getUsedSegmentsTimelinesPerDataSource()
                                                            .get(ds1);

    // Verify that the half-infinity tombstone is overshadowed and everything else is not
    Assert.assertTrue(timeline.isOvershadowed(ds1NumberedSegmentMinToMaxV0));
    Assert.assertFalse(timeline.isOvershadowed(ds1TombstoneSegmentMinTo2000V1));
    Assert.assertFalse(timeline.isOvershadowed(ds1NumberedSegment2000To2001V1));
    Assert.assertFalse(timeline.isOvershadowed(ds1TombstoneSegment2001ToMaxV1));

    runEternityTombstonesDutyAndVerify(params, allUsedSegments, expectedUsedSegments);
  }

  /**
   * Inifinity tombstones overlapping with overshadowed segments shouldn't be marked as unused.
   */
  @Test
  public void testCandidateTombstonesWithUsedOvershadowedSegments2()
  {
    final ImmutableList<DataSegment> allUsedSegments = ImmutableList.of(
        ds1NumberedSegmentMinToMaxV0,
        ds1TombstoneSegmentMinToMaxV1
    );
    final ImmutableList<DataSegment> expectedUsedSegments = ImmutableList.copyOf(allUsedSegments);
    final DruidCoordinatorRuntimeParams params = initializeServerAndGetParams(allUsedSegments);

    final SegmentTimeline ds1Timeline = segmentsMetadataManager.getSnapshotOfDataSourcesWithAllUsedSegments()
                                                               .getUsedSegmentsTimelinesPerDataSource()
                                                               .get(ds1);

    Assert.assertTrue(ds1Timeline.isOvershadowed(ds1NumberedSegmentMinToMaxV0));
    Assert.assertFalse(ds1Timeline.isOvershadowed(ds1TombstoneSegmentMinToMaxV1));

    runEternityTombstonesDutyAndVerify(params, allUsedSegments, expectedUsedSegments);
  }

  /**
   * Half-inifinity tombstones that don't overlap with an overshadowed segment should be marked as unused.
   */
  @Test
  public void testCandidateTombstonesWithNoUsedOvershadowedSegments()
  {
    final ImmutableList<DataSegment> allUsedSegments = ImmutableList.of(
        ds1TombstoneSegmentMinTo2000V1,
        ds1NumberedSegment2000To2001V1,
        ds1TombstoneSegment2001ToMaxV1
    );
    final ImmutableList<DataSegment> expectedUsedSegments = ImmutableList.of(
        ds1NumberedSegment2000To2001V1
    );
    final DruidCoordinatorRuntimeParams params = initializeServerAndGetParams(allUsedSegments);

    SegmentTimeline timeline = segmentsMetadataManager.getSnapshotOfDataSourcesWithAllUsedSegments()
                                                      .getUsedSegmentsTimelinesPerDataSource()
                                                      .get(ds1);

    Assert.assertFalse(timeline.isOvershadowed(ds1TombstoneSegmentMinTo2000V1));
    Assert.assertFalse(timeline.isOvershadowed(ds1NumberedSegment2000To2001V1));
    Assert.assertFalse(timeline.isOvershadowed(ds1TombstoneSegmentMinTo2000V2));

    runEternityTombstonesDutyAndVerify(params, allUsedSegments, expectedUsedSegments);
  }

  /**
   * Full-infinity tombstones that has no overlap should be marked as unused.
   */
  @Test
  public void testCandidateTombstonesWithNoUsedOvershadowedSegments2()
  {
    final ImmutableList<DataSegment> allUsedSegments = ImmutableList.of(
        ds1TombstoneSegmentMinToMaxV1
    );
    final ImmutableList<DataSegment> expectedUsedSegments = ImmutableList.of();
    final DruidCoordinatorRuntimeParams params = initializeServerAndGetParams(allUsedSegments);

    final SegmentTimeline ds1Timeline = segmentsMetadataManager.getSnapshotOfDataSourcesWithAllUsedSegments()
                                                               .getUsedSegmentsTimelinesPerDataSource()
                                                               .get(ds1);

    Assert.assertFalse(ds1Timeline.isOvershadowed(ds1TombstoneSegmentMinToMaxV1));

    runEternityTombstonesDutyAndVerify(params, allUsedSegments, expectedUsedSegments);
  }

  /**
   * Half-inifinity tombstones that overlap with overshadowed used segments shouldn't be marked as unused.
   */
  @Test
  public void testCandiateTombstonesWithManyOvershadowedSegments()
  {
    final ImmutableList<DataSegment> allUsedSegments = ImmutableList.of(
        ds1TombstoneSegmentMinTo2000V1,
        ds1NumberedSegment2000To2001V1,
        ds1TombstoneSegment2001ToMaxV1,
        ds1TombstoneSegmentMinTo2000V2,
        ds1TombstoneSegment2001ToMaxV2
    );
    final ImmutableList<DataSegment> expectedUsedSegments = ImmutableList.copyOf(allUsedSegments);

    final DruidCoordinatorRuntimeParams params = initializeServerAndGetParams(allUsedSegments);

    SegmentTimeline timeline = segmentsMetadataManager.getSnapshotOfDataSourcesWithAllUsedSegments()
                                                      .getUsedSegmentsTimelinesPerDataSource()
                                                      .get(ds1);

    Assert.assertTrue(timeline.isOvershadowed(ds1TombstoneSegmentMinTo2000V1));
    Assert.assertTrue(timeline.isOvershadowed(ds1TombstoneSegment2001ToMaxV1));
    Assert.assertFalse(timeline.isOvershadowed(ds1NumberedSegment2000To2001V1));
    Assert.assertFalse(timeline.isOvershadowed(ds1TombstoneSegmentMinTo2000V2));
    Assert.assertFalse(timeline.isOvershadowed(ds1TombstoneSegment2001ToMaxV2));

    runEternityTombstonesDutyAndVerify(params, allUsedSegments, expectedUsedSegments);
  }

  /**
   * <p>
   * Datasource 1 has the following half-infinity tombstones:
   * <li> {@link #ds1TombstoneSegmentMinTo2000V1} is overshadowed by {@link #ds1TombstoneSegmentMinTo2000V2}, so cannot be marked as unused. </li>
   * <li> {@link #ds1TombstoneSegmentMinTo2000V2} overlaps with {@link #ds1TombstoneSegmentMinTo2000V1}, so cannot be marked as unused. </li>
   * <li> {@link #ds1TombstoneSegment2001ToMaxV1} doesn't overlap with any other segment and can be marked as unused. </li>
   *
   * Note that {@link #ds1TombstoneSegmentMinTo2000V1} will be marked as unused by {@link MarkOvershadowedSegmentsAsUnused} duty
   * and then subsequently {@link #ds1TombstoneSegmentMinTo2000V2} will be marked as unused by the {@link MarkEternityTombstonesAsUnused}
   * duty eventually.
   * </p>
   *
   * <p>
   * Datasource 2 has half eternity tombstones that don't overlap with any other segment, so both can be removed.
   * </p>
   */
  @Test
  public void testCandidateTombstonesInMultipleDatasources()
  {
    final ImmutableList<DataSegment> allUsedSegments = ImmutableList.of(
        ds1TombstoneSegmentMinTo2000V1,
        ds1NumberedSegment2000To2001V1,
        ds1TombstoneSegment2001ToMaxV1,
        ds1TombstoneSegmentMinTo2000V2,
        ds2TombstoneSegmentMinTo2000V1,
        ds2NumberedSegment3000To4000V1,
        ds2TombstoneSegment4000ToMaxV1
    );

    final ImmutableList<DataSegment> expectedUsedSegments = ImmutableList.of(
        ds1TombstoneSegmentMinTo2000V1,
        ds1NumberedSegment2000To2001V1,
        ds1TombstoneSegmentMinTo2000V2,
        ds2NumberedSegment3000To4000V1
    );
    final DruidCoordinatorRuntimeParams params = initializeServerAndGetParams(allUsedSegments);

    SegmentTimeline ds1Timeline = segmentsMetadataManager.getSnapshotOfDataSourcesWithAllUsedSegments()
                                                         .getUsedSegmentsTimelinesPerDataSource()
                                                         .get(ds1);
    Assert.assertTrue(ds1Timeline.isOvershadowed(ds1TombstoneSegmentMinTo2000V1));
    Assert.assertFalse(ds1Timeline.isOvershadowed(ds1NumberedSegment2000To2001V1));
    Assert.assertFalse(ds1Timeline.isOvershadowed(ds1TombstoneSegment2001ToMaxV1));
    Assert.assertFalse(ds1Timeline.isOvershadowed(ds1TombstoneSegmentMinTo2000V2));

    SegmentTimeline ds2Timeline = segmentsMetadataManager.getSnapshotOfDataSourcesWithAllUsedSegments()
                                                         .getUsedSegmentsTimelinesPerDataSource()
                                                         .get(ds2);
    Assert.assertFalse(ds2Timeline.isOvershadowed(ds2TombstoneSegmentMinTo2000V1));
    Assert.assertFalse(ds2Timeline.isOvershadowed(ds2NumberedSegment3000To4000V1));
    Assert.assertFalse(ds2Timeline.isOvershadowed(ds2TombstoneSegment4000ToMaxV1));

    runEternityTombstonesDutyAndVerify(params, allUsedSegments, expectedUsedSegments);
  }

  /**
   * Half-inifinity tombstones that partially overlaps with other segments (not overshadowed) can still
   * be marked as used.
   */
  @Test
  public void testCandidateTombstonesWithPartiallyOverlappingSegment()
  {
    final ImmutableList<DataSegment> allUsedSegments = ImmutableList.of(
        ds2TombstoneSegment1995To2005V0,
        ds2TombstoneSegmentMinTo2000V1,
        ds2NumberedSegment3000To4000V1,
        ds2TombstoneSegment4000ToMaxV1
    );

    final ImmutableList<DataSegment> expectedUsedSegments = ImmutableList.of(
        ds2TombstoneSegment1995To2005V0,
        ds2NumberedSegment3000To4000V1
    );
    final DruidCoordinatorRuntimeParams params = initializeServerAndGetParams(allUsedSegments);

    final SegmentTimeline ds2Timeline = segmentsMetadataManager.getSnapshotOfDataSourcesWithAllUsedSegments()
                                                               .getUsedSegmentsTimelinesPerDataSource()
                                                               .get(ds2);

    Assert.assertFalse(ds2Timeline.isOvershadowed(ds2TombstoneSegment1995To2005V0));
    Assert.assertFalse(ds2Timeline.isOvershadowed(ds2TombstoneSegmentMinTo2000V1));
    Assert.assertFalse(ds2Timeline.isOvershadowed(ds2NumberedSegment3000To4000V1));
    Assert.assertFalse(ds2Timeline.isOvershadowed(ds2TombstoneSegment4000ToMaxV1));

    runEternityTombstonesDutyAndVerify(params, allUsedSegments, expectedUsedSegments);
  }

  /**
   * Half-inifinity tombstones that partially overlaps with other used segments would still be considered as used and
   * non-overshadowed and can be marked as unused.
   */
  @Test
  public void testCandidateTombstonesWithPartiallyOverlappingHigherVersionUsedSegment()
  {
    final ImmutableList<DataSegment> allUsedSegments = ImmutableList.of(
        ds2TombstoneSegmentMinTo2000V1,
        ds2NumberedSegment3000To4000V1,
        ds2TombstoneSegment4000ToMaxV1,
        ds2NumberedSegment1999To2500V2
    );

    final ImmutableList<DataSegment> expectedUsedSegments = ImmutableList.of(
        ds2NumberedSegment3000To4000V1,
        ds2NumberedSegment1999To2500V2
    );

    final DruidCoordinatorRuntimeParams params = initializeServerAndGetParams(allUsedSegments);

    final SegmentTimeline ds2Timeline = segmentsMetadataManager.getSnapshotOfDataSourcesWithAllUsedSegments()
                                                               .getUsedSegmentsTimelinesPerDataSource()
                                                               .get(ds2);
    Assert.assertFalse(ds2Timeline.isOvershadowed(ds2TombstoneSegmentMinTo2000V1));
    Assert.assertFalse(ds2Timeline.isOvershadowed(ds2NumberedSegment3000To4000V1));
    Assert.assertFalse(ds2Timeline.isOvershadowed(ds2TombstoneSegment4000ToMaxV1));
    Assert.assertFalse(ds2Timeline.isOvershadowed(ds2NumberedSegment1999To2500V2));

    runEternityTombstonesDutyAndVerify(params, allUsedSegments, expectedUsedSegments);
  }

  /**
   * Finite-interval tombstones shouldn't be marked as unused.
   */
  @Test
  public void testFiniteIntervalTombstone()
  {
    final ImmutableList<DataSegment> allUsedSegments = ImmutableList.of(
        ds2TombstoneSegment4000To4001V1
    );
    final ImmutableList<DataSegment> expectedUsedSegments = ImmutableList.of(
        ds2TombstoneSegment4000To4001V1
    );
    final DruidCoordinatorRuntimeParams params = initializeServerAndGetParams(allUsedSegments);

    final SegmentTimeline ds2Timeline = segmentsMetadataManager.getSnapshotOfDataSourcesWithAllUsedSegments()
                                                               .getUsedSegmentsTimelinesPerDataSource()
                                                               .get(ds2);

    Assert.assertFalse(ds2Timeline.isOvershadowed(ds2TombstoneSegment4000To4001V1));

    runEternityTombstonesDutyAndVerify(params, allUsedSegments, expectedUsedSegments);
  }

  /**
   * Tombstones with 1 core partition i.e., {@link TombstoneShardSpec#getNumCorePartitions()} == 1  shouldn't be
   * marked as unused.
   */
  @Test
  public void testTombstoneWith1CorePartition()
  {
    final ImmutableList<DataSegment> allUsedSegments = ImmutableList.of(
        ds2TombstoneSegment4000ToMaxV1With1CorePartition
    );
    final ImmutableList<DataSegment> expectedUsedSegments = ImmutableList.of(
        ds2TombstoneSegment4000ToMaxV1With1CorePartition
    );
    final DruidCoordinatorRuntimeParams params = initializeServerAndGetParams(allUsedSegments);

    final SegmentTimeline ds2Timeline = segmentsMetadataManager.getSnapshotOfDataSourcesWithAllUsedSegments()
                                                               .getUsedSegmentsTimelinesPerDataSource()
                                                               .get(ds2);

    Assert.assertFalse(ds2Timeline.isOvershadowed(ds2TombstoneSegment4000ToMaxV1With1CorePartition));

    runEternityTombstonesDutyAndVerify(params, allUsedSegments, expectedUsedSegments);
  }

  private DruidCoordinatorRuntimeParams initializeServerAndGetParams(final ImmutableList<DataSegment> segments)
  {
    final DruidServer druidServer = new DruidServer("", "", "", 0L, ServerType.fromString("broker"), "", 0);
    for (final DataSegment segment : segments) {
      segmentsMetadataManager.addSegment(segment);
      druidServer.addDataSegment(segment);
    }

    final DruidCluster druidCluster = DruidCluster
        .builder()
        .add(new ServerHolder(druidServer.toImmutableDruidServer(), new TestLoadQueuePeon()))
        .build();

    final DruidCoordinatorRuntimeParams params = DruidCoordinatorRuntimeParams
        .newBuilder(DateTimes.nowUtc())
        .withDataSourcesSnapshot(
            segmentsMetadataManager.getSnapshotOfDataSourcesWithAllUsedSegments()
        )
        .withDruidCluster(druidCluster)
        .withDynamicConfigs(
            CoordinatorDynamicConfig.builder().withMarkSegmentAsUnusedDelayMillis(0).build()
        )
        .withBalancerStrategy(new RandomBalancerStrategy())
        .withSegmentAssignerUsing(new SegmentLoadQueueManager(null, null))
        .build();

    return params;
  }

  private void runEternityTombstonesDutyAndVerify(
      DruidCoordinatorRuntimeParams params,
      final ImmutableList<DataSegment> allUsedSegments,
      final ImmutableList<DataSegment> expectedUsedSegments
  )
  {
    params = new MarkEternityTombstonesAsUnused(segmentsMetadataManager::markSegmentsAsUnused).run(params);

    final Set<DataSegment> actualUsedSegments = Sets.newHashSet(segmentsMetadataManager.iterateAllUsedSegments());

    Assert.assertEquals(expectedUsedSegments.size(), actualUsedSegments.size());
    Assert.assertTrue(actualUsedSegments.containsAll(expectedUsedSegments));

    final CoordinatorRunStats runStats = params.getCoordinatorStats();
    Assert.assertEquals(
        allUsedSegments.size() - expectedUsedSegments.size(),
        runStats.get(Stats.Segments.UNNEEDED_ETERNITY_TOMBSTONE, RowKey.of(Dimension.DATASOURCE, ds1)) +
        runStats.get(Stats.Segments.UNNEEDED_ETERNITY_TOMBSTONE, RowKey.of(Dimension.DATASOURCE, ds2))
    );
  }
}
