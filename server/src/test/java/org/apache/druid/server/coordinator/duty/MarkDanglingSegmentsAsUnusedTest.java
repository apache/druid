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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Set;

@RunWith(JUnitParamsRunner.class)
public class MarkDanglingSegmentsAsUnusedTest
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

  private final DataSegment ds1TombstoneSegmentMinTo2000V1 = DataSegment.builder().dataSource(ds1)
                                                                        .shardSpec(new TombstoneShardSpec())
                                                                        .interval(Intervals.of("%s/%s", Intervals.ETERNITY.getStart(), 2000))
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
                                                                        .interval(Intervals.of("%s/%s", 2001, Intervals.ETERNITY.getEnd()))
                                                                        .version("1")
                                                                        .size(0)
                                                                        .build();

  final DataSegment ds1TombstoneSegmentMinTo2000V2 = ds1TombstoneSegmentMinTo2000V1.withVersion("2");
  final DataSegment ds1TombstoneSegment2001ToMaxV2 = ds1TombstoneSegment2001ToMaxV1.withVersion("2");


  private final DataSegment ds2TombstoneSegment1995To1996V0 = DataSegment.builder().dataSource(ds2)
                                                                        .shardSpec(new TombstoneShardSpec())
                                                                        .interval(Intervals.of("1995/1996"))
                                                                        .version("0")
                                                                        .size(0)
                                                                        .build();

  private final DataSegment ds2TombstoneSegment1995To2005V0 = DataSegment.builder().dataSource(ds2)
                                                                         .shardSpec(new TombstoneShardSpec())
                                                                         .interval(Intervals.of("1995/2005"))
                                                                         .version("0")
                                                                         .size(0)
                                                                         .build();

  private final DataSegment ds2TombstoneSegmentMinTo2000V1 = DataSegment.builder().dataSource(ds2)
                                                                        .shardSpec(new TombstoneShardSpec())
                                                                        .interval(Intervals.of("%s/%s", Intervals.ETERNITY.getStart(), 2000))
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
                                                                        .interval(Intervals.of("%s/%s", 4000, Intervals.ETERNITY.getEnd()))
                                                                        .version("1")
                                                                        .size(0)
                                                                        .build();

  private final DataSegment ds2NumberedSegment1999To2500V2 = DataSegment.builder().dataSource(ds2)
                                                                        .interval(Intervals.of("1999/2500"))
                                                                        .version("2")
                                                                        .size(0)
                                                                        .build();

  private TestSegmentsMetadataManager segmentsMetadataManager;

  @Before
  public void setup()
  {
    segmentsMetadataManager = new TestSegmentsMetadataManager();
  }

  @Test
  @Parameters({"historical", "broker"})
  public void testDanglingTombstonesWithOvershadowedSegment(final String serverType)
  {
    final ImmutableList<DataSegment> allUsedSegments = ImmutableList.of(
        ds1NumberedSegmentMinToMaxV0,
        ds1TombstoneSegmentMinTo2000V1,
        ds1NumberedSegment2000To2001V1,
        ds1TombstoneSegment2001ToMaxV1
    );
    final ImmutableList<DataSegment> expectedUsedSegments = ImmutableList.copyOf(allUsedSegments);
    DruidCoordinatorRuntimeParams params = initializeServerAndGetParams(serverType, allUsedSegments);

    SegmentTimeline timeline = segmentsMetadataManager.getSnapshotOfDataSourcesWithAllUsedSegments()
                                                      .getUsedSegmentsTimelinesPerDataSource()
                                                      .get(ds1);

    // Verify that the eternity segment is overshadowed and everything else is not
    Assert.assertTrue(timeline.isOvershadowed(ds1NumberedSegmentMinToMaxV0));
    Assert.assertFalse(timeline.isOvershadowed(ds1TombstoneSegmentMinTo2000V1));
    Assert.assertFalse(timeline.isOvershadowed(ds1NumberedSegment2000To2001V1));
    Assert.assertFalse(timeline.isOvershadowed(ds1TombstoneSegment2001ToMaxV1));

    runDanglingTombstonesDutyAndVerify(params, allUsedSegments, expectedUsedSegments);
  }

  @Test
  @Parameters({"historical", "broker"})
  public void testDanglingTombstonesWithNoOvershadowedSegments(final String serverType)
  {
    final ImmutableList<DataSegment> allUsedSegments = ImmutableList.of(
        ds1TombstoneSegmentMinTo2000V1,
        ds1NumberedSegment2000To2001V1,
        ds1TombstoneSegment2001ToMaxV1
    );
    final ImmutableList<DataSegment> expectedUsedSegments = ImmutableList.of(
        ds1NumberedSegment2000To2001V1
    );
    DruidCoordinatorRuntimeParams params = initializeServerAndGetParams(serverType, allUsedSegments);

    SegmentTimeline timeline = segmentsMetadataManager.getSnapshotOfDataSourcesWithAllUsedSegments()
                                                      .getUsedSegmentsTimelinesPerDataSource()
                                                      .get(ds1);

    Assert.assertFalse(timeline.isOvershadowed(ds1TombstoneSegmentMinTo2000V1));
    Assert.assertFalse(timeline.isOvershadowed(ds1NumberedSegment2000To2001V1));
    Assert.assertFalse(timeline.isOvershadowed(ds1TombstoneSegmentMinTo2000V2));

    runDanglingTombstonesDutyAndVerify(params, allUsedSegments, expectedUsedSegments);
  }

  @Test
  @Parameters({"historical", "broker"})
  public void testOvershadowedDanglingTombstones(final String serverType)
  {
    final ImmutableList<DataSegment> allUsedSegments = ImmutableList.of(
        ds1TombstoneSegmentMinTo2000V1,
        ds1NumberedSegment2000To2001V1,
        ds1TombstoneSegment2001ToMaxV1,
        ds1TombstoneSegmentMinTo2000V2,
        ds1TombstoneSegment2001ToMaxV2
    );
    final ImmutableList<DataSegment> expectedUsedSegments = ImmutableList.copyOf(allUsedSegments);

    DruidCoordinatorRuntimeParams params = initializeServerAndGetParams(serverType, allUsedSegments);

    SegmentTimeline timeline = segmentsMetadataManager.getSnapshotOfDataSourcesWithAllUsedSegments()
                                                      .getUsedSegmentsTimelinesPerDataSource()
                                                      .get(ds1);

    // Verify that the dangling segments are overshadowed, so they should not be removed by the dangling
    // segments duty; rather it'd be cleaned up by the MarkOvershadowedSegmentsAsUnused duty.
    Assert.assertTrue(timeline.isOvershadowed(ds1TombstoneSegmentMinTo2000V1));
    Assert.assertTrue(timeline.isOvershadowed(ds1TombstoneSegment2001ToMaxV1));
    Assert.assertFalse(timeline.isOvershadowed(ds1NumberedSegment2000To2001V1));
    Assert.assertFalse(timeline.isOvershadowed(ds1TombstoneSegmentMinTo2000V2));
    Assert.assertFalse(timeline.isOvershadowed(ds1TombstoneSegment2001ToMaxV2));

    runDanglingTombstonesDutyAndVerify(params, allUsedSegments, expectedUsedSegments);
  }

  @Test
  @Parameters({"historical", "broker"})
  public void testDanglingTombstonesInMultipleDatasources(final String serverType)
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

    // Datasource 1 has one overshadowed dangling tombstone, ds1TombstoneSegmentMinTo2000V1, and one non-overshadowed dangling
    // tombstone, ds1TombstoneSegment2001ToMaxV1. The latter can be removed, while the former cannot be removed.
    // Datasource 2 has 2 non-overshadowed dangling tombstones that should be removed.
    final ImmutableList<DataSegment> expectedUsedSegments = ImmutableList.of(
        ds1TombstoneSegmentMinTo2000V1,
        ds1NumberedSegment2000To2001V1,
        ds1TombstoneSegmentMinTo2000V2,
        ds2NumberedSegment3000To4000V1
    );
    DruidCoordinatorRuntimeParams params = initializeServerAndGetParams(serverType, allUsedSegments);

    SegmentTimeline ds1Timeline = segmentsMetadataManager.getSnapshotOfDataSourcesWithAllUsedSegments()
                                                      .getUsedSegmentsTimelinesPerDataSource()
                                                      .get(ds1);
    Assert.assertTrue(ds1Timeline.isOvershadowed(ds1TombstoneSegmentMinTo2000V1));
    Assert.assertFalse(ds1Timeline.isOvershadowed(ds1NumberedSegment2000To2001V1));
    Assert.assertFalse(ds1Timeline.isOvershadowed(ds1TombstoneSegment2001ToMaxV1));
    Assert.assertFalse(ds1Timeline.isOvershadowed(ds1TombstoneSegmentMinTo2000V2));
    Assert.assertFalse(ds1Timeline.isOvershadowed(ds1TombstoneSegment2001ToMaxV2));

    SegmentTimeline ds2Timeline = segmentsMetadataManager.getSnapshotOfDataSourcesWithAllUsedSegments()
                                                      .getUsedSegmentsTimelinesPerDataSource()
                                                      .get(ds2);
    Assert.assertFalse(ds2Timeline.isOvershadowed(ds2TombstoneSegmentMinTo2000V1));
    Assert.assertFalse(ds2Timeline.isOvershadowed(ds2NumberedSegment3000To4000V1));
    Assert.assertFalse(ds2Timeline.isOvershadowed(ds2TombstoneSegment4000ToMaxV1));

    runDanglingTombstonesDutyAndVerify(params, allUsedSegments, expectedUsedSegments);
  }

  @Test
  @Parameters({"historical", "broker"})
  public void testDanglingTombstonesWithPartiallyOverlappingUnderlyingSegment(final String serverType)
  {
    final ImmutableList<DataSegment> allUsedSegments = ImmutableList.of(
        ds2TombstoneSegment1995To2005V0,
        ds2TombstoneSegmentMinTo2000V1,
        ds2NumberedSegment3000To4000V1,
        ds2TombstoneSegment4000ToMaxV1
    );

    // ds2TombstoneSegment1995To2005V0 isn't overshadowed by ds2TombstoneSegmentMinTo2000V1 since there's
    // only partial overlap. So we have to keep all the segments as used.
    final ImmutableList<DataSegment> expectedUsedSegments = ImmutableList.of(
        ds2TombstoneSegment1995To2005V0,
        ds2TombstoneSegmentMinTo2000V1,
        ds2NumberedSegment3000To4000V1
    );
    DruidCoordinatorRuntimeParams params = initializeServerAndGetParams(serverType, allUsedSegments);

    final SegmentTimeline ds2Timeline = segmentsMetadataManager.getSnapshotOfDataSourcesWithAllUsedSegments()
                                                               .getUsedSegmentsTimelinesPerDataSource()
                                                               .get(ds2);

    Assert.assertFalse(ds2Timeline.isOvershadowed(ds2TombstoneSegment1995To2005V0));
    Assert.assertFalse(ds2Timeline.isOvershadowed(ds2TombstoneSegmentMinTo2000V1));
    Assert.assertFalse(ds2Timeline.isOvershadowed(ds2NumberedSegment3000To4000V1));
    Assert.assertFalse(ds2Timeline.isOvershadowed(ds2TombstoneSegment4000ToMaxV1));

    runDanglingTombstonesDutyAndVerify(params, allUsedSegments, expectedUsedSegments);
  }

  @Test
  @Parameters({"historical", "broker"})
  public void testDanglingTombstonesWithPartiallyOverlappingHigherVersionSegment(final String serverType)
  {
    final ImmutableList<DataSegment> allUsedSegments = ImmutableList.of(
        ds2TombstoneSegmentMinTo2000V1,
        ds2NumberedSegment3000To4000V1,
        ds2TombstoneSegment4000ToMaxV1,
        ds2NumberedSegment1999To2500V2
    );

    // ds2TombstoneSegmentMinTo2000V1 is non-overshadowed, but has a partial overlap with the used non-overshadowed
    // segment ds2NumberedSegment1999To2500V2.
    final ImmutableList<DataSegment> expectedUsedSegments = ImmutableList.of(
        ds2TombstoneSegmentMinTo2000V1,
        ds2NumberedSegment3000To4000V1,
        ds2NumberedSegment1999To2500V2
    );

    DruidCoordinatorRuntimeParams params = initializeServerAndGetParams(serverType, allUsedSegments);

    SegmentTimeline ds2Timeline = segmentsMetadataManager.getSnapshotOfDataSourcesWithAllUsedSegments()
                                                         .getUsedSegmentsTimelinesPerDataSource()
                                                         .get(ds2);
    Assert.assertFalse(ds2Timeline.isOvershadowed(ds2TombstoneSegmentMinTo2000V1));
    Assert.assertFalse(ds2Timeline.isOvershadowed(ds2NumberedSegment3000To4000V1));
    Assert.assertFalse(ds2Timeline.isOvershadowed(ds2TombstoneSegment4000ToMaxV1));
    Assert.assertFalse(ds2Timeline.isOvershadowed(ds2NumberedSegment1999To2500V2));

    runDanglingTombstonesDutyAndVerify(params, allUsedSegments, expectedUsedSegments);
  }

  private DruidCoordinatorRuntimeParams initializeServerAndGetParams(final String serverType, final ImmutableList<DataSegment> segments)
  {
    DruidServer druidServer1 = new DruidServer("", "", "", 0L, ServerType.fromString(serverType), "", 0);
    for (DataSegment segment : segments) {
      segmentsMetadataManager.addSegment(segment);
      druidServer1.addDataSegment(segment);
    }

    DruidCluster druidCluster = DruidCluster
        .builder()
        .add(new ServerHolder(druidServer1.toImmutableDruidServer(), new TestLoadQueuePeon()))
        .build();

    DruidCoordinatorRuntimeParams params = DruidCoordinatorRuntimeParams
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

  private void runDanglingTombstonesDutyAndVerify(
      DruidCoordinatorRuntimeParams params,
      final ImmutableList<DataSegment> allUsedSegments,
      final ImmutableList<DataSegment> expectedUsedSegments)
  {
    params = new MarkDanglingTombstonesAsUnused(segmentsMetadataManager::markSegmentsAsUnused).run(params);

    Set<DataSegment> updatedUsedSegments = Sets.newHashSet(segmentsMetadataManager.iterateAllUsedSegments());

    Assert.assertEquals(expectedUsedSegments.size(), updatedUsedSegments.size());
    Assert.assertTrue(updatedUsedSegments.containsAll(expectedUsedSegments));

    CoordinatorRunStats runStats = params.getCoordinatorStats();
    Assert.assertEquals(
        allUsedSegments.size() - expectedUsedSegments.size(),
        runStats.get(Stats.Segments.DANGLING_TOMBSTONE, RowKey.of(Dimension.DATASOURCE, ds1)) +
        runStats.get(Stats.Segments.DANGLING_TOMBSTONE, RowKey.of(Dimension.DATASOURCE, ds2))
    );
  }
}
