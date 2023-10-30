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

import com.google.common.collect.Sets;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.druid.client.DruidServer;
import org.apache.druid.client.ImmutableDruidServer;
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

  private final String dataSource = "test";
  private final DataSegment eternitySegmentV0 = DataSegment.builder().dataSource(dataSource)
                                                           .interval(Intervals.ETERNITY)
                                                           .version("0")
                                                           .size(0)
                                                           .build();

  private final DataSegment tombstoneSegment1V1 = DataSegment.builder().dataSource(dataSource)
                                                             .shardSpec(new TombstoneShardSpec())
                                                             .interval(Intervals.of("%s/%s", Intervals.ETERNITY.getStart(), 2000))
                                                             .version("1")
                                                             .size(0)
                                                             .build();

  private final DataSegment dataSegmentV1 = DataSegment.builder().dataSource(dataSource)
                                                       .interval(Intervals.of("2000/2001"))
                                                       .version("1")
                                                       .size(0)
                                                       .build();

  private final DataSegment tombstoneSegment2V1 = DataSegment.builder().dataSource(dataSource)
                                                             .shardSpec(new TombstoneShardSpec())
                                                             .interval(Intervals.of("%s/%s", 2001, Intervals.ETERNITY.getEnd()))
                                                             .version("1")
                                                             .size(0)
                                                             .build();

  final DataSegment tombstoneSegment1V2 = tombstoneSegment1V1.withVersion("2");
  final DataSegment tombstoneSegment2V2 = tombstoneSegment2V1.withVersion("2");

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
    segmentsMetadataManager.addSegment(eternitySegmentV0);
    segmentsMetadataManager.addSegment(tombstoneSegment1V1);
    segmentsMetadataManager.addSegment(dataSegmentV1);
    segmentsMetadataManager.addSegment(tombstoneSegment2V1);

    final ImmutableDruidServer druidServer =
        new DruidServer("", "", "", 0L, ServerType.fromString(serverType), "", 0)
            .addDataSegment(eternitySegmentV0)
            .addDataSegment(tombstoneSegment1V1)
            .addDataSegment(dataSegmentV1)
            .addDataSegment(tombstoneSegment2V1)
            .toImmutableDruidServer();

    DruidCluster druidCluster = DruidCluster
        .builder()
        .add(new ServerHolder(druidServer, new TestLoadQueuePeon()))
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

    SegmentTimeline timeline = segmentsMetadataManager.getSnapshotOfDataSourcesWithAllUsedSegments()
                                                      .getUsedSegmentsTimelinesPerDataSource()
                                                      .get(dataSource);

    // Verify that the eternity segment is overshadowed and everything else is not
    Assert.assertTrue(timeline.isOvershadowed(eternitySegmentV0));
    Assert.assertFalse(timeline.isOvershadowed(tombstoneSegment1V1));
    Assert.assertFalse(timeline.isOvershadowed(dataSegmentV1));
    Assert.assertFalse(timeline.isOvershadowed(tombstoneSegment2V1));


    params = new MarkDanglingTombstonesAsUnused(segmentsMetadataManager).run(params);

    Set<DataSegment> updatedUsedSegments = Sets.newHashSet(segmentsMetadataManager.iterateAllUsedSegments());
    Assert.assertEquals(4, updatedUsedSegments.size());

    CoordinatorRunStats runStats = params.getCoordinatorStats();
    Assert.assertEquals(
        0,
        runStats.get(Stats.Segments.DANGLING_TOMBSTONE, RowKey.of(Dimension.DATASOURCE, dataSource))
    );
  }

  @Test
  @Parameters({"historical", "broker"})
  public void testDanglingTombstonesWithNoOvershadowedSegment(final String serverType)
  {
    segmentsMetadataManager.addSegment(tombstoneSegment1V1);
    segmentsMetadataManager.addSegment(dataSegmentV1);
    segmentsMetadataManager.addSegment(tombstoneSegment2V1);

    final ImmutableDruidServer druidServer =
        new DruidServer("", "", "", 0L, ServerType.fromString(serverType), "", 0)
            .addDataSegment(tombstoneSegment1V1)
            .addDataSegment(dataSegmentV1)
            .addDataSegment(tombstoneSegment1V2)
            .toImmutableDruidServer();

    DruidCluster druidCluster = DruidCluster
        .builder()
        .add(new ServerHolder(druidServer, new TestLoadQueuePeon()))
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

    SegmentTimeline timeline = segmentsMetadataManager.getSnapshotOfDataSourcesWithAllUsedSegments()
                                                      .getUsedSegmentsTimelinesPerDataSource()
                                                      .get(dataSource);

    Assert.assertFalse(timeline.isOvershadowed(tombstoneSegment1V1));
    Assert.assertFalse(timeline.isOvershadowed(dataSegmentV1));
    Assert.assertFalse(timeline.isOvershadowed(tombstoneSegment1V2));

    params = new MarkDanglingTombstonesAsUnused(segmentsMetadataManager).run(params);

    Set<DataSegment> updatedUsedSegments = Sets.newHashSet(segmentsMetadataManager.iterateAllUsedSegments());
    Assert.assertEquals(1, updatedUsedSegments.size());
    Assert.assertTrue(updatedUsedSegments.contains(dataSegmentV1));

    CoordinatorRunStats runStats = params.getCoordinatorStats();
    Assert.assertEquals(
        2,
        runStats.get(Stats.Segments.DANGLING_TOMBSTONE, RowKey.of(Dimension.DATASOURCE, dataSource))
    );
  }

  @Test
  @Parameters({"historical", "broker"})
  public void testOvershadowedDanglingTombstones(final String serverType)
  {
    segmentsMetadataManager.addSegment(tombstoneSegment1V1);
    segmentsMetadataManager.addSegment(dataSegmentV1);
    segmentsMetadataManager.addSegment(tombstoneSegment1V2);
    segmentsMetadataManager.addSegment(tombstoneSegment2V1);
    segmentsMetadataManager.addSegment(tombstoneSegment2V2);

    final ImmutableDruidServer druidServer =
        new DruidServer("", "", "", 0L, ServerType.fromString(serverType), "", 0)
            .addDataSegment(tombstoneSegment1V1)
            .addDataSegment(dataSegmentV1)
            .addDataSegment(tombstoneSegment1V2)
            .addDataSegment(tombstoneSegment2V1)
            .addDataSegment(tombstoneSegment2V2)
            .toImmutableDruidServer();

    DruidCluster druidCluster = DruidCluster
        .builder()
        .add(new ServerHolder(druidServer, new TestLoadQueuePeon()))
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

    SegmentTimeline timeline = segmentsMetadataManager.getSnapshotOfDataSourcesWithAllUsedSegments()
                                                      .getUsedSegmentsTimelinesPerDataSource()
                                                      .get(dataSource);


    Assert.assertTrue(timeline.isOvershadowed(tombstoneSegment1V1));
    Assert.assertTrue(timeline.isOvershadowed(tombstoneSegment2V1));
    Assert.assertFalse(timeline.isOvershadowed(dataSegmentV1));
    Assert.assertFalse(timeline.isOvershadowed(tombstoneSegment1V2));
    Assert.assertFalse(timeline.isOvershadowed(tombstoneSegment2V2));

    params = new MarkDanglingTombstonesAsUnused(segmentsMetadataManager).run(params);

    Set<DataSegment> updatedUsedSegments = Sets.newHashSet(segmentsMetadataManager.iterateAllUsedSegments());
    Assert.assertEquals(5, updatedUsedSegments.size());

    CoordinatorRunStats runStats = params.getCoordinatorStats();
    Assert.assertEquals(
        0,
        runStats.get(Stats.Segments.DANGLING_TOMBSTONE, RowKey.of(Dimension.DATASOURCE, dataSource))
    );
  }
}
