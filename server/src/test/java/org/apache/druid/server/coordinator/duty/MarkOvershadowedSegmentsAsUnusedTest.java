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
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Set;

@RunWith(JUnitParamsRunner.class)
public class MarkOvershadowedSegmentsAsUnusedTest
{
  private final DateTime start = DateTimes.of("2012-01-01");

  private final DataSegment segmentV0 = DataSegment.builder().dataSource("test")
                                                   .interval(new Interval(start, start.plusHours(1)))
                                                   .version("0")
                                                   .size(0)
                                                   .build();
  private final DataSegment segmentV1 = segmentV0.withVersion("1");
  private final DataSegment segmentV2 = segmentV0.withVersion("2");

  private TestSegmentsMetadataManager segmentsMetadataManager;

  @Before
  public void setup()
  {
    segmentsMetadataManager = new TestSegmentsMetadataManager();
  }

  @Test
  @Parameters({"historical", "broker"})
  public void testRun(String serverType)
  {
    segmentsMetadataManager.addSegment(segmentV0);
    segmentsMetadataManager.addSegment(segmentV1);
    segmentsMetadataManager.addSegment(segmentV2);

    final ImmutableDruidServer druidServer =
        new DruidServer("", "", "", 0L, ServerType.fromString(serverType), "", 0)
            .addDataSegment(segmentV1)
            .addDataSegment(segmentV2)
            .toImmutableDruidServer();

    DruidCluster druidCluster = DruidCluster
        .builder()
        .add(new ServerHolder(druidServer, new TestLoadQueuePeon()))
        .build();

    DruidCoordinatorRuntimeParams params = DruidCoordinatorRuntimeParams
        .newBuilder(DateTimes.nowUtc())
        .withSnapshotOfDataSourcesWithAllUsedSegments(
            segmentsMetadataManager.getSnapshotOfDataSourcesWithAllUsedSegments()
        )
        .withDruidCluster(druidCluster)
        .withDynamicConfigs(
            CoordinatorDynamicConfig.builder().withMarkSegmentAsUnusedDelayMillis(0).build()
        )
        .withBalancerStrategy(new RandomBalancerStrategy())
        .withSegmentAssignerUsing(new SegmentLoadQueueManager(null, null, null))
        .build();

    SegmentTimeline timeline = segmentsMetadataManager.getSnapshotOfDataSourcesWithAllUsedSegments()
                                                      .getUsedSegmentsTimelinesPerDataSource()
                                                      .get("test");

    // Verify that the segments V0 and V1 are overshadowed
    Assert.assertTrue(timeline.isOvershadowed(segmentV0));
    Assert.assertTrue(timeline.isOvershadowed(segmentV1));

    // Run the duty and verify that the overshadowed segments are marked unused
    params = new MarkOvershadowedSegmentsAsUnused(segmentsMetadataManager::markSegmentsAsUnused).run(params);

    Set<DataSegment> updatedUsedSegments = Sets.newHashSet(segmentsMetadataManager.iterateAllUsedSegments());
    Assert.assertEquals(1, updatedUsedSegments.size());
    Assert.assertTrue(updatedUsedSegments.contains(segmentV2));

    CoordinatorRunStats runStats = params.getCoordinatorStats();
    Assert.assertEquals(
        2L,
        runStats.get(Stats.Segments.OVERSHADOWED, RowKey.of(Dimension.DATASOURCE, "test"))
    );
  }
}
