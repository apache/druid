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

package org.apache.druid.server.coordinator.rules;

import org.apache.druid.client.DruidServer;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.balancer.RandomBalancerStrategy;
import org.apache.druid.server.coordinator.loading.SegmentAction;
import org.apache.druid.server.coordinator.loading.SegmentLoadQueueManager;
import org.apache.druid.server.coordinator.loading.StrategicSegmentAssigner;
import org.apache.druid.server.coordinator.loading.TestLoadQueuePeon;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.Dimension;
import org.apache.druid.server.coordinator.stats.RowKey;
import org.apache.druid.server.coordinator.stats.Stats;
import org.apache.druid.timeline.DataSegment;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class BroadcastDistributionRuleTest
{
  private int serverId = 0;

  private static final String DS_WIKI = "wiki";
  private static final String TIER_1 = "tier1";
  private static final String TIER_2 = "tier2";

  private final DataSegment wikiSegment
      = CreateDataSegments.ofDatasource(DS_WIKI).eachOfSizeInMb(100).get(0);

  @Before
  public void setUp()
  {
    serverId = 0;
  }

  @Test
  public void testSegmentIsBroadcastToAllTiers()
  {
    // 2 tiers with one server each
    final ServerHolder serverT11 = create10gbHistorical(TIER_1);
    final ServerHolder serverT21 = create10gbHistorical(TIER_2);
    DruidCluster cluster = DruidCluster.builder().add(serverT11).add(serverT21).build();
    DruidCoordinatorRuntimeParams params = makeParamsWithUsedSegments(cluster, wikiSegment);

    ForeverBroadcastDistributionRule rule = new ForeverBroadcastDistributionRule();
    CoordinatorRunStats stats = runRuleOnSegment(rule, wikiSegment, params);

    // Verify that segment is assigned to servers of all tiers
    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.ASSIGNED, TIER_1, DS_WIKI));
    Assert.assertTrue(serverT11.isLoadingSegment(wikiSegment));

    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.ASSIGNED, TIER_2, DS_WIKI));
    Assert.assertTrue(serverT21.isLoadingSegment(wikiSegment));
  }

  @Test
  public void testSegmentIsNotBroadcastToServerIfAlreadyLoaded()
  {
    // serverT11 is already serving the segment which is being broadcast
    final ServerHolder serverT11 = create10gbHistorical(TIER_1, wikiSegment);
    final ServerHolder serverT12 = create10gbHistorical(TIER_1);
    DruidCluster cluster = DruidCluster.builder().add(serverT11).add(serverT12).build();
    DruidCoordinatorRuntimeParams params = makeParamsWithUsedSegments(cluster, wikiSegment);

    ForeverBroadcastDistributionRule rule = new ForeverBroadcastDistributionRule();
    CoordinatorRunStats stats = runRuleOnSegment(rule, wikiSegment, params);

    // Verify that serverT11 is already serving and serverT12 is loading segment
    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.ASSIGNED, TIER_1, DS_WIKI));
    Assert.assertFalse(serverT11.isLoadingSegment(wikiSegment));
    Assert.assertTrue(serverT11.isServingSegment(wikiSegment));
    Assert.assertTrue(serverT12.isLoadingSegment(wikiSegment));
  }

  @Test
  public void testSegmentIsNotBroadcastToDecommissioningServer()
  {
    ServerHolder activeServer = create10gbHistorical(TIER_1);
    ServerHolder decommissioningServer = createDecommissioningHistorical(TIER_1);
    DruidCluster cluster = DruidCluster.builder()
                                       .add(activeServer)
                                       .add(decommissioningServer).build();
    DruidCoordinatorRuntimeParams params = makeParamsWithUsedSegments(cluster, wikiSegment);

    ForeverBroadcastDistributionRule rule = new ForeverBroadcastDistributionRule();
    CoordinatorRunStats stats = runRuleOnSegment(rule, wikiSegment, params);

    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.ASSIGNED, TIER_1, DS_WIKI));
    Assert.assertTrue(activeServer.isLoadingSegment(wikiSegment));
    Assert.assertTrue(decommissioningServer.getLoadingSegments().isEmpty());
  }

  @Test
  public void testBroadcastSegmentIsDroppedFromDecommissioningServer()
  {
    // Both active and decommissioning servers are already serving the segment
    ServerHolder activeServer = create10gbHistorical(TIER_1, wikiSegment);
    ServerHolder decommissioningServer = createDecommissioningHistorical(TIER_1, wikiSegment);
    DruidCluster cluster = DruidCluster.builder()
                                       .add(activeServer)
                                       .add(decommissioningServer)
                                       .build();
    DruidCoordinatorRuntimeParams params = makeParamsWithUsedSegments(cluster, wikiSegment);

    ForeverBroadcastDistributionRule rule = new ForeverBroadcastDistributionRule();
    CoordinatorRunStats stats = runRuleOnSegment(rule, wikiSegment, params);

    // Verify that segment is dropped only from the decommissioning server
    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.DROPPED, TIER_1, DS_WIKI));
    Assert.assertTrue(activeServer.getPeon().getSegmentsToDrop().isEmpty());
    Assert.assertTrue(decommissioningServer.getPeon().getSegmentsToDrop().contains(wikiSegment));
  }

  @Test
  public void testSegmentIsBroadcastToAllServerTypes()
  {
    final ServerHolder broker = new ServerHolder(
        create10gbServer(ServerType.BROKER, "broker_tier").toImmutableDruidServer(),
        new TestLoadQueuePeon()
    );
    final ServerHolder indexer = new ServerHolder(
        create10gbServer(ServerType.INDEXER_EXECUTOR, TIER_2).toImmutableDruidServer(),
        new TestLoadQueuePeon()
    );
    final ServerHolder historical = create10gbHistorical(TIER_1);

    DruidCluster cluster = DruidCluster.builder()
                                       .add(broker).add(indexer).add(historical)
                                       .build();
    DruidCoordinatorRuntimeParams params = makeParamsWithUsedSegments(cluster, wikiSegment);

    ForeverBroadcastDistributionRule rule = new ForeverBroadcastDistributionRule();
    final CoordinatorRunStats stats = runRuleOnSegment(rule, wikiSegment, params);

    // Verify that segment is assigned to historical, broker as well as indexer
    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.ASSIGNED, TIER_1, DS_WIKI));
    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.ASSIGNED, TIER_2, DS_WIKI));
    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.ASSIGNED, broker.getServer().getTier(), DS_WIKI));

    Assert.assertTrue(historical.isLoadingSegment(wikiSegment));
    Assert.assertTrue(indexer.isLoadingSegment(wikiSegment));
    Assert.assertTrue(broker.isLoadingSegment(wikiSegment));
  }

  @Test
  public void testReasonForBroadcastFailure()
  {
    final ServerHolder eligibleServer = create10gbHistorical(TIER_1);
    final ServerHolder serverWithNoDiskSpace = new ServerHolder(
        new DruidServer("server1", "server1", null, 0L, ServerType.HISTORICAL, TIER_1, 0)
            .toImmutableDruidServer(),
        new TestLoadQueuePeon()
    );

    // Create a server with full load queue
    final int maxSegmentsInLoadQueue = 5;
    final ServerHolder serverWithFullQueue = new ServerHolder(
        create10gbServer(ServerType.HISTORICAL, TIER_1).toImmutableDruidServer(),
        new TestLoadQueuePeon(), false, maxSegmentsInLoadQueue, 100
    );

    List<DataSegment> segmentsInQueue
        = CreateDataSegments.ofDatasource("koala")
                            .forIntervals(maxSegmentsInLoadQueue, Granularities.MONTH)
                            .withNumPartitions(1)
                            .eachOfSizeInMb(10);
    segmentsInQueue.forEach(s -> serverWithFullQueue.startOperation(SegmentAction.LOAD, s));
    Assert.assertTrue(serverWithFullQueue.isLoadQueueFull());

    DruidCluster cluster = DruidCluster.builder()
                                       .add(eligibleServer)
                                       .add(serverWithNoDiskSpace)
                                       .add(serverWithFullQueue)
                                       .build();
    DruidCoordinatorRuntimeParams params = makeParamsWithUsedSegments(cluster, wikiSegment);

    ForeverBroadcastDistributionRule rule = new ForeverBroadcastDistributionRule();
    final CoordinatorRunStats stats = runRuleOnSegment(rule, wikiSegment, params);

    // Verify that the segment is broadcast only to the eligible server
    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.ASSIGNED, TIER_1, DS_WIKI));
    RowKey metricKey = RowKey.with(Dimension.DATASOURCE, DS_WIKI)
                             .with(Dimension.TIER, TIER_1)
                             .and(Dimension.DESCRIPTION, "Not enough disk space");
    Assert.assertEquals(1L, stats.get(Stats.Segments.ASSIGN_SKIPPED, metricKey));

    metricKey = RowKey.with(Dimension.DATASOURCE, DS_WIKI)
                      .with(Dimension.TIER, TIER_1)
                      .and(Dimension.DESCRIPTION, "Load queue is full");
    Assert.assertEquals(1L, stats.get(Stats.Segments.ASSIGN_SKIPPED, metricKey));
  }

  private CoordinatorRunStats runRuleOnSegment(
      Rule rule,
      DataSegment segment,
      DruidCoordinatorRuntimeParams params
  )
  {
    StrategicSegmentAssigner segmentAssigner = params.getSegmentAssigner();
    rule.run(segment, segmentAssigner);
    return segmentAssigner.getStats();
  }

  private DruidCoordinatorRuntimeParams makeParamsWithUsedSegments(
      DruidCluster druidCluster,
      DataSegment... usedSegments
  )
  {
    return DruidCoordinatorRuntimeParams
        .newBuilder(DateTimes.nowUtc())
        .withDruidCluster(druidCluster)
        .withUsedSegmentsInTest(usedSegments)
        .withBalancerStrategy(new RandomBalancerStrategy())
        .withSegmentAssignerUsing(new SegmentLoadQueueManager(null, null, null))
        .build();
  }

  private ServerHolder create10gbHistorical(String tier, DataSegment... segments)
  {
    DruidServer server = create10gbServer(ServerType.HISTORICAL, tier);
    for (DataSegment segment : segments) {
      server.addDataSegment(segment);
    }
    return new ServerHolder(server.toImmutableDruidServer(), new TestLoadQueuePeon());
  }

  private ServerHolder createDecommissioningHistorical(String tier, DataSegment... segments)
  {
    DruidServer server = create10gbServer(ServerType.HISTORICAL, tier);
    for (DataSegment segment : segments) {
      server.addDataSegment(segment);
    }
    return new ServerHolder(server.toImmutableDruidServer(), new TestLoadQueuePeon(), true);
  }

  private DruidServer create10gbServer(ServerType type, String tier)
  {
    final String name = "server_" + serverId++;
    return new DruidServer(name, name, null, 10L << 30, type, tier, 0);
  }
}
