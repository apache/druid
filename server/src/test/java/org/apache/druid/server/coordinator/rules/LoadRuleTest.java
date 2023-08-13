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

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.client.DruidServer;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.balancer.BalancerStrategy;
import org.apache.druid.server.coordinator.balancer.CachingCostBalancerStrategy;
import org.apache.druid.server.coordinator.balancer.ClusterCostCache;
import org.apache.druid.server.coordinator.balancer.CostBalancerStrategy;
import org.apache.druid.server.coordinator.loading.SegmentLoadQueueManager;
import org.apache.druid.server.coordinator.loading.StrategicSegmentAssigner;
import org.apache.druid.server.coordinator.loading.TestLoadQueuePeon;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.Stats;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
@RunWith(Parameterized.class)
public class LoadRuleTest
{
  private static final String DS_WIKI = "wiki";

  private ListeningExecutorService exec;
  private BalancerStrategy balancerStrategy;

  private SegmentLoadQueueManager loadQueueManager;
  private final boolean useRoundRobinAssignment;

  private final AtomicInteger serverId = new AtomicInteger();

  @Parameterized.Parameters(name = "useRoundRobin = {0}")
  public static List<Boolean> getTestParams()
  {
    return Arrays.asList(true, false);
  }

  public LoadRuleTest(boolean useRoundRobinAssignment)
  {
    this.useRoundRobinAssignment = useRoundRobinAssignment;
  }

  @Before
  public void setUp()
  {
    exec = MoreExecutors.listeningDecorator(Execs.multiThreaded(1, "LoadRuleTest-%d"));
    balancerStrategy = new CostBalancerStrategy(exec);
    loadQueueManager = new SegmentLoadQueueManager(null, null, null);
  }

  @After
  public void tearDown()
  {
    exec.shutdown();
  }

  @Test
  public void testLoadRuleAssignsSegments()
  {
    // Cluster has 2 tiers with 1 server each
    final ServerHolder server1 = createServer(Tier.T1);
    final ServerHolder server2 = createServer(Tier.T2);
    DruidCluster druidCluster = DruidCluster
        .builder()
        .addTier(Tier.T1, server1)
        .addTier(Tier.T2, server2)
        .build();

    final DataSegment segment = createDataSegment(DS_WIKI);
    LoadRule rule = loadForever(ImmutableMap.of(Tier.T1, 1, Tier.T2, 2));
    CoordinatorRunStats stats = runRuleAndGetStats(rule, segment, druidCluster);

    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.ASSIGNED, Tier.T1, DS_WIKI));
    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.ASSIGNED, Tier.T2, DS_WIKI));
  }

  private CoordinatorRunStats runRuleAndGetStats(LoadRule rule, DataSegment segment, DruidCluster cluster)
  {
    return runRuleAndGetStats(rule, segment, makeCoordinatorRuntimeParams(cluster, segment));
  }

  private CoordinatorRunStats runRuleAndGetStats(
      LoadRule rule,
      DataSegment segment,
      DruidCoordinatorRuntimeParams params
  )
  {
    final StrategicSegmentAssigner segmentAssigner = params.getSegmentAssigner();
    rule.run(segment, segmentAssigner);
    return params.getCoordinatorStats();
  }

  private DruidCoordinatorRuntimeParams makeCoordinatorRuntimeParams(
      DruidCluster druidCluster,
      DataSegment... usedSegments
  )
  {
    return DruidCoordinatorRuntimeParams
        .newBuilder(DateTimes.nowUtc())
        .withDruidCluster(druidCluster)
        .withBalancerStrategy(balancerStrategy)
        .withUsedSegmentsInTest(usedSegments)
        .withDynamicConfigs(
            CoordinatorDynamicConfig.builder()
                                    .withSmartSegmentLoading(false)
                                    .withUseRoundRobinSegmentAssignment(useRoundRobinAssignment)
                                    .build()
        )
        .withSegmentAssignerUsing(loadQueueManager)
        .build();
  }

  @Test
  public void testLoadPrimaryAssignDoesNotOverAssign()
  {
    ServerHolder server1 = createServer(Tier.T1);
    ServerHolder server2 = createServer(Tier.T1);
    DruidCluster druidCluster = DruidCluster
        .builder()
        .addTier(Tier.T1, server1, server2)
        .build();

    final LoadRule rule = loadForever(ImmutableMap.of(Tier.T1, 1));
    final DataSegment segment = createDataSegment(DS_WIKI);
    CoordinatorRunStats firstRunStats = runRuleAndGetStats(rule, segment, druidCluster);
    Assert.assertEquals(1L, firstRunStats.getSegmentStat(Stats.Segments.ASSIGNED, Tier.T1, segment.getDataSource()));
    Assert.assertEquals(1, server1.getLoadingSegments().size() + server2.getLoadingSegments().size());

    // Verify that multiple runs don't assign primary segment again if at replication count
    CoordinatorRunStats secondRunStats = runRuleAndGetStats(rule, segment, druidCluster);
    Assert.assertFalse(secondRunStats.hasStat(Stats.Segments.ASSIGNED));
    Assert.assertEquals(1, server1.getLoadingSegments().size() + server2.getLoadingSegments().size());
  }

  @Test
  @Ignore("Enable this test when timeout behaviour is fixed")
  public void testOverAssignForTimedOutSegments()
  {
    ServerHolder server1 = createServer(Tier.T1);
    ServerHolder server2 = createServer(Tier.T1);
    DruidCluster druidCluster = DruidCluster
        .builder()
        .addTier(Tier.T1, server1, server2)
        .build();

    final LoadRule rule = loadForever(ImmutableMap.of(Tier.T1, 1));
    final DataSegment segment = createDataSegment(DS_WIKI);
    CoordinatorRunStats stats = runRuleAndGetStats(rule, segment, druidCluster);

    // Ensure that the segment is assigned to one of the historicals
    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.ASSIGNED, Tier.T1, segment.getDataSource()));

    // Ensure that the primary segment is assigned again in case the peon timed out on loading the segment
    CoordinatorRunStats statsAfterLoadPrimary = runRuleAndGetStats(rule, segment, druidCluster);
    Assert.assertEquals(1L, statsAfterLoadPrimary.getSegmentStat(Stats.Segments.ASSIGNED, Tier.T1, DS_WIKI));
  }

  @Test
  public void testSkipReplicationForTimedOutSegments()
  {
    ServerHolder server1 = createServer(Tier.T1);
    ServerHolder server2 = createServer(Tier.T1);
    DruidCluster druidCluster = DruidCluster
        .builder()
        .addTier(Tier.T1, server1, server2)
        .build();

    final LoadRule rule = loadForever(ImmutableMap.of(Tier.T1, 1));
    final DataSegment segment = createDataSegment(DS_WIKI);
    CoordinatorRunStats stats = runRuleAndGetStats(rule, segment, druidCluster);

    // Ensure that the segment is assigned to one of the historicals
    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.ASSIGNED, Tier.T1, segment.getDataSource()));

    // Add the segment to the timed out list to simulate peon timeout on loading the segment
    // Default behavior is to not replicate the timed out segments on other servers
    CoordinatorRunStats statsAfterLoadPrimary = runRuleAndGetStats(rule, segment, druidCluster);

    Assert.assertFalse(statsAfterLoadPrimary.hasStat(Stats.Segments.ASSIGNED));
  }

  @Test
  public void testLoadUsedSegmentsForAllSegmentGranularityAndCachingCostBalancerStrategy()
  {
    final List<DataSegment> segments =
        CreateDataSegments.ofDatasource(DS_WIKI)
                          .forIntervals(1, Granularities.ALL)
                          .withNumPartitions(2)
                          .eachOfSizeInMb(100);

    final ServerHolder server1 = createServer(Tier.T1);
    DruidCluster druidCluster = DruidCluster
        .builder()
        .addTier(Tier.T1, server1)
        .build();

    balancerStrategy = new CachingCostBalancerStrategy(ClusterCostCache.builder().build(), exec);

    LoadRule rule = loadForever(ImmutableMap.of(Tier.T1, 1));
    CoordinatorRunStats stats = runRuleAndGetStats(
        rule,
        segments.get(1),
        makeCoordinatorRuntimeParams(druidCluster, segments.toArray(new DataSegment[0]))
    );
    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.ASSIGNED, Tier.T1, DS_WIKI));
  }

  @Test
  public void testSegmentsAreDroppedIfLoadRuleHasZeroReplicas()
  {
    final DataSegment segment = createDataSegment(DS_WIKI);

    final ServerHolder serverT11 = createServer(Tier.T1, segment);
    final ServerHolder serverT12 = createServer(Tier.T2, segment);
    final ServerHolder serverT21 = createServer(Tier.T2, segment);

    DruidCluster druidCluster = DruidCluster
        .builder()
        .addTier(Tier.T1, serverT11)
        .addTier(Tier.T2, serverT12, serverT21)
        .build();

    LoadRule rule = loadForever(ImmutableMap.of(Tier.T1, 0, Tier.T2, 0));
    CoordinatorRunStats stats = runRuleAndGetStats(rule, segment, druidCluster);

    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.DROPPED, Tier.T1, DS_WIKI));
    Assert.assertEquals(2L, stats.getSegmentStat(Stats.Segments.DROPPED, Tier.T2, DS_WIKI));
  }

  @Test
  public void testLoadIgnoresInvalidTiers()
  {
    ServerHolder server = createServer(Tier.T1);
    DruidCluster druidCluster = DruidCluster
        .builder()
        .addTier(Tier.T1, server)
        .build();

    final DataSegment segment = createDataSegment(DS_WIKI);
    LoadRule rule = loadForever(ImmutableMap.of("invalidTier", 1, Tier.T1, 1));

    CoordinatorRunStats stats = runRuleAndGetStats(rule, segment, druidCluster);
    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.ASSIGNED, Tier.T1, DS_WIKI));
    Assert.assertEquals(0L, stats.getSegmentStat(Stats.Segments.ASSIGNED, "invalidTier", DS_WIKI));
  }

  @Test
  public void testDropIgnoresInvalidTiers()
  {
    final DataSegment segment = createDataSegment(DS_WIKI);

    // Cluster has 1 tier with 2 servers
    ServerHolder server1 = createServer(Tier.T1, segment);
    ServerHolder server2 = createServer(Tier.T1, segment);
    DruidCluster druidCluster = DruidCluster
        .builder()
        .addTier(Tier.T1, server1, server2)
        .build();

    LoadRule rule = loadForever(ImmutableMap.of("invalidTier", 1, Tier.T1, 1));
    CoordinatorRunStats stats = runRuleAndGetStats(rule, segment, druidCluster);

    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.DROPPED, Tier.T1, DS_WIKI));
    Assert.assertEquals(0L, stats.getSegmentStat(Stats.Segments.DROPPED, "invalidTier", DS_WIKI));
  }

  @Test
  public void testMaxLoadingQueueSize()
  {
    final TestLoadQueuePeon peon = new TestLoadQueuePeon();

    final int maxSegmentsInQueue = 2;
    DruidCluster druidCluster = DruidCluster
        .builder()
        .addTier(
            Tier.T1,
            new ServerHolder(
                createDruidServer(Tier.T1).toImmutableDruidServer(),
                peon, false, maxSegmentsInQueue, 10
            )
        )
        .build();

    DataSegment dataSegment1 = createDataSegment("ds1");
    DataSegment dataSegment2 = createDataSegment("ds2");
    DataSegment dataSegment3 = createDataSegment("ds3");

    DruidCoordinatorRuntimeParams params = DruidCoordinatorRuntimeParams
        .newBuilder(DateTimes.nowUtc())
        .withDruidCluster(druidCluster)
        .withBalancerStrategy(balancerStrategy)
        .withUsedSegmentsInTest(dataSegment1, dataSegment2, dataSegment3)
        .withDynamicConfigs(
            CoordinatorDynamicConfig.builder()
                                    .withSmartSegmentLoading(false)
                                    .withMaxSegmentsInNodeLoadingQueue(maxSegmentsInQueue)
                                    .withUseRoundRobinSegmentAssignment(useRoundRobinAssignment)
                                    .build()
        )
        .withSegmentAssignerUsing(loadQueueManager)
        .build();

    final LoadRule rule = loadForever(ImmutableMap.of(Tier.T1, 1));
    CoordinatorRunStats stats1 = runRuleAndGetStats(rule, dataSegment1, params);
    CoordinatorRunStats stats2 = runRuleAndGetStats(rule, dataSegment2, params);
    CoordinatorRunStats stats3 = runRuleAndGetStats(rule, dataSegment3, params);

    Assert.assertEquals(1L, stats1.getSegmentStat(Stats.Segments.ASSIGNED, Tier.T1, dataSegment1.getDataSource()));
    Assert.assertEquals(1L, stats2.getSegmentStat(Stats.Segments.ASSIGNED, Tier.T1, dataSegment2.getDataSource()));
    Assert.assertEquals(0L, stats3.getSegmentStat(Stats.Segments.ASSIGNED, Tier.T1, dataSegment3.getDataSource()));
  }

  @Test
  public void testSegmentIsAssignedOnlyToActiveServer()
  {
    final ServerHolder decommServerT1 = createDecommissioningServer(Tier.T1);
    final ServerHolder serverT2 = createServer(Tier.T2);

    DruidCluster druidCluster = DruidCluster
        .builder()
        .addTier(Tier.T1, decommServerT1)
        .addTier(Tier.T2, serverT2)
        .build();

    // Load rule requires 1 replica on each tier
    LoadRule rule = loadForever(ImmutableMap.of(Tier.T1, 1, Tier.T2, 1));
    DataSegment segment = createDataSegment(DS_WIKI);
    CoordinatorRunStats stats = runRuleAndGetStats(rule, segment, druidCluster);

    // Verify that segment is not loaded on decommissioning server
    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.ASSIGNED, Tier.T2, DS_WIKI));
    Assert.assertEquals(0, decommServerT1.getLoadingSegments().size());
    Assert.assertTrue(serverT2.getLoadingSegments().contains(segment));
  }

  @Test
  public void testSegmentIsAssignedOnlyToActiveServers()
  {
    // 2 tiers with 2 servers each, 1 server is decommissioning
    ServerHolder decommServerT11 = createDecommissioningServer(Tier.T1);
    ServerHolder serverT12 = createServer(Tier.T1);
    ServerHolder serverT21 = createServer(Tier.T2);
    ServerHolder serverT22 = createServer(Tier.T2);

    final DataSegment segment = createDataSegment(DS_WIKI);
    DruidCluster druidCluster = DruidCluster
        .builder()
        .addTier(Tier.T1, decommServerT11, serverT12)
        .addTier(Tier.T2, serverT21, serverT22)
        .build();

    // Load rule requires 2 replicas on each server
    LoadRule rule = loadForever(ImmutableMap.of(Tier.T1, 2, Tier.T2, 2));
    CoordinatorRunStats stats = runRuleAndGetStats(rule, segment, druidCluster);

    // Verify that no replica is assigned to decommissioning server
    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.ASSIGNED, Tier.T1, DS_WIKI));
    Assert.assertTrue(decommServerT11.getLoadingSegments().isEmpty());
    Assert.assertEquals(0, decommServerT11.getLoadingSegments().size());

    Assert.assertEquals(2L, stats.getSegmentStat(Stats.Segments.ASSIGNED, Tier.T2, DS_WIKI));
  }

  /**
   * 2 servers with a segment, one server decommissioning.
   * Should drop a segment from both.
   */
  @Test
  public void testDropDuringDecommissioning()
  {
    final DataSegment segment1 = createDataSegment("foo1");
    final DataSegment segment2 = createDataSegment("foo2");

    final ServerHolder server1 = createDecommissioningServer(Tier.T1, segment1);
    final ServerHolder server2 = createServer(Tier.T1, segment2);

    DruidCluster druidCluster = DruidCluster
        .builder()
        .addTier(Tier.T1, server1, server2)
        .build();

    DruidCoordinatorRuntimeParams params = makeCoordinatorRuntimeParams(druidCluster, segment1, segment2);
    final LoadRule rule = loadForever(ImmutableMap.of(Tier.T1, 0));

    CoordinatorRunStats stats = runRuleAndGetStats(rule, segment1, params);
    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.DROPPED, Tier.T1, segment1.getDataSource()));
    Assert.assertTrue(server1.getPeon().getSegmentsToDrop().contains(segment1));

    stats = runRuleAndGetStats(rule, segment2, params);
    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.DROPPED, Tier.T1, segment2.getDataSource()));
    Assert.assertTrue(server2.getPeon().getSegmentsToDrop().contains(segment2));
  }

  @Test
  public void testExtraReplicasAreDroppedFromDecommissioningServer()
  {
    final DataSegment segment1 = createDataSegment(DS_WIKI);

    // 3 servers, each serving the same segment
    final ServerHolder server1 = createServer(Tier.T1, segment1);
    final ServerHolder server2 = createDecommissioningServer(Tier.T1, segment1);
    final ServerHolder server3 = createServer(Tier.T1, segment1);

    DruidCluster druidCluster = DruidCluster
        .builder()
        .addTier(Tier.T1, server1, server2, server3)
        .build();

    // Load rule requires 2 replicas
    LoadRule rule = loadForever(ImmutableMap.of(Tier.T1, 2));
    CoordinatorRunStats stats = runRuleAndGetStats(
        rule,
        segment1,
        makeCoordinatorRuntimeParams(druidCluster, segment1)
    );

    // Verify that the extra replica is dropped from the decommissioning server
    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.DROPPED, Tier.T1, DS_WIKI));
    Assert.assertEquals(0, server1.getPeon().getSegmentsToDrop().size());
    Assert.assertEquals(1, server2.getPeon().getSegmentsToDrop().size());
    Assert.assertEquals(0, server3.getPeon().getSegmentsToDrop().size());
  }

  private DataSegment createDataSegment(String dataSource)
  {
    return new DataSegment(
        dataSource,
        Intervals.of("0/3000"),
        DateTimes.nowUtc().toString(),
        Collections.emptyMap(),
        Collections.emptyList(),
        Collections.emptyList(),
        NoneShardSpec.instance(),
        0,
        0
    );
  }

  private static LoadRule loadForever(final Map<String, Integer> tieredReplicants)
  {
    return new ForeverLoadRule(tieredReplicants, null);
  }

  private DruidServer createDruidServer(String tier)
  {
    final String serverName = "hist_" + tier + "_" + serverId.incrementAndGet();
    return new DruidServer(serverName, serverName, null, 10L << 30, ServerType.HISTORICAL, tier, 0);
  }

  private ServerHolder createServer(String tier, DataSegment... segments)
  {
    final DruidServer server = createDruidServer(tier);
    for (DataSegment segment : segments) {
      server.addDataSegment(segment);
    }

    return new ServerHolder(
        server.toImmutableDruidServer(),
        new TestLoadQueuePeon()
    );
  }

  private ServerHolder createDecommissioningServer(String tier, DataSegment... segments)
  {
    final DruidServer server = createDruidServer(tier);
    for (DataSegment segment : segments) {
      server.addDataSegment(segment);
    }

    return new ServerHolder(
        server.toImmutableDruidServer(),
        new TestLoadQueuePeon(),
        true
    );
  }

  private static class Tier
  {
    static final String T1 = "tier1";
    static final String T2 = "tier2";
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(LoadRule.class)
                  .withNonnullFields("tieredReplicants")
                  .usingGetClass()
                  .verify();
  }
}
