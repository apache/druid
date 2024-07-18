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

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.client.DruidServer;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.CoordinatorBaseTest;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.balancer.CachingCostBalancerStrategy;
import org.apache.druid.server.coordinator.balancer.ClusterCostCache;
import org.apache.druid.server.coordinator.balancer.CostBalancerStrategy;
import org.apache.druid.server.coordinator.loading.SegmentLoadQueueManager;
import org.apache.druid.server.coordinator.loading.TestLoadQueuePeon;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.Stats;
import org.apache.druid.timeline.DataSegment;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
@RunWith(Parameterized.class)
public class LoadRuleTest extends CoordinatorBaseTest
{
  private final DataSegment wikiSegment = createDataSegment(DS.WIKI);
  private final DataSegment koalaSegment = createDataSegment(DS.KOALA);
  private final AtomicInteger serverId = new AtomicInteger();

  private ListeningExecutorService exec;

  private final boolean useRoundRobinAssignment;

  private Set<DataSegment> usedSegments;
  private DruidCluster.Builder druidCluster;
  private DruidCoordinatorRuntimeParams params;
  private CoordinatorRunStats stats;

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
    params = DruidCoordinatorRuntimeParams
        .newBuilder(DateTimes.nowUtc())
        .withDruidCluster(DruidCluster.EMPTY)
        .withBalancerStrategy(new CostBalancerStrategy(exec))
        .withUsedSegments(Collections.emptyList())
        .withDynamicConfigs(
            CoordinatorDynamicConfig
                .builder()
                .withSmartSegmentLoading(false)
                .withUseRoundRobinSegmentAssignment(useRoundRobinAssignment)
                .build()
        )
        .withSegmentAssignerUsing(new SegmentLoadQueueManager(null, null))
        .build();

    exec = MoreExecutors.listeningDecorator(Execs.singleThreaded("LoadRuleTest-%d"));
    druidCluster = DruidCluster.builder();
    stats = params.getCoordinatorStats();
    usedSegments = new HashSet<>(Arrays.asList(wikiSegment, koalaSegment));
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
    druidCluster.add(createServer(Tier.T1))
                .add(createServer(Tier.T2));

    LoadRule rule = Load.on(Tier.T1, 1).andOn(Tier.T2, 2).forever();
    rule.run(wikiSegment, handler());

    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.ASSIGNED, Tier.T1, DS.WIKI));
    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.ASSIGNED, Tier.T2, DS.WIKI));
  }

  @Test
  public void testSegmentIsNotOverAssigned()
  {
    ServerHolder server1 = createServer(Tier.T1);
    ServerHolder server2 = createServer(Tier.T1);
    druidCluster.add(server1).add(server2);

    final LoadRule rule = Load.on(Tier.T1, 1).forever();
    rule.run(wikiSegment, handler());
    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.ASSIGNED, Tier.T1, wikiSegment.getDataSource()));
    Assert.assertEquals(1, server1.getLoadingSegments().size() + server2.getLoadingSegments().size());

    // Verify that multiple runs don't assign primary segment again if at replication count
    rule.run(wikiSegment, handler());
    Assert.assertFalse(stats.hasStat(Stats.Segments.ASSIGNED));
    Assert.assertEquals(1, server1.getLoadingSegments().size() + server2.getLoadingSegments().size());
  }

  @Test
  @Ignore("Enable this test when timeout behaviour is fixed")
  public void testOverAssignForTimedOutSegments()
  {
    druidCluster.add(createServer(Tier.T1))
                .add(createServer(Tier.T1));

    final LoadRule rule = Load.on(Tier.T1, 1).forever();
    rule.run(wikiSegment, handler());

    // Ensure that the segment is assigned to one of the historicals
    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.ASSIGNED, Tier.T1, wikiSegment.getDataSource()));

    // Ensure that the primary segment is assigned again in case the peon timed out on loading the segment
    rule.run(wikiSegment, handler());
    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.ASSIGNED, Tier.T1, DS.WIKI));
  }

  @Test
  @Ignore("Enable this test when timeout behaviour is fixed")
  public void testSkipReplicationForTimedOutSegments()
  {
    druidCluster.add(createServer(Tier.T1))
                .add(createServer(Tier.T1));

    final LoadRule rule = Load.on(Tier.T1, 1).forever();
    rule.run(wikiSegment, handler());

    // Ensure that the segment is assigned to one of the historicals
    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.ASSIGNED, Tier.T1, wikiSegment.getDataSource()));

    // Add the segment to the timed out list to simulate peon timeout on loading the segment
    // Default behavior is to not replicate the timed out segments on other servers
    rule.run(wikiSegment, handler());
    Assert.assertFalse(stats.hasStat(Stats.Segments.ASSIGNED));
  }

  @Test
  public void testLoadUsedSegmentsForAllSegmentGranularityAndCachingCostBalancerStrategy()
  {
    final List<DataSegment> segments =
        CreateDataSegments.ofDatasource(DS.WIKI)
                          .forIntervals(1, Granularities.ALL)
                          .withNumPartitions(2)
                          .eachOfSizeInMb(100);

    usedSegments = new HashSet<>(segments);
    druidCluster.add(createServer(Tier.T1));

    LoadRule rule = Load.on(Tier.T1, 1).forever();
    params = params.buildFromExisting()
                   .withBalancerStrategy(new CachingCostBalancerStrategy(ClusterCostCache.builder().build(), exec))
                   .build();
    rule.run(segments.get(1), handler());
    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.ASSIGNED, Tier.T1, DS.WIKI));
  }

  @Test
  public void testSegmentsAreDroppedIfLoadRuleHasZeroReplicas()
  {
    druidCluster
        .add(createServer(Tier.T1, wikiSegment))
        .add(createServer(Tier.T2, wikiSegment))
        .add(createServer(Tier.T2, wikiSegment));

    LoadRule rule = Load.on(Tier.T1, 0).andOn(Tier.T2, 0).forever();
    rule.run(wikiSegment, handler());

    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.DROPPED, Tier.T1, DS.WIKI));
    Assert.assertEquals(2L, stats.getSegmentStat(Stats.Segments.DROPPED, Tier.T2, DS.WIKI));
  }

  @Test
  public void testLoadIgnoresInvalidTiers()
  {
    druidCluster.add(createServer(Tier.T1));

    LoadRule rule = Load.on("invalidTier", 1).andOn(Tier.T1, 1).forever();
    rule.run(wikiSegment, handler());

    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.ASSIGNED, Tier.T1, DS.WIKI));
    Assert.assertEquals(0L, stats.getSegmentStat(Stats.Segments.ASSIGNED, "invalidTier", DS.WIKI));
  }

  @Test
  public void testDropIgnoresInvalidTiers()
  {
    // Cluster has 1 tier with 2 servers
    druidCluster
        .add(createServer(Tier.T1, wikiSegment))
        .add(createServer(Tier.T1, wikiSegment));

    LoadRule rule = Load.on("invalidTier", 1).andOn(Tier.T1, 1).forever();
    rule.run(wikiSegment, handler());

    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.DROPPED, Tier.T1, DS.WIKI));
    Assert.assertEquals(0L, stats.getSegmentStat(Stats.Segments.DROPPED, "invalidTier", DS.WIKI));
  }

  @Test
  public void testMaxLoadingQueueSize()
  {
    final int maxSegmentsInQueue = 2;
    druidCluster
        .add(
            new ServerHolder(
                createDruidServer(Tier.T1).toImmutableDruidServer(),
                new TestLoadQueuePeon(), false, maxSegmentsInQueue, 10
            )
        );

    CoordinatorDynamicConfig dynamicConfig = CoordinatorDynamicConfig
        .builder()
        .withSmartSegmentLoading(false)
        .withMaxSegmentsInNodeLoadingQueue(maxSegmentsInQueue)
        .withUseRoundRobinSegmentAssignment(useRoundRobinAssignment)
        .build();
    params = params.buildFromExisting().withDynamicConfigs(dynamicConfig).build();

    final LoadRule rule = Load.on(Tier.T1, 1).forever();

    rule.run(wikiSegment, handler());
    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.ASSIGNED, Tier.T1, DS.WIKI));

    rule.run(koalaSegment, handler());
    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.ASSIGNED, Tier.T1, DS.KOALA));

    final DataSegment koalaSegment2 = createDataSegment(DS.KOALA);
    usedSegments.add(koalaSegment2);

    rule.run(koalaSegment2, handler());
    Assert.assertEquals(0L, stats.getSegmentStat(Stats.Segments.ASSIGNED, Tier.T1, DS.KOALA));
  }

  @Test
  public void testSegmentIsAssignedOnlyToActiveServer()
  {
    final ServerHolder decommServerT1 = createDecommissioningServer(Tier.T1);
    final ServerHolder serverT2 = createServer(Tier.T2);

    druidCluster.add(decommServerT1, serverT2);

    // Load rule requires 1 replica on each tier
    LoadRule rule = Load.on(Tier.T1, 1).andOn(Tier.T2, 1).forever();
    rule.run(wikiSegment, handler());

    // Verify that segment is not loaded on decommissioning server
    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.ASSIGNED, Tier.T2, DS.WIKI));
    Assert.assertEquals(0, decommServerT1.getLoadingSegments().size());
    Assert.assertTrue(serverT2.getLoadingSegments().contains(wikiSegment));
  }

  @Test
  public void testSegmentIsAssignedOnlyToActiveServers()
  {
    // 2 tiers with 2 servers each, 1 server is decommissioning
    final ServerHolder decommServerT11 = createDecommissioningServer(Tier.T1);
    druidCluster
        .add(decommServerT11, createServer(Tier.T1))
        .add(createServer(Tier.T2), createServer(Tier.T2));

    // Load rule requires 2 replicas on each server
    LoadRule rule = Load.on(Tier.T1, 2).andOn(Tier.T2, 2).forever();
    rule.run(wikiSegment, handler());

    // Verify that no replica is assigned to decommissioning server
    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.ASSIGNED, Tier.T1, DS.WIKI));
    Assert.assertTrue(decommServerT11.getLoadingSegments().isEmpty());
    Assert.assertEquals(0, decommServerT11.getLoadingSegments().size());

    Assert.assertEquals(2L, stats.getSegmentStat(Stats.Segments.ASSIGNED, Tier.T2, DS.WIKI));
  }

  @Test
  public void testSegmentIsDroppedFromDecommissioningServer()
  {
    druidCluster.add(createDecommissioningServer(Tier.T1, wikiSegment));

    LoadRule rule = Load.on(Tier.T1, 0).forever();
    rule.run(wikiSegment, handler());
    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.DROPPED, Tier.T1, wikiSegment.getDataSource()));
  }

  @Test
  public void testExtraReplicasAreDroppedFromDecommissioningServer()
  {
    // 3 servers, each serving the same segment
    final ServerHolder serverT11 = createServer(Tier.T1, wikiSegment);
    final ServerHolder decommServerT12 = createDecommissioningServer(Tier.T1, wikiSegment);
    final ServerHolder serverT13 = createServer(Tier.T1, wikiSegment);

    druidCluster.add(serverT11, decommServerT12, serverT13);

    // Load rule requires 2 replicas
    LoadRule rule = Load.on(Tier.T1, 2).forever();
    rule.run(wikiSegment, handler());

    // Verify that the extra replica is dropped from the decommissioning server
    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.DROPPED, Tier.T1, DS.WIKI));
    Assert.assertEquals(0, serverT11.getPeon().getSegmentsToDrop().size());
    Assert.assertEquals(1, decommServerT12.getPeon().getSegmentsToDrop().size());
    Assert.assertEquals(0, serverT13.getPeon().getSegmentsToDrop().size());
  }

  private DataSegment createDataSegment(String dataSource)
  {
    return CreateDataSegments.ofDatasource(dataSource).eachOfSizeInMb(500).get(0);
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

  /**
   * Resets the state of the test.
   * @return Handler to use in {@link LoadRule#run}.
   */
  private SegmentActionHandler handler()
  {
    stats.clear();
    params = params
        .buildFromExisting()
        .withDruidCluster(druidCluster.build())
        .withUsedSegments(usedSegments)
        .withSegmentAssignerUsing(new SegmentLoadQueueManager(null, null))
        .build();

    return params.getSegmentAssigner();
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(LoadRule.class)
                  .withNonnullFields("tieredReplicants")
                  .withIgnoredFields("shouldSegmentBeLoaded")
                  .usingGetClass()
                  .verify();
  }
}
