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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.client.DruidServer;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.BalancerStrategy;
import org.apache.druid.server.coordinator.CachingCostBalancerStrategy;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.server.coordinator.CoordinatorRuntimeParamsTestHelpers;
import org.apache.druid.server.coordinator.CostBalancerStrategyFactory;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.server.coordinator.DruidClusterBuilder;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.LoadQueuePeon;
import org.apache.druid.server.coordinator.LoadQueuePeonTester;
import org.apache.druid.server.coordinator.ReplicationThrottler;
import org.apache.druid.server.coordinator.SegmentAction;
import org.apache.druid.server.coordinator.SegmentLoader;
import org.apache.druid.server.coordinator.SegmentReplicantLookup;
import org.apache.druid.server.coordinator.SegmentStateManager;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.cost.ClusterCostCache;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.Stats;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.assertj.core.util.Sets;
import org.easymock.EasyMock;
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
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
@RunWith(Parameterized.class)
public class LoadRuleTest
{
  private ListeningExecutorService exec;
  private BalancerStrategy balancerStrategy;

  private CachingCostBalancerStrategy cachingCostBalancerStrategy;

  private SegmentStateManager stateManager;
  private final boolean useRoundRobinAssignment;
  private BalancerStrategy mockBalancerStrategy;

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
    balancerStrategy = new CostBalancerStrategyFactory().createBalancerStrategy(exec);
    cachingCostBalancerStrategy = new CachingCostBalancerStrategy(ClusterCostCache.builder().build(), exec);

    mockBalancerStrategy = EasyMock.createMock(BalancerStrategy.class);
    stateManager = new SegmentStateManager(null, null, null);
  }

  @After
  public void tearDown() throws Exception
  {
    exec.shutdown();
  }

  @Test
  public void testLoad()
  {
    final LoadQueuePeon mockPeon = createEmptyPeon();
    mockPeon.loadSegment(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();

    LoadRule rule = createLoadRule(ImmutableMap.of(
        "hot", 1,
        DruidServer.DEFAULT_TIER, 2
    ));

    final DataSegment segment = createDataSegment("foo");

    if (!useRoundRobinAssignment) {
      EasyMock.expect(mockBalancerStrategy.findNewSegmentHomeReplicator(EasyMock.anyObject(), EasyMock.anyObject()))
              .andDelegateTo(balancerStrategy)
              .times(2);
    }

    EasyMock.replay(mockPeon, mockBalancerStrategy);

    DruidCluster druidCluster = DruidClusterBuilder
        .newBuilder()
        .addTier(
            "hot",
            new ServerHolder(
                new DruidServer("serverHot", "hostHot", null, 1000, ServerType.HISTORICAL, "hot", 1)
                    .toImmutableDruidServer(),
                mockPeon
            )
        )
        .addTier(
            DruidServer.DEFAULT_TIER,
            new ServerHolder(
                new DruidServer(
                    "serverNorm",
                    "hostNorm",
                    null,
                    1000,
                    ServerType.HISTORICAL,
                    DruidServer.DEFAULT_TIER,
                    0
                ).toImmutableDruidServer(),
                mockPeon
            )
        )
        .build();

    CoordinatorRunStats stats = runRuleAndGetStats(rule, segment, druidCluster);

    Assert.assertEquals(1L, stats.getTieredStat(Stats.Segments.ASSIGNED, "hot"));
    Assert.assertEquals(1L, stats.getTieredStat(Stats.Segments.ASSIGNED, DruidServer.DEFAULT_TIER));

    EasyMock.verify(mockPeon, mockBalancerStrategy);
  }

  private CoordinatorRunStats runRuleAndGetStats(
      LoadRule rule,
      DataSegment segment,
      DruidCluster cluster
  )
  {
    return runRuleAndGetStats(rule, segment, makeCoordinatorRuntimeParams(cluster, segment));
  }

  private CoordinatorRunStats runRuleAndGetStats(
      LoadRule rule,
      DataSegment segment,
      DruidCoordinatorRuntimeParams params
  )
  {
    final CoordinatorDynamicConfig dynamicConfig = params.getCoordinatorDynamicConfig();
    ReplicationThrottler throttler = new ReplicationThrottler(
        Sets.newHashSet(params.getDruidCluster().getTierNames()),
        dynamicConfig.getReplicationThrottleLimit(),
        dynamicConfig.getReplicantLifetime(),
        dynamicConfig.getMaxNonPrimaryReplicantsToLoad()
    );
    SegmentLoader loader = new SegmentLoader(
        stateManager,
        params.getDruidCluster(),
        params.getSegmentReplicantLookup(),
        throttler,
        params.getBalancerStrategy(),
        useRoundRobinAssignment
    );
    rule.run(segment, loader);
    return loader.getStats();
  }

  private DruidCoordinatorRuntimeParams makeCoordinatorRuntimeParams(
      DruidCluster druidCluster,
      DataSegment... usedSegments
  )
  {
    return makeCoordinatorRuntimeParams(druidCluster, false, usedSegments);
  }

  private DruidCoordinatorRuntimeParams makeCoordinatorRuntimeParams(
      DruidCluster druidCluster,
      boolean replicateAfterLoadTimeout,
      DataSegment... usedSegments
  )
  {
    return CoordinatorRuntimeParamsTestHelpers
        .newBuilder()
        .withDruidCluster(druidCluster)
        .withSegmentReplicantLookup(SegmentReplicantLookup.make(druidCluster, replicateAfterLoadTimeout))
        .withBalancerStrategy(mockBalancerStrategy)
        .withUsedSegmentsInTest(usedSegments)
        .withDynamicConfigs(
            CoordinatorDynamicConfig.builder()
                                    .withUseRoundRobinSegmentAssignment(useRoundRobinAssignment)
                                    .build()
        )
        .build();
  }

  @Test
  public void testLoadPrimaryAssignDoesNotOverAssign()
  {
    final LoadQueuePeon mockPeon = createEmptyPeon();
    mockPeon.loadSegment(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();

    LoadRule rule = createLoadRule(ImmutableMap.of("hot", 1));

    final DataSegment segment = createDataSegment("foo");

    EasyMock.expect(mockBalancerStrategy.findNewSegmentHomeReplicator(EasyMock.anyObject(), EasyMock.anyObject()))
            .andDelegateTo(balancerStrategy)
            .anyTimes();

    EasyMock.replay(mockPeon, mockBalancerStrategy);

    ImmutableDruidServer server1 =
        new DruidServer("serverHot", "hostHot", null, 1000, ServerType.HISTORICAL, "hot", 1).toImmutableDruidServer();
    ImmutableDruidServer server2 =
        new DruidServer("serverHot2", "hostHot2", null, 1000, ServerType.HISTORICAL, "hot", 1).toImmutableDruidServer();
    DruidCluster druidCluster = DruidClusterBuilder
        .newBuilder()
        .addTier("hot", new ServerHolder(server1, mockPeon), new ServerHolder(server2, mockPeon))
        .build();

    CoordinatorRunStats stats = runRuleAndGetStats(rule, segment, druidCluster);
    Assert.assertEquals(1L, stats.getTieredStat(Stats.Segments.ASSIGNED, "hot"));

    // ensure multiple runs don't assign primary segment again if at replication count
    final LoadQueuePeon loadingPeon = createLoadingPeon(segment, false);
    EasyMock.replay(loadingPeon);

    DruidCluster afterLoad = DruidClusterBuilder
        .newBuilder()
        .addTier("hot", new ServerHolder(server1, loadingPeon), new ServerHolder(server2, mockPeon))
        .build();

    CoordinatorRunStats statsAfterLoadPrimary = runRuleAndGetStats(rule, segment, afterLoad);

    Assert.assertFalse(statsAfterLoadPrimary.hasStat(Stats.Segments.ASSIGNED));

    EasyMock.verify(mockPeon, mockBalancerStrategy);
  }

  @Test
  @Ignore("Enable this test when timeout behaviour is fixed")
  public void testOverAssignForTimedOutSegments()
  {
    final LoadQueuePeon emptyPeon = createEmptyPeon();
    emptyPeon.loadSegment(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();

    LoadRule rule = createLoadRule(ImmutableMap.of("hot", 1));

    final DataSegment segment = createDataSegment("foo");

    EasyMock.expect(mockBalancerStrategy.findNewSegmentHomeReplicator(EasyMock.anyObject(), EasyMock.anyObject()))
            .andDelegateTo(balancerStrategy)
            .anyTimes();

    EasyMock.replay(emptyPeon, mockBalancerStrategy);

    ImmutableDruidServer server1 =
        new DruidServer("serverHot", "hostHot", null, 1000, ServerType.HISTORICAL, "hot", 1).toImmutableDruidServer();
    ImmutableDruidServer server2 =
        new DruidServer("serverHot2", "hostHot2", null, 1000, ServerType.HISTORICAL, "hot", 1).toImmutableDruidServer();
    DruidCluster druidCluster = DruidClusterBuilder
        .newBuilder()
        .addTier("hot", new ServerHolder(server1, emptyPeon), new ServerHolder(server2, emptyPeon))
        .build();

    CoordinatorRunStats stats = runRuleAndGetStats(
        rule,
        segment,
        makeCoordinatorRuntimeParams(druidCluster, true, segment)
    );

    // Ensure that the segment is assigned to one of the historicals
    Assert.assertEquals(1L, stats.getTieredStat(Stats.Segments.ASSIGNED, "hot"));

    // Ensure that the primary segment is assigned again in case the peon timed out on loading the segment
    final LoadQueuePeon slowLoadingPeon = createLoadingPeon(segment, true);
    EasyMock.replay(slowLoadingPeon);

    DruidCluster withLoadTimeout = DruidClusterBuilder
        .newBuilder()
        .addTier("hot", new ServerHolder(server1, slowLoadingPeon), new ServerHolder(server2, emptyPeon))
        .build();

    CoordinatorRunStats statsAfterLoadPrimary = runRuleAndGetStats(
        rule,
        segment,
        makeCoordinatorRuntimeParams(withLoadTimeout, true, segment)
    );

    Assert.assertEquals(1L, statsAfterLoadPrimary.getTieredStat(Stats.Segments.ASSIGNED, "hot"));

    EasyMock.verify(emptyPeon, mockBalancerStrategy);
  }

  @Test
  public void testSkipReplicationForTimedOutSegments()
  {
    final LoadQueuePeon emptyPeon = createEmptyPeon();
    emptyPeon.loadSegment(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();

    LoadRule rule = createLoadRule(ImmutableMap.of("hot", 1));

    final DataSegment segment = createDataSegment("foo");

    EasyMock.expect(mockBalancerStrategy.findNewSegmentHomeReplicator(EasyMock.anyObject(), EasyMock.anyObject()))
            .andDelegateTo(balancerStrategy)
            .anyTimes();

    EasyMock.replay(emptyPeon, mockBalancerStrategy);

    ImmutableDruidServer server1 =
        new DruidServer("serverHot", "hostHot", null, 1000, ServerType.HISTORICAL, "hot", 1).toImmutableDruidServer();
    ImmutableDruidServer server2 =
        new DruidServer("serverHot2", "hostHot2", null, 1000, ServerType.HISTORICAL, "hot", 1).toImmutableDruidServer();
    DruidCluster druidCluster = DruidClusterBuilder
        .newBuilder()
        .addTier("hot", new ServerHolder(server1, emptyPeon), new ServerHolder(server2, emptyPeon))
        .build();

    CoordinatorRunStats stats = runRuleAndGetStats(rule, segment, druidCluster);

    // Ensure that the segment is assigned to one of the historicals
    Assert.assertEquals(1L, stats.getTieredStat(Stats.Segments.ASSIGNED, "hot"));

    // Add the segment to the timed out list to simulate peon timeout on loading the segment
    final LoadQueuePeon slowLoadingPeon = createLoadingPeon(segment, true);
    EasyMock.replay(slowLoadingPeon);

    DruidCluster withLoadTimeout = DruidClusterBuilder
        .newBuilder()
        .addTier("hot", new ServerHolder(server1, slowLoadingPeon), new ServerHolder(server2, emptyPeon))
        .build();

    // Default behavior is to not replicate the timed out segments on other servers
    CoordinatorRunStats statsAfterLoadPrimary = runRuleAndGetStats(rule, segment, withLoadTimeout);

    Assert.assertFalse(statsAfterLoadPrimary.hasStat(Stats.Segments.ASSIGNED));

    EasyMock.verify(emptyPeon, mockBalancerStrategy);
  }

  @Test
  public void testLoadUsedSegmentsForAllSegmentGranularityAndCachingCostBalancerStrategy()
  {
    LoadRule rule = createLoadRule(ImmutableMap.of("tier1", 1));

    List<DataSegment> segments =
        CreateDataSegments.ofDatasource("foo")
                          .forIntervals(1, Granularities.ALL)
                          .withNumPartitions(2)
                          .eachOfSizeInMb(100);

    final LoadQueuePeon loadingPeon = createLoadingPeon(segments.get(0), true);

    loadingPeon.loadSegment(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().once();

    EasyMock.expect(mockBalancerStrategy.findNewSegmentHomeReplicator(EasyMock.anyObject(), EasyMock.anyObject()))
            .andDelegateTo(cachingCostBalancerStrategy)
            .anyTimes();

    EasyMock.replay(loadingPeon, mockBalancerStrategy);

    ImmutableDruidServer server = createServer("tier1").toImmutableDruidServer();

    DruidCluster druidCluster = DruidClusterBuilder
        .newBuilder()
        .addTier("tier1", new ServerHolder(server, loadingPeon))
        .build();

    final CoordinatorRunStats stats = runRuleAndGetStats(
        rule,
        segments.get(1),
        makeCoordinatorRuntimeParams(druidCluster, segments.toArray(new DataSegment[0]))
    );

    Assert.assertEquals(1L, stats.getTieredStat(Stats.Segments.ASSIGNED, "tier1"));

    EasyMock.verify(loadingPeon, mockBalancerStrategy);
  }

  @Test
  @Ignore("Segment should be quickly available on all target tiers irrespective of priority")
  public void testLoadPriority()
  {
    final LoadQueuePeon mockPeon1 = createEmptyPeon();
    final LoadQueuePeon mockPeon2 = createEmptyPeon();

    mockPeon2.loadSegment(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.isNull());
    EasyMock.expectLastCall().once();

    if (!useRoundRobinAssignment) {
      EasyMock.expect(mockBalancerStrategy.findNewSegmentHomeReplicator(EasyMock.anyObject(), EasyMock.anyObject()))
              .andDelegateTo(balancerStrategy)
              .times(2);
    }

    EasyMock.replay(mockPeon1, mockPeon2, mockBalancerStrategy);

    final LoadRule rule = createLoadRule(ImmutableMap.of("tier1", 10, "tier2", 10));

    final DruidCluster druidCluster = DruidClusterBuilder
        .newBuilder()
        .addTier(
            "tier1",
            new ServerHolder(
                new DruidServer("server1", "host1", null, 1000, ServerType.HISTORICAL, "tier1", 0)
                    .toImmutableDruidServer(),
                mockPeon1
            )
        )
        .addTier(
            "tier2",
            new ServerHolder(
                new DruidServer("server2", "host2", null, 1000, ServerType.HISTORICAL, "tier2", 1)
                    .toImmutableDruidServer(),
                mockPeon2
            ),
            new ServerHolder(
                new DruidServer("server3", "host3", null, 1000, ServerType.HISTORICAL, "tier2", 1)
                    .toImmutableDruidServer(),
                mockPeon2
            )
        )
        .build();

    final DataSegment segment = createDataSegment("foo");

    final CoordinatorRunStats stats = runRuleAndGetStats(rule, segment, druidCluster);

    Assert.assertEquals(0L, stats.getTieredStat(Stats.Segments.ASSIGNED, "tier1"));
    Assert.assertEquals(1L, stats.getTieredStat(Stats.Segments.ASSIGNED, "tier2"));

    EasyMock.verify(mockPeon1, mockPeon2, mockBalancerStrategy);
  }

  @Test
  public void testDrop()
  {
    final LoadQueuePeon mockPeon = createEmptyPeon();
    mockPeon.dropSegment(EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.expect(mockBalancerStrategy.pickServersToDrop(EasyMock.anyObject(), EasyMock.anyObject()))
            .andDelegateTo(balancerStrategy)
            .times(4);
    EasyMock.replay(mockPeon, mockBalancerStrategy);

    LoadRule rule = createLoadRule(ImmutableMap.of(
        "hot", 0,
        DruidServer.DEFAULT_TIER, 0
    ));

    final DataSegment segment = createDataSegment("foo");

    DruidServer server1 = new DruidServer("serverHot", "hostHot", null, 1000, ServerType.HISTORICAL, "hot", 0);
    server1.addDataSegment(segment);
    DruidServer server2 = new DruidServer(
        "serverNorm",
        "hostNorm",
        null,
        1000,
        ServerType.HISTORICAL,
        DruidServer.DEFAULT_TIER,
        0
    );
    server2.addDataSegment(segment);
    DruidServer server3 = new DruidServer(
        "serverNormNotServing",
        "hostNorm",
        null,
        10,
        ServerType.HISTORICAL,
        DruidServer.DEFAULT_TIER,
        0
    );
    DruidCluster druidCluster = DruidClusterBuilder
        .newBuilder()
        .addTier("hot", new ServerHolder(server1.toImmutableDruidServer(), mockPeon))
        .addTier(
            DruidServer.DEFAULT_TIER,
            new ServerHolder(server2.toImmutableDruidServer(), mockPeon),
            new ServerHolder(server3.toImmutableDruidServer(), mockPeon)
        )
        .build();

    CoordinatorRunStats stats = runRuleAndGetStats(rule, segment, druidCluster);

    Assert.assertEquals(1L, stats.getTieredStat(Stats.Segments.DROPPED, "hot"));
    Assert.assertEquals(1L, stats.getTieredStat(Stats.Segments.DROPPED, DruidServer.DEFAULT_TIER));

    EasyMock.verify(mockPeon);
  }

  @Test
  public void testLoadWithNonExistentTier()
  {
    final LoadQueuePeon mockPeon = createEmptyPeon();
    mockPeon.loadSegment(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();

    if (!useRoundRobinAssignment) {
      EasyMock.expect(mockBalancerStrategy.findNewSegmentHomeReplicator(EasyMock.anyObject(), EasyMock.anyObject()))
              .andDelegateTo(balancerStrategy)
              .times(1);
    }

    EasyMock.replay(mockPeon, mockBalancerStrategy);

    LoadRule rule = createLoadRule(ImmutableMap.of("nonExistentTier", 1, "hot", 1));

    DruidCluster druidCluster = DruidClusterBuilder
        .newBuilder()
        .addTier(
            "hot",
            new ServerHolder(
                new DruidServer("serverHot", "hostHot", null, 1000, ServerType.HISTORICAL, "hot", 0)
                    .toImmutableDruidServer(),
                mockPeon
            )
        )
        .build();

    final DataSegment segment = createDataSegment("foo");
    CoordinatorRunStats stats = runRuleAndGetStats(rule, segment, druidCluster);

    Assert.assertEquals(1L, stats.getTieredStat(Stats.Segments.ASSIGNED, "hot"));

    EasyMock.verify(mockPeon, mockBalancerStrategy);
  }

  @Test
  public void testDropWithNonExistentTier()
  {
    final LoadQueuePeon mockPeon = createEmptyPeon();
    mockPeon.dropSegment(EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.replay(mockPeon, mockBalancerStrategy);

    LoadRule rule = createLoadRule(ImmutableMap.of("nonExistentTier", 1, "hot", 1));

    final DataSegment segment = createDataSegment("foo");

    DruidServer server1 = new DruidServer("serverHot", "hostHot", null, 1000, ServerType.HISTORICAL, "hot", 0);
    DruidServer server2 = new DruidServer("serverHot2", "hostHot2", null, 1000, ServerType.HISTORICAL, "hot", 0);
    server1.addDataSegment(segment);
    server2.addDataSegment(segment);

    DruidCluster druidCluster = DruidClusterBuilder
        .newBuilder()
        .addTier(
            "hot",
            new ServerHolder(server1.toImmutableDruidServer(), mockPeon),
            new ServerHolder(server2.toImmutableDruidServer(), mockPeon)
        )
        .build();

    CoordinatorRunStats stats = runRuleAndGetStats(rule, segment, druidCluster);

    Assert.assertEquals(1L, stats.getTieredStat(Stats.Segments.DROPPED, "hot"));

    EasyMock.verify(mockPeon, mockBalancerStrategy);
  }

  @Test
  public void testMaxLoadingQueueSize()
  {
    if (!useRoundRobinAssignment) {
      EasyMock.expect(mockBalancerStrategy.findNewSegmentHomeReplicator(EasyMock.anyObject(), EasyMock.anyObject()))
              .andDelegateTo(balancerStrategy)
              .times(2);
    }
    EasyMock.replay(mockBalancerStrategy);

    final LoadQueuePeonTester peon = new LoadQueuePeonTester();

    LoadRule rule = createLoadRule(ImmutableMap.of("hot", 1));

    final int maxSegmentsInQueue = 2;
    DruidCluster druidCluster = DruidClusterBuilder
        .newBuilder()
        .addTier(
            "hot",
            new ServerHolder(
                new DruidServer("serverHot", "hostHot", null, 1000, ServerType.HISTORICAL, "hot", 0)
                    .toImmutableDruidServer(),
                peon,
                false,
                maxSegmentsInQueue
            )
        )
        .build();

    DataSegment dataSegment1 = createDataSegment("ds1");
    DataSegment dataSegment2 = createDataSegment("ds2");
    DataSegment dataSegment3 = createDataSegment("ds3");

    DruidCoordinatorRuntimeParams params = CoordinatorRuntimeParamsTestHelpers
        .newBuilder()
        .withDruidCluster(druidCluster)
        .withSegmentReplicantLookup(SegmentReplicantLookup.make(druidCluster, false))
        .withBalancerStrategy(mockBalancerStrategy)
        .withUsedSegmentsInTest(dataSegment1, dataSegment2, dataSegment3)
        .withDynamicConfigs(
            CoordinatorDynamicConfig.builder()
                                    .withMaxSegmentsInNodeLoadingQueue(maxSegmentsInQueue)
                                    .withUseRoundRobinSegmentAssignment(useRoundRobinAssignment)
                                    .build()
        ).build();

    CoordinatorRunStats stats1 = runRuleAndGetStats(rule, dataSegment1, params);
    CoordinatorRunStats stats2 = runRuleAndGetStats(rule, dataSegment2, params);
    CoordinatorRunStats stats3 = runRuleAndGetStats(rule, dataSegment3, params);

    Assert.assertEquals(1L, stats1.getTieredStat(Stats.Segments.ASSIGNED, "hot"));
    Assert.assertEquals(1L, stats2.getTieredStat(Stats.Segments.ASSIGNED, "hot"));
    Assert.assertEquals(0L, stats3.getTieredStat(Stats.Segments.ASSIGNED, "hot"));

    EasyMock.verify(mockBalancerStrategy);
  }

  /**
   * 2 servers in different tiers, the first is decommissioning.
   * Should not load a segment to the server that is decommissioning
   */
  @Test
  public void testLoadDecommissioning()
  {
    final LoadQueuePeon mockPeon1 = createEmptyPeon();
    final LoadQueuePeon mockPeon2 = createOneCallPeonMock();

    LoadRule rule = createLoadRule(ImmutableMap.of("tier1", 1, "tier2", 1));

    final DataSegment segment = createDataSegment("foo");

    if (!useRoundRobinAssignment) {
      EasyMock.expect(mockBalancerStrategy.findNewSegmentHomeReplicator(EasyMock.anyObject(), EasyMock.anyObject()))
              .andDelegateTo(balancerStrategy)
              .times(1);
    }

    EasyMock.replay(mockPeon1, mockPeon2, mockBalancerStrategy);

    DruidCluster druidCluster = DruidClusterBuilder
        .newBuilder()
        .addTier("tier1", createServerHolder("tier1", mockPeon1, true))
        .addTier("tier2", createServerHolder("tier2", mockPeon2, false))
        .build();

    CoordinatorRunStats stats = runRuleAndGetStats(rule, segment, druidCluster);

    Assert.assertEquals(1L, stats.getTieredStat(Stats.Segments.ASSIGNED, "tier2"));
    EasyMock.verify(mockPeon1, mockPeon2, mockBalancerStrategy);
  }

  /**
   * 2 tiers, 2 servers each, 1 server of the second tier is decommissioning.
   * Should not load a segment to the server that is decommssioning.
   */
  @Test
  public void testLoadReplicaDuringDecommissioning()
  {
    final LoadQueuePeon mockPeon1 = createEmptyPeon();
    final LoadQueuePeon mockPeon2 = createOneCallPeonMock();
    final LoadQueuePeon mockPeon3 = createOneCallPeonMock();
    final LoadQueuePeon mockPeon4 = createOneCallPeonMock();
    EasyMock.replay(mockPeon1, mockPeon2, mockPeon3, mockPeon4);

    LoadRule rule = createLoadRule(ImmutableMap.of("tier1", 2, "tier2", 2));

    final DataSegment segment = createDataSegment("foo");

    ServerHolder holder1 = createServerHolder("tier1", mockPeon1, true);
    ServerHolder holder2 = createServerHolder("tier1", mockPeon2, false);
    ServerHolder holder3 = createServerHolder("tier2", mockPeon3, false);
    ServerHolder holder4 = createServerHolder("tier2", mockPeon4, false);

    if (!useRoundRobinAssignment) {
      EasyMock.expect(mockBalancerStrategy.findNewSegmentHomeReplicator(segment, ImmutableList.of(holder2)))
              .andReturn(Collections.singletonList(holder2).iterator());
      EasyMock.expect(mockBalancerStrategy.findNewSegmentHomeReplicator(segment, ImmutableList.of(holder4, holder3)))
              .andReturn(Arrays.asList(holder3, holder4).iterator());
    }

    EasyMock.replay(mockBalancerStrategy);

    DruidCluster druidCluster = DruidClusterBuilder
        .newBuilder()
        .addTier("tier1", holder1, holder2)
        .addTier("tier2", holder3, holder4)
        .build();
    CoordinatorRunStats stats = runRuleAndGetStats(rule, segment, druidCluster);

    Assert.assertEquals(1L, stats.getTieredStat(Stats.Segments.ASSIGNED, "tier1"));
    Assert.assertEquals(2L, stats.getTieredStat(Stats.Segments.ASSIGNED, "tier2"));

    EasyMock.verify(mockPeon1, mockPeon2, mockPeon3, mockPeon4, mockBalancerStrategy);
  }

  /**
   * 2 servers with a segment, one server decommissioning.
   * Should drop a segment from both.
   */
  @Test
  public void testDropDuringDecommissioning()
  {
    final LoadQueuePeon mockPeon = createEmptyPeon();
    mockPeon.dropSegment(EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().times(2);
    EasyMock.expect(mockBalancerStrategy.pickServersToDrop(EasyMock.anyObject(), EasyMock.anyObject()))
            .andDelegateTo(balancerStrategy)
            .times(4);
    EasyMock.replay(mockPeon, mockBalancerStrategy);

    LoadRule rule = createLoadRule(ImmutableMap.of("tier1", 0));

    final DataSegment segment1 = createDataSegment("foo1");
    final DataSegment segment2 = createDataSegment("foo2");

    DruidServer server1 = createServer("tier1");
    server1.addDataSegment(segment1);
    DruidServer server2 = createServer("tier1");
    server2.addDataSegment(segment2);

    DruidCluster druidCluster = DruidClusterBuilder
        .newBuilder()
        .addTier(
            "tier1",
            new ServerHolder(server1.toImmutableDruidServer(), mockPeon, true),
            new ServerHolder(server2.toImmutableDruidServer(), mockPeon, false)
        )
        .build();

    DruidCoordinatorRuntimeParams params = makeCoordinatorRuntimeParams(druidCluster, segment1, segment2);
    CoordinatorRunStats stats = runRuleAndGetStats(rule, segment1, params);
    Assert.assertEquals(1L, stats.getTieredStat(Stats.Segments.DROPPED, "tier1"));
    stats = runRuleAndGetStats(rule, segment2, params);
    Assert.assertEquals(1L, stats.getTieredStat(Stats.Segments.DROPPED, "tier1"));

    EasyMock.verify(mockPeon);
  }

  /**
   * 3 servers hosting 3 replicas of the segment.
   * 1 servers is decommissioning.
   * 1 replica is redundant.
   * Should drop from the decommissioning server.
   */
  @Test
  public void testRedundantReplicaDropDuringDecommissioning()
  {
    final LoadQueuePeon mockPeon1 = new LoadQueuePeonTester();
    final LoadQueuePeon mockPeon2 = new LoadQueuePeonTester();
    final LoadQueuePeon mockPeon3 = new LoadQueuePeonTester();
    EasyMock.expect(mockBalancerStrategy.pickServersToDrop(EasyMock.anyObject(), EasyMock.anyObject()))
            .andDelegateTo(balancerStrategy)
            .times(4);
    EasyMock.replay(mockBalancerStrategy);

    LoadRule rule = createLoadRule(ImmutableMap.of("tier1", 2));

    final DataSegment segment1 = createDataSegment("foo1");

    DruidServer server1 = createServer("tier1");
    server1.addDataSegment(segment1);
    DruidServer server2 = createServer("tier1");
    server2.addDataSegment(segment1);
    DruidServer server3 = createServer("tier1");
    server3.addDataSegment(segment1);

    DruidCluster druidCluster = DruidClusterBuilder
        .newBuilder()
        .addTier(
            "tier1",
            new ServerHolder(server1.toImmutableDruidServer(), mockPeon1, false),
            new ServerHolder(server2.toImmutableDruidServer(), mockPeon2, true),
            new ServerHolder(server3.toImmutableDruidServer(), mockPeon3, false)
        )
        .build();

    CoordinatorRunStats stats = runRuleAndGetStats(rule, segment1, makeCoordinatorRuntimeParams(druidCluster, segment1));
    Assert.assertEquals(1L, stats.getTieredStat(Stats.Segments.DROPPED, "tier1"));
    Assert.assertEquals(0, mockPeon1.getSegmentsToDrop().size());
    Assert.assertEquals(1, mockPeon2.getSegmentsToDrop().size());
    Assert.assertEquals(0, mockPeon3.getSegmentsToDrop().size());
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

  private static LoadRule createLoadRule(final Map<String, Integer> tieredReplicants)
  {
    return new ForeverLoadRule(tieredReplicants);
  }

  private static LoadQueuePeon createEmptyPeon()
  {
    final LoadQueuePeon mockPeon = EasyMock.createMock(LoadQueuePeon.class);
    EasyMock.expect(mockPeon.getSegmentsToLoad()).andReturn(Collections.emptySet()).anyTimes();
    EasyMock.expect(mockPeon.getSegmentsInQueue()).andReturn(Collections.emptyMap()).anyTimes();
    EasyMock.expect(mockPeon.getSegmentsMarkedToDrop()).andReturn(Collections.emptySet()).anyTimes();
    EasyMock.expect(mockPeon.getSizeOfSegmentsToLoad()).andReturn(0L).anyTimes();
    EasyMock.expect(mockPeon.getNumberOfSegmentsToLoad()).andReturn(0).anyTimes();

    return mockPeon;
  }

  private static LoadQueuePeon createLoadingPeon(DataSegment segment, boolean slowLoading)
  {
    final Set<DataSegment> segs = Collections.singleton(segment);

    final LoadQueuePeon mockPeon = EasyMock.createMock(LoadQueuePeon.class);
    EasyMock.expect(mockPeon.getSegmentsToLoad()).andReturn(segs).anyTimes();
    EasyMock.expect(mockPeon.getSegmentsMarkedToDrop()).andReturn(Collections.emptySet()).anyTimes();
    EasyMock.expect(mockPeon.getSegmentsInQueue())
            .andReturn(ImmutableMap.of(segment, SegmentAction.LOAD)).anyTimes();
    EasyMock.expect(mockPeon.getNumberOfSegmentsToLoad()).andReturn(segs.size()).anyTimes();

    EasyMock.expect(mockPeon.getTimedOutSegments())
            .andReturn(slowLoading ? segs : Collections.emptySet()).anyTimes();

    return mockPeon;
  }

  private DruidServer createServer(String tier)
  {
    final String serverName = "hist_" + tier + "_" + serverId.incrementAndGet();
    return new DruidServer(serverName, serverName, null, 10L << 30, ServerType.HISTORICAL, tier, 0);
  }

  private static LoadQueuePeon createOneCallPeonMock()
  {
    final LoadQueuePeon mockPeon2 = createEmptyPeon();
    mockPeon2.loadSegment(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().once();
    return mockPeon2;
  }

  private ServerHolder createServerHolder(String tier, LoadQueuePeon mockPeon1, boolean isDecommissioning)
  {
    return new ServerHolder(
        createServer(tier).toImmutableDruidServer(),
        mockPeon1,
        isDecommissioning
    );
  }
}
