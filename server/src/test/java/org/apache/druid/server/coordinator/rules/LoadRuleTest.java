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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.client.DruidServer;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.core.LoggingEmitter;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.BalancerStrategy;
import org.apache.druid.server.coordinator.CachingCostBalancerStrategy;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.server.coordinator.CoordinatorRuntimeParamsTestHelpers;
import org.apache.druid.server.coordinator.CoordinatorStats;
import org.apache.druid.server.coordinator.CostBalancerStrategyFactory;
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.server.coordinator.DruidClusterBuilder;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.LoadQueuePeon;
import org.apache.druid.server.coordinator.LoadQueuePeonTester;
import org.apache.druid.server.coordinator.SegmentAction;
import org.apache.druid.server.coordinator.SegmentLoader;
import org.apache.druid.server.coordinator.SegmentReplicantLookup;
import org.apache.druid.server.coordinator.SegmentStateManager;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.cost.ClusterCostCache;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public class LoadRuleTest
{
  private static final Logger log = new Logger(LoadRuleTest.class);
  private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();

  private static final ServiceEmitter EMITTER = new ServiceEmitter(
      "service",
      "host",
      new LoggingEmitter(
          log,
          LoggingEmitter.Level.ERROR,
          JSON_MAPPER
      )
  );

  private ListeningExecutorService exec;
  private BalancerStrategy balancerStrategy;

  private CachingCostBalancerStrategy cachingCostBalancerStrategy;

  private SegmentStateManager stateManager;
  private BalancerStrategy mockBalancerStrategy;

  @Before
  public void setUp()
  {
    EmittingLogger.registerEmitter(EMITTER);
    EMITTER.start();

    exec = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1));
    balancerStrategy = new CostBalancerStrategyFactory().createBalancerStrategy(exec);

    cachingCostBalancerStrategy = new CachingCostBalancerStrategy(ClusterCostCache.builder().build(), exec);

    mockBalancerStrategy = EasyMock.createMock(BalancerStrategy.class);
    stateManager = new SegmentStateManager(null, null, true);
  }

  @After
  public void tearDown() throws Exception
  {
    exec.shutdown();
    EMITTER.close();
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

    EasyMock.expect(mockBalancerStrategy.findNewSegmentHomeReplicator(EasyMock.anyObject(), EasyMock.anyObject()))
            .andDelegateTo(balancerStrategy)
            .times(2);

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

    CoordinatorStats stats = runRuleAndGetStats(rule, segment, druidCluster);

    Assert.assertEquals(1L, stats.getTieredStat(LoadRule.ASSIGNED_COUNT, "hot"));
    Assert.assertEquals(1L, stats.getTieredStat(LoadRule.ASSIGNED_COUNT, DruidServer.DEFAULT_TIER));

    EasyMock.verify(mockPeon, mockBalancerStrategy);
  }

  private CoordinatorStats runRuleAndGetStats(
      LoadRule rule,
      DataSegment segment, DruidCluster cluster
  )
  {
    return runRuleAndGetStats(rule, segment, makeCoordinatorRuntimeParams(cluster, segment));
  }

  private CoordinatorStats runRuleAndGetStats(
      LoadRule rule,
      DataSegment segment,
      DruidCoordinatorRuntimeParams params
  )
  {
    stateManager.prepareForRun(params);
    SegmentLoader loader = new SegmentLoader(stateManager, params);
    rule.run(segment, loader);
    return loader.getStats();
  }

  private DruidCoordinatorRuntimeParams makeCoordinatorRuntimeParams(
      DruidCluster druidCluster,
      DataSegment... usedSegments
  )
  {
    return CoordinatorRuntimeParamsTestHelpers
        .newBuilder()
        .withDruidCluster(druidCluster)
        .withSegmentReplicantLookup(SegmentReplicantLookup.make(druidCluster, false))
        .withBalancerStrategy(mockBalancerStrategy)
        .withUsedSegmentsInTest(usedSegments)
        .build();
  }

  private DruidCoordinatorRuntimeParams makeCoordinatorRuntimeParamsWithLoadReplicationOnTimeout(
      DruidCluster druidCluster,
      DataSegment... usedSegments
  )
  {
    return CoordinatorRuntimeParamsTestHelpers
        .newBuilder()
        .withDruidCluster(druidCluster)
        .withSegmentReplicantLookup(SegmentReplicantLookup.make(druidCluster, true))
        .withBalancerStrategy(mockBalancerStrategy)
        .withUsedSegmentsInTest(usedSegments)
        .build();
  }

  @Test
  public void testLoadPrimaryAssignDoesNotOverAssign()
  {
    final LoadQueuePeon mockPeon = createEmptyPeon();
    mockPeon.loadSegment(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();

    LoadRule rule = createLoadRule(ImmutableMap.of(
        "hot", 1
    ));

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

    CoordinatorStats stats = runRuleAndGetStats(rule, segment, druidCluster);
    Assert.assertEquals(1L, stats.getTieredStat(LoadRule.ASSIGNED_COUNT, "hot"));

    // ensure multiple runs don't assign primary segment again if at replication count
    final LoadQueuePeon loadingPeon = createLoadingPeon(segment, false);
    EasyMock.replay(loadingPeon);

    DruidCluster afterLoad = DruidClusterBuilder
        .newBuilder()
        .addTier("hot", new ServerHolder(server1, loadingPeon), new ServerHolder(server2, mockPeon))
        .build();

    CoordinatorStats statsAfterLoadPrimary = runRuleAndGetStats(rule, segment, afterLoad);

    Assert.assertTrue(statsAfterLoadPrimary.getTiers(LoadRule.ASSIGNED_COUNT).isEmpty());

    EasyMock.verify(mockPeon, mockBalancerStrategy);
  }

  @Test
  public void testOverAssignForTimedOutSegments()
  {
    final LoadQueuePeon emptyPeon = createEmptyPeon();
    emptyPeon.loadSegment(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();

    LoadRule rule = createLoadRule(ImmutableMap.of(
        "hot", 1
    ));

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

    CoordinatorStats stats = runRuleAndGetStats(
        rule,
        segment,
        makeCoordinatorRuntimeParamsWithLoadReplicationOnTimeout(druidCluster, segment)
    );

    // Ensure that the segment is assigned to one of the historicals
    Assert.assertEquals(1L, stats.getTieredStat(LoadRule.ASSIGNED_COUNT, "hot"));

    // Ensure that the primary segment is assigned again in case the peon timed out on loading the segment
    final LoadQueuePeon slowLoadingPeon = createLoadingPeon(segment, true);
    EasyMock.replay(slowLoadingPeon);

    DruidCluster withLoadTimeout = DruidClusterBuilder
        .newBuilder()
        .addTier("hot", new ServerHolder(server1, slowLoadingPeon), new ServerHolder(server2, emptyPeon))
        .build();

    CoordinatorStats statsAfterLoadPrimary = runRuleAndGetStats(
        rule,
        segment,
        makeCoordinatorRuntimeParamsWithLoadReplicationOnTimeout(withLoadTimeout, segment)
    );

    Assert.assertEquals(1L, statsAfterLoadPrimary.getTieredStat(LoadRule.ASSIGNED_COUNT, "hot"));

    EasyMock.verify(emptyPeon, mockBalancerStrategy);
  }

  @Test
  public void testSkipReplicationForTimedOutSegments()
  {
    final LoadQueuePeon emptyPeon = createEmptyPeon();
    emptyPeon.loadSegment(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();

    LoadRule rule = createLoadRule(ImmutableMap.of(
        "hot", 1
    ));

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

    CoordinatorStats stats = runRuleAndGetStats(rule, segment, druidCluster);

    // Ensure that the segment is assigned to one of the historicals
    Assert.assertEquals(1L, stats.getTieredStat(LoadRule.ASSIGNED_COUNT, "hot"));

    // Add the segment to the timed out list to simulate peon timeout on loading the segment
    final LoadQueuePeon slowLoadingPeon = createLoadingPeon(segment, true);
    EasyMock.replay(slowLoadingPeon);

    DruidCluster withLoadTimeout = DruidClusterBuilder
        .newBuilder()
        .addTier("hot", new ServerHolder(server1, slowLoadingPeon), new ServerHolder(server2, emptyPeon))
        .build();

    // Default behavior is to not replicate the timed out segments on other servers
    CoordinatorStats statsAfterLoadPrimary = runRuleAndGetStats(rule, segment, withLoadTimeout);

    Assert.assertTrue(statsAfterLoadPrimary.getTiers(LoadRule.ASSIGNED_COUNT).isEmpty());

    EasyMock.verify(emptyPeon, mockBalancerStrategy);
  }

  @Test
  public void testLoadUsedSegmentsForAllSegmentGranularityAndCachingCostBalancerStrategy()
  {
    LoadRule rule = createLoadRule(ImmutableMap.of("tier1", 1));

    DataSegment segment0 = createDataSegmentWithIntervalAndPartition(createDataSegment("foo"),
                                                         JodaUtils.MIN_INSTANT,
                                                         JodaUtils.MAX_INSTANT,
                                                         0);
    DataSegment segment1 = createDataSegmentWithIntervalAndPartition(createDataSegment("foo"),
                                                         JodaUtils.MIN_INSTANT,
                                                         JodaUtils.MAX_INSTANT,
                                                         1);

    final LoadQueuePeon loadingPeon = createLoadingPeon(segment0, true);

    loadingPeon.loadSegment(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().once();

    EasyMock.expect(mockBalancerStrategy.findNewSegmentHomeReplicator(EasyMock.anyObject(), EasyMock.anyObject()))
            .andDelegateTo(cachingCostBalancerStrategy)
            .anyTimes();

    EasyMock.replay(loadingPeon, mockBalancerStrategy);

    ImmutableDruidServer server =
        new DruidServer("serverHot", "hostHot", null, 1000, ServerType.HISTORICAL, "tier1", 1).toImmutableDruidServer();

    DruidCluster druidCluster = DruidClusterBuilder
        .newBuilder()
        .addTier("tier1", new ServerHolder(server, loadingPeon))
        .build();

    final CoordinatorStats stats = runRuleAndGetStats(
        rule,
        segment1,
        makeCoordinatorRuntimeParamsWithLoadReplicationOnTimeout(druidCluster, segment0, segment1)
    );

    Assert.assertEquals(1L, stats.getTieredStat(LoadRule.ASSIGNED_COUNT, "tier1"));

    EasyMock.verify(loadingPeon, mockBalancerStrategy);
  }

  @Test
  public void testLoadPriority()
  {
    final LoadQueuePeon mockPeon1 = createEmptyPeon();
    final LoadQueuePeon mockPeon2 = createEmptyPeon();

    mockPeon2.loadSegment(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.isNull());
    EasyMock.expectLastCall().once();

    EasyMock.expect(mockBalancerStrategy.findNewSegmentHomeReplicator(EasyMock.anyObject(), EasyMock.anyObject()))
            .andDelegateTo(balancerStrategy)
            .times(2);

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

    final CoordinatorStats stats = runRuleAndGetStats(rule, segment, druidCluster);

    Assert.assertEquals(0L, stats.getTieredStat(LoadRule.ASSIGNED_COUNT, "tier1"));
    Assert.assertEquals(1L, stats.getTieredStat(LoadRule.ASSIGNED_COUNT, "tier2"));

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

    CoordinatorStats stats = runRuleAndGetStats(rule, segment, druidCluster);

    Assert.assertEquals(1L, stats.getTieredStat("droppedCount", "hot"));
    Assert.assertEquals(1L, stats.getTieredStat("droppedCount", DruidServer.DEFAULT_TIER));

    EasyMock.verify(mockPeon);
  }

  @Test
  public void testLoadWithNonExistentTier()
  {
    final LoadQueuePeon mockPeon = createEmptyPeon();
    mockPeon.loadSegment(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();

    EasyMock.expect(mockBalancerStrategy.findNewSegmentHomeReplicator(EasyMock.anyObject(), EasyMock.anyObject()))
            .andDelegateTo(balancerStrategy)
            .times(1);

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
    CoordinatorStats stats = runRuleAndGetStats(rule, segment, druidCluster);

    Assert.assertEquals(1L, stats.getTieredStat(LoadRule.ASSIGNED_COUNT, "hot"));

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

    CoordinatorStats stats = runRuleAndGetStats(rule, segment, druidCluster);

    Assert.assertEquals(1L, stats.getTieredStat("droppedCount", "hot"));

    EasyMock.verify(mockPeon, mockBalancerStrategy);
  }

  @Test
  public void testMaxLoadingQueueSize()
  {
    EasyMock.expect(mockBalancerStrategy.findNewSegmentHomeReplicator(EasyMock.anyObject(), EasyMock.anyObject()))
            .andDelegateTo(balancerStrategy)
            .times(2);

    EasyMock.replay(mockBalancerStrategy);

    final LoadQueuePeonTester peon = new LoadQueuePeonTester();

    LoadRule rule = createLoadRule(ImmutableMap.of("hot", 1));

    DruidCluster druidCluster = DruidClusterBuilder
        .newBuilder()
        .addTier(
            "hot",
            new ServerHolder(
                new DruidServer("serverHot", "hostHot", null, 1000, ServerType.HISTORICAL, "hot", 0)
                    .toImmutableDruidServer(),
                peon
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
        .withDynamicConfigs(CoordinatorDynamicConfig.builder().withMaxSegmentsInNodeLoadingQueue(2).build())
        .build();

    CoordinatorStats stats1 = runRuleAndGetStats(rule, dataSegment1, params);
    CoordinatorStats stats2 = runRuleAndGetStats(rule, dataSegment2, params);
    CoordinatorStats stats3 = runRuleAndGetStats(rule, dataSegment3, params);

    Assert.assertEquals(1L, stats1.getTieredStat(LoadRule.ASSIGNED_COUNT, "hot"));
    Assert.assertEquals(1L, stats2.getTieredStat(LoadRule.ASSIGNED_COUNT, "hot"));
    Assert.assertEquals(0L, stats3.getTieredStat(LoadRule.ASSIGNED_COUNT, "hot"));

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

    EasyMock.expect(mockBalancerStrategy.findNewSegmentHomeReplicator(EasyMock.anyObject(), EasyMock.anyObject()))
            .andDelegateTo(balancerStrategy)
            .times(1);

    EasyMock.replay(mockPeon1, mockPeon2, mockBalancerStrategy);

    DruidCluster druidCluster = DruidClusterBuilder
        .newBuilder()
        .addTier("tier1", createServerHolder("tier1", mockPeon1, true))
        .addTier("tier2", createServerHolder("tier2", mockPeon2, false))
        .build();

    CoordinatorStats stats = runRuleAndGetStats(rule, segment, druidCluster);

    Assert.assertEquals(1L, stats.getTieredStat(LoadRule.ASSIGNED_COUNT, "tier2"));
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

    LoadRule rule = createLoadRule(ImmutableMap.of("tier1", 2, "tier2", 2));

    final DataSegment segment = createDataSegment("foo");

    EasyMock.replay(mockPeon1, mockPeon2, mockPeon3, mockPeon4);

    ServerHolder holder1 = createServerHolder("tier1", mockPeon1, true);
    ServerHolder holder2 = createServerHolder("tier1", mockPeon2, false);
    ServerHolder holder3 = createServerHolder("tier2", mockPeon3, false);
    ServerHolder holder4 = createServerHolder("tier2", mockPeon4, false);

    EasyMock.expect(mockBalancerStrategy.findNewSegmentHomeReplicator(segment, ImmutableList.of(holder2)))
            .andReturn(Collections.singletonList(holder2).iterator());
    EasyMock.expect(mockBalancerStrategy.findNewSegmentHomeReplicator(segment, ImmutableList.of(holder4, holder3)))
            .andReturn(Arrays.asList(holder3, holder4).iterator());
    EasyMock.replay(mockBalancerStrategy);

    DruidCluster druidCluster = DruidClusterBuilder
        .newBuilder()
        .addTier("tier1", holder1, holder2)
        .addTier("tier2", holder3, holder4)
        .build();
    CoordinatorStats stats = runRuleAndGetStats(rule, segment, druidCluster);

    Assert.assertEquals(1L, stats.getTieredStat(LoadRule.ASSIGNED_COUNT, "tier1"));
    Assert.assertEquals(2L, stats.getTieredStat(LoadRule.ASSIGNED_COUNT, "tier2"));

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
    CoordinatorStats stats = runRuleAndGetStats(rule, segment1, params);
    Assert.assertEquals(1L, stats.getTieredStat("droppedCount", "tier1"));
    stats = runRuleAndGetStats(rule, segment2, params);
    Assert.assertEquals(1L, stats.getTieredStat("droppedCount", "tier1"));

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

    CoordinatorStats stats = runRuleAndGetStats(rule, segment1, makeCoordinatorRuntimeParams(druidCluster, segment1));
    Assert.assertEquals(1L, stats.getTieredStat("droppedCount", "tier1"));
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
        new HashMap<>(),
        new ArrayList<>(),
        new ArrayList<>(),
        NoneShardSpec.instance(),
        0,
        0
    );
  }

  private DataSegment createDataSegmentWithIntervalAndPartition(DataSegment dataSegment, long startMillis, long endMillis, int partitionNum)
  {
    return new DataSegment.Builder(dataSegment)
        .interval(new Interval(startMillis, endMillis, dataSegment.getInterval().getChronology()))
        .shardSpec(new LinearShardSpec(partitionNum))
        .build();
  }

  private static LoadRule createLoadRule(final Map<String, Integer> tieredReplicants)
  {
    return new LoadRule()
    {
      @Override
      public Map<String, Integer> getTieredReplicants()
      {
        return tieredReplicants;
      }

      @Override
      public int getNumReplicants(String tier)
      {
        return tieredReplicants.get(tier);
      }

      @Override
      public String getType()
      {
        return "test";
      }

      @Override
      public boolean appliesTo(DataSegment segment, DateTime referenceTimestamp)
      {
        return true;
      }

      @Override
      public boolean appliesTo(Interval interval, DateTime referenceTimestamp)
      {
        return true;
      }
    };
  }

  private static LoadQueuePeon createEmptyPeon()
  {
    final LoadQueuePeon mockPeon = EasyMock.createMock(LoadQueuePeon.class);
    EasyMock.expect(mockPeon.getSegmentsToLoad()).andReturn(Collections.emptySet()).anyTimes();
    EasyMock.expect(mockPeon.getSegmentsInQueue()).andReturn(Collections.emptyMap()).anyTimes();
    EasyMock.expect(mockPeon.getSegmentsMarkedToDrop()).andReturn(Collections.emptySet()).anyTimes();
    EasyMock.expect(mockPeon.getLoadQueueSize()).andReturn(0L).anyTimes();
    EasyMock.expect(mockPeon.getNumberOfSegmentsInQueue()).andReturn(0).anyTimes();

    return mockPeon;
  }

  private static LoadQueuePeon createLoadingPeon(DataSegment segment, boolean slowLoading)
  {
    final Set<DataSegment> segs = Collections.singleton(segment);

    final LoadQueuePeon mockPeon = EasyMock.createMock(LoadQueuePeon.class);
    EasyMock.expect(mockPeon.getSegmentsToLoad()).andReturn(segs).anyTimes();
    EasyMock.expect(mockPeon.getSegmentsMarkedToDrop()).andReturn(Collections.emptySet()).anyTimes();
    EasyMock.expect(mockPeon.getSegmentsInQueue())
            .andReturn(ImmutableMap.of(segment, SegmentAction.LOAD_AS_PRIMARY)).anyTimes();
    EasyMock.expect(mockPeon.getNumberOfSegmentsInQueue()).andReturn(segs.size()).anyTimes();

    if (slowLoading) {
      EasyMock.expect(mockPeon.getTimedOutSegments()).andReturn(segs).anyTimes();
    } else {
      EasyMock.expect(mockPeon.getTimedOutSegments()).andReturn(Collections.emptySet()).anyTimes();
    }

    return mockPeon;
  }

  private static final AtomicInteger SERVER_ID = new AtomicInteger();

  private static DruidServer createServer(String tier)
  {
    int serverId = LoadRuleTest.SERVER_ID.incrementAndGet();
    return new DruidServer(
        "server" + serverId,
        "127.0.0.1:800" + serverId,
        null,
        1000,
        ServerType.HISTORICAL,
        tier,
        0
    );
  }

  private static LoadQueuePeon createOneCallPeonMock()
  {
    final LoadQueuePeon mockPeon2 = createEmptyPeon();
    mockPeon2.loadSegment(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().once();
    return mockPeon2;
  }

  private static ServerHolder createServerHolder(String tier, LoadQueuePeon mockPeon1, boolean isDecommissioning)
  {
    return new ServerHolder(
        createServer(tier).toImmutableDruidServer(),
        mockPeon1,
        isDecommissioning
    );
  }
}
