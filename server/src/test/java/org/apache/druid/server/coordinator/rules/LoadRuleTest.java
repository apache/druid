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
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.client.DruidServer;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.core.LoggingEmitter;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.BalancerStrategy;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.server.coordinator.CoordinatorRuntimeParamsTestHelpers;
import org.apache.druid.server.coordinator.CoordinatorStats;
import org.apache.druid.server.coordinator.CostBalancerStrategyFactory;
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.server.coordinator.DruidClusterBuilder;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.LoadQueuePeon;
import org.apache.druid.server.coordinator.LoadQueuePeonTester;
import org.apache.druid.server.coordinator.ReplicationThrottler;
import org.apache.druid.server.coordinator.SegmentReplicantLookup;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

@SuppressWarnings("deprecation")
@RunWith(Parameterized.class)
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

  private ReplicationThrottler throttler;

  private ListeningExecutorService exec;
  private BalancerStrategy balancerStrategy;

  private BalancerStrategy mockBalancerStrategy;

  private final LoadRule.LoadRuleMode loadRuleMode;

  // Each test needs to run successfully using all LoadRule.LoadRuleMode values
  @Parameterized.Parameters(name = "{index}: loadRuleMode:{0}")
  public static Iterable<Object[]> data()
  {
    return Arrays.asList(
        new Object[][]{
            {LoadRule.LoadRuleMode.ALL},
            {LoadRule.LoadRuleMode.PRIMARY_ONLY},
            {LoadRule.LoadRuleMode.REPLICA_ONLY}
        }
    );
  }

  public LoadRuleTest(LoadRule.LoadRuleMode loadRuleMode)
  {
    this.loadRuleMode = loadRuleMode;
  }

  @Before
  public void setUp()
  {
    EmittingLogger.registerEmitter(EMITTER);
    EMITTER.start();
    throttler = EasyMock.createMock(ReplicationThrottler.class);

    exec = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1));
    balancerStrategy = new CostBalancerStrategyFactory().createBalancerStrategy(exec);

    mockBalancerStrategy = EasyMock.createMock(BalancerStrategy.class);
  }

  @After
  public void tearDown() throws Exception
  {
    exec.shutdown();
    EMITTER.close();
  }

  /**
   * Tests functionality of basic LoadRule.
   *
   * Rule 1: 1 replicant on hot tier, 2 replicants on DruidServer.DEFAULT_TIER
   *
   * Servers: 1 server on hot tier, 1 server on DruidServer.DEFAULT_TIER
   *
   * Segments: 1 segment in datasource foo that is not loaded to either server at start of test.
   *
   * Expected Results For Each LoadRuleMode:
   *
   * ALL - Load primary and non-primary replicant. 1 on each tier.
   * PRIMARY_ONLY - Load primary replicant.
   * REPLICA_ONLY - Load 0 replicants.
   */
  @Test
  public void testLoad()
  {
    final DataSegment segment = createDataSegment("foo");

    EasyMock.expect(throttler.canCreateReplicant(EasyMock.anyString())).andReturn(true).anyTimes();
    final LoadQueuePeon mockPeon = createEmptyPeon();

    if (loadRuleMode.equals(LoadRule.LoadRuleMode.ALL)) {
      mockPeon.loadSegment(EasyMock.anyObject(), EasyMock.anyObject());
      EasyMock.expectLastCall().atLeastOnce();
      throttler.registerReplicantCreation(DruidServer.DEFAULT_TIER, segment.getId(), "hostNorm");
      EasyMock.expectLastCall().once();
      EasyMock.expect(mockBalancerStrategy.findNewSegmentHomeReplicator(EasyMock.anyObject(), EasyMock.anyObject()))
              .andDelegateTo(balancerStrategy)
              .times(3);
    } else if (loadRuleMode.equals(LoadRule.LoadRuleMode.PRIMARY_ONLY)) {
      mockPeon.loadSegment(EasyMock.anyObject(), EasyMock.anyObject());
      EasyMock.expectLastCall().atLeastOnce();
      EasyMock.expect(mockBalancerStrategy.findNewSegmentHomeReplicator(EasyMock.anyObject(), EasyMock.anyObject()))
              .andDelegateTo(balancerStrategy)
              .times(2);
    }

    EasyMock.replay(throttler, mockPeon, mockBalancerStrategy);

    LoadRule rule = createLoadRule(ImmutableMap.of(
        "hot", 1,
        DruidServer.DEFAULT_TIER, 2
    ));

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

    CoordinatorStats stats = rule.run(null, makeCoordinatorRuntimeParams(druidCluster, segment), segment);

    if (loadRuleMode.equals(LoadRule.LoadRuleMode.ALL)) {
      Assert.assertEquals(1L, stats.getTieredStat(LoadRule.ASSIGNED_COUNT, "hot"));
      Assert.assertEquals(1L, stats.getTieredStat(LoadRule.ASSIGNED_COUNT, DruidServer.DEFAULT_TIER));
    } else if (loadRuleMode.equals(LoadRule.LoadRuleMode.PRIMARY_ONLY)) {
      Assert.assertEquals(1L, stats.getTieredStat(LoadRule.ASSIGNED_COUNT, "hot"));
      Assert.assertEquals(0L, stats.getTieredStat(LoadRule.ASSIGNED_COUNT, DruidServer.DEFAULT_TIER));
    } else {
      Assert.assertEquals(0L, stats.getTieredStat(LoadRule.ASSIGNED_COUNT, "hot"));
      Assert.assertEquals(0L, stats.getTieredStat(LoadRule.ASSIGNED_COUNT, DruidServer.DEFAULT_TIER));
    }

    EasyMock.verify(throttler, mockPeon, mockBalancerStrategy);
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
        .withReplicationManager(throttler)
        .withBalancerStrategy(mockBalancerStrategy)
        .withUsedSegmentsInTest(usedSegments)
        .withLoadRuleMode(loadRuleMode)
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
        .withReplicationManager(throttler)
        .withBalancerStrategy(mockBalancerStrategy)
        .withUsedSegmentsInTest(usedSegments)
        .build();
  }

  /**
   * Tests that a segment is not loaded with more replicants that specified by the load rule.
   *
   * Rule 1: 1 replicant on hot tier
   *
   * Servers: 2 servers on hot tier
   *
   * Segments: 1 segment in datasource foo that is not loaded to either server at start of test.
   *
   * Expected Results For Each LoadRuleMode:
   *
   * ALL - Load primary replicant on first call to run() and no replicant on 2nd call.
   * PRIMARY_ONLY - Load primary replicant on first call to run() and no replicant on 2nd call.
   * REPLICA_ONLY - Load 0 replicants for both calls to run()
   */
  @Test
  public void testLoadPrimaryAssignDoesNotOverAssign()
  {
    final DataSegment segment = createDataSegment("foo");

    EasyMock.expect(throttler.canCreateReplicant(EasyMock.anyString())).andReturn(true).anyTimes();

    final LoadQueuePeon mockPeon = createEmptyPeon();
    if (!loadRuleMode.equals(LoadRule.LoadRuleMode.REPLICA_ONLY)) {
      mockPeon.loadSegment(EasyMock.anyObject(), EasyMock.anyObject());
      EasyMock.expectLastCall().atLeastOnce();

      EasyMock.expect(mockBalancerStrategy.findNewSegmentHomeReplicator(EasyMock.anyObject(), EasyMock.anyObject()))
              .andDelegateTo(balancerStrategy)
              .anyTimes();
    }

    LoadRule rule = createLoadRule(ImmutableMap.of(
        "hot", 1
    ));

    EasyMock.replay(throttler, mockPeon, mockBalancerStrategy);

    ImmutableDruidServer server1 =
        new DruidServer("serverHot", "hostHot", null, 1000, ServerType.HISTORICAL, "hot", 1).toImmutableDruidServer();
    ImmutableDruidServer server2 =
        new DruidServer("serverHot2", "hostHot2", null, 1000, ServerType.HISTORICAL, "hot", 1).toImmutableDruidServer();
    DruidCluster druidCluster = DruidClusterBuilder
        .newBuilder()
        .addTier("hot", new ServerHolder(server1, mockPeon), new ServerHolder(server2, mockPeon))
        .build();

    CoordinatorStats stats = rule.run(
        null,
        makeCoordinatorRuntimeParams(druidCluster, segment),
        segment
    );


    if (loadRuleMode.equals(LoadRule.LoadRuleMode.ALL) || loadRuleMode.equals(LoadRule.LoadRuleMode.PRIMARY_ONLY)) {
      Assert.assertEquals(1L, stats.getTieredStat(LoadRule.ASSIGNED_COUNT, "hot"));
    } else {
      Assert.assertEquals(0L, stats.getTieredStat(LoadRule.ASSIGNED_COUNT, "hot"));
    }

    // ensure multiple runs don't assign primary segment again if at replication count
    final LoadQueuePeon loadingPeon = createLoadingPeon(ImmutableList.of(segment), false);
    EasyMock.replay(loadingPeon);

    DruidCluster afterLoad = DruidClusterBuilder
        .newBuilder()
        .addTier("hot", new ServerHolder(server1, loadingPeon), new ServerHolder(server2, mockPeon))
        .build();

    CoordinatorStats statsAfterLoadPrimary = rule.run(
        null,
        makeCoordinatorRuntimeParams(afterLoad, segment),
        segment
    );

    Assert.assertEquals(0, statsAfterLoadPrimary.getTieredStat(LoadRule.ASSIGNED_COUNT, "hot"));

    EasyMock.verify(throttler, mockPeon, mockBalancerStrategy);
  }

  @Test
  public void testOverAssignForTimedOutSegments()
  {
    EasyMock.expect(throttler.canCreateReplicant(EasyMock.anyString())).andReturn(true).anyTimes();

    final LoadQueuePeon emptyPeon = createEmptyPeon();
    emptyPeon.loadSegment(EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();

    LoadRule rule = createLoadRule(ImmutableMap.of(
        "hot", 1
    ));

    final DataSegment segment = createDataSegment("foo");

    EasyMock.expect(mockBalancerStrategy.findNewSegmentHomeReplicator(EasyMock.anyObject(), EasyMock.anyObject()))
            .andDelegateTo(balancerStrategy)
            .anyTimes();

    EasyMock.replay(throttler, emptyPeon, mockBalancerStrategy);

    ImmutableDruidServer server1 =
        new DruidServer("serverHot", "hostHot", null, 1000, ServerType.HISTORICAL, "hot", 1).toImmutableDruidServer();
    ImmutableDruidServer server2 =
        new DruidServer("serverHot2", "hostHot2", null, 1000, ServerType.HISTORICAL, "hot", 1).toImmutableDruidServer();
    DruidCluster druidCluster = DruidClusterBuilder
        .newBuilder()
        .addTier("hot", new ServerHolder(server1, emptyPeon), new ServerHolder(server2, emptyPeon))
        .build();

    CoordinatorStats stats = rule.run(
        null,
        makeCoordinatorRuntimeParamsWithLoadReplicationOnTimeout(druidCluster, segment),
        segment
    );

    // Ensure that the segment is assigned to one of the historicals
    Assert.assertEquals(1L, stats.getTieredStat(LoadRule.ASSIGNED_COUNT, "hot"));

    // Ensure that the primary segment is assigned again in case the peon timed out on loading the segment
    final LoadQueuePeon slowLoadingPeon = createLoadingPeon(ImmutableList.of(segment), true);
    EasyMock.replay(slowLoadingPeon);

    DruidCluster withLoadTimeout = DruidClusterBuilder
        .newBuilder()
        .addTier("hot", new ServerHolder(server1, slowLoadingPeon), new ServerHolder(server2, emptyPeon))
        .build();

    CoordinatorStats statsAfterLoadPrimary = rule.run(
        null,
        makeCoordinatorRuntimeParamsWithLoadReplicationOnTimeout(withLoadTimeout, segment),
        segment
    );

    Assert.assertEquals(1L, statsAfterLoadPrimary.getTieredStat(LoadRule.ASSIGNED_COUNT, "hot"));

    EasyMock.verify(throttler, emptyPeon, mockBalancerStrategy);
  }

  @Test
  public void testSkipReplicationForTimedOutSegments()
  {
    EasyMock.expect(throttler.canCreateReplicant(EasyMock.anyString())).andReturn(true).anyTimes();

    final LoadQueuePeon emptyPeon = createEmptyPeon();
    if (!loadRuleMode.equals(LoadRule.LoadRuleMode.REPLICA_ONLY)) {
      emptyPeon.loadSegment(EasyMock.anyObject(), EasyMock.anyObject());
      EasyMock.expectLastCall().atLeastOnce();
    }

    LoadRule rule = createLoadRule(ImmutableMap.of(
        "hot", 1
    ));

    final DataSegment segment = createDataSegment("foo");

    EasyMock.expect(mockBalancerStrategy.findNewSegmentHomeReplicator(EasyMock.anyObject(), EasyMock.anyObject()))
            .andDelegateTo(balancerStrategy)
            .anyTimes();

    EasyMock.replay(throttler, emptyPeon, mockBalancerStrategy);

    ImmutableDruidServer server1 =
        new DruidServer("serverHot", "hostHot", null, 1000, ServerType.HISTORICAL, "hot", 1).toImmutableDruidServer();
    ImmutableDruidServer server2 =
        new DruidServer("serverHot2", "hostHot2", null, 1000, ServerType.HISTORICAL, "hot", 1).toImmutableDruidServer();
    DruidCluster druidCluster = DruidClusterBuilder
        .newBuilder()
        .addTier("hot", new ServerHolder(server1, emptyPeon), new ServerHolder(server2, emptyPeon))
        .build();

    CoordinatorStats stats = rule.run(
        null,
        makeCoordinatorRuntimeParams(druidCluster, segment),
        segment
    );

    // Ensure that the segment is assigned to one of the historicals
    if (!loadRuleMode.equals(LoadRule.LoadRuleMode.REPLICA_ONLY)) {
      Assert.assertEquals(1L, stats.getTieredStat(LoadRule.ASSIGNED_COUNT, "hot"));
    }

    // Add the segment to the timed out list to simulate peon timeout on loading the segment
    final LoadQueuePeon slowLoadingPeon = createLoadingPeon(ImmutableList.of(segment), true);
    EasyMock.replay(slowLoadingPeon);

    DruidCluster withLoadTimeout = DruidClusterBuilder
        .newBuilder()
        .addTier("hot", new ServerHolder(server1, slowLoadingPeon), new ServerHolder(server2, emptyPeon))
        .build();

    // Default behavior is to not replicate the timed out segments on other servers
    CoordinatorStats statsAfterLoadPrimary = rule.run(
        null,
        makeCoordinatorRuntimeParams(withLoadTimeout, segment),
        segment
    );

    Assert.assertEquals(0L, statsAfterLoadPrimary.getTieredStat(LoadRule.ASSIGNED_COUNT, "hot"));

    EasyMock.verify(throttler, emptyPeon, mockBalancerStrategy);
  }

  /**
   * Tests that a segment is loaded to higher priority tier first.
   *
   * Rule 1: 10 replicants on tier1, 10 replicants on teir2
   *
   * Tiers: tier1 (priority 0), tier2 (priority 1)
   *
   * Servers: 1 server on tier1, 2 servers on tier2
   *
   * Segments: 1 segment in datasource foo that is not loaded to any server at start of test.
   *
   * Important Note: Throttler Mock does not allow any non-primary replicant segment loads.
   *
   * Expected Results For Each LoadRuleMode:
   *
   * ALL - Load primary replicant on higher priority tier
   * PRIMARY_ONLY - Load primary replicant on higher priority tier
   * REPLICA_ONLY - Load 0 replicants
   */
  @Test
  public void testLoadPriority()
  {
    EasyMock.expect(throttler.canCreateReplicant(EasyMock.anyString())).andReturn(false).anyTimes();

    final LoadQueuePeon mockPeon1 = createEmptyPeon();
    final LoadQueuePeon mockPeon2 = createEmptyPeon();

    if (!loadRuleMode.equals(LoadRule.LoadRuleMode.REPLICA_ONLY)) {
      mockPeon2.loadSegment(EasyMock.anyObject(), EasyMock.isNull());
      EasyMock.expectLastCall().once();

      EasyMock.expect(mockBalancerStrategy.findNewSegmentHomeReplicator(EasyMock.anyObject(), EasyMock.anyObject()))
              .andDelegateTo(balancerStrategy)
              .times(2);
    }

    EasyMock.replay(throttler, mockPeon1, mockPeon2, mockBalancerStrategy);

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

    final CoordinatorStats stats = rule.run(null, makeCoordinatorRuntimeParams(druidCluster, segment), segment);

    if (loadRuleMode.equals(LoadRule.LoadRuleMode.ALL) || loadRuleMode.equals(LoadRule.LoadRuleMode.PRIMARY_ONLY)) {
      Assert.assertEquals(0L, stats.getTieredStat(LoadRule.ASSIGNED_COUNT, "tier1"));
      Assert.assertEquals(1L, stats.getTieredStat(LoadRule.ASSIGNED_COUNT, "tier2"));
    } else {
      Assert.assertEquals(0L, stats.getTieredStat(LoadRule.ASSIGNED_COUNT, "tier1"));
      Assert.assertEquals(0L, stats.getTieredStat(LoadRule.ASSIGNED_COUNT, "tier2"));
    }

    EasyMock.verify(throttler, mockPeon1, mockPeon2, mockBalancerStrategy);
  }

  /**
   * Tests that a segment is properly dropped.
   *
   * Rule 1: 0 replicants on hot tier, 0 replicants on DruidServer.DEFAULT_TIER
   *
   * Tiers: hot (priority 0), DruidServer.DEFAULT_TIER (priority 0)
   *
   * Servers: 1 server on hot, 2 servers on DruidServer.DEFAULT_TIER
   *
   * Segments: 1 segment in datasource foo that is loaded onto the hot tier server and one of the
   * DruidServer.DEFAULT_TIER servers at the start of the test.
   *
   * Expected Results For Each LoadRuleMode:
   *
   * ALL - 1 replicant is dropped from each of hot and DruidServer.DEFAULT_TIER tiers
   * PRIMARY_ONLY - No replicants are dropped.
   * REPLICA_ONLY - 1 replicant is dropped from each of hot and DruidServer.DEFAULT_TIER tiers
   */
  @Test
  public void testDrop()
  {
    final LoadQueuePeon mockPeon = createEmptyPeon();

    if (!loadRuleMode.equals(LoadRule.LoadRuleMode.PRIMARY_ONLY)) {
      mockPeon.dropSegment(EasyMock.anyObject(), EasyMock.anyObject());
      EasyMock.expectLastCall().atLeastOnce();
      EasyMock.expect(mockBalancerStrategy.pickServersToDrop(EasyMock.anyObject(), EasyMock.anyObject()))
              .andDelegateTo(balancerStrategy)
              .times(4);
    }
    EasyMock.replay(throttler, mockPeon, mockBalancerStrategy);

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

    CoordinatorStats stats = rule.run(null, makeCoordinatorRuntimeParams(druidCluster, segment), segment);

    if (loadRuleMode.equals(LoadRule.LoadRuleMode.ALL) || loadRuleMode.equals(LoadRule.LoadRuleMode.REPLICA_ONLY)) {
      Assert.assertEquals(1L, stats.getTieredStat("droppedCount", "hot"));
      Assert.assertEquals(1L, stats.getTieredStat("droppedCount", DruidServer.DEFAULT_TIER));
    } else {
      Assert.assertEquals(0L, stats.getTieredStat("droppedCount", "hot"));
      Assert.assertEquals(0L, stats.getTieredStat("droppedCount", DruidServer.DEFAULT_TIER));
    }

    EasyMock.verify(throttler, mockPeon);
  }

  /**
   * Tests segment load when a tier within load rule doesn't exist with servers to load to.
   *
   * Rule 1: 1 replicants on hot tier, 1 replicants on "nonExistentTier"
   *
   * Tiers: hot (priority 0)
   *
   * Servers: 1 server on hot
   *
   * Segments: 1 segment in datasource foo that is loaded onto the hot tier
   *
   * Expected Results For Each LoadRuleMode:
   *
   * ALL - 1 replicant is loaded on hot tier
   * PRIMARY_ONLY - 1 replicant is loaded on hot tier
   * REPLICA_ONLY - 0 replicants are loaded
   */
  @Test
  public void testLoadWithNonExistentTier()
  {
    final LoadQueuePeon mockPeon = createEmptyPeon();

    if (!loadRuleMode.equals(LoadRule.LoadRuleMode.REPLICA_ONLY)) {
      mockPeon.loadSegment(EasyMock.anyObject(), EasyMock.anyObject());
      EasyMock.expectLastCall().atLeastOnce();

      EasyMock.expect(mockBalancerStrategy.findNewSegmentHomeReplicator(EasyMock.anyObject(), EasyMock.anyObject()))
              .andDelegateTo(balancerStrategy)
              .times(1);
    }

    EasyMock.replay(throttler, mockPeon, mockBalancerStrategy);

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

    CoordinatorStats stats = rule.run(
        null,
        CoordinatorRuntimeParamsTestHelpers
            .newBuilder()
            .withDruidCluster(druidCluster)
            .withSegmentReplicantLookup(SegmentReplicantLookup.make(new DruidCluster(), false))
            .withReplicationManager(throttler)
            .withBalancerStrategy(mockBalancerStrategy)
            .withUsedSegmentsInTest(segment)
            .withLoadRuleMode(loadRuleMode)
            .build(),
        segment
    );

    if (loadRuleMode.equals(LoadRule.LoadRuleMode.ALL) || loadRuleMode.equals(LoadRule.LoadRuleMode.PRIMARY_ONLY)) {
      Assert.assertEquals(1L, stats.getTieredStat(LoadRule.ASSIGNED_COUNT, "hot"));
    } else {
      Assert.assertEquals(0L, stats.getTieredStat(LoadRule.ASSIGNED_COUNT, "hot"));
    }

    EasyMock.verify(throttler, mockPeon, mockBalancerStrategy);
  }

  /**
   * Tests that a replicant is properly dropped from a tier despite there being no replicant available on a separate
   * tier in the load rule.
   *
   * Rule 1: 1 replicants on hot tier, 1 replicants on "nonExistentTier"
   *
   * Tiers: hot (priority 0)
   *
   * Servers: 2 servers on hot
   *
   * Segments: 1 segment in datasource foo that is loaded onto both hot tier servers.
   *
   * Expected Results For Each LoadRuleMode:
   *
   * ALL - 1 replicant is dropped from hot tier.
   * PRIMARY_ONLY - 0 replicants are dropped.
   * REPLICA_ONLY - 1 replicant is dropped from hot tier.
   */
  @Test
  public void testDropWithNonExistentTier()
  {
    final LoadQueuePeon mockPeon = createEmptyPeon();

    // properly mock basedon the LoadRuleMode
    if (!loadRuleMode.equals(LoadRule.LoadRuleMode.PRIMARY_ONLY)) {
      mockPeon.dropSegment(EasyMock.anyObject(), EasyMock.anyObject());
      EasyMock.expectLastCall().once();
      EasyMock.expect(mockBalancerStrategy.pickServersToDrop(EasyMock.anyObject(), EasyMock.anyObject()))
              .andDelegateTo(balancerStrategy)
              .times(2);
    }
    EasyMock.replay(throttler, mockPeon, mockBalancerStrategy);

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

    CoordinatorStats stats = rule.run(null, makeCoordinatorRuntimeParams(druidCluster, segment), segment);

    if (loadRuleMode.equals(LoadRule.LoadRuleMode.ALL) || loadRuleMode.equals(LoadRule.LoadRuleMode.REPLICA_ONLY)) {
      Assert.assertEquals(1L, stats.getTieredStat("droppedCount", "hot"));
    } else {
      Assert.assertEquals(0L, stats.getTieredStat("droppedCount", "hot"));
    }

    EasyMock.verify(throttler, mockPeon, mockBalancerStrategy);
  }

  /**
   * Tests that a maxLoadQueueSize is respected and a segment won't be added to a full load queue.
   *
   * Rule 1: 1 replicants on hot tier
   *
   * Tiers: hot (priority 0)
   *
   * Servers: 1 server on hot tier
   *
   * Segments: 3 segments created using utility method. none loaded on segments at start  of test.
   *
   * Expected Results For Each LoadRuleMode:
   *
   * ALL - segment1 and segment2 are loaded to hot tier but segment3 is not due to a full load queue
   * PRIMARY_ONLY - segment1 and segment2 are loaded to hot tier but segment3 is not due to a full load queue
   * REPLICA_ONLY - 0 replicants are loaded because ther is no primary replicant in place for any segments at test start
   */
  @Test
  public void testMaxLoadingQueueSize()
  {
    if (!loadRuleMode.equals(LoadRule.LoadRuleMode.REPLICA_ONLY)) {
      EasyMock.expect(mockBalancerStrategy.findNewSegmentHomeReplicator(EasyMock.anyObject(), EasyMock.anyObject()))
              .andDelegateTo(balancerStrategy)
              .times(2);
    }

    EasyMock.replay(throttler, mockBalancerStrategy);

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
        .withReplicationManager(throttler)
        .withBalancerStrategy(mockBalancerStrategy)
        .withUsedSegmentsInTest(dataSegment1, dataSegment2, dataSegment3)
        .withDynamicConfigs(CoordinatorDynamicConfig.builder().withMaxSegmentsInNodeLoadingQueue(2).build())
        .withLoadRuleMode(loadRuleMode)
        .build();

    CoordinatorStats stats1 = rule.run(null, params, dataSegment1);
    CoordinatorStats stats2 = rule.run(null, params, dataSegment2);
    CoordinatorStats stats3 = rule.run(null, params, dataSegment3);

    if (loadRuleMode.equals(LoadRule.LoadRuleMode.ALL) || loadRuleMode.equals(LoadRule.LoadRuleMode.PRIMARY_ONLY)) {
      Assert.assertEquals(1L, stats1.getTieredStat(LoadRule.ASSIGNED_COUNT, "hot"));
      Assert.assertEquals(1L, stats2.getTieredStat(LoadRule.ASSIGNED_COUNT, "hot"));
    } else {
      Assert.assertEquals(0L, stats1.getTieredStat(LoadRule.ASSIGNED_COUNT, "hot"));
      Assert.assertEquals(0L, stats2.getTieredStat(LoadRule.ASSIGNED_COUNT, "hot"));
    }
    Assert.assertEquals(0L, stats3.getTieredStat(LoadRule.ASSIGNED_COUNT, "hot"));

    EasyMock.verify(throttler, mockBalancerStrategy);
  }

  /**
   * Tests that a segment is not loaded to a server that is decommissioning.
   *
   * Rule 1: 1 replicant on tier1, 1 replicant on tier2
   *
   * Tiers: tier1, tier2
   *
   * Servers: 1 server on tier1 that is decommissioning, 1 server on tier2
   *
   * Segments: 1 segment in datasource foo that is not loaded to any segments at start of test.
   *
   * Expected Results For Each LoadRuleMode:
   *
   * ALL - 1 segment loaded to tier2
   * PRIMARY_ONLY - 1 segment loaded to tier2
   * REPLICA_ONLY - 0 replicants are loaded because ther is no primary replicant in place for any segments at test start
   */
  @Test
  public void testLoadDecommissioning()
  {
    final LoadQueuePeon mockPeon1 = createEmptyPeon();
    final LoadQueuePeon mockPeon2 = createEmptyPeon();

    LoadRule rule = createLoadRule(ImmutableMap.of("tier1", 1, "tier2", 1));

    final DataSegment segment = createDataSegment("foo");

    if (!loadRuleMode.equals(LoadRule.LoadRuleMode.REPLICA_ONLY)) {
      EasyMock.expect(mockBalancerStrategy.findNewSegmentHomeReplicator(EasyMock.anyObject(), EasyMock.anyObject()))
              .andDelegateTo(balancerStrategy)
              .times(1);
      mockPeon2.loadSegment(EasyMock.anyObject(), EasyMock.anyObject());
      EasyMock.expectLastCall().once();
    }

    EasyMock.replay(mockPeon1, mockPeon2, mockBalancerStrategy);


    DruidCluster druidCluster = DruidClusterBuilder
        .newBuilder()
        .addTier("tier1", createServerHolder("tier1", mockPeon1, true))
        .addTier("tier2", createServerHolder("tier2", mockPeon2, false))
        .build();

    CoordinatorStats stats = rule.run(null, makeCoordinatorRuntimeParams(druidCluster, segment), segment);

    if (loadRuleMode.equals(LoadRule.LoadRuleMode.ALL) || loadRuleMode.equals(LoadRule.LoadRuleMode.PRIMARY_ONLY)) {
      Assert.assertEquals(1L, stats.getTieredStat(LoadRule.ASSIGNED_COUNT, "tier2"));
    } else {
      Assert.assertEquals(0L, stats.getTieredStat(LoadRule.ASSIGNED_COUNT, "tier2"));
    }
    EasyMock.verify(mockPeon1, mockPeon2, mockBalancerStrategy);
  }

  /**
   * Tests that a segment is not loaded to a server that is decommissioning.
   *
   * Rule 1: 2 replicants on tier1, 2 replicants on tier2
   *
   * Tiers: tier1, tier2
   *
   * Servers: 2 servers on tier1 (1 is decommissioning), 2 serverson tier2
   *
   * Segments: 1 segment in datasource foo that is not loaded to any segments at start of test.
   *
   * Expected Results For Each LoadRuleMode:
   *
   * ALL - 1 segment loaded onto tier1 and 2 segments loaded onto tier2
   * PRIMARY_ONLY - 1 segment loaded onto tier1
   * REPLICA_ONLY - 0 replicants are loaded because ther is no primary replicant in place for any segments at test start
   */
  @Test
  public void testLoadReplicaDuringDecommissioning()
  {
    EasyMock.expect(throttler.canCreateReplicant(EasyMock.anyString())).andReturn(true).anyTimes();

    final LoadQueuePeon mockPeon1 = createEmptyPeon();
    final LoadQueuePeon mockPeon2 = createEmptyPeon();
    final LoadQueuePeon mockPeon3 = createEmptyPeon();
    final LoadQueuePeon mockPeon4 = createEmptyPeon();

    LoadRule rule = createLoadRule(ImmutableMap.of("tier1", 2, "tier2", 2));

    final DataSegment segment = createDataSegment("foo");


    ServerHolder holder1 = createServerHolder("tier1", mockPeon1, true);
    ServerHolder holder2 = createServerHolder("tier1", mockPeon2, false);
    ServerHolder holder3 = createServerHolder("tier2", mockPeon3, false);
    ServerHolder holder4 = createServerHolder("tier2", mockPeon4, false);

    // We load 3 replicnts in ALL mode
    // We load 1 replicant in PRIMARY_ONLY mode
    // We load 0 replicants in REPLICA_ONLY mode
    if (loadRuleMode.equals(LoadRule.LoadRuleMode.ALL)) {
      mockPeon2.loadSegment(EasyMock.anyObject(), EasyMock.anyObject());
      EasyMock.expectLastCall().once();
      mockPeon3.loadSegment(EasyMock.anyObject(), EasyMock.anyObject());
      EasyMock.expectLastCall().once();
      mockPeon4.loadSegment(EasyMock.anyObject(), EasyMock.anyObject());
      EasyMock.expectLastCall().once();

      EasyMock.expect(mockBalancerStrategy.findNewSegmentHomeReplicator(segment, ImmutableList.of(holder4, holder3)))
              .andReturn(holder3);
      EasyMock.expect(mockBalancerStrategy.findNewSegmentHomeReplicator(segment, ImmutableList.of(holder4)))
              .andReturn(holder4);
      EasyMock.expect(mockBalancerStrategy.findNewSegmentHomeReplicator(segment, ImmutableList.of(holder2)))
              .andReturn(holder2);
      throttler.registerReplicantCreation(EasyMock.eq("tier2"), EasyMock.anyObject(), EasyMock.anyObject());
      EasyMock.expectLastCall().times(2);
    } else if (loadRuleMode.equals(LoadRule.LoadRuleMode.PRIMARY_ONLY)) {
      mockPeon2.loadSegment(EasyMock.anyObject(), EasyMock.anyObject());
      EasyMock.expectLastCall().once();

      EasyMock.expect(mockBalancerStrategy.findNewSegmentHomeReplicator(segment, ImmutableList.of(holder4, holder3)))
              .andReturn(holder3);
      EasyMock.expect(mockBalancerStrategy.findNewSegmentHomeReplicator(segment, ImmutableList.of(holder2)))
              .andReturn(holder2);
    }

    EasyMock.replay(throttler, mockPeon1, mockPeon2, mockPeon3, mockPeon4, mockBalancerStrategy);

    DruidCluster druidCluster = DruidClusterBuilder
        .newBuilder()
        .addTier("tier1", holder1, holder2)
        .addTier("tier2", holder3, holder4)
        .build();

    CoordinatorStats stats = rule.run(null, makeCoordinatorRuntimeParams(druidCluster, segment), segment);


    if (loadRuleMode.equals(LoadRule.LoadRuleMode.ALL)) {
      Assert.assertEquals(1L, stats.getTieredStat(LoadRule.ASSIGNED_COUNT, "tier1"));
      Assert.assertEquals(2L, stats.getTieredStat(LoadRule.ASSIGNED_COUNT, "tier2"));
    } else if (loadRuleMode.equals(LoadRule.LoadRuleMode.PRIMARY_ONLY)) {
      Assert.assertEquals(1L, stats.getTieredStat(LoadRule.ASSIGNED_COUNT, "tier1"));
      Assert.assertEquals(0L, stats.getTieredStat(LoadRule.ASSIGNED_COUNT, "tier2"));
    } else {
      Assert.assertEquals(0L, stats.getTieredStat(LoadRule.ASSIGNED_COUNT, "tier1"));
      Assert.assertEquals(0L, stats.getTieredStat(LoadRule.ASSIGNED_COUNT, "tier2"));
    }

    EasyMock.verify(throttler, mockPeon1, mockPeon2, mockPeon3, mockPeon4, mockBalancerStrategy);
  }

  /**
   * Tests that segments are properly dropped when there is a decommissioning server
   *
   * Rule 1: 0 replicants on tier1
   *
   * Tiers: tier1
   *
   * Servers: 2 servers on tier1 (1 is decommissioning)
   *
   * Segments: 1 segment in datasource foo1 loaded on 1 server at start of test, one segment in datasource foo2 loaded
   * on one server at start of test
   *
   * Expected Results For Each LoadRuleMode:
   *
   * ALL - drop 1 segment form teir1 on first call to run(). drop 1 segment from tier2 on second call to run()
   * PRIMARY_ONLY - drop 0 segments on both calls to run()
   * REPLICA_ONLY - drop 1 segment form teir1 on first call to run(). drop 1 segment from tier2 on second call to run()
   */
  @Test
  public void testDropDuringDecommissioning()
  {
    final LoadQueuePeon mockPeon = createEmptyPeon();

    if (!loadRuleMode.equals(LoadRule.LoadRuleMode.PRIMARY_ONLY)) {
      mockPeon.dropSegment(EasyMock.anyObject(), EasyMock.anyObject());
      EasyMock.expectLastCall().times(2);
      EasyMock.expect(mockBalancerStrategy.pickServersToDrop(EasyMock.anyObject(), EasyMock.anyObject()))
              .andDelegateTo(balancerStrategy)
              .times(4);
    }
    EasyMock.replay(throttler, mockPeon, mockBalancerStrategy);

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
    CoordinatorStats stats = rule.run(null, params, segment1);

    if (!loadRuleMode.equals(LoadRule.LoadRuleMode.PRIMARY_ONLY)) {
      Assert.assertEquals(1L, stats.getTieredStat("droppedCount", "tier1"));
    } else {
      Assert.assertEquals(0L, stats.getTieredStat("droppedCount", "tier1"));
    }

    stats = rule.run(null, params, segment2);

    if (loadRuleMode.equals(LoadRule.LoadRuleMode.ALL) || loadRuleMode.equals(LoadRule.LoadRuleMode.REPLICA_ONLY)) {
      Assert.assertEquals(1L, stats.getTieredStat("droppedCount", "tier1"));
    } else {
      Assert.assertEquals(0L, stats.getTieredStat("droppedCount", "tier1"));
    }

    EasyMock.verify(throttler, mockPeon);
  }

  /**
   * Tests that an extra replicant is properly dropped form decommissioning server and not active server
   *
   * Rule 1: 2 replicants on tier1
   *
   * Tiers: tier1
   *
   * Servers: 3 servers on tier1 (1 is decommissioning)
   *
   * Segments: 1 segment in datasource foo1 loaded to all 3 servers on teir1 before the test.
   *
   * Expected Results For Each LoadRuleMode:
   *
   * ALL - drop 1 replicant from the decommissioning server
   * PRIMARY_ONLY - drop 0 segments
   * REPPICA_ONLY - drop 1 replicant from the decomssioning server
   */
  @Test
  public void testRedundantReplicaDropDuringDecommissioning()
  {
    final LoadQueuePeon mockPeon1 = new LoadQueuePeonTester();
    final LoadQueuePeon mockPeon2 = new LoadQueuePeonTester();
    final LoadQueuePeon mockPeon3 = new LoadQueuePeonTester();

    if (!loadRuleMode.equals(LoadRule.LoadRuleMode.PRIMARY_ONLY)) {
      EasyMock.expect(mockBalancerStrategy.pickServersToDrop(EasyMock.anyObject(), EasyMock.anyObject()))
              .andDelegateTo(balancerStrategy)
              .times(4);
    }
    EasyMock.replay(throttler, mockBalancerStrategy);

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

    CoordinatorStats stats = rule.run(null, makeCoordinatorRuntimeParams(druidCluster, segment1), segment1);

    if (loadRuleMode.equals(LoadRule.LoadRuleMode.ALL) || loadRuleMode.equals(LoadRule.LoadRuleMode.REPLICA_ONLY)) {
      Assert.assertEquals(1L, stats.getTieredStat("droppedCount", "tier1"));
      Assert.assertEquals(0, mockPeon1.getSegmentsToDrop().size());
      Assert.assertEquals(1, mockPeon2.getSegmentsToDrop().size());
      Assert.assertEquals(0, mockPeon3.getSegmentsToDrop().size());
    } else {
      Assert.assertEquals(0L, stats.getTieredStat("droppedCount", "tier1"));
      Assert.assertEquals(0, mockPeon1.getSegmentsToDrop().size());
      Assert.assertEquals(0, mockPeon2.getSegmentsToDrop().size());
      Assert.assertEquals(0, mockPeon3.getSegmentsToDrop().size());
    }
    EasyMock.verify(throttler);
  }

  /**
   * Test that non-primary replicant is properly loaded when primary replicant already loading
   *
   * Rule 1: 2 replicants on DruidServer.DEFAULT_TIER
   *
   * Tiers: DruidServer.DEFAULT_TIER
   *
   * Servers: 2 servers on DruidServer.DEFAULT_TIER
   *
   * Segments: 1 segment in datasource foo that is in loading peon for one segment
   *
   * Expected Results For Each LoadRuleMode:
   *
   * ALL - 1 replicant is loaded
   * PRIMARY_ONLY - 0 replicants are loaded because a primary replicant is already loading.
   * REPLICA_ONLY - 1 replicant is loaded
   */
  @Test
  public void testLoadReplica()
  {
    final DataSegment segment = createDataSegment("foo");

    EasyMock.expect(throttler.canCreateReplicant(EasyMock.anyString())).andReturn(true).anyTimes();

    final LoadQueuePeon mockPeon = createEmptyPeon();
    final LoadQueuePeon loadingPeon = createLoadingPeon(ImmutableList.of(segment), false);
    EasyMock.replay(loadingPeon);

    if (!loadRuleMode.equals(LoadRule.LoadRuleMode.PRIMARY_ONLY)) {
      mockPeon.loadSegment(EasyMock.anyObject(), EasyMock.anyObject());
      EasyMock.expectLastCall().atLeastOnce();

      throttler.registerReplicantCreation(DruidServer.DEFAULT_TIER, segment.getId(), "host1");
      EasyMock.expectLastCall().once();
      EasyMock.expect(mockBalancerStrategy.findNewSegmentHomeReplicator(EasyMock.anyObject(), EasyMock.anyObject()))
              .andDelegateTo(balancerStrategy)
              .anyTimes();
    }

    LoadRule rule = createLoadRule(ImmutableMap.of(
        DruidServer.DEFAULT_TIER, 2
    ));

    EasyMock.replay(throttler, mockPeon, mockBalancerStrategy);

    ImmutableDruidServer server1 =
        new DruidServer(
            "serverHot",
            "host1",
            null,
            1000,
            ServerType.HISTORICAL,
            DruidServer.DEFAULT_TIER,
            1
        ).toImmutableDruidServer();
    ImmutableDruidServer server2 =
        new DruidServer(
            "serverHot2",
            "host2",
            null,
            1000,
            ServerType.HISTORICAL,
            DruidServer.DEFAULT_TIER,
            1
        ).toImmutableDruidServer();
    DruidCluster druidCluster = DruidClusterBuilder
        .newBuilder()
        .addTier(DruidServer.DEFAULT_TIER, new ServerHolder(server1, mockPeon), new ServerHolder(server2, loadingPeon))
        .build();

    CoordinatorStats stats = rule.run(
        null,
        makeCoordinatorRuntimeParams(druidCluster, segment),
        segment
    );


    if (loadRuleMode.equals(LoadRule.LoadRuleMode.ALL) || loadRuleMode.equals(LoadRule.LoadRuleMode.REPLICA_ONLY)) {
      Assert.assertEquals(1L, stats.getTieredStat(LoadRule.ASSIGNED_COUNT, DruidServer.DEFAULT_TIER));
    } else {
      Assert.assertEquals(0L, stats.getTieredStat(LoadRule.ASSIGNED_COUNT, DruidServer.DEFAULT_TIER));
    }
    EasyMock.verify(throttler, mockPeon, mockBalancerStrategy);
  }

  /**
   * Test that a primary replicant and non-primary replicant are loaded properly based on LoadRuleMode:
   *
   * Rule 1: 2 replicants on DruidServer.DEFAULT_TIER
   *
   * Tiers: DruidServer.DEFAULT_TIER
   *
   * Servers: 2 servers on DruidServer.DEFAULT_TIER
   *
   * Segments: 1 segment in datasource foo that is not loaded on any servers before the test.
   *
   * Expected Results For Each LoadRuleMode:
   *
   * ALL - 2 replicants are loaded
   * PRIMARY_ONLY - 1 replicant is loaded
   * REPPICA_ONLY - 0 replicants are loaded because there is no primary replicant at start of test.
   */
  @Test
  public void testLoadPrimaryAndReplica()
  {
    final DataSegment segment = createDataSegment("foo");

    final LoadQueuePeon mockPeon = createEmptyPeon();

    ImmutableDruidServer server1 =
        new DruidServer(
            "server1",
            "host1",
            null,
            1000,
            ServerType.HISTORICAL,
            DruidServer.DEFAULT_TIER,
            1
        ).toImmutableDruidServer();
    ImmutableDruidServer server2 =
        new DruidServer(
            "server2",
            "host2",
            null,
            1000,
            ServerType.HISTORICAL,
            DruidServer.DEFAULT_TIER,
            1
        ).toImmutableDruidServer();
    ServerHolder holder1 = new ServerHolder(server1, mockPeon);
    ServerHolder holder2 = new ServerHolder(server2, mockPeon);

    EasyMock.expect(throttler.canCreateReplicant(EasyMock.anyString())).andReturn(true).anyTimes();


    if (loadRuleMode.equals(LoadRule.LoadRuleMode.ALL)) {
      mockPeon.loadSegment(EasyMock.anyObject(), EasyMock.anyObject());
      EasyMock.expectLastCall().atLeastOnce();
      throttler.registerReplicantCreation(DruidServer.DEFAULT_TIER, segment.getId(), "host2");
      EasyMock.expectLastCall().once();
      EasyMock.expect(mockBalancerStrategy.findNewSegmentHomeReplicator(EasyMock.anyObject(), EasyMock.anyObject()))
              .andReturn(holder1)
              .andReturn(holder2)
              .anyTimes();
    } else if (loadRuleMode.equals(LoadRule.LoadRuleMode.PRIMARY_ONLY)) {
      mockPeon.loadSegment(EasyMock.anyObject(), EasyMock.anyObject());
      EasyMock.expectLastCall().atLeastOnce();
      EasyMock.expect(mockBalancerStrategy.findNewSegmentHomeReplicator(EasyMock.anyObject(), EasyMock.anyObject()))
              .andDelegateTo(balancerStrategy)
              .anyTimes();
    }

    LoadRule rule = createLoadRule(ImmutableMap.of(
        DruidServer.DEFAULT_TIER, 2
    ));

    EasyMock.replay(throttler, mockPeon, mockBalancerStrategy);

    DruidCluster druidCluster = DruidClusterBuilder
        .newBuilder()
        .addTier(DruidServer.DEFAULT_TIER, holder1, holder2)
        .build();

    CoordinatorStats stats = rule.run(
        null,
        makeCoordinatorRuntimeParams(druidCluster, segment),
        segment
    );

    if (loadRuleMode.equals(LoadRule.LoadRuleMode.ALL)) {
      Assert.assertEquals(2L, stats.getTieredStat(LoadRule.ASSIGNED_COUNT, DruidServer.DEFAULT_TIER));
    } else if (loadRuleMode.equals(LoadRule.LoadRuleMode.PRIMARY_ONLY)) {
      Assert.assertEquals(1L, stats.getTieredStat(LoadRule.ASSIGNED_COUNT, DruidServer.DEFAULT_TIER));
    } else {
      Assert.assertEquals(0L, stats.getTieredStat(LoadRule.ASSIGNED_COUNT, DruidServer.DEFAULT_TIER));
    }
    EasyMock.verify(throttler, mockPeon, mockBalancerStrategy);
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
    EasyMock.expect(mockPeon.getSegmentsToLoad()).andReturn(new HashSet<>()).anyTimes();
    EasyMock.expect(mockPeon.getSegmentsMarkedToDrop()).andReturn(new HashSet<>()).anyTimes();
    EasyMock.expect(mockPeon.getLoadQueueSize()).andReturn(0L).anyTimes();
    EasyMock.expect(mockPeon.getNumberOfSegmentsInQueue()).andReturn(0).anyTimes();

    return mockPeon;
  }

  private static LoadQueuePeon createLoadingPeon(List<DataSegment> segments, boolean slowLoading)
  {
    final Set<DataSegment> segs = ImmutableSet.copyOf(segments);
    final long loadingSize = segs.stream().mapToLong(DataSegment::getSize).sum();

    final LoadQueuePeon mockPeon = EasyMock.createMock(LoadQueuePeon.class);
    EasyMock.expect(mockPeon.getSegmentsToLoad()).andReturn(segs).anyTimes();
    EasyMock.expect(mockPeon.getSegmentsMarkedToDrop()).andReturn(new HashSet<>()).anyTimes();
    EasyMock.expect(mockPeon.getLoadQueueSize()).andReturn(loadingSize).anyTimes();
    EasyMock.expect(mockPeon.getNumberOfSegmentsInQueue()).andReturn(segs.size()).anyTimes();

    if (slowLoading) {
      EasyMock.expect(mockPeon.getTimedOutSegments()).andReturn(new HashSet<>(segments)).anyTimes();
    } else {
      EasyMock.expect(mockPeon.getTimedOutSegments()).andReturn(new HashSet<>()).anyTimes();
    }

    return mockPeon;
  }

  private static final AtomicInteger SERVER_ID = new AtomicInteger();

  private static DruidServer createServer(String tier)
  {
    int serverId = LoadRuleTest.SERVER_ID.incrementAndGet();
    //noinspection StringConcatenationMissingWhitespace
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

  private static ServerHolder createServerHolder(String tier, LoadQueuePeon mockPeon1, boolean isDecommissioning)
  {
    return new ServerHolder(
        createServer(tier).toImmutableDruidServer(),
        mockPeon1,
        isDecommissioning
    );
  }
}
