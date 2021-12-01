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

package org.apache.druid.server.coordinator;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.client.DruidServer;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceEventBuilder;
import org.apache.druid.metadata.MetadataRuleManager;
import org.apache.druid.metadata.SegmentsMetadataManager;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.duty.RunRules;
import org.apache.druid.server.coordinator.rules.ForeverLoadRule;
import org.apache.druid.server.coordinator.rules.IntervalDropRule;
import org.apache.druid.server.coordinator.rules.IntervalLoadRule;
import org.apache.druid.server.coordinator.rules.LoadRule;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;

@RunWith(Parameterized.class)
public class RunRulesTest
{
  public static final CoordinatorDynamicConfig COORDINATOR_CONFIG_WITH_ZERO_LEADING_TIME_BEFORE_CAN_MARK_AS_UNUSED_OVERSHADOWED_SEGMENTS =
      CoordinatorDynamicConfig.builder().withLeadingTimeMillisBeforeCanMarkAsUnusedOvershadowedSegments(0L).build();

  private DruidCoordinator coordinator;
  private LoadQueuePeon mockPeon;
  private List<DataSegment> usedSegments;
  private RunRules ruleRunner;
  private ServiceEmitter emitter;
  private MetadataRuleManager databaseRuleManager;
  private SegmentsMetadataManager segmentsMetadataManager;
  private final LoadRule.LoadRuleMode loadRuleMode;

  public RunRulesTest(LoadRule.LoadRuleMode loadRuleMode)
  {
    this.loadRuleMode = loadRuleMode;
  }

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

  @Before
  public void setUp()
  {
    coordinator = EasyMock.createMock(DruidCoordinator.class);
    mockPeon = EasyMock.createMock(LoadQueuePeon.class);
    emitter = EasyMock.createMock(ServiceEmitter.class);
    EmittingLogger.registerEmitter(emitter);
    databaseRuleManager = EasyMock.createMock(MetadataRuleManager.class);
    segmentsMetadataManager = EasyMock.createNiceMock(SegmentsMetadataManager.class);

    DateTime start = DateTimes.of("2012-01-01");
    usedSegments = new ArrayList<>();
    for (int i = 0; i < 24; i++) {
      usedSegments.add(
          new DataSegment(
              "test",
              new Interval(start, start.plusHours(1)),
              DateTimes.nowUtc().toString(),
              new HashMap<>(),
              new ArrayList<>(),
              new ArrayList<>(),
              NoneShardSpec.instance(),
              IndexIO.CURRENT_VERSION_ID,
              1
          )
      );
      start = start.plusHours(1);
    }

    ruleRunner = new RunRules(new ReplicationThrottler(24, 1, false), coordinator);
  }

  @After
  public void tearDown()
  {
    EasyMock.verify(coordinator);
    EasyMock.verify(databaseRuleManager);
  }

  /**
   * Nodes:
   * normal - 2 replicants
   * maxNonPrimaryReplicantsToLoad - 10
   * Expect only 34 segments to be loaded despite there being 48 primary + non-primary replicants to load!
   *
   * Expected Results For Each LoadRuleMode:
   *
   * ALL - 34 replicants loaded
   * PRIMARY_ONLY - 24 replicants loaded
   * REPLICA_ONLY - 0 replicants loaded
   */
  @Test
  public void testOneTierTwoReplicantsWithStrictReplicantLimit()
  {
    mockCoordinator();
    if (loadRuleMode.equals(LoadRule.LoadRuleMode.ALL) || loadRuleMode.equals(LoadRule.LoadRuleMode.PRIMARY_ONLY)) {
      mockPeon.loadSegment(EasyMock.anyObject(), EasyMock.anyObject());
      EasyMock.expectLastCall().atLeastOnce();
    }
    mockEmptyPeon();

    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.anyObject())).andReturn(
        Collections.singletonList(
            new IntervalLoadRule(
                Intervals.of("2012-01-01T00:00:00.000Z/2012-01-02T00:00:00.000Z"),
                ImmutableMap.of("normal", 2)
            )
        )).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    DruidCluster druidCluster = DruidClusterBuilder
        .newBuilder()
        .addTier(
            "normal",
            new ServerHolder(
                new DruidServer("serverNorm", "hostNorm", null, 1000, ServerType.HISTORICAL, "normal", 0)
                    .toImmutableDruidServer(),
                mockPeon
            ),
            new ServerHolder(
                new DruidServer("serverNorm2", "hostNorm2", null, 1000, ServerType.HISTORICAL, "normal", 0)
                    .toImmutableDruidServer(),
                mockPeon
            )
        )
        .build();

    ListeningExecutorService exec = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1));
    BalancerStrategy balancerStrategy = new CostBalancerStrategyFactory().createBalancerStrategy(exec);

    DruidCoordinatorRuntimeParams params = makeCoordinatorRuntimeParams(druidCluster, balancerStrategy)
        .withDynamicConfigs(CoordinatorDynamicConfig.builder().withMaxSegmentsToMove(5).withMaxNonPrimaryReplicantsToLoad(10).build())
        .build();

    DruidCoordinatorRuntimeParams afterParams = ruleRunner.run(params);
    Assert.assertNotNull(afterParams);
    CoordinatorStats stats = afterParams.getCoordinatorStats();

    if (loadRuleMode.equals(LoadRule.LoadRuleMode.ALL)) {
      Assert.assertEquals(34L, stats.getTieredStat("assignedCount", "normal"));
      Assert.assertEquals(10L, stats.getGlobalStat("totalNonPrimaryReplicantsLoaded"));
    } else if (loadRuleMode.equals(LoadRule.LoadRuleMode.PRIMARY_ONLY)) {
      Assert.assertEquals(24L, stats.getTieredStat("assignedCount", "normal"));
      Assert.assertEquals(0L, stats.getGlobalStat("totalNonPrimaryReplicantsLoaded"));
    } else {
      Assert.assertEquals(0L, stats.getTieredStat("assignedCount", "normal"));
      Assert.assertEquals(0L, stats.getGlobalStat("totalNonPrimaryReplicantsLoaded"));
    }

    exec.shutdown();
    EasyMock.verify(mockPeon);
  }

  /**
   * Nodes:
   * normal - 2 replicants
   * hot - 2 replicants
   * maxNonPrimaryReplicantsToLoad - 48
   * Expect only 72 segments to be loaded despite there being 96 primary + non-primary replicants to load!
   *
   * Expected Results For Each LoadRuleMode:
   *
   * ALL - 72 replicants loaded
   * PRIMARY_ONLY - 24 replicants loaded
   * REPLICA_ONLY - 0 replicants loaded
   */
  @Test
  public void testTwoTiersTwoReplicantsWithStrictReplicantLimit()
  {
    mockCoordinator();
    if (loadRuleMode.equals(LoadRule.LoadRuleMode.ALL) || loadRuleMode.equals(LoadRule.LoadRuleMode.PRIMARY_ONLY)) {
      mockPeon.loadSegment(EasyMock.anyObject(), EasyMock.anyObject());
      EasyMock.expectLastCall().atLeastOnce();
    }
    mockEmptyPeon();

    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.anyObject())).andReturn(
        Collections.singletonList(
            new IntervalLoadRule(
                Intervals.of("2012-01-01T00:00:00.000Z/2012-01-02T00:00:00.000Z"),
                ImmutableMap.of("hot", 2, "normal", 2)
            )
        )).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    DruidCluster druidCluster = DruidClusterBuilder
        .newBuilder()
        .addTier(
            "hot",
            new ServerHolder(
                new DruidServer("serverHot", "hostHot", null, 1000, ServerType.HISTORICAL, "hot", 0)
                    .toImmutableDruidServer(),
                mockPeon
            ),
            new ServerHolder(
                new DruidServer("serverHot2", "hostHot2", null, 1000, ServerType.HISTORICAL, "hot", 0)
                    .toImmutableDruidServer(),
                mockPeon
            )
        )
        .addTier(
            "normal",
            new ServerHolder(
                new DruidServer("serverNorm", "hostNorm", null, 1000, ServerType.HISTORICAL, "normal", 0)
                    .toImmutableDruidServer(),
                mockPeon
            ),
            new ServerHolder(
                new DruidServer("serverNorm2", "hostNorm2", null, 1000, ServerType.HISTORICAL, "normal", 0)
                    .toImmutableDruidServer(),
                mockPeon
            )
        )
        .build();

    ListeningExecutorService exec = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1));
    BalancerStrategy balancerStrategy = new CostBalancerStrategyFactory().createBalancerStrategy(exec);

    DruidCoordinatorRuntimeParams params = makeCoordinatorRuntimeParams(druidCluster, balancerStrategy)
        .withDynamicConfigs(CoordinatorDynamicConfig.builder().withMaxSegmentsToMove(5).withMaxNonPrimaryReplicantsToLoad(48).build())
        .build();

    DruidCoordinatorRuntimeParams afterParams = ruleRunner.run(params);
    Assert.assertNotNull(afterParams);
    CoordinatorStats stats = afterParams.getCoordinatorStats();

    if (loadRuleMode.equals(LoadRule.LoadRuleMode.ALL)) {
      Assert.assertEquals(
          72L,
          stats.getTieredStat("assignedCount", "hot") +
          stats.getTieredStat("assignedCount", "normal")
      );
    } else if (loadRuleMode.equals(LoadRule.LoadRuleMode.PRIMARY_ONLY)) {
      Assert.assertEquals(
          24L,
          stats.getTieredStat("assignedCount", "hot") +
          stats.getTieredStat("assignedCount", "normal")
      );
    } else {
      Assert.assertEquals(
          0L,
          stats.getTieredStat("assignedCount", "hot") +
          stats.getTieredStat("assignedCount", "normal")
      );
    }

    exec.shutdown();
    EasyMock.verify(mockPeon);
  }

  /**
   * Tests segment assign functionality across tiers with rules that overlap. Since a segment can only match 1 rule,
   * the segments won't always have replicants on tiers for every interval covered by the rule.
   *
   * Rule 1: T0 - T6 1 replicant on hot tier
   * Rule 2: T0 - T12 1 replicant on normal tier
   * Rule 3: T0 - T0 (next day) 1 replicant on cold tier.
   *
   * Servers: 1 server on hot tier, 1 server on normal tier, 1 servr on cold tier
   *
   * Segments: 24 hourly segments covering from T0 to T0 (next day). None loaded before test.
   *
   * Expected Results For Each LoadRuleMode:
   *
   * ALL - Load 6 replicants in hot tier. Load 6 replicants in normal tier. Load 12 replicants in cold tier
   * PRIMARY_ONLY - Load 6 replicants in hot tier. Load 6 replicants in normal tier. Load 12 replicants in cold tier
   * REPLICA_ONLY - Load 0 replicants.
   */
  @Test
  public void testRunThreeTiersOneReplicant()
  {
    mockCoordinator();
    if (loadRuleMode.equals(LoadRule.LoadRuleMode.ALL) || loadRuleMode.equals(LoadRule.LoadRuleMode.PRIMARY_ONLY)) {
      mockPeon.loadSegment(EasyMock.anyObject(), EasyMock.anyObject());
      EasyMock.expectLastCall().atLeastOnce();
    }
    mockEmptyPeon();

    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.anyObject())).andReturn(
        Lists.newArrayList(
            new IntervalLoadRule(
                Intervals.of("2012-01-01T00:00:00.000Z/2012-01-01T06:00:00.000Z"),
                ImmutableMap.of("hot", 1)
            ),
            new IntervalLoadRule(
                Intervals.of("2012-01-01T00:00:00.000Z/2012-01-01T12:00:00.000Z"),
                ImmutableMap.of("normal", 1)
            ),
            new IntervalLoadRule(
                Intervals.of("2012-01-01T00:00:00.000Z/2012-01-02T00:00:00.000Z"),
                ImmutableMap.of("cold", 1)
            )
        )).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

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
        .addTier(
            "normal",
            new ServerHolder(
                new DruidServer("serverNorm", "hostNorm", null, 1000, ServerType.HISTORICAL, "normal", 0)
                    .toImmutableDruidServer(),
                mockPeon
            )
        )
        .addTier(
            "cold",
            new ServerHolder(
                new DruidServer("serverCold", "hostCold", null, 1000, ServerType.HISTORICAL, "cold", 0)
                    .toImmutableDruidServer(),
                mockPeon
            )
        )
        .build();

    ListeningExecutorService exec = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1));
    BalancerStrategy balancerStrategy = new CostBalancerStrategyFactory().createBalancerStrategy(exec);

    DruidCoordinatorRuntimeParams params = makeCoordinatorRuntimeParams(druidCluster, balancerStrategy)
        .withDynamicConfigs(CoordinatorDynamicConfig.builder().withMaxSegmentsToMove(5).build())
        .build();

    DruidCoordinatorRuntimeParams afterParams = ruleRunner.run(params);
    Assert.assertNotNull(afterParams);
    CoordinatorStats stats = afterParams.getCoordinatorStats();

    if (!loadRuleMode.equals(LoadRule.LoadRuleMode.REPLICA_ONLY)) {
      Assert.assertEquals(6L, stats.getTieredStat("assignedCount", "hot"));
      Assert.assertEquals(6L, stats.getTieredStat("assignedCount", "normal"));
      Assert.assertEquals(12L, stats.getTieredStat("assignedCount", "cold"));
    } else {
      Assert.assertEquals(0L, stats.getTieredStat("assignedCount", "hot"));
      Assert.assertEquals(0L, stats.getTieredStat("assignedCount", "normal"));
      Assert.assertEquals(0L, stats.getTieredStat("assignedCount", "cold"));
    }
    Assert.assertTrue(stats.getTiers("unassignedCount").isEmpty());
    Assert.assertTrue(stats.getTiers("unassignedSize").isEmpty());

    exec.shutdown();
    EasyMock.verify(mockPeon);
  }

  private DruidCoordinatorRuntimeParams.Builder makeCoordinatorRuntimeParams(
      DruidCluster druidCluster,
      BalancerStrategy balancerStrategy
  )
  {
    return makeCoordinatorRuntimeParams(druidCluster, balancerStrategy, usedSegments);
  }

  private DruidCoordinatorRuntimeParams.Builder makeCoordinatorRuntimeParams(
      DruidCluster druidCluster,
      BalancerStrategy balancerStrategy,
      List<DataSegment> dataSegments
  )
  {
    return createCoordinatorRuntimeParams(druidCluster, dataSegments)
        .withSegmentReplicantLookup(SegmentReplicantLookup.make(druidCluster, false))
        .withBalancerStrategy(balancerStrategy);
  }

  private DruidCoordinatorRuntimeParams.Builder createCoordinatorRuntimeParams(DruidCluster druidCluster)
  {
    return createCoordinatorRuntimeParams(druidCluster, usedSegments);
  }

  private DruidCoordinatorRuntimeParams.Builder createCoordinatorRuntimeParams(DruidCluster druidCluster, List<DataSegment> dataSegments)
  {
    return CoordinatorRuntimeParamsTestHelpers
        .newBuilder()
        .withDruidCluster(druidCluster)
        .withUsedSegmentsInTest(dataSegments)
        .withDatabaseRuleManager(databaseRuleManager)
        .withLoadRuleMode(loadRuleMode);
  }

  /**
   * Tests segment assign functionality across tiers with rules that overlap. Since a segment can only match 1 rule,
   * the segments won't always have replicants on tiers for every interval covered by the rule.
   *
   * Rule 1: T0 - T6 2 replicants on hot tier
   * Rule 2: T0 - T0 (next day) 1 replicant on cold tier
   *
   * Servers: 2 servers on hot tier, 1 servr on cold tier
   *
   * Segments: 24 hourly segments covering from T0 to T0 (next day). None loaded before test.
   *
   * Expected Results For Each LoadRuleMode:
   *
   * ALL - Load 12 replicants in hot tier. Load 18 replicants in cold tier
   * PRIMARY_ONLY - Load 6 replicants in hot tier. Load 18 replicants in cold tier
   * REPLICA_ONLY - Load 0 replicants.
   */
  @Test
  public void testRunTwoTiersTwoReplicants()
  {
    mockCoordinator();
    if (loadRuleMode.equals(LoadRule.LoadRuleMode.ALL) || loadRuleMode.equals(LoadRule.LoadRuleMode.PRIMARY_ONLY)) {
      mockPeon.loadSegment(EasyMock.anyObject(), EasyMock.anyObject());
      EasyMock.expectLastCall().atLeastOnce();
    }
    mockEmptyPeon();

    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.anyObject())).andReturn(
        Lists.newArrayList(
            new IntervalLoadRule(
                Intervals.of("2012-01-01T00:00:00.000Z/2012-01-01T06:00:00.000Z"),
                ImmutableMap.of("hot", 2)
            ),
            new IntervalLoadRule(
                Intervals.of("2012-01-01T00:00:00.000Z/2012-01-02T00:00:00.000Z"),
                ImmutableMap.of("cold", 1)
            )
        )
    ).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    DruidCluster druidCluster = DruidClusterBuilder
        .newBuilder()
        .addTier(
            "hot",
            new ServerHolder(
                new DruidServer("serverHot", "hostHot", null, 1000, ServerType.HISTORICAL, "hot", 1)
                    .toImmutableDruidServer(),
                mockPeon
            ),
            new ServerHolder(
                new DruidServer("serverHot2", "hostHot2", null, 1000, ServerType.HISTORICAL, "hot", 1)
                    .toImmutableDruidServer(),
                mockPeon
            )
        )
        .addTier(
            "cold",
            new ServerHolder(
                new DruidServer("serverCold", "hostCold", null, 1000, ServerType.HISTORICAL, "cold", 0)
                    .toImmutableDruidServer(),
                mockPeon
            )
        )
        .build();

    ListeningExecutorService exec = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1));
    BalancerStrategy balancerStrategy = new CostBalancerStrategyFactory().createBalancerStrategy(exec);

    DruidCoordinatorRuntimeParams params = makeCoordinatorRuntimeParams(druidCluster, balancerStrategy).build();

    DruidCoordinatorRuntimeParams afterParams = ruleRunner.run(params);
    Assert.assertNotNull(afterParams);
    CoordinatorStats stats = afterParams.getCoordinatorStats();

    if (loadRuleMode.equals(LoadRule.LoadRuleMode.ALL)) {
      Assert.assertEquals(12L, stats.getTieredStat("assignedCount", "hot"));
      Assert.assertEquals(18L, stats.getTieredStat("assignedCount", "cold"));
    } else if (loadRuleMode.equals(LoadRule.LoadRuleMode.PRIMARY_ONLY)) {
      Assert.assertEquals(6L, stats.getTieredStat("assignedCount", "hot"));
      Assert.assertEquals(18L, stats.getTieredStat("assignedCount", "cold"));
    } else {
      Assert.assertEquals(0L, stats.getTieredStat("assignedCount", "hot"));
      Assert.assertEquals(0L, stats.getTieredStat("assignedCount", "cold"));
    }
    Assert.assertTrue(stats.getTiers("unassignedCount").isEmpty());
    Assert.assertTrue(stats.getTiers("unassignedSize").isEmpty());

    exec.shutdown();
    EasyMock.verify(mockPeon);
  }

  /**
   * Tests segment assign functionality across tiers with rules that overlap. Since a segment can only match 1 rule,
   * the segments won't always have replicants on tiers for every interval covered by the rule. This test also has
   * segments pre-loaded onto some servers before test execution.
   *
   * Rule 1: T0 - T12 1 replicant on hot tier
   * Rule 2: T0 - T0 (next day) 1 replicant on normal tier
   *
   * Servers: 1 server on hot tier, 1 server on normal tier
   *
   * Segments: 24 hourly segments covering from T0 to T0 (next day). All 24 segments loade onto normal tier server
   * before test.
   *
   * Expected Results For Each LoadRuleMode:
   *
   * ALL - Load 12 replicants in hot tier. Load 0 replicants on normal tier.
   * PRIMARY_ONLY - Load 0 replicants.
   * REPLICA_ONLY - Load 12 replicants on hot tier. Load 0 replicants on normal tier.
   */
  @Test
  public void testRunTwoTiersWithExistingSegments()
  {
    mockCoordinator();
    if (!loadRuleMode.equals(LoadRule.LoadRuleMode.PRIMARY_ONLY)) {
      mockPeon.loadSegment(EasyMock.anyObject(), EasyMock.anyObject());
      EasyMock.expectLastCall().atLeastOnce();
    }
    mockEmptyPeon();

    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.anyObject())).andReturn(
        Lists.newArrayList(
            new IntervalLoadRule(
                Intervals.of("2012-01-01T00:00:00.000Z/2012-01-01T12:00:00.000Z"),
                ImmutableMap.of("hot", 1)
            ),
            new IntervalLoadRule(
                Intervals.of("2012-01-01T00:00:00.000Z/2012-01-02T00:00:00.000Z"),
                ImmutableMap.of("normal", 1)
            )
        )
    ).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    DruidServer normServer = new DruidServer("serverNorm", "hostNorm", null, 1000, ServerType.HISTORICAL, "normal", 0);
    for (DataSegment segment : usedSegments) {
      normServer.addDataSegment(segment);
    }

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
        .addTier("normal", new ServerHolder(normServer.toImmutableDruidServer(), mockPeon))
        .build();

    SegmentReplicantLookup segmentReplicantLookup = SegmentReplicantLookup.make(druidCluster, false);

    ListeningExecutorService exec = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1));
    BalancerStrategy balancerStrategy = new CostBalancerStrategyFactory().createBalancerStrategy(exec);

    DruidCoordinatorRuntimeParams params = createCoordinatorRuntimeParams(druidCluster)
        .withSegmentReplicantLookup(segmentReplicantLookup)
        .withBalancerStrategy(balancerStrategy)
        .build();

    DruidCoordinatorRuntimeParams afterParams = ruleRunner.run(params);
    Assert.assertNotNull(afterParams);
    CoordinatorStats stats = afterParams.getCoordinatorStats();

    if (loadRuleMode.equals(LoadRule.LoadRuleMode.ALL) || loadRuleMode.equals(LoadRule.LoadRuleMode.REPLICA_ONLY)) {
      Assert.assertEquals(12L, stats.getTieredStat("assignedCount", "hot"));
    } else {
      Assert.assertEquals(0L, stats.getTieredStat("assignedCount", "hot"));
    }
    Assert.assertEquals(0L, stats.getTieredStat("assignedCount", "normal"));
    Assert.assertTrue(stats.getTiers("unassignedCount").isEmpty());
    Assert.assertTrue(stats.getTiers("unassignedSize").isEmpty());

    exec.shutdown();
    EasyMock.verify(mockPeon);
  }

  /**
   * Tests segment assign functionality across tiers with rules that overlap. Since a segment can only match 1 rule,
   * the segments won't always have replicants on tiers for every interval covered by the rule. This test also tests what
   * happens when one of the tiers in the load rules does not exist with servers to load segments to.
   *
   * Rule 1: T0 - T12 1 replicant on hot tier
   * Rule 2: T0 - T0 (next day) 1 replicant on normal tier
   *
   * Servers: 1 server on normal tier (meaning there is no hot tier server for segments thatt match that rule)
   *
   * Segments: 24 hourly segments covering from T0 to T0 (next day). The segments are not loaded before the test.
   *
   * Expected Results For Each LoadRuleMode:
   *
   * ALL - Load 12 replicants in normal tier and emit 12 alerts due to no servers existing
   * PRIMARY_ONLY - load 12 replicants in normal tier and emit 12 alerts due to no servers existing
   * REPLICA_ONLY - Load 0 replicants and emit 0 alerts.
   */
  @Test
  public void testRunTwoTiersTierDoesNotExist()
  {
    mockCoordinator();
    if (!loadRuleMode.equals(LoadRule.LoadRuleMode.REPLICA_ONLY)) {
      mockPeon.loadSegment(EasyMock.anyObject(), EasyMock.anyObject());
      EasyMock.expectLastCall().atLeastOnce();
    }
    mockEmptyPeon();

    if (!loadRuleMode.equals(LoadRule.LoadRuleMode.REPLICA_ONLY)) {
      emitter.emit(EasyMock.<ServiceEventBuilder<?>>anyObject());
      EasyMock.expectLastCall().times(12);
    }
    EasyMock.replay(emitter);

    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.anyObject())).andReturn(
        Lists.newArrayList(
            new IntervalLoadRule(
                Intervals.of("2012-01-01T00:00:00.000Z/2012-01-01T12:00:00.000Z"),
                ImmutableMap.of("hot", 1)
            ),
            new IntervalLoadRule(
                Intervals.of("2012-01-01T00:00:00.000Z/2012-01-02T00:00:00.000Z"),
                ImmutableMap.of("normal", 1)
            )
        )
    ).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    DruidCluster druidCluster = DruidClusterBuilder
        .newBuilder()
        .addTier(
            "normal",
            new ServerHolder(
                new DruidServer("serverNorm", "hostNorm", null, 1000, ServerType.HISTORICAL, "normal", 0)
                    .toImmutableDruidServer(),
                mockPeon
            )
        )
        .build();

    ListeningExecutorService exec = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1));
    BalancerStrategy balancerStrategy = new CostBalancerStrategyFactory().createBalancerStrategy(exec);

    DruidCoordinatorRuntimeParams params = makeCoordinatorRuntimeParams(druidCluster, balancerStrategy)
        .withEmitter(emitter)
        .build();

    params = ruleRunner.run(params);
    Assert.assertNotNull(params);
    CoordinatorStats stats = params.getCoordinatorStats();

    if (loadRuleMode.equals(LoadRule.LoadRuleMode.ALL) || loadRuleMode.equals(LoadRule.LoadRuleMode.PRIMARY_ONLY)) {
      Assert.assertEquals(12L, stats.getTieredStat("assignedCount", "normal"));
    } else {
      Assert.assertEquals(0L, stats.getTieredStat("assignedCount", "normal"));
    }

    exec.shutdown();
    EasyMock.verify(emitter);
    EasyMock.verify(mockPeon);
  }

  /**
   * Tests that nothing is loaded when there are not matching LoadRules.
   */
  @Test
  public void testRunRuleDoesNotExist()
  {
    mockCoordinator();
    emitter.emit(EasyMock.<ServiceEventBuilder<?>>anyObject());
    EasyMock.expectLastCall().times(1);
    EasyMock.replay(emitter);

    EasyMock
        .expect(databaseRuleManager.getRulesWithDefault(EasyMock.anyObject()))
        .andReturn(
            Collections.singletonList(
                new IntervalLoadRule(
                    Intervals.of("2012-01-02T00:00:00.000Z/2012-01-03T00:00:00.000Z"),
                    ImmutableMap.of("normal", 1)
                )
            )
        )
        .atLeastOnce();

    EasyMock.expect(mockPeon.getLoadQueueSize()).andReturn(0L).anyTimes();
    EasyMock.replay(databaseRuleManager, mockPeon);

    DruidCluster druidCluster = DruidClusterBuilder
        .newBuilder()
        .addTier(
            "normal",
            new ServerHolder(
                new DruidServer("serverNorm", "hostNorm", null, 1000, ServerType.HISTORICAL, "normal", 0)
                    .toImmutableDruidServer(),
                mockPeon
            )
        )
        .build();

    DruidCoordinatorRuntimeParams params = createCoordinatorRuntimeParams(druidCluster)
        .withSegmentReplicantLookup(SegmentReplicantLookup.make(new DruidCluster(), false))
        .withEmitter(emitter)
        .build();

    ruleRunner.run(params);

    EasyMock.verify(emitter, mockPeon);
  }

  /**
   * Tests a basic DropRule to ensure proper segments are dropped.
   *
   * Rule 1: T0 - T12 1 replicant on normal tier
   * Rule 2: T0 - T0 (next day) DropRule
   *
   * Servers: 1 server on normal tier
   *
   * Segments: 24 hourly segments covering from T0 to T0 (next day). The segments are loaded on the test server.
   *
   * Expected Results For Each LoadRuleMode:
   *
   * ALL - 12 segments dropped
   * PRIMARY_ONLY - 12 segments dropped
   * REPLICA_ONLY - 12 segments dropped
   */
  @Test
  public void testDropRemove()
  {
    mockPeon.dropSegment(EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    mockEmptyPeon();

    EasyMock.expect(coordinator.getDynamicConfigs()).andReturn(createCoordinatorDynamicConfig()).anyTimes();
    coordinator.markSegmentAsUnused(EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.replay(coordinator);

    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.anyObject())).andReturn(
        Lists.newArrayList(
            new IntervalLoadRule(
                Intervals.of("2012-01-01T00:00:00.000Z/2012-01-01T12:00:00.000Z"),
                ImmutableMap.of("normal", 1)
            ),
            new IntervalDropRule(Intervals.of("2012-01-01T00:00:00.000Z/2012-01-02T00:00:00.000Z"))
        )
    ).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    DruidServer server = new DruidServer("serverNorm", "hostNorm", null, 1000, ServerType.HISTORICAL, "normal", 0);
    for (DataSegment segment : usedSegments) {
      server.addDataSegment(segment);
    }

    DruidCluster druidCluster = DruidClusterBuilder
        .newBuilder()
        .addTier("normal", new ServerHolder(server.toImmutableDruidServer(), mockPeon))
        .build();

    SegmentReplicantLookup segmentReplicantLookup = SegmentReplicantLookup.make(druidCluster, false);

    ListeningExecutorService exec = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1));
    BalancerStrategy balancerStrategy = new CostBalancerStrategyFactory().createBalancerStrategy(exec);

    DruidCoordinatorRuntimeParams params = createCoordinatorRuntimeParams(druidCluster)
        .withDynamicConfigs(COORDINATOR_CONFIG_WITH_ZERO_LEADING_TIME_BEFORE_CAN_MARK_AS_UNUSED_OVERSHADOWED_SEGMENTS)
        .withSegmentReplicantLookup(segmentReplicantLookup)
        .withBalancerStrategy(balancerStrategy)
        .build();

    DruidCoordinatorRuntimeParams afterParams = ruleRunner.run(params);
    Assert.assertNotNull(afterParams);
    CoordinatorStats stats = afterParams.getCoordinatorStats();

    Assert.assertEquals(12L, stats.getGlobalStat("deletedCount"));

    exec.shutdown();
    EasyMock.verify(coordinator);
  }

  /**
   * Test that if RunRulesMode is LOAD_RULES_ONLY, rules that are not a LoadRule
   * are not ran.
   *
   * Rule 1: T0 - T12 1 replicant on normal tier
   * Rule 2: T0 - T0 (next day) DropRule
   *
   * Servers: 1 server on normal tier
   *
   * Segments: 24 hourly segments covering from T0 to T0 (next day). The segments are loaded on the test server.
   *
   * Expected Results For Each LoadRuleMode:
   *
   * ALL - 0 segments dropped because we are in LOAD_RULES_ONLY mode
   * PRIMARY_ONLY - 12 segments dropped because we are in LOAD_RULES_ONLY mode
   * REPLICA_ONLY - 12 segments dropped because we are in LOAD_RULES_ONLY mode
   */
  @Test
  public void testNonLoadRuleIsNotRunInLoadRuleOnlyMode()
  {
    mockEmptyPeon();

    EasyMock.expect(coordinator.getDynamicConfigs()).andReturn(createCoordinatorDynamicConfig()).anyTimes();
    EasyMock.replay(coordinator);

    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.anyObject())).andReturn(
        Lists.newArrayList(
            new IntervalLoadRule(
                Intervals.of("2012-01-01T00:00:00.000Z/2012-01-01T12:00:00.000Z"),
                ImmutableMap.of("normal", 1)
            ),
            new IntervalDropRule(Intervals.of("2012-01-01T00:00:00.000Z/2012-01-02T00:00:00.000Z"))
        )
    ).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    DruidServer server = new DruidServer("serverNorm", "hostNorm", null, 1000, ServerType.HISTORICAL, "normal", 0);
    for (DataSegment segment : usedSegments) {
      server.addDataSegment(segment);
    }

    DruidCluster druidCluster = DruidClusterBuilder
        .newBuilder()
        .addTier("normal", new ServerHolder(server.toImmutableDruidServer(), mockPeon))
        .build();

    SegmentReplicantLookup segmentReplicantLookup = SegmentReplicantLookup.make(druidCluster, false);

    ListeningExecutorService exec = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1));
    BalancerStrategy balancerStrategy = new CostBalancerStrategyFactory().createBalancerStrategy(exec);

    DruidCoordinatorRuntimeParams params = createCoordinatorRuntimeParams(druidCluster)
        .withDynamicConfigs(COORDINATOR_CONFIG_WITH_ZERO_LEADING_TIME_BEFORE_CAN_MARK_AS_UNUSED_OVERSHADOWED_SEGMENTS)
        .withSegmentReplicantLookup(segmentReplicantLookup)
        .withBalancerStrategy(balancerStrategy)
        .withRunRulesMode(RunRules.RunRulesMode.LOAD_RULE_ONLY)
        .build();

    DruidCoordinatorRuntimeParams afterParams = ruleRunner.run(params);
    Assert.assertNotNull(afterParams);
    CoordinatorStats stats = afterParams.getCoordinatorStats();

    // Despite matching DropRule, we delete 0 segments, because we are in LOAD_RULE_ONLY mode
    Assert.assertEquals(0L, stats.getGlobalStat("deletedCount"));

    exec.shutdown();
    EasyMock.verify(coordinator);
  }

  /**
   * Test that segment is properly dropped by LoadRule and segments are properly deleted by DropRule
   *
   * Rule 1: T0 - T12 1 replicant on normal tier
   * Rule 2: T0 - T0 (next day) DropRule
   *
   * Servers: 2 servers on normal tier
   *
   * Segments: 24 hourly segments covering from T0 to T0 (next day). The T0-T1 segment is loaded on server1. All 24
   * segments are loaded on server2
   *
   * Expected Results For Each LoadRuleMode:
   *
   * ALL - 1 segment dropped and 12 segments deleted
   * PRIMARY_ONLY - 0 segments dropped and 12 segments deleted
   * REPLICA_ONLY - 1 segment dropped and 12 segments deleted
   */
  @Test
  public void testDropTooManyInSameTier()
  {
    mockCoordinator();
    if (!loadRuleMode.equals(LoadRule.LoadRuleMode.PRIMARY_ONLY)) {
      mockPeon.dropSegment(EasyMock.anyObject(), EasyMock.anyObject());
      EasyMock.expectLastCall().atLeastOnce();
    }
    mockEmptyPeon();

    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.anyObject())).andReturn(
        Lists.newArrayList(
            new IntervalLoadRule(
                Intervals.of("2012-01-01T00:00:00.000Z/2012-01-01T12:00:00.000Z"),
                ImmutableMap.of("normal", 1)
            ),
            new IntervalDropRule(Intervals.of("2012-01-01T00:00:00.000Z/2012-01-02T00:00:00.000Z"))
        )
    ).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    DruidServer server1 = new DruidServer("serverNorm", "hostNorm", null, 1000, ServerType.HISTORICAL, "normal", 0);
    server1.addDataSegment(usedSegments.get(0));

    DruidServer server2 = new DruidServer("serverNorm2", "hostNorm2", null, 1000, ServerType.HISTORICAL, "normal", 0);
    for (DataSegment segment : usedSegments) {
      server2.addDataSegment(segment);
    }

    DruidCluster druidCluster = DruidClusterBuilder
        .newBuilder()
        .addTier(
            "normal",
            new ServerHolder(server1.toImmutableDruidServer(), mockPeon),
            new ServerHolder(server2.toImmutableDruidServer(), mockPeon)
        )
        .build();

    SegmentReplicantLookup segmentReplicantLookup = SegmentReplicantLookup.make(druidCluster, false);

    ListeningExecutorService exec = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1));
    BalancerStrategy balancerStrategy = new CostBalancerStrategyFactory().createBalancerStrategy(exec);

    DruidCoordinatorRuntimeParams params = createCoordinatorRuntimeParams(druidCluster)
        .withDynamicConfigs(
            CoordinatorDynamicConfig
                .builder()
                .withLeadingTimeMillisBeforeCanMarkAsUnusedOvershadowedSegments(0L)
                .build()
        )
        .withSegmentReplicantLookup(segmentReplicantLookup)
        .withBalancerStrategy(balancerStrategy)
        .build();

    DruidCoordinatorRuntimeParams afterParams = ruleRunner.run(params);
    Assert.assertNotNull(afterParams);
    CoordinatorStats stats = afterParams.getCoordinatorStats();

    if (loadRuleMode.equals(LoadRule.LoadRuleMode.ALL) || loadRuleMode.equals(LoadRule.LoadRuleMode.REPLICA_ONLY)) {
      Assert.assertEquals(1L, stats.getTieredStat("droppedCount", "normal"));
    } else {
      Assert.assertEquals(0L, stats.getTieredStat("droppedCount", "normal"));
    }
    Assert.assertEquals(12L, stats.getGlobalStat("deletedCount"));

    exec.shutdown();
    EasyMock.verify(mockPeon);
  }

  /**
   * Test assign, drop, and delete functionality across tiers
   *
   * Rule 1: T0 - T12 1 replicant on hot tier
   * Rule 2: T0 - T0 (next day) DropRule
   *
   * Servers: 1 server on hot tier, 1 serer on normal tier
   *
   * Segments: 24 hourly segments covering from T0 to T0 (next day). The T0-T1 segment is loaded on hot tier server. All 24
   * segments are loaded on normal tier server
   *
   * Expected Results For Each LoadRuleMode:
   *
   * ALL - 11 segments assigned to hot. 1 segment dropped from normal. 12 segments deleted from normal
   * PRIMARY_ONLY - 0 segments assigned to hot. 0 segments dropped from normal. 12 segments deleted from normal
   * REPLICA_ONLY - 11 segments assigned to hot. 1 segment dropped from normal. 12 segments deleted from normal
   */
  @Test
  public void testDropTooManyInDifferentTiers()
  {
    mockCoordinator();
    if (!loadRuleMode.equals(LoadRule.LoadRuleMode.PRIMARY_ONLY)) {
      mockPeon.loadSegment(EasyMock.anyObject(), EasyMock.anyObject());
      EasyMock.expectLastCall().atLeastOnce();
      mockPeon.dropSegment(EasyMock.anyObject(), EasyMock.anyObject());
      EasyMock.expectLastCall().atLeastOnce();
    }
    mockEmptyPeon();

    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.anyObject())).andReturn(
        Lists.newArrayList(
            new IntervalLoadRule(
                Intervals.of("2012-01-01T00:00:00.000Z/2012-01-01T12:00:00.000Z"),
                ImmutableMap.of("hot", 1)
            ),
            new IntervalDropRule(Intervals.of("2012-01-01T00:00:00.000Z/2012-01-02T00:00:00.000Z"))
        )
    ).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    DruidServer server1 = new DruidServer("server1", "host1", null, 1000, ServerType.HISTORICAL, "hot", 0);
    server1.addDataSegment(usedSegments.get(0));
    DruidServer server2 = new DruidServer("serverNorm2", "hostNorm2", null, 1000, ServerType.HISTORICAL, "normal", 0);
    for (DataSegment segment : usedSegments) {
      server2.addDataSegment(segment);
    }

    DruidCluster druidCluster = DruidClusterBuilder
        .newBuilder()
        .addTier("hot", new ServerHolder(server1.toImmutableDruidServer(), mockPeon))
        .addTier("normal", new ServerHolder(server2.toImmutableDruidServer(), mockPeon))
        .build();

    SegmentReplicantLookup segmentReplicantLookup = SegmentReplicantLookup.make(druidCluster, false);

    ListeningExecutorService exec = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1));
    BalancerStrategy balancerStrategy = new CostBalancerStrategyFactory().createBalancerStrategy(exec);

    DruidCoordinatorRuntimeParams params = createCoordinatorRuntimeParams(druidCluster)
        .withDynamicConfigs(COORDINATOR_CONFIG_WITH_ZERO_LEADING_TIME_BEFORE_CAN_MARK_AS_UNUSED_OVERSHADOWED_SEGMENTS)
        .withSegmentReplicantLookup(segmentReplicantLookup)
        .withBalancerStrategy(balancerStrategy)
        .build();

    DruidCoordinatorRuntimeParams afterParams = ruleRunner.run(params);
    Assert.assertNotNull(afterParams);
    CoordinatorStats stats = afterParams.getCoordinatorStats();

    if (loadRuleMode.equals(LoadRule.LoadRuleMode.ALL) || loadRuleMode.equals(LoadRule.LoadRuleMode.REPLICA_ONLY)) {
      Assert.assertEquals(11L, stats.getTieredStat("assignedCount", "hot"));
      Assert.assertEquals(1L, stats.getTieredStat("droppedCount", "normal"));
      Assert.assertEquals(12L, stats.getGlobalStat("deletedCount"));
    } else if (loadRuleMode.equals(LoadRule.LoadRuleMode.PRIMARY_ONLY)) {
      Assert.assertEquals(0L, stats.getTieredStat("assignedCount", "hot"));
      Assert.assertEquals(0L, stats.getTieredStat("droppedCount", "normal"));
      Assert.assertEquals(12L, stats.getGlobalStat("deletedCount"));
    }

    exec.shutdown();
    EasyMock.verify(mockPeon);
  }

  /**
   * Test delete functionality across tiers
   *
   * Rule 1: T0 - T12 1 replicant on hot tier
   * Rule 2: T0 - T0 (next day) DropRule
   *
   * Servers: 1 server on hot tier, 1 serer on normal tier
   *
   * Segments: 24 hourly segments covering from T0 to T0 (next day). All 24 segments are loaded on normal tier server
   *
   * Expected Results For Each LoadRuleMode:
   *
   * ALL - 0 segments dropped. 12 segments deleted from normal.
   * PRIMARY_ONLY - 0 segments dropped. 12 segments deleted from normal.
   * REPLICA_ONLY - 0 segments dropped. 12 segments deleted from normal.
   */
  @Test
  public void testDontDropInDifferentTiers()
  {
    mockCoordinator();
    // We won't load any non-primary replicants in PRIMARY_ONLY Mode
    if (!loadRuleMode.equals(LoadRule.LoadRuleMode.PRIMARY_ONLY)) {
      mockPeon.loadSegment(EasyMock.anyObject(), EasyMock.anyObject());
      EasyMock.expectLastCall().atLeastOnce();
    }
    mockEmptyPeon();

    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.anyObject())).andReturn(
        Lists.newArrayList(
            new IntervalLoadRule(
                Intervals.of("2012-01-01T00:00:00.000Z/2012-01-01T12:00:00.000Z"),
                ImmutableMap.of("hot", 1)
            ),
            new IntervalDropRule(Intervals.of("2012-01-01T00:00:00.000Z/2012-01-02T00:00:00.000Z"))
        )
    ).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    DruidServer server1 = new DruidServer("server1", "host1", null, 1000, ServerType.HISTORICAL, "hot", 0);
    DruidServer server2 = new DruidServer("serverNorm2", "hostNorm2", null, 1000, ServerType.HISTORICAL, "normal", 0);
    for (DataSegment segment : usedSegments) {
      server2.addDataSegment(segment);
    }

    DruidCluster druidCluster = DruidClusterBuilder
        .newBuilder()
        .addTier("hot", new ServerHolder(server1.toImmutableDruidServer(), mockPeon))
        .addTier("normal", new ServerHolder(server2.toImmutableDruidServer(), mockPeon))
        .build();

    SegmentReplicantLookup segmentReplicantLookup = SegmentReplicantLookup.make(druidCluster, false);

    ListeningExecutorService exec = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1));
    BalancerStrategy balancerStrategy = new CostBalancerStrategyFactory().createBalancerStrategy(exec);

    DruidCoordinatorRuntimeParams params = createCoordinatorRuntimeParams(druidCluster)
        .withDynamicConfigs(COORDINATOR_CONFIG_WITH_ZERO_LEADING_TIME_BEFORE_CAN_MARK_AS_UNUSED_OVERSHADOWED_SEGMENTS)
        .withSegmentReplicantLookup(segmentReplicantLookup)
        .withBalancerStrategy(balancerStrategy)
        .withLoadRuleMode(loadRuleMode)
        .build();

    DruidCoordinatorRuntimeParams afterParams = ruleRunner.run(params);
    Assert.assertNotNull(afterParams);
    CoordinatorStats stats = afterParams.getCoordinatorStats();

    Assert.assertEquals(0L, stats.getTieredStat("droppedCount", "hot"));
    Assert.assertEquals(0L, stats.getTieredStat("droppedCount", "normal"));
    Assert.assertEquals(12L, stats.getGlobalStat("deletedCount"));

    exec.shutdown();
    EasyMock.verify(mockPeon);
  }

  /**
   * Test that a LoadRule will match for 1 used segment and that used segment will be dropped from the one server it
   * lives on.
   *
   * Rule 1: T0 - T1 0 replicant on normal tier
   *
   * Servers: 3 servers on normal tier
   *
   * Segments: 24 hourly segments covering from T0 to T0 (next day). Server1 has segment from T0-T1 loaded. Server2
   * has segment from T1-T2 loaded. Server3 has segment from T1-T2 loaded and segment from T2-T3 loaded.
   *
   * Expected Results For Each LoadRuleMode:
   *
   * ALL - 1 segment dropped.
   * PRIMARY_ONLY - 0 segment dropped.
   * REPLICA_ONLY - 1 segment dropped.
   */
  @Test
  public void testDropServerActuallyServesSegment()
  {
    mockCoordinator();
    EasyMock
        .expect(databaseRuleManager.getRulesWithDefault(EasyMock.anyObject()))
        .andReturn(
            Collections.singletonList(
                new IntervalLoadRule(
                    Intervals.of("2012-01-01T00:00:00.000Z/2012-01-01T01:00:00.000Z"),
                    ImmutableMap.of("normal", 0)
                )
            )
        )
        .atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    DruidServer server1 = new DruidServer("server1", "host1", null, 1000, ServerType.HISTORICAL, "normal", 0);
    server1.addDataSegment(usedSegments.get(0));
    DruidServer server2 = new DruidServer("serverNorm2", "hostNorm2", null, 1000, ServerType.HISTORICAL, "normal", 0);
    server2.addDataSegment(usedSegments.get(1));
    DruidServer server3 = new DruidServer("serverNorm3", "hostNorm3", null, 1000, ServerType.HISTORICAL, "normal", 0);
    server3.addDataSegment(usedSegments.get(1));
    server3.addDataSegment(usedSegments.get(2));

    if (!loadRuleMode.equals(LoadRule.LoadRuleMode.PRIMARY_ONLY)) {
      mockPeon.dropSegment(EasyMock.anyObject(), EasyMock.anyObject());
      EasyMock.expectLastCall().atLeastOnce();
    }
    mockEmptyPeon();

    LoadQueuePeon anotherMockPeon = EasyMock.createMock(LoadQueuePeon.class);
    EasyMock.expect(anotherMockPeon.getLoadQueueSize()).andReturn(10L).atLeastOnce();
    EasyMock.expect(anotherMockPeon.getSegmentsToLoad()).andReturn(new HashSet<>()).anyTimes();

    EasyMock.replay(anotherMockPeon);

    DruidCluster druidCluster = DruidClusterBuilder
        .newBuilder()
        .addTier(
            "normal",
            new ServerHolder(server1.toImmutableDruidServer(), mockPeon, false),
            new ServerHolder(server2.toImmutableDruidServer(), anotherMockPeon, false),
            new ServerHolder(server3.toImmutableDruidServer(), anotherMockPeon, false)
        )
        .build();

    SegmentReplicantLookup segmentReplicantLookup = SegmentReplicantLookup.make(druidCluster, false);

    ListeningExecutorService exec = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1));
    BalancerStrategy balancerStrategy = new CostBalancerStrategyFactory().createBalancerStrategy(exec);

    DruidCoordinatorRuntimeParams params = CoordinatorRuntimeParamsTestHelpers
        .newBuilder()
        .withDruidCluster(druidCluster)
        .withDynamicConfigs(COORDINATOR_CONFIG_WITH_ZERO_LEADING_TIME_BEFORE_CAN_MARK_AS_UNUSED_OVERSHADOWED_SEGMENTS)
        .withUsedSegmentsInTest(usedSegments)
        .withDatabaseRuleManager(databaseRuleManager)
        .withSegmentReplicantLookup(segmentReplicantLookup)
        .withBalancerStrategy(balancerStrategy)
        .withLoadRuleMode(loadRuleMode)
        .build();

    DruidCoordinatorRuntimeParams afterParams = ruleRunner.run(params);
    Assert.assertNotNull(afterParams);
    CoordinatorStats stats = afterParams.getCoordinatorStats();

    if (loadRuleMode.equals(LoadRule.LoadRuleMode.ALL) || loadRuleMode.equals(LoadRule.LoadRuleMode.REPLICA_ONLY)) {
      Assert.assertEquals(1L, stats.getTieredStat("droppedCount", "normal"));
    } else {
      Assert.assertEquals(0L, stats.getTieredStat("droppedCount", "normal"));
    }

    exec.shutdown();
    EasyMock.verify(mockPeon);
    EasyMock.verify(anotherMockPeon);
  }

  /**
   * Test that replicant throttle works when it is hit applying LoadRule. Throttle is set to 24 and no new replicants
   * should be assigned once that number is hit.
   *
   * Rule 1: T0 - T0 (next day) 2 replicants on hot tier
   *
   * Servers: 2 servers on hot tier.
   *
   * Segments: 24 hourly segments covering from T0 to T0 (next day), plus one overflow segment that also matches the
   * load rule. Segments not loaded anywhere before test.
   *
   * Expected Results For Each LoadRuleMode:
   *
   * ALL - 48 segments assigned on first call to run(). 0 assigned on second call to run()
   * PRIMARY_ONLY - 24 segments assigned on first call to run(). 0 assigned on second call to run()
   * REPLICA_ONLY - 0 segments assigned on either call to run()
   */
  @Test
  public void testReplicantThrottle()
  {
    mockCoordinator();
    // REPLICA_ONLY mode doesn't load any segments here.
    if (!loadRuleMode.equals(LoadRule.LoadRuleMode.REPLICA_ONLY)) {
      mockPeon.loadSegment(EasyMock.anyObject(), EasyMock.anyObject());
      EasyMock.expectLastCall().atLeastOnce();
    }
    mockEmptyPeon();

    EasyMock
        .expect(databaseRuleManager.getRulesWithDefault(EasyMock.anyObject()))
        .andReturn(
            Collections.singletonList(
                new IntervalLoadRule(
                    Intervals.of("2012-01-01T00:00:00.000Z/2013-01-01T00:00:00.000Z"),
                    ImmutableMap.of("hot", 2)
                )
            )
        )
        .atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    DruidCluster druidCluster = DruidClusterBuilder
        .newBuilder()
        .addTier(
            "hot",
            new ServerHolder(
                new DruidServer("serverHot", "hostHot", null, 1000, ServerType.HISTORICAL, "hot", 0)
                    .toImmutableDruidServer(),
                mockPeon
            ),
            new ServerHolder(
                new DruidServer("serverHot2", "hostHot2", null, 1000, ServerType.HISTORICAL, "hot", 0)
                    .toImmutableDruidServer(),
                mockPeon
            )
        )
        .build();

    ListeningExecutorService exec = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1));
    BalancerStrategy balancerStrategy = new CostBalancerStrategyFactory().createBalancerStrategy(exec);

    DruidCoordinatorRuntimeParams params = makeCoordinatorRuntimeParams(druidCluster, balancerStrategy).build();

    DruidCoordinatorRuntimeParams afterParams = ruleRunner.run(params);
    Assert.assertNotNull(afterParams);
    CoordinatorStats stats = afterParams.getCoordinatorStats();

    if (loadRuleMode.equals(LoadRule.LoadRuleMode.ALL)) {
      Assert.assertEquals(48L, stats.getTieredStat("assignedCount", "hot"));
    } else if (loadRuleMode.equals(LoadRule.LoadRuleMode.PRIMARY_ONLY)) {
      Assert.assertEquals(24L, stats.getTieredStat("assignedCount", "hot"));
    } else {
      Assert.assertEquals(0L, stats.getTieredStat("assignedCount", "hot"));
    }
    Assert.assertTrue(stats.getTiers("unassignedCount").isEmpty());
    Assert.assertTrue(stats.getTiers("unassignedSize").isEmpty());

    DataSegment overFlowSegment = new DataSegment(
        "test",
        Intervals.of("2012-02-01/2012-02-02"),
        DateTimes.nowUtc().toString(),
        new HashMap<>(),
        new ArrayList<>(),
        new ArrayList<>(),
        NoneShardSpec.instance(),
        1,
        0
    );

    afterParams = ruleRunner.run(
        CoordinatorRuntimeParamsTestHelpers
            .newBuilder()
            .withDruidCluster(druidCluster)
            .withEmitter(emitter)
            .withUsedSegmentsInTest(overFlowSegment)
            .withDatabaseRuleManager(databaseRuleManager)
            .withBalancerStrategy(balancerStrategy)
            .withSegmentReplicantLookup(SegmentReplicantLookup.make(new DruidCluster(), false))
            .withLoadRuleMode(loadRuleMode)
            .build()
    );
    Assert.assertNotNull(afterParams);
    stats = afterParams.getCoordinatorStats();

    if (loadRuleMode.equals(LoadRule.LoadRuleMode.ALL) || loadRuleMode.equals(LoadRule.LoadRuleMode.PRIMARY_ONLY)) {
      Assert.assertEquals(1L, stats.getTieredStat("assignedCount", "hot"));
    } else {
      Assert.assertEquals(0L, stats.getTieredStat("assignedCount", "hot"));
    }
    Assert.assertTrue(stats.getTiers("unassignedCount").isEmpty());
    Assert.assertTrue(stats.getTiers("unassignedSize").isEmpty());

    EasyMock.verify(mockPeon);
    exec.shutdown();
  }

  /**
   * Test that replicant throttle works when it is hit applying LoadRule across tiers. Throttle is set to 7
   *
   * Rule 1: T0 - T0 (next day) 1 replicant on hot tier, 1 replicant on DruidServer.DEFAULT_TIER
   *
   * Servers: 1 server on hot tier, 1 server on DruidServer.DEFAULT_TIER
   *
   * Segments: 24 hourly segments covering from T0 to T0 (next day). Segments not loaded anywhere before test
   *
   * Expected Results For Each LoadRuleMode:
   *
   * ALL - 31 segments assigned (replication throttle hit)
   * PRIMARY_ONLY - 24 segments assigned (No throttle because we only loaded primary)
   * REPLICA_ONLY - 0 segments assigned
   */
  @Test
  public void testReplicantThrottleAcrossTiers()
  {
    EasyMock
        .expect(coordinator.getDynamicConfigs())
        .andReturn(
            CoordinatorDynamicConfig.builder()
                                    .withReplicationThrottleLimit(7)
                                    .withReplicantLifetime(1)
                                    .withMaxSegmentsInNodeLoadingQueue(1000)
                                    .build()
        )
        .atLeastOnce();
    coordinator.markSegmentAsUnused(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.replay(coordinator);
    if (!loadRuleMode.equals(LoadRule.LoadRuleMode.REPLICA_ONLY)) {
      mockPeon.loadSegment(EasyMock.anyObject(), EasyMock.anyObject());
      EasyMock.expectLastCall().atLeastOnce();
    }
    mockEmptyPeon();

    EasyMock
        .expect(databaseRuleManager.getRulesWithDefault(EasyMock.anyObject()))
        .andReturn(
            Collections.singletonList(new IntervalLoadRule(
                    Intervals.of("2012-01-01T00:00:00.000Z/2013-01-01T00:00:00.000Z"),
                    ImmutableMap.of(
                        "hot", 1,
                        DruidServer.DEFAULT_TIER, 1
                    )
                )
            )
        )
        .atLeastOnce();
    EasyMock.replay(databaseRuleManager);

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

    ListeningExecutorService exec = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1));
    BalancerStrategy balancerStrategy = new CostBalancerStrategyFactory().createBalancerStrategy(exec);

    DruidCoordinatorRuntimeParams params = makeCoordinatorRuntimeParams(druidCluster, balancerStrategy).build();

    RunRules runner = new RunRules(new ReplicationThrottler(7, 1, false), coordinator);
    DruidCoordinatorRuntimeParams afterParams = runner.run(params);
    Assert.assertNotNull(afterParams);
    CoordinatorStats stats = afterParams.getCoordinatorStats();

    if (loadRuleMode.equals(LoadRule.LoadRuleMode.ALL)) {
      Assert.assertEquals(24L, stats.getTieredStat("assignedCount", "hot"));
      Assert.assertEquals(7L, stats.getTieredStat("assignedCount", DruidServer.DEFAULT_TIER));
    } else if (loadRuleMode.equals(LoadRule.LoadRuleMode.PRIMARY_ONLY)) {
      Assert.assertEquals(24L, stats.getTieredStat("assignedCount", "hot"));
      Assert.assertEquals(0L, stats.getTieredStat("assignedCount", DruidServer.DEFAULT_TIER));
    } else {
      Assert.assertEquals(0L, stats.getTieredStat("assignedCount", "hot"));
      Assert.assertEquals(0L, stats.getTieredStat("assignedCount", DruidServer.DEFAULT_TIER));
    }
    Assert.assertTrue(stats.getTiers("unassignedCount").isEmpty());
    Assert.assertTrue(stats.getTiers("unassignedSize").isEmpty());

    EasyMock.verify(mockPeon);
    exec.shutdown();
  }

  /**
   * Test that replicant throttle does not apply when dropping segments
   *
   * Rule 1: T0 - T0 (next day) 1 replicant on normal tier
   *
   * Servers: 2 segments on normal tier
   *
   * Segments: 24 hourly segments covering from T0 to T0 (next day). One extra overflow segment that also
   * matches load rule too. All segments loaded on both servers
   *
   * Expected Results For Each LoadRuleMode:
   *
   * ALL - 25 segments dropped
   * PRIMARY_ONLY - 0 segments dropped
   * REPLICA_ONLY - 25 segments dropped
   */
  @Test
  public void testDropReplicantThrottle()
  {
    mockCoordinator();
    if (!loadRuleMode.equals(LoadRule.LoadRuleMode.PRIMARY_ONLY)) {
      mockPeon.dropSegment(EasyMock.anyObject(), EasyMock.anyObject());
      EasyMock.expectLastCall().atLeastOnce();
    }
    mockEmptyPeon();

    EasyMock
        .expect(databaseRuleManager.getRulesWithDefault(EasyMock.anyObject()))
        .andReturn(
            Collections.singletonList(new IntervalLoadRule(
                    Intervals.of("2012-01-01T00:00:00.000Z/2013-01-02T00:00:00.000Z"),
                    ImmutableMap.of("normal", 1)
                )
            )
        )
        .atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    DataSegment overFlowSegment = new DataSegment(
        "test",
        Intervals.of("2012-02-01/2012-02-02"),
        DateTimes.nowUtc().toString(),
        new HashMap<>(),
        new ArrayList<>(),
        new ArrayList<>(),
        NoneShardSpec.instance(),
        1,
        0
    );
    List<DataSegment> longerUsedSegments = Lists.newArrayList(usedSegments);
    longerUsedSegments.add(overFlowSegment);

    DruidServer server1 = new DruidServer("serverNorm1", "hostNorm1", null, 1000, ServerType.HISTORICAL, "normal", 0);
    for (DataSegment segment : longerUsedSegments) {
      server1.addDataSegment(segment);
    }
    DruidServer server2 = new DruidServer("serverNorm2", "hostNorm2", null, 1000, ServerType.HISTORICAL, "normal", 0);
    for (DataSegment segment : longerUsedSegments) {
      server2.addDataSegment(segment);
    }

    DruidCluster druidCluster = DruidClusterBuilder
        .newBuilder()
        .addTier(
            "normal",
            new ServerHolder(server1.toImmutableDruidServer(), mockPeon),
            new ServerHolder(server2.toImmutableDruidServer(), mockPeon)
        )
        .build();

    SegmentReplicantLookup segmentReplicantLookup = SegmentReplicantLookup.make(druidCluster, false);

    ListeningExecutorService exec = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1));
    BalancerStrategy balancerStrategy = new CostBalancerStrategyFactory().createBalancerStrategy(exec);

    DruidCoordinatorRuntimeParams params = CoordinatorRuntimeParamsTestHelpers
        .newBuilder()
        .withDruidCluster(druidCluster)
        .withDynamicConfigs(COORDINATOR_CONFIG_WITH_ZERO_LEADING_TIME_BEFORE_CAN_MARK_AS_UNUSED_OVERSHADOWED_SEGMENTS)
        .withUsedSegmentsInTest(longerUsedSegments)
        .withDatabaseRuleManager(databaseRuleManager)
        .withSegmentReplicantLookup(segmentReplicantLookup)
        .withBalancerStrategy(balancerStrategy)
        .withLoadRuleMode(loadRuleMode)
        .build();

    DruidCoordinatorRuntimeParams afterParams = ruleRunner.run(params);
    Assert.assertNotNull(afterParams);
    CoordinatorStats stats = afterParams.getCoordinatorStats();

    if (loadRuleMode.equals(LoadRule.LoadRuleMode.ALL) || loadRuleMode.equals(LoadRule.LoadRuleMode.REPLICA_ONLY)) {
      // There is no throttling on drop
      Assert.assertEquals(25L, stats.getTieredStat("droppedCount", "normal"));
    } else {
      Assert.assertEquals(0L, stats.getTieredStat("droppedCount", "normal"));
    }

    EasyMock.verify(mockPeon);
    exec.shutdown();
  }

  /**
   * Test basica segment assign using 2 segments that have same interval and different version
   *
   * Rule 1: load forever, 1 replicant
   *
   * Servers: 1 server
   *
   * Segments: 2 segments, each with same interval but different version. Not loaded before test.
   *
   * Expected Results For Each LoadRuleMode:
   *
   * ALL - 1 replicant loaded
   * PRIMARY_ONLY - 1 replicant loaded
   * REPLICA_ONLY - 0 replicants loaded
   */
  @Test
  public void testRulesRunOnNonOvershadowedSegmentsOnly()
  {
    Set<DataSegment> usedSegments = new HashSet<>();
    DataSegment v1 = new DataSegment(
        "test",
        Intervals.of("2012-01-01/2012-01-02"),
        "1",
        new HashMap<>(),
        new ArrayList<>(),
        new ArrayList<>(),
        NoneShardSpec.instance(),
        IndexIO.CURRENT_VERSION_ID,
        1
    );
    DataSegment v2 = new DataSegment(
        "test",
        Intervals.of("2012-01-01/2012-01-02"),
        "2",
        new HashMap<>(),
        new ArrayList<>(),
        new ArrayList<>(),
        NoneShardSpec.instance(),
        IndexIO.CURRENT_VERSION_ID,
        1
    );
    usedSegments.add(v1);
    usedSegments.add(v2);

    mockCoordinator();
    if (!loadRuleMode.equals(LoadRule.LoadRuleMode.REPLICA_ONLY)) {
      mockPeon.loadSegment(EasyMock.eq(v2), EasyMock.anyObject());
      EasyMock.expectLastCall().once();
    }
    mockEmptyPeon();

    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.anyObject())).andReturn(
        Collections.singletonList(new ForeverLoadRule(ImmutableMap.of(DruidServer.DEFAULT_TIER, 1)))).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    DruidCluster druidCluster = DruidClusterBuilder
        .newBuilder()
        .addTier(
            DruidServer.DEFAULT_TIER,
            new ServerHolder(
                new DruidServer(
                    "serverHot",
                    "hostHot",
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

    ListeningExecutorService exec = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1));
    BalancerStrategy balancerStrategy = new CostBalancerStrategyFactory().createBalancerStrategy(exec);

    DruidCoordinatorRuntimeParams params = CoordinatorRuntimeParamsTestHelpers
        .newBuilder()
        .withDruidCluster(druidCluster)
        .withUsedSegmentsInTest(usedSegments)
        .withDatabaseRuleManager(databaseRuleManager)
        .withSegmentReplicantLookup(SegmentReplicantLookup.make(new DruidCluster(), false))
        .withBalancerStrategy(balancerStrategy)
        .withDynamicConfigs(CoordinatorDynamicConfig.builder().withMaxSegmentsToMove(5).build())
        .withLoadRuleMode(loadRuleMode)
        .build();

    DruidCoordinatorRuntimeParams afterParams = ruleRunner.run(params);
    Assert.assertNotNull(afterParams);
    CoordinatorStats stats = afterParams.getCoordinatorStats();

    if (loadRuleMode.equals(LoadRule.LoadRuleMode.ALL) || loadRuleMode.equals(LoadRule.LoadRuleMode.PRIMARY_ONLY)) {
      Assert.assertEquals(1, stats.getTieredStat("assignedCount", "_default_tier"));
    } else {
      Assert.assertEquals(0, stats.getTieredStat("assignedCount", "_default_tier"));
    }
    Assert.assertTrue(stats.getTiers("unassignedCount").isEmpty());
    Assert.assertTrue(stats.getTiers("unassignedSize").isEmpty());

    Assert.assertEquals(2, usedSegments.size());
    Assert.assertEquals(usedSegments, params.getUsedSegments());
    Assert.assertEquals(usedSegments, afterParams.getUsedSegments());

    EasyMock.verify(mockPeon);
    exec.shutdown();
  }

  /**
   * Test that no assign happens if there is nowhere to assign it.
   *
   * Rule 1: LoadForever 3 replicants in DruidServer.DEFAULT_TIER
   *
   * Servers: 2 servers in DruidSerer.DEFAULT_TIER
   *
   * Segments: One segment in test datasource
   *
   * Expected Results For Each LoadRuleMode:
   *
   * ALL - 0 replicants loaded
   * PRIMARY_ONLY - 0 replicants loaded
   * REPLICA_ONLY - 0 replicants loaded
   */
  @Test(timeout = 5000L)
  public void testTwoNodesOneTierThreeReplicantsRandomStrategyNotEnoughNodes()
  {
    mockCoordinator();
    mockEmptyPeon();

    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.anyObject())).andReturn(
        Collections.singletonList(
            new ForeverLoadRule(
                ImmutableMap.of(DruidServer.DEFAULT_TIER, 3)
            )
        )).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    DataSegment dataSegment = new DataSegment(
        "test",
        Intervals.utc(0, 1),
        DateTimes.nowUtc().toString(),
        new HashMap<>(),
        new ArrayList<>(),
        new ArrayList<>(),
        NoneShardSpec.instance(),
        IndexIO.CURRENT_VERSION_ID,
        1
    );

    DruidCluster druidCluster = DruidClusterBuilder
        .newBuilder()
        .addTier(
            DruidServer.DEFAULT_TIER,
            new ServerHolder(
                new DruidServer(
                    "server1",
                    "host1",
                    null,
                    1000,
                    ServerType.HISTORICAL,
                    DruidServer.DEFAULT_TIER,
                    0
                ).addDataSegment(dataSegment)
                 .toImmutableDruidServer(),
                mockPeon
            ),
            new ServerHolder(
                new DruidServer(
                    "server2",
                    "host2",
                    null,
                    1000,
                    ServerType.HISTORICAL,
                    DruidServer.DEFAULT_TIER,
                    0
                ).addDataSegment(dataSegment)
                 .toImmutableDruidServer(),
                mockPeon
            )
        )
        .build();

    RandomBalancerStrategy balancerStrategy = new RandomBalancerStrategy();

    DruidCoordinatorRuntimeParams params = makeCoordinatorRuntimeParams(
        druidCluster,
        balancerStrategy,
        Collections.singletonList(dataSegment)
    )
        .withDynamicConfigs(CoordinatorDynamicConfig.builder().withMaxSegmentsToMove(5).build())
        .build();

    DruidCoordinatorRuntimeParams afterParams = ruleRunner.run(params);
    Assert.assertNotNull(afterParams);
    CoordinatorStats stats = afterParams.getCoordinatorStats();

    Assert.assertEquals(0L, stats.getTieredStat("assignedCount", DruidServer.DEFAULT_TIER));
    Assert.assertTrue(stats.getTiers("unassignedCount").isEmpty());
    Assert.assertTrue(stats.getTiers("unassignedSize").isEmpty());

    EasyMock.verify(mockPeon);
  }


  /**
   * Test basic segment assign.
   *
   * Rule 1: LoadForever 1 replicants in DruidServer.DEFAULT_TIER
   *
   * Servers: 1 server in DruidSerer.DEFAULT_TIER
   *
   * Segments: One segment in test datasource
   *
   * Expected Results For Each LoadRuleMode:
   *
   * ALL - 1 replicants loaded
   * PRIMARY_ONLY - 1 replicants loaded
   * REPLICA_ONLY - 0 replicants loaded
   */
  @Test(timeout = 5000L)
  public void testOneNodesOneTierOneReplicantRandomStrategyEnoughSpace()
  {
    mockCoordinator();
    if (!loadRuleMode.equals(LoadRule.LoadRuleMode.REPLICA_ONLY)) {
      mockPeon.loadSegment(EasyMock.anyObject(), EasyMock.anyObject());
      EasyMock.expectLastCall().atLeastOnce();
    }
    mockEmptyPeon();

    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.anyObject())).andReturn(
        Collections.singletonList(
            new ForeverLoadRule(
                ImmutableMap.of(DruidServer.DEFAULT_TIER, 1)
            )
        )).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    DataSegment dataSegment = new DataSegment(
        "test",
        Intervals.utc(0, 1),
        DateTimes.nowUtc().toString(),
        new HashMap<>(),
        new ArrayList<>(),
        new ArrayList<>(),
        NoneShardSpec.instance(),
        IndexIO.CURRENT_VERSION_ID,
        1
    );

    DruidCluster druidCluster = DruidClusterBuilder
        .newBuilder()
        .addTier(
            DruidServer.DEFAULT_TIER,
            new ServerHolder(
                new DruidServer("server1", "host1", null, 1000, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 0)
                    .toImmutableDruidServer(),
                mockPeon
            )
        )
        .build();

    RandomBalancerStrategy balancerStrategy = new RandomBalancerStrategy();

    DruidCoordinatorRuntimeParams params = makeCoordinatorRuntimeParams(
        druidCluster,
        balancerStrategy,
        Collections.singletonList(dataSegment)
    )
        .withDynamicConfigs(CoordinatorDynamicConfig.builder().withMaxSegmentsToMove(5).build())
        .build();

    DruidCoordinatorRuntimeParams afterParams = ruleRunner.run(params);
    Assert.assertNotNull(afterParams);
    CoordinatorStats stats = afterParams.getCoordinatorStats();
    if (loadRuleMode.equals(LoadRule.LoadRuleMode.ALL) || loadRuleMode.equals(LoadRule.LoadRuleMode.PRIMARY_ONLY)) {
      Assert.assertEquals(1L, stats.getTieredStat("assignedCount", DruidServer.DEFAULT_TIER));
    } else {
      Assert.assertEquals(0L, stats.getTieredStat("assignedCount", DruidServer.DEFAULT_TIER));
    }
    Assert.assertTrue(stats.getTiers("unassignedCount").isEmpty());
    Assert.assertTrue(stats.getTiers("unassignedSize").isEmpty());

    EasyMock.verify(mockPeon);
  }

  /**
   * Test no assign because not enough space. random balancer.
   *
   * Rule 1: LoadForever 1 replicants in DruidServer.DEFAULT_TIER
   *
   * Servers: 1 server in DruidSerer.DEFAULT_TIER
   *
   * Segments: One segment in test datasource
   *
   * Expected Results For Each LoadRuleMode:
   *
   * ALL - 0 replicants loaded
   * PRIMARY_ONLY - 0 replicants loaded
   * REPLICA_ONLY - 0 replicants loaded
   */
  @Test(timeout = 5000L)
  public void testOneNodesOneTierOneReplicantRandomStrategyNotEnoughSpace()
  {
    mockCoordinator();
    mockEmptyPeon();
    int numReplicants = 1;
    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.anyObject())).andReturn(
        Collections.singletonList(
            new ForeverLoadRule(
                ImmutableMap.of(DruidServer.DEFAULT_TIER, numReplicants)
            )
        )).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    DataSegment dataSegment = new DataSegment(
        "test",
        Intervals.utc(0, 1),
        DateTimes.nowUtc().toString(),
        new HashMap<>(),
        new ArrayList<>(),
        new ArrayList<>(),
        NoneShardSpec.instance(),
        IndexIO.CURRENT_VERSION_ID,
        11
    );

    DruidCluster druidCluster = DruidClusterBuilder
        .newBuilder()
        .addTier(
            DruidServer.DEFAULT_TIER,
            new ServerHolder(
                new DruidServer("server1", "host1", null, 10, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 0)
                    .toImmutableDruidServer(),
                mockPeon
            )
        )
        .build();

    RandomBalancerStrategy balancerStrategy = new RandomBalancerStrategy();

    DruidCoordinatorRuntimeParams params = makeCoordinatorRuntimeParams(druidCluster, balancerStrategy, Collections.singletonList(dataSegment))
        .withDynamicConfigs(CoordinatorDynamicConfig.builder().withMaxSegmentsToMove(5).build())
        .build();

    DruidCoordinatorRuntimeParams afterParams = ruleRunner.run(params);
    Assert.assertNotNull(afterParams);
    CoordinatorStats stats = afterParams.getCoordinatorStats();
    Assert.assertEquals(
        dataSegment.getSize() * numReplicants,
        stats.getTieredStat(LoadRule.REQUIRED_CAPACITY, DruidServer.DEFAULT_TIER)
    );
    Assert.assertEquals(0L, stats.getTieredStat("assignedCount", DruidServer.DEFAULT_TIER));
    Assert.assertTrue(stats.getTiers("unassignedCount").isEmpty());
    Assert.assertTrue(stats.getTiers("unassignedSize").isEmpty());

    EasyMock.verify(mockPeon);
  }

  /**
   * Test no assign because not enough space. cost balancer.
   *
   * Rule 1: LoadForever 1 replicants in DruidServer.DEFAULT_TIER
   *
   * Servers: 1 server in DruidSerer.DEFAULT_TIER
   *
   * Segments: One segment in test datasource
   *
   * Expected Results For Each LoadRuleMode:
   *
   * ALL - 0 replicants loaded
   * PRIMARY_ONLY - 0 replicants loaded
   * REPLICA_ONLY - 0 replicants loaded
   */
  @Test
  public void testOneNodesOneTierOneReplicantCostBalancerStrategyNotEnoughSpace()
  {
    mockCoordinator();
    mockEmptyPeon();
    int numReplicants = 1;
    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.anyObject())).andReturn(
        Collections.singletonList(
            new ForeverLoadRule(
                ImmutableMap.of(DruidServer.DEFAULT_TIER, numReplicants)
            )
        )).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    DataSegment dataSegment = new DataSegment(
        "test",
        Intervals.utc(0, 1),
        DateTimes.nowUtc().toString(),
        new HashMap<>(),
        new ArrayList<>(),
        new ArrayList<>(),
        NoneShardSpec.instance(),
        IndexIO.CURRENT_VERSION_ID,
        11
    );

    DruidCluster druidCluster = DruidClusterBuilder
        .newBuilder()
        .addTier(
            DruidServer.DEFAULT_TIER,
            new ServerHolder(
                new DruidServer("server1", "host1", null, 10, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 0)
                    .toImmutableDruidServer(),
                mockPeon
            )
        )
        .build();

    ListeningExecutorService exec = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1));
    CostBalancerStrategy balancerStrategy = new CostBalancerStrategy(exec);

    DruidCoordinatorRuntimeParams params = makeCoordinatorRuntimeParams(druidCluster, balancerStrategy, Collections.singletonList(dataSegment))
        .withDynamicConfigs(CoordinatorDynamicConfig.builder().withMaxSegmentsToMove(5).build())
        .build();

    DruidCoordinatorRuntimeParams afterParams = ruleRunner.run(params);
    Assert.assertNotNull(afterParams);
    CoordinatorStats stats = afterParams.getCoordinatorStats();
    Assert.assertEquals(
        dataSegment.getSize() * numReplicants,
        stats.getTieredStat(LoadRule.REQUIRED_CAPACITY, DruidServer.DEFAULT_TIER)
    );
    Assert.assertEquals(0L, stats.getTieredStat("assignedCount", DruidServer.DEFAULT_TIER));
    Assert.assertTrue(stats.getTiers("unassignedCount").isEmpty());
    Assert.assertTrue(stats.getTiers("unassignedSize").isEmpty());

    exec.shutdown();
    EasyMock.verify(mockPeon);
  }

  private void mockCoordinator()
  {
    EasyMock.expect(coordinator.getDynamicConfigs()).andReturn(createCoordinatorDynamicConfig()).anyTimes();
    coordinator.markSegmentAsUnused(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.replay(coordinator, segmentsMetadataManager);
  }

  private void mockEmptyPeon()
  {
    EasyMock.expect(mockPeon.getSegmentsToLoad()).andReturn(new HashSet<>()).anyTimes();
    EasyMock.expect(mockPeon.getSegmentsMarkedToDrop()).andReturn(new HashSet<>()).anyTimes();
    EasyMock.expect(mockPeon.getLoadQueueSize()).andReturn(0L).atLeastOnce();
    EasyMock.expect(mockPeon.getNumberOfSegmentsInQueue()).andReturn(0).anyTimes();
    EasyMock.replay(mockPeon);
  }

  private CoordinatorDynamicConfig createCoordinatorDynamicConfig()
  {
    return CoordinatorDynamicConfig.builder()
                                   .withLeadingTimeMillisBeforeCanMarkAsUnusedOvershadowedSegments(0)
                                   .withMergeBytesLimit(0)
                                   .withMergeSegmentsLimit(0)
                                   .withMaxSegmentsToMove(0)
                                   .withReplicantLifetime(1)
                                   .withReplicationThrottleLimit(24)
                                   .withBalancerComputeThreads(0)
                                   .withEmitBalancingStats(false)
                                   .withSpecificDataSourcesToKillUnusedSegmentsIn(null)
                                   .withKillUnusedSegmentsInAllDataSources(false)
                                   .withMaxSegmentsInNodeLoadingQueue(1000)
                                   .withPauseCoordination(false)
                                   .build();
  }
}
