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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;

/**
 */
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

    ruleRunner = new RunRules(new ReplicationThrottler(24, 1), coordinator);
  }

  @After
  public void tearDown()
  {
    EasyMock.verify(coordinator);
    EasyMock.verify(databaseRuleManager);
  }

  /**
   * Nodes:
   * hot - 1 replicant
   * normal - 1 replicant
   * cold - 1 replicant
   */
  @Test
  public void testRunThreeTiersOneReplicant()
  {
    mockCoordinator();
    mockPeon.loadSegment(EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();
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
    CoordinatorStats stats = afterParams.getCoordinatorStats();

    Assert.assertEquals(6L, stats.getTieredStat("assignedCount", "hot"));
    Assert.assertEquals(6L, stats.getTieredStat("assignedCount", "normal"));
    Assert.assertEquals(12L, stats.getTieredStat("assignedCount", "cold"));
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
        .withSegmentReplicantLookup(SegmentReplicantLookup.make(druidCluster))
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
        .withDatabaseRuleManager(databaseRuleManager);
  }

  /**
   * Nodes:
   * hot - 2 replicants
   * cold - 1 replicant
   */
  @Test
  public void testRunTwoTiersTwoReplicants()
  {
    mockCoordinator();
    mockPeon.loadSegment(EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();
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
    CoordinatorStats stats = afterParams.getCoordinatorStats();

    Assert.assertEquals(12L, stats.getTieredStat("assignedCount", "hot"));
    Assert.assertEquals(18L, stats.getTieredStat("assignedCount", "cold"));
    Assert.assertTrue(stats.getTiers("unassignedCount").isEmpty());
    Assert.assertTrue(stats.getTiers("unassignedSize").isEmpty());

    exec.shutdown();
    EasyMock.verify(mockPeon);
  }

  /**
   * Nodes:
   * hot - 1 replicant
   * normal - 1 replicant
   */
  @Test
  public void testRunTwoTiersWithExistingSegments()
  {
    mockCoordinator();
    mockPeon.loadSegment(EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();
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

    SegmentReplicantLookup segmentReplicantLookup = SegmentReplicantLookup.make(druidCluster);

    ListeningExecutorService exec = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1));
    BalancerStrategy balancerStrategy = new CostBalancerStrategyFactory().createBalancerStrategy(exec);

    DruidCoordinatorRuntimeParams params = createCoordinatorRuntimeParams(druidCluster)
        .withSegmentReplicantLookup(segmentReplicantLookup)
        .withBalancerStrategy(balancerStrategy)
        .build();

    DruidCoordinatorRuntimeParams afterParams = ruleRunner.run(params);
    CoordinatorStats stats = afterParams.getCoordinatorStats();

    Assert.assertEquals(12L, stats.getTieredStat("assignedCount", "hot"));
    Assert.assertEquals(0L, stats.getTieredStat("assignedCount", "normal"));
    Assert.assertTrue(stats.getTiers("unassignedCount").isEmpty());
    Assert.assertTrue(stats.getTiers("unassignedSize").isEmpty());

    exec.shutdown();
    EasyMock.verify(mockPeon);
  }

  @Test
  public void testRunTwoTiersTierDoesNotExist()
  {
    mockCoordinator();
    mockPeon.loadSegment(EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    mockEmptyPeon();

    emitter.emit(EasyMock.<ServiceEventBuilder>anyObject());
    EasyMock.expectLastCall().times(12);
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

    ruleRunner.run(params);

    exec.shutdown();
    EasyMock.verify(emitter);
    EasyMock.verify(mockPeon);
  }

  @Test
  public void testRunRuleDoesNotExist()
  {
    mockCoordinator();
    emitter.emit(EasyMock.<ServiceEventBuilder>anyObject());
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
        .withSegmentReplicantLookup(SegmentReplicantLookup.make(new DruidCluster()))
        .withEmitter(emitter)
        .build();

    ruleRunner.run(params);

    EasyMock.verify(emitter, mockPeon);
  }

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

    SegmentReplicantLookup segmentReplicantLookup = SegmentReplicantLookup.make(druidCluster);

    ListeningExecutorService exec = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1));
    BalancerStrategy balancerStrategy = new CostBalancerStrategyFactory().createBalancerStrategy(exec);

    DruidCoordinatorRuntimeParams params = createCoordinatorRuntimeParams(druidCluster)
        .withDynamicConfigs(COORDINATOR_CONFIG_WITH_ZERO_LEADING_TIME_BEFORE_CAN_MARK_AS_UNUSED_OVERSHADOWED_SEGMENTS)
        .withSegmentReplicantLookup(segmentReplicantLookup)
        .withBalancerStrategy(balancerStrategy)
        .build();

    DruidCoordinatorRuntimeParams afterParams = ruleRunner.run(params);
    CoordinatorStats stats = afterParams.getCoordinatorStats();

    Assert.assertEquals(12L, stats.getGlobalStat("deletedCount"));

    exec.shutdown();
    EasyMock.verify(coordinator);
  }

  @Test
  public void testDropTooManyInSameTier()
  {
    mockCoordinator();
    mockPeon.dropSegment(EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();
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

    SegmentReplicantLookup segmentReplicantLookup = SegmentReplicantLookup.make(druidCluster);

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
    CoordinatorStats stats = afterParams.getCoordinatorStats();

    Assert.assertEquals(1L, stats.getTieredStat("droppedCount", "normal"));
    Assert.assertEquals(12L, stats.getGlobalStat("deletedCount"));

    exec.shutdown();
    EasyMock.verify(mockPeon);
  }

  @Test
  public void testDropTooManyInDifferentTiers()
  {
    mockCoordinator();
    mockPeon.loadSegment(EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    mockPeon.dropSegment(EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();
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

    SegmentReplicantLookup segmentReplicantLookup = SegmentReplicantLookup.make(druidCluster);

    ListeningExecutorService exec = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1));
    BalancerStrategy balancerStrategy = new CostBalancerStrategyFactory().createBalancerStrategy(exec);

    DruidCoordinatorRuntimeParams params = createCoordinatorRuntimeParams(druidCluster)
        .withDynamicConfigs(COORDINATOR_CONFIG_WITH_ZERO_LEADING_TIME_BEFORE_CAN_MARK_AS_UNUSED_OVERSHADOWED_SEGMENTS)
        .withSegmentReplicantLookup(segmentReplicantLookup)
        .withBalancerStrategy(balancerStrategy)
        .build();

    DruidCoordinatorRuntimeParams afterParams = ruleRunner.run(params);
    CoordinatorStats stats = afterParams.getCoordinatorStats();

    Assert.assertEquals(1L, stats.getTieredStat("droppedCount", "normal"));
    Assert.assertEquals(12L, stats.getGlobalStat("deletedCount"));

    exec.shutdown();
    EasyMock.verify(mockPeon);
  }

  @Test
  public void testDontDropInDifferentTiers()
  {
    mockCoordinator();
    mockPeon.loadSegment(EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();
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

    SegmentReplicantLookup segmentReplicantLookup = SegmentReplicantLookup.make(druidCluster);

    ListeningExecutorService exec = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1));
    BalancerStrategy balancerStrategy = new CostBalancerStrategyFactory().createBalancerStrategy(exec);

    DruidCoordinatorRuntimeParams params = createCoordinatorRuntimeParams(druidCluster)
        .withDynamicConfigs(COORDINATOR_CONFIG_WITH_ZERO_LEADING_TIME_BEFORE_CAN_MARK_AS_UNUSED_OVERSHADOWED_SEGMENTS)
        .withSegmentReplicantLookup(segmentReplicantLookup)
        .withBalancerStrategy(balancerStrategy)
        .build();

    DruidCoordinatorRuntimeParams afterParams = ruleRunner.run(params);
    CoordinatorStats stats = afterParams.getCoordinatorStats();

    Assert.assertTrue(stats.getTiers("droppedCount").isEmpty());
    Assert.assertEquals(12L, stats.getGlobalStat("deletedCount"));

    exec.shutdown();
    EasyMock.verify(mockPeon);
  }

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

    mockPeon.dropSegment(EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();
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

    SegmentReplicantLookup segmentReplicantLookup = SegmentReplicantLookup.make(druidCluster);

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
        .build();

    DruidCoordinatorRuntimeParams afterParams = ruleRunner.run(params);
    CoordinatorStats stats = afterParams.getCoordinatorStats();

    Assert.assertEquals(1L, stats.getTieredStat("droppedCount", "normal"));

    exec.shutdown();
    EasyMock.verify(mockPeon);
    EasyMock.verify(anotherMockPeon);
  }

  /**
   * Nodes:
   * hot - 2 replicants
   */
  @Test
  public void testReplicantThrottle()
  {
    mockCoordinator();
    mockPeon.loadSegment(EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();
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
    CoordinatorStats stats = afterParams.getCoordinatorStats();

    Assert.assertEquals(48L, stats.getTieredStat("assignedCount", "hot"));
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
            .withSegmentReplicantLookup(SegmentReplicantLookup.make(new DruidCluster()))
            .build()
    );
    stats = afterParams.getCoordinatorStats();

    Assert.assertEquals(1L, stats.getTieredStat("assignedCount", "hot"));
    Assert.assertTrue(stats.getTiers("unassignedCount").isEmpty());
    Assert.assertTrue(stats.getTiers("unassignedSize").isEmpty());

    EasyMock.verify(mockPeon);
    exec.shutdown();
  }

  /**
   * Nodes:
   * hot - nothing loaded
   * _default_tier - 1 segment loaded
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
    mockPeon.loadSegment(EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();
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

    RunRules runner = new RunRules(new ReplicationThrottler(7, 1), coordinator);
    DruidCoordinatorRuntimeParams afterParams = runner.run(params);
    CoordinatorStats stats = afterParams.getCoordinatorStats();

    Assert.assertEquals(24L, stats.getTieredStat("assignedCount", "hot"));
    Assert.assertEquals(7L, stats.getTieredStat("assignedCount", DruidServer.DEFAULT_TIER));
    Assert.assertTrue(stats.getTiers("unassignedCount").isEmpty());
    Assert.assertTrue(stats.getTiers("unassignedSize").isEmpty());

    EasyMock.verify(mockPeon);
    exec.shutdown();
  }

  @Test
  public void testDropReplicantThrottle()
  {
    mockCoordinator();
    mockPeon.dropSegment(EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();
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

    SegmentReplicantLookup segmentReplicantLookup = SegmentReplicantLookup.make(druidCluster);

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
        .build();

    DruidCoordinatorRuntimeParams afterParams = ruleRunner.run(params);
    CoordinatorStats stats = afterParams.getCoordinatorStats();

    // There is no throttling on drop
    Assert.assertEquals(25L, stats.getTieredStat("droppedCount", "normal"));
    EasyMock.verify(mockPeon);
    exec.shutdown();
  }

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
    mockPeon.loadSegment(EasyMock.eq(v2), EasyMock.anyObject());
    EasyMock.expectLastCall().once();
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
        .withSegmentReplicantLookup(SegmentReplicantLookup.make(new DruidCluster()))
        .withBalancerStrategy(balancerStrategy)
        .withDynamicConfigs(CoordinatorDynamicConfig.builder().withMaxSegmentsToMove(5).build())
        .build();

    DruidCoordinatorRuntimeParams afterParams = ruleRunner.run(params);
    CoordinatorStats stats = afterParams.getCoordinatorStats();

    Assert.assertEquals(1, stats.getTiers("assignedCount").size());
    Assert.assertEquals(1, stats.getTieredStat("assignedCount", "_default_tier"));
    Assert.assertTrue(stats.getTiers("unassignedCount").isEmpty());
    Assert.assertTrue(stats.getTiers("unassignedSize").isEmpty());

    Assert.assertEquals(2, usedSegments.size());
    Assert.assertEquals(usedSegments, params.getUsedSegments());
    Assert.assertEquals(usedSegments, afterParams.getUsedSegments());

    EasyMock.verify(mockPeon);
    exec.shutdown();
  }

  /**
   * Tier - __default_tier
   * Nodes - 2
   * Replicants - 3
   * Random balancer strategy should not assign anything and not get into loop as there are not enough nodes for replication
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
                new DruidServer("server1", "host1", null, 1000, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 0).addDataSegment(dataSegment)
                                                                                                                   .toImmutableDruidServer(),
                mockPeon
            ),
            new ServerHolder(
                new DruidServer("server2", "host2", null, 1000, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 0).addDataSegment(dataSegment)
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
    CoordinatorStats stats = afterParams.getCoordinatorStats();

    Assert.assertEquals(0L, stats.getTieredStat("assignedCount", DruidServer.DEFAULT_TIER));
    Assert.assertTrue(stats.getTiers("unassignedCount").isEmpty());
    Assert.assertTrue(stats.getTiers("unassignedSize").isEmpty());

    EasyMock.verify(mockPeon);
  }


  /**
   * Tier - __default_tier
   * Nodes - 1
   * Replicants - 1
   * Random balancer strategy should select the only node
   */
  @Test(timeout = 5000L)
  public void testOneNodesOneTierOneReplicantRandomStrategyEnoughSpace()
  {
    mockCoordinator();
    mockPeon.loadSegment(EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();
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

    DruidCoordinatorRuntimeParams params = makeCoordinatorRuntimeParams(druidCluster, balancerStrategy, Collections.singletonList(dataSegment))
        .withDynamicConfigs(CoordinatorDynamicConfig.builder().withMaxSegmentsToMove(5).build())
        .build();

    DruidCoordinatorRuntimeParams afterParams = ruleRunner.run(params);
    CoordinatorStats stats = afterParams.getCoordinatorStats();
    Assert.assertEquals(1L, stats.getTieredStat("assignedCount", DruidServer.DEFAULT_TIER));
    Assert.assertTrue(stats.getTiers("unassignedCount").isEmpty());
    Assert.assertTrue(stats.getTiers("unassignedSize").isEmpty());

    EasyMock.verify(mockPeon);
  }

  /**
   * Tier - __default_tier
   * Nodes - 1
   * Replicants - 1
   * Random balancer strategy should not assign anything as there is not enough space
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
    CoordinatorStats stats = afterParams.getCoordinatorStats();
    Assert.assertEquals(dataSegment.getSize() * numReplicants, stats.getTieredStat(LoadRule.REQUIRED_CAPACITY, DruidServer.DEFAULT_TIER));
    Assert.assertTrue(stats.getTiers("assignedCount").isEmpty()); // since primary assignment failed
    Assert.assertTrue(stats.getTiers("unassignedCount").isEmpty());
    Assert.assertTrue(stats.getTiers("unassignedSize").isEmpty());

    EasyMock.verify(mockPeon);
  }

  /**
   * Tier - __default_tier
   * Nodes - 1
   * Replicants - 1
   * Cost balancer strategy should not assign anything as there is not enough space
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
    CoordinatorStats stats = afterParams.getCoordinatorStats();
    Assert.assertEquals(dataSegment.getSize() * numReplicants, stats.getTieredStat(LoadRule.REQUIRED_CAPACITY, DruidServer.DEFAULT_TIER));
    Assert.assertTrue(stats.getTiers("assignedCount").isEmpty()); // since primary assignment should fail
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
