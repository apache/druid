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
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.client.DruidServer;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.core.EventMap;
import org.apache.druid.java.util.emitter.service.AlertEvent;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.CoordinatorBaseTest;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.balancer.CostBalancerStrategy;
import org.apache.druid.server.coordinator.balancer.RandomBalancerStrategy;
import org.apache.druid.server.coordinator.loading.SegmentLoadQueueManager;
import org.apache.druid.server.coordinator.loading.SegmentReplicaCount;
import org.apache.druid.server.coordinator.loading.SegmentReplicationStatus;
import org.apache.druid.server.coordinator.loading.TestLoadQueuePeon;
import org.apache.druid.server.coordinator.rules.Rule;
import org.apache.druid.server.coordinator.simulate.TestMetadataRuleManager;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.Dimension;
import org.apache.druid.server.coordinator.stats.RowKey;
import org.apache.druid.server.coordinator.stats.Stats;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 *
 */
public class RunRulesTest extends CoordinatorBaseTest
{
  private static final String INTERVAL_START = "2012-01-01";
  private static final RowKey DATASOURCE_STAT_KEY = RowKey.of(Dimension.DATASOURCE, DS.WIKI);

  private RunRules runRules;
  private StubServiceEmitter emitter;
  private TestMetadataRuleManager databaseRuleManager;

  /**
   * 24 hour segments starting at 2012-01-01.
   */
  private final DataSegment[] segments =
      CreateDataSegments.ofDatasource(DS.WIKI)
                        .forIntervals(24, Granularities.HOUR)
                        .startingAt(INTERVAL_START)
                        .withNumPartitions(1)
                        .eachOfSizeInMb(1)
                        .toArray(new DataSegment[0]);

  private ListeningExecutorService balancerExecutor;

  private int segmentVersion = 0;
  private int serverId = 0;

  @Before
  public void setUp()
  {
    serverId = 0;
    segmentVersion = 0;
    emitter = new StubServiceEmitter();
    EmittingLogger.registerEmitter(emitter);
    databaseRuleManager = new TestMetadataRuleManager();
    runRules = new RunRules(Set::size);
    balancerExecutor = MoreExecutors.listeningDecorator(Execs.multiThreaded(1, "RunRulesTest-%d"));
  }

  @After
  public void tearDown()
  {
    balancerExecutor.shutdown();
  }

  /**
   * Nodes:
   * normal - 2 replicants
   * replicationThrottleLimit - 10
   * Expect only 34 segments to be loaded despite there being 48 primary + non-primary replicants to load!
   */
  @Test
  public void testOneTierTwoReplicantsWithStrictReplicantLimit()
  {
    setRules(Load.on(Tier.T2, 2).forInterval("2012-01-01/P1D"));

    // 1 tier, 2 servers, 1 server has all segments already loaded
    final DruidCluster druidCluster = DruidCluster
        .builder()
        .add(createHistorical(Tier.T2, segments))
        .add(createHistorical(Tier.T2))
        .build();

    DruidCoordinatorRuntimeParams params = createRuntimeParams(druidCluster, segments)
        .withDynamicConfigs(
            CoordinatorDynamicConfig
                .builder()
                .withReplicationThrottleLimit(10)
                .withSmartSegmentLoading(false)
                .build()
        )
        .build();

    CoordinatorRunStats stats = runDutyAndGetStats(params);

    // There are 24 under-replicated segments, but only 10 replicas are assigned
    Assert.assertEquals(10L, stats.getSegmentStat(Stats.Segments.ASSIGNED, Tier.T2, DS.WIKI));
  }

  /**
   * Nodes:
   * normal - 2 replicants
   * hot - 2 replicants
   * replicationThrottleLimit - 48
   * Expect only 72 segments to be loaded despite there being 96 primary + non-primary replicants to load!
   */
  @Test
  public void testTwoTiersTwoReplicantsWithStrictReplicantLimit()
  {
    setRules(
        Load.on(Tier.T1, 2).andOn(Tier.T2, 2).forInterval("2012-01-01/2012-01-02")
    );

    DruidCluster druidCluster = DruidCluster
        .builder()
        .add(createHistorical(Tier.T1))
        .add(createHistorical(Tier.T1, segments))
        .add(createHistorical(Tier.T2))
        .add(createHistorical(Tier.T2))
        .build();

    DruidCoordinatorRuntimeParams params = createRuntimeParams(druidCluster, segments)
        .withDynamicConfigs(
            CoordinatorDynamicConfig.builder()
                                    .withReplicationThrottleLimit(10)
                                    .withSmartSegmentLoading(false)
                                    .build()
        )
        .build();

    CoordinatorRunStats stats = runDutyAndGetStats(params);

    // maxNonPrimaryReplicantsToLoad takes effect on hot tier, but not normal tier
    Assert.assertEquals(10L, stats.getSegmentStat(Stats.Segments.ASSIGNED, Tier.T1, DS.WIKI));
    Assert.assertEquals(48L, stats.getSegmentStat(Stats.Segments.ASSIGNED, Tier.T2, DS.WIKI));
  }

  @Test
  public void testLoadByIntervalOnThreeTiers()
  {
    setRules(
        Load.on(Tier.T1, 1).forInterval("2012-01-01/PT6H"),
        Load.on(Tier.T2, 1).forInterval("2012-01-01/PT12H"),
        Load.on(Tier.T3, 1).forInterval("2012-01-01/P1D")
    );

    DruidCluster druidCluster = DruidCluster
        .builder()
        .add(createHistorical(Tier.T1))
        .add(createHistorical(Tier.T2))
        .add(createHistorical(Tier.T3))
        .build();

    DruidCoordinatorRuntimeParams params = createRuntimeParams(druidCluster, segments).build();

    CoordinatorRunStats stats = runDutyAndGetStats(params);

    Assert.assertEquals(6L, stats.getSegmentStat(Stats.Segments.ASSIGNED, Tier.T1, DS.WIKI));
    Assert.assertEquals(6L, stats.getSegmentStat(Stats.Segments.ASSIGNED, Tier.T2, DS.WIKI));
    Assert.assertEquals(12L, stats.getSegmentStat(Stats.Segments.ASSIGNED, Tier.T3, DS.WIKI));
    Assert.assertFalse(stats.hasStat(Stats.Segments.DROPPED));
  }

  @Test
  public void testOnlyOneRuleIsAppliedOnSegment()
  {
    setRules(
        Load.on(Tier.T1, 1).forInterval("2012-01-01/PT6H"),
        Load.on(Tier.T2, 1).forever()
    );

    DruidCluster druidCluster = DruidCluster
        .builder()
        .add(createHistorical(Tier.T1))
        .add(createHistorical(Tier.T2))
        .build();

    DruidCoordinatorRuntimeParams params = createRuntimeParams(druidCluster, segments).build();
    CoordinatorRunStats stats = runDutyAndGetStats(params);

    long numFirstRuleApplied = stats.getSegmentStat(Stats.Segments.ASSIGNED, Tier.T1, DS.WIKI);
    long numSecondRuleApplied = stats.getSegmentStat(Stats.Segments.ASSIGNED, Tier.T2, DS.WIKI);
    Assert.assertEquals(6L, numFirstRuleApplied);
    Assert.assertEquals(18L, numSecondRuleApplied);
    Assert.assertEquals(segments.length, numFirstRuleApplied + numSecondRuleApplied);

    Assert.assertFalse(stats.hasStat(Stats.Segments.DROPPED));
  }

  @Test
  public void testLoadRuleDoesNotAssignFullyReplicatedSegment()
  {
    setRules(
        Load.on(Tier.T1, 1).forever()
    );

    // 1 server with all segments already loaded
    DruidCluster druidCluster = DruidCluster
        .builder()
        .add(createHistorical(Tier.T1, segments))
        .build();

    DruidCoordinatorRuntimeParams params = createRuntimeParams(druidCluster, segments).build();

    CoordinatorRunStats stats = runDutyAndGetStats(params);

    Assert.assertEquals(0L, stats.getSegmentStat(Stats.Segments.ASSIGNED, Tier.T1, DS.WIKI));
    Assert.assertFalse(stats.hasStat(Stats.Segments.DROPPED));
  }

  @Test
  public void testAlertIsRaisedIfLoadRuleRefersToInvalidTier()
  {
    setRules(
        Load.on(Tier.T1, 1).andOn(Tier.T2, 1).forever()
    );

    // Only 1 tier
    DruidCluster druidCluster = DruidCluster
        .builder()
        .add(createHistorical(Tier.T1))
        .build();

    DruidCoordinatorRuntimeParams params = createRuntimeParams(druidCluster, segments).build();

    runDutyAndGetStats(params);

    List<AlertEvent> alerts = emitter.getAlerts();
    Assert.assertEquals(1, alerts.size());
  }

  @Test
  public void testRunRuleDoesNotExist()
  {
    setRules(
        Load.on(Tier.T2, 1).forInterval("2012-01-02/P1D")
    );

    DruidCluster druidCluster = DruidCluster
        .builder()
        .add(createHistorical(Tier.T2))
        .build();

    DruidCoordinatorRuntimeParams params = createRuntimeParams(druidCluster, segments).build();

    runDutyAndGetStats(params);

    final List<AlertEvent> events = emitter.getAlerts();
    Assert.assertEquals(1, events.size());

    AlertEvent alertEvent = events.get(0);
    EventMap eventMap = alertEvent.toMap();
    Assert.assertEquals(
        "No matching retention rule for [24] segments in datasource[wiki]",
        eventMap.get("description")
    );
  }

  @Test
  public void testDropRemove()
  {
    setRules(
        Load.on(Tier.T2, 1).forInterval("2012-01-01/PT12H"),
        Drop.forInterval("2012-01-01/P1D")
    );

    DruidCluster druidCluster = DruidCluster
        .builder()
        .add(createHistorical(Tier.T2))
        .build();

    DruidCoordinatorRuntimeParams params = createRuntimeParams(druidCluster, segments).build();

    CoordinatorRunStats stats = runDutyAndGetStats(params);
    Assert.assertEquals(12L, stats.get(Stats.Segments.DELETED, DATASOURCE_STAT_KEY));
  }

  @Test
  public void testDropTooManyInSameTier()
  {
    setRules(
        Load.on(Tier.T2, 1).forInterval("2012-01-01/PT12H"),
        Drop.forInterval("2012-01-01/P1D")
    );

    DruidCluster druidCluster = DruidCluster
        .builder()
        .add(createHistorical(Tier.T2, segments))
        .add(createHistorical(Tier.T2, segments[0]))
        .build();

    DruidCoordinatorRuntimeParams params = createRuntimeParams(druidCluster, segments)
        .withDynamicConfigs(
            CoordinatorDynamicConfig
                .builder()
                .withMarkSegmentAsUnusedDelayMillis(0L)
                .build()
        )
        .build();

    CoordinatorRunStats stats = runDutyAndGetStats(params);

    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.DROPPED, Tier.T2, DS.WIKI));
    Assert.assertEquals(12L, stats.get(Stats.Segments.DELETED, DATASOURCE_STAT_KEY));
  }

  @Test
  public void testDropTooManyInDifferentTiers()
  {
    setRules(
        Load.on(Tier.T1, 1).forInterval("2012-01-01/PT12H"),
        Drop.forInterval("2012-01-01/P1D")
    );

    DruidCluster druidCluster = DruidCluster
        .builder()
        .add(createHistorical(Tier.T1, segments[0]))
        .add(createHistorical(Tier.T2, segments))
        .build();

    DruidCoordinatorRuntimeParams params = createRuntimeParams(druidCluster, segments).build();

    CoordinatorRunStats stats = runDutyAndGetStats(params);
    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.DROPPED, Tier.T2, DS.WIKI));
    Assert.assertEquals(12L, stats.get(Stats.Segments.DELETED, DATASOURCE_STAT_KEY));
  }

  @Test
  public void testDontDropInDifferentTiers()
  {
    setRules(
        Load.on(Tier.T1, 1).forInterval("2012-01-01/PT12H"),
        Drop.forInterval("2012-01-01/P1D")
    );

    DruidCluster druidCluster = DruidCluster
        .builder()
        .add(createHistorical(Tier.T1))
        .add(createHistorical(Tier.T2, segments))
        .build();

    DruidCoordinatorRuntimeParams params = createRuntimeParams(druidCluster, segments).build();

    CoordinatorRunStats stats = runDutyAndGetStats(params);
    Assert.assertFalse(stats.hasStat(Stats.Segments.DROPPED));
    Assert.assertEquals(12L, stats.get(Stats.Segments.DELETED, DATASOURCE_STAT_KEY));
  }

  @Test
  public void testDropServerActuallyServesSegment()
  {
    setRules(
        Load.on(Tier.T2, 0).forInterval("2012-01-01/PT1H")
    );

    DruidCluster druidCluster = DruidCluster
        .builder()
        .add(createHistorical(Tier.T2, segments[0]))
        .add(createHistorical(Tier.T2, segments[1]))
        .add(createHistorical(Tier.T2, segments[1], segments[2]))
        .build();
    DruidCoordinatorRuntimeParams params = createRuntimeParams(druidCluster, segments).build();

    CoordinatorRunStats stats = runDutyAndGetStats(params);
    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.DROPPED, Tier.T2, DS.WIKI));
  }

  /**
   * Nodes:
   * hot - 2 replicants
   */
  @Test
  public void testNoThrottleWhenSegmentNotLoadedInTier()
  {
    setRules(
        Load.on(Tier.T1, 2).forInterval("2012-01-01/2013-01-01")
    );

    DruidCluster druidCluster = DruidCluster
        .builder()
        .add(createHistorical(Tier.T1))
        .add(createHistorical(Tier.T1))
        .build();

    DruidCoordinatorRuntimeParams params = createRuntimeParams(druidCluster, segments).build();

    CoordinatorRunStats stats = runDutyAndGetStats(params);
    Assert.assertEquals(48L, stats.getSegmentStat(Stats.Segments.ASSIGNED, Tier.T1, DS.WIKI));
    Assert.assertFalse(stats.hasStat(Stats.Segments.DROPPED));

    DataSegment overFlowSegment = createSegment(
        Intervals.of("2012-02-01/2012-02-02"),
        0
    );

    params = createRuntimeParams(druidCluster, overFlowSegment).build();
    stats = runDutyAndGetStats(params);

    Assert.assertEquals(2L, stats.getSegmentStat(Stats.Segments.ASSIGNED, Tier.T1, DS.WIKI));
  }

  /**
   * Nodes:
   * hot - nothing loaded
   * _default_tier - 1 segment loaded
   */
  @Test
  public void testReplicantThrottleAcrossTiers()
  {
    setRules(
        Load.on(Tier.T1, 1).andOn(Tier.T2, 1).forInterval("2012-01-01/P1Y")
    );

    final DruidCluster druidCluster = DruidCluster
        .builder()
        .add(createHistorical(Tier.T1))
        .add(createHistorical(Tier.T2))
        .build();

    DruidCoordinatorRuntimeParams params = createRuntimeParams(druidCluster, segments)
        .withDynamicConfigs(CoordinatorDynamicConfig.builder().withReplicationThrottleLimit(7).build())
        .build();

    CoordinatorRunStats stats = runDutyAndGetStats(params);
    Assert.assertEquals(24L, stats.getSegmentStat(Stats.Segments.ASSIGNED, Tier.T1, DS.WIKI));
    Assert.assertEquals(24L, stats.getSegmentStat(Stats.Segments.ASSIGNED, Tier.T2, DS.WIKI));
    Assert.assertFalse(stats.hasStat(Stats.Segments.DROPPED));
  }

  @Test
  public void testDropReplicantThrottle()
  {
    setRules(
        Load.on(Tier.T2, 1).forInterval("2012-01-01/P1Y1D")
    );

    final DataSegment[] usedSegmentsWithExtra = Arrays.copyOf(segments, segments.length + 1);
    usedSegmentsWithExtra[segments.length] = createSegment(Intervals.of("2012-02-01/P1D"), 0);

    DruidCluster druidCluster =
        DruidCluster.builder()
                    .add(createHistorical(Tier.T2, usedSegmentsWithExtra))
                    .add(createHistorical(Tier.T2, usedSegmentsWithExtra))
                    .build();

    DruidCoordinatorRuntimeParams params = createRuntimeParams(druidCluster, usedSegmentsWithExtra).build();

    CoordinatorRunStats stats = runDutyAndGetStats(params);

    // There is no throttling on drop
    Assert.assertEquals(25L, stats.getSegmentStat(Stats.Segments.DROPPED, Tier.T2, DS.WIKI));
  }

  @Test
  public void testRulesRunOnNonOvershadowedSegmentsOnly()
  {
    final DataSegment[] usedSegments = new DataSegment[]{
        createSegment(Intervals.of("2012-01-01/P1D"), 1),
        createSegment(Intervals.of("2012-01-01/P1D"), 1)
    };

    setRules(
        Load.on(Tier.T1, 1).forever()
    );

    DruidCluster druidCluster = DruidCluster
        .builder()
        .add(createHistorical(Tier.T1))
        .build();

    DruidCoordinatorRuntimeParams params = createRuntimeParams(druidCluster, usedSegments)
        .withDynamicConfigs(CoordinatorDynamicConfig.builder().withMaxSegmentsToMove(5).build())
        .build();

    CoordinatorRunStats stats = runDutyAndGetStats(params);
    Assert.assertEquals(1, stats.getSegmentStat(Stats.Segments.ASSIGNED, Tier.T1, DS.WIKI));
    Assert.assertFalse(stats.hasStat(Stats.Segments.DROPPED));

    Assert.assertEquals(2, usedSegments.length);
    Assert.assertEquals(Sets.newHashSet(usedSegments), params.getUsedSegments());
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
    setRules(
        Load.on(Tier.T1, 3).forever()
    );

    DataSegment dataSegment = createSegment(Intervals.utc(0, 1), 1);

    DruidCluster druidCluster =
        DruidCluster.builder()
                    .add(createHistorical(Tier.T1, dataSegment))
                    .add(createHistorical(Tier.T1, dataSegment))
                    .build();

    DruidCoordinatorRuntimeParams params =
        createRuntimeParams(druidCluster, dataSegment)
            .withBalancerStrategy(new RandomBalancerStrategy())
            .withDynamicConfigs(CoordinatorDynamicConfig.builder().withMaxSegmentsToMove(5).build())
            .build();

    CoordinatorRunStats stats = runDutyAndGetStats(params);
    Assert.assertEquals(0L, stats.getSegmentStat(Stats.Segments.ASSIGNED, Tier.T1, DS.WIKI));
    Assert.assertFalse(stats.hasStat(Stats.Segments.DROPPED));
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
    setRules(
        Load.on(Tier.T1, 1).forever()
    );

    DataSegment dataSegment = createSegment(Intervals.utc(0, 1), 1);

    DruidCluster druidCluster =
        DruidCluster
            .builder()
            .add(createHistorical(Tier.T1))
            .build();

    DruidCoordinatorRuntimeParams params =
        createRuntimeParams(druidCluster, dataSegment)
            .withBalancerStrategy(new RandomBalancerStrategy())
            .withDynamicConfigs(CoordinatorDynamicConfig.builder().withMaxSegmentsToMove(5).build())
            .build();

    CoordinatorRunStats stats = runDutyAndGetStats(params);
    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.ASSIGNED, Tier.T1, DS.WIKI));
    Assert.assertFalse(stats.hasStat(Stats.Segments.DROPPED));
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
    setRules(
        Load.on(Tier.T1, 1).forever()
    );

    final DataSegment dataSegment = createSegment(Intervals.utc(0, 1), 11);

    ServerHolder server = new ServerHolder(
        new DruidServer("server1", "host1", null, 10, ServerType.HISTORICAL, Tier.T1, 0)
            .toImmutableDruidServer(),
        new TestLoadQueuePeon()
    );
    DruidCluster druidCluster = DruidCluster.builder().add(server).build();

    DruidCoordinatorRuntimeParams params =
        createRuntimeParams(druidCluster, dataSegment)
            .withBalancerStrategy(new RandomBalancerStrategy())
            .withDynamicConfigs(CoordinatorDynamicConfig.builder().withMaxSegmentsToMove(5).build())
            .build();

    CoordinatorRunStats stats = runDutyAndGetStats(params);
    final RowKey tierRowKey = RowKey.of(Dimension.TIER, Tier.T1);
    Assert.assertEquals(
        dataSegment.getSize(),
        stats.get(Stats.Tier.REQUIRED_CAPACITY, tierRowKey)
    );

    // Verify that primary assignment failed
    Assert.assertEquals(0L, stats.getSegmentStat(Stats.Segments.ASSIGNED, Tier.T1, DS.WIKI));
    Assert.assertFalse(stats.hasStat(Stats.Segments.DROPPED));
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
    setRules(
        Load.on(Tier.T1, 1).forever()
    );

    DataSegment dataSegment = createSegment(Intervals.utc(0, 1), 11);

    ServerHolder server = new ServerHolder(
        new DruidServer("server1", "host1", null, 10, ServerType.HISTORICAL, Tier.T1, 0)
            .toImmutableDruidServer(),
        new TestLoadQueuePeon()
    );
    DruidCluster druidCluster = DruidCluster.builder().add(server).build();

    DruidCoordinatorRuntimeParams params =
        createRuntimeParams(druidCluster, dataSegment)
            .withBalancerStrategy(new CostBalancerStrategy(balancerExecutor))
            .withDynamicConfigs(CoordinatorDynamicConfig.builder().withMaxSegmentsToMove(5).build())
            .build();

    CoordinatorRunStats stats = runDutyAndGetStats(params);
    final RowKey tierRowKey = RowKey.of(Dimension.TIER, Tier.T1);
    Assert.assertEquals(
        dataSegment.getSize(),
        stats.get(Stats.Tier.REQUIRED_CAPACITY, tierRowKey)
    );
    Assert.assertEquals(0L, stats.getSegmentStat(Stats.Segments.ASSIGNED, Tier.T1, DS.WIKI));
    Assert.assertFalse(stats.hasStat(Stats.Segments.DROPPED));
  }

  private CoordinatorRunStats runDutyAndGetStats(DruidCoordinatorRuntimeParams params, Rule... rules)
  {
    if (rules.length > 0) {
      databaseRuleManager.overrideRule(DS.WIKI, Arrays.asList(rules), null);
    }
    return runRules.run(params).getCoordinatorStats();
  }

  private void setRules(Rule... rules)
  {
    databaseRuleManager.overrideRule(DS.WIKI, Arrays.asList(rules), null);
  }

  /**
   * Create a historical with max size 10GB and serving the given segments.
   */
  private ServerHolder createHistorical(String tier, DataSegment... servedSegments)
  {
    final String name = "server_" + serverId++;
    DruidServer server = new DruidServer(name, name, null, 10L << 30, ServerType.HISTORICAL, tier, 0);
    for (DataSegment segment : servedSegments) {
      server.addDataSegment(segment);
    }
    return new ServerHolder(server.toImmutableDruidServer(), new TestLoadQueuePeon());
  }

  @Test
  public void testSegmentWithZeroRequiredReplicasHasZeroReplicationFactor()
  {
    setRules(
        Load.on(Tier.T1, 0).forever()
    );

    final DruidCluster cluster = DruidCluster
        .builder()
        .add(createHistorical(Tier.T1))
        .build();

    final DataSegment segment = segments[0];
    DruidCoordinatorRuntimeParams params = createRuntimeParams(cluster, segment).build();
    params = runRules.run(params);

    Assert.assertNotNull(params);
    SegmentReplicationStatus replicationStatus = params.getSegmentReplicationStatus();
    Assert.assertNotNull(replicationStatus);

    SegmentReplicaCount replicaCounts = replicationStatus.getReplicaCountsInCluster(segment.getId());
    Assert.assertNotNull(replicaCounts);
    Assert.assertEquals(0, replicaCounts.required());
    Assert.assertEquals(0, replicaCounts.totalLoaded());
    Assert.assertEquals(0, replicaCounts.requiredAndLoadable());
  }

  private DataSegment createSegment(Interval interval, long size)
  {
    return new DataSegment(
        DS.WIKI,
        interval,
        "" + segmentVersion++,
        Collections.emptyMap(),
        Collections.emptyList(),
        Collections.emptyList(),
        NoneShardSpec.instance(),
        IndexIO.CURRENT_VERSION_ID,
        size
    );
  }

  private DruidCoordinatorRuntimeParams.Builder createRuntimeParams(
      DruidCluster druidCluster,
      DataSegment... segments
  )
  {
    return DruidCoordinatorRuntimeParams
        .newBuilder(DateTimes.nowUtc().minusDays(1))
        .withDruidCluster(druidCluster)
        .withUsedSegments(segments)
        .withDynamicConfigs(CoordinatorDynamicConfig.builder().build())
        .withDatabaseRuleManager(databaseRuleManager)
        .withBalancerStrategy(new CostBalancerStrategy(balancerExecutor))
        .withSegmentAssignerUsing(new SegmentLoadQueueManager(null, null));
  }

}
