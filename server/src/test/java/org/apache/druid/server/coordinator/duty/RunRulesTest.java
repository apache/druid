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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
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
import org.apache.druid.metadata.MetadataRuleManager;
import org.apache.druid.metadata.SegmentsMetadataManager;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.balancer.CostBalancerStrategy;
import org.apache.druid.server.coordinator.balancer.RandomBalancerStrategy;
import org.apache.druid.server.coordinator.loading.LoadQueuePeon;
import org.apache.druid.server.coordinator.loading.SegmentLoadQueueManager;
import org.apache.druid.server.coordinator.loading.SegmentReplicaCount;
import org.apache.druid.server.coordinator.loading.SegmentReplicationStatus;
import org.apache.druid.server.coordinator.loading.TestLoadQueuePeon;
import org.apache.druid.server.coordinator.rules.ForeverLoadRule;
import org.apache.druid.server.coordinator.rules.IntervalDropRule;
import org.apache.druid.server.coordinator.rules.IntervalLoadRule;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.Dimension;
import org.apache.druid.server.coordinator.stats.RowKey;
import org.apache.druid.server.coordinator.stats.Stats;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.easymock.EasyMock;
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

/**
 *
 */
public class RunRulesTest
{
  private static final long SERVER_SIZE_10GB = 10L << 30;
  private static final String DATASOURCE = "test";
  private static final RowKey DATASOURCE_STAT_KEY = RowKey.of(Dimension.DATASOURCE, DATASOURCE);

  private LoadQueuePeon mockPeon;
  private RunRules ruleRunner;
  private StubServiceEmitter emitter;
  private MetadataRuleManager databaseRuleManager;
  private SegmentsMetadataManager segmentsMetadataManager;
  private SegmentLoadQueueManager loadQueueManager;
  private final List<DataSegment> usedSegments =
      CreateDataSegments.ofDatasource(DATASOURCE)
                        .forIntervals(24, Granularities.HOUR)
                        .startingAt("2012-01-01")
                        .withNumPartitions(1)
                        .eachOfSizeInMb(1);

  private ListeningExecutorService balancerExecutor;

  @Before
  public void setUp()
  {
    mockPeon = EasyMock.createMock(LoadQueuePeon.class);
    emitter = new StubServiceEmitter("coordinator", "host");
    EmittingLogger.registerEmitter(emitter);
    databaseRuleManager = EasyMock.createMock(MetadataRuleManager.class);
    segmentsMetadataManager = EasyMock.createNiceMock(SegmentsMetadataManager.class);
    ruleRunner = new RunRules(Set::size);
    loadQueueManager = new SegmentLoadQueueManager(null, segmentsMetadataManager, null);
    balancerExecutor = MoreExecutors.listeningDecorator(Execs.multiThreaded(1, "RunRulesTest-%d"));
  }

  @After
  public void tearDown()
  {
    balancerExecutor.shutdown();
    EasyMock.verify(databaseRuleManager);
  }

  /**
   * Nodes:
   * normal - 2 replicants
   * maxNonPrimaryReplicantsToLoad - 10
   * Expect only 34 segments to be loaded despite there being 48 primary + non-primary replicants to load!
   */
  @Test
  public void testOneTierTwoReplicantsWithStrictReplicantLimit()
  {
    mockPeon.loadSegment(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    mockEmptyPeon();

    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.anyObject())).andReturn(
        Collections.singletonList(
            new IntervalLoadRule(
                Intervals.of("2012-01-01/2012-01-02"),
                ImmutableMap.of("normal", 2),
                null
            )
        )).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    // server1 has all the segments already loaded
    final DruidServer server1 = createHistorical("server1", "normal");
    usedSegments.forEach(server1::addDataSegment);

    final DruidServer server2 = createHistorical("server2", "normal");
    final DruidCluster druidCluster = DruidCluster
        .builder()
        .addTier(
            "normal",
            new ServerHolder(server1.toImmutableDruidServer(), mockPeon),
            new ServerHolder(server2.toImmutableDruidServer(), mockPeon)
        ).build();

    DruidCoordinatorRuntimeParams params = createCoordinatorRuntimeParams(druidCluster)
        .withBalancerStrategy(new CostBalancerStrategy(balancerExecutor))
        .withDynamicConfigs(
            CoordinatorDynamicConfig
                .builder()
                .withMaxNonPrimaryReplicantsToLoad(10)
                .withSmartSegmentLoading(false)
                .build()
        )
        .withSegmentAssignerUsing(loadQueueManager)
        .build();

    CoordinatorRunStats stats = runDutyAndGetStats(params);

    // There are 24 under-replicated segments, but only 10 replicas are assigned
    Assert.assertEquals(10L, stats.getSegmentStat(Stats.Segments.ASSIGNED, "normal", DATASOURCE));

    EasyMock.verify(mockPeon);
  }

  /**
   * Nodes:
   * normal - 2 replicants
   * hot - 2 replicants
   * maxNonPrimaryReplicantsToLoad - 48
   * Expect only 72 segments to be loaded despite there being 96 primary + non-primary replicants to load!
   */
  @Test
  public void testTwoTiersTwoReplicantsWithStrictReplicantLimit()
  {
    mockPeon.loadSegment(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    mockEmptyPeon();

    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.anyObject())).andReturn(
        Collections.singletonList(
            new IntervalLoadRule(
                Intervals.of("2012-01-01T00:00:00.000Z/2012-01-02T00:00:00.000Z"),
                ImmutableMap.of("hot", 2, "normal", 2),
                null
            )
        )).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    final DruidServer serverHot1 = createHistorical("serverHot", "hot");
    final DruidServer serverHot2 = createHistorical("serverHot2", "hot");
    usedSegments.forEach(serverHot1::addDataSegment);

    final DruidServer serverNorm1 = createHistorical("serverNorm", "normal");
    final DruidServer serverNorm2 = createHistorical("serverNorm2", "normal");

    DruidCluster druidCluster = DruidCluster
        .builder()
        .addTier(
            "hot",
            new ServerHolder(serverHot1.toImmutableDruidServer(), mockPeon),
            new ServerHolder(serverHot2.toImmutableDruidServer(), mockPeon)
        )
        .addTier(
            "normal",
            new ServerHolder(serverNorm1.toImmutableDruidServer(), mockPeon),
            new ServerHolder(serverNorm2.toImmutableDruidServer(), mockPeon)
        )
        .build();

    DruidCoordinatorRuntimeParams params = createCoordinatorRuntimeParams(druidCluster)
        .withBalancerStrategy(new CostBalancerStrategy(balancerExecutor))
        .withDynamicConfigs(
            CoordinatorDynamicConfig.builder()
                                    .withMaxNonPrimaryReplicantsToLoad(10)
                                    .withSmartSegmentLoading(false)
                                    .build()
        )
        .withSegmentAssignerUsing(loadQueueManager)
        .build();

    CoordinatorRunStats stats = runDutyAndGetStats(params);

    // maxNonPrimaryReplicantsToLoad takes effect on hot tier, but not normal tier
    Assert.assertEquals(10L, stats.getSegmentStat(Stats.Segments.ASSIGNED, "hot", DATASOURCE));
    Assert.assertEquals(48L, stats.getSegmentStat(Stats.Segments.ASSIGNED, "normal", DATASOURCE));

    EasyMock.verify(mockPeon);
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
    mockPeon.loadSegment(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    mockEmptyPeon();

    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.anyObject())).andReturn(
        Lists.newArrayList(
            new IntervalLoadRule(
                Intervals.of("2012-01-01T00:00:00.000Z/2012-01-01T06:00:00.000Z"),
                ImmutableMap.of("hot", 1),
                null
            ),
            new IntervalLoadRule(
                Intervals.of("2012-01-01T00:00:00.000Z/2012-01-01T12:00:00.000Z"),
                ImmutableMap.of("normal", 1),
                null
            ),
            new IntervalLoadRule(
                Intervals.of("2012-01-01T00:00:00.000Z/2012-01-02T00:00:00.000Z"),
                ImmutableMap.of("cold", 1),
                null
            )
        )).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    DruidCluster druidCluster = DruidCluster
        .builder()
        .addTier(
            "hot",
            new ServerHolder(
                createHistorical("serverHot", "hot").toImmutableDruidServer(),
                mockPeon
            )
        )
        .addTier(
            "normal",
            new ServerHolder(
                createHistorical("serverNorm", "normal").toImmutableDruidServer(),
                mockPeon
            )
        )
        .addTier(
            "cold",
            new ServerHolder(
                createHistorical("serverCold", "cold").toImmutableDruidServer(),
                mockPeon
            )
        )
        .build();

    DruidCoordinatorRuntimeParams params = createCoordinatorRuntimeParams(druidCluster)
        .withBalancerStrategy(new CostBalancerStrategy(balancerExecutor))
        .withDynamicConfigs(CoordinatorDynamicConfig.builder().withMaxSegmentsToMove(5).build())
        .withSegmentAssignerUsing(loadQueueManager)
        .build();

    CoordinatorRunStats stats = runDutyAndGetStats(params);

    Assert.assertEquals(6L, stats.getSegmentStat(Stats.Segments.ASSIGNED, "hot", DATASOURCE));
    Assert.assertEquals(6L, stats.getSegmentStat(Stats.Segments.ASSIGNED, "normal", DATASOURCE));
    Assert.assertEquals(12L, stats.getSegmentStat(Stats.Segments.ASSIGNED, "cold", DATASOURCE));
    Assert.assertFalse(stats.hasStat(Stats.Segments.DROPPED));

    EasyMock.verify(mockPeon);
  }

  private DruidServer createHistorical(String name, String tier)
  {
    return new DruidServer(name, name, null, SERVER_SIZE_10GB, ServerType.HISTORICAL, tier, 0);
  }

  private ServerHolder createServerHolder(String name, String tier, LoadQueuePeon peon)
  {
    return new ServerHolder(createHistorical(name, tier).toImmutableDruidServer(), peon);
  }

  private DruidCoordinatorRuntimeParams.Builder createCoordinatorRuntimeParams(
      DruidCluster druidCluster,
      DataSegment segment
  )
  {
    return createCoordinatorRuntimeParams(druidCluster, Collections.singletonList(segment));
  }

  private DruidCoordinatorRuntimeParams.Builder createCoordinatorRuntimeParams(DruidCluster druidCluster)
  {
    return createCoordinatorRuntimeParams(druidCluster, usedSegments);
  }

  private DruidCoordinatorRuntimeParams.Builder createCoordinatorRuntimeParams(
      DruidCluster druidCluster,
      List<DataSegment> dataSegments
  )
  {
    return DruidCoordinatorRuntimeParams
        .newBuilder(DateTimes.nowUtc().minusDays(1))
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
    mockPeon.loadSegment(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    mockEmptyPeon();

    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.anyObject())).andReturn(
        Lists.newArrayList(
            new IntervalLoadRule(
                Intervals.of("2012-01-01T00:00:00.000Z/2012-01-01T06:00:00.000Z"),
                ImmutableMap.of("hot", 2),
                null
            ),
            new IntervalLoadRule(
                Intervals.of("2012-01-01T00:00:00.000Z/2012-01-02T00:00:00.000Z"),
                ImmutableMap.of("cold", 1),
                null
            )
        )
    ).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    DruidCluster druidCluster = DruidCluster
        .builder()
        .add(createServerHolder("serverHot", "hot", mockPeon))
        .add(createServerHolder("serverHot2", "hot", mockPeon))
        .add(createServerHolder("serverCold", "cold", mockPeon))
        .build();

    DruidCoordinatorRuntimeParams params = createCoordinatorRuntimeParams(druidCluster)
        .withBalancerStrategy(new CostBalancerStrategy(balancerExecutor))
        .withSegmentAssignerUsing(loadQueueManager)
        .build();
    CoordinatorRunStats stats = runDutyAndGetStats(params);

    Assert.assertEquals(12L, stats.getSegmentStat(Stats.Segments.ASSIGNED, "hot", DATASOURCE));
    Assert.assertEquals(18L, stats.getSegmentStat(Stats.Segments.ASSIGNED, "cold", DATASOURCE));
    Assert.assertFalse(stats.hasStat(Stats.Segments.DROPPED));

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
    mockPeon.loadSegment(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    mockEmptyPeon();

    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.anyObject())).andReturn(
        Lists.newArrayList(
            new IntervalLoadRule(
                Intervals.of("2012-01-01T00:00:00.000Z/2012-01-01T12:00:00.000Z"),
                ImmutableMap.of("hot", 1),
                null
            ),
            new IntervalLoadRule(
                Intervals.of("2012-01-01T00:00:00.000Z/2012-01-02T00:00:00.000Z"),
                ImmutableMap.of("normal", 1),
                null
            )
        )
    ).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    DruidServer normServer = createHistorical("serverNorm", "normal");
    for (DataSegment segment : usedSegments) {
      normServer.addDataSegment(segment);
    }

    DruidCluster druidCluster = DruidCluster
        .builder()
        .add(createServerHolder("serverHot", "hot", mockPeon))
        .add(new ServerHolder(normServer.toImmutableDruidServer(), mockPeon))
        .build();

    DruidCoordinatorRuntimeParams params = createCoordinatorRuntimeParams(druidCluster)
        .withBalancerStrategy(new CostBalancerStrategy(balancerExecutor))
        .withSegmentAssignerUsing(loadQueueManager)
        .build();

    CoordinatorRunStats stats = runDutyAndGetStats(params);

    Assert.assertEquals(12L, stats.getSegmentStat(Stats.Segments.ASSIGNED, "hot", DATASOURCE));
    Assert.assertEquals(0L, stats.getSegmentStat(Stats.Segments.ASSIGNED, "normal", DATASOURCE));
    Assert.assertFalse(stats.hasStat(Stats.Segments.DROPPED));

    EasyMock.verify(mockPeon);
  }

  @Test
  public void testRunTwoTiersTierDoesNotExist()
  {
    mockPeon.loadSegment(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    mockEmptyPeon();

    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.anyObject())).andReturn(
        Lists.newArrayList(
            new IntervalLoadRule(
                Intervals.of("2012-01-01T00:00:00.000Z/2012-01-01T12:00:00.000Z"),
                ImmutableMap.of("hot", 1),
                null
            ),
            new IntervalLoadRule(
                Intervals.of("2012-01-01T00:00:00.000Z/2012-01-02T00:00:00.000Z"),
                ImmutableMap.of("normal", 1),
                null
            )
        )
    ).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    DruidCluster druidCluster = DruidCluster
        .builder()
        .add(createServerHolder("serverNorm", "normal", mockPeon))
        .build();

    DruidCoordinatorRuntimeParams params = createCoordinatorRuntimeParams(druidCluster)
        .withBalancerStrategy(new CostBalancerStrategy(balancerExecutor))
        .withSegmentAssignerUsing(loadQueueManager)
        .build();

    runDutyAndGetStats(params);

    EasyMock.verify(mockPeon);
  }

  @Test
  public void testRunRuleDoesNotExist()
  {

    EasyMock
        .expect(databaseRuleManager.getRulesWithDefault(EasyMock.anyObject()))
        .andReturn(
            Collections.singletonList(
                new IntervalLoadRule(
                    Intervals.of("2012-01-02T00:00:00.000Z/2012-01-03T00:00:00.000Z"),
                    ImmutableMap.of("normal", 1),
                    null
                )
            )
        )
        .atLeastOnce();

    EasyMock.expect(mockPeon.getSegmentsInQueue()).andReturn(Collections.emptySet()).anyTimes();
    EasyMock.expect(mockPeon.getSegmentsMarkedToDrop()).andReturn(Collections.emptySet()).anyTimes();
    EasyMock.replay(databaseRuleManager, mockPeon);

    DruidCluster druidCluster = DruidCluster
        .builder()
        .add(createServerHolder("serverNorm", "normal", mockPeon))
        .build();

    DruidCoordinatorRuntimeParams params = createCoordinatorRuntimeParams(druidCluster)
        .withBalancerStrategy(new CostBalancerStrategy(balancerExecutor))
        .withSegmentAssignerUsing(loadQueueManager)
        .build();

    runDutyAndGetStats(params);

    final List<AlertEvent> events = emitter.getAlerts();
    Assert.assertEquals(1, events.size());

    AlertEvent alertEvent = events.get(0);
    EventMap eventMap = alertEvent.toMap();
    Assert.assertEquals(
        "No matching retention rule for [24] segments in datasource[test]",
        eventMap.get("description")
    );
    EasyMock.verify(mockPeon);
  }

  @Test
  public void testDropRemove()
  {
    mockPeon.dropSegment(EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    mockEmptyPeon();

    EasyMock.expect(segmentsMetadataManager.markSegmentAsUnused(EasyMock.anyObject()))
            .andReturn(true).anyTimes();
    EasyMock.replay(segmentsMetadataManager);

    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.anyObject())).andReturn(
        Lists.newArrayList(
            new IntervalLoadRule(
                Intervals.of("2012-01-01T00:00:00.000Z/2012-01-01T12:00:00.000Z"),
                ImmutableMap.of("normal", 1),
                null
            ),
            new IntervalDropRule(Intervals.of("2012-01-01T00:00:00.000Z/2012-01-02T00:00:00.000Z"))
        )
    ).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    DruidServer server = createHistorical("serverNorm", "normal");
    for (DataSegment segment : usedSegments) {
      server.addDataSegment(segment);
    }

    DruidCluster druidCluster = DruidCluster
        .builder()
        .addTier("normal", new ServerHolder(server.toImmutableDruidServer(), mockPeon))
        .build();

    DruidCoordinatorRuntimeParams params = createCoordinatorRuntimeParams(druidCluster)
        .withBalancerStrategy(new CostBalancerStrategy(balancerExecutor))
        .withSegmentAssignerUsing(loadQueueManager)
        .build();

    CoordinatorRunStats stats = runDutyAndGetStats(params);
    Assert.assertEquals(12L, stats.get(Stats.Segments.DELETED, DATASOURCE_STAT_KEY));
  }

  @Test
  public void testDropTooManyInSameTier()
  {
    mockPeon.dropSegment(EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    mockEmptyPeon();

    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.anyObject())).andReturn(
        Lists.newArrayList(
            new IntervalLoadRule(
                Intervals.of("2012-01-01T00:00:00.000Z/2012-01-01T12:00:00.000Z"),
                ImmutableMap.of("normal", 1),
                null
            ),
            new IntervalDropRule(Intervals.of("2012-01-01T00:00:00.000Z/2012-01-02T00:00:00.000Z"))
        )
    ).atLeastOnce();
    EasyMock.replay(databaseRuleManager, segmentsMetadataManager);

    DruidServer server1 = createHistorical("serverNorm", "normal");
    server1.addDataSegment(usedSegments.get(0));

    DruidServer server2 = createHistorical("serverNorm2", "normal");
    for (DataSegment segment : usedSegments) {
      server2.addDataSegment(segment);
    }

    DruidCluster druidCluster = DruidCluster
        .builder()
        .addTier(
            "normal",
            new ServerHolder(server1.toImmutableDruidServer(), mockPeon),
            new ServerHolder(server2.toImmutableDruidServer(), mockPeon)
        )
        .build();

    DruidCoordinatorRuntimeParams params = createCoordinatorRuntimeParams(druidCluster)
        .withDynamicConfigs(
            CoordinatorDynamicConfig
                .builder()
                .withMarkSegmentAsUnusedDelayMillis(0L)
                .build()
        )
        .withBalancerStrategy(new CostBalancerStrategy(balancerExecutor))
        .withSegmentAssignerUsing(loadQueueManager)
        .build();

    CoordinatorRunStats stats = runDutyAndGetStats(params);

    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.DROPPED, "normal", DATASOURCE));
    Assert.assertEquals(12L, stats.get(Stats.Segments.DELETED, DATASOURCE_STAT_KEY));

    EasyMock.verify(mockPeon);
  }

  @Test
  public void testDropTooManyInDifferentTiers()
  {
    mockPeon.loadSegment(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    mockPeon.dropSegment(EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    mockEmptyPeon();

    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.anyObject())).andReturn(
        Lists.newArrayList(
            new IntervalLoadRule(
                Intervals.of("2012-01-01T00:00:00.000Z/2012-01-01T12:00:00.000Z"),
                ImmutableMap.of("hot", 1),
                null
            ),
            new IntervalDropRule(Intervals.of("2012-01-01T00:00:00.000Z/2012-01-02T00:00:00.000Z"))
        )
    ).atLeastOnce();
    EasyMock.replay(databaseRuleManager, segmentsMetadataManager);

    DruidServer server1 = createHistorical("server1", "hot");
    server1.addDataSegment(usedSegments.get(0));
    DruidServer server2 = createHistorical("serverNorm2", "normal");
    for (DataSegment segment : usedSegments) {
      server2.addDataSegment(segment);
    }

    DruidCluster druidCluster = DruidCluster
        .builder()
        .addTier("hot", new ServerHolder(server1.toImmutableDruidServer(), mockPeon))
        .addTier("normal", new ServerHolder(server2.toImmutableDruidServer(), mockPeon))
        .build();

    DruidCoordinatorRuntimeParams params = createCoordinatorRuntimeParams(druidCluster)
        .withBalancerStrategy(new CostBalancerStrategy(balancerExecutor))
        .withSegmentAssignerUsing(loadQueueManager)
        .build();

    CoordinatorRunStats stats = runDutyAndGetStats(params);
    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.DROPPED, "normal", DATASOURCE));
    Assert.assertEquals(12L, stats.get(Stats.Segments.DELETED, DATASOURCE_STAT_KEY));

    EasyMock.verify(mockPeon);
  }

  @Test
  public void testDontDropInDifferentTiers()
  {
    mockPeon.loadSegment(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    mockEmptyPeon();

    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.anyObject())).andReturn(
        Lists.newArrayList(
            new IntervalLoadRule(
                Intervals.of("2012-01-01T00:00:00.000Z/2012-01-01T12:00:00.000Z"),
                ImmutableMap.of("hot", 1),
                null
            ),
            new IntervalDropRule(Intervals.of("2012-01-01T00:00:00.000Z/2012-01-02T00:00:00.000Z"))
        )
    ).atLeastOnce();
    EasyMock.replay(databaseRuleManager, segmentsMetadataManager);

    DruidServer server1 = createHistorical("server1", "hot");
    DruidServer server2 = createHistorical("serverNorm2", "normal");
    for (DataSegment segment : usedSegments) {
      server2.addDataSegment(segment);
    }

    DruidCluster druidCluster = DruidCluster
        .builder()
        .add(new ServerHolder(server1.toImmutableDruidServer(), mockPeon))
        .add(new ServerHolder(server2.toImmutableDruidServer(), mockPeon))
        .build();

    DruidCoordinatorRuntimeParams params = createCoordinatorRuntimeParams(druidCluster)
        .withBalancerStrategy(new CostBalancerStrategy(balancerExecutor))
        .withSegmentAssignerUsing(loadQueueManager)
        .build();

    CoordinatorRunStats stats = runDutyAndGetStats(params);
    Assert.assertFalse(stats.hasStat(Stats.Segments.DROPPED));
    Assert.assertEquals(12L, stats.get(Stats.Segments.DELETED, DATASOURCE_STAT_KEY));

    EasyMock.verify(mockPeon);
  }

  @Test
  public void testDropServerActuallyServesSegment()
  {
    EasyMock
        .expect(databaseRuleManager.getRulesWithDefault(EasyMock.anyObject()))
        .andReturn(
            Collections.singletonList(
                new IntervalLoadRule(
                    Intervals.of("2012-01-01T00:00:00.000Z/2012-01-01T01:00:00.000Z"),
                    ImmutableMap.of("normal", 0),
                    null
                )
            )
        )
        .atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    DruidServer server1 = createHistorical("server1", "normal");
    server1.addDataSegment(usedSegments.get(0));
    DruidServer server2 = createHistorical("serverNorm2", "normal");
    server2.addDataSegment(usedSegments.get(1));
    DruidServer server3 = createHistorical("serverNorm3", "normal");
    server3.addDataSegment(usedSegments.get(1));
    server3.addDataSegment(usedSegments.get(2));

    mockPeon.dropSegment(EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    mockEmptyPeon();

    LoadQueuePeon anotherMockPeon = EasyMock.createMock(LoadQueuePeon.class);
    EasyMock.expect(anotherMockPeon.getSegmentsMarkedToDrop()).andReturn(Collections.emptySet()).anyTimes();
    EasyMock.expect(anotherMockPeon.getSegmentsInQueue()).andReturn(Collections.emptySet()).anyTimes();
    EasyMock.expect(anotherMockPeon.getSegmentsToLoad()).andReturn(Collections.emptySet()).anyTimes();

    EasyMock.replay(anotherMockPeon);

    DruidCluster druidCluster = DruidCluster
        .builder()
        .addTier(
            "normal",
            new ServerHolder(server1.toImmutableDruidServer(), mockPeon, false),
            new ServerHolder(server2.toImmutableDruidServer(), anotherMockPeon, false),
            new ServerHolder(server3.toImmutableDruidServer(), anotherMockPeon, false)
        )
        .build();

    DruidCoordinatorRuntimeParams params = createCoordinatorRuntimeParams(druidCluster)
        .withBalancerStrategy(new CostBalancerStrategy(balancerExecutor))
        .withSegmentAssignerUsing(loadQueueManager)
        .build();

    CoordinatorRunStats stats = runDutyAndGetStats(params);
    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.DROPPED, "normal", DATASOURCE));

    EasyMock.verify(mockPeon);
    EasyMock.verify(anotherMockPeon);
  }

  /**
   * Nodes:
   * hot - 2 replicants
   */
  @Test
  public void testNoThrottleWhenSegmentNotLoadedInTier()
  {
    mockPeon.loadSegment(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    mockEmptyPeon();

    EasyMock
        .expect(databaseRuleManager.getRulesWithDefault(EasyMock.anyObject()))
        .andReturn(
            Collections.singletonList(
                new IntervalLoadRule(
                    Intervals.of("2012-01-01T00:00:00.000Z/2013-01-01T00:00:00.000Z"),
                    ImmutableMap.of("hot", 2),
                    null
                )
            )
        )
        .atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    DruidCluster druidCluster = DruidCluster
        .builder()
        .addTier(
            "hot",
            new ServerHolder(
                createHistorical("serverHot", "hot").toImmutableDruidServer(),
                mockPeon
            ),
            new ServerHolder(
                createHistorical("serverHot2", "hot").toImmutableDruidServer(),
                mockPeon
            )
        )
        .build();

    final CostBalancerStrategy balancerStrategy = new CostBalancerStrategy(balancerExecutor);
    DruidCoordinatorRuntimeParams params =
        createCoordinatorRuntimeParams(druidCluster)
            .withBalancerStrategy(balancerStrategy)
            .withSegmentAssignerUsing(loadQueueManager)
            .build();

    CoordinatorRunStats stats = runDutyAndGetStats(params);
    Assert.assertEquals(48L, stats.getSegmentStat(Stats.Segments.ASSIGNED, "hot", DATASOURCE));
    Assert.assertFalse(stats.hasStat(Stats.Segments.DROPPED));

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

    stats = runDutyAndGetStats(
        createCoordinatorRuntimeParams(druidCluster)
            .withUsedSegmentsInTest(overFlowSegment)
            .withBalancerStrategy(balancerStrategy)
            .withSegmentAssignerUsing(loadQueueManager)
            .build()
    );

    Assert.assertEquals(2L, stats.getSegmentStat(Stats.Segments.ASSIGNED, "hot", DATASOURCE));

    EasyMock.verify(mockPeon);
  }

  /**
   * Nodes:
   * hot - nothing loaded
   * _default_tier - 1 segment loaded
   */
  @Test
  public void testReplicantThrottleAcrossTiers()
  {
    EasyMock.expect(segmentsMetadataManager.markSegmentAsUnused(EasyMock.anyObject()))
            .andReturn(true).anyTimes();
    EasyMock.replay(segmentsMetadataManager);
    mockPeon.loadSegment(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    mockEmptyPeon();

    EasyMock
        .expect(databaseRuleManager.getRulesWithDefault(EasyMock.anyObject()))
        .andReturn(
            Collections.singletonList(
                new IntervalLoadRule(
                    Intervals.of("2012-01-01/2013-01-01"),
                    ImmutableMap.of("hot", 1, DruidServer.DEFAULT_TIER, 1),
                    null
                )
            )
        )
        .atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    final DruidCluster druidCluster = DruidCluster
        .builder()
        .addTier(
            "hot",
            new ServerHolder(
                createHistorical("serverHot", "hot").toImmutableDruidServer(),
                mockPeon
            )
        )
        .addTier(
            DruidServer.DEFAULT_TIER,
            new ServerHolder(
                createHistorical("serverNorm", "normal").toImmutableDruidServer(),
                mockPeon
            )
        )
        .build();

    DruidCoordinatorRuntimeParams params = createCoordinatorRuntimeParams(druidCluster)
        .withBalancerStrategy(new CostBalancerStrategy(balancerExecutor))
        .withDynamicConfigs(CoordinatorDynamicConfig.builder().withReplicationThrottleLimit(7).build())
        .withSegmentAssignerUsing(loadQueueManager)
        .build();

    CoordinatorRunStats stats = runDutyAndGetStats(params);
    Assert.assertEquals(24L, stats.getSegmentStat(Stats.Segments.ASSIGNED, "hot", DATASOURCE));
    Assert.assertEquals(24L, stats.getSegmentStat(Stats.Segments.ASSIGNED, DruidServer.DEFAULT_TIER, DATASOURCE));
    Assert.assertFalse(stats.hasStat(Stats.Segments.DROPPED));

    EasyMock.verify(mockPeon);
  }

  @Test
  public void testDropReplicantThrottle()
  {
    mockPeon.dropSegment(EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    mockEmptyPeon();

    EasyMock
        .expect(databaseRuleManager.getRulesWithDefault(EasyMock.anyObject()))
        .andReturn(
            Collections.singletonList(
                new IntervalLoadRule(
                    Intervals.of("2012-01-01/2013-01-02"),
                    ImmutableMap.of("normal", 1),
                    null
                )
            )
        )
        .atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    DataSegment overFlowSegment = new DataSegment(
        "test",
        Intervals.of("2012-02-01/2012-02-02"),
        DateTimes.nowUtc().toString(),
        Collections.emptyMap(),
        Collections.emptyList(),
        Collections.emptyList(),
        NoneShardSpec.instance(),
        1,
        0
    );
    List<DataSegment> longerUsedSegments = Lists.newArrayList(usedSegments);
    longerUsedSegments.add(overFlowSegment);

    DruidServer server1 = createHistorical("serverNorm1", "normal");
    for (DataSegment segment : longerUsedSegments) {
      server1.addDataSegment(segment);
    }
    DruidServer server2 = createHistorical("serverNorm2", "normal");
    for (DataSegment segment : longerUsedSegments) {
      server2.addDataSegment(segment);
    }

    DruidCluster druidCluster =
        DruidCluster.builder()
                    .add(new ServerHolder(server1.toImmutableDruidServer(), mockPeon))
                    .add(new ServerHolder(server2.toImmutableDruidServer(), mockPeon))
                    .build();

    DruidCoordinatorRuntimeParams params = createCoordinatorRuntimeParams(druidCluster)
        .withUsedSegmentsInTest(longerUsedSegments)
        .withBalancerStrategy(new CostBalancerStrategy(balancerExecutor))
        .withSegmentAssignerUsing(loadQueueManager)
        .build();

    CoordinatorRunStats stats = runDutyAndGetStats(params);

    // There is no throttling on drop
    Assert.assertEquals(25L, stats.getSegmentStat(Stats.Segments.DROPPED, "normal", DATASOURCE));
    EasyMock.verify(mockPeon);
  }

  @Test
  public void testRulesRunOnNonOvershadowedSegmentsOnly()
  {
    Set<DataSegment> usedSegments = new HashSet<>();
    DataSegment v1 = new DataSegment(
        "test",
        Intervals.of("2012-01-01/2012-01-02"),
        "1",
        Collections.emptyMap(),
        Collections.emptyList(),
        Collections.emptyList(),
        NoneShardSpec.instance(),
        IndexIO.CURRENT_VERSION_ID,
        1
    );
    DataSegment v2 = new DataSegment(
        "test",
        Intervals.of("2012-01-01/2012-01-02"),
        "2",
        Collections.emptyMap(),
        Collections.emptyList(),
        Collections.emptyList(),
        NoneShardSpec.instance(),
        IndexIO.CURRENT_VERSION_ID,
        1
    );
    usedSegments.add(v1);
    usedSegments.add(v2);

    mockPeon.loadSegment(EasyMock.eq(v2), EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().once();
    mockEmptyPeon();

    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.anyObject())).andReturn(
        Collections.singletonList(new ForeverLoadRule(ImmutableMap.of(DruidServer.DEFAULT_TIER, 1), null))).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    DruidCluster druidCluster = DruidCluster.builder().add(
        createServerHolder("serverHot", DruidServer.DEFAULT_TIER, mockPeon)
    ).build();

    DruidCoordinatorRuntimeParams params = createCoordinatorRuntimeParams(druidCluster)
        .withUsedSegmentsInTest(usedSegments)
        .withBalancerStrategy(new CostBalancerStrategy(balancerExecutor))
        .withDynamicConfigs(CoordinatorDynamicConfig.builder().withMaxSegmentsToMove(5).build())
        .withSegmentAssignerUsing(loadQueueManager)
        .build();

    CoordinatorRunStats stats = runDutyAndGetStats(params);
    Assert.assertEquals(1, stats.getSegmentStat(Stats.Segments.ASSIGNED, DruidServer.DEFAULT_TIER, DATASOURCE));
    Assert.assertFalse(stats.hasStat(Stats.Segments.DROPPED));

    Assert.assertEquals(2, usedSegments.size());
    Assert.assertEquals(usedSegments, params.getUsedSegments());

    EasyMock.verify(mockPeon);
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
    mockEmptyPeon();

    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.anyObject())).andReturn(
        Collections.singletonList(
            new ForeverLoadRule(ImmutableMap.of(DruidServer.DEFAULT_TIER, 3), null)
        )).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    DataSegment dataSegment = new DataSegment(
        "test",
        Intervals.utc(0, 1),
        DateTimes.nowUtc().toString(),
        Collections.emptyMap(),
        Collections.emptyList(),
        Collections.emptyList(),
        NoneShardSpec.instance(),
        IndexIO.CURRENT_VERSION_ID,
        1
    );

    DruidCluster druidCluster = DruidCluster.builder().addTier(
        DruidServer.DEFAULT_TIER,
        new ServerHolder(
            createHistorical("server1", DruidServer.DEFAULT_TIER)
                .addDataSegment(dataSegment)
                .toImmutableDruidServer(),
            mockPeon
        ),
        new ServerHolder(
            createHistorical("server2", DruidServer.DEFAULT_TIER)
                .addDataSegment(dataSegment)
                .toImmutableDruidServer(),
            mockPeon
        )
    ).build();

    DruidCoordinatorRuntimeParams params =
        createCoordinatorRuntimeParams(druidCluster, dataSegment)
            .withBalancerStrategy(new RandomBalancerStrategy())
            .withDynamicConfigs(CoordinatorDynamicConfig.builder().withMaxSegmentsToMove(5).build())
            .withSegmentAssignerUsing(loadQueueManager)
            .build();

    CoordinatorRunStats stats = runDutyAndGetStats(params);
    Assert.assertEquals(0L, stats.getSegmentStat(Stats.Segments.ASSIGNED, DruidServer.DEFAULT_TIER, DATASOURCE));
    Assert.assertFalse(stats.hasStat(Stats.Segments.DROPPED));

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
    mockPeon.loadSegment(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    mockEmptyPeon();

    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.anyObject())).andReturn(
        Collections.singletonList(
            new ForeverLoadRule(ImmutableMap.of(DruidServer.DEFAULT_TIER, 1), null)
        )).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    DataSegment dataSegment = new DataSegment(
        "test",
        Intervals.utc(0, 1),
        DateTimes.nowUtc().toString(),
        Collections.emptyMap(),
        Collections.emptyList(),
        Collections.emptyList(),
        NoneShardSpec.instance(),
        IndexIO.CURRENT_VERSION_ID,
        1
    );

    DruidCluster druidCluster = DruidCluster.builder().addTier(
        DruidServer.DEFAULT_TIER,
        new ServerHolder(
            createHistorical("server1", DruidServer.DEFAULT_TIER).toImmutableDruidServer(),
            mockPeon
        )
    ).build();

    DruidCoordinatorRuntimeParams params =
        createCoordinatorRuntimeParams(druidCluster, dataSegment)
            .withBalancerStrategy(new RandomBalancerStrategy())
            .withDynamicConfigs(CoordinatorDynamicConfig.builder().withMaxSegmentsToMove(5).build())
            .withSegmentAssignerUsing(loadQueueManager)
            .build();

    CoordinatorRunStats stats = runDutyAndGetStats(params);
    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.ASSIGNED, DruidServer.DEFAULT_TIER, DATASOURCE));
    Assert.assertFalse(stats.hasStat(Stats.Segments.DROPPED));

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
    mockEmptyPeon();
    int numReplicants = 1;
    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.anyObject())).andReturn(
        Collections.singletonList(
            new ForeverLoadRule(
                ImmutableMap.of(DruidServer.DEFAULT_TIER, numReplicants),
                null
            )
        )).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    final DataSegment dataSegment = new DataSegment(
        "test",
        Intervals.utc(0, 1),
        DateTimes.nowUtc().toString(),
        Collections.emptyMap(),
        Collections.emptyList(),
        Collections.emptyList(),
        NoneShardSpec.instance(),
        IndexIO.CURRENT_VERSION_ID,
        11
    );

    DruidCluster druidCluster = DruidCluster.builder().addTier(
        DruidServer.DEFAULT_TIER,
        new ServerHolder(
            new DruidServer("server1", "host1", null, 10, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 0)
                .toImmutableDruidServer(),
            mockPeon
        )
    ).build();

    DruidCoordinatorRuntimeParams params =
        createCoordinatorRuntimeParams(druidCluster, dataSegment)
            .withBalancerStrategy(new RandomBalancerStrategy())
            .withDynamicConfigs(CoordinatorDynamicConfig.builder().withMaxSegmentsToMove(5).build())
            .withSegmentAssignerUsing(loadQueueManager)
            .build();

    CoordinatorRunStats stats = runDutyAndGetStats(params);
    final RowKey tierRowKey = RowKey.of(Dimension.TIER, DruidServer.DEFAULT_TIER);
    Assert.assertEquals(
        dataSegment.getSize() * numReplicants,
        stats.get(Stats.Tier.REQUIRED_CAPACITY, tierRowKey)
    );

    // Verify that primary assignment failed
    Assert.assertEquals(0L, stats.getSegmentStat(Stats.Segments.ASSIGNED, DruidServer.DEFAULT_TIER, DATASOURCE));
    Assert.assertFalse(stats.hasStat(Stats.Segments.DROPPED));

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
    mockEmptyPeon();
    int numReplicants = 1;
    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.anyObject())).andReturn(
        Collections.singletonList(
            new ForeverLoadRule(
                ImmutableMap.of(DruidServer.DEFAULT_TIER, numReplicants),
                null
            )
        )).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    DataSegment dataSegment = new DataSegment(
        "test",
        Intervals.utc(0, 1),
        DateTimes.nowUtc().toString(),
        Collections.emptyMap(),
        Collections.emptyList(),
        Collections.emptyList(),
        NoneShardSpec.instance(),
        IndexIO.CURRENT_VERSION_ID,
        11
    );

    DruidCluster druidCluster = DruidCluster.builder().addTier(
        DruidServer.DEFAULT_TIER,
        new ServerHolder(
            new DruidServer("server1", "host1", null, 10, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 0)
                .toImmutableDruidServer(),
            mockPeon
        )
    ).build();

    DruidCoordinatorRuntimeParams params =
        createCoordinatorRuntimeParams(druidCluster, dataSegment)
            .withBalancerStrategy(new CostBalancerStrategy(balancerExecutor))
            .withDynamicConfigs(CoordinatorDynamicConfig.builder().withMaxSegmentsToMove(5).build())
            .withSegmentAssignerUsing(loadQueueManager)
            .build();

    CoordinatorRunStats stats = runDutyAndGetStats(params);
    final RowKey tierRowKey = RowKey.of(Dimension.TIER, DruidServer.DEFAULT_TIER);
    Assert.assertEquals(
        dataSegment.getSize() * numReplicants,
        stats.get(Stats.Tier.REQUIRED_CAPACITY, tierRowKey)
    );
    Assert.assertEquals(0L, stats.getSegmentStat(Stats.Segments.ASSIGNED, DruidServer.DEFAULT_TIER, DATASOURCE));
    Assert.assertFalse(stats.hasStat(Stats.Segments.DROPPED));

    EasyMock.verify(mockPeon);
  }

  @Test
  public void testSegmentWithZeroRequiredReplicasHasZeroReplicationFactor()
  {
    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.anyObject())).andReturn(
        Collections.singletonList(
            new ForeverLoadRule(Collections.emptyMap(), false)
        )
    ).anyTimes();
    EasyMock.replay(databaseRuleManager);

    final DruidCluster cluster = DruidCluster
        .builder()
        .add(createServerHolder("server", "normal", new TestLoadQueuePeon()))
        .build();

    final DataSegment segment = usedSegments.get(0);
    DruidCoordinatorRuntimeParams params = createCoordinatorRuntimeParams(cluster, segment)
        .withBalancerStrategy(new RandomBalancerStrategy())
        .withSegmentAssignerUsing(loadQueueManager)
        .build();
    params = ruleRunner.run(params);

    Assert.assertNotNull(params);
    SegmentReplicationStatus replicationStatus = params.getSegmentReplicationStatus();
    Assert.assertNotNull(replicationStatus);

    SegmentReplicaCount replicaCounts = replicationStatus.getReplicaCountsInCluster(segment.getId());
    Assert.assertNotNull(replicaCounts);
    Assert.assertEquals(0, replicaCounts.required());
    Assert.assertEquals(0, replicaCounts.totalLoaded());
    Assert.assertEquals(0, replicaCounts.requiredAndLoadable());
  }

  private CoordinatorRunStats runDutyAndGetStats(DruidCoordinatorRuntimeParams params)
  {
    params = ruleRunner.run(params);
    return params.getCoordinatorStats();
  }

  private void mockEmptyPeon()
  {
    EasyMock.expect(mockPeon.getSegmentsToLoad()).andReturn(Collections.emptySet()).anyTimes();
    EasyMock.expect(mockPeon.getSegmentsMarkedToDrop()).andReturn(Collections.emptySet()).anyTimes();
    EasyMock.expect(mockPeon.getSegmentsInQueue()).andReturn(Collections.emptySet()).anyTimes();
    EasyMock.replay(mockPeon);
  }

}
