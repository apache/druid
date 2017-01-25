/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server.coordinator;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceEventBuilder;
import io.druid.client.DruidServer;
import io.druid.metadata.MetadataRuleManager;
import io.druid.segment.IndexIO;
import io.druid.server.coordinator.helper.DruidCoordinatorRuleRunner;
import io.druid.server.coordinator.rules.ForeverLoadRule;
import io.druid.server.coordinator.rules.IntervalDropRule;
import io.druid.server.coordinator.rules.IntervalLoadRule;
import io.druid.server.coordinator.rules.Rule;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;

/**
 */
public class DruidCoordinatorRuleRunnerTest
{
  private DruidCoordinator coordinator;
  private LoadQueuePeon mockPeon;
  private List<DataSegment> availableSegments;
  private DruidCoordinatorRuleRunner ruleRunner;
  private ServiceEmitter emitter;
  private MetadataRuleManager databaseRuleManager;

  @Before
  public void setUp()
  {
    coordinator = EasyMock.createMock(DruidCoordinator.class);
    mockPeon = EasyMock.createMock(LoadQueuePeon.class);
    emitter = EasyMock.createMock(ServiceEmitter.class);
    EmittingLogger.registerEmitter(emitter);
    databaseRuleManager = EasyMock.createMock(MetadataRuleManager.class);

    DateTime start = new DateTime("2012-01-01");
    availableSegments = Lists.newArrayList();
    for (int i = 0; i < 24; i++) {
      availableSegments.add(
          new DataSegment(
              "test",
              new Interval(start, start.plusHours(1)),
              new DateTime().toString(),
              Maps.<String, Object>newHashMap(),
              Lists.<String>newArrayList(),
              Lists.<String>newArrayList(),
              NoneShardSpec.instance(),
              IndexIO.CURRENT_VERSION_ID,
              1
          )
      );
      start = start.plusHours(1);
    }

    ruleRunner = new DruidCoordinatorRuleRunner(new ReplicationThrottler(24, 1), coordinator);
  }

  @After
  public void tearDown() throws Exception
  {
    EasyMock.verify(coordinator);
    EasyMock.verify(databaseRuleManager);
  }

  /**
   * Nodes:
   * hot - 1 replicant
   * normal - 1 replicant
   * cold - 1 replicant
   *
   * @throws Exception
   */
  @Test
  public void testRunThreeTiersOneReplicant() throws Exception
  {
    mockCoordinator();
    mockPeon.loadSegment(EasyMock.<DataSegment>anyObject(), EasyMock.<LoadPeonCallback>anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.expect(mockPeon.getSegmentsToLoad()).andReturn(Sets.<DataSegment>newHashSet()).atLeastOnce();
    EasyMock.expect(mockPeon.getLoadQueueSize()).andReturn(0L).atLeastOnce();
    EasyMock.replay(mockPeon);

    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.<String>anyObject())).andReturn(
        Lists.<Rule>newArrayList(
            new IntervalLoadRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-01T06:00:00.000Z"), ImmutableMap.<String, Integer>of("hot", 1)),
            new IntervalLoadRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-01T12:00:00.000Z"), ImmutableMap.<String, Integer>of("normal", 1)),
            new IntervalLoadRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-02T00:00:00.000Z"), ImmutableMap.<String, Integer>of("cold", 1))
        )).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    DruidCluster druidCluster = new DruidCluster(
        ImmutableMap.of(
            "hot",
            MinMaxPriorityQueue.orderedBy(Ordering.natural().reverse()).create(
                Arrays.asList(
                    new ServerHolder(
                        new DruidServer(
                            "serverHot",
                            "hostHot",
                            1000,
                            "historical",
                            "hot",
                            0
                        ).toImmutableDruidServer(),
                        mockPeon
                    )
                )
            ),
            "normal",
            MinMaxPriorityQueue.orderedBy(Ordering.natural().reverse()).create(
                Arrays.asList(
                    new ServerHolder(
                        new DruidServer(
                            "serverNorm",
                            "hostNorm",
                            1000,
                            "historical",
                            "normal",
                            0
                        ).toImmutableDruidServer(),
                        mockPeon
                    )
                )
            ),
            "cold",
            MinMaxPriorityQueue.orderedBy(Ordering.natural().reverse()).create(
                Arrays.asList(
                    new ServerHolder(
                        new DruidServer(
                            "serverCold",
                            "hostCold",
                            1000,
                            "historical",
                            "cold",
                            0
                        ).toImmutableDruidServer(),
                        mockPeon
                    )
                )
            )
        )
    );

    ListeningExecutorService exec = MoreExecutors.listeningDecorator(
            Executors.newFixedThreadPool(1));
    BalancerStrategy balancerStrategy =
            new CostBalancerStrategyFactory().createBalancerStrategy(exec);

    DruidCoordinatorRuntimeParams params =
        new DruidCoordinatorRuntimeParams.Builder()
            .withDruidCluster(druidCluster)
            .withAvailableSegments(availableSegments)
            .withDatabaseRuleManager(databaseRuleManager)
            .withSegmentReplicantLookup(SegmentReplicantLookup.make(new DruidCluster()))
            .withBalancerStrategy(balancerStrategy)
            .withBalancerReferenceTimestamp(new DateTime("2013-01-01"))
            .withDynamicConfigs(new CoordinatorDynamicConfig.Builder().withMaxSegmentsToMove(5).build())
            .build();

    DruidCoordinatorRuntimeParams afterParams = ruleRunner.run(params);
    CoordinatorStats stats = afterParams.getCoordinatorStats();

    Assert.assertTrue(stats.getPerTierStats().get("assignedCount").get("hot").get() == 6);
    Assert.assertTrue(stats.getPerTierStats().get("assignedCount").get("normal").get() == 6);
    Assert.assertTrue(stats.getPerTierStats().get("assignedCount").get("cold").get() == 12);
    Assert.assertTrue(stats.getPerTierStats().get("unassignedCount") == null);
    Assert.assertTrue(stats.getPerTierStats().get("unassignedSize") == null);

    exec.shutdown();
    EasyMock.verify(mockPeon);
  }

  /**
   * Nodes:
   * hot - 2 replicants
   * cold - 1 replicant
   *
   * @throws Exception
   */
  @Test
  public void testRunTwoTiersTwoReplicants() throws Exception
  {
    mockCoordinator();
    mockPeon.loadSegment(EasyMock.<DataSegment>anyObject(), EasyMock.<LoadPeonCallback>anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.expect(mockPeon.getSegmentsToLoad()).andReturn(Sets.<DataSegment>newHashSet()).atLeastOnce();
    EasyMock.expect(mockPeon.getLoadQueueSize()).andReturn(0L).atLeastOnce();
    EasyMock.replay(mockPeon);

    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.<String>anyObject())).andReturn(
        Lists.<Rule>newArrayList(
            new IntervalLoadRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-01T06:00:00.000Z"), ImmutableMap.<String, Integer>of("hot", 2)),
            new IntervalLoadRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-02T00:00:00.000Z"), ImmutableMap.<String, Integer>of("cold", 1))
        )
    ).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    DruidCluster druidCluster = new DruidCluster(
        ImmutableMap.of(
            "hot",
            MinMaxPriorityQueue.orderedBy(Ordering.natural().reverse()).create(
                Arrays.asList(
                    new ServerHolder(
                        new DruidServer(
                            "serverHot",
                            "hostHot",
                            1000,
                            "historical",
                            "hot",
                            0
                        ).toImmutableDruidServer(),
                        mockPeon
                    ),
                    new ServerHolder(
                        new DruidServer(
                            "serverHot2",
                            "hostHot2",
                            1000,
                            "historical",
                            "hot",
                            0
                        ).toImmutableDruidServer(),
                        mockPeon
                    )
                )
            ),
            "cold",
            MinMaxPriorityQueue.orderedBy(Ordering.natural().reverse()).create(
                Arrays.asList(
                    new ServerHolder(
                        new DruidServer(
                            "serverCold",
                            "hostCold",
                            1000,
                            "historical",
                            "cold",
                            0
                        ).toImmutableDruidServer(),
                        mockPeon
                    )
                )
            )
        )
    );

    ListeningExecutorService exec = MoreExecutors.listeningDecorator(
            Executors.newFixedThreadPool(1));
    BalancerStrategy balancerStrategy =
            new CostBalancerStrategyFactory().createBalancerStrategy(exec);

    DruidCoordinatorRuntimeParams params =
        new DruidCoordinatorRuntimeParams.Builder()
            .withDruidCluster(druidCluster)
            .withAvailableSegments(availableSegments)
            .withDatabaseRuleManager(databaseRuleManager)
            .withSegmentReplicantLookup(SegmentReplicantLookup.make(new DruidCluster()))
            .withBalancerStrategy(balancerStrategy)
            .withBalancerReferenceTimestamp(new DateTime("2013-01-01"))
            .build();

    DruidCoordinatorRuntimeParams afterParams = ruleRunner.run(params);
    CoordinatorStats stats = afterParams.getCoordinatorStats();

    Assert.assertTrue(stats.getPerTierStats().get("assignedCount").get("hot").get() == 12);
    Assert.assertTrue(stats.getPerTierStats().get("assignedCount").get("cold").get() == 18);
    Assert.assertTrue(stats.getPerTierStats().get("unassignedCount") == null);
    Assert.assertTrue(stats.getPerTierStats().get("unassignedSize") == null);

    exec.shutdown();
    EasyMock.verify(mockPeon);
  }

  /**
   * Nodes:
   * hot - 1 replicant
   * normal - 1 replicant
   *
   * @throws Exception
   */
  @Test
  public void testRunTwoTiersWithExistingSegments() throws Exception
  {
    mockCoordinator();
    mockPeon.loadSegment(EasyMock.<DataSegment>anyObject(), EasyMock.<LoadPeonCallback>anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.expect(mockPeon.getSegmentsToLoad()).andReturn(Sets.<DataSegment>newHashSet()).atLeastOnce();
    EasyMock.expect(mockPeon.getLoadQueueSize()).andReturn(0L).atLeastOnce();
    EasyMock.replay(mockPeon);

    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.<String>anyObject())).andReturn(
        Lists.<Rule>newArrayList(
            new IntervalLoadRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-01T12:00:00.000Z"), ImmutableMap.<String, Integer>of("hot", 1)),
            new IntervalLoadRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-02T00:00:00.000Z"), ImmutableMap.<String, Integer>of("normal", 1))
        )
    ).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    DruidServer normServer = new DruidServer(
        "serverNorm",
        "hostNorm",
        1000,
        "historical",
        "normal",
        0
    );
    for (DataSegment availableSegment : availableSegments) {
      normServer.addDataSegment(availableSegment.getIdentifier(), availableSegment);
    }

    DruidCluster druidCluster = new DruidCluster(
        ImmutableMap.of(
            "hot",
            MinMaxPriorityQueue.orderedBy(Ordering.natural().reverse()).create(
                Arrays.asList(
                    new ServerHolder(
                        new DruidServer(
                            "serverHot",
                            "hostHot",
                            1000,
                            "historical",
                            "hot",
                            0
                        ).toImmutableDruidServer(),
                        mockPeon
                    )
                )
            ),
            "normal",
            MinMaxPriorityQueue.orderedBy(Ordering.natural().reverse()).create(
                Arrays.asList(
                    new ServerHolder(
                        normServer.toImmutableDruidServer(),
                        mockPeon
                    )
                )
            )
        )
    );

    SegmentReplicantLookup segmentReplicantLookup = SegmentReplicantLookup.make(druidCluster);

    ListeningExecutorService exec = MoreExecutors.listeningDecorator(
            Executors.newFixedThreadPool(1));
    BalancerStrategy balancerStrategy =
            new CostBalancerStrategyFactory().createBalancerStrategy(exec);

    DruidCoordinatorRuntimeParams params =
        new DruidCoordinatorRuntimeParams.Builder()
            .withDruidCluster(druidCluster)
            .withAvailableSegments(availableSegments)
            .withDatabaseRuleManager(databaseRuleManager)
            .withSegmentReplicantLookup(segmentReplicantLookup)
            .withBalancerStrategy(balancerStrategy)
            .withBalancerReferenceTimestamp(new DateTime("2013-01-01"))
            .build();

    DruidCoordinatorRuntimeParams afterParams = ruleRunner.run(params);
    CoordinatorStats stats = afterParams.getCoordinatorStats();

    Assert.assertTrue(stats.getPerTierStats().get("assignedCount").get("hot").get() == 12);
    Assert.assertTrue(stats.getPerTierStats().get("assignedCount").get("normal").get() == 0);
    Assert.assertTrue(stats.getPerTierStats().get("unassignedCount") == null);
    Assert.assertTrue(stats.getPerTierStats().get("unassignedSize") == null);

    exec.shutdown();
    EasyMock.verify(mockPeon);
  }

  @Test
  public void testRunTwoTiersTierDoesNotExist() throws Exception
  {
    mockCoordinator();
    mockPeon.loadSegment(EasyMock.<DataSegment>anyObject(), EasyMock.<LoadPeonCallback>anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.expect(mockPeon.getSegmentsToLoad()).andReturn(Sets.<DataSegment>newHashSet()).atLeastOnce();
    EasyMock.expect(mockPeon.getLoadQueueSize()).andReturn(0L).atLeastOnce();
    EasyMock.replay(mockPeon);

    emitter.emit(EasyMock.<ServiceEventBuilder>anyObject());
    EasyMock.expectLastCall().times(12);
    EasyMock.replay(emitter);

    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.<String>anyObject())).andReturn(
        Lists.<Rule>newArrayList(
            new IntervalLoadRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-01T12:00:00.000Z"), ImmutableMap.<String, Integer>of("hot",1)),
            new IntervalLoadRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-02T00:00:00.000Z"), ImmutableMap.<String, Integer>of("normal", 1))
        )
    ).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    DruidCluster druidCluster = new DruidCluster(
        ImmutableMap.of(
            "normal",
            MinMaxPriorityQueue.orderedBy(Ordering.natural().reverse()).create(
                Arrays.asList(
                    new ServerHolder(
                        new DruidServer(
                            "serverNorm",
                            "hostNorm",
                            1000,
                            "historical",
                            "normal",
                            0
                        ).toImmutableDruidServer(),
                        mockPeon
                    )
                )
            )
        )
    );

    ListeningExecutorService exec = MoreExecutors.listeningDecorator(
            Executors.newFixedThreadPool(1));
    BalancerStrategy balancerStrategy =
            new CostBalancerStrategyFactory().createBalancerStrategy(exec);

    DruidCoordinatorRuntimeParams params =
        new DruidCoordinatorRuntimeParams.Builder()
            .withEmitter(emitter)
            .withDruidCluster(druidCluster)
            .withAvailableSegments(availableSegments)
            .withDatabaseRuleManager(databaseRuleManager)
            .withSegmentReplicantLookup(SegmentReplicantLookup.make(new DruidCluster()))
            .withBalancerStrategy(balancerStrategy)
            .withBalancerReferenceTimestamp(new DateTime("2013-01-01"))
            .build();

    ruleRunner.run(params);

    exec.shutdown();
    EasyMock.verify(emitter);
    EasyMock.verify(mockPeon);
  }

  @Test
  public void testRunRuleDoesNotExist() throws Exception
  {
    mockCoordinator();
    emitter.emit(EasyMock.<ServiceEventBuilder>anyObject());
    EasyMock.expectLastCall().times(1);
    EasyMock.replay(emitter);

    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.<String>anyObject())).andReturn(
        Lists.<Rule>newArrayList(
            new IntervalLoadRule(new Interval("2012-01-02T00:00:00.000Z/2012-01-03T00:00:00.000Z"), ImmutableMap.<String, Integer>of("normal", 1))
        )
    ).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    DruidCluster druidCluster = new DruidCluster(
        ImmutableMap.of(
            "normal",
            MinMaxPriorityQueue.orderedBy(Ordering.natural().reverse()).create(
                Arrays.asList(
                    new ServerHolder(
                        new DruidServer(
                            "serverNorm",
                            "hostNorm",
                            1000,
                            "historical",
                            "normal",
                            0
                        ).toImmutableDruidServer(),
                        mockPeon
                    )
                )
            )
        )
    );

    DruidCoordinatorRuntimeParams params =
        new DruidCoordinatorRuntimeParams.Builder()
            .withEmitter(emitter)
            .withDruidCluster(druidCluster)
            .withAvailableSegments(availableSegments)
            .withDatabaseRuleManager(databaseRuleManager)
            .withSegmentReplicantLookup(SegmentReplicantLookup.make(new DruidCluster()))
            .build();

    ruleRunner.run(params);

    EasyMock.verify(emitter);
  }

  @Test
  public void testDropRemove() throws Exception
  {
    mockPeon.dropSegment(EasyMock.<DataSegment>anyObject(), EasyMock.<LoadPeonCallback>anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.expect(mockPeon.getSegmentsToLoad()).andReturn(Sets.<DataSegment>newHashSet()).atLeastOnce();
    EasyMock.expect(mockPeon.getLoadQueueSize()).andReturn(0L).atLeastOnce();
    EasyMock.replay(mockPeon);

    EasyMock.expect(coordinator.getDynamicConfigs()).andReturn(
        new CoordinatorDynamicConfig(
            0, 0, 0, 0, 1, 24, 0, false, null, false
        )
    ).anyTimes();
    coordinator.removeSegment(EasyMock.<DataSegment>anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.replay(coordinator);

    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.<String>anyObject())).andReturn(
        Lists.<Rule>newArrayList(
            new IntervalLoadRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-01T12:00:00.000Z"), ImmutableMap.<String, Integer>of("normal", 1)),
            new IntervalDropRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-02T00:00:00.000Z"))
        )
    ).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    DruidServer server = new DruidServer(
        "serverNorm",
        "hostNorm",
        1000,
        "historical",
        "normal",
        0
    );
    for (DataSegment segment : availableSegments) {
      server.addDataSegment(segment.getIdentifier(), segment);
    }

    DruidCluster druidCluster = new DruidCluster(
        ImmutableMap.of(
            "normal",
            MinMaxPriorityQueue.orderedBy(Ordering.natural().reverse()).create(
                Arrays.asList(
                    new ServerHolder(
                        server.toImmutableDruidServer(),
                        mockPeon
                    )
                )
            )
        )
    );

    SegmentReplicantLookup segmentReplicantLookup = SegmentReplicantLookup.make(druidCluster);

    ListeningExecutorService exec = MoreExecutors.listeningDecorator(
            Executors.newFixedThreadPool(1));
    BalancerStrategy balancerStrategy =
            new CostBalancerStrategyFactory().createBalancerStrategy(exec);

    DruidCoordinatorRuntimeParams params = new DruidCoordinatorRuntimeParams.Builder()
        .withDruidCluster(druidCluster)
        .withDynamicConfigs(new CoordinatorDynamicConfig.Builder().withMillisToWaitBeforeDeleting(0L).build())
        .withAvailableSegments(availableSegments)
        .withDatabaseRuleManager(databaseRuleManager)
        .withSegmentReplicantLookup(segmentReplicantLookup)
        .withBalancerStrategy(balancerStrategy)
        .withBalancerReferenceTimestamp(new DateTime("2013-01-01"))
        .build();

    DruidCoordinatorRuntimeParams afterParams = ruleRunner.run(params);
    CoordinatorStats stats = afterParams.getCoordinatorStats();

    Assert.assertTrue(stats.getGlobalStats().get("deletedCount").get() == 12);

    exec.shutdown();
    EasyMock.verify(coordinator);
  }

  @Test
  public void testDropTooManyInSameTier() throws Exception
  {
    mockCoordinator();
    mockPeon.dropSegment(EasyMock.<DataSegment>anyObject(), EasyMock.<LoadPeonCallback>anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.expect(mockPeon.getSegmentsToLoad()).andReturn(Sets.<DataSegment>newHashSet()).atLeastOnce();
    EasyMock.expect(mockPeon.getLoadQueueSize()).andReturn(0L).atLeastOnce();
    EasyMock.replay(mockPeon);

    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.<String>anyObject())).andReturn(
        Lists.<Rule>newArrayList(
            new IntervalLoadRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-01T12:00:00.000Z"), ImmutableMap.<String, Integer>of("normal", 1)),
            new IntervalDropRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-02T00:00:00.000Z"))
        )
    ).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    DruidServer server1 = new DruidServer(
        "serverNorm",
        "hostNorm",
        1000,
        "historical",
        "normal",
        0
    );
    server1.addDataSegment(availableSegments.get(0).getIdentifier(), availableSegments.get(0));

    DruidServer server2 = new DruidServer(
        "serverNorm2",
        "hostNorm2",
        1000,
        "historical",
        "normal",
        0
    );
    for (DataSegment segment : availableSegments) {
      server2.addDataSegment(segment.getIdentifier(), segment);
    }

    DruidCluster druidCluster = new DruidCluster(
        ImmutableMap.of(
            "normal",
            MinMaxPriorityQueue.orderedBy(Ordering.natural().reverse()).create(
                Arrays.asList(
                    new ServerHolder(
                        server1.toImmutableDruidServer(),
                        mockPeon
                    ),
                    new ServerHolder(
                        server2.toImmutableDruidServer(),
                        mockPeon
                    )
                )
            )
        )
    );

    SegmentReplicantLookup segmentReplicantLookup = SegmentReplicantLookup.make(druidCluster);

    ListeningExecutorService exec = MoreExecutors.listeningDecorator(
            Executors.newFixedThreadPool(1));
    BalancerStrategy balancerStrategy =
            new CostBalancerStrategyFactory().createBalancerStrategy(exec);

    DruidCoordinatorRuntimeParams params = new DruidCoordinatorRuntimeParams.Builder()
        .withDruidCluster(druidCluster)
        .withDynamicConfigs(new CoordinatorDynamicConfig.Builder().withMillisToWaitBeforeDeleting(0L).build())
        .withAvailableSegments(availableSegments)
        .withDatabaseRuleManager(databaseRuleManager)
        .withSegmentReplicantLookup(segmentReplicantLookup)
        .withBalancerStrategy(balancerStrategy)
        .withBalancerReferenceTimestamp(new DateTime("2013-01-01"))
        .build();

    DruidCoordinatorRuntimeParams afterParams = ruleRunner.run(params);
    CoordinatorStats stats = afterParams.getCoordinatorStats();

    Assert.assertTrue(stats.getPerTierStats().get("droppedCount").get("normal").get() == 1);
    Assert.assertTrue(stats.getGlobalStats().get("deletedCount").get() == 12);

    exec.shutdown();
    EasyMock.verify(mockPeon);
  }

  @Test
  public void testDropTooManyInDifferentTiers() throws Exception
  {
    mockCoordinator();
    mockPeon.loadSegment(EasyMock.<DataSegment>anyObject(), EasyMock.<LoadPeonCallback>anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    mockPeon.dropSegment(EasyMock.<DataSegment>anyObject(), EasyMock.<LoadPeonCallback>anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.expect(mockPeon.getSegmentsToLoad()).andReturn(Sets.<DataSegment>newHashSet()).atLeastOnce();
    EasyMock.expect(mockPeon.getLoadQueueSize()).andReturn(0L).atLeastOnce();
    EasyMock.replay(mockPeon);

    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.<String>anyObject())).andReturn(
        Lists.<Rule>newArrayList(
            new IntervalLoadRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-01T12:00:00.000Z"), ImmutableMap.<String, Integer>of("hot", 1)),
            new IntervalDropRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-02T00:00:00.000Z"))
        )
    ).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    DruidServer server1 = new DruidServer(
        "server1",
        "host1",
        1000,
        "historical",
        "hot",
        0
    );
    server1.addDataSegment(availableSegments.get(0).getIdentifier(), availableSegments.get(0));
    DruidServer server2 = new DruidServer(
        "serverNorm2",
        "hostNorm2",
        1000,
        "historical",
        "normal",
        0
    );
    for (DataSegment segment : availableSegments) {
      server2.addDataSegment(segment.getIdentifier(), segment);
    }

    DruidCluster druidCluster = new DruidCluster(
        ImmutableMap.of(
            "hot",
            MinMaxPriorityQueue.orderedBy(Ordering.natural().reverse()).create(
                Arrays.asList(
                    new ServerHolder(
                        server1.toImmutableDruidServer(),
                        mockPeon
                    )
                )
            ),
            "normal",
            MinMaxPriorityQueue.orderedBy(Ordering.natural().reverse()).create(
                Arrays.asList(
                    new ServerHolder(
                        server2.toImmutableDruidServer(),
                        mockPeon
                    )
                )
            )
        )
    );

    SegmentReplicantLookup segmentReplicantLookup = SegmentReplicantLookup.make(druidCluster);

    ListeningExecutorService exec = MoreExecutors.listeningDecorator(
            Executors.newFixedThreadPool(1));
    BalancerStrategy balancerStrategy =
            new CostBalancerStrategyFactory().createBalancerStrategy(exec);

    DruidCoordinatorRuntimeParams params = new DruidCoordinatorRuntimeParams.Builder()
        .withDruidCluster(druidCluster)
        .withDynamicConfigs(new CoordinatorDynamicConfig.Builder().withMillisToWaitBeforeDeleting(0L).build())
        .withAvailableSegments(availableSegments)
        .withDatabaseRuleManager(databaseRuleManager)
        .withSegmentReplicantLookup(segmentReplicantLookup)
        .withBalancerStrategy(balancerStrategy)
        .withBalancerReferenceTimestamp(new DateTime("2013-01-01"))
        .build();

    DruidCoordinatorRuntimeParams afterParams = ruleRunner.run(params);
    CoordinatorStats stats = afterParams.getCoordinatorStats();

    Assert.assertTrue(stats.getPerTierStats().get("droppedCount").get("normal").get() == 1);
    Assert.assertTrue(stats.getGlobalStats().get("deletedCount").get() == 12);

    exec.shutdown();
    EasyMock.verify(mockPeon);
  }

  @Test
  public void testDontDropInDifferentTiers() throws Exception
  {
    mockCoordinator();
    mockPeon.loadSegment(EasyMock.<DataSegment>anyObject(), EasyMock.<LoadPeonCallback>anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.expect(mockPeon.getSegmentsToLoad()).andReturn(Sets.<DataSegment>newHashSet()).atLeastOnce();
    EasyMock.expect(mockPeon.getLoadQueueSize()).andReturn(0L).atLeastOnce();
    EasyMock.replay(mockPeon);

    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.<String>anyObject())).andReturn(
        Lists.<Rule>newArrayList(
            new IntervalLoadRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-01T12:00:00.000Z"), ImmutableMap.<String, Integer>of("hot", 1)),
            new IntervalDropRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-02T00:00:00.000Z"))
        )
    ).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    DruidServer server1 = new DruidServer(
        "server1",
        "host1",
        1000,
        "historical",
        "hot",
        0
    );
    DruidServer server2 = new DruidServer(
        "serverNorm2",
        "hostNorm2",
        1000,
        "historical",
        "normal",
        0
    );
    for (DataSegment segment : availableSegments) {
      server2.addDataSegment(segment.getIdentifier(), segment);
    }
    DruidCluster druidCluster = new DruidCluster(
        ImmutableMap.of(
            "hot",
            MinMaxPriorityQueue.orderedBy(Ordering.natural().reverse()).create(
                Arrays.asList(
                    new ServerHolder(
                        server1.toImmutableDruidServer(),
                        mockPeon
                    )
                )
            ),
            "normal",
            MinMaxPriorityQueue.orderedBy(Ordering.natural().reverse()).create(
                Arrays.asList(
                    new ServerHolder(
                        server2.toImmutableDruidServer(),
                        mockPeon
                    )
                )
            )
        )
    );

    SegmentReplicantLookup segmentReplicantLookup = SegmentReplicantLookup.make(druidCluster);

    ListeningExecutorService exec = MoreExecutors.listeningDecorator(
            Executors.newFixedThreadPool(1));
    BalancerStrategy balancerStrategy =
            new CostBalancerStrategyFactory().createBalancerStrategy(exec);

    DruidCoordinatorRuntimeParams params = new DruidCoordinatorRuntimeParams.Builder()
        .withDruidCluster(druidCluster)
        .withDynamicConfigs(new CoordinatorDynamicConfig.Builder().withMillisToWaitBeforeDeleting(0L).build())
        .withAvailableSegments(availableSegments)
        .withDatabaseRuleManager(databaseRuleManager)
        .withSegmentReplicantLookup(segmentReplicantLookup)
        .withBalancerStrategy(balancerStrategy)
        .withBalancerReferenceTimestamp(new DateTime("2013-01-01"))
        .build();

    DruidCoordinatorRuntimeParams afterParams = ruleRunner.run(params);
    CoordinatorStats stats = afterParams.getCoordinatorStats();

    Assert.assertTrue(stats.getPerTierStats().get("droppedCount") == null);
    Assert.assertTrue(stats.getGlobalStats().get("deletedCount").get() == 12);

    exec.shutdown();
    EasyMock.verify(mockPeon);
  }

  @Test
  public void testDropServerActuallyServesSegment() throws Exception
  {
    mockCoordinator();
    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.<String>anyObject())).andReturn(
        Lists.<Rule>newArrayList(
            new IntervalLoadRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-01T01:00:00.000Z"), ImmutableMap.<String, Integer>of("normal", 0))
        )
    ).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    DruidServer server1 = new DruidServer(
        "server1",
        "host1",
        1000,
        "historical",
        "normal",
        0
    );
    server1.addDataSegment(availableSegments.get(0).getIdentifier(), availableSegments.get(0));
    DruidServer server2 = new DruidServer(
        "serverNorm2",
        "hostNorm2",
        1000,
        "historical",
        "normal",
        0
    );
    server2.addDataSegment(availableSegments.get(1).getIdentifier(), availableSegments.get(1));
    DruidServer server3 = new DruidServer(
        "serverNorm3",
        "hostNorm3",
        1000,
        "historical",
        "normal",
        0
    );
    server3.addDataSegment(availableSegments.get(1).getIdentifier(), availableSegments.get(1));
    server3.addDataSegment(availableSegments.get(2).getIdentifier(), availableSegments.get(2));

    mockPeon.dropSegment(EasyMock.<DataSegment>anyObject(), EasyMock.<LoadPeonCallback>anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.expect(mockPeon.getSegmentsToLoad()).andReturn(Sets.<DataSegment>newHashSet()).atLeastOnce();
    EasyMock.expect(mockPeon.getLoadQueueSize()).andReturn(0L).atLeastOnce();
    EasyMock.replay(mockPeon);

    LoadQueuePeon anotherMockPeon = EasyMock.createMock(LoadQueuePeon.class);
    EasyMock.expect(anotherMockPeon.getSegmentsToLoad()).andReturn(Sets.<DataSegment>newHashSet()).atLeastOnce();
    EasyMock.expect(anotherMockPeon.getLoadQueueSize()).andReturn(10L).atLeastOnce();
    EasyMock.replay(anotherMockPeon);

    DruidCluster druidCluster = new DruidCluster(
        ImmutableMap.of(
            "normal",
            MinMaxPriorityQueue.orderedBy(Ordering.natural().reverse()).create(
                Arrays.asList(
                    new ServerHolder(
                        server1.toImmutableDruidServer(),
                        mockPeon
                    ),
                    new ServerHolder(
                        server2.toImmutableDruidServer(),
                        anotherMockPeon
                    ),
                    new ServerHolder(
                        server3.toImmutableDruidServer(),
                        anotherMockPeon
                    )
                )
            )
        )
    );

    SegmentReplicantLookup segmentReplicantLookup = SegmentReplicantLookup.make(druidCluster);

    ListeningExecutorService exec = MoreExecutors.listeningDecorator(
            Executors.newFixedThreadPool(1));
    BalancerStrategy balancerStrategy =
            new CostBalancerStrategyFactory().createBalancerStrategy(exec);

    DruidCoordinatorRuntimeParams params = new DruidCoordinatorRuntimeParams.Builder()
        .withDruidCluster(druidCluster)
        .withDynamicConfigs(new CoordinatorDynamicConfig.Builder().withMillisToWaitBeforeDeleting(0L).build())
        .withAvailableSegments(availableSegments)
        .withDatabaseRuleManager(databaseRuleManager)
        .withSegmentReplicantLookup(segmentReplicantLookup)
        .withBalancerStrategy(balancerStrategy)
        .withBalancerReferenceTimestamp(new DateTime("2013-01-01"))
        .build();

    DruidCoordinatorRuntimeParams afterParams = ruleRunner.run(params);
    CoordinatorStats stats = afterParams.getCoordinatorStats();

    Assert.assertTrue(stats.getPerTierStats().get("droppedCount").get("normal").get() == 1);

    exec.shutdown();
    EasyMock.verify(mockPeon);
    EasyMock.verify(anotherMockPeon);
  }

  /**
   * Nodes:
   * hot - 2 replicants
   *
   * @throws Exception
   */
  @Test
  public void testReplicantThrottle() throws Exception
  {
    mockCoordinator();
    mockPeon.loadSegment(EasyMock.<DataSegment>anyObject(), EasyMock.<LoadPeonCallback>anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.expect(mockPeon.getSegmentsToLoad()).andReturn(Sets.<DataSegment>newHashSet()).atLeastOnce();
    EasyMock.expect(mockPeon.getLoadQueueSize()).andReturn(0L).atLeastOnce();
    EasyMock.replay(mockPeon);

    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.<String>anyObject())).andReturn(
        Lists.<Rule>newArrayList(
            new IntervalLoadRule(new Interval("2012-01-01T00:00:00.000Z/2013-01-01T00:00:00.000Z"), ImmutableMap.<String, Integer>of("hot", 2))
        )
    ).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    DruidCluster druidCluster = new DruidCluster(
        ImmutableMap.of(
            "hot",
            MinMaxPriorityQueue.orderedBy(Ordering.natural().reverse()).create(
                Arrays.asList(
                    new ServerHolder(
                        new DruidServer(
                            "serverHot",
                            "hostHot",
                            1000,
                            "historical",
                            "hot",
                            0
                        ).toImmutableDruidServer(),
                        mockPeon
                    ),
                    new ServerHolder(
                        new DruidServer(
                            "serverHot2",
                            "hostHot2",
                            1000,
                            "historical",
                            "hot",
                            0
                        ).toImmutableDruidServer(),
                        mockPeon
                    )
                )
            )
        )
    );

    ListeningExecutorService exec = MoreExecutors.listeningDecorator(
            Executors.newFixedThreadPool(1));
    BalancerStrategy balancerStrategy =
            new CostBalancerStrategyFactory().createBalancerStrategy(exec);

    DruidCoordinatorRuntimeParams params =
        new DruidCoordinatorRuntimeParams.Builder()
            .withDruidCluster(druidCluster)
            .withAvailableSegments(availableSegments)
            .withDatabaseRuleManager(databaseRuleManager)
            .withSegmentReplicantLookup(SegmentReplicantLookup.make(new DruidCluster()))
            .withBalancerStrategy(balancerStrategy)
            .withBalancerReferenceTimestamp(new DateTime("2013-01-01"))
            .build();

    DruidCoordinatorRuntimeParams afterParams = ruleRunner.run(params);
    CoordinatorStats stats = afterParams.getCoordinatorStats();

    Assert.assertTrue(stats.getPerTierStats().get("assignedCount").get("hot").get() == 48);
    Assert.assertTrue(stats.getPerTierStats().get("unassignedCount") == null);
    Assert.assertTrue(stats.getPerTierStats().get("unassignedSize") == null);

    DataSegment overFlowSegment = new DataSegment(
        "test",
        new Interval("2012-02-01/2012-02-02"),
        new DateTime().toString(),
        Maps.<String, Object>newHashMap(),
        Lists.<String>newArrayList(),
        Lists.<String>newArrayList(),
        NoneShardSpec.instance(),
        1,
        0
    );

    afterParams = ruleRunner.run(
        new DruidCoordinatorRuntimeParams.Builder()
            .withDruidCluster(druidCluster)
            .withEmitter(emitter)
            .withAvailableSegments(Arrays.asList(overFlowSegment))
            .withDatabaseRuleManager(databaseRuleManager)
            .withBalancerStrategy(balancerStrategy)
            .withBalancerReferenceTimestamp(new DateTime("2013-01-01"))
            .withSegmentReplicantLookup(SegmentReplicantLookup.make(new DruidCluster()))
            .build()
    );
    stats = afterParams.getCoordinatorStats();

    Assert.assertTrue(stats.getPerTierStats().get("assignedCount").get("hot").get() == 1);
    Assert.assertTrue(stats.getPerTierStats().get("unassignedCount") == null);
    Assert.assertTrue(stats.getPerTierStats().get("unassignedSize") == null);

    EasyMock.verify(mockPeon);
    exec.shutdown();
  }

  /**
   * Nodes:
   * hot - nothing loaded
   * _default_tier - 1 segment loaded
   *
   * @throws Exception
   */
  @Test
  public void testReplicantThrottleAcrossTiers() throws Exception
  {
    EasyMock.expect(coordinator.getDynamicConfigs()).andReturn(
        new CoordinatorDynamicConfig(
            0, 0, 0, 0, 1, 7, 0, false, null, false
        )
    ).atLeastOnce();
    coordinator.removeSegment(EasyMock.<DataSegment>anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.replay(coordinator);
    mockPeon.loadSegment(EasyMock.<DataSegment>anyObject(), EasyMock.<LoadPeonCallback>anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.expect(mockPeon.getSegmentsToLoad()).andReturn(Sets.<DataSegment>newHashSet()).atLeastOnce();
    EasyMock.expect(mockPeon.getLoadQueueSize()).andReturn(0L).atLeastOnce();
    EasyMock.replay(mockPeon);

    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.<String>anyObject())).andReturn(
        Lists.<Rule>newArrayList(
            new IntervalLoadRule(
                new Interval("2012-01-01T00:00:00.000Z/2013-01-01T00:00:00.000Z"),
                ImmutableMap.<String, Integer>of(
                    "hot", 1,
                    DruidServer.DEFAULT_TIER, 1
                )
            )
        )
    ).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    DruidCluster druidCluster = new DruidCluster(
        ImmutableMap.of(
            "hot",
            MinMaxPriorityQueue.orderedBy(Ordering.natural().reverse()).create(
                Arrays.asList(
                    new ServerHolder(
                        new DruidServer(
                            "serverHot",
                            "hostHot",
                            1000,
                            "historical",
                            "hot",
                            0
                        ).toImmutableDruidServer(),
                        mockPeon
                    )
                )
            ),
            DruidServer.DEFAULT_TIER,
            MinMaxPriorityQueue.orderedBy(Ordering.natural().reverse()).create(
                Arrays.asList(
                    new ServerHolder(
                        new DruidServer(
                            "serverNorm",
                            "hostNorm",
                            1000,
                            "historical",
                            DruidServer.DEFAULT_TIER,
                            0
                        ).toImmutableDruidServer(),
                        mockPeon
                    )
                )
            )
        )
    );

    ListeningExecutorService exec = MoreExecutors.listeningDecorator(
            Executors.newFixedThreadPool(1));
    BalancerStrategy balancerStrategy =
            new CostBalancerStrategyFactory().createBalancerStrategy(exec);

    DruidCoordinatorRuntimeParams params =
        new DruidCoordinatorRuntimeParams.Builder()
            .withDruidCluster(druidCluster)
            .withAvailableSegments(availableSegments)
            .withDatabaseRuleManager(databaseRuleManager)
            .withBalancerStrategy(balancerStrategy)
            .withBalancerReferenceTimestamp(new DateTime("2013-01-01"))
            .withSegmentReplicantLookup(SegmentReplicantLookup.make(new DruidCluster()))
            .build();

    DruidCoordinatorRuleRunner runner = new DruidCoordinatorRuleRunner(new ReplicationThrottler(7, 1), coordinator);
    DruidCoordinatorRuntimeParams afterParams = runner.run(params);
    CoordinatorStats stats = afterParams.getCoordinatorStats();

    Assert.assertTrue(stats.getPerTierStats().get("assignedCount").get("hot").get() == 24);
    Assert.assertTrue(stats.getPerTierStats().get("assignedCount").get(DruidServer.DEFAULT_TIER).get() == 7);
    Assert.assertTrue(stats.getPerTierStats().get("unassignedCount") == null);
    Assert.assertTrue(stats.getPerTierStats().get("unassignedSize") == null);

    EasyMock.verify(mockPeon);
    exec.shutdown();
  }

  @Test
  public void testDropReplicantThrottle() throws Exception
  {
    mockCoordinator();
    mockPeon.dropSegment(EasyMock.<DataSegment>anyObject(), EasyMock.<LoadPeonCallback>anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.expect(mockPeon.getSegmentsToLoad()).andReturn(Sets.<DataSegment>newHashSet()).atLeastOnce();
    EasyMock.expect(mockPeon.getLoadQueueSize()).andReturn(0L).atLeastOnce();
    EasyMock.replay(mockPeon);

    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.<String>anyObject())).andReturn(
        Lists.<Rule>newArrayList(
            new IntervalLoadRule(new Interval("2012-01-01T00:00:00.000Z/2013-01-02T00:00:00.000Z"), ImmutableMap.<String, Integer>of("normal", 1))
        )
    ).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    DataSegment overFlowSegment = new DataSegment(
        "test",
        new Interval("2012-02-01/2012-02-02"),
        new DateTime().toString(),
        Maps.<String, Object>newHashMap(),
        Lists.<String>newArrayList(),
        Lists.<String>newArrayList(),
        NoneShardSpec.instance(),
        1,
        0
    );
    List<DataSegment> longerAvailableSegments = Lists.newArrayList(availableSegments);
    longerAvailableSegments.add(overFlowSegment);

    DruidServer server1 = new DruidServer(
        "serverNorm1",
        "hostNorm1",
        1000,
        "historical",
        "normal",
        0
    );
    for (DataSegment availableSegment : longerAvailableSegments) {
      server1.addDataSegment(availableSegment.getIdentifier(), availableSegment);
    }
    DruidServer server2 = new DruidServer(
        "serverNorm2",
        "hostNorm2",
        1000,
        "historical",
        "normal",
        0
    );
    for (DataSegment availableSegment : longerAvailableSegments) {
      server2.addDataSegment(availableSegment.getIdentifier(), availableSegment);
    }

    DruidCluster druidCluster = new DruidCluster(
        ImmutableMap.of(
            "normal",
            MinMaxPriorityQueue.orderedBy(Ordering.natural().reverse()).create(
                Arrays.asList(
                    new ServerHolder(
                        server1.toImmutableDruidServer(),
                        mockPeon
                    ),
                    new ServerHolder(
                        server2.toImmutableDruidServer(),
                        mockPeon
                    )
                )
            )
        )
    );

    SegmentReplicantLookup segmentReplicantLookup = SegmentReplicantLookup.make(druidCluster);

    ListeningExecutorService exec = MoreExecutors.listeningDecorator(
            Executors.newFixedThreadPool(1));
    BalancerStrategy balancerStrategy =
            new CostBalancerStrategyFactory().createBalancerStrategy(exec);

    DruidCoordinatorRuntimeParams params = new DruidCoordinatorRuntimeParams.Builder()
        .withDruidCluster(druidCluster)
        .withDynamicConfigs(new CoordinatorDynamicConfig.Builder().withMillisToWaitBeforeDeleting(0L).build())
        .withAvailableSegments(longerAvailableSegments)
        .withDatabaseRuleManager(databaseRuleManager)
        .withSegmentReplicantLookup(segmentReplicantLookup)
        .withBalancerStrategy(balancerStrategy)
        .withBalancerReferenceTimestamp(new DateTime("2013-01-01"))
        .build();

    DruidCoordinatorRuntimeParams afterParams = ruleRunner.run(params);
    CoordinatorStats stats = afterParams.getCoordinatorStats();

    // There is no throttling on drop
    Assert.assertTrue(stats.getPerTierStats().get("droppedCount").get("normal").get() == 25);
    EasyMock.verify(mockPeon);
    exec.shutdown();
  }

  @Test
  public void testRulesRunOnNonOvershadowedSegmentsOnly() throws Exception
  {
    Set<DataSegment> availableSegments = new HashSet<>();
    DataSegment v1 = new DataSegment(
        "test",
        new Interval("2012-01-01/2012-01-02"),
        "1",
        Maps.<String, Object>newHashMap(),
        Lists.<String>newArrayList(),
        Lists.<String>newArrayList(),
        NoneShardSpec.instance(),
        IndexIO.CURRENT_VERSION_ID,
        1
    );
    DataSegment v2 = new DataSegment(
        "test",
        new Interval("2012-01-01/2012-01-02"),
        "2",
        Maps.<String, Object>newHashMap(),
        Lists.<String>newArrayList(),
        Lists.<String>newArrayList(),
        NoneShardSpec.instance(),
        IndexIO.CURRENT_VERSION_ID,
        1
    );
    availableSegments.add(v1);
    availableSegments.add(v2);

    mockCoordinator();
    mockPeon.loadSegment(EasyMock.eq(v2), EasyMock.<LoadPeonCallback>anyObject());
    EasyMock.expectLastCall().once();
    EasyMock.expect(mockPeon.getSegmentsToLoad()).andReturn(Sets.<DataSegment>newHashSet()).atLeastOnce();
    EasyMock.expect(mockPeon.getLoadQueueSize()).andReturn(0L).atLeastOnce();
    EasyMock.replay(mockPeon);

    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.<String>anyObject())).andReturn(
        Lists.<Rule>newArrayList(
            new ForeverLoadRule(ImmutableMap.of(DruidServer.DEFAULT_TIER, 1))
        )).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    DruidCluster druidCluster = new DruidCluster(
        ImmutableMap.of(
            DruidServer.DEFAULT_TIER,
            MinMaxPriorityQueue.orderedBy(Ordering.natural().reverse()).create(
                Arrays.asList(
                    new ServerHolder(
                        new DruidServer(
                            "serverHot",
                            "hostHot",
                            1000,
                            "historical",
                            DruidServer.DEFAULT_TIER,
                            0
                        ).toImmutableDruidServer(),
                        mockPeon
                    )
                )
            )
        )
    );

    ListeningExecutorService exec = MoreExecutors.listeningDecorator(
            Executors.newFixedThreadPool(1));
    BalancerStrategy balancerStrategy =
            new CostBalancerStrategyFactory().createBalancerStrategy(exec);

    DruidCoordinatorRuntimeParams params =
        new DruidCoordinatorRuntimeParams.Builder()
            .withDruidCluster(druidCluster)
            .withAvailableSegments(availableSegments)
            .withDatabaseRuleManager(databaseRuleManager)
            .withSegmentReplicantLookup(SegmentReplicantLookup.make(new DruidCluster()))
            .withBalancerStrategy(balancerStrategy)
            .withBalancerReferenceTimestamp(new DateTime("2013-01-01"))
            .withDynamicConfigs(new CoordinatorDynamicConfig.Builder().withMaxSegmentsToMove(5).build())
            .build();

    DruidCoordinatorRuntimeParams afterParams = ruleRunner.run(params);
    CoordinatorStats stats = afterParams.getCoordinatorStats();

    Assert.assertEquals(1, stats.getPerTierStats().get("assignedCount").size());
    Assert.assertEquals(1, stats.getPerTierStats().get("assignedCount").get("_default_tier").get());
    Assert.assertNull(stats.getPerTierStats().get("unassignedCount"));
    Assert.assertNull(stats.getPerTierStats().get("unassignedSize"));

    Assert.assertEquals(2, availableSegments.size());
    Assert.assertEquals(availableSegments, params.getAvailableSegments());
    Assert.assertEquals(availableSegments, afterParams.getAvailableSegments());

    EasyMock.verify(mockPeon);
    exec.shutdown();
  }

  private void mockCoordinator()
  {
    EasyMock.expect(coordinator.getDynamicConfigs()).andReturn(
        new CoordinatorDynamicConfig(
            0, 0, 0, 0, 1, 24, 0, false, null, false
        )
    ).anyTimes();
    coordinator.removeSegment(EasyMock.<DataSegment>anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.replay(coordinator);
  }
}
