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
import org.apache.druid.server.coordinator.CoordinatorStats;
import org.apache.druid.server.coordinator.CostBalancerStrategyFactory;
import org.apache.druid.server.coordinator.DruidCluster;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 */
public class LoadRuleTest
{
  private static final Logger log = new Logger(LoadRuleTest.class);
  private static final ObjectMapper jsonMapper = new DefaultObjectMapper();

  private static final ServiceEmitter emitter = new ServiceEmitter(
      "service",
      "host",
      new LoggingEmitter(
          log,
          LoggingEmitter.Level.ERROR,
          jsonMapper
      )
  );

  private ReplicationThrottler throttler;

  private ListeningExecutorService exec;
  private BalancerStrategy balancerStrategy;

  private BalancerStrategy mockBalancerStrategy;

  @Before
  public void setUp()
  {
    EmittingLogger.registerEmitter(emitter);
    emitter.start();
    throttler = EasyMock.createMock(ReplicationThrottler.class);

    exec = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1));
    balancerStrategy = new CostBalancerStrategyFactory().createBalancerStrategy(exec);

    mockBalancerStrategy = EasyMock.createMock(BalancerStrategy.class);
  }

  @After
  public void tearDown() throws Exception
  {
    exec.shutdown();
    emitter.close();
  }

  @Test
  public void testLoad()
  {
    EasyMock.expect(throttler.canCreateReplicant(EasyMock.anyString())).andReturn(true).anyTimes();

    final LoadQueuePeon mockPeon = createEmptyPeon();
    mockPeon.loadSegment(EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();

    LoadRule rule = createLoadRule(ImmutableMap.of(
        "hot", 1,
        DruidServer.DEFAULT_TIER, 2
    ));

    final DataSegment segment = createDataSegment("foo");

    throttler.registerReplicantCreation(DruidServer.DEFAULT_TIER, segment.getId(), "hostNorm");
    EasyMock.expectLastCall().once();

    EasyMock.expect(mockBalancerStrategy.findNewSegmentHomeReplicator(EasyMock.anyObject(), EasyMock.anyObject()))
            .andDelegateTo(balancerStrategy)
            .times(3);

    EasyMock.replay(throttler, mockPeon, mockBalancerStrategy);

    DruidCluster druidCluster = new DruidCluster(
        null,
        ImmutableMap.of(
            "hot",
            Stream.of(
                new ServerHolder(
                    new DruidServer(
                        "serverHot",
                        "hostHot",
                        null,
                        1000,
                        ServerType.HISTORICAL,
                        "hot",
                        1
                    ).toImmutableDruidServer(),
                    mockPeon
                )
            ).collect(Collectors.toCollection(() -> new TreeSet<>(Collections.reverseOrder()))),
            DruidServer.DEFAULT_TIER,
            Stream.of(
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
            ).collect(Collectors.toCollection(() -> new TreeSet<>(Collections.reverseOrder())))
        )
    );

    CoordinatorStats stats = rule.run(
        null,
        DruidCoordinatorRuntimeParams.newBuilder()
                                     .withDruidCluster(druidCluster)
                                     .withSegmentReplicantLookup(SegmentReplicantLookup.make(druidCluster))
                                     .withReplicationManager(throttler)
                                     .withBalancerStrategy(mockBalancerStrategy)
                                     .withBalancerReferenceTimestamp(DateTimes.of("2013-01-01"))
                                     .withAvailableSegments(segment).build(),
        segment
    );

    Assert.assertEquals(1L, stats.getTieredStat(LoadRule.ASSIGNED_COUNT, "hot"));
    Assert.assertEquals(1L, stats.getTieredStat(LoadRule.ASSIGNED_COUNT, DruidServer.DEFAULT_TIER));

    EasyMock.verify(throttler, mockPeon, mockBalancerStrategy);
  }

  @Test
  public void testLoadPrimaryAssignDoesNotOverAssign()
  {
    EasyMock.expect(throttler.canCreateReplicant(EasyMock.anyString())).andReturn(true).anyTimes();

    final LoadQueuePeon mockPeon = createEmptyPeon();
    mockPeon.loadSegment(EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();

    LoadRule rule = createLoadRule(ImmutableMap.of(
        "hot", 1
    ));

    final DataSegment segment = createDataSegment("foo");

    EasyMock.expect(mockBalancerStrategy.findNewSegmentHomeReplicator(EasyMock.anyObject(), EasyMock.anyObject()))
            .andDelegateTo(balancerStrategy)
            .anyTimes();

    EasyMock.replay(throttler, mockPeon, mockBalancerStrategy);

    DruidCluster druidCluster = new DruidCluster(
        null,
        ImmutableMap.of(
            "hot",
            Stream.of(
                new ServerHolder(
                    new DruidServer(
                        "serverHot",
                        "hostHot",
                        null,
                        1000,
                        ServerType.HISTORICAL,
                        "hot",
                        1
                    ).toImmutableDruidServer(),
                    mockPeon
                ), new ServerHolder(
                    new DruidServer(
                        "serverHot2",
                        "hostHot2",
                        null,
                        1000,
                        ServerType.HISTORICAL,
                        "hot",
                        1
                    ).toImmutableDruidServer(),
                    mockPeon
                )
            ).collect(Collectors.toCollection(() -> new TreeSet<>(Collections.reverseOrder())))
        )
    );

    CoordinatorStats stats = rule.run(
        null,
        DruidCoordinatorRuntimeParams.newBuilder()
                                     .withDruidCluster(druidCluster)
                                     .withSegmentReplicantLookup(SegmentReplicantLookup.make(druidCluster))
                                     .withReplicationManager(throttler)
                                     .withBalancerStrategy(mockBalancerStrategy)
                                     .withBalancerReferenceTimestamp(DateTimes.of("2013-01-01"))
                                     .withAvailableSegments(segment).build(),
        segment
    );


    Assert.assertEquals(1L, stats.getTieredStat(LoadRule.ASSIGNED_COUNT, "hot"));

    // ensure multiple runs don't assign primary segment again if at replication count
    final LoadQueuePeon loadingPeon = createLoadingPeon(ImmutableList.of(segment));
    EasyMock.replay(loadingPeon);

    DruidCluster afterLoad = new DruidCluster(
        null,
        ImmutableMap.of(
            "hot",
            Stream.of(
                new ServerHolder(
                    new DruidServer(
                        "serverHot",
                        "hostHot",
                        null,
                        1000,
                        ServerType.HISTORICAL,
                        "hot",
                        1
                    ).toImmutableDruidServer(),
                    loadingPeon
                ), new ServerHolder(
                    new DruidServer(
                        "serverHot2",
                        "hostHot2",
                        null,
                        1000,
                        ServerType.HISTORICAL,
                        "hot",
                        1
                    ).toImmutableDruidServer(),
                    mockPeon
                )
            ).collect(Collectors.toCollection(() -> new TreeSet<>(Collections.reverseOrder())))
        )
    );
    CoordinatorStats statsAfterLoadPrimary = rule.run(
        null,
        DruidCoordinatorRuntimeParams.newBuilder()
                                     .withDruidCluster(afterLoad)
                                     .withSegmentReplicantLookup(SegmentReplicantLookup.make(afterLoad))
                                     .withReplicationManager(throttler)
                                     .withBalancerStrategy(mockBalancerStrategy)
                                     .withBalancerReferenceTimestamp(DateTimes.of("2013-01-01"))
                                     .withAvailableSegments(segment).build(),
        segment
    );


    Assert.assertEquals(0, statsAfterLoadPrimary.getTieredStat(LoadRule.ASSIGNED_COUNT, "hot"));

    EasyMock.verify(throttler, mockPeon, mockBalancerStrategy);
  }

  @Test
  public void testLoadPriority()
  {
    EasyMock.expect(throttler.canCreateReplicant(EasyMock.anyString())).andReturn(false).anyTimes();

    final LoadQueuePeon mockPeon1 = createEmptyPeon();
    final LoadQueuePeon mockPeon2 = createEmptyPeon();

    mockPeon2.loadSegment(EasyMock.anyObject(), EasyMock.isNull());
    EasyMock.expectLastCall().once();

    EasyMock.expect(mockBalancerStrategy.findNewSegmentHomeReplicator(EasyMock.anyObject(), EasyMock.anyObject()))
            .andDelegateTo(balancerStrategy)
            .times(2);

    EasyMock.replay(throttler, mockPeon1, mockPeon2, mockBalancerStrategy);

    final LoadRule rule = createLoadRule(ImmutableMap.of(
        "tier1", 10,
        "tier2", 10
    ));

    final DruidCluster druidCluster = new DruidCluster(
        null,
        ImmutableMap.of(
            "tier1",
            Stream.of(
                new ServerHolder(
                    new DruidServer(
                        "server1",
                        "host1",
                        null,
                        1000,
                        ServerType.HISTORICAL,
                        "tier1",
                        0
                    ).toImmutableDruidServer(),
                    mockPeon1
                )
            ).collect(Collectors.toCollection(() -> new TreeSet<>(Collections.reverseOrder()))),
            "tier2",
            Stream.of(
                new ServerHolder(
                    new DruidServer(
                        "server2",
                        "host2",
                        null,
                        1000,
                        ServerType.HISTORICAL,
                        "tier2",
                        1
                    ).toImmutableDruidServer(),
                    mockPeon2
                ),
                new ServerHolder(
                    new DruidServer(
                        "server3",
                        "host3",
                        null,
                        1000,
                        ServerType.HISTORICAL,
                        "tier2",
                        1
                    ).toImmutableDruidServer(),
                    mockPeon2
                )
            ).collect(Collectors.toCollection(() -> new TreeSet<>(Collections.reverseOrder())))
        )
    );

    final DataSegment segment = createDataSegment("foo");

    final CoordinatorStats stats = rule.run(
        null,
        DruidCoordinatorRuntimeParams.newBuilder()
                                     .withDruidCluster(druidCluster)
                                     .withSegmentReplicantLookup(SegmentReplicantLookup.make(druidCluster))
                                     .withReplicationManager(throttler)
                                     .withBalancerStrategy(mockBalancerStrategy)
                                     .withBalancerReferenceTimestamp(DateTimes.of("2013-01-01"))
                                     .withAvailableSegments(segment).build(),
        segment
    );

    Assert.assertEquals(0L, stats.getTieredStat(LoadRule.ASSIGNED_COUNT, "tier1"));
    Assert.assertEquals(1L, stats.getTieredStat(LoadRule.ASSIGNED_COUNT, "tier2"));

    EasyMock.verify(throttler, mockPeon1, mockPeon2, mockBalancerStrategy);
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
    EasyMock.replay(throttler, mockPeon, mockBalancerStrategy);

    LoadRule rule = createLoadRule(ImmutableMap.of(
        "hot", 0,
        DruidServer.DEFAULT_TIER, 0
    ));

    final DataSegment segment = createDataSegment("foo");

    DruidServer server1 = new DruidServer(
        "serverHot",
        "hostHot",
        null,
        1000,
        ServerType.HISTORICAL,
        "hot",
        0
    );
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
    DruidCluster druidCluster = new DruidCluster(
        null,
        ImmutableMap.of(
            "hot",
            Stream.of(
                new ServerHolder(
                    server1.toImmutableDruidServer(),
                    mockPeon
                )
            ).collect(Collectors.toCollection(() -> new TreeSet<>(Collections.reverseOrder()))),
            DruidServer.DEFAULT_TIER,
            Stream.of(
                new ServerHolder(
                    server2.toImmutableDruidServer(),
                    mockPeon
                ),
                new ServerHolder(
                    server3.toImmutableDruidServer(),
                    mockPeon
                )
            ).collect(Collectors.toCollection(() -> new TreeSet<>(Collections.reverseOrder())))
        )
    );

    CoordinatorStats stats = rule.run(
        null,
        DruidCoordinatorRuntimeParams.newBuilder()
                                     .withDruidCluster(druidCluster)
                                     .withSegmentReplicantLookup(SegmentReplicantLookup.make(druidCluster))
                                     .withReplicationManager(throttler)
                                     .withBalancerStrategy(mockBalancerStrategy)
                                     .withBalancerReferenceTimestamp(DateTimes.of("2013-01-01"))
                                     .withAvailableSegments(segment).build(),
        segment
    );

    Assert.assertEquals(1L, stats.getTieredStat("droppedCount", "hot"));
    Assert.assertEquals(1L, stats.getTieredStat("droppedCount", DruidServer.DEFAULT_TIER));

    EasyMock.verify(throttler, mockPeon);
  }

  @Test
  public void testLoadWithNonExistentTier()
  {
    final LoadQueuePeon mockPeon = createEmptyPeon();
    mockPeon.loadSegment(EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();

    EasyMock.expect(mockBalancerStrategy.findNewSegmentHomeReplicator(EasyMock.anyObject(), EasyMock.anyObject()))
            .andDelegateTo(balancerStrategy)
            .times(1);

    EasyMock.replay(throttler, mockPeon, mockBalancerStrategy);

    LoadRule rule = createLoadRule(ImmutableMap.of(
        "nonExistentTier", 1,
        "hot", 1
    ));

    DruidCluster druidCluster = new DruidCluster(
        null,
        ImmutableMap.of(
            "hot",
            Stream.of(
                new ServerHolder(
                    new DruidServer(
                        "serverHot",
                        "hostHot",
                        null,
                        1000,
                        ServerType.HISTORICAL,
                        "hot",
                        0
                    ).toImmutableDruidServer(),
                    mockPeon
                )
            ).collect(Collectors.toCollection(() -> new TreeSet<>(Collections.reverseOrder())))
        )
    );

    final DataSegment segment = createDataSegment("foo");

    CoordinatorStats stats = rule.run(
        null,
        DruidCoordinatorRuntimeParams.newBuilder()
                                     .withDruidCluster(druidCluster)
                                     .withSegmentReplicantLookup(SegmentReplicantLookup.make(new DruidCluster()))
                                     .withReplicationManager(throttler)
                                     .withBalancerStrategy(mockBalancerStrategy)
                                     .withBalancerReferenceTimestamp(DateTimes.of("2013-01-01"))
                                     .withAvailableSegments(segment).build(),
        segment
    );

    Assert.assertEquals(1L, stats.getTieredStat(LoadRule.ASSIGNED_COUNT, "hot"));

    EasyMock.verify(throttler, mockPeon, mockBalancerStrategy);
  }

  @Test
  public void testDropWithNonExistentTier()
  {
    final LoadQueuePeon mockPeon = createEmptyPeon();
    mockPeon.dropSegment(EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.expect(mockBalancerStrategy.pickServersToDrop(EasyMock.anyObject(), EasyMock.anyObject()))
            .andDelegateTo(balancerStrategy)
            .times(2);
    EasyMock.replay(throttler, mockPeon, mockBalancerStrategy);

    LoadRule rule = createLoadRule(ImmutableMap.of(
        "nonExistentTier", 1,
        "hot", 1
    ));

    final DataSegment segment = createDataSegment("foo");

    DruidServer server1 = new DruidServer(
        "serverHot",
        "hostHot",
        null,
        1000,
        ServerType.HISTORICAL,
        "hot",
        0
    );
    DruidServer server2 = new DruidServer(
        "serverHo2t",
        "hostHot2",
        null,
        1000,
        ServerType.HISTORICAL,
        "hot",
        0
    );
    server1.addDataSegment(segment);
    server2.addDataSegment(segment);

    DruidCluster druidCluster = new DruidCluster(
        null,
        ImmutableMap.of(
            "hot",
            Stream.of(
                new ServerHolder(
                    server1.toImmutableDruidServer(),
                    mockPeon
                ),
                new ServerHolder(
                    server2.toImmutableDruidServer(),
                    mockPeon
                )
            ).collect(Collectors.toCollection(() -> new TreeSet<>(Collections.reverseOrder())))
        )
    );

    CoordinatorStats stats = rule.run(
        null,
        DruidCoordinatorRuntimeParams.newBuilder()
                                     .withDruidCluster(druidCluster)
                                     .withSegmentReplicantLookup(SegmentReplicantLookup.make(druidCluster))
                                     .withReplicationManager(throttler)
                                     .withBalancerStrategy(mockBalancerStrategy)
                                     .withBalancerReferenceTimestamp(DateTimes.of("2013-01-01"))
                                     .withAvailableSegments(segment).build(),
        segment
    );

    Assert.assertEquals(1L, stats.getTieredStat("droppedCount", "hot"));

    EasyMock.verify(throttler, mockPeon, mockBalancerStrategy);
  }

  @Test
  public void testMaxLoadingQueueSize()
  {
    EasyMock.expect(mockBalancerStrategy.findNewSegmentHomeReplicator(EasyMock.anyObject(), EasyMock.anyObject()))
            .andDelegateTo(balancerStrategy)
            .times(2);

    EasyMock.replay(throttler, mockBalancerStrategy);

    final LoadQueuePeonTester peon = new LoadQueuePeonTester();

    LoadRule rule = createLoadRule(ImmutableMap.of(
        "hot", 1
    ));

    DruidCluster druidCluster = new DruidCluster(
        null,
        ImmutableMap.of(
            "hot",
            Stream.of(
                new ServerHolder(
                    new DruidServer(
                        "serverHot",
                        "hostHot",
                        null,
                        1000,
                        ServerType.HISTORICAL,
                        "hot",
                        0
                    ).toImmutableDruidServer(),
                    peon
                )
            ).collect(Collectors.toCollection(() -> new TreeSet<>(Collections.reverseOrder())))
        )
    );

    DataSegment dataSegment1 = createDataSegment("ds1");
    DataSegment dataSegment2 = createDataSegment("ds2");
    DataSegment dataSegment3 = createDataSegment("ds3");

    DruidCoordinatorRuntimeParams params =
        DruidCoordinatorRuntimeParams
            .newBuilder()
            .withDruidCluster(druidCluster)
            .withSegmentReplicantLookup(SegmentReplicantLookup.make(druidCluster))
            .withReplicationManager(throttler)
            .withBalancerStrategy(mockBalancerStrategy)
            .withBalancerReferenceTimestamp(DateTimes.of("2013-01-01"))
            .withAvailableSegments(dataSegment1, dataSegment2, dataSegment3)
            .withDynamicConfigs(CoordinatorDynamicConfig.builder().withMaxSegmentsInNodeLoadingQueue(2).build())
            .build();

    CoordinatorStats stats1 = rule.run(null, params, dataSegment1);
    CoordinatorStats stats2 = rule.run(null, params, dataSegment2);
    CoordinatorStats stats3 = rule.run(null, params, dataSegment3);

    Assert.assertEquals(1L, stats1.getTieredStat(LoadRule.ASSIGNED_COUNT, "hot"));
    Assert.assertEquals(1L, stats2.getTieredStat(LoadRule.ASSIGNED_COUNT, "hot"));
    Assert.assertFalse(stats3.getTiers(LoadRule.ASSIGNED_COUNT).contains("hot"));

    EasyMock.verify(throttler, mockBalancerStrategy);
  }

  /**
   * 2 servers in different tiers, the first is in maitenance mode.
   * Should not load a segment to the server in maintenance mode.
   */
  @Test
  public void testLoadDuringMaitenance()
  {
    final LoadQueuePeon mockPeon1 = createEmptyPeon();
    final LoadQueuePeon mockPeon2 = createOneCallPeonMock();

    LoadRule rule = createLoadRule(ImmutableMap.of(
        "tier1", 1,
        "tier2", 1
    ));

    final DataSegment segment = createDataSegment("foo");

    EasyMock.expect(mockBalancerStrategy.findNewSegmentHomeReplicator(EasyMock.anyObject(), EasyMock.anyObject()))
            .andDelegateTo(balancerStrategy)
            .times(1);

    EasyMock.replay(mockPeon1, mockPeon2, mockBalancerStrategy);


    DruidCluster druidCluster = new DruidCluster(
        null,
        ImmutableMap.of(
            "tier1",
            Collections.singleton(createServerHolder("tier1", mockPeon1, true)),
            "tier2",
            Collections.singleton(createServerHolder("tier2", mockPeon2, false))
        )
    );

    CoordinatorStats stats = rule.run(
        null,
        DruidCoordinatorRuntimeParams.newBuilder()
                                     .withDruidCluster(druidCluster)
                                     .withSegmentReplicantLookup(SegmentReplicantLookup.make(druidCluster))
                                     .withReplicationManager(throttler)
                                     .withBalancerStrategy(mockBalancerStrategy)
                                     .withBalancerReferenceTimestamp(DateTimes.of("2013-01-01"))
                                     .withAvailableSegments(segment).build(),
        segment
    );

    Assert.assertEquals(1L, stats.getTieredStat(LoadRule.ASSIGNED_COUNT, "tier2"));
    EasyMock.verify(mockPeon1, mockPeon2, mockBalancerStrategy);
  }

  /**
   * 2 tiers, 2 servers each, 1 server of the second tier is in maintenance.
   * Should not load a segment to the server in maintenance mode.
   */
  @Test
  public void testLoadReplicaDuringMaitenance()
  {
    EasyMock.expect(throttler.canCreateReplicant(EasyMock.anyString())).andReturn(true).anyTimes();

    final LoadQueuePeon mockPeon1 = createEmptyPeon();
    final LoadQueuePeon mockPeon2 = createOneCallPeonMock();
    final LoadQueuePeon mockPeon3 = createOneCallPeonMock();
    final LoadQueuePeon mockPeon4 = createOneCallPeonMock();

    LoadRule rule = createLoadRule(ImmutableMap.of("tier1", 2, "tier2", 2));

    final DataSegment segment = createDataSegment("foo");

    throttler.registerReplicantCreation(EasyMock.eq("tier2"), EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().times(2);

    ServerHolder holder1 = createServerHolder("tier1", mockPeon1, true);
    ServerHolder holder2 = createServerHolder("tier1", mockPeon2, false);
    ServerHolder holder3 = createServerHolder("tier2", mockPeon3, false);
    ServerHolder holder4 = createServerHolder("tier2", mockPeon4, false);

    EasyMock.expect(mockBalancerStrategy.findNewSegmentHomeReplicator(segment, ImmutableList.of(holder2)))
            .andReturn(holder2);
    EasyMock.expect(mockBalancerStrategy.findNewSegmentHomeReplicator(segment, ImmutableList.of(holder4, holder3)))
            .andReturn(holder3);
    EasyMock.expect(mockBalancerStrategy.findNewSegmentHomeReplicator(segment, ImmutableList.of(holder4)))
            .andReturn(holder4);

    EasyMock.replay(throttler, mockPeon1, mockPeon2, mockPeon3, mockPeon4, mockBalancerStrategy);


    DruidCluster druidCluster = new DruidCluster(
        null,
        ImmutableMap.of("tier1", Arrays.asList(holder1, holder2), "tier2", Arrays.asList(holder3, holder4))
    );

    CoordinatorStats stats = rule.run(
        null,
        DruidCoordinatorRuntimeParams.newBuilder()
                                     .withDruidCluster(druidCluster)
                                     .withSegmentReplicantLookup(SegmentReplicantLookup.make(druidCluster))
                                     .withReplicationManager(throttler)
                                     .withBalancerStrategy(mockBalancerStrategy)
                                     .withBalancerReferenceTimestamp(DateTimes.of("2013-01-01"))
                                     .withAvailableSegments(segment).build(),
        segment
    );

    Assert.assertEquals(1L, stats.getTieredStat(LoadRule.ASSIGNED_COUNT, "tier1"));
    Assert.assertEquals(2L, stats.getTieredStat(LoadRule.ASSIGNED_COUNT, "tier2"));

    EasyMock.verify(throttler, mockPeon1, mockPeon2, mockPeon3, mockPeon4, mockBalancerStrategy);
  }

  /**
   * 2 servers with a segment, one server in maintenance mode.
   * Should drop a segment from both.
   */
  @Test
  public void testDropDuringMaintenance()
  {
    final LoadQueuePeon mockPeon = createEmptyPeon();
    mockPeon.dropSegment(EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().times(2);
    EasyMock.expect(mockBalancerStrategy.pickServersToDrop(EasyMock.anyObject(), EasyMock.anyObject()))
            .andDelegateTo(balancerStrategy)
            .times(4);
    EasyMock.replay(throttler, mockPeon, mockBalancerStrategy);

    LoadRule rule = createLoadRule(ImmutableMap.of("tier1", 0));

    final DataSegment segment1 = createDataSegment("foo1");
    final DataSegment segment2 = createDataSegment("foo2");

    DruidServer server1 = createServer("tier1");
    server1.addDataSegment(segment1);
    DruidServer server2 = createServer("tier1");
    server2.addDataSegment(segment2);

    DruidCluster druidCluster = new DruidCluster(
        null,
        ImmutableMap.of(
            "tier1",
            Arrays.asList(
                new ServerHolder(server1.toImmutableDruidServer(), mockPeon, true),
                new ServerHolder(server2.toImmutableDruidServer(), mockPeon, false)
            )
        )
    );

    DruidCoordinatorRuntimeParams params = DruidCoordinatorRuntimeParams
        .newBuilder()
        .withDruidCluster(druidCluster)
        .withSegmentReplicantLookup(SegmentReplicantLookup.make(druidCluster))
        .withReplicationManager(throttler)
        .withBalancerStrategy(mockBalancerStrategy)
        .withBalancerReferenceTimestamp(DateTimes.of("2013-01-01"))
        .withAvailableSegments(segment1, segment2)
        .build();
    CoordinatorStats stats = rule.run(
        null,
        params,
        segment1
    );
    Assert.assertEquals(1L, stats.getTieredStat("droppedCount", "tier1"));
    stats = rule.run(
        null,
        params,
        segment2
    );
    Assert.assertEquals(1L, stats.getTieredStat("droppedCount", "tier1"));


    EasyMock.verify(throttler, mockPeon);
  }

  /**
   * 3 servers hosting 3 replicas of the segment.
   * 1 servers is in maitenance.
   * 1 replica is redundant.
   * Should drop from the server in maintenance.
   */
  @Test
  public void testRedundantReplicaDropDuringMaintenance()
  {
    final LoadQueuePeon mockPeon1 = new LoadQueuePeonTester();
    final LoadQueuePeon mockPeon2 = new LoadQueuePeonTester();
    final LoadQueuePeon mockPeon3 = new LoadQueuePeonTester();
    EasyMock.expect(mockBalancerStrategy.pickServersToDrop(EasyMock.anyObject(), EasyMock.anyObject()))
            .andDelegateTo(balancerStrategy)
            .times(4);
    EasyMock.replay(throttler, mockBalancerStrategy);

    LoadRule rule = createLoadRule(ImmutableMap.of("tier1", 2));

    final DataSegment segment1 = createDataSegment("foo1");

    DruidServer server1 = createServer("tier1");
    server1.addDataSegment(segment1);
    DruidServer server2 = createServer("tier1");
    server2.addDataSegment(segment1);
    DruidServer server3 = createServer("tier1");
    server3.addDataSegment(segment1);

    DruidCluster druidCluster = new DruidCluster(
        null,
        ImmutableMap.of(
            "tier1",
            Arrays.asList(
                new ServerHolder(server1.toImmutableDruidServer(), mockPeon1, false),
                new ServerHolder(server2.toImmutableDruidServer(), mockPeon2, true),
                new ServerHolder(server3.toImmutableDruidServer(), mockPeon3, false)
            )
        )
    );

    DruidCoordinatorRuntimeParams params = DruidCoordinatorRuntimeParams
        .newBuilder()
        .withDruidCluster(druidCluster)
        .withSegmentReplicantLookup(SegmentReplicantLookup.make(druidCluster))
        .withReplicationManager(throttler)
        .withBalancerStrategy(mockBalancerStrategy)
        .withBalancerReferenceTimestamp(DateTimes.of("2013-01-01"))
        .withAvailableSegments(segment1)
        .build();
    CoordinatorStats stats = rule.run(
        null,
        params,
        segment1
    );
    Assert.assertEquals(1L, stats.getTieredStat("droppedCount", "tier1"));
    Assert.assertEquals(0, mockPeon1.getSegmentsToDrop().size());
    Assert.assertEquals(1, mockPeon2.getSegmentsToDrop().size());
    Assert.assertEquals(0, mockPeon3.getSegmentsToDrop().size());

    EasyMock.verify(throttler);
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

  private static LoadQueuePeon createLoadingPeon(List<DataSegment> segments)
  {
    final Set<DataSegment> segs = ImmutableSet.copyOf(segments);
    final long loadingSize = segs.stream().mapToLong(DataSegment::getSize).sum();

    final LoadQueuePeon mockPeon = EasyMock.createMock(LoadQueuePeon.class);
    EasyMock.expect(mockPeon.getSegmentsToLoad()).andReturn(segs).anyTimes();
    EasyMock.expect(mockPeon.getSegmentsMarkedToDrop()).andReturn(new HashSet<>()).anyTimes();
    EasyMock.expect(mockPeon.getLoadQueueSize()).andReturn(loadingSize).anyTimes();
    EasyMock.expect(mockPeon.getNumberOfSegmentsInQueue()).andReturn(segs.size()).anyTimes();

    return mockPeon;
  }

  private static final AtomicInteger serverId = new AtomicInteger();

  private static DruidServer createServer(String tier)
  {
    int serverId = LoadRuleTest.serverId.incrementAndGet();
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
    mockPeon2.loadSegment(EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().once();
    return mockPeon2;
  }

  private static ServerHolder createServerHolder(String tier, LoadQueuePeon mockPeon1, boolean maintenance)
  {
    return new ServerHolder(
        createServer(tier).toImmutableDruidServer(),
        mockPeon1,
        maintenance
    );
  }
}
