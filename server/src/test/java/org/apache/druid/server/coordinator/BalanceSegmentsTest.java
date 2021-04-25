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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.client.ImmutableDruidServerTests;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.easymock.EasyMock;
import org.hamcrest.Matchers;
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
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class BalanceSegmentsTest
{
  private static final int MAX_SEGMENTS_TO_MOVE = 5;
  private DruidCoordinator coordinator;
  private ImmutableDruidServer druidServer1;
  private ImmutableDruidServer druidServer2;
  private ImmutableDruidServer druidServer3;
  private ImmutableDruidServer druidServer4;
  private List<ImmutableDruidServer> druidServers;
  private LoadQueuePeonTester peon1;
  private LoadQueuePeonTester peon2;
  private LoadQueuePeonTester peon3;
  private LoadQueuePeonTester peon4;
  private List<LoadQueuePeon> peons;
  private DataSegment segment1;
  private DataSegment segment2;
  private DataSegment segment3;
  private DataSegment segment4;
  private DataSegment segment5;
  private List<DataSegment> segments;
  private ListeningExecutorService balancerStrategyExecutor;
  private BalancerStrategy balancerStrategy;
  private Set<String> broadcastDatasources;

  @Before
  public void setUp()
  {
    coordinator = EasyMock.createMock(DruidCoordinator.class);
    druidServer1 = EasyMock.createMock(ImmutableDruidServer.class);
    druidServer2 = EasyMock.createMock(ImmutableDruidServer.class);
    druidServer3 = EasyMock.createMock(ImmutableDruidServer.class);
    druidServer4 = EasyMock.createMock(ImmutableDruidServer.class);
    segment1 = EasyMock.createMock(DataSegment.class);
    segment2 = EasyMock.createMock(DataSegment.class);
    segment3 = EasyMock.createMock(DataSegment.class);
    segment4 = EasyMock.createMock(DataSegment.class);
    segment5 = EasyMock.createMock(DataSegment.class);

    DateTime start1 = DateTimes.of("2012-01-01");
    DateTime start2 = DateTimes.of("2012-02-01");
    DateTime version = DateTimes.of("2012-03-01");
    segment1 = new DataSegment(
        "datasource1",
        new Interval(start1, start1.plusHours(1)),
        version.toString(),
        new HashMap<>(),
        new ArrayList<>(),
        new ArrayList<>(),
        NoneShardSpec.instance(),
        0,
        11L
    );
    segment2 = new DataSegment(
        "datasource1",
        new Interval(start2, start2.plusHours(1)),
        version.toString(),
        new HashMap<>(),
        new ArrayList<>(),
        new ArrayList<>(),
        NoneShardSpec.instance(),
        0,
        7L
    );
    segment3 = new DataSegment(
        "datasource2",
        new Interval(start1, start1.plusHours(1)),
        version.toString(),
        new HashMap<>(),
        new ArrayList<>(),
        new ArrayList<>(),
        NoneShardSpec.instance(),
        0,
        4L
    );
    segment4 = new DataSegment(
        "datasource2",
        new Interval(start2, start2.plusHours(1)),
        version.toString(),
        new HashMap<>(),
        new ArrayList<>(),
        new ArrayList<>(),
        NoneShardSpec.instance(),
        0,
        8L
    );
    segment5 = new DataSegment(
        "datasourceBroadcast",
        new Interval(start2, start2.plusHours(1)),
        version.toString(),
        new HashMap<>(),
        new ArrayList<>(),
        new ArrayList<>(),
        NoneShardSpec.instance(),
        0,
        8L
    );

    segments = new ArrayList<>();
    segments.add(segment1);
    segments.add(segment2);
    segments.add(segment3);
    segments.add(segment4);
    segments.add(segment5);

    peon1 = new LoadQueuePeonTester();
    peon2 = new LoadQueuePeonTester();
    peon3 = new LoadQueuePeonTester();
    peon4 = new LoadQueuePeonTester();

    druidServers = ImmutableList.of(druidServer1, druidServer2, druidServer3, druidServer4);
    peons = ImmutableList.of(peon1, peon2, peon3, peon4);

    balancerStrategyExecutor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1));
    balancerStrategy = new CostBalancerStrategyFactory().createBalancerStrategy(balancerStrategyExecutor);

    broadcastDatasources = Collections.singleton("datasourceBroadcast");
  }

  @After
  public void tearDown()
  {
    EasyMock.verify(coordinator);
    EasyMock.verify(druidServer1);
    EasyMock.verify(druidServer2);
    EasyMock.verify(druidServer3);
    EasyMock.verify(druidServer4);
    balancerStrategyExecutor.shutdownNow();
  }

  @Test
  public void testMoveToEmptyServerBalancer()
  {
    mockDruidServer(druidServer1, "1", "normal", 30L, 100L, segments);
    mockDruidServer(druidServer2, "2", "normal", 0L, 100L, Collections.emptyList());

    EasyMock.replay(druidServer3);
    EasyMock.replay(druidServer4);

    // Mock stuff that the coordinator needs
    mockCoordinator(coordinator);

    BalancerStrategy predefinedPickOrderStrategy = new PredefinedPickOrderBalancerStrategy(
        balancerStrategy,
        ImmutableList.of(
            new BalancerSegmentHolder(druidServer1, segment1),
            new BalancerSegmentHolder(druidServer1, segment2),
            new BalancerSegmentHolder(druidServer1, segment3),
            new BalancerSegmentHolder(druidServer1, segment4)
        )
    );

    DruidCoordinatorRuntimeParams params = defaultRuntimeParamsBuilder(
        ImmutableList.of(druidServer1, druidServer2),
        ImmutableList.of(peon1, peon2)
    )
        .withBalancerStrategy(predefinedPickOrderStrategy)
        .withBroadcastDatasources(broadcastDatasources)
        .build();

    params = new BalanceSegmentsTester(coordinator).run(params);
    Assert.assertEquals(3, params.getCoordinatorStats().getTieredStat("movedCount", "normal"));
  }

  /**
   * Server 1 has 2 segments.
   * Server 2 (decommissioning) has 2 segments.
   * Server 3 is empty.
   * Decommissioning percent is 60.
   * Max segments to move is 3.
   * 2 (of 2) segments should be moved from Server 2 and 1 (of 2) from Server 1.
   */
  @Test
  public void testMoveDecommissioningMaxPercentOfMaxSegmentsToMove()
  {
    mockDruidServer(druidServer1, "1", "normal", 30L, 100L, Arrays.asList(segment1, segment2));
    mockDruidServer(druidServer2, "2", "normal", 30L, 100L, Arrays.asList(segment3, segment4));
    mockDruidServer(druidServer3, "3", "normal", 0L, 100L, Collections.emptyList());

    EasyMock.replay(druidServer4);

    mockCoordinator(coordinator);

    BalancerStrategy strategy = EasyMock.createMock(BalancerStrategy.class);
    EasyMock.expect(
        strategy.pickSegmentToMove(
            ImmutableList.of(
                new ServerHolder(druidServer2, peon2, false)
            ),
            broadcastDatasources,
            100
        )
    ).andReturn(
        new BalancerSegmentHolder(druidServer2, segment3)).andReturn(new BalancerSegmentHolder(druidServer2, segment4)
    );

    EasyMock.expect(strategy.pickSegmentToMove(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyInt()))
            .andReturn(new BalancerSegmentHolder(druidServer1, segment1))
            .andReturn(new BalancerSegmentHolder(druidServer1, segment2));

    EasyMock.expect(strategy.findNewSegmentHomeBalancer(EasyMock.anyObject(), EasyMock.anyObject()))
            .andReturn(new ServerHolder(druidServer3, peon3))
            .anyTimes();
    EasyMock.replay(strategy);

    DruidCoordinatorRuntimeParams params = defaultRuntimeParamsBuilder(
        ImmutableList.of(druidServer1, druidServer2, druidServer3),
        ImmutableList.of(peon1, peon2, peon3),
        ImmutableList.of(false, true, false)
    )
        .withDynamicConfigs(
            CoordinatorDynamicConfig.builder()
                                    .withMaxSegmentsToMove(3)
                                    .withDecommissioningMaxPercentOfMaxSegmentsToMove(60)
                                    .build() // ceil(3 * 0.6) = 2 segments from decommissioning servers
        )
        .withBalancerStrategy(strategy)
        .withBroadcastDatasources(broadcastDatasources)
        .build();

    params = new BalanceSegmentsTester(coordinator).run(params);
    Assert.assertEquals(3L, params.getCoordinatorStats().getTieredStat("movedCount", "normal"));
    Assert.assertThat(
        peon3.getSegmentsToLoad(),
        Matchers.is(Matchers.equalTo(ImmutableSet.of(segment1, segment3, segment4)))
    );
  }

  @Test
  public void testZeroDecommissioningMaxPercentOfMaxSegmentsToMove()
  {
    DruidCoordinatorRuntimeParams params = setupParamsForDecommissioningMaxPercentOfMaxSegmentsToMove(0);
    params = new BalanceSegmentsTester(coordinator).run(params);
    Assert.assertEquals(1L, params.getCoordinatorStats().getTieredStat("movedCount", "normal"));
    Assert.assertThat(peon3.getSegmentsToLoad(), Matchers.is(Matchers.equalTo(ImmutableSet.of(segment1))));
  }

  @Test
  public void testMaxDecommissioningMaxPercentOfMaxSegmentsToMove()
  {
    DruidCoordinatorRuntimeParams params = setupParamsForDecommissioningMaxPercentOfMaxSegmentsToMove(10);
    params = new BalanceSegmentsTester(coordinator).run(params);
    Assert.assertEquals(1L, params.getCoordinatorStats().getTieredStat("movedCount", "normal"));
    Assert.assertThat(peon3.getSegmentsToLoad(), Matchers.is(Matchers.equalTo(ImmutableSet.of(segment2))));
  }

  /**
   * Should balance segments as usual (ignoring percent) with empty decommissioningNodes.
   */
  @Test
  public void testMoveDecommissioningMaxPercentOfMaxSegmentsToMoveWithNoDecommissioning()
  {
    mockDruidServer(druidServer1, "1", "normal", 30L, 100L, Arrays.asList(segment1, segment2));
    mockDruidServer(druidServer2, "2", "normal", 0L, 100L, Arrays.asList(segment3, segment4));
    mockDruidServer(druidServer3, "3", "normal", 0L, 100L, Collections.emptyList());

    EasyMock.replay(druidServer4);

    mockCoordinator(coordinator);

    BalancerStrategy strategy = EasyMock.createMock(BalancerStrategy.class);
    EasyMock.expect(strategy.pickSegmentToMove(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyInt()))
            .andReturn(new BalancerSegmentHolder(druidServer1, segment1))
            .andReturn(new BalancerSegmentHolder(druidServer1, segment2))
            .andReturn(new BalancerSegmentHolder(druidServer2, segment3))
            .andReturn(new BalancerSegmentHolder(druidServer2, segment4));

    EasyMock.expect(strategy.findNewSegmentHomeBalancer(EasyMock.anyObject(), EasyMock.anyObject()))
            .andReturn(new ServerHolder(druidServer3, peon3))
            .anyTimes();
    EasyMock.replay(strategy);

    DruidCoordinatorRuntimeParams params = defaultRuntimeParamsBuilder(
        ImmutableList.of(druidServer1, druidServer2, druidServer3),
        ImmutableList.of(peon1, peon2, peon3),
        ImmutableList.of(false, false, false)
    )
        .withDynamicConfigs(
            CoordinatorDynamicConfig.builder()
                                    .withMaxSegmentsToMove(3)
                                    .withDecommissioningMaxPercentOfMaxSegmentsToMove(9)
                                    .build()
        )
        .withBalancerStrategy(strategy)
        .withBroadcastDatasources(broadcastDatasources)
        .build();

    params = new BalanceSegmentsTester(coordinator).run(params);
    Assert.assertEquals(3L, params.getCoordinatorStats().getTieredStat("movedCount", "normal"));
    Assert.assertThat(
        peon3.getSegmentsToLoad(),
        Matchers.is(Matchers.equalTo(ImmutableSet.of(segment1, segment2, segment3)))
    );
  }

  /**
   * Shouldn't move segments to a decommissioning server.
   */
  @Test
  public void testMoveToDecommissioningServer()
  {
    mockDruidServer(druidServer1, "1", "normal", 30L, 100L, segments);
    mockDruidServer(druidServer2, "2", "normal", 0L, 100L, Collections.emptyList());

    EasyMock.replay(druidServer3);
    EasyMock.replay(druidServer4);

    mockCoordinator(coordinator);

    BalancerStrategy strategy = EasyMock.createMock(BalancerStrategy.class);
    EasyMock.expect(strategy.pickSegmentToMove(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyInt()))
            .andReturn(new BalancerSegmentHolder(druidServer1, segment1))
            .anyTimes();
    EasyMock.expect(strategy.findNewSegmentHomeBalancer(EasyMock.anyObject(), EasyMock.anyObject())).andAnswer(() -> {
      List<ServerHolder> holders = (List<ServerHolder>) EasyMock.getCurrentArguments()[1];
      return holders.get(0);
    }).anyTimes();
    EasyMock.replay(strategy);

    DruidCoordinatorRuntimeParams params = defaultRuntimeParamsBuilder(
        ImmutableList.of(druidServer1, druidServer2),
        ImmutableList.of(peon1, peon2),
        ImmutableList.of(false, true)
    )
        .withBalancerStrategy(strategy)
        .withBroadcastDatasources(broadcastDatasources)
        .build();

    params = new BalanceSegmentsTester(coordinator).run(params);
    Assert.assertEquals(0, params.getCoordinatorStats().getTieredStat("movedCount", "normal"));
  }

  @Test
  public void testMoveFromDecommissioningServer()
  {
    mockDruidServer(druidServer1, "1", "normal", 30L, 100L, segments);
    mockDruidServer(druidServer2, "2", "normal", 0L, 100L, Collections.emptyList());

    EasyMock.replay(druidServer3);
    EasyMock.replay(druidServer4);

    mockCoordinator(coordinator);

    ServerHolder holder2 = new ServerHolder(druidServer2, peon2, false);
    BalancerStrategy strategy = EasyMock.createMock(BalancerStrategy.class);
    EasyMock.expect(strategy.pickSegmentToMove(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyInt()))
            .andReturn(new BalancerSegmentHolder(druidServer1, segment1))
            .once();
    EasyMock.expect(strategy.findNewSegmentHomeBalancer(EasyMock.anyObject(), EasyMock.anyObject()))
            .andReturn(holder2)
            .once();
    EasyMock.replay(strategy);

    DruidCoordinatorRuntimeParams params = defaultRuntimeParamsBuilder(
        ImmutableList.of(druidServer1, druidServer2),
        ImmutableList.of(peon1, peon2),
        ImmutableList.of(true, false)
    )
        .withDynamicConfigs(CoordinatorDynamicConfig.builder().withMaxSegmentsToMove(1).build())
        .withBalancerStrategy(strategy)
        .withBroadcastDatasources(broadcastDatasources)
        .build();

    params = new BalanceSegmentsTester(coordinator).run(params);
    Assert.assertEquals(1, params.getCoordinatorStats().getTieredStat("movedCount", "normal"));
    Assert.assertEquals(0, peon1.getNumberOfSegmentsInQueue());
    Assert.assertEquals(1, peon2.getNumberOfSegmentsInQueue());
  }

  @Test
  public void testMoveMaxLoadQueueServerBalancer()
  {
    mockDruidServer(druidServer1, "1", "normal", 30L, 100L, segments);
    mockDruidServer(druidServer2, "2", "normal", 0L, 100L, Collections.emptyList());

    EasyMock.replay(druidServer3);
    EasyMock.replay(druidServer4);

    // Mock stuff that the coordinator needs
    mockCoordinator(coordinator);

    BalancerStrategy predefinedPickOrderStrategy = new PredefinedPickOrderBalancerStrategy(
        balancerStrategy,
        ImmutableList.of(
            new BalancerSegmentHolder(druidServer1, segment1),
            new BalancerSegmentHolder(druidServer1, segment2),
            new BalancerSegmentHolder(druidServer1, segment3),
            new BalancerSegmentHolder(druidServer1, segment4)
        )
    );

    DruidCoordinatorRuntimeParams params = defaultRuntimeParamsBuilder(
        ImmutableList.of(druidServer1, druidServer2),
        ImmutableList.of(peon1, peon2)
    )
        .withBalancerStrategy(predefinedPickOrderStrategy)
        .withBroadcastDatasources(broadcastDatasources)
        .withDynamicConfigs(
            CoordinatorDynamicConfig
                .builder()
                .withMaxSegmentsToMove(MAX_SEGMENTS_TO_MOVE)
                .withMaxSegmentsInNodeLoadingQueue(1)
                .build()
        )
        .build();

    params = new BalanceSegmentsTester(coordinator).run(params);

    // max to move is 5, all segments on server 1, but only expect to move 1 to server 2 since max node load queue is 1
    Assert.assertEquals(1, params.getCoordinatorStats().getTieredStat("movedCount", "normal"));
  }

  @Test
  public void testMoveSameSegmentTwice()
  {
    mockDruidServer(druidServer1, "1", "normal", 30L, 100L, segments);
    mockDruidServer(druidServer2, "2", "normal", 0L, 100L, Collections.emptyList());

    EasyMock.replay(druidServer3);
    EasyMock.replay(druidServer4);

    // Mock stuff that the coordinator needs
    mockCoordinator(coordinator);

    BalancerStrategy predefinedPickOrderStrategy = new PredefinedPickOrderBalancerStrategy(
        balancerStrategy,
        ImmutableList.of(
            new BalancerSegmentHolder(druidServer1, segment1)
        )
    );

    DruidCoordinatorRuntimeParams params = defaultRuntimeParamsBuilder(
        ImmutableList.of(druidServer1, druidServer2),
        ImmutableList.of(peon1, peon2)
    )
        .withBalancerStrategy(predefinedPickOrderStrategy)
        .withBroadcastDatasources(broadcastDatasources)
        .withDynamicConfigs(
            CoordinatorDynamicConfig.builder().withMaxSegmentsToMove(
                2
            ).build()
        )
        .build();

    params = new BalanceSegmentsTester(coordinator).run(params);
    Assert.assertEquals(1, params.getCoordinatorStats().getTieredStat("movedCount", "normal"));
  }

  @Test
  public void testRun1()
  {
    // Mock some servers of different usages
    mockDruidServer(druidServer1, "1", "normal", 30L, 100L, segments);
    mockDruidServer(druidServer2, "2", "normal", 0L, 100L, Collections.emptyList());

    EasyMock.replay(druidServer3);
    EasyMock.replay(druidServer4);

    // Mock stuff that the coordinator needs
    mockCoordinator(coordinator);

    DruidCoordinatorRuntimeParams params = defaultRuntimeParamsBuilder(
        ImmutableList.of(druidServer1, druidServer2),
        ImmutableList.of(peon1, peon2)
    ).build();

    params = new BalanceSegmentsTester(coordinator).run(params);
    Assert.assertTrue(params.getCoordinatorStats().getTieredStat("movedCount", "normal") > 0);
  }

  @Test
  public void testRun2()
  {
    // Mock some servers of different usages
    mockDruidServer(druidServer1, "1", "normal", 30L, 100L, segments);
    mockDruidServer(druidServer2, "2", "normal", 0L, 100L, Collections.emptyList());
    mockDruidServer(druidServer3, "3", "normal", 0L, 100L, Collections.emptyList());
    mockDruidServer(druidServer4, "4", "normal", 0L, 100L, Collections.emptyList());

    // Mock stuff that the coordinator needs
    mockCoordinator(coordinator);

    DruidCoordinatorRuntimeParams params = defaultRuntimeParamsBuilder(druidServers, peons).build();

    params = new BalanceSegmentsTester(coordinator).run(params);
    Assert.assertTrue(params.getCoordinatorStats().getTieredStat("movedCount", "normal") > 0);
  }

  /**
   * Testing that the dynamic coordinator config value, percentOfSegmentsToConsiderPerMove, is honored when calling
   * out to pickSegmentToMove. This config limits the number of segments that are considered when looking for a segment
   * to move.
   */
  @Test
  public void testThatDynamicConfigIsHonoredWhenPickingSegmentToMove()
  {
    mockDruidServer(druidServer1, "1", "normal", 50L, 100L, Arrays.asList(segment1, segment2));
    mockDruidServer(druidServer2, "2", "normal", 30L, 100L, Arrays.asList(segment3, segment4));
    mockDruidServer(druidServer3, "3", "normal", 0L, 100L, Collections.emptyList());

    EasyMock.replay(druidServer4);

    mockCoordinator(coordinator);

    BalancerStrategy strategy = EasyMock.createMock(BalancerStrategy.class);

    // The first call for decommissioning servers
    EasyMock.expect(
        strategy.pickSegmentToMove(
            ImmutableList.of(),
            broadcastDatasources,
            40
        )
    )
            .andReturn(null);

    // The second call for the single non decommissioning server move
    EasyMock.expect(
        strategy.pickSegmentToMove(
            ImmutableList.of(
                new ServerHolder(druidServer3, peon3, false),
                new ServerHolder(druidServer2, peon2, false),
                new ServerHolder(druidServer1, peon1, false)
            ),
            broadcastDatasources,
            40
        )
    )
            .andReturn(new BalancerSegmentHolder(druidServer2, segment3));

    EasyMock.expect(strategy.findNewSegmentHomeBalancer(EasyMock.anyObject(), EasyMock.anyObject()))
            .andReturn(new ServerHolder(druidServer3, peon3))
            .anyTimes();
    EasyMock.replay(strategy);

    DruidCoordinatorRuntimeParams params = defaultRuntimeParamsBuilder(
        ImmutableList.of(druidServer1, druidServer2, druidServer3),
        ImmutableList.of(peon1, peon2, peon3),
        ImmutableList.of(false, false, false)
    )
        .withDynamicConfigs(
            CoordinatorDynamicConfig.builder()
                                    .withMaxSegmentsToMove(1)
                                    .withPercentOfSegmentsToConsiderPerMove(40)
                                    .build()
        )
        .withBalancerStrategy(strategy)
        .withBroadcastDatasources(broadcastDatasources)
        .build();

    params = new BalanceSegmentsTester(coordinator).run(params);
    Assert.assertEquals(1L, params.getCoordinatorStats().getTieredStat("movedCount", "normal"));
    Assert.assertThat(
        peon3.getSegmentsToLoad(),
        Matchers.is(Matchers.equalTo(ImmutableSet.of(segment3)))
    );
  }

  private DruidCoordinatorRuntimeParams.Builder defaultRuntimeParamsBuilder(
      List<ImmutableDruidServer> druidServers,
      List<LoadQueuePeon> peons
  )
  {
    return defaultRuntimeParamsBuilder(
        druidServers,
        peons,
        druidServers.stream().map(s -> false).collect(Collectors.toList())
    );
  }

  private DruidCoordinatorRuntimeParams.Builder defaultRuntimeParamsBuilder(
      List<ImmutableDruidServer> druidServers,
      List<LoadQueuePeon> peons,
      List<Boolean> decommissioning
  )
  {
    return CoordinatorRuntimeParamsTestHelpers
        .newBuilder()
        .withDruidCluster(
            DruidClusterBuilder
                .newBuilder()
                .addTier(
                    "normal",
                    IntStream
                        .range(0, druidServers.size())
                        .mapToObj(i -> new ServerHolder(druidServers.get(i), peons.get(i), decommissioning.get(i)))
                        .toArray(ServerHolder[]::new)
                )
                .build()
        )
        .withLoadManagementPeons(
            IntStream
                .range(0, peons.size())
                .boxed()
                .collect(Collectors.toMap(i -> String.valueOf(i + 1), peons::get))
        )
        .withUsedSegmentsInTest(segments)
        .withDynamicConfigs(CoordinatorDynamicConfig.builder().withMaxSegmentsToMove(MAX_SEGMENTS_TO_MOVE).build())
        .withBroadcastDatasources(broadcastDatasources)
        .withBalancerStrategy(balancerStrategy);
  }

  private static void mockDruidServer(
      ImmutableDruidServer druidServer,
      String name,
      String tier,
      long currentSize,
      long maxSize,
      List<DataSegment> segments
  )
  {
    EasyMock.expect(druidServer.getName()).andReturn(name).anyTimes();
    EasyMock.expect(druidServer.getTier()).andReturn(tier).anyTimes();
    EasyMock.expect(druidServer.getCurrSize()).andReturn(currentSize).atLeastOnce();
    EasyMock.expect(druidServer.getMaxSize()).andReturn(maxSize).atLeastOnce();
    ImmutableDruidServerTests.expectSegments(druidServer, segments);
    EasyMock.expect(druidServer.getHost()).andReturn(name).anyTimes();
    EasyMock.expect(druidServer.getType()).andReturn(ServerType.HISTORICAL).anyTimes();
    if (!segments.isEmpty()) {
      segments.forEach(
          s -> EasyMock.expect(druidServer.getSegment(s.getId())).andReturn(s).anyTimes()
      );
    }
    EasyMock.expect(druidServer.getSegment(EasyMock.anyObject())).andReturn(null).anyTimes();
    EasyMock.replay(druidServer);
  }

  private static void mockCoordinator(DruidCoordinator coordinator)
  {
    coordinator.moveSegment(
        EasyMock.anyObject(),
        EasyMock.anyObject(),
        EasyMock.anyObject(),
        EasyMock.anyObject(),
        EasyMock.anyObject()
    );
    EasyMock.expectLastCall().anyTimes();
    EasyMock.replay(coordinator);
  }

  private static class PredefinedPickOrderBalancerStrategy implements BalancerStrategy
  {
    private final BalancerStrategy delegate;
    private final List<BalancerSegmentHolder> pickOrder;
    private final AtomicInteger pickCounter = new AtomicInteger(0);

    PredefinedPickOrderBalancerStrategy(
        BalancerStrategy delegate,
        List<BalancerSegmentHolder> pickOrder
    )
    {
      this.delegate = delegate;
      this.pickOrder = pickOrder;
    }

    @Override
    public ServerHolder findNewSegmentHomeBalancer(DataSegment proposalSegment, List<ServerHolder> serverHolders)
    {
      return delegate.findNewSegmentHomeBalancer(proposalSegment, serverHolders);
    }

    @Override
    public ServerHolder findNewSegmentHomeReplicator(DataSegment proposalSegment, List<ServerHolder> serverHolders)
    {
      return delegate.findNewSegmentHomeReplicator(proposalSegment, serverHolders);
    }

    @Override
    public BalancerSegmentHolder pickSegmentToMove(
        List<ServerHolder> serverHolders,
        Set<String> broadcastDatasources,
        double percentOfSegmentsToConsider
    )
    {
      return pickOrder.get(pickCounter.getAndIncrement() % pickOrder.size());
    }

    @Override
    public void emitStats(String tier, CoordinatorStats stats, List<ServerHolder> serverHolderList)
    {
      delegate.emitStats(tier, stats, serverHolderList);
    }
  }

  private DruidCoordinatorRuntimeParams setupParamsForDecommissioningMaxPercentOfMaxSegmentsToMove(int percent)
  {
    mockDruidServer(druidServer1, "1", "normal", 30L, 100L, Arrays.asList(segment1, segment3));
    mockDruidServer(druidServer2, "2", "normal", 30L, 100L, Arrays.asList(segment2, segment3));
    mockDruidServer(druidServer3, "3", "normal", 0L, 100L, Collections.emptyList());

    EasyMock.replay(druidServer4);

    mockCoordinator(coordinator);

    // either decommissioning servers list or acitve ones (ie servers list is [2] or [1, 3])
    BalancerStrategy strategy = EasyMock.createMock(BalancerStrategy.class);
    EasyMock.expect(
        strategy.pickSegmentToMove(
            ImmutableList.of(
                new ServerHolder(druidServer2, peon2, true)
            ),
            broadcastDatasources,
            100
        )
    ).andReturn(
        new BalancerSegmentHolder(druidServer2, segment2)
    );
    EasyMock.expect(strategy.pickSegmentToMove(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyInt()))
            .andReturn(new BalancerSegmentHolder(druidServer1, segment1));
    EasyMock.expect(strategy.findNewSegmentHomeBalancer(EasyMock.anyObject(), EasyMock.anyObject()))
            .andReturn(new ServerHolder(druidServer3, peon3))
            .anyTimes();
    EasyMock.replay(strategy);

    return defaultRuntimeParamsBuilder(
        ImmutableList.of(druidServer1, druidServer2, druidServer3),
        ImmutableList.of(peon1, peon2, peon3),
        ImmutableList.of(false, true, false)
    )
        .withDynamicConfigs(
            CoordinatorDynamicConfig.builder()
                                    .withMaxSegmentsToMove(1)
                                    .withDecommissioningMaxPercentOfMaxSegmentsToMove(percent)
                                    .build()
        )
        .withBalancerStrategy(strategy)
        .withBroadcastDatasources(broadcastDatasources)
        .build();
  }
}
