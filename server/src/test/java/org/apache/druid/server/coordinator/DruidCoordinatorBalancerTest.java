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
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.server.coordinator.helper.DruidCoordinatorBalancer;
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
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 */
public class DruidCoordinatorBalancerTest
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
  private List<DataSegment> segments;
  private ListeningExecutorService balancerStrategyExecutor;
  private BalancerStrategy balancerStrategy;

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

    segments = new ArrayList<>();
    segments.add(segment1);
    segments.add(segment2);
    segments.add(segment3);
    segments.add(segment4);

    peon1 = new LoadQueuePeonTester();
    peon2 = new LoadQueuePeonTester();
    peon3 = new LoadQueuePeonTester();
    peon4 = new LoadQueuePeonTester();

    druidServers = ImmutableList.of(druidServer1, druidServer2, druidServer3, druidServer4);
    peons = ImmutableList.of(peon1, peon2, peon3, peon4);

    balancerStrategyExecutor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1));
    balancerStrategy = new CostBalancerStrategyFactory().createBalancerStrategy(balancerStrategyExecutor);
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
        .build();

    params = new DruidCoordinatorBalancerTester(coordinator).run(params);
    Assert.assertEquals(2, params.getCoordinatorStats().getTieredStat("movedCount", "normal"));
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
        .withDynamicConfigs(
            CoordinatorDynamicConfig
                .builder()
                .withMaxSegmentsToMove(MAX_SEGMENTS_TO_MOVE)
                .withMaxSegmentsInNodeLoadingQueue(1)
                .build()
        )
        .build();

    params = new DruidCoordinatorBalancerTester(coordinator).run(params);

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
        .withDynamicConfigs(
            CoordinatorDynamicConfig.builder().withMaxSegmentsToMove(
                2
            ).build()
        )
        .build();

    params = new DruidCoordinatorBalancerTester(coordinator).run(params);
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

    params = new DruidCoordinatorBalancerTester(coordinator).run(params);
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

    params = new DruidCoordinatorBalancerTester(coordinator).run(params);
    Assert.assertTrue(params.getCoordinatorStats().getTieredStat("movedCount", "normal") > 0);
  }

  private DruidCoordinatorRuntimeParams.Builder defaultRuntimeParamsBuilder(
      List<ImmutableDruidServer> druidServers,
      List<LoadQueuePeon> peons
  )
  {
    return DruidCoordinatorRuntimeParams
        .newBuilder()
        .withDruidCluster(
            new DruidCluster(
                null,
                ImmutableMap.of(
                    "normal",
                    IntStream
                        .range(0, druidServers.size())
                        .mapToObj(i -> new ServerHolder(druidServers.get(i), peons.get(i)))
                        .collect(
                            Collectors.toCollection(
                                () -> new TreeSet<>(DruidCoordinatorBalancer.percentUsedComparator)
                            )
                        )
                )
            )
        )
        .withLoadManagementPeons(
            IntStream
                .range(0, peons.size())
                .boxed()
                .collect(Collectors.toMap(i -> String.valueOf(i + 1), peons::get))
        )
        .withAvailableSegments(segments)
        .withDynamicConfigs(
            CoordinatorDynamicConfig.builder().withMaxSegmentsToMove(
                MAX_SEGMENTS_TO_MOVE
            ).build()
        )
        .withBalancerStrategy(balancerStrategy)
        .withBalancerReferenceTimestamp(DateTimes.of("2013-01-01"));
  }

  private void mockDruidServer(
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
    EasyMock.expect(druidServer.getSegments()).andReturn(segments).anyTimes();
    if (!segments.isEmpty()) {
      segments.forEach(
          s -> EasyMock.expect(druidServer.getSegment(s.getId())).andReturn(s).anyTimes()
      );
    } else {
      EasyMock.expect(druidServer.getSegment(EasyMock.anyObject())).andReturn(null).anyTimes();
    }
    EasyMock.replay(druidServer);
  }

  private void mockCoordinator(DruidCoordinator coordinator)
  {
    coordinator.moveSegment(
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

    public PredefinedPickOrderBalancerStrategy(
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
    public BalancerSegmentHolder pickSegmentToMove(List<ServerHolder> serverHolders)
    {
      return pickOrder.get(pickCounter.getAndIncrement() % pickOrder.size());
    }

    @Override
    public void emitStats(String tier, CoordinatorStats stats, List<ServerHolder> serverHolderList)
    {
      delegate.emitStats(tier, stats, serverHolderList);
    }
  }
}
