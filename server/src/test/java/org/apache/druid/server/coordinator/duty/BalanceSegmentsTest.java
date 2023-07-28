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

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.client.DruidServer;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.balancer.BalancerStrategy;
import org.apache.druid.server.coordinator.balancer.CostBalancerStrategyFactory;
import org.apache.druid.server.coordinator.loading.SegmentLoadQueueManager;
import org.apache.druid.server.coordinator.loading.TestLoadQueuePeon;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.Stats;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Set;

public class BalanceSegmentsTest
{
  private SegmentLoadQueueManager loadQueueManager;

  private DataSegment segment1;
  private DataSegment segment2;
  private DataSegment segment3;
  private DataSegment segment4;
  private DataSegment segment5;

  private DataSegment[] allSegments;

  private ListeningExecutorService balancerStrategyExecutor;
  private BalancerStrategy balancerStrategy;
  private Set<String> broadcastDatasources;

  private DruidServer server1;
  private DruidServer server2;
  private DruidServer server3;
  private DruidServer server4;

  @Before
  public void setUp()
  {
    loadQueueManager = new SegmentLoadQueueManager(null, null, null);

    // Create test segments for multiple datasources
    final DateTime start1 = DateTimes.of("2012-01-01");
    final DateTime start2 = DateTimes.of("2012-02-01");
    final String version = DateTimes.of("2012-03-01").toString();

    segment1 = createHourlySegment("datasource1", start1, version);
    segment2 = createHourlySegment("datasource1", start2, version);
    segment3 = createHourlySegment("datasource2", start1, version);
    segment4 = createHourlySegment("datasource2", start2, version);
    segment5 = createHourlySegment("datasourceBroadcast", start2, version);
    allSegments = new DataSegment[]{segment1, segment2, segment3, segment4, segment5};

    server1 = new DruidServer("server1", "server1", null, 100L, ServerType.HISTORICAL, "normal", 0);
    server2 = new DruidServer("server2", "server2", null, 100L, ServerType.HISTORICAL, "normal", 0);
    server3 = new DruidServer("server3", "server3", null, 100L, ServerType.HISTORICAL, "normal", 0);
    server4 = new DruidServer("server4", "server4", null, 100L, ServerType.HISTORICAL, "normal", 0);

    balancerStrategyExecutor = MoreExecutors.listeningDecorator(Execs.multiThreaded(1, "BalanceSegmentsTest-%d"));
    balancerStrategy = new CostBalancerStrategyFactory().createBalancerStrategy(balancerStrategyExecutor);

    broadcastDatasources = Collections.singleton("datasourceBroadcast");
  }

  @After
  public void tearDown()
  {
    balancerStrategyExecutor.shutdownNow();
  }

  @Test
  public void testMoveToEmptyServerBalancer()
  {
    final ServerHolder serverHolder1 = createHolder(server1, segment1, segment2, segment3, segment4);
    final ServerHolder serverHolder2 = createHolder(server2);

    DruidCoordinatorRuntimeParams params =
        defaultRuntimeParamsBuilder(serverHolder1, serverHolder2)
            .withBalancerStrategy(balancerStrategy)
            .withBroadcastDatasources(broadcastDatasources)
            .build();

    CoordinatorRunStats stats = runBalancer(params);
    long totalMoved = stats.getSegmentStat(Stats.Segments.MOVED, "normal", "datasource1")
                      + stats.getSegmentStat(Stats.Segments.MOVED, "normal", "datasource2");
    Assert.assertEquals(2L, totalMoved);
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
    final ServerHolder serverHolder1 = createHolder(server1, true, segment1, segment2, segment3, segment4);
    final ServerHolder serverHolder2 = createHolder(server2, false);

    // ceil(3 * 0.6) = 2 segments from decommissioning servers
    CoordinatorDynamicConfig dynamicConfig =
        CoordinatorDynamicConfig.builder()
                                .withSmartSegmentLoading(false)
                                .withMaxSegmentsToMove(3)
                                .withDecommissioningMaxPercentOfMaxSegmentsToMove(60)
                                .build();
    DruidCoordinatorRuntimeParams params =
        defaultRuntimeParamsBuilder(serverHolder1, serverHolder2)
            .withDynamicConfigs(dynamicConfig)
            .withBalancerStrategy(balancerStrategy)
            .withBroadcastDatasources(broadcastDatasources)
            .withSegmentAssignerUsing(loadQueueManager)
            .build();

    CoordinatorRunStats stats = runBalancer(params);

    // 2 segments are moved from the decommissioning server
    long totalMoved = stats.getSegmentStat(Stats.Segments.MOVED, "normal", "datasource1")
                      + stats.getSegmentStat(Stats.Segments.MOVED, "normal", "datasource2");
    Assert.assertEquals(2L, totalMoved);
    Set<DataSegment> segmentsMoved = serverHolder2.getPeon().getSegmentsToLoad();
    Assert.assertEquals(2, segmentsMoved.size());
  }

  @Test
  public void testZeroDecommissioningMaxPercentOfMaxSegmentsToMove()
  {
    final ServerHolder holder1 = createHolder(server1, false, segment1, segment2);
    final ServerHolder holder2 = createHolder(server2, true, segment3, segment4);
    final ServerHolder holder3 = createHolder(server3, false);

    CoordinatorDynamicConfig dynamicConfig =
        CoordinatorDynamicConfig.builder()
                                .withSmartSegmentLoading(false)
                                .withDecommissioningMaxPercentOfMaxSegmentsToMove(0)
                                .withMaxSegmentsToMove(1).build();
    DruidCoordinatorRuntimeParams params =
        defaultRuntimeParamsBuilder(holder1, holder2, holder3).withDynamicConfigs(dynamicConfig).build();

    CoordinatorRunStats stats = runBalancer(params);

    // Verify that either segment1 or segment2 is chosen for move
    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.MOVED, "normal", segment1.getDataSource()));
    DataSegment movingSegment = holder3.getPeon().getSegmentsToLoad().iterator().next();
    Assert.assertEquals(segment1.getDataSource(), movingSegment.getDataSource());
  }

  @Test
  public void testMaxDecommissioningMaxPercentOfMaxSegmentsToMove()
  {
    final ServerHolder holder1 = createHolder(server1, false, segment1, segment2);
    final ServerHolder holder2 = createHolder(server2, true, segment3, segment4);
    final ServerHolder holder3 = createHolder(server3, false);

    CoordinatorDynamicConfig dynamicConfig =
        CoordinatorDynamicConfig.builder()
                                .withSmartSegmentLoading(false)
                                .withDecommissioningMaxPercentOfMaxSegmentsToMove(100)
                                .withMaxSegmentsToMove(1).build();
    DruidCoordinatorRuntimeParams params =
        defaultRuntimeParamsBuilder(holder1, holder2, holder3).withDynamicConfigs(dynamicConfig).build();

    CoordinatorRunStats stats = runBalancer(params);

    // Verify that either segment3 or segment4 is chosen for move
    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.MOVED, "normal", segment3.getDataSource()));
    DataSegment movingSegment = holder3.getPeon().getSegmentsToLoad().iterator().next();
    Assert.assertEquals(segment3.getDataSource(), movingSegment.getDataSource());
  }

  /**
   * Should balance segments as usual (ignoring percent) with empty decommissioningNodes.
   */
  @Test
  public void testMoveDecommissioningMaxPercentOfMaxSegmentsToMoveWithNoDecommissioning()
  {
    final ServerHolder serverHolder1 = createHolder(server1, segment1, segment2, segment3, segment4);
    final ServerHolder serverHolder2 = createHolder(server2);

    CoordinatorDynamicConfig dynamicConfig =
        CoordinatorDynamicConfig.builder()
                                .withSmartSegmentLoading(false)
                                .withMaxSegmentsToMove(4)
                                .withDecommissioningMaxPercentOfMaxSegmentsToMove(9)
                                .build();
    DruidCoordinatorRuntimeParams params =
        defaultRuntimeParamsBuilder(serverHolder1, serverHolder2)
            .withDynamicConfigs(dynamicConfig)
            .withBalancerStrategy(balancerStrategy)
            .withSegmentAssignerUsing(loadQueueManager)
            .build();

    CoordinatorRunStats stats = runBalancer(params);
    long totalMoved = stats.getSegmentStat(Stats.Segments.MOVED, "normal", "datasource1")
                      + stats.getSegmentStat(Stats.Segments.MOVED, "normal", "datasource2");
    Assert.assertEquals(2L, totalMoved);
    Assert.assertEquals(2, serverHolder2.getPeon().getSegmentsToLoad().size());
  }

  /**
   * Shouldn't move segments to a decommissioning server.
   */
  @Test
  public void testMoveToDecommissioningServer()
  {
    final ServerHolder activeServer = createHolder(server1, false, allSegments);
    final ServerHolder decommissioningServer = createHolder(server2, true);

    DruidCoordinatorRuntimeParams params =
        defaultRuntimeParamsBuilder(activeServer, decommissioningServer)
            .withBalancerStrategy(balancerStrategy)
            .withBroadcastDatasources(broadcastDatasources)
            .build();

    CoordinatorRunStats stats = runBalancer(params);
    Assert.assertFalse(stats.hasStat(Stats.Segments.MOVED));
  }

  @Test
  public void testMoveFromDecommissioningServer()
  {
    final ServerHolder decommissioningServer = createHolder(server1, true, allSegments);
    final ServerHolder activeServer = createHolder(server2);

    DruidCoordinatorRuntimeParams params = defaultRuntimeParamsBuilder(decommissioningServer, activeServer)
        .withDynamicConfigs(
            CoordinatorDynamicConfig.builder()
                                    .withSmartSegmentLoading(false)
                                    .withMaxSegmentsToMove(3).build()
        )
        .withBalancerStrategy(balancerStrategy)
        .withBroadcastDatasources(broadcastDatasources)
        .build();

    runBalancer(params);
    Assert.assertEquals(0, decommissioningServer.getPeon().getSegmentsToLoad().size());
    Assert.assertEquals(3, activeServer.getPeon().getSegmentsToLoad().size());
  }

  @Test
  public void testMoveMaxLoadQueueServerBalancer()
  {
    final int maxSegmentsInQueue = 1;
    final ServerHolder holder1 = createHolder(server1, maxSegmentsInQueue, false, allSegments);
    final ServerHolder holder2 = createHolder(server2, maxSegmentsInQueue, false);

    final CoordinatorDynamicConfig dynamicConfig = CoordinatorDynamicConfig
        .builder()
        .withSmartSegmentLoading(false)
        .withMaxSegmentsInNodeLoadingQueue(maxSegmentsInQueue)
        .build();
    DruidCoordinatorRuntimeParams params =
        defaultRuntimeParamsBuilder(holder1, holder2)
            .withDynamicConfigs(dynamicConfig)
            .build();

    CoordinatorRunStats stats = runBalancer(params);

    // max to move is 5, all segments on server 1, but only expect to move 1 to server 2 since max node load queue is 1
    Assert.assertEquals(maxSegmentsInQueue, stats.getSegmentStat(Stats.Segments.MOVED, "normal", "datasource1"));
  }

  @Test
  public void testRun1()
  {
    // Mock some servers of different usages
    DruidCoordinatorRuntimeParams params = defaultRuntimeParamsBuilder(
        createHolder(server1, allSegments),
        createHolder(server2)
    ).build();

    CoordinatorRunStats stats = runBalancer(params);
    Assert.assertTrue(stats.getSegmentStat(Stats.Segments.MOVED, "normal", "datasource1") > 0);
  }

  @Test
  public void testRun2()
  {
    // Mock some servers of different usages
    DruidCoordinatorRuntimeParams params = defaultRuntimeParamsBuilder(
        createHolder(server1, allSegments),
        createHolder(server2),
        createHolder(server3),
        createHolder(server4)
    ).build();

    CoordinatorRunStats stats = runBalancer(params);
    Assert.assertTrue(stats.getSegmentStat(Stats.Segments.MOVED, "normal", "datasource1") > 0);
  }

  @Test
  public void testMaxSegmentsToMoveIsHonored()
  {
    // Move from non-decomissioning servers
    final ServerHolder holder1 = createHolder(server1, segment1, segment2);
    final ServerHolder holder2 = createHolder(server2, segment3, segment4);
    final ServerHolder holder3 = createHolder(server3);

    DruidCoordinatorRuntimeParams params =
        defaultRuntimeParamsBuilder(holder1, holder2, holder3)
            .withDynamicConfigs(
                CoordinatorDynamicConfig.builder()
                                        .withSmartSegmentLoading(false)
                                        .withMaxSegmentsToMove(1)
                                        .build()
            )
            .withBalancerStrategy(balancerStrategy)
            .withBroadcastDatasources(broadcastDatasources)
            .build();

    CoordinatorRunStats stats = runBalancer(params);
    long totalMoved = stats.getSegmentStat(Stats.Segments.MOVED, "normal", "datasource1")
                      + stats.getSegmentStat(Stats.Segments.MOVED, "normal", "datasource2");
    Assert.assertEquals(1L, totalMoved);
    Assert.assertEquals(1, holder3.getPeon().getSegmentsToLoad().size());
  }

  @Test
  public void testMoveForMultipleDatasources()
  {
    DruidCoordinatorRuntimeParams params = defaultRuntimeParamsBuilder(
        createHolder(server1, allSegments),
        createHolder(server2),
        createHolder(server3),
        createHolder(server4)
    )
        .withDynamicConfigs(
            CoordinatorDynamicConfig.builder()
                                    .withSmartSegmentLoading(false)
                                    .withMaxSegmentsToMove(2)
                                    .build()
        )
        .withBroadcastDatasources(broadcastDatasources)
        .build();

    CoordinatorRunStats stats = runBalancer(params);
    long totalMoved = stats.getSegmentStat(Stats.Segments.MOVED, "normal", "datasource1")
                      + stats.getSegmentStat(Stats.Segments.MOVED, "normal", "datasource2");
    Assert.assertEquals(2L, totalMoved);
  }

  private CoordinatorRunStats runBalancer(DruidCoordinatorRuntimeParams params)
  {
    params = new BalanceSegments().run(params);
    if (params == null) {
      Assert.fail("BalanceSegments duty returned null params");
      return new CoordinatorRunStats();
    } else {
      return params.getCoordinatorStats();
    }
  }

  private DruidCoordinatorRuntimeParams.Builder defaultRuntimeParamsBuilder(
      ServerHolder... servers
  )
  {
    return DruidCoordinatorRuntimeParams
        .newBuilder(DateTimes.nowUtc())
        .withDruidCluster(DruidCluster.builder().addTier("normal", servers).build())
        .withUsedSegmentsInTest(allSegments)
        .withBroadcastDatasources(broadcastDatasources)
        .withBalancerStrategy(balancerStrategy)
        .withSegmentAssignerUsing(loadQueueManager);
  }

  private ServerHolder createHolder(DruidServer server, DataSegment... loadedSegments)
  {
    return createHolder(server, false, loadedSegments);
  }

  private ServerHolder createHolder(DruidServer server, boolean isDecommissioning, DataSegment... loadedSegments)
  {
    return createHolder(server, 0, isDecommissioning, loadedSegments);
  }

  private ServerHolder createHolder(
      DruidServer server,
      int maxSegmentsInLoadQueue,
      boolean isDecommissioning,
      DataSegment... loadedSegments
  )
  {
    for (DataSegment segment : loadedSegments) {
      server.addDataSegment(segment);
    }

    return new ServerHolder(
        server.toImmutableDruidServer(),
        new TestLoadQueuePeon(),
        isDecommissioning,
        maxSegmentsInLoadQueue,
        10
    );
  }

  private DataSegment createHourlySegment(String datasource, DateTime start, String version)
  {
    return new DataSegment(
        datasource,
        new Interval(start, start.plusHours(1)),
        version,
        Collections.emptyMap(),
        Collections.emptyList(),
        Collections.emptyList(),
        NoneShardSpec.instance(),
        0,
        8L
    );
  }
}
