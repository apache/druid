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

import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.client.DruidServer;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.GranularityType;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.service.AlertEvent;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.simulate.BlockingExecutorService;
import org.apache.druid.timeline.DataSegment;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CostBalancerStrategyTest
{
  private static final double DELTA = 1e-6;
  private static final String DS_WIKI = "wiki";

  private StubServiceEmitter serviceEmitter;
  private ExecutorService balancerExecutor;
  private CostBalancerStrategy strategy;
  private int uniqueServerId;

  @Before
  public void setup()
  {
    balancerExecutor = new BlockingExecutorService("test-balance-exec-%d");
    strategy = new CostBalancerStrategy(MoreExecutors.listeningDecorator(balancerExecutor));

    serviceEmitter = new StubServiceEmitter("test-service", "host");
    EmittingLogger.registerEmitter(serviceEmitter);
  }

  @After
  public void tearDown()
  {
    if (balancerExecutor != null) {
      balancerExecutor.shutdownNow();
    }
  }

  @Test
  public void testIntervalCostAdditivity()
  {
    Assert.assertEquals(
        intervalCost(1, 1, 3),
        intervalCost(1, 1, 2) + intervalCost(1, 2, 3),
        DELTA
    );

    Assert.assertEquals(
        intervalCost(2, 1, 3),
        intervalCost(2, 1, 2) + intervalCost(2, 2, 3),
        DELTA
    );

    Assert.assertEquals(
        intervalCost(3, 1, 2),
        intervalCost(1, 0, 1) + intervalCost(1, 1, 2) + intervalCost(1, 1, 2),
        DELTA
    );
  }

  private double intervalCost(double x1, double y0, double y1)
  {
    return CostBalancerStrategy.intervalCost(x1, y0, y1);
  }

  @Test
  public void testIntervalCost()
  {
    // no overlap
    // [0, 1) [1, 2)
    Assert.assertEquals(0.3995764, intervalCost(1, 1, 2), DELTA);
    // [0, 1) [-1, 0)
    Assert.assertEquals(0.3995764, intervalCost(1, -1, 0), DELTA);

    // exact overlap
    // [0, 1), [0, 1)
    Assert.assertEquals(0.7357589, intervalCost(1, 0, 1), DELTA);
    // [0, 2), [0, 2)
    Assert.assertEquals(2.270671, intervalCost(2, 0, 2), DELTA);

    // partial overlap
    // [0, 2), [1, 3)
    Assert.assertEquals(1.681908, intervalCost(2, 1, 3), DELTA);
    // [0, 2), [1, 2)
    Assert.assertEquals(1.135335, intervalCost(2, 1, 2), DELTA);
    // [0, 2), [0, 1)
    Assert.assertEquals(1.135335, intervalCost(2, 0, 1), DELTA);
    // [0, 3), [1, 2)
    Assert.assertEquals(1.534912, intervalCost(3, 1, 2), DELTA);
  }

  @Test
  public void testJointSegmentsCost()
  {
    final long noGap = 0;
    final long oneDayGap = TimeUnit.DAYS.toMillis(1);
    verifyJointSegmentsCost(GranularityType.HOUR, GranularityType.HOUR, noGap, 1.980884);
    verifyJointSegmentsCost(GranularityType.HOUR, GranularityType.HOUR, oneDayGap, 1.000070);

    verifyJointSegmentsCost(GranularityType.HOUR, GranularityType.DAY, noGap, 35.110275);
    verifyJointSegmentsCost(GranularityType.DAY, GranularityType.DAY, noGap, 926.232308);
    verifyJointSegmentsCost(GranularityType.DAY, GranularityType.DAY, oneDayGap, 599.434267);
    verifyJointSegmentsCost(GranularityType.DAY, GranularityType.DAY, 7 * oneDayGap, 9.366160);

    verifyJointSegmentsCost(GranularityType.DAY, GranularityType.MONTH, noGap, 2125.100840);
    verifyJointSegmentsCost(GranularityType.MONTH, GranularityType.MONTH, noGap, 98247.576470);
    verifyJointSegmentsCost(GranularityType.MONTH, GranularityType.MONTH, 7 * oneDayGap, 79719.068161);

    verifyJointSegmentsCost(GranularityType.MONTH, GranularityType.YEAR, noGap, 100645.313535);
    verifyJointSegmentsCost(GranularityType.YEAR, GranularityType.YEAR, noGap, 1208453.347454);
    verifyJointSegmentsCost(GranularityType.YEAR, GranularityType.YEAR, 7 * oneDayGap, 1189943.571325);
  }

  @Test
  public void testJointSegmentsCostSymmetry()
  {
    final DataSegment segmentA = CreateDataSegments.ofDatasource(DS_WIKI)
                                                   .forIntervals(1, Granularities.DAY)
                                                   .startingAt("2010-01-01")
                                                   .eachOfSizeInMb(100).get(0);
    final DataSegment segmentB = CreateDataSegments.ofDatasource(DS_WIKI)
                                                   .forIntervals(1, Granularities.MONTH)
                                                   .startingAt("2010-01-01")
                                                   .eachOfSizeInMb(100).get(0);

    Assert.assertEquals(
        CostBalancerStrategy.computeJointSegmentsCost(segmentA, segmentB),
        CostBalancerStrategy.computeJointSegmentsCost(segmentB, segmentA),
        DELTA
    );
  }

  @Test
  public void testJointSegmentsCostMultipleDatasources()
  {
    final DataSegment wikiSegment = CreateDataSegments.ofDatasource(DS_WIKI)
                                                      .forIntervals(1, Granularities.DAY)
                                                      .startingAt("2010-01-01")
                                                      .eachOfSizeInMb(100).get(0);
    final DataSegment koalaSegment = CreateDataSegments.ofDatasource("koala")
                                                       .forIntervals(1, Granularities.DAY)
                                                       .startingAt("2010-01-01")
                                                       .eachOfSizeInMb(100).get(0);

    // Verify that cross datasource cost is twice that of same datasource cost
    final double crossDatasourceCost =
        CostBalancerStrategy.computeJointSegmentsCost(koalaSegment, wikiSegment);
    Assert.assertEquals(
        2 * crossDatasourceCost,
        CostBalancerStrategy.computeJointSegmentsCost(wikiSegment, wikiSegment),
        DELTA
    );
    Assert.assertEquals(
        2 * crossDatasourceCost,
        CostBalancerStrategy.computeJointSegmentsCost(koalaSegment, koalaSegment),
        DELTA
    );
  }

  @Test
  public void testJointSegmentsCostWith45DayGap()
  {
    // start of 2nd segment - end of 1st segment = 45 days
    final long gap1Day = TimeUnit.DAYS.toMillis(1);
    final long gap45Days = 45 * gap1Day;

    // This test establishes that after 45 days, all costs become negligible
    // (except with ALL granularity)

    // Add extra gap to ensure that segments have no overlap
    final long gap1Hour = TimeUnit.HOURS.toMillis(1);
    verifyJointSegmentsCost(GranularityType.HOUR, GranularityType.HOUR, gap1Hour + gap45Days, 0);
    verifyJointSegmentsCost(GranularityType.HOUR, GranularityType.DAY, gap1Hour + gap45Days, 0);

    verifyJointSegmentsCost(GranularityType.DAY, GranularityType.DAY, gap1Day + gap45Days, 0);
    verifyJointSegmentsCost(GranularityType.DAY, GranularityType.MONTH, gap1Day + gap45Days, 0);

    verifyJointSegmentsCost(GranularityType.MONTH, GranularityType.MONTH, 30 * gap1Day + gap45Days, 0);
    verifyJointSegmentsCost(GranularityType.YEAR, GranularityType.YEAR, 365 * gap1Day + gap45Days, 0);
  }

  @Test
  public void testJointSegmentsCostAllGranularity()
  {
    // Cost of ALL with other granularities
    verifyJointSegmentsCost(GranularityType.HOUR, GranularityType.ALL, 0, 138.516732);
    verifyJointSegmentsCost(GranularityType.DAY, GranularityType.ALL, 0, 3323.962523);
    verifyJointSegmentsCost(GranularityType.MONTH, GranularityType.ALL, 0, 103043.057744);
    verifyJointSegmentsCost(GranularityType.YEAR, GranularityType.ALL, 0, 1213248.808913);

    // Self cost of an ALL granularity segment
    DataSegment segmentAllGranularity =
        CreateDataSegments.ofDatasource("ds")
                          .forIntervals(1, Granularities.ALL)
                          .eachOfSizeInMb(100).get(0);
    double cost = CostBalancerStrategy.computeJointSegmentsCost(
        segmentAllGranularity,
        segmentAllGranularity
    );
    Assert.assertTrue(cost >= 3.548e14 && cost <= 3.549e14);
  }

  @Test
  public void testComputeCost()
  {
    // Create segments for different granularities
    final List<DataSegment> daySegments =
        CreateDataSegments.ofDatasource(DS_WIKI)
                          .forIntervals(10, Granularities.DAY)
                          .startingAt("2022-01-01")
                          .withNumPartitions(10)
                          .eachOfSizeInMb(100);

    final List<DataSegment> monthSegments =
        CreateDataSegments.ofDatasource(DS_WIKI)
                          .forIntervals(10, Granularities.MONTH)
                          .startingAt("2022-03-01")
                          .withNumPartitions(10)
                          .eachOfSizeInMb(100);

    final List<DataSegment> yearSegments =
        CreateDataSegments.ofDatasource(DS_WIKI)
                          .forIntervals(1, Granularities.YEAR)
                          .startingAt("2023-01-01")
                          .withNumPartitions(30)
                          .eachOfSizeInMb(100);

    // Distribute the segments randomly amongst 2 servers
    final List<DataSegment> segments = new ArrayList<>(daySegments);
    segments.addAll(monthSegments);
    segments.addAll(yearSegments);

    List<DruidServer> historicals = IntStream.range(0, 3)
                                             .mapToObj(i -> createHistorical())
                                             .collect(Collectors.toList());
    final Random random = new Random(100);
    segments.forEach(
        segment -> historicals.get(random.nextInt(historicals.size()))
                              .addDataSegment(segment)
    );

    // Create ServerHolder for each server
    final List<ServerHolder> serverHolders = historicals.stream().map(
        server -> new ServerHolder(server.toImmutableDruidServer(), new LoadQueuePeonTester())
    ).collect(Collectors.toList());

    // Verify costs for DAY, MONTH and YEAR segments
    verifyServerCosts(
        daySegments.get(0),
        serverHolders,
        5191.500804, 8691.392080, 6418.467818
    );
    verifyServerCosts(
        monthSegments.get(0),
        serverHolders,
        301935.940609, 301935.940606, 304333.677669
    );
    verifyServerCosts(
        yearSegments.get(0),
        serverHolders,
        8468764.380437, 12098919.896931, 14501440.169452
    );

    // Verify costs for an ALL granularity segment
    DataSegment allGranularitySegment =
        CreateDataSegments.ofDatasource(DS_WIKI)
                          .forIntervals(1, Granularities.ALL)
                          .eachOfSizeInMb(100).get(0);
    verifyServerCosts(
        allGranularitySegment,
        serverHolders,
        1.1534173737329768e7,
        1.6340633534241956e7,
        1.9026400521582970e7
    );
  }

  @Test
  public void testFindServerAfterExecutorShutdownThrowsException()
  {
    DataSegment segment = CreateDataSegments.ofDatasource(DS_WIKI)
                                            .forIntervals(1, Granularities.DAY)
                                            .startingAt("2012-10-24")
                                            .eachOfSizeInMb(100).get(0);

    ServerHolder serverA = new ServerHolder(createHistorical().toImmutableDruidServer(), null);
    ServerHolder serverB = new ServerHolder(createHistorical().toImmutableDruidServer(), null);

    balancerExecutor.shutdownNow();
    Assert.assertThrows(
        RejectedExecutionException.class,
        () -> strategy.findNewSegmentHomeBalancer(segment, Arrays.asList(serverA, serverB))
    );
  }

  @Test(timeout = 90_000L)
  public void testFindServerRaisesAlertOnTimeout()
  {
    DataSegment segment = CreateDataSegments.ofDatasource(DS_WIKI)
                                            .forIntervals(1, Granularities.DAY)
                                            .startingAt("2012-10-24")
                                            .eachOfSizeInMb(100).get(0);

    ServerHolder serverA = new ServerHolder(createHistorical().toImmutableDruidServer(), null);
    ServerHolder serverB = new ServerHolder(createHistorical().toImmutableDruidServer(), null);

    strategy.findNewSegmentHomeBalancer(segment, Arrays.asList(serverA, serverB));

    List<Event> events = serviceEmitter.getEvents();
    Assert.assertEquals(1, events.size());
    Assert.assertTrue(events.get(0) instanceof AlertEvent);
  }

  private void verifyServerCosts(
      DataSegment segment,
      List<ServerHolder> serverHolders,
      double... expectedCosts
  )
  {
    for (int i = 0; i < serverHolders.size(); ++i) {
      double observedCost = strategy.computeCost(segment, serverHolders.get(i), true);
      Assert.assertEquals(expectedCosts[i], observedCost, DELTA);
    }
  }

  private void verifyJointSegmentsCost(
      GranularityType granularityX,
      GranularityType granularityY,
      long startGapMillis,
      double expectedCost
  )
  {
    final DataSegment segmentX =
        CreateDataSegments.ofDatasource(DS_WIKI)
                          .forIntervals(1, granularityX.getDefaultGranularity())
                          .startingAt("2012-10-24")
                          .eachOfSizeInMb(100).get(0);

    long startTimeY = segmentX.getInterval().getStartMillis() + startGapMillis;
    final DataSegment segmentY =
        CreateDataSegments.ofDatasource(DS_WIKI)
                          .forIntervals(1, granularityY.getDefaultGranularity())
                          .startingAt(startTimeY)
                          .eachOfSizeInMb(100).get(0);

    double observedCost = CostBalancerStrategy.computeJointSegmentsCost(segmentX, segmentY);
    Assert.assertEquals(expectedCost, observedCost, DELTA);
  }

  private DruidServer createHistorical()
  {
    String serverName = "hist_" + uniqueServerId++;
    return new DruidServer(serverName, serverName, null, 10L << 30, ServerType.HISTORICAL, "hot", 1);
  }

}
