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

package org.apache.druid.server.coordinator.balancer;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.client.ImmutableDruidServerTests;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.loading.TestLoadQueuePeon;
import org.apache.druid.timeline.DataSegment;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DiskWeightedCostBalancerStrategyTest
{
  private static final Interval DAY = Intervals.of("2015-01-01T00/2015-01-01T01");

  /**
   * Create Druid cluster with serverCount servers having maxSegments segments each, and 1 server with 98 segment
   * Cost Balancer Strategy should assign the next segment to the server with less segments.
   */
  public static List<ServerHolder> setupDummyCluster(int serverCount, int maxSegments)
  {
    List<ServerHolder> serverHolderList = new ArrayList<>();
    // Create 10 servers with current size being 3K & max size being 10K
    // Each having 100 segments
    for (int i = 0; i < serverCount; i++) {
      TestLoadQueuePeon fromPeon = new TestLoadQueuePeon();

      List<DataSegment> segments = IntStream
          .range(0, maxSegments)
          .mapToObj(j -> getSegment(j))
          .collect(Collectors.toList());
      ImmutableDruidDataSource dataSource = new ImmutableDruidDataSource("DUMMY", Collections.emptyMap(), segments);

      serverHolderList.add(
          new ServerHolder(
              new ImmutableDruidServer(
                  new DruidServerMetadata(
                      "DruidServer_Name_" + i,
                      "localhost",
                      null,
                      10000000L,
                      null,
                      ServerType.HISTORICAL,
                      "hot",
                      1
                  ),
                  3000L,
                  ImmutableMap.of("DUMMY", dataSource),
                  segments.size()
              ),
              fromPeon
          ));
    }

    // The best server to be available for next segment assignment has greater max Size
    TestLoadQueuePeon fromPeon = new TestLoadQueuePeon();
    ImmutableDruidServer druidServer = EasyMock.createMock(ImmutableDruidServer.class);
    EasyMock.expect(druidServer.getName()).andReturn("BEST_SERVER").anyTimes();
    EasyMock.expect(druidServer.getCurrSize()).andReturn(3000L).anyTimes();
    EasyMock.expect(druidServer.getMaxSize()).andReturn(100000000L).anyTimes();

    EasyMock.expect(druidServer.getSegment(EasyMock.anyObject())).andReturn(null).anyTimes();
    List<DataSegment> segments = new ArrayList<>();
    for (int j = 0; j < maxSegments; j++) {
      DataSegment segment = getSegment(j);
      segments.add(segment);
      EasyMock.expect(druidServer.getSegment(segment.getId())).andReturn(segment).anyTimes();
    }
    ImmutableDruidServerTests.expectSegments(druidServer, segments);

    EasyMock.replay(druidServer);
    serverHolderList.add(new ServerHolder(druidServer, fromPeon));
    return serverHolderList;
  }

  /**
   * Returns segment with dummy id and size 100
   *
   * @param index
   *
   * @return segment
   */
  public static DataSegment getSegment(int index)
  {
    return getSegment(index, "DUMMY", DAY);
  }

  public static DataSegment getSegment(int index, String dataSource, Interval interval)
  {
    return getSegment(index, dataSource, interval, index * 100L);
  }

  public static DataSegment getSegment(int index, String dataSource, Interval interval, long size)
  {
    // Not using EasyMock as it hampers the performance of multithreads.
    DataSegment segment = new DataSegment(
        dataSource,
        interval,
        String.valueOf(index),
        new ConcurrentHashMap<>(),
        new ArrayList<>(),
        new ArrayList<>(),
        null,
        0,
        size
    );
    return segment;
  }

  @Test
  public void testNormalizedCostBalancerMultiThreadedStrategy()
  {
    List<ServerHolder> serverHolderList = setupDummyCluster(10, 20);
    DataSegment segment = getSegment(1000);

    BalancerStrategy strategy = new DiskWeightedCostBalancerStrategy(
        MoreExecutors.listeningDecorator(Execs.multiThreaded(4, "DiskWeightedCostBalancerStrategyTest-%d"))
    );
    ServerHolder holder = strategy.findServersToLoadSegment(segment, serverHolderList).next();
    Assert.assertNotNull("Should be able to find a place for new segment!!", holder);
    Assert.assertEquals("Best Server should be BEST_SERVER", "BEST_SERVER", holder.getServer().getName());
  }

  @Test
  public void testNormalizedCostBalancerSingleThreadStrategy()
  {
    List<ServerHolder> serverHolderList = setupDummyCluster(10, 20);
    DataSegment segment = getSegment(1000);

    BalancerStrategy strategy = new DiskWeightedCostBalancerStrategy(
        MoreExecutors.listeningDecorator(Execs.multiThreaded(1, "DiskWeightedCostBalancerStrategyTest-%d"))
    );
    ServerHolder holder = strategy.findServersToLoadSegment(segment, serverHolderList).next();
    Assert.assertNotNull("Should be able to find a place for new segment!!", holder);
    Assert.assertEquals("Best Server should be BEST_SERVER", "BEST_SERVER", holder.getServer().getName());
  }

  /**
   * Builds a ServerHolder with {@code segmentCount} same-datasource DAY-interval
   * segments indexed {@code [baseIndex, baseIndex + segmentCount)}, and
   * {@code sizeUsed} bytes used out of {@code maxSize}.
   */
  private static ServerHolder buildServer(
      String name,
      long maxSize,
      long sizeUsed,
      int baseIndex,
      int segmentCount
  )
  {
    List<DataSegment> segments = IntStream.range(baseIndex, baseIndex + segmentCount)
        .mapToObj(DiskWeightedCostBalancerStrategyTest::getSegment)
        .collect(Collectors.toList());
    return buildServer(name, maxSize, sizeUsed, segments);
  }

  private static ServerHolder buildServer(
      String name,
      long maxSize,
      long sizeUsed,
      List<DataSegment> segments
  )
  {
    ImmutableDruidDataSource ds =
        new ImmutableDruidDataSource("DUMMY", Collections.emptyMap(), segments);
    return new ServerHolder(
        new ImmutableDruidServer(
            new DruidServerMetadata(name, name, null, maxSize, null, ServerType.HISTORICAL, "hot", 1),
            sizeUsed,
            ImmutableMap.of("DUMMY", ds),
            segments.size()
        ),
        new TestLoadQueuePeon()
    );
  }

  private static BalancerStrategy newCostStrategy()
  {
    return new CostBalancerStrategy(
        MoreExecutors.listeningDecorator(Execs.multiThreaded(1, "DiskWeightedCostBalancerStrategyTest-%d"))
    );
  }

  private static BalancerStrategy newDiskWeightedStrategy()
  {
    return new DiskWeightedCostBalancerStrategy(
        MoreExecutors.listeningDecorator(Execs.multiThreaded(1, "DiskWeightedCostBalancerStrategyTest-%d"))
    );
  }

  @Test
  public void testDiskWeightingBeatsRawCost()
  {
    final long maxSize = 10_000_000L;
    // A: 90% usage, 5 overlapping segments -> raw cost ~= 5 * 2K.
    final ServerHolder fuller = buildServer("A", maxSize, 9_000_000L, 0, 5);
    // B: 10% usage, 30 overlapping segments -> raw cost ~= 30 * 2K.
    final ServerHolder emptier = buildServer("B", maxSize, 1_000_000L, 100, 30);

    final DataSegment proposal = getSegment(1000);
    final List<ServerHolder> servers = new ArrayList<>();
    servers.add(fuller);
    servers.add(emptier);

    // Pure CostBalancerStrategy picks A (it has the cheapest raw cost).
    Assert.assertEquals(
        "Pure CostBalancerStrategy should pick the fuller server",
        "A",
        newCostStrategy().findServersToLoadSegment(proposal, servers).next().getServer().getName()
    );

    // DiskWeighted uses projected headroom: A ~= 10K / 0.09, B ~= 60K / 0.89.
    // The emptier server wins despite the higher raw cost.
    Assert.assertEquals(
        "DiskWeightedCostBalancerStrategy must prefer the emptier server",
        "B",
        newDiskWeightedStrategy().findServersToLoadSegment(proposal, servers).next().getServer().getName()
    );
  }

  @Test
  public void testDiskWeightedFixesSkewThatCostCannotCorrect()
  {
    final long maxSize = 10_000_000L;
    // A: 80% full, 20 same-DS DAY segments (indices 0-19).
    final ServerHolder heavy = buildServer("A", maxSize, 8_000_000L, 0, 20);
    // B: 20% full, 20 same-DS DAY segments (indices 100-119 — same
    // interval/datasource, just different segment ids).
    final ServerHolder light = buildServer("B", maxSize, 2_000_000L, 100, 20);

    // The move candidate is one of A's segments.
    final DataSegment segmentToMove = getSegment(0);
    final List<ServerHolder> servers = new ArrayList<>();
    servers.add(heavy);
    servers.add(light);

    // CostBalancerStrategy:
    //   A (source, 20 segs, self-cost subtracted): 38 * K
    //   B (dest,   20 segs, no self-cost):          40 * K
    // A is cheaper by 2K, so the cluster stays skewed forever.
    Assert.assertNull(
        "Pure CostBalancerStrategy cannot correct the disk skew: no move from A to B",
        newCostStrategy().findDestinationServerToMoveSegment(segmentToMove, heavy, servers)
    );

    // DiskWeightedCostBalancerStrategy (default 5% threshold):
    //   A: 38K / 0.20 * 0.95 = 180.5K
    //   B: 40K / 0.80        =  50.0K
    // B wins decisively and the segment moves, reducing the skew.
    final ServerHolder diskWeightedResult =
        newDiskWeightedStrategy().findDestinationServerToMoveSegment(segmentToMove, heavy, servers);
    Assert.assertNotNull(
        "DiskWeighted must correct the skew by moving the segment off the heavier server",
        diskWeightedResult
    );
    Assert.assertEquals("B", diskWeightedResult.getServer().getName());
  }

  @Test
  public void testThresholdBlocksMarginalMove()
  {
    final long maxSize = 10_000_000L;
    final ServerHolder source = buildServer("SOURCE", maxSize, 8_000_000L, 0, 20);
    final ServerHolder dest = buildServer("DEST", maxSize, 7_830_000L, 100, 20);

    final DataSegment segmentToMove = getSegment(0);
    final List<ServerHolder> servers = new ArrayList<>();
    servers.add(source);
    servers.add(dest);

    // Default threshold (5%): dest is not cheap enough to justify the move.
    Assert.assertNull(
        "Default threshold must block a marginal move to prevent ping-ponging",
        newDiskWeightedStrategy().findDestinationServerToMoveSegment(segmentToMove, source, servers)
    );

    // Lowering the threshold to 1% reduces the discount; the same marginal
    // difference now triggers the move. This proves the threshold is what
    // blocks it above.
    final BalancerStrategy onePercentThreshold = new DiskWeightedCostBalancerStrategy(
        MoreExecutors.listeningDecorator(Execs.multiThreaded(1, "DiskWeightedCostBalancerStrategyTest-%d")),
        0.01
    );
    final ServerHolder movedTo = onePercentThreshold.findDestinationServerToMoveSegment(segmentToMove, source, servers);
    Assert.assertNotNull("With threshold=0.01, the marginal move should fire", movedTo);
    Assert.assertEquals("DEST", movedTo.getServer().getName());
  }

  @Test
  public void testNearFullServerIsNotChosenForNewSegmentLoad()
  {
    final long maxSize = 10_000_000L;
    // A: 95% full, 5 same-DS DAY segments -> raw cost = 10 * K (low, few co-located segs)
    final ServerHolder nearFull = buildServer("A", maxSize, 9_500_000L, 0, 5);
    // B: 70% full, 20 same-DS DAY segments -> raw cost = 40 * K (higher, more co-located)
    final ServerHolder partial = buildServer("B", maxSize, 7_000_000L, 100, 20);

    final DataSegment newSegment = getSegment(1000);
    final List<ServerHolder> servers = new ArrayList<>();
    servers.add(nearFull);
    servers.add(partial);

    // CostBalancerStrategy picks A because raw cost 10K < 40K.
    Assert.assertEquals(
        "Pure CostBalancerStrategy must pick the near-full server (lower raw cost)",
        "A",
        newCostStrategy().findServersToLoadSegment(newSegment, servers).next().getServer().getName()
    );

    // DiskWeighted uses projected headroom: A_norm = 10K / 0.04 = 250K,
    // B_norm = 40K / 0.29 = 138K -> B wins.
    Assert.assertEquals(
        "DiskWeighted must prefer the emptier server despite its higher raw cost",
        "B",
        newDiskWeightedStrategy().findServersToLoadSegment(newSegment, servers).next().getServer().getName()
    );
  }

  @Test
  public void testProjectedSegmentSizeIsUsedForNewSegmentLoad()
  {
    final long maxSize = 1_000_000L;
    // A has the lower raw cost, but the 250 KB proposal would leave only 5% headroom.
    final ServerHolder almostFullAfterLoad = buildServer("A", maxSize, 700_000L, 0, 5);
    // B has more co-located segments, but keeps 25% headroom after the proposal.
    final ServerHolder moreHeadroomAfterLoad = buildServer("B", maxSize, 500_000L, 100, 20);

    final DataSegment largeSegment = getSegment(1000, "DUMMY", DAY, 250_000L);
    final List<ServerHolder> servers = new ArrayList<>();
    servers.add(almostFullAfterLoad);
    servers.add(moreHeadroomAfterLoad);

    // CostBalancerStrategy picks A because raw cost 10K < 40K.
    Assert.assertEquals(
        "Pure CostBalancerStrategy must pick the lower raw-cost server",
        "A",
        newCostStrategy().findServersToLoadSegment(largeSegment, servers).next().getServer().getName()
    );

    // If diskWeighted used current headroom, A would also win:
    //   A_current = 10K / 0.30, B_current = 40K / 0.50.
    // With projected headroom, B wins:
    //   A_projected = 10K / 0.05, B_projected = 40K / 0.25.
    Assert.assertEquals(
        "DiskWeighted must account for the proposal size before choosing a server",
        "B",
        newDiskWeightedStrategy().findServersToLoadSegment(largeSegment, servers).next().getServer().getName()
    );
  }

  @Test
  public void testNearFullServerIsNotChosenAsMoveDestination()
  {
    final long maxSize = 10_000_000L;
    // SOURCE: 70% full, 20 same-DS DAY segments; segmentToMove is one of them.
    final ServerHolder source = buildServer("SOURCE", maxSize, 7_000_000L, 0, 20);
    // DEST: 95% full, 5 same-DS DAY segments -> raw cost 10K < SOURCE's 38K.
    final ServerHolder nearFullDest = buildServer("DEST", maxSize, 9_500_000L, 100, 5);

    final DataSegment segmentToMove = getSegment(0);
    final List<ServerHolder> servers = new ArrayList<>();
    servers.add(source);
    servers.add(nearFullDest);

    // CostBalancerStrategy: DEST raw cost (10K) < SOURCE raw cost (38K) -> recommends the move.
    final ServerHolder costResult =
        newCostStrategy().findDestinationServerToMoveSegment(segmentToMove, source, servers);
    Assert.assertNotNull("CostBalancerStrategy must recommend moving to the near-full DEST", costResult);
    Assert.assertEquals("DEST", costResult.getServer().getName());

    // DiskWeighted: DEST_norm = 10K / 0.05 = 200K > SOURCE_norm = 38K / 0.30 * 0.95 ≈ 120K.
    // Near-full DEST is too expensive after normalization -> no move.
    Assert.assertNull(
        "DiskWeighted must block the move to the near-full server",
        newDiskWeightedStrategy().findDestinationServerToMoveSegment(segmentToMove, source, servers)
    );
  }

  @Test
  public void testProjectedSegmentSizePreventsMoveThatWouldFillDestination()
  {
    final long maxSize = 10_000_000L;
    final DataSegment largeSegment = getSegment(0, "DUMMY", DAY, 2_500_000L);
    final List<DataSegment> sourceSegments = new ArrayList<>();
    sourceSegments.add(largeSegment);
    IntStream.range(1, 20)
             .mapToObj(DiskWeightedCostBalancerStrategyTest::getSegment)
             .forEach(sourceSegments::add);

    // SOURCE is fuller before the move, but already projects the segment.
    final ServerHolder source = buildServer("SOURCE", maxSize, 8_000_000L, sourceSegments);
    // DEST has low raw cost, but loading the 2.5 MB segment would leave only 5% headroom.
    final ServerHolder dest = buildServer("DEST", maxSize, 7_000_000L, 100, 5);

    final List<ServerHolder> servers = new ArrayList<>();
    servers.add(source);
    servers.add(dest);

    // CostBalancerStrategy recommends the move because DEST raw cost (10K) < SOURCE raw cost (38K).
    final ServerHolder costResult =
        newCostStrategy().findDestinationServerToMoveSegment(largeSegment, source, servers);
    Assert.assertNotNull("CostBalancerStrategy must recommend moving to the lower raw-cost DEST", costResult);
    Assert.assertEquals("DEST", costResult.getServer().getName());

    // If diskWeighted used current headroom, DEST would win: 10K / 0.30 < 38K / 0.20 * 0.95.
    // With projected headroom, DEST is too full after placement: 10K / 0.05 > 38K / 0.20 * 0.95.
    Assert.assertNull(
        "DiskWeighted must not move a large segment to a server that would become too full",
        newDiskWeightedStrategy().findDestinationServerToMoveSegment(largeSegment, source, servers)
    );
  }

  @Test
  public void testRejectsInvalidThreshold()
  {
    try {
      new DiskWeightedCostBalancerStrategy(
          MoreExecutors.listeningDecorator(Execs.multiThreaded(1, "DiskWeightedCostBalancerStrategyTest-%d")),
          1.0
      );
      Assert.fail("Expected IllegalArgumentException for threshold=1.0");
    }
    catch (IllegalArgumentException expected) {
      // expected
    }

    try {
      new DiskWeightedCostBalancerStrategy(
          MoreExecutors.listeningDecorator(Execs.multiThreaded(1, "DiskWeightedCostBalancerStrategyTest-%d")),
          -0.01
      );
      Assert.fail("Expected IllegalArgumentException for negative threshold");
    }
    catch (IllegalArgumentException expected) {
      // expected
    }
  }
}
