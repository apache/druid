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

public class DiskNormalizedCostBalancerStrategyTest
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
        index * 100L
    );
    return segment;
  }

  @Test
  public void testNormalizedCostBalancerMultiThreadedStrategy()
  {
    List<ServerHolder> serverHolderList = setupDummyCluster(10, 20);
    DataSegment segment = getSegment(1000);

    BalancerStrategy strategy = new DiskNormalizedCostBalancerStrategy(
        MoreExecutors.listeningDecorator(Execs.multiThreaded(4, "DiskNormalizedCostBalancerStrategyTest-%d"))
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

    BalancerStrategy strategy = new DiskNormalizedCostBalancerStrategy(
        MoreExecutors.listeningDecorator(Execs.multiThreaded(1, "DiskNormalizedCostBalancerStrategyTest-%d"))
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
        .mapToObj(DiskNormalizedCostBalancerStrategyTest::getSegment)
        .collect(Collectors.toList());
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
        MoreExecutors.listeningDecorator(Execs.multiThreaded(1, "DiskNormalizedCostBalancerStrategyTest-%d"))
    );
  }

  private static BalancerStrategy newDiskNormalizedStrategy()
  {
    return new DiskNormalizedCostBalancerStrategy(
        MoreExecutors.listeningDecorator(Execs.multiThreaded(1, "DiskNormalizedCostBalancerStrategyTest-%d"))
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

    // DiskNormalized: A = 10 * 0.9 = 9.0, B = 60 * 0.1 = 6.0.
    // The emptier server must win.
    Assert.assertEquals(
        "DiskNormalizedCostBalancerStrategy must prefer the emptier server",
        "B",
        newDiskNormalizedStrategy().findServersToLoadSegment(proposal, servers).next().getServer().getName()
    );
  }

  @Test
  public void testDiskNormalizedFixesSkewThatCostCannotCorrect()
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

    // DiskNormalizedCostBalancerStrategy (default 5% threshold):
    //   A: 38K * 0.80 * 0.95 = 28.88K
    //   B: 40K * 0.20        =  8.00K
    // B wins decisively and the segment moves, reducing the skew.
    final ServerHolder diskNormalizedResult =
        newDiskNormalizedStrategy().findDestinationServerToMoveSegment(segmentToMove, heavy, servers);
    Assert.assertNotNull(
        "DiskNormalized must correct the skew by moving the segment off the heavier server",
        diskNormalizedResult
    );
    Assert.assertEquals("B", diskNormalizedResult.getServer().getName());
  }

  @Test
  public void testThresholdBlocksMarginalMove()
  {
    final long maxSize = 10_000_000L;
    final ServerHolder source = buildServer("SOURCE", maxSize, 8_000_000L, 0, 20);
    final ServerHolder dest = buildServer("DEST", maxSize, 7_400_000L, 100, 20);

    final DataSegment segmentToMove = getSegment(0);
    final List<ServerHolder> servers = new ArrayList<>();
    servers.add(source);
    servers.add(dest);

    // Default threshold (5%): dest is not cheap enough to justify the move.
    Assert.assertNull(
        "Default threshold must block a marginal move to prevent ping-ponging",
        newDiskNormalizedStrategy().findDestinationServerToMoveSegment(segmentToMove, source, servers)
    );

    // threshold=0 removes the discount; the same marginal difference now
    // triggers the move. This proves the threshold is what blocks it above.
    final BalancerStrategy noDiscount = new DiskNormalizedCostBalancerStrategy(
        MoreExecutors.listeningDecorator(Execs.multiThreaded(1, "DiskNormalizedCostBalancerStrategyTest-%d")),
        0.01
    );
    final ServerHolder movedTo = noDiscount.findDestinationServerToMoveSegment(segmentToMove, source, servers);
    Assert.assertNotNull("With threshold=0.01, the marginal move should fire", movedTo);
    Assert.assertEquals("DEST", movedTo.getServer().getName());
  }

  @Test
  public void testRejectsInvalidThreshold()
  {
    try {
      new DiskNormalizedCostBalancerStrategy(
          MoreExecutors.listeningDecorator(Execs.multiThreaded(1, "DiskNormalizedCostBalancerStrategyTest-%d")),
          1.0
      );
      Assert.fail("Expected IllegalArgumentException for threshold=1.0");
    }
    catch (IllegalArgumentException expected) {
      // expected
    }

    try {
      new DiskNormalizedCostBalancerStrategy(
          MoreExecutors.listeningDecorator(Execs.multiThreaded(1, "DiskNormalizedCostBalancerStrategyTest-%d")),
          -0.01
      );
      Assert.fail("Expected IllegalArgumentException for negative threshold");
    }
    catch (IllegalArgumentException expected) {
      // expected
    }
  }
}
