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

import org.apache.druid.client.DruidServer;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.loading.TestLoadQueuePeon;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class SegmentToMoveCalculatorTest
{

  private static final Duration DEFAULT_COORDINATOR_PERIOD = Duration.standardMinutes(1);

  /**
   * 100 days x 100 partitions = 10,000 segments.
   */
  private static final List<DataSegment> WIKI_SEGMENTS
      = CreateDataSegments.ofDatasource("wiki")
                          .forIntervals(100, Granularities.DAY)
                          .withNumPartitions(100)
                          .eachOfSizeInMb(500);

  /**
   * 10 days * 1 partitions = 10 segments.
   */
  private static final List<DataSegment> KOALA_SEGMENTS
      = CreateDataSegments.ofDatasource("koala")
                          .forIntervals(10, Granularities.DAY)
                          .eachOfSizeInMb(500);

  private static final String TIER = "tier1";

  @Test
  public void testMaxSegmentsToMove1Thread()
  {
    Assert.assertEquals(0, computeMaxSegmentsToMove(0, 1));
    Assert.assertEquals(50, computeMaxSegmentsToMove(50, 1));
    Assert.assertEquals(100, computeMaxSegmentsToMove(100, 1));

    Assert.assertEquals(100, computeMaxSegmentsToMove(512, 1));
    Assert.assertEquals(200, computeMaxSegmentsToMove(1_024, 1));
    Assert.assertEquals(300, computeMaxSegmentsToMove(1_536, 1));

    Assert.assertEquals(1_900, computeMaxSegmentsToMove(10_000, 1));
    Assert.assertEquals(9_700, computeMaxSegmentsToMove(50_000, 1));
    Assert.assertEquals(19_500, computeMaxSegmentsToMove(100_000, 1));

    Assert.assertEquals(10_000, computeMaxSegmentsToMove(200_000, 1));
    Assert.assertEquals(4_000, computeMaxSegmentsToMove(500_000, 1));
    Assert.assertEquals(2_000, computeMaxSegmentsToMove(1_000_000, 1));
  }

  @Test
  public void testMaxSegmentsToMoveIncreasesWithCoordinatorPeriod()
  {
    Assert.assertEquals(5_000, computeMaxSegmentsToMoveInPeriod(200_000, Duration.millis(30_000)));
    Assert.assertEquals(10_000, computeMaxSegmentsToMoveInPeriod(200_000, Duration.millis(60_000)));
    Assert.assertEquals(15_000, computeMaxSegmentsToMoveInPeriod(200_000, Duration.millis(90_000)));
    Assert.assertEquals(20_000, computeMaxSegmentsToMoveInPeriod(200_000, Duration.millis(120_000)));

    Assert.assertEquals(2_000, computeMaxSegmentsToMoveInPeriod(500_000, Duration.millis(30_000)));
    Assert.assertEquals(4_000, computeMaxSegmentsToMoveInPeriod(500_000, Duration.millis(60_000)));
    Assert.assertEquals(6_000, computeMaxSegmentsToMoveInPeriod(500_000, Duration.millis(90_000)));
    Assert.assertEquals(8_000, computeMaxSegmentsToMoveInPeriod(500_000, Duration.millis(120_000)));
  }

  @Test
  public void testMaxSegmentsToMove8Threads()
  {
    Assert.assertEquals(0, computeMaxSegmentsToMove(0, 8));
    Assert.assertEquals(50, computeMaxSegmentsToMove(50, 8));
    Assert.assertEquals(100, computeMaxSegmentsToMove(100, 8));

    Assert.assertEquals(100, computeMaxSegmentsToMove(512, 8));
    Assert.assertEquals(200, computeMaxSegmentsToMove(1_024, 8));
    Assert.assertEquals(300, computeMaxSegmentsToMove(1_536, 8));

    Assert.assertEquals(33_000, computeMaxSegmentsToMove(500_000, 8));
    Assert.assertEquals(16_000, computeMaxSegmentsToMove(1_000_000, 8));
    Assert.assertEquals(8_000, computeMaxSegmentsToMove(2_000_000, 8));
    Assert.assertEquals(3_000, computeMaxSegmentsToMove(5_000_000, 8));
    Assert.assertEquals(1_000, computeMaxSegmentsToMove(10_000_000, 8));
  }

  @Test
  public void testMinSegmentsToMove()
  {
    Assert.assertEquals(0, computeMinSegmentsToMove(0));
    Assert.assertEquals(50, computeMinSegmentsToMove(50));

    Assert.assertEquals(100, computeMinSegmentsToMove(100));
    Assert.assertEquals(100, computeMinSegmentsToMove(1_000));

    Assert.assertEquals(100, computeMinSegmentsToMove(20_000));
    Assert.assertEquals(100, computeMinSegmentsToMove(50_000));
    Assert.assertEquals(100, computeMinSegmentsToMove(100_000));
    Assert.assertEquals(300, computeMinSegmentsToMove(200_000));
    Assert.assertEquals(700, computeMinSegmentsToMove(500_000));
    Assert.assertEquals(1_500, computeMinSegmentsToMove(1_000_000));
    Assert.assertEquals(15_200, computeMinSegmentsToMove(10_000_000));
  }

  @Test
  public void testMinSegmentsToMoveIncreasesInSteps()
  {
    Assert.assertEquals(100, computeMinSegmentsToMove(131_071));
    Assert.assertEquals(200, computeMinSegmentsToMove(131_072));

    Assert.assertEquals(500, computeMinSegmentsToMove(393_215));
    Assert.assertEquals(600, computeMinSegmentsToMove(393_216));

    Assert.assertEquals(900, computeMinSegmentsToMove(655_359));
    Assert.assertEquals(1000, computeMinSegmentsToMove(655_360));

    Assert.assertEquals(9_900, computeMinSegmentsToMove(6_553_599));
    Assert.assertEquals(10_000, computeMinSegmentsToMove(6_553_600));
  }

  @Test
  public void testMinSegmentsArePickedForMoveWhenNoSkew()
  {
    final List<ServerHolder> historicals = Arrays.asList(
        createServer("A", WIKI_SEGMENTS),
        createServer("B", WIKI_SEGMENTS)
    );

    final int minSegmentsToMove = SegmentToMoveCalculator.computeMinSegmentsToMoveInTier(20_000);
    Assert.assertEquals(100, minSegmentsToMove);

    final int segmentsToMoveToFixSkew = SegmentToMoveCalculator
        .computeNumSegmentsToMoveToBalanceTier(TIER, historicals);
    Assert.assertEquals(0, segmentsToMoveToFixSkew);

    // Find segmentsToMove with no limit on maxSegmentsToMove
    final int segmentsToMove = SegmentToMoveCalculator
        .computeNumSegmentsToMoveInTier(TIER, historicals, Integer.MAX_VALUE);

    Assert.assertEquals(minSegmentsToMove, segmentsToMove);
  }

  @Test
  public void testHalfSegmentsArePickedForMoveWhenFullSkew()
  {
    final List<ServerHolder> historicals = Arrays.asList(
        createServer("A", WIKI_SEGMENTS),
        createServer("B", Collections.emptyList())
    );

    final int minSegmentsToMove = SegmentToMoveCalculator.computeMinSegmentsToMoveInTier(10_000);
    Assert.assertEquals(100, minSegmentsToMove);

    final int segmentsToMoveToFixSkew = SegmentToMoveCalculator
        .computeNumSegmentsToMoveToBalanceTier(TIER, historicals);
    Assert.assertEquals(5_000, segmentsToMoveToFixSkew);

    // Find segmentsToMove with no limit on maxSegmentsToMove
    final int segmentsToMove = SegmentToMoveCalculator
        .computeNumSegmentsToMoveInTier(TIER, historicals, Integer.MAX_VALUE);

    Assert.assertEquals(segmentsToMoveToFixSkew, segmentsToMove);
  }

  @Test
  public void testDatasourceWithLargestGapDeterminesNumToBalanceCounts()
  {
    // Both servers have all koala segments but only A has wiki segments
    List<DataSegment> segmentsForServerA = new ArrayList<>(WIKI_SEGMENTS);
    segmentsForServerA.addAll(KOALA_SEGMENTS);

    final List<ServerHolder> historicals = Arrays.asList(
        createServer("A", segmentsForServerA),
        createServer("B", KOALA_SEGMENTS)
    );

    // Verify that half the wiki segments need to be moved for balance
    int numToMoveToBalanceCount = SegmentToMoveCalculator
        .computeSegmentsToMoveToBalanceCountsPerDatasource(TIER, historicals);
    Assert.assertEquals(WIKI_SEGMENTS.size() / 2, numToMoveToBalanceCount);
  }

  private static int computeMaxSegmentsToMove(int totalSegments, int numThreads)
  {
    return SegmentToMoveCalculator.computeMaxSegmentsToMovePerTier(
        totalSegments,
        numThreads,
        DEFAULT_COORDINATOR_PERIOD
    );
  }

  private static int computeMaxSegmentsToMoveInPeriod(int totalSegments, Duration coordinatorPeriod)
  {
    return SegmentToMoveCalculator.computeMaxSegmentsToMovePerTier(totalSegments, 1, coordinatorPeriod);
  }

  private static int computeMinSegmentsToMove(int totalSegmentsInTier)
  {
    return SegmentToMoveCalculator.computeMinSegmentsToMoveInTier(totalSegmentsInTier);
  }

  private static ServerHolder createServer(String name, List<DataSegment> segments)
  {
    final DruidServer server
        = new DruidServer(name, name, null, 10L << 30, ServerType.HISTORICAL, "tier1", 1);
    segments.forEach(server::addDataSegment);
    return new ServerHolder(server.toImmutableDruidServer(), new TestLoadQueuePeon());
  }
}
