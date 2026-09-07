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
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.loading.TestLoadQueuePeon;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Duration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
      = CreateDataSegments.ofDatasource(TestDataSource.WIKI)
                          .forIntervals(100, Granularities.DAY)
                          .withNumPartitions(100)
                          .eachOfSizeInMb(500);

  /**
   * 10 days * 1 partitions = 10 segments.
   */
  private static final List<DataSegment> KOALA_SEGMENTS
      = CreateDataSegments.ofDatasource(TestDataSource.KOALA)
                          .forIntervals(10, Granularities.DAY)
                          .eachOfSizeInMb(500);

  private static final String TIER = "tier1";

  @Test
  public void testMaxSegmentsToMove1Thread()
  {
    Assertions.assertEquals(0, computeMaxSegmentsToMove(0, 1));
    Assertions.assertEquals(50, computeMaxSegmentsToMove(50, 1));
    Assertions.assertEquals(100, computeMaxSegmentsToMove(100, 1));

    Assertions.assertEquals(100, computeMaxSegmentsToMove(512, 1));
    Assertions.assertEquals(200, computeMaxSegmentsToMove(1_024, 1));
    Assertions.assertEquals(300, computeMaxSegmentsToMove(1_536, 1));

    Assertions.assertEquals(1_900, computeMaxSegmentsToMove(10_000, 1));
    Assertions.assertEquals(9_700, computeMaxSegmentsToMove(50_000, 1));
    Assertions.assertEquals(19_500, computeMaxSegmentsToMove(100_000, 1));

    Assertions.assertEquals(10_000, computeMaxSegmentsToMove(200_000, 1));
    Assertions.assertEquals(4_000, computeMaxSegmentsToMove(500_000, 1));
    Assertions.assertEquals(2_000, computeMaxSegmentsToMove(1_000_000, 1));
  }

  @Test
  public void testMaxSegmentsToMoveIncreasesWithCoordinatorPeriod()
  {
    Assertions.assertEquals(100, computeMaxSegmentsToMoveInPeriod(200_000, Duration.millis(0)));
    Assertions.assertEquals(100, computeMaxSegmentsToMoveInPeriod(200_000, Duration.millis(10_000)));
    Assertions.assertEquals(100, computeMaxSegmentsToMoveInPeriod(200_000, Duration.millis(20_000)));

    Assertions.assertEquals(5_000, computeMaxSegmentsToMoveInPeriod(200_000, Duration.millis(30_000)));
    Assertions.assertEquals(10_000, computeMaxSegmentsToMoveInPeriod(200_000, Duration.millis(60_000)));
    Assertions.assertEquals(15_000, computeMaxSegmentsToMoveInPeriod(200_000, Duration.millis(90_000)));
    Assertions.assertEquals(20_000, computeMaxSegmentsToMoveInPeriod(200_000, Duration.millis(120_000)));

    Assertions.assertEquals(2_000, computeMaxSegmentsToMoveInPeriod(500_000, Duration.millis(30_000)));
    Assertions.assertEquals(4_000, computeMaxSegmentsToMoveInPeriod(500_000, Duration.millis(60_000)));
    Assertions.assertEquals(6_000, computeMaxSegmentsToMoveInPeriod(500_000, Duration.millis(90_000)));
    Assertions.assertEquals(8_000, computeMaxSegmentsToMoveInPeriod(500_000, Duration.millis(120_000)));
  }

  @Test
  public void testMaxSegmentsToMove8Threads()
  {
    Assertions.assertEquals(0, computeMaxSegmentsToMove(0, 8));
    Assertions.assertEquals(50, computeMaxSegmentsToMove(50, 8));
    Assertions.assertEquals(100, computeMaxSegmentsToMove(100, 8));

    Assertions.assertEquals(100, computeMaxSegmentsToMove(512, 8));
    Assertions.assertEquals(200, computeMaxSegmentsToMove(1_024, 8));
    Assertions.assertEquals(300, computeMaxSegmentsToMove(1_536, 8));

    Assertions.assertEquals(33_000, computeMaxSegmentsToMove(500_000, 8));
    Assertions.assertEquals(16_000, computeMaxSegmentsToMove(1_000_000, 8));
    Assertions.assertEquals(8_000, computeMaxSegmentsToMove(2_000_000, 8));
    Assertions.assertEquals(3_000, computeMaxSegmentsToMove(5_000_000, 8));
    Assertions.assertEquals(1_000, computeMaxSegmentsToMove(10_000_000, 8));
  }

  @Test
  public void testMinSegmentsToMove()
  {
    Assertions.assertEquals(0, computeMinSegmentsToMove(0));
    Assertions.assertEquals(50, computeMinSegmentsToMove(50));

    Assertions.assertEquals(100, computeMinSegmentsToMove(100));
    Assertions.assertEquals(100, computeMinSegmentsToMove(1_000));

    Assertions.assertEquals(100, computeMinSegmentsToMove(20_000));
    Assertions.assertEquals(100, computeMinSegmentsToMove(50_000));
    Assertions.assertEquals(100, computeMinSegmentsToMove(100_000));
    Assertions.assertEquals(300, computeMinSegmentsToMove(200_000));
    Assertions.assertEquals(700, computeMinSegmentsToMove(500_000));
    Assertions.assertEquals(1_500, computeMinSegmentsToMove(1_000_000));
    Assertions.assertEquals(15_200, computeMinSegmentsToMove(10_000_000));
  }

  @Test
  public void testMinSegmentsToMoveIncreasesInSteps()
  {
    Assertions.assertEquals(100, computeMinSegmentsToMove(131_071));
    Assertions.assertEquals(200, computeMinSegmentsToMove(131_072));

    Assertions.assertEquals(500, computeMinSegmentsToMove(393_215));
    Assertions.assertEquals(600, computeMinSegmentsToMove(393_216));

    Assertions.assertEquals(900, computeMinSegmentsToMove(655_359));
    Assertions.assertEquals(1000, computeMinSegmentsToMove(655_360));

    Assertions.assertEquals(9_900, computeMinSegmentsToMove(6_553_599));
    Assertions.assertEquals(10_000, computeMinSegmentsToMove(6_553_600));
  }

  @Test
  public void testMinSegmentsArePickedForMoveWhenNoSkew()
  {
    final List<ServerHolder> historicals = Arrays.asList(
        createServer("A", WIKI_SEGMENTS),
        createServer("B", WIKI_SEGMENTS)
    );

    final int minSegmentsToMove = SegmentToMoveCalculator.computeMinSegmentsToMoveInTier(20_000);
    Assertions.assertEquals(100, minSegmentsToMove);

    final int segmentsToMoveToFixSkew = SegmentToMoveCalculator
        .computeNumSegmentsToMoveToBalanceTier(TIER, historicals);
    Assertions.assertEquals(0, segmentsToMoveToFixSkew);

    // Find segmentsToMove with no limit on maxSegmentsToMove
    final int segmentsToMove = SegmentToMoveCalculator
        .computeNumSegmentsToMoveInTier(TIER, historicals, Integer.MAX_VALUE);

    Assertions.assertEquals(minSegmentsToMove, segmentsToMove);
  }

  @Test
  public void testHalfSegmentsArePickedForMoveWhenFullSkew()
  {
    final List<ServerHolder> historicals = Arrays.asList(
        createServer("A", WIKI_SEGMENTS),
        createServer("B", Collections.emptyList())
    );

    final int minSegmentsToMove = SegmentToMoveCalculator.computeMinSegmentsToMoveInTier(10_000);
    Assertions.assertEquals(100, minSegmentsToMove);

    final int segmentsToMoveToFixSkew = SegmentToMoveCalculator
        .computeNumSegmentsToMoveToBalanceTier(TIER, historicals);
    Assertions.assertEquals(5_000, segmentsToMoveToFixSkew);

    // Find segmentsToMove with no limit on maxSegmentsToMove
    final int segmentsToMove = SegmentToMoveCalculator
        .computeNumSegmentsToMoveInTier(TIER, historicals, Integer.MAX_VALUE);

    Assertions.assertEquals(segmentsToMoveToFixSkew, segmentsToMove);
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
    Assertions.assertEquals(WIKI_SEGMENTS.size() / 2, numToMoveToBalanceCount);
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
        = new DruidServer(name, name, null, 10L << 30, null, ServerType.HISTORICAL, "tier1", 1);
    segments.forEach(server::addDataSegment);
    return new ServerHolder(server.toImmutableDruidServer(), new TestLoadQueuePeon());
  }
}
