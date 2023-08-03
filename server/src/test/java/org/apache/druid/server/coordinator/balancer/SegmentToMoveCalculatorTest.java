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
import org.apache.druid.server.coordinator.loading.SegmentLoadingConfig;
import org.apache.druid.server.coordinator.loading.TestLoadQueuePeon;
import org.apache.druid.timeline.DataSegment;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class SegmentToMoveCalculatorTest
{

  /**
   * 100 days x 100 partitions = 10,000 segments.
   */
  private static final List<DataSegment> WIKI_SEGMENTS
      = CreateDataSegments.ofDatasource("wiki")
                          .forIntervals(100, Granularities.DAY)
                          .withNumPartitions(100)
                          .eachOfSizeInMb(500);
  private static final String TIER = "tier1";

  @Test
  public void testComputeMaxSegmentsToMove()
  {
    Assert.assertEquals(100, computeMaxSegmentsToMove(0));
    Assert.assertEquals(100, computeMaxSegmentsToMove(1_000));
    Assert.assertEquals(1_900, computeMaxSegmentsToMove(10_000));

    Assert.assertEquals(19_500, computeMaxSegmentsToMove(100_000));
    Assert.assertEquals(19_700, computeMaxSegmentsToMove(101_000));

    Assert.assertEquals(29_200, computeMaxSegmentsToMove(150_000));
    Assert.assertEquals(39_000, computeMaxSegmentsToMove(200_000));
    Assert.assertEquals(40_000, computeMaxSegmentsToMove(300_000));
    Assert.assertEquals(48_000, computeMaxSegmentsToMove(400_000));

    Assert.assertEquals(56_000, computeMaxSegmentsToMove(500_000));
    Assert.assertEquals(56_000, computeMaxSegmentsToMove(520_000));

    Assert.assertEquals(32_000, computeMaxSegmentsToMove(600_000));
    Assert.assertEquals(32_000, computeMaxSegmentsToMove(750_000));
    Assert.assertEquals(32_000, computeMaxSegmentsToMove(1_000_000));

    Assert.assertEquals(16_000, computeMaxSegmentsToMove(1_500_000));
    Assert.assertEquals(16_000, computeMaxSegmentsToMove(2_000_000));

    Assert.assertEquals(8_000, computeMaxSegmentsToMove(3_000_000));
    Assert.assertEquals(4_000, computeMaxSegmentsToMove(5_000_000));
    Assert.assertEquals(2_000, computeMaxSegmentsToMove(10_000_000));
    Assert.assertEquals(1_000, computeMaxSegmentsToMove(20_000_000));
  }

  @Test
  public void testComputeMinSegmentsToMove()
  {
    Assert.assertEquals(100, computeMinSegmentsToMove(0));
    Assert.assertEquals(100, computeMinSegmentsToMove(20_000));
    Assert.assertEquals(300, computeMinSegmentsToMove(50_000));
    Assert.assertEquals(600, computeMinSegmentsToMove(100_000));
    Assert.assertEquals(1_200, computeMinSegmentsToMove(200_000));
    Assert.assertEquals(3_000, computeMinSegmentsToMove(500_000));
    Assert.assertEquals(6_100, computeMinSegmentsToMove(1_000_000));
    Assert.assertEquals(61_000, computeMinSegmentsToMove(10_000_000));
  }

  @Test
  public void testMinSegmentsToMoveIncreasesInSteps()
  {
    Assert.assertEquals(100, computeMinSegmentsToMove(32_767));
    Assert.assertEquals(200, computeMinSegmentsToMove(32_768));

    Assert.assertEquals(500, computeMinSegmentsToMove(98_303));
    Assert.assertEquals(600, computeMinSegmentsToMove(98_304));

    Assert.assertEquals(900, computeMinSegmentsToMove(163_839));
    Assert.assertEquals(1000, computeMinSegmentsToMove(163_840));

    Assert.assertEquals(9_900, computeMinSegmentsToMove(1_638_399));
    Assert.assertEquals(10_000, computeMinSegmentsToMove(1_638_400));
  }

  @Test
  public void testMinSegmentsAreMovedWhenNoSkew()
  {
    final List<ServerHolder> historicals = Arrays.asList(
        createServer("A", WIKI_SEGMENTS),
        createServer("A", WIKI_SEGMENTS)
    );

    final int minSegmentsToMove = SegmentToMoveCalculator.computeMinSegmentsToMoveInTier(20_000);
    Assert.assertEquals(100, minSegmentsToMove);

    final int segmentsToMoveToFixSkew = SegmentToMoveCalculator
        .computeNumSegmentsToMoveInTierToFixSkew(TIER, historicals);
    Assert.assertEquals(0, segmentsToMoveToFixSkew);

    // Find segmentsToMove with no limit on maxSegmentsToMove
    final int segmentsToMove = SegmentToMoveCalculator
        .computeNumSegmentsToMoveInTier(TIER, historicals, Integer.MAX_VALUE);

    Assert.assertEquals(minSegmentsToMove, segmentsToMove);
  }

  @Test
  public void testHalfSegmentsAreMovedWhenFullSkew()
  {
    final List<ServerHolder> historicals = Arrays.asList(
        createServer("A", WIKI_SEGMENTS),
        createServer("A", Collections.emptyList())
    );

    final int minSegmentsToMove = SegmentToMoveCalculator.computeMinSegmentsToMoveInTier(10_000);
    Assert.assertEquals(100, minSegmentsToMove);

    final int segmentsToMoveToFixSkew = SegmentToMoveCalculator
        .computeNumSegmentsToMoveInTierToFixSkew(TIER, historicals);
    Assert.assertEquals(5_000, segmentsToMoveToFixSkew);

    // Find segmentsToMove with no limit on maxSegmentsToMove
    final int segmentsToMove = SegmentToMoveCalculator
        .computeNumSegmentsToMoveInTier(TIER, historicals, Integer.MAX_VALUE);

    Assert.assertEquals(segmentsToMoveToFixSkew, segmentsToMove);
  }

  private static int computeMaxSegmentsToMove(int totalSegments)
  {
    return SegmentToMoveCalculator.computeMaxSegmentsToMovePerTier(
        totalSegments,
        SegmentLoadingConfig.computeNumBalancerThreads(totalSegments)
    );
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
