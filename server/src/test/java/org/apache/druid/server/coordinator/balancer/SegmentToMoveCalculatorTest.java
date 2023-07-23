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

import org.apache.druid.server.coordinator.loading.SegmentLoadingConfig;
import org.junit.Assert;
import org.junit.Test;

public class SegmentToMoveCalculatorTest
{

  @Test
  public void testComputeMaxSegmentsToMove()
  {
    Assert.assertEquals(100, computeMaxSegmentsToMove(0));
    Assert.assertEquals(125, computeMaxSegmentsToMove(1_000));
    Assert.assertEquals(1_250, computeMaxSegmentsToMove(10_000));

    Assert.assertEquals(12_500, computeMaxSegmentsToMove(100_000));
    Assert.assertEquals(12_625, computeMaxSegmentsToMove(101_000));

    Assert.assertEquals(18_750, computeMaxSegmentsToMove(150_000));
    Assert.assertEquals(25_000, computeMaxSegmentsToMove(200_000));
    Assert.assertEquals(37_500, computeMaxSegmentsToMove(300_000));
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

  private int computeMaxSegmentsToMove(int totalSegments)
  {
    return SegmentToMoveCalculator.computeMaxSegmentsToMove(
        totalSegments,
        SegmentLoadingConfig.computeNumBalancerThreads(totalSegments)
    );
  }
}
