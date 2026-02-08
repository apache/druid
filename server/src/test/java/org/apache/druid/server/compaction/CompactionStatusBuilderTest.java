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

package org.apache.druid.server.compaction;

import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.timeline.DataSegment;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class CompactionStatusBuilderTest
{
  private static final String DATASOURCE = "test_datasource";

  @Test
  public void testNotEligible()
  {
    CompactionStatus eligibility = CompactionStatus.notEligible("test reason: %s", "failure");

    Assert.assertEquals(CompactionStatus.State.NOT_ELIGIBLE, eligibility.getState());
    Assert.assertEquals("test reason: failure", eligibility.getReason());
    Assert.assertNull(eligibility.getCompactedStats());
    Assert.assertNull(eligibility.getUncompactedStats());
    Assert.assertNull(eligibility.getUncompactedSegments());
  }

  @Test
  public void testBuilderWithCompactionStats()
  {
    CompactionStatistics compactedStats = CompactionStatistics.create(1000, 5, 2);
    CompactionStatistics uncompactedStats = CompactionStatistics.create(500, 3, 1);
    List<DataSegment> uncompactedSegments = createTestSegments(3);

    CompactionStatus eligibility =
        CompactionStatus.builder(CompactionStatus.State.ELIGIBLE, "needs full compaction")
                             .compacted(compactedStats)
                             .uncompacted(uncompactedStats)
                             .uncompactedSegments(uncompactedSegments)
                             .build();

    Assert.assertEquals(CompactionStatus.State.ELIGIBLE, eligibility.getState());
    Assert.assertEquals("needs full compaction", eligibility.getReason());
    Assert.assertEquals(compactedStats, eligibility.getCompactedStats());
    Assert.assertEquals(uncompactedStats, eligibility.getUncompactedStats());
    Assert.assertEquals(uncompactedSegments, eligibility.getUncompactedSegments());
  }

  @Test
  public void testEqualsAndHashCode()
  {
    // Test with simple eligibility objects (same state and reason)
    CompactionStatus simple1 = CompactionStatus.notEligible("reason");
    CompactionStatus simple2 = CompactionStatus.notEligible("reason");
    Assert.assertEquals(simple1, simple2);
    Assert.assertEquals(simple1.hashCode(), simple2.hashCode());

    // Test with different reasons
    CompactionStatus differentReason = CompactionStatus.notEligible("different");
    Assert.assertNotEquals(simple1, differentReason);

    // Test with different states
    CompactionStatus differentState = CompactionStatus.COMPLETE;
    Assert.assertNotEquals(simple1, differentState);

    // Test with full compaction eligibility (with stats and segments)
    CompactionStatistics stats1 = CompactionStatistics.create(1000, 5, 2);
    CompactionStatistics stats2 = CompactionStatistics.create(500, 3, 1);
    List<DataSegment> segments = createTestSegments(3);

    CompactionStatus withStats1 =
        CompactionStatus.builder(CompactionStatus.State.ELIGIBLE, "reason")
                             .compacted(stats1)
                             .uncompacted(stats2)
                             .uncompactedSegments(segments)
                             .build();

    CompactionStatus withStats2 =
        CompactionStatus.builder(CompactionStatus.State.ELIGIBLE, "reason")
                             .compacted(stats1)
                             .uncompacted(stats2)
                             .uncompactedSegments(segments)
                             .build();

    // Same values - should be equal
    Assert.assertEquals(withStats1, withStats2);
    Assert.assertEquals(withStats1.hashCode(), withStats2.hashCode());

    // Test with different compacted stats
    CompactionStatistics differentStats = CompactionStatistics.create(2000, 10, 5);
    CompactionStatus differentCompactedStats =
        CompactionStatus.builder(CompactionStatus.State.ELIGIBLE, "reason")
                             .compacted(differentStats)
                             .uncompacted(stats2)
                             .uncompactedSegments(segments)
                             .build();
    Assert.assertNotEquals(withStats1, differentCompactedStats);

    // Test with different uncompacted stats
    CompactionStatus differentUncompactedStats =
        CompactionStatus.builder(CompactionStatus.State.ELIGIBLE, "reason")
                             .compacted(stats1)
                             .uncompacted(differentStats)
                             .uncompactedSegments(segments)
                             .build();
    Assert.assertNotEquals(withStats1, differentUncompactedStats);

    // Test with different segment lists
    List<DataSegment> differentSegments = createTestSegments(5);
    CompactionStatus differentSegmentList =
        CompactionStatus.builder(CompactionStatus.State.ELIGIBLE, "reason")
                             .compacted(stats1)
                             .uncompacted(stats2)
                             .uncompactedSegments(differentSegments)
                             .build();
    Assert.assertNotEquals(withStats1, differentSegmentList);
  }

  @Test
  public void testBuilderRequiresReasonForNotEligible()
  {
    Assert.assertThrows(
        DruidException.class,
        () -> CompactionStatus.builder(CompactionStatus.State.NOT_ELIGIBLE, null).build()
    );
  }

  @Test
  public void testBuilderRequiresStatsForFullCompaction()
  {
    Assert.assertThrows(
        DruidException.class,
        () -> CompactionStatus.builder(CompactionStatus.State.ELIGIBLE, "reason").build()
    );

    Assert.assertThrows(
        DruidException.class,
        () -> CompactionStatus.builder(CompactionStatus.State.ELIGIBLE, "reason")
                                   .compacted(CompactionStatistics.create(1000, 5, 2))
                                   .build()
    );

    Assert.assertThrows(
        DruidException.class,
        () -> CompactionStatus.builder(CompactionStatus.State.ELIGIBLE, "reason")
                                   .compacted(CompactionStatistics.create(1000, 5, 2))
                                   .uncompacted(CompactionStatistics.create(500, 3, 1))
                                   .build()
    );
  }

  private static List<DataSegment> createTestSegments(int count)
  {
    if (count == 0) {
      return Collections.emptyList();
    }

    return CreateDataSegments.ofDatasource(DATASOURCE)
                             .forIntervals(count, Granularities.DAY)
                             .startingAt("2024-01-01")
                             .eachOfSizeInMb(100);
  }
}
