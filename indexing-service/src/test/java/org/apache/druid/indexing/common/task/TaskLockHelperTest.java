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

package org.apache.druid.indexing.common.task;

import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.NumberedOverwriteShardSpec;
import org.apache.druid.timeline.partition.PartitionIds;
import org.joda.time.Interval;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class TaskLockHelperTest
{
  private static final String DATA_SOURCE = "test_datasource";
  private static final Interval TEST_INTERVAL = Intervals.of("2017-01-01/2017-01-02");
  private static final String TEST_VERSION = DateTimes.nowUtc().toString();

  @Test(expected = ISE.class)
  public void testVerifyNonConsecutiveSegmentsInInputFails()
  {
    // Test that non-consecutive segments within the input list fail.
    // Compacting segments {0, 1, 3} should fail because root partition 2 is missing.
    final List<DataSegment> segments = ImmutableList.of(
        createSegment(0, 0, 1, (short) 1, (short) 1),  // rootPartitionRange [0, 1)
        createSegment(1, 1, 2, (short) 1, (short) 1),   // rootPartitionRange [1, 2)
        createSegment(3, 3, 4, (short) 1, (short) 1)    // rootPartitionRange [3, 4)
    );

    TaskLockHelper.verifyRootPartitionIsAdjacentAndAtomicUpdateGroupIsFull(segments);
  }

  @Test
  public void testVerifyConsecutiveSegmentsSucceedEvenIfOtherSegmentsMissing()
  {
    final List<DataSegment> segments = ImmutableList.of(
        createSegment(3, 3, 4, (short) 1, (short) 1),  // rootPartitionRange [3, 4)
        createSegment(4, 4, 5, (short) 1, (short) 1),   // rootPartitionRange [4, 5)
        createSegment(5, 5, 6, (short) 1, (short) 1)   // rootPartitionRange [5, 6)
    );

    TaskLockHelper.verifyRootPartitionIsAdjacentAndAtomicUpdateGroupIsFull(segments);
  }

  @Test
  public void testVerifyConsecutiveSegmentsStillWorks()
  {
    final List<DataSegment> segments = ImmutableList.of(
        createSegment(0, 0, 1, (short) 1, (short) 1),
        createSegment(1, 1, 2, (short) 1, (short) 1),
        createSegment(2, 2, 3, (short) 1, (short) 1)
    );

    TaskLockHelper.verifyRootPartitionIsAdjacentAndAtomicUpdateGroupIsFull(segments);
  }

  @Test(expected = ISE.class)
  public void testVerifyLargeGapSegmentsFails()
  {
    final List<DataSegment> segments = ImmutableList.of(
        createSegment(0, 0, 1, (short) 1, (short) 1),
        createSegment(1, 1, 2, (short) 1, (short) 1),
        createSegment(10, 10, 11, (short) 1, (short) 1)
    );

    TaskLockHelper.verifyRootPartitionIsAdjacentAndAtomicUpdateGroupIsFull(segments);
  }

  @Test
  public void testVerifyAtomicUpdateGroupValidationStillWorks()
  {
    final List<DataSegment> segments = ImmutableList.of(
        createSegment(0, 0, 1, (short) 1, (short) 2),
        createSegment(1, 0, 1, (short) 1, (short) 2)
    );

    TaskLockHelper.verifyRootPartitionIsAdjacentAndAtomicUpdateGroupIsFull(segments);
  }

  @Test(expected = ISE.class)
  public void testVerifyAtomicUpdateGroupIncompleteFails()
  {
    final List<DataSegment> segments = ImmutableList.of(
        createSegment(0, 0, 1, (short) 1, (short) 3),
        createSegment(1, 0, 1, (short) 1, (short) 3)
    );

    // Should throw ISE because atomicUpdateGroupSize is 3 but we only have 2 segments
    TaskLockHelper.verifyRootPartitionIsAdjacentAndAtomicUpdateGroupIsFull(segments);
  }

  @Test(expected = ISE.class)
  public void testVerifyDifferentMinorVersionsFail()
  {
    // Test that segments with same root partition range but different minor versions fail
    final List<DataSegment> segments = ImmutableList.of(
        createSegment(0, 0, 1, (short) 1, (short) 2),
        createSegment(1, 0, 1, (short) 2, (short) 2) // Different minor version
    );

    TaskLockHelper.verifyRootPartitionIsAdjacentAndAtomicUpdateGroupIsFull(segments);
  }

  @Test(expected = ISE.class)
  public void testVerifyDifferentAtomicUpdateGroupSizesFail()
  {
    // Test that segments with same root partition range but different atomicUpdateGroupSize fail
    final List<DataSegment> segments = ImmutableList.of(
        createSegment(0, 0, 1, (short) 1, (short) 2),
        createSegment(1, 0, 1, (short) 1, (short) 3)
    );

    TaskLockHelper.verifyRootPartitionIsAdjacentAndAtomicUpdateGroupIsFull(segments);
  }

  @Test
  public void testVerifyEmptySegmentsList()
  {
    final List<DataSegment> segments = Collections.emptyList();
    TaskLockHelper.verifyRootPartitionIsAdjacentAndAtomicUpdateGroupIsFull(segments);
  }

  @Test
  public void testVerifySingleSegment()
  {
    final List<DataSegment> segments = ImmutableList.of(
        createSegment(0, 0, 1, (short) 1, (short) 1)
    );
    TaskLockHelper.verifyRootPartitionIsAdjacentAndAtomicUpdateGroupIsFull(segments);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testVerifyDifferentIntervalsFail()
  {
    final Interval interval1 = Intervals.of("2017-01-01/2017-01-02");
    final Interval interval2 = Intervals.of("2017-01-02/2017-01-03");
    final List<DataSegment> segments = ImmutableList.of(
        DataSegment.builder(SegmentId.of(DATA_SOURCE, interval1, TEST_VERSION, 0))
            .shardSpec(new NumberedOverwriteShardSpec(
                PartitionIds.NON_ROOT_GEN_START_PARTITION_ID,
                0,
                1,
                (short) 1,
                (short) 1
            ))
            .size(0)
            .build(),
        DataSegment.builder(SegmentId.of(DATA_SOURCE, interval2, TEST_VERSION, 0))
            .shardSpec(new NumberedOverwriteShardSpec(
                PartitionIds.NON_ROOT_GEN_START_PARTITION_ID + 1,
                1,
                2,
                (short) 1,
                (short) 1
            ))
            .size(0)
            .build()
    );

    TaskLockHelper.verifyRootPartitionIsAdjacentAndAtomicUpdateGroupIsFull(segments);
  }

  /**
   * Helper method to create a test segment with NumberedOverwriteShardSpec
   */
  private DataSegment createSegment(
      int partitionId,
      int startRootPartitionId,
      int endRootPartitionId,
      short minorVersion,
      short atomicUpdateGroupSize
  )
  {
    return DataSegment.builder(SegmentId.of(DATA_SOURCE, TEST_INTERVAL, TEST_VERSION, partitionId))
        .shardSpec(new NumberedOverwriteShardSpec(
            PartitionIds.NON_ROOT_GEN_START_PARTITION_ID + partitionId,
            startRootPartitionId,
            endRootPartitionId,
            minorVersion,
            atomicUpdateGroupSize
        ))
        .size(0)
        .build();
  }
}
