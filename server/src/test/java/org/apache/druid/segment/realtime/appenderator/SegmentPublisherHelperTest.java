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

package org.apache.druid.segment.realtime.appenderator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.BuildingHashBasedNumberedShardSpec;
import org.apache.druid.timeline.partition.BuildingNumberedShardSpec;
import org.apache.druid.timeline.partition.BuildingSingleDimensionShardSpec;
import org.apache.druid.timeline.partition.HashBasedNumberedShardSpec;
import org.apache.druid.timeline.partition.HashBucketShardSpec;
import org.apache.druid.timeline.partition.NumberedOverwriteShardSpec;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.timeline.partition.PartitionIds;
import org.apache.druid.timeline.partition.ShardSpec;
import org.apache.druid.timeline.partition.SingleDimensionShardSpec;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Set;

public class SegmentPublisherHelperTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testAnnotateAtomicUpdateGroupSize()
  {
    final Set<DataSegment> segments = ImmutableSet.of(
        newSegment(
            new NumberedOverwriteShardSpec(
                PartitionIds.NON_ROOT_GEN_START_PARTITION_ID,
                0,
                3,
                (short) 1
            )
        ),
        newSegment(
            new NumberedOverwriteShardSpec(
                PartitionIds.NON_ROOT_GEN_START_PARTITION_ID + 1,
                0,
                3,
                (short) 1
            )
        ),
        newSegment(
            new NumberedOverwriteShardSpec(
                PartitionIds.NON_ROOT_GEN_START_PARTITION_ID + 2,
                0,
                3,
                (short) 1
            )
        )
    );
    final Set<DataSegment> annotated = SegmentPublisherHelper.annotateShardSpec(segments);
    for (DataSegment segment : annotated) {
      Assert.assertSame(NumberedOverwriteShardSpec.class, segment.getShardSpec().getClass());
      final NumberedOverwriteShardSpec shardSpec = (NumberedOverwriteShardSpec) segment.getShardSpec();
      Assert.assertEquals(3, shardSpec.getAtomicUpdateGroupSize());
    }
  }

  @Test
  public void testAnnotateCorePartitionSetSizeForNumberedShardSpec()
  {
    final Set<DataSegment> segments = ImmutableSet.of(
        newSegment(new BuildingNumberedShardSpec(0)),
        newSegment(new BuildingNumberedShardSpec(1)),
        newSegment(new BuildingNumberedShardSpec(2))
    );
    final Set<DataSegment> annotated = SegmentPublisherHelper.annotateShardSpec(segments);
    for (DataSegment segment : annotated) {
      Assert.assertSame(NumberedShardSpec.class, segment.getShardSpec().getClass());
      final NumberedShardSpec shardSpec = (NumberedShardSpec) segment.getShardSpec();
      Assert.assertEquals(3, shardSpec.getNumCorePartitions());
    }
  }

  @Test
  public void testAnnotateCorePartitionSetSizeForHashNumberedShardSpec()
  {
    final Set<DataSegment> segments = ImmutableSet.of(
        newSegment(new BuildingHashBasedNumberedShardSpec(0, 0, 3, null, new ObjectMapper())),
        newSegment(new BuildingHashBasedNumberedShardSpec(1, 1, 3, null, new ObjectMapper())),
        newSegment(new BuildingHashBasedNumberedShardSpec(2, 2, 3, null, new ObjectMapper()))
    );
    final Set<DataSegment> annotated = SegmentPublisherHelper.annotateShardSpec(segments);
    for (DataSegment segment : annotated) {
      Assert.assertSame(HashBasedNumberedShardSpec.class, segment.getShardSpec().getClass());
      final HashBasedNumberedShardSpec shardSpec = (HashBasedNumberedShardSpec) segment.getShardSpec();
      Assert.assertEquals(3, shardSpec.getNumCorePartitions());
    }
  }

  @Test
  public void testAnnotateCorePartitionSetSizeForSingleDimensionShardSpec()
  {
    final Set<DataSegment> segments = ImmutableSet.of(
        newSegment(new BuildingSingleDimensionShardSpec(0, "dim", null, "ccc", 0)),
        newSegment(new BuildingSingleDimensionShardSpec(1, "dim", null, "ccc", 1)),
        newSegment(new BuildingSingleDimensionShardSpec(2, "dim", null, "ccc", 2))
    );
    final Set<DataSegment> annotated = SegmentPublisherHelper.annotateShardSpec(segments);
    for (DataSegment segment : annotated) {
      Assert.assertSame(SingleDimensionShardSpec.class, segment.getShardSpec().getClass());
      final SingleDimensionShardSpec shardSpec = (SingleDimensionShardSpec) segment.getShardSpec();
      Assert.assertEquals(3, shardSpec.getNumCorePartitions());
    }
  }

  @Test
  public void testAnnotateShardSpecDoNothing()
  {
    final Set<DataSegment> segments = ImmutableSet.of(
        newSegment(new NumberedShardSpec(0, 0)),
        newSegment(new NumberedShardSpec(1, 0)),
        newSegment(new NumberedShardSpec(2, 0))
    );
    final Set<DataSegment> annotated = SegmentPublisherHelper.annotateShardSpec(segments);
    Assert.assertEquals(segments, annotated);
  }

  @Test
  public void testAnnotateShardSpecThrowingExceptionForBucketNumberedShardSpec()
  {
    final Set<DataSegment> segments = ImmutableSet.of(
        newSegment(new HashBucketShardSpec(0, 3, null, new ObjectMapper())),
        newSegment(new HashBucketShardSpec(1, 3, null, new ObjectMapper())),
        newSegment(new HashBucketShardSpec(2, 3, null, new ObjectMapper()))
    );
    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage("Cannot publish segments with shardSpec");
    SegmentPublisherHelper.annotateShardSpec(segments);
  }

  private static DataSegment newSegment(ShardSpec shardSpec)
  {
    return new DataSegment(
        "datasource",
        Intervals.of("2020-01-01/P1d"),
        "version",
        null,
        ImmutableList.of("dim"),
        ImmutableList.of("met"),
        shardSpec,
        9,
        10L
    );
  }
}
