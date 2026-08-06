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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.data.input.StringTuple;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.BuildingDimensionRangeShardSpec;
import org.apache.druid.timeline.partition.BuildingHashBasedNumberedShardSpec;
import org.apache.druid.timeline.partition.BuildingNumberedShardSpec;
import org.apache.druid.timeline.partition.BuildingSingleDimensionShardSpec;
import org.apache.druid.timeline.partition.DimensionRangeShardSpec;
import org.apache.druid.timeline.partition.DimensionValueSetShardSpec;
import org.apache.druid.timeline.partition.HashBasedNumberedShardSpec;
import org.apache.druid.timeline.partition.HashBucketShardSpec;
import org.apache.druid.timeline.partition.HashPartitionFunction;
import org.apache.druid.timeline.partition.NumberedOverwriteShardSpec;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.timeline.partition.PartitionIds;
import org.apache.druid.timeline.partition.ShardSpec;
import org.apache.druid.timeline.partition.SingleDimensionShardSpec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class SegmentPublisherHelperTest
{

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
      Assertions.assertSame(NumberedOverwriteShardSpec.class, segment.getShardSpec().getClass());
      final NumberedOverwriteShardSpec shardSpec = (NumberedOverwriteShardSpec) segment.getShardSpec();
      Assertions.assertEquals(3, shardSpec.getAtomicUpdateGroupSize());
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
      Assertions.assertSame(NumberedShardSpec.class, segment.getShardSpec().getClass());
      final NumberedShardSpec shardSpec = (NumberedShardSpec) segment.getShardSpec();
      Assertions.assertEquals(3, shardSpec.getNumCorePartitions());
    }
  }

  @Test
  public void testAnnotateCorePartitionSetSizeForHashNumberedShardSpec()
  {
    final Set<DataSegment> segments = ImmutableSet.of(
        newSegment(
            new BuildingHashBasedNumberedShardSpec(
                0,
                0,
                3,
                null,
                HashPartitionFunction.MURMUR3_32_ABS,
                new ObjectMapper()
            )
        ),
        newSegment(
            new BuildingHashBasedNumberedShardSpec(
                1,
                1,
                3,
                null,
                HashPartitionFunction.MURMUR3_32_ABS,
                new ObjectMapper()
            )
        ),
        newSegment(
            new BuildingHashBasedNumberedShardSpec(
                2,
                2,
                3,
                null,
                HashPartitionFunction.MURMUR3_32_ABS,
                new ObjectMapper()
            )
        )
    );
    final Set<DataSegment> annotated = SegmentPublisherHelper.annotateShardSpec(segments);
    for (DataSegment segment : annotated) {
      Assertions.assertSame(HashBasedNumberedShardSpec.class, segment.getShardSpec().getClass());
      final HashBasedNumberedShardSpec shardSpec = (HashBasedNumberedShardSpec) segment.getShardSpec();
      Assertions.assertEquals(3, shardSpec.getNumCorePartitions());
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
      Assertions.assertSame(SingleDimensionShardSpec.class, segment.getShardSpec().getClass());
      final SingleDimensionShardSpec shardSpec = (SingleDimensionShardSpec) segment.getShardSpec();
      Assertions.assertEquals(3, shardSpec.getNumCorePartitions());
    }
  }

  @Test
  public void testAnnotateCorePartitionSetSizeForDimensionRangeShardSpec()
  {
    final Set<DataSegment> segments = ImmutableSet.of(
        newSegment(new BuildingDimensionRangeShardSpec(
            0,
            Arrays.asList("dim1", "dim2"),
            null,
            StringTuple.create("a", "5"),
            0
        )),
        newSegment(new BuildingDimensionRangeShardSpec(
            1,
            Arrays.asList("dim1", "dim2"),
            null,
            StringTuple.create("a", "5"),
            1
        )),
        newSegment(new BuildingDimensionRangeShardSpec(
            2,
            Arrays.asList("dim1", "dim2"),
            null,
            StringTuple.create("a", "5"),
            2
        ))
    );
    final Set<DataSegment> annotated = SegmentPublisherHelper.annotateShardSpec(segments);
    for (DataSegment segment : annotated) {
      Assertions.assertSame(DimensionRangeShardSpec.class, segment.getShardSpec().getClass());
      final DimensionRangeShardSpec shardSpec = (DimensionRangeShardSpec) segment.getShardSpec();
      Assertions.assertEquals(3, shardSpec.getNumCorePartitions());
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
    Assertions.assertEquals(segments, annotated);
  }

  @Test
  public void testAnnotateShardSpecAllowsMixedFilterDimensionValueSetShardSpecsInSameInterval()
  {
    // Empty-filter and populated DimensionValueSetShardSpecs in one interval must not trip the mismatched shardSpecs
    // check, so a restart batch publishes without blocking.
    final Set<DataSegment> segments = ImmutableSet.of(
        newSegment(new DimensionValueSetShardSpec(0, 0, Collections.emptyMap())),
        newSegment(new DimensionValueSetShardSpec(1, 0, ImmutableMap.of("tenant", ImmutableList.of("tenant_a"))))
    );
    final Set<DataSegment> annotated = SegmentPublisherHelper.annotateShardSpec(segments);
    Assertions.assertEquals(segments, annotated);
    for (DataSegment segment : annotated) {
      Assertions.assertSame(DimensionValueSetShardSpec.class, segment.getShardSpec().getClass());
    }
  }

  @Test
  public void testAnnotateShardSpecThrowingExceptionForBucketNumberedShardSpec()
  {
    Throwable exception = org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class, () -> {
      final Set<DataSegment> segments = ImmutableSet.of(
          newSegment(new HashBucketShardSpec(0, 3, null, HashPartitionFunction.MURMUR3_32_ABS, new ObjectMapper())),
          newSegment(new HashBucketShardSpec(1, 3, null, HashPartitionFunction.MURMUR3_32_ABS, new ObjectMapper())),
          newSegment(new HashBucketShardSpec(2, 3, null, HashPartitionFunction.MURMUR3_32_ABS, new ObjectMapper()))
      );
      SegmentPublisherHelper.annotateShardSpec(segments);
    });
    assertTrue(exception.getMessage().contains("Cannot publish segments with shardSpec"));
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
