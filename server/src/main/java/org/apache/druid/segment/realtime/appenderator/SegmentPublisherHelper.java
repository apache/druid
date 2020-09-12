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

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.BucketNumberedShardSpec;
import org.apache.druid.timeline.partition.BuildingShardSpec;
import org.apache.druid.timeline.partition.OverwriteShardSpec;
import org.apache.druid.timeline.partition.ShardSpec;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class SegmentPublisherHelper
{
  /**
   * This method fills missing information in the shard spec if necessary when publishing segments.
   *
   * - When time chunk lock is used, the non-appending task should set the proper size of the core partitions for
   *   dynamically-partitioned segments. See {@link #annotateCorePartitionSetSizeFn}.
   * - When segment lock is used, the overwriting task should set the proper size of the atomic update group.
   *   See {@link #annotateAtomicUpdateGroupFn}.
   */
  static Set<DataSegment> annotateShardSpec(Set<DataSegment> segments)
  {
    final Map<Interval, List<DataSegment>> intervalToSegments = new HashMap<>();
    segments.forEach(
        segment -> intervalToSegments.computeIfAbsent(segment.getInterval(), k -> new ArrayList<>()).add(segment)
    );

    for (Entry<Interval, List<DataSegment>> entry : intervalToSegments.entrySet()) {
      final Interval interval = entry.getKey();
      final List<DataSegment> segmentsPerInterval = entry.getValue();
      final ShardSpec firstShardSpec = segmentsPerInterval.get(0).getShardSpec();
      final boolean anyMismatch = segmentsPerInterval.stream().anyMatch(
          segment -> segment.getShardSpec().getClass() != firstShardSpec.getClass()
      );
      if (anyMismatch) {
        throw new ISE(
            "Mismatched shardSpecs in interval[%s] for segments[%s]",
            interval,
            segmentsPerInterval
        );
      }
      final Function<DataSegment, DataSegment> annotateFn;
      if (firstShardSpec instanceof OverwriteShardSpec) {
        annotateFn = annotateAtomicUpdateGroupFn(segmentsPerInterval.size());
      } else if (firstShardSpec instanceof BuildingShardSpec) {
        annotateFn = annotateCorePartitionSetSizeFn(segmentsPerInterval.size());
      } else if (firstShardSpec instanceof BucketNumberedShardSpec) {
        throw new ISE("Cannot publish segments with shardSpec[%s]", firstShardSpec);
      } else {
        annotateFn = null;
      }

      if (annotateFn != null) {
        intervalToSegments.put(interval, segmentsPerInterval.stream().map(annotateFn).collect(Collectors.toList()));
      }
    }

    return intervalToSegments.values().stream().flatMap(Collection::stream).collect(Collectors.toSet());
  }

  private static Function<DataSegment, DataSegment> annotateAtomicUpdateGroupFn(int atomicUpdateGroupSize)
  {
    // The segments which are published together consist an atomicUpdateGroup.
    return segment -> {
      final OverwriteShardSpec shardSpec = (OverwriteShardSpec) segment.getShardSpec();
      return segment.withShardSpec(shardSpec.withAtomicUpdateGroupSize((short) atomicUpdateGroupSize));
    };
  }

  private static Function<DataSegment, DataSegment> annotateCorePartitionSetSizeFn(int corePartitionSetSize)
  {
    return segment -> {
      final BuildingShardSpec<?> shardSpec = (BuildingShardSpec<?>) segment.getShardSpec();
      return segment.withShardSpec(shardSpec.convert(corePartitionSetSize));
    };
  }

  private SegmentPublisherHelper()
  {
  }
}
