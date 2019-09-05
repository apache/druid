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

import org.apache.druid.indexing.overlord.SegmentPublishResult;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.OverwriteShardSpec;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

public interface TransactionalSegmentPublisher
{
  /**
   * Publish segments, along with some commit metadata, in a single transaction.
   *
   * @return publish result that indicates if segments were published or not. If it is unclear
   * if the segments were published or not, this method must throw an exception. The behavior is similar to
   * IndexerSQLMetadataStorageCoordinator's announceHistoricalSegments.
   *
   * @throws IOException if there was an I/O error when publishing
   * @throws RuntimeException if we cannot tell if the segments were published or not, for some other reason
   */
  SegmentPublishResult publishAnnotatedSegments(
      @Nullable Set<DataSegment> segmentsToBeOverwritten,
      Set<DataSegment> segmentsToPublish,
      @Nullable Object commitMetadata
  ) throws IOException;

  default SegmentPublishResult publishSegments(
      @Nullable Set<DataSegment> segmentsToBeOverwritten,
      Set<DataSegment> segmentsToPublish,
      @Nullable Object commitMetadata
  )
      throws IOException
  {
    return publishAnnotatedSegments(
        segmentsToBeOverwritten,
        annotateAtomicUpdateGroupSize(segmentsToPublish),
        commitMetadata
    );
  }

  static Set<DataSegment> annotateAtomicUpdateGroupSize(Set<DataSegment> segments)
  {
    final Map<Interval, List<DataSegment>> intervalToSegments = new HashMap<>();
    segments.forEach(
        segment -> intervalToSegments.computeIfAbsent(segment.getInterval(), k -> new ArrayList<>()).add(segment)
    );

    for (Entry<Interval, List<DataSegment>> entry : intervalToSegments.entrySet()) {
      final Interval interval = entry.getKey();
      final List<DataSegment> segmentsPerInterval = entry.getValue();
      final boolean isNonFirstGeneration = segmentsPerInterval.get(0).getShardSpec() instanceof OverwriteShardSpec;

      final boolean anyMismatch = segmentsPerInterval.stream().anyMatch(
          segment -> (segment.getShardSpec() instanceof OverwriteShardSpec) != isNonFirstGeneration
      );
      if (anyMismatch) {
        throw new ISE(
            "WTH? some segments have empty overshadwedSegments but others are not? "
            + "segments with non-overwritingShardSpec: [%s],"
            + "segments with overwritingShardSpec: [%s]",
            segmentsPerInterval.stream()
                    .filter(segment -> !(segment.getShardSpec() instanceof OverwriteShardSpec))
                    .collect(Collectors.toList()),
            segmentsPerInterval.stream()
                    .filter(segment -> segment.getShardSpec() instanceof OverwriteShardSpec)
                    .collect(Collectors.toList())
        );
      }

      if (isNonFirstGeneration) {
        // The segments which are published together consist an atomicUpdateGroup.

        intervalToSegments.put(
            interval,
            segmentsPerInterval
                .stream()
                .map(segment -> {
                  final OverwriteShardSpec shardSpec = (OverwriteShardSpec) segment.getShardSpec();
                  return segment.withShardSpec(shardSpec.withAtomicUpdateGroupSize((short) segmentsPerInterval.size()));
                })
                .collect(Collectors.toList())
        );
      }
    }

    return intervalToSegments.values().stream().flatMap(Collection::stream).collect(Collectors.toSet());
  }
}
