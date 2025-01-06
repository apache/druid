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

package org.apache.druid.metadata.cache;

import org.apache.druid.server.http.DataSegmentPlus;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentTimeline;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

class DatasourceSegmentCache extends BaseCache
{
  final Map<String, DataSegment> idToUsedSegment = new HashMap<>();
  final Map<String, SegmentState> idToSegmentState = new HashMap<>();
  final SegmentTimeline usedSegmentTimeline = SegmentTimeline.forSegments(Set.of());

  final Set<String> unusedSegmentIds = new HashSet<>();

  DatasourceSegmentCache()
  {
    super(true);
  }

  void clear()
  {
    withWriteLock(() -> {
      idToSegmentState.clear();
      idToUsedSegment.clear();
      unusedSegmentIds.clear();
      idToUsedSegment.values().forEach(usedSegmentTimeline::remove);
    });
  }

  boolean shouldRefreshSegment(SegmentState metadataState)
  {
    return withReadLock(() -> {
      final SegmentState cachedState = idToSegmentState.get(metadataState.getSegmentId());
      return cachedState == null
             || cachedState.getLastUpdatedTime().isBefore(metadataState.getLastUpdatedTime());
    });
  }

  boolean refreshUnusedSegment(SegmentState newState)
  {
    if (newState.isUsed()) {
      return false;
    }

    return withWriteLock(() -> {
      if (!shouldRefreshSegment(newState)) {
        return false;
      }

      final SegmentState oldState = idToSegmentState.put(newState.getSegmentId(), newState);

      if (oldState != null && oldState.isUsed()) {
        // Segment has transitioned from used to unused
        DataSegment segment = idToUsedSegment.remove(newState.getSegmentId());
        if (segment != null) {
          usedSegmentTimeline.remove(segment);
        }
      }

      unusedSegmentIds.add(newState.getSegmentId());
      return true;
    });
  }

  boolean refreshUsedSegment(DataSegmentPlus segmentPlus)
  {
    final DataSegment segment = segmentPlus.getDataSegment();
    final SegmentState newState = new SegmentState(
        segment.getId().toString(),
        segment.getDataSource(),
        Boolean.TRUE.equals(segmentPlus.getUsed()),
        segmentPlus.getUsedStatusLastUpdatedDate()
    );
    if (!newState.isUsed()) {
      return refreshUnusedSegment(newState);
    }

    return withWriteLock(() -> {
      if (!shouldRefreshSegment(newState)) {
        return false;
      }

      final SegmentState oldState = idToSegmentState.put(newState.getSegmentId(), newState);
      final DataSegment oldSegment = idToUsedSegment.put(newState.getSegmentId(), segment);

      if (oldState == null) {
        // This is a new segment
      } else if (oldState.isUsed()) {
        // Segment payload may have changed
        if (oldSegment != null) {
          usedSegmentTimeline.remove(oldSegment);
        }
      } else {
        // Segment has transitioned from unused to used
        unusedSegmentIds.remove(newState.getSegmentId());
      }

      usedSegmentTimeline.add(segment);
      return true;
    });
  }

  int removeSegmentIds(Set<String> segmentIds)
  {
    return withWriteLock(() -> {
      int removedCount = 0;
      for (String segmentId : segmentIds) {
        SegmentState state = idToSegmentState.remove(segmentId);
        if (state != null) {
          ++removedCount;
        }

        unusedSegmentIds.remove(segmentId);

        final DataSegment segment = idToUsedSegment.remove(segmentId);
        if (segment != null) {
          usedSegmentTimeline.remove(segment);
        }
      }

      return removedCount;
    });
  }

  Set<String> getUnknownSegmentIds(Set<String> knownSegmentIds)
  {
    return withReadLock(
        () -> idToSegmentState.keySet()
                              .stream()
                              .filter(knownSegmentIds::contains)
                              .collect(Collectors.toSet())
    );
  }
}
