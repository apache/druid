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

package org.apache.druid.server.coordinator;

import org.apache.druid.timeline.SegmentId;
import org.eclipse.jetty.util.ConcurrentHashSet;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Contains segments being moved or replicated in a tier.
 * <p>
 * All methods of this class except {@link #markCompleted(SegmentId)} must be
 * invoked only by the duty thread. {@link #markCompleted(SegmentId)} may be
 * invoked by both duty and callback threads.
 */
public class TierLoadingState
{
  /**
   * Map from current lifetime to the set of segments with that lifetime remaining.
   * Not thread-safe as it is only accessed by the coordinator duty thread.
   */
  private final Map<Integer, SegmentGroup> lifetimeToGroup = new HashMap<>();

  /**
   * Groups of segments which have finished their lifetime (i.e. lifetime < 0).
   * Not thread-safe as it is only accessed by the duty thread.
   */
  private final Set<SegmentGroup> expiredGroups = new HashSet<>();

  private final ConcurrentHashMap<SegmentId, LoadingSegment> processingSegments = new ConcurrentHashMap<>();

  private int minLifetime;

  public int getNumProcessingSegments()
  {
    return processingSegments.size();
  }

  public int getNumExpiredSegments()
  {
    return expiredGroups.stream().mapToInt(group -> group.segments.size()).sum();
  }

  public List<String> getExpiredSegments()
  {
    return expiredGroups.stream()
                        .flatMap(group -> group.segments.stream())
                        .map(LoadingSegment::toString)
                        .collect(Collectors.toList());
  }

  public int getMinLifetime()
  {
    return minLifetime;
  }

  void markStarted(SegmentId segmentId, String host, int lifetime)
  {
    SegmentGroup group = lifetimeToGroup.computeIfAbsent(lifetime, SegmentGroup::new);
    processingSegments.put(segmentId, new LoadingSegment(segmentId, host, group));
  }

  void markCompleted(SegmentId segmentId)
  {
    LoadingSegment loadingSegment = processingSegments.remove(segmentId);
    if (loadingSegment != null) {
      loadingSegment.group.remove(loadingSegment);
    }
  }

  void reduceLifetime()
  {
    minLifetime = 0;

    final Map<Integer, SegmentGroup> lifetimeToGroupCopy = new HashMap<>(lifetimeToGroup);
    lifetimeToGroup.clear();
    lifetimeToGroupCopy.values().forEach(
        group -> {
          if (group.segments.isEmpty()) {
            expiredGroups.remove(group);
            return;
          }

          group.lifetime--;
          int updatedLifetime = group.lifetime;
          lifetimeToGroup.put(updatedLifetime, group);
          if (updatedLifetime < 0) {
            expiredGroups.add(group);
          }
          if (updatedLifetime < minLifetime) {
            minLifetime = updatedLifetime;
          }
        }
    );
  }

  /**
   * Group of segments with the same lifetime.
   */
  private static class SegmentGroup
  {
    int lifetime;

    /**
     * Must be thread-safe as it can be accessed by callbacks too.
     */
    final Set<LoadingSegment> segments = new ConcurrentHashSet<>();

    SegmentGroup(int lifetime)
    {
      this.lifetime = lifetime;
    }

    void add(LoadingSegment segment)
    {
      segments.add(segment);
    }

    void remove(LoadingSegment segment)
    {
      segments.remove(segment);
    }
  }

  private static class LoadingSegment
  {
    final SegmentId segmentId;
    final String host;
    final SegmentGroup group;

    LoadingSegment(SegmentId segmentId, String host, SegmentGroup group)
    {
      this.segmentId = segmentId;
      this.host = host;
      this.group = group;

      group.add(this);
    }

    @Override
    public String toString()
    {
      return segmentId + " ON " + host;
    }
  }
}
