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

package org.apache.druid.metadata;

import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentTimeline;
import org.joda.time.Interval;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SegmentCache
{
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private final Map<String, SegmentTimeline> datasourceTimelines = new HashMap<>();
  private final Set<DataSegment> addedSegments = new HashSet<>();
  private final Set<DataSegment> removedSegments = new HashSet<>();
  private final Map<String, Map<Interval, Set<SegmentIdWithShardSpec>>> datasourcePendingSegments
      = new HashMap<>();

  public void clear()
  {
    lock.writeLock().lock();
    try {
      datasourceTimelines.clear();
      addedSegments.clear();
      removedSegments.clear();
      datasourcePendingSegments.clear();
    }
    finally {
      lock.writeLock().unlock();
    }
  }

  public void addPendingSegments(String datasource, Collection<SegmentIdWithShardSpec> pendingSegments)
  {
    lock.writeLock().lock();
    try {
      datasourcePendingSegments.putIfAbsent(datasource, new HashMap<>());
      for (SegmentIdWithShardSpec pendingSegment : pendingSegments) {
        datasourcePendingSegments.get(datasource)
                                 .computeIfAbsent(pendingSegment.getInterval(), itvl -> new HashSet<>())
                                 .add(pendingSegment);
      }
    }
    finally {
      lock.writeLock().unlock();
    }
  }

  public Set<SegmentIdWithShardSpec> getPendingSegments(String datasource, Interval interval)
  {
    lock.readLock().lock();
    try {
      if (!datasourcePendingSegments.containsKey(datasource)) {
        return Collections.emptySet();
      }
      Set<SegmentIdWithShardSpec> pendingSegments = new HashSet<>();
      for (Interval itvl : datasourcePendingSegments.get(datasource).keySet()) {
        if (itvl.overlaps(interval)) {
          pendingSegments.addAll(datasourcePendingSegments.get(datasource).get(itvl));
        }
      }
      return pendingSegments;
    }
    finally {
      lock.readLock().unlock();
    }
  }

  public void addSegments(String datasource, Collection<DataSegment> segments)
  {
    lock.writeLock().lock();
    try {
      //addedSegments.addAll(segments);
      //removedSegments.removeAll(segments);
      datasourceTimelines.computeIfAbsent(datasource, ds -> new SegmentTimeline())
                         .addSegments(segments.iterator());
    }
    finally {
      lock.writeLock().unlock();
    }
  }

  public void removeSegments(String datasource, Collection<DataSegment> segments)
  {
    lock.writeLock().lock();
    try {
      SegmentTimeline timeline = datasourceTimelines.get(datasource);
      if (timeline == null) {
        return;
      }
      timeline.removeSegments(segments);
      if (timeline.isEmpty()) {
        datasourceTimelines.remove(datasource);
      }
      removedSegments.addAll(segments);
      addedSegments.removeAll(segments);
    }
    finally {
      lock.writeLock().unlock();
    }
  }

  public Map<String, SegmentTimeline> getDatasourceTimelines()
  {
    lock.readLock().lock();
    try {
      return datasourceTimelines;
    }
    finally {
      lock.readLock().unlock();
    }
  }

  public Pair<Set<DataSegment>, Set<DataSegment>> getChangeSetAndReset()
  {
    lock.writeLock().lock();
    try {
      Pair<Set<DataSegment>, Set<DataSegment>> changeSet = new Pair<>(
          ImmutableSet.copyOf(addedSegments),
          ImmutableSet.copyOf(removedSegments)
      );
      addedSegments.clear();
      removedSegments.clear();
      return changeSet;
    }
    finally {
      lock.writeLock().unlock();
    }
  }
}
