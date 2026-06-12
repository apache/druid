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

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntMaps;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Maintains a count of segments for each datasource and interval.
 */
public class SegmentCountsPerInterval
{
  private int totalSegments;
  private long totalSegmentBytes;
  private final Map<String, Object2IntMap<Interval>> datasourceIntervalToSegmentCount = new HashMap<>();
  private final Object2IntMap<Interval> intervalToTotalSegmentCount = new Object2IntOpenHashMap<>();
  private final Object2IntMap<String> datasourceToTotalSegmentCount = new Object2IntOpenHashMap<>();
  private final NavigableMap<Long, Object2IntMap<Interval>> startMillisToTotalSegmentCount = new TreeMap<>();
  private final NavigableMap<Long, Object2IntMap<Interval>> endMillisToTotalSegmentCount = new TreeMap<>();
  private final Map<String, NavigableMap<Long, Object2IntMap<Interval>>> datasourceStartMillisToSegmentCount
      = new HashMap<>();
  private final Map<String, NavigableMap<Long, Object2IntMap<Interval>>> datasourceEndMillisToSegmentCount
      = new HashMap<>();

  public void addSegment(DataSegment segment)
  {
    updateCountInInterval(segment, 1);
    totalSegmentBytes += segment.getSize();
  }

  public void removeSegment(DataSegment segment)
  {
    updateCountInInterval(segment, -1);
    totalSegmentBytes -= segment.getSize();
  }

  public int getTotalSegmentCount()
  {
    return totalSegments;
  }

  public long getTotalSegmentBytes()
  {
    return totalSegmentBytes;
  }

  public Object2IntMap<String> getDatasourceToTotalSegmentCount()
  {
    return datasourceToTotalSegmentCount;
  }

  public Object2IntMap<Interval> getIntervalToSegmentCount(String datasource)
  {
    return datasourceIntervalToSegmentCount.getOrDefault(datasource, Object2IntMaps.emptyMap());
  }

  public Object2IntMap<Interval> getIntervalToTotalSegmentCount()
  {
    return intervalToTotalSegmentCount;
  }

  public NavigableMap<Long, Object2IntMap<Interval>> getStartMillisToTotalSegmentCount()
  {
    return startMillisToTotalSegmentCount;
  }

  public NavigableMap<Long, Object2IntMap<Interval>> getEndMillisToTotalSegmentCount()
  {
    return endMillisToTotalSegmentCount;
  }

  public NavigableMap<Long, Object2IntMap<Interval>> getStartMillisToSegmentCount(String datasource)
  {
    return datasourceStartMillisToSegmentCount.getOrDefault(datasource, Collections.emptyNavigableMap());
  }

  public NavigableMap<Long, Object2IntMap<Interval>> getEndMillisToSegmentCount(String datasource)
  {
    return datasourceEndMillisToSegmentCount.getOrDefault(datasource, Collections.emptyNavigableMap());
  }

  private void updateCountInInterval(DataSegment segment, int delta)
  {
    totalSegments += delta;
    updateIntervalCount(
        intervalToTotalSegmentCount,
        startMillisToTotalSegmentCount,
        endMillisToTotalSegmentCount,
        segment.getInterval(),
        delta
    );
    datasourceToTotalSegmentCount.mergeInt(segment.getDataSource(), delta, Integer::sum);

    final String datasource = segment.getDataSource();
    updateIntervalCount(
        datasourceIntervalToSegmentCount.computeIfAbsent(datasource, ds -> new Object2IntOpenHashMap<>()),
        datasourceStartMillisToSegmentCount.computeIfAbsent(datasource, ds -> new TreeMap<>()),
        datasourceEndMillisToSegmentCount.computeIfAbsent(datasource, ds -> new TreeMap<>()),
        segment.getInterval(),
        delta
    );
  }

  private static void updateIntervalCount(
      Object2IntMap<Interval> intervalToSegmentCount,
      NavigableMap<Long, Object2IntMap<Interval>> startMillisToSegmentCount,
      NavigableMap<Long, Object2IntMap<Interval>> endMillisToSegmentCount,
      Interval interval,
      int delta
  )
  {
    final int updatedCount = intervalToSegmentCount.getInt(interval) + delta;
    if (updatedCount == 0) {
      intervalToSegmentCount.removeInt(interval);
      removeFromIntervalIndex(startMillisToSegmentCount, interval.getStartMillis(), interval);
      removeFromIntervalIndex(endMillisToSegmentCount, interval.getEndMillis(), interval);
    } else {
      intervalToSegmentCount.put(interval, updatedCount);
      startMillisToSegmentCount
          .computeIfAbsent(interval.getStartMillis(), startMillis -> new Object2IntOpenHashMap<>())
          .put(interval, updatedCount);
      endMillisToSegmentCount
          .computeIfAbsent(interval.getEndMillis(), endMillis -> new Object2IntOpenHashMap<>())
          .put(interval, updatedCount);
    }
  }

  private static void removeFromIntervalIndex(
      NavigableMap<Long, Object2IntMap<Interval>> intervalIndex,
      long millis,
      Interval interval
  )
  {
    final Object2IntMap<Interval> segmentCounts = intervalIndex.get(millis);
    if (segmentCounts == null) {
      return;
    }

    segmentCounts.removeInt(interval);
    if (segmentCounts.isEmpty()) {
      intervalIndex.remove(millis);
    }
  }
}
