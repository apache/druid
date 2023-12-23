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

package org.apache.druid.server.coordinator.compact;

import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.segment.SegmentUtils;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;

import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

/**
 * List of segments to compact.
 */
public class SegmentsToCompact
{
  private static final SegmentsToCompact EMPTY_INSTANCE = new SegmentsToCompact();

  private final List<DataSegment> segments;
  private final Interval umbrellaInterval;
  private final long totalBytes;
  private final int numIntervals;

  static SegmentsToCompact empty()
  {
    return EMPTY_INSTANCE;
  }

  public static SegmentsToCompact from(List<DataSegment> segments)
  {
    if (segments == null || segments.isEmpty()) {
      return empty();
    } else {
      return new SegmentsToCompact(segments);
    }
  }

  private SegmentsToCompact()
  {
    this.segments = Collections.emptyList();
    this.totalBytes = 0L;
    this.numIntervals = 0;
    this.umbrellaInterval = null;
  }

  private SegmentsToCompact(List<DataSegment> segments)
  {
    this.segments = segments;
    this.totalBytes = segments.stream().mapToLong(DataSegment::getSize).sum();
    this.umbrellaInterval = JodaUtils.umbrellaInterval(
        segments.stream().map(DataSegment::getInterval).collect(Collectors.toList())
    );
    this.numIntervals = (int) segments.stream().map(DataSegment::getInterval).distinct().count();
  }

  public List<DataSegment> getSegments()
  {
    return segments;
  }

  public DataSegment getFirst()
  {
    if (segments.isEmpty()) {
      throw new NoSuchElementException("No segment to compact");
    } else {
      return segments.get(0);
    }
  }

  public boolean isEmpty()
  {
    return segments.isEmpty();
  }

  public long getTotalBytes()
  {
    return totalBytes;
  }

  public int size()
  {
    return segments.size();
  }

  public Interval getUmbrellaInterval()
  {
    return umbrellaInterval;
  }

  public long getNumIntervals()
  {
    return numIntervals;
  }

  @Override
  public String toString()
  {
    return "SegmentsToCompact{" +
           "segments=" + SegmentUtils.commaSeparatedIdentifiers(segments) +
           ", totalSize=" + totalBytes +
           '}';
  }
}
