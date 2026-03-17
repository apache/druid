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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Compaction input spec that targets specific segments by ID.
 * Used for native engine-based minor compaction: the timeline is built from all segments in the interval,
 * and segments not in this spec are upgraded (no physical re-compaction).
 */
public class SpecificSegmentsSpec implements CompactionInputSpec
{
  public static final String TYPE = "segments";

  private final List<String> segments;

  public static SpecificSegmentsSpec fromSegments(List<DataSegment> segments)
  {
    Preconditions.checkArgument(!segments.isEmpty(), "Empty segment list");
    return new SpecificSegmentsSpec(
        segments.stream().map(segment -> segment.getId().toString()).collect(Collectors.toList())
    );
  }

  @JsonCreator
  public SpecificSegmentsSpec(@JsonProperty("segments") List<String> segments)
  {
    Preconditions.checkArgument(segments != null && !segments.isEmpty(), "Segments must not be null or empty");
    this.segments = segments;
    // Sort segments to use in validateSegments.
    Collections.sort(this.segments);
  }

  @JsonProperty
  public List<String> getSegments()
  {
    return segments;
  }

  /**
   * Parses segment IDs to descriptors for minor compaction (compact vs upgrade partitioning).
   * Invalid IDs are filtered out.
   */
  List<SegmentDescriptor> getSegmentDescriptors(String dataSource)
  {
    return segments.stream()
        .map(id -> SegmentId.tryParse(dataSource, id))
        .filter(Objects::nonNull)
        .map(SegmentId::toDescriptor)
        .collect(Collectors.toList());
  }

  @Override
  public Interval findInterval(String dataSource)
  {
    final List<Interval> intervals = segments
        .stream()
        .map(segment -> SegmentId.tryParse(dataSource, segment))
        .filter(Objects::nonNull)
        .map(SegmentId::getInterval)
        .collect(Collectors.toList());
    return JodaUtils.umbrellaInterval(intervals);
  }

  @Override
  public boolean validateSegments(LockGranularity lockGranularityInUse, List<DataSegment> latestSegments)
  {
    final List<String> thoseSegments = latestSegments
        .stream()
        .map(segment -> segment.getId().toString())
        .sorted()
        .collect(Collectors.toList());

    return thoseSegments.containsAll(segments);
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SpecificSegmentsSpec that = (SpecificSegmentsSpec) o;
    return Objects.equals(segments, that.segments);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(segments);
  }

  @Override
  public String toString()
  {
    return "SpecificSegmentsSpec{" +
           "segments=" + segments +
           '}';
  }
}
