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
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

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
    this.segments = segments;
    // Sort segments to use in validateSegments.
    Collections.sort(this.segments);
  }

  @JsonProperty
  public List<String> getSegments()
  {
    return segments;
  }

  @Override
  public Interval findInterval(String dataSource)
  {
    final List<SegmentId> segmentIds = segments
        .stream()
        .map(segment -> SegmentId.tryParse(dataSource, segment))
        .collect(Collectors.toList());
    return JodaUtils.umbrellaInterval(
        segmentIds.stream().map(SegmentId::getInterval).collect(Collectors.toList())
    );
  }

  @Override
  public boolean validateSegments(List<DataSegment> latestSegments)
  {
    final List<String> thoseSegments = latestSegments
        .stream()
        .map(segment -> segment.getId().toString())
        .sorted()
        .collect(Collectors.toList());
    return this.segments.equals(thoseSegments);
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
