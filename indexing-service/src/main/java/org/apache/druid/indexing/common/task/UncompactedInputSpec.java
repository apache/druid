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
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Specifies uncompacted segments to compact within an interval.
 * Used for minor compaction to compact only uncompacted segments while leaving compacted segments untouched.
 */
public class UncompactedInputSpec implements CompactionInputSpec
{
  public static final String TYPE = "uncompacted";

  private final Interval interval;
  private final List<SegmentDescriptor> uncompactedSegments;

  @JsonCreator
  public UncompactedInputSpec(
      @JsonProperty("interval") Interval interval,
      @JsonProperty("uncompactedSegments") List<SegmentDescriptor> uncompactedSegments
  )
  {
    if (interval == null) {
      throw new IAE("Uncompacted interval must not be null");
    }
    if (interval.toDurationMillis() == 0) {
      throw new IAE("Uncompacted interval[%s] is empty, must specify a nonempty interval", interval);
    }
    if (uncompactedSegments == null || uncompactedSegments.isEmpty()) {
      throw new IAE("Uncompacted segments must not be null or empty");
    }

    // Validate that all segments are within the interval
    List<SegmentDescriptor> segmentsNotInInterval =
        uncompactedSegments.stream().filter(s -> !interval.contains(s.getInterval())).collect(Collectors.toList());
    if (!segmentsNotInInterval.isEmpty()) {
      throw new IAE(
          "All uncompacted segments must be within interval[%s], got segments outside interval: %s",
          interval,
          segmentsNotInInterval
      );
    }

    this.interval = interval;
    this.uncompactedSegments = uncompactedSegments;
  }

  @JsonProperty
  public Interval getInterval()
  {
    return interval;
  }

  @JsonProperty
  public List<SegmentDescriptor> getUncompactedSegments()
  {
    return uncompactedSegments;
  }

  @Override
  public Interval findInterval(String dataSource)
  {
    return interval;
  }

  @Override
  public boolean validateSegments(LockGranularity lockGranularityInUse, List<DataSegment> latestSegments)
  {
    final Interval segmentsInterval = JodaUtils.umbrellaInterval(
        latestSegments.stream().map(DataSegment::getInterval).collect(Collectors.toList())
    );
    return interval.overlaps(segmentsInterval);
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
    UncompactedInputSpec that = (UncompactedInputSpec) o;
    return Objects.equals(interval, that.interval) &&
           Objects.equals(uncompactedSegments, that.uncompactedSegments);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(interval, uncompactedSegments);
  }

  @Override
  public String toString()
  {
    return "UncompactedInputSpec{" +
           "interval=" + interval +
           ", uncompactedSegments=" + uncompactedSegments +
           '}';
  }
}
