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

package org.apache.druid.indexer.granularity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.joda.time.Interval;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;

/**
 * The segment-partitioning slice of granularity for a base-table-mode
 * {@link org.apache.druid.segment.indexing.DataSchema#baseTable}: the segment granularity that buckets rows into
 * segments plus the optional input intervals to bucket.
 * <p>
 * For backwards compatibility {@link org.apache.druid.segment.indexing.DataSchema} recombines this spec with the
 * base-table spec's query-granularity virtual column and rollup into a full {@link GranularitySpec} that legacy
 * consumers continue to read through {@code DataSchema.getGranularitySpec()}.
 */
public class SegmentGranularitySpec
{
  private final Granularity segmentGranularity;
  @Nullable
  private final List<Interval> intervals;

  @JsonCreator
  public SegmentGranularitySpec(
      @JsonProperty("segmentGranularity") @Nullable Granularity segmentGranularity,
      @JsonProperty("intervals") @Nullable List<Interval> intervals
  )
  {
    this.segmentGranularity = segmentGranularity == null
                              ? BaseGranularitySpec.DEFAULT_SEGMENT_GRANULARITY
                              : segmentGranularity;
    this.intervals = intervals;
  }

  @JsonProperty
  public Granularity getSegmentGranularity()
  {
    return segmentGranularity;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public List<Interval> getIntervals()
  {
    return intervals;
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
    SegmentGranularitySpec that = (SegmentGranularitySpec) o;
    return Objects.equals(segmentGranularity, that.segmentGranularity)
           && Objects.equals(intervals, that.intervals);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(segmentGranularity, intervals);
  }

  @Override
  public String toString()
  {
    return "SegmentGranularitySpec{" +
           "segmentGranularity=" + segmentGranularity +
           ", intervals=" + intervals +
           '}';
  }
}
