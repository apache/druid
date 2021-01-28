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

package org.apache.druid.segment.indexing.granularity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.granularity.IntervalsByGranularity;
import org.joda.time.Interval;

import java.util.List;

public class UniformGranularitySpec extends BaseGranularitySpec
{
  private static final Granularity DEFAULT_SEGMENT_GRANULARITY = Granularities.DAY;
  private static final Granularity DEFAULT_QUERY_GRANULARITY = Granularities.NONE;

  private final Granularity segmentGranularity;
  private final Granularity queryGranularity;
  private final IntervalsByGranularity intervalsByGranularity;
  protected LookupIntervalBuckets lookupTableBucketByDateTime;

  @JsonCreator
  public UniformGranularitySpec(
      @JsonProperty("segmentGranularity") Granularity segmentGranularity,
      @JsonProperty("queryGranularity") Granularity queryGranularity,
      @JsonProperty("rollup") Boolean rollup,
      @JsonProperty("intervals") List<Interval> inputIntervals
  )
  {
    super(inputIntervals, rollup);
    this.queryGranularity = queryGranularity == null ? DEFAULT_QUERY_GRANULARITY : queryGranularity;
    this.segmentGranularity = segmentGranularity == null ? DEFAULT_SEGMENT_GRANULARITY : segmentGranularity;
    intervalsByGranularity = new IntervalsByGranularity(this.inputIntervals, segmentGranularity);
    lookupTableBucketByDateTime = new LookupIntervalBuckets(sortedBucketIntervals());
  }

  public UniformGranularitySpec(
      Granularity segmentGranularity,
      Granularity queryGranularity,
      List<Interval> inputIntervals
  )
  {
    this(segmentGranularity, queryGranularity, true, inputIntervals);
  }

  @Override
  public Iterable<Interval> sortedBucketIntervals()
  {
    return () -> intervalsByGranularity.granularityIntervalsIterator();
  }

  @Override
  @JsonProperty("segmentGranularity")
  public Granularity getSegmentGranularity()
  {
    return segmentGranularity;
  }

  @Override
  @JsonProperty("queryGranularity")
  public Granularity getQueryGranularity()
  {
    return queryGranularity;
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

    UniformGranularitySpec that = (UniformGranularitySpec) o;

    if (!segmentGranularity.equals(that.segmentGranularity)) {
      return false;
    }
    if (!queryGranularity.equals(that.queryGranularity)) {
      return false;
    }
    if (isRollup() != that.isRollup()) {
      return false;
    }

    if (inputIntervals != null ? !inputIntervals.equals(that.inputIntervals) : that.inputIntervals != null) {
      return false;
    }

    return true;

  }

  @Override
  public int hashCode()
  {
    int result = segmentGranularity.hashCode();
    result = 31 * result + queryGranularity.hashCode();
    result = 31 * result + rollup.hashCode();
    result = 31 * result + (inputIntervals != null ? inputIntervals.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "UniformGranularitySpec{" +
           "segmentGranularity=" + segmentGranularity +
           ", queryGranularity=" + queryGranularity +
           ", rollup=" + rollup +
           ", inputIntervals=" + inputIntervals +
           '}';
  }

  @Override
  public GranularitySpec withIntervals(List<Interval> inputIntervals)
  {
    return new UniformGranularitySpec(segmentGranularity, queryGranularity, rollup, inputIntervals);
  }

  @Override
  protected LookupIntervalBuckets getLookupTableBuckets()
  {
    return lookupTableBucketByDateTime;
  }

}
