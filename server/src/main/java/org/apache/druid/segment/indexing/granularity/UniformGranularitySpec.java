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
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.granularity.IntervalsByGranularity;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

public class UniformGranularitySpec extends BaseGranularitySpec
{
  private final Granularity segmentGranularity;
  private final Granularity queryGranularity;
  private final IntervalsByGranularity intervalsByGranularity;
  protected LookupIntervalBuckets lookupTableBucketByDateTime;

  @JsonCreator
  public UniformGranularitySpec(
      @JsonProperty("segmentGranularity") Granularity segmentGranularity,
      @JsonProperty("queryGranularity") Granularity queryGranularity,
      @JsonProperty("rollup") Boolean rollup,
      @JsonProperty("safeInput") Boolean safeInput,
      @JsonProperty("safeStart") @Nullable DateTime safeStartTime,
      @JsonProperty("safeEnd") @Nullable DateTime safeEndTime,
      @JsonProperty("intervals") @Nullable List<Interval> inputIntervals
  )
  {
    super(inputIntervals, rollup, safeInput, safeStartTime, safeEndTime);
    this.queryGranularity = queryGranularity == null ? DEFAULT_QUERY_GRANULARITY : queryGranularity;
    this.segmentGranularity = segmentGranularity == null ? DEFAULT_SEGMENT_GRANULARITY : segmentGranularity;
    intervalsByGranularity = new IntervalsByGranularity(this.inputIntervals, segmentGranularity);
    lookupTableBucketByDateTime = new LookupIntervalBuckets(
        sortedBucketIntervals(),
        this.safeInput,
        this.safeStartTime,
        this.safeEndTime
    );
  }

  public UniformGranularitySpec(
      Granularity segmentGranularity,
      Granularity queryGranularity,
      Boolean rollup,
      List<Interval> inputIntervals
  )
  {
    this(segmentGranularity, queryGranularity, rollup, false, null, null, inputIntervals);
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
    return intervalsByGranularity::granularityIntervalsIterator;
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
    if (isSafeInput() != that.isSafeInput()) {
      return false;
    }
    if (!getSafeStart().equals(that.getSafeStart())) {
      return false;
    }
    if (!getSafeEnd().equals(that.getSafeEnd())) {
      return false;
    }

    return Objects.equals(inputIntervals, that.inputIntervals);
  }

  @Override
  public int hashCode()
  {
    int result = segmentGranularity.hashCode();
    result = 31 * result + queryGranularity.hashCode();
    result = 31 * result + rollup.hashCode();
    result = 31 * result + (inputIntervals != null ? inputIntervals.hashCode() : 0);
    result = 31 * result + safeInput.hashCode();
    result = 31 * result + safeStartTime.hashCode();
    result = 31 * result + safeEndTime.hashCode();
    return result;
  }

  @Override
  public String toString()
  {
    return "UniformGranularitySpec{" +
           "segmentGranularity=" + segmentGranularity +
           ", queryGranularity=" + queryGranularity +
           ", rollup=" + rollup +
           ", safeInput=" + safeInput +
           ", safeStart=" + safeStartTime +
           ", safeEnd=" + safeEndTime +
           ", inputIntervals=" + inputIntervals +
           '}';
  }

  @Override
  public GranularitySpec withIntervals(List<Interval> inputIntervals)
  {
    return new UniformGranularitySpec(
        segmentGranularity,
        queryGranularity,
        rollup,
        safeInput,
        safeStartTime,
        safeEndTime,
        inputIntervals
    );
  }

  @Override
  protected LookupIntervalBuckets getLookupTableBuckets()
  {
    return lookupTableBucketByDateTime;
  }
}
