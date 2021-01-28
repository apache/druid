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
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.List;

public class ArbitraryGranularitySpec extends BaseGranularitySpec
{
  private final Granularity queryGranularity;
  protected LookupIntervalBuckets lookupTableBucketByDateTime;

  @JsonCreator
  public ArbitraryGranularitySpec(
      @JsonProperty("queryGranularity") Granularity queryGranularity,
      @JsonProperty("rollup") Boolean rollup,
      @JsonProperty("intervals") @Nullable List<Interval> inputIntervals
  )
  {
    super(inputIntervals, rollup);
    this.queryGranularity = queryGranularity == null ? Granularities.NONE : queryGranularity;

    lookupTableBucketByDateTime = new LookupIntervalBuckets(inputIntervals);

    // Ensure intervals are non-overlapping (but they may abut each other)
    final PeekingIterator<Interval> intervalIterator = Iterators.peekingIterator(sortedBucketIntervals().iterator());
    while (intervalIterator.hasNext()) {
      final Interval currentInterval = intervalIterator.next();
      if (intervalIterator.hasNext()) {
        final Interval nextInterval = intervalIterator.peek();
        if (currentInterval.overlaps(nextInterval)) {
          throw new IAE("Overlapping intervals: %s, %s", currentInterval, nextInterval);
        }
      }
    }
  }

  public ArbitraryGranularitySpec(
      Granularity queryGranularity,
      List<Interval> inputIntervals
  )
  {
    this(queryGranularity, true, inputIntervals);
  }

  @Override
  public Iterable<Interval> sortedBucketIntervals()
  {
    return () -> lookupTableBucketByDateTime.iterator();
  }

  @Override
  public Granularity getSegmentGranularity()
  {
    throw new UnsupportedOperationException();
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

    ArbitraryGranularitySpec that = (ArbitraryGranularitySpec) o;

    if (!inputIntervals().equals(that.inputIntervals())) {
      return false;
    }
    if (!rollup.equals(that.rollup)) {
      return false;
    }

    return !(queryGranularity != null
             ? !queryGranularity.equals(that.queryGranularity)
             : that.queryGranularity != null);

  }

  @Override
  public int hashCode()
  {
    int result = inputIntervals().hashCode();
    result = 31 * result + rollup.hashCode();
    result = 31 * result + (queryGranularity != null ? queryGranularity.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "ArbitraryGranularitySpec{" +
           "intervals=" + inputIntervals() +
           ", queryGranularity=" + queryGranularity +
           ", rollup=" + rollup +
           '}';
  }

  @Override
  public GranularitySpec withIntervals(List<Interval> inputIntervals)
  {
    return new ArbitraryGranularitySpec(queryGranularity, rollup, inputIntervals);
  }

  @Override
  protected LookupIntervalBuckets getLookupTableBuckets()
  {
    return lookupTableBucketByDateTime;
  }

}
