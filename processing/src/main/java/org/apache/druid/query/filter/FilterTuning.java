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

package org.apache.druid.query.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * This class provides a mechansim to influence whether or not indexes are used for a {@link Filter} during processing
 * by {@link org.apache.druid.segment.QueryableIndexStorageAdapter#analyzeFilter} (i.e. will a {@link Filter} be a "pre"
 * filter in which we union indexes for all values that match the filter to create a
 * {@link org.apache.druid.segment.BitmapOffset}/{@link org.apache.druid.segment.vector.BitmapVectorOffset}, or will it
 * be used as a "post" filter and evaluated while scanning row values from the
 * {@link org.apache.druid.segment.FilteredOffset}/{@link org.apache.druid.segment.vector.FilteredVectorOffset}.
 *
 * This is currently only manually supplied by the user by adding to a {@link DimFilter} which will pass through to the
 * {@link Filter} implementation. The main purpose at this time is to facilitate experimentation so that someday we can
 * have {@link Filter} implementations intelligently, automatically use sensible defaults based on things like
 * cardinality and who yet knows what additional information.
 *
 * It can also be used for advanced users to manually control which filters will be "pre" and "post" filters as
 * described above to allow skipping indexes in known cases where filters are expensive (mostly high cardinality columns
 * with expensive filters).
 *
 * As such, it is currently undocumented in user facing documentation on purpose, but whatever this turns into once more
 * automatic usage of this is in place, should be documented in a future release.
 */
public class FilterTuning
{
  public static FilterTuning createDefault(Filter filter, BitmapIndexSelector selector)
  {
    return new FilterTuning(filter.supportsBitmapIndex(selector), null, null);
  }

  private final boolean useBitmapIndex;
  private final int minCardinalityToUseBitmapIndex;
  private final int maxCardinalityToUseBitmapIndex;

  @JsonCreator
  public FilterTuning(
      @JsonProperty("useBitmapIndex") @Nullable Boolean useBitmapIndex,
      @JsonProperty("minCardinalityToUseBitmapIndex") @Nullable Integer minCardinalityToUseBitmapIndex,
      @JsonProperty("maxCardinalityToUseBitmapIndex") @Nullable Integer maxCardinalityToUseBitmapIndex
  )
  {
    this.useBitmapIndex = useBitmapIndex != null ? useBitmapIndex : true;
    this.minCardinalityToUseBitmapIndex =
        minCardinalityToUseBitmapIndex != null ? minCardinalityToUseBitmapIndex : 0;
    this.maxCardinalityToUseBitmapIndex =
        maxCardinalityToUseBitmapIndex != null ? maxCardinalityToUseBitmapIndex : Integer.MAX_VALUE;
  }

  @JsonProperty
  public boolean getUseBitmapIndex()
  {
    return useBitmapIndex;
  }

  @JsonProperty
  public int getMinCardinalityToUseBitmapIndex()
  {
    return minCardinalityToUseBitmapIndex;
  }

  @JsonProperty
  public int getMaxCardinalityToUseBitmapIndex()
  {
    return maxCardinalityToUseBitmapIndex;
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
    FilterTuning that = (FilterTuning) o;
    return Objects.equals(useBitmapIndex, that.useBitmapIndex) &&
           Objects.equals(minCardinalityToUseBitmapIndex, that.minCardinalityToUseBitmapIndex) &&
           Objects.equals(maxCardinalityToUseBitmapIndex, that.maxCardinalityToUseBitmapIndex);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(useBitmapIndex, minCardinalityToUseBitmapIndex, maxCardinalityToUseBitmapIndex);
  }

  @Override
  public String toString()
  {
    return "FilterTuning{" +
           "useBitmapIndex=" + useBitmapIndex +
           ", minCardinalityToUseBitmapIndex=" + minCardinalityToUseBitmapIndex +
           ", maxCardinalityToUseBitmapIndex=" + maxCardinalityToUseBitmapIndex +
           '}';
  }
}
