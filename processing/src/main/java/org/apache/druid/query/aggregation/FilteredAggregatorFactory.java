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

package org.apache.druid.query.aggregation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.druid.query.PerSegmentQueryOptimizationContext;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.IntervalDimFilter;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.filter.Filters;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public class FilteredAggregatorFactory extends AggregatorFactory
{
  private final AggregatorFactory delegate;
  private final DimFilter filter;
  private final String name;

  // Constructor for backwards compat only
  public FilteredAggregatorFactory(
      AggregatorFactory delegate,
      DimFilter filter
  )
  {
    this(delegate, filter, null);
  }

  @JsonCreator
  public FilteredAggregatorFactory(
      @JsonProperty("aggregator") AggregatorFactory delegate,
      @JsonProperty("filter") DimFilter filter,
      @JsonProperty("name") String name
  )
  {
    Preconditions.checkNotNull(delegate);
    Preconditions.checkNotNull(filter);

    this.delegate = delegate;
    this.filter = filter;
    this.name = name;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory columnSelectorFactory)
  {
    final ValueMatcher valueMatcher = Filters.toFilter(filter).makeMatcher(columnSelectorFactory);
    return new FilteredAggregator(
        valueMatcher,
        delegate.factorize(columnSelectorFactory)
    );
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory columnSelectorFactory)
  {
    final ValueMatcher valueMatcher = Filters.toFilter(filter).makeMatcher(columnSelectorFactory);
    return new FilteredBufferAggregator(
        valueMatcher,
        delegate.factorizeBuffered(columnSelectorFactory)
    );
  }

  @Override
  public Comparator getComparator()
  {
    return delegate.getComparator();
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    return delegate.combine(lhs, rhs);
  }

  @Override
  public AggregateCombiner makeAggregateCombiner()
  {
    return delegate.makeAggregateCombiner();
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return delegate.getCombiningFactory();
  }

  @Override
  public Object deserialize(Object object)
  {
    return delegate.deserialize(object);
  }

  @Nullable
  @Override
  public Object finalizeComputation(@Nullable Object object)
  {
    return delegate.finalizeComputation(object);
  }

  // See https://github.com/apache/incubator-druid/pull/6219#pullrequestreview-148919845
  @JsonProperty
  @Override
  public String getName()
  {
    String name = this.name;
    if (Strings.isNullOrEmpty(name)) {
      name = delegate.getName();
    }
    return name;
  }

  @Override
  public List<String> requiredFields()
  {
    return delegate.requiredFields();
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] filterCacheKey = filter.getCacheKey();
    byte[] aggregatorCacheKey = delegate.getCacheKey();
    return ByteBuffer.allocate(1 + filterCacheKey.length + aggregatorCacheKey.length)
                     .put(AggregatorUtil.FILTERED_AGG_CACHE_TYPE_ID)
                     .put(filterCacheKey)
                     .put(aggregatorCacheKey)
                     .array();
  }

  @Override
  public String getTypeName()
  {
    return delegate.getTypeName();
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return delegate.getMaxIntermediateSizeWithNulls();
  }

  @Override
  public AggregatorFactory optimizeForSegment(PerSegmentQueryOptimizationContext optimizationContext)
  {
    if (filter instanceof IntervalDimFilter) {
      IntervalDimFilter intervalDimFilter = ((IntervalDimFilter) filter);
      if (intervalDimFilter.getExtractionFn() != null) {
        // no support for extraction functions right now
        return this;
      }

      if (!intervalDimFilter.getDimension().equals(ColumnHolder.TIME_COLUMN_NAME)) {
        // segment time boundary optimization only applies when we filter on __time
        return this;
      }

      Interval segmentInterval = optimizationContext.getSegmentDescriptor().getInterval();
      List<Interval> filterIntervals = intervalDimFilter.getIntervals();
      List<Interval> excludedFilterIntervals = new ArrayList<>();
      List<Interval> effectiveFilterIntervals = new ArrayList<>();

      boolean segmentIsCovered = false;
      for (Interval filterInterval : filterIntervals) {
        Interval overlap = filterInterval.overlap(segmentInterval);
        if (overlap == null) {
          excludedFilterIntervals.add(filterInterval);
          continue;
        }

        if (overlap.equals(segmentInterval)) {
          segmentIsCovered = true;
          break;
        } else {
          // clip the overlapping interval to the segment time boundaries
          effectiveFilterIntervals.add(overlap);
        }
      }

      // we can skip applying this filter, everything in the segment will match
      if (segmentIsCovered) {
        return delegate;
      }

      // we can skip this filter, nothing in the segment would match
      if (excludedFilterIntervals.size() == filterIntervals.size()) {
        return new SuppressedAggregatorFactory(delegate);
      }

      return new FilteredAggregatorFactory(
          delegate,
          new IntervalDimFilter(
              intervalDimFilter.getDimension(),
              effectiveFilterIntervals,
              intervalDimFilter.getExtractionFn()
          ),
          this.name
      );
    } else {
      return this;
    }
  }

  @JsonProperty
  public AggregatorFactory getAggregator()
  {
    return delegate;
  }

  @JsonProperty
  public DimFilter getFilter()
  {
    return filter;
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return delegate.getRequiredColumns();
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
    FilteredAggregatorFactory that = (FilteredAggregatorFactory) o;
    return Objects.equals(delegate, that.delegate) &&
           Objects.equals(filter, that.filter) &&
           Objects.equals(name, that.name);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(delegate, filter, name);
  }

  @Override
  public String toString()
  {
    return "FilteredAggregatorFactory{" +
           "delegate=" + delegate +
           ", filter=" + filter +
           ", name='" + name + '\'' +
           '}';
  }
}
