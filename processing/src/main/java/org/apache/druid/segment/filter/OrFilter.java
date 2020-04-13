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

package org.apache.druid.segment.filter;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.BitmapResultFactory;
import org.apache.druid.query.filter.BitmapIndexSelector;
import org.apache.druid.query.filter.BooleanFilter;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.RowOffsetMatcherFactory;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.filter.vector.BaseVectorValueMatcher;
import org.apache.druid.query.filter.vector.ReadableVectorMatch;
import org.apache.druid.query.filter.vector.VectorMatch;
import org.apache.druid.query.filter.vector.VectorValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 */
public class OrFilter implements BooleanFilter
{
  private static final Joiner OR_JOINER = Joiner.on(" || ");

  private final Set<Filter> filters;

  @VisibleForTesting
  public OrFilter(List<Filter> filters)
  {
    this(new HashSet<>(filters));
  }

  public OrFilter(Set<Filter> filters)
  {
    Preconditions.checkArgument(filters.size() > 0, "Can't construct empty OrFilter (the universe does not exist)");

    this.filters = filters;
  }

  @Override
  public <T> T getBitmapResult(BitmapIndexSelector selector, BitmapResultFactory<T> bitmapResultFactory)
  {
    if (filters.size() == 1) {
      return Iterables.getOnlyElement(filters).getBitmapResult(selector, bitmapResultFactory);
    }

    List<T> bitmapResults = new ArrayList<>();
    for (Filter filter : filters) {
      bitmapResults.add(filter.getBitmapResult(selector, bitmapResultFactory));
    }

    return bitmapResultFactory.union(bitmapResults);
  }

  @Override
  public ValueMatcher makeMatcher(ColumnSelectorFactory factory)
  {
    final ValueMatcher[] matchers = new ValueMatcher[filters.size()];

    int i = 0;
    for (Filter filter : filters) {
      matchers[i++] = filter.makeMatcher(factory);
    }
    return makeMatcher(matchers);
  }

  @Override
  public VectorValueMatcher makeVectorMatcher(final VectorColumnSelectorFactory factory)
  {
    final VectorValueMatcher[] matchers = new VectorValueMatcher[filters.size()];

    int i = 0;
    for (Filter filter : filters) {
      matchers[i++] = filter.makeVectorMatcher(factory);
    }
    return makeVectorMatcher(matchers);
  }

  @Override
  public boolean canVectorizeMatcher()
  {
    return filters.stream().allMatch(Filter::canVectorizeMatcher);
  }

  @Override
  public ValueMatcher makeMatcher(
      BitmapIndexSelector selector,
      ColumnSelectorFactory columnSelectorFactory,
      RowOffsetMatcherFactory rowOffsetMatcherFactory
  )
  {
    final List<ValueMatcher> matchers = new ArrayList<>();
    final List<ImmutableBitmap> bitmaps = new ArrayList<>();

    for (Filter filter : filters) {
      if (filter.supportsBitmapIndex(selector)) {
        bitmaps.add(filter.getBitmapIndex(selector));
      } else {
        ValueMatcher matcher = filter.makeMatcher(columnSelectorFactory);
        matchers.add(matcher);
      }
    }

    if (bitmaps.size() > 0) {
      ImmutableBitmap combinedBitmap = selector.getBitmapFactory().union(bitmaps);
      ValueMatcher offsetMatcher = rowOffsetMatcherFactory.makeRowOffsetMatcher(combinedBitmap);
      matchers.add(0, offsetMatcher);
    }

    return makeMatcher(matchers.toArray(BooleanFilter.EMPTY_VALUE_MATCHER_ARRAY));
  }

  @Override
  public Set<Filter> getFilters()
  {
    return filters;
  }

  @Override
  public double estimateSelectivity(BitmapIndexSelector indexSelector)
  {
    // Estimate selectivity with attribute value independence assumption
    double selectivity = 0;
    for (final Filter filter : filters) {
      selectivity += filter.estimateSelectivity(indexSelector);
    }
    return Math.min(selectivity, 1.);
  }

  @Override
  public String toString()
  {
    return StringUtils.format("(%s)", OR_JOINER.join(filters));
  }

  private static ValueMatcher makeMatcher(final ValueMatcher[] baseMatchers)
  {
    Preconditions.checkState(baseMatchers.length > 0);

    if (baseMatchers.length == 1) {
      return baseMatchers[0];
    }

    return new ValueMatcher()
    {
      @Override
      public boolean matches()
      {
        for (ValueMatcher matcher : baseMatchers) {
          if (matcher.matches()) {
            return true;
          }
        }
        return false;
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("firstBaseMatcher", baseMatchers[0]);
        inspector.visit("secondBaseMatcher", baseMatchers[1]);
        // Don't inspect the 3rd and all consequent baseMatchers, cut runtime shape combinations at this point.
        // Anyway if the filter is so complex, Hotspot won't inline all calls because of the inline limit.
      }
    };
  }

  private static VectorValueMatcher makeVectorMatcher(final VectorValueMatcher[] baseMatchers)
  {
    Preconditions.checkState(baseMatchers.length > 0);
    if (baseMatchers.length == 1) {
      return baseMatchers[0];
    }

    return new BaseVectorValueMatcher(baseMatchers[0])
    {
      final VectorMatch currentMask = VectorMatch.wrap(new int[getMaxVectorSize()]);
      final VectorMatch scratch = VectorMatch.wrap(new int[getMaxVectorSize()]);
      final VectorMatch retVal = VectorMatch.wrap(new int[getMaxVectorSize()]);

      @Override
      public ReadableVectorMatch match(final ReadableVectorMatch mask)
      {
        ReadableVectorMatch currentMatch = baseMatchers[0].match(mask);

        currentMask.copyFrom(mask);
        retVal.copyFrom(currentMatch);

        for (int i = 1; i < baseMatchers.length; i++) {
          if (retVal.isAllTrue(getCurrentVectorSize())) {
            // Short-circuit if the entire vector is true.
            break;
          }

          currentMask.removeAll(currentMatch);
          currentMatch = baseMatchers[i].match(currentMask);
          retVal.addAll(currentMatch, scratch);
        }

        assert retVal.isValid(mask);
        return retVal;
      }
    };
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
    OrFilter orFilter = (OrFilter) o;
    return Objects.equals(getFilters(), orFilter.getFilters());
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(getFilters());
  }
}
