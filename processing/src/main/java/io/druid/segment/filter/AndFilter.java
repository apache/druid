/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.filter;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.druid.collections.bitmap.ImmutableBitmap;
import io.druid.java.util.common.StringUtils;
import io.druid.query.BitmapResultFactory;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.BooleanFilter;
import io.druid.query.filter.Filter;
import io.druid.query.filter.RowOffsetMatcherFactory;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.ColumnSelector;
import io.druid.segment.ColumnSelectorFactory;

import java.util.ArrayList;
import java.util.List;

/**
 */
public class AndFilter implements BooleanFilter
{
  private static final Joiner AND_JOINER = Joiner.on(" && ");
  static final ValueMatcher[] EMPTY_VALUE_MATCHER_ARRAY = new ValueMatcher[0];

  private final List<Filter> filters;

  public AndFilter(List<Filter> filters)
  {
    Preconditions.checkArgument(filters.size() > 0, "Can't construct empty AndFilter");
    this.filters = filters;
  }

  public static <T> ImmutableBitmap getBitmapIndex(
      BitmapIndexSelector selector,
      BitmapResultFactory<T> bitmapResultFactory,
      List<Filter> filters
  )
  {
    return bitmapResultFactory.toImmutableBitmap(getBitmapResult(selector, bitmapResultFactory, filters));
  }

  private static <T> T getBitmapResult(
      BitmapIndexSelector selector,
      BitmapResultFactory<T> bitmapResultFactory,
      List<Filter> filters
  )
  {
    if (filters.size() == 1) {
      return filters.get(0).getBitmapResult(selector, bitmapResultFactory);
    }

    final List<T> bitmapResults = Lists.newArrayListWithCapacity(filters.size());
    for (final Filter filter : filters) {
      Preconditions.checkArgument(filter.supportsBitmapIndex(selector),
                                  "Filter[%s] does not support bitmap index", filter
      );
      final T bitmapResult = filter.getBitmapResult(selector, bitmapResultFactory);
      if (bitmapResultFactory.isEmpty(bitmapResult)) {
        // Short-circuit.
        return bitmapResultFactory.wrapAllFalse(Filters.allFalse(selector));
      }
      bitmapResults.add(bitmapResult);
    }

    return bitmapResultFactory.intersection(bitmapResults);
  }

  @Override
  public <T> T getBitmapResult(BitmapIndexSelector selector, BitmapResultFactory<T> bitmapResultFactory)
  {
    return getBitmapResult(selector, bitmapResultFactory, filters);
  }

  @Override
  public ValueMatcher makeMatcher(ColumnSelectorFactory factory)
  {
    final ValueMatcher[] matchers = new ValueMatcher[filters.size()];

    for (int i = 0; i < filters.size(); i++) {
      matchers[i] = filters.get(i).makeMatcher(factory);
    }
    return makeMatcher(matchers);
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
      ImmutableBitmap combinedBitmap = selector.getBitmapFactory().intersection(bitmaps);
      ValueMatcher offsetMatcher = rowOffsetMatcherFactory.makeRowOffsetMatcher(combinedBitmap);
      matchers.add(0, offsetMatcher);
    }

    return makeMatcher(matchers.toArray(EMPTY_VALUE_MATCHER_ARRAY));
  }

  @Override
  public List<Filter> getFilters()
  {
    return filters;
  }

  @Override
  public boolean supportsBitmapIndex(BitmapIndexSelector selector)
  {
    for (Filter filter : filters) {
      if (!filter.supportsBitmapIndex(selector)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean supportsSelectivityEstimation(
      final ColumnSelector columnSelector, final BitmapIndexSelector indexSelector
  )
  {
    for (Filter filter : filters) {
      if (!filter.supportsSelectivityEstimation(columnSelector, indexSelector)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public double estimateSelectivity(BitmapIndexSelector indexSelector)
  {
    // Estimate selectivity with attribute value independence assumption
    double selectivity = 1.0;
    for (final Filter filter : filters) {
      selectivity *= filter.estimateSelectivity(indexSelector);
    }
    return selectivity;
  }

  @Override
  public String toString()
  {
    return StringUtils.format("(%s)", AND_JOINER.join(filters));
  }

  private ValueMatcher makeMatcher(final ValueMatcher[] baseMatchers)
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
          if (!matcher.matches()) {
            return false;
          }
        }
        return true;
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


}
