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
import com.google.common.collect.Lists;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.BooleanFilter;
import io.druid.query.filter.Filter;
import io.druid.query.filter.RowOffsetMatcherFactory;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.filter.ValueMatcherFactory;

import java.util.ArrayList;
import java.util.List;

/**
 */
public class AndFilter implements BooleanFilter
{
  private static final Joiner AND_JOINER = Joiner.on(" && ");

  private final List<Filter> filters;

  public AndFilter(
      List<Filter> filters
  )
  {
    this.filters = filters;
  }

  @Override
  public ImmutableBitmap getBitmapIndex(BitmapIndexSelector selector)
  {
    if (filters.size() == 1) {
      return filters.get(0).getBitmapIndex(selector);
    }

    List<ImmutableBitmap> bitmaps = Lists.newArrayList();
    for (int i = 0; i < filters.size(); i++) {
      bitmaps.add(filters.get(i).getBitmapIndex(selector));
    }

    return selector.getBitmapFactory().intersection(bitmaps);
  }

  @Override
  public ValueMatcher makeMatcher(ValueMatcherFactory factory)
  {
    if (filters.size() == 0) {
      return new BooleanValueMatcher(false);
    }

    final ValueMatcher[] matchers = new ValueMatcher[filters.size()];

    for (int i = 0; i < filters.size(); i++) {
      matchers[i] = filters.get(i).makeMatcher(factory);
    }
    return makeMatcher(matchers);
  }

  @Override
  public ValueMatcher makeMatcher(
      BitmapIndexSelector selector,
      ValueMatcherFactory valueMatcherFactory,
      RowOffsetMatcherFactory rowOffsetMatcherFactory
  )
  {
    final List<ValueMatcher> matchers = new ArrayList<>();
    final List<ImmutableBitmap> bitmaps = new ArrayList<>();

    for (Filter filter : filters) {
      if (filter.supportsBitmapIndex(selector)) {
        bitmaps.add(filter.getBitmapIndex(selector));
      } else {
        ValueMatcher matcher = filter.makeMatcher(valueMatcherFactory);
        matchers.add(matcher);
      }
    }

    if (bitmaps.size() > 0) {
      ImmutableBitmap combinedBitmap = selector.getBitmapFactory().intersection(bitmaps);
      ValueMatcher offsetMatcher = rowOffsetMatcherFactory.makeRowOffsetMatcher(combinedBitmap);
      matchers.add(0, offsetMatcher);
    }

    return new ValueMatcher()
    {
      @Override
      public boolean matches()
      {
        for (ValueMatcher valueMatcher : matchers) {
          if (!valueMatcher.matches()) {
            return false;
          }
        }
        return true;
      }
    };
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
  public String toString()
  {
    return String.format("(%s)", AND_JOINER.join(filters));
  }

  private ValueMatcher makeMatcher(final ValueMatcher[] baseMatchers)
  {
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
    };
  }


}
