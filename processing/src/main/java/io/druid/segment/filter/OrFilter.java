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

import com.google.common.collect.Lists;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.Filter;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.filter.ValueMatcherFactory;

import java.util.List;

/**
 */
public class OrFilter implements Filter
{
  private final List<Filter> filters;

  public OrFilter(
      List<Filter> filters
  )
  {
    if (filters.size() == 0) {
      throw new IllegalArgumentException("Can't construct empty OrFilter (the universe does not exist)");
    }

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

    return selector.getBitmapFactory().union(bitmaps);
  }

  @Override
  public ValueMatcher makeMatcher(ValueMatcherFactory factory)
  {
    final ValueMatcher[] matchers = new ValueMatcher[filters.size()];

    for (int i = 0; i < filters.size(); i++) {
      matchers[i] = filters.get(i).makeMatcher(factory);
    }
    return makeMatcher(matchers);
  }

  private ValueMatcher makeMatcher(final ValueMatcher[] baseMatchers){
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
    };
  }

}
