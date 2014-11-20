/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.segment.filter;

import com.google.common.collect.Lists;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.Filter;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.filter.ValueMatcherFactory;
import io.druid.segment.ColumnSelectorFactory;

import java.util.List;

/**
 */
public class AndFilter implements Filter
{
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

  public ValueMatcher makeMatcher(ColumnSelectorFactory factory)
  {
    final ValueMatcher[] matchers = new ValueMatcher[filters.size()];

    for (int i = 0; i < filters.size(); i++) {
      matchers[i] = filters.get(i).makeMatcher(factory);
    }
    return makeMatcher(matchers);
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
