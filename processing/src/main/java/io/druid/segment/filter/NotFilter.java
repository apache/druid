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

import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.Filter;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.filter.ValueMatcherFactory;
import it.uniroma3.mat.extendedset.intset.ImmutableConciseSet;
import io.druid.segment.ColumnSelectorFactory;

/**
 */
public class NotFilter implements Filter
{
  private final Filter baseFilter;

  public NotFilter(
      Filter baseFilter
  )
  {
    this.baseFilter = baseFilter;
  }

  @Override
  public ImmutableConciseSet goConcise(BitmapIndexSelector selector)
  {
    return ImmutableConciseSet.complement(
        baseFilter.goConcise(selector),
        selector.getNumRows()
    );
  }

  @Override
  public ValueMatcher makeMatcher(ValueMatcherFactory factory)
  {
    final ValueMatcher baseMatcher = baseFilter.makeMatcher(factory);

    return new ValueMatcher()
    {
      @Override
      public boolean matches()
      {
        return !baseMatcher.matches();
      }
    };
  }

  @Override
  public ValueMatcher makeMatcher(ColumnSelectorFactory factory)
  {
    final ValueMatcher baseMatcher = baseFilter.makeMatcher(factory);

    return new ValueMatcher()
    {
      @Override
      public boolean matches()
      {
        return !baseMatcher.matches();
      }
    };
  }
}
