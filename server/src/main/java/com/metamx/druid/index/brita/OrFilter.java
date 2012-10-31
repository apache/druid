/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.index.brita;

import it.uniroma3.mat.extendedset.intset.ImmutableConciseSet;

import java.util.List;

import com.google.common.collect.Lists;

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
  public ImmutableConciseSet goConcise(InvertedIndexSelector selector)
  {
    if (filters.size() == 1) {
      return filters.get(0).goConcise(selector);
    }

    List<ImmutableConciseSet> conciseSets = Lists.newArrayList();
    for (int i = 0; i < filters.size(); i++) {
      conciseSets.add(filters.get(i).goConcise(selector));
    }

    return ImmutableConciseSet.union(conciseSets);
  }

  @Override
  public ValueMatcher makeMatcher(ValueMatcherFactory factory)
  {
    final ValueMatcher[] matchers = new ValueMatcher[filters.size()];

    for (int i = 0; i < filters.size(); i++) {
      matchers[i] = filters.get(i).makeMatcher(factory);
    }

    if (matchers.length == 1) {
      return matchers[0];
    }

    return new ValueMatcher()
    {
      @Override
      public boolean matches()
      {
        for (ValueMatcher matcher : matchers) {
          if (matcher.matches()) {
            return true;
          }
        }
        return false;
      }
    };
  }
}
