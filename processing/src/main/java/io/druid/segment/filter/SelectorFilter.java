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

/**
 */
public class SelectorFilter implements Filter
{
  private final String dimension;
  private final String value;

  public SelectorFilter(
      String dimension,
      String value
  )
  {
    this.dimension = dimension;
    this.value = value;
  }

  @Override
  public ImmutableConciseSet goConcise(BitmapIndexSelector selector)
  {
    return selector.getConciseInvertedIndex(dimension, value);
  }

  @Override
  public ValueMatcher makeMatcher(ValueMatcherFactory factory)
  {
    return factory.makeValueMatcher(dimension, value);
  }
}
