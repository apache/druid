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
import io.druid.query.extraction.DimExtractionFn;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.Filter;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.filter.ValueMatcherFactory;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.data.Indexed;
import it.uniroma3.mat.extendedset.intset.ImmutableConciseSet;

import java.util.List;

/**
 */
public class ExtractionFilter implements Filter
{
  private static final int MAX_SIZE = 50000;

  private final String dimension;
  private final String value;
  private final DimExtractionFn fn;

  public ExtractionFilter(
      String dimension,
      String value,
      DimExtractionFn fn
  )
  {
    this.dimension = dimension;
    this.value = value;
    this.fn = fn;
  }

  private List<Filter> makeFilters(BitmapIndexSelector selector)
  {
    final Indexed<String> allDimVals = selector.getDimensionValues(dimension);
    final List<Filter> filters = Lists.newArrayList();

    for (int i = 0; i < allDimVals.size(); i++) {
      String dimVal = allDimVals.get(i);
      if (value.equals(fn.apply(dimVal))) {
        filters.add(new SelectorFilter(dimension, dimVal));
      }
    }

    return filters;
  }

  @Override
  public ImmutableConciseSet goConcise(BitmapIndexSelector selector)
  {
    return new OrFilter(makeFilters(selector)).goConcise(selector);
  }

  @Override
  public ValueMatcher makeMatcher(ValueMatcherFactory factory)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public ValueMatcher makeMatcher(ColumnSelectorFactory factory)
  {
    throw new UnsupportedOperationException();
  }

}
