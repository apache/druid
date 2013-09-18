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

package io.druid.query.filter;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.List;

/**
 */
public class DimFilters
{
  public static SelectorDimFilter dimEquals(String dimension, String value)
  {
    return new SelectorDimFilter(dimension, value);
  }

  public static AndDimFilter and(DimFilter... filters)
  {
    return and(Arrays.asList(filters));
  }

  public static AndDimFilter and(List<DimFilter> filters)
  {
    return new AndDimFilter(filters);
  }

  public static OrDimFilter or(DimFilter... filters)
  {
    return or(Arrays.asList(filters));
  }

  public static OrDimFilter or(List<DimFilter> filters)
  {
    return new OrDimFilter(filters);
  }

  public static NotDimFilter not(DimFilter filter)
  {
    return new NotDimFilter(filter);
  }

  public static RegexDimFilter regex(String dimension, String pattern)
  {
    return new RegexDimFilter(dimension, pattern);
  }

  public static DimFilter dimEquals(final String dimension, String... values)
  {
    return or(
        Lists.transform(
            Arrays.asList(values),
            new Function<String, DimFilter>()
            {
              @Override
              public DimFilter apply(String input)
              {
                return dimEquals(dimension, input);
              }
            }
        )
    );
  }
}
