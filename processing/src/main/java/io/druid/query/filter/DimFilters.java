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

package io.druid.query.filter;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.List;

/**
 */
public class DimFilters
{
  public static SelectorDimFilter dimEquals(String dimension, String value)
  {
    return new SelectorDimFilter(dimension, value, null);
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
    return new RegexDimFilter(dimension, pattern, null);
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

  public static List<DimFilter> optimize(List<DimFilter> filters)
  {
    return filterNulls(
        Lists.transform(
            filters, new Function<DimFilter, DimFilter>()
            {
              @Override
              public DimFilter apply(DimFilter input)
              {
                return input.optimize();
              }
            }
        )
    );
  }

  public static List<DimFilter> filterNulls(List<DimFilter> optimized)
  {
    return Lists.newArrayList(Iterables.filter(optimized, Predicates.notNull()));
  }
}
