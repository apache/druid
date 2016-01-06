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

import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.Filter;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.filter.ValueMatcherFactory;

import java.util.List;

/**
 */
public class SelectorFilter extends Filter.AbstractFilter
{
  private final String dimension;
  private final String value;
  private final ExtractionFn extractionFn;

  public SelectorFilter(
      String dimension,
      String value,
      ExtractionFn extractionFn
  )
  {
    this.dimension = dimension;
    this.value = Strings.nullToEmpty(value);
    this.extractionFn = extractionFn;
  }

  @Override
  public ImmutableBitmap getBitmapIndex(BitmapIndexSelector selector)
  {
    if (extractionFn == null) {
      return selector.getBitmapIndex(dimension, value);
    } else {
      final List<Filter> filters = makeFiltersUsingExtractionFn(selector);
      if (filters.isEmpty()) {
        return selector.getBitmapFactory().makeEmptyImmutableBitmap();
      }
      return new OrFilter(filters).getBitmapIndex(selector);
    }
  }

  @Override
  public ValueMatcher makeMatcher(ValueMatcherFactory factory)
  {
    if (extractionFn == null) {
      return factory.makeValueMatcher(dimension, value);
    } else {
      return factory.makeValueMatcher(
          dimension, new Predicate<String>()
          {
            @Override
            public boolean apply(String input)
            {
              // Assuming that a null/absent/empty dimension are equivalent from the druid perspective
              return value.equals(Strings.nullToEmpty(extractionFn.apply(input)));
            }
          }
      );
    }
  }

  private List<Filter> makeFiltersUsingExtractionFn(BitmapIndexSelector selector)
  {
    final List<Filter> filters = Lists.newArrayList();

    Iterable<String> allDimVals = selector.getDimensionValues(dimension);
    if (allDimVals == null) {
      allDimVals = Lists.newArrayList((String) null);
    }

    for (String dimVal : allDimVals) {
      if (value.equals(Strings.nullToEmpty(extractionFn.apply(dimVal)))) {
        filters.add(new SelectorFilter(dimension, dimVal, null));
      }
    }

    return filters;
  }

  @Override
  public String toString()
  {
    return "SelectorFilter{" +
           "dimension='" + dimension + '\'' +
           ", value='" + value + '\'' +
           '}';
  }
}
