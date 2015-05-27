/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.segment.filter;

import com.google.common.collect.Lists;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.bitmap.WrappedImmutableConciseBitmap;
import io.druid.query.extraction.ExtractionFn;
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
  private final String dimension;
  private final String value;
  private final ExtractionFn fn;

  public ExtractionFilter(
      String dimension,
      String value,
      ExtractionFn fn
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
    if (allDimVals != null) {
      for (int i = 0; i < allDimVals.size(); i++) {
        String dimVal = allDimVals.get(i);
        if (value.equals(fn.apply(dimVal))) {
          filters.add(new SelectorFilter(dimension, dimVal));
        }
      }
    }

    return filters;
  }

  @Override
  public ImmutableBitmap getBitmapIndex(BitmapIndexSelector selector)
  {
    final List<Filter> filters = makeFilters(selector);
    if (filters.isEmpty()) {
      return selector.getBitmapFactory().makeEmptyImmutableBitmap();
    }
    return new OrFilter(makeFilters(selector)).getBitmapIndex(selector);
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
