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

import com.google.common.base.Strings;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.Filter;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.filter.ValueMatcherFactory;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.DimensionSelector;
import io.druid.segment.data.IndexedInts;

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
  public ImmutableBitmap getBitmapIndex(BitmapIndexSelector selector)
  {
    return selector.getBitmapIndex(dimension, value);
  }

  @Override
  public ValueMatcher makeMatcher(ValueMatcherFactory factory)
  {
    return factory.makeValueMatcher(dimension, value);
  }

  @Override
  public ValueMatcher makeMatcher(ColumnSelectorFactory columnSelectorFactory)
  {
    final DimensionSelector dimensionSelector = columnSelectorFactory.makeDimensionSelector(
        new DefaultDimensionSpec(dimension, dimension)
    );

    // Missing columns match a null or empty string value and don't match anything else
    if (dimensionSelector == null) {
      return new BooleanValueMatcher(Strings.isNullOrEmpty(value));
    } else {
      final int valueId = dimensionSelector.lookupId(value);
      return new ValueMatcher()
      {
        @Override
        public boolean matches()
        {
          final IndexedInts row = dimensionSelector.getRow();
          final int size = row.size();
          for (int i = 0; i < size; ++i) {
            if (row.get(i) == valueId) {
              return true;
            }
          }
          return false;
        }
      };
    }
  }


}
