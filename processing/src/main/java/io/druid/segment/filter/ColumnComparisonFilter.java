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

import com.google.common.base.Preconditions;
import io.druid.query.BitmapResultFactory;
import io.druid.query.ColumnSelectorPlus;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.Filter;
import io.druid.query.filter.ValueGetter;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.filter.ValueMatcherColumnSelectorStrategy;
import io.druid.query.filter.ValueMatcherColumnSelectorStrategyFactory;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.ColumnSelector;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.DimensionHandlerUtils;

import java.util.List;
import java.util.Objects;

/**
 */
public class ColumnComparisonFilter implements Filter
{
  private final List<DimensionSpec> dimensions;

  public ColumnComparisonFilter(
      final List<DimensionSpec> dimensions
  )
  {
    this.dimensions = Preconditions.checkNotNull(dimensions, "dimensions");
  }

  @Override
  public <T> T getBitmapResult(BitmapIndexSelector selector, BitmapResultFactory<T> bitmapResultFactory)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public ValueMatcher makeMatcher(ColumnSelectorFactory factory)
  {
    final ValueGetter[] valueGetters = new ValueGetter[dimensions.size()];

    for (int i = 0; i < dimensions.size(); i++) {
      final ColumnSelectorPlus<ValueMatcherColumnSelectorStrategy> selector =
          DimensionHandlerUtils.createColumnSelectorPlus(
              ValueMatcherColumnSelectorStrategyFactory.instance(),
              dimensions.get(i),
              factory
          );

      valueGetters[i] = selector.getColumnSelectorStrategy().makeValueGetter(selector.getSelector());
    }

    return makeValueMatcher(valueGetters);
  }

  public static ValueMatcher makeValueMatcher(final ValueGetter[] valueGetters)
  {
    if (valueGetters.length == 0) {
      return BooleanValueMatcher.of(true);
    }
    return new ValueMatcher()
    {
      @Override
      public boolean matches()
      {
        // Keep all values to compare against each other.
        String[][] values = new String[valueGetters.length][];

        for (int i = 0; i < valueGetters.length; i++) {
          values[i] = valueGetters[i].get();
          // Compare the new values to the values we already got.
          for (int j = 0; j < i; j++) {
            if (!overlap(values[i], values[j])) {
              return false;
            }
          }
        }
        return true;
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        // All value getters are likely the same or similar (in terms of runtime shape), so inspecting only one of them.
        inspector.visit("oneValueGetter", valueGetters[0]);
      }
    };
  }

  // overlap returns true when: a and b have one or more elements in common,
  // a and b are both null, or a and b are both empty.
  public static boolean overlap(String[] a, String[] b)
  {
    if (a == null || b == null) {
      // They only have overlap if both are null.
      return a == null && b == null;
    }
    if (a.length == 0 && b.length == 0) {
      return true;
    }

    for (int i = 0; i < a.length; i++) {
      for (int j = 0; j < b.length; j++) {
        if (Objects.equals(a[i], b[j])) {
          return true;
        }
      }
    }

    return false;
  }

  @Override
  public boolean supportsBitmapIndex(BitmapIndexSelector selector)
  {
    return false;
  }

  @Override
  public boolean supportsSelectivityEstimation(
      ColumnSelector columnSelector, BitmapIndexSelector indexSelector
  )
  {
    return false;
  }

  @Override
  public double estimateSelectivity(BitmapIndexSelector indexSelector)
  {
    throw new UnsupportedOperationException();
  }
}
