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

import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.DimensionSelector;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.filter.BooleanValueMatcher;

/**
 */
public class SelectorFilterExtension implements Filter
{
  private final String dimension;
  private final String value;
  private final BinaryOperator operator;

  public SelectorFilterExtension(
      String dimension,
      String value,
      BinaryOperator operator
  )
  {
    this.dimension = dimension;
    this.value = value;
    this.operator = operator;
  }

  public SelectorFilterExtension(String dimension, String value)
  {
    this(dimension, value, null);
  }

  @Override
  public ImmutableBitmap getBitmapIndex(BitmapIndexSelector selector)
  {
    final BitmapFactory factory = selector.getBitmapFactory();
    final Indexed<String> values = selector.getDimensionValues(dimension);
    switch (operator) {
      case GT:
      case GTE: {
        int index = values.indexOf(value);
        int start = index < 0 ? -index - 1 : operator == BinaryOperator.GT ? index + 1: index;
        ImmutableBitmap bitmap = factory.makeEmptyImmutableBitmap();
        for (int cursor = start; cursor < values.size(); cursor++) {
          bitmap = bitmap.union(selector.getBitmapIndex(dimension, values.get(cursor)));
        }
        return bitmap;
      }
      case LT:
      case LTE: {
        int index = values.indexOf(value);
        int limit = index < 0 ? -index - 2 : operator == BinaryOperator.LT ? index - 1: index;
        ImmutableBitmap bitmap = factory.makeEmptyImmutableBitmap();
        for (int cursor = 0; cursor <= limit; cursor++) {
          bitmap = bitmap.union(selector.getBitmapIndex(dimension, values.get(cursor)));
        }
        return bitmap;
      }
      case EQ:
        return selector.getBitmapIndex(dimension, value);
      case NE:
        return factory.complement(selector.getBitmapIndex(dimension, value), selector.getNumRows());
    }
    throw new IllegalArgumentException("Not supported operator " + operator);
  }

  @Override
  public ValueMatcher makeMatcher(ValueMatcherFactory factory)
  {
    if (operator == BinaryOperator.EQ || operator == BinaryOperator.NE) {
      final ValueMatcher matcher = factory.makeValueMatcher(dimension, value);
      return operator == BinaryOperator.EQ ? matcher : new RevertedMatcher(matcher);
    }
    return factory.makeValueMatcher(dimension, operator.toPredicate(value));
  }

  @Override
  public ValueMatcher makeMatcher(ColumnSelectorFactory columnSelectorFactory)
  {
    final DimensionSelector dimensionSelector = columnSelectorFactory.makeDimensionSelector(
        new DefaultDimensionSpec(dimension, dimension)
    );

    if (operator == BinaryOperator.EQ || operator == BinaryOperator.NE) {
      final ValueMatcher matcher = toValueMatcher(dimensionSelector);
      return operator == BinaryOperator.EQ ? matcher : new RevertedMatcher(matcher);
    }

    final Predicate<String> predicate = operator.toPredicate(value);
    return new ValueMatcher()
    {
      @Override
      public boolean matches()
      {
        final IndexedInts row = dimensionSelector.getRow();
        final int size = row.size();
        for (int i = 0; i < size; ++i) {
          if (predicate.apply(dimensionSelector.lookupName(row.get(i)))) {
            return true;
          }
        }
        return false;
      }
    };
  }

  private ValueMatcher toValueMatcher(final DimensionSelector dimensionSelector)
  {
    if (dimensionSelector == null) {
      // Missing columns match a null or empty string value and don't match anything else
      return new BooleanValueMatcher(Strings.isNullOrEmpty(value));
    }
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

  // for NE
  private static class RevertedMatcher implements ValueMatcher
  {
    private final ValueMatcher matcher;

    private RevertedMatcher(ValueMatcher matcher) {this.matcher = matcher;}

    @Override
    public boolean matches()
    {
      return !matcher.matches();
    }
  }
}
