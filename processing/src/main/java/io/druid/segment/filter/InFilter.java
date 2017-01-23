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

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import io.druid.collections.bitmap.ImmutableBitmap;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.DruidLongPredicate;
import io.druid.query.filter.DruidPredicateFactory;
import io.druid.query.filter.Filter;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.ColumnSelector;
import io.druid.segment.ColumnSelectorFactory;

import java.util.Set;

/**
 */
public class InFilter implements Filter
{
  private final String dimension;
  private final Set<String> values;
  private final ExtractionFn extractionFn;
  private final Supplier<DruidLongPredicate> longPredicateSupplier;

  public InFilter(
      String dimension,
      Set<String> values,
      Supplier<DruidLongPredicate> longPredicateSupplier,
      ExtractionFn extractionFn
  )
  {
    this.dimension = dimension;
    this.values = values;
    this.extractionFn = extractionFn;
    this.longPredicateSupplier = longPredicateSupplier;
  }

  @Override
  public ImmutableBitmap getBitmapIndex(final BitmapIndexSelector selector)
  {
    if (extractionFn == null) {
      return selector.getBitmapFactory().union(getBitmapIterable(selector));
    } else {
      return Filters.matchPredicate(
          dimension,
          selector,
          getPredicateFactory().makeStringPredicate()
      );
    }
  }

  @Override
  public double estimateSelectivity(ColumnSelector columnSelector, BitmapIndexSelector indexSelector)
  {
    if (extractionFn == null) {
      return Filters.estimatePredicateSelectivity(
          columnSelector,
          dimension,
          getBitmapIterable(indexSelector),
          indexSelector.getNumRows()
      );
    } else {
      return Filters.estimatePredicateSelectivity(
          columnSelector,
          dimension,
          indexSelector,
          getPredicateFactory().makeStringPredicate()
      );
    }
  }

  private Iterable<ImmutableBitmap> getBitmapIterable(final BitmapIndexSelector selector)
  {
    return Iterables.transform(
        values, new Function<String, ImmutableBitmap>()
        {
          @Override
          public ImmutableBitmap apply(String value)
          {
            return selector.getBitmapIndex(dimension, value);
          }
        }
    );
  }

  @Override
  public ValueMatcher makeMatcher(ColumnSelectorFactory factory)
  {
    return Filters.makeValueMatcher(factory, dimension, getPredicateFactory());
  }

  @Override
  public boolean supportsBitmapIndex(BitmapIndexSelector selector)
  {
    return selector.getBitmapIndex(dimension) != null;
  }

  private DruidPredicateFactory getPredicateFactory()
  {
    return new DruidPredicateFactory()
    {
      @Override
      public Predicate<String> makeStringPredicate()
      {
        if (extractionFn != null) {
          return new Predicate<String>()
          {
            @Override
            public boolean apply(String input)
            {
              return values.contains(Strings.nullToEmpty(extractionFn.apply(input)));
            }
          };
        } else {
          return new Predicate<String>()
          {
            @Override
            public boolean apply(String input)
            {
              return values.contains(Strings.nullToEmpty(input));
            }
          };
        }
      }

      @Override
      public DruidLongPredicate makeLongPredicate()
      {
        if (extractionFn != null) {
          return new DruidLongPredicate()
          {
            @Override
            public boolean applyLong(long input)
            {
              return values.contains(extractionFn.apply(input));
            }
          };
        } else {
          return longPredicateSupplier.get();
        }
      }
    };
  }
}
