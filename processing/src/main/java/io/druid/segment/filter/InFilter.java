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
import com.google.common.base.Supplier;
import io.druid.collections.bitmap.ImmutableBitmap;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.DruidFloatPredicate;
import io.druid.query.filter.DruidLongPredicate;
import io.druid.query.filter.DruidPredicateFactory;
import io.druid.query.filter.Filter;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.ColumnSelector;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.IntIteratorUtils;
import io.druid.segment.column.BitmapIndex;
import it.unimi.dsi.fastutil.ints.AbstractIntIterator;
import it.unimi.dsi.fastutil.ints.IntIterable;
import it.unimi.dsi.fastutil.ints.IntIterator;

import java.util.Iterator;
import java.util.Set;

/**
 */
public class InFilter implements Filter
{
  private final String dimension;
  private final Set<String> values;
  private final ExtractionFn extractionFn;
  private final Supplier<DruidLongPredicate> longPredicateSupplier;
  private final Supplier<DruidFloatPredicate> floatPredicateSupplier;

  public InFilter(
      String dimension,
      Set<String> values,
      Supplier<DruidLongPredicate> longPredicateSupplier,
      Supplier<DruidFloatPredicate> floatPredicateSupplier,
      ExtractionFn extractionFn
  )
  {
    this.dimension = dimension;
    this.values = values;
    this.extractionFn = extractionFn;
    this.longPredicateSupplier = longPredicateSupplier;
    this.floatPredicateSupplier = floatPredicateSupplier;
  }

  @Override
  public ImmutableBitmap getBitmapIndex(final BitmapIndexSelector selector)
  {
    if (extractionFn == null) {
      final BitmapIndex bitmapIndex = selector.getBitmapIndex(dimension);
      return selector.getBitmapFactory().union(getBitmapIterable(bitmapIndex));
    } else {
      return Filters.matchPredicate(
          dimension,
          selector,
          getPredicateFactory().makeStringPredicate()
      );
    }
  }

  @Override
  public double estimateSelectivity(BitmapIndexSelector indexSelector)
  {
    if (extractionFn == null) {
      final BitmapIndex bitmapIndex = indexSelector.getBitmapIndex(dimension);
      return Filters.estimateSelectivity(
          bitmapIndex,
          IntIteratorUtils.toIntList(getBitmapIndexIterable(bitmapIndex).iterator()),
          indexSelector.getNumRows()
      );
    } else {
      return Filters.estimateSelectivity(
          dimension,
          indexSelector,
          getPredicateFactory().makeStringPredicate()
      );
    }
  }

  private Iterable<ImmutableBitmap> getBitmapIterable(final BitmapIndex bitmapIndex)
  {
    return Filters.bitmapsFromIndexes(getBitmapIndexIterable(bitmapIndex), bitmapIndex);
  }

  private IntIterable getBitmapIndexIterable(final BitmapIndex bitmapIndex)
  {
    return new IntIterable()
    {
      @Override
      public IntIterator iterator()
      {
        return new AbstractIntIterator()
        {
          Iterator<String> iterator = values.iterator();

          @Override
          public boolean hasNext()
          {
            return iterator.hasNext();
          }

          @Override
          public int nextInt()
          {
            return bitmapIndex.getIndex(iterator.next());
          }
        };
      }
    };
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

  @Override
  public boolean supportsSelectivityEstimation(
      ColumnSelector columnSelector, BitmapIndexSelector indexSelector
  )
  {
    return Filters.supportsSelectivityEstimation(this, dimension, columnSelector, indexSelector);
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

      @Override
      public DruidFloatPredicate makeFloatPredicate()
      {
        if (extractionFn != null) {
          return new DruidFloatPredicate()
          {
            @Override
            public boolean applyFloat(float input)
            {
              return values.contains(extractionFn.apply(input));
            }
          };
        } else {
          return floatPredicateSupplier.get();
        }
      }
    };
  }
}
