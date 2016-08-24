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
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Longs;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.common.guava.FunctionalIterable;
import io.druid.math.expr.Expressions;
import io.druid.query.Query;
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.DruidLongPredicate;
import io.druid.query.filter.Filter;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.column.BitmapIndex;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.Indexed;

import java.util.Iterator;
import java.util.List;

/**
 */
public class Filters
{
  public static final List<ValueType> FILTERABLE_TYPES = ImmutableList.of(ValueType.STRING, ValueType.LONG);
  private static final String CTX_KEY_USE_FILTER_CNF = "useFilterCNF";

  /**
   * Convert a list of DimFilters to a list of Filters.
   *
   * @param dimFilters list of DimFilters, should all be non-null
   *
   * @return list of Filters
   */
  public static List<Filter> toFilters(List<DimFilter> dimFilters)
  {
    return ImmutableList.copyOf(
        FunctionalIterable
            .create(dimFilters)
            .transform(
                new Function<DimFilter, Filter>()
                {
                  @Override
                  public Filter apply(DimFilter input)
                  {
                    return input.toFilter();
                  }
                }
            )
    );
  }

  /**
   * Convert a DimFilter to a Filter.
   *
   * @param dimFilter dimFilter
   *
   * @return converted filter, or null if input was null
   */
  public static Filter toFilter(DimFilter dimFilter)
  {
    return dimFilter == null ? null : dimFilter.toFilter();
  }

  /**
   * Return the union of bitmaps for all values matching a particular predicate.
   *
   * @param dimension dimension to look at
   * @param selector  bitmap selector
   * @param predicate predicate to use
   *
   * @return bitmap of matching rows
   */
  public static ImmutableBitmap matchPredicate(
      final String dimension,
      final BitmapIndexSelector selector,
      final Predicate<String> predicate
  )
  {
    Preconditions.checkNotNull(dimension, "dimension");
    Preconditions.checkNotNull(selector, "selector");
    Preconditions.checkNotNull(predicate, "predicate");

    // Missing dimension -> match all rows if the predicate matches null; match no rows otherwise
    final Indexed<String> dimValues = selector.getDimensionValues(dimension);
    if (dimValues == null || dimValues.size() == 0) {
      if (predicate.apply(null)) {
        return selector.getBitmapFactory().complement(
            selector.getBitmapFactory().makeEmptyImmutableBitmap(),
            selector.getNumRows()
        );
      } else {
        return selector.getBitmapFactory().makeEmptyImmutableBitmap();
      }
    }

    // Apply predicate to all dimension values and union the matching bitmaps
    final BitmapIndex bitmapIndex = selector.getBitmapIndex(dimension);
    return selector.getBitmapFactory().union(
        new Iterable<ImmutableBitmap>()
        {
          @Override
          public Iterator<ImmutableBitmap> iterator()
          {
            return new Iterator<ImmutableBitmap>()
            {
              int currIndex = 0;

              @Override
              public boolean hasNext()
              {
                return currIndex < bitmapIndex.getCardinality();
              }

              @Override
              public ImmutableBitmap next()
              {
                while (currIndex < bitmapIndex.getCardinality() && !predicate.apply(dimValues.get(currIndex))) {
                  currIndex++;
                }

                if (currIndex == bitmapIndex.getCardinality()) {
                  return bitmapIndex.getBitmapFactory().makeEmptyImmutableBitmap();
                }

                return bitmapIndex.getBitmap(currIndex++);
              }

              @Override
              public void remove()
              {
                throw new UnsupportedOperationException();
              }
            };
          }
        }
    );
  }

  public static ValueMatcher getLongValueMatcher(
      final LongColumnSelector longSelector,
      Comparable value
  )
  {
    if (value == null) {
      return new BooleanValueMatcher(false);
    }

    final Long longValue = Longs.tryParse(value.toString());
    if (longValue == null) {
      return new BooleanValueMatcher(false);
    }

    return new ValueMatcher()
    {
      // store the primitive, so we don't unbox for every comparison
      final long unboxedLong = longValue.longValue();

      @Override
      public boolean matches()
      {
        return longSelector.get() == unboxedLong;
      }
    };
  }

  public static ValueMatcher getLongPredicateMatcher(
      final LongColumnSelector longSelector,
      final DruidLongPredicate predicate
  )
  {
    return new ValueMatcher()
    {
      @Override
      public boolean matches()
      {
        return predicate.applyLong(longSelector.get());
      }
    };
  }

  public static DimFilter convertToCNFFromQueryContext(Query query, DimFilter expression)
  {
    if (expression == null) {
      return null;
    }
    boolean useCNF = query.getContextBoolean(CTX_KEY_USE_FILTER_CNF, false);
    return useCNF ? convertToCNF(expression.optimize()) : expression.optimize();
  }

  public static DimFilter convertToCNF(DimFilter current)
  {
    return Expressions.convertToCNF(current, new DimFilter.Factory());
  }

  public static Filter convertToCNF(Filter current)
  {
    return Expressions.convertToCNF(current, new Filter.Factory());
  }

  public static DimFilter bind(List<DimFilter> postFilters)
  {
    if (postFilters.size() == 0) {
      return null;
    } else if (postFilters.size() == 1) {
      return postFilters.get(0);
    } else {
      return new AndDimFilter(postFilters);
    }
  }
}
