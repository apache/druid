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
import com.google.common.collect.Iterables;
import com.google.common.primitives.Longs;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.DruidLongPredicate;
import io.druid.query.filter.DruidPredicateFactory;
import io.druid.query.filter.Filter;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.filter.ValueMatcherFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 */
public class InFilter implements Filter
{
  // determined through benchmark that binary search on long[] is faster than HashSet until ~16 elements
  // Hashing threshold is not applied to String for now, String still uses ImmutableSortedSet
  public static final int LONG_HASHING_THRESHOLD = 16;

  private final String dimension;
  private final Set<String> values;
  private final ExtractionFn extractionFn;

  private boolean useLongHash;
  private long[] longArray;
  private HashSet<Long> longHashSet;

  public InFilter(String dimension, Set<String> values, ExtractionFn extractionFn)
  {
    this.dimension = dimension;
    this.values = values;
    this.extractionFn = extractionFn;
    setLongValues();
  }

  @Override
  public ImmutableBitmap getBitmapIndex(final BitmapIndexSelector selector)
  {
    if (extractionFn == null) {
      return selector.getBitmapFactory().union(
          Iterables.transform(
              values, new Function<String, ImmutableBitmap>()
              {
                @Override
                public ImmutableBitmap apply(String value)
                {
                  return selector.getBitmapIndex(dimension, value);
                }
              }
          )
      );
    } else {
      return Filters.matchPredicate(
          dimension,
          selector,
          getPredicateFactory().makeStringPredicate()
      );
    }
  }

  @Override
  public ValueMatcher makeMatcher(ValueMatcherFactory factory)
  {
    return factory.makeValueMatcher(dimension, getPredicateFactory());
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
            public boolean apply(@Nullable String input)
            {
              return values.contains(Strings.nullToEmpty(extractionFn.apply(input)));
            }
          };
        } else {
          return new Predicate<String>()
          {
            @Override
            public boolean apply(@Nullable String input)
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
          if (useLongHash) {
            return new DruidLongPredicate()
            {
              @Override
              public boolean applyLong(long input)
              {
                return longHashSet.contains(input);
              }
            };
          } else {
            return new DruidLongPredicate()
            {
              @Override
              public boolean applyLong(long input)
              {
                return Arrays.binarySearch(longArray, input) >= 0;
              }
            };
          }
        }
      }
    };
  }

  private void setLongValues()
  {
    List<Long> longs = new ArrayList<>();
    for (String value : values) {
      Long longValue = Longs.tryParse(value);
      if (longValue != null) {
        longs.add(longValue);
      }
    }

    useLongHash = longs.size() > LONG_HASHING_THRESHOLD;
    if (useLongHash) {
      longHashSet = new HashSet<Long>(longs);
    } else {
      longArray = new long[longs.size()];
      for (int i = 0; i < longs.size(); i++) {
        longArray[i] = longs.get(i).longValue();
      }
      Arrays.sort(longArray);
    }
  }
}
