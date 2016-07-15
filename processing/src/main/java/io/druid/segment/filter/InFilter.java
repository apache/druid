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
import io.druid.query.filter.DruidCompositePredicate;
import io.druid.query.filter.Filter;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.filter.ValueMatcherFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 */
public class InFilter implements Filter
{
  // determined through benchmark that binary search on long[] is faster than HashSet until ~16 elements
  // Hashing threshold is not applied to String for now, String still uses ImmutableSortedSet
  public static final int HASHING_THRESHOLD = 16;

  private final String dimension;
  private final Set<String> values;
  private final ExtractionFn extractionFn;

  private final boolean useHash;
  private final long[] longArray;
  private final HashSet<Long> longHashSet;

  public InFilter(String dimension, Set<String> values, ExtractionFn extractionFn)
  {
    this.dimension = dimension;
    this.values = values;
    this.extractionFn = extractionFn;
    this.useHash = values.size() > HASHING_THRESHOLD;
    this.longHashSet = useHash ? new HashSet<Long>() : null;
    this.longArray = useHash ? null : new long[values.size()];
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
          new Predicate<Object>()
          {
            @Override
            public boolean apply(Object inputObj)
            {
              // InDimFilter converts all null "values" to empty.
              String input = inputObj == null ? null : inputObj.toString();
              return values.contains(Strings.nullToEmpty(extractionFn.apply(input)));
            }
          }
      );
    }
  }

  @Override
  public ValueMatcher makeMatcher(ValueMatcherFactory factory)
  {
    return factory.makeValueMatcher(dimension, getPredicate());
  }

  @Override
  public boolean supportsBitmapIndex(BitmapIndexSelector selector)
  {
    return selector.getBitmapIndex(dimension) != null;
  }

  private DruidCompositePredicate getPredicate()
  {
    if (extractionFn == null) {
      if (useHash) {
        return new DruidCompositePredicate()
        {
          @Override
          public boolean apply(Object inputObj)
          {
            String input = inputObj == null ? null : inputObj.toString();
            return values.contains(Strings.nullToEmpty(input));
          }

          @Override
          public boolean applyLong(long value)
          {
            return longHashSet.contains(value);
          }
        };
      } else {
        return new DruidCompositePredicate()
        {
          @Override
          public boolean apply(Object inputObj)
          {
            String input = inputObj == null ? null : inputObj.toString();
            return values.contains(Strings.nullToEmpty(input));
          }

          @Override
          public boolean applyLong(long value)
          {
            return Arrays.binarySearch(longArray, value) >= 0;
          }
        };
      }
    } else {
      return new DruidCompositePredicate()
      {
        @Override
        public boolean apply(Object inputObj)
        {
          return values.contains(Strings.nullToEmpty(extractionFn.apply(inputObj)));
        }

        @Override
        public boolean applyLong(long value)
        {
          return values.contains(extractionFn.apply(value));
        }
      };
    }
  }

  private void setLongValues()
  {
    int idx = 0;
    for (String value : values) {
      Long longValue = Longs.tryParse(value);
      if (longValue != null) {
        if (useHash) {
          longHashSet.add(longValue);
        } else {
          longArray[idx] = longValue;
        }
        idx++;
      }
    }
    if (!useHash) {
      Arrays.sort(longArray);
    }
  }
}
