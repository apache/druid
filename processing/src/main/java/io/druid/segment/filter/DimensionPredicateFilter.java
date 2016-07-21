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
import com.google.common.base.Predicate;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.DruidLongPredicate;
import io.druid.query.filter.DruidPredicateFactory;
import io.druid.query.filter.Filter;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.filter.ValueMatcherFactory;

/**
 */
public class DimensionPredicateFilter implements Filter
{
  private final String dimension;
  private final DruidPredicateFactory predicateFactory;
  private final String basePredicateString;
  private final ExtractionFn extractionFn;

  public DimensionPredicateFilter(
      final String dimension,
      final DruidPredicateFactory predicateFactory,
      final ExtractionFn extractionFn
  )
  {
    Preconditions.checkNotNull(predicateFactory, "predicateFactory");
    this.dimension = Preconditions.checkNotNull(dimension, "dimension");
    this.basePredicateString = predicateFactory.toString();
    this.extractionFn = extractionFn;

    if (extractionFn == null) {
      this.predicateFactory = predicateFactory;
    } else {
      this.predicateFactory = new DruidPredicateFactory()
      {
        final Predicate<String> baseStringPredicate = predicateFactory.makeStringPredicate();

        @Override
        public Predicate<String> makeStringPredicate()
        {
          return new Predicate<String>()
          {
            @Override
            public boolean apply(String input)
            {
              return baseStringPredicate.apply(extractionFn.apply(input));
            }
          };
        }

        @Override
        public DruidLongPredicate makeLongPredicate()
        {
          return new DruidLongPredicate()
          {
            @Override
            public boolean applyLong(long input)
            {
              return baseStringPredicate.apply(extractionFn.apply(input));
            }
          };
        }
      };
    }
  }

  @Override
  public ImmutableBitmap getBitmapIndex(final BitmapIndexSelector selector)
  {
    return Filters.matchPredicate(dimension, selector, predicateFactory.makeStringPredicate());
  }

  @Override
  public ValueMatcher makeMatcher(ValueMatcherFactory factory)
  {
    return factory.makeValueMatcher(dimension, predicateFactory);
  }

  @Override
  public boolean supportsBitmapIndex(BitmapIndexSelector selector)
  {
    return selector.getBitmapIndex(dimension) != null;
  }

  @Override
  public String toString()
  {
    if (extractionFn != null) {
      return String.format("%s(%s) = %s", extractionFn, dimension, basePredicateString);
    } else {
      return String.format("%s = %s", dimension, basePredicateString);
    }
  }
}
