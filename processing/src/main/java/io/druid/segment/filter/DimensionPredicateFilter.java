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
import io.druid.query.filter.Filter;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.filter.ValueMatcherFactory;

/**
 */
public class DimensionPredicateFilter implements Filter
{
  private final String dimension;
  private final Predicate<String> predicate;

  public DimensionPredicateFilter(
      final String dimension,
      final Predicate<String> predicate,
      final ExtractionFn extractionFn
  )
  {
    Preconditions.checkNotNull(predicate, "predicate");
    this.dimension = Preconditions.checkNotNull(dimension, "dimension");

    if (extractionFn == null) {
      this.predicate = predicate;
    } else {
      this.predicate = new Predicate<String>()
      {
        @Override
        public boolean apply(String input)
        {
          return predicate.apply(extractionFn.apply(input));
        }
      };
    }
  }

  @Override
  public ImmutableBitmap getBitmapIndex(final BitmapIndexSelector selector)
  {
    return Filters.matchPredicate(dimension, selector, predicate);
  }

  @Override
  public ValueMatcher makeMatcher(ValueMatcherFactory factory)
  {
    return factory.makeValueMatcher(dimension, predicate);
  }
}
