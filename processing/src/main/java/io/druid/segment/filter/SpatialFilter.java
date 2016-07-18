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
import com.metamx.collections.spatial.search.Bound;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.DruidLongPredicate;
import io.druid.query.filter.DruidPredicateFactory;
import io.druid.query.filter.Filter;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.filter.ValueMatcherFactory;
import io.druid.segment.incremental.SpatialDimensionRowTransformer;

/**
 */
public class SpatialFilter implements Filter
{
  private final String dimension;
  private final Bound bound;

  public SpatialFilter(
      String dimension,
      Bound bound
  )
  {
    this.dimension = Preconditions.checkNotNull(dimension, "dimension");
    this.bound = Preconditions.checkNotNull(bound, "bound");
  }

  @Override
  public ImmutableBitmap getBitmapIndex(final BitmapIndexSelector selector)
  {
    Iterable<ImmutableBitmap> search = selector.getSpatialIndex(dimension).search(bound);
    return selector.getBitmapFactory().union(search);
  }

  @Override
  public ValueMatcher makeMatcher(ValueMatcherFactory factory)
  {
    return factory.makeValueMatcher(
        dimension,
        new DruidPredicateFactory()
        {
          @Override
          public Predicate<String> makeStringPredicate()
          {
            return new Predicate<String>()
            {
              @Override
              public boolean apply(String input)
              {
                if (input == null) {
                  return false;
                }
                final float[] coordinate = SpatialDimensionRowTransformer.decode(input);
                return bound.contains(coordinate);
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
                // SpatialFilter does not currently support longs
                return false;
              }
            };
          }
        }
    );
  }

  @Override
  public boolean supportsBitmapIndex(BitmapIndexSelector selector)
  {
    return selector.getBitmapIndex(dimension) != null;
  }
}
