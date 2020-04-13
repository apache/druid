/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.segment.filter;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.spatial.search.Bound;
import org.apache.druid.query.BitmapResultFactory;
import org.apache.druid.query.filter.BitmapIndexSelector;
import org.apache.druid.query.filter.DruidDoublePredicate;
import org.apache.druid.query.filter.DruidFloatPredicate;
import org.apache.druid.query.filter.DruidLongPredicate;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.FilterTuning;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.segment.ColumnSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.incremental.SpatialDimensionRowTransformer;

import java.util.Set;

/**
 */
public class SpatialFilter implements Filter
{
  private final String dimension;
  private final Bound bound;
  private final FilterTuning filterTuning;

  public SpatialFilter(
      String dimension,
      Bound bound,
      FilterTuning filterTuning
  )
  {
    this.dimension = Preconditions.checkNotNull(dimension, "dimension");
    this.bound = Preconditions.checkNotNull(bound, "bound");
    this.filterTuning = filterTuning;
  }

  @Override
  public <T> T getBitmapResult(BitmapIndexSelector selector, BitmapResultFactory<T> bitmapResultFactory)
  {
    Iterable<ImmutableBitmap> search = selector.getSpatialIndex(dimension).search(bound);
    return bitmapResultFactory.unionDimensionValueBitmaps(search);
  }

  @Override
  public ValueMatcher makeMatcher(ColumnSelectorFactory factory)
  {
    return Filters.makeValueMatcher(
        factory,
        dimension,
        new DruidPredicateFactory()
        {
          @Override
          public Predicate<String> makeStringPredicate()
          {
            return input -> {
              if (input == null) {
                return false;
              }
              final float[] coordinate = SpatialDimensionRowTransformer.decode(input);
              return bound.contains(coordinate);
            };
          }

          @Override
          public DruidLongPredicate makeLongPredicate()
          {
            // SpatialFilter does not currently support longs
            return DruidLongPredicate.ALWAYS_FALSE;
          }

          @Override
          public DruidFloatPredicate makeFloatPredicate()
          {
            // SpatialFilter does not currently support floats
            return DruidFloatPredicate.ALWAYS_FALSE;
          }

          @Override
          public DruidDoublePredicate makeDoublePredicate()
          {
            // SpatialFilter does not currently support doubles
            return DruidDoublePredicate.ALWAYS_FALSE;
          }
        }
    );
  }

  @Override
  public boolean supportsBitmapIndex(BitmapIndexSelector selector)
  {
    return selector.getBitmapIndex(dimension) != null;
  }

  @Override
  public boolean shouldUseBitmapIndex(BitmapIndexSelector selector)
  {
    return Filters.shouldUseBitmapIndex(this, selector, filterTuning);
  }

  @Override
  public boolean supportsSelectivityEstimation(ColumnSelector columnSelector, BitmapIndexSelector indexSelector)
  {
    return false;
  }

  @Override
  public Set<String> getRequiredColumns()
  {
    return ImmutableSet.of(dimension);
  }

  @Override
  public double estimateSelectivity(BitmapIndexSelector indexSelector)
  {
    // selectivity estimation for multi-value columns is not implemented yet.
    throw new UnsupportedOperationException();
  }
}
