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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.spatial.search.Bound;
import org.apache.druid.query.BitmapResultFactory;
import org.apache.druid.query.filter.ColumnIndexSelector;
import org.apache.druid.query.filter.DruidDoublePredicate;
import org.apache.druid.query.filter.DruidFloatPredicate;
import org.apache.druid.query.filter.DruidLongPredicate;
import org.apache.druid.query.filter.DruidObjectPredicate;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.query.filter.DruidPredicateMatch;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.FilterTuning;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.ColumnIndexCapabilities;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.SimpleColumnIndexCapabilities;
import org.apache.druid.segment.incremental.SpatialDimensionRowTransformer;
import org.apache.druid.segment.index.AllUnknownBitmapColumnIndex;
import org.apache.druid.segment.index.BitmapColumnIndex;
import org.apache.druid.segment.index.semantic.SpatialIndex;

import javax.annotation.Nullable;
import java.util.Objects;
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

  @Nullable
  @Override
  public BitmapColumnIndex getBitmapColumnIndex(ColumnIndexSelector selector)
  {
    if (!Filters.checkFilterTuningUseIndex(dimension, selector, filterTuning)) {
      return null;
    }
    final ColumnIndexSupplier indexSupplier = selector.getIndexSupplier(dimension);
    if (indexSupplier == null) {
      return new AllUnknownBitmapColumnIndex(selector);
    }
    final SpatialIndex spatialIndex = indexSupplier.as(SpatialIndex.class);
    if (spatialIndex == null) {
      return null;
    }
    return new BitmapColumnIndex()
    {
      @Override
      public ColumnIndexCapabilities getIndexCapabilities()
      {
        return new SimpleColumnIndexCapabilities(true, true);
      }

      @Override
      public <T> T computeBitmapResult(BitmapResultFactory<T> bitmapResultFactory, boolean includeUnknown)
      {
        Iterable<ImmutableBitmap> search = spatialIndex.getRTree().search(bound);
        return bitmapResultFactory.unionDimensionValueBitmaps(search);
      }
    };
  }

  @Override
  public ValueMatcher makeMatcher(ColumnSelectorFactory factory)
  {
    return Filters.makeValueMatcher(
        factory,
        dimension,
        new BoundDruidPredicateFactory(bound)

    );
  }

  @Override
  public Set<String> getRequiredColumns()
  {
    return ImmutableSet.of(dimension);
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SpatialFilter that = (SpatialFilter) o;
    return Objects.equals(dimension, that.dimension) &&
           Objects.equals(bound, that.bound) &&
           Objects.equals(filterTuning, that.filterTuning);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dimension, bound, filterTuning);
  }

  @VisibleForTesting
  static class BoundDruidPredicateFactory implements DruidPredicateFactory
  {
    private final Bound bound;

    BoundDruidPredicateFactory(Bound bound)
    {
      this.bound = bound;
    }

    @Override
    public DruidObjectPredicate<String> makeStringPredicate()
    {
      return input -> {
        if (input == null) {
          return DruidPredicateMatch.UNKNOWN;
        }
        final float[] coordinate = SpatialDimensionRowTransformer.decode(input);
        return DruidPredicateMatch.of(bound.contains(coordinate));
      };
    }

    @Override
    public DruidLongPredicate makeLongPredicate()
    {
      // SpatialFilter does not currently support longs
      return DruidLongPredicate.ALWAYS_FALSE_WITH_NULL_UNKNOWN;
    }

    @Override
    public DruidFloatPredicate makeFloatPredicate()
    {
      // SpatialFilter does not currently support floats
      return DruidFloatPredicate.ALWAYS_FALSE_WITH_NULL_UNKNOWN;
    }

    @Override
    public DruidDoublePredicate makeDoublePredicate()
    {
      // SpatialFilter does not currently support doubles
      return DruidDoublePredicate.ALWAYS_FALSE_WITH_NULL_UNKNOWN;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      BoundDruidPredicateFactory that = (BoundDruidPredicateFactory) o;
      return Objects.equals(bound, that.bound);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(bound);
    }
  }
}
