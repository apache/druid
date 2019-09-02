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

import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.query.BitmapResultFactory;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.filter.BitmapIndexSelector;
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.filter.DruidDoublePredicate;
import org.apache.druid.query.filter.DruidFloatPredicate;
import org.apache.druid.query.filter.DruidLongPredicate;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.FilterTuning;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.filter.vector.VectorValueMatcher;
import org.apache.druid.query.filter.vector.VectorValueMatcherColumnStrategizer;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.ColumnSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.IntListUtils;
import org.apache.druid.segment.column.BitmapIndex;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;

import java.util.Comparator;
import java.util.Set;

public class BoundFilter implements Filter
{
  private final BoundDimFilter boundDimFilter;
  private final Comparator<String> comparator;
  private final ExtractionFn extractionFn;
  private final FilterTuning filterTuning;

  private final Supplier<DruidLongPredicate> longPredicateSupplier;
  private final Supplier<DruidFloatPredicate> floatPredicateSupplier;
  private final Supplier<DruidDoublePredicate> doublePredicateSupplier;

  public BoundFilter(final BoundDimFilter boundDimFilter)
  {
    this.boundDimFilter = boundDimFilter;
    this.comparator = boundDimFilter.getOrdering();
    this.extractionFn = boundDimFilter.getExtractionFn();
    this.longPredicateSupplier = boundDimFilter.getLongPredicateSupplier();
    this.floatPredicateSupplier = boundDimFilter.getFloatPredicateSupplier();
    this.doublePredicateSupplier = boundDimFilter.getDoublePredicateSupplier();
    this.filterTuning = boundDimFilter.getFilterTuning();
  }

  @Override
  public <T> T getBitmapResult(BitmapIndexSelector selector, BitmapResultFactory<T> bitmapResultFactory)
  {
    if (supportShortCircuit()) {
      final BitmapIndex bitmapIndex = selector.getBitmapIndex(boundDimFilter.getDimension());

      if (bitmapIndex == null || bitmapIndex.getCardinality() == 0) {
        if (doesMatch(null)) {
          return bitmapResultFactory.wrapAllTrue(Filters.allTrue(selector));
        } else {
          return bitmapResultFactory.wrapAllFalse(Filters.allFalse(selector));
        }
      }

      return bitmapResultFactory.unionDimensionValueBitmaps(getBitmapIterator(boundDimFilter, bitmapIndex));
    } else {
      return Filters.matchPredicate(
          boundDimFilter.getDimension(),
          selector,
          bitmapResultFactory,
          getPredicateFactory().makeStringPredicate()
      );
    }
  }

  @Override
  public double estimateSelectivity(BitmapIndexSelector indexSelector)
  {
    if (supportShortCircuit()) {
      final BitmapIndex bitmapIndex = indexSelector.getBitmapIndex(boundDimFilter.getDimension());

      if (bitmapIndex == null || bitmapIndex.getCardinality() == 0) {
        return doesMatch(null) ? 1. : 0.;
      }

      return Filters.estimateSelectivity(
          bitmapIndex,
          getBitmapIndexList(boundDimFilter, bitmapIndex),
          indexSelector.getNumRows()
      );
    } else {
      return Filters.estimateSelectivity(
          boundDimFilter.getDimension(),
          indexSelector,
          getPredicateFactory().makeStringPredicate()
      );
    }
  }

  private boolean supportShortCircuit()
  {
    // Optimization for lexicographic bounds with no extractionFn => binary search through the index
    return boundDimFilter.getOrdering().equals(StringComparators.LEXICOGRAPHIC) && extractionFn == null;
  }

  @Override
  public ValueMatcher makeMatcher(ColumnSelectorFactory factory)
  {
    return Filters.makeValueMatcher(factory, boundDimFilter.getDimension(), getPredicateFactory());
  }

  @Override
  public VectorValueMatcher makeVectorMatcher(final VectorColumnSelectorFactory factory)
  {
    return DimensionHandlerUtils.makeVectorProcessor(
        boundDimFilter.getDimension(),
        VectorValueMatcherColumnStrategizer.instance(),
        factory
    ).makeMatcher(getPredicateFactory());
  }

  @Override
  public boolean canVectorizeMatcher()
  {
    return true;
  }

  @Override
  public boolean supportsBitmapIndex(BitmapIndexSelector selector)
  {
    return selector.getBitmapIndex(boundDimFilter.getDimension()) != null;
  }
  @Override
  public boolean shouldUseBitmapIndex(BitmapIndexSelector selector)
  {
    return Filters.shouldUseBitmapIndex(this, selector, filterTuning);
  }

  @Override
  public boolean supportsSelectivityEstimation(ColumnSelector columnSelector, BitmapIndexSelector indexSelector)
  {
    return Filters.supportsSelectivityEstimation(this, boundDimFilter.getDimension(), columnSelector, indexSelector);
  }

  @Override
  public Set<String> getRequiredColumns()
  {
    return boundDimFilter.getRequiredColumns();
  }

  private static Pair<Integer, Integer> getStartEndIndexes(
      final BoundDimFilter boundDimFilter,
      final BitmapIndex bitmapIndex
  )
  {
    final int startIndex; // inclusive
    int endIndex; // exclusive

    if (!boundDimFilter.hasLowerBound()) {
      startIndex = 0;
    } else {
      final int found = bitmapIndex.getIndex(NullHandling.emptyToNullIfNeeded(boundDimFilter.getLower()));
      if (found >= 0) {
        startIndex = boundDimFilter.isLowerStrict() ? found + 1 : found;
      } else {
        startIndex = -(found + 1);
      }
    }

    if (!boundDimFilter.hasUpperBound()) {
      endIndex = bitmapIndex.getCardinality();
    } else {
      final int found = bitmapIndex.getIndex(NullHandling.emptyToNullIfNeeded(boundDimFilter.getUpper()));
      if (found >= 0) {
        endIndex = boundDimFilter.isUpperStrict() ? found : found + 1;
      } else {
        endIndex = -(found + 1);
      }
    }

    endIndex = startIndex > endIndex ? startIndex : endIndex;

    return new Pair<>(startIndex, endIndex);
  }

  private static Iterable<ImmutableBitmap> getBitmapIterator(
      final BoundDimFilter boundDimFilter,
      final BitmapIndex bitmapIndex
  )
  {
    return Filters.bitmapsFromIndexes(getBitmapIndexList(boundDimFilter, bitmapIndex), bitmapIndex);
  }

  private static IntList getBitmapIndexList(
      final BoundDimFilter boundDimFilter,
      final BitmapIndex bitmapIndex
  )
  {
    // search for start, end indexes in the bitmaps; then include all bitmaps between those points
    final Pair<Integer, Integer> indexes = getStartEndIndexes(boundDimFilter, bitmapIndex);
    final int startIndex = indexes.lhs;
    final int endIndex = indexes.rhs;

    return IntListUtils.fromTo(startIndex, endIndex);
  }

  private DruidPredicateFactory getPredicateFactory()
  {
    return new DruidPredicateFactory()
    {
      @Override
      public Predicate<String> makeStringPredicate()
      {
        if (extractionFn != null) {
          return input -> doesMatch(extractionFn.apply(input));
        }
        return input -> doesMatch(input);

      }

      @Override
      public DruidLongPredicate makeLongPredicate()
      {
        if (extractionFn != null) {
          return input -> doesMatch(extractionFn.apply(input));
        }
        if (boundDimFilter.getOrdering().equals(StringComparators.NUMERIC)) {
          return longPredicateSupplier.get();
        }
        return input -> doesMatch(String.valueOf(input));
      }

      @Override
      public DruidFloatPredicate makeFloatPredicate()
      {
        if (extractionFn != null) {
          return input -> doesMatch(extractionFn.apply(input));
        }
        if (boundDimFilter.getOrdering().equals(StringComparators.NUMERIC)) {
          return floatPredicateSupplier.get();
        }
        return input -> doesMatch(String.valueOf(input));
      }

      @Override
      public DruidDoublePredicate makeDoublePredicate()
      {
        if (extractionFn != null) {
          return input -> doesMatch(extractionFn.apply(input));
        }
        if (boundDimFilter.getOrdering().equals(StringComparators.NUMERIC)) {
          return doublePredicateSupplier.get();
        }
        return input -> doesMatch(String.valueOf(input));
      }
    };
  }

  private boolean doesMatch(String input)
  {
    if (input == null) {
      return (!boundDimFilter.hasLowerBound()
              || (NullHandling.isNullOrEquivalent(boundDimFilter.getLower()) && !boundDimFilter.isLowerStrict()))
             // lower bound allows null
             && (!boundDimFilter.hasUpperBound()
                 || !NullHandling.isNullOrEquivalent(boundDimFilter.getUpper())
                 || !boundDimFilter.isUpperStrict()); // upper bound allows null
    }
    int lowerComparing = 1;
    int upperComparing = 1;
    if (boundDimFilter.hasLowerBound()) {
      lowerComparing = comparator.compare(input, boundDimFilter.getLower());
    }
    if (boundDimFilter.hasUpperBound()) {
      upperComparing = comparator.compare(boundDimFilter.getUpper(), input);
    }
    if (boundDimFilter.isLowerStrict() && boundDimFilter.isUpperStrict()) {
      return ((lowerComparing > 0)) && (upperComparing > 0);
    } else if (boundDimFilter.isLowerStrict()) {
      return (lowerComparing > 0) && (upperComparing >= 0);
    } else if (boundDimFilter.isUpperStrict()) {
      return (lowerComparing >= 0) && (upperComparing > 0);
    }
    return (lowerComparing >= 0) && (upperComparing >= 0);
  }
}
