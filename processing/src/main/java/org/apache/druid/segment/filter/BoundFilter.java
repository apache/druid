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
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Doubles;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.BitmapResultFactory;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.filter.BoundDimFilter;
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
import org.apache.druid.query.filter.vector.VectorValueMatcher;
import org.apache.druid.query.filter.vector.VectorValueMatcherColumnProcessorFactory;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnProcessors;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.ColumnIndexCapabilities;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.index.BitmapColumnIndex;
import org.apache.druid.segment.index.semantic.DruidPredicateIndexes;
import org.apache.druid.segment.index.semantic.LexicographicalRangeIndexes;
import org.apache.druid.segment.index.semantic.NullValueIndex;
import org.apache.druid.segment.index.semantic.NumericRangeIndexes;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class BoundFilter implements Filter
{
  private final BoundDimFilter boundDimFilter;
  private final ExtractionFn extractionFn;
  private final FilterTuning filterTuning;

  public BoundFilter(final BoundDimFilter boundDimFilter)
  {
    this.boundDimFilter = boundDimFilter;
    this.extractionFn = boundDimFilter.getExtractionFn();
    this.filterTuning = boundDimFilter.getFilterTuning();
  }

  @Override
  @Nullable
  public BitmapColumnIndex getBitmapColumnIndex(ColumnIndexSelector selector)
  {
    if (!Filters.checkFilterTuningUseIndex(boundDimFilter.getDimension(), selector, filterTuning)) {
      return null;
    }
    final ColumnIndexSupplier indexSupplier = selector.getIndexSupplier(boundDimFilter.getDimension());
    if (indexSupplier == null) {
      // missing column -> match all rows if the predicate matches null; match no rows otherwise
      final DruidPredicateMatch match = getPredicateFactory().makeStringPredicate().apply(null);
      return Filters.makeMissingColumnNullIndex(match, selector);
    }

    if (supportStringShortCircuit()) {
      final LexicographicalRangeIndexes rangeIndexes = indexSupplier.as(LexicographicalRangeIndexes.class);
      if (rangeIndexes != null) {
        final BitmapColumnIndex rangeBitmaps = rangeIndexes.forRange(
            boundDimFilter.getLower(),
            boundDimFilter.isLowerStrict(),
            boundDimFilter.getUpper(),
            boundDimFilter.isUpperStrict()
        );
        if (rangeBitmaps == null) {
          return null;
        }
        // preserve sad backwards compatible behavior where bound filter matches 'null' if the lower bound is not set
        if (doesMatchNull()) {
          return wrapRangeIndexWithNullValueIndex(indexSupplier, rangeBitmaps);
        } else {
          return rangeBitmaps;
        }
      }
    }
    if (supportNumericShortCircuit()) {
      final NumericRangeIndexes rangeIndexes = indexSupplier.as(NumericRangeIndexes.class);
      if (rangeIndexes != null) {
        final Number lower = boundDimFilter.hasLowerBound() ? Doubles.tryParse(boundDimFilter.getLower()) : null;
        final Number upper = boundDimFilter.hasUpperBound() ? Doubles.tryParse(boundDimFilter.getUpper()) : null;
        // valid number bounds are required to use the range index, otherwise we need to fall back to the predicate
        // index to get consistent behavior with the value matcher. in a better world this might be a much earlier
        // validation error, but.. the bound filter doesn't live in that world
        final boolean lowerValid = !(boundDimFilter.hasLowerBound() && lower == null);
        final boolean upperValid = !(boundDimFilter.hasUpperBound() && upper == null);
        if (lowerValid && upperValid) {
          final BitmapColumnIndex rangeBitmaps = rangeIndexes.forRange(
              lower,
              boundDimFilter.isLowerStrict(),
              upper,
              boundDimFilter.isUpperStrict()
          );
          if (rangeBitmaps == null) {
            return null;
          }
          // preserve sad backwards compatible behavior where bound filter matches 'null' if the lower bound is not set
          if (doesMatchNull()) {
            return wrapRangeIndexWithNullValueIndex(indexSupplier, rangeBitmaps);
          } else {
            return rangeBitmaps;
          }
        }
      }
    }

    final DruidPredicateIndexes predicateIndexes = indexSupplier.as(DruidPredicateIndexes.class);
    if (predicateIndexes != null) {
      return predicateIndexes.forPredicate(getPredicateFactory());
    }
    // index doesn't exist
    return null;
  }

  @Nullable
  private BitmapColumnIndex wrapRangeIndexWithNullValueIndex(
      ColumnIndexSupplier indexSupplier,
      BitmapColumnIndex rangeIndex
  )
  {


    final BitmapColumnIndex nullBitmap;
    final NullValueIndex nulls = indexSupplier.as(NullValueIndex.class);
    if (nulls == null) {
      return null;
    }
    nullBitmap = nulls.get();

    return new BitmapColumnIndex()
    {
      @Override
      public ColumnIndexCapabilities getIndexCapabilities()
      {
        return rangeIndex.getIndexCapabilities().merge(nullBitmap.getIndexCapabilities());
      }

      @Override
      public <T> T computeBitmapResult(BitmapResultFactory<T> bitmapResultFactory, boolean includeUnknown)
      {
        return bitmapResultFactory.union(
            ImmutableList.of(
                rangeIndex.computeBitmapResult(bitmapResultFactory, false),
                nullBitmap.computeBitmapResult(bitmapResultFactory, false)
            )
        );
      }
    };
  }

  private boolean supportStringShortCircuit()
  {
    // Optimization for lexicographic bounds with no extractionFn => binary search through the index
    return boundDimFilter.getOrdering().equals(StringComparators.LEXICOGRAPHIC) && extractionFn == null;
  }

  private boolean supportNumericShortCircuit()
  {
    // Optimization for numeric bounds with no extractionFn => binary search through the index
    return boundDimFilter.getOrdering().equals(StringComparators.NUMERIC) && extractionFn == null;
  }

  @Override
  public ValueMatcher makeMatcher(ColumnSelectorFactory factory)
  {
    return Filters.makeValueMatcher(factory, boundDimFilter.getDimension(), getPredicateFactory());
  }

  @Override
  public VectorValueMatcher makeVectorMatcher(final VectorColumnSelectorFactory factory)
  {
    return ColumnProcessors.makeVectorProcessor(
        boundDimFilter.getDimension(),
        VectorValueMatcherColumnProcessorFactory.instance(),
        factory
    ).makeMatcher(getPredicateFactory());
  }

  @Override
  public boolean canVectorizeMatcher(ColumnInspector inspector)
  {
    return true;
  }

  @Override
  public Set<String> getRequiredColumns()
  {
    return boundDimFilter.getRequiredColumns();
  }

  @Override
  public boolean supportsRequiredColumnRewrite()
  {
    return true;
  }

  @Override
  public Filter rewriteRequiredColumns(Map<String, String> columnRewrites)
  {
    String rewriteDimensionTo = columnRewrites.get(boundDimFilter.getDimension());

    if (rewriteDimensionTo == null) {
      throw new IAE(
          "Received a non-applicable rewrite: %s, filter's dimension: %s",
          columnRewrites,
          boundDimFilter.getDimension()
      );
    }
    BoundDimFilter newDimFilter = new BoundDimFilter(
        rewriteDimensionTo,
        boundDimFilter.getLower(),
        boundDimFilter.getUpper(),
        boundDimFilter.isLowerStrict(),
        boundDimFilter.isUpperStrict(),
        null,
        boundDimFilter.getExtractionFn(),
        boundDimFilter.getOrdering()
    );
    return new BoundFilter(
        newDimFilter
    );
  }

  private DruidPredicateFactory getPredicateFactory()
  {
    return new BoundDimFilterDruidPredicateFactory(extractionFn, boundDimFilter);
  }

  private boolean doesMatchNull()
  {
    return doesMatch(null, boundDimFilter);
  }

  private static boolean doesMatch(String input, BoundDimFilter boundDimFilter)
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
      lowerComparing = boundDimFilter.getOrdering().compare(input, boundDimFilter.getLower());
    }
    if (boundDimFilter.hasUpperBound()) {
      upperComparing = boundDimFilter.getOrdering().compare(boundDimFilter.getUpper(), input);
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

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BoundFilter that = (BoundFilter) o;
    return Objects.equals(boundDimFilter, that.boundDimFilter) &&
           Objects.equals(extractionFn, that.extractionFn) &&
           Objects.equals(filterTuning, that.filterTuning);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(boundDimFilter, extractionFn, filterTuning);
  }

  @Override
  public String toString()
  {
    return boundDimFilter.toString();
  }

  @VisibleForTesting
  static class BoundDimFilterDruidPredicateFactory implements DruidPredicateFactory
  {
    private final ExtractionFn extractionFn;
    private final BoundDimFilter boundDimFilter;
    private final Supplier<DruidLongPredicate> longPredicateSupplier;
    private final Supplier<DruidFloatPredicate> floatPredicateSupplier;
    private final Supplier<DruidDoublePredicate> doublePredicateSupplier;

    BoundDimFilterDruidPredicateFactory(ExtractionFn extractionFn, BoundDimFilter boundDimFilter)
    {
      this.extractionFn = extractionFn;
      this.boundDimFilter = boundDimFilter;
      this.longPredicateSupplier = boundDimFilter.getLongPredicateSupplier();
      this.floatPredicateSupplier = boundDimFilter.getFloatPredicateSupplier();
      this.doublePredicateSupplier = boundDimFilter.getDoublePredicateSupplier();
    }

    @Override
    public DruidObjectPredicate<String> makeStringPredicate()
    {
      if (extractionFn != null) {
        return input -> DruidPredicateMatch.of(doesMatch(extractionFn.apply(input), boundDimFilter));
      }
      return input -> DruidPredicateMatch.of(doesMatch(input, boundDimFilter));

    }

    @Override
    public DruidLongPredicate makeLongPredicate()
    {
      if (extractionFn != null) {
        return input -> DruidPredicateMatch.of(doesMatch(extractionFn.apply(input), boundDimFilter));
      }
      if (boundDimFilter.getOrdering().equals(StringComparators.NUMERIC)) {
        return longPredicateSupplier.get();
      }
      return input -> DruidPredicateMatch.of(doesMatch(String.valueOf(input), boundDimFilter));
    }

    @Override
    public DruidFloatPredicate makeFloatPredicate()
    {
      if (extractionFn != null) {
        return input -> DruidPredicateMatch.of(doesMatch(extractionFn.apply(input), boundDimFilter));
      }
      if (boundDimFilter.getOrdering().equals(StringComparators.NUMERIC)) {
        return floatPredicateSupplier.get();
      }
      return input -> DruidPredicateMatch.of(doesMatch(String.valueOf(input), boundDimFilter));
    }

    @Override
    public DruidDoublePredicate makeDoublePredicate()
    {
      if (extractionFn != null) {
        return input -> DruidPredicateMatch.of(doesMatch(extractionFn.apply(input), boundDimFilter));
      }
      if (boundDimFilter.getOrdering().equals(StringComparators.NUMERIC)) {
        return doublePredicateSupplier.get();
      }
      return input -> DruidPredicateMatch.of(doesMatch(String.valueOf(input), boundDimFilter));
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
      BoundDimFilterDruidPredicateFactory that = (BoundDimFilterDruidPredicateFactory) o;
      return Objects.equals(extractionFn, that.extractionFn) &&
             Objects.equals(boundDimFilter, that.boundDimFilter);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(extractionFn, boundDimFilter);
    }
  }
}
