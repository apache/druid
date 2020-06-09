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
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;
import it.unimi.dsi.fastutil.ints.IntIterable;
import it.unimi.dsi.fastutil.ints.IntIterator;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.BitmapResultFactory;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.filter.BitmapIndexSelector;
import org.apache.druid.query.filter.DruidDoublePredicate;
import org.apache.druid.query.filter.DruidFloatPredicate;
import org.apache.druid.query.filter.DruidLongPredicate;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.FilterTuning;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.filter.vector.VectorValueMatcher;
import org.apache.druid.query.filter.vector.VectorValueMatcherColumnProcessorFactory;
import org.apache.druid.segment.ColumnSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.IntIteratorUtils;
import org.apache.druid.segment.column.BitmapIndex;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The IN filter.
 * For single-valued dimension, this filter returns true if the dimension value matches to one of the
 * given {@link #values}.
 * For multi-valued dimension, this filter returns true if one of the dimension values matches to one of the
 * given {@link #values}.
 *
 * In SQL-compatible null handling mode, this filter is equivalent to {@code (dimension IN [values])} or
 * {@code (dimension IN [non-null values] OR dimension IS NULL)} when {@link #values} contains nulls.
 * In default null handling mode, this filter is equivalent to {@code (dimension IN [values])} or
 * {@code (dimension IN [non-null values, ''])} when {@link #values} contains nulls.
 */
public class InFilter implements Filter
{
  private final String dimension;
  private final Set<String> values;
  private final ExtractionFn extractionFn;
  private final FilterTuning filterTuning;
  private final Supplier<DruidLongPredicate> longPredicateSupplier;
  private final Supplier<DruidFloatPredicate> floatPredicateSupplier;
  private final Supplier<DruidDoublePredicate> doublePredicateSupplier;

  public InFilter(
      String dimension,
      Set<String> values,
      Supplier<DruidLongPredicate> longPredicateSupplier,
      Supplier<DruidFloatPredicate> floatPredicateSupplier,
      Supplier<DruidDoublePredicate> doublePredicateSupplier,
      ExtractionFn extractionFn,
      FilterTuning filterTuning
  )
  {
    this.dimension = dimension;
    this.values = values;
    this.extractionFn = extractionFn;
    this.filterTuning = filterTuning;
    this.longPredicateSupplier = longPredicateSupplier;
    this.floatPredicateSupplier = floatPredicateSupplier;
    this.doublePredicateSupplier = doublePredicateSupplier;
  }

  @Override
  public <T> T getBitmapResult(BitmapIndexSelector selector, BitmapResultFactory<T> bitmapResultFactory)
  {
    if (extractionFn == null) {
      final BitmapIndex bitmapIndex = selector.getBitmapIndex(dimension);
      return bitmapResultFactory.unionDimensionValueBitmaps(getBitmapIterable(bitmapIndex));
    } else {
      return Filters.matchPredicate(
          dimension,
          selector,
          bitmapResultFactory,
          getPredicateFactory().makeStringPredicate()
      );
    }
  }

  @Override
  public double estimateSelectivity(BitmapIndexSelector indexSelector)
  {
    if (extractionFn == null) {
      final BitmapIndex bitmapIndex = indexSelector.getBitmapIndex(dimension);
      return Filters.estimateSelectivity(
          bitmapIndex,
          IntIteratorUtils.toIntList(getBitmapIndexIterable(bitmapIndex).iterator()),
          indexSelector.getNumRows()
      );
    } else {
      return Filters.estimateSelectivity(
          dimension,
          indexSelector,
          getPredicateFactory().makeStringPredicate()
      );
    }
  }

  private Iterable<ImmutableBitmap> getBitmapIterable(final BitmapIndex bitmapIndex)
  {
    return Filters.bitmapsFromIndexes(getBitmapIndexIterable(bitmapIndex), bitmapIndex);
  }

  private IntIterable getBitmapIndexIterable(final BitmapIndex bitmapIndex)
  {
    return () -> new IntIterator()
    {
      final Iterator<String> iterator = values.iterator();

      @Override
      public boolean hasNext()
      {
        return iterator.hasNext();
      }

      @Override
      public int nextInt()
      {
        return bitmapIndex.getIndex(iterator.next());
      }
    };
  }

  @Override
  public ValueMatcher makeMatcher(ColumnSelectorFactory factory)
  {
    return Filters.makeValueMatcher(factory, dimension, getPredicateFactory());
  }

  @Override
  public VectorValueMatcher makeVectorMatcher(final VectorColumnSelectorFactory factory)
  {
    return DimensionHandlerUtils.makeVectorProcessor(
        dimension,
        VectorValueMatcherColumnProcessorFactory.instance(),
        factory
    ).makeMatcher(getPredicateFactory());
  }

  @Override
  public boolean canVectorizeMatcher()
  {
    return true;
  }

  @Override
  public Set<String> getRequiredColumns()
  {
    return ImmutableSet.of(dimension);
  }

  @Override
  public boolean supportsRequiredColumnRewrite()
  {
    return true;
  }

  @Override
  public Filter rewriteRequiredColumns(Map<String, String> columnRewrites)
  {
    String rewriteDimensionTo = columnRewrites.get(dimension);
    if (rewriteDimensionTo == null) {
      throw new IAE("Received a non-applicable rewrite: %s, filter's dimension: %s", columnRewrites, dimension);
    }

    return new InFilter(
        rewriteDimensionTo,
        values,
        longPredicateSupplier,
        floatPredicateSupplier,
        doublePredicateSupplier,
        extractionFn,
        filterTuning
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
    return Filters.supportsSelectivityEstimation(this, dimension, columnSelector, indexSelector);
  }

  private DruidPredicateFactory getPredicateFactory()
  {
    return new InFilterDruidPredicateFactory(extractionFn, values, longPredicateSupplier, floatPredicateSupplier, doublePredicateSupplier);
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
    InFilter inFilter = (InFilter) o;
    return Objects.equals(dimension, inFilter.dimension) &&
           Objects.equals(values, inFilter.values) &&
           Objects.equals(extractionFn, inFilter.extractionFn) &&
           Objects.equals(filterTuning, inFilter.filterTuning);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dimension, values, extractionFn, filterTuning);
  }

  @VisibleForTesting
  static class InFilterDruidPredicateFactory implements DruidPredicateFactory
  {
    private final ExtractionFn extractionFn;
    private final Set<String> values;
    private final Supplier<DruidLongPredicate> longPredicateSupplier;
    private final Supplier<DruidFloatPredicate> floatPredicateSupplier;
    private final Supplier<DruidDoublePredicate> doublePredicateSupplier;

    InFilterDruidPredicateFactory(
        ExtractionFn extractionFn,
        Set<String> values,
        Supplier<DruidLongPredicate> longPredicateSupplier,
        Supplier<DruidFloatPredicate> floatPredicateSupplier,
        Supplier<DruidDoublePredicate> doublePredicateSupplier
    )
    {
      this.extractionFn = extractionFn;
      this.values = values;
      this.longPredicateSupplier = longPredicateSupplier;
      this.floatPredicateSupplier = floatPredicateSupplier;
      this.doublePredicateSupplier = doublePredicateSupplier;
    }

    @Override
    public Predicate<String> makeStringPredicate()
    {
      if (extractionFn != null) {
        return input -> values.contains(extractionFn.apply(input));
      } else {
        return input -> values.contains(input);
      }
    }

    @Override
    public DruidLongPredicate makeLongPredicate()
    {
      if (extractionFn != null) {
        return input -> values.contains(extractionFn.apply(input));
      } else {
        return longPredicateSupplier.get();
      }
    }

    @Override
    public DruidFloatPredicate makeFloatPredicate()
    {
      if (extractionFn != null) {
        return input -> values.contains(extractionFn.apply(input));
      } else {
        return floatPredicateSupplier.get();
      }
    }

    @Override
    public DruidDoublePredicate makeDoublePredicate()
    {
      if (extractionFn != null) {
        return input -> values.contains(extractionFn.apply(input));
      }
      return input -> doublePredicateSupplier.get().applyDouble(input);
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
      InFilterDruidPredicateFactory that = (InFilterDruidPredicateFactory) o;
      return Objects.equals(extractionFn, that.extractionFn) &&
             Objects.equals(values, that.values);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(extractionFn, values);
    }
  }
}
