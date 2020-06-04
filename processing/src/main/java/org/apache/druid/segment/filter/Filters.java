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
import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.ints.IntIterable;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.query.BitmapResultFactory;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.filter.BitmapIndexSelector;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.FilterTuning;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.segment.ColumnProcessors;
import org.apache.druid.segment.ColumnSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.IntIteratorUtils;
import org.apache.druid.segment.column.BitmapIndex;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.data.CloseableIndexed;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.filter.cnf.CalciteCnfHelper;
import org.apache.druid.segment.filter.cnf.HiveCnfHelper;
import org.apache.druid.segment.join.filter.AllNullColumnSelectorFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *
 */
public class Filters
{
  private static final ColumnSelectorFactory ALL_NULL_COLUMN_SELECTOR_FACTORY = new AllNullColumnSelectorFactory();

  /**
   * Convert a list of DimFilters to a list of Filters.
   *
   * @param dimFilters list of DimFilters, should all be non-null
   *
   * @return list of Filters
   */
  public static Set<Filter> toFilters(List<DimFilter> dimFilters)
  {
    return dimFilters.stream().map(DimFilter::toFilter).collect(Collectors.toSet());
  }

  /**
   * Convert a DimFilter to a Filter.
   *
   * @param dimFilter dimFilter
   *
   * @return converted filter, or null if input was null
   */
  @Nullable
  public static Filter toFilter(@Nullable DimFilter dimFilter)
  {
    return dimFilter == null ? null : dimFilter.toFilter();
  }

  /**
   * Create a ValueMatcher that compares row values to the provided string.
   * <p>
   * An implementation of this method should be able to handle dimensions of various types.
   *
   * @param columnSelectorFactory Selector for columns.
   * @param columnName            The column to filter.
   * @param value                 The value to match against, represented as a String.
   *
   * @return An object that matches row values on the provided value.
   */
  public static ValueMatcher makeValueMatcher(
      final ColumnSelectorFactory columnSelectorFactory,
      final String columnName,
      final String value
  )
  {
    return ColumnProcessors.makeProcessor(
        columnName,
        new ConstantValueMatcherFactory(value),
        columnSelectorFactory
    );
  }

  /**
   * Create a ValueMatcher that applies a predicate to row values.
   * <p>
   * The caller provides a predicate factory that can create a predicate for each value type supported by Druid.
   * See {@link DruidPredicateFactory} for more information.
   * <p>
   * When creating the ValueMatcher, the ValueMatcherFactory implementation should decide what type of predicate
   * to create from the predicate factory based on the ValueType of the specified dimension.
   *
   * @param columnSelectorFactory Selector for columns.
   * @param columnName            The column to filter.
   * @param predicateFactory      Predicate factory
   *
   * @return An object that applies a predicate to row values
   */
  public static ValueMatcher makeValueMatcher(
      final ColumnSelectorFactory columnSelectorFactory,
      final String columnName,
      final DruidPredicateFactory predicateFactory
  )
  {
    return ColumnProcessors.makeProcessor(
        columnName,
        new PredicateValueMatcherFactory(predicateFactory),
        columnSelectorFactory
    );
  }

  public static ImmutableBitmap allFalse(final BitmapIndexSelector selector)
  {
    return selector.getBitmapFactory().makeEmptyImmutableBitmap();
  }

  public static ImmutableBitmap allTrue(final BitmapIndexSelector selector)
  {
    return selector.getBitmapFactory()
                   .complement(selector.getBitmapFactory().makeEmptyImmutableBitmap(), selector.getNumRows());
  }

  /**
   * Transform an iterable of indexes of bitmaps to an iterable of bitmaps
   *
   * @param indexes     indexes of bitmaps
   * @param bitmapIndex an object to retrieve bitmaps using indexes
   *
   * @return an iterable of bitmaps
   */
  static Iterable<ImmutableBitmap> bitmapsFromIndexes(final IntIterable indexes, final BitmapIndex bitmapIndex)
  {
    // Do not use Iterables.transform() to avoid boxing/unboxing integers.
    return new Iterable<ImmutableBitmap>()
    {
      @Override
      public Iterator<ImmutableBitmap> iterator()
      {
        final IntIterator iterator = indexes.iterator();

        return new Iterator<ImmutableBitmap>()
        {
          @Override
          public boolean hasNext()
          {
            return iterator.hasNext();
          }

          @Override
          public ImmutableBitmap next()
          {
            return bitmapIndex.getBitmap(iterator.nextInt());
          }

          @Override
          public void remove()
          {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
  }

  /**
   * Return the union of bitmaps for all values matching a particular predicate.
   *
   * @param dimension           dimension to look at
   * @param selector            bitmap selector
   * @param bitmapResultFactory
   * @param predicate           predicate to use
   *
   * @return bitmap of matching rows
   *
   * @see #estimateSelectivity(String, BitmapIndexSelector, Predicate)
   */
  public static <T> T matchPredicate(
      final String dimension,
      final BitmapIndexSelector selector,
      BitmapResultFactory<T> bitmapResultFactory,
      final Predicate<String> predicate
  )
  {
    return bitmapResultFactory.unionDimensionValueBitmaps(matchPredicateNoUnion(dimension, selector, predicate));
  }

  /**
   * Return an iterable of bitmaps for all values matching a particular predicate. Unioning these bitmaps
   * yields the same result that {@link #matchPredicate(String, BitmapIndexSelector, BitmapResultFactory, Predicate)}
   * would have returned.
   *
   * @param dimension dimension to look at
   * @param selector  bitmap selector
   * @param predicate predicate to use
   *
   * @return iterable of bitmaps of matching rows
   */
  public static Iterable<ImmutableBitmap> matchPredicateNoUnion(
      final String dimension,
      final BitmapIndexSelector selector,
      final Predicate<String> predicate
  )
  {
    Preconditions.checkNotNull(dimension, "dimension");
    Preconditions.checkNotNull(selector, "selector");
    Preconditions.checkNotNull(predicate, "predicate");

    // Missing dimension -> match all rows if the predicate matches null; match no rows otherwise
    try (final CloseableIndexed<String> dimValues = selector.getDimensionValues(dimension)) {
      if (dimValues == null || dimValues.size() == 0) {
        return ImmutableList.of(predicate.apply(null) ? allTrue(selector) : allFalse(selector));
      }

      // Apply predicate to all dimension values and union the matching bitmaps
      final BitmapIndex bitmapIndex = selector.getBitmapIndex(dimension);
      return makePredicateQualifyingBitmapIterable(bitmapIndex, predicate, dimValues);
    }
    catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Return an estimated selectivity for bitmaps of all values matching the given predicate.
   *
   * @param dimension     dimension to look at
   * @param indexSelector bitmap selector
   * @param predicate     predicate to use
   *
   * @return estimated selectivity
   *
   * @see #matchPredicate(String, BitmapIndexSelector, BitmapResultFactory, Predicate)
   */
  public static double estimateSelectivity(
      final String dimension,
      final BitmapIndexSelector indexSelector,
      final Predicate<String> predicate
  )
  {
    Preconditions.checkNotNull(dimension, "dimension");
    Preconditions.checkNotNull(indexSelector, "selector");
    Preconditions.checkNotNull(predicate, "predicate");

    // Missing dimension -> match all rows if the predicate matches null; match no rows otherwise
    try (final CloseableIndexed<String> dimValues = indexSelector.getDimensionValues(dimension)) {
      if (dimValues == null || dimValues.size() == 0) {
        return predicate.apply(null) ? 1. : 0.;
      }

      // Apply predicate to all dimension values and union the matching bitmaps
      final BitmapIndex bitmapIndex = indexSelector.getBitmapIndex(dimension);
      return estimateSelectivity(
          bitmapIndex,
          IntIteratorUtils.toIntList(
              makePredicateQualifyingIndexIterable(bitmapIndex, predicate, dimValues).iterator()
          ),
          indexSelector.getNumRows()
      );
    }
    catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Return an estimated selectivity for bitmaps for the dimension values given by dimValueIndexes.
   *
   * @param bitmapIndex  bitmap index
   * @param bitmaps      bitmaps to extract, by index
   * @param totalNumRows number of rows in the column associated with this bitmap index
   *
   * @return estimated selectivity
   */
  public static double estimateSelectivity(
      final BitmapIndex bitmapIndex,
      final IntList bitmaps,
      final long totalNumRows
  )
  {
    long numMatchedRows = 0;
    for (int i = 0; i < bitmaps.size(); i++) {
      final ImmutableBitmap bitmap = bitmapIndex.getBitmap(bitmaps.getInt(i));
      numMatchedRows += bitmap.size();
    }

    return Math.min(1., (double) numMatchedRows / totalNumRows);
  }

  /**
   * Return an estimated selectivity for bitmaps given by an iterator.
   *
   * @param bitmaps      iterator of bitmaps
   * @param totalNumRows number of rows in the column associated with this bitmap index
   *
   * @return estimated selectivity
   */
  public static double estimateSelectivity(
      final Iterator<ImmutableBitmap> bitmaps,
      final long totalNumRows
  )
  {
    long numMatchedRows = 0;
    while (bitmaps.hasNext()) {
      final ImmutableBitmap bitmap = bitmaps.next();
      numMatchedRows += bitmap.size();
    }

    return Math.min(1., (double) numMatchedRows / totalNumRows);
  }

  private static Iterable<ImmutableBitmap> makePredicateQualifyingBitmapIterable(
      final BitmapIndex bitmapIndex,
      final Predicate<String> predicate,
      final Indexed<String> dimValues
  )
  {
    return bitmapsFromIndexes(makePredicateQualifyingIndexIterable(bitmapIndex, predicate, dimValues), bitmapIndex);
  }

  private static IntIterable makePredicateQualifyingIndexIterable(
      final BitmapIndex bitmapIndex,
      final Predicate<String> predicate,
      final Indexed<String> dimValues
  )
  {
    return new IntIterable()
    {
      @Override
      public IntIterator iterator()
      {
        return new IntIterator()
        {
          private final int bitmapIndexCardinality = bitmapIndex.getCardinality();
          private int nextIndex = 0;
          private int found;

          {
            found = findNextIndex();
          }

          private int findNextIndex()
          {
            while (nextIndex < bitmapIndexCardinality && !predicate.apply(dimValues.get(nextIndex))) {
              nextIndex++;
            }

            if (nextIndex < bitmapIndexCardinality) {
              return nextIndex++;
            } else {
              return -1;
            }
          }

          @Override
          public boolean hasNext()
          {
            return found != -1;
          }

          @Override
          public int nextInt()
          {
            int foundIndex = this.found;
            if (foundIndex == -1) {
              throw new NoSuchElementException();
            }
            this.found = findNextIndex();
            return foundIndex;
          }
        };
      }
    };
  }

  static boolean supportsSelectivityEstimation(
      Filter filter,
      String dimension,
      ColumnSelector columnSelector,
      BitmapIndexSelector indexSelector
  )
  {
    if (filter.supportsBitmapIndex(indexSelector)) {
      final ColumnHolder columnHolder = columnSelector.getColumnHolder(dimension);
      if (columnHolder != null) {
        return !columnHolder.getCapabilities().hasMultipleValues().isMaybeTrue();
      }
    }
    return false;
  }

  @Nullable
  public static Filter convertToCNFFromQueryContext(Query query, @Nullable Filter filter)
  {
    if (filter == null) {
      return null;
    }
    boolean useCNF = query.getContextBoolean(QueryContexts.USE_FILTER_CNF_KEY, QueryContexts.DEFAULT_USE_FILTER_CNF);
    return useCNF ? Filters.toCnf(filter) : filter;
  }

  public static Filter toCnf(Filter current)
  {
    // Push down NOT filters to leaves if possible to remove NOT on NOT filters and reduce hierarchy.
    // ex) ~(a OR ~b) => ~a AND b
    current = HiveCnfHelper.pushDownNot(current);
    // Flatten nested AND and OR filters if possible.
    // ex) a AND (b AND c) => a AND b AND c
    current = HiveCnfHelper.flatten(current);
    // Pull up AND filters first to convert the filter into a conjunctive form.
    // It is important to pull before CNF conversion to not create a huge CNF.
    // ex) (a AND b) OR (a AND c AND d) => a AND (b OR (c AND d))
    current = CalciteCnfHelper.pull(current);
    // Convert filter to CNF.
    // a AND (b OR (c AND d)) => a AND (b OR c) AND (b OR d)
    current = HiveCnfHelper.convertToCnf(current);
    // Flatten again to remove any flattenable nested AND or OR filters created during CNF conversion.
    current = HiveCnfHelper.flatten(current);
    return current;
  }

  /**
   * This method provides a "standard" implementation of {@link Filter#shouldUseBitmapIndex(BitmapIndexSelector)} which takes
   * a {@link Filter}, a {@link BitmapIndexSelector}, and {@link FilterTuning} to determine if:
   * a) the filter supports bitmap indexes for all required columns
   * b) the filter tuning specifies that it should use the index
   * c) the cardinality of the column is above the minimum threshold and below the maximum threshold to use the index
   *
   * If all these things are true, {@link org.apache.druid.segment.QueryableIndexStorageAdapter} will utilize the
   * indexes.
   */
  public static boolean shouldUseBitmapIndex(
      Filter filter,
      BitmapIndexSelector indexSelector,
      @Nullable FilterTuning filterTuning
  )
  {
    final FilterTuning tuning = filterTuning != null ? filterTuning : FilterTuning.createDefault(filter, indexSelector);
    if (filter.supportsBitmapIndex(indexSelector) && tuning.getUseBitmapIndex()) {
      return filter.getRequiredColumns().stream().allMatch(column -> {
        final BitmapIndex index = indexSelector.getBitmapIndex(column);
        Preconditions.checkNotNull(index, "Column does not have a bitmap index");
        final int cardinality = index.getCardinality();
        return cardinality >= tuning.getMinCardinalityToUseBitmapIndex()
               && cardinality <= tuning.getMaxCardinalityToUseBitmapIndex();
      });
    }
    return false;
  }

  /**
   * Create a filter representing an AND relationship across a list of filters.
   *
   * @param filterList List of filters
   *
   * @return If filterList has more than one element, return an AND filter composed of the filters from filterList
   * If filterList has a single element, return that element alone
   * If filterList is empty, return null
   */
  @Nullable
  public static Filter and(List<Filter> filterList)
  {
    if (filterList.isEmpty()) {
      return null;
    }

    if (filterList.size() == 1) {
      return filterList.get(0);
    }

    return new AndFilter(filterList);
  }

  /**
   * Create a filter representing an OR relationship across a set of filters.
   *
   * @param filterSet Set of filters
   *
   * @return If filterSet has more than one element, return an OR filter composed of the filters from filterSet
   * If filterSet has a single element, return that element alone
   * If filterSet is empty, return null
   */
  @Nullable
  public static Filter or(Set<Filter> filterSet)
  {
    if (filterSet.isEmpty()) {
      return null;
    }

    if (filterSet.size() == 1) {
      return filterSet.iterator().next();
    }

    return new OrFilter(filterSet);
  }

  /**
   * @param filter the filter.
   * @return The normalized or clauses for the provided filter.
   */
  public static Set<Filter> toNormalizedOrClauses(Filter filter)
  {
    Filter normalizedFilter = Filters.toCnf(filter);

    // List of candidates for pushdown
    // CNF normalization will generate either
    // - an AND filter with multiple subfilters
    // - or a single non-AND subfilter which cannot be split further
    Set<Filter> normalizedOrClauses;
    if (normalizedFilter instanceof AndFilter) {
      normalizedOrClauses = ((AndFilter) normalizedFilter).getFilters();
    } else {
      normalizedOrClauses = Collections.singleton(normalizedFilter);
    }
    return normalizedOrClauses;
  }


  public static boolean filterMatchesNull(Filter filter)
  {
    ValueMatcher valueMatcher = filter.makeMatcher(ALL_NULL_COLUMN_SELECTOR_FACTORY);
    return valueMatcher.matches();
  }
}
