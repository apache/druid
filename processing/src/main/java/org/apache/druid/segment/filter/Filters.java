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
import com.google.common.collect.Iterables;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.DefaultBitmapResultFactory;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.filter.BooleanFilter;
import org.apache.druid.query.filter.ColumnIndexSelector;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.query.filter.DruidPredicateMatch;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.FilterTuning;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.segment.ColumnProcessors;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.filter.cnf.CNFFilterExplosionException;
import org.apache.druid.segment.filter.cnf.CalciteCnfHelper;
import org.apache.druid.segment.filter.cnf.HiveCnfHelper;
import org.apache.druid.segment.index.AllFalseBitmapColumnIndex;
import org.apache.druid.segment.index.AllTrueBitmapColumnIndex;
import org.apache.druid.segment.index.AllUnknownBitmapColumnIndex;
import org.apache.druid.segment.index.BitmapColumnIndex;
import org.apache.druid.segment.index.semantic.DictionaryEncodedStringValueIndex;
import org.apache.druid.segment.index.semantic.DruidPredicateIndexes;
import org.apache.druid.segment.join.filter.AllNullColumnSelectorFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 *
 */
public class Filters
{
  private static final ColumnSelectorFactory ALL_NULL_COLUMN_SELECTOR_FACTORY = new AllNullColumnSelectorFactory();

  /**
   * Convert a {@link DimFilter} to an optimized {@link Filter} for use at the top level of a query.
   *
   * Must not be used by {@link DimFilter} to convert their children, because the three-valued logic parameter
   * "includeUnknown" will not be correctly propagated. For {@link DimFilter} that convert their children, use
   * {@link DimFilter#toOptimizedFilter(boolean)} instead.
   *
   * @param dimFilter dimFilter
   *
   * @return converted filter, or null if input was null
   */
  @Nullable
  public static Filter toFilter(@Nullable DimFilter dimFilter)
  {
    return dimFilter == null ? null : dimFilter.toOptimizedFilter(false);
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

  @Nullable
  public static BitmapColumnIndex makePredicateIndex(
      final String column,
      final ColumnIndexSelector selector,
      final DruidPredicateFactory predicateFactory
  )
  {
    Preconditions.checkNotNull(column, "column");
    Preconditions.checkNotNull(selector, "selector");
    Preconditions.checkNotNull(predicateFactory, "predicateFactory");
    final ColumnIndexSupplier indexSupplier = selector.getIndexSupplier(column);
    if (indexSupplier != null) {
      final DruidPredicateIndexes predicateIndexes = indexSupplier.as(DruidPredicateIndexes.class);
      if (predicateIndexes != null) {
        return predicateIndexes.forPredicate(predicateFactory);
      }
      // index doesn't exist
      return null;
    }
    // missing column -> match all rows if the predicate matches null; match no rows otherwise
    final DruidPredicateMatch match = predicateFactory.makeStringPredicate().apply(null);
    return makeMissingColumnNullIndex(match, selector);
  }

  public static BitmapColumnIndex makeMissingColumnNullIndex(
      DruidPredicateMatch match,
      final ColumnIndexSelector selector
  )
  {
    if (match == DruidPredicateMatch.TRUE) {
      return new AllTrueBitmapColumnIndex(selector);
    }
    if (match == DruidPredicateMatch.UNKNOWN) {
      return new AllUnknownBitmapColumnIndex(selector);
    }
    return new AllFalseBitmapColumnIndex(selector.getBitmapFactory());
  }

  public static ImmutableBitmap computeDefaultBitmapResults(Filter filter, ColumnIndexSelector selector)
  {
    return filter.getBitmapColumnIndex(selector).computeBitmapResult(
        new DefaultBitmapResultFactory(selector.getBitmapFactory()),
        false
    );
  }

  @Nullable
  public static Filter convertToCNFFromQueryContext(Query query, @Nullable Filter filter)
  {
    if (filter == null) {
      return null;
    }
    boolean useCNF = query.context().getBoolean(QueryContexts.USE_FILTER_CNF_KEY, QueryContexts.DEFAULT_USE_FILTER_CNF);
    try {
      return useCNF ? Filters.toCnf(filter) : filter;
    }
    catch (CNFFilterExplosionException cnfFilterExplosionException) {
      return filter; // cannot convert to CNF, return the filter as is
    }
  }

  public static Filter toCnf(Filter current) throws CNFFilterExplosionException
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


  public static boolean checkFilterTuningUseIndex(
      String columnName,
      ColumnIndexSelector indexSelector,
      @Nullable FilterTuning filterTuning
  )
  {
    if (filterTuning != null) {
      if (!filterTuning.getUseBitmapIndex()) {
        return false;
      }
      if (filterTuning.hasValueCardinalityThreshold()) {
        final ColumnIndexSupplier indexSupplier = indexSelector.getIndexSupplier(columnName);
        if (indexSupplier != null) {
          final DictionaryEncodedStringValueIndex valueIndex =
              indexSupplier.as(DictionaryEncodedStringValueIndex.class);
          if (valueIndex != null) {
            final int cardinality = valueIndex.getCardinality();
            Integer min = filterTuning.getMinCardinalityToUseBitmapIndex();
            Integer max = filterTuning.getMaxCardinalityToUseBitmapIndex();
            if (min != null && cardinality < min) {
              return false;
            }
            if (max != null && cardinality > max) {
              return false;
            }
          }
        }
      }
    }
    return true;
  }



  /**
   * Create a filter representing an AND relationship across a list of filters. Deduplicates filters, flattens stacks,
   * and removes null filters and literal "false" filters.
   *
   * @param filters List of filters
   *
   * @return If "filters" has more than one filter remaining after processing, returns {@link AndFilter}.
   * If "filters" has a single element remaining after processing, return that filter alone.
   *
   * @throws IllegalArgumentException if "filters" is empty or only contains nulls
   */
  public static Filter and(final List<Filter> filters)
  {
    return maybeAnd(filters).orElseThrow(() -> new IAE("Expected nonempty filters list"));
  }

  /**
   * Like {@link #and}, but returns an empty Optional instead of throwing an exception if "filters" is empty
   * or only contains nulls.
   */
  public static Optional<Filter> maybeAnd(List<Filter> filters)
  {
    final List<Filter> nonNullFilters = nonNull(filters);

    if (nonNullFilters.isEmpty()) {
      return Optional.empty();
    }

    final LinkedHashSet<Filter> filtersToUse = flattenAndChildren(nonNullFilters);

    if (filtersToUse.isEmpty()) {
      assert !filters.isEmpty();
      // Original "filters" list must have been 100% literally-true filters.
      return Optional.of(TrueFilter.instance());
    } else if (filtersToUse.stream().anyMatch(filter -> filter instanceof FalseFilter)) {
      // AND of FALSE with anything is FALSE.
      return Optional.of(FalseFilter.instance());
    } else if (filtersToUse.size() == 1) {
      return Optional.of(Iterables.getOnlyElement(filtersToUse));
    } else {
      return Optional.of(new AndFilter(filtersToUse));
    }
  }

  /**
   * Create a filter representing an OR relationship across a list of filters. Deduplicates filters, flattens stacks,
   * and removes null filters and literal "false" filters.
   *
   * @param filters List of filters
   *
   * @return If "filters" has more than one filter remaining after processing, returns {@link OrFilter}.
   * If "filters" has a single element remaining after processing, return that filter alone.
   *
   * @throws IllegalArgumentException if "filters" is empty
   */
  public static Filter or(final List<Filter> filters)
  {
    return maybeOr(filters).orElseThrow(() -> new IAE("Expected nonempty filters list"));
  }

  /**
   * Like {@link #or}, but returns an empty Optional instead of throwing an exception if "filters" is empty
   * or only contains nulls.
   */
  public static Optional<Filter> maybeOr(final List<Filter> filters)
  {
    final List<Filter> nonNullFilters = nonNull(filters);

    if (nonNullFilters.isEmpty()) {
      return Optional.empty();
    }

    final LinkedHashSet<Filter> filtersToUse = flattenOrChildren(nonNullFilters);

    if (filtersToUse.isEmpty()) {
      assert !nonNullFilters.isEmpty();
      // Original "filters" list must have been 100% literally-false filters.
      return Optional.of(FalseFilter.instance());
    } else if (filtersToUse.stream().anyMatch(filter -> filter instanceof TrueFilter)) {
      // OR of TRUE with anything is TRUE.
      return Optional.of(TrueFilter.instance());
    } else if (filtersToUse.size() == 1) {
      return Optional.of(Iterables.getOnlyElement(filtersToUse));
    } else {
      return Optional.of(new OrFilter(filtersToUse));
    }
  }

  /**
   * @param filter the filter.
   *
   * @return The normalized or clauses for the provided filter.
   */
  public static List<Filter> toNormalizedOrClauses(Filter filter) throws CNFFilterExplosionException
  {
    Filter normalizedFilter = Filters.toCnf(filter);

    // List of candidates for pushdown
    // CNF normalization will generate either
    // - an AND filter with multiple subfilters
    // - or a single non-AND subfilter which cannot be split further
    List<Filter> normalizedOrClauses;
    if (normalizedFilter instanceof AndFilter) {
      normalizedOrClauses = new ArrayList<>(((AndFilter) normalizedFilter).getFilters());
    } else {
      normalizedOrClauses = Collections.singletonList(normalizedFilter);
    }
    return normalizedOrClauses;
  }


  public static boolean filterMatchesNull(Filter filter)
  {
    ValueMatcher valueMatcher = filter.makeMatcher(ALL_NULL_COLUMN_SELECTOR_FACTORY);
    return valueMatcher.matches(false);
  }


  /**
   * Returns a list equivalent to the input list, but with nulls removed. If the original list has no nulls,
   * it is returned directly.
   */
  private static List<Filter> nonNull(final List<Filter> filters)
  {
    if (filters.stream().anyMatch(Objects::isNull)) {
      return filters.stream().filter(Objects::nonNull).collect(Collectors.toList());
    } else {
      return filters;
    }
  }

  /**
   * Flattens children of an AND, removes duplicates, and removes literally-true filters.
   */
  private static LinkedHashSet<Filter> flattenAndChildren(final Collection<Filter> filters)
  {
    final LinkedHashSet<Filter> retVal = new LinkedHashSet<>();

    for (Filter child : filters) {
      if (child instanceof AndFilter) {
        retVal.addAll(flattenAndChildren(((AndFilter) child).getFilters()));
      } else if (!(child instanceof TrueFilter)) {
        retVal.add(child);
      }
    }

    return retVal;
  }

  /**
   * Flattens children of an OR, removes duplicates, and removes literally-false filters.
   */
  private static LinkedHashSet<Filter> flattenOrChildren(final Collection<Filter> filters)
  {
    final LinkedHashSet<Filter> retVal = new LinkedHashSet<>();

    for (Filter child : filters) {
      if (child instanceof OrFilter) {
        retVal.addAll(flattenOrChildren(((OrFilter) child).getFilters()));
      } else if (!(child instanceof FalseFilter)) {
        retVal.add(child);
      }
    }

    return retVal;
  }

  public static int countNumberOfFilters(@Nullable Filter filter)
  {
    if (filter == null) {
      return 0;
    }
    if (filter instanceof BooleanFilter) {
      return ((BooleanFilter) filter).getFilters()
                                     .stream()
                                     .map(f -> countNumberOfFilters(f))
                                     .mapToInt(Integer::intValue)
                                     .sum();
    }
    return 1;
  }
}
