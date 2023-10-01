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

package org.apache.druid.segment;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.filter.BooleanFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.NullFilter;
import org.apache.druid.query.filter.RangeFilter;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.ListIndexed;
import org.apache.druid.segment.filter.AndFilter;
import org.apache.druid.segment.filter.BoundFilter;
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.segment.filter.LikeFilter;
import org.apache.druid.segment.filter.NotFilter;
import org.apache.druid.segment.filter.OrFilter;
import org.apache.druid.segment.filter.SelectorFilter;
import org.apache.druid.segment.join.PostJoinCursor;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * This class serves as the Storage Adapter for the Unnest Segment and is responsible for creating the cursors
 * If the column is dictionary encoded it creates {@link UnnestDimensionCursor} else {@link UnnestColumnValueSelectorCursor}
 * These cursors help navigate the segments for these cases
 */
public class UnnestStorageAdapter implements StorageAdapter
{
  public StorageAdapter getBaseAdapter()
  {
    return baseAdapter;
  }

  private final StorageAdapter baseAdapter;
  private final VirtualColumn unnestColumn;
  private final String outputColumnName;

  @Nullable
  private final DimFilter unnestFilter;

  public UnnestStorageAdapter(
      final StorageAdapter baseAdapter,
      final VirtualColumn unnestColumn,
      @Nullable final DimFilter unnestFilter
  )
  {
    this.baseAdapter = baseAdapter;
    this.unnestColumn = unnestColumn;
    this.outputColumnName = unnestColumn.getOutputName();
    this.unnestFilter = unnestFilter;
  }

  @Override
  public Sequence<Cursor> makeCursors(
      @Nullable Filter filter,
      Interval interval,
      VirtualColumns virtualColumns,
      Granularity gran,
      boolean descending,
      @Nullable QueryMetrics<?> queryMetrics
  )
  {
    final String inputColumn = getUnnestInputIfDirectAccess(unnestColumn);
    final Pair<Filter, Filter> filterPair = computeBaseAndPostUnnestFilters(
        filter,
        unnestFilter != null ? unnestFilter.toFilter() : null,
        virtualColumns,
        inputColumn,
        inputColumn == null ? null : virtualColumns.getColumnCapabilitiesWithFallback(baseAdapter, inputColumn)
    );

    final Sequence<Cursor> baseCursorSequence = baseAdapter.makeCursors(
        filterPair.lhs,
        interval,
        VirtualColumns.create(Collections.singletonList(unnestColumn)),
        gran,
        descending,
        queryMetrics
    );

    return Sequences.map(
        baseCursorSequence,
        cursor -> {
          Objects.requireNonNull(cursor);
          final ColumnCapabilities capabilities = unnestColumn.capabilities(
              cursor.getColumnSelectorFactory(),
              unnestColumn.getOutputName()
          );
          final Cursor unnestCursor;

          if (useDimensionCursor(capabilities)) {
            unnestCursor = new UnnestDimensionCursor(
                cursor,
                cursor.getColumnSelectorFactory(),
                unnestColumn,
                outputColumnName
            );
          } else {
            unnestCursor = new UnnestColumnValueSelectorCursor(
                cursor,
                cursor.getColumnSelectorFactory(),
                unnestColumn,
                outputColumnName
            );
          }
          return PostJoinCursor.wrap(
              unnestCursor,
              virtualColumns,
              filterPair.rhs
          );
        }
    );
  }

  @Override
  public Interval getInterval()
  {
    return baseAdapter.getInterval();
  }

  @Override
  public Indexed<String> getAvailableDimensions()
  {
    final LinkedHashSet<String> availableDimensions = new LinkedHashSet<>();

    for (String dim : baseAdapter.getAvailableDimensions()) {
      availableDimensions.add(dim);
    }
    availableDimensions.add(outputColumnName);
    return new ListIndexed<>(Lists.newArrayList(availableDimensions));
  }

  @Override
  public Iterable<String> getAvailableMetrics()
  {
    return baseAdapter.getAvailableMetrics();
  }

  @Nullable
  public Filter getUnnestFilter()
  {
    return unnestFilter == null ? null : unnestFilter.toFilter();
  }

  @Override
  public int getDimensionCardinality(String column)
  {
    if (!outputColumnName.equals(column)) {
      return baseAdapter.getDimensionCardinality(column);
    }
    return DimensionDictionarySelector.CARDINALITY_UNKNOWN;
  }

  @Override
  public DateTime getMinTime()
  {
    return baseAdapter.getMinTime();
  }

  @Override
  public DateTime getMaxTime()
  {
    return baseAdapter.getMaxTime();
  }

  @Nullable
  @Override
  public Comparable getMinValue(String column)
  {
    if (outputColumnName.equals(column)) {
      return null;
    }

    return baseAdapter.getMinValue(column);
  }

  @Nullable
  @Override
  public Comparable getMaxValue(String column)
  {
    if (outputColumnName.equals(column)) {
      return null;
    }

    return baseAdapter.getMaxValue(column);
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    if (outputColumnName.equals(column)) {
      return unnestColumn.capabilities(baseAdapter, column);
    }

    return baseAdapter.getColumnCapabilities(column);
  }

  @Override
  public int getNumRows()
  {
    return 0;
  }

  @Override
  public DateTime getMaxIngestedEventTime()
  {
    return baseAdapter.getMaxIngestedEventTime();
  }

  @Nullable
  @Override
  public Metadata getMetadata()
  {
    return baseAdapter.getMetadata();
  }

  public VirtualColumn getUnnestColumn()
  {
    return unnestColumn;
  }

  /**
   * Split queryFilter into pre- and post-correlate filters.
   *
   * @param queryFilter            query filter passed to makeCursors
   * @param unnestFilter           filter on unnested column passed to PostUnnestCursor
   * @param queryVirtualColumns    query virtual columns passed to makeCursors
   * @param inputColumn            input column to unnest if it's a direct access; otherwise null
   * @param inputColumnCapabilites input column capabilities if known; otherwise null
   * @return pair of pre- and post-unnest filters
   */
  public Pair<Filter, Filter> computeBaseAndPostUnnestFilters(
      @Nullable final Filter queryFilter,
      @Nullable final Filter unnestFilter,
      final VirtualColumns queryVirtualColumns,
      @Nullable final String inputColumn,
      @Nullable final ColumnCapabilities inputColumnCapabilites
  )
  {
    /*
    The goal of this function is to take a filter from the top of Correlate (queryFilter)
    and a filter from the top of Uncollect (here unnest filter) and then do a rewrite
    to generate filters to be passed to base cursor (filtersPushedDownToBaseCursor) and unnest cursor (filtersForPostUnnestCursor)
    based on the following scenarios:

    1. If there is an AND filter between unnested column and left e.g. select * from foo, UNNEST(dim3) as u(d3) where d3 IN (a,b) and m1 < 10
       query filter -> m1 < 10
       unnest filter -> d3 IN (a,b)

       Output should be:
       filtersPushedDownToBaseCursor -> dim3 IN (a,b) AND m1 < 10
       filtersForPostUnnestCursor -> d3 IN (a,b)

    2. There is an AND filter between unnested column and left e.g. select * from foo, UNNEST(ARRAY[dim1,dim2]) as u(d12) where d12 IN (a,b) and m1 < 10
       query filter -> m1 < 10
       unnest filter -> d12 IN (a,b)

       Output should be:
       filtersPushedDownToBaseCursor -> m1 < 10 (as unnest is on a virtual column it cannot be added to the pre-filter)
       filtersForPostUnnestCursor -> d12 IN (a,b)

    3. There is an OR filter involving unnested and left column e.g.  select * from foo, UNNEST(dim3) as u(d3) where d3 IN (a,b) or m1 < 10
       query filter -> d3 IN (a,b) or m1 < 10
       unnest filter -> null

       Output should be:
       filtersPushedDownToBaseCursor -> dim3 IN (a,b) or m1 < 10
       filtersForPostUnnestCursor -> d3 IN (a,b) or m1 < 10

     4. There is an OR filter involving unnested and left column e.g. select * from foo, UNNEST(ARRAY[dim1,dim2]) as u(d12) where d12 IN (a,b) or m1 < 10
       query filter -> d12 IN (a,b) or m1 < 10
       unnest filter -> null

       Output should be:
       filtersPushedDownToBaseCursor -> null (as the filter cannot be re-written due to presence of virtual columns)
       filtersForPostUnnestCursor -> d12 IN (a,b) or m1 < 10
     */
    final FilterSplitter filterSplitter = new FilterSplitter(inputColumn, inputColumnCapabilites, queryVirtualColumns);

    if (queryFilter != null) {
      if (queryFilter.getRequiredColumns().contains(outputColumnName)) {
        // outside filter contains unnested column
        // requires check for OR and And filters, disqualify rewrite for non-unnest filters
        if (queryFilter instanceof BooleanFilter) {
          boolean isTopLevelAndFilter = queryFilter instanceof AndFilter;
          List<Filter> preFilterList = recursiveRewriteOnUnnestFilters(
              (BooleanFilter) queryFilter,
              inputColumn,
              inputColumnCapabilites,
              filterSplitter,
              isTopLevelAndFilter
          );
          // If rewite on entire query filter is successful then add entire filter to preFilter else skip and only add to post filter.
          if (filterSplitter.getPreFilterCount() == filterSplitter.getOriginalFilterCount()) {
            if (queryFilter instanceof AndFilter) {
              filterSplitter.addPreFilter(new AndFilter(preFilterList));
            } else if (queryFilter instanceof OrFilter) {
              filterSplitter.addPreFilter(new OrFilter(preFilterList));
            }
          }
          // add the entire query filter to unnest filter to be used in Value matcher
          filterSplitter.addPostFilterWithPreFilterIfRewritePossible(queryFilter, true);
        } else {
          // case where the outer filter has reference to the outputcolumn
          filterSplitter.addPostFilterWithPreFilterIfRewritePossible(queryFilter, false);
        }
      } else {
        // normal case without any filter on unnested column
        // add everything to pre-filters
        filterSplitter.addPreFilter(queryFilter);
      }
    }
    filterSplitter.addPostFilterWithPreFilterIfRewritePossible(unnestFilter, false);

    return Pair.of(
        Filters.maybeAnd(filterSplitter.filtersPushedDownToBaseCursor).orElse(null),
        Filters.maybeAnd(filterSplitter.filtersForPostUnnestCursor).orElse(null)
    );
  }

  class FilterSplitter
  {
    private String inputColumn;
    private ColumnCapabilities inputColumnCapabilites;
    private VirtualColumns queryVirtualColumns;

    private int originalFilterCount = 0;
    private int preFilterCount = 0;

    public FilterSplitter(
        String inputColumn,
        ColumnCapabilities inputColumnCapabilites,
        VirtualColumns queryVirtualColumns
    )
    {
      this.inputColumn = inputColumn;
      this.inputColumnCapabilites = inputColumnCapabilites;
      this.queryVirtualColumns = queryVirtualColumns;
    }

    final List<Filter> filtersPushedDownToBaseCursor = new ArrayList<>();
    final List<Filter> filtersForPostUnnestCursor = new ArrayList<>();

    void addPostFilterWithPreFilterIfRewritePossible(@Nullable final Filter filter, boolean skipPreFilters)
    {
      if (filter == null) {
        return;
      }
      if (!skipPreFilters) {
        final Filter newFilter = rewriteFilterOnUnnestColumnIfPossible(filter, inputColumn, inputColumnCapabilites);
        if (newFilter != null) {
          // Add the rewritten filter pre-unnest, so we get the benefit of any indexes, and so we avoid unnesting
          // any rows that do not match this filter at all.
          filtersPushedDownToBaseCursor.add(newFilter);
        }
      }
      // Add original filter post-unnest no matter what: we need to filter out any extraneous unnested values.
      filtersForPostUnnestCursor.add(filter);
    }

    void addPreFilter(@Nullable final Filter filter)
    {
      if (filter == null) {
        return;
      }

      final Set<String> requiredColumns = filter.getRequiredColumns();

      // Run filter post-unnest if it refers to any virtual columns. This is a conservative judgement call
      // that perhaps forces the code to use a ValueMatcher where an index would've been available,
      // which can have real performance implications. This is an interim choice made to value correctness
      // over performance. When we need to optimize this performance, we should be able to
      // create a VirtualColumnDatasource that contains all the virtual columns, in which case the query
      // itself would stop carrying them and everything should be able to be pushed down.
      if (queryVirtualColumns.getVirtualColumns().length > 0) {
        for (String column : requiredColumns) {
          if (queryVirtualColumns.exists(column)) {
            filtersForPostUnnestCursor.add(filter);
            return;
          }
        }
      }
      filtersPushedDownToBaseCursor.add(filter);

    }

    public void addToOriginalFilterCount(int c)
    {
      originalFilterCount += c;
    }

    public void addToPreFilterCount(int c)
    {
      preFilterCount += c;
    }

    public int getOriginalFilterCount()
    {
      return originalFilterCount;
    }

    public int getPreFilterCount()
    {
      return preFilterCount;
    }
  }

  /**
   * handles the nested rewrite for unnest columns in recursive way,
   * it loops through all and/or filters and rewrite only required filters in the child and add it to preFilter if qualified
   * or else skip adding it to preFilters.
   * RULES:
   * 1. Add to preFilters only when top level filter is AND.
   * for example: a=1 and (b=2 or c=2) , In this case a=1 can be added as preFilters but we can not add b=2 as preFilters.
   * 2. If Top level is OR filter then we can either choose to add entire top level OR filter to preFilter or skip it all together.
   * for example: a=1 or (b=2 and c=2)
   * 3. Filters on unnest column which is derived from Array or any other Expression can not be pushe down to base.
   * for example: a=1 and vc=3 , lets say vc is ExpressionVirtualColumn, and vc=3 can not be push down to base even if top level is AND filter.
   * A. Unnesting a single dimension e.g. select * from foo, UNNEST(MV_TO_ARRAY(dim3)) as u(d3)
   * B. Unnesting an expression from multiple columns e.g. select * from foo, UNNEST(ARRAY[dim1,dim2]) as u(d12)
   * In case A, d3 is a direct reference to dim3 so any filter using d3 can be rewritten using dim3 and added to pre filter
   * while in case B, due to presence of the expression virtual column expressionVirtualColumn("j0.unnest", "array(\"dim1\",\"dim2\")", ColumnType.STRING_ARRAY)
   * the filters on d12 cannot be pushed to the pre filters
   *
   * @param queryFilter            query filter passed to makeCursors
   * @param inputColumn            input column to unnest if it's a direct access; otherwise null
   * @param inputColumnCapabilites input column capabilities if known; otherwise null
   */
  private List<Filter> recursiveRewriteOnUnnestFilters(
      BooleanFilter queryFilter,
      final String inputColumn,
      final ColumnCapabilities inputColumnCapabilites,
      final FilterSplitter filterSplitter,
      final boolean isTopLevelAndFilter
  )
  {
    final List<Filter> preFilterList = new ArrayList<>();
    for (Filter filter : queryFilter.getFilters()) {
      if (filter.getRequiredColumns().contains(outputColumnName)) {
        if (filter instanceof AndFilter) {
          List<Filter> andChildFilters = recursiveRewriteOnUnnestFilters(
              (BooleanFilter) filter,
              inputColumn,
              inputColumnCapabilites,
              filterSplitter,
              isTopLevelAndFilter
          );
          if (!andChildFilters.isEmpty()) {
            preFilterList.add(new AndFilter(andChildFilters));
          }
        } else if (filter instanceof OrFilter) {
          // in case of Or Fiters, we set isTopLevelAndFilter to false that prevents pushing down any child filters to base
          List<Filter> orChildFilters = recursiveRewriteOnUnnestFilters(
              (BooleanFilter) filter,
              inputColumn,
              inputColumnCapabilites,
              filterSplitter,
              false
          );
          preFilterList.add(new OrFilter(orChildFilters));
        } else if (filter instanceof NotFilter) {
          continue;
        } else {
          final Filter newFilter = rewriteFilterOnUnnestColumnIfPossible(
              filter,
              inputColumn,
              inputColumnCapabilites
          );
          if (newFilter != null) {
            // this is making sure that we are not pushing the unnest columns filters to base filter without rewriting.
            preFilterList.add(newFilter);
            filterSplitter.addToPreFilterCount(1);
          }
          /*
           Push down the filters to base only if top level is And Filter
           we can not push down if top level filter is OR or unnestColumn is derived expression like arrays
           */
          if (isTopLevelAndFilter && getUnnestInputIfDirectAccess(unnestColumn) != null) {
            filterSplitter.addPreFilter(newFilter != null ? newFilter : filter);
          }
          filterSplitter.addToOriginalFilterCount(1);
        }
      } else {
        preFilterList.add(filter);
        // for filters on non unnest columns, we still need to count the nested filters if any as we are not traversing it in this method
        int filterCount = Filters.countNumberOfFilters(filter);
        filterSplitter.addToOriginalFilterCount(filterCount);
        filterSplitter.addToPreFilterCount(filterCount);
      }
    }
    return preFilterList;
  }
  /**
   * Returns the input of {@link #unnestColumn}, if it's a direct access; otherwise returns null.
   */
  @Nullable
  public String getUnnestInputIfDirectAccess(VirtualColumn unnestColumn)
  {
    if (unnestColumn instanceof ExpressionVirtualColumn) {
      return ((ExpressionVirtualColumn) unnestColumn).getParsedExpression().get().getBindingIfIdentifier();
    } else {
      return null;
    }
  }

  /**
   * Rewrites a filter on {@link #outputColumnName} to operate on the input column from
   * if possible.
   */
  @Nullable
  private Filter rewriteFilterOnUnnestColumnIfPossible(
      final Filter filter,
      @Nullable final String inputColumn,
      @Nullable final ColumnCapabilities inputColumnCapabilities
  )
  {
    // Only doing this for multi-value strings (not array types) at the moment.
    if (inputColumn == null
        || inputColumnCapabilities == null
        || inputColumnCapabilities.getType() != ValueType.STRING) {
      return null;
    }

    if (filterMapsOverMultiValueStrings(filter)) {
      return filter.rewriteRequiredColumns(ImmutableMap.of(outputColumnName, inputColumn));
    } else {
      return null;
    }
  }

  /**
   * Requirement for {@link #rewriteFilterOnUnnestColumnIfPossible}: filter must support rewrites and also must map
   * over multi-value strings. (Rather than treat them as arrays.) There isn't a method on the Filter interface that
   * tells us this, so resort to instanceof.
   */
  @VisibleForTesting
  static boolean filterMapsOverMultiValueStrings(final Filter filter)
  {
    if (filter instanceof BooleanFilter) {
      for (Filter child : ((BooleanFilter) filter).getFilters()) {
        if (!filterMapsOverMultiValueStrings(child)) {
          return false;
        }
      }
      return true;
    } else if (filter instanceof NotFilter) {
      return false;
    } else {
      return filter instanceof SelectorFilter
             || filter instanceof InDimFilter
             || filter instanceof LikeFilter
             || filter instanceof BoundFilter
             || filter instanceof NullFilter
             || filter instanceof EqualityFilter
             || filter instanceof RangeFilter;
    }
  }

  /**
   * Array and nested array columns are dictionary encoded, but not correctly for {@link UnnestDimensionCursor} which
   * is tailored for scalar logical type values that are {@link ColumnCapabilities#isDictionaryEncoded()} and possibly
   * with {@link ColumnCapabilities#hasMultipleValues()} (specifically {@link ValueType#STRING}), so we don't want to
   * use this cursor if the capabilities are unknown or if the column type is {@link ValueType#ARRAY}.
   */
  private static boolean useDimensionCursor(@Nullable ColumnCapabilities capabilities)
  {
    if (capabilities == null) {
      // capabilities being null here should be indicative of the column not existing or being a virtual column with
      // no type information, chances are it is not going to be using a very cool dimension selector and so wont work
      // with this, which requires real dictionary ids for the value matcher to work correctly
      return false;
    }
    // the column needs real, unique value dictionary so that the value matcher id lookup works correctly, otherwise
    // we must not use the dimension selector
    if (capabilities.isDictionaryEncoded().and(capabilities.areDictionaryValuesUnique()).isTrue()) {
      // if we got here, we only actually want to do this for dictionary encoded strings, since no other dictionary
      // encoded column type should ever have multiple values set. nested and array columns are also dictionary encoded,
      // but for arrays, the row is always a single dictionary id which maps to the entire array instead of an array
      // of ids for each element, so we don't want to ever use the dimension selector cursor for that
      return capabilities.is(ValueType.STRING);
    }
    // wasn't a dictionary encoded string, use the value selector
    return false;
  }
}
