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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.filter.BooleanFilter;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.ListIndexed;
import org.apache.druid.segment.filter.AndFilter;
import org.apache.druid.segment.filter.BoundFilter;
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.segment.filter.LikeFilter;
import org.apache.druid.segment.filter.NotFilter;
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
  private final StorageAdapter baseAdapter;
  private final VirtualColumn unnestColumn;
  private final String outputColumnName;
  private final LinkedHashSet<String> allowSet;

  public UnnestStorageAdapter(
      final StorageAdapter baseAdapter,
      final VirtualColumn unnestColumn,
      final LinkedHashSet<String> allowSet
  )
  {
    this.baseAdapter = baseAdapter;
    this.unnestColumn = unnestColumn;
    this.outputColumnName = unnestColumn.getOutputName();
    this.allowSet = allowSet;
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
    final String inputColumn = getUnnestInputIfDirectAccess();
    final Pair<Filter, Filter> filterPair = computeBaseAndPostCorrelateFilters(
        filter,
        virtualColumns,
        inputColumn,
        inputColumn == null || virtualColumns.exists(inputColumn)
        ? null
        : baseAdapter.getColumnCapabilities(inputColumn)
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
          Cursor retVal = cursor;
          ColumnCapabilities capabilities = unnestColumn.capabilities(
              cursor.getColumnSelectorFactory(),
              unnestColumn.getOutputName()
          );
          if (capabilities != null) {
            if (!capabilities.isArray() && capabilities.isDictionaryEncoded().and(capabilities.areDictionaryValuesUnique()).isTrue()) {
              retVal = new UnnestDimensionCursor(
                  retVal,
                  retVal.getColumnSelectorFactory(),
                  unnestColumn,
                  outputColumnName,
                  allowSet
              );
            } else {
              retVal = new UnnestColumnValueSelectorCursor(
                  retVal,
                  retVal.getColumnSelectorFactory(),
                  unnestColumn,
                  outputColumnName,
                  allowSet
              );
            }
          } else {
            retVal = new UnnestColumnValueSelectorCursor(
                retVal,
                retVal.getColumnSelectorFactory(),
                unnestColumn,
                outputColumnName,
                allowSet
            );
          }
          return PostJoinCursor.wrap(
              retVal,
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
   * @param queryVirtualColumns    query virtual columns passed to makeCursors
   * @param inputColumn            input column to unnest if it's a direct access; otherwise null
   * @param inputColumnCapabilites input column capabilities if known; otherwise null
   *
   * @return pair of pre- and post-correlate filters
   */
  private Pair<Filter, Filter> computeBaseAndPostCorrelateFilters(
      @Nullable final Filter queryFilter,
      final VirtualColumns queryVirtualColumns,
      @Nullable final String inputColumn,
      @Nullable final ColumnCapabilities inputColumnCapabilites
  )
  {
    class FilterSplitter
    {
      final List<Filter> preFilters = new ArrayList<>();
      final List<Filter> postFilters = new ArrayList<>();

      void add(@Nullable final Filter filter)
      {
        if (filter == null) {
          return;
        }

        final Set<String> requiredColumns = filter.getRequiredColumns();

        // Run filter post-correlate if it refers to any virtual columns.
        if (queryVirtualColumns.getVirtualColumns().length > 0) {
          for (String column : requiredColumns) {
            if (queryVirtualColumns.exists(column)) {
              postFilters.add(filter);
              return;
            }
          }
        }

        if (requiredColumns.contains(outputColumnName)) {
          // Try to move filter pre-correlate if possible.
          final Filter newFilter = rewriteFilterOnUnnestColumnIfPossible(filter, inputColumn, inputColumnCapabilites);
          if (newFilter != null) {
            preFilters.add(newFilter);
          } else {
            postFilters.add(filter);
          }
        } else {
          preFilters.add(filter);
        }
      }
    }

    final FilterSplitter filterSplitter = new FilterSplitter();

    if (allowSet != null && !allowSet.isEmpty()) {
      // Filter on input column if possible (it may be faster); otherwise use output column.
      filterSplitter.add(new InDimFilter(inputColumn != null ? inputColumn : outputColumnName, allowSet));
    }

    if (queryFilter instanceof AndFilter) {
      for (Filter filter : ((AndFilter) queryFilter).getFilters()) {
        filterSplitter.add(filter);
      }
    } else {
      filterSplitter.add(queryFilter);
    }

    return Pair.of(
        Filters.maybeAnd(filterSplitter.preFilters).orElse(null),
        Filters.maybeAnd(filterSplitter.postFilters).orElse(null)
    );
  }

  /**
   * Returns the input of {@link #unnestColumn}, if it's a direct access; otherwise returns null.
   */
  @Nullable
  private String getUnnestInputIfDirectAccess()
  {
    if (unnestColumn instanceof ExpressionVirtualColumn) {
      return ((ExpressionVirtualColumn) unnestColumn).getParsedExpression().get().getBindingIfIdentifier();
    } else {
      return null;
    }
  }

  /**
   * Rewrites a filter on {@link #outputColumnName} to operate on the input column from
   * {@link #getUnnestInputIfDirectAccess()}, if possible.
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
  private static boolean filterMapsOverMultiValueStrings(final Filter filter)
  {
    if (filter instanceof BooleanFilter) {
      for (Filter child : ((BooleanFilter) filter).getFilters()) {
        if (!filterMapsOverMultiValueStrings(child)) {
          return false;
        }
      }

      return true;
    } else if (filter instanceof NotFilter) {
      return filterMapsOverMultiValueStrings(((NotFilter) filter).getBaseFilter());
    } else {
      return filter instanceof SelectorFilter
             || filter instanceof InDimFilter
             || filter instanceof LikeFilter
             || filter instanceof BoundFilter;
    }
  }
}
