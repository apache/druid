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

import com.google.common.collect.Lists;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.ListIndexed;
import org.apache.druid.segment.filter.AndFilter;
import org.apache.druid.segment.filter.Filters;
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
    final Pair<Filter, Filter> filterPair = computeBaseAndPostJoinFilters(filter, virtualColumns);

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
            if (capabilities.isDictionaryEncoded().and(capabilities.areDictionaryValuesUnique()).isTrue()) {
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

  private Pair<Filter, Filter> computeBaseAndPostJoinFilters(
      @Nullable final Filter queryFilter,
      final VirtualColumns queryVirtualColumns
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

        if (requiredColumns.contains(outputColumnName)) {
          postFilters.add(filter);
        } else {
          if (queryVirtualColumns.getVirtualColumns().length > 0) {
            for (String column : requiredColumns) {
              if (queryVirtualColumns.exists(column)) {
                postFilters.add(filter);
                return;
              }
            }
          }

          preFilters.add(filter);
        }
      }
    }

    final FilterSplitter filterSplitter = new FilterSplitter();

    if (allowSet != null && !allowSet.isEmpty()) {
      final String inputColumn = getUnnestInputIfDirectAccess();

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
}

