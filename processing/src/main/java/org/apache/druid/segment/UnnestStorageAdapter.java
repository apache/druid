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
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Objects;

/**
 * This class serves as the Storage Adapter for the Unnest Segment and is responsible for creating the cursors
 * If the column is dictionary encoded it creates {@link UnnestDimensionCursor} else {@link UnnestColumnValueSelectorCursor}
 * These cursors help navigate the segments for these cases
 */
public class UnnestStorageAdapter implements StorageAdapter
{
  private final StorageAdapter baseAdapter;
  private final String dimensionToUnnest;
  private final String outputColumnName;
  private final LinkedHashSet<String> allowSet;

  public UnnestStorageAdapter(
      final StorageAdapter baseAdapter,
      final String dimension,
      final String outputColumnName,
      final LinkedHashSet<String> allowSet
  )
  {
    this.baseAdapter = baseAdapter;
    this.dimensionToUnnest = dimension;
    this.outputColumnName = outputColumnName;
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
    Filter updatedFilter;
    if (allowSet != null && !allowSet.isEmpty()) {
      final InDimFilter allowListFilters;
      allowListFilters = new InDimFilter(dimensionToUnnest, allowSet);
      if (filter != null) {
        updatedFilter = new AndFilter(Arrays.asList(filter, allowListFilters));
      } else {
        updatedFilter = allowListFilters;
      }
    } else {
      updatedFilter = filter;
    }
    final Sequence<Cursor> baseCursorSequence = baseAdapter.makeCursors(
        updatedFilter,
        interval,
        virtualColumns,
        gran,
        descending,
        queryMetrics
    );

    return Sequences.map(
        baseCursorSequence,
        cursor -> {
          Objects.requireNonNull(cursor);
          Cursor retVal = cursor;
          ColumnCapabilities capabilities = cursor.getColumnSelectorFactory().getColumnCapabilities(dimensionToUnnest);
          if (capabilities != null) {
            if (capabilities.isDictionaryEncoded().and(capabilities.areDictionaryValuesUnique()).isTrue()) {
              retVal = new UnnestDimensionCursor(
                  retVal,
                  retVal.getColumnSelectorFactory(),
                  dimensionToUnnest,
                  outputColumnName,
                  allowSet
              );
            } else {
              retVal = new UnnestColumnValueSelectorCursor(
                  retVal,
                  retVal.getColumnSelectorFactory(),
                  dimensionToUnnest,
                  outputColumnName,
                  allowSet
              );
            }
          } else {
            retVal = new UnnestColumnValueSelectorCursor(
                retVal,
                retVal.getColumnSelectorFactory(),
                dimensionToUnnest,
                outputColumnName,
                allowSet
            );
          }
          return retVal;
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
    return baseAdapter.getDimensionCardinality(dimensionToUnnest);
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
    if (!outputColumnName.equals(column)) {
      return baseAdapter.getMinValue(column);
    }
    return baseAdapter.getMinValue(dimensionToUnnest);
  }

  @Nullable
  @Override
  public Comparable getMaxValue(String column)
  {
    if (!outputColumnName.equals(column)) {
      return baseAdapter.getMaxValue(column);
    }
    return baseAdapter.getMaxValue(dimensionToUnnest);
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    if (!outputColumnName.equals(column)) {
      return baseAdapter.getColumnCapabilities(column);
    }
    return baseAdapter.getColumnCapabilities(dimensionToUnnest);
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

  public String getDimensionToUnnest()
  {
    return dimensionToUnnest;
  }
}

