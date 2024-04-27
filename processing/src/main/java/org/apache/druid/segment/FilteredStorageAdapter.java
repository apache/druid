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

import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.filter.AndFilter;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;

public class FilteredStorageAdapter implements StorageAdapter
{
  private final DimFilter filterOnDataSource;
  private final StorageAdapter baseStorageAdapter;

  public FilteredStorageAdapter(final StorageAdapter adapter, final DimFilter filter)
  {
    this.baseStorageAdapter = adapter;
    this.filterOnDataSource = filter;
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
    final Filter andFilter;
    if (filter == null) {
      if (filterOnDataSource != null) {
        andFilter = filterOnDataSource.toFilter();
      } else {
        andFilter = null;
      }
    } else {
      andFilter = new AndFilter(ImmutableList.of(filter, filterOnDataSource.toFilter()));
    }
    return baseStorageAdapter.makeCursors(andFilter, interval, virtualColumns, gran, descending, queryMetrics);
  }

  @Override
  public Interval getInterval()
  {
    return baseStorageAdapter.getInterval();
  }

  @Override
  public Indexed<String> getAvailableDimensions()
  {
    return baseStorageAdapter.getAvailableDimensions();
  }

  @Override
  public Iterable<String> getAvailableMetrics()
  {
    return baseStorageAdapter.getAvailableMetrics();
  }

  @Override
  public int getDimensionCardinality(String column)
  {
    return baseStorageAdapter.getDimensionCardinality(column);
  }

  @Override
  public DateTime getMinTime()
  {
    return baseStorageAdapter.getMinTime();
  }

  @Override
  public DateTime getMaxTime()
  {
    return baseStorageAdapter.getMaxTime();
  }

  @Nullable
  @Override
  public Comparable getMinValue(String column)
  {
    return null;
  }

  @Nullable
  @Override
  public Comparable getMaxValue(String column)
  {
    return null;
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    return baseStorageAdapter.getColumnCapabilities(column);
  }

  @Override
  public int getNumRows()
  {
    return 0;
  }

  @Override
  public DateTime getMaxIngestedEventTime()
  {
    return baseStorageAdapter.getMaxIngestedEventTime();
  }

  @Nullable
  @Override
  public Metadata getMetadata()
  {
    return baseStorageAdapter.getMetadata();
  }
}
