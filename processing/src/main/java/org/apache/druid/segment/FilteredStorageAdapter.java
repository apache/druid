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
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.filter.AndFilter;
import org.joda.time.Interval;

import javax.annotation.Nullable;

public class FilteredStorageAdapter implements StorageAdapter
{
  @Nullable
  private final DimFilter filterOnDataSource;
  private final StorageAdapter baseStorageAdapter;

  public FilteredStorageAdapter(final StorageAdapter adapter, @Nullable final DimFilter filter)
  {
    this.baseStorageAdapter = adapter;
    this.filterOnDataSource = filter;
  }

  @Override
  public CursorHolder makeCursorHolder(CursorBuildSpec spec)
  {
    final CursorBuildSpec.CursorBuildSpecBuilder buildSpecBuilder = CursorBuildSpec.builder(spec);
    final Filter newFilter;
    if (spec.getFilter() == null) {
      if (filterOnDataSource != null) {
        newFilter = filterOnDataSource.toFilter();
      } else {
        newFilter = null;
      }
    } else {
      if (filterOnDataSource != null) {
        newFilter = new AndFilter(ImmutableList.of(spec.getFilter(), filterOnDataSource.toFilter()));
      } else {
        newFilter = spec.getFilter();
      }
    }
    buildSpecBuilder.setFilter(newFilter);
    return baseStorageAdapter.makeCursorHolder(buildSpecBuilder.build());
  }

  @Override
  public Interval getInterval()
  {
    return baseStorageAdapter.getInterval();
  }

  @Override
  public RowSignature getRowSignature()
  {
    return baseStorageAdapter.getRowSignature();
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

  @Nullable
  @Override
  public Metadata getMetadata()
  {
    return baseStorageAdapter.getMetadata();
  }

  @Override
  public boolean isFromTombstone()
  {
    return baseStorageAdapter.isFromTombstone();
  }
}
