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

import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.query.filter.ColumnIndexSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnIndexCapabilities;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.NumericColumn;
import org.apache.druid.segment.column.SimpleColumnIndexCapabilities;

import javax.annotation.Nullable;

/**
 */
public class ColumnSelectorColumnIndexSelector implements ColumnIndexSelector
{
  private final BitmapFactory bitmapFactory;
  private final VirtualColumns virtualColumns;
  private final ColumnSelector index;

  public ColumnSelectorColumnIndexSelector(
      final BitmapFactory bitmapFactory,
      final VirtualColumns virtualColumns,
      final ColumnSelector index
  )
  {
    this.bitmapFactory = bitmapFactory;
    this.virtualColumns = virtualColumns;
    this.index = index;
  }

  @Override
  public int getNumRows()
  {
    try (final NumericColumn column = (NumericColumn) index.getColumnHolder(ColumnHolder.TIME_COLUMN_NAME).getColumn()) {
      return column.length();
    }
  }

  @Override
  public BitmapFactory getBitmapFactory()
  {
    return bitmapFactory;
  }

  @Nullable
  @Override
  public <T> ColumnIndexCapabilities getIndexCapabilities(
      String column,
      Class<T> clazz
  )
  {
    if (isVirtualColumn(column)) {
      return virtualColumns.getIndexCapabilities(column, index, clazz);
    }

    final ColumnHolder columnHolder = index.getColumnHolder(column);
    if (columnHolder == null || !columnHolder.getCapabilities().isFilterable()) {
      // if a column doesn't exist or isn't filterable, return true so that callers can use a value matcher to
      // either make an all true or all false bitmap if the matcher matches null
      return new SimpleColumnIndexCapabilities(true, true);
    }
    final ColumnIndexSupplier indexSupplier = columnHolder.getIndexSupplier();
    if (indexSupplier == null) {
      // column exists, but doesn't have an index supplier
      return null;
    }
    return indexSupplier.getIndexCapabilities(clazz);
  }

  @Nullable
  @Override
  public <T> T as(String column, Class<T> clazz)
  {
    if (isVirtualColumn(column)) {
      return virtualColumns.getIndex(column, index, clazz);
    }

    final ColumnHolder columnHolder = index.getColumnHolder(column);
    if (columnHolder == null || !columnHolder.getCapabilities().isFilterable()) {
      return null;
    }
    final ColumnIndexSupplier indexSupplier = columnHolder.getIndexSupplier();
    if (indexSupplier == null) {
      return null;
    }
    return indexSupplier.getIndex(clazz);
  }

  private boolean isVirtualColumn(final String columnName)
  {
    return virtualColumns.getVirtualColumn(columnName) != null;
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    return virtualColumns.getColumnCapabilities(index, column);
  }
}
