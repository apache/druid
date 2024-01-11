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
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.NumericColumn;

import javax.annotation.Nullable;

/**
 *
 */
public class ColumnSelectorColumnIndexSelector implements ColumnIndexSelector
{
  private final BitmapFactory bitmapFactory;
  private final VirtualColumns virtualColumns;
  private final ColumnSelector columnSelector;

  public ColumnSelectorColumnIndexSelector(
      final BitmapFactory bitmapFactory,
      final VirtualColumns virtualColumns,
      final ColumnSelector index
  )
  {
    this.bitmapFactory = bitmapFactory;
    this.virtualColumns = virtualColumns;
    this.columnSelector = index;
  }

  @Override
  public int getNumRows()
  {
    // This closes the __time column, which may initially seem like good behavior, but in reality leads to a
    // double-close when columnSelector is a ColumnCache (because all columns from a ColumnCache are closed when
    // the cache itself is closed). We're closing it here anyway, however, for two reasons:
    //
    //   1) Sometimes, columnSelector is DeprecatedQueryableIndexColumnSelector, not ColumnCache, and so the close
    //      here is important.
    //   2) Double-close is OK for the __time column when this method is expected to be used: the __time column is
    //      expected to be a ColumnarLongs, which is safe to double-close.

    try (final NumericColumn column = (NumericColumn) columnSelector.getColumnHolder(ColumnHolder.TIME_COLUMN_NAME)
                                                                    .getColumn()) {
      return column.length();
    }
  }

  @Override
  public BitmapFactory getBitmapFactory()
  {
    return bitmapFactory;
  }

  @Override
  public ColumnIndexSupplier getIndexSupplier(String column)
  {
    final ColumnIndexSupplier indexSupplier;
    if (isVirtualColumn(column)) {
      indexSupplier = virtualColumns.getIndexSupplier(column, this);
    } else {
      final ColumnHolder columnHolder = columnSelector.getColumnHolder(column);
      // for missing columns we return null here. This allows callers to fabricate an 'all true' or 'all false'
      // index so that filters which match the values can still use "indexes".
      if (columnHolder == null) {
        return null;
      }
      indexSupplier = columnHolder.getIndexSupplier();
    }
    return indexSupplier;
  }

  @Nullable
  @Override
  public ColumnHolder getColumnHolder(String columnName)
  {
    if (isVirtualColumn(columnName)) {
      return null;
    }
    return columnSelector.getColumnHolder(columnName);
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    return virtualColumns.getColumnCapabilitiesWithFallback(columnSelector, column);
  }

  private boolean isVirtualColumn(final String columnName)
  {
    return virtualColumns.getVirtualColumn(columnName) != null;
  }
}
