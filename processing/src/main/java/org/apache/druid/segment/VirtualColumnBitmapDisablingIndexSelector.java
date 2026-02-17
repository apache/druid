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
import org.apache.druid.segment.column.SelectableColumn;
import org.apache.druid.segment.selector.settable.SettableColumnValueSelector;
import org.apache.druid.segment.serde.NoIndexesColumnIndexSupplier;

import javax.annotation.Nullable;
import java.util.Objects;

public final class VirtualColumnBitmapDisablingIndexSelector implements ColumnIndexSelector
{
  private static final ColumnIndexSupplier NO_INDEXES = NoIndexesColumnIndexSupplier.getInstance();
  private final ColumnIndexSelector delegate;
  private final VirtualColumns virtualColumns;


  public VirtualColumnBitmapDisablingIndexSelector(
      final ColumnIndexSelector delegate,
      final VirtualColumns virtualColumns
  )
  {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
    this.virtualColumns = Objects.requireNonNull(virtualColumns, "virtualColumns");
  }

  @Override
  public int getNumRows()
  {
    return delegate.getNumRows();
  }

  @Override
  public BitmapFactory getBitmapFactory()
  {
    return delegate.getBitmapFactory();
  }

  @Override
  @Nullable
  public ColumnHolder getColumnHolder(final String columnName)
  {
    final ColumnHolder holder = delegate.getColumnHolder(columnName);
    if (holder == null || !isVirtual(columnName)) {
      return holder;
    }

    // force no indexes even if callers go via getColumnHolder().
    return new ColumnHolder()
    {
      @Override
      public ColumnCapabilities getCapabilities()
      {
        return holder.getCapabilities();
      }

      @Override
      public int getLength()
      {
        return holder.getLength();
      }

      @Override
      public SelectableColumn getColumn()
      {
        return holder.getColumn();
      }

      @Override
      public ColumnIndexSupplier getIndexSupplier()
      {
        return NO_INDEXES;
      }

      @Override
      public SettableColumnValueSelector makeNewSettableColumnValueSelector()
      {
        return holder.makeNewSettableColumnValueSelector();
      }
    };
  }

  @Override
  @Nullable
  public ColumnIndexSupplier getIndexSupplier(final String column)
  {
    if (column.isEmpty()) {
      return null;
    }

    // Only disable indexes for virtual columns
    if (isVirtual(column)) {
      return NO_INDEXES;
    }
    return delegate.getIndexSupplier(column);
  }


  private boolean isVirtual(final String columnName)
  {
    return virtualColumns.getVirtualColumn(columnName) != null;
  }
}
