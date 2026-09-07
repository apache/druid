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
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.filter.ColumnIndexSelector;
import org.apache.druid.segment.column.BaseColumn;
import org.apache.druid.segment.column.BaseColumnHolder;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.SelectableColumn;
import org.apache.druid.segment.selector.settable.SettableColumnValueSelector;

import javax.annotation.Nullable;
import java.util.HashMap;

/**
 * Wraps {@link QueryableIndex} and {@link VirtualColumns}, providing a unified view of physical and virtual columns,
 * as well as lifecycle management for physical columns.
 */
public class ColumnCache implements ColumnIndexSelector
{
  private final HashMap<String, ColumnHolder> holderCache;
  private final QueryableIndex index;
  private final VirtualColumns virtualColumns;
  private final Closer closer;

  public ColumnCache(
      QueryableIndex index,
      VirtualColumns virtualColumns,
      Closer closer
  )
  {
    this.index = index;
    this.closer = closer;
    this.virtualColumns = virtualColumns;

    this.holderCache = new HashMap<>();
  }

  @Nullable
  @Override
  public ColumnHolder getColumnHolder(String columnName)
  {
    return holderCache.computeIfAbsent(columnName, this::makeNewColumnHolder);
  }

  @Override
  public int getNumRows()
  {
    return index.getNumRows();
  }

  @Override
  public BitmapFactory getBitmapFactory()
  {
    return index.getBitmapFactoryForDimensions();
  }

  @Override
  @Nullable
  public ColumnIndexSupplier getIndexSupplier(String column)
  {
    final ColumnHolder holder = getColumnHolder(column);
    if (holder != null) {
      return holder.getIndexSupplier();
    } else {
      return null;
    }
  }

  @Nullable
  private ColumnHolder makeNewColumnHolder(final String columnName)
  {
    final VirtualColumn virtualColumn = virtualColumns.getVirtualColumn(columnName);
    if (virtualColumn != null) {
      final ColumnCapabilities capabilities = virtualColumn.capabilities(
          virtualColumns.wrapInspector(index),
          virtualColumn.getOutputName()
      );
      if (capabilities != null) {
        final SelectableColumn selectableColumn = virtualColumn.toSelectableColumn(this);
        return new VirtualColumnHolder(virtualColumn, capabilities, selectableColumn);
      } else {
        // Column wants to be treated like it doesn't exist.
        return null;
      }
    }

    final BaseColumnHolder holder = index.getColumnHolder(columnName);
    if (holder != null) {
      // Here we do a funny little dance to memoize the BaseColumn and register it with the closer.
      // It would probably be cleaner if the ColumnHolder itself was `Closeable` and did its own memoization,
      // but that change is much wider and runs the risk of even more things that need to close the thing
      // not actually closing it.  So, maybe this is a hack, maybe it's a wise decision, who knows, but at
      // least for now, we grab the holder, grab the column, register the column with the closer and then return
      // a new holder that always returns the same reference for the column.
      return new WrappedBaseColumnHolder(holder);
    }

    return null;
  }

  private class WrappedBaseColumnHolder implements BaseColumnHolder
  {
    private final BaseColumnHolder baseHolder;

    @Nullable
    private BaseColumn theColumn = null;

    WrappedBaseColumnHolder(BaseColumnHolder baseHolder)
    {
      this.baseHolder = baseHolder;
    }

    @Override
    public ColumnCapabilities getCapabilities()
    {
      return baseHolder.getCapabilities();
    }

    @Override
    public int getLength()
    {
      return baseHolder.getLength();
    }

    @Override
    public BaseColumn getColumn()
    {
      if (theColumn == null) {
        theColumn = closer.register(baseHolder.getColumn());
      }
      return theColumn;
    }

    @Nullable
    @Override
    public ColumnIndexSupplier getIndexSupplier()
    {
      return baseHolder.getIndexSupplier();
    }

    @Override
    public SettableColumnValueSelector makeNewSettableColumnValueSelector()
    {
      return baseHolder.makeNewSettableColumnValueSelector();
    }
  }

  private class VirtualColumnHolder implements ColumnHolder
  {
    private final VirtualColumn virtualColumn;
    private final ColumnCapabilities capabilities;
    private final SelectableColumn selectableColumn;

    VirtualColumnHolder(
        final VirtualColumn virtualColumn,
        final ColumnCapabilities capabilities,
        final SelectableColumn selectableColumn
    )
    {
      this.virtualColumn = virtualColumn;
      this.capabilities = capabilities;
      this.selectableColumn = selectableColumn;
    }

    @Override
    public ColumnCapabilities getCapabilities()
    {
      return capabilities;
    }

    @Override
    public int getLength()
    {
      return index.getNumRows();
    }

    @Override
    public SelectableColumn getColumn()
    {
      return selectableColumn;
    }

    @Override
    @Nullable
    public ColumnIndexSupplier getIndexSupplier()
    {
      return virtualColumn.getIndexSupplier(virtualColumn.getOutputName(), ColumnCache.this);
    }

    @Override
    public SettableColumnValueSelector makeNewSettableColumnValueSelector()
    {
      throw DruidException.defensive("Not implemented. This method is expected to only be used with physical columns.");
    }
  }
}
