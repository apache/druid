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

package org.apache.druid.segment.column;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.selector.settable.SettableColumnValueSelector;
import org.apache.druid.segment.selector.settable.SettableObjectColumnValueSelector;

import javax.annotation.Nullable;

/**
 */
class SimpleColumnHolder implements ColumnHolder
{
  private final ColumnCapabilities capabilities;
  private final Supplier<? extends BaseColumn> columnSupplier;
  @Nullable
  private final Supplier<BitmapIndex> bitmapIndex;
  @Nullable
  private final Supplier<SpatialIndex> spatialIndex;
  private static final InvalidComplexColumnTypeValueSelector INVALID_COMPLEX_COLUMN_TYPE_VALUE_SELECTOR
      = new InvalidComplexColumnTypeValueSelector();

  SimpleColumnHolder(
      ColumnCapabilities capabilities,
      @Nullable Supplier<? extends BaseColumn> columnSupplier,
      @Nullable Supplier<BitmapIndex> bitmapIndex,
      @Nullable Supplier<SpatialIndex> spatialIndex
  )
  {
    this.capabilities = capabilities;
    this.columnSupplier = columnSupplier;
    // ColumnSupplier being null is sort of a rare case but can happen when a segment
    // was created, for example, using an aggregator that was removed in later versions.
    // In such cases we are not able to deserialize the column metadata and determine
    // the column supplier.
    // For now, below check allows column supplier to be null only for complex types
    // columns as they are the ones (especially aggregators in extensions-contrib) that
    // are prone to such backward incompatible changes.
    if (columnSupplier == null) {
      Preconditions.checkArgument(
          capabilities.getType() == ValueType.COMPLEX,
          "Only complex column types can have nullable column suppliers"
      );
    }
    this.bitmapIndex = bitmapIndex;
    this.spatialIndex = spatialIndex;
  }

  @Override
  public ColumnCapabilities getCapabilities()
  {
    return capabilities;
  }

  @Override
  public int getLength()
  {
    // Not checking for null here since columnSupplier is expected to be
    // not null for numeric columns
    try (final NumericColumn column = (NumericColumn) columnSupplier.get()) {
      return column.length();
    }
  }

  @Override
  public BaseColumn getColumn()
  {
    return columnSupplier == null ? UnknownTypeComplexColumn.instance() : columnSupplier.get();
  }

  @Nullable
  @Override
  public BitmapIndex getBitmapIndex()
  {
    return bitmapIndex == null ? null : bitmapIndex.get();
  }

  @Nullable
  @Override
  public SpatialIndex getSpatialIndex()
  {
    return spatialIndex == null ? null : spatialIndex.get();
  }

  @Override
  public SettableColumnValueSelector makeNewSettableColumnValueSelector()
  {
    if (columnSupplier == null) {
      return INVALID_COMPLEX_COLUMN_TYPE_VALUE_SELECTOR;
    }
    return getCapabilities().getType().makeNewSettableColumnValueSelector();
  }

  private static class InvalidComplexColumnTypeValueSelector extends SettableObjectColumnValueSelector
  {
    @Override
    public void setValueFrom(ColumnValueSelector selector)
    {
      // no-op
    }
    @Nullable
    @Override
    public Object getObject()
    {
      return UnknownTypeComplexColumn.instance().getRowValue(0);
    }
  }

}
