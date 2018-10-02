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

import com.google.common.base.Supplier;
import org.apache.druid.segment.selector.settable.SettableColumnValueSelector;

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

  SimpleColumnHolder(
      ColumnCapabilities capabilities,
      Supplier<? extends BaseColumn> columnSupplier,
      @Nullable Supplier<BitmapIndex> bitmapIndex,
      @Nullable Supplier<SpatialIndex> spatialIndex
  )
  {
    this.capabilities = capabilities;
    this.columnSupplier = columnSupplier;
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
    try (final NumericColumn column = (NumericColumn) columnSupplier.get()) {
      return column.length();
    }
  }

  @Override
  public BaseColumn getColumn()
  {
    return columnSupplier.get();
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
    return getCapabilities().getType().makeNewSettableColumnValueSelector();
  }
}
