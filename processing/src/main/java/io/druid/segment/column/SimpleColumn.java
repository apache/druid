/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.column;

import com.google.common.base.Supplier;
import io.druid.segment.selector.settable.SettableColumnValueSelector;

/**
 */
class SimpleColumn implements Column
{
  private final ColumnCapabilities capabilities;
  private final Supplier<? extends BaseColumn> columnSupplier;
  private final Supplier<BitmapIndex> bitmapIndex;
  private final Supplier<SpatialIndex> spatialIndex;

  SimpleColumn(
      ColumnCapabilities capabilities,
      Supplier<? extends BaseColumn> columnSupplier,
      Supplier<BitmapIndex> bitmapIndex,
      Supplier<SpatialIndex> spatialIndex
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
  public DictionaryEncodedColumn getDictionaryEncoding()
  {
    return (DictionaryEncodedColumn) columnSupplier.get();
  }

  @Override
  public NumericColumn getNumericColumn()
  {
    return (NumericColumn) columnSupplier.get();
  }

  @Override
  public ComplexColumn getComplexColumn()
  {
    return (ComplexColumn) columnSupplier.get();
  }

  @Override
  public BitmapIndex getBitmapIndex()
  {
    return bitmapIndex == null ? null : bitmapIndex.get();
  }

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
