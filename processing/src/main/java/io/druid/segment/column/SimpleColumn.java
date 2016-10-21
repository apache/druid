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
import io.druid.java.util.common.guava.CloseQuietly;

/**
 */
class SimpleColumn implements Column
{
  private final ColumnCapabilities capabilities;
  private final Supplier<DictionaryEncodedColumn> dictionaryEncodedColumn;
  private final Supplier<RunLengthColumn> runLengthColumn;
  private final Supplier<GenericColumn> genericColumn;
  private final Supplier<ComplexColumn> complexColumn;
  private final Supplier<BitmapIndex> bitmapIndex;
  private final Supplier<SpatialIndex> spatialIndex;

  SimpleColumn(
      ColumnCapabilities capabilities,
      Supplier<DictionaryEncodedColumn> dictionaryEncodedColumn,
      Supplier<RunLengthColumn> runLengthColumn,
      Supplier<GenericColumn> genericColumn,
      Supplier<ComplexColumn> complexColumn,
      Supplier<BitmapIndex> bitmapIndex,
      Supplier<SpatialIndex> spatialIndex
  )
  {
    this.capabilities = capabilities;
    this.dictionaryEncodedColumn = dictionaryEncodedColumn;
    this.runLengthColumn = runLengthColumn;
    this.genericColumn = genericColumn;
    this.complexColumn = complexColumn;
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
    GenericColumn column = null;
    try {
      column = genericColumn.get();
      return column.length();
    }
    finally {
      CloseQuietly.close(column);
    }
  }

  @Override
  public DictionaryEncodedColumn getDictionaryEncoding()
  {
    return dictionaryEncodedColumn == null ? null : dictionaryEncodedColumn.get();
  }

  @Override
  public RunLengthColumn getRunLengthColumn()
  {
    return runLengthColumn == null ? null : runLengthColumn.get();
  }

  @Override
  public GenericColumn getGenericColumn()
  {
    return genericColumn == null ? null : genericColumn.get();
  }

  @Override
  public ComplexColumn getComplexColumn()
  {
    return complexColumn == null ? null : complexColumn.get();
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
}
