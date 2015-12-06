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

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;

/**
 */
public class ColumnBuilder
{
  private ValueType type = null;
  private boolean hasMultipleValues = false;

  private Supplier<DictionaryEncodedColumn> dictionaryEncodedColumn = null;
  private Supplier<RunLengthColumn> runLengthColumn = null;
  private Supplier<GenericColumn> genericColumn = null;
  private Supplier<ComplexColumn> complexColumn = null;
  private Supplier<BitmapIndex> bitmapIndex = null;
  private Supplier<SpatialIndex> spatialIndex = null;

  public ColumnBuilder setType(ValueType type)
  {
    this.type = type;
    return this;
  }

  public ColumnBuilder setHasMultipleValues(boolean hasMultipleValues)
  {
    this.hasMultipleValues = hasMultipleValues;
    return this;
  }

  public ColumnBuilder setDictionaryEncodedColumn(Supplier<DictionaryEncodedColumn> dictionaryEncodedColumn)
  {
    this.dictionaryEncodedColumn = dictionaryEncodedColumn;
    return this;
  }

  public ColumnBuilder setRunLengthColumn(Supplier<RunLengthColumn> runLengthColumn)
  {
    this.runLengthColumn = runLengthColumn;
    return this;
  }

  public ColumnBuilder setGenericColumn(Supplier<GenericColumn> genericColumn)
  {
    this.genericColumn = genericColumn;
    return this;
  }

  public ColumnBuilder setComplexColumn(Supplier<ComplexColumn> complexColumn)
  {
    this.complexColumn = complexColumn;
    return this;
  }

  public ColumnBuilder setBitmapIndex(Supplier<BitmapIndex> bitmapIndex)
  {
    this.bitmapIndex = bitmapIndex;
    return this;
  }

  public ColumnBuilder setSpatialIndex(Supplier<SpatialIndex> spatialIndex)
  {
    this.spatialIndex = spatialIndex;
    return this;
  }

  public Column build()
  {
    Preconditions.checkState(type != null, "Type must be set.");

    return new SimpleColumn(
        new ColumnCapabilitiesImpl()
            .setType(type)
            .setDictionaryEncoded(dictionaryEncodedColumn != null)
            .setHasBitmapIndexes(bitmapIndex != null)
            .setHasSpatialIndexes(spatialIndex != null)
            .setRunLengthEncoded(runLengthColumn != null)
            .setHasMultipleValues(hasMultipleValues)
        ,
        dictionaryEncodedColumn,
        runLengthColumn,
        genericColumn,
        complexColumn,
        bitmapIndex,
        spatialIndex
    );
  }
}
