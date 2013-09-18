/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
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
