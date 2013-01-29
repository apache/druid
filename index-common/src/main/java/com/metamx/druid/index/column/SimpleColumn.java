/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.index.column;

import com.google.common.base.Supplier;
import com.google.common.io.Closeables;

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

  SimpleColumn(
      ColumnCapabilities capabilities,
      Supplier<DictionaryEncodedColumn> dictionaryEncodedColumn,
      Supplier<RunLengthColumn> runLengthColumn,
      Supplier<GenericColumn> genericColumn,
      Supplier<ComplexColumn> complexColumn,
      Supplier<BitmapIndex> bitmapIndex
  )
  {
    this.capabilities = capabilities;
    this.dictionaryEncodedColumn = dictionaryEncodedColumn;
    this.runLengthColumn = runLengthColumn;
    this.genericColumn = genericColumn;
    this.complexColumn = complexColumn;
    this.bitmapIndex = bitmapIndex;
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
      Closeables.closeQuietly(column);
    }
  }

  @Override
  public DictionaryEncodedColumn getDictionaryEncoding()
  {
    return dictionaryEncodedColumn.get();
  }

  @Override
  public RunLengthColumn getRunLengthColumn()
  {
    return runLengthColumn.get();
  }

  @Override
  public GenericColumn getGenericColumn()
  {
    return genericColumn.get();
  }

  @Override
  public ComplexColumn getComplexColumn()
  {
    return complexColumn.get();
  }

  @Override
  public BitmapIndex getBitmapIndex()
  {
    return bitmapIndex.get();
  }
}
