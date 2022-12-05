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

package org.apache.druid.query.rowsandcols;

import org.apache.druid.query.rowsandcols.column.Column;
import org.apache.druid.query.rowsandcols.column.ColumnAccessor;
import org.apache.druid.query.rowsandcols.column.ObjectColumnAccessorBase;
import org.apache.druid.segment.RowAdapter;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Optional;
import java.util.function.Function;

public class ArrayListRowsAndColumns<RowType> implements RowsAndColumns
{
  private final ArrayList<RowType> rows;
  private final RowAdapter<RowType> rowAdapter;
  private final RowSignature rowSignature;

  public ArrayListRowsAndColumns(
      ArrayList<RowType> rows,
      RowAdapter<RowType> rowAdapter,
      RowSignature rowSignature
  )
  {
    this.rows = rows;
    this.rowAdapter = rowAdapter;
    this.rowSignature = rowSignature;
  }

  @Override
  public Collection<String> getColumnNames()
  {
    return rowSignature.getColumnNames();
  }

  @Override
  public int numRows()
  {
    return rows.size();
  }

  @Override
  @Nullable
  public Column findColumn(String name)
  {
    if (!rowSignature.contains(name)) {
      return null;
    }

    final Function<RowType, Object> adapterForValue = rowAdapter.columnFunction(name);
    final Optional<ColumnType> maybeColumnType = rowSignature.getColumnType(name);
    final ColumnType columnType = maybeColumnType.orElse(ColumnType.UNKNOWN_COMPLEX);
    final Comparator<Object> comparator = Comparator.nullsFirst(columnType.getStrategy());

    return new Column()
    {
      @Override
      public ColumnAccessor toAccessor()
      {
        return new ObjectColumnAccessorBase()
        {
          @Override
          protected Object getVal(int cell)
          {
            return adapterForValue.apply(rows.get(cell));
          }

          @Override
          protected Comparator<Object> getComparator()
          {
            return comparator;
          }

          @Override
          public ColumnType getType()
          {
            return columnType;
          }

          @Override
          public int numRows()
          {
            return rows.size();
          }
        };
      }

      @Override
      public <T> T as(Class<? extends T> clazz)
      {
        return null;
      }
    };
  }

  @Nullable
  @Override
  public <T> T as(Class<T> clazz)
  {
    return null;
  }
}
