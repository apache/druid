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

package org.apache.druid.query.operator;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.operator.window.value.ShiftedColumnAccessorBase;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.Column;
import org.apache.druid.query.rowsandcols.column.ColumnAccessor;

import javax.annotation.Nullable;
import java.util.Collection;

public class LimitedRowsAndColumns implements RowsAndColumns
{
  private final RowsAndColumns rac;
  private final int start;
  private final int end;

  public LimitedRowsAndColumns(RowsAndColumns rac, int start, int end)
  {
    final int numRows = rac.numRows();
    if (numRows < end) {
      throw new ISE("end[%d] is out of bounds, cannot be greater than numRows[%d]", end, numRows);
    }

    this.rac = rac;
    this.start = start;
    this.end = end;
  }

  @Override
  public Collection<String> getColumnNames()
  {
    return rac.getColumnNames();
  }

  @Override
  public int numRows()
  {
    return end - start;
  }

  @Override
  public Column findColumn(String name)
  {
    final Column column = rac.findColumn(name);
    if (column == null) {
      return null;
    }

    return new Column()
    {
      @Override
      public ColumnAccessor toAccessor()
      {
        final ColumnAccessor columnAccessor = column.toAccessor();
        return new ShiftedColumnAccessorBase(columnAccessor)
        {
          @Override
          public int numRows()
          {
            return end - start;
          }

          @Override
          protected int getActualCell(int cell)
          {
            int retVal = start + cell;
            if (retVal >= end) {
              throw new ISE("Index out of bounds[%d] >= [%d], start[%s]", retVal, end, start);
            }
            return retVal;
          }

          @Override
          protected boolean outsideBounds(int cell)
          {
            return false;
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
