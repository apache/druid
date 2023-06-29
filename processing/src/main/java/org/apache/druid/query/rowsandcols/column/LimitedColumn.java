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

package org.apache.druid.query.rowsandcols.column;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.operator.window.value.ShiftedColumnAccessorBase;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class LimitedColumn implements Column
{
  private final Column column;
  private final int start;
  private final int end;

  public LimitedColumn(Column column, int start, int end)
  {
    this.column = column;
    this.start = start;
    this.end = end;
  }

  @Nonnull
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
      protected int getActualValue(int rowNum)
      {
        int retVal = start + rowNum;
        if (retVal >= end) {
          throw new ISE("Index out of bounds[%d] >= [%d], start[%s]", retVal, end, start);
        }
        return retVal;
      }

      @Override
      protected boolean outsideBounds(int rowNum)
      {
        return false;
      }
    };
  }

  @Nullable
  @Override
  public <T> T as(Class<? extends T> clazz)
  {
    return null;
  }
}
