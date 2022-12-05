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

import org.apache.druid.segment.column.ColumnType;

public class IntArrayColumn implements Column
{
  private final int[] vals;

  public IntArrayColumn(
      int[] vals
  )
  {
    this.vals = vals;
  }

  @Override
  public ColumnAccessor toAccessor()
  {
    return new ColumnAccessor()
    {
      @Override
      public ColumnType getType()
      {
        return ColumnType.LONG;
      }

      @Override
      public int numRows()
      {
        return vals.length;
      }

      @Override
      public boolean isNull(int rowNum)
      {
        return false;
      }

      @Override
      public Object getObject(int rowNum)
      {
        return vals[rowNum];
      }

      @Override
      public double getDouble(int rowNum)
      {
        return vals[rowNum];
      }

      @Override
      public float getFloat(int rowNum)
      {
        return vals[rowNum];
      }

      @Override
      public long getLong(int rowNum)
      {
        return vals[rowNum];
      }

      @Override
      public int getInt(int rowNum)
      {
        return vals[rowNum];
      }

      @Override
      public int compareCells(int lhsRowNum, int rhsRowNum)
      {
        return Integer.compare(vals[lhsRowNum], vals[rhsRowNum]);
      }
    };
  }

  @Override
  public <T> T as(Class<? extends T> clazz)
  {
    return null;
  }
}
