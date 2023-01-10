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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;

public class ConstantObjectColumn implements Column
{
  private final Object obj;
  private final int numRows;
  private final ColumnType type;

  public ConstantObjectColumn(Object obj, int numRows, ColumnType type)
  {
    this.obj = obj;
    this.numRows = numRows;
    this.type = type;
  }

  @Nonnull
  @Override
  public ColumnAccessor toAccessor()
  {
    return new ColumnAccessor()
    {
      @Override
      public ColumnType getType()
      {
        return type;
      }

      @Override
      public int numRows()
      {
        return numRows;
      }

      @Override
      public boolean isNull(int rowNum)
      {
        return obj == null;
      }

      @Override
      public Object getObject(int rowNum)
      {
        return obj;
      }

      @Override
      public double getDouble(int rowNum)
      {
        return ((Number) obj).doubleValue();
      }

      @Override
      public float getFloat(int rowNum)
      {
        return ((Number) obj).floatValue();
      }

      @Override
      public long getLong(int rowNum)
      {
        return ((Number) obj).longValue();
      }

      @Override
      public int getInt(int rowNum)
      {
        return ((Number) obj).intValue();
      }

      @Override
      public int compareRows(int lhsRowNum, int rhsRowNum)
      {
        return 0;
      }
    };
  }

  @Nullable
  @SuppressWarnings("unchecked")
  @Override
  public <T> T as(Class<? extends T> clazz)
  {
    if (VectorCopier.class.equals(clazz)) {
      return (T) (VectorCopier) (into, intoStart) -> Arrays.fill(into, intoStart, intoStart + numRows, obj);
    }
    if (ColumnValueSwapper.class.equals(clazz)) {
      return (T) (ColumnValueSwapper) (lhs, rhs) -> {
      };
    }

    return null;
  }
}
