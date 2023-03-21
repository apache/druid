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
import org.apache.druid.query.rowsandcols.util.FindResult;
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
    return new ConstantColumnAccessor();
  }

  @Nullable
  @SuppressWarnings("unchecked")
  @Override
  public <T> T as(Class<? extends T> clazz)
  {
    if (VectorCopier.class.equals(clazz)) {
      return (T) (VectorCopier) (into, intoStart) -> {
        if (Integer.MAX_VALUE - numRows < intoStart) {
          throw new ISE("too many rows!!! intoStart[%,d], numRows[%,d] combine to exceed max_int", intoStart, numRows);
        }
        Arrays.fill(into, intoStart, intoStart + numRows, obj);
      };
    }
    if (ColumnValueSwapper.class.equals(clazz)) {
      return (T) (ColumnValueSwapper) (lhs, rhs) -> {
      };
    }

    return null;
  }

  private class ConstantColumnAccessor implements BinarySearchableAccessor
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

    @Override
    public FindResult findNull(int startIndex, int endIndex)
    {
      return findComplex(startIndex, endIndex, null);
    }

    @Override
    public FindResult findDouble(int startIndex, int endIndex, double val)
    {
      return findComplex(startIndex, endIndex, val);
    }

    @Override
    public FindResult findFloat(int startIndex, int endIndex, float val)
    {
      return findComplex(startIndex, endIndex, val);
    }

    @Override
    public FindResult findLong(int startIndex, int endIndex, long val)
    {
      return findComplex(startIndex, endIndex, val);
    }

    @Override
    public FindResult findString(int startIndex, int endIndex, String val)
    {
      return findComplex(startIndex, endIndex, val);
    }

    @Override
    public FindResult findComplex(int startIndex, int endIndex, Object val)
    {
      final boolean same;
      if (obj == null) {
        same = val == null;
      } else {
        same = obj.equals(val);
      }

      return same ? FindResult.found(startIndex, endIndex) : FindResult.notFound(endIndex);
    }
  }
}
