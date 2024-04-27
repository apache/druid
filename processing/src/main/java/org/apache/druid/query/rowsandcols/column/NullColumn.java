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

import org.apache.druid.query.rowsandcols.util.FindResult;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class NullColumn implements Column
{
  private final ColumnType type;
  private final int numRows;

  public NullColumn(
      ColumnType type,
      int numRows
  )
  {
    this.type = type;
    this.numRows = numRows;
  }

  @Nonnull
  @Override
  public ColumnAccessor toAccessor()
  {
    return new Accessor(type, numRows);
  }

  @Nullable
  @Override
  public <T> T as(Class<? extends T> clazz)
  {
    return null;
  }

  public static class Accessor implements BinarySearchableAccessor
  {
    private final ColumnType type;
    private final int size;

    public Accessor(ColumnType type, int size)
    {
      this.type = type;
      this.size = size;
    }

    @Override
    public ColumnType getType()
    {
      return type;
    }

    @Override
    public int numRows()
    {
      return size;
    }

    @Override
    public boolean isNull(int rowNum)
    {
      return true;
    }

    @Nullable
    @Override
    public Object getObject(int rowNum)
    {
      return null;
    }

    @Override
    public double getDouble(int rowNum)
    {
      return 0;
    }

    @Override
    public float getFloat(int rowNum)
    {
      return 0;
    }

    @Override
    public long getLong(int rowNum)
    {
      return 0;
    }

    @Override
    public int getInt(int rowNum)
    {
      return 0;
    }

    @Override
    public int compareRows(int lhsRowNum, int rhsRowNum)
    {
      return 0;
    }

    @Override
    public FindResult findNull(int startIndex, int endIndex)
    {
      return FindResult.found(startIndex, endIndex);
    }

    @Override
    public FindResult findDouble(int startIndex, int endIndex, double val)
    {
      return FindResult.notFound(endIndex);
    }

    @Override
    public FindResult findFloat(int startIndex, int endIndex, float val)
    {
      return FindResult.notFound(endIndex);
    }

    @Override
    public FindResult findLong(int startIndex, int endIndex, long val)
    {
      return FindResult.notFound(endIndex);
    }

    @Override
    public FindResult findString(int startIndex, int endIndex, String val)
    {
      return FindResult.notFound(endIndex);
    }

    @Override
    public FindResult findComplex(int startIndex, int endIndex, Object val)
    {
      return FindResult.notFound(endIndex);
    }
  }
}
