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

import javax.annotation.Nullable;

public class NullColumnAccessor implements ColumnAccessor
{
  private final ColumnType type;
  private final int size;

  public NullColumnAccessor(int size)
  {
    this(ColumnType.UNKNOWN_COMPLEX, size);
  }

  public NullColumnAccessor(ColumnType type, int size)
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
  public int compareCells(int lhsRowNum, int rhsRowNum)
  {
    return 0;
  }
}
