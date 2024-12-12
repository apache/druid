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

package org.apache.druid.query.rowsandcols.column.accessor;

import org.apache.druid.query.rowsandcols.column.ColumnAccessor;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;

public abstract class LongColumnAccessorBase implements ColumnAccessor
{
  @Override
  public ColumnType getType()
  {
    return ColumnType.LONG;
  }

  @Nullable
  @Override
  public final Object getObject(int rowNum)
  {
    if (isNull(rowNum)) {
      return null;
    } else {
      return getLong(rowNum);
    }
  }

  @Override
  public float getFloat(int rowNum)
  {
    return getLong(rowNum);
  }

  @Override
  public double getDouble(int rowNum)
  {
    return getLong(rowNum);
  }

  @Override
  public int getInt(int rowNum)
  {
    return (int) getLong(rowNum);
  }

  @Override
  public int compareRows(int lhsRowNum, int rhsRowNum)
  {
    if (isNull(lhsRowNum)) {
      return isNull(rhsRowNum) ? 0 : -1;
    }

    if (isNull(rhsRowNum)) {
      // if lhs was null, the code would've entered into the if clause before this one.
      return 1;
    }

    return Long.compare(getLong(lhsRowNum), getLong(rhsRowNum));
  }
}
