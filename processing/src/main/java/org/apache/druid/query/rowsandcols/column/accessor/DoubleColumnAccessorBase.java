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

public abstract class DoubleColumnAccessorBase implements ColumnAccessor
{
  @Override
  public ColumnType getType()
  {
    return ColumnType.DOUBLE;
  }

  @Nullable
  @Override
  public Object getObject(int rowNum)
  {
    if (isNull(rowNum)) {
      return null;
    } else {
      return getDouble(rowNum);
    }
  }

  @Override
  public float getFloat(int rowNum)
  {
    return (float) getDouble(rowNum);
  }

  @Override
  public long getLong(int rowNum)
  {
    return (long) getDouble(rowNum);
  }

  @Override
  public int getInt(int rowNum)
  {
    return (int) getDouble(rowNum);
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

    return Double.compare(getDouble(lhsRowNum), getDouble(rhsRowNum));
  }
}
