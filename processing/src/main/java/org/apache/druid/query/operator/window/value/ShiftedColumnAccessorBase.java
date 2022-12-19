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

package org.apache.druid.query.operator.window.value;

import org.apache.druid.query.rowsandcols.column.ColumnAccessor;
import org.apache.druid.segment.column.ColumnType;

public abstract class ShiftedColumnAccessorBase implements ColumnAccessor
{
  private final ColumnAccessor accessor;

  public ShiftedColumnAccessorBase(ColumnAccessor accessor)
  {
    this.accessor = accessor;
  }

  @Override
  public ColumnType getType()
  {
    return accessor.getType();
  }

  @Override
  public int numRows()
  {
    return accessor.numRows();
  }

  @Override
  public boolean isNull(int rowNum)
  {
    final int actualCell = getActualValue(rowNum);
    if (outsideBounds(actualCell)) {
      return true;
    }
    return accessor.isNull(actualCell);
  }

  @Override
  public Object getObject(int rowNum)
  {
    final int actualCell = getActualValue(rowNum);
    if (outsideBounds(actualCell)) {
      return null;
    }
    return accessor.getObject(actualCell);
  }

  @Override
  public double getDouble(int rowNum)
  {
    final int actualCell = getActualValue(rowNum);
    if (outsideBounds(actualCell)) {
      return 0.0D;
    }
    return accessor.getDouble(actualCell);
  }

  @Override
  public float getFloat(int rowNum)
  {
    final int actualCell = getActualValue(rowNum);
    if (outsideBounds(actualCell)) {
      return 0.0F;
    }
    return accessor.getFloat(actualCell);
  }

  @Override
  public long getLong(int rowNum)
  {
    final int actualCell = getActualValue(rowNum);
    if (outsideBounds(actualCell)) {
      return 0L;
    }
    return accessor.getLong(actualCell);
  }

  @Override
  public int getInt(int rowNum)
  {
    final int actualCell = getActualValue(rowNum);
    if (outsideBounds(actualCell)) {
      return 0;
    }
    return accessor.getInt(actualCell);
  }

  @Override
  public int compareRows(int lhsRowNum, int rhsRowNum)
  {
    int actualLhsCell = getActualValue(lhsRowNum);
    int actualRhsCell = getActualValue(rhsRowNum);
    if (outsideBounds(actualLhsCell)) {
      if (outsideBounds(actualRhsCell)) {
        // Both are null
        return 0;
      } else {
        return accessor.isNull(actualRhsCell) ? 0 : -1;
      }
    } else {
      if (outsideBounds(actualRhsCell)) {
        return accessor.isNull(actualLhsCell) ? 0 : 1;
      } else {
        return accessor.compareRows(actualLhsCell, actualRhsCell);
      }
    }
  }

  protected abstract int getActualValue(int rowNum);

  protected abstract boolean outsideBounds(int rowNum);
}
