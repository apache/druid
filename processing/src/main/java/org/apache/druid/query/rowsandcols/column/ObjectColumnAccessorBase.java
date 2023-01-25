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

import javax.annotation.Nullable;
import java.util.Comparator;

public abstract class ObjectColumnAccessorBase implements ColumnAccessor
{
  @Override
  public boolean isNull(int rowNum)
  {
    return getVal(rowNum) == null;
  }

  @Nullable
  @Override
  public Object getObject(int rowNum)
  {
    return getVal(rowNum);
  }

  @Override
  public double getDouble(int rowNum)
  {
    final Object val = getVal(rowNum);
    if (val instanceof Number) {
      return ((Number) val).doubleValue();
    } else if (val instanceof String) {
      try {
        return Double.parseDouble((String) val);
      }
      catch (NumberFormatException e) {
        return 0d;
      }
    } else {
      return 0d;
    }
  }

  @Override
  public float getFloat(int rowNum)
  {
    final Object val = getVal(rowNum);
    if (val instanceof Number) {
      return ((Number) val).floatValue();
    } else if (val instanceof String) {
      try {
        return Float.parseFloat((String) val);
      }
      catch (NumberFormatException e) {
        return 0f;
      }
    } else {
      return 0f;
    }
  }

  @Override
  public long getLong(int rowNum)
  {
    final Object val = getVal(rowNum);
    if (val instanceof Number) {
      return ((Number) val).longValue();
    } else if (val instanceof String) {
      try {
        return Long.parseLong((String) val);
      }
      catch (NumberFormatException e) {
        return 0L;
      }
    } else {
      return 0L;
    }
  }

  @Override
  public int getInt(int rowNum)
  {
    final Object val = getVal(rowNum);
    if (val instanceof Number) {
      return ((Number) val).intValue();
    } else if (val instanceof String) {
      try {
        return Integer.parseInt((String) val);
      }
      catch (NumberFormatException e) {
        return 0;
      }
    } else {
      return 0;
    }
  }

  @Override
  public int compareRows(int lhsRowNum, int rhsRowNum)
  {
    return getComparator().compare(getVal(lhsRowNum), getVal(rhsRowNum));
  }

  protected abstract Object getVal(int rowNum);

  protected abstract Comparator<Object> getComparator();
}
