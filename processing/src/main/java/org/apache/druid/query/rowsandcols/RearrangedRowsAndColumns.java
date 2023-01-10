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

package org.apache.druid.query.rowsandcols;

import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.rowsandcols.column.Column;
import org.apache.druid.query.rowsandcols.column.ColumnAccessor;
import org.apache.druid.query.rowsandcols.column.ColumnAccessorBasedColumn;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

public class RearrangedRowsAndColumns implements RowsAndColumns
{
  private final Map<String, Column> columnCache = new LinkedHashMap<>();

  private final int[] pointers;
  private final RowsAndColumns rac;

  public RearrangedRowsAndColumns(
      int[] pointers,
      RowsAndColumns rac
  )
  {
    if (pointers.length != rac.numRows()) {
      throw new IAE("length mismatch, pointers[%,d], rac[%,d]", pointers.length, rac.numRows());
    }

    this.pointers = pointers;
    this.rac = rac;
  }

  @Override
  public Collection<String> getColumnNames()
  {
    return rac.getColumnNames();
  }

  @Override
  public int numRows()
  {
    return pointers.length;
  }

  @Override
  @Nullable
  public Column findColumn(String name)
  {
    if (columnCache.containsKey(name)) {
      return columnCache.get(name);
    } else {
      final Column column = rac.findColumn(name);
      if (column == null) {
        columnCache.put(name, null);
        return null;
      }

      final ColumnAccessor accessor = column.toAccessor();
      return new ColumnAccessorBasedColumn(
          new ColumnAccessor()
          {
            @Override
            public ColumnType getType()
            {
              return accessor.getType();
            }

            @Override
            public int numRows()
            {
              return pointers.length;
            }

            @Override
            public boolean isNull(int rowNum)
            {
              return accessor.isNull(pointers[rowNum]);
            }

            @Nullable
            @Override
            public Object getObject(int rowNum)
            {
              return accessor.getObject(pointers[rowNum]);
            }

            @Override
            public double getDouble(int rowNum)
            {
              return accessor.getDouble(pointers[rowNum]);
            }

            @Override
            public float getFloat(int rowNum)
            {
              return accessor.getFloat(pointers[rowNum]);
            }

            @Override
            public long getLong(int rowNum)
            {
              return accessor.getLong(pointers[rowNum]);
            }

            @Override
            public int getInt(int rowNum)
            {
              return accessor.getInt(pointers[rowNum]);
            }

            @Override
            public int compareRows(int lhsRowNum, int rhsRowNum)
            {
              return accessor.compareRows(pointers[lhsRowNum], pointers[rhsRowNum]);
            }
          }
      );
    }
  }

  @Nullable
  @Override
  public <T> T as(Class<T> clazz)
  {
    return null;
  }
}
