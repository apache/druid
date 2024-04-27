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

/**
 * This class exists to "decorate" a rows and columns such that it pretends to exist in a new ordering.
 * <p>
 * The constructor generally takes an int[] of pointers, these pointers are used to re-map the rows from the
 * RowsAndColumns object.  That is, if the RowsAndColumns has 4 rows and the array {@code new int[]{3, 1, 2, 0}}
 * is passed in, then the order of traverals of the rows will be {@code 3 -> 1 -> 2 -> 0}.
 * <p>
 * This can be useful for sorting potentially immutable data, as the list of pointers can identify the order
 * that the rows should be traversed in.  It can also be used for clustering like-data together.
 * <p>
 * While this avoids a copy, in cases where the data will be iterated regularly, it also generates a random-access
 * pattern that is not always optimal.
 */
public class RearrangedRowsAndColumns implements RowsAndColumns
{
  private final Map<String, Column> columnCache = new LinkedHashMap<>();

  private final int[] pointers;
  private final RowsAndColumns rac;
  private final int start;
  private final int end;

  public RearrangedRowsAndColumns(
      int[] pointers,
      RowsAndColumns rac
  )
  {
    this(pointers, 0, pointers.length, rac);
  }

  public RearrangedRowsAndColumns(
      int[] pointers,
      int start,
      int end,
      RowsAndColumns rac
  )
  {
    if (end - start < 0 || end > pointers.length) {
      throw new IAE("end[%,d] - start[%,d] was invalid!? pointers.length[%,d]", end, start, pointers.length);
    }
    this.pointers = pointers;
    this.start = start;
    this.end = end;
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
    return end - start;
  }

  @Override
  @Nullable
  public Column findColumn(String name)
  {
    // We do a containsKey here so that we can negative-cache nulls.
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
              return end - start;
            }

            @Override
            public boolean isNull(int rowNum)
            {
              return accessor.isNull(pointers[start + rowNum]);
            }

            @Nullable
            @Override
            public Object getObject(int rowNum)
            {
              return accessor.getObject(pointers[start + rowNum]);
            }

            @Override
            public double getDouble(int rowNum)
            {
              return accessor.getDouble(pointers[start + rowNum]);
            }

            @Override
            public float getFloat(int rowNum)
            {
              return accessor.getFloat(pointers[start + rowNum]);
            }

            @Override
            public long getLong(int rowNum)
            {
              return accessor.getLong(pointers[start + rowNum]);
            }

            @Override
            public int getInt(int rowNum)
            {
              return accessor.getInt(pointers[start + rowNum]);
            }

            @Override
            public int compareRows(int lhsRowNum, int rhsRowNum)
            {
              return accessor.compareRows(pointers[lhsRowNum], pointers[start + rhsRowNum]);
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
