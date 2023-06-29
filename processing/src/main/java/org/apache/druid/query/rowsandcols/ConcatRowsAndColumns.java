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

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.rowsandcols.column.Column;
import org.apache.druid.query.rowsandcols.column.ColumnAccessor;
import org.apache.druid.query.rowsandcols.column.NullColumn;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A RowsAndColumns implementation that effectively concatenates multiple RowsAndColumns objects together.
 * <p>
 * That is, if it is given 3 RowsAndColumns objects, of sizes 4, 2, and 3 respectively, the resulting
 * RowsAndColumns object will have 9 rows where rows 0-to-3 will be from the first object, 4-to-5 from the next and
 * 6-to-8 from the last.
 */
public class ConcatRowsAndColumns implements RowsAndColumns
{
  private final ArrayList<RowsAndColumns> racBuffer;
  private final Map<String, Column> columnCache = new LinkedHashMap<>();

  private final int[][] rowPointers;
  private final int numRows;

  public ConcatRowsAndColumns(
      ArrayList<RowsAndColumns> racBuffer
  )
  {
    this.racBuffer = racBuffer;

    int numRows = 0;
    for (RowsAndColumns rac : racBuffer) {
      numRows += rac.numRows();
    }
    this.numRows = numRows;
    this.rowPointers = new int[2][numRows];

    int index = 0;
    for (int i = 0; i < racBuffer.size(); ++i) {
      final RowsAndColumns rac = racBuffer.get(i);
      Arrays.fill(rowPointers[0], index, index + rac.numRows(), i);
      for (int j = 0; j < rac.numRows(); ++j) {
        rowPointers[1][index + j] = j;
      }
      index += rac.numRows();
    }
  }

  @Override
  public Collection<String> getColumnNames()
  {
    return racBuffer.get(0).getColumnNames();
  }

  @Override
  public int numRows()
  {
    return numRows;
  }

  @Override
  @Nullable
  public Column findColumn(String name)
  {
    if (columnCache.containsKey(name)) {
      return columnCache.get(name);
    } else {
      final Column firstCol = racBuffer.get(0).findColumn(name);
      if (firstCol == null) {
        for (int i = 1; i < racBuffer.size(); ++i) {
          RowsAndColumns rac = racBuffer.get(i);
          if (rac.findColumn(name) != null) {
            throw new ISE("Column[%s] was not always null...", name);
          }
        }
        columnCache.put(name, null);
        return null;
      } else {
        ArrayList<ColumnAccessor> accessors = new ArrayList<>(racBuffer.size());
        final ColumnAccessor firstAccessor = firstCol.toAccessor();
        accessors.add(firstAccessor);
        final ColumnType type = firstAccessor.getType();
        for (int i = 1; i < racBuffer.size(); ++i) {
          RowsAndColumns rac = racBuffer.get(i);
          Column col = rac.findColumn(name);
          if (col == null) {
            // It doesn't exist, so must be all null!
            col = new NullColumn(type, rac.numRows());
          }
          final ColumnAccessor accessor = col.toAccessor();
          if (!type.equals(accessor.getType())) {
            throw new ISE("Type mismatch, expected[%s], got[%s] on entry[%,d]", type, accessor.getType(), i);
          }
          accessors.add(accessor);
        }

        final ConcatedidColumn retVal = new ConcatedidColumn(
            type,
            Comparator.nullsFirst(type.getStrategy()),
            accessors
        );
        columnCache.put(name, retVal);
        return retVal;
      }
    }
  }

  @Nullable
  @Override
  public <T> T as(Class<T> clazz)
  {
    return null;
  }

  private class ConcatedidColumn implements Column
  {

    private final ArrayList<ColumnAccessor> accessors;
    private final ColumnType type;
    private final Comparator<Object> comp;

    public ConcatedidColumn(
        ColumnType type,
        Comparator<Object> comp,
        ArrayList<ColumnAccessor> accessors
    )
    {
      this.accessors = accessors;
      this.type = type;
      this.comp = comp;
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
          final ColumnAccessor localAccessor = getLocalAccessor(rowNum);
          final int localIndex = getLocalIndex(rowNum);
          return localAccessor.isNull(localIndex);
        }

        @Nullable
        @Override
        public Object getObject(int rowNum)
        {
          final ColumnAccessor localAccessor = getLocalAccessor(rowNum);
          final int localIndex = getLocalIndex(rowNum);
          return localAccessor.getObject(localIndex);
        }

        @Override
        public double getDouble(int rowNum)
        {
          final ColumnAccessor localAccessor = getLocalAccessor(rowNum);
          final int localIndex = getLocalIndex(rowNum);
          return localAccessor.getDouble(localIndex);
        }

        @Override
        public float getFloat(int rowNum)
        {
          final ColumnAccessor localAccessor = getLocalAccessor(rowNum);
          final int localIndex = getLocalIndex(rowNum);
          return localAccessor.getFloat(localIndex);
        }

        @Override
        public long getLong(int rowNum)
        {
          final ColumnAccessor localAccessor = getLocalAccessor(rowNum);
          final int localIndex = getLocalIndex(rowNum);
          return localAccessor.getLong(localIndex);
        }

        @Override
        public int getInt(int rowNum)
        {
          final ColumnAccessor localAccessor = getLocalAccessor(rowNum);
          final int localIndex = getLocalIndex(rowNum);
          return localAccessor.getInt(localIndex);
        }

        @Override
        public int compareRows(int lhsRowNum, int rhsRowNum)
        {
          final ColumnAccessor lhsAccessor = getLocalAccessor(lhsRowNum);
          final int lhsIndex = getLocalIndex(lhsRowNum);

          final ColumnAccessor rhsAccessor = getLocalAccessor(rhsRowNum);
          final int rhsIndex = getLocalIndex(rhsRowNum);

          return comp.compare(lhsAccessor.getObject(lhsIndex), rhsAccessor.getObject(rhsIndex));
        }

        private int getLocalIndex(int rowNum)
        {
          return rowPointers[1][rowNum];
        }

        private ColumnAccessor getLocalAccessor(int rowNum)
        {
          return accessors.get(rowPointers[0][rowNum]);
        }
      };
    }

    @Nullable
    @Override
    public <T> T as(Class<? extends T> clazz)
    {
      return null;
    }
  }
}
