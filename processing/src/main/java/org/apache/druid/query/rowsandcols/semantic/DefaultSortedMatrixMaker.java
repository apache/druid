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

package org.apache.druid.query.rowsandcols.semantic;

import org.apache.druid.java.util.common.RE;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.BinarySearchableAccessor;
import org.apache.druid.query.rowsandcols.column.Column;
import org.apache.druid.query.rowsandcols.util.FindResult;

import java.util.ArrayList;
import java.util.List;

/**
 * The DefaultSortedMatrixMaker is a SortedMatrixMaker that works on the generic RowsAndColumns interface.
 * <p>
 * It does not validate that things are sorted, it just assumes that they must be.  As such, behavior are undefined
 * when running on top of a RowsAndColumns that is not actually sorted by the columns passed into {@link #make}
 */
public class DefaultSortedMatrixMaker implements SortedMatrixMaker
{
  private final RowsAndColumns rac;

  public DefaultSortedMatrixMaker(
      RowsAndColumns rac
  )
  {
    this.rac = rac;
  }

  @Override
  public SortedMatrix make(List<String> columns)
  {
    ArrayList<BinarySearchableAccessor> soughtColumns = new ArrayList<>(columns.size());
    for (String column : columns) {
      // Note, this can add `null` to the list.  That's intentional, iterations over this list must deal
      // with null entries meaning that the column doesn't exist
      final Column racColumn = rac.findColumn(column);
      if (racColumn == null) {
        soughtColumns.add(null);
      } else {
        soughtColumns.add(BinarySearchableAccessor.fromColumn(racColumn));
      }
    }

    return new SortedMatrix()
    {
      @Override
      public int numRows()
      {
        return rac.numRows();
      }

      @Override
      public MatrixRow getRow(int rowId)
      {
        return new MatrixRow()
        {
          @Override
          public int length()
          {
            return soughtColumns.size();
          }

          @Override
          public boolean isNull(int columnId)
          {
            final BinarySearchableAccessor column = soughtColumns.get(columnId);
            if (column == null) {
              return true;
            } else {
              return column.isNull(rowId);
            }
          }

          @Override
          public Object getObject(int columnId)
          {
            final BinarySearchableAccessor column = soughtColumns.get(columnId);
            if (column == null) {
              return null;
            } else {
              return column.getObject(rowId);
            }
          }

          @Override
          public double getDouble(int columnId)
          {
            final BinarySearchableAccessor column = soughtColumns.get(columnId);
            if (column == null) {
              return 0;
            } else {
              return column.getDouble(rowId);
            }
          }

          @Override
          public float getFloat(int columnId)
          {
            final BinarySearchableAccessor column = soughtColumns.get(columnId);
            if (column == null) {
              return 0;
            } else {
              return column.getFloat(rowId);
            }
          }

          @Override
          public long getLong(int columnId)
          {
            final BinarySearchableAccessor column = soughtColumns.get(columnId);
            if (column == null) {
              return 0;
            } else {
              return column.getLong(rowId);
            }
          }
        };
      }

      @Override
      public FindResult findRow(
          int startRowIndex,
          MatrixRow row
      )
      {
        int start = startRowIndex;
        int end = numRows();

        for (int i = 0; i < row.length(); ++i) {
          final BinarySearchableAccessor searcher = soughtColumns.get(i);
          if (searcher == null) {
            if (row.isNull(i)) {
              continue;
            } else {
              // We don't have the column at all, that means that we can only match `null`.  Given that the data
              // is sorted, the next possible value is the next value of the previous column, so return that as our
              // end.
              return FindResult.notFound(end);
            }
          }

          final FindResult result;
          if (row.isNull(i)) {
            result = searcher.findNull(start, end);
          } else {
            switch (searcher.getType().getType()) {
              case STRING:
                result = searcher.findString(start, end, (String) row.getObject(i));
                break;
              case LONG:
                if (row.isNull(i)) {
                  result = searcher.findNull(start, end);
                } else {
                  result = searcher.findLong(start, end, row.getLong(i));
                }
                break;
              case DOUBLE:
                result = searcher.findDouble(start, end, row.getDouble(i));
                break;
              case FLOAT:
                result = searcher.findFloat(start, end, row.getFloat(i));
                break;

              case ARRAY:
              case COMPLEX:
                result = searcher.findComplex(start, end, row.getObject(i));
                break;
              default:
                throw new RE("Unknown type[%s]", searcher.getType());
            }
          }

          if (result.wasFound()) {
            start = result.getStartRow();
            end = result.getEndRow();
          } else {
            return FindResult.notFound(result.getNext());
          }
        }

        return FindResult.found(start, end);
      }
    };
  }
}
