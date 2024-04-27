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

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.druid.query.rowsandcols.LimitedRowsAndColumns;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.Column;
import org.apache.druid.query.rowsandcols.column.ColumnAccessor;

import java.util.ArrayList;
import java.util.List;

public class DefaultClusteredGroupPartitioner implements ClusteredGroupPartitioner
{
  private final RowsAndColumns rac;

  public DefaultClusteredGroupPartitioner(
      RowsAndColumns rac
  )
  {
    this.rac = rac;
  }

  @Override
  public int[] computeBoundaries(List<String> columns)
  {
    if (rac.numRows() == 0) {
      return new int[]{};
    }

    // Initialize to a grouping of everything
    IntList boundaries = new IntArrayList(new int[]{0, rac.numRows()});

    for (String column : columns) {
      final Column theCol = rac.findColumn(column);
      if (theCol == null) {
        // The column doesn't exist.  In this case, we assume it's always the same value: null.  If it's always
        // the same, then it doesn't impact grouping at all and can be entirely skipped.
        continue;
      }
      final ColumnAccessor accessor = theCol.toAccessor();

      IntList newBoundaries = new IntArrayList();
      newBoundaries.add(0);
      for (int i = 1; i < boundaries.size(); ++i) {
        int start = boundaries.getInt(i - 1);
        int end = boundaries.getInt(i);
        for (int j = start + 1; j < end; ++j) {
          int comparison = accessor.compareRows(j - 1, j);
          if (comparison != 0) {
            newBoundaries.add(j);
          }
        }
        newBoundaries.add(end);
      }
      boundaries = newBoundaries;
    }

    return boundaries.toIntArray();
  }

  @Override
  public ArrayList<RowsAndColumns> partitionOnBoundaries(List<String> partitionColumns)
  {
    final int[] boundaries = computeBoundaries(partitionColumns);
    if (boundaries.length < 2) {
      return new ArrayList<>();
    }

    ArrayList<RowsAndColumns> retVal = new ArrayList<>(boundaries.length - 1);

    for (int i = 1; i < boundaries.length; ++i) {
      int start = boundaries[i - 1];
      int end = boundaries[i];
      retVal.add(new LimitedRowsAndColumns(rac, start, end));
    }

    return retVal;
  }
}
