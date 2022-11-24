/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.druid.query.operator.LimitedRowsAndColumns;
import org.apache.druid.query.rowsandcols.column.Column;
import org.apache.druid.query.rowsandcols.column.ColumnAccessor;

import java.util.ArrayList;
import java.util.List;

public class DefaultSortedGroupPartitioner implements SortedGroupPartitioner
{
  private final RowsAndColumns rac;

  public DefaultSortedGroupPartitioner(
      RowsAndColumns rac
  )
  {
    this.rac = rac;
  }

  @Override
  public ArrayList<StartAndEnd> computeBoundaries(List<String> columns)
  {
    ArrayList<StartAndEnd> retVal = new ArrayList<>();
    // Initialize to a grouping of everything
    retVal.add(new StartAndEnd(0, rac.numRows()));

    for (String column : columns) {
      final Column theCol = rac.findColumn(column);
      if (theCol == null) {
        // The column doesn't exist.  In this case, we assume it's always the same value: null.  If it's always
        // the same, then it doesn't impact grouping at all and can be entirely skipped.
        continue;
      }
      final ColumnAccessor accessor = theCol.toAccessor();

      ArrayList<StartAndEnd> newRetVal = new ArrayList<>();
      for (int i = 0; i < retVal.size(); ++i) {
        final StartAndEnd currGroup = retVal.get(i);
        int currStart = currGroup.getStart();
        for (int j = currGroup.getStart() + 1; j < currGroup.getEnd(); ++j) {
          int comparison = accessor.compareCells(j - 1, j);
          if (comparison < 0) {
            newRetVal.add(new StartAndEnd(currStart, j));
            currStart = j;
          } else if (comparison > 0) {
            throw new ISE("Pre-sorted data required, rows[%s] and [%s] were not in order", i - 1, i);
          }
        }
        newRetVal.add(new StartAndEnd(currStart, currGroup.getEnd()));
      }
      retVal = newRetVal;
    }

    return retVal;
  }

  @Override
  public ArrayList<RowsAndColumns> partitionOnBoundaries(List<String> partitionColumns)
  {
    final ArrayList<StartAndEnd> startAndEnds = computeBoundaries(partitionColumns);
    ArrayList<RowsAndColumns> retVal = new ArrayList<>(startAndEnds.size());

    for (StartAndEnd startAndEnd : startAndEnds) {
      retVal.add(new LimitedRowsAndColumns(rac, startAndEnd.getStart(), startAndEnd.getEnd()));
    }

    return retVal;
  }
}
