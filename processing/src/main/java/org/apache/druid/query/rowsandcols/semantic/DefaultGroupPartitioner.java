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

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.Column;
import org.apache.druid.query.rowsandcols.column.ColumnAccessor;

import java.util.List;

@SuppressWarnings("unused")
public class DefaultGroupPartitioner implements GroupPartitioner
{
  private final RowsAndColumns rac;

  public DefaultGroupPartitioner(
      RowsAndColumns rac
  )
  {
    this.rac = rac;
  }

  @Override
  public int[] computeGroupings(List<String> columns)
  {
    int[] retVal = new int[rac.numRows()];

    for (String column : columns) {
      final Column theCol = rac.findColumn(column);
      if (theCol == null) {
        // The column doesn't exist.  In this case, we assume it's always the same value: null.  If it's always
        // the same, then it doesn't impact grouping at all and can be entirely skipped.
        continue;
      }
      final ColumnAccessor accessor = theCol.toAccessor();

      int currGroup = 0;
      int prevGroupVal = 0;
      for (int i = 1; i < retVal.length; ++i) {
        if (retVal[i] == prevGroupVal) {
          int comparison = accessor.compareRows(i - 1, i);
          if (comparison == 0) {
            retVal[i] = currGroup;
            continue;
          } else if (comparison > 0) { // "greater than"
            throw new ISE("Pre-sorted data required, rows[%s] and [%s] were not in order", i - 1, i);
          } // the 3rd condition ("less than") means create a new group, so let it fall through
        }

        // We have a new group, so walk things forward.
        prevGroupVal = retVal[i];
        retVal[i] = ++currGroup;
      }
    }

    return retVal;
  }
}
