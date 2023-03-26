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

import it.unimi.dsi.fastutil.Arrays;
import org.apache.druid.query.operator.ColumnWithDirection;
import org.apache.druid.query.rowsandcols.ConcatRowsAndColumns;
import org.apache.druid.query.rowsandcols.EmptyRowsAndColumns;
import org.apache.druid.query.rowsandcols.RearrangedRowsAndColumns;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.Column;
import org.apache.druid.query.rowsandcols.column.ColumnAccessor;

import java.util.ArrayList;

public class DefaultNaiveSortMaker implements NaiveSortMaker
{
  private final RowsAndColumns rac;

  public DefaultNaiveSortMaker(
      RowsAndColumns rac
  )
  {
    this.rac = rac;
  }

  @Override
  public NaiveSorter make(ArrayList<ColumnWithDirection> ordering)
  {
    final NaiveSorter retVal = new DefaultNaiveSorter(ordering);
    retVal.moreData(rac);
    return retVal;
  }

  private static class DefaultNaiveSorter implements NaiveSorter
  {
    final ArrayList<RowsAndColumns> racBuffer;
    private final ArrayList<ColumnWithDirection> ordering;

    public DefaultNaiveSorter(ArrayList<ColumnWithDirection> ordering)
    {
      this.ordering = ordering;
      racBuffer = new ArrayList<>();
    }


    @Override
    public RowsAndColumns moreData(RowsAndColumns rac)
    {
      racBuffer.add(rac);
      return null;
    }

    @Override
    public RowsAndColumns complete()
    {
      if (racBuffer.isEmpty()) {
        return new EmptyRowsAndColumns();
      }

      ConcatRowsAndColumns rac = new ConcatRowsAndColumns(racBuffer);

      // One int for the racBuffer, another for the rowIndex
      int[] sortedPointers = new int[rac.numRows()];
      for (int i = 0; i < sortedPointers.length; ++i) {
        sortedPointers[i] = i;
      }

      int index = 0;
      int[] direction = new int[ordering.size()];
      ColumnAccessor[] accessors = new ColumnAccessor[ordering.size()];
      for (ColumnWithDirection orderByColumnSpec : ordering) {
        final Column col = rac.findColumn(orderByColumnSpec.getColumn());
        // Null columns are all the same value: null.  Therefore, it is already sorted, so just ignore
        if (col != null) {
          accessors[index] = col.toAccessor();
          direction[index] = orderByColumnSpec.getDirection().getDirectionInt();
          ++index;
        }
      }

      final int numColsToCompare = index;

      // Use stable sort, so peer rows retain original order.
      Arrays.mergeSort(
          0,
          rac.numRows(),
          (k1, k2) -> {
            for (int i = 0; i < numColsToCompare; ++i) {
              final ColumnAccessor accessy = accessors[i];
              int val = accessy.compareRows(sortedPointers[k1], sortedPointers[k2]);
              if (val != 0) {
                return val * direction[i];
              }
            }
            return 0;
          },
          (a, b) -> {
            int bufPos = sortedPointers[a];
            sortedPointers[a] = sortedPointers[b];
            sortedPointers[b] = bufPos;
          }
      );

      return new RearrangedRowsAndColumns(sortedPointers, rac);
    }
  }
}
