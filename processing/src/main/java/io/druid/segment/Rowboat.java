/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntRBTreeSet;
import it.unimi.dsi.fastutil.ints.IntSortedSet;
import org.joda.time.DateTime;

import java.util.Arrays;

public class Rowboat implements Comparable<Rowboat>
{
  private final long timestamp;
  private final Object[] dims;
  private final Object[] metrics;
  private final int rowNum;
  private final Int2ObjectOpenHashMap<IntSortedSet> comprisedRows;
  private final DimensionHandler[] handlers;

  public Rowboat(
      long timestamp,
      Object[] dims,
      Object[] metrics,
      int rowNum,
      DimensionHandler[] handlers
  )
  {
    this.timestamp = timestamp;
    this.dims = dims;
    this.metrics = metrics;
    this.rowNum = rowNum;
    this.handlers = handlers;

    this.comprisedRows = new Int2ObjectOpenHashMap<>();
  }

  public long getTimestamp()
  {
    return timestamp;
  }

  public Object[] getDims()
  {
    return dims;
  }

  public Object[] getMetrics()
  {
    return metrics;
  }

  public void addRow(int indexNum, int rowNum)
  {
    IntSortedSet rowNums = comprisedRows.get(indexNum);
    if (rowNums == null) {
      rowNums = new IntRBTreeSet();
      comprisedRows.put(indexNum, rowNums);
    }
    rowNums.add(rowNum);
  }

  public Int2ObjectOpenHashMap<IntSortedSet> getComprisedRows()
  {
    return comprisedRows;
  }

  public DimensionHandler[] getHandlers()
  {
    return handlers;
  }

  public int getRowNum()
  {
    return rowNum;
  }

  @Override
  public int compareTo(Rowboat rhs)
  {
    int retVal = Longs.compare(timestamp, rhs.timestamp);

    if (retVal == 0) {
      retVal = Ints.compare(dims.length, rhs.dims.length);
    }

    int index = 0;
    while (retVal == 0 && index < dims.length) {
      Object lhsVals = dims[index];
      Object rhsVals = rhs.dims[index];

      if (lhsVals == null) {
        if (rhsVals == null) {
          index++;
          continue;
        }
        return -1;
      }

      if (rhsVals == null) {
        return 1;
      }

      DimensionHandler handler = handlers[index];
      retVal = handler.compareSortedEncodedKeyComponents(lhsVals, rhsVals);
      ++index;
    }

    return retVal;
  }

  @Override
  public String toString()
  {
    return "Rowboat{" +
           "timestamp=" + new DateTime(timestamp).toString() +
           ", dims=" + Arrays.deepToString(dims) +
           ", metrics=" + Arrays.toString(metrics) +
           ", comprisedRows=" + comprisedRows +
           '}';
  }
}
