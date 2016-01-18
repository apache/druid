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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.joda.time.DateTime;

import java.util.Arrays;
import java.util.Map;
import java.util.TreeSet;

public class Rowboat implements Comparable<Rowboat>
{
  private final long timestamp;
  private final int[][] dims;
  private final Object[] metrics;
  private final int rowNum;
  private final Map<Integer, TreeSet<Integer>> comprisedRows;

  public Rowboat(
      long timestamp,
      int[][] dims,
      Object[] metrics,
      int rowNum
  )
  {
    this.timestamp = timestamp;
    this.dims = dims;
    this.metrics = metrics;
    this.rowNum = rowNum;

    this.comprisedRows = Maps.newHashMap();
  }

  public long getTimestamp()
  {
    return timestamp;
  }

  public int[][] getDims()
  {
    return dims;
  }

  public Object[] getMetrics()
  {
    return metrics;
  }

  public void addRow(int indexNum, int rowNum)
  {
    TreeSet<Integer> rowNums = comprisedRows.get(indexNum);
    if (rowNums == null) {
      rowNums = Sets.newTreeSet();
      comprisedRows.put(indexNum, rowNums);
    }
    rowNums.add(rowNum);
  }

  public Map<Integer, TreeSet<Integer>> getComprisedRows()
  {
    return comprisedRows;
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
      int[] lhsVals = dims[index];
      int[] rhsVals = rhs.dims[index];

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

      retVal = Ints.compare(lhsVals.length, rhsVals.length);

      int valsIndex = 0;
      while (retVal == 0 && valsIndex < lhsVals.length) {
        retVal = Ints.compare(lhsVals[valsIndex], rhsVals[valsIndex]);
        ++valsIndex;
      }
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
