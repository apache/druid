/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
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

  private Map<String, String> columnDescriptor;

  public Rowboat(
      long timestamp,
      int[][] dims,
      Object[] metrics,
      int rowNum,
      Map<String, String> columnDescriptor
  )
  {
    this.timestamp = timestamp;
    this.dims = dims;
    this.metrics = metrics;
    this.rowNum = rowNum;
    this.columnDescriptor = columnDescriptor;

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

  public Map<String, String> getDescriptions()
  {
    return columnDescriptor;
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
           ", dims=" + (dims == null ? null : Arrays.asList(dims)) +
           ", metrics=" + (metrics == null ? null : Arrays.asList(metrics)) +
           ", comprisedRows=" + comprisedRows +
           '}';
  }
}