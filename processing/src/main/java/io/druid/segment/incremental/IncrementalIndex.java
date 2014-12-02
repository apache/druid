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

package io.druid.segment.incremental;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import io.druid.data.input.InputRow;
import io.druid.data.input.Row;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.segment.column.ColumnCapabilities;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentNavigableMap;

/**
 */
public interface IncrementalIndex extends Iterable<Row>, Closeable
{

  List<String> getDimensions();

  ConcurrentNavigableMap<TimeAndDims, Integer> getFacts();

  Integer getDimensionIndex(String dimension);

  List<String> getMetricNames();

  DimDim getDimension(String dimension);

  AggregatorFactory[] getMetricAggs();

  Interval getInterval();

  DateTime getMinTime();

  DateTime getMaxTime();

  boolean isEmpty();

  ConcurrentNavigableMap<TimeAndDims,Integer> getSubMap(TimeAndDims start, TimeAndDims end);

  Integer getMetricIndex(String columnName);

  String getMetricType(String metric);

  ColumnCapabilities getCapabilities(String column);

  int size();

  float getMetricFloatValue(int rowOffset, int aggOffset);

  long getMetricLongValue(int rowOffset, int aggOffset);

  Object getMetricObjectValue(int rowOffset, int aggOffset);

  Iterable<Row> iterableWithPostAggregations(List<PostAggregator> postAggregatorSpecs);

  int add(InputRow inputRow);

  void close();

  InputRow formatRow(InputRow parse);

  static interface DimDim
  {
    public String get(String value);

    public int getId(String value);

    public String getValue(int id);

    public boolean contains(String value);

    public int size();

    public int add(String value);

    public int getSortedId(String value);

    public String getSortedValue(int index);

    public void sort();

    public boolean compareCannonicalValues(String s1, String s2);
  }

  static class TimeAndDims implements Comparable<TimeAndDims>
  {
    private final long timestamp;
    private final String[][] dims;

    TimeAndDims(
        long timestamp,
        String[][] dims
    )
    {
      this.timestamp = timestamp;
      this.dims = dims;
    }

    long getTimestamp()
    {
      return timestamp;
    }

    String[][] getDims()
    {
      return dims;
    }

    @Override
    public int compareTo(TimeAndDims rhs)
    {
      int retVal = Longs.compare(timestamp, rhs.timestamp);

      if (retVal == 0) {
        retVal = Ints.compare(dims.length, rhs.dims.length);
      }

      int index = 0;
      while (retVal == 0 && index < dims.length) {
        String[] lhsVals = dims[index];
        String[] rhsVals = rhs.dims[index];

        if (lhsVals == null) {
          if (rhsVals == null) {
            ++index;
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
          retVal = lhsVals[valsIndex].compareTo(rhsVals[valsIndex]);
          ++valsIndex;
        }
        ++index;
      }

      return retVal;
    }

    @Override
    public String toString()
    {
      return "TimeAndDims{" +
             "timestamp=" + new DateTime(timestamp) +
             ", dims=" + Lists.transform(
          Arrays.asList(dims), new Function<String[], Object>()
          {
            @Override
            public Object apply(@Nullable String[] input)
            {
              if (input == null || input.length == 0) {
                return Arrays.asList("null");
              }
              return Arrays.asList(input);
            }
          }
      ) +
             '}';
    }
  }
}
