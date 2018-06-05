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

import com.google.common.base.Preconditions;
import io.druid.java.util.common.DateTimes;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * TimeAndDimsPointer is used in conjunction with {@link TimeAndDimsIterator}, it's an _immutable_ object that points to
 * different logical data points, as {@link TimeAndDimsIterator#moveToNext()} is called.
 *
 * TimeAndDimsPointers are comparable by time and dimension column values, but excluding metric column values, to
 * facilicate ordering and rollup during merging of collections of rows (see {@link IndexMergerV9#merge} methods).
 *
 * The difference between "time and dims" and "row" abstraction (see {@link
 * io.druid.segment.incremental.IncrementalIndexRow}, {@link RowPointer}) is that "time and dims" is logical composite
 * of only time point and dimension and metric values, not tied to a specific position in any data structure (aka "row
 * index").
 *
 * TimeAndDimsPointer is conceptually similar to {@link Cursor}, but the latter is used for query execution rather than
 * historical segments creation (as TimeAndDimsPointer). If those abstractions could be collapsed (and if it is
 * worthwhile) is yet to be determined.
 */
public class TimeAndDimsPointer implements Comparable<TimeAndDimsPointer>
{
  final ColumnValueSelector timestampSelector;
  /**
   * This collection of dimension selectors is stored as array rather than List in order to minimize indirection in hot
   * spots, in particular in {@link #compareTo}.
   *
   * The same reasoning is applied to {@link #dimensionSelectorComparators} and {@link #metricSelectors}.
   */
  final ColumnValueSelector[] dimensionSelectors;
  private final List<DimensionHandler> dimensionHandlers;
  /**
   * Because of polymorphic nature of {@link ColumnValueSelector}, a priori there are many ways to compare two arbitrary
   * dimension column value selectors. dimensionSelectorComparators encapsulate the information how specifically we
   * should compare ColumnValueSelectors in each dimension. See {@link
   * DimensionHandler#getEncodedValueSelectorComparator()}.
   */
  private final Comparator<ColumnValueSelector>[] dimensionSelectorComparators;
  final ColumnValueSelector[] metricSelectors;
  private final List<String> metricNames;

  /**
   * TimeAndDimsPointer constructor intentionally takes dimensionSelectors and metricSelectors as arrays and doesn't
   * copy them "defensively", to allow to reuse arrays during transformations of TimeAndDimsPointers and {@link
   * RowPointer}s in some cases, particularly in {@link
   * RowCombiningTimeAndDimsIterator#RowCombiningTimeAndDimsIterator}, in order to reduce the number of array objects
   * tapped on each iteration during index merge process.
   */
  TimeAndDimsPointer(
      ColumnValueSelector timestampSelector,
      ColumnValueSelector[] dimensionSelectors,
      List<DimensionHandler> dimensionHandlers,
      ColumnValueSelector[] metricSelectors,
      List<String> metricNames
  )
  {
    this.timestampSelector = timestampSelector;
    Preconditions.checkArgument(dimensionSelectors.length == dimensionHandlers.size());
    this.dimensionSelectors = dimensionSelectors;
    this.dimensionHandlers = dimensionHandlers;
    //noinspection unchecked
    this.dimensionSelectorComparators = dimensionHandlers
        .stream()
        .map(DimensionHandler::getEncodedValueSelectorComparator)
        .toArray(Comparator[]::new);
    Preconditions.checkArgument(metricSelectors.length == metricNames.size());
    this.metricSelectors = metricSelectors;
    this.metricNames = metricNames;
  }

  public long getTimestamp()
  {
    return timestampSelector.getLong();
  }

  ColumnValueSelector getDimensionSelector(int dimIndex)
  {
    return dimensionSelectors[dimIndex];
  }

  int getNumDimensions()
  {
    return dimensionSelectors.length;
  }

  List<DimensionHandler> getDimensionHandlers()
  {
    return dimensionHandlers;
  }

  ColumnValueSelector getMetricSelector(int metricIndex)
  {
    return metricSelectors[metricIndex];
  }

  public int getNumMetrics()
  {
    return metricSelectors.length;
  }

  List<String> getMetricNames()
  {
    return metricNames;
  }

  TimeAndDimsPointer withDimensionSelectors(ColumnValueSelector[] newDimensionSelectors)
  {
    return new TimeAndDimsPointer(
        timestampSelector,
        newDimensionSelectors,
        dimensionHandlers,
        metricSelectors,
        getMetricNames()
    );
  }

  /**
   * Compares time column value and dimension column values, but not metric column values.
   */
  @Override
  public int compareTo(@Nonnull TimeAndDimsPointer rhs)
  {
    long timestamp = getTimestamp();
    long rhsTimestamp = rhs.getTimestamp();
    int timestampDiff = Long.compare(timestamp, rhsTimestamp);
    if (timestampDiff != 0) {
      return timestampDiff;
    }
    for (int dimIndex = 0; dimIndex < dimensionSelectors.length; dimIndex++) {
      int dimDiff = dimensionSelectorComparators[dimIndex].compare(
          dimensionSelectors[dimIndex],
          rhs.dimensionSelectors[dimIndex]
      );
      if (dimDiff != 0) {
        return dimDiff;
      }
    }
    return 0;
  }

  @SuppressWarnings("Contract")
  @Override
  public boolean equals(Object obj)
  {
    throw new UnsupportedOperationException("Should not compare TimeAndDimsPointers using equals(), only compareTo()");
  }

  @Override
  public int hashCode()
  {
    throw new UnsupportedOperationException("Should not compute hashCode() on TimeAndDimsPointer");
  }

  Object[] getDimensionValuesForDebug()
  {
    return Arrays.stream(dimensionSelectors).map(ColumnValueSelector::getObject).toArray();
  }

  @Override
  public String toString()
  {
    return "TimeAndDimsPointer{" +
           "timestamp=" + DateTimes.utc(getTimestamp()) +
           ", dimensions=" + getDimensionNamesToValuesForDebug() +
           ", metrics=" + getMetricNamesToValuesForDebug() +
           '}';
  }

  Map<String, Object> getDimensionNamesToValuesForDebug()
  {
    LinkedHashMap<String, Object> result = new LinkedHashMap<>();
    for (int i = 0; i < getNumDimensions(); i++) {
      Object value = dimensionSelectors[i].getObject();
      if (value instanceof Object[]) {
        value = Arrays.asList((Object[]) value);
      }
      result.put(dimensionHandlers.get(i).getDimensionName(), value);
    }
    return result;
  }

  Map<String, Object> getMetricNamesToValuesForDebug()
  {
    LinkedHashMap<String, Object> result = new LinkedHashMap<>();
    for (int i = 0; i < getNumMetrics(); i++) {
      result.put(metricNames.get(i), metricSelectors[i].getObject());
    }
    return result;
  }
}
