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

import java.util.List;
import java.util.function.IntSupplier;

/**
 * Extension of {@link TimeAndDimsPointer}, that keeps "row number" of the current data point in some collection of data
 * points, that actually makes this data point to be called "row", and the collection, thus, "collection of rows".
 * However, "row number" doesn't affect the {@link TimeAndDimsPointer#compareTo} contract. RowPointers could be compared
 * to TimeAndDimsPointers interchangeably.
 */
public final class RowPointer extends TimeAndDimsPointer
{
  final IntSupplier rowNumPointer;

  /**
   * This field is not a part of "row" abstraction. It is used only between {@link MergingRowIterator} and
   * {@link RowCombiningTimeAndDimsIterator} to transmit "index numbers" without adding more referencing data
   * structures, i. e. to improve locality. Otherwise, this field is just not used.
   */
  private int indexNum;

  public RowPointer(
      ColumnValueSelector timestampSelector,
      ColumnValueSelector[] dimensionSelectors,
      List<DimensionHandler> dimensionHandlers,
      ColumnValueSelector[] metricSelectors,
      List<String> metricNames,
      IntSupplier rowNumPointer
  )
  {
    super(timestampSelector, dimensionSelectors, dimensionHandlers, metricSelectors, metricNames);
    this.rowNumPointer = rowNumPointer;
  }

  public int getRowNum()
  {
    return rowNumPointer.getAsInt();
  }

  @Override
  RowPointer withDimensionSelectors(ColumnValueSelector[] newDimensionSelectors)
  {
    return new RowPointer(
        timestampSelector,
        newDimensionSelectors,
        getDimensionHandlers(),
        metricSelectors,
        getMetricNames(),
        rowNumPointer
    );
  }

  void setIndexNum(int indexNum)
  {
    this.indexNum = indexNum;
  }

  int getIndexNum()
  {
    return indexNum;
  }

  @Override
  public String toString()
  {
    return "RowPointer{" +
           "indexNum=" + indexNum +
           ", rowNumber=" + rowNumPointer.getAsInt() +
           ", timestamp=" + getTimestamp() +
           ", dimensions=" + getDimensionNamesToValuesForDebug() +
           ", metrics=" + getMetricNamesToValuesForDebug() +
           '}';
  }
}
