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

package io.druid.segment.incremental;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import io.druid.java.util.common.DateTimes;
import io.druid.segment.DimensionIndexer;

import javax.annotation.Nullable;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public final class IncrementalIndexRow
{
  public static final int EMPTY_ROW_INDEX = -1;

  final long timestamp;
  final Object[] dims;
  private final List<IncrementalIndex.DimensionDesc> dimensionDescsList;

  /**
   * rowIndex is not checked in {@link #equals} and {@link #hashCode} on purpose. IncrementalIndexRow acts as a Map key
   * and "entry" object (rowIndex is the "value") at the same time. This is done to reduce object indirection and
   * improve locality, and avoid boxing of rowIndex as Integer, when stored in JDK collection:
   * {@link IncrementalIndex.RollupFactsHolder} needs concurrent collections, that are not present in fastutil.
   */
  private int rowIndex;

  IncrementalIndexRow(
      long timestamp,
      Object[] dims,
      List<IncrementalIndex.DimensionDesc> dimensionDescsList
  )
  {
    this(timestamp, dims, dimensionDescsList, EMPTY_ROW_INDEX);
  }

  IncrementalIndexRow(
      long timestamp,
      Object[] dims,
      List<IncrementalIndex.DimensionDesc> dimensionDescsList,
      int rowIndex
  )
  {
    this.timestamp = timestamp;
    this.dims = dims;
    this.dimensionDescsList = dimensionDescsList;
    this.rowIndex = rowIndex;
  }

  public long getTimestamp()
  {
    return timestamp;
  }

  public Object[] getDims()
  {
    return dims;
  }

  public int getRowIndex()
  {
    return rowIndex;
  }

  void setRowIndex(int rowIndex)
  {
    this.rowIndex = rowIndex;
  }

  @Override
  public String toString()
  {
    return "IncrementalIndexRow{" +
           "timestamp=" + DateTimes.utc(timestamp) +
           ", dims=" + Lists.transform(
        Arrays.asList(dims), new Function<Object, Object>()
        {
          @Override
          public Object apply(@Nullable Object input)
          {
            if (input == null || Array.getLength(input) == 0) {
              return Collections.singletonList("null");
            }
            return Collections.singletonList(input);
          }
        }
    ) + '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    IncrementalIndexRow that = (IncrementalIndexRow) o;

    if (timestamp != that.timestamp) {
      return false;
    }
    if (dims.length != that.dims.length) {
      return false;
    }
    for (int i = 0; i < dims.length; i++) {
      final DimensionIndexer indexer = dimensionDescsList.get(i).getIndexer();
      if (!indexer.checkUnsortedEncodedKeyComponentsEqual(dims[i], that.dims[i])) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode()
  {
    int hash = (int) timestamp;
    for (int i = 0; i < dims.length; i++) {
      final DimensionIndexer indexer = dimensionDescsList.get(i).getIndexer();
      hash = 31 * hash + indexer.getUnsortedEncodedKeyComponentHashCode(dims[i]);
    }
    return hash;
  }
}
