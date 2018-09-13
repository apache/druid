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

package org.apache.druid.segment.incremental;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.segment.DimensionIndexer;

import javax.annotation.Nullable;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class IncrementalIndexRow
{
  public static final int EMPTY_ROW_INDEX = -1;

  private long timestamp;
  private Object[] dims;
  protected List<IncrementalIndex.DimensionDesc> dimensionDescsList;

  /**
   * rowIndex is not checked in {@link #equals} and {@link #hashCode} on purpose. IncrementalIndexRow acts as a Map key
   * and "entry" object (rowIndex is the "value") at the same time. This is done to reduce object indirection and
   * improve locality, and avoid boxing of rowIndex as Integer, when stored in JDK collection:
   * {@link ExternalDataIncrementalIndex.RollupFactsHolder} needs concurrent collections, that are not present in fastutil.
   */
  private int rowIndex;
  private long dimsKeySize;

  protected IncrementalIndexRow() {}

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

  protected IncrementalIndexRow(
      long timestamp,
      Object[] dims,
      List<IncrementalIndex.DimensionDesc> dimensionDescsList,
      long dimsKeySize
  )
  {
    this.timestamp = timestamp;
    this.dims = dims;
    this.dimensionDescsList = dimensionDescsList;
    this.dimsKeySize = dimsKeySize;
  }

  static IncrementalIndexRow createTimeAndDimswithDimsKeySize(
      long timestamp,
      Object[] dims,
      List<IncrementalIndex.DimensionDesc> dimensionDescsList,
      long dimsKeySize
  )
  {
    return new IncrementalIndexRow(timestamp, dims, dimensionDescsList, dimsKeySize);
  }

  public long getTimestamp()
  {
    return timestamp;
  }

  public int getDimsLength()
  {
    return dims == null ? 0 : dims.length;
  }

  public Object getDim(int dimIndex)
  {
    if (dims == null || dimIndex >= dims.length) {
      return null;
    }
    return dims[dimIndex];
  }

  public boolean isNull(int dimIndex)
  {
    return dimIndex >= getDimsLength() || dims[dimIndex] == null;
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

  public int copyStringDim(int dimIndex, int[] dst)
  {
    if (dims == null || dimIndex >= dims.length) {
      return 0;
    }
    int[] stringDim = (int[]) dims[dimIndex];
    if (stringDim != null) {
      if (dst.length < stringDim.length) {
        dst = new int[stringDim.length];
      }
      System.arraycopy((int[]) dims[dimIndex], 0, dst, 0, stringDim.length);
      return stringDim.length;
    } else {
      return 0;
    }
  }

  public int calcStringDimSize(int dimIndex)
  {
    if (dims == null || dimIndex >= dims.length || dims[dimIndex] == null) {
      return 0;
    }

    return ((int[]) dims[dimIndex]).length;

  }

  /**
   * bytesInMemory estimates the size of IncrementalIndexRow key, it takes into account the timestamp(long),
   * dims(Object Array) and dimensionDescsList(List). Each of these are calculated as follows:
   * <ul>
   * <li> timestamp : Long.BYTES
   * <li> dims array : Integer.BYTES * array length + Long.BYTES (dims object) + dimsKeySize(passed via constructor)
   * <li> dimensionDescList : Long.BYTES (shared pointer)
   * <li> dimsKeySize : this value is passed in based on the key type (int, long, double, String etc.)
   * </ul>
   *
   * @return long estimated bytesInMemory
   */
  public long estimateBytesInMemory()
  {
    long sizeInBytes = Long.BYTES + Integer.BYTES * dims.length + Long.BYTES + Long.BYTES;
    sizeInBytes += dimsKeySize;
    return sizeInBytes;
  }

  @Override
  public String toString()
  {
    return "IncrementalIndexRow{" +
            "timestamp=" + DateTimes.utc(getTimestamp()) +
            ", dims=" + Lists.transform(
            Arrays.asList(getDims()), new Function<Object, Object>()
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

    if (getTimestamp() != that.getTimestamp()) {
      return false;
    }
    if (getDimsLength() != that.getDimsLength()) {
      return false;
    }
    for (int i = 0; i < getDimsLength(); i++) {
      final DimensionIndexer indexer = dimensionDescsList.get(i).getIndexer();
      if (!indexer.checkUnsortedEncodedKeyComponentsEqual(getDim(i), that.getDim(i))) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode()
  {
    int hash = (int) getTimestamp();
    for (int i = 0; i < getDimsLength(); i++) {
      final DimensionIndexer indexer = dimensionDescsList.get(i).getIndexer();
      hash = 31 * hash + indexer.getUnsortedEncodedKeyComponentHashCode(dims[i]);
    }
    return hash;
  }
}
