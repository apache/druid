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

import com.oath.oak.OakRBuffer;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.incremental.IncrementalIndex.DimensionDesc;

import java.nio.ByteBuffer;
import java.util.List;

public class OakIncrementalIndexRow extends IncrementalIndexRow
{
  private final ByteBuffer dimensions;
  private final OakRBuffer aggregations;
  private Integer dimsLength;

  public OakIncrementalIndexRow(ByteBuffer dimentions,
                                List<DimensionDesc> dimensionDescsList,
                                OakRBuffer aggregations)
  {
    super(0, dimensionDescsList);
    this.dimensions = dimentions;
    this.dimsLength = null; // lazy initialization
    this.aggregations = aggregations;
  }

  public OakRBuffer getAggregations()
  {
    return aggregations;
  }

  @Override
  public long getTimestamp()
  {
    return dimensions.getLong(dimensions.position() + OakUtils.TIME_STAMP_INDEX);
  }

  @Override
  public int getDimsLength()
  {
    // Read length only once
    if (dimsLength == null) {
      dimsLength = dimensions.getInt(dimensions.position() + OakUtils.DIMS_LENGTH_INDEX);
    }
    return dimsLength;
  }

  @Override
  public Object getDim(int dimIndex)
  {
    if (dimIndex >= getDimsLength()) {
      return null;
    }
    return getDimValue(dimIndex);
  }

  @Override
  public int getRowIndex()
  {
    return dimensions.getInt(dimensions.position() + OakUtils.ROW_INDEX_INDEX);
  }

  @Override
  void setRowIndex(int rowIndex)
  {
    throw new UnsupportedOperationException();
  }

  /**
   * bytesInMemory estimates the size of the serialized IncrementalIndexRow key.
   * Each serialized IncrementalRoeIndex contains:
   * 1. a timeStamp
   * 2. the dims array length
   * 3. the rowIndex
   * 4. the serialization of each dim
   * 5. the array (for dims with capabilities of a String ValueType)
   *
   * @return long estimated bytesInMemory
   */
  @Override
  public long estimateBytesInMemory()
  {

    long sizeInBytes = Long.BYTES + 2 * Integer.BYTES;
    for (int dimIndex = 0; dimIndex < getDimsLength(); dimIndex++) {
      sizeInBytes += OakUtils.ALLOC_PER_DIM;
      int dimType = getDimType(dimIndex);
      if (dimType == ValueType.STRING.ordinal()) {
        int dimIndexInBuffer = getDimIndexInBuffer(dimIndex);
        int arraySize = dimensions.getInt(dimIndexInBuffer + OakUtils.ARRAY_LENGTH_OFFSET);
        sizeInBytes += (arraySize * Integer.BYTES);
      }
    }
    return sizeInBytes;
  }




  /* ---------------- OakRBuffer utils -------------- */

  private int getDimIndexInBuffer(int dimIndex)
  {
    if (dimIndex >= getDimsLength()) {
      return OakUtils.NO_DIM;
    }
    return dimensions.position() + OakUtils.DIMS_INDEX + dimIndex * OakUtils.ALLOC_PER_DIM;
  }

  private Object getDimValue(int dimIndex)
  {
    Object dimObject = null;
    if (dimIndex >= getDimsLength()) {
      return null;
    }
    int dimIndexInBuffer = getDimIndexInBuffer(dimIndex);
    int dimType = dimensions.getInt(dimIndexInBuffer);
    if (dimType == OakUtils.NO_DIM) {
      return null;
    } else if (dimType == ValueType.DOUBLE.ordinal()) {
      dimObject = dimensions.getDouble(dimIndexInBuffer + OakUtils.DATA_OFFSET);
    } else if (dimType == ValueType.FLOAT.ordinal()) {
      dimObject = dimensions.getFloat(dimIndexInBuffer + OakUtils.DATA_OFFSET);
    } else if (dimType == ValueType.LONG.ordinal()) {
      dimObject = dimensions.getLong(dimIndexInBuffer + OakUtils.DATA_OFFSET);
    } else if (dimType == ValueType.STRING.ordinal()) {
      int arrayIndexOffset = dimensions.getInt(dimIndexInBuffer + OakUtils.ARRAY_INDEX_OFFSET);
      int arrayIndex = dimensions.position() + arrayIndexOffset;
      int arraySize = dimensions.getInt(dimIndexInBuffer + OakUtils.ARRAY_LENGTH_OFFSET);
      int[] array = new int[arraySize];
      for (int i = 0; i < arraySize; i++) {
        array[i] = dimensions.getInt(arrayIndex);
        arrayIndex += Integer.BYTES;
      }
      dimObject = array;
    }

    return dimObject;
  }

  private int getDimType(int dimIndex)
  {
    if (dimIndex >= getDimsLength()) {
      return OakUtils.NO_DIM;
    }
    int dimIndexInBuffer = getDimIndexInBuffer(dimIndex);
    return dimensions.getInt(dimIndexInBuffer);
  }
}
