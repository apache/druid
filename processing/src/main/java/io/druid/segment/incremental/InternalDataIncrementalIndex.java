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

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import io.druid.segment.DimensionIndexer;
import io.druid.segment.column.ColumnCapabilitiesImpl;
import io.druid.segment.column.ValueType;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;

/**
 */
public abstract class InternalDataIncrementalIndex<AggregatorType> extends IncrementalIndex<AggregatorType>
{
  // When serializing an object from TimeAndDims.dims, we use:
  // 1. 4 bytes for representing its type (Double, Float, Long or String)
  // 2. 8 bytes for saving its value or the array position and length (in the case of String)
  static final Integer ALLOC_PER_DIM = 12;
  static final Integer NO_DIM = -1;
  static final Integer TIME_STAMP_INDEX = 0;
  static final Integer DIMS_LENGTH_INDEX = TIME_STAMP_INDEX + Long.BYTES;
  static final Integer DIMS_INDEX = DIMS_LENGTH_INDEX + Integer.BYTES;

  protected InternalDataIncrementalIndex(
      IncrementalIndexSchema incrementalIndexSchema,
      boolean deserializeComplexMetrics,
      boolean reportParseExceptions,
      boolean concurrentEventAdd
  )
  {
    super(incrementalIndexSchema, deserializeComplexMetrics, reportParseExceptions, concurrentEventAdd);
  }

<<<<<<< HEAD
  static int getDimIndexInBuffer(ByteBuffer buff, int dimIndex)
  {
    int dimsLength = getDimsLength(buff);
=======
  static int getDimIndexInBuffer(ByteBuffer buff, int dimIndex) {
    int dimsLength = getDimsLengthFromBuffer(buff);
>>>>>>> 8161fd5df6aaa009397190d7014b772bda5eabd5
    if (dimIndex >= dimsLength) {
      return NO_DIM;
    }
    return DIMS_INDEX + dimIndex * ALLOC_PER_DIM;
  }

  ByteBuffer timeAndDimsSerialization(TimeAndDims timeAndDims)
  {
    Object[] dims = timeAndDims.getDims();

    // When the dimensionDesc's capabilities are of type ValueType.STRING,
    // the object in timeAndDims.dims is of type int[].
    // In this case, we need to know the array size before allocating the ByteBuffer.
    int sumOfArrayLengths = 0;
    for (int i = 0; i < dims.length; i++) {
      Object dim = dims[i];
      if (dim == null) {
        continue;
      }
      if (getDimValueType(i) == ValueType.STRING) {
        sumOfArrayLengths += ((int[]) dim).length;
      }
    }

    // The ByteBuffer will contain:
    // 1. the timeStamp
    // 2. dims.length
    // 3. the serialization of each dim
    // 4. the array (for dims with capabilities of a String ValueType)
    int allocSize = Long.BYTES + Integer.BYTES + ALLOC_PER_DIM * dims.length + Integer.BYTES * sumOfArrayLengths;
    ByteBuffer buf = ByteBuffer.allocate(allocSize);
    buf.putLong(timeAndDims.getTimestamp());
    buf.putInt(dims.length);
    int currDimsIndex = DIMS_INDEX;
    int currArrayIndex = DIMS_INDEX + ALLOC_PER_DIM * dims.length;
    for (int dimIndex = 0; dimIndex < dims.length; dimIndex++) {
      ValueType valueType = getDimValueType(dimIndex);
      if (valueType == null) {
        buf.putInt(currDimsIndex, NO_DIM);
        currDimsIndex += ALLOC_PER_DIM;
        continue;
      }
      switch (valueType) {
        case LONG:
          buf.putInt(currDimsIndex, valueType.ordinal());
          currDimsIndex += Integer.BYTES;
          buf.putLong(currDimsIndex, (Long) dims[dimIndex]);
          currDimsIndex += Long.BYTES;
          break;
        case FLOAT:
          buf.putInt(currDimsIndex, valueType.ordinal());
          currDimsIndex += Integer.BYTES;
          buf.putFloat(currDimsIndex, (Float) dims[dimIndex]);
          currDimsIndex += Long.BYTES;
          break;
        case DOUBLE:
          buf.putInt(currDimsIndex, valueType.ordinal());
          currDimsIndex += Integer.BYTES;
          buf.putDouble(currDimsIndex, (Double) dims[dimIndex]);
          currDimsIndex += Long.BYTES;
          break;
        case STRING:
          buf.putInt(currDimsIndex, valueType.ordinal());
          currDimsIndex += Integer.BYTES;
          buf.putInt(currDimsIndex, currArrayIndex);
          currDimsIndex += Integer.BYTES;
          int[] array = (int[]) dims[dimIndex];
          buf.putInt(currDimsIndex, array.length);
          currDimsIndex += Integer.BYTES;
          for (int j = 0; j < array.length; j++) {
            buf.putInt(currArrayIndex, array[j]);
            currArrayIndex += Integer.BYTES;
          }
          break;
        default:
          buf.putInt(currDimsIndex, NO_DIM);
          currDimsIndex += ALLOC_PER_DIM;
      }
    }
    return buf;
  }

  TimeAndDims timeAndDimsDeserialization(ByteBuffer buff) {
    long timeStamp = getTimeStampFromBuffer(buff);
    int dimsLength = getDimsLengthFromBuffer(buff);
    Object[] dims = new Object[dimsLength];
    for (int dimIndex = 0; dimIndex < dimsLength; dimIndex++) {
      Object dim = getDimValueFromBuffer(buff, dimIndex);
      dims[dimIndex] = dim;
    }
    return new TimeAndDims(timeStamp, dims, dimensionDescsList, TimeAndDims.EMPTY_ROW_INDEX);
  }

  private ValueType getDimValueType(int dimIndex)
  {
    DimensionDesc dimensionDesc = getDimensions().get(dimIndex);
    if (dimensionDesc == null) {
      return null;
    }
    ColumnCapabilitiesImpl capabilities = dimensionDesc.getCapabilities();
    if (capabilities == null) {
      return null;
    }
    return capabilities.getType();
  }

  static long getTimestamp(ByteBuffer buff)
  {
    return buff.getLong(TIME_STAMP_INDEX);
  }

  static int getDimsLength(ByteBuffer buff)
  {
    return buff.getInt(DIMS_LENGTH_INDEX);
  }

  static Object getDimValue(ByteBuffer buff, int dimIndex)
  {
    Object dimObject = null;
    int dimsLength = getDimsLengthFromBuffer(buff);
    if (dimIndex >= dimsLength) {
      return null;
    }
    int dimType = buff.getInt(getDimIndexInBuffer(buff, dimIndex));
    if (dimType == NO_DIM) {
      return null;
    } else if (dimType == ValueType.DOUBLE.ordinal()) {
      dimObject = buff.getDouble(getDimIndexInBuffer(buff, dimIndex) + Integer.BYTES);
    } else if (dimType == ValueType.FLOAT.ordinal()) {
      dimObject = buff.getFloat(getDimIndexInBuffer(buff, dimIndex) + Integer.BYTES);
    } else if (dimType == ValueType.LONG.ordinal()) {
      dimObject = buff.getLong(getDimIndexInBuffer(buff, dimIndex) + Integer.BYTES);
    } else if (dimType == ValueType.STRING.ordinal()) {
      int arrayIndex = buff.getInt(getDimIndexInBuffer(buff, dimIndex) + Integer.BYTES);
      int arraySize = buff.getInt(getDimIndexInBuffer(buff, dimIndex) + 2 * Integer.BYTES);
      int[] array = new int[arraySize];
      for (int i = 0; i < arraySize; i++) {
        array[i] = buff.getInt(arrayIndex);
        arrayIndex += Integer.BYTES;
      }
      dimObject = array;
    }

    return dimObject;
  }

<<<<<<< HEAD
  static boolean checkDimsAllNull(ByteBuffer buff)
  {
    int dimsLength = getDimsLength(buff);
=======
  private static boolean checkDimsAllNullFromBuffer(ByteBuffer buff) {
    int dimsLength = getDimsLengthFromBuffer(buff);
>>>>>>> 8161fd5df6aaa009397190d7014b772bda5eabd5
    for (int index = 0; index < dimsLength; index++) {
      if (buff.getInt(getDimIndexInBuffer(buff, index)) != NO_DIM) {
        return false;
      }
    }
    return true;
  }

  public final Comparator<ByteBuffer> dimsByteBufferComparator()
  {
    return new TimeAndDimsByteBuffersComp(dimensionDescsList);
  }

  static final class TimeAndDimsByteBuffersComp implements Comparator<ByteBuffer>
  {
    private List<DimensionDesc> dimensionDescsList;

    public TimeAndDimsByteBuffersComp(List<DimensionDesc> dimensionDescsList)
    {
      this.dimensionDescsList = dimensionDescsList;
    }

    @Override
    public int compare(ByteBuffer lhs, ByteBuffer rhs)
    {
      int retVal = Longs.compare(getTimeStampFromBuffer(lhs), getTimeStampFromBuffer(rhs));
      int numComparisons = Math.min(getDimsLengthFromBuffer(lhs), getDimsLengthFromBuffer(rhs));

      int dimIndex = 0;
      while (retVal == 0 && dimIndex < numComparisons) {
        int lhsType = lhs.getInt(getDimIndexInBuffer(lhs, dimIndex));
        int rhsType = rhs.getInt(getDimIndexInBuffer(rhs, dimIndex));

        if (lhsType == NO_DIM) {
          if (rhsType == NO_DIM) {
            ++dimIndex;
            continue;
          }
          return -1;
        }

        if (rhsType == NO_DIM) {
          return 1;
        }

        final DimensionIndexer indexer = dimensionDescsList.get(dimIndex).getIndexer();
        Object lhsObject = getDimValueFromBuffer(lhs, dimIndex);
        Object rhsObject = getDimValueFromBuffer(rhs, dimIndex);
        retVal = indexer.compareUnsortedEncodedKeyComponents(lhsObject, rhsObject);
        ++dimIndex;
      }

      if (retVal == 0) {
        int lengthDiff = Ints.compare(getDimsLengthFromBuffer(lhs), getDimsLengthFromBuffer(rhs));
        if (lengthDiff == 0) {
          return 0;
        }
        ByteBuffer largerDims = lengthDiff > 0 ? lhs : rhs;
        return checkDimsAllNullFromBuffer(largerDims) ? 0 : lengthDiff;
      }

      return retVal;
    }
  }
}
