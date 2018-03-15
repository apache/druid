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

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import io.druid.segment.DimensionIndexer;
import io.druid.segment.column.ColumnCapabilitiesImpl;
import io.druid.segment.column.ValueType;

import java.nio.ByteBuffer;
import java.util.Comparator;

/**
 */
public abstract class InternalDataIncrementalIndex<AggregatorType> extends IncrementalIndex<AggregatorType>
{
  // When serializing an object from TimeAndDims.dims, we use:
  // 1. 4 bytes for representing its type (Double, Float, Long or String)
  // 2. 8 bytes for saving its value or the array position and length (in the case of String)
  static final Integer ALLOC_PER_DIM = 12;
  static  final Integer NO_DIM = -1;

  InternalDataIncrementalIndex(
      IncrementalIndexSchema incrementalIndexSchema,
      boolean deserializeComplexMetrics,
      boolean reportParseExceptions,
      boolean concurrentEventAdd
  )
  {
    super(incrementalIndexSchema, deserializeComplexMetrics, reportParseExceptions, concurrentEventAdd);
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
    int currDimsIndex = Long.BYTES + Integer.BYTES;
    int currArrayIndex = Long.BYTES + Integer.BYTES + ALLOC_PER_DIM * dims.length;
    for (int i = 0; i < dims.length; i++) {
      ValueType valueType = getDimValueType(i);
      if (valueType == null) {
        buf.putInt(currDimsIndex, NO_DIM);
        currDimsIndex += ALLOC_PER_DIM;
        continue;
      }
      switch (valueType) {
        case LONG:
          buf.putInt(currDimsIndex, valueType.ordinal());
          currDimsIndex += Integer.BYTES;
          buf.putLong(currDimsIndex, (Long) dims[i]);
          currDimsIndex += Long.BYTES;
          break;
        case FLOAT:
          buf.putInt(currDimsIndex, valueType.ordinal());
          currDimsIndex += Integer.BYTES;
          buf.putFloat(currDimsIndex, (Float) dims[i]);
          currDimsIndex += Long.BYTES;
          break;
        case DOUBLE:
          buf.putInt(currDimsIndex, valueType.ordinal());
          currDimsIndex += Integer.BYTES;
          buf.putDouble(currDimsIndex, (Double) dims[i]);
          currDimsIndex += Long.BYTES;
          break;
        case STRING:
          buf.putInt(currDimsIndex, valueType.ordinal());
          currDimsIndex += Integer.BYTES;
          buf.putInt(currDimsIndex, currArrayIndex);
          currDimsIndex += Integer.BYTES;
          int[] array = (int[]) dims[i];
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

  ValueType getDimValueType(int dimIndex)
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

  static DimensionIndexer getDimIndexer(int dimIndex)
  {
    synchronized (dimensionDescs) {
      return ImmutableList.copyOf(dimensionDescs.values());
    }
    DimensionDesc dimensionDesc = getDimensions().get(dimIndex);
    if (dimensionDesc == null) {
      return null;
    }
    return dimensionDesc.getIndexer();
  }

  static final class TimeAndDimsByteBuffersComp implements Comparator<ByteBuffer>
  {
    public TimeAndDimsByteBuffersComp() {}

    @Override
    public int compare(ByteBuffer lhs, ByteBuffer rhs)
    {
      int retVal = Longs.compare(lhs.getLong(0), rhs.getLong(0));
      int numComparisons = Math.min(lhs.getInt(Long.BYTES), rhs.getInt(Long.BYTES)); // dims length

      int index = 0;
      while (retVal == 0 && index < numComparisons) {
        int lhsType = lhs.getInt(Long.BYTES + Integer.BYTES + index * ALLOC_PER_DIM);
        int rhsType = rhs.getInt(Long.BYTES + Integer.BYTES + index * ALLOC_PER_DIM);

        if (lhsType == NO_DIM) {
          if (rhsType == NO_DIM) {
            ++index;
            continue;
          }
          return -1;
        }

        if (rhsType == NO_DIM) {
          return 1;
        }

        final DimensionIndexer indexer = getDimIndexer(index);
        retVal = indexer.compareUnsortedEncodedKeyComponents(lhsIdxs, rhsIdxs);
        ++index;
      }

      if (retVal == 0) {
        int lengthDiff = Ints.compare(lhs.getInt(Long.BYTES), rhs.getInt(Long.BYTES)); // dims length
        if (lengthDiff == 0) {
          return 0;
        }
        Object[] largerDims = lengthDiff > 0 ? lhs.dims : rhs.dims;
        return allNull(largerDims, numComparisons) ? 0 : lengthDiff;
      }

      return retVal;
    }
  }
}
