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

package org.apache.druid.segment.incremental.oak;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.yahoo.oak.OakBuffer;
import com.yahoo.oak.OakComparator;
import com.yahoo.oak.OakScopedReadBuffer;
import com.yahoo.oak.OakScopedWriteBuffer;
import com.yahoo.oak.OakSerializer;
import com.yahoo.oak.OakUnsafeDirectBuffer;
import com.yahoo.oak.UnsafeUtils;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.DimensionIndexer;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexRow;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

/**
 * Responsible for the serialization, deserialization, and comparison of keys.
 *
 * It stores the key in an off-heap buffer in the following structure:
 *  - Global metadata (buffer offset + _):
 *      +0: timestamp (long)
 *      +8: dims length (int)
 *     +12: row index (int)
 *    (total of 16 bytes)
 *  - Followed by the dimensions one after the other (buffer offset + 16 + dimIdx*12 + _):
 *     +0: value type (int)
 *     +4: data (int/long/float/double)
 *    (12 bytes per dimension)
 *    Note: For string dimension (int array), the data includes:
 *           +4: the offset of the int array in the buffer (int)
 *           +8: the length of the int array (int)
 *  - The string dimensions arrays are stored after all the dims' data (buffer offset + 16 + dimsLength*12 + _).
 *
 *  Note: the specified offsets are true in most cases, but other JVM implementations may have
 *        different offsets, depending on the size of the primitives in bytes.
 *        The offset calculation below is robust to JVM implementation changes.
 */
public final class OakKey
{
  // The off-heap buffer offsets (buffer offset + _)
  static final int TIME_STAMP_OFFSET = 0;
  static final int DIMS_LENGTH_OFFSET = TIME_STAMP_OFFSET + Long.BYTES;
  static final int ROW_INDEX_OFFSET = DIMS_LENGTH_OFFSET + Integer.BYTES;
  static final int DIMS_OFFSET = ROW_INDEX_OFFSET + Integer.BYTES;
  static final int DIM_VALUE_TYPE_OFFSET = 0;
  static final int DIM_DATA_OFFSET = DIM_VALUE_TYPE_OFFSET + Integer.BYTES;
  static final int STRING_DIM_ARRAY_POS_OFFSET = DIM_DATA_OFFSET;
  static final int STRING_DIM_ARRAY_LENGTH_OFFSET = STRING_DIM_ARRAY_POS_OFFSET + Integer.BYTES;
  static final int SIZE_PER_DIM = DIM_DATA_OFFSET + Collections.max(Arrays.asList(
          // Common data types
          Integer.BYTES,
          Long.BYTES,
          Float.BYTES,
          Double.BYTES,
          // String dimension data type
          Integer.BYTES * 2
  ));

  // Dimension types
  static final ValueType[] VALUE_ORDINAL_TYPES = ValueType.values();
  // Marks a null dimension
  static final int NULL_DIM = -1;

  private OakKey()
  {
  }

  static long getKeyAddress(OakBuffer buffer)
  {
    return ((OakUnsafeDirectBuffer) buffer).getAddress();
  }

  static long getTimestamp(long address)
  {
    return UnsafeUtils.getLong(address + TIME_STAMP_OFFSET);
  }

  static int getRowIndex(long address)
  {
    return UnsafeUtils.getInt(address + ROW_INDEX_OFFSET);
  }

  static int getDimsLength(long address)
  {
    return UnsafeUtils.getInt(address + DIMS_LENGTH_OFFSET);
  }

  static int getDimOffsetInBuffer(int dimIndex)
  {
    return DIMS_OFFSET + (dimIndex * SIZE_PER_DIM);
  }

  static boolean isValueTypeNull(int dimValueTypeID)
  {
    return dimValueTypeID < 0 || dimValueTypeID >= VALUE_ORDINAL_TYPES.length;
  }

  static boolean isDimNull(long address, int dimIndex)
  {
    long dimAddress = address + getDimOffsetInBuffer(dimIndex);
    return isValueTypeNull(UnsafeUtils.getInt(dimAddress + DIM_VALUE_TYPE_OFFSET));
  }

  @Nullable
  static Object getDim(long address, int dimIndex)
  {
    long dimAddress = address + getDimOffsetInBuffer(dimIndex);
    int dimValueTypeID = UnsafeUtils.getInt(dimAddress + DIM_VALUE_TYPE_OFFSET);

    if (isValueTypeNull(dimValueTypeID)) {
      return null;
    }

    switch (VALUE_ORDINAL_TYPES[dimValueTypeID]) {
      case DOUBLE:
        return UnsafeUtils.getDouble(dimAddress + DIM_DATA_OFFSET);
      case FLOAT:
        return UnsafeUtils.getFloat(dimAddress + DIM_DATA_OFFSET);
      case LONG:
        return UnsafeUtils.getLong(dimAddress + DIM_DATA_OFFSET);
      case STRING:
        int arrayPos = UnsafeUtils.getInt(dimAddress + STRING_DIM_ARRAY_POS_OFFSET);
        int arraySize = UnsafeUtils.getInt(dimAddress + STRING_DIM_ARRAY_LENGTH_OFFSET);
        int[] array = new int[arraySize];
        UnsafeUtils.copyToArray(address + arrayPos, array, arraySize);
        return array;
      default:
        return null;
    }
  }

  static Object[] getAllDims(long address)
  {
    int dimsLength = getDimsLength(address);
    return IntStream.range(0, dimsLength).mapToObj(dimIndex -> getDim(address, dimIndex)).toArray();
  }

  /**
   * A lazy-evaluation version of an indexed ints dimension, used by 
   * {@link OakIncrementalIndexRow#getIndexedDim(int, IndexedInts)}.
   * As opposed to the integers array version that is returned by
   * {@link IncrementalIndexRow#getIndexedDim(int, IndexedInts)}.
   */
  public static class IndexedDim implements IndexedInts
  {
    long dimensionsAddress;
    int dimIndex;
    int arraySize;
    long arrayAddress;

    public IndexedDim(long dimensionsAddress, int dimIndex)
    {
      setValues(dimensionsAddress, dimIndex);
    }

    public void setValues(long dimensionsAddress, int dimIndex)
    {
      if (this.dimensionsAddress != dimensionsAddress || this.dimIndex != dimIndex) {
        this.dimensionsAddress = dimensionsAddress;
        this.dimIndex = dimIndex;
        long dimAddress = this.dimensionsAddress + getDimOffsetInBuffer(dimIndex);
        arrayAddress = dimensionsAddress + UnsafeUtils.getInt(dimAddress + STRING_DIM_ARRAY_POS_OFFSET);
        arraySize = UnsafeUtils.getInt(dimAddress + STRING_DIM_ARRAY_LENGTH_OFFSET);
      }
    }

    @Override
    public int size()
    {
      return arraySize;
    }

    @Override
    public int get(int index)
    {
      return UnsafeUtils.getInt(arrayAddress + ((long) index) * Integer.BYTES);
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      // nothing to inspect
    }
  }

  public static class Serializer implements OakSerializer<IncrementalIndexRow>
  {
    public static final int ASSIGN_ROW_INDEX_IF_ABSENT = Integer.MIN_VALUE;

    private final List<IncrementalIndex.DimensionDesc> dimensionDescsList;
    private final AtomicInteger rowIndexGenerator;

    public Serializer(List<IncrementalIndex.DimensionDesc> dimensionDescsList, AtomicInteger rowIndexGenerator)
    {
      this.dimensionDescsList = dimensionDescsList;
      this.rowIndexGenerator = rowIndexGenerator;
    }

    @Nullable
    private ValueType getDimValueType(int dimIndex)
    {
      IncrementalIndex.DimensionDesc dimensionDesc = dimensionDescsList.get(dimIndex);
      if (dimensionDesc == null) {
        return null;
      }
      ColumnCapabilities capabilities = dimensionDesc.getCapabilities();
      return capabilities.getType();
    }

    @Override
    public void serialize(IncrementalIndexRow incrementalIndexRow, OakScopedWriteBuffer buffer)
    {
      long address = getKeyAddress(buffer);

      long timestamp = incrementalIndexRow.getTimestamp();
      int dimsLength = incrementalIndexRow.getDimsLength();
      int rowIndex = incrementalIndexRow.getRowIndex();
      if (rowIndex == ASSIGN_ROW_INDEX_IF_ABSENT) {
        rowIndex = rowIndexGenerator.getAndIncrement();
        incrementalIndexRow.setRowIndex(rowIndex);
      }
      UnsafeUtils.putLong(address + TIME_STAMP_OFFSET, timestamp);
      UnsafeUtils.putInt(address + DIMS_LENGTH_OFFSET, dimsLength);
      UnsafeUtils.putInt(address + ROW_INDEX_OFFSET, rowIndex);

      long dimsAddress = address + DIMS_OFFSET;
      // the index for writing the int arrays of the string-dim (after all the dims' data)
      int stringDimArraysPos = getDimOffsetInBuffer(dimsLength);

      for (int dimIndex = 0; dimIndex < dimsLength; dimIndex++) {
        Object dim = incrementalIndexRow.getDim(dimIndex);
        ValueType dimValueType = dim != null ? getDimValueType(dimIndex) : null;
        boolean isDimHaveValue = dimValueType != null;

        int dimValueTypeID = isDimHaveValue ? dimValueType.ordinal() : NULL_DIM;
        UnsafeUtils.putInt(dimsAddress + DIM_VALUE_TYPE_OFFSET, dimValueTypeID);

        if (isDimHaveValue) {
          switch (dimValueType) {
            case FLOAT:
              UnsafeUtils.putFloat(dimsAddress + DIM_DATA_OFFSET, (Float) dim);
              break;
            case DOUBLE:
              UnsafeUtils.putDouble(dimsAddress + DIM_DATA_OFFSET, (Double) dim);
              break;
            case LONG:
              UnsafeUtils.putLong(dimsAddress + DIM_DATA_OFFSET, (Long) dim);
              break;
            case STRING:
              int[] arr = (int[]) dim;
              int length = arr.length;
              UnsafeUtils.putInt(dimsAddress + STRING_DIM_ARRAY_POS_OFFSET, stringDimArraysPos);
              UnsafeUtils.putInt(dimsAddress + STRING_DIM_ARRAY_LENGTH_OFFSET, length);
              long lengthBytes = UnsafeUtils.copyFromArray(arr, address + stringDimArraysPos, length);
              stringDimArraysPos += (int) lengthBytes;
              break;
          }
        }

        dimsAddress += SIZE_PER_DIM;
      }
    }

    @Override
    public IncrementalIndexRow deserialize(OakScopedReadBuffer buffer)
    {
      long address = getKeyAddress(buffer);
      return new IncrementalIndexRow(
              getTimestamp(address),
              getAllDims(address),
              dimensionDescsList,
              getRowIndex(address)
      );
    }

    @Override
    public int calculateSize(IncrementalIndexRow incrementalIndexRow)
    {
      int dimsLength = incrementalIndexRow.getDimsLength();
      int sizeInBytes = getDimOffsetInBuffer(dimsLength);

      // When the dimensionDesc's capabilities are of type ValueType.STRING,
      // the object in timeAndDims.dims is of type int[].
      // In this case, we need to know the array size before allocating the ByteBuffer.
      for (int i = 0; i < dimsLength; i++) {
        if (getDimValueType(i) != ValueType.STRING) {
          continue;
        }

        Object dim = incrementalIndexRow.getDim(i);
        if (dim != null) {
          sizeInBytes += Integer.BYTES * ((int[]) dim).length;
        }
      }

      return sizeInBytes;
    }
  }

  public static class Comparator implements OakComparator<IncrementalIndexRow>
  {
    private final List<IncrementalIndex.DimensionDesc> dimensionDescs;
    private final boolean rollup;

    public Comparator(List<IncrementalIndex.DimensionDesc> dimensionDescs, boolean rollup)
    {
      this.dimensionDescs = dimensionDescs;
      this.rollup = rollup;
    }

    @Override
    public int compareKeys(IncrementalIndexRow lhs, IncrementalIndexRow rhs)
    {
      int retVal = Longs.compare(lhs.getTimestamp(), rhs.getTimestamp());
      if (retVal != 0) {
        return retVal;
      }

      int lhsDimsLength = lhs.getDimsLength();
      int rhsDimsLength = rhs.getDimsLength();
      int numComparisons = Math.min(lhsDimsLength, rhsDimsLength);

      int index = 0;
      while (retVal == 0 && index < numComparisons) {
        final Object lhsIdxs = lhs.getDim(index);
        final Object rhsIdxs = rhs.getDim(index);

        if (lhsIdxs == null) {
          if (rhsIdxs == null) {
            ++index;
            continue;
          }
          return -1;
        }

        if (rhsIdxs == null) {
          return 1;
        }

        final DimensionIndexer indexer = dimensionDescs.get(index).getIndexer();
        retVal = indexer.compareUnsortedEncodedKeyComponents(lhsIdxs, rhsIdxs);
        ++index;
      }

      if (retVal == 0) {
        int lengthDiff = Ints.compare(lhsDimsLength, rhsDimsLength);
        if (lengthDiff != 0) {
          IncrementalIndexRow largerRow = lengthDiff > 0 ? lhs : rhs;
          retVal = allNull(largerRow, numComparisons) ? 0 : lengthDiff;
        }
      }

      return retVal == 0 ? rowIndexCompare(lhs.getRowIndex(), rhs.getRowIndex()) : retVal;
    }

    @Override
    public int compareSerializedKeys(OakScopedReadBuffer lhsBuffer, OakScopedReadBuffer rhsBuffer)
    {
      long lhs = getKeyAddress(lhsBuffer);
      long rhs = getKeyAddress(rhsBuffer);

      int retVal = Longs.compare(getTimestamp(lhs), getTimestamp(rhs));
      if (retVal != 0) {
        return retVal;
      }

      int lhsDimsLength = getDimsLength(lhs);
      int rhsDimsLength = getDimsLength(rhs);
      int numComparisons = Math.min(lhsDimsLength, rhsDimsLength);

      int index = 0;
      while (retVal == 0 && index < numComparisons) {
        final Object lhsIdxs = getDim(lhs, index);
        final Object rhsIdxs = getDim(rhs, index);

        if (lhsIdxs == null) {
          if (rhsIdxs == null) {
            ++index;
            continue;
          }
          return -1;
        }

        if (rhsIdxs == null) {
          return 1;
        }

        final DimensionIndexer indexer = dimensionDescs.get(index).getIndexer();
        retVal = indexer.compareUnsortedEncodedKeyComponents(lhsIdxs, rhsIdxs);
        ++index;
      }

      if (retVal == 0) {
        int lengthDiff = Ints.compare(lhsDimsLength, rhsDimsLength);
        if (lengthDiff != 0) {
          long largerRowAddress = lengthDiff > 0 ? lhs : rhs;
          retVal = allNull(largerRowAddress, numComparisons) ? 0 : lengthDiff;
        }
      }

      return retVal == 0 ? rowIndexCompare(getRowIndex(lhs), getRowIndex(rhs)) : retVal;
    }

    @Override
    public int compareKeyAndSerializedKey(IncrementalIndexRow lhs, OakScopedReadBuffer rhsBuffer)
    {
      long rhs = getKeyAddress(rhsBuffer);

      int retVal = Longs.compare(lhs.getTimestamp(), getTimestamp(rhs));
      if (retVal != 0) {
        return retVal;
      }

      int lhsDimsLength = lhs.getDimsLength();
      int rhsDimsLength = getDimsLength(rhs);
      int numComparisons = Math.min(lhsDimsLength, rhsDimsLength);

      int index = 0;
      while (retVal == 0 && index < numComparisons) {
        final Object lhsIdxs = lhs.getDim(index);
        final Object rhsIdxs = getDim(rhs, index);

        if (lhsIdxs == null) {
          if (rhsIdxs == null) {
            ++index;
            continue;
          }
          return -1;
        }

        if (rhsIdxs == null) {
          return 1;
        }

        final DimensionIndexer indexer = dimensionDescs.get(index).getIndexer();
        retVal = indexer.compareUnsortedEncodedKeyComponents(lhsIdxs, rhsIdxs);
        ++index;
      }

      if (retVal == 0) {
        int lengthDiff = Ints.compare(lhsDimsLength, rhsDimsLength);
        if (lengthDiff != 0) {
          boolean isAllNull = lengthDiff > 0 ? allNull(lhs, numComparisons) : allNull(rhs, numComparisons);
          retVal = isAllNull ? 0 : lengthDiff;
        }
      }

      return retVal == 0 ? rowIndexCompare(lhs.getRowIndex(), getRowIndex(rhs)) : retVal;
    }

    private int rowIndexCompare(int lsIndex, int rsIndex)
    {
      if (!rollup || lsIndex == IncrementalIndexRow.EMPTY_ROW_INDEX || rsIndex == IncrementalIndexRow.EMPTY_ROW_INDEX) {
        // If we are not in a rollup mode (plain mode), then keys should never be equal.
        // In addition, if one of the keys has no index row (EMPTY_ROW_INDEX) it means it is a lower or upper bound key,
        // so we must compared it.
        return Integer.compare(lsIndex, rsIndex);
      } else {
        return 0;
      }
    }

    private static boolean allNull(IncrementalIndexRow row, int startPosition)
    {
      int dimLength = row.getDimsLength();
      for (int i = startPosition; i < dimLength; i++) {
        if (!row.isDimNull(i)) {
          return false;
        }
      }
      return true;
    }

    private static boolean allNull(long rowAddress, int startPosition)
    {
      int dimLength = getDimsLength(rowAddress);
      for (int i = startPosition; i < dimLength; i++) {
        if (!isDimNull(rowAddress, i)) {
          return false;
        }
      }
      return true;
    }
  }
}
