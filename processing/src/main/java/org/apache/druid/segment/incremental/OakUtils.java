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

import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ValueType;
import java.nio.ByteBuffer;
import java.util.List;

public final class OakUtils
{
  static final Integer ALLOC_PER_DIM = 12;
  static final Integer NO_DIM = -1;
  static final Integer TIME_STAMP_INDEX = 0;
  static final Integer DIMS_LENGTH_INDEX = TIME_STAMP_INDEX + Long.BYTES;
  static final Integer ROW_INDEX_INDEX = DIMS_LENGTH_INDEX + Integer.BYTES;
  static final Integer DIMS_INDEX = ROW_INDEX_INDEX + Integer.BYTES;
  // Serialization and deserialization offsets
  static final Integer VALUE_TYPE_OFFSET = 0;
  static final Integer DATA_OFFSET = VALUE_TYPE_OFFSET + Integer.BYTES;
  static final Integer ARRAY_INDEX_OFFSET = VALUE_TYPE_OFFSET + Integer.BYTES;
  static final Integer ARRAY_LENGTH_OFFSET = ARRAY_INDEX_OFFSET + Integer.BYTES;

  private OakUtils()
  {
  }

  static boolean checkDimsAllNull(ByteBuffer buff, int numComparisons)
  {
    int dimsLength = getDimsLength(buff);
    for (int index = 0; index < Math.min(dimsLength, numComparisons); index++) {
      if (buff.getInt(getDimIndexInBuffer(buff, dimsLength, index)) != NO_DIM) {
        return false;
      }
    }
    return true;
  }

  static long getTimestamp(ByteBuffer buff)
  {
    return buff.getLong(buff.position() + TIME_STAMP_INDEX);
  }

  static int getRowIndex(ByteBuffer buff)
  {
    return buff.getInt(buff.position() + ROW_INDEX_INDEX);
  }

  static ValueType getDimValueType(int dimIndex, List<IncrementalIndex.DimensionDesc> dimensionDescsList)
  {
    IncrementalIndex.DimensionDesc dimensionDesc = dimensionDescsList.get(dimIndex);
    if (dimensionDesc == null) {
      return null;
    }
    ColumnCapabilitiesImpl capabilities = dimensionDesc.getCapabilities();
    if (capabilities == null) {
      return null;
    }
    return capabilities.getType();
  }

  static Object getDimValue(ByteBuffer buff, int dimIndex)
  {
    int dimsLength = getDimsLength(buff);
    return getDimValue(buff, dimIndex, dimsLength);
  }

  static int getDimsLength(ByteBuffer buff)
  {
    return buff.getInt(buff.position() + DIMS_LENGTH_INDEX);
  }

  static int getDimIndexInBuffer(ByteBuffer buff, int dimsLength, int dimIndex)
  {
    if (dimIndex >= dimsLength) {
      return NO_DIM;
    }
    return buff.position() + DIMS_INDEX + dimIndex * ALLOC_PER_DIM;
  }

  static Object getDimValue(ByteBuffer buff, int dimIndex, int dimsLength)
  {
    Object dimObject = null;
    if (dimIndex >= dimsLength) {
      return null;
    }
    int dimIndexInBuffer = getDimIndexInBuffer(buff, dimsLength, dimIndex);
    int dimType = buff.getInt(dimIndexInBuffer);
    if (dimType == NO_DIM) {
      return null;
    } else if (dimType == ValueType.DOUBLE.ordinal()) {
      dimObject = buff.getDouble(dimIndexInBuffer + DATA_OFFSET);
    } else if (dimType == ValueType.FLOAT.ordinal()) {
      dimObject = buff.getFloat(dimIndexInBuffer + DATA_OFFSET);
    } else if (dimType == ValueType.LONG.ordinal()) {
      dimObject = buff.getLong(dimIndexInBuffer + DATA_OFFSET);
    } else if (dimType == ValueType.STRING.ordinal()) {
      int arrayIndexOffset = buff.getInt(dimIndexInBuffer + ARRAY_INDEX_OFFSET);
      int arrayIndex = buff.position() + arrayIndexOffset;
      int arraySize = buff.getInt(dimIndexInBuffer + ARRAY_LENGTH_OFFSET);
      int[] array = new int[arraySize];
      for (int i = 0; i < arraySize; i++) {
        array[i] = buff.getInt(arrayIndex);
        arrayIndex += Integer.BYTES;
      }
      dimObject = array;
    }
    return dimObject;
  }
}
