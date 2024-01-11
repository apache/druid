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

package org.apache.druid.query.aggregation.first;

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.SerializablePairLongString;
import org.apache.druid.segment.BaseLongColumnValueSelector;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class StringFirstLastUtils
{
  private static final int NULL_VALUE = -1;

  /**
   * Return the object at a particular index from the vector selectors.
   * index of bounds issues is the responsibility of the caller
   */
  public static SerializablePairLongString readPairFromVectorSelectorsAtIndex(
      VectorValueSelector timeSelector,
      VectorObjectSelector valueSelector,
      int index
  )
  {
    final long time;
    final String string;
    final Object object = valueSelector.getObjectVector()[index];
    if (object instanceof SerializablePairLongString) {
      final SerializablePairLongString pair = (SerializablePairLongString) object;
      time = pair.lhs;
      string = pair.rhs;
    } else if (object != null) {
      time = timeSelector.getLongVector()[index];
      string = DimensionHandlerUtils.convertObjectToString(object);
    } else {
      // Don't aggregate nulls.
      return null;
    }

    return new SerializablePairLongString(time, string);
  }

  @Nullable
  public static SerializablePairLongString readPairFromSelectors(
      final BaseLongColumnValueSelector timeSelector,
      final BaseObjectColumnValueSelector<?> valueSelector
  )
  {
    final long time;
    final String string;

    // Need to read this first (before time), just in case it's a SerializablePairLongString (we don't know; it's
    // detected at query time).
    final Object object = valueSelector.getObject();

    if (object instanceof SerializablePairLongString) {
      final SerializablePairLongString pair = (SerializablePairLongString) object;
      time = pair.lhs;
      string = pair.rhs;
    } else if (object != null) {
      if (timeSelector.isNull()) {
        return null;
      }
      time = timeSelector.getLong();
      string = DimensionHandlerUtils.convertObjectToString(object);
    } else {
      // Don't aggregate nulls.
      return null;
    }

    return new SerializablePairLongString(time, string);
  }

  public static void writePair(
      final ByteBuffer buf,
      final int position,
      final SerializablePairLongString pair,
      final int maxStringBytes
  )
  {
    ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.position(position);
    mutationBuffer.putLong(pair.lhs);

    if (pair.rhs != null) {
      mutationBuffer.position(position + Long.BYTES + Integer.BYTES);
      mutationBuffer.limit(position + Long.BYTES + Integer.BYTES + maxStringBytes);
      final int len = StringUtils.toUtf8WithLimit(pair.rhs, mutationBuffer);
      mutationBuffer.putInt(position + Long.BYTES, len);
    } else {
      mutationBuffer.putInt(NULL_VALUE);
    }
  }

  public static SerializablePairLongString readPair(final ByteBuffer buf, final int position)
  {
    ByteBuffer copyBuffer = buf.duplicate();
    copyBuffer.position(position);

    Long timeValue = copyBuffer.getLong();
    int stringSizeBytes = copyBuffer.getInt();


    if (stringSizeBytes >= 0) {
      byte[] valueBytes = new byte[stringSizeBytes];
      copyBuffer.get(valueBytes, 0, stringSizeBytes);
      return new SerializablePairLongString(timeValue, StringUtils.fromUtf8(valueBytes));
    } else {
      return new SerializablePairLongString(timeValue, null);
    }
  }
}
