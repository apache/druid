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

package org.apache.druid.query.aggregation;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import org.apache.druid.collections.SerializablePair;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.segment.serde.cell.StagedSerde;
import org.apache.druid.segment.serde.cell.StorableBuffer;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Locale;

/**
 * serializes a Long/Object(Number) pair in the context of a column/segment. Uses the minValue to perform delta
 * encoding/decoding and if the range of the segment fits in an integer (useIntegerDelta), the format is
 * Integer:Byte:Integer
 *
 * otherwise
 * Long:Integer:bytes
 */
public abstract class AbstractSerializablePairLongObjectDeltaEncodedStagedSerde<T extends SerializablePair<Long, ?>> implements
    StagedSerde<T>
{

  final long minValue;
  final boolean useIntegerDelta;
  private final Class<?> pairClass;

  AbstractSerializablePairLongObjectDeltaEncodedStagedSerde(
      long minValue,
      boolean useIntegerDelta,
      Class<?> pairClass
  )
  {
    this.minValue = minValue;
    this.useIntegerDelta = useIntegerDelta;
    this.pairClass = pairClass;
  }

  @Override
  public StorableBuffer serializeDelayed(@Nullable T value)
  {
    if (value == null) {
      return StorableBuffer.EMPTY;
    }

    Object rhsObject = value.getRhs();

    return new StorableBuffer()
    {
      @Override
      public void store(ByteBuffer byteBuffer)
      {
        Preconditions.checkNotNull(value.lhs, String.format(Locale.ENGLISH, "Long in %s must be non-null", pairClass.getSimpleName()));

        long delta = value.lhs - minValue;

        Preconditions.checkState(delta >= 0 || delta == value.lhs);

        if (useIntegerDelta) {
          byteBuffer.putInt(Ints.checkedCast(delta));
        } else {
          byteBuffer.putLong(delta);
        }

        if (rhsObject != null) {
          byteBuffer.put(NullHandling.IS_NOT_NULL_BYTE);
          if (pairClass.isAssignableFrom(SerializablePairLongLong.class)) {
            byteBuffer.putLong((long) rhsObject);
          } else if (pairClass.isAssignableFrom(SerializablePairLongDouble.class)) {
            byteBuffer.putDouble((double) rhsObject);
          } else if (pairClass.isAssignableFrom(SerializablePairLongFloat.class)) {
            byteBuffer.putFloat((float) rhsObject);
          }
        } else {
          byteBuffer.put(NullHandling.IS_NULL_BYTE);
        }
      }

      @Override
      public int getSerializedSize()
      {
        int rhsBytes = 0;

        if (rhsObject != null) {
          if (pairClass.isAssignableFrom(SerializablePairLongLong.class)) {
            rhsBytes = Long.BYTES;
          } else if (pairClass.isAssignableFrom(SerializablePairLongDouble.class)) {
            rhsBytes = Double.BYTES;
          } else if (pairClass.isAssignableFrom(SerializablePairLongFloat.class)) {
            rhsBytes = Float.BYTES;
          }
        }

        return (useIntegerDelta ? Integer.BYTES : Long.BYTES) + Byte.BYTES + rhsBytes;
      }
    };
  }
}
