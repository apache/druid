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
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.serde.cell.StorableBuffer;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * serializes a Long/String pair in the context of a column/segment. Uses the minValue to perform delta
 * encoding/decoding and if the range of the segment fits in an integer (useIntegerDelta), the format is
 * Integer:Integer:bytes
 *
 * otherwise
 * Long:Integer:bytes
 *
 * The StringSize can be following:
 * -1 : Denotes an empty string
 * 0  : Denotes a null string
 * >0 : Denotes a non-empty string
 *
 * Mapping of null and empty string is done weirdly to preserve backward compatibility when nulls were returned all the
 * time, and there was no distinction between empty and null string
 */
public class SerializablePairLongStringDeltaEncodedStagedSerde extends AbstractSerializablePairLongObjectDeltaEncodedStagedSerde<SerializablePairLongString>
{
  public SerializablePairLongStringDeltaEncodedStagedSerde(long minValue, boolean useIntegerDelta)
  {
    super(minValue, useIntegerDelta, SerializablePairLongString.class);
  }

  @Override
  public StorableBuffer serializeDelayed(@Nullable SerializablePairLongString value)
  {
    if (value == null) {
      return StorableBuffer.EMPTY;
    }

    String rhsString = value.rhs;
    byte[] rhsBytes = StringUtils.toUtf8WithNullToEmpty(rhsString);

    return new StorableBuffer()
    {
      @Override
      public void store(ByteBuffer byteBuffer)
      {
        Preconditions.checkNotNull(value.lhs, "Long in SerializablePairLongString must be non-null");

        long delta = value.lhs - minValue;

        Preconditions.checkState(delta >= 0 || delta == value.lhs);

        if (useIntegerDelta) {
          byteBuffer.putInt(Ints.checkedCast(delta));
        } else {
          byteBuffer.putLong(delta);
        }

        if (rhsString == null) {
          byteBuffer.putInt(0);
        } else if (rhsBytes.length == 0) {
          byteBuffer.putInt(-1);
        } else {
          byteBuffer.putInt(rhsBytes.length);
        }

        if (rhsBytes.length > 0) {
          byteBuffer.put(rhsBytes);
        }
      }

      @Override
      public int getSerializedSize()
      {
        return (useIntegerDelta ? Integer.BYTES : Long.BYTES) + Integer.BYTES + rhsBytes.length;
      }
    };
  }

  @Nullable
  @Override
  public SerializablePairLongString deserialize(ByteBuffer byteBuffer)
  {
    if (byteBuffer.remaining() == 0) {
      return null;
    }

    ByteBuffer readOnlyBuffer = byteBuffer.asReadOnlyBuffer().order(ByteOrder.nativeOrder());
    long lhs;

    if (useIntegerDelta) {
      lhs = readOnlyBuffer.getInt();
    } else {
      lhs = readOnlyBuffer.getLong();
    }

    lhs += minValue;

    int stringSize = readOnlyBuffer.getInt();
    String lastString = null;

    if (stringSize > 0) {
      byte[] stringBytes = new byte[stringSize];

      readOnlyBuffer.get(stringBytes, 0, stringSize);
      lastString = StringUtils.fromUtf8(stringBytes);
    } else if (stringSize < 0) {
      lastString = "";
    }

    return new SerializablePairLongString(lhs, lastString);
  }
}
