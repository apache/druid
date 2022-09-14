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
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.serde.cell.StagedSerde;
import org.apache.druid.segment.serde.cell.StorableBuffer;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * serializes a Long/String pair as
 * Long:Integer:bytes
 * <p>
 * or
 * Long:StringSize:StringData
 */
public class SerializablePairLongStringSimpleStagedSerde implements StagedSerde<SerializablePairLongString>
{
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

        byteBuffer.putLong(value.lhs);
        byteBuffer.putInt(rhsBytes.length);

        if (rhsBytes.length > 0) {
          byteBuffer.put(rhsBytes);
        }
      }

      @Override
      public int getSerializedSize()
      {
        return Long.BYTES + Integer.BYTES + rhsBytes.length;
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
    long lhs = readOnlyBuffer.getLong();
    int stringSize = readOnlyBuffer.getInt();
    String lastString = null;

    if (stringSize > 0) {
      byte[] stringBytes = new byte[stringSize];

      readOnlyBuffer.get(stringBytes, 0, stringSize);
      lastString = StringUtils.fromUtf8(stringBytes);
    }

    return new SerializablePairLongString(lhs, lastString);
  }
}
