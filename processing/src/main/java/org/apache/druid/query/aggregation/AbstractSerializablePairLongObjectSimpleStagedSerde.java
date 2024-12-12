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
import org.apache.druid.collections.SerializablePair;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.segment.serde.cell.StagedSerde;
import org.apache.druid.segment.serde.cell.StorableBuffer;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

/**
 * serializes a Long/Object pair as
 * Long:Byte:Object
 * <p>
 * or
 * Long:isNullByte:ObjectBytes
 */
public abstract class AbstractSerializablePairLongObjectSimpleStagedSerde<T extends SerializablePair<Long, ?>> implements StagedSerde<T>
{

  private final Class<?> pairCLass;

  AbstractSerializablePairLongObjectSimpleStagedSerde(Class<?> pairCLass)
  {
    this.pairCLass = pairCLass;
  }

  @Override
  public StorableBuffer serializeDelayed(
      @Nullable T value
  )
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
        Preconditions.checkNotNull(value.getLhs(), "Long in %s must be non-null", pairCLass.getSimpleName());
        byteBuffer.putLong(value.getLhs());
        if (rhsObject != null) {
          byteBuffer.put(NullHandling.IS_NOT_NULL_BYTE);
          if (pairCLass.isAssignableFrom(SerializablePairLongLong.class)) {
            byteBuffer.putLong((long) rhsObject);
          } else if (pairCLass.isAssignableFrom(SerializablePairLongDouble.class)) {
            byteBuffer.putDouble((double) rhsObject);
          } else if (pairCLass.isAssignableFrom(SerializablePairLongFloat.class)) {
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
          if (pairCLass.isAssignableFrom(SerializablePairLongLong.class)) {
            rhsBytes = Long.BYTES;
          } else if (pairCLass.isAssignableFrom(SerializablePairLongDouble.class)) {
            rhsBytes = Double.BYTES;
          } else if (pairCLass.isAssignableFrom(SerializablePairLongFloat.class)) {
            rhsBytes = Float.BYTES;
          }
        }
        return Long.BYTES + Byte.BYTES + rhsBytes;
      }
    };
  }
}
