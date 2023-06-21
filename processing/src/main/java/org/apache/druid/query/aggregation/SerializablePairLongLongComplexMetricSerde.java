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

import org.apache.druid.collections.SerializablePair;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.segment.data.ObjectStrategy;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Comparator;

public class SerializablePairLongLongComplexMetricSerde extends AbstractSerializableLongObjectPairSerde<SerializablePairLongLong>
{
  public static final String TYPE_NAME = "serializablePairLongLong";

  private static final Comparator<SerializablePair<Long, Long>> COMPARATOR = SerializablePair.createNullHandlingComparator(
      Long::compare,
      true
  );

  public SerializablePairLongLongComplexMetricSerde()
  {
    super(SerializablePairLongLong.class);
  }

  @Override
  public String getTypeName()
  {
    return TYPE_NAME;
  }

  @Override
  public ObjectStrategy<SerializablePairLongLong> getObjectStrategy()
  {
    return new ObjectStrategy<SerializablePairLongLong>()
    {
      @Override
      public int compare(SerializablePairLongLong o1, SerializablePairLongLong o2)
      {
        return COMPARATOR.compare(o1, o2);
      }

      @Override
      public Class<? extends SerializablePairLongLong> getClazz()
      {
        return SerializablePairLongLong.class;
      }

      @Override
      public SerializablePairLongLong fromByteBuffer(ByteBuffer buffer, int numBytes)
      {
        final ByteBuffer readOnlyBuffer = buffer.asReadOnlyBuffer();
        long lhs = readOnlyBuffer.getLong();
        boolean isNotNull = readOnlyBuffer.get() == NullHandling.IS_NOT_NULL_BYTE;
        if (isNotNull) {
          return new SerializablePairLongLong(lhs, readOnlyBuffer.getLong());
        } else {
          return new SerializablePairLongLong(lhs, null);
        }
      }

      @Override
      public byte[] toBytes(@Nullable SerializablePairLongLong inPair)
      {
        if (inPair == null) {
          return new byte[]{};
        }

        ByteBuffer bbuf = ByteBuffer.allocate(Long.BYTES + Byte.BYTES + Long.BYTES);
        bbuf.putLong(inPair.lhs);
        if (inPair.rhs == null) {
          bbuf.put(NullHandling.IS_NULL_BYTE);
        } else {
          bbuf.put(NullHandling.IS_NOT_NULL_BYTE);
          bbuf.putLong(inPair.rhs);
        }
        return bbuf.array();
      }
    };
  }
}
