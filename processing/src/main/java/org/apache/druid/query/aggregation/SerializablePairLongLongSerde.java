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

import java.nio.ByteBuffer;
import java.util.Comparator;

/**
 * The class serializes a Long-Long pair (SerializablePair<Long, Long>).
 * The serialization structure is: Long:Long
 * <p>
 * The class is used on first/last Long aggregators to store the time and the first/last Long.
 * Long:Long -> Timestamp:Long
 */
public class SerializablePairLongLongSerde extends AbstractSerializableLongObjectPairSerde<SerializablePairLongLong>
{
  public static final String TYPE_NAME = "serializablePairLongLong";

  /**
   * Since SerializablePairLongFloat is subclass of SerializablePair<Long,Float>,
   * it's safe to declare the generic type of comparator as SerializablePair<Long,Float>.
   */
  public static Comparator<SerializablePair<Long, Long>> VALUE_COMPARATOR = SerializablePair.createNullHandlingComparator(
      Long::compare,
      true
  );

  public SerializablePairLongLongSerde()
  {
    super(SerializablePairLongLong.class);
  }

  @Override
  public String getTypeName()
  {
    return TYPE_NAME;
  }

  @Override
  protected Comparator getLongObjectPairComparator()
  {
    return VALUE_COMPARATOR;
  }

  @Override
  protected SerializablePairLongLong toLongObjectPair(ByteBuffer buffer)
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
  protected byte[] longObjectPairToBytes(SerializablePairLongLong longObjectPair)
  {
    ByteBuffer bbuf = ByteBuffer.allocate(Long.BYTES + Byte.BYTES + Long.BYTES);
    bbuf.putLong(longObjectPair.lhs);
    if (longObjectPair.rhs == null) {
      bbuf.put(NullHandling.IS_NULL_BYTE);
    } else {
      bbuf.put(NullHandling.IS_NOT_NULL_BYTE);
      bbuf.putLong(longObjectPair.rhs);
    }
    return bbuf.array();
  }
}
