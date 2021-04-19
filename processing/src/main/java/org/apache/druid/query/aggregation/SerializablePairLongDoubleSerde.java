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

/**
 * The class serializes a Long-Double pair (SerializablePair<Long, Double>).
 * The serialization structure is: Long:Double
 * <p>
 * The class is used on first/last Double aggregators to store the time and the first/last Double.
 * (Long:timestamp, Double:value)
 */
public class SerializablePairLongDoubleSerde extends AbstractSerializableLongObjectPairSerde<SerializablePairLongDouble>
{
  public static final String TYPE_NAME = "serializablePairLongDouble";

  private static class ObjectStrategyImpl implements ObjectStrategy<SerializablePairLongDouble>
  {
    /**
     * Since SerializablePairLongDouble is subclass of SerializablePair<Long,Double>,
     * it's safe to declare the generic type of comparator as SerializablePair<Long,Double>.
     */
    private final Comparator<SerializablePair<Long, Double>> pairComparator = SerializablePair.createNullHandlingComparator(
        Double::compare,
        true
    );

    @Override
    public int compare(@Nullable SerializablePairLongDouble o1, @Nullable SerializablePairLongDouble o2)
    {
      return pairComparator.compare(o1, o2);
    }

    @Override
    public Class<SerializablePairLongDouble> getClazz()
    {
      return SerializablePairLongDouble.class;
    }

    @Override
    public SerializablePairLongDouble fromByteBuffer(ByteBuffer buffer, int numBytes)
    {
      final ByteBuffer readOnlyBuffer = buffer.asReadOnlyBuffer();
      long lhs = readOnlyBuffer.getLong();
      boolean isNotNull = readOnlyBuffer.get() == NullHandling.IS_NOT_NULL_BYTE;
      if (isNotNull) {
        return new SerializablePairLongDouble(lhs, readOnlyBuffer.getDouble());
      } else {
        return new SerializablePairLongDouble(lhs, null);
      }
    }

    @Override
    public byte[] toBytes(@Nullable SerializablePairLongDouble longObjectPair)
    {
      if (longObjectPair == null) {
        return new byte[]{};
      }

      ByteBuffer bbuf = ByteBuffer.allocate(Long.BYTES + Byte.BYTES + Double.BYTES);
      bbuf.putLong(longObjectPair.lhs);
      if (longObjectPair.rhs == null) {
        bbuf.put(NullHandling.IS_NULL_BYTE);
      } else {
        bbuf.put(NullHandling.IS_NOT_NULL_BYTE);
        bbuf.putDouble(longObjectPair.rhs);
      }
      return bbuf.array();
    }
  }

  private static final ObjectStrategy<SerializablePairLongDouble> OBJECT_STRATEGY = new ObjectStrategyImpl();

  public SerializablePairLongDoubleSerde()
  {
    super(SerializablePairLongDouble.class);
  }

  @Override
  public String getTypeName()
  {
    return TYPE_NAME;
  }

  @Override
  public ObjectStrategy getObjectStrategy()
  {
    return OBJECT_STRATEGY;
  }
}
