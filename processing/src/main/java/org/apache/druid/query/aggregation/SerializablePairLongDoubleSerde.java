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

import org.apache.druid.common.config.NullHandling;

import java.nio.ByteBuffer;

/**
 * The class serializes a Long-Double pair (SerializablePair<Long, Double>).
 * The serialization structure is: Long:Double
 * <p>
 * The class is used on first/last Double aggregators to store the time and the first/last Double.
 * Long:Double -> Timestamp:Double
 */
public class SerializablePairLongDoubleSerde extends AbstractSerializablePairSerde<SerializablePairLongDouble>
{
  public static final String TYPE_NAME = "serializablePairLongDouble";

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
  protected SerializablePairLongDouble toPairObject(ByteBuffer buffer)
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
  protected byte[] pairToBytes(SerializablePairLongDouble val)
  {
    ByteBuffer bbuf = ByteBuffer.allocate(Long.BYTES + Byte.BYTES + Double.BYTES);
    bbuf.putLong(val.lhs);
    if (val.rhs == null) {
      bbuf.put(NullHandling.IS_NULL_BYTE);
    } else {
      bbuf.put(NullHandling.IS_NOT_NULL_BYTE);
      bbuf.putDouble(val.rhs);
    }
    return bbuf.array();
  }
}
