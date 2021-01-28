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

import java.nio.ByteBuffer;

/**
 * The class serializes a Long-Float pair (SerializablePair<Long, Float>).
 * The serialization structure is: Long:Float
 * <p>
 * The class is used on first/last Float aggregators to store the time and the first/last Float.
 * Long:Long -> Timestamp:Long
 */
public class SerializablePairLongFloatSerde extends AbstractSerializablePairSerde<SerializablePairLongFloat>
{
  public static final String TYPE_NAME = "serializablePairLongFloat";

  public SerializablePairLongFloatSerde()
  {
    super(SerializablePairLongFloat.class);
  }

  @Override
  public String getTypeName()
  {
    return TYPE_NAME;
  }

  @Override
  protected SerializablePairLongFloat toPairObject(ByteBuffer buffer, int numBytes)
  {
    final ByteBuffer readOnlyBuffer = buffer.asReadOnlyBuffer();
    long lhs = readOnlyBuffer.getLong();
    float rhs = readOnlyBuffer.getFloat();
    return new SerializablePairLongFloat(lhs, rhs);
  }

  @Override
  protected byte[] pairToBytes(SerializablePairLongFloat val)
  {
    ByteBuffer bbuf = ByteBuffer.allocate(Long.BYTES + Float.BYTES);
    bbuf.putLong(val.lhs);
    bbuf.putFloat(val.rhs);
    return bbuf.array();
  }

}
