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

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class SerializablePairLongFloatDeltaEncodedStagedSerde extends AbstractSerializablePairLongObjectDeltaEncodedStagedSerde<SerializablePairLongFloat>
{

  public SerializablePairLongFloatDeltaEncodedStagedSerde(long minValue, boolean useIntegerDelta)
  {
    super(minValue, useIntegerDelta, SerializablePairLongFloat.class);
  }

  @Nullable
  @Override
  public SerializablePairLongFloat deserialize(ByteBuffer byteBuffer)
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

    Float rhs = null;
    if (readOnlyBuffer.get() == NullHandling.IS_NOT_NULL_BYTE) {
      rhs = readOnlyBuffer.getFloat();
    }

    return new SerializablePairLongFloat(lhs, rhs);
  }
}
