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

import javax.annotation.Nonnull;

public class SerializablePairLongLongBufferStore extends AbstractSerializablePairLongObjectBufferStore<SerializablePairLongLong>
{
  public SerializablePairLongLongBufferStore(SerializedStorage<SerializablePairLongLong> serializedStorage)
  {
    super(serializedStorage);
  }

  @Override
  @Nonnull
  public AbstractSerializablePairLongObjectColumnHeader<SerializablePairLongLong> createColumnHeader()
  {
    long maxDelta = maxValue - minValue;
    SerializablePairLongLongColumnHeader columnHeader;

    if (minValue < maxValue && maxDelta < 0 || minValue > maxValue) {
      maxDelta = Long.MAX_VALUE;
      minValue = 0;
    }

    if (maxDelta <= Integer.MAX_VALUE) {
      columnHeader = new SerializablePairLongLongColumnHeader(
          SerializablePairLongLongComplexMetricSerde.EXPECTED_VERSION,
          true,
          minValue
      );
    } else {
      columnHeader = new SerializablePairLongLongColumnHeader(
          SerializablePairLongLongComplexMetricSerde.EXPECTED_VERSION,
          false,
          minValue
      );
    }

    return columnHeader;
  }

  @Override
  public AbstractSerializablePairLongObjectDeltaEncodedStagedSerde<SerializablePairLongLong> createDeltaEncodedSerde(AbstractSerializablePairLongObjectColumnHeader<SerializablePairLongLong> columnHeader)
  {
    return new SerializablePairLongLongDeltaEncodedStagedSerde(minValue, columnHeader.isUseIntegerDeltas());
  }
}
