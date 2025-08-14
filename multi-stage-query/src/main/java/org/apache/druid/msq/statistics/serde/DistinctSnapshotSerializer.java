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

package org.apache.druid.msq.statistics.serde;

import org.apache.druid.collections.SerializablePair;
import org.apache.druid.frame.key.RowKey;
import org.apache.druid.msq.statistics.DistinctKeySnapshot;
import org.apache.druid.msq.statistics.KeyCollectorSnapshot;

import java.nio.ByteBuffer;

public class DistinctSnapshotSerializer extends KeyCollectorSnapshotSerializer
{
  public static final byte TYPE = (byte) 1;

  @Override
  protected byte getType()
  {
    return TYPE;
  }

  @Override
  protected byte[] serializeKeyCollector(KeyCollectorSnapshot collectorSnapshot)
  {
    final DistinctKeySnapshot snapshot = (DistinctKeySnapshot) collectorSnapshot;
    int length = 2 * Integer.BYTES;
    for (SerializablePair<RowKey, Long> key : snapshot.getKeys()) {
      length += key.lhs.array().length + Integer.BYTES + Long.BYTES;
    }

    final ByteBuffer buffer = ByteBuffer.allocate(length)
                                        .putInt(snapshot.getSpaceReductionFactor())
                                        .putInt(snapshot.getKeys().size());

    for (SerializablePair<RowKey, Long> key : snapshot.getKeys()) {
      byte[] array = key.lhs.array();
      buffer.putLong(key.rhs)
            .putInt(array.length)
            .put(array);
    }
    return buffer.array();
  }
}
