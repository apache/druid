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

import org.apache.druid.msq.statistics.ClusterByStatisticsSnapshot;
import org.apache.druid.msq.statistics.KeyCollectorSnapshot;

import java.nio.ByteBuffer;

/**
 * Serializes a {@link ClusterByStatisticsSnapshot} into a byte array.
 */
public abstract class KeyCollectorSnapshotSerializer
{
  /**
   * The type of sketch which has been serialized. The value returned by type cannot be the same across
   * various implementation.
   */
  protected abstract byte getType();

  /**
   * Converts the key collector in the argument into a byte array representation.
   */
  protected abstract byte[] serializeKeyCollector(KeyCollectorSnapshot collectorSnapshot);

  public byte[] serialize(KeyCollectorSnapshot collectorSnapshot)
  {
    byte type = getType();
    byte[] value = serializeKeyCollector(collectorSnapshot);

    return ByteBuffer.allocate(Byte.BYTES + value.length)
                     .put(type)
                     .put(value)
                     .array();
  }
}
