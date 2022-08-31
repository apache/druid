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

package org.apache.druid.msq.statistics;

import com.fasterxml.jackson.databind.JsonDeserializer;

public interface KeyCollectorFactory<TCollector extends KeyCollector<TCollector>, TSnapshot extends KeyCollectorSnapshot>
{
  /**
   * Create a new {@link KeyCollector}
   */
  TCollector newKeyCollector();

  /**
   * Fetches the deserializer that can be used to deserialize the snapshots created by the KeyCollectors corresponding
   * to this factory
   */
  JsonDeserializer<TSnapshot> snapshotDeserializer();

  /**
   * Serializes a {@link KeyCollector} to a {@link KeyCollectorSnapshot}
   */
  TSnapshot toSnapshot(TCollector collector);

  /**
   * Deserializes a {@link KeyCollectorSnapshot} to a {@link KeyCollector}
   */
  TCollector fromSnapshot(TSnapshot snapshot);
}
