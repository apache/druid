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

package org.apache.druid.segment.metadata;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.timeline.CompactionState;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * In-memory implementation of {@link CompactionStateManager} that stores
 * compaction state fingerprints in heap memory without requiring a database.
 * <p>
 * Useful for simulations and unit tests where database persistence is not needed.
 */
public class HeapMemoryCompactionStateManager extends CompactionStateManager
{
  private final ConcurrentMap<String, CompactionState> fingerprintToStateMap = new ConcurrentHashMap<>();

  /**
   * Creates an in-memory compaction state manager with a default deterministic mapper.
   * This is a convenience constructor for tests and simulations.
   */
  public HeapMemoryCompactionStateManager()
  {
    this(createDeterministicMapper());
  }

  /**
   * Creates an in-memory compaction state manager with the provided deterministic mapper
   * for fingerprint generation.
   *
   * @param deterministicMapper ObjectMapper configured for deterministic serialization
   */
  public HeapMemoryCompactionStateManager(ObjectMapper deterministicMapper)
  {
    super(
        new MetadataStorageTablesConfig(null, null, null, null, null, null, null, null, null, null, null, null, null, null),
        new DefaultObjectMapper(),
        deterministicMapper,
        null,
        new CompactionStateManagerConfig()
    );
  }

  /**
   * Creates an ObjectMapper configured for deterministic serialization.
   * Used for generating consistent fingerprints.
   */
  private static ObjectMapper createDeterministicMapper()
  {
    ObjectMapper mapper = new DefaultObjectMapper();
    mapper.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
    mapper.configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true);
    return mapper;
  }

  @Override
  public void persistCompactionState(
      final String dataSource,
      final Map<String, CompactionState> fingerprintToStateMap,
      final DateTime updateTime
  )
  {
    // Store in memory for lookup during simulations/tests
    this.fingerprintToStateMap.putAll(fingerprintToStateMap);
  }

  @Override
  @Nullable
  public CompactionState getCompactionStateByFingerprint(String fingerprint)
  {
    return fingerprintToStateMap.get(fingerprint);
  }

  /**
   * Clears all stored compaction states. Useful for test cleanup or resetting
   * state between test runs.
   */
  public void clear()
  {
    fingerprintToStateMap.clear();
  }

  /**
   * Returns the number of stored compaction state fingerprints.
   */
  public int size()
  {
    return fingerprintToStateMap.size();
  }

  /**
   * Checks if a fingerprint exists in the store.
   */
  public boolean containsFingerprint(String fingerprint)
  {
    return fingerprintToStateMap.containsKey(fingerprint);
  }
}
