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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.StringUtils;
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
 * Database-specific operations (cleanup, unused marking) are no-ops in this implementation.
 */
public class HeapMemoryCompactionStateManager implements CompactionStateManager
{
  private final ConcurrentMap<String, CompactionState> fingerprintToStateMap = new ConcurrentHashMap<>();
  private final ObjectMapper deterministicMapper;

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
    this.deterministicMapper = deterministicMapper;
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
  @SuppressWarnings("UnstableApiUsage")
  public String generateCompactionStateFingerprint(
      final CompactionState compactionState,
      final String dataSource
  )
  {
    final Hasher hasher = Hashing.sha256().newHasher();

    hasher.putBytes(StringUtils.toUtf8(dataSource));
    hasher.putByte((byte) 0xff);

    try {
      hasher.putBytes(deterministicMapper.writeValueAsBytes(compactionState));
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize CompactionState for fingerprinting", e);
    }
    hasher.putByte((byte) 0xff);

    return BaseEncoding.base16().encode(hasher.hash().asBytes());
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

  /**
   * Gets a compaction state by fingerprint. For test verification only.
   */
  @Nullable
  public CompactionState getCompactionStateByFingerprint(String fingerprint)
  {
    return fingerprintToStateMap.get(fingerprint);
  }

  /**
   * Gets all stored compaction states. For test verification only.
   */
  public Map<String, CompactionState> getAllStoredStates()
  {
    return Map.copyOf(fingerprintToStateMap);
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

}
