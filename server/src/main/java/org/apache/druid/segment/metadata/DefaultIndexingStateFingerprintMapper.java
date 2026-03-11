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
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.timeline.CompactionState;

import java.util.Optional;

/**
 * Default implementation of {@link IndexingStateFingerprintMapper} that delegates to
 * {@link IndexingStateStorage} for fingerprint generation and {@link IndexingStateCache}
 * for state lookups.
 */
public class DefaultIndexingStateFingerprintMapper implements IndexingStateFingerprintMapper
{
  private final IndexingStateCache indexingStateCache;
  private final ObjectMapper deterministicMapper;

  public DefaultIndexingStateFingerprintMapper(
      IndexingStateCache indexingStateCache,
      ObjectMapper jsonMapper
  )
  {
    this.indexingStateCache = indexingStateCache;
    this.deterministicMapper = createDeterministicMapper(jsonMapper);
  }

  @SuppressWarnings("UnstableApiUsage")
  @Override
  public String generateFingerprint(String dataSource, CompactionState indexingState)
  {
    final Hasher hasher = Hashing.sha256().newHasher();

    hasher.putBytes(StringUtils.toUtf8(dataSource));
    hasher.putByte((byte) 0xff);

    try {
      hasher.putBytes(deterministicMapper.writeValueAsBytes(indexingState));
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize CompactionState object for fingerprinting", e);
    }
    hasher.putByte((byte) 0xff);

    return BaseEncoding.base16().encode(hasher.hash().asBytes());
  }

  @Override
  public Optional<CompactionState> getStateForFingerprint(String fingerprint)
  {
    return indexingStateCache.getIndexingStateByFingerprint(fingerprint);
  }

  /**
   * Decorate the provided {@link ObjectMapper} to ensure deterministic serialization of IndexingState objects.
   */
  private static ObjectMapper createDeterministicMapper(ObjectMapper baseMapper)
  {
    ObjectMapper sortedMapper = baseMapper.copy();
    sortedMapper.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
    sortedMapper.configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true);
    return sortedMapper;
  }
}
