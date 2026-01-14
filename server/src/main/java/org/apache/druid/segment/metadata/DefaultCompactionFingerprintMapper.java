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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import org.apache.druid.guice.annotations.Deterministic;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.timeline.CompactionState;

import java.util.Optional;

/**
 * Default implementation of {@link CompactionFingerprintMapper} that delegates to
 * {@link CompactionStateStorage} for fingerprint generation and {@link IndexingStateCache}
 * for state lookups.
 */
public class DefaultCompactionFingerprintMapper implements CompactionFingerprintMapper
{
  private final IndexingStateCache indexingStateCache;
  private final ObjectMapper deterministicMapper;

  public DefaultCompactionFingerprintMapper(
      IndexingStateCache indexingStateCache,
      @Deterministic ObjectMapper deterministicMapper
  )
  {
    this.indexingStateCache = indexingStateCache;
    this.deterministicMapper = deterministicMapper;
  }

  /**
   * Generates a deterministic fingerprint for the given compaction state and datasource.
   * <p>
   * The fingerprint is a SHA-256 hash of the datasource name and serialized compaction state that is globally unique in
   * the segment space.
   *
   * @param compactionState     The compaction configuration to fingerprint
   * @param dataSource          The datasource name
   * @return A hex-encoded SHA-256 fingerprint string
   */
  @SuppressWarnings("UnstableApiUsage")
  @Override
  public String generateFingerprint(String dataSource, CompactionState compactionState)
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
  public Optional<CompactionState> getStateForFingerprint(String fingerprint)
  {
    return indexingStateCache.getIndexingStateByFingerprint(fingerprint);
  }
}
