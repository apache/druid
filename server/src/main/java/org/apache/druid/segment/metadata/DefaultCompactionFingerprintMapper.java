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

import com.google.inject.Inject;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.timeline.CompactionState;

import javax.annotation.Nullable;
import java.util.Optional;

/**
 * Default implementation of {@link CompactionFingerprintMapper} that delegates to
 * {@link CompactionStateStorage} for fingerprint generation and {@link CompactionStateCache}
 * for state lookups.
 */
@LazySingleton
public class DefaultCompactionFingerprintMapper implements CompactionFingerprintMapper
{
  private final CompactionStateStorage compactionStateStorage;
  private final CompactionStateCache compactionStateCache;

  @Inject
  public DefaultCompactionFingerprintMapper(
      CompactionStateStorage compactionStateStorage,
      @Nullable CompactionStateCache compactionStateCache
  )
  {
    this.compactionStateStorage = compactionStateStorage;
    this.compactionStateCache = compactionStateCache;
  }

  @Override
  public String generateFingerprint(String dataSource, CompactionState compactionState)
  {
    return compactionStateStorage.generateCompactionStateFingerprint(compactionState, dataSource);
  }

  @Override
  public Optional<CompactionState> getStateForFingerprint(String fingerprint)
  {
    if (compactionStateCache == null) {
      return Optional.empty();
    }
    return compactionStateCache.getCompactionStateByFingerprint(fingerprint);
  }
}
