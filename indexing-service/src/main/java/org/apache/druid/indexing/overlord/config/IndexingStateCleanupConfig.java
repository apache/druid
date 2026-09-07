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

package org.apache.druid.indexing.overlord.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.common.config.Configs;
import org.apache.druid.server.coordinator.config.MetadataCleanupConfig;
import org.joda.time.Duration;

import java.util.Objects;

/**
 * Configuration for cleaning up indexing state metadata.
 * <p>
 * Extends {@link MetadataCleanupConfig} to add support for pending state retention.
 */
public class IndexingStateCleanupConfig extends MetadataCleanupConfig
{
  public static final IndexingStateCleanupConfig DEFAULT = new IndexingStateCleanupConfig(null, null, null, null);

  @JsonProperty("pendingDurationToRetain")
  private final Duration pendingDurationToRetain;

  @JsonCreator
  public IndexingStateCleanupConfig(
      @JsonProperty("on") Boolean cleanupEnabled,
      @JsonProperty("period") Duration cleanupPeriod,
      @JsonProperty("durationToRetain") Duration durationToRetain,
      @JsonProperty("pendingDurationToRetain") Duration pendingDurationToRetain
  )
  {
    super(cleanupEnabled, cleanupPeriod, durationToRetain);
    this.pendingDurationToRetain = Configs.valueOrDefault(pendingDurationToRetain, Duration.standardDays(7));
  }

  public Duration getPendingDurationToRetain()
  {
    return pendingDurationToRetain;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    IndexingStateCleanupConfig that = (IndexingStateCleanupConfig) o;
    return Objects.equals(pendingDurationToRetain, that.pendingDurationToRetain);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), pendingDurationToRetain);
  }
}
