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

package org.apache.druid.server.coordinator.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.common.config.Configs;
import org.joda.time.Duration;

import java.util.Objects;

public class MetadataCleanupConfig
{
  public static final MetadataCleanupConfig DEFAULT = new MetadataCleanupConfig(null, null, null);

  @JsonProperty("on")
  private final boolean cleanupEnabled;

  @JsonProperty("period")
  private final Duration cleanupPeriod;

  @JsonProperty("durationToRetain")
  private final Duration durationToRetain;

  @JsonCreator
  public MetadataCleanupConfig(
      @JsonProperty("on") Boolean cleanupEnabled,
      @JsonProperty("period") Duration cleanupPeriod,
      @JsonProperty("durationToRetain") Duration durationToRetain
  )
  {
    this.cleanupEnabled = Configs.valueOrDefault(cleanupEnabled, true);
    this.cleanupPeriod = Configs.valueOrDefault(cleanupPeriod, Duration.standardDays(1));
    this.durationToRetain = Configs.valueOrDefault(durationToRetain, Duration.standardDays(90));
  }

  public Duration getCleanupPeriod()
  {
    return cleanupPeriod;
  }

  public Duration getDurationToRetain()
  {
    return durationToRetain;
  }

  public boolean isCleanupEnabled()
  {
    return cleanupEnabled;
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
    MetadataCleanupConfig that = (MetadataCleanupConfig) o;
    return cleanupEnabled == that.cleanupEnabled
           && Objects.equals(cleanupPeriod, that.cleanupPeriod)
           && Objects.equals(durationToRetain, that.durationToRetain);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(cleanupEnabled, cleanupPeriod, durationToRetain);
  }
}
