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

package org.apache.druid.indexing.overlord.setup;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public class WorkerTierSpec
{
  // key: taskType, value: tierConfig
  private final Map<String, TierConfig> tierMap;
  private final boolean strong;

  @JsonCreator
  public WorkerTierSpec(
      @JsonProperty("tierMap") Map<String, TierConfig> tierMap,
      @JsonProperty("strong") boolean strong
  )
  {
    this.tierMap = tierMap == null ? Collections.EMPTY_MAP : tierMap;
    this.strong = strong;
  }

  @JsonProperty
  public Map<String, TierConfig> getTierMap()
  {
    return tierMap;
  }

  @JsonProperty
  public boolean isStrong()
  {
    return strong;
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final WorkerTierSpec that = (WorkerTierSpec) o;
    return strong == that.strong &&
           Objects.equals(tierMap, that.tierMap);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(tierMap, strong);
  }

  @Override
  public String toString()
  {
    return "WorkerTierSpec{" +
           "tierMap=" + tierMap +
           ", strong=" + strong +
           '}';
  }

  public static class TierConfig
  {
    private final String defaultTier;
    // key: datasource, value: tier
    private final Map<String, String> tierAffinity;

    @JsonCreator
    public TierConfig(
        @JsonProperty("defaultTier") String defaultTier,
        @JsonProperty("tierAffinity") Map<String, String> tierAffinity
    )
    {
      this.defaultTier = defaultTier;
      this.tierAffinity = tierAffinity == null ? Collections.EMPTY_MAP : tierAffinity;
    }

    @JsonProperty
    public String getDefaultTier()
    {
      return defaultTier;
    }

    @JsonProperty
    public Map<String, String> getTierAffinity()
    {
      return tierAffinity;
    }

    @Override
    public boolean equals(final Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final TierConfig that = (TierConfig) o;
      return Objects.equals(defaultTier, that.defaultTier) &&
             Objects.equals(tierAffinity, that.tierAffinity);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(defaultTier, tierAffinity);
    }

    @Override
    public String toString()
    {
      return "TierConfig{" +
             "defaultTier=" + defaultTier +
             ", tierAffinity=" + tierAffinity +
             '}';
    }
  }
}
