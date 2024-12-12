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

package org.apache.druid.server.coordinator.rules;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.client.DruidServer;
import org.apache.druid.common.config.Configs;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.timeline.DataSegment;

import java.util.Map;
import java.util.Objects;

/**
 * LoadRules indicate the number of replicants a segment should have in a given tier.
 */
public abstract class LoadRule implements Rule
{
  private final Map<String, Integer> tieredReplicants;
  /**
   * Used to determing the default value if tieredReplicants is null in {@link #handleNullTieredReplicants}.
   */
  private final boolean useDefaultTierForNull;

  private final boolean shouldSegmentBeLoaded;

  protected LoadRule(Map<String, Integer> tieredReplicants, Boolean useDefaultTierForNull)
  {
    this.useDefaultTierForNull = Configs.valueOrDefault(useDefaultTierForNull, true);
    this.tieredReplicants = handleNullTieredReplicants(tieredReplicants, this.useDefaultTierForNull);
    validateTieredReplicants(this.tieredReplicants);
    this.shouldSegmentBeLoaded = this.tieredReplicants.values().stream().reduce(0, Integer::sum) > 0;
  }

  @JsonProperty
  public Map<String, Integer> getTieredReplicants()
  {
    return tieredReplicants;
  }

  @JsonProperty
  public boolean useDefaultTierForNull()
  {
    return useDefaultTierForNull;
  }

  @Override
  public void run(DataSegment segment, SegmentActionHandler handler)
  {
    handler.replicateSegment(segment, getTieredReplicants());
  }


  /**
   * @return Whether a segment that matches this rule needs to be loaded on a tier.
   *
   * Used in making handoff decisions.
   */
  @JsonIgnore
  public boolean shouldMatchingSegmentBeLoaded()
  {
    return shouldSegmentBeLoaded;
  }

  /**
   * Returns the given {@code tieredReplicants} map unchanged if it is non-null (including empty).
   * Returns the following default values if the given map is null.
   * <ul>
   * <li>If {@code useDefaultTierForNull} is true, returns a singleton map from {@link DruidServer#DEFAULT_TIER} to {@link DruidServer#DEFAULT_NUM_REPLICANTS}.</li>
   * <li>If {@code useDefaultTierForNull} is false, returns an empty map. This causes segments to have a replication factor of 0 and not get assigned to any historical.</li>
   * </ul>
   */
  private static Map<String, Integer> handleNullTieredReplicants(
      final Map<String, Integer> tieredReplicants,
      boolean useDefaultTierForNull
  )
  {
    if (useDefaultTierForNull) {
      return Configs.valueOrDefault(
          tieredReplicants,
          ImmutableMap.of(DruidServer.DEFAULT_TIER, DruidServer.DEFAULT_NUM_REPLICANTS)
      );
    } else {
      return Configs.valueOrDefault(tieredReplicants, ImmutableMap.of());
    }
  }

  private static void validateTieredReplicants(final Map<String, Integer> tieredReplicants)
  {
    for (Map.Entry<String, Integer> entry : tieredReplicants.entrySet()) {
      if (entry.getValue() == null) {
        throw InvalidInput.exception(
            "Invalid number of replicas for tier [%s]. Value must not be null.",
            entry.getKey()
        );
      }
      if (entry.getValue() < 0) {
        throw InvalidInput.exception(
            "Invalid number of replicas for tier [%s]. Value [%d] must be positive.",
            entry.getKey(),
            entry.getValue()
        );
      }
    }
  }

  public int getNumReplicants(String tier)
  {
    Integer retVal = getTieredReplicants().get(tier);
    return (retVal == null) ? 0 : retVal;
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
    LoadRule loadRule = (LoadRule) o;
    return useDefaultTierForNull == loadRule.useDefaultTierForNull && Objects.equals(
        tieredReplicants,
        loadRule.tieredReplicants
    );
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(tieredReplicants, useDefaultTierForNull);
  }
}
