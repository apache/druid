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

import com.google.common.collect.ImmutableMap;
import org.apache.druid.client.DruidServer;
import org.apache.druid.common.config.Configs;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.timeline.DataSegment;

import java.util.Map;

/**
 * LoadRules indicate the number of replicants a segment should have in a given tier.
 */
public abstract class LoadRule implements Rule
{
  @Override
  public void run(DataSegment segment, SegmentActionHandler handler)
  {
    Map<String, Integer> tieredReplicants = getTieredReplicants();
    if (tieredReplicants.isEmpty()) {
      handler.replicateSegment(segment, ImmutableMap.of(DruidServer.DEFAULT_TIER, 0));
    } else {
      handler.replicateSegment(segment, tieredReplicants);
    }
  }

  /**
   * Function to create a tiered replicants map choosing default values, in case the map is null.
   * {@code useDefaultTierForNull} decides the default value.
   * <br>
   * If the boolean is true, the default value is a singleton map with key {@link DruidServer#DEFAULT_NUM_REPLICANTS}
   * and value @{@link DruidServer#DEFAULT_TIER}.
   * <br>
   * If the boolean is false, the default value is an empty map. This will enable the new behaviour of not loading
   * segments to a historical unless the tier and number of replicants are explicitly specified.
   */
  protected static Map<String, Integer> createTieredReplicants(final Map<String, Integer> tieredReplicants, boolean useDefaultTierForNull)
  {
    if (useDefaultTierForNull) {
      return Configs.valueOrDefault(tieredReplicants, ImmutableMap.of(DruidServer.DEFAULT_TIER, DruidServer.DEFAULT_NUM_REPLICANTS));
    } else {
      return Configs.valueOrDefault(tieredReplicants, ImmutableMap.of());
    }
  }

  protected static void validateTieredReplicants(final Map<String, Integer> tieredReplicants, boolean useDefaultTierForNull)
  {
    if (tieredReplicants.size() == 0 && useDefaultTierForNull) {
      // If useDefaultTierForNull is true, null is translated to a default tier, and an empty replicant tier map is not allowed.
      throw new IAE("A rule with empty tiered replicants is invalid unless \"useDefaultTierForNull\" is set to false.");
    }
    for (Map.Entry<String, Integer> entry : tieredReplicants.entrySet()) {
      if (entry.getValue() == null) {
        throw new IAE("Replicant value cannot be empty");
      }
      if (entry.getValue() < 0) {
        throw new IAE("Replicant value [%d] is less than 0, which is not allowed", entry.getValue());
      }
    }
  }

  public abstract Map<String, Integer> getTieredReplicants();

  public abstract int getNumReplicants(String tier);

}
